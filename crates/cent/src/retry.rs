//! Broadcasts, retrying them, and the retry helpers (spec 6, 7.3).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::broadcast::BroadcastWire;
use crate::error::{BoxError, RetryStoppedError, Source, TimeoutError};
use crate::{
    BroadcastFailedChannelsError, BroadcastRequest, BroadcastResult, ChannelResult, Client, Error,
};

/// The wait before retrying, growing exponentially with the attempt: see
/// [`exponential_backoff`].
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    base: Duration,
    limit: Duration,
}

/// Returns the wait before retrying after a failed attempt, with attempt 1
/// being the first failure: `base` after it, twice as long after each next
/// one, up to `limit`. Each wait is a random time between half of it and all
/// of it, so that clients which failed together - when a broker was down - do
/// not all retry at the same moment. A limit below base counts as base, and
/// an attempt below 1 as 1.
///
/// It waits for nothing itself. In a retry loop of your own, with the same
/// `idempotency_key` on every attempt:
///
/// ```no_run
/// # async fn run(client: cent::Client, data: Box<serde_json::value::RawValue>) -> Result<(), cent::Error> {
/// use std::time::{Duration, Instant};
///
/// let backoff = cent::exponential_backoff(Duration::from_millis(200), Duration::from_secs(5));
/// let deadline = Instant::now() + Duration::from_secs(10); // all attempts and waits
/// let client = client.with_deadline(deadline);
/// // The same key on every attempt: a publication which happened in an
/// // earlier attempt, even one reported as failed, is not published again.
/// let request = cent::PublishRequest::new("orders:42").data(data).idempotency_key("order-42-paid");
/// for attempt in 1.. {
///     match client.publish(request.clone()).await {
///         Err(e) if e.is_retryable() && attempt < 3 => cent::wait(deadline, backoff.delay(attempt)).await?,
///         result => return result.map(drop),
///     }
/// }
/// # Ok(()) }
/// ```
#[must_use]
pub fn exponential_backoff(base: Duration, limit: Duration) -> Backoff {
    Backoff {
        base,
        limit: limit.max(base),
    }
}

impl Backoff {
    /// The wait after failed attempt `attempt`, from 1.
    #[must_use]
    pub fn delay(&self, attempt: u32) -> Duration {
        jitter(self.step(attempt))
    }

    /// The wait before jitter: base, doubling, up to limit - never
    /// overflowing.
    fn step(&self, attempt: u32) -> Duration {
        let mut d = self.base;
        for _ in 1..attempt.max(1) {
            if d >= self.limit / 2 {
                return self.limit;
            }
            d *= 2;
        }
        d.min(self.limit)
    }
}

/// A random wait between half of `d` and `d`.
fn jitter(d: Duration) -> Duration {
    let mut b = [0u8; 8];
    // Spreads retries out; nothing depends on it being unpredictable, so a
    // failing random source just leaves the wait at its maximum.
    let r = aws_lc_rs::rand::fill(&mut b).map_or(u64::MAX, |()| u64::from_le_bytes(b));
    let nanos = d.as_nanos();
    let half = nanos / 2;
    let span = nanos - half + 1;
    let n = half + u128::from(r) % span;
    Duration::from_nanos(u64::try_from(n).unwrap_or(u64::MAX))
}

/// Waits for `d`, or until `deadline`, and returns [`Error::Timeout`] when
/// the deadline comes first: the wait between the attempts of a retry loop
/// sharing a deadline. A sleep which ignores the deadline would keep the
/// loop - and [`Client::close`] - waiting after it.
pub async fn wait(deadline: std::time::Instant, d: Duration) -> Result<(), Error> {
    let deadline = Instant::from_std(deadline);
    let until = crate::client::after(d).min(deadline);
    tokio::time::sleep_until(until).await;
    if until >= deadline && Instant::now() >= deadline {
        return Err(Error::Timeout(TimeoutError {
            method: None,
            unsent: false,
        }));
    }
    Ok(())
}

/// Decides what happens after a failed attempt of
/// [`Client::broadcast_with_retry`]: see
/// [`BroadcastRetryOptions::on_attempt_error`].
pub type OnAttemptError = dyn Fn(&BroadcastAttempt<'_>) -> Retry + Send + Sync;

/// What [`Client::broadcast_with_retry`] does after a failed attempt, as its
/// [`on_attempt_error`](BroadcastRetryOptions::on_attempt_error) callback
/// decides.
#[derive(Debug)]
pub enum Retry {
    /// Retries after the default wait, `exponential_backoff(100 ms, 5 s)`.
    Default,
    /// Retries after this wait - ended by the call's deadline.
    After(Duration),
    /// Stops: the call returns an [`Error::RetryStopped`] holding this reason
    /// and the broadcast's error.
    Stop(BoxError),
}

impl Retry {
    /// Stops for `reason`: `Retry::stop("circuit open")`.
    #[must_use]
    pub fn stop(reason: impl Into<BoxError>) -> Self {
        Self::Stop(reason.into())
    }
}

/// How [`Client::broadcast_with_retry`] retries. The default retries once,
/// after a wait of about 100 milliseconds.
///
/// ```
/// use std::time::Duration;
///
/// let options = cent::BroadcastRetryOptions::default()
///     .max_attempts(4)
///     .attempt_timeout(Duration::from_secs(2)) // each attempt
///     .on_attempt_error(|a| {
///         eprintln!("attempt {}: {} channels to retry: {}", a.attempt, a.retry.len(), a.error);
///         cent::Retry::Default // or Retry::After(wait), Retry::stop(reason)
///     });
/// ```
#[derive(Clone, Default)]
pub struct BroadcastRetryOptions {
    max_attempts: Option<u32>,
    attempt_timeout: Option<Duration>,
    on_attempt_error: Option<Arc<OnAttemptError>>,
}

impl fmt::Debug for BroadcastRetryOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BroadcastRetryOptions")
            .field("max_attempts", &self.max_attempts)
            .field("attempt_timeout", &self.attempt_timeout)
            .field(
                "on_attempt_error",
                &self.on_attempt_error.as_ref().map(|_| "<function>"),
            )
            .finish()
    }
}

const DEFAULT_MAX_ATTEMPTS: u32 = 2;
const DEFAULT_BACKOFF: Backoff = Backoff {
    base: Duration::from_millis(100),
    limit: Duration::from_secs(5),
};

impl BroadcastRetryOptions {
    /// How many times the broadcast is sent at most, the first time
    /// included: 2 by default, one retry.
    #[must_use]
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = Some(n);
        self
    }

    /// Bounds each attempt, while the call's deadline bounds them all, waits
    /// included: an attempt which hangs times out, and its channels are
    /// retried while the call has time left. By default the call's deadline
    /// alone bounds each attempt.
    #[must_use]
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = Some(timeout);
        self
    }

    /// Calls `f` after each attempt which failed in a way a retry may fix,
    /// except the last. It may log or count, and returns a [`Retry`]: the
    /// default wait, a wait of its own, or stop. The wait ends with the
    /// call's deadline.
    ///
    /// An [`Error::Http`] in [`a.error`](BroadcastAttempt::error) carries the
    /// reply's headers: wait at least its `Retry-After` to honour one.
    #[must_use]
    pub fn on_attempt_error(
        mut self,
        f: impl Fn(&BroadcastAttempt<'_>) -> Retry + Send + Sync + 'static,
    ) -> Self {
        self.on_attempt_error = Some(Arc::new(f));
        self
    }
}

/// A failed attempt of [`Client::broadcast_with_retry`], for its
/// [`on_attempt_error`](BroadcastRetryOptions::on_attempt_error) callback.
#[derive(Debug)]
#[non_exhaustive]
pub struct BroadcastAttempt<'a> {
    /// The attempt which failed, from 1.
    pub attempt: u32,
    /// Its error: [`Error::BroadcastFailedChannels`] when Centrifugo replied
    /// and some channels failed, otherwise the error of the request as a
    /// whole.
    pub error: &'a Error,
    /// The channels the next attempt sends, with the errors this attempt
    /// failed them with. A broadcast to many channels which all failed lists
    /// every one: log a count or a few, not all of them.
    pub retry: &'a [ChannelResult],
    /// Every channel's outcome so far, as the call would return it if it
    /// stopped now.
    pub result: &'a BroadcastResult,
    /// The key every attempt is sent with.
    pub idempotency_key: &'a str,
}

impl Client {
    /// Publishes the same data into many channels in one request: the way to
    /// send one message to many channels, Centrifugo doing the work of all
    /// the publications at once.
    ///
    /// Channels succeed or fail on their own. It returns every channel's
    /// outcome when every channel got the publication, and otherwise an
    /// error:
    ///
    /// - [`Error::BroadcastFailedChannels`] when Centrifugo carried the
    ///   broadcast out and one or more channels failed, up to all; it holds
    ///   every channel's outcome.
    /// - Any other error when the broadcast failed as a whole: no channel got
    ///   a reply. After a timeout or a network error Centrifugo may still
    ///   have published it.
    ///
    /// To go on despite some failed channels:
    ///
    /// ```no_run
    /// # async fn run(client: cent::Client, request: cent::BroadcastRequest) -> Result<(), cent::Error> {
    /// match client.broadcast(request).await {
    ///     Ok(_) => {}
    ///     Err(cent::Error::BroadcastFailedChannels(e)) => {
    ///         for (channel, err) in e.errors() {
    ///             eprintln!("not published into {channel}: {err}");
    ///         }
    ///     }
    ///     Err(e) => return Err(e), // no channel got a reply
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// Retry a broadcast with [`broadcast_with_retry`](Self::broadcast_with_retry):
    /// it sends again only the channels which may still succeed, with one
    /// `idempotency_key`. Calling `broadcast` again after some channels
    /// failed sends the publication to every channel again, those which got
    /// it included - twice, without an `idempotency_key` or with the NATS
    /// broker, which ignores keys.
    pub async fn broadcast(&self, request: BroadcastRequest) -> Result<BroadcastResult, Error> {
        let channels = request.channels.clone();
        let key = request.idempotency_key.clone();
        let wire: BroadcastWire = self.call(request).await?;
        wire.into_result(&channels, &key)
    }

    /// Broadcasts as [`broadcast`](Self::broadcast) does, and retries the
    /// channels which failed for a temporary reason - see
    /// [`retryable`](crate::retryable) - up to `options`' `max_attempts`.
    ///
    /// - Only the channels which may still succeed are sent again. A channel
    ///   which failed for good, an unknown channel say, is not, and when only
    ///   such channels are left, it stops.
    /// - Every attempt carries the same `idempotency_key`, so that a channel
    ///   which got the publication in an earlier attempt - even one reported
    ///   as failed, after a timeout - does not get it again. When the request
    ///   has none, a random one is made for the call. The memory, Redis and
    ///   PostgreSQL brokers keep keys; with the NATS broker a retry may
    ///   publish again.
    /// - The result holds every channel's last outcome: the publication's
    ///   position from whichever attempt succeeded, or the error of its last
    ///   attempt. Its `idempotency_key` is the key the attempts used:
    ///   retrying the broadcast later with it, after this call gave up,
    ///   publishes only into the channels which did not get it yet.
    ///
    /// The call's deadline - its timeout, or `request_timeout` - bounds the
    /// whole call, the waits between attempts included, and
    /// `attempt_timeout` each attempt. Once the deadline passes no attempt
    /// follows.
    ///
    /// It returns as `broadcast` does: every channel's outcome when every
    /// channel got the publication, an [`Error::BroadcastFailedChannels`]
    /// listing those which still failed when some attempt got a reply, and
    /// otherwise the last attempt's error. When the retries stop early - by
    /// the callback, or by the deadline while waiting - the error is an
    /// [`Error::RetryStopped`] holding the broadcast's error.
    pub async fn broadcast_with_retry(
        &self,
        mut request: BroadcastRequest,
        options: BroadcastRetryOptions,
    ) -> Result<BroadcastResult, Error> {
        let max_attempts = options.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS);
        if max_attempts == 0 {
            return Err(Error::invalid_request("broadcast", "max_attempts is zero"));
        }
        if options.attempt_timeout.is_some_and(|t| t.is_zero()) {
            return Err(Error::invalid_request(
                "broadcast",
                "attempt_timeout is zero",
            ));
        }
        if request.idempotency_key.is_empty() {
            request.idempotency_key = random_key();
        }
        let deadline = self.deadline();
        let channels = std::mem::take(&mut request.channels);
        let mut fin = BroadcastResult {
            channels: channels
                .iter()
                .map(|ch| ChannelResult {
                    channel: ch.clone(),
                    result: Err(Error::BatchNotSent),
                    attempts: 0,
                })
                .collect(),
            idempotency_key: request.idempotency_key.clone(),
            attempts: 0,
            replied: false,
        };
        // The positions in `channels` of what the next attempt sends:
        // positions, not names, so that a channel listed twice keeps both.
        let mut pending: Vec<usize> = (0..channels.len()).collect();
        let mut last_err = None;
        for attempt in 1..=max_attempts {
            // No attempt starts once the call's deadline has passed: the
            // first fails as any call does then.
            if attempt > 1 && Instant::now() >= deadline {
                break;
            }
            let mut attempt_req = request.clone();
            attempt_req.channels = pending.iter().map(|&i| channels[i].clone()).collect();
            let attempt_deadline = match options.attempt_timeout {
                Some(t) => deadline.min(crate::client::after(t)),
                None => deadline,
            };
            let client = self.with_bound(Some(crate::client::Bound::Deadline(attempt_deadline)));
            let outcome = client.broadcast(attempt_req).await;
            let sent = !matches!(outcome, Err(Error::InvalidRequest(_)));
            fin.attempts += u32::from(sent);
            let err = match outcome {
                Ok(r) => {
                    record(&mut fin, &pending, r.channels);
                    None
                }
                Err(Error::BroadcastFailedChannels(e)) => {
                    record(&mut fin, &pending, e.result().channels.clone());
                    Some(Error::BroadcastFailedChannels(e))
                }
                // Failed as a whole: every channel sent failed with it.
                Err(e) => {
                    for &pos in &pending {
                        fin.channels[pos].result = Err(e.clone());
                        fin.channels[pos].attempts += u32::from(sent);
                    }
                    Some(e)
                }
            };
            let Some(err) = err else { break };
            // An ended deadline leaves no time for another attempt, whatever
            // the callback would do.
            if !err.is_retryable() || attempt == max_attempts || Instant::now() >= deadline {
                last_err = Some(err);
                break;
            }
            // Next, the channels which may still succeed.
            pending.retain(|&pos| {
                fin.channels[pos]
                    .result
                    .as_ref()
                    .is_err_and(crate::retryable)
            });
            let retry: Vec<ChannelResult> = pending
                .iter()
                .map(|&pos| fin.channels[pos].clone())
                .collect();
            let attempt_info = BroadcastAttempt {
                attempt,
                error: &err,
                retry: &retry,
                result: &fin,
                idempotency_key: &request.idempotency_key,
            };
            let stop = wait_or_stop(&options, &attempt_info, deadline).await;
            if let Some(reason) = stop {
                return stopped(reason, final_error(fin, Some(err)));
            }
            last_err = Some(err);
        }
        final_error(fin, last_err)
    }
}

/// Asks `on_attempt_error` what to do after a failed attempt, and waits as
/// it says - bounded by the deadline. Returns why to stop, if it should.
async fn wait_or_stop(
    options: &BroadcastRetryOptions,
    attempt: &BroadcastAttempt<'_>,
    deadline: Instant,
) -> Option<Source> {
    let decision = options
        .on_attempt_error
        .as_ref()
        .map_or(Retry::Default, |f| f(attempt));
    let wait = match decision {
        Retry::Default => DEFAULT_BACKOFF.delay(attempt.attempt),
        Retry::After(wait) => wait,
        Retry::Stop(reason) => return Some(Arc::from(reason)),
    };
    tokio::time::sleep_until(crate::client::after(wait).min(deadline)).await;
    (Instant::now() >= deadline).then(|| {
        Arc::new(Error::Timeout(TimeoutError {
            method: None,
            unsent: false,
        })) as Source
    })
}

/// The outcome of a broadcast whose retries stopped for `reason`.
fn stopped(
    reason: Source,
    outcome: Result<BroadcastResult, Error>,
) -> Result<BroadcastResult, Error> {
    match outcome {
        // The deadline, which the broadcast's error already says: naming it
        // twice says nothing more.
        Err(e)
            if e.is_timeout()
                && reason
                    .downcast_ref::<Error>()
                    .is_some_and(Error::is_timeout) =>
        {
            Err(e)
        }
        Err(e) => Err(Error::RetryStopped(Box::new(RetryStoppedError::new(
            reason, e,
        )))),
        Ok(r) => Ok(r),
    }
}

/// Records an attempt which got a reply: the outcomes of the channels it
/// sent, at their positions.
fn record(fin: &mut BroadcastResult, pending: &[usize], results: Vec<ChannelResult>) {
    fin.replied = true;
    for (&pos, r) in pending.iter().zip(results) {
        fin.channels[pos].result = r.result;
        fin.channels[pos].attempts += 1;
    }
}

/// The outcome of a broadcast sent in several attempts: every channel's
/// outcome when all got the publication, the partial error when some attempt
/// got a reply, and otherwise the last attempt's error - no channel got a
/// reply.
fn final_error(fin: BroadcastResult, last: Option<Error>) -> Result<BroadcastResult, Error> {
    if let Some(e) = last.filter(|_| !fin.replied) {
        return Err(e);
    }
    if fin.channels.iter().any(|ch| ch.result.is_err()) {
        return Err(Error::BroadcastFailedChannels(Box::new(
            BroadcastFailedChannelsError::new(fin),
        )));
    }
    Ok(fin)
}

/// A random idempotency key: `rustcent-` and 26 base32 characters, 128 bits.
fn random_key() -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut b = [0u8; 16];
    if aws_lc_rs::rand::fill(&mut b).is_err() {
        // Never in practice; a key must still differ between broadcasts.
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        b = (t.as_nanos() ^ u128::from(std::process::id())).to_le_bytes();
    }
    let n = u128::from_le_bytes(b);
    let mut key = String::from("rustcent-");
    for i in 0..26 {
        let idx = usize::try_from((n >> (i * 5)) & 31).expect("5 bits");
        key.push(char::from(ALPHABET[idx]));
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_steps() {
        let b = exponential_backoff(Duration::from_millis(100), Duration::from_secs(1));
        assert_eq!(b.step(0), Duration::from_millis(100));
        assert_eq!(b.step(1), Duration::from_millis(100));
        assert_eq!(b.step(2), Duration::from_millis(200));
        assert_eq!(b.step(4), Duration::from_millis(800));
        assert_eq!(b.step(5), Duration::from_secs(1));
        assert_eq!(b.step(u32::MAX), Duration::from_secs(1));
        // A limit below base counts as base.
        let b = exponential_backoff(Duration::from_secs(2), Duration::from_secs(1));
        assert_eq!(b.step(3), Duration::from_secs(2));
        // Doubling never overflows.
        let b = exponential_backoff(Duration::MAX / 3, Duration::MAX);
        assert_eq!(b.step(10), Duration::MAX);
        let b = exponential_backoff(Duration::ZERO, Duration::ZERO);
        assert_eq!(b.delay(7), Duration::ZERO);
    }

    #[test]
    fn jitter_stays_in_range() {
        for _ in 0..1000 {
            let d = jitter(Duration::from_millis(100));
            assert!(
                d >= Duration::from_millis(50) && d <= Duration::from_millis(100),
                "{d:?}"
            );
        }
        assert_eq!(
            jitter(Duration::MAX).max(Duration::MAX / 2),
            jitter(Duration::MAX).max(Duration::MAX / 2)
        );
    }

    #[test]
    fn keys() {
        let (a, b) = (random_key(), random_key());
        assert_ne!(a, b);
        assert_eq!(a.len(), "rustcent-".len() + 26);
        assert!(
            a["rustcent-".len()..]
                .bytes()
                .all(|c| c.is_ascii_uppercase() || (b'2'..=b'7').contains(&c))
        );
    }
}
