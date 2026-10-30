//! Broadcast results, and the partial-failure error.

use std::fmt;

#[cfg(feature = "client")]
use serde::Deserialize;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

#[cfg(feature = "client")]
use crate::error::ApiError;
use crate::{Error, PublishResult};

/// The outcome of a broadcast: one entry per channel, in the order the
/// request listed them.
///
/// A call returns it when every channel got the publication. When some
/// failed, the call's error is an [`Error::BroadcastFailedChannels`] holding
/// it. Encoded as JSON it gives every channel's outcome, by wire names.
#[derive(Debug, Clone, Default, Serialize)]
#[non_exhaustive]
pub struct BroadcastResult {
    /// Every channel's outcome.
    pub channels: Vec<ChannelResult>,
    /// The key the broadcast was sent with: the request's, or the one
    /// [`Client::broadcast_with_retry`](crate::Client::broadcast_with_retry)
    /// made when the request had none. A later retry of the same broadcast -
    /// from a job queue, say - sent with this key does not publish again into
    /// the channels which got it.
    pub idempotency_key: String,
    /// How many times the broadcast was sent: 1 from a broadcast, up to its
    /// `max_attempts` from a broadcast with retries.
    pub attempts: u32,
    /// Whether Centrifugo carried the broadcast out and replied for every
    /// channel, the failed ones included. A broadcast which failed as a whole
    /// (the network, a timeout, refused credentials) returns its own error
    /// and no result, so a result from the client always has it set; a test
    /// double may set it to stand for either case.
    pub replied: bool,
}

impl BroadcastResult {
    /// Returns a result of `channels`, for test doubles.
    #[must_use]
    pub fn new(channels: Vec<ChannelResult>) -> Self {
        Self {
            channels,
            idempotency_key: String::new(),
            attempts: 1,
            replied: true,
        }
    }

    /// The channels which did not get the publication, with their errors,
    /// in the order of the request.
    #[must_use]
    pub fn failed(&self) -> Vec<&ChannelResult> {
        self.channels
            .iter()
            .filter(|ch| ch.result.is_err())
            .collect()
    }

    /// The channels which did not get the publication, each with its error,
    /// in the order of the request.
    pub fn errors(&self) -> impl Iterator<Item = (&str, &Error)> {
        self.channels
            .iter()
            .filter_map(|ch| Some((ch.channel.as_str(), ch.error()?)))
    }
}

/// The outcome of a broadcast in one channel.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ChannelResult {
    /// The channel, as the request named it.
    pub channel: String,
    /// The publication's position in the channel's history - zero when the
    /// channel keeps none - or the channel's error.
    pub result: Result<PublishResult, Error>,
    /// How many times the channel was sent: more than 1 when a broadcast
    /// with retries retried it.
    pub attempts: u32,
}

impl ChannelResult {
    /// Returns a channel's outcome, for test doubles.
    #[must_use]
    pub fn new(channel: impl Into<String>, result: Result<PublishResult, Error>) -> Self {
        Self {
            channel: channel.into(),
            result,
            attempts: 1,
        }
    }

    /// The channel's error, or `None` when it got the publication.
    #[must_use]
    pub fn error(&self) -> Option<&Error> {
        self.result.as_ref().err()
    }
}

/// `{"channel": ..., "result": {...}, "attempts": n}`, or `"error"` in place of
/// `"result"`.
impl Serialize for ChannelResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("channel", &self.channel)?;
        match &self.result {
            Ok(r) => map.serialize_entry("result", r)?,
            Err(e) => map.serialize_entry("error", e)?,
        }
        map.serialize_entry("attempts", &self.attempts)?;
        map.end()
    }
}

/// A broadcast which Centrifugo carried out, and which failed in one or more
/// of its channels, up to all of them: the others got the publication.
///
/// It does not match its channels' errors: one unknown channel among many
/// does not make [`Error::api_code`] return 102. Look at each channel's error
/// in [`failed`](Self::failed) instead. Retrying the whole broadcast would
/// publish again into the channels which got it - unless every attempt has
/// the same `idempotency_key`: retry only
/// [`retryable_channels`](Self::retryable_channels), or use
/// [`Client::broadcast_with_retry`](crate::Client::broadcast_with_retry).
#[derive(Debug, Clone)]
pub struct BroadcastFailedChannelsError {
    result: BroadcastResult,
}

impl BroadcastFailedChannelsError {
    /// Returns the error of a broadcast whose outcome is `result`, in which
    /// some channel failed, for test doubles.
    #[must_use]
    pub fn new(result: BroadcastResult) -> Self {
        Self { result }
    }

    /// Every channel's outcome.
    #[must_use]
    pub fn result(&self) -> &BroadcastResult {
        &self.result
    }

    /// Takes the result.
    #[must_use]
    pub fn into_result(self) -> BroadcastResult {
        self.result
    }

    /// The channels which failed, with their errors, in the order of the
    /// request.
    #[must_use]
    pub fn failed(&self) -> Vec<&ChannelResult> {
        self.result.failed()
    }

    /// The channels which failed, each with its error, in the order of the
    /// request:
    ///
    /// ```
    /// # fn report(e: &cent::BroadcastFailedChannelsError) {
    /// for (channel, err) in e.errors() {
    ///     eprintln!("not published into {channel}: {err}");
    /// }
    /// # }
    /// ```
    pub fn errors(&self) -> impl Iterator<Item = (&str, &Error)> {
        self.result.errors()
    }

    /// How many channels the broadcast had.
    #[must_use]
    pub fn total(&self) -> usize {
        self.result.channels.len()
    }

    /// The channels whose failure is retryable - see
    /// [`retryable`](crate::retryable) - in the order of the request: those
    /// a retry should go to, with the same `idempotency_key`. Channels which
    /// failed for good, such as an unknown channel, are left out.
    #[must_use]
    pub fn retryable_channels(&self) -> Vec<String> {
        self.errors()
            .filter(|(_, e)| crate::retryable(e))
            .map(|(ch, _)| ch.to_owned())
            .collect()
    }
}

impl fmt::Display for BroadcastFailedChannelsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let items = self.errors().map(|(ch, e)| (ch, e.to_string()));
        write_failures(
            f,
            "broadcast",
            "channels",
            "got the publication",
            self.errors().count(),
            self.total(),
            items,
        )?;
        if !self.retryable_channels().is_empty() {
            f.write_str(" (retry only retryable_channels(), with the same idempotency_key)")?;
        }
        Ok(())
    }
}

impl std::error::Error for BroadcastFailedChannelsError {}

/// How many failures an aggregate error names in its message; all of them
/// stay available in the error itself.
const MAX_ERROR_ITEMS: usize = 3;

/// Writes the message of an error listing the failed parts of a request:
/// "batch: 2 of 3 commands failed, 1 succeeded: ...".
pub(crate) fn write_failures(
    f: &mut fmt::Formatter<'_>,
    op: &str,
    parts: &str,
    rest: &str,
    failed: usize,
    total: usize,
    items: impl Iterator<Item = (impl fmt::Display, String)>,
) -> fmt::Result {
    write!(f, "{op}: {failed} of {total} {parts} failed")?;
    if failed < total {
        write!(f, ", {} {rest}", total - failed)?;
    }
    for (i, (label, err)) in items.enumerate() {
        if i == MAX_ERROR_ITEMS {
            write!(f, "; and {} more", failed - i)?;
            break;
        }
        f.write_str(if i == 0 { ": " } else { "; " })?;
        write!(f, "{label}: {err}")?;
    }
    Ok(())
}

/// A broadcast's result as Centrifugo sends it: a response per channel,
/// positionally matching the request's channels.
#[cfg(feature = "client")]
#[derive(Debug, Default, Deserialize)]
pub(crate) struct BroadcastWire {
    #[serde(default, deserialize_with = "crate::wire::nullable")]
    responses: Vec<ChannelResponse>,
}

#[cfg(feature = "client")]
#[derive(Debug, Default, Deserialize)]
struct ChannelResponse {
    #[serde(default)]
    error: Option<ApiErrorWire>,
    #[serde(default)]
    result: Option<PublishResult>,
}

/// A Centrifugo error as it is on the wire.
#[cfg(feature = "client")]
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ApiErrorWire {
    #[serde(default)]
    pub(crate) code: u32,
    #[serde(default, deserialize_with = "crate::wire::nullable")]
    pub(crate) message: String,
    #[serde(default, deserialize_with = "crate::wire::nullable")]
    pub(crate) temporary: bool,
}

#[cfg(feature = "client")]
impl From<ApiErrorWire> for ApiError {
    fn from(w: ApiErrorWire) -> Self {
        ApiError::new(w.code, w.message).temporary(w.temporary)
    }
}

#[cfg(feature = "client")]
impl BroadcastWire {
    /// Pairs each response with its channel - the reply does not repeat
    /// them - and returns the outcome, with its partial error when any
    /// channel failed.
    pub(crate) fn into_result(
        self,
        channels: &[String],
        key: &str,
    ) -> Result<BroadcastResult, Error> {
        if self.responses.len() != channels.len() {
            return Err(Error::Decode(crate::error::DecodeError::message(format!(
                "got {} replies for {} channels",
                self.responses.len(),
                channels.len()
            ))));
        }
        let mut failed = false;
        let channels = self
            .responses
            .into_iter()
            .zip(channels)
            .map(|(r, ch)| {
                let result = match r.error {
                    Some(e) => {
                        failed = true;
                        Err(Error::Api(e.into()))
                    }
                    None => Ok(r.result.unwrap_or_default()),
                };
                ChannelResult::new(ch.clone(), result)
            })
            .collect();
        let result = BroadcastResult {
            channels,
            idempotency_key: key.to_owned(),
            attempts: 1,
            replied: true,
        };
        if failed {
            return Err(Error::BroadcastFailedChannels(Box::new(
                BroadcastFailedChannelsError::new(result),
            )));
        }
        Ok(result)
    }
}
