//! Batches: [`Batch`], [`Pending`], and the partial-failure error.

use std::fmt;
use std::sync::{Arc, OnceLock};

use serde::de::DeserializeOwned;
use serde_json::value::RawValue;

use crate::broadcast::{BroadcastWire, write_failures};
use crate::client::{
    RequestInfo, batch_body, command_bytes, decode_batch, decode_reply, is_error_reply,
};
use crate::error::InvalidRequestError;
use crate::{BroadcastRequest, BroadcastResult, Client, Error, Request};

/// How Centrifugo runs a batch. The options are set when the batch is
/// created, since they change what its commands mean together: in what order
/// they take effect.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct BatchOptions {
    /// Lets Centrifugo run the commands concurrently: faster for many
    /// independent commands, but they no longer take effect in the order
    /// they were added.
    ///
    /// Without it, the commands run one by one, in order. Centrifugo PRO
    /// sends a batch's publications to its broker together, though: each
    /// channel's publications take effect in order, but publications into
    /// different channels may not, unless the namespace sets
    /// `publication_grouping_disabled`.
    pub parallel: bool,
}

impl BatchOptions {
    /// Sets [`parallel`](Self::parallel).
    #[must_use]
    pub fn parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }
}

/// A command's outcome, where its [`Pending`] reads it: set once, when the
/// batch is sent.
type Slot<T> = Arc<OnceLock<Result<T, Error>>>;

/// Delivers a command's outcome to its `Pending`: decodes its reply, or
/// takes the error which failed the batch.
trait Deliver: Send + Sync {
    /// Decodes `reply` into the slot, and returns the command's error.
    fn reply(&self, reply: &RawValue) -> Option<Error>;
    /// Fails the command with `err`.
    fn fail(&self, err: &Error);
}

/// Decodes a command's reply into its result.
type Decode<T> = Box<dyn Fn(&RawValue) -> Result<T, Error> + Send + Sync>;

struct Delivery<T> {
    slot: Slot<T>,
    decode: Decode<T>,
}

impl<T: Send + Sync> Deliver for Delivery<T> {
    fn reply(&self, reply: &RawValue) -> Option<Error> {
        let outcome = (self.decode)(reply);
        let err = outcome.as_ref().err().cloned();
        let _ = self.slot.set(outcome);
        err
    }

    fn fail(&self, err: &Error) {
        let _ = self.slot.set(Err(err.clone()));
    }
}

/// Commands sent to Centrifugo in one request. Create one with
/// [`Client::new_batch`], add commands with its methods - one per API method,
/// such as [`Batch::publish`] - then [`send`](Batch::send) it:
///
/// ```no_run
/// # async fn run(client: cent::Client, data: Box<serde_json::value::RawValue>) -> Result<(), cent::Error> {
/// let mut batch = client.new_batch(cent::BatchOptions::default());
/// let publication = batch.publish(cent::PublishRequest::new("news").data(data));
/// let stats = batch.presence_stats(cent::PresenceStatsRequest::new("news"));
/// batch.send().await?; // an error unless every command succeeded
/// let offset = publication.result()?.offset; // this command's own outcome
/// # Ok(()) }
/// ```
///
/// Each method returns a [`Pending`] holding that command's outcome once the
/// batch is sent. Sending takes the batch: a batch is sent once, and nothing
/// can be added to a sent batch.
pub struct Batch {
    client: Client,
    options: BatchOptions,
    commands: Vec<Command>,
}

struct Command {
    method: &'static str,
    /// The command, encoded - or why it cannot be sent.
    encoded: Result<Vec<u8>, InvalidRequestError>,
    deliver: Box<dyn Deliver>,
}

impl fmt::Debug for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("options", &self.options)
            .field(
                "commands",
                &self.commands.iter().map(|c| c.method).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Returns an empty batch, sent by this client and run as `options`
    /// say. `BatchOptions::default()` runs the commands one by one, in the
    /// order they were added.
    #[must_use]
    pub fn new_batch(&self, options: BatchOptions) -> Batch {
        Batch {
            client: self.clone(),
            options,
            commands: Vec::new(),
        }
    }
}

impl Batch {
    /// How many commands the batch has.
    #[must_use]
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Whether the batch has no commands.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    // Requests are taken by value, as the client's methods take them: a
    // command and a call are written the same way.
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn add<R: Request, T: DeserializeOwned + Default + Send + Sync + 'static>(
        &mut self,
        request: R,
    ) -> Pending<T> {
        let method = R::METHOD;
        self.push(
            &request,
            Box::new(move |raw| decode_reply::<T>(raw, method)),
        )
    }

    fn push<R: Request, T: Send + Sync + 'static>(
        &mut self,
        request: &R,
        decode: Decode<T>,
    ) -> Pending<T> {
        let encoded = match request.validate() {
            Err(Error::InvalidRequest(e)) => Err(e),
            Err(e) => Err(InvalidRequestError::new(R::METHOD, e.to_string())),
            Ok(()) => serde_json::to_vec(request)
                .map(|r| command_bytes(R::METHOD, &r))
                .map_err(|e| {
                    InvalidRequestError::new(R::METHOD, format!("cannot be encoded: {e}"))
                }),
        };
        let slot = Slot::default();
        let deliver = Box::new(Delivery {
            slot: Arc::clone(&slot),
            decode,
        });
        self.commands.push(Command {
            method: R::METHOD,
            encoded,
            deliver,
        });
        Pending {
            source: Source::Batch(slot),
        }
    }

    /// Adds a `broadcast` command. Its outcome, once the batch is sent, is
    /// what [`Client::broadcast`] would have returned: every channel's
    /// outcome, and an [`Error::BroadcastFailedChannels`] when any channel
    /// failed - which fails the command in the batch too.
    #[allow(clippy::needless_pass_by_value)] // as the client's broadcast
    pub fn broadcast(&mut self, request: BroadcastRequest) -> Pending<BroadcastResult> {
        let channels = request.channels.clone();
        let key = request.idempotency_key.clone();
        self.push(
            &request,
            Box::new(move |raw| {
                decode_reply::<BroadcastWire>(raw, "broadcast")
                    .and_then(|w| w.into_result(&channels, &key))
            }),
        )
    }

    /// Sends the batch, and waits for Centrifugo's reply to every command.
    ///
    /// Like every call, it returns an error unless every command succeeded:
    ///
    /// - `Ok` means every command succeeded.
    /// - [`Error::BatchFailedCommands`] means one or more commands failed, up
    ///   to all. Every command got its own reply: each [`Pending`] holds its
    ///   outcome, and the error lists the failed ones.
    /// - Any other error means the batch failed as a whole - the network, a
    ///   timeout, refused credentials: no command got a reply, and every
    ///   `Pending` returns that error. After a timeout or a network error,
    ///   Centrifugo may still have carried the batch out.
    ///
    /// A command the client can tell is invalid fails the whole batch before
    /// anything is sent, naming it. An empty batch does nothing and
    /// succeeds - but returns [`Error::Closed`] from a closed client.
    pub async fn send(self) -> Result<BatchResult, Error> {
        let total = self.commands.len();
        match self.send_replies().await {
            Ok((commands, replies)) => {
                let mut failed = Vec::new();
                for (i, (c, raw)) in commands.iter().zip(&replies).enumerate() {
                    if let Some(error) = c.deliver.reply(raw) {
                        failed.push(FailedCommand {
                            index: i,
                            method: c.method,
                            error,
                        });
                    }
                }
                let result = BatchResult {
                    replied: true,
                    failed,
                };
                if result.failed.is_empty() {
                    Ok(result)
                } else {
                    Err(Error::BatchFailedCommands(Box::new(
                        BatchFailedCommandsError::new(result, total),
                    )))
                }
            }
            Err((commands, e)) => {
                for c in &commands {
                    c.deliver.fail(&e);
                }
                Err(e)
            }
        }
    }

    /// Sends the commands, and returns them with their replies - or with the
    /// error which failed the batch as a whole.
    #[allow(clippy::type_complexity)]
    async fn send_replies(
        self,
    ) -> Result<(Vec<Command>, Vec<Box<RawValue>>), (Vec<Command>, Error)> {
        let Batch {
            client,
            options,
            commands,
        } = self;
        let _call = match client.shared.enter() {
            Ok(c) => c,
            Err(e) => return Err((commands, e)),
        };
        if commands.is_empty() {
            return Ok((commands, Vec::new()));
        }
        let mut encoded = Vec::with_capacity(commands.len());
        for (i, c) in commands.iter().enumerate() {
            match &c.encoded {
                Ok(e) => encoded.push(e.as_slice()),
                Err(e) => {
                    let e = Error::InvalidRequest(e.clone().in_command(i));
                    return Err((commands, e));
                }
            }
        }
        let body = batch_body(encoded, options.parallel);
        let engine = match client.engine() {
            Ok(e) => e,
            Err(e) => return Err((commands, e)),
        };
        let deadline = client.deadline();
        if tokio::time::Instant::now() >= deadline {
            let e = Error::Timeout(crate::error::TimeoutError {
                method: Some("batch"),
                unsent: true,
            });
            return Err((commands, e));
        }
        let start = std::time::Instant::now();
        let mut status = 0;
        let replies = tokio::time::timeout_at(
            deadline,
            client.post(&engine, "batch", false, body, &mut status),
        )
        .await
        .unwrap_or_else(|_| Err(Error::timeout("batch")))
        .and_then(|b| decode_batch(&b, commands.len()));
        client.shared.report(&RequestInfo {
            method: "batch",
            commands: commands.len(),
            failed: replies
                .as_ref()
                .map_or(0, |r| r.iter().filter(|r| is_error_reply(r)).count()),
            auto_batch: false,
            duration: start.elapsed(),
            status,
            error: replies.as_ref().err(),
        });
        match replies {
            Ok(r) => Ok((commands, r)),
            Err(e) => Err((commands, e)),
        }
    }
}

/// The outcome of one command of a [`Batch`], available once the batch is
/// sent.
pub struct Pending<T> {
    source: Source<T>,
}

enum Source<T> {
    Batch(Slot<T>),
    Fixed(Result<T, Error>),
}

impl<T> fmt::Debug for Pending<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.source {
            Source::Batch(slot) => match slot.get() {
                None => "not sent",
                Some(Ok(_)) => "succeeded",
                Some(Err(_)) => "failed",
            },
            Source::Fixed(Ok(_)) => "succeeded",
            Source::Fixed(Err(_)) => "failed",
        };
        f.debug_struct("Pending").field("state", &state).finish()
    }
}

impl<T: Clone> Pending<T> {
    /// Returns a `Pending` with `outcome`, for test doubles of code which
    /// sends batches.
    #[must_use]
    pub fn new(outcome: Result<T, Error>) -> Self {
        Self {
            source: Source::Fixed(outcome),
        }
    }

    /// The command's result, or its error: Centrifugo's error for this
    /// command, the error which failed the whole batch, or
    /// [`Error::BatchNotSent`] before the batch is sent. A broadcast's error
    /// holds every channel's outcome, as [`Client::broadcast`]'s does.
    pub fn result(&self) -> Result<T, Error> {
        match &self.source {
            Source::Fixed(r) => r.clone(),
            Source::Batch(slot) => slot.get().cloned().unwrap_or(Err(Error::BatchNotSent)),
        }
    }
}

/// The outcome of a sent batch as a whole: each command's own result is in
/// the [`Pending`] its method returned. Encoded as JSON, it gives the failed
/// commands with their errors.
#[derive(Debug, Clone, Default, serde::Serialize)]
#[non_exhaustive]
pub struct BatchResult {
    /// Whether Centrifugo carried the batch out and replied to every command,
    /// the failed ones included. A batch which failed as a whole returns its
    /// own error and no result, so a result from the client always has it
    /// set; a test double may set it to stand for either case.
    pub replied: bool,
    /// The commands which failed, with their errors, in the order they were
    /// added.
    pub failed: Vec<FailedCommand>,
}

impl BatchResult {
    /// Returns a batch's outcome, for test doubles.
    #[must_use]
    pub fn new(replied: bool, failed: Vec<FailedCommand>) -> Self {
        Self { replied, failed }
    }
}

/// The failure of one command of a batch.
#[derive(Debug, Clone, serde::Serialize)]
#[non_exhaustive]
pub struct FailedCommand {
    /// The command's position in the batch, from zero.
    pub index: usize,
    /// The command's API method, such as `"publish"`.
    pub method: &'static str,
    /// The error its [`Pending`] returns: an [`Error::Api`], or an
    /// [`Error::BroadcastFailedChannels`] for a broadcast which failed in some
    /// of its channels.
    pub error: Error,
}

impl FailedCommand {
    /// Returns a command's failure, for test doubles.
    #[must_use]
    pub fn new(index: usize, method: &'static str, error: Error) -> Self {
        Self {
            index,
            method,
            error,
        }
    }
}

/// A batch in which one or more commands failed, up to all of them. Every
/// command got its own reply: each one's outcome, success or failure, is in
/// its [`Pending`].
///
/// It does not match its commands' errors: one unknown channel among many
/// commands does not make [`Error::api_code`] return 102. Look at each
/// command's error in [`failed`](Self::failed), or at its `Pending`. To retry,
/// build a new batch, with the same idempotency keys, from the commands whose
/// error is retryable: a batch is sent once.
#[derive(Debug, Clone)]
pub struct BatchFailedCommandsError {
    result: BatchResult,
    total: usize,
}

impl BatchFailedCommandsError {
    /// Returns the error of a batch of `total` commands whose outcome is
    /// `result`, for test doubles.
    #[must_use]
    pub fn new(result: BatchResult, total: usize) -> Self {
        Self { result, total }
    }

    /// The batch's outcome.
    #[must_use]
    pub fn result(&self) -> &BatchResult {
        &self.result
    }

    /// The commands which failed, in the order they were added.
    #[must_use]
    pub fn failed(&self) -> &[FailedCommand] {
        &self.result.failed
    }

    /// How many commands the batch had.
    #[must_use]
    pub fn total(&self) -> usize {
        self.total
    }
}

impl fmt::Display for BatchFailedCommandsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let items = self.failed().iter().map(|c| {
            // A broadcast's message repeats "broadcast: ".
            let msg = c.error.to_string();
            let msg = msg
                .strip_prefix(&format!("{}: ", c.method))
                .map_or(msg.clone(), str::to_owned);
            (format_args!("#{} {}", c.index, c.method).to_string(), msg)
        });
        write_failures(
            f,
            "batch",
            "commands",
            "succeeded",
            self.failed().len(),
            self.total,
            items,
        )
    }
}

impl std::error::Error for BatchFailedCommandsError {}
