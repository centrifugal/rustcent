//! The errors of every call, and [`retryable`].

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

#[cfg(feature = "client")]
use crate::batch::BatchFailedCommandsError;
use crate::broadcast::BroadcastFailedChannelsError;

/// A boxed error, as application functions return them: the endpoint and
/// token functions, HTTP clients, handler functions.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A shared error source: errors are [`Clone`], so that an automatic batch
/// can give its error to every call it carried.
pub(crate) type Source = Arc<dyn std::error::Error + Send + Sync + 'static>;

/// The error of a call. Every call returns one unless it did everything it
/// was asked: `?` is always correct code.
///
/// Match it by kind:
///
/// ```
/// # fn check(err: cent::Error) {
/// match &err {
///     // Centrifugo refused the request: match the code, never the message.
///     cent::Error::Api(e) if e.code == cent::ApiError::UNKNOWN_CHANNEL => {}
///     // Some channels of a broadcast failed; the others got the publication.
///     cent::Error::BroadcastFailedChannels(e) => {
///         for (channel, err) in e.errors() {
///             eprintln!("not published into {channel}: {err}");
///         }
///     }
///     _ if err.is_retryable() => {} // a temporary failure: see `retryable`
///     _ => {}
/// }
/// # }
/// ```
///
/// Centrifugo adds error codes over time: an unknown code is still an
/// [`Error::Api`], so match the codes you handle and treat the rest by kind.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    /// Centrifugo refused the request, or failed to carry it out.
    Api(ApiError),
    /// The request did not reach the API method: an HTTP status other than
    /// 200. A 401 means the credentials were refused: see
    /// [`Error::is_unauthorized`].
    Http(Box<HttpError>),
    /// The reply is not Centrifugo's - usually `api_endpoint` points
    /// elsewhere.
    Decode(DecodeError),
    /// The client could tell the request is wrong, and did not send it.
    InvalidRequest(InvalidRequestError),
    /// The client was not created, or cannot be used here: the message names
    /// the option and the fix.
    InvalidConfig(InvalidConfigError),
    /// A call made after the client was closed.
    Closed,
    /// A [`Pending`](crate::Pending) read before its batch was sent.
    BatchNotSent,
    /// Centrifugo carried out a broadcast, and one or more of its channels
    /// failed, up to all: the others got the publication.
    BroadcastFailedChannels(Box<BroadcastFailedChannelsError>),
    /// Centrifugo carried out a batch, and one or more of its commands
    /// failed, up to all: each command has its own outcome.
    #[cfg(feature = "client")]
    BatchFailedCommands(Box<BatchFailedCommandsError>),
    /// A broadcast with retries stopped by its `on_attempt_error` callback,
    /// or by its deadline while waiting to retry.
    RetryStopped(Box<RetryStoppedError>),
    /// The call's deadline passed: its timeout, or the client's
    /// `request_timeout`. Every timeout is this error.
    Timeout(TimeoutError),
    /// The network failed: a refused or reset connection, a TLS failure.
    Network(NetworkError),
    /// The application's `api_endpoint` or `bearer_token` function failed,
    /// or returned what cannot be sent.
    Callback(CallbackError),
    /// A payload did not encode, or decode as the type asked: from
    /// [`json`](crate::json) and [`PayloadExt::decode`](crate::PayloadExt::decode).
    Payload(PayloadError),
}

/// An error Centrifugo returned: it understood the request, and refused it or
/// failed to carry it out. It is the same whether Centrifugo puts it in a 200
/// reply or, in its transport error mode, in the body of an HTTP error
/// status.
///
/// Compare [`code`](Self::code) with the constants, such as
/// [`ApiError::UNKNOWN_CHANNEL`]: the code is stable, the message is for
/// people.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ApiError {
    /// Identifies the error.
    pub code: u32,
    /// Describes the error for people. Never compare it.
    pub message: String,
    /// Set by Centrifugo (v6.10.0 and newer) when the same request may
    /// succeed if retried later: the error came from a temporary condition,
    /// such as a broker being unavailable or a rate limit, not from the
    /// request. It does not mean nothing was done: a publication may have
    /// happened, so retry it with the same `idempotency_key`.
    pub temporary: bool,
    /// The method of a batch command which only Centrifugo PRO has: Centrifugo
    /// OSS answers it with code 104, where a call of its own gets a 404.
    pro_method: Option<&'static str>,
}

impl ApiError {
    /// Internal error: temporary.
    pub const INTERNAL: u32 = 100;
    /// The channel's namespace is not configured.
    pub const UNKNOWN_CHANNEL: u32 = 102;
    /// Not found. In a batch, also a Centrifugo PRO method sent to
    /// Centrifugo OSS.
    pub const NOT_FOUND: u32 = 104;
    /// Bad request.
    pub const BAD_REQUEST: u32 = 107;
    /// Not available: the feature is off for the channel.
    pub const NOT_AVAILABLE: u32 = 108;
    /// Too many requests: temporary.
    pub const TOO_MANY_REQUESTS: u32 = 111;
    /// Unrecoverable position in a channel's history.
    pub const UNRECOVERABLE_POSITION: u32 = 112;
    /// Conflict.
    pub const CONFLICT: u32 = 113;

    /// Returns an error with `code` and `message`, for test doubles.
    #[must_use]
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            temporary: false,
            pro_method: None,
        }
    }

    /// Sets [`temporary`](Self::temporary).
    #[must_use]
    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    /// The error of a batch command for `method`, with the hint of a PRO
    /// method sent to Centrifugo OSS when it may be that.
    #[cfg(feature = "client")]
    pub(crate) fn in_batch(mut self, method: &'static str) -> Self {
        if self.code == Self::NOT_FOUND && crate::request::is_pro(method) {
            self.pro_method = Some(method);
        }
        self
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = if self.message.is_empty() {
            "server error"
        } else {
            &self.message
        };
        write!(f, "{message} (code {})", self.code)?;
        if let Some(m) = self.pro_method {
            write!(
                f,
                " ({m} is a Centrifugo PRO method: is the server Centrifugo OSS?)"
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiError {}

/// A reply with an HTTP status other than 200 OK: the request did not reach
/// the API method at all.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HttpError {
    /// The HTTP status.
    pub status: http::StatusCode,
    /// The start of the reply's body, for diagnostics.
    pub body: Bytes,
    /// The reply's headers, as received: a request ID a proxy set, or a
    /// `Retry-After`.
    pub headers: http::HeaderMap,
    /// The method of the request when only Centrifugo PRO has it: a 404 for
    /// it most likely means the server is Centrifugo OSS.
    pro_method: Option<&'static str>,
}

impl HttpError {
    /// Returns an error for a reply with `status`, `headers` and `body`, for
    /// test doubles.
    #[must_use]
    pub fn new(status: http::StatusCode, headers: http::HeaderMap, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            body: body.into(),
            headers,
            pro_method: None,
        }
    }

    #[cfg(feature = "client")]
    pub(crate) fn with_pro_method(mut self, method: Option<&'static str>) -> Self {
        self.pro_method = method;
        self
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected HTTP status {}", self.status.as_u16())?;
        let code = self.status.as_u16();
        if self.status.is_redirection() {
            // Centrifugo's API never redirects: something in front of it
            // does, which the client does not follow.
            f.write_str(" (a redirect, which the client does not follow: check api_endpoint, http or https say)")?;
        } else if code == 404 {
            match self.pro_method {
                Some(m) => write!(
                    f,
                    " ({m} is a Centrifugo PRO method: if other methods work, the server is Centrifugo OSS)"
                )?,
                None => f.write_str(
                    " (is api_endpoint the API base URL, such as http://localhost:8000/api?)",
                )?,
            }
        } else if code == 401 {
            f.write_str(" (check the credentials: api_key or bearer_token)")?;
        }
        if !self.body.is_empty() {
            write!(f, ": {}", String::from_utf8_lossy(&self.body))?;
        }
        Ok(())
    }
}

impl std::error::Error for HttpError {}

/// A reply which is not Centrifugo's: usually an `api_endpoint` which does not
/// point at Centrifugo's API, so that something else answers instead.
#[derive(Debug, Clone)]
pub struct DecodeError {
    message: String,
    source: Option<Source>,
}

#[cfg(feature = "client")]
impl DecodeError {
    pub(crate) fn json(err: serde_json::Error) -> Self {
        Self {
            message: err.to_string(),
            source: Some(Arc::new(err)),
        }
    }

    pub(crate) fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decoding the reply: {}", self.message)
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as _)
    }
}

/// A payload which did not encode, or decode as the type asked.
#[derive(Debug, Clone)]
pub struct PayloadError {
    encode: bool,
    source: Option<Source>,
}

impl PayloadError {
    pub(crate) fn encode(err: serde_json::Error) -> Self {
        Self {
            encode: true,
            source: Some(Arc::new(err)),
        }
    }

    pub(crate) fn decode(err: serde_json::Error) -> Self {
        Self {
            encode: false,
            source: Some(Arc::new(err)),
        }
    }

    /// A payload field which is unset.
    pub(crate) fn missing() -> Self {
        Self {
            encode: false,
            source: None,
        }
    }
}

impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.encode {
            "encoding the payload: "
        } else {
            "decoding the payload: "
        })?;
        match &self.source {
            Some(e) => e.fmt(f),
            None => f.write_str("no payload"),
        }
    }
}

impl std::error::Error for PayloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as _)
    }
}

/// A request the client did not send, because it could tell it is wrong: a
/// publication without a channel or data, say. Only rules which hold for
/// every Centrifugo are checked; everything else is Centrifugo's to check.
#[derive(Debug, Clone)]
pub struct InvalidRequestError {
    method: &'static str,
    problem: String,
    /// The position of the command in its batch.
    command: Option<usize>,
}

impl InvalidRequestError {
    pub(crate) fn new(method: &'static str, problem: impl Into<String>) -> Self {
        Self {
            method,
            problem: problem.into(),
            command: None,
        }
    }

    #[cfg(feature = "client")]
    pub(crate) fn in_command(mut self, index: usize) -> Self {
        self.command = Some(index);
        self
    }

    /// The API method of the request, such as `"publish"`.
    #[must_use]
    pub fn method(&self) -> &'static str {
        self.method
    }

    /// The position of the command in its batch, for a batch not sent
    /// because of it.
    #[must_use]
    pub fn command(&self) -> Option<usize> {
        self.command
    }
}

impl fmt::Display for InvalidRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(i) = self.command {
            write!(f, "batch command #{i}: ")?;
        }
        write!(f, "invalid {} request: {}", self.method, self.problem)
    }
}

impl std::error::Error for InvalidRequestError {}

/// A client not created - or used where it cannot work - with a message
/// naming the option and the fix.
#[derive(Debug, Clone)]
pub struct InvalidConfigError(pub(crate) String);

impl fmt::Display for InvalidConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid config: {}", self.0)
    }
}

impl std::error::Error for InvalidConfigError {}

/// A call whose deadline passed. After a timeout Centrifugo may still have
/// carried the request out: retry a publication only with the same
/// `idempotency_key`.
#[derive(Debug, Clone)]
pub struct TimeoutError {
    pub(crate) method: Option<&'static str>,
    /// Set for a call of an automatic batch which never left the queue.
    pub(crate) unsent: bool,
}

impl TimeoutError {
    /// The API method of the call, when the timeout is a call's.
    #[must_use]
    pub fn method(&self) -> Option<&'static str> {
        self.method
    }

    /// Whether the call was never sent: it timed out before its request
    /// left - waiting in an automatic batch's queue, or with its deadline
    /// already passed. Then it surely did not take effect, and a retry
    /// cannot publish twice.
    #[must_use]
    pub fn unsent(&self) -> bool {
        self.unsent
    }
}

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(m) = self.method {
            write!(f, "{m}: ")?;
        }
        f.write_str("timed out")?;
        if self.unsent {
            f.write_str(" before it was sent")?;
        }
        Ok(())
    }
}

impl std::error::Error for TimeoutError {}

/// A failure of the network: connecting, TLS, the connection breaking.
#[derive(Debug, Clone)]
pub struct NetworkError {
    method: &'static str,
    source: Source,
    /// The server's certificate was refused: retrying cannot help.
    certificate: bool,
}

impl NetworkError {
    #[cfg(feature = "client")]
    pub(crate) fn new(method: &'static str, source: Source, certificate: bool) -> Self {
        Self {
            method,
            source,
            certificate,
        }
    }

    /// Whether the server's TLS certificate was refused, which a retry cannot
    /// fix.
    #[must_use]
    pub fn is_certificate(&self) -> bool {
        self.certificate
    }
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.method, self.source)?;
        // Show the chain: hyper's errors say only "client error (Connect)".
        let mut cause = self.source.source();
        while let Some(c) = cause {
            write!(f, ": {c}")?;
            cause = c.source();
        }
        Ok(())
    }
}

impl std::error::Error for NetworkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

/// A failure of the application's `api_endpoint` or `bearer_token` function:
/// its error, or what it returned and the client cannot send.
#[derive(Debug, Clone)]
pub struct CallbackError {
    which: &'static str,
    source: Source,
}

impl CallbackError {
    #[cfg(feature = "client")]
    pub(crate) fn new(which: &'static str, source: Source) -> Self {
        Self { which, source }
    }

    /// The callback: `"api_endpoint"` or `"bearer_token"`.
    #[must_use]
    pub fn which(&self) -> &'static str {
        self.which
    }
}

impl fmt::Display for CallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.which, self.source)
    }
}

impl std::error::Error for CallbackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

/// A broadcast with retries which stopped before its last attempt: its
/// `on_attempt_error` callback returned an error, or the call's deadline
/// came while it waited to retry. [`error`](Self::error) is the broadcast's
/// error at that point - a [`Error::BroadcastFailedChannels`] holding every
/// channel's outcome when some attempt got a reply.
#[derive(Debug, Clone)]
pub struct RetryStoppedError {
    reason: Source,
    error: Error,
}

impl RetryStoppedError {
    #[cfg(feature = "client")]
    pub(crate) fn new(reason: Source, error: Error) -> Self {
        Self { reason, error }
    }

    /// Why the retries stopped: the callback's error, or [`Error::Timeout`].
    #[must_use]
    pub fn reason(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        &*self.reason
    }

    /// The broadcast's error when it stopped.
    #[must_use]
    pub fn error(&self) -> &Error {
        &self.error
    }
}

impl fmt::Display for RetryStoppedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "broadcast retries stopped: {}; {}",
            self.reason, self.error
        )
    }
}

impl std::error::Error for RetryStoppedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl Error {
    #[cfg(feature = "client")]
    pub(crate) fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig(InvalidConfigError(message.into()))
    }

    pub(crate) fn invalid_request(method: &'static str, problem: impl Into<String>) -> Self {
        Self::InvalidRequest(InvalidRequestError::new(method, problem))
    }

    #[cfg(feature = "client")]
    pub(crate) fn timeout(method: &'static str) -> Self {
        Self::Timeout(TimeoutError {
            method: Some(method),
            unsent: false,
        })
    }

    /// The code of a Centrifugo error, compared with the constants of
    /// [`ApiError`]: `err.api_code() == Some(ApiError::UNKNOWN_CHANNEL)`.
    /// `None` for any other error - a broadcast or batch in which some parts
    /// failed included: look at each part's error instead.
    #[must_use]
    pub fn api_code(&self) -> Option<u32> {
        match self {
            Self::Api(e) => Some(e.code),
            _ => None,
        }
    }

    /// Whether Centrifugo refused the credentials: an HTTP 401.
    #[must_use]
    pub fn is_unauthorized(&self) -> bool {
        matches!(self, Self::Http(e) if e.status == http::StatusCode::UNAUTHORIZED)
    }

    /// Whether the call timed out.
    #[must_use]
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout(_))
    }

    /// Whether a new attempt, with time left, may succeed: see [`retryable`].
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        retryable(self)
    }

    /// The result of a broadcast in which some channels failed - also inside
    /// [`Error::RetryStopped`]: every channel's outcome.
    #[must_use]
    pub fn broadcast_result(&self) -> Option<&crate::BroadcastResult> {
        match self {
            Self::BroadcastFailedChannels(e) => Some(e.result()),
            Self::RetryStopped(e) => e.error.broadcast_result(),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(e) => e.fmt(f),
            Self::Http(e) => e.fmt(f),
            Self::Decode(e) => e.fmt(f),
            Self::InvalidRequest(e) => e.fmt(f),
            Self::InvalidConfig(e) => e.fmt(f),
            Self::Closed => f.write_str("client closed"),
            Self::BatchNotSent => f.write_str("batch not sent yet"),
            Self::BroadcastFailedChannels(e) => e.fmt(f),
            #[cfg(feature = "client")]
            Self::BatchFailedCommands(e) => e.fmt(f),
            Self::RetryStopped(e) => e.fmt(f),
            Self::Timeout(e) => e.fmt(f),
            Self::Network(e) => e.fmt(f),
            Self::Callback(e) => e.fmt(f),
            Self::Payload(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(e) => e.source(),
            Self::Payload(e) => e.source(),
            Self::Network(e) => Some(&*e.source),
            Self::Callback(e) => Some(&*e.source),
            Self::RetryStopped(e) => Some(&e.error),
            _ => None,
        }
    }
}

/// An error inside a result - a channel's in a broadcast, a command's in a
/// batch - encodes as `{"code": ..., "message": ...}` for a Centrifugo error,
/// and `{"message": ...}` for any other (spec 3.7).
impl Serialize for Error {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if let Self::Api(e) = self {
            map.serialize_entry("code", &e.code)?;
            map.serialize_entry("message", &e.message)?;
        } else {
            map.serialize_entry("message", &self.to_string())?;
        }
        map.end()
    }
}

/// Reports whether retrying the call which returned `err` may succeed: the
/// error came from a temporary condition, not from the request.
///
/// Retryable are:
///
/// - an [`Error::Api`] Centrifugo marked [`temporary`](ApiError::temporary),
///   and the internal error (code 100) from Centrifugo versions which did not
///   mark errors yet;
/// - a network failure, but a refused TLS certificate, and a timeout - also
///   the call's own, which a retry needs a new deadline for;
/// - an [`Error::Http`] with status 429 or 5xx, such as from a proxy in front
///   of Centrifugo;
/// - a broadcast or batch with at least one failed part which is retryable -
///   also inside [`Error::RetryStopped`].
///
/// Not retryable are the request's own errors - an unknown channel, a bad
/// request, [`Error::InvalidRequest`] - a reply which is not Centrifugo's,
/// refused credentials, [`Error::Closed`], and the errors of the
/// application's `api_endpoint` and `bearer_token` functions, unless they
/// are I/O errors a retry may fix.
///
/// A retryable error does not mean nothing was done: after a timeout, or even
/// an internal error, Centrifugo may have published already. Retry a
/// publication only with the same `idempotency_key`, so that it is not
/// published twice. Retry a broadcast with
/// [`Client::broadcast_with_retry`](crate::Client::broadcast_with_retry),
/// which sends again only the channels which may still succeed.
#[must_use]
pub fn retryable(err: &Error) -> bool {
    match err {
        Error::Api(e) => e.temporary || e.code == ApiError::INTERNAL,
        Error::Http(e) => {
            e.status == http::StatusCode::TOO_MANY_REQUESTS || e.status.is_server_error()
        }
        Error::Timeout(_) => true,
        Error::Network(e) => !e.certificate,
        Error::BroadcastFailedChannels(e) => e.errors().any(|(_, e)| retryable(e)),
        #[cfg(feature = "client")]
        Error::BatchFailedCommands(e) => e.failed().iter().any(|c| retryable(&c.error)),
        Error::RetryStopped(e) => retryable(&e.error),
        Error::Callback(e) => io_retryable(&*e.source),
        Error::Decode(_)
        | Error::InvalidRequest(_)
        | Error::InvalidConfig(_)
        | Error::Payload(_)
        | Error::Closed
        | Error::BatchNotSent => false,
    }
}

/// Whether an error of the application's - or one inside it - is an I/O
/// failure a retry may fix.
fn io_retryable(err: &(dyn std::error::Error + 'static)) -> bool {
    use std::io::ErrorKind::{
        BrokenPipe, ConnectionAborted, ConnectionRefused, ConnectionReset, Interrupted, TimedOut,
        UnexpectedEof,
    };
    for e in chain(err) {
        if let Some(e) = e.downcast_ref::<Error>() {
            return retryable(e);
        }
        let kind = e.downcast_ref::<std::io::Error>().map(std::io::Error::kind);
        if let Some(
            ConnectionRefused | ConnectionReset | ConnectionAborted | BrokenPipe | TimedOut
            | Interrupted | UnexpectedEof,
        ) = kind
        {
            return true;
        }
    }
    false
}

/// The errors of `err`'s chain, from `err` itself: by `source`, and into the
/// error an `io::Error` wraps, which its `source` skips.
pub(crate) fn chain<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'a (dyn std::error::Error + 'static)> {
    std::iter::successors(Some(err), |e| {
        match e
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
        {
            Some(inner) => Some(inner as &(dyn std::error::Error + 'static)),
            None => e.source(),
        }
    })
}
