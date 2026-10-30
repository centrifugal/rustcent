//! The backend side of Centrifugo's [HTTP
//! proxy](https://centrifugal.dev/docs/server/proxy), which calls the
//! application when a client connects, subscribes, publishes or makes an RPC
//! call: a [`Handler`] per proxy type, built from a function taking the
//! decoded request and returning a result or a [`Rejection`].
//!
//! ```
//! use cent::proxy::{self, Rejection};
//!
//! let handlers = proxy::Handlers::new().on_error(|parts, err| eprintln!("{}: {err}", parts.uri.path()));
//! let subscribe = handlers.subscribe(|_parts, req| async move {
//!     if !req.channel.starts_with("public:") {
//!         return Err(Rejection::from(proxy::Error::new(403, "permission denied")));
//!     }
//!     Ok(proxy::SubscribeResult::new())
//! });
//! ```
//!
//! Return a result to accept the request, a [`Rejection`] holding an
//! [`Error`] to reject it with an error the client receives, or a
//! [`Disconnect`] to close the client's connection. Any other error - which
//! `?` turns into a `Rejection` - fails the request with HTTP status 500, and
//! goes to `on_error` to be logged.
//!
//! Keep proxy endpoints reachable by Centrifugo only: whoever can call them
//! can claim to be any user.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::BoxError;
pub use crate::generated::proxy::*;
use crate::handler::{Handler, OnError, fail, is_post, json, method_not_allowed};

const DEFAULT_MAX_REQUEST_BYTES: usize = 16 << 20;

impl Error {
    /// Returns an error rejecting a request, with a `code` in `[400, 1999]` -
    /// the range Centrifugo reserves for application errors - and a
    /// `message` the client receives.
    #[must_use]
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            ..Self::default()
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)
    }
}

impl Disconnect {
    /// Returns a disconnect closing a client's connection, with a `code` in
    /// `[4000, 4999]` - codes in `[4500, 4999]` tell the client not to
    /// reconnect - and a `reason` of at most 32 bytes.
    #[must_use]
    pub fn new(code: u32, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
            ..Self::default()
        }
    }
}

impl fmt::Display for Disconnect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "disconnect: {} (code {})", self.reason, self.code)
    }
}

/// Why a proxy function did not accept a request.
#[derive(Debug)]
#[non_exhaustive]
pub enum Rejection {
    /// Rejects the request with an error the client receives: a code in
    /// `[400, 1999]`.
    Error(Error),
    /// Closes the client's connection: a code in `[4000, 4999]`, those in
    /// `[4500, 4999]` telling the client not to reconnect, and a reason of at
    /// most 32 bytes.
    Disconnect(Disconnect),
    /// Fails the request with HTTP status 500, reported to `on_error`.
    /// Centrifugo answers the client with a temporary internal error - and
    /// for refresh and sub refresh asks again a minute later.
    Fail(BoxError),
}

impl Rejection {
    /// A failure: see [`Rejection::Fail`].
    pub fn fail(err: impl Into<BoxError>) -> Self {
        Self::Fail(err.into())
    }
}

impl From<Error> for Rejection {
    fn from(e: Error) -> Self {
        Self::Error(e)
    }
}

impl From<Disconnect> for Rejection {
    fn from(d: Disconnect) -> Self {
        Self::Disconnect(d)
    }
}

/// Any error - a database's, say - fails the request: `?` works in a proxy
/// function. For errors which are not `std::error::Error`s themselves - a
/// `BoxError`, an `anyhow::Error` - use [`Rejection::fail`]. This is why
/// [`Error`] and [`Disconnect`] are plain values, never `std::error::Error`s:
/// `?` on them answers the client, it does not fail the request.
impl<E: std::error::Error + Send + Sync + 'static> From<E> for Rejection {
    fn from(e: E) -> Self {
        Self::Fail(Box::new(e))
    }
}

/// Which answers a proxy type's response carries: those Centrifugo acts on.
#[derive(Clone, Copy, PartialEq)]
enum Answers {
    /// Only a result: refresh and sub refresh end through the result's
    /// `expired`.
    ResultOnly,
    /// A result or an error.
    ErrorOrResult,
    /// A result, an error or a disconnect.
    All,
}

/// Builds a [`Handler`] for each proxy type, all sharing its settings.
#[derive(Clone, Default)]
pub struct Handlers {
    on_error: Option<OnError>,
    max_request_bytes: Option<usize>,
}

impl fmt::Debug for Handlers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handlers")
            .field("max_request_bytes", &self.max_request_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct Answer<'a, R> {
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a R>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a Error>,
    #[serde(skip_serializing_if = "Option::is_none")]
    disconnect: Option<&'a Disconnect>,
}

macro_rules! proxy_handler {
    ($(#[$doc:meta])* $name:ident, $label:literal, $req:ty, $res:ty, $answers:expr) => {
        $(#[$doc])*
        pub fn $name<F, Fut>(&self, f: F) -> Handler
        where
            F: Fn(http::request::Parts, $req) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<$res, Rejection>> + Send + 'static,
        {
            self.serve($label, $answers, f)
        }
    };
}

impl Handlers {
    /// Returns handlers with the default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Tells `f` of every request a handler could not answer as its function
    /// meant: one failed with HTTP status 500 - by a [`Rejection::Fail`], or
    /// an answer the proxy type cannot carry - and a malformed one, rejected
    /// with status 400. Centrifugo only logs the status, so this is where the
    /// cause is seen. It is called before the response is written, and must
    /// not panic.
    #[must_use]
    pub fn on_error(
        mut self,
        f: impl Fn(&http::request::Parts, &BoxError) + Send + Sync + 'static,
    ) -> Self {
        self.on_error = Some(Arc::new(f));
        self
    }

    /// Bounds the body of a request: 16 MiB by default, far above what
    /// Centrifugo's own limits on client messages let through - it only
    /// stops a request which is not Centrifugo's from taking the memory it
    /// likes.
    #[must_use]
    pub fn max_request_bytes(mut self, n: usize) -> Self {
        self.max_request_bytes = Some(n);
        self
    }

    fn serve<Req, Res, F, Fut>(&self, name: &'static str, can: Answers, f: F) -> Handler
    where
        Req: DeserializeOwned + Send + 'static,
        Res: Serialize + Send + 'static,
        F: Fn(http::request::Parts, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Res, Rejection>> + Send + 'static,
    {
        let f = Arc::new(f);
        let on_error = self.on_error.clone();
        let max = self.max_request_bytes.unwrap_or(DEFAULT_MAX_REQUEST_BYTES);
        Handler::new(max, self.on_error.clone(), move |parts, body: Bytes| {
            let f = Arc::clone(&f);
            let on_error = on_error.clone();
            Box::pin(async move {
                if !is_post(&parts.method) {
                    return method_not_allowed("POST");
                }
                let req: Req = match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = format!("malformed {name} request: {e}").into();
                        return fail(&parts, on_error.as_ref(), StatusCode::BAD_REQUEST, &err);
                    }
                };
                let outcome = f(parts.clone(), req).await;
                let encoded = match &outcome {
                    Ok(res) => serde_json::to_vec(&Answer {
                        result: Some(res),
                        error: None,
                        disconnect: None,
                    }),
                    Err(Rejection::Error(e)) if can != Answers::ResultOnly => {
                        serde_json::to_vec(&Answer::<Res> {
                            result: None,
                            error: Some(e),
                            disconnect: None,
                        })
                    }
                    Err(Rejection::Disconnect(d)) if can == Answers::All => {
                        serde_json::to_vec(&Answer::<Res> {
                            result: None,
                            error: None,
                            disconnect: Some(d),
                        })
                    }
                    Err(Rejection::Error(e)) => {
                        let err = format!(
                            "the {name} proxy cannot answer with an error ({e}): failing the request"
                        );
                        return fail(
                            &parts,
                            on_error.as_ref(),
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &err.into(),
                        );
                    }
                    Err(Rejection::Disconnect(d)) => {
                        let err = format!(
                            "the {name} proxy cannot answer with a disconnect ({d}): failing the request"
                        );
                        return fail(
                            &parts,
                            on_error.as_ref(),
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &err.into(),
                        );
                    }
                    Err(Rejection::Fail(e)) => {
                        let err = format!("{name}: {e}").into();
                        return fail(
                            &parts,
                            on_error.as_ref(),
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &err,
                        );
                    }
                };
                match encoded {
                    Ok(body) => json(body),
                    Err(e) => {
                        let err = format!("encoding the {name} response: {e}").into();
                        fail(
                            &parts,
                            on_error.as_ref(),
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &err,
                        )
                    }
                }
            })
        })
    }

    proxy_handler!(
        /// The connect proxy: `f` authenticates a connecting client, and
        /// returns its user and initial state. An [`Error`] rejects the
        /// connection; a [`Disconnect`] closes it.
        connect, "connect", ConnectRequest, ConnectResult, Answers::All
    );

    proxy_handler!(
        /// The refresh proxy: `f` decides whether a connection about to expire
        /// lives on, and until when. To end the connection, return a result
        /// with `expired` set. Any [`Rejection`] fails the request - Centrifugo
        /// ignores an error or a disconnect in a refresh response - and
        /// Centrifugo keeps the connection a minute longer before asking again.
        refresh, "refresh", RefreshRequest, RefreshResult, Answers::ResultOnly
    );

    proxy_handler!(
        /// The subscribe proxy: `f` decides whether a client may subscribe to
        /// a channel. An [`Error`] rejects the subscription; a [`Disconnect`]
        /// closes the connection.
        subscribe, "subscribe", SubscribeRequest, SubscribeResult, Answers::All
    );

    proxy_handler!(
        /// The publish proxy: `f` decides whether a client may publish into a
        /// channel, and may change the publication. An [`Error`] rejects the
        /// publication; a [`Disconnect`] closes the connection.
        publish, "publish", PublishRequest, PublishResult, Answers::All
    );

    proxy_handler!(
        /// The RPC proxy: `f` answers a client's RPC call. An [`Error`] fails
        /// the call; a [`Disconnect`] closes the connection.
        rpc, "rpc", RpcRequest, RpcResult, Answers::All
    );

    proxy_handler!(
        /// The sub refresh proxy: `f` decides whether a subscription about to
        /// expire lives on, and until when. To end the subscription, return a
        /// result with `expired` set. Any [`Rejection`] fails the request, and
        /// Centrifugo keeps the subscription a minute longer before asking
        /// again.
        sub_refresh, "sub refresh", SubRefreshRequest, SubRefreshResult, Answers::ResultOnly
    );

    proxy_handler!(
        /// The map publish proxy: `f` decides whether a client may publish a
        /// key into a map channel, and may change the publication. An
        /// [`Error`] rejects it; a [`Disconnect`] closes the connection.
        map_publish, "map publish", MapPublishRequest, MapPublishResult, Answers::All
    );

    proxy_handler!(
        /// The map remove proxy: `f` decides whether a client may remove a key
        /// from a map channel. An [`Error`] rejects it; a [`Disconnect`]
        /// closes the connection.
        map_remove, "map remove", MapRemoveRequest, MapRemoveResult, Answers::All
    );

    proxy_handler!(
        /// The shared poll refresh proxy: `f` returns the current data of the
        /// items tracked in a shared poll channel. An [`Error`], or any other
        /// rejection, fails the refresh: Centrifugo tries again at the next
        /// interval.
        shared_poll_refresh, "shared poll refresh", SharedPollRefreshRequest, SharedPollRefreshResult, Answers::ErrorOrResult
    );

    proxy_handler!(
        /// The cache empty proxy of Centrifugo PRO: `f` learns that a client
        /// subscribed in cache recovery mode with nothing to recover, and may
        /// publish the current state - then it returns a result with
        /// `populated` set, for Centrifugo to recover once more. Any
        /// [`Rejection`] fails the request, and the client is disconnected to
        /// reconnect.
        notify_cache_empty, "cache empty", NotifyCacheEmptyRequest, NotifyCacheEmptyResult, Answers::ResultOnly
    );

    proxy_handler!(
        /// The channel state proxy of Centrifugo PRO, in preview: `f` receives
        /// channels becoming occupied or vacated. Any [`Rejection`] fails the
        /// request.
        notify_channel_state, "channel state", NotifyChannelStateRequest, NotifyChannelStateResult, Answers::ErrorOrResult
    );
}
