//! The Rust SDK for the [HTTP server
//! API](https://centrifugal.dev/docs/server/server_api) of
//! [Centrifugo](https://centrifugal.dev): every API method, including those of
//! Centrifugo PRO; tokens; proxy handlers and the endpoints client SDKs call.
//!
#![cfg_attr(feature = "client", doc = "```no_run")]
#![cfg_attr(not(feature = "client"), doc = "```ignore")]
//! use cent::{Client, Config, PublishRequest};
//!
//! # async fn run() -> Result<(), cent::Error> {
//! let client = Client::new(Config::new("http://localhost:8000/api").api_key("secret"))?;
//! let data = cent::json(&serde_json::json!({"text": "hello"}))?;
//! let res = client.publish(PublishRequest::new("news").data(data)).await?;
//! println!("published at offset {}", res.offset);
//! # Ok(()) }
//! ```
//!
//! # The client
//!
//! [`Client`] - and [`blocking::Client`], with the `blocking` feature - has a
//! method per API method, taking its request and returning its result, both
//! generated from Centrifugo's `api.proto`. A request's `new` takes the fields
//! Centrifugo requires; a method per field sets the others:
//! `PublishRequest::new("news").data(data).idempotency_key("k")`.
//!
//! - **Payloads** are JSON, as [`RawValue`]: kept exactly - number text
//!   included - in requests and results alike, so a payload from a reply goes
//!   back out as the same JSON value. JSON `null` is `Some` of a null; unset is
//!   `None`. `serde_json` writes NaN and infinities as `null`: check floats
//!   before encoding them. Write integers which may pass 2^53 as JSON
//!   strings: browsers decode JSON numbers as doubles. [`json`] encodes any
//!   serde type as a payload, and [`PayloadExt::decode`] decodes one:
//!   `.data(cent::json(&message)?)`, `publication.data.decode::<Message>()?`.
//! - **One deadline per call**: [`Config::request_timeout`], or
//!   [`Client::with_timeout`] and [`Client::with_deadline`]; every timeout is
//!   [`Error::Timeout`].
//! - **Every call returns an error unless it did everything asked.** A
//!   broadcast or batch in which some parts failed returns
//!   [`Error::BroadcastFailedChannels`] or [`Error::BatchFailedCommands`],
//!   holding every part's outcome - see [`Client::broadcast`] and [`Batch`].
//! - **Retries**: [`retryable`] says whether a new attempt may succeed; retry
//!   a publication only with the same `idempotency_key`, so that it is not
//!   published twice. [`Client::broadcast_with_retry`] retries only the
//!   channels which may still succeed. Centrifugo remembers a key per
//!   channel for five minutes by default, and a second publication under the
//!   same key is not published: a key derived from an entity must name the
//!   event too, `order-42-paid` rather than `order-42`. The memory, Redis and
//!   PostgreSQL brokers keep keys; the NATS broker ignores them.
//! - **Automatic batching**: [`AutoBatch`] sends concurrent calls together
//!   once enough requests are in flight, never waiting for a timer.
//! - **Shutdown**: [`Client::close`] refuses new calls and waits for those in
//!   progress.
//! - **Tests** of code using the client need no Centrifugo: give it an
//!   [`HttpClient`] answering as Centrifugo does - see the README.
//!
//! # Tokens, proxy handlers, endpoints
//!
//! [`token`] signs connection, subscription and Centrifugo PRO server API
//! tokens, and builds JWKS. [`proxy`] answers Centrifugo's HTTP proxy;
//! [`endpoints`] serves what the `getToken` and `getSignature` callbacks of
//! client SDKs call, and JWKS; [`sharedpoll`] signs shared poll
//! subscriptions. Each endpoint is a [`Handler`] - one function from an
//! `http::Request<Bytes>` to a response, served from any framework, and a
//! `tower::Service` with the `tower` feature. [`Command`] writes API calls
//! for Centrifugo's async consumers.
//!
//! # Features
//!
//! | Feature | Default | What |
//! |---|---|---|
//! | `client` | yes | The async [`Client`], on tokio, with hyper and rustls |
//! | `token` | yes | [`token`], [`sharedpoll`], and the JWKS endpoint |
//! | `blocking` | | [`blocking::Client`] |
//! | `tower` | | [`Handler`] as a `tower::Service` |
//!
//! cent implements the [Centrifugo server SDK
//! specification](https://github.com/centrifugal/gocent/blob/master/spec.md),
//! version 0.2.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "client")]
mod autobatch;
#[cfg(feature = "client")]
mod batch;
#[cfg(feature = "blocking")]
pub mod blocking;
mod broadcast;
#[cfg(feature = "client")]
mod client;
mod command;
pub mod endpoints;
mod error;
mod handler;
#[cfg(feature = "client")]
mod http_client;
mod payload;
pub mod proxy;
mod request;
#[cfg(feature = "client")]
mod retry;
#[cfg(feature = "token")]
pub mod sharedpoll;
#[cfg(feature = "token")]
pub mod token;
mod validate;
mod wire;

mod generated {
    #![allow(
        clippy::doc_markdown,
        clippy::struct_excessive_bools,
        clippy::too_many_lines,
        clippy::doc_lazy_continuation
    )]
    pub(crate) mod api;
    pub(crate) mod proxy;
}

pub use generated::api::*;
pub use serde_json::value::RawValue;

#[cfg(feature = "client")]
pub use batch::{
    Batch, BatchFailedCommandsError, BatchOptions, BatchResult, FailedCommand, Pending,
};
pub use broadcast::{BroadcastFailedChannelsError, BroadcastResult, ChannelResult};
#[cfg(feature = "client")]
pub use client::{AutoBatch, Client, Config, EndpointSource, RequestInfo, TokenSource};
pub use command::Command;
pub use error::{
    ApiError, BoxError, CallbackError, DecodeError, Error, HttpError, InvalidConfigError,
    InvalidRequestError, NetworkError, PayloadError, RetryStoppedError, TimeoutError, retryable,
};
pub use handler::Handler;
#[cfg(feature = "client")]
pub use http_client::{BoxFuture, DefaultHttpClient, HttpClient};
pub use payload::{PayloadExt, json};
pub use request::Request;
#[cfg(feature = "client")]
pub use retry::{
    Backoff, BroadcastAttempt, BroadcastRetryOptions, OnAttemptError, Retry, exponential_backoff,
    wait,
};
#[cfg(feature = "token")]
pub use token::Expiry;

// The README's examples compile and run as doctests.
#[cfg(all(doctest, feature = "client", feature = "token"))]
#[doc = include_str!("../../../README.md")]
struct ReadmeDoctests;
