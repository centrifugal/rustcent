//! The API client: [`Config`], [`Client`] and the request path.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};
use std::time::Duration;

use bytes::Bytes;
use http::header::{AUTHORIZATION, CONTENT_TYPE, HOST, USER_AGENT};
use http::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::autobatch::Batcher;
use crate::broadcast::ApiErrorWire;
use crate::error::{CallbackError, DecodeError, HttpError, NetworkError, Source};
use crate::http_client::{BoxFuture, DefaultHttpClient, HttpClient, MAX_ERROR_BODY};
use crate::{ApiError, BoxError, Error, Request};

/// The `User-Agent` of requests: `<sdk>/<major>` (spec 3.2). Before 1.0 the
/// minor version is the major one.
pub(crate) const USER_AGENT_VALUE: &str = "rustcent/0.1";

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_BATCH_SIZE: usize = 1000;
/// The start of an unexpected reply's body kept in an [`HttpError`].
const MAX_ERROR_EXCERPT: usize = 512;

/// Returns Centrifugo's base URL per request: see [`Config::with_endpoint_fn`].
/// A closure returning a future of `Result<String, BoxError>` is one.
pub trait EndpointSource: Send + Sync + 'static {
    /// Returns the base URL of the API, such as `http://10.0.0.7:8000/api`.
    fn endpoint(&self) -> BoxFuture<'_, Result<String, BoxError>>;
}

impl<F, Fut> EndpointSource for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, BoxError>> + Send + 'static,
{
    fn endpoint(&self) -> BoxFuture<'_, Result<String, BoxError>> {
        Box::pin(self())
    }
}

/// Returns the token sent with every request as `Authorization: Bearer`, for
/// Centrifugo PRO's JWKS authentication of the API: see
/// [`Config::bearer_token`]. A closure returning a future of
/// `Result<String, BoxError>` is one, and so is [`token::ApiTokens`].
///
/// [`token::ApiTokens`]: crate::token::ApiTokens
pub trait TokenSource: Send + Sync + 'static {
    /// Returns the token. It is called for every request: return a cached
    /// token, refreshed before it expires.
    fn token(&self) -> BoxFuture<'_, Result<String, BoxError>>;
}

impl<F, Fut> TokenSource for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, BoxError>> + Send + 'static,
{
    fn token(&self) -> BoxFuture<'_, Result<String, BoxError>> {
        Box::pin(self())
    }
}

type OnRequest = dyn Fn(&RequestInfo<'_>) + Send + Sync;

/// Configures a [`Client`]. [`Client::new`] checks it, and reports every
/// mistake it can see before any request is made.
///
/// ```
/// use std::time::Duration;
///
/// let config = cent::Config::new("http://localhost:8000/api")
///     .api_key("secret")
///     .request_timeout(Duration::from_secs(5));
/// let client = cent::Client::new(config)?;
/// # Ok::<(), cent::Error>(())
/// ```
#[derive(Clone)]
pub struct Config {
    endpoint: Endpoint,
    api_key: Option<String>,
    bearer_token: Option<Arc<dyn TokenSource>>,
    http_client: Option<Arc<dyn HttpClient>>,
    request_timeout: Option<Duration>,
    headers: Vec<(String, String)>,
    auto_batch: AutoBatch,
    on_request: Option<Arc<OnRequest>>,
}

#[derive(Clone)]
enum Endpoint {
    Fixed(String),
    Func(Arc<dyn EndpointSource>),
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field(
                "endpoint",
                &match &self.endpoint {
                    Endpoint::Fixed(u) => u.as_str(),
                    Endpoint::Func(_) => "<function>",
                },
            )
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<function>"),
            )
            .field(
                "http_client",
                &self.http_client.as_ref().map(|_| "<custom>"),
            )
            .field("request_timeout", &self.request_timeout)
            .field(
                "headers",
                &self.headers.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            )
            .field("auto_batch", &self.auto_batch)
            .finish_non_exhaustive()
    }
}

impl Config {
    /// Returns a configuration for the API at `api_endpoint`, its base URL,
    /// such as `http://localhost:8000/api`: each method is sent to it plus
    /// its name, `/publish` say.
    #[must_use]
    pub fn new(api_endpoint: impl Into<String>) -> Self {
        Self::with(Endpoint::Fixed(api_endpoint.into()))
    }

    /// Returns a configuration whose base URL `f` returns for each request,
    /// for when Centrifugo's address is discovered rather than fixed. What it
    /// returns is checked as [`Config::new`]'s URL is, per request; an error
    /// fails the call before anything is sent.
    #[must_use]
    pub fn with_endpoint_fn(f: impl EndpointSource) -> Self {
        Self::with(Endpoint::Func(Arc::new(f)))
    }

    fn with(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            api_key: None,
            bearer_token: None,
            http_client: None,
            request_timeout: None,
            headers: Vec::new(),
            auto_batch: AutoBatch::default(),
            on_request: None,
        }
    }

    /// Sends `key` with every request in the `X-API-Key` header. Leave it and
    /// [`bearer_token`](Self::bearer_token) unset when requests are
    /// authenticated some other way (mutual TLS in the HTTP client, a proxy)
    /// or not at all; a missing key shows as an [`Error::Http`] 401 on the
    /// first call.
    #[must_use]
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// Sends the token `source` returns with every request, as
    /// `Authorization: Bearer <token>`, for Centrifugo PRO's JWKS
    /// authentication of the API. It is called for each request, so it should
    /// return a cached token - [`token::ApiTokens`] signs and caches them. An
    /// error from it, or an empty token, fails the call before anything is
    /// sent. It may not be set together with [`api_key`](Self::api_key):
    /// Centrifugo PRO judges a request with a bearer token by the token
    /// alone.
    ///
    /// Centrifugo PRO accepts a token signed with any key of its JWKS
    /// endpoint, and checks audience, issuer and scope only when configured
    /// to: sign API tokens with keys of their own, published at an endpoint
    /// of their own, and have Centrifugo require an audience or scope only
    /// API tokens carry - otherwise any user can call the API with their
    /// connection token.
    ///
    /// [`token::ApiTokens`]: crate::token::ApiTokens
    #[must_use]
    pub fn bearer_token(mut self, source: impl TokenSource) -> Self {
        self.bearer_token = Some(Arc::new(source));
        self
    }

    /// Sends requests with `client` instead of a [`DefaultHttpClient`]: to
    /// add tracing, to go through a proxy, or to test code without
    /// Centrifugo. The client is never changed: credentials and headers go on
    /// each request.
    #[must_use]
    pub fn http_client(mut self, client: impl HttpClient) -> Self {
        self.http_client = Some(Arc::new(client));
        self
    }

    /// Bounds each call without a deadline of its own - see
    /// [`Client::with_timeout`]. 10 seconds by default.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Adds a header sent with every request, for a proxy in front of
    /// Centrifugo say. It may not be `X-API-Key`, `Authorization` or
    /// `Content-Type`. A `User-Agent` replaces the client's own, and a `Host`
    /// is sent instead of the host of the endpoint.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Sends calls made concurrently in batches: see [`AutoBatch`].
    #[must_use]
    pub fn auto_batch(mut self, auto_batch: AutoBatch) -> Self {
        self.auto_batch = auto_batch;
        self
    }

    /// Calls `f` after every HTTP request to the API, with what happened: for
    /// metrics without a dependency of the crate's. A broadcast with retries
    /// calls it for each attempt, an automatic batch once for the batch. The
    /// call waits for it, so it must be quick, and it must not panic: a panic
    /// is the call's, though the request was made.
    #[must_use]
    pub fn on_request(mut self, f: impl Fn(&RequestInfo<'_>) + Send + Sync + 'static) -> Self {
        self.on_request = Some(Arc::new(f));
        self
    }
}

/// Lets the client send concurrent calls together, as one batch request, once
/// enough of them are in flight. Off unless set.
///
/// Nothing waits for a batch to fill. While fewer than `max_in_flight`
/// requests are in flight, every call is sent at once, alone, exactly as
/// without it. Only a call made when `max_in_flight` requests are already in
/// flight waits - for one of them to finish, not for a timer. Then all the
/// calls waiting at that moment go out together as one batch. The busier the
/// client, the larger the batches, and the fewer requests Centrifugo handles
/// for the same work. It is for processes making many calls at once - a
/// server publishing for every request it serves, a worker draining a queue
/// concurrently.
///
/// Every call gets its own command's reply: its own result, or its own error.
/// Calls made one after another keep their order: a call waits for its reply,
/// so the next one can only join a later batch. The calls sharing a batch
/// were made concurrently, with no order between them, and Centrifugo runs
/// them in parallel.
///
/// Calls sharing a batch share its request, though. When the batch request
/// itself fails - the network, the credentials, a body too large for a proxy
/// in front of Centrifugo - every call in it fails with that error. nginx
/// refuses bodies over 1 MB by default (`client_max_body_size`), failing
/// every call in the batch with a 413 none of them would get alone: lower
/// `max_batch_size`, or raise the proxy's limit. Each call keeps its own
/// deadline in a batch: the batch request lasts until the latest deadline of
/// its calls, and at least `request_timeout`. A call which times out before
/// its batch's request ends may still be carried out, as after any timeout.
#[derive(Debug, Clone, Copy, Default)]
pub struct AutoBatch {
    max_in_flight: usize,
    max_batch_size: Option<usize>,
}

impl AutoBatch {
    /// Batches calls once `max_in_flight` requests are in flight. Zero
    /// disables it.
    ///
    /// Choose about half the calls the process makes at once: above the
    /// calls in flight it never batches, and just below it batches calls one
    /// at a time, only limiting them.
    #[must_use]
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            max_in_flight,
            max_batch_size: None,
        }
    }

    /// Caps how many calls one batch carries: 1000 by default.
    #[must_use]
    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = Some(size);
        self
    }
}

/// Describes one HTTP request to the API, for [`Config::on_request`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RequestInfo<'a> {
    /// The API method the request went to: `"publish"`, say, or `"batch"`.
    pub method: &'a str,
    /// How many commands the request carried: 1, or a batch's size.
    pub commands: usize,
    /// How many of them Centrifugo answered with an error. A broadcast which
    /// failed in some channels is not counted: its command was answered,
    /// channel by channel.
    pub failed: usize,
    /// Whether it is a batch [`AutoBatch`] made of concurrent calls.
    pub auto_batch: bool,
    /// How long it took, from before getting its endpoint and credentials to
    /// its reply read.
    pub duration: Duration,
    /// The HTTP status of the reply, 0 without one.
    pub status: u16,
    /// The error of the request as a whole: the network, a timeout, an HTTP
    /// error status, a reply which is not Centrifugo's, or - in Centrifugo's
    /// transport error mode - the error of the request's only command. `None`
    /// when Centrifugo's reply was read: the commands' own outcomes are in
    /// `failed`.
    pub error: Option<&'a Error>,
}

/// Calls Centrifugo's HTTP server API: a method per API method, taking its
/// request and returning its result.
///
/// It is cheap to clone, and safe for concurrent use: create one and share
/// it. It runs on tokio: calls are made from a tokio runtime, the one its
/// first call was made from, whose tasks send its automatic batches and own
/// its connections. A program with several runtimes creates a client per
/// runtime; one without any uses [`blocking::Client`](crate::blocking::Client).
///
/// ```no_run
/// # async fn run() -> Result<(), cent::Error> {
/// let client = cent::Client::new(cent::Config::new("http://localhost:8000/api").api_key("secret"))?;
/// let data = cent::json(&serde_json::json!({"text": "hello"}))?;
/// let res = client.publish(cent::PublishRequest::new("news").data(data)).await?;
/// println!("published at offset {}", res.offset);
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Client {
    pub(crate) shared: Arc<Shared>,
    bound: Option<Bound>,
}

/// The deadline of a client's calls, set by [`Client::with_timeout`] and
/// [`Client::with_deadline`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum Bound {
    Timeout(Duration),
    Deadline(Instant),
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("request_timeout", &self.shared.request_timeout)
            .field("bound", &self.bound)
            .finish_non_exhaustive()
    }
}

pub(crate) struct Shared {
    endpoint: Endpoint,
    token: Option<Arc<dyn TokenSource>>,
    headers: HeaderMap,
    pub(crate) request_timeout: Duration,
    on_request: Option<Arc<OnRequest>>,
    custom_http: Option<Arc<dyn HttpClient>>,
    auto_batch: Option<(usize, usize)>,
    calls: watch::Sender<Calls>,
    engine: RwLock<Arc<Engine>>,
}

/// The calls in progress, so that `close` can wait for them.
#[derive(Debug, Clone, Copy, Default)]
struct Calls {
    closed: bool,
    active: usize,
}

/// What a client owns in one process: its HTTP client and connections, and
/// the runtime its tasks run on. A fork starts a new one (spec 3.1).
pub(crate) struct Engine {
    pid: u32,
    http: Mutex<Option<Arc<dyn HttpClient>>>,
    own_http: bool,
    runtime: OnceLock<RuntimeBinding>,
    auto_batch: Option<(usize, usize)>,
}

struct RuntimeBinding {
    id: tokio::runtime::Id,
    batcher: Option<Arc<Batcher>>,
}

impl Engine {
    fn new(
        shared_http: Option<&Arc<dyn HttpClient>>,
        auto_batch: Option<(usize, usize)>,
    ) -> Result<Self, Error> {
        let (http, own_http) = match shared_http {
            Some(h) => (Arc::clone(h), false),
            None => (
                Arc::new(DefaultHttpClient::new()?) as Arc<dyn HttpClient>,
                true,
            ),
        };
        Ok(Self {
            pid: std::process::id(),
            http: Mutex::new(Some(http)),
            own_http,
            runtime: OnceLock::new(),
            auto_batch,
        })
    }

    pub(crate) fn batcher(&self) -> Option<&Arc<Batcher>> {
        self.runtime.get().and_then(|r| r.batcher.as_ref())
    }

    fn http(&self) -> Result<Arc<dyn HttpClient>, Error> {
        self.http
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or(Error::Closed)
    }
}

/// Counts a call in progress until dropped.
pub(crate) struct CallGuard(Arc<Shared>);

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.0
            .calls
            .send_modify(|c| c.active = c.active.saturating_sub(1));
    }
}

impl Shared {
    /// Counts a call in, unless the client is closed.
    pub(crate) fn enter(self: &Arc<Self>) -> Result<CallGuard, Error> {
        let mut entered = false;
        self.calls.send_if_modified(|c| {
            if c.closed {
                return false;
            }
            c.active += 1;
            entered = true;
            true
        });
        if entered {
            Ok(CallGuard(Arc::clone(self)))
        } else {
            Err(Error::Closed)
        }
    }

    /// Counts in work which goes on for calls already counted - an automatic
    /// batch which outlives its callers - closed or not.
    pub(crate) fn add(self: &Arc<Self>) -> CallGuard {
        self.calls.send_modify(|c| c.active += 1);
        CallGuard(Arc::clone(self))
    }

    pub(crate) fn report(&self, info: &RequestInfo<'_>) {
        if let Some(f) = &self.on_request {
            f(info);
        }
    }
}

impl Client {
    /// Returns a client for `config`, or an [`Error::InvalidConfig`] naming
    /// what is wrong with it.
    pub fn new(config: Config) -> Result<Self, Error> {
        let invalid = |m: String| Err(Error::invalid_config(m));
        if let Endpoint::Fixed(url) = &config.endpoint {
            if let Err(e) = check_endpoint(url) {
                return invalid(format!("api_endpoint: {e}"));
            }
        }
        if config.api_key.is_some() && config.bearer_token.is_some() {
            return invalid(
                "set api_key or bearer_token, not both: a request with a bearer token is judged by the token alone"
                    .into(),
            );
        }
        let mut headers = HeaderMap::new();
        let mut host_set = false;
        for (name, value) in &config.headers {
            let Ok(header) = HeaderName::from_bytes(name.as_bytes()) else {
                return invalid(format!(
                    "header name {name:?} is not a valid HTTP header name"
                ));
            };
            match header.as_str() {
                "x-api-key" => return invalid("header may not set X-API-Key: use api_key".into()),
                "authorization" => {
                    return invalid("header may not set Authorization: use bearer_token".into());
                }
                "content-type" => return invalid("header may not set Content-Type".into()),
                _ => {}
            }
            let Some(v) = header_value(value) else {
                return invalid(format!(
                    "header {name} has a value with a line break or another control character"
                ));
            };
            if header == HOST {
                if host_set || value.is_empty() {
                    return invalid("header Host must have exactly one non-empty value".into());
                }
                host_set = true;
            }
            if header == USER_AGENT {
                headers.insert(USER_AGENT, v);
            } else {
                headers.append(header, v);
            }
        }
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if !headers.contains_key(USER_AGENT) {
            headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));
        }
        if let Some(key) = &config.api_key {
            let Some(v) = header_value(key) else {
                return invalid(
                    "api_key contains a line break or another control character".into(),
                );
            };
            headers.insert(HeaderName::from_static("x-api-key"), v);
        }
        let request_timeout = match config.request_timeout {
            Some(t) if t.is_zero() => {
                return invalid("request_timeout is zero, which times out every call".into());
            }
            Some(t) => t,
            None => DEFAULT_REQUEST_TIMEOUT,
        };
        let ab = config.auto_batch;
        let auto_batch = match (ab.max_in_flight, ab.max_batch_size) {
            (0, None) => None,
            (0, Some(_)) => {
                return invalid(
                    "auto_batch.max_batch_size is set but max_in_flight is zero, which disables it"
                        .into(),
                );
            }
            (_, Some(0 | 1)) => {
                return invalid(
                    "auto_batch.max_batch_size below 2 never batches: use 2 or more".into(),
                );
            }
            (n, size) => Some((n, size.unwrap_or(DEFAULT_MAX_BATCH_SIZE))),
        };
        let engine = Engine::new(config.http_client.as_ref(), auto_batch)?;
        let (calls, _) = watch::channel(Calls::default());
        Ok(Self {
            shared: Arc::new(Shared {
                endpoint: config.endpoint,
                token: config.bearer_token,
                headers,
                request_timeout,
                on_request: config.on_request,
                custom_http: config.http_client,
                auto_batch,
                calls,
                engine: RwLock::new(Arc::new(engine)),
            }),
            bound: None,
        })
    }

    /// Returns a handle to the same client whose calls each time out after
    /// `timeout` instead of the configured `request_timeout`. It is cheap:
    /// make one per call that needs it.
    ///
    /// ```no_run
    /// # async fn run(client: cent::Client) -> Result<(), cent::Error> {
    /// use std::time::Duration;
    /// let info = client.with_timeout(Duration::from_millis(500)).info(cent::InfoRequest::new()).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// A timeout around a call's future - `tokio::time::timeout` - ends it
    /// too, by dropping it; within an automatic batch, a call dropped before
    /// its batch is sent leaves without being sent. The client's own timeout
    /// lets it report the timeout to [`Config::on_request`], and a batch know
    /// how long its calls wait.
    #[must_use]
    pub fn with_timeout(&self, timeout: Duration) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            bound: Some(Bound::Timeout(timeout)),
        }
    }

    /// Returns a handle to the same client whose calls end at `deadline` - for
    /// several calls sharing one, such as the attempts of a retry loop and the
    /// waits between them (see [`wait`](crate::wait)).
    #[must_use]
    pub fn with_deadline(&self, deadline: std::time::Instant) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            bound: Some(Bound::Deadline(Instant::from_std(deadline))),
        }
    }

    pub(crate) fn with_bound(&self, bound: Option<Bound>) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            bound,
        }
    }

    /// The deadline of a call starting now.
    pub(crate) fn deadline(&self) -> Instant {
        match self.bound {
            Some(Bound::Timeout(t)) => after(t),
            Some(Bound::Deadline(d)) => d,
            None => after(self.shared.request_timeout),
        }
    }

    /// Stops the client: calls made from now on return [`Error::Closed`],
    /// and `close` waits for the calls in progress to end - requests on
    /// their way, calls queued for an automatic batch, batches sent for
    /// callers who left. A broadcast being retried stops at its next attempt.
    /// It returns [`Error::Timeout`] when `timeout` passes first; the calls
    /// left go on then. With its default HTTP client the client then closes
    /// its idle connections. It may be called again: it waits as the first
    /// call did.
    ///
    /// Call it when the application shuts down - on SIGTERM - so that what
    /// was accepted is sent before the process exits.
    pub async fn close(&self, timeout: Duration) -> Result<(), Error> {
        self.shared.calls.send_modify(|c| c.closed = true);
        let mut rx = self.shared.calls.subscribe();
        match tokio::time::timeout(timeout, rx.wait_for(|c| c.active == 0)).await {
            Ok(_) => {}
            Err(_) => {
                return Err(Error::Timeout(crate::error::TimeoutError {
                    method: None,
                    unsent: false,
                }));
            }
        }
        let engine = Arc::clone(
            &self
                .shared
                .engine
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if engine.own_http {
            // Dropping the pool closes its idle connections.
            engine
                .http
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
        }
        Ok(())
    }

    /// The engine of this process, bound to the current tokio runtime.
    pub(crate) fn engine(&self) -> Result<Arc<Engine>, Error> {
        let pid = std::process::id();
        let mut engine = Arc::clone(
            &self
                .shared
                .engine
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if engine.pid != pid {
            // Forked: the parent's connections and tasks are not this
            // process's. Start anew, with a pool of its own.
            let mut slot = self
                .shared
                .engine
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            if slot.pid != pid {
                *slot = Arc::new(Engine::new(
                    self.shared.custom_http.as_ref(),
                    self.shared.auto_batch,
                )?);
                // The parent's calls in progress are not this process's.
                self.shared.calls.send_modify(|c| c.active = 0);
            }
            engine = Arc::clone(&slot);
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return Err(Error::invalid_config(
                "cent::Client is used outside a tokio runtime: call it from one, or use cent::blocking::Client",
            ));
        };
        let binding = engine.runtime.get_or_init(|| RuntimeBinding {
            id: handle.id(),
            batcher: engine
                .auto_batch
                .map(|(n, size)| Arc::new(Batcher::new(n, size, handle.clone()))),
        });
        if binding.id != handle.id() {
            return Err(Error::invalid_config(
                "this Client was first used on another tokio runtime, which owns its connections and batches: create a Client per runtime",
            ));
        }
        Ok(engine)
    }

    /// Sends `request` and returns its result: the path of every method.
    pub(crate) async fn call<R: Request, T: DeserializeOwned + Default>(
        &self,
        request: R,
    ) -> Result<T, Error> {
        request.validate()?;
        let _call = self.shared.enter()?;
        let engine = self.engine()?;
        let deadline = self.deadline();
        // No request starts once its deadline has passed.
        if Instant::now() >= deadline {
            return Err(Error::Timeout(crate::error::TimeoutError {
                method: Some(R::METHOD),
                unsent: true,
            }));
        }
        // Encoded once, before anything else: nothing reads the request after.
        let body = serde_json::to_vec(&request)
            .map_err(|e| Error::invalid_request(R::METHOD, format!("cannot be encoded: {e}")))?;
        drop(request);
        match engine.batcher() {
            Some(b) => {
                Arc::clone(b)
                    .call(self, &engine, R::METHOD, R::PRO_ONLY, body, deadline)
                    .await
            }
            None => {
                self.send_alone(&engine, R::METHOD, R::PRO_ONLY, body, deadline)
                    .await
            }
        }
    }

    /// Sends one call as a request of its own, and reports it.
    pub(crate) async fn send_alone<T: DeserializeOwned + Default>(
        &self,
        engine: &Engine,
        method: &'static str,
        pro: bool,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<T, Error> {
        let start = std::time::Instant::now();
        let mut status = 0;
        let reply =
            tokio::time::timeout_at(deadline, self.post(engine, method, pro, body, &mut status))
                .await
                .unwrap_or_else(|_| Err(Error::timeout(method)));
        let (outcome, failed, whole) = match reply {
            Ok(bytes) => match decode_object::<Envelope<T>>(&bytes) {
                Ok(Envelope { error: Some(e), .. }) => (Err(Error::Api(e.into())), 1, false),
                Ok(Envelope { result, .. }) => (Ok(result.unwrap_or_default()), 0, false),
                Err(e) => (Err(Error::Decode(DecodeError::json(e))), 0, true),
            },
            Err(e) => (Err(e), 0, true),
        };
        self.shared.report(&RequestInfo {
            method,
            commands: 1,
            failed,
            auto_batch: false,
            duration: start.elapsed(),
            status,
            error: if whole { outcome.as_ref().err() } else { None },
        });
        outcome
    }

    /// Posts `body` to the method's endpoint, and returns the body of a 200
    /// reply. `status` gets the reply's status.
    pub(crate) async fn post(
        &self,
        engine: &Engine,
        method: &'static str,
        pro: bool,
        body: Vec<u8>,
        status: &mut u16,
    ) -> Result<Bytes, Error> {
        let shared = &self.shared;
        let base = match &shared.endpoint {
            Endpoint::Fixed(url) => url.trim_end_matches('/').to_owned(),
            Endpoint::Func(f) => {
                let url = f
                    .endpoint()
                    .await
                    .map_err(|e| callback("api_endpoint", e.into()))?;
                check_endpoint(&url).map_err(|e| callback("api_endpoint", message(e)))?
            }
        };
        let mut auth = None;
        if let Some(source) = &shared.token {
            let token = source
                .token()
                .await
                .map_err(|e| callback("bearer_token", e.into()))?;
            match header_value(&format!("Bearer {token}")) {
                Some(v) if !token.is_empty() => auth = Some(v),
                _ => {
                    return Err(callback(
                        "bearer_token",
                        message(
                            "returned an empty token, or one with a line break or another control character",
                        ),
                    ));
                }
            }
        }
        let mut request = http::Request::post(format!("{base}/{method}"))
            .body(Bytes::from(body))
            .map_err(|e| callback("api_endpoint", Arc::new(e)))?;
        *request.headers_mut() = shared.headers.clone();
        if let Some(auth) = auth {
            request.headers_mut().insert(AUTHORIZATION, auth);
        }
        let response = engine
            .http()?
            .send(request)
            .await
            .map_err(|e| transport_error(method, e))?;
        *status = response.status().as_u16();
        if response.status() == http::StatusCode::OK {
            return Ok(response.into_body());
        }
        let (parts, body) = response.into_parts();
        let body = body.slice(..body.len().min(MAX_ERROR_BODY));
        if let Some(e) = transport_mode_error(parts.status, &body) {
            return Err(Error::Api(e));
        }
        let excerpt = body.slice(..body.len().min(MAX_ERROR_EXCERPT));
        let excerpt = trim_ascii(&excerpt);
        Err(Error::Http(Box::new(
            HttpError::new(parts.status, parts.headers, body.slice_ref(excerpt))
                .with_pro_method(pro.then_some(method)),
        )))
    }
}

/// The instant `d` from now, saturating far ahead: a timeout of
/// `Duration::MAX` means no timeout, not a panic.
pub(crate) fn after(d: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(d)
        .unwrap_or_else(|| now + Duration::from_secs(100 * 365 * 86_400))
}

fn trim_ascii(b: &[u8]) -> &[u8] {
    let start = b
        .iter()
        .position(|c| !c.is_ascii_whitespace())
        .unwrap_or(b.len());
    let end = b
        .iter()
        .rposition(|c| !c.is_ascii_whitespace())
        .map_or(start, |i| i + 1);
    &b[start..end]
}

/// An error source holding only a message.
fn message(m: impl Into<String>) -> Source {
    Arc::from(BoxError::from(m.into()))
}

fn callback(which: &'static str, source: Source) -> Error {
    Error::Callback(CallbackError::new(which, source))
}

/// The envelope every method replies with.
#[derive(Deserialize)]
struct Envelope<T> {
    #[serde(default)]
    error: Option<ApiErrorWire>,
    #[serde(default = "none")]
    result: Option<T>,
}

fn none<T>() -> Option<T> {
    None
}

/// Decodes the reply to one command of a batch: its error, or its result,
/// which a command without one - an empty result - lacks.
pub(crate) fn decode_reply<T: DeserializeOwned + Default>(
    reply: &RawValue,
    method: &'static str,
) -> Result<T, Error> {
    let fields: std::collections::HashMap<&str, &RawValue> =
        serde_json::from_str(reply.get()).map_err(|e| Error::Decode(DecodeError::json(e)))?;
    if let Some(e) = fields.get("error").filter(|e| e.get() != "null") {
        let e: ApiErrorWire =
            serde_json::from_str(e.get()).map_err(|e| Error::Decode(DecodeError::json(e)))?;
        return Err(Error::Api(ApiError::from(e).in_batch(method)));
    }
    match fields.get(method) {
        Some(r) => {
            decode_object(r.get().as_bytes()).map_err(|e| Error::Decode(DecodeError::json(e)))
        }
        None => Ok(T::default()),
    }
}

/// Decodes a JSON object: serde reads a struct from an array too, and a
/// reply of another shape is not Centrifugo's.
fn decode_object<T: DeserializeOwned>(body: &[u8]) -> Result<T, serde_json::Error> {
    if body.trim_ascii_start().first() != Some(&b'{') {
        // The decoder's own error, saying what it found instead.
        serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(body)?;
    }
    serde_json::from_slice(body)
}

/// Whether a batch reply is an error, for counting failed commands.
pub(crate) fn is_error_reply(reply: &RawValue) -> bool {
    #[derive(Deserialize)]
    struct Peek<'a> {
        #[serde(borrow, default)]
        error: Option<&'a RawValue>,
    }
    serde_json::from_str::<Peek<'_>>(reply.get())
        .is_ok_and(|p| p.error.is_some_and(|e| e.get() != "null"))
}

/// The commands of a batch, encoded: `{"commands":[...],"parallel":true}`.
pub(crate) fn batch_body<'a>(
    commands: impl IntoIterator<Item = &'a [u8]>,
    parallel: bool,
) -> Vec<u8> {
    let mut body = b"{\"commands\":[".to_vec();
    for (i, c) in commands.into_iter().enumerate() {
        if i > 0 {
            body.push(b',');
        }
        body.extend_from_slice(c);
    }
    body.push(b']');
    if parallel {
        body.extend_from_slice(b",\"parallel\":true");
    }
    body.push(b'}');
    body
}

/// A request encoded as a batch command: `{"publish":{...}}`.
pub(crate) fn command_bytes(method: &str, request: &[u8]) -> Vec<u8> {
    let mut c = Vec::with_capacity(method.len() + request.len() + 5);
    c.extend_from_slice(b"{\"");
    c.extend_from_slice(method.as_bytes());
    c.extend_from_slice(b"\":");
    c.extend_from_slice(request);
    c.push(b'}');
    c
}

/// The replies of a batch, one per command.
#[derive(Deserialize)]
pub(crate) struct BatchReplies {
    #[serde(default, deserialize_with = "crate::wire::nullable")]
    pub(crate) replies: Vec<Box<RawValue>>,
}

/// Decodes a batch's replies, which must be one per command.
pub(crate) fn decode_batch(body: &[u8], commands: usize) -> Result<Vec<Box<RawValue>>, Error> {
    let r: BatchReplies = decode_object(body).map_err(|e| Error::Decode(DecodeError::json(e)))?;
    if r.replies.len() != commands {
        return Err(Error::Decode(DecodeError::message(format!(
            "got {} replies for {commands} commands",
            r.replies.len()
        ))));
    }
    Ok(r.replies)
}

/// The Centrifugo error in the body of a reply with an HTTP error status, if
/// it holds one.
///
/// In its transport error mode, Centrifugo replies to a failed call with an
/// HTTP status and the error alone as the body - the same error as in a 200
/// reply. Only the statuses Centrifugo maps its errors to are taken for one:
/// a proxy in front of it may answer with a JSON body of the same shape. A
/// code which is an HTTP error status - `{"code":429}` from a rate limiting
/// gateway - is the gateway's, and stays an `HttpError` (spec 4.2).
fn transport_mode_error(status: http::StatusCode, body: &[u8]) -> Option<ApiError> {
    if !matches!(status.as_u16(), 400 | 404 | 409 | 416 | 429 | 500) {
        return None;
    }
    let e: ApiErrorWire = serde_json::from_slice(body).ok()?;
    (e.code >= 100 && !(400..600).contains(&e.code)).then(|| e.into())
}

/// The error of a request the HTTP client failed: a timeout of its own is a
/// timeout too, so that a caller has one check for "the request timed out"
/// whichever layer's clock ran out.
fn transport_error(method: &'static str, err: BoxError) -> Error {
    let err: Source = Arc::from(err);
    let mut certificate = false;
    for e in crate::error::chain(&*err) {
        if let Some(e) = e.downcast_ref::<Error>() {
            // An HttpClient of the application's - a test double - may
            // return the crate's errors as they are.
            return e.clone();
        }
        if e.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::TimedOut)
        {
            return Error::timeout(method);
        }
        if let Some(rustls::Error::InvalidCertificate(_)) = e.downcast_ref::<rustls::Error>() {
            certificate = true;
        }
    }
    Error::Network(NetworkError::new(method, err, certificate))
}

/// A header value with no control characters other than tab: a line break in
/// particular.
fn header_value(v: &str) -> Option<HeaderValue> {
    if v.bytes().any(|c| (c < b' ' && c != b'\t') || c == 0x7f) {
        return None;
    }
    HeaderValue::from_str(v).ok()
}

/// Checks the base URL of the API, and returns it without a trailing slash:
/// an absolute `http` or `https` URL with a host, no query, fragment or
/// credentials, and not ending with a method's name (spec 3.1).
pub(crate) fn check_endpoint(url: &str) -> Result<String, String> {
    if url.contains('#') {
        return Err(format!("{url:?} must not have a fragment"));
    }
    let uri: http::Uri = url
        .parse()
        .map_err(|e| format!("{url:?} is not a valid URL: {e}"))?;
    match uri.scheme_str() {
        Some("http" | "https") => {}
        _ => {
            return Err(format!(
                "{url:?} must be an absolute http:// or https:// URL"
            ));
        }
    }
    let Some(authority) = uri.authority() else {
        return Err(format!("{url:?} has no host"));
    };
    if authority.host().is_empty() {
        return Err(format!("{url:?} has no host"));
    }
    if authority.as_str().contains('@') || uri.query().is_some() {
        return Err(format!(
            "{url:?} must not have a query or credentials: use api_key"
        ));
    }
    let path = uri.path().trim_end_matches('/');
    if let Some(last) = path.rsplit('/').next() {
        if crate::request::is_method(last) {
            return Err(format!(
                "{url:?} ends with the method {last:?}: api_endpoint is the API base URL, such as http://localhost:8000/api"
            ));
        }
    }
    Ok(url.trim_end_matches('/').to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        assert_eq!(
            check_endpoint("http://localhost:8000/api/").unwrap(),
            "http://localhost:8000/api"
        );
        assert_eq!(
            check_endpoint("https://c.example.com").unwrap(),
            "https://c.example.com"
        );
        for bad in [
            "localhost:8000/api",
            "ftp://h/api",
            "http:///api",
            "http://h/api?x=1",
            "http://h/api#f",
            "http://u:p@h/api",
            "http://h/api/publish",
            "http://h/api/batch/",
            "",
        ] {
            assert!(check_endpoint(bad).is_err(), "{bad} accepted");
        }
    }

    #[test]
    fn transport_mode() {
        let e = transport_mode_error(
            http::StatusCode::BAD_REQUEST,
            br#"{"code":102,"message":"unknown channel"}"#,
        );
        assert_eq!(e.unwrap().code, 102);
        assert!(
            transport_mode_error(http::StatusCode::TOO_MANY_REQUESTS, br#"{"code":429}"#).is_none()
        );
        assert!(
            transport_mode_error(http::StatusCode::SERVICE_UNAVAILABLE, br#"{"code":100}"#)
                .is_none()
        );
        assert!(transport_mode_error(http::StatusCode::INTERNAL_SERVER_ERROR, b"<html>").is_none());
        assert!(transport_mode_error(http::StatusCode::NOT_FOUND, br#"{"code":42}"#).is_none());
    }

    #[test]
    fn user_agent_names_the_major_version() {
        let v = env!("CARGO_PKG_VERSION");
        let mut parts = v.split('.');
        let (major, minor) = (parts.next().unwrap(), parts.next().unwrap());
        let want = if major == "0" {
            format!("rustcent/0.{minor}")
        } else {
            format!("rustcent/{major}")
        };
        assert_eq!(USER_AGENT_VALUE, want);
    }
}
