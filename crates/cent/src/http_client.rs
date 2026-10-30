//! The HTTP client the API client sends requests with: the [`HttpClient`]
//! trait, and [`DefaultHttpClient`].

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tokio::sync::Semaphore;

use crate::{BoxError, Error};

/// A boxed future, as the traits of the crate return them.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Sends the client's HTTP requests: [`DefaultHttpClient`], or one of the
/// application's - to add tracing, to test code using the client without
/// Centrifugo.
///
/// A closure taking an `http::Request<Bytes>` and returning a future of the
/// response is one:
///
/// ```
/// use bytes::Bytes;
///
/// // A fake Centrifugo for tests: every publish succeeds.
/// let fake = |_req: http::Request<Bytes>| async {
///     Ok::<_, cent::BoxError>(http::Response::new(Bytes::from(r#"{"result":{"offset":1}}"#)))
/// };
/// let client = cent::Client::new(cent::Config::new("http://centrifugo/api").http_client(fake))?;
/// # Ok::<(), cent::Error>(())
/// ```
///
/// The client gives each request its whole deadline, which ends the future
/// returned here: an `HttpClient` needs no timeouts of its own. It must not
/// follow redirects, which would send the credentials wherever they point;
/// one which does is left to it. The client passes each request its own
/// headers, credentials included, and never changes the `HttpClient`.
pub trait HttpClient: Send + Sync + 'static {
    /// Sends `request`, and returns the response with its whole body.
    fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> BoxFuture<'static, Result<http::Response<Bytes>, BoxError>>;
}

impl<F, Fut> HttpClient for F
where
    F: Fn(http::Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<http::Response<Bytes>, BoxError>> + Send + 'static,
{
    fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> BoxFuture<'static, Result<http::Response<Bytes>, BoxError>> {
        Box::pin(self(request))
    }
}

/// Connections kept to a host, and requests on them at once.
const MAX_CONNECTIONS: usize = 256;
/// An idle connection is closed after this, before load balancers commonly
/// close theirs, so that a call does not pick one which is being closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(50);
/// The body of an unexpected status is read up to this for a Centrifugo
/// error in it (spec 3.2)...
pub(crate) const MAX_ERROR_BODY: usize = 4 << 10;
/// ... and drained up to this, so that the connection is reused.
const MAX_DRAIN: usize = 64 << 10;

/// The HTTP client a [`Client`](crate::Client) uses unless given another:
/// hyper with rustls, verifying certificates with the operating system's
/// trust store.
///
/// It keeps connections alive, at most 256 to Centrifugo, and keeps all of
/// them while idle: concurrent calls reuse them, and a burst never opens
/// connections only to close them. A call made while all 256 are busy waits
/// for one, within its deadline; for more concurrent calls than that, see
/// [`AutoBatch`](crate::AutoBatch). An idle connection is closed after 50
/// seconds. It speaks HTTP/1.1, and HTTP/2 where TLS negotiates it.
///
/// It sets no timeouts of its own: each call's deadline bounds the whole
/// request, connecting included. It never follows redirects. It does not use
/// the `HTTP_PROXY` and `HTTPS_PROXY` environment variables: a server's
/// calls to Centrifugo are usually internal - give the client an
/// [`HttpClient`] of your own to go through a proxy.
///
/// To add to it - a span per request, say - wrap it in an `HttpClient` of
/// your own which calls its [`send`](HttpClient::send).
#[derive(Clone)]
pub struct DefaultHttpClient {
    client: HyperClient<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
    permits: Arc<Semaphore>,
}

impl fmt::Debug for DefaultHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultHttpClient").finish_non_exhaustive()
    }
}

impl DefaultHttpClient {
    /// Returns a client with a connection pool of its own. It fails when the
    /// operating system's trust store cannot be loaded.
    pub fn new() -> Result<Self, Error> {
        use rustls_platform_verifier::BuilderVerifierExt as _;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .and_then(rustls::ConfigBuilder::with_platform_verifier)
            .map_err(|e| {
                Error::invalid_config(format!("loading the system's TLS trust store: {e}"))
            })?
            .with_no_client_auth();
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(true);
        http.set_keepalive(Some(Duration::from_secs(30)));
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http);
        let client = HyperClient::builder(TokioExecutor::new())
            .pool_idle_timeout(IDLE_TIMEOUT)
            .pool_max_idle_per_host(MAX_CONNECTIONS)
            .pool_timer(TokioTimer::new())
            .timer(TokioTimer::new())
            .build(https);
        Ok(Self {
            client,
            permits: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        })
    }
}

impl HttpClient for DefaultHttpClient {
    fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> BoxFuture<'static, Result<http::Response<Bytes>, BoxError>> {
        let client = self.client.clone();
        let permits = Arc::clone(&self.permits);
        Box::pin(async move {
            // At most MAX_CONNECTIONS requests at once, so at most as many
            // connections: hyper's pool has no cap of its own.
            let _permit = permits.acquire_owned().await?;
            let (parts, body) = request.into_parts();
            let response = client
                .request(http::Request::from_parts(parts, Full::new(body)))
                .await?;
            let (parts, mut body) = response.into_parts();
            let ok = parts.status == http::StatusCode::OK;
            let mut buf = BytesMut::new();
            let mut drained = 0;
            while let Some(frame) = body.frame().await {
                let Ok(data) = frame?.into_data() else {
                    continue;
                };
                if ok {
                    buf.extend_from_slice(&data);
                    continue;
                }
                // An unexpected status: keep the start, for an error in it,
                // and drain the rest up to a bound, so that the connection is
                // reused. Past the bound the body is dropped, which closes
                // the connection.
                let keep = MAX_ERROR_BODY.saturating_sub(buf.len()).min(data.len());
                buf.extend_from_slice(&data[..keep]);
                drained += data.len();
                if drained > MAX_DRAIN {
                    break;
                }
            }
            Ok(http::Response::from_parts(parts, buf.freeze()))
        })
    }
}
