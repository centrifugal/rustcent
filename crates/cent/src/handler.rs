//! The framework-free core of the HTTP handlers: proxy handlers, the
//! endpoints client SDKs call, the JWKS endpoint.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{ALLOW, CONTENT_TYPE};
use http::{HeaderValue, Method, Response, StatusCode};

use crate::BoxError;

/// A boxed future of a handler's response.
type ResponseFuture = Pin<Box<dyn Future<Output = Response<Bytes>> + Send>>;
type Serve = dyn Fn(http::request::Parts, Bytes) -> ResponseFuture + Send + Sync;

/// Told of every request a handler could not answer as its function meant -
/// see the `on_error` of the handlers' builders.
pub(crate) type OnError = Arc<dyn Fn(&http::request::Parts, &BoxError) + Send + Sync>;

/// An HTTP endpoint: a proxy handler, an endpoint client SDKs call, or a JWKS
/// endpoint. It is one function, [`handle`](Self::handle), from a request
/// with its whole body to a response, holding all of the endpoint's
/// behaviour - statuses, headers, body limits - so that it serves from any
/// framework:
///
/// - with the `tower` feature it is a `tower::Service` taking any
///   `http::Request<B>` with a body, for axum (`Router::route_service`),
///   hyper and tower stacks;
/// - from any other framework, build an `http::Request<Bytes>` and turn the
///   response back:
///
/// ```no_run
/// # async fn serve(handler: cent::Handler, req: http::Request<bytes::Bytes>) {
/// let response: http::Response<bytes::Bytes> = handler.handle(req).await;
/// # }
/// ```
///
/// The function a handler calls gets the request's `http::request::Parts`:
/// its headers, URI and extensions - what an axum middleware put there, the
/// authenticated user say. Cloning a handler is cheap.
#[derive(Clone)]
pub struct Handler {
    serve: Arc<Serve>,
    max_body: usize,
    on_error: Option<OnError>,
}

impl fmt::Debug for Handler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handler")
            .field("max_body", &self.max_body)
            .finish_non_exhaustive()
    }
}

impl Handler {
    pub(crate) fn new(
        max_body: usize,
        on_error: Option<OnError>,
        serve: impl Fn(http::request::Parts, Bytes) -> ResponseFuture + Send + Sync + 'static,
    ) -> Self {
        Self {
            serve: Arc::new(serve),
            max_body,
            on_error,
        }
    }

    /// Answers `request`. A body over the handler's limit is answered with
    /// 400, as a malformed request is.
    pub async fn handle(&self, request: http::Request<Bytes>) -> Response<Bytes> {
        let (parts, body) = request.into_parts();
        if body.len() > self.max_body {
            return self.too_large(&parts);
        }
        (self.serve)(parts, body).await
    }

    /// The largest body the handler reads: an adapter reading a body stream
    /// stops past it.
    #[must_use]
    pub fn max_body_bytes(&self) -> usize {
        self.max_body
    }

    fn too_large(&self, parts: &http::request::Parts) -> Response<Bytes> {
        let err: BoxError = format!("request body over {} bytes", self.max_body).into();
        fail(parts, self.on_error.as_ref(), StatusCode::BAD_REQUEST, &err)
    }
}

/// Answers `status` with its reason, after telling `on_error` of `err`.
pub(crate) fn fail(
    parts: &http::request::Parts,
    on_error: Option<&OnError>,
    status: StatusCode,
    err: &BoxError,
) -> Response<Bytes> {
    if let Some(f) = on_error {
        f(parts, err);
    }
    plain(status, status.canonical_reason().unwrap_or(""))
}

/// A plain text response.
pub(crate) fn plain(status: StatusCode, text: &str) -> Response<Bytes> {
    let mut r = Response::new(Bytes::from(format!("{text}\n")));
    *r.status_mut() = status;
    r.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    r
}

/// 405, with the methods allowed.
pub(crate) fn method_not_allowed(allow: &'static str) -> Response<Bytes> {
    let mut r = plain(
        StatusCode::METHOD_NOT_ALLOWED,
        &format!("only {allow} allowed"),
    );
    r.headers_mut()
        .insert(ALLOW, HeaderValue::from_static(allow));
    r
}

/// A JSON response.
pub(crate) fn json(body: Vec<u8>) -> Response<Bytes> {
    let mut r = Response::new(Bytes::from(body));
    r.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    r
}

pub(crate) fn is_post(m: &Method) -> bool {
    m == Method::POST
}

#[cfg(feature = "tower")]
mod tower {
    use std::convert::Infallible;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use http_body_util::{BodyExt, Full, Limited};

    use super::{Handler, ResponseFuture};

    /// A handler is a `tower::Service`: in axum,
    /// `Router::new().route_service("/centrifugo/connect", handler)`. The
    /// body is read up to the handler's limit; past it the request is
    /// answered with 400.
    impl<B> tower_service::Service<http::Request<B>> for Handler
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<crate::BoxError>,
    {
        type Response = http::Response<Full<Bytes>>;
        type Error = Infallible;
        type Future =
            std::pin::Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<B>) -> Self::Future {
            let handler = self.clone();
            Box::pin(async move {
                let (parts, body) = request.into_parts();
                let response = match Limited::new(body, handler.max_body).collect().await {
                    Ok(body) => {
                        let fut: ResponseFuture = (handler.serve)(parts, body.to_bytes());
                        fut.await
                    }
                    Err(e) => {
                        let err: crate::BoxError = e;
                        let status = if err.is::<http_body_util::LengthLimitError>() {
                            return Ok(handler.too_large(&parts).map(Full::new));
                        } else {
                            http::StatusCode::BAD_REQUEST
                        };
                        super::fail(&parts, handler.on_error.as_ref(), status, &err)
                    }
                };
                Ok(response.map(Full::new))
            })
        }
    }
}
