//! The endpoints client SDKs call: what their `getToken` callbacks fetch for
//! connection and subscription tokens, what a shared poll subscription's
//! `getSignature` callback fetches, and the JWKS endpoint Centrifugo fetches
//! public keys from.
//!
//! Each is a [`Handler`] around a function of the application's deciding who
//! gets what. It answers a JSON object, never cached; [`Unauthorized`] - also
//! inside another error - with 403, on which client SDKs stop asking and
//! disconnect or unsubscribe; and any other error with 500, which client SDKs
//! retry.
//!
#![cfg_attr(feature = "token", doc = "```")]
#![cfg_attr(not(feature = "token"), doc = "```ignore")]
//! use std::time::Duration;
//! use cent::endpoints::{Handlers, Unauthorized};
//! use cent::token::{self, ConnectionClaims, Key};
//!
//! let key = Key::hmac("secret")?;
//! let connection_token = Handlers::new().connection_token(move |parts| {
//!     let key = key.clone();
//!     async move {
//!         // The user an authentication middleware put into the extensions.
//!         let Some(user) = parts.extensions.get::<String>() else {
//!             return Err(Unauthorized.into());
//!         };
//!         let claims = ConnectionClaims::new(user, cent::Expiry::after(Duration::from_secs(3600)));
//!         Ok(token::connection(&key, &claims)?)
//!     }
//! });
//! # Ok::<(), cent::token::TokenError>(())
//! ```

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use http::header::CACHE_CONTROL;
use http::{HeaderValue, Method, StatusCode};
use serde::{Deserialize, Serialize};

use crate::BoxError;
use crate::handler::{Handler, OnError, fail, is_post, json, method_not_allowed};

/// Bodies are read up to this: a channel and the keys of a shared poll
/// subscription at most.
const MAX_REQUEST_BYTES: usize = 1 << 20;

/// Returned - or inside the error returned - by an endpoint's function when
/// the user may not have what they ask for: answered with HTTP status 403, on
/// which client SDKs throw `UnauthorizedError` in their `getToken` callbacks,
/// stop asking, and disconnect or unsubscribe.
///
/// It is the endpoints' own, named after client SDKs' `UnauthorizedError`,
/// and distinct from the API client's [`Error::is_unauthorized`](crate::Error::is_unauthorized),
/// which means Centrifugo refused the client's credentials.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unauthorized;

impl fmt::Display for Unauthorized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unauthorized")
    }
}

impl std::error::Error for Unauthorized {}

fn is_unauthorized(err: &BoxError) -> bool {
    crate::error::chain(&**err).any(<dyn std::error::Error>::is::<Unauthorized>)
}

/// Builds the [`Handler`]s of the endpoints client SDKs call, all sharing its
/// settings.
#[derive(Clone, Default)]
pub struct Handlers {
    on_error: Option<OnError>,
}

impl fmt::Debug for Handlers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handlers").finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct TokenResponse<'a> {
    token: &'a str,
}

#[derive(Deserialize)]
struct ChannelRequest {
    #[serde(default)]
    channel: String,
}

#[derive(Deserialize)]
struct SignatureRequest {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    keys: Vec<String>,
}

/// The answer of a [`shared_poll_signature`](Handlers::shared_poll_signature)
/// function: the keys the user may track, and their signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct SignedKeys {
    /// The signature of `keys`, from
    /// [`sharedpoll::sign`](crate::sharedpoll::sign), say.
    pub signature: String,
    /// The keys signed, in the order signed: those asked for which the user
    /// may track. The client stops tracking the others.
    pub keys: Vec<String>,
}

impl SignedKeys {
    /// Returns `keys` with their `signature`.
    #[must_use]
    pub fn new(keys: Vec<String>, signature: impl Into<String>) -> Self {
        Self {
            signature: signature.into(),
            keys,
        }
    }
}

impl Handlers {
    /// Returns handlers with the default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Tells `f` of every request a handler could not answer as its function
    /// meant: one failed with HTTP status 500 - by an error other than
    /// [`Unauthorized`] - and a malformed one, rejected with status 400. It
    /// must not panic.
    #[must_use]
    pub fn on_error(
        mut self,
        f: impl Fn(&http::request::Parts, &BoxError) + Send + Sync + 'static,
    ) -> Self {
        self.on_error = Some(Arc::new(f));
        self
    }

    /// The endpoint the `getToken` callback of a client calls: `f` returns
    /// the connection token of the request's user - from
    /// [`token::connection`](crate::token::connection), say - or
    /// [`Unauthorized`]. It takes `GET` and `POST`, the body ignored, and
    /// answers `{"token": "..."}`.
    pub fn connection_token<F, Fut>(&self, f: F) -> Handler
    where
        F: Fn(http::request::Parts) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, BoxError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let on_error = self.on_error.clone();
        Handler::new(
            MAX_REQUEST_BYTES,
            self.on_error.clone(),
            move |parts, _body| {
                let (f, on_error) = (Arc::clone(&f), on_error.clone());
                Box::pin(async move {
                    if parts.method != Method::GET && parts.method != Method::POST {
                        return method_not_allowed("GET, POST");
                    }
                    let outcome = f(parts.clone()).await;
                    answer(
                        &parts,
                        on_error.as_ref(),
                        outcome.map(|t| encode(&TokenResponse { token: &t })),
                    )
                })
            },
        )
    }

    /// The endpoint the `getToken` callback of a subscription calls: `f`
    /// returns the token of the request's user for `channel` - from
    /// [`token::subscription`](crate::token::subscription), say - or
    /// [`Unauthorized`] when the user may not subscribe. The channel comes
    /// from a `POST`'s JSON body, `{"channel": "..."}`, or from the `channel`
    /// query parameter of a `GET`; a request without one is answered with
    /// 400.
    pub fn subscription_token<F, Fut>(&self, f: F) -> Handler
    where
        F: Fn(http::request::Parts, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, BoxError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let on_error = self.on_error.clone();
        Handler::new(
            MAX_REQUEST_BYTES,
            self.on_error.clone(),
            move |parts, body: Bytes| {
                let (f, on_error) = (Arc::clone(&f), on_error.clone());
                Box::pin(async move {
                    let channel = match parts.method {
                        Method::GET => query_param(parts.uri.query().unwrap_or(""), "channel")
                            .unwrap_or_default(),
                        Method::POST => match serde_json::from_slice::<ChannelRequest>(&body) {
                            Ok(r) => r.channel,
                            Err(e) => {
                                return fail(
                                    &parts,
                                    on_error.as_ref(),
                                    StatusCode::BAD_REQUEST,
                                    &e.into(),
                                );
                            }
                        },
                        _ => return method_not_allowed("GET, POST"),
                    };
                    if channel.is_empty() {
                        let err = "subscription token request without a channel".into();
                        return fail(&parts, on_error.as_ref(), StatusCode::BAD_REQUEST, &err);
                    }
                    let outcome = f(parts.clone(), channel).await;
                    answer(
                        &parts,
                        on_error.as_ref(),
                        outcome.map(|t| encode(&TokenResponse { token: &t })),
                    )
                })
            },
        )
    }

    /// The endpoint a shared poll subscription's `getSignature` callback
    /// calls: it takes a `POST` of `{"channel": ..., "keys": [...]}` and
    /// answers `{"signature": ..., "keys": [...]}`.
    ///
    /// `f` decides which of `keys` the user may track in `channel`, and signs
    /// them - with [`sharedpoll::sign`](crate::sharedpoll::sign), say. It
    /// returns them with the signature as [`SignedKeys`]; the keys it leaves
    /// out are revoked: the client stops tracking them. To end the
    /// subscription instead, it returns [`Unauthorized`]. It may return only
    /// keys it was asked for: a key it was not asked for fails the request
    /// with 500.
    pub fn shared_poll_signature<F, Fut>(&self, f: F) -> Handler
    where
        F: Fn(http::request::Parts, String, Vec<String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SignedKeys, BoxError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let on_error = self.on_error.clone();
        Handler::new(
            MAX_REQUEST_BYTES,
            self.on_error.clone(),
            move |parts, body: Bytes| {
                let (f, on_error) = (Arc::clone(&f), on_error.clone());
                Box::pin(async move {
                    if !is_post(&parts.method) {
                        return method_not_allowed("POST");
                    }
                    let req = match serde_json::from_slice::<SignatureRequest>(&body) {
                        Ok(r) => r,
                        Err(e) => {
                            return fail(
                                &parts,
                                on_error.as_ref(),
                                StatusCode::BAD_REQUEST,
                                &e.into(),
                            );
                        }
                    };
                    if req.channel.is_empty() {
                        let err = "shared poll signature request without a channel".into();
                        return fail(&parts, on_error.as_ref(), StatusCode::BAD_REQUEST, &err);
                    }
                    let asked: std::collections::HashSet<String> =
                        req.keys.iter().cloned().collect();
                    let outcome =
                        f(parts.clone(), req.channel, req.keys)
                            .await
                            .and_then(|signed| {
                                if let Some(k) =
                                    signed.keys.iter().find(|k| !asked.contains(k.as_str()))
                                {
                                    return Err(format!(
                                        "signed key {k:?} the client did not ask for"
                                    )
                                    .into());
                                }
                                Ok(encode(&signed))
                            });
                    answer(&parts, on_error.as_ref(), outcome)
                })
            },
        )
    }
}

/// Serves the JSON Web Key Set of `keys` (see [`token::jwks`](crate::token::jwks))
/// at the URL configured as Centrifugo's `client.token.jwks_public_endpoint`,
/// or Centrifugo PRO's `http_api.jwks.endpoint`. It answers `GET` and `HEAD`,
/// cacheable for five minutes by caches between Centrifugo and the
/// application; Centrifugo keeps the keys for five minutes of its own.
///
/// It fails when it is built from keys `jwks` refuses, not when it is called.
#[cfg(feature = "token")]
pub fn jwks(keys: &[crate::token::Key]) -> Result<Handler, crate::token::TokenError> {
    let set = Bytes::from(crate::token::jwks(keys)?);
    Ok(Handler::new(MAX_REQUEST_BYTES, None, move |parts, _| {
        let set = set.clone();
        Box::pin(async move {
            if parts.method != Method::GET && parts.method != Method::HEAD {
                return method_not_allowed("GET, HEAD");
            }
            let mut r = json(if parts.method == Method::GET {
                set.to_vec()
            } else {
                Vec::new()
            });
            r.headers_mut().insert(
                CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=300"),
            );
            r
        })
    }))
}

fn encode(v: &impl Serialize) -> Vec<u8> {
    serde_json::to_vec(v).expect("strings and string lists encode")
}

/// Answers a function's outcome: its JSON, never cached; 403 for
/// [`Unauthorized`]; 500 for any other error, told to `on_error`.
fn answer(
    parts: &http::request::Parts,
    on_error: Option<&OnError>,
    outcome: Result<Vec<u8>, BoxError>,
) -> http::Response<Bytes> {
    match outcome {
        Ok(body) => {
            let mut r = json(body);
            // A token or signature belongs to one user: no cache may keep it.
            r.headers_mut()
                .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
            r
        }
        Err(e) if is_unauthorized(&e) => crate::handler::plain(StatusCode::FORBIDDEN, "Forbidden"),
        Err(e) => fail(parts, on_error, StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

/// The first value of `name` in a URL query, percent-decoded.
fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (decode(k)? == name).then(|| decode(v)).flatten()
    })
}

/// Decodes `application/x-www-form-urlencoded`: `+` is a space, `%XX` a byte.
fn decode(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        match b {
            b'+' => out.push(b' '),
            b'%' => {
                let hi = char::from(bytes.next()?).to_digit(16)?;
                let lo = char::from(bytes.next()?).to_digit(16)?;
                out.push(u8::try_from(hi * 16 + lo).ok()?);
            }
            _ => out.push(b),
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries() {
        assert_eq!(
            query_param("channel=news", "channel").as_deref(),
            Some("news")
        );
        assert_eq!(
            query_param("a=1&channel=chat%3A1+x", "channel").as_deref(),
            Some("chat:1 x")
        );
        assert_eq!(query_param("channels=x", "channel"), None);
        assert_eq!(query_param("channel=%zz", "channel"), None);
        assert_eq!(query_param("", "channel"), None);
    }
}
