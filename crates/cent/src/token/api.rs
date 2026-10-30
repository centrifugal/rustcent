//! Server API tokens of Centrifugo PRO.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use serde::Serialize;

use super::{Expiry, Key, Registered, TokenError, sign};

/// The claims of a token authenticating calls to the server API of
/// Centrifugo PRO, which verifies it with the keys of `http_api.jwks.endpoint`.
/// See <https://centrifugal.dev/docs/pro/server_api_enhancements>.
///
/// A token passing Centrifugo PRO's checks can call every API method, and
/// Centrifugo checks only what it is configured to: the signature, by any key
/// of the JWKS endpoint, and the audience, issuer and scope only when
/// `http_api.jwks` sets them. So the token must carry a claim no other token
/// carries - above all the connection and subscription tokens, which users
/// hold: otherwise any user can call the API with their connection token.
///
/// - Sign API tokens with keys of their own, published at a JWKS endpoint of
///   their own - never the one of `client.token.jwks_public_endpoint`.
/// - Set an audience, a scope or both, to values only API tokens carry, and
///   configure `http_api.jwks.audience` and `http_api.jwks.scope` to require
///   them. [`api`] refuses claims with neither.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ApiClaims {
    /// The `aud` claim: `http_api.jwks.audience`.
    pub audience: String,
    /// The `scope` claim, its scopes joined by spaces: `http_api.jwks.scope`
    /// is one of them.
    pub scope: Vec<String>,
    /// The `iss` claim: `http_api.jwks.issuer`.
    pub issuer: String,
    /// The `sub` claim: who calls the API, for the application's own logs.
    pub subject: String,
    /// When the token expires: required by [`api`], best minutes ahead.
    /// Centrifugo PRO would accept a token without `exp` for as long as its
    /// key is published, so none is ever signed. [`ApiTokens`] sets it.
    pub expires_at: Option<SystemTime>,
    /// The `iat` claim: now when unset. [`ApiTokens`] sets it.
    pub issued_at: Option<SystemTime>,
    /// The `jti` claim.
    pub id: String,
}

impl ApiClaims {
    /// Returns claims with nothing set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets [`audience`](Self::audience).
    #[must_use]
    pub fn audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = audience.into();
        self
    }

    /// Sets [`scope`](Self::scope).
    #[must_use]
    pub fn scope(mut self, scope: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.scope = scope.into_iter().map(Into::into).collect();
        self
    }

    /// Sets [`issuer`](Self::issuer).
    #[must_use]
    pub fn issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = issuer.into();
        self
    }

    /// Sets [`subject`](Self::subject).
    #[must_use]
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = subject.into();
        self
    }

    /// Sets [`expires_at`](Self::expires_at).
    #[must_use]
    pub fn expires_at(mut self, t: SystemTime) -> Self {
        self.expires_at = Some(t);
        self
    }

    /// Sets [`issued_at`](Self::issued_at).
    #[must_use]
    pub fn issued_at(mut self, t: SystemTime) -> Self {
        self.issued_at = Some(t);
        self
    }

    /// Sets [`id`](Self::id).
    #[must_use]
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }
}

#[derive(Serialize)]
struct ApiWire<'a> {
    #[serde(flatten)]
    registered: Registered<'a>,
    #[serde(skip_serializing_if = "String::is_empty")]
    scope: String,
}

/// Returns a token for the server API of Centrifugo PRO with `c`'s claims,
/// signed with `key` - see [`ApiClaims`] for keeping it apart from client
/// tokens, and [`ApiTokens`] for one sent with every call. Centrifugo PRO
/// finds the key to verify it in its JWKS by the key ID, so `key` must be an
/// asymmetric key with an ID, published by [`jwks`](super::jwks).
pub fn api(key: &Key, c: &ApiClaims) -> Result<String, TokenError> {
    if !key.is_asymmetric() {
        return Err(TokenError::new(
            "an API token needs an RSA, ECDSA or Ed25519 key: Centrifugo PRO verifies it with the public keys of its JWKS endpoint",
        ));
    }
    if key.key_id().is_none() {
        return Err(TokenError::new(
            "an API token needs a key ID, by which Centrifugo PRO finds the key in its JWKS: set one with with_key_id",
        ));
    }
    if c.audience.is_empty() && c.scope.is_empty() {
        return Err(TokenError::new(
            "set an audience or a scope, required by http_api.jwks, so that client tokens signed with the same keys cannot call the server API",
        ));
    }
    let Some(expires_at) = c.expires_at else {
        return Err(TokenError::new(
            "set expires_at: Centrifugo PRO accepts an API token without one for as long as its key is published",
        ));
    };
    if c.scope
        .iter()
        .any(|s| s.is_empty() || s.contains(char::is_whitespace))
    {
        return Err(TokenError::new("a scope is empty or has a space"));
    }
    sign(
        key,
        &ApiWire {
            registered: Registered::new(
                &c.subject,
                Expiry::At(expires_at),
                c.issued_at,
                &c.id,
                &c.audience,
                &c.issuer,
            )?,
            scope: c.scope.join(" "),
        },
    )
}

/// The current key of [`ApiTokens::current_key`].
type CurrentKey = dyn Fn() -> Result<Key, TokenError> + Send + Sync;

/// Issues the tokens a [`Client`](crate::Client) sends to Centrifugo PRO: it
/// is a [`TokenSource`](crate::TokenSource), for
/// [`Config::bearer_token`](crate::Config::bearer_token). It signs a token
/// with [`api`] and reuses it for half its lifetime, so a token sent has at
/// least half of it left. It is safe for concurrent use.
///
/// ```
/// # fn run(api_key: cent::token::Key) -> Result<(), cent::Error> {
/// use cent::token::{ApiClaims, ApiTokens};
///
/// let tokens = ApiTokens::new(
///     api_key, // its own key, not the client tokens' - see ApiClaims
///     ApiClaims::new().audience("centrifugo-api").scope(["centrifugo:api"]),
/// );
/// let client = cent::Client::new(cent::Config::new("https://centrifugo.example.com/api").bearer_token(tokens))?;
/// # Ok(()) }
/// ```
pub struct ApiTokens {
    key: Key,
    current_key: Option<Arc<CurrentKey>>,
    claims: ApiClaims,
    ttl: Duration,
    cache: Mutex<Option<(String, SystemTime)>>,
}

impl fmt::Debug for ApiTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiTokens")
            .field("key", &self.key)
            .field("claims", &self.claims)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl ApiTokens {
    /// Signs tokens with `key` and `claims`, each valid for 5 minutes.
    /// `claims` must leave `expires_at` and `issued_at` unset: the tokens are
    /// issued when needed and expire a TTL later.
    #[must_use]
    pub fn new(key: Key, claims: ApiClaims) -> Self {
        Self {
            key,
            current_key: None,
            claims,
            ttl: Duration::from_secs(300),
            cache: Mutex::new(None),
        }
    }

    /// How long a token is valid: 5 minutes by default.
    #[must_use]
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Signs each new token with the key `f` returns instead: the key active
    /// now, when keys rotate. A key must be published in the JWKS before it
    /// signs: Centrifugo PRO refuses a token whose key it cannot find.
    #[must_use]
    pub fn current_key(
        mut self,
        f: impl Fn() -> Result<Key, TokenError> + Send + Sync + 'static,
    ) -> Self {
        self.current_key = Some(Arc::new(f));
        self
    }

    /// Returns a token valid for at least half the TTL, signing a new one
    /// when the previous one has less left.
    pub fn token(&self) -> Result<String, TokenError> {
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        let now = SystemTime::now();
        if let Some((token, refresh_at)) = &*cache {
            if now < *refresh_at {
                return Ok(token.clone());
            }
        }
        if self.claims.expires_at.is_some() || self.claims.issued_at.is_some() {
            return Err(TokenError::new(
                "ApiTokens sets expires_at and issued_at itself: leave them unset in its claims, and set its ttl",
            ));
        }
        if self.ttl.is_zero() {
            return Err(TokenError::new("ApiTokens with a zero ttl"));
        }
        let key = match &self.current_key {
            Some(f) => f()?,
            None => self.key.clone(),
        };
        let mut claims = self.claims.clone();
        let Some(expires_at) = now.checked_add(self.ttl) else {
            return Err(TokenError::new("ApiTokens with a ttl too far ahead"));
        };
        claims.issued_at = Some(now);
        claims.expires_at = Some(expires_at);
        let token = api(&key, &claims)?;
        *cache = Some((token.clone(), now + self.ttl / 2)); // below expires_at: no overflow
        Ok(token)
    }
}

#[cfg(feature = "client")]
impl crate::TokenSource for ApiTokens {
    fn token(&self) -> crate::BoxFuture<'_, Result<String, crate::BoxError>> {
        let token = ApiTokens::token(self).map_err(Into::into);
        Box::pin(async move { token })
    }
}
