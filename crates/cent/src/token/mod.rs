//! Tokens: the connection and subscription tokens Centrifugo authenticates
//! clients with, the server API tokens of Centrifugo PRO, and JWKS.
//!
//! ```
//! use std::time::Duration;
//! use cent::token::{self, ConnectionClaims, Key};
//!
//! let key = Key::hmac("secret")?; // client.token.hmac_secret_key
//! let jwt = token::connection(&key, &ConnectionClaims::new("42", cent::Expiry::after(Duration::from_secs(3600))))?;
//! # Ok::<(), cent::token::TokenError>(())
//! ```
//!
//! The endpoints the `getToken` callbacks of client SDKs call are in
//! [`endpoints`](crate::endpoints).

mod api;
mod jwks;
mod key;

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;
use serde_json::value::RawValue;

pub use api::{ApiClaims, ApiTokens, api};
pub use jwks::jwks;
pub use key::Key;

use crate::{FilterNode, SubscribeOptionOverride};

/// Why a token, a JWKS or a signature was not made: the message says what is
/// wrong.
#[derive(Debug, Clone)]
pub struct TokenError(String);

impl TokenError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TokenError {}

/// When a token, a connection, a subscription or a signature expires: a
/// time, or never - chosen explicitly, since a token which never expires
/// works forever once leaked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiry {
    /// At this time.
    At(SystemTime),
    /// Never.
    Never,
}

impl Expiry {
    /// Expires `d` from now - or never, for a `d` past what a time can
    /// hold.
    #[must_use]
    pub fn after(d: Duration) -> Self {
        SystemTime::now()
            .checked_add(d)
            .map_or(Self::Never, Self::At)
    }
}

pub(crate) fn unix(t: SystemTime) -> Result<i64, TokenError> {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TokenError::new("a time before 1970"))?
        .as_secs();
    i64::try_from(secs).map_err(|_| TokenError::new("a time too far ahead"))
}

/// The claims of a connection token. See
/// <https://centrifugal.dev/docs/server/authentication>.
///
/// Its expiry is when the token stops being accepted - the `exp` claim. The
/// connection expires then too, unless [`connection_expiry`](Self::connection_expiry)
/// says otherwise: Centrifugo asks the client for a new token, and closes the
/// connection without one.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnectionClaims {
    /// The ID of the user the connection belongs to: the `sub` claim. Empty
    /// for an anonymous connection.
    pub user: String,
    /// Until when the token is accepted: `exp`, left out for [`Expiry::Never`].
    pub expiry: Expiry,
    /// When the connection expires instead of at the token's expiry: the
    /// `expire_at` claim, for a token accepted briefly opening a longer
    /// connection. [`Expiry::Never`] makes the connection never expire, while
    /// the token's expiry still bounds when the token is accepted - a
    /// reconnect presents it again. Most applications leave it unset.
    pub connection_expiry: Option<Expiry>,
    /// The `iat` claim: now when unset.
    pub issued_at: Option<SystemTime>,
    /// The `jti` claim, for Centrifugo PRO's token revocation.
    pub id: String,
    /// The `aud` claim, which Centrifugo checks when configured to.
    pub audience: String,
    /// The `iss` claim, which Centrifugo checks when configured to.
    pub issuer: String,
    /// Information about the connection, as JSON: part of presence data,
    /// join and leave events and the client's publications.
    pub info: Option<Box<RawValue>>,
    /// Binary info, base64-encoded, instead of `info`.
    pub b64info: String,
    /// Server-side channels to subscribe the connection to. Set `channels`
    /// or `subs`, not both: Centrifugo ignores `channels` when `subs` is set.
    pub channels: Vec<String>,
    /// Server-side channels to subscribe the connection to, each with its
    /// options.
    pub subs: BTreeMap<String, SubscribeOptions>,
    /// Custom data attached to the connection, as JSON. Other clients never
    /// see it, but the token carries it, visible to the user owning the
    /// token: put nothing in it that user must not see - the connect proxy's
    /// meta never reaches the client.
    pub meta: Option<Box<RawValue>>,
    /// Capabilities granting the connection operations in channels.
    /// Centrifugo PRO only.
    pub caps: Vec<ChannelsCapability>,
    /// The connection's client labels: immutable, for metrics, targeted
    /// server API calls and CEL expressions. Centrifugo PRO only.
    pub labels: BTreeMap<String, String>,
}

impl ConnectionClaims {
    /// Returns the claims of `user`'s token, accepted until `expiry`.
    #[must_use]
    pub fn new(user: impl Into<String>, expiry: Expiry) -> Self {
        Self {
            user: user.into(),
            expiry,
            connection_expiry: None,
            issued_at: None,
            id: String::new(),
            audience: String::new(),
            issuer: String::new(),
            info: None,
            b64info: String::new(),
            channels: Vec::new(),
            subs: BTreeMap::new(),
            meta: None,
            caps: Vec::new(),
            labels: BTreeMap::new(),
        }
    }

    /// Sets [`connection_expiry`](Self::connection_expiry).
    #[must_use]
    pub fn connection_expiry(mut self, expiry: Expiry) -> Self {
        self.connection_expiry = Some(expiry);
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

    /// Sets [`audience`](Self::audience).
    #[must_use]
    pub fn audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = audience.into();
        self
    }

    /// Sets [`issuer`](Self::issuer).
    #[must_use]
    pub fn issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = issuer.into();
        self
    }

    /// Sets [`info`](Self::info).
    #[must_use]
    pub fn info(mut self, info: Box<RawValue>) -> Self {
        self.info = Some(info);
        self
    }

    /// Sets [`b64info`](Self::b64info).
    #[must_use]
    pub fn b64info(mut self, b64info: impl Into<String>) -> Self {
        self.b64info = b64info.into();
        self
    }

    /// Sets [`channels`](Self::channels).
    #[must_use]
    pub fn channels(mut self, channels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.channels = channels.into_iter().map(Into::into).collect();
        self
    }

    /// Sets [`subs`](Self::subs).
    #[must_use]
    pub fn subs(
        mut self,
        subs: impl IntoIterator<Item = (impl Into<String>, SubscribeOptions)>,
    ) -> Self {
        self.subs = subs.into_iter().map(|(k, v)| (k.into(), v)).collect();
        self
    }

    /// Sets [`meta`](Self::meta).
    #[must_use]
    pub fn meta(mut self, meta: Box<RawValue>) -> Self {
        self.meta = Some(meta);
        self
    }

    /// Sets [`caps`](Self::caps).
    #[must_use]
    pub fn caps(mut self, caps: impl IntoIterator<Item = ChannelsCapability>) -> Self {
        self.caps = caps.into_iter().collect();
        self
    }

    /// Sets [`labels`](Self::labels).
    #[must_use]
    pub fn labels(
        mut self,
        labels: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.labels = labels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self
    }
}

/// The options of a server-side subscription in [`ConnectionClaims::subs`].
#[derive(Debug, Clone, Default, Serialize)]
#[non_exhaustive]
pub struct SubscribeOptions {
    /// Channel-specific information about the connection, as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<Box<RawValue>>,
    /// Binary info, base64-encoded, instead of `info`.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub b64info: String,
    /// Sent to the client with the subscription, as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<RawValue>>,
    /// Binary data, base64-encoded, instead of `data`.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub b64data: String,
    /// Overrides channel options of the namespace for this subscription.
    #[serde(rename = "override", skip_serializing_if = "Option::is_none")]
    pub r#override: Option<SubscribeOptionOverride>,
    /// Makes the subscription receive only publications whose tags match it.
    /// Centrifugo PRO only, which refuses a malformed filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_tags_filter: Option<FilterNode>,
}

impl SubscribeOptions {
    /// Returns options with nothing set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets [`info`](Self::info).
    #[must_use]
    pub fn info(mut self, info: Box<RawValue>) -> Self {
        self.info = Some(info);
        self
    }

    /// Sets [`data`](Self::data).
    #[must_use]
    pub fn data(mut self, data: Box<RawValue>) -> Self {
        self.data = Some(data);
        self
    }

    /// Sets [`override`](Self::override).
    #[must_use]
    pub fn r#override(mut self, o: SubscribeOptionOverride) -> Self {
        self.r#override = Some(o);
        self
    }

    /// Sets [`server_tags_filter`](Self::server_tags_filter).
    #[must_use]
    pub fn server_tags_filter(mut self, filter: FilterNode) -> Self {
        self.server_tags_filter = Some(filter);
        self
    }
}

/// Grants operations in channels: a connection may do an operation in a
/// channel when any of its capabilities both allows the operation and matches
/// the channel. Centrifugo PRO only.
#[derive(Debug, Clone, Default, Serialize)]
#[non_exhaustive]
pub struct ChannelsCapability {
    /// The channels, or channel patterns, it applies to.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub channels: Vec<String>,
    /// The operations it grants: `"sub"` (subscribe), `"pub"` (publish),
    /// `"hst"` (history) and `"prs"` (presence).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
    /// How `channels` are matched: empty for exact names, `"wildcard"` or
    /// `"regex"`.
    #[serde(rename = "match", skip_serializing_if = "String::is_empty")]
    pub r#match: String,
}

impl ChannelsCapability {
    /// Grants `allow` in `channels`, matched exactly.
    #[must_use]
    pub fn new(
        channels: impl IntoIterator<Item = impl Into<String>>,
        allow: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            channels: channels.into_iter().map(Into::into).collect(),
            allow: allow.into_iter().map(Into::into).collect(),
            r#match: String::new(),
        }
    }

    /// Sets [`match`](Self::match).
    #[must_use]
    pub fn r#match(mut self, m: impl Into<String>) -> Self {
        self.r#match = m.into();
        self
    }
}

/// The claims of a subscription token. See
/// <https://centrifugal.dev/docs/server/channel_token_auth>.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SubscriptionClaims {
    /// The ID of the user the connection belongs to: the `sub` claim. It must
    /// be the connection's user.
    pub user: String,
    /// The channel the token allows to subscribe to.
    pub channel: String,
    /// Until when the token is accepted: `exp`, left out for [`Expiry::Never`].
    /// The subscription expires then too, unless
    /// [`subscription_expiry`](Self::subscription_expiry) says otherwise.
    pub expiry: Expiry,
    /// When the subscription expires instead of at the token's expiry: the
    /// `expire_at` claim. [`Expiry::Never`] makes it never expire, while the
    /// token's expiry still bounds when the token is accepted: with a short
    /// one, a token which subscribes once.
    pub subscription_expiry: Option<Expiry>,
    /// The `iat` claim: now when unset.
    pub issued_at: Option<SystemTime>,
    /// The `jti` claim, for Centrifugo PRO's token revocation.
    pub id: String,
    /// The `aud` claim.
    pub audience: String,
    /// The `iss` claim.
    pub issuer: String,
    /// Channel-specific information about the connection, as JSON.
    pub info: Option<Box<RawValue>>,
    /// Binary info, base64-encoded, instead of `info`.
    pub b64info: String,
    /// Sent to the client in the subscribe reply, as JSON.
    pub data: Option<Box<RawValue>>,
    /// Binary data, base64-encoded, instead of `data`.
    pub b64data: String,
    /// Overrides channel options of the namespace for this subscription.
    pub r#override: Option<SubscribeOptionOverride>,
    /// Capabilities granted in the channel besides subscribing: `"pub"`,
    /// `"hst"` and `"prs"`. Centrifugo PRO only.
    pub allow: Vec<String>,
    /// Makes the subscription receive only publications whose tags match it.
    /// Centrifugo PRO only.
    pub server_tags_filter: Option<FilterNode>,
}

impl SubscriptionClaims {
    /// Returns the claims of `user`'s token for `channel`, accepted until
    /// `expiry`.
    #[must_use]
    pub fn new(user: impl Into<String>, channel: impl Into<String>, expiry: Expiry) -> Self {
        Self {
            user: user.into(),
            channel: channel.into(),
            expiry,
            subscription_expiry: None,
            issued_at: None,
            id: String::new(),
            audience: String::new(),
            issuer: String::new(),
            info: None,
            b64info: String::new(),
            data: None,
            b64data: String::new(),
            r#override: None,
            allow: Vec::new(),
            server_tags_filter: None,
        }
    }

    /// Sets [`subscription_expiry`](Self::subscription_expiry).
    #[must_use]
    pub fn subscription_expiry(mut self, expiry: Expiry) -> Self {
        self.subscription_expiry = Some(expiry);
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

    /// Sets [`audience`](Self::audience).
    #[must_use]
    pub fn audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = audience.into();
        self
    }

    /// Sets [`issuer`](Self::issuer).
    #[must_use]
    pub fn issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = issuer.into();
        self
    }

    /// Sets [`info`](Self::info).
    #[must_use]
    pub fn info(mut self, info: Box<RawValue>) -> Self {
        self.info = Some(info);
        self
    }

    /// Sets [`data`](Self::data).
    #[must_use]
    pub fn data(mut self, data: Box<RawValue>) -> Self {
        self.data = Some(data);
        self
    }

    /// Sets [`override`](Self::override).
    #[must_use]
    pub fn r#override(mut self, o: SubscribeOptionOverride) -> Self {
        self.r#override = Some(o);
        self
    }

    /// Sets [`allow`](Self::allow).
    #[must_use]
    pub fn allow(mut self, allow: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.allow = allow.into_iter().map(Into::into).collect();
        self
    }

    /// Sets [`server_tags_filter`](Self::server_tags_filter).
    #[must_use]
    pub fn server_tags_filter(mut self, filter: FilterNode) -> Self {
        self.server_tags_filter = Some(filter);
        self
    }
}

/// The registered claims Centrifugo reads, in the order the spec's vectors
/// write them.
#[derive(Serialize)]
struct Registered<'a> {
    sub: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    exp: Option<i64>,
    iat: i64,
    #[serde(skip_serializing_if = "empty_str")]
    jti: &'a str,
    #[serde(skip_serializing_if = "empty_str")]
    aud: &'a str,
    #[serde(skip_serializing_if = "empty_str")]
    iss: &'a str,
}

impl<'a> Registered<'a> {
    fn new(
        sub: &'a str,
        expiry: Expiry,
        issued_at: Option<SystemTime>,
        jti: &'a str,
        aud: &'a str,
        iss: &'a str,
    ) -> Result<Self, TokenError> {
        Ok(Self {
            sub,
            exp: match expiry {
                Expiry::At(t) => Some(unix(t)?),
                Expiry::Never => None,
            },
            iat: unix(issued_at.unwrap_or_else(SystemTime::now))?,
            jti,
            aud,
            iss,
        })
    }
}

// Emptiness of the borrowed fields of the wire structs, which serde passes by
// reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn empty_str(s: &&str) -> bool {
    s.is_empty()
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn empty_slice<T>(s: &&[T]) -> bool {
    s.is_empty()
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn empty_map<K, V>(m: &&BTreeMap<K, V>) -> bool {
    m.is_empty()
}

/// The `expire_at` claim: left out, zero for never, or a time.
fn expire_at(e: Option<Expiry>) -> Result<Option<i64>, TokenError> {
    Ok(match e {
        None => None,
        Some(Expiry::Never) => Some(0),
        Some(Expiry::At(t)) => Some(unix(t)?),
    })
}

#[derive(Serialize)]
struct ConnectionWire<'a> {
    #[serde(flatten)]
    registered: Registered<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expire_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    info: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "empty_str")]
    b64info: &'a str,
    #[serde(skip_serializing_if = "empty_slice")]
    channels: &'a [String],
    #[serde(skip_serializing_if = "empty_map")]
    subs: &'a BTreeMap<String, SubscribeOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "empty_slice")]
    caps: &'a [ChannelsCapability],
    #[serde(skip_serializing_if = "empty_map")]
    labels: &'a BTreeMap<String, String>,
}

#[derive(Serialize)]
struct SubscriptionWire<'a> {
    #[serde(flatten)]
    registered: Registered<'a>,
    channel: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    expire_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    info: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "empty_str")]
    b64info: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "empty_str")]
    b64data: &'a str,
    #[serde(rename = "override", skip_serializing_if = "Option::is_none")]
    r#override: Option<&'a SubscribeOptionOverride>,
    #[serde(skip_serializing_if = "empty_slice")]
    allow: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    server_tags_filter: Option<&'a FilterNode>,
}

/// Returns a connection token with `c`'s claims, signed with `key`.
pub fn connection(key: &Key, c: &ConnectionClaims) -> Result<String, TokenError> {
    if !c.channels.is_empty() && !c.subs.is_empty() {
        return Err(TokenError::new(
            "set channels or subs, not both: Centrifugo ignores channels when subs is set",
        ));
    }
    sign(
        key,
        &ConnectionWire {
            registered: Registered::new(
                &c.user,
                c.expiry,
                c.issued_at,
                &c.id,
                &c.audience,
                &c.issuer,
            )?,
            expire_at: expire_at(c.connection_expiry)?,
            info: c.info.as_deref(),
            b64info: &c.b64info,
            channels: &c.channels,
            subs: &c.subs,
            meta: c.meta.as_deref(),
            caps: &c.caps,
            labels: &c.labels,
        },
    )
}

/// Returns a subscription token with `c`'s claims, signed with `key`.
pub fn subscription(key: &Key, c: &SubscriptionClaims) -> Result<String, TokenError> {
    if c.channel.is_empty() {
        return Err(TokenError::new("a subscription token without a channel"));
    }
    sign(
        key,
        &SubscriptionWire {
            registered: Registered::new(
                &c.user,
                c.expiry,
                c.issued_at,
                &c.id,
                &c.audience,
                &c.issuer,
            )?,
            channel: &c.channel,
            expire_at: expire_at(c.subscription_expiry)?,
            info: c.info.as_deref(),
            b64info: &c.b64info,
            data: c.data.as_deref(),
            b64data: &c.b64data,
            r#override: c.r#override.as_ref(),
            allow: &c.allow,
            server_tags_filter: c.server_tags_filter.as_ref(),
        },
    )
}

/// Signs `claims` as a JWT: `base64url(header).base64url(claims).base64url(signature)`.
fn sign(key: &Key, claims: &impl Serialize) -> Result<String, TokenError> {
    #[derive(Serialize)]
    struct Header<'a> {
        alg: &'a str,
        typ: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        kid: Option<&'a str>,
    }
    let header = serde_json::to_vec(&Header {
        alg: key.algorithm(),
        typ: "JWT",
        kid: key.key_id(),
    })
    .map_err(|e| TokenError::new(e.to_string()))?;
    let claims = serde_json::to_vec(claims).map_err(|e| TokenError::new(e.to_string()))?;
    let mut signed = URL_SAFE_NO_PAD.encode(header);
    signed.push('.');
    signed.push_str(&URL_SAFE_NO_PAD.encode(claims));
    let signature = key.sign(signed.as_bytes())?;
    signed.push('.');
    signed.push_str(&URL_SAFE_NO_PAD.encode(signature));
    Ok(signed)
}

#[cfg(test)]
mod tests;
