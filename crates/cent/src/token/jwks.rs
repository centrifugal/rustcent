//! JSON Web Key Sets.

use std::collections::HashSet;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;

use super::key::PublicJwk;
use super::{Key, TokenError};

/// A public key as a JSON Web Key (RFC 7517): the members Centrifugo reads.
#[derive(Serialize)]
struct Jwk<'a> {
    kty: &'a str,
    kid: &'a str,
    /// Centrifugo skips keys without it.
    #[serde(rename = "use")]
    usage: &'a str,
    alg: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    crv: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    e: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    x: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    y: Option<String>,
}

/// Returns the public keys of `keys` as a JSON Web Key Set, which Centrifugo
/// fetches from `client.token.jwks_public_endpoint` - or Centrifugo PRO from
/// `http_api.jwks.endpoint` - to verify tokens by their `kid`. Each key needs
/// an ID, unique in the set - see [`Key::with_key_id`]. HMAC keys have no
/// public key to publish: they are refused, as a set without keys is.
///
/// A set is how keys rotate without restarting Centrifugo:
///
/// 1. publish the new key next to the old one;
/// 2. sign with it only once every instance of the application publishes it,
///    from an activation time set ahead or in a second deploy: Centrifugo
///    fetches the set when a token names a key it does not know, from
///    whichever instance answers, and refuses the token if that one does not
///    publish the key yet;
/// 3. drop the old key once the tokens it signed have expired.
///
/// Never give a key's ID to another key: Centrifugo caches keys by ID.
///
/// Publish the keys of client tokens and of Centrifugo PRO's server API
/// tokens at separate endpoints: a token signed with any key of a set passes
/// Centrifugo's signature check.
pub fn jwks(keys: &[Key]) -> Result<String, TokenError> {
    if keys.is_empty() {
        return Err(TokenError::new("a JWKS without keys"));
    }
    let mut seen = HashSet::new();
    let mut set = Vec::with_capacity(keys.len());
    let enc = |b: &[u8]| Some(URL_SAFE_NO_PAD.encode(b));
    for (i, k) in keys.iter().enumerate() {
        let Some(public) = k.public_jwk() else {
            return Err(TokenError::new(format!(
                "JWKS key #{i} is an HMAC key, which has no public key to publish"
            )));
        };
        let Some(kid) = k.key_id() else {
            return Err(TokenError::new(format!(
                "JWKS key #{i} has no key ID: set one with with_key_id"
            )));
        };
        if !seen.insert(kid) {
            return Err(TokenError::new(format!("JWKS key ID {kid:?} used twice")));
        }
        let mut jwk = Jwk {
            kty: "",
            kid,
            usage: "sig",
            alg: k.algorithm(),
            crv: None,
            n: None,
            e: None,
            x: None,
            y: None,
        };
        match &public {
            PublicJwk::Rsa { n, e } => {
                jwk.kty = "RSA";
                jwk.n = enc(n);
                jwk.e = enc(e);
            }
            PublicJwk::Ec { crv, x, y } => {
                jwk.kty = "EC";
                jwk.crv = Some(crv);
                jwk.x = enc(x);
                jwk.y = enc(y);
            }
            PublicJwk::Okp { x } => {
                jwk.kty = "OKP";
                jwk.crv = Some("Ed25519");
                jwk.x = enc(x);
            }
        }
        set.push(serde_json::to_value(&jwk).map_err(|e| TokenError::new(e.to_string()))?);
    }
    serde_json::to_string(&serde_json::json!({ "keys": set }))
        .map_err(|e| TokenError::new(e.to_string()))
}
