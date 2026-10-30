//! Signing keys.

use std::fmt;
use std::sync::Arc;

use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{self, EcdsaKeyPair, Ed25519KeyPair, KeyPair as _, RsaKeyPair};
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::pem::PemObject as _;

use super::TokenError;

/// A key signing tokens: HMAC, RSA, ECDSA or Ed25519, matching the key
/// Centrifugo verifies with.
///
/// | Key | Algorithm | Centrifugo option |
/// |---|---|---|
/// | [`Key::hmac`] secret | HS256 | `client.token.hmac_secret_key` |
/// | RSA private key | RS256 | `client.token.rsa_public_key` |
/// | ECDSA private key, P-256, P-384 or P-521 | ES256, ES384, ES512 | `client.token.ecdsa_public_key` |
/// | Ed25519 private key | `EdDSA` | `client.token.ed25519_public_key` (Centrifugo v6.10.2+) |
///
/// Any of the asymmetric keys may instead be published at a JWKS endpoint -
/// see [`jwks`](super::jwks) - with a key ID set by
/// [`with_key_id`](Self::with_key_id). Cloning a key is cheap.
#[derive(Clone)]
pub struct Key {
    pub(super) kid: Option<String>,
    inner: Arc<Inner>,
}

enum Inner {
    Hmac(Box<aws_lc_rs::hmac::Key>),
    Rsa(RsaKeyPair),
    Ecdsa(EcdsaKeyPair, &'static str),
    Ed25519(Ed25519KeyPair),
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Key")
            .field("alg", &self.algorithm())
            .field("kid", &self.kid)
            .finish_non_exhaustive()
    }
}

/// The ECDSA curves Centrifugo verifies, and their JWS algorithms.
const CURVES: [(&signature::EcdsaSigningAlgorithm, &str, &str); 3] = [
    (
        &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
        "ES256",
        "P-256",
    ),
    (
        &signature::ECDSA_P384_SHA384_FIXED_SIGNING,
        "ES384",
        "P-384",
    ),
    (
        &signature::ECDSA_P521_SHA512_FIXED_SIGNING,
        "ES512",
        "P-521",
    ),
];

impl Key {
    /// Returns a key signing with HS256 and `secret`, Centrifugo's
    /// `client.token.hmac_secret_key`. An empty secret is refused.
    pub fn hmac(secret: impl AsRef<[u8]>) -> Result<Self, TokenError> {
        let secret = secret.as_ref();
        if secret.is_empty() {
            return Err(TokenError::new("an empty HMAC secret"));
        }
        Ok(Self::of(Inner::Hmac(Box::new(aws_lc_rs::hmac::Key::new(
            aws_lc_rs::hmac::HMAC_SHA256,
            secret,
        )))))
    }

    /// Returns an Ed25519 key from its 32-byte seed - the private key as
    /// RFC 8032 defines it.
    pub fn ed25519(seed: &[u8; 32]) -> Result<Self, TokenError> {
        let pair = Ed25519KeyPair::from_seed_unchecked(seed)
            .map_err(|e| TokenError::new(format!("an Ed25519 seed: {e}")))?;
        Ok(Self::of(Inner::Ed25519(pair)))
    }

    /// Returns a key from a PEM-encoded private key: RSA (PKCS#1 or PKCS#8),
    /// ECDSA on P-256, P-384 or P-521 (SEC1 or PKCS#8), or Ed25519 (PKCS#8).
    /// The algorithm follows from the key. Other curves are refused.
    pub fn from_pem(pem: impl AsRef<[u8]>) -> Result<Self, TokenError> {
        let der = PrivateKeyDer::from_pem_slice(pem.as_ref())
            .map_err(|e| TokenError::new(format!("reading a PEM private key: {e}")))?;
        Self::from_der(&der)
    }

    /// Returns a key from a DER-encoded private key, as [`from_pem`](Self::from_pem).
    pub fn from_der(der: &PrivateKeyDer<'_>) -> Result<Self, TokenError> {
        let unsupported = || {
            TokenError::new(
                "an unsupported private key: want RSA, ECDSA on P-256, P-384 or P-521, or Ed25519",
            )
        };
        let inner = match der {
            PrivateKeyDer::Pkcs1(k) => {
                Inner::Rsa(RsaKeyPair::from_der(k.secret_pkcs1_der()).map_err(|_| unsupported())?)
            }
            PrivateKeyDer::Sec1(k) => CURVES
                .iter()
                .find_map(|(alg, name, _)| {
                    EcdsaKeyPair::from_private_key_der(alg, k.secret_sec1_der())
                        .ok()
                        .map(|p| Inner::Ecdsa(p, name))
                })
                .ok_or_else(unsupported)?,
            PrivateKeyDer::Pkcs8(k) => {
                let der = k.secret_pkcs8_der();
                if let Ok(p) = RsaKeyPair::from_pkcs8(der) {
                    Inner::Rsa(p)
                } else if let Ok(p) = Ed25519KeyPair::from_pkcs8_maybe_unchecked(der) {
                    Inner::Ed25519(p)
                } else {
                    CURVES
                        .iter()
                        .find_map(|(alg, name, _)| {
                            EcdsaKeyPair::from_pkcs8(alg, der)
                                .ok()
                                .map(|p| Inner::Ecdsa(p, name))
                        })
                        .ok_or_else(unsupported)?
                }
            }
            _ => return Err(unsupported()),
        };
        Ok(Self::of(inner))
    }

    fn of(inner: Inner) -> Self {
        Self {
            kid: None,
            inner: Arc::new(inner),
        }
    }

    /// Returns the key with `kid` written as the `kid` header of the tokens it
    /// signs: the ID of its public key in the JWKS Centrifugo verifies with.
    /// Never give the ID of a key to another: Centrifugo caches keys by ID,
    /// and would refuse the other key's tokens until its cache expires.
    #[must_use]
    pub fn with_key_id(mut self, kid: impl Into<String>) -> Self {
        self.kid = Some(kid.into());
        self
    }

    /// The key's ID, if it has one.
    #[must_use]
    pub fn key_id(&self) -> Option<&str> {
        // An empty ID is none: JWKS and API tokens refuse a key without one.
        self.kid.as_deref().filter(|k| !k.is_empty())
    }

    /// The JWS algorithm the key signs with: `"HS256"`, `"RS256"`, `"ES256"`,
    /// `"ES384"`, `"ES512"` or `"EdDSA"`.
    #[must_use]
    pub fn algorithm(&self) -> &'static str {
        match &*self.inner {
            Inner::Hmac(_) => "HS256",
            Inner::Rsa(_) => "RS256",
            Inner::Ecdsa(_, alg) => alg,
            Inner::Ed25519(_) => "EdDSA",
        }
    }

    /// Whether the key is asymmetric: one with a public key to publish.
    pub(crate) fn is_asymmetric(&self) -> bool {
        !matches!(&*self.inner, Inner::Hmac(_))
    }

    pub(crate) fn sign(&self, data: &[u8]) -> Result<Vec<u8>, TokenError> {
        let failed = |_| TokenError::new("signing failed");
        Ok(match &*self.inner {
            Inner::Hmac(k) => aws_lc_rs::hmac::sign(k, data).as_ref().to_vec(),
            Inner::Rsa(k) => {
                let mut sig = vec![0; k.public_modulus_len()];
                k.sign(
                    &signature::RSA_PKCS1_SHA256,
                    &SystemRandom::new(),
                    data,
                    &mut sig,
                )
                .map_err(failed)?;
                sig
            }
            // Fixed signatures: r and s as big-endian halves of the curve's
            // size, as JWS wants them - not ASN.1.
            Inner::Ecdsa(k, _) => k
                .sign(&SystemRandom::new(), data)
                .map_err(failed)?
                .as_ref()
                .to_vec(),
            Inner::Ed25519(k) => k.sign(data).as_ref().to_vec(),
        })
    }

    /// The public key as a JSON Web Key's members, or `None` for HMAC.
    pub(crate) fn public_jwk(&self) -> Option<PublicJwk> {
        Some(match &*self.inner {
            Inner::Hmac(_) => return None,
            Inner::Rsa(k) => PublicJwk::Rsa {
                n: k.public_key()
                    .modulus()
                    .big_endian_without_leading_zero()
                    .to_vec(),
                e: k.public_key()
                    .exponent()
                    .big_endian_without_leading_zero()
                    .to_vec(),
            },
            Inner::Ecdsa(k, alg) => {
                // 0x04, then X and Y at the curve's size.
                let point = k.public_key().as_ref();
                let size = (point.len() - 1) / 2;
                let crv = CURVES.iter().find(|c| c.1 == *alg).map_or("", |c| c.2);
                PublicJwk::Ec {
                    crv,
                    x: point[1..=size].to_vec(),
                    y: point[1 + size..].to_vec(),
                }
            }
            Inner::Ed25519(k) => PublicJwk::Okp {
                x: k.public_key().as_ref().to_vec(),
            },
        })
    }
}

pub(crate) enum PublicJwk {
    Rsa {
        n: Vec<u8>,
        e: Vec<u8>,
    },
    Ec {
        crv: &'static str,
        x: Vec<u8>,
        y: Vec<u8>,
    },
    Okp {
        x: Vec<u8>,
    },
}
