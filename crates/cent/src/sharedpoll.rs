//! Signatures for [shared poll](https://centrifugal.dev/docs/server/shared_poll)
//! subscriptions, which clients tracking keys of a shared poll channel need
//! from the backend. Shared poll subscriptions are experimental in
//! Centrifugo, and the module follows their changes.
//!
//! [`endpoints::Handlers::shared_poll_signature`](crate::endpoints::Handlers::shared_poll_signature)
//! serves the endpoint a subscription's `getSignature` callback calls.

use std::time::SystemTime;

use aws_lc_rs::{digest, hmac};

use crate::token::{Expiry, TokenError, unix};

/// Returns the signature authorizing `user` to track `keys` in `channel`
/// until `expiry`, as Centrifugo verifies it: `"iat:exp:mac"`, where `iat` is
/// now and `exp` the expiry in Unix seconds, 0 for [`Expiry::Never`] - valid
/// until the secret changes.
///
/// `secret` is Centrifugo's `shared_poll.hmac_secret_key`; `user` is the ID
/// of the connection's user, empty for an anonymous one. The signature covers
/// exactly `keys`, in their order: a client tracking other keys, or the same
/// in another order, is refused.
///
/// ```
/// use std::time::Duration;
/// let sig = cent::sharedpoll::sign(b"secret", "42", "poll:prices", &["AAPL", "GOOG"], cent::Expiry::after(Duration::from_secs(3600)))?;
/// # Ok::<(), cent::token::TokenError>(())
/// ```
pub fn sign(
    secret: &[u8],
    user: &str,
    channel: &str,
    keys: &[impl AsRef<str>],
    expiry: Expiry,
) -> Result<String, TokenError> {
    sign_at(secret, user, channel, keys, expiry, SystemTime::now())
}

/// Signs at `now`.
fn sign_at(
    secret: &[u8],
    user: &str,
    channel: &str,
    keys: &[impl AsRef<str>],
    expiry: Expiry,
    now: SystemTime,
) -> Result<String, TokenError> {
    if secret.is_empty() {
        return Err(TokenError::new("an empty shared poll secret"));
    }
    let iat = unix(now)?;
    let exp = match expiry {
        Expiry::At(t) => unix(t)?,
        Expiry::Never => 0,
    };
    let mut joined = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            joined.push(0);
        }
        joined.extend_from_slice(k.as_ref().as_bytes());
    }
    let keys_hash = hex(digest::digest(&digest::SHA256, &joined).as_ref());
    // NUL-separated, so that a colon in a user or channel cannot move
    // between fields: ("alice", "news:tech") and ("alice:news", "tech") sign
    // differently.
    let message = format!("{iat}\0{exp}\0{user}\0{channel}\0{keys_hash}");
    let mac = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, secret),
        message.as_bytes(),
    );
    Ok(format!("{iat}:{exp}:{}", hex(mac.as_ref())))
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    // Spec 15.1.
    #[test]
    fn vectors() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let exp = Expiry::At(UNIX_EPOCH + Duration::from_secs(1_700_003_600));
        assert_eq!(
            sign_at(
                b"shared_poll_secret",
                "42",
                "poll:prices",
                &["AAPL", "GOOG"],
                exp,
                now
            )
            .unwrap(),
            "1700000000:1700003600:c61dbf52085a6973537e71644fa30a3bbfc29a52d7ce9f1b7618531596ce587d"
        );
        let none: [&str; 0] = [];
        assert_eq!(
            sign_at(
                b"shared_poll_secret",
                "",
                "poll:prices",
                &none,
                Expiry::Never,
                now
            )
            .unwrap(),
            "1700000000:0:92dd64af5612afde06cad03bb63435d951e037cb55a087af05c2a1399abade0e"
        );
    }

    #[test]
    fn refusals() {
        assert!(sign(b"", "1", "c", &["k"], Expiry::Never).is_err());
        assert!(
            sign(
                b"s",
                "1",
                "c",
                &["k"],
                Expiry::At(UNIX_EPOCH - Duration::from_secs(1))
            )
            .is_err()
        );
    }
}
