//! A backend serving what Centrifugo and client SDKs call, on axum:
//!
//! - `/centrifugo/connection_token`: the endpoint of a client's `getToken`;
//! - `/centrifugo/subscribe`: Centrifugo's subscribe proxy;
//! - `/centrifugo/jwks`: the public keys Centrifugo verifies tokens with
//!   (`client.token.jwks_public_endpoint`), rotating by activation time.
//!
//! ```sh
//! cargo run --example backend
//! curl -H 'x-user: 42' localhost:3000/centrifugo/connection_token
//! ```
//!
//! Key rotation by activation time: every instance publishes every key -
//! the next one included - and signs with the latest one whose activation
//! time has passed. A new key is added with an activation time ahead, so that
//! every instance publishes it before any signs with it; the old key is
//! dropped once the tokens it signed have expired. Never give a key's ID to
//! another key: Centrifugo caches keys by ID.

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example shows what happened

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cent::Expiry;
use cent::endpoints::{self, Unauthorized};
use cent::proxy::{self, Rejection};
use cent::token::{self, ConnectionClaims, Key};

/// Signing keys, each with the time it starts signing.
struct Keys(Vec<(SystemTime, Key)>);

impl Keys {
    /// The key signing at `now`: the latest activated.
    fn current(&self, now: SystemTime) -> Option<&Key> {
        self.0
            .iter()
            .filter(|(from, _)| *from <= now)
            .max_by_key(|(from, _)| *from)
            .map(|(_, k)| k)
    }

    fn all(&self) -> Vec<Key> {
        self.0.iter().map(|(_, k)| k.clone()).collect()
    }
}

#[tokio::main]
async fn main() -> Result<(), cent::BoxError> {
    // In an application, keys come from a secret store: PEM files read with
    // Key::from_pem. These are fixed seeds, for the example only.
    let now = SystemTime::now();
    let keys = Arc::new(Keys(vec![
        (
            now - Duration::from_secs(86_400),
            Key::ed25519(&[7; 32])?.with_key_id("2026-10-01"),
        ),
        // Published now, signing from tomorrow.
        (
            now + Duration::from_secs(86_400),
            Key::ed25519(&[8; 32])?.with_key_id("2026-10-30"),
        ),
    ]));

    let on_error = |parts: &http::request::Parts, err: &cent::BoxError| {
        eprintln!("{}: {err}", parts.uri.path());
    };
    let tokens = endpoints::Handlers::new().on_error(on_error);
    let k = Arc::clone(&keys);
    let connection_token = tokens.connection_token(move |parts| {
        let keys = Arc::clone(&k);
        async move {
            // The user your authentication found: a header set by a proxy in
            // front, for the example.
            let Some(user) = parts.headers.get("x-user").and_then(|v| v.to_str().ok()) else {
                return Err(Unauthorized.into()); // 403: the client stops asking
            };
            let key = keys.current(SystemTime::now()).ok_or("no key active yet")?;
            let claims = ConnectionClaims::new(user, Expiry::after(Duration::from_secs(3600)));
            Ok(token::connection(key, &claims)?)
        }
    });
    let subscribe = proxy::Handlers::new()
        .on_error(on_error)
        .subscribe(|_, req| async move {
            if req.channel.starts_with("private:") && req.user.is_empty() {
                return Err(Rejection::from(proxy::Error::new(403, "permission denied")));
            }
            Ok(proxy::SubscribeResult::new())
        });
    let jwks = endpoints::jwks(&keys.all())?;

    // With the tower feature a Handler is a tower Service, which reads the
    // body up to its limit: axum's route_service takes it as it is.
    let app = axum::Router::new()
        .route_service("/centrifugo/connection_token", connection_token)
        .route_service("/centrifugo/subscribe", subscribe)
        .route_service("/centrifugo/jwks", jwks);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("listening on http://127.0.0.1:3000");
    axum::serve(listener, app).await?;
    Ok(())
}
