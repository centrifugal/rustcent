//! Against a real Centrifugo, set up as `testdata/centrifugo.json` says:
//! history and presence for channels without a namespace, a `map` and a
//! `poll` namespace, HMAC and Ed25519 client tokens, the unidirectional HTTP
//! stream, the connect and refresh proxies.
//!
//! ```sh
//! docker compose -f testdata/centrifugo-pro.compose.yml up -d --wait   # Centrifugo PRO
//! CENTRIFUGO_TESTS=1 cargo test -p cent --test integration
//! ```
//!
//! Skipped unless `CENTRIFUGO_TESTS` is set. `RUSTCENT_TEST_ADDR` and
//! `RUSTCENT_TEST_API_KEY` default to `http://localhost:8100/api` and
//! `api_key`. The PRO tests skip against Centrifugo OSS, unless
//! `RUSTCENT_TEST_PRO` is set: then a server which is not PRO fails them.
//! The proxy handlers are served on port 3100 and an API JWKS on 3101 of the
//! host, which Centrifugo reaches as `host.docker.internal`.
//!
//! The PRO tests use Centrifugo PRO in sandbox mode, without a license key.
//! Centrifugo PRO is commercial software, under the Centrifugal Labs License
//! Agreement (<https://centrifugal.dev/license>) - not under rustcent's MIT
//! license.

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![cfg(all(feature = "client", feature = "token"))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cent::token::{self, ApiClaims, ApiTokens, ConnectionClaims, Key};
use cent::*;
use http_body_util::{BodyExt, Full};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

fn enabled() -> bool {
    std::env::var_os("CENTRIFUGO_TESTS").is_some()
}

macro_rules! skip_unless_enabled {
    () => {
        if !enabled() {
            eprintln!("skipped: set CENTRIFUGO_TESTS=1 with Centrifugo running");
            return;
        }
    };
}

fn addr() -> String {
    std::env::var("RUSTCENT_TEST_ADDR").unwrap_or_else(|_| "http://localhost:8100/api".into())
}

fn config() -> Config {
    Config::new(addr())
        .api_key(std::env::var("RUSTCENT_TEST_API_KEY").unwrap_or_else(|_| "api_key".into()))
}

async fn client() -> Client {
    let c = Client::new(config()).unwrap();
    if let Err(e) = c.info(InfoRequest::new()).await {
        panic!("Centrifugo not reachable at {}: {e}", addr());
    }
    c
}

/// The client when the server is Centrifugo PRO; `None` to skip otherwise.
async fn pro() -> Option<Client> {
    let c = client().await;
    match c
        .connections(ConnectionsRequest::new().user("nobody"))
        .await
    {
        Ok(_) => Some(c),
        Err(e) if std::env::var_os("RUSTCENT_TEST_PRO").is_some() => {
            panic!("RUSTCENT_TEST_PRO is set, but the server is not Centrifugo PRO: {e}")
        }
        Err(_) => None,
    }
}

static N: AtomicUsize = AtomicUsize::new(0);

fn unique(prefix: &str) -> String {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{prefix}rustcent-{t:x}-{}",
        N.fetch_add(1, Ordering::SeqCst)
    )
}

fn raw(v: &Value) -> Box<RawValue> {
    serde_json::value::to_raw_value(v).unwrap()
}

fn now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
}

/// A client connection over Centrifugo's unidirectional HTTP stream: one
/// JSON message per line.
struct Stream {
    rx: mpsc::UnboundedReceiver<Value>,
    task: tokio::task::JoinHandle<()>,
}

impl Stream {
    async fn open(body: Value) -> Self {
        let url = format!(
            "{}/connection/uni_http_stream",
            addr().trim_end_matches("/api")
        );
        let http =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build_http::<Full<Bytes>>();
        let req = http::Request::post(url)
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let Ok(resp) = http.request(req).await else {
                return;
            };
            let mut body = resp.into_body();
            let mut buf = Vec::new();
            while let Some(Ok(frame)) = body.frame().await {
                let Ok(data) = frame.into_data() else {
                    continue;
                };
                buf.extend_from_slice(&data);
                while let Some(i) = buf.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=i).collect();
                    let line = String::from_utf8_lossy(&line);
                    let line = line.trim();
                    if !line.is_empty() && line != "{}" {
                        let _ = tx.send(serde_json::from_str(line).unwrap_or(Value::Null));
                    }
                }
            }
        });
        Self { rx, task }
    }

    /// The next message, `None` once the connection is closed; pings skipped.
    async fn next(&mut self) -> Option<Value> {
        tokio::time::timeout(Duration::from_secs(10), self.rx.recv())
            .await
            .expect("a message in 10 seconds")
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serves `routes` - path to handler - on all interfaces at `port`, where
/// Centrifugo reaches them as `host.docker.internal`.
async fn serve(port: u16, routes: HashMap<&'static str, Handler>) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap();
    let routes = Arc::new(routes);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let routes = Arc::clone(&routes);
            tokio::spawn(async move {
                let svc =
                    hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let routes = Arc::clone(&routes);
                        async move {
                            let (parts, body) = req.into_parts();
                            let body = body
                                .collect()
                                .await
                                .map(http_body_util::Collected::to_bytes)
                                .unwrap_or_default();
                            let resp = match routes.get(parts.uri.path()) {
                                Some(h) => h.handle(http::Request::from_parts(parts, body)).await,
                                None => {
                                    let mut r = http::Response::new(Bytes::new());
                                    *r.status_mut() = http::StatusCode::NOT_FOUND;
                                    r
                                }
                            };
                            Ok::<_, std::convert::Infallible>(resp.map(Full::new))
                        }
                    });
                let io = hyper_util::rt::TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    })
}

#[tokio::test]
async fn publishes_and_reads_history_payloads_exactly() {
    skip_unless_enabled!();
    let c = client().await;
    let ch = unique("");
    let text = r#"{"n":1.50,"big":9007199254740993,"empty":{},"keys":{"0":"a"}}"#;
    let data = RawValue::from_string(text.into()).unwrap();
    let res = c
        .publish(PublishRequest::new(&ch).data(data).tags([("k", "v")]))
        .await
        .unwrap();
    assert_eq!(res.offset, 1);
    assert!(!res.epoch.is_empty());
    let pubs = c
        .history(HistoryRequest::new(&ch).limit(10))
        .await
        .unwrap()
        .publications;
    assert_eq!(pubs.len(), 1);
    assert_eq!(pubs[0].tags.get("k").map(String::as_str), Some("v"));
    // Sent back unchanged, the payload is the same JSON value - the integer
    // past 2^53 included.
    let data = pubs[0].data.clone().unwrap();
    let got: Value = serde_json::from_str(data.get()).unwrap();
    assert_eq!(got["big"].to_string(), "9007199254740993");
    c.publish(PublishRequest::new(&ch).data(data.clone()))
        .await
        .unwrap();
    let again = c
        .history(HistoryRequest::new(&ch).limit(10).reverse(true))
        .await
        .unwrap()
        .publications;
    assert_eq!(
        serde_json::from_str::<Value>(again[0].data.as_ref().unwrap().get()).unwrap(),
        got
    );
    c.history_remove(HistoryRemoveRequest::new(&ch))
        .await
        .unwrap();
    assert!(
        c.history(HistoryRequest::new(&ch).limit(10))
            .await
            .unwrap()
            .publications
            .is_empty()
    );
}

#[tokio::test]
async fn publishes_once_per_idempotency_key() {
    skip_unless_enabled!();
    let c = client().await;
    let ch = unique("");
    let first = c
        .publish(
            PublishRequest::new(&ch)
                .data(raw(&json!(1)))
                .idempotency_key("k1"),
        )
        .await
        .unwrap();
    let second = c
        .publish(
            PublishRequest::new(&ch)
                .data(raw(&json!(2)))
                .idempotency_key("k1"),
        )
        .await
        .unwrap();
    assert_eq!(first.offset, second.offset);
    assert_eq!(
        c.history(HistoryRequest::new(&ch).limit(10))
            .await
            .unwrap()
            .publications
            .len(),
        1
    );
}

#[tokio::test]
async fn broadcasts() {
    skip_unless_enabled!();
    let c = client().await;
    let (a, b) = (unique("a-"), unique("b-"));
    let res = c
        .broadcast(BroadcastRequest::new([&a, &b]).data(raw(&json!({"x": 1}))))
        .await
        .unwrap();
    let offsets: Vec<u64> = res
        .channels
        .iter()
        .map(|ch| ch.result.as_ref().unwrap().offset)
        .collect();
    assert_eq!(offsets, [1, 1]);
    let e = c
        .broadcast(BroadcastRequest::new([a.as_str(), "unknown:x"]).data(raw(&json!(2))))
        .await
        .unwrap_err();
    let Error::BroadcastFailedChannels(be) = &e else {
        panic!("{e:?}")
    };
    let failed = be.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].channel, "unknown:x");
    assert_eq!(
        failed[0].result.as_ref().unwrap_err().api_code(),
        Some(ApiError::UNKNOWN_CHANNEL)
    );
    assert!(be.result().channels[0].result.is_ok());
    assert!(!retryable(&e));

    // With retry, under one key: retried later with it, nothing is published
    // again.
    let res = c
        .broadcast_with_retry(
            BroadcastRequest::new([&a]).data(raw(&json!(1))),
            Default::default(),
        )
        .await
        .unwrap();
    assert!(res.idempotency_key.starts_with("rustcent-"));
    let before = c
        .history(HistoryRequest::new(&a).limit(10))
        .await
        .unwrap()
        .publications
        .len();
    c.broadcast(
        BroadcastRequest::new([&a])
            .data(raw(&json!(1)))
            .idempotency_key(&res.idempotency_key),
    )
    .await
    .unwrap();
    assert_eq!(
        c.history(HistoryRequest::new(&a).limit(10))
            .await
            .unwrap()
            .publications
            .len(),
        before
    );
}

#[tokio::test]
async fn batches() {
    skip_unless_enabled!();
    let c = client().await;
    let ch = unique("");
    let mut b = c.new_batch(BatchOptions::default());
    let publication = b.publish(PublishRequest::new(&ch).data(raw(&json!(1))));
    let history = b.history(HistoryRequest::new(&ch).limit(1));
    let bad = b.publish(PublishRequest::new("unknown:x").data(raw(&json!(1))));
    let e = b.send().await.unwrap_err();
    let Error::BatchFailedCommands(be) = &e else {
        panic!("{e:?}")
    };
    assert_eq!(be.failed().iter().map(|f| f.index).collect::<Vec<_>>(), [2]);
    assert_eq!(publication.result().unwrap().offset, 1);
    assert_eq!(history.result().unwrap().publications.len(), 1);
    assert_eq!(
        bad.result().unwrap_err().api_code(),
        Some(ApiError::UNKNOWN_CHANNEL)
    );
}

#[tokio::test]
async fn refused_requests_and_pro_hints() {
    skip_unless_enabled!();
    let c = client().await;
    let e = c
        .history(HistoryRequest::new("unknown:x"))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Api(_)) && !retryable(&e));
    let Err(e) = c
        .connections(ConnectionsRequest::new().user("nobody"))
        .await
    else {
        return; // Centrifugo PRO
    };
    assert!(matches!(&e, Error::Http(h) if h.status == 404), "{e:?}");
    assert!(e.to_string().contains("Centrifugo PRO method"));
    let mut b = c.new_batch(BatchOptions::default());
    let conns = b.connections(ConnectionsRequest::new().user("nobody"));
    let _ = b.send().await;
    let e = conns.result().unwrap_err();
    assert_eq!(e.api_code(), Some(ApiError::NOT_FOUND));
    assert!(e.to_string().contains("Centrifugo PRO method"));
}

#[tokio::test]
async fn info_and_channels() {
    skip_unless_enabled!();
    let c = client().await;
    let info = c.info(InfoRequest::new()).await.unwrap();
    assert!(!info.nodes.is_empty());
    assert!(!info.nodes[0].version.is_empty());
    c.channels(ChannelsRequest::new().pattern("rustcent-*"))
        .await
        .unwrap();
}

#[tokio::test]
async fn maps_and_shared_polls() {
    skip_unless_enabled!();
    let c = client().await;
    let ch = format!("map:{}", unique(""));
    c.map_publish(MapPublishRequest::new(&ch, "k1").data(raw(&json!({"v": 1}))))
        .await
        .unwrap();
    c.map_publish(MapPublishRequest::new(&ch, "k2").data(raw(&json!({"v": 2}))))
        .await
        .unwrap();
    let state = c
        .map_read_state(MapReadStateRequest::new(&ch).limit(10))
        .await
        .unwrap();
    let mut keys: Vec<&str> = state.entries.iter().map(|e| e.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["k1", "k2"]);
    c.map_read_stream(MapReadStreamRequest::new(&ch).limit(10))
        .await
        .unwrap();
    c.map_remove(MapRemoveRequest::new(&ch, "k1"))
        .await
        .unwrap();
    assert_eq!(
        c.map_stats(MapStatsRequest::new(&ch))
            .await
            .unwrap()
            .num_keys,
        1
    );
    c.map_clear(MapClearRequest::new(&ch)).await.unwrap();
    let poll = format!("poll:{}", unique(""));
    c.shared_poll_publish(SharedPollPublishRequest::new(&poll, "k", 1).data(raw(&json!({"v": 1}))))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batches_concurrent_calls_automatically_in_order_per_caller() {
    skip_unless_enabled!();
    let batched = Arc::new(AtomicUsize::new(0));
    let b2 = Arc::clone(&batched);
    let c = Client::new(config().auto_batch(AutoBatch::new(2)).on_request(move |i| {
        if i.auto_batch {
            b2.fetch_add(1, Ordering::SeqCst);
        }
    }))
    .unwrap();
    let ch = unique("");
    let tasks: Vec<_> = (0..16)
        .map(|w| {
            let (c, ch) = (c.clone(), format!("{ch}-{w}"));
            tokio::spawn(async move {
                for n in 0..20 {
                    c.publish(PublishRequest::new(&ch).data(raw(&json!({ "n": n }))))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    c.close(Duration::from_secs(10)).await.unwrap();
    assert!(batched.load(Ordering::SeqCst) > 0);
    let plain = client().await;
    for w in 0..16 {
        let pubs = plain
            .history(HistoryRequest::new(format!("{ch}-{w}")).limit(100))
            .await
            .unwrap()
            .publications;
        let ns: Vec<i64> = pubs
            .iter()
            .map(|p| {
                serde_json::from_str::<Value>(p.data.as_ref().unwrap().get()).unwrap()["n"]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        assert_eq!(
            ns,
            (0..20).collect::<Vec<_>>(),
            "one after another: in order"
        );
    }
}

#[tokio::test]
async fn tokens_connect_a_real_client() {
    skip_unless_enabled!();
    let c = client().await;
    let exp = SystemTime::now() + Duration::from_secs(3600);
    // HMAC, and Ed25519 verified by client.token.ed25519_public_key: the
    // public key of the seed of 32 bytes of 2.
    for (name, key) in [
        ("hmac", Key::hmac("client_secret").unwrap()),
        ("ed25519", Key::ed25519(&[2; 32]).unwrap()),
    ] {
        let user = unique(&format!("{name}-"));
        let ch = format!("{user}-news");
        let claims = ConnectionClaims::new(&user, Expiry::At(exp))
            .info(raw(&json!({"name": "Alice"})))
            .channels([&ch]);
        let tok = token::connection(&key, &claims).unwrap();
        let mut s = Stream::open(json!({ "token": tok })).await;
        let msg = s.next().await.unwrap();
        assert!(msg.get("connect").is_some(), "{name}: {msg}");
        assert!(msg.to_string().contains(&ch));
        let presence = c
            .presence(PresenceRequest::new(&ch))
            .await
            .unwrap()
            .presence;
        let infos: Vec<(String, Value)> = presence
            .values()
            .map(|i| {
                (
                    i.user.clone(),
                    serde_json::from_str(i.conn_info.as_ref().unwrap().get()).unwrap(),
                )
            })
            .collect();
        assert_eq!(infos, [(user.clone(), json!({"name": "Alice"}))]);
        assert_eq!(
            c.presence_stats(PresenceStatsRequest::new(&ch))
                .await
                .unwrap()
                .num_users,
            1
        );
        c.publish(PublishRequest::new(&ch).data(raw(&json!({"text": "hello"}))))
            .await
            .unwrap();
        assert!(s.next().await.unwrap().to_string().contains("hello"));
        let other = format!("{ch}-2");
        c.subscribe(SubscribeRequest::new(&other).user(&user))
            .await
            .unwrap();
        assert!(s.next().await.unwrap().to_string().contains(&other));
        c.unsubscribe(UnsubscribeRequest::new(&other).user(&user))
            .await
            .unwrap();
        c.refresh(RefreshRequest::new().user(&user).expire_at(now() + 3600))
            .await
            .unwrap();
        c.disconnect(DisconnectRequest::new().user(&user))
            .await
            .unwrap();
        while s.next().await.is_some() {}
    }
    // A token signed with another key is refused.
    let bad = token::connection(
        &Key::ed25519(&[9; 32]).unwrap(),
        &ConnectionClaims::new("u", Expiry::At(exp)),
    )
    .unwrap();
    let mut s = Stream::open(json!({ "token": bad })).await;
    let msg = s.next().await;
    assert!(
        msg.is_none() || msg.as_ref().unwrap().get("disconnect").is_some(),
        "{msg:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_handlers_called_by_centrifugo() {
    skip_unless_enabled!();
    let c = client().await;
    let user = unique("proxy-");
    let ch = format!("{user}-news");
    let connects: Arc<Mutex<Vec<proxy::ConnectRequest>>> = Arc::default();
    let refreshes: Arc<Mutex<Vec<proxy::RefreshRequest>>> = Arc::default();
    let errors: Arc<Mutex<Vec<String>>> = Arc::default();
    let e2 = Arc::clone(&errors);
    let handlers =
        proxy::Handlers::new().on_error(move |_, e| e2.lock().unwrap().push(e.to_string()));
    let (seen, u, ch2) = (Arc::clone(&connects), user.clone(), ch.clone());
    let connect = handlers.connect(move |_, req| {
        seen.lock().unwrap().push(req.clone());
        let (user, ch) = (u.clone(), ch2.clone());
        async move {
            let allow: Value = req
                .data
                .as_ref()
                .map_or(Value::Null, |d| serde_json::from_str(d.get()).unwrap());
            if allow["allow"] != true {
                return Err(proxy::Disconnect::new(4501, "unauthorized").into());
            }
            Ok(proxy::ConnectResult::new()
                .user(user)
                .expire_at(now() + 2)
                .info(raw(&json!({"name": "Alice"})))
                .data(raw(&json!({"welcome": true})))
                .channels([ch]))
        }
    });
    let seen = Arc::clone(&refreshes);
    let refresh = handlers.refresh(move |_, req| {
        seen.lock().unwrap().push(req);
        async { Ok(proxy::RefreshResult::new().expired(true)) }
    });
    let port = std::env::var("RUSTCENT_TEST_PROXY_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3100);
    let server = serve(
        port,
        HashMap::from([("/connect", connect), ("/refresh", refresh)]),
    )
    .await;

    // Refused: the connection is closed with the disconnect's code.
    let mut refused = Stream::open(json!({"data": {"allow": false}})).await;
    let msg = refused.next().await.unwrap();
    assert_eq!(msg["disconnect"]["code"], 4501, "{msg}");
    {
        let first = &connects.lock().unwrap()[0];
        assert_eq!(
            (
                first.transport.as_str(),
                first.protocol.as_str(),
                first.encoding.as_str()
            ),
            ("uni_http_stream", "json", "json")
        );
        assert!(!first.client.is_empty());
    }

    // Accepted: the result's data and server-side channel reach the client,
    // and so does a publication into that channel.
    let mut s = Stream::open(json!({"data": {"allow": true}})).await;
    let reply = s.next().await.unwrap();
    assert_eq!(
        reply["connect"]["data"],
        json!({"welcome": true}),
        "{reply}"
    );
    assert!(reply["connect"]["subs"].get(&ch).is_some());
    c.publish(PublishRequest::new(&ch).data(raw(&json!({"text": "hello"}))))
        .await
        .unwrap();
    assert!(s.next().await.unwrap().to_string().contains("hello"));

    // It expires: Centrifugo asks the refresh proxy, which ends it.
    while s.next().await.is_some() {}
    let refreshed = refreshes.lock().unwrap()[0].clone();
    assert_eq!(refreshed.user, user);
    assert_eq!(
        refreshed.client,
        reply["connect"]["client"].as_str().unwrap()
    );
    assert!(
        errors.lock().unwrap().is_empty(),
        "{:?}",
        errors.lock().unwrap()
    );
    server.abort();
}

#[tokio::test]
async fn pro_user_status_and_control() {
    skip_unless_enabled!();
    let Some(c) = pro().await else { return };
    let user = unique("");
    c.update_user_status(UpdateUserStatusRequest::new([&user]).state("away"))
        .await
        .unwrap();
    let st = c
        .get_user_status(GetUserStatusRequest::new([&user]))
        .await
        .unwrap()
        .statuses
        .remove(0);
    assert_eq!(
        (st.user.as_str(), st.state.as_str()),
        (user.as_str(), "away")
    );
    assert!(st.active > 0);
    c.delete_user_status(DeleteUserStatusRequest::new([&user]))
        .await
        .unwrap();
    assert_eq!(
        c.get_user_status(GetUserStatusRequest::new([&user]))
            .await
            .unwrap()
            .statuses[0]
            .active,
        0
    );

    let expire_at = now() + 60;
    c.block_user(BlockUserRequest::new(&user).expire_at(expire_at))
        .await
        .unwrap();
    c.unblock_user(UnblockUserRequest::new(&user))
        .await
        .unwrap();
    c.revoke_token(RevokeTokenRequest::new(format!("{user}-jti")).expire_at(expire_at))
        .await
        .unwrap();
    c.invalidate_user_tokens(
        InvalidateUserTokensRequest::new(&user)
            .issued_before(now())
            .expire_at(expire_at),
    )
    .await
    .unwrap();
    c.connections(ConnectionsRequest::new().user(&user))
        .await
        .unwrap();
}

/// A browser `PushSubscription`, as webpush devices are registered with.
fn webpush_token() -> String {
    let pair =
        aws_lc_rs::agreement::PrivateKey::generate(&aws_lc_rs::agreement::ECDH_P256).unwrap();
    let public = pair.compute_public_key().unwrap();
    let b64 = |b: &[u8]| {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
    };
    json!({
        "endpoint": format!("https://fcm.googleapis.com/fcm/send/{}", b64(&public.as_ref()[..8])),
        "keys": {"p256dh": b64(public.as_ref()), "auth": b64(&[0; 16])},
    })
    .to_string()
}

#[tokio::test]
async fn pro_devices_and_push() {
    skip_unless_enabled!();
    let Some(c) = pro().await else { return };
    let user = unique("");
    let reg = c
        .device_register(
            DeviceRegisterRequest::new("webpush", webpush_token(), "web")
                .user(&user)
                .meta([("app", "rustcent")])
                .topics(["news"]),
        )
        .await
        .unwrap();
    assert!(!reg.id.is_empty());
    let by_user = || DeviceFilter::new().users([&user]);
    let list = c
        .device_list(
            DeviceListRequest::new()
                .filter(by_user())
                .include_meta(true)
                .include_topics(true)
                .include_total_count(true),
        )
        .await
        .unwrap();
    assert_eq!(list.total_count, 1);
    assert_eq!(list.items[0].id, reg.id);
    assert_eq!(
        list.items[0].meta.get("app").map(String::as_str),
        Some("rustcent")
    );
    assert!(list.items[0].topics.iter().any(|t| t == "news"));
    c.device_topic_update(DeviceTopicUpdateRequest::new(&reg.id, "add").topics(["sport"]))
        .await
        .unwrap();
    let topics = c
        .device_topic_list(
            DeviceTopicListRequest::new()
                .filter(DeviceTopicFilter::new().device_ids([&reg.id]))
                .include_total_count(true),
        )
        .await
        .unwrap();
    assert_eq!(topics.total_count, 2);
    c.device_update(
        DeviceUpdateRequest::new()
            .ids([&reg.id])
            .meta_update(DeviceMetaUpdate::new([("app", "updated")])),
    )
    .await
    .unwrap();
    let list = c
        .device_list(
            DeviceListRequest::new()
                .filter(by_user())
                .include_meta(true),
        )
        .await
        .unwrap();
    assert_eq!(
        list.items[0].meta.get("app").map(String::as_str),
        Some("updated")
    );
    c.user_topic_update(UserTopicUpdateRequest::new(&user, "add").topics(["alerts"]))
        .await
        .unwrap();
    let user_topics = c
        .user_topic_list(UserTopicListRequest::new().filter(UserTopicFilter::new().users([&user])))
        .await
        .unwrap();
    assert_eq!(
        user_topics
            .items
            .iter()
            .map(|i| i.topic.as_str())
            .collect::<Vec<_>>(),
        ["alerts"]
    );
    let push = c
        .send_push_notification(
            SendPushNotificationRequest::new(
                PushRecipient::new().filter(by_user()),
                PushNotification::new()
                    .webpush(WebPushPushNotification::new(raw(&json!({"title": "hi"})))),
            )
            .send_at(now() + 3600),
        )
        .await
        .unwrap();
    assert!(!push.uid.is_empty());
    c.cancel_push(CancelPushRequest::new(&push.uid))
        .await
        .unwrap();
    // Push status needs push analytics in ClickHouse: without it, the method
    // is not available.
    if let Err(e) = c
        .update_push_status(UpdatePushStatusRequest::new(
            &push.uid,
            "delivered",
            &reg.id,
        ))
        .await
    {
        assert_eq!(e.api_code(), Some(ApiError::NOT_AVAILABLE), "{e}");
    }
    c.device_remove(DeviceRemoveRequest::new().ids([&reg.id]))
        .await
        .unwrap();
    assert!(
        c.device_list(DeviceListRequest::new().filter(by_user()))
            .await
            .unwrap()
            .items
            .is_empty()
    );
}

#[tokio::test]
async fn pro_methods_in_a_batch() {
    skip_unless_enabled!();
    let Some(c) = pro().await else { return };
    let user = unique("");
    let mut b = c.new_batch(BatchOptions::default());
    b.update_user_status(UpdateUserStatusRequest::new([&user]).state("busy"));
    let status = b.get_user_status(GetUserStatusRequest::new([&user]));
    let reg = b
        .device_register(DeviceRegisterRequest::new("webpush", webpush_token(), "web").user(&user));
    let conns = b.connections(ConnectionsRequest::new().user(&user));
    b.send().await.unwrap();
    assert_eq!(status.result().unwrap().statuses[0].state, "busy");
    let id = reg.result().unwrap().id;
    assert!(conns.result().unwrap().connections.is_empty());
    c.device_remove(DeviceRemoveRequest::new().ids([id]))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pro_api_tokens() {
    skip_unless_enabled!();
    if pro().await.is_none() {
        return;
    }
    // The same key on every run, from a fixed seed: Centrifugo caches keys
    // by ID, so a new key under a known ID would be refused.
    let key = Key::ed25519(&[1; 32])
        .unwrap()
        .with_key_id("gocent-api-test");
    let jwks = endpoints::jwks(std::slice::from_ref(&key)).unwrap();
    let port = std::env::var("RUSTCENT_TEST_API_JWKS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3101);
    let server = serve(port, HashMap::from([("/api_jwks", jwks)])).await;
    let publish = |config: Config| async move {
        Client::new(config)
            .unwrap()
            .publish(PublishRequest::new(unique("")).data(raw(&json!({}))))
            .await
    };
    let claims = ApiClaims::new()
        .audience("centrifugo-api")
        .scope(["centrifugo:api"]);
    publish(Config::new(addr()).bearer_token(ApiTokens::new(key.clone(), claims)))
        .await
        .unwrap();

    let expires = SystemTime::now() + Duration::from_secs(60);
    let refused = [
        token::api(
            &key,
            &ApiClaims::new()
                .audience("centrifugo-api")
                .expires_at(expires),
        )
        .unwrap(),
        token::api(
            &key,
            &ApiClaims::new()
                .scope(["centrifugo:api"])
                .expires_at(expires),
        )
        .unwrap(),
        token::connection(&key, &ConnectionClaims::new("42", Expiry::At(expires))).unwrap(),
        token::api(
            &Key::ed25519(&[2; 32])
                .unwrap()
                .with_key_id("gocent-unpublished"),
            &ApiClaims::new()
                .audience("centrifugo-api")
                .expires_at(expires),
        )
        .unwrap(),
    ];
    for tok in refused {
        let e = publish(Config::new(addr()).bearer_token(move || {
            let tok = tok.clone();
            async move { Ok(tok) }
        }))
        .await
        .unwrap_err();
        assert!(e.is_unauthorized(), "{e:?}");
    }
    server.abort();
}

#[tokio::test]
async fn pro_commands_consumed_from_a_redis_stream() {
    skip_unless_enabled!();
    let Some(c) = pro().await else { return };
    let ch = unique("");
    let apart = Command::publish(PublishRequest::new(&ch).data(raw(&json!({"n": 1})))).unwrap();
    xadd(
        "rustcent_commands",
        &["method", apart.method(), "payload", apart.payload().get()],
    )
    .await;
    let whole = Command::publish(PublishRequest::new(&ch).data(raw(&json!({"n": 2})))).unwrap();
    xadd("rustcent_commands", &["payload", &whole.to_json()]).await;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let pubs = c
            .history(HistoryRequest::new(&ch).limit(10))
            .await
            .unwrap()
            .publications;
        if pubs.len() == 2 {
            let data: Vec<Value> = pubs
                .iter()
                .map(|p| serde_json::from_str(p.data.as_ref().unwrap().get()).unwrap())
                .collect();
            assert_eq!(data, [json!({"n": 1}), json!({"n": 2})]);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} of 2 commands consumed after 15s",
            pubs.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `XADD stream * fields...`, over RESP.
async fn xadd(stream: &str, fields: &[&str]) {
    let redis = std::env::var("RUSTCENT_TEST_REDIS").unwrap_or_else(|_| "localhost:46379".into());
    let mut args = vec!["XADD", stream, "*"];
    args.extend_from_slice(fields);
    let mut cmd = format!("*{}\r\n", args.len());
    for a in args {
        cmd.push_str(&format!("${}\r\n{a}\r\n", a.len()));
    }
    let mut s = tokio::net::TcpStream::connect(redis).await.unwrap();
    s.write_all(cmd.as_bytes()).await.unwrap();
    let mut reply = [0; 64];
    let n = s.read(&mut reply).await.unwrap();
    assert_eq!(reply[0], b'$', "{}", String::from_utf8_lossy(&reply[..n]));
}
