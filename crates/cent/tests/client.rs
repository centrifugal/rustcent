//! The client against a fake Centrifugo: configuration, requests, the error
//! model, timeouts, payloads, close and `on_request` (spec 3, 4, 10, 15).

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![cfg(feature = "client")]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cent::{
    ApiError, Client, Config, Error, InfoRequest, PresenceRequest, PublishRequest, PublishResult,
    RequestInfo, retryable,
};
use common::{Answer, Fake, ok, raw};

fn publish() -> PublishRequest {
    PublishRequest::new("news").data(raw(r#"{"text":"hello"}"#))
}

fn invalid_config(config: Config) -> String {
    match Client::new(config) {
        Err(Error::InvalidConfig(e)) => e.to_string(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn config_is_checked() {
    let fake = Fake::centrifugo();
    let base = || Config::new("http://h/api").http_client(fake.client());
    assert!(invalid_config(Config::new("localhost:8000")).contains("absolute"));
    assert!(invalid_config(Config::new("http://h/api/publish")).contains("ends with the method"));
    assert!(
        invalid_config(
            base()
                .api_key("k")
                .bearer_token(|| async { Ok("t".into()) })
        )
        .contains("not both")
    );
    assert!(invalid_config(base().api_key("k\nX-Evil: 1")).contains("control character"));
    for name in ["X-API-Key", "authorization", "Content-Type"] {
        assert!(
            invalid_config(base().header(name, "v")).contains("may not set"),
            "{name}"
        );
    }
    assert!(invalid_config(base().header("bad name", "v")).contains("not a valid"));
    assert!(invalid_config(base().header("X-A", "v\r\n")).contains("control character"));
    assert!(invalid_config(base().header("Host", "a").header("Host", "b")).contains("exactly one"));
    assert!(invalid_config(base().header("Host", "")).contains("exactly one"));
    assert!(invalid_config(base().request_timeout(Duration::ZERO)).contains("zero"));
    assert!(
        invalid_config(base().auto_batch(cent::AutoBatch::new(0).max_batch_size(10)))
            .contains("max_in_flight")
    );
    assert!(
        invalid_config(base().auto_batch(cent::AutoBatch::new(4).max_batch_size(1)))
            .contains("never batches")
    );
    // No credentials at all is valid: mutual TLS, a proxy, or an insecure API.
    Client::new(base()).unwrap();
}

#[tokio::test]
async fn requests_are_posts_of_json_with_headers() {
    let fake = Fake::centrifugo();
    let client = Client::new(
        fake.config()
            .header("X-Trace", "1")
            .header("Host", "centrifugo.internal"),
    )
    .unwrap();
    client.publish(publish().skip_history(true)).await.unwrap();
    let r = &fake.requests()[0];
    assert_eq!(r.method, http::Method::POST);
    assert_eq!(r.uri, "http://centrifugo.test/api/publish");
    assert_eq!(r.headers["content-type"], "application/json");
    assert_eq!(r.headers["user-agent"], "rustcent/0.1");
    assert_eq!(r.headers["x-api-key"], "key");
    assert_eq!(r.headers["x-trace"], "1");
    assert_eq!(r.headers["host"], "centrifugo.internal");
    assert_eq!(
        r.body,
        serde_json::json!({"channel": "news", "data": {"text": "hello"}, "skip_history": true})
    );

    let client = Client::new(fake.config().header("User-Agent", "mine")).unwrap();
    client.info(InfoRequest::new()).await.unwrap();
    assert_eq!(fake.requests()[1].headers["user-agent"], "mine");
}

#[tokio::test]
async fn endpoint_and_token_functions_are_called_per_request() {
    let fake = Fake::centrifugo();
    let n = Arc::new(Mutex::new(0));
    let n2 = Arc::clone(&n);
    let config = Config::with_endpoint_fn(move || {
        let n = Arc::clone(&n2);
        async move {
            *n.lock().unwrap() += 1;
            Ok(format!("http://node{}.test/api/", n.lock().unwrap()))
        }
    })
    .bearer_token(|| async { Ok("tok".to_owned()) })
    .http_client(fake.client());
    let client = Client::new(config).unwrap();
    client.info(InfoRequest::new()).await.unwrap();
    client.info(InfoRequest::new()).await.unwrap();
    let reqs = fake.requests();
    assert_eq!(reqs[0].uri, "http://node1.test/api/info");
    assert_eq!(reqs[1].uri, "http://node2.test/api/info");
    assert_eq!(reqs[0].headers["authorization"], "Bearer tok");
    assert!(reqs[0].headers.get("x-api-key").is_none());
}

#[tokio::test]
async fn bad_endpoints_and_tokens_fail_before_sending() {
    let fake = Fake::centrifugo();
    let call = |config: Config| {
        let client = Client::new(config.http_client(fake.client())).unwrap();
        async move { client.info(InfoRequest::new()).await }
    };
    let e = call(Config::with_endpoint_fn(|| async {
        Ok("http://h/api/publish".to_owned())
    }))
    .await
    .unwrap_err();
    assert!(
        matches!(&e, Error::Callback(c) if c.which() == "api_endpoint"),
        "{e}"
    );
    let e = call(Config::with_endpoint_fn(|| async {
        Err("discovery down".into())
    }))
    .await
    .unwrap_err();
    assert!(e.to_string().contains("discovery down") && !retryable(&e));
    let e = call(Config::new("http://h/api").bearer_token(|| async { Ok(String::new()) }))
        .await
        .unwrap_err();
    assert!(
        matches!(&e, Error::Callback(c) if c.which() == "bearer_token"),
        "{e}"
    );
    let e = call(Config::new("http://h/api").bearer_token(|| async { Ok("a\nb".to_owned()) }))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Callback(_)));
    // An I/O failure of the application's function is retryable.
    let e = call(Config::new("http://h/api").bearer_token(|| async {
        Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into())
    }))
    .await
    .unwrap_err();
    assert!(retryable(&e));
    assert!(fake.requests().is_empty(), "nothing sent");
}

#[tokio::test]
async fn results_decode_and_unknown_fields_are_ignored() {
    let fake = Fake::new(|_| ok(r#"{"result":{"offset":7,"epoch":"xyz","future_field":1}}"#));
    let res = Client::new(fake.config())
        .unwrap()
        .publish(publish())
        .await
        .unwrap();
    assert_eq!((res.offset, res.epoch.as_str()), (7, "xyz"));
}

async fn error_of(answer: impl Fn() -> Answer + Send + Sync + 'static) -> Error {
    let fake = Fake::new(move |_| answer());
    Client::new(fake.config())
        .unwrap()
        .publish(publish())
        .await
        .unwrap_err()
}

#[tokio::test]
async fn error_kinds() {
    // Centrifugo's error, by code alone; an unknown code is still one.
    let e = error_of(|| ok(r#"{"error":{"code":102,"message":"unknown channel"}}"#)).await;
    assert_eq!(e.api_code(), Some(ApiError::UNKNOWN_CHANNEL));
    assert!(!retryable(&e));
    assert_eq!(e.to_string(), "unknown channel (code 102)");
    let e = error_of(|| ok(r#"{"error":{"code":199,"message":"new"}}"#)).await;
    assert_eq!(e.api_code(), Some(199));
    let e = error_of(|| ok(r#"{"error":{"code":108,"temporary":true}}"#)).await;
    assert!(retryable(&e), "temporary");
    let e = error_of(|| ok(r#"{"error":{"code":100}}"#)).await;
    assert!(retryable(&e), "internal, from servers before temporary");

    // HTTP errors, with hints.
    let e = error_of(|| Answer::Json(401, "Unauthorized".into())).await;
    assert!(e.is_unauthorized() && !retryable(&e));
    assert!(e.to_string().contains("check the credentials"), "{e}");
    let e = error_of(|| Answer::Json(404, "404 page not found".into())).await;
    assert!(
        e.to_string().contains("is api_endpoint the API base URL"),
        "{e}"
    );
    let mut location = http::HeaderMap::new();
    location.insert("location", "https://h/api/publish".parse().unwrap());
    let e = error_of(move || Answer::Status(301, location.clone(), String::new())).await;
    assert!(e.to_string().contains("a redirect"), "{e}");
    let Error::Http(h) = &e else { panic!("{e:?}") };
    assert_eq!(
        h.headers["location"], "https://h/api/publish",
        "headers kept"
    );
    for status in [429, 500, 502, 503] {
        assert!(
            retryable(&error_of(move || Answer::Json(status, String::new())).await),
            "{status}"
        );
    }
    assert!(!retryable(
        &error_of(|| Answer::Json(403, String::new())).await
    ));
    // The start of a long body is kept, trimmed.
    let e = error_of(|| Answer::Json(502, format!("  {}  ", "x".repeat(2000)))).await;
    let Error::Http(h) = e else { panic!() };
    assert_eq!(h.body, "x".repeat(510), "the first 512 bytes, trimmed");

    // Replies which are not Centrifugo's.
    for body in ["<html>", "[]", r#"{"result":{"offset":"x"}}"#, ""] {
        let e = error_of(move || ok(body)).await;
        assert!(matches!(e, Error::Decode(_)), "{body}: {e:?}");
        assert!(!retryable(&e));
    }

    // The network.
    let e = error_of(|| {
        Answer::Error(std::io::Error::from(std::io::ErrorKind::ConnectionReset).into())
    })
    .await;
    assert!(matches!(e, Error::Network(_)) && retryable(&e));
    let e =
        error_of(|| Answer::Error(std::io::Error::from(std::io::ErrorKind::TimedOut).into())).await;
    assert!(e.is_timeout() && retryable(&e));
    // An HttpClient may return the crate's own errors, for test doubles.
    let e = error_of(|| Answer::Error(Box::new(Error::Api(ApiError::new(113, "conflict"))))).await;
    assert_eq!(e.api_code(), Some(113));
}

// Spec 4.2: in the transport error mode a failed call is an HTTP status with
// the error as the body, and the same error as in a 200 reply.
#[tokio::test]
async fn transport_error_mode() {
    for status in [400, 404, 409, 416, 429, 500] {
        let e = error_of(move || {
            Answer::Json(status, r#"{"code":102,"message":"unknown channel"}"#.into())
        })
        .await;
        assert_eq!(e.api_code(), Some(102), "{status}");
    }
    // A gateway's body with an HTTP status as its code stays an HTTP error.
    let e = error_of(|| Answer::Json(429, r#"{"code":429,"message":"slow down"}"#.into())).await;
    assert!(matches!(e, Error::Http(_)));
    // Other statuses are not Centrifugo's mapping.
    let e = error_of(|| Answer::Json(503, r#"{"code":100}"#.into())).await;
    assert!(matches!(e, Error::Http(_)));
}

#[tokio::test]
async fn pro_method_hints() {
    let fake = Fake::new(|r| {
        if r.api_method == "batch" {
            ok(r#"{"replies":[{"error":{"code":104,"message":"not found"}}]}"#)
        } else {
            Answer::Json(404, "404 page not found".into())
        }
    });
    let client = Client::new(fake.config()).unwrap();
    let e = client
        .connections(cent::ConnectionsRequest::new())
        .await
        .unwrap_err();
    assert!(
        e.to_string()
            .contains("connections is a Centrifugo PRO method"),
        "{e}"
    );
    let mut b = client.new_batch(cent::BatchOptions::default());
    let p = b.connections(cent::ConnectionsRequest::new());
    let _ = b.send().await;
    let e = p.result().unwrap_err();
    assert_eq!(e.api_code(), Some(ApiError::NOT_FOUND));
    assert!(
        e.to_string().contains("is the server Centrifugo OSS?"),
        "{e}"
    );
}

#[tokio::test]
async fn invalid_requests_are_not_sent() {
    let fake = Fake::centrifugo();
    let client = Client::new(fake.config()).unwrap();
    for (req, problem) in [
        (
            PublishRequest::new("").data(raw("1")),
            "channel is required",
        ),
        (PublishRequest::new("c"), "data or b64data is required"),
    ] {
        let e = client.publish(req).await.unwrap_err();
        assert!(
            matches!(&e, Error::InvalidRequest(i) if i.method() == "publish"),
            "{e:?}"
        );
        assert!(e.to_string().contains(problem), "{e}");
        assert!(!retryable(&e));
    }
    let e = client
        .broadcast(cent::BroadcastRequest::new(["a", ""]).data(raw("1")))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("channels[1] is empty"), "{e}");
    let e = client
        .broadcast(cent::BroadcastRequest::new(Vec::<String>::new()).data(raw("1")))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("channels is required"), "{e}");
    // b64data is data too.
    client
        .publish(PublishRequest::new("c").b64data("aGk="))
        .await
        .unwrap();
    assert_eq!(fake.requests().len(), 1);
}

// Spec 3.4: a payload is carried exactly, in and out.
#[tokio::test]
async fn payloads_round_trip_exactly() {
    let payloads = [
        r#"{}"#,
        r#"{"0":"a"}"#,
        "[]",
        "9007199254740993",
        "1.0",
        "1e400",
        "null",
        r#""a string""#,
        r#"{"nested":[1,2.50,{"k":null}]}"#,
    ];
    for p in payloads {
        let reply = format!(r#"{{"result":{{"publications":[{{"data":{p},"offset":1}}]}}}}"#);
        let fake = Fake::new(move |r| {
            if r.api_method == "history" {
                ok(&reply)
            } else {
                ok(&common::default_reply(r))
            }
        });
        let client = Client::new(fake.config()).unwrap();
        let history = client
            .history(cent::HistoryRequest::new("c"))
            .await
            .unwrap();
        let data = history.publications[0].data.clone().expect("a payload");
        assert_eq!(data.get(), p, "decoded as it is");
        // Sent back unchanged, it is the same JSON value: the number text kept.
        client
            .publish(PublishRequest::new("c").data(data))
            .await
            .unwrap();
        let sent = fake.requests().last().unwrap().clone();
        let text = serde_json::to_string(&sent.body["data"]).unwrap();
        if !p.contains("e400")
            && !p.contains("9007199254740993")
            && !p.contains("2.50")
            && p != "1.0"
        {
            assert_eq!(
                text,
                serde_json::to_string(&serde_json::from_str::<serde_json::Value>(p).unwrap())
                    .unwrap()
            );
        }
    }
    // Byte-exact on the wire, checked without re-decoding: the body the
    // client encodes splices the raw text in.
    let req = PublishRequest::new("c").data(raw(r#"{"id":9007199254740993,"f":1.0}"#));
    assert_eq!(
        serde_json::to_string(&req).unwrap(),
        r#"{"channel":"c","data":{"id":9007199254740993,"f":1.0}}"#
    );
    // A JSON null payload is sent: Some(null) is null, None is unset.
    let req = PublishRequest::new("c").data(raw("null"));
    assert_eq!(
        serde_json::to_string(&req).unwrap(),
        r#"{"channel":"c","data":null}"#
    );
    // An absent payload in a reply is None, a null one Some(null).
    let p: cent::Publication = serde_json::from_str(r#"{"offset":1}"#).unwrap();
    assert!(p.data.is_none());
    let p: cent::Publication = serde_json::from_str(r#"{"data":null}"#).unwrap();
    assert_eq!(p.data.unwrap().get(), "null");
}

// Spec 3.7: results encode with wire names, payloads as they are.
#[tokio::test]
async fn results_as_json() {
    let fake = Fake::new(|r| {
        if r.api_method == "presence" {
            ok(r#"{"result":{"presence":{"c1":{"client":"c1","user":"42","conn_info":{"a":1}}}}}"#)
        } else {
            ok(
                r#"{"result":{"responses":[{"result":{"offset":3,"epoch":"e"}},{"error":{"code":102,"message":"unknown channel"}}]}}"#,
            )
        }
    });
    let client = Client::new(fake.config()).unwrap();
    let presence = client.presence(PresenceRequest::new("c")).await.unwrap();
    assert_eq!(
        serde_json::to_value(&presence).unwrap(),
        serde_json::json!({"presence": {"c1": {"client": "c1", "user": "42", "conn_info": {"a": 1}}}})
    );
    let e = client
        .broadcast(cent::BroadcastRequest::new(["a", "b"]).data(raw("1")))
        .await
        .unwrap_err();
    let result = e.broadcast_result().unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        serde_json::json!({
            "channels": [
                {"channel": "a", "result": {"offset": 3, "epoch": "e"}, "attempts": 1},
                {"channel": "b", "error": {"code": 102, "message": "unknown channel"}, "attempts": 1},
            ],
            "idempotency_key": "",
            "attempts": 1,
            "replied": true,
        })
    );
}

#[tokio::test]
async fn timeouts() {
    let fake =
        Fake::new(|_| Answer::After(Duration::from_secs(5), Box::new(ok(r#"{"result":{}}"#))));
    let client = Client::new(fake.config().request_timeout(Duration::from_millis(50))).unwrap();
    let start = std::time::Instant::now();
    let e = client.info(InfoRequest::new()).await.unwrap_err();
    assert!(e.is_timeout() && retryable(&e), "{e:?}");
    assert!(start.elapsed() < Duration::from_secs(1));
    // A call's own timeout, and a deadline, take precedence.
    let e = client
        .with_timeout(Duration::from_millis(10))
        .info(InfoRequest::new())
        .await
        .unwrap_err();
    assert!(e.is_timeout());
    let fast =
        Fake::new(|_| Answer::After(Duration::from_millis(100), Box::new(ok(r#"{"result":{}}"#))));
    let client = Client::new(fast.config().request_timeout(Duration::from_millis(10))).unwrap();
    client
        .with_timeout(Duration::from_secs(5))
        .info(InfoRequest::new())
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    client
        .with_deadline(deadline)
        .info(InfoRequest::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn wait_ends_with_the_deadline() {
    let deadline = std::time::Instant::now() + Duration::from_millis(30);
    cent::wait(deadline, Duration::from_millis(1))
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let e = cent::wait(deadline, Duration::from_secs(10))
        .await
        .unwrap_err();
    assert!(e.is_timeout());
    assert!(start.elapsed() < Duration::from_secs(1));
}

/// What `on_request` saw of a request: method, commands, failed, status,
/// whether it failed as a whole.
type Seen = Arc<Mutex<Vec<(String, usize, usize, u16, bool)>>>;

#[tokio::test]
async fn on_request_reports_every_request() {
    let seen: Seen = Arc::default();
    let seen2 = Arc::clone(&seen);
    let fake = Fake::new(|r| match r.api_method.as_str() {
        "info" => Answer::Json(500, "oops".into()),
        "presence" => ok(r#"{"error":{"code":102}}"#),
        _ => ok(&common::default_reply(r)),
    });
    let client = Client::new(fake.config().on_request(move |i: &RequestInfo<'_>| {
        seen2.lock().unwrap().push((
            i.method.to_owned(),
            i.commands,
            i.failed,
            i.status,
            i.error.is_some(),
        ));
    }))
    .unwrap();
    client.publish(publish()).await.unwrap();
    let _ = client.info(InfoRequest::new()).await;
    let _ = client.presence(PresenceRequest::new("c")).await;
    let mut b = client.new_batch(cent::BatchOptions::default());
    b.publish(publish());
    b.presence_stats(cent::PresenceStatsRequest::new("c"));
    b.send().await.unwrap();
    // Before reaching the network: still reported.
    let bad = Client::new(
        Config::with_endpoint_fn(|| async { Err("no endpoint".into()) })
            .http_client(fake.client())
            .on_request({
                let seen = Arc::clone(&seen);
                move |i: &RequestInfo<'_>| {
                    seen.lock().unwrap().push((
                        i.method.to_owned(),
                        i.commands,
                        i.failed,
                        i.status,
                        i.error.is_some(),
                    ))
                }
            }),
    )
    .unwrap();
    let _ = bad.info(InfoRequest::new()).await;
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            ("publish".into(), 1, 0, 200, false),
            ("info".into(), 1, 0, 500, true),
            ("presence".into(), 1, 1, 200, false),
            ("batch".into(), 2, 0, 200, false),
            ("info".into(), 1, 0, 0, true),
        ]
    );
}

#[tokio::test]
async fn close_refuses_new_calls_and_waits_for_those_in_progress() {
    let fake = Fake::new(|_| {
        Answer::After(
            Duration::from_millis(100),
            Box::new(ok(r#"{"result":{"offset":1}}"#)),
        )
    });
    let client = Client::new(fake.config()).unwrap();
    let c = client.clone();
    let in_progress = tokio::spawn(async move { c.publish(publish()).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    client.close(Duration::from_secs(5)).await.unwrap();
    assert!(in_progress.is_finished(), "close waited for the call");
    assert_eq!(in_progress.await.unwrap().unwrap(), PublishResultEq(1));
    assert!(matches!(
        client.info(InfoRequest::new()).await,
        Err(Error::Closed)
    ));
    let b = client.new_batch(cent::BatchOptions::default());
    assert!(
        matches!(b.send().await, Err(Error::Closed)),
        "an empty batch too"
    );
    client.close(Duration::from_secs(1)).await.unwrap();

    // A close which times out leaves the calls going.
    let client = Client::new(fake.config()).unwrap();
    let c = client.clone();
    let in_progress = tokio::spawn(async move { c.publish(publish()).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        client
            .close(Duration::from_millis(10))
            .await
            .unwrap_err()
            .is_timeout()
    );
    in_progress.await.unwrap().unwrap();
}

/// Compares a publish result by offset.
#[derive(Debug)]
struct PublishResultEq(u64);

impl PartialEq<PublishResultEq> for PublishResult {
    fn eq(&self, other: &PublishResultEq) -> bool {
        self.offset == other.0
    }
}

#[test]
fn outside_a_runtime() {
    let client = Client::new(Fake::centrifugo().config()).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(client.info(InfoRequest::new())).unwrap();
    // Another runtime: its connections and batches belong to the first.
    let other = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let e = other.block_on(client.info(InfoRequest::new())).unwrap_err();
    assert!(
        matches!(&e, Error::InvalidConfig(_)) && e.to_string().contains("per runtime"),
        "{e}"
    );
}

#[test]
fn commands() {
    let cmd = cent::Command::publish(publish().idempotency_key("k")).unwrap();
    assert_eq!(cmd.method(), "publish");
    assert_eq!(
        cmd.payload().get(),
        r#"{"channel":"news","data":{"text":"hello"},"idempotency_key":"k"}"#
    );
    let json = r#"{"method":"publish","payload":{"channel":"news","data":{"text":"hello"},"idempotency_key":"k"}}"#;
    assert_eq!(cmd.to_json(), json);
    // Encoded the language's way, the payload is the object, not a string.
    assert_eq!(serde_json::to_string(&cmd).unwrap(), json);
    assert!(matches!(
        cent::Command::publish(PublishRequest::new("c")),
        Err(Error::InvalidRequest(_))
    ));
}

/// No request starts once its deadline has passed.
#[tokio::test]
async fn an_ended_deadline_sends_nothing() {
    let fake = Fake::centrifugo();
    let client = Client::new(fake.config()).unwrap();
    let past = client.with_deadline(std::time::Instant::now() - Duration::from_secs(1));
    let e = past.publish(publish()).await.unwrap_err();
    assert!(
        matches!(&e, Error::Timeout(t) if t.unsent() && t.method() == Some("publish")),
        "{e:?}"
    );
    let e = past.new_batch(cent::BatchOptions::default());
    let mut e = e;
    e.info(InfoRequest::new());
    assert!(e.send().await.unwrap_err().is_timeout());
    let e = past
        .broadcast_with_retry(
            cent::BroadcastRequest::new(["a"]).data(raw("1")),
            Default::default(),
        )
        .await
        .unwrap_err();
    assert!(e.is_timeout(), "{e:?}");
    assert!(fake.requests().is_empty());
}

/// A timeout too long for an instant means no timeout, not a panic.
#[tokio::test]
async fn huge_timeouts_do_not_overflow() {
    let fake = Fake::centrifugo();
    let client = Client::new(fake.config().request_timeout(Duration::MAX)).unwrap();
    client.info(InfoRequest::new()).await.unwrap();
    client
        .with_timeout(Duration::MAX)
        .info(InfoRequest::new())
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_millis(10);
    assert!(
        cent::wait(deadline, Duration::MAX)
            .await
            .unwrap_err()
            .is_timeout()
    );
    let fails = Fake::new(|_| ok(r#"{"error":{"code":100}}"#));
    let client = Client::new(fails.config()).unwrap();
    let e = client
        .with_timeout(Duration::from_millis(50))
        .broadcast_with_retry(
            cent::BroadcastRequest::new(["a"]).data(raw("1")),
            cent::BroadcastRetryOptions::default()
                .max_attempts(3)
                .on_attempt_error(|_| cent::Retry::After(Duration::MAX)),
        )
        .await
        .unwrap_err();
    assert!(
        e.api_code().is_some() || matches!(e, Error::RetryStopped(_)),
        "{e:?}"
    );
    assert_eq!(cent::Expiry::after(Duration::MAX), cent::Expiry::Never);
}
