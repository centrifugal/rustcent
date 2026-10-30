//! The HTTP handlers: proxy, endpoints for client SDKs, JWKS (spec 11.3, 12,
//! 14), through their framework-free core and as tower services.

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use cent::endpoints::{self, SignedKeys, Unauthorized};
use cent::proxy::{self, Rejection};
use cent::{BoxError, Handler};
use serde_json::{Value, json};

fn request(method: &str, uri: &str, body: &str) -> http::Request<Bytes> {
    http::Request::builder()
        .method(method)
        .uri(uri)
        .body(Bytes::from(body.to_owned()))
        .unwrap()
}

async fn call(h: &Handler, method: &str, body: &str) -> (u16, http::HeaderMap, String) {
    let r = h.handle(request(method, "/h", body)).await;
    (
        r.status().as_u16(),
        r.headers().clone(),
        String::from_utf8(r.body().to_vec()).unwrap(),
    )
}

fn json_of(s: &str) -> Value {
    serde_json::from_str(s).unwrap()
}

type Errors = Arc<Mutex<Vec<String>>>;

fn proxies() -> (proxy::Handlers, Errors) {
    let errors: Errors = Arc::default();
    let e2 = Arc::clone(&errors);
    (
        proxy::Handlers::new().on_error(move |_, e| e2.lock().unwrap().push(e.to_string())),
        errors,
    )
}

#[tokio::test]
async fn proxy_answers_exactly_one_of_result_error_and_disconnect() {
    let (h, errors) = proxies();
    let connect = h.connect(|parts, req| async move {
        assert_eq!(parts.headers["x-forwarded"], "1");
        match req.client.as_str() {
            "reject" => Err(proxy::Error::new(403, "permission denied").into()),
            "close" => Err(proxy::Disconnect::new(4501, "banned").into()),
            "fail" => Err(Rejection::fail("database down")),
            "io" => Err(std::io::Error::other("io down").into()),
            _ => {
                let info = req
                    .data
                    .clone()
                    .unwrap_or_else(|| cent::RawValue::from_string("null".into()).unwrap());
                Ok(proxy::ConnectResult::new().user("42").info(info))
            }
        }
    });
    let send = |client: &str, data: &str| {
        let mut r = request(
            "POST",
            "/connect",
            &format!(
                r#"{{"client":"{client}","transport":"websocket","protocol":"json","encoding":"json","data":{data}}}"#
            ),
        );
        r.headers_mut().insert("x-forwarded", "1".parse().unwrap());
        let connect = connect.clone();
        async move {
            let r = connect.handle(r).await;
            (
                r.status().as_u16(),
                String::from_utf8(r.body().to_vec()).unwrap(),
            )
        }
    };
    let (status, body) = send("ok", r#"{"a":1.0}"#).await;
    assert_eq!(status, 200);
    // The payload is carried exactly: number text kept.
    assert_eq!(body, r#"{"result":{"user":"42","info":{"a":1.0}}}"#);
    assert_eq!(
        json_of(&send("reject", "1").await.1),
        json!({"error": {"code": 403, "message": "permission denied"}})
    );
    assert_eq!(
        json_of(&send("close", "1").await.1),
        json!({"disconnect": {"code": 4501, "reason": "banned"}})
    );
    assert_eq!(send("fail", "1").await.0, 500);
    assert_eq!(send("io", "1").await.0, 500);
    assert_eq!(
        *errors.lock().unwrap(),
        ["connect: database down", "connect: io down"]
    );

    // An empty result still accepts: {"result":{}}.
    let sub = h.subscribe(|_, _| async { Ok(proxy::SubscribeResult::new()) });
    assert_eq!(
        call(&sub, "POST", r#"{"channel":"c"}"#).await.2,
        r#"{"result":{}}"#
    );
}

#[tokio::test]
async fn proxy_types_refuse_answers_they_cannot_carry() {
    let (h, errors) = proxies();
    let refresh = h.refresh(|_, _| async { Err(proxy::Error::new(400, "no").into()) });
    assert_eq!(call(&refresh, "POST", "{}").await.0, 500);
    let refresh = h.refresh(|_, _| async { Ok(proxy::RefreshResult::new().expired(true)) });
    assert_eq!(
        json_of(&call(&refresh, "POST", "{}").await.2),
        json!({"result": {"expired": true}})
    );
    let poll =
        h.shared_poll_refresh(|_, _| async { Err(proxy::Disconnect::new(4000, "x").into()) });
    assert_eq!(call(&poll, "POST", "{}").await.0, 500);
    let poll = h.shared_poll_refresh(|_, _| async { Err(proxy::Error::new(1000, "x").into()) });
    assert_eq!(
        json_of(&call(&poll, "POST", "{}").await.2),
        json!({"error": {"code": 1000, "message": "x"}})
    );
    assert_eq!(errors.lock().unwrap().len(), 2);
    assert!(errors.lock().unwrap()[0].contains("cannot answer with an error"));
}

#[tokio::test]
async fn proxy_requests() {
    let (h, errors) = proxies();
    let rpc = h.rpc(|_, req| async move { Ok(proxy::RpcResult::new().data(req.data.unwrap())) });
    let (status, headers, _) = call(&rpc, "GET", "").await;
    assert_eq!((status, headers["allow"].to_str().unwrap()), (405, "POST"));
    assert_eq!(call(&rpc, "POST", "not json").await.0, 400);
    assert_eq!(
        errors.lock().unwrap().len(),
        1,
        "malformed requests are reported"
    );
    // Binary mode: payloads arrive in the b64 fields.
    let publish = h.publish(|_, req| async move {
        assert_eq!(
            (req.b64data.as_str(), req.encoding.as_str()),
            ("aGk=", "binary")
        );
        Ok(proxy::PublishResult::new().b64data(req.b64data))
    });
    let (_, _, body) = call(
        &publish,
        "POST",
        r#"{"channel":"c","b64data":"aGk=","encoding":"binary"}"#,
    )
    .await;
    assert_eq!(json_of(&body), json!({"result": {"b64data": "aGk="}}));
    // The body limit.
    let small = proxy::Handlers::new()
        .max_request_bytes(10)
        .rpc(|_, _| async { Ok(proxy::RpcResult::new()) });
    assert_eq!(
        call(&small, "POST", r#"{"method":"long enough"}"#).await.0,
        400
    );
}

#[tokio::test]
async fn token_endpoints() {
    let errors: Errors = Arc::default();
    let e2 = Arc::clone(&errors);
    let h = endpoints::Handlers::new().on_error(move |_, e| e2.lock().unwrap().push(e.to_string()));
    let conn = h.connection_token(|parts| async move {
        match parts
            .headers
            .get("x-user")
            .map(|v| v.to_str().unwrap().to_owned())
        {
            Some(u) if u == "bad" => Err("token service down".into()),
            Some(u) => Ok(format!("token-of-{u}")),
            None => Err(Box::new(std::io::Error::other(Unauthorized)) as BoxError),
        }
    });
    let mut r = request("GET", "/t", "");
    r.headers_mut().insert("x-user", "42".parse().unwrap());
    let resp = conn.handle(r).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers()["cache-control"], "no-store");
    assert_eq!(resp.headers()["content-type"], "application/json");
    assert_eq!(resp.body().as_ref(), br#"{"token":"token-of-42"}"#);
    assert_eq!(
        call(&conn, "POST", "ignored").await.0,
        403,
        "Unauthorized, also wrapped"
    );
    let mut r = request("POST", "/t", "");
    r.headers_mut().insert("x-user", "bad".parse().unwrap());
    assert_eq!(conn.handle(r).await.status(), 500);
    let (status, headers, _) = call(&conn, "PUT", "").await;
    assert_eq!(
        (status, headers["allow"].to_str().unwrap()),
        (405, "GET, POST")
    );

    let sub = h.subscription_token(|_, channel| async move { Ok(format!("token-for-{channel}")) });
    let r = sub.handle(request("GET", "/s?channel=chat%3A1", "")).await;
    assert_eq!(r.body().as_ref(), br#"{"token":"token-for-chat:1"}"#);
    assert_eq!(
        call(&sub, "POST", r#"{"channel":"news"}"#).await.2,
        r#"{"token":"token-for-news"}"#
    );
    assert_eq!(
        call(&sub, "POST", r#"{}"#).await.0,
        400,
        "a channel is required"
    );
    assert_eq!(call(&sub, "POST", "nope").await.0, 400);
    assert_eq!(errors.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn shared_poll_signature_endpoint() {
    let h = endpoints::Handlers::new();
    let sign = h.shared_poll_signature(|_, channel, keys| async move {
        assert_eq!(channel, "poll:prices");
        if keys.iter().any(|k| k == "stranger") {
            return Ok(SignedKeys::new(vec!["other".to_owned()], "sig"));
        }
        let allowed: Vec<String> = keys.into_iter().filter(|k| k != "secret").collect();
        Ok(SignedKeys::new(allowed, "sig"))
    });
    let (status, headers, body) = call(
        &sign,
        "POST",
        r#"{"channel":"poll:prices","keys":["AAPL","secret"]}"#,
    )
    .await;
    assert_eq!(
        (status, headers["cache-control"].to_str().unwrap()),
        (200, "no-store")
    );
    assert_eq!(
        json_of(&body),
        json!({"signature": "sig", "keys": ["AAPL"]}),
        "left out keys are revoked"
    );
    let (_, _, body) = call(
        &sign,
        "POST",
        r#"{"channel":"poll:prices","keys":["secret"]}"#,
    )
    .await;
    assert_eq!(
        json_of(&body),
        json!({"signature": "sig", "keys": []}),
        "keys is always an array"
    );
    let (status, _, _) = call(
        &sign,
        "POST",
        r#"{"channel":"poll:prices","keys":["stranger"]}"#,
    )
    .await;
    assert_eq!(status, 500, "a key the client did not ask for");
    assert_eq!(call(&sign, "POST", r#"{"keys":[]}"#).await.0, 400);
    assert_eq!(call(&sign, "GET", "").await.0, 405);
}

#[cfg(feature = "token")]
#[tokio::test]
async fn jwks_endpoint() {
    let key = cent::token::Key::ed25519(&[1; 32])
        .unwrap()
        .with_key_id("k1");
    let h = endpoints::jwks(std::slice::from_ref(&key)).unwrap();
    let (status, headers, body) = call(&h, "GET", "").await;
    assert_eq!(status, 200);
    assert_eq!(headers["cache-control"], "public, max-age=300");
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(json_of(&body)["keys"][0]["kid"], "k1");
    let (status, _, body) = call(&h, "HEAD", "").await;
    assert_eq!((status, body.as_str()), (200, ""));
    assert_eq!(call(&h, "POST", "").await.0, 405);
    // Refused when built, not when called.
    assert!(endpoints::jwks(&[]).is_err());
    assert!(endpoints::jwks(&[key.clone(), key]).is_err());
}

#[cfg(feature = "tower")]
#[tokio::test]
async fn as_a_tower_service() {
    use http_body_util::{BodyExt, Full};
    use tower_service::Service;

    let mut h = proxy::Handlers::new()
        .max_request_bytes(64)
        .subscribe(
            |_, req| async move { Ok(proxy::SubscribeResult::new().data(req.data.unwrap())) },
        );
    let req = http::Request::post("/sub")
        .body(Full::new(Bytes::from(r#"{"channel":"c","data":[1]}"#)))
        .unwrap();
    let resp = h.call(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), br#"{"result":{"data":[1]}}"#);
    let big = http::Request::post("/sub")
        .body(Full::new(Bytes::from(vec![b' '; 100])))
        .unwrap();
    assert_eq!(h.call(big).await.unwrap().status(), 400, "past the limit");
}
