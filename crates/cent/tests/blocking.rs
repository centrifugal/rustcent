//! The blocking client behaves as the async one (spec conventions).

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![cfg(feature = "blocking")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cent::{
    AutoBatch, BatchOptions, BroadcastRequest, BroadcastRetryOptions, Error, InfoRequest,
    PublishRequest, blocking,
};
use common::{Answer, Fake, ok, raw};

fn publish(n: u64) -> PublishRequest {
    PublishRequest::new(format!("c{n}")).data(raw("1"))
}

#[test]
fn calls_errors_and_batches() {
    let fake = Fake::new(|r| match r.api_method.as_str() {
        "presence" => ok(r#"{"error":{"code":102,"message":"unknown channel"}}"#),
        "publish" => ok(r#"{"result":{"offset":3}}"#),
        _ => ok(&common::default_reply(r)),
    });
    let client = blocking::Client::new(fake.config()).unwrap();
    assert_eq!(client.publish(publish(1)).unwrap().offset, 3);
    assert_eq!(
        client
            .presence(cent::PresenceRequest::new("c"))
            .unwrap_err()
            .api_code(),
        Some(102)
    );
    assert!(matches!(
        client.publish(PublishRequest::new("c")),
        Err(Error::InvalidRequest(_))
    ));
    let res = client
        .broadcast(BroadcastRequest::new(["a", "b"]).data(raw("1")))
        .unwrap();
    assert_eq!(res.channels.len(), 2);
    let res = client
        .broadcast_with_retry(
            BroadcastRequest::new(["a"]).data(raw("1")),
            BroadcastRetryOptions::default(),
        )
        .unwrap();
    assert!(res.idempotency_key.starts_with("rustcent-"));
    let mut b = client.new_batch(BatchOptions::default());
    let p = b.info(InfoRequest::new());
    assert_eq!(b.len(), 1);
    b.send().unwrap();
    p.result().unwrap();
}

#[test]
fn timeouts_and_close() {
    let fake =
        Fake::new(|_| Answer::After(Duration::from_millis(200), Box::new(ok(r#"{"result":{}}"#))));
    let client = blocking::Client::new(fake.config()).unwrap();
    let e = client
        .with_timeout(Duration::from_millis(20))
        .info(InfoRequest::new())
        .unwrap_err();
    assert!(e.is_timeout());
    client.info(InfoRequest::new()).unwrap();
    let c = client.clone();
    let in_progress = std::thread::spawn(move || c.info(InfoRequest::new()));
    std::thread::sleep(Duration::from_millis(50));
    client.close(Duration::from_secs(5)).unwrap();
    in_progress.join().unwrap().unwrap();
    assert!(matches!(
        client.info(InfoRequest::new()),
        Err(Error::Closed)
    ));

    let deadline = std::time::Instant::now() + Duration::from_millis(20);
    assert!(
        blocking::wait(deadline, Duration::from_secs(5))
            .unwrap_err()
            .is_timeout()
    );
}

/// A config whose requests each take `delay`, and the count of those in
/// flight: a request is counted out however it ends.
fn slow(fake: &Fake, delay: Duration) -> (cent::Config, Arc<AtomicUsize>) {
    struct Counted(Arc<AtomicUsize>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let inner = fake.client();
    let in_flight = Arc::new(AtomicUsize::new(0));
    let n = Arc::clone(&in_flight);
    let http = move |req: http::Request<bytes::Bytes>| {
        n.fetch_add(1, Ordering::SeqCst);
        let counted = Counted(Arc::clone(&n));
        let reply = cent::HttpClient::send(&inner, req);
        async move {
            let _counted = counted;
            tokio::time::sleep(delay).await;
            reply.await
        }
    };
    (
        cent::Config::new("http://centrifugo.test/api").http_client(http),
        in_flight,
    )
}

/// Spec 15's shutdown line for the blocking client: close called again and
/// concurrently waits for the call in progress, a close whose deadline ends
/// first returns its error and the call goes on.
#[test]
fn close_again_concurrently_and_its_deadline() {
    let fake = Fake::centrifugo();
    let (config, in_flight) = slow(&fake, Duration::from_millis(200));
    let client = blocking::Client::new(config).unwrap();
    let c = client.clone();
    let in_progress = std::thread::spawn(move || c.info(InfoRequest::new()));
    std::thread::sleep(Duration::from_millis(30));
    assert!(
        client
            .close(Duration::from_millis(10))
            .unwrap_err()
            .is_timeout()
    );
    assert_eq!(in_flight.load(Ordering::SeqCst), 1, "the call goes on");
    let closes: Vec<_> = (0..4)
        .map(|_| {
            let (c, n) = (client.clone(), Arc::clone(&in_flight));
            std::thread::spawn(move || {
                c.close(Duration::from_secs(5)).unwrap();
                assert_eq!(n.load(Ordering::SeqCst), 0, "close returned early");
            })
        })
        .collect();
    for c in closes {
        c.join().unwrap();
    }
    in_progress.join().unwrap().unwrap();
    assert!(matches!(
        client.info(InfoRequest::new()),
        Err(Error::Closed)
    ));
}

/// Calls queued for an automatic batch are still sent after close, and a
/// batch whose callers left is waited for.
#[test]
fn close_waits_for_automatic_batches() {
    let fake = Fake::new(|r| {
        ok(&if r.api_method == "batch" {
            let n = r.body["commands"].as_array().unwrap().len();
            serde_json::json!({ "replies": vec![serde_json::json!({"publish": {"offset": 1}}); n] })
                .to_string()
        } else {
            r#"{"result":{"offset":1}}"#.to_owned()
        })
    });
    let (config, in_flight) = slow(&fake, Duration::from_millis(150));
    let client = blocking::Client::new(config.auto_batch(AutoBatch::new(1))).unwrap();
    let c = client.clone();
    let first = std::thread::spawn(move || c.publish(publish(0)));
    std::thread::sleep(Duration::from_millis(30));
    // Queued: two whose callers stay, two who leave while their batch is
    // on its way.
    let staying: Vec<_> = (1..=2)
        .map(|n| {
            let c = client.clone();
            std::thread::spawn(move || c.publish(publish(n)))
        })
        .collect();
    let leaving: Vec<_> = (3..=4)
        .map(|n| {
            let c = client.with_timeout(Duration::from_millis(200));
            std::thread::spawn(move || c.publish(publish(n)))
        })
        .collect();
    std::thread::sleep(Duration::from_millis(30));
    client.close(Duration::from_secs(5)).unwrap();
    assert_eq!(in_flight.load(Ordering::SeqCst), 0, "close returned early");
    first.join().unwrap().unwrap();
    for t in staying {
        t.join().unwrap().unwrap();
    }
    for t in leaving {
        assert!(t.join().unwrap().unwrap_err().is_timeout());
    }
    let methods: Vec<String> = fake
        .requests()
        .iter()
        .map(|r| r.api_method.clone())
        .collect();
    assert_eq!(methods, ["publish", "batch"]);
}

/// A broadcast retry stops at its next attempt after close, with `Closed`.
#[test]
fn close_stops_a_broadcast_retry() {
    let fake = Fake::new(|r| {
        let n = r.body["channels"].as_array().map_or(0, Vec::len);
        let failed =
            serde_json::json!({"error": {"code": 100, "message": "failed", "temporary": true}});
        ok(&serde_json::json!({"result": {"responses": vec![failed; n]}}).to_string())
    });
    let client = blocking::Client::new(fake.config()).unwrap();
    let closer = Arc::new(Mutex::new(None));
    let (c, cl) = (client.clone(), Arc::clone(&closer));
    let e = client
        .broadcast_with_retry(
            BroadcastRequest::new(["a"]).data(raw("1")),
            BroadcastRetryOptions::default()
                .max_attempts(10)
                .on_attempt_error(move |_| {
                    let c = c.clone();
                    *cl.lock().unwrap() =
                        Some(std::thread::spawn(move || c.close(Duration::from_secs(5))));
                    cent::Retry::After(Duration::from_millis(100))
                }),
        )
        .unwrap_err();
    let res = e.broadcast_result().unwrap();
    assert!(res.attempts <= 2, "{} attempts after close", res.attempts);
    assert!(matches!(res.channels[0].result, Err(Error::Closed)));
    assert!(!cent::retryable(&e));
    assert_eq!(fake.requests().len(), 1);
    let closer = closer.lock().unwrap().take().unwrap();
    closer.join().unwrap().unwrap();
}

#[test]
fn from_many_threads_with_automatic_batching() {
    let fake = Fake::new(|r| {
        Answer::After(
            Duration::from_millis(5),
            Box::new(ok(&if r.api_method == "batch" {
                let n = r.body["commands"].as_array().unwrap().len();
                serde_json::json!({ "replies": vec![serde_json::json!({"publish": {"offset": 1}}); n] }).to_string()
            } else {
                r#"{"result":{"offset":1}}"#.to_owned()
            })),
        )
    });
    let client = blocking::Client::new(fake.config().auto_batch(AutoBatch::new(2))).unwrap();
    let threads: Vec<_> = (0..16)
        .map(|n| {
            let c = client.clone();
            std::thread::spawn(move || {
                for i in 0..5 {
                    c.publish(publish(n * 10 + i)).unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert!(
        fake.requests().iter().any(|r| r.api_method == "batch"),
        "concurrent calls were batched"
    );
}

#[tokio::test]
async fn usable_from_async_code_too() {
    // It blocks the thread, as any blocking call does, but does not panic.
    let client = blocking::Client::new(Fake::centrifugo().config()).unwrap();
    client.info(InfoRequest::new()).unwrap();
    drop(client); // dropping it inside a runtime neither blocks nor panics
}

#[test]
fn a_panic_in_a_hook_is_the_callers() {
    let client = blocking::Client::new(
        Fake::centrifugo()
            .config()
            .on_request(|_| panic!("hook panicked")),
    )
    .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        client.info(InfoRequest::new())
    }));
    assert_eq!(
        *panic.unwrap_err().downcast::<&str>().unwrap(),
        "hook panicked"
    );
    // The client goes on working.
    let client = blocking::Client::new(Fake::centrifugo().config()).unwrap();
    client.info(InfoRequest::new()).unwrap();
}
