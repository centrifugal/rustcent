//! The blocking client behaves as the async one (spec conventions).

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![cfg(feature = "blocking")]

mod common;

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
