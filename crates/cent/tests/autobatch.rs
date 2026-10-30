//! Automatic batching (spec 8): sent alone below the cap, batched at it,
//! every call its own reply, deadlines kept, and a randomized stress test.

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![cfg(feature = "client")]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cent::{AutoBatch, Client, Error, PublishRequest};
use common::{Answer, Fake, Recorded, ok, raw};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

fn publish(n: u64) -> PublishRequest {
    PublishRequest::new(format!("c{n}")).data(raw("1"))
}

/// The offset a publication into "c{n}" gets: n, so that a caller can tell
/// its own reply.
fn reply_for(r: &Recorded) -> String {
    let offset = |req: &Value| {
        req["channel"].as_str().unwrap()[1..]
            .parse::<u64>()
            .unwrap()
    };
    if r.api_method == "batch" {
        let replies: Vec<Value> = r.body["commands"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| json!({"publish": {"offset": offset(&c["publish"])}}))
            .collect();
        json!({ "replies": replies }).to_string()
    } else {
        json!({"result": {"offset": offset(&r.body)}}).to_string()
    }
}

/// A fake holding each request until the test releases it, counting the
/// requests in flight.
struct Gate {
    fake: Fake,
    open: Arc<Semaphore>,
    in_flight: Arc<AtomicUsize>,
    max_seen: Arc<AtomicUsize>,
}

fn gate() -> Gate {
    Gate {
        fake: Fake::new(|r| ok(&reply_for(r))),
        open: Arc::new(Semaphore::new(0)),
        in_flight: Arc::new(AtomicUsize::new(0)),
        max_seen: Arc::new(AtomicUsize::new(0)),
    }
}

impl Gate {
    fn config(&self, max_in_flight: usize) -> cent::Config {
        let inner = self.fake.client();
        let (open, in_flight, max_seen) = (
            Arc::clone(&self.open),
            Arc::clone(&self.in_flight),
            Arc::clone(&self.max_seen),
        );
        let client = move |req: http::Request<bytes::Bytes>| {
            let n = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            max_seen.fetch_max(n, Ordering::SeqCst);
            let (open, in_flight) = (Arc::clone(&open), Arc::clone(&in_flight));
            let reply = cent::HttpClient::send(&inner, req);
            async move {
                open.acquire().await.unwrap().forget();
                in_flight.fetch_sub(1, Ordering::SeqCst);
                reply.await
            }
        };
        cent::Config::new("http://centrifugo.test/api")
            .http_client(client)
            .auto_batch(AutoBatch::new(max_in_flight))
    }
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(30)).await;
}

#[tokio::test]
async fn below_the_cap_calls_go_alone_at_once() {
    let fake = Fake::new(|r| ok(&reply_for(r)));
    let client = Client::new(fake.config().auto_batch(AutoBatch::new(4))).unwrap();
    for n in 1..=3 {
        assert_eq!(client.publish(publish(n)).await.unwrap().offset, n);
    }
    let paths: Vec<String> = fake
        .requests()
        .iter()
        .map(|r| r.api_method.clone())
        .collect();
    assert_eq!(
        paths,
        ["publish", "publish", "publish"],
        "calls one after another never batch"
    );
}

#[tokio::test]
async fn at_the_cap_calls_queue_and_go_together() {
    let g = gate();
    let client = Client::new(g.config(1)).unwrap();
    let first = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(100)).await }
    });
    settle().await;
    let queued: Vec<_> = (1..=5)
        .map(|n| {
            let c = client.clone();
            tokio::spawn(async move { c.publish(publish(n)).await })
        })
        .collect();
    settle().await;
    assert_eq!(
        g.fake.requests().len(),
        1,
        "nothing waits for a timer: the queued calls wait for a slot"
    );
    g.open.add_permits(10);
    assert_eq!(first.await.unwrap().unwrap().offset, 100);
    for (n, q) in (1..=5).zip(queued) {
        assert_eq!(
            q.await.unwrap().unwrap().offset,
            n,
            "each call gets its own reply"
        );
    }
    let reqs = g.fake.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[1].api_method, "batch");
    assert_eq!(reqs[1].body["parallel"], true);
    assert_eq!(reqs[1].body["commands"].as_array().unwrap().len(), 5);
    assert_eq!(
        g.max_seen.load(Ordering::SeqCst),
        1,
        "requests in flight never exceed the cap"
    );
}

#[tokio::test]
async fn batches_are_capped() {
    let g = gate();
    let client = Client::new(g.config(1).auto_batch(AutoBatch::new(1).max_batch_size(2))).unwrap();
    let mut calls = vec![tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(0)).await }
    })];
    settle().await;
    for n in 1..=5 {
        let c = client.clone();
        calls.push(tokio::spawn(async move { c.publish(publish(n)).await }));
    }
    settle().await;
    g.open.add_permits(100);
    for (n, c) in calls.into_iter().enumerate() {
        assert_eq!(c.await.unwrap().unwrap().offset, n as u64);
    }
    let sizes: Vec<usize> = g
        .fake
        .requests()
        .iter()
        .skip(1)
        .map(|r| r.body["commands"].as_array().unwrap().len())
        .collect();
    assert_eq!(sizes, [2, 2, 1]);
}

#[tokio::test]
async fn one_command_batches_still_go_to_batch() {
    let g = gate();
    let client = Client::new(g.config(1)).unwrap();
    let first = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(0)).await }
    });
    settle().await;
    let second = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(1)).await }
    });
    settle().await;
    g.open.add_permits(10);
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(g.fake.requests()[1].api_method, "batch");
}

#[tokio::test]
async fn a_queued_call_whose_deadline_ends_is_not_sent() {
    let g = gate();
    let client = Client::new(g.config(1)).unwrap();
    let first = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(0)).await }
    });
    settle().await;
    let e = client
        .with_timeout(Duration::from_millis(20))
        .publish(publish(1))
        .await
        .unwrap_err();
    assert!(e.is_timeout());
    assert!(e.to_string().contains("before it was sent"), "{e}");
    // A dropped call leaves the queue too.
    let dropped = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(2)).await }
    });
    settle().await;
    dropped.abort();
    settle().await;
    g.open.add_permits(10);
    first.await.unwrap().unwrap();
    settle().await;
    assert_eq!(g.fake.requests().len(), 1, "neither left call was sent");
    // The slot was passed on and freed: a new call goes alone.
    let c = client.clone();
    let next = tokio::spawn(async move { c.publish(publish(3)).await });
    g.open.add_permits(1);
    assert_eq!(next.await.unwrap().unwrap().offset, 3);
    assert_eq!(g.fake.requests()[1].api_method, "publish");
}

#[tokio::test]
async fn a_batch_failing_as_a_whole_fails_its_calls() {
    // The first call is held until the others queued; the batch carrying
    // them is refused as a whole.
    let release = Arc::new(Semaphore::new(0));
    let r2 = Arc::clone(&release);
    let fake = Fake::new(|r| {
        if r.api_method == "batch" {
            Answer::Json(413, "Request Entity Too Large".into())
        } else {
            ok(&reply_for(r))
        }
    });
    let inner = fake.client();
    let held = move |req: http::Request<bytes::Bytes>| {
        let first = !req.uri().path().ends_with("/batch");
        let reply = cent::HttpClient::send(&inner, req);
        let r2 = Arc::clone(&r2);
        async move {
            if first {
                r2.acquire().await.unwrap().forget();
            }
            reply.await
        }
    };
    let client = Client::new(
        cent::Config::new("http://centrifugo.test/api")
            .http_client(held)
            .auto_batch(AutoBatch::new(1)),
    )
    .unwrap();
    let first = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(0)).await }
    });
    settle().await;
    let queued: Vec<_> = (1..=3)
        .map(|n| {
            let c = client.clone();
            tokio::spawn(async move { c.publish(publish(n)).await })
        })
        .collect();
    settle().await;
    release.add_permits(1);
    first.await.unwrap().unwrap();
    for q in queued {
        let e = q.await.unwrap().unwrap_err();
        assert!(matches!(&e, Error::Http(h) if h.status == 413), "{e:?}");
    }
    let methods: Vec<String> = fake
        .requests()
        .iter()
        .map(|r| r.api_method.clone())
        .collect();
    assert_eq!(methods, ["publish", "batch"], "no call retried alone");
}

#[tokio::test]
async fn errors_of_commands_in_automatic_batches() {
    let fake = Fake::new(|r| {
        if r.api_method == "batch" {
            ok(r#"{"replies":[{"error":{"code":102,"message":"unknown channel"}}]}"#)
        } else {
            Answer::After(Duration::from_millis(50), Box::new(ok(r#"{"result":{}}"#)))
        }
    });
    let client = Client::new(fake.config().auto_batch(AutoBatch::new(1))).unwrap();
    let first = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(0)).await }
    });
    settle().await;
    let e = client.publish(publish(1)).await.unwrap_err();
    assert_eq!(e.api_code(), Some(102));
    first.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_waits_for_queued_calls_and_their_batch() {
    let fake = Fake::new(|r| Answer::After(Duration::from_millis(40), Box::new(ok(&reply_for(r)))));
    let client = Client::new(fake.config().auto_batch(AutoBatch::new(1))).unwrap();
    let calls: Vec<_> = (0..4)
        .map(|n| {
            let c = client.clone();
            tokio::spawn(async move { c.publish(publish(n)).await })
        })
        .collect();
    settle().await;
    client.close(Duration::from_secs(5)).await.unwrap();
    for c in calls {
        assert!(c.is_finished());
        c.await.unwrap().unwrap();
    }
}

/// Random concurrency: tasks making calls one after another, some timing out
/// or being dropped, against a fake answering after a random wait. Every
/// call gets its own reply, requests in flight never exceed the cap, and
/// nothing is left behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress() {
    let rounds: usize = std::env::var("RUSTCENT_STRESS_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    for round in 0..rounds {
        let max_in_flight = 1 + round % 4;
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let seed = Arc::new(Mutex::new(round as u64 * 7919 + 1));
        let fake = Fake::new(|r| ok(&reply_for(r)));
        let inner = fake.client();
        let (i2, m2, s2) = (
            Arc::clone(&in_flight),
            Arc::clone(&max_seen),
            Arc::clone(&seed),
        );
        let http = move |req: http::Request<bytes::Bytes>| {
            let n = i2.fetch_add(1, Ordering::SeqCst) + 1;
            m2.fetch_max(n, Ordering::SeqCst);
            // Counted out however the request ends: a call's timeout drops it.
            let counted = Counted(Arc::clone(&i2));
            let wait = Duration::from_micros(next(&s2) % 2000);
            let reply = cent::HttpClient::send(&inner, req);
            async move {
                let _counted = counted;
                tokio::time::sleep(wait).await;
                reply.await
            }
        };
        let client = Client::new(
            cent::Config::new("http://centrifugo.test/api")
                .http_client(http)
                .auto_batch(AutoBatch::new(max_in_flight).max_batch_size(3)),
        )
        .unwrap();
        let mut tasks = Vec::new();
        for t in 0..16u64 {
            let client = client.clone();
            let seed = Arc::clone(&seed);
            tasks.push(tokio::spawn(async move {
                for i in 0..10u64 {
                    let n = t * 1000 + i;
                    let r = next(&seed) % 10;
                    if r == 0 {
                        // A call dropped at a random point.
                        let c = client.clone();
                        let call = tokio::spawn(async move { c.publish(publish(n)).await });
                        tokio::time::sleep(Duration::from_micros(next(&seed) % 500)).await;
                        call.abort();
                        continue;
                    }
                    let c = if r == 1 {
                        client.with_timeout(Duration::from_micros(300))
                    } else {
                        client.clone()
                    };
                    match c.publish(publish(n)).await {
                        Ok(res) => assert_eq!(res.offset, n, "a reply which is not the call's own"),
                        Err(e) => assert!(r == 1 && e.is_timeout(), "{e:?}"),
                    }
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        client.close(Duration::from_secs(10)).await.unwrap();
        assert!(
            max_seen.load(Ordering::SeqCst) <= max_in_flight,
            "round {round}: over the cap"
        );
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
    }
}

/// A request in flight, counted out when dropped.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn next(seed: &Mutex<u64>) -> u64 {
    let mut s = seed.lock().unwrap();
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

/// A queued call whose deadline passed while it waited is not sent, even when
/// the request ahead of it ends at that same moment and passes its slot on:
/// its caller reports it unsent.
#[tokio::test]
async fn a_queued_call_past_its_deadline_is_not_sent() {
    let g = gate();
    let client = Client::new(g.config(1)).unwrap();
    // One deadline for both: the documented way to share one.
    let shared = client.with_deadline(std::time::Instant::now() + Duration::from_millis(100));
    let first = tokio::spawn({
        let c = shared.clone();
        async move { c.publish(publish(0)).await }
    });
    settle().await;
    let e = shared.publish(publish(1)).await.unwrap_err();
    assert!(matches!(&e, Error::Timeout(t) if t.unsent()), "{e:?}");
    assert!(first.await.unwrap().unwrap_err().is_timeout());
    settle().await;
    let methods: Vec<String> = g
        .fake
        .requests()
        .iter()
        .map(|r| r.api_method.clone())
        .collect();
    assert_eq!(methods, ["publish"], "the queued call was never sent");
}

/// A batch whose task dies - here, `on_request` panicking - fails its calls at
/// once, rather than leaving them to wait for their deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_batch_fails_its_calls_at_once() {
    let g = gate();
    let client = Client::new(
        g.config(1)
            .on_request(|i| assert!(!i.auto_batch, "a hook panicking")),
    )
    .unwrap();
    let first = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(0)).await }
    });
    settle().await;
    let start = std::time::Instant::now();
    let queued = tokio::spawn({
        let c = client.with_timeout(Duration::from_secs(30));
        async move { c.publish(publish(1)).await }
    });
    settle().await;
    g.open.add_permits(10);
    first.await.unwrap().unwrap();
    let e = queued.await.unwrap().unwrap_err();
    assert!(matches!(e, Error::Network(_)), "{e:?}");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "{:?}",
        start.elapsed()
    );
}

/// A batch timing out as a whole gives each call a timeout naming its own
/// method.
#[tokio::test]
async fn a_batch_timeout_is_each_calls_own() {
    let g = gate();
    let client = Client::new(g.config(1).request_timeout(Duration::from_millis(100))).unwrap();
    let first = tokio::spawn({
        let c = client.clone();
        async move { c.publish(publish(0)).await }
    });
    settle().await;
    let queued = tokio::spawn({
        let c = client.with_timeout(Duration::from_secs(5));
        async move { c.publish(publish(1)).await }
    });
    settle().await;
    // The first times out, passing its slot to a batch which hangs too.
    assert!(first.await.unwrap().unwrap_err().is_timeout());
    let e = queued.await.unwrap().unwrap_err();
    assert!(
        matches!(&e, Error::Timeout(t) if t.method() == Some("publish") && !t.unsent()),
        "{e:?}"
    );
}
