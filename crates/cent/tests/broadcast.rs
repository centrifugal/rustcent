//! Broadcasts, broadcasts with retries, and batches (spec 7).

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![cfg(feature = "client")]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cent::{
    BatchOptions, BroadcastRequest, BroadcastRetryOptions, Client, Error, PresenceStatsRequest,
    PublishRequest, Retry, retryable,
};
use common::{Answer, Fake, ok, raw};
use serde_json::{Value, json};

fn broadcast(channels: &[&str]) -> BroadcastRequest {
    BroadcastRequest::new(channels.iter().copied()).data(raw(r#"{"text":"hi"}"#))
}

/// A fake whose broadcast answers each channel as `outcome` says: `Ok` with
/// its offset, or an error code.
fn fake_broadcast(
    outcome: impl Fn(&str, usize) -> Result<u64, (u32, bool)> + Send + Sync + 'static,
) -> Fake {
    let calls = Arc::new(Mutex::new(0));
    Fake::new(move |r| {
        let call = {
            let mut c = calls.lock().unwrap();
            *c += 1;
            *c
        };
        let req = if r.api_method == "batch" {
            r.body["commands"][0]["broadcast"].clone()
        } else {
            r.body.clone()
        };
        let responses: Vec<Value> = req["channels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|ch| match outcome(ch.as_str().unwrap(), call) {
                Ok(offset) => json!({"result": {"offset": offset, "epoch": "e"}}),
                Err((code, temporary)) => {
                    json!({"error": {"code": code, "message": "failed", "temporary": temporary}})
                }
            })
            .collect();
        let result = json!({ "responses": responses });
        if r.api_method == "batch" {
            ok(&json!({"replies": [{"broadcast": result}]}).to_string())
        } else {
            ok(&json!({ "result": result }).to_string())
        }
    })
}

#[tokio::test]
async fn every_channel_has_an_outcome() {
    let fake = fake_broadcast(|ch, _| {
        if ch == "bad" {
            Err((102, false))
        } else {
            Ok(1)
        }
    });
    let client = Client::new(fake.config()).unwrap();
    let res = client
        .broadcast(broadcast(&["a", "b"]).idempotency_key("k"))
        .await
        .unwrap();
    assert!(res.replied);
    assert_eq!(res.idempotency_key, "k");
    assert_eq!(
        res.channels
            .iter()
            .map(|c| c.channel.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(res.failed().is_empty());

    // Partial: the error holds every channel; it does not match its parts.
    let e = client
        .broadcast(broadcast(&["a", "bad", "c"]))
        .await
        .unwrap_err();
    let Error::BroadcastFailedChannels(be) = &e else {
        panic!("{e:?}")
    };
    assert_eq!(be.total(), 3);
    assert_eq!(be.failed().len(), 1);
    assert_eq!(be.failed()[0].channel, "bad");
    let errors: Vec<_> = be.errors().map(|(ch, e)| (ch, e.api_code())).collect();
    assert_eq!(errors, [("bad", Some(102))]);
    assert!(be.result().channels[0].error().is_none());
    assert_eq!(
        be.result().channels[1].error().unwrap().api_code(),
        Some(102)
    );
    assert_eq!(
        e.api_code(),
        None,
        "a partial error is not its parts' error"
    );
    assert!(be.result().replied, "replied agrees with the error");
    assert!(be.retryable_channels().is_empty());
    assert!(!retryable(&e));
    assert_eq!(
        e.to_string(),
        "broadcast: 1 of 3 channels failed, 2 got the publication: bad: failed (code 102)"
    );
    // Also with one channel, and with every channel failing.
    let e = client.broadcast(broadcast(&["bad"])).await.unwrap_err();
    assert!(matches!(e, Error::BroadcastFailedChannels(_)) && e.api_code().is_none());
}

#[tokio::test]
async fn whole_failures_are_their_own_error() {
    let fake = Fake::new(|_| Answer::Json(503, "unavailable".into()));
    let client = Client::new(fake.config()).unwrap();
    let e = client.broadcast(broadcast(&["a", "b"])).await.unwrap_err();
    assert!(matches!(e, Error::Http(_)));
    assert!(e.broadcast_result().is_none(), "no channel got a reply");
    // A reply count different from the channels' is not Centrifugo's.
    let fake = Fake::new(|_| ok(r#"{"result":{"responses":[{"result":{}}]}}"#));
    let e = Client::new(fake.config())
        .unwrap()
        .broadcast(broadcast(&["a", "b"]))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Decode(_)), "{e:?}");
}

#[tokio::test]
async fn retryable_channels_and_messages() {
    let fake = fake_broadcast(|ch, _| match ch {
        "busy" => Err((111, true)),
        "bad" => Err((102, false)),
        _ => Ok(1),
    });
    let e = Client::new(fake.config())
        .unwrap()
        .broadcast(broadcast(&["busy", "bad", "ok", "busy"]))
        .await
        .unwrap_err();
    let Error::BroadcastFailedChannels(be) = &e else {
        panic!()
    };
    assert_eq!(be.retryable_channels(), ["busy", "busy"]);
    assert!(retryable(&e), "a retryable part makes it retryable");
    assert!(
        e.to_string()
            .ends_with("(retry only retryable_channels(), with the same idempotency_key)"),
        "{e}"
    );
}

#[tokio::test]
async fn retries_only_the_channels_which_may_succeed() {
    // "flaky" fails temporarily on the first attempt, "bad" for good.
    let fake = fake_broadcast(|ch, call| match (ch, call) {
        ("flaky", 1) => Err((100, true)),
        ("bad", _) => Err((102, false)),
        (_, n) => Ok(n as u64),
    });
    let client = Client::new(fake.config()).unwrap();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&attempts);
    let opts = BroadcastRetryOptions::default()
        .max_attempts(3)
        .on_attempt_error(move |a| {
            seen.lock().unwrap().push((
                a.attempt,
                a.retry
                    .iter()
                    .map(|c| c.channel.clone())
                    .collect::<Vec<_>>(),
            ));
            Retry::After(Duration::from_millis(1))
        });
    let e = client
        .broadcast_with_retry(broadcast(&["ok", "flaky", "bad"]), opts)
        .await
        .unwrap_err();
    let reqs = fake.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(
        reqs[1].body["channels"],
        json!(["flaky"]),
        "only the channel which may succeed"
    );
    let key = reqs[0].body["idempotency_key"].as_str().unwrap();
    assert!(key.starts_with("rustcent-"), "a key made for the call");
    assert_eq!(
        reqs[1].body["idempotency_key"], key,
        "the same key on every attempt"
    );
    assert_eq!(
        *attempts.lock().unwrap(),
        vec![(1, vec!["flaky".to_owned()])]
    );

    let Error::BroadcastFailedChannels(be) = e else {
        panic!()
    };
    let res = be.result();
    assert_eq!(res.idempotency_key, key);
    assert_eq!(res.attempts, 2);
    let by: Vec<(&str, u32, bool)> = res
        .channels
        .iter()
        .map(|c| (c.channel.as_str(), c.attempts, c.result.is_ok()))
        .collect();
    assert_eq!(by, [("ok", 1, true), ("flaky", 2, true), ("bad", 1, false)]);
    assert_eq!(
        res.channels[1].result.as_ref().unwrap().offset,
        2,
        "the outcome of its last attempt"
    );
}

#[tokio::test]
async fn retry_until_success_and_stops() {
    let fake = fake_broadcast(|_, call| if call < 3 { Err((100, true)) } else { Ok(9) });
    let client = Client::new(fake.config()).unwrap();
    let res = client
        .broadcast_with_retry(
            broadcast(&["a"]).idempotency_key("mine"),
            BroadcastRetryOptions::default()
                .max_attempts(5)
                .on_attempt_error(|_| Retry::After(Duration::ZERO)),
        )
        .await
        .unwrap();
    assert_eq!((res.attempts, res.idempotency_key.as_str()), (3, "mine"));

    // The callback stops it: both errors are kept.
    let fake = fake_broadcast(|_, _| Err((100, true)));
    let client = Client::new(fake.config()).unwrap();
    let e = client
        .broadcast_with_retry(
            broadcast(&["a"]),
            BroadcastRetryOptions::default()
                .max_attempts(5)
                .on_attempt_error(|_| Retry::stop("enough")),
        )
        .await
        .unwrap_err();
    let Error::RetryStopped(stopped) = &e else {
        panic!("{e:?}")
    };
    assert_eq!(stopped.reason().to_string(), "enough");
    assert!(matches!(stopped.error(), Error::BroadcastFailedChannels(_)));
    assert!(e.broadcast_result().is_some() && retryable(&e));
    assert_eq!(fake.requests().len(), 1);

    // A failure no retry can fix stops it without the callback.
    let fake = Fake::new(|_| Answer::Json(401, String::new()));
    let called = Arc::new(Mutex::new(false));
    let c2 = Arc::clone(&called);
    let e = Client::new(fake.config())
        .unwrap()
        .broadcast_with_retry(
            broadcast(&["a"]),
            BroadcastRetryOptions::default()
                .max_attempts(5)
                .on_attempt_error(move |_| {
                    *c2.lock().unwrap() = true;
                    Retry::Default
                }),
        )
        .await
        .unwrap_err();
    assert!(e.is_unauthorized() && !*called.lock().unwrap());
    assert_eq!(fake.requests().len(), 1);

    // Refused before any attempt.
    let client = Client::new(Fake::centrifugo().config()).unwrap();
    for opts in [
        BroadcastRetryOptions::default().max_attempts(0),
        BroadcastRetryOptions::default().attempt_timeout(Duration::ZERO),
    ] {
        assert!(matches!(
            client.broadcast_with_retry(broadcast(&["a"]), opts).await,
            Err(Error::InvalidRequest(_))
        ));
    }
}

#[tokio::test]
async fn retries_end_with_the_deadline() {
    // Each attempt hangs; attempt_timeout bounds it, the call's deadline all.
    let fake = Fake::new(|_| Answer::After(Duration::from_secs(10), Box::new(ok("{}"))));
    let client = Client::new(fake.config()).unwrap();
    let start = std::time::Instant::now();
    let e = client
        .with_timeout(Duration::from_millis(300))
        .broadcast_with_retry(
            broadcast(&["a"]),
            BroadcastRetryOptions::default()
                .max_attempts(100)
                .attempt_timeout(Duration::from_millis(50)),
        )
        .await
        .unwrap_err();
    assert!(e.is_timeout(), "{e:?}");
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(fake.requests().len() >= 2, "a hung attempt is retried");
}

#[tokio::test]
async fn batches() {
    let fake = Fake::new(|r| {
        assert_eq!(r.api_method, "batch");
        ok(
            r#"{"replies":[{"publish":{"offset":5}},{"error":{"code":102,"message":"unknown channel"}},{"presence_stats":{"num_clients":2}}]}"#,
        )
    });
    let client = Client::new(fake.config()).unwrap();
    let mut b = client.new_batch(BatchOptions::default());
    let p0 = b.publish(PublishRequest::new("a").data(raw("1")));
    let p1 = b.publish(PublishRequest::new("bad").data(raw("1")));
    let p2 = b.presence_stats(PresenceStatsRequest::new("a"));
    assert!(matches!(p0.result(), Err(Error::BatchNotSent)));
    assert_eq!(b.len(), 3);
    let e = b.send().await.unwrap_err();
    let Error::BatchFailedCommands(be) = &e else {
        panic!("{e:?}")
    };
    assert_eq!(
        (
            be.total(),
            be.failed().len(),
            be.failed()[0].index,
            be.failed()[0].method
        ),
        (3, 1, 1, "publish")
    );
    assert!(be.result().replied);
    assert_eq!(
        e.api_code(),
        None,
        "a partial error is not its parts' error"
    );
    assert_eq!(
        e.to_string(),
        "batch: 1 of 3 commands failed, 2 succeeded: #1 publish: unknown channel (code 102)"
    );
    assert_eq!(p0.result().unwrap().offset, 5);
    assert_eq!(p1.result().unwrap_err().api_code(), Some(102));
    assert_eq!(p2.result().unwrap().num_clients, 2);
    assert!(
        fake.requests()[0].body.get("parallel").is_none(),
        "sequential by default"
    );

    let client = Client::new(Fake::centrifugo().config()).unwrap();
    let b = client.new_batch(BatchOptions::default().parallel(true));
    assert!(b.send().await.unwrap().replied, "an empty batch succeeds");
}

#[tokio::test]
async fn batches_failing_as_a_whole() {
    let fake = Fake::new(|_| Answer::Json(500, "oops".into()));
    let client = Client::new(fake.config()).unwrap();
    let mut b = client.new_batch(BatchOptions::default());
    let p = b.publish(PublishRequest::new("a").data(raw("1")));
    let bp = b.broadcast(broadcast(&["a"]));
    let e = b.send().await.unwrap_err();
    assert!(matches!(e, Error::Http(_)));
    assert!(
        matches!(p.result(), Err(Error::Http(_))),
        "every command gets the batch's error"
    );
    assert!(matches!(bp.result(), Err(Error::Http(_))));

    // An invalid command fails the batch before sending, naming its index.
    let fake = Fake::centrifugo();
    let client = Client::new(fake.config()).unwrap();
    let mut b = client.new_batch(BatchOptions::default());
    b.publish(PublishRequest::new("a").data(raw("1")));
    let p = b.publish(PublishRequest::new("a"));
    let e = b.send().await.unwrap_err();
    assert!(
        matches!(&e, Error::InvalidRequest(i) if i.command() == Some(1)),
        "{e:?}"
    );
    assert!(
        e.to_string()
            .starts_with("batch command #1: invalid publish request"),
        "{e}"
    );
    assert!(matches!(p.result(), Err(Error::InvalidRequest(_))));
    assert!(fake.requests().is_empty());

    // A reply count different from the commands' is not Centrifugo's.
    let fake = Fake::new(|_| ok(r#"{"replies":[]}"#));
    let client = Client::new(fake.config()).unwrap();
    let mut b = client.new_batch(BatchOptions::default());
    b.publish(PublishRequest::new("a").data(raw("1")));
    assert!(matches!(b.send().await, Err(Error::Decode(_))));
}

#[tokio::test]
async fn a_broadcast_in_a_batch_is_a_broadcast() {
    let fake = fake_broadcast(|ch, _| {
        if ch == "bad" {
            Err((102, false))
        } else {
            Ok(1)
        }
    });
    let client = Client::new(fake.config()).unwrap();
    let mut b = client.new_batch(BatchOptions::default());
    let p = b.broadcast(broadcast(&["a", "bad"]).idempotency_key("k"));
    let e = b.send().await.unwrap_err();
    let Error::BatchFailedCommands(be) = &e else {
        panic!("{e:?}")
    };
    assert!(matches!(
        be.failed()[0].error,
        Error::BroadcastFailedChannels(_)
    ));
    assert_eq!(
        e.to_string(),
        "batch: 1 of 1 commands failed: #0 broadcast: 1 of 2 channels failed, 1 got the publication: bad: failed (code 102)"
    );
    let e = p.result().unwrap_err();
    let result = e.broadcast_result().unwrap();
    assert_eq!(
        (result.channels.len(), result.idempotency_key.as_str()),
        (2, "k")
    );
}

#[test]
fn test_doubles_are_constructible() {
    let failed = cent::ChannelResult::new(
        "b",
        Err(Error::Api(cent::ApiError::new(102, "unknown channel"))),
    );
    let ok_channel = cent::ChannelResult::new("a", Ok(cent::PublishResult::default()));
    let result = cent::BroadcastResult::new(vec![ok_channel, failed]);
    let e =
        Error::BroadcastFailedChannels(Box::new(cent::BroadcastFailedChannelsError::new(result)));
    assert_eq!(e.broadcast_result().unwrap().failed().len(), 1);
    let batch = cent::BatchResult::new(
        true,
        vec![cent::FailedCommand::new(0, "publish", Error::Closed)],
    );
    let e = cent::BatchFailedCommandsError::new(batch, 2);
    assert_eq!(e.failed().len(), 1);
    let p = cent::Pending::new(Ok(cent::PublishResult::default()));
    assert!(p.result().is_ok());
}
