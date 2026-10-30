//! Property tests: payloads carried exactly, replies of any shape decoded
//! without panicking, and the backoff in its bounds.

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![cfg(feature = "client")]

mod common;

use std::time::Duration;

use cent::{Client, PublishRequest, RawValue};
use common::{Answer, Fake, ok};
use proptest::prelude::*;

/// Arbitrary JSON text: values of every kind, nested, with numbers written as
/// they come - exponents, fractions with trailing zeros, integers past 2^53.
fn json_text() -> impl Strategy<Value = String> {
    let number = prop_oneof![
        any::<i64>().prop_map(|n| n.to_string()),
        any::<u64>().prop_map(|n| format!("{n}0")),
        (any::<i32>(), 0u32..1000).prop_map(|(a, b)| format!("{a}.{b:03}")),
        (1u32..10, -400i32..400).prop_map(|(m, e)| format!("{m}e{e}")),
    ];
    let leaf = prop_oneof![
        Just("null".to_owned()),
        Just("true".to_owned()),
        Just("false".to_owned()),
        number,
        "[a-z\\u{e9}\\u{1f600} \"\\\\]{0,8}".prop_map(|s| serde_json::to_string(&s).unwrap()),
    ];
    leaf.prop_recursive(4, 32, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(|v| format!("[{}]", v.join(","))),
            prop::collection::vec(("[a-z0-9]{0,4}", inner), 0..5).prop_map(|v| {
                let fields: Vec<String> = v
                    .into_iter()
                    .map(|(k, v)| format!("{}:{v}", serde_json::to_string(&k).unwrap()))
                    .collect();
                format!("{{{}}}", fields.join(","))
            }),
        ]
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// A payload goes out as its text, and comes back from a reply as the
    /// same text: what came out can go back in unchanged.
    #[test]
    fn payloads_are_carried_exactly(text in json_text()) {
        let payload = RawValue::from_string(text.clone()).unwrap();
        let encoded = serde_json::to_string(&PublishRequest::new("c").data(payload)).unwrap();
        prop_assert_eq!(encoded, format!(r#"{{"channel":"c","data":{text}}}"#));
        let reply = format!(r#"{{"result":{{"publications":[{{"data":{text}}}]}}}}"#);
        let fake = Fake::new(move |_| ok(&reply));
        let client = Client::new(fake.config()).unwrap();
        let history = runtime().block_on(client.history(cent::HistoryRequest::new("c"))).unwrap();
        let data = history.publications[0].data.as_ref().unwrap();
        prop_assert_eq!(data.get(), text.as_str());
    }

    /// Any reply - bytes, or JSON of any shape - decodes to a result or an
    /// error, never a panic.
    #[test]
    fn replies_never_panic(body in prop_oneof![
        prop::collection::vec(any::<u8>(), 0..64).prop_map(|b| String::from_utf8_lossy(&b).into_owned()),
        json_text(),
        json_text().prop_map(|t| format!(r#"{{"result":{t}}}"#)),
        json_text().prop_map(|t| format!(r#"{{"error":{t}}}"#)),
        json_text().prop_map(|t| format!(r#"{{"replies":[{t}]}}"#)),
        json_text().prop_map(|t| format!(r#"{{"result":{{"responses":[{t}]}}}}"#)),
    ], status in prop_oneof![Just(200u16), Just(400), Just(404), Just(500), Just(503)]) {
        let fake = Fake::new(move |_| Answer::Json(status, body.clone()));
        let client = Client::new(fake.config()).unwrap();
        let rt = runtime();
        let _ = rt.block_on(client.info(cent::InfoRequest::new()));
        let _ = rt.block_on(client.broadcast(cent::BroadcastRequest::new(["a"]).data(RawValue::from_string("1".into()).unwrap())));
        let mut b = client.new_batch(cent::BatchOptions::default());
        let p = b.history(cent::HistoryRequest::new("c"));
        let _ = rt.block_on(b.send());
        let _ = p.result();
    }

    /// Each wait is in [d/2, d] of the doubling step, capped at the limit.
    #[test]
    fn backoff_in_bounds(base_ms in 0u64..10_000, limit_ms in 0u64..100_000, attempt in 0u32..100) {
        let (base, limit) = (Duration::from_millis(base_ms), Duration::from_millis(limit_ms));
        let d = cent::exponential_backoff(base, limit).delay(attempt);
        let cap = limit.max(base);
        let step = base.saturating_mul(1u32.checked_shl(attempt.max(1) - 1).unwrap_or(u32::MAX)).min(cap);
        prop_assert!(d <= step, "{d:?} > {step:?}");
        prop_assert!(d >= step / 2, "{d:?} < half of {step:?}");
    }

    /// Results encode as JSON with wire names, payloads as they are, and
    /// decode back the same.
    #[test]
    fn results_round_trip(text in json_text(), offset in any::<u64>()) {
        let json = format!(r#"{{"publications":[{{"data":{text},"offset":{offset}}}],"epoch":"e","offset":{offset}}}"#);
        let res: cent::HistoryResult = serde_json::from_str(&json).unwrap();
        let encoded = serde_json::to_string(&res).unwrap();
        let back: cent::HistoryResult = serde_json::from_str(&encoded).unwrap();
        prop_assert_eq!(back.publications[0].data.as_ref().unwrap().get(), text.as_str());
        prop_assert_eq!((back.offset, back.publications[0].offset), (offset, offset));
        prop_assert!(encoded.contains(&format!(r#""data":{text}"#)), "{}", encoded);
    }
}
