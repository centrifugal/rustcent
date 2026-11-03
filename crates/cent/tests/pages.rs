//! Pages and stream positions (spec 3.8, 15): every `*_all` method over no
//! items, one, exactly a page and several pages; the cursor or `since` of
//! each request after the first; leaving early; errors; `history_all`'s
//! refusals; `position()`.

#![cfg(feature = "client")]
#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion

mod common;

use cent::*;
use common::{Fake, ok};
use serde_json::{Value, json};

const LIMIT: usize = 2;

/// A fake Centrifugo holding `n` items for every paged method, served
/// `limit` at a time: by cursor - the index to start from - or, for history,
/// by `since`, publications having offsets 1 to `n` in epoch "e". With
/// `fail_from`, a request for a page starting there fails.
fn fake(n: usize, fail_from: Option<usize>) -> Fake {
    Fake::new(move |r| {
        let limit = r.body["limit"].as_u64().unwrap() as usize;
        let start = if r.api_method == "history" {
            r.body["since"]["offset"].as_u64().unwrap_or(0) as usize
        } else {
            r.body["cursor"].as_str().unwrap_or("0").parse().unwrap()
        };
        if fail_from == Some(start) {
            return ok(r#"{"error":{"code":100,"message":"internal server error"}}"#);
        }
        let end = (start + limit).min(n);
        let next = if end < n {
            end.to_string()
        } else {
            String::new()
        };
        let page = start..end;
        let result = match r.api_method.as_str() {
            "history" => json!({
                "publications": page.map(|i| json!({"offset": i + 1})).collect::<Vec<_>>(),
                "offset": n,
                "epoch": "e",
            }),
            "device_list" => json!({
                "items": page.map(|i| json!({"id": i.to_string()})).collect::<Vec<_>>(),
                "next_cursor": next,
            }),
            "device_topic_list" | "user_topic_list" => json!({
                "items": page.map(|i| json!({"topic": i.to_string()})).collect::<Vec<_>>(),
                "next_cursor": next,
            }),
            "map_read_state" => json!({
                "entries": page.map(|i| json!({"key": i.to_string()})).collect::<Vec<_>>(),
                "cursor": next,
                "offset": 7,
                "epoch": "e",
            }),
            m => panic!("not a paged method: {m}"),
        };
        ok(&json!({ "result": result }).to_string())
    })
}

/// Reads a pager to its end: the items, as their numbers, and the error
/// which ended it, if one did.
async fn drain<T: Send + 'static>(
    mut pager: Pager<T>,
    id: fn(&T) -> String,
) -> (Vec<String>, Option<Error>) {
    let mut items = Vec::new();
    while let Some(item) = pager.next().await {
        match item {
            Ok(item) => items.push(id(&item)),
            Err(e) => {
                assert!(pager.next().await.is_none(), "an error ends the iteration");
                return (items, Some(e));
            }
        }
    }
    (items, None)
}

/// Every `*_all` method of `client`, read to its end.
async fn every_method(client: &Client) -> Vec<(&'static str, Vec<String>, Option<Error>)> {
    let limit = LIMIT as i32;
    vec![
        {
            let (items, err) = drain(
                client.history_all(HistoryRequest::new("c").limit(limit)),
                |p| (p.offset - 1).to_string(),
            )
            .await;
            ("history", items, err)
        },
        {
            let (items, err) = drain(
                client.device_list_all(DeviceListRequest::new().limit(limit)),
                |d| d.id.clone(),
            )
            .await;
            ("device_list", items, err)
        },
        {
            let (items, err) = drain(
                client.device_topic_list_all(DeviceTopicListRequest::new().limit(limit)),
                |t| t.topic.clone(),
            )
            .await;
            ("device_topic_list", items, err)
        },
        {
            let (items, err) = drain(
                client.user_topic_list_all(UserTopicListRequest::new().limit(limit)),
                |t| t.topic.clone(),
            )
            .await;
            ("user_topic_list", items, err)
        },
        {
            let (items, err) = drain(
                client.map_read_state_all(MapReadStateRequest::new("c").limit(limit)),
                |e| e.key.clone(),
            )
            .await;
            ("map_read_state", items, err)
        },
    ]
}

/// Where each request of `method` started: its cursor, or its `since`.
fn starts(fake: &Fake, method: &str) -> Vec<Value> {
    fake.requests()
        .iter()
        .filter(|r| r.api_method == method)
        .map(|r| {
            if method == "history" {
                r.body.get("since").cloned().unwrap_or(Value::Null)
            } else {
                r.body.get("cursor").cloned().unwrap_or(Value::Null)
            }
        })
        .collect()
}

#[tokio::test]
async fn every_item_once_and_in_order() {
    // No items, one, exactly a page, several pages and a part.
    for n in [0, 1, LIMIT, 2 * LIMIT + 1] {
        let fake = fake(n, None);
        let client = Client::new(fake.config()).unwrap();
        for (method, items, err) in every_method(&client).await {
            assert!(err.is_none(), "{method} over {n}: {err:?}");
            let want: Vec<String> = (0..n).map(|i| i.to_string()).collect();
            assert_eq!(items, want, "{method} over {n}");

            // The first request is as given; each after it carries the
            // cursor, or the since, of the reply before.
            let starts = starts(&fake, method);
            if method == "history" {
                // A page shorter than limit is the last: a full one asks again.
                let pages = n / LIMIT + 1;
                let mut want = vec![Value::Null];
                want.extend((1..pages).map(|p| json!({"offset": p * LIMIT, "epoch": "e"})));
                assert_eq!(starts, want, "history over {n}");
            } else {
                let pages = n.div_ceil(LIMIT).max(1);
                let mut want = vec![Value::Null];
                want.extend((1..pages).map(|p| json!((p * LIMIT).to_string())));
                assert_eq!(starts, want, "{method} over {n}");
            }
        }
    }
}

#[tokio::test]
async fn leaving_early_reads_no_further_page() {
    let fake = fake(10, None);
    let client = Client::new(fake.config()).unwrap();
    let mut devices = client.device_list_all(DeviceListRequest::new().limit(LIMIT as i32));
    assert_eq!(devices.next().await.unwrap().unwrap().id, "0");
    assert_eq!(devices.next().await.unwrap().unwrap().id, "1");
    drop(devices);
    let mut history = client.history_all(HistoryRequest::new("c").limit(LIMIT as i32));
    assert_eq!(history.next().await.unwrap().unwrap().offset, 1);
    drop(history);
    assert_eq!(fake.requests().len(), 2, "one page of each");
}

#[tokio::test]
async fn an_error_ends_the_iteration_after_the_items_before_it() {
    let fake = fake(10, Some(LIMIT));
    let client = Client::new(fake.config()).unwrap();
    for (method, items, err) in every_method(&client).await {
        assert_eq!(items, ["0", "1"], "{method}");
        assert_eq!(
            err.unwrap().api_code(),
            Some(ApiError::INTERNAL),
            "{method}"
        );
    }
    assert_eq!(fake.requests().len(), 10, "two pages of each method");
}

#[tokio::test]
async fn history_all_refuses_before_sending() {
    let fake = fake(3, None);
    let client = Client::new(fake.config()).unwrap();
    for req in [
        HistoryRequest::new("c"),
        HistoryRequest::new("c").limit(-1),
        HistoryRequest::new("c").limit(10).reverse(true),
    ] {
        let mut pager = client.history_all(req);
        let err = pager.next().await.unwrap().unwrap_err();
        assert!(matches!(err, Error::InvalidRequest(_)), "{err}");
        assert!(pager.next().await.is_none());
    }
    assert!(fake.requests().is_empty());
}

#[tokio::test]
async fn history_all_reads_on_from_a_publication() {
    let fake = Fake::new(|r| match r.api_method.as_str() {
        "publish" => ok(r#"{"result":{"offset":5,"epoch":"e"}}"#),
        _ => ok(r#"{"result":{"publications":[{"offset":6}],"offset":6,"epoch":"e"}}"#),
    });
    let client = Client::new(fake.config()).unwrap();
    let published = client
        .publish(PublishRequest::new("c").data(common::raw("1")))
        .await
        .unwrap();
    let mut after = client.history_all(
        HistoryRequest::new("c")
            .limit(10)
            .since(published.position()),
    );
    assert_eq!(after.next().await.unwrap().unwrap().offset, 6);
    assert!(after.next().await.is_none());
    assert_eq!(
        fake.requests()[1].body["since"],
        json!({"offset": 5, "epoch": "e"})
    );
}

/// Every page of a map's state after the first is read at the first one's
/// revision, so that a reset between pages fails with code 112 instead of
/// mixing two states; a revision the request sets is kept for every page.
#[tokio::test]
async fn map_read_state_all_pins_the_revision() {
    let first_page = (Value::Null, Value::Null);
    let from_reply = (json!(7), json!("e"));
    let callers = (json!(4), json!("x"));
    for (req, first, later) in [
        (
            MapReadStateRequest::new("c").limit(LIMIT as i32),
            first_page,
            from_reply,
        ),
        (
            MapReadStateRequest::new("c")
                .limit(LIMIT as i32)
                .revision_offset(4)
                .revision_epoch("x"),
            callers.clone(),
            callers,
        ),
    ] {
        let fake = fake(2 * LIMIT + 1, None);
        let client = Client::new(fake.config()).unwrap();
        let (items, err) = drain(client.map_read_state_all(req), |e| e.key.clone()).await;
        assert!(err.is_none(), "{err:?}");
        assert_eq!(items.len(), 2 * LIMIT + 1);
        let revisions: Vec<(Value, Value)> = fake
            .requests()
            .iter()
            .map(|r| {
                (
                    r.body
                        .get("revision_offset")
                        .cloned()
                        .unwrap_or(Value::Null),
                    r.body.get("revision_epoch").cloned().unwrap_or(Value::Null),
                )
            })
            .collect();
        assert_eq!(revisions.len(), 3);
        assert_eq!(revisions[0], first);
        assert_eq!(revisions[1..], [later.clone(), later]);
    }
}

/// `history_all` yields each publication with its position, the page's
/// epoch beside its offset; `map_read_state_all` each entry with the state's
/// position, the revision every page is read at: the first page's, or the
/// request's own (spec 3.8). The items deref to what they carry.
#[tokio::test]
async fn items_carry_their_positions() {
    let fake = fake(2 * LIMIT + 1, None);
    let client = Client::new(fake.config()).unwrap();
    let (positions, err) = drain(
        client.history_all(HistoryRequest::new("c").limit(LIMIT as i32)),
        |p| {
            assert_eq!(p.position().offset, p.offset);
            format!("{}/{}", p.position().offset, p.position().epoch)
        },
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(positions, ["1/e", "2/e", "3/e", "4/e", "5/e"]);

    for (req, want) in [
        (MapReadStateRequest::new("c").limit(LIMIT as i32), "7/e"),
        (
            MapReadStateRequest::new("c")
                .limit(LIMIT as i32)
                .revision_offset(4)
                .revision_epoch("x"),
            "4/x",
        ),
    ] {
        let (states, err) = drain(client.map_read_state_all(req), |e| {
            format!("{}:{}/{}", e.key, e.state.offset, e.state.epoch)
        })
        .await;
        assert!(err.is_none(), "{err:?}");
        let keys: Vec<String> = (0..2 * LIMIT + 1).map(|i| format!("{i}:{want}")).collect();
        assert_eq!(states, keys);
    }
}

/// A fake answering every page after `delay`: two devices a page, two
/// pages.
fn slow(delay: std::time::Duration) -> Fake {
    Fake::new(move |r| {
        let next = if r.body.get("cursor").is_none() {
            "1"
        } else {
            ""
        };
        common::Answer::After(
            delay,
            Box::new(ok(&json!({"result": {
                "items": [{"id": "a"}, {"id": "b"}],
                "next_cursor": next,
            }})
            .to_string())),
        )
    })
}

/// Each page's request is bounded by `page_timeout`, replacing
/// `request_timeout` for it, longer or shorter, and by `request_timeout`
/// without one; the iteration takes no per-call timeout, so a handle's
/// `with_timeout` does not bound its pages; a zero `page_timeout` is refused
/// before sending (spec 3.2, 3.8).
#[tokio::test]
async fn pages_are_bounded_by_page_timeout() {
    use std::time::Duration;
    let ms = Duration::from_millis;
    let fake = slow(ms(100));
    let short = Client::new(fake.config().request_timeout(ms(20))).unwrap();
    let long = Client::new(fake.config().request_timeout(Duration::from_secs(60))).unwrap();
    let id = |d: &Device| d.id.clone();

    // Longer than request_timeout: each slow page finishes.
    let (items, err) = drain(
        short
            .device_list_all(DeviceListRequest::new())
            .page_timeout(ms(1000)),
        id,
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(items, ["a", "b", "a", "b"]);
    // Without it, request_timeout bounds each page.
    let (items, err) = drain(short.device_list_all(DeviceListRequest::new()), id).await;
    assert!(
        items.is_empty() && err.as_ref().is_some_and(Error::is_timeout),
        "{err:?}"
    );
    // Shorter than request_timeout: it ends the page.
    let start = std::time::Instant::now();
    let (_, err) = drain(
        long.device_list_all(DeviceListRequest::new())
            .page_timeout(ms(20)),
        id,
    )
    .await;
    assert!(err.as_ref().is_some_and(Error::is_timeout), "{err:?}");
    assert!(start.elapsed() < ms(90), "{:?}", start.elapsed());
    // A handle's with_timeout, a call's own, does not bound the pages.
    let (items, err) = drain(
        long.with_timeout(ms(20))
            .device_list_all(DeviceListRequest::new()),
        id,
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(items.len(), 4);
    // A deadline of the handle still ends the iteration.
    let deadline = std::time::Instant::now() + ms(150);
    let (items, err) = drain(
        long.with_deadline(deadline)
            .device_list_all(DeviceListRequest::new())
            .page_timeout(ms(1000)),
        id,
    )
    .await;
    assert_eq!(items, ["a", "b"]);
    assert!(err.as_ref().is_some_and(Error::is_timeout), "{err:?}");

    // Zero is refused, before sending.
    let sent = fake.requests().len();
    let (_, err) = drain(
        long.history_all(HistoryRequest::new("c").limit(1))
            .page_timeout(Duration::ZERO),
        |p| p.offset.to_string(),
    )
    .await;
    assert!(
        matches!(&err, Some(Error::InvalidRequest(i)) if i.method() == "history"),
        "{err:?}"
    );
    let (_, err) = drain(
        long.device_list_all(DeviceListRequest::new())
            .page_timeout(Duration::ZERO),
        id,
    )
    .await;
    assert!(
        matches!(&err, Some(Error::InvalidRequest(i)) if i.method() == "device_list"),
        "{err:?}"
    );
    assert_eq!(fake.requests().len(), sent, "nothing sent");
}

#[cfg(feature = "blocking")]
#[test]
fn the_blocking_pager_takes_page_timeout() {
    use std::time::Duration;
    let fake = slow(Duration::from_millis(100));
    let client =
        cent::blocking::Client::new(fake.config().request_timeout(Duration::from_millis(20)))
            .unwrap();
    let ids: Vec<String> = client
        .device_list_all(DeviceListRequest::new())
        .page_timeout(Duration::from_secs(1))
        .map(|d| d.unwrap().id)
        .collect();
    assert_eq!(ids, ["a", "b", "a", "b"]);
    let mut zero = client
        .device_list_all(DeviceListRequest::new())
        .page_timeout(Duration::ZERO);
    assert!(matches!(zero.next(), Some(Err(Error::InvalidRequest(_)))));
    assert!(zero.next().is_none());
}

#[test]
fn positions() {
    let want = |p: StreamPosition| (p.offset, p.epoch);
    let publish: PublishResult = serde_json::from_str(r#"{"offset":1,"epoch":"a"}"#).unwrap();
    assert_eq!(want(publish.position()), (1, "a".into()));
    let history: HistoryResult = serde_json::from_str(r#"{"offset":2,"epoch":"b"}"#).unwrap();
    assert_eq!(want(history.position()), (2, "b".into()));
    let state: MapReadStateResult = serde_json::from_str(r#"{"offset":3,"epoch":"c"}"#).unwrap();
    assert_eq!(want(state.position()), (3, "c".into()));
    let stream: MapReadStreamResult = serde_json::from_str(r#"{"offset":4,"epoch":"d"}"#).unwrap();
    assert_eq!(want(stream.position()), (4, "d".into()));
    let map_publish: MapPublishResult =
        serde_json::from_str(r#"{"offset":5,"epoch":"e"}"#).unwrap();
    assert_eq!(want(map_publish.position()), (5, "e".into()));
    let map_remove: MapRemoveResult = serde_json::from_str(r#"{"offset":6,"epoch":"f"}"#).unwrap();
    assert_eq!(want(map_remove.position()), (6, "f".into()));
}

#[cfg(feature = "blocking")]
#[test]
fn the_blocking_client_iterates() {
    let fake = fake(2 * LIMIT + 1, None);
    let client = blocking::Client::new(fake.config()).unwrap();
    let limit = LIMIT as i32;
    let want: Vec<String> = (0..2 * LIMIT + 1).map(|i| i.to_string()).collect();
    let ids = |r: Result<String, Error>| r.unwrap();
    let got: Vec<String> = client
        .history_all(HistoryRequest::new("c").limit(limit))
        .map(|p| p.map(|p| (p.offset - 1).to_string()))
        .map(ids)
        .collect();
    assert_eq!(got, want);
    for got in [
        client
            .device_list_all(DeviceListRequest::new().limit(limit))
            .map(|d| d.map(|d| d.id))
            .map(ids)
            .collect::<Vec<_>>(),
        client
            .device_topic_list_all(DeviceTopicListRequest::new().limit(limit))
            .map(|t| t.map(|t| t.topic))
            .map(ids)
            .collect(),
        client
            .user_topic_list_all(UserTopicListRequest::new().limit(limit))
            .map(|t| t.map(|t| t.topic))
            .map(ids)
            .collect(),
        client
            .map_read_state_all(MapReadStateRequest::new("c").limit(limit))
            .map(|e| e.map(|e| e.entry.key))
            .map(ids)
            .collect(),
    ] {
        assert_eq!(got, want);
    }
    // An error ends it; a refusal sends nothing.
    let mut refused = client.history_all(HistoryRequest::new("c"));
    assert!(matches!(
        refused.next(),
        Some(Err(Error::InvalidRequest(_)))
    ));
    assert!(refused.next().is_none());
}

/// A pager and its `next` future can move to another task.
#[test]
fn pagers_are_send() {
    fn send<T: Send>(_: &T) {}
    let client = Client::new(fake(0, None).config()).unwrap();
    let mut pager = client.device_list_all(DeviceListRequest::new());
    send(&pager);
    send(&pager.next());
}
