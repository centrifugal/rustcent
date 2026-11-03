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
            .map(|e| e.map(|e| e.key))
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
