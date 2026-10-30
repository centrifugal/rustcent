//! A fake Centrifugo for the tests: an `HttpClient` recording every request
//! and answering as a function of the test says.

#![allow(clippy::unwrap_used, clippy::pedantic)] // tests: unwrap is the assertion
#![allow(dead_code, unreachable_pub)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use cent::{BoxError, Config, HttpClient, RawValue};
use serde_json::Value;

/// A request the fake received.
#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: http::Method,
    pub uri: String,
    /// The API method: the last segment of the path.
    pub api_method: String,
    pub headers: http::HeaderMap,
    pub body: Value,
}

/// What the fake answers.
pub enum Answer {
    Json(u16, String),
    Status(u16, http::HeaderMap, String),
    Error(BoxError),
    /// Waits, then answers.
    After(Duration, Box<Answer>),
}

pub fn ok(json: &str) -> Answer {
    Answer::Json(200, json.to_owned())
}

pub type Respond = dyn Fn(&Recorded) -> Answer + Send + Sync;

#[derive(Clone)]
pub struct Fake {
    pub requests: Arc<Mutex<Vec<Recorded>>>,
    respond: Arc<Respond>,
}

impl Fake {
    pub fn new(respond: impl Fn(&Recorded) -> Answer + Send + Sync + 'static) -> Self {
        Self {
            requests: Arc::default(),
            respond: Arc::new(respond),
        }
    }

    /// A fake Centrifugo answering every method with an empty result, a
    /// broadcast with a publication per channel, and a batch with the
    /// command's empty result per command.
    pub fn centrifugo() -> Self {
        Self::new(|r| ok(&default_reply(r)))
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    pub fn config(&self) -> Config {
        Config::new("http://centrifugo.test/api")
            .api_key("key")
            .http_client(self.client())
    }

    pub fn client(&self) -> impl HttpClient {
        let fake = self.clone();
        move |req: http::Request<Bytes>| {
            let (parts, body) = req.into_parts();
            let uri = parts.uri.to_string();
            let rec = Recorded {
                method: parts.method,
                api_method: uri.rsplit('/').next().unwrap_or("").to_owned(),
                uri,
                headers: parts.headers,
                body: serde_json::from_slice(&body).unwrap_or(Value::Null),
            };
            fake.requests.lock().unwrap().push(rec.clone());
            let answer = (fake.respond)(&rec);
            async move { answer_with(answer).await }
        }
    }
}

fn answer_with(
    answer: Answer,
) -> std::pin::Pin<Box<dyn Future<Output = Result<http::Response<Bytes>, BoxError>> + Send>> {
    Box::pin(async move {
        match answer {
            Answer::Json(status, body) => {
                let mut r = http::Response::new(Bytes::from(body));
                *r.status_mut() = http::StatusCode::from_u16(status).unwrap();
                Ok(r)
            }
            Answer::Status(status, headers, body) => {
                let mut r = http::Response::new(Bytes::from(body));
                *r.status_mut() = http::StatusCode::from_u16(status).unwrap();
                *r.headers_mut() = headers;
                Ok(r)
            }
            Answer::Error(e) => Err(e),
            Answer::After(d, a) => {
                tokio::time::sleep(d).await;
                answer_with(*a).await
            }
        }
    })
}

/// Centrifugo's reply to a request with nothing failing.
pub fn default_reply(r: &Recorded) -> String {
    match r.api_method.as_str() {
        "batch" => {
            let replies: Vec<Value> = r.body["commands"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| {
                    let (method, req) = c.as_object().unwrap().iter().next().unwrap();
                    serde_json::json!({ method: command_result(method, req) })
                })
                .collect();
            serde_json::json!({ "replies": replies }).to_string()
        }
        m => serde_json::json!({ "result": command_result(m, &r.body) }).to_string(),
    }
}

fn command_result(method: &str, req: &Value) -> Value {
    if method == "broadcast" {
        let n = req["channels"].as_array().map_or(0, Vec::len);
        let responses: Vec<Value> = (0..n)
            .map(|i| serde_json::json!({"result": {"offset": i + 1, "epoch": "e"}}))
            .collect();
        return serde_json::json!({ "responses": responses });
    }
    serde_json::json!({})
}

pub fn raw(json: &str) -> Box<RawValue> {
    RawValue::from_string(json.to_owned()).unwrap()
}
