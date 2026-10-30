//! Commands for Centrifugo's async consumers (spec 9).

use serde::Serialize;
use serde_json::value::RawValue;

use crate::{Error, Request};

/// An API call left for Centrifugo's [async
/// consumers](https://centrifugal.dev/docs/server/consumers) to make, instead
/// of calling Centrifugo: written to a PostgreSQL outbox table, a Kafka topic,
/// a Redis stream. A function per API method makes one, such as
/// [`Command::publish`]: it checks the request as the client does before
/// sending it - an invalid request fails here, rather than in a consumer,
/// which drops a command it cannot run - and encodes it.
///
/// Consumers take a command in one of two forms:
///
/// - its [`method`](Self::method) and [`payload`](Self::payload) apart: the
///   outbox table's `method` and `payload` columns, or a payload with the
///   method in a message attribute (Google Cloud Pub/Sub, AWS SQS, Azure
///   Service Bus, a Redis stream);
/// - the command encoded as JSON, `{"method": ..., "payload": {...}}`, as the
///   message itself (Kafka, NATS JetStream): [`to_json`](Self::to_json), or
///   the command serialized with serde, which gives the payload as the JSON
///   object, never as a string of it.
///
/// ```
/// let data = cent::json(&serde_json::json!({"text": "hi"}))?;
/// let cmd = cent::Command::publish(cent::PublishRequest::new("news").data(data))?;
/// // INSERT INTO centrifugo_outbox (method, payload, partition) VALUES ($1, $2::jsonb, $3)
/// assert_eq!(cmd.method(), "publish");
/// assert_eq!(cmd.payload().get(), r#"{"channel":"news","data":{"text":"hi"}}"#);
/// # Ok::<(), cent::Error>(())
/// ```
///
/// Within a partition - an outbox table's partition, a Kafka partition - a
/// consumer runs commands in order; across partitions it does not. To keep a
/// channel's commands in order, write them to one partition, derived from the
/// channel the same way by every writer: the channel as a Kafka message key,
/// say, or a hash of it modulo the outbox table's number of partitions.
#[derive(Debug, Clone, Serialize)]
pub struct Command {
    method: &'static str,
    payload: Box<RawValue>,
}

impl Command {
    #[allow(clippy::needless_pass_by_value)] // as the client's methods take it
    pub(crate) fn new<R: Request>(request: R) -> Result<Self, Error> {
        request.validate()?;
        let payload = serde_json::value::to_raw_value(&request)
            .map_err(|e| Error::invalid_request(R::METHOD, format!("cannot be encoded: {e}")))?;
        Ok(Self {
            method: R::METHOD,
            payload,
        })
    }

    /// The API method, such as `"publish"`.
    #[must_use]
    pub fn method(&self) -> &'static str {
        self.method
    }

    /// The request, as JSON: for a `jsonb` column, write
    /// [`payload().get()`](RawValue::get).
    #[must_use]
    pub fn payload(&self) -> &RawValue {
        &self.payload
    }

    /// The command as a JSON message: `{"method": ..., "payload": {...}}`.
    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            "{{\"method\":{},\"payload\":{}}}",
            serde_json::Value::from(self.method),
            self.payload.get()
        )
    }
}
