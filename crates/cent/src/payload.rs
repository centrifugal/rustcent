//! Payloads to and from application types: [`json`] and [`PayloadExt`].

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::Error;
use crate::error::PayloadError;

/// Encodes `value` as a payload, for the `data`, `info` and other payload
/// fields of requests and proxy results:
///
/// ```
/// # fn run() -> Result<(), cent::Error> {
/// #[derive(serde::Serialize)]
/// struct Message { text: String }
///
/// let request = cent::PublishRequest::new("news")
///     .data(cent::json(&Message { text: "hello".into() })?);
/// let request = cent::PublishRequest::new("news")
///     .data(cent::json(&serde_json::json!({"text": "hello"}))?);
/// # Ok(()) }
/// ```
///
/// A [`serde_json::Value`] - or any type serde can encode - goes in as it
/// encodes; a payload already JSON text is [`RawValue::from_string`].
///
/// # Errors
///
/// [`Error::Payload`] when `value` cannot be encoded: a map whose keys are not
/// strings, or a `Serialize` impl's own error.
pub fn json<T: Serialize + ?Sized>(value: &T) -> Result<Box<RawValue>, Error> {
    serde_json::value::to_raw_value(value).map_err(|e| Error::Payload(PayloadError::encode(e)))
}

/// Decodes a payload into an application type: a publication's `data`, a
/// proxy request's `data`, a presence entry's `conn_info`.
///
/// ```
/// use cent::PayloadExt;
///
/// #[derive(serde::Deserialize)]
/// struct Message { text: String }
///
/// # fn run(publication: cent::Publication) -> Result<(), cent::Error> {
/// let message: Message = publication.data.decode()?;
/// # Ok(()) }
/// ```
///
/// It works on a payload field - an `Option<Box<RawValue>>`, where `None`
/// is an error - and on a [`RawValue`] itself.
pub trait PayloadExt: sealed::Sealed {
    /// Decodes the payload as a `T`.
    ///
    /// # Errors
    ///
    /// [`Error::Payload`] when the payload is not a `T`, or is unset.
    fn decode<'a, T: Deserialize<'a>>(&'a self) -> Result<T, Error>;
}

impl PayloadExt for RawValue {
    fn decode<'a, T: Deserialize<'a>>(&'a self) -> Result<T, Error> {
        serde_json::from_str(self.get()).map_err(|e| Error::Payload(PayloadError::decode(e)))
    }
}

impl PayloadExt for Box<RawValue> {
    fn decode<'a, T: Deserialize<'a>>(&'a self) -> Result<T, Error> {
        (**self).decode()
    }
}

impl PayloadExt for Option<Box<RawValue>> {
    fn decode<'a, T: Deserialize<'a>>(&'a self) -> Result<T, Error> {
        self.as_deref()
            .ok_or_else(|| Error::Payload(PayloadError::missing()))?
            .decode()
    }
}

mod sealed {
    use serde_json::value::RawValue;

    pub trait Sealed {}
    impl Sealed for RawValue {}
    impl Sealed for Box<RawValue> {}
    impl Sealed for Option<Box<RawValue>> {}
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Message {
        text: String,
    }

    #[test]
    fn round_trip() {
        let raw = json(&Message { text: "hi".into() }).unwrap();
        assert_eq!(raw.get(), r#"{"text":"hi"}"#);
        assert_eq!(
            raw.decode::<Message>().unwrap(),
            Message { text: "hi".into() }
        );
        assert_eq!(
            Some(raw).decode::<Message>().unwrap(),
            Message { text: "hi".into() }
        );
        // Borrowed from the payload.
        let raw = json("x").unwrap();
        assert_eq!(raw.decode::<&str>().unwrap(), "x");
    }

    #[test]
    fn errors() {
        let mut keys = std::collections::HashMap::new();
        keys.insert((1, 2), 3);
        let e = json(&keys).unwrap_err();
        assert!(matches!(e, Error::Payload(_)), "{e}");
        assert!(e.to_string().starts_with("encoding the payload: "), "{e}");
        assert!(!e.is_retryable());

        let e = json("x").unwrap().decode::<Message>().unwrap_err();
        assert!(e.to_string().starts_with("decoding the payload: "), "{e}");
        assert!(std::error::Error::source(&e).is_some());
        let e = None::<Box<RawValue>>.decode::<Message>().unwrap_err();
        assert_eq!(e.to_string(), "decoding the payload: no payload");
    }
}
