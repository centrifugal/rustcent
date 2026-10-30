//! The `Request` trait every request type implements.

use serde::Serialize;

pub(crate) use sealed::Sealed;

mod sealed {
    /// Only the crate's request types are requests.
    pub trait Sealed {}
}

/// Implemented by the request type of every API method: [`PublishRequest`],
/// [`BroadcastRequest`] and the others. Only the crate's types implement it.
///
/// [`PublishRequest`]: crate::PublishRequest
/// [`BroadcastRequest`]: crate::BroadcastRequest
pub trait Request: Serialize + Sealed + Send + 'static {
    /// The API method the request is sent to, such as `"publish"`. With the
    /// request encoded as JSON, it makes a command for Centrifugo's async
    /// consumers - see [`Command`](crate::Command).
    const METHOD: &'static str;

    /// Whether only Centrifugo PRO has the method.
    const PRO_ONLY: bool;

    /// Checks the rules the client checks before sending (spec 3.5).
    #[doc(hidden)]
    fn validate(&self) -> Result<(), crate::Error> {
        Ok(())
    }
}

/// Whether only Centrifugo PRO has `method`.
#[cfg(feature = "client")]
pub(crate) fn is_pro(method: &str) -> bool {
    crate::generated::api::METHODS
        .iter()
        .any(|&(m, pro)| pro && m == method)
}

/// Whether `name` is an API method, `batch` included.
#[cfg(feature = "client")]
pub(crate) fn is_method(name: &str) -> bool {
    crate::generated::api::METHODS
        .iter()
        .any(|&(m, _)| m == name)
}
