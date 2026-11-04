//! The rules the client checks before sending (spec 3.5). Everything else is
//! Centrifugo's to check: copying its rules here would refuse what a newer
//! Centrifugo accepts.

use crate::{BroadcastRequest, Error, PublishRequest, Request};

/// A generated input message: the path of the first field the proto marks
/// "Required." which has no value - unset or empty, the same on the wire -
/// such as `limit_strategy.rate_limit.policies[0].rate` (spec 3.3, 3.5).
pub(crate) trait Required {
    fn missing(&self) -> Option<String>;
}

/// Fails, naming its path, when a required field of `r` has no value: a
/// constructor makes the caller pass it, but `new("")` passes it empty.
pub(crate) fn required<R: Request + Required>(r: &R) -> Result<(), Error> {
    match r.missing() {
        Some(path) => Err(Error::invalid_request(
            R::METHOD,
            format!("{path} is required"),
        )),
        None => Ok(()),
    }
}

pub(crate) fn publish(r: &PublishRequest) -> Result<(), Error> {
    if r.channel.is_empty() {
        return Err(Error::invalid_request("publish", "channel is required"));
    }
    if r.data.is_none() && r.b64data.is_empty() {
        return Err(Error::invalid_request(
            "publish",
            "data or b64data is required",
        ));
    }
    Ok(())
}

pub(crate) fn broadcast(r: &BroadcastRequest) -> Result<(), Error> {
    if r.channels.is_empty() {
        return Err(Error::invalid_request("broadcast", "channels is required"));
    }
    if r.data.is_none() && r.b64data.is_empty() {
        return Err(Error::invalid_request(
            "broadcast",
            "data or b64data is required",
        ));
    }
    if let Some(i) = r.channels.iter().position(String::is_empty) {
        return Err(Error::invalid_request(
            "broadcast",
            format!("channels[{i}] is empty"),
        ));
    }
    Ok(())
}
