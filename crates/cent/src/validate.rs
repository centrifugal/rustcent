//! The rules the client checks before sending (spec 3.5). Everything else is
//! Centrifugo's to check: copying its rules here would refuse what a newer
//! Centrifugo accepts.

use crate::{BroadcastRequest, Error, PublishRequest};

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
