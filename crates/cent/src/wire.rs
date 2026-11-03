//! Serde helpers of the generated types: zero values left out, `null` read as
//! the zero value, payloads kept raw, and wrapper messages as `Option`s.

use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

/// Whether a scalar is its zero value, which is not sent (spec 2).
pub(crate) fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

/// Reads `null` as the zero value: Centrifugo leaves zero values out, but a
/// proxy or another server between may write them as `null`.
pub(crate) fn nullable<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// Reads a payload as it is: `null` is a payload too, `Some` of a JSON null,
/// while an absent field is `None` (spec 3.4).
pub(crate) fn payload<'de, D>(d: D) -> Result<Option<Box<RawValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    Box::<RawValue>::deserialize(d).map(Some)
}

/// A wrapper message - `BoolValue`, `Int32Value` - as an `Option` of its
/// value: `None` leaves the server's setting alone, which `false` or a zero
/// cannot say (spec 2).
pub(crate) mod wrapped {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Wrapper<T> {
        #[serde(default)]
        value: T,
    }

    #[allow(clippy::ref_option)] // serde's `with` passes a reference
    pub(crate) fn serialize<T: Serialize + Copy, S: Serializer>(
        v: &Option<T>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match v {
            Some(value) => Wrapper { value: *value }.serialize(s),
            None => s.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, T, D>(d: D) -> Result<Option<T>, D::Error>
    where
        T: Deserialize<'de> + Default,
        D: Deserializer<'de>,
    {
        Ok(Option::<Wrapper<T>>::deserialize(d)?.map(|w| w.value))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        DeviceLocaleUpdate, DeviceMetaUpdate, DeviceTimezoneUpdate, DeviceUpdateRequest,
        DeviceUserUpdate, FilterNode, SubscribeOptionOverride, SubscribeRequest,
    };

    #[test]
    fn zero_values_are_not_sent() {
        let req = SubscribeRequest::new("chat").user("42");
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"channel":"chat","user":"42"}"#);
    }

    #[test]
    fn wrappers_are_tri_state() {
        let o = SubscribeOptionOverride::new()
            .presence(true)
            .join_leave(false);
        let json = serde_json::to_string(&o).unwrap();
        assert_eq!(
            json,
            r#"{"presence":{"value":true},"join_leave":{"value":false}}"#
        );
        let back: SubscribeOptionOverride = serde_json::from_str(&json).unwrap();
        assert_eq!(back.presence, Some(true));
        assert_eq!(back.join_leave, Some(false));
        assert_eq!(back.force_recovery, None);
    }

    /// The updates of `device_update` are presence wrappers (spec 2): an
    /// empty one clears or detaches, so it is sent as `{}`, and one left
    /// unset is not sent.
    #[test]
    fn empty_presence_wrappers_are_sent() {
        let req = DeviceUpdateRequest::new()
            .users(["42"])
            .user_update(DeviceUserUpdate::new(""))
            .timezone_update(DeviceTimezoneUpdate::new(""))
            .locale_update(DeviceLocaleUpdate::new(""))
            .meta_update(DeviceMetaUpdate::new(BTreeMap::<String, String>::new()));
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"users":["42"],"user_update":{},"meta_update":{},"timezone_update":{},"locale_update":{}}"#
        );
        let unset = serde_json::to_string(&DeviceUpdateRequest::new().users(["42"])).unwrap();
        assert_eq!(unset, r#"{"users":["42"]}"#);
    }

    /// Each constant generated from a "One of:" sentence is its value, named
    /// by field and value, with `FilterNode`'s `op` named by its message in
    /// the API, where the topic updates' `op` is listed in more messages
    /// (spec 3.3).
    #[test]
    fn constants_are_their_values() {
        use crate::proxy;
        let api = [
            (crate::FILTER_NODE_OP_AND, "and"),
            (crate::FILTER_NODE_OP_OR, "or"),
            (crate::FILTER_NODE_OP_NOT, "not"),
            (crate::OP_ADD, "add"),
            (crate::OP_REMOVE, "remove"),
            (crate::OP_SET, "set"),
            (crate::CMP_EQ, "eq"),
            (crate::CMP_NEQ, "neq"),
            (crate::CMP_IN, "in"),
            (crate::CMP_NIN, "nin"),
            (crate::CMP_EX, "ex"),
            (crate::CMP_NEX, "nex"),
            (crate::CMP_SW, "sw"),
            (crate::CMP_EW, "ew"),
            (crate::CMP_CT, "ct"),
            (crate::CMP_GT, "gt"),
            (crate::CMP_GTE, "gte"),
            (crate::CMP_LT, "lt"),
            (crate::CMP_LTE, "lte"),
            (crate::PROVIDER_FCM, "fcm"),
            (crate::PROVIDER_HMS, "hms"),
            (crate::PROVIDER_APNS, "apns"),
            (crate::PROVIDER_WEBPUSH, "webpush"),
            (crate::PLATFORM_IOS, "ios"),
            (crate::PLATFORM_ANDROID, "android"),
            (crate::PLATFORM_WEB, "web"),
            (crate::STATUS_DELIVERED, "delivered"),
            (crate::STATUS_INTERACTED, "interacted"),
            (crate::KEY_MODE_IF_NEW, "if_new"),
            (crate::KEY_MODE_IF_EXISTS, "if_exists"),
        ];
        let proxy = [
            (proxy::OP_AND, "and"),
            (proxy::OP_OR, "or"),
            (proxy::OP_NOT, "not"),
            (proxy::CMP_EQ, "eq"),
            (proxy::CMP_LTE, "lte"),
            (proxy::PROTOCOL_JSON, "json"),
            (proxy::PROTOCOL_PROTOBUF, "protobuf"),
            (proxy::ENCODING_JSON, "json"),
            (proxy::ENCODING_BINARY, "binary"),
            (proxy::ALLOW_SUB, "sub"),
            (proxy::ALLOW_PUB, "pub"),
            (proxy::ALLOW_HST, "hst"),
            (proxy::ALLOW_PRS, "prs"),
            (proxy::MATCH_WILDCARD, "wildcard"),
            (proxy::MATCH_REGEX, "regex"),
            (proxy::KEY_MODE_IF_NEW, "if_new"),
            (proxy::KEY_MODE_IF_EXISTS, "if_exists"),
            (proxy::TYPE_OCCUPIED, "occupied"),
            (proxy::TYPE_VACATED, "vacated"),
        ];
        for (constant, value) in api.into_iter().chain(proxy) {
            assert_eq!(constant, value);
        }
    }

    /// The fields stay strings: a value no constant names is still sent.
    #[test]
    fn a_value_without_a_constant_is_sent() {
        let req = crate::DeviceRegisterRequest::new("fcm_v2", "x", crate::PLATFORM_IOS);
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            r#"{"provider":"fcm_v2","token":"x","platform":"ios"}"#
        );
    }

    #[test]
    fn null_reads_as_zero() {
        let f: FilterNode =
            serde_json::from_str(r#"{"op":null,"vals":null,"nodes":null}"#).unwrap();
        assert!(f.op.is_empty() && f.vals.is_empty() && f.nodes.is_empty());
    }
}
