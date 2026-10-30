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
    use crate::{FilterNode, SubscribeOptionOverride, SubscribeRequest};

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

    #[test]
    fn null_reads_as_zero() {
        let f: FilterNode =
            serde_json::from_str(r#"{"op":null,"vals":null,"nodes":null}"#).unwrap();
        assert!(f.op.is_empty() && f.vals.is_empty() && f.nodes.is_empty());
    }
}
