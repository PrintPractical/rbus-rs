//! Broker control messages: compact JSON payloads sent to rtrouted's
//! built-in topics (no RawBinary flag).
//!
//! Field order matches cJSON's output from the C client, so encoded messages
//! are byte-identical to theirs. Never send an empty body: rtrouted stalls on
//! empty payloads, which is why [`RegisteredComponents`] carries `{"dummy":0}`.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Adds or removes routes: inbox, component and element aliases.
pub const TOPIC_SUBSCRIBE: &str = "_RTROUTED.INBOX.SUBSCRIBE";
/// Resolves a partial path to the routes under it.
pub const TOPIC_QUERY: &str = "_RTROUTED.INBOX.QUERY";
/// Maps exact element names to their owning components.
pub const TOPIC_TRACE_ORIGIN: &str = "_trace_origin_object";
/// Lists the elements registered by a component.
pub const TOPIC_ENUMERATE_ELEMENTS: &str = "_enumerate_elements";
/// Lists every registered route.
pub const TOPIC_REGISTERED_COMPONENTS: &str = "_registered_components";
/// Connect and disconnect notices, for clients that register this topic.
pub const TOPIC_ADVISORY: &str = "_RTROUTED.ADVISORY";

/// Encodes a control message as compact JSON.
pub fn encode<T: Serialize>(msg: &T) -> Vec<u8> {
    serde_json::to_vec(msg).expect("control messages always serialize")
}

/// Decodes a control message. Trailing NULs, which some C builds append, are
/// ignored.
pub fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T> {
    let end = payload.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    Ok(serde_json::from_slice(&payload[..end])?)
}

/// `_RTROUTED.INBOX.SUBSCRIBE` body. Sent with no flags for the inbox and the
/// component, and as a Request (answered with [`ResultReply`]) for aliases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Route {
    /// 1 to add, 0 to remove.
    pub add: i32,
    /// The inbox, component name or element name.
    pub topic: String,
    /// Route id: 1 for the inbox, 10000 and up for components. An alias uses
    /// its component's id.
    pub route_id: u32,
}

impl Route {
    /// Adds `topic` under `route_id`.
    pub fn add(topic: impl Into<String>, route_id: u32) -> Self {
        Route {
            add: 1,
            topic: topic.into(),
            route_id,
        }
    }

    /// Removes `topic`.
    pub fn remove(topic: impl Into<String>, route_id: u32) -> Self {
        Route {
            add: 0,
            topic: topic.into(),
            route_id,
        }
    }
}

/// `{"result":rc}`: the reply to a route or alias request. 1001 means the name
/// is already registered; 1003 means it conflicts with a table or property.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultReply {
    /// 0 on success.
    pub result: i32,
}

/// `_RTROUTED.INBOX.QUERY` and `_enumerate_elements` body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expression {
    /// A partial path (`Device.WiFi.`) for QUERY; a component name for
    /// `_enumerate_elements`.
    pub expression: String,
}

/// `_trace_origin_object` body: the exact element names to look up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameList {
    /// Number of names.
    pub count: usize,
    /// The names.
    pub items: Vec<String>,
}

impl NameList {
    /// A list of names.
    pub fn new(items: Vec<String>) -> Self {
        NameList {
            count: items.len(),
            items,
        }
    }
}

/// `_registered_components` body. Its only purpose is to not be empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RegisteredComponents {
    /// Always 0.
    pub dummy: i32,
}

/// Reply to the discovery queries. `result` is absent from the
/// `_enumerate_elements` and `_registered_components` replies, and `items` is
/// absent when there are none.
///
/// **Use `items`, not `count`.** rtrouted's `_trace_origin_object` reply writes
/// `"count":1` once per name looked up, so a three-name query yields three
/// duplicate `count` keys next to three items. Duplicate keys are accepted
/// (the last wins), as cJSON does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "serde_json::Value")]
pub struct DiscoveryReply {
    /// 0 on success, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<i32>,
    /// The count as sent; unreliable, see above.
    pub count: usize,
    /// Routes, components or element names, depending on the query. cJSON
    /// omits the key when there are none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<String>,
}

impl TryFrom<serde_json::Value> for DiscoveryReply {
    type Error = String;

    fn try_from(v: serde_json::Value) -> std::result::Result<Self, String> {
        let obj = v
            .as_object()
            .ok_or("discovery reply is not a JSON object")?;
        let int = |key: &str| obj.get(key).and_then(serde_json::Value::as_i64);
        let items = match obj.get("items") {
            None => Vec::new(),
            Some(serde_json::Value::Array(a)) => a
                .iter()
                .map(|i| i.as_str().map(str::to_owned).ok_or("non-string item"))
                .collect::<std::result::Result<_, _>>()?,
            Some(_) => return Err("items is not an array".into()),
        };
        Ok(DiscoveryReply {
            result: int("result").map(|r| r as i32),
            count: int("count").ok_or("missing count")? as usize,
            items,
        })
    }
}

/// `_RTROUTED.ADVISORY` notice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Advisory {
    /// 0 when a client connected, 1 when it disconnected.
    pub event: i32,
    /// The client's inbox.
    pub inbox: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_match_cjson_output() {
        assert_eq!(
            encode(&Route::add("rbus.rbuscli.INBOX.109", 1)),
            br#"{"add":1,"topic":"rbus.rbuscli.INBOX.109","route_id":1}"#
        );
        assert_eq!(
            encode(&Route::remove("rbuscli-109", 10000)),
            br#"{"add":0,"topic":"rbuscli-109","route_id":10000}"#
        );
    }

    #[test]
    fn queries_match_recorded_bytes() {
        assert_eq!(
            encode(&NameList::new(vec![
                "Device.SampleProvider.SampleData.IntData".into()
            ])),
            br#"{"count":1,"items":["Device.SampleProvider.SampleData.IntData"]}"#
        );
        assert_eq!(encode(&RegisteredComponents::default()), br#"{"dummy":0}"#);
        assert_eq!(
            encode(&Expression {
                expression: "rbusSampleProvider".into()
            }),
            br#"{"expression":"rbusSampleProvider"}"#
        );
    }

    #[test]
    fn replies_decode_with_optional_fields() {
        let r: DiscoveryReply = decode(br#"{"result":0,"count":0}"#).unwrap();
        assert_eq!((r.result, r.count, r.items.len()), (Some(0), 0, 0));
        let r: DiscoveryReply = decode(br#"{"count":2,"items":["a","b"]}"#).unwrap();
        assert_eq!(
            (r.result, r.items),
            (None, vec!["a".to_string(), "b".to_string()])
        );
        let r: ResultReply = decode(b"{\"result\":1001}\0").unwrap();
        assert_eq!(r.result, 1001);
        // Recorded from rtrouted: one duplicate "count" per name looked up.
        let r: DiscoveryReply =
            decode(br#"{"result":0,"count":1,"items":["p","p","p"],"count":1,"count":1}"#).unwrap();
        assert_eq!((r.count, r.items.len()), (1, 3));
        for bad in [
            &br#"[1]"#[..],
            br#"{"count":1,"items":"x"}"#,
            br#"{"count":1,"items":[1]}"#,
            br#"{"items":[]}"#,
        ] {
            assert!(decode::<DiscoveryReply>(bad).is_err());
        }
        let a: Advisory = decode(br#"{"event":1,"inbox":"rbus.x.INBOX.7"}"#).unwrap();
        assert_eq!(a.event, 1);
        assert!(decode::<ResultReply>(b"not json").is_err());
        assert!(decode::<ResultReply>(b"").is_err());
    }
}
