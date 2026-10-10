//! The rbus message envelope: a positional body followed by a meta section.
//!
//! Requests and responses carry `[method, traceparent, tracestate]` as meta;
//! responses use the method name `METHOD_RESPONSE`. Events carry
//! `[event name, provider component, 1, traceparent, tracestate]` (legacy CCSP
//! events: `[event name, object name, 0]`).

use bytes::Bytes;

use crate::error::Result;
use crate::msgpack::{Reader, Writer};

/// The method named in a request's meta section (`core/rbuscore_types.h`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Method {
    /// `METHOD_GETPARAMETERVALUES`: get one or more values.
    GetParameterValues,
    /// `METHOD_SETPARAMETERVALUES`: set one or more values.
    SetParameterValues,
    /// `METHOD_COMMIT`: commit a session (a no-op for rbus providers).
    Commit,
    /// `METHOD_GETPARAMETERNAMES`: element names, or a table's row names.
    GetParameterNames,
    /// `METHOD_SETPARAMETERATTRIBUTES` (CCSP).
    SetParameterAttributes,
    /// `METHOD_GETPARAMETERATTRIBUTES` (CCSP).
    GetParameterAttributes,
    /// `METHOD_ADDTBLROW`: add a table row.
    AddTableRow,
    /// `METHOD_DELETETBLROW`: remove a table row.
    DeleteTableRow,
    /// `METHOD_RPC`: invoke a method.
    Rpc,
    /// `METHOD_SUBSCRIBE`: subscribe to an event.
    Subscribe,
    /// `METHOD_UNSUBSCRIBE`: unsubscribe from an event.
    Unsubscribe,
    /// `METHOD_OPENDIRECT_CONN`: direct mode; rbus-rs only rejects it.
    OpenDirectConn,
    /// `METHOD_CLOSEDIRECT_CONN`: direct mode; rbus-rs only rejects it.
    CloseDirectConn,
    /// `METHOD_RESPONSE`: the meta method of every response.
    Response,
    /// A method name this crate does not know.
    Other(String),
}

const METHODS: [(Method, &str); 14] = [
    (Method::GetParameterValues, "METHOD_GETPARAMETERVALUES"),
    (Method::SetParameterValues, "METHOD_SETPARAMETERVALUES"),
    (Method::Commit, "METHOD_COMMIT"),
    (Method::GetParameterNames, "METHOD_GETPARAMETERNAMES"),
    (
        Method::SetParameterAttributes,
        "METHOD_SETPARAMETERATTRIBUTES",
    ),
    (
        Method::GetParameterAttributes,
        "METHOD_GETPARAMETERATTRIBUTES",
    ),
    (Method::AddTableRow, "METHOD_ADDTBLROW"),
    (Method::DeleteTableRow, "METHOD_DELETETBLROW"),
    (Method::Rpc, "METHOD_RPC"),
    (Method::Subscribe, "METHOD_SUBSCRIBE"),
    (Method::Unsubscribe, "METHOD_UNSUBSCRIBE"),
    (Method::OpenDirectConn, "METHOD_OPENDIRECT_CONN"),
    (Method::CloseDirectConn, "METHOD_CLOSEDIRECT_CONN"),
    (Method::Response, "METHOD_RESPONSE"),
];

impl Method {
    /// The wire name.
    pub fn as_str(&self) -> &str {
        match self {
            Method::Other(name) => name,
            m => METHODS
                .iter()
                .find(|(x, _)| x == m)
                .map(|(_, s)| *s)
                .expect("listed"),
        }
    }

    /// Parses a wire name.
    pub fn parse(name: &str) -> Method {
        METHODS
            .iter()
            .find(|(_, s)| *s == name)
            .map_or_else(|| Method::Other(name.to_owned()), |(m, _)| m.clone())
    }
}

/// W3C trace context propagated with requests and events. Empty strings mean
/// "no context".
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct TraceContext {
    /// The `traceparent` value.
    pub traceparent: String,
    /// The `tracestate` value.
    pub tracestate: String,
}

/// Meta section of a request or response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MethodMeta {
    /// The method; [`Method::Response`] for responses.
    pub method: Method,
    /// Trace context.
    pub trace: TraceContext,
}

impl MethodMeta {
    /// Meta for a request.
    pub fn request(method: Method, trace: TraceContext) -> Self {
        MethodMeta { method, trace }
    }

    /// Meta for a response: `["METHOD_RESPONSE", "", ""]`.
    pub fn response() -> Self {
        MethodMeta {
            method: Method::Response,
            trace: TraceContext::default(),
        }
    }

    /// Writes the meta fields.
    pub fn encode(&self, w: &mut Writer) {
        w.str(self.method.as_str());
        w.str(&self.trace.traceparent);
        w.str(&self.trace.tracestate);
    }

    /// Reads the meta fields. Missing trace fields read as empty.
    pub fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let method = Method::parse(&r.str()?);
        let traceparent = if r.is_empty() {
            String::new()
        } else {
            r.str()?
        };
        let tracestate = if r.is_empty() {
            String::new()
        } else {
            r.str()?
        };
        Ok(MethodMeta {
            method,
            trace: TraceContext {
                traceparent,
                tracestate,
            },
        })
    }
}

/// Meta section of an event message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMeta {
    /// The event name, as subscribed (the routing key on the subscriber side).
    pub event_name: String,
    /// The publishing component (rbus) or object name (CCSP).
    pub object_name: String,
    /// False for legacy CCSP events.
    pub rbus2: bool,
    /// Trace context (rbus events only).
    pub trace: TraceContext,
}

impl EventMeta {
    /// Writes the meta fields.
    pub fn encode(&self, w: &mut Writer) {
        w.str(&self.event_name);
        w.str(&self.object_name);
        w.flag(self.rbus2);
        if self.rbus2 {
            w.str(&self.trace.traceparent);
            w.str(&self.trace.tracestate);
        }
    }

    /// Reads the meta fields.
    pub fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let event_name = r.str()?;
        let object_name = r.str()?;
        let rbus2 = r.flag()?;
        let mut trace = TraceContext::default();
        if !r.is_empty() {
            trace.traceparent = r.str()?;
            trace.tracestate = r.str()?;
        }
        Ok(EventMeta {
            event_name,
            object_name,
            rbus2,
            trace,
        })
    }
}

/// Something that can be written as, or read from, the body of an rbus message.
pub trait Body: Sized {
    /// Writes the body items.
    fn encode(&self, w: &mut Writer);
    /// Reads the body items.
    fn decode(r: &mut Reader<'_>) -> Result<Self>;
}

/// Encodes a request payload: `body` followed by `[method, trace]` meta.
pub fn request_payload(method: Method, trace: TraceContext, body: &impl Body) -> Vec<u8> {
    with_meta(body, &MethodMeta::request(method, trace))
}

/// Encodes a response payload: `body` followed by `METHOD_RESPONSE` meta.
pub fn response_payload(body: &impl Body) -> Vec<u8> {
    with_meta(body, &MethodMeta::response())
}

fn with_meta(body: &impl Body, meta: &MethodMeta) -> Vec<u8> {
    let mut w = Writer::new();
    body.encode(&mut w);
    w.finish_with_meta(|m| meta.encode(m))
}

/// Splits a request or response payload into a body reader and its meta.
/// `bin` items read from the body are zero-copy slices of `payload`.
pub fn split_method_payload(payload: &Bytes) -> Result<(Reader<'_>, MethodMeta)> {
    let (body, mut meta) = Reader::from_bytes(payload).split_meta()?;
    Ok((body, MethodMeta::decode(&mut meta)?))
}

/// Splits an event payload into a body reader and its meta.
pub fn split_event_payload(payload: &Bytes) -> Result<(Reader<'_>, EventMeta)> {
    let (body, mut meta) = Reader::from_bytes(payload).split_meta()?;
    Ok((body, EventMeta::decode(&mut meta)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Status(i32);
    impl Body for Status {
        fn encode(&self, w: &mut Writer) {
            w.i32(self.0);
        }
        fn decode(r: &mut Reader<'_>) -> Result<Self> {
            Ok(Status(r.i32()?))
        }
    }

    #[test]
    fn method_names_round_trip() {
        for (m, s) in &METHODS {
            assert_eq!(m.as_str(), *s);
            assert_eq!(Method::parse(s), *m);
        }
        assert_eq!(Method::parse("METHOD_X"), Method::Other("METHOD_X".into()));
        assert_eq!(Method::Other("METHOD_X".into()).as_str(), "METHOD_X");
    }

    #[test]
    fn response_payload_matches_recorded_bytes() {
        // set_values fixture: a successful single set response.
        let p = response_payload(&Status(0));
        let mut want = vec![0x00];
        want.push(0xb0);
        want.extend_from_slice(b"METHOD_RESPONSE\0");
        want.extend_from_slice(&[0xa1, 0, 0xa1, 0, 0xd2, 0, 0, 0, 1]);
        assert_eq!(p, want);
        let p = Bytes::from(p);
        let (mut body, meta) = split_method_payload(&p).unwrap();
        assert_eq!(Status::decode(&mut body).unwrap().0, 0);
        assert_eq!(meta, MethodMeta::response());
    }

    #[test]
    fn request_meta_carries_trace() {
        let trace = TraceContext {
            traceparent: "00-abc-def-01".into(),
            tracestate: "k=v".into(),
        };
        let p = Bytes::from(request_payload(Method::Rpc, trace.clone(), &Status(5)));
        let (_, meta) = split_method_payload(&p).unwrap();
        assert_eq!(meta, MethodMeta::request(Method::Rpc, trace));
        // A CCSP-style meta with only the method name.
        let mut w = Writer::new();
        w.str("METHOD_COMMIT");
        let b = w.into_bytes();
        let m = MethodMeta::decode(&mut Reader::new(&b)).unwrap();
        assert_eq!(m.method, Method::Commit);
        assert_eq!(m.trace, TraceContext::default());
    }

    #[test]
    fn event_meta_rbus_and_legacy() {
        let meta = EventMeta {
            event_name: "Device.Provider1.Event1!".into(),
            object_name: "EventProvider".into(),
            rbus2: true,
            trace: TraceContext::default(),
        };
        let mut w = Writer::new();
        Status(1).encode(&mut w);
        let p = Bytes::from(w.finish_with_meta(|m| meta.encode(m)));
        let (_, got) = split_event_payload(&p).unwrap();
        assert_eq!(got, meta);
        let legacy = EventMeta {
            rbus2: false,
            ..meta
        };
        let mut w = Writer::new();
        legacy.encode(&mut w);
        let b = w.into_bytes();
        assert_eq!(EventMeta::decode(&mut Reader::new(&b)).unwrap(), legacy);
    }
}
