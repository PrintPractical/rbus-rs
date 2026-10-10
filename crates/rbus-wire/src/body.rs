//! Request and response bodies for every rbus operation, field for field as the
//! C library writes them (`rbus/rbus.c`, `core/rbuscore.c`). Status codes are
//! kept as raw `i32`; interpret them with [`crate::status::Status::from_wire`].
//!
//! Errors carry only the status, except where noted: a failed set names the
//! failed parameter, and a failed method call still returns its output object.

use crate::error::{Error, Result};
use crate::event::{Event, Filter};
use crate::message::Body;
use crate::msgpack::{Reader, Writer};
use crate::value::{Object, Property, decode_properties, encode_properties};

/// True for a success status, including the legacy CCSP success code (100),
/// after which C peers send the same body as after 0.
fn succeeded(status: i32) -> bool {
    crate::status::Status::from_wire(status).is_success()
}

/// A response that carries only a status: delete row, commit, and any error
/// reply to an operation whose success body has more fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusResponse {
    /// Raw status code.
    pub status: i32,
}

impl Body for StatusResponse {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.status);
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(StatusResponse { status: r.i32()? })
    }
}

/// `METHOD_GETPARAMETERVALUES` request. Sent to the element name, or to the
/// owning component's name for partial paths and wildcards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRequest {
    /// The caller's component name.
    pub requesting_component: String,
    /// Names, partial paths (`Device.WiFi.`) or wildcards (`A.*.B`).
    pub names: Vec<String>,
}

impl Body for GetRequest {
    fn encode(&self, w: &mut Writer) {
        w.str(&self.requesting_component);
        w.i32(self.names.len() as i32);
        for n in &self.names {
            w.str(n);
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let requesting_component = r.str()?;
        let count = r.count("names")?;
        let names = (0..count).map(|_| r.str()).collect::<Result<_>>()?;
        Ok(GetRequest {
            requesting_component,
            names,
        })
    }
}

/// `METHOD_GETPARAMETERVALUES` response.
#[derive(Debug, Clone, PartialEq)]
pub struct GetResponse {
    /// Raw status code.
    pub status: i32,
    /// The values, when the status is success.
    pub properties: Vec<Property>,
}

impl Body for GetResponse {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.status);
        if self.status == 0 {
            encode_properties(&self.properties, w);
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let status = r.i32()?;
        let properties = if succeeded(status) && !r.is_empty() {
            decode_properties(r)?
        } else {
            Vec::new()
        };
        Ok(GetResponse { status, properties })
    }
}

/// `METHOD_SETPARAMETERVALUES` request.
#[derive(Debug, Clone, PartialEq)]
pub struct SetRequest {
    /// Legacy session id; 0 outside a session.
    pub session_id: u32,
    /// The caller's component name.
    pub component: String,
    /// Ask the provider to restore old values if any set fails.
    pub rollback: bool,
    /// Names and new values.
    pub properties: Vec<Property>,
    /// False only for the non-final sets of a legacy session.
    pub commit: bool,
}

impl Body for SetRequest {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.session_id as i32);
        w.str(&self.component);
        w.flag(self.rollback);
        encode_properties(&self.properties, w);
        w.str(if self.commit { "TRUE" } else { "FALSE" });
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let session_id = r.i32()? as u32;
        let component = r.str()?;
        let rollback = r.flag()?;
        let properties = decode_properties(r)?;
        let commit = r.is_empty() || r.str()?.eq_ignore_ascii_case("TRUE");
        Ok(SetRequest {
            session_id,
            component,
            rollback,
            properties,
            commit,
        })
    }
}

/// `METHOD_SETPARAMETERVALUES` response.
#[derive(Debug, Clone, PartialEq)]
pub struct SetResponse {
    /// Raw status code.
    pub status: i32,
    /// On success: the previous values when rollback was requested, else empty.
    pub old_values: Vec<Property>,
    /// On failure: the parameter that failed, if the provider said.
    pub failed_element: Option<String>,
}

impl Body for SetResponse {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.status);
        if self.status == 0 {
            encode_properties(&self.old_values, w);
        } else if let Some(name) = &self.failed_element {
            w.str(name);
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let status = r.i32()?;
        let mut out = SetResponse {
            status,
            old_values: Vec::new(),
            failed_element: None,
        };
        if !r.is_empty() {
            if succeeded(status) {
                out.old_values = decode_properties(r)?;
            } else {
                out.failed_element = Some(r.str()?);
            }
        }
        Ok(out)
    }
}

/// `METHOD_GETPARAMETERNAMES` request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamesMode {
    /// Element names with type and access flags (`rbusElementInfo_get`).
    Elements,
    /// A table's row instance numbers and aliases (`rbusTable_getRowNames`).
    Rows,
}

/// `METHOD_GETPARAMETERNAMES` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamesRequest {
    /// The element, object or table to start from.
    pub name: String,
    /// Depth: 0 = the element, n = n levels, -n = exactly level n, -1 = all
    /// rows (row mode).
    pub depth: i32,
    /// Elements or rows. CCSP peers omit the field; that reads as `Elements`.
    pub mode: NamesMode,
}

impl Body for NamesRequest {
    fn encode(&self, w: &mut Writer) {
        w.str(&self.name);
        w.i32(self.depth);
        w.flag(self.mode == NamesMode::Rows);
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let name = r.str()?;
        let depth = r.i32()?;
        let rows = !r.is_empty() && r.flag()?;
        Ok(NamesRequest {
            name,
            depth,
            mode: if rows {
                NamesMode::Rows
            } else {
                NamesMode::Elements
            },
        })
    }
}

/// One element in an element-names response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElementName {
    /// Fully qualified name; objects and tables end in `.`.
    pub name: String,
    /// `rbusElementType_t`: 0 object, 1 property, 2 table, 3 event, 4 method.
    pub element_type: i32,
    /// `rbusAccess_t` bits: get 1, set 2, add row 4, remove row 8,
    /// subscribe 16, invoke 32.
    pub access: u32,
}

/// `METHOD_GETPARAMETERNAMES` response in [`NamesMode::Elements`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElementNamesResponse {
    /// Raw status code.
    pub status: i32,
    /// The elements, on success.
    pub elements: Vec<ElementName>,
}

impl Body for ElementNamesResponse {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.status);
        if self.status == 0 {
            w.i32(self.elements.len() as i32);
            for e in &self.elements {
                w.str(&e.name);
                w.i32(e.element_type);
                w.i32(e.access as i32);
            }
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let status = r.i32()?;
        let mut elements = Vec::new();
        if succeeded(status) && !r.is_empty() {
            for _ in 0..r.count("element names")? {
                elements.push(ElementName {
                    name: r.str()?,
                    element_type: r.i32()?,
                    access: r.i32()? as u32,
                });
            }
        }
        Ok(ElementNamesResponse { status, elements })
    }
}

/// One table row in a row-names response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowName {
    /// Instance number.
    pub instance: u32,
    /// Alias, if the row has one.
    pub alias: Option<String>,
}

/// `METHOD_GETPARAMETERNAMES` response in [`NamesMode::Rows`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowNamesResponse {
    /// Raw status code.
    pub status: i32,
    /// The rows, on success.
    pub rows: Vec<RowName>,
}

impl Body for RowNamesResponse {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.status);
        if self.status == 0 {
            w.i32(self.rows.len() as i32);
            for row in &self.rows {
                w.i32(row.instance as i32);
                w.str(row.alias.as_deref().unwrap_or(""));
            }
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let status = r.i32()?;
        let mut rows = Vec::new();
        if succeeded(status) && !r.is_empty() {
            for _ in 0..r.count("row names")? {
                let instance = r.i32()? as u32;
                let alias = Some(r.str()?).filter(|a| !a.is_empty());
                rows.push(RowName { instance, alias });
            }
        }
        Ok(RowNamesResponse { status, rows })
    }
}

/// `METHOD_ADDTBLROW` request, sent to the table name (`Device.X.Table.`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRowRequest {
    /// Legacy session id; 0 outside a session.
    pub session_id: u32,
    /// The table, ending in `.`.
    pub table: String,
    /// Alias for the new row.
    pub alias: Option<String>,
}

impl Body for AddRowRequest {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.session_id as i32);
        w.str(&self.table);
        w.str(self.alias.as_deref().unwrap_or(""));
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let session_id = r.i32()? as u32;
        let table = r.str()?;
        // CCSP callers leave the alias out (`rbus.c:2359-2361`).
        let alias = if r.is_empty() { None } else { Some(r.str()?) };
        Ok(AddRowRequest {
            session_id,
            table,
            alias: alias.filter(|a| !a.is_empty()),
        })
    }
}

/// `METHOD_ADDTBLROW` response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddRowResponse {
    /// Raw status code.
    pub status: i32,
    /// The new row's instance number, on success.
    pub instance: Option<u32>,
}

impl Body for AddRowResponse {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.status);
        if let Some(i) = self.instance {
            w.i32(i as i32);
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let status = r.i32()?;
        let instance = if r.is_empty() {
            None
        } else {
            Some(r.i32()? as u32)
        };
        Ok(AddRowResponse { status, instance })
    }
}

/// `METHOD_DELETETBLROW` request, sent to the row name (`Table.3.` or
/// `Table.[alias].`). The response is a [`StatusResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRowRequest {
    /// Legacy session id; 0 outside a session.
    pub session_id: u32,
    /// The row.
    pub row: String,
}

impl Body for DeleteRowRequest {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.session_id as i32);
        w.str(&self.row);
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(DeleteRowRequest {
            session_id: r.i32()? as u32,
            row: r.str()?,
        })
    }
}

/// `METHOD_RPC` request, sent to the method name (`Device.X.Reboot()`).
#[derive(Debug, Clone, PartialEq)]
pub struct RpcRequest {
    /// Legacy session id; 0 outside a session.
    pub session_id: u32,
    /// The method name.
    pub method: String,
    /// Input arguments.
    pub input: Option<Object>,
}

impl Body for RpcRequest {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.session_id as i32);
        w.str(&self.method);
        w.flag(self.input.is_some());
        if let Some(obj) = &self.input {
            obj.encode(w);
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let session_id = r.i32()? as u32;
        let method = r.str()?;
        let input = if r.flag()? {
            Some(Object::decode(r)?)
        } else {
            None
        };
        Ok(RpcRequest {
            session_id,
            method,
            input,
        })
    }
}

/// `METHOD_RPC` response. C providers return the output object on failure
/// too, carrying `error_code` and `error_string`.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcResponse {
    /// Raw status code.
    pub status: i32,
    /// Output arguments.
    pub output: Option<Object>,
}

impl Body for RpcResponse {
    fn encode(&self, w: &mut Writer) {
        w.i32(self.status);
        if let Some(obj) = &self.output {
            obj.encode(w);
        }
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let status = r.i32()?;
        let output = if r.is_empty() {
            None
        } else {
            Some(Object::decode(r)?)
        };
        Ok(RpcResponse { status, output })
    }
}

/// Subscription options, carried as a nested message in a subscribe request
/// and echoed back in every event.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeOptions {
    /// The subscriber's component id; events echo it.
    pub component_id: i32,
    /// Interval in seconds (0 = none).
    pub interval: u32,
    /// Duration in seconds (0 = none).
    pub duration: u32,
    /// Filter, if any.
    pub filter: Option<Filter>,
}

impl SubscribeOptions {
    fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.i32(self.component_id);
        w.i32(self.interval as i32);
        w.i32(self.duration as i32);
        w.flag(self.filter.is_some());
        if let Some(f) = &self.filter {
            f.encode(&mut w);
        }
        w.into_bytes()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        Ok(SubscribeOptions {
            component_id: r.i32()?,
            interval: r.i32()? as u32,
            duration: r.i32()? as u32,
            filter: if r.flag()? {
                Some(Filter::decode(&mut r)?)
            } else {
                None
            },
        })
    }
}

/// `METHOD_SUBSCRIBE` / `METHOD_UNSUBSCRIBE` request, sent to the event name.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeRequest {
    /// The event, property or table to subscribe to.
    pub event_name: String,
    /// The subscriber's inbox, where events will be sent.
    pub subscriber: String,
    /// Options; C always sends them.
    pub options: Option<SubscribeOptions>,
    /// Ask for the current value in the response.
    pub publish_on_subscribe: bool,
    /// Raw-data subscription (out of scope for rbus-rs).
    pub raw_data: bool,
}

impl Body for SubscribeRequest {
    fn encode(&self, w: &mut Writer) {
        w.str(&self.event_name);
        w.str(&self.subscriber);
        w.flag(self.options.is_some());
        if let Some(opts) = &self.options {
            w.bin(&opts.encode());
        }
        w.flag(self.publish_on_subscribe);
        w.flag(self.raw_data);
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let event_name = r.str()?;
        let subscriber = r.str()?;
        let options = if r.flag()? {
            Some(SubscribeOptions::decode(r.bin()?)?)
        } else {
            None
        };
        Ok(SubscribeRequest {
            event_name,
            subscriber,
            options,
            publish_on_subscribe: !r.is_empty() && r.flag()?,
            raw_data: !r.is_empty() && r.flag()?,
        })
    }
}

/// The initial-value part of a subscribe response.
#[derive(Debug, Clone, PartialEq)]
pub enum InitialValue {
    /// The request did not ask for one, so the field is absent.
    NotRequested,
    /// Asked for, but the provider had none (for example, no getter).
    Unavailable,
    /// The current value, as an [`crate::event::EventKind::InitialValue`] event.
    Value(Box<Event>),
}

/// `METHOD_SUBSCRIBE` response. Its layout depends on whether the request set
/// `publish_on_subscribe`, so it is decoded with
/// [`SubscribeResponse::decode_for`] rather than through [`Body`].
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeResponse {
    /// Raw status code (an rbus or rbuscore code, depending on the failure).
    pub status: i32,
    /// The initial value, on success.
    pub initial: InitialValue,
    /// The provider's id for the subscription, on success.
    pub subscription_id: Option<i32>,
}

impl SubscribeResponse {
    /// Writes the body.
    pub fn encode(&self, w: &mut Writer) {
        w.i32(self.status);
        match &self.initial {
            InitialValue::NotRequested => {}
            InitialValue::Unavailable => w.flag(false),
            InitialValue::Value(ev) => {
                w.flag(true);
                ev.encode(w);
            }
        }
        if let Some(id) = self.subscription_id {
            w.i32(id);
        }
    }

    /// Reads the body of a response to a request with the given
    /// `publish_on_subscribe`.
    pub fn decode_for(r: &mut Reader<'_>, publish_on_subscribe: bool) -> Result<Self> {
        let status = r.i32()?;
        let mut out = SubscribeResponse {
            status,
            initial: InitialValue::NotRequested,
            subscription_id: None,
        };
        if status != 0 || r.is_empty() {
            return Ok(out);
        }
        if publish_on_subscribe {
            out.initial = if r.flag()? {
                InitialValue::Value(Box::new(Event::decode(r)?))
            } else {
                InitialValue::Unavailable
            };
        }
        if !r.is_empty() {
            out.subscription_id = Some(r.i32()?);
        }
        Ok(out)
    }
}

/// Fails if a reader still has items after a body was decoded. Useful in
/// strict contexts such as tests and fixture checks.
pub fn expect_end(r: &Reader<'_>, what: &'static str) -> Result<()> {
    if r.is_empty() {
        Ok(())
    } else {
        Err(Error::InvalidValue {
            ty: what,
            reason: "unexpected trailing items".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventKind, RelationOp};
    use crate::value::Value;

    fn round_trip<B: Body + PartialEq + std::fmt::Debug>(b: &B) -> Vec<u8> {
        let mut w = Writer::new();
        b.encode(&mut w);
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        assert_eq!(&B::decode(&mut r).unwrap(), b);
        expect_end(&r, "test body").unwrap();
        bytes
    }

    #[test]
    fn get_bodies() {
        // get_single fixture request body.
        let bytes = round_trip(&GetRequest {
            requesting_component: "rbuscli-33".into(),
            names: vec!["Device.SampleProvider.SampleData.IntData".into()],
        });
        assert_eq!(&bytes[..12], b"\xabrbuscli-33\x00");
        assert_eq!(bytes[12], 1);
        round_trip(&GetResponse {
            status: 0,
            properties: vec![Property::new("a", Value::I32(101))],
        });
        round_trip(&GetResponse {
            status: 17,
            properties: vec![],
        });
    }

    #[test]
    fn set_bodies() {
        let req = SetRequest {
            session_id: 0,
            component: "rbuscli-91".into(),
            rollback: false,
            properties: vec![Property::new("x", Value::I32(42))],
            commit: true,
        };
        let bytes = round_trip(&req);
        assert_eq!(&bytes[bytes.len() - 6..], b"\xa5TRUE\x00");
        round_trip(&SetRequest {
            commit: false,
            rollback: true,
            session_id: 7,
            ..req
        });
        round_trip(&SetResponse {
            status: 0,
            old_values: vec![Property::new("x", Value::I32(143))],
            failed_element: None,
        });
        round_trip(&SetResponse {
            status: 27,
            old_values: vec![],
            failed_element: Some("Device.X".into()),
        });
        let mut r = Reader::new(&[0x02]);
        assert_eq!(
            SetResponse::decode(&mut r).unwrap().failed_element,
            None,
            "status only"
        );
    }

    #[test]
    fn names_bodies() {
        let bytes = round_trip(&NamesRequest {
            name: "Device.Tables1.T1.".into(),
            depth: -1,
            mode: NamesMode::Rows,
        });
        assert_eq!(&bytes[bytes.len() - 2..], &[0xff, 0x01]);
        round_trip(&NamesRequest {
            name: "Device.".into(),
            depth: 2,
            mode: NamesMode::Elements,
        });
        // CCSP omits the mode field.
        let mut w = Writer::new();
        w.str("Device.");
        w.i32(1);
        let b = w.into_bytes();
        assert_eq!(
            NamesRequest::decode(&mut Reader::new(&b)).unwrap().mode,
            NamesMode::Elements
        );
        round_trip(&RowNamesResponse {
            status: 0,
            rows: vec![
                RowName {
                    instance: 1,
                    alias: Some("myalias".into()),
                },
                RowName {
                    instance: 2,
                    alias: None,
                },
            ],
        });
        round_trip(&RowNamesResponse {
            status: 2,
            rows: vec![],
        });
        round_trip(&ElementNamesResponse {
            status: 0,
            elements: vec![ElementName {
                name: "Device.Tables1.T1.1.".into(),
                element_type: 0,
                access: 3,
            }],
        });
        round_trip(&ElementNamesResponse {
            status: 17,
            elements: vec![],
        });
    }

    #[test]
    fn table_bodies() {
        round_trip(&AddRowRequest {
            session_id: 0,
            table: "Device.Tables1.T1.".into(),
            alias: Some("myalias".into()),
        });
        round_trip(&AddRowRequest {
            session_id: 0,
            table: "T.".into(),
            alias: None,
        });
        round_trip(&AddRowResponse {
            status: 0,
            instance: Some(1),
        });
        round_trip(&AddRowResponse {
            status: 9,
            instance: None,
        });
        round_trip(&DeleteRowRequest {
            session_id: 0,
            row: "Device.Tables1.T1.[myalias].".into(),
        });
        round_trip(&StatusResponse { status: 0 });
        // A CCSP add-row request has no alias at all.
        let mut w = Writer::new();
        w.i32(0);
        w.str("T.");
        let bytes = w.into_bytes();
        let req = AddRowRequest::decode(&mut Reader::new(&bytes)).unwrap();
        assert_eq!((req.table.as_str(), req.alias), ("T.", None));
    }

    #[test]
    fn rpc_bodies() {
        let input = Object::new()
            .with("param1", Value::String("hello".into()))
            .with("param2", Value::I32(5));
        round_trip(&RpcRequest {
            session_id: 0,
            method: "Device.Methods.SimpleMethod()".into(),
            input: Some(input),
        });
        round_trip(&RpcRequest {
            session_id: 0,
            method: "M()".into(),
            input: None,
        });
        round_trip(&RpcResponse {
            status: 22,
            output: Some(Object::new().with("error_code", Value::I32(56789))),
        });
        round_trip(&RpcResponse {
            status: 1,
            output: None,
        });
    }

    #[test]
    fn subscribe_bodies() {
        let req = SubscribeRequest {
            event_name: "Device.Provider1.Param1".into(),
            subscriber: "rbus.rbuscli.INBOX.450".into(),
            options: Some(SubscribeOptions {
                component_id: 1,
                interval: 0,
                duration: 0,
                filter: Some(Filter::Relation {
                    op: RelationOp::GreaterThan,
                    value: Value::I32(2),
                }),
            }),
            publish_on_subscribe: true,
            raw_data: false,
        };
        let bytes = round_trip(&req);
        // The nested options message, as recorded in subscribe_filter_initial.
        let opts = b"\x01\x00\x00\x01\x00\x00\xa7filter\x00\xcd\x05\x07\x02";
        assert!(bytes.windows(opts.len()).any(|w| w == opts));
        round_trip(&SubscribeRequest {
            options: None,
            ..req.clone()
        });

        let initial = Event {
            name: "Device.Provider1.Param1".into(),
            kind: EventKind::InitialValue,
            data: Some(Object::new().with("initialValue", Value::I32(1))),
            filter: req.options.as_ref().unwrap().filter.clone(),
            interval: 0,
            duration: 0,
            component_id: 1,
        };
        for (resp, pos) in [
            (
                SubscribeResponse {
                    status: 0,
                    initial: InitialValue::Value(Box::new(initial)),
                    subscription_id: Some(3),
                },
                true,
            ),
            (
                SubscribeResponse {
                    status: 0,
                    initial: InitialValue::Unavailable,
                    subscription_id: Some(4),
                },
                true,
            ),
            (
                SubscribeResponse {
                    status: 0,
                    initial: InitialValue::NotRequested,
                    subscription_id: Some(3),
                },
                false,
            ),
            (
                SubscribeResponse {
                    status: 2,
                    initial: InitialValue::NotRequested,
                    subscription_id: None,
                },
                true,
            ),
        ] {
            let mut w = Writer::new();
            resp.encode(&mut w);
            let b = w.into_bytes();
            let mut r = Reader::new(&b);
            assert_eq!(SubscribeResponse::decode_for(&mut r, pos).unwrap(), resp);
            assert!(r.is_empty());
        }
    }

    #[test]
    fn trailing_items_are_reported() {
        let r = Reader::new(&[0x01]);
        assert!(expect_end(&r, "x").is_err());
        let mut w = Writer::new();
        Event {
            name: "e".into(),
            kind: EventKind::General,
            data: None,
            filter: None,
            interval: 0,
            duration: 0,
            component_id: 1,
        }
        .encode(&mut w);
        let b = w.into_bytes();
        assert_eq!(Event::decode(&mut Reader::new(&b)).unwrap().name, "e");
    }

    #[test]
    fn legacy_success_code_carries_a_body() {
        // CCSP peers answer 100 (CCSP_SUCCESS) and then the same body as 0.
        let mut w = Writer::new();
        w.i32(100);
        encode_properties(&[Property::new("Device.A", Value::I32(1))], &mut w);
        let b = w.into_bytes();
        let got = GetResponse::decode(&mut Reader::new(&b)).unwrap();
        assert_eq!(got.properties.len(), 1);
        let mut w = Writer::new();
        w.i32(100);
        w.i32(7);
        let b = w.into_bytes();
        let got = AddRowResponse::decode(&mut Reader::new(&b)).unwrap();
        assert_eq!(got.instance, Some(7));
    }
}
