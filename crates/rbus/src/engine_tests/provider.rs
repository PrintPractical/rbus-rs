//! M4 engine tests: what a provider sends and answers over the socket.

use bytes::Bytes;
use rbus_wire::body::{GetRequest, GetResponse, StatusResponse};
use rbus_wire::control::{self, ResultReply, Route};
use rbus_wire::frame::{Flags, Frame, Header};
use rbus_wire::message::{Body, Method, TraceContext, request_payload, split_method_payload};
use rbus_wire::value::Value;

use super::{Listener, Peer, short};
use crate::provider::{Access, Context, Fault, Provider, Rows, Schema};
use crate::{Error, PropertyName, Status};

pub(super) struct Model;

impl Provider for Model {
    fn schema(&self) -> Schema {
        Schema::new()
            .property("Device.M.Value", Access::ReadOnly)
            .table("Device.M.T.", Rows::Fixed)
            .method("Device.M.Do()")
    }

    async fn get(&self, cx: &Context, _name: &PropertyName) -> Result<Value, Fault> {
        Ok(Value::String(cx.caller().unwrap_or_default().to_owned()))
    }
}

/// Reads one alias registration and checks it.
pub(super) async fn expect_alias(peer: &mut Peer, add: i32, name: &str, route: u32) -> Frame {
    let f = peer.recv().await;
    assert_eq!(f.header.topic, control::TOPIC_SUBSCRIBE);
    let r: Route = control::decode(&f.payload).unwrap();
    assert_eq!((r.add, r.topic.as_str(), r.route_id), (add, name, route));
    let flags = if add == 1 {
        Flags::REQUEST
    } else {
        Flags::NONE
    };
    assert_eq!(f.header.flags, flags, "{name}");
    f
}

pub(super) const NAMES: [&str; 3] = ["Device.M.Value", "Device.M.T.{i}.", "Device.M.Do()"];

/// A request as the broker forwards it from a consumer's inbox.
fn inbound(seq: u32, topic: &str, method: Method, body: &impl Body) -> Frame {
    let payload = request_payload(method, TraceContext::default(), body);
    let header = Header::new(
        seq,
        Flags::REQUEST | Flags::RAW_BINARY,
        topic,
        "rbus.consumer.INBOX.42",
    );
    Frame::new(header, payload)
}

fn decode<B: Body>(frame: &Frame) -> B {
    let (mut body, meta) =
        split_method_payload(&frame.payload, &rbus_wire::Limits::DEFAULT).unwrap();
    assert_eq!(meta.method, Method::Response);
    B::decode(&mut body).unwrap()
}

#[tokio::test]
async fn p1_p3_register_serve_and_unregister() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let route = bus.inner.conn.component_route;
    let provide = tokio::spawn({
        let bus = bus.clone();
        async move { bus.provide(Model).serialized().await }
    });
    // Every alias is in flight before the first reply.
    let mut regs = Vec::new();
    for name in NAMES {
        regs.push(expect_alias(&mut peer, 1, name, route).await);
    }
    for f in &regs {
        peer.reply_json(f, &ResultReply { result: 0 }).await;
    }
    let provided = provide.await.unwrap().unwrap();
    assert!(format!("{provided:?}").contains("names: 3"));
    assert_eq!(provided.bus().component(), "test.component");

    // A get from a consumer is answered to its inbox, echoing the sequence
    // number, with topic and reply topic swapped.
    let req = GetRequest {
        requesting_component: "consumer".into(),
        names: vec!["Device.M.Value".into()],
    };
    peer.send(inbound(
        77,
        "Device.M.Value",
        Method::GetParameterValues,
        &req,
    ))
    .await;
    let resp = peer.recv().await;
    assert_eq!(resp.header.seq, 77);
    assert_eq!(resp.header.flags, Flags::RESPONSE | Flags::RAW_BINARY);
    assert_eq!(resp.header.topic, "rbus.consumer.INBOX.42");
    assert_eq!(resp.header.reply_topic, "Device.M.Value");
    let body: GetResponse = decode(&resp);
    assert_eq!(body.properties[0].value, Value::String("consumer".into()));

    // Direct mode is refused (P19).
    peer.send(inbound(78, "Device.M.Value", Method::OpenDirectConn, &req))
        .await;
    let resp: StatusResponse = decode(&peer.recv().await);
    assert_eq!(resp.status, 9);

    // A body that doesn't decode gets InvalidInput; a payload without meta
    // gets nothing.
    let mut garbage = request_payload(
        Method::GetParameterValues,
        TraceContext::default(),
        &StatusResponse { status: 1 },
    );
    peer.send(Frame::new(
        Header::new(
            79,
            Flags::REQUEST | Flags::RAW_BINARY,
            "Device.M.Value",
            "x",
        ),
        garbage.clone(),
    ))
    .await;
    let resp: StatusResponse = decode(&peer.recv().await);
    assert_eq!(Status::from_wire(resp.status), Status::InvalidInput);
    garbage.truncate(3);
    peer.send(Frame::new(
        Header::new(
            80,
            Flags::REQUEST | Flags::RAW_BINARY,
            "Device.M.Value",
            "x",
        ),
        Bytes::from(garbage),
    ))
    .await;

    // The provider's own rows.
    provided.add_row("Device.M.T.", 1).unwrap();
    assert_eq!(provided.rows("Device.M.T.").unwrap().len(), 1);
    assert!(matches!(
        provided.add_row("Device.M.T.", 1),
        Err(Error::Status {
            status: Status::ElementNameDuplicate,
            ..
        })
    ));
    assert!(matches!(
        provided.add_row("bad name.", 1),
        Err(Error::Name(_))
    ));
    provided.remove_row("Device.M.T.", 1).unwrap();
    assert!(matches!(
        provided.remove_row("Device.M.T.", 1),
        Err(Error::Status {
            status: Status::ElementDoesNotExist,
            ..
        })
    ));
    assert!(matches!(
        provided.rows("Device.M.Nope."),
        Err(Error::Status {
            status: Status::ElementDoesNotExist,
            ..
        })
    ));

    // Dropping it removes every alias, fire and forget.
    drop(provided);
    for name in NAMES {
        expect_alias(&mut peer, 0, name, route).await;
    }
    // Requests for the names now find nothing.
    peer.send(inbound(
        81,
        "Device.M.Value",
        Method::GetParameterValues,
        &req,
    ))
    .await;
    let resp: GetResponse = decode(&peer.recv().await);
    assert_eq!(Status::from_wire(resp.status), Status::ElementDoesNotExist);
}

#[tokio::test]
async fn a_refused_alias_unregisters_the_others() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let route = bus.inner.conn.component_route;
    for (code, status) in [
        (1001, Status::ElementNameDuplicate),
        (1003, Status::InvalidNamespace),
        (5, Status::BusError),
    ] {
        let provide = tokio::spawn({
            let bus = bus.clone();
            async move { bus.provide(Model).await }
        });
        let mut regs = Vec::new();
        for name in NAMES {
            regs.push(expect_alias(&mut peer, 1, name, route).await);
        }
        peer.reply_json(&regs[0], &ResultReply { result: 0 }).await;
        peer.reply_json(&regs[1], &ResultReply { result: code })
            .await;
        peer.reply_json(&regs[2], &ResultReply { result: 0 }).await;
        let err = provide.await.unwrap().unwrap_err();
        assert!(
            matches!(&err, Error::Status { name, status: s } if *s == status && name == "Device.M.T.{i}."),
            "{err:?}"
        );
        expect_alias(&mut peer, 0, NAMES[0], route).await;
        expect_alias(&mut peer, 0, NAMES[2], route).await;
    }
    // A broker that doesn't answer is a timeout; nothing stays registered.
    let quick = Listener::new();
    let (bus, mut peer) = quick
        .connect(crate::Timeouts::uniform(std::time::Duration::from_millis(
            100,
        )))
        .await;
    let err = bus.provide(Model).await.unwrap_err();
    assert!(matches!(err, Error::Timeout { op: "register" }), "{err:?}");
    for name in NAMES {
        expect_alias(&mut peer, 1, name, bus.inner.conn.component_route).await;
    }
    // The model can be registered again: the failed attempt left nothing.
    let again = tokio::spawn({
        let bus = bus.clone();
        async move { bus.provide(Model).await }
    });
    for name in NAMES {
        let f = expect_alias(&mut peer, 1, name, bus.inner.conn.component_route).await;
        peer.reply_json(&f, &ResultReply { result: 0 }).await;
    }
    again.await.unwrap().unwrap();
}

#[tokio::test]
async fn bad_schemas_fail_before_any_io() {
    struct Bad(Schema);
    impl Provider for Bad {
        fn schema(&self) -> Schema {
            self.0.clone()
        }
        async fn get(&self, _cx: &Context, _n: &PropertyName) -> Result<Value, Fault> {
            Ok(Value::None)
        }
    }
    let l = Listener::new();
    let (bus, peer) = l.connect(short()).await;
    let cases = [
        (Schema::new().property("A B", Access::ReadOnly), None),
        (Schema::new().method("A.B"), None),
        (
            Schema::new()
                .property("A.B", Access::ReadOnly)
                .property("A.B", Access::ReadOnly),
            Some(Status::ElementNameDuplicate),
        ),
        (
            Schema::new()
                .property("A.B", Access::ReadOnly)
                .property("A.B.C", Access::ReadOnly),
            Some(Status::InvalidNamespace),
        ),
    ];
    for (schema, want) in cases {
        let err = bus.provide(Bad(schema)).await.unwrap_err();
        match want {
            None => assert!(matches!(err, Error::Name(_)), "{err:?}"),
            Some(s) => assert!(
                matches!(err, Error::Status { status, .. } if status == s),
                "{err:?}"
            ),
        }
    }
    // A closed connection can't register anything.
    drop(peer);
    bus.close().await.unwrap();
    let err = bus.provide(Model).await.unwrap_err();
    assert!(matches!(err, Error::Disconnected), "{err:?}");
}

/// A model written with the derives (M6), registered over the scripted
/// socket.
#[cfg(feature = "macros")]
mod derived {
    use rbus_wire::body::{AddRowRequest, AddRowResponse, DeleteRowRequest};
    use rbus_wire::value::Object;

    use super::*;
    use crate::provider::{Event, Prop, Table};

    #[derive(crate::Model, Default)]
    #[rbus(path = "Device.H.")]
    struct Hosts {
        #[rbus(writable)]
        count: Prop<u32>,
        #[rbus(name = "Host", add_row, remove_row)]
        hosts: Table<Host>,
        alarm: Event<Object>,
    }

    #[derive(crate::Row, Clone, PartialEq, Default, Debug)]
    struct Host {
        name: String,
        #[rbus(add_row)]
        ip: Table<Ip>,
    }

    #[derive(crate::Row, Clone, PartialEq, Default, Debug)]
    struct Ip {
        address: String,
    }

    const NAMES: [&str; 6] = [
        "Device.H.Count",
        "Device.H.Host.{i}.",
        "Device.H.Host.{i}.Name",
        "Device.H.Host.{i}.Ip.{i}.",
        "Device.H.Host.{i}.Ip.{i}.Address",
        "Device.H.Alarm!",
    ];

    #[tokio::test]
    async fn pt3_a_model_records_its_rows_and_consumers_rows() {
        let l = Listener::new();
        let (bus, mut peer) = l.connect(short()).await;
        let route = bus.inner.conn.component_route;
        // Rows that exist before registration are recorded with it.
        let model = Hosts::default();
        let host = Host {
            name: "router".into(),
            ..Host::default()
        };
        host.ip
            .insert(Ip {
                address: "10.0.0.1".into(),
            })
            .unwrap();
        model.hosts.insert(host).unwrap();
        let provide = tokio::spawn({
            let bus = bus.clone();
            async move { bus.provide(model).await }
        });
        let mut regs = Vec::new();
        for name in NAMES {
            regs.push(expect_alias(&mut peer, 1, name, route).await);
        }
        for f in &regs {
            peer.reply_json(f, &ResultReply { result: 0 }).await;
        }
        let hosts = provide.await.unwrap().unwrap();
        let handle = hosts.handle();
        assert!(format!("{handle:?}").contains("connected: true"));
        assert_eq!(hosts.rows("Device.H.Host.").unwrap().len(), 1);
        assert_eq!(hosts.rows("Device.H.Host.1.Ip.").unwrap().len(), 1);

        // A consumer's row is recorded by the table itself; the reply
        // carries its number, and the engine doesn't add it twice.
        let req = AddRowRequest {
            session_id: 0,
            table: "Device.H.Host.".into(),
            alias: Some("lan".into()),
        };
        peer.send(inbound(5, "Device.H.Host.", Method::AddTableRow, &req))
            .await;
        let resp: AddRowResponse = decode(&peer.recv().await);
        assert_eq!((resp.status, resp.instance), (0, Some(2)));
        assert_eq!(hosts.hosts.find("lan"), Some(2));
        assert_eq!(hosts.rows("Device.H.Host.").unwrap().len(), 2);
        let req = AddRowRequest {
            session_id: 0,
            table: "Device.H.Host.2.Ip.".into(),
            alias: None,
        };
        peer.send(inbound(6, "Device.H.Host.2.Ip.", Method::AddTableRow, &req))
            .await;
        let resp: AddRowResponse = decode(&peer.recv().await);
        assert_eq!((resp.status, resp.instance), (0, Some(1)));
        // Without subscribers, changes send nothing.
        hosts.count.set(3);
        hosts
            .hosts
            .update(1, |h| h.name = "gateway".into())
            .unwrap();
        hosts.alarm.publish(Object::new()).unwrap();
        let req = DeleteRowRequest {
            session_id: 0,
            row: "Device.H.Host.2.".into(),
        };
        peer.send(inbound(7, "Device.H.Host.2.", Method::DeleteTableRow, &req))
            .await;
        let resp: StatusResponse = decode(&peer.recv().await);
        assert_eq!(resp.status, 0);
        assert_eq!(hosts.hosts.len(), 1);
        assert!(hosts.rows("Device.H.Host.2.Ip.").is_err());

        // Once the connection is gone, the handle says so.
        drop(peer);
        bus.close().await.unwrap();
        assert!(matches!(
            handle.add_row("Device.H.Host.", 9),
            Err(Error::Disconnected)
        ));
        assert!(matches!(
            hosts.hosts.insert(Host::default()),
            Err(Error::Disconnected)
        ));
        assert!(matches!(
            hosts.alarm.publish(Object::new()),
            Err(Error::Disconnected)
        ));
        hosts.count.set(4);
        // The row goes even though the engine can't be told.
        assert_eq!(hosts.hosts.remove(1).unwrap().name, "gateway");
        assert!(handle.subscribers(None).is_empty());
        assert!(handle.subscription_events().next().await.is_none());
        assert!(handle.notify_changed("Device.H.Count").await.is_err());
        assert!(
            handle
                .publish_value_change("Device.H.Count", 1u32, 2u32)
                .is_err()
        );
        assert!(handle.publish("Device.H.Alarm!", &Object::new()).is_err());
        assert!(handle.remove_row("Device.H.Host.", 1).is_err());
        assert!(handle.rows("Device.H.Host.").is_err());
        assert!(format!("{handle:?}").contains("connected: false"));
    }
}

#[tokio::test]
async fn p3_a_cancelled_registration_leaves_no_names_behind() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let route = bus.inner.conn.component_route;
    let provide = tokio::spawn({
        let bus = bus.clone();
        async move { bus.provide(Model).await }
    });
    let mut regs = Vec::new();
    for name in NAMES {
        regs.push(expect_alias(&mut peer, 1, name, route).await);
    }
    // The caller gives up while the requests are in flight.
    provide.abort();
    assert!(provide.await.unwrap_err().is_cancelled());
    // One name turns out to be taken; the other two registered, so they are
    // ours to remove again. The taken one is not: rtrouted would remove the
    // other component's alias.
    peer.reply_json(&regs[0], &ResultReply { result: 1001 })
        .await;
    for f in &regs[1..] {
        peer.reply_json(f, &ResultReply { result: 0 }).await;
    }
    for name in &NAMES[1..] {
        expect_alias(&mut peer, 0, name, route).await;
    }
    assert!(bus.inner.conn.aliases().is_empty());

    // The names can be claimed again.
    let provide = tokio::spawn({
        let bus = bus.clone();
        async move { bus.provide(Model).await }
    });
    let mut regs = Vec::new();
    for name in NAMES {
        regs.push(expect_alias(&mut peer, 1, name, route).await);
    }
    for f in &regs {
        peer.reply_json(f, &ResultReply { result: 0 }).await;
    }
    let provided = provide.await.unwrap().unwrap();
    assert_eq!(bus.inner.conn.aliases().len(), NAMES.len());
    drop(provided);
    for name in NAMES {
        expect_alias(&mut peer, 0, name, route).await;
    }
}
