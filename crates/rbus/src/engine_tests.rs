//! Connection-engine tests against a scripted socket peer.
//!
//! These cover what a real rtrouted can't be made to do on demand: replies that
//! never come, a broker that vanishes mid-request, garbage on the socket, and
//! exact checks of what we send. Feature behaviour is tested against the real
//! broker in `tests/` (decisions.md D13, D25).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rbus_wire::body::{GetRequest, GetResponse, RpcRequest, RpcResponse, SetRequest, SetResponse};
use rbus_wire::control::{self, DiscoveryReply, NameList, Route};
use rbus_wire::frame::{Flags, Frame, FrameDecoder, Header};
use rbus_wire::message::{Body, Method, response_payload, split_method_payload};
use rbus_wire::value::{Object, Property, Value};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::{ConnectionState, Error, Rbus, Status, Timeouts};

static NEXT_SOCKET: AtomicU32 = AtomicU32::new(0);

/// A listening socket standing in for rtrouted.
struct Listener {
    listener: UnixListener,
    path: PathBuf,
}

impl Listener {
    fn new() -> Listener {
        let path = std::env::temp_dir().join(format!(
            "rbus-rs-{}-{}.sock",
            std::process::id(),
            NEXT_SOCKET.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        Listener { listener, path }
    }

    /// Connects a bus to this socket and returns both ends, with the two
    /// registration frames already consumed.
    async fn connect(&self, timeouts: Timeouts) -> (Rbus, Peer) {
        let bus = Rbus::builder("test.component")
            .socket(&self.path)
            .timeouts(timeouts)
            .connect()
            .await
            .unwrap();
        let (stream, _) = self.listener.accept().await.unwrap();
        let mut peer = Peer {
            stream,
            decoder: FrameDecoder::new(),
        };
        peer.recv().await;
        peer.recv().await;
        (bus, peer)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn short() -> Timeouts {
    Timeouts::uniform(Duration::from_secs(5))
}

/// The broker's end of one connection.
struct Peer {
    stream: UnixStream,
    decoder: FrameDecoder,
}

impl Peer {
    async fn try_recv(&mut self) -> Option<Frame> {
        loop {
            if let Some(f) = self.decoder.next_frame().unwrap() {
                return Some(f);
            }
            if self
                .stream
                .read_buf(self.decoder.buffer_mut())
                .await
                .unwrap()
                == 0
            {
                return None;
            }
        }
    }

    async fn recv(&mut self) -> Frame {
        self.try_recv().await.expect("connection closed")
    }

    async fn send(&mut self, frame: Frame) {
        let bytes = frame.encode_unchecked().unwrap();
        self.stream.write_all(&bytes).await.unwrap();
    }

    /// Answers `req` the way rtrouted relays a provider's response.
    async fn reply(&mut self, req: &Frame, flags: Flags, payload: Vec<u8>) {
        let h = Header::new(
            req.header.seq,
            Flags::RESPONSE | flags,
            req.header.reply_topic.clone(),
            req.header.topic.clone(),
        );
        self.send(Frame::new(h, payload)).await;
    }

    async fn reply_rbus(&mut self, req: &Frame, body: &impl Body) {
        self.reply(req, Flags::RAW_BINARY, response_payload(body))
            .await;
    }

    async fn reply_json<T: Serialize>(&mut self, req: &Frame, msg: &T) {
        self.reply(req, Flags::NONE, control::encode(msg)).await;
    }

    async fn undeliverable(&mut self, req: &Frame) {
        self.reply(req, Flags::UNDELIVERABLE | Flags::RAW_BINARY, Vec::new())
            .await;
    }
}

fn request<B: Body>(frame: &Frame, method: Method) -> B {
    assert_eq!(frame.header.flags, Flags::REQUEST | Flags::RAW_BINARY);
    let (mut body, meta) = split_method_payload(&frame.payload).unwrap();
    assert_eq!(meta.method, method);
    B::decode(&mut body).unwrap()
}

fn get_ok(props: Vec<Property>) -> GetResponse {
    GetResponse {
        status: 0,
        properties: props,
    }
}

#[tokio::test]
async fn l1_registers_inbox_then_component() {
    let l = Listener::new();
    let bus = Rbus::builder("test.component")
        .socket(&l.path)
        .connect()
        .await
        .unwrap();
    let (stream, _) = l.listener.accept().await.unwrap();
    let mut peer = Peer {
        stream,
        decoder: FrameDecoder::new(),
    };
    let inbox = peer.recv().await;
    let component = peer.recv().await;
    for f in [&inbox, &component] {
        assert_eq!(f.header.topic, control::TOPIC_SUBSCRIBE);
        assert_eq!(f.header.flags, Flags::NONE);
    }
    let inbox: Route = control::decode(&inbox.payload).unwrap();
    assert_eq!((inbox.add, inbox.route_id), (1, 1));
    assert_eq!(inbox.topic, bus.inbox());
    let pid = std::process::id().to_string();
    let parts: Vec<&str> = bus.inbox().split('.').collect();
    assert_eq!(parts.len(), 4, "{}", bus.inbox());
    assert_eq!(
        (parts[0], parts[2], parts[3]),
        ("rbus", "INBOX", pid.as_str())
    );
    let component: Route = control::decode(&component.payload).unwrap();
    assert_eq!(
        (component.add, component.topic.as_str()),
        (1, "test.component")
    );
    assert!(component.route_id >= 10_000);
    assert_eq!(bus.component(), "test.component");
    assert_eq!(*bus.state().borrow(), ConnectionState::Connected);
    assert_eq!(bus.timeouts(), &Timeouts::default());
    let dbg = format!("{bus:?}");
    assert!(
        dbg.contains("test.component") && dbg.contains("Connected"),
        "{dbg}"
    );
}

#[tokio::test]
async fn connect_errors() {
    let missing = Rbus::builder("c")
        .socket("/nonexistent/rtrouted")
        .connect()
        .await;
    assert!(matches!(missing, Err(Error::Io(_))));
    let bad = Rbus::builder("bad name").connect().await;
    assert!(matches!(bad, Err(Error::Name(e)) if e.kind == "component"));
}

#[tokio::test]
async fn c1_get_sends_one_request_to_the_name() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.A.B").await }
    });
    let req = peer.recv().await;
    assert_eq!(req.header.topic, "Device.A.B");
    assert_eq!(req.header.reply_topic, bus.inbox());
    let body: GetRequest = request(&req, Method::GetParameterValues);
    assert_eq!(body.requesting_component, "test.component");
    assert_eq!(body.names, ["Device.A.B"]);
    peer.reply_rbus(
        &req,
        &get_ok(vec![Property::new("Device.A.B", Value::U32(7))]),
    )
    .await;
    assert_eq!(get.await.unwrap().unwrap(), 7);
}

#[tokio::test]
async fn get_failures() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;

    // Undeliverable: nobody owns the name.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.Nope").await }
    });
    let req = peer.recv().await;
    peer.undeliverable(&req).await;
    assert!(matches!(get.await.unwrap(), Err(Error::NotFound(n)) if n == "Device.Nope"));

    // An error status from the provider, including a legacy CCSP code.
    for (code, status) in [
        (28, Status::NotReadable),
        (192, Status::DestinationNotFound),
    ] {
        let get = tokio::spawn({
            let bus = bus.clone();
            async move { bus.get::<u32>("Device.A").await }
        });
        let req = peer.recv().await;
        peer.reply_rbus(
            &req,
            &GetResponse {
                status: code,
                properties: vec![],
            },
        )
        .await;
        match get.await.unwrap() {
            Err(Error::Status { name, status: s }) => {
                assert_eq!((name.as_str(), s), ("Device.A", status))
            }
            other => panic!("{other:?}"),
        }
    }

    // A reply without the value asked for.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.A").await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(
        &req,
        &get_ok(vec![Property::new("Device.Other", Value::U32(1))]),
    )
    .await;
    assert!(matches!(get.await.unwrap(), Err(Error::Protocol(_))));

    // The wrong type.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.A").await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(
        &req,
        &get_ok(vec![Property::new("Device.A", Value::Bool(true))]),
    )
    .await;
    assert!(matches!(get.await.unwrap(), Err(Error::Type(_))));

    // A payload that isn't an rbus message.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.A").await }
    });
    let req = peer.recv().await;
    peer.reply(&req, Flags::RAW_BINARY, b"junk".to_vec()).await;
    assert!(matches!(get.await.unwrap(), Err(Error::Wire(_))));

    // Invalid names never reach the wire.
    assert!(matches!(
        bus.get::<u32>("Device.A()").await,
        Err(Error::Name(_))
    ));
}

#[tokio::test]
async fn l6_timeout_and_cancellation_leave_no_state() {
    let l = Listener::new();
    let (bus, mut peer) = l
        .connect(Timeouts::uniform(Duration::from_millis(50)))
        .await;
    assert!(matches!(
        bus.get::<u32>("Device.A").await,
        Err(Error::Timeout { op: "get" })
    ));
    assert_eq!(bus.inner.conn.pending_len(), 0);
    let late = peer.recv().await;

    // Cancel a call by dropping it.
    let bus2 = Rbus::clone(&bus);
    let call = async move { bus2.get::<u32>("Device.B").await };
    assert!(
        tokio::time::timeout(Duration::from_millis(10), call)
            .await
            .is_err()
    );
    assert_eq!(bus.inner.conn.pending_len(), 0);
    let cancelled = peer.recv().await;

    // Late replies to both are dropped, and the connection keeps working.
    peer.reply_rbus(&late, &get_ok(vec![])).await;
    peer.reply_rbus(&cancelled, &get_ok(vec![])).await;
    // So is a message that is not a response.
    peer.send(Frame::new(
        Header::new(99, Flags::RAW_BINARY, bus.inbox(), "x"),
        b"event".to_vec(),
    ))
    .await;
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<i32>("Device.C").await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(
        &req,
        &get_ok(vec![Property::new("Device.C", Value::I32(-1))]),
    )
    .await;
    assert_eq!(get.await.unwrap().unwrap(), -1);
}

#[tokio::test]
async fn broker_going_away_fails_pending_and_later_calls() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.A").await }
    });
    peer.recv().await;
    drop(peer);
    assert!(matches!(get.await.unwrap(), Err(Error::Disconnected)));
    let mut state = bus.state();
    state
        .wait_for(|s| *s == ConnectionState::Closed)
        .await
        .unwrap();
    assert!(matches!(
        bus.get::<u32>("Device.A").await,
        Err(Error::Disconnected)
    ));
    bus.close().await.unwrap();
}

#[tokio::test]
async fn garbage_from_the_broker_closes_the_connection() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    peer.stream.write_all(&[0u8; 64]).await.unwrap();
    bus.state()
        .wait_for(|s| *s == ConnectionState::Closed)
        .await
        .unwrap();
}

#[tokio::test]
async fn l13_close_unregisters_then_closes() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let closing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.close().await }
    });
    let component: Route = control::decode(&peer.recv().await.payload).unwrap();
    let inbox: Route = control::decode(&peer.recv().await.payload).unwrap();
    assert_eq!(
        (component.add, component.topic.as_str()),
        (0, "test.component")
    );
    assert_eq!((inbox.add, inbox.topic.as_str()), (0, bus.inbox()));
    assert!(
        peer.try_recv().await.is_none(),
        "expected EOF after unregistering"
    );
    drop(peer);
    closing.await.unwrap().unwrap();
    assert_eq!(*bus.state().borrow(), ConnectionState::Closed);
    assert!(matches!(
        bus.get::<u32>("Device.A").await,
        Err(Error::Disconnected)
    ));
    bus.close().await.unwrap();
}

#[tokio::test]
async fn close_gives_up_on_a_broker_that_stays_open() {
    let l = Listener::new();
    let (bus, _peer) = l
        .connect(Timeouts::uniform(Duration::from_millis(50)))
        .await;
    bus.close().await.unwrap();
    assert_eq!(*bus.state().borrow(), ConnectionState::Closed);
}

#[tokio::test]
async fn l13_drop_unregisters_best_effort() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let clone = bus.clone();
    drop(bus);
    drop(clone);
    let component: Route = control::decode(&peer.recv().await.payload).unwrap();
    let inbox: Route = control::decode(&peer.recv().await.payload).unwrap();
    assert_eq!((component.add, inbox.add), (0, 0));
    assert!(peer.try_recv().await.is_none());
}

/// Answers an owner lookup.
async fn owners(peer: &mut Peer, expect: &[&str], owners: &[&str]) {
    let req = peer.recv().await;
    assert_eq!(req.header.topic, control::TOPIC_TRACE_ORIGIN);
    assert_eq!(req.header.flags, Flags::REQUEST);
    let list: NameList = control::decode(&req.payload).unwrap();
    assert_eq!(list.items, expect);
    peer.reply_json(
        &req,
        &DiscoveryReply {
            result: Some(0),
            count: owners.len(),
            items: owners.iter().map(|s| s.to_string()).collect(),
        },
    )
    .await;
}

#[tokio::test]
async fn c2_get_many_batches_per_owner() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let get = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.get_many(["Device.A.1", "Device.B.1", "Device.A.2"])
                .await
        }
    });
    owners(
        &mut peer,
        &["Device.A.1", "Device.B.1", "Device.A.2"],
        &["compA", "compB", "compA"],
    )
    .await;
    // Both requests are sent before either is answered.
    let a = peer.recv().await;
    let b = peer.recv().await;
    assert_eq!(a.header.topic, "Device.A.1");
    assert_eq!(
        request::<GetRequest>(&a, Method::GetParameterValues).names,
        ["Device.A.1", "Device.A.2"]
    );
    assert_eq!(b.header.topic, "Device.B.1");
    peer.reply_rbus(&b, &get_ok(vec![Property::new("Device.B.1", Value::U8(2))]))
        .await;
    peer.reply_rbus(
        &a,
        &get_ok(vec![
            Property::new("Device.A.2", Value::U8(3)),
            Property::new("Device.A.1", Value::U8(1)),
        ]),
    )
    .await;
    let values = get.await.unwrap().unwrap();
    let got: Vec<(&str, &Value)> = values.iter().map(|(k, v)| (k.as_str(), v)).collect();
    assert_eq!(
        got,
        [
            ("Device.A.1", &Value::U8(1)),
            ("Device.B.1", &Value::U8(2)),
            ("Device.A.2", &Value::U8(3))
        ]
    );
    assert!(bus.get_many(Vec::<&str>::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn get_many_names_the_unowned_name() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_many(["Device.A", "Device.Nope"]).await }
    });
    // rtrouted leaves unknown names out, so the client asks one at a time.
    owners(&mut peer, &["Device.A", "Device.Nope"], &["compA"]).await;
    owners(&mut peer, &["Device.A"], &["compA"]).await;
    owners(&mut peer, &["Device.Nope"], &[]).await;
    assert!(matches!(get.await.unwrap(), Err(Error::NotFound(n)) if n == "Device.Nope"));

    // A provider that leaves a value out is a protocol error.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_many(["Device.A"]).await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(&req, &get_ok(vec![])).await;
    assert!(matches!(get.await.unwrap(), Err(Error::Protocol(_))));

    // Owner lookups that come back complete after all.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_many(["Device.A", "Device.B"]).await }
    });
    owners(&mut peer, &["Device.A", "Device.B"], &["compA", ""]).await;
    owners(&mut peer, &["Device.A"], &["compA"]).await;
    owners(&mut peer, &["Device.B"], &["compA"]).await;
    let req = peer.recv().await;
    peer.reply_rbus(
        &req,
        &get_ok(vec![
            Property::new("Device.A", Value::None),
            Property::new("Device.B", Value::None),
        ]),
    )
    .await;
    assert_eq!(get.await.unwrap().unwrap().len(), 2);
    assert!(matches!(
        bus.get_many(["Device.A", "bad name"]).await,
        Err(Error::Name(_))
    ));
}

#[tokio::test]
async fn c6_set_sends_a_committed_single_set() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let set = tokio::spawn({
        let bus = bus.clone();
        async move { bus.set("Device.A", 5u16).await }
    });
    let req = peer.recv().await;
    assert_eq!(req.header.topic, "Device.A");
    let body: SetRequest = request(&req, Method::SetParameterValues);
    assert_eq!(body.component, "test.component");
    assert!(body.commit && !body.rollback);
    assert_eq!(body.properties, [Property::new("Device.A", Value::U16(5))]);
    peer.reply_rbus(
        &req,
        &SetResponse {
            status: 0,
            old_values: vec![],
            failed_element: None,
        },
    )
    .await;
    set.await.unwrap().unwrap();

    // Failures name the parameter the provider reported, or the one sent.
    for (failed, want) in [
        (Some("Device.A"), "Device.A"),
        (None, "Device.A"),
        (Some("not a name"), "Device.A"),
    ] {
        let set = tokio::spawn({
            let bus = bus.clone();
            async move { bus.set("Device.A", true).await }
        });
        let req = peer.recv().await;
        peer.reply_rbus(
            &req,
            &SetResponse {
                status: 27,
                old_values: vec![],
                failed_element: failed.map(String::from),
            },
        )
        .await;
        match set.await.unwrap() {
            Err(Error::SetFailed { name, status }) => {
                assert_eq!((name.as_str(), status), (want, Status::NotWritable))
            }
            other => panic!("{other:?}"),
        }
    }

    // Setting "nothing" is refused locally, as in C.
    assert!(matches!(
        bus.set("Device.A", Value::None).await,
        Err(Error::SetFailed {
            status: Status::InvalidInput,
            ..
        })
    ));
    let set = tokio::spawn({
        let bus = bus.clone();
        async move { bus.set("Device.A", 1i32).await }
    });
    let req = peer.recv().await;
    peer.undeliverable(&req).await;
    assert!(matches!(set.await.unwrap(), Err(Error::NotFound(_))));
}

#[tokio::test]
async fn c7_c9_set_many_is_one_request_per_owner_and_stops_at_a_failure() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let set = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.set_many()
                .set("Device.A.1", 1i32)
                .set("Device.B.1", "x")
                .set("Device.A.2", 2i32)
                .set("Device.C.1", 3i32)
                .await
        }
    });
    owners(
        &mut peer,
        &["Device.A.1", "Device.B.1", "Device.A.2", "Device.C.1"],
        &["compA", "compB", "compA", "compC"],
    )
    .await;
    let a = peer.recv().await;
    let body: SetRequest = request(&a, Method::SetParameterValues);
    assert_eq!(a.header.topic, "Device.A.1");
    assert_eq!(body.properties.len(), 2);
    assert!(body.commit);
    peer.reply_rbus(
        &a,
        &SetResponse {
            status: 0,
            old_values: vec![],
            failed_element: None,
        },
    )
    .await;
    let b = peer.recv().await;
    assert_eq!(b.header.topic, "Device.B.1");
    peer.reply_rbus(
        &b,
        &SetResponse {
            status: 30,
            old_values: vec![],
            failed_element: Some("Device.B.1".into()),
        },
    )
    .await;
    match set.await.unwrap() {
        Err(Error::SetFailed { name, status }) => {
            assert_eq!(
                (name.as_str(), status),
                ("Device.B.1", Status::InvalidParameterValue)
            )
        }
        other => panic!("{other:?}"),
    }

    // Edge cases: empty, single, and an invalid name.
    bus.set_many().await.unwrap();
    let set = tokio::spawn({
        let bus = bus.clone();
        async move { bus.set_many().set("Device.A", 1u8).await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(
        &req,
        &SetResponse {
            status: 0,
            old_values: vec![],
            failed_element: None,
        },
    )
    .await;
    set.await.unwrap().unwrap();
    let r = bus
        .set_many()
        .set("bad name", 1u8)
        .set("Device.A", 1u8)
        .await;
    assert!(matches!(r, Err(Error::Name(_))));
}

#[derive(Serialize)]
struct Args {
    count: u32,
    label: &'static str,
}

#[derive(Deserialize, Debug, PartialEq)]
struct Reply {
    value: String,
}

#[tokio::test]
async fn m1_m3_invoke() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let call = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.invoke::<_, Reply>(
                "Device.M()",
                Args {
                    count: 2,
                    label: "x",
                },
            )
            .await
        }
    });
    let req = peer.recv().await;
    assert_eq!(req.header.topic, "Device.M()");
    let body: RpcRequest = request(&req, Method::Rpc);
    assert_eq!(body.method, "Device.M()");
    let input = body.input.unwrap();
    assert_eq!(input.get("count"), Some(&Value::U32(2)));
    assert_eq!(input.get("label"), Some(&Value::String("x".into())));
    peer.reply_rbus(
        &req,
        &RpcResponse {
            status: 0,
            output: Some(Object::new().with("value", "done")),
        },
    )
    .await;
    assert_eq!(
        call.await.unwrap().unwrap(),
        Reply {
            value: "done".into()
        }
    );

    // No arguments, no output.
    let call = tokio::spawn({
        let bus = bus.clone();
        async move { bus.invoke::<_, ()>("Device.M()", ()).await }
    });
    let req = peer.recv().await;
    assert_eq!(request::<RpcRequest>(&req, Method::Rpc).input, None);
    peer.reply_rbus(
        &req,
        &RpcResponse {
            status: 0,
            output: None,
        },
    )
    .await;
    call.await.unwrap().unwrap();

    // A failure with the C error convention, and one without.
    for (output, message) in [
        (
            Some(
                Object::new()
                    .with("error_code", 56789i32)
                    .with("error_string", "Provider Specific Error Code"),
            ),
            Some("Provider Specific Error Code"),
        ),
        (None, None),
    ] {
        let call = tokio::spawn({
            let bus = bus.clone();
            async move { bus.invoke::<_, Object>("Device.M()", ()).await }
        });
        let req = peer.recv().await;
        peer.reply_rbus(
            &req,
            &RpcResponse {
                status: 22,
                output: output.clone(),
            },
        )
        .await;
        match call.await.unwrap() {
            Err(Error::Method {
                method,
                status,
                message: m,
                detail,
            }) => {
                assert_eq!(method, "Device.M()");
                assert_eq!(status, Status::InvalidMethod);
                assert_eq!(m.as_deref(), message);
                assert_eq!(*detail, output.unwrap_or_default());
            }
            other => panic!("{other:?}"),
        }
    }

    // Undeliverable, and arguments that are not an object.
    let call = tokio::spawn({
        let bus = bus.clone();
        async move { bus.invoke::<_, ()>("Device.M()", ()).await }
    });
    let req = peer.recv().await;
    peer.undeliverable(&req).await;
    assert!(matches!(call.await.unwrap(), Err(Error::NotFound(_))));
    assert!(matches!(
        bus.invoke::<_, ()>("Device.M()", 5u8).await,
        Err(Error::Type(_))
    ));
    assert!(matches!(
        bus.invoke::<_, ()>("Device.M", ()).await,
        Err(Error::Name(_))
    ));
}
