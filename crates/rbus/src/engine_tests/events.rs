//! Consumer engine tests: subscriptions, advisories, resubscription, trees, tables
//! and discovery against the scripted socket.

use std::time::Duration;

use futures_util::StreamExt;
use rbus_wire::body::{
    AddRowRequest, AddRowResponse, DeleteRowRequest, InitialValue, NamesMode, NamesRequest,
    RowName, RowNamesResponse, StatusResponse, SubscribeRequest, SubscribeResponse,
};
use rbus_wire::control::{self, DiscoveryReply, Expression, Route};
use rbus_wire::event::{Event, EventKind};
use rbus_wire::frame::{Flags, Frame, Header};
use rbus_wire::message::{EventMeta, Method, MethodMeta, TraceContext};
use rbus_wire::msgpack::Writer;
use rbus_wire::value::{Object, Property, Value};

use super::{Listener, Peer, get_ok, request, short};
use crate::{
    Advisory, ConnectionState, Error, EventError, Overflow, Rbus, Retry, RowEvent, RowId, Sample,
    SampleKind, Status, Timeouts,
};

/// The advisory route is registered once, before the first subscription.
pub(super) async fn expect_advisory_route(peer: &mut Peer) {
    let f = peer.recv().await;
    assert_eq!(f.header.topic, control::TOPIC_SUBSCRIBE);
    let route: Route = control::decode(&f.payload).unwrap();
    assert_eq!(
        (route.add, route.topic.as_str(), route.route_id),
        (1, control::TOPIC_ADVISORY, 2)
    );
}

pub(super) async fn reply_subscribe(peer: &mut Peer, req: &Frame, resp: SubscribeResponse) {
    let mut w = Writer::new();
    resp.encode(&mut w);
    let payload = w.finish_with_meta(|m| MethodMeta::response().encode(m));
    peer.reply(req, Flags::RAW_BINARY, payload).await;
}

pub(super) fn subscribed(id: i32) -> SubscribeResponse {
    SubscribeResponse {
        status: 0,
        initial: InitialValue::NotRequested,
        subscription_id: Some(id),
    }
}

pub(super) fn event_frame(to: &str, routing: &str, ev: Event) -> Frame {
    let meta = EventMeta {
        event_name: routing.into(),
        object_name: "Prov".into(),
        rbus2: true,
        trace: TraceContext::default(),
    };
    let mut w = Writer::new();
    ev.encode(&mut w);
    let payload = w.finish_with_meta(|m| meta.encode(m));
    Frame::new(Header::new(1, Flags::RAW_BINARY, to, "Prov"), payload)
}

pub(super) fn event(name: &str, kind: EventKind, data: Object, component_id: i32) -> Event {
    Event {
        name: name.into(),
        kind,
        data: Some(data),
        filter: None,
        interval: 0,
        duration: 0,
        component_id,
    }
}

fn advisory_frame(event: i32, inbox: &str) -> Frame {
    let payload = control::encode(&control::Advisory {
        event,
        inbox: inbox.into(),
    })
    .unwrap();
    Frame::new(
        Header::new(1, Flags::NONE, control::TOPIC_ADVISORY, ""),
        payload,
    )
}

#[tokio::test]
async fn e1_e3_e9_value_subscription() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe_value::<i32>("Device.A").initial_value().await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    assert_eq!(req.header.topic, "Device.A");
    let body: SubscribeRequest = request(&req, Method::Subscribe);
    assert_eq!(body.event_name, "Device.A");
    assert_eq!(body.subscriber, bus.inbox());
    assert!(body.publish_on_subscribe && !body.raw_data);
    let id = body.options.unwrap().component_id;
    // An event that races the subscribe reply still comes after the initial value.
    peer.send(event_frame(
        bus.inbox(),
        "Device.A",
        event(
            "Device.A",
            EventKind::ValueChanged,
            Object::new()
                .with("value", 6i32)
                .with("oldValue", 5i32)
                .with("by", "x"),
            id,
        ),
    ))
    .await;
    reply_subscribe(
        &mut peer,
        &req,
        SubscribeResponse {
            status: 0,
            initial: InitialValue::Value(Box::new(event(
                "Device.A",
                EventKind::InitialValue,
                Object::new().with("initialValue", 5i32),
                id,
            ))),
            subscription_id: Some(3),
        },
    )
    .await;
    let mut sub = sub.await.unwrap().unwrap();
    assert_eq!(sub.name(), "Device.A");
    assert!(format!("{sub:?}").contains("Device.A"));
    let first = sub.next().await.unwrap().unwrap();
    assert_eq!((first.new, first.old), (5, None));
    let second = StreamExt::next(&mut sub).await.unwrap().unwrap();
    assert_eq!(
        (second.new, second.old, second.by.as_deref()),
        (6, Some(5), Some("x"))
    );
    // A value of the wrong type is reported, not dropped.
    peer.send(event_frame(
        bus.inbox(),
        "Device.A",
        event(
            "Device.A",
            EventKind::ValueChanged,
            Object::new().with("value", "s"),
            id,
        ),
    ))
    .await;
    assert!(matches!(
        sub.next().await,
        Some(Err(EventError::Decode { .. }))
    ));
    // Events for unknown subscriptions and undecodable events are dropped.
    peer.send(event_frame(
        bus.inbox(),
        "Device.Other",
        event("Device.Other", EventKind::General, Object::new(), 999),
    ))
    .await;
    peer.send(Frame::new(
        Header::new(1, Flags::RAW_BINARY, bus.inbox(), "Prov"),
        b"junk".to_vec(),
    ))
    .await;

    // Explicit unsubscribe waits for the provider.
    let unsub = tokio::spawn(sub.unsubscribe());
    let req = peer.recv().await;
    let body: SubscribeRequest = request(&req, Method::Unsubscribe);
    assert_eq!(body.options.unwrap().component_id, id);
    peer.reply_rbus(&req, &StatusResponse { status: 0 }).await;
    unsub.await.unwrap().unwrap();
    assert_eq!(bus.inner.conn.sub_count(), 0);
}

#[tokio::test]
async fn e2_dropping_a_stream_unsubscribes() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    let id = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let sub = sub.await.unwrap().unwrap();
    assert_eq!(bus.inner.conn.sub_count(), 1);
    drop(sub);
    assert_eq!(bus.inner.conn.sub_count(), 0);
    let req = peer.recv().await;
    let body: SubscribeRequest = request(&req, Method::Unsubscribe);
    assert_eq!(body.options.unwrap().component_id, id);

    // Unsubscribing from a provider that is gone succeeds, as in C; a refusal
    // is reported.
    for (reply, ok) in [(None, true), (Some(18), false)] {
        let sub = tokio::spawn({
            let bus = bus.clone();
            async move { bus.subscribe::<Object>("Device.E!").await }
        });
        let req = peer.recv().await;
        reply_subscribe(&mut peer, &req, subscribed(1)).await;
        let unsub = tokio::spawn(sub.await.unwrap().unwrap().unsubscribe());
        let req = peer.recv().await;
        match reply {
            None => peer.undeliverable(&req).await,
            Some(status) => peer.reply_rbus(&req, &StatusResponse { status }).await,
        }
        assert_eq!(unsub.await.unwrap().is_ok(), ok);
    }
}

fn interval_of(req: &Frame, method: Method) -> u32 {
    request::<SubscribeRequest>(req, method)
        .options
        .unwrap()
        .interval
}

/// Providers key subscriptions on the interval, so every request about a
/// subscription (subscribe, resubscribe, unsubscribe) must carry it.
#[tokio::test]
async fn e7_the_interval_goes_with_every_request() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    // A bad interval fails before anything is sent.
    let bad = bus
        .subscribe_value::<i32>("Device.A")
        .interval(Duration::from_millis(100))
        .await;
    assert!(matches!(bad, Err(Error::InvalidArgument(_))), "{bad:?}");

    let sub = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.subscribe_value::<i32>("Device.A")
                .interval(Duration::from_secs(2))
                .initial_value()
                .await
        }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    let body: SubscribeRequest = request(&req, Method::Subscribe);
    assert!(body.publish_on_subscribe);
    let opts = body.options.unwrap();
    assert_eq!(opts.interval, 2);
    let id = opts.component_id;
    reply_subscribe(
        &mut peer,
        &req,
        SubscribeResponse {
            status: 0,
            initial: InitialValue::Value(Box::new(event(
                "Device.A",
                EventKind::InitialValue,
                Object::new().with("initialValue", 4i32),
                id,
            ))),
            subscription_id: Some(3),
        },
    )
    .await;
    let mut sub = sub.await.unwrap().unwrap();
    assert!(format!("{sub:?}").contains("interval: 2"));
    let mut tick = event(
        "Device.A",
        EventKind::Interval,
        Object::new().with("Device.A", 5i32),
        id,
    );
    tick.interval = 2;
    peer.send(event_frame(bus.inbox(), "Device.A", tick)).await;
    let items: Vec<Sample<i32>> = [sub.next().await, sub.next().await]
        .map(|i| i.unwrap().unwrap())
        .into();
    assert_eq!(
        items
            .iter()
            .map(|s| (s.name.as_str(), s.value, s.kind))
            .collect::<Vec<_>>(),
        [
            ("Device.A", 4, SampleKind::Initial),
            ("Device.A", 5, SampleKind::Interval)
        ]
    );

    // A restarted provider gets the same interval again.
    peer.send(advisory_frame(0, "rbus.provider.INBOX.2")).await;
    let req = peer.recv().await;
    assert_eq!(interval_of(&req, Method::Subscribe), 2);
    reply_subscribe(&mut peer, &req, subscribed(4)).await;

    // Unsubscribing, explicitly or by dropping, sends it too.
    let unsub = tokio::spawn(sub.unsubscribe());
    let req = peer.recv().await;
    assert_eq!(interval_of(&req, Method::Unsubscribe), 2);
    peer.reply_rbus(&req, &StatusResponse { status: 0 }).await;
    unsub.await.unwrap().unwrap();

    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").interval_raw(100).await }
    });
    let req = peer.recv().await;
    assert_eq!(interval_of(&req, Method::Subscribe), 100);
    reply_subscribe(&mut peer, &req, subscribed(5)).await;
    drop(sub.await.unwrap().unwrap());
    assert_eq!(interval_of(&peer.recv().await, Method::Unsubscribe), 100);

    // So does giving up on a subscription half set up.
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.subscribe_value::<i32>("Device.A")
                .interval_raw(250)
                .interval(Duration::from_secs(7))
                .await
        }
    });
    let req = peer.recv().await;
    assert_eq!(interval_of(&req, Method::Subscribe), 7);
    sub.abort();
    assert!(sub.await.unwrap_err().is_cancelled());
    assert_eq!(interval_of(&peer.recv().await, Method::Unsubscribe), 7);

    // And closing the bus.
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.subscribe::<Object>("Device.E!")
                .interval(Duration::from_secs(1))
                .interval_raw(30_000)
                .await
        }
    });
    let req = peer.recv().await;
    reply_subscribe(&mut peer, &req, subscribed(6)).await;
    let _sub = sub.await.unwrap().unwrap();
    let closing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.close().await }
    });
    assert_eq!(interval_of(&peer.recv().await, Method::Unsubscribe), 30_000);
    drop(peer);
    closing.await.unwrap().unwrap();
}

/// An event for an unknown id falls back to a subscription with the same
/// name and interval, as in C: a sample still in flight from a dropped
/// interval subscription must not reach a plain one.
#[tokio::test]
async fn e7_late_samples_do_not_reach_a_plain_subscription() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let plain = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe_value::<i32>("Device.A").await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let mut plain = plain.await.unwrap().unwrap();
    let samples = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.subscribe_value::<i32>("Device.A")
                .interval(Duration::from_secs(1))
                .await
        }
    });
    let req = peer.recv().await;
    let dropped = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    reply_subscribe(&mut peer, &req, subscribed(2)).await;
    drop(samples.await.unwrap().unwrap());
    request::<SubscribeRequest>(&peer.recv().await, Method::Unsubscribe);

    let mut late = event(
        "Device.A",
        EventKind::Interval,
        Object::new().with("Device.A", 1i32),
        dropped,
    );
    late.interval = 1;
    peer.send(event_frame(bus.inbox(), "Device.A", late)).await;
    // An unknown id with no interval still finds the plain subscription.
    peer.send(event_frame(
        bus.inbox(),
        "Device.A",
        event(
            "Device.A",
            EventKind::ValueChanged,
            Object::new().with("value", 7i32).with("oldValue", 6i32),
            999,
        ),
    ))
    .await;
    let change = plain.next().await.unwrap().unwrap();
    assert_eq!((change.old, change.new), (Some(6), 7));
}

#[tokio::test]
async fn e4_e5_table_and_general_events() {
    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct Alarm {
        level: u8,
    }
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let table = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe_table("Device.T.").await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    let tid = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let mut table = table.await.unwrap().unwrap();

    let alarms = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Alarm>("Device.Alarm!").await }
    });
    let req = peer.recv().await;
    let aid = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    assert_ne!(aid, tid, "each subscription has its own id");
    reply_subscribe(&mut peer, &req, subscribed(2)).await;
    let mut alarms = alarms.await.unwrap().unwrap();

    peer.send(event_frame(
        bus.inbox(),
        "Device.T.",
        event(
            "Device.T.",
            EventKind::ObjectCreated,
            Object::new()
                .with("rowName", "Device.T.2")
                .with("instNum", 2u32)
                .with("alias", "two"),
            tid,
        ),
    ))
    .await;
    peer.send(event_frame(
        bus.inbox(),
        "Device.Alarm!",
        event(
            "Device.Alarm!",
            EventKind::General,
            Object::new().with("level", 4u8),
            aid,
        ),
    ))
    .await;
    assert_eq!(
        table.next().await.unwrap().unwrap(),
        RowEvent::Added {
            table: "Device.T.".into(),
            row: RowId {
                inst: 2,
                alias: Some("two".into())
            }
        }
    );
    assert_eq!(alarms.next().await.unwrap().unwrap(), Alarm { level: 4 });

    // The streams end when the connection does.
    drop(peer);
    assert!(table.next().await.is_none());
    assert!(alarms.next().await.is_none());
}

#[tokio::test]
async fn d12_a_slow_consumer_sees_lagged() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").buffer(2).await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    let id = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let mut sub = sub.await.unwrap().unwrap();
    for i in 0..5u8 {
        peer.send(event_frame(
            bus.inbox(),
            "Device.E!",
            event(
                "Device.E!",
                EventKind::General,
                Object::new().with("i", i),
                id,
            ),
        ))
        .await;
    }
    // A round trip proves the reader has queued all five.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u8>("Device.X").await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(&req, &get_ok(vec![Property::new("Device.X", Value::U8(1))]))
        .await;
    get.await.unwrap().unwrap();
    assert_eq!(sub.next().await.unwrap(), Err(EventError::Lagged(3)));
    let next = sub.next().await.unwrap().unwrap();
    assert_eq!(next.get("i"), Some(&Value::U8(3)));
}

#[tokio::test]
async fn d12_dropping_a_full_block_subscription_frees_the_connection() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.subscribe::<Object>("Device.E!")
                .buffer(1)
                .overflow(Overflow::Block)
                .await
        }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    let id = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let sub = sub.await.unwrap().unwrap();
    // The second event fills the queue; the reader waits on the third.
    for i in 0..3u8 {
        peer.send(event_frame(
            bus.inbox(),
            "Device.E!",
            event(
                "Device.E!",
                EventKind::General,
                Object::new().with("i", i),
                id,
            ),
        ))
        .await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Dropping the subscription must release the reader, or no reply would
    // ever be read again.
    drop(sub);
    let unsub = peer.recv().await;
    let _: SubscribeRequest = request(&unsub, Method::Unsubscribe);
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.A").await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(
        &req,
        &get_ok(vec![Property::new("Device.A", Value::U32(1))]),
    )
    .await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), get)
            .await
            .expect("the reader is stuck")
            .unwrap()
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn d12_block_overflow_holds_the_connection() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.subscribe::<Object>("Device.E!")
                .buffer(1)
                .overflow(Overflow::Block)
                .await
        }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    let id = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let mut sub = sub.await.unwrap().unwrap();
    for i in 0..3u8 {
        peer.send(event_frame(
            bus.inbox(),
            "Device.E!",
            event(
                "Device.E!",
                EventKind::General,
                Object::new().with("i", i),
                id,
            ),
        ))
        .await;
    }
    // Nothing is lost: the reader waits for room instead.
    for i in 0..3u8 {
        let ev = sub.next().await.unwrap().unwrap();
        assert_eq!(ev.get("i"), Some(&Value::U8(i)));
    }
}

#[tokio::test]
async fn subscribe_failures_and_retry() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;

    // An error status.
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    reply_subscribe(
        &mut peer,
        &req,
        SubscribeResponse {
            status: 18,
            initial: InitialValue::NotRequested,
            subscription_id: None,
        },
    )
    .await;
    match sub.await.unwrap() {
        Err(Error::Status { name, status }) => {
            assert_eq!(
                (name.as_str(), status),
                ("Device.E!", Status::AccessNotAllowed)
            )
        }
        other => panic!("{other:?}"),
    }
    // The failed setup unsubscribes, in case the provider kept it.
    let req = peer.recv().await;
    request::<SubscribeRequest>(&req, Method::Unsubscribe);
    assert_eq!(bus.inner.conn.sub_count(), 0);

    // No provider and no retry.
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").await }
    });
    let req = peer.recv().await;
    peer.undeliverable(&req).await;
    assert!(matches!(sub.await.unwrap(), Err(Error::NotFound(_))));

    // With retry: the provider appears on the third attempt.
    let retry = Retry::exponential()
        .initial(Duration::from_millis(5))
        .max_delay(Duration::from_millis(10))
        .max_elapsed(Duration::from_secs(5));
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").retry(retry).await }
    });
    for _ in 0..2 {
        let req = peer.recv().await;
        peer.undeliverable(&req).await;
    }
    let req = peer.recv().await;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let sub = sub.await.unwrap().unwrap();
    drop(sub);
    peer.recv().await;

    // Retry gives up after max_elapsed.
    let retry = Retry::exponential()
        .initial(Duration::from_millis(20))
        .max_elapsed(Duration::from_millis(30));
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").retry(retry).await }
    });
    for _ in 0..2 {
        let req = peer.recv().await;
        peer.undeliverable(&req).await;
    }
    assert!(matches!(sub.await.unwrap(), Err(Error::NotFound(_))));

    // A cancelled subscribe cleans up after itself.
    let bus2 = bus.clone();
    let attempt = async move { bus2.subscribe::<Object>("Device.E!").await };
    assert!(
        tokio::time::timeout(Duration::from_millis(20), attempt)
            .await
            .is_err()
    );
    peer.recv().await;
    let req = peer.recv().await;
    request::<SubscribeRequest>(&req, Method::Unsubscribe);
    assert_eq!(bus.inner.conn.sub_count(), 0);

    // Invalid names never reach the wire.
    assert!(matches!(
        bus.subscribe::<Object>("Device.E").await,
        Err(Error::Name(_))
    ));
}

#[tokio::test]
async fn d5_advisories_and_resubscription() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let mut adv = bus.discover().advisories();
    assert!(format!("{adv:?}").contains("Advisories"));
    expect_advisory_route(&mut peer).await;
    let mut adv2 = bus.discover().advisories();
    peer.send(advisory_frame(0, "rbus.other.INBOX.1")).await;
    peer.send(advisory_frame(1, "rbus.other.INBOX.1")).await;
    assert_eq!(
        adv.next().await.unwrap().unwrap(),
        Advisory::Connected {
            inbox: "rbus.other.INBOX.1".into()
        }
    );
    assert_eq!(
        StreamExt::next(&mut adv).await.unwrap().unwrap(),
        Advisory::Disconnected {
            inbox: "rbus.other.INBOX.1".into()
        }
    );
    assert!(adv2.next().await.unwrap().is_ok());
    drop(adv2);
    // Bad advisories are ignored.
    peer.send(Frame::new(
        Header::new(1, Flags::NONE, control::TOPIC_ADVISORY, ""),
        b"not json".to_vec(),
    ))
    .await;

    // A subscription, then a provider restarts: its connect advisory makes us
    // send the subscription again, without asking for an initial value.
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe_value::<i32>("Device.A").initial_value().await }
    });
    let req = peer.recv().await;
    let id = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    reply_subscribe(
        &mut peer,
        &req,
        SubscribeResponse {
            status: 0,
            initial: InitialValue::Unavailable,
            subscription_id: Some(1),
        },
    )
    .await;
    let _sub = sub.await.unwrap().unwrap();
    // Our own connect advisory changes nothing; a burst of others is debounced.
    peer.send(advisory_frame(0, bus.inbox())).await;
    for _ in 0..3 {
        peer.send(advisory_frame(0, "rbus.provider.INBOX.2")).await;
    }
    // The provider is still registering its elements at first.
    let req = peer.recv().await;
    let body: SubscribeRequest = request(&req, Method::Subscribe);
    assert_eq!(body.options.unwrap().component_id, id);
    assert!(!body.publish_on_subscribe);
    peer.undeliverable(&req).await;
    let req = peer.recv().await;
    reply_subscribe(&mut peer, &req, subscribed(5)).await;
    // A provider that still has it answers "already exists"; that's fine.
    peer.send(advisory_frame(0, "rbus.provider.INBOX.3")).await;
    let req = peer.recv().await;
    reply_subscribe(
        &mut peer,
        &req,
        SubscribeResponse {
            status: 24,
            initial: InitialValue::NotRequested,
            subscription_id: None,
        },
    )
    .await;
    // Other failures are logged and the stream stays.
    peer.send(advisory_frame(0, "rbus.provider.INBOX.4")).await;
    let req = peer.recv().await;
    reply_subscribe(
        &mut peer,
        &req,
        SubscribeResponse {
            status: 18,
            initial: InitialValue::NotRequested,
            subscription_id: None,
        },
    )
    .await;
    assert_eq!(bus.inner.conn.sub_count(), 1);
    drop(peer);
    // Everything queued is still delivered, then the stream ends.
    let mut seen = 0;
    while let Some(a) = adv.next().await {
        assert!(matches!(a.unwrap(), Advisory::Connected { .. }));
        seen += 1;
    }
    assert_eq!(seen, 6);
}

#[tokio::test]
async fn c3_c5_get_tree_fans_out_and_reports_failures() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_tree("Device.W.").await }
    });
    let q = peer.recv().await;
    assert_eq!(q.header.topic, control::TOPIC_QUERY);
    let expr: Expression = control::decode(&q.payload).unwrap();
    assert_eq!(expr.expression, "Device.W.");
    peer.reply_json(
        &q,
        &DiscoveryReply {
            result: Some(0),
            count: 3,
            items: vec!["compA".into(), "compB".into(), "compA".into()],
        },
    )
    .await;
    let a = peer.recv().await;
    let b = peer.recv().await;
    assert_eq!(
        (a.header.topic.as_str(), b.header.topic.as_str()),
        ("compA", "compB")
    );
    let body: rbus_wire::body::GetRequest = request(&a, Method::GetParameterValues);
    assert_eq!(body.names, ["Device.W."]);
    peer.reply_rbus(
        &a,
        &get_ok(vec![Property::new("Device.W.X.Y", Value::I32(1))]),
    )
    .await;
    peer.reply_rbus(
        &b,
        &rbus_wire::body::GetResponse {
            status: 1,
            properties: vec![],
        },
    )
    .await;
    let tree = get.await.unwrap().unwrap();
    assert_eq!(tree.get("Device.W.X.Y"), Some(&Value::I32(1)));
    assert_eq!(tree.failures().len(), 1);
    assert_eq!(tree.failures()[0].component, "compB");
    assert_eq!(tree.failures()[0].error.status(), Some(Status::BusError));

    // An empty answer from one component still counts: the call succeeds
    // with an empty tree and reports the other's failure.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_tree("Device.W.").await }
    });
    let q = peer.recv().await;
    peer.reply_json(
        &q,
        &DiscoveryReply {
            result: Some(0),
            count: 2,
            items: vec!["compA".into(), "compB".into()],
        },
    )
    .await;
    let a = peer.recv().await;
    let b = peer.recv().await;
    peer.reply_rbus(&a, &get_ok(vec![])).await;
    peer.undeliverable(&b).await;
    let tree = get.await.unwrap().unwrap();
    assert_eq!((tree.iter().count(), tree.failures().len()), (0, 1));

    // Every component failing fails the call.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_tree("Device.W.").await }
    });
    let q = peer.recv().await;
    peer.reply_json(
        &q,
        &DiscoveryReply {
            result: Some(0),
            count: 1,
            items: vec!["compA".into()],
        },
    )
    .await;
    let a = peer.recv().await;
    peer.undeliverable(&a).await;
    assert!(matches!(get.await.unwrap(), Err(Error::NotFound(_))));

    // A wildcard QUERY can't resolve falls back to _trace_origin_object, as
    // in C; when that finds nobody either, the name is not found.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_tree("Device.*.X").await }
    });
    let q = peer.recv().await;
    assert_eq!(q.header.topic, control::TOPIC_QUERY);
    peer.reply_json(
        &q,
        &DiscoveryReply {
            result: Some(0),
            count: 0,
            items: vec![],
        },
    )
    .await;
    let q = peer.recv().await;
    assert_eq!(q.header.topic, control::TOPIC_TRACE_ORIGIN);
    peer.reply_json(
        &q,
        &DiscoveryReply {
            result: Some(0),
            count: 0,
            items: vec![],
        },
    )
    .await;
    assert!(matches!(get.await.unwrap(), Err(Error::NotFound(n)) if n == "Device.*.X"));

    // When the fallback finds the owner, the get goes to the wildcard name.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_tree("Device.T.*.X").await }
    });
    let q = peer.recv().await;
    peer.reply_json(
        &q,
        &DiscoveryReply {
            result: Some(0),
            count: 0,
            items: vec![],
        },
    )
    .await;
    let q = peer.recv().await;
    peer.reply_json(
        &q,
        &DiscoveryReply {
            result: Some(0),
            count: 1,
            items: vec!["compA".into()],
        },
    )
    .await;
    let a = peer.recv().await;
    assert_eq!(a.header.topic, "Device.T.*.X");
    peer.reply_rbus(
        &a,
        &get_ok(vec![Property::new("Device.T.1.X", Value::I32(3))]),
    )
    .await;
    assert_eq!(get.await.unwrap().unwrap().len(), 1);

    // An exact name is looked up with _trace_origin_object and sent to itself.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get_tree("Device.W.X.Y").await }
    });
    let q = peer.recv().await;
    assert_eq!(q.header.topic, control::TOPIC_TRACE_ORIGIN);
    peer.reply_json(
        &q,
        &DiscoveryReply {
            result: Some(0),
            count: 1,
            items: vec!["compA".into()],
        },
    )
    .await;
    let a = peer.recv().await;
    assert_eq!(a.header.topic, "Device.W.X.Y");
    peer.reply_rbus(
        &a,
        &get_ok(vec![Property::new("Device.W.X.Y", Value::I32(2))]),
    )
    .await;
    assert_eq!(get.await.unwrap().unwrap().len(), 1);
}

#[tokio::test]
async fn t1_t3_tables() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let t = bus.table("Device.T.");

    let add = tokio::spawn({
        let t = t.clone();
        async move { t.add_row().alias("guest").await }
    });
    let req = peer.recv().await;
    assert_eq!(req.header.topic, "Device.T.");
    let body: AddRowRequest = request(&req, Method::AddTableRow);
    assert_eq!(
        (body.table.as_str(), body.alias.as_deref()),
        ("Device.T.", Some("guest"))
    );
    peer.reply_rbus(
        &req,
        &AddRowResponse {
            status: 0,
            instance: Some(7),
        },
    )
    .await;
    let row = add.await.unwrap().unwrap();
    assert_eq!(
        row,
        RowId {
            inst: 7,
            alias: Some("guest".into())
        }
    );

    let rows = tokio::spawn({
        let t = t.clone();
        async move { t.rows().await }
    });
    let req = peer.recv().await;
    let body: NamesRequest = request(&req, Method::GetParameterNames);
    assert_eq!((body.depth, body.mode), (-1, NamesMode::Rows));
    peer.reply_rbus(
        &req,
        &RowNamesResponse {
            status: 0,
            rows: vec![RowName {
                instance: 7,
                alias: Some("guest".into()),
            }],
        },
    )
    .await;
    assert_eq!(rows.await.unwrap().unwrap(), std::slice::from_ref(&row));

    for (r, name) in [
        (crate::RowRef::from(&row), "Device.T.7."),
        (crate::RowRef::Alias("guest".into()), "Device.T.[guest]."),
        (crate::RowRef::from(3u32), "Device.T.3."),
    ] {
        let rm = tokio::spawn({
            let t = t.clone();
            async move { t.remove_row(r).await }
        });
        let req = peer.recv().await;
        assert_eq!(req.header.topic, name);
        let body: DeleteRowRequest = request(&req, Method::DeleteTableRow);
        assert_eq!(body.row, name);
        peer.reply_rbus(&req, &StatusResponse { status: 0 }).await;
        rm.await.unwrap().unwrap();
    }

    // Failures.
    let add = tokio::spawn({
        let t = t.clone();
        async move { t.add_row().await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(
        &req,
        &AddRowResponse {
            status: 9,
            instance: None,
        },
    )
    .await;
    assert!(matches!(
        add.await.unwrap(),
        Err(Error::Status {
            status: Status::InvalidOperation,
            ..
        })
    ));
    let add = tokio::spawn({
        let t = t.clone();
        async move { t.add_row().await }
    });
    let req = peer.recv().await;
    let mut w = Writer::new();
    w.i32(0);
    peer.reply(
        &req,
        Flags::RAW_BINARY,
        w.finish_with_meta(|m| MethodMeta::response().encode(m)),
    )
    .await;
    assert!(matches!(add.await.unwrap(), Err(Error::Protocol(_))));
    let rows = tokio::spawn({
        let t = t.clone();
        async move { t.rows().await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(
        &req,
        &RowNamesResponse {
            status: 17,
            rows: vec![],
        },
    )
    .await;
    assert!(rows.await.unwrap().is_err());
    let rm = tokio::spawn({
        let t = t.clone();
        async move { t.remove_row(1u32).await }
    });
    let req = peer.recv().await;
    peer.reply_rbus(&req, &StatusResponse { status: 9 }).await;
    assert!(rm.await.unwrap().is_err());

    let bad = bus.table("Device.T");
    assert!(matches!(bad.rows().await, Err(Error::Name(_))));
    assert!(matches!(bad.add_row().await, Err(Error::Name(_))));
    assert!(matches!(bad.remove_row(1u32).await, Err(Error::Name(_))));
    assert!(format!("{t:?}").contains("Device.T."));
}

#[tokio::test]
async fn d1_d2_d4_discovery() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let reply = |items: &[&str]| DiscoveryReply {
        result: Some(0),
        count: items.len(),
        items: items.iter().map(|s| s.to_string()).collect(),
    };

    let owner = tokio::spawn({
        let bus = bus.clone();
        async move { bus.discover().owner("Device.A").await }
    });
    let q = peer.recv().await;
    assert_eq!(q.header.topic, control::TOPIC_TRACE_ORIGIN);
    peer.reply_json(&q, &reply(&["compA"])).await;
    assert_eq!(owner.await.unwrap().unwrap(), "compA");

    let owner = tokio::spawn({
        let bus = bus.clone();
        async move { bus.discover().owner("Device.Nope").await }
    });
    let q = peer.recv().await;
    peer.reply_json(&q, &reply(&[])).await;
    assert!(matches!(owner.await.unwrap(), Err(Error::NotFound(_))));

    let owners = tokio::spawn({
        let bus = bus.clone();
        async move { bus.discover().owners("Device.").await }
    });
    let q = peer.recv().await;
    assert_eq!(q.header.topic, control::TOPIC_QUERY);
    peer.reply_json(&q, &reply(&["a", "b", "a", ""])).await;
    assert_eq!(owners.await.unwrap().unwrap(), ["a", "b"]);

    let elements = tokio::spawn({
        let bus = bus.clone();
        async move { bus.discover().elements("compA").await }
    });
    let q = peer.recv().await;
    assert_eq!(q.header.topic, control::TOPIC_ENUMERATE_ELEMENTS);
    peer.reply_json(&q, &reply(&["compA", "Device.A", "Device.M()"]))
        .await;
    let names = elements.await.unwrap().unwrap();
    assert_eq!(names, ["Device.A", "Device.M()"]);

    let elements = tokio::spawn({
        let bus = bus.clone();
        async move { bus.discover().elements("compA").await }
    });
    let q = peer.recv().await;
    peer.reply_json(&q, &reply(&["bad name"])).await;
    assert!(matches!(elements.await.unwrap(), Err(Error::Protocol(_))));

    let comps = tokio::spawn({
        let bus = bus.clone();
        async move { bus.discover().components().await }
    });
    let q = peer.recv().await;
    assert_eq!(q.header.topic, control::TOPIC_REGISTERED_COMPONENTS);
    assert_eq!(&q.payload[..], b"{\"dummy\":0}");
    peer.reply_json(
        &q,
        &reply(&["rbus.x.INBOX.1", "compA", "_RTROUTED.ADVISORY", "compB"]),
    )
    .await;
    assert_eq!(comps.await.unwrap().unwrap(), ["compA", "compB"]);
    assert!(matches!(
        bus.discover().owner("bad name").await,
        Err(Error::Name(_))
    ));
}

#[tokio::test]
async fn advisories_on_a_closed_connection_end_at_once() {
    let l = Listener::new();
    let (bus, peer) = l
        .connect(Timeouts::uniform(Duration::from_millis(50)))
        .await;
    drop(peer);
    bus.state()
        .wait_for(|s| *s == ConnectionState::Closed)
        .await
        .unwrap();
    let mut adv = bus.discover().advisories();
    // Registered after close: the listener is never fed.
    let r = tokio::time::timeout(Duration::from_millis(50), adv.next()).await;
    assert!(r.is_err() || r.unwrap().is_none());
    let _ = Rbus::clone(&bus);
}

#[tokio::test]
async fn d11_no_resubscribe_while_a_subscription_is_being_set_up() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    // The provider connects while our subscribe is still unanswered.
    peer.send(advisory_frame(0, "rbus.provider.INBOX.9")).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let _sub = sub.await.unwrap().unwrap();
    // The next frame is ours to provoke: a get, not a stray resubscribe.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u8>("Device.X").await }
    });
    let next = peer.recv().await;
    assert_eq!(next.header.topic, "Device.X");
    peer.reply_rbus(
        &next,
        &get_ok(vec![Property::new("Device.X", Value::U8(1))]),
    )
    .await;
    get.await.unwrap().unwrap();
}

#[tokio::test]
async fn l13_close_unsubscribes_first() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect(short()).await;
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe::<Object>("Device.E!").await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let mut sub = sub.await.unwrap().unwrap();
    let closing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.close().await }
    });
    let unsub = peer.recv().await;
    request::<SubscribeRequest>(&unsub, Method::Unsubscribe);
    let component: Route = control::decode(&peer.recv().await.payload).unwrap();
    assert_eq!(component.add, 0);
    peer.recv().await; // the inbox
    assert!(peer.try_recv().await.is_none());
    drop(peer);
    closing.await.unwrap().unwrap();
    assert!(sub.next().await.is_none(), "the stream ends");
    drop(sub); // nothing left to unsubscribe
}
