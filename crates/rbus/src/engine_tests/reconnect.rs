//! Reconnect engine tests: losing the broker and connecting again.

use std::collections::BTreeSet;
use std::time::Duration;

use rbus_wire::body::{GetRequest, SubscribeRequest};
use rbus_wire::control::{self, ResultReply, Route};
use rbus_wire::event::EventKind;
use rbus_wire::frame::{Flags, Frame, Header};
use rbus_wire::message::Method;
use rbus_wire::value::{Object, Property, Value};

use super::events::{event, event_frame, expect_advisory_route, reply_subscribe, subscribed};
use super::provider::{Model, NAMES, expect_alias};
use super::{Listener, Peer, get_ok, request, short};
use crate::{ConnectionState, Error, Rbus, Retry};

fn quick() -> Retry {
    Retry::exponential()
        .initial(Duration::from_millis(5))
        .max_delay(Duration::from_millis(20))
}

fn builder(retry: Retry) -> crate::RbusBuilder {
    Rbus::builder("test.component")
        .timeouts(short())
        .reconnect(retry)
}

async fn wait_for(bus: &Rbus, state: ConnectionState) {
    tokio::time::timeout(
        Duration::from_secs(5),
        bus.state().wait_for(|s| *s == state),
    )
    .await
    .expect("state change")
    .unwrap();
}

/// Reads a route registration (no reply expected).
async fn expect_route(peer: &mut Peer, topic: &str, route_id: u32) {
    let f = peer.recv().await;
    assert_eq!(
        (f.header.topic.as_str(), f.header.flags),
        (control::TOPIC_SUBSCRIBE, Flags::NONE)
    );
    let r: Route = control::decode(&f.payload).unwrap();
    assert_eq!((r.add, r.topic.as_str(), r.route_id), (1, topic, route_id));
}

#[tokio::test]
async fn l5_reconnect_registers_everything_again() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect_with(builder(quick())).await;
    let route = bus.inner.conn.component_route;

    // A provider and a subscription, set up on the first socket.
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
    let sub = tokio::spawn({
        let bus = bus.clone();
        async move { bus.subscribe_value::<i32>("Device.A").await }
    });
    expect_advisory_route(&mut peer).await;
    let req = peer.recv().await;
    let id = request::<SubscribeRequest>(&req, Method::Subscribe)
        .options
        .unwrap()
        .component_id;
    reply_subscribe(&mut peer, &req, subscribed(1)).await;
    let mut sub = sub.await.unwrap().unwrap();

    // The broker goes away with a get in flight.
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.X").await }
    });
    peer.recv().await;
    drop(peer);
    assert!(matches!(get.await.unwrap(), Err(Error::Disconnected)));
    wait_for(&bus, ConnectionState::Reconnecting).await;
    assert!(format!("{bus:?}").contains("Reconnecting"));

    // Meanwhile calls fail at once.
    assert!(matches!(
        bus.get::<u32>("Device.X").await,
        Err(Error::Disconnected)
    ));

    // The new socket gets the registration first, then every alias, then
    // the subscription.
    let mut peer = l.accept().await;
    expect_route(&mut peer, bus.inbox(), 1).await;
    expect_route(&mut peer, "test.component", route).await;
    expect_route(&mut peer, control::TOPIC_ADVISORY, 2).await;
    wait_for(&bus, ConnectionState::Connected).await;
    let mut names = BTreeSet::new();
    let mut regs = Vec::new();
    for _ in NAMES {
        let f = peer.recv().await;
        assert_eq!(f.header.flags, Flags::REQUEST);
        let r: Route = control::decode(&f.payload).unwrap();
        assert_eq!((r.add, r.route_id), (1, route));
        names.insert(r.topic);
        regs.push(f);
    }
    assert_eq!(names, NAMES.iter().map(|n| n.to_string()).collect());
    // A name another component took meanwhile is given up; the rest
    // carries on.
    let taken: Route = control::decode(&regs[0].payload).unwrap();
    peer.reply_json(&regs[0], &ResultReply { result: 1001 })
        .await;
    for f in &regs[1..] {
        peer.reply_json(f, &ResultReply { result: 0 }).await;
    }
    let req = peer.recv().await;
    let body: SubscribeRequest = request(&req, Method::Subscribe);
    assert_eq!(body.event_name, "Device.A");
    assert!(!body.publish_on_subscribe);
    assert_eq!(body.options.unwrap().component_id, id);
    reply_subscribe(&mut peer, &req, subscribed(1)).await;

    // A frame queued for the lost socket never reaches the new one.
    let stale = Frame::new(
        Header::new(1, Flags::RAW_BINARY, "Device.M.Lost!", "test.component"),
        b"lost".to_vec(),
    );
    bus.inner.conn.queue_for(&stale, 0).unwrap();

    // Events and calls work again.
    peer.send(event_frame(
        bus.inbox(),
        "Device.A",
        event(
            "Device.A",
            EventKind::ValueChanged,
            Object::new().with("value", 7i32),
            id,
        ),
    ))
    .await;
    assert_eq!(sub.next().await.unwrap().unwrap().new, 7);
    let get = tokio::spawn({
        let bus = bus.clone();
        async move { bus.get::<u32>("Device.X").await }
    });
    let req = peer.recv().await;
    let _: GetRequest = request(&req, Method::GetParameterValues);
    peer.reply_rbus(
        &req,
        &get_ok(vec![Property::new("Device.X", Value::U32(3))]),
    )
    .await;
    assert_eq!(get.await.unwrap().unwrap(), 3);

    // Dropping the provider removes the names it still holds, so a later
    // reconnect wouldn't register them. The taken name is never removed:
    // rtrouted would remove the other component's alias.
    assert!(!bus.inner.conn.aliases().contains(&taken.topic));
    drop(provided);
    for name in NAMES.iter().filter(|n| **n != taken.topic) {
        expect_alias(&mut peer, 0, name, route).await;
    }
    assert!(bus.inner.conn.aliases().is_empty());
    let closing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.close().await }
    });
    // Closing unsubscribes and unregisters the component and inbox, and
    // still leaves the taken name alone.
    while let Some(f) = peer.try_recv().await {
        if f.header.topic == control::TOPIC_SUBSCRIBE {
            let r: Route = control::decode(&f.payload).unwrap();
            assert_ne!(r.topic, taken.topic, "removed a name we don't own");
        }
    }
    drop(peer);
    closing.await.unwrap().unwrap();
}

#[tokio::test]
async fn l5_a_failed_registration_is_not_replayed() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect_with(builder(quick())).await;
    let route = bus.inner.conn.component_route;
    let provide = tokio::spawn({
        let bus = bus.clone();
        async move { bus.provide(Model).await }
    });
    let mut regs = Vec::new();
    for name in NAMES {
        regs.push(expect_alias(&mut peer, 1, name, route).await);
    }
    peer.reply_json(&regs[0], &ResultReply { result: 1001 })
        .await;
    for f in &regs[1..] {
        peer.reply_json(f, &ResultReply { result: 0 }).await;
    }
    assert!(provide.await.unwrap().is_err());
    for name in &NAMES[1..] {
        expect_alias(&mut peer, 0, name, route).await;
    }
    assert!(bus.inner.conn.aliases().is_empty());
}

#[tokio::test]
async fn l5_losing_the_socket_again_while_replaying() {
    let l = Listener::new();
    let (bus, mut peer) = l.connect_with(builder(quick())).await;
    let route = bus.inner.conn.component_route;
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
    let _provided = provide.await.unwrap().unwrap();
    drop(peer);

    // The second socket goes away before answering the replay.
    let mut peer = l.accept().await;
    peer.recv().await;
    peer.recv().await;
    peer.recv().await;
    drop(peer);

    let mut peer = l.accept().await;
    peer.recv().await;
    peer.recv().await;
    wait_for(&bus, ConnectionState::Connected).await;
    let mut regs = Vec::new();
    for _ in NAMES {
        regs.push(peer.recv().await);
    }
    for f in &regs {
        peer.reply_json(f, &ResultReply { result: 0 }).await;
    }
    let closing = tokio::spawn({
        let bus = bus.clone();
        async move { bus.close().await }
    });
    while peer.try_recv().await.is_some() {}
    drop(peer);
    closing.await.unwrap().unwrap();
}

#[tokio::test]
async fn l5_close_while_reconnecting() {
    let l = Listener::new();
    let (bus, peer) = l.connect_with(builder(quick())).await;
    // Nobody listens any more.
    drop(l);
    drop(peer);
    wait_for(&bus, ConnectionState::Reconnecting).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    tokio::time::timeout(Duration::from_secs(1), bus.close())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*bus.state().borrow(), ConnectionState::Closed);
    assert!(matches!(
        bus.get::<u32>("Device.X").await,
        Err(Error::Disconnected)
    ));
}

#[tokio::test]
async fn l5_dropping_the_bus_while_reconnecting() {
    let l = Listener::new();
    let (bus, peer) = l.connect_with(builder(quick())).await;
    let mut state = bus.state();
    drop(l);
    drop(peer);
    state
        .wait_for(|s| *s == ConnectionState::Reconnecting)
        .await
        .unwrap();
    drop(bus);
    // The connection task ends, and with it the state's sender.
    tokio::time::timeout(Duration::from_secs(1), async {
        while state.changed().await.is_ok() {}
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn l5_reconnecting_gives_up() {
    let l = Listener::new();
    let retry = quick().max_elapsed(Duration::from_millis(50));
    let (bus, peer) = l.connect_with(builder(retry)).await;
    drop(l);
    drop(peer);
    wait_for(&bus, ConnectionState::Closed).await;
    assert!(matches!(
        bus.get::<u32>("Device.X").await,
        Err(Error::Disconnected)
    ));
    bus.close().await.unwrap();
}

#[tokio::test]
async fn l5_garbage_from_the_broker_reconnects() {
    use tokio::io::AsyncWriteExt;
    let l = Listener::new();
    let (bus, mut peer) = l.connect_with(builder(quick())).await;
    peer.stream.write_all(&[0u8; 64]).await.unwrap();
    let mut peer = l.accept().await;
    expect_route(&mut peer, bus.inbox(), 1).await;
    wait_for(&bus, ConnectionState::Connected).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn l5_closed_stays_closed() {
    let l = Listener::new();
    let (bus, peer) = l
        .connect_with(
            Rbus::builder("test.component")
                .timeouts(crate::Timeouts::uniform(Duration::from_millis(50)))
                .reconnect(quick()),
        )
        .await;
    // The broker never closes its side, so close() gives up and drops the
    // connection itself; losing the socket after that changes nothing.
    bus.close().await.unwrap();
    drop(peer);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(*bus.state().borrow(), ConnectionState::Closed);
    assert!(matches!(
        bus.get::<u32>("Device.X").await,
        Err(Error::Disconnected)
    ));
}
