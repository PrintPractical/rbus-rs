//! Connection and lifecycle against the real rtrouted (L1, L13).
//! Run with `scripts/interop.sh`.

mod common;

use common::{connect, lists, test_provider, wait_for_routes};
use rbus::{ConnectionState, Error};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l1_connect_registers_inbox_and_component() {
    let bus = connect("l1-register").await;
    let (inbox, component) = (bus.inbox().to_owned(), bus.component().to_owned());
    let routes = wait_for_routes(|r| lists(r, &inbox) && lists(r, &component));
    assert!(lists(&routes, &inbox), "inbox {inbox} not routed: {routes}");
    assert!(
        lists(&routes, &component),
        "component {component} not routed: {routes}"
    );
    assert!(inbox.ends_with(&format!(".INBOX.{}", std::process::id())));
    assert_eq!(*bus.state().borrow(), ConnectionState::Connected);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l1_requests_work_right_after_connect() {
    let _provider = test_provider();
    // Registration is fire-and-forget; the broker handles our frames in order,
    // so the reply to the first request still finds the inbox.
    let bus = connect("l1-first-request").await;
    let v: i32 = bus.get("Device.TestProvider.Value.INT32").await.unwrap();
    assert_eq!(v, -10_000_000);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l1_handles_are_independent_components() {
    let a = connect("l1-a").await;
    let b = connect("l1-b").await;
    assert_ne!(a.inbox(), b.inbox());
    a.close().await.unwrap();
    // Closing one connection leaves the other working.
    let _provider = test_provider();
    let s: String = b.get("Device.TestProvider.Value.STRING").await.unwrap();
    assert_eq!(s, "short string");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l13_close_unregisters_from_the_broker() {
    let bus = connect("l13-close").await;
    let (inbox, component) = (bus.inbox().to_owned(), bus.component().to_owned());
    wait_for_routes(|r| lists(r, &component));
    bus.close().await.unwrap();
    assert_eq!(*bus.state().borrow(), ConnectionState::Closed);
    let routes = wait_for_routes(|r| !lists(r, &inbox) && !lists(r, &component));
    assert!(
        !lists(&routes, &component),
        "still routed after close: {routes}"
    );
    assert!(
        !lists(&routes, &inbox),
        "still routed after close: {routes}"
    );
    // Every clone sees the closed connection.
    let clone = bus.clone();
    assert!(matches!(
        clone.get::<u32>("Device.TestProvider.Value.UINT32").await,
        Err(Error::Disconnected)
    ));
    bus.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l13_dropping_the_last_handle_unregisters() {
    let bus = connect("l13-drop").await;
    let component = bus.component().to_owned();
    wait_for_routes(|r| lists(r, &component));
    let clone = bus.clone();
    drop(bus);
    // Still registered while a clone lives.
    assert!(lists(&wait_for_routes(|_| true), &component));
    drop(clone);
    let routes = wait_for_routes(|r| !lists(r, &component));
    assert!(
        !lists(&routes, &component),
        "still routed after drop: {routes}"
    );
}
