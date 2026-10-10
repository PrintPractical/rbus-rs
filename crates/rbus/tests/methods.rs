//! Method invocation through a real C provider (M1, M3, V6, L6).
//! Run with `scripts/interop.sh`.
//!
//! `rbusTestProvider` runs method handlers on its one callback thread, so the
//! 4-second `MethodAsync1()` delays the other tests here a little.

mod common;

use std::time::Duration;

use common::{connect, test_provider};
use rbus::{Error, Object, Rbus, Status, Value};
use rbus_testkit::broker::broker_socket;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug, PartialEq)]
struct Named {
    name: String,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn m1_invoke_and_deserialize_the_result() {
    let _p = test_provider();
    let bus = connect("m1-invoke").await;
    let out: Named = bus
        .invoke("Device.TestProvider.MethodAsync1()", ())
        .await
        .unwrap();
    assert_eq!(out.name, "MethodAsync1()");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn m1_unknown_method_is_not_found() {
    let _p = test_provider();
    let bus = connect("m1-unknown").await;
    let r = bus
        .invoke::<_, ()>("Device.TestProvider.NoSuchMethod()", ())
        .await;
    assert!(matches!(r, Err(Error::NotFound(n)) if n == "Device.TestProvider.NoSuchMethod()"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn m3_method_error_carries_message_and_detail() {
    let _p = test_provider();
    let bus = connect("m3-error").await;
    match bus
        .invoke::<_, Object>("Device.TestProvider.Method3()", ())
        .await
    {
        Err(Error::Method {
            method,
            status,
            message,
            detail,
        }) => {
            assert_eq!(method, "Device.TestProvider.Method3()");
            assert_eq!(status, Status::InvalidOperation);
            assert_eq!(message.as_deref(), Some("RBUS_ERROR_INVALID_OPERATION"));
            assert_eq!(detail.get("error_code"), Some(&Value::I32(9)));
            assert_eq!(detail.get("name"), Some(&Value::String("Method3()".into())));
        }
        other => panic!("{other:?}"),
    }
    // A handler that only returns a status: the C library fills in
    // error_code and error_string itself (rbus.c:2524-2532).
    match bus
        .invoke::<_, ()>("Device.TestProvider.Method11()", ())
        .await
    {
        Err(Error::Method {
            status,
            message,
            detail,
            ..
        }) => {
            assert_eq!(status, Status::InvalidOperation);
            assert_eq!(message.as_deref(), Some("invalid operation"));
            assert_eq!(detail.get("error_code"), Some(&Value::I32(9)));
        }
        other => panic!("{other:?}"),
    }
}

#[derive(Serialize)]
struct EchoArgs {
    param1: String,
    count: u32,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn v6_serde_arguments_reach_the_c_provider() {
    let _p = test_provider();
    let bus = connect("v6-args").await;
    // MethodAsync3() answers asynchronously with INVALID_INPUT and echoes
    // param1 as the C library prints it.
    let args = EchoArgs {
        param1: "hello from rust".into(),
        count: 3,
    };
    match bus
        .invoke::<_, ()>("Device.TestProvider.MethodAsync3()", args)
        .await
    {
        Err(Error::Method { status, detail, .. }) => {
            assert_eq!(status, Status::InvalidInput);
            assert_eq!(
                detail.get("value"),
                Some(&Value::String(
                    "Async Method Response inParams=hello from rust\n".into()
                ))
            );
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn l6_default_timeout_applies_to_every_call() {
    let _p = test_provider();
    let bus = Rbus::builder("rbus-rs.test.l6-timeout")
        .socket(broker_socket())
        .default_timeout(Duration::from_millis(500))
        .connect()
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let r = bus
        .invoke::<_, ()>("Device.TestProvider.MethodAsync1()", ())
        .await;
    assert!(matches!(r, Err(Error::Timeout { op: "invoke" })), "{r:?}");
    assert!(started.elapsed() < Duration::from_secs(3));
    // A per-call limit is plain tokio, and cancel-safe.
    let r = tokio::time::timeout(
        Duration::from_millis(100),
        bus.invoke::<_, ()>("Device.TestProvider.MethodAsync1()", ()),
    )
    .await;
    assert!(r.is_err());
}
