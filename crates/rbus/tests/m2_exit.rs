//! The M2 exit test: against the C sample providers, get, set and invoke work,
//! and an unknown name gives `NotFound`. Run with `scripts/interop.sh`.
//!
//! `rbusMethodProvider` exits 10 seconds after it starts, so it gets this
//! binary to itself.

mod common;

use common::{connect, sample_provider};
use rbus::{Error, Status, Value};
use rbus_testkit::broker::shared_peer;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct Args {
    param1: String,
    param2: i32,
}

#[derive(Deserialize)]
struct Reply {
    value: String,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn m2_exit_get_set_invoke_against_the_c_sample_providers() {
    let _sample = sample_provider();
    let _methods = shared_peer(
        "rbusMethodProvider",
        &[],
        "Device.Methods.SimpleMethod()",
        "MethodProvider",
    );
    let bus = connect("m2-exit").await;

    bus.set("Device.SampleProvider.SampleData.UIntData", 2024u32)
        .await
        .unwrap();
    let got: u32 = bus
        .get("Device.SampleProvider.SampleData.UIntData")
        .await
        .unwrap();
    assert_eq!(got, 2024);

    let reply: Reply = bus
        .invoke(
            "Device.Methods.SimpleMethod()",
            Args {
                param1: "hello".into(),
                param2: 5,
            },
        )
        .await
        .unwrap();
    assert_eq!(reply.value, "Simple Method Response");

    match bus
        .invoke::<_, ()>("Device.Methods.SimpleMethod1()", ())
        .await
    {
        Err(Error::Method {
            status,
            message,
            detail,
            ..
        }) => {
            assert_eq!(status, Status::InvalidMethod);
            assert_eq!(message.as_deref(), Some("Provider Specific Error Code"));
            assert_eq!(detail.get("error_code"), Some(&Value::I32(56789)));
        }
        other => panic!("{other:?}"),
    }

    assert!(matches!(
        bus.get::<Value>("Device.Does.Not.Exist").await,
        Err(Error::NotFound(_))
    ));
    bus.close().await.unwrap();
}
