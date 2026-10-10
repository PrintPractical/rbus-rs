//! The design.md section 4 examples that M2 implements, run against real
//! providers. Element names are swapped for ones the C peers serve. The
//! `name!` macro (M6) and `.reconnect(..)` (M7) arrive with their milestones.
//! Run with `scripts/interop.sh`.

mod common;

use std::time::Duration;

use common::{sample_provider, test_provider};
use rbus::{ConnectionState, Error, MethodName, Object, PropertyName, Rbus, Value};
use rbus_testkit::broker::broker_socket;
use serde::{Deserialize, Serialize};

/// 4.4 Connecting and lifecycle.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_4_connecting_and_lifecycle() -> rbus::Result<()> {
    let _p = test_provider();
    let bus = Rbus::connect("com.example.wifi").await?; // unix:///tmp/rtrouted
    bus.close().await?;

    let bus = Rbus::builder("com.example.wifi")
        .socket(broker_socket()) // L1: unix socket only
        .default_timeout(Duration::from_secs(5)) // L6
        .connect()
        .await?;

    // Per-call limits are plain tokio, and cancel-safe.
    let up: u32 = tokio::time::timeout(
        Duration::from_secs(1),
        bus.get("Device.TestProvider.Value.UINT32"),
    )
    .await
    .expect("in time")?;
    assert_eq!(up, 10_000_000);

    let state = bus.state(); // watch::Receiver<ConnectionState>
    assert_eq!(*state.borrow(), ConnectionState::Connected);
    bus.close().await?; // L13
    assert_eq!(*state.borrow(), ConnectionState::Closed);
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct RebootArgs {
    delay: u32,
    reason: String,
}

/// 4.5 Names and values.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_5_names_and_values() -> rbus::Result<()> {
    const UPTIME: PropertyName = PropertyName::from_static("Device.TestProvider.Value.UINT32");
    const METHOD: MethodName = MethodName::from_static("Device.TestProvider.Method11()");

    let _p = test_provider();
    let bus = Rbus::connect("com.example.names").await?;

    let up: u32 = bus.get(UPTIME).await?; // T: RbusType
    assert_eq!(up, 10_000_000);
    // A typed method name goes straight through...
    assert!(matches!(
        bus.invoke::<_, ()>(METHOD, ()).await,
        Err(Error::Method { .. })
    ));
    // ...and a string is checked at run time.
    assert!(matches!(
        bus.invoke::<_, ()>("Device.TestProvider.Method11", ())
            .await,
        Err(Error::Name(_))
    ));

    let obj: Object = rbus::to_object(&RebootArgs {
        delay: 5,
        reason: "update".into(),
    })?;
    let back: RebootArgs = rbus::from_object(obj)?;
    assert_eq!(back.reason, "update");
    Ok(())
}

#[derive(Deserialize)]
struct Named {
    name: String,
}

/// 4.6 Consumer API: get, set and methods.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn design_4_6_consumer_get_set_invoke() -> rbus::Result<()> {
    let _s = sample_provider();
    let _t = test_provider();
    let bus = Rbus::connect("com.example.consumer").await?;

    // Get (C1, C2)
    let maker: String = bus
        .get("Device.DeviceInfo.SampleProvider.Manufacturer")
        .await?;
    assert_eq!(maker, "COMCAST");
    let values = bus
        .get_many([
            "Device.DeviceInfo.SampleProvider.ModelName",
            "Device.TestProvider.Value.STRING",
        ])
        .await?; // IndexMap<PropertyName, Value>
    assert_eq!(
        values["Device.TestProvider.Value.STRING"],
        Value::String("short string".into())
    );

    // Set (C6, C7, C9): one transaction; the commit flag never appears.
    bus.set("Device.SampleProvider.AllTypes.BoolData", true)
        .await?;
    bus.set_many()
        .set("Device.SampleProvider.AllTypes.BoolData", false)
        .set("Device.SampleProvider.AllTypes.UInt32Data", 36u32)
        .await?;
    let r = bus
        .set_many()
        .set("Device.SampleProvider.AllTypes.BoolData", true)
        .set("Device.DeviceInfo.SampleProvider.ModelName", "XB7")
        .await;
    assert!(
        matches!(&r, Err(Error::SetFailed { name, .. }) if name == "Device.DeviceInfo.SampleProvider.ModelName"),
        "{r:?}"
    );

    // Methods (M1, M3): serde in, serde out.
    let reply: Named = bus.invoke("Device.TestProvider.MethodAsync1()", ()).await?;
    assert_eq!(reply.name, "MethodAsync1()");
    Ok(())
}
