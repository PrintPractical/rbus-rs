//! Get and set through real C providers (C1, C2, C6, C7, C9, X3).
//! Run with `scripts/interop.sh`.
//!
//! `rbusSampleProvider` owns `Device.SampleProvider.*` and opens a second
//! component, `Second-Provider-for-Sample`, for `.AllTypes.*`: one process,
//! two owners. Tests share one instance, so each writes its own elements.

mod common;

use common::{connect, sample_provider};
use rbus::{Error, Status, Value};

const SP: &str = "Device.SampleProvider";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c1_get_a_single_value() {
    let _p = sample_provider();
    let bus = connect("c1-get").await;
    let maker: String = bus
        .get("Device.DeviceInfo.SampleProvider.Manufacturer")
        .await
        .unwrap();
    assert_eq!(maker, "COMCAST");
    let version: f32 = bus
        .get("Device.DeviceInfo.SampleProvider.SoftwareVersion")
        .await
        .unwrap();
    assert_eq!(version, 2.14);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c1_unknown_name_is_not_found() {
    let _p = sample_provider();
    let bus = connect("c1-unknown").await;
    for name in [
        "Device.Does.Not.Exist",
        "Device.SampleProvider.SampleData.Nope",
    ] {
        match bus.get::<Value>(name).await {
            Err(Error::NotFound(n)) => assert_eq!(n, name),
            other => panic!("{name}: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c2_get_many_across_two_components() {
    let _p = sample_provider();
    let bus = connect("c2-get-many").await;
    let names = [
        "Device.DeviceInfo.SampleProvider.Manufacturer",
        "Device.SampleProvider.AllTypes.UInt64Data",
        "Device.DeviceInfo.SampleProvider.ModelName",
        "Device.SampleProvider.NestedObject2.TestParam",
    ];
    let values = bus.get_many(names).await.unwrap();
    let keys: Vec<&str> = values.keys().map(|k| k.as_str()).collect();
    assert_eq!(keys, names, "results keep the order asked for");
    assert_eq!(values[names[0]], Value::String("COMCAST".into()));
    assert_eq!(values[names[1]], Value::U64(u64::MAX));
    assert_eq!(values[names[2]], Value::String("XB3".into()));
    assert_eq!(
        values[names[3]],
        Value::String("Testing Rbus Nested Object".into())
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c2_get_many_names_the_missing_element() {
    let _p = sample_provider();
    let bus = connect("c2-missing").await;
    let r = bus
        .get_many([
            "Device.DeviceInfo.SampleProvider.Manufacturer",
            "Device.SampleProvider.Nope",
        ])
        .await;
    assert!(matches!(r, Err(Error::NotFound(n)) if n == "Device.SampleProvider.Nope"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c6_set_a_single_value() {
    let _p = sample_provider();
    let bus = connect("c6-set").await;
    let name = format!("{SP}.SampleData.IntData");
    bus.set(name.as_str(), 42).await.unwrap();
    // This provider's getter adds 101 on every read.
    assert_eq!(bus.get::<i32>(name.as_str()).await.unwrap(), 143);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn x3_provider_errors_are_typed() {
    let _p = sample_provider();
    let bus = connect("x3-errors").await;
    // The provider rejects the wrong type.
    match bus.set(format!("{SP}.SampleData.UIntData"), "seven").await {
        Err(Error::SetFailed { name, status }) => {
            assert_eq!(name, "Device.SampleProvider.SampleData.UIntData");
            assert_eq!(status, Status::InvalidInput);
        }
        other => panic!("{other:?}"),
    }
    // The C library refuses writes to an element without a setter.
    match bus
        .set("Device.DeviceInfo.SampleProvider.Manufacturer", "ACME")
        .await
    {
        Err(e @ Error::SetFailed { .. }) => assert_eq!(e.status(), Some(Status::NotWritable)),
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c7_set_many_across_two_components() {
    let _p = sample_provider();
    let bus = connect("c7-set-many").await;
    let (b, u, i) = (
        format!("{SP}.SampleData.BoolData"),
        format!("{SP}.SampleData.UIntData"),
        format!("{SP}.AllTypes.Int16Data"),
    );
    bus.set_many()
        .set(b.as_str(), true)
        .set(i.as_str(), -5i16)
        .set(u.as_str(), 77u32)
        .await
        .unwrap();
    let values = bus.get_many([&b, &u, &i]).await.unwrap();
    assert_eq!(values[b.as_str()], Value::Bool(true));
    assert_eq!(values[u.as_str()], Value::U32(77));
    assert_eq!(values[i.as_str()], Value::I16(-5));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c9_set_many_names_the_failing_parameter() {
    let _p = sample_provider();
    let bus = connect("c9-failed").await;
    let ok = format!("{SP}.AllTypes.UInt16Data");
    let read_only = format!("{SP}.NestedObject1.TestParam");
    let r = bus
        .set_many()
        .set(ok.as_str(), 99u16)
        .set(read_only.as_str(), "x")
        .await;
    match r {
        Err(Error::SetFailed { name, status }) => {
            assert_eq!(name, read_only.as_str());
            assert_eq!(status, Status::NotWritable);
        }
        other => panic!("{other:?}"),
    }
    // The other component's batch went first and was applied: no rollback.
    assert_eq!(bus.get::<u16>(ok.as_str()).await.unwrap(), 99);
}
