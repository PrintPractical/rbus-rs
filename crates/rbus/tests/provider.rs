//! The provider engine (M4) against C consumers (`rbuscli`) and the Rust
//! consumer, through the real broker. Each test serves the sample model of
//! `common/model.rs` under its own root. Run with `scripts/interop.sh`.

mod common;

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::model::{ResetArgs, ResetReply, Sample};
use common::{connect, raw_client, rbuscli, rbuscli_values};
use rbus::provider::{Access, Context, Fault, Provider, Schema};
use rbus::{Error, Object, PropertyName, RowId, Status, Value};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p1_c_and_rust_consumers_get_and_set() {
    let provider_bus = connect("p1-provider").await;
    let _p = Sample::start(&provider_bus, "P1").await;
    let bus = connect("p1-consumer").await;

    // C consumer.
    let out = rbuscli(&["set", "Device.P1.Name", "string", "from-c"]).await;
    assert!(out.contains("setvalues succeeded"), "{out}");
    let out = rbuscli(&["get", "Device.P1.Name"]).await;
    assert_eq!(
        rbuscli_values(&out),
        [("Device.P1.Name".into(), "from-c".into())]
    );

    // Rust consumer: typed gets, a batch, and the C status codes.
    assert_eq!(bus.get::<String>("Device.P1.Name").await.unwrap(), "from-c");
    bus.set("Device.P1.Enable", false).await.unwrap();
    assert!(!bus.get::<bool>("Device.P1.Enable").await.unwrap());
    let values = bus
        .get_many(["Device.P1.Enable", "Device.P1.Radio.[guest].Channel"])
        .await
        .unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values["Device.P1.Radio.[guest].Channel"], Value::U32(2));
    let err = bus.set("Device.P1.Uptime", 5u32).await.unwrap_err();
    assert_eq!(err.status(), Some(Status::NotWritable), "{err}");
    let err = bus.get::<Value>("Device.P1.Secret").await.unwrap_err();
    assert_eq!(err.status(), Some(Status::InvalidOperation), "{err}");
    let err = bus
        .get::<Value>("Device.P1.Radio.9.Channel")
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementDoesNotExist), "{err}");
    bus.set("Device.P1.Secret", "hidden").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c7_c9_a_transaction_fails_whole_and_names_the_parameter() {
    let provider_bus = connect("c9-provider").await;
    let _p = Sample::start(&provider_bus, "C9").await;
    let bus = connect("c9-consumer").await;
    let err = bus
        .set_many()
        .set("Device.C9.Name", "changed")
        .set("Device.C9.Radio.1.Channel", 14u32)
        .await
        .unwrap_err();
    match err {
        Error::SetFailed { name, status } => {
            assert_eq!(name, "Device.C9.Radio.1.Channel");
            assert_eq!(status, Status::InvalidParameterValue);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(bus.get::<String>("Device.C9.Name").await.unwrap(), "rust");
    bus.set_many()
        .set("Device.C9.Name", "changed")
        .set("Device.C9.Radio.1.Channel", 11u32)
        .await
        .unwrap();
    assert_eq!(
        bus.get::<u32>("Device.C9.Radio.1.Channel").await.unwrap(),
        11
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p2_handlers_run_concurrently() {
    let provider_bus = connect("p2-provider").await;
    let provided = Sample::start(&provider_bus, "P2").await;
    let bus = connect("p2-consumer").await;
    let start = Instant::now();
    let gets = (0..4).map(|_| {
        let bus = bus.clone();
        tokio::spawn(async move { bus.get::<bool>("Device.P2.Slow").await })
    });
    for g in gets.collect::<Vec<_>>() {
        assert!(g.await.unwrap().unwrap());
    }
    // Four 300 ms handlers overlapped.
    assert!(
        start.elapsed() < Duration::from_millis(1000),
        "{:?}",
        start.elapsed()
    );
    assert!(provided.max_in_flight.load(Ordering::SeqCst) > 1);
    // A handler may call back into the bus it serves without deadlocking.
    assert_eq!(
        provider_bus
            .get::<String>("Device.P2.Caller")
            .await
            .unwrap(),
        provider_bus.component()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p3_dropping_the_provider_unregisters_its_names() {
    let provider_bus = connect("p3-provider").await;
    let provided = Sample::start(&provider_bus, "P3").await;
    let bus = connect("p3-consumer").await;
    let mut raw = raw_client();
    assert!(
        raw.owner_of("Device.P3.Name")
            .unwrap()
            .contains(provider_bus.component())
    );
    assert_eq!(bus.get::<String>("Device.P3.Name").await.unwrap(), "rust");
    drop(provided);
    raw.wait_for_no_owner(
        "Device.P3.Name",
        provider_bus.component(),
        Duration::from_secs(5),
    )
    .unwrap();
    let err = bus.get::<Value>("Device.P3.Name").await.unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    let out = rbuscli(&["get", "Device.P3.Name"]).await;
    assert!(out.contains("Failed to get the data"), "{out}");
    // The connection itself is still registered and can provide again.
    let again = Sample::start(&provider_bus, "P3").await;
    assert_eq!(bus.get::<String>("Device.P3.Name").await.unwrap(), "rust");
    drop(again);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p1_overlapping_names_are_refused() {
    let a = connect("overlap-a").await;
    let b = connect("overlap-b").await;
    let _p = Sample::start(&a, "Overlap").await;
    // The same names on the same connection, checked by the engine.
    let err = a.provide(Sample::new("Overlap")).await.unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementNameDuplicate), "{err:?}");
    // On another connection, refused by the broker (1001).
    let err = b.provide(Sample::new("Overlap")).await.unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementNameDuplicate), "{err:?}");
    // The refused registration left nothing behind on b.
    let mut raw = raw_client();
    assert!(
        !raw.owner_of("Device.Overlap.Name")
            .unwrap()
            .contains(b.component())
    );
    let other = b.provide(Sample::new("Overlap2")).await.unwrap();
    drop(other);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p5_methods_from_rust_and_c() {
    let provider_bus = connect("p5-provider").await;
    let _p = Sample::start(&provider_bus, "P5").await;
    let bus = connect("p5-consumer").await;
    let reply: ResetReply = bus
        .invoke("Device.P5.Radio.[guest].Reset()", ResetArgs { delay: 3 })
        .await
        .unwrap();
    assert_eq!(reply, ResetReply { radio: 2, delay: 3 });
    let echo: Object = bus
        .invoke("Device.P5.Echo()", Object::new().with("x", Value::I16(-2)))
        .await
        .unwrap();
    assert_eq!(echo.get("x"), Some(&Value::I16(-2)));
    // A Fault reaches the Rust consumer as Error::Method, with the message
    // and the detail object.
    match bus
        .invoke::<_, Object>("Device.P5.Fail()", ())
        .await
        .unwrap_err()
    {
        Error::Method {
            status,
            message,
            detail,
            ..
        } => {
            assert_eq!(status, Status::InvalidOperation);
            assert_eq!(message.as_deref(), Some("refused"));
            assert_eq!(detail.get("retry_after"), Some(&Value::U32(5)));
            assert_eq!(detail.get("error_code"), Some(&Value::I32(9)));
        }
        other => panic!("{other:?}"),
    }
    let err = bus
        .invoke::<_, Object>("Device.P5.Radio.[guest].Reset()", ())
        .await
        .unwrap_err();
    assert_eq!(
        err.status(),
        Some(Status::InvalidInput),
        "bad arguments: {err:?}"
    );
    let out = rbuscli(&[
        "method_values",
        "Device.P5.Radio.1.Reset()",
        "delay",
        "uint32",
        "9",
    ])
    .await;
    assert_eq!(
        rbuscli_values(&out),
        [("radio".into(), "1".into()), ("delay".into(), "9".into())],
        "{out}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p7_context_names_the_caller_and_the_row() {
    let provider_bus = connect("p7-provider").await;
    let _p = Sample::start(&provider_bus, "P7").await;
    let bus = connect("p7-consumer").await;
    assert_eq!(
        bus.get::<String>("Device.P7.Caller").await.unwrap(),
        bus.component()
    );
    let out = rbuscli(&["get", "Device.P7.Caller"]).await;
    let caller = &rbuscli_values(&out)[0].1;
    assert!(caller.starts_with("rbuscli"), "{out}");
    // The alias arrives as the instance number.
    let reply: ResetReply = bus
        .invoke("Device.P7.Radio.[guest].Reset()", ResetArgs { delay: 0 })
        .await
        .unwrap();
    assert_eq!(reply.radio, 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt2_pt3_rows_from_consumers_and_from_the_provider() {
    let provider_bus = connect("pt2-provider").await;
    let provided = Sample::start(&provider_bus, "PT2").await;
    let bus = connect("pt2-consumer").await;
    let radios = bus.table("Device.PT2.Radio.");
    let row = radios.add_row().alias("rust").await.unwrap();
    assert_eq!(
        row,
        RowId {
            inst: 3,
            alias: Some("rust".into())
        }
    );
    assert_eq!(
        bus.get::<String>("Device.PT2.Radio.[rust].Status")
            .await
            .unwrap(),
        "Up"
    );
    let err = radios.add_row().alias("rust").await.unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementNameDuplicate), "{err:?}");
    // C consumers see and change the same rows.
    let out = rbuscli(&["addrow", "Device.PT2.Radio.", "c"]).await;
    assert!(out.contains("Device.PT2.Radio.4. added"), "{out}");
    let rows = radios.rows().await.unwrap();
    let aliases: Vec<Option<&str>> = rows.iter().map(|r| r.alias.as_deref()).collect();
    assert_eq!(aliases, [None, Some("guest"), Some("rust"), Some("c")]);
    radios.remove_row(&row).await.unwrap();
    let out = rbuscli(&["delrow", "Device.PT2.Radio.4."]).await;
    assert!(out.contains("deleted successfully"), "{out}");
    let err = radios.remove_row(3).await.unwrap_err();
    assert_eq!(err.status(), Some(Status::ElementDoesNotExist), "{err:?}");
    // Rows the provider adds itself (PT3) are listed the same way.
    provided.add_row("Device.PT2.Radio.", 9).unwrap();
    let rows: Vec<u32> = radios
        .rows()
        .await
        .unwrap()
        .iter()
        .map(|r| r.inst)
        .collect();
    assert_eq!(rows, [1, 2, 9]);
    provided.remove_row("Device.PT2.Radio.", 9).unwrap();
    assert_eq!(provided.rows("Device.PT2.Radio.").unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt8_partial_path_and_wildcard_gets_over_our_tree() {
    let provider_bus = connect("pt8-provider").await;
    let _p = Sample::start(&provider_bus, "PT8").await;
    let bus = connect("pt8-consumer").await;
    let tree = bus.get_tree("Device.PT8.Radio.").await.unwrap();
    let names: Vec<&str> = tree.iter().map(|(n, _)| n).collect();
    assert_eq!(
        names,
        [
            "Device.PT8.Radio.1.Channel",
            "Device.PT8.Radio.1.Status",
            "Device.PT8.Radio.2.Channel",
            "Device.PT8.Radio.2.Status"
        ]
    );
    #[derive(serde::Deserialize)]
    struct Radio {
        #[serde(rename = "Channel")]
        channel: u32,
    }
    #[derive(serde::Deserialize)]
    struct Radios {
        #[serde(rename = "Radio")]
        radio: Vec<Radio>,
    }
    let all = bus.get_tree("Device.PT8.").await.unwrap();
    let radios: Radios = all.deserialize().unwrap();
    assert_eq!(radios.radio[1].channel, 2);
    let wild = bus.get_tree("Device.PT8.Radio.*.Status").await.unwrap();
    assert_eq!(wild.len(), 2);
    assert!(wild.failures().is_empty());
    // The same from a C consumer.
    let out = rbuscli(&["get", "Device.PT8.Radio.*.Channel"]).await;
    assert_eq!(rbuscli_values(&out).len(), 2, "{out}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn pt10_element_names_and_row_names() {
    let provider_bus = connect("pt10-provider").await;
    let _p = Sample::start(&provider_bus, "PT10").await;
    let bus = connect("pt10-consumer").await;
    let rows = bus.table("Device.PT10.Radio.").rows().await.unwrap();
    assert_eq!(
        rows[1],
        RowId {
            inst: 2,
            alias: Some("guest".into())
        }
    );
    let out = rbuscli(&["getrows", "Device.PT10.Radio."]).await;
    assert!(
        out.contains("instNum=2, alias=guest, name=Device.PT10.Radio.2."),
        "{out}"
    );
    let out = rbuscli(&["getnames", "Device.PT10.Radio.2."]).await;
    for (name, ty, access) in [
        ("Device.PT10.Radio.2.Channel", "Property", "110010"),
        ("Device.PT10.Radio.2.Status", "Property", "100010"),
        ("Device.PT10.Radio.2.Reset()", "Method", "000001"),
    ] {
        let entry = format!("Name  : {name}\n              Type  : {ty}");
        let at = out.find(&entry).unwrap_or_else(|| panic!("{name}: {out}"));
        assert!(
            out[at..].contains(&format!("Access Flags: {access}")),
            "{name}: {out}"
        );
    }
    // The broker lists the registered templates.
    let elements = bus
        .discover()
        .elements(provider_bus.component())
        .await
        .unwrap();
    assert!(
        elements
            .iter()
            .any(|e| e == "Device.PT10.Radio.{i}.Reset()"),
        "{elements:?}"
    );
    let owner = bus
        .discover()
        .owner("Device.PT10.Radio.2.Status")
        .await
        .unwrap();
    assert_eq!(owner, provider_bus.component());
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn p19_direct_mode_is_refused() {
    let provider_bus = connect("p19-provider").await;
    let _p = Sample::start(&provider_bus, "P19").await;
    let out = rbuscli(&["opendirect", "Device.P19.Name"]).await;
    assert!(
        out.contains("Failed to open direct connection to Device.P19.Name"),
        "{out}"
    );
    // The provider still serves the normal way afterwards.
    let out = rbuscli(&["get", "Device.P19.Name"]).await;
    assert_eq!(rbuscli_values(&out)[0].1, "rust", "{out}");
}

/// A provider whose getter waits longer than the consumer does.
struct Stuck;

impl Provider for Stuck {
    fn schema(&self) -> Schema {
        Schema::new().property("Device.C5Rust.Stuck.Value", Access::ReadOnly)
    }

    async fn get(&self, _cx: &Context, _name: &PropertyName) -> Result<Value, Fault> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Ok(Value::Bool(true))
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn c5_a_slow_rust_component_is_reported_not_masked() {
    let good_bus = connect("c5-good").await;
    let _good = Sample::start(&good_bus, "C5Rust").await;
    let stuck_bus = connect("c5-stuck").await;
    let _stuck = stuck_bus.provide(Stuck).await.unwrap();
    let bus = rbus::Rbus::builder("rbus-rs.test.c5-consumer")
        .socket(rbus_testkit::broker::broker_socket())
        .default_timeout(Duration::from_secs(1))
        .connect()
        .await
        .unwrap();
    let tree = bus.get_tree("Device.C5Rust.").await.unwrap();
    assert_eq!(tree.get("Device.C5Rust.Name"), Some(&Value::from("rust")));
    assert_eq!(tree.failures().len(), 1);
    assert_eq!(tree.failures()[0].component, stuck_bus.component());
    assert!(matches!(tree.failures()[0].error, Error::Timeout { .. }));
}
