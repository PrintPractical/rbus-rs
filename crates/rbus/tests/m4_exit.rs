//! M4 exit test: `rbuscli` get, set, setv, addrow, delrow, getnames and
//! method invoke all work against a hand-written Rust provider. The other
//! half of the exit test, the `rbusTestConsumer` groups against the
//! conformance provider, is `test_consumer.rs`. Run with `scripts/interop.sh`.

mod common;

use common::model::Sample;
use common::{connect, rbuscli, rbuscli_values};

fn value_of(out: &str, name: &str) -> Option<String> {
    rbuscli_values(out)
        .into_iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs rtrouted: run scripts/interop.sh"]
async fn m4_exit_rbuscli_drives_a_rust_provider() {
    let bus = connect("m4-exit").await;
    let _provided = Sample::start(&bus, "M4Exit").await;
    let p = |leaf: &str| format!("Device.M4Exit.{leaf}");

    // get: one value, a partial path, a wildcard.
    let out = rbuscli(&["get", &p("Name")]).await;
    assert_eq!(value_of(&out, &p("Name")).as_deref(), Some("rust"), "{out}");
    let out = rbuscli(&["get", &p("")]).await;
    let names: Vec<String> = rbuscli_values(&out).into_iter().map(|(n, _)| n).collect();
    assert_eq!(
        names,
        [
            "Enable",
            "Name",
            "Uptime",
            "Slow",
            "Caller",
            "Radio.1.Channel",
            "Radio.1.Status",
            "Radio.2.Channel",
            "Radio.2.Status"
        ]
        .map(p),
        "{out}"
    );
    assert!(value_of(&out, &p("Caller")).unwrap().starts_with("rbuscli"));
    let out = rbuscli(&["get", &p("Radio.*.Channel")]).await;
    assert_eq!(rbuscli_values(&out).len(), 2, "{out}");

    // set, and a read-only property refused with NOT_WRITABLE (27).
    let out = rbuscli(&["set", &p("Name"), "string", "from-cli"]).await;
    assert!(out.contains("setvalues succeeded"), "{out}");
    assert_eq!(bus.get::<String>(p("Name")).await.unwrap(), "from-cli");
    let out = rbuscli(&["set", &p("Uptime"), "uint32", "1"]).await;
    assert!(out.contains("failed with return value: 27"), "{out}");

    // setv: one transaction. An invalid channel fails all of it, and the
    // parameter is named.
    let out = rbuscli(&[
        "setv",
        &p("Enable"),
        "bool",
        "false",
        &p("Radio.[guest].Channel"),
        "uint32",
        "6",
    ])
    .await;
    assert!(out.contains("setvalues succeeded"), "{out}");
    assert!(!bus.get::<bool>(p("Enable")).await.unwrap());
    assert_eq!(bus.get::<u32>(p("Radio.2.Channel")).await.unwrap(), 6);
    let out = rbuscli(&[
        "setv",
        &p("Enable"),
        "bool",
        "true",
        &p("Radio.1.Channel"),
        "uint32",
        "14",
    ])
    .await;
    assert!(
        out.contains(&format!("Failed to set value for {}", p("Radio.1.Channel"))),
        "{out}"
    );
    assert!(
        !bus.get::<bool>(p("Enable")).await.unwrap(),
        "nothing applied"
    );

    // addrow, getrows, delrow.
    let out = rbuscli(&["addrow", &p("Radio."), "cli"]).await;
    assert!(out.contains(&format!("{} added", p("Radio.3."))), "{out}");
    let out = rbuscli(&["getrows", &p("Radio.")]).await;
    assert!(
        out.contains(&format!("instNum=3, alias=cli, name={}", p("Radio.3."))),
        "{out}"
    );
    let out = rbuscli(&["get", &p("Radio.[cli].Status")]).await;
    assert_eq!(
        value_of(&out, &p("Radio.[cli].Status")).as_deref(),
        Some("Up"),
        "{out}"
    );
    let out = rbuscli(&["delrow", &p("Radio.[cli].")]).await;
    assert!(out.contains("deleted successfully"), "{out}");
    let out = rbuscli(&["getrows", &p("Radio.")]).await;
    assert!(!out.contains("alias=cli"), "{out}");

    // getnames, next level only (the default) and the whole tree.
    let out = rbuscli(&["getnames", &p("")]).await;
    assert!(
        out.contains(&format!("Component {}:", bus.component())),
        "{out}"
    );
    for (name, ty) in [
        ("Enable", "Property"),
        ("Secret", "Property"),
        ("Radio.", "Table"),
        ("Echo()", "Method"),
    ] {
        assert!(
            out.contains(&format!("Name  : {}\n              Type  : {ty}", p(name))),
            "{name}: {out}"
        );
    }
    assert!(!out.contains(&p("Radio.1.")), "next level only: {out}");
    let out = rbuscli(&["getnames", &p(""), "false"]).await;
    for name in ["", "Radio.1.", "Radio.2.Reset()"] {
        assert!(
            out.contains(&format!("Name  : {}\n", p(name))),
            "{name}: {out}"
        );
    }

    // Methods, with arguments and failing.
    let out = rbuscli(&[
        "method_values",
        &p("Radio.[guest].Reset()"),
        "delay",
        "uint32",
        "7",
    ])
    .await;
    assert_eq!(
        rbuscli_values(&out),
        [("radio".into(), "2".into()), ("delay".into(), "7".into())],
        "{out}"
    );
    let out = rbuscli(&["method_noargs", &p("Fail()")]).await;
    assert!(
        out.contains("failed for") && out.contains("'invalid operation'"),
        "{out}"
    );
    assert!(
        out.contains("name=error_string") && out.contains("value:refused"),
        "{out}"
    );
}
