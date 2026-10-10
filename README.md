# rbus-rs

An async, idiomatic Rust client and provider for RDK's RBUS.

rbus-rs speaks the `rtrouted` wire protocol directly. There is no C library to link, no FFI and no `unsafe`, yet it interoperates with C consumers and providers in both directions.

- **Consume:** get and set values (a batch of sets is one transaction), read partial paths and wildcards as trees, call methods through serde, manage table rows, discover components, and receive events as typed `Stream`s.
- **Provide:** declare a data model as a struct and rbus serves gets, applies sets, keeps table rows and publishes every change. Impl blocks, a builder and a plain `Provider` trait are there when you need more control.
- **Interop-tested:** CI runs the suite against the real `rtrouted`, `rbuscli` and rbus's own conformance programs.

> **Status:** pre-release. The crates are not on crates.io yet, and the API may still change.

The RBUS protocol and its reference implementation are [RDK's rbus](https://github.com/rdkcentral/rbus) (Apache-2.0). rbus-rs is an independent reimplementation that contains no rbus code. The C sources served as the protocol reference, and the C programs serve as interop test peers.

## No unsafe, no C

Every crate is `#![forbid(unsafe_code)]`, and no dependency links a native library. The C formats live in one sans-I/O crate, `rbus-wire`. The layers above it are plain async Rust:

```text
+--------------+-------------------------------------------------------------------------+
| Your app     | Consumer calls on Rbus; bus.provide(model) for what you serve           |
| rbus-macros  | name!, derive(Model), derive(Row), rbus::provider: expand to trait impls|
| Traits       | Provider, RbusType, serde Object mapping; the DataModel builder         |
| rbus engine  | Rbus handle, connection task, transactions, subscription registry       |
| rbus-wire    | Header framing, msgpack, JSON control, legacy decoding; no I/O          |
+--------------+-------------------------------------------------------------------------+
| rtrouted     | The C broker, unchanged: routes by element name and inbox               |
+--------------+-------------------------------------------------------------------------+
```

| Crate | What it is |
| --- | --- |
| `rbus` | The `Rbus` handle: connection, consumer API, provider engine. Built on tokio. |
| `rbus-macros` | `name!`, `#[derive(Model)]`, `#[derive(Row)]` and `#[rbus::provider]`. Re-exported by `rbus` (feature `macros`, on by default). |
| `rbus-wire` | Framing, msgpack payloads, values, events and broker control messages. No I/O and no runtime. |
| `rbus-testkit` | Test-only helpers for the interop suite. Not published. |

## Quick start

A consumer:

```rust,no_run
use rbus::Rbus;

#[tokio::main]
async fn main() -> rbus::Result<()> {
    let bus = Rbus::connect("com.example.app").await?;

    let uptime: u32 = bus.get("Device.DeviceInfo.UpTime").await?;
    println!("up for {uptime} s");

    bus.set_many()
        .set("Device.WiFi.Radio.1.Enable", true)
        .set("Device.WiFi.Radio.1.Channel", 36u32)
        .await?;

    let mut changes = bus
        .subscribe_value::<bool>("Device.WiFi.Radio.1.Enable")
        .initial_value()
        .await?;
    while let Some(change) = changes.next().await {
        match change {
            Ok(c) => println!("enable is now {}", c.new),
            Err(e) => eprintln!("{e}"),   // missed events, or a value of another type
        }
    }
    bus.close().await
}
```

A provider, declared as a struct:

```rust,no_run
use rbus::provider::{Event, Prop, Table};
use rbus::Rbus;

#[derive(rbus::Model, Default)]
#[rbus(path = "Device.Example.")]
struct Example {
    #[rbus(writable)]
    enable: Prop<bool>,                // Device.Example.Enable
    #[rbus(name = "Host", add_row, remove_row)]
    hosts: Table<Host>,                // Device.Example.Host.{i}.
    alarm: Event<Alarm>,               // Device.Example.Alarm!
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
struct Host {
    name: String,                      // Device.Example.Host.{i}.Name
    #[rbus(writable)]
    active: bool,
}

#[derive(serde::Serialize)]
struct Alarm {
    level: u8,
}

#[tokio::main]
async fn main() -> rbus::Result<()> {
    let bus = Rbus::connect("com.example.provider").await?;
    let model = bus.provide(Example::default()).await?;   // unregistered on drop

    model.enable.set(true);                               // publishes a value change
    let id = model.hosts.insert(Host { name: "lan".into(), active: true })?;
    model.hosts.update(id, |h| h.active = false)?;
    model.alarm.publish(Alarm { level: 3 })?;

    std::future::pending::<()>().await;
    Ok(())
}
```

`crates/rbus/examples` has more: `consumer.rs`, `provider_model.rs` and `bridge.rs`, a component that consumes and provides on one connection. The rustdoc for `rbus` covers the whole API.

## Compatibility

- **rbus:** v2.14.0, commit `e8cf808`. This is the `third_party/rbus` submodule pin, and CI builds the broker and the C peers from it.
- **Transport:** the broker's unix socket, `/tmp/rtrouted` by default.
- **Platforms:** Linux. The crates also build and pass their unit tests on macOS, though RBUS itself runs on Linux.
- **Rust:** 1.85 or later, with tokio.
- **C peers:** works with C consumers and providers in both directions. It accepts legacy CCSP type ids and error codes, and sends only current rbus ones. Its inbox names end in the process id, as C providers expect.
- **Broker restarts:** opt in to `RbusBuilder::reconnect`, and the connection registers its component, elements and subscriptions again. C clients don't register their elements again.

## Features

Every feature has integration tests against the real broker, named after its ID (`c3_...`, `l5_...`).

| Area | Supported |
| --- | --- |
| Connection | Unix socket (L1); reconnect after a broker restart, with a `state()` watch (L5); per-operation default timeouts (L6); graceful close, plus best-effort cleanup on drop (L13) |
| Values | Every scalar type, plus DateTime (`time` feature), objects and property lists (V1–V4); conversions to native types (V5); serde for values and objects (V6); legacy CCSP values (V7) |
| Get and set | One value or many (C1, C2); partial paths and wildcards as a `Tree`, with per-component failures reported (C3–C5); set and batch set as transactions that name the failing parameter (C6, C7, C9) |
| Tables and methods | Add, remove and list rows, with aliases (T1–T3); methods with serde arguments and results, and typed errors (M1, M3) |
| Discovery | Element owners, component elements, all components, connect and disconnect advisories (D1, D2, D4, D5) |
| Events | Typed streams for value changes, table rows and general events (E1–E5); initial value (E9); retry until the provider appears (E10); a bounded buffer that reports lag, or applies backpressure |
| Providers | Properties, events and methods with async handlers (P1–P5); caller identity (P7); subscription hooks (P8); value-change events, including from state (P9, P11); provider-side filters (P12); initial values (P15); direct-mode requests rejected (P19) |
| Provider tables | Row templates, consumer and app rows, aliases, nested tables (PT1–PT5); partial-path gets over the model (PT8); row events (PT9); row names for `rbuscli` (PT10) |
| Cross-cutting | `tracing` spans per operation (X1); a typed `Error` and `Status` (X3); legacy CCSP codes (X4); both header sizes (X5); tokio (X8) |

## Not supported

- **Out of scope:**
  - direct mode, whose open requests are rejected with an error;
  - TCP and `/etc/rbus_client.conf`;
  - SPAKE2+ and rtCipher encryption;
  - raw-data events;
  - the `rbusMessage_*` API;
  - the session manager API and session ids;
  - interval and duration subscriptions;
  - the element-info browsing API;
  - the C timeout environment variables and override files.
- **Per-call timeouts:** use `tokio::time::timeout`. Every request future is cancel-safe.
- **Planned after 0.1:**
  - parsing values from strings (V8);
  - consumer-side event filters (E6);
  - multiplexed event streams (E14);
  - auto-publish by polling getters (P10);
  - persisting a provider's subscribers across restarts (P17);
  - dynamic table sync and hidden rows (PT6, PT7);
  - OpenTelemetry trace propagation (X2).

## Running the tests

```sh
cargo test --workspace --all-features    # unit tests and doctests; interop tests are skipped
scripts/interop.sh                       # everything, against a live rtrouted in a container
scripts/interop.sh coverage              # the same, under cargo-llvm-cov
```

Interop tests need Linux and the real broker, so `scripts/interop.sh` runs them in a container built from `third_party/rbus` (`git submodule update --init`). Set `CONTAINER_ENGINE=podman` to use Podman instead of Docker. Tests that restart the broker run last, on their own.

## License

Apache-2.0. See [LICENSE](LICENSE).
