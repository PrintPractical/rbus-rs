# rbus-rs

An async, idiomatic Rust client and provider for RDK's RBUS.

rbus-rs speaks the `rtrouted` wire protocol directly. There is no C library to link, no FFI and no `unsafe`, yet it interoperates with C consumers and providers in both directions.

- **Consume:** get and set values (a batch of sets is one transaction), read partial paths and wildcards as trees, call methods through serde, manage table rows, discover components, and receive events as typed `Stream`s.
- **Provide:** declare a data model as a struct and rbus serves gets, applies sets, keeps table rows and publishes every change. Impl blocks, a builder and a plain `Provider` trait are there when you need more control.
- **Interop-tested:** CI runs the suite against the real `rtrouted`, `rbuscli` and rbus's own conformance programs.

> **Status:** experimental, pre-1.0. The API may still change. rbus-rs reimplements the protocol rather than linking the C library, so a change to RDK's rbus or `rtrouted` can break compatibility until rbus-rs catches up; see [Compatibility](#compatibility) for the version it is tested against.

> **Built with AI:** rbus-rs was co-developed with Claude. AI wrote a substantial part of the code, tests and documentation. I'm saying so up front so you can weigh that when you decide whether to use it.

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

### Consumer

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

### Providers

There are three ways to write a provider. All three implement the same `rbus::provider::Provider` trait, which you can also implement by hand. A connection can serve several providers at once, in any mix of styles.

**1. A struct of state** (`#[derive(Model)]`). Declare the data model, and rbus serves gets, applies sets, keeps table rows and publishes every change. Most of a TR-181 model is plain data, so this is the usual choice.

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

**2. An impl block** (`#[rbus::provider]`). Annotated functions serve values computed on demand, setters and methods. The path may hold `{i}` templates, and `cx.instances()` says which row a request is for.

```rust,no_run
use rbus::provider::{Context, Fault};
use rbus::Rbus;
use serde::{Deserialize, Serialize};

struct Radios;

#[derive(Deserialize)]
struct ScanArgs {
    seconds: u32,
}

#[derive(Serialize)]
struct ScanReply {
    radio: u32,
    networks: u32,
}

#[rbus::provider(path = "Device.Example.Radio.{i}.")]
impl Radios {
    #[rbus(get)]                       // Device.Example.Radio.{i}.Noise
    async fn noise(&self, cx: &Context) -> Result<i32, Fault> {
        let [radio] = cx.instances()?;
        Ok(-90 - radio as i32)
    }

    #[rbus(set = "Channel")]           // Device.Example.Radio.{i}.Channel
    async fn set_channel(&self, _cx: &Context, channel: u32) -> Result<(), Fault> {
        if !(1..=165).contains(&channel) {
            return Err(Fault::invalid_value("channel"));
        }
        Ok(())
    }

    #[rbus(method)]                    // Device.Example.Radio.{i}.Scan()
    async fn scan(&self, cx: &Context, args: ScanArgs) -> Result<ScanReply, Fault> {
        let [radio] = cx.instances()?;
        Ok(ScanReply { radio, networks: args.seconds * 2 })
    }
}

#[tokio::main]
async fn main() -> rbus::Result<()> {
    let bus = Rbus::connect("com.example.radios").await?;
    let radios = bus.provide(Radios).await?;
    radios.add_row("Device.Example.Radio.", 1)?;          // the rows this provider serves
    std::future::pending::<()>().await;
    Ok(())
}
```

**3. A builder** (`DataModel`). Closures serve names that are known only at run time, such as a model read from a configuration file.

```rust,no_run
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use rbus::provider::DataModel;
use rbus::{Object, Rbus};

#[tokio::main]
async fn main() -> rbus::Result<()> {
    let bus = Rbus::connect("com.example.counter").await?;
    let count = Arc::new(AtomicU32::new(0));
    let model = DataModel::new()
        .property("Device.Counter.Value", {
            let count = count.clone();
            move |_cx| {
                let count = count.clone();
                async move { Ok(count.load(Ordering::Relaxed)) }
            }
        })
        .method("Device.Counter.Increment()", move |_cx, _args: Object| {
            let count = count.clone();
            async move {
                let value = count.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(Object::new().with("value", value))
            }
        });
    let _counter = bus.provide(model).await?;
    std::future::pending::<()>().await;
    Ok(())
}
```

The [examples](https://github.com/PrintPractical/rbus-rs/tree/main/crates/rbus/examples) have more: `consumer.rs`, `provider_model.rs` and `bridge.rs`, a component that consumes and provides on one connection. The rustdoc for `rbus` covers the whole API.

## Compatibility

- **rbus:** v2.14.0, commit `e8cf808`. This is the `third_party/rbus` submodule pin, and CI builds the broker and the C peers from it.
- **Transport:** the broker's unix socket, `/tmp/rtrouted` by default.
- **Platforms:** Linux. The crates also build and pass their unit tests on macOS, though RBUS itself runs on Linux.
- **Rust:** 1.85 or later, with tokio.
- **C peers:** works with C consumers and providers in both directions. It accepts legacy CCSP type ids and error codes, and sends only current rbus ones. Its inbox names end in the process id, as C providers expect.
- **Broker restarts:** opt in to `RbusBuilder::reconnect`, and the connection registers its component, elements and subscriptions again. C clients don't register their elements again.

## Features

Every feature has integration tests against the real broker.

| Area | Supported |
| --- | --- |
| Connection | Unix socket; reconnect after a broker restart, with a `state()` watch; per-operation default timeouts; graceful close, plus best-effort cleanup on drop |
| Values | Every scalar type, plus DateTime (`time` feature), objects and property lists; conversions to native types; serde for values and objects; legacy CCSP values |
| Get and set | One value or many; partial paths and wildcards as a `Tree`, with per-component failures reported; set and batch set as transactions that name the failing parameter |
| Tables and methods | Add, remove and list rows, with aliases; methods with serde arguments and results, and typed errors |
| Discovery | Element owners, component elements, all components, connect and disconnect advisories |
| Events | Typed streams for value changes, table rows and general events; initial value; interval subscriptions; retry until the provider appears; a bounded buffer that reports lag, or applies backpressure |
| Providers | Properties, events and methods with async handlers; caller identity; subscription hooks; value-change events, including from state; provider-side filters; initial values; direct-mode requests rejected |
| Provider tables | Row templates, consumer and app rows, aliases, nested tables; partial-path gets over the model; row events; row names for `rbuscli` |
| Cross-cutting | `tracing` spans per operation; a typed `Error` and `Status`; legacy CCSP codes; both header sizes; tokio; configurable limits on what peers send (`RbusBuilder::limits`) |

## Not supported

- **Out of scope:**
  - direct mode, whose open requests are rejected with an error;
  - TCP and `/etc/rbus_client.conf`;
  - SPAKE2+ and rtCipher encryption;
  - raw-data events;
  - the `rbusMessage_*` API;
  - the session manager API and session ids;
  - duration subscriptions;
  - the element-info browsing API;
  - the C timeout environment variables and override files.
- **Per-call timeouts:** use `tokio::time::timeout`. Dropping a request future cancels it and leaves nothing behind. A `set_many` that spans several providers goes to them one at a time, so cancelling it part-way can leave the earlier ones' sets applied.
- **Planned for later releases:**
  - parsing values from strings;
  - consumer-side event filters;
  - multiplexed event streams;
  - auto-publish by polling getters;
  - providers that publish on an interval (Rust providers refuse interval subscriptions for now);
  - persisting a provider's subscribers across restarts;
  - dynamic table sync and hidden rows;
  - OpenTelemetry trace propagation.

## Running the tests

```sh
cargo test --workspace --all-features    # unit tests and doctests; interop tests are skipped
scripts/interop.sh                       # everything, against a live rtrouted in a container
scripts/interop.sh coverage              # the same, under cargo-llvm-cov
```

Interop tests need Linux and the real broker, so `scripts/interop.sh` runs them in a container built from `third_party/rbus` (`git submodule update --init`). Set `CONTAINER_ENGINE=podman` to use Podman instead of Docker. Tests that restart the broker run last, on their own.

## License

Apache-2.0. See [LICENSE](https://github.com/PrintPractical/rbus-rs/blob/main/LICENSE).
