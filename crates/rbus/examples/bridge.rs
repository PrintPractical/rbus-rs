//! One connection that consumes and provides (design.md 4.11): it watches
//! another component's uptime and publishes its own event when that
//! component restarts.
//!
//! ```sh
//! cargo run --example bridge -- Device.DeviceInfo.UpTime
//! rbuscli sub Device.Bridge.Rebooted!
//! ```

use rbus::Rbus;
use rbus::provider::{Event, Prop};
use serde::Serialize;

#[derive(rbus::Model, Default)]
#[rbus(path = "Device.Bridge.")]
struct Bridge {
    /// `Device.Bridge.LastUptime`
    last_uptime: Prop<u32>,
    /// `Device.Bridge.Rebooted!`
    rebooted: Event<Rebooted>,
}

#[derive(Serialize)]
struct Rebooted {
    uptime_before: u32,
}

#[tokio::main]
async fn main() -> rbus::Result<()> {
    let source = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Device.DeviceInfo.UpTime".into());
    let bus = Rbus::connect("com.example.bridge").await?;
    let bridge = bus.provide(Bridge::default()).await?;

    let mut uptime = bus.subscribe_value::<u32>(&source).initial_value().await?;
    while let Some(item) = uptime.next().await {
        // Skip `Lagged` and values that didn't convert.
        let Ok(change) = item else { continue };
        let last = bridge.last_uptime.get();
        if change.new < last {
            bridge.rebooted.publish(Rebooted {
                uptime_before: last,
            })?;
        }
        bridge.last_uptime.set(change.new);
    }
    Ok(())
}
