//! A provider declared as a struct: rbus serves gets, applies sets, keeps
//! the table's rows and publishes every change (design.md 4.8).
//!
//! ```sh
//! cargo run --example provider_model
//! rbuscli get Device.Example.WiFi.
//! rbuscli setv Device.Example.WiFi.Radio.1.Channel uint32 11
//! rbuscli addrow Device.Example.WiFi.Radio.
//! ```

use std::time::Duration;

use rbus::provider::{Context, Event, Fault, Prop, Table};
use rbus::{Rbus, Retry};
use serde::Serialize;

#[derive(rbus::Model, Default)]
#[rbus(path = "Device.Example.WiFi.")]
struct Wifi {
    /// `Device.Example.WiFi.Enable`, which consumers may set.
    #[rbus(writable)]
    enable: Prop<bool>,
    /// `Device.Example.WiFi.Radio.{i}.`; consumers may add and remove rows.
    #[rbus(name = "Radio", add_row = Wifi::new_radio, remove_row)]
    radios: Table<Radio>,
    /// `Device.Example.WiFi.Alarm!`
    alarm: Event<Alarm>,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
struct Radio {
    #[rbus(writable)]
    enable: bool,
    #[rbus(writable, validate = valid_channel)]
    channel: u32,
    status: String,
}

#[derive(Serialize)]
struct Alarm {
    level: u8,
    text: String,
}

impl Wifi {
    /// A row a consumer adds.
    async fn new_radio(&self, _cx: &Context, _alias: Option<&str>) -> Result<Radio, Fault> {
        Ok(Radio {
            channel: 1,
            status: "Down".into(),
            ..Radio::default()
        })
    }
}

fn valid_channel(ch: &u32) -> Result<(), Fault> {
    matches!(ch, 1..=13 | 36..=165)
        .then_some(())
        .ok_or(Fault::invalid_value("channel"))
}

#[tokio::main]
async fn main() -> rbus::Result<()> {
    let bus = Rbus::builder("com.example.wifi")
        .reconnect(Retry::exponential().max_elapsed(Duration::MAX))
        .connect()
        .await?;
    // Dropping `wifi` unregisters the model.
    let wifi = bus.provide(Wifi::default()).await?;

    wifi.enable.set(true);
    let radio = wifi.radios.insert(Radio {
        enable: true,
        channel: 36,
        status: "Up".into(),
    })?;
    // Runs until the process is stopped; the broker drops its routes then.
    println!("serving Device.Example.WiFi.");

    // The app changes its state; subscribers hear about it.
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    tick.tick().await;
    loop {
        tick.tick().await;
        wifi.radios.update(radio, |r| {
            r.status = if r.status == "Up" { "Dormant" } else { "Up" }.into();
        })?;
        wifi.alarm.publish(Alarm {
            level: 1,
            text: "status changed".into(),
        })?;
    }
}
