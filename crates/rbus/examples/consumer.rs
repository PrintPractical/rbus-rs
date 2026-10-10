//! A consumer: reads a property and the object around it, prints the next
//! ten changes to the property, and closes.
//!
//! ```sh
//! cargo run --example consumer -- Device.SampleProvider.SampleData.IntData
//! ```

use std::time::Duration;

use rbus::{Rbus, Retry, Value};

#[tokio::main]
async fn main() -> rbus::Result<()> {
    let name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Device.DeviceInfo.UpTime".into());

    // Come back after a broker restart instead of closing.
    let bus = Rbus::builder("com.example.consumer")
        .reconnect(Retry::exponential().max_elapsed(Duration::MAX))
        .connect()
        .await?;

    // One property. Any `RbusType` works; `Value` takes whatever comes.
    let value: Value = bus.get(&name).await?;
    println!("{name} = {value:?}");

    // Everything under its parent object, as a tree.
    if let Some((parent, _)) = name.rsplit_once('.') {
        let tree = bus.get_tree(format!("{parent}.")).await?;
        for (name, value) in tree.iter() {
            println!("  {name} = {value:?}");
        }
    }

    // Value changes, as a stream.
    let mut changes = bus.subscribe_value::<Value>(&name).await?;
    println!("watching {name} for ten changes");
    for _ in 0..10 {
        match changes.next().await {
            Some(Ok(c)) => println!("{}: {:?} -> {}", c.name, c.old, c.new),
            Some(Err(e)) => eprintln!("{e}"),
            None => break,
        }
    }
    // Unsubscribes, unregisters and closes the socket.
    bus.close().await
}
