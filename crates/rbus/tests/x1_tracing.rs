//! X1: every consumer operation runs in a `tracing` span.
//!
//! This lives in its own test binary: tracing caches callsite interest
//! process-wide, so capturing spans while other tests run in parallel threads
//! is unreliable. The "broker" here is a socket that never answers; the calls
//! time out, which is enough to see their spans.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rbus::{Rbus, Timeouts};
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tracing_subscriber::layer::SubscriberExt;

/// Records each new span as `name field=value ...`.
#[derive(Clone, Default)]
struct SpanNames(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanNames {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Collect<'a>(&'a mut String);
        impl tracing::field::Visit for Collect<'_> {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.0.push_str(&format!(" {}={v:?}", f.name()));
            }
        }
        let mut line = attrs.metadata().name().to_owned();
        attrs.record(&mut Collect(&mut line));
        self.0.lock().unwrap().push(line);
    }
}

#[tokio::test]
async fn x1_every_operation_has_a_span() {
    let spans = SpanNames::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(spans.clone()))
        .unwrap();

    let path = std::env::temp_dir().join(format!("rbus-rs-x1-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink).await;
    });

    let bus = Rbus::builder("x1.tracing")
        .socket(&path)
        .timeouts(Timeouts::uniform(Duration::from_millis(20)))
        .connect()
        .await
        .unwrap();
    let _ = bus.get::<u32>("Device.A").await;
    let _ = bus.get_many(["Device.A", "Device.B"]).await;
    let _ = bus.set("Device.A", 1u8).await;
    let _ = bus.set_many().set("Device.A", 1u8).await;
    let _ = bus.invoke::<_, ()>("Device.M()", ()).await;
    let _ = std::fs::remove_file(&path);

    let spans = spans.0.lock().unwrap().clone();
    for want in [
        "rbus.get name=Device.A",
        "rbus.get_many count=2",
        "rbus.set name=Device.A",
        "rbus.set_many count=1",
        "rbus.invoke method=Device.M()",
    ] {
        assert!(
            spans.iter().any(|s| s == want),
            "no span {want:?} in {spans:?}"
        );
    }
}
