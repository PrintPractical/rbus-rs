//! A hand-written provider for the M4 tests: values, a table consumers may
//! add rows to, and methods, under a root of the test's choosing so tests
//! running at once don't collide.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use rbus::provider::{
    Access, Context, Fault, Provided, Provider, Rows, Schema, SetFault, Transaction,
};
use rbus::{MethodName, Object, PropertyName, Rbus, RowId, Status, TablePath, Value};
use serde::{Deserialize, Serialize};

/// `Device.<Root>.`: `Enable`, `Name`, `Uptime` (read-only, counts up on
/// every read, polled every second),
/// `Secret` (write-only), `Slow` (read-only, takes 300 ms), `Caller`
/// (read-only, the requesting component), `Radio.{i}.` (consumers may add
/// and remove rows) with `Channel` and `Status`, the methods `Echo()`,
/// `Fail()` and `Radio.{i}.Reset()`, and the event `Alarm!`.
pub struct Sample {
    root: String,
    values: Mutex<HashMap<String, Value>>,
    uptime: AtomicU32,
    next_radio: AtomicU32,
    in_flight: AtomicUsize,
    /// The most `Slow` gets running at once.
    pub max_in_flight: AtomicUsize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ResetArgs {
    pub delay: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ResetReply {
    pub radio: u32,
    pub delay: u32,
}

impl Sample {
    pub fn new(root: &str) -> Self {
        let values = [("Enable", Value::Bool(true)), ("Name", Value::from("rust"))]
            .into_iter()
            .map(|(k, v)| (format!("Device.{root}.{k}"), v))
            .collect();
        Sample {
            root: format!("Device.{root}"),
            values: Mutex::new(values),
            uptime: AtomicU32::new(0),
            next_radio: AtomicU32::new(1),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        }
    }

    /// Registers a sample under `Device.<root>.` with radio rows 1 and 2
    /// (alias `guest`).
    pub async fn start(bus: &Rbus, root: &str) -> Provided<Sample> {
        let provided = bus.provide(Sample::new(root)).await.unwrap();
        let radios = format!("Device.{root}.Radio.");
        for alias in [None, Some("guest")] {
            let inst = provided.next_radio.fetch_add(1, Ordering::Relaxed);
            provided.set_radio(inst);
            let alias = alias.map(str::to_owned);
            provided
                .add_row(radios.as_str(), RowId { inst, alias })
                .unwrap();
        }
        provided
    }

    fn name(&self, leaf: &str) -> String {
        format!("{}.{leaf}", self.root)
    }

    fn set_radio(&self, inst: u32) {
        let mut values = self.values.lock().unwrap();
        let radio = format!("{}.Radio.{inst}", self.root);
        values.insert(format!("{radio}.Channel"), Value::U32(inst));
        values.insert(format!("{radio}.Status"), Value::from("Up"));
    }

    fn leaf<'a>(&self, name: &'a str) -> &'a str {
        name.strip_prefix(self.root.as_str())
            .and_then(|n| n.strip_prefix('.'))
            .unwrap_or(name)
    }
}

impl Provider for Sample {
    fn schema(&self) -> Schema {
        Schema::new()
            .property(self.name("Enable"), Access::ReadWrite)
            .property(self.name("Name"), Access::ReadWrite)
            .property(self.name("Uptime"), Access::ReadOnly)
            .poll(Duration::from_secs(1))
            .property(self.name("Secret"), Access::WriteOnly)
            .property(self.name("Slow"), Access::ReadOnly)
            .property(self.name("Caller"), Access::ReadOnly)
            .table(self.name("Radio."), Rows::Dynamic)
            .property(self.name("Radio.{i}.Channel"), Access::ReadWrite)
            .property(self.name("Radio.{i}.Status"), Access::ReadOnly)
            .method(self.name("Radio.{i}.Reset()"))
            .method(self.name("Echo()"))
            .method(self.name("Fail()"))
            .event(self.name("Alarm!"))
    }

    async fn get(&self, cx: &Context, name: &PropertyName) -> Result<Value, Fault> {
        match self.leaf(name) {
            "Uptime" => Ok(Value::U32(self.uptime.fetch_add(1, Ordering::Relaxed))),
            "Caller" => Ok(Value::from(cx.caller().unwrap_or_default())),
            "Slow" => {
                let n = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight.fetch_max(n, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(300)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(Value::Bool(true))
            }
            _ => self
                .values
                .lock()
                .unwrap()
                .get(name.as_str())
                .cloned()
                .ok_or_else(Fault::not_found),
        }
    }

    async fn set(&self, _cx: &Context, tx: Transaction) -> Result<(), SetFault> {
        // Check everything first: all or nothing.
        for change in &tx {
            if self.leaf(change.name()).ends_with("Channel") {
                let ch: u32 = change.get()?;
                if !matches!(ch, 1..=13 | 36..=165) {
                    return Err(SetFault::at(change.name(), Fault::invalid_value("channel")));
                }
            }
        }
        let mut values = self.values.lock().unwrap();
        for change in tx {
            values.insert(change.name().to_string(), change.into_value());
        }
        Ok(())
    }

    async fn invoke(
        &self,
        cx: &Context,
        method: &MethodName,
        args: Object,
    ) -> Result<Object, Fault> {
        match self.leaf(method) {
            "Echo()" => Ok(args),
            "Fail()" => Err(Fault::custom(Status::InvalidOperation, "refused")
                .with_detail(Object::new().with("retry_after", Value::U32(5)))),
            _ => {
                let [radio] = cx.instances()?;
                let args: ResetArgs =
                    rbus::from_object(args).map_err(|e| Fault::invalid_input(e.to_string()))?;
                rbus::to_object(&ResetReply {
                    radio,
                    delay: args.delay,
                })
                .map_err(Fault::internal)
            }
        }
    }

    async fn add_row(
        &self,
        _cx: &Context,
        _table: &TablePath,
        _alias: Option<&str>,
    ) -> Result<u32, Fault> {
        let inst = self.next_radio.fetch_add(1, Ordering::Relaxed);
        self.set_radio(inst);
        Ok(inst)
    }

    async fn remove_row(&self, _cx: &Context, table: &TablePath, row: &RowId) -> Result<(), Fault> {
        let prefix = format!("{table}{}.", row.inst);
        self.values
            .lock()
            .unwrap()
            .retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }
}
