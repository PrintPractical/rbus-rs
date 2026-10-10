//! The conformance provider: a hand-written [`Provider`] that serves the
//! `Device.TestProvider.*` data model of rbus's own `rbusTestProvider.c`, so
//! the C `rbusTestConsumer` groups can run against a Rust provider. See
//! `README.md` next to this file for what it covers.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rbus::provider::{
    Access, Context, Fault, Provided, Provider, Rows, Schema, SetFault, SubscriberInfo, Transaction,
};
use rbus::{
    DateTime, MethodName, Object, Property, PropertyName, Rbus, RowId, Status, TablePath, Value,
};

const P: &str = "Device.TestProvider";

/// The `Value.*` properties and their first values, as `testValueHelper.c`
/// builds them. `rbusTestConsumer --Value` compares against its own copy,
/// quirks included: `UINT16` holds a `UINT16` although its table says INT16,
/// `DATETIME` is `localtime(500)` (all three times land in slot 0), and
/// "object 1" holds the properties built for "object 2".
fn test_values() -> Vec<(&'static str, Value)> {
    let localtime_500 = DateTime {
        sec: 20,
        min: 8,
        hour: 0,
        mday: 1,
        mon: 0,
        year: 70,
        wday: 4,
        yday: 0,
        ..DateTime::default()
    };
    let level3 = Object {
        name: "depth 3".into(),
        ..Object::new().with("property_level_3_string", "level 3")
    };
    let level2 = Object {
        name: "depth 2".into(),
        ..Object::new()
            .with("property_level_2_string", "level 2")
            .with("property_level_2_object", Value::Object(level3))
    };
    let object1 = Object {
        name: "object 1".into(),
        ..Object::new()
            .with("property_level_1_string", "level 1")
            .with("property_level_1_object", Value::Object(level2))
    };
    vec![
        ("BOOLEAN", Value::Bool(true)),
        ("CHAR", Value::Char(b'a')),
        ("BYTE", Value::Byte(0x0a)),
        ("INT16", Value::I16(-10000)),
        ("UINT16", Value::U16(10000)),
        ("INT32", Value::I32(-10000000)),
        ("UINT32", Value::U32(10000000)),
        ("INT64", Value::I64(-12345678900001)),
        ("UINT64", Value::U64(98765432100023)),
        ("SINGLE", Value::F32(-0.000001)),
        ("DOUBLE", Value::F64(-0.000000000000001)),
        ("DATETIME", Value::DateTime(localtime_500)),
        ("STRING", Value::from("short string")),
        ("BYTES", Value::Bytes(Bytes::from_static(b"short string"))),
        (
            "PROPERTY",
            Value::Properties(vec![Property::new(
                "property 1",
                Value::from("property 1 value 1"),
            )]),
        ),
        ("OBJECT", Value::Object(object1)),
    ]
}

/// The 999 printable characters `rbusTestProvider` starts its big values with.
fn big_text() -> String {
    (0..999).map(|i| char::from(32 + (i % 96) as u8)).collect()
}

fn name(suffix: &str) -> String {
    format!("{P}.{suffix}")
}

/// The leaf of a canonical name: `Param5` for `Device.X.1.Param5`.
fn leaf(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

/// How often C polls subscribed properties (`RBUS_VALUECHANGE_PERIOD`).
const VC_PERIOD: Duration = Duration::from_secs(2);

/// The read counters behind the value-change properties, which fake a change
/// every so many reads, as `rbusTestProvider`'s getters do.
#[derive(Default)]
struct Counters {
    vc: u32,
    /// `VCParamInt{i}` / `VCParamStr{i}`: reads, current value, step.
    int: [(i32, i32, i32); 6],
    str: [(i32, i32, i32); 6],
    /// `EventsTable.{i}.Prop`: reads.
    events_table: [i32; 10],
}

/// One step of `getVCIntHandler`: every second read moves the value one
/// step between 0 and 5.
fn vc_step(state: &mut (i32, i32, i32)) -> i32 {
    let (count, data, delta) = state;
    if *delta == 0 {
        *delta = 1;
    }
    *count += 1;
    if *count % 2 == 0 {
        *data += *delta;
        if *data == 5 {
            *delta = -1;
        } else if *data == 0 {
            *delta = 1;
        }
    }
    *data
}

/// The digit after `prefix` in `name`: the index of `VCParamInt3`, the row
/// of `EventsTable.2.Prop` (`getEventTableInstNum`).
fn index_after(name: &str, prefix: &str) -> usize {
    name.split(prefix)
        .nth(1)
        .and_then(|rest| rest.chars().next())
        .and_then(|c| c.to_digit(10))
        .unwrap_or(0) as usize
}

/// The conformance provider. Values live in one map by canonical name; table
/// rows are numbered from 1 per table and never reused, as in the C provider.
pub struct Conformance {
    values: Mutex<HashMap<String, Value>>,
    next_inst: Mutex<HashMap<String, u32>>,
    counters: Mutex<Counters>,
    by_value: AtomicI32,
    /// Set by `TestProviderNotFound`; the main loop counts the seconds since.
    provider_not_found: AtomicU32,
}

/// The provider `rbusTestProvider` registers 20 s after
/// `TestProviderNotFound` is set: one event.
struct LateEvent;

impl Provider for LateEvent {
    fn schema(&self) -> Schema {
        Schema::new().event(name("ProviderNotFoundEvent1!"))
    }

    async fn get(&self, _cx: &Context, _name: &PropertyName) -> Result<Value, Fault> {
        Err(Fault::not_allowed())
    }
}

/// A running conformance provider: the registration plus the task that does
/// what `rbusTestProvider`'s `main` loop does every second. Dropping it stops
/// both.
pub struct Running {
    pub provided: Arc<Provided<Conformance>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Conformance {
    pub fn new() -> Self {
        let mut values: HashMap<String, Value> = test_values()
            .into_iter()
            .map(|(k, v)| (name(&format!("Value.{k}")), v))
            .collect();
        let text = big_text();
        let mut bytes = text.clone().into_bytes();
        bytes.push(0);
        values.insert(name("BigString"), Value::String(text));
        values.insert(name("BigBytes"), Value::Bytes(bytes.into()));
        values.insert(name("TestRunning"), Value::Bool(false));
        Conformance {
            values: Mutex::new(values),
            next_inst: Mutex::new(HashMap::new()),
            counters: Mutex::new(Counters::default()),
            by_value: AtomicI32::new(0),
            provider_not_found: AtomicU32::new(0),
        }
    }

    /// Registers the provider, adds the `PartialPath1` and `PartialPath2`
    /// rows `rbusTestProvider` registers at start-up (two rows, then one and
    /// three `SubTable` rows), and starts the main loop.
    pub async fn start(bus: &Rbus) -> rbus::Result<Running> {
        let provided = Arc::new(bus.provide(Conformance::new()).await?);
        for pp in ["PartialPath1", "PartialPath2"] {
            for (table, count) in [
                (format!("{P}.{pp}."), 2),
                (format!("{P}.{pp}.1.SubTable."), 1),
                (format!("{P}.{pp}.2.SubTable."), 3),
            ] {
                for _ in 0..count {
                    let inst = provided.next(&table);
                    provided.add_row(table.as_str(), inst)?;
                }
            }
        }
        let task = tokio::spawn(main_loop(provided.clone()));
        Ok(Running { provided, task })
    }

    fn next(&self, table: &str) -> u32 {
        let mut next = self.next_inst.lock().unwrap();
        let n = next.entry(table.to_owned()).or_insert(1);
        *n += 1;
        *n - 1
    }

    fn is_partial_path_param(name: &str) -> bool {
        name.starts_with(&format!("{P}.PartialPath")) && leaf(name).starts_with("Param")
    }
}

impl Default for Conformance {
    fn default() -> Self {
        Self::new()
    }
}

/// What `rbusTestProvider`'s `main` does once a second while it runs.
async fn main_loop(provided: Arc<Provided<Conformance>>) {
    let subscribed = |event: &str| !provided.subscribers(Some(event)).is_empty();
    let mut event_counts = [0i32; 2];
    let mut table_reg_done = [false; 2];
    let mut late: Option<Provided<LateEvent>> = None;
    let (mut no_auto_pub, mut dir) = (3i32, 1i32);
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for (j, count) in event_counts.iter_mut().enumerate() {
            let event = name(&format!("Event{}!", j + 1));
            if subscribed(&event) {
                let data = Object::new()
                    .with("buffer", format!("event {j} data {count}"))
                    .with("index", Value::I32(*count));
                let _ = provided.publish(&event, &data);
                *count += 1;
            }
        }
        let not_found = &provided.provider_not_found;
        if not_found.load(Ordering::Relaxed) != 0 {
            if not_found.fetch_add(1, Ordering::Relaxed) == 20 && late.is_none() {
                late = provided.bus().provide(LateEvent).await.ok();
            }
            let event = name("ProviderNotFoundEvent1!");
            if let Some(late) = &late {
                if !late.subscribers(Some(&event)).is_empty() {
                    let data = Object::new().with("buffer", "provider now up!");
                    let _ = late.publish(&event, &data);
                }
            }
        }
        for (k, table) in [name("TableReg."), name("TableReg.1.TableReg.")]
            .iter()
            .enumerate()
        {
            if subscribed(table) && !table_reg_done[k] {
                table_reg_done[k] = true;
                for inst in [1, 2] {
                    let _ = provided.add_row(table.as_str(), inst);
                }
            }
        }
        for i in 0..10 {
            let event = name(&format!("EventsTable.{i}.Event"));
            if subscribed(&event) {
                let data = Object::new().with("data", format!("tableEvent data instNum={i}"));
                let _ = provided.publish(&event, &data);
            }
        }
        // `NoAutoPubInt` has no getter: the loop publishes its changes,
        // cycling between 3 and 7.
        let property = name("NoAutoPubInt");
        if subscribed(&property) {
            let old = no_auto_pub;
            no_auto_pub += dir;
            if no_auto_pub == 3 {
                dir = 1;
            } else if no_auto_pub == 7 {
                dir = -1;
            }
            let _ = provided.publish_value_change(&property, old, no_auto_pub);
        }
    }
}

impl Provider for Conformance {
    /// The elements of `rbusTestProvider.c`'s `main`, in its order. C polls
    /// every subscribed property every 2 s; here the properties whose values
    /// change without a set are declared with `poll`.
    fn schema(&self) -> Schema {
        let mut s = Schema::new();
        for (leaf, _) in test_values() {
            s = s.property(name(&format!("Value.{leaf}")), Access::ReadWrite);
        }
        s = s
            .event(name("Event1!"))
            .event(name("Event2!"))
            .event(name("ErrorSubHandlerEvent!"))
            .property(name("VCParam"), Access::ReadOnly)
            .poll(VC_PERIOD);
        for kind in ["Int", "Str"] {
            for i in 0..6 {
                s = s
                    .property(name(&format!("VCParam{kind}{i}")), Access::ReadOnly)
                    .poll(VC_PERIOD);
            }
        }
        s = s
            .property(name("VCParamBy"), Access::ReadWrite)
            .poll(VC_PERIOD)
            .property(name("NoAutoPubInt"), Access::ReadOnly)
            .table(name("Table1.{i}."), Rows::Dynamic)
            .table(name("Table1.{i}.Table2.{i}."), Rows::Dynamic)
            .table(name("Table1.{i}.Table2.{i}.Table3.{i}."), Rows::Dynamic)
            .property(name("Table1.{i}.data"), Access::ReadWrite)
            .property(name("Table1.{i}.Table2.{i}.data"), Access::ReadWrite)
            .property(
                name("Table1.{i}.Table2.{i}.Table3.{i}.data"),
                Access::ReadWrite,
            )
            .table(name("TableReg.{i}."), Rows::Dynamic)
            .table(name("TableReg.{i}.TableReg.{i}."), Rows::Dynamic)
            .property(name("ResetTables"), Access::WriteOnly)
            .method(name("Method11()"))
            .method(name("Method3()"))
            .method(name("Table1.{i}.Method1()"))
            .method(name("Table1.{i}.Table2.{i}.Method2()"))
            .method(name("MethodAsync1()"))
            .method(name("Table1.{i}.MethodAsync2()"))
            .method(name("MethodAsync3()"));
        for pp in ["PartialPath1", "PartialPath2"] {
            s = s
                .table(name(&format!("{pp}.{{i}}.")), Rows::Dynamic)
                .property(name(&format!("{pp}.{{i}}.Param1")), Access::ReadOnly)
                .property(name(&format!("{pp}.{{i}}.Param2")), Access::ReadOnly)
                .property(
                    name(&format!("{pp}.{{i}}.SubObject1.Param3")),
                    Access::ReadOnly,
                )
                .property(
                    name(&format!("{pp}.{{i}}.SubObject1.Param4")),
                    Access::ReadOnly,
                )
                .table(name(&format!("{pp}.{{i}}.SubTable.{{i}}.")), Rows::Dynamic)
                .property(
                    name(&format!("{pp}.{{i}}.SubTable.{{i}}.Param5")),
                    Access::ReadOnly,
                )
                .property(
                    name(&format!("{pp}.{{i}}.SubTable.{{i}}.SubObject2.Param6")),
                    Access::ReadOnly,
                )
                .property(
                    name(&format!("{pp}.{{i}}.SubTable.{{i}}.SubObject2.Param7")),
                    Access::ReadOnly,
                );
        }
        s.property(name("TestProviderNotFound"), Access::WriteOnly)
            .property(name("BigString"), Access::ReadWrite)
            .property(name("BigBytes"), Access::ReadWrite)
            .table(name("EventsTable.{i}."), Rows::Dynamic)
            .property(name("EventsTable.{i}.Prop"), Access::ReadOnly)
            .poll(VC_PERIOD)
            .event(name("EventsTable.{i}.Event"))
            .property(name("TestRunning"), Access::ReadWrite)
    }

    async fn get(&self, _cx: &Context, name: &PropertyName) -> Result<Value, Fault> {
        // The partial-path parameters hold their own names, so the consumer
        // can check each value (`ppParamGetHandler`).
        if Self::is_partial_path_param(name) {
            return Ok(Value::from(name.as_str()));
        }
        let leaf = leaf(name);
        let mut c = self.counters.lock().unwrap();
        match leaf {
            // `getVCHandler`: "value N", with N going up every second read.
            "VCParam" => {
                c.vc += 1;
                return Ok(Value::String(format!("value {}", (c.vc - 1) / 2)));
            }
            "VCParamBy" => return Ok(Value::I32(self.by_value.load(Ordering::Relaxed))),
            // No getter in the C provider.
            "NoAutoPubInt" => return Err(Fault::not_allowed()),
            "Prop" => {
                let i = index_after(name, "EventsTable.");
                c.events_table[i] += 1;
                return Ok(Value::I32(c.events_table[i]));
            }
            _ if leaf.starts_with("VCParamInt") => {
                let i = index_after(leaf, "VCParamInt");
                return Ok(Value::I32(vc_step(&mut c.int[i])));
            }
            _ if leaf.starts_with("VCParamStr") => {
                const STRS: [&str; 6] = ["aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"];
                let i = index_after(leaf, "VCParamStr");
                return Ok(Value::from(STRS[vc_step(&mut c.str[i]) as usize]));
            }
            _ => {}
        }
        drop(c);
        if let Some(v) = self.values.lock().unwrap().get(name.as_str()) {
            return Ok(v.clone());
        }
        // A table row's `data` starts out empty (`createPropertyNode`).
        if leaf == "data" {
            return Ok(Value::from(""));
        }
        Err(Fault::not_found())
    }

    async fn set(&self, _cx: &Context, tx: Transaction) -> Result<(), SetFault> {
        for change in &tx {
            // `setRunningParamHandler` only takes a boolean.
            if leaf(change.name()) == "TestRunning" && !matches!(change.value(), Value::Bool(_)) {
                return Err(SetFault::at(
                    change.name(),
                    Fault::new(Status::InvalidInput),
                ));
            }
        }
        let mut values = self.values.lock().unwrap();
        for change in tx {
            match leaf(change.name()) {
                "VCParamBy" => {
                    let v: i32 = change.get().unwrap_or_default();
                    self.by_value.store(v, Ordering::Relaxed);
                }
                // The main loop registers the late event 20 s from now.
                "TestProviderNotFound" => self.provider_not_found.store(1, Ordering::Relaxed),
                // The C provider resets its own row data; the engine's rows
                // stay, as the library's do.
                "ResetTables" => {}
                _ => {
                    values.insert(change.name().to_string(), change.into_value());
                }
            }
        }
        Ok(())
    }

    /// `methodHandler`. The C provider answers `MethodAsync2()` and
    /// `MethodAsync3()` from a thread 3 s later; here the handler awaits.
    async fn invoke(
        &self,
        _cx: &Context,
        method: &MethodName,
        args: Object,
    ) -> Result<Object, Fault> {
        let reply = |name: &str| Object::new().with("name", name);
        let async_value = || {
            let param = match args.get("param1") {
                Some(Value::String(s)) => s.clone(),
                Some(Value::I32(i)) => i.to_string(),
                other => format!("{other:?}"),
            };
            Value::String(format!("Async Method Response inParams={param}\n"))
        };
        match leaf(method) {
            "Method1()" | "Method2()" => Ok(reply(leaf(method))),
            "Method11()" => Err(Fault::new(Status::InvalidOperation)),
            "Method3()" => Err(Fault::new(Status::InvalidOperation).with_detail(
                reply("Method3()")
                    .with("error_code", Value::I32(Status::InvalidOperation.code()))
                    .with("error_string", "RBUS_ERROR_INVALID_OPERATION"),
            )),
            "MethodAsync1()" => {
                tokio::time::sleep(Duration::from_secs(4)).await;
                Ok(reply("MethodAsync1()"))
            }
            "MethodAsync2()" => {
                // `rbusTestConsumer --Methods` makes five of these calls at
                // once, with `param1` = 0..4, and counts passes with a
                // non-atomic `gCountPass++` from each reply's callback
                // thread (`test_macros.h`). Replies that land together can
                // lose a count. The C provider has the same race. Staggering
                // the replies by 100 ms keeps the callbacks apart; the five
                // calls still run at the same time.
                let stagger = match args.get("param1") {
                    Some(Value::I32(i)) => Duration::from_millis(100 * u64::from(i.unsigned_abs())),
                    _ => Duration::ZERO,
                };
                tokio::time::sleep(Duration::from_secs(3) + stagger).await;
                Ok(reply("MethodAsync2()").with("value", async_value()))
            }
            "MethodAsync3()" => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                Err(Fault::new(Status::InvalidInput)
                    .with_detail(reply("MethodAsync3()").with("value", async_value())))
            }
            _ => Err(Fault::new(Status::BusError)),
        }
    }

    async fn add_row(
        &self,
        _cx: &Context,
        table: &TablePath,
        _alias: Option<&str>,
    ) -> Result<u32, Fault> {
        // `tableRegAddRowHandler` refuses; only the provider adds those rows.
        if table.contains(".TableReg.") {
            return Err(Fault::new(Status::InvalidInput));
        }
        Ok(self.next(table))
    }

    async fn remove_row(&self, _cx: &Context, table: &TablePath, row: &RowId) -> Result<(), Fault> {
        let prefix = format!("{table}{}.", row.inst);
        self.values
            .lock()
            .unwrap()
            .retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }

    /// `provideErrorSubHandler` refuses every subscription;
    /// `tableRegSubHandler` and the others accept.
    async fn on_subscribe(&self, _cx: &Context, sub: &SubscriberInfo) -> Result<(), Fault> {
        if sub.event == name("ErrorSubHandlerEvent!") {
            return Err(Fault::access_denied());
        }
        Ok(())
    }
}
