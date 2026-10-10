//! The conformance provider: the `Device.TestProvider.*` data model of
//! rbus's own `rbusTestProvider.c`, written with the macros, so the C
//! `rbusTestConsumer` groups can run against a Rust provider. See
//! `README.md` next to this file for what it covers.
//!
//! - [`Conformance`] (`#[derive(Model)]`) holds the stored values, the
//!   tables and the events.
//! - [`Computed`] (`#[rbus::provider]`) serves the values that change on
//!   every read, the write-only switches and the top-level methods.
//! - [`Table1Methods`], [`Table2Methods`] and [`EventsTableProps`] serve the
//!   methods and computed values inside table rows.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rbus::provider::{Context, Event, Fault, Prop, Provided, SubscriberInfo, Table};
use rbus::{DateTime, Object, Property, Rbus, Status, Value};

const P: &str = "Device.TestProvider";

fn name(suffix: &str) -> String {
    format!("{P}.{suffix}")
}

/// The `Value.*` properties, with the first values `testValueHelper.c`
/// builds. `rbusTestConsumer --Value` compares against its own copy, quirks
/// included: `UINT16` holds a `UINT16` although its table says INT16,
/// `DATETIME` is `localtime(500)` (all three times land in slot 0), and
/// "object 1" holds the properties built for "object 2". Values of any type
/// can be set, as in C.
#[derive(rbus::Model)]
pub struct Values {
    #[rbus(name = "BOOLEAN", writable)]
    boolean: Prop<Value>,
    #[rbus(name = "CHAR", writable)]
    char: Prop<Value>,
    #[rbus(name = "BYTE", writable)]
    byte: Prop<Value>,
    #[rbus(name = "INT16", writable)]
    int16: Prop<Value>,
    #[rbus(name = "UINT16", writable)]
    uint16: Prop<Value>,
    #[rbus(name = "INT32", writable)]
    int32: Prop<Value>,
    #[rbus(name = "UINT32", writable)]
    uint32: Prop<Value>,
    #[rbus(name = "INT64", writable)]
    int64: Prop<Value>,
    #[rbus(name = "UINT64", writable)]
    uint64: Prop<Value>,
    #[rbus(name = "SINGLE", writable)]
    single: Prop<Value>,
    #[rbus(name = "DOUBLE", writable)]
    double: Prop<Value>,
    #[rbus(name = "DATETIME", writable)]
    datetime: Prop<Value>,
    #[rbus(name = "STRING", writable)]
    string: Prop<Value>,
    #[rbus(name = "BYTES", writable)]
    bytes: Prop<Value>,
    #[rbus(name = "PROPERTY", writable)]
    property: Prop<Value>,
    #[rbus(name = "OBJECT", writable)]
    object: Prop<Value>,
}

impl Default for Values {
    fn default() -> Self {
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
        Values {
            boolean: Prop::new(Value::Bool(true)),
            char: Prop::new(Value::Char(b'a')),
            byte: Prop::new(Value::Byte(0x0a)),
            int16: Prop::new(Value::I16(-10000)),
            uint16: Prop::new(Value::U16(10000)),
            int32: Prop::new(Value::I32(-10000000)),
            uint32: Prop::new(Value::U32(10000000)),
            int64: Prop::new(Value::I64(-12345678900001)),
            uint64: Prop::new(Value::U64(98765432100023)),
            single: Prop::new(Value::F32(-0.000001)),
            double: Prop::new(Value::F64(-0.000000000000001)),
            datetime: Prop::new(Value::DateTime(localtime_500)),
            string: Prop::new(Value::from("short string")),
            bytes: Prop::new(Value::Bytes(Bytes::from_static(b"short string"))),
            property: Prop::new(Value::Properties(vec![Property::new(
                "property 1",
                Value::from("property 1 value 1"),
            )])),
            object: Prop::new(Value::Object(object1)),
        }
    }
}

/// `Table1.{i}.`: consumers add rows, three tables deep; each row starts
/// with an empty `data` (`createPropertyNode`).
#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct Table1Row {
    #[rbus(name = "Table2", add_row, remove_row)]
    table2: Table<Table2Row>,
    #[rbus(name = "data", writable)]
    data: String,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct Table2Row {
    #[rbus(name = "Table3", add_row, remove_row)]
    table3: Table<Table3Row>,
    #[rbus(name = "data", writable)]
    data: String,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct Table3Row {
    #[rbus(name = "data", writable)]
    data: String,
}

/// `TableReg.{i}.TableReg.{i}.`: only the provider adds rows
/// (`tableRegAddRowHandler` refuses consumers'); the main loop adds them
/// once they are subscribed to.
#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct TableRegRow {
    #[rbus(name = "TableReg", add_row = TableRegRow::refuse, remove_row)]
    table_reg: Table<TableRegInner>,
}

impl TableRegRow {
    async fn refuse(&self, _cx: &Context, _alias: Option<&str>) -> Result<TableRegInner, Fault> {
        Err(Fault::new(Status::InvalidInput))
    }
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct TableRegInner {}

/// `PartialPath1.{i}.` and `PartialPath2.{i}.`: every parameter holds its
/// own name, so the consumer can check each value (`ppParamGetHandler`).
#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct PartialPathRow {
    param1: String,
    param2: String,
    #[rbus(object)]
    sub_object1: SubObject1,
    #[rbus(add_row, remove_row)]
    sub_table: Table<SubTableRow>,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct SubObject1 {
    param3: String,
    param4: String,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct SubTableRow {
    param5: String,
    #[rbus(object)]
    sub_object2: SubObject2,
}

#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct SubObject2 {
    param6: String,
    param7: String,
}

impl PartialPathRow {
    fn named(row: &str) -> Self {
        PartialPathRow {
            param1: format!("{row}Param1"),
            param2: format!("{row}Param2"),
            sub_object1: SubObject1 {
                param3: format!("{row}SubObject1.Param3"),
                param4: format!("{row}SubObject1.Param4"),
            },
            sub_table: Table::new(),
        }
    }
}

impl SubTableRow {
    fn named(row: &str) -> Self {
        SubTableRow {
            param5: format!("{row}Param5"),
            sub_object2: SubObject2 {
                param6: format!("{row}SubObject2.Param6"),
                param7: format!("{row}SubObject2.Param7"),
            },
        }
    }
}

/// `EventsTable.{i}.`: each row has an event, published by the main loop,
/// and a read counter (`EventsTableProps`).
#[derive(rbus::Row, Clone, PartialEq, Default)]
pub struct EventsRow {
    #[rbus(name = "Event")]
    event: Event<Object>,
}

/// The stored part of `Device.TestProvider.`, in `rbusTestProvider.c`'s
/// order.
#[derive(rbus::Model)]
#[rbus(path = "Device.TestProvider.", on_subscribe = Conformance::on_subscribe)]
pub struct Conformance {
    value: Values,
    #[rbus(name = "Event1!")]
    event1: Event<Object>,
    #[rbus(name = "Event2!")]
    event2: Event<Object>,
    #[rbus(name = "ErrorSubHandlerEvent!")]
    error_sub_handler_event: Event<Object>,
    #[rbus(name = "Table1", add_row, remove_row)]
    table1: Table<Table1Row>,
    #[rbus(name = "TableReg", add_row = Conformance::refuse, remove_row)]
    table_reg: Table<TableRegRow>,
    #[rbus(add_row, remove_row)]
    partial_path1: Table<PartialPathRow>,
    #[rbus(add_row, remove_row)]
    partial_path2: Table<PartialPathRow>,
    #[rbus(writable)]
    big_string: Prop<Value>,
    #[rbus(writable)]
    big_bytes: Prop<Value>,
    #[rbus(add_row, remove_row)]
    events_table: Table<EventsRow>,
    #[rbus(writable, validate = boolean)]
    test_running: Prop<Value>,
}

/// `setRunningParamHandler` only takes a boolean.
fn boolean(v: &Value) -> Result<(), Fault> {
    match v {
        Value::Bool(_) => Ok(()),
        _ => Err(Fault::new(Status::InvalidInput)),
    }
}

impl Conformance {
    fn new() -> Self {
        // The 999 printable characters `rbusTestProvider` starts its big
        // values with.
        let text: String = (0..999).map(|i| char::from(32 + (i % 96) as u8)).collect();
        let mut bytes = text.clone().into_bytes();
        bytes.push(0);
        Conformance {
            value: Values::default(),
            event1: Event::new(),
            event2: Event::new(),
            error_sub_handler_event: Event::new(),
            table1: Table::new(),
            table_reg: Table::new(),
            partial_path1: Table::new(),
            partial_path2: Table::new(),
            big_string: Prop::new(Value::String(text)),
            big_bytes: Prop::new(Value::Bytes(bytes.into())),
            events_table: Table::new(),
            test_running: Prop::new(Value::Bool(false)),
        }
    }

    /// `tableRegAddRowHandler` refuses; only the provider adds those rows.
    async fn refuse(&self, _cx: &Context, _alias: Option<&str>) -> Result<TableRegRow, Fault> {
        Err(Fault::new(Status::InvalidInput))
    }

    /// `provideErrorSubHandler` refuses every subscription;
    /// `tableRegSubHandler` and the others accept.
    async fn on_subscribe(&self, _cx: &Context, sub: &SubscriberInfo) -> Result<(), Fault> {
        if sub.event == name("ErrorSubHandlerEvent!") {
            return Err(Fault::access_denied());
        }
        Ok(())
    }

    /// The rows `rbusTestProvider` registers at start-up: two rows in each
    /// `PartialPath` table, then one and three `SubTable` rows.
    fn add_partial_path_rows(&self) -> rbus::Result<()> {
        for (pp, table) in [
            ("PartialPath1", &self.partial_path1),
            ("PartialPath2", &self.partial_path2),
        ] {
            for (inst, sub_rows) in [(1, 1), (2, 3)] {
                let row = PartialPathRow::named(&name(&format!("{pp}.{inst}.")));
                for sub in 1..=sub_rows {
                    row.sub_table.insert(SubTableRow::named(&name(&format!(
                        "{pp}.{inst}.SubTable.{sub}."
                    ))))?;
                }
                assert_eq!(table.insert(row)?, inst);
            }
        }
        Ok(())
    }
}

/// The read counters behind the value-change properties, which fake a
/// change every so many reads, as `rbusTestProvider`'s getters do.
#[derive(Default)]
struct Counters {
    vc: u32,
    /// `VCParamInt{i}` / `VCParamStr{i}`: reads, current value, step.
    int: [(i32, i32, i32); 6],
    str: [(i32, i32, i32); 6],
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

/// The values of `Device.TestProvider.` that `rbusTestProvider` computes on
/// every read, its write-only switches and its top-level methods. C polls
/// every subscribed property every 2 s; here the properties whose values
/// change without a set ask for it.
#[derive(Default)]
pub struct Computed {
    counters: Mutex<Counters>,
    by_value: AtomicI32,
    /// Set by `TestProviderNotFound`; the main loop counts the seconds since.
    provider_not_found: AtomicU32,
}

impl Computed {
    fn int(&self, i: usize) -> i32 {
        vc_step(&mut self.counters.lock().unwrap().int[i])
    }

    fn str(&self, i: usize) -> String {
        const STRS: [&str; 6] = ["aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"];
        STRS[vc_step(&mut self.counters.lock().unwrap().str[i]) as usize].to_owned()
    }
}

/// `{"name": method}`, the output of every test method.
fn reply(method: &str) -> Object {
    Object::new().with("name", method)
}

/// What the async test methods put in `value`.
fn async_value(args: &Object) -> Value {
    let param = match args.get("param1") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::I32(i)) => i.to_string(),
        other => format!("{other:?}"),
    };
    Value::String(format!("Async Method Response inParams={param}\n"))
}

#[rbus::provider(path = "Device.TestProvider.")]
impl Computed {
    /// `getVCHandler`: "value N", with N going up every second read.
    #[rbus(get = "VCParam", poll = "2s")]
    fn vc_param(&self, _cx: &Context) -> Result<String, Fault> {
        let mut c = self.counters.lock().unwrap();
        c.vc += 1;
        Ok(format!("value {}", (c.vc - 1) / 2))
    }

    #[rbus(get = "VCParamInt0", poll = "2s")]
    fn vc_int0(&self, _cx: &Context) -> Result<i32, Fault> {
        Ok(self.int(0))
    }
    #[rbus(get = "VCParamInt1", poll = "2s")]
    fn vc_int1(&self, _cx: &Context) -> Result<i32, Fault> {
        Ok(self.int(1))
    }
    #[rbus(get = "VCParamInt2", poll = "2s")]
    fn vc_int2(&self, _cx: &Context) -> Result<i32, Fault> {
        Ok(self.int(2))
    }
    #[rbus(get = "VCParamInt3", poll = "2s")]
    fn vc_int3(&self, _cx: &Context) -> Result<i32, Fault> {
        Ok(self.int(3))
    }
    #[rbus(get = "VCParamInt4", poll = "2s")]
    fn vc_int4(&self, _cx: &Context) -> Result<i32, Fault> {
        Ok(self.int(4))
    }
    #[rbus(get = "VCParamInt5", poll = "2s")]
    fn vc_int5(&self, _cx: &Context) -> Result<i32, Fault> {
        Ok(self.int(5))
    }
    #[rbus(get = "VCParamStr0", poll = "2s")]
    fn vc_str0(&self, _cx: &Context) -> Result<String, Fault> {
        Ok(self.str(0))
    }
    #[rbus(get = "VCParamStr1", poll = "2s")]
    fn vc_str1(&self, _cx: &Context) -> Result<String, Fault> {
        Ok(self.str(1))
    }
    #[rbus(get = "VCParamStr2", poll = "2s")]
    fn vc_str2(&self, _cx: &Context) -> Result<String, Fault> {
        Ok(self.str(2))
    }
    #[rbus(get = "VCParamStr3", poll = "2s")]
    fn vc_str3(&self, _cx: &Context) -> Result<String, Fault> {
        Ok(self.str(3))
    }
    #[rbus(get = "VCParamStr4", poll = "2s")]
    fn vc_str4(&self, _cx: &Context) -> Result<String, Fault> {
        Ok(self.str(4))
    }
    #[rbus(get = "VCParamStr5", poll = "2s")]
    fn vc_str5(&self, _cx: &Context) -> Result<String, Fault> {
        Ok(self.str(5))
    }

    #[rbus(get = "VCParamBy", poll = "2s")]
    fn vc_param_by(&self, _cx: &Context) -> Result<i32, Fault> {
        Ok(self.by_value.load(Ordering::Relaxed))
    }
    #[rbus(set = "VCParamBy")]
    fn set_vc_param_by(&self, _cx: &Context, v: i32) -> Result<(), Fault> {
        self.by_value.store(v, Ordering::Relaxed);
        Ok(())
    }

    /// No getter in the C provider; the main loop publishes its changes.
    #[rbus(get = "NoAutoPubInt")]
    fn no_auto_pub_int(&self, _cx: &Context) -> Result<i32, Fault> {
        Err(Fault::not_allowed())
    }

    /// The C provider resets its own row data; the engine's rows stay, as
    /// the library's do.
    #[rbus(set = "ResetTables")]
    fn reset_tables(&self, _cx: &Context, _v: Value) -> Result<(), Fault> {
        Ok(())
    }

    /// The main loop registers the late event 20 s from now.
    #[rbus(set = "TestProviderNotFound")]
    fn set_provider_not_found(&self, _cx: &Context, _v: Value) -> Result<(), Fault> {
        self.provider_not_found.store(1, Ordering::Relaxed);
        Ok(())
    }

    #[rbus(method = "Method11()")]
    fn method11(&self, _cx: &Context) -> Result<Object, Fault> {
        Err(Fault::new(Status::InvalidOperation))
    }

    #[rbus(method = "Method3()")]
    fn method3(&self, _cx: &Context) -> Result<Object, Fault> {
        Err(Fault::new(Status::InvalidOperation).with_detail(
            reply("Method3()")
                .with("error_code", Value::I32(Status::InvalidOperation.code()))
                .with("error_string", "RBUS_ERROR_INVALID_OPERATION"),
        ))
    }

    /// The C provider blocks its callback thread for 4 s.
    #[rbus(method = "MethodAsync1()")]
    async fn method_async1(&self, _cx: &Context) -> Result<Object, Fault> {
        tokio::time::sleep(Duration::from_secs(4)).await;
        Ok(reply("MethodAsync1()"))
    }

    /// The C provider answers from a thread 3 s later
    /// (`rbusMethod_SendAsyncResponse`); here the handler awaits.
    #[rbus(method = "MethodAsync3()")]
    async fn method_async3(&self, _cx: &Context, args: Object) -> Result<Object, Fault> {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Err(Fault::new(Status::InvalidInput)
            .with_detail(reply("MethodAsync3()").with("value", async_value(&args))))
    }
}

/// The methods of `Table1.{i}.`.
pub struct Table1Methods;

#[rbus::provider(path = "Device.TestProvider.Table1.{i}.")]
impl Table1Methods {
    #[rbus(method = "Method1()")]
    fn method1(&self, _cx: &Context) -> Result<Object, Fault> {
        Ok(reply("Method1()"))
    }

    #[rbus(method = "MethodAsync2()")]
    async fn method_async2(&self, _cx: &Context, args: Object) -> Result<Object, Fault> {
        // `rbusTestConsumer --Methods` makes five of these calls at once,
        // with `param1` = 0..4, and counts passes with a non-atomic
        // `gCountPass++` from each reply's callback thread (`test_macros.h`).
        // Replies that land together can lose a count. The C provider has
        // the same race. Staggering the replies by 100 ms keeps the
        // callbacks apart; the five calls still run at the same time.
        let stagger = match args.get("param1") {
            Some(Value::I32(i)) => Duration::from_millis(100 * u64::from(i.unsigned_abs())),
            _ => Duration::ZERO,
        };
        tokio::time::sleep(Duration::from_secs(3) + stagger).await;
        Ok(reply("MethodAsync2()").with("value", async_value(&args)))
    }
}

/// The methods of `Table1.{i}.Table2.{i}.`.
pub struct Table2Methods;

#[rbus::provider(path = "Device.TestProvider.Table1.{i}.Table2.{i}.")]
impl Table2Methods {
    #[rbus(method = "Method2()")]
    fn method2(&self, _cx: &Context) -> Result<Object, Fault> {
        Ok(reply("Method2()"))
    }
}

/// `EventsTable.{i}.Prop`: how often each row's value was read.
#[derive(Default)]
pub struct EventsTableProps {
    reads: Mutex<HashMap<u32, i32>>,
}

#[rbus::provider(path = "Device.TestProvider.EventsTable.{i}.")]
impl EventsTableProps {
    #[rbus(get = "Prop", poll = "2s")]
    fn prop(&self, cx: &Context) -> Result<i32, Fault> {
        let [row] = cx.instances()?;
        let mut reads = self.reads.lock().unwrap();
        let n = reads.entry(row).or_default();
        *n += 1;
        Ok(*n)
    }
}

/// The provider `rbusTestProvider` registers 20 s after
/// `TestProviderNotFound` is set: one event.
#[derive(rbus::Model, Default)]
#[rbus(path = "Device.TestProvider.")]
struct LateEvent {
    #[rbus(name = "ProviderNotFoundEvent1!")]
    provider_not_found_event1: Event<Object>,
}

/// A running conformance provider: the registrations plus the task that
/// does what `rbusTestProvider`'s `main` loop does every second. Dropping it
/// stops all of them.
pub struct Running {
    pub provided: Arc<Provided<Conformance>>,
    computed: Arc<Provided<Computed>>,
    rows: (
        Provided<Table1Methods>,
        Provided<Table2Methods>,
        Provided<EventsTableProps>,
    ),
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Conformance {
    /// Registers the providers, adds the `PartialPath` rows and starts the
    /// main loop.
    pub async fn start(bus: &Rbus) -> rbus::Result<Running> {
        let provided = Arc::new(bus.provide(Conformance::new()).await?);
        let computed = Arc::new(bus.provide(Computed::default()).await?);
        let rows = (
            bus.provide(Table1Methods).await?,
            bus.provide(Table2Methods).await?,
            bus.provide(EventsTableProps::default()).await?,
        );
        provided.add_partial_path_rows()?;
        let task = tokio::spawn(main_loop(provided.clone(), computed.clone()));
        Ok(Running {
            provided,
            computed,
            rows,
            task,
        })
    }
}

/// What `rbusTestProvider`'s `main` does once a second while it runs.
async fn main_loop(model: Arc<Provided<Conformance>>, computed: Arc<Provided<Computed>>) {
    let mut event_counts = [0i32; 2];
    let mut table_reg_done = [false; 2];
    let mut late: Option<Provided<LateEvent>> = None;
    let (mut no_auto_pub, mut dir) = (3i32, 1i32);
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        for (j, (event, count)) in [&model.event1, &model.event2]
            .into_iter()
            .zip(&mut event_counts)
            .enumerate()
        {
            if !event.subscribers().is_empty() {
                let data = Object::new()
                    .with("buffer", format!("event {j} data {count}"))
                    .with("index", Value::I32(*count));
                let _ = event.publish(data);
                *count += 1;
            }
        }
        let not_found = &computed.provider_not_found;
        if not_found.load(Ordering::Relaxed) != 0 {
            if not_found.fetch_add(1, Ordering::Relaxed) == 20 && late.is_none() {
                late = model.bus().provide(LateEvent::default()).await.ok();
            }
            if let Some(late) = &late {
                let event = &late.provider_not_found_event1;
                if !event.subscribers().is_empty() {
                    let _ = event.publish(Object::new().with("buffer", "provider now up!"));
                }
            }
        }
        // The `TableReg` rows appear once their tables are subscribed to.
        if !table_reg_done[0] && !model.table_reg.subscribers().is_empty() {
            table_reg_done[0] = true;
            for _ in 0..2 {
                let _ = model.table_reg.insert(TableRegRow::default());
            }
        }
        if !table_reg_done[1] {
            if let Some(inner) = model.table_reg.with(1, |r| r.table_reg.clone()) {
                if !inner.subscribers().is_empty() {
                    table_reg_done[1] = true;
                    for _ in 0..2 {
                        let _ = inner.insert(TableRegInner {});
                    }
                }
            }
        }
        for (id, row) in model.events_table.rows() {
            if !row.event.subscribers().is_empty() {
                let data =
                    Object::new().with("data", format!("tableEvent data instNum={}", id.inst));
                let _ = row.event.publish(data);
            }
        }
        // `NoAutoPubInt` has no getter: the loop publishes its changes,
        // cycling between 3 and 7.
        let property = name("NoAutoPubInt");
        if !computed.subscribers(Some(&property)).is_empty() {
            let old = no_auto_pub;
            no_auto_pub += dir;
            if no_auto_pub == 3 {
                dir = 1;
            } else if no_auto_pub == 7 {
                dir = -1;
            }
            let _ = computed.publish_value_change(&property, old, no_auto_pub);
        }
    }
}
