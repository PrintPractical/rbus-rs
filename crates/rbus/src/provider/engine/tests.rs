//! The request handlers against a fake provider, without a socket. What goes
//! over the wire is tested in `engine_tests::provider` and, against real C
//! consumers, in `tests/`.

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tracing::Span;

use super::*;
use crate::provider::schema::Access;

struct Fake {
    values: StdMutex<HashMap<String, Value>>,
    next_row: AtomicU32,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    calls: StdMutex<Vec<String>>,
}

impl Fake {
    fn new() -> Self {
        let values = [
            ("A.Name", Value::String("a".into())),
            ("A.T.1.X", Value::I32(1)),
            ("A.T.2.X", Value::I32(2)),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();
        Fake {
            values: StdMutex::new(values),
            next_row: AtomicU32::new(10),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            calls: StdMutex::new(Vec::new()),
        }
    }

    fn log(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl Provider for Fake {
    fn schema(&self) -> Schema {
        Schema::new()
            .property("A.Name", Access::ReadWrite)
            .property("A.Fail", Access::ReadWrite)
            .property("A.Bad", Access::ReadWrite)
            .property("A.Slow", Access::ReadOnly)
            .property("A.RO", Access::ReadOnly)
            .property("A.WO", Access::WriteOnly)
            .table("A.T.", Rows::Dynamic)
            .property("A.T.{i}.X", Access::ReadWrite)
            .method("A.T.{i}.Do()")
            .table("A.F.", Rows::Fixed)
            .method("A.Ok()")
            .method("A.Fail()")
            .method("A.Plain()")
            .method("A.Coded()")
            .event("A.E!")
    }

    async fn get(&self, cx: &Context, name: &PropertyName) -> Result<Value, Fault> {
        self.log(format!(
            "get {name} {:?} {:?}",
            cx.instances::<1>().ok(),
            cx.caller()
        ));
        match name.as_str() {
            "A.Fail" => Err(Fault::access_denied()),
            "A.Slow" => {
                let n = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight.fetch_max(n, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(Value::Bool(true))
            }
            n => self
                .values
                .lock()
                .unwrap()
                .get(n)
                .cloned()
                .ok_or(Fault::not_found()),
        }
    }

    async fn set(&self, cx: &Context, tx: Transaction) -> Result<(), SetFault> {
        let names: Vec<&str> = tx.iter().map(|c| c.name().as_str()).collect();
        self.log(format!("set {} {:?}", names.join(","), cx.caller()));
        if let Some(c) = tx.get("A.Bad") {
            return Err(SetFault::at(c.name(), Fault::invalid_value("bad")));
        }
        if tx.get("A.Fail").is_some() {
            return Err(Fault::access_denied().into());
        }
        let mut values = self.values.lock().unwrap();
        for c in tx {
            values.insert(c.name().to_string(), c.into_value());
        }
        Ok(())
    }

    async fn invoke(
        &self,
        cx: &Context,
        method: &MethodName,
        args: Object,
    ) -> Result<Object, Fault> {
        match method.as_str() {
            "A.Ok()" => Ok(args.with("inbox", Value::String(cx.inbox().to_owned()))),
            "A.Fail()" => Err(Fault::custom(Status::InvalidInput, "nope")
                .with_detail(Object::new().with("name", Value::String("Fail()".into())))),
            "A.Coded()" => Err(Fault::new(Status::InvalidOperation)
                .with_detail(Object::new().with("error_code", Value::I32(7)))),
            "A.T.2.Do()" => Ok(Object::new().with("row", Value::U32(cx.instances::<1>()?[0]))),
            _ => Err(Fault::invalid_operation()),
        }
    }

    async fn add_row(
        &self,
        cx: &Context,
        table: &TablePath,
        alias: Option<&str>,
    ) -> Result<u32, Fault> {
        self.log(format!(
            "add {table} {alias:?} {:?}",
            cx.instances::<0>().ok()
        ));
        match alias {
            Some("fail") => Err(Fault::invalid_input("no")),
            // A provider bug: hands out a number that is taken.
            Some("reuse") => Ok(1),
            _ => Ok(self.next_row.fetch_add(1, Ordering::Relaxed)),
        }
    }

    async fn remove_row(&self, _cx: &Context, table: &TablePath, row: &RowId) -> Result<(), Fault> {
        self.log(format!("remove {table} {} {:?}", row.inst, row.alias));
        if row.inst == 2 {
            return Err(Fault::access_denied());
        }
        Ok(())
    }
}

/// A provider with nothing but defaults.
struct Bare;

impl Provider for Bare {
    fn schema(&self) -> Schema {
        Schema::new()
            .property("B.P", Access::ReadWrite)
            .method("B.M()")
            .table("B.T.", Rows::Dynamic)
    }

    async fn get(&self, _cx: &Context, _name: &PropertyName) -> Result<Value, Fault> {
        Ok(Value::U8(1))
    }
}

/// An outbox that drops everything.
struct NoOut;

impl Outbox for NoOut {
    fn component(&self) -> &str {
        "test.provider"
    }
    fn send_event(&self, _inbox: &str, _payload: Vec<u8>) {}
    fn watch_disconnects(&self) {}
}

impl Registry {
    async fn handle_test(&self, req: Request, cx: Context) -> Vec<u8> {
        self.handle(req, cx, &NoOut).await
    }
}

fn registry() -> (Registry, Arc<Fake>) {
    let r = Registry::default();
    let fake = Arc::new(Fake::new());
    r.mount(fake.schema(), fake.clone(), false).unwrap();
    r.add_local_row(0, "A.T.", RowId::from(1), &NoOut).unwrap();
    r.add_local_row(
        0,
        "A.T.",
        RowId {
            inst: 2,
            alias: Some("two".into()),
        },
        &NoOut,
    )
    .unwrap();
    (r, fake)
}

fn cx() -> Context {
    Context::traced(Some("caller".into()), "rbus.c.INBOX.1".into(), Span::none())
}

fn decode<B: Body>(payload: Vec<u8>) -> B {
    let bytes = Bytes::from(payload);
    let (mut body, meta) = split_method_payload(&bytes, &rbus_wire::Limits::DEFAULT).unwrap();
    assert_eq!(meta.method, Method::Response);
    B::decode(&mut body).unwrap()
}

async fn get(r: &Registry, names: &[&str]) -> GetResponse {
    let req = GetRequest {
        requesting_component: "caller".into(),
        names: names.iter().map(|n| n.to_string()).collect(),
    };
    decode(r.handle_test(Request::Get(req), cx()).await)
}

fn props(resp: &GetResponse) -> Vec<(&str, &Value)> {
    resp.properties
        .iter()
        .map(|p| (p.name.as_str(), &p.value))
        .collect()
}

fn status(code: i32) -> Status {
    Status::from_wire(code)
}

#[tokio::test]
async fn p1_gets_follow_rbus_c() {
    let (r, fake) = registry();
    let one = get(&r, &["A.Name", "A.T.[two].X"]).await;
    assert_eq!(one.status, 0);
    assert_eq!(
        props(&one),
        [
            ("A.Name", &Value::String("a".into())),
            ("A.T.[two].X", &Value::I32(2))
        ],
        "exact names come back as asked"
    );
    assert!(
        fake.calls()
            .contains(&"get A.T.2.X Some([2]) Some(\"caller\")".to_owned()),
        "the provider sees canonical names and instance numbers: {:?}",
        fake.calls()
    );
    let cases: [(&[&str], Status); 11] = [
        (&["A.Nope"], Status::ElementDoesNotExist),
        (&["A.T.3.X"], Status::ElementDoesNotExist),
        (&["A.WO"], Status::InvalidOperation),
        (&["A.T"], Status::InvalidOperation),
        (&["A.T."], Status::Success),
        (&["A.Fail"], Status::AccessNotAllowed),
        (&["A.Name", "A.Fail"], Status::AccessNotAllowed),
        (&["A.RO"], Status::ElementDoesNotExist),
        (&[], Status::InvalidInput),
        (&["A.*.X.Y"], Status::AccessNotAllowed),
        (&["A.Name."], Status::ElementDoesNotExist),
    ];
    for (names, want) in cases {
        let resp = get(&r, names).await;
        assert_eq!(status(resp.status), want, "{names:?}");
        if want != Status::Success {
            assert!(resp.properties.is_empty());
        }
    }
}

#[tokio::test]
async fn pt8_partial_paths_and_wildcards() {
    let (r, _) = registry();
    let tree = get(&r, &["A."]).await;
    assert_eq!(
        props(&tree),
        [
            ("A.Name", &Value::String("a".into())),
            ("A.Slow", &Value::Bool(true)),
            ("A.T.1.X", &Value::I32(1)),
            ("A.T.2.X", &Value::I32(2)),
        ],
        "failing getters are left out of partial paths"
    );
    let wild = get(&r, &["A.T.*.X"]).await;
    assert_eq!(props(&wild).len(), 2);
    assert_eq!(props(&get(&r, &["A.T.*"]).await).len(), 2);
    assert_eq!(
        props(&get(&r, &["A.T.[two]."]).await),
        [("A.T.2.X", &Value::I32(2))]
    );
    // A wildcard needs every property it names.
    r.add_local_row(0, "A.T.", RowId::from(3), &NoOut).unwrap();
    let missing = get(&r, &["A.T.*.X"]).await;
    assert_eq!(status(missing.status), Status::ElementDoesNotExist);
    assert_eq!(props(&get(&r, &["A.T."]).await).len(), 2);
}

#[tokio::test]
async fn p2_requests_run_concurrently() {
    let (r, fake) = registry();
    let r = Arc::new(r);
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let r = r.clone();
            tokio::spawn(async move { get(&r, &["A.Slow"]).await })
        })
        .collect();
    for t in tasks {
        assert_eq!(t.await.unwrap().status, 0);
    }
    assert!(fake.max_in_flight.load(Ordering::SeqCst) > 1);
}

#[test]
fn serialized_requests_are_found_by_name() {
    let r = Registry::default();
    let fake = Arc::new(Fake::new());
    r.mount(fake.schema(), fake.clone(), true).unwrap();
    let other = Schema::new().property("B.X", Access::ReadOnly);
    r.mount(other, Arc::new(Fake::new()), false).unwrap();
    let names = |n: &[&str]| n.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert!(r.reaches_serialized(&names(&["A.Name"])));
    assert!(r.reaches_serialized(&names(&["B.X", "A.Ok()"])));
    // Partial paths, rows, and subscription templates.
    assert!(r.reaches_serialized(&names(&["A."])));
    assert!(r.reaches_serialized(&names(&["A.T."])));
    assert!(r.reaches_serialized(&names(&["A.T.{i}.X"])));
    assert!(r.reaches_serialized(&names(&["A.E!"])));
    assert!(!r.reaches_serialized(&names(&["B.X"])));
    assert!(!r.reaches_serialized(&names(&["Nobody.Here"])));
    assert!(!r.reaches_serialized(&[]));
}

#[tokio::test]
async fn the_serial_worker_runs_jobs_whole_and_in_order() {
    let r = Registry::default();
    let log = Arc::new(StdMutex::new(Vec::new()));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();
    for i in 0..5u64 {
        let log = log.clone();
        let done = done_tx.clone();
        // Earlier jobs take longer; run concurrently, they'd finish backwards.
        r.run_serially(Box::pin(async move {
            log.lock().unwrap().push(format!("start {i}"));
            tokio::time::sleep(Duration::from_millis(5 * (5 - i))).await;
            log.lock().unwrap().push(format!("end {i}"));
            let _ = done.send(());
        }));
    }
    // A panicking job doesn't stop the worker.
    r.run_serially(Box::pin(async { panic!("handler bug") }));
    let done = done_tx.clone();
    r.run_serially(Box::pin(async move {
        let _ = done.send(());
    }));
    for _ in 0..6 {
        done_rx.recv().await.unwrap();
    }
    let expected: Vec<String> = (0..5)
        .flat_map(|i| [format!("start {i}"), format!("end {i}")])
        .collect();
    assert_eq!(*log.lock().unwrap(), expected);
}

fn set_req(props: &[(&str, Value)], commit: bool, session: u32) -> Request {
    Request::Set(SetRequest {
        session_id: session,
        component: "caller".into(),
        rollback: false,
        properties: props
            .iter()
            .map(|(n, v)| Property::new(*n, v.clone()))
            .collect(),
        commit,
    })
}

async fn set(r: &Registry, props: &[(&str, Value)]) -> SetResponse {
    decode(r.handle_test(set_req(props, true, 0), cx()).await)
}

#[tokio::test]
async fn c7_sets_are_one_transaction_checked_against_the_schema() {
    let (r, fake) = registry();
    let ok = set(
        &r,
        &[("A.Name", Value::from("b")), ("A.T.[two].X", Value::I32(5))],
    )
    .await;
    assert_eq!((ok.status, ok.failed_element), (0, None));
    assert!(
        fake.calls()
            .contains(&"set A.Name,A.T.2.X Some(\"caller\")".to_owned())
    );
    assert_eq!(
        get(&r, &["A.T.2.X"]).await.properties[0].value,
        Value::I32(5)
    );
    type Case<'a> = (&'a [(&'a str, Value)], Status, &'a str);
    let cases: [Case<'_>; 7] = [
        (
            &[("A.Nope", Value::I32(1))],
            Status::ElementDoesNotExist,
            "A.Nope",
        ),
        (
            &[("A.Name", Value::I32(1)), ("A.RO", Value::I32(1))],
            Status::NotWritable,
            "A.RO",
        ),
        (&[("A.Ok()", Value::I32(1))], Status::NotWritable, "A.Ok()"),
        (&[("A.T.", Value::I32(1))], Status::NotWritable, "A.T."),
        (
            &[("A.Name", Value::I32(1)), ("A.Bad", Value::I32(1))],
            Status::InvalidParameterValue,
            "A.Bad",
        ),
        // Without a name, the first parameter is blamed.
        (
            &[("A.Name", Value::I32(1)), ("A.Fail", Value::I32(1))],
            Status::AccessNotAllowed,
            "A.Name",
        ),
        (&[], Status::InvalidInput, "caller"),
    ];
    for (props, want, failed) in cases {
        let resp = set(&r, props).await;
        assert_eq!(status(resp.status), want, "{props:?}");
        assert_eq!(resp.failed_element.as_deref(), Some(failed), "{props:?}");
    }
    // The failed transactions left the earlier value alone.
    assert_eq!(
        get(&r, &["A.Name"]).await.properties[0].value,
        Value::from("b")
    );
}

#[tokio::test]
async fn d8_legacy_sessions_are_buffered_until_commit() {
    let (r, fake) = registry();
    let resp: SetResponse = decode(
        r.handle_test(set_req(&[("A.Name", Value::from("x"))], false, 7), cx())
            .await,
    );
    assert_eq!(resp.status, 0);
    assert!(fake.calls().is_empty(), "nothing applied before the commit");
    // Another session commits on its own.
    let other: SetResponse = decode(
        r.handle_test(set_req(&[("A.T.1.X", Value::I32(9))], true, 8), cx())
            .await,
    );
    assert_eq!(other.status, 0);
    let resp: SetResponse = decode(
        r.handle_test(set_req(&[("A.T.2.X", Value::I32(3))], true, 7), cx())
            .await,
    );
    assert_eq!(resp.status, 0);
    assert_eq!(
        fake.calls(),
        [
            "set A.T.1.X Some(\"caller\")",
            "set A.Name,A.T.2.X Some(\"caller\")"
        ]
    );
}

#[tokio::test]
async fn d8_legacy_sessions_are_per_inbox_bounded_and_dropped_with_the_caller() {
    let r = Registry::new(&Limits::default().with_max_items(2));
    let fake = Arc::new(Fake::new());
    r.mount(fake.schema(), fake.clone(), false).unwrap();
    r.add_local_row(0, "A.T.", RowId::from(1), &NoOut).unwrap();
    let from = |inbox: &str| Context::traced(Some("caller".into()), inbox.into(), Span::none());
    let buffer = |inbox: &'static str, v: &'static str| {
        r.handle_test(
            set_req(&[("A.Name", Value::from(v))], false, 1),
            from(inbox),
        )
    };
    // Two callers that claim the same component name don't share a session.
    assert_eq!(
        decode::<SetResponse>(buffer("rbus.a.INBOX.1", "a").await).status,
        0
    );
    assert_eq!(
        decode::<SetResponse>(buffer("rbus.b.INBOX.2", "b").await).status,
        0
    );
    assert_eq!(
        decode::<SetResponse>(buffer("rbus.a.INBOX.1", "a2").await).status,
        0
    );
    // A third uncommitted value from one caller is refused.
    let full: SetResponse = decode(buffer("rbus.a.INBOX.1", "a3").await);
    assert_eq!(status(full.status), Status::OutOfResources);
    // A caller that disconnects leaves nothing behind.
    r.client_gone("rbus.a.INBOX.1");
    let commit = r.handle_test(set_req(&[], true, 1), from("rbus.a.INBOX.1"));
    assert_eq!(
        status(decode::<SetResponse>(commit.await).status),
        Status::InvalidInput
    );
    assert!(fake.calls().is_empty());
    let commit = r.handle_test(
        set_req(&[("A.T.1.X", Value::I32(1))], true, 1),
        from("rbus.b.INBOX.2"),
    );
    assert_eq!(decode::<SetResponse>(commit.await).status, 0);
    assert_eq!(fake.calls(), ["set A.Name,A.T.1.X Some(\"caller\")"]);
}

#[tokio::test]
async fn sets_spanning_two_providers_go_to_each() {
    let (r, fake) = registry();
    r.mount(Bare.schema(), Arc::new(Bare), false).unwrap();
    let resp = set(&r, &[("A.Name", Value::from("q")), ("B.P", Value::U8(2))]).await;
    // Bare has no setter: the default refuses, and its first parameter is named.
    assert_eq!(status(resp.status), Status::NotWritable);
    assert_eq!(resp.failed_element.as_deref(), Some("B.P"));
    assert_eq!(fake.calls().len(), 1, "the first provider was called");
    // Buffered sets of an unmounted provider are dropped.
    let _: SetResponse = decode(
        r.handle_test(set_req(&[("B.P", Value::U8(2))], false, 0), cx())
            .await,
    );
    r.unmount(1);
    let resp = set(&r, &[("A.Name", Value::from("r"))]).await;
    assert_eq!(resp.status, 0);
    assert_eq!(fake.calls().last().unwrap(), "set A.Name Some(\"caller\")");
}

async fn names(r: &Registry, name: &str, depth: i32, mode: NamesMode) -> Vec<u8> {
    let req = NamesRequest {
        name: name.into(),
        depth,
        mode,
    };
    r.handle_test(Request::Names(req), cx()).await
}

#[tokio::test]
async fn pt10_element_and_row_names() {
    let (r, _) = registry();
    let rows: RowNamesResponse = decode(names(&r, "A.T.", -1, NamesMode::Rows).await);
    assert_eq!(rows.status, 0);
    assert_eq!(
        rows.rows,
        [
            RowName {
                instance: 1,
                alias: None
            },
            RowName {
                instance: 2,
                alias: Some("two".into())
            }
        ]
    );
    let not_table: RowNamesResponse = decode(names(&r, "A.Name", -1, NamesMode::Rows).await);
    assert_eq!(status(not_table.status), Status::InvalidInput);
    let missing: RowNamesResponse = decode(names(&r, "A.Q.", -1, NamesMode::Rows).await);
    assert_eq!(status(missing.status), Status::ElementDoesNotExist);
    let elems: ElementNamesResponse = decode(names(&r, "A.T.2.", -1, NamesMode::Elements).await);
    let listed: Vec<(&str, i32, u32)> = elems
        .elements
        .iter()
        .map(|e| (e.name.as_str(), e.element_type, e.access))
        .collect();
    assert_eq!(listed, [("A.T.2.X", 1, 19), ("A.T.2.Do()", 4, 32)]);
    let missing: ElementNamesResponse = decode(names(&r, "Z.", 1, NamesMode::Elements).await);
    assert_eq!(status(missing.status), Status::ElementDoesNotExist);
}

async fn add(r: &Registry, table: &str, alias: Option<&str>) -> AddRowResponse {
    let req = AddRowRequest {
        session_id: 0,
        table: table.into(),
        alias: alias.map(str::to_owned),
    };
    decode(r.handle_test(Request::AddRow(req), cx()).await)
}

async fn remove(r: &Registry, row: &str) -> Status {
    let req = DeleteRowRequest {
        session_id: 0,
        row: row.into(),
    };
    let resp: StatusResponse = decode(r.handle_test(Request::RemoveRow(req), cx()).await);
    status(resp.status)
}

#[tokio::test]
async fn pt2_consumers_add_and_remove_rows() {
    let (r, fake) = registry();
    let added = add(&r, "A.T.", Some("ten")).await;
    assert_eq!((added.status, added.instance), (0, Some(10)));
    assert!(
        fake.calls()
            .contains(&"add A.T. Some(\"ten\") Some([])".to_owned())
    );
    let row = get(&r, &["A.T.[ten].X"]).await;
    assert_eq!(
        status(row.status),
        Status::ElementDoesNotExist,
        "the row exists, but the fake has no value for it"
    );
    let cases = [
        ("A.T.", Some("ten"), Status::ElementNameDuplicate),
        ("A.T.", Some("fail"), Status::InvalidInput),
        ("A.T.", Some("reuse"), Status::ElementNameDuplicate),
        ("A.F.", None, Status::InvalidOperation),
        ("A.Name", None, Status::ElementDoesNotExist),
        ("A.Q.", None, Status::ElementDoesNotExist),
    ];
    for (table, alias, want) in cases {
        let resp = add(&r, table, alias).await;
        assert_eq!(status(resp.status), want, "{table} {alias:?}");
        assert_eq!(resp.instance, Some(0));
    }
    assert_eq!(remove(&r, "A.T.[ten].").await, Status::Success);
    assert!(
        fake.calls()
            .contains(&"remove A.T. 10 Some(\"ten\")".to_owned())
    );
    assert_eq!(remove(&r, "A.T.10").await, Status::ElementDoesNotExist);
    assert_eq!(
        remove(&r, "A.T.2").await,
        Status::AccessNotAllowed,
        "the provider refused"
    );
    assert_eq!(
        remove(&r, "A.T.1").await,
        Status::Success,
        "no trailing dot, as rbusTestConsumer sends it"
    );
    assert_eq!(
        r.rows("A.T.").unwrap(),
        [RowId {
            inst: 2,
            alias: Some("two".into())
        }]
    );
    r.add_local_row(0, "A.F.", RowId::from(1), &NoOut).unwrap();
    assert_eq!(remove(&r, "A.F.1.").await, Status::InvalidOperation);
    // The trait's defaults refuse.
    r.mount(Bare.schema(), Arc::new(Bare), false).unwrap();
    assert_eq!(
        status(add(&r, "B.T.", None).await.status),
        Status::InvalidOperation
    );
    r.add_local_row(1, "B.T.", RowId::from(4), &NoOut).unwrap();
    assert_eq!(remove(&r, "B.T.4.").await, Status::InvalidOperation);
}

#[tokio::test]
async fn pt3_provider_owned_rows() {
    let (r, _) = registry();
    r.mount(Bare.schema(), Arc::new(Bare), false).unwrap();
    assert_eq!(
        r.add_local_row(1, "A.T.", RowId::from(5), &NoOut),
        Err(Status::AccessNotAllowed)
    );
    assert_eq!(
        r.add_local_row(7, "B.T.", RowId::from(5), &NoOut),
        Err(Status::AccessNotAllowed)
    );
    assert_eq!(
        r.add_local_row(0, "A.Nope.", RowId::from(5), &NoOut),
        Err(Status::ElementDoesNotExist)
    );
    assert_eq!(
        r.add_local_row(0, "A.T.", RowId::from(1), &NoOut),
        Err(Status::ElementNameDuplicate)
    );
    assert_eq!(
        r.remove_local_row(1, "A.T.", 1, &NoOut),
        Err(Status::AccessNotAllowed)
    );
    assert_eq!(
        r.remove_local_row(0, "A.T.", 9, &NoOut),
        Err(Status::ElementDoesNotExist)
    );
    assert_eq!(r.remove_local_row(0, "A.T.", 1, &NoOut), Ok(()));
    assert_eq!(r.rows("A.T.").unwrap().len(), 1);
    assert_eq!(r.rows("A.Name"), Err(Status::ElementDoesNotExist));
    r.unmount(0);
    assert_eq!(r.rows("A.T."), Err(Status::ElementDoesNotExist));
}

async fn invoke(r: &Registry, method: &str, input: Option<Object>) -> RpcResponse {
    let req = RpcRequest {
        session_id: 0,
        method: method.into(),
        input,
    };
    decode(r.handle_test(Request::Rpc(req), cx()).await)
}

#[tokio::test]
async fn p5_methods_and_their_errors() {
    let (r, _) = registry();
    let ok = invoke(&r, "A.Ok()", Some(Object::new().with("x", Value::I32(1)))).await;
    assert_eq!(ok.status, 0);
    let out = ok.output.unwrap();
    assert_eq!(out.get("x"), Some(&Value::I32(1)));
    assert_eq!(out.get("inbox"), Some(&Value::from("rbus.c.INBOX.1")));
    let row = invoke(&r, "A.T.[two].Do()", None).await;
    assert_eq!(row.output.unwrap().get("row"), Some(&Value::U32(2)));
    // A failure carries the detail plus error_code and error_string.
    let fail = invoke(&r, "A.Fail()", None).await;
    assert_eq!(status(fail.status), Status::InvalidInput);
    let out = fail.output.unwrap();
    assert_eq!(out.get("name"), Some(&Value::from("Fail()")));
    assert_eq!(out.get("error_code"), Some(&Value::I32(2)));
    assert_eq!(out.get("error_string"), Some(&Value::from("nope")));
    // Without a message, C's text for the status.
    let plain = invoke(&r, "A.Plain()", None).await;
    assert_eq!(
        plain.output.unwrap().get("error_string"),
        Some(&Value::from("invalid operation"))
    );
    // A provider's own error_code is kept as it is.
    let coded = invoke(&r, "A.Coded()", None).await.output.unwrap();
    assert_eq!(
        (coded.get("error_code"), coded.get("error_string")),
        (Some(&Value::I32(7)), None)
    );
    // Unknown names and non-methods are answered as rbus.c answers them.
    let cases = [
        (
            "A.Nope()",
            Status::ElementDoesNotExist,
            "RBUS_ERROR_ELEMENT_DOES_NOT_EXIST",
        ),
        (
            "A.T.3.Do()",
            Status::ElementDoesNotExist,
            "RBUS_ERROR_ELEMENT_DOES_NOT_EXIST",
        ),
        (
            "A.Name",
            Status::InvalidOperation,
            "RBUS_ERROR_INVALID_OPERATION",
        ),
    ];
    for (method, want, text) in cases {
        let resp = invoke(&r, method, None).await;
        assert_eq!(status(resp.status), want, "{method}");
        let out = resp.output.unwrap();
        assert_eq!(out.get("error_code"), Some(&Value::I32(want.code())));
        assert_eq!(out.get("error_string"), Some(&Value::from(text)));
    }
    r.mount(Bare.schema(), Arc::new(Bare), false).unwrap();
    let bare = invoke(&r, "B.M()", None).await;
    assert_eq!(status(bare.status), Status::InvalidMethod);
}

#[test]
fn p19_other_methods_get_a_bare_status() {
    let mut w = rbus_wire::msgpack::Writer::new();
    w.i32(0);
    let bytes = w.into_bytes();
    let status = |m: Method| match Request::decode(&m, &mut Reader::new(&bytes)).unwrap() {
        Request::Status(s) => s,
        _ => panic!("{m:?}"),
    };
    assert_eq!(status(Method::Commit), 0);
    assert_eq!(status(Method::GetParameterAttributes), 0);
    assert_eq!(status(Method::OpenDirectConn), UNSUPPORTED_METHOD);
    assert_eq!(status(Method::CloseDirectConn), UNSUPPORTED_METHOD);
    assert_eq!(status(Method::Other("METHOD_X".into())), UNSUPPORTED_METHOD);
    assert!(Request::decode(&Method::GetParameterValues, &mut Reader::new(&bytes)).is_err());
    assert_eq!(wire_status(Status::Success), Status::BusError.code());
}

#[test]
fn mounts_may_not_overlap() {
    let r = Registry::default();
    r.mount(Bare.schema(), Arc::new(Bare), false).unwrap();
    assert!(matches!(
        r.mount(Bare.schema(), Arc::new(Bare), false),
        Err(SchemaError::Duplicate(_))
    ));
    // The failed mount took nothing; the next one gets a fresh id.
    let id = r.mount(
        Schema::new().property("C.P", Access::ReadOnly),
        Arc::new(Bare),
        false,
    );
    assert_eq!(id, Ok(1));
}
