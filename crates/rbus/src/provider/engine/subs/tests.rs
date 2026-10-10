//! The subscription registry against a fake provider and a recording outbox.

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering as AtomicOrdering};

use bytes::Bytes;
use rbus_wire::event::{Filter, RelationOp};
use rbus_wire::message::split_event_payload;
use rbus_wire::msgpack::Reader;

use super::*;
use crate::provider::schema::{Access, Rows};

/// Records every event the engine sends.
#[derive(Default)]
struct Rec {
    sent: StdMutex<Vec<(String, Event, EventMeta)>>,
    watching: AtomicBool,
}

impl Outbox for Rec {
    fn component(&self) -> &str {
        "test.provider"
    }
    fn send_event(&self, inbox: &str, payload: Vec<u8>) {
        let bytes = Bytes::from(payload);
        let (mut body, meta) = split_event_payload(&bytes, &rbus_wire::Limits::DEFAULT).unwrap();
        let event = Event::decode(&mut body).unwrap();
        self.sent
            .lock()
            .unwrap()
            .push((inbox.to_owned(), event, meta));
    }
    fn watch_disconnects(&self) {
        self.watching.store(true, AtomicOrdering::Relaxed);
    }
}

impl Rec {
    fn take(&self) -> Vec<(String, Event, EventMeta)> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

/// `Device.S.`: a value that counts its reads, a settable value, a polled
/// value, an event, a table with a value per row, and a refused event.
struct Model {
    reads: AtomicI32,
    value: StdMutex<Value>,
    fail_reads: AtomicBool,
}

impl Provider for Model {
    fn schema(&self) -> Schema {
        Schema::new()
            .property("Device.S.Counter", Access::ReadOnly)
            .property("Device.S.Value", Access::ReadWrite)
            .property("Device.S.Polled", Access::ReadOnly)
            .poll(Duration::from_secs(2))
            .property("Device.S.Write", Access::WriteOnly)
            .event("Device.S.Ping!")
            .event("Device.S.Refused!")
            .table("Device.S.T.", Rows::Dynamic)
            .property("Device.S.T.{i}.X", Access::ReadWrite)
            .method("Device.S.Do()")
    }

    async fn get(&self, _cx: &Context, name: &PropertyName) -> Result<Value, Fault> {
        if self.fail_reads.load(AtomicOrdering::Relaxed) {
            return Err(Fault::access_denied());
        }
        let n = self.reads.fetch_add(1, AtomicOrdering::Relaxed);
        match name.as_str() {
            "Device.S.Counter" | "Device.S.Polled" => Ok(Value::I32(n)),
            _ => Ok(self.value.lock().unwrap().clone()),
        }
    }

    async fn set(&self, _cx: &Context, tx: Transaction) -> Result<(), SetFault> {
        for c in tx {
            *self.value.lock().unwrap() = c.into_value();
        }
        Ok(())
    }

    async fn add_row(&self, _cx: &Context, _t: &TablePath, _a: Option<&str>) -> Result<u32, Fault> {
        Ok(5)
    }

    async fn remove_row(&self, _cx: &Context, _t: &TablePath, _r: &RowId) -> Result<(), Fault> {
        Ok(())
    }

    async fn on_subscribe(&self, _cx: &Context, sub: &SubscriberInfo) -> Result<(), Fault> {
        if sub.event.ends_with("Refused!") {
            return Err(Fault::access_denied());
        }
        Ok(())
    }
}

fn setup() -> (Registry, Arc<Model>, Rec) {
    let r = Registry::default();
    let m = Arc::new(Model {
        reads: AtomicI32::new(0),
        value: StdMutex::new(Value::I32(0)),
        fail_reads: AtomicBool::new(false),
    });
    r.mount(m.schema(), m.clone(), false).unwrap();
    let rec = Rec::default();
    r.add_local_row(
        0,
        "Device.S.T.",
        RowId {
            inst: 1,
            alias: Some("one".into()),
        },
        &rec,
    )
    .unwrap();
    (r, m, rec)
}

fn cx(inbox: &str) -> Context {
    Context::traced(None, inbox.into(), Span::none())
}

fn req(
    name: &str,
    inbox: &str,
    id: i32,
    filter: Option<Filter>,
    initial: bool,
) -> SubscribeRequest {
    SubscribeRequest {
        event_name: name.into(),
        subscriber: inbox.into(),
        options: Some(SubscribeOptions {
            component_id: id,
            interval: 0,
            duration: 0,
            filter,
        }),
        publish_on_subscribe: initial,
        raw_data: false,
    }
}

fn decode(payload: Vec<u8>, initial: bool) -> SubscribeResponse {
    let bytes = Bytes::from(payload);
    let (mut body, meta) =
        rbus_wire::message::split_method_payload(&bytes, &rbus_wire::Limits::DEFAULT).unwrap();
    assert_eq!(meta.method, Method::Response);
    SubscribeResponse::decode_for(&mut body, initial).unwrap()
}

async fn sub(r: &Registry, out: &Rec, rq: SubscribeRequest) -> SubscribeResponse {
    let initial = rq.publish_on_subscribe;
    let inbox = rq.subscriber.clone();
    decode(r.subscribe(rq, true, cx(&inbox), out).await, initial)
}

async fn unsub(r: &Registry, out: &Rec, rq: SubscribeRequest) -> Status {
    let inbox = rq.subscriber.clone();
    Status::from_wire(decode(r.subscribe(rq, false, cx(&inbox), out).await, false).status)
}

async fn set(r: &Registry, out: &Rec, name: &str, value: Value) {
    let req = Request::Set(SetRequest {
        session_id: 0,
        component: "setter".into(),
        rollback: false,
        properties: vec![Property::new(name, value)],
        commit: true,
    });
    r.handle(
        req,
        Context::traced(Some("setter".into()), "i".into(), Span::none()),
        out,
    )
    .await;
}

fn data(e: &Event, key: &str) -> Option<Value> {
    e.data.as_ref().and_then(|d| d.get(key).cloned())
}

fn gt(v: i32) -> Filter {
    Filter::Relation {
        op: RelationOp::GreaterThan,
        value: Value::I32(v),
    }
}

#[tokio::test]
async fn p9_a_set_publishes_the_change_to_each_subscriber() {
    let (r, _m, out) = setup();
    let resp = sub(
        &r,
        &out,
        req("Device.S.Value", "rbus.a.INBOX.1", 7, None, false),
    )
    .await;
    assert_eq!((resp.status, resp.subscription_id), (0, Some(3)));
    assert!(out.watching.load(AtomicOrdering::Relaxed));
    // The same subscription again is refused, as C does (D29, D35).
    let again = sub(
        &r,
        &out,
        req("Device.S.Value", "rbus.a.INBOX.1", 7, None, false),
    )
    .await;
    assert_eq!(
        Status::from_wire(again.status),
        Status::SubscriptionAlreadyExist
    );
    // Another component id from the same inbox is a second subscription.
    let second = sub(
        &r,
        &out,
        req("Device.S.Value", "rbus.a.INBOX.1", 8, None, false),
    )
    .await;
    assert_eq!(second.subscription_id, Some(4));
    set(&r, &out, "Device.S.Value", Value::I32(5)).await;
    let sent = out.take();
    assert_eq!(sent.len(), 2);
    let (inbox, ev, meta) = &sent[0];
    assert_eq!(inbox, "rbus.a.INBOX.1");
    assert_eq!(
        (ev.name.as_str(), ev.kind, ev.component_id),
        ("Device.S.Value", EventKind::ValueChanged, 7)
    );
    assert_eq!(
        (data(ev, "value"), data(ev, "oldValue")),
        (Some(Value::I32(5)), Some(Value::I32(0)))
    );
    assert_eq!(data(ev, "by"), Some(Value::from("setter")));
    assert_eq!(data(ev, "filter"), None);
    assert_eq!(
        (meta.event_name.as_str(), meta.object_name.as_str()),
        ("Device.S.Value", "test.provider")
    );
    assert_eq!(sent[1].1.component_id, 8);
    // Setting the same value again changes nothing.
    set(&r, &out, "Device.S.Value", Value::I64(5)).await;
    assert!(out.take().is_empty());
    assert_eq!(
        unsub(
            &r,
            &out,
            req("Device.S.Value", "rbus.a.INBOX.1", 7, None, false)
        )
        .await,
        Status::Success
    );
    assert_eq!(
        unsub(
            &r,
            &out,
            req("Device.S.Value", "rbus.a.INBOX.1", 7, None, false)
        )
        .await,
        Status::InvalidInput
    );
    set(&r, &out, "Device.S.Value", Value::I32(6)).await;
    assert_eq!(out.take().len(), 1, "one subscription left");
}

#[tokio::test]
async fn p12_filters_report_flips_only() {
    let (r, _m, out) = setup();
    sub(&r, &out, req("Device.S.Value", "f", 1, Some(gt(3)), false)).await;
    sub(&r, &out, req("Device.S.Value", "plain", 1, None, false)).await;
    let mut flips = Vec::new();
    for v in [2, 4, 5, 3, 1] {
        set(&r, &out, "Device.S.Value", Value::I32(v)).await;
        for (inbox, ev, _) in out.take() {
            if inbox == "f" {
                flips.push((data(&ev, "value"), data(&ev, "filter")));
                assert_eq!(ev.filter, Some(gt(3)));
            } else {
                assert_eq!(
                    data(&ev, "filter"),
                    None,
                    "only the filtered subscriber gets 'filter'"
                );
            }
        }
    }
    assert_eq!(
        flips,
        [
            (Some(Value::I32(4)), Some(Value::Bool(true))),
            (Some(Value::I32(3)), Some(Value::Bool(false)))
        ]
    );
}

#[tokio::test]
async fn p4_events_go_to_their_subscribers_only() {
    let (r, _m, out) = setup();
    sub(&r, &out, req("Device.S.Ping!", "e", 2, None, false)).await;
    sub(&r, &out, req("Device.S.Value", "v", 2, None, false)).await;
    let obj = Object::new().with("n", 1i32);
    r.publish_event(0, &out, "Device.S.Ping!", obj.clone())
        .unwrap();
    let sent = out.take();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        (sent[0].0.as_str(), sent[0].1.kind),
        ("e", EventKind::General)
    );
    assert_eq!(sent[0].1.data, Some(obj.clone()));
    assert_eq!(
        r.publish_event(0, &out, "Device.S.Value", obj.clone()),
        Err(Status::ElementDoesNotExist)
    );
    assert_eq!(
        r.publish_event(1, &out, "Device.S.Ping!", obj.clone()),
        Err(Status::AccessNotAllowed)
    );
    assert_eq!(
        r.publish_event(0, &out, "Device.S.Nope!", obj),
        Err(Status::ElementDoesNotExist)
    );
    // The provider can refuse a subscription.
    let refused = sub(&r, &out, req("Device.S.Refused!", "e", 2, None, false)).await;
    assert_eq!(Status::from_wire(refused.status), Status::AccessNotAllowed);
}

#[tokio::test]
async fn pt9_row_events_follow_patterns() {
    let (r, _m, out) = setup();
    sub(&r, &out, req("Device.S.T.", "t", 1, None, false)).await;
    sub(&r, &out, req("Device.S.T.*", "w", 1, None, false)).await;
    sub(&r, &out, req("Device.S.T.[one].X", "x", 1, None, false)).await;
    // A consumer adds a row.
    let add = Request::AddRow(AddRowRequest {
        session_id: 0,
        table: "Device.S.T.".into(),
        alias: Some("five".into()),
    });
    r.handle(add, cx("c"), &out).await;
    let sent = out.take();
    let to: Vec<&str> = sent.iter().map(|(i, _, _)| i.as_str()).collect();
    assert_eq!(to, ["t", "w"]);
    let ev = &sent[0].1;
    assert_eq!(
        (ev.name.as_str(), ev.kind),
        ("Device.S.T.", EventKind::ObjectCreated)
    );
    assert_eq!(data(ev, "rowName"), Some(Value::from("Device.S.T.5")));
    assert_eq!(data(ev, "instNum"), Some(Value::U32(5)));
    assert_eq!(data(ev, "alias"), Some(Value::from("five")));
    assert_eq!(sent[1].2.event_name, "Device.S.T.*", "named as subscribed");
    // ... and removes it.
    let rm = Request::RemoveRow(DeleteRowRequest {
        session_id: 0,
        row: "Device.S.T.[five]".into(),
    });
    r.handle(rm, cx("c"), &out).await;
    let sent = out.take();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].1.kind, EventKind::ObjectDeleted);
    assert_eq!(
        sent[0].1.data,
        Some(Object::new().with("rowName", "Device.S.T.5"))
    );
    // Provider-owned rows publish too.
    r.add_local_row(0, "Device.S.T.", RowId::from(9), &out)
        .unwrap();
    assert_eq!(data(&out.take()[0].1, "alias"), Some(Value::from("")));
    r.remove_local_row(0, "Device.S.T.", 9, &out).unwrap();
    assert_eq!(out.take().len(), 2);
    // A row's value goes to the alias subscription.
    set(&r, &out, "Device.S.T.1.X", Value::I32(1)).await;
    let sent = out.take();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        (sent[0].0.as_str(), sent[0].1.name.as_str()),
        ("x", "Device.S.T.1.X")
    );
}

#[tokio::test]
async fn d9_requests_are_checked_like_rbus_c() {
    let (r, _m, out) = setup();
    let mut interval = req("Device.S.Value", "i", 1, None, false);
    interval.options.as_mut().unwrap().interval = 5;
    let mut raw = req("Device.S.Ping!", "i", 1, None, false);
    raw.raw_data = true;
    for (rq, status) in [
        (interval, Status::InvalidOperation),
        (raw, Status::InvalidOperation),
        (req("Device.S.T", "i", 1, None, false), Status::InvalidEvent),
        (
            req("Device.S.T.7.X", "i", 1, None, false),
            Status::ElementDoesNotExist,
        ),
        (
            req("Device.S.Nope", "i", 1, None, false),
            Status::ElementDoesNotExist,
        ),
        (
            req("Device.S.*.X", "i", 1, None, false),
            Status::ElementDoesNotExist,
        ),
    ] {
        let name = rq.event_name.clone();
        let resp = sub(&r, &out, rq).await;
        assert_eq!(Status::from_wire(resp.status), status, "{name}");
        assert_eq!(resp.initial, InitialValue::NotRequested);
    }
    // Patterns over rows that don't exist yet are fine; so are objects and
    // methods, which never get events.
    for name in ["Device.S.T.*.X", "Device.S.", "Device.S.Do()"] {
        assert_eq!(
            sub(&r, &out, req(name, "i", 1, None, false)).await.status,
            0,
            "{name}"
        );
    }
    assert_eq!(
        unsub(&r, &out, req("Device.S.Gone", "i", 1, None, false)).await,
        Status::ElementDoesNotExist
    );
    // Requests without options use component id 0.
    let mut bare = req("Device.S.Ping!", "b", 0, None, false);
    bare.options = None;
    assert_eq!(sub(&r, &out, bare).await.status, 0);
}

#[tokio::test]
async fn p15_initial_values() {
    let (r, m, out) = setup();
    let initial = |resp: SubscribeResponse| match resp.initial {
        InitialValue::Value(ev) => *ev,
        other => panic!("{other:?}"),
    };
    let ev = initial(sub(&r, &out, req("Device.S.Value", "i", 4, Some(gt(1)), true)).await);
    assert_eq!(
        (ev.name.as_str(), ev.kind, ev.component_id),
        ("Device.S.Value", EventKind::InitialValue, 4)
    );
    assert_eq!(ev.data, Some(Object::new().with("initialValue", 0i32)));
    assert_eq!(ev.filter, Some(gt(1)));
    let table = initial(sub(&r, &out, req("Device.S.T.", "i", 4, None, true)).await);
    assert_eq!(
        table.data,
        Some(
            Object::new()
                .with("numberOfEntries", 1i32)
                .with("path1", "Device.S.T.1.")
        )
    );
    let wild = initial(sub(&r, &out, req("Device.S.T.*.X", "i", 4, None, true)).await);
    assert_eq!(
        wild.data,
        Some(
            Object::new()
                .with("numberOfEntries", 1i32)
                .with("Device.S.T.1.X", 0i32)
        )
    );
    let event = sub(&r, &out, req("Device.S.Ping!", "i", 4, None, true)).await;
    assert_eq!(event.initial, InitialValue::Unavailable);
    m.fail_reads.store(true, AtomicOrdering::Relaxed);
    let failed = sub(&r, &out, req("Device.S.Counter", "i", 4, None, true)).await;
    assert_eq!(
        (failed.status, failed.initial),
        (0, InitialValue::Unavailable)
    );
    let failed = sub(&r, &out, req("Device.S.T.*.X", "j", 4, None, true)).await;
    assert_eq!(failed.initial, InitialValue::Unavailable);
}

#[tokio::test]
async fn p10_polled_properties_publish_their_changes() {
    let (r, m, out) = setup();
    // Not subscribed: never read.
    r.poll_once(&out, Instant::now() + Duration::from_secs(60))
        .await;
    assert_eq!(m.reads.load(AtomicOrdering::Relaxed), 0);
    sub(&r, &out, req("Device.S.Polled", "p", 1, None, false)).await;
    assert_eq!(m.reads.load(AtomicOrdering::Relaxed), 1, "baseline read");
    let t0 = Instant::now();
    r.poll_once(&out, t0).await;
    assert!(out.take().is_empty(), "first poll schedules");
    r.poll_once(&out, t0 + Duration::from_secs(1)).await;
    assert!(out.take().is_empty(), "not due yet");
    r.poll_once(&out, t0 + Duration::from_secs(2)).await;
    let sent = out.take();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        (data(&sent[0].1, "value"), data(&sent[0].1, "oldValue")),
        (Some(Value::I32(1)), Some(Value::I32(0)))
    );
    assert_eq!(data(&sent[0].1, "by"), Some(Value::from("test.provider")));
    // Unpolled properties aren't read by the poller.
    sub(&r, &out, req("Device.S.Counter", "p", 1, None, false)).await;
    let reads = m.reads.load(AtomicOrdering::Relaxed);
    r.poll_once(&out, t0 + Duration::from_secs(4)).await;
    assert_eq!(m.reads.load(AtomicOrdering::Relaxed), reads + 1);
    // A failing read changes nothing.
    m.fail_reads.store(true, AtomicOrdering::Relaxed);
    r.poll_once(&out, t0 + Duration::from_secs(6)).await;
    assert_eq!(out.take().len(), 1, "only the earlier poll's event");
}

#[tokio::test]
async fn p11_providers_report_changes() {
    let (r, m, out) = setup();
    // Nobody subscribed: nothing to do.
    r.notify_changed(0, &out, "Device.S.Counter").await.unwrap();
    assert_eq!(m.reads.load(AtomicOrdering::Relaxed), 0);
    r.publish_change(0, &out, "Device.S.Value", Value::I32(1), Value::I32(2))
        .unwrap();
    assert!(out.take().is_empty());
    sub(&r, &out, req("Device.S.Counter", "n", 1, None, false)).await;
    r.notify_changed(0, &out, "Device.S.Counter").await.unwrap();
    let sent = out.take();
    assert_eq!(
        (data(&sent[0].1, "value"), data(&sent[0].1, "oldValue")),
        (Some(Value::I32(1)), Some(Value::I32(0)))
    );
    sub(&r, &out, req("Device.S.Value", "n", 1, Some(gt(5)), false)).await;
    r.publish_change(0, &out, "Device.S.Value", Value::I32(5), Value::I32(6))
        .unwrap();
    let sent = out.take();
    assert_eq!(data(&sent[0].1, "filter"), Some(Value::Bool(true)));
    r.publish_change(0, &out, "Device.S.Value", Value::I32(6), Value::I32(6))
        .unwrap();
    assert!(out.take().is_empty(), "equal values are no change");
    assert_eq!(
        r.notify_changed(0, &out, "Device.S.Write").await,
        Err(Status::ElementDoesNotExist)
    );
    assert_eq!(
        r.publish_change(0, &out, "Device.S.Ping!", Value::None, Value::None),
        Err(Status::ElementDoesNotExist)
    );
    assert_eq!(
        r.publish_change(0, &out, "Device.S.T.", Value::None, Value::None),
        Err(Status::ElementDoesNotExist)
    );
}

struct Kept(StdMutex<Vec<Vec<SubscriberInfo>>>, Vec<SubscriberInfo>);

impl SubscriberStore for Arc<Kept> {
    fn save(&self, subscribers: &[SubscriberInfo]) {
        self.0.lock().unwrap().push(subscribers.to_vec());
    }
    fn load(&self) -> Vec<SubscriberInfo> {
        self.1.clone()
    }
}

#[tokio::test]
async fn p8_subscribers_are_visible_and_disconnects_prune_them() {
    let (r, _m, out) = setup();
    let mut events = r.listen(0);
    sub(
        &r,
        &out,
        req("Device.S.T.*.X", "rbus.a.INBOX.1", 1, None, false),
    )
    .await;
    sub(
        &r,
        &out,
        req("Device.S.Ping!", "rbus.b.INBOX.2", 1, None, false),
    )
    .await;
    let all = r.subscribers(0, None);
    assert_eq!(all.len(), 2);
    assert_eq!(
        r.subscribers(0, Some("Device.S.T.[one].X")).len(),
        1,
        "a pattern covers the row"
    );
    assert_eq!(r.subscribers(0, Some("Device.S.Ping!")).len(), 1);
    assert!(r.subscribers(1, None).is_empty());
    assert!(
        matches!(events.next().await, Some(Ok(SubscriptionEvent::Added(i))) if i.event == "Device.S.T.*.X")
    );
    r.client_gone("rbus.a.INBOX.1");
    r.client_gone("rbus.unknown.INBOX.9");
    assert!(matches!(
        events.next().await,
        Some(Ok(SubscriptionEvent::Added(_)))
    ));
    assert!(
        matches!(events.next().await, Some(Ok(SubscriptionEvent::Removed(i))) if i.inbox == "rbus.a.INBOX.1")
    );
    assert_eq!(r.subscribers(0, None).len(), 1);
    // Unmounting ends the stream and drops the subscriptions.
    r.unmount(0);
    assert!(events.next().await.is_none());
    assert!(r.listen(0).next().await.is_none(), "no such provider");
}

#[tokio::test]
async fn d11_a_store_sees_every_change_and_restores_live_subscribers() {
    let me = format!("rbus.me.INBOX.{}", std::process::id());
    let keep = SubscriberInfo {
        event: "Device.S.Ping!".into(),
        inbox: me.clone(),
        component_id: 2,
        filter: None,
        id: 11,
    };
    let stale = SubscriberInfo {
        event: "Device.S.Gone!".into(),
        id: 12,
        ..keep.clone()
    };
    let store = Arc::new(Kept(StdMutex::new(Vec::new()), vec![keep.clone(), stale]));
    let r = Registry::default();
    let m = Arc::new(Model {
        reads: AtomicI32::new(0),
        value: StdMutex::new(Value::I32(0)),
        fail_reads: AtomicBool::new(false),
    });
    let id = r
        .mount_with(m.schema(), m.clone(), false, Arc::new(store.clone()))
        .unwrap();
    let out = Rec::default();
    // Listeners from attach() hear about restored subscribers.
    let mut events = r.listen(id);
    assert!(r.restore(id, &out), "restored, so the poller is started");
    assert!(!r.restore(id, &out));
    assert!(matches!(events.next().await, Some(Ok(SubscriptionEvent::Added(i))) if i == keep));
    assert_eq!(
        r.subscribers(id, None),
        std::slice::from_ref(&keep),
        "restored once; unknown names skipped"
    );
    assert!(out.watching.load(AtomicOrdering::Relaxed));
    r.publish_event(id, &out, "Device.S.Ping!", Object::new())
        .unwrap();
    assert_eq!(out.take()[0].0, me);
    let resp = sub(&r, &out, req("Device.S.Value", "x", 1, None, false)).await;
    assert_eq!(
        resp.subscription_id,
        Some(12),
        "ids continue after restored ones"
    );
    assert_eq!(store.0.lock().unwrap().last().unwrap().len(), 2);
    assert!(!r.restore(9, &out));

    // A consumer that subscribed again before the restore isn't doubled,
    // though its new subscription has another id.
    let r = Registry::default();
    let id = r
        .mount_with(m.schema(), m.clone(), false, Arc::new(store.clone()))
        .unwrap();
    let again = sub(&r, &out, req("Device.S.Ping!", &me, 2, None, false)).await;
    assert_eq!(again.status, 0);
    assert!(!r.restore(id, &out));
    assert_eq!(r.subscribers(id, None).len(), 1);
}

#[tokio::test]
async fn subscribe_requests_arrive_through_handle() {
    let (r, _m, out) = setup();
    let payload = request_body(&req("Device.S.Ping!", "h", 1, None, false));
    let request = Request::decode(&Method::Subscribe, &mut Reader::new(&payload)).unwrap();
    assert!(matches!(request, Request::Subscribe(_, true)));
    assert_eq!(
        decode(r.handle(request, cx("h"), &out).await, false).status,
        0
    );
    let request = Request::decode(&Method::Unsubscribe, &mut Reader::new(&payload)).unwrap();
    assert_eq!(
        decode(r.handle(request, cx("h"), &out).await, false).status,
        0
    );
    assert!(r.start_poller());
    assert!(!r.start_poller());
}

fn request_body(rq: &SubscribeRequest) -> Vec<u8> {
    let mut w = Writer::new();
    rq.encode(&mut w);
    w.into_bytes()
}

/// A store that reads the registry back while saving, as a store that
/// logs its provider's subscribers might.
struct ReadBack {
    registry: std::sync::OnceLock<Arc<Registry>>,
    saved: StdMutex<Vec<Vec<String>>>,
}

impl SubscriberStore for ReadBack {
    fn save(&self, subscribers: &[SubscriberInfo]) {
        // Re-entering the registry from a save deadlocked while saves ran
        // under its lock.
        let now = self.registry.get().unwrap().subscribers(0, None);
        assert_eq!(now.len(), subscribers.len());
        self.saved
            .lock()
            .unwrap()
            .push(subscribers.iter().map(|s| s.event.clone()).collect());
    }
}

#[tokio::test]
async fn pt2_removing_a_row_drops_the_exact_subscriptions_under_it() {
    let r = Arc::new(Registry::default());
    let m = Arc::new(Model {
        reads: AtomicI32::new(0),
        value: StdMutex::new(Value::I32(0)),
        fail_reads: AtomicBool::new(false),
    });
    let store = Arc::new(ReadBack {
        registry: std::sync::OnceLock::new(),
        saved: StdMutex::new(Vec::new()),
    });
    let _ = store.registry.set(r.clone());
    struct Shared(Arc<ReadBack>);
    impl SubscriberStore for Shared {
        fn save(&self, subscribers: &[SubscriberInfo]) {
            self.0.save(subscribers);
        }
    }
    r.mount_with(
        m.schema(),
        m.clone(),
        false,
        Arc::new(Shared(store.clone())),
    )
    .unwrap();
    let out = Rec::default();
    for (inst, alias) in [(1, "one"), (2, "two")] {
        let row = RowId {
            inst,
            alias: Some(alias.into()),
        };
        r.add_local_row(0, "Device.S.T.", row, &out).unwrap();
    }
    // By alias, by number, under the other row, and by pattern.
    for (i, name) in [
        "Device.S.T.[one].X",
        "Device.S.T.1.X",
        "Device.S.T.2.X",
        "Device.S.T.*.X",
    ]
    .into_iter()
    .enumerate()
    {
        let resp = sub(&r, &out, req(name, "rbus.c.INBOX.7", i as i32, None, false)).await;
        assert_eq!(resp.status, 0, "{name}");
    }
    r.remove_local_row(0, "Device.S.T.", 1, &out).unwrap();
    let left: Vec<String> = r
        .subscribers(0, None)
        .into_iter()
        .map(|s| s.event)
        .collect();
    assert_eq!(left, ["Device.S.T.2.X", "Device.S.T.*.X"]);
    // The store saw it, after the lock was released.
    assert_eq!(
        store.saved.lock().unwrap().last().unwrap(),
        &["Device.S.T.2.X", "Device.S.T.*.X"]
    );
    // A row number used again doesn't bring the old subscription back.
    let back = RowId::from(1);
    r.add_local_row(0, "Device.S.T.", back, &out).unwrap();
    assert_eq!(r.subscribers(0, None).len(), 2);
}
