//! The subscription registry (M5): subscribe and unsubscribe requests, and
//! publishing to subscribers (decisions.md D9, D35).
//!
//! Like `rbus.c`, the engine keeps every subscriber and sends each one its
//! own copy of an event, addressed to its inbox and named as it subscribed:
//!
//! - **General events** go to subscribers of the event (`Foo!`).
//! - **Value changes** go to subscribers of the property, with data
//!   `{value, oldValue, by}`. A subscriber with a filter gets an event only
//!   when the filter's result flips, with `filter` set to the new result
//!   (`rbusEvent_Publish`). A change is found after a consumer's set (`by`
//!   is the setter), by polling a property declared with `Schema::poll`, or
//!   when the provider reports it (`by` is our component).
//! - **Row events** (`OBJECT_CREATED`, `OBJECT_DELETED`) go to subscribers of
//!   the table, for rows added or removed by consumers or the provider (PT9).
//!
//! Patterns (`*`, `[alias]`) are matched against the rows when an event is
//! sent, so rows added later are covered.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::{Arc, Weak};

use rbus_wire::body::{InitialValue, SubscribeOptions, SubscribeRequest, SubscribeResponse};
use rbus_wire::event::{Event, EventKind};
use rbus_wire::message::{EventMeta, MethodMeta, TraceContext};
use rbus_wire::msgpack::Writer;
use tokio::time::{Duration, Instant};
use tracing::Span;

use super::*;
use crate::provider::events::{
    SUBSCRIPTION_EVENT_BUFFER, SubscriberInfo, SubscriptionEvent, SubscriptionEvents,
};
use crate::queue::{Overflow, Queue};

/// How often the poller checks for properties that are due.
const POLL_TICK: Duration = Duration::from_millis(100);

/// A subscription, as the registry keeps it.
pub(crate) struct Sub {
    pub(crate) info: SubscriberInfo,
    kind: SubKind,
    /// The provider owning the element; `None` for implicit objects.
    pub(super) mount: Option<MountId>,
}

/// What a subscription is to: it decides which events it receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubKind {
    /// A property: value changes.
    Value,
    /// An event: general events.
    Event,
    /// A table: row events.
    Table,
    /// Anything else (an object, a method): accepted, as C does, but no
    /// events.
    Other,
}

impl SubKind {
    fn receives(self, kind: EventKind) -> bool {
        matches!(
            (self, kind),
            (SubKind::Value, EventKind::ValueChanged)
                | (SubKind::Event, EventKind::General)
                | (
                    SubKind::Table,
                    EventKind::ObjectCreated | EventKind::ObjectDeleted
                )
        )
    }
}

/// True if the process whose id ends `inbox` (`...INBOX.<pid>`) is running,
/// as `rbus_subscriptions.c` checks it. Without `/proc`, assume it is.
fn listener_running(inbox: &str) -> bool {
    let Some(pid) = inbox.rsplit('.').next().and_then(|p| p.parse::<u32>().ok()) else {
        return true;
    };
    !Path::new("/proc/self").exists() || Path::new(&format!("/proc/{pid}")).exists()
}

/// A response to a subscribe or unsubscribe request.
fn subscribe_payload(resp: &SubscribeResponse) -> Vec<u8> {
    let mut w = Writer::new();
    resp.encode(&mut w);
    w.finish_with_meta(|m| MethodMeta::response().encode(m))
}

fn refused(status: Status) -> Vec<u8> {
    subscribe_payload(&SubscribeResponse {
        status: status.code(),
        initial: InitialValue::NotRequested,
        subscription_id: Some(0),
    })
}

/// A context for reads the engine makes itself (baselines, polls).
fn internal_cx(out: &dyn Outbox) -> Context {
    Context::traced(
        Some(out.component().to_owned()),
        String::new(),
        Span::none(),
    )
}

impl State {
    /// Works out what a subscription name refers to (`retrieveInstanceElementEx`
    /// in `_subscribe_callback_handler`). Exact names need their rows to
    /// exist; patterns only need to fit the templates.
    fn plan_subscription(&self, name: &str) -> Result<(SubKind, Option<MountId>), Status> {
        let target = if name.contains('*') {
            self.model.resolve_template(name)?
        } else {
            self.model.resolve(name)?.target
        };
        let node = match target {
            Target::Node(n) | Target::Table(n) | Target::Row(n, _) => self.model.tree.node(n),
        };
        let kind = match (target, node.kind) {
            (Target::Table(_), _) if !name.ends_with('.') && !name.ends_with(".*") => {
                return Err(Status::InvalidEvent);
            }
            (Target::Table(_), _) => SubKind::Table,
            (Target::Node(_), NodeKind::Property(_)) => SubKind::Value,
            (Target::Node(_), NodeKind::Event) => SubKind::Event,
            _ => SubKind::Other,
        };
        Ok((kind, node.owner))
    }

    /// The handler of the provider that owns a found property.
    fn handler_of(&self, found: &Found) -> Option<Handler> {
        self.handler(self.model.tree.node(found.node).owner)
    }

    pub(super) fn mount(&self, id: MountId) -> Option<&Mount> {
        self.mounts.iter().find(|(m, _)| *m == id).map(|(_, m)| m)
    }

    fn mount_mut(&mut self, id: MountId) -> Option<&mut Mount> {
        self.mounts
            .iter_mut()
            .find(|(m, _)| *m == id)
            .map(|(_, m)| m)
    }

    /// Tells the mount's listeners about a change, and marks its store for
    /// saving once the lock is released ([`Registry::flush_saves`]): the
    /// store is the user's code, which may block or call back in.
    fn changed(&mut self, mount: Option<MountId>, event: SubscriptionEvent) {
        let Some(id) = mount else { return };
        if let Some(m) = self.mount_mut(id) {
            m.listeners.retain(|w| w.strong_count() > 0);
            for q in m.listeners.iter().filter_map(Weak::upgrade) {
                q.push_now(event.clone());
            }
            if !self.unsaved.contains(&id) {
                self.unsaved.push(id);
            }
        }
    }

    /// A mount's store and its current subscribers.
    pub(super) fn snapshot(
        &self,
        id: MountId,
    ) -> Option<(Arc<dyn SubscriberStore>, Vec<SubscriberInfo>)> {
        let m = self.mount(id)?;
        let subscribers = self
            .subs
            .iter()
            .filter(|s| s.mount == Some(id))
            .map(|s| s.info.clone())
            .collect();
        Some((m.store.clone(), subscribers))
    }

    /// Removes a row with everything under it, publishes `OBJECT_DELETED`,
    /// and drops the exact subscriptions to anything in it, as C does
    /// (`rbus_subscriptions.c:308-390`). Patterns stay: later rows may
    /// match them.
    pub(super) fn remove_row(&mut self, out: &dyn Outbox, table: &str, inst: u32) -> Option<RowId> {
        let prefix = format!("{table}{inst}.");
        // Resolved before the row goes, while its alias still resolves.
        let doomed: Vec<u32> = self
            .subs
            .iter()
            .filter(|s| !s.info.event.contains('*') && !s.info.event.contains("{i}"))
            .filter(|s| {
                self.model
                    .resolve(&s.info.event)
                    .is_ok_and(|r| r.canonical.starts_with(&prefix))
            })
            .map(|s| s.info.id)
            .collect();
        let row = self.model.remove_row(table, inst)?;
        self.row_event(out, table, &row, false);
        self.remove_subs(|s| doomed.contains(&s.info.id));
        Some(row)
    }

    /// Removes the subscriptions `remove` picks, telling everyone, and drops
    /// value records no subscription needs any more.
    fn remove_subs(&mut self, remove: impl Fn(&Sub) -> bool) -> usize {
        let (gone, kept): (Vec<Sub>, Vec<Sub>) = std::mem::take(&mut self.subs)
            .into_iter()
            .partition(|s| remove(s));
        self.subs = kept;
        let n = gone.len();
        for sub in gone {
            debug!(event = %sub.info.event, inbox = %sub.info.inbox, "unsubscribed");
            self.changed(sub.mount, SubscriptionEvent::Removed(sub.info));
        }
        self.prune_values();
        n
    }

    pub(super) fn prune_values(&mut self) {
        let (subs, model) = (&self.subs, &self.model);
        self.values.retain(|name, _| {
            subs.iter()
                .any(|s| s.kind == SubKind::Value && model.matches(&s.info.event, name))
        });
        let values = &self.values;
        self.poll_due.retain(|name, _| values.contains_key(name));
    }

    /// True if some subscriber wants value changes of `canonical`.
    fn watched(&self, canonical: &str) -> bool {
        self.subs
            .iter()
            .any(|s| s.kind == SubKind::Value && self.model.matches(&s.info.event, canonical))
    }

    /// The readable properties a value subscription covers right now.
    fn covered(&self, pattern: &str) -> Vec<Found> {
        let mut found = Vec::new();
        for r in self.model.resolve_matching(pattern) {
            if let Target::Node(n) = r.target {
                if matches!(self.model.tree.node(n).kind, NodeKind::Property(a) if a.readable()) {
                    found.push(Found {
                        name: r.canonical,
                        node: n,
                        instances: r.instances,
                    });
                }
            }
        }
        found
    }

    /// Sends an event about `canonical` to every subscriber it concerns.
    /// For a value change, `change` is `(new, old)` and filters apply.
    fn deliver(
        &self,
        out: &dyn Outbox,
        canonical: &str,
        kind: EventKind,
        data: &Object,
        change: Option<(&Value, &Value)>,
    ) {
        for sub in &self.subs {
            if !sub.kind.receives(kind) || !self.model.matches(&sub.info.event, canonical) {
                continue;
            }
            let mut data = data.clone();
            if let (Some(filter), Some((new, old))) = (&sub.info.filter, change) {
                let now = filter.matches(new);
                if now == filter.matches(old) {
                    continue;
                }
                data.properties
                    .push(Property::new("filter", Value::Bool(now)));
            }
            let event = Event {
                name: canonical.to_owned(),
                kind,
                data: Some(data),
                filter: sub.info.filter.clone(),
                interval: 0,
                duration: 0,
                component_id: sub.info.component_id,
            };
            let meta = EventMeta {
                event_name: sub.info.event.clone(),
                object_name: out.component().to_owned(),
                rbus2: true,
                trace: TraceContext::default(),
            };
            let mut w = Writer::new();
            event.encode(&mut w);
            trace!(event = %sub.info.event, inbox = %sub.info.inbox, ?kind, "event sent");
            out.send_event(&sub.info.inbox, w.finish_with_meta(|m| meta.encode(m)));
        }
    }

    /// Publishes a value change, if `new` differs from `old`.
    fn deliver_change(
        &self,
        out: &dyn Outbox,
        canonical: &str,
        old: &Value,
        new: &Value,
        by: &str,
    ) {
        if old.compare(new) == Ordering::Equal {
            return;
        }
        let data = Object::new()
            .with("value", new.clone())
            .with("oldValue", old.clone())
            .with("by", by);
        self.deliver(
            out,
            canonical,
            EventKind::ValueChanged,
            &data,
            Some((new, old)),
        );
    }

    /// Publishes `OBJECT_CREATED` or `OBJECT_DELETED` for a row of the table
    /// at `table` (`registerTableRow`, `unregisterTableRow`).
    pub(super) fn row_event(&self, out: &dyn Outbox, table: &str, row: &RowId, created: bool) {
        let row_name = format!("{table}{}", row.inst);
        let (kind, data) = if created {
            let data = Object::new()
                .with("rowName", row_name)
                .with("instNum", Value::U32(row.inst))
                .with("alias", row.alias.clone().unwrap_or_default());
            (EventKind::ObjectCreated, data)
        } else {
            (
                EventKind::ObjectDeleted,
                Object::new().with("rowName", row_name),
            )
        };
        self.deliver(out, table, kind, &data, None);
    }
}

impl Registry {
    /// `METHOD_SUBSCRIBE` / `METHOD_UNSUBSCRIBE` (`_subscribe_callback_handler`,
    /// `subscribeHandlerImpl`).
    pub(super) async fn subscribe(
        &self,
        req: SubscribeRequest,
        added: bool,
        cx: Context,
        out: &dyn Outbox,
    ) -> Vec<u8> {
        let opts = req.options.clone().unwrap_or(SubscribeOptions {
            component_id: 0,
            interval: 0,
            duration: 0,
            filter: None,
        });
        let info = SubscriberInfo {
            event: req.event_name.clone(),
            inbox: req.subscriber.clone(),
            component_id: opts.component_id,
            filter: opts.filter.clone(),
            id: 0,
        };
        let same = |s: &Sub| {
            s.info.inbox == info.inbox
                && s.info.component_id == info.component_id
                && s.info.event == info.event
                && s.info.filter == info.filter
        };
        let planned = {
            let state = self.state();
            state
                .plan_subscription(&req.event_name)
                .and_then(|(kind, mount)| {
                    if !added {
                        return Ok((kind, mount, None));
                    }
                    // D9: intervals, durations and raw data are not supported.
                    if opts.interval != 0 || opts.duration != 0 || req.raw_data {
                        return Err(Status::InvalidOperation);
                    }
                    if state.subs.iter().any(same) {
                        return Err(Status::SubscriptionAlreadyExist);
                    }
                    Ok((kind, mount, state.handler(mount)))
                })
        };
        let (kind, mount, handler) = match planned {
            Ok(p) => p,
            Err(status) => return refused(status),
        };
        if !added {
            let removed = self.state().remove_subs(same);
            return if removed == 0 {
                // Not subscribed (`subscribeHandlerImpl`).
                refused(Status::InvalidInput)
            } else {
                refused(Status::Success)
            };
        }
        if let Some(handler) = &handler {
            if let Err(fault) = handler.provider.on_subscribe(&cx, &info).await {
                debug!(event = %info.event, %fault, "subscription refused by the provider");
                return refused(Status::from_wire(wire_status(fault.status())));
            }
        }
        // Baseline values for value changes, read before the subscription
        // counts, as `rbusValueChange_AddPropertyNode` does.
        if kind == SubKind::Value {
            let missing: Vec<(Found, Handler)> = {
                let state = self.state();
                state
                    .covered(&info.event)
                    .into_iter()
                    .filter(|f| !state.values.contains_key(&f.name))
                    .filter_map(|f| {
                        let h = state.handler_of(&f)?;
                        Some((f, h))
                    })
                    .collect()
            };
            self.record_baselines(out, missing).await;
        }
        let info = {
            let mut state = self.state();
            if state.subs.iter().any(same) {
                return refused(Status::SubscriptionAlreadyExist);
            }
            state.next_sub_id = state.next_sub_id.max(3);
            let info = SubscriberInfo {
                id: state.next_sub_id,
                ..info
            };
            state.next_sub_id += 1;
            state.subs.push(Sub {
                info: info.clone(),
                kind,
                mount,
            });
            state.changed(mount, SubscriptionEvent::Added(info.clone()));
            info
        };
        debug!(event = %info.event, inbox = %info.inbox, id = info.id, "subscribed");
        out.watch_disconnects();
        let initial = if req.publish_on_subscribe {
            self.initial_value(&req, &opts, kind, out).await
        } else {
            InitialValue::NotRequested
        };
        subscribe_payload(&SubscribeResponse {
            status: 0,
            initial,
            subscription_id: Some(info.id as i32),
        })
    }

    /// Reads a property for the engine itself; failures read as `None`.
    async fn read(&self, handler: &Handler, found: &Found, cx: Context) -> Option<Value> {
        let name: PropertyName = name_of(&found.name).ok()?;
        let cx = cx.with_instances(found.instances.clone());
        handler.provider.get(&cx, &name).await.ok()
    }

    /// The `INITIAL_VALUE` event for publishOnSubscribe (P15):
    /// `{initialValue}` for a property, `{numberOfEntries, path<i>...}` for a
    /// table, `{numberOfEntries, <name>...}` for a wildcard.
    async fn initial_value(
        &self,
        req: &SubscribeRequest,
        opts: &SubscribeOptions,
        kind: SubKind,
        out: &dyn Outbox,
    ) -> InitialValue {
        let name = &req.event_name;
        let data = if name.contains('*') && kind != SubKind::Table {
            let reads: Vec<(Found, Option<Handler>)> = {
                let state = self.state();
                let mut found = Vec::new();
                for r in state.model.resolve_all(name).unwrap_or_default() {
                    state.model.properties_under(&r, &mut found);
                }
                found
                    .into_iter()
                    .map(|f| {
                        let h = state.handler_of(&f);
                        (f, h)
                    })
                    .collect()
            };
            let mut data = Object::new().with("numberOfEntries", Value::I32(reads.len() as i32));
            for (found, handler) in reads {
                let value = match handler {
                    Some(h) => self.read(&h, &found, internal_cx(out)).await,
                    None => None,
                };
                let Some(value) = value else {
                    return InitialValue::Unavailable;
                };
                data.properties.push(Property::new(found.name, value));
            }
            data
        } else if kind == SubKind::Table {
            let rows: Vec<u32> = {
                let state = self.state();
                let mut rows = Vec::new();
                for r in state.model.resolve_all(name).unwrap_or_default() {
                    rows.extend(state.model.rows(&r.canonical).iter().map(|row| row.inst));
                }
                rows
            };
            let mut data = Object::new().with("numberOfEntries", Value::I32(rows.len() as i32));
            for inst in rows {
                data.properties.push(Property::new(
                    format!("path{inst}"),
                    Value::String(format!("{name}{inst}.")),
                ));
            }
            data
        } else if kind == SubKind::Value {
            let read = {
                let state = self.state();
                state.covered(name).into_iter().next().and_then(|f| {
                    let h = state.handler_of(&f)?;
                    Some((f, h))
                })
            };
            let value = match read {
                Some((found, handler)) => self.read(&handler, &found, internal_cx(out)).await,
                None => None,
            };
            match value {
                Some(v) => Object::new().with("initialValue", v),
                None => return InitialValue::Unavailable,
            }
        } else {
            return InitialValue::Unavailable;
        };
        InitialValue::Value(Box::new(Event {
            name: name.clone(),
            kind: EventKind::InitialValue,
            data: Some(data),
            filter: opts.filter.clone(),
            interval: opts.interval,
            duration: opts.duration,
            component_id: opts.component_id,
        }))
    }

    /// A client left the bus: drop its subscriptions (`rbusSubscriptions_handleClientDisconnect`).
    pub(crate) fn client_gone(&self, inbox: &str) {
        let n = {
            let mut state = self.state();
            // Its uncommitted sets can never be committed now.
            state.sessions.retain(|(caller, _), _| caller != inbox);
            state.remove_subs(|s| s.info.inbox == inbox)
        };
        if n > 0 {
            debug!(inbox, subscriptions = n, "subscriber disconnected");
        }
    }

    /// Restores the subscribers a store kept, skipping those whose process
    /// is gone (`rbusSubscriptions_loadCache`).
    ///
    /// Returns true if it restored any, so the caller starts the poller
    /// that value changes need.
    pub(crate) fn restore(&self, id: MountId, out: &dyn Outbox) -> bool {
        let Some(store) = self.state().mount(id).map(|m| m.store.clone()) else {
            return false;
        };
        // Outside the lock: the store is the user's code.
        let loaded = store.load();
        let mut restored = 0;
        for info in loaded {
            let mut state = self.state();
            // The D35 key: a consumer may have subscribed again already.
            let same = |s: &Sub| {
                s.info.inbox == info.inbox
                    && s.info.component_id == info.component_id
                    && s.info.event == info.event
                    && s.info.filter == info.filter
            };
            if !listener_running(&info.inbox) || state.subs.iter().any(same) {
                continue;
            }
            let Ok((kind, mount)) = state.plan_subscription(&info.event) else {
                continue;
            };
            state.next_sub_id = state.next_sub_id.max(3).max(info.id + 1);
            state.subs.push(Sub {
                info: info.clone(),
                kind,
                mount,
            });
            state.changed(mount, SubscriptionEvent::Added(info));
            restored += 1;
        }
        if restored > 0 {
            debug!(restored, "subscribers restored");
            out.watch_disconnects();
        }
        restored > 0
    }

    /// Subscriptions a provider's elements have. With a name, only those
    /// that cover it.
    pub(crate) fn subscribers(&self, id: MountId, name: Option<&str>) -> Vec<SubscriberInfo> {
        let state = self.state();
        let canonical = name.map(|n| {
            state
                .model
                .resolve(n)
                .map_or_else(|_| n.to_owned(), |r| r.canonical)
        });
        state
            .subs
            .iter()
            .filter(|s| s.mount == Some(id))
            .filter(|s| match &canonical {
                Some(c) => {
                    s.info.event == name.unwrap_or_default()
                        || state.model.matches(&s.info.event, c)
                }
                None => true,
            })
            .map(|s| s.info.clone())
            .collect()
    }

    /// A stream of a provider's subscription changes (P8).
    pub(crate) fn listen(&self, id: MountId) -> SubscriptionEvents {
        let queue = Arc::new(Queue::new(SUBSCRIPTION_EVENT_BUFFER, Overflow::DropOldest));
        match self.state().mount_mut(id) {
            Some(m) => m.listeners.push(Arc::downgrade(&queue)),
            None => queue.close(),
        }
        SubscriptionEvents { queue }
    }

    /// The canonical name and node of an element the provider `id` owns, if
    /// it is of the wanted kind.
    fn own_element(
        &self,
        id: MountId,
        name: &str,
        wanted: impl Fn(NodeKind) -> bool,
    ) -> Result<Found, Status> {
        let state = self.state();
        let r = state.model.resolve(name)?;
        let Target::Node(n) = r.target else {
            return Err(Status::ElementDoesNotExist);
        };
        let node = state.model.tree.node(n);
        if !wanted(node.kind) {
            return Err(Status::ElementDoesNotExist);
        }
        if node.owner != Some(id) {
            return Err(Status::AccessNotAllowed);
        }
        Ok(Found {
            name: r.canonical,
            node: n,
            instances: r.instances,
        })
    }

    /// Publishes a general event (P4). Without subscribers it does nothing
    /// (D10).
    pub(crate) fn publish_event(
        &self,
        id: MountId,
        out: &dyn Outbox,
        name: &str,
        data: Object,
    ) -> Result<(), Status> {
        let found = self.own_element(id, name, |k| k == NodeKind::Event)?;
        self.state()
            .deliver(out, &found.name, EventKind::General, &data, None);
        Ok(())
    }

    /// Publishes a value change the provider made (P9).
    pub(crate) fn publish_change(
        &self,
        id: MountId,
        out: &dyn Outbox,
        name: &str,
        old: Value,
        new: Value,
    ) -> Result<(), Status> {
        let found = self.own_element(id, name, |k| matches!(k, NodeKind::Property(_)))?;
        let mut state = self.state();
        if state.watched(&found.name) {
            state.values.insert(found.name.clone(), new.clone());
        }
        state.deliver_change(out, &found.name, &old, &new, out.component());
        Ok(())
    }

    /// Reads a property again and publishes a value change if it differs
    /// from the last value subscribers saw (P11).
    pub(crate) async fn notify_changed(
        &self,
        id: MountId,
        out: &dyn Outbox,
        name: &str,
    ) -> Result<(), Status> {
        let found = self.own_element(
            id,
            name,
            |k| matches!(k, NodeKind::Property(a) if a.readable()),
        )?;
        let handler = {
            let state = self.state();
            if !state.watched(&found.name) {
                return Ok(());
            }
            state.handler(Some(id)).ok_or(Status::ElementDoesNotExist)?
        };
        self.check_change(out, &handler, found, out.component())
            .await;
        Ok(())
    }

    /// Reads a watched property and publishes a change against the recorded
    /// value. Without a record, the value read becomes the record.
    async fn check_change(&self, out: &dyn Outbox, handler: &Handler, found: Found, by: &str) {
        let Some(new) = self.read(handler, &found, internal_cx(out)).await else {
            return;
        };
        let mut state = self.state();
        if !state.watched(&found.name) {
            return;
        }
        if let Some(old) = state.values.insert(found.name.clone(), new.clone()) {
            state.deliver_change(out, &found.name, &old, &new, by);
        }
    }

    /// Before a set: records the current value of each watched parameter
    /// that has no record yet, so the change can be reported.
    pub(super) async fn before_set(&self, out: &dyn Outbox, handler: &Handler, changes: &[Change]) {
        let missing: Vec<(Found, Handler)> = {
            let state = self.state();
            changes
                .iter()
                .filter(|c| {
                    state.watched(c.name()) && !state.values.contains_key(c.name().as_str())
                })
                .filter_map(|c| {
                    let r = state.model.resolve(c.name()).ok()?;
                    let Target::Node(n) = r.target else {
                        return None;
                    };
                    matches!(state.model.tree.node(n).kind, NodeKind::Property(a) if a.readable())
                        .then(|| {
                            let found = Found {
                                name: r.canonical,
                                node: n,
                                instances: r.instances,
                            };
                            (found, handler.clone())
                        })
                })
                .collect()
        };
        self.record_baselines(out, missing).await;
    }

    /// Reads each property, outside the lock, and records its value as the
    /// one subscribers last saw, unless something recorded one meanwhile.
    async fn record_baselines(&self, out: &dyn Outbox, reads: Vec<(Found, Handler)>) {
        for (found, handler) in reads {
            if let Some(v) = self.read(&handler, &found, internal_cx(out)).await {
                self.state().values.entry(found.name).or_insert(v);
            }
        }
    }

    /// After a set: publishes the changes, by the caller.
    pub(super) async fn after_set(
        &self,
        out: &dyn Outbox,
        handler: &Handler,
        changes: Vec<(PropertyName, Vec<u32>)>,
        by: &str,
    ) {
        for (name, instances) in changes {
            let found = {
                let state = self.state();
                if !state.values.contains_key(name.as_str()) {
                    continue;
                }
                let Ok(r) = state.model.resolve(&name) else {
                    continue;
                };
                let Target::Node(n) = r.target else { continue };
                Found {
                    name: r.canonical,
                    node: n,
                    instances,
                }
            };
            self.check_change(out, handler, found, by).await;
        }
    }

    /// Polls the watched properties declared with `Schema::poll` that are
    /// due (P10).
    pub(crate) async fn poll_once(&self, out: &dyn Outbox, now: Instant) {
        let due: Vec<(Found, Handler)> = {
            let mut state = self.state();
            let mut candidates: Vec<(Found, Duration)> = Vec::new();
            for sub in state.subs.iter().filter(|s| s.kind == SubKind::Value) {
                for f in state.covered(&sub.info.event) {
                    if let Some(every) = state.model.tree.node(f.node).poll {
                        if !candidates.iter().any(|(c, _)| c.name == f.name) {
                            candidates.push((f, every));
                        }
                    }
                }
            }
            let mut due = Vec::new();
            for (found, every) in candidates {
                let next = state
                    .poll_due
                    .entry(found.name.clone())
                    .or_insert(now + every);
                if now >= *next {
                    *next = now + every;
                    if let Some(h) = state.handler_of(&found) {
                        due.push((found, h));
                    }
                }
            }
            due
        };
        for (found, handler) in due {
            self.check_change(out, &handler, found, out.component())
                .await;
        }
    }

    /// Starts the poller the first time a subscription arrives.
    pub(crate) fn start_poller(&self) -> bool {
        !self
            .poller_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
    }
}

/// Polls the connection's providers until the connection is gone.
pub(crate) async fn poller(core: Weak<Core>) {
    loop {
        tokio::time::sleep(POLL_TICK).await;
        let Some(core) = core.upgrade() else { return };
        if core.is_closed() {
            return;
        }
        core.registry.poll_once(&*core, Instant::now()).await;
    }
}

#[cfg(test)]
mod tests;
