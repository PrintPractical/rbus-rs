//! Subscriptions as typed streams (E1–E5, E9, E10, D12).
//!
//! The kind of element decides the item type:
//!
//! - a property gives [`ValueChange<T>`] ([`Rbus::subscribe_value`]);
//! - a table gives [`RowEvent`] ([`Rbus::subscribe_table`]);
//! - an event gives your serde type ([`Rbus::subscribe`]).
//!
//! Each subscription is registered with the provider under its own id (the C
//! `componentId`), so events reach exactly one stream. Dropping the stream
//! unsubscribes.

use std::future::{Future, IntoFuture, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use rbus_wire::body::{
    InitialValue, StatusResponse, SubscribeOptions, SubscribeRequest, SubscribeResponse,
};
use rbus_wire::event::{Event, EventKind};
use rbus_wire::frame::{Flags, Frame};
use rbus_wire::message::{Body, Method, TraceContext, request_payload, split_method_payload};
use rbus_wire::value::Value;
use serde::de::DeserializeOwned;
use tracing::{Instrument, debug, debug_span, warn};

use crate::bus::Rbus;
use crate::conn::{Core, Pending, SubEntry};
use crate::convert::from_object;
use crate::error::{Error, Result, Status, TypeError};
use crate::name::{EventName, IntoName, Name, NameError, PropertyName, TablePath};
use crate::queue::{Overflow, Popped, Queue};
use crate::retry::Retry;
use crate::table::RowId;
use crate::types::RbusType;

/// How long resubscription keeps retrying a provider that is still starting.
const RESUBSCRIBE_FOR: Duration = Duration::from_secs(30);
/// Default buffer of a subscription stream.
const DEFAULT_BUFFER: usize = 256;

/// Why a stream item is not an event.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum EventError {
    /// The consumer fell behind and this many events were dropped (D12).
    #[error("{0} events were dropped because the consumer fell behind")]
    Lagged(u64),
    /// An event arrived that doesn't convert to the stream's item type.
    #[error("event {name}: {error}")]
    Decode {
        /// The event's name.
        name: String,
        /// Why it didn't convert.
        error: TypeError,
    },
}

/// A property's new value (E3), or its current value when the subscription
/// asked for one (E9).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ValueChange<T> {
    /// The property that changed: the instance name for a wildcard subscription.
    pub name: String,
    /// The new value.
    pub new: T,
    /// The previous value; `None` for the initial value.
    pub old: Option<T>,
    /// The component that made the change, when the provider says.
    pub by: Option<String>,
}

/// A row added to or removed from a table (E4).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RowEvent {
    /// A row was added.
    Added {
        /// The table, as the provider names it (`Device.Hosts.Host.`).
        table: String,
        /// The new row.
        row: RowId,
    },
    /// A row was removed.
    Removed {
        /// The table.
        table: String,
        /// The removed row; its alias is not known.
        row: RowId,
    },
}

impl RowEvent {
    /// The row.
    pub fn row(&self) -> &RowId {
        match self {
            RowEvent::Added { row, .. } | RowEvent::Removed { row, .. } => row,
        }
    }

    /// The table.
    pub fn table(&self) -> &str {
        match self {
            RowEvent::Added { table, .. } | RowEvent::Removed { table, .. } => table,
        }
    }
}

type Decode<I> = fn(Event) -> Result<I, TypeError>;

fn property<'a>(data: &'a rbus_wire::value::Object, name: &str) -> Option<&'a Value> {
    data.get(name).filter(|v| !matches!(v, Value::None))
}

fn decode_value<T: RbusType>(ev: Event) -> Result<ValueChange<T>, TypeError> {
    let data = ev.data.unwrap_or_default();
    let key = if ev.kind == EventKind::InitialValue {
        "initialValue"
    } else {
        "value"
    };
    let new = property(&data, key)
        .cloned()
        .ok_or_else(|| TypeError::Custom(format!("{:?} event without {key:?}", ev.kind)))?;
    let old = property(&data, "oldValue")
        .cloned()
        .map(T::from_value)
        .transpose()?;
    let by = match property(&data, "by") {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    Ok(ValueChange {
        name: ev.name,
        new: T::from_value(new)?,
        old,
        by,
    })
}

/// The instance number at the end of a row name (`Device.Hosts.Host.7`).
fn instance_of(row_name: &str) -> Option<u32> {
    row_name
        .trim_end_matches('.')
        .rsplit('.')
        .next()?
        .parse()
        .ok()
}

fn decode_row(ev: Event) -> Result<RowEvent, TypeError> {
    let data = ev.data.unwrap_or_default();
    let row_name = match property(&data, "rowName") {
        Some(Value::String(s)) => s.clone(),
        Some(v) => return Err(TypeError::mismatch("row name string", v)),
        None => return Err(TypeError::Custom("row event without rowName".into())),
    };
    let inst = match property(&data, "instNum") {
        Some(v) => u32::from_value(v.clone())?,
        None => instance_of(&row_name)
            .ok_or_else(|| TypeError::Custom(format!("no instance number in {row_name:?}")))?,
    };
    let alias = match property(&data, "alias") {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    };
    let row = RowId { inst, alias };
    match ev.kind {
        EventKind::ObjectCreated => Ok(RowEvent::Added {
            table: ev.name,
            row,
        }),
        EventKind::ObjectDeleted => Ok(RowEvent::Removed {
            table: ev.name,
            row,
        }),
        kind => Err(TypeError::Custom(format!(
            "{kind:?} event on a table subscription"
        ))),
    }
}

fn decode_general<T: DeserializeOwned>(ev: Event) -> Result<T, TypeError> {
    from_object(ev.data.unwrap_or_default())
}

fn subscribe_request(core: &Core, name: &str, id: i32, initial: bool) -> SubscribeRequest {
    SubscribeRequest {
        event_name: name.to_owned(),
        subscriber: core.inbox().to_owned(),
        options: Some(SubscribeOptions {
            component_id: id,
            interval: 0,
            duration: 0,
            filter: None,
        }),
        publish_on_subscribe: initial,
        raw_data: false,
    }
}

/// Sends a subscribe or unsubscribe request now.
fn start(core: &Arc<Core>, method: Method, name: &str, id: i32, initial: bool) -> Result<Pending> {
    let body = subscribe_request(core, name, id, initial);
    core.rbus_request(
        name,
        request_payload(method, TraceContext::default(), &body),
    )
}

fn check_delivered(frame: &Frame, name: &str) -> Result<()> {
    if frame.header.flags.contains(Flags::UNDELIVERABLE) {
        return Err(Error::NotFound(Name::parse(name.to_owned())?));
    }
    Ok(())
}

fn status_error(name: &str, code: i32) -> Result<()> {
    let status = Status::from_wire(code);
    if status.is_success() {
        return Ok(());
    }
    Err(Error::Status {
        name: Name::parse(name.to_owned())?,
        status,
    })
}

async fn subscribe_once(
    core: &Arc<Core>,
    name: &str,
    id: i32,
    initial: bool,
) -> Result<SubscribeResponse> {
    let frame = start(core, Method::Subscribe, name, id, initial)?
        .wait(core.timeouts.subscribe, "subscribe")
        .await?;
    check_delivered(&frame, name)?;
    let (mut body, _meta) = split_method_payload(&frame.payload, &core.limits)?;
    let resp = SubscribeResponse::decode_for(&mut body, initial)?;
    status_error(name, resp.status)?;
    Ok(resp)
}

async fn unsubscribe_once(core: &Arc<Core>, name: &str, id: i32) -> Result<()> {
    let frame = start(core, Method::Unsubscribe, name, id, false)?
        .wait(core.timeouts.subscribe, "unsubscribe")
        .await?;
    // Like C, unsubscribing from a provider that is gone succeeds.
    if frame.header.flags.contains(Flags::UNDELIVERABLE) {
        return Ok(());
    }
    let (mut body, _meta) = split_method_payload(&frame.payload, &core.limits)?;
    status_error(name, StatusResponse::decode(&mut body)?.status)
}

/// Sends an unsubscribe for every live subscription without waiting for the
/// replies, and forgets them (L13).
pub(crate) fn unsubscribe_all(core: &Arc<Core>) {
    for (id, name) in core.sub_names() {
        core.remove_sub(id);
        drop(start(core, Method::Unsubscribe, &name, id, false));
    }
}

/// Re-sends every subscription after a connect advisory. Providers that still
/// have one answer "already exists", which is fine; providers that are still
/// registering their elements are retried for a while (D11).
pub(crate) async fn resubscribe_all(core: &Arc<Core>) {
    let mut todo = core.sub_names();
    if todo.is_empty() {
        return;
    }
    debug!(count = todo.len(), "resubscribing after a connect advisory");
    let deadline = tokio::time::Instant::now() + RESUBSCRIBE_FOR;
    let mut delay = Duration::from_millis(100);
    loop {
        let mut again = Vec::new();
        for (id, name) in todo {
            match subscribe_once(core, &name, id, false).await {
                Ok(_) => debug!(name, "resubscribed"),
                Err(Error::Status {
                    status: Status::SubscriptionAlreadyExist,
                    ..
                }) => {}
                Err(Error::NotFound(_)) => again.push((id, name)),
                Err(Error::Disconnected) => return,
                Err(e) => warn!(name, error = %e, "resubscribe failed"),
            }
        }
        let live: Vec<i32> = core.sub_names().into_iter().map(|(id, _)| id).collect();
        again.retain(|(id, _)| live.contains(id));
        if again.is_empty() || tokio::time::Instant::now() + delay > deadline {
            for (_, name) in &again {
                warn!(name, "provider did not come back; subscription is idle");
            }
            return;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(2));
        todo = again;
    }
}

impl Rbus {
    /// Subscribes to a property's value changes (E3). The provider detects the
    /// changes.
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
    /// let mut enable = bus
    ///     .subscribe_value::<bool>("Device.WiFi.Radio.1.Enable")
    ///     .initial_value()
    ///     .await?;
    /// while let Some(item) = enable.next().await {
    ///     match item {
    ///         Ok(change) => println!("{:?} -> {}", change.old, change.new),
    ///         Err(e) => eprintln!("{e}"),
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    pub fn subscribe_value<T: RbusType>(
        &self,
        name: impl IntoName<PropertyName>,
    ) -> SubscribeBuilder<'_, ValueChange<T>> {
        SubscribeBuilder::new(self, name.into_name().map(Name::from), decode_value::<T>)
    }

    /// Subscribes to rows being added to and removed from a table (E4).
    pub fn subscribe_table(
        &self,
        table: impl IntoName<TablePath>,
    ) -> SubscribeBuilder<'_, RowEvent> {
        SubscribeBuilder::new(self, table.into_name().map(Name::from), decode_row)
    }

    /// Subscribes to a provider-defined event (E5); its data is decoded into
    /// `T` with serde. Use [`crate::Object`] to take it as it comes.
    pub fn subscribe<T: DeserializeOwned + 'static>(
        &self,
        event: impl IntoName<EventName>,
    ) -> SubscribeBuilder<'_, T> {
        SubscribeBuilder::new(self, event.into_name().map(Name::from), decode_general::<T>)
    }
}

/// A subscription being set up; await it to subscribe. Made by
/// [`Rbus::subscribe_value`], [`Rbus::subscribe_table`] and [`Rbus::subscribe`].
#[must_use = "a subscription does nothing until awaited"]
pub struct SubscribeBuilder<'a, I> {
    bus: &'a Rbus,
    name: Result<Name, NameError>,
    decode: Decode<I>,
    initial: bool,
    retry: Option<Retry>,
    buffer: usize,
    overflow: Overflow,
}

impl<I> std::fmt::Debug for SubscribeBuilder<'_, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscribeBuilder")
            .field("name", &self.name.as_ref().ok())
            .field("initial_value", &self.initial)
            .field("retry", &self.retry)
            .field("buffer", &self.buffer)
            .field("overflow", &self.overflow)
            .finish()
    }
}

impl<'a, I> SubscribeBuilder<'a, I> {
    fn new(bus: &'a Rbus, name: Result<Name, NameError>, decode: Decode<I>) -> Self {
        SubscribeBuilder {
            bus,
            name,
            decode,
            initial: false,
            retry: None,
            buffer: DEFAULT_BUFFER,
            overflow: Overflow::default(),
        }
    }

    /// Keep trying while no provider owns the name (E10). Without this, a
    /// missing provider fails with [`Error::NotFound`].
    pub fn retry(mut self, retry: Retry) -> Self {
        self.retry = Some(retry);
        self
    }

    /// How many events to hold for a slow consumer (default 256).
    pub fn buffer(mut self, events: usize) -> Self {
        self.buffer = events;
        self
    }

    /// What to do when the buffer is full (default: drop the oldest).
    pub fn overflow(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }
}

impl<T> SubscribeBuilder<'_, ValueChange<T>> {
    /// Make the current value the first item (E9).
    pub fn initial_value(mut self) -> Self {
        self.initial = true;
        self
    }
}

/// Unregisters a subscription whose setup didn't finish (an error, or the
/// future was dropped).
struct SetupGuard<'a> {
    core: &'a Arc<Core>,
    id: i32,
    name: String,
    armed: bool,
}

impl Drop for SetupGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.core.remove_sub(self.id);
            // The provider may have subscribed us already.
            drop(start(
                self.core,
                Method::Unsubscribe,
                &self.name,
                self.id,
                false,
            ));
        }
    }
}

impl<'a, I: Send + 'static> IntoFuture for SubscribeBuilder<'a, I> {
    type Output = Result<Subscription<I>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let name = self.name?;
            let span = debug_span!("rbus.subscribe", name = %name);
            async {
                let core = &self.bus.inner.conn;
                core.ensure_advisories()?;
                let id = core.next_sub_id();
                let queue = Arc::new(Queue::new(self.buffer, self.overflow));
                core.add_sub(
                    id,
                    SubEntry {
                        event_name: name.to_string(),
                        queue: queue.clone(),
                        ready: false,
                    },
                );
                let mut guard = SetupGuard {
                    core,
                    id,
                    name: name.to_string(),
                    armed: true,
                };
                let started = tokio::time::Instant::now();
                let mut delay = self.retry.map(|r| r.first_delay());
                let resp = loop {
                    match subscribe_once(core, &name, id, self.initial).await {
                        Err(Error::NotFound(n)) => {
                            let (Some(retry), Some(d)) = (self.retry, delay) else {
                                guard.armed = false;
                                core.remove_sub(id);
                                return Err(Error::NotFound(n));
                            };
                            if retry.gives_up(started.elapsed(), d) {
                                guard.armed = false;
                                core.remove_sub(id);
                                return Err(Error::NotFound(n));
                            }
                            debug!(?d, "no provider yet; retrying");
                            tokio::time::sleep(d).await;
                            delay = Some(retry.next_delay(d));
                        }
                        other => break other?,
                    }
                };
                guard.armed = false;
                core.mark_ready(id);
                if let InitialValue::Value(ev) = resp.initial {
                    queue.push_front(*ev);
                }
                Ok(Subscription {
                    bus: self.bus.clone(),
                    id,
                    name,
                    queue,
                    decode: self.decode,
                    active: true,
                })
            }
            .instrument(span)
            .await
        })
    }
}

/// A stream of events from one subscription. Ends when the connection closes.
/// Dropping it unsubscribes (E2).
///
/// It implements [`Stream`]; [`Subscription::next`] works without importing
/// a stream extension trait.
#[must_use = "dropping a subscription unsubscribes"]
pub struct Subscription<I> {
    bus: Rbus,
    id: i32,
    name: Name,
    queue: Arc<Queue<Event>>,
    decode: Decode<I>,
    active: bool,
}

impl<I> Subscription<I> {
    /// The name subscribed to.
    pub fn name(&self) -> &Name {
        &self.name
    }

    /// The next event; `None` once the connection is closed.
    pub async fn next(&mut self) -> Option<Result<I, EventError>> {
        poll_fn(|cx| self.poll_item(cx)).await
    }

    fn poll_item(&self, cx: &mut Context<'_>) -> Poll<Option<Result<I, EventError>>> {
        self.queue.poll_pop(cx).map(|popped| {
            popped.map(|p| match p {
                Popped::Lagged(n) => Err(EventError::Lagged(n)),
                Popped::Item(ev) => {
                    let name = ev.name.clone();
                    (self.decode)(ev).map_err(|error| EventError::Decode { name, error })
                }
            })
        })
    }

    /// Unsubscribes and waits for the provider to confirm (E2).
    pub async fn unsubscribe(mut self) -> Result<()> {
        self.active = false;
        let core = &self.bus.inner.conn;
        core.remove_sub(self.id);
        unsubscribe_once(core, &self.name, self.id)
            .instrument(debug_span!("rbus.unsubscribe", name = %self.name))
            .await
    }
}

impl<I> Stream for Subscription<I> {
    type Item = Result<I, EventError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_item(cx)
    }
}

impl<I> Drop for Subscription<I> {
    fn drop(&mut self) {
        if self.active {
            let core = &self.bus.inner.conn;
            if core.remove_sub(self.id).is_some() && !core.is_closed() {
                // Fire and forget: the reply, if any, is dropped as late.
                drop(start(core, Method::Unsubscribe, &self.name, self.id, false));
            }
        }
    }
}

impl<I> std::fmt::Debug for Subscription<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("name", &self.name)
            .field("id", &self.id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbus_wire::value::Object;

    fn event(kind: EventKind, data: Object) -> Event {
        Event {
            name: "Device.T.".into(),
            kind,
            data: Some(data),
            filter: None,
            interval: 0,
            duration: 0,
            component_id: 1,
        }
    }

    #[test]
    fn value_changes_and_initial_values() {
        let ch: ValueChange<i32> = decode_value(event(
            EventKind::ValueChanged,
            Object::new()
                .with("value", 2i32)
                .with("oldValue", 1i32)
                .with("by", "comp"),
        ))
        .unwrap();
        assert_eq!(
            (ch.new, ch.old, ch.by.as_deref()),
            (2, Some(1), Some("comp"))
        );
        let init: ValueChange<i32> = decode_value(event(
            EventKind::InitialValue,
            Object::new().with("initialValue", 7i32).with("by", ""),
        ))
        .unwrap();
        assert_eq!((init.new, init.old, init.by), (7, None, None));
        assert!(decode_value::<i32>(event(EventKind::ValueChanged, Object::new())).is_err());
        assert!(
            decode_value::<i32>(event(
                EventKind::ValueChanged,
                Object::new().with("value", "x")
            ))
            .is_err()
        );
    }

    #[test]
    fn row_events() {
        let added = decode_row(event(
            EventKind::ObjectCreated,
            Object::new()
                .with("rowName", "Device.T.3")
                .with("instNum", 3u32)
                .with("alias", "a"),
        ))
        .unwrap();
        assert_eq!(
            added,
            RowEvent::Added {
                table: "Device.T.".into(),
                row: RowId {
                    inst: 3,
                    alias: Some("a".into())
                }
            }
        );
        assert_eq!(added.table(), "Device.T.");
        let removed = decode_row(event(
            EventKind::ObjectDeleted,
            Object::new().with("rowName", "Device.T.4"),
        ))
        .unwrap();
        assert_eq!(
            removed.row(),
            &RowId {
                inst: 4,
                alias: None
            }
        );
        assert_eq!(removed.table(), "Device.T.");
        for bad in [
            event(EventKind::ObjectDeleted, Object::new()),
            event(EventKind::ObjectDeleted, Object::new().with("rowName", 5u8)),
            event(
                EventKind::ObjectDeleted,
                Object::new().with("rowName", "Device.T.x"),
            ),
            event(
                EventKind::General,
                Object::new().with("rowName", "Device.T.1"),
            ),
        ] {
            assert!(decode_row(bad).is_err());
        }
        assert_eq!(instance_of("Device.T.12."), Some(12));
    }

    #[test]
    fn general_events_and_errors() {
        #[derive(serde::Deserialize)]
        struct Alarm {
            level: u8,
        }
        let a: Alarm =
            decode_general(event(EventKind::General, Object::new().with("level", 3u8))).unwrap();
        assert_eq!(a.level, 3);
        let mut no_data = event(EventKind::General, Object::new());
        no_data.data = None;
        let () = decode_general(no_data).unwrap();
        assert_eq!(
            EventError::Lagged(3).to_string(),
            "3 events were dropped because the consumer fell behind"
        );
        let e = EventError::Decode {
            name: "E!".into(),
            error: TypeError::mismatch("u8", &Value::Bool(true)),
        };
        assert_eq!(e.to_string(), "event E!: expected u8, found bool");
    }
}
