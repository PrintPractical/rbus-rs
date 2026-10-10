//! Consumer operations: get, set and invoke.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::time::Duration;

use indexmap::IndexMap;
use rbus_wire::Limits;
use rbus_wire::body::{GetRequest, GetResponse, RpcRequest, RpcResponse, SetRequest, SetResponse};
use rbus_wire::frame::{Flags, Frame};
use rbus_wire::message::{Body, Method, TraceContext, request_payload, split_method_payload};
use rbus_wire::value::{Property, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{Instrument, debug_span};

use crate::bus::Rbus;
use crate::convert::{from_object, to_value};
use crate::error::{Error, Result, Status, TypeError, kind_of};
use crate::name::{IntoName, MethodName, Name, PropertyName};
use crate::types::{IntoValue, RbusType};

/// Values returned by [`Rbus::get_many`], in the order they were asked for.
pub type Values = IndexMap<PropertyName, Value>;

/// Decodes an rbus response, or reports why there is none.
fn response<B: Body>(frame: &Frame, limits: &Limits, name: impl FnOnce() -> Name) -> Result<B> {
    if frame.header.flags.contains(Flags::UNDELIVERABLE) {
        return Err(Error::NotFound(name()));
    }
    let (mut body, _meta) = split_method_payload(&frame.payload, limits)?;
    Ok(B::decode(&mut body)?)
}

fn status_of(code: i32) -> Option<Status> {
    let status = Status::from_wire(code);
    (!status.is_success()).then_some(status)
}

/// Groups `names` by owner, keeping first-appearance order.
fn group_by_owner<T: Clone>(items: &[T], owners: Vec<String>) -> Vec<Vec<T>> {
    let mut groups: IndexMap<String, Vec<T>> = IndexMap::new();
    for (item, owner) in items.iter().zip(owners) {
        groups.entry(owner).or_default().push(item.clone());
    }
    groups.into_values().collect()
}

impl Rbus {
    fn trace() -> TraceContext {
        TraceContext::default()
    }

    /// Sends an rbus request now; the returned future waits for the reply.
    pub(crate) fn start_call<B: Body, Q: Body>(
        &self,
        topic: &str,
        method: Method,
        body: &Q,
        timeout: Duration,
        op: &'static str,
        name: Name,
    ) -> Result<impl Future<Output = Result<B>> + Send + use<B, Q>> {
        let payload = request_payload(method, Self::trace(), body);
        let pending = self.inner.conn.rbus_request(topic, payload)?;
        let limits = self.inner.conn.limits;
        Ok(async move {
            let frame = pending.wait(timeout, op).await?;
            response(&frame, &limits, || name)
        })
    }

    /// The owning component of each name, from the broker (`_trace_origin_object`).
    async fn owners(&self, names: &[PropertyName]) -> Result<Vec<String>> {
        let items = self.trace_origin(names).await?;
        if items.len() == names.len() && items.iter().all(|o| !o.is_empty()) {
            return Ok(items);
        }
        // The broker leaves unknown names out of the reply, so it can't say
        // which is missing. Ask one by one to name it.
        let mut owners = Vec::with_capacity(names.len());
        for name in names {
            match self.trace_origin(std::slice::from_ref(name)).await?.pop() {
                Some(owner) if !owner.is_empty() => owners.push(owner),
                _ => return Err(Error::NotFound(name.clone().into())),
            }
        }
        Ok(owners)
    }

    /// Gets one property.
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
    /// let uptime: u32 = bus.get("Device.DeviceInfo.UpTime").await?;
    /// let any: rbus::Value = bus.get("Device.DeviceInfo.UpTime").await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Fails with [`Error::NotFound`] when no provider owns the name, and
    /// [`Error::Type`] when the value doesn't convert to `T`.
    pub async fn get<T: RbusType>(&self, name: impl IntoName<PropertyName>) -> Result<T> {
        let name = name.into_name()?;
        let span = debug_span!("rbus.get", name = %name);
        async {
            let resp = self.start_get(std::slice::from_ref(&name))?.await?;
            let value = resp
                .properties
                .into_iter()
                .find(|p| p.name == *name)
                .ok_or_else(|| Error::Protocol(format!("no value for {name} in the get response")))?
                .value;
            Ok(T::from_value(value)?)
        }
        .instrument(span)
        .await
    }

    /// Sends one GET to the owner of `names[0]`; all names must share that
    /// owner. The returned future waits for the reply.
    fn start_get(
        &self,
        names: &[PropertyName],
    ) -> Result<impl Future<Output = Result<GetResponse>> + Send + use<>> {
        let first = names[0].clone();
        let body = GetRequest {
            requesting_component: self.component().to_owned(),
            names: names.iter().map(|n| n.to_string()).collect(),
        };
        let call = self.start_call::<GetResponse, _>(
            &first,
            Method::GetParameterValues,
            &body,
            self.inner.timeouts.get,
            "get",
            first.clone().into(),
        )?;
        Ok(async move {
            let resp = call.await?;
            match status_of(resp.status) {
                None => Ok(resp),
                Some(status) => Err(Error::Status {
                    name: first.into(),
                    status,
                }),
            }
        })
    }

    /// Gets several properties, with one request per owning component.
    /// The result keeps the order of `names`.
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
    /// let values = bus.get_many(["Device.A.X", "Device.B.Y"]).await?;
    /// let x = &values["Device.A.X"];
    /// # Ok(()) }
    /// ```
    ///
    /// A component that answers with an error fails the whole call with
    /// [`Error::Status`], naming the first property sent to it.
    pub async fn get_many<I, N>(&self, names: I) -> Result<Values>
    where
        I: IntoIterator<Item = N>,
        N: IntoName<PropertyName>,
    {
        let names: Vec<PropertyName> = names
            .into_iter()
            .map(IntoName::into_name)
            .collect::<Result<_, _>>()?;
        let span = debug_span!("rbus.get_many", count = names.len());
        async {
            let groups = match names.len() {
                0 => return Ok(Values::new()),
                1 => vec![names.clone()],
                _ => group_by_owner(&names, self.owners(&names).await?),
            };
            // Every request goes out before the first reply is awaited.
            let pending = groups
                .iter()
                .map(|group| self.start_get(group))
                .collect::<Result<Vec<_>>>()?;
            let mut received: IndexMap<String, Value> = IndexMap::new();
            for batch in pending {
                for p in batch.await?.properties {
                    received.insert(p.name, p.value);
                }
            }
            names
                .into_iter()
                .map(|name| match received.get(name.as_str()) {
                    Some(v) => Ok((name, v.clone())),
                    None => Err(Error::Protocol(format!(
                        "no value for {name} in the get response"
                    ))),
                })
                .collect()
        }
        .instrument(span)
        .await
    }

    /// Sets one property.
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
    /// bus.set("Device.WiFi.Radio.1.Enable", true).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Fails with [`Error::SetFailed`] when the provider refuses the value.
    pub async fn set(
        &self,
        name: impl IntoName<PropertyName>,
        value: impl IntoValue,
    ) -> Result<()> {
        let name = name.into_name()?;
        let span = debug_span!("rbus.set", name = %name);
        let value = value.into_value();
        async {
            self.set_batch(vec![(name, value)], self.inner.timeouts.set)
                .await
        }
        .instrument(span)
        .await
    }

    /// Starts a transaction that sets several properties. Await it to
    /// send it.
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
    /// bus.set_many()
    ///     .set("Device.WiFi.Radio.1.Enable", true)
    ///     .set("Device.WiFi.Radio.1.Channel", 36u32)
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Each owning component gets one request carrying all of its properties,
    /// in the order the components first appear. The first failure stops the
    /// transaction and is reported as [`Error::SetFailed`] naming the failing
    /// property. Components already updated keep their new values: RBUS
    /// has no cross-component rollback.
    pub fn set_many(&self) -> SetMany<'_> {
        SetMany {
            bus: self,
            props: Ok(Vec::new()),
        }
    }

    /// One SET to the owner of the first property; all must share that owner.
    async fn set_batch(&self, props: Vec<(PropertyName, Value)>, timeout: Duration) -> Result<()> {
        let first = props[0].0.clone();
        if let Some((name, _)) = props.iter().find(|(_, v)| matches!(v, Value::None)) {
            return Err(Error::SetFailed {
                name: name.clone().into(),
                status: Status::InvalidInput,
            });
        }
        let body = SetRequest {
            session_id: 0,
            component: self.component().to_owned(),
            rollback: false,
            properties: props
                .into_iter()
                .map(|(n, v)| Property::new(n.to_string(), v))
                .collect(),
            // The commit flag is a legacy session detail; rbus providers apply
            // each request as a whole.
            commit: true,
        };
        let resp: SetResponse = self
            .start_call(
                &first,
                Method::SetParameterValues,
                &body,
                timeout,
                "set",
                first.clone().into(),
            )?
            .await?;
        match status_of(resp.status) {
            None => Ok(()),
            Some(status) => Err(Error::SetFailed {
                name: resp
                    .failed_element
                    .and_then(|n| Name::parse(n).ok())
                    .unwrap_or_else(|| first.into()),
                status,
            }),
        }
    }

    async fn run_set_many(&self, props: Vec<(PropertyName, Value)>) -> Result<()> {
        let timeout = self.inner.timeouts.set_many;
        let groups = match props.len() {
            0 => return Ok(()),
            1 => vec![props],
            _ => {
                let names: Vec<PropertyName> = props.iter().map(|(n, _)| n.clone()).collect();
                group_by_owner(&props, self.owners(&names).await?)
            }
        };
        for group in groups {
            self.set_batch(group, timeout).await?;
        }
        Ok(())
    }

    /// Invokes a method and waits for its result. Arguments and results
    /// go through serde ([`crate::to_object`]); use `()` for none, and
    /// [`crate::Object`] or [`Value`] to handle them dynamically.
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
    /// #[derive(serde::Serialize)]
    /// struct Args { delay: u32 }
    /// #[derive(serde::Deserialize)]
    /// struct Reply { status: String }
    ///
    /// let reply: Reply = bus.invoke("Device.Reboot()", Args { delay: 5 }).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// A failing method is reported as [`Error::Method`], with the provider's
    /// `error_string` and its whole output object.
    pub async fn invoke<A, R>(&self, method: impl IntoName<MethodName>, args: A) -> Result<R>
    where
        A: Serialize,
        R: DeserializeOwned,
    {
        let method = method.into_name()?;
        let span = debug_span!("rbus.invoke", method = %method);
        let input = match to_value(&args)? {
            Value::None => None,
            Value::Object(o) => Some(o),
            v => {
                return Err(TypeError::Mismatch {
                    expected: "method arguments as a struct, map or ()",
                    found: kind_of(&v),
                }
                .into());
            }
        };
        async {
            let body = RpcRequest {
                session_id: 0,
                method: method.to_string(),
                input,
            };
            let resp: RpcResponse = self
                .start_call(
                    &method,
                    Method::Rpc,
                    &body,
                    self.inner.timeouts.invoke,
                    "invoke",
                    method.clone().into(),
                )?
                .await?;
            let output = resp.output.unwrap_or_default();
            match status_of(resp.status) {
                None => Ok(from_object(output)?),
                Some(status) => Err(Error::Method {
                    message: match output.get("error_string") {
                        Some(Value::String(s)) => Some(s.clone()),
                        _ => None,
                    },
                    method: method.clone(),
                    status,
                    detail: Box::new(output),
                }),
            }
        }
        .instrument(span)
        .await
    }
}

/// A set transaction under construction; made by [`Rbus::set_many`]. Nothing
/// is sent until it is awaited.
#[must_use = "a transaction does nothing until awaited"]
pub struct SetMany<'a> {
    bus: &'a Rbus,
    props: Result<Vec<(PropertyName, Value)>>,
}

impl std::fmt::Debug for SetMany<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Option<Vec<&str>> = self
            .props
            .as_ref()
            .ok()
            .map(|p| p.iter().map(|(n, _)| n.as_str()).collect());
        f.debug_struct("SetMany").field("names", &names).finish()
    }
}

impl SetMany<'_> {
    /// Adds a property to set. An invalid name is reported when the
    /// transaction is awaited.
    pub fn set(mut self, name: impl IntoName<PropertyName>, value: impl IntoValue) -> Self {
        if let Ok(props) = &mut self.props {
            match name.into_name() {
                Ok(name) => props.push((name, value.into_value())),
                Err(e) => self.props = Err(e.into()),
            }
        }
        self
    }
}

impl<'a> IntoFuture for SetMany<'a> {
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let SetMany { bus, props } = self;
        Box::pin(async move {
            let props = props?;
            let span = debug_span!("rbus.set_many", count = props.len());
            bus.run_set_many(props).instrument(span).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_keep_first_appearance_order() {
        let items = ["a", "b", "c", "d"];
        let owners = ["X", "Y", "X", "Z"].map(String::from).to_vec();
        assert_eq!(
            group_by_owner(&items, owners),
            vec![vec!["a", "c"], vec!["b"], vec!["d"]]
        );
    }

    #[test]
    fn statuses() {
        assert_eq!(status_of(0), None);
        assert_eq!(status_of(100), None);
        assert_eq!(status_of(27), Some(Status::NotWritable));
    }
}
