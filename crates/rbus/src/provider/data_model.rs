//! A provider built at run time from closures (design.md 4.9).

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rbus_wire::value::{Object, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::name::{MethodName, PropertyName, TablePath};
use crate::provider::derive::{BoxFuture, value_as};
use crate::provider::{Access, Context, Fault, Provider, Rows, Schema, SetFault, Transaction};
use crate::subscribe::RowId;
use crate::types::{IntoValue, RbusType};

type Getter = Arc<dyn Fn(Context) -> BoxFuture<'static, Result<Value, Fault>> + Send + Sync>;
/// Converts the value, then returns the setter's future.
type Setter = Arc<
    dyn Fn(Context, &Value) -> Result<BoxFuture<'static, Result<(), Fault>>, Fault> + Send + Sync,
>;
type Method =
    Arc<dyn Fn(Context, Object) -> BoxFuture<'static, Result<Object, Fault>> + Send + Sync>;

enum Entry {
    Property {
        get: Option<Getter>,
        set: Option<Setter>,
    },
    Method(Method),
    Other,
}

/// A data model defined at run time, for trees only known then, such as a
/// bridge or a plugin host. Each property and method is a closure; the
/// closures get an owned [`Context`].
///
/// ```no_run
/// use rbus::Object;
/// use rbus::provider::DataModel;
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
///
/// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
/// let counter = Arc::new(AtomicU32::new(0));
/// let model = DataModel::new()
///     .property("Device.Sample.Counter", move |_cx| {
///         let c = counter.clone();
///         async move { Ok(c.fetch_add(1, Relaxed)) }
///     })
///     .method("Device.Sample.Reset()", |_cx, _args: Object| async { Ok(Object::new()) });
/// let provided = bus.provide(model).await?;
/// # Ok(()) }
/// ```
///
/// Names are templates, as in [`Schema`]: `Device.Sample.Host.{i}.Name`
/// is served for every row of `Device.Sample.Host.`, and
/// [`Context::instances`] says which. Rows of a [`Rows::Dynamic`] table are
/// numbered from 1 when consumers add them; the provider adds its own with
/// [`Provided::add_row`](crate::provider::Provided::add_row).
#[derive(Default)]
pub struct DataModel {
    schema: Schema,
    entries: Vec<(String, Entry)>,
    next_inst: Mutex<HashMap<String, u32>>,
}

impl DataModel {
    /// An empty model.
    pub fn new() -> Self {
        Self::default()
    }

    fn with(mut self, name: &str, schema: Schema, entry: Entry) -> Self {
        self.schema = schema;
        self.entries.push((name.to_owned(), entry));
        self
    }

    /// A read-only property, read by `get`.
    pub fn property<T, F, Fut>(self, name: &str, get: F) -> Self
    where
        T: IntoValue,
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, Fault>> + Send + 'static,
    {
        let schema = self.schema.clone().property(name, Access::ReadOnly);
        let entry = Entry::Property {
            get: Some(getter(get)),
            set: None,
        };
        self.with(name, schema, entry)
    }

    /// A property consumers can read and write. A set whose value doesn't
    /// convert to `T` fails with `InvalidParameterType` before any setter
    /// of its transaction runs.
    pub fn writable_property<T, G, GFut, S, SFut>(self, name: &str, get: G, set: S) -> Self
    where
        T: RbusType,
        G: Fn(Context) -> GFut + Send + Sync + 'static,
        GFut: Future<Output = Result<T, Fault>> + Send + 'static,
        S: Fn(Context, T) -> SFut + Send + Sync + 'static,
        SFut: Future<Output = Result<(), Fault>> + Send + 'static,
    {
        let schema = self.schema.clone().property(name, Access::ReadWrite);
        let set: Setter = Arc::new(move |cx, value: &Value| {
            let value: T = value_as(value)?;
            Ok(Box::pin(set(cx, value)) as BoxFuture<'static, _>)
        });
        let entry = Entry::Property {
            get: Some(getter(get)),
            set: Some(set),
        };
        self.with(name, schema, entry)
    }

    /// A method. Its arguments deserialize into `A` and its result
    /// serializes from `R`; [`Object`] works for both.
    pub fn method<A, R, F, Fut>(self, name: &str, f: F) -> Self
    where
        A: DeserializeOwned,
        R: Serialize,
        F: Fn(Context, A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, Fault>> + Send + 'static,
    {
        let schema = self.schema.clone().method(name);
        let f = Arc::new(f);
        let method: Method = Arc::new(move |cx, args| {
            let f = f.clone();
            Box::pin(async move {
                let args: A =
                    crate::from_object(args).map_err(|e| Fault::invalid_input(e.to_string()))?;
                let reply = f(cx, args).await?;
                crate::to_object(&reply).map_err(Fault::internal)
            })
        });
        self.with(name, schema, Entry::Method(method))
    }

    /// An event; publish it with [`Provided::publish`](crate::provider::Provided::publish).
    pub fn event(self, name: &str) -> Self {
        let schema = self.schema.clone().event(name);
        self.with(name, schema, Entry::Other)
    }

    /// A table.
    pub fn table(self, path: &str, rows: Rows) -> Self {
        let schema = self.schema.clone().table(path, rows);
        self.with(path, schema, Entry::Other)
    }

    /// Polls the property added last for changes; see [`Schema::poll`].
    pub fn poll(mut self, every: Duration) -> Self {
        self.schema = self.schema.poll(every);
        self
    }

    /// The entry whose template matches `name`, a canonical name.
    fn entry(&self, name: &str) -> Option<&Entry> {
        self.entries
            .iter()
            .find(|(template, _)| matches_template(template, name))
            .map(|(_, e)| e)
    }
}

fn getter<T, F, Fut>(get: F) -> Getter
where
    T: IntoValue,
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, Fault>> + Send + 'static,
{
    Arc::new(move |cx| {
        let fut = get(cx);
        Box::pin(async move { fut.await.map(IntoValue::into_value) })
    })
}

/// True if `name` is `template` with a number for each `{i}`.
fn matches_template(template: &str, name: &str) -> bool {
    let (t, n): (Vec<&str>, Vec<&str>) = (template.split('.').collect(), name.split('.').collect());
    t.len() == n.len()
        && t.iter().zip(&n).all(|(t, n)| {
            t == n || (*t == "{i}" && !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        })
}

impl Provider for DataModel {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    async fn get(&self, cx: &Context, name: &PropertyName) -> Result<Value, Fault> {
        match self.entry(name) {
            Some(Entry::Property { get: Some(get), .. }) => get(cx.clone()).await,
            _ => Err(Fault::not_found()),
        }
    }

    /// Converts every value first, then runs the setters in order. A
    /// setter's failure stops the batch; the setters before it have run.
    async fn set(&self, cx: &Context, tx: Transaction) -> Result<(), SetFault> {
        let mut pending = Vec::with_capacity(tx.len());
        for c in &tx {
            let fut = match self.entry(c.name()) {
                Some(Entry::Property { set: Some(set), .. }) => set(cx.clone(), c.value()),
                _ => Err(Fault::not_writable()),
            };
            pending.push((c.name(), fut.map_err(|f| SetFault::at(c.name(), f))?));
        }
        for (name, fut) in pending {
            fut.await.map_err(|f| SetFault::at(name, f))?;
        }
        Ok(())
    }

    async fn invoke(
        &self,
        cx: &Context,
        method: &MethodName,
        args: Object,
    ) -> Result<Object, Fault> {
        match self.entry(method) {
            Some(Entry::Method(m)) => m(cx.clone(), args).await,
            _ => Err(Fault::invalid_method()),
        }
    }

    async fn add_row(
        &self,
        _cx: &Context,
        table: &TablePath,
        _alias: Option<&str>,
    ) -> Result<u32, Fault> {
        let mut next = self.next_inst.lock().unwrap_or_else(|e| e.into_inner());
        let n = next.entry(table.to_string()).or_insert(1);
        *n += 1;
        Ok(*n - 1)
    }

    async fn remove_row(
        &self,
        _cx: &Context,
        _table: &TablePath,
        _row: &RowId,
    ) -> Result<(), Fault> {
        Ok(())
    }
}

impl fmt::Debug for DataModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataModel")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Status;
    use tracing::Span;

    fn cx() -> Context {
        Context::new(Some("c".into()), "rbus.c.INBOX.1".into(), Span::none())
    }

    fn tx(changes: &[(&'static str, Value)]) -> Transaction {
        Transaction {
            changes: changes
                .iter()
                .map(|(n, v)| crate::provider::Change {
                    name: PropertyName::from_static(n),
                    value: v.clone(),
                    instances: Vec::new(),
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn closures_serve_the_model() {
        let stored = Arc::new(Mutex::new(Vec::new()));
        let s = stored.clone();
        let model = DataModel::new()
            .property("A.Count", |_cx| async { Ok(7u32) })
            .poll(Duration::from_secs(1))
            .writable_property(
                "A.Host.{i}.Name",
                |cx: Context| async move { Ok(format!("host{}", cx.instances::<0>().is_ok())) },
                move |_cx, v: String| {
                    let s = s.clone();
                    async move {
                        if v == "bad" {
                            return Err(Fault::invalid_value("bad"));
                        }
                        s.lock().unwrap().push(v);
                        Ok(())
                    }
                },
            )
            .table("A.Host.", Rows::Dynamic)
            .event("A.Alarm!")
            .method("A.Echo()", |_cx, args: Object| async move { Ok(args) });
        assert_eq!(model.schema().elements().len(), 5);
        assert_eq!(
            model.schema().elements()[0].poll(),
            Some(Duration::from_secs(1))
        );
        assert!(format!("{model:?}").contains("DataModel"));
        let cx = cx();
        async fn get(model: &DataModel, name: &'static str) -> Result<Value, Fault> {
            model
                .get(&super::tests::cx(), &PropertyName::from_static(name))
                .await
        }
        assert_eq!(get(&model, "A.Count").await.unwrap(), Value::U32(7));
        assert_eq!(
            get(&model, "A.Host.3.Name").await.unwrap(),
            Value::from("hosttrue")
        );
        for missing in ["A.Host.x.Name", "A.Nope"] {
            let err = get(&model, missing).await.unwrap_err();
            assert_eq!(err.status(), Status::ElementDoesNotExist);
        }

        model
            .set(&cx, tx(&[("A.Host.1.Name", Value::from("one"))]))
            .await
            .unwrap();
        // A type error stops the batch before any setter runs.
        let err = model
            .set(
                &cx,
                tx(&[
                    ("A.Host.1.Name", Value::from("two")),
                    ("A.Host.2.Name", Value::U32(2)),
                ]),
            )
            .await
            .unwrap_err();
        assert_eq!(err.name().unwrap(), "A.Host.2.Name");
        assert_eq!(err.fault().status(), Status::InvalidParameterType);
        let err = model
            .set(&cx, tx(&[("A.Host.1.Name", Value::from("bad"))]))
            .await
            .unwrap_err();
        assert_eq!(err.fault().status(), Status::InvalidParameterValue);
        let err = model
            .set(&cx, tx(&[("A.Count", Value::U32(1))]))
            .await
            .unwrap_err();
        assert_eq!(err.fault().status(), Status::NotWritable);
        assert_eq!(*stored.lock().unwrap(), ["one"]);

        let args = Object::new().with("x", Value::I32(1));
        let echo = MethodName::from_static("A.Echo()");
        assert_eq!(model.invoke(&cx, &echo, args.clone()).await.unwrap(), args);
        let nope = MethodName::from_static("A.Nope()");
        assert_eq!(
            model
                .invoke(&cx, &nope, Object::new())
                .await
                .unwrap_err()
                .status(),
            Status::InvalidMethod
        );
        let table = TablePath::from_static("A.Host.");
        assert_eq!(model.add_row(&cx, &table, None).await.unwrap(), 1);
        assert_eq!(model.add_row(&cx, &table, Some("x")).await.unwrap(), 2);
        model
            .remove_row(&cx, &table, &RowId::from(1))
            .await
            .unwrap();
    }

    #[test]
    fn templates_match_numbers_only() {
        assert!(matches_template("A.{i}.B", "A.12.B"));
        assert!(!matches_template("A.{i}.B", "A..B"));
        assert!(!matches_template("A.{i}.B", "A.x.B"));
        assert!(!matches_template("A.{i}.B", "A.1.B.C"));
        assert!(matches_template("A.B()", "A.B()"));
    }
}
