//! What a provider handler learns about the request it serves.

use rbus_wire::value::Value;
use tracing::Span;

use crate::name::PropertyName;
use crate::provider::fault::{Fault, SetFault};
use crate::types::RbusType;

/// Checks that `instances` has exactly `N` entries.
fn exactly<const N: usize>(instances: &[u32]) -> Result<[u32; N], Fault> {
    instances.try_into().map_err(|_| {
        Fault::internal(InstanceCount {
            expected: N,
            got: instances.len(),
        })
    })
}

#[derive(Debug, thiserror::Error)]
#[error("expected {expected} instance numbers, the name has {got}")]
struct InstanceCount {
    expected: usize,
    got: usize,
}

/// The request a handler is serving (P7).
///
/// ```
/// # use rbus::provider::{Context, Fault};
/// async fn noise(cx: &Context) -> Result<i32, Fault> {
///     let [radio] = cx.instances()?;        // Device.WiFi.Radio.{i}.Noise
///     tracing::debug!(parent: cx.span(), radio, caller = ?cx.caller(), "reading noise");
///     Ok(-90)
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Context {
    caller: Option<String>,
    inbox: String,
    instances: Vec<u32>,
    span: Span,
}

impl Context {
    pub(crate) fn new(caller: Option<String>, inbox: String, span: Span) -> Self {
        Context {
            caller,
            inbox,
            instances: Vec::new(),
            span,
        }
    }

    pub(crate) fn with_instances(&self, instances: Vec<u32>) -> Self {
        Context {
            instances,
            ..self.clone()
        }
    }

    /// The requesting component. Gets and sets carry it; other requests
    /// don't, so it is `None` for them.
    pub fn caller(&self) -> Option<&str> {
        self.caller.as_deref()
    }

    /// The inbox the reply goes to, such as `rbus.rbuscli.INBOX.4711`. Every
    /// request has one; it ends in the caller's process id.
    pub fn inbox(&self) -> &str {
        &self.inbox
    }

    /// The instance numbers of the rows in the element's name, outermost
    /// first: `[3, 1]` for `Device.A.3.B.1.Value` registered as
    /// `Device.A.{i}.B.{i}.Value`. Aliases (`[lan]`) are already resolved to
    /// numbers. Fails if the name has a different number of rows than `N`.
    pub fn instances<const N: usize>(&self) -> Result<[u32; N], Fault> {
        exactly(&self.instances)
    }

    /// The tracing span the request runs in (`rbus.serve`).
    pub fn span(&self) -> &Span {
        &self.span
    }
}

/// One parameter of a [`Transaction`].
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    pub(crate) name: PropertyName,
    pub(crate) value: Value,
    pub(crate) instances: Vec<u32>,
}

impl Change {
    /// The parameter, with aliases resolved to instance numbers.
    pub fn name(&self) -> &PropertyName {
        &self.name
    }

    /// The new value.
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// The new value as `T`; a type mismatch fails with a [`SetFault`]
    /// naming this parameter (`InvalidParameterType`).
    pub fn get<T: RbusType>(&self) -> Result<T, SetFault> {
        T::from_value(self.value.clone())
            .map_err(|e| SetFault::at(&self.name, Fault::invalid_type(e.to_string())))
    }

    /// The instance numbers in the name, as for [`Context::instances`].
    pub fn instances<const N: usize>(&self) -> Result<[u32; N], Fault> {
        exactly(&self.instances)
    }

    /// Takes the value.
    pub fn into_value(self) -> Value {
        self.value
    }
}

/// The parameters of one set request, delivered together (C7, D8).
///
/// Names, writability and row existence are already checked against the
/// schema. Apply all of them or none: return a [`SetFault`] naming the
/// parameter that failed, and the consumer sees which one (C9).
///
/// ```
/// # use rbus::provider::{Context, SetFault, Transaction};
/// async fn set(cx: &Context, tx: Transaction) -> Result<(), SetFault> {
///     for change in &tx {
///         let enable: bool = change.get()?;
///         println!("{} = {enable}", change.name());
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    pub(crate) changes: Vec<Change>,
}

impl Transaction {
    /// The changes, in the order the consumer sent them.
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }

    /// Iterates over the changes.
    pub fn iter(&self) -> std::slice::Iter<'_, Change> {
        self.changes.iter()
    }

    /// The change to `name`, if the transaction has one.
    pub fn get(&self, name: &str) -> Option<&Change> {
        self.changes.iter().find(|c| c.name == name)
    }

    /// Number of changes.
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    /// True if there are no changes (never the case for a consumer's set).
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

impl IntoIterator for Transaction {
    type Item = Change;
    type IntoIter = std::vec::IntoIter<Change>;
    fn into_iter(self) -> Self::IntoIter {
        self.changes.into_iter()
    }
}

impl<'a> IntoIterator for &'a Transaction {
    type Item = &'a Change;
    type IntoIter = std::slice::Iter<'a, Change>;
    fn into_iter(self) -> Self::IntoIter {
        self.changes.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Status;

    #[test]
    fn instances_are_counted() {
        let cx = Context::new(Some("c".into()), "rbus.c.INBOX.1".into(), Span::none())
            .with_instances(vec![3, 1]);
        assert_eq!(cx.instances::<2>().unwrap(), [3, 1]);
        let err = cx.instances::<1>().unwrap_err();
        assert_eq!(err.status(), Status::BusError);
        assert_eq!(
            err.message(),
            Some("expected 1 instance numbers, the name has 2")
        );
        assert_eq!((cx.caller(), cx.inbox()), (Some("c"), "rbus.c.INBOX.1"));
        assert!(cx.span().is_none());
    }

    #[test]
    fn transactions_hand_out_typed_values() {
        let tx = Transaction {
            changes: vec![Change {
                name: PropertyName::from_static("A.1.B"),
                value: Value::Bool(true),
                instances: vec![1],
            }],
        };
        assert_eq!(tx.len(), 1);
        assert!(!tx.is_empty());
        let c = tx.get("A.1.B").unwrap();
        assert!(c.get::<bool>().unwrap());
        let bad = c.get::<String>().unwrap_err();
        assert_eq!(bad.name(), Some(c.name()));
        assert_eq!(bad.fault().status(), Status::InvalidParameterType);
        assert_eq!(c.instances::<1>().unwrap(), [1]);
        assert_eq!(c.value(), &Value::Bool(true));
        assert!(tx.get("A.2.B").is_none());
        assert_eq!(tx.changes().len(), (&tx).into_iter().count());
        assert_eq!(tx.iter().count(), 1);
        let values: Vec<Value> = tx.into_iter().map(Change::into_value).collect();
        assert_eq!(values, [Value::Bool(true)]);
    }
}
