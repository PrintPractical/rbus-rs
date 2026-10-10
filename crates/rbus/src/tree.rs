//! Partial-path and wildcard gets (C3, C4) with per-component failures (C5).

use rbus_wire::body::{GetRequest, GetResponse};
use rbus_wire::message::Method;
use rbus_wire::value::{Object, ObjectKind, Property, Value};
use serde::de::DeserializeOwned;
use tracing::{Instrument, debug_span};

use crate::bus::Rbus;
use crate::convert::from_object;
use crate::discover::Route;
use crate::error::{Error, Result, Status, TypeError};
use crate::name::{IntoName, Name};

/// A component whose part of a [`Tree`] could not be read (C5).
#[derive(Debug)]
pub struct TreeFailure {
    /// The component.
    pub component: String,
    /// What went wrong.
    pub error: Error,
}

/// The values under a partial path or matching a wildcard, from every
/// component that owns some of them (C3, C4).
///
/// On the wire a partial-path get returns a flat list of properties. A `Tree`
/// keeps that list ([`Tree::iter`]) and can rebuild the hierarchy below its
/// root ([`Tree::to_object`], [`Tree::deserialize`]): `Device.WiFi.Radio.1.`
/// with `Enable` and `Stats.BytesSent` becomes `{Enable, Stats: {BytesSent}}`,
/// and table rows become sequences.
#[derive(Debug)]
pub struct Tree {
    path: Name,
    properties: Vec<Property>,
    failures: Vec<TreeFailure>,
}

impl Tree {
    /// The path asked for.
    pub fn path(&self) -> &Name {
        &self.path
    }

    /// The part of every name that the hierarchy is built below: the path up
    /// to its first wildcard segment.
    pub fn root(&self) -> &str {
        match self.path.find('*') {
            Some(i) => &self.path[..i],
            None if self.path.ends_with('.') => &self.path,
            None => self.path.rfind('.').map_or("", |i| &self.path[..=i]),
        }
    }

    /// Number of values.
    pub fn len(&self) -> usize {
        self.properties.len()
    }

    /// True if no values were returned.
    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }

    /// Full names and values, in the order the providers sent them.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.properties.iter().map(|p| (p.name.as_str(), &p.value))
    }

    /// The value of a full name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.properties
            .iter()
            .find(|p| p.name == name)
            .map(|p| &p.value)
    }

    /// Components that failed; their values are missing (C5).
    pub fn failures(&self) -> &[TreeFailure] {
        &self.failures
    }

    /// The flat list of properties.
    pub fn into_properties(self) -> Vec<Property> {
        self.properties
    }

    /// The hierarchy below [`Tree::root`]: one nested object per path
    /// segment. Objects whose children are instance numbers are
    /// multi-instance, so they read as sequences.
    pub fn to_object(&self) -> Object {
        let root = self.root();
        let mut top = Object::new();
        for p in &self.properties {
            let rel = p.name.strip_prefix(root).unwrap_or(&p.name);
            insert(&mut top, rel, p.value.clone());
        }
        top
    }

    /// Deserializes the hierarchy below [`Tree::root`] with serde. TR-181
    /// names map to field names; table rows map to a `Vec` (in row order) or
    /// a map keyed by instance number.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, TypeError> {
        from_object(self.to_object())
    }
}

impl<'a> IntoIterator for &'a Tree {
    type Item = (&'a str, &'a Value);
    type IntoIter = Box<dyn Iterator<Item = (&'a str, &'a Value)> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// Inserts `value` at the dotted path `rel` below `obj`. A name that is both
/// a value and a parent keeps the first one seen.
fn insert(obj: &mut Object, rel: &str, value: Value) {
    let (head, rest) = match rel.split_once('.') {
        Some((h, r)) if !r.is_empty() => (h, Some(r)),
        Some((h, _)) => (h, None),
        None => (rel, None),
    };
    if head.parse::<u32>().is_ok() && obj.properties.is_empty() {
        obj.kind = ObjectKind::MultiInstance;
    }
    let slot = obj.properties.iter().position(|p| p.name == head);
    match (rest, slot) {
        (None, None) => obj.properties.push(Property::new(head, value)),
        (None, Some(_)) => {}
        (Some(rest), None) => {
            let mut child = Object::new();
            insert(&mut child, rest, value);
            obj.properties
                .push(Property::new(head, Value::Object(child)));
        }
        (Some(rest), Some(i)) => {
            if let Value::Object(child) = &mut obj.properties[i].value {
                insert(child, rest, value);
            }
        }
    }
}

impl Rbus {
    /// Gets every value under a partial path (`Device.WiFi.`) or matching a
    /// wildcard (`Device.IP.Interface.*.Status`), with one request per
    /// owning component, all in flight at once (C3, C4).
    ///
    /// ```no_run
    /// # async fn demo(bus: rbus::Rbus) -> rbus::Result<()> {
    /// #[derive(serde::Deserialize)]
    /// struct Radio { #[serde(rename = "Enable")] enable: bool }
    ///
    /// let tree = bus.get_tree("Device.WiFi.Radio.1.").await?;
    /// let radio: Radio = tree.deserialize()?;
    /// for failure in tree.failures() { eprintln!("{}: {}", failure.component, failure.error); }
    /// # Ok(()) }
    /// ```
    ///
    /// A component that fails is reported in [`Tree::failures`] while the
    /// others' values are kept (C5); the call fails only if every component
    /// fails, or none owns the path ([`Error::NotFound`]).
    pub async fn get_tree(&self, path: impl IntoName<Name>) -> Result<Tree> {
        let path = path.into_name()?;
        let span = debug_span!("rbus.get_tree", path = %path);
        async {
            let (owners, route) = self.resolve_owners(&path).await?;
            let body = GetRequest {
                requesting_component: self.component().to_owned(),
                names: vec![path.to_string()],
            };
            let mut pending = Vec::with_capacity(owners.len());
            for owner in owners {
                let topic = match route {
                    Route::Components => owner.as_str(),
                    Route::Name => path.as_str(),
                };
                let call = self.start_call::<GetResponse, _>(
                    topic,
                    Method::GetParameterValues,
                    &body,
                    self.inner.timeouts.get,
                    "get",
                    path.clone(),
                );
                pending.push((owner, call));
            }
            let mut tree = Tree {
                path: path.clone(),
                properties: Vec::new(),
                failures: Vec::new(),
            };
            for (component, call) in pending {
                let result = match call {
                    Ok(call) => call.await,
                    Err(e) => Err(e),
                };
                match result.and_then(|resp| match Status::from_wire(resp.status) {
                    s if s.is_success() => Ok(resp.properties),
                    status => Err(Error::Status {
                        name: path.clone(),
                        status,
                    }),
                }) {
                    Ok(props) => tree.properties.extend(props),
                    Err(error) => tree.failures.push(TreeFailure { component, error }),
                }
            }
            if tree.properties.is_empty() && !tree.failures.is_empty() {
                return Err(tree.failures.remove(0).error);
            }
            Ok(tree)
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::BTreeMap;

    fn tree(path: &str, props: &[(&str, Value)]) -> Tree {
        Tree {
            path: Name::parse(path.to_owned()).unwrap(),
            properties: props
                .iter()
                .map(|(n, v)| Property::new(*n, v.clone()))
                .collect(),
            failures: vec![],
        }
    }

    #[derive(Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "PascalCase")]
    struct Stats {
        bytes_sent: u64,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "PascalCase")]
    struct Radio {
        enable: bool,
        stats: Stats,
    }

    #[test]
    fn rebuilds_the_hierarchy() {
        let t = tree(
            "Device.WiFi.Radio.1.",
            &[
                ("Device.WiFi.Radio.1.Enable", Value::Bool(true)),
                ("Device.WiFi.Radio.1.Stats.BytesSent", Value::U64(9)),
                ("Device.WiFi.Radio.1.Enable.Oops", Value::Bool(false)),
                ("Device.WiFi.Radio.1.Enable", Value::Bool(false)),
            ],
        );
        assert_eq!(t.root(), "Device.WiFi.Radio.1.");
        assert_eq!(t.len(), 4);
        assert!(!t.is_empty());
        assert_eq!(
            t.get("Device.WiFi.Radio.1.Enable"),
            Some(&Value::Bool(true))
        );
        assert_eq!(t.get("nope"), None);
        let radio: Radio = t.deserialize().unwrap();
        assert_eq!(
            radio,
            Radio {
                enable: true,
                stats: Stats { bytes_sent: 9 }
            }
        );
        assert_eq!((&t).into_iter().count(), 4);
        assert_eq!(t.path(), "Device.WiFi.Radio.1.");
        assert!(t.failures().is_empty());
        assert_eq!(t.into_properties().len(), 4);
    }

    #[test]
    fn tables_read_as_sequences_or_maps() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Host {
            #[serde(rename = "Name")]
            name: String,
        }
        #[derive(Deserialize, Debug, PartialEq)]
        struct Hosts {
            #[serde(rename = "Host")]
            host: Vec<Host>,
        }
        let t = tree(
            "Device.Hosts.",
            &[
                ("Device.Hosts.Host.1.Name", Value::String("a".into())),
                ("Device.Hosts.Host.3.Name", Value::String("b".into())),
            ],
        );
        let hosts: Hosts = t.deserialize().unwrap();
        assert_eq!(hosts.host.len(), 2);
        let w = tree(
            "Device.Hosts.Host.*.Name",
            &[
                ("Device.Hosts.Host.1.Name", Value::String("a".into())),
                ("Device.Hosts.Host.3.Name", Value::String("b".into())),
            ],
        );
        assert_eq!(w.root(), "Device.Hosts.Host.");
        let by_inst: BTreeMap<u32, Host> = w.deserialize().unwrap();
        assert_eq!(by_inst[&3].name, "b");
        let rows: Vec<Host> = w.deserialize().unwrap();
        assert_eq!(rows[0].name, "a");
        assert_eq!(w.to_object().kind, ObjectKind::MultiInstance);
    }

    #[test]
    fn exact_names_and_odd_paths() {
        let t = tree(
            "Device.A.B",
            &[("Device.A.B", Value::I32(1)), ("Other.X", Value::I32(2))],
        );
        assert_eq!(t.root(), "Device.A.");
        let o = t.to_object();
        assert_eq!(o.get("B"), Some(&Value::I32(1)));
        let Some(Value::Object(other)) = o.get("Other") else {
            panic!()
        };
        assert_eq!(other.get("X"), Some(&Value::I32(2)));
        let flat = tree("X", &[("X", Value::I32(1))]);
        assert_eq!(flat.root(), "");
        assert_eq!(flat.to_object().get("X"), Some(&Value::I32(1)));
    }
}
