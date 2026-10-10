//! Partial-path and wildcard gets (C3, C4) with per-component failures (C5).

use indexmap::IndexMap;
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
#[non_exhaustive]
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
    /// multi-instance, so they read as sequences. A name that is both a
    /// value and a parent keeps the first one seen. Names more than 128
    /// segments deep are left out, as no data model nests that far;
    /// [`Tree::iter`] still has them.
    pub fn to_object(&self) -> Object {
        let root = self.root();
        let mut top = Node::default();
        for p in &self.properties {
            let rel = p.name.strip_prefix(root).unwrap_or(&p.name);
            top.insert(rel, &p.value);
        }
        top.into_object()
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

/// The deepest name, in segments, that [`Tree::to_object`] places.
const MAX_SEGMENTS: usize = 128;

/// The hierarchy being built: children in the order first seen, found by
/// name in constant time.
#[derive(Default)]
struct Node {
    multi_instance: bool,
    children: IndexMap<String, Slot>,
}

enum Slot {
    Value(Value),
    Node(Node),
}

impl Node {
    /// Places `value` at the dotted path `rel`, without recursing.
    fn insert(&mut self, rel: &str, value: &Value) {
        let mut segments: Vec<&str> = rel.split('.').collect();
        if segments.len() > 1 && segments.last() == Some(&"") {
            segments.pop();
        }
        if segments.len() > MAX_SEGMENTS {
            return;
        }
        let mut node = self;
        let last = segments.len() - 1;
        for (i, seg) in segments.into_iter().enumerate() {
            if node.children.is_empty() && seg.parse::<u32>().is_ok() {
                node.multi_instance = true;
            }
            if i == last {
                node.children
                    .entry(seg.to_owned())
                    .or_insert_with(|| Slot::Value(value.clone()));
                return;
            }
            let slot = node
                .children
                .entry(seg.to_owned())
                .or_insert_with(|| Slot::Node(Node::default()));
            match slot {
                Slot::Node(child) => node = child,
                // A value already sits here; it wins.
                Slot::Value(_) => return,
            }
        }
    }

    /// The object for this node. Recursion is bounded by `MAX_SEGMENTS`.
    fn into_object(self) -> Object {
        let mut obj = Object::new();
        if self.multi_instance {
            obj.kind = ObjectKind::MultiInstance;
        }
        for (name, slot) in self.children {
            let value = match slot {
                Slot::Value(v) => v,
                Slot::Node(n) => Value::Object(n.into_object()),
            };
            obj.properties.push(Property::new(name, value));
        }
        obj
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
            let mut succeeded = false;
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
                    Ok(props) => {
                        succeeded = true;
                        tree.properties.extend(props);
                    }
                    Err(error) => tree.failures.push(TreeFailure { component, error }),
                }
            }
            // An empty answer is still an answer.
            if !succeeded && !tree.failures.is_empty() {
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

    #[test]
    fn hostile_names_cost_little() {
        // A name a million segments deep is left out of the hierarchy,
        // without recursing once per segment.
        let deep = "D.".to_owned() + &"a.".repeat(1_000_000) + "x";
        let mut props = vec![(deep.as_str(), Value::I32(1))];
        // Many siblings are found by index, not by scanning.
        let wide: Vec<String> = (0..50_000).map(|i| format!("D.W.N{i}")).collect();
        props.extend(wide.iter().map(|n| (n.as_str(), Value::I32(2))));
        let t = tree("D.", &props);
        let obj = t.to_object();
        assert_eq!(obj.properties.len(), 1, "only W");
        let Value::Object(w) = &obj.properties[0].value else {
            panic!("W is an object")
        };
        assert_eq!(w.properties.len(), 50_000);
        assert_eq!(t.get(&deep), Some(&Value::I32(1)));
    }
}
