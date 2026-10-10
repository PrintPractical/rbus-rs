//! The data model every provider on a connection serves, as the engine sees
//! it: a tree of the registered name templates, plus the table rows that
//! exist. The engine owns the rows (as the C library does), so it can resolve
//! `[alias]` names, list rows and walk partial paths without asking providers.
//!
//! Names are matched the way `rbus.c` matches them:
//!
//! - **Exact names** (`Device.T.3.Prop`, `Device.T.[lan].Prop`) resolve to one
//!   element. The row must exist.
//! - **Partial paths** end in `.` and name an object, a table or a row; a get
//!   returns every readable property below it, in schema order and row order
//!   (`_get_recursive_partialpath_handler`).
//! - **Wildcards** put `*` where a row goes and fan out over every row
//!   (`_get_recursive_wildcard_handler_internal`). A trailing `.*` reads as
//!   `.`.
//!
//! Results always use canonical names: instance numbers, never aliases.

use std::collections::HashMap;
use std::time::Duration;

use rbus_wire::body::ElementName;

use crate::error::Status;
use crate::name::NameError;
use crate::provider::schema::{Access, Element, ElementKind, Rows};
use crate::subscribe::RowId;

/// Identifies one registered provider.
pub(crate) type MountId = u64;

/// Index of a node in [`Tree::nodes`].
pub(crate) type NodeId = usize;

const ROOT: NodeId = 0;

/// `rbusAccess_t` bits.
const ACCESS_GET: u32 = 1;
const ACCESS_SET: u32 = 2;
const ACCESS_ADDROW: u32 = 4;
const ACCESS_REMOVEROW: u32 = 8;
const ACCESS_SUBSCRIBE: u32 = 16;
const ACCESS_INVOKE: u32 = 32;

/// `rbusElementType_t`, plus 0 for objects and rows.
const TYPE_OBJECT: i32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    /// A path segment with children; implicit unless it is the root.
    Object,
    Property(Access),
    /// A table. Its children are the elements of each row (the `{i}` level is
    /// folded into the table node).
    Table(Rows),
    Method,
    Event,
}

impl NodeKind {
    fn wire_type(self) -> i32 {
        match self {
            NodeKind::Object => TYPE_OBJECT,
            NodeKind::Property(_) => 1,
            NodeKind::Table(_) => 2,
            NodeKind::Event => 3,
            NodeKind::Method => 4,
        }
    }

    fn access(self) -> u32 {
        match self {
            NodeKind::Object => 0,
            NodeKind::Property(a) => {
                let mut bits = ACCESS_SUBSCRIBE;
                if a.readable() {
                    bits |= ACCESS_GET;
                }
                if a.writable() {
                    bits |= ACCESS_SET;
                }
                bits
            }
            NodeKind::Table(Rows::Dynamic) => ACCESS_ADDROW | ACCESS_REMOVEROW | ACCESS_SUBSCRIBE,
            NodeKind::Table(Rows::Fixed) => ACCESS_SUBSCRIBE,
            NodeKind::Method => ACCESS_INVOKE,
            NodeKind::Event => ACCESS_SUBSCRIBE,
        }
    }

    fn has_children(self) -> bool {
        matches!(self, NodeKind::Object | NodeKind::Table(_))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Node {
    seg: String,
    pub(crate) kind: NodeKind,
    /// The provider that registered this element; `None` for implicit
    /// objects and tables.
    pub(crate) owner: Option<MountId>,
    /// How often to poll a property for value changes.
    pub(crate) poll: Option<Duration>,
    children: Vec<NodeId>,
}

/// Why a schema could not be added to the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaError {
    /// A name is malformed.
    Name(NameError),
    /// A name is already registered.
    Duplicate(String),
    /// A name clashes with the shape of another (an object used as a table,
    /// a property with children, ...).
    Conflict(String),
}

/// The registered name templates.
#[derive(Debug, Clone)]
pub(crate) struct Tree {
    nodes: Vec<Node>,
}

impl Default for Tree {
    fn default() -> Self {
        Tree {
            nodes: vec![Node {
                seg: String::new(),
                kind: NodeKind::Object,
                owner: None,
                poll: None,
                children: Vec::new(),
            }],
        }
    }
}

fn bad(name: &str, reason: &'static str) -> SchemaError {
    SchemaError::Name(NameError {
        name: name.to_owned(),
        kind: "element template",
        reason,
    })
}

/// Splits a name into segments; the flag says whether it ended in `.`.
fn split(name: &str) -> (Vec<&str>, bool) {
    match name.strip_suffix('.') {
        Some(rest) => (rest.split('.').collect(), true),
        None => (name.split('.').collect(), false),
    }
}

impl Tree {
    /// Builds the tree for these schemas, in order.
    pub(crate) fn build<'a>(
        schemas: impl IntoIterator<Item = (MountId, &'a [Element])>,
    ) -> Result<Tree, SchemaError> {
        let mut tree = Tree::default();
        for (mount, elements) in schemas {
            for e in elements {
                tree.insert(mount, e)?;
            }
        }
        Ok(tree)
    }

    pub(crate) fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id]
    }

    fn child(&self, parent: NodeId, seg: &str) -> Option<NodeId> {
        self.nodes[parent]
            .children
            .iter()
            .copied()
            .find(|&c| self.nodes[c].seg == seg)
    }

    fn add_child(&mut self, parent: NodeId, seg: &str, kind: NodeKind) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(Node {
            seg: seg.to_owned(),
            kind,
            owner: None,
            poll: None,
            children: Vec::new(),
        });
        self.nodes[parent].children.push(id);
        id
    }

    fn insert(&mut self, mount: MountId, e: &Element) -> Result<(), SchemaError> {
        let name = e.name();
        crate::name::Name::parse(name.to_owned()).map_err(SchemaError::Name)?;
        let (segs, dot) = split(name);
        let (path, leaf) = match e.kind() {
            ElementKind::Table(_) => {
                // `A.T.{i}.`: the table node is `T`.
                if !dot || segs.len() < 2 || segs[segs.len() - 1] != "{i}" {
                    return Err(bad(name, "a table template must end in \".{i}.\""));
                }
                (&segs[..segs.len() - 2], segs[segs.len() - 2])
            }
            _ if dot => return Err(bad(name, "only tables may end in '.'")),
            _ => (&segs[..segs.len() - 1], segs[segs.len() - 1]),
        };
        match e.kind() {
            ElementKind::Method if !leaf.ends_with("()") || leaf.len() == 2 => {
                return Err(bad(name, "a method name must end in \"()\""));
            }
            ElementKind::Event if leaf.ends_with("()") || leaf == "!" => {
                return Err(bad(name, "an event name can't end in \"()\""));
            }
            ElementKind::Property(_) | ElementKind::Table(_)
                if leaf.ends_with("()") || leaf.ends_with('!') =>
            {
                return Err(bad(name, "only methods end in \"()\" and events in '!'"));
            }
            _ => {}
        }
        if leaf == "{i}" || !plain(leaf.trim_end_matches("()").trim_end_matches('!')) {
            return Err(bad(name, "a segment holds a reserved character"));
        }
        if e.poll().is_some() && !matches!(e.kind(), ElementKind::Property(_)) {
            return Err(bad(name, "only properties can be polled"));
        }
        let mut cur = ROOT;
        let mut in_table = false;
        for &seg in path {
            if seg == "{i}" {
                match self.nodes[cur].kind {
                    NodeKind::Table(_) if in_table => in_table = false,
                    // An implicit object that so far has no children becomes
                    // an implicit, provider-filled table.
                    NodeKind::Object if cur != ROOT && self.nodes[cur].children.is_empty() => {
                        self.nodes[cur].kind = NodeKind::Table(Rows::Fixed);
                        in_table = false;
                    }
                    _ => return Err(SchemaError::Conflict(name.to_owned())),
                }
                continue;
            }
            if in_table || !plain(seg) {
                return Err(if plain(seg) {
                    SchemaError::Conflict(name.to_owned())
                } else {
                    bad(name, "a segment holds a reserved character")
                });
            }
            cur = match self.child(cur, seg) {
                Some(c) if self.nodes[c].kind.has_children() => c,
                Some(_) => return Err(SchemaError::Conflict(name.to_owned())),
                None => self.add_child(cur, seg, NodeKind::Object),
            };
            in_table = matches!(self.nodes[cur].kind, NodeKind::Table(_));
        }
        if in_table {
            return Err(SchemaError::Conflict(name.to_owned()));
        }
        let kind = match e.kind() {
            ElementKind::Property(a) => NodeKind::Property(a),
            ElementKind::Table(r) => NodeKind::Table(r),
            ElementKind::Method => NodeKind::Method,
            ElementKind::Event => NodeKind::Event,
        };
        let id = match (self.child(cur, leaf), kind) {
            (None, _) => self.add_child(cur, leaf, kind),
            (Some(c), _) if self.nodes[c].owner.is_some() => {
                return Err(SchemaError::Duplicate(name.to_owned()));
            }
            // Declaring a table that a deeper name already implied.
            (Some(c), NodeKind::Table(_)) if matches!(self.nodes[c].kind, NodeKind::Table(_)) => c,
            (Some(c), NodeKind::Table(_))
                if self.nodes[c].kind == NodeKind::Object && self.nodes[c].children.is_empty() =>
            {
                c
            }
            (Some(_), _) => return Err(SchemaError::Conflict(name.to_owned())),
        };
        let node = &mut self.nodes[id];
        node.kind = kind;
        node.owner = Some(mount);
        node.poll = e.poll();
        Ok(())
    }
}

/// True if `seg` is a usable path segment: not empty, no wildcard, alias or
/// template characters.
fn plain(seg: &str) -> bool {
    !seg.is_empty() && !seg.contains(['*', '[', ']', '{', '}'])
}

/// Where a name led.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Target {
    /// A property, method, event or object.
    Node(NodeId),
    /// A table, before a row is chosen.
    Table(NodeId),
    /// A row of the table.
    Row(NodeId, u32),
}

/// A name resolved against the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resolved {
    pub(crate) target: Target,
    /// The name with instance numbers for aliases; objects, tables and rows
    /// end in `.`.
    pub(crate) canonical: String,
    /// The instance numbers of the rows on the way, outermost first.
    pub(crate) instances: Vec<u32>,
}

impl Resolved {
    fn root() -> Self {
        Resolved {
            target: Target::Node(ROOT),
            canonical: String::new(),
            instances: Vec::new(),
        }
    }
}

/// A readable property found under a partial path or wildcard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Found {
    pub(crate) name: String,
    pub(crate) node: NodeId,
    pub(crate) instances: Vec<u32>,
}

/// The tree plus the rows that exist, keyed by the canonical table path
/// (`Device.A.3.B.`), each in the order the rows were added.
#[derive(Debug, Clone, Default)]
pub(crate) struct Model {
    pub(crate) tree: Tree,
    rows: HashMap<String, Vec<RowId>>,
}

impl Model {
    /// Replaces the tree and drops rows of tables that no longer exist.
    pub(crate) fn set_tree(&mut self, tree: Tree) {
        self.tree = tree;
        let mut tables: Vec<String> = self.rows.keys().cloned().collect();
        tables.sort_by_key(String::len);
        for table in tables {
            if !matches!(
                self.resolve(&table),
                Ok(Resolved {
                    target: Target::Table(_),
                    ..
                })
            ) {
                self.rows.remove(&table);
            }
        }
    }

    /// The rows of the table at `canonical`.
    pub(crate) fn rows(&self, canonical: &str) -> &[RowId] {
        self.rows.get(canonical).map_or(&[], Vec::as_slice)
    }

    /// Finds the row `seg` (an instance number or `[alias]`) of a table.
    fn find_row(&self, table: &str, seg: &str) -> Option<u32> {
        let rows = self.rows(table);
        if let Some(alias) = seg.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            rows.iter()
                .find(|r| r.alias.as_deref() == Some(alias))
                .map(|r| r.inst)
        } else {
            let inst: u32 = seg.parse().ok()?;
            rows.iter().any(|r| r.inst == inst).then_some(inst)
        }
    }

    /// Takes one step from `at` along `seg`, or fans out at a `*`.
    fn step(&self, at: &Resolved, seg: &str, out: &mut Vec<Resolved>) -> Result<(), Status> {
        let parent = match at.target {
            Target::Table(t) => {
                let insts: Vec<u32> = if seg == "*" {
                    self.rows(&at.canonical).iter().map(|r| r.inst).collect()
                } else {
                    vec![
                        self.find_row(&at.canonical, seg)
                            .ok_or(Status::ElementDoesNotExist)?,
                    ]
                };
                for inst in insts {
                    let mut instances = at.instances.clone();
                    instances.push(inst);
                    out.push(Resolved {
                        target: Target::Row(t, inst),
                        canonical: format!("{}{inst}.", at.canonical),
                        instances,
                    });
                }
                return Ok(());
            }
            Target::Row(t, _) => t,
            Target::Node(n) if self.tree.node(n).kind == NodeKind::Object => n,
            Target::Node(_) => return Err(Status::ElementDoesNotExist),
        };
        if seg == "*" {
            // `rbus.c` only expands `*` at a table.
            return Err(Status::AccessNotAllowed);
        }
        let child = self
            .tree
            .child(parent, seg)
            .ok_or(Status::ElementDoesNotExist)?;
        let node = self.tree.node(child);
        let (target, canonical) = match node.kind {
            NodeKind::Table(_) => (Target::Table(child), format!("{}{seg}.", at.canonical)),
            NodeKind::Object => (Target::Node(child), format!("{}{seg}.", at.canonical)),
            _ => (Target::Node(child), format!("{}{seg}", at.canonical)),
        };
        out.push(Resolved {
            target,
            canonical,
            instances: at.instances.clone(),
        });
        Ok(())
    }

    /// Resolves a name that may hold `*`, to every match. A trailing `.` must
    /// lead to an object, table or row.
    pub(crate) fn resolve_all(&self, name: &str) -> Result<Vec<Resolved>, Status> {
        self.expand(name, false)
    }

    /// Like [`Model::resolve_all`], but the branches of a `*` that don't fit
    /// the rest of the name are dropped instead of failing it: for
    /// `T.*.U.[x].V`, the rows of `T` whose `U` has no row `x`. This is how
    /// subscriptions match (`TokenChain_match`); gets fail instead, as
    /// `rbus.c` does.
    pub(crate) fn resolve_matching(&self, name: &str) -> Vec<Resolved> {
        self.expand(name, true).unwrap_or_default()
    }

    fn expand(&self, name: &str, lenient: bool) -> Result<Vec<Resolved>, Status> {
        let name = name.strip_suffix(".*").map_or(name, |n| &name[..=n.len()]);
        let (segs, dot) = split(name);
        if segs.iter().any(|s| s.is_empty()) {
            return Err(Status::ElementDoesNotExist);
        }
        let mut at = vec![Resolved::root()];
        for seg in segs {
            let mut next = Vec::new();
            for r in &at {
                match self.step(r, seg, &mut next) {
                    Err(_) if lenient => {}
                    other => other?,
                }
            }
            at = next;
        }
        if dot
            && at.iter().any(
                |r| matches!(r.target, Target::Node(n) if !self.tree.node(n).kind.has_children()),
            )
        {
            return Err(Status::ElementDoesNotExist);
        }
        Ok(at)
    }

    /// Resolves an exact name (no `*`).
    pub(crate) fn resolve(&self, name: &str) -> Result<Resolved, Status> {
        if name.contains('*') {
            return Err(Status::ElementDoesNotExist);
        }
        let mut all = self.resolve_all(name)?;
        Ok(all.remove(0))
    }

    /// Resolves a subscription name against the templates only: any row
    /// position may hold `*`, a number or an alias, existing or not. Returns
    /// where the name leads, with `Target::Row(table, 0)` for a row.
    pub(crate) fn resolve_template(&self, name: &str) -> Result<Target, Status> {
        let name = name.strip_suffix(".*").map_or(name, |n| &name[..=n.len()]);
        let (segs, dot) = split(name);
        let mut at = Target::Node(ROOT);
        for seg in segs {
            if seg.is_empty() {
                return Err(Status::ElementDoesNotExist);
            }
            let parent = match at {
                Target::Table(t) => {
                    at = Target::Row(t, 0);
                    continue;
                }
                Target::Row(t, _) => t,
                Target::Node(n) if self.tree.node(n).kind == NodeKind::Object => n,
                Target::Node(_) => return Err(Status::ElementDoesNotExist),
            };
            let child = self
                .tree
                .child(parent, seg)
                .ok_or(Status::ElementDoesNotExist)?;
            at = match self.tree.node(child).kind {
                NodeKind::Table(_) => Target::Table(child),
                _ => Target::Node(child),
            };
        }
        match at {
            Target::Node(n) if dot && !self.tree.node(n).kind.has_children() => {
                Err(Status::ElementDoesNotExist)
            }
            Target::Node(ROOT) => Err(Status::ElementDoesNotExist),
            at => Ok(at),
        }
    }

    /// True if the canonical name `canonical` is one of the names `pattern`
    /// (exact, with aliases, or with `*`) stands for right now.
    pub(crate) fn matches(&self, pattern: &str, canonical: &str) -> bool {
        pattern == canonical
            || self
                .resolve_matching(pattern)
                .iter()
                .any(|r| r.canonical == canonical)
    }

    /// Resolves the path of a table (`Device.A.3.B.`, trailing `.` optional).
    pub(crate) fn resolve_table(&self, path: &str) -> Result<(NodeId, Resolved), Status> {
        let path = path.strip_suffix('.').unwrap_or(path);
        let r = self.resolve(path)?;
        match r.target {
            Target::Table(t) => Ok((t, r)),
            _ => Err(Status::ElementDoesNotExist),
        }
    }

    /// Resolves the name of a row (`Device.A.3.` or `Device.A.[x]`).
    pub(crate) fn resolve_row(&self, name: &str) -> Result<(NodeId, u32, Resolved), Status> {
        let name = name.strip_suffix('.').unwrap_or(name);
        let r = self.resolve(name)?;
        match r.target {
            Target::Row(t, inst) => Ok((t, inst, r)),
            _ => Err(Status::ElementDoesNotExist),
        }
    }

    /// Every readable property under an object, table or row, in schema and
    /// row order.
    pub(crate) fn properties_under(&self, at: &Resolved, out: &mut Vec<Found>) {
        let children = match at.target {
            Target::Table(t) => {
                for row in self.rows(&at.canonical) {
                    let mut instances = at.instances.clone();
                    instances.push(row.inst);
                    let row_at = Resolved {
                        target: Target::Row(t, row.inst),
                        canonical: format!("{}{}.", at.canonical, row.inst),
                        instances,
                    };
                    self.properties_in(t, &row_at, out);
                }
                return;
            }
            Target::Row(t, _) => t,
            Target::Node(n) => match self.tree.node(n).kind {
                NodeKind::Object => n,
                NodeKind::Property(a) if a.readable() => {
                    out.push(Found {
                        name: at.canonical.clone(),
                        node: n,
                        instances: at.instances.clone(),
                    });
                    return;
                }
                _ => return,
            },
        };
        self.properties_in(children, at, out);
    }

    /// The readable properties below the children of `parent`, which are
    /// reached at `at`.
    fn properties_in(&self, parent: NodeId, at: &Resolved, out: &mut Vec<Found>) {
        for &c in &self.tree.node(parent).children {
            let node = self.tree.node(c);
            let mut child = Resolved {
                target: Target::Node(c),
                canonical: format!("{}{}", at.canonical, node.seg),
                instances: at.instances.clone(),
            };
            match node.kind {
                NodeKind::Property(a) if a.readable() => out.push(Found {
                    name: child.canonical,
                    node: c,
                    instances: child.instances,
                }),
                NodeKind::Object => {
                    child.canonical.push('.');
                    self.properties_under(&child, out);
                }
                NodeKind::Table(_) => {
                    child.canonical.push('.');
                    child.target = Target::Table(c);
                    self.properties_under(&child, out);
                }
                _ => {}
            }
        }
    }

    /// The element names under `at` for `GETPARAMETERNAMES`, as
    /// `_get_parameter_names_recurse` lists them: a depth `n >= 0` lists the
    /// element and everything up to `n` levels below it; a depth `-n` lists
    /// only level `n`.
    pub(crate) fn element_names(&self, at: &Resolved, depth: i32) -> Vec<ElementName> {
        let mut out = Vec::new();
        self.names_rec(at, depth, 0, &mut out);
        out
    }

    fn names_rec(&self, at: &Resolved, depth: i32, level: i32, out: &mut Vec<ElementName>) {
        let max = depth.abs();
        if depth >= 0 || level == max {
            let (element_type, access) = match at.target {
                Target::Node(n) => {
                    let kind = self.tree.node(n).kind;
                    (kind.wire_type(), kind.access())
                }
                Target::Table(t) => {
                    let kind = self.tree.node(t).kind;
                    (kind.wire_type(), kind.access())
                }
                // Rows show read-write access, as CCSP expects (`rbus.c:2165`).
                Target::Row(..) => (TYPE_OBJECT, ACCESS_GET | ACCESS_SET),
            };
            if at.target != Target::Node(ROOT) {
                out.push(ElementName {
                    name: at.canonical.clone(),
                    element_type,
                    access,
                });
            }
        }
        if level >= max {
            return;
        }
        for child in self.children(at) {
            self.names_rec(&child, depth, level + 1, out);
        }
    }

    /// The objects, tables, rows and elements directly below `at`.
    fn children(&self, at: &Resolved) -> Vec<Resolved> {
        let parent = match at.target {
            Target::Table(t) => {
                return self
                    .rows(&at.canonical)
                    .iter()
                    .map(|row| {
                        let mut instances = at.instances.clone();
                        instances.push(row.inst);
                        Resolved {
                            target: Target::Row(t, row.inst),
                            canonical: format!("{}{}.", at.canonical, row.inst),
                            instances,
                        }
                    })
                    .collect();
            }
            Target::Row(t, _) => t,
            Target::Node(n) => n,
        };
        let mut out = Vec::new();
        for &c in &self.tree.node(parent).children {
            // Only the step itself matters here, and it can't fail.
            let seg = &self.tree.node(c).seg;
            let _ = self.step(at, seg, &mut out);
        }
        out
    }

    /// Adds a row to the table at `table` (canonical). Fails if the number
    /// or the alias is taken.
    pub(crate) fn add_row(&mut self, table: &str, row: RowId) -> Result<(), Status> {
        let rows = self.rows.entry(table.to_owned()).or_default();
        if rows.iter().any(|r| r.inst == row.inst)
            || (row.alias.is_some() && rows.iter().any(|r| r.alias == row.alias))
        {
            return Err(Status::ElementNameDuplicate);
        }
        rows.push(row);
        Ok(())
    }

    /// True if the table already has a row with this alias.
    pub(crate) fn alias_taken(&self, table: &str, alias: &str) -> bool {
        self.rows(table)
            .iter()
            .any(|r| r.alias.as_deref() == Some(alias))
    }

    /// Removes a row and every row of the tables inside it.
    pub(crate) fn remove_row(&mut self, table: &str, inst: u32) -> Option<RowId> {
        let rows = self.rows.get_mut(table)?;
        let i = rows.iter().position(|r| r.inst == inst)?;
        let row = rows.remove(i);
        let prefix = format!("{table}{inst}.");
        self.rows.retain(|k, _| !k.starts_with(&prefix));
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::schema::Schema;

    fn model(schema: &Schema) -> Model {
        let mut m = Model::default();
        m.set_tree(Tree::build([(1, schema.elements())]).unwrap());
        m
    }

    fn row(inst: u32, alias: Option<&str>) -> RowId {
        RowId {
            inst,
            alias: alias.map(str::to_owned),
        }
    }

    /// The `Device.TestProvider.PartialPath1` model of `rbusTestProvider.c`.
    fn partial_path() -> Model {
        let s = Schema::new()
            .table("D.P.{i}.", Rows::Dynamic)
            .property("D.P.{i}.Param1", Access::ReadOnly)
            .property("D.P.{i}.Param2", Access::ReadOnly)
            .property("D.P.{i}.SubObject1.Param3", Access::ReadOnly)
            .property("D.P.{i}.SubObject1.Param4", Access::ReadOnly)
            .table("D.P.{i}.SubTable.{i}.", Rows::Dynamic)
            .property("D.P.{i}.SubTable.{i}.Param5", Access::ReadOnly)
            .property("D.P.{i}.SubTable.{i}.SubObject2.Param6", Access::ReadOnly)
            .method("D.P.{i}.Reset()")
            .property("D.W", Access::WriteOnly)
            .event("D.E!");
        let mut m = model(&s);
        m.add_row("D.P.", row(1, None)).unwrap();
        m.add_row("D.P.", row(2, Some("two"))).unwrap();
        m.add_row("D.P.1.SubTable.", row(1, None)).unwrap();
        for i in 1..=3 {
            m.add_row("D.P.2.SubTable.", row(i, None)).unwrap();
        }
        m
    }

    fn names(found: &[Found]) -> Vec<&str> {
        found.iter().map(|f| f.name.as_str()).collect()
    }

    fn under(m: &Model, path: &str) -> Vec<String> {
        let mut out = Vec::new();
        for r in m.resolve_all(path).unwrap() {
            m.properties_under(&r, &mut out);
        }
        names(&out).into_iter().map(str::to_owned).collect()
    }

    #[test]
    fn exact_names_resolve_aliases_to_numbers() {
        let m = partial_path();
        let r = m.resolve("D.P.[two].SubTable.3.Param5").unwrap();
        assert_eq!(r.canonical, "D.P.2.SubTable.3.Param5");
        assert_eq!(r.instances, [2, 3]);
        assert!(
            matches!(r.target, Target::Node(n) if m.tree.node(n).kind == NodeKind::Property(Access::ReadOnly))
        );
        assert_eq!(
            m.resolve("D.P.1.Reset()").unwrap().canonical,
            "D.P.1.Reset()"
        );
        for missing in [
            "D.P.3.Param1",
            "D.P.[nope].Param1",
            "D.P.x.Param1",
            "D.P.1.Nope",
            "D.P.1.Param1.More",
            "D.P.1.Param1.",
            "X",
            "D..P",
            "D.P.*.Param1",
        ] {
            assert_eq!(
                m.resolve(missing),
                Err(Status::ElementDoesNotExist),
                "{missing}"
            );
        }
        let (t, r) = m.resolve_table("D.P.2.SubTable").unwrap();
        assert_eq!(
            (m.tree.node(t).kind, r.canonical.as_str()),
            (NodeKind::Table(Rows::Dynamic), "D.P.2.SubTable.")
        );
        assert_eq!(m.resolve_table("D.P.2."), Err(Status::ElementDoesNotExist));
        let (_, inst, r) = m.resolve_row("D.P.[two]").unwrap();
        assert_eq!((inst, r.canonical.as_str()), (2, "D.P.2."));
        assert_eq!(
            m.resolve_row("D.P.").map(|r| r.1),
            Err(Status::ElementDoesNotExist)
        );
    }

    #[test]
    fn partial_paths_walk_schema_then_row_order() {
        let m = partial_path();
        assert_eq!(
            under(&m, "D.P.1."),
            [
                "D.P.1.Param1",
                "D.P.1.Param2",
                "D.P.1.SubObject1.Param3",
                "D.P.1.SubObject1.Param4",
                "D.P.1.SubTable.1.Param5",
                "D.P.1.SubTable.1.SubObject2.Param6",
            ]
        );
        assert_eq!(under(&m, "D.P.").len(), 6 + 4 + 3 * 2);
        assert_eq!(
            under(&m, "D.").len(),
            16,
            "write-only and non-properties are skipped"
        );
        assert_eq!(
            under(&m, "D.P.[two].SubTable.2.SubObject2."),
            ["D.P.2.SubTable.2.SubObject2.Param6"]
        );
        assert_eq!(under(&m, "D.P.2.SubTable.*"), under(&m, "D.P.2.SubTable."));
        let found = {
            let mut out = Vec::new();
            m.properties_under(&m.resolve("D.P.2.SubTable.3.Param5").unwrap(), &mut out);
            out
        };
        assert_eq!(found[0].instances, [2, 3]);
        assert!(under(&m, "D.W").is_empty());
    }

    #[test]
    fn wildcards_fan_out_over_rows() {
        let m = partial_path();
        let all: Vec<String> = m
            .resolve_all("D.P.*.SubTable.*.Param5")
            .unwrap()
            .into_iter()
            .map(|r| r.canonical)
            .collect();
        assert_eq!(
            all,
            [
                "D.P.1.SubTable.1.Param5",
                "D.P.2.SubTable.1.Param5",
                "D.P.2.SubTable.2.Param5",
                "D.P.2.SubTable.3.Param5",
            ]
        );
        assert_eq!(m.resolve_all("D.*.Param1"), Err(Status::AccessNotAllowed));
        assert_eq!(
            m.resolve_all("D.P.*.Nope"),
            Err(Status::ElementDoesNotExist)
        );
    }

    #[test]
    fn element_names_follow_rbus_c() {
        let m = partial_path();
        let at = m.resolve("D.P.1.").unwrap();
        let level1: Vec<(String, i32, u32)> = m
            .element_names(&at, -1)
            .into_iter()
            .map(|e| (e.name, e.element_type, e.access))
            .collect();
        assert_eq!(
            level1,
            [
                ("D.P.1.Param1".into(), 1, 17),
                ("D.P.1.Param2".into(), 1, 17),
                ("D.P.1.SubObject1.".into(), 0, 0),
                ("D.P.1.SubTable.".into(), 2, 28),
                ("D.P.1.Reset()".into(), 4, 32),
            ]
        );
        let rows: Vec<String> = m
            .element_names(&m.resolve("D.P.").unwrap(), -1)
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(rows, ["D.P.1.", "D.P.2."]);
        let row = &m.element_names(&m.resolve("D.P.").unwrap(), -1)[0];
        assert_eq!((row.element_type, row.access), (0, 3));
        // Depth >= 0 includes the element itself and everything down to it.
        let all = m.element_names(&m.resolve("D.").unwrap(), 2);
        let all: Vec<&str> = all.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(all, ["D.", "D.P.", "D.P.1.", "D.P.2.", "D.W", "D.E!"]);
        let w = m.element_names(&m.resolve("D.W").unwrap(), 0);
        assert_eq!((w[0].element_type, w[0].access), (1, 18));
        let root = m.element_names(&Resolved::root(), -1);
        assert_eq!(root[0].name, "D.");
    }

    #[test]
    fn rows_are_unique_and_removed_with_their_children() {
        let mut m = partial_path();
        assert_eq!(
            m.add_row("D.P.", row(1, None)),
            Err(Status::ElementNameDuplicate)
        );
        assert_eq!(
            m.add_row("D.P.", row(9, Some("two"))),
            Err(Status::ElementNameDuplicate)
        );
        assert!(m.alias_taken("D.P.", "two") && !m.alias_taken("D.P.", "one"));
        assert_eq!(m.remove_row("D.P.", 2), Some(row(2, Some("two"))));
        assert!(m.rows("D.P.2.SubTable.").is_empty());
        assert_eq!(m.rows("D.P.1.SubTable.").len(), 1);
        assert_eq!(m.remove_row("D.P.", 2), None);
        assert_eq!(m.remove_row("D.Q.", 2), None);
    }

    #[test]
    fn rows_of_vanished_tables_are_dropped() {
        let mut m = partial_path();
        m.set_tree(
            Tree::build([(1, Schema::new().table("D.P.", Rows::Fixed).elements())]).unwrap(),
        );
        assert_eq!(m.rows("D.P.").len(), 2);
        assert!(m.rows("D.P.1.SubTable.").is_empty());
        m.set_tree(Tree::default());
        assert!(m.rows("D.P.").is_empty());
    }

    #[test]
    fn subscription_names_resolve_against_templates() {
        let m = partial_path();
        let node = |t: Target| match t {
            Target::Node(n) | Target::Table(n) | Target::Row(n, _) => m.tree.node(n).seg.clone(),
        };
        let t = m.resolve_template("D.P.*.SubTable.").unwrap();
        assert!(matches!(t, Target::Table(_)));
        assert_eq!(node(t), "SubTable");
        // Rows need not exist.
        let p = m.resolve_template("D.P.[nope].SubTable.9.Param5").unwrap();
        assert_eq!(node(p), "Param5");
        assert!(matches!(
            m.resolve_template("D.P.7."),
            Ok(Target::Row(_, 0))
        ));
        assert!(matches!(m.resolve_template("D.P.*"), Ok(Target::Table(_))));
        for bad in ["D.Q", "D.W.", "D.P.1.Param1.X", "", "D..P", "D.W.X"] {
            assert_eq!(
                m.resolve_template(bad),
                Err(Status::ElementDoesNotExist),
                "{bad}"
            );
        }
        assert!(m.matches("D.P.*.SubTable.", "D.P.2.SubTable."));
        assert!(m.matches("D.P.[two].Param1", "D.P.2.Param1"));
        assert!(!m.matches("D.P.[two].Param1", "D.P.1.Param1"));
        assert!(!m.matches("D.Q.", "D.Q.1."));
        // A row alias that only some rows of a `*` have.
        let mut m = partial_path();
        m.add_row("D.P.2.SubTable.", row(9, Some("nine"))).unwrap();
        assert!(m.matches("D.P.*.SubTable.[nine].Param5", "D.P.2.SubTable.9.Param5"));
        assert!(
            m.resolve_all("D.P.*.SubTable.[nine].Param5").is_err(),
            "gets stay strict"
        );
        assert_eq!(m.resolve_matching("D.P.*.SubTable.[nine].Param5").len(), 1);
        assert!(m.resolve_matching("D.Q.*").is_empty());
    }

    #[test]
    fn schemas_are_checked() {
        let one = |s: Schema| Tree::build([(1, s.elements())]).map(|_| ());
        let bad_name = |s: Schema| matches!(one(s), Err(SchemaError::Name(_)));
        assert!(bad_name(Schema::new().property("A.B.", Access::ReadOnly)));
        assert!(bad_name(Schema::new().property("A.B()", Access::ReadOnly)));
        assert!(bad_name(Schema::new().method("A.B")));
        assert!(bad_name(Schema::new().method("A.()")));
        assert!(bad_name(Schema::new().event("A.B()")));
        assert!(bad_name(
            Schema::new().method("A.M()").poll(Duration::from_secs(1))
        ));
        assert!(Tree::build([(1, Schema::new().event("A.T.{i}.Event").elements())]).is_ok());
        assert!(bad_name(Schema::new().property("A.*.B", Access::ReadOnly)));
        assert!(bad_name(
            Schema::new().property("A.[x].B", Access::ReadOnly)
        ));
        assert!(bad_name(Schema::new().property("A.{i}", Access::ReadOnly)));
        assert!(bad_name(Schema::new().property("A B", Access::ReadOnly)));
        let conflict = |s: Schema| matches!(one(s), Err(SchemaError::Conflict(_)));
        assert!(conflict(
            Schema::new()
                .property("A.B", Access::ReadOnly)
                .property("A.B.C", Access::ReadOnly)
        ));
        assert!(conflict(
            Schema::new()
                .property("A.B.C", Access::ReadOnly)
                .property("A.B", Access::ReadOnly)
        ));
        assert!(conflict(
            Schema::new()
                .table("A.T.", Rows::Fixed)
                .property("A.T.X", Access::ReadOnly)
        ));
        assert!(conflict(
            Schema::new()
                .property("A.T.X", Access::ReadOnly)
                .property("A.T.{i}.Y", Access::ReadOnly)
        ));
        assert!(conflict(Schema::new().property("{i}.Y", Access::ReadOnly)));
        assert!(conflict(
            Schema::new().property("A.T.{i}.{i}.Y", Access::ReadOnly)
        ));
        assert!(conflict(
            Schema::new()
                .property("A.T.X", Access::ReadOnly)
                .table("A.T.", Rows::Fixed)
        ));
        assert_eq!(
            one(Schema::new()
                .property("A.B", Access::ReadOnly)
                .property("A.B", Access::ReadWrite)),
            Err(SchemaError::Duplicate("A.B".into()))
        );
        // Two mounts may not share an element either.
        let a = Schema::new().property("A.B", Access::ReadOnly);
        assert!(matches!(
            Tree::build([(1, a.elements()), (2, a.elements())]),
            Err(SchemaError::Duplicate(_))
        ));
        // A table may be declared after names inside it implied it, and a
        // `{i}` below an unlisted table makes it a fixed one.
        let t = Tree::build([(
            1,
            Schema::new()
                .property("A.T.{i}.X", Access::ReadOnly)
                .table("A.T.", Rows::Dynamic)
                .property("A.U.{i}.X", Access::ReadOnly)
                .elements(),
        )])
        .unwrap();
        let mut m = Model::default();
        m.set_tree(t);
        let (u, _) = m.resolve_table("A.U.").unwrap();
        assert_eq!(
            (m.tree.node(u).kind, m.tree.node(u).owner),
            (NodeKind::Table(Rows::Fixed), None)
        );
        let (t, _) = m.resolve_table("A.T.").unwrap();
        assert_eq!(m.tree.node(t).owner, Some(1));
    }
}
