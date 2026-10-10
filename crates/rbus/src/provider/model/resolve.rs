//! Resolving names against the tree and the rows that exist.

use super::*;

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
    pub(super) fn root() -> Self {
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

    fn names_rec(&self, at: &Resolved, depth: i32, level: u32, out: &mut Vec<ElementName>) {
        // Unsigned: the depth comes off the wire, and `i32::MIN.abs()` overflows.
        let max = depth.unsigned_abs();
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
