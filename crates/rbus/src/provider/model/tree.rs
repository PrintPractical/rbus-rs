//! The tree of registered name templates.

use super::*;

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

    pub(super) fn child(&self, parent: NodeId, seg: &str) -> Option<NodeId> {
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
