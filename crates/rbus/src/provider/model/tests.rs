//! The model against schemas and rows, without a provider.

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
    // Extreme depths from the wire neither overflow nor recurse forever.
    assert!(
        m.element_names(&m.resolve("D.").unwrap(), i32::MIN)
            .is_empty()
    );
    assert_eq!(
        m.element_names(&m.resolve("D.").unwrap(), i32::MAX),
        m.element_names(&m.resolve("D.").unwrap(), 100),
    );
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
    m.set_tree(Tree::build([(1, Schema::new().table("D.P.", Rows::Fixed).elements())]).unwrap());
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
