//! The macros' own error messages (M6), checked with trybuild: each file in
//! `ui/` must fail to compile with the errors in its `.stderr` file. After a
//! deliberate change, regenerate them with `TRYBUILD=overwrite`.

#[test]
fn macro_errors_point_at_the_problem() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
