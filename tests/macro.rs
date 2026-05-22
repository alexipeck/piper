#[test]
fn pipeline_macro_compiles() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/trybuild/pipeline_pass.rs");
    tests.compile_fail("tests/trybuild/graph_type_fail.rs");
}
