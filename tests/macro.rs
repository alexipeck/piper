#[test]
fn pipeline_macro_compiles() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/trybuild/pipeline_pass.rs");
}
