use std::process::Command;

fn cargo_check(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO"))
        .arg("check")
        .arg("--release")
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo check should spawn");
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (output.status.success(), stderr)
}

#[test]
fn additive_kanal_feature_compiles() {
    let (ok, stderr) = cargo_check(&["--features", "channel-kanal"]);
    assert!(
        ok,
        "expected compile with default + channel-kanal: {stderr}"
    );
}

#[test]
fn kanal_wins_when_both_features_enabled() {
    let (ok, stderr) = cargo_check(&[
        "--no-default-features",
        "--features",
        "channel-kanal,channel-crossbeam",
    ]);
    assert!(
        ok,
        "kanal should take precedence when both are enabled: {stderr}"
    );
}

#[test]
fn no_channel_feature_fails_to_compile() {
    let (ok, stderr) = cargo_check(&["--no-default-features"]);
    assert!(!ok);
    assert!(
        stderr.contains("enable `channel-kanal` or `channel-crossbeam`"),
        "unexpected stderr: {stderr}"
    );
}
