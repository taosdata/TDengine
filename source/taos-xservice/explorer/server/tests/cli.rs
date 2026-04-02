use assert_fs::fixture::FileWriteStr;

#[test]
fn test_help() -> anyhow::Result<(), anyhow::Error> {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!();
    let assert = cmd.arg("--help").assert();
    assert.success().stdout(predicates::str::contains(
        "You can view the databases and tables with a tree structure.",
    ));
    Ok(())
}

#[test]
fn test_version() -> anyhow::Result<(), anyhow::Error> {
    for arg in ["--version", "-V"] {
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!();
        cmd.arg(arg)
            .assert()
            .success()
            .stdout(predicates::str::contains(env!("CARGO_PKG_VERSION")));
    }
    Ok(())
}

#[test]
fn test_config_file_not_exist() -> anyhow::Result<(), anyhow::Error> {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!();
    let assert = cmd
        .arg("-c")
        .arg("not_exist.toml")
        .timeout(std::time::Duration::from_secs(15))
        .assert();
    assert.failure().stderr(predicates::str::contains(
        "Custom configuration file not_exist.toml not found",
    ));
    Ok(())
}

#[test]
fn test_config_file_invalid_toml() -> anyhow::Result<(), anyhow::Error> {
    // config file
    let config_file = assert_fs::NamedTempFile::new("explorer.toml")?;
    config_file.write_str("port =\"")?;
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!();
    let assert = cmd
        .arg("-c")
        .arg(config_file.path().to_str().unwrap())
        .timeout(std::time::Duration::from_secs(15))
        .assert();
    assert.failure().stderr(predicates::str::contains(
        "Failed to parse configuration from",
    ));
    Ok(())
}

#[test]
fn test_config_type_invalid() -> anyhow::Result<(), anyhow::Error> {
    // config file
    let config_file = assert_fs::NamedTempFile::new("explorer.toml")?;
    config_file.write_str(
        r#"
port = "invalid_port"
"#,
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!();
    let assert = cmd
        .arg("-c")
        .arg(config_file.path().to_str().unwrap())
        .timeout(std::time::Duration::from_secs(15))
        .assert();
    assert
        .failure()
        .stderr(predicates::str::contains("invalid type: string"));
    Ok(())
}

#[test]
fn test_startup_normal() -> anyhow::Result<(), anyhow::Error> {
    // config file
    let config_file = assert_fs::NamedTempFile::new("explorer.toml")?;
    config_file.write_str(
        r#"
port = 0
addr = "127.0.0.1"
log_level = "info"
cluster = "http://localhost:6041"
x_api = "http://localhost:6050"
grpc = "http://localhost:6055"
cors = true
"#,
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!();
    let assert = cmd
        .arg("-c")
        .arg(config_file.path().to_str().unwrap())
        .timeout(std::time::Duration::from_secs(3))
        .assert();
    assert.interrupted();
    Ok(())
}

#[test]
fn test_startup_wrong_address() -> anyhow::Result<(), anyhow::Error> {
    // config file
    let config_file = assert_fs::NamedTempFile::new("explorer.toml")?;
    config_file.write_str(
        r#"
port = 0
addr = "512.0.0.0"
log_level = "info"
cluster = "http://localhost:6041"
x_api = "http://localhost:6050"
grpc = "http://localhost:6055"
cors = true
"#,
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!();
    let assert = cmd
        .arg("-c")
        .arg(config_file.path().to_str().unwrap())
        .timeout(std::time::Duration::from_secs(15))
        .assert();
    assert.failure();
    Ok(())
}

#[test]
fn test_startup_ssl() -> anyhow::Result<(), anyhow::Error> {
    // config file
    let config_file = assert_fs::NamedTempFile::new("explorer.toml")?;
    config_file.write_str(
        r#"
port = 0
addr = "0.0.0.0"
log_level = "info"
cluster = "http://localhost:6041"
x_api = "http://localhost:6050"
grpc = "http://localhost:6055"
cors = true
[ssl]
certificate = "tests/assets/cert.pem"
certificate_key = "tests/assets/cert-key.pem"
"#,
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!();
    let assert = cmd
        .arg("-c")
        .arg(config_file.path().to_str().unwrap())
        .timeout(std::time::Duration::from_secs(3))
        .assert();
    assert.interrupted();
    Ok(())
}
