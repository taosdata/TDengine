use assert_fs::fixture::FileWriteStr;
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

    let mut cmd = assert_cmd::Command::cargo_bin("taos-explorer")?;
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

    let mut cmd = assert_cmd::Command::cargo_bin("taos-explorer")?;
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

    let mut cmd = assert_cmd::Command::cargo_bin("taos-explorer")?;
    let assert = cmd
        .arg("-c")
        .arg(config_file.path().to_str().unwrap())
        .timeout(std::time::Duration::from_secs(3))
        .assert();
    assert.interrupted();
    Ok(())
}
