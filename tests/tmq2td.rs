use assert_cmd::{prelude::*, Command};

#[test]
fn test_td_33080_with_taos() -> anyhow::Result<(), anyhow::Error> {
    const SOURCE: &str = "td33080";
    const SINK: &str = "td33080s";
    {
        Command::new("taos")
            .args(["-s"])
            .arg(format!(
                "DROP TOPIC IF EXISTS `{SOURCE}`; DROP DATABASE IF EXISTS `{SOURCE}`; DROP DATABASE IF EXISTS `{SINK}`;"
            ))
            .output()
            .expect("failed to execute process")
            .assert()
            .append_context("taos", "clean-up resources")
            .success();
        // Prepare
        Command::new("taosBenchmark")
            .args(["-y", "-d", SOURCE, "-n", "100", "-t", "100"])
            .output()
            .expect("failed to execute process")
            .assert()
            .append_context("taosBenchmark", "insert with benchmark tool")
            .success();
        Command::new("taos")
            .arg("-s")
            .arg(format!(
                "CREATE TOPIC `{SOURCE}` as DATABASE `{SOURCE}`;CREATE DATABASE `{SINK}`;"
            ))
            .assert()
            .append_context("taos", "create topic without meta")
            .success();
    }
    let data_dir = tempfile::tempdir()?;
    let mut cmd = Command::cargo_bin("taosx")?;
    let now = chrono::Utc::now().timestamp_millis();
    cmd.arg("run")
        .arg("-f")
        .arg(format!("tmq:///{}?group.id={}&timeout=1s", SOURCE, now))
        .arg("-t")
        .arg(format!("taos:///{}", SINK))
        .env("TAOSX_DATA_DIR", data_dir.path())
        .timeout(std::time::Duration::from_secs(30))
        .assert()
        .append_context("taosx", "with default parameters")
        .success();

    Command::new("taos")
        .arg("-s")
        .arg(format!("DROP TABLE `{SINK}`.meters;"))
        .assert()
        .append_context("taos", "drop table meters in sink database")
        .success();
    let now = chrono::Utc::now().timestamp_millis();
    let data_dir = tempfile::tempdir()?;
    let mut cmd = Command::cargo_bin("taosx")?;
    cmd.arg("run")
        .arg("-f")
        .arg(format!(
            "tmq:///{}?group.id={}&enable.concurrent.polling=false&timeout=1s",
            SOURCE, now
        ))
        .arg("-t")
        .arg(format!("taos:///{}", SINK))
        .env("TAOSX_DATA_DIR", data_dir.path())
        .timeout(std::time::Duration::from_secs(30))
        .assert()
        .append_context("taosx", "with enable.concurrent.polling=false")
        .success();
    {
        Command::new("taos")
            .args(["-s"])
            .arg(format!(
                "DROP TOPIC IF EXISTS `{SOURCE}`; DROP DATABASE IF EXISTS `{SOURCE}`; DROP DATABASE IF EXISTS `{SINK}`;"
            ))
            .output()
            .expect("failed to execute process")
            .assert()
            .append_context("taos", "clean-up resources after all");
    }
    Ok(())
}
