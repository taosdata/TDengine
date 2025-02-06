use assert_cmd::{prelude::*, Command};

#[test]
fn test_td_33256_with_taos() -> anyhow::Result<(), anyhow::Error> {
    const SOURCE: &str = "td33256";
    const SINK: &str = "td33256s";
    const USER: &str = "td33256";
    const PASS: &str = "Ab1@#$%^&*()_+";
    {
        Command::new("taos")
            .args(["-s"])
            .arg(format!(
                "DROP TOPIC IF EXISTS `{SOURCE}`;
                DROP DATABASE IF EXISTS `{SOURCE}`;
                DROP DATABASE IF EXISTS `{SINK}`;
                DROP USER IF EXISTS `{USER}`;"
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
                "CREATE USER `{USER}` PASS '{PASS}';\
                GRANT ALL ON `{SOURCE}` TO `{USER}`;\
                CREATE DATABASE `{SINK}`;"
            ))
            .assert()
            .append_context("taos", "create topic without meta")
            .success();
    }
    let data_dir = tempfile::tempdir()?;
    let mut cmd = Command::cargo_bin("taosx")?;
    cmd.arg("run")
        .arg("-f")
        .arg(format!(
            "taos://{USER}:{PASS}@localhost:6030/{SOURCE}?mode=history"
        ))
        .arg("-t")
        .arg(format!("taos:///{}", SINK))
        .env("TAOSX_DATA_DIR", data_dir.path())
        .timeout(std::time::Duration::from_secs(30))
        .assert()
        .append_context("taosx", "with default parameters")
        .success();

    {
        Command::new("taos")
            .args(["-s"])
            .arg(format!(
                "DROP TOPIC IF EXISTS `{SOURCE}`; 
                DROP DATABASE IF EXISTS `{SOURCE}`;
                DROP DATABASE IF EXISTS `{SINK}`;
                DROP USER IF EXISTS `td33256`;"
            ))
            .output()
            .expect("failed to execute process")
            .assert()
            .append_context("taos", "clean-up resources after all")
            .success();
    }
    Ok(())
}
