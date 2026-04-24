use std::{env, path::Path, time::Duration};

use taos::*;

async fn wait_for_count(taos: &Taos, sql: &str, expected: i32) -> anyhow::Result<i32> {
    const MAX_ATTEMPTS: usize = 20;
    const RETRY_DELAY: Duration = Duration::from_secs(3);

    let mut last = 0;
    for attempt in 1..=MAX_ATTEMPTS {
        let count = taos.query_one::<_, i32>(sql).await?.unwrap_or(0);
        if count == expected {
            return Ok(count);
        }

        last = count;
        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }

    Err(anyhow::anyhow!(
        "timed out waiting for `{sql}` to reach {expected}; last observed {last}"
    ))
}

#[tokio::test]
async fn test_td2local_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "td2local_src";
    const DB_DST: &str = "td2local_dst";

    // Create source and destination databases with seed data.
    let taos = taosx_core::utils::sql::connect_taos(&host, ws_enable).await?;
    taos.exec_many(vec![
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_DST}`"),
        format!(
            "CREATE TABLE `{DB_SRC}`.t(ts timestamp, val double, a int, b varchar(120), c bool)"
        ),
        format!("INSERT INTO `{DB_SRC}`.t VALUES(now, 3.1415926, 1, 'hello', true)"),
        format!("CREATE TABLE `{DB_SRC}`.stb(ts timestamp, f1 int) TAGS(id int)"),
        format!(
            "INSERT INTO `{DB_SRC}`.t1 USING `{DB_SRC}`.stb TAGS(1) VALUES('2025-09-01T12:00:00+0800', 1)"
        ),
        format!(
            "INSERT INTO `{DB_SRC}`.t2 USING `{DB_SRC}`.stb TAGS(2) VALUES('2025-09-01T12:00:00+0800', 2)"
        ),
        format!(
            "INSERT INTO `{DB_SRC}`.t3 USING `{DB_SRC}`.stb TAGS(3) VALUES('2025-09-01T12:00:00+0800', 3)"
        ),
        // format!("CREATE TABLE `{DB_SRC}`.`Abc`(ts timestamp, f1 float, f2 int, f3 varchar(20)) TAGS(id int, name varchar(200))"),
        // format!("INSERT INTO `{DB_SRC}`.`Ctb1` USING `{DB_SRC}`.`Abc` TAGS(1, 'child table 1') VALUES('2025-09-01T12:00:00+0800', 1.11, 1, 'abc1')"),
        // format!("INSERT INTO `{DB_SRC}`.`Ctb2` USING `{DB_SRC}`.`Abc` TAGS(2, 'child table 2') VALUES('2025-09-01T12:00:00+0800', 2.22, 2, 'abc2')"),
        // format!("INSERT INTO `{DB_SRC}`.`Ctb3` USING `{DB_SRC}`.`Abc` TAGS(3, 'child table 3') VALUES('2025-09-01T12:00:00+0800', 3.33, 3, 'abc3')"),
    ])
    .await?;

    // Prepare the backup directory for this test run.
    let tmp_dir = tempfile::tempdir()?;
    let backup_dir = match env::var("LOCAL_DIR").ok() {
        Some(p) => {
            let p = Path::new(&p);
            if !p.exists() {
                std::fs::create_dir_all(p)?;
            }
            p.to_path_buf()
        }
        None => tmp_dir.path().to_path_buf(),
    };

    let (from, to) = if ws_enable {
        let from = format!("taos+ws://{host}:6041/{DB_SRC}?workers=4");
        let to = format!("local:{}", backup_dir.display());
        (from, to)
    } else {
        let from = format!("taos://{host}:6030/{DB_SRC}?workers=4");
        let to = format!("local:{}", backup_dir.display());
        (from, to)
    };
    let logs_dir = backup_dir.join("logs");
    std::fs::create_dir_all(&logs_dir)?;

    // Run the backup: taosx run -f "taos://..." -t "local:..."
    let mut taosx = assert_cmd::cargo::cargo_bin_cmd!("taosx");
    taosx
        .args(["run", "-f", &from, "-t", &to, "-v"])
        .env("TAOSX_DATA_DIR", backup_dir.as_path())
        .env("TAOSX_LOGS_HOME", logs_dir.as_path())
        .assert()
        .success();

    // Verify the backup files were created.
    let meta_file = backup_dir.join("schema.meta");
    assert!(meta_file.exists());
    for entry in std::fs::read_dir(&backup_dir)? {
        // Print the directory content for debugging when the test fails.
        let entry = entry?;
        let path = entry.path();
        println!("found entry: {}", path.display());
        // The backup should include schema metadata plus zfile payloads.
        if path.is_file() {
            let file_name = path.file_name().unwrap().to_string_lossy();
            dbg!(&file_name);
        }
    }

    let (from, to) = if ws_enable {
        let from = format!("local:{}?watch=false", backup_dir.display());
        let to = format!("taos+ws://{host}:6041/{DB_DST}");
        (from, to)
    } else {
        let from = format!("local:{}?watch=false", backup_dir.display());
        let to = format!("taos://{host}:6030/{DB_DST}");
        (from, to)
    };
    // Run the restore in one-shot mode so local_to_taos exits after
    // processing the current backup files instead of watching forever.
    let mut taosx = assert_cmd::cargo::cargo_bin_cmd!("taosx");
    taosx
        .args(["run", "-f", &from, "-t", &to, "-v"])
        .env("TAOSX_DATA_DIR", backup_dir.as_path())
        .env("TAOSX_LOGS_HOME", logs_dir.as_path())
        .assert()
        .success();

    // Wait until the restored rows are visible in the target database.
    let count = wait_for_count(&taos, &format!("SELECT COUNT(*) FROM `{DB_DST}`.t"), 1).await?;
    assert_eq!(count, 1);

    let count = wait_for_count(&taos, &format!("SELECT COUNT(*) FROM `{DB_DST}`.stb"), 3).await?;
    assert_eq!(count, 3);

    // let count: i32 = taos
    //     .query_one(format!("SELECT COUNT(*) FROM `{DB_DST}`.`Abc`"))
    //     .await
    //     .unwrap()
    //     .unwrap_or(0);
    // assert_eq!(count, 3);

    Ok(())
}
