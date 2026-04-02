use std::{env, path::Path};

use taos::*;

#[tokio::test]
async fn test_td2local_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "td2local_src";
    const DB_DST: &str = "td2local_dst";

    // 建库建表，插入测试数据
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

    // 创建临时目录作为备份目录
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
    // 执行备份：taosx run -f "taos://..." -t "local:..."
    let mut taosx = assert_cmd::cargo::cargo_bin_cmd!("taosx");
    taosx
        .args(["run", "-f", &from, "-t", &to, "-v"])
        .env("TAOSX_DATA_DIR", backup_dir.as_path())
        .assert()
        .success();

    // 检查备份文件是否生成
    let meta_file = backup_dir.join("schema.meta");
    assert!(meta_file.exists());
    for entry in std::fs::read_dir(&backup_dir)? {
        // 列出目录内容
        let entry = entry?;
        let path = entry.path();
        println!("found entry: {}", path.display());
        // 备份文件包括：data.bin.* 和 schema.meta
        if path.is_file() {
            let file_name = path.file_name().unwrap().to_string_lossy();
            dbg!(&file_name);
        }
    }

    let (from, to) = if ws_enable {
        let from = format!("local:{}", backup_dir.display());
        let to = format!("taos+ws://{host}:6041/{DB_DST}");
        (from, to)
    } else {
        let from = format!("local:{}", backup_dir.display());
        let to = format!("taos://{host}:6030/{DB_DST}");
        (from, to)
    };
    // 执行恢复：taosx run -f "local:..." -t "taos://..."
    let mut taosx = assert_cmd::cargo::cargo_bin_cmd!("taosx");
    taosx
        .args(["run", "-f", &from, "-t", &to, "-v"])
        .env("TAOSX_DATA_DIR", backup_dir.as_path())
        .assert()
        .success();

    // 检查恢复结果
    let count: i32 = taos
        .query_one(format!("SELECT COUNT(*) FROM `{DB_DST}`.t"))
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(count, 1);

    let count: i32 = taos
        .query_one(format!("SELECT COUNT(*) FROM `{DB_DST}`.stb"))
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(count, 3);

    // let count: i32 = taos
    //     .query_one(format!("SELECT COUNT(*) FROM `{DB_DST}`.`Abc`"))
    //     .await
    //     .unwrap()
    //     .unwrap_or(0);
    // assert_eq!(count, 3);

    Ok(())
}
