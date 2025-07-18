use assert_cmd::Command;
use chrono::Utc;
use futures_util::TryStreamExt;
use rand::Rng;
use std::time::Duration;
use taos::{AsyncFetchable, AsyncQueryable};

/// # description
/// test sync database
/// 1. Create source and destination databases, and a topic that subscribes to the source database.
/// 2. Create 10 super tables in the source database and insert data into them.
/// 3. Create a sync task to sync data from the source database to the destination database.
/// 4. Verify that the super tables in the destination database match the data in the source database.
/// # description_cn
/// 同步数据库
/// 1. 创建数据库 DB_SRC 和 DB_DST；创建 topic，订阅 DB_SRC
/// 2. 在 DB_SRC 中创建超级表 stb1 ~ stb10，并插入数据
/// 3. 创建同步任务，将 DB_SRC 同步到 DB_DST
/// 4. 检查 DB_DST 中的超级表 stb1 ~ stb10 是否与 DB_SRC 中的数据，一致则用例通过，否则失败
/// # jira
/// Close https://jira.taosdata.com:18080/browse/TD-32960
/// # example
/// ```shell
/// cargo nextest run test_sync_database_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_database_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "td32960_001_src";
    const DB_DST: &str = "td32960_001_dst";
    const TOPIC: &str = "test_sync_database";
    const ROWS: u64 = 10;

    let taos = taosx_core::utils::sql::connect_taos(&host, ws_enable).await?;
    taos.exec_many(vec![
        format!("DROP TOPIC IF EXISTS force `{TOPIC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_DST}`"),
        format!("CREATE TOPIC IF NOT EXISTS {TOPIC} WITH META AS DATABASE `{DB_SRC}`"),
    ])
    .await?;
    for i in 1..=ROWS {
        taos.exec_many(vec![
            format!("CREATE TABLE `{DB_SRC}`.stb{i} (ts timestamp, val float) TAGS (id int)"),
            format!(
                "INSERT INTO `{DB_SRC}`.t{i} using `{DB_SRC}`.stb{i} TAGS({i}) VALUES (now, {i}.{i})",
            ),
        ])
            .await?;
    }

    // create sync task
    let (from, to) = if ws_enable {
        let from = format!("tmq+ws://{host}:6041/{TOPIC}");
        let to = format!("taos+ws://{host}:6041/{DB_DST}");
        (from, to)
    } else {
        let from = format!("tmq://{host}/{TOPIC}");
        let to = format!("taos://{host}/{DB_DST}");
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from)
        .arg("-t")
        .arg(to)
        .assert()
        .success();

    // check data
    for i in 1..=ROWS {
        let (ts_src, val_src): (i64, f64) = taos
            .query_one(format!("SELECT ts, val FROM `{DB_SRC}`.stb{i}"))
            .await?
            .unwrap_or((0, 0.0));
        let (ts_dst, val_dst): (i64, f64) = taos
            .query_one(format!("SELECT ts, val FROM `{DB_DST}`.stb{i}"))
            .await?
            .unwrap_or((0, 0.0));
        assert_eq!(ts_src, ts_dst);
        assert_eq!(val_src, val_dst);
    }

    // clean
    taos.exec_many(vec![
        format!("DROP TOPIC IF EXISTS force `{TOPIC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
    ])
    .await?;

    Ok(())
}

/// # description
/// test sync super table
/// 1. Create source and destination databases, and 10 super tables in the source database.
/// 2. Create a topic that subscribes to one of the super tables in the source database.
/// 3. Create a sync task to sync the topic to the destination database.
/// 4. Verify that the super table in the destination database matches the data in the source database, and that there is only one super table in the destination database.
/// # description_cn
/// 同步超级表
/// 1. 创建数据库 DB_SRC 和 DB_DST；在 DB_SRC 中创建超级表 stb1 ~ stb10，并插入数据
/// 2. 创建 topic，随机订阅 DB_SRC 中的一个超级表
/// 3. 创建同步任务，将 TOPIC 同步到 DB_DST
/// 4. 检查 DB_DST 中的超级表是否与 DB_SRC 中的数据一致，且 DB_DST 中只有一个超级表，否则用例失败
/// # jira
/// Close https://jira.taosdata.com:18080/browse/TD-32960
/// # example
/// ```shell
/// cargo nextest run test_sync_stable_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_stable_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "td32960_002_src";
    const DB_DST: &str = "td32960_002_dst";
    const TOPIC: &str = "test_sync_stable";
    const TABLES: u64 = 10;

    // create database
    let taos = taosx_core::utils::sql::connect_taos(&host, ws_enable).await?;
    taos.exec_many(vec![
        format!("DROP TOPIC IF EXISTS force `{TOPIC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_DST}`"),
    ])
    .await?;
    for i in 1..=TABLES {
        taos.exec_many(vec![
            format!("CREATE STABLE `{DB_SRC}`.stb{i} (ts timestamp, val float) TAGS (id int)"),
            format!("INSERT INTO `{DB_SRC}`.t{i} USING `{DB_SRC}`.stb{i} TAGS({i}) VALUES (now, {i}.{i})", ),
        ])
            .await?;
    }
    let table_idx = rand::thread_rng().gen_range(1..=TABLES);
    taos.exec(format!(
        "CREATE TOPIC IF NOT EXISTS {TOPIC} WITH META AS STABLE `{DB_SRC}`.stb{table_idx}",
    ))
    .await?;

    // create sync task
    let (from, to) = if ws_enable {
        let from = format!("tmq+ws://{host}:6041/{TOPIC}");
        let to = format!("taos+ws://{host}:6041/{DB_DST}");
        (from, to)
    } else {
        let from = format!("tmq://{host}/{TOPIC}");
        let to = format!("taos://{host}/{DB_DST}");
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from)
        .arg("-t")
        .arg(to)
        .assert()
        .success();

    // check data
    let (ts_src, val_src): (i64, f64) = taos
        .query_one(format!("SELECT ts, val FROM `{DB_SRC}`.stb{table_idx}"))
        .await?
        .unwrap_or((0, 0.0));
    let (ts_dst, val_dst): (i64, f64) = taos
        .query_one(format!("SELECT ts, val FROM `{DB_DST}`.stb{table_idx}"))
        .await?
        .unwrap_or((0, 0.0));
    assert_eq!(ts_src, ts_dst);
    assert_eq!(val_src, val_dst);
    let tbname_dst: Vec<String> = taos
        .query(format!("show `{DB_DST}`.tables"))
        .await?
        .deserialize::<String>()
        .try_collect()
        .await?;
    assert_eq!(tbname_dst.len(), 1);
    assert_eq!(tbname_dst[0], format!("t{table_idx}"));

    // clean
    taos.exec_many(vec![
        format!("DROP TOPIC IF EXISTS force `{TOPIC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
    ])
    .await?;

    Ok(())
}

/// # description
/// test sync query
/// 1. Create source and destination databases, and a super table in the source database.
/// 2. Create a topic that subscribes to a SELECT query from the source database.
/// 3. Create a sync task to sync the topic to the destination database.
/// 4. Verify that the data in the destination database matches the result of the SELECT query from the source database.
/// # description_cn
/// 同步一个 SELECT 查询
/// 1. 创建数据库 DB_SRC 和 DB_DST；在 DB_SRC 中创建超级表 stb，并插入数据
/// 2. 创建 topic，订阅 DB_SRC 中的一个 SELECT 查询结果；同时，在 DB_DST 中创建一个普通表，表结构与查询结果一致
/// 3. 创建同步任务，将 TOPIC 同步到 DB_DST
/// 4. 检查 DB_DST 中的表数据是否与 DB_SRC 中的查询结果一致，否则用例失败
/// # jira
/// Close https://jira.taosdata.com:18080/browse/TD-32960
/// # example
/// ```shell
/// cargo nextest run test_sync_query_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_query_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "td32960_003_src";
    const DB_DST: &str = "td32960_003_dst";
    const TOPIC: &str = "test_sync_query";
    const ROWS: u64 = 10;

    // create database
    let taos = taosx_core::utils::sql::connect_taos(&host, ws_enable).await?;
    let now = Utc::now();
    taos.exec_many(vec![
        format!("DROP TOPIC IF EXISTS FORCE `{TOPIC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_DST}`"),
        format!("CREATE TABLE `{DB_SRC}`.stb (ts timestamp, f1 int, f2 float, f3 varchar(10)) TAGS (id int)"),
    ])
        .await?;
    for i in 1..=ROWS {
        taos.exec(format!(
            "INSERT INTO `{DB_SRC}`.t{i} USING `{DB_SRC}`.stb TAGS({i}) VALUES ({}, {i}, {i}.{i}, 'hello-{i}')",
            (now + Duration::from_secs(i)).timestamp_millis()
        ))
            .await?;
    }
    taos.exec_many(vec![
        format!("CREATE TABLE `{DB_DST}`.`{TOPIC}` (ts timestamp, f1 int, id int)"),
        format!("CREATE TOPIC `{TOPIC}` AS SELECT ts,f1,id FROM `{DB_SRC}`.stb"),
    ])
    .await?;

    // create sync task
    let (from, to) = if ws_enable {
        let from = format!("tmq+ws://{host}:6041/{TOPIC}");
        let to = format!("taos+ws://{host}:6041/{DB_DST}");
        (from, to)
    } else {
        let from = format!("tmq://{host}/{TOPIC}");
        let to = format!("taos://{host}/{DB_DST}");
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from)
        .arg("-t")
        .arg(to)
        .assert()
        .success();

    // check data
    let rows_src = taos
        .query(format!("SELECT * FROM `{DB_SRC}`.stb ORDER BY ts ASC"))
        .await?
        .deserialize::<(i64, i32, f32, String, i32)>()
        .try_collect::<Vec<_>>()
        .await?;
    let rows_dst = taos
        .query(format!(
            "SELECT * FROM `{DB_DST}`.`{TOPIC}` ORDER BY ts ASC"
        ))
        .await?
        .deserialize::<(i64, i32, i32)>()
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(rows_src.len(), rows_dst.len());
    for (row_src, row_dst) in rows_src.iter().zip(rows_dst.iter()) {
        assert_eq!(row_src.0, row_dst.0); // ts
        assert_eq!(row_src.1, row_dst.1); // f1
        assert_eq!(row_src.4, row_dst.2); // id
    }

    // clean
    taos.exec_many(vec![
        format!("DROP TOPIC IF EXISTS FORCE `{TOPIC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
    ])
    .await?;

    Ok(())
}

/// # description
/// test sync database with add tag action
/// 1. Create source and destination databases, and two super tables in the source database.
/// 2. Create a topic that subscribes to the source database.
/// 3. Create a sync task to sync the source database to the destination database, and add a tag `location=beijing` to the super tables in the destination database.
/// 4. Verify that the super tables in the destination database match the data in the source database, and that each row has the tag `location=beijing`.
/// # description_cn
/// 同步数据库，并且为 DB_SRC 的超级表添加一个 TAG
/// 1. 创建数据库 DB_SRC 和 DB_DST；在 DB_SRC 中创建超级表 stb1 和 stb2，并插入数据
/// 2. 创建 topic，订阅 DB_SRC
/// 3. 创建同步任务，将 DB_SRC 同步到 DB_DST，并且指定 Action: `add-tag:location=beijing`
/// 4. 检查 DB_DST 中的超级表 stb1 和 stb2 是否与 DB_SRC 中的数据一致，且每行数据都添加了 location=beijing 的 TAG，否则用例失败
/// # jira
/// Close https://jira.taosdata.com:18080/browse/TD-32960
/// # example
/// ```shell
/// cargo nextest run test_add_tag_action_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_add_tag_action_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "td32960_004_src";
    const DB_DST: &str = "td32960_004_dst";
    const TOPIC: &str = "test_add_tag_action";
    const ROWS: u64 = 10;
    const STABLES: u64 = 2;

    // create database
    let taos = taosx_core::utils::sql::connect_taos(&host, ws_enable).await?;
    taos.exec_many(vec![
        format!("DROP TOPIC IF EXISTS force `{TOPIC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}`"),
        format!("CREATE DATABASE IF NOT EXISTS `{DB_DST}`"),
        format!("CREATE TABLE `{DB_SRC}`.stb1 (ts timestamp, val float) TAGS (id int)"),
        format!("CREATE TABLE `{DB_SRC}`.stb2 (ts timestamp, val float) TAGS (id int)"),
    ])
    .await?;
    for row_idx in 1..=ROWS {
        for stb_idx in 1..=STABLES {
            taos.exec(format!(
                "INSERT INTO `{DB_SRC}`.t{row_idx}{stb_idx} USING `{DB_SRC}`.stb{stb_idx} TAGS({row_idx}{stb_idx}) VALUES (now, {row_idx}.{stb_idx})"
            ))
            .await?;
        }
    }
    taos.exec(format!(
        "CREATE TOPIC `{TOPIC}` WITH META AS DATABASE `{DB_SRC}`"
    ))
    .await?;

    // create sync task
    let (from, to) = if ws_enable {
        let from = format!("tmq+ws://{host}:6041/{TOPIC}");
        let to = format!("taos+ws://{host}:6041/{DB_DST}");
        (from, to)
    } else {
        let from = format!("tmq://{host}/{TOPIC}");
        let to = format!("taos://{host}/{DB_DST}");
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from)
        .arg("-t")
        .arg(to)
        .args(["-T", "add-tag:location=beijing"])
        .assert()
        .success();

    // check data
    for i in 1..=STABLES {
        let rows_src = taos
            .query(format!("SELECT * FROM `{DB_SRC}`.stb{i} ORDER BY ts ASC"))
            .await?
            .deserialize::<(i64, f32, i32)>()
            .try_collect::<Vec<_>>()
            .await?;
        let rows_dst = taos
            .query(format!("SELECT * FROM `{DB_DST}`.stb{i} ORDER BY ts ASC"))
            .await?
            .deserialize::<(i64, f32, i32, String)>()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(rows_src.len(), rows_dst.len());
        for (row_src, row_dst) in rows_src.iter().zip(rows_dst.iter()) {
            assert_eq!(row_src.0, row_dst.0); // ts
            assert_eq!(row_src.1, row_dst.1); // val
            assert_eq!(row_src.2.to_string(), row_dst.2.to_string()); // id
            assert_eq!(row_dst.3, "beijing"); // location tag
        }
    }

    // clean
    taos.exec_many(vec![
        format!("DROP TOPIC IF EXISTS force `{TOPIC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
        format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
    ])
    .await?;

    Ok(())
}
