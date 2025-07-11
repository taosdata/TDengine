use assert_cmd::{Command, prelude::*};
use chrono::Utc;
use itertools::Itertools;
use legacy_to_taos::legacy_to_taos;
use std::time::Duration;
use taos::{AsyncQueryable, AsyncTBuilder, IntoDsn, TaosBuilder};
use taosx_core::{core_metrics::clear_metrics, get_data_dir};
use tokio_util::sync::CancellationToken;

/// # description
/// test schema only sync task from DB_SRC, which has streams, to DB_DST
/// 1. create databases, stable and stream
/// 2. write some data
/// 3. create a schema only synchronization task from DB_SRC to DB_DST
/// 4. write some data in new tables
/// 5. create a schema only synchronization task from DB_SRC to DB_DST
/// 6. check the schema of DB_DST
/// # description_cn
/// 同步 stream 创建的表 schema
/// 1. 创建数据库：DB_SRC 和 DB_DST，在 DB_SRC 建超级表，建 stream；
/// 2. 写入数据到 DB_SRC；
/// 3. 创建数据同步任务，schema=only；
/// 4. 写入数据到 DB_SRC，stream 会产生新的表和数据；
/// 5. 再次执行数据同步任务，schema=only；
/// 6. 检查 DB_SRC 和 DB_DST 的表，schema 一致，用例通过，否则失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TS-6499
/// # example
/// ```shell
/// cargo nextest run test_ts6499_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_ts6499_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    const DB_SRC: &str = "ts6499_src";
    const DB_DST: &str = "ts6499_dst";
    const STREAM: &str = "current_state_window";
    const TID: i64 = 6499;
    let ws_enable = true;

    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // 1. create databases, stable and stream
    println!("====== create databases and stream =====");
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("\
        drop stream if exists `{STREAM}`;\
        drop database if exists `{DB_SRC}`;\
        drop database if exists `{DB_DST}`;\
        create database if not exists `{DB_SRC}`;\
        create database if not exists `{DB_DST}`;\
        create table `{DB_SRC}`.`meters`(ts timestamp, val float) tags(id int);\
        create stream `{STREAM}` into `{DB_SRC}`.`{STREAM}` as select tbname,_wstart,avg(val) from `{DB_SRC}`.meters partition by tbname state_window(cast(val as int));\
        "
        ))
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty());

    // 2. write some data in DB_SRC
    println!("======= write data =====");
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("\
            use {DB_SRC};\
            insert into t1 using meters tags(1) values(now, 11.1),(now+1s,11.2),(now+2s,11.3),(now+3s,10.1),(now+4s,10.2),(now+5s,10.3);\
            insert into t2 using meters tags(2) values(now, 22.1),(now+1s,22.2),(now+2s,22.3),(now+3s,21.1),(now+4s,21.2),(now+5s,21.3);\
            insert into t3 using meters tags(3) values(now, 33.1),(now+1s,33.2),(now+2s,33.3),(now+3s,32.1),(now+4s,32.2),(now+5s,32.3);\
            insert into t4 using meters tags(4) values(now, 44.1),(now+1s,44.2),(now+2s,44.3),(now+3s,43.1),(now+4s,43.2),(now+5s,43.3);\
            insert into t5 using meters tags(5) values(now, 55.1),(now+1s,55.2),(now+2s,55.3),(now+3s,54.1),(now+4s,54.2),(now+5s,55.3);\
            "))
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 3. create a schema only synchronization task
    println!("====== start schema only synchronization task =====");
    let (from, to) = if ws_enable {
        let from = format!("taos+ws://{host}:6041/{DB_SRC}?schema=only").into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!("taos://{host}/{DB_SRC}??schema=only").into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    let cancel = CancellationToken::new();
    let res = legacy_to_taos(from, vec![], to, cancel, Some(TID)).await;
    assert!(res.is_ok());

    // 4. write some data in DB_SRC
    println!("======= write data again =====");
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("\
            use {DB_SRC};\
            insert into t6 using meters tags(6) values(now, 11.1),(now+1s,11.2),(now+2s,11.3),(now+3s,10.1),(now+4s,10.2),(now+5s,10.3);\
            insert into t7 using meters tags(7) values(now, 22.1),(now+1s,22.2),(now+2s,22.3),(now+3s,21.1),(now+4s,21.2),(now+5s,21.3);\
            insert into t8 using meters tags(8) values(now, 33.1),(now+1s,33.2),(now+2s,33.3),(now+3s,32.1),(now+4s,32.2),(now+5s,32.3);\
            insert into t9 using meters tags(9) values(now, 44.1),(now+1s,44.2),(now+2s,44.3),(now+3s,43.1),(now+4s,43.2),(now+5s,43.3);\
            insert into t10 using meters tags(10) values(now, 55.1),(now+1s,55.2),(now+2s,55.3),(now+3s,54.1),(now+4s,54.2),(now+5s,55.3);\
            "))
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 5. create a schema only synchronization task
    println!("====== start schema only synchronization task again =====");
    let (from, to) = if ws_enable {
        let from = format!("taos+ws://{host}:6041/{DB_SRC}?schema=only").into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!("taos://{host}/{DB_SRC}??schema=only").into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    let cancel = CancellationToken::new();
    let res = legacy_to_taos(from, vec![], to, cancel, Some(TID)).await;
    assert!(res.is_ok());

    // 6. check the schema
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("select table_name from information_schema.ins_tables where db_name = '{DB_SRC}' order by table_name asc;"))
        .output()?;
    let src_out = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|l| l.contains("|"))
        .map(|l| l.to_string())
        .collect_vec();
    let src_err = String::from_utf8_lossy(&output.stderr);
    assert!(src_err.is_empty(), "{}", src_err);

    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("select table_name from information_schema.ins_tables where db_name = '{DB_DST}' order by table_name asc;"))
        .output()?;
    let dst_out = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|l| l.contains("|"))
        .map(|l| l.to_string())
        .collect_vec();
    let dst_err = String::from_utf8_lossy(&output.stderr);
    assert!(dst_err.is_empty(), "{}", dst_err);

    assert_eq!(src_out, dst_out);

    // 7. clean
    println!("====== clean up =====");
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "\
        drop stream if exists `{STREAM}`;\
        drop database if exists `{DB_SRC}`;\
        drop database if exists `{DB_DST}`;\
        "
        ))
        .output()?;
    let out = String::from_utf8_lossy(&output.stdout);
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", out);

    Ok(())
}

/// # description
/// This case test the breakpoint recovery of realtime data synchronization
/// # description_cn
/// Realtime 模式支持从断点开始同步
/// 1. 建2个数据库：DB_SRC 和 DB_DST，在 DB_SRC 内建表；
/// 2. 创建同步任务，mode=realtime，sparse=true，运行 60 秒后，停止；
/// 3. 写入数据到 DB_SRC；
/// 4. 重启同步任务，运行 60 秒后，停止
/// 5. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// Close https://jira.taosdata.com:18080/browse/TS-6402
/// # example
/// ```shell
/// cargo nextest run test_ts6402_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_ts6402_with_taos() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    const DB_SRC: &str = "test_ts6402_src";
    const DB_DST: &str = "test_ts6402_dst";

    // create databases and tables
    println!("=========CREATE DATABASE AND WRITE=========");
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "drop database if exists `{DB_SRC}`;\
            drop database if exists `{DB_DST}`;\
            create database if not exists `{DB_SRC}`;\
            create database if not exists `{DB_DST}`;\
            create table `{DB_SRC}`.`meters`(ts timestamp, val float) tags(id int);\
            "
        ))
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);

    let from = format!(
        "taos://{host}/{DB_SRC}?mode=realtime&schema=always&schema-polling-interval=5s&sparse=true"
    )
    .into_dsn()?;
    let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
    let tid = 6402;

    let breakpoints_dir = get_data_dir()
        .join("tasks")
        .join(tid.to_string())
        .join("breakpoints");
    if breakpoints_dir.exists() {
        std::fs::remove_dir_all(&breakpoints_dir)?;
    }

    // 1. create a legacy_to_taos task
    println!("=========START LEGACY TO TAOS TASK=========");
    let cancel = CancellationToken::new();
    let from_clone = from.clone();
    let to_clone = to.clone();
    let cancel_clone = cancel.clone();
    let h = tokio::spawn(async move {
        let _ = legacy_to_taos(from_clone, vec![], to_clone, cancel_clone, Some(tid)).await;
    });
    tokio::time::sleep(Duration::from_secs(60)).await;
    cancel.cancel();
    h.await?;

    // 2. write some data
    println!("=========WRITE SOME DATA=========");
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "insert into `{DB_SRC}`.`t1` using `{DB_SRC}`.`meters` tags(1) values (now, 11.0);\
            insert into `{DB_SRC}`.`t2` using `{DB_SRC}`.`meters` tags(2) values (now, 22.0);\
            insert into `{DB_SRC}`.`t3` using `{DB_SRC}`.`meters` tags(3) values (now, 33.0);\
            insert into `{DB_SRC}`.`t4` using `{DB_SRC}`.`meters` tags(4) values (now, 44.0);\
            insert into `{DB_SRC}`.`t5` using `{DB_SRC}`.`meters` tags(5) values (now, 55.0);\
            "
        ))
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);

    // 3. restart the legacy_to_taos task
    dbg!("=========RESTART LEGACY TO TAOS TASK=========");
    let cancel = CancellationToken::new();
    let from_clone = from.clone();
    let to_clone = to.clone();
    let cancel_clone = cancel.clone();
    let h = tokio::spawn(async move {
        let _ = legacy_to_taos(from_clone, vec![], to_clone, cancel_clone, Some(tid)).await;
    });
    tokio::time::sleep(Duration::from_secs(60)).await;
    cancel.cancel();
    h.await?;

    // 4. check the data
    println!("=========CHECK DATA=========");
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("select count(*) from `{DB_SRC}`.`meters`"))
        .output()?;
    let src_out: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_owned())
        .collect();
    let src_err = String::from_utf8_lossy(&output.stderr);
    assert!(src_err.is_empty(), "{}", src_err);

    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("select count(*) from `{DB_DST}`.`meters`"))
        .output()?;
    let dst_out = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_string())
        .collect::<Vec<String>>();
    let dst_err = String::from_utf8_lossy(&output.stderr);
    assert!(dst_err.is_empty(), "{}", dst_err);

    let count_src = src_out.get(6);
    let count_dst = dst_out.get(6);
    assert_eq!(count_src, count_dst);

    // 5. drop databases
    Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "drop database if exists `{DB_SRC}`;
            drop database if exists `{DB_DST}`;
            "
        ))
        .output()?
        .assert()
        .success();

    Ok(())
}

/// # description
/// This case test the password with special characters
/// # description_cn
/// 密码中带特殊字符
/// 1. 创建数据库 SOURCE，向 SOURCE 中写入 1 万行；
/// 2. 创建 USER，密码带特殊字符，grant all on SOURCE to USER；
/// 3. 创建数据库 SINK；
/// 4. 创建数据同步任务，mode=history
/// 5. 任务成功后，检查 SOURCE 和 SINK 的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-33256
/// # example
/// ```shell
/// cargo nextest run test_td_33256_with_taos --nocapture --retries 0
/// ```
#[test]
fn test_td_33256_with_taos() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt().with_level(true).init();

    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    const SOURCE: &str = "td33256";
    const SINK: &str = "td33256s";
    const USER: &str = "td33256";
    const PASS: &str = "Ab1@#$%^&*()_+";

    // Prepare
    Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "DROP TOPIC IF EXISTS `{SOURCE}`;\
            DROP DATABASE IF EXISTS `{SOURCE}`;\
            DROP DATABASE IF EXISTS `{SINK}`;\
            DROP USER IF EXISTS `{USER}`;"
        ))
        .output()?
        .assert()
        .success();
    Command::new("taosBenchmark")
        .args([
            "-y",
            "-h",
            host.as_str(),
            "-d",
            SOURCE,
            "-n",
            "100",
            "-t",
            "100",
        ])
        .output()?
        .assert()
        .success();
    Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "CREATE USER `{USER}` PASS '{PASS}';\
                GRANT ALL ON `{SOURCE}` TO `{USER}`;\
                CREATE DATABASE `{SINK}`;"
        ))
        .assert()
        .success();

    // sync data, mode=history
    let data_dir = tempfile::tempdir()?;
    Command::cargo_bin("taosx")?
        .arg("run")
        .arg("-f")
        .arg(format!(
            "taos://{USER}:{PASS}@{host}:6030/{SOURCE}?mode=history"
        ))
        .arg("-t")
        .arg(format!("taos://{host}/{}", SINK))
        .env("TAOSX_DATA_DIR", data_dir.path())
        .timeout(Duration::from_secs(30))
        .assert()
        .success();

    // clean
    Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "DROP TOPIC IF EXISTS `{SOURCE}`;
                DROP DATABASE IF EXISTS `{SOURCE}`;
                DROP DATABASE IF EXISTS `{SINK}`;
                DROP USER IF EXISTS `td33256`;"
        ))
        .output()?
        .assert()
        .success();

    Ok(())
}

/// # description
/// This case test synchronization task with several stables
/// # description_cn
/// 同步 1～N 个超级表
/// 1. 创建数据库 DB_SRC 和 DB_DST
/// 2. 在 DB_SRC 中创建 1～N 个超级表，每个超级表中写入 M 行数据；
/// 3. 创建数据同步任务
/// 4. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-34842
/// # example
/// ```shell
/// cargo nextest run test_sync_several_stables_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_several_stables_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or(String::from("127.0.0.1"));
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|ws_enable| ws_enable.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "test_sync_several_stables_src";
    const DB_DST: &str = "test_sync_several_stables_dst";
    const N: i32 = 10;
    const M: i32 = 10;

    // create databases and stables, write some data
    let mut sqls = format!(
        "drop database if exists `{DB_SRC}`;\
        drop database if exists `{DB_DST}`;\
        create database if not exists `{DB_SRC}`;\
        create database if not exists `{DB_DST}`;\
        "
    );
    let mut stables = vec![];
    for i in 1..=N {
        sqls.push_str(&format!(
            "create table `{DB_SRC}`.`Stb-{i}`(ts timestamp, val float) tags(id int);"
        ));
        stables.push(format!("Stb-{i}"));

        for j in 1..=M {
            sqls.push_str(&format!(
                "insert into `{DB_SRC}`.`Tb{i}{j}` using `{DB_SRC}`.`Stb-{i}` tags({i}{j}) values (now, {j}.{j});"
            ));
        }
    }
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(sqls.clone())
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);

    // sync
    let (from, to) = if ws_enable {
        let from = format!(
            "taos+ws://{host}:6041/{DB_SRC}?stables={}",
            stables.join(",")
        )
        .into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!("taos://{host}/{DB_SRC}?stables={}", stables.join(",")).into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from.to_string())
        .arg("-t")
        .arg(to.to_string())
        .assert()
        .success();

    // then
    for i in 1..=N {
        let output = Command::new("taos")
            .args(["-h", host.as_str(), "-s"])
            .arg(format!("select count(*) from `{DB_SRC}`.`Stb-{i}`"))
            .output()?;
        let count_src = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|l| l.to_string())
            .collect_vec();
        let output = Command::new("taos")
            .args(["-h", host.as_str(), "-s"])
            .arg(format!("select count(*) from `{DB_DST}`.`Stb-{i}`"))
            .output()?;
        let count_dst = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|l| l.to_string())
            .collect_vec();
        assert_eq!(count_src.get(6), count_dst.get(6));
    }

    // clean
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "drop database if exists `{DB_SRC}`;\
            drop database if exists `{DB_DST}`;\
            "
        ))
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);

    Ok(())
}

/// # description
/// This case test synchronization task with specified tables and normal tables
/// # description_cn
/// 同步 N 个子表和普通表
/// 1. 创建数据库 DB_SRC 和 DB_DST
/// 2. 在 DB_SRC 中创建 1 个超级表，向 N 个子表中写入数据；
/// 3. 创建数据同步任务，指定表和普通表；
/// 4. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-34842
/// # example
/// ```shell
/// cargo nextest run test_sync_specified_tables_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_specified_tables_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or(String::from("127.0.0.1"));
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|ws_enable| ws_enable.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "test_sync_specified_tables_src";
    const DB_DST: &str = "test_sync_specified_tables_dst";
    const N: i32 = 10;

    // create databases and stables, write some data
    let mut sqls = format!(
        "drop database if exists `{DB_SRC}`;\
        drop database if exists `{DB_DST}`;\
        create database if not exists `{DB_SRC}`;\
        create database if not exists `{DB_DST}`;\
        create table `{DB_SRC}`.`Stb`(ts timestamp, val float) tags(id int);\
        create table `{DB_SRC}`.`nTb`(ts timestamp, f1 float, f2 int);\
        "
    );
    for i in 1..=N {
        sqls.push_str(&format!(
            "insert into `{DB_SRC}`.`Tb{i}` using `{DB_SRC}`.`Stb` tags({i}) values (now, {i}.{i});\
            insert into `{DB_SRC}`.`nTb` values (now, {i}.{i}, {i});\
            "
        ));
    }
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(sqls.clone())
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);

    // sync
    let (from, to) = if ws_enable {
        let from = format!("taos+ws://{host}:6041/{DB_SRC}?tables=Tb1,Tb2,Tb3,nTb").into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!("taos://{host}/{DB_SRC}?tables=Tb1,Tb2,Tb3,nTb").into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from.to_string())
        .arg("-t")
        .arg(to.to_string())
        .assert()
        .success();

    // then
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("select count(*) from `{DB_SRC}`.`Stb`"))
        .output()?;
    let count_src = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_string())
        .collect_vec();
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("select count(*) from `{DB_DST}`.`Stb`"))
        .output()?;
    let count_dst = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_string())
        .collect_vec();
    // DB_SRC = N, DB_DST = 3
    assert!(count_src.get(6).unwrap().contains(N.to_string().as_str()));
    assert!(count_dst.get(6).unwrap().contains(3.to_string().as_str()));
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("select count(*) from `{DB_SRC}`.`nTb`"))
        .output()?;
    let count_src = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_string())
        .collect_vec();
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!("select count(*) from `{DB_DST}`.`nTb`"))
        .output()?;
    let count_dst = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_string())
        .collect_vec();
    assert_eq!(count_src.get(6), count_dst.get(6));

    // clean
    let output = Command::new("taos")
        .args(["-h", host.as_str(), "-s"])
        .arg(format!(
            "drop database if exists `{DB_SRC}`;\
            drop database if exists `{DB_DST}`;\
            "
        ))
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);

    Ok(())
}

/// # description
/// This case test synchronize database with specified time range
/// # description_cn
/// 同步数据库，指定时间区间：[strat, ∞), (∞, end), [start, end)
/// 1. 创建数据库 DB_SRC 和 DB_DST
/// 2. 在 DB_SRC 中创建 1 个超级表，写入 30 天的数据，每天 N 条；
/// 3. 创建数据同步任务，分别指定时间区间为：[strat, ∞), (∞, end), [start, end)
/// 4. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-34842
/// # example
/// ```shell
/// cargo nextest run test_sync_time_range_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_time_range_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|ws_enable| ws_enable.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "test_sync_time_range_src";
    const DB_DST: &str = "test_sync_time_range_dst";
    const DAYS: i64 = 30;
    const N: i64 = 10;

    // create databases and stables
    let taos = if ws_enable {
        TaosBuilder::from_dsn(format!("taos+ws://{host}:6041").into_dsn()?)?
            .build()
            .await?
    } else {
        TaosBuilder::from_dsn(format!("taos://{host}").into_dsn()?)?
            .build()
            .await?
    };
    taos.exec_many(vec![
        format!("drop database if exists `{DB_SRC}`;"),
        format!("drop database if exists `{DB_DST}`;"),
        format!("create database if not exists `{DB_SRC}`;"),
        format!("create database if not exists `{DB_DST}`;"),
        format!("create table `{DB_SRC}`.`Stb`(ts timestamp, val float) tags(id int);"),
    ])
    .await?;

    let (src, dst) = if ws_enable {
        let src = TaosBuilder::from_dsn(format!("taos+ws://{host}:6041/{DB_SRC}").into_dsn()?)?
            .build()
            .await?;
        let dst = TaosBuilder::from_dsn(format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?)?
            .build()
            .await?;
        (src, dst)
    } else {
        let src = TaosBuilder::from_dsn(format!("taos://{host}/{DB_SRC}").into_dsn()?)?
            .build()
            .await?;
        let dst = TaosBuilder::from_dsn(format!("taos://{host}/{DB_DST}").into_dsn()?)?
            .build()
            .await?;
        (src, dst)
    };

    // write some data
    let now = Utc::now();
    for i in 0..DAYS {
        for j in 0..N {
            let sql = format!(
                "insert into `{DB_SRC}`.`Tb{j}` using `{DB_SRC}`.`Stb` tags({j}) values ({}, {i}.{j});",
                (now - chrono::Duration::days(i)).timestamp_millis()
            );
            src.exec(sql).await?;
        }
    }

    // sync: start = now - DAYS/2
    let (from, to) = if ws_enable {
        let from = format!(
            "taos+ws://{host}:6041/{DB_SRC}?start={}",
            (now - chrono::Duration::days(DAYS / 2)).to_rfc3339()
        )
        .into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!(
            "taos://{host}/{DB_SRC}?start={}",
            (now - chrono::Duration::days(DAYS / 2)).to_rfc3339()
        )
        .into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from.to_string())
        .arg("-t")
        .arg(to.to_string())
        .assert()
        .success();
    // then
    let first: i64 = dst.query_one("select first(ts) from `Stb`").await?.unwrap();
    assert_eq!(
        first,
        (now - chrono::Duration::days(DAYS / 2)).timestamp_millis()
    );
    let last: i64 = dst.query_one("select last(ts) from `Stb`").await?.unwrap();
    assert_eq!(last, now.timestamp_millis());

    dst.exec("delete from `Stb`").await?;
    // sync: end = now - DAYS/2
    let (from, to) = if ws_enable {
        let from = format!(
            "taos+ws://{host}:6041/{DB_SRC}?end={}",
            (now - chrono::Duration::days(DAYS / 2)).to_rfc3339()
        )
        .into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!(
            "taos://{host}/{DB_SRC}?end={}",
            (now - chrono::Duration::days(DAYS / 2)).to_rfc3339()
        )
        .into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from.to_string())
        .arg("-t")
        .arg(to.to_string())
        .assert()
        .success();
    // then
    let first: i64 = dst.query_one("select first(ts) from `Stb`").await?.unwrap();
    assert_eq!(
        first,
        (now - chrono::Duration::days(DAYS - 1)).timestamp_millis()
    );
    let last: i64 = dst.query_one("select last(ts) from `Stb`").await?.unwrap();
    assert_eq!(
        last,
        (now - chrono::Duration::days(DAYS / 2 + 1)).timestamp_millis()
    );

    dst.exec("delete from `Stb`").await?;
    // sync: start = now - (DAYS - 2), end = now - 1
    let (from, to) = if ws_enable {
        let from = format!(
            "taos+ws://{host}:6041/{DB_SRC}?start={}&end={}",
            (now - chrono::Duration::days(DAYS - 2)).to_rfc3339(),
            (now - chrono::Duration::days(1)).to_rfc3339()
        )
        .into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!(
            "taos://{host}/{DB_SRC}?start={}&end={}",
            (now - chrono::Duration::days(DAYS - 2)).to_rfc3339(),
            (now - chrono::Duration::days(1)).to_rfc3339()
        )
        .into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from.to_string())
        .arg("-t")
        .arg(to.to_string())
        .assert()
        .success();
    // then
    let first: i64 = dst.query_one("select first(ts) from `Stb`").await?.unwrap();
    assert_eq!(
        first,
        (now - chrono::Duration::days(DAYS - 2)).timestamp_millis()
    );
    let last: i64 = dst.query_one("select last(ts) from `Stb`").await?.unwrap();
    assert_eq!(last, (now - chrono::Duration::days(2)).timestamp_millis());

    // clean
    taos.exec_many(vec![
        format!("drop database if exists `{DB_SRC}`;"),
        format!("drop database if exists `{DB_DST}`;"),
    ])
    .await?;

    Ok(())
}

/// # description
/// This case test the realtime mode data synchronization
/// # description_cn
/// 同步，mode=realtime，restro=5m, interval=1s,excursion=500ms
/// 1. 创建数据库 DB_SRC 和 DB_DST
/// 2. 在 DB_SRC 中创建 1 个超级表，向 N 个子表中写入数据；
/// 3. 创建数据同步任务，mode=realtime，restro=5m, interval=1s,excursion=500ms
/// 4. 向 DB_SRC 中写入数据；运行 60 秒后，停止；
/// 5. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-34842
/// # example
/// ```shell
/// cargo nextest run test_sync_realtime_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_realtime_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|ws_enable| ws_enable.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "test_sync_realtime_src";
    const DB_DST: &str = "test_sync_realtime_dst";
    const N: i32 = 10;
    const TID: i64 = 34842001;

    // create databases and stables
    let taos = if ws_enable {
        TaosBuilder::from_dsn(format!("taos+ws://{host}:6041").into_dsn()?)?
            .build()
            .await?
    } else {
        TaosBuilder::from_dsn(format!("taos://{host}").into_dsn()?)?
            .build()
            .await?
    };
    taos.exec_many(vec![
        format!("drop database if exists `{DB_SRC}`;"),
        format!("drop database if exists `{DB_DST}`;"),
        format!("create database if not exists `{DB_SRC}`;"),
        format!("create database if not exists `{DB_DST}`;"),
        format!("create table `{DB_SRC}`.`Stb`(ts timestamp, val float) tags(id int);"),
    ])
    .await?;

    // write some historical data
    let now = Utc::now();
    for i in 0..N {
        let sql = format!(
            "insert into `{DB_SRC}`.`Tb{i}` using `{DB_SRC}`.`Stb` tags({i}) values ({}, {i}.{i});",
            (now - chrono::Duration::days(10)).timestamp_millis()
        );
        taos.exec(sql).await?;
    }

    // start sync task, mode=realtime, restro=5m, interval=1s, excursion=500ms
    let (from, to) = if ws_enable {
        let from = format!(
            "taos+ws://{host}:6041/{DB_SRC}?mode=realtime&restro=5m&interval=1s&excursion=500ms"
        )
        .into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from =
            format!("taos://{host}/{DB_SRC}?mode=realtime&restro=5m&interval=1s&excursion=500ms")
                .into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let handle =
        tokio::spawn(
            async move { legacy_to_taos(from, vec![], to, cancel_clone, Some(TID)).await },
        );

    // write some realtime data
    for i in 0..N {
        let sql = format!(
            "insert into `{DB_SRC}`.`Tb{i}` using `{DB_SRC}`.`Stb` tags({i}) values ({}, {i}.{i});",
            (now + chrono::Duration::seconds(i as i64)).timestamp_millis()
        );
        taos.exec(sql).await?;
    }
    // wait for 60 seconds
    tokio::time::sleep(Duration::from_secs(20)).await;

    // stop
    cancel.cancel();
    handle.await??;

    // check result
    let count_src: i32 = taos
        .query_one(format!("select count(*) from `{DB_SRC}`.`Stb`"))
        .await?
        .unwrap();
    assert_eq!(count_src, N * 2);
    let count_dst: i32 = taos
        .query_one(format!("select count(*) from `{DB_DST}`.`Stb`"))
        .await?
        .unwrap();
    assert_eq!(count_dst, N);

    // clean
    taos.exec_many(vec![
        format!("drop database if exists `{DB_SRC}`;"),
        format!("drop database if exists `{DB_DST}`;"),
    ])
    .await?;

    Ok(())
}

/// # description
/// This case test the all mode data synchronization
/// # description_cn
/// 同步，mode=all
/// 1. 创建数据库 DB_SRC 和 DB_DST
/// 2. 在 DB_SRC 中创建 1 个超级表，向 N 个子表中写入数据；
/// 3. 创建数据同步任务，mode=all
/// 4. 向 DB_SRC 中写入数据；运行 60 秒后，停止；
/// 5. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-34842
/// # example
/// ```shell
/// cargo nextest run test_sync_all_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_all_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|ws_enable| ws_enable.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "test_sync_all_src";
    const DB_DST: &str = "test_sync_all_dst";
    const N: i32 = 10;
    const TID: i64 = 34842002;

    clear_metrics(TID).await;

    // create databases and stables
    let taos = if ws_enable {
        TaosBuilder::from_dsn(format!("taos+ws://{host}:6041").into_dsn()?)?
            .build()
            .await?
    } else {
        TaosBuilder::from_dsn(format!("taos://{host}").into_dsn()?)?
            .build()
            .await?
    };
    taos.exec_many(vec![
        format!("drop database if exists `{DB_SRC}`;"),
        format!("drop database if exists `{DB_DST}`;"),
        format!("create database if not exists `{DB_SRC}`;"),
        format!("create database if not exists `{DB_DST}`;"),
        format!("create table `{DB_SRC}`.`Stb`(ts timestamp, val float) tags(id int);"),
    ])
    .await?;

    // write some historical data
    let now = Utc::now();
    for i in 0..N {
        let sql = format!(
            "insert into `{DB_SRC}`.`Tb{i}` using `{DB_SRC}`.`Stb` tags({i}) values ({}, {i}.{i});",
            (now - chrono::Duration::minutes(4)).timestamp_millis()
        );
        taos.exec(sql).await?;
    }

    // start sync task, mode=realtime, restro=5m, interval=1s, excursion=500ms
    let (from, to) = if ws_enable {
        let from = format!(
            "taos+ws://{host}:6041/{DB_SRC}?mode=all&restro=5m&interval=1s&excursion=500ms"
        )
        .into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!("taos://{host}/{DB_SRC}?mode=all&restro=5m&interval=1s&excursion=500ms")
            .into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let handle =
        tokio::spawn(
            async move { legacy_to_taos(from, vec![], to, cancel_clone, Some(TID)).await },
        );

    // write some realtime data
    for i in 0..N {
        let sql = format!(
            "insert into `{DB_SRC}`.`Tb{i}` using `{DB_SRC}`.`Stb` tags({i}) values ({}, {i}.{i});",
            (now + chrono::Duration::seconds(i as i64)).timestamp_millis()
        );
        taos.exec(sql).await?;
    }
    // wait for 60 seconds
    tokio::time::sleep(Duration::from_secs(20)).await;

    // stop
    cancel.cancel();
    handle.await??;

    // check result
    let count_src: i32 = taos
        .query_one(format!("select count(*) from `{DB_SRC}`.`Stb`"))
        .await?
        .unwrap();
    let count_dst: i32 = taos
        .query_one(format!("select count(*) from `{DB_DST}`.`Stb`"))
        .await?
        .unwrap();
    assert_eq!(count_src, count_dst);

    // clean
    taos.exec_many(vec![
        format!("drop database if exists `{DB_SRC}`;"),
        format!("drop database if exists `{DB_DST}`;"),
    ])
    .await?;

    Ok(())
}

/// # description
/// This case test sync database which specified subtables, and use select-from-stable
/// # description_cn
/// 同步指定子表的数据，且从超级表取数据
/// 1. 创建数据库 DB_SRC 和 DB_DST
/// 2. 在 DB_SRC 中创建 1 个超级表，向 N 个子表中写入数据；
/// 3. 创建数据同步任务，指定 3 个子表，从超级表中取数据；
/// 4. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致为用例通过，否则用例失败。
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-34842
/// # example
/// ```shell
/// cargo nextest run test_sync_select_from_stable_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_sync_select_from_stable_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|ws_enable| ws_enable.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "test_sync_select_from_stable_src";
    const DB_DST: &str = "test_sync_select_from_stable_dst";
    const N: i32 = 10;

    let taos = if ws_enable {
        TaosBuilder::from_dsn(format!("taos+ws://{host}:6041").into_dsn()?)?
            .build()
            .await?
    } else {
        TaosBuilder::from_dsn(format!("taos://{host}").into_dsn()?)?
            .build()
            .await?
    };

    // create databases and stables
    taos.exec_many(vec![
        format!("drop database if exists `{DB_SRC}`;"),
        format!("drop database if exists `{DB_DST}`;"),
        format!("create database if not exists `{DB_SRC}`;"),
        format!("create database if not exists `{DB_DST}`;"),
        format!("create table `{DB_SRC}`.`Stb`(ts timestamp, val float) tags(id int);"),
    ])
    .await?;

    // write some data
    for i in 0..N {
        let sql = format!(
            "insert into `{DB_SRC}`.`Tb{i}` using `{DB_SRC}`.`Stb` tags({i}) values (now, {i}.{i});"
        );
        taos.exec(sql).await?;
    }

    // sync
    let (from, to) = if ws_enable {
        let from = format!(
            "taos+ws://{host}:6041/{DB_SRC}?tables=Stb.Tb1,Stb.Tb2,Stb.Tb3&select_from_stable=true"
        )
        .into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!(
            "taos://{host}/{DB_SRC}?tables=Stb.Tb1,Stb.Tb2,Stb.Tb3&select_from_stable=true"
        )
        .into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    Command::cargo_bin("taosx")?
        .args(["run", "-f"])
        .arg(from.to_string())
        .arg("-t")
        .arg(to.to_string())
        .assert()
        .success();

    // check result
    let count_src: i32 = taos
        .query_one(format!("select count(*) from `{DB_SRC}`.`Stb`"))
        .await?
        .unwrap();
    let count_dst: i32 = taos
        .query_one(format!("select count(*) from `{DB_DST}`.`Stb`"))
        .await?
        .unwrap();
    assert_eq!(count_src, N);
    assert_eq!(count_dst, 3);

    // clean
    taos.exec_many(vec![
        format!("drop database if exists `{DB_SRC}`;"),
        format!("drop database if exists `{DB_DST}`;"),
    ])
    .await?;

    Ok(())
}
