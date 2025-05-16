use assert_cmd::{prelude::*, Command};
use itertools::Itertools;
use std::time::Duration;
use taos::IntoDsn;
use taosx_core::{get_data_dir, legacy_to_taos};
use tokio_util::sync::CancellationToken;

/// # case
/// test schema only sync task from DB_SRC, which has streams, to DB_DST
/// 1. create databases, stable and stream
/// 2. write some data
/// 3. create a schema only synchronization task from DB_SRC to DB_DST
/// 4. write some data in new tables
/// 5. create a schema only synchronization task from DB_SRC to DB_DST
/// 6. check the schema of DB_DST
/// # jira
/// https://jira.taosdata.com:18080/browse/TS-6499
/// # example
/// ```shell
/// cargo nextest run test_ts6499_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_ts6499_with_taos() -> anyhow::Result<()> {
    const HOST: &str = "127.0.0.1";
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
        .args(["-h", HOST, "-s"])
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
            .args(["-h", HOST, "-s"])
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
        let from = format!("taos+ws://{HOST}:6041/{DB_SRC}?schema=only").into_dsn()?;
        let to = format!("taos+ws://{HOST}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!("taos://{HOST}/{DB_SRC}??schema=only").into_dsn()?;
        let to = format!("taos://{HOST}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    let cancel = CancellationToken::new();
    let res = legacy_to_taos(from, vec![], to, cancel, Some(TID)).await;
    assert!(res.is_ok());

    // 4. write some data in DB_SRC
    println!("======= write data again =====");
    let output = Command::new("taos")
        .args(["-h", HOST, "-s"])
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
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 5. create a schema only synchronization task
    println!("====== start schema only synchronization task again =====");
    let (from, to) = if ws_enable {
        let from = format!("taos+ws://{HOST}:6041/{DB_SRC}?schema=only").into_dsn()?;
        let to = format!("taos+ws://{HOST}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!("taos://{HOST}/{DB_SRC}??schema=only").into_dsn()?;
        let to = format!("taos://{HOST}/{DB_DST}").into_dsn()?;
        (from, to)
    };
    let cancel = CancellationToken::new();
    let res = legacy_to_taos(from, vec![], to, cancel, Some(TID)).await;
    assert!(res.is_ok());

    // 6. check the schema
    let output = Command::new("taos")
        .args(["-h", HOST, "-s"])
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
        .args(["-h", HOST, "-s"])
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
        .args(["-h", HOST, "-s"])
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

/// This case test the breakpoint recovery of realtime data synchronization
/// Close https://jira.taosdata.com:18080/browse/TS-6402
#[tokio::test]
async fn test_ts6402_with_taos() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    const HOST: &str = "127.0.0.1";
    const DB_SRC: &str = "test_ts6402_src";
    const DB_DST: &str = "test_ts6402_dst";

    // create databases and tables
    println!("=========CREATE DATABASE AND WRITE=========");
    let output = Command::new("taos")
        .arg("-h")
        .arg(HOST)
        .arg("-s")
        .arg(format!(
            "drop database if exists `{DB_SRC}`;
            create database if not exists `{DB_SRC}`;
            drop database if exists `{DB_DST}`;
            create database if not exists `{DB_DST}`;
            create table `{DB_SRC}`.`meters`(ts timestamp, val float) tags(id int);
            "
        ))
        .output()
        .expect("failed to execute process");
    let out = String::from_utf8_lossy(&output.stdout);
    let err = String::from_utf8_lossy(&output.stderr);
    dbg!(&out, &err);

    let from = format!(
        "taos://{HOST}/{DB_SRC}?mode=realtime&schema=always&schema-polling-interval=5s&sparse=true"
    )
    .into_dsn()?;
    let to = format!("taos://{HOST}/{DB_DST}").into_dsn()?;
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
        .arg("-h")
        .arg(HOST)
        .arg("-s")
        .arg(format!(
            "insert into `{DB_SRC}`.`t1` using `{DB_SRC}`.`meters` tags(1) values (now, 11.0);
            insert into `{DB_SRC}`.`t2` using `{DB_SRC}`.`meters` tags(2) values (now, 22.0);
            insert into `{DB_SRC}`.`t3` using `{DB_SRC}`.`meters` tags(3) values (now, 33.0);
            insert into `{DB_SRC}`.`t4` using `{DB_SRC}`.`meters` tags(4) values (now, 44.0);
            insert into `{DB_SRC}`.`t5` using `{DB_SRC}`.`meters` tags(5) values (now, 55.0);
            "
        ))
        .output()
        .expect("failed to execute process");
    let out = String::from_utf8_lossy(&output.stdout);
    let err = String::from_utf8_lossy(&output.stderr);
    dbg!(&out, &err);

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
        .arg("-h")
        .arg(HOST)
        .arg("-s")
        .arg(format!("select count(*) from `{DB_SRC}`.`meters`"))
        .output()
        .expect("failed to execute process");
    let src_out: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_owned())
        .collect();
    let src_err: Vec<String> = String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(|l| l.to_owned())
        .collect();
    dbg!(&src_out, &src_err);

    let output = Command::new("taos")
        .arg("-h")
        .arg(HOST)
        .arg("-s")
        .arg(format!("select count(*) from `{DB_DST}`.`meters`"))
        .output()
        .expect("failed to execute process");
    let dst_out = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_string())
        .collect::<Vec<String>>();
    let dst_err = String::from_utf8_lossy(&output.stderr)
        .lines()
        .map(|l| l.to_string())
        .collect::<Vec<String>>();
    dbg!(&dst_out, &dst_err);

    let count_src = src_out.get(6);
    let count_dst = dst_out.get(6);
    assert_eq!(count_src, count_dst);

    // 5. drop databases
    Command::new("taos")
        .arg("-h")
        .arg(HOST)
        .arg("-s")
        .arg(format!(
            "drop database if exists `{DB_SRC}`;
            drop database if exists `{DB_DST}`;
            "
        ))
        .output()
        .expect("failed to execute process")
        .assert()
        .append_context("taos", "drop databases")
        .success();

    Ok(())
}

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
