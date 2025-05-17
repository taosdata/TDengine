use assert_cmd::{prelude::*, Command};
use taos::IntoDsn;
use taosx_core::tmq_to_td;
use tokio_util::sync::CancellationToken;

/// # case
/// test tmq_to_td task sync stream tables
/// 1. create databases, stable and stream
/// 2. create a replication task(tmq_to_td) from DB_SRC to DB_DST
/// 3. write data in DB_SRC
/// 4. check the result
/// # jira
/// Close http://jira.tdengine.org/browse/TD-34829
/// # example
/// ```shell
/// cargo nextest run test_td34829_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_td34829_with_taos() -> anyhow::Result<()> {
    const HOST: &str = "127.0.0.1";
    const DB_SRC: &str = "td34829_src";
    const DB_DST: &str = "td34829_dst";
    const TID: &str = "34829";
    const STREAM: &str = "current_state_window";
    let ws_enable = false;
    let group_id = format!("test_td{TID}");
    let topic_name = format!("test_replica_td{TID}");

    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // 1. create two database： td34829_src & td34829_dst
    println!("====== create databases and stream =====");
    let output = Command::new("taos")
        .args(["-h", HOST, "-s"])
        .arg(format!("\
        drop topic if exists `{topic_name}`;\
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
    assert!(err.is_empty(), "{}", err);

    // 2. create a replication task(tmq_to_td) from td34829_src to td34829_dst
    println!("====== start replication task =====");
    let (from, to) = if ws_enable {
        let from = format!(
            "tmq+ws://{HOST}:6041/{DB_SRC}?group.id={group_id}&timeout=never&use.topic.name={topic_name}"
        )
            .into_dsn()?;
        let to = format!("taos+ws://{HOST}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!(
            "tmq://{HOST}/{DB_SRC}?group.id={group_id}&timeout=never&use.topic.name={topic_name}"
        )
        .into_dsn()?;
        let to = format!("taos://{HOST}/{DB_DST}").into_dsn()?;
        (from, to)
    };

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let (tx, rx) = flume::unbounded();
    let tx_clone = tx.clone();
    let h = tokio::spawn(async move {
        tmq_to_td(
            from,
            vec![],
            to,
            cancel_clone,
            Some(TID.to_string()),
            tx_clone,
        )
        .await
    });

    // 3. write data in DB_SRC
    println!("======= write data =====");
    for _ in 0..20 {
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
    }
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;

    drop(tx);
    cancel.cancel();
    h.await??;
    while let Ok(msg) = rx.recv() {
        println!("{msg:?}");
    }

    // 4. check the result
    println!("====== check the result =====");
    let output = Command::new("taos")
        .args(["-h", HOST, "-s"])
        .arg(format!("select count(*) from `{DB_SRC}`.meters;"))
        .output()?;
    let src_out: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_owned())
        .collect();
    let src_err = String::from_utf8_lossy(&output.stderr);
    assert!(src_err.is_empty(), "{}", src_err);

    let output = Command::new("taos")
        .args(["-h", HOST, "-s"])
        .arg(format!("select count(*) from `{DB_DST}`.meters;"))
        .output()?;
    let dst_out: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_owned())
        .collect();
    let dst_err = String::from_utf8_lossy(&output.stderr);
    assert!(dst_err.is_empty(), "{}", dst_err);

    assert_eq!(src_out.get(6), dst_out.get(6));

    let output = Command::new("taos")
        .args(["-h", HOST, "-s"])
        .arg(format!("select count(*) from `{DB_SRC}`.`{STREAM}`;"))
        .output()?;
    let src_out: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_owned())
        .collect();
    let src_err = String::from_utf8_lossy(&output.stderr);
    assert!(src_err.is_empty(), "{}", src_err);

    let output = Command::new("taos")
        .args(["-h", HOST, "-s"])
        .arg(format!("select count(*) from `{DB_DST}`.`{STREAM}`;"))
        .output()?;
    let dst_out: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.to_owned())
        .collect();
    let dst_err = String::from_utf8_lossy(&output.stderr);
    assert!(dst_err.is_empty(), "{}", dst_err);

    assert_eq!(src_out.get(6), dst_out.get(6));

    // 5. clean
    println!("====== clean up =====");
    let output = Command::new("taos")
        .args(["-h", HOST, "-s"])
        .arg(format!(
            "\
        drop topic if exists `{topic_name}`;\
        drop stream if exists `{STREAM}`;\
        drop database if exists `{DB_SRC}`;\
        drop database if exists `{DB_DST}`;\
        "
        ))
        .output()?;
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.is_empty(), "{}", err);

    Ok(())
}

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
