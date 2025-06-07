use assert_cmd::{prelude::*, Command};
use taos::{AsyncQueryable, AsyncTBuilder, IntoDsn, TaosBuilder};
use taosx_core::tmq_to_td;
use tokio_util::sync::CancellationToken;

/// # description
/// test tmq_to_td task sync stream tables
/// 1. create databases, stable and stream
/// 2. create a replication task(tmq_to_td) from DB_SRC to DB_DST
/// 3. write data in DB_SRC, and stream will generate new tables and data
/// 4. run for 20 seconds, then stop the replication task
/// 5. check the result
/// # description_cn
/// tmq 同步数据库中写入的数据以及 stream 产生的数据
/// 1. 创建数据库 DB_SRC 和 DB_DST，在 DB_SRC 中创建超级表和 stream；
/// 2. 创建数据复制任务，timeout=never
/// 3. 向 DB_SRC 中写入数据，同时 stream 会产生新表和新数据；
/// 4. 运行 20 秒后，停止数据复制任务；
/// 5. 检查 DB_SRC 和 DB_DST 中的数据，表和 stream 的数据都完成了同步，则用例通过，否则失败。
/// # jira
/// Close https://jira.taosdata.com:18080/browse/TD-34829
/// # example
/// ```shell
/// cargo nextest run test_td34829_with_taos --nocapture --retries 0
/// ```
#[tokio::test]
async fn test_td34829_with_taos() -> anyhow::Result<()> {
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
    let ws_enable = std::env::var("WS_ENABLE")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    const DB_SRC: &str = "td34829_src";
    const DB_DST: &str = "td34829_dst";
    const TID: i32 = 34829;
    const STREAM: &str = "current_state_window";
    let group_id = format!("test_td{TID}");
    let topic_name = format!("test_replica_td{TID}");

    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // 1. create two database： td34829_src & td34829_dst
    println!("====== create databases and stream =====");
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
        format!("drop topic if exists force `{topic_name}`"),
        format!("drop stream if exists `{STREAM}`"),
        format!("drop database if exists `{DB_SRC}`"),
        format!("drop database if exists `{DB_DST}`"),
        format!("create database if not exists `{DB_SRC}`"),
        format!("create database if not exists `{DB_DST}`"),
        format!("create table `{DB_SRC}`.`meters`(ts timestamp, val float) tags(id int)"),
        format!("create stream `{STREAM}` into `{DB_SRC}`.`{STREAM}` as select tbname,_wstart,avg(val) from `{DB_SRC}`.meters partition by tbname state_window(cast(val as int))"),
    ]).await?;

    // 2. create a replication task(tmq_to_td) from td34829_src to td34829_dst
    println!("====== start replication task =====");
    let (from, to) = if ws_enable {
        let from = format!(
            "tmq+ws://{host}:6041/{DB_SRC}?group.id={group_id}&timeout=never&use.topic.name={topic_name}"
        ).into_dsn()?;
        let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
        (from, to)
    } else {
        let from = format!(
            "tmq://{host}/{DB_SRC}?group.id={group_id}&timeout=never&use.topic.name={topic_name}"
        )
        .into_dsn()?;
        let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
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
        taos.exec_many(vec![
            format!("insert into {DB_SRC}.t1 using {DB_SRC}.meters tags(1) values(now, 11.1),(now+1s,11.2),(now+2s,11.3),(now+3s,10.1),(now+4s,10.2),(now+5s,10.3)"),
            format!("insert into {DB_SRC}.t2 using {DB_SRC}.meters tags(2) values(now, 22.1),(now+1s,22.2),(now+2s,22.3),(now+3s,21.1),(now+4s,21.2),(now+5s,21.3)"),
            format!("insert into {DB_SRC}.t3 using {DB_SRC}.meters tags(3) values(now, 33.1),(now+1s,33.2),(now+2s,33.3),(now+3s,32.1),(now+4s,32.2),(now+5s,32.3)"),
            format!("insert into {DB_SRC}.t4 using {DB_SRC}.meters tags(4) values(now, 44.1),(now+1s,44.2),(now+2s,44.3),(now+3s,43.1),(now+4s,43.2),(now+5s,43.3)"),
            format!("insert into {DB_SRC}.t5 using {DB_SRC}.meters tags(5) values(now, 55.1),(now+1s,55.2),(now+2s,55.3),(now+3s,54.1),(now+4s,54.2),(now+5s,55.3)"),
        ]).await?;
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
    let count_src: u64 = taos
        .query_one(format!("select count(*) from `{DB_SRC}`.meters"))
        .await?
        .unwrap();
    let count_dst: u64 = taos
        .query_one(format!("select count(*) from `{DB_DST}`.meters"))
        .await?
        .unwrap();
    assert_eq!(count_src, count_dst);

    let stream_src: u64 = taos
        .query_one(format!("select count(*) from `{DB_SRC}`.`{STREAM}`"))
        .await?
        .unwrap();
    let stream_dst: u64 = taos
        .query_one(format!("select count(*) from `{DB_DST}`.`{STREAM}`"))
        .await?
        .unwrap();
    assert_eq!(stream_src, stream_dst);

    // 5. clean
    println!("====== clean up =====");
    taos.exec_many(vec![
        format!("drop topic if exists force `{topic_name}`"),
        format!("drop stream if exists `{STREAM}`"),
        format!("drop database if exists `{DB_SRC}`"),
        format!("drop database if exists `{DB_DST}`"),
    ])
    .await?;

    Ok(())
}

/// # description
///
/// # description_cn
///
/// # jira
/// close https://jira.taosdata.com:18080/browse/TD-33080
/// # example
/// ```shell
/// cargo nextest run test_td33080_with_taos --nocapture --retries 0
/// ```
#[test]
fn test_td33080_with_taos() -> anyhow::Result<()> {
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
