use std::time::Duration;

use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "trace,tungstenite=error,tokio_tungstenite=error",
    );
    pretty_env_logger::init();
    let dsn = "taos+ws://localhost:6041";
    let builder = TaosBuilder::from_dsn(dsn)?;

    let taos = builder.build().await?;
    let db = "json_meta_test";

    // prepare database
    taos.exec_many([
        "DROP TOPIC IF EXISTS tmq_meters".to_string(),
        format!("DROP DATABASE IF EXISTS `{db}`"),
        format!("CREATE DATABASE `{db}`"),
        format!("USE `{db}`"),
        // create super table
        "CREATE TABLE `meters` (`ts` TIMESTAMP, v1 INT) TAGS (`t1` NCHAR(16))".to_string(),
        // create topic for subscription
        format!("CREATE TOPIC tmq_meters with META AS DATABASE {db}"),
    ])
    .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    async fn prepare(taos: Taos) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let inserted = taos
            .exec(
                // insert and automatically create table with tags if not exists
                "INSERT INTO `d1` USING `meters` TAGS(\"ab\") values(now - 9s,1)",
            )
            .await?;

        assert_eq!(inserted, 1);

        tokio::time::sleep(Duration::from_secs(1)).await;
        let inserted = taos
            .exec(
                // insert and automatically create table with tags if not exists
                "INSERT INTO `d2` USING `meters` TAGS(NULL) values(now - 9s,1)",
            )
            .await?;

        assert_eq!(inserted, 1);
        Ok(())
    }

    let task = tokio::spawn(prepare(taos));

    // subscribe
    let tmq = TmqBuilder::from_dsn(
        "taos+ws://localhost:6041/?group.id=test&timeout=5s&experimental.snapshot.enable=false&msg.enable.batchmeta=1",
    )?;

    let mut consumer = tmq.build().await?;
    consumer.subscribe(["tmq_meters"]).await?;

    let u = consumer.default_timeout();
    let mut has_consumed_meta = false;
    while let Some((offset, message)) = consumer.recv_timeout(u).await? {
        // get information from offset

        // the topic
        let topic = offset.topic();
        // the vgroup id, like partition id in kafka.
        let vgroup_id = offset.vgroup_id();
        println!("* in vgroup id {vgroup_id} of topic {topic}\n");
        match message {
            MessageSet::Meta(meta) => {
                let json = meta.as_json_meta().await?;
                dbg!(&json);
                for meta in json.iter() {
                    println!("meta: {}", meta);
                }
            }
            MessageSet::Data(data) => {
                println!("data message:");
                while let Some(block) = data.fetch_raw_block().await? {
                    // one block for one table, get table name if needed
                    let name = block.table_name();
                    let nrows = block.nrows();
                    let ncols = block.ncols();
                    if let Some(name) = name {
                        eprintln!(
                            "data block with table name {}, cols: {}, rows: {}",
                            name, ncols, nrows
                        );
                        eprintln!("{}", block.pretty_format());
                    } else {
                        eprintln!(
                            "data block without table name, cols: {}, rows: {}",
                            ncols, nrows
                        );
                        eprintln!("{}", block.pretty_format());
                    }
                }
            }
            MessageSet::MetaData(meta, data) => {
                let json = meta.as_json_meta().await?;
                dbg!(&json);
                for meta in json.iter() {
                    println!("meta: {}", meta);
                }
                has_consumed_meta = true;
                println!("metadata message:");
                while let Some(block) = data.fetch_raw_block().await? {
                    // one block for one table, get table name if needed
                    let name = block.table_name();
                    let nrows = block.nrows();
                    let ncols = block.ncols();
                    if let Some(name) = name {
                        eprintln!(
                            "data block with table name {}, cols: {}, rows: {}",
                            name, ncols, nrows
                        );
                        eprintln!("{}", block.pretty_format());
                    } else {
                        eprintln!(
                            "data block without table name, cols: {}, rows: {}",
                            ncols, nrows
                        );
                        eprintln!("{}", block.pretty_format());
                    }
                }
            }
        }
        consumer.commit(offset).await?;
    }

    consumer.unsubscribe().await;
    assert!(has_consumed_meta, "should have consumed meta message");
    task.await??;

    Ok(())
}
