use std::time::Duration;
use std::str::FromStr;

use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::formatted_timed_builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    use taos_query::prelude::*;
    let dsn = "taos://localhost:6030".to_string();
    log::info!("dsn: {}", dsn);
    let mut dsn = Dsn::from_str(&dsn)?;

    let taos = TaosBuilder::from_dsn(&dsn)?.build().await?;

    // prepare database and table
    taos.exec_many([
        "drop topic if exists topic_meters",
        "drop database if exists power",
        "create database if not exists power WAL_RETENTION_PERIOD 86400",
        "use power",

        "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))",

        "create table if not exists power.d001 using power.meters tags(1,'location')",

    ])
    .await?;

    taos.exec_many([
        "drop database if exists db2",
        "create database if not exists db2 wal_retention_period 3600",
        "use db2",
    ])
    .await?;

    // ANCHOR: create_topic
    taos.exec_many([
        "CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM power.meters",
    ])
    .await?;
    // ANCHOR_END: create_topic

    // ANCHOR: create_consumer
    dsn.params.insert("auto.offset.reset".to_string(), "latest".to_string());
    dsn.params.insert("msg.with.table.name".to_string(), "true".to_string());
    dsn.params.insert("enable.auto.commit".to_string(), "true".to_string());
    dsn.params.insert("auto.commit.interval.ms".to_string(), "1000".to_string());
    dsn.params.insert("group.id".to_string(), "group1".to_string());
    dsn.params.insert("client.id".to_string(), "client1".to_string());

    let builder = TmqBuilder::from_dsn(&dsn)?;
    let mut consumer = builder.build().await?;
    // ANCHOR_END: create_consumer

    // ANCHOR: subscribe
    consumer.subscribe(["topic_meters"]).await?;
    // ANCHOR_END: subscribe

    // ANCHOR: consume
    {
        let mut stream = consumer.stream_with_timeout(Timeout::from_secs(1));

        while let Some((offset, message)) = stream.try_next().await? {

            let topic: &str = offset.topic();
            let database = offset.database();
            let vgroup_id = offset.vgroup_id();
            log::debug!(
                "topic: {}, database: {}, vgroup_id: {}",
                topic,
                database,
                vgroup_id
            );

            match message {
                MessageSet::Meta(meta) => {
                    log::info!("Meta");
                    let raw = meta.as_raw_meta().await?;
                    taos.write_raw_meta(&raw).await?;

                    let json = meta.as_json_meta().await?;
                    let sql = json.to_string();
                    if let Err(err) = taos.exec(sql).await {
                        println!("maybe error: {}", err);
                    }
                }
                MessageSet::Data(data) => {
                    log::info!("Data");
                    while let Some(data) = data.fetch_raw_block().await? {
                        log::debug!("data: {:?}", data);
                    }
                }
                MessageSet::MetaData(meta, data) => {
                    log::info!("MetaData");
                    let raw = meta.as_raw_meta().await?;
                    taos.write_raw_meta(&raw).await?;

                    let json = meta.as_json_meta().await?;
                    let sql = json.to_string();
                    if let Err(err) = taos.exec(sql).await {
                        println!("maybe error: {}", err);
                    }

                    while let Some(data) = data.fetch_raw_block().await? {
                        log::debug!("data: {:?}", data);
                    }
                }
            }
            consumer.commit(offset).await?;
        }
    }
    // ANCHOR_END: consume

    // ANCHOR: assignments
    let assignments = consumer.assignments().await.unwrap();
    log::info!("assignments: {:?}", assignments);
    // ANCHOR_END: assignments

    // seek offset
    for topic_vec_assignment in assignments {
        let topic = &topic_vec_assignment.0;
        let vec_assignment = topic_vec_assignment.1;
        for assignment in vec_assignment {
            let vgroup_id = assignment.vgroup_id();
            let current = assignment.current_offset();
            let begin = assignment.begin();
            let end = assignment.end();
            log::debug!(
                "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                topic,
                vgroup_id,
                current,
                begin,
                end
            );
            // ANCHOR: seek_offset
            let res = consumer.offset_seek(topic, vgroup_id, end).await;
            if res.is_err() {
                log::error!("seek offset error: {:?}", res);
                let a = consumer.assignments().await.unwrap();
                log::error!("assignments: {:?}", a);
            }
            // ANCHOR_END: seek_offset
        }

        let topic_assignment = consumer.topic_assignment(topic).await;
        log::debug!("topic assignment: {:?}", topic_assignment);
    }

    // after seek offset
    let assignments = consumer.assignments().await.unwrap();
    log::info!("after seek offset assignments: {:?}", assignments);

    // ANCHOR: unsubscribe
    consumer.unsubscribe().await;
    // ANCHOR_END: unsubscribe

    tokio::time::sleep(Duration::from_secs(1)).await;

    taos.exec_many([
        "drop database db2",
        "drop topic topic_meters",
        "drop database power",
    ])
    .await?;
    Ok(())
}
