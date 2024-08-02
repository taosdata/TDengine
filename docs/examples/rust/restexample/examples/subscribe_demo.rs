use std::time::Duration;

use chrono::{DateTime, Local};
use taos::*;

// Query options 2, use deserialization with serde.
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct Record {
    // deserialize timestamp to chrono::DateTime<Local>
    ts: DateTime<Local>,
    // float to f32
    current: Option<f32>,
    // int to i32
    voltage: Option<i32>,
    phase: Option<f32>,
}

async fn prepare(taos: Taos) -> anyhow::Result<()> {
    let inserted = taos.exec_many([
        // create child table
        "CREATE TABLE `d0` USING `meters` TAGS(0, 'California.LosAngles')",
        // insert into child table
        "INSERT INTO `d0` values(now - 10s, 10, 116, 0.32)",
        // insert with NULL values
        "INSERT INTO `d0` values(now - 8s, NULL, NULL, NULL)",
        // insert and automatically create table with tags if not exists
        "INSERT INTO `d1` USING `meters` TAGS(1, 'California.SanFrancisco') values(now - 9s, 10.1, 119, 0.33)",
        // insert many records in a single sql
        "INSERT INTO `d1` values (now-8s, 10, 120, 0.33) (now - 6s, 10, 119, 0.34) (now - 4s, 11.2, 118, 0.322)",
    ]).await?;
    assert_eq!(inserted, 6);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://localhost:6030";
    let builder = TaosBuilder::from_dsn(dsn)?;

    let taos = builder.build().await?;
    let db = "tmq";

    // prepare database
    taos.exec_many([
        format!("DROP TOPIC IF EXISTS tmq_meters"),
        format!("DROP DATABASE IF EXISTS `{db}`"),
        format!("CREATE DATABASE `{db}` WAL_RETENTION_PERIOD 3600"),
        format!("USE `{db}`"),
        // create super table
        format!("CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` BINARY(24))"),
        // create topic for subscription
        format!("CREATE TOPIC tmq_meters AS SELECT * FROM `meters`")
    ])
    .await?;

    let task = tokio::spawn(prepare(taos));

    tokio::time::sleep(Duration::from_secs(1)).await;

    // subscribe
    let tmq = TmqBuilder::from_dsn("taos://localhost:6030/?group.id=test")?;

    let mut consumer = tmq.build().await?;
    consumer.subscribe(["tmq_meters"]).await?;

    // ANCHOR: consumer_commit_manually   
    consumer
        .stream()
        .try_for_each(|(offset, message)| async {
            let topic = offset.topic();
            // the vgroup id, like partition id in kafka.
            let vgroup_id = offset.vgroup_id();
            println!("* in vgroup id {vgroup_id} of topic {topic}\n");

            if let Some(data) = message.into_data() {
                while let Some(block) = data.fetch_raw_block().await? {
                    let records: Vec<Record> = block.deserialize().try_collect()?;
                    println!("** read {} records: {:#?}\n", records.len(), records);
                }
            }
            // commit offset manually when you have processed the message.
            consumer.commit(offset).await?;
            Ok(())
        })
        .await?;
    // ANCHOR_END: consumer_commit_manually

 
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
   
    consumer.unsubscribe().await;

    task.await??;

    Ok(())
}
