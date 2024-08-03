use std::time::Duration;
use std::str::FromStr;
use chrono::Local;
use chrono::DateTime;
use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::formatted_timed_builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    use taos_query::prelude::*;
    // ANCHOR: create_consumer_dsn    
    let dsn = "ws://localhost:6041".to_string();
    log::info!("dsn: {}", dsn);
    let mut dsn = Dsn::from_str(&dsn)?;
    // ANCHOR_END: create_consumer_dsn

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

    // ANCHOR: create_topic
    taos.exec_many([
        "CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM power.meters",
    ])
    .await?;
    // ANCHOR_END: create_topic

    // ANCHOR: create_consumer_ac
    dsn.params.insert("auto.offset.reset".to_string(), "latest".to_string());
    dsn.params.insert("msg.with.table.name".to_string(), "true".to_string());
    dsn.params.insert("enable.auto.commit".to_string(), "true".to_string());
    dsn.params.insert("auto.commit.interval.ms".to_string(), "1000".to_string());
    dsn.params.insert("group.id".to_string(), "group1".to_string());
    dsn.params.insert("client.id".to_string(), "client1".to_string());

    let builder = TmqBuilder::from_dsn(&dsn)?;
    let mut consumer = builder.build().await?;
    // ANCHOR_END: create_consumer_ac

    // ANCHOR: subscribe
    consumer.subscribe(["topic_meters"]).await?;
    // ANCHOR_END: subscribe

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
        groupid: i32,
        // binary/varchar to String
        location: String,
    }

    // ANCHOR: consume

    consumer
    .stream()
    .try_for_each(|(offset, message)| async move {
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
        Ok(())
    })
    .await?;

    // ANCHOR_END: consume

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

    // ANCHOR: unsubscribe
    consumer.unsubscribe().await;
    // ANCHOR_END: unsubscribe

    tokio::time::sleep(Duration::from_secs(1)).await;

    taos.exec_many([
        "drop topic topic_meters",
        "drop database power",
    ])
    .await?;
    Ok(())
}
