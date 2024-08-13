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
    let dsn = "taos://localhost:6030".to_string();
    println!("dsn: {}", dsn);
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
    let group_id = "group1".to_string();
    let client_id = "client1".to_string();
    dsn.params.insert("auto.offset.reset".to_string(), "latest".to_string());
    dsn.params.insert("msg.with.table.name".to_string(), "true".to_string());
    dsn.params.insert("enable.auto.commit".to_string(), "true".to_string());
    dsn.params.insert("auto.commit.interval.ms".to_string(), "1000".to_string());
    dsn.params.insert("group.id".to_string(), group_id.clone());
    dsn.params.insert("client.id".to_string(), client_id.clone());

    let builder = TmqBuilder::from_dsn(&dsn)?;
    let mut consumer = match builder.build().await{
        Ok(consumer) => {
            println!("Create consumer successfully, dsn: {}, groupId: {}, clientId: {}.", dsn, group_id, client_id);
            consumer
        }
        Err(err) => {
            eprintln!("Failed to create consumer, dsn: {}; ErrMessage: {}", dsn, err);
            return Err(err.into());
        }
    };
    // ANCHOR_END: create_consumer_ac

    // ANCHOR: consume
    match consumer.subscribe(["topic_meters"]).await{
        Ok(_) => println!("Subscribe topics successfully."),
        Err(err) => {
            eprintln!("Failed to subscribe topic_meters, dsn: {}; ErrMessage: {}", dsn, err);
            return Err(err.into());
        }
    }

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
    .await.map_err(|e| {
        eprintln!("Failed to execute consumer functions. ErrMessage: {:?}", e);
        e
    })?;

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
            match consumer.commit(offset).await{
                Ok(_) => println!("Commit offset manually successfully."),
                Err(err) => {
                    eprintln!("Failed to commit offset manually, dsn: {}; ErrMessage: {}", dsn, err);
                    return Err(err.into());
                }
            }
            Ok(())
        })
        .await.map_err(|e| {
            eprintln!("Failed to execute consumer functions. ErrMessage: {:?}", e);
            e
        })?;
    // ANCHOR_END: consumer_commit_manually


    // ANCHOR: seek_offset
    let assignments = consumer.assignments().await.unwrap();
    println!("Now assignments: {:?}", assignments);

    // seek offset
    for topic_vec_assignment in assignments {
        let topic = &topic_vec_assignment.0;
        let vec_assignment = topic_vec_assignment.1;
        for assignment in vec_assignment {
            let vgroup_id = assignment.vgroup_id();
            let current = assignment.current_offset();
            let begin = assignment.begin();
            let end = assignment.end();
            println!(
                "topic: {}, vgroup_id: {}, current offset: {} begin {}, end: {}",
                topic,
                vgroup_id,
                current,
                begin,
                end
            );

            match consumer.offset_seek(topic, vgroup_id, begin).await{
                Ok(_) => (),
                Err(err) => {
                    eprintln!("Seek example failed; ErrMessage: {}", err);
                    return Err(err.into());
                }
            }
        }

        let topic_assignment = consumer.topic_assignment(topic).await;
        println!("Topic assignment: {:?}", topic_assignment);
    }
    println!("Assignment seek to beginning successfully.");
    // after seek offset
    let assignments = consumer.assignments().await.unwrap();
    println!("After seek offset assignments: {:?}", assignments);
    // ANCHOR_END: seek_offset

    // ANCHOR: unsubscribe
    consumer.unsubscribe().await;
    println!("Consumer unsubscribed successfully.");
    // ANCHOR_END: unsubscribe

    tokio::time::sleep(Duration::from_secs(1)).await;

    taos.exec_many([
        "drop topic topic_meters",
        "drop database power",
    ])
    .await?;
    Ok(())
}
