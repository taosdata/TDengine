use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use linked_hash_map::LinkedHashMap;
use rdkafka::{
    Message, Offset,
    consumer::{BaseConsumer, Consumer},
};
use serde_json::json;
use taos::Dsn;
use taosx_core::{task_set::prelude::DsSampleIn, utils::codec::Processor};

use crate::config::{
    connect::KafkaConnectConfig,
    task::{KafkaTaskConfig, build_client_config},
};

pub async fn get_sample(dsn: &Dsn, limit: usize, timeout: Duration) -> anyhow::Result<DsSampleIn> {
    let sample_list: Vec<String> = get_sample_impl(dsn, limit, timeout).await?;

    let mut sample_vec: Vec<LinkedHashMap<String, serde_json::Value>> = Vec::new();
    for payload in sample_list {
        let mut p = LinkedHashMap::new();
        p.insert("payload".to_string(), json!(payload));
        sample_vec.push(p);
    }

    let sample_json = json!({
        "input": sample_vec,
        "parser": {}
    });

    let sample: DsSampleIn = serde_json::from_value(sample_json.clone()).map_err(|err| {
        anyhow::anyhow!(
            "failed to parse kafka sample data: {:?}, cause: {:?}",
            sample_json,
            err
        )
    })?;

    Ok(sample)
}

async fn get_sample_impl(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<String>> {
    let connect_config = KafkaConnectConfig::from_dsn(dsn)?;
    let fallback_offset = KafkaTaskConfig::parse_fallback_offset(dsn)?;

    // create consumer
    let mut client_config = build_client_config(connect_config)?;
    let consumer: BaseConsumer = client_config
        .set("group.id", "test")
        .set("auto.offset.reset", &fallback_offset)
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|err| anyhow::anyhow!("failed to create client, cause: {:#}", err))?;

    // subscribe topics
    let topics = KafkaTaskConfig::parse_topics(dsn)?;
    let topics = topics.iter().map(|p| p.as_str()).collect::<Vec<&str>>();
    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");

    let _ = tracing_all_topics(&topics, &consumer);

    // assign offset to the beginning or end
    let mut tp_list = consumer
        .assignment()
        .with_context(|| format!("Get topics `{}` partition list error", topics.join(",")))?;
    match fallback_offset.as_str() {
        "smallest" | "earliest" | "beginning" => {
            tp_list
                .set_all_offsets(Offset::Beginning)
                .expect("failed to set offset");
        }
        "largest" | "latest" | "end" => {
            tp_list
                .set_all_offsets(Offset::End)
                .expect("failed to set offset");
        }
        _ => {
            // nothing to do
        }
    };
    consumer
        .assign(&tp_list)
        .with_context(|| format!("Assign consumer on topics `{}`", topics.join(",")))?;

    let processor = KafkaTaskConfig::parse_codec_processor(dsn)?;

    // polling message from kafka
    let start = Utc::now().timestamp();
    let mut count = 0;
    let mut payload_list: Vec<String> = Vec::new();
    loop {
        let message = consumer.poll(Duration::from_secs(1));
        if let Some(msg) = message {
            match msg {
                Ok(m) => {
                    if let Some(p) = m.payload() {
                        let payload = processor.process(p.to_vec())?;
                        payload_list
                            .push(String::from_utf8(payload).context("payload not valid string")?);
                    }
                }
                Err(err) => {
                    tracing::error!("Kafka polling error: {:#}", err);
                    anyhow::bail!("Kafka polling error: {:#}", err);
                }
            }
            count += 1;
        }
        let now = Utc::now().timestamp();
        if now - start > timeout.as_secs() as i64 || count >= limit {
            break;
        }
    }

    Ok(payload_list)
}

fn tracing_all_topics(topics: &[&str], consumer: &BaseConsumer) -> anyhow::Result<()> {
    for topic in topics {
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(1))
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to load meta data for topic: {}, cause: {:#}",
                    topic,
                    err
                )
            })?;

        for topic_meta in metadata.topics() {
            for partition in topic_meta.partitions() {
                let (low, high) = consumer
                    .fetch_watermarks(topic_meta.name(), partition.id(), Duration::from_secs(1))
                    .map_err(|err| {
                        anyhow::anyhow!(
                            "failed to fetch watermarks for topic: {}, partition: {}, cause: {:#}",
                            topic_meta.name(),
                            partition.id(),
                            err
                        )
                    })?;
                tracing::info!(
                    "topic: {}, partition: {}, low: {}, high: {}",
                    topic_meta.name(),
                    partition.id(),
                    low,
                    high
                );
            }
        }
    }
    Ok(())
}
