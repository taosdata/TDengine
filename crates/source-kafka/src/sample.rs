use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use rdkafka::{
    Message, Offset,
    consumer::{Consumer, StreamConsumer},
};
use serde_json::json;
use taos::Dsn;
use taosx_core::{task_set::prelude::DsSampleIn, utils::codec::Processor};

use crate::config::{
    connect::KafkaConnectConfig,
    task::{KafkaTaskConfig, build_client_config},
};

pub async fn get_sample(dsn: &Dsn, limit: usize, timeout: Duration) -> anyhow::Result<DsSampleIn> {
    let sample_list = get_sample_impl(dsn, limit, timeout).await?;

    let sample_json = json!({
        "input": sample_list,
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
) -> anyhow::Result<Vec<HashMap<&'static str, String>>> {
    let connect_config = KafkaConnectConfig::from_dsn(dsn)?;
    let fallback_offset = KafkaTaskConfig::parse_fallback_offset(dsn)?;

    // create consumer
    let mut client_config = build_client_config(connect_config)?;
    let consumer: StreamConsumer = client_config
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
    let deadline = tokio::time::Instant::now() + timeout;
    let mut count = 0;
    let mut payload_list = Vec::with_capacity(limit);
    loop {
        let message = tokio::time::timeout_at(deadline, consumer.recv()).await;
        let Ok(message) = message else { break };
        match message {
            Ok(m) => {
                let mut res = HashMap::new();
                if let Some(p) = m.payload() {
                    let payload: String = processor
                        .process(p.to_vec())?
                        .try_into()
                        .context("payload not valid utf8 string")?;
                    res.insert("payload", payload);
                }
                if let Some(key) = m.key() {
                    let key: String = key
                        .to_vec()
                        .try_into()
                        .context("kafka key not valid utf8 string")?;
                    res.insert("key", key);
                }
                payload_list.push(res);
            }
            Err(err) => {
                tracing::error!("Kafka polling error: {:#}", err);
                anyhow::bail!("Kafka polling error: {:#}", err);
            }
        }
        count += 1;
        if !deadline.elapsed().is_zero() || count >= limit {
            break;
        }
    }

    Ok(payload_list)
}

fn tracing_all_topics(topics: &[&str], consumer: &StreamConsumer) -> anyhow::Result<()> {
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
