use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use rdkafka::{
    Message, Offset,
    consumer::{Consumer, StreamConsumer},
};
use serde_json::json;
use taos::Dsn;
use taosx_core::{task_set::prelude::DsSampleIn, utils::codec::Processor};

use crate::blocking::{fetch_metadata, fetch_watermarks};
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

fn apply_fallback_offset(
    tp_list: &mut rdkafka::TopicPartitionList,
    fallback_offset: &str,
) -> anyhow::Result<()> {
    match fallback_offset {
        "smallest" | "earliest" | "beginning" => tp_list
            .set_all_offsets(Offset::Beginning)
            .context("failed to set offset to beginning"),
        "largest" | "latest" | "end" => tp_list
            .set_all_offsets(Offset::End)
            .context("failed to set offset to end"),
        _ => Ok(()),
    }
}

async fn get_sample_impl(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<HashMap<&'static str, String>>> {
    let connect_config = KafkaConnectConfig::from_dsn(dsn)?;
    let fallback_offset = KafkaTaskConfig::parse_fallback_offset(dsn)?;

    // create consumer
    let mut client_config = build_client_config(connect_config).await?;
    let consumer: StreamConsumer = client_config
        .set("group.id", "test")
        .set("auto.offset.reset", &fallback_offset)
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|err| anyhow::anyhow!("failed to create client, cause: {:#}", err))?;

    // subscribe topics
    let topic_names = KafkaTaskConfig::parse_topics(dsn)?;
    let topics = topic_names
        .iter()
        .map(|p| p.as_str())
        .collect::<Vec<&str>>();
    consumer
        .subscribe(&topics)
        .context("failed to subscribe to specified topics")?;

    let topic_fast_strs: Vec<faststr::FastStr> =
        topic_names.iter().map(faststr::FastStr::new).collect();
    let consumer = tracing_all_topics(&topic_fast_strs, consumer).await?;

    // assign offset to the beginning or end
    let mut tp_list = consumer
        .assignment()
        .with_context(|| format!("Get topics `{}` partition list error", topics.join(",")))?;
    apply_fallback_offset(&mut tp_list, fallback_offset.as_str())?;
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
                    res.insert("value", payload);
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

async fn tracing_all_topics(
    topics: &[faststr::FastStr],
    mut consumer: StreamConsumer,
) -> anyhow::Result<StreamConsumer> {
    for topic in topics {
        let (next_consumer, metadata_result) =
            fetch_metadata(consumer, Some(topic.to_string()), Duration::from_secs(1)).await?;
        consumer = next_consumer;
        let metadata = match metadata_result {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::warn!("failed to load metadata for sample topic: {topic}, error: {err:#}");
                return Ok(consumer);
            }
        };

        let topic_partitions = metadata
            .topics()
            .iter()
            .flat_map(|topic_meta| {
                topic_meta
                    .partitions()
                    .iter()
                    .map(move |partition| (topic_meta.name().to_string(), partition.id()))
            })
            .collect::<Vec<_>>();

        for (topic_name, partition_id) in topic_partitions {
            let (next_consumer, watermarks_result) = fetch_watermarks(
                consumer,
                topic_name.clone(),
                partition_id,
                Duration::from_secs(1),
            )
            .await?;
            consumer = next_consumer;
            let (low, high) = match watermarks_result {
                Ok(watermarks) => watermarks,
                Err(err) => {
                    tracing::warn!(
                        topic = topic_name.as_str(),
                        partition = partition_id,
                        error = ?err,
                        "failed to fetch sample topic watermarks"
                    );
                    return Ok(consumer);
                }
            };
            tracing::info!(
                "topic: {}, partition: {}, low: {}, high: {}",
                topic_name,
                partition_id,
                low,
                high
            );
        }
    }
    Ok(consumer)
}

#[cfg(test)]
mod tests {
    use rdkafka::TopicPartitionList;

    use super::*;

    #[test]
    fn apply_fallback_offset_sets_beginning_without_panicking() {
        let mut tp_list = TopicPartitionList::with_capacity(1);
        tp_list.add_partition("topic_a", 0);

        apply_fallback_offset(&mut tp_list, "beginning")
            .expect("fallback offset helper should return a result");

        let elements = tp_list.elements();
        assert_eq!(1, elements.len());
        assert_eq!(Offset::Beginning, elements[0].offset());
    }

    #[test]
    fn apply_fallback_offset_accepts_all_beginning_aliases() {
        for fallback_offset in ["smallest", "earliest", "beginning"] {
            let mut tp_list = TopicPartitionList::with_capacity(2);
            tp_list.add_partition("topic_a", 0);
            tp_list.add_partition("topic_a", 1);

            apply_fallback_offset(&mut tp_list, fallback_offset).unwrap();

            let offsets = tp_list
                .elements()
                .iter()
                .map(|element| element.offset())
                .collect::<Vec<_>>();
            assert_eq!(offsets, vec![Offset::Beginning, Offset::Beginning]);
        }
    }

    #[test]
    fn apply_fallback_offset_accepts_all_end_aliases() {
        for fallback_offset in ["largest", "latest", "end"] {
            let mut tp_list = TopicPartitionList::with_capacity(2);
            tp_list.add_partition("topic_a", 0);
            tp_list.add_partition("topic_b", 0);

            apply_fallback_offset(&mut tp_list, fallback_offset).unwrap();

            let offsets = tp_list
                .elements()
                .iter()
                .map(|element| element.offset())
                .collect::<Vec<_>>();
            assert_eq!(offsets, vec![Offset::End, Offset::End]);
        }
    }

    #[test]
    fn apply_fallback_offset_leaves_unknown_value_unchanged() {
        let mut tp_list = TopicPartitionList::with_capacity(2);
        tp_list.add_partition("topic_a", 0);
        tp_list.add_partition("topic_b", 0);
        tp_list.set_all_offsets(Offset::Offset(42)).unwrap();

        apply_fallback_offset(&mut tp_list, "stored").unwrap();

        let offsets = tp_list
            .elements()
            .iter()
            .map(|element| element.offset())
            .collect::<Vec<_>>();
        assert_eq!(offsets, vec![Offset::Offset(42), Offset::Offset(42)]);
    }
}
