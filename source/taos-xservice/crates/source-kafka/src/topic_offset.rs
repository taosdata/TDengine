use std::{sync::Arc, time::Duration};

use anyhow::Context;
use taos::Dsn;
use taosx_core::{
    core_metrics::{CoreMetrics, find_metrics_arc},
    sink::ipc_metric::IpcMetrics,
};

use crate::{
    LoggingConsumer,
    blocking::{fetch_metadata, fetch_watermarks},
    config::task::KafkaTaskConfig,
};

#[derive(Debug, serde::Serialize)]
pub struct TopicOffsetInfo {
    pub topic: String,
    pub partition: i32,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

pub async fn get_topics_offset(
    task_job_id: Option<(i64, i64)>,
    from: &Dsn,
) -> anyhow::Result<Vec<TopicOffsetInfo>> {
    // kafka task config
    let config = KafkaTaskConfig::from_dsn(from)?;

    let metrics_arc =
        find_metrics_arc(task_job_id).unwrap_or(Arc::new(CoreMetrics::IPC(IpcMetrics::default())));

    let topics = config
        .topics
        .iter()
        .map(String::as_str)
        .collect::<Vec<&str>>();

    let mut consumer: LoggingConsumer = config.build_consumer(None, &topics, &metrics_arc).await?;
    let (next_consumer, metadata_result) =
        fetch_metadata(consumer, None, Duration::from_secs(1)).await?;
    consumer = next_consumer;
    let metadata = metadata_result.context("failed to load meta data")?;

    let mut topic_offset_ranges = Vec::new();
    for tp in metadata.topics() {
        if !topics.contains(&tp.name()) {
            continue;
        }

        for partition in tp.partitions() {
            let (next_consumer, watermarks_result) = fetch_watermarks(
                consumer,
                tp.name().to_string(),
                partition.id(),
                Duration::from_secs(1),
            )
            .await?;
            consumer = next_consumer;
            let (low, high) = watermarks_result.context("failed to fetch watermarks")?;
            let offset = TopicOffsetInfo {
                topic: tp.name().to_string(),
                partition: partition.id(),
                low_watermark: low,
                high_watermark: high,
            };
            topic_offset_ranges.push(offset);
        }
    }

    Ok(topic_offset_ranges)
}
