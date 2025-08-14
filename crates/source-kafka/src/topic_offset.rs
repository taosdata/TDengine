use std::{sync::Arc, time::Duration};

use anyhow::Context;
use rdkafka::consumer::Consumer;
use taos::Dsn;
use taosx_core::{
    core_metrics::{CoreMetrics, find_metrics_arc},
    sink::ipc_metric::IpcMetrics,
};

use crate::{LoggingConsumer, config::task::KafkaTaskConfig};

#[derive(Debug, serde::Serialize)]
pub struct TopicOffsetInfo {
    pub topic: String,
    pub partition: i32,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

pub async fn get_topics_offset(
    task_id: Option<i64>,
    from: &Dsn,
) -> anyhow::Result<Vec<TopicOffsetInfo>> {
    // kafka task config
    let config = KafkaTaskConfig::from_dsn(from)?;

    let metrics_arc = find_metrics_arc(task_id)
        .await
        .unwrap_or(Arc::new(CoreMetrics::IPC(IpcMetrics::default())));

    let topics = config
        .topics
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();

    let consumer: LoggingConsumer = config.build_consumer(None, &topics, &metrics_arc).await?;

    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(1))
        .context("failed to load meta data")?;

    let mut topic_offset_ranges = Vec::new();
    for tp in metadata.topics() {
        if !topics.contains(&tp.name()) {
            continue;
        }

        for partition in tp.partitions() {
            let (low, high) = consumer
                .fetch_watermarks(tp.name(), partition.id(), Duration::from_secs(1))
                .expect("failed to fetch watermarks");
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
