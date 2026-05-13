use crate::{
    METRIC_TOTAL_PARTITIONS,
    config::task::PulsarTaskConfig,
    consumer::{CustomConsumer, build_consumer, split_topics},
};
use std::sync::Arc;
use taosx_core::core_metrics::CoreMetrics;

pub struct SubTask {
    pub id: String,
    /// topics to consume.
    pub topic: String,
    /// pulsar task config for rebuilding consumer.
    pub config: Arc<PulsarTaskConfig>,
    /// initial consumer.
    pub consumer: CustomConsumer,
    /// timeout for polling messages in milliseconds.
    pub timeout: i64,
}

impl SubTask {
    pub async fn build_tasks(
        config: &PulsarTaskConfig,
        metrics: &Arc<CoreMetrics>,
    ) -> anyhow::Result<Vec<Self>> {
        let mut topics = vec![];
        for topic in config.topics.iter() {
            topics.extend(split_topics(config, topic).await?);
        }

        metrics
            .ipc()
            .set_extra_metric(&METRIC_TOTAL_PARTITIONS, topics.len() as _);

        let mut sub_tasks = Vec::new();
        let concurrency = match config.advanced_options.read_concurrency {
            Some(n) if n > 0 => n.max(topics.len()),
            _ => topics.len(),
        };

        let config = Arc::new(config.clone());
        let topics = Arc::new(topics);

        for i in 0..concurrency {
            let id = format!("{i}-{}", uuid::Uuid::new_v4());
            let topic = topics[i % topics.len()].clone();
            let consumer = build_consumer(&config, &topic, metrics).await?;

            let sub_task = SubTask {
                config: config.clone(),
                topic,
                id,
                consumer,
                timeout: config.timeout,
            };
            sub_tasks.push(sub_task);
        }

        Ok(sub_tasks)
    }
}
