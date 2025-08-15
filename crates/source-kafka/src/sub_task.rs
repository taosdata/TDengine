use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use itertools::Itertools;
use rdkafka::consumer::{BaseConsumer, Consumer};
use taosx_core::core_metrics::CoreMetrics;

use crate::{
    FETCH_METADATA_TIMEOUT, LoggingConsumer, METRIC_TOTAL_PARTITIONS,
    config::task::{KafkaTaskConfig, build_client_config},
};

pub struct SubTask {
    /// kafka task config for rebuilding consumer.
    pub config: Arc<KafkaTaskConfig>,
    /// topics to consume.
    pub topics: Arc<Vec<String>>,
    /// unique id in the group, for rdkafka `group.instance.id` configuration.
    pub instance: Option<String>,
    /// initial consumer.
    pub consumer: LoggingConsumer,
    /// timeout for polling messages in milliseconds.
    pub timeout: i64,
}

impl SubTask {
    pub async fn build_tasks(
        config: KafkaTaskConfig,
        metrics: &Arc<CoreMetrics>,
    ) -> anyhow::Result<Vec<Self>> {
        let client_config = build_client_config(config.connect.clone())?;

        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .context("failed to create consumer")?;

        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, FETCH_METADATA_TIMEOUT)
            .context("failed to load meta data")?;

        tracing::info!(
            brokers = metadata
                .brokers()
                .iter()
                .map(|b| format!("{}={}:{}", b.id(), b.host(), b.port()))
                .join(","),
            broker.id = metadata.orig_broker_id(),
            broker.name = metadata.orig_broker_name(),
            "kafka metadata"
        );

        // topic -> partition count
        let topic_partitions: HashMap<&str, usize> = metadata
            .topics()
            .iter()
            .filter(|tp| !tp.name().starts_with("__"))
            .filter(|tp| config.topics.contains(&tp.name().to_string()))
            .map(|tp| (tp.name(), tp.partitions().len()))
            .collect();

        if topic_partitions.is_empty() {
            tracing::error!(
                "topics is empty, expected: {:?}, please check your topic authorization",
                config.topics
            );
            anyhow::bail!(
                "topics is empty, expected: {:?}, please check your topic authorization",
                config.topics
            );
        }
        metrics.ipc().set_extra_metric(
            &METRIC_TOTAL_PARTITIONS,
            topic_partitions.values().sum::<usize>() as _,
        );

        if topic_partitions.len() != config.topics.len() {
            tracing::error!(
                "Some topics are not readable, expected: {:?}, actual: {:?}, please check your topic authorization",
                config.topics.len(),
                topic_partitions.len()
            );
            anyhow::bail!(
                "Some topics are not readable, expected: {:?}, actual: {:?}, please check your topic authorization",
                config.topics.len(),
                topic_partitions.len()
            );
        }

        let mut sub_tasks = Vec::new();
        let concurrency = match config.advanced_options.read_concurrency {
            Some(n) if n > 0 => n.min(topic_partitions.values().sum()),
            _ => topic_partitions.values().sum(),
        };

        let config = Arc::new(config);
        let topics = topic_partitions.keys().map(|k| k.to_string()).collect_vec();
        let topics = Arc::new(topics);

        for i in 0..concurrency {
            let instance = if config.enable_group_instance_id {
                Some(format!("{i}-{}", uuid::Uuid::new_v4()))
            } else {
                None
            };
            let consumer = config
                .build_consumer(
                    instance.as_deref(),
                    &topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                    metrics,
                )
                .await?;
            let topics = topics.clone();

            let sub_task = SubTask {
                config: config.clone(),
                topics,
                instance,
                consumer,
                timeout: config.timeout,
            };
            sub_tasks.push(sub_task);
        }
        for (idx, t) in sub_tasks.iter().enumerate() {
            match t.consumer.assignment() {
                Ok(tp_list) => {
                    for tp in tp_list.elements() {
                        tracing::info!(
                            consumer.id = idx,
                            consumer.topic = tp.topic(),
                            consumer.partition = tp.partition(),
                            consumer.offset = ?tp.offset(),
                        );
                    }
                }
                Err(err) => {
                    tracing::error!(
                        consumer.id = idx,
                        "Consumer {idx} failed to assign partitions: {err}",
                    );
                }
            }
        }

        Ok(sub_tasks)
    }
}
