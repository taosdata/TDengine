use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use anyhow::Context;
use itertools::Itertools;
use rdkafka::consumer::{BaseConsumer, Consumer};
use taosx_core::core_metrics::CoreMetrics;

use crate::{
    LoggingConsumer, METRIC_TOTAL_PARTITIONS,
    blocking::fetch_metadata,
    config::task::{KafkaTaskConfig, build_client_config},
};

const METADATA_RETRIES: usize = 3;
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const METADATA_RETRY_SLEEP: Duration = Duration::from_secs(2);

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
        let client_config = build_client_config(config.connect.clone()).await?;

        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .context("failed to create consumer")?;

        // fetch metadata
        let metadata = fetch_metadata_with_retries(consumer).await?;

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

async fn fetch_metadata_with_retries(
    consumer: BaseConsumer,
) -> anyhow::Result<rdkafka::metadata::Metadata> {
    retry_startup_metadata(
        consumer,
        METADATA_RETRY_SLEEP,
        |_attempt, consumer| async move {
            let (next_consumer, metadata_result) =
                fetch_metadata(consumer, None, METADATA_TIMEOUT).await?;
            Ok((next_consumer, metadata_result))
        },
    )
    .await
}

async fn retry_startup_metadata<S, T, Op, Fut>(
    mut state: S,
    retry_sleep: Duration,
    mut op: Op,
) -> anyhow::Result<T>
where
    Op: FnMut(usize, S) -> Fut,
    Fut: Future<Output = anyhow::Result<(S, anyhow::Result<T>)>>,
{
    let mut last_error = None;

    for attempt in 1..=METADATA_RETRIES {
        let (next_state, result) = op(attempt, state).await?;
        state = next_state;
        match result {
            Ok(value) => return Ok(value),
            Err(err) => {
                tracing::warn!(
                    "failed to load kafka metadata while building startup tasks, attempt: {attempt}/{METADATA_RETRIES}, error: {err:#}"
                );
                last_error = Some(err);
                if attempt < METADATA_RETRIES {
                    tokio::time::sleep(retry_sleep).await;
                }
            }
        }
    }

    let err = last_error
        .unwrap_or_else(|| anyhow::anyhow!("metadata load failed without an underlying error"));
    Err(err).context(format!(
        "failed to load meta data after {METADATA_RETRIES} attempts"
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use anyhow::anyhow;

    use super::{METADATA_RETRIES, METADATA_RETRY_SLEEP, METADATA_TIMEOUT, retry_startup_metadata};

    #[test]
    fn startup_metadata_retry_constants_match_plan() {
        assert_eq!(METADATA_RETRIES, 3);
        assert_eq!(METADATA_TIMEOUT, Duration::from_secs(10));
        assert_eq!(METADATA_RETRY_SLEEP, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn retry_startup_metadata_stops_after_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = retry_startup_metadata((), Duration::ZERO, {
            let attempts = attempts.clone();
            move |_attempt, state| {
                let attempts = attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if current < 2 {
                        Ok((
                            state,
                            Err::<usize, _>(anyhow!(
                                "transient startup metadata failure #{current}"
                            )),
                        ))
                    } else {
                        Ok((state, Ok(current)))
                    }
                }
            }
        })
        .await
        .expect("retry helper should return the first successful startup attempt");

        assert_eq!(result, 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retry_startup_metadata_returns_last_error_with_context() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let err = retry_startup_metadata((), Duration::ZERO, {
            let attempts = attempts.clone();
            move |_attempt, state| {
                let attempts = attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok((
                        state,
                        Err::<usize, _>(anyhow!("transient startup metadata failure #{current}")),
                    ))
                }
            }
        })
        .await
        .expect_err("retry helper should fail after exhausting startup metadata retries");

        assert_eq!(attempts.load(Ordering::SeqCst), METADATA_RETRIES);
        assert_eq!(
            format!("{err:#}"),
            format!(
                "failed to load meta data after {METADATA_RETRIES} attempts: transient startup metadata failure #{METADATA_RETRIES}"
            )
        );
    }
}
