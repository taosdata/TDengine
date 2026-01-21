use std::{collections::HashSet, time::Duration};

use anyhow::Context;
use ha_core::types::{SplitJobResult, SplitJobTask};
use rdkafka::consumer::{BaseConsumer, Consumer};
use taosx_utils::dsn::{dsn_to_json, parse_multiple_value};

use crate::config::task::{KafkaTaskConfig, build_client_config};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct TopicInfo {
    name: String,
    concurrency: usize,
}

pub async fn split_job(task: SplitJobTask) -> anyhow::Result<SplitJobResult> {
    let from = task.from;
    let input_topics =
        parse_multiple_value::<String>(&from, "topics")?.context("kafka topics not found")?;

    let config = KafkaTaskConfig::from_dsn(&from)?;
    let client_config = build_client_config(config.connect)?;
    let consumer: BaseConsumer = client_config
        .create()
        .context("kafka build consumer error")?;
    let metadata = tokio::task::spawn_blocking(move || {
        consumer
            .fetch_metadata(None, Duration::from_secs(30))
            .context("kafka fetch metadata error")
    })
    .await??;
    let input_topics = HashSet::<String>::from_iter(input_topics);
    let mut topics = Vec::with_capacity(input_topics.len());
    for topic in metadata.topics() {
        let name = topic.name();
        if !input_topics.contains(name) {
            continue;
        }
        let partitions = topic.partitions().len();
        topics.push(TopicInfo {
            name: name.into(),
            concurrency: partitions,
        });
    }
    let topics_value = serde_json::to_value(topics).context("serialize topics error")?;

    let mut from_json = dsn_to_json(&from);
    if let Some(from) = from_json.as_object_mut() {
        from.insert("topics".into(), topics_value);
    }

    Ok(SplitJobResult {
        from: from_json,
        to: task.to.to_string(),
        parser: task.parser,
    })
}
