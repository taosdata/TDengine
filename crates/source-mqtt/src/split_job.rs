use anyhow::Context;
use ha_core::types::{SplitJobResult, SplitJobTask};
use taosx_utils::dsn::dsn_to_json;

use crate::config::MqttConfig;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Task {
    from: String,
    to: String,
    parser: serde_json::Value,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct TopicInfo {
    name: String,
    qos: u8,
    concurrency: usize,
}

pub async fn split_job(task: SplitJobTask) -> anyhow::Result<SplitJobResult> {
    let from = task.from;
    let config = MqttConfig::try_from(&from)?;

    let mut topics = Vec::with_capacity(config.topics.len());
    for (topic, qos) in config.topics {
        if topic.starts_with("$share/") {
            topics.push(TopicInfo {
                name: topic,
                qos,
                concurrency: 0,
            })
        } else {
            topics.push(TopicInfo {
                name: topic,
                qos,
                concurrency: 1,
            })
        }
    }
    let topics_value = serde_json::to_value(topics).context("serialize mqtt topics info error")?;

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
