use std::collections::VecDeque;

use crate::controller::alloc_jobs::source_kafka::{TopicConcurrency, alloc, update_dsn};

use super::*;

#[derive(Debug, serde::Deserialize)]
pub struct TopicConsumers {
    pub name: String,
    pub concurrency: Option<usize>,
}

pub fn alloc_jobs(
    mut task: SplitJobResult,
    xnodes: &XNodes,
    via: Option<i64>,
) -> Result<AllocatedJobs> {
    let Some(from_map) = task.from.as_object_mut() else {
        return FromDsnNotObjectSnafu.fail();
    };
    let Some(topics) = from_map.remove("topics") else {
        return SplitTopicsNotFoundSnafu.fail();
    };
    let Some(topics) = topics.as_array() else {
        return SplitTopicNotArraySnafu.fail();
    };
    let mut mqtt_topics = topics
        .iter()
        .map(|v| serde_json::from_value::<TopicConsumers>(v.clone()))
        .collect::<std::result::Result<VecDeque<_>, _>>()
        .context(SplitTopicInvalidSnafu)?;
    if mqtt_topics.is_empty() {
        return TopicEmptySnafu.fail();
    }
    // mqtt 先处理 concurrency 为 None 的
    let mut topics = Vec::with_capacity(mqtt_topics.len());
    let mut jobs = Vec::new();
    let available_xnodes = xnodes.availables();
    while let Some(topic) = mqtt_topics.pop_front() {
        match topic.concurrency {
            Some(concurrency) => topics.push(TopicConcurrency {
                name: topic.name,
                concurrency,
            }),
            None => {
                let mut from = json_to_dsn(&task.from).context(JsonToDsnSnafu)?;
                from.set("topics", topic.name);
                for xnode in &available_xnodes {
                    if let Some(client_id) = from.get("client_id") {
                        from.set("client_id", format!("{client_id}_{}", uuid::Uuid::now_v7()));
                    }
                    jobs.push((
                        *xnode,
                        HaTask {
                            from: from.to_string(),
                            to: task.to.clone(),
                            parser: task.parser.clone(),
                            via,
                        },
                    ))
                }
            }
        }
    }
    let total_concurrency: usize = topics.iter().map(|v| v.concurrency).sum();
    tracing::info!(total_concurrency);
    let xnode_concurrency = xnodes.alloc_concurrency(total_concurrency, via);
    tracing::info!(?xnode_concurrency);
    jobs.extend(alloc(task, topics, xnode_concurrency, update_dsn, via)?);
    if jobs.len() == 1
        && let Some((id, job)) = jobs.pop()
    {
        Ok(AllocatedJobs::Task(id, job))
    } else {
        Ok(AllocatedJobs::Jobs(jobs))
    }
}
