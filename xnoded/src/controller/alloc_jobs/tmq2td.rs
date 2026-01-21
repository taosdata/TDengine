use ha_core::types::SplitJobResult;
use tracing::instrument;

use super::*;
use crate::controller::{
    alloc_jobs::source_kafka::{TopicConcurrency, alloc},
    xnodes::XNodes,
};

#[derive(Debug, serde::Deserialize)]
pub struct TopicVgroups {
    pub name: String,
    pub vgroups: usize,
}

impl From<TopicVgroups> for TopicConcurrency {
    fn from(value: TopicVgroups) -> Self {
        Self {
            name: value.name,
            concurrency: value.vgroups,
        }
    }
}

#[instrument(skip_all)]
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
    let topics = topics
        .iter()
        .map(|v| serde_json::from_value::<TopicVgroups>(v.clone()))
        .map(|v| v.map(Into::<TopicConcurrency>::into))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context(SplitTopicInvalidSnafu)?;
    if topics.is_empty() {
        return TopicEmptySnafu.fail();
    }
    let total_concurrency: usize = topics.iter().map(|v| v.concurrency).sum();
    tracing::info!(total_concurrency);
    let xnode_concurrency = xnodes.alloc_concurrency(total_concurrency, via);
    tracing::info!(?xnode_concurrency);

    let mut jobs = alloc(task, topics, xnode_concurrency, update_dsn, via)?;
    if jobs.len() == 1
        && let Some((id, job)) = jobs.pop()
    {
        Ok(AllocatedJobs::Task(id, job))
    } else {
        Ok(AllocatedJobs::Jobs(jobs))
    }
}

fn update_dsn(mut from: Dsn, topic: &str, concurrency: usize) -> String {
    from.subject = Some(topic.to_string());
    from.set("read_concurrency", concurrency.to_string());
    from.to_string()
}
