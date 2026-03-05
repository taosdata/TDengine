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

fn update_dsn(mut from: Dsn, topic: &str, concurrency: usize, _job_index: usize) -> String {
    from.subject = Some(topic.to_string());
    from.set("read_concurrency", concurrency.to_string());
    from.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use ha_core::types::SplitJobResult;

    #[test]
    fn topic_vgroups_converts_to_topic_concurrency() {
        let vgroups = TopicVgroups {
            name: "topic1".into(),
            vgroups: 3,
        };
        let tc: TopicConcurrency = vgroups.into();
        assert_eq!(tc.name, "topic1");
        assert_eq!(tc.concurrency, 3);
    }

    #[test]
    fn update_dsn_sets_subject_and_concurrency() {
        let dsn = Dsn::from_str("tmq://").unwrap();
        let updated = update_dsn(dsn, "subj", 5, 0);
        let parsed = Dsn::from_str(&updated).unwrap();
        assert_eq!(parsed.subject.as_deref(), Some("subj"));
        assert_eq!(
            parsed.get("read_concurrency"),
            Some("5".to_string()).as_ref()
        );
    }

    #[test]
    fn alloc_jobs_errors_when_topics_missing_or_empty() {
        let xnodes = XNodes::new();

        let task_no_topics = SplitJobResult {
            from: serde_json::json!({"type": "tmq"}),
            to: "taos://localhost:6030".into(),
            parser: None,
        };
        let res = alloc_jobs(task_no_topics, &xnodes, None);
        assert!(matches!(res, Err(Error::SplitTopicsNotFound)));

        let task_empty_topics = SplitJobResult {
            from: serde_json::json!({"type": "tmq", "topics": []}),
            to: "taos://localhost:6030".into(),
            parser: None,
        };
        let res = alloc_jobs(task_empty_topics, &xnodes, None);
        assert!(matches!(res, Err(Error::TopicEmpty)));
    }
}
