use ha_core::types::SplitJobResult;
use taosx_utils::dsn::json_to_dsn;

use super::*;

#[derive(Debug, serde::Deserialize)]
pub struct TopicConcurrency {
    pub name: String,
    pub concurrency: usize,
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
    let topics = topics
        .iter()
        .map(|v| serde_json::from_value::<TopicConcurrency>(v.clone()))
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

pub fn alloc<I, F>(
    task: SplitJobResult,
    topics: I,
    xnode_concurrency: Vec<(i32, usize)>,
    job_dsn: F,
    via: Option<i64>,
) -> Result<Vec<(i32, HaTask)>>
where
    I: IntoIterator<Item = TopicConcurrency>,
    F: Fn(Dsn, &str, usize, usize) -> String,
{
    let mut topics = topics.into_iter();
    let from = json_to_dsn(&task.from).context(JsonToDsnSnafu)?;
    let mut jobs = Vec::new();
    let mut current_tp = topics.next();
    for (id, mut concurrency) in xnode_concurrency {
        loop {
            let Some(tp) = &mut current_tp else {
                break;
            };
            if tp.concurrency == 0 || concurrency == 0 {
                break;
            }
            if concurrency >= tp.concurrency {
                let from = job_dsn(from.clone(), &tp.name, tp.concurrency, jobs.len());
                let job = HaTask {
                    from,
                    to: task.to.clone(),
                    parser: task.parser.clone(),
                    via,
                    labels: None,
                };
                jobs.push((id, job));
                concurrency -= tp.concurrency;
                current_tp = topics.next();
            } else {
                let from = job_dsn(from.clone(), &tp.name, concurrency, jobs.len());
                let job = HaTask {
                    from,
                    to: task.to.clone(),
                    parser: task.parser.clone(),
                    via,
                    labels: None,
                };
                jobs.push((id, job));
                tp.concurrency -= concurrency;
                break;
            }
        }
    }

    Ok(jobs)
}

pub fn update_dsn(mut from: Dsn, topic: &str, concurrency: usize, _job_index: usize) -> String {
    from.set("topics", topic.to_string());
    from.set("read_concurrency", concurrency.to_string());
    from.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_test() {
        let task = SplitJobResult {
            from: serde_json::json!({
                "type": "kafka"
            }),
            to: "taos://localhost:6030".into(),
            parser: None,
        };
        let topics = vec![
            TopicConcurrency {
                name: "a".into(),
                concurrency: 3,
            },
            TopicConcurrency {
                name: "b".into(),
                concurrency: 5,
            },
            TopicConcurrency {
                name: "c".into(),
                concurrency: 8,
            },
        ];
        let xnode_concurrency = vec![(0, 3), (1, 4), (2, 9)];
        let jobs = alloc(task, topics, xnode_concurrency, update_dsn, None).unwrap();
        assert_eq!(
            jobs,
            vec![
                (
                    0,
                    HaTask {
                        from: "kafka://?read_concurrency=3&topics=a".into(),
                        to: "taos://localhost:6030".into(),
                        parser: None,
                        via: None,
                        labels: None
                    },
                ),
                (
                    1,
                    HaTask {
                        from: "kafka://?read_concurrency=4&topics=b".into(),
                        to: "taos://localhost:6030".into(),
                        parser: None,
                        via: None,
                        labels: None
                    },
                ),
                (
                    2,
                    HaTask {
                        from: "kafka://?read_concurrency=1&topics=b".into(),
                        to: "taos://localhost:6030".into(),
                        parser: None,
                        via: None,
                        labels: None
                    },
                ),
                (
                    2,
                    HaTask {
                        from: "kafka://?read_concurrency=8&topics=c".into(),
                        to: "taos://localhost:6030".into(),
                        parser: None,
                        via: None,
                        labels: None
                    },
                )
            ]
        )
    }

    #[test]
    fn alloc_propagates_via_to_all_jobs() {
        let task = SplitJobResult {
            from: serde_json::json!({
                "type": "kafka"
            }),
            to: "taos://localhost:6030".into(),
            parser: None,
        };
        let topics = vec![TopicConcurrency {
            name: "a".into(),
            concurrency: 3,
        }];
        let xnode_concurrency = vec![(0, 1), (1, 2)];
        let jobs = alloc(task, topics, xnode_concurrency, update_dsn, Some(7)).unwrap();
        assert!(!jobs.is_empty());
        for (_, job) in jobs {
            assert_eq!(job.via, Some(7));
        }
    }

    #[test]
    fn alloc_single_node_assigns_all_jobs_to_same_xnode() {
        let task = SplitJobResult {
            from: serde_json::json!({
                "type": "kafka"
            }),
            to: "taos://localhost:6030".into(),
            parser: None,
        };
        let topics = vec![
            TopicConcurrency {
                name: "a".into(),
                concurrency: 1,
            },
            TopicConcurrency {
                name: "b".into(),
                concurrency: 2,
            },
        ];
        let xnode_concurrency = vec![(5, 3)];
        let jobs = alloc(task, topics, xnode_concurrency, update_dsn, None).unwrap();
        assert!(!jobs.is_empty());
        for (xid, _) in jobs {
            assert_eq!(xid, 5);
        }
    }

    #[test]
    fn update_dsn_sets_topic_and_concurrency() {
        let dsn = Dsn::from_str("kafka://").unwrap();
        let updated = update_dsn(dsn, "my_topic", 4, 0);
        assert!(updated.contains("topics=my_topic"));
        assert!(updated.contains("read_concurrency=4"));
    }

    #[test]
    fn alloc_jobs_errors_when_topics_missing_or_empty() {
        let xnodes = XNodes::new();

        let task_no_topics = SplitJobResult {
            from: serde_json::json!({"type": "kafka"}),
            to: "taos://localhost:6030".into(),
            parser: None,
        };
        let res = super::alloc_jobs(task_no_topics, &xnodes, None);
        assert!(matches!(res, Err(Error::SplitTopicsNotFound)));

        let task_empty_topics = SplitJobResult {
            from: serde_json::json!({"type": "kafka", "topics": []}),
            to: "taos://localhost:6030".into(),
            parser: None,
        };
        let res = super::alloc_jobs(task_empty_topics, &xnodes, None);
        assert!(matches!(res, Err(Error::TopicEmpty)));
    }
}
