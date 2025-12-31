use std::str::FromStr;

use ha_core::types::{HaTask, SplitJobResult};
use snafu::{OptionExt, ResultExt};
use taos::Dsn;
use taosx_utils::dsn::json_to_dsn;

use crate::controller::xnodes::XNodes;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Dsn driver not found"))]
    DsnDriverNotFound,
    #[snafu(display("Dsn driver not string"))]
    DsnDriverNotStr,
    #[snafu(display("Dsn driver not object"))]
    FromDsnNotObject,
    #[snafu(display("Failed to convert json to dsn"))]
    JsonToDsn { source: anyhow::Error },
    #[snafu(display("Kafka split topics not found"))]
    KafkaSplitTopicsNotFound,
    #[snafu(display("Kafka split topics not array"))]
    KafkaSplitTopicNotArray,
    #[snafu(display("Kafka split topic invalid"))]
    KafkaSplitTopicInvalid { source: serde_json::Error },
    #[snafu(display("No xnode available"))]
    NoXnodeAvailable,
    #[snafu(display("Kafka topic not found"))]
    KafkaTopicNotFound,
    #[snafu(display("From dsn invalid type"))]
    FromDsnInvalidType,
    #[snafu(display("Invalid dsn"))]
    InvalidDsn { dsn: String, source: taos::DsnError },
    #[snafu(display("Invalid json dsn"))]
    InvalidJsonDsn { source: anyhow::Error },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum AllocatedJobs {
    Task(i32, HaTask),
    Jobs(Vec<(i32, HaTask)>),
}

/// Split a task into multiple jobs based on the number of xnodes.
pub fn alloc_jobs(mut task: SplitJobResult, xnodes: &XNodes) -> Result<AllocatedJobs> {
    let from = match &task.from {
        serde_json::Value::String(dsn) => Dsn::from_str(dsn).context(InvalidDsnSnafu { dsn })?,
        o @ serde_json::Value::Object(_) => json_to_dsn(o).context(InvalidJsonDsnSnafu)?,
        _ => return FromDsnInvalidTypeSnafu.fail(),
    };

    let mut jobs = Vec::with_capacity(xnodes.len());
    let res = match from.driver.as_str() {
        "kafka" => {
            #[derive(Debug, serde::Deserialize)]
            struct TopicPartition {
                name: String,
                partitions: usize,
            }
            let Some(from_map) = task.from.as_object_mut() else {
                return FromDsnNotObjectSnafu.fail();
            };
            let Some(topics) = from_map.remove("topics") else {
                return KafkaSplitTopicsNotFoundSnafu.fail();
            };
            let Some(topics) = topics.as_array() else {
                return KafkaSplitTopicNotArraySnafu.fail();
            };
            let mut topics = topics
                .iter()
                .map(|v| serde_json::from_value::<TopicPartition>(v.clone()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(KafkaSplitTopicInvalidSnafu)?;
            if topics.is_empty() {
                return KafkaTopicNotFoundSnafu.fail();
            }
            let total_concurrency: usize = topics.iter().map(|v| v.partitions).sum();
            tracing::info!(total_concurrency);
            let xnode_concurrency = xnodes.alloc_concurrency(total_concurrency);
            tracing::info!(?xnode_concurrency);
            let mut from = json_to_dsn(&task.from).context(JsonToDsnSnafu)?;
            let mut current_tp = topics.pop();
            for (id, mut concurrency) in xnode_concurrency {
                loop {
                    let Some(tp) = &mut current_tp else {
                        break;
                    };
                    if tp.partitions == 0 || concurrency == 0 {
                        break;
                    }
                    from.set("topics", tp.name.clone());
                    if concurrency >= tp.partitions {
                        from.set("read_concurrency", tp.partitions.to_string());
                        let job = HaTask {
                            from: from.to_string(),
                            to: task.to.clone(),
                            parser: task.parser.clone(),
                        };
                        jobs.push((id, job));
                        concurrency -= tp.partitions;
                        current_tp = topics.pop();
                    } else {
                        from.set("read_concurrency", concurrency.to_string());
                        let job = HaTask {
                            from: from.to_string(),
                            to: task.to.clone(),
                            parser: task.parser.clone(),
                        };
                        jobs.push((id, job));
                        tp.partitions -= concurrency;
                        break;
                    }
                }
            }
            if jobs.len() == 1
                && let Some((id, job)) = jobs.pop()
            {
                AllocatedJobs::Task(id, job)
            } else {
                AllocatedJobs::Jobs(jobs)
            }
        }
        _ => {
            let xnode = xnodes.best_xnode().context(NoXnodeAvailableSnafu)?;
            let job = HaTask {
                from: task.from.to_string(),
                to: task.to,
                parser: task.parser,
            };
            AllocatedJobs::Task(xnode, job)
        }
    };

    Ok(res)
}
