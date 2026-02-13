mod source_db;
mod source_kafka;
mod source_mqtt;
mod tmq2td;
mod utils;

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
    #[snafu(display("From dsn not string"))]
    FromDsnNotString,
    #[snafu(display("Failed to convert json to dsn"))]
    JsonToDsn { source: anyhow::Error },
    #[snafu(display("Split topics not found"))]
    SplitTopicsNotFound,
    #[snafu(display("Split topics not array"))]
    SplitTopicNotArray,
    #[snafu(display("Split topic invalid"))]
    SplitTopicInvalid { source: serde_json::Error },
    #[snafu(display("No xnode available"))]
    NoXnodeAvailable,
    #[snafu(display("Topic empty"))]
    TopicEmpty,
    #[snafu(display("From dsn invalid type"))]
    FromDsnInvalidType,
    #[snafu(display("Invalid dsn"))]
    InvalidDsn { dsn: String, source: taos::DsnError },
    #[snafu(display("Invalid json dsn"))]
    InvalidJsonDsn { source: anyhow::Error },
    #[snafu(display("Start timestamp not found"))]
    StartTimestampNotFound,
    #[snafu(display("Invalid timestamp {ts}"))]
    InvalidTimestamp {
        ts: String,
        source: chrono::ParseError,
    },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum AllocatedJobs {
    Task(i32, HaTask),
    Jobs(Vec<(i32, HaTask)>),
}

/// Split a task into multiple jobs based on the number of xnodes.
pub fn alloc_jobs(
    task: SplitJobResult,
    xnodes: &XNodes,
    via: Option<i64>,
) -> Result<AllocatedJobs> {
    let from = match &task.from {
        serde_json::Value::String(dsn) => Dsn::from_str(dsn).context(InvalidDsnSnafu { dsn })?,
        o @ serde_json::Value::Object(_) => json_to_dsn(o).context(InvalidJsonDsnSnafu)?,
        _ => return FromDsnInvalidTypeSnafu.fail(),
    };
    let task_to = task.to.clone();
    let to = Dsn::from_str(&task_to).context(InvalidDsnSnafu { dsn: &task_to })?;

    let res = match (from.driver.as_str(), to.driver.as_str()) {
        ("kafka", _) => source_kafka::alloc_jobs(task, xnodes, via)?,
        ("mqtt", _) => source_mqtt::alloc_jobs(task, xnodes, via)?,
        ("tmq" | "sync", "taos") => tmq2td::alloc_jobs(task, xnodes, via)?,
        ("mysql" | "postgres" | "oracle" | "mssql" | "mongodb", _) | ("taos", "taos") => {
            source_db::alloc_jobs(task, xnodes, via)?
        }
        _ => {
            let xnode = xnodes.best_xnode(via).context(NoXnodeAvailableSnafu)?;
            let job = HaTask {
                from: task.from.to_string(),
                to: task_to,
                parser: task.parser,
                via: None,
                labels: None,
            };
            AllocatedJobs::Task(xnode, job)
        }
    };

    Ok(res)
}
