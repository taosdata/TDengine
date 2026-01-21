use std::collections::HashMap;

use arrow::array::{RecordBatch, timezone::Tz};
use chrono::{DateTime, Utc};
use taos::{Dsn, DsnError};

use crate::{
    activity::{AgentStatus, TaskStatus},
    batch::build_batch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcClientType {
    Xnoded,
    Guest,
    Agent,
}

impl RpcClientType {
    pub fn as_str(&self) -> &'static str {
        match self {
            RpcClientType::Xnoded => "xnoded",
            RpcClientType::Guest => "guest",
            RpcClientType::Agent => "agent",
        }
    }
}

impl From<&str> for RpcClientType {
    fn from(value: &str) -> Self {
        match value {
            "xnoded" => RpcClientType::Xnoded,
            "agent" => RpcClientType::Agent,
            _ => RpcClientType::Guest,
        }
    }
}

impl std::fmt::Display for RpcClientType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcClientType::Xnoded => write!(f, "xnoded"),
            RpcClientType::Guest => write!(f, "guest"),
            RpcClientType::Agent => write!(f, "agent"),
        }
    }
}

pub struct RpcRecord<'a> {
    pub ts: DateTime<Tz>,
    pub action: &'a str,
    pub context: &'a str,
    pub req_id: u64,
}

impl<'a> TryFrom<RpcRecord<'a>> for RecordBatch {
    type Error = arrow::error::ArrowError;
    fn try_from(record: RpcRecord<'a>) -> Result<RecordBatch, Self::Error> {
        build_batch(record.action, record.context, record.req_id)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response<T = ()> {
    Data(T),
    Fail(String),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TaskJobId {
    pub task_id: i64,
    pub job_id: i64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct XnodedId {
    pub cluster_id: String,
    pub leader_ep: String,
}

impl std::fmt::Display for XnodedId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cluster_id={}, leader_ep={}",
            self.cluster_id, self.leader_ep
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize, serde::Serialize)]
pub struct HaTask {
    pub from: String,
    pub to: String,
    pub parser: Option<serde_json::Value>,
    pub via: Option<i64>,
}

#[derive(Debug)]
pub struct SplitJobTask {
    pub from: Dsn,
    pub to: Dsn,
    pub parser: Option<serde_json::Value>,
}

impl TryFrom<HaTask> for SplitJobTask {
    type Error = DsnError;
    fn try_from(task: HaTask) -> Result<Self, Self::Error> {
        let from: Dsn = task.from.parse()?;
        let to: Dsn = task.to.parse()?;
        Ok(Self {
            from,
            to,
            parser: task.parser,
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SplitJobResult {
    pub from: serde_json::Value,
    pub to: String,
    pub parser: Option<serde_json::Value>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CheckValidParam {
    pub from: String,
    pub to: String,
    pub via: Option<i64>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct GetSamplesParam {
    pub from: String,
    pub via: Option<i64>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TaskPreviewParam {
    pub from: String,
    pub parser: serde_json::Value,
    #[serde(flatten)]
    pub input: Samples,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Samples {
    Input(Vec<HashMap<String, serde_json::Value>>),
    Samples(Vec<serde_json::Value>),
}

/// A streaming workflow task description.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StartTaskJobParam {
    /// Unique id for the task item.
    pub task_id: i64,

    /// Unique id for the job item.
    pub job_id: i64,

    /// The source of the task data stream.
    pub from: String,

    /// The target of the task data stream.
    pub to: String,

    /// The parser of the task stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parser: Option<serde_json::Value>,

    /// Agent Id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via: Option<i64>,
}

pub type StopTaskJobParam = TaskJobId;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatMetrics {
    pub cpu_cores: usize,
    pub cpu_usage: f32,
    pub memory: u64,
    pub used_memory: u64,
    pub free_memory: u64,
}

pub type AddAgentsParam<'a> = &'a [String];

pub type DelAgentsParam<'a> = &'a [i64];

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DelAgentErrorStatus {
    pub id: i64,
    pub error: String,
}

pub type ListAgentsResult = Vec<ListAgentStatusResult>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ListAgentStatusResult {
    pub id: i64,
    pub status: AgentStatus,
}

pub type ListTaskJobStatesResult = Vec<ListTaskJobStatesParam>;

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct ListTaskJobStatesParam {
    pub task_id: i64,
    pub job_id: i64,
    pub state: TaskStatus,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TaskMetrics {
    pub ts: chrono::DateTime<Utc>,
    pub task_id: i64,
    pub job_id: i64,
    pub r#type: MetricsType,
    pub metrics: serde_json::Value,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum MetricsType {
    Ipc,
    Tmq,
    Legacy,
}

impl std::fmt::Display for MetricsType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricsType::Ipc => write!(f, "ipc"),
            MetricsType::Tmq => write!(f, "tmq"),
            MetricsType::Legacy => write!(f, "legacy"),
        }
    }
}

impl MetricsType {
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s {
            "ipc" => Some(MetricsType::Ipc),
            "tmq" => Some(MetricsType::Tmq),
            "legacy" => Some(MetricsType::Legacy),
            _ => None,
        }
    }
}

pub type GetSamplesFrom = String;

pub type GetSamplesResult = serde_json::Value;
