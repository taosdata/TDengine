use std::collections::HashMap;

use arrow::array::{RecordBatch, timezone::Tz};
use chrono::{DateTime, Utc};
use taos::{Dsn, DsnError};

use crate::batch::build_batch;

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

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct HaTask {
    pub from: String,
    pub to: String,
    pub parser: Option<serde_json::Value>,
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

    pub job_id: i64,

    /// The stream data source.
    pub from: String,

    /// The target of the stream.
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

pub type AddAgentsParam = Vec<String>;

pub type AddAgentsResult = HashMap<String, String>;

pub type DelAgentsParam = Vec<i64>;

pub type DelAgentsResult = Vec<DelAgentErrorStatus>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DelAgentErrorStatus {
    pub id: i64,
    pub error: String,
}

pub type ListAgentsResult = Vec<ListAgentStatesParam>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ListAgentStatesParam {
    pub id: i64,
    pub state: AgentState,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Idle,
    Wait,
    Connected,
    Disconnected,
    Closed,
}

pub type ListTaskJobStatesResult = Vec<ListTaskJobStatesParam>;

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct ListTaskJobStatesParam {
    pub task_id: i64,
    pub job_id: i64,
    pub state: TaskStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Created,
    Queued,
    Running,
    Stopping,
    Stopped,
    Completed,
    Failed,
}

impl TaskStatus {
    pub fn is_stopped(&self) -> bool {
        matches!(
            self,
            TaskStatus::Stopped | TaskStatus::Completed | TaskStatus::Failed
        )
    }

    pub fn is_running(&self) -> bool {
        matches!(self, TaskStatus::Queued | TaskStatus::Running)
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TaskStatus::Created => "created",
            TaskStatus::Queued => "queued",
            TaskStatus::Running => "running",
            TaskStatus::Stopping => "stopping",
            TaskStatus::Stopped => "stopped",
            TaskStatus::Completed => "completed",
            TaskStatus::Failed => "failed",
        };
        write!(f, "{s}")
    }
}

impl std::convert::From<&TaskStatus> for TaskStatus {
    fn from(value: &TaskStatus) -> Self {
        *value
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Connected,
    Waiting,
    Transferring,
    Disconnected,
}

impl std::fmt::Display for AgentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            AgentStatus::Connected => "connected",
            AgentStatus::Waiting => "waiting",
            AgentStatus::Transferring => "transferring",
            AgentStatus::Disconnected => "disconnected",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Initial,
    Ready,
    Idle,
    Active,
    Pending,
    Busy,
    Bounce,
    SourceError,
    SinkError,
    Fatal,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Initial => write!(f, "initial"),
            HealthStatus::Ready => write!(f, "ready"),
            HealthStatus::Idle => write!(f, "idle"),
            HealthStatus::Active => write!(f, "active"),
            HealthStatus::Pending => write!(f, "pending"),
            HealthStatus::Busy => write!(f, "busy"),
            HealthStatus::Bounce => write!(f, "bounce"),
            HealthStatus::SourceError => write!(f, "source_error"),
            HealthStatus::SinkError => write!(f, "sink_error"),
            HealthStatus::Fatal => write!(f, "fatal"),
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum ActivityStatus {
    Task(TaskStatus),
    Agent(AgentStatus),
    Health(HealthStatus),
}

impl std::fmt::Display for ActivityStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityStatus::Task(status) => write!(f, "{}", status),
            ActivityStatus::Agent(status) => write!(f, "{}", status),
            ActivityStatus::Health(status) => write!(f, "{}", status),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl std::fmt::Display for ActivityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityLevel::Error => write!(f, "error"),
            ActivityLevel::Warn => write!(f, "warn"),
            ActivityLevel::Info => write!(f, "info"),
            ActivityLevel::Debug => write!(f, "debug"),
            ActivityLevel::Trace => write!(f, "trace"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Activity {
    pub agent_id: i64,
    pub task_id: i64,
    pub job_id: i64,
    pub at: chrono::DateTime<Utc>,
    pub level: ActivityLevel,
    pub activity: String,
    pub status: Option<ActivityStatus>,
    pub context: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TaskMetrics {
    pub ts: chrono::DateTime<Utc>,
    pub task_id: i64,
    pub job_id: i64,
    pub metrics: serde_json::Value,
}

pub type GetSamplesFrom = String;

pub type GetSamplesResult = Vec<serde_json::Value>;
