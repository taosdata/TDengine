use std::str::FromStr;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use ha_core::{
    activity::{AgentStatus, TaskStatus},
    types::{CheckValidParam, HaTask},
};
use taos::Dsn;
use taosx_utils::dsn::{dsn_to_json, json_to_dsn};

macro_rules! extract_from {
    ($value: expr) => {
        match ($value.from, $value.from_json) {
            (Some(from), _) if !from.is_empty() => from,
            (_, Some(from_json)) if from_json.as_object().is_some_and(|v| !v.is_empty()) => {
                json_to_dsn(&from_json)?.to_string()
            }
            _ => bail!("from or from_json is required"),
        }
    };
}

#[derive(Debug, serde::Deserialize)]
pub struct TaskRecord {
    pub id: i64,
    pub name: String,
    pub from: String,
    pub to: String,
    pub parser: Option<String>,
    pub via: Option<i64>,
    pub status: Option<TaskStatus>,
    pub create_time: DateTime<Utc>,
    pub xnode_id: Option<i32>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ExpandDsn {
    subject: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct GetTaskResult {
    id: i64,
    name: String,
    from_json: serde_json::Value,
    parser: Option<serde_json::Value>,
    to: String,
    via: Option<i64>,
    status: TaskStatus,
    created_at: String,
    to_expand: Option<ExpandDsn>,
}

impl TryFrom<TaskRecord> for GetTaskResult {
    type Error = anyhow::Error;

    fn try_from(v: TaskRecord) -> anyhow::Result<Self> {
        let from_dsn = Dsn::from_str(&v.from).context("invalid `from` task param")?;
        let to_dsn = Dsn::from_str(&v.to).context("invalid `to` task param")?;
        let parser = v
            .parser
            .map(|v| serde_json::from_str(&v))
            .transpose()
            .context("invalid `parser` task param")?;
        Ok(GetTaskResult {
            id: v.id,
            name: v.name,
            from_json: dsn_to_json(&from_dsn),
            parser,
            to: v.to,
            via: v.via,
            status: v.status.unwrap_or(TaskStatus::Created),
            created_at: v.create_time.to_rfc3339(),
            to_expand: Some(ExpandDsn {
                subject: to_dsn.subject,
            }),
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Task {
    pub name: String,
    from: Option<String>,
    from_json: Option<serde_json::Value>,
    to: String,
    parser: Option<serde_json::Value>,
    via: Option<i64>,
}

impl TryFrom<Task> for HaTask {
    type Error = anyhow::Error;

    fn try_from(task: Task) -> Result<Self, Self::Error> {
        let from = extract_from!(task);
        let to = task.to;
        let parser = task.parser;
        let via = task.via;
        Ok(HaTask {
            from,
            to,
            parser,
            via,
        })
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct ExportTaskParam {
    ids: String,
}

impl ExportTaskParam {
    pub fn ids(&self) -> anyhow::Result<Vec<i64>> {
        self.ids
            .split(',')
            .map(|id| id.parse().context("task id invalid"))
            .collect()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ExportTaskResult {
    pub tasks_num: usize,
    pub export_time: String,
    pub tasks: Vec<ExportedTask>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ExportedTask {
    id: i64,
    name: String,
    from: serde_json::Value,
    to: String,
    parser: Option<serde_json::Value>,
    via: Option<i64>,
    created_at: DateTime<Utc>,
}

impl From<ExportedTask> for Task {
    fn from(value: ExportedTask) -> Self {
        Self {
            name: value.name,
            from: None,
            from_json: Some(value.from),
            to: value.to,
            parser: value.parser,
            via: value.via,
        }
    }
}

impl TryFrom<TaskRecord> for ExportedTask {
    type Error = anyhow::Error;
    fn try_from(task: TaskRecord) -> Result<Self, Self::Error> {
        let from = {
            let dsn = Dsn::from_str(&task.from).context("param `from` not valid dsn")?;
            dsn_to_json(&dsn)
        };
        let parser = task
            .parser
            .map(|v| serde_json::from_str(&v))
            .transpose()
            .context("param `parser` not valid json")?;
        Ok(Self {
            id: task.id,
            name: task.name,
            from,
            to: task.to,
            parser,
            via: task.via,
            created_at: task.create_time,
        })
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct Xnode {
    pub id: i32,
    pub url: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct ApiCheckValidParam {
    pub from: Option<String>,
    pub from_json: Option<serde_json::Value>,
    pub to: String,
    pub via: Option<i64>,
}

impl TryFrom<ApiCheckValidParam> for CheckValidParam {
    type Error = anyhow::Error;

    fn try_from(value: ApiCheckValidParam) -> Result<Self, Self::Error> {
        let from = extract_from!(value);
        let to = value.to;
        let via = value.via;
        Ok(Self { from, to, via })
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct ApiGetSampleParam {
    pub dsn: serde_json::Value,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AgentRecord {
    pub id: i64,
    pub name: String,
    pub token: String,
    pub status: Option<AgentStatus>,
    #[serde(rename(serialize = "created_at"))]
    pub create_time: DateTime<Utc>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ActivityLog {
    id: i64,
    at: i64,
    level: String,
    status: String,
    activity: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct WsId {
    pub cluster_id: String,
    pub token: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct TaskWsId {
    pub task_id: i64,
    pub token: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct Cluster {
    pub id: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct JobRecord {
    pub via: Option<i64>,
    pub status: Option<TaskStatus>,
}
