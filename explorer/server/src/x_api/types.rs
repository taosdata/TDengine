use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use ha_core::{
    activity::{AgentStatus, TaskStatus},
    types::{CheckValidParam, HaTask},
};
use taos::Dsn;
use taosx_utils::{
    dsn::{dsn_to_json, json_to_dsn},
    labels::build_json_labels_from_iter,
};

macro_rules! extract_from {
    ($value: expr) => {
        match ($value.from.as_ref(), $value.from_json.as_ref()) {
            (Some(from), _) if !from.is_empty() => from.clone(),
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
    pub labels: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct GetTaskResult {
    id: i64,
    name: String,
    from: String,
    from_json: serde_json::Value,
    parser: Option<serde_json::Value>,
    to: String,
    via: Option<i64>,
    status: TaskStatus,
    created_at: String,
    trigger: Option<serde_json::Value>,
    from_expand: Option<ExpandedDsn>,
    to_expand: Option<ExpandedDsn>,
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
        let trigger = v
            .labels
            .map(|v| serde_json::from_str::<serde_json::Value>(&v))
            .transpose()
            .context("invalid `labels` task param")?
            .and_then(|v| v.as_object().and_then(|v| v.get("trigger").cloned()));
        Ok(GetTaskResult {
            id: v.id,
            name: v.name,
            from: v.from,
            from_json: dsn_to_json(&from_dsn),
            parser,
            to: v.to,
            via: v.via,
            status: v.status.unwrap_or(TaskStatus::Created),
            created_at: v.create_time.to_rfc3339(),
            trigger,
            from_expand: Some(from_dsn.into()),
            to_expand: Some(to_dsn.into()),
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
    labels: Vec<String>,
    trigger: Option<serde_json::Value>,
}

impl Task {
    pub fn extract_from_to(&self) -> anyhow::Result<(Dsn, Dsn)> {
        let from_dsn = Dsn::from_str(&extract_from!(self)).context("invalid `from` dsn")?;
        let to_dsn = Dsn::from_str(&self.to).context("invalid `to` dsn")?;
        Ok((from_dsn, to_dsn))
    }
}

impl TryFrom<Task> for HaTask {
    type Error = anyhow::Error;

    fn try_from(task: Task) -> Result<Self, Self::Error> {
        let from = extract_from!(task);
        let to = task.to;
        let parser = task.parser;
        let via = task.via;
        let mut labels = build_json_labels_from_iter(&task.labels);
        if let (Some(labels), Some(trigger)) = (labels.as_object_mut(), task.trigger) {
            labels.insert("trigger".into(), trigger);
        }
        Ok(HaTask {
            from,
            to,
            parser,
            via,
            labels: Some(labels),
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
    trigger: Option<serde_json::Value>,
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
            labels: vec![],
            trigger: value.trigger,
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
        let mut labels = task
            .labels
            .map(|v| serde_json::from_str::<HashMap<String, serde_json::Value>>(&v))
            .transpose()
            .context("param `labels` no valid json")?
            .unwrap_or_default();
        Ok(Self {
            id: task.id,
            name: task.name,
            from,
            to: task.to,
            parser,
            via: task.via,
            created_at: task.create_time,
            trigger: labels.remove("trigger"),
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
pub struct TaskWsId {
    pub task_id: i64,
    pub token: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct JobRecord {
    pub via: Option<i64>,
    pub status: Option<TaskStatus>,
}

#[derive(Debug, serde::Deserialize)]
pub struct GetTaskParam {
    pub labels: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ExpandedDsn {
    pub id: String,
    pub protocol: Option<String>,
    pub path: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub subject: Option<String>,
    pub params: HashMap<String, Option<String>>,
}

impl From<Dsn> for ExpandedDsn {
    fn from(value: Dsn) -> Self {
        let (host, port) = match value.addresses.into_iter().next() {
            Some(addr) => (addr.host, addr.port),
            None => (None, None),
        };
        Self {
            id: value.driver,
            protocol: value.protocol,
            path: value.path,
            host,
            port,
            username: value.username,
            password: value.password,
            subject: value.subject,
            params: value
                .params
                .into_iter()
                .map(|(k, v)| (k, if v.is_empty() { None } else { Some(v) }))
                .collect(),
        }
    }
}
