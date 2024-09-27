use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use crate::types::dsv::DataSourceValidation;
use chrono::{DateTime, Utc};
use faststr::FastStr;
use serde::{Deserialize, Serialize};
use taosx_metrics::MetricsEvents;
pub mod dsv;

#[derive(Serialize, Deserialize, Clone, Debug, Hash, PartialEq, Eq)]
pub struct DataSetsReq {
    pub from: String,
    pub via: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    pub categories: Vec<String>,
    pub offset: usize,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lang: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DataSet {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<OptionSet>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

impl PartialEq for DataSet {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl DataSet {
    pub fn new(id: impl Into<String>) -> Self {
        DataSet {
            id: id.into(),
            name: None,
            category: None,
            r#type: None,
            options: None,
            format: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct OptionSet {
    pub name: String,
    pub display: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub required: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ListResponse {
    pub req_id: u64,
    pub req: DataSetsReq,
    pub res: Response<Vec<DataSet>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckResponse {
    pub req_id: u64,
    pub req: String,
    pub res: DataSourceValidation,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SampleResponse {
    pub req_id: u64,
    pub req: String,
    pub res: Response<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PutFileReq {
    /// 要发送的文件，相对于 data 目录的路径, 例如: "tasks/1/1.csv"
    pub path: String,
    pub data: Vec<u8>,
    /// 写文件到 agent 时，是否自动解压, 默认 false,目前只支持 gzip
    /// 如果为 true，path 必须以 .gz 结尾, 解压后的文件名为 path 去掉 .gz
    pub decompress: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PutFileResp {
    pub req_id: u64,
    pub path: String,
    pub res: Response<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryDataSourceReq {
    /// from DSN
    pub from: String,
    /// 启动参数
    pub args: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryDataSourceResp {
    pub req_id: u64,
    pub output: Response<String>,
}

/// Task endpoint error responses
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Fail<T> {
    /// Error code
    pub code: i64,
    /// Error message
    pub message: String,
    /// Error context
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<T>,
}

impl<T: Debug> Display for Fail<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Error: [{}] {}", self.code, self.message))?;
        if let Some(context) = self.context.as_ref() {
            f.write_fmt(format_args!("\n\nWith context:\n{:?}", context))?;
        }
        Ok(())
    }
}

impl<T> Fail<T> {
    pub fn new(error: impl Display) -> Self {
        Self {
            code: 0xFFFF,
            message: format!("{error:#}"),
            context: None,
        }
    }
}

impl<T: Debug> std::error::Error for Fail<T> {}

/// Result OK or error with context
pub type Response<T, C = String> = std::result::Result<T, Fail<C>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub req: chrono::DateTime<Utc>,
    pub res: chrono::DateTime<Utc>,
}

impl HeartbeatResponse {
    pub fn duration(&self) -> chrono::Duration {
        self.res - self.req
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Activity {
    pub id: i64,
    pub at: chrono::DateTime<Utc>,
    pub level: LevelFilter,
    pub activity: String,
    pub status: String,
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum LevelFilter {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}
impl Activity {
    pub fn new<T: ToString>(
        id: i64,
        at: DateTime<Utc>,
        level: LevelFilter,
        activity: impl Into<String>,
        status: impl Into<String>,
        context: impl Into<Option<T>>,
    ) -> Self {
        Activity {
            id,
            at,
            level,
            activity: activity.into(),
            status: status.into(),
            context: context.into().map(|v| {
                let v = v.to_string();
                serde_json::Value::from_str(v.as_str())
                    .unwrap_or_else(|_| serde_json::Value::String(v))
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[repr(C)]
pub enum TaskMetricsVariant {
    Set = 0,
    Inc,
    Dec,
}
pub type TaskMetricItem = (i64, FastStr, TaskMetricsVariant, u64);
pub type TaskMetrics = Vec<TaskMetricItem>;
pub enum RespAction {
    Heartbeat,
    HeartbeatOk(HeartbeatResponse),
    TaskError(i64),
    /// ReqId, Resp
    ListOk(ListResponse),
    CheckOk(CheckResponse),
    SampleOk(SampleResponse),
    PutFileOk(PutFileResp),
    AgentActivity(Activity),
    TaskActivity(Activity),
    TaskMetrics(TaskMetrics),
    Metrics(MetricsEvents),
    QueryDataSourceOk(QueryDataSourceResp),
}
