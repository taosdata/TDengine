use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
// use taos::Code;

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
    pub req: DataSetsReq,
    pub res: Response<Vec<DataSet>>,
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
pub enum RespAction {
    Heartbeat,
    HeartbeatOk(HeartbeatResponse),
    TaskError(i64),
    ListOk(ListResponse),
    AgentActivity(Activity),
    TaskActivity(Activity),
}
