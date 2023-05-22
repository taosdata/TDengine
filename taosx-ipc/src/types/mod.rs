use std::fmt::{Debug, Display};

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
            message: error.to_string(),
            context: None,
        }
    }
}

impl<T: Debug> std::error::Error for Fail<T> {}

/// Result OK or error with context
pub type Response<T, C = String> = std::result::Result<T, Fail<C>>;

pub enum RespAction {
    Heartbeat,
    TaskError(i64),
    ListOk(ListResponse),
}
