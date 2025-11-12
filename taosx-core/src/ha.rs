use serde_json as json;
use taos::Dsn;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct HaTask {
    pub task_id: i64,
    pub job_id: Option<i64>,
    pub from: String,
    pub to: String,
    pub parser: json::Value,
}

#[derive(Debug)]
pub struct SplitJobTask {
    pub from: Dsn,
    pub to: Dsn,
    pub parser: json::Value,
}

impl SplitJobTask {
    pub fn new(from: Dsn, to: Dsn, parser: json::Value) -> Self {
        Self { from, to, parser }
    }
}
