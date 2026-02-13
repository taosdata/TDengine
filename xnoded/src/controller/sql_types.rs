use ha_core::{activity::TaskStatus, types::HaTask};

#[derive(Debug, serde::Deserialize)]
pub struct TaskRecord {
    pub id: i64,
    pub xnode_id: Option<i32>,
    pub from: String,
    pub to: String,
    pub parser: Option<String>,
    pub status: Option<TaskStatus>,
    pub via: Option<i64>,
    pub labels: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct JobId {
    pub id: i64,
    pub task_id: i64,
    pub xnode_id: i32,
}

#[derive(Debug, serde::Deserialize)]
pub struct JobRecord {
    pub id: i64,
    pub task_id: i64,
    pub xnode_id: i32,
    pub config: String,
    pub status: Option<TaskStatus>,
    pub via: Option<i64>,
}

impl TryFrom<JobRecord> for HaTask {
    type Error = serde_json::Error;

    fn try_from(value: JobRecord) -> Result<Self, Self::Error> {
        let mut config: Self = serde_json::from_str(&value.config)?;
        config.via = value.via;
        Ok(config)
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct XnodeId {
    pub id: i32,
    pub url: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct AgentRecord {
    pub id: i64,
    pub token: String,
}
