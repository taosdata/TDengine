use ha_core::types::TaskStatus;

#[derive(Debug, serde::Deserialize)]
pub struct TaskRecord {
    pub id: i64,
    pub xnode_id: Option<i32>,
    pub from: String,
    pub to: String,
    pub parser: Option<String>,
    pub status: Option<TaskStatus>,
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
}

#[derive(Debug, serde::Deserialize)]
pub struct XnodeId {
    pub id: i32,
    pub url: String,
}
