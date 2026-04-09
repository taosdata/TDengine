//! # TaosX API Integration Tests
//!
//! This module contains comprehensive integration tests for the TaosX REST API.
//!
//! ## Test Coverage
//!
//! ### Basic Tests (`test_taosx_api`)
//! 1. **Health Check** - Verifies API health endpoint
//! 2. **API Reachability** - Confirms API is accessible
//! 3. **Swagger Endpoint** - Tests OpenAPI documentation endpoint
//! 4. **List Tasks (Empty)** - Lists tasks when none exist
//! 5. **Create Task (Invalid)** - Tests validation with invalid data source
//! 6. **Create Task (Valid)** - Creates a task with valid parameters
//! 7. **Get Task by ID** - Retrieves specific task details
//! 8. **List Tasks (With Data)** - Lists tasks when tasks exist
//! 9. **Update Task** - Updates task properties
//! 10. **Start Task** - Initiates task execution
//! 11. **Stop Task** - Stops running task
//! 12. **Delete Task** - Removes task
//! 13. **Verify Task Deleted** - Confirms task no longer exists
//!
//! ## Running Tests
//!
//! Run all API tests:
//! ```bash
//! cargo test -p taosx-integration-tests test_taosx_api
//! ```
//!
//! ## API Endpoints Tested
//!
//! - `GET /health` - Health check
//! - `GET /swagger.json` - OpenAPI specification
//! - `GET /tasks` - List all tasks
//! - `POST /tasks` - Create new task
//! - `GET /tasks/{id}` - Get specific task
//! - `PATCH /tasks/{id}` - Update task
//! - `DELETE /tasks/{id}` - Delete task
//! - `POST /tasks/{id}/start` - Start task
//! - `POST /tasks/{id}/stop` - Stop task

#[derive(serde::Serialize)]
pub struct NewTask {
    pub name: String,
    pub from: String,
    pub to: String,
    pub parser: Option<serde_json::Value>,
    pub via: Option<i64>,
    pub labels: Option<Vec<String>>,
}

#[derive(serde::Deserialize, Debug, Clone)]
#[allow(dead_code)]
pub struct Task {
    pub id: u32,
    pub name: String,
    pub from: String,
    pub to: String,
    pub parser: Option<taosx_core::Parser>,
    pub via: Option<u32>,
    pub status: ha_core::activity::TaskStatus,
}

#[derive(serde::Serialize)]
pub struct UpdateTask {
    pub name: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub parser: Option<serde_json::Value>,
    pub via: Option<u32>,
}

#[derive(serde::Deserialize, Debug)]
#[allow(dead_code)]
pub struct ErrorResponse {
    pub code: u16,
    pub message: String,
}

pub type HealthResponse = String;

#[derive(serde::Deserialize, Debug)]
pub struct TaskListResponse(pub Vec<Task>);

#[derive(serde::Serialize)]
pub struct AgentProps {
    pub name: String,
    pub dsn: String,
    pub cluster_id: String,
    pub user_id: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Agent {
    pub id: i64,
    pub name: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct AgentWithToken {
    pub id: i64,
    pub name: String,
    pub token: String,
}

#[derive(serde::Serialize)]
pub struct AgentUpdates {
    pub cluster_id: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
pub struct ProfileResponse {
    pub version: String,
    pub commit: String,
}

#[derive(serde::Serialize)]
pub struct TaskBatchReq {
    pub ids: Vec<u32>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct DsnAgentQuery {
    pub dsn: serde_json::Value,
    pub to: Option<String>,
    pub via: Option<i64>,
    pub timeout: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct DsnAgentQueryV2 {
    pub from: Option<String>,
    pub from_json: Option<serde_json::Value>,
    pub to: Option<String>,
    pub via: Option<i64>,
    pub timeout: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct DataSourcesReq {
    pub from: Option<String>,
    pub from_json: Option<serde_json::Value>,
    pub via: Option<i64>,
    pub pattern: Option<String>,
    pub categories: Vec<String>,
    pub offset: usize,
    pub limit: usize,
    pub lang: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct DownloadParams {
    pub file_path: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct DownloadAllPointsParams {
    pub from: Option<String>,
    pub from_json: Option<serde_json::Value>,
    pub via: Option<i64>,
    pub categories: String,
    pub lang: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct TaskTicket {
    pub code: Option<i32>,
    pub ticket: String,
    pub complete: Option<bool>,
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

pub mod client;
pub use client::{ApiCheckValidParamClient, ApiClient};

#[cfg(test)]
mod basic;

#[cfg(test)]
mod upload_download;
