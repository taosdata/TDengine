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
//! ### Extended Tests (`test_taosx_api_extended`)
//! 1. **Profile Endpoint** - Gets system version and build info
//! 2. **Metrics Endpoint** - Retrieves Prometheus-style metrics
//! 3. **Metrics Description (English)** - Gets metric descriptions in English
//! 4. **Metrics Description (Chinese)** - Gets metric descriptions in Chinese
//! 5. **Task Count** - Gets total count of tasks
//! 6. **Get Non-existent Task** - Error handling for missing task
//! 7. **Delete Non-existent Task** - Error handling for deletion
//! 8. **Create Agent** - Creates a new agent with cluster ID
//! 9. **List Agents** - Lists all agents
//! 10. **List Agents with Filter** - Filters agents by cluster ID
//! 11. **Get Agent by ID** - Retrieves specific agent
//! 12. **Update Agent** - Updates agent cluster ID
//! 13. **Delete Agent** - Removes agent
//! 14. **Verify Agent Deleted** - Confirms agent deletion
//! 15. **Data Source Validation** - Tests DSN validation for various protocols
//! 16. **Create Multiple Tasks** - Creates tasks for batch operations
//! 17. **Batch Start Tasks** - Starts multiple tasks at once
//! 18. **Batch Stop Tasks** - Stops multiple tasks at once
//! 19. **Batch Delete Tasks** - Deletes multiple tasks at once
//! 20. **Verify Batch Deleted** - Confirms all batch deletions
//! 21. **Update Non-existent Task** - Error handling for updates
//! 22. **Start Non-existent Task** - Error handling for start operation
//! 23. **Stop Non-existent Task** - Error handling for stop operation
//! 24. **Create Task with Empty Name** - Tests validation for empty names
//! 25. **Create Task with Long Name** - Tests handling of very long names
//! 26. **Create Task with Special Characters** - Tests special character support
//! 27. **Multiple Rapid Requests** - Tests API under rapid concurrent requests
//! 28. **API Reachability After Operations** - Verifies API stability
//! 29. **Final Task Count** - Gets final state of task count
//! 30. **Final Task List** - Lists all remaining tasks
//!
//! ## Running Tests
//!
//! Run all API tests:
//! ```bash
//! cargo test -p taosx-integration-tests test_taosx_api
//! ```
//!
//! Run specific test:
//! ```bash
//! cargo test -p taosx-integration-tests test_taosx_api_extended -- --nocapture
//! ```
//!
//! ## API Endpoints Tested
//!
//! - `GET /health` - Health check
//! - `GET /swagger.json` - OpenAPI specification
//! - `GET /profile` - System version and build info
//! - `GET /metrics` - Prometheus metrics
//! - `GET /metrics/description` - Metric descriptions
//! - `GET /tasks` - List all tasks
//! - `GET /tasks/count` - Get task count
//! - `POST /tasks` - Create new task
//! - `GET /tasks/{id}` - Get specific task
//! - `PATCH /tasks/{id}` - Update task
//! - `DELETE /tasks/{id}` - Delete task
//! - `POST /tasks/{id}/start` - Start task
//! - `POST /tasks/{id}/stop` - Stop task
//! - `POST /tasks/start` - Batch start tasks
//! - `POST /tasks/stop` - Batch stop tasks
//! - `POST /tasks/delete` - Batch delete tasks
//! - `POST /agents` - Create agent
//! - `GET /agents` - List agents
//! - `GET /agents/{id}` - Get specific agent
//! - `PATCH /agents/{id}` - Update agent
//! - `DELETE /agents/{id}` - Delete agent
//! - `GET /datasources/valid` - Validate data source

pub struct ApiClient {
    pub url: url::Url,
    pub client: reqwest::blocking::Client,
}

#[derive(serde::Serialize)]
pub struct NewTask {
    pub name: String,
    pub from: String,
    pub to: String,
    pub parser: Option<serde_json::Value>,
    pub via: Option<u32>,
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
    pub cluster_id: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct AgentWithToken {
    pub id: i64,
    pub cluster_id: String,
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

impl ApiClient {
    pub fn new(url: &str) -> Self {
        ApiClient {
            url: url::Url::parse(url).unwrap(),
            client: reqwest::blocking::Client::builder()
                .user_agent("taosx-test-client")
                .build()
                .unwrap(),
        }
    }

    pub fn health(&self) -> Result<HealthResponse, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("health")?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let health = response.text()?;
            Ok(health)
        } else {
            Err(format!("Health check failed: {}", response.status()).into())
        }
    }

    pub fn swagger(&self) -> Result<String, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("api-doc/openapi.json")?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let swagger_json = response.text()?;
            Ok(swagger_json)
        } else {
            Err(format!("Failed to fetch swagger: {}", response.status()).into())
        }
    }

    #[allow(dead_code)]
    pub fn upload(&self, _data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        // Placeholder for upload implementation
        Ok(())
    }

    pub fn list_tasks(&self) -> Result<Vec<Task>, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("tasks")?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let list_response: TaskListResponse = response.json()?;
            Ok(list_response.0)
        } else {
            Err(format!("Failed to list tasks: {}", response.status()).into())
        }
    }

    pub fn get_task(&self, tid: u32) -> Result<Task, Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("tasks/{}", tid))?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let task: Task = response.json()?;
            Ok(task)
        } else if response.status().as_u16() == 404 {
            Err(format!("Task {} not found", tid).into())
        } else {
            Err(format!("Failed to get task: {}", response.status()).into())
        }
    }

    pub fn create_task(&self, task: &NewTask) -> Result<Task, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("tasks")?;
        println!(
            "Creating task at endpoint: {} with data {}",
            endpoint,
            serde_json::to_string(task).unwrap()
        );
        let response = self.client.post(endpoint).json(task).send()?;
        if response.status().is_success() {
            let created_task: Task = response.json()?;
            println!("Successfully created task with ID: {}", created_task.id);
            Ok(created_task)
        } else {
            println!("Response status: {}", response.status());
            let response = response.json::<ErrorResponse>()?;
            println!("Response message: {}", response.message);
            Err(format!("Failed to create task: {}", response.message).into())
        }
    }

    pub fn update_task(
        &self,
        tid: u32,
        task: &UpdateTask,
    ) -> Result<Task, Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("tasks/{}", tid))?;
        let response = self.client.patch(endpoint).json(task).send()?;
        if response.status().is_success() {
            let task: Task = response.json()?;
            Ok(task)
        } else if response.status().as_u16() == 404 {
            Err(format!("Task {} not found", tid).into())
        } else {
            Err(format!("Failed to update task: {}", response.text()?).into())
        }
    }

    pub fn delete_task(&self, tid: u32) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("tasks/{}", tid))?;
        let response = self.client.delete(endpoint).send()?;
        if response.status().is_success() || response.status().as_u16() == 204 {
            Ok(())
        } else if response.status().as_u16() == 404 {
            Err(format!("Task {} not found", tid).into())
        } else {
            Err(format!("Failed to delete task: {}", response.status()).into())
        }
    }

    pub fn start_task(&self, tid: u32) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("tasks/{}/start", tid))?;
        let response = self.client.post(endpoint).send()?;
        if response.status().is_success() {
            Ok(())
        } else if response.status().as_u16() == 404 {
            Err(format!("Task {} not found", tid).into())
        } else {
            Err(format!("Failed to start task: {}", response.status()).into())
        }
    }

    pub fn stop_task(&self, tid: u32) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("tasks/{}/stop", tid))?;
        let response = self.client.post(endpoint).send()?;
        if response.status().is_success() {
            Ok(())
        } else if response.status().as_u16() == 404 {
            Err(format!("Task {} not found", tid).into())
        } else {
            Err(format!("Failed to stop task: {}", response.status()).into())
        }
    }

    pub fn get_task_count(&self) -> Result<u32, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("tasks/count")?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let count: serde_json::Value = response.json()?;
            Ok(count.as_u64().unwrap_or(0) as u32)
        } else {
            Err(format!("Failed to get task count: {}", response.status()).into())
        }
    }

    pub fn batch_start_tasks(&self, ids: Vec<u32>) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.url.join("tasks/start")?;
        let body = TaskBatchReq { ids };
        let response = self.client.post(endpoint).json(&body).send()?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("Failed to batch start tasks: {}", response.status()).into())
        }
    }

    pub fn batch_stop_tasks(&self, ids: Vec<u32>) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.url.join("tasks/stop")?;
        let body = TaskBatchReq { ids };
        let response = self.client.post(endpoint).json(&body).send()?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("Failed to batch stop tasks: {}", response.status()).into())
        }
    }

    pub fn batch_delete_tasks(&self, ids: Vec<u32>) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.url.join("tasks/delete")?;
        let body = TaskBatchReq { ids };
        let response = self.client.post(endpoint).json(&body).send()?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("Failed to batch delete tasks: {}", response.status()).into())
        }
    }

    pub fn profile(&self) -> Result<ProfileResponse, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("profile")?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let profile: ProfileResponse = response.json()?;
            Ok(profile)
        } else {
            Err(format!("Failed to get profile: {}", response.status()).into())
        }
    }

    pub fn metrics(&self) -> Result<String, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("metrics")?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let metrics = response.text()?;
            Ok(metrics)
        } else {
            Err(format!("Failed to get metrics: {}", response.status()).into())
        }
    }

    pub fn metrics_description(
        &self,
        lang: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let endpoint = self
            .url
            .join(&format!("metrics/description?lang={}", lang))?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let desc: serde_json::Value = response.json()?;
            Ok(desc)
        } else {
            Err(format!("Failed to get metrics description: {}", response.status()).into())
        }
    }

    pub fn list_data_sources(
        &self,
        lang: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("ds/in")?;
        if let Some(lang) = lang {
            endpoint.query_pairs_mut().append_pair("lang", lang);
        }
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to list data sources: {}", response.status()).into())
        }
    }

    pub fn get_data_source(
        &self,
        name: &str,
        lang: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join(&format!("ds/in/{}", name))?;
        if let Some(lang) = lang {
            endpoint.query_pairs_mut().append_pair("lang", lang);
        }
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else if response.status().as_u16() == 404 {
            Err(format!("Data source {} not found", name).into())
        } else {
            Err(format!("Failed to get data source: {}", response.status()).into())
        }
    }

    pub fn validate_data_source(
        &self,
        query: &DsnAgentQuery,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("ds/in/validate")?;
        let response = self.client.get(endpoint).query(query).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to validate data source: {}", response.status()).into())
        }
    }

    pub fn validate_data_source_sink(
        &self,
        query: &DsnAgentQueryV2,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("ds/in/validate")?;
        let response = self.client.post(endpoint).json(query).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to validate sink data source: {}", response.status()).into())
        }
    }

    pub fn collect_data_sources(
        &self,
        req: &DataSourcesReq,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("ds/in/sets")?;
        let response = self.client.post(endpoint).json(req).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to collect data sources: {}", response.status()).into())
        }
    }

    pub fn get_sample_data(
        &self,
        query: &DsnAgentQuery,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("ds/in/sample")?;
        let response = self.client.post(endpoint).json(query).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to get sample data: {}", response.status()).into())
        }
    }

    pub fn get_sample_data_by_dsn(
        &self,
        dsn: &str,
        via: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("ds/in/sample")?;
        {
            let mut pairs = endpoint.query_pairs_mut();
            pairs.append_pair("dsn", dsn);
            if let Some(v) = via {
                pairs.append_pair("via", v);
            }
        }
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to get sample by dsn: {}", response.status()).into())
        }
    }

    pub fn upload_files(
        &self,
        req_id: &str,
        files: Vec<(&str, Vec<u8>)>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("upload")?;
        let mut form = reqwest::blocking::multipart::Form::new().text("req_id", req_id.to_string());
        for (name, data) in files {
            let part = reqwest::blocking::multipart::Part::bytes(data).file_name(name.to_string());
            form = form.part("file", part);
        }
        let response = self.client.post(endpoint).multipart(form).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to upload files: {}", response.status()).into())
        }
    }

    pub fn download_file(&self, file_path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("download")?;
        endpoint
            .query_pairs_mut()
            .append_pair("file_path", file_path);
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            Ok(response.bytes()?.to_vec())
        } else {
            Err(format!("Failed to download file: {}", response.status()).into())
        }
    }

    pub fn check_file_exists(&self, file_path: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("check_exists")?;
        endpoint
            .query_pairs_mut()
            .append_pair("file_path", file_path);
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let val: serde_json::Value = response.json()?;
            Ok(val.get("exists").and_then(|v| v.as_bool()).unwrap_or(false))
        } else {
            Err(format!("Failed to check file: {}", response.status()).into())
        }
    }

    pub fn start_point_download_task(
        &self,
        params: &DownloadAllPointsParams,
    ) -> Result<TaskTicket, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("ds/in/point/file/download/task")?;
        let response = self.client.post(endpoint).json(params).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to start point download task: {}", response.status()).into())
        }
    }

    pub fn point_task_ready(&self, ticket: &str) -> Result<TaskTicket, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("ds/in/point/file/are/you/ready")?;
        endpoint.query_pairs_mut().append_pair("ticket", ticket);
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to check point task: {}", response.status()).into())
        }
    }

    pub fn download_point_file(
        &self,
        ticket: &str,
        remain: bool,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("ds/in/point/file/async")?;
        {
            let mut pairs = endpoint.query_pairs_mut();
            pairs.append_pair("ticket", ticket);
            pairs.append_pair("remain", &remain.to_string());
        }
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            Ok(response.bytes()?.to_vec())
        } else {
            Err(format!("Failed to download point file: {}", response.status()).into())
        }
    }

    pub fn point_data_page(
        &self,
        ticket: &str,
        page: Option<usize>,
        page_size: Option<usize>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("ds/in/point/data/page")?;
        {
            let mut pairs = endpoint.query_pairs_mut();
            pairs.append_pair("ticket", ticket);
            if let Some(p) = page {
                pairs.append_pair("page", &p.to_string());
            }
            if let Some(ps) = page_size {
                pairs.append_pair("page_size", &ps.to_string());
            }
        }
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            Ok(response.json()?)
        } else {
            Err(format!("Failed to get point page: {}", response.status()).into())
        }
    }

    pub fn download_point_template(
        &self,
        driver: &str,
        lang: Option<&str>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("ds/in/point/file/template")?;
        {
            let mut pairs = endpoint.query_pairs_mut();
            pairs.append_pair("driver", driver);
            if let Some(lang) = lang {
                pairs.append_pair("lang", lang);
            }
        }
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            Ok(response.bytes()?.to_vec())
        } else {
            Err(format!("Failed to download point template: {}", response.status()).into())
        }
    }

    pub fn create_agent(
        &self,
        name: &str,
        dsn: &str,
        cluster_id: &str,
        user: &str,
    ) -> Result<AgentWithToken, Box<dyn std::error::Error>> {
        let endpoint = self.url.join("agents")?;
        let body = AgentProps {
            name: name.to_string(),
            dsn: dsn.to_string(),
            cluster_id: cluster_id.to_string(),
            user_id: Some(user.to_string()),
        };
        let response = self.client.post(endpoint).json(&body).send()?;
        if response.status().is_success() {
            let agent: AgentWithToken = response.json()?;
            Ok(agent)
        } else {
            Err(format!("Failed to create agent: {}", response.status()).into())
        }
    }

    pub fn list_agents(
        &self,
        cluster_id: Option<&str>,
    ) -> Result<Vec<Agent>, Box<dyn std::error::Error>> {
        let mut endpoint = self.url.join("agents")?;
        if let Some(cid) = cluster_id {
            endpoint.set_query(Some(&format!("cluster_id={}", cid)));
        }
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let agents: Vec<Agent> = response.json()?;
            Ok(agents)
        } else {
            Err(format!("Failed to list agents: {}", response.status()).into())
        }
    }

    pub fn get_agent(&self, agent_id: i64) -> Result<Agent, Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("agents/{}", agent_id))?;
        let response = self.client.get(endpoint).send()?;
        if response.status().is_success() {
            let agent: Agent = response.json()?;
            Ok(agent)
        } else if response.status().as_u16() == 404 {
            Err(format!("Agent {} not found", agent_id).into())
        } else {
            Err(format!("Failed to get agent: {}", response.status()).into())
        }
    }

    pub fn update_agent(
        &self,
        agent_id: i64,
        cluster_id: &str,
    ) -> Result<AgentWithToken, Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("agents/{}", agent_id))?;
        let body = AgentUpdates {
            cluster_id: Some(cluster_id.to_string()),
        };
        let response = self.client.patch(endpoint).json(&body).send()?;
        if response.status().is_success() {
            let agent: AgentWithToken = response.json()?;
            Ok(agent)
        } else {
            Err(format!("Failed to update agent: {}", response.status()).into())
        }
    }

    pub fn delete_agent(&self, agent_id: i64) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("agents/{}", agent_id))?;
        let response = self.client.delete(endpoint).send()?;
        if response.status().is_success() {
            Ok(())
        } else if response.status().as_u16() == 404 {
            Err(format!("Agent {} not found", agent_id).into())
        } else {
            Err(format!("Failed to delete agent: {}", response.status()).into())
        }
    }

    pub fn data_source_is_valid(&self, dsn: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let endpoint = self.url.join(&format!("datasources/valid?dsn={}", dsn))?;
        let response = self.client.get(endpoint).send()?;
        Ok(response.status().is_success())
    }
}

#[cfg(test)]
mod basic;

#[cfg(test)]
mod extended;

#[cfg(test)]
mod upload_download;
