use std::{path::PathBuf, time::Duration};

use anyhow::{bail, Context, Result};
use base64::Engine;
use ha_core::activity::TaskStatus;
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE},
    Client,
};
use serde::{de::DeserializeOwned, Serialize};
use taosx_utils::backoff::RetryBackoff;
use url::Url;
use uuid::Uuid;

use super::{
    Agent, AgentProps, AgentUpdates, AgentWithToken, DataSourcesReq, DownloadAllPointsParams,
    DsnAgentQuery, DsnAgentQueryV2, ErrorResponse, HealthResponse, ProfileResponse, Task,
    TaskBatchReq, TaskListResponse, TaskTicket, UpdateTask,
};

fn default_headers(basic_auth: Option<(&str, &str)>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some((username, password)) = basic_auth {
        let credentials = format!("{}:{}", username, password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(credentials.as_bytes());
        if let Ok(value) = HeaderValue::from_str(&format!("Basic {}", encoded)) {
            headers.insert(AUTHORIZATION, value);
        }
    }
    headers
}

/// 用于构建 [ApiClient] 的 Builder。
pub struct ClientBuilder {
    url: String,
    auth: Option<(String, String)>,
}

impl ClientBuilder {
    /// 从 base URL 创建 builder，默认无认证。
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            auth: None,
        }
    }

    /// 设置 HTTP Basic 认证。
    pub fn with_auth(mut self, username: &str, password: &str) -> Self {
        self.auth = Some((username.to_string(), password.to_string()));
        self
    }

    /// 构建 [ApiClient]。
    pub fn build(self) -> Result<ApiClient> {
        let url = Url::parse(&self.url)?;
        let headers = self
            .auth
            .as_ref()
            .map(|(u, p)| default_headers(Some((u.as_str(), p.as_str()))))
            .unwrap_or_else(|| default_headers(None));
        let client = Client::builder()
            .user_agent("taosx-test-client")
            .default_headers(headers)
            .build()?;
        Ok(ApiClient { url, client })
    }
}

pub struct ApiClient {
    pub url: Url,
    pub client: Client,
}

impl ApiClient {
    /// Extract error message from a failed HTTP response
    async fn handle_error_response<T>(response: reqwest::Response, operation: &str) -> Result<T> {
        let status = response.status();
        let message = response
            .text()
            .await
            .context(format!("Failed to get {} response text", operation))?;
        bail!("Failed to {}: {}, message: {}", operation, status, message);
    }

    fn build_endpoint_with_query<K, V, I>(&self, path: &str, pairs: I) -> Result<Url>
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut endpoint = self.url.join(path)?;
        {
            let mut query = endpoint.query_pairs_mut();
            for (key, value) in pairs {
                query.append_pair(key.as_ref(), value.as_ref());
            }
        }
        Ok(endpoint)
    }

    async fn get_json_from_endpoint<T: DeserializeOwned>(
        &self,
        endpoint: Url,
        operation: &str,
    ) -> Result<T> {
        let response = self.client.get(endpoint).send().await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Self::handle_error_response(response, operation).await
        }
    }

    /// GET request that returns JSON deserialized to type T
    async fn get_json<T: DeserializeOwned>(&self, path: &str, operation: &str) -> Result<T> {
        self.get_json_from_endpoint(self.url.join(path)?, operation)
            .await
    }

    /// GET request that returns Option<T> (404 → None)
    async fn get_json_optional<T: DeserializeOwned>(
        &self,
        path: &str,
        operation: &str,
    ) -> Result<Option<T>> {
        let endpoint = self.url.join(path)?;
        let response = self.client.get(endpoint).send().await?;

        if response.status().is_success() {
            Ok(Some(response.json().await?))
        } else if response.status().as_u16() == 404 {
            Ok(None)
        } else {
            Self::handle_error_response(response, operation).await
        }
    }

    /// POST request with JSON body, returns JSON response
    async fn post_json<T: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
        operation: &str,
    ) -> Result<R> {
        let endpoint = self.url.join(path)?;
        let response = self.client.post(endpoint).json(body).send().await?;

        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Self::handle_error_response(response, operation).await
        }
    }

    /// POST request with JSON body, no response expected
    async fn post_json_no_response<T: Serialize>(
        &self,
        path: &str,
        body: &T,
        operation: &str,
    ) -> Result<()> {
        let endpoint = self.url.join(path)?;
        let response = self.client.post(endpoint).json(body).send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Self::handle_error_response(response, operation).await
        }
    }

    /// GET request that returns bytes
    async fn get_bytes_from_endpoint(&self, endpoint: Url, operation: &str) -> Result<Vec<u8>> {
        let response = self.client.get(endpoint).send().await?;

        if response.status().is_success() {
            Ok(response.bytes().await?.to_vec())
        } else {
            Self::handle_error_response(response, operation).await
        }
    }

    /// GET request that returns response text
    async fn get_text(&self, path: &str, operation: &str) -> Result<String> {
        let endpoint = self.url.join(path)?;
        let response = self.client.get(endpoint).send().await?;

        if response.status().is_success() {
            Ok(response.text().await?)
        } else {
            Self::handle_error_response(response, operation).await
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ApiCheckValidParamClient {
    pub from: Option<String>,
    pub from_json: Option<serde_json::Value>,
    pub to: String,
    pub via: Option<i64>,
}

impl ApiClient {
    /// 创建用于构建 API 客户端的 [ClientBuilder]。
    pub fn builder(url: &str) -> ClientBuilder {
        ClientBuilder::new(url)
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        self.get_text("health", "check health").await
    }

    pub async fn swagger(&self) -> Result<String> {
        self.get_text("api-doc/openapi.json", "fetch swagger").await
    }

    pub async fn list_tasks(&self) -> Result<Vec<Task>> {
        let list_response: TaskListResponse = self.get_json("tasks", "list tasks").await?;
        Ok(list_response.0)
    }

    pub async fn get_task(&self, tid: u32) -> Result<Option<Task>> {
        self.get_json_optional(&format!("tasks/{}", tid), "get task")
            .await
    }

    pub async fn get_task_metrics(&self, task_id: u32) -> Result<serde_json::Value> {
        self.get_json(&format!("tasks/{}/metrics", task_id), "get task metrics")
            .await
    }

    /// 从 task metrics 的 JSON 中解析 `current.written_rows`，返回 `Option<u64>`。
    fn written_rows(metrics: &serde_json::Value) -> Option<u64> {
        metrics
            .get("current")
            .and_then(|v| v.get("written_rows"))
            .and_then(|v| v.as_u64())
    }

    /// 拉取任务 metrics 并解析出 `written_rows`，仅返回 `written_rows`。
    pub async fn get_task_metrics_written_rows(&self, task_id: u32) -> Result<Option<u64>> {
        let value = self
            .get_task_metrics(task_id)
            .await
            .context("get task metrics via api")?;
        Ok(Self::written_rows(&value))
    }

    /// 默认退避：初始 500ms，最大 10s，最多 20 次。
    const TASK_POLL_INIT: Duration = Duration::from_secs(1);
    const TASK_POLL_MAX: Duration = Duration::from_secs(5);
    const TASK_POLL_MAX_ATTEMPTS: usize = 10;

    /// 轮询直到任务状态为 `Running`，使用指数退避；超时或达到最大次数时返回错误。
    pub async fn wait_until_running(&self, task_id: u32) -> Result<Task> {
        self.wait_until_status(task_id, TaskStatus::Running).await
    }

    /// 轮询直到任务状态等于 `target`，使用指数退避；超时或达到最大次数时返回错误。
    pub async fn wait_until_status(&self, task_id: u32, target: TaskStatus) -> Result<Task> {
        self.wait_until_status_with_options(
            task_id,
            target,
            Self::TASK_POLL_INIT,
            Self::TASK_POLL_MAX,
            Self::TASK_POLL_MAX_ATTEMPTS,
        )
        .await
    }

    /// 带可选参数的“等待任务到达指定状态”；`init`/`max` 为退避间隔，`max_attempts` 为最大重试次数。
    pub async fn wait_until_status_with_options(
        &self,
        task_id: u32,
        target: TaskStatus,
        init: Duration,
        max: Duration,
        max_attempts: usize,
    ) -> Result<Task> {
        let mut backoff = RetryBackoff::new(init, max);
        let mut task = self
            .get_task(task_id)
            .await
            .context("get task via api")?
            .context("task not found")?;
        for attempt in 1..=max_attempts {
            tracing::info!("task {} status: {:?}", task_id, task.status);
            if task.status == target {
                tracing::info!(
                    "task {} reached {} after {} attempt(s)",
                    task_id,
                    target,
                    attempt
                );
                return Ok(task);
            }
            if attempt == max_attempts {
                anyhow::bail!(
                    "task {} did not reach {:?} after {} attempts (last status: {:?})",
                    task_id,
                    target,
                    max_attempts,
                    task.status
                );
            }
            backoff.wait().await;
            task = self
                .get_task(task_id)
                .await
                .context("get task via api")?
                .context("task not found")?;
        }
        anyhow::bail!(
            "task {} did not reach {:?} after {} attempts (last status: {:?})",
            task_id,
            target,
            max_attempts,
            task.status
        );
    }

    /// 轮询直到任务 metrics 中 `written_rows` 达到 `expected_rows`，使用指数退避；超时则返回错误。
    pub async fn wait_until_written_rows(
        &self,
        task_id: u32,
        expected_rows: u64,
    ) -> Result<serde_json::Value> {
        let mut backoff = RetryBackoff::new(Self::TASK_POLL_INIT, Self::TASK_POLL_MAX);
        loop {
            let last_written = self
                .get_task_metrics_written_rows(task_id)
                .await
                .context("get task metrics via api")?;
            match last_written {
                None => {
                    if backoff.retries() >= Self::TASK_POLL_MAX_ATTEMPTS {
                        anyhow::bail!(
                            "task {} written_rows did not reach {} after {} attempts (no written_rows in metrics)",
                            task_id,
                            expected_rows,
                            Self::TASK_POLL_MAX_ATTEMPTS
                        );
                    }
                    backoff.wait().await;
                }
                Some(written) => {
                    tracing::info!("task {} written_rows: {}", task_id, written);
                    if written >= expected_rows {
                        tracing::info!(
                            "task {} written_rows {} >= {} after {} attempt(s)",
                            task_id,
                            written,
                            expected_rows,
                            backoff.retries()
                        );
                        return self
                            .get_task_metrics(task_id)
                            .await
                            .context("get task metrics via api");
                    }
                    if backoff.retries() > Self::TASK_POLL_MAX_ATTEMPTS {
                        anyhow::bail!(
                            "task {} written_rows did not reach {} after {} attempts (last written_rows: {})",
                            task_id,
                            expected_rows,
                            Self::TASK_POLL_MAX_ATTEMPTS,
                            written
                        );
                    }
                    backoff.wait().await;
                }
            }
        }
    }

    pub async fn create_task(&self, task: &super::NewTask) -> Result<Task> {
        let endpoint = self.url.join("tasks")?;
        tracing::info!(
            "Creating task at endpoint: {} with data {}",
            endpoint,
            serde_json::to_string(task).unwrap()
        );
        let response = self.client.post(endpoint).json(task).send().await?;
        if response.status().is_success() {
            let created_task: Task = response.json().await?;
            tracing::info!("Successfully created task with ID: {}", created_task.id);
            Ok(created_task)
        } else {
            let status = response.status();
            let body = response.text().await?;

            tracing::info!("Response status: {}", status);

            match serde_json::from_str::<ErrorResponse>(&body) {
                Ok(err) => {
                    tracing::info!("Response message: {}", err.message);
                    bail!("Failed to create task: {}", err.message);
                }
                Err(parse_err) => {
                    tracing::warn!(
                        "Failed to parse error JSON: {}, status: {}, raw body: {}",
                        parse_err,
                        status,
                        body
                    );
                    bail!("Failed to create task, status: {}, body: {}", status, body);
                }
            }
        }
    }

    pub async fn update_task(&self, tid: u32, task: &UpdateTask) -> Result<Task> {
        let endpoint = self.url.join(&format!("tasks/{}", tid))?;
        let response = self.client.patch(endpoint).json(task).send().await?;
        if response.status().is_success() {
            let task: Task = response.json().await?;
            Ok(task)
        } else if response.status().as_u16() == 404 {
            bail!("Task {} not found", tid);
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get update task response text")?;
            bail!("Failed to update task: {status}, message: {message}");
        }
    }

    pub async fn delete_task(&self, tid: u32) -> Result<()> {
        let endpoint = self.url.join(&format!("tasks/{}", tid))?;
        let response = self.client.delete(endpoint).send().await?;
        if response.status().is_success() || response.status().as_u16() == 204 {
            Ok(())
        } else if response.status().as_u16() == 404 {
            bail!("Task {} not found", tid);
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get delete task response text")?;
            bail!("Failed to delete task: {status}, message: {message}");
        }
    }

    pub async fn start_task(&self, tid: u32) -> Result<()> {
        let endpoint = self.url.join(&format!("tasks/{}/start", tid))?;
        let response = self.client.post(endpoint).send().await?;
        if response.status().is_success() {
            Ok(())
        } else if response.status().as_u16() == 404 {
            bail!("Task {} not found", tid);
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get start task response text")?;
            bail!("Failed to start task: {status}, message: {message}");
        }
    }

    pub async fn stop_task(&self, tid: u32) -> Result<()> {
        let endpoint = self.url.join(&format!("tasks/{}/stop", tid))?;
        let response = self.client.post(endpoint).send().await?;
        if response.status().is_success() {
            Ok(())
        } else if response.status().as_u16() == 404 {
            bail!("Task {} not found", tid);
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get stop task response text")?;
            bail!("Failed to stop task: {status}, message: {message}");
        }
    }

    pub async fn get_task_count(&self) -> Result<u32> {
        let count: serde_json::Value = self.get_json("tasks/count", "get task count").await?;
        Ok(count.as_u64().unwrap_or(0) as u32)
    }

    pub async fn batch_start_tasks(&self, ids: Vec<u32>) -> Result<()> {
        let body = TaskBatchReq { ids };
        self.post_json_no_response("tasks/start", &body, "batch start tasks")
            .await
    }

    pub async fn batch_stop_tasks(&self, ids: Vec<u32>) -> Result<()> {
        let body = TaskBatchReq { ids };
        self.post_json_no_response("tasks/stop", &body, "batch stop tasks")
            .await
    }

    pub async fn batch_delete_tasks(&self, ids: Vec<u32>) -> Result<()> {
        let body = TaskBatchReq { ids };
        self.post_json_no_response("tasks/delete", &body, "batch delete tasks")
            .await
    }

    pub async fn profile(&self) -> Result<ProfileResponse> {
        self.get_json("profile", "get profile").await
    }

    pub async fn metrics(&self) -> Result<String> {
        self.get_text("metrics", "get metrics").await
    }

    pub async fn metrics_description(&self, lang: &str) -> Result<serde_json::Value> {
        let endpoint = self.build_endpoint_with_query("metrics/description", [("lang", lang)])?;
        self.get_json_from_endpoint(endpoint, "get metrics description")
            .await
    }

    pub async fn list_data_sources(&self, lang: Option<&str>) -> Result<serde_json::Value> {
        let mut endpoint = self.url.join("ds/in")?;
        if let Some(lang) = lang {
            endpoint.query_pairs_mut().append_pair("lang", lang);
        }
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get list data sources response text")?;
            bail!("Failed to list data sources: {status}, message: {message}");
        }
    }

    pub async fn get_data_source(
        &self,
        name: &str,
        lang: Option<&str>,
    ) -> Result<serde_json::Value> {
        let mut endpoint = self.url.join(&format!("ds/in/{}", name))?;
        if let Some(lang) = lang {
            endpoint.query_pairs_mut().append_pair("lang", lang);
        }
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else if response.status().as_u16() == 404 {
            bail!("Data source {} not found", name);
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get data source response text")?;
            bail!("Failed to get data source: {status}, message: {message}");
        }
    }

    pub async fn validate_data_source(
        &self,
        body: &ApiCheckValidParamClient,
    ) -> Result<serde_json::Value> {
        self.post_json("ds/in/validate", body, "validate data source")
            .await
    }

    pub async fn validate_data_source_sink(
        &self,
        query: &DsnAgentQueryV2,
    ) -> Result<serde_json::Value> {
        self.post_json("ds/in/validate", query, "validate sink data source")
            .await
    }

    pub async fn collect_data_sources(&self, req: &DataSourcesReq) -> Result<serde_json::Value> {
        self.post_json("ds/in/sets", req, "collect data sources")
            .await
    }

    pub async fn get_sample_data(&self, query: &DsnAgentQuery) -> Result<serde_json::Value> {
        self.post_json("ds/in/sample", query, "get sample data")
            .await
    }

    pub async fn get_sample_data_by_dsn(
        &self,
        dsn: &str,
        via: Option<&str>,
    ) -> Result<serde_json::Value> {
        let mut endpoint = self.url.join("ds/in/sample")?;
        {
            let mut pairs = endpoint.query_pairs_mut();
            pairs.append_pair("dsn", dsn);
            if let Some(v) = via {
                pairs.append_pair("via", v);
            }
        }
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get sample by dsn response text")?;
            bail!("Failed to get sample by dsn: {status}, message: {message}");
        }
    }

    pub async fn upload_files(&self, files: Vec<(String, Vec<u8>)>) -> Result<Vec<String>> {
        let endpoint = self.url.join("upload")?;
        let form = build_upload_form(files);
        let response = self.client.post(endpoint).multipart(form).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get upload files response text")?;
            bail!("Failed to upload files: {status}, message: {message}");
        }
    }

    /// Upload files by local paths via `/upload` endpoint.
    ///
    /// Reads each file into memory and forwards to [`upload_files`].
    pub async fn upload_files_from_paths(&self, paths: Vec<PathBuf>) -> Result<Vec<String>> {
        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .context("file name is not valid UTF-8")?
                .to_string();
            let data = tokio::fs::read(&path)
                .await
                .with_context(|| format!("failed to read file {}", path.display()))?;
            files.push((name, data));
        }
        self.upload_files(files).await
    }

    pub async fn download_file(&self, file_path: &str) -> Result<Vec<u8>> {
        let endpoint = self.build_endpoint_with_query("download", [("file_path", file_path)])?;
        self.get_bytes_from_endpoint(endpoint, "download file")
            .await
    }

    pub async fn check_file_exists(&self, file_path: &str) -> Result<bool> {
        let mut endpoint = self.url.join("check_exists")?;
        endpoint
            .query_pairs_mut()
            .append_pair("file_path", file_path);
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let val: serde_json::Value = response.json().await?;
            Ok(val.get("exists").and_then(|v| v.as_bool()).unwrap_or(false))
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get check file response text")?;
            bail!("Failed to check file: {status}, message: {message}");
        }
    }

    pub async fn start_point_download_task(
        &self,
        params: &DownloadAllPointsParams,
    ) -> Result<TaskTicket> {
        self.post_json(
            "ds/in/point/file/download/task",
            params,
            "start point download task",
        )
        .await
    }

    pub async fn point_task_ready(&self, ticket: &str) -> Result<TaskTicket> {
        let endpoint =
            self.build_endpoint_with_query("ds/in/point/file/are/you/ready", [("ticket", ticket)])?;
        self.get_json_from_endpoint(endpoint, "check point task")
            .await
    }

    pub async fn download_point_file(&self, ticket: &str, remain: bool) -> Result<Vec<u8>> {
        let endpoint = self.build_endpoint_with_query(
            "ds/in/point/file/async",
            [
                ("ticket", ticket.to_string()),
                ("remain", remain.to_string()),
            ],
        )?;
        self.get_bytes_from_endpoint(endpoint, "download point file")
            .await
    }

    pub async fn point_data_page(
        &self,
        ticket: &str,
        page: Option<usize>,
        page_size: Option<usize>,
    ) -> Result<serde_json::Value> {
        let mut query = vec![("ticket", ticket.to_string())];
        if let Some(p) = page {
            query.push(("page", p.to_string()));
        }
        if let Some(ps) = page_size {
            query.push(("page_size", ps.to_string()));
        }
        let endpoint = self.build_endpoint_with_query("ds/in/point/data/page", query)?;
        self.get_json_from_endpoint(endpoint, "get point data page")
            .await
    }

    pub async fn download_point_template(
        &self,
        driver: &str,
        lang: Option<&str>,
    ) -> Result<Vec<u8>> {
        let mut query = vec![("driver", driver.to_string())];
        if let Some(lang) = lang {
            query.push(("lang", lang.to_string()));
        }
        let endpoint = self.build_endpoint_with_query("ds/in/point/file/template", query)?;
        self.get_bytes_from_endpoint(endpoint, "download point template")
            .await
    }

    pub async fn create_agent(
        &self,
        name: &str,
        dsn: &str,
        cluster_id: &str,
        user: &str,
    ) -> Result<AgentWithToken> {
        let body = AgentProps {
            name: name.to_string(),
            dsn: dsn.to_string(),
            cluster_id: cluster_id.to_string(),
            user_id: Some(user.to_string()),
        };
        self.post_json("agents", &body, "create agent").await
    }

    pub async fn list_agents(&self, cluster_id: Option<&str>) -> Result<Vec<Agent>> {
        let endpoint = if let Some(cid) = cluster_id {
            self.build_endpoint_with_query("agents", [("cluster_id", cid)])?
        } else {
            self.url.join("agents")?
        };
        self.get_json_from_endpoint(endpoint, "list agents").await
    }

    pub async fn get_agent_by_name(&self, name: &str) -> Result<Agent> {
        let agents = self
            .list_agents(None)
            .await
            .context("list agents via api")?;
        agents
            .into_iter()
            .find(|a| a.name == name)
            .with_context(|| format!("agent with name {} not found", name))
    }

    pub async fn get_agent(&self, agent_id: i64) -> Result<Agent> {
        let path = format!("agents/{}", agent_id);
        match self.get_json_optional(&path, "get agent").await? {
            Some(agent) => Ok(agent),
            None => bail!("Agent {} not found", agent_id),
        }
    }

    pub async fn update_agent(&self, agent_id: i64, cluster_id: &str) -> Result<AgentWithToken> {
        let endpoint = self.url.join(&format!("agents/{}", agent_id))?;
        let body = AgentUpdates {
            cluster_id: Some(cluster_id.to_string()),
        };
        let response = self.client.patch(endpoint).json(&body).send().await?;
        if response.status().is_success() {
            let agent: AgentWithToken = response.json().await?;
            Ok(agent)
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get update agent response text")?;
            bail!("Failed to update agent: {status}, message: {message}");
        }
    }

    pub async fn delete_agent(&self, agent_id: i64) -> Result<()> {
        let endpoint = self.url.join(&format!("agents/{}", agent_id))?;
        let response = self.client.delete(endpoint).send().await?;
        if response.status().is_success() {
            Ok(())
        } else if response.status().as_u16() == 404 {
            bail!("Agent {} not found", agent_id);
        } else {
            let status = response.status();
            let message = response
                .text()
                .await
                .context("Failed to get delete agent response text")?;
            bail!("Failed to delete agent: {status}, message: {message}");
        }
    }

    pub async fn data_source_is_valid(&self, dsn: &str) -> Result<bool> {
        let endpoint = self.build_endpoint_with_query("datasources/valid", [("dsn", dsn)])?;
        let response = self.client.get(endpoint).send().await?;
        Ok(response.status().is_success())
    }
}

// ── Shared upload helpers ─────────────────────────────────────────────────────

/// Build a multipart form for a file-upload request.
///
/// Each entry in `files` is `(filename, bytes)`. A collision-resistant `req_id`
/// field is added automatically so concurrent uploads never share a bucket.
fn new_upload_req_id() -> String {
    format!("upload-{}", Uuid::new_v4())
}

fn build_upload_form(files: Vec<(String, Vec<u8>)>) -> reqwest::multipart::Form {
    let req_id = new_upload_req_id();
    let mut form = reqwest::multipart::Form::new().text("req_id", req_id);
    for (name, data) in files {
        let part = reqwest::multipart::Part::bytes(data).file_name(name);
        form = form.part("file", part);
    }
    form
}

// ── Explorer API client ───────────────────────────────────────────────────────

/// Raw bytes returned by the export endpoint together with its content-type.
///
/// The content-type is `application/json` for tasks without uploaded-file
/// references and `application/zip` for tasks that bundle uploaded files.
pub struct ExportResponse {
    pub bytes: Vec<u8>,
    pub content_type: String,
}

/// Minimal task view returned by Explorer `/api/x/tasks`.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct ExplorerTaskSummary {
    pub id: u32,
    pub name: String,
}

/// Task detail returned by Explorer `/api/x/tasks/{id}`.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct ExplorerTask {
    pub id: u32,
    pub name: String,
    pub status: TaskStatus,
}

/// Builder for [`ExplorerApiClient`].
pub struct ExplorerClientBuilder {
    url: String,
    auth: Option<(String, String)>,
}

impl ExplorerClientBuilder {
    /// Create a builder targeting `url` (the taos-explorer base URL).
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            auth: None,
        }
    }

    /// Set HTTP Basic authentication credentials.
    pub fn with_auth(mut self, username: &str, password: &str) -> Self {
        self.auth = Some((username.to_string(), password.to_string()));
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<ExplorerApiClient> {
        let url = Url::parse(&self.url)?;
        let headers = self
            .auth
            .as_ref()
            .map(|(u, p)| default_headers(Some((u.as_str(), p.as_str()))))
            .unwrap_or_else(|| default_headers(None));
        let client = Client::builder()
            .user_agent("taosx-test-client")
            .default_headers(headers)
            .build()?;
        Ok(ExplorerApiClient { url, client })
    }
}

/// HTTP client for the `taos-explorer` REST API.
///
/// Covers the import/export endpoints under `/api/x/tasks/`.
pub struct ExplorerApiClient {
    pub url: Url,
    pub client: Client,
}

impl ExplorerApiClient {
    /// Create a [`ExplorerClientBuilder`] targeting `url`.
    pub fn builder(url: &str) -> ExplorerClientBuilder {
        ExplorerClientBuilder::new(url)
    }

    /// POST a task-export JSON payload to `POST /api/x/tasks/import`.
    ///
    /// The payload must conform to the `ExportTaskResult` schema produced by
    /// the export endpoint: `{ tasks_num, export_time, tasks: [...] }`.
    pub async fn import_tasks(&self, payload: &serde_json::Value) -> Result<()> {
        let endpoint = self.url.join("api/x/tasks/import")?;
        let response = self.client.post(endpoint).json(payload).send().await?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read import response body")?;
            bail!("import tasks failed: {status}, body: {body}");
        }
    }

    /// Read a JSON fixture file and import it via [`import_tasks`].
    pub async fn import_tasks_from_json_path(&self, path: &std::path::Path) -> Result<()> {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("read fixture {}", path.display()))?;
        let payload: serde_json::Value =
            serde_json::from_slice(&bytes).context("parse fixture as JSON")?;
        self.import_tasks(&payload).await
    }

    /// GET `GET /api/x/tasks/export?ids=<comma-separated>` and return the raw
    /// response bytes together with the `Content-Type` header value.
    pub async fn export_tasks(&self, ids: &[i64]) -> Result<ExportResponse> {
        let ids_str = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut endpoint = self.url.join("api/x/tasks/export")?;
        endpoint.query_pairs_mut().append_pair("ids", &ids_str);
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            let bytes = response.bytes().await?.to_vec();
            Ok(ExportResponse {
                bytes,
                content_type,
            })
        } else {
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read export response body")?;
            bail!("export tasks failed: {status}, body: {body}");
        }
    }

    /// GET `GET /api/x/tasks` and return the visible task list.
    pub async fn list_tasks(&self) -> Result<Vec<ExplorerTaskSummary>> {
        let endpoint = self.url.join("api/x/tasks")?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read list tasks response body")?;
            bail!("list tasks failed: {status}, body: {body}");
        }
    }

    /// DELETE `DELETE /api/x/tasks/{id}`.
    pub async fn delete_task(&self, id: u32) -> Result<()> {
        let endpoint = self.url.join(&format!("api/x/tasks/{id}"))?;
        let response = self.client.delete(endpoint).send().await?;
        if response.status().is_success() || response.status().as_u16() == 204 {
            Ok(())
        } else {
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read delete task response body")?;
            bail!("delete task failed: {status}, body: {body}");
        }
    }

    /// GET `GET /api/x/tasks/{id}` to retrieve task details including status.
    pub async fn get_task(&self, id: u32) -> Result<ExplorerTask> {
        let endpoint = self.url.join(&format!("api/x/tasks/{id}"))?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read get task response body")?;
            bail!("get task failed: {status}, body: {body}");
        }
    }

    /// POST `POST /api/x/tasks/{id}/start` to start a task.
    pub async fn start_task(&self, id: u32) -> Result<()> {
        let endpoint = self.url.join(&format!("api/x/tasks/{id}/start"))?;
        let response = self.client.post(endpoint).send().await?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read start task response body")?;
            bail!("start task failed: {status}, body: {body}");
        }
    }

    /// Poll until task reaches `Running` status.
    ///
    /// Uses exponential backoff with default intervals (1s initial, 5s max)
    /// and retries up to 10 times. Returns the task once it reaches Running.
    pub async fn wait_until_running(&self, id: u32) -> Result<ExplorerTask> {
        const POLL_INIT: Duration = Duration::from_secs(1);
        const POLL_MAX: Duration = Duration::from_secs(5);
        const MAX_ATTEMPTS: usize = 10;

        let mut backoff = RetryBackoff::new(POLL_INIT, POLL_MAX);
        let mut task = self.get_task(id).await.context("get task via api")?;

        for attempt in 1..=MAX_ATTEMPTS {
            tracing::info!("task {} status: {:?}", id, task.status);
            if task.status == TaskStatus::Running {
                tracing::info!("task {} reached Running after {} attempt(s)", id, attempt);
                return Ok(task);
            }
            if attempt == MAX_ATTEMPTS {
                bail!(
                    "task {} did not reach Running after {} attempts (last status: {:?})",
                    id,
                    MAX_ATTEMPTS,
                    task.status
                );
            }
            backoff.wait().await;
            task = self.get_task(id).await.context("get task via api")?;
        }
        bail!(
            "task {} did not reach Running after {} attempts (last status: {:?})",
            id,
            MAX_ATTEMPTS,
            task.status
        );
    }

    /// Upload raw file bytes via `POST /api/x/upload` (proxied to taosx).
    ///
    /// Returns the server-assigned paths for each uploaded file.
    pub async fn upload_files(&self, files: Vec<(String, Vec<u8>)>) -> Result<Vec<String>> {
        let endpoint = self.url.join("api/x/upload")?;
        let form = build_upload_form(files);
        let response = self.client.post(endpoint).multipart(form).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read upload response body")?;
            bail!("upload files failed: {status}, body: {body}");
        }
    }

    /// Import a task-export ZIP fixture produced by the export endpoint.
    ///
    /// This method replicates the browser-side ZIP handling:
    /// 1. Extract `tasks.json` and any bundled files from the archive.
    /// 2. If there are bundled files, upload them via [`upload_files`] to
    ///    obtain new server-assigned paths, then rewrite every `@files/…`
    ///    reference in the JSON to point at the new paths.
    /// 3. POST the (possibly rewritten) JSON to `import_tasks`.
    ///
    /// When there are no bundled files the JSON is posted as-is, which is
    /// equivalent to a legacy JSON import.
    pub async fn import_tasks_from_zip_path(&self, zip_path: &std::path::Path) -> Result<()> {
        use std::io::{Cursor, Read};

        let zip_bytes = tokio::fs::read(zip_path)
            .await
            .with_context(|| format!("read zip fixture {}", zip_path.display()))?;

        let mut tasks_json_bytes: Option<Vec<u8>> = None;
        // (zip-relative-path, file-bytes)
        let mut bundled: Vec<(String, Vec<u8>)> = Vec::new();

        let cursor = Cursor::new(&zip_bytes[..]);
        let mut archive = zip::ZipArchive::new(cursor).context("open zip archive")?;
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).context("read zip entry")?;
            let name = entry.name().to_string();
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).context("read zip entry data")?;
            if name == "tasks.json" {
                tasks_json_bytes = Some(buf);
            } else if name.starts_with("files/") && !name.ends_with('/') {
                bundled.push((name, buf));
            }
        }

        let raw = tasks_json_bytes.context("tasks.json not found in ZIP")?;
        let mut payload: serde_json::Value =
            serde_json::from_slice(&raw).context("parse tasks.json")?;

        if !bundled.is_empty() {
            // Keep relative ZIP paths for rewriting and upload each file
            // separately so duplicate basenames from different ZIP paths
            // cannot overwrite each other in the same server-side bucket.
            let mut rel_paths: Vec<String> = Vec::with_capacity(bundled.len());
            let mut uploaded: Vec<String> = Vec::with_capacity(bundled.len());

            for (rel, data) in bundled {
                rel_paths.push(rel.clone());
                let filename = std::path::Path::new(&rel)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(&rel)
                    .to_string();
                let mut uploaded_paths = self
                    .upload_files(vec![(filename, data)])
                    .await
                    .with_context(|| format!("upload bundled file from ZIP: {rel}"))?;
                if uploaded_paths.len() != 1 {
                    bail!(
                        "expected one uploaded path for ZIP entry {rel}, got {}",
                        uploaded_paths.len()
                    );
                }
                uploaded.push(uploaded_paths.remove(0));
            }
            rewrite_zip_file_refs(&mut payload, &rel_paths, &uploaded);
        }

        self.import_tasks(&payload).await
    }
}

/// Replace `@files/<bucket>/<filename>` references in a JSON task payload with
/// the server paths returned after uploading the bundled files.
///
/// The match is done by relative ZIP path: the i-th element of `bundled_paths`
/// corresponds to the i-th element of `uploaded_paths`.
fn rewrite_zip_file_refs(
    value: &mut serde_json::Value,
    bundled_paths: &[String],
    uploaded_paths: &[String],
) {
    assert_eq!(
        bundled_paths.len(),
        uploaded_paths.len(),
        "invariant violated: bundled file count ({}) != uploaded path count ({})",
        bundled_paths.len(),
        uploaded_paths.len(),
    );
    let pairs: Vec<(&str, &str)> = bundled_paths
        .iter()
        .zip(uploaded_paths.iter())
        .map(|(old, new)| (old.as_str(), new.as_str()))
        .collect();
    rewrite_json_file_refs(value, &pairs);
}

/// Recursively walk a JSON value and replace every `@<old>` string with
/// `@<new>` for each `(old, new)` pair in `replacements`.
pub(crate) fn rewrite_json_file_refs(value: &mut serde_json::Value, replacements: &[(&str, &str)]) {
    match value {
        serde_json::Value::String(s) => {
            if let Some(raw) = s.strip_prefix('@') {
                for (old, new) in replacements {
                    if raw == *old {
                        *s = format!("@{new}");
                        return;
                    }
                }
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                rewrite_json_file_refs(v, replacements);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                rewrite_json_file_refs(v, replacements);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    fn spawn_single_response_server(content_type: &str, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let content_type = content_type.to_string();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        format!("http://{addr}/")
    }

    fn spawn_recording_server(
        content_type: &str,
        body: &'static str,
    ) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let content_type = content_type.to_string();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let raw = read_http_request(&mut stream);
            tx.send(String::from_utf8_lossy(&raw).to_string()).unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        (format!("http://{addr}/"), rx)
    }

    /// Read a complete HTTP/1.1 request from `stream`, returning all bytes.
    ///
    /// Reads until the `\r\n\r\n` header boundary, parses `Content-Length`,
    /// then drains the remaining body bytes with [`Read::read_exact`].
    fn read_http_request(stream: &mut impl Read) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut tmp = vec![0u8; 8192];
        let mut header_end: Option<usize> = None;

        while header_end.is_none() {
            let n = stream.read(&mut tmp).unwrap_or(0);
            if n == 0 {
                return buf;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                header_end = Some(pos + 4);
            }
        }

        let hdr_end = header_end.unwrap();
        let content_length: usize = std::str::from_utf8(&buf[..hdr_end])
            .unwrap_or("")
            .lines()
            .find_map(|line| {
                let lower = line.to_ascii_lowercase();
                lower
                    .starts_with("content-length:")
                    .then(|| lower["content-length:".len()..].trim().parse().ok())
                    .flatten()
            })
            .unwrap_or(0);

        let already_have_body = buf.len() - hdr_end;
        let remaining = content_length.saturating_sub(already_have_body);
        if remaining > 0 {
            let old_len = buf.len();
            buf.resize(old_len + remaining, 0);
            stream.read_exact(&mut buf[old_len..]).unwrap();
        }

        buf
    }

    #[tokio::test]
    async fn test_health_accepts_plain_text_response() {
        let server = spawn_single_response_server("text/plain", "ok");
        let client = ApiClient::builder(server.as_str()).build().unwrap();

        let health = client.health().await.unwrap();

        assert_eq!(health, "ok");
    }

    #[tokio::test]
    async fn test_swagger_returns_raw_json_text() {
        let server = spawn_single_response_server("application/json", "{\"openapi\":\"3.1.0\"}");
        let client = ApiClient::builder(server.as_str()).build().unwrap();

        let swagger = client.swagger().await.unwrap();

        assert_eq!(swagger, "{\"openapi\":\"3.1.0\"}");
    }

    #[tokio::test]
    async fn test_download_file_encodes_query_parameter() {
        let (server, request_rx) = spawn_recording_server("application/octet-stream", "payload");
        let client = ApiClient::builder(server.as_str()).build().unwrap();

        let payload = client.download_file("dir/a&b?.txt").await.unwrap();
        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        assert_eq!(payload, b"payload");
        assert!(
            request.starts_with("GET /download?file_path=dir%2Fa%26b%3F.txt HTTP/1.1"),
            "unexpected request line: {request}"
        );
    }

    // ── ExplorerApiClient tests (written before implementation — TDD) ─────────

    /// Verifies ExplorerApiClient can be built from a URL string.
    #[test]
    fn test_explorer_client_builder_succeeds() {
        let client = ExplorerApiClient::builder("http://127.0.0.1:6060/")
            .build()
            .unwrap();
        assert!(client.url.as_str().contains("127.0.0.1:6060"));
    }

    /// Verifies import_tasks sends a POST to /api/x/tasks/import with the JSON body.
    #[tokio::test]
    async fn test_import_tasks_posts_to_correct_path() {
        let (server, request_rx) = spawn_recording_server("application/json", "{}");
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();
        let payload = serde_json::json!({"tasks_num": 0, "export_time": "t", "tasks": []});
        client.import_tasks(&payload).await.unwrap();
        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            request.starts_with("POST /api/x/tasks/import HTTP/1.1"),
            "unexpected request line: {request}"
        );
    }

    /// Verifies export_tasks sends a GET to /api/x/tasks/export with ids query param.
    #[tokio::test]
    async fn test_export_tasks_sends_ids_query() {
        let (server, request_rx) = spawn_recording_server("application/json", "{}");
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();
        // The response will fail to parse as ExportResponse since the server
        // returns "{}"; the important thing is the request URL is correct.
        let _ = client.export_tasks(&[1, 2, 3]).await;
        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            request.starts_with("GET /api/x/tasks/export?ids=1%2C2%2C3 HTTP/1.1"),
            "unexpected request line: {request}"
        );
    }

    /// Verifies import_tasks_from_json_path reads a JSON file and posts it.
    #[tokio::test]
    async fn test_import_tasks_from_json_path_reads_file() {
        let (server, request_rx) = spawn_recording_server("application/json", "{}");
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();
        let fixture = crate::common::fixtures::import_export_fixture_path("mqtt-legacy.json");
        client.import_tasks_from_json_path(&fixture).await.unwrap();
        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            request.starts_with("POST /api/x/tasks/import HTTP/1.1"),
            "unexpected request line: {request}"
        );
    }

    /// Verifies export_tasks returns the response body bytes.
    #[tokio::test]
    async fn test_export_tasks_returns_bytes() {
        let server = spawn_single_response_server("application/json", "{\"ok\":true}");
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();
        let resp = client.export_tasks(&[42]).await.unwrap();
        assert_eq!(resp.bytes, b"{\"ok\":true}");
        assert!(resp.content_type.contains("application/json"));
    }

    /// Verifies list_tasks sends a GET to `/api/x/tasks` and parses the response.
    #[tokio::test]
    async fn test_explorer_list_tasks_uses_tasks_endpoint() {
        let body = r#"[{"id":42,"name":"demo"}]"#;
        let (server, request_rx) = spawn_recording_server("application/json", body);
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();
        let tasks = client.list_tasks().await.unwrap();
        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            request.starts_with("GET /api/x/tasks HTTP/1.1"),
            "unexpected request line: {request}"
        );
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, 42);
        assert_eq!(tasks[0].name, "demo");
    }

    /// Verifies delete_task sends a DELETE to `/api/x/tasks/{id}`.
    #[tokio::test]
    async fn test_explorer_delete_task_uses_task_endpoint() {
        let (server, request_rx) = spawn_recording_server("application/json", "{}");
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();
        client.delete_task(42).await.unwrap();
        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            request.starts_with("DELETE /api/x/tasks/42 HTTP/1.1"),
            "unexpected request line: {request}"
        );
    }

    /// Verifies multipart uploads are not sent with the client's default JSON content-type.
    #[tokio::test]
    async fn test_explorer_upload_files_uses_multipart_content_type() {
        let (server, request_rx) =
            spawn_recording_server("application/json", r#"["/tmp/demo.pem"]"#);
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();

        client
            .upload_files(vec![("demo.pem".to_string(), b"demo".to_vec())])
            .await
            .unwrap();

        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let request_lower = request.to_ascii_lowercase();
        assert!(
            request.starts_with("POST /api/x/upload HTTP/1.1"),
            "unexpected request line: {request}"
        );
        assert!(
            request_lower.contains("content-type: multipart/form-data;"),
            "upload request must use multipart/form-data; got:\n{request}"
        );
        assert!(
            !request_lower.contains("content-type: application/json"),
            "upload request must not reuse the default application/json content-type; got:\n{request}"
        );
    }

    #[test]
    fn test_new_upload_req_id_is_unique() {
        let first = new_upload_req_id();
        let second = new_upload_req_id();

        assert_ne!(first, second);
        assert!(first.starts_with("upload-"));
        assert!(second.starts_with("upload-"));
    }

    /// Verifies that when importing a ZIP with bundled files the client first
    /// uploads the files, then posts to /api/x/tasks/import with the rewritten
    /// file path returned by the upload endpoint.
    #[tokio::test]
    async fn test_import_tasks_from_zip_uploads_bundled_files_then_imports() {
        // Build a tiny server that handles two sequential requests:
        //   1. POST /api/x/upload    → returns a JSON array (upload result)
        //   2. POST /api/x/tasks/import → returns {}
        // The OPCUA fixture ZIP contains a bundled CSV (~78 KB), so the upload
        // step must drain the full multipart body before responding.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel::<String>();

        std::thread::spawn(move || {
            // First request: file upload (multipart) — drain entire body then respond.
            {
                let (mut stream, _) = listener.accept().unwrap();
                let _upload_req = read_http_request(&mut stream);
                let upload_body = r#"["files/new_bucket/test.csv"]"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{upload_body}",
                    len = upload_body.len()
                );
                stream.write_all(resp.as_bytes()).unwrap();
            }
            // Second request: task import — capture full request and respond {}.
            {
                let (mut stream, _) = listener.accept().unwrap();
                let import_req = read_http_request(&mut stream);
                tx.send(String::from_utf8_lossy(&import_req).into_owned())
                    .unwrap();
                let import_body = "{}";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{import_body}",
                    len = import_body.len()
                );
                stream.write_all(resp.as_bytes()).unwrap();
            }
        });

        let server_url = format!("http://{addr}/");
        let client = ExplorerApiClient::builder(&server_url).build().unwrap();
        let fixture = crate::common::fixtures::import_export_fixture_path("opcua-with-files.zip");

        client.import_tasks_from_zip_path(&fixture).await.unwrap();

        let request = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            request.starts_with("POST /api/x/tasks/import HTTP/1.1"),
            "unexpected request line: {request}"
        );
        assert!(
            request.contains("files/new_bucket/test.csv"),
            "import body should reference the uploaded path; got:\n{request}"
        );
    }

    #[tokio::test]
    async fn test_import_tasks_from_zip_uploads_duplicate_basenames_separately() {
        use std::io::Write;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = std::sync::mpsc::channel::<String>();

        std::thread::spawn(move || {
            let responses = [
                r#"["files/bucket-a/config.csv"]"#,
                r#"["files/bucket-b/config.csv"]"#,
                "{}",
            ];

            for response_body in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_http_request(&mut stream);
                tx.send(String::from_utf8_lossy(&request).into_owned())
                    .unwrap();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{response_body}",
                    len = response_body.len()
                );
                stream.write_all(resp.as_bytes()).unwrap();
            }
        });

        let temp_dir = tempfile::tempdir().unwrap();
        let zip_path = temp_dir.path().join("duplicate-basenames.zip");
        {
            let file = std::fs::File::create(&zip_path).unwrap();
            let mut zip = zip::ZipWriter::new(file);
            let opts = zip::write::SimpleFileOptions::default();
            zip.start_file("tasks.json", opts).unwrap();
            zip.write_all(
                br#"{"tasks_num":1,"export_time":"2026-04-13T00:00:00Z","tasks":[{"id":1,"name":"demo","from":{"file_a":"@files/a/config.csv","file_b":"@files/b/config.csv"},"to":"taos:///target","parser":null,"via":null,"created_at":"2026-04-13T00:00:00Z","trigger":null,"labels":null}]}"#,
            )
            .unwrap();
            zip.start_file("files/a/config.csv", opts).unwrap();
            zip.write_all(b"first").unwrap();
            zip.start_file("files/b/config.csv", opts).unwrap();
            zip.write_all(b"second").unwrap();
            zip.finish().unwrap();
        }

        let server_url = format!("http://{addr}/");
        let client = ExplorerApiClient::builder(&server_url).build().unwrap();
        client.import_tasks_from_zip_path(&zip_path).await.unwrap();

        let first_upload = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let second_upload = rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let import_request = rx.recv_timeout(Duration::from_secs(2)).unwrap();

        assert!(
            first_upload.starts_with("POST /api/x/upload HTTP/1.1"),
            "unexpected first request: {first_upload}"
        );
        assert!(
            second_upload.starts_with("POST /api/x/upload HTTP/1.1"),
            "unexpected second request: {second_upload}"
        );
        assert!(
            import_request.starts_with("POST /api/x/tasks/import HTTP/1.1"),
            "unexpected import request: {import_request}"
        );
        assert!(
            import_request.contains("files/bucket-a/config.csv"),
            "import body should reference first uploaded duplicate path; got:\n{import_request}"
        );
        assert!(
            import_request.contains("files/bucket-b/config.csv"),
            "import body should reference second uploaded duplicate path; got:\n{import_request}"
        );
    }

    /// Verifies get_task sends a GET to /api/x/tasks/{id}.
    #[tokio::test]
    async fn test_explorer_get_task_uses_task_endpoint() {
        let (server, request_rx) = spawn_recording_server(
            "application/json",
            r#"{"id":42,"name":"test-task","status":"running"}"#,
        );
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();
        let task = client.get_task(42).await.unwrap();
        assert_eq!(task.id, 42);
        assert_eq!(task.name, "test-task");
        assert_eq!(task.status, TaskStatus::Running);

        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            request.starts_with("GET /api/x/tasks/42 HTTP/1.1"),
            "unexpected request line: {request}"
        );
    }

    /// Verifies start_task sends a POST to /api/x/tasks/{id}/start.
    #[tokio::test]
    async fn test_explorer_start_task_uses_task_start_endpoint() {
        let (server, request_rx) = spawn_recording_server("application/json", "{}");
        let client = ExplorerApiClient::builder(server.as_str()).build().unwrap();
        client.start_task(99).await.unwrap();

        let request = request_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            request.starts_with("POST /api/x/tasks/99/start HTTP/1.1"),
            "unexpected request line: {request}"
        );
    }

    // ── rewrite_json_file_refs unit tests ─────────────────────────────────────

    /// Verifies that a top-level `@<old>` string is replaced with `@<new>`.
    #[test]
    fn test_rewrite_json_file_refs_replaces_top_level_string() {
        let mut value = serde_json::json!("@files/bucket/a.csv");
        rewrite_json_file_refs(&mut value, &[("files/bucket/a.csv", "files/new/a.csv")]);
        assert_eq!(value, serde_json::json!("@files/new/a.csv"));
    }

    /// Verifies that string values nested inside objects and arrays are rewritten.
    #[test]
    fn test_rewrite_json_file_refs_rewrites_nested_values() {
        let mut value = serde_json::json!({
            "task": {
                "config": "@files/bucket/cfg.csv",
                "tags": ["@files/bucket/tags.csv", "plain"]
            }
        });
        rewrite_json_file_refs(
            &mut value,
            &[
                ("files/bucket/cfg.csv", "files/new/cfg.csv"),
                ("files/bucket/tags.csv", "files/new/tags.csv"),
            ],
        );
        assert_eq!(
            value["task"]["config"],
            serde_json::json!("@files/new/cfg.csv")
        );
        assert_eq!(
            value["task"]["tags"][0],
            serde_json::json!("@files/new/tags.csv")
        );
        // Plain strings must not be touched.
        assert_eq!(value["task"]["tags"][1], serde_json::json!("plain"));
    }

    /// Verifies that strings not matching any replacement pair are left unchanged.
    #[test]
    fn test_rewrite_json_file_refs_leaves_unmatched_strings_unchanged() {
        let mut value = serde_json::json!("@files/other/file.csv");
        rewrite_json_file_refs(&mut value, &[("files/bucket/a.csv", "files/new/a.csv")]);
        assert_eq!(value, serde_json::json!("@files/other/file.csv"));
    }

    /// Verifies that strings without the `@` prefix are not modified.
    #[test]
    fn test_rewrite_json_file_refs_ignores_non_at_strings() {
        let mut value = serde_json::json!("files/bucket/a.csv");
        rewrite_json_file_refs(&mut value, &[("files/bucket/a.csv", "files/new/a.csv")]);
        assert_eq!(value, serde_json::json!("files/bucket/a.csv"));
    }
}
