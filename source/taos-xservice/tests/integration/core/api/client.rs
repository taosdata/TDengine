use std::{path::PathBuf, time::Duration};

use anyhow::{bail, Context, Result};
use base64::Engine;
use chrono::Utc;
use ha_core::activity::TaskStatus;
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE},
    Client,
};
use taosx_utils::backoff::RetryBackoff;
use url::Url;

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
        let endpoint = self.url.join("health")?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let health = response.text().await?;
            Ok(health)
        } else {
            bail!("Health check failed: {}", response.status());
        }
    }

    pub async fn swagger(&self) -> Result<String> {
        let endpoint = self.url.join("api-doc/openapi.json")?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let swagger_json = response.text().await?;
            Ok(swagger_json)
        } else {
            bail!("Failed to fetch swagger: {}", response.status());
        }
    }

    pub async fn list_tasks(&self) -> Result<Vec<Task>> {
        let endpoint = self.url.join("tasks")?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let list_response: TaskListResponse = response.json().await?;
            Ok(list_response.0)
        } else {
            bail!("Failed to list tasks: {}", response.status());
        }
    }

    pub async fn get_task(&self, tid: u32) -> Result<Option<Task>> {
        let endpoint = self.url.join(&format!("tasks/{}", tid))?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let task = response.json().await?;
            Ok(Some(task))
        } else if response.status().as_u16() == 404 {
            Ok(None)
        } else {
            bail!("Failed to get task: {}", response.status());
        }
    }

    pub async fn get_task_metrics(&self, task_id: u32) -> Result<serde_json::Value> {
        let endpoint = self.url.join(&format!("tasks/{}/metrics", task_id))?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let value: serde_json::Value = response.json().await?;
            Ok(value)
        } else {
            bail!("Failed to get task metrics: {}", response.status());
        }
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
            bail!("Failed to update task: {}", response.text().await?);
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
            bail!("Failed to delete task: {}", response.status());
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
            bail!("Failed to start task: {}", response.status());
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
            bail!("Failed to stop task: {}", response.status());
        }
    }

    pub async fn get_task_count(&self) -> Result<u32> {
        let endpoint = self.url.join("tasks/count")?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let count: serde_json::Value = response.json().await?;
            Ok(count.as_u64().unwrap_or(0) as u32)
        } else {
            bail!("Failed to get task count: {}", response.status());
        }
    }

    pub async fn batch_start_tasks(&self, ids: Vec<u32>) -> Result<()> {
        let endpoint = self.url.join("tasks/start")?;
        let body = TaskBatchReq { ids };
        let response = self.client.post(endpoint).json(&body).send().await?;
        if response.status().is_success() {
            Ok(())
        } else {
            bail!("Failed to batch start tasks: {}", response.status());
        }
    }

    pub async fn batch_stop_tasks(&self, ids: Vec<u32>) -> Result<()> {
        let endpoint = self.url.join("tasks/stop")?;
        let body = TaskBatchReq { ids };
        let response = self.client.post(endpoint).json(&body).send().await?;
        if response.status().is_success() {
            Ok(())
        } else {
            bail!("Failed to batch stop tasks: {}", response.status());
        }
    }

    pub async fn batch_delete_tasks(&self, ids: Vec<u32>) -> Result<()> {
        let endpoint = self.url.join("tasks/delete")?;
        let body = TaskBatchReq { ids };
        let response = self.client.post(endpoint).json(&body).send().await?;
        if response.status().is_success() {
            Ok(())
        } else {
            bail!("Failed to batch delete tasks: {}", response.status());
        }
    }

    pub async fn profile(&self) -> Result<ProfileResponse> {
        let endpoint = self.url.join("profile")?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let profile: ProfileResponse = response.json().await?;
            Ok(profile)
        } else {
            bail!("Failed to get profile: {}", response.status());
        }
    }

    pub async fn metrics(&self) -> Result<String> {
        let endpoint = self.url.join("metrics")?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let metrics = response.text().await?;
            Ok(metrics)
        } else {
            bail!("Failed to get metrics: {}", response.status());
        }
    }

    pub async fn metrics_description(&self, lang: &str) -> Result<serde_json::Value> {
        let endpoint = self
            .url
            .join(&format!("metrics/description?lang={}", lang))?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let desc: serde_json::Value = response.json().await?;
            Ok(desc)
        } else {
            bail!("Failed to get metrics description: {}", response.status());
        }
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
            bail!("Failed to list data sources: {}", response.status());
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
            bail!("Failed to get data source: {}", response.status());
        }
    }

    pub async fn validate_data_source(
        &self,
        body: &ApiCheckValidParamClient,
    ) -> Result<serde_json::Value> {
        let endpoint = self.url.join("ds/in/validate")?;
        let response = self.client.post(endpoint).json(&body).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            bail!("Failed to validate data source: {}", response.status());
        }
    }

    pub async fn validate_data_source_sink(
        &self,
        query: &DsnAgentQueryV2,
    ) -> Result<serde_json::Value> {
        let endpoint = self.url.join("ds/in/validate")?;
        let response = self.client.post(endpoint).json(query).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            bail!("Failed to validate sink data source: {}", response.status());
        }
    }

    pub async fn collect_data_sources(&self, req: &DataSourcesReq) -> Result<serde_json::Value> {
        let endpoint = self.url.join("ds/in/sets")?;
        let response = self.client.post(endpoint).json(req).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            bail!("Failed to collect data sources: {}", response.status());
        }
    }

    pub async fn get_sample_data(&self, query: &DsnAgentQuery) -> Result<serde_json::Value> {
        let endpoint = self.url.join("ds/in/sample")?;
        let response = self.client.post(endpoint).json(query).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            bail!("Failed to get sample data: {}", response.status());
        }
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
            bail!("Failed to get sample by dsn: {}", response.status());
        }
    }

    pub async fn upload_files(&self, files: Vec<(&str, Vec<u8>)>) -> Result<Vec<String>> {
        let endpoint = self.url.join("upload")?;
        let req_id = Utc::now().timestamp_millis().to_string();
        let mut form = reqwest::multipart::Form::new().text("req_id", req_id);
        for (name, data) in files {
            let part = reqwest::multipart::Part::bytes(data).file_name(name.to_string());
            form = form.part("file", part);
        }
        let response = self.client.post(endpoint).multipart(form).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            bail!("Failed to upload files: {}", response.status());
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
                .context("file name is not valid UTF-8")?;
            let data = tokio::fs::read(&path)
                .await
                .with_context(|| format!("failed to read file {}", path.display()))?;
            files.push((name.to_string(), data));
        }
        // convert into owned filename + bytes tuples for upload_files
        let files_owned: Vec<(String, Vec<u8>)> = files;
        let files_for_upload: Vec<(&str, Vec<u8>)> = files_owned
            .iter()
            .map(|(n, d)| (n.as_str(), d.clone()))
            .collect();
        self.upload_files(files_for_upload).await
    }

    pub async fn download_file(&self, file_path: &str) -> Result<Vec<u8>> {
        let mut endpoint = self.url.join("download")?;
        endpoint
            .query_pairs_mut()
            .append_pair("file_path", file_path);
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.bytes().await?.to_vec())
        } else {
            bail!("Failed to download file: {}", response.status());
        }
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
            bail!("Failed to check file: {}", response.status());
        }
    }

    pub async fn start_point_download_task(
        &self,
        params: &DownloadAllPointsParams,
    ) -> Result<TaskTicket> {
        let endpoint = self.url.join("ds/in/point/file/download/task")?;
        let response = self.client.post(endpoint).json(params).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            bail!("Failed to start point download task: {}", response.status());
        }
    }

    pub async fn point_task_ready(&self, ticket: &str) -> Result<TaskTicket> {
        let mut endpoint = self.url.join("ds/in/point/file/are/you/ready")?;
        endpoint.query_pairs_mut().append_pair("ticket", ticket);
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            bail!("Failed to check point task: {}", response.status());
        }
    }

    pub async fn download_point_file(&self, ticket: &str, remain: bool) -> Result<Vec<u8>> {
        let mut endpoint = self.url.join("ds/in/point/file/async")?;
        {
            let mut pairs = endpoint.query_pairs_mut();
            pairs.append_pair("ticket", ticket);
            pairs.append_pair("remain", &remain.to_string());
        }
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.bytes().await?.to_vec())
        } else {
            bail!("Failed to download point file: {}", response.status());
        }
    }

    pub async fn point_data_page(
        &self,
        ticket: &str,
        page: Option<usize>,
        page_size: Option<usize>,
    ) -> Result<serde_json::Value> {
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
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            bail!("Failed to get point page: {}", response.status());
        }
    }

    pub async fn download_point_template(
        &self,
        driver: &str,
        lang: Option<&str>,
    ) -> Result<Vec<u8>> {
        let mut endpoint = self.url.join("ds/in/point/file/template")?;
        {
            let mut pairs = endpoint.query_pairs_mut();
            pairs.append_pair("driver", driver);
            if let Some(lang) = lang {
                pairs.append_pair("lang", lang);
            }
        }
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.bytes().await?.to_vec())
        } else {
            bail!("Failed to download point template: {}", response.status());
        }
    }

    pub async fn create_agent(
        &self,
        name: &str,
        dsn: &str,
        cluster_id: &str,
        user: &str,
    ) -> Result<AgentWithToken> {
        let endpoint = self.url.join("agents")?;
        let body = AgentProps {
            name: name.to_string(),
            dsn: dsn.to_string(),
            cluster_id: cluster_id.to_string(),
            user_id: Some(user.to_string()),
        };
        let response = self.client.post(endpoint).json(&body).send().await?;
        if response.status().is_success() {
            let agent: AgentWithToken = response.json().await?;
            Ok(agent)
        } else {
            bail!("Failed to create agent: {}", response.status());
        }
    }

    pub async fn list_agents(&self, cluster_id: Option<&str>) -> Result<Vec<Agent>> {
        let mut endpoint = self.url.join("agents")?;
        if let Some(cid) = cluster_id {
            endpoint.set_query(Some(&format!("cluster_id={}", cid)));
        }
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let agents: Vec<Agent> = response.json().await?;
            Ok(agents)
        } else {
            bail!("Failed to list agents: {}", response.status());
        }
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
        let endpoint = self.url.join(&format!("agents/{}", agent_id))?;
        let response = self.client.get(endpoint).send().await?;
        if response.status().is_success() {
            let agent: Agent = response.json().await?;
            Ok(agent)
        } else if response.status().as_u16() == 404 {
            bail!("Agent {} not found", agent_id);
        } else {
            bail!("Failed to get agent: {}", response.status());
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
            bail!("Failed to update agent: {}", response.status());
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
            bail!("Failed to delete agent: {}", response.status());
        }
    }

    pub async fn data_source_is_valid(&self, dsn: &str) -> Result<bool> {
        let endpoint = self.url.join(&format!("datasources/valid?dsn={}", dsn))?;
        let response = self.client.get(endpoint).send().await?;
        Ok(response.status().is_success())
    }
}
