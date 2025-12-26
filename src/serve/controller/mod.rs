use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use anyhow::{Context as _, bail};
use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use flume::Sender;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use tracing::instrument;
use utoipa::*;
use uuid::Uuid;

use self::agent::AgentToken;
use self::trigger::Strategy;
use super::scheduler::TaskScheduler;
use crate::serve::controller::activity::Activity;
use crate::serve::rpc::utils::build_activity_batch;
use crate::serve::scheduler;
use crate::serve::scheduler::agent::AgentState;
use crate::serve::scheduler::runner::InnerState;
use crate::serve::utils::csv::encode_csv_config_file;
use taosx_core::QueryDataSourceReq;
use taosx_core::dsv::DataSourceValidation;
use taosx_core::plugins::sink::point::csv::CsvParser;
use taosx_core::plugins::transform::sample::DsSamples;
use taosx_core::runners::opc::config::OPCConfig;
use taosx_core::utils::breakpoints::{breakpoints_get_all, export_breakpoints_to_compressed_csv};
use taosx_core::utils::get_string_content_from_param_value;
use taosx_core::{DataSet, DataSetsReq, PutFileReq, Response, get_data_dir};

pub mod activity;
pub(crate) mod agent;

pub type AgentDataSetsSender = Sender<Response<Vec<DataSet>>>;
pub type DsvSender = Sender<DataSourceValidation>;
pub type StringSender = Sender<Response<String>>;

#[derive(Debug, Clone)]
pub enum AgentAction {
    /// Tuple for (TaskId, JobId, RunId)
    Run(i64, i64, Uuid, u64),
    Stop(i64, i64),
    /// Equivalent to `Suspend`.
    Cancel(i64, i64),
    ListDataSets(DataSetsReq, AgentDataSetsSender),
    /// check data source validation
    Check(String, DsvSender),
    /// get sample data
    GetSample(String, StringSender),
    /// send file to agent
    PutFile(PutFileReq, StringSender),
    /// query data source via connectors
    QueryDataSource(QueryDataSourceReq, StringSender),
}

pub(crate) struct TaskController {
    /// Task scheduler
    pub scheduler: TaskScheduler,

    /// 合法的 agent
    pub valid_agents: RwLock<HashSet<i64>>,
}

impl Debug for TaskController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskController")
            .field("scheduler", &"...")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TaskControllerRef(Arc<TaskController>);

impl TaskControllerRef {
    pub fn new(scheduler: TaskScheduler) -> Self {
        Self(Arc::new(TaskController {
            scheduler,
            valid_agents: RwLock::new(HashSet::new()),
        }))
    }
}

impl std::ops::Deref for TaskControllerRef {
    type Target = Arc<TaskController>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<TaskController> for TaskControllerRef {
    fn from(value: TaskController) -> Self {
        Self(Arc::new(value))
    }
}

impl From<Arc<TaskController>> for TaskControllerRef {
    fn from(value: Arc<TaskController>) -> Self {
        Self(value)
    }
}

impl Drop for TaskController {
    fn drop(&mut self) {
        self.scheduler.try_shutdown();
    }
}

async fn set_file_contents(dsn: &mut Dsn) -> anyhow::Result<()> {
    let dsn_clone = dsn.clone();
    let mut map = BTreeMap::new();
    for (k, v) in dsn_clone.params {
        let mut new_value = String::new();
        if v.contains("@") {
            new_value.push_str(
                get_string_content_from_param_value(&v, false, false)?
                    .unwrap_or(String::new())
                    .as_str(),
            );
        }
        let new_value = if new_value.is_empty() { v } else { new_value };
        map.insert(k, new_value);
    }
    dsn.params = map;
    Ok(())
}

// 与 set_file_contents 类似，但允许排除某些 key 不进行内容内联。
async fn set_file_contents_except(dsn: &mut Dsn, exclude: &[&str]) -> anyhow::Result<()> {
    let dsn_clone = dsn.clone();
    let mut map = BTreeMap::new();
    for (k, v) in dsn_clone.params {
        if exclude.iter().any(|ek| k == *ek) {
            map.insert(k, v); // 保持原值
            continue;
        }
        let mut new_value = String::new();
        if v.contains('@') {
            new_value.push_str(
                get_string_content_from_param_value(&v, false, false)?
                    .unwrap_or(String::new())
                    .as_str(),
            );
        }
        let new_value = if new_value.is_empty() { v } else { new_value };
        map.insert(k, new_value);
    }
    dsn.params = map;
    Ok(())
}

impl TaskController {
    pub async fn get_task(&self, task_id: i64, job_id: i64) -> Option<Task> {
        self.scheduler
            .tasks
            .read()
            .await
            .get_by_task_job_id(&(task_id, job_id))
            .map(|t| t.task.task.as_ref().clone())
    }

    pub fn add_valid_agents(&self, id: i64) {
        self.valid_agents.write().insert(id);
    }

    pub async fn list_agent_states(&self) -> HashMap<i64, AgentState> {
        let mut states = self
            .scheduler
            .global_state
            .agent_worker
            .list_agent_states()
            .await;
        let valid_agents = { self.valid_agents.read().clone() };
        for agent_id in valid_agents {
            states.entry(agent_id).or_insert(AgentState::Idle);
        }
        states
    }

    pub async fn list_task_states(&self) -> HashMap<(i64, i64), InnerState> {
        let tasks = {
            self.scheduler
                .tasks
                .read()
                .await
                .iter_by_task_job_id()
                .map(|v| (v.task_job_id, v.task.clone()))
                .collect::<Vec<_>>()
        };
        let mut states = HashMap::with_capacity(tasks.len());
        for (id, task) in tasks {
            let state = task.state.read().await.clone();
            states.insert(id, state);
        }
        states
    }

    pub fn is_agent_exists(&self, id: i64) -> bool {
        self.valid_agents.read().contains(&id)
    }

    pub fn del_valid_agent(&self, id: i64) {
        self.valid_agents.write().remove(&id);
    }

    pub fn check_agent_id(&self, token: &AgentToken) -> anyhow::Result<Option<i64>> {
        let claim = token.jwt_decode()?;
        let agent_id = claim.sub;
        Ok(self.is_agent_exists(agent_id).then_some(agent_id))
    }

    #[instrument(skip_all, fields(task.id = task.id,task.agent = task.via))]
    pub async fn start_task(
        &self,
        task: Task,
        xnoded_tx: flume::Sender<Result<RecordBatch, FlightError>>,
    ) -> anyhow::Result<()> {
        let from: Dsn = task
            .from
            .parse()
            .with_context(|| format!("Invalid data source `{}`", task.from))?;

        if let Some(via) = task.via {
            if !self.agent_alive(via).await {
                self.scheduler
                    .global_state
                    .send_task_activity(Activity::error(
                        task.id,
                        task.job_id,
                        format!("Agent {} is not alive", via),
                    ));
                bail!("Agent {} is not alive", via);
            }
            if from.driver == "pibackfill" || from.driver == "pi" {
                let file_to_send = from.params.get("transform_config_file");
                if let Some(path) = file_to_send {
                    tracing::info!("Put file to agent {}: {}", via, path);
                    self.put_file_to_agent(via, path.clone()).await?;
                }
                if from.driver == "pibackfill" {
                    let (task_id, job_id) = (task.id, task.job_id);
                    let breakpoints_file = export_breakpoints_to_compressed_csv(task_id, job_id)?;
                    if let Some(breakpoints_file) = breakpoints_file {
                        tracing::info!("Put file to agent {}: {}", via, breakpoints_file);
                        self.put_file_to_agent(via, breakpoints_file).await?;
                    } else {
                        tracing::info!("No breakpoints file to send");
                    }
                }
            } else {
                let file_to_send = from.params.get("sasl_kerberos_keytab");
                if let Some(path) = file_to_send {
                    tracing::info!("Put file to agent {}: {}", via, path);
                    self.put_file_to_agent(via, path.clone()).await?;
                }
            }
        }

        let to: Dsn = task
            .to
            .parse()
            .with_context(|| format!("Invalid target `{}`", task.to))?;

        if let (_, "taos") = (from.driver.as_str(), to.driver.as_str()) {
            TaosBuilder::from_dsn(&to)?.build().await?;
        }

        self.scheduler.push_task(task, xnoded_tx).await
    }

    pub async fn stop_task(&self, task_id: i64, job_id: i64) -> anyhow::Result<()> {
        tracing::info!(task.id = task_id, job.id = job_id, "Controller stop task");
        if let Err(err) = self.scheduler.try_stop((task_id, job_id)).await {
            match err {
                scheduler::StopError::NotFound(_) => {
                    tracing::info!(task.id = task_id, job.id = job_id, "Task not found");
                }
                scheduler::StopError::AlreadyStopped(_) => {
                    tracing::info!(task.id = task_id, job.id = job_id, "Task already stopped");
                }
                scheduler::StopError::RemoveJob(e) => {
                    return Err(anyhow::Error::new(e));
                }
            }
        }
        self.scheduler.wait_task((task_id, job_id)).await;
        Ok(())
    }

    pub async fn stop_all_task(&self) -> anyhow::Result<()> {
        let tasks = {
            self.scheduler
                .tasks
                .read()
                .await
                .iter_by_task_job_id()
                .map(|v| v.task_job_id)
                .collect::<Vec<_>>()
                .clone()
        };
        for (task_id, job_id) in tasks {
            self.stop_task(task_id, job_id).await?;
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let scheduler = self.scheduler.clone();
        let _ = tokio::time::timeout(Duration::from_secs(11), scheduler.suspend_all()).await;
        scheduler.shutdown().await;
        Ok(())
    }

    /// Check if agent is connected.
    pub async fn agent_alive(&self, agent_id: i64) -> bool {
        self.scheduler.agent_is_alive(agent_id).await
    }

    /// Agent connection with token.
    ///
    ///
    pub async fn agent_connect_with_token(
        &self,
        token: &AgentToken,
        client: Option<&SocketAddr>,
        flight_tx: &flume::Sender<Result<RecordBatch, FlightError>>,
    ) -> anyhow::Result<i64> {
        let agent = self.check_agent_id(token)?;
        let Some(agent_id) = agent else {
            bail!("The agent which token(`{token}`) bind to might be deleted")
        };
        let client = client.map(ToString::to_string).unwrap_or_default();
        let activity = Activity::agent_connect(agent_id, &client);
        let batch = build_activity_batch(activity).context("build activity batch error")?;
        flight_tx.try_send(Ok(batch)).ok();
        Ok(agent_id)
    }

    pub async fn list_datasets_via_agent_v1(
        &self,
        agent_id: i64,
        dsn: &mut Dsn,
        categories: String,
        via: Option<i64>,
    ) -> anyhow::Result<Vec<DataSet>> {
        if let Some(csv_config_file) = OPCConfig::parse_csv_config_file(dsn) {
            let new_value = encode_csv_config_file(csv_config_file).await?;
            dsn.params.insert("csv_config_file".to_string(), new_value);
        }
        set_file_contents(dsn).await?;

        let data = DataSetsReq {
            from: Some(dsn.to_string()),
            from_json: None,
            categories: vec![categories],
            via,
            offset: 0,
            pattern: None,
            limit: usize::MAX / 2 - 1,
            lang: None,
        };

        self.list_datasets_via_agent(agent_id, data).await
    }

    pub async fn list_datasets_via_agent(
        &self,
        agent_id: i64,
        req: DataSetsReq,
    ) -> anyhow::Result<Vec<DataSet>> {
        if !self.agent_alive(agent_id).await {
            bail!("Agent {} is not alive", agent_id);
        }

        let scheduler = self.scheduler.clone();
        let handle =
            tokio::spawn(async move { scheduler.list_datasets_via_agent(agent_id, req).await });
        match tokio::time::timeout(Duration::from_secs(600), handle).await {
            Ok(data) => data?.context("Retrieve datasets result error"),
            Err(err) => {
                tracing::error!("Retrieve datasets result timeout from agent");
                Err(err).context("Retrieve datasets result timeout from agent")
            }
        }
    }

    pub async fn query_data_source_via_agent(
        &self,
        request: QueryDataSourceReq,
        agent_id: i64,
    ) -> anyhow::Result<String> {
        if !self.agent_alive(agent_id).await {
            bail!("Agent {} is not alive", agent_id);
        }
        let scheduler = self.scheduler.clone();
        scheduler
            .query_datasource_via_agent(agent_id, request)
            .await
    }

    pub async fn put_file_to_agent(&self, agent_id: i64, path: String) -> anyhow::Result<()> {
        if !self.agent_alive(agent_id).await {
            bail!("Agent {} is not alive", agent_id);
        }

        let scheduler = self.scheduler.clone();
        let handle = tokio::spawn(async move {
            let path = path.trim_start_matches("@");
            let data = tokio::fs::read(path).await;
            match data {
                Ok(data) => {
                    let res = scheduler.put_file_to_agent(agent_id, path, data).await;
                    match res {
                        Ok(_) => Ok(()),
                        Err(err) => {
                            tracing::error!("Put file {path} error: {err}");
                            bail!("Put file {path} error: {err}");
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("Read file {path} error: {err}");
                    bail!("Read file {path} error: {err}");
                }
            }
        });
        handle.await?
    }

    pub async fn validate_dsn_via_agent(&self, agent: i64, dsn: &Dsn) -> DataSourceValidation {
        let scheduler = self.scheduler.clone();
        if !self.agent_alive(agent).await {
            return DataSourceValidation::invalid(
                dsn.driver.to_string(),
                format!("Agent {} is not alive", agent),
            );
        }

        let mut dsn_agent = dsn.clone();
        // 检查是否有需要发送到 agent 的文件
        let file_to_send = dsn_agent.params.get("sasl_kerberos_keytab");
        if let Some(path) = file_to_send {
            tracing::info!("Put file to agent {}: {}", agent, path);
            let _ = self.put_file_to_agent(agent, path.clone()).await;
            let _ = dsn_agent.params.insert(
                String::from("sasl_kerberos_keytab"),
                get_data_dir()
                    .join(path.trim_start_matches("@"))
                    .display()
                    .to_string(),
            );
        }
        // 避免在校验时把 csv_config_file 内联为巨大字符串：
        // 1) 如果是 @path，优先下发至 agent 并改为 agent 本地 @路径；
        if let Some(csv_cfg) = dsn_agent.params.get("csv_config_file").cloned()
            && csv_cfg.starts_with('@')
            && csv_cfg.len() > 1
        {
            tracing::info!("Put csv_config_file to agent {}: {}", agent, csv_cfg);
            let _ = self.put_file_to_agent(agent, csv_cfg.clone()).await;
            let local = get_data_dir()
                .join(csv_cfg.trim_start_matches('@'))
                .display()
                .to_string();
            dsn_agent
                .params
                .insert("csv_config_file".to_string(), format!("@{}", local));
        }
        // 2) 其余 @file 参数按原逻辑内联，但跳过 csv_config_file。
        let result = set_file_contents_except(&mut dsn_agent, &["csv_config_file"]).await;
        if let Err(err) = result {
            return DataSourceValidation::invalid(dsn.driver.to_string(), err.to_string());
        }

        let result = tokio::time::timeout(
            Duration::from_secs(600),
            scheduler.validate_dsn_via_agent(agent, dsn_agent),
        )
        .await;
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                tracing::error!("Validate dsn timeout from agent");
                return DataSourceValidation::invalid(
                    dsn.driver.to_string(),
                    "Validate dsn timeout from agent".to_string(),
                );
            }
        };
        match result {
            Ok(dsv) => dsv,
            Err(err) => DataSourceValidation::invalid(dsn.driver.to_string(), err.to_string()),
        }
    }

    pub async fn get_sample_via_agent(&self, agent: i64, dsn: String) -> anyhow::Result<DsSamples> {
        let scheduler = self.scheduler.clone();
        if !self.agent_alive(agent).await {
            bail!("Agent {} is not alive", agent);
        }
        let dsn_agent = Dsn::from_str(&dsn);
        if let Ok(dsn_agent) = dsn_agent {
            // 检查是否有需要发送到 agent 的文件
            let file_to_send = dsn_agent.params.get("sasl_kerberos_keytab");
            if let Some(path) = file_to_send {
                tracing::info!("Put file to agent {}: {}", agent, path);
                let _ = self.put_file_to_agent(agent, path.clone()).await;
            }
        }
        scheduler.get_sample_via_agent(agent, dsn).await
    }

    pub async fn send_opc_csv_to_agnet(&self, agent_id: i64, dsn: &Dsn) -> anyhow::Result<()> {
        if !self.agent_alive(agent_id).await {
            bail!("Agent {} is not alive", agent_id);
        }

        let parser = CsvParser::from_dsn(dsn)?;
        let (path, csv) = parser.read_to_string().await?;

        tracing::debug!(
            "send opc csv file to agent: {}, path: {:?}, csv: {}",
            agent_id,
            path,
            csv
        );
        let scheduler = self.scheduler.clone();
        scheduler
            .put_file_to_agent(agent_id, path.unwrap().as_str(), csv.into_bytes())
            .await?;

        Ok(())
    }
}

pub mod trigger;

/// A streaming workflow task description.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Task {
    /// Unique id for the task item.
    pub id: i64,

    pub job_id: i64,

    /// The stream data source.
    pub from: String,

    /// The target of the stream.
    pub to: String,

    /// The parser of the task stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parser: Option<serde_json::Value>,

    /// Agent Id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via: Option<i64>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    #[serde(default)]
    pub oneshot_topic: Option<String>,

    /// Task trigger events, default will be oneshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<Strategy>,

    /// break points
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakpoints: Option<String>,
}

impl From<ha_core::types::StartTaskJobParam> for Task {
    fn from(param: ha_core::types::StartTaskJobParam) -> Self {
        Self {
            id: param.task_id,
            job_id: param.job_id,
            from: param.from,
            to: param.to,
            parser: param.parser,
            via: param.via,
            oneshot_topic: None,
            trigger: None,
            breakpoints: None,
        }
    }
}

pub fn load_breakpoints(task_id: i64, job_id: i64) -> Option<String> {
    let breakpoints_res = breakpoints_get_all(task_id, job_id);
    if let Ok(breakpoints) = breakpoints_res {
        let formatted_pairs: Vec<String> = breakpoints
            .iter()
            .map(|(first, second)| format!("{}:{}", first, second))
            .collect();

        Some(formatted_pairs.join("&"))
    } else {
        None
    }
}

/// Create new task with json object.
///
/// Required properties:
///
/// - *name*: The task name.
/// - *from*/*from_json* */: The data source configuration
/// - *to*: The data sink DSN.
///
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub(crate) struct NewTask {
    stream_type: Option<String>,
    /// Task name.
    #[schema(example = "demo")]
    pub name: Option<String>,
    /// Task trigger events, default will be oneshot.
    ///
    /// For schedule trigger:
    ///
    /// - Run hourly/daily/weekly/monthly: "schedule:@daily"
    /// - Run with crontab schedule: "schedule:@daily", checkout https://crontab.guru/ for human-readable crontab.
    #[schema(example = "schedule:@daily")]
    pub trigger: Option<Strategy>,
    /// The stream data source.
    #[schema(example = "tmq+ws://localhost:6041/test?group.id=test-test2&client.id=taosx")]
    from: Option<String>,
    /// the json parameters required for task execution
    ///
    /// the parameter values vary depending on the task type
    from_json: Option<serde_json::Value>,
    /// The stream data source cluster id.
    from_cluster: Option<String>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "taos://localhost:6030/test2")]
    to: String,

    /// The parser of the task stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parser: Option<serde_json::Value>,

    /// The stream data target cluster id.
    to_cluster: Option<String>,

    /// Agent id
    via: Option<i64>,

    /// Set if the target database should be cleared before running task.
    #[serde(default)]
    clear: bool,

    /// Jobs number
    #[serde(default)]
    jobs: u16,

    /// Compression level when need (for backup only)
    #[serde(default)]
    compression_level: Option<u8>,

    /// Force to do some risking steps.
    #[serde(default)]
    force: bool,

    /// Add after_delete hook action, the string would be action name, with or without some configuration.
    ///
    /// It will do nothing if the action is not supported by a specific task case.
    after_delete: Option<String>,

    /// Labels for a task.
    ///
    /// You can use k-v style label such as `key::value` or key-only label `key`.
    ///
    /// You can filter tasks by some labels.
    labels: Option<Vec<String>>,

    /// Do not start immediately. Default is false, means start immediately after created.
    ///
    #[serde(default)]
    not_start: bool,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_csv() {
        let dsn = Dsn::from_str("csv:./ab.csv,./cd.csv?param=1").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./ab.csv,./cd.csv");
    }
}
