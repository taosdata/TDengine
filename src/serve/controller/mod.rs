use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::vec;
use std::{collections::HashMap, time::Duration};

use actix_files::NamedFile;
use agent::ActivityOrder;
use anyhow::{Context, anyhow, bail};
use async_backtrace::framed;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use flume::Sender;
use itertools::Itertools;
use lazy_static::lazy_static;
use linked_hash_map::LinkedHashMap;
use replica::{Replica, ReplicaOpts, ReplicaTask};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::pool::PoolOptions;
use sqlx::{ConnectOptions, Sqlite};
use sqlx::{FromRow, SqlitePool, migrate::Migrator, sqlite::SqliteJournalMode};
use strum::{AsRefStr, Display, EnumString, IntoStaticStr};
use taos::taos_query::tmq::Assignment;
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, IntoDsn, TaosBuilder};
use taosx_core::task_set::prelude::{HealthNotify, HealthState};
use taosx_core::utils::dsn::{dsn_to_json, json_to_dsn};
use taosx_task::TaskOpts;
use taosx_task::validate::validate_dsn;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};
use utoipa::*;
use uuid::Uuid;

use self::agent::{
    Agent, AgentActivityFilter, AgentProps, AgentStatus, AgentToken, AgentUpdates, AgentWithToken,
    LevelFilter,
};
use self::transferred::Transferred;
use self::trigger::Strategy;
use super::data_sources::DataSourceDefinition;
use super::scheduler::TaskScheduler;
use super::scheduler::agent::{AgentId, TaskId};
use super::task::ImportTasksParams;
use crate::build;
pub use crate::serve::controller::agent::Activity;
use crate::serve::rpc::encode_csv_config_file;
use crate::serve::task::{DeleteTaskParam, ExportTaskDetail, ExportTasksResult};
use taosx_core::QueryDataSourceReq;
use taosx_core::core_metrics::clear_metrics;
use taosx_core::dsv::DataSourceValidation;
use taosx_core::plugins::transform::sample::DsSamples;
use taosx_core::runners::opc::{config::OPCConfig, csv::CsvParser};
use taosx_core::utils::breakpoints::{breakpoints_get_all, export_breakpoints_to_compressed_csv};
use taosx_core::utils::get_string_content_from_param_value;
use taosx_core::{DataSet, DataSetsReq, PutFileReq, Response, get_data_dir};
use tmq_to_local::conf::BackupConfigBuilder;

pub(crate) mod agent;
pub mod license;
pub(crate) mod replica;
pub(crate) mod transferred;

mod datetime_format {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    type Target = DateTime<Utc>;

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error> where S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &Target, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = date
            .to_rfc3339_opts(SecondsFormat::Millis, true)
            .to_string();
        serializer.serialize_str(&s)
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<D>(D) -> Result<T, D::Error> where D: Deserializer
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Target, D::Error>
    where
        D: Deserializer<'de>,
    {
        Target::deserialize(deserializer)
    }
}

mod option_datetime_format {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    type Target = Option<DateTime<Utc>>;

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error> where S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(date) = date.as_ref() {
            let s = date
                .to_rfc3339_opts(SecondsFormat::Millis, true)
                .to_string();
            serializer.serialize_str(&s)
        } else {
            serializer.serialize_none()
        }
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<D>(D) -> Result<T, D::Error> where D: Deserializer
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Target, D::Error>
    where
        D: Deserializer<'de>,
    {
        Target::deserialize(deserializer)
    }
}

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

// const TASK_SELECT: &str = "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks";

pub type AgentDataSetsSender = Sender<Response<Vec<DataSet>>>;
pub type DsvSender = Sender<DataSourceValidation>;
pub type StringSender = Sender<Response<String>>;

#[derive(Debug, Clone)]
pub enum AgentAction {
    /// Tuple for (TaskId, JobId, RunId)
    Run(TaskId, Uuid, u64),
    #[allow(dead_code)]
    Stop(i64),
    /// Equivalent to `Suspend`.
    Cancel(i64),
    /// Interrupt and do nothing.
    Interrupt(i64),
    ListDataSets(DataSetsReq, AgentDataSetsSender),
    #[allow(dead_code)]
    RetrieveDataSets(DataSetsReq, Vec<DataSet>),
    /// check data source validation
    Check(String, DsvSender),
    /// get sample data
    GetSample(String, StringSender),
    /// send file to agent
    PutFile(PutFileReq, StringSender),
    /// query data source via connectors
    QueryDataSource(QueryDataSourceReq, StringSender),
}
// pub type AgentTasksReceiver = tokio::sync::broadcast::Receiver<AgentAction>;
// pub type AgentTasksSender = tokio::sync::broadcast::Sender<AgentAction>;
// pub type AgentTasksError = tokio::sync::broadcast::error::SendError<AgentAction>;
// // pub type AgentStatusChannel
// pub struct AgentTasks {
//     pub current: Arc<DashSet<i64>>,
//     pub datasets: Arc<DashMap<DataSetsReq, AgentDataSetsSender>>,
//     pub sender: AgentTasksSender,
//     pub receiver: AgentTasksReceiver,
//     pub alive: AtomicBool,
// }

// impl AgentTasks {
//     pub fn new() -> Self {
//         let (sender, receiver) = tokio::sync::broadcast::channel(10000);
//         Self {
//             current: Arc::new(DashSet::new()),
//             datasets: Arc::new(DashMap::new()),
//             sender,
//             receiver,
//             alive: AtomicBool::new(false),
//         }
//     }
//     pub fn spawn_listener(&self) -> JoinHandle<()> {
//         let mut rx = self.sender.subscribe();
//         let current = self.current.clone();
//         let datasets = self.datasets.clone();
//         tokio::spawn(async move {
//             loop {
//                 match rx.recv().await {
//                     Ok(action) => {
//                         tracing::info!("agent action: {action:?}");
//                         match action {
//                             AgentAction::Run(task) => {
//                                 current.insert(task);
//                             }
//                             AgentAction::Stop(task) => {
//                                 current.remove(&task);
//                             }
//                             AgentAction::Cancel(task) => {
//                                 current.remove(&task);
//                             }
//                             AgentAction::ListDataSets(req, sender) => {
//                                 datasets.insert(req, sender);
//                             }
//                             AgentAction::RetrieveDataSets(req, sets) => {
//                                 if let Some(sender) = datasets.remove(&req) {
//                                     let _ = sender.1.send_async(Ok(sets)).await;
//                                 }
//                             }
//                         }
//                     }
//                     Err(err) => {
//                         tracing::error!("err: {err}");
//                         break;
//                     }
//                 }
//             }
//         })
//     }

//     pub fn send(&self, action: AgentAction) -> Result<usize, AgentTasksError> {
//         self.sender.send(action) // tokio send
//     }
// }

type TaskMap = HashMap<
    i64,
    (
        tokio::task::JoinHandle<Result<(), anyhow::Error>>,
        CancellationToken,
    ),
>;

pub(crate) struct TaskController {
    pub pool: SqlitePool,
    pub tasks: RwLock<TaskMap>,
    pub secret: RwLock<Option<Bytes>>,
    /// An Agent-to-Tasks-Vector hashmap.
    // pub agent_tasks: RwLock<HashMap<i64, AgentTasks>>,
    // An Task-to-Assignments-Vector hashmap.
    #[allow(clippy::type_complexity)]
    pub offsets: RwLock<HashMap<i64, Arc<DashMap<String, Vec<Assignment>>>>>,
    // pub agent_workers: RwLock<HashMap<i64, AgentWorker>>
    // tasks: Mutex<Vec<Task>>,
    pub transferred: Transferred,
    /// Task scheduler
    pub scheduler: TaskScheduler,

    #[allow(dead_code)]
    pub ctl_alive: Arc<AtomicBool>,

    pub shutdown_notify: Arc<tokio::sync::Notify>,

    /// Max activities per task or agent.
    pub max_activities_per_entity: usize,

    pub max_activities_keep_interval: Duration,

    /// for lock, function can only be called once at a time.
    pub lock_flag: Arc<tokio::sync::Mutex<i32>>,
}

impl Debug for TaskController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskController")
            .field("pool", &self.pool)
            .field("tasks", &self.tasks)
            .field("offsets", &self.offsets)
            .field("max_activities_per_entity", &self.max_activities_per_entity)
            .field(
                "max_activities_keep_interval",
                &self.max_activities_keep_interval,
            )
            .field("scheduler", &"...")
            .finish()
    }
}

// pub(super) async fn start_all_with_schedule(controller: Arc<TaskController>) -> anyhow::Result<()> {
//     // tracing::info!("")
//     TaskControllerRef(controller)
//         .start_all_with_schedule()
//         .await
// }

#[derive(Debug, Clone)]
pub(crate) struct TaskControllerRef(Arc<TaskController>);

impl TaskControllerRef {
    pub async fn from_sqlite(
        sqlite: &str,
        scheduler: TaskScheduler,
        max_activities_per_entity: usize,
    ) -> anyhow::Result<Self> {
        TaskController::from_sqlite(sqlite, scheduler, max_activities_per_entity)
            .in_current_span()
            .await
            .map(|v| Self(Arc::new(v)))
    }

    /// Start all tasks in database.
    ///
    /// Better to call this function in a new spawned task.
    pub async fn start_all_with_schedule(&self) -> anyhow::Result<()> {
        let tasks: Vec<Task> = sqlx::query_as::<_, Task>(
            "select * from task_with_labels where status not in (?, ?, ?, ?, ?) and `deleted` != TRUE order by created_at desc")
            .bind(Status::Completed)
            .bind(Status::Failed)
            .bind(Status::Stopped)
            .bind(Status::Created)
            .bind(Status::Stopping)
            .fetch_all(&self.pool)
            .in_current_span()
            .await?;
        for mut task in tasks {
            tracing::info!(
                task.id,
                task.name,
                task.status = task.status.as_str(),
                "wake up task"
            );
            if task.from.starts_with('"') {
                task.from = task.from.trim_matches('"').replace(r#"\""#, r#"""#)
            }
            let id = task.id;
            task.load_breakpoints().await?;
            push_task_activity(
                &self.pool,
                &Activity::info(id, "Automatically wake up task.".to_string(), "waken"),
            )
            .await?;
            if let Err(err) = self.scheduler.push_task(task).await {
                tracing::error!(task.id = id, "Push task to scheduler error: {err:?}");
                push_task_activity(&self.pool, &Activity::failed(id, format!("{:#}", err))).await?;
            }
        }
        Ok(())
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

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
// #[derive(sqlx::Type)]
pub(super) enum Schedule {
    Oneshot,
    Repeated(String),
}

#[derive(Debug, thiserror::Error)]
pub enum PushTaskActivityError {
    #[error("Push task activity error: {0:#}")]
    DatabaseError(#[from] sqlx::Error),
}
async fn push_task_activity(pool: &SqlitePool, activity: &Activity) -> anyhow::Result<()> {
    #[async_backtrace::framed]
    async fn push_task_activity_once(
        pool: &SqlitePool,
        activity: &Activity,
    ) -> Result<(), PushTaskActivityError> {
        if activity.id == 0 || activity.id == -1 {
            tracing::debug!("task id is 0 or -1, ignore activity");
            return Ok(());
        }
        let exists = sqlx::query!("select id, status from tasks where id = ?", activity.id)
            .fetch_optional(pool)
            .in_current_span()
            .await?;
        if exists.is_none() {
            tracing::warn!("task {id} not found", id = activity.id);
            return Ok(());
        }
        let mut txn = pool.begin().await?;
        let record = exists.unwrap();
        if activity.status == "completed" {
            let _ = sqlx::query!(
                "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ? AND status != ?",
                activity.at,
                activity.status,
                activity.id,
                activity.status,
            )
            .execute(txn.as_mut())
            .in_current_span()
            .await?;
        }
        match activity.status.as_str() {
            // with reason
            "failed" | "stopped" => {
                sqlx::query(
                    "UPDATE tasks SET status = ?, reason = ?, finished_at = ? WHERE id = ?",
                )
                .bind(activity.status.as_str())
                .bind(activity.activity.as_str())
                .bind(activity.at)
                .bind(activity.id)
                .execute(txn.as_mut())
                .in_current_span()
                .await?;
            }
            // with reason
            "interrupted" | "suspending" | "waiting" | "waken" => {
                sqlx::query("UPDATE tasks SET status = ?, reason = ? WHERE id = ?")
                    .bind(activity.status.as_str())
                    .bind(activity.activity.as_str())
                    .bind(activity.id)
                    .execute(txn.as_mut())
                    .in_current_span()
                    .await?;
            }
            "running" => {
                if matches!(record.status.as_str(), "stopped" | "stopping") {
                    tracing::warn!(
                        "Task {} is already stopped or suspended, ignore {}",
                        activity.id,
                        activity.activity,
                    );
                    return Ok(());
                }
                sqlx::query("UPDATE tasks SET status = ?, reason = ? WHERE id = ?")
                    .bind(activity.status.as_str())
                    .bind(activity.activity.as_str())
                    .bind(activity.id)
                    .execute(txn.as_mut())
                    .in_current_span()
                    .await?;
            }
            "suspended" => {
                sqlx::query("UPDATE tasks SET status = ? WHERE id = ?")
                    .bind(activity.status.as_str())
                    .bind(activity.id)
                    .execute(txn.as_mut())
                    .in_current_span()
                    .await?;
            }
            "health" => {
                sqlx::query("UPDATE tasks SET health = ? WHERE id = ?")
                    .bind(activity.activity.as_str())
                    .bind(activity.id)
                    .execute(txn.as_mut())
                    .in_current_span()
                    .await?;
            }
            "logging" => {
                // do nothing
            }
            _ => {
                sqlx::query("UPDATE tasks SET status = ?, reason = NULL WHERE id = ?")
                    .bind(activity.status.as_str())
                    .bind(activity.id)
                    .execute(txn.as_mut())
                    .in_current_span()
                    .await?;
            }
        }
        sqlx::query(
            "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)")
            .bind(activity.id)
            .bind(activity.at)
            .bind(activity.level)
            .bind(&activity.activity)
            .bind(&activity.status)
            .bind(&activity.context)
        .execute(txn.as_mut())
        .in_current_span()
        .await?;
        txn.commit().await?;
        Ok(())
    }

    let mut retry = 5;
    loop {
        match push_task_activity_once(pool, activity).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                tracing::warn!("push task activity error: {err:#}");
                if retry == 0 {
                    Err(err)?;
                }
                retry -= 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn push_agent_activity(pool: &SqlitePool, activity: &Activity) -> anyhow::Result<()> {
    let mut status = activity.status.as_str();
    match status {
        "connected" | "disconnected" | "created" | "outdated" => {}
        "transferring" | "online" => {
            status = "connected";
        }
        "pending" | "offline" => {
            status = "disconnected";
        }
        _ => {
            status = "connected";
        }
    }
    tracing::debug!(
        "push agent activity. agent.id={}, status={}, activity={}",
        activity.id,
        status,
        activity.activity
    );
    sqlx::query(
            "INSERT INTO agent_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)"
        )
        .bind(activity.id)
        .bind(activity.at)
        .bind(activity.level)
        .bind(&activity.activity)
        .bind(status)
        .bind(&activity.context)
        .execute(pool)
        .in_current_span()
        .await?;
    Ok(())
}

/// Keep max activities per task or agent, but at least keep 10 activities.
async fn keep_max_activities(pool: &SqlitePool, max: usize) -> anyhow::Result<()> {
    tracing::info!("check and delete activities, max: {}", max);
    let max = if max > 9 { max - 1 } else { 9 } as i64;
    // tasks
    let tasks = sqlx::query_scalar::<_, i64>("select id from tasks")
        .fetch_all(pool)
        .in_current_span()
        .await?;
    for id in tasks {
        if let Some(at) = sqlx::query_scalar::<_, DateTime<Utc>>(
            "select `at` from task_activities where id = ? order by `at` desc limit 1 offset ?",
        )
        .bind(id)
        .bind(max)
        .fetch_optional(pool)
        .await?
        {
            sqlx::query("delete from task_activities where id = ? and `at` < ?")
                .bind(id)
                .bind(at)
                .execute(pool)
                .in_current_span()
                .await?;
        }
    }

    // agents
    let agents = sqlx::query_scalar::<_, i64>("select id from agents")
        .fetch_all(pool)
        .await?;
    for id in agents {
        if let Some(at) = sqlx::query_scalar::<_, DateTime<Utc>>(
            "select `at` from agent_activities where id = ? order by `at` desc limit 1 offset ?",
        )
        .bind(id)
        .bind(max)
        .fetch_optional(pool)
        .in_current_span()
        .await?
        {
            sqlx::query("delete from agent_activities where id = ? and `at` < ?")
                .bind(id)
                .bind(at)
                .execute(pool)
                .await?;
        }
    }

    // run vacuum to reclaim space
    sqlx::query("vacuum").execute(pool).await?;
    Ok(())
}

async fn database_initiate(pool: &SqlitePool) -> anyhow::Result<()> {
    let tasks = sqlx::query_scalar::<_, TaskId>(
        "select id from tasks where status in (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(Status::Running)
    .bind(Status::Waiting)
    .bind(Status::Suspending)
    .bind(Status::Queued)
    .bind(Status::Interrupted)
    .bind(Status::Ticked)
    .bind(Status::Waken)
    .fetch_all(pool)
    .in_current_span()
    .await?;
    if !tasks.is_empty() {
        tracing::info!(
            "{} tasks are in running status, set them to suspended",
            tasks.len()
        );

        sqlx::query("update tasks set status = ? where status in (?, ?, ?, ?, ?, ?)")
            .bind(Status::Suspended)
            .bind(Status::Running)
            .bind(Status::Waiting)
            .bind(Status::Suspending)
            .bind(Status::Queued)
            .bind(Status::Interrupted)
            .bind(Status::Ticked)
            .execute(pool)
            .in_current_span()
            .await?;
        for id in tasks {
            sqlx::query("insert into task_activities (`id`,`at`, `level`, `activity`, `status`) values(?, ?, ?, ?, ?)")
            .bind(id)
            .bind(Utc::now())
            .bind(LevelFilter::Info)
            .bind("Database initiated")
            .bind("suspended")
            .execute(pool)
            .in_current_span()
            .await?;
        }
    }

    sqlx::query("update tasks set status = ? where status = ?")
        .bind(Status::Stopped)
        .bind(Status::Stopping)
        .execute(pool)
        .in_current_span()
        .await?;
    Ok(())
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

impl TaskController {
    pub async fn from_sqlite(
        sqlite: &str,
        scheduler: TaskScheduler,
        max_activities_per_entity: usize,
    ) -> anyhow::Result<Self> {
        let sqlite = if !sqlite.contains(":memory:") {
            let file = sqlite.trim_start_matches("sqlite:");
            tracing::debug!("check sqlite file: {}", file);
            let path = std::path::Path::new(&file);
            if let Some(dir) = path.parent() {
                if !dir.exists() {
                    std::fs::create_dir_all(dir).context("Cannot create directory for database")?;
                }
            }
            if path.is_absolute() {
                path.to_string_lossy().to_string()
            } else {
                std::env::current_dir()
                    .context("Cannot get current directory")?
                    .join(file)
                    .to_string_lossy()
                    .to_string()
            }
        } else {
            sqlite.to_string()
        };
        let connect_options = sqlx::sqlite::SqliteConnectOptions::from_str(&sqlite)
            .with_context(|| format!("invalid sqlite file path: {sqlite}"))?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(10))
            .auto_vacuum(sqlx::sqlite::SqliteAutoVacuum::Incremental)
            .optimize_on_close(true, None)
            .log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(4))
            .statement_cache_capacity(300)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .journal_mode(SqliteJournalMode::Wal);

        // Defaults:
        // ```
        //     max_connections: 10,
        //     min_connections: 0,
        //     acquire_timeout: Duration::from_secs(30),
        //     idle_timeout: Some(Duration::from_secs(10 * 60)),
        //     max_lifetime: Some(Duration::from_secs(30 * 60)),
        //     fair: true,
        // ```
        let pool = PoolOptions::new()
            .min_connections(1)
            .max_connections(128)
            .acquire_timeout(Duration::from_secs(60))
            .idle_timeout(Some(Duration::from_secs(60 * 60)))
            .max_lifetime(Some(Duration::from_secs(60 * 60 * 24)))
            .connect_with(connect_options)
            .await
            .with_context(|| format!("invalid sqlite data file: {sqlite}"))?;
        tracing::debug!("sqlite pool created, start migration");
        MIGRATOR.run(&pool).await?;

        let notify_channel = scheduler.notify_channel();
        let pool_cloned = pool.clone();
        let ctl_alive = Arc::new(AtomicBool::new(true));
        let ctl_alive_cloned = ctl_alive.clone();
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let shutdown_notify_cloned = shutdown_notify.clone();
        tokio::spawn(
            async move {
                tracing::info!("scheduler notify listener in controller start");
                let mut rx = notify_channel;
                let pool = pool_cloned;
                loop {
                    match rx.recv().await {
                        Ok(notify) => match notify {
                            crate::serve::scheduler::SchedulerNotify::TaskActivity(task) => {
                                tracing::debug!(
                                    "task: {} {:?} {:?}",
                                    task.id,
                                    task.activity,
                                    task.status
                                );
                                if let Err(err) = push_task_activity(&pool, &task).await {
                                    tracing::error!("push task activity error: {err:?}");
                                }
                            }
                            crate::serve::scheduler::SchedulerNotify::AgentActivity(agent) => {
                                tracing::debug!(
                                    "agent: {} {:?} {:?}",
                                    agent.id,
                                    agent.activity,
                                    agent.status
                                );
                                if let Err(err) = push_agent_activity(&pool, &agent).await {
                                    tracing::error!("push task activity error: {err:?}");
                                }
                            }
                        },
                        Err(err) => match err {
                            tokio::sync::broadcast::error::RecvError::Closed => break,
                            tokio::sync::broadcast::error::RecvError::Lagged(n) => {
                                tracing::error!("scheduler notify channel lagged {n} items");
                                continue;
                            }
                        },
                    }
                }
                shutdown_notify_cloned.notify_waiters();
                ctl_alive_cloned.store(false, std::sync::atomic::Ordering::SeqCst);
                tracing::info!("scheduler notify listener in controller stop");
            }
            .instrument(tracing::info_span!(
                "scheduler_notify_listener_in_controller"
            )),
        );
        let transferred = Transferred::new(pool.clone(), Duration::from_secs(10));

        database_initiate(&pool).in_current_span().await?;

        let max_activities_pool = pool.clone();
        let max_activities_keep_interval = Duration::from_secs(10 * 60);
        tokio::task::spawn(
            async move {
                loop {
                    if let Err(err) =
                        keep_max_activities(&max_activities_pool, max_activities_per_entity).await
                    {
                        tracing::error!("keep max activities error: {err:?}");
                    }
                    tokio::time::sleep(max_activities_keep_interval).await;
                }
            }
            .instrument(tracing::info_span!("keep_max_activities")),
        );
        Ok(Self {
            pool,
            tasks: Default::default(),
            scheduler,
            secret: RwLock::new(None),
            offsets: Default::default(),
            transferred,
            ctl_alive,
            shutdown_notify,
            max_activities_per_entity,
            max_activities_keep_interval,
            lock_flag: Arc::new(tokio::sync::Mutex::new(0)),
        })
    }

    #[instrument(skip_all, fields(task.id = task.id,task.agent = task.via))]
    pub async fn start_task(&self, task: &Task) -> anyhow::Result<()> {
        let from: Dsn = task
            .from
            .parse()
            .map_err(|err| anyhow::format_err!("Invalid data source `{}`: {err}", task.from))?;

        if let Some(via) = task.via {
            if !self.agent_alive(via).await {
                self.scheduler
                    .global_state
                    .send_task_activity(Activity::error(
                        task.id,
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
                    let task_id = task.id.to_string();
                    let breakpoints_file = export_breakpoints_to_compressed_csv(task_id.as_str())?;
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
            .map_err(|err| anyhow::format_err!("Invalid target `{}`: {err}", task.to))?;

        if let (_, "taos") = (from.driver.as_str(), to.driver.as_str()) {
            TaosBuilder::from_dsn(&to)?.build().await?;
        }

        license::validate_task(&from, &to, Some(&self.pool)).await?;
        self.scheduler.push_task(task.clone()).await
    }

    pub async fn tasks(&self, mut filter: TaskFilter) -> anyhow::Result<Vec<TaskDetail>> {
        tracing::info!("list tasks");
        let condition = filter.gen_sql_conditions()?;
        let tasks = sqlx::query_as::<_, Task>(&format!(
            "select * from task_with_labels where {condition} order by created_at desc"
        ))
        .fetch_all(&self.pool)
        .in_current_span()
        .await
        .context("Database error")?;

        let mut tasks = tasks
            .iter()
            .map(|task| {
                let mut task = task.clone();
                if task.from.starts_with('"') {
                    task.from = task.from.trim_matches('"').replace(r#"\""#, r#"""#)
                }
                task
            })
            .collect_vec();

        if filter.has_labels_filter() {
            filter.filter_task_labels(&mut tasks);
        }

        let mut tasks = if let Some(in_scheduler) = filter.in_scheduler {
            let mut filtered = Vec::with_capacity(tasks.len());
            for task in tasks.into_iter() {
                if self.scheduler.exists(task.id).await == in_scheduler {
                    filtered.push(task);
                }
            }
            filtered
        } else {
            tasks
        };

        let span = tracing::trace_span!("request_tasks", "url" = "GET /tasks");
        let _guard = span.enter();
        tasks.iter_mut().for_each(|task| {
            task.backport_labels();
        });
        Ok(tasks.into_iter().map(TaskDetail::new).collect())
    }

    pub async fn tasks_count(&self, filter: TaskFilter) -> anyhow::Result<usize> {
        let tasks = self.tasks(filter).await?;
        Ok(tasks.len())
    }

    pub async fn tasks_export(&self, ids: Option<String>) -> anyhow::Result<NamedFile> {
        tracing::info!("export tasks: {:?}", ids);
        let sql = if let Some(ids) = ids {
            format!("select * from task_with_labels where id in ({ids}) order by created_at desc")
        } else {
            "select * from task_with_labels order by created_at desc".to_string()
        };
        let tasks = sqlx::query_as::<_, Task>(&sql)
            .fetch_all(&self.pool)
            .in_current_span()
            .await
            .context("Database error")?;

        // get cluster id and user from the first task
        let (cluster_id, user) = if let Some(task) = tasks.first() {
            let cluster_id = task.labels.find("cluster-id").unwrap_or_default();
            let user = task.labels.find("user").unwrap_or_default();
            (cluster_id, user)
        } else {
            ("", "")
        };
        let now = Utc::now();

        let tasks = tasks
            .iter()
            .map(|task| {
                let from_json = if let Ok(dsn) = task.from.clone().into_dsn() {
                    dsn_to_json(&dsn)
                } else {
                    serde_json::Value::Null
                };
                ExportTaskDetail {
                    id: task.id,
                    name: task.name.clone(),
                    from: from_json,
                    to: task.to.clone(),
                    parser: task.parser.clone(),
                    via: task.via,
                    jobs: task.jobs,
                    compression_level: task.compression_level,
                    oneshot_topic: task.oneshot_topic.clone(),
                    created_at: task.created_at,
                }
            })
            .collect_vec();
        let result = ExportTasksResult {
            server_version: build::TD_VERSION.to_string(),
            version: build::PKG_VERSION.to_string(),
            commit: build::COMMIT_HASH.to_string(),
            build: format!("{} {}", build::BUILD_OS, build::BUILD_TIME),
            cluster_id: cluster_id.to_string(),
            export_user: user.to_string(),
            export_time: now.to_rfc3339(),
            tasks_num: tasks.len(),
            tasks,
        };
        let result =
            serde_json::to_string_pretty(&result).context("failed to serialize result to JSON")?;

        let file_name_json = format!(
            "/tmp/taos-explorer-tasks-{}.json",
            now.format("%Y%m%d%H%M%S")
        );
        let mut file_json = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&file_name_json)
            .context(format!("failed to create file {file_name_json}"))?;
        file_json.write_all(result.as_bytes())?;
        file_json.flush().context("failed to flush JSON file")?;

        NamedFile::open(file_name_json).context("failed to open json file")
    }

    pub async fn tasks_import(&self, params: ImportTasksParams) -> anyhow::Result<usize> {
        tracing::info!("import tasks: {:?}", params);
        let tasks = params.tasks;

        let mut count = 0;
        for task in tasks {
            let task = NewTask {
                stream_type: None,
                name: task.name,
                trigger: None,
                from: None,
                from_json: Some(task.from),
                from_cluster: None,
                oneshot_topic: task.oneshot_topic,
                to: task.to,
                parser: task.parser,
                to_cluster: None,
                via: task.via,
                clear: false,
                jobs: task.jobs,
                compression_level: task.compression_level,
                force: false,
                after_delete: None,
                labels: params.labels.clone(),
                not_start: true,
            };
            self.create(task).await?;
            count += 1;
        }
        Ok(count)
    }

    #[instrument(skip_all, name = "task::create")]
    pub async fn create(&self, mut task: NewTask) -> anyhow::Result<TaskDetail> {
        tracing::info!(task.name, task.via, "create new task");

        let not_start = task.not_start;
        // let mut from: Dsn = task
        //     .from
        //     .parse()
        //     .map_err(|err| anyhow::format_err!("Invalid data source `{}`: {err}", task.from))?;
        let mut from = if let Some(from_json) = task.from_json.clone() {
            json_to_dsn(&from_json)?
        } else if let Some(from) = task.from.clone() {
            from.parse()
                .map_err(|err| anyhow::format_err!("Invalid data source `{}`: {err}", from))?
        } else {
            anyhow::bail!("from is required");
        };
        if let Some(topic) = task.oneshot_topic.as_deref() {
            if topic.len() > 64 {
                anyhow::bail!("Max length of topic name is 64, please rewrite the topic name");
            }
            from.set("use.topic.name", topic);
            tracing::info!("Set oneshot topic name: {}", topic);
        };
        if let Some(trigger) = task.trigger.as_ref() {
            // 备份计划：将 upcoming 和 interval 添加到 dsn 中
            if let Some(upcoming) = &trigger.upcoming {
                from.set("upcoming", upcoming.to_rfc3339().to_string());
            }
            if let Some((interval, _)) = &trigger.interval {
                from.set("interval", interval.to_string());
            }
        }
        let agent = if let Some(id) = task.via {
            let agent = self
                .get_agent_by_id(id)
                .await?
                .ok_or_else(|| anyhow::format_err!("Agent ID not found: {}", id))?;
            if !self.agent_alive(id).await {
                anyhow::bail!("Agent {id} is not alive");
            }
            Some(agent)
        } else {
            None
        };

        let to: Dsn = task
            .to
            .parse()
            .map_err(|err| anyhow::format_err!("Invalid target `{}`: {err}", task.to))?;

        license::validate_task(&from, &to, Some(&self.pool)).await?;
        if let ("tmq", "local") = (from.driver.as_str(), to.driver.as_str()) {
            BackupConfigBuilder::new(None, &from, &to).build().await?;
        }

        if task.via.is_none() && !task.not_start {
            validate_dsn(&from).await.ok()?;
        }

        if task.clear && to.driver == "taos" {
            taosx_core::utils::clear_database(&to)
                .await
                .with_context(|| format!("Failed to clear target database with {to}"))?;
        }
        task.patch_labels();
        let now = chrono::Utc::now();

        tracing::info!(task.name, task.via, "acquire task creation lock");
        let lock_flag = self.lock_flag.lock().await;
        tracing::info!(task.name, task.via, "got creation lock, create");
        if let Some(name) = &task.name {
            let tasks = self
                .tasks(TaskFilter {
                    name: Some(name.clone()),
                    labels: task.labels.as_ref().map(|s| s.join(",")),
                    ..Default::default()
                })
                .await?;
            if !tasks.is_empty() {
                anyhow::bail!("Task name {:?} already exists", name);
            }
        }
        let mut txn = self
            .pool
            .begin()
            .await
            .context("begin tnx error on new task")?;
        let res = sqlx::query(
            "INSERT INTO tasks (`name`, `from`, `oneshot_topic`, `to`, `jobs`, `compression_level`, \
                 `created_at`, `status`, `after_delete`, `trigger`, `via`, `parser`) \
                 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task.name)
        .bind(from.to_string())
        .bind(&task.oneshot_topic)
        .bind(&task.to)
        .bind(task.jobs)
        .bind(task.compression_level)
        .bind(now)
        .bind(&Status::Created)
        .bind(&task.after_delete)
        .bind(&task.trigger)
        .bind(task.via)
        .bind(&task.parser)
        .execute(txn.as_mut())
        .in_current_span()
        .await?;
        let id: i64 = res.last_insert_rowid();

        let backup_topic = if let ("tmq", "local") = (from.driver.as_str(), to.driver.as_str()) {
            let task_id = Some(id.to_string());
            let oneshot_topic = tmq_to_local::conf::BackupConfig::group_id(&task_id, &from, &to);
            Some(oneshot_topic)
        } else {
            None
        };

        let set_id = |from: &mut Dsn,
                      field_to_build: &str,
                      build_switch_name: &str,
                      context: &'static str|
         -> anyhow::Result<()> {
            let origin_id = from
                .get(field_to_build)
                .filter(|s| !s.trim().is_empty())
                .context(context)?;
            if from
                .get(build_switch_name)
                .is_some_and(|s| s.eq_ignore_ascii_case("true"))
            {
                from.set(
                    field_to_build,
                    format!("{}{id}{origin_id}", build::CUS_CLI_NAME),
                );
            } else {
                from.set(
                    field_to_build,
                    format!("{}{origin_id}", build::CUS_CLI_NAME),
                );
            }
            Ok(())
        };

        const CLIENT_ID: &str = "client_id";
        const CLIENT_ID_SWITCH: &str = "client_id_with_task_id";
        const CLIENT_ID_CONTEXT: &str = "client ID not set";

        const GROUP_ID: &str = "group";
        const GROUP_ID_CONTEXT: &str = "consumer group ID not set";
        const GROUP_ID_SWITCH: &str = "group_id_with_task_id";
        match from.driver.as_str() {
            "mqtt" => {
                set_id(&mut from, CLIENT_ID, CLIENT_ID_SWITCH, CLIENT_ID_CONTEXT)?;
            }
            "kafka" => {
                set_id(&mut from, GROUP_ID, GROUP_ID_SWITCH, GROUP_ID_CONTEXT)?;
                set_id(&mut from, CLIENT_ID, CLIENT_ID_SWITCH, CLIENT_ID_CONTEXT)?;
            }
            _ => {}
        }

        match backup_topic {
            None => {
                // sqlx::query("update tasks set `from` = ? where id = ?")
                //     .bind(from.to_string())
                //     .bind(id)
                //     .execute(txn.as_mut())
                //     .in_current_span()
                //     .await
                //     .context("update task error")?;
            }
            Some(oneshot_topic) => {
                // sqlx::query("update tasks set `from` = ?,`oneshot_topic` = ? where id = ?")
                //     .bind(from.to_string())
                //     .bind(oneshot_topic)
                //     .bind(id)
                //     .execute(txn.as_mut())
                //     .in_current_span()
                //     .await
                //     .context("update task error")?;
                sqlx::query("update tasks set `oneshot_topic` = ? where id = ?")
                    .bind(oneshot_topic)
                    .bind(id)
                    .execute(txn.as_mut())
                    .in_current_span()
                    .await
                    .context("update task error")?;
            }
        }
        txn.commit().await.context("commit update task txn error")?;

        tracing::info!(task.name, task.via, "release creation lock");
        drop(lock_flag);

        let path = get_data_dir();
        let path = path.join("tasks").join(id.to_string());
        if path.exists() {
            tracing::info!("task dir already exists and will be deleted");
            std::fs::remove_dir_all(&path)?;
        }

        if let Some(labels) = &task.labels {
            let values = labels
                .iter()
                .map(|label| match label.split_once("::") {
                    Some((key, value)) => format!("({id}, '{key}', '{value}')"),
                    None => format!("({id}, '{label}', NULL)"),
                })
                .join(",");
            sqlx::query(&format!("INSERT INTO labels VALUES {values}"))
                .execute(&self.pool)
                .in_current_span()
                .await?;
        }

        let context = serde_json::to_string(&task).unwrap();
        let activity = format!("create task from {}:** to {}:**", from.driver, to.driver);
        sqlx::query!(
            "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)",
            id,
            now,
            LevelFilter::Info,
            activity,
            "created",
            context
        )
        .execute(&self.pool)
        .in_current_span()
        .await?;

        // let opts = taosx::TaskOpts::try_from(task.clone())?;
        let mut task = self.get(id).await?.unwrap();
        task.backport_labels();
        task.agent = agent;

        if not_start {
            return Ok(task);
        }

        if let Err(err) = self.start_task(&task).await {
            tracing::error!(task = ?task, "Start task {id} error: {err:?}");
            let err = err.to_string();
            let now = Utc::now();
            let status = Status::Failed;
            let _ = sqlx::query!(
                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ?",
                now,
                status,
                err,
                id
            )
            .execute(&self.pool)
            .in_current_span()
            .await?;
            let context =
                json!({ "code": 0xFFFFi32, "error": err.to_string(), "task": id }).to_string();
            let activity = "Trying to start task but failed".to_string();
            sqlx::query!(
                "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)",
                id,
                now,
                LevelFilter::Error,
                activity,
                "failed",
                context
            )
            .execute(&self.pool)
            .in_current_span()
            .await?;
            task.reason.replace(err.to_string());
            task.status = status;
        }
        Ok(task)
    }

    #[instrument(skip_all, name = "task::update", fields(task.id = id))]
    pub async fn update(
        &self,
        id: i64,
        mut task: UpdateTask,
    ) -> anyhow::Result<Option<TaskDetail>> {
        let old = self
            .get(id)
            .await?
            .ok_or_else(|| anyhow!("Task not found: {}", id))?;
        tracing::info!("update task {id}: {task:?}");
        if let Some(topic) = task.oneshot_topic.as_deref() {
            if topic.len() > 64 {
                anyhow::bail!("Max length of topic name is 64, please rewrite the topic name");
            }
        }

        let from = if let Some(from_json) = task.from_json.clone() {
            let dsn = json_to_dsn(&from_json)?;
            task.from = Some(dsn.to_string());
            &dsn.to_string()
        } else {
            task.from.as_deref().unwrap_or(old.from.as_str())
        };

        // 云服务的UpdateTask没有name，需要从labels中解析
        if task.name.is_none() {
            task.name = task.labels.as_ref().and_then(|labels| {
                labels
                    .iter()
                    .find(|t| t.starts_with("name::"))
                    .map(|t| t[6..].to_string())
            });
        }

        let mut sql = Vec::new();
        macro_rules! add_bind_sql {
            ($($field:ident )*) => {
                $(if task.$field.is_some() {
                    sql.push(concat!("`", stringify!($field), "` = ?"));
                })*
            };
        }
        add_bind_sql!(name stream_type from from_cluster oneshot_topic to to_cluster jobs compression_level force parser trigger);

        sql.push("`via` = ?");

        let query = format!("UPDATE `tasks` SET {} WHERE `id` = {}", sql.join(","), id);
        let mut query = sqlx::query(&query);

        macro_rules! bind_fields {
            ($($field:ident )*) => {
                $(if let Some(field) = task.$field.as_ref() {
                    query = query.bind(field);
                })*
            };
        }
        bind_fields!(name stream_type from from_cluster oneshot_topic to to_cluster jobs compression_level force parser trigger);
        query = query.bind(task.via);

        if task.via.is_none() {
            validate_dsn(from).await.ok()?;
        }

        let res = query.execute(&self.pool).await?;

        let now = chrono::Utc::now();
        sqlx::query!(
            "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)",
            id,
            now,
            LevelFilter::Info,
            "Update task",
            "updated",
            Option::<String>::None
        )
        .execute(&self.pool)
        .in_current_span()
        .await?;

        if res.rows_affected() == 1 {
            let task = self
                .get(id)
                .await?
                .ok_or_else(|| anyhow!("Task not found: {}", id))?;
            let scheduler = self.scheduler.clone();
            let task_in_spawn = task.task.clone();
            let from: Dsn = task
                .from
                .parse()
                .map_err(|err| anyhow::format_err!("Invalid data source `{}`: {err}", task.from))?;
            if let Some(via) = task.via {
                if !self.agent_alive(via).await {
                    bail!("Agent {} is not alive", via);
                }
                // 检查是否有需要发送到 agent 的文件
                let file_to_send = from.params.get("transform_config_file");
                if let Some(path) = file_to_send {
                    tracing::info!("Put file to agent {}: {}", via, path);
                    self.put_file_to_agent(via, path.clone()).await?;
                }
                let file_to_send = from.params.get("sasl_kerberos_keytab");
                if let Some(path) = file_to_send {
                    tracing::info!("Put file to agent {}: {}", via, path);
                    self.put_file_to_agent(via, path.clone()).await?;
                }
            }

            tokio::spawn(async move {
                let _ = scheduler.stop_task(id, Duration::from_secs(60)).await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                let _ = scheduler.push_task(task_in_spawn).await;
            });
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip_all, name = "start_by_id")]
    pub async fn start(&self, id: i64) -> anyhow::Result<Option<()>> {
        tracing::info!("start task by id {}", id);
        let task = self.get(id).await?;

        if task.is_none() {
            return Ok(None);
        }

        let task = task.unwrap();

        self.start_task(&task.task).await.map(Some)
    }

    #[instrument(skip_all, name = "task::get", fields(task.id = id))]
    pub async fn get(&self, id: i64) -> anyhow::Result<Option<TaskDetail>> {
        let task = sqlx::query_as("select * from task_with_labels where id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .in_current_span()
            .await?
            .map(|mut t: Task| {
                t.backport_labels();
                t
            });
        if let Some(mut task) = task {
            if task.from.starts_with('"') {
                task.from = task.from.trim_matches('"').replace(r#"\""#, r#"""#)
            };
            task.load_breakpoints().await?;
            return Ok(Some(task.into()));
        }
        Ok(None)
    }
    #[instrument(skip_all, name = "task::delete", fields(task.id = id))]
    pub async fn delete(
        &self,
        id: i64,
        params: Option<DeleteTaskParam>,
    ) -> anyhow::Result<Option<TaskDetail>> {
        if !self.scheduler.stop_if_safe_to_delete(id).await {
            bail!("Task is in scheduler, please stop it first");
        }
        let now = Utc::now();
        let res = sqlx::query_as_unchecked!(
            Task,
            "UPDATE tasks SET `deleted` = TRUE, `last_modified_at` = ? where id = ?",
            now,
            id
        )
        .execute(&self.pool)
        .in_current_span()
        .await?;
        if res.rows_affected() == 1 {
            tracing::info!("successfully deleted task by id {id}");
        }

        let task: Option<Task> = sqlx::query_as("select * from task_with_labels where id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .in_current_span()
            .await?;
        if task.is_none() {
            return Ok(None);
        }

        let mut task = task.unwrap();
        if task.from.starts_with('"') {
            task.from = task.from.trim_matches('"').replace(r#"\""#, r#"""#)
        }
        task.backport_labels();
        if let Some(params) = params {
            task.after_delete = params.after_delete;
        }

        let task_out = task.clone();
        let pool = self.pool.clone();
        let scheduler = self.scheduler.clone();
        tokio::spawn(
            async move {
                scheduler.wait_task(task.id).await;
                tracing::info!("task {id} successfully stopped");
                if let Some(topic) = task.oneshot_topic.as_deref() {
                    tracing::info!("drop oneshot topic: {}", topic);
                    let mut dsn: Dsn = task.from.parse()?;
                    let _ = dsn.subject.take();
                    let builder =
                        TaosBuilder::from_dsn(dsn).context("cannot drop oneshot topic")?;
                    let taos = builder.build().await.context("cannot drop oneshot topic")?;
                    let mut retries = 0;
                    loop {
                        if retries > 20 {
                            tracing::error!("can not drop topic {topic}");
                            break;
                        }
                        if let Err(_err) = taos
                            .exec(format!("drop topic if exists {topic}"))
                            .in_current_span()
                            .await
                        {
                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        } else {
                            break;
                        }
                    }
                }

                if let Some(action) = task.after_delete.as_deref() {
                    if action == "clear" {
                        let from = task.from.parse::<Dsn>()?;
                        let to = task.to.parse::<Dsn>()?;
                        match (from.driver.as_str(), to.driver.as_str()) {
                            ("tmq", "local") => {
                                if let Some(path) = to.path.as_deref() {
                                    let dir = std::path::Path::new(path)
                                        .join(task.id.to_string())
                                        .canonicalize()
                                        .map_err(|err| {
                                            anyhow::Error::from(err).context(format!(
                                                "failed to canonicalize path: {}, task: {}",
                                                path, task.id
                                            ))
                                        })?
                                        .display()
                                        .to_string();
                                    tokio::spawn(async move {
                                        // 删除目录下的所有文件
                                        taosx_core::utils::clear_local_dir(dir.as_str()).await
                                    });
                                }
                            }
                            (_, "local") => {
                                let dsn: Dsn = task.to.parse()?;
                                // std::mem::drop(task);
                                tokio::spawn(
                                    async move { taosx_core::utils::clear_local(&dsn).await },
                                );
                            }
                            _ => {}
                        }
                    }
                }
                sqlx::query!("DELETE FROM tasks where id = ?", id)
                    .execute(&pool)
                    .in_current_span()
                    .await?;

                let from: Dsn = task.from.parse()?;
                let to: Dsn = task.to.parse()?;
                let (tx, _rx) = flume::unbounded();
                let opts = TaskOpts {
                    from,
                    transform: vec![],
                    to,
                    parser: None,
                    health: None,
                    cancel: Default::default(),
                    with_agent: None,
                    breakpoints: None,
                    task_id: Some(task.id.to_string()),
                    notify: tx,
                };
                opts.delete_task().await?;

                // breakpoints_clear
                let task_id = id.to_string();
                tokio::task::spawn_blocking(move || {
                    taosx_core::utils::breakpoints::breakpoints_clear(&task_id)
                })
                .await??;

                // metrics_clear
                clear_metrics(id).await;

                tracing::info!("successfully deleted task by id {id}");
                anyhow::Ok(())
            }
            .in_current_span(),
        );
        Ok(Some(task_out.into()))
    }

    #[instrument(skip_all, name = "task::stop", fields(task.id = id))]
    pub async fn stop(&self, id: i64) -> anyhow::Result<Option<()>> {
        tracing::info!("Stop task by id {id}");
        if let Err(err) = self.scheduler.try_stop(id).await {
            match err {
                crate::serve::scheduler::StopError::NotFound(_) => {
                    tracing::info!("Task {id} not scheduler");
                    sqlx::query("UPDATE tasks SET status = ? WHERE id = ?")
                        .bind(Status::Stopped)
                        .bind(id)
                        .execute(&self.pool)
                        .in_current_span()
                        .await?;
                    return Ok(Some(()));
                }
                err => Err(err)?,
            }
        }
        let scheduler = self.scheduler.clone();
        let handle = tokio::spawn(
            async move {
                scheduler.wait_task(id).await;
                tracing::info!("task {id} successfully stopped");
            }
            .in_current_span(),
        );
        match tokio::time::timeout(Duration::from_secs(2), handle).await {
            Ok(Ok(_)) => Ok(Some(())),
            Ok(Err(err)) => Err(anyhow!("Spawn task {id} join error: {err}")),
            Err(_) => {
                tracing::warn!("task {id} stop timeout");
                Ok(Some(()))
            }
        }
    }

    pub async fn task_activities(
        &self,
        id: i64,
        filter: &AgentActivityFilter,
    ) -> anyhow::Result<Vec<Activity>> {
        let cond = filter.condition();
        let sql = format!("select * from task_activities where `id` = {id} {cond}");
        let items = sqlx::query_as(&sql)
            .fetch_all(&self.pool)
            .in_current_span()
            .await?;
        Ok(items)
    }

    pub async fn all_tasks_activities(&self) -> anyhow::Result<Vec<Activity>> {
        let cond = AgentActivityFilter {
            since: None,
            level: None,
            limit: Some(10000),
            order: Some(ActivityOrder::Asc),
        }
        .condition();
        let sql = format!(
            "select * from (select *, row_number() over (partition by id order by at desc) as rn from task_activities) r where r.rn<=5 {cond}"
        );
        let items = sqlx::query_as(&sql)
            .fetch_all(&self.pool)
            .in_current_span()
            .await?;
        Ok(items)
    }

    pub async fn _suspend_all(&self) -> anyhow::Result<()> {
        self.scheduler.suspend_all().await;
        Ok(())
    }

    pub async fn _clear(&self) -> anyhow::Result<()> {
        self.scheduler.suspend_all().await;
        Ok(())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let scheduler = self.scheduler.clone();
        let _ = tokio::time::timeout(Duration::from_secs(11), scheduler.suspend_all()).await;
        scheduler.shutdown().await;
        let _ = tokio::time::timeout(Duration::from_secs(2), self.shutdown_notify.notified()).await;

        // Set all running status to suspended?
        Ok(())
    }
    pub async fn offsets(&self, id: i64) -> anyhow::Result<Option<serde_json::Value>> {
        let task = self.get(id).await?;
        match task {
            Some(task) => {
                // dbg!(&task);
                let from = task.from.parse::<Dsn>()?;
                let to = task.to.parse::<Dsn>()?;
                match (from.driver.as_str(), to.driver.as_str()) {
                    ("tmq" | "sync", _) => {
                        let offsets = self.tmq_offsets(id).await?;
                        Ok(offsets)
                    }
                    ("taos", "taos") => {
                        let offsets = self.taos_offsets(id).await?;
                        Ok(offsets)
                    }
                    ("influxdb", "taos") => {
                        let offsets = self.influxdb_offsets(id).await?;
                        Ok(offsets)
                    }
                    ("opentsdb", "taos") => {
                        let offsets = self.opentsdb_offsets(id).await?;
                        Ok(offsets)
                    }
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub async fn taos_offsets(&self, id: i64) -> anyhow::Result<Option<serde_json::Value>> {
        let offsets = breakpoints_get_all(id.to_string().as_str())?;
        // dbg!(&offsets);
        let res = serde_json::to_value(offsets)?;
        Ok(Some(res))
    }

    pub async fn influxdb_offsets(&self, id: i64) -> anyhow::Result<Option<serde_json::Value>> {
        let offsets = breakpoints_get_all(id.to_string().as_str())?;
        // dbg!(&offsets);
        let res = serde_json::to_value(offsets)?;
        Ok(Some(res))
    }

    pub async fn opentsdb_offsets(&self, id: i64) -> anyhow::Result<Option<serde_json::Value>> {
        let offsets = breakpoints_get_all(id.to_string().as_str())?;
        // dbg!(&offsets);
        let res = serde_json::to_value(offsets)?;
        Ok(Some(res))
    }

    pub async fn tmq_offsets(&self, id: i64) -> anyhow::Result<Option<serde_json::Value>> {
        let from = self.get(id).await?;
        if let Some(task) = from {
            let mut from = task.from.parse::<Dsn>()?;
            if from.driver == *"sync" {
                from.driver = "tmq".to_string();
            }
            let offsets = tmq_to_td::tmq_offsets(from).await?;
            let res = serde_json::to_value(&offsets)?;
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    pub async fn find_agent_by_name_and_cluster_id(
        &self,
        name: &str,
        cluster_id: Option<&str>,
        id: Option<usize>,
    ) -> anyhow::Result<Vec<Agent>> {
        let mut sql = if id.is_some() {
            format!(
                "select * from agents where name = '{}' and id != '{}'",
                name,
                id.unwrap()
            )
        } else {
            format!("select * from agents where name = '{}'", name)
        };
        if cluster_id.is_some() {
            sql.push_str(format!(" and cluster_id = '{}'", cluster_id.unwrap()).as_str());
        }
        let result: Vec<Agent> = sqlx::query_as(sql.as_str())
            .fetch_all(&self.pool)
            .in_current_span()
            .await?;
        Ok(result)
    }

    pub async fn create_agent(&self, agent: AgentProps) -> anyhow::Result<AgentWithToken> {
        let result = self
            .find_agent_by_name_and_cluster_id(&agent.name, Some(&agent.cluster_id), None)
            .await?;
        if !result.is_empty() {
            anyhow::bail!("agent name has existed");
        }

        let res = sqlx::query(
            "INSERT INTO agents (`dsn`, `name`, `cluster_id`, `user_id`, created_at) \
            VALUES(?, ?, ?, ?, ?)",
        )
        .bind(&agent.dsn)
        .bind(&agent.name)
        .bind(&agent.cluster_id)
        .bind(&agent.user_id)
        .bind(Utc::now())
        .execute(&self.pool)
        .in_current_span()
        .await?;
        let id = res.last_insert_rowid();
        let activity = Activity::new::<String>(
            id,
            Utc::now(),
            LevelFilter::Info,
            format!("Agent {} is created successfully", agent.name),
            "created",
            None,
        );
        self.push_agent_activity(activity).await?;
        let secret = self.jwt_secret().await?;
        self.get_agent_by_id(id)
            .await
            .map(|agent| agent.unwrap().with_token(secret))
    }

    pub async fn get_agents(&self, filter: AgentFilter) -> anyhow::Result<Vec<Agent>> {
        let sql = match filter.to_sql_condition() {
            Some(cond) => format!("select * from agents where {cond}"),
            None => "select * from agents".to_string(),
        };
        let mut agents: Vec<Agent> = sqlx::query_as(&sql)
            .fetch_all(&self.pool)
            .in_current_span()
            .await?;
        for agent in &mut agents {
            agent.status.replace(self.agent_status(agent.id).await);
        }
        Ok(agents)
    }

    pub async fn get_agent_with_token(&self, token: &AgentToken) -> anyhow::Result<Option<Agent>> {
        let claims = token.jwt_decode(self.jwt_secret().await?)?;
        let agent = self.get_agent_by_id(claims.sub).await?;
        Ok(agent)
    }

    pub async fn get_agent_by_id(&self, agent_id: i64) -> anyhow::Result<Option<Agent>> {
        let sql = format!("select * from agents where id = {agent_id}");
        let mut agent: Option<Agent> = sqlx::query_as(&sql).fetch_optional(&self.pool).await?;
        if let Some(agent) = &mut agent {
            agent.status.replace(self.agent_status(agent_id).await);
        }
        Ok(agent)
    }
    /// Check if agent is connected.
    pub async fn agent_alive(&self, agent_id: i64) -> bool {
        self.scheduler.agent_is_alive(agent_id).await
    }

    pub async fn agent_status(&self, agent_id: i64) -> AgentStatus {
        let activities = sqlx::query_scalar!(
            "select count(*) from agent_activities where id = ? limit 2",
            agent_id
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or_default();
        if activities == 1 {
            return AgentStatus::Created;
        }
        if self.scheduler.agent_is_alive(agent_id).await {
            AgentStatus::Connected
        } else {
            AgentStatus::Disconnected
        }
    }
    /// Update agent activities.
    pub async fn push_agent_activity(&self, activity: Activity) -> anyhow::Result<()> {
        // if activity.status == "pending" {
        //     self.agent_tasks.write().await.remove(&activity.id);
        // } else if activity.status == "busy" {
        //     activity.status = "transferring".to_string();
        // }
        push_agent_activity(&self.pool, &activity).await
    }

    /// Agent connection with token.
    ///
    ///
    pub async fn agent_connect_with_token(
        &self,
        token: &AgentToken,
        client: Option<&SocketAddr>, // Remote address of the request
    ) -> anyhow::Result<Agent> {
        let agent = self.get_agent_with_token(token).await?;
        if let Some(agent) = agent {
            let client = client.map(ToString::to_string).unwrap_or_default();
            let activity = Activity::new(
                agent.id,
                Utc::now(),
                LevelFilter::Info,
                format!("Agent is connected with client addr {client}"),
                "idle",
                json!({ "client": client }),
            );
            push_agent_activity(&self.pool, &activity).await?;
            Ok(agent)
        } else {
            bail!("The agent which token(`{token}`) bind to might be deleted")
        }
    }

    pub async fn update_agent(
        &self,
        agent_id: i64,
        update: AgentUpdates,
    ) -> anyhow::Result<Option<AgentWithToken>> {
        let name = update.name.as_str();
        let result = self
            .find_agent_by_name_and_cluster_id(name, None, Some(agent_id as usize))
            .await?;
        if !result.is_empty() {
            anyhow::bail!("Agent name {} exists", name);
        }
        // let sql = update.update_agent_with(agent_id);
        sqlx::query("UPDATE agents SET `name` = ? WHERE id = ?")
            .bind(name)
            .bind(agent_id)
            .execute(&self.pool)
            .in_current_span()
            .await?;
        let secret = self.jwt_secret().await?;
        Ok(self
            .get_agent_by_id(agent_id)
            .in_current_span()
            .await?
            .map(|a| a.with_token(&secret)))
    }

    pub async fn delete_agent(&self, agent_id: i64) -> anyhow::Result<()> {
        let ids = sqlx::query_as::<_, (i64,)>("select id from tasks where via = ?")
            .bind(agent_id)
            .fetch_all(&self.pool)
            .in_current_span()
            .await?;
        if !ids.is_empty() {
            anyhow::bail!("should delete associated tasks before delete agent");
        }

        sqlx::query("delete from agent_activities where id = ?")
            .bind(agent_id)
            .execute(&self.pool)
            .in_current_span()
            .await?;
        tracing::info!("Deleted agent with id {agent_id}");

        sqlx::query("delete from agents where id = ?")
            .bind(agent_id)
            .execute(&self.pool)
            .in_current_span()
            .await?;

        Ok(())
    }

    pub async fn jwt_secret(&self) -> anyhow::Result<Bytes> {
        let guard = self.secret.read().await;
        let secret = guard.as_ref().map(Clone::clone);
        drop(guard);
        if let Some(secret) = secret {
            Ok(secret)
        } else {
            let mut guard = self.secret.write().await;
            const SECRET_PREFIX: &str = "XaNeGt";
            if guard.is_none() {
                let secret: Option<String> = sqlx::query_scalar("select `secret` from `secret`")
                    .fetch_optional(&self.pool)
                    .in_current_span()
                    .await?;
                let secret = if let Some(value) = secret {
                    value
                } else {
                    use rand::distributions::{Alphanumeric, DistString};
                    let random = Alphanumeric.sample_string(&mut rand::thread_rng(), 64);

                    sqlx::query(&format!("insert into `secret` values('{random}')"))
                        .execute(&self.pool)
                        .in_current_span()
                        .await?;
                    random
                };
                guard.replace(Bytes::from(format!("{SECRET_PREFIX}-ZiTsEn-{secret}")));
                Ok(guard.as_ref().unwrap().clone())
            } else {
                Ok(guard.as_ref().unwrap().clone())
            }
        }
    }

    pub async fn get_tasks_of_agent(&self, agent_id: i64) -> anyhow::Result<Vec<TaskDetail>> {
        self.tasks(TaskFilter::default().via(agent_id)).await
    }

    pub async fn list_datasets_via_agent_v1(
        &self,
        agent_id: i64,
        dsn: &mut Dsn,
        categories: String,
        via: Option<i64>,
    ) -> anyhow::Result<Vec<DataSet>> {
        if let Some(csv_config_file) = OPCConfig::parse_csv_config_file(dsn) {
            let new_value = encode_csv_config_file(csv_config_file)?;
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
        let result = set_file_contents(&mut dsn_agent).await;
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

    pub async fn cluster_transferred(
        &self,
        cluster_id: i64,
    ) -> anyhow::Result<Vec<ConnectorTransferred>> {
        let vec: Vec<ConnectorTransferred> =
            sqlx::query_as("select * from connector_transferred where cluster_id = ?")
                .bind(cluster_id)
                .fetch_all(&self.pool)
                .in_current_span()
                .await?;
        Ok(vec)
    }

    #[instrument(skip_all)]
    pub async fn get_task_summaries(&self, interval: u64) -> (i32, i32, i32) {
        let interval = Duration::from_secs(interval);
        let finished_at = Utc::now() - interval;
        let running_tasks_count = sqlx::query_scalar!(
            "select count(*) from tasks where status = ?",
            Status::Running
        )
        .fetch_one(&self.pool)
        .in_current_span()
        .await
        .unwrap_or_default();
        // count tasks completed in last 10 seconds
        let completed_tasks_count = sqlx::query_scalar!(
            "select count(*) from tasks where status = ? and finished_at > ?",
            Status::Completed,
            finished_at,
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or_default();
        // count failed tasks in last 10 seconds
        let failed_tasks_count = sqlx::query_scalar!(
            "select count(*) from tasks where status = ? and finished_at > ?",
            Status::Failed,
            finished_at,
        )
        .fetch_one(&self.pool)
        .in_current_span()
        .await
        .unwrap_or_default();
        (
            running_tasks_count as i32,
            completed_tasks_count as i32,
            failed_tasks_count as i32,
        )
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

    async fn search_replicas_by_source_sink(
        &self,
        replica: &Replica,
    ) -> anyhow::Result<Option<Vec<ReplicaTask>>> {
        let labels = replica.labels_with_source_sink();
        let tasks = self
            .tasks(TaskFilter::default().with_joined_labels(labels))
            .await?;

        if tasks.is_empty() {
            return Ok(None);
        }

        let replicas = tasks
            .into_iter()
            .map(ReplicaTask::from_task)
            .try_collect::<_, Vec<_>, _>()?;

        Ok(Some(replicas))
    }

    async fn list_replicas(&self) -> anyhow::Result<Vec<(Replica, Vec<ReplicaTask>)>> {
        let labels = "type::replica".to_string();
        let tasks = self
            .tasks(TaskFilter::default().with_joined_labels(labels))
            .await?;

        if tasks.is_empty() {
            return Ok(Vec::new());
        }
        let replicas: Vec<_> = tasks
            .into_iter()
            .map(ReplicaTask::from_task)
            .try_collect::<_, Vec<_>, _>()?
            .into_iter()
            .flat_map(|task| task.replica().ok().map(|r| (r, task)))
            .into_group_map()
            .into_iter()
            .collect();

        Ok(replicas)
    }

    /// Start replica monitor.
    pub async fn start_replica_monitor(
        self: &Arc<Self>,
        mut opts: ReplicaOpts,
    ) -> anyhow::Result<ReplicaOpts> {
        let mut trans = self.pool.begin().await?;
        let lock = trans.lock_handle().await?;
        // Step 1: check if the replica exists
        let replica = sqlx::query_as::<_, ReplicaOpts>(
            "select * from replicas where source = ? and sink = ?",
        )
        .bind(&opts.source)
        .bind(&opts.sink)
        .fetch_optional(&self.pool)
        .await?;

        let mut should_update = true;
        if let Some(mut exist) = replica {
            let request_rid = opts.id.take();
            let id = exist.id.take().expect("id should not be None");
            if let Some(request_rid) = request_rid.as_deref() {
                if request_rid != id {
                    bail!(
                        "Replica with source: {}, sink: {} already exists as id: {}",
                        exist.source,
                        exist.sink,
                        id,
                    );
                }
            }
            if exist == opts {
                if MONITOR_TASK
                    .read()
                    .await
                    .get(&id)
                    .is_some_and(|h| !h.is_finished())
                {
                    tracing::info!("No changes, an exist monitor task has been running");
                    return Ok(exist);
                }
                should_update = false;
            }
            opts = exist;
            opts.id = Some(id);
        }

        // Step 2: make sure the replica is not running
        let mut replica = opts.build_replica()?;
        let tasks = self.search_replicas_by_source_sink(&replica).await?;

        if let Some(id) = tasks
            .as_deref()
            .and_then(|tasks| tasks.first().map(|task| task.replica_id()))
        {
            // If source/sink replica exists, check if the id is the same.
            if id != replica.id() {
                bail!(
                    "Replica with source: {}, sink: {} already exists as id: {}",
                    replica.canonical_source(),
                    replica.canonical_sink(),
                    id,
                );
            }
            if opts.id.is_none() {
                opts.id = Some(id.to_string());
            }
        } else if opts.id.is_none() {
            // If id is not set, update the id with the existing replicas to avoid id conflict.
            let replicas = self
                .list_replicas()
                .await?
                .into_iter()
                .map(|(r, _)| r)
                .collect::<Vec<_>>();
            replica.update_id_with(&replicas);
            opts.id.replace(replica.id().to_string());
        }
        // Step 3: abort if the monitor task is running.
        {
            if let Some(task) = MONITOR_TASK.write().await.remove(replica.id()) {
                task.abort()
            }
        }

        let id = replica.id();
        let now = chrono::Local::now();
        if should_update {
            // Step 3: insert or update the replica
            sqlx::query::<Sqlite>(
            "INSERT INTO replicas (`id`, `source`, `sink`, `topic_prefix`, `group`, \
                `keep_topic_after_remove`, `new_databases_checking_interval`, `created_at`, `updated_at`) \
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET \
                `topic_prefix` = ?, `group` = ?, `keep_topic_after_remove` = ?, \
                `new_databases_checking_interval` = ?, `updated_at` = ?",
            )
            .bind(id)
            .bind(&opts.source)
            .bind(&opts.sink)
            .bind(now)
            .bind(now)
            .bind(&opts.topic_prefix)
            .bind(&opts.group)
            .bind(opts.keep_topic_after_remove)
            .bind(opts.new_databases_checking_interval)
            .bind(&opts.topic_prefix)
            .bind(&opts.group)
            .bind(opts.keep_topic_after_remove)
            .bind(opts.new_databases_checking_interval)
            .bind(now)
            .execute(&self.pool).await.context("Update replicas failed")?;
        }
        drop(lock);
        drop(trans);

        let inner_opts = opts.clone();
        let controller = TaskControllerRef(self.clone());
        tokio::spawn(start_replica_monitor_with(
            controller, replica, inner_opts, tasks,
        ));
        Ok(opts)
    }

    /// Stop replica monitor by id.
    pub async fn stop_replica_monitor(&self, id: &str) -> anyhow::Result<()> {
        if let Some(task) = MONITOR_TASK.write().await.remove(id) {
            task.abort()
        };
        Ok(())
    }

    /// Remove replica monitor by id.
    pub async fn remove_replica_monitor(&self, id: &str) -> anyhow::Result<Option<ReplicaOpts>> {
        self.stop_replica_monitor(id).await?;
        let opts = sqlx::query_as::<_, ReplicaOpts>("select * from replicas where id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        if opts.is_none() {
            return Ok(None);
        }
        sqlx::query("delete from replicas where id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(opts)
    }

    /// Start replica monitor by id.
    pub async fn start_replica_monitor_by_id(
        self: &Arc<Self>,
        id: &str,
        new_databases_checking_interval: Option<u32>,
    ) -> anyhow::Result<Option<ReplicaOpts>> {
        let opts = sqlx::query_as::<_, ReplicaOpts>("select * from replicas where id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        if opts.is_none() {
            return Ok(None);
        }
        let mut opts = opts.unwrap();
        if let Some(v) = new_databases_checking_interval {
            opts.new_databases_checking_interval.replace(v);
        }
        self.start_replica_monitor(opts).await.map(Some)
    }

    pub async fn start_all_replicas_monitor(self: &Arc<Self>) -> anyhow::Result<()> {
        let ids: Vec<String> = sqlx::query_scalar("select id from replicas where id = ?")
            .fetch_all(&self.pool)
            .await?;
        let mut errors = anyhow::anyhow!("Start replicas monitor error");
        let mut has_errors = false;
        for id in ids {
            if let Err(err) = self.start_replica_monitor_by_id(&id, None).await {
                errors = errors.context(format!("Start replica {id} error: {err:#}"));
                has_errors = true;
            }
        }
        if has_errors { Err(errors) } else { Ok(()) }
    }
}

lazy_static! {
    static ref MONITOR_TASK: RwLock<HashMap<String, JoinHandle<()>>> = RwLock::new(HashMap::new());
}
#[framed]
async fn start_replica_monitor_with(
    ctl: TaskControllerRef,
    replica: Replica,
    opts: ReplicaOpts,
    tasks: Option<Vec<ReplicaTask>>,
) {
    let id = replica.id().to_string();
    let handle = tokio::spawn(async_backtrace::frame!(async move {
        let replica = replica;
        let mut interval = tokio::time::interval(Duration::from_secs(
            opts.new_databases_checking_interval() as _,
        ));
        let mut set = HashSet::new();
        if let Some(tasks) = tasks {
            for task in tasks {
                if set.contains(&task.database) {
                    continue;
                }
                set.insert(task.database.clone());
            }
        }

        loop {
            let databases = replica.databases(Duration::from_secs(30)).await;
            if let Ok(databases) = databases {
                for database in databases {
                    if set.contains(&database) {
                        continue;
                    }
                    let task = replica.build_task(&opts, &database, &database);
                    tracing::info!("Create replica task: {:?}", task);
                    if ctl.create(task).await.is_ok() {
                        set.insert(database);
                    }
                }
            }
            interval.tick().await;
        }
    }));
    MONITOR_TASK.write().await.insert(id, handle);
}
#[derive(Debug, Default, Serialize, ToSchema, FromRow)]
pub struct ConnectorTransferred {
    pub connector: String,
    pub tables: i32,
    pub records: i64,
    pub points: i64,
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct AgentFilter {
    pub cluster_id: Option<String>,
    user_id: Option<String>,
}

impl AgentFilter {
    pub fn default() -> Self {
        Self {
            cluster_id: None,
            user_id: None,
        }
    }

    pub fn to_sql_condition(&self) -> Option<String> {
        match (self.cluster_id.as_ref(), self.user_id.as_ref()) {
            (None, None) => None,
            (c, u) => Some(
                c.into_iter()
                    .map(|s| format!("`cluster_id` = '{s}'"))
                    .chain(u.map(|s| format!("`user_id` = '{s}'")))
                    .join(" AND "),
            ),
        }
    }
}

/// State.
///
/// Initial state: Created.
///
/// Final states:
/// - Completed: Oneshot task finished successfully.
/// - Failed: Oneshot task finished with some error.
/// - Stopped: Any task stopped by manual
///
/// ```
/// Created -> Running
/// Running -> Completed
/// Running -> Failed
/// Running -> Interrupted
/// Interrupted -> Running
/// Running -> Stopped
/// Running -> Cancelled
/// ```
#[derive(
    Serialize,
    Deserialize,
    ToSchema,
    Clone,
    Debug,
    PartialEq,
    Eq,
    EnumString,
    AsRefStr,
    Display,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Status {
    /// Created by API.
    Created,
    /// Clear target database.
    Cleared,
    /// Started by start API.
    Started,
    /// In running state.
    Running,
    /// Cancelled tasks, this might be stopped or not.
    Cancelled,
    /// Cronjob ticking finished.
    Ticked,
    /// Task has been finished.
    Completed,
    /// Task completed with error.
    Failed,
    /// For never stop task, it's not in service, but will retry.
    Interrupted,
    /// Manually stopped by API, but not finished stopping background jobs.
    Stopping,
    /// Manually stopped by API.
    Stopped,
    /// Nothing, waiting for some reason.
    Waiting,
    /// In suspending state.
    Suspending,
    /// Task is suspended by controller/agent.
    Suspended,
    /// Task paused manually by user.
    Paused,
    /// Task is queued.
    Queued,
    /// Task is scheduled.
    Scheduled,
    /// Task is resuming.
    Resuming,
    /// Task is resumed.
    Resumed,
    /// Waken
    Waken,
    /// Task is in unknown state.
    #[strum(default)]
    #[serde(untagged)]
    __NonExhaustive(String),
}

impl PartialEq<str> for Status {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}
impl PartialEq<Status> for &Status {
    fn eq(&self, other: &Status) -> bool {
        *self == other
    }
}

impl Status {
    fn as_str(&self) -> &'static str {
        self.into()
    }

    #[allow(dead_code)]
    fn in_final_state(&self) -> bool {
        matches!(
            self,
            Self::Stopped
                | Self::Interrupted
                | Self::Cancelled
                | Self::Failed
                | Self::Created
                | Self::Suspended
        )
    }
}

impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for Status
where
    &'r str: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::database::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let v: &'r str = sqlx::Decode::decode(value)?;
        Self::from_str(v).map_err(|err| Box::new(err) as _)
    }
}

impl<'q, DB: sqlx::Database> sqlx::encode::Encode<'q, DB> for Status
where
    &'q str: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        self.as_str().encode(buf as _)
    }

    fn size_hint(&self) -> usize {
        self.as_str().size_hint()
    }
}

impl<'t, DB: sqlx::Database> sqlx::Type<DB> for Status
where
    &'t str: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <&'t str as sqlx::Type<DB>>::type_info()
    }
}

pub mod trigger;

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct TaskWithAgent {
    #[serde(flatten)]
    task: Task,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent: Option<Agent>,
}

/// A streaming workflow task description.
#[derive(
    Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
)]
pub struct Task {
    /// Unique id for the task item.
    #[schema(read_only, example = 1)]
    pub id: i64,

    /// Task stream data type. **Deprecated**, use labels instead.
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    stream_type: Option<String>,

    /// The stream data source.
    #[schema(example = "tmq:///test")]
    pub from: String,

    /// Cluster identifier for stream from. **Deprecated**, use labels instead.
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    from_cluster: Option<String>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    #[serde(default)]
    pub oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "local:/path/to/backup/test")]
    pub to: String,

    /// The parser of the task stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    pub parser: Option<serde_json::Value>,

    /// Cluster identifier for stream to. **Deprecated**, use labels instead.
    #[schema(example = "null")]
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[sqlx(default)]
    to_cluster: Option<String>,

    /// Number of jobs for task running.
    #[schema(example = 0)]
    #[serde(default)]
    jobs: u16,

    /// Agent Id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via: Option<i64>,

    /// Compression level when need (for backup only)
    compression_level: Option<u8>,

    /// Created time.
    #[schema(read_only)]
    #[serde(with = "datetime_format")]
    #[serde(default = "Utc::now")]
    created_at: DateTime<Utc>,

    /// Stopped time.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "option_datetime_format")]
    pub finished_at: Option<DateTime<Utc>>,

    /// Last modified time.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "option_datetime_format")]
    last_modified_at: Option<DateTime<Utc>>,

    /// The current status of the tasks.
    #[schema(read_only, value_type = String)]
    status: Status,

    /// Status reason (only for status: failed).
    #[schema(read_only)]
    reason: Option<String>,

    /// Mark the task done as expected.
    #[schema(read_only)]
    completed: bool,
    /// Mark the task is cancelled or not.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    cancelled: bool,

    /// Mark the task deleted or not, deleted tasks will not be listed when query all.
    #[schema(read_only)]
    #[serde(skip_serializing_if = "is_false")]
    deleted: bool,

    /// Add after_delete hook action, the string would be action name, with or without some configuration.
    ///
    /// It will do nothing if the action is not supported by a specific task case.
    #[serde(skip_serializing_if = "Option::is_none")]
    after_delete: Option<String>,
    /// A task name.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(read_only, example = "null")]
    pub name: Option<String>,

    /// Task trigger events, default will be oneshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<Strategy>,

    /// Labels for a task.
    ///
    /// You can use k-v style label such as `key::value` or key-only label `key`.
    ///
    /// You can filter tasks by some labels.
    #[serde(skip_serializing_if = "Labels::is_empty")]
    #[serde(default)]
    #[sqlx(try_from = "String", default)]
    // #[serde(deserialize_with = "labels_serde::deserialize")]
    pub labels: Labels,

    /// break points
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    pub breakpoints: Option<String>,
}

#[allow(dead_code)]
impl Activity {
    pub fn stop(id: i64) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Task stopping".to_string(),
            status: "stopping".to_string(),
            context: None,
        }
    }
    pub fn stopped(id: i64) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Task has been stopped".to_string(),
            status: "stopped".to_string(),
            context: None,
        }
    }
    pub fn stopping_timeout(id: i64) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Stopping task timed out.".to_string(),
            status: "stopped".to_string(),
            context: None,
        }
    }
    pub fn scheduled(id: i64) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "scheduled".to_string(),
            status: "scheduled".to_string(),
            context: None,
        }
    }
    pub fn queued(id: i64, jid: Uuid) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: format!("Enqueue task {id} by job id: {jid}"),
            status: "queued".to_string(),
            context: None,
        }
    }

    /// Start task job and set state as running.
    pub fn started(id: i64, jid: Uuid) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: format!("Started task {id} by job id: {jid}"),
            status: "running".to_string(),
            context: None,
        }
    }

    /// Info-level activity under running state.
    pub fn running(id: i64, message: String) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: message,
            status: "running".to_string(),
            context: None,
        }
    }

    /// Info-level activity under any state.
    pub fn logging(id: i64, message: String) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: message,
            status: "logging".to_string(),
            context: None,
        }
    }

    /// Info-level activity under running state.
    pub fn agent_transferring(id: i64, message: String) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: message,
            status: "connected".to_string(),
            context: None,
        }
    }

    /// Error-level activity under running state.
    pub fn error(id: i64, message: String) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Error,
            activity: message,
            status: "logging".to_string(),
            context: None,
        }
    }
    /// Warn-level activity under running state.
    pub fn warn(id: i64, message: String) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Warn,
            activity: message,
            status: "logging".to_string(),
            context: None,
        }
    }
    pub fn tick(id: i64, jid: Uuid) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Wait for next tick in schedule.".to_string(),
            status: "ticked".to_string(),
            context: Some(json!({"jid": jid}).into()),
        }
    }
    pub fn completed(id: i64, jid: Uuid) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: format!("Finished with job id: {jid}."),
            status: "completed".to_string(),
            context: None,
        }
    }

    pub fn ipc_started(id: i64) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "Agent is putting data".to_string(),
            status: "ipc-started".to_string(),
            context: None,
        }
    }
    pub fn ipc_finished(id: i64) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: "IPC finished".to_string(),
            status: "ipc-finished".to_string(),
            context: None,
        }
    }
    /// Set state as suspending.
    pub fn suspend(id: i64, jid: Uuid) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: format!("Suspending with job id: {jid}."),
            status: "suspending".to_string(),
            context: None,
        }
    }
    pub fn suspended(id: i64, jid: Uuid) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Warn,
            activity: format!("Suspended with job id: {jid}."),
            status: "suspended".to_string(),
            context: None,
        }
    }
    pub fn suspending_with(id: i64, activity: String) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Error,
            activity,
            status: "suspending".to_string(),
            context: None,
        }
    }
    pub fn suspending_timeout(id: i64, jid: Uuid) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Info,
            activity: format!("Suspending timed out with job id: {jid}."),
            status: "suspended".to_string(),
            context: None,
        }
    }
    pub fn interrupted(id: i64, message: impl std::fmt::Display) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Error,
            activity: format!("Error: {message}."),
            status: "interrupted".to_string(),
            context: None,
        }
    }

    pub fn interrupt(id: i64, message: impl std::fmt::Display) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Error,
            activity: format!("Error: {message}."),
            status: "interrupt".to_string(),
            context: None,
        }
    }
    pub fn failed(id: i64, message: impl std::fmt::Display) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Error,
            activity: format!("Failed with error: {message}"),
            status: "failed".to_string(),
            context: None,
        }
    }

    pub fn waiting(id: i64, message: impl std::fmt::Display) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Warn,
            activity: message.to_string(),
            status: "waiting".to_string(),
            context: None,
        }
    }
    pub fn agent_resumed(id: i64, agent_id: AgentId) -> Self {
        Self {
            id,
            at: Utc::now(),
            level: LevelFilter::Warn,
            activity: format!("Agent {agent_id} resumed"),
            status: "resumed".to_string(),
            context: None,
        }
    }

    pub fn health_state(id: i64, state: HealthNotify) -> Self {
        Self {
            id,
            at: state.at,
            level: if state.state >= HealthState::Busy {
                LevelFilter::Warn
            } else {
                LevelFilter::Info
            },
            activity: format!("{}", state.state),
            status: "health".to_string(),
            context: Some(json!(state).into()),
        }
    }
}

lazy_static::lazy_static! {
    /// Data source definition map/list
    pub static ref DATA_SOURCE_DEFINITIONS: LinkedHashMap<String, DataSourceDefinition> = {
        let mut def: Vec<DataSourceDefinition> = Vec::new();
        macro_rules! include_ds_yaml {
            ($ds:literal) => {
                let yaml = include_str!(concat!("../data_sources/en/", $ds, ".yaml"));
                let yaml = if crate::build::CUS_NAME != "TDengine" {
                    yaml
                        .replace("taosX", crate::build::CUS_APP_NAME)
                        .replace("TDengine", crate::build::CUS_NAME)
                        .replace("taosdata", crate::build::CUS_PROMPT)
                        .replace("taosAdapter",const_format::concatcp!(crate::build::CUS_PROMPT, "Adapter"))
                }else {
                    yaml.to_string()
                };
                def.push(serde_yaml::from_str(yaml.as_str()).unwrap());
            };
        }
        include_ds_yaml!("tmq");
        include_ds_yaml!("taos");
        include_ds_yaml!("pi");
        include_ds_yaml!("pi-backfill");
        include_ds_yaml!("opcua");
        include_ds_yaml!("opcda");
        include_ds_yaml!("influxdb");
        include_ds_yaml!("opentsdb");
        include_ds_yaml!("mqtt");
        include_ds_yaml!("kafka");
        include_ds_yaml!("csv");
        include_ds_yaml!("historian");
        include_ds_yaml!("mysql");
        include_ds_yaml!("postgres");
        include_ds_yaml!("oracle");
        include_ds_yaml!("mssql");
        include_ds_yaml!("mongodb");
        for ds in &mut def {
            ds.compute();
        }
        def.into_iter().map(|ds| (ds.id.to_string(), ds)).collect()
    };
    pub static ref DATA_SOURCE_DEFINITIONS_CN: LinkedHashMap<String, DataSourceDefinition> = {
        let mut def: Vec<DataSourceDefinition> = Vec::new();
        macro_rules! include_ds_yaml {
            ($ds:literal) => {
                let yaml = include_str!(concat!("../data_sources/cn/", $ds, ".yaml"));
                let yaml = if crate::build::CUS_NAME != "TDengine" {
                    yaml
                        .replace("taosX", crate::build::CUS_APP_NAME)
                        .replace("TDengine", crate::build::CUS_NAME)
                        .replace("taosdata", crate::build::CUS_PROMPT)
                        .replace("taosAdapter",const_format::concatcp!(crate::build::CUS_PROMPT, "Adapter"))
                } else {
                    yaml.to_string()
                };
                def.push(serde_yaml::from_str(yaml.as_str()).unwrap());
            };
        }
        include_ds_yaml!("tmq");
        include_ds_yaml!("taos");
        include_ds_yaml!("pi");
        include_ds_yaml!("pi-backfill");
        include_ds_yaml!("opcua");
        include_ds_yaml!("opcda");
        include_ds_yaml!("influxdb");
        include_ds_yaml!("opentsdb");
        include_ds_yaml!("mqtt");
        include_ds_yaml!("kafka");
        include_ds_yaml!("csv");
        include_ds_yaml!("historian");
        include_ds_yaml!("mysql");
        include_ds_yaml!("postgres");
        include_ds_yaml!("oracle");
        include_ds_yaml!("mssql");
        include_ds_yaml!("mongodb");
        for ds in &mut def {
            ds.compute();
        }
        def.into_iter().map(|ds| (ds.id.to_string(), ds)).collect()
    };
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct ExpandedDsn {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Option<String>>,
}

impl From<Dsn> for ExpandedDsn {
    fn from(value: Dsn) -> Self {
        let (host, port) = match value.addresses.into_iter().next() {
            Some(addr) => (addr.host, addr.port),
            None => (None, None),
        };
        Self {
            id: value.driver,
            protocol: value.protocol,
            path: value.path,
            host,
            port,
            username: value.username,
            password: value.password,
            subject: value.subject,
            params: value
                .params
                .into_iter()
                .map(|(k, v)| (k, if v.is_empty() { None } else { Some(v) }))
                .collect(),
        }
    }
}

/// A streaming workflow task description.
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct TaskDetail {
    #[serde(flatten)]
    pub task: Task,

    /// Serialized to json from `from` field.
    from_json: Option<serde_json::Value>,

    /// Expanded DSN for source.
    from_expand: Option<ExpandedDsn>,

    /// Expanded DSN for sink.
    to_expand: Option<ExpandedDsn>,

    /// Agent
    #[serde(skip_serializing_if = "Option::is_none")]
    agent: Option<Agent>,
}

impl From<Task> for TaskDetail {
    fn from(value: Task) -> Self {
        Self::new(value)
    }
}

impl std::ops::Deref for TaskDetail {
    type Target = Task;

    fn deref(&self) -> &Self::Target {
        &self.task
    }
}

impl std::ops::DerefMut for TaskDetail {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.task
    }
}

impl TaskDetail {
    pub fn new(task: Task) -> Self {
        TaskDetail {
            task,
            from_json: None,
            from_expand: None,
            to_expand: None,
            agent: None,
        }
    }

    pub(super) fn status(&self) -> &Status {
        &self.task.status
    }

    pub fn expand_detail(self, _lang: Option<String>) -> Self {
        let value = self.task;
        tracing::warn!("expand_detail: {}", value.from);
        let from_dsn: Dsn = value.from.as_str().parse().unwrap();
        let from_json = dsn_to_json(&from_dsn);
        let to_dsn: Dsn = value.to.as_str().parse().unwrap();
        TaskDetail {
            from_expand: Some(ExpandedDsn::from(from_dsn.clone())),
            from_json: Some(from_json),
            to_expand: Some(ExpandedDsn::from(to_dsn.clone())),
            task: value,
            agent: None,
        }
        // if let Some(lang) = lang {
        //     match lang.as_str() {
        //         "zh" => TaskDetail {
        //             from_expand: Some(ExpandedDsn::from(from_dsn.clone())),
        //             from_json: Some(from_json),
        //             to_expand: Some(ExpandedDsn::from(to_dsn.clone())),
        //             task: value,
        //             agent: None,
        //         },
        //         _ => TaskDetail {
        //             from_expand: Some(ExpandedDsn::from(from_dsn.clone())),
        //             from_json: Some(from_json),
        //             to_expand: Some(ExpandedDsn::from(to_dsn.clone())),
        //             task: value,
        //             agent: None,
        //         },
        //     }
        // } else {
        //     TaskDetail {
        //         from_expand: Some(ExpandedDsn::from(from_dsn.clone())),
        //         from_json: Some(from_json),
        //         to_expand: Some(ExpandedDsn::from(to_dsn.clone())),
        //         task: value,
        //         agent: None,
        //     }
        // }
    }

    pub fn expand(mut self) -> Self {
        let value = &self.task;
        // let from_dsn: Dsn = value.from.as_str().parse().unwrap();
        let from_dsn = json_to_dsn(&serde_json::Value::String(value.from.clone())).unwrap();
        let to_dsn: Dsn = value.to.as_str().parse().unwrap();
        self.from_expand = Some(from_dsn.into());
        self.to_expand = Some(to_dsn.into());
        self
    }

    pub fn decorate(self, decorator: &TaskDecorator) -> Self {
        if decorator.detail.is_some() {
            self.expand_detail(decorator.lang.clone())
        } else if decorator.expand.unwrap_or_default() {
            self.expand()
        } else {
            self
        }
    }
}

impl Task {
    fn backport_labels(&mut self) {
        if let Some(labels) = self.labels.as_deref() {
            for label in labels {
                if label.starts_with("from_cluster") {
                    if let Some(value) = label.split_once("::") {
                        self.from_cluster = Some(value.1.to_string())
                    }
                } else if label.starts_with("to_cluster") {
                    if let Some(value) = label.split_once("::") {
                        self.to_cluster = Some(value.1.to_string())
                    }
                } else if label.starts_with("stream_type") {
                    if let Some(value) = label.split_once("::") {
                        self.stream_type = Some(value.1.to_string())
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    fn set_breakpoints(&mut self, breakpoints: Option<String>) {
        self.breakpoints = breakpoints;
    }

    pub async fn load_breakpoints(&mut self) -> anyhow::Result<()> {
        let id = self.id;
        if let Some(s) = tokio::task::spawn_blocking(move || load_breakpoints(id)).await? {
            self.set_breakpoints(Some(s))
        }
        Ok(())
    }
}

pub fn load_breakpoints(task_id: TaskId) -> Option<String> {
    let breakpoints_res = breakpoints_get_all(&task_id.to_string());
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

#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, PartialOrd, ToSchema)]
pub struct Labels(Option<Vec<String>>);

impl Labels {
    /// Check if labels is empty.
    fn is_empty(&self) -> bool {
        self.0.as_ref().map(|v| v.is_empty()).unwrap_or(true)
    }

    pub fn to_hash_map(&self) -> HashMap<&str, &str> {
        self.0
            .as_deref()
            .map(|v| v.iter().flat_map(|v| v.split_once("::")).collect())
            .unwrap_or_default()
    }

    /// Find the label value by key
    pub fn find(&self, key: &str) -> Option<&str> {
        self.0
            .as_deref()
            .and_then(|v| {
                v.iter()
                    .flat_map(|v| v.split_once("::"))
                    .find(|v| v.0 == key)
            })
            .map(|s| s.1)
    }
}

#[tokio::test]
#[ignore]
async fn test_labels() {
    let db = sqlx::SqlitePool::connect("sqlite:./target/taosx.dev.db")
        .await
        .unwrap();

    #[derive(
        Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
    )]
    struct JsonTest {
        #[sqlx(try_from = "String")]
        labels: Labels,
    }

    let json: JsonTest = sqlx::query_as("select labels from task_with_labels")
        .fetch_one(&db)
        .await
        .unwrap();
    dbg!(json);
}

impl TryFrom<String> for Labels {
    type Error = serde_json::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        serde_json::from_str(&value)
    }
}

impl std::ops::Deref for Labels {
    type Target = Option<Vec<String>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Labels {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Task {
    fn contains_label(&self, label: &str) -> bool {
        self.labels
            .as_deref()
            .map(|labels| labels.iter().any(|e| e == label))
            .unwrap_or(false)
    }

    fn contains_labels(&self, labels: &[impl AsRef<str>]) -> bool {
        labels
            .iter()
            .all(|label| self.contains_label(label.as_ref()))
    }

    fn contains_any_labels(&self, labels: &[impl AsRef<str>]) -> bool {
        labels
            .iter()
            .any(|label| self.contains_label(label.as_ref()))
    }
}

const fn is_false(b: &bool) -> bool {
    !*b
}

/// Create new task with json object.
///
/// Required properties:
///
/// - *name*: The task name.
/// - *from*/*from_json* */: The data source configuration
/// - *to*: The data sink DSN.
///
#[derive(
    Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
)]
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
    #[sqlx(default)]
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

impl NewTask {
    fn patch_labels(&mut self) {
        let mut labels = self.labels.take().unwrap_or_default();
        if let Some(value) = self.stream_type.as_ref() {
            labels.push(format!("stream_type::{}", value));
        }

        if let Some(value) = self.from_cluster.as_deref() {
            labels.push(format!("from_cluster::{value}"))
        }
        if let Some(value) = self.to_cluster.as_deref() {
            labels.push(format!("to_cluster::{value}"))
        }
        if !labels.is_empty() {
            self.labels = Some(labels)
        } else {
            self.labels = None
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, FromRow)]
struct NewTaskV1 {
    /// The stream data source.
    #[schema(example = "tmq:///test")]
    from: Option<String>,
    from_json: Option<serde_json::Value>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    // #[serde(default)]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "local:/path/to/backup/test")]
    to: String,

    /// Set if the target database should be cleared before running task.
    #[schema(example = "false")]
    #[serde(default)]
    clear: bool,

    /// Jobs number
    #[schema(example = 0)]
    #[serde(default)]
    jobs: u16,

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
}

impl From<NewTask> for NewTaskV1 {
    fn from(value: NewTask) -> Self {
        let mut labels = value.labels.unwrap_or_default();
        if let Some(value) = value.stream_type {
            labels.push(format!("stream_type::{}", value));
        }
        if let Some(value) = value.from_cluster {
            labels.push(format!("from_cluster::{value}"))
        }
        if let Some(value) = value.to_cluster {
            labels.push(format!("from_cluster::{value}"))
        }
        Self {
            from: value.from,
            from_json: value.from_json,
            oneshot_topic: value.oneshot_topic,
            to: value.to,
            clear: value.clear,
            jobs: value.jobs,
            after_delete: value.after_delete,
            labels: Some(labels),
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema, Default, Clone, Debug, sqlx::FromRow)]
#[serde(default)]
#[schema(example = json!({"from": "tmq:///test", "to": "taos:///test2"}))]
pub struct UpdateTask {
    /// Task name
    name: Option<String>,
    /// Update trigger,
    trigger: Option<Strategy>,
    /// *Deprecated*.
    stream_type: Option<String>,
    /// The stream data source.
    from: Option<String>,
    from_json: Option<serde_json::Value>,
    /// *Deprecated*. The stream data source cluster id.
    from_cluster: Option<String>,
    /// Use oneshot topic for a task, delete the topic after task deleted.
    oneshot_topic: Option<String>,
    /// The target of the stream.
    to: Option<String>,
    /// Agent id
    via: Option<i64>,

    /// The parser of the task stream.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    pub parser: Option<serde_json::Value>,

    /// *Deprecated*. The stream data target cluster id.
    to_cluster: Option<String>,
    /// Jobs number
    jobs: Option<u16>,
    /// *Deprecated*.
    compression_level: Option<u8>,
    force: Option<bool>,
    /// Labels for a task.
    labels: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Default, Clone, IntoParams)]
#[serde(default)]
pub struct TaskFilter {
    /// task name
    name: Option<String>,
    /// stream type label
    stream_type: Option<String>,
    /// source cluster label
    from_cluster: Option<String>,
    /// target cluster label
    to_cluster: Option<String>,
    /// task status
    status: Option<String>,
    /// task start time
    start_create_time: Option<String>,
    /// task end time
    end_create_time: Option<String>,
    /// task delete state
    with_deleted: Option<bool>,
    /// task labels
    pub labels: Option<String>,
    /// any labels include to filter
    any_labels: Option<String>,
    /// filter exclude labels
    without_labels: Option<String>,
    /// task agent id
    via: Option<i64>,
    in_scheduler: Option<bool>,
}

#[derive(Serialize, Deserialize, Default, Clone, IntoParams, Debug)]
#[serde(default)]
pub struct TaskDecorator {
    expand: Option<bool>,
    detail: Option<bool>,
    lang: Option<String>,
}

impl TaskFilter {
    pub fn gen_sql_conditions(&mut self) -> std::result::Result<String, std::fmt::Error> {
        use std::fmt::Write;
        let mut sql = String::new();
        if !self.with_deleted.unwrap_or_default() {
            write!(sql, "`deleted` = FALSE")?;
        } else {
            write!(sql, "1 = 1")?;
        }
        if let Some(val) = self.name.as_deref() {
            write!(sql, " AND `name` = '{val}'")?;
        }
        if let Some(val) = self.stream_type.as_deref() {
            // write!(sql, " AND `stream_type` = '{val}'")?;
            let val = format!("stream_type::{}", val);
            if let Some(labels) = self.labels.as_mut() {
                labels.push(',');
                labels.push_str(&val);
            } else {
                self.labels.replace(val);
            }
        }
        if let Some(val) = self.status.as_ref() {
            write!(sql, " AND `status` = '{val}'")?;
        }
        if let Some(val) = self.from_cluster.as_deref() {
            // write!(sql, " AND `from_cluster` = '{from_cluster}'")?;
            let val = format!("from_cluster::{}", val);
            if let Some(labels) = self.labels.as_mut() {
                labels.push(',');
                labels.push_str(&val);
            } else {
                self.labels.replace(val);
            }
        }
        if let Some(val) = self.to_cluster.as_deref() {
            // write!(sql, " AND `to_cluster` = '{val}'")?;
            let val = format!("to_cluster::{}", val);
            if let Some(labels) = self.labels.as_mut() {
                labels.push(',');
                labels.push_str(&val);
            } else {
                self.labels.replace(val);
            }
        }
        if let Some(val) = self.start_create_time.as_deref() {
            write!(sql, " AND `created_at` >= '{val}'")?;
        }
        if let Some(val) = self.end_create_time.as_deref() {
            write!(sql, " AND `created_at` <= '{val}'")?;
        }
        if let Some(val) = self.via.as_ref() {
            write!(sql, " AND `via` = {val}")?;
        }
        Ok(sql)
    }

    fn has_labels_filter(&self) -> bool {
        !(self.labels.is_none() && self.any_labels.is_none() && self.without_labels.is_none())
    }

    fn filter_task_labels(&self, tasks: &mut Vec<Task>) {
        if let Some(labels) = self.labels.as_deref() {
            tasks.retain(|task| task.contains_labels(&labels.split(",").collect_vec()));
        }
        if let Some(labels) = self.any_labels.as_deref() {
            tasks.retain(|task| task.contains_any_labels(&labels.split(",").collect_vec()));
        }
        if let Some(labels) = self.without_labels.as_deref() {
            // remove tasks contains any labels in `without_labels`.
            tasks.retain(|task| !task.contains_any_labels(&labels.split(",").collect_vec()));
        }
    }

    #[allow(dead_code)]
    fn via(mut self, agent_id: i64) -> Self {
        self.via.replace(agent_id);
        self
    }

    fn with_joined_labels(mut self, labels: String) -> Self {
        self.labels.replace(labels);
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::serve::tests::{generate_scheduler_for_test, tracing_subscriber_init};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "debug");
        tracing_subscriber_init()?;
        let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
        dbg!(&controller);
        let new: AgentProps = serde_json::from_str(
            r#"
        {
            "dsn": "",
            "name": "agent1",
            "cluster_id": "xxx",
            "user_id":"root",
            "expire_date": "2024-01-01",
            "connectors": ["opc"]
        }
        "#,
        )
        .unwrap();
        dbg!(&new);
        let agent = controller.create_agent(new).await?;
        dbg!(&agent);
        let detail = controller.get_agent_by_id(agent.id).await?;
        dbg!(&detail);

        let found = controller.get_agent_with_token(&agent.token).await?;
        dbg!(&found);

        let res = controller
            .agent_connect_with_token(&agent.token, "127.0.0.1:8080".parse().ok().as_ref())
            .await?;
        dbg!(res);

        let task: NewTask = serde_json::from_str(&format!(
            r#"
        {{
            "from": "tmq:///test", "to":"taos:///test", "via": {}
        }}
        "#,
            agent.id
        ))
        .unwrap();

        let task = controller.create(task).await;
        assert!(task.is_err()); // agent is not alive.

        let activities = controller
            .agent_activities(agent.id, &Default::default())
            .await?;
        dbg!(activities);

        controller.delete_agent(agent.id).await?;

        // let deleted_task = controller.get(task.id).await?;
        // dbg!(&deleted_task);
        // assert!(deleted_task.is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_patch() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "taos=debug");
        tracing_subscriber_init()?;
        let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;

        let new: AgentProps = serde_json::from_str(
            r#"
        {
            "dsn": "",
            "name": "代理1",
            "cluster_id": "xxx",
            "user_id":"root",
            "expire_date": "2024-01-01",
            "connectors": ["opc"]
        }
        "#,
        )
        .unwrap();
        dbg!(&new);
        let agent = controller.create_agent(new).await?;

        let detail = controller.get_agent_by_id(agent.id).await?;
        dbg!(&detail);

        let patch: AgentUpdates = serde_json::from_str(
            r#"{
            "name": "代理2",
            "connectors": ["opc", "modbus"]
        }
        "#,
        )
        .unwrap();

        let _agent = controller.update_agent(agent.id, patch).await?;

        let detail = controller.get_agent_by_id(agent.id).await?;
        dbg!(&detail);

        controller.delete_agent(agent.id).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_task_when_agent_not_alive() -> anyhow::Result<()> {
        tracing_subscriber_init()?;
        let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
        let agent = controller
            .create_agent(AgentProps {
                dsn: "".to_string(),
                name: "a1".to_string(),
                cluster_id: "".to_string(),
                user_id: "".to_string(),
            })
            .await?;
        dbg!(&agent);

        let task_props: NewTask = serde_json::from_str(
            r#"
        {
            "from": "mqtt:///db2",
            "to":"taos:///db2",
            "via": 1
        }
        "#,
        )
        .unwrap();

        let task = controller.create(task_props).await;
        assert!(task.is_err());
        dbg!(&task);
        assert!(
            task.unwrap_err()
                .to_string()
                .contains("Agent 1 is not alive")
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test_task_offset_with_taos() -> anyhow::Result<()> {
        unsafe {
            std::env::set_var("RUST_LOG", "taos=info");
        }
        tracing_subscriber_init()?;

        let dsn = "taos://localhost:6030".to_string();
        tracing::info!("dsn: {}", dsn);

        let taos = taos::TaosBuilder::from_dsn(&dsn)?.build().await?;
        taos.exec_many([
            "drop topic if exists ws_abc1",
            "drop database if exists ws_abc1",
            "create database ws_abc1 wal_retention_period 3600",
            "create topic ws_abc1 with meta as database ws_abc1",
            "use ws_abc1",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists ws_abc2",
            "create database if not exists ws_abc2",
        ])
        .await?;

        let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;

        let task_props: NewTask = serde_json::from_str(
            r#"
        {
            "from": "tmq:///ws_abc1",
            "to":"taos:///ws_abc2",
            "force": true
        }
        "#,
        )
        .unwrap();

        let task = controller.create(task_props).await?;
        // dbg!(&task);

        // let tasks = controller.tasks(TaskFilter::default()).await?;

        controller.start_task(&task).await?;

        // sleep to wait for task started.
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        // let task_after_start = controller.get(task.id).await?;
        // dbg!(&task_after_start);

        controller.stop(task.id).await?;
        let offset = controller.offsets(task.id).await?.unwrap();
        dbg!(&offset);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_max_activities_per_entity() -> anyhow::Result<()> {
        tracing_subscriber_init()?;
        let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
        let agent = controller
            .create_agent(AgentProps {
                dsn: "".to_string(),
                name: "a1".to_string(),
                cluster_id: "".to_string(),
                user_id: "".to_string(),
            })
            .await?;
        dbg!(&agent);
        let pool = controller.pool.clone();
        for _i in 0..1000 {
            let _ = push_agent_activity(
                &pool,
                &Activity::agent_transferring(agent.id, "test".to_string()),
            )
            .await;
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        keep_max_activities(&pool, 100).await?;

        let len = sqlx::query_scalar::<_, i64>("select count(*) from agent_activities")
            .fetch_one(&pool)
            .await?;
        assert_eq!(len, 100);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_task_summaries() -> anyhow::Result<()> {
        tracing_subscriber_init()?;
        let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
        let _ = controller.get_task_summaries(10).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn legacy_edition_check_with_taos() -> anyhow::Result<()> {
        let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
        let from = Dsn::from_str("taos://")?;
        let to = Dsn::from_str("taos+ws://localhost:6041")?;
        license::validate_task(&from, &to, Some(&controller.pool)).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn active_active_edition_check_with_taos() -> anyhow::Result<()> {
        let _ = tracing_subscriber_init();
        let from = Dsn::from_str("tmq+ws://localhost:16041/test?replica")?;
        let to = Dsn::from_str("taos+ws://localhost:6041/test")?;
        license::validate_task(&from, &to, None).await?;
        let from = Dsn::from_str("tmq:///test?replica")?;
        let to = Dsn::from_str("taos:///test")?;
        license::validate_task(&from, &to, None).await?;

        let from = Dsn::from_str("tmq+ws://localhost:16041/test?replica")?;
        let to = Dsn::from_str("taos+ws://localhost:6041/test")?;
        let res = license::validate_task(&from, &to, None).await;
        dbg!(&res);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_parse_csv() {
        let dsn = Dsn::from_str("csv:./ab.csv,./cd.csv?param=1").unwrap();
        dbg!(&dsn);
        assert_eq!(dsn.path.unwrap(), "./ab.csv,./cd.csv");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test_replica_docker() -> anyhow::Result<()> {
        std::thread::spawn(move || {
            std::env::set_current_dir("tests/active-active").unwrap();
            let _ = std::process::Command::new("docker")
                .args(["compose", "up", "-d", "--remove-orphans"])
                .output();
        })
        .join()
        .unwrap();
        let source = TaosBuilder::from_dsn("http://localhost:7041")?
            .build()
            .await?;
        let sink = TaosBuilder::from_dsn("http://localhost:8041")?
            .build()
            .await?;
        {
            // prepare data
            source
                .exec_many([
                    "drop topic if exists __replica__rep1",
                    "drop database if exists rep1",
                    "create database if not exists rep1",
                    "create table if not exists rep1.t1 (ts timestamp, c1 int)",
                    "insert into rep1.t1 values(now, 1)",
                    "drop topic if exists __replica__rep2",
                    "drop database if exists rep2",
                    "create database if not exists rep2",
                    "create table if not exists rep2.st1 (ts timestamp, c1 int) tags(t1 int)",
                    "insert into rep2.t1 using rep2.st1 tags(1) values(now, 2)",
                ])
                .await?;
            sink.exec_many([
                "drop database if exists rep1",
                "drop database if exists rep2",
            ])
            .await?;
        }
        let _ = tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_file(true)
            .pretty()
            .try_init();
        let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
        let opts = ReplicaOpts {
            source: "http://localhost:7041".to_string(),
            sink: "http://localhost:8041".to_string(),
            new_databases_checking_interval: Some(1),
            ..Default::default()
        };
        let arc = Arc::new(controller);
        let replica = arc.start_replica_monitor(opts).await?;
        assert!(replica.id.is_some(), "replica id is none: {replica:?}");
        let reps = arc.list_replicas().await?;
        assert!(
            reps.is_empty() || reps[0].1.is_empty(),
            "replicas is not empty: {reps:?}"
        );

        {
            sink.exec("create database if not exists rep1").await?;
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let reps = arc.list_replicas().await?;
        assert_eq!(reps[0].1.len(), 1, "replicas should have 1 task: {reps:?}");

        {
            sink.exec("create database if not exists rep2").await?;
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let reps = arc.list_replicas().await?;
        assert_eq!(reps[0].1.len(), 2, "replicas should have 2 task: {reps:?}");

        let del = arc
            .remove_replica_monitor(replica.id.as_deref().unwrap())
            .await?;
        assert!(del.is_some());
        Ok(())
    }
}
