use std::collections::BTreeMap;
use std::env;
use std::fmt::{Debug, Display};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::{DashMap, DashSet};
use flume::Sender;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use metrics::counter;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{migrate::Migrator, sqlite::SqliteJournalMode, FromRow, SqlitePool};
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use taosx_core::utils::mask_dsn;
use taosx_core::utils::port_pool::PortPool;
use taosx_core::{ConnectorLicense, DataSet, DataSetsReq, Response, TaskOpts};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio::{runtime::Runtime, sync::RwLock};
use tokio_cron_scheduler::{Job, JobScheduler};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};
use utoipa::*;

pub(crate) mod agent;
pub(crate) mod transferred;

mod datetime_format {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

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
        let s = format!("{}", date.to_rfc3339_opts(SecondsFormat::Millis, true));
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
    use serde::{self, Deserialize, Deserializer, Serializer};

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
            let s = format!("{}", date.to_rfc3339_opts(SecondsFormat::Millis, true));
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

use crate::serve::controller::agent::Activity;
use crate::serve::task;

use self::agent::{
    Agent, AgentActivityFilter, AgentProps, AgentToken, AgentUpdates, AgentWithToken, LevelFilter,
};
use self::transferred::Transferred;

use super::data_sources::DataSourceDefinition;

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

// const TASK_SELECT: &str = "select *, `status` == 'completed' as `completed`, `status` == 'cancelled' as `cancelled` from tasks";

type AgentDataSetsSender = Sender<Response<Vec<DataSet>>>;
#[derive(Debug, Clone)]
pub enum AgentAction {
    Run(i64),
    Stop(i64),
    Cancel(i64),
    ListDataSets(DataSetsReq, AgentDataSetsSender),
    #[allow(dead_code)]
    RetrieveDataSets(DataSetsReq, Vec<DataSet>),
}
pub type AgentTasksReceiver = tokio::sync::broadcast::Receiver<AgentAction>;
pub type AgentTasksSender = tokio::sync::broadcast::Sender<AgentAction>;
pub type AgentTasksError = tokio::sync::broadcast::error::SendError<AgentAction>;
// pub type AgentStatusChannel
pub struct AgentTasks {
    pub current: Arc<DashSet<i64>>,
    pub datasets: Arc<DashMap<DataSetsReq, AgentDataSetsSender>>,
    pub sender: AgentTasksSender,
    pub receiver: AgentTasksReceiver,
    pub alive: AtomicBool,
}

impl AgentTasks {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::broadcast::channel(10);
        Self {
            current: Arc::new(DashSet::new()),
            datasets: Arc::new(DashMap::new()),
            sender,
            receiver,
            alive: AtomicBool::new(false),
        }
    }
    pub fn spawn_listener(&self) -> JoinHandle<()> {
        let mut rx = self.sender.subscribe();
        let current = self.current.clone();
        let datasets = self.datasets.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(action) => {
                        tracing::info!("agent action: {action:?}");
                        match action {
                            AgentAction::Run(task) => {
                                current.insert(task);
                            }
                            AgentAction::Stop(task) => {
                                current.remove(&task);
                            }
                            AgentAction::Cancel(task) => {
                                current.remove(&task);
                            }
                            AgentAction::ListDataSets(req, sender) => {
                                datasets.insert(req, sender);
                            }
                            AgentAction::RetrieveDataSets(req, sets) => {
                                if let Some(sender) = datasets.remove(&req) {
                                    let _ = sender.1.send_async(Ok(sets)).await;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        tracing::error!("err: {err}");
                        break;
                    }
                }
            }
        })
    }

    pub fn send(&self, action: AgentAction) -> Result<usize, AgentTasksError> {
        self.sender.send(action) // tokio send
    }
}

use taos::taos_query::tmq::Assignment;

#[derive(Debug, Deserialize)]
pub struct TaskStatus {
    id: i64,
    at: DateTime<Utc>,
    action: String,
    message: Option<String>,
    // TODO: use context as task activity level
    #[allow(dead_code)]
    context: Option<String>,
}

pub(crate) struct TaskController {
    pub pool: SqlitePool,
    pub runtime: Option<Runtime>,
    pub tasks: RwLock<
        HashMap<
            i64,
            (
                tokio::task::JoinHandle<Result<(), anyhow::Error>>,
                CancellationToken,
            ),
        >,
    >,
    pub scheduler: Arc<JobScheduler>,
    pub secret: RwLock<Option<Bytes>>,
    /// An Agent-to-Tasks-Vector hashmap.
    pub agent_tasks: RwLock<HashMap<i64, AgentTasks>>,
    // An Task-to-Assignments-Vector hashmap.
    pub offsets: RwLock<HashMap<i64, Arc<DashMap<String, Vec<Assignment>>>>>,
    // pub agent_workers: RwLock<HashMap<i64, AgentWorker>>
    // tasks: Mutex<Vec<Task>>,
    pub transferred: Transferred,
}

impl Debug for TaskController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskController")
            .field("pool", &self.pool)
            .field("runtime", &self.runtime)
            .field("tasks", &self.tasks)
            .field("offsets", &self.offsets)
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
    pub async fn from_sqlite(sqlite: &str) -> anyhow::Result<Self> {
        TaskController::from_sqlite(sqlite)
            .await
            .map(|v| Self(Arc::new(v)))
    }
    #[async_backtrace::framed]
    pub async fn from_sqlite_with_runtime(
        sqlite: &str,
        rt: tokio::runtime::Runtime,
    ) -> anyhow::Result<Self> {
        match Self::from_sqlite(sqlite).await {
            Ok(c) => Ok(c),
            Err(err) => {
                let _ = std::thread::spawn(move || {
                    std::mem::drop(rt);
                })
                .join();
                Err(err)
            }
        }
    }
    pub async fn start_all_with_schedule(&self) -> anyhow::Result<()> {
        let tasks: Vec<Task> = sqlx::query_as::<_, Task>(
            "select * from task_with_labels where via is NULL and status not in (?, ?, ?) and `deleted` != TRUE order by created_at desc")
            .bind(Status::Completed)
            .bind(Status::Failed)
            .bind(Status::Stopped)
            .fetch_all(&self.pool)
            .await?;
        // Ok(tasks)
        for task in &tasks {
            if let Err(err) = self.start_task(task).await {
                let id = task.id;
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
                .await?;

                let activity = Activity::new(
                    id,
                    now,
                    LevelFilter::Error,
                    format!("Start task {id}"),
                    "failed",
                    err,
                );
                self.push_task_activity(&activity).await?;
            }
        }
        let tasks: Vec<Task> = sqlx::query_as::<_, Task>(
            "select * from task_with_labels where trigger is not null and status != ? and `deleted` != TRUE order by created_at desc")
            .bind(Status::Stopped)
            .fetch_all(&self.pool)
            .await?;
        let sched = self.scheduler.clone();
        for task in tasks {
            if let Some(trigger) = task.trigger.as_deref() {
                let schedule = trigger.trim_start_matches("schedule:");
                let id = task.id;
                let controller = self.clone();
                match Job::new_async(schedule, move |uuid, mut l| {
                    let controller = controller.clone();
                    Box::pin(async move {
                        tracing::info!("waiting for next tick");
                        let next_tick = l.next_tick_for_job(uuid).await;
                        match next_tick {
                            Ok(Some(ts)) => {
                                tracing::info!("Next tick is {:?}", ts);
                                let _ = controller.start(id).await.unwrap();
                            }
                            _ => tracing::warn!("Could not get next tick"),
                        }
                    })
                }) {
                    Ok(job) => {
                        tracing::debug!("add cron job for task: {task:?}");
                        sched.add(job).await?;
                    }
                    Err(err) => {
                        tracing::error!("Scheduler task error: {err:?}, task:{task:?}");
                        Err(err)
                            .with_context(|| format!("Schedule task error, task:{:?}", task))?;
                    }
                }
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
        if let Some(rt) = self.runtime.take() {
            // rt.block_on(self.clear()).unwrap();
            std::thread::spawn(move || {
                tracing::debug!("dropping tokio runtime in another thread");
                std::mem::drop(rt);
            })
            .join()
            .unwrap();
        }
        if let Some(sched) = Arc::get_mut(&mut self.scheduler) {
            let _ = sched.shutdown();
        };
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

static ONCE: OnceCell<PortPool> = OnceCell::const_new();

async fn push_task_activity(pool: &SqlitePool, activity: &Activity) -> anyhow::Result<()> {
    if activity.status == "completed" {
        let _ = sqlx::query!(
            "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ? AND status != ?",
            activity.at,
            Status::Completed,
            activity.id,
            Status::Completed,
        )
        .execute(pool)
        .await?;
    }
    sqlx::query(
            "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)")
            .bind(
            activity.id).bind(&
            activity.at).bind(&
            activity.level).bind(&
            activity.activity).bind(&
            activity.status).bind(&
            activity.context)
        .execute(pool)
        .await?;
    Ok(())
}
async fn push_agent_activity(pool: &SqlitePool, activity: &Activity) -> anyhow::Result<()> {
    sqlx::query(
            "INSERT INTO agent_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)")
            .bind(
            activity.id).bind(&
            activity.at).bind(&
            activity.level).bind(&
            activity.activity).bind(&
            activity.status).bind(&
            activity.context)
        .execute(pool)
        .await?;
    Ok(())
}

impl TaskController {
    pub async fn from_sqlite(sqlite: &str) -> anyhow::Result<Self> {
        if !sqlite.contains(":memory:") {
            let file = sqlite.replacen("sqlite:", "", 1);
            let path = std::path::Path::new(&file);
            if let Some(dir) = path.parent() {
                if !dir.exists() {
                    std::fs::create_dir_all(&dir)
                        .context("Cannot create directory for database")?;
                }
            }
        }
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(sqlite)?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(30))
            .journal_mode(SqliteJournalMode::Wal);
        let pool = sqlx::SqlitePool::connect_with(options).await?;
        MIGRATOR.run(&pool).await?;
        let scheduler = JobScheduler::new().await?;
        scheduler.start().await?;
        let transferred = Transferred::new(pool.clone(), Duration::from_secs(10));
        Ok(Self {
            pool,
            runtime: None,
            tasks: Default::default(),
            scheduler: Arc::new(scheduler),
            secret: RwLock::new(None),
            agent_tasks: Default::default(),
            offsets: Default::default(),
            transferred,
        })
    }

    // pub fn with_runtime(mut self, rt: tokio::runtime::Runtime) -> Self {
    //     self.runtime = Some(rt);
    //     self
    // }

    #[instrument(skip_all, name = "task::start", parent = None, fields(
        task.id = task.id,
        task.source = %mask_dsn(&task.from.parse().unwrap()),
        task.sink = %mask_dsn(&task.to.parse().unwrap()),
        task.agent = task.via,
    ))]
    async fn start_task(&self, task: &Task) -> anyhow::Result<()> {
        tracing::info!("start task");
        let id = task.id;
        let now = Utc::now();

        let mut remove_finished_task = false;

        {
            // for read guard lifetime
            let guard = self.tasks.read().await;
            if let Some(h) = guard.get(&id) {
                if !h.0.is_finished() {
                    tracing::info!("try start task {id} but it is running");
                    let context = format!("Trying to start task {id} but it is running");
                    let activity = Activity::new(
                        id,
                        now,
                        LevelFilter::Info,
                        "Trying to start task but already running".to_string(),
                        "running".to_string(),
                        json!({ "message": context }),
                    );
                    return push_task_activity(&self.pool, &activity).await;
                } else {
                    remove_finished_task = true;
                }
            }
            drop(guard);
        }

        if remove_finished_task {
            // write guard lifetime.
            let mut guard = self.tasks.write().await;
            guard.remove(&id);
            drop(guard);
        }

        let from = if let Some(topic) = task.oneshot_topic.as_deref() {
            let mut from: Dsn = task.from.parse()?;
            from.set("use.topic.name", topic);
            tracing::info!("Set task from: {from}");
            from
        } else {
            task.from.parse()?
        };
        let to_dsn: Dsn = task.to.parse()?;

        let token = tokio_util::sync::CancellationToken::new();
        let cloned_token = token.clone();
        let offsets = Arc::new(DashMap::new());
        self.offsets.write().await.insert(id, offsets.clone());

        let agent_task_worker = if let Some(id) = task.via {
            if !self.agent_alive(id).await {
                anyhow::bail!("Agent {id} is not alive");
            }
            Some((
                task.id,
                self.agent_tasks
                    .read()
                    .await
                    .get(&id)
                    .unwrap()
                    .sender
                    .clone(),
                id,
            ))
        } else {
            None
        };

        let activity = Activity::new(
            id,
            now,
            LevelFilter::Info,
            "Spawn task worker",
            "started",
            serde_json::to_value(task).unwrap(),
        );
        push_task_activity(&self.pool, &activity).await?;
        let pool = self.pool.clone();

        let transferred = match from.driver.as_str() {
            "opcua" | "opcda" | "influxdb" | "opentsdb" | "pi" | "mqtt" | "kafka" => {
                let taos = TaosBuilder::from_dsn(&to_dsn)?.build().await?;
                let cluster_id: Option<i64> = taos
                    .query_one("select id from information_schema.ins_cluster")
                    .await
                    .map_err(|err| anyhow::format_err!("Cannot retrieve cluster id: {err}"))
                    .unwrap_or_default();
                // let license = taos.query_one(sql)
                let connector = match from.driver.as_str() {
                    "opcua" => "opc_ua",
                    "opcda" => "opc_da",
                    "influxdb" => "influxdb",
                    "opentsdb" => "opentsdb",
                    "pi" => "pi",
                    "kafka" => "kafka",
                    "mqtt" => "mqtt",
                    _ => unreachable!(),
                };
                let license: Option<ConnectorLicense> = taos
                    .query_one::<_, String>(format!(
                        "select `{connector}` from information_schema.ins_grants"
                    ))
                    .await
                    .unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok());

                if let Some(license) = license {
                    if license.is_expired() {
                        anyhow::bail!(
                            "Connector {connector} expired, please contact the database administrator for license",
                        )
                    }
                }
                if let Some(cluster_id) = cluster_id {
                    self.transferred
                        .get(&(cluster_id, from.driver.clone()))
                        .await
                } else {
                    None
                }
            }
            _ => None,
        };

        // todo! add trace id to
        let span = tracing::info_span!(
            "task::spawned",
            task.id = id,
            trace_id = tracing::field::Empty
        );
        dbg!(tracing::Span::current().metadata());
        // span.record("request", value)
        let opts = TaskOpts {
            transform: vec![],
            from: from.clone(),
            to: to_dsn.clone(),
            parser: task
                .parser
                .as_ref()
                .map(|v| serde_json::from_value(v.clone()).unwrap()),
            jobs: task.jobs as _,
            compression_level: task.compression_level.map(Into::into),
            force: true,
            cancel: CancellationToken::new(),
            // port_pool: ONCE,
            with_agent: None,
            offsets,
            transferred,
            span: span.clone(),
            task_id: Some(id.to_string()),
        };
        // dbg!(&opts);
        // dbg!(&agent_task_worker);

        // if let Some(atomic) = &runnings {
        //     atomic.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // }

        let task_handler = async move {
            // let _ = span.clone().entered();
            tracing::info!(task.id = id, "start worker");
            // set current dir for upload files
            let path = task::ENV_TAOSX_UPLOAD_FILE_HOME_DEFAULT.replace("files", "");
            let root = std::path::Path::new(path.as_str());
            let _ = env::set_current_dir(&root);
            let now = Utc::now();
            let _ = sqlx::query!(
                "UPDATE tasks SET last_modified_at = ?, status = ? WHERE id = ?",
                now,
                Status::Started,
                id
            )
            .execute(&pool)
            .in_current_span()
            .await?;
            // let span = opts.span.clone();

            // let sender2 = agent_task_worker.clone();
            let cloned_token2 = cloned_token.clone();
            tokio::select! {
                _ = cloned_token.cancelled() => {
                    // let _ = tr.record("action", "cancel").clone().entered();
                    let status: Status = sqlx::query_scalar("select status from tasks where id = ?")
                        .bind(id).fetch_one(&pool).await?;
                    if matches!(status, Status::Completed | Status::Stopped | Status::Failed) {
                        return Ok(());
                    }
                    let activity;
                    if let Some((id, sender, _)) = agent_task_worker.as_ref() {
                        let _ = sender.send(AgentAction::Cancel(*id)); // tokio send
                        activity = "Send cancel signal to agent".to_string();
                    } else {
                        opts.cancel();
                        activity = "Cancel task".to_string();
                    }
                    tracing::debug!("cancel task {id}");
                    let now = Utc::now();
                    let status = Status::Cancelled;

                    let activity = Activity {
                        id,
                        at: now,
                        level: LevelFilter::Info,
                        activity,
                        status: "cancelled".to_string(),
                        context: None,
                    };
                    push_task_activity(&pool, &activity).await?;

                    match opts.from.driver.as_str() {
                        "opc" | "opcua" | "opcda" | "pi" => {
                            let _ = sqlx::query!(
                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND status not in (?, ?)",
                                now,
                                status,
                                None::<String>,
                                id,
                                Status::Stopped, Status::Failed
                            )
                            .execute(&pool)
                            .await?;
                        },
                        _ => {
                            let _ = sqlx::query!(
                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND status not in (?, ?, ?)",
                                now,
                                status,
                                None::<String>,
                                id,
                                Status::Completed, Status::Stopped, Status::Failed
                            )
                            .execute(&pool)
                            .await?;
                        }
                    }
                    // span.exit();
                }
                result = async {
                    if agent_task_worker.is_none() && opts.from.driver == "tmq" && opts.from.get("timeout").map(|s| s == "never").unwrap_or(false) {
                        let mut restarts = 0;
                        let mut sleep = Duration::from_secs(2);
                        let mut last_restart_time = Instant::now();
                        loop {
                            let now = Utc::now();
                            let none: Option<String> = None;
                            let _ = sqlx::query!(
                                "UPDATE tasks SET last_modified_at = ?, status = ?, reason = ? WHERE id = ?",
                                now,
                                Status::Running,
                                none,
                                id
                            )
                            .execute(&pool)
                            .await?;
                            if restarts > 0 {
                                tracing::info!("resume task {id} as {restarts} restarts");

                                let activity = Activity {
                                    id,
                                    at: now,
                                    level: LevelFilter::Info,
                                    activity: "Resume task".to_string(),
                                    status: "ok".to_string(),
                                    context: None,
                                };
                                push_task_activity(&pool, &activity).await?;
                                last_restart_time = Instant::now();
                            } else {
                                let activity = Activity {
                                    id,
                                    at: now,
                                    level: LevelFilter::Info,
                                    activity: "Start worker".to_string(),
                                    status: "ok".to_string(),
                                    context: None,
                                };
                                push_task_activity(&pool, &activity).await?;

                                tracing::info!("start task {id}");
                            }
                            let instant = std::time::Instant::now();
                            let result = opts.run(ONCE.get_or_init(|| async { PortPool::default() }).await).await;
                            let timing = instant.elapsed();
                            match result {
                                Ok(_) => {
                                    let now = Utc::now();
                                    let _ = sqlx::query!(
                                        "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ?",
                                        now,
                                        Status::Interrupted,
                                        none,
                                        id
                                    )
                                    .execute(&pool)
                                    .await?;
                                    let activity = Activity {
                                        id,
                                        at: now,
                                        level: LevelFilter::Info,
                                        activity: format!("Task is completed in {timing:?}"),
                                        status: "completed".to_string(),
                                        context: None,
                                    };
                                    push_task_activity(&pool, &activity).await?;
                                }
                                Err(err) => {
                                    let err_string = format!("{err:#}");
                                    // let code = err.code();

                                    match err_string.as_str() {
                                        e if e.contains("Unsupported HTTP method used - only GET is allowed") => {
                                            // todo(@huolinhe): we got 401 Authentication failure with HTTPS, but this error with HTTP.
                                            //   Maybe you should check the websocket implementations for the low-level reason.
                                            let err = "Authentication failure";
                                            tracing::error!("run task {id} failed with: {err:#}, please check the instance status or token");
                                            let err = format!("{err:#}");
                                            let now = Utc::now();
                                            let _ = sqlx::query!(
                                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND deleted != TRUE",
                                                now,
                                                Status::Failed,
                                                err,
                                                id
                                            )
                                            .execute(&pool)
                                            .await?;

                                            let activity = Activity::new(id,now, LevelFilter::Error, "Authentication failure".to_string(), "failed".to_string(),json!({"message": err}));
                                            push_task_activity(&pool, &activity).await?;

                                            break;
                                        }
                                        e if e.contains("WebSocket protocol error") || e.contains("WebSocket internal error") || e.contains("0x000B") || e.contains("0xE002") => {
                                            tracing::warn!("run task {id} failed: {err}, wait for resume...");
                                            let err = format!("{err:#}");
                                            let now = Utc::now();
                                            let _ = sqlx::query!(
                                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND deleted != TRUE AND status != ?",
                                                now,
                                                Status::Interrupted,
                                                err,
                                                id,
                                                Status::Failed
                                            )
                                            .execute(&pool)
                                            .await?;

                                            let context = json!({
                                                "code": 0xFFFFi32,
                                                "message": err,
                                            });

                                            let activity = Activity::new(id, now, LevelFilter::Warn, "Resume task", "interrupted",serde_json::to_value(&context).unwrap());
                                            push_task_activity(&pool, &activity).await?;
                                        }
                                        _ => {
                                            tracing::error!("run task {id} failed with: {err}, please check the task information");
                                            let err = format!("{err:#}");
                                            let now = Utc::now();
                                            let _ = sqlx::query!(
                                                "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND deleted != TRUE",
                                                now,
                                                Status::Failed,
                                                err,
                                                id
                                            )
                                            .execute(&pool)
                                            .await?;

                                            let context = json!({
                                                "code": 0xFFFFi32,
                                                "message": err,
                                            });
                                            let activity = Activity::new(id, now, LevelFilter::Error, format!("Failed with: {err:#}"), "failed", serde_json::to_value(&context).unwrap());
                                            push_task_activity(&pool, &activity).await?;
                                            break;
                                        }
                                    }
                                }
                            }
                            tracing::info!("resume task {id} in {sleep:?}");
                            let running_elapsed = last_restart_time.elapsed();
                            if running_elapsed > sleep {
                                sleep = Duration::from_millis(500);
                            }
                            tokio::time::sleep(sleep).await;
                            if sleep < Duration::from_secs(60) {
                                sleep = sleep * 2;
                            }
                            restarts += 1;
                        }
                    } else {
                        let now = Utc::now();
                        let _ = sqlx::query!(
                            "UPDATE tasks SET last_modified_at = ?, status = ?, reason = ? WHERE id = ?",
                            now,
                            Status::Running,
                            None::<String>,
                            id
                        )
                        .execute(&pool)
                        .await?;
                        if let Some((id, sender, agent_id)) = &agent_task_worker {
                            let send = sender.send(AgentAction::Run(*id)).map_err(|_| anyhow::format_err!("Unable to start task {id} with agent {agent_id}")).map(|_| ()); // tokio send
                            let _ = dbg!(send);
                            cloned_token2.cancelled().await;
                        } else {
                            let instant = std::time::Instant::now();
                            let result = opts
                                .run(ONCE.get_or_init(|| async { PortPool::default() }).await)
                                .in_current_span().await;
                            let timing = instant.elapsed();

                        match result {
                            Ok(_) => {
                                let now = Utc::now();
                                let status = Status::Completed;
                                let _ = sqlx::query!(
                                    "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ?",
                                    now,
                                    status,
                                    None::<String>,
                                    id
                                )
                                .execute(&pool)
                                .await?;

                                let activity = Activity::new::<String>(
                                    id, now, LevelFilter::Info, format!("Task {id} is completed in {timing:?}"), "completed".to_string(), None
                                );
                                push_task_activity(&pool, &activity).await?;
                            }
                            Err(err) => {
                                tracing::error!("run task {id} failed: {err:#}");
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
                                .execute(&pool)
                                .await?;
                                let context = json!({
                                    "code": 0xFFFFi32,
                                    "message": err,
                                });
                                let activity = Activity::new::<String>(id, now, LevelFilter::Warn, format!("Task {id} failed with: {err:#}"), "failed", serde_json::to_string(&context).unwrap());
                                push_task_activity(&pool, &activity).await?;
                            }
                        }};
                    }
                    // span.exit();
                    return Ok::<(), anyhow::Error>(())
                }.in_current_span() => {
                    let _ = result?;
                    tracing::info!("task {} done", id);
                }
            }
            Ok(())
        }.instrument(span.clone());
        let handle = if let Some(rt) = self.runtime.as_ref() {
            tracing::info!("spawn task with exist runtime");
            rt.spawn(task_handler)
        } else {
            tokio::spawn(task_handler)
        };

        let _ = span.enter();
        self.tasks.write().await.insert(id, (handle, token));
        Ok(())
    }

    pub async fn tasks(&self, mut filter: TaskFilter) -> anyhow::Result<Vec<TaskDetail>> {
        tracing::info!("list tasks");
        let condition = filter.to_sql_conditions()?;
        let mut tasks = sqlx::query_as::<_, Task>(&format!(
            "select * from task_with_labels where {condition} order by created_at desc"
        ))
        .fetch_all(&self.pool)
        .await
        .context("Database error")?;

        if filter.has_labels_filter() {
            filter.filter_task_labels(&mut tasks);
        }

        let span = tracing::trace_span!("request_tasks", "url" = "GET /tasks");
        let _guard = span.enter();
        counter!("tasks", tasks.len() as u64);

        tasks.iter_mut().for_each(|task| task.backport_labels());
        Ok(tasks.into_iter().map(TaskDetail::new).collect())
    }

    pub async fn tasks_count(&self, filter: TaskFilter) -> anyhow::Result<usize> {
        let tasks = self.tasks(filter).await?;
        Ok(tasks.len())
    }

    pub async fn validate_connector_license(&self, from: &Dsn, to: &Dsn) -> anyhow::Result<()> {
        let builder = TaosBuilder::from_dsn(to)?;
        let taos = builder.build().await?;
        let is_enterprise = builder.is_enterprise_edition().await?;

        let assert_enterprise = builder.assert_enterprise_edition().await;

        #[cfg(not(feature = "disable-enterprise-only-validation"))]
        if let Err(err) = assert_enterprise {
            anyhow::bail!(
                format!("{err:?}. Only enterprise edition is supported. If it's not your case, please contact us.")
            )
        }
        // is cloud?
        if to
            .protocol
            .as_ref()
            .map(|p| match p.as_str() {
                "http" | "https" | "ws" | "wss" => true,
                _ => false,
            })
            .unwrap_or(false)
            && is_enterprise
        {
            return Ok(());
        }

        let endpoint = match (
            from.addresses[0].host.as_deref(),
            from.addresses[0].port.as_ref(),
        ) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => format!("{host}"),
            (None, Some(port)) => format!(":{port}"),
            (None, None) => format!(""),
        };
        let cluster_id: i64 = taos
            .query_one("select id from information_schema.ins_cluster")
            .await
            .map_err(|err| anyhow::format_err!("Cannot retrieve cluster id: {err}"))?
            .unwrap();
        let exists : u32 =
                    sqlx::query_scalar(&format!("select count(*) from tasks join labels where key='cluster-id' and `value` = '{}' and deleted = false and `from` like '{}://%{}%';",cluster_id, from.driver, endpoint))
                        .fetch_one(&self.pool)
                        .await?;
        if exists > 0 {
            return Ok(());
        }

        let used: Vec<String> = sqlx::query_scalar(&format!("select `from` from tasks join labels where key='cluster-id' and `value` = '{}' and deleted = false and `from` like '{}://%';",cluster_id, from.driver))
                        .fetch_all(&self.pool)
                        .await?;
        let used = used
            .into_iter()
            .map(|s| s.parse::<Dsn>().unwrap().addresses[0].to_string())
            .collect::<std::collections::HashSet<_>>()
            .len();

        // let license = taos.query_one(sql)
        let connector = match from.driver.as_str() {
            "opcua" => "opc_ua",
            "opcda" => "opc_da",
            "influxdb" => "influxdb",
            "opentsdb" => "opentsdb",
            "pi" => "pi",
            "kafka" => "kafka",
            "mqtt" => "mqtt",
            _ => unreachable!(),
        };

        let license: Option<ConnectorLicense> = taos
            .query_one::<_, String>(format!(
                "select `{connector}` from information_schema.ins_grants"
            ))
            .await
            .unwrap_or(None)
            .and_then(|s| serde_json::from_str(&s).ok());

        if let Some(license) = license {
            if license.is_expired() {
                anyhow::bail!(
                            "Connector {connector} expired, please contact the database administrator for license",
                        )
            } else {
                match license.number {
                    0 => anyhow::bail!("Connector {connector} is disabled by license"),
                    n if n > 0 => {
                        if used >= n as usize {
                            anyhow::bail!("Connector {connector} reaches connection number limit({n}) by license");
                        }
                    }
                    _ => (),
                }
            }
        } else {
            anyhow::bail!("Only enterprise version is supported for connector {connector}")
        }
        Ok(())
    }

    #[instrument(skip_all, name = "task::create", parent = None, fields(
        task.source = %mask_dsn(&task.from.parse().unwrap()),
        task.sink = %mask_dsn(&task.to.parse().unwrap()),
        task.agent = task.via,
    ))]
    pub async fn create(&self, mut task: NewTask) -> anyhow::Result<TaskDetail> {
        tracing::info!("create new task");
        let from: Dsn = task
            .from
            .parse()
            .map_err(|err| anyhow::format_err!("Invalid data source `{}`: {err}", task.from))?;

        let to: Dsn = task
            .to
            .parse()
            .map_err(|err| anyhow::format_err!("Invalid target `{}`: {err}", task.to))?;

        match from.driver.as_str() {
            "opcua" | "opcda" | "influxdb" | "opentsdb" | "pi" | "mqtt" | "kafka" => {
                self.validate_connector_license(&from, &to).await?;
            }
            _ => (),
        }
        if let Some(topic) = task.oneshot_topic.as_deref() {
            if topic.len() > 64 {
                anyhow::bail!("Max length of topic name is 64, please rewrite the topic name");
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

        if task.clear {
            if to.driver == "taos" {
                taosx_core::utils::clear_database(&to)
                    .await
                    .with_context(|| format!("Failed to clear target database with {to}"))?;
            }
        }
        task.patch_labels();
        let now = chrono::Utc::now();
        let res = sqlx::query(
            "INSERT INTO tasks (`name`, `from`, `oneshot_topic`, `to`, `jobs`, `compression_level`, \
                 `created_at`, `status`, `after_delete`, `trigger`, `via`, `parser`) \
                 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&task.name)
        .bind(&task.from)
        .bind(&task.oneshot_topic)
        .bind(&task.to)
        .bind(&task.jobs)
        .bind(&task.compression_level)
        .bind(&now)
        .bind(&Status::Created)
        .bind(&task.after_delete)
        .bind(&task.trigger)
        .bind(&task.via)
        .bind(&task.parser)
        .execute(&self.pool)
        .await?;
        let id = res.last_insert_rowid();

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
        .await?;

        // let opts = taosx::TaskOpts::try_from(task.clone())?;
        let mut task = self.get(id).await?.unwrap();
        task.backport_labels();
        task.agent = agent;

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
            .await?;
            let context =
                json!({ "code": 0xFFFFi32, "error": err.to_string(), "task": id }).to_string();
            let activity = format!("Trying to start task but failed");
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
            .await?;
            task.reason.replace(err.to_string());
            task.status = status;
        }
        Ok(task.into())
    }

    #[instrument(skip_all, name = "task::update", parent = None, fields(task.id = id))]
    pub async fn update(&self, id: i64, task: UpdateTask) -> anyhow::Result<Option<TaskDetail>> {
        tracing::info!("update task {id}");
        if let Some(topic) = task.oneshot_topic.as_deref() {
            if topic.len() > 64 {
                anyhow::bail!("Max length of topic name is 64, please rewrite the topic name");
            }
        }

        let mut sql = Vec::new();
        macro_rules! add_bind_sql {
            ($($field:ident )*) => {
                $(if task.$field.is_some() {
                    sql.push(concat!("`", stringify!($field), "` = ?"));
                })*
            };
        }
        add_bind_sql!(name stream_type from from_cluster oneshot_topic to to_cluster jobs compression_level force via parser);

        if sql.len() == 0 {
            let task = self.get(id).await?.unwrap();
            self.start_task(&task).await?;
            return Ok(Some(task));
        }

        let query = format!("UPDATE `tasks` SET {} WHERE `id` = {}", sql.join(","), id);
        let mut query = sqlx::query(&query);

        macro_rules! bind_fields {
            ($($field:ident )*) => {
                $(if let Some(field) = task.$field.as_ref() {
                    query = query.bind(field);
                })*
            };
        }
        bind_fields!(name stream_type from from_cluster oneshot_topic to to_cluster jobs compression_level force via parser);

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
        .await?;

        if res.rows_affected() == 1 {
            let task = self.get(id).await?.unwrap();

            self.start_task(&task.task).await?;
            Ok(Some(task.into()))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip_all, name = "task::start", parent = None, fields(task.id = id))]
    pub async fn start(&self, id: i64) -> anyhow::Result<Option<()>> {
        let task = self.get(id).await?;

        if task.is_none() {
            return Ok(None);
        }

        let task = task.unwrap();

        self.start_task(&task.task).await.map(Some)
    }

    #[instrument(skip_all, name = "task::get", parent = None, fields(task.id = id))]
    pub async fn get(&self, id: i64) -> anyhow::Result<Option<TaskDetail>> {
        let task: Option<Task> = sqlx::query_as("select * from task_with_labels where id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(task
            .map(|mut t| {
                t.backport_labels();
                t
            })
            .map(Into::into))
    }
    #[instrument(skip_all, name = "task::delete", parent = None, fields(task.id = id))]
    pub async fn delete(&self, id: i64) -> anyhow::Result<Option<TaskDetail>> {
        {
            if let Some((handle, token)) = self.tasks.write().await.remove(&id) {
                token.cancel();
                if !handle.is_finished() {
                    // token.cancel();
                    tracing::info!("Cancel task {id} before deleted");
                    let _ = handle.await;
                }
            }
        }
        let now = Utc::now();
        let res = sqlx::query_as_unchecked!(
            Task,
            "UPDATE tasks SET `deleted` = TRUE, `last_modified_at` = ? where id = ?",
            now,
            id
        )
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 1 {
            tracing::info!("successfully deleted task by id {id}");
        }

        let task: Option<Task> = sqlx::query_as("select * from task_with_labels where id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        if task.is_none() {
            return Ok(None);
        }
        let mut task = task.unwrap();
        task.backport_labels();
        if let Some(topic) = task.oneshot_topic.as_deref() {
            let mut dsn: Dsn = task.from.parse()?;
            let _ = dsn.subject.take();
            let builder = TaosBuilder::from_dsn(dsn).context("cannot drop oneshot topic")?;
            let taos = builder.build().await.context("cannot drop oneshot topic")?;
            let mut retries = 0;
            loop {
                if retries > 20 {
                    tracing::error!("can not drop topic {topic}");
                    break;
                }
                if let Err(_err) = taos.exec(format!("drop topic if exists {topic}")).await {
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                } else {
                    break;
                }
            }
        }

        if let Some(action) = task.after_delete.as_deref() {
            if task.to.starts_with("local") && action == "clear" {
                let dsn: Dsn = task.to.parse()?;
                // std::mem::drop(task);
                tokio::spawn(async move { taosx_core::utils::clear_local(&dsn).await });
            }
        }
        sqlx::query!("DELETE FROM tasks where id = ?", id)
            .execute(&self.pool)
            .await?;

        let from: Dsn = task.from.parse()?;
        let to: Dsn = task.to.parse()?;
        let opts = TaskOpts {
            from,
            transform: vec![],
            to,
            parser: None,
            jobs: 0,
            compression_level: None,
            force: false,
            cancel: Default::default(),
            with_agent: None,
            offsets: Arc::new(Default::default()),
            transferred: None,
            span: tracing::info_span!(
                "task::delete",
                task.id = id,
                trace_id = tracing::field::Empty
            ),
            task_id: Some(task.id.to_string()),
        };
        opts.delete_task().await?;

        Ok(Some(task.into()))
    }

    #[instrument(skip_all, name = "task::stop", parent = None, fields(task.id = id))]
    pub async fn stop(&self, id: i64) -> anyhow::Result<Option<()>> {
        tracing::info!("Stop task by id {id}");
        if let Some((handle, token)) = { self.tasks.write().await.remove(&id) } {
            tracing::info_span!("stop_task", task = id);
            let now = Utc::now();

            let agent: Option<(Option<i64>, Status)> = sqlx::query_as::<_, (Option<i64>, Status)>(
                "select via, status from tasks where id = ?",
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
            let (agent, status) = agent.ok_or_else(|| anyhow::anyhow!(""))?;

            if !matches!(
                status,
                Status::Running | Status::Created | Status::Interrupted
            ) {
                tracing::warn!("Trying to stop task {id} but it's not in running state");
                return Ok(Some(()));
            }
            if let Some(agent_id) = agent {
                if let Some(worker) = self.agent_tasks.read().await.get(&agent_id) {
                    let res = worker.send(AgentAction::Stop(id));
                    if let Err(err) = res {
                        tracing::error!(
                            agent = agent_id,
                            backtrace = ?err,
                            "trying to stop task via agent:{agent_id} but failed: {err:#}"
                        );
                    }
                } else {
                    tracing::warn!("trying to stop task via agent:{agent_id} but not found");
                }
            }

            let res = sqlx::query_as_unchecked!(
                Task,
                "UPDATE tasks SET `last_modified_at` = ?, `status` = ? where id = ?",
                now,
                Status::Stopped,
                id
            )
            .execute(&self.pool)
            .await?;
            // dbg!(res);
            if res.rows_affected() == 1 {
                tracing::info!("successfully stop task by id {id}");
            }

            sqlx::query!(
                "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)",
                id,
                now,
                LevelFilter::Info,
                "stop",
                "stopped",
                None::<String>
            )
            .execute(&self.pool)
            .await?;

            token.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(10), handle).await??;
            Ok(Some(()))
        } else {
            Ok(Some(()))
        }
    }

    pub async fn task_activities(
        &self,
        id: i64,
        filter: &AgentActivityFilter,
    ) -> anyhow::Result<Vec<Activity>> {
        let cond = filter.condition();
        let sql = format!("select * from task_activities where `id` = {id} {cond}");
        let items = sqlx::query_as(&sql).fetch_all(&self.pool).await?;
        Ok(items)
    }

    pub async fn push_task_status(&self, status: &TaskStatus) -> anyhow::Result<()> {
        let id = status.id;
        match status.action.as_str() {
            "failed" => {
                tracing::error!(
                    "run task {id} failed with: {:?}, please check the task information",
                    status.message
                );
                // let err = err.to_string();
                let at = status.at;
                let _ = sqlx::query!(
                    "UPDATE tasks SET finished_at = ?, status = ?, reason = ? WHERE id = ? AND deleted != TRUE",
                    at,
                    Status::Failed,
                    status.message,
                    id
                )
                .execute(&self.pool)
                .await?;
                sqlx::query!(
                    "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)",
                    id,
                    at,
                    LevelFilter::Error,
                    "failed",
                    status.message,
                    status.context,
                )
                .execute(&self.pool)
                .await?;
                if let Some((handle, token)) = self.tasks.write().await.remove(&id) {
                    tracing::error!("Cancel task by id {id}");
                    token.cancel();
                    let _ = handle.await?;
                    // handle.abort();
                    // if !handle.is_finished() {
                    //     // token.cancel();
                    //     tracing::error!("Cancel task by id {id}");
                    //     let _ = handle.await;
                    // }
                }
                Ok(())
            }
            action => {
                sqlx::query!(
                    "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)",
                    id,
                    status.at,
                    LevelFilter::Info,
                    action,
                    status.message,
                    status.context,
                )
                .execute(&self.pool)
                .await?;
                tracing::error!("Invalid task action: {action}");
                Ok(())
            }
        }
    }

    pub async fn stop_all(&self) -> anyhow::Result<()> {
        for (id, (handle, token)) in self.tasks.write().await.drain() {
            token.cancel();
            if !handle.is_finished() {
                // token.cancel();
                tracing::error!("Cancel task by id {id}");
                let _ = handle.await;
            }

            let now = Utc::now();
            let res = sqlx::query!(
                "UPDATE tasks SET finished_at = ?, status = ? WHERE id = ? AND status not in (?, ?, ?)",
                now,
                Status::Cancelled,
                id,
                Status::Completed, Status::Stopped, Status::Failed
            )
            .execute(&self.pool)
            .await?;

            let activity = format!("Stopping all tasks.., now {id}");

            sqlx::query!(
                "INSERT INTO task_activities (`id`,`at`, `level`, `activity`, `status`, `context`) values(?, ?, ?, ?, ?, ?)",
                id,
                now,
                LevelFilter::Info,
                activity,
                "ok",
                None::<String>
            )
            .execute(&self.pool)
            .await?;
            // dbg!(res);
            if res.rows_affected() == 1 {
                tracing::info!("successfully cancelled task by id {id}");
            }
        }
        Ok(())
    }

    pub async fn _clear(&self) -> anyhow::Result<()> {
        for (id, (handle, token)) in self.tasks.write().await.drain() {
            token.cancel();
            if !handle.is_finished() {
                // token.cancel();
                tracing::error!("Cancel task by id {id}");
                let _ = handle.await;
            }

            let res = sqlx::query_as_unchecked!(
                Task,
                "UPDATE tasks SET `deleted` = TRUE where id = ?",
                id
            )
            .execute(&self.pool)
            .await?;
            // dbg!(res);
            if res.rows_affected() == 1 {
                tracing::info!("successfully deleted task by id {id}");
            }
        }
        Ok(())
    }

    pub async fn offsets(
        &self,
        id: i64,
    ) -> anyhow::Result<Option<Arc<DashMap<String, Vec<Assignment>>>>> {
        let offsets = self.offsets.read().await.get(&id).cloned();
        Ok(offsets)
    }

    pub async fn find_agent_by_name_and_cluster_id(
        &self,
        name: &str,
        cluster_id: Option<&str>,
        id: Option<usize>,
    ) -> anyhow::Result<Vec<Agent>> {
        let mut sql = if id.is_some() {
            format!(
                "select * from agents_view where name = '{}' and id != '{}'",
                name,
                id.unwrap()
            )
        } else {
            format!("select * from agents_view where name = '{}'", name)
        };
        if cluster_id.is_some() {
            sql.push_str(format!(" and cluster_id = '{}'", cluster_id.unwrap()).as_str());
        }
        let result: Vec<Agent> = sqlx::query_as(sql.as_str()).fetch_all(&self.pool).await?;
        Ok(result)
    }

    pub async fn create_agent(&self, agent: AgentProps) -> anyhow::Result<AgentWithToken> {
        let result = self
            .find_agent_by_name_and_cluster_id(&agent.name, Some(&agent.cluster_id), None)
            .await?;
        if result.len() > 0 {
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
        self.push_agent_activity(&activity).await?;
        let secret = self.jwt_secret().await?;
        self.get_agent_by_id(id)
            .await
            .map(|agent| agent.unwrap().with_token(secret))
    }

    pub async fn get_agents(&self, filter: AgentFilter) -> anyhow::Result<Vec<Agent>> {
        let sql = match filter.to_sql_condition() {
            Some(cond) => format!("select * from agents_view where {cond}"),
            None => format!("select * from agents_view"),
        };
        let agent = sqlx::query_as(&sql).fetch_all(&self.pool).await?;
        Ok(agent)
    }

    pub async fn get_agent_with_token(&self, token: &AgentToken) -> anyhow::Result<Option<Agent>> {
        let claims = token.jwt_decode(self.jwt_secret().await?)?;
        let agent = self.get_agent_by_id(claims.sub).await?;
        if agent.is_some() {
            //
        }
        Ok(agent)
    }

    pub async fn get_agent_by_id(&self, agent_id: i64) -> anyhow::Result<Option<Agent>> {
        let sql = format!("select * from agents_view where id = {agent_id}");
        let agent = sqlx::query_as(&sql).fetch_optional(&self.pool).await?;
        Ok(agent)
    }

    pub async fn init_agent_worker(&self, agent_id: i64) {
        let exists = { self.agent_tasks.read().await.contains_key(&agent_id) };
        if !exists {
            let mut write = self.agent_tasks.write().await;
            write.insert(agent_id, AgentTasks::new());
            write.get(&agent_id).unwrap().spawn_listener();
        }
    }

    /// Check if agent is online.
    pub async fn agent_alive(&self, agent_id: i64) -> bool {
        self.agent_tasks.read().await.contains_key(&agent_id)
    }

    /// Update agent activities.
    pub async fn push_agent_activity(&self, activity: &Activity) -> anyhow::Result<()> {
        if activity.status == "pending" {
            self.agent_tasks.write().await.remove(&activity.id);
        }
        push_agent_activity(&self.pool, activity).await
    }

    pub async fn push_task_activity(&self, activity: &Activity) -> anyhow::Result<()> {
        push_task_activity(&self.pool, activity).await
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

    pub async fn agent_disconnect(&self, agent_id: i64) -> anyhow::Result<()> {
        let tasks = self
            .tasks(TaskFilter {
                via: Some(agent_id),
                status: Some("running".to_string()),
                ..Default::default()
            })
            .await?;
        for task in tasks {
            let id = task.id;
            let _ = sqlx::query(&format!(
                "UPDATE tasks SET `status` = `cancelled` WHERE id = {id} AND `status` == 'running'"
            ))
            .execute(&self.pool)
            .await;
            let activity = Activity::new::<String>(
                id,
                Utc::now(),
                LevelFilter::Info,
                format!("Task {id} is cancelled because agent is disconnected"),
                "cancelled",
                None,
            );
            self.push_task_activity(&activity).await?;
        }
        Ok(())
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
        if result.len() > 0 {
            anyhow::bail!("Agent name {} exists", name);
        }
        // let sql = update.update_agent_with(agent_id);
        sqlx::query("UPDATE agents SET `name` = ? WHERE id = ?")
            .bind(name)
            .bind(agent_id)
            .execute(&self.pool)
            .await?;
        let secret = self.jwt_secret().await?;
        Ok(self
            .get_agent_by_id(agent_id)
            .await?
            .map(|a| a.with_token(&secret)))
    }

    pub async fn delete_agent(&self, agent_id: i64) -> anyhow::Result<()> {
        let ids = sqlx::query_as::<_, (i64,)>("select id from tasks where via = ?")
            .bind(agent_id)
            .fetch_all(&self.pool)
            .await?;
        if !ids.is_empty() {
            anyhow::bail!("should delete associated tasks before delete agent");
        }

        sqlx::query("delete from agent_activities where id = ?")
            .bind(agent_id)
            .execute(&self.pool)
            .await?;
        tracing::info!("Deleted agent with id {agent_id}");

        sqlx::query("delete from agents where id = ?")
            .bind(agent_id)
            .execute(&self.pool)
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
                    .await?;
                let secret = if let Some(value) = secret {
                    value
                } else {
                    use rand::distributions::{Alphanumeric, DistString};
                    let random = Alphanumeric.sample_string(&mut rand::thread_rng(), 64);

                    sqlx::query(&format!("insert into `secret` values('{random}')"))
                        .execute(&self.pool)
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

    pub async fn list_datasets_via_agent(
        &self,
        agent_id: i64,
        req: DataSetsReq,
    ) -> anyhow::Result<Vec<DataSet>> {
        let (sender, recv) = flume::bounded(1);
        let agent_tasks = self.agent_tasks.read().await;
        let agent = agent_tasks
            .get(&agent_id)
            .ok_or_else(|| anyhow::format_err!("Unknown or inactive agent {agent_id}"))?;

        agent.send(AgentAction::ListDataSets(req, sender))?;
        let data = recv.recv_async().await??;
        Ok(data)
    }

    pub async fn cluster_transferred(
        &self,
        cluster_id: i64,
    ) -> anyhow::Result<Vec<ConnectorTransferred>> {
        let vec: Vec<ConnectorTransferred> =
            sqlx::query_as("select * from connector_transferred where cluster_id = ?")
                .bind(cluster_id)
                .fetch_all(&self.pool)
                .await?;
        Ok(vec)
    }
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
    cluster_id: Option<String>,
    user_id: Option<String>,
}

impl AgentFilter {
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
#[derive(Serialize, Deserialize, ToSchema, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Status {
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
    /// Task has been finished.
    Completed,
    /// Task completed with error.
    Failed,
    /// For never stop task, it's not in service, but will retry.
    Interrupted,
    /// Manually stopped by API.
    Stopped,
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Created => f.write_str("created"),
            Status::Cleared => f.write_str("cleared"),
            Status::Started => f.write_str("started"),
            Status::Running => f.write_str("running"),
            Status::Cancelled => f.write_str("cancelled"),
            Status::Completed => f.write_str("completed"),
            Status::Failed => f.write_str("failed"),
            Status::Interrupted => f.write_str("interrupted"),
            Status::Stopped => f.write_str("stopped"),
        }
    }
}

impl Status {
    fn as_str(&self) -> &'static str {
        match self {
            Status::Created => "created",
            Status::Cleared => "cleared",
            Status::Started => "started",
            Status::Running => "running",
            Status::Cancelled => "cancelled",
            Status::Completed => "completed",
            Status::Failed => "failed",
            Status::Interrupted => "interrupted",
            Status::Stopped => "stopped",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid status: {0}")]
pub struct InvalidStatus(String);

impl FromStr for Status {
    type Err = InvalidStatus;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "created" => Ok(Status::Created),
            "cleared" => Ok(Status::Cleared),
            "started" => Ok(Status::Started),
            "running" => Ok(Status::Running),
            "cancelled" => Ok(Status::Cancelled),
            "completed" => Ok(Status::Completed),
            "failed" => Ok(Status::Failed),
            "interrupted" => Ok(Status::Interrupted),
            "stopped" => Ok(Status::Stopped),
            _ => Err(InvalidStatus(s.to_string())),
        }
    }
}

impl<'r, DB: sqlx::Database> sqlx::Decode<'r, DB> for Status
where
    &'r str: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef,
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
        buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
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
    oneshot_topic: Option<String>,

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
    jobs: u16,

    /// Agent Id
    #[serde(skip_serializing_if = "Option::is_none")]
    via: Option<i64>,

    /// Compression level when need (for backup only)
    compression_level: Option<u8>,

    /// Created time.
    #[schema(read_only)]
    #[serde(with = "datetime_format")]
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
    name: Option<String>,

    /// Task trigger events, default will be oneshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<String>,

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
}
/// Task Activity
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::FromRow)]
pub struct TaskActivity {
    /// Task id.
    #[schema(read_only)]
    id: i64,
    /// Stopped time.
    #[schema(read_only)]
    #[serde(with = "datetime_format")]
    at: DateTime<Utc>,

    /// Level
    level: LevelFilter,

    /// Activity
    #[schema(read_only)]
    activity: String,

    /// Activity result.
    status: String,
    /// Context
    #[schema(read_only)]
    context: Option<String>,
}

lazy_static::lazy_static! {
    /// Data source definition map/list
    pub static ref DATA_SOURCE_DEFINITIONS: LinkedHashMap<String, DataSourceDefinition> = {
        let mut def: Vec<DataSourceDefinition> = Vec::new();
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/tmq.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/taos.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/pi.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/pi-backfill.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/opcua.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/opcda.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/influxdb.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/opentsdb.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/mqtt.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/kafka.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/en/csv.yaml")).unwrap());
        for ds in &mut def {
            ds.compute();
        }
        def.into_iter().map(|ds| (ds.id.to_string(), ds)).collect()
    };
    pub static ref DATA_SOURCE_DEFINITIONS_CN: LinkedHashMap<String, DataSourceDefinition> = {
        let mut def: Vec<DataSourceDefinition> = Vec::new();
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/tmq.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/taos.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/pi.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/pi-backfill.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/opcua.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/opcda.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/influxdb.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/opentsdb.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/mqtt.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/kafka.yaml")).unwrap());
        def.push(serde_yaml::from_str(include_str!("../data_sources/cn/csv.yaml")).unwrap());
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

    /// Expanded DSN for source.
    from_expand: Option<ExpandedDsn>,
    /// Expanded DSN definition with values.
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    from_detail: Option<DataSourceDefinition>,

    /// Expanded DSN for sink.
    to_expand: Option<ExpandedDsn>,
    /// Expanded DSN definition with values.
    #[serde(default)]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    to_detail: Option<DataSourceDefinition>,

    /// Agent
    #[serde(skip_serializing_if = "Option::is_none")]
    agent: Option<Agent>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parser: Option<serde_json::Value>,
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
            parser: task.parser.clone(),
            task,
            from_expand: None,
            from_detail: None,
            to_expand: None,
            to_detail: None,
            agent: None,
        }
    }

    pub(super) fn status(&self) -> Status {
        self.task.status
    }

    pub fn expand_detail(self, lang: Option<String>) -> Self {
        let value = self.task;
        let parser = value.parser.clone();
        let from_dsn: Dsn = value.from.as_str().parse().unwrap();
        let to_dsn: Dsn = value.to.as_str().parse().unwrap();
        if lang.is_some() {
            match lang.unwrap().as_str() {
                "zh" => TaskDetail {
                    from_expand: Some(ExpandedDsn::from(from_dsn.clone())),
                    from_detail: DATA_SOURCE_DEFINITIONS_CN
                        .get(&from_dsn.driver)
                        .map(|d| d.clone().values_from(from_dsn)),
                    to_expand: Some(ExpandedDsn::from(to_dsn.clone())),
                    to_detail: DATA_SOURCE_DEFINITIONS_CN
                        .get(&to_dsn.driver)
                        .map(|d| d.clone().values_from(to_dsn)),
                    task: value,
                    agent: None,
                    parser,
                },
                _ => TaskDetail {
                    from_expand: Some(ExpandedDsn::from(from_dsn.clone())),
                    from_detail: DATA_SOURCE_DEFINITIONS
                        .get(&from_dsn.driver)
                        .map(|d| d.clone().values_from(from_dsn)),
                    to_expand: Some(ExpandedDsn::from(to_dsn.clone())),
                    to_detail: DATA_SOURCE_DEFINITIONS
                        .get(&to_dsn.driver)
                        .map(|d| d.clone().values_from(to_dsn)),
                    task: value,
                    agent: None,
                    parser,
                },
            }
        } else {
            TaskDetail {
                from_expand: Some(ExpandedDsn::from(from_dsn.clone())),
                from_detail: DATA_SOURCE_DEFINITIONS
                    .get(&from_dsn.driver)
                    .map(|d| d.clone().values_from(from_dsn)),
                to_expand: Some(ExpandedDsn::from(to_dsn.clone())),
                to_detail: DATA_SOURCE_DEFINITIONS
                    .get(&to_dsn.driver)
                    .map(|d| d.clone().values_from(to_dsn)),
                task: value,
                agent: None,
                parser,
            }
        }
    }

    pub fn expand(mut self) -> Self {
        let value = &self.task;
        let from_dsn: Dsn = value.from.as_str().parse().unwrap();
        let to_dsn: Dsn = value.to.as_str().parse().unwrap();
        self.from_expand = Some(from_dsn.into());
        self.to_expand = Some(to_dsn.into());
        self
    }

    pub fn _detail(mut self) -> Self {
        let value = &self.task;
        let from_dsn: Dsn = value.from.as_str().parse().unwrap();
        let to_dsn: Dsn = value.to.as_str().parse().unwrap();
        self.from_detail = DATA_SOURCE_DEFINITIONS
            .get(&from_dsn.driver)
            .map(|d| d.clone().values_from(from_dsn));
        self.to_detail = DATA_SOURCE_DEFINITIONS
            .get(&to_dsn.driver)
            .map(|d| d.clone().values_from(to_dsn));
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
}
#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, PartialOrd, ToSchema)]
pub struct Labels(Option<Vec<String>>);

impl Labels {
    /// Check if labels is empty.
    fn is_empty(&self) -> bool {
        self.0.as_ref().map(|v| v.is_empty()).unwrap_or(true)
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
/// - *from*: The data source DSN.
/// - *to*: The data sink DSN.
///
#[derive(
    Serialize, Deserialize, ToSchema, Clone, Debug, sqlx::Decode, sqlx::Encode, sqlx::FromRow,
)]
#[schema(
    example = json!({
        "name": "demo",
        "from": "tmq:///test?group.id=test-test2&client.id=taosx",
        "to": "taos:///test2"
    })
)]
pub(crate) struct NewTask {
    stream_type: Option<String>,
    /// Task name.
    #[schema(example = "demo")]
    name: Option<String>,
    /// Task trigger events, default will be oneshot.
    ///
    /// For schedule trigger:
    ///
    /// - Run hourly/daily/weekly/monthly: "schedule:@daily"
    /// - Run with crontab schedule: "schedule:@daily", checkout https://crontab.guru/ for human-readable crontab.
    #[schema(example = "schedule:@daily")]
    pub trigger: Option<String>,
    /// The stream data source.
    #[schema(example = "tmq:///test")]
    from: String,
    /// The stream data source cluster id.
    from_cluster: Option<String>,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    // #[serde(default)]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    #[schema(example = "local:/tmp/taosx/test")]
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
        let mut labels = match self.labels.take() {
            Some(labels) => labels,
            None => {
                vec![]
            }
        };
        if let Some(value) = self.stream_type.as_ref() {
            labels.push(format!("stream_type::{}", value));
        }

        if let Some(value) = self.from_cluster.as_deref() {
            labels.push(format!("from_cluster::{value}"))
        }
        if let Some(value) = self.to_cluster.as_deref() {
            labels.push(format!("to_cluster::{value}"))
        }
        if labels.len() > 0 {
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
    from: String,

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
        let mut labels = match value.labels {
            Some(labels) => labels,
            None => {
                vec![]
            }
        };
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
    trigger: Option<String>,
    /// *Deprecated*.
    stream_type: Option<String>,
    /// The stream data source.
    from: Option<String>,
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
    name: Option<String>,
    stream_type: Option<String>,
    from_cluster: Option<String>,
    to_cluster: Option<String>,
    status: Option<String>,
    start_create_time: Option<String>,
    end_create_time: Option<String>,
    with_deleted: Option<bool>,
    labels: Option<String>,
    any_labels: Option<String>,
    without_labels: Option<String>,
    via: Option<i64>,
}

#[derive(Serialize, Deserialize, Default, Clone, IntoParams)]
#[serde(default)]
pub struct TaskDecorator {
    expand: Option<bool>,
    detail: Option<bool>,
    lang: Option<String>,
}

impl TaskFilter {
    pub fn to_sql_conditions(&mut self) -> std::result::Result<String, std::fmt::Error> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_agent() -> anyhow::Result<()> {
        // std::env::set_var("RUST_LOG", "debug");
        // pretty_env_logger::init();
        let controller = TaskController::from_sqlite("sqlite::memory:").await?;
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
        // pretty_env_logger::init();
        let controller = TaskController::from_sqlite("sqlite::memory:").await?;

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
        let controller = TaskController::from_sqlite("sqlite::memory:").await?;
        let agent = controller
            .create_agent(AgentProps {
                dsn: "".to_string(),
                name: "a1".to_string(),
                cluster_id: "".to_string(),
                user_id: "".to_string(),
            })
            .await?;
        dbg!(&agent);

        let task_props: NewTask = serde_json::from_str(&format!(
            r#"
        {{
            "from": "mqtt:///db2",
            "to":"taos:///db2",
            "via": 1
        }}
        "#,
        ))
        .unwrap();

        let task = controller.create(task_props).await;
        assert!(task.is_err());
        dbg!(&task);
        assert!(task
            .unwrap_err()
            .to_string()
            .contains("Agent 1 is not alive"));

        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_task_offset() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "taos=info");
        pretty_env_logger::init();

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

        taos.exec_many(["drop database if exists db2"]).await?;

        let controller = TaskController::from_sqlite("sqlite::memory:").await?;

        let task_props: NewTask = serde_json::from_str(&format!(
            r#"
        {{
            "from": "tmq:///ws_abc1",
            "to":"taos:///db2",
            "force": true
        }}
        "#,
        ))
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
}
