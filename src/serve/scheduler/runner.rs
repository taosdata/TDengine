use std::{
    fmt::{Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering},
    },
    time::Duration,
};

use anyhow::bail;
use dashmap::DashMap;
use metrics::atomics::AtomicU64;
use multi_index_map::MultiIndexMap;
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use taoslog::{QidManager, utils::QidMetadataGetter};
use taosx_task::TaskOpts;
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio_cron_scheduler::JobScheduler;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, instrument, warn};
use uuid::Uuid;

use taosx_core::{ConnectorLicense, DataSet, get_data_dir, utils::port_pool::PortPool};
use taosx_core::{
    TaskNotify, TaskNotifyReceiver,
    core_metrics::{
        CoreMetrics, GLOBAL_METRICS, TaskMetrics, auto_save_task_metrics, get_metrics_arc_from_i64,
        init_task_metrics, save_task_metrics_finally,
    },
    dsv::DataSourceValidation,
    plugins::{self, transform::sample::DsSamples},
    sink::ipc_metric::IpcMetrics,
    task_set::prelude::EventLevel,
    utils::{
        dsn::json_to_dsn, get_main_version_from_server_version, get_server_version,
        sql::get_timestamp_range,
    },
};
use taosx_core::{plugins::transform::sample::DsSampleIn, utils::trace::Qid};

use crate::serve::{
    controller::{
        AgentAction, Status, Task,
        agent::Activity,
        license::LicenseValidator,
        load_breakpoints,
        trigger::{Schedule, StopCondition, Strategy},
    },
    health,
};

use super::{
    NotifySender, SchedulerNotify, StopError,
    agent::{AgentState, AgentTask, AgentWorker},
};

#[instrument(skip_all)]
#[async_backtrace::framed]
async fn task_opts_init(
    task: &Task,
    cancel: CancellationToken,
) -> anyhow::Result<(TaskOpts, TaskNotifyReceiver)> {
    let id = task.id;
    let from = if let Some(topic) = task.oneshot_topic.as_deref() {
        // let mut from: Dsn = task.from.parse()?;
        let mut from = json_to_dsn(&serde_json::Value::String(task.from.clone()))?;
        from.set("use.topic.name", topic);
        tracing::info!("Set task from: {from}");
        from
    } else {
        // task.from.parse()?
        json_to_dsn(&serde_json::Value::String(task.from.clone()))?
    };
    let to_dsn: Dsn = task.to.parse()?;
    match from.driver.as_str() {
        "opcua" | "opcda" | "pi" | "pibackfill" => {
            let taos = TaosBuilder::from_dsn(&to_dsn)?.build().await?;
            let cluster_id: Option<i64> = taos
                .query_one("select id from information_schema.ins_cluster")
                .await
                .unwrap_or_default();
            // let license = taos.query_one(sql)
            let connector = match from.driver.as_str() {
                "opcua" => "opc_ua",
                "opcda" => "opc_da",
                "pi" | "pibackfill" => "pi",
                _ => unreachable!(),
            };
            // get tdengine server version and handle compatibility
            let server_version = get_server_version(&taos).await?;
            let (a, b, c) = get_main_version_from_server_version(&server_version).unwrap();
            let grants_sql = if a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3) {
                format!(
                    "select `limits` from information_schema.ins_grants_full where grant_name='{connector}'"
                )
            } else {
                format!("select `{connector}` from information_schema.ins_grants")
            };
            let license: Option<ConnectorLicense> = taos
                .query_one::<_, String>(grants_sql)
                .await
                .unwrap_or(None)
                .and_then(|s| serde_json::from_str(&s).ok());

            if let Some(license) = license {
                if a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3) {
                    if license.is_expired_second() {
                        anyhow::bail!(
                            "The current connector {connector} has bean expired, please contact the TDengine customer success team to get the activation code."
                        )
                    }
                } else if license.is_expired_day() {
                    anyhow::bail!(
                        "The current connector {connector} has bean expired, please contact the TDengine customer success team to get the activation code."
                    )
                }
            }
        }
        _ => {}
    };

    let breakpoints = load_breakpoints(id);

    let (notify, notify_rx) = flume::unbounded();

    let parser: Option<plugins::Parser> = task
        .parser
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()).unwrap());
    let parser = if let Some(parser) = parser {
        let pool = {
            let builder = taos::TaosBuilder::from_dsn(&to_dsn)?;
            let mut pool_config = builder.default_pool_config();
            let timeout = parser
                .global()
                .process_on_abnormal
                .connection_timeout_in_second_value;
            pool_config.timeouts.wait = Some(Duration::from_secs(timeout as u64));
            builder.with_pool_config(pool_config)?
        };
        let (_, minimum_timestamp, maximum_timestamp) =
            get_timestamp_range(&pool, &mut None, 3, &cancel).await?;
        let metrics = get_metrics_arc_from_i64(Some(id)).await;
        let parser = match parser {
            plugins::Parser::Inner(parser) => {
                let mut parser = parser;
                parser.set_maximum_timestamp(maximum_timestamp);
                if let Some(minimum_timestamp) = minimum_timestamp {
                    parser.set_minimum_timestamp(minimum_timestamp);
                }
                parser.organize_archive(task.id);
                parser.organize_cache(task.id);
                plugins::Parser::Inner(parser)
            }
            plugins::Parser::WithSample { parser, input } => {
                let mut parser = parser;
                parser.set_maximum_timestamp(maximum_timestamp);
                if let Some(minimum_timestamp) = minimum_timestamp {
                    parser.set_minimum_timestamp(minimum_timestamp);
                }
                parser.organize_archive(task.id);
                parser.organize_cache(task.id);
                plugins::Parser::WithSample { parser, input }
            }
        };
        Some(parser)
    } else {
        None
    };

    Ok((
        TaskOpts {
            transform: vec![],
            from: from.clone(),
            to: to_dsn.clone(),
            parser,
            health: task.trigger.as_ref().map(|v| v.health),
            cancel,
            // port_pool: ONCE,
            with_agent: None,
            breakpoints,
            task_id: Some(id.to_string()),
            notify,
        },
        notify_rx,
    ))
}

async fn run_task(
    global: &GlobalState,
    state: &TaskState,
    job_id: &Uuid,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    debug_assert!(state.task.via.is_none());
    let _ = state.span.clone().entered();
    let task = &state.task;
    let task_id = task.id;
    let (opts, task_rx) = task_opts_init(task, cancel).await?;
    tracing::info!("start worker");

    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    let instant = std::time::Instant::now();
    let global_sender = global.clone();
    let health_opts = state.task.trigger.as_ref().map(|v| v.health);
    let logging_abort = tokio::spawn(async move {
        use taosx_core::task_set::prelude::health_checker;
        let metrics = get_metrics_arc_from_i64(Some(task_id)).await;
        let (health_tx, health_rx) = flume::bounded(64);
        if let Some(health_opts) = health_opts {
            let (_handle, mut rx) = health_checker(health_opts, health_rx, metrics);
            let global_sender = global_sender.clone();
            tokio::spawn(async move {
                while let Ok(item) = rx.recv().await {
                    global_sender.send_task_activity(Activity::health_state(task_id, item));
                }
            });
        }
        while let Ok(notify) = task_rx.recv_async().await {
            let activity = match notify.level {
                EventLevel::Error => Activity::error(task_id, notify.message.clone()),
                EventLevel::Warn => Activity::warn(task_id, notify.message.clone()),
                EventLevel::Info => Activity::logging(task_id, notify.message.clone()),
                _ => break,
            };
            if health_opts.is_some() {
                health_tx.send_async(notify).await.ok();
            }
            global_sender.send_task_activity(activity);
        }
    });
    let res = opts.run(&global.port_pool).in_current_span().await;

    logging_abort.abort();
    tracing::Span::current().record("task.elapsed", tracing::field::debug(instant.elapsed()));
    if let Err(error) = res {
        error!(task.elapsed = ?instant.elapsed(), error.message = %error, error.backtrace = ?error);
        Err(error)
    } else {
        tracing::info!(task.elapsed = ?instant.elapsed(), "task finished");
        Ok(())
    }
}

pub type TaskId = i64;
pub type AgentId = i64;
pub type AgentTaskActivitiesReceiver = tokio::sync::broadcast::Receiver<Activity>;
pub type AgentActionsSender = tokio::sync::mpsc::Sender<(AgentId, AgentAction)>;
pub type AgentClientSender = tokio::sync::mpsc::Sender<Status>;

pub struct AgentServer {
    pub(crate) agent_actions_sender: AgentActionsSender,
    pub(crate) task_activities: AgentTaskActivitiesReceiver,
}

#[derive(Debug)]
pub enum AgentIntegrationChannel {
    Server(AgentWorker),
    Client(AgentClientSender),
}

#[derive(Debug, Clone)]
pub enum AgentRuntimeRef {
    Server(AgentWorker),
    Client(Arc<RwLock<AgentClientSender>>),
}

impl AgentRuntimeRef {
    fn new(runtime: AgentIntegrationChannel) -> Self {
        match runtime {
            AgentIntegrationChannel::Server(rt) => Self::Server(rt),
            AgentIntegrationChannel::Client(rt) => Self::Client(Arc::new(RwLock::new(rt))),
        }
    }

    pub(crate) async fn list_data_sets(
        &self,
        agent_id: i64,
        req: taosx_core::DataSetsReq,
    ) -> anyhow::Result<Vec<DataSet>> {
        match self {
            Self::Server(rt) => rt.list_data_sets(agent_id, req).await,
            Self::Client(_) => {
                bail!("not implemented")
            }
        }
    }

    pub(crate) async fn query_data_source(
        &self,
        agent_id: i64,
        req: taosx_core::QueryDataSourceReq,
    ) -> anyhow::Result<String> {
        match self {
            Self::Server(rt) => rt.query_data_source(agent_id, req).await,
            Self::Client(_) => {
                bail!("not implemented")
            }
        }
    }

    pub async fn check(&self, agent_id: i64, req: String) -> anyhow::Result<DataSourceValidation> {
        match self {
            Self::Server(rt) => rt.check(agent_id, req).await,
            Self::Client(_) => {
                bail!("not implemented")
            }
        }
    }

    pub async fn get_sample(&self, agent_id: i64, dsn: String) -> anyhow::Result<DsSamples> {
        match self {
            Self::Server(rt) => rt.get_sample(agent_id, dsn).await,
            Self::Client(_) => {
                bail!("not implemented")
            }
        }
    }

    pub async fn put_file_to_agent(
        &self,
        agent_id: i64,
        path: &str,
        content: Vec<u8>,
    ) -> anyhow::Result<()> {
        match self {
            Self::Server(rt) => rt.put_file_to_agent(agent_id, path, content).await,
            Self::Client(_) => {
                bail!("not implemented")
            }
        }
    }

    async fn insert(&self, task: AgentTask) {
        match self {
            Self::Server(rt) => {
                rt.insert(task).await;
            }
            Self::Client(_) => {}
        }
    }
    async fn remove(&self, task_id: TaskId) {
        match self {
            Self::Server(rt) => {
                rt.remove(task_id).await;
            }
            Self::Client(_) => {}
        }
    }
    async fn stop(&self, task_id: TaskId) {
        match self {
            Self::Server(rt) => {
                rt.stop(task_id).await;
            }
            Self::Client(_) => {}
        }
    }
    async fn suspend(&self, task_id: TaskId) {
        match self {
            Self::Server(rt) => {
                rt.suspend(task_id).await;
            }
            Self::Client(_) => {}
        }
    }
    pub async fn agent_is_alive(&self, agent_id: AgentId) -> bool {
        match self {
            Self::Server(rt) => rt.agent_is_alive(agent_id).await,
            Self::Client(_) => true,
        }
    }
    pub async fn push_action(&self, agent_id: i64, action: AgentAction) -> anyhow::Result<()> {
        match self {
            Self::Server(rt) => rt.push_action(agent_id, action).await,
            Self::Client(_) => {
                // todo! implement client
                Ok(())
            }
        }
    }

    pub(crate) async fn remove_task(&self, task_id: TaskId) {
        match self {
            Self::Server(rt) => rt.remove_task(task_id).await,
            Self::Client(_) => {}
        }
    }
}
#[derive(Clone)]
pub struct GlobalState {
    /// Global aliveness flag.
    pub(crate) alive: Arc<AtomicBool>,
    /// Global job scheduler.
    pub(crate) scheduler: JobScheduler,
    /// Global task activities notify sender.
    pub(crate) notify_sender: NotifySender,
    /// Global port pool.
    pub(crate) port_pool: PortPool,
    /// Global Agent task manager
    pub(crate) agent_runtime: AgentRuntimeRef,
}

impl Debug for GlobalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalState")
            .field("scheduler", &"..")
            .field("sender", &self.notify_sender)
            .field("port_pool", &self.port_pool)
            .finish()
    }
}

impl GlobalState {
    pub fn new(
        scheduler: JobScheduler,
        notify_sender: NotifySender,
        agent_runtime: AgentIntegrationChannel,
    ) -> Self {
        Self {
            alive: Arc::new(AtomicBool::new(true)),
            scheduler,
            notify_sender,
            port_pool: PortPool::default(),
            agent_runtime: AgentRuntimeRef::new(agent_runtime),
        }
    }

    pub fn send_task_activity(&self, activity: Activity) {
        if let Err(err) = self
            .notify_sender
            .upgrade()
            .map(|sender| sender.send(SchedulerNotify::TaskActivity(activity)))
            .transpose()
        {
            error!("send task activity error: {:#}", err);
        }
    }
    pub fn send_agent_activity(&self, activity: Activity) {
        if let Err(err) = self
            .notify_sender
            .upgrade()
            .map(|sender| sender.send(SchedulerNotify::AgentActivity(activity)))
            .transpose()
        {
            error!("send agent activity error: {:#}", err);
        }
    }
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn ensure_alive(&self) -> anyhow::Result<()> {
        if !self.is_alive() {
            bail!("Scheduler is not alive");
        }
        Ok(())
    }

    pub async fn go_die(&self) -> anyhow::Result<()> {
        self.alive.store(false, Ordering::Relaxed);
        self.scheduler.clone().shutdown().await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AgentWaiter {
    /// Agent state if task is running on agent.
    agent_state: Arc<RwLock<AgentState>>,
    /// Agent task activities receiver.
    agent_activities: Arc<RwLock<tokio::sync::mpsc::Receiver<Activity>>>,
    /// Agent close waiter.
    agent_close_waiter: Arc<Mutex<Option<oneshot::Receiver<anyhow::Result<()>>>>>,
}

/// Inner state or one-shot state of task under job scheduler.
///
/// 1. Initial state is `Queued`.
/// 2. When task is spawned, the state will be `Running`.
/// 3. When task is stopped, the state will be `Stopped`.
/// 4. When task is completed, the state will be `Completed`.
/// 5. When task is failed, the state will be `Failed`.
#[derive(Debug, Clone, Default)]
pub enum InnerState {
    #[default]
    /// Task is scheduled
    Queued,
    /// Task is running.
    Running,
    /// Task is stopping.
    Stopping,
    /// Task is stopped.
    Stopped,
    /// Task is completed.
    Completed,
    /// Task is interrupted.
    Interrupted,
    /// Cronjob tick is done.
    Ticked,
    /// Task is failed.
    Failed(String),
}

impl InnerState {
    pub fn is_running(&self) -> bool {
        matches!(self, InnerState::Running)
    }
    pub fn is_queued(&self) -> bool {
        matches!(self, InnerState::Queued)
    }
    pub fn is_idle(&self) -> bool {
        matches!(
            self,
            InnerState::Queued
                | InnerState::Stopped
                | InnerState::Completed
                | InnerState::Interrupted
                | InnerState::Ticked
        )
    }

    pub fn in_final_state(&self) -> bool {
        matches!(
            self,
            InnerState::Completed
                | InnerState::Stopped
                | InnerState::Failed(_)
                | InnerState::Stopping
        )
    }

    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            InnerState::Completed | InnerState::Stopped | InnerState::Failed(_)
        )
    }

    pub fn safe_to_delete(&self) -> bool {
        matches!(
            self,
            InnerState::Completed
                | InnerState::Stopped
                | InnerState::Failed(_)
                | InnerState::Interrupted
                | InnerState::Ticked
        )
    }

    pub(crate) fn is_stopped(&self) -> bool {
        matches!(self, InnerState::Stopped)
    }
    pub(crate) fn ready_to_remove_job(&self) -> bool {
        matches!(
            self,
            InnerState::Completed
                | InnerState::Stopped
                | InnerState::Failed(_)
                | InnerState::Interrupted
                | InnerState::Ticked
        )
    }
    pub fn start(&mut self) -> anyhow::Result<&mut Self> {
        match self {
            InnerState::Running => Ok(self),
            InnerState::Stopping | InnerState::Stopped => bail!("Task is stopping"),

            _ => {
                *self = Self::Running;
                Ok(self)
            }
        }
    }
    pub fn stop(&mut self) -> &mut Self {
        match self {
            InnerState::Stopping => self,
            InnerState::Stopped => self,
            _ => {
                *self = Self::Stopping;
                self
            }
        }
    }
    pub fn stopped(&mut self) -> &mut Self {
        *self = Self::Stopped;
        self
    }
    pub fn completed(&mut self) -> &mut Self {
        *self = Self::Completed;
        self
    }
    pub fn ticked(&mut self) -> &mut Self {
        *self = Self::Ticked;
        self
    }
    pub fn interrupted(&mut self) -> &mut Self {
        *self = Self::Interrupted;
        self
    }

    pub fn fail(&mut self, message: impl Display) -> &mut Self {
        *self = Self::Failed(format!("{}", message));
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Operator {
    Run,
    Stop,
    Suspend,
}
#[derive(Clone)]
pub struct TaskOperator(Arc<AtomicU8>);

impl TaskOperator {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU8::new(0)))
    }
    pub fn as_str(&self) -> &'static str {
        match self.0.load(Ordering::SeqCst) {
            0 => "run",
            1 => "stop",
            2 => "suspend",
            _ => unreachable!(),
        }
    }
    pub fn stop(&self) {
        self.0.store(1, Ordering::SeqCst);
    }

    pub fn suspend(&self) {
        self.0.store(2, Ordering::SeqCst);
    }

    pub fn start(&self) {
        self.0.store(0, Ordering::SeqCst);
    }

    pub fn operator(&self) -> Operator {
        match self.0.load(Ordering::SeqCst) {
            0 => Operator::Run,
            1 => Operator::Stop,
            2 => Operator::Suspend,
            _ => unreachable!(),
        }
    }

    pub fn is_suspended(&self) -> bool {
        self.0.load(Ordering::SeqCst) == 2
    }
    pub fn is_stopped(&self) -> bool {
        self.0.load(Ordering::SeqCst) == 1
    }
    pub fn is_running(&self) -> bool {
        self.0.load(Ordering::SeqCst) == 0
    }
}

impl Debug for TaskOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.operator(), f)
    }
}
impl Display for TaskOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[derive(Debug, Clone)]
pub struct TaskState {
    span: tracing::Span,
    /// Current job run times.
    runs: Arc<AtomicU64>,
    /// Task details.
    pub(crate) task: Arc<Task>,

    pub(crate) operator: TaskOperator,

    pub(crate) state: Arc<RwLock<InnerState>>,

    /// Job schedule.
    schedule: Arc<Schedule>,
    /// Stop condition of current job.
    stop_condition: StopCondition,
    /// Stop a running task by sending a cancellation signal.
    pub(crate) cancellation: CancellationToken,

    /// Agent state if task is running on agent.
    agent_waiter: Option<AgentWaiter>,

    /// Last state
    ///
    /// When task finished unexpectedly, the last state will be None.
    ///
    /// When task finished successfully, the last state will be one of
    /// `Done`, `Stopped` or `Error`.
    last_state: Arc<RwLock<Option<LastState>>>,

    /// Job listener.
    last_waiter: Arc<Mutex<Option<oneshot::Receiver<bool>>>>,
}

impl TaskState {
    pub async fn new(task: Task, global: &GlobalState) -> Self {
        let mut local_strategy = Strategy::const_new();
        let strategy = if let Some(v) = task.trigger.as_ref() {
            v
        } else {
            let dsn = json_to_dsn(&serde_json::Value::String(task.from.clone())).unwrap();
            if dsn.driver.starts_with("csv") {
                let new_file_notify = dsn
                    .get("new_file_notify")
                    .and_then(|v| {
                        if v.trim().is_empty() {
                            Some(false)
                        } else {
                            v.parse().ok()
                        }
                    })
                    .unwrap_or(false);
                if !new_file_notify {
                    local_strategy = local_strategy.never_resume();
                }
            }
            &local_strategy
        };
        let schedule = strategy.schedule();
        let task_id = task.id;

        let stop_condition = strategy.stop_condition();
        let cancellation = CancellationToken::new();

        let agent_waiter = if let Some(via) = task.via {
            let agent_state = Arc::new(RwLock::new(AgentState::default()));
            let (sender, agent_activities) = tokio::sync::mpsc::channel(100);
            let (stop_sender, stop_waiter) = tokio::sync::oneshot::channel();
            let task = AgentTask {
                agent_id: via,
                task_id,
                agent_state: agent_state.clone(),
                sender,
                stop_sender: Arc::new(stop_sender),
            };
            global.agent_runtime.insert(task).await;
            Some(AgentWaiter {
                agent_state,
                agent_activities: Arc::new(RwLock::new(agent_activities)),
                agent_close_waiter: Arc::new(Mutex::new(Some(stop_waiter))),
            })
        } else {
            None
        };
        Self {
            span: tracing::info_span!("task_runner", task.id = task_id),
            runs: Arc::new(AtomicU64::new(0)),
            task: Arc::new(task),
            state: Arc::new(RwLock::new(InnerState::Queued)),
            schedule: Arc::new(schedule),
            stop_condition,
            cancellation,
            agent_waiter,
            operator: TaskOperator::new(),
            last_state: Arc::new(RwLock::new(None)),
            last_waiter: Arc::new(Mutex::new(None)),
        }
    }

    pub fn schedule(&self) -> &Schedule {
        &self.schedule
    }
}

#[derive(Debug)]
pub enum LastState {
    Done,
    Stopped,
    Error(anyhow::Error),
}

impl Display for LastState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LastState::Done => write!(f, "Done"),
            LastState::Stopped => write!(f, "Stopped"),
            LastState::Error(err) => write!(f, "Error: {:#}", err),
        }
    }
}

/// Task job runner with shared state and global state.
#[derive(MultiIndexMap, Debug, Clone)]
pub struct TaskJob {
    #[multi_index(hashed_unique)]
    pub task_id: i64,
    #[multi_index(hashed_unique)]
    pub job_id: Uuid,

    /// The task that is associated with this job and shared amount all ticks of this job.
    pub task: TaskState,

    /// Global shared state across all jobs/tasks.
    pub global: GlobalState,
}

impl Debug for MultiIndexTaskJobMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiIndexTaskJobMap")
            .field("capacity", &self.capacity())
            .field("len", &self.len())
            .field(
                "items",
                &self.iter().map(|(_, task)| task).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl MultiIndexTaskJobMap {
    pub async fn try_stop(&mut self, task: i64) -> Result<(), StopError> {
        let task_job = self
            .get_by_task_id(&task)
            .ok_or(StopError::NotFound(task))?;
        let job_id = task_job.job_id;
        tracing::info!(task.id = task, job.id = %job_id, "task `{task}` will be removed");

        if task_job.in_final_state().await {
            return Err(StopError::AlreadyStopped(task));
        }

        let state = task_job.stop().await;

        if state.ready_to_remove_job() {
            // If job has not been ticked, remove task state handler directly.
            self.remove_by_task_id(&task);
            tracing::info!(task.id = task, job.id = %job_id, "task `{task}` is stopped");
            Ok(())
        } else {
            tracing::info!(task.id = task, job.id = %job_id, "Try stop task in scheduler");
            Ok(())
        }
    }
}

pub type MultiIndexTaskJobMapRef = Arc<RwLock<MultiIndexTaskJobMap>>;

impl TaskJob {
    /// Create a new task job runner.
    pub fn new(job_id: Uuid, task: TaskState, global_state: GlobalState) -> Self {
        let task_id = task.task.id;
        Self {
            task_id,
            job_id,
            task,
            global: global_state,
        }
    }

    /// Check if a task is running.
    pub async fn is_running(&self) -> bool {
        self.task.state.read().await.is_running()
    }

    /// Check if a task is in final state.
    pub async fn in_final_state(&self) -> bool {
        self.task.state.read().await.in_final_state()
    }
    /// Check if a task is in final state.
    pub async fn is_finished(&self) -> bool {
        self.task.state.read().await.is_finished()
    }

    /// Check if a task is safe to delete.
    pub async fn safe_to_delete(&self) -> bool {
        self.task.state.read().await.safe_to_delete()
    }

    /// Stop a job manually.
    pub async fn stop(&self) -> InnerState {
        let id = self.task_id;
        tracing::info!(task.id = self.task_id, job.id = %self.job_id, "task `{id}` will be removed");

        self.task.operator.stop();
        // Send stopping state updating activity.
        self.global.send_task_activity(Activity::stop(id));

        // Set task state to stopping if already scheduled.
        {
            // cancel spawned task.
            let mut state = self.task.state.write().await;
            if state.is_idle() {
                // Set Task state to stopped directly.
                self.global.send_task_activity(Activity::stopped(id));
                state.stopped();
            } else {
                // Set task state to stopping so that it will be stopped when it's ticked properly.
                state.stop();
            }
        };

        // Remove job from scheduler.
        tracing::info!(task.id = self.task_id, job.id = %self.job_id, cause = "stopped");
        if let Err(err) = self.global.scheduler.remove(&self.job_id).await {
            error!("remove job error: {:#}", err);
        }
        // Send cancellation signal to running task.
        self.task.cancellation.cancel();

        // Remove agent task.
        if self.task.task.via.is_some() {
            self.global.agent_runtime.stop(self.task.task.id).await;
        }

        { self.task.state.read().await.clone() }
    }

    /// Suspend a job.
    pub(super) async fn suspend(&self) -> InnerState {
        let id = self.task_id;
        tracing::info!(task.id = self.task_id, job.id = %self.job_id, "task `{id}` will be suspended");

        self.task.operator.suspend();
        // Send stopping state updating activity.
        self.global
            .send_task_activity(Activity::suspend(id, self.job_id));

        // Set task state to suspending if already scheduled.
        {
            // cancel spawned task.
            let mut state = self.task.state.write().await;
            if state.is_idle() {
                // Job has not been ticked yet (one it's ticked, state should be running)

                // Send stopped state directly.
                self.global
                    .send_task_activity(Activity::suspended(id, self.job_id));

                // Set task state to stopping so that it will be stopped when it's ticked properly.
                state.stopped();
            } else {
                state.stop();
            }
        };

        // Remove job from scheduler.
        tracing::info!(task.id = self.task_id, job.id = %self.job_id, cause = "suspended");
        if let Err(err) = self.global.scheduler.remove(&self.job_id).await {
            error!("remove job error: {:#}", err);
        }
        // Send cancellation signal to running task.
        self.task.cancellation.cancel();

        // Remove agent task.
        if self.task.task.via.is_some() {
            self.global.agent_runtime.suspend(self.task.task.id).await;
        }
        { self.task.state.read().await.clone() }
    }

    /// ## Cancellation safety.
    ///
    /// If the task is cancelled, the task running future will be dropped.
    /// But there're still some staff doing in background,
    /// release sockets, close connections, push offsets,
    /// persist checkpoints/caches etc.
    ///
    /// It should not be a problem, and the task is ok to be resumed.
    ///
    /// (: We pretend that no remaining staff could prevent task to be resumed.
    pub async fn spawn(&self) {
        let opts = self.task.clone();
        let task_id = self.task_id;
        let jid = self.job_id;
        let global = self.global.clone();

        let (tx, rx) = tokio::sync::oneshot::channel();

        /// Returns false for tracker cancelled, true for license suspending.
        pub async fn license_tracker(
            license_tracker_cancellation_token: CancellationToken,
            license_tracker_state: TaskState,
            license_tracker_global: GlobalState,
            task_id: TaskId,
        ) -> bool {
            // Check license for each 8 hours.
            let license_tracker_interval_seconds =
                std::env::var("LICENSE_TRACKER_INTERVAL_SECONDS")
                    .unwrap_or_else(|_| "3600".to_string())
                    .parse::<u64>()
                    .unwrap_or(3600);
            let mut interval =
                tokio::time::interval(Duration::from_secs(license_tracker_interval_seconds));

            let (from, to) = (
                // license_tracker_state.task.from.parse().unwrap(),
                json_to_dsn(&serde_json::Value::String(
                    license_tracker_state.task.from.clone(),
                ))
                .unwrap(),
                license_tracker_state.task.to.parse().unwrap(),
            );
            let validator = LicenseValidator::new(&from, &to);
            'track: loop {
                tokio::select! {
                    _ = license_tracker_cancellation_token.cancelled() => {
                        tracing::info!("License tracker cancelled");
                        break false;
                    }
                    _ = interval.tick() => {
                        match validator.validate_connector().in_current_span().await {
                        // match anyhow::Ok(LicenseKind::Edition(anyhow::anyhow!("Community"))) {
                            Ok(kind) => {
                                if let Err(err) = kind.ok() {
                                    tracing::warn!(error = %err, "License error, wait for validation in next 5m");
                                    let mut err = err;
                                    let mut cross_validate_times = 0;
                                    loop {
                                        if license_tracker_cancellation_token.is_cancelled() {
                                            tracing::info!("License tracker cancelled");
                                            return false;
                                        }
                                        tokio::time::sleep(Duration::from_secs(60)).await;
                                        match validator.validate_connector().in_current_span().await.map(|kind| kind.ok()) {
                                            Ok(Ok(_)) => {
                                                tracing::info!("License validation passed, continue tracking");
                                                continue 'track;
                                            },
                                            Ok(Err(e)) => {
                                                let err_str = format!("{e:#}");
                                                if err_str.contains("edition: unknown") {
                                                    tracing::info!("License validation in unknown state, continue tracking");
                                                    continue 'track;
                                                }
                                                cross_validate_times += 1;
                                                err = e;
                                                if cross_validate_times >= 5 {
                                                    // Trust the error, then suspend.
                                                    tracing::info!("License validation confirmed finally suspend task");
                                                    break;
                                                }
                                                tracing::info!("License validation confirmed, continue next validate loop");
                                                continue;
                                            }
                                            Err(e) => {
                                                tracing::warn!(error = format!("{e:#}"), "License validation tracking error");
                                            }
                                        }
                                    }
                                    license_tracker_global.send_task_activity(Activity::suspending_with(task_id, format!("{err:#}")));
                                    license_tracker_state.operator.suspend();
                                    license_tracker_cancellation_token.cancel();
                                    break true;
                                } else {
                                    tracing::info!("License validation tracking ok");
                                }
                            }
                            Err(err) => {
                                tracing::warn!(error = format!("{err:#}"), "License validation tracking error");
                            }
                        }
                    }
                }
            }
        }

        if let Some(agent_id) = opts.task.via {
            // let task_name = self.task.task.name.clone();
            let future = async move {
                #[derive(Debug)]
                enum AgentTaskState {
                    Stopped,
                    Failed,
                    Ticked,
                    Completed,
                    Suspended,
                    Interrupted,
                }
                let run_id = opts.runs.fetch_add(1, Ordering::Release);
                let state = opts;
                let mut waiting = 0;
                let cancellation = state.cancellation.child_token();
                let drop_guard = cancellation.clone().drop_guard();
                tracing::debug!(
                    "spawned new run_task, task.id={} task.rid={}",
                    task_id,
                    run_id
                );
                let license_tracker_cancellation_token = cancellation.clone();
                let license_tracker_state = state.clone();
                let license_tracker_global = global.clone();
                tokio::spawn(
                    license_tracker(
                        license_tracker_cancellation_token,
                        license_tracker_state,
                        license_tracker_global,
                        task_id,
                    )
                    .in_current_span(),
                );
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        tracing::info!(agent.id = agent_id, task.id = task_id, job.id = %jid, "task `{task_id}` cancelled");
                        let operator = state.operator.operator();
                        match operator {
                            Operator::Suspend => {
                                global.send_task_activity(Activity::suspended(task_id, jid));
                                state.state.write().await.stopped();
                            }
                            Operator::Stop => {
                                global.send_task_activity(Activity::stopped(task_id));
                                state.state.write().await.stopped();
                            }
                            Operator::Run => {
                                unreachable!("Cancellation should be only trigger by stop or suspend operator")
                            }
                        }
                        let _ = tx.send(true);
                        tracing::warn!(agent.id = agent_id, task.id = task_id, job.id = %jid,"Task {task_id} cancelled");
                        return
                    }
                    _ = async {
                        loop {
                            if global.agent_runtime.agent_is_alive(agent_id).await {
                                break;
                            }

                            warn!("Agent {} is not alive, waiting...", agent_id);
                            global
                                .send_task_activity(Activity::waiting(task_id, "Waiting for agent..."));
                            if waiting < 5 {
                                waiting += 1;
                            }
                            tokio::time::sleep(Duration::from_secs(1) * waiting).await;
                        }
                    } => {}
                }

                global.send_task_activity(Activity::running(
                    task_id,
                    format!("Agent {agent_id} now alive"),
                ));
                global.send_agent_activity(Activity::agent_transferring(
                    agent_id,
                    format!("Task {task_id} now running"),
                ));
                tracing::debug!("Agent {} is alive, sending command run", agent_id);
                let _ = global
                    .agent_runtime
                    .push_action(agent_id, AgentAction::Run(task_id, jid, run_id))
                    .await;
                tracing::debug!("Command run sending ok");
                let waiter = state.agent_waiter.as_ref().unwrap();

                let agent_activities = waiter.agent_activities.clone();
                let is_cron_job = state.schedule().is_repeatable_job();

                #[instrument(skip_all, fields(task.id = task_id, task.jid = %jid, task.rid = run_id, task.agent = agent_id,))]
                async fn agent_activities_listener(
                    operator: TaskOperator,
                    is_cron_job: bool,
                    global: &GlobalState,
                    state: &TaskState,
                    task_id: TaskId,
                    agent_id: AgentId,
                    jid: Uuid,
                    run_id: u64,
                    ipc_in_progress: Arc<AtomicI32>,
                    agent_activities: Arc<RwLock<tokio::sync::mpsc::Receiver<Activity>>>,
                ) -> anyhow::Result<AgentTaskState> {
                    let mut ipc_in_progress = ipc_in_progress.load(Ordering::SeqCst);
                    let mut signal: Option<&'static str> = None;
                    tracing::info!(
                        ipc_in_progress,
                        "Listening agent activities to stop the task"
                    );
                    loop {
                        let mut recv = agent_activities.write().await;
                        match tokio::select! {
                            _ = state.cancellation.cancelled() => {
                                tracing::info!(%ipc_in_progress, agent.id = agent_id, task.id = task_id, job.id = %jid, "task runner `{task_id}` cancelled");
                                match operator.operator() {
                                    Operator::Suspend => {
                                        global.send_task_activity(Activity::suspended(task_id, jid));
                                        state.state.write().await.stopped();
                                    }
                                    Operator::Stop => {
                                        if ipc_in_progress > 0 {
                                            tracing::info!("Ingesting data with worker {} is in progress, waiting...", ipc_in_progress);
                                            global.send_task_activity(Activity::running(
                                                task_id,
                                                format!(
                                                    "Ingesting data with worker {} is in progress, waiting...",
                                                    ipc_in_progress
                                                ),
                                            ));
                                            tokio::time::sleep(Duration::from_millis(500)).await;
                                            continue;
                                        } else {
                                            tracing::info!(signal, "task will be stopped after ingesting data completed");
                                        }
                                        global.send_task_activity(Activity::stopped(task_id));
                                        state.state.write().await.stopped();
                                    }
                                    Operator::Run => {
                                        tracing::warn!("operator is run, expect stop or suspend");
                                        global.send_task_activity(Activity::stopped(task_id));
                                        state.state.write().await.stopped();
                                        //unreachable!("Cancellation should be only trigger by stop or suspend operator")
                                    }
                                }
                                tracing::warn!(ipc_in_progress, agent.id = agent_id, task.id = task_id, job.id = %jid, "Task {task_id} cancelled");
                                break Ok(AgentTaskState::Stopped);
                            },
                            item = recv.recv() => item,
                        } {
                            Some(mut activity) => {
                                tracing::warn!(
                                    activity = activity.activity,
                                    status = activity.status
                                );
                                match activity.status.as_str() {
                                    "interrupt" => {
                                        tracing::info!("task interrupted: {}", activity.activity);
                                        global
                                            .agent_runtime
                                            .push_action(agent_id, AgentAction::Interrupt(task_id))
                                            .await?;
                                        activity.status = "interrupted".to_string();
                                        global.send_task_activity(activity);
                                        state.state.write().await.interrupted();
                                        // wait for agent task cancelled timeout.
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                        break Ok(AgentTaskState::Interrupted);
                                    }
                                    "interrupted" => {
                                        tracing::info!("task interrupted: {}", activity.activity);
                                        activity.status = "interrupted".to_string();
                                        global.send_task_activity(activity);
                                        state.state.write().await.interrupted();
                                        break Ok(AgentTaskState::Interrupted);
                                    }
                                    "started" => {
                                        tracing::info!("task started");
                                        global.send_task_activity(activity);
                                    }
                                    "resumed" => {
                                        tracing::info!("agent resumed");
                                        global.send_task_activity(activity.clone());
                                        // Send run command again.
                                        global
                                            .agent_runtime
                                            .push_action(
                                                agent_id,
                                                AgentAction::Run(task_id, jid, run_id),
                                            )
                                            .await?;
                                        activity.status = "running".to_string();
                                        global.send_task_activity(activity);
                                    }
                                    "suspended" => match operator.operator() {
                                        Operator::Suspend => {
                                            tracing::info!("task suspended");
                                            global.send_task_activity(activity);
                                            state.state.write().await.stopped();
                                            break Ok(AgentTaskState::Suspended);
                                        }
                                        _ => {
                                            warn!(
                                                "Received `suspended` status but not in suspending, skip"
                                            );
                                        }
                                    },
                                    "ipc-started" => {
                                        ipc_in_progress += 1;
                                        tracing::info!(
                                            "Start ingesting data with worker {}",
                                            ipc_in_progress
                                        );
                                        global.send_task_activity(Activity::running(
                                            task_id,
                                            format!(
                                                "Start ingesting data with worker {}",
                                                ipc_in_progress
                                            ),
                                        ));
                                    }
                                    "ipc-finished" => {
                                        tracing::info!(
                                            "Ingesting worker {} is completed",
                                            ipc_in_progress
                                        );

                                        global.send_task_activity(Activity::logging(
                                            task_id,
                                            format!(
                                                "Ingesting data with worker {} completed",
                                                ipc_in_progress
                                            ),
                                        ));
                                        if ipc_in_progress >= 1 {
                                            ipc_in_progress -= 1;
                                        }
                                        if ipc_in_progress > 0 {
                                            continue;
                                        }
                                        drop(activity); // drop activity explicitly to not use it anymore
                                        if let Some(status) = signal {
                                            match status {
                                                "completed" => match operator.operator() {
                                                    Operator::Suspend => {
                                                        global.send_task_activity(
                                                            Activity::suspended(task_id, jid),
                                                        );
                                                        state.state.write().await.stopped();
                                                        break Ok(AgentTaskState::Suspended);
                                                    }
                                                    Operator::Stop => {
                                                        tracing::info!("task stopped");
                                                        global.send_task_activity(
                                                            Activity::stopped(task_id),
                                                        );
                                                        state.state.write().await.stopped();
                                                        break Ok(AgentTaskState::Stopped);
                                                    }
                                                    Operator::Run => {
                                                        tracing::info!("task completed");
                                                        if is_cron_job {
                                                            global.send_task_activity(
                                                                Activity::tick(task_id, jid),
                                                            );
                                                            state.state.write().await.ticked();
                                                            break Ok(AgentTaskState::Ticked);
                                                        }
                                                        global.send_task_activity(
                                                            Activity::completed(task_id, jid),
                                                        );
                                                        state.state.write().await.completed();
                                                        break Ok(AgentTaskState::Completed);
                                                    }
                                                },
                                                "stopped" => {
                                                    tracing::info!("task stopped");
                                                    global.send_task_activity(Activity::stopped(
                                                        task_id,
                                                    ));
                                                    state.state.write().await.stopped();
                                                    break Ok(AgentTaskState::Stopped);
                                                }
                                                _ => unreachable!("Invalid signal: {}", status),
                                            }
                                        }
                                    }
                                    "completed" => {
                                        if ipc_in_progress <= 0 {
                                            tracing::info!("task completed");
                                            if is_cron_job {
                                                activity.status = "ticked".to_string();
                                                global.send_task_activity(activity);
                                                state.state.write().await.ticked();
                                                break Ok(AgentTaskState::Ticked);
                                            }
                                            global.send_task_activity(activity);
                                            state.state.write().await.completed();
                                            break Ok(AgentTaskState::Completed);
                                        } else {
                                            tracing::info!(
                                                "Task completed but still have {} workers ingesting data",
                                                ipc_in_progress
                                            );
                                            signal = Some("completed");
                                            continue;
                                        }
                                    }
                                    "stopped" => match operator.operator() {
                                        Operator::Stop => {
                                            if ipc_in_progress == 0 {
                                                tracing::info!("task stopped");
                                                global.send_task_activity(activity);
                                                state.state.write().await.stopped();
                                                break Ok(AgentTaskState::Stopped);
                                            } else {
                                                signal = Some("stopped");

                                                tracing::info!(
                                                    ipc_in_progress,
                                                    "Task stopped but still have {} workers ingesting data",
                                                    ipc_in_progress
                                                );
                                                continue;
                                            }
                                        }
                                        _ => {
                                            warn!(
                                                "Received `stopped` status but not in stopping, skip"
                                            );
                                        }
                                    },
                                    "failed" => {
                                        tracing::error!(
                                            is_cron_job,
                                            "task failed: {}",
                                            activity.activity
                                        );
                                        if is_cron_job {
                                            activity.status = "interrupted".to_string();
                                            global.send_task_activity(activity);
                                            state.state.write().await.interrupted();
                                            break Ok(AgentTaskState::Interrupted);
                                        }
                                        let result = Err(anyhow::anyhow!("{}", activity.activity));
                                        let should_stop =
                                            state.stop_condition.should_stop_with(&result);
                                        if should_stop {
                                            tracing::warn!(
                                                should_stop,
                                                "task failed: {}",
                                                activity.activity
                                            );
                                            global.send_task_activity(activity.clone());
                                            state.state.write().await.fail(activity.activity);
                                            break Ok(AgentTaskState::Failed);
                                        } else {
                                            activity.status = "interrupted".to_string();
                                            tracing::warn!(
                                                should_stop,
                                                "task interrupted: {}",
                                                activity.activity
                                            );
                                            global.send_task_activity(activity);
                                            state.state.write().await.interrupted();
                                            break Ok(AgentTaskState::Interrupted);
                                        }
                                    }
                                    status => {
                                        tracing::info!(status, message = activity.activity);
                                        global.send_task_activity(activity);
                                    }
                                }
                            }
                            None => {
                                break Err(anyhow::anyhow!("All agent activities sender dropped"));
                            }
                        }
                    }
                }

                let mut ipc_in_progress = Arc::new(AtomicI32::new(0));
                let mut listener = agent_activities_listener(
                    state.operator.clone(),
                    is_cron_job,
                    &global,
                    &state,
                    task_id,
                    agent_id,
                    jid,
                    run_id,
                    ipc_in_progress.clone(),
                    agent_activities.clone(),
                );
                tokio::pin!(listener);

                let res = tokio::select! {
                    _ = cancellation.cancelled() => {
                        tracing::info!("Task {task_id} cancelled, wait 1h for remain data ingestion");
                        match tokio::time::timeout(
                            Duration::from_secs(60 * 60), // 1 hour
                            listener).await {

                        Ok(res) => res,
                        Err(_) => {
                            let operator = state.operator.operator();
                            match operator {
                                Operator::Suspend => {
                                    global.send_task_activity(Activity::suspending_timeout(
                                        task_id, jid,
                                    ));
                                    state.state.write().await.stopped();
                                }
                                Operator::Stop => {
                                    global.send_task_activity(Activity::stopping_timeout(
                                        task_id,
                                    ));
                                    state.state.write().await.stopped();
                                }
                                Operator::Run => {
                                    unreachable!("Cancellation should be only trigger by stop or suspend operator")
                                }
                            }
                            Err(anyhow::anyhow!(
                                "Stopping task {} at agent {} timed out",
                                task_id,
                                agent_id
                            ))
                        }
                            }
                    },
                    res = &mut listener => {
                        res
                    },
                };

                tracing::info!("Task {task_id} agent task finished: {:#?}", res);
                drop(drop_guard);
                match res {
                    Ok(AgentTaskState::Stopped)
                    | Ok(AgentTaskState::Failed)
                    | Ok(AgentTaskState::Completed)
                    | Ok(AgentTaskState::Suspended) => {
                        let _ = tx.send(true);
                    }
                    Ok(_) => {
                        let _ = tx.send(false);
                    }
                    Err(err) => {
                        let _ = tx.send(false);
                        tracing::warn!("agent activities listener error: {:#}", err);
                    }
                }
            };
            tokio::spawn(future.in_current_span());
        } else {
            tokio::spawn(
                async move {
                    global.send_task_activity(Activity::started(opts.task.id, jid));
                    let runs = opts.runs.load(Ordering::Relaxed);
                    tracing::debug!(
                        "spawned new run_task, task.id={} task.rid={}",
                        task_id,
                        runs
                    );
                    let span = tracing::info_span!("run_task", task.rid = runs);

                    let cancellation = opts.cancellation.child_token();

                    // let license_tracker_cancellation_token = opts.cancellation.clone();
                    let license_tracker_cancellation_token = cancellation.clone();

                    let drop_guard = license_tracker_cancellation_token.clone().drop_guard();
                    let license_tracker_state = opts.clone();
                    let license_tracker_global = global.clone();
                    let license_tracker_task = tokio::spawn(
                        license_tracker(
                            license_tracker_cancellation_token,
                            license_tracker_state,
                            license_tracker_global,
                            task_id,
                        )
                        .instrument(span.clone()),
                    );
                    let future = run_task(&global, &opts, &jid, cancellation.clone())
                        .instrument(span.clone());
                    tokio::pin!(future);

                    let stop_condition = opts.stop_condition.clone();
                    let last_state = opts.last_state.clone();
                    let span_handler = span.clone();
                    let handler = |result| async {
                        let _ = span_handler.enter();

                        if let Err(err) = &result {
                            error!(error = %err, backtrace = ?err, "task error");
                        } else {
                            info!("task finished");
                        }
                        let should_stop = stop_condition.should_stop_with(&result);
                        match result {
                            Ok(_) => {
                                last_state.write().await.replace(LastState::Done);
                            }
                            Err(err) => {
                                last_state.write().await.replace(LastState::Error(err));
                            }
                        }
                        if should_stop {
                            tracing::info!(should_stop, ?stop_condition, ?opts, "stop condition reached");
                        }
                        should_stop
                    };

                    let stop_or_suspend_handler = async {
                        let _ = span.enter();
                        let operator = opts.operator.operator();
                        opts.last_state.write().await.replace(LastState::Stopped);
                        if opts.cancellation.is_cancelled() {
                            // Caused by upstream, suspend or stop.
                            tracing::info!("task cancelled");
                            opts.last_state.write().await.replace(LastState::Stopped);
                            true
                        } else {
                            // Caused by current task.
                            false
                        }
                    };

                    let _ = span.enter();
                    let mut should_stop = tokio::select! {
                        biased;
                        result = &mut future => {
                            if opts.cancellation.is_cancelled() {
                                tracing::info!("task cancelled");
                                opts.last_state.write().await.replace(LastState::Stopped);
                                true
                            } else {
                                handler(result).await
                            }
                        }
                        _ = opts.cancellation.cancelled() => {
                            tracing::info!("task cancelled");
                            opts.last_state.write().await.replace(LastState::Stopped);
                            (&mut future).await;
                            true
                        }
                    };

                    if !should_stop {
                        should_stop = opts.stop_condition.should_stop();
                    }
                    let state_guard = opts.last_state.read().await;
                    let last_state = state_guard.as_ref().expect("task should have a last state");
                    match last_state {
                        LastState::Done => match opts.operator.operator() {
                            Operator::Suspend => {
                                global.send_task_activity(Activity::suspended(opts.task.id, jid));
                                opts.state.write().await.stopped();
                            }
                            Operator::Stop => {
                                global.send_task_activity(Activity::stopped(opts.task.id));
                                opts.state.write().await.stopped();
                            }
                            Operator::Run => {
                                if should_stop {
                                    global
                                        .send_task_activity(Activity::completed(opts.task.id, jid));
                                    opts.state.write().await.completed();
                                } else {
                                    global.send_task_activity(Activity::tick(opts.task.id, jid));
                                    opts.state.write().await.ticked();
                                }
                            }
                        },
                        LastState::Stopped => match opts.operator.operator() {
                            Operator::Suspend => {
                                global.send_task_activity(Activity::suspended(opts.task.id, jid));
                                opts.state.write().await.stopped();
                            }
                            _ => {
                                global.send_task_activity(Activity::stopped(opts.task.id));
                                opts.state.write().await.stopped();
                            }
                        },
                        LastState::Error(err) => {
                            if should_stop {
                                global.send_task_activity(Activity::failed(
                                    opts.task.id,
                                    format!("{err:#}"),
                                ));
                                opts.state.write().await.fail(err);
                            } else {
                                tracing::info!(?opts.schedule, ?opts.stop_condition, "task interrupted: {:#}", err);
                                global.send_task_activity(Activity::interrupted(
                                    opts.task.id,
                                    format!("{err:#}"),
                                ));
                                opts.state.write().await.interrupted();
                            }
                        }
                    }
                    opts.runs.fetch_add(1, Ordering::Release);
                    let _ = tx.send(should_stop);
                    drop(drop_guard);
                }
                .in_current_span(),
            );
        }
        self.task.last_waiter.lock().await.replace(rx);
    }

    pub async fn wait(&self) -> Option<LastState> {
        // wait for spawned task finished.
        let mut waiter = { self.task.last_waiter.lock().await.take() };
        let instant = std::time::Instant::now();

        match waiter.unwrap().await {
            Ok(should_stop) => {
                tracing::info!(should_stop, elapsed = ?instant.elapsed(), "Spawned task is finished");
                if should_stop {
                    tracing::info!(should_stop, task.id = self.task.task.id, job.id = %self.job_id, cause = "strategy stop");
                    if let Err(err) = self.global.scheduler.remove(&self.job_id).await {
                        error!("remove job error: {:#}", err);
                    }
                }
            }
            Err(err) => {
                error!("waiter error: {:#}", err);
            }
        }
        // Use last state to send task activities.
        self.task.last_state.write().await.take()
    }
}

#[instrument(skip_all, fields(task.id = task.task.id))]
pub async fn task_job_run(jid: Uuid, task: TaskState, global_state: Arc<GlobalState>) {
    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    qid.set_task_id(task.task.id as u16);
    if task.operator.is_suspended() || task.operator.is_stopped() {
        tracing::info!("task suspended");
        return;
    }
    if task.stop_condition.should_stop() {
        tracing::error!(job.id = %jid, "stop condition reached");
        if let Err(err) = global_state.scheduler.remove(&jid).await {
            error!("remove job error: {:#}", err);
        }
        return;
    }
    {
        if let Err(err) = task.state.write().await.start() {
            error!("task start error: {:#}", err);
            return;
        }
    }
    task.operator.start();
    task.stop_condition.tick();
    let opts = TaskJob::new(jid, task.clone(), global_state.as_ref().clone());

    // let from_dsn: Dsn = task.task.from.parse().unwrap();
    let from_dsn = json_to_dsn(&serde_json::Value::String(task.task.from.clone())).unwrap();
    let to_dsn = task.task.to.parse().unwrap();
    let task_id = task.task.id;
    let task_name = task.task.name.clone();
    let metrics = init_task_metrics(&from_dsn, &to_dsn, task_id, task_name)
        .in_current_span()
        .await;
    let (_sender, stop_save_metrics_signal) = oneshot::channel::<()>();
    if metrics.is_some() {
        auto_save_task_metrics(task_id, stop_save_metrics_signal)
            .in_current_span()
            .await;
    }
    opts.spawn().in_current_span().await;

    let completed = opts.wait().in_current_span().await;
    match completed {
        Some(LastState::Done) => {
            tracing::info!("task completed");
            // let _ = task.state.write().await.completed();
        }
        Some(LastState::Stopped) => {
            tracing::info!("task stopped");
            // task.state.write().await.stopped();
        }
        Some(LastState::Error(err)) => {
            tracing::info!("task error: {:#}", err);
        }
        None => {
            tracing::info!("task finished without state(usually means the job runs on an agent)");
        }
    }
    if metrics.is_some() {
        save_task_metrics_finally(task.task.id)
            .in_current_span()
            .await;
    }
}
