use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::bail;
use dashmap::DashMap;
use metrics::atomics::AtomicU64;
use multi_index_map::MultiIndexMap;
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use taosx_core::dsv::DataSourceValidation;
use taosx_core::{get_data_dir, utils::port_pool::PortPool, ConnectorLicense, DataSet, TaskOpts};
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio_cron_scheduler::JobScheduler;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn, Instrument};
use uuid::Uuid;

use crate::serve::controller::{
    agent::Activity,
    trigger::{Schedule, StopCondition, Strategy},
    AgentAction, Status, Task, TaskActivity,
};

use super::{
    agent::{AgentState, AgentTask, AgentWorker},
    NotifySender, SchedulerNotify,
};

#[instrument(skip_all)]
#[async_backtrace::framed]
async fn task_opts_init(task: &Task) -> anyhow::Result<TaskOpts> {
    let id = task.id;
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

    match from.driver.as_str() {
        "opcua" | "opcda" | "pi" => {
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
                "pi" => "pi",
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
        }
        _ => {}
    }

    // todo! add trace id to
    let span = tracing::info_span!(
        "task::spawned",
        task.id = id,
        trace_id = tracing::field::Empty
    );

    let breakpoints = task.breakpoints.clone();

    Ok(TaskOpts {
        transform: vec![],
        from: from.clone(),
        to: to_dsn.clone(),
        parser: task
            .parser
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap()),
        jobs: 0,
        compression_level: None,
        force: true,
        cancel: CancellationToken::new(),
        // port_pool: ONCE,
        with_agent: None,
        breakpoints,
        offsets,
        transferred: None,
        span: span.clone(),
        task_id: Some(id.to_string()),
    })
}

async fn run_task(global: &GlobalState, task: &TaskState, job_id: &Uuid) -> anyhow::Result<()> {
    debug_assert!(task.task.via.is_none());
    let _ = task.span.clone().entered();
    let state = task;
    let task = &state.task;
    let task_id = task.id;

    let opts = task_opts_init(task).await?;
    tracing::info!("start worker");
    // set current dir for upload files
    let path = get_data_dir();
    let _ = std::env::set_current_dir(&path);
    let instant = std::time::Instant::now();
    let res = opts.run(&global.port_pool).in_current_span().await;
    tracing::Span::current().record("task.elapsed", tracing::field::debug(instant.elapsed()));
    if let Err(error) = res {
        error!(task.elapsed = ?instant.elapsed(), error.message = %error, error.backtrace = ?error);
        Err(error)
    } else {
        tracing::info!(task.elapsed = ?instant.elapsed(), "task finished");
        Ok(())
    }
}

// pub type JobLock = Arc<Mutex<u32>>;

// pub type TaskErrorSender = tokio::sync::mpsc::Sender<anyhow::Error>;
// pub type TaskErrorReceiver = tokio::sync::mpsc::Receiver<anyhow::Error>;

pub type TaskId = i64;
pub type AgentId = i64;
pub type AgentTaskActivitiesReceiver = tokio::sync::broadcast::Receiver<TaskActivity>;
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

    pub async fn check(&self, agent_id: i64, req: String) -> anyhow::Result<DataSourceValidation> {
        match self {
            Self::Server(rt) => rt.check(agent_id, req).await,
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
    async fn cancel(&self, task_id: TaskId) {
        match self {
            Self::Server(rt) => {
                rt.cancel(task_id).await;
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

    pub fn send_task_activity(&self, activity: TaskActivity) {
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
    agent_activities: Arc<RwLock<tokio::sync::mpsc::Receiver<TaskActivity>>>,
    /// Agent close waiter.
    agent_close_waiter: Arc<Mutex<Option<oneshot::Receiver<anyhow::Result<()>>>>>,
}

/// Inner state of task under job scheduler.
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
                | InnerState::Stopping
                | InnerState::Interrupted
                | InnerState::Ticked
        )
    }

    pub(crate) fn is_stopped(&self) -> bool {
        matches!(self, InnerState::Stopped)
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
        match self.0.load(Ordering::Relaxed) {
            0 => "run",
            1 => "stop",
            2 => "suspend",
            _ => unreachable!(),
        }
    }
    pub fn stop(&self) {
        self.0.store(1, Ordering::Relaxed);
    }

    pub fn suspend(&self) {
        self.0.store(2, Ordering::Relaxed);
    }

    pub fn operator(&self) -> Operator {
        match self.0.load(Ordering::Relaxed) {
            0 => Operator::Run,
            1 => Operator::Stop,
            2 => Operator::Suspend,
            _ => unreachable!(),
        }
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
    cancellation: CancellationToken,

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
        let strategy = task.trigger.as_ref().unwrap_or(Strategy::DEFAULT);
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
                task_id: task_id,
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

pub type MultiIndexTaskJobMapRef = Arc<RwLock<MultiIndexTaskJobMap>>;

impl TaskJob {
    /// Create a new task job runner.
    pub fn new(job_id: Uuid, task: TaskState, global_state: GlobalState) -> Self {
        let task_id = task.task.id;
        Self {
            task_id,
            job_id: job_id,
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
        self.global.send_task_activity(TaskActivity::stop(id));

        // Set task state to stopping if already scheduled.
        {
            // cancel spawned task.
            let mut state = self.task.state.write().await;
            if state.is_queued() {
                // Job has not been ticked yet (one it's ticked, state should be running)

                // Send stopped state directly.
                self.global.send_task_activity(TaskActivity::stopped(id));

                // Set task state to stopping so that it will be stopped when it's ticked properly.
                state.stopped();
            } else {
                state.stop();
            }
        };

        // Remove job from scheduler.
        if let Err(err) = self.global.scheduler.remove(&self.job_id).await {
            error!("remove job error: {:#}", err);
        }
        // Send cancellation signal to running task.
        self.task.cancellation.cancel();

        // Remove agent task.
        if self.task.task.via.is_some() {
            self.global.agent_runtime.stop(self.task.task.id).await;
        }

        {
            self.task.state.read().await.clone()
        }
    }

    /// Suspend a job.
    pub(super) async fn suspend(&self) -> InnerState {
        let id = self.task_id;
        tracing::info!(task.id = self.task_id, job.id = %self.job_id, "task `{id}` will be suspended");

        self.task.operator.suspend();
        // Send stopping state updating activity.
        self.global
            .send_task_activity(TaskActivity::suspend(id, self.job_id));

        // Set task state to suspending if already scheduled.
        {
            // cancel spawned task.
            let mut state = self.task.state.write().await;
            if state.is_queued() {
                // Job has not been ticked yet (one it's ticked, state should be running)

                // Send stopped state directly.
                self.global
                    .send_task_activity(TaskActivity::suspended(id, self.job_id));

                // Set task state to stopping so that it will be stopped when it's ticked properly.
                state.stopped();
            } else {
                state.stop();
            }
        };

        // Remove job from scheduler.
        if let Err(err) = self.global.scheduler.remove(&self.job_id).await {
            error!("remove job error: {:#}", err);
        }
        // Send cancellation signal to running task.
        self.task.cancellation.cancel();

        // Remove agent task.
        if self.task.task.via.is_some() {
            self.global.agent_runtime.cancel(self.task.task.id).await;
        }
        {
            self.task.state.read().await.clone()
        }
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

        if let Some(agent_id) = opts.task.via {
            tokio::spawn(async move {
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
                let cancellation = state.cancellation.clone();

                tokio::select! {
                    _ = cancellation.cancelled() => {
                        let operator = state.operator.operator();
                        match operator {
                            Operator::Suspend => {
                                global.send_task_activity(TaskActivity::suspended(task_id, jid));
                                state.state.write().await.stopped();
                            }
                            Operator::Stop => {
                                global.send_task_activity(TaskActivity::stopped(task_id));
                                state.state.write().await.stopped();
                            }
                            Operator::Run => {
                                unreachable!("Cancellation should be only trigger by stop or suspend operator")
                            }
                        }
                        tx.send(state.stop_condition.should_stop());
                        return
                    }
                    _ = async {
                        loop {
                            if global.agent_runtime.agent_is_alive(agent_id).await {
                                break;
                            }

                            warn!("Agent {} is not alive, waiting...", agent_id);
                            global
                                .send_task_activity(TaskActivity::waiting(task_id, "Waiting for agent..."));
                            if waiting < 10 {
                                waiting += 1;
                            }
                            tokio::time::sleep(Duration::from_secs(1) * waiting).await;
                        }
                    } => {}
                }

                global.send_task_activity(TaskActivity::running(
                    task_id,
                    format!("Agent {agent_id} now alive"),
                ));
                tracing::debug!("Agent {} is alive, sending command run", agent_id);
                let _ = global
                    .agent_runtime
                    .push_action(agent_id, AgentAction::Run(task_id, jid, run_id))
                    .await;
                tracing::debug!("Command run sending ok");
                let waiter = state.agent_waiter.as_ref().unwrap();

                let agent_activities = waiter.agent_activities.clone();
                let is_cron_job = state.schedule().is_cron_job();

                async fn agent_activities_listener(
                    operator: Operator,
                    is_cron_job: bool,
                    global: &GlobalState,
                    state: &TaskState,
                    task_id: TaskId,
                    agent_id: AgentId,
                    jid: Uuid,
                    run_id: u64,
                    agent_activities: Arc<RwLock<tokio::sync::mpsc::Receiver<TaskActivity>>>,
                ) -> anyhow::Result<AgentTaskState> {
                    loop {
                        let mut recv = agent_activities.write().await;
                        match recv.recv().await {
                            Some(mut activity) => {
                                match activity.status.as_str() {
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
                                    "suspended" => {
                                        tracing::info!("task suspended");
                                        global.send_task_activity(activity);
                                        break Ok(AgentTaskState::Suspended);
                                    }
                                    "completed" => {
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
                                    }
                                    "stopped" => {
                                        tracing::info!("task stopped");
                                        global.send_task_activity(activity);
                                        state.state.write().await.stopped();
                                        break Ok(AgentTaskState::Stopped);
                                    }
                                    "failed" => {
                                        tracing::info!("task failed");
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
                                            global.send_task_activity(activity.clone());
                                            state.state.write().await.fail(activity.activity);
                                            break Ok(AgentTaskState::Failed);
                                        } else {
                                            activity.status = "interrupted".to_string();
                                            global.send_task_activity(activity);
                                            state.state.write().await.interrupted();
                                            break Ok(AgentTaskState::Interrupted);
                                        }
                                    }
                                    status => {
                                        tracing::info!("task {}: {}", status, activity.activity);
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

                let res = tokio::select! {
                    _ = cancellation.cancelled() => {
                        tracing::info!("Task {task_id} cancelled");
                        let operator = state.operator.operator();
                        // wait for agent receive timeout.
                        match tokio::time::timeout(
                            Duration::from_secs(60 * 5),
                            agent_activities_listener(operator, is_cron_job, &global, &state, task_id, agent_id, jid, run_id, agent_activities.clone()),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(_) => {
                                match operator {
                                    Operator::Suspend => {
                                        global.send_task_activity(TaskActivity::suspended(task_id, jid));
                                        state.state.write().await.stopped();
                                    }
                                    Operator::Stop => {
                                        global.send_task_activity(TaskActivity::stopped(task_id));
                                        state.state.write().await.stopped();
                                    }
                                    Operator::Run => {
                                        unreachable!("Cancellation should be only trigger by stop or suspend operator")
                                    }
                                }
                                Err(anyhow::anyhow!("Stopping task {} at agent {} timed out", task_id, agent_id))
                            }
                        }
                    },
                    res = agent_activities_listener(state.operator.operator(), is_cron_job, &global,&state,task_id,agent_id, jid, run_id, agent_activities.clone())=> {
                        res
                    },
                };
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
                        tracing::warn!("agent activities listener error: {:#}", err);
                    }
                }
            });
        } else {
            tokio::spawn(async move {
                global.send_task_activity(TaskActivity::started(opts.task.id, jid));
                let runs = opts.runs.load(Ordering::Relaxed);
                let span = tracing::info_span!(
                    "run_task",
                    task.id = opts.task.id,
                    task.jid = %jid,
                    task.rid = runs,
                    task.agent = opts.task.via
                );
                let future = run_task(&global, &opts, &jid).instrument(span);

                let stop_condition = opts.stop_condition.clone();
                let last_state = opts.last_state.clone();

                let handler = move |result| async move {
                    info!("task finished");
                    if let Err(err) = &result {
                        error!(error = %err, backtrace = ?err);
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
                    return should_stop;
                };

                let mut should_stop = tokio::select! {
                    _ = opts.cancellation.cancelled() => {
                        tracing::info!("task cancelled");
                        opts.last_state.write().await.replace(LastState::Stopped);
                        true
                    }
                    result = future => {
                        handler(result).await
                    }
                };

                if !should_stop {
                    should_stop = opts.stop_condition.should_stop();
                }

                let state_guard = opts.last_state.read().await;
                let state = state_guard.as_ref().expect("task should have a last state");
                match state {
                    LastState::Done => {
                        global.send_task_activity(TaskActivity::completed(opts.task.id, jid));
                        opts.state.write().await.completed();
                    }
                    LastState::Stopped => match opts.operator.operator() {
                        Operator::Suspend => {
                            global.send_task_activity(TaskActivity::suspended(opts.task.id, jid));
                        }
                        _ => {
                            global.send_task_activity(TaskActivity::stopped(opts.task.id));
                            opts.state.write().await.stopped();
                        }
                    },
                    LastState::Error(err) => {
                        if should_stop {
                            global.send_task_activity(TaskActivity::failed(
                                opts.task.id,
                                format!("{err:#}"),
                            ));
                            opts.state.write().await.fail(&err);
                        } else {
                            global.send_task_activity(TaskActivity::interrupted(
                                opts.task.id,
                                format!("{err:#}"),
                            ));
                            opts.state.write().await.interrupted();
                        }
                    }
                }
                opts.runs.fetch_add(1, Ordering::Release);
                let _ = tx.send(should_stop);
            });
        }
        self.task.last_waiter.lock().await.replace(rx);
    }

    pub async fn wait(&self) -> Option<LastState> {
        // wait for spawned task finished.
        let mut waiter = { self.task.last_waiter.lock().await.take() };

        match waiter.take().unwrap().await {
            Ok(should_stop) => {
                if should_stop {
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
pub async fn task_job_run(jid: Uuid, task: TaskState, global_state: Arc<GlobalState>) {
    if task.stop_condition.should_stop() {
        tracing::error!("stop condition reached");
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
    let (tx, rx) = oneshot::channel::<()>();
    let opts = TaskJob::new(jid, task.clone(), global_state.as_ref().clone());

    let opts_cancellation_handler = opts.clone();
    tokio::spawn(async move {
        match rx.await {
            Ok(_) => {
                // Normally completed.
                tracing::debug!("task finished successfully");
            }
            Err(err) => {
                tracing::warn!(
                    "task is stopped unexpectedly, gracefully release job resources for {}",
                    opts_cancellation_handler.job_id
                );
                error!("task error: {:#}", err);
                opts_cancellation_handler.stop().await;
            }
        }
    });

    opts.spawn().await;

    let completed = opts.wait().await;
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
    debug_assert!(tx.send(()).is_ok());
}
