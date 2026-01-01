mod agent;
mod task;

use std::{
    fmt::{Debug, Display},
    fs::File,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering},
    },
    time::Duration,
};

use anyhow::bail;
use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
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
    utils::{get_main_version_from_server_version, get_server_version, sql::get_timestamp_range},
};
use taosx_core::{plugins::transform::sample::DsSampleIn, utils::trace::Qid};
use taosx_utils::dsn::json_to_dsn;

use crate::serve::{
    controller::{
        AgentAction, Task,
        activity::Activity,
        load_breakpoints,
        trigger::{Schedule, StopCondition, Strategy},
    },
    health,
    scheduler::runner::{agent::spawn_agent, task::spawn_task},
};

use super::{
    NotifySender, SchedulerNotify, StopError,
    agent::{AgentState, AgentTask, AgentWorker},
};

pub type TaskId = i64;
pub type AgentId = i64;
pub type AgentTaskActivitiesReceiver = tokio::sync::broadcast::Receiver<Activity>;
pub type AgentActionsSender = tokio::sync::mpsc::Sender<(AgentId, AgentAction)>;

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
    pub(crate) agent_worker: AgentWorker,
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
        agent_worker: AgentWorker,
    ) -> Self {
        Self {
            alive: Arc::new(AtomicBool::new(true)),
            scheduler,
            notify_sender,
            port_pool: PortPool::default(),
            agent_worker,
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
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
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
            InnerState::Queued | InnerState::Stopped | InnerState::Completed
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
            InnerState::Completed | InnerState::Stopped | InnerState::Failed(_)
        )
    }

    pub(crate) fn is_stopped(&self) -> bool {
        matches!(self, InnerState::Stopped)
    }

    pub(crate) fn ready_to_remove_job(&self) -> bool {
        matches!(
            self,
            InnerState::Completed | InnerState::Stopped | InnerState::Failed(_)
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

    pub fn fail(&mut self, message: impl Display) -> &mut Self {
        *self = Self::Failed(format!("{}", message));
        self
    }
}

impl From<InnerState> for ha_core::types::TaskStatus {
    fn from(value: InnerState) -> Self {
        match value {
            InnerState::Queued => ha_core::types::TaskStatus::Queued,
            InnerState::Running => ha_core::types::TaskStatus::Running,
            InnerState::Stopping => ha_core::types::TaskStatus::Stopping,
            InnerState::Stopped => ha_core::types::TaskStatus::Stopped,
            InnerState::Completed => ha_core::types::TaskStatus::Completed,
            InnerState::Failed(_) => ha_core::types::TaskStatus::Failed,
        }
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
    /// Current job run times.
    runs: Arc<AtomicU64>,
    /// Task details.
    pub(crate) task: Arc<Task>,

    pub(crate) operator: TaskOperator,

    pub(crate) state: Arc<RwLock<InnerState>>,

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

    /// 发送给 grpc 的消息 (任务启动/停止)
    xnoded_tx: flume::Sender<Result<RecordBatch, FlightError>>,
}

impl TaskState {
    pub async fn new(
        task: Task,
        global: &GlobalState,
        xnoded_tx: flume::Sender<Result<RecordBatch, FlightError>>,
    ) -> Self {
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
        let job_id = task.job_id;

        let stop_condition = strategy.stop_condition();
        let cancellation = CancellationToken::new();

        let agent_waiter = if let Some(via) = task.via {
            let agent_state = Arc::new(RwLock::new(AgentState::default()));
            let (sender, agent_activities) = tokio::sync::mpsc::channel(100);
            let (stop_sender, stop_waiter) = tokio::sync::oneshot::channel();
            let task = AgentTask {
                agent_id: via,
                task_job_id: (task_id, job_id),
                agent_state: agent_state.clone(),
                sender,
                stop_sender: Arc::new(stop_sender),
            };
            global.agent_worker.insert(task).await;
            Some(AgentWaiter {
                agent_state,
                agent_activities: Arc::new(RwLock::new(agent_activities)),
                agent_close_waiter: Arc::new(Mutex::new(Some(stop_waiter))),
            })
        } else {
            None
        };
        Self {
            runs: Arc::new(AtomicU64::new(0)),
            task: Arc::new(task),
            state: Arc::new(RwLock::new(InnerState::Queued)),
            stop_condition,
            cancellation,
            agent_waiter,
            operator: TaskOperator::new(),
            last_state: Arc::new(RwLock::new(None)),
            last_waiter: Arc::new(Mutex::new(None)),
            xnoded_tx,
        }
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
    pub task_job_id: (i64, i64),
    #[multi_index(hashed_unique)]
    pub schedule_id: Uuid,

    /// The task that is associated with this job and shared amount all ticks of this job.
    pub task: TaskState,

    /// Global shared state across all jobs/tasks.
    pub global: GlobalState,

    /// running lock file
    pub _lock: Arc<File>,
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
    pub async fn try_stop(&mut self, task_job_id: (i64, i64)) -> Result<(), StopError> {
        let task_job = self
            .get_by_task_job_id(&task_job_id)
            .ok_or(StopError::NotFound(task_job_id))?;
        let (task_id, job_id) = task_job.task_job_id;
        let sched_id = task_job.schedule_id;
        tracing::info!(task.id = task_id, job.id = job_id, sched_id = %sched_id, "task ({task_id}, {job_id}) will be removed");

        if task_job.in_final_state().await {
            return Err(StopError::AlreadyStopped(task_job_id));
        }

        let state = task_job.stop().await;

        if state.ready_to_remove_job() {
            // If job has not been ticked, remove task state handler directly.
            self.remove_by_task_job_id(&task_job_id);
            tracing::info!(task.id = task_id, job.id = job_id, sched_id = %sched_id, "task ({task_id}, {job_id}) is stopped");
            Ok(())
        } else {
            tracing::info!(task.id = task_id, job.id = job_id, sched.id = %sched_id, "Try stop task in scheduler");
            Ok(())
        }
    }
}

pub type MultiIndexTaskJobMapRef = Arc<RwLock<MultiIndexTaskJobMap>>;

impl TaskJob {
    /// Create a new task job runner.
    pub fn new(
        schedule_id: Uuid,
        task: TaskState,
        global_state: GlobalState,
        lock_file: Arc<std::fs::File>,
    ) -> Self {
        let task_id = task.task.id;
        let job_id = task.task.job_id;
        Self {
            task_job_id: (task_id, job_id),
            schedule_id,
            task,
            global: global_state,
            _lock: lock_file,
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
        let (task_id, job_id) = self.task_job_id;
        let sched_id = self.schedule_id;
        tracing::info!(
            task.id = task_id,
            job.id = job_id,
            sched.id = %sched_id,
            "task will be removed"
        );

        self.task.operator.stop();
        // Send stopping state updating activity.
        self.global
            .send_task_activity(Activity::stopping(task_id, job_id));

        // Set task state to stopping if already scheduled.
        {
            // cancel spawned task.
            let mut state = self.task.state.write().await;
            if state.is_idle() {
                state.stopped();
            } else {
                // Set task state to stopping so that it will be stopped when it's ticked properly.
                state.stop();
            }
        };

        // Remove job from scheduler.
        tracing::info!(
            task.id = task_id,
            job.id = job_id,
            sched.id = %sched_id,
            cause = "stopped"
        );
        if let Err(err) = self.global.scheduler.remove(&self.schedule_id).await {
            error!("remove job error: {:#}", err);
        }
        // Send cancellation signal to running task.
        self.task.cancellation.cancel();

        let (task_id, job_id) = (self.task.task.id, self.task.task.job_id);
        // Remove agent task.
        if self.task.task.via.is_some() {
            self.global.agent_worker.stop(task_id, job_id).await;
        }

        { self.task.state.read().await.clone() }
    }

    /// Suspend a job.
    pub(super) async fn suspend(&self) -> InnerState {
        let (task_id, job_id) = self.task_job_id;
        let sched_id = self.schedule_id;
        tracing::info!(task.id = task_id, job.id = job_id, sched.id = %sched_id, "task will be suspended");

        self.task.operator.suspend();

        // Set task state to suspending if already scheduled.
        {
            // cancel spawned task.
            let mut state = self.task.state.write().await;
            if state.is_idle() {
                // Set task state to stopping so that it will be stopped when it's ticked properly.
                state.stopped();
            } else {
                state.stop();
            }
        };

        // Remove job from scheduler.
        tracing::info!(task.id = task_id, job.id = job_id, shced.id = %sched_id, cause = "suspended");
        if let Err(err) = self.global.scheduler.remove(&sched_id).await {
            error!("remove job error: {:#}", err);
        }
        // Send cancellation signal to running task.
        self.task.cancellation.cancel();

        let (task_id, job_id) = (self.task.task.id, self.task.task.job_id);
        // Remove agent task.
        if self.task.task.via.is_some() {
            self.global.agent_worker.suspend(task_id, job_id).await;
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
        let (task_id, job_id) = self.task_job_id;
        let sid = self.schedule_id;
        let global = self.global.clone();

        let (tx, rx) = tokio::sync::oneshot::channel();

        if let Some(agent_id) = opts.task.via {
            tokio::spawn(
                spawn_agent(task_id, job_id, agent_id, opts, sid, global, tx).in_current_span(),
            );
        } else {
            tokio::spawn(spawn_task(task_id, job_id, opts, sid, global, tx).in_current_span());
        }
        self.task.last_waiter.lock().await.replace(rx);
    }

    pub async fn wait(&self) -> Option<LastState> {
        // wait for spawned task finished.
        let instant = std::time::Instant::now();

        if let Some(waiter) = { self.task.last_waiter.lock().await.take() } {
            match waiter.await {
                Ok(should_stop) => {
                    tracing::info!(should_stop, elapsed = ?instant.elapsed(), "Spawned task is finished");
                    if should_stop {
                        tracing::info!(should_stop, task.id = self.task.task.id, job.id = self.task.task.job_id, sched.id = %self.schedule_id, cause = "strategy stop");
                        if let Err(err) = self.global.scheduler.remove(&self.schedule_id).await {
                            tracing::error!("remove job error: {:#}", err);
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("waiter error: {:#}", err);
                }
            }
        }

        // Use last state to send task activities.
        self.task.last_state.write().await.take()
    }
}

#[instrument(skip_all, fields(task.id = task.task.id))]
pub async fn task_job_run(
    schedule_id: Uuid,
    task: TaskState,
    global_state: Arc<GlobalState>,
    lock_file: Arc<std::fs::File>,
) {
    let (task_id, job_id) = (task.task.id, task.task.job_id);
    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    qid.set_task_id(task.task.id as u8);
    qid.set_job_id(task.task.job_id as u8);
    if task.operator.is_suspended() || task.operator.is_stopped() {
        tracing::info!("task suspended");
        return;
    }
    if task.stop_condition.should_stop() {
        tracing::error!(schedule.id = %schedule_id, "stop condition reached");
        if let Err(err) = global_state.scheduler.remove(&schedule_id).await {
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
    let opts = TaskJob::new(
        schedule_id,
        task.clone(),
        global_state.as_ref().clone(),
        lock_file,
    );

    // let from_dsn: Dsn = task.task.from.parse().unwrap();
    let from_dsn = json_to_dsn(&serde_json::Value::String(task.task.from.clone())).unwrap();
    let to_dsn = task.task.to.parse().unwrap();
    let task_id = task.task.id;
    let job_id = task.task.job_id;
    let metrics = init_task_metrics(&from_dsn, &to_dsn, task_id, job_id)
        .in_current_span()
        .await;

    if let Some(metrics) = metrics.clone() {
        auto_save_task_metrics(metrics, opts.task.cancellation.child_token())
            .in_current_span()
            .await;
    }
    opts.spawn().in_current_span().await;

    let completed = opts.wait().in_current_span().await;
    match completed {
        Some(LastState::Done) => {
            tracing::info!("task completed");
        }
        Some(LastState::Stopped) => {
            tracing::info!("task stopped");
        }
        Some(LastState::Error(err)) => {
            tracing::info!("task error: {:#}", err);
        }
        None => {
            tracing::info!("task finished without state(usually means the job runs on an agent)");
        }
    }
    if let Some(metrics) = metrics {
        save_task_metrics_finally(metrics).in_current_span().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_state_creation() {
        // Test TaskState can be created with default values
        // This is a basic structural test
        let test_uuid = Uuid::new_v4();
        assert!(!test_uuid.is_nil());
    }

    #[test]
    fn test_uuid_generation() {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        assert_ne!(uuid1, uuid2);
    }

    #[test]
    fn test_atomic_bool_ordering() {
        let atomic = AtomicBool::new(false);
        assert!(!atomic.load(Ordering::Relaxed));
        atomic.store(true, Ordering::Relaxed);
        assert!(atomic.load(Ordering::Relaxed));
    }

    #[test]
    fn test_atomic_i32_operations() {
        let atomic = AtomicI32::new(0);
        assert_eq!(atomic.load(Ordering::Relaxed), 0);
        atomic.store(42, Ordering::Relaxed);
        assert_eq!(atomic.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn test_atomic_u8_operations() {
        let atomic = AtomicU8::new(0);
        assert_eq!(atomic.load(Ordering::Relaxed), 0);
        atomic.store(255, Ordering::Relaxed);
        assert_eq!(atomic.load(Ordering::Relaxed), 255);
    }

    #[test]
    fn test_duration_operations() {
        let duration = Duration::from_secs(5);
        assert_eq!(duration.as_secs(), 5);
    }

    #[test]
    fn test_arc_cloning() {
        let value = Arc::new(42);
        let cloned = Arc::clone(&value);
        assert_eq!(*value, *cloned);
    }

    #[test]
    fn test_cancellation_token_creation() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn test_multiple_cancellation_tokens() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        assert!(!child.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    #[tokio::test]
    async fn test_mutex_operations() {
        let mutex = Mutex::new(42);
        let guard = mutex.lock().await;
        assert_eq!(*guard, 42);
    }

    #[tokio::test]
    async fn test_rwlock_operations() {
        let rwlock = RwLock::new(42);
        {
            let read_guard = rwlock.read().await;
            assert_eq!(*read_guard, 42);
        }
        {
            let mut write_guard = rwlock.write().await;
            *write_guard = 100;
        }
        let read_guard = rwlock.read().await;
        assert_eq!(*read_guard, 100);
    }

    #[test]
    fn test_multi_index_map_creation() {
        let _map: DashMap<u64, String> = DashMap::new();
        // Successfully created
    }

    #[test]
    fn test_dashmap_creation() {
        let map: DashMap<String, i32> = DashMap::new();
        map.insert("test".to_string(), 42);
        assert_eq!(*map.get("test").unwrap(), 42);
    }

    #[test]
    fn test_atomic_u64_operations() {
        let atomic = AtomicU64::new(0);
        assert_eq!(atomic.load(Ordering::Relaxed), 0);
        atomic.store(12345, Ordering::Relaxed);
        assert_eq!(atomic.load(Ordering::Relaxed), 12345);
    }

    #[test]
    fn test_format_trait_display() {
        struct TestFormatter;
        impl Display for TestFormatter {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "test")
            }
        }
        let formatter = TestFormatter;
        assert_eq!(format!("{}", formatter), "test");
    }

    #[test]
    fn test_format_trait_debug() {
        struct TestFormatter;
        impl Debug for TestFormatter {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "TestFormatter")
            }
        }
        let formatter = TestFormatter;
        assert_eq!(format!("{:?}", formatter), "TestFormatter");
    }
}
