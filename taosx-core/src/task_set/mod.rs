use std::{
    fmt::Debug,
    mem::transmute_copy,
    ops::{ControlFlow, Deref},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use crate::{
    Action, Parser, TaskNotify, TaskNotifySender,
    core_metrics::CoreMetrics,
    plugins::transform::sample::DsSampleIn,
    utils::breakpoints::{BreakpointDb, breakpoints_db_dir},
};
use anyhow::bail;
use bon::Builder;
use faststr::FastStr;
use serde::{Deserialize, Serialize};
use taos::{Dsn, tokio::task::JoinSet};
use tokio::{
    sync::{Mutex, broadcast},
    task::AbortHandle,
};
use tokio_util::sync::{CancellationToken, DropGuard};

mod error;
mod health;
pub mod prelude;

pub enum SourceType {
    /// Stream data source with single schema.
    FlatStream,
    /// Stream data source with multiple schema.
    LushStream,
    /// Stream data source with multiple schema.
    PointStream,
    /// TDengine message queue.
    Tmq,
    /// Schemaless stream data source.
    Sml,
}

pub struct TaskOptions {
    metrics: CoreMetrics,
    env: Runtime,
}

#[derive(Builder)]
pub struct SinkName {
    pub id: FastStr,
    pub name: FastStr,
    #[builder(default)]
    pub aliases: Vec<FastStr>,
}

#[derive(Builder)]
pub struct SourceName {
    pub id: FastStr,
    pub name: FastStr,
    #[builder(default)]
    pub aliases: Vec<FastStr>,
}

/// Runtime environment.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Runtime {
    /// For command line runtime environment.
    Cli {
        /// Work dir, usually the directory where the cache/checkpoints files located.
        wd: PathBuf,
    },
    /// For temporary runtime environment, usually used for task setup in server side.
    Tmp {
        /// Agent ID
        aid: Option<i64>,
        /// Remote server.
        remote: Option<FastStr>,
    },
    /// For server/api runtime environment with specified task id.
    ///
    /// This is used for server to run a task, either with or without agent.
    Server {
        /// Task ID.
        tid: i64,
        /// Job ID.
        jid: i64,
        /// Schedule UUID.
        sid: FastStr,
        /// Agent ID.
        aid: Option<i64>,
    },
    /// For agent runtime environment with specified agent id, task id and job uuid.
    ///
    /// This is used for agent to run a task.
    Agent {
        /// Task ID
        tid: i64,
        /// Job ID.
        jid: i64,
        /// Schedule UUID
        sid: FastStr,
        /// Agent ID
        aid: i64,
        /// Remote server
        remote: FastStr,
    },
}

impl Runtime {
    pub fn is_cli(&self) -> bool {
        matches!(self, Runtime::Cli { .. })
    }

    pub fn is_serve(&self) -> bool {
        matches!(
            self,
            Runtime::Server { .. } | Runtime::Tmp { aid: None, .. }
        )
    }

    pub fn is_agent(&self) -> bool {
        matches!(
            self,
            Runtime::Agent { .. } | Runtime::Tmp { aid: Some(_), .. }
        )
    }

    /// Run a task in local environment.
    pub fn in_local(&self) -> bool {
        matches!(
            self,
            Runtime::Cli { .. } | Runtime::Server { aid: None, .. } | Runtime::Agent { .. }
        )
    }

    pub fn id(&self) -> FastStr {
        match self {
            Runtime::Cli { wd } => {
                let s = format!("cli:{}", wd.display());
                FastStr::from_string(s)
            }
            Runtime::Tmp { aid, remote } => {
                if aid.is_none() {
                    return FastStr::from_static_str("tmp");
                }
                let s = format!("tmp:{:?}:{:?}", aid, remote);
                FastStr::from_string(s)
            }
            Runtime::Server { tid, sid: jid, .. } => {
                FastStr::from_string(format!("serve:{tid}:{jid}"))
            }
            Runtime::Agent {
                aid, tid, sid: jid, ..
            } => FastStr::from_string(format!("agent:{aid}:{tid}:{jid}")),
        }
    }

    pub fn task_job_id(&self) -> Option<(i64, i64)> {
        match self {
            Runtime::Server { tid, jid, .. } | Runtime::Agent { tid, jid, .. } => {
                Some((*tid, *jid))
            }
            _ => None,
        }
    }

    pub fn breakpoint_db(&self) -> anyhow::Result<BreakpointDb> {
        match self {
            Runtime::Cli { wd } => BreakpointDb::open(&wd.join("breakpoints")),
            Runtime::Server { tid, jid, .. } | Runtime::Agent { tid, jid, .. } => {
                BreakpointDb::open(&breakpoints_db_dir(*tid, *jid))
            }
            _ => bail!("Breakpoint not supported for this runtime"),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Environment {
    /// Runtime environment.
    pub runtime: Runtime,
    /// Task guard.
    pub guard: TaskGuard,
    /// Breakpoints.
    pub breakpoints: Option<String>,
    /// Task properties.
    pub props: ExecOpts,
    /// Task activities notifier.
    pub notifier: TaskNotifySender,
}

impl Deref for Environment {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}
impl Environment {
    pub fn new(runtime: Runtime, props: ExecOpts, notifier: TaskNotifySender) -> Self {
        let guard = TaskGuard::new();
        Self {
            runtime,
            guard,
            breakpoints: None,
            props,
            notifier,
        }
    }
    pub fn from_cli<T: Into<PathBuf>>(wd: T, props: ExecOpts, notifier: TaskNotifySender) -> Self {
        let wd = wd.into();
        let runtime = Runtime::Cli { wd };
        let guard = TaskGuard::new();
        Self {
            runtime,
            guard,
            breakpoints: None,
            props,
            notifier,
        }
    }

    pub fn from_server(
        tid: i64,
        jid: i64,
        sid: FastStr,
        aid: Option<i64>,
        props: ExecOpts,
        notifier: TaskNotifySender,
    ) -> Self {
        let runtime = Runtime::Server { tid, jid, sid, aid };
        let guard = TaskGuard::new();
        Self {
            runtime,
            guard,
            breakpoints: None,
            props,
            notifier,
        }
    }

    pub fn from_agent(
        aid: i64,
        tid: i64,
        jid: i64,
        sid: FastStr,
        remote: FastStr,
        props: ExecOpts,
        notifier: TaskNotifySender,
    ) -> Self {
        let runtime = Runtime::Agent {
            aid,
            tid,
            jid,
            sid,
            remote,
        };
        let guard = TaskGuard::new();
        Self {
            runtime,
            guard,
            breakpoints: None,
            props,
            notifier,
        }
    }

    pub fn guard(&self) -> &TaskGuard {
        &self.guard
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn eid(&self) -> FastStr {
        self.runtime.id()
    }

    pub fn task_job_id(&self) -> Option<(i64, i64)> {
        self.runtime.task_job_id()
    }
    pub fn into_context(self) -> anyhow::Result<Context> {
        Context::new(self)
    }
}
/// Task notification for stop or cancel signal.
#[derive(Debug, Clone)]
pub struct TaskGuard {
    stop: CancellationToken,
    cancel: CancellationToken,
    sender: broadcast::Sender<TaskNotify>,
}

pub enum TaskGuardSignal {
    Stop,
    Cancel,
}
impl Default for TaskGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskGuard {
    /// Create a new task notification channel.
    pub fn new() -> Self {
        let stop = CancellationToken::new();
        let cancellation = stop.child_token();
        let (sender, _receiver) = broadcast::channel(64);
        Self {
            cancel: cancellation,
            stop,
            sender,
        }
    }

    pub fn new_with_token(stop: CancellationToken) -> Self {
        let cancellation = stop.child_token();
        let (sender, _receiver) = broadcast::channel(64);
        Self {
            cancel: cancellation,
            stop,
            sender,
        }
    }

    /// Send stop signal to stop the task.
    pub fn stop(&self) {
        self.stop.cancel();
    }

    /// Send cancel signal to cancel the task.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Get the guard for task cancellation.
    pub fn guard(&self) -> DropGuard {
        self.cancel.clone().drop_guard()
    }

    /// Check if the task is stopped manually.
    pub fn is_stopped(&self) -> bool {
        self.stop.is_cancelled()
    }

    /// Check if the task is cancelled by system.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Wait for the task to be cancelled.
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await
    }

    /// Wait for the task to be stopped.
    pub async fn stopped(&self) {
        self.stop.cancelled().await
    }

    /// Wait for the task to be stopped or cancelled.
    pub async fn signal(&self) -> TaskGuardSignal {
        tokio::select! {
            biased;

            // High priority for stop signal.
            _ = self.stop.cancelled() => TaskGuardSignal::Stop,
            // Low priority for cancel signal.
            _ = self.cancel.cancelled() => TaskGuardSignal::Cancel,
        }
    }

    pub async fn notify(
        &self,
        msg: TaskNotify,
    ) -> Result<usize, broadcast::error::SendError<TaskNotify>> {
        self.sender.send(msg)
    }

    pub fn receiver(&self) -> broadcast::Receiver<TaskNotify> {
        self.sender.subscribe()
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TaskOpts {
    pub from: Dsn,
    pub transform: Vec<Action>,
    pub parser: Option<Parser>,
    pub to: Dsn,
}

pub struct TaskProps {
    source_type: SourceType,
    source_name: SourceName,
    sink_name: SinkName,
    reenterable: bool,
    resettable: bool,
    environment: Runtime,
    source_dsn: Dsn,
    sink_dsn: Dsn,
}

#[derive(Debug, Clone)]
pub struct ExecOpts {
    /// Does the task support reentering?
    reenterable: bool,
    /// Does the task support resetting?
    resettable: bool,
    /// Backoff time in seconds when task failed.
    backoff: u32,
    /// Maximum backoff time in seconds.
    max_backoff: u32,
    /// Maximum retry times. Task will be failed if retry times exceed from last interruption.
    max_retry: u32,
    /// License checking interval
    license_interval: Duration,
    /// Max wait time for stopping a task.
    stop_timeout: Duration,
}

pub enum ErrorGrade {
    /// Error is recoverable.
    Recoverable,
    /// Error is fatal.
    Fatal,
}

#[derive(Debug, Default)]
pub struct Current {
    pub retries: u32,
    pub re_entered: bool,
    pub running: bool,
}
#[derive(Debug)]
pub struct Context {
    /// Runtime environment.
    pub env: Environment,
    /// Breakpoints database.
    pub breakpoints: BreakpointDb,
    /// Runtime set for task spawner.
    pub runtime: JoinSet<anyhow::Result<()>>,
    /// Current task status.
    pub current: Current,
    // Task notification sender.
    // pub notify: broadcast::Sender<TaskNotify>,
}

impl Drop for Context {
    fn drop(&mut self) {
        futures::executor::block_on(self.runtime.shutdown());
    }
}

impl Context {
    /// Create new context from environment.
    ///
    pub fn new(env: Environment) -> anyhow::Result<Self> {
        let breakpoints = env.breakpoint_db()?;
        let runtime = JoinSet::new();
        Ok(Self {
            env,
            breakpoints,
            runtime,
            current: Default::default(),
        })
    }

    pub fn child_token(&self) -> CancellationToken {
        self.env.guard.cancel.clone()
    }

    /// Shutdown the context, once called, all tasks in the context will be aborted.
    pub async fn shutdown(&mut self) {
        self.runtime.shutdown().await;
    }
}

pub struct TaskContainer {
    task: Box<dyn TaskExecutor>,
    props: TaskProps,
}

#[derive(Debug)]
pub enum TaskExitStatus {
    /// Task is finished.
    Ok,
    /// Task is stopped.
    Stopped,
    /// Task stopped after timeout.
    ///
    /// This usually means the task is not stopped gracefully.
    StoppedTimeout(Duration),
    /// Task is failed when stopped or cancelled.
    StopError(anyhow::Error),
    /// Fatal error occurred.
    Fatal(anyhow::Error),
    /// Exceeded the maximum retry times.
    Exceeded(u32),
}

pub enum Exit {
    Completed,
    Cancelled,
}

impl TaskExitStatus {
    pub fn is_stopped(&self) -> bool {
        matches!(
            self,
            Self::Stopped | Self::StoppedTimeout(_) | Self::StopError(_)
        )
    }
}
pub enum TaskStatus {
    /// Task is initialized.
    Initialized,
    /// Task is running.
    Running,
    /// Task is stopped.
    Stopped,
    /// Task is finished.
    Finished,
    /// Task is failed.
    Failed,
}
/// # Methods and lifetimes
///
/// ```text
///
/// ```
#[async_trait::async_trait]
pub trait TaskExecutor: std::fmt::Debug + Send + Sync {
    /// To check if the task is license ok or not.
    async fn license(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Check if the source to sink task is valid or not.
    async fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Get sample data for testing or initializing from source.
    async fn sample(&self) -> anyhow::Result<DsSampleIn>;

    /// The metrics of the task.
    fn metrics(&self) -> &Arc<CoreMetrics>;

    /// Run the task once.
    async fn run(&self, context: &Context) -> anyhow::Result<Exit>;

    /// Do thing when a task is started.
    async fn initialize(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Do things before executing the data transferring.
    async fn before_start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Do thing when a task is reset.
    async fn reset(&self) {}

    /// Do things when an error occurred.
    ///
    /// Returns whether to break the task or continue to retry.
    async fn on_error(&self, _err: anyhow::Error) -> ControlFlow<anyhow::Error> {
        ControlFlow::Continue(())
    }
    /// Do things when a fatal error occurred.
    async fn on_fatal(&self) {}

    /// Do things before completing the task.
    async fn on_completed(&self) {}

    /// Do things when stop signal received.
    async fn on_stop(&self) {}

    /// Callback after stopped a task if successful.
    async fn after_stop(&self) {}
}

type SpawnerId = (FastStr, FastStr);

#[derive(Debug, Clone, Default, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum RunState {
    #[default]
    /// Task executor is initialized, we say the task is in backlog.
    Backlog,
    /// Task is started, but not in running state.
    Prepare,
    /// Task is running.
    Running,
    /// Task is pending, means the task is failed and is waiting for retry.
    Pending,
    /// Task run into the end, wait for post scripts running and releasing resources.
    Releasing,
    /// Task runs into end normally.
    Completed,
    /// Task is failed.
    Failed,
    /// Task is stopped normally.
    Stopped,
    /// Task is cancelled normally by system.
    Cancelled,
    /// Task is aborted by tokio cancellation event, which means the task is not finished in container.
    Aborted,
    /// Task finished into final state, means the task is completed, stopped, cancelled or failed.
    Final,
}

impl RunState {
    pub fn is_executing(&self) -> bool {
        *self > Self::Backlog && *self < Self::Completed
    }
    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped | Self::Cancelled)
    }
}

impl From<u8> for RunState {
    fn from(v: u8) -> Self {
        unsafe { transmute_copy(&v) }
    }
}

/// Atomic run state for task execution.
///
/// You can clone it any times, and it will share the same state.
#[derive(Debug, Clone)]
struct AtomicRunState {
    state: Arc<AtomicU8>,
    broadcast: Arc<broadcast::Sender<RunState>>,
}

struct AtomicRusStateGuard {
    state: Arc<AtomicU8>,
}
impl Drop for AtomicRusStateGuard {
    fn drop(&mut self) {
        let _ = self
            .state
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |s| {
                if RunState::from(s).is_executing() {
                    Some(RunState::Aborted)
                } else {
                    None
                }
                .map(|v| v as _)
            });
    }
}

impl AtomicRunState {
    fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(0)),
            broadcast: Arc::new(broadcast::channel(64).0),
        }
    }

    fn set(&self, state: RunState) {
        self.broadcast.send(state).ok();
        self.state.store(state as _, Ordering::Relaxed);
    }

    pub fn get(&self) -> RunState {
        self.state.load(Ordering::Relaxed).into()
    }

    fn prepare(&self) {
        self.set(RunState::Prepare);
    }
    fn run(&self) {
        self.set(RunState::Running);
    }
    fn pend(&self) {
        self.set(RunState::Pending);
    }

    fn stop(&self) {
        self.set(RunState::Stopped);
    }
    fn cancel(&self) {
        self.set(RunState::Cancelled);
    }
    fn release(&self) {
        self.set(RunState::Releasing);
    }
    fn complete(&self) {
        self.set(RunState::Completed);
    }
    fn fail(&self) {
        self.set(RunState::Failed);
    }

    // fn fetch_update<F: FnMut(RunState) -> Option<RunState>>(&self, mut f: F) {
    //     let _ = self
    //         .state
    //         .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |s| {
    //             f(s.into()).map(|v| v as _)
    //         });
    // }

    fn guard(&self) -> AtomicRusStateGuard {
        AtomicRusStateGuard {
            state: self.state.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Executor {
    env: Environment,
    opts: Arc<TaskOpts>,
    executor: Arc<Box<dyn TaskExecutor>>,
    state: AtomicRunState,
}

impl Executor {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn execute(self) -> anyhow::Result<TaskExitStatus> {
        let exec = self;
        let _guard = exec.state.guard();
        exec.state.prepare();

        let mut context = exec.env.into_context()?;

        // let _health = health::health_checker(options, rx, metrics);
        let guard = context.env.guard().clone();
        let stop_timeout = context.env.props.stop_timeout;

        exec.executor.license().await?;
        exec.executor.initialize().await?;

        let mut backoff = 0;
        let mut elapsed = Duration::ZERO;
        loop {
            let instant = tokio::time::Instant::now();
            if backoff > 0 {
                context.current.re_entered = true
            }
            exec.executor.before_start().await?;
            context.current.running = true;
            exec.state.run();
            let mut running = exec.executor.run(&context);
            // tokio::pin!(running);
            elapsed += instant.elapsed();
            let handle_stop = async {
                tracing::info!("Stop");
                exec.executor.on_stop().await;
            };
            let exit = tokio::select! {
                biased; // High priority for stop signal.
                _ = guard.cancelled() => {
                    if guard.is_stopped() {
                        exec.state.stop();
                        handle_stop.await;
                    } else {
                        exec.state.cancel();
                    }
                    tracing::info!(stop.timeout = ?stop_timeout, "Task is cancelled");
                    let exit = tokio::time::timeout(stop_timeout, running.as_mut())
                        .await
                        .map_or_else(|_| {
                            tracing::info!("Task stop timeout, force stop");
                            TaskExitStatus::StoppedTimeout(stop_timeout)
                        }, |res| {
                            res.map_or_else(|err| {
                                TaskExitStatus::StopError(err)
                            }, |_| {
                                TaskExitStatus::Stopped
                            })
                        });

                    drop(running);
                    context.current.running = false;
                    break Ok(exit);
                }
                exit = &mut running => {
                    exit
                }
            };
            drop(running);
            context.current.running = false;
            match exit {
                Ok(Exit::Completed) => {
                    tracing::info!(?elapsed, "Task finished");
                    exec.state.release();
                    exec.executor.on_completed().await;
                    tracing::info!("Release task resources");
                    context.shutdown().await;
                    exec.state.complete();
                    break Ok(TaskExitStatus::Ok);
                }
                Ok(Exit::Cancelled) => {
                    tracing::info!(?elapsed, "Task finished with cancel signal");
                    context.shutdown().await;
                    exec.state.cancel();
                    break Ok(TaskExitStatus::Ok);
                }
                Err(err) => {
                    tracing::error!(?elapsed, cause = err.root_cause(), context = %err, "Task failed");
                    let c = exec.executor.on_error(err).await;
                    if let ControlFlow::Break(exit) = c {
                        exec.state.fail();
                        exec.executor.on_fatal().await;
                        break Ok(TaskExitStatus::Fatal(exit));
                    }
                    context.current.retries += 1;
                    context.current.re_entered = true;
                    if context.current.retries > context.env.props.max_retry {
                        exec.state.fail();
                        exec.executor.on_fatal().await;
                        context.shutdown().await;
                        break Ok(TaskExitStatus::Exceeded(context.current.retries));
                    }
                    exec.state.pend();
                    if backoff + context.env.props.backoff > context.env.props.max_backoff {
                        backoff = context.env.props.max_backoff;
                    } else {
                        backoff += context.env.props.backoff;
                    }
                    tracing::info!(
                        retries = context.current.retries,
                        backoff,
                        "Task failed, retry with backoff {}s",
                        backoff
                    );
                    tokio::time::sleep(Duration::from_secs(backoff as _)).await;
                }
            }
        }
    }
}
#[derive(Default)]
pub struct Container {
    /// Spawner set.
    set: linked_hash_map::LinkedHashMap<SpawnerId, Box<dyn TaskSpawner>>,
    aliases: linked_hash_map::LinkedHashMap<SpawnerId, SpawnerId>,

    /// Executors built from task spawners.
    ///
    /// This is used to cache the executors for the same task options.
    executors: scc::HashMap<FastStr, Executor>,

    /// Executors indexed by task id.
    executors_by_id: scc::HashMap<(i64, i64), Executor>,

    /// Join set for all executors.
    join_set: Arc<Mutex<JoinSet<anyhow::Result<TaskExitStatus>>>>,
}

pub struct TaskHandler {
    id: FastStr,
    task_job_id: Option<(i64, i64)>,
    state: AtomicRunState,
    handle: AbortHandle,
}

impl Container {
    /// Register new task spawner.
    pub fn register<T: TaskSpawner>(&mut self, spawner: T) -> anyhow::Result<()> {
        let source = spawner.source_name();
        let sink = spawner.sink_name();
        let id_pair = (source.id.clone(), sink.id.clone());
        if self
            .set
            .insert(id_pair.clone(), Box::new(spawner))
            .is_some()
        {
            tracing::warn!(
                "Replace task spawner({} -> {}) by new plugin",
                source.id,
                sink.id
            );
        }

        for source_alias in source.aliases {
            let alias = (source_alias.clone(), id_pair.1.clone());
            self.aliases
                .insert(alias, id_pair.clone())
                .inspect(|(k, v)| {
                    tracing::info!(
                        "Alias override ({} -> {}) to ({} -> {})",
                        k,
                        v,
                        source.id,
                        sink.id
                    );
                });
            for sink_alias in sink.aliases.iter() {
                let alias = (source_alias.clone(), sink_alias.clone());
                self.aliases
                    .insert(alias, id_pair.clone())
                    .inspect(|(k, v)| {
                        tracing::info!(
                            "Alias override ({} -> {}) to ({} -> {})",
                            k,
                            v,
                            source.id,
                            sink.id
                        );
                    });
            }
        }

        Ok(())
    }

    /// Run a task and wait for completion or stop/cancel signals.
    #[tracing::instrument(level = "debug", skip_all, fields(tid = ?env.task_job_id()))]
    pub async fn spawn(&self, opts: TaskOpts, env: Environment) -> anyhow::Result<TaskHandler> {
        let exec = self.get_or_build_executor(opts, env).await?;
        if exec.state.get().is_executing() {
            Err(anyhow::anyhow!("Task is already running"))
        } else {
            let id = exec.env.id();
            let task_job_id = exec.env.task_job_id();
            let state = exec.state.clone();
            let handle = self.join_set.lock().await.spawn(exec.execute());
            Ok(TaskHandler {
                id,
                task_job_id,
                state,
                handle,
            })
        }
    }

    /// Run a task and wait for completion or stop/cancel signals.
    #[tracing::instrument(level = "debug", skip_all, fields(tid = ?env.task_job_id()))]
    pub async fn run_task(
        &self,
        opts: TaskOpts,
        env: Environment,
    ) -> anyhow::Result<TaskExitStatus> {
        let exec = self.get_or_build_executor(opts, env).await?;
        exec.execute().await
    }

    /// Stop a task by environment.
    #[tracing::instrument(level = "debug", skip_all, fields(eid = %env.id()))]
    pub async fn stop_task(&self, env: &Environment) -> anyhow::Result<()> {
        let id = env.id();
        if let Some(entry) = self.executors.get(&id) {
            entry.env.guard().stop();
        }
        Ok(())
    }

    /// Stop a task by task ID.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn stop_task_by_id(&self, task_id: i64, job_id: i64) -> anyhow::Result<()> {
        if let Some(entry) = self.executors_by_id.get(&(task_id, job_id)) {
            entry.env.guard().stop();
        }
        Ok(())
    }

    /// Stop all tasks.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn stop_all(&self) {
        self.executors
            .scan_async(|eid, v| {
                tracing::info!(%eid, tid = ?v.env.task_job_id(), "Stop task");
                v.env.guard().stop();
            })
            .await;
    }

    /// Reset a task by task ID.
    ///
    /// Resetting a task will clear the task status, metrics, and other runtime data.
    ///
    /// This is useful when a task is failed and need to be restarted, or a task
    /// is completed and need to be rerun.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn reset_task_by_id(&self, task_id: i64, job_id: i64) -> anyhow::Result<()> {
        if let Some(entry) = self.executors_by_id.get_async(&(task_id, job_id)).await {
            entry.executor.metrics().reset();
            entry.executor.reset().await;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get_or_build_executor(
        &self,
        opts: TaskOpts,
        env: Environment,
    ) -> anyhow::Result<Executor> {
        let opts = Arc::new(opts);
        let sid = (
            opts.from.driver.clone().into(),
            opts.to.driver.clone().into(),
        );
        let sid = self.aliases.get(&sid).cloned().unwrap_or(sid);

        if let Some(spawner) = self.set.get(&sid) {
            let id = env.id();
            if let Some(entry) = self.executors.get(&id) {
                let exec = entry.get();
                if exec.opts == opts {
                    return Ok(entry.get().clone());
                } else {
                    let _ = entry.remove(); // Remove the old executor if the options are different.
                }
            }
            let executor = spawner.executor(&opts).await?;
            let executor_id = env.id();
            let executor = Arc::new(executor);
            let state = AtomicRunState::new();

            // Update the executor cache.
            let exec = Executor {
                executor,
                env,
                opts,
                state,
            };
            if let Some(tid) = exec.env.task_job_id() {
                let _ = self.executors_by_id.insert(tid, exec.clone());
            }
            let _ = self.executors.insert(executor_id, exec);
            self.executors
                .get(&id)
                .ok_or_else(|| anyhow::anyhow!("Task not found"))
                .map(|e| e.get().clone())
        } else {
            anyhow::bail!("Task {} -> {} not found", sid.0, sid.1);
        }
    }
}

#[derive(Debug, Default)]
#[non_exhaustive]
pub struct SpawnerMetrics {
    spawned_tasks: u64,
    running_tasks: u64,
}

#[async_trait::async_trait]
pub trait TaskSpawner: 'static + Send + Sync + Debug {
    fn source_name(&self) -> SourceName;
    fn sink_name(&self) -> SinkName;
    // fn metrics(&self) -> SpawnerMetrics;

    async fn executor(&self, opts: &TaskOpts) -> anyhow::Result<Box<dyn TaskExecutor>>;
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use anyhow::Ok;
    use linked_hash_map::LinkedHashMap;

    use super::*;

    #[test]
    fn test_fast_str() {
        let a = FastStr::from_static_str("hello");
        let b = FastStr::from_static_str("hello");
        let mut map = LinkedHashMap::new();
        map.insert(a.clone(), b.clone());

        let s = "hello".to_string();
        let v = map.get(s.as_str());
        dbg!(v);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test() {
        #[derive(Debug)]
        struct Tmq2Taos;

        #[derive(Debug)]
        struct Tmq2TaosExecutor {
            opts: TaskOpts,
        }

        #[async_trait::async_trait]
        impl TaskSpawner for Tmq2Taos {
            fn source_name(&self) -> SourceName {
                SourceName {
                    id: "tmq".into(),
                    name: "TDengine Subscription".into(),
                    aliases: vec!["sync".into()],
                }
            }

            fn sink_name(&self) -> SinkName {
                SinkName {
                    id: "taos".into(),
                    name: "TDengine".into(),
                    aliases: vec![],
                }
            }

            async fn executor(&self, opts: &TaskOpts) -> anyhow::Result<Box<dyn TaskExecutor>> {
                Ok(Box::new(Tmq2TaosExecutor { opts: opts.clone() }))
            }
        }

        #[async_trait::async_trait]
        impl TaskExecutor for Tmq2TaosExecutor {
            async fn license(&self) -> anyhow::Result<()> {
                Ok(())
            }

            async fn sample(&self) -> anyhow::Result<DsSampleIn> {
                anyhow::bail!("Not supported")
            }

            fn metrics(&self) -> &Arc<CoreMetrics> {
                todo!()
            }

            async fn initialize(&self) -> anyhow::Result<()> {
                tracing::info!("initialize");
                Ok(())
            }

            async fn reset(&self) {
                tracing::info!("reset tmq to taos");
            }

            async fn run(&self, context: &Context) -> anyhow::Result<Exit> {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                let mut count = 0;
                const MAX: u32 = 10;
                loop {
                    if context.env.guard.is_cancelled() {
                        break Ok(Exit::Cancelled);
                    }
                    interval.tick().await;
                    context
                        .env
                        .notifier
                        .send_async(TaskNotify::info("tick"))
                        .await
                        .unwrap();
                    tracing::info!("tick");
                    if count >= MAX {
                        break Ok(Exit::Completed);
                    }
                    count += 1;
                }
            }
            async fn on_stop(&self) {
                tracing::info!("stop");
            }

            async fn before_start(&self) -> anyhow::Result<()> {
                tracing::info!("pre_start");
                Ok(())
            }

            async fn on_error(&self, error: anyhow::Error) -> ControlFlow<anyhow::Error> {
                tracing::info!(%error, "on_error");
                ControlFlow::Continue(())
            }

            async fn on_fatal(&self) {
                tracing::error!("on_fatal");
            }

            async fn on_completed(&self) {
                tracing::error!("on completed");
            }
        }
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let mut set = Container::default();
        set.register(Tmq2Taos).expect("register test task");
        let set = Arc::new(set);

        let opts = TaskOpts {
            from: Dsn::from_str("sync:///testnometa").unwrap(),
            transform: vec![],
            to: Dsn::from_str("taos://").unwrap(),
            parser: None,
        };
        let exec_opts = ExecOpts {
            reenterable: true,
            resettable: true,
            backoff: 100,
            max_backoff: 10,
            max_retry: 5,
            license_interval: Duration::from_secs(1),
            stop_timeout: Duration::from_secs(10),
        };
        let (sender, _receiver) = flume::unbounded();
        let env = Environment::from_cli("/tmp/task-set-env", exec_opts, sender);

        let task = set
            .get_or_build_executor(opts.clone(), env.clone())
            .await
            .expect("get task");

        set.spawn(opts.clone(), env.clone())
            .await
            .expect("spawn task");

        tracing::info!("spawned");
        tokio::time::sleep(Duration::from_secs(5)).await;

        let state = task.state.get();
        assert!(state.is_executing());

        // let _ = task.stop().await.expect("stop task");
        set.stop_task(&env).await.expect("Stop task error");
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(task.state.get(), RunState::Stopped);

        let opts = TaskOpts {
            from: Dsn::from_str("unknown:///testnometa").unwrap(),
            transform: vec![],
            to: Dsn::from_str("taos://").unwrap(),
            parser: None,
        };
        let task = set.get_or_build_executor(opts, env).await;
        assert!(
            task.inspect_err(|err| {
                dbg!(err);
                assert_eq!(
                    err.to_string(),
                    "Task unknown -> taos not found",
                    "Error: {err:#}"
                );
            })
            .is_err()
        );
    }
}
