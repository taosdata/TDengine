use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use itertools::Itertools;
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Weak},
    time::Duration,
};
use taos::{Dsn, IntoDsn};
use taoslog::{
    QidManager,
    utils::{QidMetadataSetter, Span},
};
use taosx_core::plugins::transform::sample::DsSampleIn;
use taosx_core::sink::lush::TableTagCache;
use taosx_core::utils::{breakpoints::BreakpointDb, trace::Qid};
use taosx_core::{DataSet, get_data_dir};
use taosx_core::{dsv::DataSourceValidation, plugins::transform::sample::DsSamples};
use thiserror::Error;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio_cron_scheduler::{Job, JobBuilder, JobScheduler};
use tracing::{Instrument, instrument};

use self::runner::{GlobalState, MultiIndexTaskJobMap};
use super::controller::Task;
use crate::serve::scheduler::{
    agent::AgentWorker,
    runner::{TaskJob, TaskState},
};
use ha_core::{
    activity::Activity,
    types::{HaTask, SplitJobResult},
};

#[derive(Debug, Clone)]
pub enum SchedulerNotify {
    TaskActivity(Activity),
    AgentActivity(Activity),
}
pub type NotifyChannel = tokio::sync::broadcast::Receiver<SchedulerNotify>;
pub type NotifySender = Weak<tokio::sync::broadcast::Sender<SchedulerNotify>>;
pub type SchedulerNotifier = Arc<tokio::sync::broadcast::Sender<SchedulerNotify>>;

pub trait NotifySenderExt {
    fn push_task_activity(&self, activity: Activity);
    fn push_agent_activity(&self, activity: Activity);
}

impl NotifySenderExt for NotifySender {
    fn push_task_activity(&self, activity: Activity) {
        if let Some(sender) = self.upgrade() {
            let _ = sender.send(SchedulerNotify::TaskActivity(activity));
        }
    }

    fn push_agent_activity(&self, activity: Activity) {
        if let Some(sender) = self.upgrade() {
            let _ = sender.send(SchedulerNotify::AgentActivity(activity));
        }
    }
}

pub type SchedulerTaskSender = tokio::sync::mpsc::Sender<Task>;
pub type SchedulerTaskReceiver = tokio::sync::mpsc::Receiver<Task>;

pub type TaskStopBarrier = tokio::sync::Barrier;

pub mod agent;
pub mod notify;
pub mod runner;

pub type ShutdownHandler = Box<dyn std::future::Future<Output = ()> + Send + 'static>;
#[derive(Clone)]
pub struct TaskScheduler {
    pub tasks: Arc<RwLock<MultiIndexTaskJobMap>>,
    pub global_state: Arc<GlobalState>,
    pub shutdown_handler: Arc<Mutex<Option<ShutdownHandler>>>,
    pub drop_notifier: Arc<Notify>,
    pub dropped_notifier: Arc<Notify>,
    // An Task-to-TableTagCache hashmap.
    #[allow(clippy::type_complexity)]
    pub lush_table_cache: Arc<RwLock<HashMap<(i64, i64), Arc<TableTagCache>>>>,
    // 任务的断点数据库，目前只有 PI 任务从这里获取断点数据库
    pub task_breakpoint_db: Arc<RwLock<HashMap<(i64, i64), BreakpointDb>>>,
}

impl Debug for TaskScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskScheduler")
            .field("tasks", &self.tasks.try_read().unwrap())
            .field("global", &self.global_state)
            .finish()
    }
}
impl Drop for TaskScheduler {
    fn drop(&mut self) {
        let c = Arc::strong_count(&self.shutdown_handler);
        if c == 1 {
            // this is the last reference. so we try to shutdown the scheduler.
            self.try_shutdown();
        }
    }
}

#[derive(Debug, Error)]
pub enum StopError {
    #[error("Task {0:?} is not in scheduler")]
    NotFound((i64, i64)),
    #[error("Task {0:?} already stopped")]
    AlreadyStopped((i64, i64)),
    #[error("Remove job from scheduler error: {0}")]
    RemoveJob(#[from] tokio_cron_scheduler::JobSchedulerError),
}

impl TaskScheduler {
    pub async fn new(
        owned_notify_sender: SchedulerNotifier,
        agent_worker: AgentWorker,
    ) -> Result<TaskScheduler> {
        let tasks = Arc::new(RwLock::new(MultiIndexTaskJobMap::default()));
        let lush_table_cache = Arc::new(RwLock::new(HashMap::new()));
        let task_breakpoint_db = Arc::new(RwLock::new(HashMap::new()));
        // let (notify_sender, notify_receiver) = tokio::sync::broadcast::channel(1024);
        // let owned_notify_sender = Arc::new(notify_sender);
        let notify_sender = Arc::downgrade(&owned_notify_sender);
        let mut scheduler = JobScheduler::new().await?;
        let shutdown_barrier = Arc::new(tokio::sync::Barrier::new(4));
        let shutdown_notifier = Arc::new(Notify::const_new());

        let shutdown_notifier_clone = shutdown_notifier.clone();
        let shutdown_barrier_clone = shutdown_barrier.clone();

        let notify_receiver = owned_notify_sender.subscribe();
        use tokio::sync::broadcast::error::RecvError;
        tokio::spawn(
            async move {
                tokio::pin!(notify_receiver);
                loop {
                    tokio::select! {
                        _ = shutdown_notifier_clone.notified() => {
                            break;
                        }
                        res = notify_receiver.recv() => {
                            match res {
                                Ok(act) => {
                                    tracing::info!(?act);
                                }
                                Err(RecvError::Closed) => {
                                    break;
                                }
                                Err(err) => {
                                    continue;
                                }
                            }
                        }
                    }
                }
                // This will cause recursive barrier lock.
                // shutdown_barrier_clone.wait().await;
            }
            .in_current_span(),
        );

        let notify_created = scheduler.context.notify_created_tx.subscribe();
        let shutdown_notifier_clone = shutdown_notifier.clone();
        let shutdown_barrier_clone = shutdown_barrier.clone();
        tokio::spawn(async move {
            tokio::pin!(notify_created);
            loop {
                tokio::select! {
                    _ = shutdown_notifier_clone.notified() => {
                        break;
                    }
                    res = notify_created.recv() => {
                        match res {
                            Ok(id) => {
                                tracing::info!("job is created: {:?}", id);
                            }
                            Err(err) => {
                                tracing::error!("job create error: {:?}", err);
                                break;
                            }
                        }
                    }
                }
            }
            shutdown_barrier_clone.wait().await;
        });

        let notify_deleted_rx = scheduler.context.notify_deleted_tx.subscribe();
        let shutdown_notifier_clone = shutdown_notifier.clone();
        let shutdown_barrier_clone = shutdown_barrier.clone();
        tokio::spawn(async move {
            tokio::pin!(notify_deleted_rx);
            loop {
                tokio::select! {
                    _ = shutdown_notifier_clone.notified() => {
                        break;
                    }
                    res = notify_deleted_rx.recv() => {
                        match res {
                            Ok(id) => {
                                tracing::info!("notification is deleted: {:?}", id);
                            }
                            Err(err) => {
                                tracing::error!("notify_deleted channel error: {:?}", err);
                                break;
                            }
                        }
                    }
                }
            }
            shutdown_barrier_clone.wait().await;
        });

        let job_deleted_rx = scheduler.context.job_deleted_tx.subscribe();
        let shutdown_notifier_clone = shutdown_notifier.clone();
        let shutdown_barrier_clone = shutdown_barrier.clone();

        tokio::spawn(async move {
            tokio::pin!(job_deleted_rx);
            loop {
                tokio::select! {
                    _ = shutdown_notifier_clone.notified() => {
                        break;
                    }
                    res = job_deleted_rx.recv() => {
                        match res {
                            Ok(id) => {
                                tracing::info!("job is deleted: {:?}", id);
                            }
                            Err(err) => {
                                tracing::error!("job_deleted channel error: {:?}", err);
                                break;
                            }
                        }
                    }
                }
            }
            shutdown_barrier_clone.wait().await;
        });

        let notify_rx = scheduler.context.notify_tx.subscribe();
        let task_notify_tx = Arc::downgrade(&owned_notify_sender);
        let shutdown_barrier_clone = shutdown_barrier.clone();
        let tasks_index_map = tasks.clone();

        let global_state = Arc::new(GlobalState::new(
            scheduler.clone(),
            notify_sender,
            agent_worker,
        ));
        let global_state_in_notify_handler = global_state.clone();
        let lush_table_cache_in_notify_handler = lush_table_cache.clone();
        let task_breakpoint_db_in_notify_handler = task_breakpoint_db.clone();
        tokio::spawn(
            async move {
                let global = global_state_in_notify_handler;
                let lush_table_cache = lush_table_cache_in_notify_handler;
                let task_breakpoint_db = task_breakpoint_db_in_notify_handler;
                tokio::pin!(notify_rx);
                loop {
                    match notify_rx.recv().await {
                        Ok((job_id, state)) => {
                            tracing::info!("job notify: {:?} {:?}", job_id, state);
                            notify::notify_by_job_id(
                                &tasks_index_map,
                                &global,
                                &job_id,
                                &state,
                                &lush_table_cache,
                                &task_breakpoint_db,
                            )
                            .await;
                        }
                        Err(err) => {
                            tracing::error!("job create error: {:?}", err);
                            break;
                        }
                    }
                }
                shutdown_barrier_clone.wait().await;
            }
            .in_current_span(),
        );

        let global_state_in_drop_handler = global_state.clone();
        scheduler
            .set_shutdown_handler(async move {
                tracing::info!("Shutting down scheduler");
                shutdown_notifier.notify_waiters();
                tokio::time::timeout(Duration::from_secs(5), shutdown_barrier.wait()).await;
                tracing::info!("Scheduler is shutdown completely");
                // owned_notify_sender.receiver_count();
                debug_assert!(Arc::strong_count(&owned_notify_sender) == 1);
            })
            .await;

        scheduler.start().await?;

        let drop_notifier = Arc::new(Notify::const_new());
        let dropped_notifier = Arc::new(Notify::const_new());

        tokio::spawn({
            let tasks = tasks.clone();
            let drop_notifier_cloned = drop_notifier.clone();
            let dropped_notifier_cloned = dropped_notifier.clone();
            async move {
                drop_notifier_cloned.notified().await;
                tracing::info!("scheduler is dropping, suspend all running jobs");
                // tasks.write().await.clear();
                {
                    let tasks = tasks.write().await;
                    for (_, task) in tasks.iter() {
                        task.stop().await;
                    }
                    tracing::info!(tasks.shutdown = tasks.len(), "all tasks are canceled");
                }
                if let Err(err) = global_state_in_drop_handler.go_die().await {
                    tracing::error!(
                        error.backtrace = format!("{:?}", err),
                        error.message = "Shutdown task scheduler error: {err:#}",
                        error.issuer = "global_state_in_drop_handler",
                    )
                }
                // Notify all waiters that the scheduler is dropped.
                dropped_notifier_cloned.notify_waiters();
            }
        });
        Ok(Self {
            tasks,
            global_state,
            shutdown_handler: Arc::new(Mutex::new(None)),
            drop_notifier,
            dropped_notifier,
            lush_table_cache,
            task_breakpoint_db,
        })
        // TaskScheduler { tasks: Vec::new() }
    }

    pub fn notify_channel(&self) -> NotifyChannel {
        self.global_state
            .notify_sender
            .upgrade()
            .unwrap()
            .subscribe()
    }

    pub fn notify_sender(&self) -> Option<Arc<tokio::sync::broadcast::Sender<SchedulerNotify>>> {
        self.global_state.notify_sender.upgrade()
    }

    /// Stop a task, note that this does not imply that the task is already finished.
    ///
    /// This method will remove the task from the scheduler, and the task will take a
    /// while to finish its remaining work.
    pub async fn try_stop(&self, task_job_id: (i64, i64)) -> Result<(), StopError> {
        let mut tasks = self.tasks.write().await;
        let task_job = tasks
            .get_by_task_job_id(&task_job_id)
            .ok_or(StopError::NotFound(task_job_id))?;
        let (task_id, job_id) = task_job.task_job_id;
        let sched_id = task_job.schedule_id;
        tracing::info!(task.id = task_id, job.id = job_id, sched.id = %sched_id, "task {task_job_id:?} will be removed");

        if task_job.in_final_state().await {
            return Err(StopError::AlreadyStopped(task_job_id));
        }

        let state = task_job.stop().await;

        if state.ready_to_remove_job() {
            // If job has not been ticked, remove task state handler directly.
            tasks.remove_by_task_job_id(&task_job_id);
            tracing::info!(
                task.id = task_id,
                job.id = job_id,
                sched.id = %sched_id,
                "task {task_job_id:?} is stopped"
            );
        }
        tracing::info!("Cancel task {task_job_id:?}");
        Ok(())
    }

    /// Wait until a task is stopped completely.
    #[instrument(skip_all, fields(task.id = task_job_id.0, job.id = task_job_id.1, elapsed = tracing::field::Empty))]
    pub async fn wait_task(&self, task_job_id: (i64, i64)) {
        tracing::info!("Waiting for task {task_job_id:?} to finish");
        let instant = std::time::Instant::now();
        loop {
            let tasks = self.tasks.read().await;
            let Some(task) = tasks.get_by_task_job_id(&task_job_id) else {
                break;
            };
            if task.is_finished().await {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        tracing::Span::current().record("elapsed", tracing::field::debug(instant.elapsed()));
        self.tasks.write().await.remove_by_task_job_id(&task_job_id);
        tracing::info!("task has been completely finished in scheduler");
    }

    pub async fn stop_task(
        &self,
        task_job_id: (i64, i64),
        timeout: Duration,
    ) -> anyhow::Result<()> {
        self.try_stop(task_job_id).await?;
        tokio::time::timeout(timeout, self.wait_task(task_job_id))
            .await
            .context("Stopping task timed out")?;
        Ok(())
    }

    pub async fn exists(&self, task_job_id: (i64, i64)) -> bool {
        self.tasks
            .read()
            .await
            .get_by_task_job_id(&task_job_id)
            .is_some()
    }

    pub async fn is_cancelled(&self, task_job_id: (i64, i64)) -> bool {
        if let Some(task) = self.tasks.read().await.get_by_task_job_id(&task_job_id) {
            task.task.cancellation.is_cancelled()
        } else {
            true
        }
    }

    #[instrument(skip_all, fields(task.id = task.id))]
    pub async fn push_task(
        &self,
        task: Task,
        xnoded_tx: flume::Sender<Result<RecordBatch, FlightError>>,
    ) -> anyhow::Result<()> {
        tracing::info!("Push task to scheduler: {:?}", task);
        self.global_state.ensure_alive()?;
        let task_id = task.id;
        let job_id = task.job_id;
        // 防止任务意外结束，没有正常释放断点数据库
        let _ = self.remove_task_breakpoint_db(task_id, job_id).await;
        {
            let mut tasks = self.tasks.write().await;
            if let Some(task) = tasks.get_by_task_job_id(&(task_id, job_id)) {
                if task.is_finished().await {
                    tasks.remove_by_task_job_id(&(task_id, job_id));
                } else {
                    anyhow::bail!(
                        "Task ({task_id},{job_id}) already in scheduler, please do not start it twice",
                    );
                }
            }
        }

        let task_lock_file_path = get_data_dir()
            .join("tasks")
            .join(task_id.to_string())
            .join(job_id.to_string())
            .join("running.lock");
        if let Some(dir) = task_lock_file_path.parent()
            && !dir.exists()
        {
            tokio::fs::create_dir_all(dir)
                .await
                .with_context(|| format!("create task lock dir {} error", dir.display()))?;
        }
        let file = tokio::task::spawn_blocking(move || {
            let file = Arc::new(std::fs::File::create(&task_lock_file_path).with_context(
                || {
                    format!(
                        "create job lock file {} error",
                        task_lock_file_path.display()
                    )
                },
            )?);
            match file.try_lock() {
                Ok(_) => Ok(file),
                Err(std::fs::TryLockError::WouldBlock) => {
                    anyhow::bail!("Task ({task_id},{job_id}) is still running, file locked");
                }
                Err(std::fs::TryLockError::Error(e)) => {
                    anyhow::bail!("Failed to lock task file: {}", e);
                }
            }
        })
        .await
        .context("create task lock file panic")?
        .context("create task lock file error")?;

        let task_state = TaskState::new(task, &self.global_state, xnoded_tx).await;
        let global = self.global_state.clone();

        let job = {
            let task = task_state.clone();
            let file = file.clone();
            Job::new_one_shot_async(Duration::from_secs(0), move |sid, _| {
                tracing::info!(job.id = job_id, task.id = task_id, sched.id = %sid, "job is scheduled");
                Box::pin(runner::task_job_run(
                    sid,
                    task.clone(),
                    global.clone(),
                    file.clone(),
                ))
            })?
        };

        tracing::info!(task.id = task_id, job.id = job_id, "job created");

        let job_scheduler = &self.global_state.scheduler;
        let sched_id = job_scheduler.add(job).await.with_context(|| {
            tracing::error!(task.id = task_id, "Add task `{}` error", task_id);
            format!("Add task `{}` error", task_id)
        })?;

        self.global_state
            .send_task_activity(Activity::queued(task_id, job_id, sched_id));

        let task_job_ref = TaskJob::new(
            sched_id,
            task_state,
            self.global_state.as_ref().clone(),
            file,
        );
        self.tasks.write().await.insert(task_job_ref);

        Ok(())
    }

    pub async fn stop_if_safe_to_delete(&self, task_job_id: (i64, i64)) -> bool {
        let mut guard = self.tasks.write().await;
        if let Some(task) = guard.get_by_task_job_id(&task_job_id) {
            if task.safe_to_delete().await {
                if let Err(err) = guard.try_stop(task_job_id).await {
                    tracing::error!(task.id = task_job_id.0, job.id = task_job_id.1, error = %err, "stop task error");
                }
                true
            } else {
                false
            }
        } else {
            true
        }
    }
    /// Shutdown the scheduler, this will stop and wait all tasks to be cancelled in scheduler.
    ///
    /// Side effect:
    /// 1. all tasks in scheduler will be stopped.
    /// 2. send shutdown signal to agent workers scheduler.
    /// 3. send shutdown signal to running tasks.
    pub async fn shutdown(mut self) {
        tracing::info!(
            "Shutdown scheduler, waiting for all tasks to stop, expect all tasks state be suspended"
        );
        self.try_shutdown();
        self.wait_shutdown().await;
    }

    /// Send shutdown signal but not wait for all tasks to stop.
    ///
    /// This will be called automatically when the scheduler is dropped.
    pub fn try_shutdown(&mut self) {
        self.drop_notifier.notify_waiters();
    }

    async fn wait_shutdown(self) {
        self.dropped_notifier.notified().await;
    }

    pub(crate) async fn agent_is_alive(&self, agent_id: i64) -> bool {
        self.global_state
            .agent_worker
            .agent_is_alive(agent_id)
            .await
    }

    pub(crate) async fn agent_tasks(&self, agent_id: i64) -> Vec<(i64, i64)> {
        self.global_state.agent_worker.agent_tasks(agent_id).await
    }

    pub(crate) async fn list_datasets_via_agent(
        &self,
        agent_id: i64,
        req: taosx_core::DataSetsReq,
    ) -> anyhow::Result<Vec<DataSet>> {
        self.global_state
            .agent_worker
            .list_data_sets(agent_id, req)
            .await
    }

    pub(crate) async fn query_datasource_via_agent(
        &self,
        agent_id: i64,
        req: taosx_core::QueryDataSourceReq,
    ) -> anyhow::Result<String> {
        self.global_state
            .agent_worker
            .query_data_source(agent_id, req)
            .await
    }

    pub async fn validate_dsn_via_agent(
        &self,
        agent: i64,
        dsn: Dsn,
    ) -> anyhow::Result<DataSourceValidation> {
        self.global_state
            .agent_worker
            .check(agent, dsn.to_string())
            .await
    }

    pub async fn get_sample_via_agent(&self, agent: i64, dsn: String) -> anyhow::Result<DsSamples> {
        self.global_state.agent_worker.get_sample(agent, dsn).await
    }

    pub async fn split_task_via_agent(
        &self,
        agent: i64,
        task: HaTask,
    ) -> anyhow::Result<SplitJobResult> {
        self.global_state.agent_worker.split_task(agent, task).await
    }

    pub async fn put_file_to_agent(
        &self,
        agent: i64,
        path: &str,
        content: Vec<u8>,
    ) -> anyhow::Result<()> {
        self.global_state
            .agent_worker
            .put_file_to_agent(agent, path, content)
            .await
    }

    async fn remove_task_breakpoint_db(&self, task_id: i64, job_id: i64) -> Option<BreakpointDb> {
        self.task_breakpoint_db
            .write()
            .await
            .remove(&(task_id, job_id))
    }
}
