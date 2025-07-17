use anyhow::{Context, Result};
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
use taosx_core::DataSet;
use taosx_core::plugins::transform::sample::DsSampleIn;
use taosx_core::sink::lush::TableTagCache;
use taosx_core::utils::{breakpoints::BreakpointDb, trace::Qid};
use taosx_core::{dsv::DataSourceValidation, plugins::transform::sample::DsSamples};
use thiserror::Error;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio_cron_scheduler::{Job, JobBuilder, JobScheduler};
use tracing::{Instrument, instrument};

use self::runner::{AgentIntegrationChannel, GlobalState, MultiIndexTaskJobMap};
use super::controller::{Activity, Task};
use crate::serve::scheduler::runner::{TaskJob, TaskState};

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
    pub lush_table_cache: Arc<RwLock<HashMap<i64, Arc<TableTagCache>>>>,
    // 任务的断点数据库，目前只有 PI 任务从这里获取断点数据库
    pub task_breakpoint_db: Arc<RwLock<HashMap<i64, BreakpointDb>>>,
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
    #[error("Task {0} is not in scheduler")]
    NotFound(i64),
    #[error("Task {0} already stopped")]
    AlreadyStopped(i64),
    #[error("Remove job from scheduler error: {0}")]
    RemoveJob(#[from] tokio_cron_scheduler::JobSchedulerError),
}

impl TaskScheduler {
    pub async fn new(
        owned_notify_sender: SchedulerNotifier,
        agent_runtime: AgentIntegrationChannel,
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
            agent_runtime,
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

        let tasks_cloned = tasks.clone();
        let drop_notifier = Arc::new(Notify::const_new());
        let dropped_notifier = Arc::new(Notify::const_new());

        let drop_notifier_cloned = drop_notifier.clone();
        let dropped_notifier_cloned = dropped_notifier.clone();
        tokio::spawn(async move {
            drop_notifier_cloned.notified().await;
            tracing::info!("scheduler is dropping, suspend all running jobs");
            // tasks.write().await.clear();
            {
                let tasks = tasks_cloned.write().await;
                for (_, task) in tasks.iter() {
                    task.suspend().await;
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

    fn notify_sender(&self) -> NotifySender {
        self.global_state.notify_sender.clone()
    }

    /// Stop a task, note that this does not imply that the task is already finished.
    ///
    /// This method will remove the task from the scheduler, and the task will take a
    /// while to finish its remaining work.
    pub async fn try_stop(&self, task: i64) -> Result<(), StopError> {
        let mut tasks = self.tasks.write().await;
        let task_job = tasks
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
            tasks.remove_by_task_id(&task);
            tracing::info!(task.id = task, job.id = %job_id, "task `{task}` is stopped");
        }
        tracing::info!("Cancel task {}", task);
        Ok(())
    }

    /// Suspend a task, note that this does not imply that the task is already finished.
    ///
    /// This method will remove the task from the scheduler, and the task will take a
    /// while to finish its remaining work.
    pub async fn try_suspend(&self, task: i64) -> Result<(), StopError> {
        let mut tasks = self.tasks.write().await;
        let task_job = tasks
            .get_by_task_id(&task)
            .ok_or(StopError::NotFound(task))?;
        let job_id = task_job.job_id;
        tracing::info!(task.id = task, job.id = %job_id, "task `{task}` will be removed");

        if task_job.in_final_state().await {
            return Err(StopError::AlreadyStopped(task));
        }

        let state = task_job.suspend().await;

        if state.ready_to_remove_job() {
            // If job has not been ticked, remove task state handler directly.
            tasks.remove_by_task_id(&task);
            tracing::info!(task.id = task, job.id = %job_id, "task `{task}` is suspended");
        }
        tracing::info!("Cancel task {}", task);
        Ok(())
    }
    /// Wait until a task is stopped completely.
    #[instrument(skip_all, fields(task.id = task, elapsed = tracing::field::Empty))]
    pub async fn wait_task(&self, task: i64) {
        tracing::info!("Waiting for task {} to finish", task);
        let instant = std::time::Instant::now();
        loop {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get_by_task_id(&task) {
                if task.is_finished().await {
                    break;
                }
            } else {
                // task has been removed.
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        tracing::Span::current().record("elapsed", tracing::field::debug(instant.elapsed()));
        self.tasks.write().await.remove_by_task_id(&task);
        tracing::info!("task has been completely finished in scheduler");
    }

    pub async fn stop_task(&self, task: i64, timeout: Duration) -> anyhow::Result<()> {
        self.try_stop(task).await?;
        tokio::time::timeout(timeout, self.wait_task(task))
            .await
            .context("Stopping task timed out")?;
        Ok(())
    }

    pub async fn exists(&self, id: i64) -> bool {
        self.tasks.read().await.get_by_task_id(&id).is_some()
    }

    pub async fn is_cancelled(&self, id: i64) -> bool {
        if let Some(task) = self.tasks.read().await.get_by_task_id(&id) {
            task.task.cancellation.is_cancelled()
        } else {
            true
        }
    }

    #[instrument(skip_all, fields(task.id = task.id))]
    pub async fn push_task(&self, task: Task) -> anyhow::Result<()> {
        tracing::info!("Push task to scheduler: {:?}", task);
        self.global_state.ensure_alive()?;
        let task_id = task.id;
        // 防止任务意外结束，没有没有正常释放断点数据库
        let _ = self.remove_task_breakpoint_db(task_id).await;
        {
            let mut tasks = self.tasks.write().await;
            if let Some(task) = tasks.get_by_task_id(&task_id) {
                if task.is_finished().await {
                    tasks.remove_by_task_id(&task_id);
                } else {
                    anyhow::bail!(
                        "Task `{}` already in scheduler, please do not start it twice",
                        task_id
                    );
                }
            }
        }
        let task = TaskState::new(task, &self.global_state).await;
        use crate::serve::trigger::Schedule::*;

        let job = match task.schedule() {
            Cron(schedule) => {
                let task = task.clone();
                let global = self.global_state.clone();
                tracing::debug!("add cron job in scheduler, cron: {}", schedule);
                Job::new_cron_job_async(schedule.as_str(), move |jid, _| {
                    tracing::debug!(job.id = %jid, task.id = task.task.id, schedule = ?task.schedule(), "Cron job is scheduled");
                    Box::pin(runner::task_job_run(jid, task.clone(), global.clone()))
                })?
            }
            Oneshot => {
                let task = task.clone();
                let global = self.global_state.clone();
                tracing::debug!("add oneshot job in scheduler");
                Job::new_one_shot_async(Duration::from_secs(0), move |jid, _| {
                    tracing::info!(job.id = %jid, task.id = task.task.id, schedule = ?task.schedule(), "Oneshot job is scheduled");
                    Box::pin(runner::task_job_run(jid, task.clone(), global.clone()))
                })?
            }
            Repeated(interval) => {
                let task = task.clone();
                let global = self.global_state.clone();
                tracing::debug!("add repeated job in scheduler, interval: {:?}", interval);
                Job::new_repeated_async(*interval, move |jid, _| {
                    tracing::info!(job.id = %jid, task.id = task.task.id, schedule = ?task.schedule(), "Repeated job is scheduled");
                    Box::pin(runner::task_job_run(jid, task.clone(), global.clone()))
                })?
            }
            RepeatedWithStartAt(interval, start_at) => {
                let task = task.clone();
                let global = self.global_state.clone();
                tracing::debug!(
                    "add repeated job in scheduler, interval: {:?}, start_at: {:?}",
                    interval,
                    start_at
                );
                JobBuilder::new()
                    .with_timezone(chrono::Utc)
                    .with_repeated_job_type()
                    .every_seconds(interval.as_secs())
                    .start_at(*start_at)
                    .with_run_async(Box::new(move |jid, _| {
                        Box::pin(runner::task_job_run(jid, task.clone(), global.clone()))
                    }))
                    .build()?
            }
            RepeatedLimit(interval, _) => {
                let task = task.clone();
                let global = self.global_state.clone();
                tracing::debug!(
                    "add repeated limit job in scheduler, interval: {:?}",
                    interval
                );
                Job::new_repeated_async(*interval, move |jid, _| {
                    Box::pin(runner::task_job_run(jid, task.clone(), global.clone()))
                })?
            }
        };

        tracing::info!(task.id = task_id, job.id = %job.guid(), "job created");

        let job_scheduler = &self.global_state.scheduler;
        let job_id = job_scheduler.add(job).await.with_context(|| {
            tracing::error!(task.id = task_id, "Add task `{}` error", task_id);
            format!("Add task `{}` error", task_id)
        })?;

        self.global_state
            .send_task_activity(Activity::queued(task_id, job_id));

        let task_job_ref = TaskJob::new(job_id, task, self.global_state.as_ref().clone());
        self.tasks.write().await.insert(task_job_ref);

        Ok(())
    }

    pub async fn stop_if_safe_to_delete(&self, task_id: i64) -> bool {
        let mut guard = self.tasks.write().await;
        if let Some(task) = guard.get_by_task_id(&task_id) {
            if task.safe_to_delete().await {
                if let Err(err) = guard.try_stop(task_id).await {
                    tracing::error!(task.id = task_id, error = %err, "stop task error");
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
            .agent_runtime
            .agent_is_alive(agent_id)
            .await
    }

    pub(crate) async fn list_datasets_via_agent(
        &self,
        agent_id: i64,
        req: taosx_core::DataSetsReq,
    ) -> anyhow::Result<Vec<DataSet>> {
        self.global_state
            .agent_runtime
            .list_data_sets(agent_id, req)
            .await
    }

    pub(crate) async fn query_datasource_via_agent(
        &self,
        agent_id: i64,
        req: taosx_core::QueryDataSourceReq,
    ) -> anyhow::Result<String> {
        self.global_state
            .agent_runtime
            .query_data_source(agent_id, req)
            .await
    }

    pub async fn validate_dsn_via_agent(
        &self,
        agent: i64,
        dsn: Dsn,
    ) -> anyhow::Result<DataSourceValidation> {
        self.global_state
            .agent_runtime
            .check(agent, dsn.to_string())
            .await
    }

    pub async fn get_sample_via_agent(&self, agent: i64, dsn: String) -> anyhow::Result<DsSamples> {
        self.global_state.agent_runtime.get_sample(agent, dsn).await
    }

    pub async fn put_file_to_agent(
        &self,
        agent: i64,
        path: &str,
        content: Vec<u8>,
    ) -> anyhow::Result<()> {
        self.global_state
            .agent_runtime
            .put_file_to_agent(agent, path, content)
            .await
    }

    pub(crate) async fn suspend_all(&self) {
        let tasks = self
            .tasks
            .read()
            .await
            .iter_by_task_id()
            .map(|task| task.task_id)
            .collect_vec();

        for task in &tasks {
            if let Err(err) = self.try_suspend(*task).await {
                tracing::error!(task.id = task, error = %err, "suspend task error");
            }
        }
        for task in tasks {
            self.wait_task(task).await;
        }
    }

    async fn remove_task_breakpoint_db(&self, task_id: i64) -> Option<BreakpointDb> {
        self.task_breakpoint_db.write().await.remove(&task_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::serve::{
        controller::{NewTask, Status, TaskController, agent::AgentActivityFilter},
        rpc::AgentRpcChannel,
        scheduler::agent::{AgentNotify, AgentWorker},
        tests::{tracing_subscriber_init, wait_notify_channel},
    };
    use itertools::Itertools;
    use tracing_subscriber::EnvFilter;
    use uuid::Uuid;

    use super::super::tests::generate_scheduler_for_test;
    use super::{agent::AgentNotifySender, *};

    #[tokio::test()]
    #[ignore]
    async fn schedule_without_agent() -> Result<()> {
        tracing_subscriber_init()?;
        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;
        let mut notify_channel = scheduler.notify_channel();

        tracing::info!("task controller created: {:?}", scheduler);

        {
            // 1. Fake task for completed

            let new: NewTask = serde_json::from_str(
                r#"{
            "from": "fake+stable:///?sleep=2s",
            "to": "taos:///fake",
            "not_start": true
            }"#,
            )
            .unwrap();
            let task = controller.create(new).await?;
            tracing::info!("push task: {:?}", task);

            let id = task.id;
            scheduler.push_task(task.task.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;

            let task = controller.get(id).await.unwrap().unwrap();
            dbg!(&task);
            assert_eq!(task.status(), Status::Completed);
        }

        {
            // 2. Fake task for failed

            let new: NewTask = serde_json::from_str(
                r#"{
            "from": "fake+stable:///?sleep=2s&bail=some error",
            "to": "taos:///fake",
            "not_start": true
            }"#,
            )
            .unwrap();
            let task = controller.create(new).await?;
            tracing::info!("push task: {:?}", task);

            let id = task.id;
            scheduler.push_task(task.task.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;

            let task = controller.get(id).await.unwrap().unwrap();
            dbg!(&task);
            assert_eq!(task.status(), Status::Failed);
        }

        {
            // 3. Fake task for stopped immediately after enqueued

            let new: NewTask = serde_json::from_str(
                r#"{
            "from": "fake+stable:///?sleep=20s&bail=some error",
            "to": "taos:///fake",
            "not_start": true
            }"#,
            )
            .unwrap();
            let task = controller.create(new).await?;
            tracing::info!("push task: {:?}", task);

            let id = task.id;
            scheduler.push_task(task.task.clone()).await.unwrap();
            if let Err(err) = scheduler.try_stop(id).await {
                tracing::error!("stop task error: {:?}", err);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;

            let task = controller.get(id).await.unwrap().unwrap();
            dbg!(&task);
            let activities = controller
                .task_activities(id, &AgentActivityFilter::default())
                .await?;
            tracing::info!(task.id = id, ?activities);
            assert_eq!(task.status(), Status::Stopped);
        }

        {
            // 4. Fake task for stopped after running.

            let new: NewTask = serde_json::from_str(
                r#"{
            "from": "fake+stable:///?sleep=20s&bail=some error",
            "to": "taos:///fake",
            "not_start": true
            }"#,
            )
            .unwrap();
            let task = controller.create(new).await?;
            tracing::info!("push task: {:?}", task);

            let id = task.id;
            scheduler.push_task(task.task.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
            if let Err(err) = scheduler.try_stop(id).await {
                tracing::error!("stop task error: {:?}", err);
            }
            tokio::time::sleep(Duration::from_secs(3)).await;

            let task = controller.get(id).await.unwrap().unwrap();
            dbg!(&task);
            let activities = controller
                .task_activities(id, &AgentActivityFilter::default())
                .await?;
            tracing::info!(task.id = id, ?activities);
            assert_eq!(task.status(), Status::Stopped);

            let status = activities
                .iter()
                .rev()
                .map(|act| act.status.as_str())
                .collect_vec();
            assert_eq!(
                status,
                vec!["created", "queued", "running", "stopping", "stopped"]
            );

            scheduler.push_task(task.task.clone()).await.unwrap();
            if let Err(err) = scheduler.try_stop(id).await {
                tracing::error!("stop task error: {:?}", err);
            }
            scheduler.wait_task(id).await;

            tokio::time::sleep(Duration::from_secs(3)).await;

            let task = controller.get(id).await.unwrap().unwrap();
            dbg!(&task);
            let activities = controller
                .task_activities(id, &AgentActivityFilter::default())
                .await?;
            tracing::info!(task.id = id, ?activities);
            assert_eq!(task.status(), Status::Stopped);
        }
        scheduler.shutdown().await;
        wait_notify_channel(notify_channel).await;
        Ok(())
    }
    #[tokio::test()]
    #[ignore]
    async fn test_scheduler_with_default_strategy() -> Result<()> {
        let _ = tracing_subscriber_init();
        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;
        let mut notify_channel = scheduler.notify_channel();

        tracing::info!("task controller created: {:?}", scheduler);

        let new: NewTask = serde_json::from_str(
            r#"{
            "from": "fake+stable:///?sleep=7s",
            "to": "taos:///fake",
            "not_start": true
            }"#,
        )
        .unwrap();
        let task = controller.create(new).await?;
        tracing::info!("push task: {:?}", task);

        let id = task.id;
        scheduler.push_task(task.task.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
        scheduler.try_stop(id).await.unwrap();
        scheduler.wait_task(id).await;
        scheduler.push_task(task.task.clone()).await.unwrap();
        scheduler.wait_task(id).await;

        // tokio::time::sleep(Duration::from_secs(10)).await;
        dbg!(&scheduler);
        scheduler.shutdown().await;
        // drop(scheduler);

        notify_channel.len();

        loop {
            match notify_channel.recv().await {
                Ok(act) => {
                    dbg!(act);
                }
                Err(err) => {
                    dbg!(&err);
                    match err {
                        tokio::sync::broadcast::error::RecvError::Closed => {
                            tracing::info!("notify channel closed");
                            break;
                        }
                        tokio::sync::broadcast::error::RecvError::Lagged(lagged) => {
                            tracing::warn!(
                                "notify channel lagged: {lagged}, resubscribe it from current offset"
                            );
                            notify_channel.resubscribe();
                            continue;
                        }
                    }
                    break;
                }
            }
        }
        // scheduler.push_task(task.task.clone()).await.unwrap();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[ignore]
    async fn test_scheduler_with_agent() -> Result<()> {
        let _ = tracing_subscriber_init();
        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;
        let mut notify_channel = scheduler.notify_channel();

        let agent = serde_json::from_str(
            r#"{
            "name": "fake",
            "dsn": "taos:///",
            "cluster_id": "",
            "user_id": ""
            }"#,
        )?;
        let agent = controller.create_agent(agent).await?;
        agent_notify_sender.send(AgentNotify::AgentConnected(agent.id));
        let new: NewTask = serde_json::from_str(
            r#"{
            "from": "fake+stable:///?sleep=7s",
            "to": "taos:///fake",
            "via": 1,
            "not_start": true
            }"#,
        )
        .unwrap();
        let task = controller.create(new).await?;

        tracing::info!("push task: {:?}", task);

        let id = task.id;
        scheduler.push_task(task.task.clone()).await.unwrap();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::AgentConnected(1))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    Activity::running(id, "info activity".to_string()),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    Activity::error(id, "error activity".to_string()),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    Activity::completed(id, Uuid::new_v4()),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(11)).await;
        });

        scheduler.wait_task(id).await;

        // tokio::time::sleep(Duration::from_secs(10)).await;
        dbg!(&scheduler);
        scheduler.shutdown().await;
        // drop(scheduler);

        notify_channel.len();

        loop {
            match notify_channel.recv().await {
                Ok(act) => {
                    dbg!(act);
                }
                Err(err) => {
                    dbg!(&err);
                    match err {
                        tokio::sync::broadcast::error::RecvError::Closed => {
                            tracing::info!("notify channel closed");
                            break;
                        }
                        tokio::sync::broadcast::error::RecvError::Lagged(lagged) => {
                            tracing::warn!(
                                "notify channel lagged: {lagged}, resubscribe it from current offset"
                            );
                            notify_channel.resubscribe();
                            continue;
                        }
                    }
                }
            }
        }
        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        let activities = controller
            .task_activities(id, &AgentActivityFilter::default())
            .await?;
        tracing::info!(task.id = id, ?activities);
        assert_eq!(task.status(), Status::Completed);

        let status = activities
            .iter()
            .rev()
            .map(|act| act.status.as_str())
            .unique()
            .collect_vec();
        assert_eq!(status, vec!["created", "queued", "running", "completed"]);
        // scheduler.push_task(task.task.clone()).await.unwrap();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_scheduler_with_agent_stop_task_immediately_after_enqueued() -> Result<()> {
        let _ = tracing_subscriber_init();
        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;

        let mut notify_channel = scheduler.notify_channel();

        let agent = serde_json::from_str(
            r#"{
            "name": "fake",
            "dsn": "taos:///",
            "cluster_id": "",
            "user_id": ""
            }"#,
        )?;
        let agent = controller.create_agent(agent).await?;
        agent_notify_sender.send(AgentNotify::AgentConnected(agent.id));
        let new: NewTask = serde_json::from_str(
            r#"{
            "from": "fake+stable:///?sleep=7s",
            "to": "taos:///fake",
            "via": 1,
            "not_start": true,
            "trigger": {"interval": "1s"}
            }"#,
        )
        .unwrap();
        let task = controller.create(new).await?;

        tracing::info!("push task: {:?}", task);

        let id = task.id;
        scheduler.push_task(task.task.clone()).await.unwrap();

        scheduler.try_stop(id).await?;

        scheduler.wait_task(id).await;

        // tokio::time::sleep(Duration::from_secs(10)).await;
        dbg!(&scheduler);
        scheduler.shutdown().await;
        // drop(scheduler);

        notify_channel.len();

        loop {
            match notify_channel.recv().await {
                Ok(act) => {
                    dbg!(act);
                }
                Err(err) => {
                    dbg!(&err);
                    match err {
                        tokio::sync::broadcast::error::RecvError::Closed => {
                            tracing::info!("notify channel closed");
                            break;
                        }
                        tokio::sync::broadcast::error::RecvError::Lagged(lagged) => {
                            tracing::warn!(
                                "notify channel lagged: {lagged}, resubscribe it from current offset"
                            );
                            notify_channel.resubscribe();
                            continue;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;

        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        assert_eq!(task.status(), Status::Stopped);
        // scheduler.push_task(task.task.clone()).await.unwrap();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[ignore]
    async fn test_scheduler_with_agent_stop_task_while_running() -> Result<()> {
        let _ = tracing_subscriber_init();

        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;
        let mut notify_channel = scheduler.notify_channel();

        let agent = serde_json::from_str(
            r#"{
            "name": "fake",
            "dsn": "taos:///",
            "cluster_id": "",
            "user_id": ""
            }"#,
        )?;
        let agent = controller.create_agent(agent).await?;
        agent_notify_sender.send(AgentNotify::AgentConnected(agent.id));
        let new: NewTask = serde_json::from_str(
            r#"{
            "from": "fake+stable:///?sleep=7s",
            "to": "taos:///fake",
            "via": 1,
            "not_start": true,
            "trigger": {"interval": "1s"}
            }"#,
        )
        .unwrap();
        let task = controller.create(new).await?;

        tracing::info!("push task: {:?}", task);

        let id = task.id;
        scheduler.push_task(task.task.clone()).await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;
        scheduler.try_stop(id).await?;

        scheduler.wait_task(id).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        assert_eq!(task.status(), Status::Stopped);

        tokio::time::sleep(Duration::from_secs(2)).await;

        scheduler.push_task(task.task.clone()).await.unwrap();
        scheduler.try_stop(id).await?;

        scheduler.wait_task(id).await;

        tokio::time::sleep(Duration::from_secs(2)).await;
        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        assert_eq!(task.status(), Status::Stopped);

        // tokio::time::sleep(Duration::from_secs(10)).await;
        dbg!(&scheduler);
        scheduler.shutdown().await;
        // drop(scheduler);

        notify_channel.len();

        loop {
            match notify_channel.recv().await {
                Ok(act) => {
                    dbg!(act);
                }
                Err(err) => {
                    dbg!(&err);
                    match err {
                        tokio::sync::broadcast::error::RecvError::Closed => {
                            tracing::info!("notify channel closed");
                            break;
                        }
                        tokio::sync::broadcast::error::RecvError::Lagged(lagged) => {
                            tracing::warn!(
                                "notify channel lagged: {lagged}, resubscribe it from current offset"
                            );
                            notify_channel.resubscribe();
                            continue;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        assert_eq!(task.status(), Status::Stopped);

        tokio::time::sleep(Duration::from_secs(5)).await;
        // scheduler.push_task(task.task.clone()).await.unwrap();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[ignore]
    async fn test_scheduler_with_agent_shutdown_task_while_running() -> Result<()> {
        tracing_subscriber_init()?;

        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;
        let mut notify_channel = scheduler.notify_channel();

        let agent = serde_json::from_str(
            r#"{
            "name": "fake",
            "dsn": "taos:///",
            "cluster_id": "",
            "user_id": ""
            }"#,
        )?;
        let agent = controller.create_agent(agent).await?;
        agent_notify_sender.send(AgentNotify::AgentConnected(agent.id));
        let new: NewTask = serde_json::from_str(
            r#"{
            "from": "fake+stable:///?sleep=7s",
            "to": "taos:///fake",
            "via": 1,
            "not_start": true,
            "trigger": {"interval": "1s"}
            }"#,
        )
        .unwrap();
        let task = controller.create(new).await?;

        tracing::info!("push task: {:?}", task);

        let id = task.id;
        scheduler.push_task(task.task.clone()).await.unwrap();
        scheduler.try_suspend(id).await?;

        let agent_notify_sender_cloned = agent_notify_sender.clone();
        scheduler.wait_task(id).await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        assert_eq!(task.status(), Status::Suspended);

        controller.start(id).await?;

        // tokio::spawn(async move {
        //     tokio::time::sleep(Duration::from_secs(1)).await;
        //     agent_notify_sender
        //         .send(AgentNotify::TaskActivity(
        //             id as _,
        //             TaskActivity::running(id, format!("info activity")),
        //         ))
        //         .unwrap();
        //     tokio::time::sleep(Duration::from_secs(4)).await;
        //     agent_notify_sender
        //         .send(AgentNotify::TaskActivity(
        //             id as _,
        //             TaskActivity::suspended(id, Uuid::nil()),
        //         ))
        //         .unwrap();
        // });

        // Wait for task in scheduler ticking.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        // Currently, the task is running.
        assert_eq!(task.status(), Status::Running);
        // Then we can suspend it.
        scheduler.try_suspend(id).await?;

        // Wait for suspending in agent.
        scheduler.wait_task(id).await;

        tracing::warn!("task suspended");

        // Wait for controller to update task status (suspended).
        tokio::time::sleep(Duration::from_secs(2)).await;
        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        assert_eq!(task.status(), Status::Suspended);

        // run it agent
        controller.start(id).await?;
        // wait for task in scheduler ticking.
        tokio::time::sleep(Duration::from_secs(2)).await;
        // shutdown the scheduler.
        scheduler.shutdown().await;
        // drop(scheduler);

        dbg!(notify_channel.len());

        loop {
            match notify_channel.recv().await {
                Ok(act) => {
                    dbg!(act);
                }
                Err(err) => {
                    dbg!(&err);
                    match err {
                        tokio::sync::broadcast::error::RecvError::Closed => {
                            tracing::info!("notify channel closed");
                            break;
                        }
                        tokio::sync::broadcast::error::RecvError::Lagged(lagged) => {
                            tracing::warn!(
                                "notify channel lagged: {lagged}, resubscribe it from current offset"
                            );
                            notify_channel.resubscribe();
                            continue;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        let task = controller.get(id).await.unwrap().unwrap();
        dbg!(&task);
        assert_eq!(task.status(), Status::Suspended);

        tokio::time::sleep(Duration::from_secs(5)).await;
        // scheduler.push_task(task.task.clone()).await.unwrap();
        Ok(())
    }
}
