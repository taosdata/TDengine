use std::{
    fmt::Debug,
    sync::{Arc, Weak},
    time::Duration,
};

use taosx_core::DataSet;
use thiserror::Error;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio_cron_scheduler::{Job, JobScheduler};

use anyhow::{Context, Result};
use tracing::{info, instrument};

use crate::serve::scheduler::runner::{TaskJob, TaskState};

use self::runner::{AgentIntegrationChannel, GlobalState, MultiIndexTaskJobMap};

use super::controller::{Task, TaskActivity};

pub type NotifyChannel = tokio::sync::broadcast::Receiver<TaskActivity>;
pub type NotifySender = Weak<tokio::sync::broadcast::Sender<TaskActivity>>;

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
    #[error("Task {0} not found")]
    NotFound(i64),
    #[error("Remove job from scheduler error: {0}")]
    RemoveJob(#[from] tokio_cron_scheduler::JobSchedulerError),
}

impl TaskScheduler {
    pub async fn new(agent_runtime: AgentIntegrationChannel) -> Result<TaskScheduler> {
        let tasks = Arc::new(RwLock::new(MultiIndexTaskJobMap::default()));

        let (notify_sender, notify_receiver) = tokio::sync::broadcast::channel(1024);
        let owned_notify_sender = Arc::new(notify_sender);
        let notify_sender = Arc::downgrade(&owned_notify_sender);
        let mut scheduler = JobScheduler::new().await?;
        let shutdown_barrier = Arc::new(tokio::sync::Barrier::new(5));
        let shutdown_notifier = Arc::new(Notify::const_new());

        let shutdown_notifier_clone = shutdown_notifier.clone();
        let shutdown_barrier_clone = shutdown_barrier.clone();

        tokio::spawn(async move {
            tokio::pin!(notify_receiver);
            loop {
                tokio::select! {
                    _ = shutdown_notifier_clone.notified() => {
                        break;
                    }
                    res = notify_receiver.recv() => {
                        tracing::debug!("task activity: {:?}", res);
                    }
                }
            }
            shutdown_barrier_clone.wait().await;
        });

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
        tokio::spawn(async move {
            tokio::pin!(notify_rx);
            loop {
                match notify_rx.recv().await {
                    Ok((job_id, state)) => {
                        tracing::info!("job notify: {:?} {:?}", job_id, state);
                        notify::notify_by_job_id(
                            &tasks_index_map,
                            &task_notify_tx,
                            &job_id,
                            &state,
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
        });

        scheduler
            .set_shutdown_handler(async move {
                tracing::info!("Shutting down scheduler");
                shutdown_notifier.notify_waiters();
                shutdown_barrier.wait().await;
                tracing::info!("Scheduler is shutdown completely");
                // owned_notify_sender.receiver_count();
                debug_assert!(Arc::strong_count(&owned_notify_sender) == 1);
            })
            .await;

        scheduler.start().await?;
        let global_state = Arc::new(GlobalState::new(scheduler, notify_sender, agent_runtime));

        let global_state_in_drop_handler = global_state.clone();

        let tasks_cloned = tasks.clone();
        let drop_notifier = Arc::new(Notify::const_new());
        let dropped_notifier = Arc::new(Notify::const_new());

        let drop_notifier_cloned = drop_notifier.clone();
        let dropped_notifier_cloned = dropped_notifier.clone();
        tokio::spawn(async move {
            drop_notifier_cloned.notified().await;
            info!("scheduler is dropping");
            // tasks.write().await.clear();
            {
                let tasks = tasks_cloned.read().await;
                for (_, task) in tasks.iter() {
                    task.cancel().await;
                }
                info!(tasks.shutdown = tasks.len(), "all tasks are canceled");
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
            .ok_or_else(|| StopError::NotFound(task))?;
        let job_id = task_job.job_id;
        tracing::info!(task.id = task, job.id = %job_id, "task `{task}` will be removed");

        let in_running = {
            // cancel spawned task.
            let mut state = task_job.task.state.write().await;
            if state.is_scheduled() {
                state.stop().unwrap(); // must not fail
                false
            } else {
                // not scheduled
                true
            }
        };

        task_job.cancel().await;

        if !in_running {
            tasks.remove_by_task_id(&task);
            self.global_state
                .send_task_activity(TaskActivity::suspended(task, job_id));
            tracing::info!(task.id = task, job.id = %job_id, "task `{task}` is removed");
        }
        tracing::info!("Cancel task {}", task);
        Ok(())
    }

    /// Wait until a task is stopped completely.
    #[instrument(skip(self))]
    pub async fn wait_stop(&self, task: i64) {
        loop {
            let tasks = self.tasks.read().await;
            if let Some(task) = tasks.get_by_task_id(&task) {
                if !task.is_running().await {
                    break;
                }
            } else {
                // task has been removed.
                break;
            }
            info!("Waiting for task {} to stop", task);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        tracing::info!(task.id = task, "task has been completely stopped");
    }

    pub async fn push_task(&self, task: Task) -> anyhow::Result<()> {
        self.global_state.ensure_alive()?;
        let task_id = task.id;
        let task = TaskState::new(task, &self.global_state).await;
        use crate::serve::trigger::Schedule::*;

        let job = match task.schedule() {
            Cron(schedule) => {
                let task = task.clone();
                let global = self.global_state.clone();
                Job::new_cron_job_async(schedule.as_ref(), move |jid, _| {
                    Box::pin(runner::task_job_run(jid, task.clone(), global.clone()))
                })?
            }
            Oneshot => {
                let task = task.clone();
                let global = self.global_state.clone();
                Job::new_one_shot_async(Duration::from_secs(0), move |jid, _| {
                    Box::pin(runner::task_job_run(jid, task.clone(), global.clone()))
                })?
            }
            Repeated(interval) => {
                let task = task.clone();

                let global = self.global_state.clone();
                Job::new_repeated_async(*interval, move |jid, _| {
                    Box::pin(runner::task_job_run(jid, task.clone(), global.clone()))
                })?
            }
            RepeatedLimit(interval, _) => {
                let task = task.clone();
                let global = self.global_state.clone();
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
        if let Some(sender) = self.global_state.notify_sender.upgrade() {
            let _ = sender.send(TaskActivity::queued(task_id, job_id));
        }

        let task_job_ref = TaskJob::new(job_id, task, self.global_state.as_ref().clone());
        self.tasks.write().await.insert(task_job_ref);

        Ok(())
    }

    /// Shutdown the scheduler, this will stop and wait all tasks to be cancelled in scheduler.
    ///
    /// Side effect:
    /// 1. all tasks in scheduler will be stopped.
    /// 2. send shutdown signal to agent workers scheduler.
    /// 3. send shutdown signal to running tasks.
    pub async fn shutdown(mut self) {
        tracing::info!("Shutdown scheduler, waiting for all tasks to stop, expect all tasks state be suspended");
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
}

#[cfg(test)]
mod tests {
    use crate::serve::{
        controller::{NewTask, TaskController},
        rpc::AgentRpcChannel,
        scheduler::agent::{AgentNotify, AgentWorker},
    };
    use tracing_subscriber::EnvFilter;

    use super::{agent::AgentNotifySender, *};

    async fn generate_scheduler_for_test(
    ) -> anyhow::Result<(TaskController, TaskScheduler, AgentNotifySender)> {
        let (agent_activity_sender, agent_activity_receiver) =
            tokio::sync::broadcast::channel(1024);

        tokio::spawn(async move {
            tokio::pin!(agent_activity_receiver);
            loop {
                match agent_activity_receiver.recv().await {
                    Ok(act) => {
                        dbg!(act);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged)) => {
                        tracing::warn!(
                            "agent activity channel lagged: {lagged}, resubscribe it from current offset"
                        );
                        continue;
                    }
                }
            }
        });
        let (agent_notify_sender, agent_notify_receiver) = tokio::sync::broadcast::channel(1024);

        let agent_worker = AgentWorker::new(agent_activity_sender, agent_notify_receiver).await;
        let agent_runtime = AgentIntegrationChannel::Server(agent_worker);
        let mut scheduler = TaskScheduler::new(agent_runtime).await.unwrap();
        let mut notify_channel = scheduler.notify_channel();
        tracing::info!("scheduler created: {:?}", scheduler);
        let controller = TaskController::from_sqlite("sqlite::memory:", scheduler.clone()).await?;
        tracing::info!("task controller created: {:?}", scheduler);
        Ok((controller, scheduler, agent_notify_sender))
    }

    #[tokio::test()]
    async fn test_scheduler_with_default_strategy() -> Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
            .with_file(true)
            .pretty()
            .init();
        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;

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
        scheduler.wait_stop(id).await;
        scheduler.push_task(task.task.clone()).await.unwrap();
        scheduler.wait_stop(id).await;

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
    async fn test_scheduler_with_agent() -> Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
            .with_file(true)
            .pretty()
            .init();

        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;

        let agent = serde_json::from_str(
            r#"{
            "name": "fake",
            "dsn": "taos:///",
            "cluster_id": "",
            "user_id": ""
            }"#,
        )?;
        let agent = controller.create_agent(agent).await?;
        controller.set_agent_alive(agent.id).await;
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
                    TaskActivity::running(id, format!("info activity")),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    TaskActivity::error(id, format!("error activity")),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    TaskActivity::completed(id, Uuid::new_v4()),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(11)).await;
        });

        scheduler.wait_stop(id).await;

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
        // scheduler.push_task(task.task.clone()).await.unwrap();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_scheduler_with_agent_stop_task_immediately_after_enqueued() -> Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
            .with_file(true)
            .pretty()
            .init();

        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;

        let agent = serde_json::from_str(
            r#"{
            "name": "fake",
            "dsn": "taos:///",
            "cluster_id": "",
            "user_id": ""
            }"#,
        )?;
        let agent = controller.create_agent(agent).await?;
        controller.set_agent_alive(agent.id).await;
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
                    TaskActivity::running(id, format!("info activity")),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    TaskActivity::error(id, format!("error activity")),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    TaskActivity::completed(id, Uuid::new_v4()),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(11)).await;
        });

        scheduler.try_stop(id).await?;

        scheduler.wait_stop(id).await;

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
        // scheduler.push_task(task.task.clone()).await.unwrap();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_scheduler_with_agent_stop_task_while_running() -> Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
            .with_file(true)
            .pretty()
            .init();

        let (controller, mut scheduler, agent_notify_sender) =
            generate_scheduler_for_test().await?;

        let agent = serde_json::from_str(
            r#"{
            "name": "fake",
            "dsn": "taos:///",
            "cluster_id": "",
            "user_id": ""
            }"#,
        )?;
        let agent = controller.create_agent(agent).await?;
        controller.set_agent_alive(agent.id).await;
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
                    TaskActivity::running(id, format!("info activity")),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    TaskActivity::error(id, format!("error activity")),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            agent_notify_sender
                .send(AgentNotify::TaskActivity(
                    1i64,
                    TaskActivity::completed(id, Uuid::new_v4()),
                ))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(11)).await;
        });

        tokio::time::sleep(Duration::from_secs(2)).await;
        scheduler.try_stop(id).await?;

        scheduler.wait_stop(id).await;

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
        // scheduler.push_task(task.task.clone()).await.unwrap();
        Ok(())
    }
}
