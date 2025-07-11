use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use taosx_core::{
    task_set::prelude::EventLevel, utils::dsn::json_to_dsn, Activity, LevelFilter, RespAction,
    TaskNotify,
};
use taosx_task::TaskOpts;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::agent::Task;

#[allow(clippy::large_enum_variant)]
pub enum Action {
    Run(Task),
    Stop(i64),
    Cancel(i64),
    Interrupt(i64),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TaskStatus {
    id: i64,
    at: DateTime<Utc>,
    action: String,
    message: Option<String>,
    context: Option<String>,
}

pub struct Worker {
    handle: JoinHandle<Result<()>>,
    cancellation: CancellationToken,
}

impl Worker {
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished() || self.cancelled()
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

type SpawnHandle = (
    JoinHandle<Result<()>>,
    Arc<DashMap<i64, Worker>>,
    flume::Sender<Action>,
    flume::Receiver<Activity>,
);

pub fn spawn_runner(
    agent_id: i64,
    endpoint: impl Display,
    token: impl Display,
    sender: flume::Sender<RespAction>,
) -> SpawnHandle {
    let (tx, rx) = flume::bounded(1);
    let (status_tx, status_rx) = flume::bounded(10);
    let endpoint = endpoint.to_string();
    let token = token.to_string();
    let mut tasks_map = DashMap::new();
    let _ = tasks_map.try_reserve(20);
    let tasks_origin: Arc<DashMap<i64, Worker>> = Arc::new(tasks_map);
    let tasks = tasks_origin.clone();
    (
        tokio::task::spawn(async move {
            tracing::info_span!("agent_runner", id = agent_id);
            let port_pool = taosx_core::utils::port_pool::PortPool::default();

            let working_tasks = Arc::new(AtomicUsize::new(0));
            // let stop_notify = tokio::sync::Notify::new();
            // let scheduler = Arc::new()
            loop {
                if let Ok(action) = rx.recv_async().await {
                    match action {
                        Action::Run(task) => {
                            if let Some(running) = tasks.get(&task.id) {
                                if running.value().is_finished() {
                                    running.cancelled();
                                    tasks.remove(&task.id);
                                } else {
                                    // TODO: 通知runner

                                    tracing::info!("[{}] Runner has been started", running.key());
                                    continue;
                                }
                            }
                            let agent = task.via.unwrap();
                            let activity = Activity::new(
                                agent,
                                Utc::now(),
                                LevelFilter::Info,
                                format!("Start task {}", task.id),
                                "transferring",
                                json!({
                                    "agent": agent,
                                    "task": task.id,
                                }),
                            );
                            let _ = sender.send_async(RespAction::AgentActivity(activity)).await;
                            let cancellation = CancellationToken::new();
                            let cancel = cancellation.clone();

                            let (task_tx, task_rx) = flume::unbounded();

                            let opts = TaskOpts {
                                transform: vec![],
                                // from: task.from.parse().unwrap(),
                                from: json_to_dsn(&task.from).unwrap(),
                                to: task.to.parse()?,
                                health: task.health,
                                cancel,
                                parser: None,
                                // port_pool: ONCE,
                                with_agent: Some((
                                    task.id,
                                    endpoint.to_string(),
                                    token.to_string(),
                                )),
                                breakpoints: task.breakpoints,
                                task_id: Some(task.id.to_string()),
                                notify: task_tx,
                            };
                            let status_sender = status_tx.clone();
                            tokio::spawn(async move {
                                while let Ok(TaskNotify { level, message, .. }) = task_rx.recv_async().await {
                                    let activity = match level {
                                        EventLevel::Error | EventLevel::Fatal => Activity::new(
                                            task.id,
                                            Utc::now(),
                                            LevelFilter::Error,
                                            format!("Task {} error: {}", task.id, message),
                                            "running",
                                            json!({
                                                "task": task.id,
                                                "message": message,
                                            }),
                                        ),
                                        EventLevel::Warn => Activity::new(
                                            task.id,
                                            Utc::now(),
                                            LevelFilter::Warn,
                                            message,
                                            "running",
                                            json!({
                                                "task": task.id,
                                            }),
                                        ),
                                        EventLevel::Info => Activity::new(
                                            task.id,
                                            Utc::now(),
                                            LevelFilter::Info,
                                            message,
                                            "running",
                                            json!({
                                                "task": task.id,
                                            }),
                                        ),
                                    };
                                    let _ = status_sender.send_async(activity).await;
                                }
                            });
                            let pool = port_pool.clone();
                            let status_tx = status_tx.clone();
                            let sender = sender.clone();
                            let working_tasks = working_tasks.clone();
                            let tasks2 = tasks.clone();
                            let id = task.id;
                            let handle = tokio::spawn(async move {
                                let order = Ordering::Relaxed;
                                working_tasks.fetch_add(1, order);
                                let instant = Instant::now();
                                let res = opts.run(&pool).await;
                                let timing = format!("{:?}", instant.elapsed());
                                let worker = tasks2.remove(&id);
                                if worker.is_some() {
                                    working_tasks.fetch_sub(1, order);
                                }
                                if let Err(err) = res {
                                    let status = Activity::new(
                                        task.id,
                                        Utc::now(),
                                        LevelFilter::Error,
                                        format!("{err:#}"),
                                        "failed",
                                        json!({
                                            "task": task.id,
                                            "timing": timing,
                                            "backtrace": format!("{:?}", err),
                                        }),
                                        // context: Some(err.chain().join("\n")),
                                    );
                                    let _ = status_tx.send_async(status).await;

                                    let activity = Activity::new(
                                        agent,
                                        Utc::now(),
                                        LevelFilter::Error,
                                        format!("Running task {id} error: {err:#}"),
                                        "error",
                                        json!({
                                            "task": task.id,
                                            "timing": timing,
                                        }),
                                    );
                                    let _ = sender
                                        .send_async(RespAction::AgentActivity(activity))
                                        .await;

                                    // update task activity

                                    let activity = Activity::new(
                                        task.id,
                                        Utc::now(),
                                        LevelFilter::Error,
                                        format!(
                                            "Running task {id} error via agent {agent_id}: {err:#}"
                                        ),
                                        "failed",
                                        json!({
                                            "task": task.id,
                                            "timing": timing,
                                        }),
                                    );
                                    let _ =
                                        sender.send_async(RespAction::TaskActivity(activity)).await;
                                    Err(err)
                                } else {
                                    if worker.is_some() {
                                        let status = if working_tasks.load(order) > 0 {
                                            "transferring"
                                        } else {
                                            "idle"
                                        };
                                        let activity = Activity::new(
                                            agent,
                                            Utc::now(),
                                            LevelFilter::Info,
                                            format!("Task {} is completed in {}", task.id, timing),
                                            status,
                                            json!({
                                                "task": task.id,
                                                "timing": timing,
                                            }),
                                        );
                                        let _ = sender
                                            .send_async(RespAction::AgentActivity(activity))
                                            .await;

                                        // update task activity

                                        let activity = Activity::new(
                                            task.id,
                                            Utc::now(),
                                            LevelFilter::Info,
                                            format!(
                                                "Task {id} is completed via agent {agent_id} in {timing}"
                                            ),
                                            "completed",
                                            json!({
                                                "task": task.id,
                                                "timing": timing,
                                            }),
                                        );
                                        let _ = sender
                                            .send_async(RespAction::TaskActivity(activity))
                                            .await;
                                    } else {
                                        tracing::info!(
                                            "Worker {} has been already removed",
                                            task.id
                                        )
                                    }
                                    Ok(())
                                }
                            });
                            tasks.insert(
                                task.id,
                                Worker {
                                    handle,
                                    cancellation,
                                },
                            );
                        }
                        Action::Stop(id) => {
                            let order = Ordering::Relaxed;

                            if let Some((id, worker)) = tasks.remove(&id) {
                                tracing::info!(
                                    id = id,
                                    "[{id}] Remove runner for task {id}, wait for finished"
                                );
                                worker.cancel();
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                worker.handle.abort();

                                // work tasks - 1
                                working_tasks.fetch_sub(1, order);

                                // rebuild status.
                                let status = if working_tasks.load(order) > 0 {
                                    "transferring"
                                } else {
                                    "idle"
                                };

                                // update agent activity.
                                let activity = Activity::new(
                                    agent_id,
                                    Utc::now(),
                                    LevelFilter::Info,
                                    format!("Stop task {id}"),
                                    status,
                                    json!({
                                        "task": id,
                                    }),
                                );
                                let _ =
                                    sender.send_async(RespAction::AgentActivity(activity)).await;

                                // update task activity
                                let activity = Activity::new(
                                    id,
                                    Utc::now(),
                                    LevelFilter::Info,
                                    format!("Stop task via agent {agent_id}"),
                                    "stopped",
                                    json!({
                                        "via": agent_id,
                                    }),
                                );
                                let _ = sender.send_async(RespAction::TaskActivity(activity)).await;
                            } else {
                                tracing::warn!(
                                    task = id,
                                    action = "stop",
                                    "Task runner {id} not found"
                                );

                                // rebuild status.
                                let status = if working_tasks.load(order) > 0 {
                                    "transferring"
                                } else {
                                    "idle"
                                };
                                // update agent activity.
                                let activity = Activity::new(
                                    agent_id,
                                    Utc::now(),
                                    LevelFilter::Warn,
                                    format!("Trying to stop task {id}, but it has been already completed or stopped"),
                                    status,
                                    json!({
                                        "code": 0xFFFFi32,
                                        "message": format!("Task {id} not in running status"),
                                        "task": id,
                                    }),
                                );
                                let _ =
                                    sender.send_async(RespAction::AgentActivity(activity)).await;
                            }
                        }
                        Action::Cancel(id) => {
                            let order = Ordering::SeqCst;

                            if let Some((id, worker)) = tasks.remove(&id) {
                                tracing::info!(
                                    task = id,
                                    action = "cancel",
                                    "[{id}] Remove runner for task {id}, wait for task to be finished"
                                );
                                worker.cancel();
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                worker.handle.abort();

                                // work tasks - 1
                                working_tasks.fetch_sub(1, order);

                                let working_tasks_count = working_tasks.load(order);
                                // rebuild status.
                                let status = if working_tasks_count > 0 {
                                    "transferring"
                                } else {
                                    "idle"
                                };
                                tracing::info!(
                                    task = id,
                                    action = "cancel",
                                    "Task {id} finished, agent now is in {status}(runners: {working_tasks_count})",
                                );

                                // update agent activity.
                                let activity = Activity::new(
                                    agent_id,
                                    Utc::now(),
                                    LevelFilter::Info,
                                    format!("Cancel task {id}"),
                                    status,
                                    json!({
                                        "task": id,
                                        "action": "cancel"
                                    }),
                                );
                                let _ =
                                    sender.send_async(RespAction::AgentActivity(activity)).await;

                                // update task activity
                                let activity = Activity::new(
                                    id,
                                    Utc::now(),
                                    LevelFilter::Info,
                                    format!("Cancel task {id} via agent {agent_id}"),
                                    "suspended",
                                    json!({
                                        "via": agent_id,
                                        "action": "cancel"
                                    }),
                                );
                                let _ = sender.send_async(RespAction::TaskActivity(activity)).await;
                            } else {
                                tracing::warn!(
                                    task = id,
                                    action = "cancel",
                                    "Task runner {id} not found"
                                );

                                // rebuild status.
                                let status = if working_tasks.load(order) > 0 {
                                    "transferring"
                                } else {
                                    "idle"
                                };
                                // update agent activity.
                                let activity = Activity::new(
                                    agent_id,
                                    Utc::now(),
                                    LevelFilter::Warn,
                                    format!("Trying to stop task {id}, but it has been already completed or stopped"),
                                    status,
                                    json!({
                                        "code": 0xFFFFi32,
                                        "message": format!("Task {id} not in running status"),
                                        "task": id,
                                        "action": "cancel"
                                    }),
                                );
                                let _ =
                                    sender.send_async(RespAction::AgentActivity(activity)).await;
                            }
                        }
                        Action::Interrupt(id) => {
                            let order = Ordering::SeqCst;

                            if let Some((id, worker)) = tasks.remove(&id) {
                                tracing::info!(
                                    task = id,
                                    action = "interrupt",
                                    "[{id}] Remove runner for task {id}, wait for task to be finished"
                                );
                                worker.cancel();
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                worker.handle.abort();

                                // work tasks - 1
                                working_tasks.fetch_sub(1, order);
                            }
                        }
                    }
                } else {
                    tracing::info!("Task listener stopped, now cancel all running tasks...");
                    for task in tasks.iter() {
                        task.cancel()
                    }
                    break Ok(());
                }
            }
        }.in_current_span()),
        tasks_origin,
        tx,
        status_rx,
    )
}
