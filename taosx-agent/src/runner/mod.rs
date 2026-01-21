use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use dashmap::DashMap;
use ha_core::activity::{Activity, ActivityLevel};
use taosx_task::TaskOpts;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::agent::Task;
use taosx_core::{RespAction, TaskNotify, Via, task_set::prelude::EventLevel};
use taosx_utils::dsn::json_to_dsn;

#[allow(clippy::large_enum_variant)]
pub enum Action {
    Run(Task),
    Stop(i64, i64),
    Cancel(i64, i64),
    Exit,
}

pub struct Worker {
    cancellation: CancellationToken,
}

impl Worker {
    pub fn is_finished(&self) -> bool {
        self.cancelled()
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

type SpawnHandle = (flume::Sender<Action>, flume::Receiver<Activity>);

pub fn spawn_runner(
    agent_id: i64,
    endpoint: &str,
    token: &str,
    sender: flume::Sender<RespAction>,
    handle: &mut JoinSet<Result<()>>,
    cancel: CancellationToken,
) -> SpawnHandle {
    let (tx, rx) = flume::bounded(1);
    let (activity_tx, activity_rx) = flume::bounded(10);
    let endpoint = endpoint.to_string();
    let token = token.to_string();
    handle.spawn(process_action(
        agent_id,
        endpoint,
        activity_tx,
        sender,
        rx,
        token,
        cancel,
    ));
    (tx, activity_rx)
}

async fn process_action(
    agent_id: i64,
    endpoint: String,
    activity_tx: flume::Sender<Activity>,
    sender: flume::Sender<RespAction>,
    rx: flume::Receiver<Action>,
    token: String,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let tasks = Arc::new(DashMap::<(i64, i64), Worker>::with_capacity(20));
    tracing::info_span!("agent_runner", id = agent_id);
    let _guard = cancel.drop_guard_ref();
    let port_pool = taosx_core::utils::port_pool::PortPool::default();

    let working_tasks = Arc::new(AtomicUsize::new(0));
    let mut running_tasks = JoinSet::new();
    while let Some(Ok(action)) = cancel.run_until_cancelled(rx.recv_async()).await {
        match action {
            Action::Run(task) => {
                let (task_id, job_id) = (task.id, task.job_id);
                if let Some(running) = tasks.get(&(task_id, job_id)) {
                    if running.is_finished() {
                        running.cancelled();
                        tasks.remove(&(task_id, job_id));
                    } else {
                        tracing::info!(task_id, job_id, "Runner has been started");
                        continue;
                    }
                }
                let agent = task.via.unwrap();

                let (task_tx, task_rx) = flume::bounded(100);

                let cancellation = cancel.child_token();

                let opts = TaskOpts {
                    transform: vec![],
                    // from: task.from.parse().unwrap(),
                    from: json_to_dsn(&task.from).unwrap(),
                    to: task.to.parse()?,
                    health: task.health,
                    cancel: cancellation.clone(),
                    parser: None,
                    with_agent: Some(Via {
                        task_id,
                        job_id,
                        endpoint: endpoint.clone(),
                        token: token.clone(),
                    }),
                    breakpoints: task.breakpoints,
                    task_job_id: Some((task_id, job_id)),
                    notify: task_tx,
                };
                let status_sender = activity_tx.clone();
                tokio::spawn({
                    let cancel = cancellation.clone();
                    async move {
                        while let Some(Ok(TaskNotify { level, message, .. })) =
                            cancel.run_until_cancelled(task_rx.recv_async()).await
                        {
                            let activity = match level {
                                EventLevel::Error | EventLevel::Fatal => {
                                    Activity::error(task_id, task.job_id, message)
                                }
                                EventLevel::Warn => Activity::warn(task_id, job_id, message),
                                EventLevel::Info => Activity::info(task_id, job_id, message),
                            };
                            let _ = status_sender.send_async(activity).await;
                        }
                    }
                });
                let pool = port_pool.clone();
                let activity_tx = activity_tx.clone();
                let sender = sender.clone();
                let working_tasks = working_tasks.clone();
                let tasks2 = tasks.clone();
                let task_id = task.id;
                let job_id = task.job_id;
                let child_cancel = cancellation.clone();
                running_tasks.spawn(async move {
                    let order = Ordering::Relaxed;
                    working_tasks.fetch_add(1, order);
                    let instant = Instant::now();
                    let activity = Activity::agent_transferring(
                        agent_id,
                        format!("Start task ({task_id},{job_id})"),
                    );
                    sender
                        .send_async(RespAction::AgentActivity(activity))
                        .await
                        .ok();
                    let res = opts.run(&pool).await;
                    let timing = format!("{:?}", instant.elapsed());
                    let worker = tasks2.remove(&(task_id, job_id));
                    if worker.is_some() {
                        working_tasks.fetch_sub(1, order);
                    }
                    if let Err(err) = res {
                        let message = format!("{err:#}");
                        let status = Activity::failed(task_id, job_id, message.clone());
                        let _ = activity_tx.send_async(status).await;

                        let activity = Activity::agent_error(agent, task_id, job_id, message);
                        let _ = sender.send_async(RespAction::AgentActivity(activity)).await;

                        // update task activity
                        let activity = Activity::failed(
                            task_id,
                            job_id,
                            format!("Running task error via agent {agent_id}: {err:#}"),
                        );
                        let _ = sender.send_async(RespAction::TaskActivity(activity)).await;
                        Err(err)
                    } else {
                        if worker.is_some() {
                            let message = format!("Task {task_id} is completed in {timing}");
                            let activity = if working_tasks.load(order) > 0 {
                                Activity::agent_transferring(agent_id, message)
                            } else {
                                Activity::agent_idle(agent_id, message)
                            };
                            let _ = sender.send_async(RespAction::AgentActivity(activity)).await;

                            let activity = if child_cancel.is_cancelled() {
                                Activity::running(
                                    task_id,
                                    job_id,
                                    format!("Task is cancelled via agent {agent_id} in {timing}"),
                                )
                                .level(ActivityLevel::Warn)
                            } else {
                                Activity::completed(task_id, job_id).message(format!(
                                    "Task is completed via agent {agent_id} in {timing}"
                                ))
                            };
                            let _ = sender.send_async(RespAction::TaskActivity(activity)).await;
                        } else {
                            tracing::info!("Worker {} has been already removed", task.id)
                        }
                        Ok(())
                    }
                });
                tasks.insert((task_id, job_id), Worker { cancellation });
            }
            Action::Stop(task_id, job_id) => {
                let order = Ordering::Relaxed;
                if let Some((_, worker)) = tasks.remove(&(task_id, job_id)) {
                    tracing::info!(task_id, job_id, "remove runner for task, wait for finished");
                    worker.cancel();
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    // work tasks - 1
                    working_tasks.fetch_sub(1, order);
                    let message = format!("Stop task ({task_id},{job_id})");
                    // rebuild status.
                    let activity = if working_tasks.load(order) > 0 {
                        Activity::agent_transferring(agent_id, message)
                    } else {
                        Activity::agent_idle(agent_id, message)
                    };

                    let _ = sender.send_async(RespAction::AgentActivity(activity)).await;

                    // update task activity
                    let activity = Activity::stopped(task_id, job_id)
                        .message(format!("stop task via agent {agent_id}"));
                    let _ = sender.send_async(RespAction::TaskActivity(activity)).await;
                } else {
                    tracing::warn!(task_id, job_id, action = "stop", "Task runner not found");

                    let message = format!(
                        "Trying to stop task ({task_id},{job_id}), but it has been already completed or stopped"
                    );

                    // rebuild status.
                    let activity = if working_tasks.load(order) > 0 {
                        Activity::agent_transferring(agent_id, message).level(ActivityLevel::Warn)
                    } else {
                        Activity::agent_idle(agent_id, message).level(ActivityLevel::Warn)
                    };
                    let _ = sender.send_async(RespAction::AgentActivity(activity)).await;
                }
            }
            Action::Cancel(task_id, job_id) => {
                let order = Ordering::SeqCst;

                if let Some((_, worker)) = tasks.remove(&(task_id, job_id)) {
                    tracing::info!(
                        task_id,
                        job_id,
                        action = "cancel",
                        "remove runner for task, wait for task to be finished"
                    );
                    worker.cancel();
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    // work tasks - 1
                    working_tasks.fetch_sub(1, order);

                    let working_tasks_count = working_tasks.load(order);
                    let message = format!("Cancel task ({task_id}, {job_id})");
                    // rebuild status.
                    let activity = if working_tasks_count > 0 {
                        Activity::agent_transferring(agent_id, message)
                    } else {
                        Activity::agent_idle(agent_id, message)
                    };
                    tracing::info!(
                        task_id,
                        job_id,
                        action = "cancel",
                        "Task finished (runners: {working_tasks_count})",
                    );

                    let _ = sender.send_async(RespAction::AgentActivity(activity)).await;

                    // update task activity
                    let activity = Activity::stopped(task_id, job_id)
                        .message(format!("cancel task via agent {agent_id}"));
                    let _ = sender.send_async(RespAction::TaskActivity(activity)).await;
                } else {
                    tracing::warn!(task_id, job_id, action = "cancel", "Task runner not found");

                    // rebuild status.
                    let message = format!(
                        "Trying to stop task ({task_id}, {job_id}), but it has been already completed or stopped"
                    );
                    let activity = if working_tasks.load(order) > 0 {
                        Activity::agent_transferring(agent_id, message)
                    } else {
                        Activity::agent_idle(agent_id, message)
                    }
                    .level(ActivityLevel::Warn);

                    let _ = sender.send_async(RespAction::AgentActivity(activity)).await;
                }
            }
            Action::Exit => break,
        }
    }

    tracing::info!("Task listener stopped, now cancel all running tasks...");
    for task in tasks.iter() {
        task.cancel()
    }
    while let Some(res) = running_tasks.join_next().await {
        match res {
            Ok(Ok(())) => tracing::info!("Task completed successfully"),
            Ok(Err(e)) => tracing::error!("Task failed with error: {e:#}"),
            Err(e) => tracing::error!("Task panicked: {e:#}"),
        }
    }
    tracing::info!("All running tasks have been completed");
    Ok(())
}
