use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use tokio::sync::{RwLock, oneshot};
use tracing::instrument;
use uuid::Uuid;

use crate::serve::{
    controller::{AgentAction, activity::Activity},
    scheduler::runner::{AgentId, GlobalState, Operator, TaskOperator, TaskState},
};

#[derive(Debug)]
enum AgentTaskState {
    Stopped,
    Failed,
    Ticked,
    Completed,
    Suspended,
    Interrupted,
}

pub async fn spawn_agent(
    task_id: i64,
    job_id: i64,
    agent_id: AgentId,
    opts: TaskState,
    sid: Uuid,
    global: GlobalState,
    tx: oneshot::Sender<bool>,
) {
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

    tokio::select! {
        _ = cancellation.cancelled() => {
            tracing::info!(agent.id = agent_id, task.id = task_id, job.id = %sid, "task `{task_id}` cancelled");
            let operator = state.operator.operator();
            match operator {
                Operator::Suspend | Operator::Stop => {
                    global.send_task_activity(Activity::stopped(task_id, job_id));
                    state.state.write().await.stopped();
                }
                Operator::Run => {
                    unreachable!("Cancellation should be only trigger by stop or suspend operator")
                }
            }
            let _ = tx.send(true);
            tracing::warn!(agent.id = agent_id, task.id = task_id, job.id = %sid,"Task {task_id} cancelled");
            return
        }
        _ = async {
            loop {
                if global.agent_worker.agent_is_alive(agent_id).await {
                    break;
                }

                tracing::warn!("Agent {agent_id} is not alive, waiting...");
                global
                    .send_task_activity(Activity::waiting(task_id, job_id, agent_id, "Waiting for agent..."));
                if waiting < 5 {
                    waiting += 1;
                }
                tokio::time::sleep(Duration::from_secs(1) * waiting).await;
            }
        } => {}
    }

    global.send_task_activity(Activity::running(
        task_id,
        job_id,
        format!("Agent {agent_id} now alive"),
    ));
    global.send_agent_activity(Activity::agent_transferring(
        agent_id,
        format!("Task {task_id} now running"),
    ));
    tracing::debug!("Agent {} is alive, sending command run", agent_id);
    let _ = global
        .agent_worker
        .push_action(agent_id, AgentAction::Run(task_id, job_id, sid, run_id))
        .await;
    tracing::debug!("Command run sending ok");
    let waiter = state.agent_waiter.as_ref().unwrap();

    let agent_activities = waiter.agent_activities.clone();

    let mut listener = agent_activities_listener(
        state.operator.clone(),
        &global,
        &state,
        task_id,
        job_id,
        agent_id,
        sid,
        run_id,
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

                    Operator::Suspend | Operator::Stop => {
                        global.send_task_activity(Activity::stopping_timeout(
                            task_id, job_id,
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
}

#[instrument(skip_all, fields(task.id = task_id, task.jid = job_id, sched.id = %sid, task.rid = run_id, task.agent = agent_id,))]
async fn agent_activities_listener(
    operator: TaskOperator,
    global: &GlobalState,
    state: &TaskState,
    task_id: i64,
    job_id: i64,
    agent_id: AgentId,
    sid: Uuid,
    run_id: u64,
    agent_activities: Arc<RwLock<tokio::sync::mpsc::Receiver<Activity>>>,
) -> anyhow::Result<AgentTaskState> {
    let mut signal: Option<&'static str> = None;
    loop {
        let mut recv = agent_activities.write().await;
        let item = tokio::select! {
            _ = state.cancellation.cancelled() => {
                tracing::info!(agent.id = agent_id, task.id = task_id, job.id = %sid, "task runner `{task_id}` cancelled");
                match operator.operator() {
                    Operator::Suspend => {
                        global.send_task_activity(Activity::stopped(task_id, job_id));
                        state.state.write().await.stopped();
                    }
                    Operator::Stop => {
                        tracing::info!(signal, "task will be stopped after ingesting data completed");
                        global.send_task_activity(Activity::stopped(task_id, job_id));
                        state.state.write().await.stopped();
                    }
                    Operator::Run => {
                        tracing::warn!("operator is run, expect stop or suspend");
                        global.send_task_activity(Activity::stopped(task_id, job_id));
                        state.state.write().await.stopped();
                    }
                }
                tracing::warn!( agent.id = agent_id, task.id = task_id, job.id = %sid, "Task {task_id} cancelled");
                break Ok(AgentTaskState::Stopped);
            },
            item = recv.recv() => item,
        };
        let Some(mut activity) = item else {
            anyhow::bail!("All agent activities sender dropped");
        };
        tracing::warn!(activity = activity.activity, status = activity.status);
        let Some(status) = activity.status.as_ref() else {
            continue;
        };
        match status.as_str() {
            "completed" => {
                signal = Some("completed");
                continue;
            }
            "stopped" => match operator.operator() {
                Operator::Stop => {
                    signal = Some("stopped");
                    continue;
                }
                _ => {
                    tracing::warn!("Received `stopped` status but not in stopping, skip");
                }
            },
            "failed" => {
                tracing::error!("task failed: {}", activity.activity);
                global.send_task_activity(activity.clone());
                state.state.write().await.fail(activity.activity);
                break Ok(AgentTaskState::Failed);
            }
            status => {
                tracing::info!(status, message = activity.activity);
                global.send_task_activity(activity);
            }
        }
    }
}
