use std::collections::HashMap;

use ha_core::{
    activity::TaskStatus,
    types::{ListTaskJobStates, ListTaskJobStatesResult},
};
use tracing::instrument;

use taosx_utils::taos_conn::TaosConn;

use crate::controller::{agents::Agents, tasks::Tasks, xnodes::XNodes};

#[instrument(skip_all)]
pub async fn update_agent_status(conn: &TaosConn, xnodes: &XNodes, agents: &Agents, agent_id: i64) {
    let state = xnodes.agent_status(agent_id);
    if let Some(prev_state) = agents.get_cached_agent_state(agent_id)
        && prev_state == state
    {
        return;
    }

    let sql = format!("ALTER XNODE AGENT {agent_id} WITH STATUS '{state}'");
    match conn.exec(&sql).await {
        Ok(_) => {
            agents.set_cached_agent_state(agent_id, state);
        }
        Err(e) => {
            tracing::error!("Failed to update agent status: {:#}", anyhow::Error::new(e));
        }
    }
}

/// Resolve the effective status for a task/job that is absent from the live
/// `state_map`. When the owning xnode is online the job genuinely finished, so
/// we transition non-stopped states to `Stopped`. When the xnode is offline we
/// preserve the current status so the rebalancer can still migrate the task.
fn resolve_unreported_status(
    cached: Option<TaskStatus>,
    db_status: Option<TaskStatus>,
    xnode_online: bool,
) -> Option<TaskStatus> {
    if xnode_online {
        match cached {
            Some(s) if s.is_stopped() => Some(s),
            Some(_) => Some(TaskStatus::Stopped),
            None => db_status,
        }
    } else {
        cached.or(db_status)
    }
}

/// Persist a status change to the database and update the corresponding cache.
/// On failure, logs the error and leaves the cache unchanged.
#[instrument(skip_all)]
async fn persist_status(
    conn: &TaosConn,
    tasks: &Tasks,
    task_id: i64,
    job_id: i64,
    state: TaskStatus,
) {
    let sql = if job_id < 0 {
        format!("ALTER XNODE TASK {task_id} WITH STATUS '{state}'")
    } else {
        format!("ALTER XNODE JOB {job_id} WITH STATUS '{state}'")
    };
    if let Err(e) = conn.exec(&sql).await {
        tracing::error!(
            task_id,
            job_id,
            "failed to update status: {:#}",
            anyhow::Error::new(e)
        );
        return;
    }
    tracing::info!(task_id, job_id, %state, "updated status in database");
    tasks.set_cached_status(task_id, job_id, state);
}

#[instrument(skip_all)]
pub async fn update_task_status(
    conn: &TaosConn,
    xnodes: &XNodes,
    tasks: &Tasks,
    tid: Option<i64>,
) -> ListTaskJobStatesResult {
    // Step 1: Collect live states from all online xnodes.
    let state_map = collect_xnode_states(xnodes).await;

    // Step 2: Query database for tasks and jobs (optionally filtered by tid).
    let (db_tasks, db_jobs) = query_db_records(conn, tid).await;

    let mut task_status = Vec::new();

    // Step 3: Sync each job's status against the live state map.
    for job in &db_jobs {
        let state = state_map.get(&(job.task_id, job.id)).copied().or_else(|| {
            resolve_unreported_status(
                tasks.get_cached_job_status(job.task_id, job.id),
                job.status,
                xnodes.is_online(job.xnode_id),
            )
        });
        let Some(state) = state else {
            continue;
        };

        task_status.push(ListTaskJobStates {
            task_id: job.task_id,
            job_id: job.id,
            state,
        });

        if tasks
            .get_cached_job_status(job.task_id, job.id)
            .is_some_and(|v| v == state)
        {
            continue;
        }

        tasks.set_status(job.task_id, job.id, state);
        persist_status(conn, tasks, job.task_id, job.id, state).await;
    }

    // Step 4: Sync each task-level entry and compute aggregate status.
    for task in &db_tasks {
        let has_jobs = tasks.task_has_jobs(task.id);

        // For tasks with sub-jobs, skip the individual task-level state sync
        // and only use the aggregate status below. This avoids a redundant DB
        // write that would be immediately overwritten by the aggregate.
        if !has_jobs {
            let state = state_map.get(&(task.id, -1)).copied().or_else(|| {
                let xnode_online = task.xnode_id.is_some_and(|xid| xnodes.is_online(xid));
                resolve_unreported_status(
                    tasks.get_cached_task_status(task.id),
                    task.status,
                    xnode_online,
                )
            });
            if let Some(state) = state {
                task_status.push(ListTaskJobStates {
                    task_id: task.id,
                    job_id: -1,
                    state,
                });

                if !tasks
                    .get_cached_task_status(task.id)
                    .is_some_and(|v| v == state)
                {
                    tasks.set_status(task.id, -1, state);
                    persist_status(conn, tasks, task.id, -1, state).await;
                }
            }
            continue;
        }

        // Compute aggregate status across all jobs belonging to this task.
        // This must stay consistent with `event.rs::update_task` — both
        // must account for `manually_stopped` to avoid status oscillation.
        let agg_state = compute_aggregate_status(tasks, task.id);
        task_status.push(ListTaskJobStates {
            task_id: task.id,
            job_id: -1,
            state: agg_state,
        });
        if tasks
            .get_cached_task_status(task.id)
            .is_some_and(|v| v == agg_state)
        {
            continue;
        }
        persist_status(conn, tasks, task.id, -1, agg_state).await;
    }

    task_status
}

/// Compute the aggregate status for a task that has sub-jobs.
///
/// Used by both the periodic updater and the real-time event loop
/// (`event.rs::update_task`) to ensure consistent aggregate status
/// computation. Both paths must account for `manually_stopped`.
pub fn compute_aggregate_status(tasks: &Tasks, task_id: i64) -> TaskStatus {
    if tasks.is_manually_stopped(task_id) {
        if tasks.is_stopped(task_id) {
            TaskStatus::Stopped
        } else {
            TaskStatus::Stopping
        }
    } else if tasks.is_stopped(task_id) {
        TaskStatus::Stopped
    } else {
        TaskStatus::Running
    }
}

/// Collect live task/job states from all online xnodes via RPC.
///
/// Only iterates xnodes that are currently online (`availables()`), so
/// offline/drain nodes are skipped without producing noisy log messages.
async fn collect_xnode_states(xnodes: &XNodes) -> HashMap<(i64, i64), TaskStatus> {
    let mut state_map = HashMap::new();
    for xnode_id in xnodes.availables() {
        let Some(client) = xnodes.get_client(xnode_id) else {
            continue;
        };
        let x_states = match client.list_task_job_states().await {
            Ok(states) => states,
            Err(ha_rpc_client::error::Error::EventLoopDropped) => {
                xnodes.set_offline(xnode_id);
                tracing::error!(xnode_id, "rpc eventloop dropped");
                continue;
            }
            Err(e) => {
                tracing::error!(
                    xnode_id,
                    "failed to list task job states: {:#}",
                    anyhow::Error::new(e)
                );
                continue;
            }
        };
        for ListTaskJobStates {
            task_id,
            job_id,
            state,
        } in x_states
        {
            state_map.insert((task_id, job_id), state);
        }
    }
    state_map
}

/// Query the database for task and job records, optionally filtered by task id.
async fn query_db_records(
    conn: &TaosConn,
    tid: Option<i64>,
) -> (
    Vec<super::sql_types::TaskRecord>,
    Vec<super::sql_types::JobRecord>,
) {
    let tasks_sql = match tid {
        Some(tid) => format!("SHOW XNODE TASKS WHERE ID = {tid}"),
        None => "SHOW XNODE TASKS".to_string(),
    };
    let jobs_sql = match tid {
        Some(tid) => format!("SHOW XNODE JOBS WHERE TASK_ID = {tid}"),
        None => "SHOW XNODE JOBS".to_string(),
    };

    let db_tasks = match conn.query::<super::sql_types::TaskRecord>(&tasks_sql).await {
        Ok(tasks) => tasks,
        Err(e) => {
            tracing::error!("show xnode tasks error: {:#}", anyhow::Error::new(e));
            Vec::new()
        }
    };
    let db_jobs = match conn.query::<super::sql_types::JobRecord>(&jobs_sql).await {
        Ok(jobs) => jobs,
        Err(e) => {
            tracing::error!("show xnode jobs error: {:#}", anyhow::Error::new(e));
            Vec::new()
        }
    };

    (db_tasks, db_jobs)
}

#[cfg(test)]
mod tests {
    use super::*;

    use ha_core::activity::{AgentStatus, TaskStatus};

    #[test]
    fn agent_status_cache_basic_behaviour() {
        let agents = Agents::new();

        let agent_id = 42;
        agents.set_cached_agent_state(agent_id, AgentStatus::Connected);
        assert_eq!(
            agents.get_cached_agent_state(agent_id),
            Some(AgentStatus::Connected)
        );

        agents.remove_cached_agent_state(agent_id);
        assert!(agents.get_cached_agent_state(agent_id).is_none());
    }

    #[test]
    fn task_and_job_status_cache_isolated_and_cleared() {
        let tasks = Tasks::new();

        let task_id = 1_i64;
        let other_task_id = 2_i64;
        let job_id_a = 10_i64;
        let job_id_b = 11_i64;

        assert!(tasks.get_cached_task_status(task_id).is_none());
        assert!(tasks.get_cached_job_status(task_id, job_id_a).is_none());

        tasks.set_cached_task_status(task_id, TaskStatus::Running);
        tasks.set_cached_job_status(task_id, job_id_a, TaskStatus::Running);
        tasks.set_cached_job_status(task_id, job_id_b, TaskStatus::Stopped);

        tasks.set_cached_task_status(other_task_id, TaskStatus::Stopped);
        tasks.set_cached_job_status(other_task_id, job_id_a, TaskStatus::Stopped);

        assert_eq!(
            tasks.get_cached_task_status(task_id),
            Some(TaskStatus::Running)
        );
        assert_eq!(
            tasks.get_cached_job_status(task_id, job_id_a),
            Some(TaskStatus::Running)
        );
        assert_eq!(
            tasks.get_cached_job_status(task_id, job_id_b),
            Some(TaskStatus::Stopped)
        );
        assert_eq!(
            tasks.get_cached_task_status(other_task_id),
            Some(TaskStatus::Stopped)
        );
        assert_eq!(
            tasks.get_cached_job_status(other_task_id, job_id_a),
            Some(TaskStatus::Stopped)
        );

        tasks.del_cached_task_status(task_id);

        assert!(tasks.get_cached_task_status(task_id).is_none());
        assert!(tasks.get_cached_job_status(task_id, job_id_a).is_none());
        assert!(tasks.get_cached_job_status(task_id, job_id_b).is_none());

        assert_eq!(
            tasks.get_cached_task_status(other_task_id),
            Some(TaskStatus::Stopped)
        );
        assert_eq!(
            tasks.get_cached_job_status(other_task_id, job_id_a),
            Some(TaskStatus::Stopped)
        );
    }

    #[test]
    fn resolve_unreported_status_xnode_online() {
        // Running task on online xnode that didn't report -> Stopped
        assert_eq!(
            resolve_unreported_status(Some(TaskStatus::Running), None, true),
            Some(TaskStatus::Stopped)
        );

        // Already stopped task on online xnode -> keep Stopped
        assert_eq!(
            resolve_unreported_status(Some(TaskStatus::Stopped), None, true),
            Some(TaskStatus::Stopped)
        );

        // Completed task on online xnode -> keep Completed
        assert_eq!(
            resolve_unreported_status(Some(TaskStatus::Completed), None, true),
            Some(TaskStatus::Completed)
        );

        // Failed task on online xnode -> keep Failed
        assert_eq!(
            resolve_unreported_status(Some(TaskStatus::Failed), None, true),
            Some(TaskStatus::Failed)
        );

        // No cache, fall back to db_status
        assert_eq!(
            resolve_unreported_status(None, Some(TaskStatus::Running), true),
            Some(TaskStatus::Running)
        );

        // No cache, no db_status -> None
        assert_eq!(resolve_unreported_status(None, None, true), None);
    }

    #[test]
    fn resolve_unreported_status_xnode_offline() {
        // Running task on offline xnode -> preserve Running (allow rebalance)
        assert_eq!(
            resolve_unreported_status(Some(TaskStatus::Running), None, false),
            Some(TaskStatus::Running)
        );

        // Stopped task on offline xnode -> preserve Stopped
        assert_eq!(
            resolve_unreported_status(Some(TaskStatus::Stopped), None, false),
            Some(TaskStatus::Stopped)
        );

        // No cache, fall back to db_status
        assert_eq!(
            resolve_unreported_status(None, Some(TaskStatus::Running), false),
            Some(TaskStatus::Running)
        );

        // No cache, no db_status -> None
        assert_eq!(resolve_unreported_status(None, None, false), None);
    }

    #[test]
    fn compute_aggregate_status_matches_event_update_task() {
        use ha_core::types::HaTask;

        let config = HaTask {
            from: "taos://localhost:6030".to_string(),
            to: "taos://localhost:6030".to_string(),
            parser: None,
            via: None,
            labels: None,
        };

        // All jobs running, not manually stopped -> Running
        let tasks = Tasks::new();
        tasks
            .add(1, 10, 1, config.clone(), Some(TaskStatus::Running))
            .unwrap();
        tasks
            .add(1, 11, 1, config.clone(), Some(TaskStatus::Running))
            .unwrap();
        assert_eq!(compute_aggregate_status(&tasks, 1), TaskStatus::Running);

        // All jobs stopped, not manually stopped -> Stopped
        tasks.set_status(1, 10, TaskStatus::Stopped);
        tasks.set_status(1, 11, TaskStatus::Stopped);
        assert_eq!(compute_aggregate_status(&tasks, 1), TaskStatus::Stopped);

        // Manually stopped, all jobs stopped -> Stopped
        tasks.set_manually_stopped(1, 10);
        assert_eq!(compute_aggregate_status(&tasks, 1), TaskStatus::Stopped);

        // Manually stopped, some jobs still running -> Stopping
        tasks.set_status(1, 10, TaskStatus::Running);
        assert_eq!(compute_aggregate_status(&tasks, 1), TaskStatus::Stopping);
    }
}
