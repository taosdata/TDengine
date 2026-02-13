use std::{collections::HashMap, sync::LazyLock};

use ha_core::{
    activity::{AgentStatus, TaskStatus},
    types::{ListTaskJobStates, ListTaskJobStatesResult},
};
use parking_lot::RwLock;
use tracing::instrument;

use crate::{
    controller::{tasks::Tasks, xnodes::XNodes},
    utils::taos_conn::TaosConn,
};

static AGENT_STATUS: LazyLock<RwLock<HashMap<i64, AgentStatus>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn get_cached_agent_state(agent_id: i64) -> Option<AgentStatus> {
    AGENT_STATUS.read().get(&agent_id).copied()
}

pub fn set_cached_agent_state(agent_id: i64, state: AgentStatus) {
    AGENT_STATUS.write().insert(agent_id, state);
}

pub fn remove_cached_agent_state(agent_id: i64) {
    AGENT_STATUS.write().remove(&agent_id);
}

#[instrument(skip_all)]
pub async fn update_agent_status(conn: &TaosConn, xnodes: &XNodes, agent_id: i64) {
    let state = xnodes.agent_status(agent_id);
    if let Some(prev_state) = get_cached_agent_state(agent_id)
        && prev_state == state
    {
        return;
    }

    let sql = format!("ALTER XNODE AGENT {agent_id} WITH STATUS '{state}'");
    match conn.exec(&sql).await {
        Ok(_) => {
            set_cached_agent_state(agent_id, state);
        }
        Err(e) => {
            tracing::error!("Failed to update agent status: {:#}", anyhow::Error::new(e));
        }
    }
}

static CACHE_TASK_STATUS: LazyLock<RwLock<HashMap<i64, TaskStatus>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static CACHE_JOB_STATUS: LazyLock<RwLock<HashMap<(i64, i64), TaskStatus>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn get_task_status(task_id: i64) -> Option<TaskStatus> {
    CACHE_TASK_STATUS.read().get(&task_id).copied()
}

pub fn set_task_status(task_id: i64, status: TaskStatus) {
    CACHE_TASK_STATUS.write().insert(task_id, status);
}

pub fn get_job_status(task_id: i64, job_id: i64) -> Option<TaskStatus> {
    CACHE_JOB_STATUS.read().get(&(task_id, job_id)).copied()
}

pub fn set_job_status(task_id: i64, job_id: i64, status: TaskStatus) {
    CACHE_JOB_STATUS.write().insert((task_id, job_id), status);
}

pub fn del_task_status(task_id: i64) {
    CACHE_TASK_STATUS.write().remove(&task_id);
    CACHE_JOB_STATUS
        .write()
        .retain(|(tid, _), _| tid != &task_id);
}

#[instrument(skip_all)]
pub async fn update_task_status(
    conn: &TaosConn,
    xnodes: &XNodes,
    tasks: &Tasks,
    tid: Option<i64>,
) -> ListTaskJobStatesResult {
    // Step 1: Collect x_states from all xnodes into a HashMap<(task_id, job_id), TaskStatus>
    let mut state_map: HashMap<(i64, i64), TaskStatus> = HashMap::new();
    let all_xnodes = xnodes.all();
    for xnode_id in all_xnodes {
        let Some(client) = xnodes.get_client(xnode_id) else {
            tracing::warn!("xnode offline or not found");
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

    // Step 2: Query database for tasks and jobs (optionally filtered by tid)
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

    let mut task_status = Vec::new();

    // Step 3: Iterate DB jobs, combine with state_map, update tasks memory and database
    for job in &db_jobs {
        let key = (job.task_id, job.id);
        let state = if let Some(s) = state_map.get(&key).copied() {
            Some(s)
        } else {
            // Not in state_map: only set Stopped if cached status is not already stopped
            let cached = get_job_status(job.task_id, job.id);
            if let Some(cached_state) = cached {
                if !cached_state.is_stopped() {
                    Some(TaskStatus::Stopped)
                } else {
                    Some(cached_state)
                }
            } else {
                job.status
            }
        };
        let Some(state) = state else {
            continue;
        };

        task_status.push(ListTaskJobStates {
            task_id: job.task_id,
            job_id: job.id,
            state,
        });

        if get_job_status(job.task_id, job.id).is_some_and(|v| v == state) {
            continue;
        }

        tasks.set_status(job.task_id, job.id, state);
        let sql = format!("ALTER XNODE JOB {} WITH STATUS '{state}'", job.id);
        if let Err(e) = conn.exec(&sql).await {
            tracing::error!(
                task_id = job.task_id,
                job_id = job.id,
                "failed to update job status: {:#}",
                anyhow::Error::new(e)
            );
        } else {
            set_job_status(job.task_id, job.id, state);
        }
    }

    // Step 4: Handle task-level entries against DB tasks
    for task in &db_tasks {
        let key = (task.id, -1);
        // Find task-level xnode state entry (job_id < 0) for this task in state_map
        let state = if let Some(s) = state_map.get(&key).copied() {
            Some(s)
        } else {
            // Not in state_map: only set Stopped if cached status is not already stopped
            let cached = get_task_status(task.id);
            if let Some(cached_state) = cached {
                if !cached_state.is_stopped() {
                    Some(TaskStatus::Stopped)
                } else {
                    Some(cached_state)
                }
            } else {
                task.status
            }
        };
        let Some(state) = state else {
            continue;
        };

        task_status.push(ListTaskJobStates {
            task_id: task.id,
            job_id: -1,
            state,
        });

        if get_task_status(task.id).is_some_and(|v| v == state) {
            continue;
        }

        tasks.set_status(task.id, -1, state);
        let sql = format!("ALTER XNODE TASK {} WITH STATUS '{state}'", task.id);
        if let Err(e) = conn.exec(&sql).await {
            tracing::error!(
                task_id = task.id,
                "failed to update task status: {:#}",
                anyhow::Error::new(e)
            );
        } else {
            set_task_status(task.id, state);
        }
    }

    // Step 5: Compute and update aggregate task status for each DB task
    for task in &db_tasks {
        if !tasks.task_has_jobs(task.id) {
            continue;
        }
        let state = if tasks.is_stopped(task.id) {
            TaskStatus::Stopped
        } else {
            TaskStatus::Running
        };
        let old_state = get_task_status(task.id);
        if old_state.is_some_and(|v| v == state) {
            continue;
        }
        let sql = format!("ALTER XNODE TASK {} WITH STATUS '{state}'", task.id);
        if let Err(e) = conn.exec(&sql).await {
            tracing::error!(
                task_id = task.id,
                "failed to update task status: {:#}",
                anyhow::Error::new(e)
            );
        } else {
            set_task_status(task.id, state);
        }
    }

    task_status
}
