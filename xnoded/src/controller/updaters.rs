use std::{collections::HashMap, sync::LazyLock};

use ha_core::{
    activity::{AgentStatus, TaskStatus},
    types::{ListTaskJobStatesParam, ListTaskJobStatesResult},
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
    let mut task_status = Vec::new();
    let all_xnodes = xnodes.all();
    for xnode_id in all_xnodes {
        let Some(client) = xnodes.get_client(xnode_id) else {
            tracing::warn!("xnode offline or not found");
            continue;
        };
        let x_states = match client.list_task_job_states().await {
            Ok(states) => states,
            Err(e) => {
                tracing::error!(
                    xnode_id,
                    "failed to list task job states: {:#}",
                    anyhow::Error::new(e)
                );
                continue;
            }
        };
        task_status.extend(&x_states);
        // 设置内存状态和数据库状态
        for ListTaskJobStatesParam {
            task_id,
            job_id,
            state,
        } in x_states
        {
            if tid.is_some_and(|v| v != task_id) {
                continue;
            }
            if get_job_status(task_id, job_id).is_some_and(|v| v == state) {
                continue;
            }
            tasks.set_status(task_id, job_id, state);
            let sql = if job_id < 0 {
                format!("ALTER XNODE TASK {task_id} WITH STATUS '{state}'",)
            } else {
                format!("ALTER XNODE JOB {job_id} WITH STATUS '{state}'")
            };
            if let Err(e) = conn.exec(&sql).await {
                tracing::error!(
                    xnode_id,
                    task_id,
                    job_id,
                    "failed to update task/job status: {:#}",
                    anyhow::Error::new(e)
                );
            } else {
                set_job_status(task_id, job_id, state);
            }
        }
    }

    let tids = match tid {
        Some(tid) => vec![tid],
        None => tasks.all_tasks(),
    };
    for tid in tids {
        if !tasks.task_has_jobs(tid) {
            continue;
        }
        let state = if tasks.is_stopped(tid) {
            TaskStatus::Stopped
        } else {
            TaskStatus::Running
        };
        let old_state = get_task_status(tid);
        if old_state.is_some_and(|v| v == state) {
            continue;
        }
        let sql = format!("ALTER XNODE TASK {tid} WITH STATUS '{state}'",);
        if let Err(e) = conn.exec(&sql).await {
            tracing::error!(
                task_id = tid,
                "failed to update task status: {:#}",
                anyhow::Error::new(e)
            );
        } else {
            set_task_status(tid, state);
        }
    }

    task_status
}
