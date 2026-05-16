use std::{collections::HashMap, sync::Arc};

use ha_core::types::{HaTask, XnodedId};
use snafu::ResultExt;
use taos::Dsn;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use taosx_utils::taos_conn::TaosConn;

use crate::controller::{
    BuildTaosConnSnafu, Result, agents::Agents, sql_types, start_job, start_task, tasks::Tasks,
    xnodes::XNodes,
};

use super::rpc_transport;

#[instrument(skip_all, fields(xnode_id=id))]
pub async fn reconnect_loop(
    id: i32,
    xnoded_id: XnodedId,
    meta: rpc_transport::RpcTransportMeta,
    endpoint: rpc_transport::BuiltRpcEndpoint,
    xnodes: XNodes,
    agents: Agents,
    tasks: Tasks,
    taos_dsn: Dsn,
    reconnect_rx: flume::Receiver<oneshot::Sender<bool>>,
    cancel: CancellationToken,
) -> Result<()> {
    let _cleanup = taosx_utils::defer::defer(|| {
        xnodes.set_offline(id);
        tracing::info!(xnode_id = id, "reconnect loop exited");
    });
    let _guard = cancel.drop_guard_ref();

    let taos_conn = Arc::new(
        TaosConn::create(&taos_dsn, 3)
            .await
            .context(BuildTaosConnSnafu)?,
    );
    while let Some(Ok(tx)) = cancel.run_until_cancelled(reconnect_rx.recv_async()).await {
        if !xnodes.is_offline(id) {
            tx.send(true).ok();
            continue;
        }
        // Retry the RPC connection when the xnode is offline.
        let res = reconnect(
            &meta,
            &endpoint,
            id,
            &xnoded_id,
            &xnodes,
            cancel.child_token(),
        )
        .await;
        tx.send(res).ok();

        // After a successful reconnect, re-send agent state.
        resend_agents(id, &xnodes, &agents).await;

        // After a successful reconnect, re-send task state.
        restart_task_job(id, &xnodes, &tasks, &taos_conn, cancel.child_token()).await;
    }
    Ok(())
}

#[instrument(skip_all)]
async fn reconnect(
    meta: &rpc_transport::RpcTransportMeta,
    endpoint: &rpc_transport::BuiltRpcEndpoint,
    xnode_id: i32,
    xnoded_id: &XnodedId,
    xnodes: &XNodes,
    cancel: CancellationToken,
) -> bool {
    let channel = match endpoint.clone().connect().await {
        Ok(channel) => channel,
        Err(err) => {
            let category = rpc_transport::classify_connect_error(&err);
            if let Some(hint) = rpc_transport::possible_scheme_mismatch_hint(meta.transport, &err) {
                tracing::error!(
                    endpoint = %meta.endpoint,
                    transport = meta.transport,
                    verify_mode = %meta.verify_mode,
                    category,
                    possible_cause = hint,
                    "failed to connect xnode transport: {err:#}"
                );
            } else {
                tracing::error!(
                    endpoint = %meta.endpoint,
                    transport = meta.transport,
                    verify_mode = %meta.verify_mode,
                    category,
                    "failed to connect xnode transport: {err:#}"
                );
            }
            return false;
        }
    };
    let (new_event_tx, new_event_rx) = flume::bounded(1000);
    let rpc_client_cancel = cancel.child_token();
    let client = match ha_rpc_client::create_client(
        channel,
        xnoded_id,
        new_event_tx,
        rpc_client_cancel.clone(),
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(endpoint = %meta.endpoint, "create rpc client error: {:#}", anyhow::Error::new(e));
            return false;
        }
    };
    xnodes.set_online(xnode_id, client.clone(), new_event_rx, rpc_client_cancel);
    tracing::info!(xnode_id, "xnode connected");
    true
}

#[instrument(skip_all)]
async fn resend_agents(xnode_id: i32, xnodes: &XNodes, agents: &Agents) {
    let Some(client) = xnodes.get_client(xnode_id) else {
        return;
    };
    let tokens = agents.all_tokens();
    if let Err(e) = client.add_agents(&tokens).await {
        tracing::error!(
            xnode_id,
            "add agent tokens error: {:#}",
            anyhow::Error::new(e)
        );
    }
}

#[instrument(skip_all)]
async fn restart_task_job(
    xnode_id: i32,
    xnodes: &XNodes,
    tasks: &Tasks,
    conn: &Arc<TaosConn>,
    cancel: CancellationToken,
) {
    if xnodes.is_offline(xnode_id) {
        return;
    }

    restart_tasks(xnode_id, xnodes, tasks, conn, cancel.clone()).await;
    restart_jobs(xnode_id, xnodes, tasks, conn, cancel).await
}

fn task_record_config(task: &sql_types::TaskRecord) -> Option<HaTask> {
    let parser = match task
        .parser
        .as_ref()
        .map(|value| serde_json::from_str(value))
        .transpose()
    {
        Ok(parser) => parser,
        Err(e) => {
            tracing::error!(task_id = task.id, "parse task parser error: {e}");
            return None;
        }
    };
    let labels = match task
        .labels
        .as_ref()
        .map(|value| serde_json::from_str(value))
        .transpose()
    {
        Ok(labels) => labels,
        Err(e) => {
            tracing::error!(task_id = task.id, "parse task labels error: {e}");
            return None;
        }
    };
    Some(HaTask {
        name: task.name.clone(),
        from: task.from.clone(),
        to: task.to.clone(),
        parser,
        via: task.via,
        labels,
    })
}

fn collect_restartable_task_configs(
    xnode_id: i32,
    db_tasks: &[sql_types::TaskRecord],
    tasks: &Tasks,
) -> Vec<(i64, HaTask)> {
    let mut candidates = HashMap::new();

    for task in db_tasks {
        if task.status.is_none_or(|status| !status.is_running()) {
            continue;
        }
        if task.xnode_id != Some(xnode_id) {
            continue;
        }
        let Some(config) = task_record_config(task) else {
            continue;
        };
        candidates.insert(task.id, config);
    }

    for (task_id, job_id) in tasks.xnode_jobs(xnode_id) {
        if job_id >= 0 {
            continue;
        }
        let Some(info) = tasks.job(task_id, job_id) else {
            continue;
        };
        if info.manually_stopped || info.status.is_none_or(|status| !status.is_running()) {
            continue;
        }
        candidates.entry(task_id).or_insert(info.config);
    }

    candidates.into_iter().collect()
}

fn collect_restartable_job_configs(
    xnode_id: i32,
    db_jobs: &[sql_types::JobRecord],
    tasks: &Tasks,
) -> Vec<((i64, i64), HaTask)> {
    let mut candidates = HashMap::new();

    for job in db_jobs {
        if job.status.is_none_or(|status| !status.is_running()) {
            continue;
        }
        if job.xnode_id != xnode_id {
            continue;
        }
        let config: HaTask = match serde_json::from_str(&job.config) {
            Ok(config) => config,
            Err(e) => {
                tracing::error!(
                    task_id = job.task_id,
                    job_id = job.id,
                    "parse job config error: {:#}",
                    anyhow::Error::new(e)
                );
                continue;
            }
        };
        candidates.insert((job.task_id, job.id), config);
    }

    for (task_id, job_id) in tasks.xnode_jobs(xnode_id) {
        if job_id < 0 {
            continue;
        }
        let Some(info) = tasks.job(task_id, job_id) else {
            continue;
        };
        if info.manually_stopped || info.status.is_none_or(|status| !status.is_running()) {
            continue;
        }
        candidates.entry((task_id, job_id)).or_insert(info.config);
    }

    candidates.into_iter().collect()
}

#[instrument(skip_all)]
async fn restart_tasks(
    xnode_id: i32,
    xnodes: &XNodes,
    tasks: &Tasks,
    conn: &Arc<TaosConn>,
    cancel: CancellationToken,
) {
    let sql = format!("SHOW XNODE TASKS WHERE XNODE_ID = {xnode_id}");
    let db_tasks = match conn.query::<sql_types::TaskRecord>(&sql).await {
        Ok(tasks) => tasks,
        Err(e) => {
            tracing::error!("show xnode tasks error: {:#}", anyhow::Error::new(e));
            return;
        }
    };

    for (task_id, config) in collect_restartable_task_configs(xnode_id, &db_tasks, tasks) {
        tokio::spawn({
            let xnodes = xnodes.clone();
            let tasks = tasks.clone();
            let conn = conn.clone();
            let cancel = cancel.clone();
            async move {
                let Some(res) = cancel
                    .run_until_cancelled(start_task(
                        xnode_id, task_id, &xnodes, &tasks, &conn, config, false,
                    ))
                    .await
                else {
                    return;
                };
                if let Err(e) = res {
                    tracing::error!(
                        task_id,
                        xnode_id,
                        "start task error: {:#}",
                        anyhow::Error::new(e)
                    );
                }
            }
        });
    }
}

#[instrument(skip_all)]
async fn restart_jobs(
    xnode_id: i32,
    xnodes: &XNodes,
    tasks: &Tasks,
    conn: &Arc<TaosConn>,
    cancel: CancellationToken,
) {
    let sql = format!("SHOW XNODE JOBS WHERE XNODE_ID = {xnode_id}");
    let db_jobs = match conn.query::<sql_types::JobRecord>(&sql).await {
        Ok(jobs) => jobs,
        Err(e) => {
            tracing::error!("show xnode jobs error: {:#}", anyhow::Error::new(e));
            return;
        }
    };

    for ((task_id, job_id), config) in collect_restartable_job_configs(xnode_id, &db_jobs, tasks) {
        tokio::spawn({
            let xnodes = xnodes.clone();
            let tasks = tasks.clone();
            let conn = conn.clone();
            let cancel = cancel.clone();
            async move {
                let Some(res) = cancel
                    .run_until_cancelled(start_job(
                        xnode_id, task_id, job_id, &xnodes, &tasks, &conn, config,
                    ))
                    .await
                else {
                    return;
                };
                if let Err(e) = res {
                    tracing::error!(
                        task_id,
                        job_id,
                        xnode_id,
                        "start job error: {:#}",
                        anyhow::Error::new(e)
                    );
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ha_core::{activity::TaskStatus, types::HaTask};

    use crate::controller::sql_types::{JobRecord, TaskRecord};

    fn make_task() -> HaTask {
        HaTask {
            name: "test".into(),
            from: "taos://localhost:6030".into(),
            to: "taos://localhost:6030".into(),
            parser: None,
            via: None,
            labels: None,
        }
    }

    #[test]
    fn reconnect_candidates_include_memory_running_task_when_db_task_is_not_running() {
        let tasks = Tasks::new();
        let config = make_task();
        tasks
            .add(1, -1, 7, config.clone(), Some(TaskStatus::Running))
            .expect("test task should be added");

        let db_tasks = vec![TaskRecord {
            name: "task-1".into(),
            id: 1,
            xnode_id: Some(7),
            from: config.from.clone(),
            to: config.to.clone(),
            parser: None,
            status: Some(TaskStatus::Stopped),
            via: None,
            labels: None,
        }];

        let candidates = collect_restartable_task_configs(7, &db_tasks, &tasks)
            .into_iter()
            .map(|(task_id, _)| task_id)
            .collect::<Vec<_>>();

        assert_eq!(
            candidates,
            vec![1],
            "reconnect should resend tasks that are still running in memory even if the DB task status is stale"
        );
    }

    #[test]
    fn reconnect_candidates_include_memory_running_job_when_db_job_is_not_running() {
        let tasks = Tasks::new();
        let config = make_task();
        tasks
            .add(1, 2, 7, config.clone(), Some(TaskStatus::Running))
            .expect("test job should be added");

        let db_jobs = vec![JobRecord {
            id: 2,
            task_id: 1,
            xnode_id: 7,
            config: serde_json::to_string(&config).expect("job config"),
            status: Some(TaskStatus::Stopped),
            via: None,
        }];

        let candidates = collect_restartable_job_configs(7, &db_jobs, &tasks)
            .into_iter()
            .map(|((task_id, job_id), _)| (task_id, job_id))
            .collect::<Vec<_>>();

        assert_eq!(
            candidates,
            vec![(1, 2)],
            "reconnect should resend jobs that are still running in memory even if the DB job status is stale"
        );
    }
}
