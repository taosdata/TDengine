use std::sync::Arc;

use ha_core::types::{HaTask, XnodedId};
use snafu::ResultExt;
use taos::Dsn;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tracing::instrument;

use crate::{
    controller::{
        BuildTaosConnSnafu, Result, agents::Agents, sql_types, start_job, start_task, tasks::Tasks,
        xnodes::XNodes,
    },
    utils::taos_conn::TaosConn,
};

#[instrument(skip_all, fields(xnode_id=id))]
pub async fn reconnect_loop(
    id: i32,
    xnoded_id: XnodedId,
    addr: String,
    endpoint: Endpoint,
    xnodes: XNodes,
    agents: Agents,
    tasks: Tasks,
    taos_dsn: Dsn,
    reconnect_rx: flume::Receiver<oneshot::Sender<bool>>,
    cancel: CancellationToken,
) -> Result<()> {
    let _cleanup = crate::utils::defer::defer(|| {
        xnodes.set_offline(id);
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
        // 重连
        reconnect(
            &endpoint,
            &addr,
            tx,
            id,
            &xnoded_id,
            &xnodes,
            cancel.child_token(),
        )
        .await;

        // 重连成功后，重新发送 agent
        resend_agents(id, &xnodes, &agents).await;

        // 重连成功后，重新发送任务
        restart_task_job(id, &xnodes, &tasks, &taos_conn, cancel.child_token()).await;
    }
    Ok(())
}

#[instrument(skip_all)]
async fn reconnect(
    endpoint: &Endpoint,
    addr: &str,
    tx: oneshot::Sender<bool>,
    xnode_id: i32,
    xnoded_id: &XnodedId,
    xnodes: &XNodes,
    cancel: CancellationToken,
) {
    let channel = match endpoint.connect().await {
        Ok(channel) => channel,
        Err(e) => {
            tracing::error!(addr, "build rpc channel error: {e:#}");
            tx.send(false).ok();
            return;
        }
    };
    let (new_event_tx, new_event_rx) = flume::bounded(1000);
    let client = match ha_rpc_client::create_client(channel, xnoded_id, new_event_tx, cancel).await
    {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(addr, "create rpc client error: {:#}", anyhow::Error::new(e));
            tx.send(false).ok();
            return;
        }
    };
    xnodes.set_online(xnode_id, client.clone(), new_event_rx);
    tracing::info!("xnode {xnode_id} connected");
    tx.send(true).ok();
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

#[instrument(skip_all)]
async fn restart_tasks(
    xnode_id: i32,
    xnodes: &XNodes,
    tasks: &Tasks,
    conn: &Arc<TaosConn>,
    cancel: CancellationToken,
) {
    let sql = format!("SHOW XNODE JOBS WHERE XNODE_ID = {xnode_id}");
    let db_tasks = match conn.query::<sql_types::TaskRecord>(&sql).await {
        Ok(tasks) => tasks,
        Err(e) => {
            tracing::error!("show xnode jobs error: {:#}", anyhow::Error::new(e));
            return;
        }
    };

    for task in db_tasks {
        if task.status.is_none_or(|v| !v.is_running()) {
            continue;
        }
        if task.xnode_id.is_none_or(|v| v != xnode_id) {
            continue;
        }
        let task_id = task.id;
        let parser = match task.parser.map(|v| serde_json::from_str(&v)).transpose() {
            Ok(parser) => parser,
            Err(e) => {
                tracing::error!("parse task parser error: {e}",);
                continue;
            }
        };
        let config = HaTask {
            from: task.from,
            to: task.to,
            parser,
            via: task.via,
        };

        tokio::spawn({
            let xnodes = xnodes.clone();
            let tasks = tasks.clone();
            let conn = conn.clone();
            let cancel = cancel.clone();
            async move {
                let Some(res) = cancel
                    .run_until_cancelled(start_task(
                        xnode_id, task_id, &xnodes, &tasks, &conn, config,
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

    for job in db_jobs {
        if job.status.is_none_or(|v| !v.is_running()) {
            continue;
        }
        if xnode_id != job.xnode_id {
            continue;
        }
        let (task_id, job_id) = (job.task_id, job.id);
        let config: HaTask = match serde_json::from_str(&job.config) {
            Ok(config) => config,
            Err(e) => {
                tracing::error!(
                    task_id,
                    job_id,
                    "parse job config error: {:#}",
                    anyhow::Error::new(e)
                );
                continue;
            }
        };
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
