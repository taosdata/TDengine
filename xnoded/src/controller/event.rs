use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::Duration,
};

use arrow_flight::error::FlightError;
use ha_core::{
    activity::{Activity, ActivityLevel, ActivityStatus, AgentStatus, TaskStatus},
    batch::{BatchIter, build_batch},
    consts::{
        AGENT_ACTIVITIES_STABLE, DROP_CONNECTION, HEARTBEAT_REQ, HEARTBEAT_RESP,
        TASK_ACTIVITIES_STABLE, TASK_METRICS, TASK_METRICS_STABLE, XNODE_ACTIVITIES,
    },
    types::TaskMetrics,
};
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;
use taos::Dsn;
use taosx_utils::sql::sql_value_escaped_fmt;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    controller::{
        BuildBatchIterSnafu, BuildTaosConnSnafu, CreateAgentActivityTableSnafu,
        CreateLogDatabaseSnafu, CreateMetricsTableSnafu, CreateTaskActivityTableSnafu, Result,
        agents::Agents,
        start_task_job,
        tasks::Tasks,
        updaters::{
            del_task_status, get_job_status, get_task_status, set_job_status, set_task_status,
            update_agent_status,
        },
        xnodes::XNodes,
    },
    utils::{
        backoff::{BackoffDuration, RetryBackoff},
        taos_conn::{self, TaosConn},
    },
};

type TaskFailedBackoff = HashMap<(i64, i64), Mutex<BackoffDuration>>;
static TASK_FILED_BACKOFF: LazyLock<RwLock<TaskFailedBackoff>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

fn task_backoff_duration(task_id: i64, job_id: i64) -> Duration {
    if let Some(backoff) = TASK_FILED_BACKOFF.read().get(&(task_id, job_id)) {
        return backoff.lock().next();
    }

    let mut backoff = BackoffDuration::new(Duration::from_millis(500), Duration::from_secs(10));
    let duration = backoff.next();
    TASK_FILED_BACKOFF
        .write()
        .insert((task_id, job_id), Mutex::new(backoff));
    duration
}

fn del_task_backoff(task_id: i64) {
    TASK_FILED_BACKOFF
        .write()
        .retain(|(tid, _), _| tid != &task_id);
}

#[instrument(skip_all)]
async fn init_log_db(conn: &TaosConn) -> bool {
    if let Err(e) = init_log_db_inner(conn).await {
        tracing::error!(
            "Failed to initialize log database and stables: {:#}",
            anyhow::Error::new(e)
        );
        return false;
    }
    true
}

#[instrument(skip_all)]
async fn init_log_db_inner(conn: &TaosConn) -> Result<()> {
    static LOG_DB_INITED: tokio::sync::RwLock<bool> = tokio::sync::RwLock::const_new(false);
    if *LOG_DB_INITED.read().await {
        return Ok(());
    }
    let mut inited = LOG_DB_INITED.write().await;
    if *inited {
        return Ok(());
    }

    let sql = r"CREATE DATABASE IF NOT EXISTS log";
    conn.exec(sql).await.context(CreateLogDatabaseSnafu)?;

    let sql = format!(
        "CREATE STABLE IF NOT EXISTS log.`{TASK_ACTIVITIES_STABLE}` \
        (ts TIMESTAMP, level VARCHAR(5), status VARCHAR(20), activity VARCHAR(2048)) \
        TAGS (xnode_id INT, task_id INT, job_id INT)"
    );
    conn.exec(&sql)
        .await
        .context(CreateTaskActivityTableSnafu)?;
    let sql = format!(
        "CREATE STABLE IF NOT EXISTS log.`{AGENT_ACTIVITIES_STABLE}` \
        (ts TIMESTAMP, level VARCHAR(5), status VARCHAR(20), activity VARCHAR(2048)) \
        TAGS (xnode_id INT, agent_id INT)"
    );
    conn.exec(&sql)
        .await
        .context(CreateAgentActivityTableSnafu)?;

    let sql = format!(
        "CREATE STABLE IF NOT EXISTS log.`{TASK_METRICS_STABLE}` \
        (`ts` TIMESTAMP, `value` VARCHAR(2048)) \
        TAGS (xnode_id INT, task_id INT, job_id INT, type VARCHAR(10))"
    );
    conn.exec(&sql).await.context(CreateMetricsTableSnafu)?;
    *inited = true;
    Ok(())
}

#[instrument(skip_all, fields(xnode_id=id))]
pub async fn event_loop(
    id: i32,
    taos_dsn: Dsn,
    xnodes: XNodes,
    agents: Agents,
    tasks: Tasks,
    reconnect_tx: flume::Sender<oneshot::Sender<bool>>,
    rebalance_tx: flume::Sender<i32>,
    cancel: CancellationToken,
) -> Result<()> {
    let _cleanup = crate::utils::defer::defer(|| {
        xnodes.set_offline(id);
        tracing::info!(xnode_id = id, "event loop exited");
    });
    let _guard = cancel.drop_guard_ref();
    let taos_conn = Arc::new(
        TaosConn::create(&taos_dsn, 3)
            .await
            .context(BuildTaosConnSnafu)?,
    );

    init_log_db(&taos_conn).await;

    macro_rules! call_with_cancel {
        ($fut: expr) => {
            if cancel.run_until_cancelled($fut).await.is_none() {
                return Ok(());
            }
        };
    }

    let mut backoff = RetryBackoff::new(Duration::from_secs(1), Duration::from_secs(10));

    loop {
        if xnodes.is_offline(id) {
            if cancel.run_until_cancelled(backoff.wait()).await.is_none() {
                break;
            }
            if backoff.elapsed() >= Duration::from_secs(30)
                && cancel
                    .run_until_cancelled(rebalance_tx.send_async(id))
                    .await
                    .is_none()
            {
                break;
            }
            let (tx, rx) = oneshot::channel();
            if reconnect_tx.send_async(tx).await.is_err() {
                break;
            };
            let Some(Ok(ok)) = cancel.run_until_cancelled(rx).await else {
                break;
            };
            if !ok {
                continue;
            }
        }
        let Some(event_rx) = xnodes.get_event_rx(id) else {
            continue;
        };
        let Some(received) = cancel.run_until_cancelled(event_rx.recv_async()).await else {
            break;
        };
        match received {
            Ok(Ok(batch)) => {
                let Some(record) = BatchIter::new(&batch).context(BuildBatchIterSnafu)?.next()
                else {
                    continue;
                };
                if record.action == DROP_CONNECTION {
                    tracing::info!(xnode_id = id, "Received DROP_CONNECTION event");
                    xnodes.set_offline(id);
                    return Ok(());
                }
                if record.action == HEARTBEAT_REQ {
                    match xnodes.get_client(id) {
                        Some(client) => {
                            if let Ok(batch) =
                                build_batch(HEARTBEAT_RESP, record.context, record.req_id)
                            {
                                client.send_no_reply_batch(batch).await.ok();
                            }
                        }
                        None => continue,
                    }
                }

                match record.action {
                    XNODE_ACTIVITIES => {
                        let Activity {
                            agent_id,
                            task_id,
                            job_id,
                            status,
                            activity,
                            at,
                            level,
                            ..
                        } = match serde_json::from_str(record.context) {
                            Ok(activity) => activity,
                            Err(e) => {
                                tracing::error!(
                                    xnode_id = id,
                                    "Failed to deserialize activity: {e}",
                                );
                                continue;
                            }
                        };

                        let ts = at.timestamp_millis();

                        if let ActivityStatus::Agent(agent_status) = status {
                            call_with_cancel!(insert_agent_activity(
                                id,
                                &taos_conn,
                                agent_id,
                                ts,
                                level,
                                agent_status,
                                &activity,
                            ));
                            call_with_cancel!(process_agent_status(
                                id,
                                &taos_conn,
                                &xnodes,
                                &agents,
                                agent_id,
                                agent_status,
                            ));
                            continue;
                        }

                        let ActivityStatus::Task(task_status) = status else {
                            continue;
                        };

                        call_with_cancel!(insert_task_activity(
                            &taos_conn,
                            id,
                            task_id,
                            job_id,
                            ts,
                            level,
                            task_status,
                            &activity,
                        ));

                        // 更新原子任务的 status
                        call_with_cancel!(update_task_job(
                            &tasks,
                            &taos_conn,
                            task_id,
                            job_id,
                            task_status,
                            &activity,
                        ));

                        // 更新总任务状态
                        call_with_cancel!(update_task(&tasks, &taos_conn, task_id, job_id));

                        // 调度失败的任务
                        call_with_cancel!(schedule_job(
                            &tasks,
                            &xnodes,
                            &taos_conn,
                            task_id,
                            job_id,
                            task_status,
                            cancel.child_token(),
                        ));
                    }
                    TASK_METRICS => insert_metrics(&taos_conn, id, record.context).await,
                    _ => {}
                }
            }
            Ok(Err(flight_error)) => match &flight_error {
                FlightError::Tonic(status)
                    if matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::DataLoss
                    ) =>
                {
                    tracing::error!("eventloop received tonic error: {flight_error:#}",);
                    xnodes.set_offline(id);
                }
                e => {
                    tracing::error!("eventloop received flight error: {e:#}");
                }
            },
            Err(_) => {
                tracing::error!("rpc eventloop exited");
                xnodes.set_offline(id);
            }
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn update_task_job(
    tasks: &Tasks,
    taos_conn: &TaosConn,
    task_id: i64,
    job_id: i64,
    task_status: TaskStatus,
    activity: &str,
) {
    tasks.set_status(task_id, job_id, task_status);
    let cached_status = get_job_status(task_id, job_id);
    if !tasks.contains(task_id, job_id) && task_status.is_stopped() {
        return;
    }
    if !cached_status.is_none_or(|v| v != task_status) {
        return;
    }

    let status = task_status.to_string();
    tracing::info!(task_id, job_id, status, "set job status");
    let sql = if job_id < 0 {
        format!("ALTER XNODE TASK {task_id} WITH STATUS '{status}'")
    } else {
        format!("ALTER XNODE JOB {job_id} WITH STATUS '{status}'")
    };

    if let Err(e) = taos_conn.exec(&sql).await {
        if !matches!(e, taos_conn::Error::TaskJobNotExists) {
            tracing::error!("Failed to alter job status: {:#}", anyhow::Error::new(e));
        }
    } else {
        set_job_status(task_id, job_id, task_status);
    }

    // 更新原子任务的 reason
    if cached_status.is_none_or(|v| matches!(v, TaskStatus::Failed))
        && !matches!(task_status, TaskStatus::Failed)
    {
        tracing::info!(task_id, job_id, "reset task reason");
        let sql = if job_id < 0 {
            format!("ALTER XNODE TASK {task_id} WITH REASON ''")
        } else {
            format!("ALTER XNODE JOB {job_id} WITH REASON ''")
        };

        if let Err(e) = taos_conn.exec(&sql).await
            && !matches!(e, taos_conn::Error::TaskJobNotExists)
        {
            tracing::error!(
                task_id,
                job_id,
                sql,
                "Failed to set task job reason: {:#}",
                anyhow::Error::new(e)
            );
        }
    }

    // 更新原子任务的失败原因
    if matches!(task_status, TaskStatus::Failed) {
        let sql = if job_id < 0 {
            format!("ALTER XNODE TASK {task_id} WITH REASON '{activity}'")
        } else {
            format!("ALTER XNODE JOB {job_id} WITH REASON '{activity}'")
        };

        if let Err(e) = taos_conn.exec(&sql).await
            && !matches!(e, taos_conn::Error::TaskJobNotExists)
        {
            tracing::error!(
                task_id,
                job_id,
                sql,
                "Failed to alter task job failed reason: {:#}",
                anyhow::Error::new(e)
            );
        }
    }
}

#[instrument(skip_all)]
async fn update_task(tasks: &Tasks, taos_conn: &TaosConn, task_id: i64, job_id: i64) {
    if job_id < 0 || !tasks.contains(task_id, job_id) {
        return;
    }

    let status = if tasks.is_manually_stopped(task_id) {
        if tasks.is_stopped(task_id) {
            TaskStatus::Stopped
        } else {
            TaskStatus::Stopping
        }
    } else if tasks.is_stopped(task_id) {
        TaskStatus::Stopped
    } else {
        TaskStatus::Running
    };

    if !get_task_status(task_id).is_none_or(|v| v != status) {
        return;
    }

    tracing::info!(task_id, %status, "set task status");

    let sql = format!("ALTER XNODE TASK {task_id} WITH STATUS '{status}'");

    if let Err(e) = taos_conn.exec(&sql).await {
        if !matches!(e, taos_conn::Error::TaskJobNotExists) {
            tracing::error!(
                task_id,
                job_id,
                sql,
                "Failed to set stopped task status: {:#}",
                anyhow::Error::new(e)
            );
        }
    } else {
        set_task_status(task_id, status);
    }
}

#[instrument(skip_all)]
async fn schedule_job(
    tasks: &Tasks,
    xnodes: &XNodes,
    taos_conn: &Arc<TaosConn>,
    task_id: i64,
    job_id: i64,
    task_status: TaskStatus,
    cancel: CancellationToken,
) {
    if tasks.is_manually_stopped(task_id) || tasks.is_oneshot(task_id) {
        if tasks.is_stopped(task_id) {
            tasks.del_task(task_id);
            del_task_status(task_id);
            del_task_backoff(task_id);
        }
        return;
    }

    // 失败的任务才重新调度
    if !matches!(task_status, TaskStatus::Failed) {
        return;
    }
    let Some(task) = tasks.job(task_id, job_id) else {
        return;
    };

    // 任务报错，不迁移任务，只在当前节点重启
    let xnode_id = task.xnode_id;
    tokio::spawn({
        let xnodes = xnodes.clone();
        let tasks = tasks.clone();
        let taos_conn = taos_conn.clone();
        let cancel = cancel.child_token();
        async move {
            let duration = task_backoff_duration(task_id, job_id);
            tokio::time::sleep(duration).await;

            let start_job_fut = start_task_job(
                xnode_id,
                task_id,
                job_id,
                &xnodes,
                &tasks,
                &taos_conn,
                task.config,
            );
            let Some(res) = cancel.run_until_cancelled(start_job_fut).await else {
                return;
            };
            if let Err(e) = res {
                tracing::error!(
                    task_id,
                    job_id,
                    "failed to start task job: {:#}",
                    anyhow::Error::new(e)
                );
            }
        }
    });
}

#[instrument(skip_all)]
async fn insert_metrics(conn: &TaosConn, id: i32, context: &str) {
    if !init_log_db(conn).await {
        return;
    }
    let TaskMetrics {
        ts,
        task_id,
        job_id,
        r#type,
        metrics,
    } = match serde_json::from_str(context) {
        Ok(metrics) => metrics,
        Err(e) => {
            tracing::error!(xnode_id = id, "Failed to deserialize task metrics: {e}",);
            return;
        }
    };
    let metrics = match serde_json::to_string(&metrics) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(xnode_id = id, "Failed to serialize task metrics: {e}",);
            return;
        }
    };
    let table = if job_id < 0 {
        format!("{TASK_METRICS_STABLE}_xnode_{id}_task_{task_id}")
    } else {
        format!("{TASK_METRICS_STABLE}_xnode_{id}_task_{task_id}_job_{job_id}")
    };
    let ts = ts.timestamp_millis();
    let sql = format!(
        "INSERT INTO log.`{table}` \
        USING log.`{TASK_METRICS_STABLE}` TAGS ({id}, {task_id}, {job_id}, '{}') \
        VALUES ({ts}, '{metrics}')",
        r#type
    );
    if let Err(e) = conn.exec(&sql).await {
        tracing::error!("Failed to insert metrics: {:#}", anyhow::Error::new(e));
    }
}

#[instrument(skip_all)]
async fn insert_task_activity(
    conn: &TaosConn,
    id: i32,
    task_id: i64,
    job_id: i64,
    ts: i64,
    level: ActivityLevel,
    status: TaskStatus,
    activity: &str,
) {
    if !init_log_db(conn).await {
        return;
    }
    let table = if job_id < 0 {
        format!("{TASK_ACTIVITIES_STABLE}_xnode_{id}_task_{task_id}")
    } else {
        format!("{TASK_ACTIVITIES_STABLE}_xnode_{id}_task_{task_id}_job_{job_id}")
    };
    let sql = format!(
        "INSERT INTO log.`{table}` \
        USING log.`{TASK_ACTIVITIES_STABLE}` TAGS ({id}, {task_id}, {job_id}) \
        VALUES ({ts}, '{level}', '{status}', {})",
        sql_value_escaped_fmt(activity)
    );
    if let Err(e) = conn.exec(&sql).await {
        tracing::error!("Failed to insert activity: {:#}", anyhow::Error::new(e));
    }
}

#[instrument(skip_all)]
async fn insert_agent_activity(
    id: i32,
    conn: &TaosConn,
    agent_id: i64,
    ts: i64,
    level: ActivityLevel,
    status: AgentStatus,
    activity: &str,
) {
    if !init_log_db(conn).await {
        return;
    }
    let sql = format!(
        "INSERT INTO log.`{AGENT_ACTIVITIES_STABLE}_xnode_{id}_agent_{agent_id}` \
        USING log.`{AGENT_ACTIVITIES_STABLE}` TAGS ({id}, {agent_id}) \
        VALUES ({ts}, '{level}', '{status}', {})",
        sql_value_escaped_fmt(activity)
    );
    if let Err(e) = conn.exec(&sql).await {
        tracing::error!("Failed to insert activity: {:#}", anyhow::Error::new(e));
    }
}

#[instrument(skip_all)]
async fn process_agent_status(
    id: i32,
    conn: &TaosConn,
    xnodes: &XNodes,
    agents: &Agents,
    agent_id: i64,
    status: AgentStatus,
) {
    if agents.has(agent_id) {
        xnodes.set_agent_status(id, agent_id, status);
    }
    update_agent_status(conn, xnodes, agent_id).await;
}
