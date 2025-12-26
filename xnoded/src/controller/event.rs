use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::Duration,
};

use arrow_flight::error::FlightError;
use ha_core::{
    batch::BatchIter,
    consts::DROP_CONNECTION,
    types::{Activity, ActivityStatus, TaskStatus},
};
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    controller::{
        BuildBatchIterSnafu, BuildTaosConnSnafu, DeserializeActivitySnafu, Result, start_task_job,
        tasks::Tasks, xnodes::XNodes,
    },
    utils::{
        backoff::{BackoffDuration, RetryBackoff},
        taos_conn::{self, TaosConn},
    },
};

static CACHE_TASK_STATUS: LazyLock<RwLock<HashMap<i64, TaskStatus>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static CACHE_JOB_STATUS: LazyLock<RwLock<HashMap<(i64, i64), TaskStatus>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

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

fn get_task_status(task_id: i64) -> Option<TaskStatus> {
    CACHE_TASK_STATUS.read().get(&task_id).copied()
}

fn set_task_status(task_id: i64, status: TaskStatus) {
    CACHE_TASK_STATUS.write().insert(task_id, status);
}

fn get_job_status(task_id: i64, job_id: i64) -> Option<TaskStatus> {
    CACHE_JOB_STATUS.read().get(&(task_id, job_id)).copied()
}

fn set_job_status(task_id: i64, job_id: i64, status: TaskStatus) {
    CACHE_JOB_STATUS.write().insert((task_id, job_id), status);
}

fn del_task_status(task_id: i64) {
    CACHE_TASK_STATUS.write().remove(&task_id);
    CACHE_JOB_STATUS
        .write()
        .retain(|(tid, _), _| tid != &task_id);
    TASK_FILED_BACKOFF
        .write()
        .retain(|(tid, _), _| tid != &task_id);
}

#[instrument(skip_all, fields(xnode_id=id))]
pub async fn event_loop(
    id: i32,
    taos_dsn: String,
    xnodes: XNodes,
    tasks: Tasks,
    reconnect_tx: flume::Sender<oneshot::Sender<bool>>,
    rebalance_tx: flume::Sender<i32>,
    cancel: CancellationToken,
) -> Result<()> {
    let _cleanup = crate::utils::defer::defer(|| {
        xnodes.set_offline(id);
        xnodes.remove(id);
    });
    let _guard = cancel.drop_guard_ref();
    let taos_conn = Arc::new(
        TaosConn::create(&taos_dsn, 3)
            .await
            .context(BuildTaosConnSnafu)?,
    );
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
                if record.action != ha_core::consts::TASK_ACTIVITIES {
                    continue;
                }
                let Activity {
                    task_id,
                    job_id,
                    status,
                    activity,
                    ..
                } = serde_json::from_str(record.context).context(DeserializeActivitySnafu)?;

                let Some(ActivityStatus::Task(task_status)) = status else {
                    continue;
                };

                // 更新原子任务的 status
                let fut =
                    update_task_job(&tasks, &taos_conn, task_id, job_id, task_status, &activity);
                if cancel.run_until_cancelled(fut).await.is_none() {
                    return Ok(());
                }

                // 更新总任务状态
                let fut = update_task(&tasks, &taos_conn, task_id, job_id);
                if cancel.run_until_cancelled(fut).await.is_none() {
                    return Ok(());
                }

                // 调度失败的任务
                let fut = schedule_job(
                    &tasks,
                    &xnodes,
                    &taos_conn,
                    task_id,
                    job_id,
                    task_status,
                    cancel.child_token(),
                );
                if cancel.run_until_cancelled(fut).await.is_none() {
                    return Ok(());
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
