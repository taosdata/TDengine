use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU32, Ordering},
    },
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
    types::{MetricsType, TaskMetrics},
};
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;
use taos::Dsn;
use taosx_utils::backoff::{BackoffDuration, RetryBackoff};
use taosx_utils::sql::sql_value_escaped_fmt;
use taosx_utils::taos_conn::{self, Error as TaosConnError, TaosConn};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::controller::{
    BuildBatchIterSnafu, BuildTaosConnSnafu, CreateAgentActivityTableSnafu, CreateLogDatabaseSnafu,
    CreateMetricsTableSnafu, CreateTaskActivityTableSnafu, Result,
    agents::Agents,
    start_task_job,
    tasks::Tasks,
    updaters::{compute_aggregate_status, update_agent_status},
    xnodes::XNodes,
};

type TaskFailedBackoff = HashMap<(i64, i64), Mutex<BackoffDuration>>;
static TASK_FILED_BACKOFF: LazyLock<RwLock<TaskFailedBackoff>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Maximum VARCHAR width for the `value` column in TDengine (platform limit).
const TASK_METRICS_VALUE_VARCHAR_MAX: u32 = 65517;
/// Initial VARCHAR width matching the CREATE STABLE definition.
const TASK_METRICS_VALUE_VARCHAR_INITIAL: u32 = 2048;
/// Tracks the current VARCHAR width so ALTER STABLE is only issued when needed.
static TASK_METRICS_VALUE_WIDTH: AtomicU32 = AtomicU32::new(TASK_METRICS_VALUE_VARCHAR_INITIAL);

/// Returns the smallest power of two that is ≥ `n` (minimum 1).
fn next_pow2_at_least(n: u32) -> u32 {
    n.max(1).next_power_of_two()
}

/// Returns `true` when `err` represents a "value too long" class of TDengine error.
///
/// Only the source chain is inspected — the outer `Display` for `TaosConnError::Taos`
/// includes the full SQL payload (`Failed to query sql {sql}`), so matching against
/// it would cause false positives when the data itself contains trigger phrases.
fn is_value_too_long_error(err: &TaosConnError) -> bool {
    use std::error::Error as StdError;

    let mut src: Option<&dyn StdError> = err.source();
    while let Some(e) = src {
        let lower = e.to_string().to_ascii_lowercase();
        if lower.contains("value too long")
            || lower.contains("string column length too long")
            || lower.contains("length exceeds")
            || lower.contains("string overflow")
        {
            return true;
        }
        src = e.source();
    }
    false
}

/// Issues `ALTER STABLE … MODIFY COLUMN value VARCHAR(new_size)`.
async fn grow_task_metrics_value_column(
    conn: &TaosConn,
    new_size: u32,
) -> std::result::Result<(), TaosConnError> {
    let sql = format!(
        "ALTER STABLE log.`{TASK_METRICS_STABLE}` MODIFY COLUMN `value` VARCHAR({new_size})"
    );
    conn.exec(&sql).await.map(|_| ())
}

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

fn del_job_backoff(task_id: i64, job_id: i64) {
    TASK_FILED_BACKOFF.write().remove(&(task_id, job_id));
}

fn clear_skipped_failed_task_restart(tasks: &Tasks, task_id: i64, job_id: i64) {
    del_job_backoff(task_id, job_id);
    if tasks.is_stopped(task_id) {
        tasks.del_task(task_id);
        tasks.del_cached_task_status(task_id);
        del_task_backoff(task_id);
    }
}

fn should_skip_failed_task_restart(tasks: &Tasks, task_id: i64, job_id: i64) -> bool {
    tasks.is_manually_stopped(task_id)
        || tasks.is_oneshot(task_id)
        || tasks.job(task_id, job_id).is_none()
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
    let _cleanup = taosx_utils::defer::defer(|| {
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
                            if matches!(agent_status, AgentStatus::Unknown) {
                                continue;
                            }
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

                        if matches!(task_status, TaskStatus::Unknown) {
                            continue;
                        }

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
    // For no-sub-job tasks (job_id < 0) the DB column is the task-level status,
    // so we compare and update task_status_cache instead of job_status_cache.
    let cached_status = tasks.get_cached_status(task_id, job_id);
    if !tasks.contains(task_id, job_id) && task_status.is_stopped() {
        return;
    }
    if cached_status.is_some_and(|v| v == task_status) {
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
        tasks.set_cached_status(task_id, job_id, task_status);
    }

    // Reset the failure reason when transitioning away from Failed.
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

    // Record the failure reason when the job transitions to Failed.
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

    let status = compute_aggregate_status(tasks, task_id);

    if tasks
        .get_cached_task_status(task_id)
        .is_some_and(|v| v == status)
    {
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
        tasks.set_cached_task_status(task_id, status);
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
    if should_skip_failed_task_restart(tasks, task_id, job_id) {
        clear_skipped_failed_task_restart(tasks, task_id, job_id);
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
            if should_skip_failed_task_restart(&tasks, task_id, job_id) {
                clear_skipped_failed_task_restart(&tasks, task_id, job_id);
                return;
            }

            let start_job_fut = start_task_job(
                xnode_id,
                task_id,
                job_id,
                &xnodes,
                &tasks,
                &taos_conn,
                task.config,
                false,
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
    let ts = ts.timestamp_millis();
    let sql = build_task_metrics_insert_sql(id, task_id, job_id, ts, &r#type, &metrics);
    match conn.exec(&sql).await {
        Ok(_) => {}
        Err(e) if is_value_too_long_error(&e) => {
            let metrics_len = metrics.len() as u32;
            let current = TASK_METRICS_VALUE_WIDTH.load(Ordering::Relaxed);
            let needed = next_pow2_at_least(metrics_len).min(TASK_METRICS_VALUE_VARCHAR_MAX);
            if needed <= current {
                // Race A: another writer already grew the column. The cached width is
                // already wide enough, so skip ALTER and retry the INSERT once.
                tracing::warn!(
                    "Metrics value length {metrics_len} fits VARCHAR({current}) but INSERT \
                    still failed with overflow; retrying (concurrent growth may have already \
                    widened the column): {:#}",
                    anyhow::Error::new(e)
                );
                if let Err(retry_err) = conn.exec(&sql).await {
                    tracing::error!(
                        "Failed to insert metrics on retry after concurrent column growth: {:#}",
                        anyhow::Error::new(retry_err)
                    );
                }
                return;
            }
            tracing::warn!(
                "Metrics value length {metrics_len} exceeds VARCHAR({current}); \
                growing log.{TASK_METRICS_STABLE}.value to VARCHAR({needed})"
            );
            if let Err(alter_err) = grow_task_metrics_value_column(conn, needed).await {
                // Race B: another writer may have issued the same ALTER and succeeded.
                // Log the failure and retry the INSERT once instead of dropping the record.
                tracing::error!(
                    "Failed to grow {TASK_METRICS_STABLE}.value to VARCHAR({needed}): {:#}",
                    anyhow::Error::new(alter_err)
                );
                if let Err(retry_err) = conn.exec(&sql).await {
                    tracing::error!(
                        "Failed to insert metrics after ALTER failure: {:#}",
                        anyhow::Error::new(retry_err)
                    );
                }
                return;
            }
            TASK_METRICS_VALUE_WIDTH.fetch_max(needed, Ordering::Relaxed);
            if let Err(retry_err) = conn.exec(&sql).await {
                tracing::error!(
                    "Failed to insert metrics after growing column: {:#}",
                    anyhow::Error::new(retry_err)
                );
            }
        }
        Err(e) => {
            tracing::error!("Failed to insert metrics: {:#}", anyhow::Error::new(e));
        }
    }
}

fn build_task_metrics_insert_sql(
    id: i32,
    task_id: i64,
    job_id: i64,
    ts: i64,
    task_type: &MetricsType,
    metrics: &str,
) -> String {
    let table = if job_id < 0 {
        format!("{TASK_METRICS_STABLE}_xnode_{id}_task_{task_id}")
    } else {
        format!("{TASK_METRICS_STABLE}_xnode_{id}_task_{task_id}_job_{job_id}")
    };
    let task_type = task_type.to_string();
    let escaped_task_type = escape_task_type_tag(&task_type);
    let escaped_metrics = sql_value_escaped_fmt(metrics);
    format!(
        "INSERT INTO log.`{table}` \
        USING log.`{TASK_METRICS_STABLE}` TAGS ({id}, {task_id}, {job_id}, {escaped_task_type}) \
        VALUES ({ts}, {escaped_metrics})"
    )
}

fn escape_task_type_tag(task_type: &str) -> String {
    sql_value_escaped_fmt(task_type).to_string()
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
    update_agent_status(conn, xnodes, agents, agent_id).await;
}

#[cfg(test)]
mod auto_extend_tests {
    use super::*;

    #[test]
    fn next_pow2_basic() {
        assert_eq!(next_pow2_at_least(0), 1);
        assert_eq!(next_pow2_at_least(1), 1);
        assert_eq!(next_pow2_at_least(2), 2);
        assert_eq!(next_pow2_at_least(3), 4);
        assert_eq!(next_pow2_at_least(2048), 2048);
        assert_eq!(next_pow2_at_least(2049), 4096);
        assert_eq!(next_pow2_at_least(4096), 4096);
        assert_eq!(next_pow2_at_least(65517), 65536);
    }

    #[test]
    fn varchar_constants_match_design() {
        assert_eq!(TASK_METRICS_VALUE_VARCHAR_INITIAL, 2048);
        assert_eq!(TASK_METRICS_VALUE_VARCHAR_MAX, 65517);
    }

    #[test]
    fn is_value_too_long_error_matches_known_patterns() {
        use taosx_utils::taos_conn::Error as TaosConnError;

        let patterns = [
            "value too long for the column",
            "string column length too long",
            "length exceeds the limit",
            "string overflow detected",
        ];
        for msg in patterns {
            let err = TaosConnError::Taos {
                sql: "INSERT INTO log.x".into(),
                source: taos::RawError::new(0x2600, msg),
            };
            assert!(
                is_value_too_long_error(&err),
                "expected true for message: {msg}"
            );
        }

        let safe_err = TaosConnError::Taos {
            sql: "INSERT INTO log.x".into(),
            source: taos::RawError::new(0x2603, "table does not exist"),
        };
        assert!(!is_value_too_long_error(&safe_err));
    }

    /// Verifies that trigger words appearing only in the SQL payload do not cause
    /// a non-overflow error to be misclassified as a width-overflow error.
    #[test]
    fn is_value_too_long_error_ignores_sql_payload() {
        use taosx_utils::taos_conn::Error as TaosConnError;

        // The SQL payload contains "value too long" but the actual TDengine error
        // is an unrelated syntax error; this must NOT be classified as overflow.
        let err = TaosConnError::Taos {
            sql: "INSERT INTO log.x VALUES (1, 'value too long payload text here')".into(),
            source: taos::RawError::new(0x2600, "syntax error"),
        };
        assert!(
            !is_value_too_long_error(&err),
            "SQL payload containing 'value too long' must not trigger overflow misclassification"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ha_core::types::{HaTask, MetricsType};

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
    fn failed_restart_is_skipped_after_manual_stop() {
        let tasks = Tasks::new();
        tasks
            .add(1, -1, 7, make_task(), Some(TaskStatus::Failed))
            .expect("test task should be added");
        tasks.set_manually_stopped(1, -1);

        assert!(
            should_skip_failed_task_restart(&tasks, 1, -1),
            "manually stopped tasks must not be restarted after the backoff delay"
        );
    }

    #[test]
    fn failed_restart_is_skipped_after_task_removal() {
        let tasks = Tasks::new();
        tasks
            .add(1, -1, 7, make_task(), Some(TaskStatus::Failed))
            .expect("test task should be added");
        tasks.del_task(1);

        assert!(
            should_skip_failed_task_restart(&tasks, 1, -1),
            "removed task jobs must not be restarted after the backoff delay"
        );
    }

    #[test]
    fn skipped_failed_restart_clears_only_target_job_backoff() {
        let tasks = Tasks::new();
        tasks
            .add(1, -1, 7, make_task(), Some(TaskStatus::Failed))
            .expect("first test task should be added");
        tasks
            .add(1, 2, 7, make_task(), Some(TaskStatus::Running))
            .expect("second test task should be added");
        tasks.del_task_job(1, -1);

        task_backoff_duration(1, -1);
        task_backoff_duration(1, 2);
        assert!(TASK_FILED_BACKOFF.read().contains_key(&(1, -1)));
        assert!(TASK_FILED_BACKOFF.read().contains_key(&(1, 2)));

        clear_skipped_failed_task_restart(&tasks, 1, -1);

        assert!(
            !TASK_FILED_BACKOFF.read().contains_key(&(1, -1)),
            "skipped job backoff should be cleaned up"
        );
        assert!(
            TASK_FILED_BACKOFF.read().contains_key(&(1, 2)),
            "other job backoff entries for the same task should be preserved"
        );

        del_task_backoff(1);
    }

    #[test]
    fn escape_task_type_tag_escapes_quotes() {
        let escaped = escape_task_type_tag("raw'type");

        assert!(
            escaped.contains("raw''type"),
            "task type should be SQL-escaped inside TAGS: {escaped}"
        );
        assert!(
            escaped == "'raw''type'",
            "escaped task type should remain a single SQL literal: {escaped}"
        );
    }

    #[test]
    fn build_task_metrics_insert_sql_accepts_metrics_type() {
        let sql = build_task_metrics_insert_sql(7, 11, -1, 42, &MetricsType::Tmq, "'{}'");
        assert!(sql.contains("TAGS (7, 11, -1, 'tmq')"), "{sql}");
    }
}
