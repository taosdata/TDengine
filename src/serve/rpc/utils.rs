use std::sync::Arc;

use anyhow::Context;
use arrow_flight::error::FlightError;
use chrono::Utc;
use ha_core::{
    activity::Activity,
    batch::build_batch,
    consts::{TASK_JOB_FINISH, TASK_METRICS, XNODE_ACTIVITIES},
    types::{MetricsType, TaskMetrics},
    utils::next_req_id,
};
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use taosx_core::core_metrics::CoreMetrics;
use tonic::{Code, Status};
use tracing::instrument;

use crate::serve::rpc::FlightResult;

pub fn build_rpc_ok_batch(
    action: &str,
    context: impl serde::Serialize,
    req_id: u64,
) -> FlightResult {
    ha_core::batch::build_ok_batch(action, context, req_id).map_err(internal_err)
}

pub fn build_rpc_failed_batch(action: &str, e: FlightError, req_id: u64) -> FlightResult {
    let message = match e {
        FlightError::Tonic(status) => {
            format!("status: {:?}, message: {}", status.code(), status.message())
        }
        e => e.to_string(),
    };
    ha_core::batch::build_failed_batch(action, message, req_id).map_err(internal_err)
}

pub fn internal_err(e: anyhow::Error) -> FlightError {
    FlightError::Tonic(Status::new(Code::Internal, format!("{e:#}")).into())
}

pub fn decode_err(e: anyhow::Error) -> FlightError {
    FlightError::DecodeError(format!("{e:#}"))
}

pub fn build_metrics_batch(metrics: Arc<CoreMetrics>) -> FlightResult {
    let (task_id, job_id) = metrics.task_job_id();
    macro_rules! serialize_metrics {
        ($t: expr, $metrics: expr) => {
            (
                $t,
                serde_json::to_value($metrics)
                    .with_context(|| format!("Rpc serialize {} metrics value error", $t))
                    .map_err(internal_err)?,
            )
        };
    }
    let (metrics_type, metrics) = match metrics.as_ref() {
        CoreMetrics::Legacy(metrics) => serialize_metrics!(MetricsType::Legacy, metrics),
        CoreMetrics::TMQ(metrics) => serialize_metrics!(MetricsType::Tmq, metrics),
        CoreMetrics::IPC(metrics) => serialize_metrics!(MetricsType::Ipc, metrics),
    };
    let metrics_json = TaskMetrics {
        ts: Utc::now(),
        task_id,
        job_id,
        r#type: metrics_type,
        metrics,
    };

    let context = serde_json::to_string(&metrics_json)
        .context("Rpc serialize metrics payload error")
        .map_err(internal_err)?;
    build_batch(TASK_METRICS, &context, next_req_id())
        .context("build metrics batch error")
        .map_err(internal_err)
}

pub fn build_activity_batch(activity: Activity) -> FlightResult {
    let context = serde_json::to_string(&activity)
        .context("Rpc serialize activities value error")
        .map_err(internal_err)?;
    build_batch(XNODE_ACTIVITIES, &context, next_req_id())
        .context("build activity batch")
        .map_err(internal_err)
}

pub fn build_task_job_finish_batch(
    task_id: i64,
    job_id: i64,
    result: &anyhow::Result<()>,
) -> FlightResult {
    let context = match result {
        Ok(_) => serde_json::json!({
            "task_id": task_id,
            "job_id": job_id
        }),
        Err(e) => serde_json::json!({
            "task_id": task_id,
            "job_id": job_id,
            "error": format!("{e:#}")
        }),
    };
    let context = serde_json::to_string(&context)
        .context("Rpc serialize task job finish payload error")
        .map_err(internal_err)?;
    build_batch(TASK_JOB_FINISH, &context, next_req_id())
        .context("build task job finish batch error")
        .map_err(internal_err)
}

#[instrument(skip_all)]
pub async fn check_taos_connectivity(dsn: &Dsn) -> anyhow::Result<()> {
    let conn = TaosBuilder::from_dsn(dsn)
        .context("build taos builder error")?
        .build()
        .await
        .context("build taos connection error")?;
    conn.server_version()
        .await
        .context("fetch server_version error")?;
    Ok(())
}
