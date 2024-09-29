use crate::serve::controller::Status;
use crate::serve::data_sources::LangQuery;
use actix_web::{get, web::Query, HttpResponse, Responder};
use std::{collections::BTreeMap, sync::Arc};
use taos::Dsn;
use taosx_core::{
    core_metrics::{
        compute_avg_speed, compute_total_avg_speed, split_to_total_and_current, try_get_metrics,
        CoreMetrics, TaskMetrics,
    },
    legacy_metric::LegacyToTaosMetrics,
    runners,
    sink::ipc_metric::IpcMetrics,
    tmq::tmq_metric::TmqMetrics,
};
use taosx_metrics::TaosXRecorderHandle;

use super::TaskDetail;
pub(crate) mod ws;

/// Metrics like node-exporter.
#[utoipa::path(
    responses(
        (status = 200, description = "Export all metrics", body = String),
    )
)]
#[get("/metrics")]
async fn metrics_exporter(handle: actix_web::web::Data<TaosXRecorderHandle>) -> impl Responder {
    handle.render()
}

#[utoipa::path(
    responses(
        (status = 200, description = "Description of all metrics")
    )
)]
#[get("/metrics/description")]
async fn metrics_desc(lang: Query<LangQuery>) -> impl Responder {
    if lang.is_cn() {
        HttpResponse::Ok().json(&(*METRICS_DESC_ZH))
    } else {
        HttpResponse::Ok().json(&(*METRICS_DESC_EN))
    }
}

lazy_static::lazy_static! {
    pub static ref METRICS_DESC_ZH: BTreeMap<String, String> = match serde_yaml::from_str(include_str!("./metrics-desc-zh.yaml")) {
        Ok(data) => data,
        Err(error) => {
            tracing::error!("failed to read metrics-desc-zh.yaml: {}", error);
            BTreeMap::new()
        }
    };
    pub static ref METRICS_DESC_EN: BTreeMap<String, String> = match serde_yaml::from_str(include_str!("./metrics-desc-en.yaml")) {
        Ok(data) => data,
        Err(error) => {
            tracing::error!("failed to read metrics-desc-en.yaml: {}", error);
            BTreeMap::new()
        }
    };
}

pub async fn try_get_metrics_from_task_detail(task: &TaskDetail) -> Option<Arc<CoreMetrics>> {
    let parse_dsn_result: Result<Dsn, _> = task.task.from.parse();
    if parse_dsn_result.is_err() {
        tracing::error!(
            "parse dsn error: {}, from={}",
            parse_dsn_result.unwrap_err(),
            task.task.from
        );
        return None;
    }
    let dsn = parse_dsn_result.unwrap();
    let task_id = task.task.id;
    match dsn.driver.as_str() {
        "taos" => try_get_metrics::<LegacyToTaosMetrics>(task_id).await,
        "tmq" | "sync" => try_get_metrics::<TmqMetrics>(task_id).await,
        "opc"
        | "opcua"
        | "opcda"
        | "pi"
        | "pibackfill"
        | "mqtt"
        | "influxdb"
        | "opentsdb"
        | runners::kafka::KAFKA_ID
        | runners::historian::AVEVA_HISTORIAN_ID
        | "csv"
        | runners::mysql::MYSQL_ID
        | runners::postgres::POSTGRES_ID
        | runners::oracle::ORACLE_ID
        | runners::mssql::MSSQL_ID
        | runners::mongodb::MONGODB_ID => try_get_metrics::<IpcMetrics>(task_id).await,
        _ => None,
    }
}

pub fn get_task_metrics_string(status: &Status, metrics: Arc<CoreMetrics>) -> String {
    // 根据任务的状态判断任务是否正在运行。这里的正在运行的含义是：任务正在被 scheduler 执行。
    // 这里的 running 更准确的说是任务处于需要被计算运行时间的状态。
    let running =
        status == Status::Running || status == Status::Stopping || status == Status::Waiting;
    let mut is_tmq = false;
    let (common_metrics, json) = match metrics.as_ref() {
        CoreMetrics::Legacy(legacy_metrics) => (legacy_metrics.com(), legacy_metrics.to_json()),
        CoreMetrics::TMQ(tmq_metrics) => {
            is_tmq = true;
            (tmq_metrics.com(), tmq_metrics.to_json())
        }
        CoreMetrics::IPC(ipc_metrics) => (ipc_metrics.com(), ipc_metrics.to_json()),
    };
    let mut map =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json.as_str()).unwrap();
    map.remove("task_id");
    map.remove("stable");
    map.remove("task_name");
    if is_tmq {
        map.remove("written_rows");
        map.remove("total_written_rows");
        map.remove("written_points");
        map.remove("total_written_points");
        map.remove("success_blocks");
        map.remove("total_success_blocks");
        map.remove("write_raw_fails");
        map.remove("total_write_raw_fails");
    } else {
        compute_total_avg_speed(common_metrics, &mut map);
        compute_avg_speed(common_metrics, &mut map, running);
    }
    let result = split_to_total_and_current(&map);
    serde_json::to_string(&result).unwrap()
}

/// Profile.
#[utoipa::path(
    responses(
        (status = 200, description = "version/commit"),
    )
)]
#[get("/profile")]
async fn profile() -> HttpResponse {
    HttpResponse::Ok().json(get_profile())
}

fn get_profile() -> serde_json::Value {
    serde_json::json!({
        "version": crate::build::TD_VERSION,
        "core": crate::build::PKG_VERSION,
        "branch": crate::build::BRANCH,
        "commit": crate::build::SHORT_COMMIT,
        "build_time": crate::build::BUILD_TIME_3339,
        "build_target": crate::build::BUILD_TARGET,
        "build_os": crate::build::BUILD_OS,
    })
}
