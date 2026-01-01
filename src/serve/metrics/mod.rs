use crate::serve::{controller::Task, data_sources::LangQuery};
use actix_web::{HttpResponse, Responder, get, web::Query};
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};
use taosx_core::{
    core_metrics::{CoreMetrics, try_get_metrics},
    legacy_metric::LegacyToTaosMetrics,
    sink::ipc_metric::IpcMetrics,
    tmq::tmq_metric::TmqMetrics,
};
use taosx_metrics::TaosXRecorderHandle;
use taosx_utils::dsn::json_to_dsn;

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

pub static METRICS_DESC_ZH: LazyLock<BTreeMap<String, String>> =
    LazyLock::new(
        || match serde_yaml::from_str(include_str!("./metrics-desc-zh.yaml")) {
            Ok(data) => data,
            Err(error) => {
                tracing::error!("failed to read metrics-desc-zh.yaml: {}", error);
                BTreeMap::new()
            }
        },
    );
pub static METRICS_DESC_EN: LazyLock<BTreeMap<String, String>> =
    LazyLock::new(
        || match serde_yaml::from_str(include_str!("./metrics-desc-en.yaml")) {
            Ok(data) => data,
            Err(error) => {
                tracing::error!("failed to read metrics-desc-en.yaml: {}", error);
                BTreeMap::new()
            }
        },
    );

pub async fn try_get_metrics_from_task(task: &Task) -> Option<Arc<CoreMetrics>> {
    // let parse_dsn_result: Result<Dsn, _> = task.task.from.parse();
    let parse_dsn_result = json_to_dsn(&serde_json::Value::String(task.from.clone()));
    if parse_dsn_result.is_err() {
        tracing::error!(
            "parse dsn error: {}, from={}",
            parse_dsn_result.unwrap_err(),
            task.from
        );
        return None;
    }
    let dsn = parse_dsn_result.unwrap();
    let task_id = task.id;
    let job_id = task.job_id;
    match dsn.driver.as_str() {
        "taos" => try_get_metrics::<LegacyToTaosMetrics>(task_id, job_id, &dsn).await,
        "tmq" | "sync" => try_get_metrics::<TmqMetrics>(task_id, job_id, &dsn).await,
        "opc" | "opcua" | "opcda" | "pi" | "pibackfill" | "mqtt" | "influxdb" | "opentsdb"
        | "kafka" | "avevaHistorian" | "csv" | "mysql" | "postgres" | "oracle" | "mssql"
        | "mongodb" | "sparkplugb" | "pulsar" | "pulsarTuya" | "kinghist" => {
            try_get_metrics::<IpcMetrics>(task_id, job_id, &dsn).await
        }
        _ => None,
    }
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
        "grpc_tls_enabled": crate::serve::controller::agent::get_grpc_ssl_ca_certificate().is_some(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_desc_maps_are_loadable() {
        // Ensure lazy statics are initialized without panic and basic iteration works.
        assert_eq!(METRICS_DESC_EN.iter().count(), METRICS_DESC_EN.len());
        assert_eq!(METRICS_DESC_ZH.iter().count(), METRICS_DESC_ZH.len());
    }

    #[test]
    fn profile_contains_expected_keys() {
        let prof = get_profile();
        for key in [
            "version",
            "core",
            "branch",
            "commit",
            "build_time",
            "build_target",
            "build_os",
            "grpc_tls_enabled",
        ] {
            assert!(
                prof.get(key).is_some(),
                "missing profile key: {} in {:?}",
                key,
                prof
            );
        }
    }
}
