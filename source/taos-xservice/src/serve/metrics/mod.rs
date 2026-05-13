use crate::serve::data_sources::LangQuery;
use actix_web::{HttpResponse, Responder, get, web::Query};
use std::{collections::BTreeMap, sync::LazyLock};
use taosx_metrics::TaosXRecorderHandle;

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
