use actix_web::{get, web::Query, HttpResponse, Responder};
use metrics::{describe_gauge, gauge, register_gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};
use std::{collections::BTreeMap, time::Duration};
use tracing::instrument;

use crate::serve::data_sources::LangQuery;

pub(crate) mod ws;

#[derive(Debug, Default)]
pub struct Metrics {
    push: Option<String>,
    push_interval: Option<Duration>,
    interval: Option<u16>,
}

pub const METRIC_SYS_CPUS: &str = "taosx_sys_cpus";
pub const METRIC_SYS_TOTAL_MEMORY: &str = "taosx_sys_total_memory";
pub const METRIC_SYS_USED_MEMORY: &str = "taosx_sys_used_memory";
pub const METRIC_SYS_AVAILABLE_MEMORY: &str = "taosx_sys_available_memory";
pub const METRIC_SYS_UPTIME_IN_SECONDS: &str = "taosx_sys_uptime_in_seconds";
pub const METRIC_PROCESS_CPU_PERCENT: &str = "taosx_process_cpu_percent";
pub const METRIC_PROCESS_MEM_PERCENT: &str = "taosx_process_mem_percent";
pub const METRIC_PROCESS_IO_READ_BYTES: &str = "taosx_process_io_read_bytes";
pub const METRIC_PROCESS_IO_WRITTEN_BYTES: &str = "taosx_process_io_written_bytes";
#[cfg(target_os = "linux")]
pub const METRIC_PROCESS_TASKS: &str = "taosx_process_tasks";
pub const TAOSX_PROCESS_UPTIME: &str = "taosx_process_uptime";

pub fn process_metrics_init() {
    register_gauge!(METRIC_SYS_CPUS);
    describe_gauge!(METRIC_SYS_CPUS, "number of cpus");

    register_gauge!(METRIC_PROCESS_CPU_PERCENT);
    describe_gauge!(METRIC_PROCESS_CPU_PERCENT, "CPU percent of the process");

    register_gauge!(METRIC_PROCESS_IO_READ_BYTES);
    describe_gauge!(
        METRIC_PROCESS_IO_READ_BYTES,
        "IO read in bytes of the process"
    );
    register_gauge!("taosx_process_io_written_bytes");
    describe_gauge!(
        "taosx_process_io_written_bytes",
        "IO written in bytes of the process"
    );
}

pub fn process_metrics(sys: &mut sysinfo::System) -> anyhow::Result<()> {
    use sysinfo::*;
    sys.refresh_all();

    gauge!(METRIC_SYS_CPUS, sys.cpus().len() as f64);
    gauge!(METRIC_SYS_TOTAL_MEMORY, sys.total_memory() as f64);
    gauge!(METRIC_SYS_USED_MEMORY, sys.used_memory() as f64);
    gauge!(METRIC_SYS_AVAILABLE_MEMORY, sys.available_memory() as f64);
    gauge!(METRIC_SYS_UPTIME_IN_SECONDS, sys.uptime() as f64);

    let pid = get_current_pid();
    if pid.is_err() {
        let err = pid.unwrap_err();
        tracing::warn!("process metrics does not supported on current platform: {err}");
        return Ok(());
    }
    let pid = pid.unwrap();
    if let Some(ps) = sys.process(pid) {
        let cpu = ps.cpu_usage();
        gauge!(METRIC_PROCESS_CPU_PERCENT, cpu as f64);

        let mem = ps.memory() as f64 / sys.total_memory() as f64 * 100.0;
        gauge!(METRIC_PROCESS_MEM_PERCENT, mem);

        #[cfg(target_os = "linux")]
        gauge!(METRIC_PROCESS_TASKS, ps.tasks.len() as f64);

        let disk = ps.disk_usage();
        gauge!(METRIC_PROCESS_IO_READ_BYTES, disk.read_bytes as f64);
        gauge!(METRIC_PROCESS_IO_WRITTEN_BYTES, disk.written_bytes as f64);

        gauge!(TAOSX_PROCESS_UPTIME, ps.run_time() as f64);
    }
    Ok(())
}

impl Metrics {
    pub fn init(self) -> anyhow::Result<PrometheusRecorder> {
        let mut exporter = PrometheusBuilder::new();
        let interval = self.interval();
        let dur = Duration::from_secs(interval as u64);

        // if let Some(listen) = self.listen {
        //     exporter = exporter.with_http_listener(listen);
        // }
        let recorder = if let Some(push) = self.push {
            let interval = self.push_interval.unwrap_or(Duration::from_secs(30));
            exporter = exporter.with_push_gateway(push, interval, None, None)?;

            let (recorder, exporter) = exporter.build()?;
            tokio::spawn(exporter);
            recorder
        } else {
            exporter.build_recorder()
        };

        // let recorder = exporter.build_recorder();
        process_metrics_init();
        std::thread::spawn(move || {
            use sysinfo::SystemExt;
            let mut sys = sysinfo::System::new_all();
            loop {
                let _ = process_metrics(&mut sys);
                std::thread::sleep(dur);
            }
        });
        Ok(recorder)
    }

    fn interval(&self) -> u16 {
        self.interval.unwrap_or(1)
    }
}
/// Metrics like node-exporter.
#[utoipa::path(
    responses(
        (status = 200, description = "Export all metrics", body = String),
    )
)]
#[get("/metrics")]
async fn metrics_exporter(handle: actix_web::web::Data<PrometheusHandle>) -> impl Responder {
    let output = handle.render();
    output
}

#[utoipa::path(
    responses(
        (status = 200, description = "Description of all metrics")
    )
)]
#[get("/metrics/description")]
#[instrument(skip_all)]
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

/// Profile.
#[utoipa::path(
    responses(
        (status = 200, description = "version/commit"),
    )
)]
#[get("/profile")]
#[instrument(skip_all)]
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
