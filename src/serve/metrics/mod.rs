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
use tracing::instrument;

use super::TaskDetail;
pub(crate) mod ws;

// use std::time::Duration;
// use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};
// #[derive(Debug, Default)]
// pub struct Metrics {
//     push: Option<String>,
//     push_interval: Option<Duration>,
//     interval: Option<u16>,
// }

// pub const METRIC_SYS_CPUS: &str = "taosx_sys_cpus";
// pub const METRIC_SYS_TOTAL_MEMORY: &str = "taosx_sys_total_memory";
// pub const METRIC_SYS_USED_MEMORY: &str = "taosx_sys_used_memory";
// pub const METRIC_SYS_AVAILABLE_MEMORY: &str = "taosx_sys_available_memory";
// pub const METRIC_SYS_UPTIME_IN_SECONDS: &str = "taosx_sys_uptime_in_seconds";
// pub const METRIC_PROCESS_CPU_PERCENT: &str = "taosx_process_cpu_percent";
// pub const METRIC_PROCESS_MEM_PERCENT: &str = "taosx_process_mem_percent";
// pub const METRIC_PROCESS_IO_READ_BYTES: &str = "taosx_process_io_read_bytes";
// pub const METRIC_PROCESS_IO_WRITTEN_BYTES: &str = "taosx_process_io_written_bytes";
// #[cfg(target_os = "linux")]
// pub const METRIC_PROCESS_TASKS: &str = "taosx_process_tasks";
// pub const TAOSX_PROCESS_UPTIME: &str = "taosx_process_uptime";

// pub fn process_metrics_init() {
// register_gauge!(METRIC_SYS_CPUS);
// describe_gauge!(METRIC_SYS_CPUS, "number of cpus");

// register_gauge!(METRIC_PROCESS_CPU_PERCENT);
// describe_gauge!(METRIC_PROCESS_CPU_PERCENT, "CPU percent of the process");

// register_gauge!(METRIC_PROCESS_IO_READ_BYTES);
// describe_gauge!(
//     METRIC_PROCESS_IO_READ_BYTES,
//     "IO read in bytes of the process"
// );
// register_gauge!("taosx_process_io_written_bytes");
// describe_gauge!(
//     "taosx_process_io_written_bytes",
//     "IO written in bytes of the process"
// );
// }

// pub fn process_metrics(sys: &mut sysinfo::System) -> anyhow::Result<()> {
//     use sysinfo::*;
//     sys.refresh_all();

// gauge!(METRIC_SYS_CPUS, sys.cpus().len() as f64);
// gauge!(METRIC_SYS_TOTAL_MEMORY, sys.total_memory() as f64);
// gauge!(METRIC_SYS_USED_MEMORY, sys.used_memory() as f64);
// gauge!(METRIC_SYS_AVAILABLE_MEMORY, sys.available_memory() as f64);
// gauge!(
//     METRIC_SYS_UPTIME_IN_SECONDS,
//     sysinfo::System::uptime() as f64
// );

// let pid = get_current_pid();
// if pid.is_err() {
//     let err = pid.unwrap_err();
//     tracing::warn!("process metrics does not supported on current platform: {err}");
//     return Ok(());
// }
// let pid = pid.unwrap();
// if let Some(ps) = sys.process(pid) {
//     let cpu = ps.cpu_usage();
//     gauge!(METRIC_PROCESS_CPU_PERCENT, cpu as f64);

//     let mem = ps.memory() as f64 / sys.total_memory() as f64 * 100.0;
//     gauge!(METRIC_PROCESS_MEM_PERCENT, mem);

//     #[cfg(target_os = "linux")]
//     gauge!(METRIC_PROCESS_TASKS, ps.tasks().unwrap().len() as f64);

//     let disk = ps.disk_usage();
//     gauge!(METRIC_PROCESS_IO_READ_BYTES, disk.read_bytes as f64);
//     gauge!(METRIC_PROCESS_IO_WRITTEN_BYTES, disk.written_bytes as f64);

//     gauge!(TAOSX_PROCESS_UPTIME, ps.run_time() as f64);
// }
// Ok(())
// }

// impl Metrics {
//     pub fn init(self) -> anyhow::Result<PrometheusRecorder> {
//         let mut exporter = PrometheusBuilder::new();
//         let interval = self.interval();
//         let dur = Duration::from_secs(interval as u64);

//         // if let Some(listen) = self.listen {
//         //     exporter = exporter.with_http_listener(listen);
//         // }
//         let recorder = if let Some(push) = self.push {
//             let interval = self.push_interval.unwrap_or(dur);
//             exporter = exporter.with_push_gateway(push, interval, None, None)?;

//             let (recorder, exporter) = exporter.build()?;
//             tokio::spawn(exporter);
//             recorder
//         } else {
//             exporter.build_recorder()
//         };

//         // let recorder = exporter.build_recorder();
//         // process_metrics_init();
//         // std::thread::spawn(move || {
//         //     let mut sys = sysinfo::System::new_all();
//         //     loop {
//         //         let _ = process_metrics(&mut sys);
//         //         std::thread::sleep(dur);
//         //     }
//         // });
//         Ok(recorder)
//     }

//     fn interval(&self) -> u16 {
//         self.interval.unwrap_or(1)
//     }
// }
/// Metrics like node-exporter.
#[utoipa::path(
    responses(
        (status = 200, description = "Export all metrics", body = String),
    )
)]
#[get("/metrics")]
async fn metrics_exporter(handle: actix_web::web::Data<TaosXRecorderHandle>) -> impl Responder {
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

pub fn try_get_metrics_from_task_detail(task: &TaskDetail) -> Option<Arc<CoreMetrics>> {
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
        "taos" => try_get_metrics::<LegacyToTaosMetrics>(task_id),
        "tmq" => try_get_metrics::<TmqMetrics>(task_id),
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
        | "csv" => try_get_metrics::<IpcMetrics>(task_id),
        _ => None,
    }
}

pub fn get_task_metrics_string(status: &Status, metrics: Arc<CoreMetrics>) -> String {
    // 根据任务的状态判断任务是否正在运行。这里的正在运行的含义是：任务正在被 scheduler 执行。
    // 这里的 running 更准确的说是任务处于需要被计算运行时间的状态。
    let running = status == Status::Running
        || status == Status::Stopping
        || status == Status::Waiting
        || status == Status::Interrupted;
    let (common_metrics, json) = match metrics.as_ref() {
        CoreMetrics::Legacy(legacy_metrics) => (legacy_metrics.com(), legacy_metrics.to_json()),
        CoreMetrics::TMQ(tmq_metrics) => (tmq_metrics.com(), tmq_metrics.to_json()),
        CoreMetrics::IPC(ipc_metrics) => (ipc_metrics.com(), ipc_metrics.to_json()),
    };
    let mut map =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json.as_str()).unwrap();
    map.remove("task_id");
    map.remove("stable");
    map.remove("task_name");
    compute_total_avg_speed(common_metrics, &mut map);
    compute_avg_speed(common_metrics, &mut map, running);
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
