use actix_web::{get, Responder};
use metrics::{describe_gauge, gauge, register_gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct Metrics {
    push: Option<String>,
    push_interval: Option<Duration>,
    interval: Option<u16>,
}

pub fn process_metrics_init() {
    register_gauge!("taosx_sys_cpus");
    describe_gauge!("taosx_sys_cpus", "number of cpus");

    register_gauge!("taosx_process_cpu_percent");
    describe_gauge!("taosx_process_cpu_percent", "CPU percent of the process");

    register_gauge!("taosx_process_io_read_bytes");
    describe_gauge!(
        "taosx_process_io_read_bytes",
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

    gauge!("taosx_sys_cpus", sys.cpus().len() as f64);
    gauge!("taosx_sys_cpus", sys.cpus().len() as f64);
    gauge!("taosx_sys_total_memory", sys.total_memory() as f64);
    gauge!("taosx_sys_used_memory", sys.used_memory() as f64);
    gauge!("taosx_sys_free_memory", sys.free_memory() as f64);
    gauge!("taosx_sys_available_memory", sys.available_memory() as f64);
    gauge!("taosx_sys_uptime_in_seconds", sys.uptime() as f64);

    let pid = get_current_pid();
    if pid.is_err() {
        let err = pid.unwrap_err();
        log::warn!("process metrics does not supported on current platform: {err}");
        return Ok(());
    }
    let pid = pid.unwrap();
    if let Some(ps) = sys.process(pid) {
        let cpu = ps.cpu_usage();
        gauge!("taosx_process_cpu_percent", cpu as f64);

        let mem = ps.memory() as f64 / sys.total_memory() as f64 * 100.0;
        gauge!("taosx_process_mem_percent", mem);

        #[cfg(target_os = "linux")]
        gauge!("taosx_process_tasks", ps.tasks.len() as f64);

        let disk = ps.disk_usage();
        gauge!("taosx_process_io_read_bytes", disk.read_bytes as f64);
        gauge!("taosx_process_io_written_bytes", disk.written_bytes as f64);

        gauge!("taosx_process_uptime", ps.run_time() as f64);
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
        (status = 200, description = "Task found from storage", body = String),
    )
)]
#[get("/metrics")]
async fn metrics_exporter(handle: actix_web::web::Data<PrometheusHandle>) -> impl Responder {
    let output = handle.render();
    output
}
