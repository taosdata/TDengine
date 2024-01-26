use anyhow::Ok;
use clap::Parser;
use gethostname::gethostname;
use metrics::gauge;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use taosx_metrics::TaosXRecorder;
use taosx_metrics::TaosXRecorderHandle;
use tracing::instrument;

#[derive(Parser, Debug, Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct MonitorCfg {
    /// FQDN of taosKeeper service
    #[clap(long = "monitor-fqdn", env = "MONITOR_FQDN")]
    fqdn: Option<String>,

    /// Port of taosKeeper service
    #[clap(
        long = "monitor-port",
        env = "MONITOR_PORT",
        global = true,
        default_value = "6043"
    )]
    port: u16,

    /// Interval(in second) for reporting metrics to taosKeeper. default to 30s.
    #[clap(
        long = "monitor-interval",
        env = "MONITOR_INTERVAL",
        global = true,
        default_value = "30"
    )]
    interval: u64,
}

impl MonitorCfg {
    pub fn merge_from(&mut self, other: &MonitorCfg) {
        if other.fqdn.is_some() {
            self.fqdn = other.fqdn.clone();
        }
        if other.port != 0 {
            self.port = other.port;
        }
        if other.interval != 0 {
            self.interval = other.interval;
        }
    }

    pub fn as_map(&self) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        if let Some(fqdn) = &self.fqdn {
            map.insert("fqdn".to_string(), fqdn.clone());
        }
        map.insert("port".to_string(), self.port.to_string());
        map.insert("interval".to_string(), self.interval.to_string());
        map
    }
}

pub struct Monitor {
    cfg: MonitorCfg,
    taosx_id: &'static str,
}

impl Monitor {
    pub fn new(cfg: MonitorCfg, taosx_port: &str) -> Self {
        let hostname = gethostname();
        let hostname = match hostname.to_str() {
            Some(hostname) => hostname.to_string(),
            None => {
                tracing::error!("gethostname error");
                "unknown".to_string()
            }
        };
        let taosx_id = hostname.to_string() + ":" + taosx_port;
        // make taosx_id static
        let taosx_id = Box::leak(taosx_id.into_boxed_str());
        tracing::info!("taosx_id: {}", taosx_id);
        Self { cfg, taosx_id }
    }

    #[instrument(skip_all)]
    pub fn init(&self) -> TaosXRecorderHandle {
        let monitor_interval = self.cfg.interval;
        let idle_timeout = Duration::from_secs(monitor_interval * 2);
        let recorder = TaosXRecorder::new(Some(idle_timeout));
        let recorder_handle: TaosXRecorderHandle = recorder.handle();
        let handle_clone = recorder_handle.clone();
        recorder.install();
        let taosx_id = self.taosx_id;
        tokio::spawn(async move {
            use sysinfo::*;
            tracing::info!("start update process metrics task");
            let mut interval = tokio::time::interval(Duration::from_secs(monitor_interval));
            let mut sys = System::new_all();
            let process_id = get_current_pid();
            if process_id.is_err() {
                let err = process_id.unwrap_err();
                tracing::error!(
                    "stop update process metrics task since get process id error: {err}"
                );
                return;
            }
            let process_id = process_id.unwrap();
            loop {
                interval.tick().await;
                let _ = process_metrics(&mut sys, taosx_id, process_id);
            }
        });

        if let Some(fqdn) = &self.cfg.fqdn {
            tracing::info!("nonitor is enabled");
            let url = format!("http://{}:{}/general-metric", fqdn, self.cfg.port);
            tokio::spawn(async move {
                tracing::info!("start send metrics task");
                let exporter = TaosKeeperExporter { url: &url };
                let mut interval = tokio::time::interval(Duration::from_secs(monitor_interval));
                loop {
                    interval.tick().await;
                    let body = prepare_data_to_taoskeeper(&recorder_handle);
                    exporter.push_taoskeeper(body).await;
                }
            });
        }
        handle_clone
    }
}

pub fn process_metrics(
    sys: &mut sysinfo::System,
    taosx_id: &'static str,
    process_id: sysinfo::Pid,
) -> anyhow::Result<()> {
    sys.refresh_all();
    let labels = [("stable", "taosx_sys"), ("taosx_id", taosx_id)];
    // sys metrics
    gauge!("sys_cpu_cores", &labels).set(sys.cpus().len() as f64);
    gauge!("sys_total_memory", &labels).set(sys.total_memory() as f64);
    gauge!("sys_used_memory", &labels).set(sys.used_memory() as f64);
    gauge!("sys_available_memory", &labels).set(sys.available_memory() as f64);
    // process metrics
    gauge!("process_id", &labels).set(process_id.as_u32() as f64);
    if let Some(ps) = sys.process(process_id) {
        let cpu = ps.cpu_usage();
        gauge!("process_cpu_percent", &labels).set(cpu as f64);
        let mem = ps.memory() as f64 / sys.total_memory() as f64 * 100.0;
        gauge!("process_memory_percent", &labels).set(mem);
        let disk = ps.disk_usage();
        gauge!("process_disk_read_bytes", &labels).set(disk.read_bytes as f64);
        gauge!("process_disk_written_bytes", &labels).set(disk.written_bytes as f64);
        gauge!("process_uptime", &labels).set(ps.run_time() as f64);
    }
    Ok(())
}

pub fn prepare_data_to_taoskeeper(_recorder_handle: &TaosXRecorderHandle) -> String {
    tracing::info!("send_metrics_to_taoskeeper");
    let data: HashMap<&str, String> = HashMap::new();
    serde_json::to_string(&data).unwrap()
}

struct TaosKeeperExporter<'a> {
    url: &'a str,
}

impl<'a> TaosKeeperExporter<'a> {
    pub async fn push_taoskeeper(&self, body: String) {
        let client = reqwest::Client::new();
        let res = client.post(self.url).body(body).send().await;
        if let Err(err) = res {
            tracing::error!("push_taoskeeper error: {}", err);
        }
    }
}
