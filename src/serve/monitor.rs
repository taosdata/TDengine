use anyhow::Ok;
use clap::Parser;
use metrics::gauge;
use serde::Deserialize;
use serde::Serialize;
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
}

impl Monitor {
    pub fn new(cfg: MonitorCfg) -> Self {
        Self { cfg }
    }

    #[instrument(skip_all)]
    pub fn init(&self) -> TaosXRecorderHandle {
        let monitor_interval = self.cfg.interval;
        let idle_timeout = Duration::from_secs(monitor_interval * 2);
        let recorder = TaosXRecorder::new(Some(idle_timeout));
        let recorder_handle: TaosXRecorderHandle = recorder.handle();
        let handle_clone = recorder_handle.clone();
        recorder.install();
        // Update metrics
        tokio::spawn(async move {
            tracing::info!("start update metrics task");
            let mut interval = tokio::time::interval(Duration::from_secs(monitor_interval));
            let mut sys = sysinfo::System::new_all();
            loop {
                interval.tick().await;
                let _ = update_metrics(&mut sys);
            }
        });

        if let Some(_fqdn) = &self.cfg.fqdn {
            tracing::info!("nonitor is enabled");
            // Sent metrics to taosKeeper
            tokio::spawn(async move {
                tracing::info!("start send metrics task");
                let mut interval = tokio::time::interval(Duration::from_secs(monitor_interval));
                loop {
                    interval.tick().await;
                    let _ = send_metrics_to_taoskeeper(&recorder_handle);
                }
            });
        }
        handle_clone
    }
}

pub const METRIC_SYS_CPUS: &str = "taosx_sys_cpus";
pub const METRIC_SYS_TOTAL_MEMORY: &str = "taosx_sys_total_memory";
pub const METRIC_SYS_USED_MEMORY: &str = "taosx_sys_used_memory";
pub const METRIC_SYS_AVAILABLE_MEMORY: &str = "taosx_sys_available_memory";

pub fn update_metrics(sys: &mut sysinfo::System) -> anyhow::Result<()> {
    tracing::info!("update_metrics");
    sys.refresh_all();

    gauge!(METRIC_SYS_CPUS, "lable1" => "value1").set(sys.cpus().len() as f64); // test
    gauge!(METRIC_SYS_TOTAL_MEMORY, "label2" => "value2").set(sys.total_memory() as f64); // test
    gauge!(METRIC_SYS_USED_MEMORY).set(sys.used_memory() as f64);
    gauge!(METRIC_SYS_AVAILABLE_MEMORY).set(sys.available_memory() as f64);
    Ok(())
}

pub fn send_metrics_to_taoskeeper(_recorder_handle: &TaosXRecorderHandle) -> anyhow::Result<()> {
    tracing::info!("send_metrics_to_taoskeeper");
    // TODO: send metrics to taosKeeper
    // let snapshot = recorder_handle.snapshot();
    // println!("{:?}", snapshot);
    Ok(())
}
