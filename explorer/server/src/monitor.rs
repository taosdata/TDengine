use clap::Parser;
use gethostname::gethostname;
use metrics::Label;
use metrics::gauge;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use sysinfo::ProcessRefreshKind;
use sysinfo::ProcessesToUpdate;
use taosx_metrics::TaosXRecorder;
use taosx_metrics::TaosXRecorderHandle;
use tracing::Instrument;
use tracing::instrument;

#[derive(Parser, Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct MonitorCfg {
    /// FQDN of taosKeeper service
    #[clap(long, env = "MONITOR_FQDN")]
    #[serde(rename = "fqdn")]
    pub monitor_fqdn: Option<String>,

    /// Port of taosKeeper service
    #[clap(long, env = "MONITOR_PORT", global = true, default_value = "6043")]
    #[serde(rename = "port")]
    pub monitor_port: u16,

    #[clap(
        long,
        env = "MONITOR_INTERVAL",
        global = true,
        default_value = "10",
        value_parser=less_than_10
    )]
    #[serde(rename = "interval")]
    pub monitor_interval: u64,
}

impl Default for MonitorCfg {
    fn default() -> Self {
        Self {
            monitor_fqdn: None,
            monitor_port: 6043,
            monitor_interval: 10,
        }
    }
}

fn less_than_10(s: &str) -> Result<u64, String> {
    let val = s.parse::<u64>().map_err(|e| format!("{e}"))?;
    let min = 1;
    let max = 10;
    if val > max {
        Err(format!("exceeds maximum of {max}"))
    } else if val < min {
        Err(format!("less than minimum of {min}"))
    } else {
        Ok(val)
    }
}

impl MonitorCfg {
    fn interval(&self) -> u64 {
        self.monitor_interval.max(1)
    }
}

#[derive(Debug, Clone)]
pub struct Monitor {
    pub cfg: MonitorCfg,
    pub endpoint: &'static str,
}

impl Monitor {
    pub fn new(cfg: MonitorCfg, port: u16) -> Self {
        let hostname = gethostname();
        let hostname = match hostname.to_str() {
            Some(hostname) => hostname.to_string(),
            None => {
                tracing::error!("gethostname error");
                "unknown".to_string()
            }
        };
        let id = format!("{}:{}", hostname, port);
        let endpoint = Box::leak(id.into_boxed_str());
        tracing::info!("explorer id for metrics: {}", endpoint);
        Self { cfg, endpoint }
    }

    #[instrument(skip_all)]
    pub fn init(&self) -> TaosXRecorderHandle {
        let monitor_interval = self.cfg.interval();
        let idle_timeout = Duration::from_secs(monitor_interval * 2);
        let recorder = TaosXRecorder::new(Some(idle_timeout));
        let recorder_handle: TaosXRecorderHandle = recorder.handle();
        let handle_clone = recorder_handle.clone();
        recorder.install();
        let endpoint = self.endpoint;
        tokio::spawn(
            async move {
                use sysinfo::*;
                tracing::info!("start update process metrics task");
                let duration = Duration::from_secs(monitor_interval);
                let mut interval = tokio::time::interval(duration);
                let kind = RefreshKind::nothing()
                    .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
                    .with_memory(MemoryRefreshKind::nothing().with_ram());
                let mut sys = System::new_with_specifics(kind);
                let process_id = match get_current_pid() {
                    Ok(process_id) => process_id,
                    Err(err) => {
                        tracing::error!(
                            "stop update process metrics task since get process id error: {err}"
                        );
                        return;
                    }
                };
                loop {
                    interval.tick().await;
                    let _ = process_metrics(&mut sys, kind, endpoint, process_id, monitor_interval)
                        .await;
                }
            }
            .in_current_span(),
        );
        if let Some(fqdn) = &self.cfg.monitor_fqdn {
            tracing::info!("monitor is enabled");
            let url = format!("http://{}:{}/general-metric", fqdn, self.cfg.monitor_port);
            tokio::spawn(
                async move {
                    tracing::info!("start send metrics task");
                    let exporter = TaosKeeperExporter { url: &url };
                    let mut interval = tokio::time::interval(Duration::from_secs(monitor_interval));
                    const MIN_BACKOFF: Duration = Duration::from_secs(2);
                    const MAX_BACKOFF: Duration = Duration::from_secs(300);
                    let mut backoff = MIN_BACKOFF;
                    loop {
                        interval.tick().await;
                        let snapshot: taosx_metrics::Snapshot = recorder_handle.snapshot();
                        let records = snapshot2records(snapshot);
                        let tables = records2tables(records);
                        let stables = grouptables2stable(tables);
                        if stables.is_empty() {
                            continue;
                        }
                        let body = stable2json(stables);
                        tracing::trace!("data send to taoskeeper: {}", &body);
                        if !exporter.push_taoskeeper(body).await {
                            tokio::time::sleep(backoff).await;
                            backoff *= 2;
                            if backoff > MAX_BACKOFF {
                                backoff = MAX_BACKOFF;
                            }
                        } else {
                            backoff = MIN_BACKOFF;
                        }
                    }
                }
                .in_current_span(),
            );
        }
        handle_clone
    }
}

pub async fn process_metrics(
    sys: &mut sysinfo::System,
    kind: sysinfo::RefreshKind,
    endpoint: &'static str,
    process_id: sysinfo::Pid,
    monitor_interval: u64,
) -> anyhow::Result<()> {
    sys.refresh_specifics(kind);
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[process_id]),
        false,
        ProcessRefreshKind::nothing()
            .with_cpu()
            .with_memory()
            .with_disk_usage()
            .with_tasks(),
    );
    let labels = [("stable", "explorer_sys"), ("endpoint", endpoint)];
    // system metrics
    let cpu_cores = sys.cpus().len() as f64;
    gauge!("sys_cpu_cores", &labels).set(cpu_cores);
    gauge!("sys_total_memory", &labels).set(sys.total_memory() as f64);
    gauge!("sys_used_memory", &labels).set(sys.used_memory() as f64);
    gauge!("sys_available_memory", &labels).set(sys.available_memory() as f64);
    // current process metrics
    gauge!("process_id", &labels).set(process_id.as_u32() as f64);
    if let Some(ps) = sys.process(process_id) {
        let cpu = ps.cpu_usage();
        gauge!("process_cpu_percent", &labels).set(cpu as f64 / cpu_cores);
        let mem = ps.memory() as f64 / sys.total_memory() as f64 * 100.0;
        gauge!("process_memory_percent", &labels).set(mem);
        let disk = ps.disk_usage();
        gauge!("process_disk_read_bytes", &labels)
            .set(disk.read_bytes as f64 / monitor_interval as f64);
        gauge!("process_disk_written_bytes", &labels)
            .set(disk.written_bytes as f64 / monitor_interval as f64);
        gauge!("process_uptime", &labels).set(ps.run_time() as f64);
    }
    Ok(())
}

struct TaosKeeperExporter<'a> {
    url: &'a str,
}

impl<'a> TaosKeeperExporter<'a> {
    pub async fn push_taoskeeper(&self, body: String) -> bool {
        let client = reqwest::Client::new();
        let result = client
            .post(self.url)
            .body(body)
            .timeout(Duration::from_secs(2))
            .send()
            .await;
        match result {
            Ok(res) => {
                if !res.status().is_success() {
                    tracing::error!("send metrics to taoskeeper failed: {:?}", res);
                    false
                } else {
                    true
                }
            }
            Err(err) => {
                tracing::error!("send metrics to taoskeeper failed: {:?}", err);
                false
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct Table {
    table_key: TableKey,
    metrics: Vec<Metric>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash)]
struct TableKey {
    stable: String,
    tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash)]
struct Tag {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct Metric {
    name: String,
    value: f64,
}

/// Record 对应 Snapshot 中的一条记录
struct Record {
    table: String,
    tags: Vec<Tag>,
    metric: Metric,
}

/// Stable 对应一个超级表
#[derive(Debug, Clone, Serialize, PartialEq)]
struct Stable {
    name: String,
    metric_groups: Vec<MetricGroup>,
}

/// MetricsGroup 对应一个子表
#[derive(Debug, Clone, Serialize, PartialEq)]
struct MetricGroup {
    tags: Vec<Tag>,
    metrics: Vec<Metric>,
}

/// 将属于同一子表的 metrics 聚合到一起
fn records2tables(vec: Vec<Record>) -> Vec<Table> {
    let mut tables: HashMap<TableKey, Table> = HashMap::new();
    for record in vec {
        let table_key = TableKey {
            stable: record.table.clone(),
            tags: record.tags.clone(),
        };
        let table = tables.entry(table_key).or_insert(Table {
            table_key: TableKey {
                stable: record.table,
                tags: record.tags,
            },
            metrics: Vec::new(),
        });
        table.metrics.push(record.metric);
    }
    tables.into_values().collect()
}

/// 将属于同一超级表的子表聚合到一起
fn grouptables2stable(vec: Vec<Table>) -> Vec<Stable> {
    let mut stables: HashMap<String, Stable> = HashMap::new();
    for table in vec {
        let stable = stables
            .entry(table.table_key.stable.clone())
            .or_insert(Stable {
                name: table.table_key.stable,
                metric_groups: Vec::new(),
            });
        let metrics_group = MetricGroup {
            tags: table.table_key.tags,
            metrics: table.metrics,
        };
        stable.metric_groups.push(metrics_group);
    }
    stables.into_values().collect()
}

/// 最终的 JSON 结构
fn stable2json(stables: Vec<Stable>) -> String {
    let ts = chrono::Utc::now().timestamp_millis();
    let json = json!([{
        "ts": ts.to_string(),
        "tables": stables,
    }]);
    serde_json::to_string(&json).unwrap()
}

fn snapshot2records(snapshot: taosx_metrics::Snapshot) -> Vec<Record> {
    let mut records: Vec<Record> = Vec::new();
    for (key, value) in snapshot.data() {
        let (key_name, labels) = key.into_parts();
        let table = get_table_from_labels(&labels);
        if table.is_none() {
            tracing::warn!("no stable in labels: {:?}", labels);
            continue;
        }
        let table = table.unwrap();
        let tags = labels2tags(labels);
        let metric = match value {
            taosx_metrics::DebugValue::Counter(v) => Metric {
                name: key_name.as_str().to_string(),
                value: v as f64,
            },
            taosx_metrics::DebugValue::Gauge(v) => Metric {
                name: key_name.as_str().to_string(),
                value: v,
            },
        };
        records.push(Record {
            table,
            tags,
            metric,
        });
    }
    records
}

/// 所有要写入 taoskeeper 的 metrics, 都要有 stable 这个 label
fn get_table_from_labels(labels: &Vec<Label>) -> Option<String> {
    for label in labels {
        if label.key() == "stable" {
            return Some(label.value().to_string());
        }
    }
    None
}

fn labels2tags(labels: Vec<Label>) -> Vec<Tag> {
    let mut tags: Vec<Tag> = Vec::new();
    for label in labels {
        if label.key() == "stable" {
            continue;
        }
        tags.push(Tag {
            name: label.key().to_string(),
            value: label.value().to_string(),
        });
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_monitor() {
        let args = MonitorCfg::parse_from(["explorer", "--monitor-fqdn", "localhost"]);
        assert_eq!(args.monitor_fqdn, Some("localhost".to_string()));
        assert_eq!(args.monitor_port, 6043);
        assert_eq!(args.monitor_interval, 10);
        assert_eq!(args.interval(), 10);

        unsafe {
            std::env::set_var("MONITOR_FQDN", "fake1");
        }
        unsafe {
            std::env::set_var("MONITOR_PORT", "6044");
        }
        unsafe {
            std::env::set_var("MONITOR_INTERVAL", "5");
        }
        let args = MonitorCfg::parse_from(["explorer"]);
        assert_eq!(args.monitor_fqdn, Some("fake1".to_string()));
        assert_eq!(args.monitor_port, 6044);
        assert_eq!(args.monitor_interval, 5);
        assert_eq!(args.interval(), 5);
        unsafe {
            std::env::remove_var("MONITOR_FQDN");
        }
        unsafe {
            std::env::remove_var("MONITOR_PORT");
        }
        unsafe {
            std::env::remove_var("MONITOR_INTERVAL");
        }

        let args: MonitorCfg = serde_json::from_str(
            r#"{
                "fqdn": "fake2",
                "port": 6045,
                "interval": 3
            }"#,
        )
        .unwrap();
        assert_eq!(args.monitor_fqdn, Some("fake2".to_string()));
        assert_eq!(args.monitor_port, 6045);
        assert_eq!(args.monitor_interval, 3);
        assert_eq!(args.interval(), 3);

        let args: MonitorCfg = serde_json::from_str(
            r#"{
                "fqdn": "fake3"
            }"#,
        )
        .unwrap();
        assert_eq!(args.monitor_fqdn, Some("fake3".to_string()));
        assert_eq!(args.monitor_port, 6043);
        assert_eq!(args.monitor_interval, 10);
        assert_eq!(args.interval(), 10);
        let args = MonitorCfg::parse_from([
            "explorer",
            "--monitor-fqdn",
            "localhost",
            "--monitor-port",
            "6043",
            "--monitor-interval",
            "2",
        ]);
        assert_eq!(args.monitor_fqdn, Some("localhost".to_string()));
        assert_eq!(args.monitor_port, 6043);
        assert_eq!(args.monitor_interval, 2);
        assert_eq!(args.interval(), 2);
        let monitor = Monitor::new(args.clone(), 6060);
        let handle = monitor.init();
        tokio::time::sleep(Duration::from_secs(5)).await;
        let a = handle.render();
        println!("{}", a);
        println!("{:?}", &handle.snapshot());
        let snapshot = handle.snapshot();
        assert!(!snapshot.data().is_empty());
    }
}
