use std::sync::atomic::Ordering::SeqCst;

use super::TaskControllerRef;
use super::scheduler::runner::MultiIndexTaskJobMap;
use clap::Parser;
use dashmap::DashMap;
use metrics::Label;
use metrics::gauge;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::ProcessRefreshKind;
use sysinfo::ProcessesToUpdate;
use taos::taos_query::tmq::Assignment;
use taosx_core::core_metrics::{self, CommonMetrics, TaskMetrics};
use taosx_core::legacy_metric::LegacyToTaosMetrics;
use taosx_core::sink::ipc_metric::IpcMetrics;
use taosx_core::tmq::tmq_metric::TmqMetrics;
use taosx_core::utils::monitor::update_sub_connector_process_metrics;
use taosx_metrics::TaosXRecorder;
use taosx_metrics::TaosXRecorderHandle;
use tokio::sync::RwLock;
use tracing::Instrument;
use tracing::instrument;

#[derive(Parser, Debug, Deserialize, Serialize, Default, Clone)]
#[serde(default)]
pub struct MonitorCfg {
    /// FQDN of taosKeeper service
    #[clap(long = "monitor-fqdn", env = "MONITOR_FQDN")]
    pub fqdn: Option<String>,

    /// Port of taosKeeper service
    #[clap(
        long = "monitor-port",
        env = "MONITOR_PORT",
        global = true,
        default_value = "6043"
    )]
    pub port: u16,

    #[clap(
        long = "monitor-interval",
        env = "MONITOR_INTERVAL",
        global = true,
        default_value = "10",
        value_parser=less_than_10
    )]
    pub interval: u64,
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
    pub fn merge_from(&mut self, other: &MonitorCfg) {
        if other.fqdn.is_some() {
            self.fqdn = other.fqdn.clone();
        }
        if other.port != 0 {
            self.port = other.port;
        }
        if other.interval >= 1 && other.interval <= 10 {
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

#[derive(Debug, Clone)]
pub struct Monitor {
    pub cfg: MonitorCfg,
    pub taosx_id: String,
    tasks: Arc<RwLock<MultiIndexTaskJobMap>>,
}

impl Monitor {
    pub fn new(cfg: MonitorCfg, taosx_id: String, controller: TaskControllerRef) -> Self {
        let tasks = controller.scheduler.tasks.clone();
        Self {
            cfg,
            taosx_id,
            tasks,
        }
    }

    #[instrument(skip_all)]
    pub fn init(&self) -> TaosXRecorderHandle {
        let monitor_interval = self.cfg.interval;
        let idle_timeout = Duration::from_secs(monitor_interval * 2);
        let recorder = TaosXRecorder::new(Some(idle_timeout));
        let recorder_handle: TaosXRecorderHandle = recorder.handle();
        let handle_clone = recorder_handle.clone();
        recorder.install();
        let taosx_id = self.taosx_id.clone();
        tokio::spawn(
            async move {
                use sysinfo::*;
                tracing::info!("start update process metrics task");
                let duration = Duration::from_secs(monitor_interval);
                let mut interval = tokio::time::interval(duration);
                let kind = sysinfo::RefreshKind::nothing()
                    .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
                    .with_memory(MemoryRefreshKind::nothing().with_ram());
                let mut sys = System::new_with_specifics(kind);
                let process_id = match get_current_pid() {
                    Ok(pid) => pid,
                    Err(err) => {
                        tracing::error!(
                            "stop update process metrics task since get process id error: {err}"
                        );
                        return;
                    }
                };
                loop {
                    interval.tick().await;
                    process_metrics(&mut sys, kind, &taosx_id, process_id, monitor_interval).await;
                }
            }
            .in_current_span(),
        );
        let tasks = self.tasks.clone();
        if let Some(fqdn) = &self.cfg.fqdn {
            tracing::info!("monitor is enabled");
            let url = format!("http://{}:{}/general-metric", fqdn, self.cfg.port);
            let taosx_id = self.taosx_id.clone();
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
                        let mut tables = records2tables(records);
                        add_task_metrics_tables(&tasks, &mut tables, &taosx_id).await;
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

/// 遍历 scheduler 中的所有 task, 将 task 的 metric 转换成 Table, 并加入 tables 中
async fn add_task_metrics_tables(
    tasks: &Arc<RwLock<MultiIndexTaskJobMap>>,
    tables: &mut Vec<Table>,
    taosx_id: &str,
) {
    for (_, task) in tasks.read().await.iter() {
        let (task_id, job_id) = task.task_job_id;

        let metrics = core_metrics::get_metrics(task_id, job_id);
        match metrics {
            Some(metrics) => match metrics.as_ref() {
                core_metrics::CoreMetrics::Legacy(metrics) => {
                    tables.push(metrics.gen_table(taosx_id))
                }
                core_metrics::CoreMetrics::TMQ(metrics) => {
                    tables.push(metrics.gen_table(taosx_id));
                    if !metrics.progress.is_empty() {
                        add_task_progress_tables(&metrics.progress, taosx_id, task_id, tables);
                    }
                }
                core_metrics::CoreMetrics::IPC(metrics) => tables.push(metrics.gen_table(taosx_id)),
            },
            None => {
                tracing::debug!("metrics for task {} is not initialized", task_id);
                continue;
            }
        }
    }
}

/// 将 TMQ task 的 progress 转换成 Table
fn add_task_progress_tables(
    progress: &DashMap<String, DashMap<i32, Assignment>>,
    taosx_id: &str,
    task_id: i64,
    tables: &mut Vec<Table>,
) {
    for entry in progress.iter() {
        let topic = entry.key().clone();
        let topic_progress = entry.value();
        for entry in topic_progress.iter() {
            let assignment = entry.value();
            let table_key = TableKey {
                stable: "taosx_task_progress".to_string(),
                tags: vec![
                    Tag {
                        name: "taosx_id".to_string(),
                        value: taosx_id.to_string(),
                    },
                    Tag {
                        name: "task_id".to_string(),
                        value: task_id.to_string(),
                    },
                    Tag {
                        name: "topic".to_string(),
                        value: topic.clone(),
                    },
                    Tag {
                        name: "vgroup".to_string(),
                        value: assignment.vgroup_id().to_string(),
                    },
                ],
            };
            let metrics = vec![
                Metric {
                    name: "offset".to_string(),
                    value: assignment.current_offset() as f64,
                },
                Metric {
                    name: "latest".to_string(),
                    value: assignment.end() as f64,
                },
            ];
            let table = Table { table_key, metrics };
            tables.push(table);
        }
    }
}

pub async fn process_metrics(
    sys: &mut sysinfo::System,
    kind: sysinfo::RefreshKind,
    taosx_id: &str,
    process_id: sysinfo::Pid,
    monitor_interval: u64,
) {
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
    let labels = [
        ("stable", "taosx_sys".to_string()),
        ("taosx_id", taosx_id.to_string()),
    ];
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
    // connector process metrics
    update_sub_connector_process_metrics(
        sys,
        taosx_id.to_string(),
        process_id,
        monitor_interval as f64,
        cpu_cores,
    )
    .await;
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

trait IntoTags {
    fn gen_tags(&self, taosx_id: String) -> Vec<Tag>;
}

impl IntoTags for CommonMetrics {
    fn gen_tags(&self, taosx_id: String) -> Vec<Tag> {
        vec![
            Tag {
                name: "taosx_id".to_string(),
                value: taosx_id,
            },
            Tag {
                name: "task_id".to_string(),
                value: self.task_id.to_string(),
            },
        ]
    }
}

trait IntoMetrics {
    fn gen_metrics(&self) -> Vec<Metric>;
}

macro_rules! value2metric {
    ($name:expr, $value:expr) => {
        Metric {
            name: $name.to_string(),
            value: $value as f64,
        }
    };
}

impl IntoMetrics for CommonMetrics {
    fn gen_metrics(&self) -> Vec<Metric> {
        vec![
            value2metric!("start_time", self.start_time.get()),
            value2metric!("written_rows", self.written_rows.load(SeqCst)),
            value2metric!("written_points", self.written_points.load(SeqCst)),
            value2metric!("execute_time", self.execute_time.load(SeqCst)),
            value2metric!("total_written_rows", self.total_written_rows.load(SeqCst)),
            value2metric!(
                "total_written_points",
                self.total_written_points.load(SeqCst)
            ),
            value2metric!("total_execute_time", self.total_execute_time.load(SeqCst)),
            value2metric!("received_messages", self.processed_messages.load(SeqCst)),
            value2metric!("processed_messages", self.processed_messages.load(SeqCst)),
        ]
    }
}

impl IntoMetrics for LegacyToTaosMetrics {
    fn gen_metrics(&self) -> Vec<Metric> {
        let mut vec = self.com.gen_metrics();
        vec.push(value2metric!(
            "read_concurrency",
            self.read_concurrency.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_stables",
            self.total_stables.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_tables",
            self.total_tables.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_finished_tables",
            self.total_finished_tables.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_success_blocks",
            self.total_success_blocks.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_updated_tags",
            self.total_updated_tags.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_created_tables",
            self.total_created_tables.load(SeqCst)
        ));
        vec.push(value2metric!(
            "finished_tables",
            self.finished_tables.load(SeqCst)
        ));
        vec.push(value2metric!(
            "success_blocks",
            self.success_blocks.load(SeqCst)
        ));
        vec.push(value2metric!(
            "updated_tags",
            self.updated_tags.load(SeqCst)
        ));
        vec.push(value2metric!(
            "created_tables",
            self.created_tables.load(SeqCst)
        ));
        vec
    }
}

impl IntoMetrics for TmqMetrics {
    fn gen_metrics(&self) -> Vec<Metric> {
        let mut vec = self.com.gen_metrics();
        vec.push(value2metric!(
            "total_messages",
            self.total_messages.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_messages_bytes",
            self.total_messages_bytes.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_messages_of_meta",
            self.total_messages_of_meta.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_messages_of_data",
            self.total_messages_of_data.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_write_raw_fails",
            self.total_write_raw_fails.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_success_blocks",
            self.total_success_blocks.load(SeqCst)
        ));
        vec.push(value2metric!("messages", self.messages.load(SeqCst)));
        vec.push(value2metric!(
            "messages_bytes",
            self.messages_bytes.load(SeqCst)
        ));
        vec.push(value2metric!(
            "messages_of_meta",
            self.messages_of_meta.load(SeqCst)
        ));
        vec.push(value2metric!(
            "messages_of_data",
            self.messages_of_data.load(SeqCst)
        ));
        vec.push(value2metric!(
            "write_raw_fails",
            self.write_raw_fails.load(SeqCst)
        ));
        vec.push(value2metric!(
            "success_blocks",
            self.success_blocks.load(SeqCst)
        ));
        vec.push(value2metric!("commits", self.commits.load(SeqCst)));
        vec.push(value2metric!("consumers", self.consumers.load(SeqCst)));
        vec.push(value2metric!(
            "total_consume_cost_ms",
            self.total_consume_cost_ms.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_write_cost_ms",
            self.total_write_cost_ms.load(SeqCst)
        ));

        vec
    }
}

impl IntoMetrics for IpcMetrics {
    fn gen_metrics(&self) -> Vec<Metric> {
        let mut vec = self.com.gen_metrics();
        vec.push(value2metric!(
            "total_received_batches",
            self.total_received_batches.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_processed_batches",
            self.total_processed_batches.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_processed_rows",
            self.total_processed_rows.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_inserted_sqls",
            self.total_inserted_sqls.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_failed_sqls",
            self.total_failed_sqls.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_created_stables",
            self.total_created_stables.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_created_tables",
            self.total_created_tables.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_failed_rows",
            self.total_failed_rows.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_drained_rows",
            self.total_drained_rows.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_failed_points",
            self.total_failed_points.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_written_raw_blocks",
            self.total_written_raw_blocks.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_failed_raw_blocks",
            self.total_failed_raw_blocks.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_failed_batches",
            self.total_failed_batches.load(SeqCst)
        ));
        vec.push(value2metric!(
            "total_archived_rows",
            self.total_archived_rows.load(SeqCst)
        ));
        vec.push(value2metric!(
            "received_batches",
            self.received_batches.load(SeqCst)
        ));
        vec.push(value2metric!(
            "processed_batches",
            self.processed_batches.load(SeqCst)
        ));
        vec.push(value2metric!(
            "failed_batches",
            self.failed_batches.load(SeqCst)
        ));
        vec.push(value2metric!(
            "processed_rows",
            self.processed_rows.load(SeqCst)
        ));
        vec.push(value2metric!(
            "inserted_sqls",
            self.inserted_sqls.load(SeqCst)
        ));
        vec.push(value2metric!("failed_sqls", self.failed_sqls.load(SeqCst)));
        vec.push(value2metric!(
            "created_stables",
            self.created_stables.load(SeqCst)
        ));
        vec.push(value2metric!(
            "created_tables",
            self.created_tables.load(SeqCst)
        ));
        vec.push(value2metric!("failed_rows", self.failed_rows.load(SeqCst)));
        vec.push(value2metric!(
            "failed_points",
            self.failed_points.load(SeqCst)
        ));
        vec.push(value2metric!(
            "written_raw_blocks",
            self.written_raw_blocks.load(SeqCst)
        ));
        vec.push(value2metric!(
            "failed_raw_blocks",
            self.failed_raw_blocks.load(SeqCst)
        ));
        vec.push(value2metric!(
            "archived_rows",
            self.archived_rows.load(SeqCst)
        ));

        let guard = scc::ebr::Guard::new();
        let iter = self.extras.iter(&guard);
        for (key, value) in iter {
            vec.push(Metric {
                name: key.to_string(),
                value: *value as f64,
            });
        }

        vec
    }
}

trait IntoTable {
    fn gen_table(&self, taosx_id: &str) -> Table;
}

impl<T> IntoTable for T
where
    T: IntoMetrics + TaskMetrics,
{
    fn gen_table(&self, taosx_id: &str) -> Table {
        Table {
            table_key: TableKey {
                stable: self.com().stable.clone(),
                tags: self.com().gen_tags(taosx_id.to_string()),
            },
            metrics: self.gen_metrics(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitor_cfg_default() {
        let cfg = MonitorCfg::default();
        // Default from trait is 0, CLI default is 6043
        assert_eq!(cfg.port, 0u16);
        assert!(cfg.fqdn.is_none());
    }

    #[test]
    fn test_monitor_cfg_serialization() {
        let cfg = MonitorCfg {
            fqdn: Some("localhost".to_string()),
            port: 6043,
            ..Default::default()
        };
        let json = serde_json::to_string(&cfg);
        assert!(json.is_ok());
    }

    #[test]
    fn test_monitor_cfg_deserialization() {
        let json = r#"{"fqdn":"localhost","port":6043}"#;
        let cfg: Result<MonitorCfg, _> = serde_json::from_str(json);
        assert!(cfg.is_ok());
        if let Ok(c) = cfg {
            assert_eq!(c.port, 6043);
        }
    }

    #[test]
    fn test_table_key_creation() {
        let table_key = TableKey {
            stable: "test_stable".to_string(),
            tags: vec![],
        };
        assert_eq!(table_key.stable, "test_stable");
        assert!(table_key.tags.is_empty());
    }

    #[test]
    fn test_metric_structure() {
        let metric = Metric {
            name: "test_metric".to_string(),
            value: 42.5,
        };
        assert_eq!(metric.name, "test_metric");
        assert_eq!(metric.value, 42.5);
    }

    #[test]
    fn test_table_structure() {
        let table = Table {
            table_key: TableKey {
                stable: "test".to_string(),
                tags: vec![],
            },
            metrics: vec![],
        };
        assert_eq!(table.table_key.stable, "test");
        assert!(table.metrics.is_empty());
    }

    #[test]
    fn test_monitor_cfg_clone() {
        let cfg = MonitorCfg {
            fqdn: Some("test".to_string()),
            port: 9999,
            ..Default::default()
        };
        let cloned = cfg.clone();
        assert_eq!(cfg.port, cloned.port);
        assert_eq!(cfg.fqdn, cloned.fqdn);
    }

    #[test]
    fn test_monitor_cfg_with_custom_port() {
        let cfg = MonitorCfg {
            port: 9999,
            ..Default::default()
        };
        assert_eq!(cfg.port, 9999);
    }

    #[test]
    fn test_process_refresh_kind() {
        let _kind = ProcessRefreshKind::nothing();
        // Test that ProcessRefreshKind can be created
    }

    #[test]
    fn test_dashboard_url_generation() {
        let monitor_cfg = MonitorCfg {
            fqdn: Some("localhost".to_string()),
            port: 6043,
            ..Default::default()
        };
        // Verify the URL can be constructed
        let url = format!(
            "http://{}:{}",
            monitor_cfg
                .fqdn
                .as_ref()
                .unwrap_or(&"localhost".to_string()),
            monitor_cfg.port
        );
        assert!(url.contains("localhost"));
        assert!(url.contains("6043"));
    }

    #[test]
    fn test_table_key_with_tags() {
        let tags = vec![];
        let table_key = TableKey {
            stable: "test".to_string(),
            tags,
        };
        assert_eq!(table_key.tags.len(), 0);
    }

    #[test]
    fn test_atomic_operations_in_metrics() {
        use std::sync::atomic::AtomicI64;
        let atomic = AtomicI64::new(0);
        assert_eq!(atomic.load(SeqCst), 0);
        atomic.store(100, SeqCst);
        assert_eq!(atomic.load(SeqCst), 100);
    }

    #[test]
    fn test_metric_collection() {
        let metrics = [
            Metric {
                name: "metric1".to_string(),
                value: 10.0,
            },
            Metric {
                name: "metric2".to_string(),
                value: 20.0,
            },
        ];
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].value, 10.0);
    }
}
