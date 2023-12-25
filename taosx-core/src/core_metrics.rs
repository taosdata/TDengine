//! Crate level metrics related data structures and functions.
//! Define metrics data structure for each supported datasource.
//! And supply a global accessible map to store all metrics data.

use crate::legacy::legacy_metric::LegacyToTaosMetrics;
use crate::plugins::sink::ipc_metric::IpcMetrics;
use crate::tmq::tmq_metric::TmqMetrics;
use crate::utils::metrics_db::MetricsDb;
use lazy_static::lazy_static;
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use serde_json;
use std::cell::Cell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use taos::Dsn;
use tokio::sync::oneshot;
use tracing::Instrument;

/// MetricsType is an enum to store all supported metrics data structure.
pub enum CoreMetrics {
    Legacy(LegacyToTaosMetrics),
    TMQ(TmqMetrics),
    IPC(IpcMetrics),
}

impl CoreMetrics {
    /// Unwrap this enum to get the LegacyToTaosMetrics.
    pub fn legacy(&self) -> &LegacyToTaosMetrics {
        match self {
            CoreMetrics::Legacy(legacy) => legacy,
            _ => panic!("metrics type not match"),
        }
    }

    /// Unwrap this enum to get the TMQMetrics.
    pub fn tmq(&self) -> &TmqMetrics {
        match self {
            CoreMetrics::TMQ(tmq) => tmq,
            _ => panic!("metrics type not match"),
        }
    }

    pub fn ipc(&self) -> &IpcMetrics {
        match self {
            CoreMetrics::IPC(ipc) => ipc,
            _ => panic!("metrics type not match"),
        }
    }
}

/// CommonMetrics is a data structure to store metrics that are common to all task types.
#[derive(Serialize, Deserialize, Debug)]
pub struct CommonMetrics {
    pub task_id: i64,
    pub start_time: TaskStartTime,
    pub total_execute_time: AtomicU64,
    pub total_written_rows: AtomicU64,
    pub total_written_points: AtomicU64,
    #[serde(skip)]
    pub last_persist_time: LastPersistTime,
    pub execute_time: AtomicU64,
    pub written_rows: AtomicU64,
    pub written_points: AtomicU64,
}

impl Default for CommonMetrics {
    fn default() -> Self {
        Self {
            task_id: -1,
            start_time: TaskStartTime::default(),
            total_execute_time: AtomicU64::new(0),
            total_written_rows: AtomicU64::new(0),
            total_written_points: AtomicU64::new(0),
            last_persist_time: LastPersistTime::default(),
            execute_time: AtomicU64::new(0),
            written_rows: AtomicU64::new(0),
            written_points: AtomicU64::new(0),
        }
    }
}

impl CommonMetrics {
    pub fn new(task_id: i64) -> Self {
        Self {
            task_id,
            ..Default::default()
        }
    }

    /// 更新总执行时间和本次运行时间
    pub fn update_execute_time(&self) {
        let elapsed = self.last_persist_time.elapsed_millis();
        self.total_execute_time.fetch_add(elapsed, SeqCst);
        self.execute_time.fetch_add(elapsed, SeqCst);
        self.last_persist_time.reset();
    }

    pub fn reset(&self) {
        self.start_time.reset();
        self.written_rows.store(0, SeqCst);
        self.written_points.store(0, SeqCst);
        self.execute_time.store(0, SeqCst);
    }
}

pub trait TaosXMetrics: Into<CoreMetrics> + Serialize {
    /// Reset run level metrics
    fn reset(&self);

    /// Return CommonMetrics
    fn com(&self) -> &CommonMetrics;

    /// Convert metrics to json string.
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// Restore metrics from json string.
    fn from_json(json: &str) -> Option<Self>;

    /// Save metrics to database
    fn save(&self) -> anyhow::Result<()> {
        self.com().update_execute_time();
        let task_id = self.com().task_id.to_string();
        let db = MetricsDb::new(task_id.as_str())?;
        db.set(self.to_json().as_str())
    }

    #[inline]
    fn total_execute_time(&self) -> u64 {
        self.com().total_execute_time.load(SeqCst)
    }

    #[inline]
    fn total_written_rows(&self) -> u64 {
        self.com().total_written_rows.load(SeqCst)
    }

    #[inline]
    fn written_rows(&self) -> u64 {
        self.com().written_rows.load(SeqCst)
    }

    #[inline]
    fn total_written_points(&self) -> u64 {
        self.com().total_written_points.load(SeqCst)
    }

    #[inline]
    fn written_points(&self) -> u64 {
        self.com().written_points.load(SeqCst)
    }

    #[inline]
    fn start_time(&self) -> i64 {
        self.com().start_time.get()
    }

    #[inline]
    fn add_written_rows(&self, n: u64) {
        self.com().total_written_rows.fetch_add(n, SeqCst);
        self.com().written_rows.fetch_add(n, SeqCst);
    }

    #[inline]
    fn add_written_points(&self, n: u64) {
        self.com().total_written_points.fetch_add(n, SeqCst);
        self.com().written_points.fetch_add(n, SeqCst);
    }
}

lazy_static! {
    pub static ref GLOBAL_METRICS: Mutex<HashMap<i64, Arc<CoreMetrics>>> =
        Mutex::new(HashMap::new());
}

/// Try to get metrics from global metrics map.
pub fn get_metrics(task_id: i64) -> Option<Arc<CoreMetrics>> {
    let metrics = GLOBAL_METRICS.lock().unwrap();
    metrics.get(&task_id).cloned()
}

/// Get metrics of a task after it's metrics has been initialized,
/// so that it's metrics must exist in the global map.
#[inline]
pub fn get_metrics_arc(task_id: Option<String>) -> Arc<CoreMetrics> {
    let task_id = match task_id {
        Some(id) => id.parse::<i64>().unwrap(),
        _ => -1,
    };
    get_metrics(task_id).expect("metrics not found")
}

pub fn get_metrics_arc_from_i64(task_id: Option<i64>) -> Arc<CoreMetrics> {
    let task_id = match task_id {
        Some(id) => id,
        _ => -1,
    };
    get_metrics(task_id).expect("metrics not found")
}

/// Try to load metrics from persistence.
pub fn load_metrics<T: TaosXMetrics>(task_id: &str) -> Option<T> {
    let path_buf = MetricsDb::db_dir(task_id);
    let db_path = Path::new(&path_buf);
    if db_path.exists() {
        match MetricsDb::from_path(db_path) {
            Ok(db) => match db.get() {
                Ok(json) => {
                    if let Some(json) = json {
                        let j = json.as_str();
                        T::from_json(j)
                    } else {
                        tracing::error!("get metrics from db return None {}", db_path.display());
                        None
                    }
                }
                Err(err) => {
                    tracing::error!("get metrics from db failed: {:?}", &err);
                    None
                }
            },
            Err(err) => {
                tracing::error!("load metrics from db failed: {:?}", &err);
                None
            }
        }
    } else {
        None
    }
}

pub fn get_task_metrics_string(running: bool, metrics: Arc<CoreMetrics>) -> String {
    let (common_metrics, json) = match metrics.as_ref() {
        CoreMetrics::Legacy(legacy_metrics) => (legacy_metrics.com(), legacy_metrics.to_json()),
        CoreMetrics::TMQ(tmq_metrics) => (tmq_metrics.com(), tmq_metrics.to_json()),
        CoreMetrics::IPC(ipc_metrics) => (ipc_metrics.com(), ipc_metrics.to_json()),
    };
    let mut map =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json.as_str()).unwrap();
    map.remove("task_id");
    compute_total_avg_speed(common_metrics, &mut map);
    compute_avg_speed(common_metrics, &mut map, running);
    serde_json::to_string(&map).unwrap()
}

#[inline]
fn compute_total_avg_speed(
    common_metrics: &CommonMetrics,
    map: &mut serde_json::Map<String, serde_json::Value>,
) {
    let total_execute_time = common_metrics.total_execute_time.load(SeqCst);
    let total_written_rows = common_metrics.total_written_rows.load(SeqCst);
    let total_written_points = common_metrics.total_written_points.load(SeqCst);
    if total_execute_time > 0 {
        map.insert(
            "total_records_per_second".to_string(),
            (total_written_rows as f64 * 1000_f64 / total_execute_time as f64).into(),
        );
        map.insert(
            "total_points_per_second".to_string(),
            (total_written_points as f64 * 1000_f64 / total_execute_time as f64).into(),
        );
    }
}

#[inline]
fn compute_avg_speed(
    common_metrics: &CommonMetrics,
    map: &mut serde_json::Map<String, serde_json::Value>,
    running: bool,
) {
    let written_rows = common_metrics.written_rows.load(SeqCst);
    let written_points = common_metrics.written_points.load(SeqCst);
    let execute_time = if running {
        let start_time = common_metrics.start_time.get();
        let now = chrono::Utc::now().timestamp_millis();
        (now - start_time) as u64
    } else {
        common_metrics.execute_time.load(SeqCst)
    };
    map.insert("execute_time".to_string(), execute_time.into());
    map.insert(
        "records_per_second".to_string(),
        (written_rows as f64 * 1000_f64 / execute_time as f64).into(),
    );
    map.insert(
        "points_per_second".to_string(),
        (written_points as f64 * 1000_f64 / execute_time as f64).into(),
    );
}

/// Get metrics from global metrics map first, if not exist, try to load metrics from persistence.
/// If both failed, return None.
pub fn try_get_metrics<T: TaosXMetrics>(task_id: i64) -> Option<Arc<CoreMetrics>> {
    if let Some(metrics) = get_metrics(task_id) {
        Some(metrics)
    } else {
        tracing::info!("load metrics for task {}", task_id);
        if let Some(metrics) = load_metrics::<T>(task_id.to_string().as_str()) {
            let metrics = Arc::new(metrics.into());
            let mut global_metrics = GLOBAL_METRICS.lock().unwrap();
            global_metrics.insert(task_id, metrics.clone());
            Some(metrics)
        } else {
            tracing::warn!("no metrics found for task {}", task_id);
            None
        }
    }
}

pub fn clear_metrics(task_id: i64) {
    let mut metrics = GLOBAL_METRICS.lock().unwrap();
    let _ = metrics.remove(&task_id);
    let _ = MetricsDb::clear(task_id.to_string().as_str());
}

pub fn init_task_metrics(from: Dsn, to: Dsn, task_id: i64) -> Arc<CoreMetrics> {
    match (from.driver.as_str(), to.driver.as_str()) {
        ("taos", "taos") => {
            let metrics = try_get_metrics::<LegacyToTaosMetrics>(task_id);
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task {}", task_id);
                metrics.legacy().reset();
                metrics
            } else {
                tracing::info!("create new metrics for task {}", task_id);
                let metrics = Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::new(task_id)));
                GLOBAL_METRICS
                    .lock()
                    .unwrap()
                    .insert(task_id, metrics.clone());
                metrics
            }
        }
        ("tmq", "taos" | "local") => {
            let metrics = try_get_metrics::<TmqMetrics>(task_id);
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task {}", task_id);
                metrics.tmq().reset();
                metrics
            } else {
                tracing::info!("create new metrics for task {}", task_id);
                let metrics = Arc::new(CoreMetrics::TMQ(TmqMetrics::new(task_id)));
                GLOBAL_METRICS
                    .lock()
                    .unwrap()
                    .insert(task_id, metrics.clone());
                metrics
            }
        }
        (
            "opc" | "opcua" | "opcda" | "pi" | "pibackfill" | "mqtt" | "influxdb" | "opentsdb"
            | "kafka" | "historian" | "csv",
            "taos",
        ) => {
            let metrics = try_get_metrics::<IpcMetrics>(task_id);
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task {}", task_id);
                metrics.ipc().reset();
                metrics
            } else {
                tracing::info!("create new metrics for task {}", task_id);
                let metrics = Arc::new(CoreMetrics::IPC(IpcMetrics::new(task_id)));
                GLOBAL_METRICS
                    .lock()
                    .unwrap()
                    .insert(task_id, metrics.clone());
                metrics
            }
        }
        _ => {
            tracing::error!("unsupported datasource");
            panic!("unsupported datasource")
        }
    }
}

#[derive(Debug)]
pub struct LastPersistTime(Cell<Instant>);

/// LastPersistTime is a wrapper of Instant to store the last persist time of a task,
/// so that it can be accessed by multiple threads and updated concurrently.
impl LastPersistTime {
    pub fn elapsed_millis(&self) -> u64 {
        self.0.get().elapsed().as_millis() as u64
    }

    pub fn reset(&self) {
        self.0.set(Instant::now());
    }
}

impl Default for LastPersistTime {
    fn default() -> Self {
        Self(Cell::new(Instant::now()))
    }
}

unsafe impl Sync for LastPersistTime {}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskStartTime(Cell<i64>);

/// TaskStartTime is a wrapper of i64 to store the start time of a task,
/// so that it can be accessed by multiple threads and updated concurrently.
impl TaskStartTime {
    pub fn new() -> Self {
        Self(Cell::new(chrono::Utc::now().timestamp_millis()))
    }

    pub fn reset(&self) {
        self.0.set(chrono::Utc::now().timestamp_millis());
    }

    pub fn get(&self) -> i64 {
        self.0.get()
    }
}

impl Default for TaskStartTime {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Sync for TaskStartTime {}

/// Save every 10 seconds
pub fn auto_save_task_metrics(
    metrics_arc: Arc<CoreMetrics>,
    mut close_signal: oneshot::Receiver<()>,
) {
    tokio::spawn(
        async move {
            tracing::info!("auto-save metrics task start");
            loop {
                match close_signal.try_recv() {
                    Ok(_) => {
                        break;
                    }
                    Err(recv_error) => match recv_error {
                        oneshot::error::TryRecvError::Closed => {
                            tracing::debug!("auto-save metrics channel closed");
                            break;
                        }
                        oneshot::error::TryRecvError::Empty => {
                            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                            match save_metrics(metrics_arc.clone()) {
                                Ok(_) => {
                                    tracing::debug!("auto-save metrics success")
                                }
                                Err(err) => {
                                    tracing::error!("auto-save metrics failed. {}", err);
                                }
                            }
                        }
                    },
                }
            }
            tracing::info!("auto-save metrics task exit");
        }
        .in_current_span(),
    );
}

pub fn save_task_metrics_finally(task_id: i64) {
    let metrics = get_metrics(task_id);
    match metrics {
        Some(metrics) => match save_metrics(metrics) {
            Ok(_) => {
                tracing::info!("finally save metrics success");
            }
            Err(err) => {
                tracing::error!("finally save metrics failed. {}", err);
            }
        },
        None => {
            tracing::error!("finally save metrics failed, metrics not found");
        }
    }
}

fn save_metrics(metrics: Arc<CoreMetrics>) -> anyhow::Result<()> {
    match metrics.as_ref() {
        CoreMetrics::Legacy(legacy_metrics) => legacy_metrics.save(),
        CoreMetrics::TMQ(tmq_metrics) => tmq_metrics.save(),
        CoreMetrics::IPC(ipc_metrics) => ipc_metrics.save(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This test case is to verify that the global metrics can be accessed by multiple threads and the metrics can be updated concurrently.
    #[test]
    fn test_global_metrics() {
        let mut metrics = GLOBAL_METRICS.lock().unwrap();
        let legacy_to_taos_metrics = LegacyToTaosMetrics::new(1);
        legacy_to_taos_metrics
            .workers
            .fetch_add(10, std::sync::atomic::Ordering::SeqCst);
        metrics.insert(1, Arc::new(CoreMetrics::Legacy(legacy_to_taos_metrics)));
        drop(metrics);

        let t1 = std::thread::spawn(|| {
            println!("thread 1");
            let metrics = get_metrics(1).unwrap();
            let legacy_to_taos_metrics = metrics.legacy();
            // let legacy_to_taos_metrics = metrics.as_ref().legacy();
            println!("thread 1 get metrics");
            legacy_to_taos_metrics
                .total_created_tables
                .fetch_add(100, std::sync::atomic::Ordering::SeqCst);
            println!("thread 1 end")
        });

        let t2 = std::thread::spawn(|| {
            println!("thread 2");
            let metrics = get_metrics(1).unwrap();
            println!("thread 2 get metrics");
            let legacy_to_taos_metrics = metrics.legacy();
            println!(
                "created_tables: {}",
                legacy_to_taos_metrics
                    .total_created_tables
                    .load(std::sync::atomic::Ordering::SeqCst)
            );
            legacy_to_taos_metrics
                .total_created_tables
                .fetch_add(100, std::sync::atomic::Ordering::SeqCst);
            println!("thread 2 end");
        });

        t2.join().unwrap();
        t1.join().unwrap();
        let metrics = get_metrics(1).unwrap();
        let legacy_to_taos_metrics = metrics.legacy();
        println!(
            "workers: {}",
            legacy_to_taos_metrics
                .workers
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        println!(
            "created_tables: {}",
            legacy_to_taos_metrics
                .total_created_tables
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    /// This test case is to verify that the metrics can be loaded from persistence.
    #[test]
    fn test_load_metrics() {
        let legacy_to_taos_metrics = LegacyToTaosMetrics::new(1024);
        legacy_to_taos_metrics
            .workers
            .fetch_add(10, std::sync::atomic::Ordering::SeqCst);

        {
            let db = MetricsDb::new("1024").unwrap();
            db.set(legacy_to_taos_metrics.to_json().as_str()).unwrap();
        }
        let metrics = load_metrics::<LegacyToTaosMetrics>("1024").unwrap();
        assert_eq!(
            metrics.workers.load(std::sync::atomic::Ordering::SeqCst),
            10
        );
        MetricsDb::clear("1024").unwrap();
    }

    /// This test case is to verify that the metrics can be saved to persistence and cleared.
    #[test]
    fn test_save_and_clear_metrics() {
        let path_buf = MetricsDb::db_dir("10240");
        let db_path = Path::new(&path_buf);

        let legacy_to_taos_metrics = LegacyToTaosMetrics::new(1024);
        legacy_to_taos_metrics
            .workers
            .fetch_add(10, std::sync::atomic::Ordering::SeqCst);
        let metrics = Arc::new(CoreMetrics::Legacy(legacy_to_taos_metrics));
        {
            let mut global_metrics = GLOBAL_METRICS.lock().unwrap();
            global_metrics.insert(10240, metrics.clone());
        }
        metrics.as_ref().legacy().save().unwrap();
        assert!(db_path.exists());
        clear_metrics(10240);
        let metrics = get_metrics(10240);
        assert!(metrics.is_none());
        assert!(!db_path.exists());
    }
}
