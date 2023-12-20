//! Crate level metrics related data structures and functions.
//! Define metrics data structure for each supported datasource.
//! And supply a global accessible map to store all metrics data.

use crate::legacy::legacy_metric::LegacyToTaosMetrics;
use crate::plugins::sink::ipc_metric::IPCMetrics;
use crate::tmq::tmq_metric::TMQMetrics;
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

/// MetricsType is an enum to store all supported metrics data structure.
pub enum CoreMetrics {
    Legacy(LegacyToTaosMetrics),
    TMQ(TMQMetrics),
    IPC(IPCMetrics),
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
    pub fn tmq(&self) -> &TMQMetrics {
        match self {
            CoreMetrics::TMQ(tmq) => tmq,
            _ => panic!("metrics type not match"),
        }
    }

    pub fn ipc(&self) -> &IPCMetrics {
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

    pub fn update_total_execute_time(&self) {
        let elapsed = self.last_persist_time.elapsed_millis();
        self.total_execute_time.fetch_add(elapsed, SeqCst);
        self.last_persist_time.reset();
    }

    pub fn reset(&self) {
        self.start_time.reset();
        self.written_rows.store(0, SeqCst);
        self.written_points.store(0, SeqCst);
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

    /// Resore metrics from json string.
    fn from_json(json: &str) -> Self;

    /// Save metrics to database
    fn save(&self) -> anyhow::Result<()> {
        self.com().update_total_execute_time();
        let task_id = self.com().task_id.to_string();
        let db = MetricsDb::new(task_id.as_str())?;
        db.set(self.to_json().as_str())
    }

    #[inline]
    fn shold_save(&self) -> bool {
        self.com().task_id > -1
    }

    #[inline]
    fn compute_total_avg_speed(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        let total_execute_time = self.total_execute_time();
        if total_execute_time > 0 {
            map.insert(
                "total_records_per_second".to_string(),
                (self.total_written_rows() as f64 * 1000_f64 / total_execute_time as f64).into(),
            );
            map.insert(
                "total_points_per_second".to_string(),
                (self.total_written_points() as f64 * 1000_f64 / total_execute_time as f64).into(),
            );
        }
    }

    #[inline]
    fn compute_avg_speed(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        let execute_time = (chrono::Utc::now().timestamp_millis() - self.start_time()) as f64;
        map.insert("execute_time".to_string(), execute_time.into());
        map.insert(
            "records_per_second".to_string(),
            (self.written_rows() as f64 * 1000_f64 / execute_time).into(),
        );
        map.insert(
            "points_per_second".to_string(),
            (self.written_points() as f64 * 1000_f64 / execute_time).into(),
        );
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
                        Some(T::from_json(j))
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

pub fn get_legacy_metrics_for_explorer(
    running: bool,
    legacy_metrics: &LegacyToTaosMetrics,
) -> String {
    let json = legacy_metrics.to_json();
    let mut map =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json.as_str()).unwrap();
    legacy_metrics.compute_total_avg_speed(&mut map);
    if running {
        legacy_metrics.compute_avg_speed(&mut map)
    }
    serde_json::to_string(&map).unwrap()
}

/// Get metrics from global metrics map first, if not exist, try to load metrics from persistence.
/// If both failed, return None.
pub fn try_get_metrics<T: TaosXMetrics>(task_id: i64) -> Option<Arc<CoreMetrics>> {
    if let Some(metrics) = get_metrics(task_id) {
        Some(metrics)
    } else {
        if let Some(metrics) = load_metrics::<T>(task_id.to_string().as_str()) {
            let metrics = Arc::new(metrics.into());
            let mut global_metrics = GLOBAL_METRICS.lock().unwrap();
            global_metrics.insert(task_id, metrics.clone());
            Some(metrics)
        } else {
            None
        }
    }
}

pub fn clear_metrics(task_id: i64) {
    let mut metrics = GLOBAL_METRICS.lock().unwrap();
    let _ = metrics.remove(&task_id);
    let _ = MetricsDb::clear(task_id.to_string().as_str());
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
