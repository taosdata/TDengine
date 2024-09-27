//! Crate level metrics related data structures and functions.
//! Define metrics data structure for each supported datasource.
//! And supply a global accessible map to store all metrics data.
//!
//! Concepts:
//! 1. Run Metrics：metrics that will be reset before each run.
//! 2. Total Metrics：metrics that will be accumulated during the whole life cycle of a task.

use crate::legacy::legacy_metric::LegacyToTaosMetrics;
use crate::plugins::sink::ipc_metric::IpcMetrics;
use crate::runners;
use crate::tmq::tmq_metric::TmqMetrics;
use crate::utils::metrics_db::MetricsStore;
use lazy_static::lazy_static;
use metrics::atomics::AtomicU64;
use scc::HashMap;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Instant;
use taos::Dsn;
use tokio::sync::oneshot;
use tracing::Instrument;

/// MetricsType is an enum to store all supported metrics data structure.
#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMetrics {
    Legacy(LegacyToTaosMetrics),
    TMQ(TmqMetrics),
    IPC(IpcMetrics),
}

impl CoreMetrics {
    /// Unwrap this enum to get the LegacyToTaosMetrics.
    #[inline]
    pub fn legacy(&self) -> &LegacyToTaosMetrics {
        match self {
            CoreMetrics::Legacy(legacy) => legacy,
            _ => panic!("metrics type not match"),
        }
    }

    /// Unwrap this enum to get the TMQMetrics.
    #[inline]
    pub fn tmq(&self) -> &TmqMetrics {
        match self {
            CoreMetrics::TMQ(tmq) => tmq,
            _ => panic!("metrics type not match"),
        }
    }

    /// Unwrap this enum to get the IpcMetrics.
    #[inline]
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
    /// 监控系统对应的超级表名
    pub stable: String,
    pub task_id: i64,
    pub task_name: Option<String>,
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
            stable: String::new(),
            task_id: -1,
            task_name: None,
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
    pub fn new(stable: String, task_id: i64, task_name: Option<String>) -> Self {
        Self {
            stable,
            task_id,
            task_name,
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
        self.last_persist_time.reset();
    }
}

pub trait TaskMetrics: Into<CoreMetrics> + Serialize {
    /// Reset all "run metrics"
    fn reset(&self);

    /// Return CommonMetrics
    fn com(&self) -> &CommonMetrics;

    /// Convert metrics to json string.
    fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }

    /// Restore metrics from json string.
    fn from_json(json: &str) -> Option<Self>;

    fn save(&self) -> anyhow::Result<()> {
        self.com().update_execute_time();
        let task_id = self.com().task_id.to_string();
        let store = MetricsStore::new(task_id.as_str());
        store.set(self.to_json().as_str())
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
    /// Global metrics map to store all metrics data. The key is task_id.
    pub static ref GLOBAL_METRICS: HashMap<i64, Arc<CoreMetrics>> =
        HashMap::new();
}

pub async fn insert_metrics(mut task_id: i64, mut metrics: Arc<CoreMetrics>) {
    let mut retries = 0;
    loop {
        match GLOBAL_METRICS.insert_async(task_id, metrics).await {
            Ok(_) => {
                if retries > 0 {
                    tracing::info!("Insert metrics success with {retries} retries");
                }
                break;
            }
            Err(entry) => {
                (task_id, metrics) = entry;
                retries += 1;
                if retries > 3 {
                    tracing::error!("Insert metrics failed after 3 retries");
                    break;
                }
            }
        }
    }
}

/// Try to get metrics from global metrics map.
pub async fn get_metrics(task_id: i64) -> Option<Arc<CoreMetrics>> {
    GLOBAL_METRICS.read_async(&task_id, |_, v| v.clone()).await
}

/// Get metrics of a task after it's metrics has been initialized,
/// so that it's metrics must exist in the global map.
#[inline]
pub async fn get_metrics_arc(task_id: Option<String>) -> Arc<CoreMetrics> {
    let task_id = match task_id {
        Some(id) => id.parse::<i64>().unwrap(),
        _ => -1,
    };
    get_metrics(task_id).await.expect("metrics not found")
}

pub async fn get_metrics_arc_from_i64(task_id: Option<i64>) -> Arc<CoreMetrics> {
    let task_id = match task_id {
        Some(id) => id,
        _ => -1,
    };
    get_metrics(task_id).await.expect("metrics not found")
}

/// Try to load metrics from persistence.
pub fn load_metrics<T: TaskMetrics>(task_id: &str) -> Option<T> {
    let store = MetricsStore::new(task_id);
    if store.path.exists() {
        match store.get_string() {
            Ok(json) => {
                let j = json.as_str();
                T::from_json(j)
            }
            Err(err) => {
                tracing::error!("get metrics from disk failed: {:?}", &err);
                None
            }
        }
    } else {
        None
    }
}

pub fn split_to_total_and_current(
    map: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let mut total_map = BTreeMap::new();
    let mut current_map = BTreeMap::new();
    for (k, v) in map {
        if k.starts_with("total_") {
            total_map.insert(k.to_string(), v);
        } else {
            current_map.insert(k.to_string(), v);
        }
    }
    serde_json::json!({
        "total": total_map,
        "current": current_map
    })
}

#[inline]
pub fn compute_total_avg_speed(
    common_metrics: &CommonMetrics,
    map: &mut serde_json::Map<String, serde_json::Value>,
) {
    let total_execute_time = common_metrics.total_execute_time.load(SeqCst);
    let total_written_rows = common_metrics.total_written_rows.load(SeqCst);
    let total_written_points = common_metrics.total_written_points.load(SeqCst);
    if total_execute_time > 0 {
        map.insert(
            "total_rows_per_second".to_string(),
            (total_written_rows as f64 * 1000_f64 / total_execute_time as f64).into(),
        );
        map.insert(
            "total_points_per_second".to_string(),
            (total_written_points as f64 * 1000_f64 / total_execute_time as f64).into(),
        );
    }
}

#[inline]
pub fn compute_avg_speed(
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
        "rows_per_second".to_string(),
        (written_rows as f64 * 1000_f64 / execute_time as f64).into(),
    );
    map.insert(
        "points_per_second".to_string(),
        (written_points as f64 * 1000_f64 / execute_time as f64).into(),
    );
}

/// Get metrics from global metrics map first, if not exist, try to load metrics from persistence.
/// If both failed, return None.
pub async fn try_get_metrics<T: TaskMetrics>(task_id: i64) -> Option<Arc<CoreMetrics>> {
    if let Some(metrics) = get_metrics(task_id).await {
        Some(metrics)
    } else {
        tracing::info!("load metrics for task {}", task_id);
        if let Some(metrics) = load_metrics::<T>(task_id.to_string().as_str()) {
            let metrics = Arc::new(metrics.into());
            insert_metrics(task_id, metrics.clone()).await;
            Some(metrics)
        } else {
            tracing::debug!("no metrics found for task {}", task_id);
            None
        }
    }
}

pub async fn clear_metrics(task_id: i64) {
    let _ = GLOBAL_METRICS.remove_async(&task_id).await;
    let store = MetricsStore::new(task_id.to_string().as_str());
    match store.clear() {
        Ok(_) => {
            tracing::info!("clear metrics success");
        }
        Err(err) => {
            tracing::error!("clear metrics failed: {:?}", err);
        }
    }
}

pub async fn init_task_metrics(
    from: &Dsn,
    to: &Dsn,
    task_id: i64,
    task_name: Option<String>,
) -> Option<Arc<CoreMetrics>> {
    let driver = from.driver.as_str();
    match (driver, to.driver.as_str()) {
        ("taos", "taos") => {
            let metrics = try_get_metrics::<LegacyToTaosMetrics>(task_id).await;
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task {}", task_id);
                metrics.legacy().reset();
                Some(metrics)
            } else {
                tracing::info!("create new metrics for task {}", task_id);
                let stable = String::from("taosx_task_tdengine2");
                let metrics = Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::new(
                    stable, task_id, task_name,
                )));
                insert_metrics(task_id, metrics.clone()).await;
                Some(metrics)
            }
        }
        ("tmq" | "sync", "taos" | "local") => {
            let metrics = try_get_metrics::<TmqMetrics>(task_id).await;
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task {}", task_id);
                metrics.tmq().reset();
                Some(metrics)
            } else {
                tracing::info!("create new metrics for task {}", task_id);
                let stable = String::from("taosx_task_tdengine3");
                let metrics = Arc::new(CoreMetrics::TMQ(TmqMetrics::new(
                    stable, task_id, task_name,
                )));
                insert_metrics(task_id, metrics.clone()).await;
                Some(metrics)
            }
        }
        (
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
            | "csv"
            | runners::mysql::MYSQL_ID
            | runners::postgres::POSTGRES_ID
            | runners::oracle::ORACLE_ID
            | runners::mssql::MSSQL_ID
            | runners::mongodb::MONGODB_ID,
            "taos",
        ) => {
            let metrics = try_get_metrics::<IpcMetrics>(task_id).await;
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task {}", task_id);
                metrics.ipc().reset();
                Some(metrics)
            } else {
                tracing::info!("create new metrics for task {}", task_id);
                let stable = String::from("taosx_task_") + driver;
                let metrics = Arc::new(CoreMetrics::IPC(IpcMetrics::new(
                    stable, task_id, task_name,
                )));
                insert_metrics(task_id, metrics.clone()).await;
                Some(metrics)
            }
        }
        _ => {
            tracing::trace!(
                "no metrics defined for datasource from={}, to={}",
                from.driver,
                to.driver
            );
            None
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
pub async fn auto_save_task_metrics(task_id: i64, mut close_signal: oneshot::Receiver<()>) {
    let metrics_arc = get_metrics(task_id).await.unwrap();
    tokio::spawn(
        async move {
            tracing::info!("start");
            loop {
                match close_signal.try_recv() {
                    Ok(_) => {
                        break;
                    }
                    Err(recv_error) => match recv_error {
                        oneshot::error::TryRecvError::Closed => {
                            tracing::debug!("channel closed");
                            break;
                        }
                        oneshot::error::TryRecvError::Empty => {
                            match save_metrics(metrics_arc.clone()) {
                                Ok(_) => {
                                    tracing::trace!("success");
                                }
                                Err(err) => {
                                    tracing::error!("failed. {}", err);
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                        }
                    },
                }
            }
            tracing::info!("exit");
        }
        .in_current_span(),
    );
}

pub async fn save_task_metrics_finally(task_id: i64) {
    let metrics = get_metrics(task_id).await;
    match metrics {
        Some(metrics) => match save_metrics(metrics) {
            Ok(_) => {
                tracing::debug!("finally save metrics success");
            }
            Err(err) => {
                tracing::error!("finally save metrics failed. {}", err);
            }
        },
        None => {
            tracing::trace!("finally save metrics failed, metrics not found");
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
    use crate::get_data_dir;
    const TEST_STABLE: &'static str = "test_stable";

    /// This test case is to verify that the global metrics can be accessed by multiple threads and the metrics can be updated concurrently.
    #[tokio::test]
    async fn test_global_metrics() {
        let legacy_to_taos_metrics = LegacyToTaosMetrics::new(TEST_STABLE.to_string(), 1, None);
        legacy_to_taos_metrics
            .read_concurrency
            .fetch_add(10, std::sync::atomic::Ordering::SeqCst);
        GLOBAL_METRICS
            .insert(1, Arc::new(CoreMetrics::Legacy(legacy_to_taos_metrics)))
            .unwrap();

        let t1 = tokio::spawn(async move {
            println!("thread 1");
            let metrics = get_metrics(1).await.unwrap();
            let legacy_to_taos_metrics = metrics.legacy();
            // let legacy_to_taos_metrics = metrics.as_ref().legacy();
            println!("thread 1 get metrics");
            legacy_to_taos_metrics
                .total_created_tables
                .fetch_add(100, std::sync::atomic::Ordering::SeqCst);
            println!("thread 1 end")
        });

        let t2 = tokio::spawn(async move {
            println!("thread 2");
            let metrics = get_metrics(1).await.unwrap();
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

        t2.await.unwrap();
        t1.await.unwrap();
        let metrics = get_metrics(1).await.unwrap();
        let legacy_to_taos_metrics = metrics.legacy();
        println!(
            "workers: {}",
            legacy_to_taos_metrics
                .read_concurrency
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        println!(
            "created_tables: {}",
            legacy_to_taos_metrics
                .total_created_tables
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_save_load_and_clear_metrics() {
        let task_dir = get_data_dir().join("tasks").join("1024");
        std::fs::create_dir_all(&task_dir).unwrap();
        let db = MetricsStore::new("1024");

        let legacy_to_taos_metrics = LegacyToTaosMetrics::new(TEST_STABLE.to_string(), 1024, None);
        legacy_to_taos_metrics
            .read_concurrency
            .fetch_add(10, std::sync::atomic::Ordering::SeqCst);
        let metrics = Arc::new(CoreMetrics::Legacy(legacy_to_taos_metrics));
        {
            GLOBAL_METRICS.insert(1024, metrics.clone()).unwrap();
        }
        metrics.as_ref().legacy().save().unwrap();
        assert!(db.path.exists());
        clear_metrics(1024).await;
        let metrics = get_metrics(1024).await;
        assert!(metrics.is_none());
        assert!(!db.path.exists());
    }
}
