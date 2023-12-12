//! Crate level metrics related data structures and functions.
//! Define metrics data structure for each supported datasource.
//! And supply a global accessable map to store all metrics data.

use crate::legacy::metric::LegacyToTaosMetrics;
use crate::tmq::metric::TMQMetrics;
use crate::utils::metrics_db::MetricsDb;
use lazy_static::lazy_static;
use std::cell::Cell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// MetricsType is an enum to store all supported metrics data structure.
pub enum CoreMetrics {
    Legacy(LegacyToTaosMetrics),
    TMQ(TMQMetrics),
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
}

pub trait TaosXMetrics: Into<CoreMetrics> {
    /// Convert metrics to json string.
    fn to_json(&self) -> String;
    /// Resore metrics from json string.
    fn from_json(json: &str) -> Self;
    /// Reset run level metrics
    fn reset(&self);
    fn update_total_execute_time(&self);
    /// Save metrics to database
    fn save(&self, task_id: &str) -> anyhow::Result<()> {
        self.update_total_execute_time();
        let db = MetricsDb::new(task_id)?;
        db.set(self.to_json().as_str())
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

/// Try to load metrics from persistence.
pub fn load_metrics<T: TaosXMetrics>(task_id: &str) -> Option<T> {
    let path_buf = MetricsDb::db_dir(task_id);
    let db_path = Path::new(&path_buf);
    if db_path.exists() {
        match MetricsDb::from_path(db_path) {
            Ok(db) => match db.get() {
                Ok(json) => {
                    if let Some(json) = json {
                        Some(T::from_json(json.as_str()))
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
    if !running {
        return json;
    }
    let current_execute_time =
        (chrono::Utc::now().timestamp_millis() - legacy_metrics.start_time) as f64;
    let current_speed = legacy_metrics
        .current_suc_records
        .load(std::sync::atomic::Ordering::SeqCst) as f64
        * 1000_f64
        / current_execute_time;
    let mut map =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json.as_str()).unwrap();
    map.insert(
        "current_execute_time".to_string(),
        current_execute_time.into(),
    );
    map.insert("current_speed".to_string(), current_speed.into());
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

#[cfg(test)]
mod tests {
    use super::*;

    /// This test case is to verify that the global metrics can be accessed by multiple threads and the metrics can be updated concurrently.
    #[test]
    fn test_global_metrics() {
        let mut metrics = GLOBAL_METRICS.lock().unwrap();
        let legacy_to_taos_metrics = LegacyToTaosMetrics::default();
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
                .created_tables
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
                    .created_tables
                    .load(std::sync::atomic::Ordering::SeqCst)
            );
            legacy_to_taos_metrics
                .created_tables
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
                .created_tables
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    /// This test case is to verify that the metrics can be loaded from persistence.
    #[test]
    fn test_load_metrics() {
        let legacy_to_taos_metrics = LegacyToTaosMetrics::default();
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

        let legacy_to_taos_metrics = LegacyToTaosMetrics::default();
        legacy_to_taos_metrics
            .workers
            .fetch_add(10, std::sync::atomic::Ordering::SeqCst);
        let metrics = Arc::new(CoreMetrics::Legacy(legacy_to_taos_metrics));
        {
            let mut global_metrics = GLOBAL_METRICS.lock().unwrap();
            global_metrics.insert(10240, metrics.clone());
        }
        metrics.as_ref().legacy().save("10240").unwrap();
        assert!(db_path.exists());
        clear_metrics(10240);
        let metrics = get_metrics(1);
        assert!(metrics.is_none());
        assert!(!db_path.exists());
    }
}
