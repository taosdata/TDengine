//! Crate level metrics related data structures and functions.
//! Define metrics data structure for each supported datasource.
//! And supply a global accessible map to store all metrics data.
//!
//! Concepts:
//! 1. Run Metrics：metrics that will be reset before each run.
//! 2. Total Metrics：metrics that will be accumulated during the whole life cycle of a task.
//!
//! use std::collections::HashMap;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::{self, SeqCst};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use crate::legacy::legacy_metric::LegacyToTaosMetrics;
use crate::plugins::sink::ipc_metric::IpcMetrics;
use crate::tmq::tmq_metric::TmqMetrics;
use crate::utils::metrics_db::MetricsStore;

use anyhow::Context;
use futures::FutureExt as _;
use ha_core::activity::TaskStatus;
use metrics::atomics::AtomicU64;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use taos::Dsn;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// MetricsType is an enum to store all supported metrics data structure.
#[allow(clippy::upper_case_acronyms)]
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

    pub fn task_job_id(&self) -> (i64, i64) {
        match self {
            CoreMetrics::Legacy(m) => (m.com.task_id, m.com.job_id),
            CoreMetrics::TMQ(m) => (m.com.task_id, m.com.job_id),
            CoreMetrics::IPC(m) => (m.com.task_id, m.com.job_id),
        }
    }
}

impl std::ops::Deref for CoreMetrics {
    type Target = CommonMetrics;

    fn deref(&self) -> &Self::Target {
        match self {
            CoreMetrics::Legacy(legacy) => &legacy.com,
            CoreMetrics::TMQ(tmq) => &tmq.com,
            CoreMetrics::IPC(ipc) => &ipc.com,
        }
    }
}

impl std::ops::AddAssign for CoreMetrics {
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (CoreMetrics::Legacy(lhs), CoreMetrics::Legacy(rhs)) => {
                lhs.add_assign(rhs);
            }
            (CoreMetrics::TMQ(lhs), CoreMetrics::TMQ(rhs)) => {
                lhs.add_assign(rhs);
            }
            (CoreMetrics::IPC(lhs), CoreMetrics::IPC(rhs)) => {
                lhs.add_assign(rhs);
            }
            _ => {}
        }
    }
}

/// CommonMetrics is a data structure to store metrics that are common to all task types.
#[derive(Serialize, Deserialize, Debug)]
pub struct CommonMetrics {
    /// 监控系统对应的超级表名
    pub stable: String,
    pub task_id: i64,
    pub job_id: i64,
    pub start_time: TaskStartTime,
    pub total_execute_time: AtomicU64,
    pub total_written_rows: AtomicU64,
    pub total_written_points: AtomicU64,

    #[serde(skip)]
    pub last_persist_time: LastPersistTime,
    pub execute_time: AtomicU64,
    pub written_rows: AtomicU64,
    pub written_points: AtomicU64,
    pub received_messages: AtomicU64,
    pub processed_messages: AtomicU64,
}

impl Default for CommonMetrics {
    fn default() -> Self {
        Self {
            stable: String::new(),
            task_id: -1,
            job_id: -1,
            start_time: TaskStartTime::default(),
            total_execute_time: AtomicU64::new(0),
            total_written_rows: AtomicU64::new(0),
            total_written_points: AtomicU64::new(0),
            last_persist_time: LastPersistTime::default(),
            execute_time: AtomicU64::new(0),
            written_rows: AtomicU64::new(0),
            written_points: AtomicU64::new(0),
            received_messages: AtomicU64::new(0),
            processed_messages: AtomicU64::new(0),
        }
    }
}

impl std::ops::AddAssign for CommonMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.total_written_rows
            .fetch_add(rhs.total_written_rows.load(SeqCst), SeqCst);
        self.total_written_points
            .fetch_add(rhs.total_written_points.load(SeqCst), SeqCst);
        self.written_rows
            .fetch_add(rhs.written_rows.load(SeqCst), SeqCst);
        self.written_points
            .fetch_add(rhs.written_points.load(SeqCst), SeqCst);
        self.received_messages
            .fetch_add(rhs.received_messages.load(SeqCst), SeqCst);
        self.processed_messages
            .fetch_add(rhs.processed_messages.load(SeqCst), SeqCst);
    }
}

impl CommonMetrics {
    pub fn new(stable: String, task_id: i64, job_id: i64) -> Self {
        Self {
            stable,
            task_id,
            job_id,
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
        self.processed_messages.store(0, SeqCst);
        self.received_messages.store(0, SeqCst);
    }

    pub fn received_messages(&self) -> u64 {
        self.received_messages.load(SeqCst)
    }
    pub fn processed_messages(&self) -> u64 {
        self.processed_messages.load(SeqCst)
    }

    #[inline]
    pub fn add_received_messages(&self, n: u64) {
        self.received_messages.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_processed_messages(&self, n: u64) {
        self.processed_messages.fetch_add(n, SeqCst);
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

    fn save(&self) -> impl std::future::Future<Output = Result<(), anyhow::Error>> + Send {
        self.com().update_execute_time();
        let task_id = self.com().task_id;
        let job_id = self.com().job_id;
        let json = self.to_json();
        MetricsStore::new(task_id, job_id).then(|store| async move { store.set(&json).await })
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

    #[inline]
    fn add_received_messages(&self, n: u64) {
        self.com().received_messages.fetch_add(n, SeqCst);
    }

    #[inline]
    fn add_processed_messages(&self, n: u64) {
        self.com().processed_messages.fetch_add(n, SeqCst);
    }

    #[inline]
    fn received_messages(&self) {
        self.com().received_messages();
    }
    #[inline]
    fn processed_messages(&self) {
        self.com().processed_messages();
    }
}

#[derive(Debug, Clone)]
pub enum MetricsEvent {
    Insert(i64, i64, Arc<CoreMetrics>),
    Update(i64, i64, Arc<CoreMetrics>),
    Delete(i64, i64),
}

#[derive(Debug)]
pub struct MetricsState {
    watch_tx: watch::Sender<Option<MetricsEvent>>,
    task_watchers: RwLock<HashMap<(i64, i64), watch::Sender<MetricsEvent>>>,
}

impl MetricsState {
    pub fn new() -> Self {
        Self {
            watch_tx: watch::Sender::new(None),
            task_watchers: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, task_id: i64, job_id: i64, metrics: Arc<CoreMetrics>) {
        let sender = watch::Sender::new(MetricsEvent::Update(task_id, job_id, metrics.clone()));
        let event = MetricsEvent::Insert(task_id, job_id, metrics);
        self.watch_tx.send(Some(event)).ok();
        self.task_watchers.write().insert((task_id, job_id), sender);
    }

    pub fn update(&self, task_id: i64, job_id: i64) {
        let metrics = GLOBAL_METRICS.read().get(&(task_id, job_id)).cloned();
        if let (Some(metrics), Some(sender)) =
            (metrics, self.task_watchers.read().get(&(task_id, job_id)))
        {
            sender
                .send(MetricsEvent::Update(task_id, job_id, metrics))
                .ok();
        }
    }

    pub fn remove(&self, task_id: i64, job_id: i64) {
        self.watch_tx
            .send(Some(MetricsEvent::Delete(task_id, job_id)))
            .ok();
        self.task_watchers.write().remove(&(task_id, job_id));
    }
}

impl Default for MetricsState {
    fn default() -> Self {
        Self::new()
    }
}

static GLOBAL_METRICS_NOTIFIER: LazyLock<MetricsState> = LazyLock::new(MetricsState::new);

pub fn subscribe_task_metrics_watcher(
    task_id: i64,
    job_id: i64,
) -> Option<watch::Receiver<MetricsEvent>> {
    GLOBAL_METRICS_NOTIFIER
        .task_watchers
        .read()
        .get(&(task_id, job_id))
        .map(|sender| sender.subscribe())
}

pub fn subscribe_all_task_metrics_watcher() -> Vec<watch::Receiver<MetricsEvent>> {
    let watchers = &GLOBAL_METRICS_NOTIFIER.task_watchers.read();
    let mut res = Vec::with_capacity(watchers.len());
    for (_, sender) in watchers.iter() {
        res.push(sender.subscribe());
    }
    res
}

pub fn subscribe_metrics_watcher() -> watch::Receiver<Option<MetricsEvent>> {
    GLOBAL_METRICS_NOTIFIER.watch_tx.subscribe()
}

type GlobalMetrics = RwLock<HashMap<(i64, i64), Arc<CoreMetrics>>>;
pub static GLOBAL_METRICS: LazyLock<GlobalMetrics> = LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn insert_metrics(task_id: i64, job_id: i64, metrics: Arc<CoreMetrics>) {
    GLOBAL_METRICS
        .write()
        .insert((task_id, job_id), metrics.clone());
    GLOBAL_METRICS_NOTIFIER.insert(task_id, job_id, metrics);
}

/// Try to get metrics from global metrics map.
pub fn get_metrics(task_id: i64, job_id: i64) -> Option<Arc<CoreMetrics>> {
    GLOBAL_METRICS.read().get(&(task_id, job_id)).cloned()
}

pub async fn clear_metrics(task_id: i64, job_id: i64) {
    {
        if GLOBAL_METRICS.write().remove(&(task_id, job_id)).is_some() {
            GLOBAL_METRICS_NOTIFIER.remove(task_id, job_id);
        }
    }
    let store = MetricsStore::new(task_id, job_id).await;
    match store.clear().await {
        Ok(_) => {
            tracing::info!("clear metrics success");
        }
        Err(err) => {
            tracing::error!("clear metrics failed: {:?}", err);
        }
    }
}

pub fn update_metrics(task_id: i64, job_id: i64) {
    GLOBAL_METRICS_NOTIFIER.update(task_id, job_id);
}

/// Get metrics of a task after it's metrics has been initialized,
/// so that it's metrics must exist in the global map.
#[inline]
pub fn get_metrics_arc(task_job_id: Option<(i64, i64)>) -> Arc<CoreMetrics> {
    let (task_id, job_id) = match task_job_id {
        Some((task_id, job_id)) => (task_id, job_id),
        _ => (-1, -1),
    };
    get_metrics(task_id, job_id).expect("metrics not found")
}

pub fn get_metrics_arc_or<F: Fn() -> Arc<CoreMetrics>>(
    task_job_id: Option<(i64, i64)>,
    f: F,
) -> Arc<CoreMetrics> {
    if let Some(id) = task_job_id {
        let (task_id, job_id) = id;
        if let Some(metrics) = get_metrics(task_id, job_id) {
            return metrics;
        }
        let metrics = f();
        insert_metrics(task_id, job_id, metrics.clone());
        metrics
    } else {
        f()
    }
}

pub fn get_metrics_arc_from_i64(task_job_id: Option<(i64, i64)>) -> Arc<CoreMetrics> {
    let (task_id, job_id) = task_job_id.unwrap_or((-1, -1));
    get_metrics(task_id, job_id).expect("metrics not found")
}

pub fn find_metrics_arc(task_job_id: Option<(i64, i64)>) -> Option<Arc<CoreMetrics>> {
    let (task_id, job_id) = task_job_id?;
    get_metrics(task_id, job_id)
}

/// Try to load metrics from persistence.
pub async fn load_metrics<T: TaskMetrics>(task_id: i64, job_id: i64) -> Option<T> {
    let store = MetricsStore::new(task_id, job_id).await;
    if !store.path.exists() {
        return None;
    }
    match store.get_string().await {
        Ok(json) => T::from_json(&json),
        Err(err) => {
            tracing::error!("get metrics from disk failed: {:?}", &err);
            None
        }
    }
}

pub fn split_to_total_and_current(map: &HashMap<String, serde_json::Value>) -> serde_json::Value {
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
    map: &mut HashMap<String, serde_json::Value>,
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
    map: &mut HashMap<String, serde_json::Value>,
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
pub async fn try_get_metrics<T: TaskMetrics>(
    task_id: i64,
    job_id: i64,
    from: &Dsn,
) -> Option<Arc<CoreMetrics>> {
    if let Some(metrics) = get_metrics(task_id, job_id) {
        // 根据 dsn 过滤 TmqMetrics::progress 中的 topic
        if let CoreMetrics::TMQ(tmq_metrics) = metrics.as_ref() {
            filter_metrics_by_dsn(from, tmq_metrics);
        }
        Some(metrics)
    } else {
        tracing::info!("load metrics for task ({},{})", task_id, job_id);
        if let Some(metrics) = load_metrics::<T>(task_id, job_id).await {
            let metrics = Arc::new(metrics.into());
            // 根据 dsn 过滤 TmqMetrics::progress 中的 topic
            if let CoreMetrics::TMQ(tmq_metrics) = metrics.as_ref() {
                filter_metrics_by_dsn(from, tmq_metrics);
            }
            insert_metrics(task_id, job_id, metrics.clone());
            Some(metrics)
        } else {
            tracing::debug!("no metrics found for task {}", task_id);
            None
        }
    }
}

fn filter_metrics_by_dsn(from: &Dsn, metrics: &TmqMetrics) {
    if let Some(database) = from.subject.clone() {
        let topics: HashSet<String> = database.split(",").map(|s| s.trim().to_string()).collect();
        metrics.progress.retain(|topic, _| topics.contains(topic));
    }
}

pub async fn init_task_metrics(
    from: &Dsn,
    to: &Dsn,
    task_id: i64,
    job_id: i64,
) -> Option<Arc<CoreMetrics>> {
    let driver = from.driver.as_str();
    match (driver, to.driver.as_str()) {
        ("taos", "taos") => {
            let metrics = try_get_metrics::<LegacyToTaosMetrics>(task_id, job_id, from).await;
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task ({task_id},{job_id})");
                metrics.legacy().reset();
                Some(metrics)
            } else {
                tracing::info!("create new metrics for task ({task_id},{job_id})");
                let stable = String::from("taosx_task_tdengine2");
                let metrics = Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::new(
                    stable, task_id, job_id,
                )));
                insert_metrics(task_id, job_id, metrics.clone());
                Some(metrics)
            }
        }
        ("tmq" | "sync", "taos" | "local") => {
            let metrics = try_get_metrics::<TmqMetrics>(task_id, job_id, from).await;
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task {}", task_id);
                metrics.tmq().reset();
                Some(metrics)
            } else {
                tracing::info!("create new metrics for task ({task_id},{job_id})");
                let stable = String::from("taosx_task_tdengine3");
                let metrics = Arc::new(CoreMetrics::TMQ(TmqMetrics::new(stable, task_id, job_id)));
                insert_metrics(task_id, job_id, metrics.clone());
                Some(metrics)
            }
        }
        (
            "opc" | "opcua" | "opcda" | "pi" | "pibackfill" | "mqtt" | "sparkplugb" | "influxdb"
            | "opentsdb" | "kafka" | "avevaHistorian" | "csv" | "mysql" | "postgres" | "oracle"
            | "mssql" | "mongodb" | "local" | "orc" | "pulsar" | "pulsarTuya" | "kinghist",
            "taos" | "tmq",
        )
        | ("tmq", "mqtt") => {
            let metrics = try_get_metrics::<IpcMetrics>(task_id, job_id, from).await;
            if let Some(metrics) = metrics {
                tracing::info!("reset metrics for task ({task_id},{job_id})");
                metrics.ipc().reset();
                Some(metrics)
            } else {
                tracing::info!("create new metrics for task {}", task_id);
                let stable = String::from("taosx_task_") + driver;
                let metrics = Arc::new(CoreMetrics::IPC(IpcMetrics::new(stable, task_id, job_id)));
                insert_metrics(task_id, job_id, metrics.clone());
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
pub struct LastPersistTime(RwLock<Instant>);

/// LastPersistTime is a wrapper of Instant to store the last persist time of a task,
/// so that it can be accessed by multiple threads and updated concurrently.
impl LastPersistTime {
    pub fn elapsed_millis(&self) -> u64 {
        self.0.read().elapsed().as_millis() as u64
    }

    pub fn reset(&self) {
        *self.0.write() = Instant::now();
    }
}

impl Default for LastPersistTime {
    fn default() -> Self {
        Self(RwLock::new(Instant::now()))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskStartTime(AtomicI64);

/// TaskStartTime is a wrapper of i64 to store the start time of a task,
/// so that it can be accessed by multiple threads and updated concurrently.
impl TaskStartTime {
    pub fn new() -> Self {
        Self(AtomicI64::new(chrono::Utc::now().timestamp_millis()))
    }

    pub fn reset(&self) {
        self.0
            .store(chrono::Utc::now().timestamp_millis(), Ordering::SeqCst);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Default for TaskStartTime {
    fn default() -> Self {
        Self::new()
    }
}

/// Save every 10 seconds
pub async fn auto_save_task_metrics(metrics: Arc<CoreMetrics>, cancel: CancellationToken) {
    use tokio_util::time::FutureExt;
    tokio::spawn(async move {
        while cancel
            .cancelled()
            .timeout(Duration::from_secs(10))
            .await
            .is_err()
        {
            if let Err(e) = save_metrics(metrics.clone()).await {
                tracing::error!("Save metrics json error: {e:#}");
            }
        }
    });
}

pub async fn save_task_metrics_finally(metrics: Arc<CoreMetrics>) {
    match save_metrics(metrics).await {
        Ok(_) => {
            tracing::debug!("finally save metrics success");
        }
        Err(err) => {
            tracing::error!("finally save metrics failed. {}", err);
        }
    }
}

async fn save_metrics(metrics: Arc<CoreMetrics>) -> anyhow::Result<()> {
    match metrics.as_ref() {
        CoreMetrics::Legacy(legacy_metrics) => legacy_metrics.save().await,
        CoreMetrics::TMQ(tmq_metrics) => tmq_metrics.save().await,
        CoreMetrics::IPC(ipc_metrics) => ipc_metrics.save().await,
    }
}

pub fn get_task_metrics_string(
    status: TaskStatus,
    metrics: Arc<CoreMetrics>,
) -> anyhow::Result<String> {
    let running = status.is_running();
    let mut is_tmq = false;
    let (common_metrics, json) = match metrics.as_ref() {
        CoreMetrics::Legacy(legacy_metrics) => (legacy_metrics.com(), legacy_metrics.to_json()),
        CoreMetrics::TMQ(tmq_metrics) => {
            is_tmq = true;
            (tmq_metrics.com(), tmq_metrics.to_json())
        }
        CoreMetrics::IPC(ipc_metrics) => (ipc_metrics.com(), ipc_metrics.to_json()),
    };
    let mut map = serde_json::from_str::<HashMap<String, serde_json::Value>>(&json)
        .context("deserialize metrics to map")?;
    map.remove("task_id");
    map.remove("stable");
    //map.remove("task_name");
    if is_tmq {
        map.remove("written_rows");
        map.remove("total_written_rows");
        map.remove("written_points");
        map.remove("total_written_points");
        map.remove("success_blocks");
        map.remove("total_success_blocks");
        map.remove("write_raw_fails");
        map.remove("total_write_raw_fails");
    } else {
        compute_total_avg_speed(common_metrics, &mut map);
        compute_avg_speed(common_metrics, &mut map, running);
    }
    let result = split_to_total_and_current(&map);
    serde_json::to_string(&result).context("serialize metrics to string")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::get_data_dir;
    const TEST_STABLE: &str = "test_stable";

    /// This test case is to verify that the global metrics can be accessed by multiple threads and the metrics can be updated concurrently.
    #[tokio::test]
    async fn test_global_metrics() {
        let legacy_to_taos_metrics = LegacyToTaosMetrics::new(TEST_STABLE.to_string(), 1, 1);
        legacy_to_taos_metrics
            .read_concurrency
            .fetch_add(10, std::sync::atomic::Ordering::SeqCst);
        insert_metrics(1, 1, Arc::new(CoreMetrics::Legacy(legacy_to_taos_metrics)));

        let t1 = tokio::spawn(async move {
            println!("thread 1");
            let metrics = get_metrics(1, 1).unwrap();
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
            let metrics = get_metrics(1, 1).unwrap();
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
        let metrics = get_metrics(1, 1).unwrap();
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
        let db = MetricsStore::new(1024, -1).await;

        let legacy_to_taos_metrics = LegacyToTaosMetrics::new(TEST_STABLE.to_string(), 1024, -1);
        legacy_to_taos_metrics
            .read_concurrency
            .fetch_add(10, std::sync::atomic::Ordering::SeqCst);
        let metrics = Arc::new(CoreMetrics::Legacy(legacy_to_taos_metrics));
        insert_metrics(1024, -1, metrics.clone());
        metrics.as_ref().legacy().save().await.unwrap();
        assert!(db.path.exists());
        clear_metrics(1024, -1).await;
        let metrics = get_metrics(1024, -1);
        assert!(metrics.is_none());
        assert!(!db.path.exists());
    }

    #[test]
    fn test_common_metrics_new() {
        let metrics = CommonMetrics::new("test_stable".to_string(), 123, -1);

        assert_eq!(metrics.stable, "test_stable");
        assert_eq!(metrics.task_id, 123);
        assert_eq!(metrics.total_execute_time.load(SeqCst), 0);
        assert_eq!(metrics.total_written_rows.load(SeqCst), 0);
        assert_eq!(metrics.total_written_points.load(SeqCst), 0);
    }

    #[test]
    fn test_common_metrics_default() {
        let metrics = CommonMetrics::default();

        assert_eq!(metrics.stable, "");
        assert_eq!(metrics.task_id, -1);
        assert_eq!(metrics.total_execute_time.load(SeqCst), 0);
    }

    #[test]
    fn test_common_metrics_received_messages() {
        let metrics = CommonMetrics::default();

        metrics.add_received_messages(10);
        assert_eq!(metrics.received_messages(), 10);

        metrics.add_received_messages(5);
        assert_eq!(metrics.received_messages(), 15);
    }

    #[test]
    fn test_common_metrics_processed_messages() {
        let metrics = CommonMetrics::default();

        metrics.add_processed_messages(20);
        assert_eq!(metrics.processed_messages(), 20);

        metrics.add_processed_messages(8);
        assert_eq!(metrics.processed_messages(), 28);
    }

    #[test]
    fn test_common_metrics_reset() {
        let metrics = CommonMetrics::default();

        // Set some values
        metrics.written_rows.store(100, SeqCst);
        metrics.written_points.store(200, SeqCst);
        metrics.execute_time.store(5000, SeqCst);
        metrics.received_messages.store(50, SeqCst);
        metrics.processed_messages.store(45, SeqCst);

        // Reset
        metrics.reset();

        // Verify all run metrics are reset
        assert_eq!(metrics.written_rows.load(SeqCst), 0);
        assert_eq!(metrics.written_points.load(SeqCst), 0);
        assert_eq!(metrics.execute_time.load(SeqCst), 0);
        assert_eq!(metrics.received_messages.load(SeqCst), 0);
        assert_eq!(metrics.processed_messages.load(SeqCst), 0);
    }

    #[test]
    fn test_task_start_time() {
        let start_time = TaskStartTime::new();
        let initial = start_time.get();

        // Sleep a bit and reset
        std::thread::sleep(std::time::Duration::from_millis(10));
        start_time.reset();
        let after_reset = start_time.get();

        assert!(after_reset > initial, "Reset should update to current time");
    }

    #[test]
    fn test_last_persist_time_elapsed() {
        let persist_time = LastPersistTime::default();

        // Sleep a bit
        std::thread::sleep(std::time::Duration::from_millis(50));

        let elapsed = persist_time.elapsed_millis();
        assert!(
            elapsed >= 50,
            "Elapsed time should be at least 50ms, got {}",
            elapsed
        );
        assert!(
            elapsed < 100,
            "Elapsed time should be less than 100ms, got {}",
            elapsed
        );
    }

    #[test]
    fn test_last_persist_time_reset() {
        let persist_time = LastPersistTime::default();

        // Sleep and check elapsed
        std::thread::sleep(std::time::Duration::from_millis(30));
        let first_elapsed = persist_time.elapsed_millis();

        // Reset
        persist_time.reset();

        // Check elapsed again should be small
        let after_reset = persist_time.elapsed_millis();
        assert!(
            after_reset < first_elapsed,
            "After reset, elapsed should be less than before"
        );
    }

    #[test]
    fn test_core_metrics_deref() {
        let legacy_metrics = LegacyToTaosMetrics::new("test".to_string(), 1, -1);
        let core_metrics = CoreMetrics::Legacy(legacy_metrics);

        // Test deref to CommonMetrics
        assert_eq!(core_metrics.stable, "test");
        assert_eq!(core_metrics.task_id, 1);
    }

    #[test]
    #[should_panic(expected = "metrics type not match")]
    fn test_core_metrics_legacy_panic() {
        let tmq_metrics = TmqMetrics::new("test".to_string(), 1, -1);
        let core_metrics = CoreMetrics::TMQ(tmq_metrics);

        // This should panic because it's TMQ, not Legacy
        let _ = core_metrics.legacy();
    }

    #[test]
    #[should_panic(expected = "metrics type not match")]
    fn test_core_metrics_tmq_panic() {
        let legacy_metrics = LegacyToTaosMetrics::new("test".to_string(), 1, -1);
        let core_metrics = CoreMetrics::Legacy(legacy_metrics);

        // This should panic because it's Legacy, not TMQ
        let _ = core_metrics.tmq();
    }

    #[test]
    #[should_panic(expected = "metrics type not match")]
    fn test_core_metrics_ipc_panic() {
        let legacy_metrics = LegacyToTaosMetrics::new("test".to_string(), 1, -1);
        let core_metrics = CoreMetrics::Legacy(legacy_metrics);

        // This should panic because it's Legacy, not IPC
        let _ = core_metrics.ipc();
    }

    #[test]
    fn test_compute_total_avg_speed_nonzero() {
        let metrics = CommonMetrics::default();
        metrics.total_execute_time.store(2000, SeqCst); // 2 seconds
        metrics.total_written_rows.store(4000, SeqCst);
        metrics.total_written_points.store(1000, SeqCst);

        let mut map = HashMap::new();
        compute_total_avg_speed(&metrics, &mut map);

        let rows_per_second = map.get("total_rows_per_second").unwrap().as_f64().unwrap();
        let points_per_second = map
            .get("total_points_per_second")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((rows_per_second - 2000.0).abs() < 1e-6);
        assert!((points_per_second - 500.0).abs() < 1e-6);
    }

    #[test]
    fn test_compute_avg_speed_running_false() {
        let metrics = CommonMetrics::default();
        metrics.execute_time.store(2500, SeqCst); // 2.5 seconds
        metrics.written_rows.store(1250, SeqCst);
        metrics.written_points.store(250, SeqCst);

        let mut map = HashMap::new();
        compute_avg_speed(&metrics, &mut map, false);

        let execute_time = map.get("execute_time").unwrap().as_u64().unwrap();
        let rows_per_second = map.get("rows_per_second").unwrap().as_f64().unwrap();
        let points_per_second = map.get("points_per_second").unwrap().as_f64().unwrap();

        assert_eq!(execute_time, 2500);
        assert!((rows_per_second - 500.0).abs() < 1e-6);
        assert!((points_per_second - 100.0).abs() < 1e-6);
    }

    #[test]
    fn test_compute_avg_speed_running_true() {
        let metrics = CommonMetrics::default();
        // Reset start_time to now, then wait a bit to accumulate elapsed
        metrics.start_time.reset();
        metrics.written_rows.store(300, SeqCst);
        metrics.written_points.store(150, SeqCst);

        std::thread::sleep(std::time::Duration::from_millis(50));

        let mut map = HashMap::new();
        compute_avg_speed(&metrics, &mut map, true);

        let execute_time = map.get("execute_time").unwrap().as_u64().unwrap();
        let rows_per_second = map.get("rows_per_second").unwrap().as_f64().unwrap();
        let points_per_second = map.get("points_per_second").unwrap().as_f64().unwrap();

        assert!(execute_time >= 50, "execute_time should be >= 50ms");
        // rows_per_second ≈ 300 * 1000 / execute_time
        let expected_rows = 300.0 * 1000.0 / execute_time as f64;
        let expected_points = 150.0 * 1000.0 / execute_time as f64;
        assert!((rows_per_second - expected_rows).abs() < 1e-6);
        assert!((points_per_second - expected_points).abs() < 1e-6);
    }

    #[test]
    fn test_split_to_total_and_current() {
        let mut map = HashMap::new();
        map.insert("total_execute_time".to_string(), serde_json::json!(3000));
        map.insert("total_written_rows".to_string(), serde_json::json!(6000));
        map.insert("written_rows".to_string(), serde_json::json!(100));
        map.insert("execute_time".to_string(), serde_json::json!(500));

        let v = split_to_total_and_current(&map);
        let total = v.get("total").unwrap();
        let current = v.get("current").unwrap();

        // total map contains only keys starting with "total_"
        assert!(total.get("total_execute_time").is_some());
        assert!(total.get("total_written_rows").is_some());
        assert!(total.get("written_rows").is_none());

        // current map contains other keys
        assert!(current.get("written_rows").is_some());
        assert!(current.get("execute_time").is_some());
        assert!(current.get("total_execute_time").is_none());
    }

    #[test]
    fn test_compute_total_avg_speed_zero_time_no_insert() {
        let metrics = CommonMetrics::default();
        metrics.total_execute_time.store(0, SeqCst);
        metrics.total_written_rows.store(100, SeqCst);
        metrics.total_written_points.store(50, SeqCst);

        let mut map = HashMap::new();
        compute_total_avg_speed(&metrics, &mut map);

        assert!(!map.contains_key("total_rows_per_second"));
        assert!(!map.contains_key("total_points_per_second"));
    }

    #[tokio::test]
    async fn test_get_metrics_arc_or_insert_and_none_path() {
        // None path: should return f() without inserting
        let arc_none = get_metrics_arc_or(None, || {
            Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::new(
                "stb_none".to_string(),
                -99,
                1,
            )))
        });
        // None path returns provided metrics without insertion; verify attributes
        assert_eq!(arc_none.task_id, -99);
        assert_eq!(arc_none.stable, "stb_none");

        // Some(id) path: not existing -> insert and return
        let id = 7777_i64;
        let arc_new = get_metrics_arc_or(Some((id, id)), || {
            Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::new(
                "stb_insert".to_string(),
                id,
                id,
            )))
        });

        assert_eq!(arc_new.task_id, id);
        // Verify it can be fetched again from global map
        let arc_fetch = get_metrics(id, id).unwrap();
        assert_eq!(arc_fetch.task_id, id);
        assert_eq!(arc_fetch.stable, "stb_insert");
    }
}
