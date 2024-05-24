use crate::core_metrics::{CommonMetrics, CoreMetrics, TaskMetrics};
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering::SeqCst;

#[derive(Serialize, Deserialize, Debug)]
pub struct IpcMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    #[serde(default)]
    pub total_received_batches: AtomicU64,
    #[serde(default)]
    pub total_processed_batches: AtomicU64,
    #[serde(default)]
    pub total_failed_batches: AtomicU64,
    #[serde(default)]
    pub total_processed_rows: AtomicU64,
    #[serde(default)]
    pub total_inserted_sqls: AtomicU64,
    #[serde(default)]
    pub total_failed_sqls: AtomicU64,
    #[serde(default)]
    pub total_created_stables: AtomicU64,
    #[serde(default)]
    pub total_created_tables: AtomicU64,
    #[serde(default)]
    pub total_failed_rows: AtomicU64,
    #[serde(default)]
    pub total_failed_points: AtomicU64,
    #[serde(default)]
    pub total_written_raw_blocks: AtomicU64,
    #[serde(default)]
    pub total_failed_raw_blocks: AtomicU64,
    #[serde(default)]
    pub received_batches: AtomicU64,
    #[serde(default)]
    pub processed_batches: AtomicU64,
    #[serde(default)]
    pub failed_batches: AtomicU64,
    #[serde(default)]
    pub processed_rows: AtomicU64,
    #[serde(default)]
    pub inserted_sqls: AtomicU64,
    #[serde(default)]
    pub failed_sqls: AtomicU64,
    #[serde(default)]
    pub created_stables: AtomicU64,
    #[serde(default)]
    pub created_tables: AtomicU64,
    #[serde(default)]
    pub failed_rows: AtomicU64,
    #[serde(default)]
    pub failed_points: AtomicU64,
    #[serde(default)]
    pub written_raw_blocks: AtomicU64,
    #[serde(default)]
    pub failed_raw_blocks: AtomicU64,
}

impl Default for IpcMetrics {
    fn default() -> Self {
        Self {
            com: CommonMetrics::default(),
            total_received_batches: AtomicU64::new(0),
            total_processed_batches: AtomicU64::new(0),
            total_failed_batches: AtomicU64::new(0),
            total_processed_rows: AtomicU64::new(0),
            total_inserted_sqls: AtomicU64::new(0),
            total_failed_sqls: AtomicU64::new(0),
            total_created_stables: AtomicU64::new(0),
            total_created_tables: AtomicU64::new(0),
            total_failed_rows: AtomicU64::new(0),
            total_failed_points: AtomicU64::new(0),
            total_written_raw_blocks: AtomicU64::new(0),
            total_failed_raw_blocks: AtomicU64::new(0),
            received_batches: AtomicU64::new(0),
            processed_batches: AtomicU64::new(0),
            failed_batches: AtomicU64::new(0),
            processed_rows: AtomicU64::new(0),
            inserted_sqls: AtomicU64::new(0),
            failed_sqls: AtomicU64::new(0),
            created_stables: AtomicU64::new(0),
            created_tables: AtomicU64::new(0),
            failed_rows: AtomicU64::new(0),
            failed_points: AtomicU64::new(0),
            written_raw_blocks: AtomicU64::new(0),
            failed_raw_blocks: AtomicU64::new(0),
        }
    }
}

impl IpcMetrics {
    pub fn new(stable: String, task_id: i64, task_name: Option<String>) -> Self {
        Self {
            com: CommonMetrics::new(stable, task_id, task_name),
            total_received_batches: AtomicU64::new(0),
            total_processed_batches: AtomicU64::new(0),
            total_failed_batches: AtomicU64::new(0),
            total_processed_rows: AtomicU64::new(0),
            total_inserted_sqls: AtomicU64::new(0),
            total_failed_sqls: AtomicU64::new(0),
            total_created_stables: AtomicU64::new(0),
            total_created_tables: AtomicU64::new(0),
            total_failed_rows: AtomicU64::new(0),
            total_failed_points: AtomicU64::new(0),
            total_written_raw_blocks: AtomicU64::new(0),
            total_failed_raw_blocks: AtomicU64::new(0),
            received_batches: AtomicU64::new(0),
            processed_batches: AtomicU64::new(0),
            failed_batches: AtomicU64::new(0),
            processed_rows: AtomicU64::new(0),
            inserted_sqls: AtomicU64::new(0),
            failed_sqls: AtomicU64::new(0),
            created_stables: AtomicU64::new(0),
            created_tables: AtomicU64::new(0),
            failed_rows: AtomicU64::new(0),
            failed_points: AtomicU64::new(0),
            written_raw_blocks: AtomicU64::new(0),
            failed_raw_blocks: AtomicU64::new(0),
        }
    }
    #[inline]
    pub fn add_received_batches(&self, n: u64) {
        self.total_received_batches.fetch_add(n, SeqCst);
        self.received_batches.fetch_add(n, SeqCst);
    }
    #[inline]
    pub fn total_received_batches(&self) -> u64 {
        self.total_received_batches.load(SeqCst)
    }

    #[inline]
    pub fn add_processed_batches(&self, n: u64) {
        self.total_processed_batches.fetch_add(n, SeqCst);
        self.processed_batches.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_processed_rows(&self, n: u64) {
        self.total_processed_rows.fetch_add(n, SeqCst);
        self.processed_rows.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_inserted_sqls(&self, n: u64) {
        self.total_inserted_sqls.fetch_add(n, SeqCst);
        self.inserted_sqls.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_failed_sqls(&self, n: u64) {
        self.total_failed_sqls.fetch_add(n, SeqCst);
        self.failed_sqls.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_created_stables(&self, n: u64) {
        self.total_created_stables.fetch_add(n, SeqCst);
        self.created_stables.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_created_tables(&self, n: u64) {
        self.total_created_tables.fetch_add(n, SeqCst);
        self.created_tables.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_failed_rows(&self, n: u64) {
        self.total_failed_rows.fetch_add(n, SeqCst);
        self.failed_rows.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_failed_points(&self, n: u64) {
        self.total_failed_points.fetch_add(n, SeqCst);
        self.failed_points.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_written_raw_blocks(&self, n: u64) {
        self.total_written_raw_blocks.fetch_add(n, SeqCst);
        self.written_raw_blocks.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_failed_raw_blocks(&self, n: u64) {
        self.total_failed_raw_blocks.fetch_add(n, SeqCst);
        self.failed_raw_blocks.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_failed_batches(&self, n: u64) {
        self.total_failed_batches.fetch_add(n, SeqCst);
        self.failed_batches.fetch_add(n, SeqCst);
    }
}

impl Into<CoreMetrics> for IpcMetrics {
    fn into(self) -> CoreMetrics {
        CoreMetrics::IPC(self)
    }
}

impl TaskMetrics for IpcMetrics {
    fn reset(&self) {
        self.com.reset();
        self.received_batches.store(0, SeqCst);
        self.processed_batches.store(0, SeqCst);
        self.processed_rows.store(0, SeqCst);
        self.inserted_sqls.store(0, SeqCst);
        self.failed_sqls.store(0, SeqCst);
        self.created_stables.store(0, SeqCst);
        self.created_tables.store(0, SeqCst);
        self.failed_rows.store(0, SeqCst);
        self.failed_points.store(0, SeqCst);
        self.written_raw_blocks.store(0, SeqCst);
        self.failed_raw_blocks.store(0, SeqCst);
        self.failed_batches.store(0, SeqCst);
    }

    fn com(&self) -> &CommonMetrics {
        &self.com
    }

    /// Restore metrics from json string.
    fn from_json(json: &str) -> Option<Self> {
        match serde_json::from_str(json) {
            Ok(metrics) => metrics,
            Err(err) => {
                tracing::error!("failed to deserialize metrics: {:?}", err);
                None
            }
        }
    }
}
