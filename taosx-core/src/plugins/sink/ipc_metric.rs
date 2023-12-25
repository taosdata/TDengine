use crate::core_metrics::{CommonMetrics, CoreMetrics, TaosXMetrics};
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering::SeqCst;
use tracing;

#[derive(Serialize, Deserialize, Debug)]
pub struct IPCMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    pub total_received_batches: AtomicU64,
    pub total_processed_batches: AtomicU64,
    pub total_insert_sqls: AtomicU64,
    pub total_failed_sqls: AtomicU64,
    pub total_created_stables: AtomicU64,
    pub total_created_tables: AtomicU64,
    pub total_failed_rows: AtomicU64,
    pub total_failed_points: AtomicU64,
    pub total_written_raw_blocks: AtomicU64,
    pub total_failed_raw_blocks: AtomicU64,
    pub received_batches: AtomicU64,
    pub processed_batches: AtomicU64,
    pub processed_records: AtomicU64,
    pub insert_sqls: AtomicU64,
    pub failed_sqls: AtomicU64,
    pub created_stables: AtomicU64,
    pub created_tables: AtomicU64,
    pub failed_rows: AtomicU64,
    pub failed_points: AtomicU64,
    pub written_raw_blocks: AtomicU64,
    pub failed_raw_blocks: AtomicU64,
}

impl Default for IPCMetrics {
    fn default() -> Self {
        Self {
            com: CommonMetrics::default(),
            total_received_batches: AtomicU64::new(0),
            total_processed_batches: AtomicU64::new(0),
            total_insert_sqls: AtomicU64::new(0),
            total_failed_sqls: AtomicU64::new(0),
            total_created_stables: AtomicU64::new(0),
            total_created_tables: AtomicU64::new(0),
            total_failed_rows: AtomicU64::new(0),
            total_failed_points: AtomicU64::new(0),
            total_written_raw_blocks: AtomicU64::new(0),
            total_failed_raw_blocks: AtomicU64::new(0),
            received_batches: AtomicU64::new(0),
            processed_batches: AtomicU64::new(0),
            processed_records: AtomicU64::new(0),
            insert_sqls: AtomicU64::new(0),
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

impl IPCMetrics {
    pub fn new(task_id: i64) -> Self {
        Self {
            com: CommonMetrics::new(task_id),
            total_received_batches: AtomicU64::new(0),
            total_processed_batches: AtomicU64::new(0),
            total_insert_sqls: AtomicU64::new(0),
            total_failed_sqls: AtomicU64::new(0),
            total_created_stables: AtomicU64::new(0),
            total_created_tables: AtomicU64::new(0),
            total_failed_rows: AtomicU64::new(0),
            total_failed_points: AtomicU64::new(0),
            total_written_raw_blocks: AtomicU64::new(0),
            total_failed_raw_blocks: AtomicU64::new(0),
            received_batches: AtomicU64::new(0),
            processed_batches: AtomicU64::new(0),
            processed_records: AtomicU64::new(0),
            insert_sqls: AtomicU64::new(0),
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
    pub fn add_processed_batches(&self, n: u64) {
        self.total_processed_batches.fetch_add(n, SeqCst);
        self.processed_batches.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_processed_records(&self, n: u64) {
        self.total_processed_batches.fetch_add(n, SeqCst);
        self.processed_records.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_insert_sqls(&self, n: u64) {
        self.total_insert_sqls.fetch_add(n, SeqCst);
        self.insert_sqls.fetch_add(n, SeqCst);
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
}

impl Into<CoreMetrics> for IPCMetrics {
    fn into(self) -> CoreMetrics {
        CoreMetrics::IPC(self)
    }
}

impl TaosXMetrics for IPCMetrics {
    fn reset(&self) {
        self.com.reset();
        self.received_batches.store(0, SeqCst);
        self.processed_records.store(0, SeqCst);
        self.insert_sqls.store(0, SeqCst);
        self.failed_sqls.store(0, SeqCst);
        self.created_stables.store(0, SeqCst);
        self.created_tables.store(0, SeqCst);
        self.failed_rows.store(0, SeqCst);
        self.failed_points.store(0, SeqCst);
        self.written_raw_blocks.store(0, SeqCst);
        self.failed_raw_blocks.store(0, SeqCst);
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
