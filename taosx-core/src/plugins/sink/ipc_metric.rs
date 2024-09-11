use crate::core_metrics::{CommonMetrics, CoreMetrics, TaskMetrics};
use faststr::FastStr;
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering::SeqCst;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct IpcMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    pub total_received_batches: AtomicU64,
    pub total_processed_batches: AtomicU64,
    pub total_failed_batches: AtomicU64,
    pub total_processed_rows: AtomicU64,
    pub total_inserted_sqls: AtomicU64,
    pub total_failed_sqls: AtomicU64,
    pub total_created_stables: AtomicU64,
    pub total_created_tables: AtomicU64,
    pub total_failed_rows: AtomicU64,
    pub total_drained_rows: AtomicU64,
    pub total_failed_points: AtomicU64,
    pub total_written_raw_blocks: AtomicU64,
    pub total_failed_raw_blocks: AtomicU64,
    pub received_batches: AtomicU64,
    pub processed_batches: AtomicU64,
    pub failed_batches: AtomicU64,
    pub processed_rows: AtomicU64,
    pub inserted_sqls: AtomicU64,
    pub failed_sqls: AtomicU64,
    pub created_stables: AtomicU64,
    pub created_tables: AtomicU64,
    pub drained_rows: AtomicU64,
    pub failed_rows: AtomicU64,
    pub failed_points: AtomicU64,
    pub written_raw_blocks: AtomicU64,
    pub failed_raw_blocks: AtomicU64,

    #[serde(flatten)]
    pub extras: scc::HashIndex<FastStr, u64>,
}

impl IpcMetrics {
    pub fn new(stable: String, task_id: i64, task_name: Option<String>) -> Self {
        Self {
            com: CommonMetrics::new(stable, task_id, task_name),
            ..Default::default()
        }
    }

    pub fn set_extra_metric(&self, key: &FastStr, value: u64) {
        if let Some(entry) = self.extras.get(key) {
            entry.update(value);
        } else {
            self.extras.entry(key.clone()).or_insert_with(|| value);
        }
    }

    pub fn add_extra_metric(&self, key: &FastStr, value: u64) {
        if let Some(entry) = self.extras.get(key) {
            let new = *entry.get() + value;
            entry.update(new);
        } else {
            self.extras.entry(key.clone()).or_insert_with(|| value);
        }
    }
    pub fn sub_extra_metric(&self, key: &FastStr, value: u64) {
        if let Some(entry) = self.extras.get(key) {
            let new = if *entry.get() > value {
                *entry.get() - value
            } else {
                0
            };
            entry.update(new);
        } else {
            self.extras.entry(key.clone()).or_insert_with(|| 0);
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
    pub fn add_drained_rows(&self, n: u64) {
        self.total_drained_rows.fetch_add(n, SeqCst);
        self.drained_rows.fetch_add(n, SeqCst);
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
        self.drained_rows.store(0, SeqCst);
        self.failed_points.store(0, SeqCst);
        self.written_raw_blocks.store(0, SeqCst);
        self.failed_raw_blocks.store(0, SeqCst);
        self.failed_batches.store(0, SeqCst);

        self.extras.retain(|k, _| k.contains("total"));
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
