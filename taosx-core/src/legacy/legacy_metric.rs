use crate::core_metrics::{CommonMetrics, CoreMetrics, TaskMetrics};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;

#[derive(Serialize, Deserialize, Debug)]
pub struct LegacyToTaosMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    #[serde(default)]
    pub read_concurrency: AtomicU32,
    #[serde(default)]
    pub total_stables: AtomicU32,
    #[serde(default)]
    pub total_tables: AtomicU32,
    #[serde(default)]
    pub total_finished_tables: AtomicU32,
    #[serde(default)]
    pub total_success_blocks: AtomicU64,
    #[serde(default)]
    pub total_updated_tags: AtomicU32,
    #[serde(default)]
    pub total_created_tables: AtomicU32,
    #[serde(skip)]
    pub finished_tables: AtomicU32,
    #[serde(default)]
    pub success_blocks: AtomicU64,
    #[serde(default)]
    pub updated_tags: AtomicU32,
    #[serde(default)]
    pub created_tables: AtomicU32,
}

impl LegacyToTaosMetrics {
    pub fn new(stable: String, task_id: i64, task_name: Option<String>) -> Self {
        Self {
            com: CommonMetrics::new(stable, task_id, task_name),
            read_concurrency: Default::default(),
            total_stables: Default::default(),
            total_tables: Default::default(),
            total_finished_tables: Default::default(),
            total_success_blocks: Default::default(),
            total_updated_tags: Default::default(),
            total_created_tables: Default::default(),
            finished_tables: Default::default(),
            success_blocks: Default::default(),
            updated_tags: Default::default(),
            created_tables: Default::default(),
        }
    }

    pub fn add_success_blocks(&self, n: u64) {
        self.total_success_blocks.fetch_add(n, SeqCst);
        self.success_blocks.fetch_add(n, SeqCst);
    }

    pub fn add_updated_tags(&self, n: u32) {
        self.total_updated_tags.fetch_add(n, SeqCst);
        self.updated_tags.fetch_add(n, SeqCst);
    }

    pub fn add_created_tables(&self, n: u32) {
        self.total_created_tables.fetch_add(n, SeqCst);
        self.created_tables.fetch_add(n, SeqCst);
    }

    pub fn add_finished_tables(&self, n: u32) {
        self.total_finished_tables.fetch_add(n, SeqCst);
        self.finished_tables.fetch_add(n, SeqCst);
    }

    pub fn finished_tables(&self) -> u32 {
        self.finished_tables.load(SeqCst)
    }

    pub fn total_tables(&self) -> u32 {
        self.total_tables.load(SeqCst)
    }
}

impl Default for LegacyToTaosMetrics {
    fn default() -> Self {
        Self {
            com: CommonMetrics::default(),
            read_concurrency: AtomicU32::new(0),
            total_stables: AtomicU32::new(0),
            total_tables: AtomicU32::new(0),
            total_finished_tables: AtomicU32::new(0),
            total_success_blocks: AtomicU64::new(0),
            total_updated_tags: AtomicU32::new(0),
            total_created_tables: AtomicU32::new(0),
            finished_tables: AtomicU32::new(0),
            success_blocks: AtomicU64::new(0),
            updated_tags: AtomicU32::new(0),
            created_tables: AtomicU32::new(0),
        }
    }
}

impl Display for LegacyToTaosMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let records = self.com.written_rows.load(SeqCst);
        let points = self.com.written_points.load(SeqCst);
        let mut cost = ((Utc::now().timestamp_millis() - self.com.start_time.get()) / 1000) as u64;

        if cost == 0 {
            cost = 1;
        }
        write!(
            f,
            "# Metrics\n\
            read_concurrency: {}\n\
            created tables: {}\n\
            updated tags: {}\n\
            finished tables: {}\n\
            blocks: {}\n\
            records: {} ({} r/s)\n\
            points: {} ({} p/s)\n\
            time cost: {:?} s",
            self.read_concurrency.load(SeqCst),
            self.created_tables.load(SeqCst),
            self.updated_tags.load(SeqCst),
            self.finished_tables.load(SeqCst),
            self.success_blocks.load(SeqCst),
            records,
            records / cost,
            points,
            points / cost,
            cost
        )?;
        Ok(())
    }
}

impl TaskMetrics for LegacyToTaosMetrics {
    /// Reset run level metrics
    fn reset(&self) {
        self.com.reset();
        self.finished_tables.store(0, SeqCst);
        self.success_blocks.store(0, SeqCst);
        self.updated_tags.store(0, SeqCst);
        self.created_tables.store(0, SeqCst);
    }

    fn com(&self) -> &CommonMetrics {
        &self.com
    }

    /// Restore metrics from json string.
    fn from_json(json: &str) -> Option<Self> {
        match serde_json::from_str(json) {
            Ok(metrics) => metrics,
            Err(err) => {
                tracing::error!("failed to deserialize metrics: {}", err);
                None
            }
        }
    }
}

impl From<LegacyToTaosMetrics> for CoreMetrics {
    fn from(val: LegacyToTaosMetrics) -> Self {
        CoreMetrics::Legacy(val)
    }
}
