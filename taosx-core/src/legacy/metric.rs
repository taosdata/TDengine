use crate::core_metrics::CommonMetrics;
use crate::core_metrics::CoreMetrics;
use crate::core_metrics::TaosXMetrics;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use tracing;

#[derive(Serialize, Deserialize, Debug)]
pub struct LegacyToTaosMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    // task level metrics
    pub workers: AtomicU32,
    pub total_stables: AtomicU32,
    pub total_tables: AtomicU32,
    pub total_finished_tables: AtomicU32,
    pub total_suc_blocks: AtomicU64,
    pub total_updated_tags: AtomicU32,
    pub total_created_tables: AtomicU32,
    // instant
    #[serde(skip)]
    pub finished_tables: AtomicU32,
    pub suc_blocks: AtomicU64,
    pub updated_tags: AtomicU32,
    pub created_tables: AtomicU32,
    // api level metrics (update on every api call)
    // total_avg_speed
    // execute_time
    // avg_speed
}

impl LegacyToTaosMetrics {
    pub fn add_suc_blocks(&self, n: u64) {
        self.total_suc_blocks.fetch_add(n, SeqCst);
        self.suc_blocks.fetch_add(n, SeqCst);
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
}

impl Default for LegacyToTaosMetrics {
    fn default() -> Self {
        Self {
            com: CommonMetrics::default(),
            workers: AtomicU32::new(0),
            total_stables: AtomicU32::new(0),
            total_tables: AtomicU32::new(0),
            total_finished_tables: AtomicU32::new(0),
            total_suc_blocks: AtomicU64::new(0),
            total_updated_tags: AtomicU32::new(0),
            total_created_tables: AtomicU32::new(0),
            finished_tables: AtomicU32::new(0),
            suc_blocks: AtomicU64::new(0),
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
            workers: {}\n\
            created tables: {}\n\
            updated tags: {}\n\
            finished tables: {}\n\
            blocks: {}\n\
            records: {} ({} r/s)\n\
            points: {} ({} p/s)\n\
            time cost: {:?} s",
            self.workers.load(SeqCst),
            self.created_tables.load(SeqCst),
            self.updated_tags.load(SeqCst),
            self.finished_tables.load(SeqCst),
            self.suc_blocks.load(SeqCst),
            records,
            records / cost,
            points,
            points / cost,
            cost
        )?;
        Ok(())
    }
}

impl TaosXMetrics for LegacyToTaosMetrics {
    fn from_json(json: &str) -> Self {
        match serde_json::from_str(json) {
            Ok(metrics) => metrics,
            Err(err) => {
                tracing::error!("failed to deserialize metrics: {}", err);
                Self::default()
            }
        }
    }

    /// Reset run level metrics
    fn reset(&self) {
        self.com.reset();
        self.finished_tables.store(0, SeqCst);
        self.suc_blocks.store(0, SeqCst);
        self.updated_tags.store(0, SeqCst);
        self.created_tables.store(0, SeqCst);
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn com(&self) -> &CommonMetrics {
        &self.com
    }
}

impl Into<CoreMetrics> for LegacyToTaosMetrics {
    fn into(self) -> CoreMetrics {
        CoreMetrics::Legacy(self)
    }
}
