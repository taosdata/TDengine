use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;

use crate::core_metrics::CoreMetrics;
use crate::core_metrics::TaosXMetrics;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Instant;

#[derive(Serialize, Deserialize, Debug)]
pub struct LegacyToTaosMetrics {
    // task level metrics
    pub workers: AtomicU32,
    pub total_stables: AtomicU32,
    pub total_tables: AtomicU32,
    pub finished_tables: AtomicU32,
    pub suc_blocks: AtomicU64,
    pub suc_records: AtomicU64,
    pub suc_points: AtomicU64,
    pub updated_tags: AtomicU32,
    pub created_tables: AtomicU32,
    // total execute time in seconds
    pub total_execute_time: AtomicU64,
    // instant
    #[serde(skip)]
    #[serde(default = "Instant::now")]
    pub last_persist_time: Instant,
    // all metrics bellow are for current run
    pub start_time: i64,
    pub current_finished_tables: AtomicU32,
    pub current_suc_blocks: AtomicU64,
    pub current_suc_records: AtomicU64,
    pub current_suc_points: AtomicU64,
    pub current_updated_tags: AtomicU32,
    pub current_created_tables: AtomicU32,
    // api level metrics (update on every api call)
    // current_execute_time
    // current_avg_speed
}

impl Default for LegacyToTaosMetrics {
    fn default() -> Self {
        Self {
            workers: AtomicU32::new(0),
            total_stables: AtomicU32::new(0),
            total_tables: AtomicU32::new(0),
            finished_tables: AtomicU32::new(0),
            suc_blocks: AtomicU64::new(0),
            suc_records: AtomicU64::new(0),
            suc_points: AtomicU64::new(0),
            updated_tags: AtomicU32::new(0),
            created_tables: AtomicU32::new(0),
            total_execute_time: AtomicU64::new(0),
            last_persist_time: Instant::now(),
            start_time: Utc::now().timestamp_millis(),
            current_finished_tables: AtomicU32::new(0),
            current_suc_blocks: AtomicU64::new(0),
            current_suc_records: AtomicU64::new(0),
            current_suc_points: AtomicU64::new(0),
            current_updated_tags: AtomicU32::new(0),
            current_created_tables: AtomicU32::new(0),
        }
    }
}

impl Display for LegacyToTaosMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let records = self.current_suc_records.load(SeqCst);
        let points = self.current_suc_points.load(SeqCst);
        let mut cost = ((Utc::now().timestamp_millis() - self.start_time) / 1000) as u64;

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
            self.current_created_tables.load(SeqCst),
            self.current_updated_tags.load(SeqCst),
            self.current_finished_tables.load(SeqCst),
            self.current_suc_blocks.load(SeqCst),
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
        serde_json::from_str(json).unwrap()
    }

    /// Reset run level metrics
    fn reset(&self) {
        self.current_finished_tables.store(0, SeqCst);
        self.current_suc_blocks.store(0, SeqCst);
        self.current_suc_records.store(0, SeqCst);
        self.current_suc_points.store(0, SeqCst);
        self.current_updated_tags.store(0, SeqCst);
        self.current_created_tables.store(0, SeqCst);
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn update_total_execute_time(&self) {
        self.total_execute_time
            .fetch_add(self.last_persist_time.elapsed().as_secs(), SeqCst);
        // self.last_persist_time = Instant::now();
    }
}

impl Into<CoreMetrics> for LegacyToTaosMetrics {
    fn into(self) -> CoreMetrics {
        CoreMetrics::Legacy(self)
    }
}
