use crate::core_metrics::{CommonMetrics, CoreMetrics, TaosXMetrics};
use chrono::Utc;
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::SeqCst;
use tracing;

#[derive(Serialize, Deserialize, Debug)]
pub struct TmqMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    pub topics: AtomicU16,
    pub consumers: AtomicU16,
    pub total_messages: AtomicU64,
    pub total_messages_of_meta: AtomicU64,
    pub total_messages_of_data: AtomicU64,
    pub total_write_raw_fails: AtomicU64,
    pub total_success_blocks: AtomicU64,
    pub messages: AtomicU64,
    pub messages_of_meta: AtomicU64,
    pub messages_of_data: AtomicU64,
    pub write_raw_fails: AtomicU64,
    pub success_blocks: AtomicU64,
}

impl Default for TmqMetrics {
    fn default() -> Self {
        Self {
            com: CommonMetrics::default(),
            topics: AtomicU16::new(0),
            consumers: AtomicU16::new(0),
            total_messages: AtomicU64::new(0),
            total_messages_of_meta: AtomicU64::new(0),
            total_messages_of_data: AtomicU64::new(0),
            total_write_raw_fails: AtomicU64::new(0),
            total_success_blocks: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            messages_of_meta: AtomicU64::new(0),
            messages_of_data: AtomicU64::new(0),
            write_raw_fails: AtomicU64::new(0),
            success_blocks: AtomicU64::new(0),
        }
    }
}

impl TmqMetrics {
    pub fn new(task_id: i64) -> Self {
        Self {
            com: CommonMetrics::new(task_id),
            topics: AtomicU16::new(0),
            consumers: AtomicU16::new(0),
            total_messages: AtomicU64::new(0),
            total_messages_of_meta: AtomicU64::new(0),
            total_messages_of_data: AtomicU64::new(0),
            total_write_raw_fails: AtomicU64::new(0),
            total_success_blocks: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            messages_of_meta: AtomicU64::new(0),
            messages_of_data: AtomicU64::new(0),
            write_raw_fails: AtomicU64::new(0),
            success_blocks: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn add_messages(&self, n: u64) {
        self.total_messages.fetch_add(n, SeqCst);
        self.messages.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_messages_of_meta(&self, n: u64) -> u64 {
        self.total_messages_of_meta.fetch_add(n, SeqCst);
        self.messages_of_meta.fetch_add(n, SeqCst)
    }

    #[inline]
    pub fn add_messages_of_data(&self, n: u64) -> u64 {
        self.total_messages_of_data.fetch_add(n, SeqCst);
        self.messages_of_data.fetch_add(n, SeqCst)
    }

    #[inline]
    pub fn add_write_raw_fails(&self, n: u64) {
        self.total_write_raw_fails.fetch_add(n, SeqCst);
        self.write_raw_fails.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_suc_blocks(&self, n: u64) {
        self.total_success_blocks.fetch_add(n, SeqCst);
        self.success_blocks.fetch_add(n, SeqCst);
    }
}

impl TaosXMetrics for TmqMetrics {
    fn reset(&self) {
        self.com.reset();
        self.topics.store(0, SeqCst);
        self.consumers.store(0, SeqCst);
        self.messages.store(0, SeqCst);
        self.messages_of_meta.store(0, SeqCst);
        self.messages_of_data.store(0, SeqCst);
        self.write_raw_fails.store(0, SeqCst);
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

impl Into<CoreMetrics> for TmqMetrics {
    fn into(self) -> CoreMetrics {
        CoreMetrics::TMQ(self)
    }
}

impl Display for TmqMetrics {
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
            topics: {}\n\
            workers: {}\n\
            messages(total): {}\n\
            messages(meta only): {}\n\
            messages(data only): {}\n\
            blocks: {}\n\
            records: {} ({} r/s)\n\
            points: {} ({} p/s)\n\
            time cost: {:?}",
            self.topics.load(SeqCst),
            self.consumers.load(SeqCst),
            self.messages.load(std::sync::atomic::Ordering::SeqCst),
            self.messages_of_meta
                .load(std::sync::atomic::Ordering::SeqCst),
            self.messages_of_data
                .load(std::sync::atomic::Ordering::SeqCst),
            self.success_blocks
                .load(std::sync::atomic::Ordering::SeqCst),
            records,
            records / cost,
            points,
            points / cost,
            cost
        )?;
        Ok(())
    }
}
