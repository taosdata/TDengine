use crate::core_metrics::{CommonMetrics, CoreMetrics, TaskMetrics};
use chrono::Utc;
use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::SeqCst;
use std::time::{Duration, Instant};
use taos::taos_query::tmq::Assignment;

#[derive(Serialize, Deserialize, Debug)]
pub struct TmqMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    #[serde(default)]
    pub topics: AtomicU16,
    #[serde(default)]
    pub consumers: AtomicU16,
    #[serde(default)]
    pub total_messages: AtomicU64,
    #[serde(default)]
    pub total_messages_bytes: AtomicU64,
    #[serde(default)]
    pub total_messages_of_meta: AtomicU64,
    #[serde(default)]
    pub total_messages_of_data: AtomicU64,
    #[serde(default)]
    pub total_write_raw_fails: AtomicU64,
    #[serde(default)]
    pub total_success_blocks: AtomicU64,
    #[serde(default)]
    pub total_success_messages: AtomicU64,
    #[serde(default)]
    pub messages: AtomicU64,
    #[serde(default)]
    pub messages_bytes: AtomicU64,
    #[serde(default)]
    pub messages_of_meta: AtomicU64,
    #[serde(default)]
    pub messages_of_data: AtomicU64,
    #[serde(default)]
    pub success_messages: AtomicU64,
    #[serde(default)]
    pub write_raw_fails: AtomicU64,
    #[serde(default)]
    pub success_blocks: AtomicU64,
    #[serde(default)]
    pub out_of_range_rows: AtomicU64,
    #[serde(default)]
    pub total_out_of_range_rows: AtomicU64,
    #[serde(default)]
    pub total_consume_cost_ms: AtomicU64,
    #[serde(default)]
    pub total_write_raw_cost_ms: AtomicU64,
    #[serde(default)]
    pub total_write_cost_ms: AtomicU64,
    #[serde(default)]
    pub commits: AtomicU64,
    // Topic Name -> Vgroup ID -> Assignment
    #[serde(skip)]
    pub progress: DashMap<String, DashMap<i32, Assignment>>,

    /// Last message timestamp in milliseconds.
    #[serde(skip, default = "default_instant")]
    pub last_message_instant: AtomicCell<Instant>,
}

fn default_instant() -> AtomicCell<Instant> {
    AtomicCell::new(Instant::now())
}
#[derive(Serialize, Deserialize, Debug)]
struct TopicProgress {
    pub topic: String,
    pub vgroup: i32,
    pub offset: i64,
    pub latest: i64,
}

impl Default for TmqMetrics {
    fn default() -> Self {
        Self {
            com: CommonMetrics::default(),
            topics: AtomicU16::new(0),
            consumers: AtomicU16::new(0),
            total_messages: AtomicU64::new(0),
            total_messages_bytes: AtomicU64::new(0),
            total_messages_of_meta: AtomicU64::new(0),
            total_messages_of_data: AtomicU64::new(0),
            total_write_raw_fails: AtomicU64::new(0),
            total_success_blocks: AtomicU64::new(0),
            total_success_messages: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            messages_bytes: AtomicU64::new(0),
            messages_of_meta: AtomicU64::new(0),
            messages_of_data: AtomicU64::new(0),
            success_messages: AtomicU64::new(0),
            write_raw_fails: AtomicU64::new(0),
            success_blocks: AtomicU64::new(0),
            out_of_range_rows: AtomicU64::new(0),
            total_out_of_range_rows: AtomicU64::new(0),
            total_consume_cost_ms: AtomicU64::new(0),
            total_write_raw_cost_ms: AtomicU64::new(0),
            total_write_cost_ms: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            progress: DashMap::new(),
            last_message_instant: AtomicCell::new(Instant::now()),
        }
    }
}

use std::ops::AddAssign;
use std::sync::atomic::Ordering;

impl AddAssign for TmqMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.com += rhs.com;

        self.topics
            .fetch_add(rhs.topics.load(Ordering::Relaxed), Ordering::Relaxed);
        self.consumers
            .fetch_add(rhs.consumers.load(Ordering::Relaxed), Ordering::Relaxed);

        self.total_messages.fetch_add(
            rhs.total_messages.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_messages_bytes.fetch_add(
            rhs.total_messages_bytes.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_messages_of_meta.fetch_add(
            rhs.total_messages_of_meta.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_messages_of_data.fetch_add(
            rhs.total_messages_of_data.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_write_raw_fails.fetch_add(
            rhs.total_write_raw_fails.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_success_blocks.fetch_add(
            rhs.total_success_blocks.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_success_messages.fetch_add(
            rhs.total_success_messages.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.messages
            .fetch_add(rhs.messages.load(Ordering::Relaxed), Ordering::Relaxed);
        self.messages_bytes.fetch_add(
            rhs.messages_bytes.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.messages_of_meta.fetch_add(
            rhs.messages_of_meta.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.messages_of_data.fetch_add(
            rhs.messages_of_data.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.success_messages.fetch_add(
            rhs.success_messages.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.write_raw_fails.fetch_add(
            rhs.write_raw_fails.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.success_blocks.fetch_add(
            rhs.success_blocks.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.out_of_range_rows.fetch_add(
            rhs.out_of_range_rows.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_out_of_range_rows.fetch_add(
            rhs.total_out_of_range_rows.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_consume_cost_ms.fetch_add(
            rhs.total_consume_cost_ms.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_write_raw_cost_ms.fetch_add(
            rhs.total_write_raw_cost_ms.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.total_write_cost_ms.fetch_add(
            rhs.total_write_cost_ms.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.commits
            .fetch_add(rhs.commits.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

impl TmqMetrics {
    pub fn new(stable: String, task_id: i64, job_id: i64) -> Self {
        Self {
            com: CommonMetrics::new(stable, task_id, job_id),
            ..Default::default()
        }
    }

    pub fn last_message_elapsed(&self) -> Duration {
        self.last_message_instant.load().elapsed()
    }

    pub fn update_last_message_instant(&self) {
        self.last_message_instant.store(Instant::now());
    }

    #[inline]
    pub fn add_messages(&self, n: u64) {
        self.total_messages.fetch_add(n, SeqCst);
        self.messages.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_message_bytes(&self, bytes: u64) {
        self.total_messages_bytes.fetch_add(bytes, SeqCst);
        self.messages_bytes.fetch_add(bytes, SeqCst);
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
    pub fn add_success_messages(&self, n: u64) {
        self.total_success_messages.fetch_add(n, SeqCst);
        self.success_messages.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_suc_blocks(&self, n: u64) {
        self.total_success_blocks.fetch_add(n, SeqCst);
        self.success_blocks.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn add_out_of_range_rows(&self, n: u64) {
        self.total_out_of_range_rows.fetch_add(n, SeqCst);
        self.out_of_range_rows.fetch_add(n, SeqCst);
    }

    #[inline]
    pub fn update_progress(&self, assignments: HashMap<&str, Vec<Assignment>>) {
        for (topic, assignments) in assignments {
            if !self.progress.contains_key(topic) {
                self.progress.insert(topic.to_string(), DashMap::new());
            }
            if let Some(topic_progress) = self.progress.get_mut(topic) {
                for assignment in assignments {
                    topic_progress.insert(assignment.vgroup_id(), assignment);
                }
            }
        }
    }
    #[inline]
    pub fn update_progress_of_topic(&self, topic: &str, assignments: Vec<Assignment>) {
        if !self.progress.contains_key(topic) {
            self.progress.insert(topic.to_string(), DashMap::new());
        }
        if let Some(topic_progress) = self.progress.get_mut(topic) {
            for assignment in assignments {
                topic_progress.insert(assignment.vgroup_id(), assignment);
            }
        }
    }

    pub fn get_progress_string(&self) -> String {
        let ts = chrono::Utc::now().timestamp_millis();
        let mut data = Vec::<TopicProgress>::new();
        for entry in self.progress.iter() {
            let topic = entry.key().clone();
            let topic_progress = entry.value();
            for entry in topic_progress {
                let assignment = entry.value();
                data.push(TopicProgress {
                    topic: topic.clone(),
                    vgroup: assignment.vgroup_id(),
                    offset: assignment.current_offset(),
                    latest: assignment.end(),
                });
            }
        }
        let json_value = json!({
            "update_time": ts,
            "data": data,
        });
        serde_json::to_string(&json_value).unwrap()
    }

    pub fn add_consume_cost_ms(&self, n: u64) {
        self.total_consume_cost_ms.fetch_add(n, SeqCst);
    }

    pub fn add_write_cost_ms(&self, n: u64) {
        self.total_write_cost_ms.fetch_add(n, SeqCst);
    }

    pub fn add_write_raw_cost_ms(&self, n: u64) {
        self.total_write_raw_cost_ms.fetch_add(n, SeqCst);
    }

    pub fn add_commits(&self, n: u64) {
        self.commits.fetch_add(n, SeqCst);
    }
}

impl TaskMetrics for TmqMetrics {
    fn reset(&self) {
        self.com.reset();
        self.topics.store(0, SeqCst);
        self.consumers.store(0, SeqCst);
        self.messages.store(0, SeqCst);
        self.messages_bytes.store(0, SeqCst);
        self.messages_of_meta.store(0, SeqCst);
        self.messages_of_data.store(0, SeqCst);
        self.success_messages.store(0, SeqCst);
        self.write_raw_fails.store(0, SeqCst);
        self.success_blocks.store(0, SeqCst);
        self.out_of_range_rows.store(0, SeqCst);
        self.commits.store(0, SeqCst);
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

impl From<TmqMetrics> for CoreMetrics {
    fn from(val: TmqMetrics) -> Self {
        CoreMetrics::TMQ(val)
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
            commits: {}\n",
            self.topics.load(SeqCst),
            self.consumers.load(SeqCst),
            self.messages.load(std::sync::atomic::Ordering::SeqCst),
            self.messages_of_meta
                .load(std::sync::atomic::Ordering::SeqCst),
            self.messages_of_data
                .load(std::sync::atomic::Ordering::SeqCst),
            self.commits.load(SeqCst),
        )?;

        if self.messages.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            write!(
                f,
                "consume poll ms: {}\n\
                write cost ms: {}(api cost: {})\n",
                self.total_consume_cost_ms.load(SeqCst),
                self.total_write_cost_ms.load(SeqCst),
                self.total_write_raw_cost_ms.load(SeqCst),
            )?;
        }

        let out_range_rows = self.out_of_range_rows.load(SeqCst);
        if out_range_rows > 0 {
            writeln!(f, "out of range rows: {out_range_rows}")?;
        }

        let blocks = self.success_blocks.load(SeqCst);
        if blocks > 0 {
            write!(
                f,
                "blocks: {}\n\
                records: {} ({} r/s)\n\
                points: {} ({} p/s)\n\
                time cost: {:?}",
                blocks,
                records,
                records / cost,
                points,
                points / cost,
                cost
            )?;
        } else {
            write!(f, "time cost: {:?}", cost)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_progress_string() {
        let tmq_metrics = TmqMetrics::default();
        tmq_metrics.update_progress(HashMap::from_iter([(
            "topic1",
            vec![Assignment::default()],
        )]));
        tmq_metrics.update_progress_of_topic("topic2", vec![Assignment::default()]);
        let progress = tmq_metrics.get_progress_string();
        println!("{}", progress);
    }
}
