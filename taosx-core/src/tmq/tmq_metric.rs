use crate::core_metrics::{CommonMetrics, CoreMetrics, TaskMetrics};
use chrono::Utc;
use dashmap::DashMap;
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt::Display;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering::SeqCst;
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
    pub total_messages_of_meta: AtomicU64,
    #[serde(default)]
    pub total_messages_of_data: AtomicU64,
    #[serde(default)]
    pub total_write_raw_fails: AtomicU64,
    #[serde(default)]
    pub total_success_blocks: AtomicU64,
    #[serde(default)]
    pub messages: AtomicU64,
    #[serde(default)]
    pub messages_of_meta: AtomicU64,
    #[serde(default)]
    pub messages_of_data: AtomicU64,
    #[serde(default)]
    pub write_raw_fails: AtomicU64,
    #[serde(default)]
    pub success_blocks: AtomicU64,
    // Topic Name -> Vgroup ID -> Assignment
    #[serde(skip)]
    pub progress: DashMap<String, DashMap<i32, Assignment>>,
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
            total_messages_of_meta: AtomicU64::new(0),
            total_messages_of_data: AtomicU64::new(0),
            total_write_raw_fails: AtomicU64::new(0),
            total_success_blocks: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            messages_of_meta: AtomicU64::new(0),
            messages_of_data: AtomicU64::new(0),
            write_raw_fails: AtomicU64::new(0),
            success_blocks: AtomicU64::new(0),
            progress: DashMap::new(),
        }
    }
}

impl TmqMetrics {
    pub fn new(stable: String, task_id: i64, task_name: Option<String>) -> Self {
        Self {
            com: CommonMetrics::new(stable, task_id, task_name),
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
            progress: DashMap::new(),
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

    #[inline]
    pub fn update_progress(&self, assignments: Vec<(String, Vec<Assignment>)>) {
        for (topic, assignments) in assignments {
            let topic_progress = self.progress.entry(topic).or_insert_with(DashMap::new);
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
}

impl TaskMetrics for TmqMetrics {
    fn reset(&self) {
        self.com.reset();
        self.topics.store(0, SeqCst);
        self.consumers.store(0, SeqCst);
        self.messages.store(0, SeqCst);
        self.messages_of_meta.store(0, SeqCst);
        self.messages_of_data.store(0, SeqCst);
        self.write_raw_fails.store(0, SeqCst);
        self.success_blocks.store(0, SeqCst);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_progress_string() {
        let tmq_metrics = TmqMetrics::default();
        tmq_metrics.update_progress(vec![("topic1".to_string(), vec![Assignment::default()])]);
        let progress = tmq_metrics.get_progress_string();
        println!("{}", progress);
    }
}
