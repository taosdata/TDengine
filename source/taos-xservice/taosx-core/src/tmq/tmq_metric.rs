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
    // Topic Name -> Vgroup ID -> Assignment (in-memory only; not serialized).
    #[serde(skip)]
    pub progress: DashMap<String, DashMap<i32, Assignment>>,

    // Serializable snapshot of `progress`, refreshed on every progress update.
    // This is what xnoded persists to log.xnode_task_metrics and what explorer reads.
    #[serde(
        default,
        serialize_with = "serialize_progress_snapshot",
        deserialize_with = "deserialize_progress_snapshot"
    )]
    pub progress_snapshot: std::sync::Mutex<Vec<TopicProgress>>,

    /// Last message timestamp in milliseconds.
    #[serde(skip, default = "default_instant")]
    pub last_message_instant: AtomicCell<Instant>,
}

fn default_instant() -> AtomicCell<Instant> {
    AtomicCell::new(Instant::now())
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicProgress {
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
            progress_snapshot: std::sync::Mutex::new(Vec::new()),
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

        // Merge progress snapshots.
        // When deserialized, `self.progress` is empty and `self.progress_snapshot` holds
        // the persisted state. If `self` has live progress but no snapshot yet, collect it first.
        let mut lhs_snapshot = self
            .progress_snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut rhs_snapshot = rhs
            .progress_snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let lhs_items = if lhs_snapshot.is_empty() && !self.progress.is_empty() {
            self.collect_progress_snapshot()
        } else {
            std::mem::take(&mut *lhs_snapshot)
        };

        *lhs_snapshot = merge_progress_snapshots(lhs_items, std::mem::take(&mut *rhs_snapshot));
    }
}

/// Merges two progress snapshot vectors by `(topic, vgroup)`, taking max `offset` and `latest`.
/// Returns a sorted vector ordered by topic then vgroup.
fn merge_progress_snapshots(
    lhs: Vec<TopicProgress>,
    rhs: Vec<TopicProgress>,
) -> Vec<TopicProgress> {
    let mut merged = std::collections::BTreeMap::<(String, i32), (i64, i64)>::new();
    for item in lhs.into_iter().chain(rhs) {
        let slot = merged
            .entry((item.topic, item.vgroup))
            .or_insert((i64::MIN, i64::MIN));
        slot.0 = slot.0.max(item.offset);
        slot.1 = slot.1.max(item.latest);
    }
    merged
        .into_iter()
        .map(|((topic, vgroup), (offset, latest))| TopicProgress {
            topic,
            vgroup,
            offset,
            latest,
        })
        .collect()
}

impl TmqMetrics {
    pub fn new(stable: String, task_id: i64, job_id: i64, task_name: Option<String>) -> Self {
        Self {
            com: CommonMetrics::new(stable, task_id, job_id, task_name),
            ..Default::default()
        }
    }

    pub fn last_message_elapsed(&self) -> Duration {
        self.last_message_instant.load().elapsed()
    }

    pub fn update_last_message_instant(&self) {
        self.last_message_instant.store(Instant::now());
    }

    /// Collect current live progress from the in-memory `progress` DashMap.
    /// Returns a sorted vector of topic/vgroup progress entries.
    fn collect_progress_snapshot(&self) -> Vec<TopicProgress> {
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
        data.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.vgroup.cmp(&b.vgroup)));
        data
    }

    /// Rebuild `progress_snapshot` from the current `progress` DashMap.
    /// Called from `update_progress_of_topic` and `update_progress` so the
    /// next metrics flush sees an up-to-date snapshot.
    pub fn refresh_progress_snapshot(&self) {
        let data = self.collect_progress_snapshot();
        let mut guard = self
            .progress_snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = data;
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
        self.refresh_progress_snapshot();
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
        self.refresh_progress_snapshot();
    }

    pub fn get_progress_string(&self) -> String {
        let ts = chrono::Utc::now().timestamp_millis();
        let data = self.collect_progress_snapshot();
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

fn serialize_progress_snapshot<S>(
    snapshot: &std::sync::Mutex<Vec<TopicProgress>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let guard = snapshot
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    serde::Serialize::serialize(&*guard, serializer)
}

fn deserialize_progress_snapshot<'de, D>(
    deserializer: D,
) -> Result<std::sync::Mutex<Vec<TopicProgress>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v: Vec<TopicProgress> = serde::Deserialize::deserialize(deserializer)?;
    Ok(std::sync::Mutex::new(v))
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

    #[test]
    fn refresh_and_serialize_progress_snapshot_roundtrip() {
        let metrics = TmqMetrics::default();
        let mut assignments = HashMap::new();
        assignments.insert("topic_b", vec![Assignment::new(2, 200, 0, 400)]);
        assignments.insert("topic_a", vec![Assignment::new(1, 100, 0, 200)]);
        metrics.update_progress(assignments);

        metrics.update_progress_of_topic("topic_a", vec![Assignment::new(3, 150, 0, 300)]);

        let snapshot = metrics
            .progress_snapshot
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(snapshot.len(), 3);
        // Verify sorted order: (topic_a, vg1) < (topic_a, vg3) < (topic_b, vg2)
        assert_eq!(snapshot[0].topic, "topic_a");
        assert_eq!(snapshot[0].vgroup, 1);
        assert_eq!(snapshot[0].offset, 100);
        assert_eq!(snapshot[0].latest, 200);

        assert_eq!(snapshot[1].topic, "topic_a");
        assert_eq!(snapshot[1].vgroup, 3);
        assert_eq!(snapshot[1].offset, 150);
        assert_eq!(snapshot[1].latest, 300);

        assert_eq!(snapshot[2].topic, "topic_b");
        assert_eq!(snapshot[2].vgroup, 2);
        assert_eq!(snapshot[2].offset, 200);
        assert_eq!(snapshot[2].latest, 400);
        drop(snapshot);

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(
            json.contains("\"progress_snapshot\""),
            "json should contain progress_snapshot field: {json}"
        );
        let restored: TmqMetrics = serde_json::from_str(&json).unwrap();
        let restored_snapshot = restored
            .progress_snapshot
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(restored_snapshot.len(), 3);
        assert_eq!(restored_snapshot[0].topic, "topic_a");
        assert_eq!(restored_snapshot[0].vgroup, 1);
        assert_eq!(restored_snapshot[0].offset, 100);
        assert_eq!(restored_snapshot[0].latest, 200);

        assert_eq!(restored_snapshot[1].topic, "topic_a");
        assert_eq!(restored_snapshot[1].vgroup, 3);
        assert_eq!(restored_snapshot[1].offset, 150);
        assert_eq!(restored_snapshot[1].latest, 300);

        assert_eq!(restored_snapshot[2].topic, "topic_b");
        assert_eq!(restored_snapshot[2].vgroup, 2);
        assert_eq!(restored_snapshot[2].offset, 200);
        assert_eq!(restored_snapshot[2].latest, 400);
    }

    #[test]
    fn add_assign_merges_progress_snapshot() {
        let mut lhs = TmqMetrics::default();
        let rhs = TmqMetrics::default();

        // Populate lhs with (topic_a, vg1) and (topic_b, vg2)
        {
            let mut snapshot = lhs.progress_snapshot.lock().unwrap();
            snapshot.push(TopicProgress {
                topic: "topic_a".into(),
                vgroup: 1,
                offset: 100,
                latest: 200,
            });
            snapshot.push(TopicProgress {
                topic: "topic_b".into(),
                vgroup: 2,
                offset: 300,
                latest: 400,
            });
        }

        // Populate rhs with overlapping (topic_a, vg1) and new (topic_c, vg3)
        {
            let mut snapshot = rhs.progress_snapshot.lock().unwrap();
            snapshot.push(TopicProgress {
                topic: "topic_a".into(),
                vgroup: 1,
                offset: 150, // Higher offset
                latest: 250, // Higher latest
            });
            snapshot.push(TopicProgress {
                topic: "topic_c".into(),
                vgroup: 3,
                offset: 500,
                latest: 600,
            });
        }

        lhs += rhs;

        let merged = lhs.progress_snapshot.lock().unwrap();
        assert_eq!(merged.len(), 3, "should have 3 unique entries");

        // Verify sorted order and max values for duplicates
        assert_eq!(merged[0].topic, "topic_a");
        assert_eq!(merged[0].vgroup, 1);
        assert_eq!(merged[0].offset, 150); // max(100, 150)
        assert_eq!(merged[0].latest, 250); // max(200, 250)

        assert_eq!(merged[1].topic, "topic_b");
        assert_eq!(merged[1].vgroup, 2);
        assert_eq!(merged[1].offset, 300);
        assert_eq!(merged[1].latest, 400);

        assert_eq!(merged[2].topic, "topic_c");
        assert_eq!(merged[2].vgroup, 3);
        assert_eq!(merged[2].offset, 500);
        assert_eq!(merged[2].latest, 600);
    }

    #[test]
    fn add_assign_collects_live_progress_when_snapshot_empty() {
        let mut lhs = TmqMetrics::default();
        let rhs = TmqMetrics::default();

        // Populate lhs with live progress but no snapshot
        let mut assignments = HashMap::new();
        assignments.insert("topic_live", vec![Assignment::new(1, 100, 0, 200)]);
        lhs.update_progress(assignments);

        // Clear lhs snapshot to simulate live progress without snapshot refresh
        {
            let mut snapshot = lhs.progress_snapshot.lock().unwrap();
            snapshot.clear();
        }

        // Populate rhs with one snapshot entry
        {
            let mut snapshot = rhs.progress_snapshot.lock().unwrap();
            snapshot.push(TopicProgress {
                topic: "topic_rhs".into(),
                vgroup: 2,
                offset: 300,
                latest: 400,
            });
        }

        lhs += rhs;

        let merged = lhs.progress_snapshot.lock().unwrap();
        assert_eq!(merged.len(), 2, "should contain both live and rhs entries");

        // Verify sorted order: (topic_live, vg1) < (topic_rhs, vg2)
        assert_eq!(merged[0].topic, "topic_live");
        assert_eq!(merged[0].vgroup, 1);
        assert_eq!(merged[0].offset, 100);
        assert_eq!(merged[0].latest, 200);

        assert_eq!(merged[1].topic, "topic_rhs");
        assert_eq!(merged[1].vgroup, 2);
        assert_eq!(merged[1].offset, 300);
        assert_eq!(merged[1].latest, 400);
    }
}
