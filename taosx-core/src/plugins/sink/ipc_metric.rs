use crate::core_metrics::{CommonMetrics, CoreMetrics, TaskMetrics};
use faststr::FastStr;
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use std::sync::{OnceLock, atomic::Ordering::SeqCst};
use taosx_ipc::types::{TaskMetricItem, TaskMetricsVariant};

/// Metrics sender for agent.
///
/// Items: (task_id, key, value)
pub static AGENT_METRICS_SENDER: OnceLock<flume::Sender<TaskMetricItem>> = OnceLock::new();

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct IpcMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    /// 当前任务，收到的数据总批数
    pub total_received_batches: AtomicU64,
    /// 当前任务，成功处理的总批数
    pub total_processed_batches: AtomicU64,
    /// 当前任务，处理失败的总批数
    pub total_failed_batches: AtomicU64,
    /// 当前任务，执行解析后的总行数
    pub total_parsed_rows: AtomicU64,
    /// 当前任务，执行解析后过滤器筛掉的总行数
    pub total_filter_skipped_rows: AtomicU64,
    /// 当前任务，执行解析后前置合法检查筛掉的行数
    pub total_check_skipped_rows: AtomicU64,
    /// 当前任务，待写入的行数
    pub total_write_ready_rows: AtomicU64,
    /// 当前任务，成功处理的总行数
    pub total_processed_rows: AtomicU64,
    /// 当前任务，处理失败的总行数
    pub total_failed_rows: AtomicU64,
    /// 当前任务，时间戳无法入库的总行数
    pub total_drained_rows: AtomicU64,
    /// 当前任务，归档数据总行数
    pub total_archived_rows: AtomicU64,
    /// 当前任务，写入失败的测点数
    pub total_failed_points: AtomicU64,
    /// 当前任务，执行的 INSERT SQL 总条数
    pub total_inserted_sqls: AtomicU64,
    /// 当前任务，执行失败的 INSERT SQL 总条数
    pub total_failed_sqls: AtomicU64,
    /// 当前任务，创建的超级表总数
    pub total_created_stables: AtomicU64,
    /// 当前任务，尝试创建子表总数
    pub total_created_tables: AtomicU64,
    /// 当前任务，写入成功的 raw block 总数
    pub total_written_raw_blocks: AtomicU64,
    /// 当前任务，写入失败的 raw block 总数
    pub total_failed_raw_blocks: AtomicU64,

    /// 本次运行，收到的数据总批数
    pub received_batches: AtomicU64,
    /// 本次运行，成功的批数
    pub processed_batches: AtomicU64,
    /// 本次运行，失败的批数
    pub failed_batches: AtomicU64,
    /// 本次运行，执行解析后行数
    pub parsed_rows: AtomicU64,
    /// 本次运行，过滤器筛掉的行数
    pub filter_skipped_rows: AtomicU64,
    /// 本次运行，前置合法检查筛掉的行数
    pub check_skipped_rows: AtomicU64,
    /// 本次运行，待写入的行数
    pub write_ready_rows: AtomicU64,
    /// 本次运行，写入成功的行数
    pub processed_rows: AtomicU64,
    /// 本次运行，写入失败的行数
    pub failed_rows: AtomicU64,
    /// 本次运行，写入失败后，跳过的行数（无法恢复）
    pub drained_rows: AtomicU64,
    /// 本次运行，写入失败后，归档的行数（可以恢复）
    pub archived_rows: AtomicU64,
    /// 本次运行，任务执行的 INSERT SQL 总条数
    pub inserted_sqls: AtomicU64,
    /// 本次运行，执行失败的 INSERT SQL 总条数
    pub failed_sqls: AtomicU64,
    /// 本次运行，创建的超级表总数
    pub created_stables: AtomicU64,
    /// 本次运行，创建的子表总数
    pub created_tables: AtomicU64,
    /// 本次运行，写入失败的总点数
    pub failed_points: AtomicU64,
    /// 本次运行，写入成功的 raw block 总数
    pub written_raw_blocks: AtomicU64,
    /// 本次运行，写入失败的 raw block 总数
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

    fn task_id(&self) -> i64 {
        self.com.task_id
    }

    /// parsed_rows - filter_skipped_rows - check_skipped_rows = write_ready_rows
    pub fn add_parsed_rows(&self, n: u64) {
        self.total_parsed_rows.fetch_add(n, SeqCst);
        self.parsed_rows.fetch_add(n, SeqCst);
    }

    pub fn add_filter_skipped_rows(&self, n: u64) {
        self.total_filter_skipped_rows.fetch_add(n, SeqCst);
        self.filter_skipped_rows.fetch_add(n, SeqCst);
    }

    pub fn add_check_skipped_rows(&self, n: u64) {
        self.total_check_skipped_rows.fetch_add(n, SeqCst);
        self.check_skipped_rows.fetch_add(n, SeqCst);
    }

    pub fn add_write_ready_rows(&self, n: u64) {
        self.total_write_ready_rows.fetch_add(n, SeqCst);
        self.write_ready_rows.fetch_add(n, SeqCst);
    }

    pub fn set_extra_metric(&self, key: &FastStr, value: u64) {
        if let Some(entry) = self.extras.get(key) {
            entry.update(value);
        } else {
            self.extras.entry(key.clone()).or_insert_with(|| value);
        }
        if let Some(sender) = AGENT_METRICS_SENDER.get() {
            let _ = sender.try_send((self.task_id(), key.clone(), TaskMetricsVariant::Set, value));
        }
    }

    pub fn add_extra_metric(&self, key: &FastStr, value: u64) {
        if let Some(entry) = self.extras.get(key) {
            let new = *entry.get() + value;
            entry.update(new);
        } else {
            self.extras.entry(key.clone()).or_insert_with(|| value);
        }

        if let Some(sender) = AGENT_METRICS_SENDER.get() {
            let _ = sender.try_send((self.task_id(), key.clone(), TaskMetricsVariant::Inc, value));
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
        if let Some(sender) = AGENT_METRICS_SENDER.get() {
            let _ = sender.try_send((self.task_id(), key.clone(), TaskMetricsVariant::Dec, value));
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

    #[inline]
    pub fn add_archived_rows(&self, n: u64) {
        self.total_archived_rows.fetch_add(n, SeqCst);
        self.archived_rows.fetch_add(n, SeqCst);
    }
}

impl From<IpcMetrics> for CoreMetrics {
    fn from(val: IpcMetrics) -> Self {
        CoreMetrics::IPC(val)
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
        self.archived_rows.store(0, SeqCst);

        self.extras.retain(|k, _| k.contains("total"));
    }

    fn com(&self) -> &CommonMetrics {
        &self.com
    }

    /// Restore metrics from json string.
    fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str(json)
            .inspect_err(|err| {
                tracing::error!("failed to deserialize metrics: {:?}", err);
            })
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_metrics::TaskMetrics;
    use faststr::FastStr;

    #[test]
    fn test_counters_and_reset() {
        let metrics = IpcMetrics::new("stable".into(), 7, Some("task".into()));

        metrics.add_received_batches(3);
        metrics.add_processed_batches(2);
        metrics.add_failed_batches(1);
        metrics.add_processed_rows(5);
        metrics.add_failed_rows(1);
        metrics.add_drained_rows(7);
        metrics.add_archived_rows(9);
        metrics.add_inserted_sqls(4);
        metrics.add_failed_sqls(1);
        metrics.add_created_stables(1);
        metrics.add_created_tables(2);
        metrics.add_failed_points(1);
        metrics.add_written_raw_blocks(2);
        metrics.add_failed_raw_blocks(1);
        metrics.com().add_received_messages(5);
        metrics.com().add_processed_messages(4);

        let foo = FastStr::from_static_str("foo");
        let total_bar = FastStr::from_static_str("total_bar");
        metrics.set_extra_metric(&foo, 10);
        metrics.set_extra_metric(&total_bar, 20);

        assert_eq!(metrics.total_received_batches(), 3);
        assert_eq!(metrics.received_batches.load(SeqCst), 3);
        assert_eq!(metrics.processed_batches.load(SeqCst), 2);
        assert_eq!(metrics.processed_rows.load(SeqCst), 5);
        assert_eq!(metrics.total_processed_rows.load(SeqCst), 5);
        assert_eq!(metrics.failed_rows.load(SeqCst), 1);
        assert_eq!(metrics.total_failed_rows.load(SeqCst), 1);
        assert_eq!(metrics.total_created_stables.load(SeqCst), 1);
        assert_eq!(metrics.total_created_tables.load(SeqCst), 2);
        assert_eq!(metrics.failed_points.load(SeqCst), 1);
        assert_eq!(metrics.written_raw_blocks.load(SeqCst), 2);
        assert_eq!(metrics.failed_raw_blocks.load(SeqCst), 1);
        assert_eq!(metrics.drained_rows.load(SeqCst), 7);
        assert_eq!(metrics.archived_rows.load(SeqCst), 9);
        assert_eq!(metrics.com().received_messages(), 5);
        assert_eq!(metrics.com().processed_messages(), 4);
        assert_eq!(*metrics.extras.get(&foo).unwrap().get(), 10);
        assert_eq!(*metrics.extras.get(&total_bar).unwrap().get(), 20);

        metrics.reset();

        assert_eq!(metrics.received_batches.load(SeqCst), 0);
        assert_eq!(metrics.processed_batches.load(SeqCst), 0);
        assert_eq!(metrics.processed_rows.load(SeqCst), 0);
        assert_eq!(metrics.failed_rows.load(SeqCst), 0);
        assert_eq!(metrics.drained_rows.load(SeqCst), 0);
        assert_eq!(metrics.archived_rows.load(SeqCst), 0);
        assert_eq!(metrics.written_raw_blocks.load(SeqCst), 0);
        assert_eq!(metrics.failed_raw_blocks.load(SeqCst), 0);
        assert_eq!(metrics.failed_batches.load(SeqCst), 0);
        assert_eq!(metrics.com().received_messages(), 0);
        assert_eq!(metrics.com().processed_messages(), 0);

        // Totals remain accumulated
        assert_eq!(metrics.total_received_batches(), 3);
        assert_eq!(metrics.total_processed_rows.load(SeqCst), 5);
        assert_eq!(metrics.total_failed_rows.load(SeqCst), 1);
        assert_eq!(metrics.total_created_stables.load(SeqCst), 1);
        assert_eq!(metrics.total_created_tables.load(SeqCst), 2);
        assert_eq!(metrics.total_failed_points.load(SeqCst), 1);
        assert_eq!(metrics.total_written_raw_blocks.load(SeqCst), 2);
        assert_eq!(metrics.total_failed_raw_blocks.load(SeqCst), 1);
        assert_eq!(metrics.total_failed_batches.load(SeqCst), 1);
        assert_eq!(metrics.total_archived_rows.load(SeqCst), 9);

        // Extras without "total" are dropped on reset
        assert!(metrics.extras.get(&foo).is_none());
        assert_eq!(*metrics.extras.get(&total_bar).unwrap().get(), 20);
    }

    #[test]
    fn test_json_roundtrip() {
        let metrics = IpcMetrics::new("stable".into(), 9, None);
        metrics.add_processed_rows(11);
        metrics.add_failed_rows(2);
        let key = FastStr::from_static_str("total_custom");
        metrics.set_extra_metric(&key, 5);

        let json = metrics.to_json();
        let decoded = IpcMetrics::from_json(&json).expect("should deserialize");

        assert_eq!(decoded.com.stable, "stable");
        assert_eq!(decoded.task_id(), 9);
        assert_eq!(
            decoded.total_processed_rows.load(SeqCst),
            metrics.total_processed_rows.load(SeqCst)
        );
        assert_eq!(
            decoded.total_failed_rows.load(SeqCst),
            metrics.total_failed_rows.load(SeqCst)
        );
        assert_eq!(*decoded.extras.get(&key).unwrap().get(), 5);
    }
}
