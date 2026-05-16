use pulsar::consumer::{InitialPosition, data::MessageData};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, atomic::AtomicUsize};
use taosx_core::{core_metrics::CoreMetrics, sink::ipc_metric::IpcMetrics};

use crate::config::connect::DataVendor;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Offset {
    pub ledger_id: u64,
    pub entry_id: u64,
}

pub struct CustomContext {
    pub offsets_cache: scc::HashIndex<(String, i32), MessageData>,
    /// Metric 1: joins, times that a retryable consumer joins to the group.
    joins: AtomicUsize,
    pub metrics: Arc<CoreMetrics>,
    pub initial_position: Option<InitialPosition>,
    pub seek_to: Option<Offset>, // 如果使用户指定 latest，可以使用 current timestamp 替代? 如何判断是否是已经存在的 subscription
    pub data_vendor: DataVendor,
    pub tuya_access_key: Option<String>,
}

impl CustomContext {
    pub fn fetch_add_joins(&self) -> usize {
        self.joins.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn current_joins(&self) -> usize {
        self.joins.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn new(metrics: Arc<CoreMetrics>, data_vendor: DataVendor) -> Self {
        Self {
            offsets_cache: scc::HashIndex::with_capacity(1),
            joins: AtomicUsize::new(0),
            metrics,
            initial_position: None,
            seek_to: None,
            data_vendor,
            tuya_access_key: None,
        }
    }

    pub fn metrics(&self) -> &IpcMetrics {
        self.metrics.ipc()
    }
}
