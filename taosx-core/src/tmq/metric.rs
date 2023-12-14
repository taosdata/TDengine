use crate::core_metrics::{CoreMetrics, TaosXMetrics, CommonMetrics};
use metrics::atomics::AtomicU64;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU16;
use tracing;

#[derive(Serialize, Deserialize, Debug)]
pub struct TMQMetrics {
    #[serde(flatten)]
    pub com: CommonMetrics,
    pub topics: AtomicU16,
    pub workers: AtomicU16,
    pub total_messages: AtomicU64,
    pub total_messages_of_meta: AtomicU64,
    pub total_messages_of_data: AtomicU64,
    pub total_write_meta_fails: AtomicU64,
    pub messages: AtomicU64,
    pub messages_of_meta: AtomicU64,
    pub messages_of_data: AtomicU64,
    pub write_meta_fails: AtomicU64,
}

impl Default for TMQMetrics {
    fn default() -> Self {
        Self {
            com: CommonMetrics::default(),
            topics: AtomicU16::new(0),
            workers: AtomicU16::new(0),
            total_messages: AtomicU64::new(0),
            total_messages_of_meta: AtomicU64::new(0),
            total_messages_of_data: AtomicU64::new(0),
            total_write_meta_fails: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            messages_of_meta: AtomicU64::new(0),
            messages_of_data: AtomicU64::new(0),
            write_meta_fails: AtomicU64::new(0),
        }
    }
}

impl TaosXMetrics for TMQMetrics {

    fn reset(&self) {
        todo!()
    }

    fn com(&self) -> &crate::core_metrics::CommonMetrics {
        todo!()
    }

    /// Resore metrics from json string.
    fn from_json(json: &str) -> Self {
        match serde_json::from_str(json) {
            Ok(metrics) => metrics,
            Err(err) => {
                tracing::error!("failed to deserialize metrics: {}", err);
                Self::default()
            }
        }
    }
    
}

impl Into<CoreMetrics> for TMQMetrics {
    fn into(self) -> CoreMetrics {
        CoreMetrics::TMQ(self)
    }
}
