use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use taosx_core::core_metrics::CoreMetrics;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalToTaosMetrics {
    pub ipc: Arc<CoreMetrics>,
    pub processed_files: Arc<AtomicU64>,
    pub processed_bytes: Arc<AtomicU64>,
}

impl LocalToTaosMetrics {
    pub fn new(ipc: Arc<CoreMetrics>) -> Self {
        Self {
            ipc,
            processed_files: Arc::new(AtomicU64::new(0)),
            processed_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn add_received_batch(&self) {
        let metrics = self.ipc.ipc();
        metrics.received_batches.fetch_add(1, SeqCst);
        metrics.total_received_batches.fetch_add(1, SeqCst);
    }

    pub fn add_processed_files(&self, count: u64) {
        self.processed_files
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn add_processed_bytes(&self, count: u64) {
        self.processed_bytes
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn add_processed_batch(&self) {
        let metrics = self.ipc.ipc();
        metrics.processed_batches.fetch_add(1, SeqCst);
        metrics.total_processed_batches.fetch_add(1, SeqCst);
    }

    pub fn add_failed_batch(&self) {
        let metrics = self.ipc.ipc();
        metrics.failed_batches.fetch_add(1, SeqCst);
        metrics.total_failed_batches.fetch_add(1, SeqCst);
    }
}

impl Display for LocalToTaosMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "# local_to_taos Metrics\n\
            received_batches: {}\n\
            processed_batches: {}\n\
            failed_batches: {}\n\
            processed_files: {}\n\
            processed_bytes: {}\n",
            self.ipc.ipc().received_batches.load(SeqCst),
            self.ipc.ipc().processed_batches.load(SeqCst),
            self.ipc.ipc().failed_batches.load(SeqCst),
            self.processed_files.load(SeqCst),
            self.processed_bytes.load(SeqCst)
        )
    }
}
