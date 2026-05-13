use std::{
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
    time::{Duration, Instant},
};

use faststr::FastStr;
use parking_lot::RwLock;

use crate::core_metrics::CoreMetrics;

const SPB_FETCHED_ACKS: FastStr = FastStr::from_static_str("spb_fetched_acks");
const SPB_SENT_BATCHES: FastStr = FastStr::from_static_str("spb_sent_batches");
const SPB_RECEIVED_MESSAGES: FastStr = FastStr::from_static_str("spb_received_messages");
const SPB_RECEIVED_METRICS: FastStr = FastStr::from_static_str("spb_received_metrics");

pub struct Metrics {
    core: Arc<CoreMetrics>,
    last_report: RwLock<Instant>,
    report_interval: Duration,

    sent_batches: AtomicU64,
    fetched_acks: AtomicU64,
    received_messages: AtomicU64,
    received_metrics: AtomicU64,
}

impl Metrics {
    pub fn new(core: Arc<CoreMetrics>) -> Self {
        Self {
            core,
            last_report: RwLock::new(Instant::now()),
            report_interval: Duration::from_millis(100),
            sent_batches: AtomicU64::default(),
            fetched_acks: AtomicU64::default(),
            received_messages: AtomicU64::default(),
            received_metrics: AtomicU64::default(),
        }
    }

    pub fn add_fetched_acks(&self) {
        self.fetched_acks.fetch_add(1, atomic::Ordering::SeqCst);
        self.update_metrics();
    }

    pub fn fetched_acks(&self) -> u64 {
        self.fetched_acks.load(atomic::Ordering::SeqCst)
    }

    pub fn add_sent_batches(&self) {
        self.sent_batches.fetch_add(1, atomic::Ordering::SeqCst);
        self.update_metrics();
    }

    pub fn sent_batches(&self) -> u64 {
        self.sent_batches.load(atomic::Ordering::SeqCst)
    }

    pub fn add_received_messages(&self) {
        self.received_messages
            .fetch_add(1, atomic::Ordering::SeqCst);
        self.update_metrics();
    }

    pub fn received_messages(&self) -> u64 {
        self.received_messages.load(atomic::Ordering::SeqCst)
    }

    pub fn add_received_metrics(&self) {
        self.received_metrics.fetch_add(1, atomic::Ordering::SeqCst);
        self.update_metrics();
    }

    pub fn received_metrics(&self) -> u64 {
        self.received_metrics.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn update_metrics(&self) {
        if self.last_report.read().elapsed() < self.report_interval {
            return;
        }
        let Some(mut last_report) = self.last_report.try_write() else {
            return;
        };
        *last_report = Instant::now();

        let metrics = self.core.ipc();
        metrics.set_extra_metric(&SPB_FETCHED_ACKS, self.fetched_acks());
        metrics.set_extra_metric(&SPB_SENT_BATCHES, self.sent_batches());
        metrics.set_extra_metric(&SPB_RECEIVED_MESSAGES, self.received_messages());
        metrics.set_extra_metric(&SPB_RECEIVED_METRICS, self.received_metrics());
    }

    pub fn reset(&self) {
        let metrics = self.core.ipc();
        metrics.set_extra_metric(&SPB_FETCHED_ACKS, 0);
        metrics.set_extra_metric(&SPB_SENT_BATCHES, 0);
        metrics.set_extra_metric(&SPB_RECEIVED_MESSAGES, 0);
        metrics.set_extra_metric(&SPB_RECEIVED_METRICS, 0);
    }
}
