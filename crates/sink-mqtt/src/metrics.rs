use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use faststr::FastStr;
use parking_lot::RwLock;

use taosx_core::core_metrics::CoreMetrics;

const METRICS_RECEIVED_MESSAGES: FastStr = FastStr::from_static_str("tmq2mqtt_received_messages");
const METRICS_PUBLISHED_MESSAGES: FastStr = FastStr::from_static_str("tmq2mqtt_published_messages");

pub struct Metrics {
    core: Arc<CoreMetrics>,

    last_report: RwLock<Instant>,
    report_interval: Duration,

    pub received_messages: AtomicU64,
    pub published_messages: AtomicU64,
}

impl Metrics {
    pub fn new(core: Arc<CoreMetrics>) -> Self {
        Self {
            core,
            last_report: RwLock::new(Instant::now()),
            report_interval: Duration::from_millis(500),
            received_messages: AtomicU64::default(),
            published_messages: AtomicU64::default(),
        }
    }

    pub fn add_received_messages(&self, val: u64) {
        self.received_messages.fetch_add(val, Ordering::SeqCst);
        self.update();
    }

    pub fn received_messages(&self) -> u64 {
        self.received_messages.load(Ordering::SeqCst)
    }

    pub fn add_published_messages(&self) {
        self.published_messages.fetch_add(1, Ordering::SeqCst);
        self.update();
    }

    pub fn published_messages(&self) -> u64 {
        self.published_messages.load(Ordering::SeqCst)
    }

    pub fn update(&self) {
        if !self.can_report() {
            return;
        }
        let Some(mut last_report) = self.last_report.try_write() else {
            return;
        };
        *last_report = Instant::now();

        let metrics = self.core.ipc();
        metrics.set_extra_metric(&METRICS_RECEIVED_MESSAGES, self.received_messages());
        metrics.set_extra_metric(&METRICS_PUBLISHED_MESSAGES, self.published_messages());
    }

    fn can_report(&self) -> bool {
        self.last_report.read().elapsed() >= self.report_interval
    }

    pub fn reset(&self) {
        let metrics = self.core.ipc();
        metrics.set_extra_metric(&METRICS_RECEIVED_MESSAGES, 0);
        metrics.set_extra_metric(&METRICS_PUBLISHED_MESSAGES, 0);
    }
}
