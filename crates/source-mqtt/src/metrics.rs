use std::sync::{
    Arc,
    atomic::{self, AtomicU64},
};

use faststr::FastStr;

use taosx_core::core_metrics::CoreMetrics;

// metrics keys
const FETCHED_MESSAGES: FastStr = FastStr::from_static_str("mqtt_fetched_messages");
const DUMPED_MESSAGES: FastStr = FastStr::from_static_str("mqtt_dumped_messages");
const FETCHED_ACKS: FastStr = FastStr::from_static_str("mqtt_fetched_acks");
const ACK_FAILS: FastStr = FastStr::from_static_str("mqtt_ack_fails");
const UNPROCESSED_MESSAGES: FastStr = FastStr::from_static_str("mqtt_unprocessed_messages");
const SENT_BATCHES: FastStr = FastStr::from_static_str("mqtt_sent_batches");
const DISCARDED_MESSAGES: FastStr = FastStr::from_static_str("mqtt_discarded_messages");
const DISCARDED_DUMP_MESSAGES: FastStr = FastStr::from_static_str("mqtt_discarded_dump_messages");
const RECEIVED_BYTES: FastStr = FastStr::from_static_str("mqtt_received_bytes");

#[derive(Debug)]
pub(crate) struct MqttMetrics {
    metrics: Arc<CoreMetrics>,

    fetched_messages: AtomicU64,
    dumped_messages: AtomicU64,
    fetched_acks: AtomicU64,
    ack_fails: AtomicU64,
    unprocessed_messages: AtomicU64,
    sent_batches: AtomicU64,
    discard_messages: AtomicU64,
    discard_dump_messages: AtomicU64,
    received_bytes: AtomicU64,
}

impl MqttMetrics {
    pub(crate) fn new(metrics: Arc<CoreMetrics>) -> Self {
        Self {
            metrics,
            fetched_messages: AtomicU64::default(),
            dumped_messages: AtomicU64::default(),
            fetched_acks: AtomicU64::default(),
            ack_fails: AtomicU64::default(),
            unprocessed_messages: AtomicU64::default(),
            sent_batches: AtomicU64::default(),
            discard_messages: AtomicU64::default(),
            discard_dump_messages: AtomicU64::default(),
            received_bytes: AtomicU64::default(),
        }
    }
    pub(crate) fn add_fetched_messages(&self) {
        self.fetched_messages.fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn fetched_messages(&self) -> u64 {
        self.fetched_messages.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn add_dumped_messages(&self) {
        self.dumped_messages.fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn dumped_messages(&self) -> u64 {
        self.dumped_messages.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn add_fetched_acks(&self) {
        self.fetched_acks.fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn fetched_acks(&self) -> u64 {
        self.fetched_acks.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn add_ack_fails(&self) {
        self.ack_fails.fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn ack_fails(&self) -> u64 {
        self.ack_fails.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn add_unprocessed_messages(&self) {
        self.unprocessed_messages
            .fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn sub_unprocessed_messages(&self, value: u64) {
        self.unprocessed_messages
            .fetch_sub(value, atomic::Ordering::SeqCst);
    }

    pub(crate) fn unprocessed_messages(&self) -> u64 {
        self.unprocessed_messages.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn add_sent_batches(&self) {
        self.sent_batches.fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn sent_batches(&self) -> u64 {
        self.sent_batches.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn add_discarded_messages(&self) {
        self.discard_messages.fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn discarded_messages(&self) -> u64 {
        self.discard_messages.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn add_discarded_dump_messages(&self) {
        self.discard_dump_messages
            .fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn discard_dump_messages(&self) -> u64 {
        self.discard_dump_messages.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn add_received_bytes(&self, bytes: u64) {
        self.received_bytes
            .fetch_add(bytes, atomic::Ordering::SeqCst);
    }

    pub(crate) fn received_bytes(&self) -> u64 {
        self.received_bytes.load(atomic::Ordering::SeqCst)
    }

    pub(crate) fn reset_metrics(&self) {
        let metrics = self.metrics.ipc();
        metrics.set_extra_metric(&FETCHED_MESSAGES, 0);
        metrics.set_extra_metric(&DUMPED_MESSAGES, 0);
        metrics.set_extra_metric(&FETCHED_ACKS, 0);
        metrics.set_extra_metric(&ACK_FAILS, 0);
        metrics.set_extra_metric(&UNPROCESSED_MESSAGES, 0);
        metrics.set_extra_metric(&SENT_BATCHES, 0);
        metrics.set_extra_metric(&DISCARDED_MESSAGES, 0);
        metrics.set_extra_metric(&DISCARDED_DUMP_MESSAGES, 0);
        metrics.set_extra_metric(&RECEIVED_BYTES, 0);
    }

    pub(crate) fn update_metrics(&self) {
        let metrics = self.metrics.ipc();
        metrics.set_extra_metric(&FETCHED_MESSAGES, self.fetched_messages());
        metrics.set_extra_metric(&DUMPED_MESSAGES, self.dumped_messages());
        metrics.set_extra_metric(&FETCHED_ACKS, self.fetched_acks());
        metrics.set_extra_metric(&ACK_FAILS, self.ack_fails());
        metrics.set_extra_metric(&UNPROCESSED_MESSAGES, self.unprocessed_messages());
        metrics.set_extra_metric(&SENT_BATCHES, self.sent_batches());
        metrics.set_extra_metric(&DISCARDED_MESSAGES, self.discarded_messages());
        metrics.set_extra_metric(&DISCARDED_DUMP_MESSAGES, self.discard_dump_messages());
        metrics.set_extra_metric(&RECEIVED_BYTES, self.received_bytes());
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use scc::HashIndex;

    use taosx_core::core_metrics::init_task_metrics;

    use super::*;

    #[tokio::test]
    async fn metrics_test() -> anyhow::Result<()> {
        let metrics = init_task_metrics(&"mqtt://".parse()?, &"taos://".parse()?, 1, None)
            .await
            .context("metrics not found")?;
        let metrics = MqttMetrics::new(metrics);
        metrics.add_fetched_messages();
        metrics.add_dumped_messages();
        metrics.add_fetched_acks();
        metrics.add_ack_fails();
        metrics.add_unprocessed_messages();
        metrics.add_unprocessed_messages();
        metrics.sub_unprocessed_messages(1);
        metrics.add_sent_batches();
        metrics.add_discarded_messages();
        metrics.add_discarded_dump_messages();

        metrics.update_metrics();
        let extras = &metrics.metrics.ipc().extras;
        assert_eq!(get_value(extras, &FETCHED_MESSAGES).await, Some(1));
        assert_eq!(get_value(extras, &DUMPED_MESSAGES).await, Some(1));
        assert_eq!(get_value(extras, &FETCHED_ACKS).await, Some(1));
        assert_eq!(get_value(extras, &ACK_FAILS).await, Some(1));
        assert_eq!(get_value(extras, &UNPROCESSED_MESSAGES).await, Some(1));
        assert_eq!(get_value(extras, &SENT_BATCHES).await, Some(1));
        assert_eq!(get_value(extras, &DISCARDED_MESSAGES).await, Some(1));
        assert_eq!(get_value(extras, &DISCARDED_DUMP_MESSAGES).await, Some(1));

        metrics.reset_metrics();
        let extras = &metrics.metrics.ipc().extras;
        assert_eq!(get_value(extras, &FETCHED_MESSAGES).await, Some(0));
        assert_eq!(get_value(extras, &DUMPED_MESSAGES).await, Some(0));
        assert_eq!(get_value(extras, &FETCHED_ACKS).await, Some(0));
        assert_eq!(get_value(extras, &ACK_FAILS).await, Some(0));
        assert_eq!(get_value(extras, &UNPROCESSED_MESSAGES).await, Some(0));
        assert_eq!(get_value(extras, &SENT_BATCHES).await, Some(0));
        assert_eq!(get_value(extras, &DISCARDED_MESSAGES).await, Some(0));
        assert_eq!(get_value(extras, &DISCARDED_DUMP_MESSAGES).await, Some(0));

        Ok(())
    }

    async fn get_value(map: &HashIndex<FastStr, u64>, key: &FastStr) -> Option<u64> {
        map.get_async(key).await.map(|v| *v.get())
    }
}
