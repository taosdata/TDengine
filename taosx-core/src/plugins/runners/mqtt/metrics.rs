use std::sync::{
    atomic::{self, AtomicU64},
    Arc,
};

use crate::core_metrics::CoreMetrics;

use super::{
    ACK_FAILS, DISCARDED_DUMP_MESSAGES, DISCARDED_MESSAGES, DUMPED_MESSAGES, FETCHED_ACKS,
    FETCHED_MESSAGES, PROCESSING_BATCHES, UNPROCESSED_MESSAGES,
};

#[derive(Debug, Default)]
pub(crate) struct MqttMetrics {
    fetched_messages: AtomicU64,
    dumped_messages: AtomicU64,
    fetched_acks: AtomicU64,
    ack_fails: AtomicU64,
    unprocessed_messages: AtomicU64,
    processing_batches: AtomicU64,
    discard_messages: AtomicU64,
    discard_dump_messages: AtomicU64,
}

impl MqttMetrics {
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

    pub(crate) fn add_processing_batches(&self) {
        self.processing_batches
            .fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn sub_processing_batches(&self) {
        self.processing_batches
            .fetch_sub(1, atomic::Ordering::SeqCst);
    }

    pub(crate) fn processing_batches(&self) -> u64 {
        self.processing_batches.load(atomic::Ordering::SeqCst)
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

    pub(crate) fn update_metrics(&self, metrics: Arc<CoreMetrics>) {
        let metrics = metrics.ipc();
        metrics.set_extra_metric(&FETCHED_MESSAGES, self.fetched_messages());
        metrics.set_extra_metric(&DUMPED_MESSAGES, self.dumped_messages());
        metrics.set_extra_metric(&FETCHED_ACKS, self.fetched_acks());
        metrics.set_extra_metric(&ACK_FAILS, self.ack_fails());
        metrics.set_extra_metric(&UNPROCESSED_MESSAGES, self.unprocessed_messages());
        metrics.set_extra_metric(&PROCESSING_BATCHES, self.processing_batches());
        metrics.set_extra_metric(&DISCARDED_MESSAGES, self.discarded_messages());
        metrics.set_extra_metric(&DISCARDED_DUMP_MESSAGES, self.discard_dump_messages());
    }
}
