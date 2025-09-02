use std::{
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use rdkafka::{
    ClientContext, Offset, TopicPartitionList,
    consumer::{BaseConsumer, ConsumerContext, Rebalance},
    error::{KafkaError, KafkaResult},
    types::RDKafkaErrorCode,
};
use taosx_core::{core_metrics::CoreMetrics, sink::ipc_metric::IpcMetrics};

use crate::METRIC_CONSUMING_PARTITIONS;

/// due to this issue: https://github.com/fede1024/rust-rdkafka/issues/681
/// do not use `{:?}` for `TopicPartitionList` struct, or we will meet a panic
/// we use a temporary workaround for now
pub struct CustomContext {
    pub offsets_cache: scc::HashIndex<(String, i32), i64>,
    sem: tokio::sync::Semaphore,
    /// Metric 1: joins, times that a retryable consumer joins to the group.
    joins: AtomicUsize,
    rebalanced_count: AtomicUsize,
    commits: AtomicUsize,
    metrics: Arc<CoreMetrics>,
    pub seek_to: Option<Offset>,
}

impl CustomContext {
    pub fn fetch_add_joins(&self) -> usize {
        self.joins.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn current_joins(&self) -> usize {
        self.joins.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn new(metrics: Arc<CoreMetrics>) -> Self {
        Self {
            offsets_cache: scc::HashIndex::with_capacity(1),
            sem: tokio::sync::Semaphore::new(1),
            joins: AtomicUsize::new(0),
            rebalanced_count: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
            metrics,
            seek_to: None,
        }
    }

    pub fn metrics(&self) -> &IpcMetrics {
        self.metrics.ipc()
    }
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        if is_rebalance_empty(rebalance) {
            return;
        }

        self.sem.forget_permits(1);
        if !self.offsets_cache.is_empty() {
            tracing::info!("Pre rebalance {:?}, will clear offsets cache", rebalance);
            self.offsets_cache.clear();
        } else {
            tracing::info!("Pre rebalance {:?}", rebalance);
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        if is_rebalance_empty(rebalance) {
            return;
        }

        match rebalance {
            Rebalance::Assign(tpl) => {
                tracing::info!("Post Assign {}", tpl.count());
                self.metrics()
                    .add_extra_metric(&METRIC_CONSUMING_PARTITIONS, tpl.count() as _);
            }
            Rebalance::Revoke(tpl) => {
                tracing::info!("Post Revoke {}", tpl.count());
                self.metrics()
                    .sub_extra_metric(&METRIC_CONSUMING_PARTITIONS, tpl.count() as _);
            }
            Rebalance::Error(err) => {
                tracing::error!("Pre rebalance error: {:?}", err);
            }
        }
        let rebalances = self
            .rebalanced_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tracing::info!(rebalances, "Post rebalance {:?}", rebalance);
        self.sem.add_permits(1);
    }

    fn commit_callback(&self, result: KafkaResult<()>, tpl: &TopicPartitionList) {
        if is_tplist_empty(tpl) {
            return;
        }

        if let Err(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::RequestTimedOut | RDKafkaErrorCode::OperationTimedOut,
        )) = result
        {
            let commits = self
                .commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tracing::warn!(
                commits,
                "{:?} Commit timeout, commit error:{:#}",
                tpl,
                result.unwrap_err()
            );
        } else if let Err(err) = result {
            tracing::error!(
                commits = self.commits.load(std::sync::atomic::Ordering::SeqCst),
                "{:?} Commit error: {:#}",
                tpl,
                err
            );
        } else {
            let commits = self
                .commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tracing::debug!(commits, "{:?}, Committing offsets: {:?}", tpl, result);
        }
    }

    fn main_queue_min_poll_interval(&self) -> rdkafka::util::Timeout {
        rdkafka::util::Timeout::After(Duration::from_millis(200))
    }
}

fn is_rebalance_empty(r: &Rebalance) -> bool {
    match r {
        Rebalance::Assign(tpl) => is_tplist_empty(tpl),
        Rebalance::Revoke(tpl) => is_tplist_empty(tpl),
        Rebalance::Error(_) => false,
    }
}

fn is_tplist_empty(tpl: &TopicPartitionList) -> bool {
    tpl.capacity() == 0
}
