use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use arrow::array::{
    BinaryBuilder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow_schema::SchemaRef;
use futures_ext::TryReadyChunksError;
use futures_util::{StreamExt, stream::FuturesOrdered};
use parking_lot::RwLock;
use rdkafka::{
    consumer::{Consumer, ConsumerContext},
    error::KafkaError,
    topic_partition_list::Offset,
    types::RDKafkaErrorCode,
};
use taosx_core::{
    TaskNotify, TaskNotifySender, core_metrics::CoreMetrics, utils::codec::StringDecoder,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};

use crate::{
    ExitStatus, LoggingConsumer, PendingBatches, PendingState, WritePressureSnapshot,
    WritePressureState, message_sender::MessagesSender, pending_ack_fut::PendingAckResult,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KafkaErrorKind {
    Recoverable,
    SessionFatal,
    Unknown,
}

const MAX_CONSECUTIVE_RECOVERABLE_ERRORS: usize = 3;
const RECOVERABLE_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoverableAction {
    Retry(Duration),
    Escalate,
}

const INITIAL_NON_MESSAGE_WARNING_INTERVAL: u64 = 30;
const MAX_NON_MESSAGE_WARNING_INTERVAL: u64 = 480;
const MAX_BACKOFF: u64 = 16;
const TIMEOUT_MAX_BACKOFF: u64 = 50;

fn classify(err: &KafkaError) -> KafkaErrorKind {
    let code = match err {
        KafkaError::Global(code)
        | KafkaError::GroupListFetch(code)
        | KafkaError::MessageConsumption(code) => code,
        _ => return KafkaErrorKind::Unknown,
    };

    match code {
        RDKafkaErrorCode::BrokerTransportFailure
        | RDKafkaErrorCode::OperationTimedOut
        | RDKafkaErrorCode::AllBrokersDown
        // rdkafka 0.39 exposes the group coordinator errors under these names.
        | RDKafkaErrorCode::NotCoordinator
        | RDKafkaErrorCode::CoordinatorNotAvailable
        | RDKafkaErrorCode::RequestTimedOut
        | RDKafkaErrorCode::RebalanceInProgress => KafkaErrorKind::Recoverable,
        RDKafkaErrorCode::PollExceeded
        | RDKafkaErrorCode::UnknownMemberId
        | RDKafkaErrorCode::IllegalGeneration
        | RDKafkaErrorCode::FencedInstanceId => KafkaErrorKind::SessionFatal,
        _ => KafkaErrorKind::Unknown,
    }
}

async fn wait_recoverable_retry(aborted: &CancellationToken, duration: Duration) -> ExitStatus {
    tokio::select! {
        _ = aborted.cancelled() => ExitStatus::Aborted,
        _ = tokio::time::sleep(duration) => ExitStatus::Finished,
    }
}

fn next_recoverable_action(retries: &mut usize) -> RecoverableAction {
    if *retries >= MAX_CONSECUTIVE_RECOVERABLE_ERRORS {
        RecoverableAction::Escalate
    } else {
        *retries += 1;
        RecoverableAction::Retry(RECOVERABLE_RETRY_DELAY)
    }
}

fn is_write_blocked(pressure: WritePressureSnapshot, has_pending_ack: bool) -> bool {
    pressure.write_blocked || has_pending_ack
}

fn should_escalate_idle_timeout(
    idle_elapsed: Duration,
    warning_window: Duration,
    pressure: WritePressureSnapshot,
    has_pending_ack: bool,
) -> bool {
    idle_elapsed > warning_window && !is_write_blocked(pressure, has_pending_ack)
}

fn reset_idle_tracking(
    last_message: &mut Instant,
    global_last_message: &RwLock<Instant>,
    last_warning_interval: &mut u64,
) {
    let now = Instant::now();
    *last_message = now;
    *global_last_message.write() = now;
    *last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;
}

fn write_pressure_sleep(backoff: u64) -> Duration {
    Duration::from_millis((backoff * 200).clamp(500, 5_000))
}

fn seek_partition<C, Ctx>(
    consumer: &C,
    topic: &str,
    partition: i32,
    offset: Offset,
    timeout: Duration,
) -> anyhow::Result<()>
where
    C: Consumer<Ctx>,
    Ctx: ConsumerContext,
{
    consumer
        .seek(topic, partition, offset, timeout)
        .with_context(|| {
            format!("failed to seek topic `{topic}` partition {partition} to {offset:?}")
        })
}

#[instrument(skip_all)]
pub async fn poll_message(
    index: usize,
    consumer: &mut LoggingConsumer,
    tx: &flume::Sender<RecordBatch>,
    timeout: i64,
    aborted: &CancellationToken,
    schema: &SchemaRef,
    batch_size: usize,
    batch_timeout_ms: i64,
    notify: &TaskNotifySender,
    codec_processor: Option<StringDecoder>,
    pending_batches: &PendingBatches,
    permits: &Arc<tokio::sync::Semaphore>,
    metrics: &Arc<CoreMetrics>,
    global_last_message: &Arc<RwLock<Instant>>,
) -> anyhow::Result<ExitStatus> {
    const MAX_READY_CHUNK_SIZE: usize = 100;

    let mut ready_chunks =
        futures_ext::TryReadyChunks::new(consumer.stream(), batch_size.max(MAX_READY_CHUNK_SIZE));

    let timeout_duration = if timeout > 0 {
        Duration::from_millis(timeout as u64)
    } else {
        Duration::MAX
    };
    let batch_timeout = Duration::from_millis(batch_timeout_ms as u64);

    let timestamp = TimestampNanosecondBuilder::new();
    let topic = StringBuilder::new();
    let partition: arrow::array::PrimitiveBuilder<arrow::datatypes::Int32Type> =
        Int32Builder::new();
    let offset = Int64Builder::new();
    let key = BinaryBuilder::new();
    let value = BinaryBuilder::new();
    let write_pressure = Arc::new(RwLock::new(WritePressureState::new(Duration::from_secs(5))));

    let mut sender = MessagesSender {
        consumer,
        tx,
        bid: 0,
        batch_size,
        batch_timeout,
        polling_timeout_ms: timeout,
        last_polling: Instant::now(),
        new_polling: None,
        codec_processor,
        codec_err_count: 0,
        timestamp,
        topic,
        partition,
        offset,
        key,
        value,
        schema,
        pending_batches,
        permits,
        max_acquire_elapsed: Duration::ZERO,
        write_pressure: &write_pressure,
        metrics,
    };

    // static RANDOM_ERROR_ATOMIC: AtomicUsize = AtomicUsize::new(1);

    let mut backoff = 1;
    let mut last_message = Instant::now();
    let mut consecutive_recoverable_errors = 0usize;

    let mut last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;

    let mut seek_to = consumer.context().seek_to;

    let mut pending_futs = FuturesOrdered::new();
    let mut max_ack_elapsed = Duration::ZERO;

    loop {
        tracing::trace!("Kafka consumer-{} polling by ready trunks", index);
        tokio::select! {
            biased;
            _ = aborted.cancelled() => {
                tracing::info!("Kafka consumer-{} cancelled", index);
                return Ok(ExitStatus::Aborted);
            }
            res = pending_futs.next(), if !pending_futs.is_empty() => {
                let Some(res) = res else {
                    return Ok(ExitStatus::Finished)
                };
                consecutive_recoverable_errors = 0;
                match res {
                    PendingAckResult::State((elapsed, offsets)) => {
                        write_pressure.write().record_ack_wait(elapsed);
                        if elapsed >= max_ack_elapsed {
                            max_ack_elapsed = elapsed;
                            tracing::info!("ack elapsed: {elapsed:?}");
                        }
                        let mut assignment = None;
                        let mut no_offsets = true;
                        for PendingState { topic, partition, offset } in offsets {
                            let Err(err) = sender.consumer.store_offset(&topic, partition, offset) else {
                                no_offsets = false;
                                continue;
                            };
                            tracing::warn!(
                                cause = %err,
                                topic,
                                partition,
                                offset,
                                "Store offset error in partition {}",
                                partition,
                            );
                            if sender.consumer.assignment_lost() {
                                tracing::warn!("Consumer assignment lost, skip");
                                continue;
                            }
                            if assignment.is_none() {
                                assignment = sender
                                    .consumer
                                    .assignment()
                                    .inspect_err(
                                        |err| tracing::error!(cause  = %err, "Get consumer assignment error"),
                                    )
                                    .ok();
                            }
                            if let Some(assignment) = assignment.as_ref() {
                                if assignment
                                    .elements_for_topic(&topic)
                                    .iter()
                                    .all(|item| item.partition() != partition)
                                {
                                    tracing::warn!(
                                        "Rebalanced, partition {partition} is no longer assigned to this consumer"
                                    );
                                } else {
                                    Err(err).with_context(|| {
                                        format!(
                                            "Store offset error in partition {partition} of topic `{topic}`"
                                        )
                                    })?;
                                }
                            } else {
                                tracing::warn!(
                                    "Store offset error in partition {partition} of topic `{topic}`, seems assignment lost"
                                );
                                continue;
                            }
                        }

                        if no_offsets {
                            tracing::warn!(batch.size = batch_size, "No offsets stored, skip commit");
                        }
                    },
                    PendingAckResult::Closed => {
                        tracing::error!("pending ack dropped");
                    },
                    PendingAckResult::TimedOut { batch_id, elapsed } => {
                        if let Some((_, (_, permit))) = pending_batches.remove_async(&batch_id).await {
                            drop(permit);
                        }
                        let message = format!(
                            "pending ack timed out after {elapsed:?} for batch {batch_id}"
                        );
                        let _ = notify.send_async(TaskNotify::warn(message.clone())).await;
                        anyhow::bail!("{message}");
                    }
                }
            }
            _ = tokio::time::sleep(timeout_duration) => {
                consecutive_recoverable_errors = 0;
                tracing::info!("Kafka consumer-{} polling timeout", index);
                let pressure = write_pressure.read().snapshot();
                if global_last_message.read().elapsed() >= timeout_duration
                    && !is_write_blocked(pressure, !pending_futs.is_empty())
                {
                    match sender.send(&mut pending_futs, aborted).in_current_span().await? {
                        ExitStatus::Aborted => return Ok(ExitStatus::Aborted),
                        ExitStatus::None | ExitStatus::Finished | ExitStatus::Timeout => {}
                    }
                    return Ok(ExitStatus::Timeout)
                }
            }
            chunk = ready_chunks.next() => {
                match chunk {
                    Some(Ok(chunk)) => {
                        consecutive_recoverable_errors = 0;
                        if let Some(to) = seek_to {
                            let assigns = consumer
                                .assignment()
                                .context("failed to get assignment")?;
                            for a in assigns.elements() {
                                tracing::info!(
                                    "seeking topic: {}, partition: {} to offset: {:?}",
                                    a.topic(),
                                    a.partition(),
                                    to
                                );
                                seek_partition(
                                    consumer,
                                    a.topic(),
                                    a.partition(),
                                    to,
                                    Duration::from_secs(1),
                                )?;
                            }
                            seek_to = None;
                            continue;
                        }

                        backoff = 1;
                        last_message = Instant::now();
                        {
                            *global_last_message.write() = Instant::now();
                        }
                        last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;
                        if chunk.is_empty() {
                            tracing::trace!("Empty chunk, go next polling");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        match sender.send_chunk(&chunk, &mut pending_futs, aborted).in_current_span().await? {
                            ExitStatus::None => {
                                continue;
                            }
                            ExitStatus::Timeout => {
                                tracing::warn!("Ready chunks should never exit by polling timeout");
                                return Ok(ExitStatus::Timeout);
                            }
                            ExitStatus::Aborted => {
                                tracing::warn!("Kafka consumer should not be aborted with ready chunks");
                                return Ok(ExitStatus::Aborted);
                            }
                            ExitStatus::Finished => {
                                continue;
                            }
                        }
                    }
                    Some(Err(TryReadyChunksError(_, e))) => {
                        if aborted.is_cancelled() {
                            tracing::debug!(error = %e, "Consumer aborted even error occur");
                            return Ok(ExitStatus::Aborted);
                        }
                        match classify(&e) {
                            KafkaErrorKind::Recoverable => {
                                match next_recoverable_action(&mut consecutive_recoverable_errors) {
                                    RecoverableAction::Retry(duration) => {
                                        tracing::warn!(
                                            retry = consecutive_recoverable_errors,
                                            "Recoverable Kafka error, retry in-place: {e:#}"
                                        );
                                        let _ = notify
                                            .send_async(TaskNotify::warn(format!(
                                                "recoverable kafka polling error: {e:#}"
                                            )))
                                            .await;
                                        if matches!(
                                            wait_recoverable_retry(aborted, duration).await,
                                            ExitStatus::Aborted
                                        ) {
                                            return Ok(ExitStatus::Aborted);
                                        }
                                        continue;
                                    }
                                    RecoverableAction::Escalate => {
                                        let message = format!(
                                            "recoverable kafka polling error exceeded retry limit: {e:#}"
                                        );
                                        let _ = notify
                                            .send_async(TaskNotify::warn(message.clone()))
                                            .await;
                                        anyhow::bail!("{message}");
                                    }
                                }
                            }
                            KafkaErrorKind::SessionFatal => {
                                if e == KafkaError::MessageConsumption(RDKafkaErrorCode::PollExceeded) {
                                    tracing::warn!("Maximum application poll interval exceeded");
                                }
                                let _ = notify
                                    .send_async(TaskNotify::warn(format!(
                                        "session-fatal kafka polling error: {e:#}"
                                    )))
                                    .await;
                                tracing::error!("session-fatal kafka polling error: {e:#}");
                                anyhow::bail!("session-fatal kafka polling error: {e:#}");
                            }
                            KafkaErrorKind::Unknown => {
                                let _ = notify
                                    .send_async(TaskNotify::warn(format!(
                                        "unknown kafka polling error: {e:#}"
                                    )))
                                    .await;
                                tracing::error!("unknown kafka polling error: {e:#}");
                                anyhow::bail!("unknown kafka polling error: {e:#}");
                            }
                        }
                    }
                    None => {
                        consecutive_recoverable_errors = 0;
                        tracing::trace!("Kafka polling return None, continue");
                        match sender.send(&mut pending_futs, aborted).in_current_span().await? {
                            ExitStatus::None => {
                                let pressure = write_pressure.read().snapshot();
                                if is_write_blocked(pressure, !pending_futs.is_empty()) {
                                    reset_idle_tracking(
                                        &mut last_message,
                                        global_last_message,
                                        &mut last_warning_interval,
                                    );
                                    if backoff < TIMEOUT_MAX_BACKOFF {
                                        backoff = (backoff * 2).min(TIMEOUT_MAX_BACKOFF);
                                    }
                                    tokio::time::sleep(write_pressure_sleep(backoff)).await;
                                    continue;
                                }
                                if backoff < MAX_BACKOFF {
                                    backoff *= 2;
                                }
                                let duration = Duration::from_secs(last_warning_interval);
                                let elapsed = last_message.elapsed();
                                if should_escalate_idle_timeout(
                                    elapsed,
                                    duration,
                                    pressure,
                                    !pending_futs.is_empty(),
                                ) {
                                    tracing::warn!(
                                        "consumer {index} has no messages received in {elapsed:?}, exceeding polling timeout"
                                    );
                                    let _ = notify.send_async(crate::TaskNotify::warn(format!(
                                        "consumer {index} has no messages received in {elapsed:?}, exceeding polling timeout"
                                    ))).await;
                                    if last_warning_interval < MAX_NON_MESSAGE_WARNING_INTERVAL {
                                        last_warning_interval *= 2;
                                        last_message = Instant::now();
                                        *global_last_message.write() = Instant::now();
                                    } else {
                                        tracing::error!("Consumer {index} has no messages received in {elapsed:?}");
                                        anyhow::bail!("Consumer {index} has no messages received in {elapsed:?}");
                                    }
                                }
                                tokio::time::sleep(Duration::from_millis(backoff * 100)).await;
                            }
                            ExitStatus::Timeout => {
                                let pressure = write_pressure.read().snapshot();
                                if is_write_blocked(pressure, !pending_futs.is_empty()) {
                                    reset_idle_tracking(
                                        &mut last_message,
                                        global_last_message,
                                        &mut last_warning_interval,
                                    );
                                    if backoff < TIMEOUT_MAX_BACKOFF {
                                        backoff = (backoff * 2).min(TIMEOUT_MAX_BACKOFF);
                                    }
                                    tokio::time::sleep(write_pressure_sleep(backoff)).await;
                                    continue;
                                }
                                tracing::info!("None messages received, consumer polling timeout");
                                if global_last_message.read().elapsed() >= timeout_duration {
                                    return Ok(ExitStatus::Timeout)
                                }
                                if backoff < TIMEOUT_MAX_BACKOFF {
                                    backoff *= 2;
                                } else {
                                    backoff = TIMEOUT_MAX_BACKOFF;
                                }
                                tokio::time::sleep(Duration::from_millis(backoff * 100)).await;
                            }
                            ExitStatus::Aborted => {
                                tracing::info!("None messages received, exiting with consumer aborted");
                                return Ok(ExitStatus::Aborted);
                            }
                            ExitStatus::Finished => {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rdkafka::{ClientConfig, consumer::BaseConsumer, topic_partition_list::Offset};

    use super::*;

    #[test]
    fn classify_broker_transport_failure_as_recoverable() {
        let kind = classify(&KafkaError::MessageConsumption(
            RDKafkaErrorCode::BrokerTransportFailure,
        ));
        assert!(matches!(kind, KafkaErrorKind::Recoverable));
    }

    #[test]
    fn classify_poll_exceeded_as_session_fatal() {
        let kind = classify(&KafkaError::MessageConsumption(
            RDKafkaErrorCode::PollExceeded,
        ));
        assert!(matches!(kind, KafkaErrorKind::SessionFatal));
    }

    #[test]
    fn classify_unknown_code_as_unknown() {
        let kind = classify(&KafkaError::MessageConsumption(
            RDKafkaErrorCode::UnknownTopicOrPartition,
        ));
        assert!(matches!(kind, KafkaErrorKind::Unknown));
    }

    #[test]
    fn classify_group_coordinator_errors_as_recoverable() {
        for code in [
            RDKafkaErrorCode::NotCoordinator,
            RDKafkaErrorCode::CoordinatorNotAvailable,
        ] {
            let kind = classify(&KafkaError::MessageConsumption(code));
            assert!(matches!(kind, KafkaErrorKind::Recoverable));
        }
    }

    #[test]
    fn classify_global_transport_failure_as_recoverable() {
        let kind = classify(&KafkaError::Global(
            RDKafkaErrorCode::BrokerTransportFailure,
        ));
        assert!(matches!(kind, KafkaErrorKind::Recoverable));
    }

    #[test]
    fn classify_group_list_poll_exceeded_as_session_fatal() {
        let kind = classify(&KafkaError::GroupListFetch(RDKafkaErrorCode::PollExceeded));
        assert!(matches!(kind, KafkaErrorKind::SessionFatal));
    }

    #[tokio::test]
    async fn wait_recoverable_retry_returns_aborted_when_cancelled() {
        let aborted = CancellationToken::new();
        aborted.cancel();

        let status = wait_recoverable_retry(&aborted, Duration::from_secs(5)).await;

        assert!(matches!(status, ExitStatus::Aborted));
    }

    #[test]
    fn idle_escalation_requires_no_write_pressure() {
        let pressure = crate::WritePressureSnapshot {
            write_blocked: true,
            last_permit_wait: Duration::from_secs(6),
            last_ack_wait: Duration::ZERO,
        };

        assert!(!should_escalate_idle_timeout(
            Duration::from_secs(600),
            Duration::from_secs(480),
            pressure,
            false,
        ));
    }

    #[test]
    fn reset_idle_tracking_refreshes_local_and_global_timers() {
        let mut last_message = Instant::now() - Duration::from_secs(600);
        let global_last_message = Arc::new(RwLock::new(Instant::now() - Duration::from_secs(600)));
        let mut last_warning_interval = MAX_NON_MESSAGE_WARNING_INTERVAL;

        reset_idle_tracking(
            &mut last_message,
            &global_last_message,
            &mut last_warning_interval,
        );

        assert!(last_message.elapsed() < Duration::from_secs(1));
        assert!(global_last_message.read().elapsed() < Duration::from_secs(1));
        assert_eq!(last_message, *global_last_message.read());
        assert_eq!(last_warning_interval, INITIAL_NON_MESSAGE_WARNING_INTERVAL);
    }

    #[test]
    fn recoverable_errors_escalate_after_limit() {
        let mut retries = 0;

        for _ in 0..MAX_CONSECUTIVE_RECOVERABLE_ERRORS {
            assert!(matches!(
                next_recoverable_action(&mut retries),
                RecoverableAction::Retry(_)
            ));
        }

        assert!(matches!(
            next_recoverable_action(&mut retries),
            RecoverableAction::Escalate
        ));
    }

    #[test]
    fn seek_partition_returns_error_instead_of_panicking() {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", "127.0.0.1:1");
        let consumer: BaseConsumer = client_config
            .create()
            .expect("Consumer should be created successfully in test");

        let result = seek_partition(
            &consumer,
            "missing-topic",
            0,
            Offset::Beginning,
            Duration::from_millis(10),
        );

        assert!(
            result.is_err(),
            "seek helper should report the original seek failure instead of panicking"
        );
    }
}
