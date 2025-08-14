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
use rdkafka::{consumer::Consumer, error::KafkaError, types::RDKafkaErrorCode};
use taosx_core::{
    TaskNotify, TaskNotifySender, core_metrics::CoreMetrics, utils::codec::StringDecoder,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    ExitStatus, LoggingConsumer, PendingBatches, PendingState, message_sender::MessagesSender,
    pending_ack_fut::PendingAckResult,
};

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
    pending_batches: PendingBatches,
    permits: Arc<tokio::sync::Semaphore>,
    metrics: Arc<CoreMetrics>,
) -> anyhow::Result<ExitStatus> {
    const MAX_READY_CHUNK_SIZE: usize = 100;

    let mut ready_chunks =
        futures_ext::TryReadyChunks::new(consumer.stream(), batch_size.max(MAX_READY_CHUNK_SIZE));

    let timeout_duration = if timeout > 0 {
        Duration::from_millis(timeout as u64)
    } else {
        Duration::MAX
    };
    // let batch_timeout = Duration::from_millis(batch_timeout_ms as u64);

    let timestamp = TimestampNanosecondBuilder::new();
    let topic = StringBuilder::new();
    let partition: arrow::array::PrimitiveBuilder<arrow::datatypes::Int32Type> =
        Int32Builder::new();
    let offset = Int64Builder::new();
    let key = BinaryBuilder::new();
    let value = BinaryBuilder::new();

    let mut sender = MessagesSender {
        consumer,
        tx,
        bid: 0,
        batch_size,
        batch_timeout_ms,
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
        permits: permits.clone(),
        max_acquire_elapsed: Duration::ZERO,
        metrics: metrics.clone(),
    };

    // static RANDOM_ERROR_ATOMIC: AtomicUsize = AtomicUsize::new(1);

    let mut backoff = 1;
    let mut last_message = Instant::now();
    const INITIAL_NON_MESSAGE_WARNING_INTERVAL: u64 = 30;
    const MAX_NON_MESSAGE_WARNING_INTERVAL: u64 = 480;
    const MAX_BACKOFF: u64 = 16;
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
                match res {
                    PendingAckResult::State((elapsed, offsets)) => {
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
                }
            }
            _ = tokio::time::sleep(timeout_duration) => {
                tracing::info!("Kafka consumer-{} polling timeout", index);
                return Ok(ExitStatus::Timeout);
            }
            chunk = ready_chunks.next() => {
                match chunk {
                    Some(Ok(chunk)) => {
                        if let Some(to) = seek_to {
                            let assigns = consumer.assignment().expect("failed to get assignment");
                            for a in assigns.elements() {
                                tracing::info!(
                                    "seeking topic: {}, partition: {} to offset: {:?}",
                                    a.topic(),
                                    a.partition(),
                                    to
                                );
                                consumer
                                    .seek(a.topic(), a.partition(), to, Duration::from_secs(1))
                                    .expect("failed to seek");
                            }
                            seek_to = None;
                            continue;
                        }

                        backoff = 1;
                        last_message = Instant::now();
                        last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;
                        match sender.send_chunk(&chunk, &mut pending_futs, aborted).in_current_span().await? {
                            ExitStatus::None => {
                                tokio::time::sleep(Duration::from_millis(100)).await;
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
                        if e == KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut) {
                            tracing::warn!("Kafka polling timeout, continue");
                            tokio::time::sleep(Duration::from_millis(5000)).await;
                            continue;
                        }
                        if e == KafkaError::MessageConsumption(RDKafkaErrorCode::PollExceeded) {
                            tracing::warn!("Maximum application poll interval (max.poll.interval.ms) exceeded");
                        }

                        // Ready chunks may still contains some messages, but we just skip them.
                        // It will be handled by next consuming.
                        let _ = notify.send(TaskNotify::warn(format!("failed to polling from kafka, cause: {:#}", e)));
                        tracing::error!("failed to polling from kafka, cause: {:#}", e);
                        anyhow::bail!("failed to polling from kafka, cause: {:#}", e);
                    }
                    None => {
                        tracing::trace!("Kafka polling return None, continue");
                        match sender.send(&mut pending_futs, aborted).in_current_span().await? {
                            ExitStatus::None => {
                                if backoff < MAX_BACKOFF {
                                    backoff *= 2;
                                }
                                let duration = Duration::from_secs(last_warning_interval);
                                let elapsed = last_message.elapsed();
                                if elapsed > duration {
                                    tracing::warn!("Consumer {index} has no messages received in {:?} consumer polling timeout", elapsed);
                                    let _ = notify.send(crate::TaskNotify::warn(format!("Consumer {index} has no messages received in {:?} consumer polling timeout", duration)));
                                    if last_warning_interval < MAX_NON_MESSAGE_WARNING_INTERVAL {
                                        last_warning_interval *= 2;
                                        last_message = Instant::now();
                                    } else {
                                        anyhow::bail!("Consumer {index} has no messages received in {:?}", elapsed);
                                    }
                                }
                                tokio::time::sleep(Duration::from_millis(backoff * 100)).await;
                            }
                            ExitStatus::Timeout => {
                                tracing::info!("None messages received, exit with consumer polling timeout");
                                return Ok(ExitStatus::Timeout);
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
