use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::array::{
    BinaryBuilder, Int32Builder, RecordBatch, StringBuilder, TimestampNanosecondBuilder,
    UInt64Builder,
};
use arrow_schema::SchemaRef;
use futures_util::{StreamExt, stream::FuturesOrdered};
use parking_lot::RwLock;
use pulsar::proto::MessageIdData;
use taosx_core::{
    TaskNotify, TaskNotifySender, core_metrics::CoreMetrics, utils::codec::StringDecoder,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    ExitStatus, METRIC_MSG_ACKS, PendingBatches, PendingState, consumer::CustomConsumer,
    message_sender::MessagesSender, pending_ack_fut::PendingAckResult,
};

pub async fn poll_message(
    index: usize,
    consumer: &mut CustomConsumer,
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

    let timeout_duration = if timeout > 0 {
        Duration::from_millis(timeout as u64)
    } else {
        Duration::MAX
    };

    let timestamp = TimestampNanosecondBuilder::new();
    let topic = StringBuilder::new();
    let partition: arrow::array::PrimitiveBuilder<arrow::datatypes::Int32Type> =
        Int32Builder::new();
    let ledger_id = UInt64Builder::new();
    let entry_id = UInt64Builder::new();
    let key = BinaryBuilder::new();
    let value = BinaryBuilder::new();

    let mut sender = MessagesSender {
        context: consumer.context(),
        tx,
        batch_size,
        batch_timeout: Duration::from_millis(batch_timeout_ms as u64),
        polling_timeout_ms: timeout,
        last_polling: Instant::now(),
        new_polling: None,
        codec_processor,
        codec_err_count: 0,
        timestamp,
        topic,
        partition,
        ledger_id,
        entry_id,
        key,
        value,
        schema,
        pending_batches,
        permits,
        max_acquire_elapsed: Duration::ZERO,
        metrics,
    };

    let pulsar = consumer.pulsar.clone();

    let mut backoff = 1;
    let mut last_message = Instant::now();

    const INITIAL_NON_MESSAGE_WARNING_INTERVAL: u64 = 30;
    const MAX_NON_MESSAGE_WARNING_INTERVAL: u64 = 480;
    const MAX_BACKOFF: u64 = 16;
    const TIMEOUT_MAX_BACKOFF: u64 = 50;
    let mut last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;
    if let Some(to) = consumer.context().seek_to {
        let topics = consumer.topics();
        // 首次判断，如果 consumer 设置了 seek_to, 就先 seek 到指定 offset
        let message_id = MessageIdData {
            ledger_id: to.ledger_id,
            entry_id: to.entry_id,
            ..Default::default()
        };
        consumer
            .seek(Some(topics), Some(message_id), None, pulsar.clone())
            .await?;
    }
    let mut pending_futs = FuturesOrdered::new();
    let mut max_ack_elapsed = Duration::ZERO;

    let mut chunk = vec![];
    let batch_size = batch_size.max(MAX_READY_CHUNK_SIZE);

    loop {
        tracing::trace!("Pulsar consumer-{} is going to poll trunks", index);
        tokio::select! {
            biased;
            _ = aborted.cancelled() => {
                tracing::info!("Pulsar consumer-{} cancelled", index);
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
                        let mut ack_batch_size = 0;
                        // 确认机制
                        for PendingState { topic, partition, ledger_id, entry_id , batch_size} in offsets {
                            ack_batch_size = batch_size;
                            let message_id = MessageIdData {
                                ledger_id,
                                entry_id,
                                ..Default::default()
                            };
                            if let Err(err) = consumer.cumulative_ack_with_id(&topic, message_id).await {
                                tracing::warn!(
                                    topic,
                                    partition,
                                    ledger_id,
                                    entry_id,
                                    error = %err,
                                    "pulsar cumulative ack offset error",
                                );
                                continue;
                            };
                            tracing::debug!(
                                topic,
                                partition,
                                ledger_id,
                                entry_id,
                                ack_batch_size,
                                "Pulsar ack offsets success",
                            );
                        }
                        metrics.ipc().add_extra_metric(&METRIC_MSG_ACKS, ack_batch_size as u64);
                    },
                    PendingAckResult::Closed => {
                        tracing::error!("pending ack dropped");
                    },
                }
            }
            _ = tokio::time::sleep(timeout_duration) => {
                tracing::info!("Pulsar consumer-{} polling timeout", index);
                if global_last_message.read().elapsed() >= timeout_duration {
                    return Ok(ExitStatus::Timeout)
                }
            }
            msg = tokio::time::timeout(Duration::from_millis(500), consumer.next()) => {
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(_) => {
                        tracing::trace!("Timeout reached, chunk.is_empty: {}, chunk.len: {}", chunk.is_empty(), chunk.len());
                        if chunk.is_empty() {
                            match sender.send(&mut pending_futs, aborted).in_current_span().await? {
                                ExitStatus::Timeout => {
                                    tracing::info!("Pulsar consumer-{} polling timeout", index);
                                    if global_last_message.read().elapsed() >= timeout_duration {
                                        return Ok(ExitStatus::Timeout)
                                    }
                                },
                                ExitStatus::Aborted => {
                                    tracing::info!("Pulsar consumer-{} aborted", index);
                                    return Ok(ExitStatus::Aborted);
                                },
                                ExitStatus::None | ExitStatus::Finished => {},
                            }
                            continue;
                        }

                        backoff = 1;
                        last_message = Instant::now();
                        {
                            *global_last_message.write() = Instant::now();
                        }
                        last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;

                        match sender.send_chunk(&chunk, &mut pending_futs, aborted).in_current_span().await? {
                            ExitStatus::None => {}
                            ExitStatus::Timeout => {
                                tracing::warn!("Ready chunks should never exit by polling timeout");
                                return Ok(ExitStatus::Timeout);
                            }
                            ExitStatus::Aborted => {
                                tracing::warn!("Pulsar consumer should not be aborted with ready chunks");
                                return Ok(ExitStatus::Aborted);
                            }
                            ExitStatus::Finished => {}
                        }
                        chunk.clear();
                        continue;
                    }
                };
                match msg {
                    Some(Ok(msg)) => {
                        chunk.push(msg);
                        if chunk.len() < batch_size {
                            continue;
                        }

                        backoff = 1;
                        last_message = Instant::now();
                        {
                            *global_last_message.write() = Instant::now();
                        }
                        last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;

                        match sender.send_chunk(&chunk, &mut pending_futs, aborted).in_current_span().await? {
                            ExitStatus::None => {}
                            ExitStatus::Timeout => {
                                tracing::warn!("Ready chunks should never exit by polling timeout");
                                return Ok(ExitStatus::Timeout);
                            }
                            ExitStatus::Aborted => {
                                tracing::warn!("Pulsar consumer should not be aborted with ready chunks");
                                return Ok(ExitStatus::Aborted);
                            }
                            ExitStatus::Finished => {}
                        }
                        chunk.clear();
                    }
                    Some(Err(e)) => {
                        if aborted.is_cancelled() {
                            tracing::debug!(error = %e, "Consumer aborted even error occur");
                            return Ok(ExitStatus::Aborted);
                        }

                        // It will be handled by next consuming.
                        let _ = notify.send_async(TaskNotify::warn(format!("failed to polling from Pulsar, cause: {:#}", e))).await;
                        anyhow::bail!("failed to polling from Pulsar, cause: {:#}", e);
                    }
                    None => {
                        tracing::trace!("Pulsar polling return None, continue");
                        match sender.send(&mut pending_futs, aborted).in_current_span().await? {
                            ExitStatus::None => {
                                if backoff < MAX_BACKOFF {
                                    backoff *= 2;
                                }
                                let duration = Duration::from_secs(last_warning_interval);
                                let elapsed = last_message.elapsed();
                                if elapsed > duration {
                                    tracing::warn!("Consumer {index} has no messages received in {:?} consumer polling timeout", elapsed);
                                    let _ = notify.send_async(crate::TaskNotify::warn(format!("Consumer {index} has no messages received in {:?} consumer polling timeout", duration))).await;
                                    if last_warning_interval < MAX_NON_MESSAGE_WARNING_INTERVAL {
                                        last_warning_interval *= 2;
                                        last_message = Instant::now();
                                        *global_last_message.write() = Instant::now();
                                    } else {
                                        anyhow::bail!("Consumer {index} has no messages received in {elapsed:?}");
                                    }
                                }
                                tokio::time::sleep(Duration::from_millis(backoff * 100)).await;
                            }
                            ExitStatus::Timeout => {
                                tracing::info!("None messages received, consumer {index} polling timeout");
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
                                tracing::info!("None messages received, exiting with consumer {index} aborted");
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
