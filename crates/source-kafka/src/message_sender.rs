use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use anyhow::Context;
use arrow::array::{
    ArrayBuilder, BinaryBuilder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow_schema::SchemaRef;
use chrono::Utc;
use futures_ext::InspectTimeoutFuture;
use futures_util::stream::FuturesOrdered;
use itertools::Itertools;
use rdkafka::{Message, consumer::Consumer, message::BorrowedMessage};
use scc::ebr::Guard;
use taosx_core::{
    core_metrics::CoreMetrics,
    utils::codec::{Processor, StringDecoder},
};
use tokio::sync::{OwnedSemaphorePermit, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    BATCH_ID, ExitStatus, LoggingConsumer, METRIC_CONSUMED_MESSAGES, METRIC_SENT_BATCHES,
    METRIC_TOTAL_CONSUMED_MESSAGES, PENDING_ACK_TIMEOUT, PendingBatches, PendingState,
    pending_ack_fut::PendingAckFuture,
};

pub struct MessagesSender<'a> {
    pub consumer: &'a LoggingConsumer,
    pub tx: &'a flume::Sender<RecordBatch>,
    /// Batch id/sequence number
    pub bid: u64,
    // Send options
    pub batch_size: usize,
    pub batch_timeout_ms: i64,
    pub polling_timeout_ms: i64,
    pub last_polling: Instant,
    pub new_polling: Option<Instant>,

    // codec
    pub codec_processor: Option<StringDecoder>,
    pub codec_err_count: usize,

    // Builders
    pub timestamp: TimestampNanosecondBuilder,
    pub topic: StringBuilder,
    pub partition: Int32Builder,
    pub offset: Int64Builder,
    pub key: BinaryBuilder,
    pub value: BinaryBuilder,
    pub schema: &'a SchemaRef,

    pub pending_batches: PendingBatches,
    pub permits: Arc<tokio::sync::Semaphore>,
    pub max_acquire_elapsed: Duration,

    pub metrics: Arc<CoreMetrics>,
}

impl<'a> MessagesSender<'a> {
    pub async fn send_chunk(
        &mut self,
        chunk: &[BorrowedMessage<'a>],
        pending_futs: &mut FuturesOrdered<InspectTimeoutFuture<PendingAckFuture>>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<ExitStatus> {
        if chunk.is_empty() {
            tracing::trace!("Empty chunk, go next polling");
            return Ok(ExitStatus::None);
        }

        let now = std::time::Instant::now();
        let context = self.consumer.context();
        context
            .metrics()
            .add_extra_metric(&METRIC_CONSUMED_MESSAGES, chunk.len() as _);
        context
            .metrics()
            .add_extra_metric(&METRIC_TOTAL_CONSUMED_MESSAGES, chunk.len() as _);
        let chunks = chunk.iter().chunk_by(|msg| (msg.topic(), msg.partition()));

        for ((topic, partition), iter) in &chunks {
            let mut offset = None;
            for msg in iter {
                offset.replace(msg.offset());
                if let Some(s) = msg.payload() {
                    let value = match self.codec_processor.process(s.to_vec()) {
                        Ok(payload) => {
                            self.codec_err_count = 0;
                            payload
                        }
                        Err(e) => {
                            tracing::error!("codec process message error: {e:#}");
                            self.codec_err_count += 1;
                            if self.codec_err_count < 3 {
                                continue;
                            }

                            return Err(e);
                        }
                    };
                    self.timestamp.append_value(
                        Utc::now()
                            .timestamp_nanos_opt()
                            .expect("Get now timestamp in nanosecond should always success"),
                    );
                    self.topic.append_value(msg.topic());
                    self.partition.append_value(msg.partition());
                    self.offset.append_value(msg.offset());
                    self.key.append_value(msg.key().unwrap_or(&[]));
                    self.value.append_value(value);
                }
            }
            let offset = offset.expect("offset should always exist");

            if let Some(entry) = context.offsets_cache.get(&(topic.to_string(), partition)) {
                entry.update(offset);
            } else {
                let _ = context
                    .offsets_cache
                    .insert((topic.to_string(), partition), offset)
                    .inspect_err(|_| {
                        tracing::warn!(
                            topic,
                            partition,
                            offset,
                            "Push offset error for topic `{topic}`"
                        )
                    });
            }
        }

        tracing::debug!(
            elapsed = ?now.elapsed(),
            cache.len = self.value.len(),
            chunk.len = chunk.len(),
            "Push to batch"
        );

        self.last_polling = Instant::now();
        if self.new_polling.is_none() {
            self.new_polling = Some(Instant::now());
        }

        if self.value.len() == 0 {
            tracing::trace!("Empty values, go next polling");
            // Empty values, go next polling.
            return Ok(ExitStatus::None);
        }

        if self.value.len() >= self.batch_size {
            tracing::debug!(
                cache.len = self.value.len(),
                "Batch size reached, send directly"
            );
            // Reaches batch size, send directly.
            return self
                .send_batch(pending_futs, cancel)
                .in_current_span()
                .await;
        }

        if self
            .new_polling
            .is_some_and(|v| v.elapsed().as_millis() as i64 > self.batch_timeout_ms)
        {
            tracing::debug!(
                cache.len = self.value.len(),
                "Batch timeout reached, send directly"
            );
            self.new_polling = None;
            // Reaches batch timeout, send directly.
            return self
                .send_batch(pending_futs, cancel)
                .in_current_span()
                .await;
        }

        tracing::trace!(
            cache.len = self.value.len(),
            "Stay in cache, go next polling"
        );
        // Partially in cache, go next polling.
        anyhow::Ok(ExitStatus::None)
    }

    async fn send_batch(
        &mut self,
        pending_futs: &mut FuturesOrdered<InspectTimeoutFuture<PendingAckFuture>>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<ExitStatus> {
        let start = Instant::now();
        let waiting_permit_fut = InspectTimeoutFuture::new(
            PENDING_ACK_TIMEOUT,
            self.permits.clone().acquire_owned(),
            Box::new(|elapsed| {
                tracing::warn!("waited to acquire permit for {elapsed:?}");
            }),
        );
        let Some(permit) = cancel.run_until_cancelled(waiting_permit_fut).await else {
            return Ok(ExitStatus::Aborted);
        };
        let elapsed = start.elapsed();
        if elapsed > self.max_acquire_elapsed {
            self.max_acquire_elapsed = elapsed;
            tracing::info!("acquire permit elapsed: {elapsed:?}");
        }
        let permit = permit.context("get permit error")?;
        debug_assert!(
            self.value.len() > 0,
            "value length should be greater than 0"
        );
        let mut batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.topic.finish()),
                Arc::new(self.partition.finish()),
                Arc::new(self.offset.finish()),
                Arc::new(self.key.finish()),
                Arc::new(self.value.finish()),
            ],
        )?;
        let batch_size = batch.num_rows();
        self.bid += 1;
        tracing::debug!(
            ipc.sender.cap = self.tx.capacity(),
            ipc.sender.len = self.tx.len(),
            ipc.batch.size = batch_size,
            ipc.batch.id = self.bid,
            "Send batch to IPC Writer",
        );

        let context = self.consumer.context();
        let offsets = context
            .offsets_cache
            .iter(&Guard::new())
            .map(|((topic, partition), offset)| PendingState {
                topic: topic.into(),
                partition: *partition,
                offset: *offset,
            })
            .collect::<Vec<_>>();
        let batch_id = BATCH_ID.fetch_add(1, Ordering::SeqCst);

        let offsets = serde_json::to_string(&offsets).context("seralize kafka offset error")?;
        let metaata = batch.schema_metadata_mut();
        metaata.insert("offsets".into(), offsets);
        metaata.insert("batch_id".into(), batch_id.to_string());
        self.send_batch_inner(batch_id, permit, batch, pending_futs)
            .await?;

        anyhow::Ok(ExitStatus::Finished)
    }

    pub async fn send_batch_inner(
        &mut self,
        batch_id: u64,
        permit: OwnedSemaphorePermit,
        batch: RecordBatch,
        pending_futs: &mut FuturesOrdered<InspectTimeoutFuture<PendingAckFuture>>,
    ) -> anyhow::Result<()> {
        self.tx
            .send_async(batch.clone())
            .await
            .context("Writer seems closed")?;
        let (tx, rx) = oneshot::channel();
        self.pending_batches
            .insert_async(batch_id, (tx, permit))
            .await
            .ok();
        let fut = InspectTimeoutFuture::new(
            PENDING_ACK_TIMEOUT,
            PendingAckFuture::new(rx),
            Box::new(|elapsed| {
                tracing::warn!("pending ack has been waiting for {elapsed:?}");
            }),
        );
        pending_futs.push_back(fut);
        self.metrics.ipc().add_extra_metric(&METRIC_SENT_BATCHES, 1);
        Ok(())
    }

    /// Safely send batches in cache or return timeout.
    pub async fn send(
        &mut self,
        pending_futs: &mut FuturesOrdered<InspectTimeoutFuture<PendingAckFuture>>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<ExitStatus> {
        if self.value.len() == 0 {
            if self.polling_timeout_ms <= 0 {
                return Ok(ExitStatus::None);
            }
            if self.last_polling.elapsed().as_millis() as i64 > self.polling_timeout_ms {
                // Reaches batch timeout.
                return Ok(ExitStatus::Timeout);
            }
            return Ok(ExitStatus::None);
        }
        self.send_batch(pending_futs, cancel)
            .in_current_span()
            .await
    }
}
