use anyhow::Context;
use arrow::array::{
    ArrayBuilder, BinaryBuilder, Int32Builder, RecordBatch, StringBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
};
use arrow_schema::SchemaRef;
use chrono::Utc;
use futures_ext::InspectTimeoutFuture;
use futures_util::stream::FuturesOrdered;
use itertools::Itertools;
use pulsar::consumer::Message;
use scc::ebr::Guard;
use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use taosx_core::{
    core_metrics::CoreMetrics,
    utils::codec::{Processor, StringDecoder},
};
use tokio::sync::{OwnedSemaphorePermit, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    BATCH_ID, ExitStatus, METRIC_CONSUMED_MESSAGES, METRIC_SEND_MESSAGES, METRIC_SENT_BATCHES,
    PENDING_ACK_TIMEOUT, PendingBatches, PendingState,
    config::{
        connect::DataVendor,
        tuya::{self, ENCRYPT_MODEL, TuyaMessage},
    },
    context::CustomContext,
    decrypt::Decryptor,
    pending_ack_fut::PendingAckFuture,
};

pub struct MessagesSender<'a> {
    // pub consumer: &'a LoggingConsumer,
    pub context: Arc<CustomContext>,
    pub tx: &'a flume::Sender<RecordBatch>,
    // Send options
    pub batch_size: usize,
    pub batch_timeout: Duration,
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
    pub ledger_id: UInt64Builder,
    pub entry_id: UInt64Builder,
    pub key: BinaryBuilder,
    pub value: BinaryBuilder,
    pub schema: &'a SchemaRef,

    pub pending_batches: &'a PendingBatches,
    pub permits: &'a Arc<tokio::sync::Semaphore>,
    pub max_acquire_elapsed: Duration,

    pub metrics: &'a Arc<CoreMetrics>,
}

impl<'a> MessagesSender<'a> {
    pub async fn send_chunk(
        &mut self,
        chunk: &[Message<Vec<u8>>],
        pending_futs: &mut FuturesOrdered<InspectTimeoutFuture<PendingAckFuture>>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<ExitStatus> {
        let now = std::time::Instant::now();
        self.context
            .metrics()
            .add_extra_metric(&METRIC_CONSUMED_MESSAGES, chunk.len() as _);
        let chunks = chunk
            .iter()
            .chunk_by(|msg| (&msg.topic, msg.message_id().partition.unwrap_or(-1)));

        let mut error = None;
        for ((topic, partition), iter) in &chunks {
            let mut offset = None;
            for msg in iter {
                let decryptor = msg
                    .metadata()
                    .properties
                    .iter()
                    .filter_map(|kv| {
                        if kv.key == ENCRYPT_MODEL {
                            Some(Decryptor::from(kv.value.as_str()))
                        } else {
                            None
                        }
                    })
                    .next();
                offset.replace(msg.message_id.clone());
                let s = msg.deserialize();
                if !s.is_empty() {
                    let mut value = match self.codec_processor.process(s) {
                        Ok(payload) => {
                            self.codec_err_count = 0;
                            payload
                        }
                        Err(e) => {
                            tracing::error!("pulsar source codec process message error: {e:#}");
                            self.codec_err_count += 1;
                            if self.codec_err_count < 3 {
                                error = Some(e);
                                continue;
                            }

                            return Err(e);
                        }
                    };
                    if self.context.data_vendor == DataVendor::Tuya {
                        value =
                            descypt_tuya(decryptor, self.context.tuya_access_key.as_ref(), &value)
                                .with_context(|| {
                                    format!(
                                        "pulsar tuya decrypt message error, msg properties: {:?}",
                                        msg.metadata().properties
                                    )
                                })?;
                    }
                    self.timestamp.append_value(
                        Utc::now()
                            .timestamp_nanos_opt()
                            .expect("Get now timestamp in nanosecond should always success"),
                    );
                    self.topic.append_value(&msg.topic);
                    self.partition.append_value(partition);
                    self.ledger_id.append_value(msg.message_id().ledger_id);
                    self.entry_id.append_value(msg.message_id().entry_id);
                    self.key.append_value(msg.key().unwrap_or("".to_owned()));
                    self.value.append_value(value);
                }
            }
            let offset = offset.expect("offset should always exist");

            if let Some(entry) = self
                .context
                .offsets_cache
                .get(&(topic.to_string(), partition))
            {
                entry.update(offset);
            } else {
                let _ = self
                    .context
                    .offsets_cache
                    .insert((topic.to_string(), partition), offset.clone())
                    .inspect_err(|_| {
                        tracing::warn!(
                            topic,
                            partition,
                            "Pulsar push offset error for topic `{topic}`, offset: {offset:?}"
                        )
                    });
            }
        }
        if self.value.is_empty()
            && let Some(e) = error
        {
            return Err(e);
        }

        tracing::debug!(
            elapsed = ?now.elapsed(),
            cache.len = self.value.len(),
            chunk.len = chunk.len(),
            "Pulsar push to batch"
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
            .is_some_and(|v| v.elapsed() > self.batch_timeout)
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

        tracing::debug!(
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
            tracing::info!("send_batch acquire permit elapsed: {elapsed:?}");
        }
        let permit = permit.context("get permit error")?;
        debug_assert!(
            self.value.len() > 0,
            "send_batch value length should be greater than 0"
        );
        let mut batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.topic.finish()),
                Arc::new(self.partition.finish()),
                Arc::new(self.ledger_id.finish()),
                Arc::new(self.entry_id.finish()),
                Arc::new(self.key.finish()),
                Arc::new(self.value.finish()),
            ],
        )?;
        let batch_id = BATCH_ID.fetch_add(1, Ordering::SeqCst);
        let batch_size = batch.num_rows();
        tracing::debug!(
            ipc.sender.cap = self.tx.capacity(),
            ipc.sender.len = self.tx.len(),
            ipc.batch.size = batch_size,
            ipc.batch.id = batch_id,
            "Send batch to IPC Writer",
        );
        let offsets = self
            .context
            .offsets_cache
            .iter(&Guard::new())
            .map(|((topic, partition), offset)| PendingState {
                topic: topic.into(),
                partition: *partition,
                ledger_id: offset.id.ledger_id,
                entry_id: offset.id.entry_id,
                batch_size,
            })
            .collect::<Vec<_>>();
        let offsets = serde_json::to_string(&offsets).context("serialize pulsar offset error")?;
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
        let (tx, rx) = oneshot::channel();
        self.pending_batches
            .insert_async(batch_id, (tx, permit))
            .await
            .ok();
        self.tx
            .send_async(batch.clone())
            .await
            .context("Writer seems closed")?;
        let fut = InspectTimeoutFuture::new(
            PENDING_ACK_TIMEOUT,
            PendingAckFuture::new(rx),
            Box::new(move |elapsed| {
                tracing::warn!("pending ack {batch_id} has been waiting for {elapsed:?}");
            }),
        );
        pending_futs.push_back(fut);
        self.metrics.ipc().add_extra_metric(&METRIC_SENT_BATCHES, 1);
        self.metrics
            .ipc()
            .add_extra_metric(&METRIC_SEND_MESSAGES, batch.num_rows() as u64);
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

pub fn descypt_tuya(
    decryptor: Option<Decryptor>,
    key: Option<&String>,
    value: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let Some(decryptor) = decryptor else {
        return Err(anyhow::anyhow!(
            "pulsar tuya source has no em field in properties"
        ));
    };
    let mut tuya_msg = match serde_json::from_slice::<TuyaMessage>(value) {
        Ok(tuya_msg) => tuya_msg,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "pulsar tuya parse TuyaMessage error: {e:#}"
            ));
        }
    };
    if let Some(key) = key {
        tuya_msg.data = decryptor.decrypt(&tuya_msg.data, &key[tuya::KEY_START..tuya::KEY_END])?;
    };
    Ok(serde_json::to_vec(&tuya_msg)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decrypt::Decryptor;

    #[test]
    fn test_descypt_tuya() {
        let decryptor = Decryptor::from("aes_gcm");
        let key = "fbe6805862cc4527a90e782967c79b31".to_string();
        let data = r#"qzGAKa00XbTK4HPjElvpkuYN/fXYyj2BhdTbE6+l6cONQLctSXTwwsSlkrNo+30mlJQeaNZ73vh/NuSeyf4HQgmHrdb3bonBWkxjdbD+bGrDUAr77zAj2RUTyR8inKwqJWaSfnva4UEUW2xRUfWCTRYjyyLJsHO5m8Plg+lW8q5Rg83yEPQniHi1UjEOL34c7fz88PBaNm7MD+5deyG4czT4ZsO+VpwZ2yB6CXDwgGtZhspEHF6EaiNvzo+Rxr0kL+UW+f/dmCkGjxmcHlqpDqdUrrI0ZPc="#.to_string();
        let tuya_msg = TuyaMessage {
            data,
            protocol: 4,
            pv: "1.0".to_string(),
            sign: "eee8d1dec8b008c8b71ab37635365aab".to_string(),
            t: 1762254678813,
        };
        let decrypted = descypt_tuya(
            Some(decryptor),
            Some(&key),
            &serde_json::to_vec(&tuya_msg).unwrap(),
        )
        .unwrap();
        let decrypted_msg = serde_json::from_slice::<TuyaMessage>(&decrypted).unwrap();
        assert_eq!(
            decrypted_msg.data,
            r#"{"dataId":"000642BB7075D85F7DA5A0BF6807233B","devId":"ebc778f3c5d9908ff6plgl","productKey":"9exm2qiar0dvqoxv","status":[{"3":"40","code":"humidity_current","t":1762222673354,"value":40}]}"#
        );
    }
}
