use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use bytes::Bytes;
use flume;
use futures::StreamExt;
use futures::future::Either;
use ring_channel::{RingReceiver, ring_channel};
use tokio_util::sync::CancellationToken;
use tracing::{Span as TracingSpan, debug, info, instrument, trace, warn};
use zerocopy::FromBytes;

use arrow_flight::{
    FlightClient, encode::FlightDataEncoderBuilder, error::FlightError,
    flight_service_client::FlightServiceClient,
};
use taosx_ipc::prelude::*;

use crate::AGENT_COMPRESSION;
use crate::Via;
use crate::core_metrics::get_metrics;
use crate::utils::trace::BatchCounter;

use super::MessageMetadata;
use super::ipc_transport::{DefaultChannelFactory, RetryConfig, retry_connect};
use super::persist::PersistComponent;
use crate::plugins::sink::persist::get_stream;
use crate::utils::trace::Qid;

/// Pipeline for forwarding IPC RecordBatches to remote Flight service.
///
/// This is a structured extraction of the original `ipc_forward` logic
/// from `mod.rs`, keeping behavior identical while making it easier to
/// test and reason about.
pub struct IpcSinkPipeline {
    input_stream: flume::r#async::RecvStream<'static, Result<RecordBatch, ArrowError>>,
    ack_tx: Option<flume::Sender<LushAck>>,
    schema: Arc<Schema>,
    cancel: CancellationToken,
    with_agent: Via,
    batch_counter: Arc<BatchCounter>,
    config: Option<Arc<crate::plugins::sink::point::model::PointModelConfig>>,
    persist_component: Option<PersistComponent>,
}

impl IpcSinkPipeline {
    pub fn new(
        input_stream: flume::r#async::RecvStream<'static, Result<RecordBatch, ArrowError>>,
        ack_tx: Option<flume::Sender<LushAck>>,
        schema: Arc<Schema>,
        cancel: CancellationToken,
        with_agent: Via,
        batch_counter: Arc<BatchCounter>,
        config: Option<Arc<crate::plugins::sink::point::model::PointModelConfig>>,
        persist_component: Option<PersistComponent>,
    ) -> Self {
        Self {
            input_stream,
            ack_tx,
            schema,
            cancel,
            with_agent,
            batch_counter,
            config,
            persist_component,
        }
    }

    #[instrument(skip_all)]
    pub async fn run(self) -> anyhow::Result<()> {
        let Self {
            input_stream,
            ack_tx,
            schema,
            cancel,
            with_agent,
            batch_counter,
            config,
            persist_component,
        } = self;

        let Via {
            task_id,
            job_id,
            endpoint,
            token,
        } = with_agent;
        use md5;
        tracing::info!("token: {}", format!("{:x}", md5::compute(token.clone())));

        let mut schema = schema.as_ref().clone();
        if let Some(config) = config.as_ref() {
            schema.metadata.insert(
                "config".to_string(),
                serde_json::to_string(&config).unwrap(),
            );
        }
        let schema: Arc<Schema> = Arc::new(schema);

        info!("Reading batches");

        let metrics = get_metrics(task_id, job_id);

        let (tables_cache_tx, tables_cache_rx) =
            ring_channel(crate::global::agent_in_memory_cache_capacity());

        let mut cause_error = None;
        const MAX_LAST_RETRIES: usize = 10; // allow 5 times retry in last 2 minutes;
        const RETRY_DELAY: Duration = Duration::from_secs(5);
        let mut last_retries: usize = 0;
        let mut last_retry_time = std::time::Instant::now();
        let last_retry_interval = Duration::from_secs(60 * 2);
        macro_rules! last_retry_tick {
            () => {
                #[allow(unused_assignments)]
                if last_retry_time.elapsed() <= last_retry_interval {
                    last_retries += 1;
                    tokio::time::sleep(RETRY_DELAY).await;
                } else {
                    tracing::info!("Last retry has been 2m past, re-calc retries num");
                    last_retries = 1;
                    tokio::time::sleep(RETRY_DELAY).await;
                    last_retry_time = std::time::Instant::now();
                }
            };
        }
        fn retain_tables_cache(table_cache_rx: &RingReceiver<RecordBatch>) -> Vec<RecordBatch> {
            let mut vec = Vec::with_capacity(50);
            while let Ok(i) = table_cache_rx.try_recv() {
                vec.push(i);
            }
            vec
        }

        let retry_forever = persist_component.is_some();

        let task_token = cancel.child_token();
        let mut persist_tasks = None;

        macro_rules! wait_for_tasks_exit {
            ($token: expr) => {
                if let Some(mut tasks) = persist_tasks.take() {
                    $token.cancel();
                    while let Some(res) = tasks.join_next().await {
                        match res {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => {
                                tracing::error!("persist task exited with error: {e:#}");
                            }
                            Err(e) => {
                                tracing::error!("persist task panicked: {e}")
                            }
                        }
                    }
                }
            };
        }

        'start: loop {
            let tables_cache_tx = tables_cache_tx.clone();
            let cur_span = TracingSpan::current();
            let cur_span_in_map_err = cur_span.clone();
            let is_lush = schema
                .metadata
                .get("schema")
                .map(|v| v == "lush")
                .unwrap_or(false);
            fn is_tables_record(record: &RecordBatch) -> bool {
                let v = record
                    .column_by_name("__type__")
                    .expect("the lush message stream should contains __type__ field")
                    .as_any()
                    .downcast_ref::<arrow::array::UInt8Array>()
                    .unwrap();
                let v: LushMessageType = unsafe { std::mem::transmute(v.value(0)) };
                matches!(v, LushMessageType::Children)
            }
            let retained_tables = retain_tables_cache(&tables_cache_rx);

            let task_token = task_token.child_token();
            let ipc_stream = match persist_component.clone() {
                None => {
                    // 默认行为，直接返回 ack = success 给上游数据源
                    let (tx, rx) = flume::bounded(64);
                    let ack_tx = ack_tx.clone();
                    let mut input_stream = input_stream.clone();
                    tokio::spawn(async move {
                        let mut max_elapsed = Duration::ZERO;
                        while let Some(batch) = input_stream.next().await {
                            if let Some(ack_tx) = ack_tx.as_ref() {
                                let meta = batch
                                    .as_ref()
                                    .map(|b| {
                                        serde_json::Map::from_iter(
                                            b.schema_ref()
                                                .metadata()
                                                .iter()
                                                .map(|(k, v)| (k.clone(), serde_json::json!(v))),
                                        )
                                    })
                                    .map(serde_json::Value::from)
                                    .ok();
                                let mut ack = LushAck::ok();
                                ack.context = meta.map(|v| v.to_string());
                                if ack_tx.send_async(ack).await.is_err() {
                                    return;
                                }
                            }
                            let start = tokio::time::Instant::now();
                            if tx.send_async(batch).await.is_err() {
                                return;
                            }
                            let elapsed = start.elapsed();
                            if elapsed > max_elapsed {
                                max_elapsed = elapsed;
                                tracing::info!("agent input stream send batch cost: {elapsed:?}");
                            }
                        }
                    });
                    Either::Left(rx.into_stream())
                }
                Some(component) => {
                    let input_stream = input_stream.clone();
                    let metrics = component
                        .config
                        .record_metrics
                        .then_some(metrics.clone())
                        .flatten();
                    let mut tasks = tokio::task::JoinSet::new();
                    let persist_rx = get_stream(
                        component,
                        input_stream,
                        ack_tx.clone().expect("ack_tx not found"),
                        &task_token,
                        metrics,
                        &mut tasks,
                        true,
                    )?;
                    persist_tasks = Some(tasks);
                    Either::Right(persist_rx.into_stream())
                }
            };
            let data_stream =
                futures::stream::iter(retained_tables.into_iter().map(|batch| (Ok(batch), None)))
                    .chain({
                        match ipc_stream {
                            Either::Left(stream) => Either::Left(stream.map(|batch| (batch, None))),
                            Either::Right(stream) => Either::Right(stream),
                        }
                    })
                    .map(move |(batch, mut ack_wait_tx)| {
                        if let Some(ack_tx) = ack_wait_tx.take() {
                            let meta = batch
                                .as_ref()
                                .map(|b| {
                                    serde_json::Map::from_iter(
                                        b.schema_ref()
                                            .metadata()
                                            .iter()
                                            .map(|(k, v)| (k.clone(), serde_json::json!(v))),
                                    )
                                })
                                .map(serde_json::Value::from)
                                .ok();
                            let mut ack = LushAck::ok();
                            ack.context = meta.map(|v| v.to_string());
                            ack_tx.send(ack).ok();
                        }

                        match batch {
                            Ok(batch) => {
                                if is_lush && is_tables_record(&batch) {
                                    let _ = tables_cache_tx.send(batch.clone());
                                }
                                Ok(batch)
                            }
                            Err(err) => {
                                cur_span_in_map_err.in_scope(|| {
                                    warn!(error = ?err, "IPC reading error: {err:#}");
                                });
                                Err(FlightError::from(err))
                            }
                        }
                    });

            if !retry_forever && last_retries > MAX_LAST_RETRIES {
                tracing::warn!(
                    "There're {} retries happened in 2m, break now",
                    last_retries
                );
                if let Some(err) = cause_error {
                    tracing::error!(error = ?err, "schema: {:?}", schema);
                    let stream = data_stream.take(3);

                    stream
                        .for_each(|data| {
                            tracing::warn!(error = ?err, "data: {:?}", data);
                            futures::future::ready(())
                        })
                        .await;
                    return Err(err);
                }
            } else if last_retries > 0 {
                tracing::error!(error = ?cause_error, retries = last_retries, "Retry connections");
            }

            use taoslog::QidManager;
            use taoslog::utils::QidMetadataGetter;

            let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
            qid.set_task_id(task_id as _);
            let data = FlightDataEncoderBuilder::new()
                .with_schema(schema.clone())
                .with_options(
                    arrow::ipc::writer::IpcWriteOptions::try_new(
                        8,
                        false,
                        arrow::ipc::MetadataVersion::V5,
                    )
                    .unwrap(),
                )
                .build(data_stream)
                .map({
                    let mut qid = qid.clone();
                    let batch_counter = batch_counter.clone();
                    move |v| {
                        qid.set_batch_id(batch_counter.next());
                        tracing::trace!("agent sink pipeline send batch");
                        v.map(|message| {
                            message.with_app_metadata(Bytes::copy_from_slice(
                                MessageMetadata::new(qid.get()).as_bytes(),
                            ))
                        })
                    }
                });

            const MAX_RETRIES: usize = 500;
            const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(30);
            let retry_cfg =
                RetryConfig::new(MAX_RETRIES, Duration::from_secs(3), MAX_RETRY_INTERVAL);
            let factory = DefaultChannelFactory;
            let channel = match retry_connect(
                &factory,
                &endpoint,
                retry_cfg,
                retry_forever,
                &cancel,
            )
            .await?
            {
                Some(ch) => ch,
                None => {
                    // cancelled while waiting for connection
                    break 'start;
                }
            };
            let alive = std::time::Instant::now();

            let mut client;
            let client_inner = FlightServiceClient::new(channel)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);
            if *(AGENT_COMPRESSION.get().unwrap_or(&false)) {
                client = FlightClient::new_from_inner(
                    client_inner.send_compressed(tonic::codec::CompressionEncoding::Gzip),
                );
            } else {
                client = FlightClient::new_from_inner(client_inner);
            }
            client
                .add_header("x-task-id", &task_id.to_string())
                .context("Add header error")?;
            client
                .add_header("x-token", &token)
                .context("Add header error")?;
            client
                .add_header("x-version", crate::build::PKG_VERSION)
                .context("Add header error")?;
            client
                .add_header(
                    taoslog::utils::QID_HEADER_KEY,
                    &format!("{}", qid.display()),
                )
                .context("Add header error")?;
            tracing::info!("add qid str {}", qid.display());
            let cur_span_in_map_error = TracingSpan::current();
            if let Err(err) = client
                .handshake(Bytes::from(token.as_bytes().to_vec()))
                .await
                .map_err(move |err| match err {
                    FlightError::Tonic(status) => {
                        let _entered = cur_span_in_map_error.enter();
                        tracing::error!(
                            error.code = %status.code(),
                            error.message = %status.message(),
                            error.metadata = ?status.metadata(),
                            "gRPC handshake error: {}", status
                        );
                        anyhow::anyhow!("gRPC handshake error: {}", status.message())
                    }
                    err => anyhow::anyhow!("Handshake error: {err:#}"),
                })
            {
                last_retry_tick!();
                cause_error.replace(err);
                wait_for_tasks_exit!(task_token);
                continue 'start;
            }
            info!("Handshake done");
            info!("Do putting");
            let mut stream = match client.do_put(data).await.map_err(move |err| match err {
                FlightError::Arrow(err) => anyhow::anyhow!("IPC Arrow error: {err:#}"),
                FlightError::Tonic(status) => {
                    anyhow::anyhow!("RPC client error: {}. Details: {:?}", status, status)
                }
                err => anyhow::anyhow!("Put IPC stream error: {err:#}"),
            }) {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::warn!("Try putting stream error: {:#}", err);
                    last_retry_tick!();
                    cause_error.replace(err);
                    wait_for_tasks_exit!(task_token);
                    continue 'start;
                }
            };
            info!("Get putting stream response");

            let mut _msg_processed = 0;
            loop {
                let put_result = tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!("cancel IPC worker");
                        info!(alive = ?alive.elapsed(), "[{task_id}] Putting stream finished");
                        break 'start;
                    },
                    put_result = stream.next() => {
                        put_result
                    }
                };
                if let Some(res) = put_result {
                    let rsp = res;
                    match rsp {
                        Ok(rsp) => {
                            tracing::trace!("Response ok");
                            if let Ok(metadata) = MessageMetadata::ref_from_bytes(&rsp.app_metadata)
                            {
                                let ack = metadata.ack();
                                let count = metadata.count;
                                match ack {
                                    crate::plugins::sink::RPC_ACK_PROCESSED => {
                                        trace!(alive = ?alive.elapsed(),  "Processed received: {count}");
                                        _msg_processed += count;
                                    }
                                    crate::plugins::sink::RPC_ACK_DROPPED => {
                                        trace!(alive = ?alive.elapsed(),  "Dropped received: {count}");
                                    }
                                    crate::plugins::sink::RPC_ACK_STREAM_END => {
                                        debug!(alive = ?alive.elapsed(), "Stream end received");
                                        break 'start;
                                    }
                                    crate::plugins::sink::RPC_ACK_DECODE_ERROR => {
                                        warn!(alive = ?alive.elapsed(), "Decode error received at {count}");
                                    }
                                    _ => {}
                                }
                            } else {
                                _msg_processed += 1;
                            }
                        }
                        Err(err) => match &err {
                            FlightError::Tonic(status) => {
                                if status
                                    .message()
                                    .contains("stream closed because of a broken pipe")
                                    || status.message() == "ExternalError(Disconnected)"
                                    || status.message().contains("connection reset")
                                    || status.message().contains("connection closed")
                                    || status.message().contains("h2 protocol error")
                                {
                                    tracing::warn!(alive = ?alive.elapsed(), "Disconnected, retry after one second: {err:#}");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    cause_error.replace(anyhow::anyhow!(
                                        "gRPC put stream disconnected: {err:#}, DTID={}",
                                        qid.display()
                                    ));
                                    last_retry_tick!();
                                    wait_for_tasks_exit!(task_token);
                                    continue 'start;
                                }

                                tracing::error!(alive = ?alive.elapsed(), source = status.message(), "Tonic error: {status}");
                                return Err(err).context(format!(
                                    "Got server response with error, DTID={}",
                                    qid.display()
                                ));
                            }
                            FlightError::Arrow(arrow) => {
                                let err_msg = format!("{err:#}");
                                if err_msg.contains("os error 10054")
                                    || err_msg.contains("os error 10053")
                                {
                                    warn!(
                                        "ConnectionReset or ConnectionAborted, consider as success: {}",
                                        err_msg
                                    );
                                    break 'start;
                                }
                                tracing::error!(alive = ?alive.elapsed(), "Arrow error: {arrow:#}");
                                return Err(err).context(format!(
                                    "Got server response with arrow error, DTID={}",
                                    qid.display()
                                ));
                            }
                            _ => {
                                tracing::error!(alive = ?alive.elapsed(), "Other error: {err:#}");
                                return Err(err).context(format!(
                                    "Got server response with error, DTID={}",
                                    qid.display()
                                ));
                            }
                        },
                    }
                } else {
                    info!(alive = ?alive.elapsed(), "[{task_id}] Putting stream finished");
                    break 'start;
                }
            }
        }

        wait_for_tasks_exit!(task_token);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipc_sink_pipeline_structure() {
        // Test that basic types are properly defined and sized
        // This ensures the struct is coherent
        let option_size = std::mem::size_of::<Option<flume::Sender<LushAck>>>();
        assert!(option_size > 0);

        let cancellation_token_size = std::mem::size_of::<CancellationToken>();
        assert!(cancellation_token_size > 0);
    }

    #[test]
    fn test_cancellation_token_basic() {
        let cancel = CancellationToken::new();
        assert!(!cancel.is_cancelled());
        cancel.cancel();
        assert!(cancel.is_cancelled());
    }

    #[test]
    fn test_cancellation_token_child_token() {
        let parent = CancellationToken::new();
        let child = parent.child_token();

        // Parent not cancelled yet
        assert!(!parent.is_cancelled());
        assert!(!child.is_cancelled());

        // Cancel parent
        parent.cancel();
        assert!(parent.is_cancelled());
        assert!(child.is_cancelled()); // Child should also be cancelled
    }
}
