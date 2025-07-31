use anyhow::{anyhow, bail, Context};
use archive::utils::files::read_parquet_file;
use archive::{get_rewrite_files, Archive, ArchiveConsumer, ArchiveType, Cache};
use arrow::array::{Array, StringArray, UInt8Array};
use arrow::{datatypes::Schema, ipc::writer::IpcWriteOptions, record_batch::RecordBatch};
use arrow_compute_ext::RecordBatchExt;
use arrow_flight::{flight_service_client::FlightServiceClient, FlightClient};
use arrow_schema::ArrowError;
use async_backtrace::framed;
use bytes::Bytes;
use chrono::{TimeDelta, Utc};
use deadpool::managed::{PoolError, TimeoutType};
use faststr::FastStr;
use flume::Sender;
use futures::future::Either;
use futures_util::{Sink, Stream, StreamExt};
use persist::{get_stream, PersistComponent, PersistComponents};
use ring_channel::{ring_channel, RingReceiver};
use serde_json::json;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::ops::Mul;
use std::path::PathBuf;
use std::{
    any::Any,
    cell::Cell,
    collections::{HashMap, HashSet},
    io::{Read, Write},
    iter::zip,
    net::SocketAddr,
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use taos::{
    taos_query::{common::Describe, Manager},
    Itertools, Taos, TaosPool, Value,
};
use taoslog::utils::QidMetadataGetter;
use taoslog::QidManager;
use taosx_ipc::{prelude::*, stream::point::PointMessage};
use tokio::sync::{Mutex, Notify, OnceCell};
use tonic::transport::ClientTlsConfig;
use tonic::{codec::CompressionEncoding, transport::Channel};
use tracing::{debug, error, info, instrument, trace, warn};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::core_metrics::{get_metrics, get_metrics_arc_from_i64, get_metrics_arc_or};
use crate::core_metrics::{CoreMetrics, TaskMetrics};
use crate::plugins::runners::opc::config::OPCConfig;
use crate::plugins::runners::opc::model::OpcModelConfig;
use crate::plugins::transform::archive_records;
use crate::plugins::transform::handling_strategy::{HandlingResult, ProcessOnAbnormalEnum};
use crate::plugins::transform::WrittenMethod;
use crate::plugins::*;
use crate::utils::breakpoints::BreakpointDb;
use crate::utils::sql::get_minimum_timestamp;
use crate::utils::trace::{BatchCounter, Qid};
use crate::AGENT_COMPRESSION;
use crate::{utils::breakpoints::breakpoints_set, ConnectorLicense, Parser, Transferred};

use self::point::handle_transform;
use self::point::point_records_to_sql;
use self::{
    flat::{
        flat_write_with_raw_block, flat_write_with_sql, ipc_flat_stream_worker_concurrent,
        ipc_flat_stream_worker_vgroup, ipc_flat_stream_worker_vgroup_sequential,
    },
    ipc_metric::IpcMetrics,
    lush::{LushModelConfig, TableTagCache},
};

pub mod flat;
pub mod ipc_metric;
pub mod lush;
pub mod persist;
pub mod point;

pub const RPC_ACK_REQUEST: u8 = 0;
pub const RPC_ACK_RECEIVED: u8 = 1;
pub const RPC_ACK_PROCESSED: u8 = 2;
pub const RPC_ACK_DROPPED: u8 = 3;
pub const RPC_ACK_STREAM_END: u8 = 0xFE;
pub const RPC_ACK_DECODE_ERROR: u8 = 0xFF;

#[derive(FromBytes, Immutable, KnownLayout, IntoBytes)]
#[repr(C, packed)]
pub struct MessageMetadata {
    /// Ack count.
    ///
    /// Only the first 8 bytes are used.
    /// - 0 for request.
    /// - 1 for received.
    /// - 2 for processed.
    /// - 3 for dropped.
    /// - 0xFE for sink stream end.
    /// - 0xFF for decode error.
    ///
    ack: [u8; 8],
    /// taos Qid
    qid: u64,
    // Current received batch count.
    count: u64,
}

impl MessageMetadata {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }

    #[inline]
    pub fn ack(&self) -> u8 {
        self.ack[0]
    }

    #[inline]
    pub fn qid(&self) -> u64 {
        self.qid
    }

    #[inline]
    pub fn new(qid: u64) -> Self {
        Self {
            ack: [0; 8],
            qid,
            count: 0,
        }
    }

    #[inline]
    pub fn new_ack(ack: u8, trace_id: u64, count: u64) -> Self {
        Self {
            ack: [ack, 0, 0, 0, 0, 0, 0, 0],
            qid: trace_id,
            count,
        }
    }

    pub fn set_ack(&mut self, ack: u8) -> &mut Self {
        self.ack[0] = ack;
        self
    }
}

async fn ipc_tcp_forward(
    stream: std::net::TcpStream, // socket2::Socket,
    cancel: CancellationToken,
    remote: String, // "http://127.0.0.1:6051"
    token: String,
    task_id: i64,
    batch_counter: BatchCounter,
    config: Option<OpcModelConfig>,
    persist_components: Option<PersistComponents>,
) -> anyhow::Result<()> {
    use md5;
    tracing::info!("token: {}", format!("{:x}", md5::compute(token.clone())));
    use arrow_flight::{encode::FlightDataEncoderBuilder, error::FlightError};
    use futures::StreamExt;
    let reader_stream = stream
        .try_clone()
        .context("Try clone IPC stream as reader error")?;
    let ipc_reader = tokio::task::spawn_blocking(move || IpcReader::new(reader_stream))
        .in_current_span()
        .await?;
    if let Err(err) = ipc_reader {
        let msg = format!("{err:#}");
        if msg.contains("Parser error: Unable to get root as message") {
            // 关闭没有发过数据的连接会导致这个错误, 是正常行为
            tracing::warn!("Build IPC stream reader error: {err:#}");
            return Ok(());
        } else {
            return Err(err).context("Build IPC stream reader error");
        }
    }
    let ipc_reader = ipc_reader.unwrap();
    let ack = ipc_reader.ack();
    let mut ipc_ack_writer =
        tokio::task::spawn_blocking(move || AckWriterBuilder::new(ack).open(stream))
            .in_current_span()
            .await
            .context("Spawn AckWriter error")?
            .context("Create AckWriter error")?;

    let schema = ipc_reader.schema.clone();
    let persist_component = match persist_components {
        Some(components) => match components.components.get(&schema) {
            Some(component) => Some(component.clone()),
            None => {
                tracing::error!("persist component not found for schema: {schema}");
                None
            }
        },
        None => None,
    };
    let mut schema = schema.as_ref().clone();
    if let Some(config) = config.as_ref() {
        schema.metadata.insert(
            "config".to_string(),
            serde_json::to_string(&config).unwrap(),
        );
    }
    let schema: Arc<Schema> = Arc::new(schema);

    info!("Reading batches");

    let metrics = get_metrics(task_id).await;

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
    let (input_stream, ack_tx) = if let Some(component) = persist_component.as_ref() {
        let batch_chunk_size = component.config.batch_chunk_size.unwrap_or(100);
        let (ack_tx, ack_rx) = flume::bounded(batch_chunk_size);
        tokio::task::spawn_blocking(move || {
            while let Ok(ack) = ack_rx.recv() {
                ipc_ack_writer.ack(ack).inspect_err(|err| {
                    tracing::error!("Write ack error: {err:#}");
                })?;
            }
            anyhow::Ok(())
        });
        (ipc_reader.into_raw_stream(), Some(ack_tx))
    } else {
        (ipc_reader.into_raw_stream_qos_0(ipc_ack_writer), None)
    };

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
                            tracing::error!("persist task paniced: {e}")
                        }
                    }
                }
            }
        };
    }

    'start: loop {
        let tables_cache_tx = tables_cache_tx.clone();
        let cur_span = Span::current();
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
                .downcast_ref::<UInt8Array>()
                .unwrap();
            let v: LushMessageType = unsafe { std::mem::transmute(v.value(0)) };
            matches!(v, LushMessageType::Children)
        }
        let retained_tables = retain_tables_cache(&tables_cache_rx);

        let task_token = task_token.child_token();
        let ipc_stream = match persist_component.clone() {
            None => {
                // 默认行为，直接返回 ack = success 给上游数据源
                Either::Left(input_stream.clone())
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
                    let meta = batch
                        .as_ref()
                        .map(|b| {
                            serde_json::Map::from_iter(
                                b.schema_ref()
                                    .metadata()
                                    .iter()
                                    .map(|(k, v)| (k.clone(), json!(v))),
                            )
                        })
                        .map(serde_json::Value::from)
                        .ok();
                    if let Some(ack_tx) = ack_wait_tx.take() {
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

        let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
        // debug_assert!(qid.task_id() > 0);
        let cur_batch_number = batch_counter.next().await?;
        qid.set_batch_id(cur_batch_number);
        let data = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .with_options(
                IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap(),
            )
            .build(data_stream)
            .map({
                let qid = qid.clone();
                move |v| {
                    v.map(|message| {
                        message.with_app_metadata(Bytes::copy_from_slice(
                            MessageMetadata::new(qid.get()).as_bytes(),
                        ))
                    })
                }
            });

        const MAX_RETRIES: usize = 500;
        const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(30);
        let mut retries = 0;
        let mut retry_interval = Duration::from_secs(3);
        let channel = loop {
            match try_establish_channel(remote.clone()).await {
                Ok(channel) => {
                    tracing::info!("connect to {remote} successfully!");
                    break channel;
                }
                Err(err) => {
                    retries += 1;
                    tracing::error!("Failed to establish connection: {}. Retrying...", err);
                    if !retry_forever && retries >= MAX_RETRIES {
                        tracing::error!("Max retries reached. Exiting...");
                        return Err(err);
                    }
                    if tokio::time::timeout(retry_interval, cancel.cancelled())
                        .await
                        .is_ok()
                    {
                        break 'start;
                    }
                    retry_interval = retry_interval.mul(2).min(MAX_RETRY_INTERVAL);
                }
            }
        };
        let alive = std::time::Instant::now();

        let mut client;
        let client_inner = FlightServiceClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        if *(AGENT_COMPRESSION.get().unwrap_or(&false)) {
            client = FlightClient::new_from_inner(
                client_inner.send_compressed(CompressionEncoding::Gzip),
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
        let cur_span_in_map_error = Span::current();
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
        // dbg!(res);
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
                        use zerocopy::FromBytes;
                        if let Ok(metadata) = MessageMetadata::ref_from_bytes(&rsp.app_metadata) {
                            let ack = metadata.ack();
                            let count = metadata.count;
                            match ack {
                                RPC_ACK_PROCESSED => {
                                    trace!(alive = ?alive.elapsed(),  "Processed received: {count}");
                                    _msg_processed += count;
                                }
                                RPC_ACK_DROPPED => {
                                    trace!(alive = ?alive.elapsed(),  "Dropped received: {count}");
                                }
                                RPC_ACK_STREAM_END => {
                                    debug!(alive = ?alive.elapsed(), "Stream end received");
                                    break 'start;
                                }
                                RPC_ACK_DECODE_ERROR => {
                                    warn!(alive = ?alive.elapsed(), "Decode error received at {count}");
                                }
                                _ => {}
                            }
                        } else {
                            _msg_processed += 1;
                        }
                    }
                    Err(err) => {
                        match &err {
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
                                    warn!("ConnectionReset or ConnectionAborted, consider as success: {}", err_msg);
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
                        }
                    }
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

async fn try_establish_channel(remote: String) -> anyhow::Result<Channel> {
    let mut endpoint = tonic::transport::Endpoint::try_from(remote)?
        .keep_alive_while_idle(true)
        .keep_alive_timeout(Duration::from_secs(300))
        .http2_keep_alive_interval(Duration::from_secs(39))
        .tcp_keepalive(Some(Duration::from_secs(7200))); // keep alive for 2 hours

    if let Some(ca) = crate::global::get_agent_client_ca() {
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(ca)
                    .with_enabled_roots(),
            )
            .context("Unable to create TLS config for endpoint")?;
    }
    let channel = endpoint.connect().await?;
    Ok(channel)
}

#[framed]

async fn ipc_tcp_read(
    pool: TaosPool,
    stream: std::net::TcpStream, //socket2::Socket,
    opc_model_config: Option<OpcModelConfig>,
    lush_model_config: Option<LushModelConfig>,
    cancel: CancellationToken,
    parser: Option<Parser>,
    connector: Option<&'static str>,
    task_id: Option<i64>,
    batch_counter: Option<BatchCounter>,
    notifier: crate::TaskNotifySender,
    persist_component: Option<PersistComponents>,
) -> anyhow::Result<()> {
    // let stream = Arc::new(stream);
    // let reader = stream.clone();
    info!("Prepare IPC stream reader");
    let reader_stream = stream.try_clone().context("Clone tcp stream error")?;
    let ipc_reader = tokio::task::spawn_blocking(move || {
        IpcReader::new(reader_stream).context("IPC reading error")
    })
    .await??;
    info!("Prepare IPC ACK writer");
    // dbg!(ipc_reader.ack());
    let ack = ipc_reader.ack();
    let ipc_ack_writer =
        tokio::task::spawn_blocking(move || AckWriterBuilder::new(ack).open(stream))
            .await?
            .context("Can't open IPC ACK writer")?;
    info!("Processing IPC stream");
    ipc_process(
        pool,
        ipc_reader,
        ipc_ack_writer,
        opc_model_config,
        lush_model_config,
        cancel,
        parser,
        connector,
        task_id,
        batch_counter,
        notifier,
        persist_component,
    )
    .await?;
    info!("IPC stream processed");
    Ok(())
    // tokio::select! {
    // _ = cancel.cancelled() => {
    //     tracing::debug!("cancel IPC worker");
    //     Ok(())
    // },
    // done = ipc_process(client, pool, ipc_reader, ipc_ack_writer, lock, config, parser, connector, transferred) => {
    //     tracing::info!("IPC stopped");
    //     done
    // }
    // }
}

// #[cfg(unix)]
// async fn ipc_unix_read(
//     client: String,
//     pool: TaosPool,
//     stream: std::os::unix::net::UnixStream,
//     lock: Arc<Mutex<()>>,
//     config: Option<OpcTableConfig>,
// ) -> anyhow::Result<()> {
//     let ipc_reader = IpcReader::new(&stream).unwrap();
//     let ipc_ack_writer = AckWriterBuilder::new(ipc_reader.ack()).open(&stream);
//     ipc_process(
//         client,
//         pool,
//         ipc_reader,
//         ipc_ack_writer,
//         lock,
//         config,
//         None,
//         None,
//         None,
//     )
//     .await
// }

struct LushMessageTagModify {
    // (create table sql, overflow, table count in sql)
    sqls: Vec<(String, bool, u16)>,
    tags: Vec<(FastStr, Value)>,
}

// #[instrument(skip(taos, record, names, marks))]
#[instrument(skip_all)]
async fn consume_lush_record(
    pool: &TaosPool,
    taos: &mut Option<deadpool::managed::Object<Manager<TaosBuilder>>>,
    record: LushMessage,
    columns: &[String],
    count: &mut usize,
    task: Option<i64>,
    metrics: &IpcMetrics,
) -> anyhow::Result<()> {
    if unsafe { crate::global::DRY_RUN } {
        tracing::trace!("consume lush record in dry-run mode");
        return Ok(());
    }
    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    match record {
        LushMessage::Tables(tables, _) => {
            let taos = taos.as_ref().unwrap();
            // let mut sql = format!("CREATE TABLE ");
            // map: <stable_name, (Vec<sql, sql_overflow?>, Vec<tag_name, tag_value>)>
            let mut create_sql_map: HashMap<FastStr, LushMessageTagModify> = HashMap::new();
            // map: <stable_name, table_count>
            let mut table_set = HashSet::new();
            for table in tables {
                let table_name = table.table_name();
                if !table_set.insert(table_name.to_string()) {
                    continue;
                }
                let tags = table.tags();
                if tags.is_none() {
                    continue;
                }
                let tags = tags.as_ref().unwrap();
                let mut query_tags_sql = "SELECT distinct tbname,".to_string();
                for (tagname, _) in tags {
                    query_tags_sql.push_str(format!("`{tagname}`,").as_str());
                }
                query_tags_sql.pop();
                query_tags_sql.push_str(format!(" from `{table_name}`").as_str());
                match taos.query_one::<_, Vec<Value>>(&query_tags_sql).await {
                    Ok(rs) => {
                        let mut rs = rs.expect("query tags result should not be empty");
                        rs.remove(0);

                        for (exist, (tagname, expect)) in rs.iter().zip(tags) {
                            if expect.is_null() {
                                continue;
                            }
                            let exist_value = exist.to_sql_value();
                            let expect_value = expect.to_sql_value();
                            if exist_value != expect_value {
                                qid.add_sub_batch_id();
                                tracing::info!(
                                    "table {table_name} tag value not match, new: {}, old:{}",
                                    expect.to_sql_value(),
                                    exist.to_sql_value()
                                );
                                let alter_set_sql = format!(
                                    "alter table `{table_name}` set TAG `{tagname}`={}",
                                    expect.to_sql_value()
                                );
                                tracing::info!("alter_set_sql: {alter_set_sql}");
                                if let Err(err) = taos
                                    .exec_with_req_id(alter_set_sql, qid.get())
                                    .in_current_span()
                                    .await
                                {
                                    tracing::info!("Try to alter table {table_name} tag `{tagname}` error: {err:#}");
                                }
                            }
                        }
                    }
                    Err(err) => {
                        let errstr = format!("{err:#}");
                        if errstr.contains("0x2603") || errstr.contains("0x2662") {
                            // 0x2603: the table does not exist
                            // 0x2662: the table does not exist
                            let table_sql = table.to_sql(None);
                            if table_sql.is_some() {
                                let stable_name = table.stable_name().unwrap();
                                let table_sql = table_sql.unwrap();
                                let sql_vec = create_sql_map.get_mut(stable_name);
                                let mut insert_done = false;
                                if sql_vec.is_some() {
                                    let tag_modify = sql_vec.unwrap();
                                    for index in 0..tag_modify.sqls.len() {
                                        let (create_sql, overflow, table_count) =
                                            tag_modify.sqls.get_mut(index).unwrap();
                                        if *overflow {
                                            continue;
                                        } else {
                                            let sql_suffix = table_sql.replace("CREATE TABLE ", "");
                                            if create_sql.len() + sql_suffix.len() > 1000 * 1000 {
                                                *overflow = true;
                                                continue;
                                            } else {
                                                create_sql.push_str(sql_suffix.as_str());
                                                insert_done = true;
                                                *table_count += 1;
                                            }
                                        }
                                    }
                                    if !insert_done {
                                        // init sql shouldn't overflow
                                        tag_modify.sqls.push((table_sql, false, 1u16));
                                    }
                                } else {
                                    let sql_vec = vec![(table_sql, false, 1u16)];
                                    let tag_modify_message = LushMessageTagModify {
                                        sqls: sql_vec,
                                        tags: table.tags().clone().unwrap(),
                                    };
                                    create_sql_map.insert(stable_name.clone(), tag_modify_message);
                                }
                            }
                        } else {
                            tracing::warn!(
                                sql = query_tags_sql,
                                error = errstr,
                                "query_tags_sql err"
                            );
                            bail!("lush message table query error: {err:#}");
                        }
                    }
                }
            }

            for (stable_name, message_modify) in create_sql_map {
                for sql in message_modify.sqls {
                    qid.add_sub_batch_id();
                    info!("Tables: {}", sql.0);
                    match taos
                        .exec_with_req_id(&sql.0, qid.get())
                        .in_current_span()
                        .await
                    {
                        Ok(_) => {
                            tracing::trace!("exec sql successfully");
                            metrics.add_created_tables(sql.2 as u64);
                        }
                        Err(err) => {
                            let err_str = format!("{err:#}");
                            tracing::warn!(sql = sql.0, error = err_str, "create table error");
                            if err_str.contains("0x2653") {
                                // column or tag length not enough
                                // 0x2653: value too long for column/tag
                                let desc = taos.describe(stable_name.as_str()).await?;
                                let fields = message_modify
                                    .tags
                                    .iter()
                                    .filter(|(_, value)| {
                                        matches!(value, Value::VarChar(_))
                                            || matches!(value, Value::NChar(_))
                                    })
                                    .map(|(tag_name, value)| match value {
                                        Value::VarChar(v) => {
                                            (tag_name.clone(), IpcDataType::VarChar(v.len() as u32))
                                        }
                                        Value::NChar(v) => {
                                            (tag_name.clone(), IpcDataType::NChar(v.len() as u32))
                                        }
                                        _ => unimplemented!(),
                                    })
                                    .collect_vec();
                                let alter_sqls = generate_alter_sql_diff_desc(
                                    &stable_name,
                                    &desc,
                                    &fields,
                                    true,
                                );
                                if alter_sqls.is_some() {
                                    for alter_sql in alter_sqls.unwrap() {
                                        qid.add_sub_batch_id();
                                        info!("lush table alter sql: {alter_sql}");
                                        taos.exec_with_req_id(alter_sql, qid.get())
                                            .in_current_span()
                                            .await
                                            .inspect(|_| {
                                                tracing::trace!("table alter successfully")
                                            })?;
                                    }
                                }
                            } else {
                                bail!("lush message table create error: {err:#}");
                            }
                        }
                    }
                }
            }
        }
        LushMessage::Insert(record) => {
            // let guard = mutex.lock().await;
            for record in record {
                if record.num_rows() == 0 {
                    continue;
                }
                *count += record.num_rows();
                metrics.add_processed_rows(record.num_rows() as u64);
                let data = record.to_column_views();
                let cols = columns.len();
                // RawBlock
                // taos.write_raw_block()
                let sqls = record.generate_insert_sql_from_tablename(&data, columns);
                if let Some((task, stable, sqls)) = task
                    .and_then(|task| record.stable_name().map(|stable| (task, stable)))
                    .and_then(|(task, stable)| sqls.as_ref().map(|(sqls, _)| (task, stable, sqls)))
                {
                    for sql in sqls {
                        if let Some(ts) = get_ts_from_sql(sql) {
                            let task_clone = task.to_string();
                            let stable_clone = stable.to_string();
                            let ts_clone = ts.clone();

                            std::thread::spawn(move || {
                                tracing::debug!(
                                    "breakpoints set start, task: {} stable: {} ts: {}",
                                    &task_clone,
                                    &stable_clone,
                                    &ts_clone
                                );
                                let res = breakpoints_set(&task_clone, &stable_clone, &ts_clone);
                                if res.is_err() {
                                    tracing::debug!(
                                        "breakpoints set error, task: {} stable: {} \n{:#?}",
                                        &task_clone,
                                        &stable_clone,
                                        res
                                    );
                                }
                            });
                            break;
                        }
                    }
                }
                if let Some((sqls, field_map)) = sqls {
                    for sql in sqls {
                        tracing::debug!("insert sql: {sql}");
                        let mut retry = 0;
                        let mut count = 0;
                        let mut break_err = Ok(());
                        loop {
                            qid.add_sub_batch_id();
                            match taos
                                .as_ref()
                                .unwrap()
                                .exec_with_req_id(&sql, qid.get())
                                .in_current_span()
                                .await
                            {
                                Ok(num) => {
                                    tracing::trace!("exec sql successfully");
                                    count += num;
                                    metrics.add_inserted_sqls(1);
                                    metrics.add_written_rows(num as u64);
                                    metrics.add_written_points((num * cols) as u64);
                                    break;
                                }
                                Err(err) => {
                                    if retry > 10 {
                                        tracing::warn!("retry write failed continue: {err:#}");
                                        metrics.add_failed_sqls(1);
                                        if break_err.is_err() {
                                            break_err?;
                                        }
                                        break;
                                    }
                                    let errstr = format!("{err:#}");
                                    tracing::error!(
                                        // sql = sql,
                                        error = errstr,
                                        "Lush stream writing error"
                                    );
                                    let code: i32 = err.code().into();
                                    match code {
                                        0xE000 | 0xE001 | 0xE002 | 0xE003 | 0xE004 | 0x000B => {
                                            // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                            // 0xE000: dsn error
                                            // 0xE001: internal error
                                            // 0xE002: connection closed
                                            // 0xE003: send timeout
                                            // 0xE004: receive timeout
                                            // 0x000B: unable to establish connection
                                            taos.replace(pool.get().await?);
                                            retry += 1;
                                        }
                                        0x2603 | 0x0618 => {
                                            // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                            // 0x2603: the table does not exist
                                            // 0x0618: the table does not exist
                                            tokio::time::sleep(Duration::from_millis(100)).await;
                                        }
                                        0x2653 => {
                                            // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                            // 0x2653: value too long for column/tag
                                            let fields = Vec::from_iter(field_map.clone());
                                            // get stable name
                                            let stable_name = record.stable_name();
                                            if stable_name.is_none() {
                                                tracing::error!(
                                        "record should contains init message for stable name"
                                    );
                                                break;
                                            }
                                            let stable_name = stable_name.unwrap();
                                            let desc = taos
                                                .as_ref()
                                                .unwrap()
                                                .describe(&stable_name)
                                                .await?;
                                            let alter_sqls = generate_alter_sql_diff_desc(
                                                &stable_name,
                                                &desc,
                                                &fields.clone(),
                                                false,
                                            );
                                            if alter_sqls.is_some() {
                                                let alter_sqls = alter_sqls.unwrap();
                                                for alter_sql in alter_sqls {
                                                    qid.add_sub_batch_id();
                                                    tracing::info!("alter sql: {alter_sql}");
                                                    if let Err(err) = taos
                                                        .as_ref()
                                                        .unwrap()
                                                        .exec_with_req_id(alter_sql, qid.get())
                                                        .in_current_span()
                                                        .await
                                                    {
                                                        tracing::warn!("alter sql error: {err:#}");
                                                    }
                                                }
                                            }
                                        }
                                        _ => {
                                            retry += 1;
                                        }
                                    }
                                    break_err = Err(err).with_context(|| {
                                        format!("lush stream error with {retry} retries")
                                    });
                                }
                            }
                        }
                        info!("written [{count}] records");
                    }
                } else {
                    error!("lush message insert sqls should not be none");
                }
            }
        }
        LushMessage::Control(_) => todo!(),
    }
    info!("consume lush record done");
    Ok(())
}

#[instrument(skip_all, name = "consume")]
async fn consume_lush_record_with_transform(
    pool: &TaosPool,
    record: LushMessage,
    count: &mut usize,
    metrics_arc: &Arc<CoreMetrics>,
    lush_model_config: Arc<LushModelConfig>,
    table_cache: Arc<TableTagCache>,
    breakpoint_db: BreakpointDb,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<()> {
    if unsafe { crate::global::DRY_RUN } {
        tracing::trace!("consume lush record in dry-run mode with transform");
        return Ok(());
    }
    match record {
        LushMessage::Control(message) => match message {
            taosx_ipc::stream::lush::LushMessageControl::DELETE(msg) => {
                let table_id = msg.table_id();
                let table_name = lush::get_table_name_from_table_id(
                    table_id,
                    table_cache.clone(),
                    lush_model_config.clone(),
                );
                if table_name.is_none() {
                    tracing::error!("Can't get table_name from table_id: {}", table_id);
                    return Ok(());
                }
                let table_name = table_name.unwrap();
                tracing::info!("Deleting data from table: {}", table_name);
                lush::delete_table_data(pool, table_name.as_str(), msg.condition.as_str()).await?;
            }
            taosx_ipc::stream::lush::LushMessageControl::ALTER(msg) => {
                let table_id = msg.table_id();
                let table_name = lush::get_table_name_from_table_id(
                    table_id,
                    table_cache.clone(),
                    lush_model_config.clone(),
                );
                if table_name.is_none() {
                    tracing::error!("Can't get table_name from table_id: {}", table_id);
                    return Ok(());
                }
                let table_name = table_name.unwrap();
                tracing::info!("Alter table: {}", table_name);
                lush::alter_table(pool, table_name.as_str(), msg.alter_table_clause.as_str())
                    .await?;
            }
            taosx_ipc::stream::lush::LushMessageControl::DROP(msg) => {
                let table_id = msg.table_id();
                let table_name = lush::get_table_name_from_table_id(
                    table_id,
                    table_cache.clone(),
                    lush_model_config.clone(),
                );
                if table_name.is_none() {
                    tracing::error!("Can't get table_name from table_id: {}", table_id);
                    return Ok(());
                }
                let table_name = table_name.unwrap();
                tracing::info!("Dropping table: {}", table_name);
                lush::drop_table(pool, table_name.as_str()).await?;
            }
            taosx_ipc::stream::lush::LushMessageControl::INSERT(msg) => {
                let table_id = msg.table_id();
                let table_name = lush::get_table_name_from_table_id(
                    table_id,
                    table_cache.clone(),
                    lush_model_config.clone(),
                );
                if table_name.is_none() {
                    tracing::error!("Can't get table_name from table_id: {}", table_id);
                    return Ok(());
                }
                let table_name = table_name.unwrap();
                tracing::info!("Insert into table: {}", table_name);
                lush::insert_into_table(pool, table_name.as_str(), msg.column_values()).await?;
            }
        },
        LushMessage::Tables(tables, full_record) => {
            tracing::debug!(
                "Got tables: {}",
                tables.iter().map(|t| t.table_name()).join(",")
            );
            // 默认超级表名(transform 前)
            let default_super_table = tables[0].stable_name();
            let default_super_table = default_super_table.unwrap().as_str();
            let super_table = lush_model_config
                .super_table_name_mapping
                .get(default_super_table);
            let super_table = super_table.ok_or_else(|| {
                anyhow!(
                    "super table {} not found in model_config.super_table_name_mapping",
                    default_super_table
                )
            })?;
            let super_table = super_table.to_owned();
            // 缓存子表 tag 值
            for table in tables {
                table_cache
                    .insert_async(table.table_name().to_owned(), table)
                    .await;
            }
            if full_record.is_none() {
                tracing::error!("Lush message tables should contains full_record");
                return Ok(());
            }
            let full_record = full_record.unwrap();
            // 获取 tranfrom::Parser
            let parser: &transform::Parser = lush_model_config
                .super_table_parsers
                .get(super_table.as_str())
                .ok_or_else(|| {
                    anyhow!(
                        "super table {} not found in model_config.super_table_parsers",
                        super_table
                    )
                })?;
            // 创建超级表
            let super_table_sql = lush_model_config
                .super_table_sqls
                .get(super_table.as_str())
                .with_context(|| {
                    format!(
                        "super table {} not found in model_config.super_table_sqls",
                        super_table
                    )
                })?;
            let mut taos = Some(pool.get().await.context("Target connection error")?);
            let _ = lush::assert_create_table(
                pool,
                &mut taos,
                super_table_sql,
                true,
                metrics_arc.ipc(),
            )
            .await;
            // transform tables 消息
            let message: transform::Message = parser
                .parse_message_from_records(&full_record, false, archive_tx.clone())
                .with_context(|| {
                    format!(
                        "failed to transform Tables message, super table: {}",
                        super_table
                    )
                })?;
            // 创建子表
            if let transform::Message::Records(tables) = message {
                lush::create_sub_tables(
                    pool,
                    &mut taos,
                    super_table.as_str(),
                    &tables,
                    metrics_arc.ipc(),
                )
                .await?;
            }
        }
        LushMessage::Insert(record) => {
            let pool = pool.clone();
            let metrics_arc = metrics_arc.clone();
            use rayon::prelude::*;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            let handle = tokio::spawn(async move {
                let mut count = 0;
                let mut set = tokio::task::JoinSet::new();
                while let Some(future) = rx.recv().await {
                    set.spawn(future);
                }
                while let Some(res) = set.join_next().await {
                    count += res.context("Transform lush record join error")??;
                }
                anyhow::Ok(count)
            });
            let span = tracing::Span::current();
            tokio::task::spawn_blocking(move || {
                record.into_par_iter().try_for_each_with(tx, |tx, record| {
                    let _enter = span.enter();
                    let num_rows = record.num_rows();
                    if num_rows == 0 {
                        tracing::debug!("No data in record");
                        return anyhow::Ok(());
                    }
                    let timer = std::time::Instant::now();
                    let name_of_table_id_column = lush_model_config.table_id_column.as_str();
                    // 只包含普通列的值
                    let values_records: &RecordBatch = record.record();
                    let table_id_column: &Arc<dyn Array> = values_records
                        .column_by_name(name_of_table_id_column)
                        .ok_or_else(|| anyhow!("table_name_column not found"))?;
                    let table_id_column: &StringArray = table_id_column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    // 只包含 tag 列的值
                    let tags_records: Result<RecordBatch, anyhow::Error> =
                        lush::create_tags_record(name_of_table_id_column, table_id_column, table_cache.clone());
                    if let Err(err) = tags_records {
                        tracing::error!("{err:#}");
                        return Ok(());
                        // continue;
                    }
                    let tags_records: RecordBatch = tags_records.unwrap();
                    // 左右合并 RecordBatch
                    let combined_records: RecordBatch = tags_records.concat_by_columns(values_records).unwrap();
                    // 类型转换
                    let parsed_records: RecordBatch = combined_records;

                    // 按超级表名分组
                    // let grouped_batches: LinkedHashMap<String, RecordBatch> =
                    //     lush::group_by_super_table_name(
                    //         &parsed_records,
                    //         name_of_table_name_column,
                    //         &lush_model_config.super_table_name_mapping,
                    //     );

                    // 性能优化，多列模型无需按超级表分组
                    // let grouped_batches = lush::group_by_super_table_name2(&parsed_records);
                    let prepare_elapsed = timer.elapsed();
                    let skip_null = lush_model_config.skip_null;
                    // for (default_super_table, record_batch) in grouped_batches {
                    let timer = std::time::Instant::now();
                    let record_batch = parsed_records;
                    let default_super_table = record_batch
                        .column_by_name("_using")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0);
                    let super_table = lush_model_config
                        .super_table_name_mapping
                        .get(default_super_table);
                    if super_table.is_none() {
                        tracing::error!(
                        "default_super_table {} not found in super_table_name_mapping",
                        default_super_table
                    );
                        return Ok(());
                        // continue;
                    }
                    let super_table = super_table.unwrap();
                    let parser: &transform::Parser = lush_model_config
                        .super_table_parsers
                        .get(super_table.as_str())
                        .ok_or_else(|| {
                            anyhow!(
                            "super table {} not found in model_config.super_table_parsers",
                            super_table
                        )
                        })?;
                    let message: transform::Message =
                        parser.parse_message_from_records(&record_batch, true, archive_tx.clone()).with_context(|| {
                            format!("transform failed for super table: {}", super_table)
                        })?;

                    let transform_elapsed = timer.elapsed();
                    if let crate::plugins::transform::Message::Records(message) = message {
                        if message.is_empty() {
                            return Ok(());
                        }
                        let table_count = message.len();
                        let pool = pool.clone();
                        let super_table = super_table.clone();
                        let metrics_ref = metrics_arc.clone();
                        let breakpoints = breakpoint_db.clone();
                        let table_id_column_name = name_of_table_id_column.to_string();
                        let parser = parser.clone();
                        let archive_tx = archive_tx.clone();
                        if let Err(err) = tx.send(async move {
                            let metrics = metrics_ref.ipc();
                            lush::write(
                                &pool,
                                super_table.as_str(),
                                taos::Precision::Millisecond,
                                message,
                                metrics,
                                skip_null,
                                table_id_column_name.as_str(),
                                breakpoints,
                                &parser,
                                archive_tx.clone(),
                            ).in_current_span().await.map(|(written_rows, gen_sql_time, write_time)| {
                                tracing::info!(
                            "stable,{},tables,{},rows,{},prepare_elapsed,{},transform_elapsed,{},gensql_elapsed,{},write_elapsed,{}",
                            super_table,
                            table_count,
                            written_rows,
                            prepare_elapsed.as_millis(),
                            transform_elapsed.as_millis(),
                            gen_sql_time.as_millis(),
                            write_time.as_millis(),
                        );
                                metrics.add_processed_rows(num_rows as u64);
                                num_rows
                            })
                        }.in_current_span()) {
                            tracing::error!("send to tx error: {err:#}");
                            bail!("Send future error: {err:#}");
                        }
                    }
                    anyhow::Ok(())
                })
            }).await.context("Spawn blocking transform lush records inserts")??;

            *count += handle.await??;
        }
    }
    Ok(())
}

fn get_ts_from_sql(sql: &str) -> Option<String> {
    let re = regex::Regex::new(r"VALUES \((\d+),[^,]*\)").unwrap();

    if let Some(caps) = re.captures(sql) {
        if let Some(value) = caps.get(1) {
            return Some(value.as_str().to_string());
        }
    }

    None
}

/********** handle Point Message START **********/
/// PointMessage 的初始化：如果 table_config_map 中的 enabled 为 0，则删除对应的表
#[instrument(skip_all)]
pub async fn handle_point_message_init(config: &OpcModelConfig, taos: &Taos) -> anyhow::Result<()> {
    let point_config_map = &config.point_config_map;
    let table_config_map = &config.table_config_map;

    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    for point_id in point_config_map.keys() {
        let table_config = table_config_map.get(point_id).ok_or(anyhow::anyhow!(
            "point_id: {} not exist in table config map",
            point_id
        ))?;
        if table_config.enabled == Some(0i8) {
            let tbname = point_config_map
                .get(point_id)
                .ok_or(anyhow::anyhow!(
                    "point_id: {} not exist in point config map",
                    point_id
                ))?
                .code
                .clone();
            let drop_sql = format!("DROP TABLE IF EXISTS `{}`", tbname);
            qid.add_sub_batch_id();
            tracing::info!("drop table sql: {drop_sql}");
            taos.exec_with_req_id(&drop_sql, qid.get())
                .in_current_span()
                .await
                .with_context(|| format!("failed to drop table: {}", tbname))?;
        }
    }

    Ok(())
}

/// 处理 PointMessage
#[instrument(skip_all, fields(target_precision = ?target_precision))]
async fn consume_point_record(
    pool: &TaosPool,
    taos: &mut Option<deadpool::managed::Object<Manager<TaosBuilder>>>,
    record: &PointMessage,
    count: &mut usize,
    config: &OpcModelConfig,
    target_precision: taos::Precision,
    metrics: &IpcMetrics,
) -> anyhow::Result<usize> {
    if unsafe { crate::global::DRY_RUN } {
        tracing::trace!("consume point record in dry-run mode");
        return Ok(0);
    }
    tracing::trace!("consume point record, opc model config: {:?}", config);

    let mut points = 0;
    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    for message in record.records() {
        let message = handle_transform(message, config).await?;

        let message_rows = message.record().num_rows() as u64;

        let (stable_insert_map, child_table_create_sql_map) =
            point_records_to_sql(message, config, target_precision).await?;

        for (stable_name, sql_vec) in stable_insert_map {
            for sql_insertion in sql_vec {
                tracing::debug!("point message insert sql len: {}", sql_insertion.sql.len());

                let mut retry = 0;
                let mut break_err = Ok(());
                'outer: loop {
                    if retry >= 5 {
                        tracing::error!(error = ?break_err, "sql error cannot be solved, sql: {};", sql_insertion.sql);
                        metrics.add_failed_sqls(1);
                        if break_err.is_err() {
                            break_err.context("Point message sql error")?;
                        }
                        break 'outer;
                    }
                    qid.add_sub_batch_id();
                    let sql_res = taos
                        .as_ref()
                        .unwrap()
                        .exec_with_req_id(&sql_insertion.sql, qid.get())
                        .in_current_span()
                        .await;

                    match sql_res {
                        Ok(n) => {
                            tracing::trace!("exec sql successfully");
                            *count += n;
                            metrics.add_inserted_sqls(1);
                            metrics.add_written_rows(n as u64);
                            // metrics.add_written_points(
                            //     (n * column_insert.columns_insert.len()) as u64,
                            // );
                            // TODO: points is wrong
                            metrics.add_written_points(n as u64);
                            points += n;
                            break 'outer;
                        }
                        Err(err) => {
                            let errstr = format!("{err:#}");
                            tracing::warn!(
                                sql = sql_insertion.sql,
                                error = errstr,
                                "Insert point record error",
                            );

                            if errstr.contains("[0x2603]") || errstr.contains("0x0200") {
                                // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                // 0x2603: the table does not exist
                                // 0x0200: stmt bind param does not support normal value in sql
                                let value_column_config =
                                    &sql_insertion.point_insertion.value_column_config;
                                let value_column_name = value_column_config.name.clone();
                                let value_column_alias = value_column_config
                                    .alias
                                    .clone()
                                    .unwrap_or(value_column_name.clone());
                                let value_column_type = sql_insertion.value_column_type.clone();
                                let mut temp_conlumns =
                                    sql_insertion.point_insertion.other_columns.clone();
                                let value_col =
                                    format!(",`{}` {}", value_column_alias, value_column_type);
                                temp_conlumns.push_str(value_col.as_str());

                                let tags = sql_insertion.point_insertion.tags.clone();
                                let stable_sql = format!(
                                    "CREATE STABLE `{}` ({}) tags ({})",
                                    stable_name, temp_conlumns, tags
                                );
                                qid.add_sub_batch_id();
                                tracing::info!("create stable sql: {}", &stable_sql);
                                match taos
                                    .as_ref()
                                    .unwrap()
                                    .exec_with_req_id(&stable_sql, qid.get())
                                    .in_current_span()
                                    .await
                                {
                                    Ok(_) => {
                                        tracing::trace!("exec sql successfully");
                                        metrics.add_created_stables(1);
                                    }
                                    Err(err) => {
                                        let code: i32 = err.code().into();
                                        if matches!(
                                            code,
                                            0x0360 | 0x032C | 0x0115 | 0x0603 | 0x03C7 | 0x03D3
                                        ) {
                                            // 0x0360: stable already exists
                                            // 0x032C: Object is creating
                                            // 0x0115: invalid msg
                                            // 0x0603: table already exists
                                            // 0x03C7: stable uid not match
                                            // 0x03D3: Conflict transaction not completed
                                            tracing::debug!("error encountered, ignore: {err:#}",);
                                        } else {
                                            tracing::warn!(
                                                "create stable {stable_name} error: {err:#}"
                                            );
                                            let err_str = err.to_string();
                                            if err_str.contains("0xE00") {
                                                // 0xE00: connection error
                                                taos.replace(pool.get().await?);
                                                break_err = Err(err);
                                            } else {
                                                tracing::error!("create stable sql error: {err:#}");
                                            }
                                            retry += 1;
                                            continue 'outer;
                                        }
                                    }
                                }

                                // 创建子表
                                let mut child_table_create_sqls = Vec::new();
                                let mut child_table_counts_vec = Vec::<u32>::new();
                                let mut sql_prefix = "CREATE TABLE".to_string();
                                let mut child_table_count = 0u32;
                                let child_table_create_sql_map =
                                    child_table_create_sql_map.get(&stable_name).unwrap();
                                for (child_table_name, child_table_create_sql) in
                                    child_table_create_sql_map
                                {
                                    let suffix_sql = format!(" `{child_table_name}` USING `{stable_name}` {child_table_create_sql}");
                                    if sql_prefix.len() + suffix_sql.len() > 1024 * 1024 {
                                        child_table_create_sqls.push(sql_prefix);
                                        sql_prefix = "CREATE TABLE".to_string();
                                        child_table_counts_vec.push(child_table_count);
                                        child_table_count = 0;
                                    }
                                    sql_prefix.push_str(&suffix_sql);
                                    child_table_count += 1;
                                }
                                child_table_create_sqls.push(sql_prefix);
                                child_table_counts_vec.push(child_table_count);
                                for (create_child_sql, child_table_count) in
                                    zip(child_table_create_sqls, child_table_counts_vec)
                                {
                                    qid.add_sub_batch_id();
                                    tracing::debug!("create child sql: {create_child_sql}");
                                    match taos
                                        .as_ref()
                                        .unwrap()
                                        .exec_with_req_id(&create_child_sql, qid.get())
                                        .in_current_span()
                                        .await
                                    {
                                        // match taos.as_ref().unwrap().exec(&create_child_sql).await {
                                        Ok(_n) => {
                                            tracing::trace!("exec sql successfully");
                                            metrics.add_created_tables(child_table_count as u64);
                                        }
                                        Err(err) => {
                                            tracing::warn!("create child table error: {err:#}");
                                            let err_str = err.to_string();
                                            if err.to_string().contains("0x032C") {
                                                // 0x032C: Object is creating
                                                // Object is creating, maybe should ignore
                                                tracing::warn!("create table sql encounter 0x032C");
                                            } else if err_str.contains("0xE00") {
                                                // 0xE00: connection error
                                                taos.replace(pool.get().await?);
                                                break_err = Err(err);
                                            } else {
                                                tracing::error!(
                                                    sql = create_child_sql,
                                                    "create table sql error: {err:#}"
                                                );
                                            }
                                            retry += 1;
                                            continue 'outer;
                                        }
                                    }
                                }
                            } else if errstr.contains("[0x2602]") || errstr.contains("[0x263F]") {
                                // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                // 0x2602: invalid column
                                // 0x263F: invalid columns number
                                for column_config in &sql_insertion.point_insertion.column_configs {
                                    // alter stable column not supported by taosd
                                    let desc = taos.as_ref().unwrap().describe(&stable_name).await;
                                    let desc = match desc {
                                        Ok(desc) => desc,
                                        Err(err) => {
                                            tracing::warn!("describe error: {err:#}");
                                            let code: i32 = err.code().into();
                                            let _err_str = err.to_string();
                                            match code {
                                                0xE000..=0xE004 => {
                                                    // 0xE000: dsn error
                                                    // 0xE001: internal error
                                                    // 0xE002: connection closed
                                                    // 0xE003: send timeout
                                                    // 0xE004: receive timeout
                                                    taos.replace(pool.get().await?);
                                                    break_err = Err(err);
                                                    retry += 1;
                                                    continue 'outer;
                                                }
                                                _ => {
                                                    tracing::error!(
                                                        "describe table {stable_name} error: {err:#}"
                                                    );
                                                    return Err(err.context("describe error"))?;
                                                }
                                            }
                                        }
                                    };
                                    // 增加 column
                                    let column_real_name =
                                        column_config.alias.as_ref().unwrap_or(&column_config.name);
                                    let need_add = desc
                                        .into_iter()
                                        .all(|column_meta| column_real_name != column_meta.field());
                                    if need_add {
                                        if column_config.r#type.is_none() {
                                            // shouldn't happen if normal, encounter when rename value column
                                            tracing::error!("column {} column_type is error, maybe stable set error", column_real_name);
                                            break 'outer;
                                        }
                                        let add_column_sql = format!(
                                            "alter table `{stable_name}` ADD COLUMN {} {}",
                                            column_real_name,
                                            column_config.r#type.unwrap()
                                        );
                                        qid.add_sub_batch_id();
                                        tracing::info!("add_column_sql: {}", add_column_sql);
                                        let res = taos
                                            .as_ref()
                                            .unwrap()
                                            .exec_with_req_id(&add_column_sql, qid.get())
                                            .in_current_span()
                                            .await;
                                        if let Err(err) = res {
                                            tracing::warn!("describe error: {err:#}");
                                            let code: i32 = err.code().into();
                                            let _err_str = err.to_string();
                                            match code {
                                                0x032C => {
                                                    // 0x032C: Object is creating
                                                    tracing::warn!(
                                                        "create table sql encounter 0x032C"
                                                    );
                                                }
                                                0xE000..=0xE004 => {
                                                    // 0xE000: dsn error
                                                    // 0xE001: internal error
                                                    // 0xE002: connection closed
                                                    // 0xE003: send timeout
                                                    // 0xE004: receive timeout
                                                    taos.replace(pool.get().await?);
                                                    break_err = Err(err);
                                                    continue 'outer;
                                                }
                                                _ => {
                                                    tracing::error!(
                                                        sql = add_column_sql,
                                                        "create table sql error: {err:#}"
                                                    );
                                                    Err(err)?;
                                                }
                                            }
                                        }
                                    }
                                }
                                // 增加 tag
                                if let Some(tag_configs) =
                                    &sql_insertion.point_insertion.tag_configs
                                {
                                    let desc =
                                        taos.as_ref().unwrap().describe(&stable_name).await?;
                                    let fields = tag_configs
                                        .iter()
                                        .map(|config| (config.name.clone(), config.r#type.clone()))
                                        .collect_vec();
                                    let sqls = generate_alter_sql_diff_desc(
                                        &stable_name,
                                        &desc,
                                        &fields,
                                        true,
                                    );
                                    if sqls.is_some() {
                                        let sqls = sqls.unwrap();
                                        for alter_sql in sqls {
                                            qid.add_sub_batch_id();
                                            tracing::info!("alter table sql: {alter_sql}");
                                            match taos
                                                .as_ref()
                                                .unwrap()
                                                .exec_with_req_id(&alter_sql, qid.get())
                                                .in_current_span()
                                                .await
                                            {
                                                // match taos.as_ref().unwrap().exec(alter_sql).await {
                                                Ok(_) => {
                                                    tracing::trace!("exec sql successfully");
                                                }
                                                Err(err) => {
                                                    if err.to_string().contains("0x0369") {
                                                        // 0x0369: Tag already exists
                                                        // Tag already exists occur when concurrent exec same alter
                                                        tracing::warn!(
                                                            "alter table err: {}, will be ignored",
                                                            err.to_string()
                                                        );
                                                    } else {
                                                        tracing::warn!(
                                                            sql = alter_sql,
                                                            "alter table err: {}",
                                                            err.to_string()
                                                        );
                                                        retry += 1;
                                                        break 'outer;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                retry += 1;
                                continue 'outer;
                            } else if errstr.contains("[0x2653]") {
                                // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                // 0x2653: value too long for column/tag
                                let desc = taos
                                    .as_ref()
                                    .unwrap()
                                    .describe(stable_name.as_str())
                                    .await?;
                                let mut tags_for_diff = Vec::new();
                                tags_for_diff.push((
                                    "point_id".to_string(),
                                    IpcDataType::from_str(
                                        format!("varchar({})", sql_insertion.modify.id.len())
                                            .as_str(),
                                    )
                                    .unwrap(),
                                ));
                                tags_for_diff.push((
                                    "point_name".to_string(),
                                    IpcDataType::from_str(
                                        format!(
                                            "varchar({})",
                                            sql_insertion.modify.point_name.len()
                                        )
                                        .as_str(),
                                    )
                                    .unwrap(),
                                ));

                                if let Some(tag_configs) =
                                    &sql_insertion.point_insertion.tag_configs
                                {
                                    for tag_conf in tag_configs {
                                        tags_for_diff
                                            .push((tag_conf.name.clone(), tag_conf.r#type.clone()));
                                    }
                                }

                                let sqls = generate_alter_sql_diff_desc(
                                    &stable_name,
                                    &desc,
                                    &tags_for_diff,
                                    true,
                                );
                                if sqls.is_some() {
                                    let sqls = sqls.unwrap();
                                    for sql in sqls {
                                        qid.add_sub_batch_id();
                                        taos.as_ref()
                                            .unwrap()
                                            .exec_with_req_id(sql, qid.get())
                                            .in_current_span()
                                            .await
                                            .context("Writing point stream error")?;
                                    }
                                }
                                for column_meta in desc {
                                    if column_meta.ty().is_var_type()
                                        && column_meta.field()
                                            == sql_insertion.modify.value_column_name
                                        && sql_insertion.modify.value_column_length
                                            > column_meta.length()
                                    {
                                        let sql = format!(
                                            "alter table `{stable_name}` modify column `{}` {}({})",
                                            column_meta.field(),
                                            column_meta.ty(),
                                            sql_insertion.modify.value_column_length,
                                        );
                                        qid.add_sub_batch_id();
                                        tracing::info!("add execute sql: {}", &sql);
                                        let _ = taos.as_ref().unwrap().exec_with_req_id(&sql, qid.get()).in_current_span()
                                            .await
                                            .context(
                                                "Modify column length error while writing point stream",
                                            );
                                    }
                                }
                                retry += 1;
                                continue 'outer;
                            } else if errstr.contains("[0xE000]")
                                || errstr.contains("[0xE001]")
                                || errstr.contains("[0xE002]")
                                || errstr.contains("[0xE003]")
                                || errstr.contains("[0xE004]")
                                || errstr.contains("[0x000B]")
                            {
                                // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                // 0xE000: dsn error
                                // 0xE001: internal error
                                // 0xE002: connection closed
                                // 0xE003: send timeout
                                // 0xE004: receive timeout
                                // 0x000B: unable to establish connection
                                taos.replace(pool.get().await?);
                                retry += 1;
                                continue 'outer;
                            } else {
                                metrics.add_failed_sqls(1);
                                Err(err)?;
                                break 'outer;
                            }
                            break_err = Err(err);
                        }
                    }
                }

                tracing::trace!(retry, "Insert point record success");
            }
        }

        metrics.add_processed_rows(message_rows);
    }
    Ok(points)
}

/********** handle Point Message END **********/

const DEFAULT_MAX_RETRIES_FOR_CONNECTION: u32 = 5;

/// Write flat message to TDengine.
///
/// # Arguments
///
/// - `count` will be increased by the number of rows written. Note that the number of rows written may be less than the number of rows in the message.
#[framed]
#[instrument(skip_all, fields(writer.count = count))]
async fn consume_flat_record(
    pool: &TaosPool,
    taos: &mut Option<deadpool::managed::Object<Manager<TaosBuilder>>>,
    batch: &RecordBatch,
    count: &mut usize,
    cancel: &CancellationToken,
    parser: &Parser,
    target_precision: taos::Precision,
    metrics: &IpcMetrics,
    notifier: Option<&crate::TaskNotifySender>,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<()> {
    macro_rules! metrics_failed {
        ($rows:expr, $cols:expr) => {
            metrics.add_failed_sqls(1_u64);
            metrics.add_failed_rows($rows as u64);
            metrics.add_failed_points(($rows * $cols) as u64);
        };
    }

    if unsafe { crate::global::DRY_RUN } {
        metrics.add_processed_rows(batch.num_rows() as u64);
        return Ok(());
    }

    if cancel.is_cancelled() {
        tracing::warn!("Task is cancelled");
        return Ok(());
    }

    let timeout = parser
        .global()
        .process_on_abnormal
        .connection_timeout_in_second_value as u32;
    let retry_start = Utc::now();
    loop {
        if taos.is_none() {
            match pool.get().await {
                Ok(new_taos) => {
                    taos.replace(new_taos);
                }
                Err(e) => {
                    tracing::debug!("get taos connection from pool error: {e:?}");
                    let sleep = ((timeout * 1000) / DEFAULT_MAX_RETRIES_FOR_CONNECTION) as u64;
                    let pool_status = pool.status();
                    if pool_status.available == 0
                        && matches!(e, PoolError::Timeout(TimeoutType::Wait))
                    {
                        let new_size = pool_status.max_size + parser.global().concurrent_limit();
                        pool.resize(new_size);
                        tracing::warn!(new_size, "connection pool resized");
                        tokio::time::sleep(Duration::from_millis(sleep)).await;
                        continue;
                    }

                    if let PoolError::Backend(e) = e {
                        let errno: i32 = e.code().into();
                        // 0x0388: Database not exist
                        if errno == 0x0388 {
                            tracing::debug!(
                                "database not exist, handle abnormal strategy:{:?}",
                                parser.global().process_on_abnormal.database_not_exist,
                            );
                            metrics_failed!(batch.num_rows(), batch.num_columns());
                            handle_flat_abnormal(
                                ProcessOnAbnormalEnum::DatabaseNotExist(
                                    &parser.global().process_on_abnormal.database_not_exist,
                                ),
                                batch,
                                archive_tx.clone(),
                            )?;
                            tokio::time::sleep(Duration::from_millis(sleep)).await;
                            continue;
                        }
                    }

                    if Utc::now() - retry_start > TimeDelta::seconds(timeout as i64) {
                        tracing::debug!(
                            "handle database connection abnormal, strategy:{:?}",
                            parser
                                .global()
                                .process_on_abnormal
                                .database_connection_error,
                        );
                        handle_flat_abnormal(
                            ProcessOnAbnormalEnum::DatabaseConnectionError(
                                &parser
                                    .global()
                                    .process_on_abnormal
                                    .database_connection_error,
                            ),
                            batch,
                            archive_tx.clone(),
                        )?;
                    }
                    tokio::time::sleep(Duration::from_millis(sleep)).await;
                }
            }
        } else {
            break;
        }
    }
    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    let mut max_lengths = HashMap::new();
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(());
    }

    let instant = std::time::Instant::now();
    let batch = tokio::task::spawn_blocking({
        let parser = parser.clone();
        let batch = batch.clone();
        let archive_tx = archive_tx.clone();
        move || parser.parse_message_from_records(&batch, true, archive_tx)
    })
    .await?
    .context("Transformer parse error")?;

    if tracing::event_enabled!(tracing::Level::TRACE) {
        let elapsed = instant.elapsed();
        tracing::trace!(cost = ?elapsed, "Parse message elapsed: {:?}", elapsed);
    }

    match batch {
        transform::Message::Records(mut message) => {
            if message.is_empty() {
                return Ok(());
            }

            let write_ready_rows = message
                .iter()
                .map(|message| message.records.num_rows())
                .sum::<usize>();
            let factor = write_ready_rows / message.len();
            let res = if factor < 200 {
                flat_write_with_sql(
                    pool,
                    taos,
                    target_precision,
                    &message,
                    metrics,
                    notifier,
                    cancel,
                    parser.global(),
                    archive_tx.clone(),
                )
                .in_current_span()
                .await
            } else {
                flat_write_with_raw_block(
                    pool,
                    taos,
                    &mut max_lengths,
                    parser,
                    target_precision,
                    &message,
                    metrics,
                    notifier,
                    cancel,
                    parser.global(),
                    archive_tx.clone(),
                )
                .in_current_span()
                .await
            };
            match res {
                Ok(n) => {
                    *count += n;
                    metrics.add_processed_rows(write_ready_rows as u64);
                }
                Err(err) => {
                    let errstr = format!("{:#}", err);
                    if errstr.contains("Timestamp data out of range") {
                        qid.add_sub_batch_id();
                        tracing::warn!("Contains invalid timestamp, filter out them");
                        // filter timestamp.
                        let (_, min) = get_minimum_timestamp(
                            pool,
                            taos,
                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                            cancel,
                        )
                        .in_current_span()
                        .await?;
                        tracing::debug!("Minimus timestamp: {}", min.to_rfc3339());
                        let rows: usize = message.iter().map(|m| m.records.num_rows()).sum();
                        message = message
                            .into_iter()
                            .flat_map(|item| item.filter_by_primary_timestamp(&min))
                            .collect();

                        let rows_after: usize = message.iter().map(|m| m.records.num_rows()).sum();

                        let filtered = rows - rows_after;
                        tracing::info!(rows, filtered, after = rows_after, "Filter out records");
                        metrics.add_drained_rows(filtered as _);

                        if message.is_empty() {
                            return Ok(());
                        }
                        let factor = message
                            .iter()
                            .map(|message| message.records.num_rows())
                            .sum::<usize>()
                            / message.len();
                        let n = if factor < 200 {
                            flat_write_with_sql(
                                pool,
                                taos,
                                target_precision,
                                &message,
                                metrics,
                                notifier,
                                cancel,
                                parser.global(),
                                archive_tx.clone(),
                            )
                            .in_current_span()
                            .await
                        } else {
                            flat_write_with_raw_block(
                                pool,
                                taos,
                                &mut max_lengths,
                                parser,
                                target_precision,
                                &message,
                                metrics,
                                notifier,
                                cancel,
                                parser.global(),
                                archive_tx.clone(),
                            )
                            .in_current_span()
                            .await
                        }?;
                        *count += n;
                        metrics.add_processed_rows(num_rows as u64);
                    } else {
                        metrics.add_failed_rows(write_ready_rows as u64);
                        return Err(err);
                    }
                }
            }
        }
        _ => unimplemented!(),
    }
    Ok(())
}

fn handle_flat_abnormal<'a>(
    abnormal_stragy: ProcessOnAbnormalEnum<'a>,
    batch: &RecordBatch,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<()> {
    match abnormal_stragy {
        ProcessOnAbnormalEnum::DatabaseConnectionError(database_connection_error) => {
            match database_connection_error
                .handle("get taos connection from pool error".to_string())
            {
                Ok((HandlingResult::Skip, _)) => Ok(()),
                Ok((HandlingResult::Archive, err)) => {
                    if let Err(e) = process_archive(&err, batch, archive_tx.clone()) {
                        tracing::error!("archive error: {e:#}");
                    }
                    Ok(())
                }
                Ok((HandlingResult::Modify(_), _)) => unreachable!(),
                Ok((HandlingResult::ModifyAndArchive(_), _)) => unreachable!(),
                Ok((HandlingResult::Retry, _)) => {
                    if let Err(e) = process_cache(batch, archive_tx.clone()) {
                        tracing::error!("cache error: {e:#}");
                    }
                    Ok(())
                }
                Err(e) => Err(e).context("get taos connection from pool error")?,
            }
        }
        ProcessOnAbnormalEnum::DatabaseNotExist(handling_strategy) => {
            match handling_strategy.handle("Database not exist".to_string()) {
                Ok((HandlingResult::Archive, err)) => {
                    if let Err(e) = process_archive(&err, batch, archive_tx.clone()) {
                        tracing::error!("archive error: {e:#}");
                    }
                    Ok(())
                }
                Ok((HandlingResult::Skip, err)) => {
                    tracing::warn!("skip info: {err:#}");
                    Ok(())
                }
                Ok((_, _)) => unreachable!(),
                Err(e) => Err(e).context("get taos connection from pool error")?,
            }
        }
    }
}

fn process_cache(batch: &RecordBatch, archive_tx: Sender<ArchiveType>) -> anyhow::Result<()> {
    if batch.num_rows() > 0 {
        archive_tx
            .send(ArchiveType::Cache(batch.clone()))
            .context("archive process task exit")?;
    }
    Ok(())
}

fn process_archive(
    err: &str,
    batch: &RecordBatch,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<()> {
    // possible difference in schema, so archive them separately
    let err_vec = vec![err.to_string(); batch.num_rows()];
    let err_timestamp_vec = vec![Utc::now().timestamp_nanos_opt().unwrap(); batch.num_rows()];
    archive_records(
        batch,
        err_vec.clone(),
        err_timestamp_vec.clone(),
        archive_tx.clone(),
    )
}

#[instrument(skip_all)]
async fn ipc_lush_stream_reader<R: Read + Send + 'static, W: Write>(
    pool: &TaosPool,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
    lush_model_config: Option<LushModelConfig>,
    task_id: Option<i64>,
    notifier: crate::TaskNotifySender,
    ipc_error_strategy: IpcErrorStrategy,
    metrics: &IpcMetrics,
    metrics_arc: &Arc<CoreMetrics>,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<()> {
    // let taos = pool.get().await?;
    let columns = ipc_reader
        .columns()
        .into_iter()
        .map(|s| s.to_string())
        .collect_vec();

    let mut count = 0;
    let mut stream = ipc_reader.into_stream();

    let acks: AtomicUsize = AtomicUsize::new(0);
    let lush_model_config = lush_model_config.map(Arc::new);

    // TODO: 使用 scheduler 中的 lush_table_cache
    let lush_table_cache = Arc::new(TableTagCache::new());
    // 暂不支持无 agent 运行 pi 和 pibackfill
    let breakpoint_db: Option<BreakpointDb> = None;

    // let qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    // let mut taos = Some(taos);
    while let Some(record) = stream.try_next().await.context("next item error")? {
        let raw_rows = record.nrows();
        metrics.add_received_batches(1);
        metrics.add_received_messages(raw_rows as u64);
        let taos = pool.get().await?;
        let mut taos = Some(taos);
        info!("Writing batch");
        let record = *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
            std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
        })
        .unwrap();
        let last = count;
        let result = if let Some(lush_model_config) = lush_model_config.clone() {
            consume_lush_record_with_transform(
                pool,
                record,
                &mut count,
                metrics_arc,
                lush_model_config,
                lush_table_cache.clone(),
                breakpoint_db.as_ref().unwrap().clone(),
                archive_tx.clone(),
            )
            .await
        } else {
            consume_lush_record(
                pool, &mut taos, record, &columns, &mut count, task_id, metrics,
            )
            .await
        };
        if let Err(err) = result {
            metrics.add_failed_batches(1);
            tracing::error!("Writing batch error: {err:#}");
            let written = count - last;
            if ipc_error_strategy.will_stop() {
                bail!("write batch error: {err:#}");
            }
            let _ = ipc_ack_writer.ack(LushAck {
                code: 0,
                message: Some(err.to_string()),
                context: Some(
                    json!({
                        "stream": "lush",
                        "written":  written,
                    })
                    .to_string(),
                ),
            });

            if notifier
                .send(crate::TaskNotify::sink_error(format!("{:#}", err)))
                .is_err()
            {
                bail!("write batch error: {err:#}");
            }
        } else {
            tracing::info!("ack");
            let _ = ipc_ack_writer
                .ack(LushAck {
                    code: 0,
                    message: None,
                    context: Some(
                        json!({
                            "stream": "lush",
                            "written":  count - last,
                        })
                        .to_string(),
                    ),
                })
                .context("write ack error");
            tracing::debug!(acks = acks.load(Ordering::SeqCst), "ack done");
        }
        acks.fetch_add(1, Ordering::SeqCst);
        metrics.add_processed_batches(1);
        metrics.add_processed_messages(raw_rows as u64);
        drop(taos);
    }
    println!("finished, totally {count} rows");
    Ok(())
}

#[instrument(skip_all)]
async fn ipc_point_reader<R: Read + Send + 'static, W: Write + Send + 'static>(
    pool: &TaosPool,
    ipc_reader: IpcReader<R>,
    mut ipc_ack_writer: AckWriter<W>,
    config: Option<OpcModelConfig>,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    _ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
    persist_component: Option<PersistComponent>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let count = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct WriterContext {
        pool: TaosPool,
        config: Option<Arc<OpcModelConfig>>,
        target_precision: taos::Precision,
    }

    async fn parse(
        context: WriterContext,
        record: Result<RecordBatch, arrow::error::ArrowError>,
        metrics: &IpcMetrics,
    ) -> anyhow::Result<usize> {
        let record = record?;
        let pool = &context.pool;
        let taos = context.pool.get().await?;
        let mut count = 0;
        let mut taos = Some(taos);
        let record = PointMessage::new(vec![record.into()]);
        let raw_rows = record.nrows();
        metrics.add_received_messages(raw_rows as u64);
        let n = consume_point_record(
            pool,
            &mut taos,
            &record,
            &mut count,
            context.config.as_ref().unwrap(),
            context.target_precision,
            metrics,
        )
        .await;
        metrics.add_processed_messages(raw_rows as u64);
        n
    }

    let context = WriterContext {
        pool: pool.clone(),
        config: config.map(Arc::new),
        target_precision,
    };

    let (ack_tx, ack_rx) = flume::bounded(1);
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn_blocking(move || {
        for ack in ack_rx {
            let _ = ipc_ack_writer.ack(ack);
        }
        anyhow::Ok(())
    });
    let qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    let stream = ipc_reader.into_raw_stream_with_capycity(100);
    let (stream, ack_tx) = match persist_component {
        None => (Either::Left(stream.map(|v| (v, None))), Some(ack_tx)),
        Some(component) => {
            let metrics = component
                .config
                .record_metrics
                .then_some(metrics_arc.clone());
            let persist_rx = get_stream(
                component, stream, ack_tx, &cancel, metrics, &mut tasks, true,
            )?;
            (Either::Right(persist_rx.into_stream()), None)
        }
    };
    stream
        .for_each_concurrent(48, |(record, ack_wait_tx)| {
            let context = context.clone();
            let count = count.clone();
            let notifier = notifier.clone();
            let batch_counter = batch_counter.clone();
            let mut qid = qid.clone();
            let ack_tx = ack_tx.clone();
            debug!("Writing batch");
            let metrics_arc_clone = metrics_arc.clone();
            async move {
                if let Some(batch_counter) = batch_counter {
                    let batch_number = batch_counter.next().await.unwrap_or_default();
                    qid.set_batch_id(batch_number);
                }
                let metrics = metrics_arc_clone.ipc();
                metrics.add_received_batches(1);
                let meta = {
                    record
                        .as_ref()
                        .ok()
                        .map(|b| {
                            serde_json::Map::from_iter(
                                b.schema_ref()
                                    .metadata()
                                    .iter()
                                    .map(|(k, v)| (k.clone(), json!(v))),
                            )
                        })
                        .map(serde_json::Value::from)
                };
                let n = parse(context, record, metrics).await;
                match n {
                    Ok(n) => {
                        let mut ack = LushAck::ok();
                        ack.context = meta.map(|m| m.to_string());
                        metrics.add_processed_batches(1);
                        if let Some(ack_tx) = ack_tx {
                            let _ = ack_tx.send_async(ack).await;
                        } else if let Some(ack_tx) = ack_wait_tx {
                            let _ = ack_tx.send(ack);
                        }
                        count.fetch_add(n, Ordering::SeqCst);
                    }
                    Err(err) => {
                        metrics.add_failed_batches(1);
                        tracing::warn!("Writing batch error: {err:#}");
                        let _ = notifier.send(crate::TaskNotify::sink_error(format!("{:#}", err)));
                        let ack = LushAck {
                            code: 0,
                            message: Some(err.to_string()),
                            context: meta.map(|m| m.to_string()),
                        };
                        if let Some(ack_tx) = ack_tx {
                            let _ = ack_tx.send_async(ack).await;
                        } else if let Some(ack_tx) = ack_wait_tx {
                            let _ = ack_tx.send(ack);
                        }
                    }
                }
            }
            .in_current_span()
        })
        .await;
    while let Some(task) = tasks.join_next().await {
        task.context("Point stream task paniced")?
            .context("Point stream worker error")?;
    }
    println!(
        "IPC stream finished, total {} records in this stream",
        count.load(Ordering::SeqCst)
    );
    Ok(())
}

#[framed]
async fn ipc_flat_stream_worker(
    pool: &TaosPool,
    stream: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
    sink: impl Sink<LushAck, Error = ArrowError> + Send + 'static,
    cancel: CancellationToken,
    parser: Option<&Parser>,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
    archive_tx: Sender<ArchiveType>,
    persist_component: Option<PersistComponent>,
) -> anyhow::Result<()> {
    let parser = parser.ok_or_else(|| anyhow::anyhow!("Parser should be set with flat stream"))?;
    // tokio::pin!(stream);

    match parser.global().written_method() {
        WrittenMethod::Concurrent => {
            return ipc_flat_stream_worker_concurrent(
                pool,
                stream,
                sink,
                cancel,
                parser,
                target_precision,
                notifier,
                ipc_error_strategy,
                metrics_arc,
                batch_counter,
                archive_tx.clone(),
                persist_component,
            )
            .await;
        }
        WrittenMethod::VgroupConcurrent => {
            return ipc_flat_stream_worker_vgroup(
                pool,
                stream,
                sink,
                parser,
                target_precision,
                notifier,
                ipc_error_strategy,
                metrics_arc,
                batch_counter,
                cancel,
                archive_tx.clone(),
            )
            .await
        }
        WrittenMethod::VgroupSequential => {
            return ipc_flat_stream_worker_vgroup_sequential(
                pool,
                stream,
                sink,
                parser,
                target_precision,
                notifier,
                ipc_error_strategy,
                metrics_arc,
                batch_counter,
                cancel,
                archive_tx.clone(),
            )
            .await
        }
        WrittenMethod::Sequential => {
            return ipc_flat_stream_worker_vgroup_sequential(
                pool,
                stream,
                sink,
                parser,
                target_precision,
                notifier,
                ipc_error_strategy,
                metrics_arc,
                batch_counter,
                cancel,
                archive_tx.clone(),
            )
            .await
        }
    }
}

#[framed]
#[instrument(skip_all)]
async fn ipc_flat_stream_reader<R: Read + Send + 'static, W: Write + Send + 'static>(
    pool: &TaosPool,
    ipc_reader: IpcReader<R>,
    ipc_ack_writer: AckWriter<W>,
    cancel: CancellationToken,
    parser: Option<&Parser>,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
    archive_tx: Sender<ArchiveType>,
    persist_component: Option<PersistComponent>,
) -> anyhow::Result<()> {
    let stream = ipc_reader.into_raw_stream_with_capycity(
        persist_component
            .as_ref()
            .and_then(|c| c.config.batch_chunk_size)
            .unwrap_or(100),
    );
    let sink = futures_util::sink::unfold(ipc_ack_writer, |mut ack_writer, ack| async move {
        ack_writer.ack(ack).map_err(|err| {
            error!("Write ack error: {err:#}");
            err
        })?;
        tracing::trace!("Ack done");
        Ok(ack_writer)
    });

    ipc_flat_stream_worker(
        pool,
        stream,
        sink,
        cancel,
        parser,
        target_precision,
        notifier,
        ipc_error_strategy,
        metrics_arc,
        batch_counter,
        archive_tx.clone(),
        persist_component,
    )
    .in_current_span()
    .await
}

pub fn generate_alter_sql_diff_desc(
    tablename: &str,
    desc: &Describe,
    fields: &Vec<(impl AsRef<str>, IpcDataType)>,
    is_tag: bool,
) -> Option<Vec<String>> {
    let mut alter_sql = Vec::new();
    // diff columns and tags
    for (name, ty) in fields {
        let name = name.as_ref();
        if name == "__table_name__" {
            continue;
        }
        let mut should_alter = false;
        let mut should_add = true;
        desc.iter().for_each(|c| {
            if c.field() == name {
                should_add = false;
                let original_ty = c.ty();
                let new_def_ty = ty.ty();
                if original_ty.is_var_type() {
                    match ty {
                        IpcDataType::VarChar(len) | IpcDataType::NChar(len) => {
                            if original_ty.to_string() != new_def_ty.to_string()
                                || *len as usize > c.length()
                            {
                                should_alter = true;
                            }
                        }
                        _ => (),
                    }
                } else if original_ty.to_string() != new_def_ty.to_string() {
                    should_alter = true;
                }
            }
        });
        if should_alter && !is_tag {
            alter_sql.push(format!(
                "ALTER TABLE `{tablename}` MODIFY COLUMN `{name}` {} ",
                ty.sql_repr()
            ));
        } else if should_alter {
            alter_sql.push(format!(
                "ALTER TABLE `{tablename}` MODIFY TAG `{name}` {} ",
                ty.sql_repr()
            ));
        }
        if should_add && !is_tag {
            alter_sql.push(format!(
                "ALTER TABLE `{tablename}` ADD COLUMN `{name}` {} ",
                ty.sql_repr()
            ));
        } else if should_add {
            alter_sql.push(format!(
                "ALTER TABLE `{tablename}` ADD TAG `{name}` {} ",
                ty.sql_repr()
            ));
        }
    }
    if alter_sql.is_empty() {
        None
    } else {
        Some(alter_sql)
    }
}

pub async fn get_current_precision(conn: &Taos) -> anyhow::Result<taos::Precision> {
    let database: String = conn
        .query_one("select database()")
        .await?
        .expect("target database should be set");

    let precision = conn
        .query_one(format!(
            "select `precision` from information_schema.ins_databases where name = '{}'",
            database
        ))
        .await?
        .unwrap_or("ms".to_string());
    let target_precision = match precision.as_str() {
        "ms" => taos::Precision::Millisecond,
        "us" => taos::Precision::Microsecond,
        "ns" => taos::Precision::Nanosecond,
        _ => bail!("Unknown precision: {precision}"),
    };
    Ok(target_precision)
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(u8)]
pub enum IpcErrorStrategy {
    #[default]
    Stop = 0,
    Report,
}

impl IpcErrorStrategy {
    pub fn will_stop(&self) -> bool {
        matches!(self, IpcErrorStrategy::Stop)
    }

    fn from_connector(connector: &str) -> Self {
        match connector {
            "taos" | "opentsdb" | "influxdb" | "csv" => IpcErrorStrategy::Stop,
            _ => IpcErrorStrategy::Report,
        }
    }
}

impl From<Option<&str>> for IpcErrorStrategy {
    fn from(connector: Option<&str>) -> Self {
        match connector {
            Some(connector) => Self::from_connector(connector),
            None => Self::default(),
        }
    }
}

#[framed]
#[instrument(skip_all)]
async fn ipc_process<R: Read + Send + 'static, W: Write + Send + 'static>(
    pool: TaosPool,
    ipc_reader: IpcReader<R>,
    ipc_ack_writer: AckWriter<W>,
    opc_model_config: Option<OpcModelConfig>,
    lush_model_config: Option<LushModelConfig>,
    cancel: CancellationToken,
    parser: Option<Parser>,
    connector: Option<&str>,
    task_id: Option<i64>,
    batch_counter: Option<BatchCounter>,
    notifier: crate::TaskNotifySender,
    persist_component: Option<PersistComponents>,
) -> anyhow::Result<()> {
    // the queue for transmitting cache and archived data
    let (archive_tx, archive_rx) = flume::bounded(0);
    // clone the configurations
    let parser_clone = parser.clone();
    let cancel_clone = cancel.clone();
    // spawn a thread to write data to files
    let process_archive = tokio::spawn(async move {
        let _a = crate::utils::defer::defer(|| {
            tracing::info!("the 'cache & archive' thread has completed, task id: {task_id:?}",);
        });
        if parser_clone.is_some() && task_id.is_some() {
            let (cache, archive) = match parser_clone {
                Some(parser) => (
                    parser.global().process_on_abnormal.cache.clone(),
                    parser.global().process_on_abnormal.archive.clone(),
                ),
                None => (Cache::default(), Archive::default()),
            };
            let metrics = get_metrics_arc_from_i64(Some(task_id.unwrap())).await;

            match ArchiveConsumer::new(task_id.unwrap(), cache, archive, |num_rows: u64| {
                let metrics = metrics.ipc();
                metrics.add_archived_rows(num_rows);
                Ok::<_, anyhow::Error>(())
            })
            .consume(archive_rx)
            .await
            {
                Ok(_) => Ok(()),
                Err(err) => {
                    tracing::error!("archive consumer error: {err:#}");
                    Err(err)
                }
            }
        } else {
            drop(archive_rx);
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => {
                        tracing::info!("stop the 'cache & archive' thread, task cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    }
                }
            }
            Ok(())
        }
    });
    // spawn a thread to rewrite cache data to files
    let pool_clone = pool.clone();
    let parser_clone = parser.clone();
    let cancel_clone = cancel.clone();
    let archive_tx_clone = archive_tx.clone();
    let process_cache = tokio::spawn(async move {
        let _a = crate::utils::defer::defer(|| {
            tracing::info!("the 'rewrite file' thread has completed, task id: {task_id:?}",);
        });
        if task_id.is_none() {
            return Ok(());
        }
        if let Some(parser) = parser_clone {
            read_cache_and_rewrite(
                task_id.unwrap(),
                &pool_clone,
                &parser,
                archive_tx_clone,
                &cancel_clone,
            )
            .await
        } else {
            Ok(())
        }
    });

    let abort_handle_process_archive = process_archive.abort_handle();
    let future_process_archive = async move {
        process_archive.await??;
        anyhow::Ok(())
    };
    let abort_handle_process_cache = process_cache.abort_handle();

    info!("IPC stream processing...");
    const MAX_RETRIES: usize = 10;
    let mut retries = 0;
    let taos = loop {
        match pool.get().await {
            Ok(obj) => break obj,
            Err(err) => {
                if retries < MAX_RETRIES && !cancel.is_cancelled() {
                    retries += 1;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                } else {
                    tracing::error!("Get connection from pool failed: {err:#}");
                    return Err(anyhow::anyhow!("Get connection from pool failed: {err:#}"));
                }
            }
        }
    };
    let target_precision = get_current_precision(&taos).in_current_span().await?;

    let ipc_error_strategy = IpcErrorStrategy::from_connector(connector.unwrap_or("taos"));
    let metadata = ipc_reader.metadata();
    let stream_type = *metadata.stream_type();
    let metrics_arc = get_metrics_arc_from_i64(task_id).await;
    let metrics = metrics_arc.ipc();
    // handle lush message init
    if lush_model_config.is_none() {
        if let Some(sql) = metadata.init_sql_string() {
            let init = metadata.init().unwrap();
            handle_lush_message_init(init, &taos, &sql, metrics).await?;
        }
    }
    // handle point message init
    if let Some(opc_model_config) = &opc_model_config {
        handle_point_message_init(opc_model_config, &taos).await?;
    }

    drop(taos);
    info!(?stream_type, "Processing stream");

    let schema = ipc_reader.schema();
    let persist_component = match persist_component {
        Some(components) => match components.components.get(&schema) {
            Some(component) => Some(component.clone()),
            None => {
                tracing::error!("persist component not found for schema: {schema}");
                None
            }
        },
        None => None,
    };

    let cancel_clone = cancel.clone();
    let metrics_arc_clone = metrics_arc.clone();
    let future_ipc = async move {
        tracing::info!("IPC stream processing, stream type: {stream_type:?}",);
        match stream_type {
            StreamType::Line => todo!(),
            StreamType::Flat => ipc_flat_stream_reader(
                &pool,
                ipc_reader,
                ipc_ack_writer,
                cancel_clone,
                parser.as_ref(),
                target_precision,
                notifier,
                ipc_error_strategy,
                metrics_arc_clone.clone(),
                batch_counter,
                archive_tx.clone(),
                persist_component,
            )
            .await
            .inspect_err(|err| {
                tracing::error!("IPC stream error: {err:#}");
            }),
            StreamType::Lush => ipc_lush_stream_reader(
                &pool,
                ipc_reader,
                ipc_ack_writer,
                lush_model_config,
                task_id,
                notifier,
                ipc_error_strategy,
                metrics,
                &metrics_arc_clone,
                archive_tx.clone(),
            )
            .await
            .inspect_err(|err| {
                tracing::error!("IPC stream error: {err:#}");
            }),
            StreamType::Point => ipc_point_reader(
                &pool,
                ipc_reader,
                ipc_ack_writer,
                opc_model_config,
                target_precision,
                notifier,
                ipc_error_strategy,
                metrics_arc_clone.clone(),
                batch_counter,
                persist_component,
                cancel_clone,
            )
            .await
            .inspect_err(|err| {
                tracing::error!("IPC stream error: {err:#}");
            }),
        }
    };

    tokio::select! {
        res = future_ipc => {
            tracing::info!("IPC stream processing done, future_ipc: {res:?}",);
            res?
        }
        res = future_process_archive => {
            tracing::info!("IPC stream processing done, future_consume: {res:?}",);
            res?
        }
        _ = cancel.cancelled() => {}
    };
    abort_handle_process_archive.abort();
    abort_handle_process_cache.abort();
    Ok(())
}

#[instrument(skip_all)]
pub async fn handle_lush_message_init(
    init: &LushMessageInit,
    taos: &Taos,
    sql: &str,
    metrics: &IpcMetrics,
) -> anyhow::Result<()> {
    let max_retries = 10;
    let mut i = 0;
    let stable_name = init.name();
    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    loop {
        // alter table
        let desc = taos.describe(stable_name).await;
        match desc {
            Ok(desc) => {
                tracing::debug!("table {stable_name} exists");
                let sql = generate_alter_sql_diff_desc(
                    stable_name,
                    &desc,
                    init.columns().as_ref(),
                    false,
                );
                if sql.is_some() {
                    for sql in sql.unwrap() {
                        qid.add_sub_batch_id();
                        tracing::info!("alter table sql: {}", sql.clone());
                        taos.exec_with_req_id(sql, qid.get())
                            .in_current_span()
                            .await?;
                    }
                }
                let sql =
                    generate_alter_sql_diff_desc(stable_name, &desc, init.tags().as_ref(), true);
                if sql.is_some() {
                    for sql in sql.unwrap() {
                        qid.add_sub_batch_id();
                        tracing::info!("alter table sql: {}", sql.clone());
                        taos.exec_with_req_id(sql, qid.get())
                            .in_current_span()
                            .await?;
                    }
                }
                break;
            }
            Err(err) => {
                tracing::warn!("describe failed: {}", err.to_string());
                // create table
                qid.add_sub_batch_id();
                info!("create sql: {sql}");
                let res: Result<usize, taos::Error> = taos
                    .exec_with_req_id(sql, qid.get())
                    .in_current_span()
                    .await;
                if let Err(err) = res {
                    tracing::error!("Query error with {sql}: {err:?}");
                    i += 1;
                    if i > max_retries {
                        break;
                    }
                } else {
                    metrics.add_created_stables(1);
                    break;
                }
            }
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub struct IpcStreamWorker {
    pool: TaosPool,
    pub parser: IpcParser,
    lock: Arc<Mutex<()>>,
    task: Option<i64>,
    from: Dsn,
    config: Option<Arc<OPCConfig>>,
    opc_table_config: OnceCell<OpcModelConfig>,
    pub lush_model_config: OnceCell<Arc<LushModelConfig>>,
    pub lush_table_cache: Option<Arc<TableTagCache>>,
    breakpoint_db: Option<BreakpointDb>,
    license: Option<Arc<ConnectorLicense>>,
    transferred: Option<Arc<Transferred>>,
    taos: Cell<Option<deadpool::managed::Object<Manager<TaosBuilder>>>>,
    target_precision: taos::Precision,
    span: tracing::Span,
    cancel: CancellationToken,
}

unsafe impl Send for IpcStreamWorker {}
unsafe impl Sync for IpcStreamWorker {}

impl Clone for IpcStreamWorker {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            parser: self.parser.clone(),
            lock: self.lock.clone(),
            task: self.task,
            from: self.from.clone(),
            config: self.config.clone(),
            opc_table_config: self.opc_table_config.clone(),
            lush_model_config: self.lush_model_config.clone(),
            lush_table_cache: self.lush_table_cache.clone(),
            breakpoint_db: self.breakpoint_db.clone(),
            license: self.license.clone(),
            transferred: self.transferred.clone(),
            span: self.span.clone(),
            taos: Cell::new(None),
            target_precision: self.target_precision,
            cancel: self.cancel.clone(),
        }
    }
}

impl IpcStreamWorker {
    pub async fn new(
        pool: TaosPool,
        from: Dsn,
        lock: Arc<Mutex<()>>,
        schema: Arc<Schema>,
        license: Option<ConnectorLicense>,
        transferred: Option<Transferred>,
        lush_table_cache: Option<Arc<TableTagCache>>,
        breakpoint_db: Option<BreakpointDb>,
        span: tracing::Span,
        task: Option<i64>,
        // license: Option<>
    ) -> anyhow::Result<Self> {
        let opc_table_config: OnceCell<OpcModelConfig> = OnceCell::const_new();
        if let Some(config) = schema.metadata().get("config") {
            opc_table_config
                .get_or_try_init(|| async {
                    serde_json::from_str::<OpcModelConfig>(config).context("config error")
                })
                .await?;
        }
        let taos = pool.get().await?;
        let target_precision = get_current_precision(&taos).in_current_span().await?;

        let lush_model_config = OnceCell::const_new();
        match from.driver.as_str() {
            "pi" | "pibackfill" => {
                let config = LushModelConfig::try_from(from.clone()).unwrap();
                lush_model_config.set(Arc::new(config)).unwrap();
            }
            _ => {}
        };

        let cancel = CancellationToken::new();

        // let stmt = Stmt::init(&taos)?;
        Ok(Self {
            pool,
            from,
            parser: IpcParser::new(schema),
            lock,
            task,
            config: None,
            opc_table_config,
            lush_model_config,
            lush_table_cache,
            breakpoint_db,
            license: license.map(Arc::new),
            transferred: transferred.map(Arc::new), // stmt: Arc::new(UnsafeCell::new(stmt)),
            taos: Cell::new(Some(taos)),
            target_precision,
            span,
            cancel,
        })
    }

    pub fn stream_type(&self) -> &StreamType {
        self.parser.metadata().stream_type()
    }
    pub fn with_presets(mut self, preset: OPCConfig) -> Self {
        self.config.replace(Arc::new(preset));
        self
    }

    #[instrument(skip_all)]
    pub async fn process_record(
        &self,
        record: RecordBatch,
        parser: Option<&Parser>,
        metrics: &IpcMetrics,
        metrics_arc: &Arc<CoreMetrics>,
        tables_messages_in_progress: &Arc<AtomicUsize>,
        notifier: Option<&crate::TaskNotifySender>,
        archive_tx: Sender<ArchiveType>,
    ) -> anyhow::Result<usize> {
        let taos = unsafe { &mut *self.taos.as_ptr() };
        if taos.is_none() {
            *taos = Some(self.pool.get().await?);
        }
        match self.parser.metadata().stream_type() {
            StreamType::Line => {
                todo!()
            }
            StreamType::Flat => {
                let mut count = 0;
                let mut taos = None;
                consume_flat_record(
                    &self.pool,
                    &mut taos,
                    &record,
                    &mut count,
                    &self.cancel,
                    parser.ok_or_else(|| {
                        anyhow::format_err!("Parser should be set with flat stream")
                    })?,
                    self.target_precision,
                    metrics,
                    notifier,
                    archive_tx.clone(),
                )
                .await?;
                Ok(count)
            }
            StreamType::Lush => {
                let columns = self
                    .parser
                    .columns()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect_vec();
                let message = self.parser.parse(record)?;
                let mut count = 0;

                let record = *Box::<dyn Any>::downcast::<LushMessage>(unsafe {
                    std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(message)
                })
                .map_err(|_| anyhow::format_err!("Unable to read lush message"))?;
                let task = self.task;
                let lush_model_config = self.lush_model_config.get();

                if let Some(lush_model_config) = lush_model_config {
                    let is_tables = record.is_tables();
                    if is_tables {
                        tables_messages_in_progress.fetch_add(1, Ordering::SeqCst);
                    } else {
                        loop {
                            let tables = tables_messages_in_progress.load(Ordering::SeqCst);
                            if tables == 0 {
                                break;
                            } else {
                                tracing::debug!(tables, "waiting for tables caches to be ready");
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                    let table_tag_cache = self.lush_table_cache.clone().unwrap();
                    let res = consume_lush_record_with_transform(
                        &self.pool,
                        record,
                        &mut count,
                        metrics_arc,
                        lush_model_config.clone(),
                        table_tag_cache,
                        self.breakpoint_db.as_ref().unwrap().clone(),
                        archive_tx.clone(),
                    )
                    .await;
                    if is_tables {
                        tables_messages_in_progress.fetch_sub(1, Ordering::SeqCst);
                    }
                    res?;
                } else {
                    // let mut taos = Some(self.pool.get().await?);
                    consume_lush_record(
                        &self.pool, taos, record, &columns, &mut count, task, metrics,
                    )
                    .await?;
                }
                Ok(count)
            }
            StreamType::Point => {
                let message = self.parser.parse(record)?;
                let mut count = 0;
                let record = *Box::<dyn Any>::downcast::<PointMessage>(unsafe {
                    std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(message)
                })
                .unwrap();
                let mut taos = Some(self.pool.get().await?);
                let _n = consume_point_record(
                    &self.pool,
                    &mut taos,
                    &record,
                    &mut count,
                    self.opc_table_config
                        .get()
                        .ok_or_else(|| anyhow::format_err!("OPC table config not found"))?,
                    self.target_precision,
                    metrics,
                )
                .await?;
                if let Some(transferred) = &self.transferred {
                    transferred.points.fetch_add(_n as _, Ordering::SeqCst);
                }
                Ok(count)
            }
        }
    }

    pub fn opc_model_config(&self) -> Option<&OpcModelConfig> {
        self.opc_table_config.get()
    }
}

pub async fn listen_tcp_socket_with_agent(
    socket: Option<impl AsRef<str>>,
    cancel: CancellationToken,
    with_agent: (i64, String, String),
    config: Option<OpcModelConfig>,
    persist_config: Option<PersistConfig>,
) -> anyhow::Result<(IpcHandler, SocketAddr)> {
    let (sender, error_receiver) = tokio::sync::mpsc::channel(1);

    let (listener, listen_addr) = bind_tcp(socket).await?;

    let batch_counter = BatchCounter::new(with_agent.0 as u16).await?;

    // let (closer, mut receiver) = tokio::sync::mpsc::channel::<()>(1);
    // let closed = Arc::new(AtomicBool::new(false));
    // let closed2 = closed.clone();

    let tasks_token = cancel.child_token();
    let (persist_component, persist_tasks) = match persist_config {
        Some(config) => Some(persist::get_persist(config, &tasks_token).await?).unzip(),
        _ => (None, None),
    };

    let notify = Arc::new(tokio::sync::Notify::new());
    let notified = notify.clone();
    let thread = tokio::spawn(
        async move {
            let mut handlers = vec![];
            let cancel = cancel.child_token();
            let accept_stream = |stream: tokio::net::TcpStream, addr: std::net::SocketAddr| {
                tracing::info!("new tcp client!: {:?}", addr);
                let span = tracing::info_span!("agent_ipc_handler", client = %addr);
                let stream = stream.into_std().unwrap();
                let _ = stream.set_read_timeout(None);
                let _ = stream.set_nonblocking(false);
                let _ = stream.set_nodelay(true);
                // let client = addr.as_socket_ipv4().unwrap().to_string();
                let se = sender.clone();
                let cancel = cancel.clone();
                let (id, remote, token) = with_agent.clone();
                let config = config.clone();
                let notify = notified.clone();
                let batch_counter = batch_counter.clone();
                let persist_component = persist_component.clone();

                tokio::spawn(async move {
                    info!("Spawned IPC reader in Agent");
                    let cancel2 = cancel.clone();
                    let res =
                        ipc_tcp_forward(stream, cancel, remote, token, id, batch_counter, config, persist_component).in_current_span().await;
                    if let Err(err) = res {
                        let error_msg = format!("{:?}", err);
                        // 如果不以"transport error"开头，且包含"os error 10060"
                        // 则认为是 windows 下的 IPC 连接断开，可能是 connector 的正常行为，仅记录 warn 日志
                        if error_msg.contains("os error 10060") && !error_msg.starts_with("transport error") {
                            tracing::warn!("IPC reader stopped with warn: {}", error_msg);
                        } else {
                            tracing::error!("{:?}", err);
                            if cancel2.is_cancelled() {
                                tracing::debug!("IPC handler completed");
                                return;
                            }
                            // notify the listener to stop
                            notify.notify_waiters();
                            tokio::spawn(async move {
                                let r = se.send(format!("{err:?}")).await;
                                if let Err(send_err) = r {
                                    tracing::error!("error <{err:?}> reported to server: {send_err:?}");
                                }
                            });
                        }
                    } else {
                        tracing::info!("IPC reader stopped");
                    }
                }.instrument(span))
            };
            let mut backoff = 1;
            loop {
                tokio::select! {
                    _ = notified.notified() => {
                        break;
                    }
                    _ = cancel.cancelled() => {
                        tracing::debug!("Agent IPC listener received task cancel signal");
                        notified.notify_waiters();
                        break;
                    }
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, addr)) => {
                                backoff = 1;
                                let h = accept_stream(stream, addr);
                                handlers.push(h);
                            }
                            Err(e) => {
                                if backoff > 64 {
                                    // Accept has been failed too many times. break the loop.
                                    tracing::warn!("IPC stream acceptation error {e:#}, might be stopped");
                                    break;
                                }
                                tokio::time::sleep(Duration::from_secs(backoff)).await;
                                backoff *= 2;
                            }
                        }
                    }
                }
            }
            drop(persist_component);
            tracing::info!(ipc.handlers = handlers.len(), "IPC stream listener stopped");
            let instant = std::time::Instant::now();

            for h in handlers {
                match tokio::time::timeout(Duration::from_secs(5), h).await {
                    Err(timeout) => {
                        tracing::warn!("IPC stream handler timeout: {timeout:#}");
                    }
                    Ok(Err(_)) => {
                        tracing::warn!("IPC stream handler join error");
                    }
                    Ok(Ok(())) => {
                        tracing::info!("IPC stream handler finished");
                    }
                }
            }
            if let Some(mut tasks) = persist_tasks {
                tasks_token.cancel();
                loop {
                    match tokio::time::timeout(Duration::from_secs(5), tasks.join_next()).await {
                        Ok(None) => break,
                        Ok(Some(Ok(Ok(_)))) => {}
                        Ok(Some(Ok(Err(e)))) => {
                            tracing::warn!("persist task exit with error: {e:#}");
                        }
                        Ok(Some(Err(e))) => {
                            tracing::warn!("persist task exit paniced: {e}");
                        }
                        Err(_) => {
                            tracing::warn!("waiting persist tasks exit timeout");
                        }
                    }
                }
            }

            tracing::info!("IPC stream handlers finished after {:?}", instant.elapsed());
            anyhow::Ok(())
        }
            .instrument(tracing::info_span!("agent_ipc_listener")),
    );

    let notified = notify.clone();
    let handle = tokio::spawn(async move {
        notified.notified().await;
        tracing::debug!("shutdown socket");
        match tokio::time::timeout(Duration::from_secs(60 * 60), thread).await {
            Ok(Ok(_)) => anyhow::Ok(()),
            Ok(Err(err)) => anyhow::bail!("Thread join error: {err}"),
            Err(_) => {
                anyhow::bail!("Task running deadline elapsed(1h), but seems not finished")
            }
        }
    });
    Ok((IpcHandler::new(notify, handle, error_receiver), listen_addr))
}

pub struct IpcHandler {
    closer: Arc<Notify>,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    receiver: tokio::sync::mpsc::Receiver<String>,
}

impl IpcHandler {
    fn new(
        closer: Arc<Notify>,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
        receiver: tokio::sync::mpsc::Receiver<String>,
    ) -> Self {
        Self {
            closer,
            handle,
            receiver,
        }
    }

    pub fn send<T>(&self, _: T) -> Result<(), tokio::sync::mpsc::error::SendError<()>> {
        // self.closer.send(()).await
        self.closer.notify_waiters();
        Ok(())
    }

    pub async fn wait(mut self) -> anyhow::Result<()> {
        (&mut self.handle).await?
    }

    /// Receive error
    pub async fn recv_error(&mut self) -> Option<String> {
        self.receiver.recv().await
    }

    /// Receive error
    pub fn try_recv_error(&mut self) -> Result<String, tokio::sync::mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Close IPC listener and wait until IPC handler joint.
    pub async fn close(self) -> anyhow::Result<()> {
        // let _ = self.closer.send(()).await;
        self.closer.notify_waiters();
        self.handle.await??;
        Ok(())
    }
}

#[instrument(skip_all)]

pub async fn listen_tcp_socket(
    target: TaosPool,
    socket: Option<impl AsRef<str>>,
    opc_model_config: Option<OpcModelConfig>,
    lush_model_config: Option<LushModelConfig>,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    parser: Option<Parser>,
    connector: Option<&'static str>,
    task_id: Option<i64>,
    notifier: crate::TaskNotifySender,
    persist_config: Option<PersistConfig>,
) -> anyhow::Result<(IpcHandler, SocketAddr)> {
    let (sender, error_receiver) = tokio::sync::mpsc::channel(1);

    let (listener, listen_addr) = bind_tcp(socket).await?;

    info!("listen on socket address: {listen_addr}");
    let socket = Arc::new(listener);
    let notify = Arc::new(tokio::sync::Notify::new());
    let notified = notify.clone();

    let mut batch_counter = None;
    if let Some((task_id, _, _)) = with_agent.as_ref() {
        batch_counter = Some(BatchCounter::new(*task_id as u16).await?);
    } else if let Some(task_id) = task_id.as_ref() {
        batch_counter = Some(BatchCounter::new(*task_id as u16).await?);
    };

    let (persist_component, mut persist_tasks) = match persist_config {
        Some(config) => Some(persist::get_persist(config, &cancel).await?).unzip(),
        _ => (None, None),
    };

    let thread = tokio::task::spawn(
        async move {
            info!("waiting for IPC connections");
            let cancel = cancel.child_token();
            let server = listen_addr;
            let mut set = tokio::task::JoinSet::new();
            let mut accept_stream = |stream: tokio::net::TcpStream, addr: std::net::SocketAddr, ipc_id: usize| {
                tracing::info!("new tcp client!: {:?}", addr);
                let span = tracing::info_span!("ipc_reader", ipc.id = ipc_id, %server, client = %addr);
                let stream = stream.into_std().unwrap();
                let _ = stream.set_read_timeout(None);
                let _ = stream.set_nonblocking(false);
                let se = sender.clone();
                let cancel = cancel.clone();

                if let Some((id, server, token)) = with_agent.clone() {
                    let opc_model_config = opc_model_config.clone();
                    let batch_counter = batch_counter.clone().unwrap();
                    let persist_component = persist_component.clone();
                    set.spawn(async move {
                        let res = ipc_tcp_forward(stream, cancel, server, token, id, batch_counter, opc_model_config, persist_component).in_current_span().await;
                        if let Err(err) = res {
                            tracing::error!("ipc read err: {:#}", err);
                            let _ = se.send(format!("{:#}", err)).await;
                        }
                    })
                } else {
                    let pool = target.clone();
                    let opc_model_config = opc_model_config.clone();
                    let lush_model_config = lush_model_config.clone();
                    let parser = parser.clone();
                    let notifier = notifier.clone();
                    let notify = notified.clone();
                    let batch_counter = batch_counter.clone();
                    let persist_components = persist_component.clone();
                    set.spawn(async move {
                        // let dsn: Dsn = "taos:///db2".parse().unwrap();
                        // let pool = TaosBuilder::from_dsn(dsn).unwrap().pool().unwrap();
                        info!("Spawned IPC reader");
                        let cancel2 = cancel.clone();
                        let res = ipc_tcp_read(
                            pool,
                            stream,
                            opc_model_config,
                            lush_model_config,
                            cancel,
                            parser,
                            connector,
                            task_id,
                            batch_counter,
                            notifier,
                            persist_components,
                        )
                            .in_current_span()
                            .await;
                        if let Err(err) = res {
                            // panic!("{err:?}");
                            println!("{err:?}");
                            tracing::error!("ipc read err: {:#}", err);
                            if cancel2.is_cancelled() {
                                tracing::debug!("IPC handler completed");
                                return;
                            }
                            // notify the listener to stop
                            notify.notify_waiters();
                            // Found error, now cancel all IPC runners.
                            cancel2.cancel();
                            let _ = se.send(format!("{:#}", err)).await;
                        } else {
                            tracing::debug!("IPC handler completed");
                        }
                    }.instrument(span))
                }
            };
            let mut backoff = 1;
            let mut ipc_id = 0;
            use futures_ext::OptionFuture;
            loop {
                tokio::select! {
                    _ = notified.notified() => {
                        tracing::debug!("IPC listener received close signal");
                        break;
                    }
                    accept = socket.accept() => {
                        match accept {
                            Ok((stream, addr)) => {
                                backoff = 1;
                                accept_stream(stream, addr, ipc_id);
                                ipc_id += 1;
                            }
                            Err(e) => {
                                if backoff > 64 {
                                    // Accept has been failed too many times. break the loop.
                                    tracing::warn!("IPC stream acceptation error {e:#}, might be stopped");
                                    break;
                                }
                                tokio::time::sleep(Duration::from_secs(backoff)).await;
                                backoff *= 2;
                            }
                        }
                    }
                    res = OptionFuture::from(persist_tasks.as_mut().map(|t| t.join_next())) => {
                        let Some(res) = res else {
                            break
                        };
                        match res {
                            Ok(Ok(_)) => {},
                            Ok(Err(e)) => {
                                tracing::error!("persist task exited with error: {e:#}");
                                break
                            }
                            Err(e) => {
                                tracing::error!("persist task paniced: {e}");
                                break
                            }
                        }
                    }
                    _ = cancel.cancelled() => {
                        tracing::debug!("IPC listener received task cancel signal");
                        break;
                    }
                }
            }
            notified.notify_waiters();
            drop(persist_component);
            tracing::info!(ipc.handlers = ipc_id, "IPC stream listener would wait for handlers to finish");

            let _ = tracing::info_span!("wait for ipc handlers to be finished").entered();

            let instant = std::time::Instant::now();
            // let handlers = handlers.into_iter().map(|h| {
            //     tokio::time::timeout(Duration::from_secs(10), h)
            // }).collect::<Vec<_>>();
            let max_wait_timeout = Duration::from_secs(60 * 10);
            if tokio::time::timeout(max_wait_timeout, async {
                while let Some(res) = set.join_next().await {
                    let _ = res;
                }
                if let Some(mut tasks) = persist_tasks.take() {
                    while let Some(res) = tasks.join_next().await {
                        match res {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => {
                                tracing::error!("persist task exit with error: {e:#}");
                            }
                            Err(e) => {
                                tracing::error!("persist task paniced: {e}");
                            }
                        }
                    }
                }
            }).await.is_err() {
                set.abort_all();
            }
            tracing::info!("IPC stream handlers finished after {:?}", instant.elapsed());
        }
            .instrument(tracing::info_span!("plain_ipc_listener")),
    );
    let notified = notify.clone();
    let handle = tokio::spawn(
        async move {
            // closed.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = notified.notified().await;
            tracing::info!("stop listener");
            match thread.await {
                Ok(_) => {
                    tracing::info!("IPC listener thread finished");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("IPC listener thread error: {:#?}", e);
                    anyhow::bail!("IPC listener thread error: {:#?}", e);
                }
            }
        }
        .instrument(tracing::info_span!("plain_ipc_listener_abort_handle")),
    );
    Ok((IpcHandler::new(notify, handle, error_receiver), listen_addr))
}

async fn bind_tcp(
    addr: Option<impl AsRef<str>>,
) -> anyhow::Result<(tokio::net::TcpListener, SocketAddr)> {
    match addr {
        Some(socket) => {
            let addr = socket.as_ref();
            let socket = tokio::net::TcpSocket::new_v4()?;
            let addr: SocketAddr = addr.parse()?;
            socket.bind(addr)?;
            Ok((socket.listen(65535)?, addr))
        }
        None => {
            let listener = tokio::net::TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
                .await
                .context("bind tcp listener error")?;
            let addr = listener
                .local_addr()
                .context("get tcp listener addr error")?;
            Ok((listener, addr))
        }
    }
}

#[instrument(skip_all)]
#[allow(clippy::type_complexity)]
pub async fn channel_based_transformer(
    target: TaosPool,
    cancel: CancellationToken,
    parser: Option<Parser>,
    connector: Option<&'static str>,
    task_id: Option<i64>,
    notifier: crate::TaskNotifySender,
    send_capacity: usize,
) -> anyhow::Result<(
    flume::Sender<Result<RecordBatch, ArrowError>>,
    flume::Receiver<LushAck>,
)> {
    // the queue for transmitting cache and archived data
    let (archive_tx, archive_rx) = flume::bounded(0);
    // clone the configurations
    let parser_clone = parser.clone();
    let cancel_clone = cancel.clone();
    // spawn a thread to write data to files
    let process_archive = tokio::spawn(async move {
        let _a = crate::utils::defer::defer(|| {
            tracing::info!("the 'cache & archive' thread has completed, task id: {task_id:?}",);
        });

        if parser_clone.is_some() && task_id.is_some() {
            let (cache, archive) = match parser_clone {
                Some(parser) => (
                    parser.global().process_on_abnormal.cache.clone(),
                    parser.global().process_on_abnormal.archive.clone(),
                ),
                None => (Cache::default(), Archive::default()),
            };
            let metrics = get_metrics_arc_from_i64(Some(task_id.unwrap())).await;

            ArchiveConsumer::new(task_id.unwrap(), cache, archive, |num_rows: u64| {
                let metrics = metrics.ipc();
                metrics.add_archived_rows(num_rows);
                Ok::<_, anyhow::Error>(())
            })
            .consume(archive_rx)
            .await
        } else {
            drop(archive_rx);

            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => {
                        tracing::info!("stop the 'cache & archive' thread, task cancelled");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
            Ok(())
        }
    });

    // spawn a thread to rewrite cache data to files
    let pool_clone = target.clone();
    let parser_clone = parser.clone();
    let cancel_clone = cancel.clone();
    let archive_tx_clone = archive_tx.clone();
    let process_cache = tokio::spawn(async move {
        let _a = crate::utils::defer::defer(|| {
            tracing::info!("the 'rewrite file' thread has completed, task id: {task_id:?}",);
        });

        match (parser_clone, task_id) {
            (Some(parser), Some(task_id)) => {
                read_cache_and_rewrite(
                    task_id,
                    &pool_clone,
                    &parser,
                    archive_tx_clone,
                    &cancel_clone,
                )
                .await
            }
            _ => Ok(()),
        }
    });

    let abort_handle_process_archive = process_archive.abort_handle();
    let future_process_archive = async move {
        process_archive.await??;
        anyhow::Ok(())
    };
    let abort_handle_process_cache = process_cache.abort_handle();

    let taos = target.get().await?;
    let target_precision = get_current_precision(&taos).in_current_span().await?;
    let (msg_tx, msg_rx) = flume::bounded(send_capacity);
    let (ack_tx, ack_rx) = flume::unbounded();

    let stream = msg_rx.into_stream();
    let sink = futures_util::sink::unfold(ack_tx, |ack_tx, ack| async move {
        ack_tx
            .send_async(ack)
            .await
            .map_err(|err| ArrowError::MemoryError(format!("ACK channel error: {err:#}")))?;
        Ok(ack_tx)
    });

    let ipc_error_strategy = IpcErrorStrategy::from_connector(connector.unwrap_or("taos"));

    let batch_counter = if let Some(task_id) = task_id {
        Some(BatchCounter::new(task_id as u16).await?)
    } else {
        None
    };
    let metrics = get_metrics_arc_or(task_id, || {
        Arc::new(CoreMetrics::IPC(IpcMetrics::default()))
    })
    .await;
    tokio::spawn(
        async move {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("IPC stream cancelled");
                },
                _ = async {
                    ipc_flat_stream_worker(
                        &target,
                        stream,
                        sink,
                        cancel.clone(),
                        parser.as_ref(),
                        target_precision,
                        notifier,
                        ipc_error_strategy,
                        metrics,
                        batch_counter,
                        archive_tx.clone(),
                        None,
                    )
                    .in_current_span()
                    .await
                } => {},
                status = future_process_archive => {
                    if let Err(e) = status {
                        tracing::error!("archive consumer error: {e:#}");
                    }
                }
            }
            abort_handle_process_archive.abort();
            abort_handle_process_cache.abort();
        }
        .in_current_span(),
    );
    Ok((msg_tx, ack_rx))
}

pub async fn read_cache_and_rewrite(
    task_id: i64,
    pool: &TaosPool,
    parser: &Parser,
    archive_tx: Sender<ArchiveType>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let cache_path = parser.global().process_on_abnormal.cache.location.clone();
    let metrics_arc = get_metrics_arc_from_i64(Some(task_id)).await;
    let metrics = metrics_arc.ipc();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("rewrite file cancelled");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if let Ok(taos) = pool.get().await {
                    let target_precision = match get_current_precision(&taos).in_current_span().await {
                        Ok(precision) => precision,
                        Err(e) => {
                            tracing::error!("get current precision error, e: {e:#}");
                            continue;
                        }
                    };
                    //get all cached files at current time point
                    let files = match get_rewrite_files(archive_tx.clone()).await {
                        Ok(files) => files,
                        Err(e) => {
                            tracing::error!("get rewrite files error, e: {e:#}");
                            continue;
                        }
                    };
                    let mut taos_mut = Some(taos);
                    for file in files {
                        match read_file_and_rewrite(
                            file.clone(),
                            pool,
                            &mut taos_mut ,
                            target_precision,
                            metrics,
                            parser,
                            archive_tx.clone(),
                            cancel,
                        ).await {
                            Ok(_) => {
                                let _ = std::fs::remove_file(file);
                            }
                            Err(e) => {
                                tracing::error!("rewrite file error, path: {file:?}, e: {e:#}");
                            }
                        }
                    }
                }
            }
        }
        tracing::debug!("rewrite file loop, task: {task_id}, cache: {cache_path}");
    }
    Ok(())
}

async fn read_file_and_rewrite(
    file: PathBuf,
    pool: &TaosPool,
    taos: &mut Option<deadpool::managed::Object<Manager<TaosBuilder>>>,
    target_precision: taos::Precision,
    metrics: &IpcMetrics,
    parser: &Parser,
    archive_tx: Sender<ArchiveType>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let batches = read_parquet_file(file.clone())?;
    for batch in batches {
        let message = parser.parse_message_from_records(&batch, true, archive_tx.clone())?;
        let messages = match message {
            crate::plugins::transform::Message::Raw(_) => todo!(),
            crate::plugins::transform::Message::Tables(_) => todo!(),
            crate::plugins::transform::Message::ChildTables(_) => todo!(),
            crate::plugins::transform::Message::Records(messages) => messages,
        };
        let _ = flat_write_with_sql(
            pool,
            taos,
            target_precision,
            &messages,
            metrics,
            None,
            cancel,
            parser.global(),
            archive_tx.clone(),
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::plugins::runners::opc::model::PointConfig;
    use crate::plugins::runners::opc::model::TableConfig;
    use crate::runners::opc::OpcType;
    use crate::utils::port_pool::PortPool;
    use crate::utils::trace::{DEFAULT_INSTANCE_ID, INSTANCE_ID};
    use linked_hash_map::LinkedHashMap;
    use std::env;
    use taos::sync::Fetchable;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tracing_subscriber;

    use super::*;

    /// # Example
    /// ```shell
    /// TAOS_ADDR=taos+ws://192.168.0.201:6041 cargo nextest run -p taosx-core test_handle_point_message_init_with_taos --no-capture --retries 0
    /// ```
    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_point_message_init_with_taos() {
        tracing_subscriber::fmt::try_init().ok();
        INSTANCE_ID.set(DEFAULT_INSTANCE_ID).unwrap();

        // given
        let dsn = env::var("TAOS_ADDR").ok().unwrap_or("taos://".to_string());
        let taos = TaosBuilder::from_dsn(&dsn).unwrap().build().await.unwrap();
        let db = "test_handle_point_message_init";
        taos.exec_many([
            format!("drop database if exists {db}"),
            format!("create database {db}"),
            format!("use {db}"),
            "create stable opc_int(ts timestamp, v int) tags(t int)".to_string(),
            "create table `AbC-3-1001` using opc_int tags(1)".to_string(),
            "create table `AbC-3-1002` using opc_int tags(2)".to_string(),
            "create table `AbC-3-1003` using opc_int tags(3)".to_string(),
        ])
        .await
        .unwrap();
        let mut point_config_map = LinkedHashMap::new();
        let mut table_config_map = LinkedHashMap::new();
        for i in 1..=3 {
            point_config_map.insert(
                i.to_string(),
                PointConfig {
                    row_index: 1,
                    code: format!("AbC-3-100{}", i),
                    stable: None,
                    tag_values: None,
                    value_type: None,
                },
            );
            table_config_map.insert(
                i.to_string(),
                TableConfig {
                    enabled: Some(i % 2),
                    stable_prefix: None,
                    column_configs: vec![],
                    tag_configs: None,
                },
            );
        }
        let config = OpcModelConfig {
            opc_type: OpcType::OPCUA,
            generate_rule: None,
            point_config_map,
            table_config_map,
        };
        let taos = TaosBuilder::from_dsn(format!("{}/{}", &dsn, &db))
            .unwrap()
            .build()
            .await
            .unwrap();
        // when
        handle_point_message_init(&config, &taos).await.unwrap();
        // then
        let mut res = taos.query("show tables").await.unwrap();
        let mut tables = vec![];
        for row in res.to_rows_vec().unwrap() {
            let table = row.first().unwrap().to_string().unwrap();
            tables.push(table);
        }
        let tables = tables.iter().map(|s| s.as_str()).sorted().collect_vec();
        assert_eq!(tables, ["AbC-3-1001", "AbC-3-1003"]);

        // clean
        taos.exec(format!("drop database {db}")).await.unwrap();
    }

    async fn listen(listener: tokio::net::TcpListener) -> anyhow::Result<()> {
        let (mut stream, _) = listener.accept().await?;
        let mut buf = Vec::new();
        {
            stream.read_to_end(&mut buf).await?;
        }
        assert_eq!(&buf, b"hello, world");
        Ok(())
    }

    async fn connect(addr: SocketAddr) -> anyhow::Result<()> {
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        stream.write_all(b"hello, world").await?;
        stream.flush().await?;
        Ok(())
    }

    #[tokio::test]
    async fn system_bind_tcp_test() -> anyhow::Result<()> {
        let (listener, addr) = bind_tcp(None::<&str>).await?;
        assert_eq!(listener.local_addr()?, addr);
        tokio::try_join!(listen(listener), connect(addr))?;
        Ok(())
    }

    #[tokio::test]
    async fn manually_bind_tcp_test() -> anyhow::Result<()> {
        let port_pool = PortPool::default();
        let port = port_pool.get().await.context("port")?;
        let addr = format!("127.0.0.1:{}", port.get());
        let Ok((listener, listen_addr)) = bind_tcp(Some(&addr)).await else {
            // bind tcp manully may fail
            return Ok(());
        };
        assert_eq!(listener.local_addr()?, listen_addr);
        assert_eq!(listen_addr, addr.parse::<SocketAddr>()?);

        tokio::try_join!(listen(listener), connect(listen_addr))?;
        Ok(())
    }

    #[tokio::test]
    async fn db_not_exist_abnormal() -> anyhow::Result<()> {
        let config = r#"{
            "global": {
                "database_not_exist": "archive"
            },
            "mutate": [],
            "model": []
        }"#;
        let parser = serde_json::from_str::<Parser>(config)?;
        let batch = arrow::array::record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)])
        )?;
        let (archive_tx, rx) = flume::unbounded();
        handle_flat_abnormal(
            ProcessOnAbnormalEnum::DatabaseNotExist(
                &parser.global().process_on_abnormal.database_not_exist,
            ),
            &batch,
            archive_tx.clone(),
        )?;
        let rs = rx.recv();
        dbg!(&rs);
        assert!(rs.is_ok());

        let config = r#"{
            "global": {
                "database_not_exist": "break"
            },
            "mutate": [],
            "model": []
        }"#;
        let parser = serde_json::from_str::<Parser>(config)?;
        let rs = handle_flat_abnormal(
            ProcessOnAbnormalEnum::DatabaseNotExist(
                &parser.global().process_on_abnormal.database_not_exist,
            ),
            &batch,
            archive_tx.clone(),
        );
        assert!(rs.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn db_conn_abnormal() -> anyhow::Result<()> {
        let config = r#"{
            "global": {
                "database_connection_error": "cache"
            },
            "mutate": [],
            "model": []
        }"#;
        let parser = serde_json::from_str::<Parser>(config)?;
        let batch = arrow::array::record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)])
        )?;
        let (cache_tx, rx) = flume::unbounded();
        handle_flat_abnormal(
            ProcessOnAbnormalEnum::DatabaseConnectionError(
                &parser
                    .global()
                    .process_on_abnormal
                    .database_connection_error,
            ),
            &batch,
            cache_tx.clone(),
        )?;
        let rs = rx.recv();
        dbg!(&rs);
        assert!(rs.is_ok());

        let config = r#"{
            "global": {
                "database_connection_error": "break"
            },
            "mutate": [],
            "model": []
        }"#;
        let parser = serde_json::from_str::<Parser>(config)?;
        let rs = handle_flat_abnormal(
            ProcessOnAbnormalEnum::DatabaseConnectionError(
                &parser
                    .global()
                    .process_on_abnormal
                    .database_connection_error,
            ),
            &batch,
            cache_tx.clone(),
        );
        dbg!(&rs);
        assert!(rs.is_err());
        Ok(())
    }
}
