use std::cmp;
use std::{
    any::Any,
    cell::Cell,
    collections::{HashMap, HashSet},
    io::{Read, Write},
    iter::zip,
    net::SocketAddr,
    num::NonZeroUsize,
    str::FromStr,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use crate::runners::opc::config::model::ColumnConfig;
use crate::runners::opc::config::model::OpcModelConfig;
use crate::runners::opc::config::model::TableConfig;
use crate::runners::opc::config::model::TagConfig;

use crate::core_metrics::get_metrics_arc_from_i64;
use crate::utils::breakpoints::BreakpointDb;
use crate::utils::sql::get_minimum_timestamp;
use crate::utils::trace::{BatchCounter, Qid};
use crate::{
    core_metrics::{CoreMetrics, TaskMetrics},
    runners::opc::config::OPCConfig,
};
use crate::{utils::breakpoints::breakpoints_set, ConnectorLicense, Parser, Transferred};
use anyhow::{anyhow, bail, Context};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::{datatypes::Schema, ipc::writer::IpcWriteOptions, record_batch::RecordBatch};
use arrow_flight::{flight_service_client::FlightServiceClient, FlightClient};
use arrow_schema::{ArrowError, Field};
use arrow_schema::{DataType, TimeUnit};
use async_backtrace::framed;
use bytes::Bytes;
use faststr::FastStr;
use futures_util::{Sink, Stream, StreamExt};
use rhai::{Dynamic, Engine, Scope};
use ring_channel::{ring_channel, RingReceiver};
use serde_json::json;
use taos::{
    taos_query::{common::Describe, Manager},
    Itertools, Taos, TaosPool, Ty, Value,
};
use taoslog::utils::QidMetadataGetter;
use taoslog::QidManager;
use tokio::sync::{Mutex, Notify, OnceCell};
use tonic::{codec::CompressionEncoding, transport::Channel};
use tracing::{debug, error, info, instrument};

use taosx_ipc::stream::point::{RecordMessage, RecordTransform};
use taosx_ipc::{
    prelude::*,
    stream::{flat::FlatMessage, point::PointMessage},
};

use tracing::{trace, warn};

use crate::{
    plugins::transform::WrittenMethod,
    sink::flat::{
        ipc_flat_stream_worker_concurrent, ipc_flat_stream_worker_vgroup,
        ipc_flat_stream_worker_vgroup_sequential,
    },
};

use super::super::AGENT_COMPRESSION;
use super::*;

use self::{
    flat::{flat_write_with_raw_block, flat_write_with_sql},
    ipc_metric::IpcMetrics,
    lush::{LushModelConfig, TableTagCache},
};
use arrow_compute_ext::RecordBatchExt;

pub mod flat;
pub mod ipc_metric;
pub mod lush;

use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub const RPC_ACK_REQUEST: u8 = 0;
pub const RPC_ACK_RECEIVED: u8 = 1;
pub const RPC_ACK_PROCESSED: u8 = 2;
pub const RPC_ACK_DROPPED: u8 = 3;
pub const RPC_ACK_STREAM_END: u8 = 0xFE;
pub const RPC_ACK_DECODE_ERROR: u8 = 0xFF;

#[derive(FromZeroes, FromBytes, AsBytes)]
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
        zerocopy::AsBytes::as_bytes(self)
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
) -> anyhow::Result<()> {
    use md5;
    tracing::info!("token: {}", format!("{:x}", md5::compute(token.clone())));
    let _ = cancel;
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
    let ipc_ack_writer =
        tokio::task::spawn_blocking(move || AckWriterBuilder::new(ack).open(stream))
            .in_current_span()
            .await
            .context("Spawn AckWriter error")?
            .context("Create AckWriter error")?;

    let schema = ipc_reader.schema.clone();
    let mut schema = schema.as_ref().clone();
    if let Some(config) = config.as_ref() {
        schema.metadata.insert(
            "config".to_string(),
            serde_json::to_string(&config).unwrap(),
        );
    }
    let schema: Arc<Schema> = Arc::new(schema);

    info!("Reading batches");
    let ipc_stream = ipc_reader.into_raw_stream_qos_0(ipc_ack_writer);

    let (tables_cache_tx, tables_cache_rx) = ring_channel(NonZeroUsize::new(50).unwrap());

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
        let data_stream = futures::stream::iter(retained_tables)
            .map(Ok)
            .chain(ipc_stream.clone())
            .inspect_ok(move |batch| {
                if is_lush && is_tables_record(batch) {
                    let _ = tables_cache_tx.send(batch.clone());
                }
            })
            .map_err(move |err: ArrowError| {
                cur_span_in_map_err.in_scope(|| {
                    warn!(error = ?err, "IPC reading error: {err:#}");
                });
                FlightError::from(err)
            });
        if last_retries > MAX_LAST_RETRIES {
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

        const MAX_RETRIES: usize = 5;
        let mut retries = 0;
        let channel = loop {
            match try_establish_channel(remote.clone()).await {
                Ok(channel) => break channel,
                Err(err) => {
                    retries += 1;
                    tracing::error!("Failed to establish connection: {}. Retrying...", err);
                    if retries >= MAX_RETRIES {
                        tracing::error!("Max retries reached. Exiting...");
                        return Err(err);
                    }
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        };
        let alive = std::time::Instant::now();

        let mut client;
        if *(AGENT_COMPRESSION.get().unwrap_or(&false)) {
            let client_inner = FlightServiceClient::new(channel);
            client = FlightClient::new_from_inner(
                client_inner.send_compressed(CompressionEncoding::Gzip),
            );
        } else {
            client = FlightClient::new(channel);
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
            continue 'start;
        }
        info!("Handshake done");
        // dbg!(res);
        info!("Do putting");
        let mut stream = match client
            .do_put(data)
            .await
            .map_err(move |err| match dbg!(err) {
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
                    return Ok(());
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
                        if let Some(metadata) = MessageMetadata::ref_from(&rsp.app_metadata) {
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
                                    return Ok(());
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
                                    return Ok(());
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
                return Ok(());
            }
        }
    }
}

async fn try_establish_channel(remote: String) -> anyhow::Result<Channel> {
    let endpoint = tonic::transport::Endpoint::try_from(remote)?
        .keep_alive_while_idle(true)
        .keep_alive_timeout(Duration::from_secs(300))
        .http2_keep_alive_interval(Duration::from_secs(39))
        .tcp_keepalive(Some(Duration::from_secs(7200))); // keep alive for 2 hours
    let channel = endpoint.connect().await?;
    Ok(channel)
}

#[framed]
async fn ipc_tcp_read(
    pool: TaosPool,
    stream: std::net::TcpStream, //socket2::Socket,
    lock: Arc<Mutex<()>>,
    opc_model_config: Option<OpcModelConfig>,
    lush_model_config: Option<LushModelConfig>,
    cancel: CancellationToken,
    parser: Option<Parser>,
    connector: Option<&'static str>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    batch_counter: Option<BatchCounter>,
    notifier: crate::TaskNotifySender,
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
        lock,
        opc_model_config,
        lush_model_config,
        cancel,
        parser,
        connector,
        transferred,
        task_id,
        batch_counter,
        notifier,
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
    columns: &Vec<String>,
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
                            // table not exists
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
                                    let mut sql_vec = Vec::new();
                                    sql_vec.push((table_sql, false, 1u16));
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
                                        0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                                            taos.replace(pool.get().await?);
                                            retry += 1;
                                        }
                                        0x2603 | 0x0618 => {
                                            // table not exists
                                            tokio::time::sleep(Duration::from_millis(100)).await;
                                        }
                                        0x2653 => {
                                            // column or tag length not enough
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
                .parse_message_from_records(&full_record, false)
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
                        parser.parse_message_from_records(&record_batch, true).with_context(|| {
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
                    }) }.in_current_span()) {
                        tracing::error!("send to tx error: {err:#}");
                        bail!("Send future error: {err:#}");
                    }
                }
                anyhow::Ok(())
            })}).await.context("Spawn blocking transform lush records inserts")??;

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

struct ModifyStructForPointMessage {
    id: String,
    point_name: String,
    value_column_name: String,
    value_column_length: usize,
}

/// handle value_transform, ts_transform, rts_transform
async fn handle_transform(
    message: &RecordMessage,
    config: &OpcModelConfig,
) -> anyhow::Result<RecordMessage> {
    // id
    let id_col = message.clone_column_by_name("id")?;

    // name
    let name_col = message.clone_column_by_name("name")?;

    // transform ts
    let ts_config_map = config.get_column_config_map_by_name(ColumnConfig::ORIGINAL_TS);
    let mut ts_transform = to_record_transform_map(&ts_config_map);
    // 通过规则生成的 transform
    let generated_ts_config: HashMap<String, ColumnConfig> = config
        .generate_transform_map(ColumnConfig::ORIGINAL_TS)
        .await;
    let generated_ts_transform_map = to_record_transform_map(&generated_ts_config);
    // 将生成的 transform_map 添加到原始的 transform_map 中
    for (point_id, transform) in generated_ts_transform_map {
        ts_transform.entry(point_id).or_insert(transform);
    }
    let transformed_ts_col = transform_by_name(message.record(), "ts", ts_transform)?;

    // transform received_ts
    let rts_config_map = config.get_column_config_map_by_name(ColumnConfig::RECEIVED_TS);
    let mut rts_transform = to_record_transform_map(&rts_config_map);
    // 通过规则生成的 transform
    let generated_rts_config: HashMap<String, ColumnConfig> = config
        .generate_transform_map(ColumnConfig::RECEIVED_TS)
        .await;
    let generated_rts_transform_map = to_record_transform_map(&generated_rts_config);
    // 将生成的 transform_map 添加到原始的 transform_map 中
    for (point_id, transform) in generated_rts_transform_map {
        rts_transform.entry(point_id).or_insert(transform);
    }
    let transformed_received_col = transform_by_name(message.record(), "received", rts_transform)?;

    // transform value
    let val_config_map = config.get_column_config_map_by_name(ColumnConfig::VALUE);
    let mut value_transform = to_record_transform_map(&val_config_map);
    // 通过规则生成的 transform
    let generated_value_config: HashMap<String, ColumnConfig> =
        config.generate_transform_map(ColumnConfig::VALUE).await;
    let generated_value_transform = to_record_transform_map(&generated_value_config);
    // 将生成的 transform_map 添加到原始的 transform_map 中
    for (point_id, transform) in generated_value_transform {
        value_transform.entry(point_id).or_insert(transform);
    }

    let transformed_value_col = transform_by_name(message.record(), "value", value_transform)?;

    // status
    let status_col = message.clone_column_by_name("status")?;

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("ts", transformed_ts_col.data_type().clone(), false),
        Field::new(
            "received",
            transformed_received_col.data_type().clone(),
            false,
        ),
        Field::new("value", transformed_value_col.data_type().clone(), true),
        Field::new("status", DataType::Int64, false),
    ]);

    let transformed_record = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            id_col,
            name_col,
            transformed_ts_col,
            transformed_received_col,
            transformed_value_col,
            status_col,
        ],
    )?;

    Ok(RecordMessage::from_record(transformed_record))
}

/// convert ColumnConfig map to RecordTransform map
/// return (point_id, RecordTransform) pairs
fn to_record_transform_map(
    config_map: &HashMap<String, ColumnConfig>,
) -> HashMap<String, RecordTransform> {
    config_map
        .iter()
        .filter(|(_, ts_config)| ts_config.transform.is_some())
        .map(|(point_id, ts_config)| {
            let transform = RecordTransform {
                column_name: ts_config.alias.clone(),
                transform_expression: ts_config.transform.clone(),
            };
            (point_id.clone(), transform)
        })
        .collect()
}

/// get a transformed column by name and data type
/// # Arguments
/// * `col_name` - column name
/// * `col_type` - column data type
/// * `transform_map` - (point_id, transform_expression) pairs
fn transform_by_name(
    record: &RecordBatch,
    col_name: &str,
    transform_map: HashMap<String, RecordTransform>,
) -> anyhow::Result<ArrayRef> {
    let rows = record.num_rows();
    if transform_map.is_empty() || rows == 0 {
        let raw_column = record
            .column_by_name(col_name)
            .ok_or(anyhow::anyhow!(
                "column: {} not exist in record batch",
                col_name
            ))?
            .clone();
        return Ok(raw_column);
    }

    let schema = record.schema();
    let columns = record.columns();
    let id_col_index = schema.index_of("id").unwrap();
    let col_index = schema.index_of(col_name).unwrap();
    let col_type = schema.field(col_index).data_type();

    let mut values: Vec<Dynamic> = Vec::with_capacity(rows);
    for row_index in 0..rows {
        let point_id = columns[id_col_index]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row_index);

        let expression = get_transform_exprssion_by_id(point_id, &transform_map);

        match expression {
            Some((name, expr)) => {
                let mut scope = Scope::new();
                match col_type {
                    DataType::Boolean => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    DataType::Int8 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Int16 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Int32 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Int64 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::UInt8 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::UInt16 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<UInt16Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::UInt32 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::UInt64 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Float16 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Float16Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value.to_f64());
                    }
                    DataType::Float32 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value as f64);
                    }
                    DataType::Float64 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    DataType::Binary => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, String::from_utf8_lossy(value).to_string());
                    }
                    DataType::Utf8 => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value.to_string());
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, None) => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, None) => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                        let value = columns[col_index]
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap()
                            .value(row_index);
                        scope.set_or_push(name, value);
                    }
                    dt => {
                        tracing::warn!(
                            "unsupported data type: {}, expression scope not set",
                            dt.clone()
                        )
                    }
                }
                let engine = Arc::new(Engine::new());
                let ast = engine.compile_expression(&expr)?;
                let new_value: Dynamic = match engine.eval_ast_with_scope(&mut scope, &ast) {
                    Ok(v) => v,
                    Err(_) => rhai::Dynamic::UNIT,
                };
                values.push(new_value);
            }
            None => {
                // no transform expression for this point_id, use raw value
                let value: Dynamic = to_dynamic_value(record, col_type, col_index, row_index)?;
                values.push(value);
            }
        }
    }

    let mut is_none = true;
    for v in &values {
        if !v.is_unit() {
            is_none = false;
        }
    }

    if is_none || values.is_empty() {
        let raw_column = record
            .column_by_name(col_name)
            .ok_or(anyhow::anyhow!(
                "column: {} not exist in record batch",
                col_name
            ))?
            .clone();
        return Ok(raw_column);
    }

    crate::plugins::expr::array_from_rhai_dynamics(values).ok_or(anyhow::anyhow!(
        "failed to transform Vec<Dynamic> to ArrayRef"
    ))
}
fn to_dynamic_value(
    record_batch: &RecordBatch,
    col_type: &DataType,
    col_index: usize,
    row_index: usize,
) -> anyhow::Result<Dynamic> {
    let columns = record_batch.columns();
    let value: Dynamic = match col_type {
        DataType::Boolean => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value)
        }
        DataType::Int8 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Int16 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Int32 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Int64 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::UInt8 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::UInt16 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::UInt32 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::UInt64 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Float16 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Float16Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value.to_f64())
        }
        DataType::Float32 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value as f64)
        }
        DataType::Float64 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value)
        }
        DataType::Binary => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(String::from_utf8_lossy(value).to_string())
        }
        DataType::Utf8 => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value.to_string())
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(row_index);
            Dynamic::from(value)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let value = columns[col_index]
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(row_index);
            rhai::Dynamic::from(value)
        }
        dt => {
            unimplemented!("unsupported data type: {}", dt.clone())
        }
    };

    Ok(value)
}

fn get_transform_exprssion_by_id(
    id: &str,
    map: &HashMap<String, RecordTransform>,
) -> Option<(String, String)> {
    map.get(id).and_then(|transform| {
        match (&transform.column_name, &transform.transform_expression) {
            (Some(name), Some(expr)) => Some((name.clone(), expr.clone())),
            (Some(name), None) => Some((name.clone(), name.clone())),
            _ => None,
        }
    })
}

#[cfg(test)]
mod handle_transform_tests {
    use crate::runners::opc::config::csv::CsvParser;
    use crate::sink::handle_transform;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use std::str::FromStr;
    use std::sync::Arc;
    use taos::Dsn;
    use taosx_ipc::stream::point::RecordMessage;

    #[tokio::test]
    async fn test_handle_transform() {
        let message = RecordMessage::from_record(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Utf8, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new(
                        "ts",
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new(
                        "received",
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("value", DataType::Int32, true),
                    Field::new("status", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec![
                        "ns=3;i=1005",
                        "ns=3;i=1006",
                        "ns=3;i=1007",
                    ])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(
                        TimestampMillisecondArray::from(vec![
                            1700000000000,
                            1700000000000,
                            1700000000000,
                        ])
                        .with_timezone_opt::<&str>(None),
                    ),
                    Arc::new(
                        TimestampMillisecondArray::from(vec![
                            1700000000000,
                            1700000000000,
                            1700000000000,
                        ])
                        .with_timezone_opt::<&str>(None),
                    ),
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int64Array::from(vec![0, 1, 0])),
                ],
            )
            .unwrap(),
        );

        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opcua-utf8bom.csv").unwrap();
        let parser = CsvParser::from_dsn(&dsn).unwrap();
        let model_config = parser.parse().await.unwrap();

        let transformed_msg = handle_transform(&message, &model_config).await.unwrap();

        let value = transformed_msg
            .record()
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(value, vec![33, 12, 3]);

        let ts = transformed_msg
            .record()
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ts, vec![1700000000000, 1700028800000, 1_699_999_994_000]);

        let received = transformed_msg
            .record()
            .column_by_name("received")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(
            received,
            vec![1700028800000, 1700000000000, 1_699_999_994_000]
        );
    }
}

/// 按照 stable_name > {prefix}_{raw_type} > None 的顺序生成 stable_name
fn stable_name(
    stable_name: &Option<String>,
    prefix: &Option<String>,
    raw_type: &IpcDataType,
) -> Option<String> {
    if let Some(stable_name) = stable_name {
        if stable_name.contains("{type}") {
            let stable = match raw_type {
                IpcDataType::VarChar(_len) => stable_name.replace("{type}", "varchar"),
                IpcDataType::NChar(_len) => stable_name.replace("{type}", "nchar"),
                _ => stable_name.replace("{type}", &raw_type.sql_repr().replace(" ", "_")),
            };
            return Some(stable);
        } else {
            return Some(stable_name.clone());
        }
    }

    if let Some(prefix) = prefix {
        let stable_name = match raw_type {
            IpcDataType::VarChar(_len) => format!("{}_varchar", prefix),
            IpcDataType::NChar(_len) => format!("{}_nchar", prefix),
            _ => format!("{}_{}", prefix, raw_type.sql_repr().replace(" ", "_")),
        };
        return Some(stable_name);
    }

    None
}

#[derive(Clone, Debug)]
struct PointInsertion {
    column_configs: Vec<ColumnConfig>,
    tag_configs: Option<Vec<TagConfig>>,
    columns: Vec<(String, String)>, // column_name(original_ts/received_ts/value/quality), column_alias
    value_column_config: Option<ColumnConfig>,
    other_columns: String,
    tags: String,
}

impl PointInsertion {
    fn from_table_config(table_config: &TableConfig, raw_type: &IpcDataType) -> Self {
        let mut columns: Vec<(String, String)> = Vec::new();
        let mut value_column_config = None;
        let mut other_columns = String::new();

        for column_config in &table_config.column_configs {
            if column_config.is_primary_key {
                let primary_key_column_name = column_config.name.clone();
                let primary_key_column_alias = column_config
                    .alias
                    .clone()
                    .unwrap_or(primary_key_column_name.clone());
                columns.insert(
                    0,
                    (primary_key_column_name, primary_key_column_alias.clone()),
                );
                other_columns.insert_str(
                    0,
                    format!("`{primary_key_column_alias}` TIMESTAMP,").as_str(),
                );
            } else {
                let column_name = column_config.name.clone();
                let column_alias = column_config.alias.clone().unwrap_or(column_name.clone());

                columns.push((column_name, column_alias.clone()));

                let column_type = if column_config.r#type.is_some() {
                    column_config.r#type.unwrap().to_string()
                } else {
                    raw_type.sql_repr().clone()
                };

                if column_config.name == ColumnConfig::VALUE {
                    value_column_config = Some(column_config.clone());
                } else {
                    other_columns.push_str(format!("`{column_alias}` {},", column_type).as_str());
                }
            }
        }
        // remove last char
        other_columns.pop();

        // tags
        let tags = if table_config.tag_configs.is_none() {
            "`point_id` VARCHAR(256),`point_name` VARCHAR(256)".to_string()
        } else {
            let tag_configs = table_config.tag_configs.clone().unwrap();
            tag_configs
                .iter()
                .map(|tag| format!("`{}` {}", tag.name, tag.r#type.sql_repr()))
                .collect::<Vec<String>>()
                .join(",")
        };

        Self {
            column_configs: table_config.column_configs.clone(),
            tag_configs: table_config.tag_configs.clone(),
            columns,
            value_column_config,
            other_columns,
            tags,
        }
    }
}

struct SqlInsertion {
    point_insertion: PointInsertion,
    sql: String,
    overflow: bool,
    value_column_type: String,
    modify: ModifyStructForPointMessage,
}

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

    let mut point_config_map = config.point_config_map.clone();
    let mut table_config_map = config.table_config_map.clone();

    let mut points = 0;
    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    for message in record.records() {
        // handle value_transform, ts_transform, rts_transform
        let message = handle_transform(message, config).await?;

        let cv_vec = record_batch_to_column_view(message.record(), target_precision);

        // process id, name, ts, value, status
        let schema = message.schema();
        // id
        let id_index = schema.index_of("id")?;
        let id_column_view = cv_vec.get(id_index).unwrap();
        // name
        let name_index = schema.index_of("name")?;
        let name_column_view = cv_vec.get(name_index).unwrap();
        // ts
        let ts_index = schema.index_of("ts")?;
        let ts_column_view = cv_vec.get(ts_index).unwrap();
        // value
        let value_index = schema.index_of("value")?;
        let value_column_view = cv_vec.get(value_index).unwrap();
        let value_field = schema.field_with_name("value")?;
        let value_raw_type = IpcDataType::from(value_field.data_type());
        // received
        let received_index = schema.index_of("received")?;
        let received_column_view = cv_vec.get(received_index).unwrap();
        // status
        let status_index = schema.index_of("status")?;
        let status_column_view = cv_vec.get(status_index).unwrap();

        // stable: Vec<insert_sql, sql length overflow?, value_column_type, modify_message>
        let mut stable_insert_map: HashMap<String, Vec<SqlInsertion>> = HashMap::new();
        // child_table_name: create_sql
        let mut child_table_create_sql_map: HashMap<String, HashMap<String, String>> =
            HashMap::new();

        for i in 0..id_column_view.len() {
            let point_id = id_column_view
                .get(i)
                .unwrap()
                .into_value()
                .to_string()
                .unwrap();

            let mapping = config.get_point_mapping(&point_id)?;
            if mapping.is_none() {
                // 如果在一开始的 modelConfig 中找不到点位对应的 PoingConfig 和 TableConfig，则尝试使用规则生成
                tracing::warn!(
                    "point mapping not found and try to auto generate, point_id: {}",
                    point_id
                );
                let mapping = config
                    .generate_point_mapping(&point_id, &value_raw_type)
                    .await;
                match mapping {
                    Err(err) => {
                        tracing::warn!(
                            "failed to generate point mapping with point_id: {}, cause: {:?}",
                            point_id,
                            err
                        );
                        continue;
                    }
                    Ok((p, t)) => {
                        tracing::debug!(
                            "generate point mapping, point config: {:?}, table config: {:?}",
                            p,
                            t
                        );
                        point_config_map.insert(point_id.clone(), p);
                        table_config_map.insert(point_id.clone(), t);
                    }
                }
            }
            let point_config = point_config_map.get(&point_id).unwrap();
            let table_config = table_config_map.get(&point_id).unwrap();

            // stable_name
            let stable_name = stable_name(
                &point_config.stable,
                &table_config.stable_prefix,
                &value_raw_type,
            )
            .ok_or(anyhow::anyhow!(
                "failed to get stable name, point_id: {}, point_config: {:?}, table_config: {:?}",
                point_id,
                point_config,
                table_config
            ))?;

            // tbname
            let child_table_name = point_config.code.to_string();

            // point_insertion
            let point_insertion = PointInsertion::from_table_config(table_config, &value_raw_type);

            let mut value_column_name = "value";
            let mut value_column_length = 0;
            let mut values = String::new();
            let mut columns_in_insert = String::new();
            for (temp_name, temp_alias) in &point_insertion.columns {
                if temp_name == "received_ts" || temp_name == "received_time" {
                    values.push_str(
                        format!(
                            "{},",
                            received_column_view
                                .slice(i..i + 1)
                                .unwrap()
                                .get(0)
                                .unwrap()
                                .into_value()
                                .to_sql_value()
                        )
                        .as_str(),
                    );
                } else if temp_name == "original_ts" || temp_name == "original_time" {
                    values.push_str(
                        format!(
                            "{},",
                            ts_column_view
                                .slice(i..i + 1)
                                .unwrap()
                                .get(0)
                                .unwrap()
                                .into_value()
                                .to_sql_value()
                        )
                        .as_str(),
                    );
                } else if temp_name == "value" {
                    let value_column = value_column_view
                        .slice(i..i + 1)
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .into_value()
                        .to_sql_value()
                        .replace("NaN", "NULL");
                    values.push_str(format!("{value_column},").as_str());
                    value_column_name = temp_alias;
                    value_column_length = cmp::max(value_column.len(), value_column_length);
                } else if temp_name == "quality" {
                    values.push_str(
                        format!(
                            "{},",
                            status_column_view
                                .slice(i..i + 1)
                                .unwrap()
                                .get(0)
                                .unwrap()
                                .into_value()
                                .to_sql_value()
                        )
                        .as_str(),
                    );
                }
                columns_in_insert.push_str(format!("`{temp_alias}`,").as_str());
            }
            // remove last `,` in sql
            values.pop();
            columns_in_insert.pop();

            let point_name = name_column_view
                .slice(i..i + 1)
                .unwrap()
                .get(0)
                .unwrap()
                .to_sql_value();
            let mut tag_names = String::new();
            let mut tag_values = String::new();
            if table_config.tag_configs.is_some() {
                // let mut index = 0;
                for ele in table_config.tag_configs.as_ref().unwrap() {
                    let tag_name = ele.name.clone();
                    tag_names.push_str(format!("`{}`,", tag_name).as_str());
                    let value = point_config
                        .tag_values
                        .as_ref()
                        .unwrap()
                        .get(&tag_name)
                        .unwrap();
                    let value = match ele.r#type {
                        IpcDataType::VarChar(_) | IpcDataType::NChar(_) | IpcDataType::Json => {
                            format!("\"{value}\"")
                        }
                        _ => value.to_string(),
                    };
                    tag_values.push_str(format!("{},", value.replace("NaN", "NULL")).as_str());
                }
                tag_names.pop();
                tag_values.pop();
            }

            if tag_names.is_empty() {
                if child_table_create_sql_map.contains_key(&stable_name) {
                    let map = child_table_create_sql_map.get_mut(&stable_name).unwrap();
                    map.insert(
                        child_table_name.clone(),
                        format!(
                            "(`point_id`, `point_name`) TAGS (\"{point_id}\", {})",
                            &point_name
                        ),
                    );
                } else {
                    let mut map = HashMap::new();
                    map.insert(
                        child_table_name.clone(),
                        format!(
                            "(`point_id`, `point_name`) TAGS (\"{point_id}\", {})",
                            &point_name
                        ),
                    );
                    child_table_create_sql_map.insert(stable_name.clone(), map);
                }
            } else if child_table_create_sql_map.contains_key(&stable_name) {
                let map = child_table_create_sql_map.get_mut(&stable_name).unwrap();
                map.insert(
                    child_table_name.clone(),
                    format!("({}) TAGS ({})", tag_names, tag_values),
                );
            } else {
                let mut map = HashMap::new();
                map.insert(
                    child_table_name.clone(),
                    format!("({}) TAGS ({})", tag_names, tag_values),
                );
                child_table_create_sql_map.insert(stable_name.clone(), map);
            }

            let sql_vec = stable_insert_map.get_mut(&stable_name);
            let mut insert_done = false;

            if sql_vec.is_none() {
                let sql = format!(
                    "insert into `{}` ({}) VALUES ({})",
                    child_table_name,
                    columns_in_insert.as_str(),
                    values
                );

                let value_column_type = if point_config.value_type.is_some() {
                    // maybe should replace value column type
                    point_config.value_type.clone().unwrap().sql_repr()
                } else {
                    value_raw_type.sql_repr().clone()
                };

                let mut sql_vec = Vec::new();
                sql_vec.push(SqlInsertion {
                    point_insertion: point_insertion.clone(),
                    sql,
                    overflow: false,
                    value_column_type,
                    modify: ModifyStructForPointMessage {
                        id: point_id,
                        point_name,
                        value_column_name: value_column_name.to_string(),
                        value_column_length,
                    },
                });
                stable_insert_map.insert(stable_name.clone(), sql_vec);
            } else {
                // 这部分是拼多个点位的sql，注意：需要合并 columnConfig, 合并modify
                let sql_vec = sql_vec.unwrap();

                for index in 0..sql_vec.len() {
                    let sql_insertion = sql_vec.get_mut(index).unwrap();
                    if sql_insertion.overflow {
                        continue;
                    } else {
                        let sql_suffix = format!(
                            " `{child_table_name}` ({}) VALUES ({}) ",
                            columns_in_insert.as_str(),
                            values
                        );
                        if sql_insertion.sql.len() + sql_suffix.len() > 1000 * 1000 {
                            sql_insertion.overflow = true;
                            continue;
                        } else {
                            // 不同点位入同一张表的情况，需要合并column_configs
                            let exist_column_configs =
                                &mut sql_insertion.point_insertion.column_configs;
                            let column_configs = &table_config.column_configs;
                            for column_config in column_configs {
                                if !exist_column_configs.contains(column_config) {
                                    exist_column_configs.push(column_config.clone());
                                }
                            }
                            // 需要更新 modify.value_column_length
                            let exist_value_column_length =
                                sql_insertion.modify.value_column_length;
                            sql_insertion.modify.value_column_length =
                                cmp::max(exist_value_column_length, value_column_length);

                            sql_insertion.sql.push_str(sql_suffix.as_str());
                            insert_done = true;
                        }
                    }
                }

                if !insert_done {
                    let value_column_type = if point_config.value_type.is_some() {
                        // maybe should replace value column type
                        point_config.value_type.clone().unwrap().sql_repr()
                    } else {
                        value_raw_type.sql_repr().clone()
                    };
                    let sql = format!(
                        "insert into `{}` ({}) VALUES ({})",
                        child_table_name,
                        columns_in_insert.as_str(),
                        values
                    );

                    sql_vec.push(SqlInsertion {
                        point_insertion: point_insertion.clone(),
                        sql,
                        overflow: false,
                        value_column_type,
                        modify: ModifyStructForPointMessage {
                            id: point_id,
                            point_name,
                            value_column_name: value_column_name.to_string(),
                            value_column_length,
                        },
                    });
                }
            }
        }

        for (stable_name, sql_vec) in stable_insert_map {
            for sql_insertion in sql_vec {
                debug!("point message insert sql len: {}", sql_insertion.sql.len());
                tracing::trace!("sql>>>{}", sql_insertion.sql);

                let mut retry = 0;
                let mut break_err = Ok(());
                'outer: loop {
                    if retry >= 5 {
                        tracing::warn!(error = ?break_err, "sql error cannot be solved, break;");
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
                                "Insert point record error"
                            );

                            if errstr.contains("[0x2603]") || errstr.contains("0x0200") {
                                // 超级表或子表不存在, 创建超级表
                                let value_column_config = sql_insertion
                                    .point_insertion
                                    .value_column_config
                                    .as_ref()
                                    .unwrap();
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
                                            tracing::debug!("error encountered, ignore: {err:#}",);
                                        } else {
                                            tracing::warn!(
                                                "create stable {stable_name} error: {err:#}"
                                            );
                                            let err_str = err.to_string();
                                            if err_str.contains("0xE00") {
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
                                    tracing::info!("create child sql: {create_child_sql}");
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
                                                // Object is creating, maybe should ignore
                                                tracing::warn!("create table sql encounter 0x032C");
                                            } else if err_str.contains("0xE00") {
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
                                // Illegal number of columns or tags, alter to add columns or tag
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
                                                0x0E001..=0x0E003 => {
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
                                    let column_real_name = get_real_column_name(column_config);
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
                                                    tracing::warn!(
                                                        "create table sql encounter 0x032C"
                                                    );
                                                }
                                                0x0E001..=0x0E003 => {
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
                                // column or tag length not enough
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
                                    if (column_meta.ty == Ty::VarChar
                                        || column_meta.ty == Ty::NChar)
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
                            } else if errstr.contains("[0xE002]") || errstr.contains("[0xE003]") {
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

        metrics.add_processed_rows(message.record().num_rows() as u64);
    }
    Ok(points)
}

#[inline]
fn get_real_column_name(column_config: &ColumnConfig) -> &String {
    column_config.alias.as_ref().unwrap_or(&column_config.name)
}

const DEFAULT_MAX_RETRIES_FOR_CONNECTION: u32 = 10;

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
    record: &FlatMessage,
    count: &mut usize,
    cancel: &CancellationToken,
    parser: &Parser,
    target_precision: taos::Precision,
    metrics: &IpcMetrics,
    notifier: Option<&crate::TaskNotifySender>,
) -> anyhow::Result<()> {
    if cancel.is_cancelled() {
        tracing::warn!("Task is cancelled");
        return Ok(());
    }
    if taos.is_none() {
        taos.replace(pool.get().await?);
    }

    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    // let stmt = Stmt::init(taos.as_ref().unwrap())?;
    let mut max_lengths = HashMap::new();
    for message in record.records() {
        let batch = message.record();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        let instant = std::time::Instant::now();
        let batch = parser
            .parse_message_from_records(batch, true)
            .context("Transformer parse error")?;
        if tracing::event_enabled!(tracing::Level::TRACE) {
            let elapsed = instant.elapsed();
            tracing::trace!(cost = ?elapsed, "Parse message elapsed: {:?}", elapsed);
        }
        match batch {
            crate::plugins::transform::Message::Raw(_) => todo!(),
            crate::plugins::transform::Message::Tables(_) => todo!(),
            crate::plugins::transform::Message::ChildTables(_) => todo!(),
            crate::plugins::transform::Message::Records(mut message) => {
                if message.is_empty() {
                    continue;
                }
                if unsafe { crate::global::DRY_RUN } {
                    *count += num_rows;
                    metrics.add_processed_rows(num_rows as u64);
                    continue;
                }
                let factor = message
                    .iter()
                    .map(|message| message.records.num_rows())
                    .sum::<usize>()
                    / message.len();
                let res = if factor < 200 {
                    flat_write_with_sql(
                        pool,
                        taos,
                        target_precision,
                        &message,
                        metrics,
                        notifier,
                        cancel,
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
                    )
                    .in_current_span()
                    .await
                };
                match res {
                    Ok(n) => {
                        *count += n;
                        metrics.add_processed_rows(num_rows as u64);
                    }
                    Err(err) => {
                        let errstr = format!("{:#}", err);
                        if errstr.contains("Timestamp data out of range") {
                            qid.add_sub_batch_id();
                            tracing::warn!("Contains invalid timestamp, filter out them");
                            // filter timestamp.
                            let min = get_minimum_timestamp(
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

                            let rows_after: usize =
                                message.iter().map(|m| m.records.num_rows()).sum();

                            let filtered = rows - rows_after;
                            tracing::info!(
                                rows,
                                filtered,
                                after = rows_after,
                                "Filter out records"
                            );
                            metrics.add_drained_rows(filtered as _);

                            if message.is_empty() {
                                continue;
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
                                )
                                .in_current_span()
                                .await
                            }?;
                            *count += n;
                            metrics.add_processed_rows(num_rows as u64);
                        } else {
                            return Err(err);
                        }
                    }
                }
                metrics.add_processed_rows(num_rows as u64);
            }
        }
    }
    Ok(())
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
        metrics.add_received_batches(1);
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
                .send(crate::TaskNotify::Error(format!("{:#}", err)))
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
            tracing::info!(acks = acks.load(Ordering::SeqCst), "ack done");
        }
        acks.fetch_add(1, Ordering::SeqCst);
        metrics.add_processed_batches(1);
        drop(taos);
    }
    println!("finished, totally {count} rows");
    Ok(())
}

#[instrument(skip_all)]
async fn ipc_point_reader<R: Read + Send + 'static, W: Write>(
    pool: &TaosPool,
    ipc_reader: IpcReader<R>,
    ipc_ack_writer: AckWriter<W>,
    config: Option<OpcModelConfig>,
    _license: Option<&ConnectorLicense>,
    _transferred: Option<&Transferred>,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    _ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
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
        record: Result<Box<dyn IpcMessage>, arrow::error::ArrowError>,
        metrics: &IpcMetrics,
    ) -> anyhow::Result<usize> {
        let record = record?;
        let pool = &context.pool;
        let taos = context.pool.get().await?;
        let mut count = 0;
        let mut taos = Some(taos);
        let record = *Box::<dyn Any>::downcast::<PointMessage>(unsafe {
            std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
        })
        .unwrap();
        let n = consume_point_record(
            pool,
            &mut taos,
            &record,
            &mut count,
            context.config.as_ref().unwrap(),
            context.target_precision,
            metrics,
        )
        .await?;
        Ok(n)
    }

    let context = WriterContext {
        pool: pool.clone(),
        config: config.map(Arc::new),
        target_precision,
    };
    let ipc_ack_writer = Arc::new(Mutex::new(ipc_ack_writer));
    let qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    ipc_reader
        .into_stream()
        .for_each_concurrent(48, |record| {
            let context = context.clone();
            let ipc_ack_writer = ipc_ack_writer.clone();
            let count = count.clone();
            let notifier = notifier.clone();
            let batch_counter = batch_counter.clone();
            let mut qid = qid.clone();
            debug!("Writing batch");
            let metrics_arc_clone = metrics_arc.clone();
            async move {
                if let Some(batch_counter) = batch_counter {
                    let batch_number = batch_counter.next().await.unwrap_or_default();
                    qid.set_batch_id(batch_number);
                }
                let metrics = metrics_arc_clone.ipc();
                metrics.add_received_batches(1);
                let n = parse(context, record, metrics).await;
                match n {
                    Ok(n) => {
                        metrics.add_processed_batches(1);
                        let _ = ipc_ack_writer.lock().await.write_ok();
                        count.fetch_add(n, Ordering::SeqCst);
                    }
                    Err(err) => {
                        metrics.add_failed_batches(1);
                        tracing::warn!("Writing batch error: {err:#}");
                        let _ = notifier.send(crate::TaskNotify::Error(format!("{:#}", err)));
                        let _ = ipc_ack_writer.lock().await.ack(LushAck {
                            code: 0,
                            message: Some(err.to_string()),
                            context: None,
                        });
                    }
                }
            }
            .in_current_span()
        })
        .await;
    println!(
        "IPC stream finished, total {} records in this stream",
        count.load(Ordering::SeqCst)
    );
    Ok(())
}

#[framed]
async fn ipc_flat_stream_worker(
    pool: &TaosPool,
    stream: impl Stream<Item = Result<Box<dyn IpcMessage>, ArrowError>> + Unpin,
    sink: impl Sink<LushAck, Error = ArrowError> + Send + 'static,
    cancel: CancellationToken,
    parser: Option<&Parser>,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
) -> anyhow::Result<()> {
    let parser = parser.ok_or_else(|| anyhow::anyhow!("Parser should be set with flat stream"))?;
    tokio::pin!(stream);

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
    _license: Option<&ConnectorLicense>,
    _transferred: Option<&Transferred>,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
) -> anyhow::Result<()> {
    let stream = ipc_reader.into_stream();
    let sink = futures_util::sink::unfold(ipc_ack_writer, |mut ack_writer, ack| async move {
        ack_writer.ack(ack).map_err(|err| {
            error!("Write ack error: {err:#}");
            err
        })?;
        info!("Ack done");
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

async fn get_current_precision(conn: &Taos) -> anyhow::Result<taos::Precision> {
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
    _lock: Arc<Mutex<()>>,
    opc_model_config: Option<OpcModelConfig>,
    lush_model_config: Option<LushModelConfig>,
    cancel: CancellationToken,
    parser: Option<Parser>,
    connector: Option<&str>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    batch_counter: Option<BatchCounter>,
    notifier: crate::TaskNotifySender,
) -> anyhow::Result<()> {
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

    let license: Option<ConnectorLicense> = None;

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
    match stream_type {
        StreamType::Line => todo!(),
        StreamType::Flat => ipc_flat_stream_reader(
            &pool,
            ipc_reader,
            ipc_ack_writer,
            cancel,
            parser.as_ref(),
            license.as_ref(),
            transferred.as_deref(),
            target_precision,
            notifier,
            ipc_error_strategy,
            metrics_arc.clone(),
            batch_counter,
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
            &metrics_arc,
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
            license.as_ref(),
            transferred.as_deref(),
            target_precision,
            notifier,
            ipc_error_strategy,
            metrics_arc.clone(),
            batch_counter,
        )
        .await
        .inspect_err(|err| {
            tracing::error!("IPC stream error: {err:#}");
        }),
    }
}

#[instrument(skip_all)]
pub async fn handle_point_message_init(config: &OpcModelConfig, taos: &Taos) -> anyhow::Result<()> {
    let point_config_map = &config.point_config_map;
    let table_config_map = &config.table_config_map;

    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    for point_id in point_config_map.keys() {
        let table_config = table_config_map.get(point_id).unwrap();
        if table_config.enabled == Some(0i8) {
            let tbname = point_config_map
                .get(point_id)
                .ok_or(anyhow::anyhow!(
                    "point_id: {} not exist in point config map",
                    point_id
                ))?
                .code
                .clone();
            let drop_sql = format!("DROP TABLE IF EXISTS {}", tbname);
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
                let message = self.parser.parse(record)?;
                let mut count = 0;
                let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
                    std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(message)
                })
                .unwrap();
                let mut taos = Some(self.pool.get().await?);
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
    socket: impl AsRef<str>,
    cancel: CancellationToken,
    with_agent: (i64, String, String),
    config: Option<OpcModelConfig>,
) -> anyhow::Result<IpcHandler> {
    let addr = socket.as_ref();

    let (sender, error_receiver) = tokio::sync::mpsc::channel(1);

    let socket = tokio::net::TcpSocket::new_v4()?;
    let addr: SocketAddr = addr.parse()?;
    socket.bind(addr)?;
    let socket = socket.listen(65535)?;

    let batch_counter = BatchCounter::new(with_agent.0 as u16).await?;

    // let (closer, mut receiver) = tokio::sync::mpsc::channel::<()>(1);
    // let closed = Arc::new(AtomicBool::new(false));
    // let closed2 = closed.clone();

    let notify = Arc::new(tokio::sync::Notify::new());
    let notified = notify.clone();
    let thread = tokio::spawn(
        async move {
            let mut handlers = vec![];
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

                tokio::spawn(async move {
                    let res =
                        ipc_tcp_forward(stream, cancel, remote, token, id, batch_counter, config).in_current_span().await;
                    if let Err(err) = res {
                        let error_msg = format!("{:?}", err);
                        if error_msg.contains("os error 10060") {
                            tracing::warn!("IPC reader stopped with warn: {}", error_msg);
                        } else {
                            tracing::error!("{:?}", err);

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
                    accept = socket.accept() => {
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
    Ok(IpcHandler::new(notify, handle, error_receiver))
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

    pub async fn send<T>(&self, _: T) -> Result<(), tokio::sync::mpsc::error::SendError<()>> {
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
    socket: impl AsRef<str>,
    opc_model_config: Option<OpcModelConfig>,
    lush_model_config: Option<LushModelConfig>,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    parser: Option<Parser>,
    connector: Option<&'static str>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notifier: crate::TaskNotifySender,
) -> anyhow::Result<IpcHandler> {
    let (sender, error_receiver) = tokio::sync::mpsc::channel(1);

    let addr = socket.as_ref();
    let socket = tokio::net::TcpSocket::new_v4()?;
    let addr: SocketAddr = addr.parse()?;
    socket.bind(addr)?;
    let socket = socket.listen(65535)?;

    info!("listen on socket address: {addr}");
    let sql_lock = Arc::new(Mutex::new(()));
    let socket = Arc::new(socket);
    let notify = Arc::new(tokio::sync::Notify::new());
    let notified = notify.clone();

    let mut batch_counter = None;
    if let Some((task_id, _, _)) = with_agent.as_ref() {
        batch_counter = Some(BatchCounter::new(*task_id as u16).await?);
    } else if let Some(task_id) = task_id.as_ref() {
        batch_counter = Some(BatchCounter::new(*task_id as u16).await?);
    };

    let thread = tokio::task::spawn(
        async move {
            info!("waiting for IPC connections");
            let cancel = cancel.child_token();
            let server = addr;
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
                    set.spawn(async move {
                        let res = ipc_tcp_forward(stream, cancel, server, token, id, batch_counter, opc_model_config).in_current_span().await;
                        if let Err(err) = res {
                            tracing::error!("ipc read err: {:#}", err);
                            let _ = se.send(format!("{:#}", err)).await;
                        }
                    })
                } else {
                    let pool = target.clone();
                    let lock = sql_lock.clone();
                    let opc_model_config = opc_model_config.clone();
                    let lush_model_config = lush_model_config.clone();
                    let parser = parser.clone();
                    let transferred = transferred.clone();
                    let notifier = notifier.clone();
                    let notify = notified.clone();
                    let batch_counter = batch_counter.clone();
                    set.spawn(async move {
                        // let dsn: Dsn = "taos:///db2".parse().unwrap();
                        // let pool = TaosBuilder::from_dsn(dsn).unwrap().pool().unwrap();
                        info!("Spawned IPC reader");
                        let cancel2 = cancel.clone();
                        let res = ipc_tcp_read(
                            pool,
                            stream,
                            lock,
                            opc_model_config,
                            lush_model_config,
                            cancel,
                            parser,
                            connector,
                            transferred,
                            task_id,
                            batch_counter,
                            notifier,
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
                }
            }
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
    Ok(IpcHandler::new(notify, handle, error_receiver))
}

#[instrument(skip_all)]
pub async fn channel_based_transformer(
    target: TaosPool,
    cancel: CancellationToken,
    parser: Option<Parser>,
    connector: Option<&'static str>,
    task_id: Option<i64>,
    notifier: crate::TaskNotifySender,
) -> anyhow::Result<(
    flume::Sender<Result<Box<dyn IpcMessage>, ArrowError>>,
    flume::Receiver<LushAck>,
)> {
    let taos = target.get().await?;
    let target_precision = get_current_precision(&taos).in_current_span().await?;
    let (msg_tx, msg_rx) = flume::bounded(32);
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
    tokio::spawn(
        async move {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("IPC stream cancelled");
                }
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
                        get_metrics_arc_from_i64(task_id).await,
                        batch_counter,
                    )
                    .in_current_span()
                    .await
                } => {}
            }
        }
        .in_current_span(),
    );
    Ok((msg_tx, ack_rx))
}
