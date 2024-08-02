use std::sync::{Arc, Weak};

use anyhow::{bail, Context};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use arrow_flight::{decode::DecodedFlightData, FlightData, PutResult};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use tonic::{Status, Streaming};
use tracing::{instrument, Instrument, Span};
use zerocopy::{AsBytes as _, FromBytes};

use taosx_core::sink::handle_point_message_init;
use taosx_core::{
    core_metrics::get_metrics,
    sink::{
        handle_lush_message_init, lush::TableTagCache, IpcErrorStrategy, MessageMetadata,
        RPC_ACK_PROCESSED, RPC_ACK_RECEIVED, RPC_ACK_STREAM_END,
    },
    utils::{
        breakpoints::BreakpointDb,
        get_main_version_from_server_version, get_server_version,
        trace::{set_data_trace_id_for_current_span, RequestID, TraceDataId, TraceStreamId},
    },
    ConnectorLicense, IpcStreamWorker, Parser,
};

use crate::serve::{
    controller::{transferred::ConnectorTransferred, TaskActivity, TaskControllerRef, TaskDetail},
    scheduler::agent::{AgentNotifySender, AgentSpawnSender},
};

#[derive(Debug)]
pub struct PutStream {
    req: Streaming<FlightData>,
    controller: TaskControllerRef,
    task_id: i64,
    remote: Option<std::net::SocketAddr>,
    notify_sender: AgentNotifySender,
    stream_trace_id: TraceStreamId,
    spawn_sender: AgentSpawnSender,
    cluster_id: i64,
    agent_id: i64,
}

// type PutStreamReceiver = flume::Receiver<Result<PutResult, Status>>;
type PutStreamInner = flume::r#async::RecvStream<'static, Result<PutResult, Status>>;
// type PutStreamSender = flume::Sender<Result<PutResult, Status>>;
type PutStreamBatchSender = flume::Sender<(RecordBatch, TraceDataId)>;
type PutStreamAbortReceiver = flume::Receiver<Result<PutResult, Status>>;
type PutStreamChannel = (
    Arc<PutStreamBatchSender>,
    PutStreamAbortReceiver,
    Arc<tokio::sync::Notify>,
);
// (
//     PutStreamSender,
//     PutStreamReceiver,
//     Arc<PutStreamBatchSender>,
//     Arc<tokio::sync::Notify>,
// );
// type PutStreamAbortSender = tokio::sync::oneshot::Sender<()>;

#[derive(Serialize, Deserialize)]
struct AppMetadata {
    data_trace_id: u64,
}

// lazy_static! {
//     static ref IPC_STREAM_CACHE: Arc<RwLock<HashMap<TraceStreamId, PutStreamChannel>>> =
//         Arc::new(RwLock::new(HashMap::new()));
// }

// pub async fn get_ipc_stream_channel(trace_id: TraceStreamId) -> Option<PutStreamChannel> {
//     let mut cache = IPC_STREAM_CACHE.write().await;
//     cache.remove(&trace_id)
// }

// pub async fn put_ipc_stream_channel(trace_id: TraceStreamId, channel: PutStreamChannel) {
//     let mut cache = IPC_STREAM_CACHE.write().await;
//     cache.insert(trace_id, channel);
// }

async fn ipc_stream_writer(
    notify_sender: AgentNotifySender,
    agent_id: i64,
    task: TaskDetail,
    pool: taos::TaosPool,
    lock: Arc<tokio::sync::Mutex<()>>,
    schema: Arc<arrow::datatypes::Schema>,
    tx: Weak<flume::Sender<(arrow::record_batch::RecordBatch, TraceDataId)>>,
    rx: flume::Receiver<(arrow::record_batch::RecordBatch, TraceDataId)>,
    // rsp_tx: flume::Sender<anyhow::Result<()>>,
    license: Option<ConnectorLicense>,
    _transferred: Option<Arc<ConnectorTransferred>>,
    lush_table_cache: Option<Arc<TableTagCache>>,
    breakpoint_db: Option<BreakpointDb>,
    span: tracing::Span,
    abort_message_tx: flume::Sender<Result<PutResult, Status>>,
    ipc_error_strategy: IpcErrorStrategy,
    stream_trace_id: TraceStreamId,
    notify: Arc<tokio::sync::Notify>,
) -> anyhow::Result<()> {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let _drop_guard = cancellation.clone().drop_guard();
    tokio::spawn(async move {
        cancellation.cancelled().await;
        notify.notify_waiters();
    });
    // dbg!(&task);
    notify_sender.send(crate::serve::scheduler::agent::AgentNotify::TaskActivity(
        agent_id,
        TaskActivity::ipc_started(task.id),
    ))?;
    let task_id = task.id;
    let from = task.from.parse().unwrap();
    let taos = pool.get().await?;
    let worker = IpcStreamWorker::new(
        pool.clone(),
        from,
        lock,
        schema,
        license,
        None,
        lush_table_cache,
        breakpoint_db,
        span,
        Some(task_id),
    )
    .in_current_span()
    .await?;
    let parser: Option<Arc<Parser>> = task
        .parser
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()).unwrap())
        .map(Arc::new);
    let metadata = worker.parser.metadata();
    let metrics_arc = get_metrics(task.id).await.expect("metrics not found");
    let metrics = metrics_arc.ipc();
    if worker.lush_model_config.get().is_none() {
        if let Some(sql) = metadata.init_sql_string() {
            let init = metadata.init().unwrap();
            let req_id = RequestID::new(stream_trace_id.as_u64());
            handle_lush_message_init(init, &taos, &sql, &req_id, metrics).await?;
        }
    }
    // handle point message init
    if let Some(opc_model_config) = worker.opc_model_config() {
        handle_point_message_init(opc_model_config, &taos).await?;
    }

    if let Some(init) = metadata.init() {
        tracing::info!("Start IPC stream writer for stable: {}", init.name());
    } else {
        tracing::info!("Start IPC stream writer");
    }

    let stream = rx.stream();

    use futures::StreamExt;

    const MAX_GRPC_WORKERS_CONCURRENCY: usize = 8;
    let limit = std::env::var("GRPC_WORKERS_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            std::thread::available_parallelism()
                .map(|v| (v.get() / 2).max(MAX_GRPC_WORKERS_CONCURRENCY))
                .ok()
        })
        .unwrap_or(MAX_GRPC_WORKERS_CONCURRENCY);
    tracing::info!("Start IPC stream writer with concurrency limit: {}", limit);
    let contiguous_errors = Arc::new(std::sync::atomic::AtomicU32::new(0));

    // only for lush stream supported transform.
    let tables_messages_in_progress = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    if let Err(err) = stream
        .map(|(record, trace_id)| {
            anyhow::Ok((
                record,
                trace_id,
                &worker,
                &parser,
                &notify_sender,
                &tx,
                &abort_message_tx,
                &contiguous_errors,
                &metrics_arc,
                &tables_messages_in_progress,
            ))
        })
        .try_for_each_concurrent(
            limit,
            |(
                record,
                trace_id,
                worker,
                parser,
                notify_sender,
                tx,
                abort_message_tx,
                contiguous_errors,
                metrics_arc,
                tables_messages_in_progress,
            )| {
                async move {
                    tracing::info!("Writing batch {trace_id}");
                    if let Err(err) = worker
                        .process_record(
                            record.clone(),
                            parser.as_deref(),
                            trace_id,
                            metrics,
                            metrics_arc,
                            tables_messages_in_progress,
                            None,
                        )
                        .in_current_span()
                        .await
                    {
                        let last_errors =
                            contiguous_errors.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        metrics.add_failed_batches(1);
                        tracing::error!(
                            continuous_errors = last_errors,
                            error = format!("{:#}", err),
                            backtrace = %err.backtrace(),
                            "Writing batch error {}",
                            trace_id,
                        );
                        let period = match last_errors {
                            errors if errors < 8 => 8,
                            errors if errors < 16 => 16,
                            errors if errors < 32 => 32,
                            errors if errors < 64 => 64,
                            _ => 128,
                        };
                        tokio::time::sleep(std::time::Duration::from_millis(period * 80)).await;
                        let message = format!("IPC processing record {trace_id} error: {err:#}");
                        let _ = notify_sender.send(
                            crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                                agent_id,
                                TaskActivity::warn(task_id, message),
                            ),
                        );
                        if ipc_error_strategy.will_stop() || last_errors > 10 {
                            let _ = abort_message_tx.send(Err(Status::cancelled(format!(
                                "IPC worker will be stopped since {err:#}"
                            ))));
                            let _ = notify_sender.send(
                                crate::serve::scheduler::agent::AgentNotify::WriterError(
                                    agent_id,
                                    task_id,
                                    format!("{err:#}"),
                                ),
                            );
                            bail!("IPC worker will be stopped since {err:#}");
                        }
                        if let Some(tx) = tx.upgrade() {
                            tracing::warn!(
                                trace_id = %trace_id,
                                "Re-queue with {} rows record",
                                record.num_rows()
                            );
                            tx.send_async((record, trace_id))
                                .await
                                .context("Re-queue error")?;
                        } else {
                            tracing::warn!(
                                trace_id = %trace_id,
                                "IPC channel is closed, cannot re-queue record {trace_id}"
                            );
                        }
                    } else {
                        metrics.add_processed_batches(1);
                        let last_errors =
                            contiguous_errors.load(std::sync::atomic::Ordering::SeqCst);
                        if last_errors > 0 {
                            tracing::info!(
                                continuous_errors = last_errors,
                                "Rescue from {} continuous errors",
                                last_errors
                            );
                            let _ = notify_sender.send(
                                crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                                    agent_id,
                                    TaskActivity::info(
                                        task_id,
                                        format!("Rescue from {} continuous errors", last_errors),
                                        "running",
                                    ),
                                ),
                            );
                            contiguous_errors.store(0, std::sync::atomic::Ordering::SeqCst);
                        } else {
                            tracing::debug!("Writing batch success {}", trace_id);
                        }
                    }
                    Ok(())
                }
                .in_current_span()
            },
        )
        .in_current_span()
        .await
    {
        tracing::warn!("Receiving finished with error: {err:#}")
    }

    tracing::info!(task.id = task_id, "IPC stream writer finished");

    Ok(())
}

async fn spawn_stream_writer(
    stream_trace_id: TraceStreamId,
    cluster_id: i64,
    task_id: i64,
    agent_id: i64,
    controller: &TaskControllerRef,
    notify_sender: AgentNotifySender,
    spawn_sender: AgentSpawnSender,
    schema: Arc<Schema>,
) -> anyhow::Result<PutStreamChannel> {
    let task = controller
        .get(task_id)
        .await
        .map_err(|err| Status::internal(err.to_string()))
        .unwrap()
        .unwrap();
    // dbg!(&task);
    let builder = TaosBuilder::from_dsn(&task.to)?;
    let pool = builder.pool()?;
    let lock = Arc::new(tokio::sync::Mutex::new(()));

    let from_dsn: Dsn = task.from.parse()?;
    let to_dsn: Dsn = task.to.parse()?;

    let connector = match from_dsn.driver.as_str() {
        "opcda" => Some("opc_da"),
        "opcua" => Some("opc_ua"),
        "pi" => Some("pi"),
        "pibackfill" => Some("pi"),
        "influxdb" => Some("influxdb"),
        "opentsdb" => Some("opentsdb"),
        taosx_core::runners::kafka::KAFKA_ID => Some("kafka"),
        taosx_core::runners::historian::AVEVA_HISTORIAN_ID => Some("avevahistorian"),
        "mqtt" => Some("mqtt"),
        _ => None,
    };

    // data channel
    let (tx, rx) = match from_dsn.driver.as_str() {
        "pi" | "pibackfill" => flume::bounded(50),
        _ => flume::bounded(1024),
    };

    let (lush_table_cache, breakpoint_db) = match from_dsn.driver.as_str() {
        "pi" | "pibackfill" => {
            let task_lush_table_cache_lock = controller.scheduler.lush_table_cache.clone();
            let mut task_lush_table_cache = task_lush_table_cache_lock.write().await;
            let lush_table_cache = if task_lush_table_cache.contains_key(&task_id) {
                tracing::info!("Got existing lush_table_cache");
                Some(task_lush_table_cache.get(&task_id).unwrap().clone())
            } else {
                tracing::info!("Create new lush_table_cache");
                let table_tag_cache = Arc::new(TableTagCache::new());
                task_lush_table_cache.insert(task_id, table_tag_cache.clone());
                Some(table_tag_cache)
            };
            let task_breakpoint_db_lock = controller.scheduler.task_breakpoint_db.clone();
            let mut task_breakpoint_db = task_breakpoint_db_lock.write().await;
            let breakpoint_db = if task_breakpoint_db.contains_key(&task_id) {
                tracing::info!("Got existing breakpoint_db");
                Some(task_breakpoint_db.get(&task_id).unwrap().clone())
            } else {
                tracing::info!("Create new breakpoint_db");
                let task_id_str = task_id.to_string();
                let breakpoint_db = BreakpointDb::new_with_task(task_id_str.as_str()).await;
                if let Err(err) = breakpoint_db {
                    tracing::error!("BreakpointDb init error: {}", err);
                    return Err(err);
                }
                let breakpoint_db = breakpoint_db.unwrap();
                task_breakpoint_db.insert(task_id, breakpoint_db.clone());
                Some(breakpoint_db)
            };
            (lush_table_cache, breakpoint_db)
        }
        _ => (None, None),
    };

    let tx = Arc::new(tx);

    tracing::trace!(schema = ?schema, "parsing put stream schema");
    let tx_cloned = Arc::downgrade(&tx);
    let taos = pool.get().await?;

    let ipc_error_strategy = IpcErrorStrategy::from(connector);

    // let should_abort = Arc::new(AtomicBool::new(false));
    let (abort_message_tx, abort_message_rx) = flume::bounded(1);
    let notify = Arc::new(tokio::sync::Notify::new());

    let license: Option<ConnectorLicense> = if let Some(connector) = connector {
        // get tdengine server version and handle compatibility
        let server_version = get_server_version(&taos).await?;
        let (a, b, c) = get_main_version_from_server_version(&server_version).unwrap();
        let grants_sql = if a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3) {
            format!("select `limits` from information_schema.ins_grants_full where grant_name='{connector}'")
        } else {
            format!("select `{connector}` from information_schema.ins_grants")
        };

        #[cfg(feature = "disable-enterprise-connector-validation")]
        let license: Option<ConnectorLicense> = None;
        #[cfg(not(feature = "disable-enterprise-connector-validation"))]
        let license: Option<ConnectorLicense> =
            if to_dsn.get("token").is_some() && to_dsn.protocol.is_some() {
                None
            } else {
                taos.query_one::<_, String>(&grants_sql)
                    .await
                    .unwrap_or(None)
                    .and_then(|s| serde_json::from_str(&s).ok())
            };

        if let Some(license) = license {
            if a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3) {
                if license.is_expired_second() {
                    anyhow::bail!("The current connector {connector} has bean expired, please contact the TDengine customer success team to get the activation code.")
                }
            } else {
                if license.is_expired_day() {
                    anyhow::bail!("The current connector {connector} has bean expired, please contact the TDengine customer success team to get the activation code.")
                }
            }
        }
        None
    } else {
        None
    };

    let transferred = match connector {
        Some(_) => {
            controller
                .transferred
                .get(&(cluster_id, from_dsn.driver.clone()))
                .await
        }
        _ => None,
    };

    // Spawn writer task.
    tokio::spawn({
        let notify = notify.clone();
        async move {
            let task_id = task.id;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let timeout = std::time::Duration::from_secs(60);
            if let Err(err) = tokio::time::timeout(
                timeout,
                spawn_sender.send_async((
                    Box::pin(
                        ipc_stream_writer(
                            notify_sender.clone(),
                            agent_id,
                            task,
                            pool,
                            lock,
                            schema,
                            tx_cloned,
                            rx.clone(),
                            // rsp_tx,
                            license,
                            transferred,
                            lush_table_cache,
                            breakpoint_db,
                            Span::current(),
                            abort_message_tx,
                            ipc_error_strategy,
                            stream_trace_id,
                            notify,
                        )
                        .in_current_span(),
                    ),
                    sender,
                )),
            )
            .await
            {
                tracing::error!(
                    error.source = format!("{err:#}"),
                    "IPC stream writer spawn error"
                );
                let _ =
                    notify_sender.send(crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                        agent_id,
                        TaskActivity::warn(task_id, format!("{err:#}")),
                    ));
                drop(rx);
                return;
            }
            match receiver.await {
                Ok(Ok(_)) => {
                    tracing::info!("IPC stream writer spawned successfully");
                }
                Ok(Err(err)) => {
                    tracing::error!("IPC stream writer stopped, err:{:#?}", err);
                }
                Err(err) => {
                    tracing::error!("IPC stream writer stopped, err:{:#?}", err);
                }
            }
            let _ = notify_sender.send(crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                agent_id,
                TaskActivity::ipc_finished(task_id),
            ));

            tracing::info!(
                ipc.channel.capacity = rx.capacity(),
                ipc.channel.len = rx.len(),
                ipc.channel.receiver_count = rx.receiver_count(),
                ipc.channel.sender_count = rx.sender_count(),
                ipc.channel.is_disconnected = rx.is_disconnected(),
                "IPC stream writer stopped successfully"
            );
            drop(rx);
        }
        .in_current_span()
    });
    Ok((tx, abort_message_rx, notify))
}

impl PutStream {
    pub(super) async fn new(
        controller: TaskControllerRef,
        task_id: i64,
        req: Streaming<FlightData>,
        notify_sender: AgentNotifySender,
        remote: Option<std::net::SocketAddr>,
        stream_trace_id: TraceStreamId,
        spawn_sender: AgentSpawnSender,
    ) -> anyhow::Result<Self> {
        let task = controller
            .get(task_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))
            .unwrap()
            .unwrap();
        let builder = TaosBuilder::from_dsn(&task.to)?;
        let _ = builder.pool()?;

        let cluster_id: i64 = if let Some(cluster_id) = task.task.labels.find("to_cluster") {
            cluster_id.parse().map_err(|err| {
                anyhow::format_err!("Cannot parse cluster id from \"{cluster_id}\": {err}")
            })?
        } else {
            let taos = TaosBuilder::from_dsn(&task.to)?.build().await?;
            taos.query_one("select id from information_schema.ins_cluster")
                .await
                .map_err(|err| {
                    anyhow::format_err!("Cannot retrieve cluster id in grpc putting stream: {err}")
                })?
                .unwrap()
        };
        let agent_id = task
            .via
            .ok_or_else(|| anyhow::format_err!("Cannot find agent id for task {}", task_id))?;
        Ok(Self {
            req,
            controller,
            task_id,
            notify_sender,
            remote,
            stream_trace_id,
            spawn_sender,
            cluster_id,
            agent_id,
        })
    }

    #[instrument(skip_all, name="put_stream", fields(task.id=%self.task_id, remote=self.remote.as_ref().map(ToString::to_string)))]
    pub async fn into_flight_put_result(
        self,
    ) -> anyhow::Result<impl Stream<Item = Result<PutResult, Status>> + std::marker::Send> {
        // todo: directly use task detail instead of id.
        // dbg!(&self.task_id);
        let stream_trace_id = self.stream_trace_id;
        let agent_id = self.agent_id;
        set_data_trace_id_for_current_span(&stream_trace_id);
        tracing::info!("Put stream by task id {}", self.task_id,);
        // return self.req.map_ok(|data| PutResult {
        //     app_metadata: data.app_metadata,
        // });
        let mut stream = arrow_flight::decode::FlightDataDecoder::new(self.req.map_err(|err| {
            tracing::error!(
                error.source = format!("{err:#}"),
                "Invalid IPC stream error"
            );
            Into::into(err)
        }));
        // let (abort_tx, abort_rx) = tokio::sync::oneshot::channel();

        // let (tx, abort_message_rx, notify) = if let Some((tx, abort_message_rx, notify)) =
        //     get_ipc_stream_channel(stream_trace_id).await
        // {
        //     // 如果之前有连接，说明是重连
        //     tracing::info!("Reconnect IPC stream");
        //     // 如果之前有连接，直接返回，不需要再次创建 Writer
        //     (tx, abort_message_rx, notify)
        // } else {
        //     let schema = stream
        //         .try_next()
        //         .await?
        //         .ok_or_else(|| anyhow::format_err!("Invalid IPC stream"))?;
        //     let schema =
        //         if let arrow_flight::decode::DecodedPayload::Schema(schema) = schema.payload {
        //             schema
        //             // let _ = span.enter();
        //         } else {
        //             anyhow::bail!("Invalid IPC stream");
        //         };

        //     spawn_stream_writer(
        //         self.stream_trace_id,
        //         self.cluster_id,
        //         self.task_id,
        //         self.agent_id,
        //         &self.controller,
        //         self.notify_sender.clone(),
        //         self.spawn_sender.clone(),
        //         schema,
        //     )
        //     .in_current_span()
        //     .await?
        // };

        // response channel
        // tokio::spawn({
        //     let tx = tx.clone();
        //     let abort_message_rx = abort_message_rx.clone();
        //     let notify = notify.clone();
        //     async move {
        //         let put_stream_cache = async {
        //             tracing::info!(
        //                 worker.senders = tx.sender_count(),
        //                 worker.receivers = tx.receiver_count(),
        //                 worker.capacity = tx.capacity(),
        //                 "IPC stream abort"
        //             );
        //             if abort_message_rx.is_disconnected() {
        //                 tracing::info!(
        //                     "IPC worker will be stopped since abort message channel is closed"
        //                 );
        //                 return;
        //             }
        //             put_ipc_stream_channel(
        //                 stream_trace_id,
        //                 (tx, abort_message_rx.clone(), notify.clone()),
        //             )
        //             .await;
        //             tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        //             {
        //                 let mut cache = IPC_STREAM_CACHE.write().await;
        //                 if let Some(channel) = cache.get(&stream_trace_id) {
        //                     if channel.1.same_channel(&abort_message_rx) {
        //                         cache.remove(&stream_trace_id);
        //                         tracing::info!(
        //                             "IPC worker has not been reconnected, remove from cache"
        //                         );
        //                     }
        //                 }
        //             }
        //         };
        //         tokio::select! {
        //             _ = abort_rx => {
        //                 put_stream_cache.await;
        //             }
        //             _ = notify.notified() => {
        //                 tracing::info!("IPC stream closed");
        //             }
        //         }
        //     }
        //     .in_current_span()
        // });

        let schema = stream
            .try_next()
            .await?
            .ok_or_else(|| anyhow::format_err!("Invalid IPC stream"))?;
        let schema = if let arrow_flight::decode::DecodedPayload::Schema(schema) = schema.payload {
            schema
            // let _ = span.enter();
        } else {
            anyhow::bail!("Invalid IPC stream");
        };

        let (tx, abort_message_rx, notify) = spawn_stream_writer(
            self.stream_trace_id,
            self.cluster_id,
            self.task_id,
            self.agent_id,
            &self.controller,
            self.notify_sender.clone(),
            self.spawn_sender.clone(),
            schema,
        )
        .in_current_span()
        .await?;

        // 任务的 metrics 在启动任务的时候已经放入全局 Map 中，所以这里一定存在
        let metrics_arc = get_metrics(self.task_id).await.expect("metrics not found");
        let cur_span = Span::current();
        let notify_sender = self.notify_sender.clone();
        let task_id = self.task_id;
        let (put_tx, put_rx) = flume::bounded(0);

        tokio::spawn(async move {
            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(53));
            tokio::pin!(stream);

            let process_item = |message: DecodedFlightData, trace_id: TraceDataId| {
                let metrics = metrics_arc.ipc();
                let tx = tx.clone();
                let metadata = MessageMetadata::new_ack(
                    RPC_ACK_PROCESSED,
                    trace_id.as_u64(),
                    metrics.total_received_batches(),
                );
                let app_metadata = Bytes::copy_from_slice(metadata.as_bytes());
                async move {
                    match message.payload {
                        arrow_flight::decode::DecodedPayload::RecordBatch(batch) => {
                            tracing::trace!("Enqueue batch {}\nschema=\n{:?} \ncolumns=\n{:?}", trace_id, batch.schema(), batch.columns());
                            if let Err(err) = tx.send_async((batch, trace_id)).await {
                                tracing::warn!(
                                    trace_id = %trace_id,
                                    ipc.channel.capacity = tx.capacity(),
                                    ipc.channel.len = tx.len(),
                                    ipc.channel.receiver_count = tx.receiver_count(),
                                    ipc.channel.sender_count = tx.sender_count(),
                                    ipc.channel.is_disconnected = tx.is_disconnected(),
                                    "IPC channel sent err: {:#}",
                                    err
                                );

                                return None;
                            } else {
                                return Some(PutResult { app_metadata })
                            }
                        }
                        payload => {
                            tracing::warn!(payload = ?payload, metadata = ?app_metadata, "Invalid IPC message");
                            return Some(PutResult { app_metadata })
                        }
                    }
                }.instrument(cur_span.clone())
            };

            // Limit message decode errors as 10 per 60s (cps) to avoid infinite loop
            const MAX_MESSAGE_ERRORS: usize = 10;
            let mut error_check_interval =
                tokio::time::interval(std::time::Duration::from_secs(60));
            let mut message_error_count = 0;
            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        tracing::trace!("Send heartbeat");
                        put_tx
                            .send_async(Ok(PutResult { app_metadata: "heartbeat".into() }))
                            .await
                            .inspect_err(|err| {
                                cur_span.in_scope(|| {
                                    tracing::info!(error.source = format!("{err:#}"), "IPC stream finished");
                                });
                            })?;
                    }
                    _ = error_check_interval.tick() => {
                        message_error_count = 0;
                    }
                    message = stream.next() => {
                        if message.is_none() {
                            break;
                        }
                        let message = message.unwrap();
                        match message {
                            Err(err) => {
                                // To deal with decode error
                                message_error_count += 1;
                                if message_error_count > MAX_MESSAGE_ERRORS {
                                    cur_span.in_scope(|| {
                                        tracing::warn!(
                                            error.source = format!("{err:#}"),
                                            "Too many IPC stream errors"
                                        );
                                    });
                                    put_tx.send_async(Err(Status::aborted(format!("Too many put stream errors: {:#}", err))))
                                        .await
                                        .inspect_err(|err| {
                                            cur_span.in_scope(|| {
                                                tracing::info!(error.source = format!("{err:#}"), "IPC stream finished");
                                            });
                                        })?;

                                    if let Err(err) = notify_sender.send(
                                        crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                                            agent_id,
                                            TaskActivity::warn(task_id, format!("Put stream message error: {err:#}")),
                                        ),
                                    ) {
                                        tracing::warn!(
                                            error.source = format!("{err:#}"),
                                            "Put stream message error"
                                        );
                                    }
                                } else {
                                    cur_span.in_scope(|| {
                                        tracing::warn!(
                                            error.source = format!("{err:#}"),
                                            error.backtrace = ?err,
                                            "IPC stream message error"
                                        );
                                    });
                                }
                            }
                            Ok(message) => {
                                let app_metadata = message.app_metadata();
                                let trace_id: u64 = get_trace_id_from_app_meta(&app_metadata);
                                let trace_id = TraceDataId(trace_id);
                                metrics_arc.ipc().add_received_batches(1);
                                let count = metrics_arc.ipc().total_received_batches();
                                cur_span.in_scope(|| {
                                    tracing::debug!("Receive batch {}", trace_id);
                                });
                                let mut metadata = MessageMetadata::new_ack(RPC_ACK_RECEIVED, trace_id.as_u64(), count);
                                let app_metadata = Bytes::copy_from_slice(metadata.as_bytes());
                                // 1. send ack for received message
                                put_tx.send_async(Ok(PutResult { app_metadata}))
                                    .await
                                    .inspect_err(|err| {
                                        cur_span.in_scope(|| {
                                            tracing::info!(error.source = format!("{err:#}"), "Put stream response stream closed");
                                        });
                                    })?;
                                let item = process_item(message, trace_id).in_current_span().await;

                                // 2. send processed message
                                if let Some(item) = item {
                                    put_tx.send_async(Ok(item))
                                        .await
                                        .inspect_err(|err| {
                                            cur_span.in_scope(|| {
                                                tracing::info!(error.source = format!("{err:#}"), "Put stream response stream closed");
                                            });
                                        })?;
                                } else {
                                    cur_span.in_scope(|| {
                                        tracing::warn!("Put stream worker dropped");
                                    });
                                    metadata.set_ack(RPC_ACK_STREAM_END);
                                    let app_metadata = Bytes::copy_from_slice(metadata.as_bytes());
                                    put_tx.send_async(Ok(PutResult { app_metadata })).await
                                        .inspect_err(|err| {
                                            cur_span.in_scope(|| {
                                                tracing::info!(error.source = format!("{err:#}"), "Put stream response stream closed");
                                            });
                                        })?;
                                    drop(put_tx);
                                    break;
                                }
                            }
                        }
                    }
                    abort = abort_message_rx.recv_async() => {
                        tracing::info!("IPC stream abort");
                        if let Ok(abort) = abort {
                            put_tx.send_async(abort).await
                                .inspect_err(|err| {
                                    cur_span.in_scope(|| {
                                        tracing::info!(error.source = format!("{err:#}"), "Put stream response stream closed");
                                    });
                                })?;
                        }
                        drop(put_tx);
                        break;
                    }
                }
            }
            notify.notify_waiters();
            anyhow::Ok(())
        });
        Ok(PutStreamResp {
            put_rx: put_rx.into_stream(),
            // abort_tx,
        })
    }
}
pub(super) struct PutStreamResp {
    put_rx: PutStreamInner,
    // #[allow(dead_code)]
    // abort_tx: PutStreamAbortSender,
}

impl Stream for PutStreamResp {
    type Item = Result<PutResult, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.put_rx.poll_next_unpin(cx)
    }
}

unsafe impl Sync for PutStream {}
unsafe impl Send for PutStream {}

fn get_trace_id_from_app_meta(app_metadata: &bytes::Bytes) -> u64 {
    if app_metadata[0] == 0 {
        return MessageMetadata::ref_from(&app_metadata)
            .map(|m| m.trace_id())
            .unwrap_or_default();
    }
    let meta_bytes = app_metadata.as_bytes();
    match serde_json::from_slice::<AppMetadata>(meta_bytes) {
        Ok(app_meta) => app_meta.data_trace_id,
        Err(err) => {
            tracing::error!("parse app metadata error, {}", err);
            0
        }
    }
}
