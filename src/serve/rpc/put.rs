use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Context, bail};
use archive::{Archive, ArchiveConsumer, ArchiveType, Cache};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use arrow_flight::{FlightData, PutResult, decode::DecodedFlightData};
use bytes::Bytes;
use flume::Sender;
use futures::{Stream, TryFutureExt, TryStreamExt};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use taoslog::QidManager;
use taoslog::utils::{QidMetadataGetter, QidMetadataSetter};
use taosx_core::core_metrics::{TaskMetrics, get_metrics_arc_from_i64, init_task_metrics};
use taosx_core::plugins;
use taosx_core::sink::{handle_point_message_init, read_cache_and_rewrite};
use taosx_core::utils::dsn::json_to_dsn;
use taosx_core::utils::trace::Qid;
use taosx_core::{
    ConnectorLicense, IpcStreamWorker, Parser,
    core_metrics::get_metrics,
    sink::{
        IpcErrorStrategy, MessageMetadata, RPC_ACK_PROCESSED, RPC_ACK_RECEIVED, RPC_ACK_STREAM_END,
        handle_lush_message_init, lush::TableTagCache,
    },
    utils::{breakpoints::BreakpointDb, get_main_version_from_server_version, get_server_version},
};
use tonic::{Status, Streaming};
use tracing::{Instrument, Span, instrument};
use zerocopy::FromBytes;

use crate::serve::{
    controller::{Activity, TaskControllerRef, TaskDetail, transferred::ConnectorTransferred},
    scheduler::agent::{AgentNotifySender, AgentSpawnSender},
};

#[derive(Debug)]
pub struct PutStream {
    req: Streaming<FlightData>,
    controller: TaskControllerRef,
    task_id: i64,
    remote: Option<std::net::SocketAddr>,
    notify_sender: AgentNotifySender,
    qid: Qid,
    spawn_sender: AgentSpawnSender,
    cluster_id: i64,
    agent_id: i64,
}

// type PutStreamReceiver = flume::Receiver<Result<PutResult, Status>>;
type PutStreamInner = flume::r#async::RecvStream<'static, Result<PutResult, Status>>;
// type PutStreamSender = flume::Sender<Result<PutResult, Status>>;
type PutStreamBatchSender = flume::Sender<(RecordBatch, Qid)>;
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
    tx: Weak<flume::Sender<(arrow::record_batch::RecordBatch, Qid)>>,
    rx: flume::Receiver<(arrow::record_batch::RecordBatch, Qid)>,
    // rsp_tx: flume::Sender<anyhow::Result<()>>,
    license: Option<ConnectorLicense>,
    _transferred: Option<Arc<ConnectorTransferred>>,
    lush_table_cache: Option<Arc<TableTagCache>>,
    breakpoint_db: Option<BreakpointDb>,
    span: tracing::Span,
    abort_message_tx: flume::Sender<Result<PutResult, Status>>,
    ipc_error_strategy: IpcErrorStrategy,
    notify: Arc<tokio::sync::Notify>,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<()> {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let _drop_guard = cancellation.clone().drop_guard();
    tokio::spawn({
        let notify = notify.clone();
        async move {
            cancellation.cancelled().await;
            notify.notify_waiters();
        }
    });
    // dbg!(&task);
    notify_sender.send(crate::serve::scheduler::agent::AgentNotify::TaskActivity(
        agent_id,
        Activity::ipc_started(task.id),
    ))?;
    let task_id = task.id;
    // let from: Dsn = task.from.parse().unwrap();
    let from = json_to_dsn(&serde_json::Value::String(task.from.clone()))?;
    let to: Dsn = task.to.parse().unwrap();
    let taos = pool.get().await?;
    let worker = IpcStreamWorker::new(
        pool.clone(),
        from.clone(),
        lock,
        schema,
        license,
        None,
        lush_table_cache,
        breakpoint_db.clone(),
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
    let metrics_arc = {
        if let Some(arc) = get_metrics(task_id).await {
            arc
        } else {
            let _ = init_task_metrics(&from, &to, task_id, None).await;
            get_metrics(task_id)
                .await
                .ok_or_else(|| anyhow::format_err!("metrics not found"))?
        }
    };

    let metrics = metrics_arc.ipc();
    if worker.lush_model_config.get().is_none() {
        if let Some(sql) = metadata.init_sql_string() {
            let init = metadata.init().unwrap();
            handle_lush_message_init(init, &taos, &sql, metrics).await?;
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

    let stream = rx.into_stream();

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

    let handle = tokio::spawn(async move {
        let metrics = metrics_arc.ipc();
        stream
            .map(|(record, qid)| {
                anyhow::Ok((
                    record,
                    qid,
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
                    qid,
                    worker,
                    parser,
                    notify_sender,
                    tx,
                    abort_message_tx,
                    contiguous_errors,
                    metrics_arc,
                    tables_messages_in_progress,
                )| {
                    let archive_tx = archive_tx.clone();
                    async move {
                        taoslog::utils::Span.set_qid(&qid);
                        tracing::trace!("Writing batch");
                        let raw_rows = record.num_rows();
                        if let Err(err) = worker
                            .process_record(
                                record.clone(),
                                parser.as_deref(),
                                metrics,
                                metrics_arc,
                                tables_messages_in_progress,
                                None,
                                archive_tx.clone(),
                            )
                            .await
                        {
                            let last_errors =
                                contiguous_errors.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            metrics.add_failed_batches(1);
                            tracing::error!(
                                continuous_errors = last_errors,
                                error = format!("{:#}", err),
                                backtrace = %err.backtrace(),
                                "Writing batch error",
                            );
                            let period = match last_errors {
                                errors if errors < 8 => 8,
                                errors if errors < 16 => 16,
                                errors if errors < 32 => 32,
                                errors if errors < 64 => 64,
                                _ => 128,
                            };
                            tokio::time::sleep(std::time::Duration::from_millis(period * 80)).await;
                            let message =
                                format!("IPC processing record {} error: {err:#}", qid.display());
                            let _ = notify_sender.send(
                                crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                                    agent_id,
                                    Activity::warn(task_id, message),
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
                                tracing::warn!("Re-queue with {} rows record", record.num_rows());
                                tx.send_async((record, qid))
                                    .await
                                    .context("Re-queue error")?;
                            } else {
                                tracing::warn!("IPC channel is closed, cannot re-queue record");
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
                                        Activity::info(
                                            task_id,
                                            format!(
                                                "Rescue from {} continuous errors",
                                                last_errors
                                            ),
                                            "running",
                                        ),
                                    ),
                                );
                                contiguous_errors.store(0, std::sync::atomic::Ordering::SeqCst);
                            } else {
                                tracing::debug!("Writing batch success");
                            }
                        }
                        metrics.add_processed_messages(raw_rows as u64);
                        Ok(())
                    }
                    .in_current_span()
                },
            )
            .in_current_span()
            .await
    });

    tokio::select! {
        _ = notify.notified() => {
            tracing::info!("IPC stream writer notified close");
        }
        res = handle => {
            if let Err(err) = res {
                tracing::error!(
                    error.source = format!("{err:#}"),
                    "IPC stream writer error"
                );
            }
        }
    }
    tracing::info!(task.id = task_id, "IPC stream writer finished");

    Ok(())
}

#[instrument(skip_all)]
async fn spawn_stream_writer(
    cluster_id: i64,
    task_id: i64,
    agent_id: i64,
    controller: &TaskControllerRef,
    notify_sender: AgentNotifySender,
    spawn_sender: AgentSpawnSender,
    schema: Arc<Schema>,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<PutStreamChannel> {
    let task = controller
        .get(task_id)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| anyhow::format_err!("Cannot find task {}", task_id))?;
    // dbg!(&task);
    let builder = TaosBuilder::from_dsn(&task.to)?;
    let pool = builder.pool()?;
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    // let from_dsn: Dsn = task.from.parse()?;
    let from_dsn = json_to_dsn(&serde_json::Value::String(task.from.clone()))?;
    let to_dsn: Dsn = task.to.parse()?;

    let connector = match from_dsn.driver.as_str() {
        "opcda" => Some("opc_da"),
        "opcua" => Some("opc_ua"),
        "pi" => Some("pi"),
        "pibackfill" => Some("pi"),
        "influxdb" => Some("influxdb"),
        "opentsdb" => Some("opentsdb"),
        "kafka" => Some("kafka"),
        "avevaHistorian" => Some("avevahistorian"),
        "mqtt" => Some("mqtt"),
        _ => None,
    };

    // data channel
    let (tx, rx) = match from_dsn.driver.as_str() {
        "pi" | "pibackfill" => flume::bounded(50),
        _ => flume::bounded(1024),
    };

    let (lush_table_cache, breakpoint_db) = match from_dsn.driver.as_str() {
        "pi" | "pibackfill" | "influxdb" | "opentsdb" => {
            let task_lush_table_cache_lock = controller.scheduler.lush_table_cache.clone();
            let mut task_lush_table_cache = task_lush_table_cache_lock.write().await;
            let lush_table_cache = if let std::collections::hash_map::Entry::Vacant(e) =
                task_lush_table_cache.entry(task_id)
            {
                tracing::info!("Create new lush_table_cache");
                let table_tag_cache = Arc::new(TableTagCache::new());
                e.insert(table_tag_cache.clone());
                Some(table_tag_cache)
            } else {
                tracing::info!("Got existing lush_table_cache");
                Some(task_lush_table_cache.get(&task_id).unwrap().clone())
            };
            let task_breakpoint_db_lock = controller.scheduler.task_breakpoint_db.clone();
            let mut task_breakpoint_db = task_breakpoint_db_lock.write().await;
            let breakpoint_db = if let std::collections::hash_map::Entry::Vacant(e) =
                task_breakpoint_db.entry(task_id)
            {
                tracing::info!("Create new breakpoint_db");
                let task_id_str = task_id.to_string();
                let breakpoint_db = BreakpointDb::new_with_task(task_id_str.as_str()).await;
                if let Err(err) = breakpoint_db {
                    tracing::error!("BreakpointDb init error: {}", err);
                    return Err(err);
                }
                let breakpoint_db = breakpoint_db.unwrap();
                e.insert(breakpoint_db.clone());
                Some(breakpoint_db)
            } else {
                tracing::info!("Got existing breakpoint_db");
                Some(task_breakpoint_db.get(&task_id).unwrap().clone())
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
            format!(
                "select `limits` from information_schema.ins_grants_full where grant_name='{connector}'"
            )
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
                    anyhow::bail!(
                        "The current connector {connector} has bean expired, please contact the TDengine customer success team to get the activation code."
                    )
                }
            } else if license.is_expired_day() {
                anyhow::bail!(
                    "The current connector {connector} has bean expired, please contact the TDengine customer success team to get the activation code."
                )
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
                            notify,
                            archive_tx.clone(),
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
                        Activity::warn(task_id, format!("{err:#}")),
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
                Activity::ipc_finished(task_id),
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
        qid: Qid,
        spawn_sender: AgentSpawnSender,
    ) -> anyhow::Result<Self> {
        use tokio_retry2::RetryError;
        use tokio_retry2::strategy::{ExponentialBackoff, MaxInterval, jitter};
        let mut retry = ExponentialBackoff::from_millis(100)
            .factor(2)
            .max_delay_millis(100)
            .max_interval(5000)
            .map(jitter)
            .take(5);

        let task = loop {
            let cond = controller
                .get(task_id)
                .map_ok_or_else(RetryError::to_transient, move |task| {
                    task.ok_or_else(|| {
                        RetryError::permanent(anyhow::format_err!(
                            "Cannot find task task_id{task_id}"
                        ))
                    })
                })
                .instrument(tracing::info_span!("RetryGetTask"))
                .await;
            match cond {
                Ok(task) => {
                    break task;
                }
                Err(err) => {
                    tracing::error!(
                        error.source = format!("{err:#}"),
                        "Cannot get task in controller"
                    );
                    match err {
                        RetryError::Permanent(err) => {
                            return Err(err);
                        }
                        RetryError::Transient { err, retry_after } => {
                            if let Some(duration) = retry_after.or(retry.next()) {
                                tokio::time::sleep(duration).await;
                            } else {
                                return Err(err);
                            }
                        }
                    }
                }
            }
        };
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
                .ok_or_else(|| {
                    anyhow::format_err!("Cannot find cluster id in grpc putting stream")
                })?
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
            qid,
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
        taoslog::utils::Span.set_qid(&self.qid);
        // debug_assert!(self.qid.task_id() > 0);
        // debug_assert!(self.qid.batch_id() > 0);
        let agent_id = self.agent_id;
        tracing::info!("Put stream by task id {}", self.task_id);
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

        let task_id = self.task_id;
        let task = self
            .controller
            .get(task_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| anyhow::format_err!("Cannot find task {}", task_id))?;
        let mut parser: Option<Parser> = task
            .parser
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap());
        if let Some(parser) = parser.as_mut() {
            match parser {
                plugins::Parser::Inner(parser) => {
                    parser.organize_archive(task.id)?;
                    parser.organize_cache(task.id)?;
                }
                plugins::Parser::WithSample { parser, input: _ } => {
                    parser.organize_archive(task.id)?;
                    parser.organize_cache(task.id)?;
                }
            };
        }

        // the queue for transmitting cache and archived data
        let (archive_tx, archive_rx) = flume::bounded(0);
        // clone the configurations
        let parser_clone = parser.clone();
        let to: Dsn = task.to.parse()?;
        let cancellation = tokio_util::sync::CancellationToken::new();
        // spawn a thread to write data to files
        let process_archive = tokio::spawn(async move {
            let _a = taosx_core::utils::defer::defer(|| {
                tracing::info!("the 'cache & archive' thread has completed, task id: {task_id:?}",);
            });
            if parser_clone.is_some() {
                let (cache, archive) = match parser_clone {
                    Some(parser) => (
                        parser.global().process_on_abnormal.cache.clone(),
                        parser.global().process_on_abnormal.archive.clone(),
                    ),
                    None => (Cache::default(), Archive::default()),
                };
                let metrics = get_metrics_arc_from_i64(Some(task_id)).await;

                match ArchiveConsumer::new(task_id, cache, archive, |num_rows: u64| {
                    let metrics = metrics.ipc();
                    metrics.add_archived_rows(num_rows);
                    Ok::<_, anyhow::Error>(())
                })
                .consume(archive_rx)
                .await
                {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        tracing::error!(
                            error.source = format!("{err:#}"),
                            "Archive consumer error"
                        );
                        Err(err)
                    }
                }
            } else {
                drop(archive_rx);
                loop {
                    tokio::select! {
                        _ = cancellation.cancelled() => {
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
        let pool = {
            let builder = taos::TaosBuilder::from_dsn(to)?;
            let mut pool_config = builder.default_pool_config();
            let timeout = match parser.clone() {
                Some(parser) => {
                    parser
                        .global()
                        .process_on_abnormal
                        .connection_timeout_in_second_value
                }
                None => 30,
            };
            pool_config.timeouts.wait = Some(Duration::from_secs(timeout as u64));
            builder.with_pool_config(pool_config)?
        };
        let parser_clone = parser.clone();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let _drop_guard = cancellation.clone().drop_guard();
        let archive_tx_clone = archive_tx.clone();
        let process_cache = tokio::spawn(async move {
            let _a = taosx_core::utils::defer::defer(|| {
                tracing::info!("the 'rewrite file' thread has completed, task id: {task_id:?}",);
            });
            if let Some(parser) = parser_clone {
                read_cache_and_rewrite(task_id, &pool, &parser, archive_tx_clone, &cancellation)
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

        let (tx, abort_message_rx, notify) = spawn_stream_writer(
            self.cluster_id,
            self.task_id,
            self.agent_id,
            &self.controller,
            self.notify_sender.clone(),
            self.spawn_sender.clone(),
            schema,
            archive_tx.clone(),
        )
        .await?;

        // 任务的 metrics 在启动任务的时候已经放入全局 Map 中，所以这里一定存在
        let metrics_arc = {
            if let Some(arc) = get_metrics(self.task_id).await {
                arc
            } else {
                let task = self
                    .controller
                    .get(self.task_id)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))
                    .unwrap()
                    .unwrap();
                // let from: Dsn = task.from.parse()?;
                let from = json_to_dsn(&serde_json::Value::String(task.from.clone()))?;
                let to: Dsn = task.to.parse()?;
                let _ = init_task_metrics(&from, &to, self.task_id, None).await;
                get_metrics(self.task_id)
                    .await
                    .ok_or_else(|| anyhow::format_err!("metrics not found"))?
            }
        };

        let notify_sender = self.notify_sender.clone();
        let task_id = self.task_id;
        let (put_tx, put_rx) = flume::bounded(0);

        tokio::spawn(async move {
            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(53));
            tokio::pin!(stream);

            let process_item = |message: DecodedFlightData| {
                let metrics = metrics_arc.ipc();
                let tx = tx.clone();
                let qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
                // debug_assert!(qid.task_id() > 0);
                // debug_assert!(qid.batch_id() > 0);
                let metadata = MessageMetadata::new_ack(
                    RPC_ACK_PROCESSED,
                    qid.get(),
                    metrics.total_received_batches(),
                );
                let app_metadata = Bytes::copy_from_slice(metadata.as_bytes());
                async move {
                    match message.payload {
                        arrow_flight::decode::DecodedPayload::RecordBatch(batch) => {
                            tracing::trace!(schema = ?batch.schema(), columns = ?batch.columns(), "Enqueue batch");
                            metrics.add_received_messages(batch.num_rows() as u64);
                            if let Err(err) = tx.send_async((batch, qid)).await {
                                tracing::warn!(
                                    ipc.channel.capacity = tx.capacity(),
                                    ipc.channel.len = tx.len(),
                                    ipc.channel.receiver_count = tx.receiver_count(),
                                    ipc.channel.sender_count = tx.sender_count(),
                                    ipc.channel.is_disconnected = tx.is_disconnected(),
                                    "IPC channel sent err: {:#}",
                                    err
                                );

                                None
                            } else {
                                Some(PutResult { app_metadata })
                            }
                        }
                        payload => {
                            tracing::warn!(payload = ?payload, metadata = ?app_metadata, "Invalid IPC message");
                            Some(PutResult { app_metadata })
                        }
                    }
                }.in_current_span()
            };

            // Limit message decode errors as 10 per 60s (cps) to avoid infinite loop
            const MAX_MESSAGE_ERRORS: usize = 10;
            let mut error_check_interval =
                tokio::time::interval(std::time::Duration::from_secs(60));
            let mut message_error_count = 0;
            let metrics_arc_clone = metrics_arc.clone();
            let notify_clone = notify.clone();
            let future_flight = async move {
                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            tracing::trace!("Send heartbeat");
                            put_tx
                                .send_async(Ok(PutResult { app_metadata: "heartbeat".into() }))
                                .await
                                .inspect_err(|err| {
                                    tracing::info!(error.source = format!("{err:#}"), "IPC stream finished");
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
                                        tracing::warn!(
                                                error.source = format!("{err:#}"),
                                                "Too many IPC stream errors"
                                            );
                                        put_tx.send_async(Err(Status::aborted(format!("Too many put stream errors: {:#}", err))))
                                            .await
                                            .inspect_err(|err| {
                                                tracing::info!(error.source = format!("{err:#}"), "IPC stream finished");
                                                notify_clone.notify_waiters();
                                            })?;

                                        if let Err(err) = notify_sender.send(
                                            crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                                                agent_id,
                                                Activity::warn(task_id, format!("Put stream message error: {err:#}")),
                                            ),
                                        ) {
                                            tracing::warn!(
                                                error.source = format!("{err:#}"),
                                                "Put stream message error"
                                            );
                                        }
                                    } else {
                                        tracing::warn!(
                                                error.source = format!("{err:#}"),
                                                error.backtrace = ?err,
                                                "IPC stream message error"
                                            );
                                    }
                                }
                                Ok(message) => {
                                    let app_metadata = message.app_metadata();
                                    let trace_id: u64 = get_trace_id_from_app_meta(&app_metadata);
                                    let qid = Qid::from(trace_id);
                                    taoslog::utils::Span.set_qid(&qid);
                                    metrics_arc_clone.ipc().add_received_batches(1);
                                    let count = metrics_arc_clone.ipc().total_received_batches();
                                    tracing::debug!("Receive batch");
                                    let mut metadata = MessageMetadata::new_ack(RPC_ACK_RECEIVED, trace_id, count);
                                    let app_metadata = Bytes::copy_from_slice(metadata.as_bytes());
                                    // 1. send ack for received message
                                    put_tx.send_async(Ok(PutResult { app_metadata}))
                                        .await
                                        .inspect_err(|err| {
                                            tracing::info!(error.source = format!("{err:#}"), "Put stream response stream closed");
                                            notify_clone.notify_waiters();
                                        })?;
                                    let item = process_item(message).in_current_span().await;

                                    // 2. send processed message
                                    if let Some(item) = item {
                                        put_tx.send_async(Ok(item))
                                            .await
                                            .inspect_err(|err| {
                                                tracing::info!(error.source = format!("{err:#}"), "Put stream response stream closed");
                                                notify_clone.notify_waiters();
                                            })?;
                                    } else {
                                        tracing::warn!("Put stream worker dropped");
                                        metadata.set_ack(RPC_ACK_STREAM_END);
                                        let app_metadata = Bytes::copy_from_slice(metadata.as_bytes());
                                        put_tx.send_async(Ok(PutResult { app_metadata })).await
                                            .inspect_err(|err| {
                                                tracing::info!(error.source = format!("{err:#}"), "Put stream response stream closed");
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
                                        tracing::info!(error.source = format!("{err:#}"), "Put stream response stream closed");
                                    })?;
                            }
                            drop(put_tx);
                            break;
                        }
                    }
                }
                anyhow::Ok(())
            };
            tokio::select! {
                res = future_flight => {
                    if let Err(ref err) = res {
                        tracing::error!(
                            error.source = format!("{err:#}"),
                            "Future flight error"
                        );
                    }
                    res?
                }
                res = future_process_archive => {
                    if let Err(ref err) = res {
                        tracing::error!(
                            error.source = format!("{err:#}"),
                            "Future process archive error"
                        );
                    }
                    res?
                }
            }
            abort_handle_process_archive.abort();
            abort_handle_process_cache.abort();
            notify.notify_waiters();
            tracing::info!("IPC stream writer finished");
            anyhow::Ok(())
        }.in_current_span());
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
        return MessageMetadata::ref_from_bytes(app_metadata)
            .map(|m| m.qid())
            .unwrap_or_default();
    }
    match serde_json::from_slice::<AppMetadata>(app_metadata) {
        Ok(app_meta) => app_meta.data_trace_id,
        Err(err) => {
            tracing::error!("parse app metadata error, {}", err);
            0
        }
    }
}
