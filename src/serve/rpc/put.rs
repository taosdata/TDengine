use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Context, bail};
use archive::{Archive, ArchiveConsumer, ArchiveType, Cache};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use arrow_flight::{FlightData, PutResult, decode::DecodedFlightData};
use bytes::Bytes;
use flume::Sender;
use futures::{Stream, TryStreamExt};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use taoslog::QidManager;
use taoslog::utils::{QidMetadataGetter, QidMetadataSetter};
use taosx_core::core_metrics::{TaskMetrics, get_metrics_arc_from_i64, init_task_metrics};
use taosx_core::plugins;
use taosx_core::sink::{handle_point_message_init, read_cache_and_rewrite};
use taosx_core::utils::trace::Qid;
use taosx_core::{
    IpcStreamWorker, Parser,
    core_metrics::get_metrics,
    sink::{
        IpcErrorStrategy, MessageMetadata, RPC_ACK_PROCESSED, RPC_ACK_RECEIVED, RPC_ACK_STREAM_END,
        handle_lush_message_init, lush::TableTagCache,
    },
    utils::breakpoints::BreakpointDb,
};
use taosx_utils::dsn::json_to_dsn;
use tonic::{Status, Streaming};
use tracing::{Instrument, Span, instrument};
use zerocopy::FromBytes;

use crate::serve::controller::Task;
use crate::serve::scheduler::agent::AgentNotify;
use crate::serve::{
    controller::{TaskControllerRef, activity::Activity},
    scheduler::agent::{AgentNotifySender, AgentSpawnSender},
};

#[derive(Debug)]
pub struct PutStream {
    req: Streaming<FlightData>,
    controller: TaskControllerRef,
    task_id: i64,
    job_id: i64,
    remote: Option<std::net::SocketAddr>,
    notify_sender: AgentNotifySender,
    qid: Qid,
    spawn_sender: AgentSpawnSender,
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

async fn ipc_stream_writer(
    notify_sender: AgentNotifySender,
    agent_id: i64,
    task: Task,
    pool: taos::TaosPool,
    lock: Arc<tokio::sync::Mutex<()>>,
    schema: Arc<arrow::datatypes::Schema>,
    tx: Weak<flume::Sender<(arrow::record_batch::RecordBatch, Qid)>>,
    rx: flume::Receiver<(arrow::record_batch::RecordBatch, Qid)>,
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
    let job_id = task.job_id;
    let from = json_to_dsn(&serde_json::Value::String(task.from.clone()))?;
    let to: Dsn = task.to.parse().unwrap();
    let taos = pool.get().await?;
    let worker = IpcStreamWorker::new(
        pool.clone(),
        from.clone(),
        lock,
        schema,
        lush_table_cache,
        breakpoint_db.clone(),
        span,
        Some((task_id, job_id)),
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
        if let Some(arc) = get_metrics(task_id, job_id) {
            arc
        } else {
            let _ = init_task_metrics(&from, &to, task_id, job_id).await;
            get_metrics(task_id, job_id).ok_or_else(|| anyhow::format_err!("metrics not found"))?
        }
    };

    let metrics = metrics_arc.ipc();
    if worker.lush_model_config.get().is_none()
        && let Some(sql) = metadata.init_sql_string()
    {
        let init = metadata.init().unwrap();
        handle_lush_message_init(init, &taos, &sql, metrics).await?;
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

    let handle = tokio::spawn(
        async move {
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
                                    Some(&archive_tx),
                                )
                                .await
                            {
                                let last_errors = contiguous_errors
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                                tokio::time::sleep(std::time::Duration::from_millis(period * 80))
                                    .await;
                                let message = format!(
                                    "IPC processing record {} error: {err:#}",
                                    qid.display()
                                );
                                notify_sender
                                    .send(
                                        crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                                            agent_id,
                                            Activity::warn(task_id, job_id, message),
                                        ),
                                    )
                                    .ok();
                                if ipc_error_strategy.will_stop() || last_errors > 10 {
                                    abort_message_tx
                                        .send_async(Err(Status::cancelled(format!(
                                            "IPC worker will be stopped since {err:#}"
                                        ))))
                                        .await
                                        .ok();
                                    notify_sender
                                    .send(crate::serve::scheduler::agent::AgentNotify::WriterError(
                                        agent_id,
                                        task_id,
                                        job_id,
                                        format!("{err:#}"),
                                    ))
                                    .ok();
                                    bail!("IPC worker will be stopped since {err:#}");
                                }
                                if let Some(tx) = tx.upgrade() {
                                    tracing::warn!(
                                        "Re-queue with {} rows record",
                                        record.num_rows()
                                    );
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
                                            Activity::running(
                                                task_id,
                                                job_id,
                                                format!(
                                                    "Rescue from {last_errors} continuous errors",
                                                ),
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
        }
        .in_current_span(),
    );

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
    task_id: i64,
    job_id: i64,
    agent_id: i64,
    controller: &TaskControllerRef,
    notify_sender: AgentNotifySender,
    spawn_sender: AgentSpawnSender,
    schema: Arc<Schema>,
    archive_tx: Sender<ArchiveType>,
) -> anyhow::Result<PutStreamChannel> {
    let task = controller
        .get_task(task_id, job_id)
        .await
        .with_context(|| format!("Cannot find task ({task_id},{job_id})"))?;
    // dbg!(&task);
    let builder = TaosBuilder::from_dsn(&task.to)?;
    let pool = builder.pool()?;
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    // let from_dsn: Dsn = task.from.parse()?;
    let from_dsn = json_to_dsn(&serde_json::Value::String(task.from.clone()))?;

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
        "pulsar" | "pulsarTuya" => Some("pulsar"),
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
                task_lush_table_cache.entry((task_id, job_id))
            {
                tracing::info!("Create new lush_table_cache");
                let table_tag_cache = Arc::new(TableTagCache::new());
                e.insert(table_tag_cache.clone());
                Some(table_tag_cache)
            } else {
                tracing::info!("Got existing lush_table_cache");
                Some(
                    task_lush_table_cache
                        .get(&(task_id, job_id))
                        .unwrap()
                        .clone(),
                )
            };
            let task_breakpoint_db_lock = controller.scheduler.task_breakpoint_db.clone();
            let mut task_breakpoint_db = task_breakpoint_db_lock.write().await;
            let breakpoint_db = if let std::collections::hash_map::Entry::Vacant(e) =
                task_breakpoint_db.entry((task_id, job_id))
            {
                tracing::info!("Create new breakpoint_db");
                let breakpoint_db = BreakpointDb::new_with_task(task_id, job_id).await;
                if let Err(err) = breakpoint_db {
                    tracing::error!("BreakpointDb init error: {}", err);
                    return Err(err);
                }
                let breakpoint_db = breakpoint_db.unwrap();
                e.insert(breakpoint_db.clone());
                Some(breakpoint_db)
            } else {
                tracing::info!("Got existing breakpoint_db");
                Some(task_breakpoint_db.get(&(task_id, job_id)).unwrap().clone())
            };
            (lush_table_cache, breakpoint_db)
        }
        _ => (None, None),
    };

    let tx = Arc::new(tx);

    tracing::trace!(schema = ?schema, "parsing put stream schema");
    let tx_cloned = Arc::downgrade(&tx);

    let ipc_error_strategy = IpcErrorStrategy::from(connector);

    // let should_abort = Arc::new(AtomicBool::new(false));
    let (abort_message_tx, abort_message_rx) = flume::bounded(1);
    let notify = Arc::new(tokio::sync::Notify::new());

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
                notify_sender
                    .send(AgentNotify::TaskActivity(
                        agent_id,
                        Activity::warn(task_id, job_id, format!("{err:#}")),
                    ))
                    .ok();
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
            notify_sender
                .send(AgentNotify::TaskActivity(
                    agent_id,
                    Activity::ipc_finished(task_id, job_id),
                ))
                .ok();

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
        job_id: i64,
        req: Streaming<FlightData>,
        notify_sender: AgentNotifySender,
        remote: Option<std::net::SocketAddr>,
        qid: Qid,
        spawn_sender: AgentSpawnSender,
    ) -> anyhow::Result<Self> {
        let task = controller
            .get_task(task_id, job_id)
            .await
            .context("Task not found")?;
        let builder = TaosBuilder::from_dsn(&task.to)?;
        let _ = builder.pool()?;

        let agent_id = task
            .via
            .ok_or_else(|| anyhow::format_err!("Cannot find agent id for task {}", task_id))?;
        Ok(Self {
            req,
            controller,
            task_id,
            job_id,
            notify_sender,
            remote,
            qid,
            spawn_sender,
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
        let job_id = self.job_id;
        let task = self
            .controller
            .get_task(task_id, job_id)
            .await
            .with_context(|| format!("Cannot find task {task_id}"))?;
        let mut parser: Option<Parser> = task
            .parser
            .as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap());
        if let Some(parser) = parser.as_mut() {
            match parser {
                plugins::Parser::Inner(parser) => {
                    parser.organize_archive(task.id, job_id)?;
                    parser.organize_cache(task.id, job_id)?;
                }
                plugins::Parser::WithSample { parser, input: _ } => {
                    parser.organize_archive(task.id, job_id)?;
                    parser.organize_cache(task.id, job_id)?;
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
                let metrics = get_metrics_arc_from_i64(Some((task_id, job_id)));

                match ArchiveConsumer::new(task_id, job_id, cache, archive, |num_rows: u64| {
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
                read_cache_and_rewrite(
                    (task_id, job_id),
                    &pool,
                    &parser,
                    &archive_tx_clone,
                    &cancellation,
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

        let (tx, abort_message_rx, notify) = spawn_stream_writer(
            self.task_id,
            self.job_id,
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
            if let Some(arc) = get_metrics(self.task_id, self.job_id) {
                arc
            } else {
                let task = self
                    .controller
                    .get_task(self.task_id, self.job_id)
                    .await
                    .with_context(|| format!("Task ({task_id},{job_id}) not found"))?;
                // let from: Dsn = task.from.parse()?;
                let from = json_to_dsn(&serde_json::Value::String(task.from.clone()))?;
                let to: Dsn = task.to.parse()?;
                let _ = init_task_metrics(&from, &to, self.task_id, self.job_id).await;
                get_metrics(self.task_id, self.job_id)
                    .ok_or_else(|| anyhow::format_err!("metrics not found"))?
            }
        };

        let notify_sender = self.notify_sender.clone();
        let (task_id, job_id) = (self.task_id, self.job_id);
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
            let controller = self.controller.clone();
            let future_flight = async move {
                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            // Check if task is cancelled
                            let is_cancelled = {
                                let tasks = controller.scheduler.tasks.read().await;
                                if let Some(job) = tasks.get_by_task_job_id(&(task_id, job_id)) {
                                    job.task.cancellation.is_cancelled()
                                } else {
                                    true
                                }
                            };
                            if is_cancelled {
                                tracing::info!("Task {task_id} cancelled, stopping IPC stream");
                                break;
                            }

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
                                                Activity::warn(task_id, job_id, format!("Put stream message error: {err:#}")),
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
        })
    }
}

pub(super) struct PutStreamResp {
    put_rx: PutStreamInner,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_metadata_serialize() {
        let metadata = AppMetadata {
            data_trace_id: 12345,
        };
        let json = serde_json::to_string(&metadata);
        assert!(json.is_ok());
    }

    #[test]
    fn test_app_metadata_deserialize() {
        let json = r#"{"data_trace_id":12345}"#;
        let metadata: Result<AppMetadata, _> = serde_json::from_str(json);
        assert!(metadata.is_ok());
        assert_eq!(metadata.unwrap().data_trace_id, 12345);
    }

    #[test]
    fn test_app_metadata_roundtrip() {
        let orig = AppMetadata {
            data_trace_id: 99999,
        };
        let json = serde_json::to_string(&orig).unwrap();
        let restored: AppMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(orig.data_trace_id, restored.data_trace_id);
    }

    #[test]
    fn test_app_metadata_zero_trace_id() {
        let metadata = AppMetadata { data_trace_id: 0 };
        assert_eq!(metadata.data_trace_id, 0);
    }

    #[test]
    fn test_app_metadata_large_trace_id() {
        let metadata = AppMetadata {
            data_trace_id: u64::MAX,
        };
        let json = serde_json::to_string(&metadata).unwrap();
        let restored: AppMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(metadata.data_trace_id, restored.data_trace_id);
    }

    #[test]
    fn test_put_stream_debug() {
        // PutStream should implement Debug
        // This is validated by the struct definition
    }

    #[test]
    fn test_rpc_ack_constants() {
        // Test that RPC ACK constants exist and are not equal
        assert_ne!(RPC_ACK_RECEIVED, RPC_ACK_PROCESSED);
        assert_ne!(RPC_ACK_RECEIVED, RPC_ACK_STREAM_END);
        assert_ne!(RPC_ACK_PROCESSED, RPC_ACK_STREAM_END);
    }
}
