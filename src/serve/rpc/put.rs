use std::sync::{Arc, Weak};

use anyhow::{bail, Context};
use arrow_flight::{error::FlightError, FlightData, PutResult};
use futures::{Stream, TryStreamExt};
use futures_util::StreamExt;
use linked_hash_map::LinkedHashMap;
use parquet::data_type::AsBytes;
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, TaosBuilder};
use taosx_core::{
    core_metrics::get_metrics,
    sink::{handle_lush_message_init, IpcErrorStrategy},
    utils::{
        get_main_version_from_server_version, get_server_version,
        trace::{
            get_data_trace_id_str, get_stream_id_u64, set_data_trace_id_for_current_span, RequestID,
        },
    },
    ConnectorLicense, IpcStreamWorker, Parser,
};
use tonic::{Status, Streaming};
use tracing::{instrument, Instrument, Span};

use crate::serve::{
    controller::{transferred::ConnectorTransferred, TaskActivity, TaskControllerRef, TaskDetail},
    scheduler::agent::AgentNotifySender,
};
use taosx_core::plugins::transform::parse::{cast, FieldParser, ParserImpl};

#[derive(Debug)]
pub struct PutStream {
    req: Streaming<FlightData>,
    controller: TaskControllerRef,
    task_id: i64,
    notify_sender: AgentNotifySender,
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct AppMetadata {
    data_trace_id: u64,
}

impl PutStream {
    pub(super) fn new(
        controller: TaskControllerRef,
        task_id: i64,
        req: Streaming<FlightData>,
        notify_sender: AgentNotifySender,
    ) -> Self {
        Self {
            req,
            controller,
            task_id,
            notify_sender,
        }
    }

    #[instrument(skip_all, name="put_stream", fields(task.id=%self.task_id))]
    pub async fn into_flight_put_result(
        self,
        stream_trace_id: String,
    ) -> anyhow::Result<impl Stream<Item = Result<PutResult, Status>> + std::marker::Send> {
        // todo: directly use task detail instead of id.
        // dbg!(&self.task_id);
        set_data_trace_id_for_current_span(stream_trace_id.as_str());
        tracing::info!("Put stream by task id {}", self.task_id,);
        let task = self
            .controller
            .get(self.task_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))
            .unwrap()
            .unwrap();
        // dbg!(&task);

        let builder = TaosBuilder::from_dsn(&task.to)?;
        let pool = builder.pool()?;

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
            .ok_or_else(|| anyhow::format_err!("Cannot find agent id for task {}", self.task_id))?;
        // return self.req.map_ok(|data| PutResult {
        //     app_metadata: data.app_metadata,
        // });
        let mut stream = arrow_flight::decode::FlightDataDecoder::new(self.req.map_err(Into::into));
        // let schema = stream.schema();
        // dbg!(schema);

        let lock = Arc::new(tokio::sync::Mutex::new(()));

        // data channel
        let (tx, rx) = flume::bounded(1024);
        let tx = Arc::new(tx);
        // response channel
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

        tracing::trace!(schema = ?schema, "parsing put stream schema");
        let tx_cloned = Arc::downgrade(&tx);
        let taos = pool.get().await?;
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

        let ipc_error_strategy = IpcErrorStrategy::from(connector);

        // let should_abort = Arc::new(AtomicBool::new(false));
        let (abort_message_tx, abort_message_rx) = flume::bounded(1);

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
                self.controller
                    .transferred
                    .get(&(cluster_id, from_dsn.driver.clone()))
                    .await
            }
            _ => None,
        };

        async fn ipc_stream_writer(
            notify_sender: AgentNotifySender,
            agent_id: i64,
            task: TaskDetail,
            pool: &taos::TaosPool,
            lock: Arc<tokio::sync::Mutex<()>>,
            schema: Arc<arrow::datatypes::Schema>,
            tx: Weak<flume::Sender<(arrow::record_batch::RecordBatch, u64)>>,
            rx: flume::Receiver<(arrow::record_batch::RecordBatch, u64)>,
            // rsp_tx: flume::Sender<anyhow::Result<()>>,
            license: Option<ConnectorLicense>,
            _transferred: Option<Arc<ConnectorTransferred>>,
            span: tracing::Span,
            abort_message_tx: flume::Sender<Result<PutResult, Status>>,
            ipc_error_strategy: IpcErrorStrategy,
            stream_trace_id_u64: u64,
        ) -> anyhow::Result<()> {
            // dbg!(&task);
            notify_sender.send(crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                agent_id,
                TaskActivity::ipc_started(task.id),
            ))?;
            let task_id = task.id;
            let from = task.from.parse().unwrap();
            let taos = pool.get().await?;
            let _ = span.clone().entered();
            let worker = IpcStreamWorker::new(
                pool.clone(),
                from,
                lock,
                schema,
                license,
                None,
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
            let lush_parser: Option<ParserImpl> = metadata.init().map(|init| {
                let columns = init.columns();
                let fields: LinkedHashMap<String, FieldParser> = columns
                    .iter()
                    .map(|(col_name, ipc_type)| {
                        (
                            col_name.clone(),
                            FieldParser::Cast(cast::Cast::new(ipc_type.clone())),
                        )
                    })
                    .collect();
                ParserImpl::new(fields)
            });
            let lush_parser = lush_parser.as_ref();
            if worker.lush_model_config.get().is_none() {
                if let Some(sql) = metadata.init_sql_string() {
                    let init = metadata.init().unwrap();
                    let req_id = RequestID::new(stream_trace_id_u64);
                    handle_lush_message_init(init, &taos, &sql, &req_id, metrics).await?;
                }
            }

            if let Some(parser) = lush_parser {
                tracing::info!(
                    "Start IPC stream writer with lush parser: {}",
                    serde_json::to_string(parser).unwrap()
                );
            } else {
                tracing::info!("Start IPC stream writer");
            }

            let stream = rx.stream();

            use futures::StreamExt;

            // limit = cores/2 in [4, 32], default 4.
            let limit = std::thread::available_parallelism()
                .map(|v| {
                    (v.get() / 2).max(4).min(
                        std::env::var("GRPC_WORKERS_CONCURRENCY")
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(32),
                    )
                })
                .unwrap_or(4);
            // Continue to process the next batch if the previous batch has error.
            let contiguous_errors = Arc::new(std::sync::atomic::AtomicU32::new(0));
            if let Err(err) = stream
                .map(|(record, trace_id)| {
                    let trace_id_str = get_data_trace_id_str(trace_id);
                    tracing::info!(
                        num.rows = record.num_rows(),
                        num.columns = record.num_columns(),
                        "Writing batch {trace_id_str}"
                    );
                    tracing::debug!(columns = ?record.columns()); // debug
                    anyhow::Ok((
                        record,
                        trace_id,
                        trace_id_str,
                        worker.clone(),
                        parser.clone(),
                        notify_sender.clone(),
                        tx.clone(),
                        abort_message_tx.clone(),
                        contiguous_errors.clone(),
                    ))
                })
                .try_for_each_concurrent(
                    limit,
                    |(
                        record,
                        trace_id,
                        trace_id_str,
                        worker,
                        parser,
                        notify_sender,
                        _tx,
                        abort_message_tx,
                        contiguous_errors,
                    )| {
                        async move {
                            if let Err(err) = worker
                                .process_record(
                                    record.clone(),
                                    parser.as_deref(),
                                    trace_id,
                                    &trace_id_str,
                                    metrics,
                                    lush_parser,
                                )
                                .in_current_span()
                                .await
                            {
                                let last_errors = contiguous_errors
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                metrics.add_failed_batches(1);
                                tracing::error!(
                                    continuous_errors = last_errors,
                                    error = format!("{:#}", err),
                                    backtrace = %err.backtrace(),
                                    "Writing batch {} error",
                                    trace_id_str,
                                );
                                let period = match last_errors {
                                    errors if errors < 8 => 8,
                                    errors if errors < 16 => 16,
                                    errors if errors < 32 => 32,
                                    errors if errors < 64 => 64,
                                    _ => 128,
                                };
                                tokio::time::sleep(std::time::Duration::from_millis(period * 8))
                                    .await;
                                let message =
                                    format!("IPC processing record {trace_id_str} error: {err:#}");
                                let _ = notify_sender.send(
                                    crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                                        agent_id,
                                        TaskActivity::warn(task_id, message),
                                    ),
                                );
                                if ipc_error_strategy.will_stop() {
                                    let _ = abort_message_tx.send(Err(Status::cancelled(format!(
                                        "IPC worker will be stopped since{err:#}"
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
                                tracing::warn!("Abort record {trace_id_str}");
                                // if let Some(tx) = tx.upgrade() {
                                //     tracing::warn!(
                                //         trace_id = trace_id_str,
                                //         "Re-queue with {} rows record",
                                //         record.num_rows()
                                //     );
                                //     tx.send_async((record, trace_id))
                                //         .await
                                //         .context("Re-queue error")?;
                                // } else {
                                //     tracing::warn!(
                                //         trace_id = trace_id_str,
                                //         "IPC channel is closed, cannot re-queue record {trace_id}"
                                //     );
                                // }
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
                                    tracing::debug!(
                                        trace_id = trace_id_str,
                                        "Writing batch success",
                                    );
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
        let notify_sender = self.notify_sender.clone();
        // 任务的 metrics 在启动任务的时候已经放入全局 Map 中，所以这里一定存在
        let metrics_arc = get_metrics(self.task_id).await.expect("metrics not found");
        tokio::spawn(
            async move {
                let stream_trace_id_u64 = get_stream_id_u64(stream_trace_id.as_str());
                let task_id = task.id;
                if let Err(err) = ipc_stream_writer(
                    notify_sender.clone(),
                    agent_id,
                    task,
                    &pool,
                    lock,
                    schema,
                    tx_cloned,
                    rx.clone(),
                    // rsp_tx,
                    license,
                    transferred,
                    Span::current(),
                    abort_message_tx,
                    ipc_error_strategy,
                    stream_trace_id_u64,
                )
                .in_current_span()
                .await
                {
                    tracing::error!("IPC stream writer stopped, err:{:#?}", err);
                }
                let _ =
                    notify_sender.send(crate::serve::scheduler::agent::AgentNotify::TaskActivity(
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
            .in_current_span(),
        );
        let cur_span = Span::current();

        let stream = stream
            .then(move |message| {
                let metrics = metrics_arc.ipc();
                let message = message.map(|message| {
                    let app_metadata = message.app_metadata();
                    let trace_id: u64 = get_trace_id_from_app_meta(&app_metadata);
                    cur_span.in_scope(|| {
                        metrics.add_received_batches(1);
                        tracing::debug!("Receive batch {}", get_data_trace_id_str(trace_id));
                    });
                    (message, app_metadata, trace_id, tx.clone())
                });
                async move {
                    let (message, app_metadata, trace_id, tx) = message?;
                    Ok(match message.payload {
                        arrow_flight::decode::DecodedPayload::RecordBatch(batch) => {
                            if let Err(err) = tx.send_async((batch, trace_id)).await {
                                tracing::warn!(
                                    trace_id = get_data_trace_id_str(trace_id),
                                    ipc.channel.capacity = tx.capacity(),
                                    ipc.channel.len = tx.len(),
                                    ipc.channel.receiver_count = tx.receiver_count(),
                                    ipc.channel.sender_count = tx.sender_count(),
                                    ipc.channel.is_disconnected = tx.is_disconnected(),
                                    "IPC channel sent err: {:#}",
                                    err
                                );

                                return Err(FlightError::ExternalError(Box::new(err)));
                            } else {
                                PutResult { app_metadata }
                            }
                        }
                        _ => PutResult { app_metadata },
                    })
                }
            })
            .map_err(|err: FlightError| {
                tracing::warn!(error.source = format!("{err:#}"), "IPC stream error");
                Status::data_loss(format!("IPC worker seems stopped: {:#}", err))
            })
            .chain(abort_message_rx.into_stream().map_ok(|v| v));

        Ok(stream)
    }
}

unsafe impl Sync for PutStream {}
unsafe impl Send for PutStream {}

fn get_trace_id_from_app_meta(app_metadata: &bytes::Bytes) -> u64 {
    let meta_bytes = app_metadata.as_bytes();
    match serde_json::from_slice::<AppMetadata>(meta_bytes) {
        Ok(app_meta) => app_meta.data_trace_id,
        Err(err) => {
            tracing::error!("parse app metadata error, {}", err);
            0
        }
    }
}
