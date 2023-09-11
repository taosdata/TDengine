use std::sync::Arc;

use anyhow::Context;
use arrow_flight::{error::FlightError, FlightData, PutResult};
use chrono::Utc;
use futures::{Stream, TryStreamExt};
use futures_util::StreamExt;
use taos::{AsyncBindable, AsyncQueryable, AsyncTBuilder, Dsn, Stmt, TaosBuilder};
use taosx_core::{ConnectorLicense, IpcStreamWorker, Parser, METRICS_TIME_START};
use tonic::{Status, Streaming};
use tracing::Instrument;

use crate::serve::controller::{transferred::ConnectorTransferred, TaskControllerRef, TaskDetail};

#[derive(Debug)]
pub struct PutStream {
    req: Streaming<FlightData>,
    controller: TaskControllerRef,
    task_id: i64,
}

impl PutStream {
    pub(super) fn new(
        controller: TaskControllerRef,
        task_id: i64,
        req: Streaming<FlightData>,
    ) -> Self {
        Self {
            req,
            controller,
            task_id,
        }
    }
    pub async fn into_flight_put_result(
        self,
    ) -> anyhow::Result<impl Stream<Item = Result<PutResult, Status>> + std::marker::Send> {
        // todo: directly use task detail instead of id.
        // dbg!(&self.task_id);
        tracing::debug!("Put stream by id {}", self.task_id);

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
                .map_err(|err| anyhow::format_err!("Cannot retrieve cluster id: {err}"))?
                .unwrap()
        };
        // return self.req.map_ok(|data| PutResult {
        //     app_metadata: data.app_metadata,
        // });
        let mut stream = arrow_flight::decode::FlightDataDecoder::new(self.req.map_err(Into::into));
        // let schema = stream.schema();
        // dbg!(schema);

        let lock = Arc::new(tokio::sync::Mutex::new(()));

        // data channel
        let (tx, rx) = flume::bounded(100);
        // response channel
        let (rsp_tx, rsp_rx) = flume::bounded(100);

        let schema = stream
            .try_next()
            .await?
            .ok_or_else(|| anyhow::format_err!("Invalid IPC stream"))?;
        if let arrow_flight::decode::DecodedPayload::Schema(schema) = schema.payload {
            let taos = pool.get().await?;
            let from_dsn: Dsn = task.from.parse()?;
            let to_dsn: Dsn = task.to.parse()?;

            let connector = match from_dsn.driver.as_str() {
                "opcda" => Some("opc_da"),
                "opcua" => Some("opc_ua"),
                "mqtt" => Some("mqtt"),
                "influxdb" => Some("influxdb"),
                "opentsdb" => Some("opentsdb"),
                "kafka" => Some("kafka"),
                "pi" => Some("pi"),
                _ => None,
            };

            let license: Option<ConnectorLicense> = if let Some(connector) = connector {
                #[cfg(feature = "disable-enterprise-connector-validation")]
                let license: Option<ConnectorLicense> = None;
                #[cfg(not(feature = "disable-enterprise-connector-validation"))]
                let license: Option<ConnectorLicense> =
                    if to_dsn.get("token").is_some() && to_dsn.protocol.is_some() {
                        None
                    } else {
                        taos.query_one::<_, String>(&format!(
                            "select {connector} from information_schema.ins_grants"
                        ))
                        .await
                        .unwrap_or(None)
                        .and_then(|s| serde_json::from_str(&s).ok())
                    };

                if let Some(license) = license {
                    if license.is_expired() {
                        anyhow::bail!(
                            "Connector expired, please contact the database administrator for license"
                        )
                    }
                    None
                } else {
                    None
                }
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

            // todo! add trace id to
            let span = tracing::info_span!(
                "task::spawned",
                task.id = task.id,
                trace_id = tracing::field::Empty
            );

            // let transferred = self.controller.transferred.get((cluster_id, ))
            let span_clone = span.clone();
            async fn ipc_stream_writer(
                task: TaskDetail,
                pool: &taos::TaosPool,
                lock: Arc<tokio::sync::Mutex<()>>,
                schema: Arc<arrow::datatypes::Schema>,
                rx: flume::Receiver<arrow::record_batch::RecordBatch>,
                rsp_tx: flume::Sender<anyhow::Result<()>>,
                license: Option<ConnectorLicense>,
                transferred: Option<Arc<ConnectorTransferred>>,
                span: tracing::Span,
            ) -> anyhow::Result<()> {
                // dbg!(&task);
                metrics::gauge!(METRICS_TIME_START, Utc::now().timestamp_millis() as f64);
                let from = task.from.parse().unwrap();
                let taos = pool.get().await?;
                let mut stmt = Stmt::init(&taos).await.context("Initialize STMT")?;
                let _ = span.clone().entered();

                let worker = IpcStreamWorker::new(
                    &pool,
                    from,
                    lock,
                    schema,
                    license.as_ref(),
                    transferred.as_deref(),
                    span.clone(),
                )
                .unwrap();
                dbg!(&task);
                let parser: Option<Parser> = task
                    .parser
                    .as_ref()
                    .map(|v| serde_json::from_value(v.clone()).unwrap());
                tracing::info!("Start IPC stream writer");
                loop {
                    match rx.recv_async().await {
                        Ok(record) => {
                            log::info!("Start writing records: {record:?}");
                            if let Err(err) = worker
                                .process_record(&mut stmt, record, parser.as_ref())
                                .await
                            {
                                log::warn!("Write stream error: {err}");
                                let _ = rsp_tx.send_async(Err(err)).await;
                            } else {
                                let _ = rsp_tx.send_async(Ok(())).await;
                            }
                        }
                        Err(err) => {
                            log::warn!("IPC stream worker stopped, err:{}", err.to_string());
                            break Ok(());
                        }
                    }
                }
            }
            tokio::spawn(
                async move {
                    ipc_stream_writer(
                        task,
                        &pool,
                        lock,
                        schema,
                        rx,
                        rsp_tx,
                        license,
                        transferred,
                        span,
                    )
                    .in_current_span()
                    .await
                }
                .instrument(span_clone),
            );
            // let _ = span.enter();
        } else {
            anyhow::bail!("Invalid IPC stream");
        }

        // stream;

        let (p_tx, p_rx) = flume::bounded(10);
        tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                let item = match message {
                    Ok(message) => {
                        dbg!(&message);
                        let app_metadata = message.app_metadata();
                        match message.payload {
                            arrow_flight::decode::DecodedPayload::None => None,
                            arrow_flight::decode::DecodedPayload::Schema(schema) => {
                                dbg!(schema);
                                Some(Ok(PutResult { app_metadata }))
                            }
                            arrow_flight::decode::DecodedPayload::RecordBatch(batch) => {
                                if let Err(err) = tx.send_async(batch).await {
                                    log::warn!(
                                        "into_flight_put_result channel send err: {}",
                                        err.to_string()
                                    );
                                    Some(Err(FlightError::ExternalError(Box::new(err))))
                                } else {
                                    rsp_rx
                                        .recv_async()
                                        .await
                                        .map_err(|err| {
                                            FlightError::from_external_error(Box::new(err))
                                        })
                                        .and_then(|res| {
                                            res.map_err(|err| {
                                                FlightError::Tonic(Status::invalid_argument(
                                                    format!("{err:#}"),
                                                ))
                                            })
                                            .map(|_| PutResult { app_metadata })
                                        })
                                        .map(Some)
                                        .transpose()
                                }
                            }
                        }
                    }
                    Err(err) => Some(Err(err)),
                };
                if let Some(item) = item {
                    if p_tx.send_async(item).await.is_err() {
                        log::info!("into_flight_put_result channel closed");
                        break;
                    }
                }
            }
            tracing::info!("IPC stream writer stopped");
        });
        tokio::task::yield_now().await;

        Ok(p_rx
            .into_stream()
            .map_err(|err| Status::from_error(Box::new(err))))

        // let tx = Arc::new(tx);
        // // let cloned_tx = tx.clone();
        // let rsp_rx = Arc::new(rsp_rx);
        // let channel = (tx, rsp_rx);
        // let iter = std::iter::repeat(channel.clone());
        // let rx_iter = futures::stream::iter(iter);

        // Ok(stream
        //     .zip(rx_iter)
        //     .filter_map(|(message, (tx, rsp_rx))| async move {
        //         match message {
        //             Ok(message) => {
        //                 dbg!(&message);
        //                 let app_metadata = message.app_metadata();
        //                 match message.payload {
        //                     arrow_flight::decode::DecodedPayload::None => todo!(),
        //                     arrow_flight::decode::DecodedPayload::Schema(schema) => {
        //                         dbg!(schema);
        //                     }
        //                     arrow_flight::decode::DecodedPayload::RecordBatch(batch) => {
        //                         // dbg!(&batch);
        //                         if let Err(err) = tx.send_async(batch).await {
        //                             log::warn!(
        //                                 "into_flight_put_result channel send err: {}",
        //                                 err.to_string()
        //                             );
        //                             return Some(Err(FlightError::ExternalError(Box::new(err))));
        //                         } else {
        //                             return rsp_rx
        //                                 .recv_async()
        //                                 .await
        //                                 .map_err(|err| {
        //                                     FlightError::from_external_error(Box::new(err))
        //                                 })
        //                                 .and_then(|res| {
        //                                     res.map_err(|err| {
        //                                         FlightError::Tonic(Status::invalid_argument(
        //                                             format!("{err:#}"),
        //                                         ))
        //                                     })
        //                                     .map(|_| PutResult { app_metadata })
        //                                 })
        //                                 .map(Some)
        //                                 .transpose();
        //                         }
        //                     }
        //                 }
        //                 // let app_metadata = message.app_metadata;
        //                 Some(Ok(PutResult { app_metadata }))
        //             }
        //             Err(err) => Some(Err(err)),
        //         }
        //     })
        //     .map_err(|err| Status::from_error(Box::new(err))))
    }
}

unsafe impl Sync for PutStream {}
unsafe impl Send for PutStream {}
