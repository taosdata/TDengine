use std::sync::Arc;

use actix_web::App;
use anyhow::Context;
use arrow_flight::{error::FlightError, FlightData, PutResult};
use chrono::Utc;
use futures::{Stream, TryStreamExt};
use futures_util::StreamExt;
use parquet::data_type::AsBytes;
use taos::{AsyncBindable, AsyncQueryable, AsyncTBuilder, Dsn, Stmt, TaosBuilder};
use taosx_core::{
    utils::trace::{get_data_trace_id_str, set_data_trace_id_for_current_span},
    ConnectorLicense, IpcStreamWorker, Parser, METRICS_TIME_START,
};
use tonic::{Status, Streaming};
use tracing::{debug, instrument, Instrument, Span};

use crate::serve::controller::{transferred::ConnectorTransferred, TaskControllerRef, TaskDetail};

#[derive(Debug)]
pub struct PutStream {
    req: Streaming<FlightData>,
    controller: TaskControllerRef,
    task_id: i64,
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
    ) -> Self {
        Self {
            req,
            controller,
            task_id,
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
        tracing::info!(
            "Put stream by task id {}",
            self.task_id,
        );
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
            debug!(schema = ?schema, "parsing put stream schema");
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

            // let transferred = self.controller.transferred.get((cluster_id, ))
            async fn ipc_stream_writer(
                task: TaskDetail,
                pool: &taos::TaosPool,
                lock: Arc<tokio::sync::Mutex<()>>,
                schema: Arc<arrow::datatypes::Schema>,
                rx: flume::Receiver<(arrow::record_batch::RecordBatch, u64)>,
                rsp_tx: flume::Sender<anyhow::Result<()>>,
                license: Option<ConnectorLicense>,
                _transferred: Option<Arc<ConnectorTransferred>>,
                span: tracing::Span,
            ) -> anyhow::Result<()> {
                // dbg!(&task);
                metrics::gauge!(METRICS_TIME_START, Utc::now().timestamp_millis() as f64);
                let from = task.from.parse().unwrap();
                let taos = pool.get().await?;
                let mut stmt = Stmt::init(&taos).await.context("Initialize STMT")?;
                let worker = IpcStreamWorker::new(
                    pool.clone(),
                    from,
                    lock,
                    schema,
                    license,
                    None,
                    Span::current(),
                    Some(task.id),
                )
                .await?;
                let parser: Option<Parser> = task
                    .parser
                    .as_ref()
                    .map(|v| serde_json::from_value(v.clone()).unwrap());
                tracing::info!("Start IPC stream writer");
                loop {
                    match rx.recv_async().await {
                        Ok((record, trace_id)) => {
                            tracing::debug!(columns = ?record.columns(), num.rows = record.num_rows(), num.columns = record.num_columns());
                            if let Err(err) = worker
                                .process_record(&mut stmt, record, parser.as_ref(), trace_id)
                                .await
                            {
                                tracing::warn!("Write stream error: {err}");
                                let _ = rsp_tx.send_async(Err(err)).await;
                            } else {
                                let _ = rsp_tx.send_async(Ok(())).await;
                            }
                        }
                        Err(err) => {
                            tracing::warn!("IPC stream worker stopped, err:{}", err.to_string());
                            break Ok(());
                        }
                    }
                }
            }
            tokio::spawn(
                async move {
                    if let Err(err) = ipc_stream_writer(
                        task,
                        &pool,
                        lock,
                        schema,
                        rx,
                        rsp_tx,
                        license,
                        transferred,
                        Span::current(),
                    )
                    .in_current_span()
                    .await
                    {
                        tracing::warn!("IPC stream writer stopped, err:{:?}", err);
                    }
                }.in_current_span(),
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
                        // dbg!(&message);
                        let app_metadata = message.app_metadata();
                        let trace_id: u64 = get_trace_id_from_app_meta(&app_metadata);
                        let trace_id_str = get_data_trace_id_str(trace_id);
                        tracing::info!("receive batch {trace_id_str}");
                        match message.payload {
                            arrow_flight::decode::DecodedPayload::None => None,
                            arrow_flight::decode::DecodedPayload::Schema(schema) => {
                                dbg!(schema);
                                Some(Ok(PutResult { app_metadata }))
                            }
                            arrow_flight::decode::DecodedPayload::RecordBatch(batch) => {
                                if let Err(err) = tx.send_async((batch, trace_id)).await {
                                    tracing::warn!(
                                        "into_flight_put_result channel send err: {}",
                                        err.to_string()
                                    );
                                    Some(Err(FlightError::ExternalError(Box::new(err))))
                                } else {
                                    rsp_rx
                                        .recv_async()
                                        .await
                                        .map_err(|err| {
                                            tracing::warn!(
                                                "IPC stream worker stopped, err:{}",
                                                err.to_string()
                                            );
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
                    Err(err) => {
                        tracing::warn!("Flight error: {:#}", err);
                        Some(Err(err))
                    }
                };

                if let Some(item) = item {
                    let is_err = item.is_err();
                    if p_tx.send_async(item).await.is_err() {
                        tracing::info!("into_flight_put_result channel closed");
                        break;
                    }
                    if is_err {
                        tracing::warn!("Flight error, break");
                        break;
                    }
                }
            }
            tracing::info!("IPC stream writer stopped");
        }.in_current_span());
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
        //                             tracing::warn!(
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

fn get_trace_id_from_app_meta(app_metadata: &bytes::Bytes) -> u64 {
    let meta_bytes = app_metadata.as_bytes();
    match serde_json::from_slice::<AppMetadata>(meta_bytes) {
        Ok(app_meta)  => app_meta.data_trace_id,
        Err(err) => {
            tracing::error!("parse app metadata error, {}", err);
            0
        }
    }
}
