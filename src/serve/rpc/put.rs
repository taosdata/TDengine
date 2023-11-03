use std::sync::Arc;

use anyhow::Context;
use arrow_flight::{error::FlightError, FlightData, PutResult};
use chrono::Utc;
use futures::{Stream, TryStreamExt};
use futures_util::StreamExt;
use taos::{AsyncBindable, AsyncQueryable, AsyncTBuilder, Dsn, Stmt, TaosBuilder};
use taosx_core::{ConnectorLicense, IpcStreamWorker, Parser, METRICS_TIME_START};
use tonic::{Status, Streaming};
use tracing::{debug, Instrument};

use crate::serve::{
    controller::{transferred::ConnectorTransferred, TaskActivity, TaskControllerRef, TaskDetail},
    scheduler::agent::AgentNotifySender,
};

#[derive(Debug)]
pub struct PutStream {
    req: Streaming<FlightData>,
    controller: TaskControllerRef,
    task_id: i64,
    notify_sender: AgentNotifySender,
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
        let (tx, rx) = flume::bounded(1000000);
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

        debug!(schema = ?schema, "parsing put stream schema");
        let tx_cloned = tx.clone();
        let taos = pool.get().await?;
        let from_dsn: Dsn = task.from.parse()?;
        let to_dsn: Dsn = task.to.parse()?;

        let connector = match from_dsn.driver.as_str() {
            "opcda" => Some("opc_da"),
            "opcua" => Some("opc_ua"),
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

        // todo! add trace id to
        let span = tracing::info_span!(
            "task::spawned",
            task.id = task.id,
            trace_id = tracing::field::Empty
        );

        // let transferred = self.controller.transferred.get((cluster_id, ))
        let span_clone = span.clone();
        async fn ipc_stream_writer(
            notify_sender: AgentNotifySender,
            agent_id: i64,
            task: TaskDetail,
            pool: &taos::TaosPool,
            lock: Arc<tokio::sync::Mutex<()>>,
            schema: Arc<arrow::datatypes::Schema>,
            tx: flume::Sender<arrow::record_batch::RecordBatch>,
            rx: flume::Receiver<arrow::record_batch::RecordBatch>,
            // rsp_tx: flume::Sender<anyhow::Result<()>>,
            license: Option<ConnectorLicense>,
            _transferred: Option<Arc<ConnectorTransferred>>,
            span: tracing::Span,
        ) -> anyhow::Result<()> {
            // dbg!(&task);
            metrics::gauge!(METRICS_TIME_START, Utc::now().timestamp_millis() as f64);
            let from = task.from.parse().unwrap();
            let taos = pool.get().await?;
            let mut stmt = Stmt::init(&taos).await.context("Initialize STMT")?;
            let _ = span.clone().entered();

            let worker = IpcStreamWorker::new(
                pool.clone(),
                from,
                lock,
                schema,
                license,
                None,
                span.clone(),
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
                    Ok(record) => {
                        tracing::debug!(columns = ?record.columns(), num.rows = record.num_rows(), num.columns = record.num_columns(),
                                "Start writing records");
                        if let Err(err) = worker
                            .process_record(&mut stmt, record.clone(), parser.as_ref())
                            .await
                        {
                            tracing::warn!(
                                error.message = format!("{err:#}"),
                                error.root_cause = err.root_cause(),
                                backtrace = format!("{}", err.backtrace())
                            );
                            let _ = notify_sender.send(
                                crate::serve::scheduler::agent::AgentNotify::TaskActivity(
                                    agent_id,
                                    TaskActivity::error(task.id, format!("{err:#}")),
                                ),
                            );
                            tracing::warn!("Can't write batch to database, err: {}", err);
                            tx.send_async(record).await?;
                        }
                    }
                    Err(err) => {
                        tracing::warn!("IPC stream worker stopped, err:{}", err.to_string());
                        break Ok(());
                    }
                }
            }
        }
        let notify_sender = self.notify_sender.clone();
        tokio::spawn(
            async move {
                if let Err(err) = ipc_stream_writer(
                    notify_sender,
                    agent_id,
                    task,
                    &pool,
                    lock,
                    schema,
                    tx_cloned,
                    rx,
                    // rsp_tx,
                    license,
                    transferred,
                    span,
                )
                .in_current_span()
                .await
                {
                    tracing::warn!("IPC stream writer stopped, err:{:?}", err);
                }
            }
            .instrument(span_clone),
        );

        let stream = stream
            .zip(futures::stream::repeat(tx))
            .map(|(message, tx)| {
                let message = message?;
                let app_metadata = message.app_metadata();
                Ok(match message.payload {
                    arrow_flight::decode::DecodedPayload::RecordBatch(batch) => {
                        if let Err(err) = tx.send(batch) {
                            tracing::warn!("Channel send err: {}", err.to_string());
                            return Err(FlightError::ExternalError(Box::new(err)));
                        } else {
                            PutResult { app_metadata }
                        }
                    }
                    _ => PutResult { app_metadata },
                })
            })
            .map_err(|err: FlightError| {
                tracing::warn!(error.message = format!("{err:#}"));
                Status::data_loss(format!("{}", err))
            });

        Ok(stream)
    }
}

unsafe impl Sync for PutStream {}
unsafe impl Send for PutStream {}
