use std::sync::{Arc, Mutex};

use anyhow::Context;
use arrow::ipc::RecordBatch;
use arrow_flight::{FlightData, PutResult};
use futures::{Stream, TryStreamExt};
use taos::{AsyncTBuilder, Bindable, Stmt, TaosBuilder, TaosPool};
use taosx_core::{IpcStreamWorker, Parser};
use tonic::{Status, Streaming};

use crate::serve::controller::{Task, TaskControllerRef, TaskDetail};

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
        dbg!(&self.task_id);
        let task = self
            .controller
            .get(self.task_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))
            .unwrap()
            .unwrap();
        dbg!(&task);

        let pool = TaosBuilder::from_dsn(&task.to)?.pool()?;

        // return self.req.map_ok(|data| PutResult {
        //     app_metadata: data.app_metadata,
        // });
        let mut stream = arrow_flight::decode::FlightDataDecoder::new(self.req.map_err(Into::into));
        let schema = stream.schema();
        dbg!(schema);

        let lock = Arc::new(tokio::sync::Mutex::new(()));

        let (tx, rx) = flume::bounded(100);

        let schema = stream
            .try_next()
            .await?
            .ok_or_else(|| anyhow::format_err!("Invalid IPC stream"))?;
        if let arrow_flight::decode::DecodedPayload::Schema(schema) = schema.payload {
            let taos = pool.get().await?;
            async fn ipc_stream_writer(
                task: TaskDetail,
                taos: &taos::Taos,
                lock: Arc<tokio::sync::Mutex<()>>,
                schema: Arc<arrow::datatypes::Schema>,
                rx: flume::Receiver<arrow::record_batch::RecordBatch>,
            ) -> anyhow::Result<()> {
                dbg!(&task);
                let from = task.from.parse().unwrap();
                let mut stmt = Stmt::init(taos).context("Initialize STMT")?;
                let worker = IpcStreamWorker::new(&taos, from, lock, schema).unwrap();
                dbg!(&task);
                let parser :Option<Parser> = task.parser.as_ref().map(|v| serde_json::from_value(v.clone()).unwrap());
                loop {
                    if let Ok(a) = rx.recv() {
                        log::info!("Start writing records: {a:?}");
                        if let Err(err) = worker.process_record(&mut stmt, a, parser.as_ref()).await {
                            log::warn!("Write stream error: {err}");
                        }
                    } else {
                        log::warn!("IPC stream worker stopped");
                        break Ok(());
                    }
                }
            }
            tokio::spawn(async move { ipc_stream_writer(task, &taos, lock, schema, rx).await });
        }

        Ok(stream
            .map_ok(move |message| {
                // message.payload
                let app_metadata = message.app_metadata();
                match message.payload {
                    arrow_flight::decode::DecodedPayload::None => todo!(),
                    arrow_flight::decode::DecodedPayload::Schema(schema) => {
                        dbg!(schema);
                    }
                    arrow_flight::decode::DecodedPayload::RecordBatch(batch) => {
                        dbg!(&batch);
                        if let Err(err) = tx.send(batch) {
                            log::warn!("into_flight_put_result channel send err: {}", err.to_string());
                        }
                    }
                }
                // let app_metadata = message.app_metadata;
                PutResult { app_metadata }
            })
            .map_err(|err| Status::from_error(Box::new(err))))
    }
}

unsafe impl Sync for PutStream {}
unsafe impl Send for PutStream {}
