use std::{
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
    task::Poll,
};

use arrow::{
    array::{ArrayRef, StringArray, TimestampMillisecondArray},
    datatypes::{Field, Fields, Schema},
    record_batch::RecordBatch,
};
use async_backtrace::framed;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::Deserialize;
use taosx_core::ListResponse;
#[cfg(unix)]
use tokio::net::UnixListener;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use arrow_flight::{
    decode::FlightDataDecoder,
    encode::{FlightDataEncoder, FlightDataEncoderBuilder},
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};

use crate::serve::{
    controller::{
        agent::{Agent, AgentToken},
        TaskStatus,
    },
    rpc::put::PutStream,
};

use super::controller::{AgentAction, TaskControllerRef};

mod put;

#[derive(Clone)]
pub(super) struct FlightServiceImpl {
    controller: TaskControllerRef,
}

// impl FlightServiceImpl {
//     pub(super) fn new(controller: TaskControllerRef) -> Self {
//         Self { controller }
//     }
// }

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    async fn handshake(
        &self,
        req: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let addr = req.remote_addr();
        let (meta, _extensions, mut req) = req.into_parts();

        let req = req.message().await?;

        if let Some(req) = req {
            let mut res = HandshakeResponse {
                protocol_version: req.protocol_version,
                payload: req.payload,
            };
            let agent = self
                .controller
                .get_agent_with_token(&AgentToken::from(&res.payload))
                .await
                .map_err(|err| Status::permission_denied(format!("Invalid token: {err}")))?
                .ok_or_else(|| Status::permission_denied("Agent not found"))?;
            res.payload = serde_json::to_vec(&agent).unwrap().into();
            let handshake_stream = futures::stream::once(async { Ok(res) });
            return Ok(Response::new(Box::pin(handshake_stream)));
        }
        Err(Status::permission_denied("Token not found"))
    }
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get"))
    }

    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;

    async fn do_put(
        &self,
        req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let (meta, _extension, req) = req.into_parts();

        let task_id = meta
            .get("x-task-id")
            .ok_or_else(|| Status::unavailable("Task id should be set"))
            .unwrap();
        let task_id: i64 = task_id.to_str().unwrap().parse().unwrap();

        // let message = req.try_next().await?;

        let put_stream = PutStream::new(self.controller.clone(), task_id, req);

        struct ResultStream(Streaming<FlightData>);
        unsafe impl Sync for ResultStream {}
        unsafe impl Send for ResultStream {}

        // impl futures::Stream for ResultStream {
        //     type Item = Result<PutResult, Status>;
        // }

        Ok(Response::new(Box::pin(
            put_stream
                .into_flight_put_result()
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?,
            // req.map_ok(|v| PutResult {
            //     app_metadata: v.app_metadata,
            // }),
        )))
    }

    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    async fn do_exchange(
        &self,
        req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let (meta, extension, req) = req.into_parts();

        let token = meta
            .get("x-token")
            .unwrap()
            .to_str()
            .map_err(|err| Status::aborted(format!("Invalid token: {err}")))?;


        let controller = self.controller.clone();
        let agent = controller
            .get_agent_with_token(&AgentToken(token.to_string()))
            .await
            .map_err(|err| Status::permission_denied(format!("Token error: {err}")))?
            .ok_or_else(|| Status::permission_denied(format!("Agent has been deleted")))?;

        // let agent: Agent = serde_json::from_str(r#"
        // {
        //     "id": 2, "dsn": "taos:///", "name": "agent1", "cluster_id":"", "user_id":"", "connectors": [], "created_at":"2022-02-02T00:00:00Z"
        // }"#).unwrap();

        let (tx, rx) = flume::bounded(100);

        // let sender = tx.clone();
        let controller_runner = controller.clone();
        let agent_id = agent.id;
        tokio::spawn(async move {
            // let agent = controller_runner.get_agent_by_id(agent_id).await?;
            // let schema = Arc::new(Schema::new(Fields::from(vec![
            //     Field::new(
            //         "ts",
            //         arrow::datatypes::DataType::Timestamp(
            //             arrow::datatypes::TimeUnit::Millisecond,
            //             None,
            //         ),
            //         false,
            //     ),
            //     Field::new("action", arrow::datatypes::DataType::Utf8, false),
            //     Field::new("context", arrow::datatypes::DataType::Utf8, false),
            // ])));
            let encoder = FlightDataDecoder::new(req.map_err(FlightError::Tonic));
            let _ = encoder
                .try_for_each_concurrent(1, |data| async {
                    let payload = data.payload;
                    match payload {
                        arrow_flight::decode::DecodedPayload::None => (),
                        arrow_flight::decode::DecodedPayload::Schema(_) => (),
                        arrow_flight::decode::DecodedPayload::RecordBatch(res) => {
                            let rows = res.num_rows();
                            debug_assert!(rows == 1);

                            let ts = res
                                .column(0)
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap();
                            let action = res
                                .column(1)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            let context = res
                                .column(2)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            for _ in 0..rows {
                                let (ts, action, context) = (
                                    ts.value_as_datetime_with_tz(
                                        0,
                                        ts.timezone().unwrap_or("UTC").parse().unwrap(),
                                    )
                                    .unwrap(),
                                    action.value(0),
                                    context.value(0),
                                );

                                log::info!("At [{ts}] action `{action}` triggered");
                                match action {
                                    "list" => {
                                        let req: ListResponse =
                                            serde_json::from_str(&context).unwrap();

                                        if let Some((_, sender)) = controller_runner
                                            .agent_tasks
                                            .read()
                                            .await
                                            .get(&agent_id)
                                            .unwrap()
                                            .datasets
                                            .remove(&req.req)
                                        {
                                            let _ = sender.send(req.res).unwrap();
                                        }
                                    }
                                    "heartbeat" => {
                                        //
                                    }
                                    _ => unreachable!(),
                                }
                            }
                            // batch.
                            // todo: send data to controller.
                        }
                    }
                    Ok(())
                })
                .await;
            Ok::<_, anyhow::Error>(())
        });
        let stream: Self::DoExchangeStream = Box::pin(IpcStream::new(rx));
        let response = tonic::Response::from_parts(meta, stream, extension);
        struct IpcStream {
            // request: Streaming<FlightData>,
            encoder: FlightDataEncoder,
            marker: AtomicUsize,
        }

        unsafe impl Send for IpcStream {}
        unsafe impl Sync for IpcStream {}
        impl IpcStream {
            fn new(receiver: flume::Receiver<Result<RecordBatch, FlightError>>) -> Self {
                let schema = Arc::new(Schema::new(Fields::from(vec![
                    Field::new(
                        "ts",
                        arrow::datatypes::DataType::Timestamp(
                            arrow::datatypes::TimeUnit::Millisecond,
                            None,
                        ),
                        false,
                    ),
                    Field::new("action", arrow::datatypes::DataType::Utf8, false),
                    Field::new("context", arrow::datatypes::DataType::Utf8, false),
                ])));

                let encoder = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .build(receiver.into_stream());
                Self {
                    // request,
                    encoder,
                    marker: AtomicUsize::new(0),
                }
            }
        }

        impl futures::Stream for IpcStream {
            type Item = Result<FlightData, Status>;
            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                let c = self
                    .marker
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                log::info!("polled: {c} {cx:?}");

                if c % 2 == 0 {
                    // todo: why this is require?
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                let recv = self.encoder.poll_next_unpin(cx);
                recv.map(|v| v.map(|res| res.map_err(|err| Status::unknown(format!("{err}")))))
            }
        }

        async fn listen_tasks(
            controller: TaskControllerRef,
            agent: Agent,
            tx: flume::Sender<Result<RecordBatch, FlightError>>,
        ) -> anyhow::Result<()> {
            controller.init_agent_worker(agent.id).await;
            let mut receiver = {
                let agent_tasks = controller.agent_tasks.read().await;
                let listener = agent_tasks.get(&agent.id).unwrap();

                // let current = { listener.current.lock().await.clone() };

                for task in listener.current.iter() {
                    if let Some(task) = controller.get(*task.key()).await? {
                        let action: ArrayRef =
                            Arc::new(StringArray::from_iter_values(["run".to_string()]));
                        let context: ArrayRef =
                            Arc::new(StringArray::from_iter_values([serde_json::to_string(
                                &task,
                            )
                            .unwrap()]));
                        let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values([
                            chrono::Utc::now().timestamp_millis(),
                        ]));
                        let batch = RecordBatch::try_from_iter(vec![
                            ("ts", ts),
                            ("action", action),
                            ("context", context),
                        ])
                        .unwrap();

                        if let Err(err) = tx.send_async(Ok(batch)).await {
                            log::warn!("Task listener closed");
                            break;
                        }
                    }
                }
                listener.receiver.resubscribe()
            };

            // /* begin test */
            // let mut tick = tokio::time::interval(Duration::from_millis(500));
            // loop {
            //     tick.tick().await;
            //     let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values([
            //         chrono::Utc::now().timestamp_nanos(),
            //     ]));
            //     let action: ArrayRef = Arc::new(StringArray::from_iter_values(["run".to_string()]));
            //     let context: ArrayRef =
            //         Arc::new(StringArray::from_iter_values(["run".to_string()]));
            //     let batch = RecordBatch::try_from_iter(vec![
            //         ("ts", ts),
            //         ("action", action),
            //         ("context", context),
            //     ])
            //     .unwrap();

            //     if let Err(err) = tx.send_async(Ok(batch)).await {
            //         dbg!(&err);
            //         log::warn!("Task listener closed");
            //         break;
            //     }
            //     continue;
            // } /* end test */
            loop {
                log::info!("Waiting for new task");
                if let Ok(data) = receiver.recv().await {
                    log::info!("{data:?}");

                    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values([
                        chrono::Utc::now().timestamp_millis(),
                    ]));

                    match data {
                        AgentAction::Run(id) => {
                            let task = controller.get(id).await?;
                            if let Some(task) = task {
                                let context: ArrayRef = Arc::new(StringArray::from_iter_values([
                                    serde_json::to_string(&task).unwrap(),
                                ]));
                                let action: ArrayRef =
                                    Arc::new(StringArray::from_iter_values(["run".to_string()]));
                                let batch = RecordBatch::try_from_iter(vec![
                                    ("ts", ts),
                                    ("action", action),
                                    ("context", context),
                                ])
                                .unwrap();

                                if let Err(err) = tx.send_async(Ok(batch)).await {
                                    log::warn!("Task listener closed");
                                    break;
                                }
                            } else {
                                // todo!()
                            }
                        }
                        AgentAction::Cancel(id) => {
                            let task = controller.get(id).await?;
                            if let Some(task) = task {
                                let context: ArrayRef = Arc::new(StringArray::from_iter_values([
                                    serde_json::to_string(&task).unwrap(),
                                ]));
                                let action: ArrayRef =
                                    Arc::new(StringArray::from_iter_values(["cancel".to_string()]));
                                let batch = RecordBatch::try_from_iter(vec![
                                    ("ts", ts),
                                    ("action", action),
                                    ("context", context),
                                ])
                                .unwrap();

                                if let Err(err) = tx.send_async(Ok(batch)).await {
                                    log::warn!("Task listener closed");
                                    break;
                                }
                            } else {
                                // todo!()
                            }
                        }
                        AgentAction::ListDataSets(dataset, _) => {
                            let context: ArrayRef =
                                Arc::new(StringArray::from_iter_values([serde_json::to_string(
                                    &dataset,
                                )
                                .unwrap()]));
                            let action: ArrayRef =
                                Arc::new(StringArray::from_iter_values(["list".to_string()]));
                            let batch = RecordBatch::try_from_iter(vec![
                                ("ts", ts),
                                ("action", action),
                                ("context", context),
                            ])
                            .unwrap();

                            if let Err(err) = tx.send_async(Ok(batch)).await {
                                log::warn!("Task listener closed");
                                break;
                            }
                        }
                        _ => todo!(),
                    }
                } else {
                    break;
                }
            }
            Ok(())
        }

        tokio::spawn(listen_tasks(controller, agent, tx));

        Ok(response)
    }

    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let (meta, part, action) = request.into_parts();
        match action.r#type.as_str() {
            "TaskStatus" => {
                // task.

                let status: TaskStatus = serde_json::from_slice(&action.body)
                    .map_err(|err| Status::invalid_argument(format!("{err}: {:?}", action.body)))?;

                self.controller
                    .push_task_status(&status)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?;
                Ok(Response::new(Box::pin(futures::stream::iter([]))))
            }
            s => Err(Status::unimplemented(format!("Unknown action: {}", s))),
        }
    }

    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }
}

#[derive(Debug, Deserialize)]
pub struct RpcConfig {
    pub tcp: Option<SocketAddr>,
    pub unix: Option<PathBuf>,
}

impl RpcConfig {
    /// Start a Flight gRPC server
    #[framed]
    pub(super) async fn serve_with_controller(
        self,
        controller: TaskControllerRef,
    ) -> Result<(), anyhow::Error> {
        if let Some(tcp) = self.tcp {
            let service = FlightServiceImpl {
                controller: controller.clone(),
            };
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_shutdown(tcp, async {
                    let _ = tokio::signal::ctrl_c().await;
                    tracing::info!("Ctrl+C invoked, shutdown RPC service")
                })
                .await?;
        }
        #[cfg(unix)]
        if let Some(path) = self.unix {
            let uds = UnixListener::bind(path).unwrap();
            let stream = UnixListenerStream::new(uds);
            let service = FlightServiceImpl { controller };
            Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming_shutdown(stream, async {
                    let _ = tokio::signal::ctrl_c().await;
                    tracing::info!("Ctrl+C invoked, shutdown RPC service")
                })
                .await?;
        }
        Ok(())
    }
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            tcp: Some("0.0.0.0:6055".parse().unwrap()),
            unix: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::{Duration, Instant};

    use arrow::array::{ArrayRef, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use arrow::{
        datatypes::{DataType, Field, Schema, SchemaRef},
        ipc::writer::IpcWriteOptions,
    };
    use arrow_flight::decode::FlightDataDecoder;
    use arrow_flight::{
        encode::{FlightDataEncoder, FlightDataEncoderBuilder},
        error::FlightError,
        flight_service_client::FlightServiceClient,
        FlightData, HandshakeRequest,
    };
    use futures::TryStreamExt;
    use tempfile::NamedTempFile;
    use tonic::{
        codegen::Bytes,
        transport::{Channel, Endpoint},
        IntoStreamingRequest,
    };

    // use super::FlightServiceImpl;
    // async fn client_with_uds(path: String) -> FlightServiceClient<Channel> {
    //     let connector = tower::service_fn(move |_| UnixStream::connect(path.clone()));
    //     let channel = Endpoint::try_from("http://[::1]:50051")
    //         .unwrap()
    //         .connect_with_connector(connector)
    //         .await
    //         .unwrap();
    //     FlightServiceClient::new(channel)
    // }
    async fn client_with_tcp() -> FlightServiceClient<Channel> {
        // let connector = tower::service_fn(move |_| TcpStream::connect("127.0.0.1:6051"));
        let channel = Endpoint::try_from("http://127.0.0.1:6051")
            .unwrap()
            .connect()
            .await
            .unwrap();
        // .connect_with_connector(connector)
        // .await
        // .unwrap();
        FlightServiceClient::new(channel)
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn server_client() -> anyhow::Result<()> {
        std::env::set_var("RUST_LOG", "INFO");
        pretty_env_logger::init();
        let file = NamedTempFile::new().unwrap();
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let _ = std::fs::remove_file(path.clone());

        // let uds = UnixListener::bind(path.clone()).unwrap();
        // let stream = UnixListenerStream::new(uds);

        // let controller = TaskControllerRef::from_sqlite("sqlite:memory:")
        //     .await
        //     .unwrap();

        // let task = serde_json::from_str(
        //     r#"{"from": "pi:///", "agent": "localhost:9090", "to": "taos:///pi"}"#,
        // )?;
        // controller.create(task).await?;
        // let service = FlightServiceImpl { controller };
        // let serve_future = Server::builder()
        //     .add_service(FlightServiceServer::new(service))
        //     .serve_with_incoming(stream);

        let request_future = async {
            let mut client = client_with_tcp().await;
            let req = HandshakeRequest::default();
            client
                .handshake(futures::stream::once(async { req }))
                .await
                .unwrap();
            // client.list_flights(Criteria::default()).await.unwrap();

            // futures::stream::repeat();

            // let mut metadata = MetadataMap::new();

            let schema = Arc::new(
                Schema::new(vec![Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                )])
                .with_metadata(HashMap::from_iter([(
                    "x-task-id".to_string(),
                    "1".to_string(),
                )])),
            );
            // schema.with_metadata(metadata)

            // let ipc = arrow::ipc::reader::StreamReader::try_new();
            struct FakeStream(SchemaRef, tokio::time::Interval, Instant);

            impl futures::Stream for FakeStream {
                type Item = Result<RecordBatch, FlightError>;
                fn poll_next(
                    mut self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Option<Self::Item>> {
                    // std::thread::sleep(Duration::from_millis(100));
                    if Instant::now() > self.2 {
                        return Poll::Ready(None);
                    }
                    match self.1.poll_tick(cx) {
                        Poll::Ready(_) => (),
                        Poll::Pending => return Poll::Pending,
                    }
                    // fut.poll_unpin(cx);
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values(vec![0, 1]))
                        as ArrayRef;
                    let item = RecordBatch::try_from_iter(vec![("ts", val)]).map_err(Into::into);
                    log::info!("{item:?}");
                    std::task::Poll::Ready(Some(item))
                }
            }
            // let schema = arrow
            // let mut data = FlightDataEncoderBuilder::new()
            //     .with_schema(schema.clone())
            //     .with_metadata(Bytes::from("metadata"))
            //     .with_options(
            //         IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap(),
            //     )
            //     .build(FakeStream(
            //         schema.clone(),
            //         tokio::time::interval(Duration::from_millis(1000)),
            //         Instant::now() + Duration::from_secs(10),
            //     ));

            struct Data {
                data: FlightDataEncoder,
            }
            impl futures::Stream for Data {
                type Item = FlightData;
                fn poll_next(
                    mut self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Option<Self::Item>> {
                    self.data
                        .try_poll_next_unpin(cx)
                        .map(|u| u.transpose().unwrap())
                        .map(|u| {
                            u.map(|mut v| {
                                if v.app_metadata.is_empty() {
                                    v.app_metadata = Bytes::from("request");
                                    v
                                } else {
                                    v
                                }
                            })
                        })
                }
            }

            // let mut req = Data { data }.into_streaming_request();
            // req.metadata_mut().append("x-task-id", "2".parse().unwrap());

            // let stream = client.do_put(req).await.unwrap().into_inner();

            // stream
            //     .try_for_each(|res| async {
            //         dbg!(res.app_metadata);

            //         Ok(())
            //     })
            //     .await
            //     .unwrap();

            let data = FlightDataEncoderBuilder::new()
                .with_schema(schema.clone())
                .with_metadata(Bytes::from("metadata"))
                .with_options(
                    IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap(),
                )
                .build(FakeStream(
                    schema.clone(),
                    tokio::time::interval(Duration::from_millis(1000)),
                    Instant::now() + Duration::from_secs(10),
                ));

            let req = Data { data }.into_streaming_request();

            let response = client.do_exchange(req).await.unwrap();
            let stream = FlightDataDecoder::new(
                response.into_inner().map_err(|err| FlightError::Tonic(err)),
            );
            // .into_inner();

            stream
                .try_for_each(|res| async move {
                    // dbg!(res.app_metadata);
                    Ok(())
                })
                .await
                .unwrap();

            // client.do_put().await;
        };

        tokio::select! {
            _ = request_future => println!("Client finished"),
            // _ = serve_future => println!("Server finished!"),
        }
        Ok(())
    }
}
