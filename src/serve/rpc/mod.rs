use std::pin::Pin;

use futures::{Stream, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};

use crate::serve::rpc::put::PutStream;

use super::controller::TaskControllerRef;

mod put;

#[derive(Clone)]
pub(super) struct FlightServiceImpl {
    controller: TaskControllerRef,
}

impl FlightServiceImpl {
    pub(super) fn new(controller: TaskControllerRef) -> Self {
        Self { controller }
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    async fn handshake(
        &self,
        req: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        dbg!(&req);
        let addr = req.remote_addr();
        dbg!(&addr);
        let (meta, extension, mut req) = req.into_parts();

        let req = req.message().await?;

        if let Some(req) = req {
            let res = HandshakeResponse {
                protocol_version: req.protocol_version,
                payload: req.payload,
            };
            let handshake_stream = futures::stream::once(async { Ok(res) });
            return Ok(Response::new(Box::pin(handshake_stream)));
        }
        // dbg!(req.try_collect::<Vec<_>>());
        // dbg!(meta, extension, req);

        // Ok(Response::new())
        Err(Status::unimplemented("Implement handshake"))
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
        let (meta, extension, mut req) = req.into_parts();

        // let message = req.try_next().await?;
        let task_id = meta
            .get("x-task-id")
            .ok_or_else(|| Status::unavailable("Task id should be set"))?;
        let task_id: i64 = task_id.to_str().unwrap().parse().unwrap();

        let put_stream = PutStream::new(self.controller.clone(), task_id, req);

        struct ResultStream(Streaming<FlightData>);
        unsafe impl Sync for ResultStream {}
        unsafe impl Send for ResultStream {}

        // impl futures::Stream for ResultStream {
        //     type Item = Result<PutResult, Status>;
        // }

        // dbg!(&message);

        Ok(Response::new(Box::pin(
            put_stream.into_flight_put_result().await,
        )))
    }

    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }

    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use arrow::{
        datatypes::{DataType, Field, Schema, SchemaRef},
        error::ArrowError,
        ipc::{writer::IpcWriteOptions, TimestampBuilder},
    };
    use arrow_flight::{
        encode::{FlightDataEncoder, FlightDataEncoderBuilder},
        error::FlightError,
        flight_service_client::FlightServiceClient,
        flight_service_server::FlightServiceServer,
        Criteria, FlightData, HandshakeRequest,
    };
    use futures::TryStreamExt;
    use tempfile::NamedTempFile;
    use tokio::net::{UnixListener, UnixStream};
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::{
        codegen::Bytes,
        metadata::MetadataMap,
        transport::{Channel, Endpoint, Server},
        IntoStreamingRequest,
    };

    use crate::serve::controller::{NewTask, TaskController, TaskControllerRef};

    use super::FlightServiceImpl;
    async fn client_with_uds(path: String) -> FlightServiceClient<Channel> {
        let connector = tower::service_fn(move |_| UnixStream::connect(path.clone()));
        let channel = Endpoint::try_from("http://[::1]:50051")
            .unwrap()
            .connect_with_connector(connector)
            .await
            .unwrap();
        FlightServiceClient::new(channel)
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn server_client() -> anyhow::Result<()> {
        let file = NamedTempFile::new().unwrap();
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let _ = std::fs::remove_file(path.clone());

        let uds = UnixListener::bind(path.clone()).unwrap();
        let stream = UnixListenerStream::new(uds);

        let controller = TaskControllerRef::from_sqlite("sqlite:memory:")
            .await
            .unwrap();

        let task = serde_json::from_str(
            r#"{"from": "pi:///", "agent": "localhost:9090", "to": "taos:///pi"}"#,
        )?;
        controller.create(task).await?;
        let service = FlightServiceImpl { controller };
        let serve_future = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(stream);

        let request_future = async {
            let mut client = client_with_uds(path).await;
            let req = HandshakeRequest::default();
            client
                .handshake(futures::stream::once(async { req }))
                .await
                .unwrap();
            // client.list_flights(Criteria::default()).await.unwrap();

            // futures::stream::repeat();

            let mut metadata = MetadataMap::new();

            let schema = Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            )]));

            // let ipc = arrow::ipc::reader::StreamReader::try_new();
            struct FakeStream(SchemaRef);

            impl futures::Stream for FakeStream {
                type Item = Result<RecordBatch, FlightError>;
                fn poll_next(
                    self: std::pin::Pin<&mut Self>,
                    _: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Option<Self::Item>> {
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values(vec![0, 1]))
                        as ArrayRef;
                    let item = RecordBatch::try_from_iter(vec![("ts", val)]).map_err(Into::into);

                    std::task::Poll::Ready(Some(item))
                }
            }
            // let schema = arrow
            let mut data = FlightDataEncoderBuilder::new()
                .with_schema(schema.clone())
                .with_metadata(Bytes::from("metadata"))
                .with_options(
                    IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap(),
                )
                .build(FakeStream(schema.clone()));

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
            let stream = client.do_put(Data { data }).await?.into_inner();

            stream
                .try_for_each_concurrent(10, |res| async {
                    dbg!(res.app_metadata);

                    Ok(())
                })
                .await

            // client.do_put().await;
        };

        tokio::select! {
            _ = request_future => println!("Client finished"),
            _ = serve_future => println!("Server finished!"),
        }
        Ok(())
    }
}
