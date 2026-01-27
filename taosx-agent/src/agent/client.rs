use std::{
    collections::HashMap, net::SocketAddr, ops::RangeInclusive, pin::Pin, str::FromStr, sync::Arc,
    task::Poll, time::Duration,
};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
};
use arrow_flight::{
    Action as FlightAction, FlightClient, encode::FlightDataEncoderBuilder, error::FlightError,
    flight_service_client::FlightServiceClient,
};
use chrono::Utc;
use futures::{StreamExt, TryStreamExt};
use ha_core::{activity::Activity, consts::*, types::HaTask, utils::next_req_id};
use hyper::Uri;
use hyper_util::rt::TokioIo;
use snafu::{OptionExt, ResultExt};
use taos::Dsn;
use taosx_core::{
    CheckResponse, DataSetsReq, Fail, HeartbeatResponse, ListResponse, PutFileReq,
    QueryDataSourceReq, QueryDataSourceResp, RespAction, Response, SampleResponse,
    SplitTaskResponse, list_datasets_from,
};
use taosx_task::{sample::get_sample, split_job::plan_task, validate::validate_dsn};
use tokio::net::{TcpSocket, TcpStream};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tower::{BoxError, Service};
use tracing::instrument;

use crate::{
    agent::{Task, do_put_file},
    runner::Action,
};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Failed to build TLS configuration"))]
    BuildTlsConfig { source: tonic::transport::Error },
    #[snafu(display("Failed to connect to channel: {endpoint}"))]
    ChannelConnect {
        endpoint: String,
        source: tonic::transport::Error,
    },
    #[snafu(display("Failed to handshake with server"))]
    Handshake {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Failed to parse handshake response"))]
    InvalidHandshakeResp { source: serde_json::Error },
    #[snafu(display("Failed to parse uri: {uri}"))]
    InvalidUri { uri: Uri },
    #[snafu(display("Failed to connect socket"))]
    SocketConnect { source: std::io::Error },
    #[snafu(display("Do action error"))]
    DoAction {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Failed to add header"))]
    AddHeader {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Failed to parse endpoint: {endpoint}"))]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
    },
    #[snafu(display("Failed to serialize activity"))]
    SerializeActivity { source: serde_json::Error },
    #[snafu(display("Collect action response error"))]
    CollectActionResp {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(transparent)]
    JsonToDsn { source: anyhow::Error },
    #[snafu(display("Do exchange error"))]
    DoExchange {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Fetch exchange item error"))]
    FetchExchangeItem {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Invalid payload for action: {action}"))]
    InvalidActionPayload {
        action: String,
        source: serde_json::Error,
    },
    #[snafu(display("Invalid DSN: {dsn}"))]
    InvalidDsn { dsn: String, source: taos::DsnError },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Client {
    pub client: FlightClient,
    pub agent_id: i64,
    endpoint: String,
}

async fn new_channel(
    endpoint: String,
    ports: Option<RangeInclusive<u16>>,
    ca: Option<Certificate>,
) -> Result<Channel> {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
           let tcp_keepalive = None;
        } else {
           let tcp_keepalive = Some(Duration::from_secs(5));
        }
    };
    let mut endpoint_builder = Endpoint::try_from(endpoint.clone())
        .context(InvalidEndpointSnafu {
            endpoint: &endpoint,
        })?
        .keep_alive_while_idle(true)
        .tcp_keepalive(tcp_keepalive)
        .http2_keep_alive_interval(Duration::from_secs(13))
        .keep_alive_timeout(Duration::from_secs(120));
    if let Some(ca) = ca {
        endpoint_builder = endpoint_builder
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(ca)
                    .with_native_roots(),
            )
            .context(BuildTlsConfigSnafu)?;
    }

    match ports {
        Some(ports) => {
            let connector = Svc::new(ports);
            endpoint_builder.connect_with_connector(connector).await
        }
        None => endpoint_builder.connect().await,
    }
    .context(ChannelConnectSnafu { endpoint })
}

struct Svc {
    ports: RangeInclusive<u16>,
}

impl Svc {
    fn new(ports: RangeInclusive<u16>) -> Self {
        Self { ports }
    }
}

impl Service<Uri> for Svc {
    type Response = TokioIo<TcpStream>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        match TcpSocket::new_v4() {
            Ok(socket) => {
                for port in self.ports.clone() {
                    if let Err(err) = socket.bind(format!("0.0.0.0:{port}").parse().unwrap()) {
                        match err.kind() {
                            std::io::ErrorKind::AddrInUse => {
                                continue;
                            }
                            _ => {
                                tracing::error!("{err:#}");
                                return Box::pin(async move { Err(Box::new(err) as _) });
                            }
                        }
                    } else {
                        break;
                    };
                }

                Box::pin(async move {
                    Ok(TokioIo::new({
                        let addr = parse_addr(&uri).context(InvalidUriSnafu { uri })?;
                        socket.connect(addr).await.context(SocketConnectSnafu)?
                    }))
                })
            }
            Err(err) => Box::pin(async move { Err(Box::new(err) as _) }),
        }
    }
}

fn parse_addr(uri: &Uri) -> Option<SocketAddr> {
    let host = uri.host()?;
    let port = uri.port_u16()?;
    let addr = format!("{}:{}", host, port);
    addr.parse().ok()
}

impl Client {
    pub async fn new(
        endpoint: &str,
        token: &str,
        ca: Option<Certificate>,
        ports: &Option<RangeInclusive<u16>>,
    ) -> Result<Self> {
        let endpoint = endpoint.to_string();
        let token = token.to_string();

        let channel = new_channel(endpoint.clone(), ports.clone(), ca.clone()).await?;

        let inner = FlightServiceClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        let mut client = FlightClient::new_from_inner(inner);
        client
            .add_header("x-token", &token)
            .context(AddHeaderSnafu)?;
        client
            .add_header("x-version", crate::build::PKG_VERSION)
            .context(AddHeaderSnafu)?;
        let result = client
            .handshake(token.to_string())
            .await
            .context(HandshakeSnafu)?;
        let agent: i64 = serde_json::from_slice(&result).context(InvalidHandshakeRespSnafu)?;

        Ok(Self {
            client,
            agent_id: agent,
            endpoint,
        })
    }

    pub fn agent_id(&self) -> i64 {
        self.agent_id
    }
    pub async fn push_status(&mut self, status: &Activity) -> Result<()> {
        let status_bytes = serde_json::to_vec(status).context(SerializeActivitySnafu)?;
        let action = FlightAction::new(ACTION_TASK_STATUS, status_bytes);
        let _resp: Vec<_> = self
            .client
            .do_action(action)
            .await
            .context(DoActionSnafu)?
            .try_collect()
            .await
            .context(CollectActionRespSnafu)?;
        Ok(())
    }

    pub async fn get_taosx_monitor_config(&mut self) -> Option<HashMap<String, String>> {
        tracing::info!("Get monitor config from server");
        let action = FlightAction::new(ACTION_GET_MONITOR_CONFIG, "GetMonitorConfig");
        let result = match self.client.do_action(action).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("Can't get monitor config from server: {e}");
                return None;
            }
        };
        let result: std::result::Result<Vec<bytes::Bytes>, arrow_flight::error::FlightError> =
            result.try_collect().await;
        if let Err(err) = result {
            tracing::error!("Can't get monitor config from server: {err:#}");
            return None;
        }
        let resp = result.unwrap();
        if resp.is_empty() {
            tracing::error!("Can't get monitor config from server");
            return None;
        }
        let config = serde_json::from_slice::<HashMap<String, String>>(&resp[0]);
        if let Err(err) = config {
            tracing::error!("Can't deserialize response data: {err:#}");
            return None;
        }
        let config = config.unwrap();
        tracing::info!("Got config from server: {:?}", &config);
        Some(config)
    }

    #[instrument(skip_all)]
    pub async fn process_actions(
        mut self,
        sender: flume::Sender<Action>,
        resp_tx: flume::Sender<RespAction>,
        resp_rx: flume::Receiver<RespAction>,
    ) -> Result<()> {
        tracing::info!("Wait tasks from server");
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("action", DataType::Utf8, false),
            Field::new("context", DataType::Utf8, true),
            Field::new("req_id", DataType::UInt64, false),
        ]));

        let req = FlightDataEncoderBuilder::new().with_schema(schema).build(
            resp_rx
                .into_stream()
                .map(|action| resp_action_to_arrow(action).map_err(FlightError::Arrow)),
        );
        let mut stream = self
            .client
            .do_exchange(req)
            .await
            .context(DoExchangeSnafu)?;
        let mut max_server_delay = None;
        while let Some(res) = stream.try_next().await.context(FetchExchangeItemSnafu)? {
            let rows = res.num_rows();
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
            let req_id = res
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            for _ in 0..rows {
                let (ts, action, context, req_id) = (
                    ts.value_as_datetime_with_tz(
                        0,
                        ts.timezone().unwrap_or("UTC").parse().unwrap(),
                    )
                    .unwrap(),
                    action.value(0),
                    context.value(0),
                    req_id.value(0),
                );

                #[derive(serde::Deserialize)]
                struct TaskWithId {
                    id: i64,
                    job_id: i64,
                }
                match action {
                    ACTION_RUN => {
                        let task: Task = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        tracing::info!(task_id = task.id, job_id = task.job_id, "Start task");
                        if sender.send_async(Action::Run(task)).await.is_err() {
                            return Ok(());
                        }
                    }
                    ACTION_STOP => {
                        let task: TaskWithId = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        tracing::info!(task_id = task.id, job_id = task.job_id, "Stop task");
                        if sender
                            .send_async(Action::Stop(task.id, task.job_id))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    ACTION_CANCEL => {
                        let task: TaskWithId = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        tracing::info!(task_id = task.id, job_id = task.job_id, "Cancel task");
                        if sender
                            .send_async(Action::Cancel(task.id, task.job_id))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    ACTION_LIST_DATA_SETS => {
                        tracing::info!("List datasets request received");
                        let req: DataSetsReq = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let resp_tx = resp_tx.clone();

                        tokio::spawn(async move {
                            let sets = list_datasets_from(&req).await.map_err(Fail::new);
                            resp_tx
                                .send_async(RespAction::ListOk(ListResponse {
                                    req_id,
                                    req,
                                    res: sets,
                                }))
                                .await
                                .ok();
                        });
                    }
                    ACTION_CHECK => {
                        tracing::info!("Check data source request received");

                        let dsn =
                            Dsn::from_str(context).context(InvalidDsnSnafu { dsn: context })?;
                        let resp_tx = resp_tx.clone();

                        tokio::spawn(async move {
                            let dsv = validate_dsn(&dsn).await;
                            resp_tx
                                .send_async(RespAction::CheckOk(CheckResponse {
                                    req_id,
                                    req: dsn.to_string(),
                                    res: dsv,
                                }))
                                .await
                                .ok();
                        });
                    }
                    ACTION_GET_SAMPLE => {
                        tracing::info!("Sample data source request received");
                        let dsn =
                            Dsn::from_str(context).context(InvalidDsnSnafu { dsn: context })?;
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(async move {
                            let sample = get_sample(&dsn).await;
                            let res = match sample {
                                Ok(sample) => match serde_json::to_string(&sample) {
                                    Ok(s) => Response::Ok(s),
                                    Err(err) => Response::Err(Fail::new(anyhow::anyhow!(
                                        "failed to serialize sample data, cause: {}",
                                        err
                                    ))),
                                },
                                Err(err) => Response::Err(Fail::new(err)),
                            };

                            resp_tx
                                .send_async(RespAction::SampleOk(SampleResponse {
                                    req_id,
                                    req: dsn.to_string(),
                                    res,
                                }))
                                .await
                                .ok();
                        });
                    }
                    ACTION_SPLIT_TASK => {
                        tracing::info!("Split task request received");
                        let task: HaTask = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(async move {
                            let tasks = plan_task(task.clone()).await;
                            let res = match tasks {
                                Ok(res) => Response::Ok(res),
                                Err(err) => Response::Err(Fail::new(err)),
                            };

                            resp_tx
                                .send_async(RespAction::SplitTaskOk(SplitTaskResponse {
                                    req_id,
                                    req: task,
                                    res,
                                }))
                                .await
                                .ok();
                        });
                    }
                    ACTION_PUT_FILE => {
                        let req: PutFileReq = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let resp_tx = resp_tx.clone();
                        tracing::info!("Put file request received");
                        tokio::spawn(do_put_file(req, req_id, resp_tx));
                    }
                    ACTION_QUERY_DATA_SOURCE => {
                        tracing::info!(?req_id, "[query-data-source]: {}", &context);
                        let req: QueryDataSourceReq = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let resp_tx = resp_tx.clone();
                        tracing::info!("Query data source request received");
                        tokio::spawn(async move {
                            let result = taosx_core::plugins::query_data_source(req).await;
                            match result {
                                Ok(output) => {
                                    tracing::info!(?req_id, "Query data source ok");
                                    let _ = resp_tx
                                        .send_async(RespAction::QueryDataSourceOk(
                                            QueryDataSourceResp {
                                                req_id,
                                                output: Response::Ok(output),
                                            },
                                        ))
                                        .await;
                                }
                                Err(err) => {
                                    tracing::error!(?req_id, "Query data source error: {err:#}");
                                    let _ = resp_tx
                                        .send_async(RespAction::QueryDataSourceOk(
                                            QueryDataSourceResp {
                                                req_id,
                                                output: Response::Err(Fail::new(err)),
                                            },
                                        ))
                                        .await;
                                }
                            }
                        });
                    }
                    MESSAGE_HEARTBEAT => {
                        let resp = HeartbeatResponse {
                            req: ts.naive_utc().and_utc(),
                            res: Utc::now(),
                        };
                        if resp_tx
                            .send_async(RespAction::HeartbeatOk(req_id, resp))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    MESSAGE_HEARTBEAT_OK => {
                        let resp: HeartbeatResponse = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let delay = resp.duration().num_milliseconds();
                        if Some(delay) > max_server_delay {
                            max_server_delay = Some(delay);
                            tracing::info!(
                                endpoint = self.endpoint,
                                "Server is alive, delay: {}ms",
                                resp.duration().num_milliseconds()
                            );
                        }
                    }
                    ACTION_EXIT => {
                        tracing::info!("Received exit command");
                        if sender.send_async(Action::Exit).await.is_err() {
                            return Ok(());
                        }
                    }
                    action => {
                        tracing::error!("Unknown action {action}");
                    }
                }
            }
        }

        Ok(())
    }
}

fn resp_action_to_arrow(action: RespAction) -> std::result::Result<RecordBatch, ArrowError> {
    match action {
        RespAction::Heartbeat(req_id) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context: ArrayRef = Arc::new(StringArray::from_iter([Option::<String>::None]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([MESSAGE_HEARTBEAT]));
            tracing::debug!("Send heartbeat request: {req_id}");
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::HeartbeatOk(req_id, resp) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([MESSAGE_HEARTBEAT_OK]));
            tracing::debug!("Send heartbeat response: {req_id}");
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::TaskError(_) => unreachable!(),
        RespAction::ListOk(sets) => {
            let req_id = sets.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context =
                serde_json::to_string(&sets).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_LIST_DATA_SETS]));
            let req_id_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let item = RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id_arr),
            ]);

            tracing::debug!(
                "RespAction::ListOk, result len: {}, req_id: {}",
                sets.res.as_ref().map_or(0, |r| r.len()),
                req_id
            );
            item
        }
        RespAction::CheckOk(response) => {
            let req_id = response.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context = serde_json::to_string(&response)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_CHECK]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::SampleOk(resp) => {
            let req_id = resp.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_GET_SAMPLE]));
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::SplitTaskOk(resp) => {
            let req_id = resp.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_SPLIT_TASK]));
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::PutFileOk(resp) => {
            let req_id = resp.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_PUT_FILE]));
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::QueryDataSourceOk(resp) => {
            let req_id = resp.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef =
                Arc::new(StringArray::from_iter_values([ACTION_QUERY_DATA_SOURCE]));
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::AgentActivity(activity) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context = serde_json::to_string(&activity)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef =
                Arc::new(StringArray::from_iter_values([MESSAGE_AGENT_ACTIVITY]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([next_req_id()]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::TaskActivity(activity) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context = serde_json::to_string(&activity)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([MESSAGE_TASK_ACTIVITY]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([next_req_id()]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::TaskMetrics(metrics) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([MESSAGE_TASK_METRICS]));
            let context = serde_json::to_string(&metrics)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([next_req_id()]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::Metrics(metrics_event) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef =
                Arc::new(StringArray::from_iter_values([MESSAGE_METRICS_EVENTS]));
            let context = serde_json::to_string(&metrics_event)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([next_req_id()]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
    }
}

#[cfg(test)]
mod agent_tests {
    use std::process::{self, Command};

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_new_channel() {
        let endpoint = String::from("0.0.0.0:6030");
        let ports = Some(9000..=9099);
        let _channel = new_channel(endpoint, ports, None).await.unwrap();

        // get the current process ID
        let pid = process::id();

        // get listening ports by netstat
        let output = Command::new("netstat")
            .arg("-anp")
            .output()
            .expect("Failed to execute command");
        let output_str = String::from_utf8_lossy(&output.stdout);
        let filtered_output: Vec<&str> = output_str
            .lines()
            .filter(|line| line.contains(&pid.to_string()))
            .collect();
        println!("Listening ports:\n{:?}", filtered_output);
    }
}
