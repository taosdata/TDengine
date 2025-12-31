use std::{
    borrow::Cow,
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, bail};
use arrow::record_batch::RecordBatch;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightDataDecoder,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
};
use async_backtrace::framed;
use chrono::Utc;
use futures::{Stream, TryStreamExt, stream::FuturesUnordered};
use ha_core::{
    batch::build_ok_batch,
    consts::DROP_CONNECTION,
    jwt::XnodedToken,
    types::{RpcClientType, XnodedId},
    utils::next_req_id,
};
use linked_hash_map::LinkedHashMap;
use parking_lot::RwLock;
use semver::VersionReq;
use serde::Deserialize;
use serde_json::json;
use taoslog::{QidManager, utils::QidMetadataSetter};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Identity, Server, ServerTlsConfig},
};
use tracing::{Instrument, info_span, instrument};

use taosx_core::{DataSet, Fail, dsv::DataSourceValidation, get_data_dir, utils::trace::Qid};

use crate::serve::{TAOSX_GRPC_DEFAULT_PORT, rpc::utils::build_activity_batch};
use crate::serve::{
    controller::{activity::Activity, agent::AgentToken},
    rpc::put::PutStream,
    scheduler::agent::AgentNotify,
};

use ha_core::batch::SCHEMA;

use super::{
    controller::TaskControllerRef,
    monitor::Monitor,
    scheduler::agent::{AgentActionsReceiver, AgentId, AgentNotifySender, AgentSpawnSender},
    utils::ip::is_support_ipv6,
};

mod processor;
mod put;
pub mod utils;

type DataSetsSenders =
    Arc<RwLock<LinkedHashMap<u64, flume::Sender<Result<Vec<DataSet>, Fail<String>>>>>>;
type DsvSenders = Arc<RwLock<LinkedHashMap<u64, flume::Sender<DataSourceValidation>>>>;
type StringSenders = Arc<RwLock<LinkedHashMap<u64, flume::Sender<Result<String, Fail<String>>>>>>;

type FlightResult = Result<RecordBatch, FlightError>;

pub struct AgentRpcChannel {
    agent_activity_receiver: AgentActionsReceiver,
    agent_notify_sender: AgentNotifySender,
}

impl AgentRpcChannel {
    pub fn new(
        agent_activity_receiver: AgentActionsReceiver,
        agent_notify_sender: AgentNotifySender,
    ) -> Self {
        Self {
            agent_activity_receiver,
            agent_notify_sender,
        }
    }
}

type ConnectionId = u64;
type XnodedConnections = Arc<RwLock<HashMap<XnodedId, Option<flume::Sender<FlightResult>>>>>;

pub(super) struct FlightServiceImpl {
    controller: TaskControllerRef,

    // 返回给 AgentWorker 的 agent 消息
    notify_sender: AgentNotifySender,
    // 来自 AgentWorker 需要发送给 agent 的命令
    action_receiver: Arc<AgentActionsReceiver>,

    agent_connections: Arc<RwLock<HashMap<AgentId, ConnectionId>>>,

    datasets_senders: DataSetsSenders,
    dsv_senders: DsvSenders,
    string_senders: StringSenders,

    spawn_sender: AgentSpawnSender,
    monitor: Monitor,

    current_xnoded: Arc<RwLock<Option<XnodedId>>>,
    xnoded_connections: XnodedConnections,
    xnoded_tx: flume::Sender<Result<RecordBatch, FlightError>>,
    xnoded_rx: flume::Receiver<Result<RecordBatch, FlightError>>,

    cancel_token: CancellationToken,
}

impl FlightServiceImpl {
    fn set_xnoded_id(&self, xnoded_id: XnodedId) {
        *self.current_xnoded.write() = Some(xnoded_id);
    }

    fn xnoded_id(&self) -> Option<XnodedId> {
        self.current_xnoded.read().clone()
    }

    fn get_old_xnoded_tx(
        &self,
    ) -> Vec<(XnodedId, flume::Sender<Result<RecordBatch, FlightError>>)> {
        let Some(current_id) = self.xnoded_id() else {
            return vec![];
        };

        self.xnoded_connections
            .read()
            .iter()
            .filter_map(|(id, tx)| {
                if id == &current_id {
                    None
                } else {
                    tx.clone().map(|v| (id.clone(), v))
                }
            })
            .collect()
    }

    fn add_xnoded_tx(
        &self,
        xnoded_id: XnodedId,
        tx: flume::Sender<Result<RecordBatch, FlightError>>,
    ) {
        let mut xnodeds = self.xnoded_connections.write();
        xnodeds.insert(xnoded_id, Some(tx));
    }

    pub fn subscribe_agent_action_flight(
        &self,
        agent_id: AgentId,
        flight_tx: flume::Sender<Result<RecordBatch, FlightError>>,
    ) {
        let mut receiver = self.action_receiver.resubscribe();

        let (senders, dsv_senders, string_senders, controller) = (
            self.datasets_senders.clone(),
            self.dsv_senders.clone(),
            self.string_senders.clone(),
            self.controller.clone(),
        );

        // 发送给 agent 的命令
        tokio::spawn({
            let tx = flight_tx.clone();
            async move {
                let _ = std::env::set_current_dir(get_data_dir());
                tracing::info!("agent action flight listener start");
                loop {
                    match receiver.recv().await {
                        Ok((id, action)) => {
                            if id == agent_id {
                                tracing::debug!("receive action: {:?}, agent id: {}", action, id);
                                if let Some(batch) = processor::agent::action::action_to_arrow(
                                    &senders,
                                    &dsv_senders,
                                    &string_senders,
                                    &controller,
                                    action,
                                )
                                .await
                                .map_err(|err| Status::internal(err.to_string()).into())
                                .transpose()
                                    && let Err(err) = tx.send_async(batch).await
                                {
                                    tracing::info!(agent_id, "Task listener disconnected: {err:#}");
                                    break;
                                }
                            } else if tx.is_disconnected() {
                                tracing::info!(agent_id, "Task listener disconnected");
                                break;
                            }
                        }
                        Err(err) => match err {
                            tokio::sync::broadcast::error::RecvError::Closed => break,
                            tokio::sync::broadcast::error::RecvError::Lagged(_) => continue,
                        },
                    }
                }
                tracing::info!("agent action flight listener stop");
            }
        });
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
        let addr = req.remote_addr();

        let (meta, _extensions, mut req) = req.into_parts();

        let client_type: RpcClientType = meta
            .get("x-client-type")
            .map(|s| s.to_str())
            .transpose()
            .map_err(|e| Status::aborted(format!("Invalid client type: {e}")))?
            .map(|s| s.into())
            .unwrap_or(RpcClientType::Agent);
        tracing::info!(?addr, %client_type, "receive handshake");

        let Some(req) = req.message().await? else {
            return Err(Status::aborted("handshake message not found"));
        };

        let mut res = HandshakeResponse {
            protocol_version: req.protocol_version,
            payload: req.payload.clone(),
        };

        if matches!(client_type, RpcClientType::Guest) {
            return Ok(Response::new(Box::pin(futures::stream::once(async {
                Ok(res)
            }))));
        }

        if matches!(client_type, RpcClientType::Xnoded) {
            let token = String::from_utf8(req.payload.to_vec())
                .map_err(|_| Status::aborted("Token not Invalid utf8 string"))?
                .to_string();
            let xnoded_id = XnodedToken::from(token)
                .jwt_decode()
                .map_err(|e| Status::aborted(format!("Invalid xnoded id payload: {e}")))?;
            let id = self.xnoded_id();
            self.set_xnoded_id(xnoded_id.clone());
            if id.as_ref().is_some_and(|v| v != &xnoded_id) {
                // 通知其他连接关闭
                let drop_conn_req = build_ok_batch(DROP_CONNECTION, xnoded_id, next_req_id())
                    .map_err(|_| Status::internal("build drop conn batch error"))?;
                for (id, tx) in self.get_old_xnoded_tx() {
                    tracing::info!("send DROP CONNECTION to {id}");
                    tx.send_async(Ok(drop_conn_req.clone())).await.ok();
                }
            }
            return Ok(Response::new(Box::pin(futures::stream::once(async {
                Ok(res)
            }))));
        }

        let client_version = meta.get("x-version").ok_or_else(|| {
            Status::aborted("The server does not compatible to your agent, please upgrade to a newer version")
        })?.to_str().map_err(|err| Status::aborted(format!("Invalid agent version: {err}")))?;

        let agent_id = match self
            .controller
            .check_agent_id(&AgentToken::from(&res.payload))
            .map_err(|err| Status::permission_denied(format!("Invalid token: {err:#}")))?
        {
            Some(agent_id) => agent_id,
            None => {
                return Err(Status::permission_denied("invalid agent id"));
            }
        };
        // Agent version compatible check
        let req = VersionReq::parse(">=1.3.0").unwrap();

        let version = semver::Version::parse(client_version)
            .map_err(|err| Status::aborted(format!("Invalid agent version: {err:#}", err = err)))?;

        if !req.matches(&version) {
            self.notify_sender
                .send(AgentNotify::AgentDisconnected(agent_id))
                .map_err(|err| {
                    Status::internal(format!("Scheduler is not ready: {err:#}", err = err))
                })?;

            tracing::error!(
                "Agent core version {version} is not compatible to server, please upgrade to a newer version"
            );

            return Err(Status::aborted("agent version not compatible"));
        }
        res.payload = serde_json::to_vec(&agent_id).unwrap().into();
        let handshake_stream = futures::stream::once(async { Ok(res) });
        Ok(Response::new(Box::pin(handshake_stream)))
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

    #[instrument(skip_all)]
    async fn do_put(
        &self,
        req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let addr = req.remote_addr();
        let (meta, _extension, req) = req.into_parts();

        let task_id = meta
            .get("x-task-id")
            .ok_or_else(|| Status::unavailable("Task id should be set"))
            .unwrap();
        let job_id = meta
            .get("x-job-id")
            .ok_or_else(|| Status::unavailable("Job id should be set"))
            .unwrap();
        let task_id: i64 = task_id.to_str().unwrap().parse().unwrap();
        let job_id: i64 = job_id.to_str().unwrap().parse().unwrap();
        let qid_str = match meta.get(taoslog::utils::QID_HEADER_KEY) {
            Some(stream_trace_id) => Cow::Borrowed(stream_trace_id.to_str().unwrap()),
            None => {
                tracing::warn!(task_id, "qid not found in put stream");
                Cow::Owned(Qid::init().display().to_string())
            }
        };
        let qid = Qid::from(u64::from_str_radix(&qid_str[2..], 16).expect("invalid qid"));
        taoslog::utils::Span.set_qid(&qid);

        tracing::info!("received qid str: {qid_str}, task_id: {task_id}");

        let put_stream = PutStream::new(
            self.controller.clone(),
            task_id,
            job_id,
            req,
            self.notify_sender.clone(),
            addr,
            qid,
            self.spawn_sender.clone(),
        )
        .await
        .map_err(|err| {
            tracing::error!(task_id, "Failed to create put stream: {err:#}");
            Status::unavailable(err.to_string())
        })?;

        Ok(Response::new(Box::pin(
            put_stream.into_flight_put_result().await.map_err(|err| {
                tracing::error!(task_id, "Failed to put result into stream: {err:#}");
                Status::unavailable(err.to_string())
            })?,
        )))
    }

    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    #[instrument(skip(self, req))]
    async fn do_exchange(
        &self,
        req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let cancel = self.cancel_token.child_token();
        let remote = req.remote_addr();
        let (mut meta, extension, req) = req.into_parts();

        let token = meta
            .get("x-token")
            .map(|s| s.to_str())
            .transpose()
            .map_err(|err| Status::aborted(format!("Invalid token: {err}")))?
            .ok_or(Status::aborted("token is required"))?;

        let client_type: RpcClientType = meta
            .get("x-client-type")
            .map(|s| s.to_str())
            .transpose()
            .map_err(|e| Status::aborted(format!("Invalid client type: {e}")))?
            .map(|v| v.into())
            .unwrap_or(RpcClientType::Agent);

        tracing::info!(?remote, "Receive do_exchange stream from {:?}", remote);

        let (tx, rx) = flume::bounded(1000);

        let mut tasks = JoinSet::new();

        let is_xnoded = matches!(client_type, RpcClientType::Xnoded);
        let is_agent = matches!(client_type, RpcClientType::Agent);

        let xnoded_id = self.xnoded_id();

        if is_xnoded {
            let Some(xnoded_id) = xnoded_id.clone() else {
                return Err(Status::aborted("handshake first"));
            };
            let token_xnoded_id = XnodedToken::from(token)
                .jwt_decode()
                .map_err(|e| Status::aborted(format!("Invalid xnoded id payload: {e}")))?;
            if xnoded_id != token_xnoded_id {
                return Err(Status::aborted("xnoded id not equal with handshake token"));
            }
            self.add_xnoded_tx(xnoded_id, tx.clone());
            tasks.spawn({
                let cancel = cancel.clone();
                let xnoded_rx = self.xnoded_rx.clone();
                let tx = tx.clone();
                async move {
                    while let Some(Ok(payload)) =
                        cancel.run_until_cancelled(xnoded_rx.recv_async()).await
                    {
                        if tx.send_async(payload).await.is_err() {
                            break;
                        }
                    }

                    Ok(())
                }
            });
        }

        let controller = self.controller.clone();
        let agent_id = if is_agent {
            Some(
                controller
                    .agent_connect_with_token(
                        &AgentToken(token.to_string()),
                        remote.as_ref(),
                        &self.xnoded_tx,
                    )
                    .await
                    .map_err(|err| {
                        Status::permission_denied(format!("Agent connection error: {err}"))
                    })?,
            )
        } else {
            None
        };

        if let Some(agent_id) = agent_id {
            self.subscribe_agent_action_flight(agent_id, tx.clone());
        }

        let connection_id = Utc::now().timestamp_millis() as u64;
        meta.append("x-cid", connection_id.to_string().parse().unwrap());
        {
            // Update agent connection id to ensure only one connection per agent.
            if let Some(agent_id) = agent_id {
                self.agent_connections
                    .write()
                    .insert(agent_id, connection_id);
                self.notify_sender
                    .send(AgentNotify::AgentConnected(agent_id))
                    .map_err(|err| Status::internal(format!("Scheduler is not ready: {err:#}")))?;
                tracing::debug!(
                    "Sent agent notify: {:?}",
                    AgentNotify::AgentConnected(agent_id)
                );
            }
        }

        let task_message_tx = match client_type {
            RpcClientType::Xnoded => Some(self.xnoded_tx.clone()),
            RpcClientType::Guest => Some(tx.clone()),
            RpcClientType::Agent => None,
        };

        if let Some(tx) = task_message_tx {
            processor::xnode::tasks::spawn_task(
                &mut tasks,
                cancel.clone(),
                self.controller.clone(),
                tx,
            );
        }

        // 处理接收到的消息
        tasks.spawn({
            let xnode_id = self.xnoded_id();
            let flight_tx = tx;
            let xnoded_tx = self.xnoded_tx.clone();
            let controller = controller.clone();
            let notify_sender = self.notify_sender.clone();
            let datasets_senders = self.datasets_senders.clone();
            let dsv_senders = self.dsv_senders.clone();
            let string_senders = self.string_senders.clone();
            let agent_connections = self.agent_connections.clone();
            let cancel = cancel.clone();
            async move {
                let tx = match client_type {
                    RpcClientType::Xnoded => xnoded_tx,
                    _ => flight_tx
                };
                let mut decoder = FlightDataDecoder::new(req.map_err(FlightError::from));
                let parallel = 20;
                let mut futs = FuturesUnordered::new();
                let result: Result<(), FlightError> = loop {
                    tokio::select! {
                        biased;
                        res = futs.next(), if !futs.is_empty() => {
                            let res: Option<Result<(), FlightError>> = res;
                            let Some(res) = res else {
                                break Ok(());
                            };
                            if res.is_err() {
                                break res;
                            }
                        }
                        res = decoder.next(), if futs.len() < parallel => {
                            let Some(data) = res else {
                                break Ok(());
                            };
                            let data = match data {
                                Ok(data) => data,
                                Err(e) => {
                                    break Err(e)
                                },
                            };
                            let fut = processor::received::process(
                                xnode_id.as_ref(),
                                &controller,
                                data,
                                agent_id,
                                &datasets_senders,
                                &dsv_senders,
                                &string_senders,
                                &notify_sender,
                                &tx,
                                client_type
                            );
                            futs.push(fut);
                        }
                        _ = cancel.cancelled() => {
                            break Ok(())
                        },
                    }
                };
                while futs.next().await.is_some() {}

                tracing::info!(
                    agent.id = agent_id,
                    agent.cid = connection_id,
                    "RPC stopped"
                );
                if let Err(e) = &result {
                    tracing::error!("Process received rpc data error: {e}");
                }

                if let Some(agent_id) = agent_id && let Some(cid) = {agent_connections.read().get(&agent_id).cloned()} {
                    if cid == connection_id {
                        {agent_connections.write().remove(&agent_id);}

                        let context = result.err().map(|err| {
                            json!({"code": 0xFFFFi32, "message": err.to_string()})
                        });
                        let activity = Activity::agent_disconnect(agent_id, context);
                        if let Err(err) = notify_sender.send(AgentNotify::AgentDisconnected(agent_id)) {
                            tracing::error!(agent = agent_id, "Agent disconnected: {err:#}");
                        }
                        let batch = build_activity_batch(activity).context("build agent activity batch error")?;
                        tx.send_async(Ok(batch)).await.ok();
                        tracing::info!(agent = agent_id, "Agent RPC stopped");
                    } else {
                        tracing::warn!(
                            agent.id = agent_id,
                            agent.cid = connection_id,
                            "Agent RPC stopped but current connection id({cid}) is not matched, do nothing."
                        );
                    }
                }

                cancel.cancel();
                Ok::<_, anyhow::Error>(())
            }.instrument(info_span!("agent_rpc"))
        });

        // 等待所有任务退出
        tokio::spawn({
            let cancel = cancel.clone();
            let xnoded_id = self.xnoded_id();
            let xnoded_conns = self.xnoded_connections.clone();
            async move {
                let _guard = taosx_core::utils::defer::defer(|| {
                    tracing::info!(xnoded.id = ?xnoded_id, "all rpc tasks exit");
                });
                let _tx_guard = taosx_core::utils::defer::defer(|| {
                    if is_xnoded && let Some(xid) = &xnoded_id {
                        xnoded_conns.write().remove(xid);
                    }
                });
                cancel.cancelled().await;
                while let Some(task) = tasks.join_next().await {
                    match task {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => {
                            tracing::error!("RPC task failed: {e:#}");
                        }
                        Err(e) => {
                            tracing::error!("RPC task panicked: {e:#}");
                        }
                    }
                }

                if is_xnoded {
                    tracing::info!("xnoded connection dropped, stop all tasks");
                    if let Err(e) = controller.stop_all_task().await {
                        tracing::error!("Failed to stop all tasks: {e}");
                    }
                }
            }
        });

        let stream: Self::DoExchangeStream = Box::pin({
            FlightDataEncoderBuilder::new()
                .with_schema(SCHEMA.clone())
                .build(rx.into_stream())
                .map_err(|err| Status::internal(err.to_string()))
        });
        Ok(tonic::Response::from_parts(meta, stream, extension))
    }

    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        taoslog::utils::Span.set_qid(&Qid::init());
        let (_meta, _part, action) = request.into_parts();
        // dbg!(_meta, _part, &action);
        match action.r#type.as_str() {
            "TaskStatus" => {
                // task.
                let activity: Activity = serde_json::from_slice(&action.body)
                    .map_err(|err| Status::invalid_argument(format!("{err}: {:?}", action.body)))?;

                tracing::info!(?activity, "Received task status");
                let (task_id, job_id) = (activity.task_id, activity.job_id);
                let task = self
                    .controller
                    .get_task(task_id, job_id)
                    .await
                    .ok_or_else(|| Status::internal(format!("Task {task_id} not found")))?;

                let agent_id = task.via.ok_or_else(|| {
                    Status::internal(format!("Task {task_id} does not relate to any agent"))
                })?;
                self.notify_sender
                    .send(AgentNotify::TaskActivity(agent_id, activity))
                    .map_err(|err| Status::internal(format!("Scheduler is not ready: {err:#}")))?;

                Ok(Response::new(Box::pin(futures::stream::iter([]))))
            }
            "GetMonitorConfig" => {
                let mut config = self.monitor.cfg.as_map();
                config.insert("taosx_id".to_string(), self.monitor.taosx_id.to_string());
                let message = serde_json::to_vec(&config).unwrap();
                Ok(Response::new(Box::pin(futures::stream::iter([Ok(
                    arrow_flight::Result {
                        body: message.into(),
                    },
                )]))))
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

    async fn poll_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> std::result::Result<tonic::Response<PollInfo>, tonic::Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct RpcConfig {
    pub tcp: Vec<SocketAddr>,
    pub unix: Option<PathBuf>,
    pub ssl_cert: Option<String>,
    pub ssl_key: Option<String>,
    pub ssl_ca: Option<String>,
}

impl RpcConfig {
    /// Start a Flight gRPC server
    #[framed]
    pub(super) async fn serve_with_controller(
        self,
        controller: TaskControllerRef,
        channel: AgentRpcChannel,
        spawn_sender: AgentSpawnSender,
        monitor: Monitor,
        cancel_token: CancellationToken,
    ) -> Result<(), anyhow::Error> {
        let max_frame_size: Option<u32> = Some((1 << 24) - 1_u32);
        let activity_receiver = channel.agent_activity_receiver;
        let (xnoded_tx, xnoded_rx) = flume::bounded(1000);
        let service = FlightServiceImpl {
            controller: controller.clone(),
            notify_sender: channel.agent_notify_sender,
            action_receiver: Arc::new(activity_receiver),
            agent_connections: Arc::new(RwLock::new(HashMap::new())),
            datasets_senders: Arc::new(RwLock::new(LinkedHashMap::new())),
            dsv_senders: Arc::new(RwLock::new(LinkedHashMap::new())),
            string_senders: Arc::new(RwLock::new(LinkedHashMap::new())),
            spawn_sender,
            monitor,
            current_xnoded: Arc::new(RwLock::new(None)),
            xnoded_connections: Arc::new(RwLock::new(HashMap::with_capacity(1))),
            xnoded_tx,
            xnoded_rx,
            cancel_token,
        };
        let flight_service = FlightServiceServer::new(service);
        let flight_service = flight_service
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        if !self.tcp.is_empty() {
            let mut builder = Server::builder();
            builder = builder
                .max_frame_size(max_frame_size)
                .http2_keepalive_interval(Some(Duration::from_secs(60 * 2)))
                .http2_keepalive_timeout(Some(Duration::from_secs(60)));

            if let Some(cert_path) = self.ssl_cert {
                let key_path = self.ssl_key.ok_or_else(|| {
                    anyhow::format_err!("Certificate and private key should both exist")
                })?;
                if let Some(ca) = &self.ssl_ca {
                    let ca = taosx_core::utils::cert::parse_certificate_to_string(ca)
                        .map_err(|err| anyhow::format_err!("Invalid ssl ca cert: {err:#}"))?;
                    crate::serve::controller::agent::set_grpc_ssl_ca_certificate(ca);
                } else {
                    bail!("ssl_cert, ssl_key, and ssl_ca should all exist");
                }
                let cert = std::fs::read_to_string(&cert_path)
                    .with_context(|| format!("Unable to open ssl cert file {}", cert_path))?;
                let key = std::fs::read_to_string(&key_path)
                    .with_context(|| format!("Unable to open ssl key file {}", key_path))?;
                let tls_config = ServerTlsConfig::new().identity(Identity::from_pem(&cert, &key));
                tracing::info!("SSL certificate loaded from {} and {}", cert_path, key_path);
                builder = builder
                    .tls_config(tls_config)
                    .context("SSL certificate error")?;
            }

            let servers = self
                .tcp
                .iter()
                .map(|addr| {
                    builder
                        .add_service(flight_service.clone())
                        .serve_with_shutdown(*addr, async {
                            let _ = tokio::signal::ctrl_c().await;
                            tracing::info!("Ctrl+C invoked, shutdown RPC service")
                        })
                })
                .collect::<Vec<_>>();
            futures::future::try_join_all(servers).await?;
        }
        #[cfg(unix)]
        if let Some(path) = self.unix {
            let uds = UnixListener::bind(path).unwrap();
            let stream = UnixListenerStream::new(uds);
            // let service = FlightServiceImpl { controller };
            Server::builder()
                .max_frame_size(max_frame_size)
                .http2_keepalive_interval(Some(Duration::from_secs(60 * 2)))
                .http2_keepalive_timeout(Some(Duration::from_secs(60)))
                .add_service(flight_service)
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
        let tcp = if is_support_ipv6() {
            vec![SocketAddr::from((
                Ipv6Addr::UNSPECIFIED,
                TAOSX_GRPC_DEFAULT_PORT,
            ))]
        } else {
            vec![SocketAddr::from((
                Ipv4Addr::UNSPECIFIED,
                TAOSX_GRPC_DEFAULT_PORT,
            ))]
        };
        Self {
            tcp,
            unix: Default::default(),
            ssl_cert: Default::default(),
            ssl_key: Default::default(),
            ssl_ca: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::{Duration, Instant};

    use crate::serve::tests::tracing_subscriber_init;
    use arrow::array::{ArrayRef, TimestampMillisecondArray};
    use arrow::record_batch::RecordBatch;
    use arrow::{
        datatypes::{DataType, Field, Schema},
        ipc::writer::IpcWriteOptions,
    };
    use arrow_flight::decode::FlightDataDecoder;
    use arrow_flight::{
        FlightData, HandshakeRequest,
        encode::{FlightDataEncoder, FlightDataEncoderBuilder},
        error::FlightError,
        flight_service_client::FlightServiceClient,
    };
    use futures::TryStreamExt;
    use tempfile::NamedTempFile;
    use tonic::{
        IntoStreamingRequest,
        codegen::Bytes,
        transport::{Channel, Endpoint},
    };

    async fn client_with_tcp() -> FlightServiceClient<Channel> {
        // let connector = tower::service_fn(move |_| TcpStream::connect("127.0.0.1:6051"));
        let channel = Endpoint::try_from("http://127.0.0.1:6051")
            .unwrap()
            .connect()
            .await
            .unwrap();
        FlightServiceClient::new(channel)
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn server_client() -> anyhow::Result<()> {
        unsafe {
            std::env::set_var("RUST_LOG", "INFO");
        }
        tracing_subscriber_init()?;
        let file = NamedTempFile::new().unwrap();
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let _ = std::fs::remove_file(path.clone());

        let request_future = async {
            let mut client = client_with_tcp().await;
            let req = HandshakeRequest::default();
            client
                .handshake(futures::stream::once(async { req }))
                .await
                .unwrap();

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

            struct FakeStream((), tokio::time::Interval, Instant);

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
                    tracing::info!("{item:?}");
                    std::task::Poll::Ready(Some(item))
                }
            }

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

            let data = FlightDataEncoderBuilder::new()
                .with_schema(schema.clone())
                .with_metadata(Bytes::from("metadata"))
                .with_options(
                    IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap(),
                )
                .build(FakeStream(
                    (),
                    tokio::time::interval(Duration::from_millis(1000)),
                    Instant::now() + Duration::from_secs(10),
                ));

            let req = Data { data }.into_streaming_request();

            let response = client.do_exchange(req).await.unwrap();
            let stream = FlightDataDecoder::new(response.into_inner().map_err(FlightError::from));

            stream
                .try_for_each(|_res| async move { Ok(()) })
                .await
                .unwrap();
        };

        request_future.await;
        println!("Client finished");
        Ok(())
    }
}
