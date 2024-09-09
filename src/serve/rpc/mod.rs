use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use anyhow::Context;
use arrow::{
    array::{ArrayRef, StringArray, TimestampMillisecondArray, UInt64Array},
    datatypes::{Field, Fields, Schema},
    record_batch::RecordBatch,
};
use arrow_flight::{
    decode::FlightDataDecoder,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use async_backtrace::framed;
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use futures::{Stream, TryStreamExt};
use linked_hash_map::LinkedHashMap;
use metrics::{atomics::AtomicU64, counter, gauge, histogram, IntoLabels};
use semver::VersionReq;
use serde::{Deserialize, Serialize};
use serde_json::json;
use taos::{Dsn, IntoDsn};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::RwLock;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use taosx_core::utils::get_string_content_from_param_value;
use taosx_core::{
    get_data_dir, utils::trace::TraceStreamId, CheckResponse, HeartbeatResponse, ListResponse,
    PutFileResp, QueryDataSourceResp,
};
use taosx_ipc::types::SampleResponse;
use taosx_metrics::MetricsEvents;

use crate::serve::controller::StringSender;
use crate::serve::{
    controller::{
        agent::{Activity, AgentToken, LevelFilter},
        TaskActivity, TaskDetail,
    },
    rpc::put::PutStream,
    scheduler::agent::AgentNotify,
};

use super::{
    controller::{AgentAction, AgentDataSetsSender, DsvSender, Task, TaskControllerRef},
    monitor::Monitor,
    scheduler::agent::{AgentActionsReceiver, AgentId, AgentNotifySender, AgentSpawnSender},
};

mod put;

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

pub(super) struct FlightServiceImpl {
    controller: TaskControllerRef,
    notify_sender: AgentNotifySender,
    activity_receiver: Arc<AgentActionsReceiver>,
    agent_connections: Arc<RwLock<HashMap<AgentId, ConnectionId>>>,
    request_id: Arc<AtomicU64>,
    datasets_senders: Arc<RwLock<LinkedHashMap<u64, AgentDataSetsSender>>>,
    dsv_senders: Arc<RwLock<LinkedHashMap<u64, DsvSender>>>,
    string_senders: Arc<RwLock<LinkedHashMap<u64, StringSender>>>,
    spawn_sender: AgentSpawnSender,
    monitor: Monitor,
}

async fn action_to_arrow(
    request_id: &Arc<AtomicU64>,
    datasets_senders: &Arc<RwLock<LinkedHashMap<u64, AgentDataSetsSender>>>,
    dsv_senders: &Arc<RwLock<LinkedHashMap<u64, DsvSender>>>,
    string_senders: &Arc<RwLock<LinkedHashMap<u64, StringSender>>>,
    controller: &TaskControllerRef,
    action: AgentAction,
) -> anyhow::Result<Option<RecordBatch>> {
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values([
        chrono::Utc::now().timestamp_millis(),
    ]));
    let req_id = request_id.fetch_add(1, Ordering::SeqCst);

    match action {
        AgentAction::Run(id, jid, rid) => {
            tracing::info!(
                task.id = id,
                task.jid = %jid,
                task.rid = rid,
                "Send run action"
            );
            let task = controller.get(id).await?;
            if let Some(mut task) = task {
                // handle dsn(from) params contains file(@)
                if let Err(err) = modify_task_dsn_params(&mut task.task).await {
                    tracing::error!(task.id = id, "Failed to modify task dsn params: {err:#}");
                    return Err(err);
                }
                #[derive(Serialize)]
                struct TaskInAgent {
                    #[serde(flatten)]
                    task: TaskDetail,
                    jid: Uuid,
                    rid: u64,
                }
                let context: ArrayRef =
                    Arc::new(StringArray::from_iter_values([serde_json::to_string(
                        &TaskInAgent { task, jid, rid },
                    )
                    .unwrap()]));
                let action: ArrayRef = Arc::new(StringArray::from_iter_values(["run".to_string()]));
                let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("ts", ts),
                    ("action", action),
                    ("context", context),
                    ("req_id", req_id),
                ])
                .context("failed to build record batch")?;
                return Ok(Some(batch));
            } else {
                tracing::warn!("Received Run action for task {id} but currently not found");
                return Ok(None);
            }
        }
        AgentAction::Stop(id) => {
            tracing::info!(task.id = id, "Send stop action to task {id}");
            let task = controller.get(id).await?;
            if let Some(task) = task {
                let context: ArrayRef =
                    Arc::new(StringArray::from_iter_values([serde_json::to_string(
                        &task,
                    )
                    .unwrap()]));
                let action: ArrayRef =
                    Arc::new(StringArray::from_iter_values(["stop".to_string()]));
                let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("ts", ts),
                    ("action", action),
                    ("context", context),
                    ("req_id", req_id),
                ])
                .context("failed to build record batch")?;
                return Ok(Some(batch));
            } else {
                tracing::warn!("Received Stop action for task {id} but currently not found");
                return Ok(None);
            }
        }
        AgentAction::Interrupt(id) => {
            tracing::info!(task.id = id, "Send interrupt action to task {id}");
            let task = controller.get(id).await?;
            if let Some(task) = task {
                let context: ArrayRef =
                    Arc::new(StringArray::from_iter_values([serde_json::to_string(
                        &task,
                    )
                    .unwrap()]));
                let action: ArrayRef =
                    Arc::new(StringArray::from_iter_values(["interrupt".to_string()]));
                let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("ts", ts),
                    ("action", action),
                    ("context", context),
                    ("req_id", req_id),
                ])
                .context("failed to build record batch")?;
                return Ok(Some(batch));
            } else {
                tracing::warn!("Received Cancel action for task {id} but currently not found");
                return Ok(None);
            }
        }
        AgentAction::Cancel(id) => {
            tracing::info!(task.id = id, "Send suspend action to task {id}");
            let task = controller.get(id).await?;
            if let Some(task) = task {
                let context: ArrayRef =
                    Arc::new(StringArray::from_iter_values([serde_json::to_string(
                        &task,
                    )
                    .unwrap()]));
                let action: ArrayRef =
                    Arc::new(StringArray::from_iter_values(["cancel".to_string()]));
                let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("ts", ts),
                    ("action", action),
                    ("context", context),
                    ("req_id", req_id),
                ])
                .context("failed to build record batch")?;
                return Ok(Some(batch));
            } else {
                tracing::warn!("Received Cancel action for task {id} but currently not found");
                return Ok(None);
            }
        }
        AgentAction::ListDataSets(dataset, sender) => {
            let context: ArrayRef =
                Arc::new(StringArray::from_iter_values([serde_json::to_string(
                    &dataset,
                )
                .unwrap()]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values(["list".to_string()]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build record batch")?;

            let datasets_senders = datasets_senders.clone();
            tokio::spawn(async move {
                let mut senders = datasets_senders.write().await;
                senders.insert(req_id, sender);
            });
            return Ok(Some(batch));
        }
        AgentAction::Check(dsn, sender) => {
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([
                serde_json::to_string(&dsn).unwrap(),
            ]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values(["check".to_string()]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build record batch")?;

            let dsv_senders = dsv_senders.clone();
            tokio::spawn(async move {
                let mut senders = dsv_senders.write().await;
                senders.insert(req_id, sender);
            });
            return Ok(Some(batch));
        }
        AgentAction::GetSample(dsn, sender) => {
            let action: ArrayRef = Arc::new(StringArray::from_iter_values(["sample".to_string()]));
            // modify dsn params
            let dsn = modify_dsn_params(dsn).await?.to_string();
            let context: ArrayRef =
                Arc::new(StringArray::from_iter_values([serde_json::to_string(&dsn)
                    .map_err(|err| {
                        anyhow::format_err!("failed to serialize dsn: {err:#}")
                    })?]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build GetSample message")?;

            let string_senders = string_senders.clone();
            tokio::spawn(async move {
                let mut senders = string_senders.write().await;
                senders.insert(req_id, sender);
            });
            return Ok(Some(batch));
        }
        AgentAction::PutFile(put_file_req, sender) => {
            let context: ArrayRef =
                Arc::new(StringArray::from_iter_values([serde_json::to_string(
                    &put_file_req,
                )
                .unwrap()]));
            let action: ArrayRef =
                Arc::new(StringArray::from_iter_values(["put-file".to_string()]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build record batch")?;
            let string_senders = string_senders.clone();
            tokio::spawn(async move {
                let mut senders = string_senders.write().await;
                senders.insert(req_id, sender);
            });
            return Ok(Some(batch));
        }
        AgentAction::QueryDataSource(query_data_source_req, sender) => {
            let context: ArrayRef =
                Arc::new(StringArray::from_iter_values([serde_json::to_string(
                    &query_data_source_req,
                )
                .unwrap()]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([
                "query-data-source".to_string()
            ]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build record batch")?;
            let string_senders = string_senders.clone();
            tokio::spawn(async move {
                let mut senders = string_senders.write().await;
                senders.insert(req_id, sender);
            });
            return Ok(Some(batch));
        }
        action => {
            tracing::warn!("Unknown action: {action:?}");
            return Ok(None);
        }
    }
}
impl FlightServiceImpl {
    pub fn _subscribe_agent_activity(&self, agent_id: AgentId) -> flume::Receiver<AgentAction> {
        let receiver = self.activity_receiver.resubscribe();

        let (tx, rx) = flume::bounded(1000);
        tokio::spawn(async move {
            let mut receiver = receiver;
            loop {
                match receiver.recv().await {
                    Ok((id, activity)) => {
                        if id == agent_id {
                            let _ = tx.send_async(activity).await;
                        }
                    }
                    Err(err) => match err {
                        tokio::sync::broadcast::error::RecvError::Closed => break,
                        tokio::sync::broadcast::error::RecvError::Lagged(_) => continue,
                    },
                }
            }
        });
        rx
    }

    pub fn subscribe_agent_action_flight(
        &self,
        agent_id: AgentId,
    ) -> (
        flume::Sender<Result<RecordBatch, FlightError>>,
        flume::Receiver<Result<RecordBatch, FlightError>>,
    ) {
        let receiver = self.activity_receiver.resubscribe();

        let (tx, rx) = flume::bounded(1000);
        let tx_cloned = tx.clone();
        let (req_id, senders, dsv_senders, string_senders, controller) = (
            self.request_id.clone(),
            self.datasets_senders.clone(),
            self.dsv_senders.clone(),
            self.string_senders.clone(),
            self.controller.clone(),
        );
        tokio::spawn(async move {
            let mut receiver = receiver;
            let tx = tx_cloned;
            let _ = std::env::set_current_dir(get_data_dir());
            info!("agent action flight listener start");
            loop {
                match receiver.recv().await {
                    Ok((id, action)) => {
                        if id == agent_id {
                            tracing::debug!("receive action: {:?}, agent id: {}", action, id);
                            if let Some(batch) = action_to_arrow(
                                &req_id,
                                &senders,
                                &dsv_senders,
                                &string_senders,
                                &controller,
                                action,
                            )
                            .await
                            .map_err(|err| FlightError::Tonic(Status::internal(err.to_string())))
                            .transpose()
                            {
                                if let Err(err) = tx.send_async(batch).await {
                                    tracing::info!(agent_id, "Task listener disconnected: {err:#}");
                                    break;
                                }
                            }
                        } else {
                            if tx.is_disconnected() {
                                tracing::info!(agent_id, "Task listener disconnected");
                                break;
                            }
                        }
                    }
                    Err(err) => match err {
                        tokio::sync::broadcast::error::RecvError::Closed => break,
                        tokio::sync::broadcast::error::RecvError::Lagged(_) => continue,
                    },
                }
            }
            info!("agent action flight listener stop");
        });
        (tx, rx)
    }

    fn replay_metrics_events_from_agent(metrics_events: MetricsEvents) {
        for event in metrics_events.events().to_owned() {
            let labels = event.labels.into_labels();
            match event.operation {
                taosx_metrics::MetricOperation::IncrementCounter(value) => {
                    counter!(event.key, labels).increment(value);
                }
                taosx_metrics::MetricOperation::SetCounter(value) => {
                    counter!(event.key, labels).absolute(value);
                }
                taosx_metrics::MetricOperation::IncrementGauge(value) => {
                    gauge!(event.key, labels).increment(value);
                }
                taosx_metrics::MetricOperation::DecrementGauge(value) => {
                    gauge!(event.key, labels).decrement(value);
                }
                taosx_metrics::MetricOperation::SetGauge(value) => {
                    gauge!(event.key, labels).set(value);
                }
                taosx_metrics::MetricOperation::RecordHistogram(value) => {
                    histogram!(event.key, labels).record(value);
                }
            }
        }
    }
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

        let client_version = meta.get("x-version").ok_or_else(|| {
            Status::aborted("The server does not compatible to your agent, please upgrade to a newer version")
        })?.to_str().map_err(|err| Status::aborted(format!("Invalid agent version: {err}")))?;
        // dbg!(&meta, &extension);
        tracing::info!("handshake with client {:?}", addr);

        let req = req.message().await?;

        if let Some(req) = req {
            // trigger agent "connect" action
            let mut res = HandshakeResponse {
                protocol_version: req.protocol_version,
                payload: req.payload,
            };
            let agent = self
                .controller
                .get_agent_with_token(&AgentToken::from(&res.payload))
                .await
                .map_err(|err| Status::permission_denied(format!("Invalid token: {err:#}")))?
                .ok_or_else(|| Status::permission_denied("Agent not found"))?;

            {
                // Agent version compatible check

                let req = VersionReq::parse(">=1.3.0").unwrap();

                let version = semver::Version::parse(client_version).map_err(|err| {
                    Status::aborted(format!("Invalid agent version: {err:#}", err = err))
                })?;

                if !req.matches(&version) {
                    self.notify_sender
                        .send(AgentNotify::AgentDisconnected(agent.id))
                        .map_err(|err| {
                            Status::internal(format!("Scheduler is not ready: {err:#}", err = err))
                        })?;

                    let outdated = format!("Agent core version {version} is not compatible to server, please upgrade to a newer version");
                    self.notify_sender
                        .send(AgentNotify::AgentActivity(
                            agent.id,
                            Activity::new::<String>(
                                agent.id,
                                Utc::now(),
                                LevelFilter::Error,
                                &outdated,
                                "outdated",
                                Some(
                                    json!({
                                        "version": client_version.to_string(),
                                    })
                                    .to_string(),
                                ),
                            ),
                        ))
                        .map_err(|err| {
                            Status::internal(format!("Scheduler is not ready: {err:#}", err = err))
                        })?;
                    return Err(Status::aborted(outdated));
                }
            }
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
        let addr = req.remote_addr();
        let (meta, _extension, req) = req.into_parts();

        let task_id = meta
            .get("x-task-id")
            .ok_or_else(|| Status::unavailable("Task id should be set"))
            .unwrap();
        let task_id: i64 = task_id.to_str().unwrap().parse().unwrap();
        let stream_trace_id = match meta.get("x-trace-id") {
            Some(stream_trace_id) => stream_trace_id.to_str().unwrap(),
            None => "0000",
        };

        // let message = req.try_next().await?;

        let put_stream = PutStream::new(
            self.controller.clone(),
            task_id,
            req,
            self.notify_sender.clone(),
            addr,
            TraceStreamId::from_hex(stream_trace_id),
            self.spawn_sender.clone(),
        )
        .await
        .map_err(|err| Status::unavailable(err.to_string()))?;

        Ok(Response::new(Box::pin(
            put_stream
                .into_flight_put_result()
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?,
        )))
    }

    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    #[instrument(skip(self, req))]
    async fn do_exchange(
        &self,
        req: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let remote = req.remote_addr();
        let (mut meta, extension, req) = req.into_parts();
        tracing::info!("Receive do_exchange stream from {:?}", remote);
        let token = meta
            .get("x-token")
            .ok_or_else(|| Status::aborted("Token should be set"))?
            .to_str()
            .map_err(|err| Status::aborted(format!("Invalid token: {err}")))?;

        let controller = self.controller.clone();
        let agent = controller
            .agent_connect_with_token(&AgentToken(token.to_string()), remote.as_ref())
            .await
            .map_err(|err| Status::permission_denied(format!("Agent connection error: {err}")))?;

        let agent_id = agent.id;
        let (tx, rx) = self.subscribe_agent_action_flight(agent_id);

        let connection_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        meta.append("x-cid", connection_id.to_string().parse().unwrap());
        {
            // Update agent connection id to ensure only one connection per agent.
            self.agent_connections
                .write()
                .await
                .insert(agent_id, connection_id);
            self.notify_sender
                .send(AgentNotify::AgentConnected(agent_id))
                .map_err(|err| Status::internal(format!("Scheduler is not ready: {err:#}")))?;
            tracing::debug!(
                "Sent agent notify: {:?}",
                AgentNotify::AgentConnected(agent_id)
            );
        }

        // dbg!(&agent);
        // let agent: Agent = serde_json::from_str(r#"
        // {
        //     "id": 2, "dsn": "taos:///", "name": "agent1", "cluster_id":"", "user_id":"", "connectors": [], "created_at":"2022-02-02T00:00:00Z"
        // }"#).unwrap();

        // let (tx, rx) = flume::bounded::<Result<RecordBatch, FlightError>>(100);

        // let sender = tx.clone();
        let controller_runner = controller.clone();
        let agent_id = agent.id;
        // let tx = tx.clone();
        let notify_sender = self.notify_sender.clone();
        let datasets_sender = self.datasets_senders.clone();
        let dsv_senders = self.dsv_senders.clone();
        let string_senders = self.string_senders.clone();
        let agent_connections = self.agent_connections.clone();
        tokio::spawn(async move {
            let span = tracing::trace_span!("agent_rpc", agent = agent_id);
            let _enter = span.enter();

            let encoder = FlightDataDecoder::new(req.map_err(FlightError::Tonic));
            let last_heart_ms = AtomicU64::new(0);
            let result = encoder
                .try_for_each_concurrent(20, |data| async {
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
                            let req_id = res
                                .column(3)
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap();

                            const ORDER: Ordering = Ordering::Relaxed;

                            for _ in 0..rows {
                                let (ts, action, _req_id, context) = (
                                    ts.value_as_datetime_with_tz(
                                        0,
                                        ts.timezone().unwrap_or("UTC").parse().unwrap(),
                                    )
                                    .unwrap(),
                                    action.value(0),
                                    req_id.value(0),
                                    res
                                        .column(2)
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .unwrap().value(0),
                                );
                                match action {
                                    "list" => {
                                        let resp: ListResponse =
                                            serde_json::from_str(&context).unwrap();

                                        let datasets_senders = datasets_sender.clone();
                                        tokio::spawn(async move {
                                            let req_id = resp.req_id;
                                            if let Some(sender) =
                                                datasets_senders.write().await.remove(&req_id)
                                            {
                                                if let Err(err) = sender.send_async(resp.res).await {
                                                    warn!(
                                                        agent = agent_id,
                                                        req_id = req_id,
                                                        "List data sets response send failed: {err:#}"
                                                    );
                                                }
                                            } else {
                                                warn!(
                                                    agent = agent_id,
                                                    req_id = req_id,
                                                    "List data sets request id has no receiver"
                                                );
                                            }
                                        });
                                    }
                                    "check" => {
                                        let resp: CheckResponse =
                                            serde_json::from_str(&context).unwrap();

                                        let dsv_senders = dsv_senders.clone();
                                        tokio::spawn(async move {
                                            let req_id = resp.req_id;
                                            if let Some(sender) =
                                                dsv_senders.write().await.remove(&req_id)
                                            {
                                                if let Err(err) = sender.send_async(resp.res).await {
                                                    warn!(
                                                        agent = agent_id,
                                                        req_id = req_id,
                                                        "List data sets response send failed: {err:#}"
                                                    );
                                                }
                                            } else {
                                                warn!(
                                                    agent = agent_id,
                                                    req_id = req_id,
                                                    "List data sets request id has no receiver"
                                                );
                                            }
                                        });
                                    }
                                    "sample" => {
                                        let resp: SampleResponse = serde_json::from_str(&context).unwrap();
                                        let string_senders = string_senders.clone();
                                        tokio::spawn(async move {
                                            let req_id = resp.req_id;
                                            if let Some(sender) = string_senders.write().await.remove(&req_id)
                                            {
                                                if let Err(err) = sender.send_async(resp.res).await {
                                                    warn!(
                                                        agent = agent_id,
                                                        req_id = req_id,
                                                        "get sample response send failed: {err:#}"
                                                    );
                                                }
                                            } else {
                                                warn!(
                                                    agent = agent_id,
                                                    req_id = req_id,
                                                    "get sample request id has no receiver"
                                                );
                                            }
                                        });
                                    }
                                    "put-file" => {
                                        let resp: PutFileResp = serde_json::from_str(&context).unwrap();
                                        let string_senders = string_senders.clone();
                                        tokio::spawn(async move {
                                            let req_id = resp.req_id;
                                            if let Some(sender) = string_senders.write().await.remove(&req_id)
                                            {
                                                if let Err(err) = sender.send_async(resp.res).await {
                                                    error!(
                                                        agent = agent_id,
                                                        req_id = req_id,
                                                        "Send PutFileResp failed: {err:#}"
                                                    );
                                                }
                                            } else {
                                                error!(
                                                    agent = agent_id,
                                                    req_id = req_id,
                                                    "PutFileResp has no receiver"
                                                );
                                            }

                                        });
                                    }
                                    "query-data-source" => {
                                        let resp: QueryDataSourceResp = serde_json::from_str(&context).unwrap();
                                        let string_senders = string_senders.clone();
                                        tokio::spawn(async move {
                                            let req_id = resp.req_id;
                                            if let Some(sender) = string_senders.write().await.remove(&req_id)
                                            {
                                                if let Err(err) = sender.send_async(resp.output).await {
                                                    error!(
                                                        agent = agent_id,
                                                        req_id = req_id,
                                                        "Send QueryDataSourceResp failed: {err:#}"
                                                    );
                                                }
                                            } else {
                                                error!(
                                                    agent = agent_id,
                                                    req_id = req_id,
                                                    "QueryDataSourceResp has no receiver"
                                                );
                                            }
                                        });
                                    }
                                    "agent-activity" => {
                                        let activity: Activity = serde_json::from_str(&context)
                                            .map_err(|err| {
                                                anyhow::format_err!(
                                                    "Invalid activity `{context}`: {err:#}"
                                                )
                                            })
                                            .unwrap();
                                        info!(?activity, "agent activity");
                                        // let _ =
                                        //     controller_runner.push_agent_activity(activity).await;
                                        let _ = notify_sender
                                            .send(AgentNotify::AgentActivity(agent_id, activity));
                                    }
                                    "task-activity" => {
                                        let activity: TaskActivity = serde_json::from_str(&context)
                                            .map_err(|err| {
                                                anyhow::format_err!(
                                                    "Invalid activity `{context}`: {err:#}"
                                                )
                                            })
                                            .unwrap();
                                        let notify_sender = notify_sender.clone();
                                        tokio::spawn(async move {
                                            info!(?activity, "task activity");
                                            let _ = notify_sender
                                                .send(AgentNotify::TaskActivity(agent_id, activity));
                                        });
                                    }
                                    "heartbeat-ok" => {
                                        let resp: HeartbeatResponse =
                                            serde_json::from_str(&context).unwrap();
                                        let delay = resp.duration();
                                        if delay > chrono::Duration::seconds(5) {
                                            info!(
                                                agent = agent_id,
                                                "Agent maybe not health, delay {:?}", delay
                                            );
                                        } else {
                                            info!(
                                                agent = agent_id,
                                                "Agent is alive, delay: {:?}", delay
                                            );
                                        }
                                    }
                                    "heartbeat" => {
                                        tracing::trace!("Received heartbeat");
                                        let req = ts.naive_utc().and_utc();
                                        last_heart_ms.store(req.timestamp_millis() as u64, ORDER);
                                        let resp = HeartbeatResponse {
                                            req,
                                            res: Utc::now(),
                                        };

                                        let val =
                                            Arc::new(TimestampMillisecondArray::from_iter_values([
                                                Utc::now().timestamp_millis(),
                                            ]))
                                                as ArrayRef;
                                        let context: ArrayRef =
                                            Arc::new(StringArray::from_iter_values([
                                                serde_json::to_string(&resp).unwrap(),
                                            ]));
                                        let action: ArrayRef =
                                            Arc::new(StringArray::from_iter_values([
                                                "heartbeat-ok".to_string(),
                                            ]));
                                        let req_id: ArrayRef =
                                            Arc::new(UInt64Array::from_iter_values([
                                                0u64,
                                            ]));
                                        let item = RecordBatch::try_from_iter(vec![
                                            ("ts", val),
                                            ("action", action),
                                            ("context", context),
                                            ("req_id", req_id),
                                        ])
                                        .map_err(FlightError::Arrow);
                                        // tracing::info!("Send heartbeat response");
                                        let _ = tx.send_async(item).await;
                                        // return std::task::Poll::Ready(Some(item));
                                    }
                                    "metrics-events" => {
                                        match serde_json::from_str::<MetricsEvents>(context) {
                                            Ok(events) => {
                                                tokio::spawn(async move {
                                                    tracing::trace!("Received metrics events, total: {}", events.len());
                                                    Self::replay_metrics_events_from_agent(events);
                                                });
                                            }
                                            Err(err) => {
                                                tracing::warn!(?err, "Invalid metrics events");
                                            }
                                        }

                                    }
                                    action => {
                                        warn!("Unknown action: {action}");
                                    }
                                }
                            }
                            // batch.
                            // todo: send data to controller.
                        }
                    }
                    Ok(())
                })
                .await;
            tracing::info!(
                agent.id = agent_id,
                agent.cid = connection_id,
                "Agent RPC stopped"
            );

            let mut guard = agent_connections.write().await;
            if let Some(cid) = guard.get(&agent_id) {
                if *cid == connection_id {
                    guard.remove(&agent_id);

                    let context = result.err().map(|err| {
                        json!({"code": 0xFFFFi32, "message": err.to_string()}).to_string()
                    });
                    let activity = Activity::new::<String>(
                        agent_id,
                        Utc::now(),
                        LevelFilter::Warn,
                        "Disconnected.",
                        "disconnected",
                        context,
                    );
                    if let Err(err) = notify_sender.send(AgentNotify::AgentDisconnected(agent_id)) {
                        tracing::error!(agent = agent_id, "Agent disconnected: {err:#}");
                    }
                    let _ = controller_runner.push_agent_activity(activity).await?;

                    tracing::info!(agent = agent_id, "Agent RPC stopped");
                } else {
                    tracing::warn!(
                        agent.id = agent_id,
                        agent.cid = connection_id,
                        "Agent RPC stopped but current connection id({cid}) is not matched, do nothing."
                    );
                }
            }
            Ok::<_, anyhow::Error>(())
        });
        let stream: Self::DoExchangeStream = Box::pin({
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
                Field::new("req_id", arrow::datatypes::DataType::UInt64, false),
            ])));

            let encoder = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(rx.into_stream());
            encoder.map_err(|err| Status::internal(err.to_string()))
        });
        let response = tonic::Response::from_parts(meta, stream, extension);
        Ok(response)
    }

    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let (_meta, _part, action) = request.into_parts();
        // dbg!(_meta, _part, &action);
        match action.r#type.as_str() {
            "TaskStatus" => {
                // task.

                let mut status: TaskActivity = serde_json::from_slice(&action.body)
                    .map_err(|err| Status::invalid_argument(format!("{err}: {:?}", action.body)))?;

                if status.activity == "taosx-agent is suspended by SIGINT" {
                    status.status = "waiting".to_string();
                }

                tracing::info!(?status, "Received task status");
                let task_id = status.id;
                let task = self
                    .controller
                    .get(task_id)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?
                    .ok_or_else(|| Status::not_found(format!("Task {task_id} not found")))?;

                let agent_id = task.via.ok_or_else(|| {
                    Status::internal(format!("Task {task_id} does not relate to any agent"))
                })?;
                self.notify_sender
                    .send(AgentNotify::TaskActivity(agent_id, status))
                    .map_err(|err| Status::internal(format!("Scheduler is not ready: {err:#}")))?;

                // self.controller
                //     .push_task_status(&status)
                //     .await
                //     .map_err(|err| Status::internal(err.to_string()))?;
                Ok(Response::new(Box::pin(futures::stream::iter([]))))
            }
            "GetMonitorConfig" => {
                let mut config = self.monitor.cfg.as_map();
                config.insert("taosx_id".to_string(), self.monitor.taosx_id.to_string());
                let message = serde_json::to_vec(&config).unwrap();
                Ok(Response::new(Box::pin(futures::stream::iter([Ok(
                    arrow_flight::Result {
                        body: message.into(),
                        ..Default::default()
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
}

#[instrument(skip(task))]
async fn modify_task_dsn_params(task: &mut Task) -> anyhow::Result<()> {
    let dsn = modify_dsn_params(task.from.clone()).await?;
    task.from = dsn.to_string();
    Ok(())
}

#[instrument(skip(dsn))]
async fn modify_dsn_params(dsn: impl IntoDsn) -> anyhow::Result<Dsn> {
    let mut dsn = dsn.into_dsn()?.clone();
    tracing::debug!("dsn before modify: {:?}", &dsn);
    // let mut map = BTreeMap::new();
    // for (k, v) in dsn.params {
    //     let new_value = if k == "csv_config_file" {
    //         encode_csv_config_file(v.clone())?
    //     } else if k == "transform_config_file" {
    //         String::new()
    //     } else if v.contains("@") {
    //         get_string_content_from_param_value(&v, false, false)?.unwrap_or(String::new())
    //     } else {
    //         String::new()
    //     };
    //     let new_value = if new_value.is_empty() { v } else { new_value };
    //     map.insert(k, new_value);
    // }
    // dsn.params = map;

    if let Some(v) = dsn.params.get("csv_config_file") {
        let csv_path = &v[1..];
        dsn.params
            .insert("csv_config_file_origin".to_string(), csv_path.to_string());
    }
    for (k, v) in dsn.params.iter_mut() {
        if k == "csv_config_file" {
            *v = encode_csv_config_file(v.clone())?;
            continue;
        }

        if v.contains("@") {
            if let Some(new_value) = get_string_content_from_param_value(&v, false, false)? {
                *v = new_value;
            }
        }
    }

    tracing::debug!("dsn after modify: {:?}", &dsn);
    Ok(dsn)
}

pub fn encode_csv_config_file(csv_path: String) -> anyhow::Result<String> {
    let mut new_value = String::new();

    // TODO use mime instead
    let (files, strs): (Vec<String>, Vec<String>) = csv_path
        .split(",")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .partition(|v| v.starts_with("@"));
    let file_len = files.len();
    for file in files {
        tracing::debug!(
            "current dir: {}",
            std::env::current_dir().unwrap().to_str().unwrap()
        );
        let file_data = std::fs::read(&file[1..])
            .with_context(|| anyhow::format_err!("Failed to read file: {}", &file[1..]))?;
        new_value.push_str(general_purpose::STANDARD.encode(file_data).as_str());
        new_value.push_str(",");
    }
    if file_len > 0 {
        new_value.pop();
    }
    let str_len = strs.len();
    for content in strs {
        new_value.push_str(content.as_str());
        new_value.push_str(",");
    }
    if str_len > 0 {
        new_value.pop();
    }

    Ok(new_value)
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
        channel: AgentRpcChannel,
        spawn_sender: AgentSpawnSender,
        monitor: Monitor,
    ) -> Result<(), anyhow::Error> {
        let max_frame_size: Option<u32> = Some((1 << 24) - 1 as u32);
        let activity_receiver = channel.agent_activity_receiver;
        let service = FlightServiceImpl {
            controller: controller.clone(),
            notify_sender: channel.agent_notify_sender,
            activity_receiver: Arc::new(activity_receiver),
            datasets_senders: Arc::new(RwLock::new(LinkedHashMap::new())),
            dsv_senders: Arc::new(RwLock::new(LinkedHashMap::new())),
            string_senders: Arc::new(RwLock::new(LinkedHashMap::new())),
            request_id: Arc::new(AtomicU64::new(0)),
            agent_connections: Arc::new(RwLock::new(HashMap::new())),
            monitor,
            spawn_sender,
        };
        let flight_service = FlightServiceServer::new(service);
        let flight_service = flight_service
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .max_decoding_message_size(std::usize::MAX)
            .max_encoding_message_size(std::usize::MAX);
        if let Some(tcp) = self.tcp {
            Server::builder()
                .max_frame_size(max_frame_size)
                .http2_keepalive_interval(Some(Duration::from_secs(60 * 2)))
                .http2_keepalive_timeout(Some(Duration::from_secs(60)))
                .add_service(flight_service.clone())
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
        datatypes::{DataType, Field, Schema},
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

    use crate::serve::tests::tracing_subscriber_init;

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
    #[ignore]
    async fn server_client() -> anyhow::Result<()> {
        unsafe {
            std::env::set_var("RUST_LOG", "INFO");
        }
        tracing_subscriber_init()?;
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
                    (),
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
                .try_for_each(|_res| async move {
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
