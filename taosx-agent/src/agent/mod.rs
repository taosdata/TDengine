use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::FlightClient;
use arrow_flight::{encode::FlightDataEncoderBuilder, Action as FlightAction};
use cfg_if::cfg_if;
use chrono::{DateTime, Utc};
use flume::{Receiver, Sender};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use taosx_core::utils::files::decompress_and_write_file;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tracing::{info, instrument};

use taosx_core::{
    get_data_dir, list_datasets_from, plugins, validate_dsn, Activity, CheckResponse, DataSetsReq,
    Fail, HeartbeatResponse, ListResponse, PutFileReq, PutFileResp, QueryDataSourceReq,
    QueryDataSourceResp, RespAction, Response, SampleResponse,
};

use crate::runner::Action;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Client {
    pub endpoint: String,
    pub client: FlightClient,
    pub agent: Agent,
    pub req_id: Arc<AtomicU64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct Agent {
    pub id: i64,

    pub dsn: String,
    pub name: String,
    pub cluster_id: String,
    pub user_id: String,
}

/// A streaming workflow task description.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Task {
    /// Unique id for the task item.
    pub id: i64,

    /// Job id.
    pub jid: uuid::Uuid,

    /// Current run id in the job.
    pub rid: i64,

    /// The stream data source.
    pub from: String,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    pub to: String,

    /// Number of jobs for task running.
    pub jobs: u16,

    /// Agent Id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via: Option<i64>,

    /// Compression level when need (for backup only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_level: Option<u8>,

    /// Force for some risking steps.
    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    pub force: bool,

    /// Created time.
    created_at: DateTime<Utc>,

    /// Stopped time.
    #[serde(skip_serializing_if = "Option::is_none")]
    finished_at: Option<DateTime<Utc>>,

    /// Last modified time.
    #[serde(skip_serializing_if = "Option::is_none")]
    last_modified_at: Option<DateTime<Utc>>,

    /// The current status of the tasks.
    status: String,

    /// Status reason (only for status: failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,

    /// Add after_delete hook action, the string would be action name, with or without some configuration.
    ///
    /// It will do nothing if the action is not supported by a specific task case.
    #[serde(skip_serializing_if = "Option::is_none")]
    after_delete: Option<String>,
    /// A task name.
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    /// break points
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakpoints: Option<String>,
}
const fn is_false(b: &bool) -> bool {
    *b
}

async fn new_channel(endpoint: String) -> anyhow::Result<Channel> {
    cfg_if! {
        if #[cfg(windows)] {
           let tcp_keepalive = None;
        } else {
           let tcp_keepalive = Some(Duration::from_secs(5));
        }
    };
    Endpoint::try_from(endpoint.clone())
        .map_err(|err| anyhow::format_err!("Unable to create endpoint on `{endpoint}`: {err:#}"))?
        .keep_alive_while_idle(true)
        .tcp_keepalive(tcp_keepalive)
        .http2_keep_alive_interval(Duration::from_secs(13))
        .keep_alive_timeout(Duration::from_secs(120))
        .connect()
        .await
        .map_err(|err| anyhow::format_err!("Unable to connect with endpoint `{endpoint}`: {err:#}"))
}
impl Client {
    pub async fn new(endpoint: impl Display, token: impl Display) -> Result<Self> {
        let endpoint = endpoint.to_string();
        let token = token.to_string();
        const MAX_RETRIES: usize = 5;
        let mut retries = 0;
        let channel = loop {
            match new_channel(endpoint.clone()).await {
                Ok(channel) => break Ok(channel),
                Err(err) => {
                    retries += 1;
                    info!(
                        "Unable to connect to server, retry {}/{}",
                        retries, MAX_RETRIES
                    );
                    if retries > MAX_RETRIES {
                        break Err(err);
                    } else {
                        continue;
                    }
                }
            }
        }?;

        let mut client = FlightClient::new(channel);
        client.add_header("x-token", &token)?;
        client.add_header("x-version", crate::build::PKG_VERSION)?;
        let result = client
            .handshake(token.to_string())
            .await
            .with_context(|| anyhow::format_err!("Handshake error with token"))?;
        let agent: Agent = serde_json::from_slice(&result)?;

        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
            agent,
            req_id: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn agent(&self) -> &Agent {
        &self.agent
    }
    pub async fn push_status(&mut self, status: &Activity) -> Result<()> {
        tracing::info!("Push status {status:?} to server");
        let status_bytes = serde_json::to_vec(status)?;
        let action = FlightAction::new("TaskStatus", status_bytes);
        let _resp: Vec<_> = self.client.do_action(action).await?.try_collect().await?;
        Ok(())
    }

    pub async fn get_taosx_monitor_config(&mut self) -> Option<HashMap<String, String>> {
        tracing::info!("Get monitor config from server");
        let action = arrow_flight::Action::new("GetMonitorConfig", "GetMonitorConfig");
        let result = self.client.do_action(action).await;
        if let Err(err) = result {
            tracing::error!("Can't get monitor config from server: {err:#}");
            return None;
        }
        let result: std::prelude::v1::Result<Vec<bytes::Bytes>, arrow_flight::error::FlightError> =
            result.unwrap().try_collect().await;
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

    pub async fn wait_tasks(
        &mut self,
        sender: flume::Sender<Action>,
        resp_tx: Sender<RespAction>,
        resp_rx: Receiver<RespAction>,
    ) -> Result<()> {
        tracing::info!("Wait tasks from server");
        let schema = Arc::new(
            Schema::new(vec![
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("action", DataType::Utf8, false),
                Field::new("context", DataType::Utf8, true),
                Field::new("req_id", DataType::UInt64, false),
            ])
            .with_metadata(HashMap::from_iter([(
                "x-task-id".to_string(),
                "1".to_string(),
            )])),
        );

        fn resp_action_to_arrow(
            action: RespAction,
            req_id: u64,
        ) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
            match action {
                RespAction::Heartbeat => {
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                        Utc::now().timestamp_millis()
                    ])) as ArrayRef;
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter([Option::<String>::None]));
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(["heartbeat".to_string()]));
                    tracing::info!("Send heartbeat request: {req_id}");
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

                    RecordBatch::try_from_iter(vec![
                        ("ts", val),
                        ("action", action),
                        ("context", context),
                        ("req_id", req_id),
                    ])
                }
                RespAction::HeartbeatOk(resp) => {
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                        Utc::now().timestamp_millis()
                    ])) as ArrayRef;
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &resp,
                        )
                        .unwrap()]));
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(["heartbeat-ok".to_string()]));
                    tracing::info!("Send heartbeat response: {req_id}");
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
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                        Utc::now().timestamp_millis()
                    ])) as ArrayRef;
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &sets,
                        )
                        .unwrap()]));
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(["list".to_string()]));
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
                    let item = RecordBatch::try_from_iter(vec![
                        ("ts", val),
                        ("action", action),
                        ("context", context),
                        ("req_id", req_id),
                    ]);
                    tracing::info!("{item:?}");
                    item
                }
                RespAction::CheckOk(response) => {
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                        Utc::now().timestamp_millis()
                    ])) as ArrayRef;
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &response,
                        )
                        .unwrap()]));
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(["check".to_string()]));
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

                    RecordBatch::try_from_iter(vec![
                        ("ts", val),
                        ("action", action),
                        ("context", context),
                        ("req_id", req_id),
                    ])
                }
                RespAction::SampleOk(resp) => {
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                        Utc::now().timestamp_millis()
                    ])) as ArrayRef;
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(["sample".to_string()]));
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &resp,
                        )
                        .unwrap()]));
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

                    RecordBatch::try_from_iter(vec![
                        ("ts", val),
                        ("action", action),
                        ("context", context),
                        ("req_id", req_id),
                    ])
                }
                RespAction::PutFileOk(resp) => {
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                        Utc::now().timestamp_millis()
                    ])) as ArrayRef;
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(["put-file".to_string()]));
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &resp,
                        )
                        .unwrap()]));
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

                    RecordBatch::try_from_iter(vec![
                        ("ts", val),
                        ("action", action),
                        ("context", context),
                        ("req_id", req_id),
                    ])
                }
                RespAction::QueryDataSourceOk(resp) => {
                    let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                        Utc::now().timestamp_millis()
                    ])) as ArrayRef;
                    let action: ArrayRef = Arc::new(StringArray::from_iter_values([
                        "query-data-source".to_string(),
                    ]));
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &resp,
                        )
                        .unwrap()]));
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
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &activity,
                        )
                        .unwrap()]));
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(
                            ["agent-activity".to_string()],
                        ));
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

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
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &activity,
                        )
                        .unwrap()]));
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(["task-activity".to_string()]));
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

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
                    let action: ArrayRef =
                        Arc::new(StringArray::from_iter_values(["task-metrics".to_string()]));
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &metrics,
                        )
                        .unwrap()]));
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

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
                        Arc::new(StringArray::from_iter_values(
                            ["metrics-events".to_string()],
                        ));
                    let context: ArrayRef =
                        Arc::new(StringArray::from_iter_values([serde_json::to_string(
                            &metrics_event,
                        )
                        .unwrap()]));
                    let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

                    RecordBatch::try_from_iter(vec![
                        ("ts", val),
                        ("action", action),
                        ("context", context),
                        ("req_id", req_id),
                    ])
                }
            }
        }

        let req = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(
                resp_rx
                    .into_stream()
                    .enumerate()
                    .map(|(req_id, action)| Ok(resp_action_to_arrow(action, req_id as _).unwrap())),
            )
            .map(|v| v.unwrap());
        let mut stream = self.client.do_exchange(req).await?;
        while let Some(res) = stream.try_next().await? {
            // dbg!(&res);
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

                tracing::info!("At [{ts}] action `{action}` triggered");
                #[derive(Deserialize)]
                struct TaskWithId {
                    id: i64,
                }
                match action {
                    "run" => {
                        let task: Task = serde_json::from_str(context).unwrap();
                        info!("Start task {}", task.id);
                        sender.send_async(Action::Run(task)).await?;
                    }
                    "stop" => {
                        let task: TaskWithId = serde_json::from_str(context).unwrap();
                        info!("Stop task {}", task.id);
                        sender.send_async(Action::Stop(task.id)).await?;
                        // let task:
                    }
                    "cancel" => {
                        let task: TaskWithId = serde_json::from_str(context).unwrap();
                        info!("Cancel task {}", task.id);
                        sender.send_async(Action::Cancel(task.id)).await?;
                        // let task:
                    }
                    "interrupt" => {
                        let task: TaskWithId = serde_json::from_str(context).unwrap();
                        info!("Interrupt task {}", task.id);
                        sender.send_async(Action::Interrupt(task.id)).await?;
                        // let task:
                    }
                    "list" => {
                        let req: DataSetsReq = serde_json::from_str(context).unwrap();
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(async move {
                            let sets = list_datasets_from(&req).await.map_err(Fail::new);
                            let send_ok = resp_tx
                                .send_async(RespAction::ListOk(ListResponse {
                                    req_id,
                                    req,
                                    res: sets,
                                }))
                                .await;
                            if let Err(err) = send_ok {
                                tracing::error!("Can't send list response to server: {err:#}");
                            }
                        });
                    }
                    "check" => {
                        let dsn: String = serde_json::from_str(context).unwrap();
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(async move {
                            let dsv = validate_dsn(dsn.clone()).await;
                            let send_ok = resp_tx
                                .send_async(RespAction::CheckOk(CheckResponse {
                                    req_id,
                                    req: dsn,
                                    res: dsv,
                                }))
                                .await;
                            if let Err(err) = send_ok {
                                tracing::error!(
                                    "Can't send data source validation response to server: {err:#}"
                                );
                            }
                        });
                    }
                    "sample" => {
                        let dsn: String = serde_json::from_str(context).unwrap();
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(async move {
                            let sample = plugins::get_sample(dsn.clone()).await;
                            let res = match sample {
                                Ok(sample) => match serde_json::to_string(&sample) {
                                    Ok(s) => Response::Ok(s),
                                    Err(err) => Response::Err(Fail::new(anyhow::anyhow!(
                                        "failed to serialize sample data, cause: {}",
                                        err.to_string()
                                    ))),
                                },
                                Err(err) => Response::Err(Fail::new(err)),
                            };

                            let send_ok = resp_tx
                                .send_async(RespAction::SampleOk(SampleResponse {
                                    req_id,
                                    req: dsn,
                                    res,
                                }))
                                .await;
                            if let Err(err) = send_ok {
                                tracing::error!("Can't send GetSample response to server: {err:#}");
                            }
                        });
                    }
                    "put-file" => {
                        let req: PutFileReq = serde_json::from_str(context).unwrap();
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(do_put_file(req, req_id, resp_tx));
                    }
                    "query-data-source" => {
                        tracing::info!(?req_id, "[query-data-source]: {}", &context);
                        let req: QueryDataSourceReq = serde_json::from_str(context).unwrap();
                        let resp_tx = resp_tx.clone();
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
                    "heartbeat" => {
                        let resp = HeartbeatResponse {
                            req: ts.naive_utc().and_utc(),
                            res: Utc::now(),
                        };
                        resp_tx.send_async(RespAction::HeartbeatOk(resp)).await?;
                    }
                    "heartbeat-ok" => {
                        let resp: HeartbeatResponse = serde_json::from_str(context).unwrap();
                        // let delay = resp.duration().to_std().unwrap();
                        info!(
                            "Server is alive, delay: {}ms",
                            resp.duration().num_milliseconds()
                        );
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

#[instrument(skip_all)]
pub async fn listen_task_metrics(resp_tx: Sender<RespAction>) -> Result<()> {
    use crate::agent::plugins::sink::ipc_metric::AGENT_METRICS_SENDER;

    let (tx, rx) = flume::unbounded();

    let _ = AGENT_METRICS_SENDER.set(tx);

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let mut vec = Vec::new();
        while let Ok(v) = rx.try_recv() {
            vec.push(v);
        }
        if !vec.is_empty() {
            let resp = RespAction::TaskMetrics(vec);
            resp_tx.send_async(resp).await?;
        }
    }
}

async fn do_put_file(req: PutFileReq, req_id: u64, resp_tx: Sender<RespAction>) {
    let data_dir = get_data_dir();
    let mut path = data_dir.join(req.path);
    let decompress = req.decompress;
    tracing::info!("[put-file] path={path:?}");
    if decompress {
        let extension = path.extension().unwrap_or_default();
        if extension == "gz" {
            path.set_extension("");
            tracing::info!("[put-file] Decompress file to {}", path.display());
        } else {
            let err_msg = "Decompress is enabled, but file extension is not .gz";
            tracing::error!("[put-file] {}", err_msg);
            let _send_err = resp_tx.send_async(RespAction::PutFileOk(PutFileResp {
                req_id,
                path: path.display().to_string(),
                res: Response::Err(Fail::new(anyhow::anyhow!("{}", err_msg))),
            }));
            return;
        }
    } else {
        tracing::info!("[put-file] Write file to {}", path.display());
    }
    // If parent folders not exists, try to create them
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            match tokio::fs::create_dir_all(&parent).await {
                Ok(_) => tracing::info!("[put-file] Directory created successfully"),
                Err(e) => tracing::error!("[put-file] Failed to create directory: {}", e),
            }
        }
    }
    let result = if decompress {
        decompress_and_write_file(&path, &req.data)
    } else {
        tokio::fs::write(&path, &req.data).await
    };

    match result {
        Ok(_) => {
            let _send_ok = resp_tx
                .send_async(RespAction::PutFileOk(PutFileResp {
                    req_id,
                    path: path.display().to_string(),
                    res: Response::Ok("Ok".to_string()),
                }))
                .await;
        }
        Err(err) => {
            tracing::error!("[put-file] Write file error: {err:#}");
            let _send_ok = resp_tx
                .send_async(RespAction::PutFileOk(PutFileResp {
                    req_id,
                    path: path.display().to_string(),
                    res: Response::Err(Fail::new(err)),
                }))
                .await;
        }
    }
}
