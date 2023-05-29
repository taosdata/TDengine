use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::writer::IpcWriteOptions,
};

use arrow_flight::FlightClient;
use arrow_flight::{
    encode::{FlightDataEncoder, FlightDataEncoderBuilder},
    error::FlightError,
    FlightData,
};
use chrono::{DateTime, NaiveDate, Utc};
use flume::Receiver;
use futures::{FutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use taosx_core::{list_datasets_from, DataSet, DataSetsReq, Fail, ListResponse, RespAction};
use tonic::{codegen::Bytes, transport::Endpoint};
use tracing::info;

use crate::runner::Action;

#[derive(Debug)]
pub struct Client {
    pub endpoint: String,
    pub client: FlightClient,
    pub agent: Agent,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Created,
    Alive,
    Error,
}
#[derive(Debug, Clone, Deserialize)]
pub struct Agent {
    pub id: i64,

    pub dsn: String,
    pub name: String,
    pub cluster_id: String,
    pub user_id: String,
    pub expire_date: Option<NaiveDate>,
    pub connectors: Vec<String>,

    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    #[allow(dead_code)]
    last_modified_at: Option<DateTime<Utc>>,
    #[allow(dead_code)]
    status: Option<AgentStatus>,
}

/// A streaming workflow task description.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Task {
    /// Unique id for the task item.
    pub id: i64,

    /// The stream data source.
    pub from: String,

    /// Use oneshot topic for a task, delete the topic after task deleted.
    #[serde(default)]
    oneshot_topic: Option<String>,

    /// The target of the stream.
    pub to: String,

    /// Number of jobs for task running.
    pub jobs: u16,

    /// Agent Id
    pub via: Option<i64>,

    /// Compression level when need (for backup only)
    pub compression_level: Option<u8>,

    /// Force for some risking steps.
    #[serde(default)]
    pub force: bool,

    /// Created time.
    created_at: DateTime<Utc>,

    /// Stopped time.
    finished_at: Option<DateTime<Utc>>,

    /// Last modified time.
    last_modified_at: Option<DateTime<Utc>>,

    /// The current status of the tasks.
    status: String,

    /// Status reason (only for status: failed).
    reason: Option<String>,

    /// Add after_delete hook action, the string would be action name, with or without some configuration.
    ///
    /// It will do nothing if the action is not supported by a specific task case.
    after_delete: Option<String>,
    /// A task name.
    name: Option<String>,

    /// Task trigger events, default will be oneshot.
    pub trigger: Option<String>,
    // / Labels for a task.
    // /
    // / You can use k-v style label such as `key::value` or key-only label `key`.
    // /
    // / You can filter tasks by some labels.
    // #[serde(deserialize_with = "labels_serde::deserialize")]
    // #[serde(default)]
    // labels: Vec<(String, Option<String>)>,
}

impl Client {
    pub async fn new(endpoint: impl Display, token: impl Display) -> Result<Self> {
        let endpoint = endpoint.to_string();
        let token = token.to_string();
        let channel = Endpoint::try_from(endpoint.clone())
            .map_err(|err| anyhow::format_err!("Unable to create endpoint on `{endpoint}`: {err}"))?
            .connect()
            .await
            .map_err(|err| {
                anyhow::format_err!("Unable to connect with endpoint `{endpoint}`: {err}")
            })?;

        let mut client = FlightClient::new(channel);
        client.add_header("x-token", &token)?;
        let result = client
            .handshake(token.to_string())
            .await
            .with_context(|| anyhow::format_err!("Handshake error with token"))?;
        dbg!(&result);
        let agent: Agent = serde_json::from_slice(&result)?;

        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
            agent,
        })
    }

    pub async fn wait_tasks(&mut self, sender: flume::Sender<Action>) -> Result<()> {
        struct FakeStream(
            SchemaRef,
            tokio::time::Interval,
            Instant,
            Receiver<RespAction>,
        );

        impl futures::Stream for FakeStream {
            type Item = Result<RecordBatch, FlightError>;
            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                info!("polled");
                match self.3.recv_async().poll_unpin(cx) {
                    Poll::Ready(Ok(action)) => match action {
                        RespAction::Heartbeat => {
                            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                                Utc::now().timestamp_millis(),
                            ])) as ArrayRef;
                            let context: ArrayRef =
                                Arc::new(StringArray::from_iter([Option::<String>::None]));
                            let action: ArrayRef =
                                Arc::new(StringArray::from_iter_values(["heartbeat".to_string()]));
                            let item = RecordBatch::try_from_iter(vec![
                                ("ts", val),
                                ("action", action),
                                ("context", context),
                            ])
                            .map_err(Into::into);
                            log::info!("{item:?}");
                            return std::task::Poll::Ready(Some(item));
                        }
                        RespAction::TaskError(_) => (),
                        RespAction::ListOk(sets) => {
                            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                                Utc::now().timestamp_millis(),
                            ])) as ArrayRef;
                            let context: ArrayRef =
                                Arc::new(StringArray::from_iter_values([serde_json::to_string(
                                    &sets,
                                )
                                .unwrap()]));
                            let action: ArrayRef =
                                Arc::new(StringArray::from_iter_values(["list".to_string()]));
                            let item = RecordBatch::try_from_iter(vec![
                                ("ts", val),
                                ("action", action),
                                ("context", context),
                            ])
                            .map_err(Into::into);
                            log::info!("{item:?}");
                            cx.waker().wake_by_ref();
                            return std::task::Poll::Ready(Some(item));
                        }
                    },
                    _ => (),
                    // Poll::Ready(Err(err)) => {
                    //     tracing::error!("Error: {err}");
                    //     cx.waker().wake_by_ref();
                    //     return Poll::Pending;
                    // }
                    // _ => {
                    //     return Poll::Pending;
                    // }
                }

                match self.1.poll_tick(cx) {
                    Poll::Ready(_) => (),
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
        struct Data {
            data: FlightDataEncoder,

            counter: AtomicU64,
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
                                v.app_metadata = Bytes::from(format!(
                                    "{}",
                                    self.counter
                                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                                ));
                                v
                            } else {
                                v
                            }
                        })
                    })
            }
        }

        let schema = Arc::new(
            Schema::new(vec![
                Field::new(
                    "ts",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("action", DataType::Utf8, false),
                Field::new("context", DataType::Utf8, true),
            ])
            .with_metadata(HashMap::from_iter([(
                "x-task-id".to_string(),
                "1".to_string(),
            )])),
        );
        let (resp_tx, resp_rx) = flume::unbounded();
        let data = FlightDataEncoderBuilder::new()
            .with_schema(schema.clone())
            .with_options(
                IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5).unwrap(),
            )
            .build(FakeStream(
                schema.clone(),
                tokio::time::interval(Duration::from_secs(5)),
                Instant::now(),
                resp_rx,
            ));

        let req = Data {
            data,
            counter: AtomicU64::new(1),
        };

        let mut stream = self.client.do_exchange(req).await?;
        // .into_inner();

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

                log::info!("At [{ts}] action `{action}` triggered with: {context}");
                match action {
                    "run" => {
                        let task: Task = serde_json::from_str(&context).unwrap();
                        info!("Start task {:?}", &task);
                        sender.send(Action::Run(task)).unwrap();
                    }
                    "cancel" => {
                        let task: Task = serde_json::from_str(&context).unwrap();
                        info!("Stop task {}", task.id);
                        sender.send(Action::Cancel(task.id)).unwrap();
                        // let task:
                    }
                    "list" => {
                        let req: DataSetsReq = serde_json::from_str(&context).unwrap();
                        let sets = list_datasets_from(&req).await.map_err(Fail::new);
                        let _ = resp_tx.send(RespAction::ListOk(ListResponse { req, res: sets }));
                    }
                    "heartbeat" => {
                        //
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(())
    }
}
