use crate::config::task::PulsarTaskConfig;
use crate::consumer::{build_consumer, build_consumer_with_context};
use crate::poll::poll_message;
use crate::sub_task::SubTask;
use anyhow::Context;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use faststr::FastStr;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use taos::{AsyncTBuilder, Dsn};
use taosx_core::core_metrics::{CoreMetrics, get_metrics_arc_from_i64};
use taosx_core::sink::{channel_based_transformer, ipc_forward};
use taosx_core::utils::trace::BatchCounter;
use taosx_core::{Parser, TaskNotify, TaskNotifySender};
use taosx_ipc::ack::LushAck;
use taosx_ipc::prelude::ArrowDataType;
use tokio::sync::{OwnedSemaphorePermit, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument, warn};

mod config;
pub mod consumer;
mod context;
pub mod decrypt;
mod message_sender;
pub mod pending_ack_fut;
pub mod poll;
pub mod sample;
mod sub_task;
pub mod valid;

pub use sample::get_sample;
pub use valid::is_valid;

pub const PULSAR_ID: &str = "pulsar";
pub const PULSAR_TUYA_ID: &str = "pulsarTuya";

// metrics
const METRIC_TOTAL_PARTITIONS: FastStr = FastStr::from_static_str("pulsar_total_partitions");
const METRIC_CONSUMERS: FastStr = FastStr::from_static_str("pulsar_consumers");
const METRIC_CONSUMED_MESSAGES: FastStr = FastStr::from_static_str("pulsar_consumed_messages");
const METRIC_SEND_MESSAGES: FastStr = FastStr::from_static_str("pulsar_send_msgs");
const METRIC_MSG_ACKS: FastStr = FastStr::from_static_str("pulsar_msg_acks");
const METRIC_SENT_BATCHES: FastStr = FastStr::from_static_str("pulsar_sent_batches");
const METRIC_RECEIVED_BATCHES: FastStr = FastStr::from_static_str("pulsar_received_batches");

// consts
const PENDING_ACK_TIMEOUT: Duration = Duration::from_secs(30);

static BATCH_ID: LazyLock<AtomicU64> = LazyLock::new(AtomicU64::default);

type PulsarJoinSet = JoinSet<anyhow::Result<ExitStatus>>;
type PendingBatches =
    Arc<scc::HashMap<u64, (oneshot::Sender<Vec<PendingState>>, OwnedSemaphorePermit)>>;

pub enum ExitStatus {
    /// Nothing to consume
    None,
    /// Finished
    Finished,
    /// Timeout to poll next message
    Timeout,
    /// Cancelled by upstream or other consumers.
    Aborted,
}

impl ExitStatus {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PendingState {
    topic: String,
    partition: i32,
    ledger_id: u64,
    entry_id: u64,
    batch_size: usize,
}

#[instrument(skip_all)]
pub async fn pulsar_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    upstream_cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    task_id: Option<i64>,
    notify: TaskNotifySender,
) -> anyhow::Result<()> {
    let cancel = upstream_cancel.child_token();
    let _drop_guard = cancel.clone().drop_guard();
    tracing::info!(
        "pulsar_to_taos, detail params task_id: {}, from: {}, parser: {}, to: {}",
        task_id.unwrap_or(-1),
        from,
        serde_json::to_string(&parser)?,
        to
    );
    if with_agent.is_some() {
        let _ = taosx_core::core_metrics::init_task_metrics(
            &from,
            &to,
            task_id.ok_or_else(|| anyhow::anyhow!("No task id with agent runner"))?,
            None,
        )
        .await;
    }
    let metrics_arc = get_metrics_arc_from_i64(task_id).await;

    let parallel = parser
        .as_ref()
        .map(|v| v.global().concurrent_limit())
        .unwrap_or(16);

    let pool = taos::TaosBuilder::from_dsn(&to)
        .context("taos builder from `to` dsn error")?
        .pool()
        .context("get taos pool error")?;

    let (batch_sender, ack_receiver) = match with_agent {
        Some(with_agent) => {
            let (input_tx, input_rx) = flume::bounded(parallel);
            let (ack_tx, ack_rx) = flume::bounded(parallel);
            let schema = Arc::new(build_schema());
            let batch_counter = BatchCounter::new(with_agent.0 as u16).await?;
            let cancel = cancel.clone();
            tokio::spawn(
                async move {
                    if let Err(e) = ipc_forward(
                        input_rx.into_stream(),
                        Some(ack_tx),
                        schema,
                        cancel,
                        with_agent,
                        batch_counter,
                        None,
                        None,
                    )
                    .await
                    {
                        tracing::error!("pulsar ipc forward error: {e}");
                    }
                }
                .in_current_span(),
            );
            (input_tx, ack_rx)
        }
        None => {
            let connector = if from.driver.as_str() == PULSAR_TUYA_ID {
                PULSAR_TUYA_ID
            } else {
                PULSAR_ID
            };
            channel_based_transformer(
                pool,
                &cancel,
                parser,
                Some(connector),
                task_id,
                notify.clone(),
                parallel,
            )
            .await?
        }
    };

    macro_rules! reset_metrics {
        () => {
            metrics_arc
                .ipc()
                .set_extra_metric(&METRIC_TOTAL_PARTITIONS, 0);
            metrics_arc.ipc().set_extra_metric(&METRIC_CONSUMERS, 0);
            metrics_arc
                .ipc()
                .set_extra_metric(&METRIC_CONSUMED_MESSAGES, 0);
            metrics_arc.ipc().set_extra_metric(&METRIC_SEND_MESSAGES, 0);
            metrics_arc.ipc().set_extra_metric(&METRIC_MSG_ACKS, 0);
            metrics_arc.ipc().set_extra_metric(&METRIC_SENT_BATCHES, 0);
            metrics_arc
                .ipc()
                .set_extra_metric(&METRIC_RECEIVED_BATCHES, 0);
        };
    }

    reset_metrics!();
    let aborted_cloned = cancel.clone();
    let mut join_set = match execute(
        from,
        batch_sender,
        ack_receiver,
        aborted_cloned,
        notify.clone(),
        metrics_arc.clone(),
        parallel,
    )
    .in_current_span()
    .await
    {
        Ok(set) => set,
        Err(err) => {
            cancel.cancel();
            reset_metrics!();
            anyhow::bail!("Pulsar subscribe error: {:#}", err);
        }
    };
    tokio::select! {
        // application exit with error code
        status = async {
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(Ok(status)) => {
                        if status.is_timeout() {
                            return Ok(status);
                        }
                    }
                    Ok(Err(err)) => {
                        tracing::error!("Pulsar consumer exit with error: {:#}", err);
                        Err(err).context("Pulsar runners error")?;
                    }
                    Err(err) => {
                        tracing::error!("Pulsar worker exit with error: {:#}", err);
                        anyhow::bail!("Pulsar worker exit with error: {:#}", err);
                    }
                }
            }
            tracing::debug!("Pulsar polling finished");
            Ok(ExitStatus::Finished)
        } => {
            match status {
                Ok(status) => {
                    cancel.cancel();
                    if status.is_timeout() {
                        // wait for completion
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        join_set.abort_all();
                        reset_metrics!();
                        // stop the connector
                        tracing::info!("Pulsar task timeout");
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    join_set.abort_all();
                    reset_metrics!();
                }
                Err(err) => {
                    cancel.cancel();
                    join_set.abort_all();
                    reset_metrics!();
                    anyhow::bail!("Pulsar exit with error: {:#}", err);
                }
            }
        },
        _ = upstream_cancel.cancelled() => {
            tracing::info!("Pulsar task cancelled");
            join_set.abort_all();
            reset_metrics!();
        }
    }
    tracing::info!(task_id = task_id.unwrap_or(-1), "Pulsar task Done");
    // wait for completion
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}

async fn execute(
    from: Dsn,
    batch_sender: flume::Sender<Result<RecordBatch, ArrowError>>,
    ack_receiver: flume::Receiver<LushAck>,
    aborted: CancellationToken,
    notify: TaskNotifySender,
    metrics_arc: Arc<CoreMetrics>,
    parallel: usize,
) -> anyhow::Result<PulsarJoinSet> {
    let mut consumers = JoinSet::new();

    // create pulsar consumers
    let config = PulsarTaskConfig::from_dsn(&from)?;
    let batch_size = config.advanced_options.batch_size.unwrap_or(1000);
    let batch_timeout_ms = config.advanced_options.batch_timeout.unwrap_or(1000) as i64;
    // split into sub tasks
    let sub_tasks = SubTask::build_tasks(&config, &metrics_arc).await?;
    let schema = Arc::new(build_schema());
    let pending_batches: PendingBatches = Arc::new(scc::HashMap::new());
    let global_last_message = Arc::new(RwLock::new(Instant::now()));

    tracing::info!(
        timeout = config.timeout,
        batch.size = batch_size,
        batch.timeout.ms = batch_timeout_ms,
        parallel = parallel,
        subtask.len = sub_tasks.len(),
        "Pulsar consumer configuration"
    );

    let permits = Arc::new(tokio::sync::Semaphore::new(
        parallel.max(sub_tasks.len() * 4),
    ));

    for (idx, task) in sub_tasks.into_iter().enumerate() {
        // let tx = tx.clone();
        let aborted = aborted.clone();
        let schema = schema.clone();
        let notify = notify.clone();

        // multi producer(PulsarConsumer) and single consumer(IPC Writer)
        let (tx, rx) = flume::bounded::<RecordBatch>(parallel);

        let ack_span = tracing::info_span!("pulsar_ack_reader", pulsar.consumer.id = idx);

        if unsafe { taosx_core::global::DRY_RUN } {
            let pending_batches = pending_batches.clone();
            let cancel = aborted.clone();
            consumers.spawn(
                async move {
                    while let Some(Ok(batch)) = cancel.run_until_cancelled(rx.recv_async()).await {
                        let metadata = batch.schema_ref().metadata();
                        let Some(offsets) = metadata
                            .get("offsets")
                            .map(|v| {
                                serde_json::from_str::<Vec<PendingState>>(v)
                                    .context("deserialize offsets error")
                            })
                            .transpose()?
                        else {
                            continue;
                        };
                        let Some(batch_id) = metadata
                            .get("batch_id")
                            .map(|v| v.parse::<u64>().context("desirialize ack batch id error"))
                            .transpose()?
                        else {
                            continue;
                        };
                        if let Some((_, (ack, permit))) =
                            pending_batches.remove_async(&batch_id).await
                        {
                            ack.send(offsets).ok();
                            drop(permit);
                        }
                    }
                    Ok(ExitStatus::Finished)
                }
                .instrument(ack_span),
            );
        } else {
            let ack_receiver = ack_receiver.clone();
            let pending_batches = pending_batches.clone();
            let cancel = aborted.clone();
            let metrics = metrics_arc.clone();
            // receive ACK from IPC
            consumers.spawn( async move {
                while let Some(Ok(ack)) = cancel.run_until_cancelled(ack_receiver.recv_async()).await {

                    if !ack.success() {
                        tracing::error!(ack.code = %ack.code(), ack.message = ack.message(), ack.context = ack.context(), "Pulsar ack found error");
                        if let Some(message) = ack.message() {
                            anyhow::bail!("Pulsar IPC writer error: {message:#}");
                        } else {
                            anyhow::bail!("Pulsar IPC writer error with code: {}", ack.code());
                        }
                    }
                    let Some(metadata) = ack.context().map(|v| serde_json::from_str::<HashMap<String,serde_json::Value>>(v).context("deserialize ack context error")).transpose()? else {
                        continue;
                    };
                    let Some(offsets) = metadata
                        .get("offsets")
                        .map(|v| {
                            let v = v.as_str().context("ack metadata offsets not string")?;
                            serde_json::from_str::<Vec<PendingState>>(v)
                                .context("deserialize offsets error")
                        })
                        .transpose()?
                    else {
                        continue;
                    };
                    let Some(batch_id) = metadata
                        .get("batch_id")
                        .map(|v| {
                            let v = v.as_str().context("ack metadata batch_id not string")?;
                            v.parse::<u64>().context("desirialize ack batch id error")
                        })
                        .transpose()?
                    else {
                        continue;
                    };
                    // ack 也必须和 topic 走了，后面需要整理发送
                    if let Some((_, (ack, permit))) = pending_batches.remove_async(&batch_id).await {
                        ack.send(offsets).ok();
                        drop(permit);
                        metrics.ipc().add_extra_metric(&METRIC_RECEIVED_BATCHES, 1);
                    }
                }
                tracing::info!("Pulsar ACK reader finished");
                Ok(ExitStatus::Finished)
            }.instrument(ack_span));
            // IPC Writer
            let ipc_span = tracing::info_span!("pulsar_ipc_writer", pulsar.consumer.id = idx);
            let batch_sender = batch_sender.clone();
            let cancel = aborted.clone();
            // polling from pulsar and send to ipc writer
            consumers.spawn(
                async move {
                    while let Some(Ok(batch)) = cancel.run_until_cancelled(rx.recv_async()).await {
                        if batch_sender.send_async(Ok(batch)).await.is_err() {
                            break;
                        }
                    }
                    Ok(ExitStatus::Finished)
                }
                .instrument(ipc_span),
            );
        }

        let metrics = metrics_arc.clone();
        let pending_batches = pending_batches.clone();
        let permits = permits.clone();
        let global_last_message = global_last_message.clone();
        consumers.spawn(
            async move {
                let SubTask {
                    id,
                    topic,
                    config,
                    mut consumer,
                    timeout,
                } = task;
                let mut errors = 0;
                let mut last_errors = std::time::Instant::now();
                const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(300);
                const MAX_RETRY_TIMES: usize = 3;
                loop {
                    match poll_message(
                        idx,
                        &mut consumer,
                        &tx,
                        timeout,
                        &aborted,
                        &schema,
                        batch_size,
                        batch_timeout_ms,
                        &notify,
                        config.codec_processor,
                        &pending_batches,
                        &permits,
                        &metrics,
                        &global_last_message,
                    )
                    .in_current_span()
                    .await
                    {
                        Ok(status) => return Ok(status),
                        Err(err) => {
                            if aborted.is_cancelled() {
                                return Ok(ExitStatus::Aborted);
                            }
                            if last_errors.elapsed() >= MAX_RETRY_INTERVAL {
                                errors = 0;
                            }
                            errors += 1;
                            let error = format!("{err:#}");
                            if errors <= MAX_RETRY_TIMES {
                                let context = consumer.context().clone();
                                drop(consumer);
                                context.metrics().sub_extra_metric(&METRIC_CONSUMERS, 1);
                                let joins = context.current_joins();

                                warn!(error, id, "Try to rebuild consumer {idx}");

                                consumer = match Arc::into_inner(context) {
                                    Some(context) => build_consumer_with_context(
                                        config.as_ref(),
                                        &topic,
                                        context,
                                    )
                                    .await
                                    .with_context(|| {
                                        format!("{joins} loop to rebuild consumer {idx} error")
                                    })?,
                                    None => build_consumer(config.as_ref(), &topic, &metrics)
                                        .await
                                        .with_context(|| {
                                            format!("{joins} loop to rebuild consumer {idx} error")
                                        })?,
                                };

                                consumer
                                    .context
                                    .metrics()
                                    .add_extra_metric(&METRIC_CONSUMERS, 1);
                                notify
                                    .send_async(TaskNotify::info(format!(
                                        "Rebuild consumer {idx} with subtask id {id}"
                                    )))
                                    .await
                                    .context("Task logging listener seems closed")?;

                                continue;
                            }
                            last_errors = std::time::Instant::now();
                            warn!(error, "Pulsar consuming error");
                            Err(err)?;
                        }
                    }
                }
            }
            .instrument(tracing::info_span!("consumer", pulsar.consumer.id = idx)),
        );
    }

    Ok(consumers)
}

fn build_schema() -> Schema {
    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("lush"));
    let flat_columns = vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("topic", ArrowDataType::Utf8, false),
        Field::new("partition", ArrowDataType::Int32, false),
        Field::new("ledger_id", ArrowDataType::UInt64, false),
        Field::new("entry_id", ArrowDataType::UInt64, false),
        Field::new("key", ArrowDataType::Binary, true),
        Field::new("value", ArrowDataType::Binary, true),
    ];

    Schema::new(flat_columns).with_metadata(metadata)
}
