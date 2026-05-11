use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use anyhow::Context;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use faststr::FastStr;
use parking_lot::RwLock;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use taos::{AsyncTBuilder, Dsn};
use taosx_core::sink::{channel_based_transformer, ipc_forward};
use taosx_core::utils::trace::BatchCounter;
use tokio::sync::{OwnedSemaphorePermit, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument, warn};

use taosx_ipc::ack::LushAck;
use taosx_ipc::prelude::ArrowDataType;

use taosx_core::core_metrics::{CoreMetrics, get_metrics_arc_from_i64};
use taosx_core::{TaskNotify, TaskNotifySender, TransformConfig, Via};

use crate::config::task::KafkaTaskConfig;
use crate::context::CustomContext;
use crate::poll::poll_message;
use crate::sub_task::SubTask;

mod blocking;
mod config;
mod context;
mod message_sender;
pub mod pending_ack_fut;
pub mod poll;
pub mod sample;
pub mod split_job;
mod sub_task;
pub mod topic_offset;
pub mod valid;

pub use sample::get_sample;
pub use topic_offset::{TopicOffsetInfo, get_topics_offset};
pub use valid::is_valid;

pub const KAFKA_ID: &str = "kafka";

// metrics
const METRIC_CONSUMERS: FastStr = FastStr::from_static_str("kafka_consumers");
const METRIC_TOTAL_PARTITIONS: FastStr = FastStr::from_static_str("kafka_total_partitions");
const METRIC_CONSUMING_PARTITIONS: FastStr = FastStr::from_static_str("kafka_consuming_partitions");
const METRIC_CONSUMED_MESSAGES: FastStr = FastStr::from_static_str("kafka_consumed_messages");
const METRIC_TOTAL_CONSUMED_MESSAGES: FastStr =
    FastStr::from_static_str("total_kafka_consumed_messages");
const METRIC_SENT_BATCHES: FastStr = FastStr::from_static_str("kafka_sent_batches");
const METRIC_RECEIVED_ACKS: FastStr = FastStr::from_static_str("kafka_received_acks");

// consts
const PENDING_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_REBUILD_BACKOFF: Duration = Duration::from_secs(1);
const MAX_REBUILD_BACKOFF: Duration = Duration::from_secs(60);
const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(300);
const DEFAULT_MAX_TOTAL_REBUILDS: usize = 50;

static BATCH_ID: LazyLock<AtomicU64> = LazyLock::new(AtomicU64::default);

type KafkaJoinSet = JoinSet<anyhow::Result<ExitStatus>>;
type PendingBatches =
    Arc<scc::HashMap<u64, (oneshot::Sender<Vec<PendingState>>, OwnedSemaphorePermit)>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WritePressureSnapshot {
    pub write_blocked: bool,
    pub last_permit_wait: Duration,
    pub last_ack_wait: Duration,
}

#[derive(Debug)]
pub(crate) struct WritePressureState {
    hold_window: Duration,
    permit_blocked_until: Option<Instant>,
    ack_blocked_until: Option<Instant>,
    last_permit_wait: Duration,
    last_ack_wait: Duration,
}

impl WritePressureState {
    pub(crate) fn new(hold_window: Duration) -> Self {
        Self {
            hold_window,
            permit_blocked_until: None,
            ack_blocked_until: None,
            last_permit_wait: Duration::ZERO,
            last_ack_wait: Duration::ZERO,
        }
    }

    pub(crate) fn record_permit_wait(&mut self, elapsed: Duration) {
        self.last_permit_wait = elapsed;
        self.permit_blocked_until =
            (elapsed >= self.hold_window).then_some(Instant::now() + self.hold_window);
    }

    pub(crate) fn record_ack_wait(&mut self, elapsed: Duration) {
        self.last_ack_wait = elapsed;
        self.ack_blocked_until =
            (elapsed >= self.hold_window).then_some(Instant::now() + self.hold_window);
    }

    pub(crate) fn snapshot(&self) -> WritePressureSnapshot {
        let now = Instant::now();
        WritePressureSnapshot {
            write_blocked: self
                .permit_blocked_until
                .is_some_and(|deadline| deadline > now)
                || self
                    .ack_blocked_until
                    .is_some_and(|deadline| deadline > now),
            last_permit_wait: self.last_permit_wait,
            last_ack_wait: self.last_ack_wait,
        }
    }
}

#[cfg(test)]
impl WritePressureState {
    fn clear(&mut self) {
        self.permit_blocked_until = None;
        self.ack_blocked_until = None;
        self.last_permit_wait = Duration::ZERO;
        self.last_ack_wait = Duration::ZERO;
    }
}

fn resolve_max_total_rebuilds(configured: Option<String>) -> usize {
    configured
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_MAX_TOTAL_REBUILDS)
}

fn max_total_rebuilds() -> usize {
    resolve_max_total_rebuilds(std::env::var("TAOSX_KAFKA_MAX_REBUILDS").ok())
}

fn compute_rebuild_backoff(errors: usize) -> Duration {
    let shift = errors.saturating_sub(1).min(16) as u32;
    MIN_REBUILD_BACKOFF
        .checked_mul(1u32 << shift)
        .unwrap_or(MAX_REBUILD_BACKOFF)
        .min(MAX_REBUILD_BACKOFF)
}

fn next_rebuild_backoff(
    errors: &mut usize,
    total_rebuilds: &mut usize,
    last_errors: &mut Instant,
    rebuild_cap: usize,
) -> Option<Duration> {
    // If the consumer ran cleanly for at least MAX_RETRY_INTERVAL, treat the
    // previous incident as fully recovered for backoff purposes by resetting
    // only the consecutive-error counter. The lifetime rebuild cap remains
    // monotonic so spaced failures still hit the global safety limit.
    if last_errors.elapsed() >= MAX_RETRY_INTERVAL {
        *errors = 0;
    }

    *total_rebuilds += 1;
    if *total_rebuilds > rebuild_cap {
        return None;
    }

    *errors += 1;
    *last_errors = Instant::now();
    Some(compute_rebuild_backoff(*errors))
}

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
    offset: i64,
}

#[instrument(skip_all)]
pub async fn kafka_to_taos(
    from: Dsn,
    mut parser: Option<TransformConfig>,
    to: Dsn,
    upstream_cancel: CancellationToken,
    with_agent: Option<Via>,
    task_job_id: Option<(i64, i64)>,
    notify: TaskNotifySender,
) -> anyhow::Result<()> {
    let cancel = upstream_cancel.child_token();
    let _drop_guard = cancel.clone().drop_guard();
    let (task_id, job_id) = task_job_id.unwrap_or((-1, -1));
    tracing::info!(
        "Kafka task: ({task_id},{job_id}) start, from: {from}, parser: {}, to: {to}",
        serde_json::to_string(&parser)?,
    );
    if let Some(via) = &with_agent {
        let _ =
            taosx_core::core_metrics::init_task_metrics(&from, &to, via.task_id, via.job_id, None)
                .await;
    }
    let metrics_arc = get_metrics_arc_from_i64(task_job_id);
    if let Some(parser) = parser.as_mut() {
        parser.set_metrics(metrics_arc.clone());
    }

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
            let batch_counter = BatchCounter::new(with_agent.task_id, with_agent.job_id).await?;
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
                        tracing::error!("kafka ipc forward error: {e}");
                    }
                }
                .in_current_span(),
            );
            (input_tx, ack_rx)
        }
        None => {
            channel_based_transformer(
                pool,
                &cancel,
                parser,
                Some(KAFKA_ID),
                Some((task_id, job_id)),
                notify.clone(),
                parallel,
            )
            .await?
        }
    };

    macro_rules! reset_metrics {
        () => {
            metrics_arc.ipc().set_extra_metric(&METRIC_CONSUMERS, 0);
            metrics_arc
                .ipc()
                .set_extra_metric(&METRIC_CONSUMING_PARTITIONS, 0);
            metrics_arc.ipc().set_extra_metric(&METRIC_SENT_BATCHES, 0);
            metrics_arc.ipc().set_extra_metric(&METRIC_RECEIVED_ACKS, 0);
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
            anyhow::bail!("Kafka subscribe error: {:#}", err);
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
                        tracing::error!("Kafka consumer exit with error: {:#}", err);
                        Err(err).context("Kafka runners error")?;
                    }
                    Err(err) => {
                        tracing::error!("Kafka worker exit with error: {:#}", err);
                        anyhow::bail!("Kafka worker exit with error: {:#}", err);
                    }
                }
            }
            tracing::debug!("Kafka polling finished");
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
                        tracing::info!("Kafka task timeout");
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
                    anyhow::bail!("Kafka exit with error: {:#}", err);
                }
            }
        },
        _ = upstream_cancel.cancelled() => {
            tracing::info!("Kafka task cancelled");
            join_set.abort_all();
            reset_metrics!();
        }
    }
    // send an empty tuple
    // stop the connector
    tracing::info!(task.id = task_id, job.id = job_id, "Kafka task Done");
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
) -> anyhow::Result<KafkaJoinSet> {
    let mut consumers = JoinSet::new();

    // kafka task config
    let config = KafkaTaskConfig::from_dsn(&from)?;

    let batch_size = config.advanced_options.batch_size.unwrap_or(1000);
    let batch_timeout_ms = config.advanced_options.batch_timeout.unwrap_or(1000) as i64;
    tracing::info!(
        timeout = config.timeout,
        batch.size = batch_size,
        batch.timeout.ms = batch_timeout_ms,
        "Kafka consumer configuration"
    );

    let permits = Arc::new(tokio::sync::Semaphore::new(parallel));

    // split into sub tasks
    let sub_tasks = SubTask::build_tasks(config, &metrics_arc).await?;

    let schema = Arc::new(build_schema());

    let pending_batches: PendingBatches = Arc::new(scc::HashMap::new());

    let global_last_message = Arc::new(RwLock::new(Instant::now()));

    for (idx, task) in sub_tasks.into_iter().enumerate() {
        // let tx = tx.clone();
        let aborted = aborted.clone();
        let schema = schema.clone();
        let notify = notify.clone();

        // multi producer(KafkaConsumer) and single consumer(IPC Writer)
        let (tx, rx) = flume::bounded::<RecordBatch>(parallel);

        let ack_span = tracing::info_span!("kafka_ack_reader", kafka.consumer.id = idx);

        if taosx_core::global::dry_run() {
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
                        tracing::error!(ack.code = %ack.code(), ack.message = ack.message(), ack.context = ack.context(), "Kafka ack found error");
                        if let Some(message) = ack.message() {
                            anyhow::bail!("Kafka IPC writer error: {message:#}");
                        } else {
                            anyhow::bail!("Kafka IPC writer error with code: {}", ack.code());
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
                    if let Some((_, (ack, permit))) = pending_batches.remove_async(&batch_id).await {
                        ack.send(offsets).ok();
                        drop(permit);
                        metrics.ipc().add_extra_metric(&METRIC_RECEIVED_ACKS, 1);
                    }
                }
                pending_batches.clear_async().await;
                tracing::info!("Kafka ACK reader finished");
                Ok(ExitStatus::Finished)
            }.instrument(ack_span));
            // IPC Writer
            let ipc_span = tracing::info_span!("kafka_ipc_writer", kafka.consumer.id = idx);
            let batch_sender = batch_sender.clone();
            let cancel = aborted.clone();
            // polling from kafka and send to ipc writer
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
                    topics,
                    config,
                    mut instance,
                    mut consumer,
                    timeout,
                } = task;
                let mut errors = 0;
                let mut total_rebuilds = 0usize;
                let mut last_errors = std::time::Instant::now();
                let rebuild_cap = max_total_rebuilds();
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
                    .await
                    {
                        Ok(status) => return Ok(status),
                        Err(err) => {
                            if aborted.is_cancelled() {
                                return Ok(ExitStatus::Aborted);
                            }
                            let Some(rebuild_backoff) = next_rebuild_backoff(
                                &mut errors,
                                &mut total_rebuilds,
                                &mut last_errors,
                                rebuild_cap,
                            ) else {
                                anyhow::bail!(
                                    "consumer {idx} exceeded max rebuild count {rebuild_cap}: {err:#}"
                                );
                            };
                            tokio::select! {
                                _ = aborted.cancelled() => return Ok(ExitStatus::Aborted),
                                _ = tokio::time::sleep(rebuild_backoff) => {}
                            }

                            let error = format!("{err:#}");
                            let context = consumer.context().clone();
                            drop(consumer);
                            context.metrics().sub_extra_metric(&METRIC_CONSUMERS, 1);
                            let joins = context.current_joins();

                            if instance.is_some() && error.contains("FencedInstanceId") {
                                instance = Some(format!("{idx}-{}", uuid::Uuid::new_v4()));
                            }
                            warn!(error, instance, "Try to rebuild consumer {idx}");

                            consumer = match Arc::into_inner(context) {
                                Some(context) => config
                                    .build_consumer_with_context(
                                        instance.as_deref(),
                                        &topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                                        context,
                                    )
                                    .await
                                    .with_context(|| {
                                        format!("{joins} loop to rebuild consumer {idx} error")
                                    })?,
                                None => config
                                    .build_consumer(
                                        instance.as_deref(),
                                        &topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                                        &metrics,
                                    )
                                    .await
                                    .with_context(|| {
                                        format!("{joins} loop to rebuild consumer {idx} error")
                                    })?,
                            };

                            notify
                                .send_async(TaskNotify::info(instance.as_deref().map_or_else(
                                    || format!("Rebuild consumer {idx}"),
                                    |instance| {
                                        format!(
                                            "Rebuild consumer {idx} with instance id {instance}"
                                        )
                                    },
                                )))
                                .await
                                .context("Task logging listener seems closed")?;
                            continue;
                        }
                    }
                }
            }
            .instrument(tracing::info_span!("consumer", kafka.consumer.id = idx)),
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
            "__ts__",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("topic", ArrowDataType::Utf8, false),
        Field::new("partition", ArrowDataType::Int32, false),
        Field::new("offset", ArrowDataType::Int64, false),
        Field::new("key", ArrowDataType::Binary, true),
        Field::new("value", ArrowDataType::Binary, true),
    ];

    Schema::new(flat_columns).with_metadata(metadata)
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

#[cfg(test)]
mod tests {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::BaseConsumer;
    use std::env;
    use std::str::FromStr;
    use taos::IntoDsn;

    use crate::blocking::fetch_metadata;
    use crate::config::connect::KafkaConnectConfig;
    use crate::config::task::build_client_config;
    use crate::topic_offset::get_topics_offset;
    use crate::valid::is_valid;

    use super::*;

    #[tokio::test]
    async fn run_blocking_returns_closure_result() {
        let value = crate::blocking::run_blocking("unit test", || Ok::<_, anyhow::Error>(7))
            .await
            .expect("blocking helper should return closure result");
        assert_eq!(7, value);
    }

    #[test]
    fn write_pressure_state_reports_blocked_while_pressure_is_recent() {
        let mut state = WritePressureState::new(Duration::from_secs(5));
        assert!(!state.snapshot().write_blocked);

        state.record_permit_wait(Duration::from_secs(6));
        let snapshot = state.snapshot();
        assert!(snapshot.write_blocked);
        assert_eq!(Duration::from_secs(6), snapshot.last_permit_wait);

        state.clear();
        assert!(!state.snapshot().write_blocked);
    }

    /// Example:
    /// ```shell
    /// KAFKA_DSN="kafka://192.168.1.45:9092?topics=zyyang&group=test" cargo nextest run -p taosx-core test_get_topics_offset --no-capture --retries 0
    /// ```
    #[tokio::test]
    async fn test_get_topics_offset() {
        if let Ok(kafka_dsn) = env::var("KAFKA_DSN") {
            let dsn = kafka_dsn.into_dsn().expect("Always valid");
            let offsets = get_topics_offset(None, &dsn)
                .await
                .expect("Get topics offset should success");
            dbg!(offsets);
        }
    }

    #[tokio::test]
    async fn test_is_valid() {
        let dsn = Dsn::from_str("kafka://127.0.0.1:9092").expect("DSN parse should be success");
        let result = is_valid(&dsn).await;
        assert!(!result.valid);
        assert!(!result.support);
        assert_eq!(KAFKA_ID, result.data_source);
        assert_eq!(
            "invalid dsn: kafka://127.0.0.1:9092, cause: topics is required",
            result.message.expect("message should exists")
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_use_ssl() {
        let dsn = format!(
            "kafka://{}?ca={}&ca_password=abcdefgh&cert={}&cert_key={}",
            "192.168.2.19:9093",
            "@../tests/kafka/ca-cert",
            "@../tests/kafka/client_test_client.pem",
            "@../tests/kafka/client_test_client.key",
        )
        .into_dsn()
        .expect("SSL DSN should be valid");

        let config = KafkaConnectConfig::from_dsn(&dsn).expect("Config should success in test");
        let client_config: ClientConfig = build_client_config(config.clone())
            .await
            .expect("Client config should success in test");
        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|err| anyhow::anyhow!("failed to create consumer, cause: {:#}", err))
            .expect("Consumer should created successfully in test");
        // fetch metadata
        let (_, metadata_result) = fetch_metadata(consumer, None, Duration::from_secs(5))
            .await
            .expect("Metadata fetch helper should succeed in test");
        let metadata = metadata_result
            .map_err(|err| anyhow::anyhow!("failed to load meta data, cause: {:#}", err))
            .expect("Metadata should be fetched successfully in test");

        dbg!(metadata.topics().len());
    }

    #[tokio::test]
    #[ignore]
    async fn test_use_sasl() {
        let dsn = format!(
            "kafka://{}?sasl_mechanism={}&sasl_username={}&sasl_password={}",
            "192.168.2.19:9094", "PLAIN", "nick", "nick-sec",
        )
        .into_dsn()
        .expect("DSN should be valid");

        let config = KafkaConnectConfig::from_dsn(&dsn).expect("Config should success in test");
        let client_config: ClientConfig = build_client_config(config.clone())
            .await
            .expect("Client config should success in test");
        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|err| anyhow::anyhow!("failed to create consumer, cause: {:#}", err))
            .expect("Consumer should created successfully in test");
        // fetch metadata
        let (_, metadata_result) = fetch_metadata(consumer, None, Duration::from_secs(5))
            .await
            .expect("Metadata fetch helper should succeed in test");
        let metadata = metadata_result
            .map_err(|err| anyhow::anyhow!("failed to load meta data, cause: {:#}", err))
            .expect("Metadata should be fetched successfully in test");

        dbg!(metadata.topics().len());
        // filter topics
        let topics = [String::from("test_taosx_sasl")];
        let topics_readable = metadata
            .topics()
            .iter()
            .filter(|tp| {
                println!("{}", tp.name());
                !tp.name().starts_with("__")
            })
            .filter(|tp| topics.contains(&tp.name().to_string()))
            .collect::<Vec<_>>();
        dbg!(topics_readable.len());
    }
}

#[cfg(test)]
#[test]
fn rebuild_backoff_grows_exponentially_until_cap() {
    assert_eq!(compute_rebuild_backoff(1), Duration::from_secs(1));
    assert_eq!(compute_rebuild_backoff(2), Duration::from_secs(2));
    assert_eq!(compute_rebuild_backoff(3), Duration::from_secs(4));
    assert_eq!(compute_rebuild_backoff(10), Duration::from_secs(60));
}

#[cfg(test)]
static ENV_VAR_LOCK: LazyLock<std::sync::Mutex<()>> = LazyLock::new(|| std::sync::Mutex::new(()));

#[cfg(test)]
struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl EnvVarGuard {
    fn unset(key: &'static str) -> Self {
        let lock = ENV_VAR_LOCK.lock().expect("env var test lock poisoned");
        let previous = std::env::var(key).ok();
        unsafe { std::env::remove_var(key) };
        Self {
            key,
            previous,
            _lock: lock,
        }
    }
}

#[cfg(test)]
impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.previous.as_deref() {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

#[cfg(test)]
#[test]
fn rebuild_cap_uses_default_when_env_missing() {
    let _guard = EnvVarGuard::unset("TAOSX_KAFKA_MAX_REBUILDS");
    assert_eq!(max_total_rebuilds(), DEFAULT_MAX_TOTAL_REBUILDS);
}

#[cfg(test)]
#[test]
fn rebuild_stops_after_max_rebuilds() {
    let mut errors = 0;
    let mut total_rebuilds = 0;
    let mut last_errors = Instant::now();

    for _ in 0..DEFAULT_MAX_TOTAL_REBUILDS {
        assert!(
            next_rebuild_backoff(
                &mut errors,
                &mut total_rebuilds,
                &mut last_errors,
                DEFAULT_MAX_TOTAL_REBUILDS,
            )
            .is_some()
        );
    }

    assert!(
        next_rebuild_backoff(
            &mut errors,
            &mut total_rebuilds,
            &mut last_errors,
            DEFAULT_MAX_TOTAL_REBUILDS,
        )
        .is_none()
    );
}

#[cfg(test)]
#[test]
fn rebuild_cap_stays_monotonic_after_stable_period() {
    let mut errors = 0;
    let mut total_rebuilds = 0;
    let mut last_errors = Instant::now();

    for _ in 0..DEFAULT_MAX_TOTAL_REBUILDS {
        assert!(
            next_rebuild_backoff(
                &mut errors,
                &mut total_rebuilds,
                &mut last_errors,
                DEFAULT_MAX_TOTAL_REBUILDS,
            )
            .is_some()
        );
    }

    // Simulate the consumer running cleanly for longer than MAX_RETRY_INTERVAL.
    last_errors = Instant::now()
        .checked_sub(MAX_RETRY_INTERVAL + Duration::from_secs(1))
        .expect("Instant subtraction must succeed");

    // After a stable period, the lifetime rebuild cap should still apply.
    assert!(
        next_rebuild_backoff(
            &mut errors,
            &mut total_rebuilds,
            &mut last_errors,
            DEFAULT_MAX_TOTAL_REBUILDS,
        )
        .is_none()
    );
    assert_eq!(total_rebuilds, DEFAULT_MAX_TOTAL_REBUILDS + 1);
    assert_eq!(errors, 0);
}
