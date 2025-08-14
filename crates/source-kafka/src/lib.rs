use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::Context;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_schema::ArrowError;
use faststr::FastStr;
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
use taosx_core::{Parser, TaskNotify, TaskNotifySender};

use crate::config::task::KafkaTaskConfig;
use crate::context::CustomContext;
use crate::poll::poll_message;
use crate::sub_task::SubTask;

mod config;
mod context;
mod message_sender;
pub mod pending_ack_fut;
pub mod poll;
pub mod sample;
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
const FETCH_METADATA_TIMEOUT: Duration = Duration::from_secs(30);

static BATCH_ID: LazyLock<AtomicU64> = LazyLock::new(AtomicU64::default);

type KafkaJoinSet = JoinSet<anyhow::Result<ExitStatus>>;
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
    offset: i64,
}

#[instrument(skip_all)]
pub async fn kafka_to_taos(
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
        "Kafka task: {} start, from: {}, parser: {}, to: {}",
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
                        tracing::error!("kafka ipc forword error: {e}");
                    }
                }
                .in_current_span(),
            );
            (input_tx, ack_rx)
        }
        None => {
            channel_based_transformer(
                pool,
                cancel.child_token(),
                parser,
                Some(KAFKA_ID),
                task_id,
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
    tracing::info!(task_id = task_id.unwrap_or(-1), "Kafka task Done");
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

    for (idx, task) in sub_tasks.into_iter().enumerate() {
        // let tx = tx.clone();
        let aborted = aborted.clone();
        let schema = schema.clone();
        let notify = notify.clone();

        // multi producer(KafkaConsumer) and single consumer(IPC Writer)
        let (tx, rx) = flume::bounded::<RecordBatch>(parallel);

        let ack_span = tracing::info_span!("kafka_ack_reader", kafka.consumer.id = idx);
        let ack_num = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ack_num_clone = ack_num.clone();

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
                            .map(|v| v.parse::<u64>())
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
                    ack_num_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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
                        pending_batches.clone(),
                        permits.clone(),
                        metrics.clone(),
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

                                if instance.is_some() && error.contains("FencedInstanceId") {
                                    instance = Some(format!("{idx}-{}", uuid::Uuid::new_v4()));
                                }
                                warn!(error, instance, "Try to rebuild consumer {idx}");

                                consumer = match Arc::into_inner(context) {
                                    Some(context) => config
                                        .build_consumer_with_context(
                                            instance.as_deref(),
                                            &topics
                                                .iter()
                                                .map(|s| s.as_str())
                                                .collect::<Vec<&str>>(),
                                            context,
                                        )
                                        .await
                                        .with_context(|| {
                                            format!("{joins} loop to rebuild consumer {idx} error")
                                        })?,
                                    None => config
                                        .build_consumer(
                                            instance.as_deref(),
                                            &topics
                                                .iter()
                                                .map(|s| s.as_str())
                                                .collect::<Vec<&str>>(),
                                            &metrics,
                                        )
                                        .await
                                        .with_context(|| {
                                            format!("{joins} loop to rebuild consumer {idx} error")
                                        })?,
                                };

                                notify
                                    .send(TaskNotify::info(instance.as_deref().map_or_else(
                                        || format!("Rebuild consumer {idx}"),
                                        |instance| {
                                            format!(
                                                "Rebuild consumer {idx} with instance id {instance}"
                                            )
                                        },
                                    )))
                                    .context("Task logging listener seems closed")?;
                                continue;
                            }
                            last_errors = std::time::Instant::now();
                            warn!(error, "Kafka consuming error");
                            Err(err)?;
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
            "ts",
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

    use crate::config::connect::KafkaConnectConfig;
    use crate::config::task::build_client_config;
    use crate::topic_offset::get_topics_offset;
    use crate::valid::is_valid;

    use super::*;

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
        let client_config: ClientConfig =
            build_client_config(config.clone()).expect("Client config should success in test");
        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|err| anyhow::anyhow!("failed to create consumer, cause: {:#}", err))
            .expect("Consumer should created successfully in test");
        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
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
        let client_config: ClientConfig =
            build_client_config(config.clone()).expect("Client config should success in test");
        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|err| anyhow::anyhow!("failed to create consumer, cause: {:#}", err))
            .expect("Consumer should created successfully in test");
        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
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
