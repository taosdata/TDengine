use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use arrow::array::{
    ArrayBuilder, BinaryBuilder, Int32Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use chrono::Utc;
use faststr::FastStr;
use futures_ext::TryReadyChunksError;
use futures_util::StreamExt;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::Offset;
use scc::HashIndex;
use serde_json::json;
use taos::Dsn;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, instrument, warn, Instrument};

use taosx_ipc::ack::{AckReaderBuilder, LushAck};
use taosx_ipc::prelude::ArrowDataType;

use crate::core_metrics::{get_metrics_arc_from_i64, CoreMetrics};
use crate::plugins::dsv::DataSourceValidation;
use crate::plugins::transform::sample::DsSampleIn;
use crate::runners::kafka::config::connect::KafkaConnectConfig;
use crate::runners::kafka::config::KafkaTaskConfig;
use crate::runners::set_tcp_keepalive;
use crate::sink::ipc_metric::IpcMetrics;
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

mod config;

pub const KAFKA_ID: &'static str = "kafka";
const FETCH_METADATA_TIMEOUT: Duration = Duration::from_secs(30);
const METRIC_CONSUMERS: FastStr = FastStr::from_static_str("kafka_consumers");
const METRIC_TOTAL_PARTITIONS: FastStr = FastStr::from_static_str("kafka_total_partitions");
const METRIC_CONSUMING_PARTITIONS: FastStr = FastStr::from_static_str("kafka_consuming_partitions");
const METRIC_CONSUMED_MESSAGES: FastStr = FastStr::from_static_str("kafka_consumed_messages");
const METRIC_TOTAL_CONSUMED_MESSAGES: FastStr =
    FastStr::from_static_str("total_kafka_consumed_messages");

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_valid_impl(dsn) {
        Ok(()) => DataSourceValidation::valid(KAFKA_ID.to_string(), None),
        Err(err) => DataSourceValidation::invalid(KAFKA_ID.to_string(), format!("{err:#}")),
    }
}

fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
    let config = KafkaTaskConfig::from_dsn(dsn)
        .map_err(|err| anyhow::anyhow!("invalid dsn: {}, cause: {:#}", dsn, err))?;

    let client_config = build_client_config(config.connect)?;
    let consumer: BaseConsumer = client_config
        .create()
        .map_err(|err| anyhow::anyhow!("failed to create client, cause: {:#}", err))?;

    let metadata = consumer
        .fetch_metadata(None, FETCH_METADATA_TIMEOUT)
        .context("failed to load meta data while checking kafka data source")?;

    tracing::info!(
        brokers = metadata
            .brokers()
            .iter()
            .map(|b| format!("{}={}:{}", b.id(), b.host(), b.port()))
            .join(","),
        broker.id = metadata.orig_broker_id(),
        broker.name = metadata.orig_broker_name(),
        "kafka metadata"
    );
    Ok(())
}

pub async fn get_sample(dsn: &Dsn, limit: usize, timeout: Duration) -> anyhow::Result<DsSampleIn> {
    let sample_list: Vec<String> = get_sample_impl(dsn, limit, timeout).await?;

    let mut sample_vec: Vec<LinkedHashMap<String, serde_json::Value>> = Vec::new();
    for payload in sample_list {
        let mut p = LinkedHashMap::new();
        p.insert("payload".to_string(), json!(payload));
        sample_vec.push(p);
    }

    let sample_json = json!({
        "input": sample_vec,
        "parser": {}
    });

    let sample: DsSampleIn = serde_json::from_value(sample_json.clone()).map_err(|err| {
        anyhow::anyhow!(
            "failed to parse kafka sample data: {:?}, cause: {:?}",
            sample_json,
            err
        )
    })?;

    Ok(sample)
}

async fn get_sample_impl(
    dsn: &Dsn,
    limit: usize,
    timeout: Duration,
) -> anyhow::Result<Vec<String>> {
    let connect_config = KafkaConnectConfig::from_dsn(dsn)?;
    let fallback_offset = KafkaTaskConfig::parse_fallback_offset(dsn)?;

    // create consumer
    let mut client_config = build_client_config(connect_config).unwrap();
    let consumer: BaseConsumer = client_config
        .set("group.id", "test")
        .set("auto.offset.reset", &fallback_offset)
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|err| anyhow::anyhow!("failed to create client, cause: {:#}", err))?;

    // subscribe topics
    let topics = KafkaTaskConfig::parse_topics(dsn)?;
    let topics = topics.iter().map(|p| p.as_str()).collect::<Vec<&str>>();
    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");

    let _ = tracing_all_topics(topics, &consumer);

    // assign offset to the beginning or end
    let mut tp_list = consumer.assignment().unwrap();
    match fallback_offset.as_str() {
        "smallest" | "earliest" | "beginning" => {
            tp_list
                .set_all_offsets(Offset::Beginning)
                .expect("failed to set offset");
        }
        "largest" | "latest" | "end" => {
            tp_list
                .set_all_offsets(Offset::End)
                .expect("failed to set offset");
        }
        _ => {
            // nothing to do
        }
    };
    consumer.assign(&tp_list).unwrap();

    // polling message from kafka
    let start = Utc::now().timestamp();
    let mut count = 0;
    let mut payload_list: Vec<String> = Vec::new();
    loop {
        let message = consumer.poll(Duration::from_secs(1));
        if let Some(msg) = message {
            match msg {
                Ok(m) => {
                    m.payload().map(|p| {
                        payload_list.push(String::from_utf8_lossy(p).to_string());
                    });
                }
                Err(err) => {
                    tracing::error!("Kafka polling error: {:#}", err);
                    anyhow::bail!("Kafka polling error: {:#}", err);
                }
            }
            count += 1;
        }
        let now = Utc::now().timestamp();
        if now - start > timeout.as_secs() as i64 || count >= limit {
            break;
        }
    }

    Ok(payload_list)
}

fn tracing_all_topics(topics: Vec<&str>, consumer: &BaseConsumer) -> anyhow::Result<()> {
    for topic in topics {
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(1))
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to load meta data for topic: {}, cause: {:#}",
                    topic,
                    err
                )
            })?;

        for topic_meta in metadata.topics() {
            for partition in topic_meta.partitions() {
                let (low, high) = consumer
                    .fetch_watermarks(topic_meta.name(), partition.id(), Duration::from_secs(1))
                    .map_err(|err| {
                        anyhow::anyhow!(
                            "failed to fetch watermarks for topic: {}, partition: {}, cause: {:#}",
                            topic_meta.name(),
                            partition.id(),
                            err
                        )
                    })?;
                tracing::info!(
                    "topic: {}, partition: {}, low: {}, high: {}",
                    topic_meta.name(),
                    partition.id(),
                    low,
                    high
                );
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
pub async fn kafka_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    _transform: Vec<Action>,
    to: Dsn,
    _jobs: usize,
    port_pool: &PortPool,
    upstream_cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
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
        let _ = crate::core_metrics::init_task_metrics(
            &from,
            &to,
            task_id.ok_or_else(|| anyhow::anyhow!("No task id with agent runner"))?,
            None,
        )
        .await;
    }
    let metrics_arc = get_metrics_arc_from_i64(task_id).await;

    let ipc_port = port_pool
        .get()
        .await
        .ok_or_else(|| anyhow::format_err!("No available port for Kafka connection"))?;
    let socket = format!("127.0.0.1:{}", ipc_port);
    let mut ipc = build_ipc(
        &socket,
        parser,
        &to,
        Some(KAFKA_ID),
        None,
        None,
        &cancel,
        with_agent,
        transferred,
        task_id.clone(),
        notify.clone(),
    )
    .await?;

    macro_rules! reset_metrics {
        () => {
            metrics_arc.ipc().set_extra_metric(&METRIC_CONSUMERS, 0);
            metrics_arc
                .ipc()
                .set_extra_metric(&METRIC_CONSUMING_PARTITIONS, 0);
        };
    }

    reset_metrics!();
    let aborted_cloned = cancel.clone();
    let mut join_set = match execute(
        from,
        ipc_port.get(),
        aborted_cloned,
        notify.clone(),
        metrics_arc.clone(),
    )
    .in_current_span()
    .await
    {
        Ok(set) => set,
        Err(err) => {
            cancel.cancel();
            reset_metrics!();
            let _ = ipc.send(());
            let _ = ipc.close().await;
            anyhow::bail!("Kafka subscribe error: {:#}", err);
        }
    };
    tokio::spawn(async move {
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
                            ipc.close().await?;
                            return Ok(());
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        join_set.abort_all();
                        reset_metrics!();
                        match ipc.try_recv_error() {
                            Ok(res) => {
                                tracing::error!("IPC Error: {res}");
                                anyhow::bail!("Kafka worker exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("Kafka worker done successfully");
                            }
                        }
                    }
                    Err(err) => {
                        cancel.cancel();
                        join_set.abort_all();
                        reset_metrics!();
                        let _ = ipc.send(());
                        anyhow::bail!("Kafka exit with error: {:#}", err);
                    }
                }
            },
            err = ipc.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                cancel.cancel();
                join_set.abort_all();
                reset_metrics!();
                if let Some(err) = err {
                    let _ = ipc.send(()).await;
                    let _ = ipc.close().await;
                    anyhow::bail!("Kafka writer error: {err:#}");
                }
            },
            _ = upstream_cancel.cancelled() => {
                tracing::info!("Kafka task cancelled");
                join_set.abort_all();
                reset_metrics!();
            }
        }
        // send an empty tuple
        let _ = ipc.send(()).await;
        // stop the connector
        tracing::info!("Kafka task Done");
        ipc.close().await?;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }.in_current_span()).await??;

    tracing::info!("Kafka task: {} stopped", task_id.unwrap_or(-1));
    Ok(())
}

type KafkaJoinSet = JoinSet<anyhow::Result<ExitStatus>>;

async fn execute(
    from: Dsn,
    ipc_server_port: u16,
    aborted: CancellationToken,
    notify: crate::TaskNotifySender,
    metrics_arc: Arc<CoreMetrics>,
) -> anyhow::Result<KafkaJoinSet> {
    let ipc_server = format!("127.0.0.1:{}", ipc_server_port);

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

    // split into sub tasks
    let sub_tasks = SubTask::build_tasks(config, &notify, &metrics_arc)?;

    let schema = Arc::new(build_schema());

    for (idx, task) in sub_tasks.into_iter().enumerate() {
        // let tx = tx.clone();
        let aborted = aborted.clone();
        let schema = schema.clone();
        let notify = notify.clone();

        // ipc writer stream
        let stream = std::net::TcpStream::connect(ipc_server.as_str())?;
        set_tcp_keepalive(&stream)?;
        stream.set_read_timeout(None)?;

        // ack reader stream
        let ack_stream = stream.try_clone()?;
        set_tcp_keepalive(&ack_stream)?;
        ack_stream.set_read_timeout(None)?;

        // multi producer(KafkaConsumer) and single consumer(IPC Writer)
        let (tx, rx) =
            flume::bounded(std::thread::available_parallelism().map_or_else(|_| 8, |n| n.get()));
        let max_wait_ack = 10;

        let ack_span = tracing::info_span!("kafka_ack_reader", kafka.consumer.id = idx);
        let ack_num = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ack_num_clone = ack_num.clone();

        let (ack_tx, ack_rx) = flume::bounded(0);

        // receive ACK from IPC
        consumers.spawn_blocking(move || {
            let _entered = ack_span.entered();
            let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
            for ack in ack_reader {
                ack_num_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                if !ack.success() {
                    tracing::error!(ack.code = %ack.code(), ack.message = ack.message(), ack.context = ack.context(), "Kafka ack found error");
                    if let Some(message) = ack.message() {
                        anyhow::bail!("Kafka IPC writer error: {message}");
                    } else {
                        anyhow::bail!("Kafka IPC writer error with code: {}", ack.code());
                    }
                }
                ack_tx.send(ack).unwrap();
            }
            tracing::info!("Kafka ACK reader finished");
            Ok(ExitStatus::Finished)
        });
        // IPC Writer
        let schema_clone = schema.clone();
        let ipc_span = tracing::info_span!("kafka_ipc_writer", kafka.consumer.id = idx);
        // polling from kafka and send to ipc writer
        consumers.spawn_blocking(move || {
            let _entered = ipc_span.entered();
            let mut writer = StreamWriter::try_new(stream, &schema_clone)?;

            let mut row_count = 0;
            let mut batches = 0;
            let mut backoff = 0;
            while let Ok(batch) = rx.recv() {
                loop {
                    let ack = ack_num.load(std::sync::atomic::Ordering::SeqCst);
                    if batches - ack > max_wait_ack {
                        if tracing::enabled!(tracing::Level::TRACE) {
                            tracing::debug!(
                                ack = ack,
                                backoff,
                                batches,
                                "Kafka IPC Writer ack not catch up, wait for ack"
                            );
                        } else if backoff > 0 {
                            tracing::debug!(
                                ack = ack,
                                backoff,
                                batches,
                                "Kafka IPC Writer ack not catch up, wait for ack"
                            );
                        }
                        backoff += 1;
                        std::thread::sleep(Duration::from_millis(backoff * 100));
                        continue;
                    } else {
                        backoff = 0;
                        break;
                    }
                }
                writer.write(&batch)?;
                row_count += batch.num_rows();
                tracing::trace!(
                    batches,
                    rows = row_count,
                    "Kafka IPC Writer send {} rows",
                    batch.num_rows()
                );

                batches += 1;
            }
            let _ = writer.finish()?;
            tracing::info!(
                send.batches = batches,
                send.records = row_count,
                "Kafka IPC Writer finished, waiting for persisting"
            );
            Ok(ExitStatus::Finished)
        });

        let metrics = metrics_arc.clone();
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
                const MAX_RETRY_TIMES: usize = 5;
                loop {
                    match poll_message(
                        idx,
                        &mut consumer,
                        &tx,
                        &ack_rx,
                        timeout,
                        &aborted,
                        &schema,
                        batch_size,
                        batch_timeout_ms,
                        &notify,
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
                            let error = format!("{err:#}");
                            if errors < MAX_RETRY_TIMES {
                                let context = consumer.context().clone();
                                drop(consumer);
                                context.metrics().sub_extra_metric(&METRIC_CONSUMERS, 1);
                                let joins = context.current_joins();

                                if instance.is_some() && error.contains("FencedInstanceId") {
                                    instance = Some(format!("{idx}-{}", uuid::Uuid::new_v4()));
                                }
                                warn!(error, instance, "Try to rebuild consumer {idx}");

                                consumer = Arc::into_inner(context)
                                    .map_or_else(
                                        || {
                                            config.build_consumer(
                                                instance.as_deref(),
                                                &topics
                                                    .iter()
                                                    .map(|s| s.as_str())
                                                    .collect::<Vec<&str>>(),
                                                &metrics,
                                            )
                                        },
                                        |context| {
                                            config.build_consumer_with_context(
                                                instance.as_deref(),
                                                &topics
                                                    .iter()
                                                    .map(|s| s.as_str())
                                                    .collect::<Vec<&str>>(),
                                                context,
                                            )
                                        },
                                    )
                                    .with_context(|| {
                                        format!("{joins} loop to rebuild consumer {idx} error")
                                    })?;

                                notify
                                    .send(crate::TaskNotify::info(instance.as_deref().map_or_else(
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
                            errors += 1;
                            warn!(error, "Kafka consuming error");
                            Err(err)?;
                        }
                    }
                }
            }
            .instrument(tracing::info_span!("consumer", kafka.consumer.id = idx)),
        );
    }

    // drop(tx);

    Ok(consumers)
}

struct SubTask {
    /// Kafka task config for rebuilding consumer.
    config: Arc<KafkaTaskConfig>,
    /// Topics to consume.
    topics: Arc<Vec<String>>,
    /// Unique id in the group, for rdkafka `group.instance.id` configuration.
    instance: Option<String>,
    /// Initial consumer.
    consumer: LoggingConsumer,
    /// Timeout for polling messages in milliseconds.
    timeout: i64,
}

impl SubTask {
    pub fn build_tasks(
        config: KafkaTaskConfig,
        _notify: &crate::TaskNotifySender,
        metrics: &Arc<CoreMetrics>,
    ) -> anyhow::Result<Vec<Self>> {
        let client_config = build_client_config(config.connect.clone()).unwrap();

        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .context("failed to create consumer")?;

        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, FETCH_METADATA_TIMEOUT)
            .context("failed to load meta data")?;

        tracing::info!(
            brokers = metadata
                .brokers()
                .iter()
                .map(|b| format!("{}={}:{}", b.id(), b.host(), b.port()))
                .join(","),
            broker.id = metadata.orig_broker_id(),
            broker.name = metadata.orig_broker_name(),
            "kafka metadata"
        );

        // topic -> partition count
        let topic_partitions: HashMap<&str, usize> = metadata
            .topics()
            .iter()
            .filter(|tp| !tp.name().starts_with("__"))
            .filter(|tp| config.topics.contains(&tp.name().to_string()))
            .map(|tp| (tp.name(), tp.partitions().len()))
            .collect();

        if topic_partitions.is_empty() {
            tracing::error!(
                "topics is empty, expected: {:?}, please check your topic authorization",
                config.topics
            );
            anyhow::bail!(
                "topics is empty, expected: {:?}, please check your topic authorization",
                config.topics
            );
        }
        metrics.ipc().set_extra_metric(
            &METRIC_TOTAL_PARTITIONS,
            topic_partitions.values().sum::<usize>() as _,
        );

        if topic_partitions.len() != config.topics.len() {
            tracing::error!(
                "Some topics are not readable, expected: {:?}, actual: {:?}, please check your topic authorization",
                config.topics.len(),
                topic_partitions.len());
            anyhow::bail!(
                    "Some topics are not readable, expected: {:?}, actual: {:?}, please check your topic authorization",
                    config.topics.len(),
                    topic_partitions.len());
        }

        let mut sub_tasks = Vec::new();
        let concurrency = match config.advanced_options.read_concurrency {
            Some(n) if n > 0 => n.min(topic_partitions.values().sum()),
            _ => topic_partitions.values().sum(),
        };

        let config = Arc::new(config);
        let topics = topic_partitions
            .keys()
            .into_iter()
            .map(|k| k.to_string())
            .collect_vec();
        let topics = Arc::new(topics);

        for i in 0..concurrency {
            let instance = if config.enable_group_instance_id {
                Some(format!("{i}-{}", uuid::Uuid::new_v4()))
            } else {
                None
            };
            let consumer = config.build_consumer(
                instance.as_deref(),
                &topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
                metrics,
            )?;
            let topics = topics.clone();

            let sub_task = SubTask {
                config: config.clone(),
                topics,
                instance,
                consumer,
                timeout: config.timeout,
            };
            sub_tasks.push(sub_task);
        }
        for (idx, t) in sub_tasks.iter().enumerate() {
            match t.consumer.assignment() {
                Ok(tp_list) => {
                    for tp in tp_list.elements() {
                        tracing::info!(
                            consumer.id = idx,
                            consumer.topic = tp.topic(),
                            consumer.partition = tp.partition(),
                            consumer.offset = ?tp.offset(),
                        );
                    }
                }
                Err(err) => {
                    tracing::error!(
                        consumer.id = idx,
                        "Consumer {idx} failed to assign partitions: {err}",
                    );
                }
            }
        }

        Ok(sub_tasks)
    }
}

enum ExitStatus {
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

struct MessagesSender<'a> {
    consumer: &'a LoggingConsumer,
    tx: &'a flume::Sender<RecordBatch>,
    commit_rx: &'a flume::Receiver<LushAck>,
    /// Batch id/sequence number
    bid: u64,
    // Send options
    batch_size: usize,
    batch_timeout_ms: i64,
    polling_timeout_ms: i64,
    last_polling: i64,
    last_sent: i64,
    runtime_polling: Instant,

    // Builders
    timestamp: TimestampNanosecondBuilder,
    topic: StringBuilder,
    partition: Int32Builder,
    offset: Int64Builder,
    key: BinaryBuilder,
    value: BinaryBuilder,
    schema: &'a SchemaRef,
}

impl<'a> MessagesSender<'a> {
    pub async fn send_chuck(
        &mut self,
        chunk: &[BorrowedMessage<'a>],
    ) -> anyhow::Result<ExitStatus> {
        if chunk.is_empty() {
            tracing::trace!("Empty chunk, go next polling");
            return Ok(ExitStatus::None);
        }

        let now = std::time::Instant::now();
        let context = self.consumer.context();
        context
            .metrics()
            .add_extra_metric(&METRIC_CONSUMED_MESSAGES, chunk.len() as _);
        context
            .metrics()
            .add_extra_metric(&METRIC_TOTAL_CONSUMED_MESSAGES, chunk.len() as _);
        let chunks = chunk.iter().chunk_by(|msg| (msg.topic(), msg.partition()));

        let permit = context.sem.acquire().await;
        for ((topic, partition), iter) in &chunks {
            let mut offset = None;
            for msg in iter {
                offset.replace(msg.offset());
                if let Some(s) = msg.payload() {
                    self.timestamp
                        .append_value(Utc::now().timestamp_nanos_opt().unwrap());
                    self.topic.append_value(msg.topic());
                    self.partition.append_value(msg.partition());
                    self.offset.append_value(msg.offset());
                    self.key.append_value(msg.key().unwrap_or(&[]));
                    self.value.append_value(s);
                }
            }
            let offset = offset.expect("offset should always exist");

            if let Some(map) = context.offsets_cache.get(topic) {
                unsafe {
                    map.entry(partition)
                        .and_modify(|v| *v = offset)
                        .or_insert(offset);
                }
            } else {
                let map = HashIndex::with_capacity(1);
                let _ = map
                    .insert(partition, offset)
                    .inspect_err(|(partition, offset)| {
                        tracing::warn!(topic, partition, offset, "Push offset error")
                    });
                let _ = context
                    .offsets_cache
                    .insert(topic.to_string(), map)
                    .inspect_err(|_| {
                        tracing::warn!(
                            topic,
                            partition,
                            offset,
                            "Push offset error for topic `{topic}`"
                        )
                    });
            }
        }
        drop(permit);

        tracing::debug!(
            elapsed = ?now.elapsed(),
            cache.len = self.value.len(),
            chunk.len = chunk.len(),
            "Push to batch"
        );

        self.last_polling = chrono::Utc::now().timestamp_millis();

        if self.value.len() == 0 {
            tracing::trace!("Empty values, go next polling");
            // Empty values, go next polling.
            return Ok(ExitStatus::None);
        }

        if self.value.len() >= self.batch_size {
            tracing::debug!(
                cache.len = self.value.len(),
                "Batch size reached, send directly"
            );
            self.runtime_polling = Instant::now();
            // Reaches batch size, send directly.
            return unsafe { self.send_unchecked().in_current_span().await };
        }

        let now = chrono::Utc::now().timestamp_millis();
        if now - self.last_sent > self.batch_timeout_ms {
            tracing::debug!(
                cache.len = self.value.len(),
                "Batch timeout reached, send directly"
            );
            self.runtime_polling = Instant::now();
            // Reaches batch timeout, send directly.
            return unsafe { self.send_unchecked().in_current_span().await };
        }

        tracing::trace!(
            cache.len = self.value.len(),
            "Stay in cache, go next polling"
        );
        // Partially in cache, go next polling.
        anyhow::Ok(ExitStatus::None)
    }

    async unsafe fn send_unchecked(&mut self) -> anyhow::Result<ExitStatus> {
        let cost = self.runtime_polling.elapsed();
        if cost > Duration::from_secs(1) {
            tracing::warn!(runtime.cost = ?cost, "Send batch to IPC Writer");
        } else {
            tracing::debug!(runtime.cost = ?cost, "Send batch to IPC Writer");
        }
        debug_assert!(
            self.value.len() > 0,
            "value length should be greater than 0"
        );
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.topic.finish()),
                Arc::new(self.partition.finish()),
                Arc::new(self.offset.finish()),
                Arc::new(self.key.finish()),
                Arc::new(self.value.finish()),
            ],
        )?;
        let batch_size = batch.num_rows();
        tracing::debug!(
            ipc.sender.cap = self.tx.capacity(),
            ipc.sender.len = self.tx.len(),
            ipc.batch.size = batch_size,
            ipc.batch.id = self.bid,
            "Send batch to IPC Writer",
        );
        self.runtime_polling = Instant::now();
        self.tx
            .send_async(batch)
            .await
            .context("Writer seems closed")?;
        let ack = self
            .commit_rx
            .recv_async()
            .await
            .context("Writer seems closed")?;
        if !ack.success() {
            tracing::error!(ack.code = %ack.code(), ack.message = ack.message(), ack.context = ack.context(), "Kafka ack found error");
            if let Some(message) = ack.message() {
                anyhow::bail!("Kafka IPC writer error: {message}");
            } else {
                anyhow::bail!("Kafka IPC writer error with code: {}", ack.code());
            }
        }
        self.bid += 1;
        self.last_sent = chrono::Utc::now().timestamp_millis();
        let cost = self.runtime_polling.elapsed();

        if cost > Duration::from_secs(2) {
            tracing::warn!(runtime.cost = ?cost, "Send batch to IPC Writer seems slow");
        } else {
            tracing::debug!(runtime.cost = ?cost, "Send batch to IPC Writer");
        }

        let context = self.consumer.context();
        let permit = context.sem.acquire().await;
        let guard = scc::ebr::Guard::new();

        let mut assignment = None;
        let mut no_offsets = true;
        let mut error_offsets = Vec::new();
        for (topic, map) in context.offsets_cache.iter(&guard) {
            for (partition, offset) in map.iter(&guard) {
                if let Err(err) = self.consumer.store_offset(&topic, *partition, *offset) {
                    warn!(
                        cause = %err,
                        topic,
                        partition,
                        offset,
                        "Store offset error in partition {}",
                        partition,
                    );
                    error_offsets.push((topic.to_string(), *partition));
                    if self.consumer.assignment_lost() {
                        warn!("Consumer assignment lost, skip");
                        continue;
                    }
                    if assignment.is_none() {
                        assignment = self.consumer.assignment().inspect_err(|err| {
                            tracing::error!(cause  = %err, "Get consumer assignment error")
                        }).ok();
                    }
                    if assignment.is_none() {
                        warn!("Store offset error in partition {partition} of topic `{topic}`, seems assignment lost");
                        continue;
                    }
                    if assignment
                        .as_ref()
                        .unwrap()
                        .elements_for_topic(topic)
                        .iter()
                        .all(|item| item.partition() != *partition)
                    {
                        tracing::warn!("Rebalanced, partition {partition} is no longer assigned to this consumer");
                    } else {
                        Err(err).with_context(|| {
                            format!(
                                "Store offset error in partition {partition} of topic `{topic}`"
                            )
                        })?;
                    }
                } else {
                    no_offsets = false;
                }
            }
        }
        drop(guard);
        for (topic, partition) in error_offsets {
            if let Some(index) = context.offsets_cache.get(&topic) {
                index.remove(&partition);
                if index.is_empty() {
                    index.remove_entry();
                }
            }
        }
        drop(permit);

        if no_offsets {
            tracing::warn!(batch.size = batch_size, "No offsets stored, skip commit");
            // return anyhow::Ok(ExitStatus::Finished);
        }

        // if self.consumer.assignment_lost() {
        //     warn!("Consumer assignment lost, continue");
        //     return anyhow::Ok(ExitStatus::Finished);
        // }

        // if let Err(err) = self.consumer.commit_consumer_state(CommitMode::Sync) {
        //     let err_str = format!("{:#}", err);
        //     tracing::warn!("failed to commit consumer state, cause: {}", err_str);
        //     if err_str.contains("NoOffset")
        //         || err_str.contains("AssignmentLost")
        //         || err_str.contains("UnknownMemberId")
        //     {
        //         // Maybe the consumer has been rebalanced, so we continue to see next.
        //         return Ok(ExitStatus::Finished);
        //     }
        //     bail!("failed to commit consumer state, cause: {}", err_str);
        // }

        anyhow::Ok(ExitStatus::Finished)
    }

    /// Safely send batches in cache or return timeout.
    async fn send(&mut self) -> anyhow::Result<ExitStatus> {
        if self.value.len() == 0 {
            if self.polling_timeout_ms <= 0 {
                return Ok(ExitStatus::None);
            }
            let now = chrono::Utc::now().timestamp_millis();
            if now - self.last_polling > self.polling_timeout_ms {
                // Reaches batch timeout.
                return Ok(ExitStatus::Timeout);
            }
            return Ok(ExitStatus::None);
        }
        unsafe { self.send_unchecked().in_current_span().await }
    }
}

async fn poll_message<'a>(
    index: usize,
    consumer: &'a mut LoggingConsumer,
    tx: &flume::Sender<RecordBatch>,
    commit_rx: &flume::Receiver<LushAck>,
    timeout: i64,
    aborted: &CancellationToken,
    schema: &SchemaRef,
    batch_size: usize,
    batch_timeout_ms: i64,
    notify: &crate::TaskNotifySender,
) -> anyhow::Result<ExitStatus> {
    const MAX_READY_CHUNK_SIZE: usize = 100;

    let mut ready_chunks =
        futures_ext::TryReadyChunks::new(consumer.stream(), batch_size.max(MAX_READY_CHUNK_SIZE));

    let timeout_duration = if timeout > 0 {
        Duration::from_millis(timeout as u64)
    } else {
        Duration::MAX
    };
    // let batch_timeout = Duration::from_millis(batch_timeout_ms as u64);

    let timestamp = TimestampNanosecondBuilder::new();
    let topic = StringBuilder::new();
    let partition: arrow::array::PrimitiveBuilder<arrow::datatypes::Int32Type> =
        Int32Builder::new();
    let offset = Int64Builder::new();
    let key = BinaryBuilder::new();
    let value = BinaryBuilder::new();

    let mut sender = MessagesSender {
        consumer,
        tx,
        commit_rx,
        bid: 0,
        batch_size,
        batch_timeout_ms,
        polling_timeout_ms: timeout,
        last_polling: chrono::Utc::now().timestamp_millis(),
        last_sent: chrono::Utc::now().timestamp_millis(),
        runtime_polling: Instant::now(),
        timestamp,
        topic,
        partition,
        offset,
        key,
        value,
        schema,
    };

    // static RANDOM_ERROR_ATOMIC: AtomicUsize = AtomicUsize::new(1);

    let mut backoff = 1;
    let mut last_message = Instant::now();
    const INITIAL_NON_MESSAGE_WARNING_INTERVAL: u64 = 30;
    const MAX_NON_MESSAGE_WARNING_INTERVAL: u64 = 480;
    const MAX_BACKOFF: u64 = 16;
    let mut last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;
    loop {
        tracing::trace!("Kafka consumer-{} polling by ready trunks", index);
        tokio::select! {
            biased;
            _ = aborted.cancelled() => {
                tracing::info!("Kafka consumer-{} cancelled", index);
                return Ok(ExitStatus::Aborted);
            }
            _ = tokio::time::sleep(timeout_duration) => {
                tracing::info!("Kafka consumer-{} polling timeout", index);
                return Ok(ExitStatus::Timeout);
            }
            chunk = ready_chunks.next() => {
                // if RANDOM_ERROR_ATOMIC.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % 32 == 0 {
                //     bail!("Random error");
                // };
                match chunk {
                    Some(Ok(chunk)) => {
                        backoff = 1;
                        last_message = Instant::now();
                        last_warning_interval = INITIAL_NON_MESSAGE_WARNING_INTERVAL;
                        match sender.send_chuck(&chunk).in_current_span().await? {
                            ExitStatus::None => {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                            ExitStatus::Timeout => {
                                tracing::warn!("Ready chunks should never exit by polling timeout");
                                return Ok(ExitStatus::Timeout);
                            }
                            ExitStatus::Aborted => {
                                tracing::warn!("Kafka consumer should not be aborted with ready chunks");
                                return Ok(ExitStatus::Aborted);
                            }
                            ExitStatus::Finished => {
                                continue;
                            }
                        }
                    }
                    Some(Err(TryReadyChunksError(_, e))) => {
                        if aborted.is_cancelled() {
                            tracing::debug!(error = %e, "Consumer aborted even error occur");
                            return Ok(ExitStatus::Aborted);
                        }
                        if e == KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut) {
                            tracing::warn!("Kafka polling timeout, continue");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        if e == KafkaError::MessageConsumption(RDKafkaErrorCode::PollExceeded) {
                            tracing::warn!("Maximum application poll interval (max.poll.interval.ms) exceeded");
                        }

                        // Ready chunks may still contains some messages, but we just skip them.
                        // It will be handled by next consuming.
                        let _ = notify.send(crate::TaskNotify::warn(format!("failed to polling from kafka, cause: {:#}", e)));
                        tracing::error!("failed to polling from kafka, cause: {:#}", e);
                        bail!("failed to polling from kafka, cause: {:#}", e);
                    }
                    None => {
                        tracing::trace!("Kafka polling return None, continue");
                        match sender.send().in_current_span().await? {
                            ExitStatus::None => {
                                if backoff < MAX_BACKOFF {
                                    backoff *= 2;
                                }
                                let duration = Duration::from_secs(last_warning_interval);
                                let elapsed = last_message.elapsed();
                                if elapsed > duration {
                                    tracing::warn!("Consumer {index} has no messages received in {:?} consumer polling timeout", elapsed);
                                    let _ = notify.send(crate::TaskNotify::warn(format!("Consumer {index} has no messages received in {:?} consumer polling timeout", duration)));
                                    if last_warning_interval < MAX_NON_MESSAGE_WARNING_INTERVAL {
                                        last_warning_interval *= 2;
                                        last_message = Instant::now();
                                    } else {
                                        bail!("Consumer {index} has no messages received in {:?}", elapsed);
                                    }
                                }
                                tokio::time::sleep(Duration::from_millis(backoff * 100)).await;
                            }
                            ExitStatus::Timeout => {
                                tracing::info!("None messages received, exit with consumer polling timeout");
                                return Ok(ExitStatus::Timeout);
                            }
                            ExitStatus::Aborted => {
                                tracing::info!("None messages received, exiting with consumer aborted");
                                return Ok(ExitStatus::Aborted);
                            }
                            ExitStatus::Finished => {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }
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
        Field::new("value", ArrowDataType::Binary, false),
    ];
    let schema = Schema::new(flat_columns).with_metadata(metadata);
    schema
}

/// due to this issue: https://github.com/fede1024/rust-rdkafka/issues/681
/// do not use `{:?}` for `TopicPartitionList` struct, or we will meet a panic
/// we use a temporary workaround for now
struct CustomContext {
    offsets_cache: scc::HashIndex<String, scc::HashIndex<i32, i64>>,
    sem: tokio::sync::Semaphore,
    /// Metric 1: joins, times that a retryable consumer joins to the group.
    joins: AtomicUsize,
    rebalances: AtomicUsize,
    commits: AtomicUsize,
    metrics: Arc<CoreMetrics>,
}

impl CustomContext {
    fn fetch_add_joins(&self) -> usize {
        self.joins.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn current_joins(&self) -> usize {
        self.joins.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn new(metrics: Arc<CoreMetrics>) -> Self {
        Self {
            offsets_cache: scc::HashIndex::with_capacity(1),
            sem: tokio::sync::Semaphore::new(1),
            joins: AtomicUsize::new(0),
            rebalances: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
            metrics,
        }
    }

    fn metrics(&self) -> &IpcMetrics {
        self.metrics.ipc()
    }
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        if is_rebalance_empty(rebalance) {
            return;
        }
        // match rebalance {
        //     Rebalance::Assign(tpl) => {
        //         tracing::info!("Assign {}", tpl.count());
        //         self.metrics()
        //             .sub_extra_metric(&METRIC_CONSUMING_PARTITIONS, tpl.count() as _);
        //     }
        //     Rebalance::Revoke(tpl) => {
        //         tracing::info!("Revoke {}", tpl.count());
        //         self.metrics()
        //             .sub_extra_metric(&METRIC_CONSUMING_PARTITIONS, tpl.count() as _);
        //     }
        //     Rebalance::Error(err) => {
        //         tracing::error!("Pre rebalance error: {:?}", err);
        //     }
        // }
        self.sem.forget_permits(1);
        if !self.offsets_cache.is_empty() {
            tracing::info!("Pre rebalance {:?}, will clear offsets cache", rebalance);
            self.offsets_cache.clear();
        } else {
            tracing::info!("Pre rebalance {:?}", rebalance);
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        if is_rebalance_empty(rebalance) {
            return;
        }

        match rebalance {
            Rebalance::Assign(tpl) => {
                tracing::info!("Post Assign {}", tpl.count());
                self.metrics()
                    .add_extra_metric(&METRIC_CONSUMING_PARTITIONS, tpl.count() as _);
            }
            Rebalance::Revoke(tpl) => {
                tracing::info!("Post Revoke {}", tpl.count());
                self.metrics()
                    .sub_extra_metric(&METRIC_CONSUMING_PARTITIONS, tpl.count() as _);
            }
            Rebalance::Error(err) => {
                tracing::error!("Pre rebalance error: {:?}", err);
            }
        }
        let rebalances = self
            .rebalances
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tracing::info!(rebalances, "Post rebalance {:?}", rebalance);
        self.sem.add_permits(1);
    }

    fn commit_callback(&self, result: KafkaResult<()>, tpl: &TopicPartitionList) {
        if is_tplist_empty(tpl) {
            return;
        }
        if let Err(err) = result {
            error!(commits = self.commits.load(std::sync::atomic::Ordering::SeqCst), error = %err, "Commit error");
        } else {
            let commits = self
                .commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tracing::info!(commits, "Committing offsets: {:?}", result);
        }
    }

    fn main_queue_min_poll_interval(&self) -> rdkafka::util::Timeout {
        rdkafka::util::Timeout::After(Duration::from_millis(200))
    }
}

fn is_rebalance_empty(r: &Rebalance) -> bool {
    match r {
        Rebalance::Assign(tpl) => is_tplist_empty(tpl),
        Rebalance::Revoke(tpl) => is_tplist_empty(tpl),
        Rebalance::Error(_) => false,
    }
}

fn is_tplist_empty(tpl: &TopicPartitionList) -> bool {
    tpl.capacity() == 0
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

impl KafkaTaskConfig {
    fn build_consumer(
        &self,
        instance: Option<&str>,
        topics: &[&str],
        metrics: &Arc<CoreMetrics>,
    ) -> anyhow::Result<LoggingConsumer> {
        self.build_consumer_with_context(instance, topics, CustomContext::new(metrics.clone()))
    }

    fn build_consumer_with_context(
        &self,
        instance: Option<&str>,
        topics: &[&str],
        context: CustomContext,
    ) -> anyhow::Result<LoggingConsumer> {
        let mut client = build_client_config(self.connect.clone()).unwrap();
        // Client identifier, default "rdkafka".
        if let Some(client_id) = &self.client_id {
            client.set("client.id", client_id);
        }
        // All clients sharing the same group.id belong to the same group.
        client.set("group.id", &self.group);
        // Action to take when there is no initial offset in offset store or the desired offset is out of range.
        // smallest, earliest, beginning, largest, latest, end, error
        client.set("auto.offset.reset", &self.fallback_offset);

        // Refer to [rdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
        // > Note: It is recommended to set `enable.auto.offset.store=false`
        // >  for long-time processing applications and then explicitly store offsets
        // >  (using offsets_store()) after message processing, to make sure
        // >  offsets are not auto-committed prior to processing has finished.
        client.set("enable.auto.offset.store", "false");
        client.set("enable.auto.commit", "true");

        client.set(
            "auto.commit.interval.ms",
            self.commit_interval
                .as_ref()
                .map_or(5000, |d| d.as_millis())
                .to_string(),
        );

        client.set("queued.max.messages.kbytes", "262144");

        // Maximum time the broker may wait to fill the Fetch response with fetch.min.bytes of messages, default 500ms.
        client.set(
            "fetch.wait.max.ms",
            self.fetch_max_wait_time
                .map(|v| v.as_millis())
                .unwrap_or(500)
                .to_string(),
        );

        // Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll())
        // for high-level consumers. If this interval is exceeded the consumer is considered failed
        // and the group will rebalance in order to reassign the partitions to another consumer group member.
        // Warning: Offset commits may be not possible at this point.
        client.set("max.poll.interval.ms", "3600000");

        if let Some(instance) = instance {
            // client.set("enable.idempotence", "true");
            client.set("group.instance.id", instance);
        }

        // A larger value allows the consumer to fetch more messages in one request.
        // client.set("queued.min.messages", "1000000");

        // Minimum number of bytes the broker responds with, default is 1.
        client.set(
            "fetch.min.bytes",
            self.fetch_min_bytes.unwrap_or(1).to_string(),
        );

        // Initial maximum number of bytes per topic+partition to request when fetching messages from the broker.
        if self.fetch_max_bytes_per_partition.is_some() {
            client.set(
                "fetch.message.max.bytes",
                self.fetch_max_bytes_per_partition.unwrap().to_string(),
            );
        }
        // Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption to the messages occurred
        if self.fetch_crc_validation.is_some() {
            client.set("check.crcs", self.fetch_crc_validation.unwrap().to_string());
        }
        // Close broker connections after the specified time of inactivity.
        if self.connection_idle_timeout.is_some() {
            client.set(
                "connections.max.idle.ms",
                self.connection_idle_timeout
                    .unwrap()
                    .as_millis()
                    .to_string(),
            );
        }

        client.set("partition.assignment.strategy", "cooperative-sticky");
        client.set("socket.keepalive.enable", "true");
        client.set("socket.timeout.ms", "300000");

        if let Some(extras) = &self.extras {
            for (k, v) in extras.iter() {
                client.set(k.as_str(), v.as_str());
                tracing::info!("Set extra config: {}={}", k, v);
            }
        }
        // Set log level and create consumer
        let joins = context.fetch_add_joins();
        match instance.as_deref() {
            Some(instance) => {
                tracing::info!(joins, "Consumer {instance} begin join");
            }
            None => {
                tracing::info!(joins, "Consumer begin join");
            }
        }

        let consumer: LoggingConsumer = client
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(context)
            .context("Consumer creation failed")?;

        consumer
            .subscribe(topics)
            .context("Kafka subscribe consumer error")?;

        let subscription = consumer
            .subscription()
            .context("Kafka get consumer subscription metadata error")?;
        consumer
            .context()
            .metrics()
            .add_extra_metric(&METRIC_CONSUMERS, 1);
        for t in subscription.elements() {
            tracing::info!(
                kafka.consumed.partions = subscription.count(),
                "Consumer subscribed to topic: {}:{}:{:?}",
                t.topic(),
                t.partition(),
                t.offset()
            );
        }

        if subscription.count() > 0 {
            let _ = consumer.store_offsets(&subscription);
        } else {
            tracing::info!("No subscription found");
        }

        Ok(consumer)
    }
}
fn build_client_config(config: KafkaConnectConfig) -> anyhow::Result<ClientConfig> {
    let mut client_config = ClientConfig::new();

    // set bootstrap servers
    client_config.set("bootstrap.servers", config.bootstrap_servers.join(","));

    // security.protocol: plaintext, ssl, sasl_plaintext, sasl_ssl
    match (config.use_ssl, config.use_sasl) {
        (true, true) => client_config.set("security.protocol", "sasl_ssl"),
        (true, false) => client_config.set("security.protocol", "ssl"),
        (false, true) => client_config.set("security.protocol", "sasl_plaintext"),
        (false, false) => client_config.set("security.protocol", "plaintext"),
    };

    // ssl settings
    if config.use_ssl {
        if let Some(ca_cert) = config.ca_cert {
            client_config.set("ssl.ca.pem", ca_cert);
        }
        if let Some(ca_password) = config.ca_cert_password {
            client_config.set("ssl.key.password", ca_password);
        }
        if let Some(client_cert) = config.client_cert {
            client_config.set("ssl.certificate.pem", client_cert);
        }
        if let Some(client_key) = config.client_key {
            client_config.set("ssl.key.pem", client_key);
        }
        // ref: https://karafka.io/docs/FAQ/#why-am-i-getting-error0a000086ssl-routinescertificate-verify-failed-after-upgrading-karafka
        client_config.set("ssl.endpoint.identification.algorithm", "none");
    }

    // sasl settings
    if config.use_sasl {
        if let Some(sasl_mechanism) = config.sasl_mechanism {
            if sasl_mechanism == "GSSAPI" {
                client_config.set("sasl.mechanisms", "GSSAPI");
                // get config or use default
                let sasl_kerberos_service_name =
                    if let Some(val) = config.sasl_kerberos_service_name {
                        val
                    } else {
                        "".to_string()
                    };
                let sasl_kerberos_principal = if let Some(val) = config.sasl_kerberos_principal {
                    val
                } else {
                    "".to_string()
                };
                let sasl_kerberos_kinit_cmd = if let Some(val) = config.sasl_kerberos_kinit_cmd {
                    val
                } else {
                    "kinit -R -t \"%{sasl.kerberos.keytab}\" -k %{sasl.kerberos.principal} || kinit -t \"%{sasl.kerberos.keytab}\" -k %{sasl.kerberos.principal}".to_string()
                };
                let sasl_kerberos_keytab = if let Some(val) = config.sasl_kerberos_keytab {
                    val
                } else {
                    "".to_string()
                };
                // verify the broker's kinit.cmd, keytab and principal
                let init_cmd = sasl_kerberos_kinit_cmd
                    .replace("%{sasl.kerberos.keytab}", &sasl_kerberos_keytab.as_str())
                    .replace(
                        "%{sasl.kerberos.principal}",
                        &sasl_kerberos_principal.as_str(),
                    );
                let output = std::process::Command::new("bash")
                    .arg("-c")
                    .arg(init_cmd)
                    .output();
                match output {
                    Ok(output) => {
                        if !output.status.success() {
                            tracing::error!("{}", std::str::from_utf8(&output.stderr).unwrap());
                            anyhow::bail!(
                                "{}",
                                std::str::from_utf8(&output.stderr)
                                    .unwrap()
                                    .lines()
                                    .next()
                                    .unwrap()
                            );
                        }
                    }
                    Err(_) => {}
                }
                // set to client
                client_config.set("sasl.kerberos.service.name", sasl_kerberos_service_name);
                client_config.set("sasl.kerberos.principal", sasl_kerberos_principal);
                client_config.set("sasl.kerberos.kinit.cmd", sasl_kerberos_kinit_cmd);
                client_config.set("sasl.kerberos.keytab", sasl_kerberos_keytab);
                // each entry will be resolved and expanded into a list of canonical names
                // client_config.set(
                //     "client.dns.lookup",
                //     "resolve_canonical_bootstrap_servers_only",
                // );
            } else {
                client_config.set("sasl.mechanisms", sasl_mechanism);
                if let Some(sasl_username) = config.sasl_username {
                    client_config.set("sasl.username", sasl_username);
                }
                if let Some(sasl_password) = config.sasl_password {
                    client_config.set("sasl.password", sasl_password);
                }
            }
        }
    }

    Ok(client_config)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use taos::IntoDsn;

    use super::*;

    #[tokio::test]
    async fn test_is_valid() {
        let dsn = Dsn::from_str("kafka://127.0.0.1:9092").unwrap();
        let result = is_valid(&dsn).await;
        assert_eq!(false, result.valid);
        assert_eq!(false, result.support);
        assert_eq!(KAFKA_ID, result.data_source);
        assert_eq!(
            "invalid dsn: kafka://127.0.0.1:9092, cause: topics is required",
            result.message.unwrap()
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
        .unwrap();

        let config = KafkaConnectConfig::from_dsn(&dsn).unwrap();
        let client_config: ClientConfig = build_client_config(config.clone()).unwrap();
        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|err| anyhow::anyhow!("failed to create consumer, cause: {:#}", err))
            .unwrap();
        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .map_err(|err| anyhow::anyhow!("failed to load meta data, cause: {:#}", err))
            .unwrap();

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
        .unwrap();

        let config = KafkaConnectConfig::from_dsn(&dsn).unwrap();
        let client_config: ClientConfig = build_client_config(config.clone()).unwrap();
        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|err| anyhow::anyhow!("failed to create consumer, cause: {:#}", err))
            .unwrap();
        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .map_err(|err| anyhow::anyhow!("failed to load meta data, cause: {:#}", err))
            .unwrap();
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

    #[tokio::test]
    #[ignore]
    async fn produce_messages() {
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
        use rdkafka::util::Timeout;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "kafka:9092")
            .create()
            .unwrap();

        for _ in 0..300 {
            producer
                .send(
                    FutureRecord::to("tp1")
                        .payload(r#"{"a": 1725518745000, "c": 3}"#)
                        .key("key"),
                    Timeout::Never,
                )
                .await
                .unwrap();
        }
        producer.flush(std::time::Duration::from_secs(5)).unwrap();
    }
}
