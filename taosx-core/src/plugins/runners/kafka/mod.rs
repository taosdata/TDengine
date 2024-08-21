use std::collections::HashMap;
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
use futures_ext::TryReadyChunksError;
use futures_util::StreamExt;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::Offset;
use scc::HashIndex;
use serde_json::json;
use taos::Dsn;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, warn, Instrument, Span};

use taosx_ipc::ack::AckReaderBuilder;
use taosx_ipc::prelude::ArrowDataType;

use crate::plugins::dsv::DataSourceValidation;
use crate::plugins::transform::sample::DsSampleIn;
use crate::runners::kafka::config::connect::KafkaConnectConfig;
use crate::runners::kafka::config::KafkaTaskConfig;
use crate::runners::set_tcp_keepalive;
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

mod config;

pub const KAFKA_ID: &'static str = "kafka";
const FETCH_METADATA_TIMEOUT: Duration = Duration::from_secs(30);

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
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
    span: Span,
    task_id: Option<i64>,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let cancel = cancel.child_token();
    let _drop_guard = cancel.clone().drop_guard();
    tracing::info!(
        "Kafka task: {} start, from: {}, parser: {}, to: {}",
        task_id.unwrap_or(-1),
        from,
        serde_json::to_string(&parser)?,
        to
    );

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
        span,
        task_id.clone(),
        notify.clone(),
    )
    .await?;

    let aborted_cloned = cancel.clone();
    let mut join_set = execute(from, ipc_port.get(), aborted_cloned, notify.clone())
        .in_current_span()
        .await?;
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
                        if status.is_timeout() {
                            cancel.cancel();
                            // wait for completion
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            join_set.abort_all();
                            // stop the connector
                            tracing::info!("Kafka task timeout");
                            ipc.close().await?;
                            return Ok(());
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
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
                        join_set.abort_all();
                        let _ = ipc.send(());
                        anyhow::bail!("Kafka exit with error: {:#}", err);
                    }
                }
            },
            err = ipc.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                cancel.cancel();
                join_set.abort_all();
                if let Some(err) = err {
                    let _ = ipc.send(()).await;
                    let _ = ipc.close().await;
                    join_set.abort_all();
                    anyhow::bail!("Kafka writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("Kafka task cancelled");
                join_set.abort_all();
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
) -> anyhow::Result<KafkaJoinSet> {
    let ipc_server = format!("127.0.0.1:{}", ipc_server_port);

    // ipc writer stream
    let stream = std::net::TcpStream::connect(ipc_server)?;
    set_tcp_keepalive(&stream)?;
    stream.set_read_timeout(None)?;

    // ack reader stream
    let ack_stream = stream.try_clone()?;
    set_tcp_keepalive(&ack_stream)?;
    ack_stream.set_read_timeout(None)?;
    let mut consumers = JoinSet::new();

    let ack_span = tracing::info_span!("kafka_ack_reader");
    // receive ACK from IPC
    consumers.spawn_blocking(move || {
        let _entered = ack_span.entered();
        let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
        for ack in ack_reader {
            if !ack.success() {
                tracing::error!(ack.code = %ack.code(), ack.message = ack.message(), ack.context = ack.context(), "Kafka ack found error");
                if let Some(message) = ack.message() {
                    anyhow::bail!("Kafka IPC writer error: {message}");
                }
            }
        }
        tracing::info!("Kafka ACK reader finished");
        Ok(ExitStatus::Finished)
    });

    let schema = build_schema();
    // multi producer(KafkaConsumer) and single consumer(IPC Writer)
    let (tx, rx) = flume::bounded(0);

    // IPC Writer
    let schema_clone = schema.clone();
    let ipc_span = tracing::info_span!("kafka_ipc_writer");
    // polling from kafka and send to ipc writer
    consumers.spawn_blocking(move || {
        let _entered = ipc_span.entered();
        let mut writer = StreamWriter::try_new(stream, &schema_clone)?;

        let mut row_count = 0;
        let mut batches = 0;
        while let Ok(batch) = rx.recv() {
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
    let sub_tasks: Vec<SubTask> = SubTask::build_tasks(config, notify.clone())?;
    for (idx, task) in sub_tasks.into_iter().enumerate() {
        let tx = tx.clone();
        let aborted = aborted.clone();
        let schema = schema.clone();
        let consumer = task.consumer;
        let timeout = task.timeout;
        let notify = notify.clone();

        consumers.spawn(
            async move {
                poll_message(
                    idx,
                    &consumer,
                    tx,
                    timeout,
                    aborted,
                    schema,
                    batch_size,
                    batch_timeout_ms,
                    notify,
                )
                .in_current_span()
                .await
            }
            .instrument(tracing::info_span!("consumer", id = idx)),
        );
    }

    drop(tx);

    Ok(consumers)
}

struct SubTask {
    consumer: LoggingConsumer,
    timeout: i64,
}

impl SubTask {
    pub fn build_tasks(
        config: KafkaTaskConfig,
        _notify: crate::TaskNotifySender,
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

        for _ in 0..concurrency {
            let consumer = consumer_builder(&config)?;
            let topics = topic_partitions
                .keys()
                .into_iter()
                .map(|k| *k)
                .collect_vec();
            consumer
                .subscribe(&topics)
                .context("Kafka subscribe consumer error")?;

            let sub_task = SubTask {
                consumer,
                timeout: config.timeout,
            };
            sub_tasks.push(sub_task);
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
    tx: flume::Sender<RecordBatch>,
    // Send options
    batch_size: usize,
    batch_timeout_ms: i64,
    polling_timeout_ms: i64,
    last_polling: i64,
    last_sent: i64,

    // Builders
    timestamp: TimestampNanosecondBuilder,
    topic: StringBuilder,
    partition: Int32Builder,
    offset: Int64Builder,
    key: BinaryBuilder,
    value: BinaryBuilder,
    schema: SchemaRef,
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
        let chunks = chunk.iter().chunk_by(|msg| (msg.topic(), msg.partition()));

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
            // Reaches batch size, send directly.
            return unsafe { self.send_unchecked().in_current_span().await };
        }

        let now = chrono::Utc::now().timestamp_millis();
        if now - self.last_sent > self.batch_timeout_ms {
            tracing::debug!(
                cache.len = self.value.len(),
                "Batch timeout reached, send directly"
            );
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
        self.tx.send_async(batch).await?;
        self.last_sent = chrono::Utc::now().timestamp_millis();

        tracing::debug!(
            "Kafka consumer send batch to IPC Writer, batch size: {}",
            batch_size
        );

        let context = self.consumer.context();
        let guard = scc::ebr::Guard::new();

        let mut assignment = None;
        let mut no_offsets = true;
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
                    if assignment.is_none() {
                        assignment = self.consumer.assignment().inspect_err(|err| {
                            tracing::error!(cause  = %err, "Get consumer assignment error")
                        }).ok();
                    }
                    if assignment.is_none() {
                        bail!("Store offset error in partition {partition} of topic `{topic}`, seems assignment lost");
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

        if no_offsets {
            tracing::warn!(batch.size = batch_size, "No offsets stored, skip commit");
            return anyhow::Ok(ExitStatus::Finished);
        }

        if let Err(err) = self.consumer.commit_consumer_state(CommitMode::Sync) {
            let err_str = format!("{:#}", err);
            tracing::warn!("failed to commit consumer state, cause: {}", err_str);
            if err_str.contains("NoOffset") {
                return Ok(ExitStatus::Finished);
            }
            bail!("failed to commit consumer state, cause: {}", err_str);
        }

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
        unsafe { self.send_unchecked().await }
    }
}

async fn poll_message<'a>(
    index: usize,
    consumer: &'a LoggingConsumer,
    tx: flume::Sender<RecordBatch>,
    timeout: i64,
    aborted: CancellationToken,
    schema: Schema,
    batch_size: usize,
    batch_timeout_ms: i64,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<ExitStatus> {
    const MAX_READY_CHUNK_SIZE: usize = 100;

    let ready_chunks =
        futures_ext::TryReadyChunks::new(consumer.stream(), batch_size.max(MAX_READY_CHUNK_SIZE));
    tokio::pin!(ready_chunks);

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
    let schema = Arc::new(schema);

    let mut sender = MessagesSender {
        consumer,
        tx,
        batch_size,
        batch_timeout_ms,
        polling_timeout_ms: timeout,
        last_polling: chrono::Utc::now().timestamp_millis(),
        last_sent: chrono::Utc::now().timestamp_millis(),
        timestamp,
        topic,
        partition,
        offset,
        key,
        value,
        schema,
    };

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
                match chunk {
                    Some(Ok(chunk)) => {
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
                        if e == KafkaError::MessageConsumption(RDKafkaErrorCode::OperationTimedOut) {
                            tracing::warn!("Kafka polling timeout, continue");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }
                        if e == KafkaError::MessageConsumption(RDKafkaErrorCode::PollExceeded) {
                            tracing::warn!("Maximum application poll interval (max.poll.interval.ms) exceeded, try continue");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        }

                        // Ready chunks may still contains some messages, but we just skip them.
                        // It will be handled by next consuming.
                        let _ = notify.send(crate::TaskNotify::error(format!("failed to polling from kafka, cause: {:#}", e)));
                        tracing::error!("failed to polling from kafka, cause: {:#}", e);
                        bail!("failed to polling from kafka, cause: {:#}", e);
                    }
                    None => {
                        tracing::trace!("Kafka polling return None, continue");
                        match sender.send().in_current_span().await? {
                            ExitStatus::None => {
                                tokio::time::sleep(Duration::from_millis(200)).await;
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
}

impl Default for CustomContext {
    fn default() -> Self {
        Self {
            offsets_cache: scc::HashIndex::with_capacity(1),
        }
    }
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        if is_rebalance_empty(rebalance) {
            return;
        }
        tracing::info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        if is_rebalance_empty(rebalance) {
            return;
        }
        tracing::info!("Post rebalance {:?}, clear offsets", rebalance);
        self.offsets_cache.clear();
    }

    fn commit_callback(&self, result: KafkaResult<()>, tpl: &TopicPartitionList) {
        if is_tplist_empty(tpl) {
            return;
        }
        tracing::info!("Committing offsets: {:?}", result);
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

fn consumer_builder(config: &KafkaTaskConfig) -> anyhow::Result<LoggingConsumer> {
    let mut client = build_client_config(config.connect.clone()).unwrap();
    // Client identifier, default "rdkafka".
    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }
    // All clients sharing the same group.id belong to the same group.
    client.set("group.id", &config.group);
    // Action to take when there is no initial offset in offset store or the desired offset is out of range.
    // smallest, earliest, beginning, largest, latest, end, error
    client.set("auto.offset.reset", &config.fallback_offset);

    // Refer to [rdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
    // > Note: It is recommended to set `enable.auto.offset.store=false`
    // >  for long-time processing applications and then explicitly store offsets
    // >  (using offsets_store()) after message processing, to make sure
    // >  offsets are not auto-committed prior to processing has finished.
    client.set("enable.auto.offset.store", "false");

    // Maximum time the broker may wait to fill the Fetch response with fetch.min.bytes of messages, default 500ms.
    client.set(
        "fetch.wait.max.ms",
        config
            .fetch_max_wait_time
            .map(|v| v.as_millis())
            .unwrap_or(500)
            .to_string(),
    );

    // Minimum number of bytes the broker responds with, default is 1.
    client.set(
        "fetch.min.bytes",
        config.fetch_min_bytes.unwrap_or(1).to_string(),
    );

    // Initial maximum number of bytes per topic+partition to request when fetching messages from the broker.
    if config.fetch_max_bytes_per_partition.is_some() {
        client.set(
            "fetch.message.max.bytes",
            config.fetch_max_bytes_per_partition.unwrap().to_string(),
        );
    }
    // Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption to the messages occurred
    if config.fetch_crc_validation.is_some() {
        client.set(
            "check.crcs",
            config.fetch_crc_validation.unwrap().to_string(),
        );
    }
    // Close broker connections after the specified time of inactivity.
    if config.connection_idle_timeout.is_some() {
        client.set(
            "connections.max.idle.ms",
            config
                .connection_idle_timeout
                .unwrap()
                .as_millis()
                .to_string(),
        );
    }
    // Set log level and create consumer
    let consumer = client
        .set_log_level(RDKafkaLogLevel::Info)
        .create_with_context(CustomContext::default())
        .context("Consumer creation failed")?;
    Ok(consumer)
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
                client_config.set(
                    "client.dns.lookup",
                    "resolve_canonical_bootstrap_servers_only",
                );
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
}
