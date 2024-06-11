use std::cmp;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayBuilder, BinaryBuilder, Int32Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use futures_util::TryStreamExt;
use linked_hash_map::LinkedHashMap;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use serde_json::json;
use taos::Dsn;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Span;

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

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_valid_impl(dsn) {
        Ok(()) => DataSourceValidation::valid(KAFKA_ID.to_string(), None),
        Err(err) => DataSourceValidation::invalid(KAFKA_ID.to_string(), err.to_string()),
    }
}

fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
    let config = KafkaTaskConfig::from_dsn(dsn)
        .map_err(|err| anyhow::anyhow!("invalid dsn: {}, cause: {}", dsn, err.to_string()))?;

    let client_config = build_client_config(config.connect);
    let consumer: BaseConsumer = client_config
        .create()
        .map_err(|err| anyhow::anyhow!("failed to create client, cause: {}", err.to_string()))?;

    let _metadata = consumer
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(|err| anyhow::anyhow!("failed to load meta data, cause: {}", err.to_string()))?;
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
    // create consumer
    let connect_config = KafkaConnectConfig::from_dsn(dsn)?;
    let mut client_config = build_client_config(connect_config);
    let consumer: BaseConsumer = client_config
        .set("group.id", "test")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|err| anyhow::anyhow!("failed to create client, cause: {}", err.to_string()))?;

    // subscribe topics
    let topics = KafkaTaskConfig::parse_topics(dsn)?;
    let topics = topics.iter().map(|p| p.as_str()).collect::<Vec<&str>>();
    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");
    // assign offset to the beginning
    let mut partitions = consumer.assignment().unwrap();
    partitions.set_all_offsets(Offset::Beginning).unwrap();
    consumer.assign(&partitions).unwrap();

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
                        // println!("payload: {}", String::from_utf8_lossy(p));
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

    let aborted = Arc::new(AtomicBool::new(false));
    let aborted_cloned = aborted.clone();
    let mut join_set = execute(from, ipc_port, aborted_cloned, notify.clone()).await?;

    let port_pool = port_pool.clone();
    tokio::spawn(async move {
        tokio::select! {
            // application exit with error code
            status = async {
                while let Some(res) = join_set.join_next().await {
                    match res {
                        Ok(_) => {}
                        Err(err) => {
                            tracing::error!("Kafka worker exit with error: {:#}", err);
                            anyhow::bail!("Kafka worker exit with error: {:#}", err);
                        }
                    }
                }
                tracing::debug!("Kafka polling finished");
                Ok(())
            } => {
                match status {
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        match ipc.try_recv_error() {
                            Ok(res) => {
                                tracing::error!("IPC Error: {res}");
                                anyhow::bail!("Kafka worker exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("Kafka worker done successfully");
                                let _ = ipc.send(()).await;
                            }
                        }
                    }
                    Err(err) => {
                        let _ = ipc.send(());
                        anyhow::bail!("Kafka exit with error: {:#}", err);
                    }
                }
            },
            err = ipc.recv_error() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                aborted.store(true, std::sync::atomic::Ordering::Relaxed);
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
                aborted.store(true, std::sync::atomic::Ordering::Relaxed);
                join_set.abort_all();
            }
        }
        // send an empty tuple
        let _ = ipc.send(()).await;
        // stop the connector
        tracing::info!("Kafka task Done");
        ipc.close().await?;
        // put ipc port back to port pool.
        port_pool.put(ipc_port).await;
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    tracing::info!("Kafka task: {} stopped", task_id.unwrap_or(-1));
    Ok(())
}

type KafkaJoinSet = JoinSet<anyhow::Result<()>>;

async fn execute(
    from: Dsn,
    ipc_server_port: u16,
    aborted: Arc<AtomicBool>,
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

    // receive ACK from IPC
    consumers.spawn_blocking(move || {
        let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
        for ack in ack_reader {
            if !ack.success() {
                tracing::error!("Kafka write records error: {ack:?}");
                if let Some(message) = ack.message() {
                    anyhow::bail!("Kafka IPC writer error: {message}")
                }
            }
        }
        tracing::info!("Kafka ACK reader finished");
        Ok(())
    });

    let schema = build_schema();
    // multi producer(KafkaConsumer) and single consumer(IPC Writer)
    let (tx, rx) = flume::bounded(0);

    // IPC Writer
    let schema_clone = schema.clone();
    // polling from kafka and send to ipc writer
    consumers.spawn_blocking(move || {
        let mut writer = StreamWriter::try_new(stream, &schema_clone)?;

        let mut row_count = 0;
        let mut batches = 0;
        while let Ok(batch) = rx.recv() {
            writer.write(&batch)?;
            tracing::debug!("Kafka IPC Writer send {} rows", batch.num_rows());

            row_count += batch.num_rows();
            batches += 1;
        }
        let _ = writer.finish()?;
        tracing::info!(
            send.batches = batches,
            send.records = row_count,
            "Kafka IPC Writer finished, waiting for persisting"
        );
        anyhow::Ok(())
    });

    // kafka task config
    let config = KafkaTaskConfig::from_dsn(&from)?;

    let batch_size = config.advanced_options.batch_size.unwrap_or(1000);

    // split into sub tasks
    let sub_tasks: Vec<SubTask> = SubTask::build_tasks(config, notify.clone())?;
    for (idx, task) in sub_tasks.into_iter().enumerate() {
        let tx = tx.clone();
        let aborted = aborted.clone();
        let schema = schema.clone();
        let consumer = task.consumer;
        let timeout = task.timeout;

        consumers.spawn(poll_message(
            idx,
            consumer,
            tx,
            timeout,
            aborted,
            schema,
            batch_size,
            notify.clone(),
        ));
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
        let client_config = build_client_config(config.connect.clone());

        // create a base consumer
        let consumer: BaseConsumer = client_config.create().map_err(|err| {
            anyhow::anyhow!("failed to create consumer, cause: {}", err.to_string())
        })?;

        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .map_err(|err| {
                anyhow::anyhow!("failed to load meta data, cause: {}", err.to_string())
            })?;

        let mut topic_partitions: Vec<String> = Vec::new();

        // filter topics
        let topics_readable = metadata
            .topics()
            .iter()
            .filter(|tp| !tp.name().starts_with("__"))
            .filter(|tp| config.topics.contains(&tp.name().to_string()))
            .collect::<Vec<_>>();
        if topics_readable.len() != config.topics.len() {
            tracing::error!(
                "Some topics are not readable, expected: {:?}, actual: {:?}, please check your topic authorization",
                config.topics.len(),
                topics_readable.len());
            anyhow::bail!(
                    "Some topics are not readable, expected: {:?}, actual: {:?}, please check your topic authorization",
                    config.topics.len(),
                    topics_readable.len());
        }

        topics_readable.into_iter().for_each(|tp| {
            let topic_name = tp.name();
            let partitions: Vec<String> = tp
                .partitions()
                .iter()
                .map(|partition| format!("{}:{}", topic_name, partition.id()))
                .collect();
            topic_partitions.extend(partitions);
        });

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

        let mut concurrency = config
            .advanced_options
            .read_concurrency
            .unwrap_or(usize::MAX);
        if concurrency == 0 {
            concurrency = topic_partitions.len();
        }
        concurrency = cmp::min(concurrency, topic_partitions.len());

        // let chunk_size = topic_partitions.len().div_ceil(concurrency);
        let chunk_size = (topic_partitions.len() + concurrency - 1) / concurrency;

        let mut sub_tasks = Vec::new();
        for (index, chunk) in topic_partitions.chunks(chunk_size).enumerate() {
            // let mut topic_partitions: HashMap<String, Vec<i32>> = HashMap::new();
            let mut topic_partition_list = TopicPartitionList::new();
            for c in chunk {
                let mut parts = c.split(":");
                let topic = parts.next().unwrap().to_string();
                let partition = parts.next().unwrap().parse::<i32>().unwrap();

                topic_partition_list.add_partition(topic.as_str(), partition);
            }
            tracing::info!(
                "kafka consumer-{} assigned topic partitions: {:?}",
                index,
                topic_partitions
            );

            let consumer = consumer_builder(config.clone())?;
            consumer
                .assign(&topic_partition_list)
                .expect("Can't assign to specified topics");

            let sub_task = SubTask {
                consumer,
                timeout: config.timeout,
            };
            sub_tasks.push(sub_task);
        }
        Ok(sub_tasks)
    }
}

async fn poll_message(
    index: usize,
    consumer: LoggingConsumer,
    tx: flume::Sender<RecordBatch>,
    timeout: i64,
    aborted: Arc<AtomicBool>,
    schema: Schema,
    batch_size: usize,
    notify: crate::TaskNotifySender,
) -> anyhow::Result<()> {
    let mut last_polling = chrono::Utc::now().timestamp_millis();

    loop {
        // let message_sets = consumer.poll().context("Kafka polling error")?;
        if aborted.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::info!("Kafka consumer-{} cancelled", index);
            break;
        }

        let mut timestamp = TimestampNanosecondBuilder::new();
        let mut topic = StringBuilder::new();
        let mut partition = Int32Builder::new();
        let mut offset = Int64Builder::new();
        let mut key = BinaryBuilder::new();
        let mut value = BinaryBuilder::new();

        let mut read_chunks = consumer.stream().try_ready_chunks(batch_size);
        let fetch = read_chunks.try_next();

        match tokio::time::timeout(Duration::from_millis(timeout as u64), fetch).await? {
            Ok(chunk) => {
                if let Some(chunk) = chunk {
                    for msg in chunk {
                        match msg.payload_view::<str>() {
                            None => {}
                            Some(Ok(s)) => {
                                timestamp.append_value(Utc::now().timestamp_nanos_opt().unwrap());
                                topic.append_value(msg.topic());
                                partition.append_value(msg.partition());
                                offset.append_value(msg.offset());
                                key.append_value(msg.key().unwrap_or(&[]));
                                value.append_value(s);
                            }
                            Some(Err(e)) => {
                                tracing::warn!(
                                    "Error while deserializing message payload: {:?}",
                                    e
                                );
                            }
                        };
                    }
                    consumer
                        .commit_consumer_state(CommitMode::Async)
                        .map_err(|err| {
                            anyhow::anyhow!(
                                "failed to commit consumer state, cause: {}",
                                err.to_string()
                            )
                        })?;
                }
            }
            Err(err) => {
                let _ = notify.send(crate::TaskNotify::error(format!(
                    "failed to polling from kafka, cause: {}",
                    err.to_string()
                )));
                tracing::error!("failed to polling from kafka, cause: {}", err.to_string());
            }
        };

        if value.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = chrono::Utc::now().timestamp_millis();
            if timeout >= 0 && now - last_polling > timeout {
                tracing::info!("Kafka consumer-{} polling timeout", index);
                break;
            } else {
                continue;
            }
        }

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(timestamp.finish()),
                Arc::new(topic.finish()),
                Arc::new(partition.finish()),
                Arc::new(offset.finish()),
                Arc::new(key.finish()),
                Arc::new(value.finish()),
            ],
        )?;

        let batch_size = batch.num_rows();
        tx.send_async(batch).await?;

        tracing::debug!(
            "Kafka consumer-{} send batch to IPC Writer, batch size: {}",
            index,
            batch_size
        );

        last_polling = chrono::Utc::now().timestamp_millis();
    }
    Ok(())
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

/// A context can be used to change the behavior of producers and consumers by adding callbacks
/// that will be executed by librdkafka.
/// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        tracing::info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        tracing::info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        tracing::info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

fn consumer_builder(config: KafkaTaskConfig) -> anyhow::Result<LoggingConsumer> {
    let mut client = build_client_config(config.connect.clone());
    // Client identifier, default "rdkafka".
    if config.client_id.is_some() {
        client.set("client.id", config.client_id.unwrap());
    }
    // All clients sharing the same group.id belong to the same group.
    client.set("group.id", config.group);
    // Action to take when there is no initial offset in offset store or the desired offset is out of range.
    // smallest, earliest, beginning, largest, latest, end, error
    client.set("auto.offset.reset", config.fallback_offset);
    // Maximum time the broker may wait to fill the Fetch response with fetch.min.bytes of messages.
    if config.fetch_max_wait_time.is_some() {
        client.set(
            "fetch.wait.max.ms",
            config.fetch_max_wait_time.unwrap().as_millis().to_string(),
        );
    }
    // Minimum number of bytes the broker responds with.
    if config.fetch_min_bytes.is_some() {
        client.set(
            "fetch.min.bytes",
            config.fetch_min_bytes.unwrap().to_string(),
        );
    }
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
        .create_with_context(CustomContext)
        .expect("Consumer creation failed");
    Ok(consumer)
}

fn build_client_config(config: KafkaConnectConfig) -> ClientConfig {
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
            client_config.set("sasl.mechanisms", sasl_mechanism);
        }
        if let Some(sasl_username) = config.sasl_username {
            client_config.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = config.sasl_password {
            client_config.set("sasl.password", sasl_password);
        }
    }

    client_config
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
        let client_config: ClientConfig = build_client_config(config.clone());
        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|err| anyhow::anyhow!("failed to create consumer, cause: {}", err.to_string()))
            .unwrap();
        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .map_err(|err| anyhow::anyhow!("failed to load meta data, cause: {}", err.to_string()))
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
        let client_config: ClientConfig = build_client_config(config.clone());
        // create a base consumer
        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|err| anyhow::anyhow!("failed to create consumer, cause: {}", err.to_string()))
            .unwrap();
        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .map_err(|err| anyhow::anyhow!("failed to load meta data, cause: {}", err.to_string()))
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
