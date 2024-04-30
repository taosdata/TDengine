use std::cmp;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use arrow::array::{
    ArrayBuilder, BinaryBuilder, Int32Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use futures::future;
use futures::stream::StreamExt;
use kafka::consumer::{Builder, GroupOffsetStorage};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::Timeout;
use rhai::packages::BasicArrayPackage;
use taos::Dsn;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use taosx_ipc::ack::AckReaderBuilder;
use taosx_ipc::prelude::ArrowDataType;

use crate::plugins::dsv::DataSourceValidation;
use crate::runners::kafka::config::connect::KafkaConnectConfig;
use crate::runners::kafka::config::KafkaTaskConfig;
use crate::runners::set_tcp_keepalive;
use crate::utils::port_pool::PortPool;
use crate::{build, build_ipc, Action, Parser, Transferred};

mod config;

pub const KAFKA_ID: &str = "kafka";

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = KafkaTaskConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            KAFKA_ID.to_string(),
            format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            // let mut client = KafkaClient::new(c.connect.bootstrap_servers);
            // let result = client.load_metadata_all();
            let client = build_client(c.connect.clone()).expect("Client creation failed");
            let consumer: BaseConsumer = client.create().expect("Consumer creation failed");
            let result = consumer.fetch_metadata(None, Duration::from_secs(5));
            match result {
                Ok(_) => DataSourceValidation::valid(KAFKA_ID.to_string(), None),
                Err(err) => DataSourceValidation::invalid(
                    KAFKA_ID.to_string(),
                    format!("failed to connect to kafka, cause: {}", err.to_string()),
                ),
            }
        }
    }
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
        &cancel,
        with_agent,
        transferred,
        span,
        task_id.clone(),
        notify,
    )
    .await?;

    let aborted = Arc::new(AtomicBool::new(false));
    let aborted_cloned = aborted.clone();
    let worker = tokio::spawn(execute(from, ipc_port, aborted_cloned));
    let abort_handle = worker.abort_handle();

    let port_pool = port_pool.clone();
    tokio::spawn(async move {
        tokio::select! {
            // application exit with error code
            status = worker => {
                match status? {
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
                abort_handle.abort();
                if let Some(err) = err {
                    let _ = ipc.send(()).await;
                    let _ = ipc.close().await;
                    abort_handle.abort();
                    anyhow::bail!("Kafka writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("Kafka task cancelled");
                aborted.store(true, std::sync::atomic::Ordering::Relaxed);
                abort_handle.abort();
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

async fn execute(from: Dsn, ipc_server_port: u16, aborted: Arc<AtomicBool>) -> anyhow::Result<()> {
    let ipc_server = format!("127.0.0.1:{}", ipc_server_port);

    // ipc writer stream
    let stream = std::net::TcpStream::connect(ipc_server)?;
    set_tcp_keepalive(&stream)?;
    stream.set_read_timeout(None)?;

    // ack reader stream
    let ack_stream = stream.try_clone()?;
    set_tcp_keepalive(&ack_stream)?;
    ack_stream.set_read_timeout(None)?;

    // receive ACK from IPC
    let ack = tokio::task::spawn_blocking(move || {
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
    let ipc_writer = tokio::task::spawn_blocking(move || {
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
    // split into sub tasks
    let sub_tasks: Vec<SubTask> = SubTask::from_kafka_config(config)?;
    // polling from kafka and send to ipc writer
    let mut consumers = Vec::new();
    for (idx, task) in sub_tasks.into_iter().enumerate() {
        let tx = tx.clone();
        let aborted = aborted.clone();
        let schema = schema.clone();
        let consumer = task.consumer;
        let timeout = task.timeout;

        let sub_task = tokio::spawn(async move {
            let _ = poll_message(idx, consumer, tx, timeout, aborted, schema).await;
        });
        consumers.push(sub_task);
    }

    drop(tx);
    for c in consumers {
        c.await?;
    }
    tracing::debug!("Kafka polling finished");
    ack.await??;
    tracing::debug!("Kafka ACK reader finished");
    ipc_writer.await??;
    tracing::debug!("Kafka IPC Writer finished");
    Ok(())
}

struct SubTask {
    consumer: LoggingConsumer,
    timeout: i64,
}

impl SubTask {
    pub fn from_kafka_config(config: KafkaTaskConfig) -> anyhow::Result<Vec<Self>> {
        let client = build_client(config.connect.clone())?;
        // client.load_metadata_all()?;

        // create a base consumer
        let consumer: BaseConsumer = client.create().expect("Consumer creation failed");
        // fetch metadata
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .expect("Failed to fetch metadata");

        let topics = config.topics.clone();
        let mut topic_partitions: Vec<String> = Vec::new();
        metadata
            .topics()
            .iter()
            .filter(|tp| !tp.name().starts_with("__"))
            .filter(|tp| topics.contains(&tp.name().to_string()))
            .for_each(|tp| {
                for partition in tp.partitions() {
                    topic_partitions.push(format!("{}:{}", tp.name(), partition.id()))
                }
            });
        if topic_partitions.is_empty() {
            bail!("no invalid topic, topics: {:?}", topics);
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
                // if topic_partitions.contains_key(&topic) {
                //     topic_partitions.get_mut(&topic).unwrap().push(partition);
                // } else {
                //     topic_partitions.insert(topic, vec![partition]);
                // }
                topic_partition_list.add_partition(topic.as_str(), partition);
            }
            tracing::info!(
                "kafka consumer-{} assigned topic partitions: {:?}",
                index,
                topic_partitions
            );

            // let mut builder = consumer_builder(config.clone())?;
            // for (topic, partitions) in topic_partitions {
            //     builder = builder.with_topic_partitions(topic, partitions.as_slice());
            // }
            // let consumer = builder.create().map_err(|err| {
            //     anyhow::format_err!("Kafka consumer-{} create error: {:?}", index, err)
            // })?;

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
    mut consumer: LoggingConsumer,
    tx: flume::Sender<RecordBatch>,
    timeout: i64,
    aborted: Arc<AtomicBool>,
    schema: Schema,
) -> anyhow::Result<()> {
    let mut last_polling = chrono::Utc::now().timestamp_millis();
    loop {
        // let message_sets = consumer.poll().context("Kafka polling error")?;
        if aborted.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::info!("Kafka consumer-{} cancelled", index);
            break;
        }

        // if message_sets.is_empty() {
        //     tokio::time::sleep(Duration::from_millis(100)).await;
        //     let now = chrono::Utc::now().timestamp_millis();
        //     if timeout >= 0 && now - &last_polling > timeout {
        //         tracing::info!("Kafka consumer-{} polling timeout", index);
        //         break;
        //     } else {
        //         continue;
        //     }
        // }

        let mut timestamp = TimestampNanosecondBuilder::new();
        let mut topic = StringBuilder::new();
        let mut partition = Int32Builder::new();
        let mut offset = Int64Builder::new();
        let mut key = BinaryBuilder::new();
        let mut value = BinaryBuilder::new();

        let _ = consumer
            .stream()
            .take(100)
            .for_each(|message| {
                match message {
                    Err(e) => tracing::warn!("Kafka error: {}", e),
                    Ok(m) => {
                        match m.payload_view::<str>() {
                            None => {}
                            Some(Ok(s)) => {
                                // if let Some(headers) = m.headers() {
                                //     for header in headers.iter() {
                                //         tracing::info!(
                                //             "Header {:#?}: {:?}",
                                //             header.key,
                                //             header.value
                                //         );
                                //     }
                                // }
                                timestamp.append_value(Utc::now().timestamp_nanos_opt().unwrap());
                                topic.append_value(m.topic());
                                partition.append_value(m.partition());
                                offset.append_value(m.offset());
                                key.append_value(m.key().unwrap_or(&[]));
                                value.append_value(s);
                                // commit offset
                                consumer.commit_message(&m, CommitMode::Async).unwrap();
                            }
                            Some(Err(e)) => {
                                tracing::warn!(
                                    "Error while deserializing message payload: {:?}",
                                    e
                                );
                            }
                        };
                    }
                };
                future::ready(())
            })
            .await;

        // for ms in message_sets.iter() {
        //     for m in ms.messages() {
        //         let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap();

        //         timestamp.append_value(ts);
        //         topic.append_value(ms.topic());
        //         partition.append_value(ms.partition());
        //         offset.append_value(m.offset.clone());
        //         key.append_value(m.key);
        //         value.append_value(m.value);
        //     }
        //     consumer.consume_messageset(ms)?;
        // }

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
        // consumer.commit_consumed()?;

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

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
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
    let mut client = build_client(config.connect.clone())?;
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

// use rdkafka::ClientConfig;
fn build_client(connect: KafkaConnectConfig) -> anyhow::Result<ClientConfig> {
    let mut client = ClientConfig::new();
    // set bootstrap servers
    client.set("bootstrap.servers", connect.bootstrap_servers.join(","));
    // security.protocol: plaintext, ssl, sasl_plaintext, sasl_ssl
    if connect.use_ssl && connect.use_sasl {
        client.set("security.protocol", "sasl_ssl");
    } else if connect.use_ssl {
        client.set("security.protocol", "ssl");
    } else if connect.use_sasl {
        client.set("security.protocol", "sasl_plaintext");
    } else {
        client.set("security.protocol", "plaintext");
    }
    // ssl settings
    if connect.use_ssl {
        if let Some(ca_cert) = connect.ca_cert {
            client.set("ssl.ca.pem", ca_cert);
        }
        if let Some(client_cert) = connect.client_cert {
            client.set("ssl.certificate.pem", client_cert);
        }
        if let Some(client_key) = connect.client_key {
            client.set("ssl.key.pem", client_key);
        }
    }
    // sasl settings
    if connect.use_sasl {
        if let Some(sasl_mechanism) = connect.sasl_mechanism {
            client.set("sasl.mechanisms", sasl_mechanism);
        }
        if let Some(sasl_username) = connect.sasl_username {
            client.set("sasl.username", sasl_username);
        }
        if let Some(sasl_password) = connect.sasl_password {
            client.set("sasl.password", sasl_password);
        }
    }
    Ok(client.clone())
}

// fn consumer_builder(config: KafkaTaskConfig) -> anyhow::Result<Builder> {
//     let mut client = build_client(config.connect)?;
//     client.load_metadata_all()?;

//     let mut builder = Consumer::from_client(client);
//     // group
//     builder = builder.with_group(config.group);

//     // fallback_offset
//     builder = builder.with_fallback_offset(config.fallback_offset);

//     // offset_storage: use Kafka as Default
//     builder = builder.with_offset_storage(Some(GroupOffsetStorage::Kafka));

//     if config.fetch_max_wait_time.is_some() {
//         builder = builder.with_fetch_max_wait_time(config.fetch_max_wait_time.unwrap());
//     }
//     if config.fetch_min_bytes.is_some() {
//         builder = builder.with_fetch_min_bytes(config.fetch_min_bytes.unwrap());
//     }
//     if config.fetch_max_bytes_per_partition.is_some() {
//         builder = builder
//             .with_fetch_max_bytes_per_partition(config.fetch_max_bytes_per_partition.unwrap());
//     }
//     if config.fetch_crc_validation.is_some() {
//         builder = builder.with_fetch_crc_validation(config.fetch_crc_validation.unwrap());
//     }
//     if config.offset_storage.is_some() {
//         builder = builder.with_offset_storage(config.offset_storage);
//     }
//     if config.retry_max_bytes_limit.is_some() {
//         builder = builder.with_retry_max_bytes_limit(config.retry_max_bytes_limit.unwrap());
//     }
//     if config.connection_idle_timeout.is_some() {
//         builder = builder.with_connection_idle_timeout(config.connection_idle_timeout.unwrap());
//     }
//     if config.client_id.is_some() {
//         builder = builder.with_client_id(config.client_id.unwrap());
//     }

//     Ok(builder)
// }

// // use kafka::client::KafkaClient;
// fn build_client(connect: KafkaConnectConfig) -> anyhow::Result<KafkaClient> {
//     let client = if connect.use_ssl {
//         let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
//         builder.set_cipher_list("DEFAULT")?;
//         builder.set_verify(SslVerifyMode::PEER);
//         if let (Some(ccert), Some(ckey)) = (connect.client_cert, connect.client_key) {
//             tracing::info!("loading cert-file={}, key-file={}", ccert, ckey);

//             builder
//                 .set_certificate_file(ccert, SslFiletype::PEM)
//                 .unwrap();
//             builder
//                 .set_private_key_file(ckey, SslFiletype::PEM)
//                 .unwrap();
//             builder.check_private_key().unwrap();
//         }

//         if let Some(ca_cert) = connect.ca_cert {
//             tracing::info!("loading ca-file={}", ca_cert);
//             builder.set_ca_file(ca_cert).unwrap();
//         } else {
//             // ~ allow client specify the CAs through the default paths:
//             // "These locations are read from the SSL_CERT_FILE and
//             // SSL_CERT_DIR environment variables if present, or defaults
//             // specified at OpenSSL build time otherwise."
//             builder.set_default_verify_paths().unwrap();
//         }
//         let connector = builder.build();

//         // ~ instantiate KafkaClient with the previous OpenSSL setup
//         let client = KafkaClient::new_secure(
//             connect.bootstrap_servers,
//             SecurityConfig::new(connector).with_hostname_verification(false),
//         );

//         client
//     } else {
//         KafkaClient::new(connect.bootstrap_servers)
//     };

//     Ok(client)
// }

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_invalid() {
        let dsn = Dsn::from_str("kafka://127.0.0.1:9092").unwrap();
        let result = is_valid(&dsn).await;
        assert_eq!(false, result.valid);
        assert_eq!(false, result.support);
        assert_eq!(KAFKA_ID, result.data_source);
        assert_eq!(
            "failed to connect to kafka, cause: No host reachable",
            result.message.unwrap()
        );
    }

    #[ignore]
    #[tokio::test]
    async fn test_valid() {
        let dsn = Dsn::from_str("kafka://192.168.1.92:9092").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!(KAFKA_ID, dsv.data_source);
        assert_eq!(None, dsv.version);

        let dsn = Dsn::from_str("kafka://192.168.1.92:9092,jf92:9092").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!(KAFKA_ID, dsv.data_source);
        assert_eq!(None, dsv.version);

        let dsn = Dsn::from_str("kafka://127.0.0.1:9092,jf92:9092").unwrap();
        let dsv = is_valid(&dsn).await;
        assert_eq!(true, dsv.valid);
        assert_eq!(true, dsv.support);
        assert_eq!(KAFKA_ID, dsv.data_source);
        assert_eq!(None, dsv.version);
    }

    #[test]
    fn test_build_client() {
        let dsn = Dsn::from_str("kafka://192.168.2.19:9093?ca=/data/ypzhang/kafka-ca/ca.pem&cert=/data/ypzhang/kafka-ca/client.pem&cert_key=/data/ypzhang/kafka-ca/client-key.pem&topics=test&fallback_offset=Earliest&read_concurrency=0").unwrap();
        let config = KafkaConnectConfig::from_dsn(&dsn).unwrap();
        let client = build_client(config).unwrap();

        assert_eq!(
            "192.168.2.19:9093",
            client.get("bootstrap.servers").unwrap()
        );

        let consumer: BaseConsumer = client.create().expect("Consumer creation failed");
        let result = consumer.fetch_metadata(None, Duration::from_secs(5));
    }
}
