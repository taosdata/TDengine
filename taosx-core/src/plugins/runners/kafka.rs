use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};
use anyhow::Context;
use arrow::array::{
    BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use kafka::client::{KafkaClient, SecurityConfig};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message, MessageSet};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use taos::Dsn;
use taosx_ipc::prelude::ArrowDataType;
use tokio_util::sync::CancellationToken;

async fn kafka_worker(mut from: Dsn, port: u16) -> anyhow::Result<()> {
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    let schema = build_schema();

    let mut writer = StreamWriter::try_new(&stream, &schema)?;

    let mut consumer = build_consumer(&mut from)?;
    let timeout = parse_timeout(&from);
    let mut start = chrono::Utc::now().timestamp_millis();
    loop {
        let message_sets = consumer.poll().context("Kafka polling error")?;
        if message_sets.is_empty() {
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            let now = chrono::Utc::now().timestamp_millis();
            if timeout >= 0 && now - &start > timeout {
                break;
            } else {
                continue;
            }
        }

        let mut timestamp = TimestampNanosecondBuilder::new();
        let mut topic = StringBuilder::new();
        let mut partition = Int32Builder::new();
        let mut offset = Int64Builder::new();
        let mut key = BinaryBuilder::new();
        let mut value = BinaryBuilder::new();


        for ms in message_sets.iter() {
            for m in ms.messages() {
                let ts = chrono::Utc::now().timestamp_nanos();
                let default_print_value = String::from("false");
                let print_value: bool = from.params.get("print_value").unwrap_or(&default_print_value).parse().unwrap();
                if print_value {
                    print_message(&ms, &m, &ts);
                }

                timestamp.append_value(ts);
                topic.append_value(ms.topic());
                partition.append_value(ms.partition());
                offset.append_value(m.offset.clone());
                key.append_value(m.key.clone());
                value.append_value(m.value.clone());
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
            writer.write(&batch)?;
            tokio::task::yield_now().await;

            consumer.consume_messageset(ms)?;
        }


        consumer.commit_consumed()?;
        start = chrono::Utc::now().timestamp_millis();
    }

    println!("{} kafka_to_taos stopped", chrono::Utc::now().to_string());
    Ok(())
}

pub async fn kafka_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    _: Vec<Action>,
    to: Dsn,
    _: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    println!(
        "{} kafka_to_taos started, from: {}, to: {}",
        chrono::Utc::now().to_string(),
        from.to_string(),
        to.to_string()
    );
    let port = port_pool
        .get()
        .ok_or_else(|| anyhow::format_err!("No available port for Kafka connection"))?;
    let socket = format!("127.0.0.1:{}", port);
    let (abort, mut closed) = build_ipc(&socket, parser, &to, &cancel, with_agent, transferred)?;

    let worker = tokio::spawn(kafka_worker(from, port));
    let abort_handle = worker.abort_handle();

    let port_pool = port_pool.clone();

    tokio::spawn(async move {
        tokio::select! {
            // application exit with error code
            status = worker => {
                match status? {
                    Ok(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        match closed.try_recv() {
                            Ok(res) => {
                                tracing::error!("IPC Error: {res}");
                                anyhow::bail!("Kafka worker exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("Kafka worker done successfully");
                                let _ = abort.send(());
                            }
                        }
                    }
                    Err(err) => {
                        let _ = abort.send(());
                        anyhow::bail!("Kafka exit with error: {:#}", err);
                    }
                }
            },
            err = closed.recv() => {
                tracing::info!("have received worker thread panicked message, terminate child process");
                abort_handle.abort();
                if let Some(err) = err {
                    let _ = abort.send(());
                    abort_handle.abort();
                    anyhow::bail!("Kafka writer error: {err:#}");
                }
            },
            _ = cancel.cancelled() => {
                tracing::info!("Kafka task cancelled");
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = abort.send(());
        // stop the connector
        tracing::info!("Kafka task Done");
        // put ipc port back to port pool.
        port_pool.put(port);
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

fn build_schema() -> Schema {
    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("none"));
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

fn build_consumer(dsn: &mut Dsn) -> Result<Consumer, KafkaConfigError> {
    let use_ssl_default = String::from("false");
    let use_ssl = dsn.params.get("use_ssl").unwrap_or(&use_ssl_default);
    let use_ssl = use_ssl.parse().unwrap_or(false);
    let mut builder: kafka::consumer::Builder;
    if use_ssl {
        builder = build_ssl_builder(dsn)?;
    } else {
        builder = build_builder(dsn);
    }

    let default_group = String::from("");
    let group = dsn.params.get("group").unwrap_or(&default_group);
    builder = builder.with_group(group.to_string());

    if dsn.params.contains_key("topics") {
        let topic = dsn.params.get("topics");
        for t in topic.unwrap().split(",") {
            builder = builder.with_topic(t.to_string());
        }
    }

    if dsn.params.contains_key("topic_partitions") {
        let topic_partitions = dsn.params.get("topic_partitions");
        for tp in topic_partitions.unwrap().split(",") {
            if tp.contains(":") {
                let topic_partition = tp.split(":").collect::<Vec<&str>>();
                let topic = topic_partition[0];
                let partition = topic_partition[1];
                if partition.contains("..") {
                    let partition_range = partition.split("..").collect::<Vec<&str>>();
                    let start = partition_range[0].parse::<i32>().unwrap();
                    let end = partition_range[1].parse::<i32>().unwrap();
                    if start > end {
                        panic!("invalid partition range: {}", partition);
                    }
                    let partitions = (start..=end).collect::<Vec<i32>>();
                    builder = builder.with_topic_partitions(topic.to_string(), &partitions);
                } else {
                    let partition = partition.parse::<i32>().unwrap();
                    builder = builder.with_topic_partitions(topic.to_string(), &[partition]);
                }
            } else {
                builder = builder.with_topic(tp.to_string());
            }
        }
    }

    let fallback_offset = parse_fallback_offset(dsn.params.get("fallback_offset"));
    builder = builder.with_fallback_offset(fallback_offset);

    let offset_storage = parse_offset_storage(dsn.params.get("offset_storage"));
    builder = builder.with_offset_storage(offset_storage);

    let consumer = builder.create()?;
    Ok(consumer)
}

fn build_builder(dsn: &Dsn) -> kafka::consumer::Builder {
    let bootstrap_servers = parse_bootstrap_servers(dsn);
    let builder = Consumer::from_hosts(bootstrap_servers);
    builder
}

#[derive(Debug, thiserror::Error)]
enum KafkaConfigError {
    #[error("Kafka source CA config read error, cause: {0}")]
    CAConfigReadError(String),
    #[error(transparent)]
    KafkaConsumerError(#[from] kafka::Error),
    // #[error("Kafka source config parse error, cause: {0}")]
    // KafkaSourceConfigParseError(String),
}

fn build_ssl_builder(dsn: &mut Dsn) -> Result<kafka::consumer::Builder, KafkaConfigError> {
    let bootstrap_servers = parse_bootstrap_servers(dsn);

    let cert_key = super::mqtt::get_string_from_param_or_file(dsn, "cert_key", true, None)
        .map_err(|s| KafkaConfigError::CAConfigReadError(s))?;
    let cert = super::mqtt::get_string_from_param_or_file(dsn, "cert", true, None)
        .map_err(|s| KafkaConfigError::CAConfigReadError(s))?;

    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_cipher_list("DEFAULT").unwrap();
    builder
        .set_certificate_file(cert.unwrap(), SslFiletype::PEM)
        .unwrap();
    builder
        .set_private_key_file(cert_key.unwrap(), SslFiletype::PEM)
        .unwrap();
    builder.check_private_key().unwrap();
    builder.set_default_verify_paths().unwrap();
    builder.set_verify(SslVerifyMode::PEER);
    let connector = builder.build();

    let mut client = KafkaClient::new_secure(bootstrap_servers, SecurityConfig::new(connector));
    match client.load_metadata_all() {
        Err(e) => {
            //TODO: handle error
            println!("Error: {:?}", e);
        }
        Ok(_) => {
            if client.topics().len() == 0 {
                println!("No topics available");
            }
        }
    }

    let builder = Consumer::from_client(client);
    Ok(builder)
}

fn parse_bootstrap_servers(dsn: &Dsn) -> Vec<String> {
    let mut bootstrap_servers = Vec::new();
    for address in dsn.addresses.iter() {
        bootstrap_servers.push(format!(
            "{}:{}",
            address.host.clone().unwrap(),
            address.port.clone().unwrap()
        ));
    }
    bootstrap_servers
}

#[allow(dead_code)]
fn print_message(ms: &MessageSet, m: &Message, ts: &i64) {
    println!(
        "topic: {}, partition: {}, offset: {},ts: {}, key: {}, values: {}",
        ms.topic(),
        ms.partition(),
        m.offset,
        ts,
        String::from_utf8_lossy(m.key),
        String::from_utf8_lossy(m.value)
    );
}

fn parse_timeout(dsn: &Dsn) -> i64 {
    let default_timeout = String::from("500");
    let timeout = dsn.params.get("timeout").unwrap_or(&default_timeout);
    if timeout == "never" {
        return -1;
    }
    timeout.parse::<i64>().unwrap()
}

fn parse_fallback_offset(fallback_offset: Option<&String>) -> FetchOffset {
    if fallback_offset.is_none() {
        return FetchOffset::Earliest;
    }

    let fallback_offset = fallback_offset.unwrap();
    if fallback_offset.eq(&String::from("Earliest")) {
        return FetchOffset::Earliest;
    }

    if fallback_offset.eq(&String::from("Latest")) {
        return FetchOffset::Latest;
    }

    if fallback_offset.parse::<i64>().is_ok() {
        return FetchOffset::ByTime(fallback_offset.parse::<i64>().unwrap());
    }

    panic!("invalid fallback_offset: {}", fallback_offset);
}

fn parse_offset_storage(offset_storage: Option<&String>) -> GroupOffsetStorage {
    if offset_storage.is_none() {
        return GroupOffsetStorage::Kafka;
    }

    let offset_storage = offset_storage.unwrap();
    if offset_storage.eq(&String::from("Kafka")) {
        return GroupOffsetStorage::Kafka;
    }

    if offset_storage.eq(&String::from("Zookeeper")) {
        return GroupOffsetStorage::Zookeeper;
    }

    panic!("invalid offset_storage: {}", offset_storage);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::str::FromStr;
    use taosx_ipc::prelude::ArrowDataType;

    #[test]
    fn test_arrow() {
        let mut metadata = HashMap::new();
        metadata.insert(String::from("version"), String::from("1.0"));
        metadata.insert(String::from("stream"), String::from("flat"));
        metadata.insert(String::from("ack"), String::from("none"));
        let flat_columns = vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("topic", ArrowDataType::Utf8, false),
            Field::new("qos", ArrowDataType::UInt8, false),
            Field::new("payload", ArrowDataType::Binary, false),
        ];
        let record_list = DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None);
        let schema = Schema::new(flat_columns).with_metadata(metadata);
    }

    #[test]
    fn test_topics() {
        let mut dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topics=tp2").unwrap();
        let consumer = build_consumer(&mut dsn).unwrap();
        assert_eq!("", consumer.group());
        let subscriptions = consumer.subscriptions();
        assert_eq!(1, subscriptions.len());

        let mut dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topics=tp1,tp2").unwrap();
        let consumer = build_consumer(&mut dsn).unwrap();
        assert_eq!("", consumer.group());
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
    }

    #[test]
    #[should_panic]
    fn test_topics_invalid() {
        let mut dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topics=invalid").unwrap();
        let consumer = build_consumer(&mut dsn);
    }

    #[test]
    fn test_topic_partitions() {
        let mut dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1,tp2").unwrap();
        let consumer = build_consumer(&mut dsn).unwrap();
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(5, subscriptions.get("tp1").unwrap().len());
        println!("{:?}", subscriptions);

        let mut dsn =
            Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:1,tp2").unwrap();
        let consumer = build_consumer(&mut dsn).unwrap();
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(1, subscriptions.get("tp1").unwrap().len());
        println!("{:?}", subscriptions);

        let mut dsn =
            Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:0..2,tp2&group=test")
                .unwrap();
        let consumer = build_consumer(&mut dsn).unwrap();
        assert_eq!("test", consumer.group());
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(3, subscriptions.get("tp1").unwrap().len());
        println!("{:?}", subscriptions);
    }

    #[test]
    #[should_panic]
    fn test_topic_partitions_invalid() {
        let mut dsn =
            Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:2..1,tp2").unwrap();
        let consumer = build_consumer(&mut dsn);
    }

    #[test]
    fn test_parse_timeout() {
        let dsn = Dsn::from_str("kafka://localhost:9092?timeout=99999").unwrap();
        let result = parse_timeout(&dsn);
        assert_eq!(99999, result);

        let dsn = Dsn::from_str("kafka://localhost:9092?timeout=never").unwrap();
        let result = parse_timeout(&dsn);
        assert_eq!(-1, result);
    }

    #[test]
    #[should_panic]
    fn test_parse_timeout_invalid() {
        let dsn = Dsn::from_str("kafka://localhost:9092?timeout=invalid").unwrap();
        let result = parse_timeout(&dsn);
    }

    #[test]
    fn test_parse_fallback_offset() {
        // Earliest
        let fallback_offset = Some(String::from("Earliest"));
        let result = parse_fallback_offset(fallback_offset.as_ref());
        assert_eq!("Earliest", format!("{result:?}"));

        // Latest
        let fallback_offset = Some(String::from("Latest"));
        let result = parse_fallback_offset(fallback_offset.as_ref());
        assert_eq!("Latest", format!("{result:?}"));

        // ByTime
        let fallback_offset = Some(String::from("1600000000000"));
        let result = parse_fallback_offset(fallback_offset.as_ref());
        assert_eq!("ByTime(1600000000000)", format!("{result:?}"));
    }

    #[test]
    #[should_panic]
    fn test_parse_fallback_offset_invalid() {
        // invalid
        let fallback_offset = Some(String::from("invalid"));
        let result = parse_fallback_offset(fallback_offset.as_ref());
    }

    #[test]
    fn test_parse_offset_storage() {
        // Kafka
        let offset_storage = Some(String::from("Kafka"));
        let result = parse_offset_storage(offset_storage.as_ref());
        assert_eq!("Kafka", format!("{result:?}"));

        // Zookeeper
        let offset_storage = Some(String::from("Zookeeper"));
        let result = parse_offset_storage(offset_storage.as_ref());
        assert_eq!("Zookeeper", format!("{result:?}"));
    }

    #[test]
    #[should_panic]
    fn test_parse_offset_storage_invalid() {
        // invalid
        let offset_storage = Some(String::from("invalid"));
        let result = parse_offset_storage(offset_storage.as_ref());
    }
}
