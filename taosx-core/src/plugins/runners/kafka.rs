use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
use taos::Dsn;
use tokio_util::sync::CancellationToken;

use taosx_ipc::prelude::ArrowDataType;
use tracing::Span;

use crate::{Action, build_ipc, Parser, Transferred};
use crate::utils::port_pool::PortPool;

async fn kafka_worker(mut from: Dsn, port: u16) -> anyhow::Result<()> {
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    let schema = build_schema();
    let mut writer = StreamWriter::try_new(&stream, &schema)?;

    let mut consumer = build_consumer(&mut from)?;
    let timeout = parse_timeout(&from)?;
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
                let print_value: bool = from
                    .params
                    .get("print_value")
                    .unwrap_or(&default_print_value)
                    .parse()?;
                if print_value {
                    print_message(&ms, &m, &ts);
                }

                timestamp.append_value(ts);
                topic.append_value(ms.topic());
                partition.append_value(ms.partition());
                offset.append_value(m.offset.clone());
                key.append_value(m.key);
                value.append_value(m.value);
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
    span: Span,
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
    let mut ipc = build_ipc(
        &socket,
        parser,
        &to,
        Some("kafka"),
        None,
        &cancel,
        with_agent,
        transferred,
        span,
    )
    .await?;

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
                        match ipc.try_recv_error() {
                            Ok(res) => {
                                tracing::error!("IPC Error: {res}");
                                anyhow::bail!("Kafka worker exit with IPC error: {res}");
                            }
                            Err(_) => {
                                tracing::info!("Kafka worker done successfully");
                                let _ = ipc.send(());
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
                abort_handle.abort();
                if let Some(err) = err {
                    let _ = ipc.send(());
                    let _ = ipc.close().await;
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
        let _ = ipc.send(());
        // stop the connector
        tracing::info!("Kafka task Done");
        ipc.close().await?;
        // put ipc port back to port pool.
        port_pool.put(port);
        // wait for completion
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }).await??;

    Ok(())
}

pub async fn is_kafka_available(dsn: &Dsn) -> anyhow::Result<bool> {
    build_consumer(dsn)?;
    Ok(true)
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

fn build_consumer(dsn: &Dsn) -> anyhow::Result<Consumer> {
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
                    let start = partition_range[0].parse::<i32>()?;
                    let end = partition_range[1].parse::<i32>()?;
                    if start > end {
                        let msg = format!("invalid partition range: {}", partition);
                        return Err(KafkaSourceError::InvalidParameterError(msg))?;
                    }
                    let partitions = (start..=end).collect::<Vec<i32>>();
                    builder = builder.with_topic_partitions(topic.to_string(), &partitions);
                } else {
                    let partition = partition.parse::<i32>()?;
                    builder = builder.with_topic_partitions(topic.to_string(), &[partition]);
                }
            } else {
                builder = builder.with_topic(tp.to_string());
            }
        }
    }

    let fallback_offset =
        parse_fallback_offset(dsn.params.get("fallback_offset").map(String::as_str))?;
    builder = builder.with_fallback_offset(fallback_offset);

    let offset_storage =
        parse_offset_storage(dsn.params.get("offset_storage").map(String::as_str))?;
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
enum KafkaSourceError {
    #[error("invalid parameter error, cause: {0}")]
    InvalidParameterError(String),

    #[error("Kafka source CA config read error, cause: {0}")]
    CAConfigReadError(String),

    #[error(transparent)]
    KafkaError(#[from] kafka::Error),
}

fn build_ssl_builder(dsn: &Dsn) -> anyhow::Result<kafka::consumer::Builder> {
    let bootstrap_servers = parse_bootstrap_servers(dsn);

    let mut dsn_copy = dsn.clone();

    let cert_key =
        super::mqtt::get_string_from_param_or_file(&mut dsn_copy, "cert_key", true, None)
            .map_err(|s| KafkaSourceError::CAConfigReadError(s))?;
    let cert = super::mqtt::get_string_from_param_or_file(&mut dsn_copy, "cert", true, None)
        .map_err(|s| KafkaSourceError::CAConfigReadError(s))?;

    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_cipher_list("DEFAULT")?;
    builder.set_certificate_file(cert.unwrap(), SslFiletype::PEM)?;
    builder.set_private_key_file(cert_key.unwrap(), SslFiletype::PEM)?;
    builder.check_private_key()?;
    builder.set_default_verify_paths()?;
    builder.set_verify(SslVerifyMode::PEER);
    let connector = builder.build();

    let mut client = KafkaClient::new_secure(bootstrap_servers, SecurityConfig::new(connector));
    client.load_metadata_all()?;

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

fn parse_timeout(dsn: &Dsn) -> anyhow::Result<i64> {
    let default_timeout = String::from("500");
    let timeout = dsn.params.get("timeout").unwrap_or(&default_timeout);
    if timeout == "never" {
        return Ok(-1);
    }
    timeout
        .parse::<i64>()
        .map_err(|e| anyhow::anyhow!("invalid timeout: {}, cause: {}", timeout, e))
}

fn parse_fallback_offset(fallback_offset: Option<&str>) -> anyhow::Result<FetchOffset> {
    match fallback_offset {
        Some("Earliest") | None => Ok(FetchOffset::Earliest),
        Some("Latest") => Ok(FetchOffset::Latest),
        Some(s) => s
            .parse::<i64>()
            .map(FetchOffset::ByTime)
            .map_err(|e| anyhow::anyhow!("invalid fallback_offset: {}, cause: {}", s, e)),
    }
}

fn parse_offset_storage(offset_storage: Option<&str>) -> anyhow::Result<GroupOffsetStorage> {
    match offset_storage {
        Some("Kafka") | None => Ok(GroupOffsetStorage::Kafka),
        Some("Zookeeper") => Ok(GroupOffsetStorage::Zookeeper),
        Some(s) => Err(anyhow::anyhow!("invalid offset_storage: {}", s)),
    }

    // if offset_storage.is_none() {
    //     return GroupOffsetStorage::Kafka;
    // }
    //
    // let offset_storage = offset_storage.unwrap();
    // if offset_storage.eq(&String::from("Kafka")) {
    //     return GroupOffsetStorage::Kafka;
    // }
    //
    // if offset_storage.eq(&String::from("")) {
    //     return GroupOffsetStorage::Zookeeper;
    // }
    //
    // panic!("invalid offset_storage: {}", offset_storage);
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

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
    fn test_topics_invalid() {
        let mut dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topics=invalid").unwrap();
        let consumer = build_consumer(&mut dsn);
        assert!(consumer.is_err());
    }

    #[test]
    fn test_topic_partitions() {
        let mut dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1,tp2").unwrap();
        let consumer = build_consumer(&mut dsn).unwrap();
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(5, subscriptions.get("tp1").unwrap().len());

        let mut dsn =
            Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:1,tp2").unwrap();
        let consumer = build_consumer(&mut dsn).unwrap();
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(1, subscriptions.get("tp1").unwrap().len());

        let mut dsn =
            Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:0..2,tp2&group=test")
                .unwrap();
        let consumer = build_consumer(&mut dsn).unwrap();
        assert_eq!("test", consumer.group());
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(3, subscriptions.get("tp1").unwrap().len());
    }

    #[test]
    fn test_topic_partitions_invalid() {
        let mut dsn =
            Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:2..1,tp2").unwrap();
        let consumer = build_consumer(&mut dsn);
        assert!(consumer.is_err());
    }

    #[test]
    fn test_parse_timeout() {
        let dsn = Dsn::from_str("kafka://localhost:9092?timeout=99999").unwrap();
        let result = parse_timeout(&dsn).unwrap();
        assert_eq!(99999, result);

        let dsn = Dsn::from_str("kafka://localhost:9092?timeout=never").unwrap();
        let result = parse_timeout(&dsn).unwrap();
        assert_eq!(-1, result);
    }

    #[test]
    fn test_parse_timeout_invalid() {
        let dsn = Dsn::from_str("kafka://localhost:9092?timeout=invalid").unwrap();
        let result = parse_timeout(&dsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_fallback_offset() {
        // Earliest
        let fallback_offset = Some(String::from("Earliest"));
        let result = parse_fallback_offset(fallback_offset.as_deref()).unwrap();
        assert_eq!("Earliest", format!("{result:?}"));

        // Latest
        let fallback_offset = Some(String::from("Latest"));
        let result = parse_fallback_offset(fallback_offset.as_deref()).unwrap();
        assert_eq!("Latest", format!("{result:?}"));

        // ByTime
        let fallback_offset = Some(String::from("1600000000000"));
        let result = parse_fallback_offset(fallback_offset.as_deref()).unwrap();
        assert_eq!("ByTime(1600000000000)", format!("{result:?}"));
    }

    #[test]
    fn test_parse_fallback_offset_invalid() {
        // invalid
        let fallback_offset = Some(String::from("invalid"));
        let result = parse_fallback_offset(fallback_offset.as_deref());
        assert!(result.is_err())
    }

    #[test]
    fn test_parse_offset_storage() {
        // Kafka
        let offset_storage = Some(String::from("Kafka"));
        let result = parse_offset_storage(offset_storage.as_deref()).unwrap();
        assert_eq!("Kafka", format!("{result:?}"));

        // Zookeeper
        let offset_storage = Some(String::from("Zookeeper"));
        let result = parse_offset_storage(offset_storage.as_deref()).unwrap();
        assert_eq!("Zookeeper", format!("{result:?}"));
    }

    #[test]
    fn test_parse_offset_storage_invalid() {
        // invalid
        let offset_storage = Some(String::from("invalid"));
        let result = parse_offset_storage(offset_storage.as_deref());
        assert!(result.is_err());
    }
}
