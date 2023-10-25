use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::Context;
use arrow::array::{
    BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use kafka::client::{KafkaClient, SecurityConfig};
use kafka::consumer::{Consumer, GroupOffsetStorage, Message, MessageSet};
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use taos::Dsn;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use taosx_ipc::prelude::ArrowDataType;

use crate::plugins::runners::kafka::config::SourceConfig;
use crate::plugins::validation::DataSourceValidation;
use crate::utils::port_pool::PortPool;
use crate::{build_ipc, Action, Parser, Transferred};

mod config;

pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    let config = SourceConfig::from_dsn(dsn);
    match config {
        Err(err) => DataSourceValidation::invalid(
            "kafka".to_string(),
            format!(
                "invalid dsn: {}, cause: {}",
                dsn.to_string(),
                err.to_string()
            ),
        ),
        Ok(c) => {
            let mut client = KafkaClient::new(c.bootstrap_servers);
            let result = client.load_metadata_all();
            match result {
                Ok(()) => DataSourceValidation {
                    valid: true,
                    support: true,
                    data_source: "kafka".to_string(),
                    version: None,
                    message: None,
                },
                //
                Err(err) => DataSourceValidation {
                    valid: false,
                    support: true,
                    data_source: "kafka".to_string(),
                    version: None,
                    message: Some(err.to_string()),
                },
            }
        }
    }
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

    let worker = tokio::task::spawn_blocking(move || kafka_worker(from, port));
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
                abort_handle.abort();
            }
        }
        // send an empty tuple
        let _ = ipc.send(()).await;
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

fn kafka_worker(mut from: Dsn, port: u16) -> anyhow::Result<()> {
    let socket = format!("127.0.0.1:{}", port);
    let stream = std::net::TcpStream::connect(socket)?;
    let schema = build_schema();
    let mut writer = StreamWriter::try_new(&stream, &schema)?;

    let mut consumer = build_consumer(&mut from)?;
    let timeout = SourceConfig::parse_timeout(&from)?;
    let mut start = chrono::Utc::now().timestamp_millis();

    loop {
        let message_sets = consumer.poll().context("Kafka polling error")?;
        if message_sets.is_empty() {
            thread::sleep(Duration::from_millis(100));
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
                let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap();

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
            consumer.consume_messageset(ms)?;
        }

        consumer.commit_consumed()?;
        start = chrono::Utc::now().timestamp_millis();
    }

    println!("{} kafka_to_taos stopped", chrono::Utc::now().to_string());
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

fn build_consumer(dsn: &Dsn) -> anyhow::Result<Consumer> {
    let config = SourceConfig::from_dsn(dsn)?;
    let mut client = build_client(&config)?;
    client.load_metadata_all()?;

    let mut builder = Consumer::from_client(client);
    // group
    builder = builder.with_group(config.group);
    // topics
    for topic in config.topics.unwrap_or(vec![]) {
        builder = builder.with_topic(topic);
    }
    // topic_partitions
    for (t, p) in config.topic_partitions.unwrap_or(HashMap::new()).iter() {
        if p.is_empty() {
            builder = builder.with_topic(t.to_string());
        } else {
            builder = builder.with_topic_partitions(t.to_string(), p);
        }
    }
    // fallback_offset
    builder = builder.with_fallback_offset(config.fallback_offset);
    // offset_storage: use Kafka as Default
    builder = builder.with_offset_storage(Some(GroupOffsetStorage::Kafka));

    if config.fetch_max_wait_time.is_some() {
        builder = builder.with_fetch_max_wait_time(config.fetch_max_wait_time.unwrap());
    }
    if config.fetch_min_bytes.is_some() {
        builder = builder.with_fetch_min_bytes(config.fetch_min_bytes.unwrap());
    }
    if config.fetch_max_bytes_per_partition.is_some() {
        builder = builder
            .with_fetch_max_bytes_per_partition(config.fetch_max_bytes_per_partition.unwrap());
    }
    if config.fetch_crc_validation.is_some() {
        builder = builder.with_fetch_crc_validation(config.fetch_crc_validation.unwrap());
    }
    if config.offset_storage.is_some() {
        builder = builder.with_offset_storage(config.offset_storage);
    }
    if config.retry_max_bytes_limit.is_some() {
        builder = builder.with_retry_max_bytes_limit(config.retry_max_bytes_limit.unwrap());
    }
    if config.connection_idle_timeout.is_some() {
        builder = builder.with_connection_idle_timeout(config.connection_idle_timeout.unwrap());
    }
    if config.client_id.is_some() {
        builder = builder.with_client_id(config.client_id.unwrap());
    }

    let consumer = builder.create()?;
    Ok(consumer)
}

fn build_client(config: &SourceConfig) -> anyhow::Result<KafkaClient> {
    let client;
    if config.use_ssl {
        let mut ssl_builder = SslConnector::builder(SslMethod::tls())?;
        ssl_builder.set_cipher_list("DEFAULT")?;
        ssl_builder
            .set_certificate_file(config.cert.clone().unwrap().as_path(), SslFiletype::PEM)?;
        ssl_builder
            .set_private_key_file(config.cert_key.clone().unwrap().as_path(), SslFiletype::PEM)?;
        ssl_builder.check_private_key()?;
        ssl_builder.set_default_verify_paths()?;
        ssl_builder.set_verify(SslVerifyMode::PEER);
        let connector = ssl_builder.build();

        client = KafkaClient::new_secure(
            config.bootstrap_servers.clone(),
            SecurityConfig::new(connector),
        );
    } else {
        client = KafkaClient::new(config.bootstrap_servers.clone());
    }
    Ok(client)
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_is_valid() {
        let dsn = Dsn::from_str("kafka://192.168.1.92:9092,jf92:9092").unwrap();
        let result = is_valid(&dsn).await;
        assert_eq!(true, result.valid);
        assert_eq!(true, result.support);
        assert_eq!("kafka", result.data_source);

        let dsn = Dsn::from_str("kafka://127.0.0.1:9092").unwrap();
        let result = is_valid(&dsn).await;
        assert_eq!(false, result.valid);
        assert_eq!(true, result.support);
        assert_eq!("kafka", result.data_source);
        assert_eq!("No host reachable", result.message.unwrap());

        let dsn = Dsn::from_str("kafka://127.0.0.1:9092,jf92:9092").unwrap();
        let result = is_valid(&dsn).await;
        assert_eq!(true, result.valid);
        assert_eq!(true, result.support);
        assert_eq!("kafka", result.data_source);
    }
}
