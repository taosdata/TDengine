use std::array;
use std::char::MAX;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bitvec::macros::internal::funty::Fundamental;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message, MessageSet};
use serde_with::TimestampNanoSeconds;
use taos::{AsyncTBuilder, Dsn, TaosBuilder};
use tokio_util::sync::CancellationToken;
use taosx_ipc::prelude::ArrowDataType;
use crate::{Action, Parser, Transferred};
use crate::plugins::sink;
use crate::utils::port_pool::PortPool;

pub async fn kafka_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    transformers: Vec<Action>,
    to: Dsn,
    jobs: usize,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    transferred: Option<Arc<Transferred>>,
) -> anyhow::Result<()> {
    println!("{} kafka_to_taos started, from: {}, to: {}", chrono::Utc::now().to_string(), from.to_string(), to.to_string());

    let port = port_pool.get().ok_or_else(|| anyhow::format_err!("No available port"))?;
    let socket = format!("127.0.0.1:{}", port);
    let ipc = build_ipc(&socket, parser, &to, &cancel, with_agent, transferred)?;

    let stream = std::net::TcpStream::connect(socket)?;
    let schema = build_schema();

    let mut writer = StreamWriter::try_new(&stream, &schema)?;

    let mut consumer = build_consumer(&from);
    let timeout = parse_timeout(&from);
    let mut start = chrono::Utc::now().timestamp_millis();
    loop {
        let message_sets = consumer.poll().unwrap();
        if message_sets.is_empty() {
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
                print_message(&ms, &m);
                timestamp.append_value(chrono::Utc::now().timestamp_nanos());
                topic.append_value(ms.topic());
                partition.append_value(ms.partition());
                offset.append_value(m.offset.clone());
                key.append_value(m.key.clone());
                value.append_value(m.value.clone());
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
        writer.write(&batch)?;

        consumer.commit_consumed().unwrap();
        start = chrono::Utc::now().timestamp_millis();
    }

    ipc.send(());
    println!("{} kafka_to_taos stopped", chrono::Utc::now().to_string());
    Ok(())
}

fn build_ipc(socket: &str, parser: Option<Parser>, to: &Dsn, cancel: &CancellationToken, with_agent: Option<(i64, String, String)>, transferred: Option<Arc<Transferred>>) -> anyhow::Result<Sender<()>> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let ipc = if with_agent.is_none() {
        let builder = taos::TaosBuilder::from_dsn(to)?;
        sink::listen_tcp_socket(
            builder.pool()?,
            socket,
            sender,
            None,
            cancel.clone(),
            with_agent,
            parser,
            Some("kafka"),
            transferred,
        )?
    } else {
        sink::listen_tcp_socket_with_agent(
            socket,
            sender,
            None,
            cancel.clone(),
            with_agent.unwrap(),
        )?
    };
    Ok(ipc)
}

fn build_schema() -> Schema {
    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("none"));
    let flat_columns = vec![
        Field::new("ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
        Field::new("topic", ArrowDataType::Utf8, false),
        Field::new("partition", ArrowDataType::Int32, false),
        Field::new("offset", ArrowDataType::Int64, false),
        Field::new("key", ArrowDataType::Binary, true),
        Field::new("value", ArrowDataType::Binary, false),
    ];
    let schema = Schema::new(flat_columns).with_metadata(metadata);
    schema
}

fn build_consumer(dsn: &Dsn) -> Consumer {
    let mut bootstrap_servers = Vec::new();
    for address in dsn.addresses.iter() {
        bootstrap_servers.push(format!("{}:{}", address.host.clone().unwrap(), address.port.clone().unwrap()));
    }
    let mut consumer = Consumer::from_hosts(bootstrap_servers);

    let default_group = String::from("");
    let group = dsn.params.get("group").unwrap_or(&default_group);
    consumer = consumer.with_group(group.to_string());

    if dsn.params.contains_key("topics") {
        let topic = dsn.params.get("topics");
        for t in topic.unwrap().split(",") {
            consumer = consumer.with_topic(t.to_string());
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
                    consumer = consumer.with_topic_partitions(topic.to_string(), &partitions);
                } else {
                    let partition = partition.parse::<i32>().unwrap();
                    consumer = consumer.with_topic_partitions(topic.to_string(), &[partition]);
                }
            } else {
                consumer = consumer.with_topic(tp.to_string());
            }
        }
    }

    let fallback_offset = parse_fallback_offset(dsn.params.get("fallback_offset"));
    consumer = consumer.with_fallback_offset(fallback_offset);

    let offset_storage = parse_offset_storage(dsn.params.get("offset_storage"));
    consumer = consumer.with_offset_storage(offset_storage);

    consumer.create().unwrap()
}

fn print_message(ms: &MessageSet, m: &Message) {
    println!("topic: {}, partition: {}, offset: {}, key: {}, values: {}", ms.topic(), ms.partition(), m.offset, String::from_utf8_lossy(m.key), String::from_utf8_lossy(m.value));
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
    use std::collections::HashMap;
    use std::fmt;
    use std::fmt::{Debug, format};
    use std::str::FromStr;
    use arrow::array::{Int32Builder, Int64Builder, PrimitiveArray, PrimitiveBuilder};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, Time64NanosecondType};
    use chrono::Timelike;
    use itertools::assert_equal;
    use taosx_ipc::prelude::ArrowDataType;
    use super::*;

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
        let dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topics=tp2").unwrap();
        let consumer = build_consumer(&dsn);
        assert_eq!("", consumer.group());
        let subscriptions = consumer.subscriptions();
        assert_eq!(1, subscriptions.len());

        let dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topics=tp1,tp2").unwrap();
        let consumer = build_consumer(&dsn);
        assert_eq!("", consumer.group());
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
    }

    #[test]
    #[should_panic]
    fn test_topics_invalid() {
        let dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topics=invalid").unwrap();
        let consumer = build_consumer(&dsn);
    }

    #[test]
    fn test_topic_partitions() {
        let dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1,tp2").unwrap();
        let consumer = build_consumer(&dsn);
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(5, subscriptions.get("tp1").unwrap().len());
        println!("{:?}", subscriptions);

        let dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:1,tp2").unwrap();
        let consumer = build_consumer(&dsn);
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(1, subscriptions.get("tp1").unwrap().len());
        println!("{:?}", subscriptions);

        let dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:0..2,tp2&group=test").unwrap();
        let consumer = build_consumer(&dsn);
        assert_eq!("test", consumer.group());
        let subscriptions = consumer.subscriptions();
        assert_eq!(2, subscriptions.len());
        assert_eq!(3, subscriptions.get("tp1").unwrap().len());
        println!("{:?}", subscriptions);
    }

    #[test]
    #[should_panic]
    fn test_topic_partitions_invalid() {
        let dsn = Dsn::from_str("kafka://192.168.1.92:9092/?topic_partitions=tp1:2..1,tp2").unwrap();
        let consumer = build_consumer(&dsn);
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