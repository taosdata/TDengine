use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures_util::TryStreamExt;
use itertools::Itertools;
use kafka::client::RequiredAcks;
use kafka::producer::{Producer, Record};
use serde_json::{Map, Value};
use taos::{
    AsAsyncConsumer, AsyncQueryable, AsyncTBuilder, Consumer, Dsn, IsAsyncData, TaosBuilder,
    TmqBuilder,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;

use taosx_core::utils;

pub async fn tmq_to_kafka(from: Dsn, to: Dsn, cancel: CancellationToken) -> Result<()> {
    let sinker = KafkaSinker::new(from, to).await?;
    sinker.sink(cancel).await?;
    Ok(())
}

#[allow(dead_code)]
pub async fn clean_task(from: Dsn) -> Result<()> {
    tracing::warn!("clean task {}", &from.to_string());
    let mut dsn = from.clone();
    let db = dsn.subject.ok_or(anyhow!("db in dsn is null"))?;
    let table = dsn.params.remove("table").ok_or(anyhow!("table is null"))?;
    let topic_suffix = dsn
        .params
        .remove("topic_suffix")
        .ok_or(anyhow!("topic suffix is null"))?;

    let conn = TaosBuilder::from_dsn(from)?.build().await?;
    let sql = format!(
        "drop topic if exists {}",
        tmq_topic_name(&db, &table, &topic_suffix)
    );
    conn.exec(sql).await?;
    Ok(())
}

struct KafkaSinker {
    source: TMQSource,
    producer: KafkaProducer,
}

struct TMQSource {
    consumer_dsn: Dsn,
    topic: String,
    sender: Sender<String>,
    concurrent: usize,
}

struct KafkaProducer {
    receiver: Receiver<String>,
    topic: String,
    kafka_server: Vec<String>,
    ack_timeout: u64,
    batch_size: usize,
}

impl TMQSource {
    // from dsn: tmq|tmq+ws://user:password@host:port/db?table=table&topic_suffix=topic_suffix&[start=start&][end=end&]cols=cols&tags=tags&concurrent=1
    async fn new(mut dsn: Dsn, sender: Sender<String>) -> Result<TMQSource> {
        let mut consumer_dsn = dsn.clone();
        let group_id = "x_kafka_sink";
        consumer_dsn.set("group.id", group_id);

        let conn = TaosBuilder::from_dsn(&dsn)?.build().await?;
        let db = dsn.subject.ok_or(anyhow!("db in dsn is null"))?;
        let table = dsn
            .params
            .remove("table")
            .ok_or(anyhow!("table in dsn is null"))?;
        let topic_suffix = dsn
            .params
            .remove("topic_suffix")
            .ok_or(anyhow!("topic suffix is null"))?;
        let ts_field = dsn.params.remove("ts").unwrap_or("ts".to_string());
        let start = dsn.params.remove("start");
        let end = dsn.params.remove("end");
        let concurrent: usize = dsn
            .params
            .remove("concurrent")
            .unwrap_or("1".to_string())
            .parse()?;

        let cols = dsn
            .params
            .remove("cols")
            .map(|cols| cols.split(",").map(String::from).collect::<Vec<String>>());
        let tags = dsn
            .params
            .remove("tags")
            .map(|tags| tags.split(",").map(String::from).collect::<Vec<String>>());

        if cols.is_none() && tags.is_some() {
            return Err(anyhow!("cols is null and tags is not null"));
        }
        let topic = tmq_topic_name(&db, &table, &topic_suffix);

        let topic_sql =
            TMQSource::tmq_sql(&cols, &tags, &db, &table, &topic, &ts_field, &start, &end);
        conn.exec(&topic_sql).await?;

        Ok(TMQSource {
            consumer_dsn,
            topic,
            sender,
            concurrent,
        })
    }

    fn tmq_sql(
        cols: &Option<Vec<String>>,
        tags: &Option<Vec<String>>,
        db: &String,
        table: &String,
        topic: &String,
        ts_field: &String,
        start: &Option<String>,
        end: &Option<String>,
    ) -> String {
        let mut columns = String::from("*");
        if let Some(cols) = cols {
            columns = cols.iter().map(|s| format!("`{s}`")).join(", ");
        };
        if let Some(tags) = tags {
            // tags is not allow to exist without cols
            columns.push_str(", ");
            columns.push_str(tags.iter().map(|s| format!("`{s}`")).join(", ").as_str());
        }

        let mut sql = format!(
            "create topic if not exists `{}` as select {} from `{}`.`{}` ",
            topic, columns, db, table
        );
        let mut conditions = Vec::with_capacity(2);
        if let Some(start) = start {
            conditions.push(format!(" {} >= '{}' ", ts_field, start))
        }
        if let Some(end) = end {
            conditions.push(format!(" {} <= '{}' ", ts_field, end))
        }
        if !conditions.is_empty() {
            sql.push_str(" where ");
            sql.push_str(conditions.join(" and ").as_str());
        }

        sql
    }

    async fn read(&self) -> Result<Vec<JoinHandle<Result<()>>>> {
        let mut futures = Vec::with_capacity(self.concurrent);

        for _ in 0..self.concurrent {
            let dsn = self.consumer_dsn.clone();
            let topic = self.topic.to_string();
            let sender = self.sender.clone();
            let consumer = TmqBuilder::from_dsn(dsn)?.build().await?;

            let future = tokio::spawn(TMQSource::consume(consumer, topic, sender));
            futures.push(future);
        }

        Ok(futures)
    }

    async fn consume(mut consumer: Consumer, topic: String, sender: Sender<String>) -> Result<()> {
        AsAsyncConsumer::subscribe(&mut consumer, [topic]).await?;

        'outer: loop {
            let mut stream = consumer.stream();

            while let Some((_offset, message)) = stream.try_next().await? {
                if let Some(data) = message.into_data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        let records: Vec<Map<String, Value>> = block.deserialize().try_collect()?;
                        for record in records {
                            let record_json = serde_json::to_string(&record)?;
                            tracing::debug!("receive from tmq consumer {}", record_json);

                            if sender.send(record_json).await.is_err() {
                                // channel is closed.
                                break 'outer;
                            };
                        }
                    }
                }
            }
        }

        AsAsyncConsumer::unsubscribe(consumer).await;
        Ok(())
    }
}

impl KafkaProducer {
    // create kafka producer from dsn, the dsn: kafka://host:port/topic?ack_timeout=1&batch_size=1
    fn new(mut dsn: Dsn, receiver: Receiver<String>) -> Result<KafkaProducer> {
        let mut kafka_server = Vec::new();
        for address in dsn.addresses.into_iter() {
            let host = address.host.ok_or(anyhow!("host in dsn is null"))?;
            let port = address.port.ok_or(anyhow!("port in dsn is null"))?;
            kafka_server.push(format!("{}:{}", host, port));
        }
        let topic = dsn
            .subject
            .ok_or(anyhow!("kafka sink topic should not be null"))?;
        let batch_size: usize = dsn
            .params
            .remove("batch_size")
            .unwrap_or("1".to_string())
            .parse()?;
        let ack_timeout: u64 = utils::parse_duration(
            dsn.params
                .remove("ack_timeout")
                .unwrap_or("1".to_string())
                .as_str(),
        )
        .context("ack timeout config error, should be a valid duartion config")?
        .as_secs();

        Ok(KafkaProducer {
            topic,
            kafka_server,
            ack_timeout,
            receiver,
            batch_size,
        })
    }

    async fn sink(self, cancel: CancellationToken) -> Result<JoinHandle<Result<()>>> {
        let receiver = self.receiver;
        let kafka_server = self.kafka_server.clone();
        let topic = self.topic;
        let ack_timeout = self.ack_timeout;
        let batch_size = self.batch_size;

        dbg!(&kafka_server);
        let server = kafka_server.iter().join(",");
        let producer = Producer::from_hosts(kafka_server)
            .with_required_acks(RequiredAcks::One)
            .with_ack_timeout(Duration::from_secs(ack_timeout))
            .create()
            .with_context(|| format!("Create kafka producer error from {server}"))?;
        tracing::info!("Start kafka producer");

        let handler = tokio::spawn(KafkaProducer::deal_message(
            receiver, producer, topic, batch_size, cancel,
        ));
        Ok(handler)
    }

    async fn deal_message(
        mut receiver: Receiver<String>,
        mut producer: Producer,
        topic: String,
        batch_size: usize,
        cancel: CancellationToken,
    ) -> Result<()> {
        tokio::spawn(async move {
            let mut messages: Vec<String> = Vec::with_capacity(batch_size + 2);
            let mut interval = time::interval(Duration::from_secs(1));

            'outer: loop {
                tokio::select! {
                    Some(message) = receiver.recv() => {
                        messages.push(message);
                        if messages.len() >= batch_size {
                            KafkaProducer::send_messages(&mut producer, &messages, &topic).await?;
                            messages = Vec::with_capacity(batch_size + 2);
                        }
                    },
                    _ = interval.tick() => {
                        if !messages.is_empty() {
                            KafkaProducer::send_messages(&mut producer, &messages, &topic).await?;
                            messages = Vec::with_capacity(batch_size + 2);
                        }
                    },
                    _ = cancel.cancelled() => {
                        break 'outer;
                    }
                }
            }

            if !messages.is_empty() {
                KafkaProducer::send_messages(&mut producer, &messages, &topic).await?;
            }
            Result::<(), anyhow::Error>::Ok(())
        })
        .await??;

        tracing::info!("Kafka producer stopped");
        Ok(())
    }

    async fn send_messages(
        producer: &mut Producer,
        messages: &[String],
        topic: &str,
    ) -> Result<()> {
        let records: Vec<Record<_, _>> = messages
            .iter()
            .map(|r| Record::from_value(topic, r.as_bytes()))
            .collect::<Vec<Record<_, _>>>();

        producer
            .send_all(&records)
            .context("Kafka send message error")?;
        Ok(())
    }
}

impl KafkaSinker {
    // from dsn: tmq|tmq+ws://user:password@host:port/db?table=table&[start=start&][end=end&]cols=cols&tags=tags
    // to dsn: kafka://host:port/topic?ack_timeout=1
    async fn new(from: Dsn, to: Dsn) -> Result<KafkaSinker> {
        let (tx, rx) = mpsc::channel(10);
        let source = TMQSource::new(from, tx).await?;
        let producer = KafkaProducer::new(to, rx)?;

        let sinker = KafkaSinker { source, producer };
        Ok(sinker)
    }

    async fn sink(self, cancel: CancellationToken) -> Result<()> {
        tracing::info!("start to sink data from tmq to kafka");
        let source_futures = self.source.read().await?;
        let sink_future = self.producer.sink(cancel.clone()).await?;
        tokio::spawn(async move {
            tokio::select! {
                _=cancel.cancelled() =>{
                    tracing::warn!("kafka sinker task is canceled");
                    for future in &source_futures {
                        future.abort();
                    }
                    sink_future.abort();
                }
            }

            for future in source_futures {
                future.await??;
            }
            sink_future.await??;
            Result::<(), anyhow::Error>::Ok(())
        })
        .await??;

        Ok(())
    }
}

// tmq topic is base on task id
fn tmq_topic_name(db: &String, table: &String, topic_suffix: &String) -> String {
    format!("x_kafka_sink_{}_{}_{}", db, table, topic_suffix)
}
