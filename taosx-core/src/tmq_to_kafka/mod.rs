use std::time::Duration;

use anyhow::{anyhow, Result};
use futures_util::TryStreamExt;
use itertools::Itertools;
use kafka::client::RequiredAcks;
use kafka::producer::{Producer, Record};
use serde_json::{Map, Value};
use taos::sync::{Queryable, TBuilder};
use taos::{AsAsyncConsumer, Dsn, IsAsyncData, TaosBuilder, TmqBuilder};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

pub async fn tmq_to_kafka(from: Dsn, to: Dsn) -> Result<()> {
    let sinker = KafkaSinker::new(from, to)?;
    sinker.sink().await?;
    Ok(())
}

pub async fn clean_task(from: Dsn, task_id: String) -> Result<()> {
    let taos = TaosBuilder::from_dsn(from)?.build()?;
    let topic = tmq_topic_name(task_id);
    taos.exec(format!("drop topic if exists {}", topic))
        .unwrap();
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
    // from dsn: tmq|tmq+ws://user:password@host:port/db?table=table&task_id=task_id&[start=start&][end=end&]cols=cols&tags=tags&concurrent=1
    fn new(mut dsn: Dsn, sender: Sender<String>) -> Result<TMQSource> {
        let mut consumer_dsn = Dsn::from(dsn.clone());
        let group_id = "taosx_kafka_sink";
        consumer_dsn.set("group.id", group_id);

        let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
        let db = dsn.subject.ok_or(anyhow!("db in dsn is null"))?;
        let table = dsn
            .params
            .remove("table")
            .ok_or(anyhow!("table in dsn is null"))?;
        let task_id = dsn
            .params
            .remove("task_id")
            .ok_or(anyhow!("task id is null"))?;
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

        if cols.is_none() && !tags.is_none() {
            return Err(anyhow!("cols is null and tags is not null"));
        }
        let topic = tmq_topic_name(task_id);

        let topic_sql =
            TMQSource::tmq_sql(&cols, &tags, &db, &table, &topic, &ts_field, &start, &end);
        taos.exec(&topic_sql)?;

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
            columns = cols.join(", ");
        };
        if let Some(tags) = tags {
            // tags is not allow to exist without cols
            columns.push_str(", ");
            columns.push_str(tags.join(", ").as_str());
        }

        let mut sql = format!(
            "create topic {} as select {} from {}.{} ",
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
            let future = tokio::spawn(async move {
                TMQSource::consume(dsn, topic, sender).await.unwrap();
                Ok(())
            });
            futures.push(future);
        }

        Ok(futures)
    }

    async fn consume(dsn: Dsn, topic: String, sender: Sender<String>) -> Result<()> {
        let mut consumer = TmqBuilder::from_dsn(dsn)?.build()?;
        AsAsyncConsumer::subscribe(&mut consumer, [topic]).await?;

        loop {
            let mut stream = consumer.stream();

            while let Some((_offset, message)) = stream.try_next().await? {
                if let Some(data) = message.into_data() {
                    while let Some(block) = data.fetch_raw_block().await? {
                        let records: Vec<Map<String, Value>> = block.deserialize().try_collect()?;
                        for record in records {
                            let record_json = serde_json::to_string(&record)?;
                            log::debug!("receive from tmq {}", record_json);

                            sender.send(record_json).await?;
                        }
                    }
                }
            }
        }
    }
}

impl KafkaProducer {
    // create kafka producer from dsn, the dsn: kafka://host:port/topic?ack_timeout=1&batch_size=1
    fn new(mut dsn: Dsn, receiver: Receiver<String>) -> Result<KafkaProducer> {
        let kafka_server: Vec<String> = dsn
            .addresses
            .into_iter()
            .map(|address| {
                format!(
                    "{}:{}",
                    address.host.ok_or(anyhow!("host in dsn is null")).unwrap(),
                    address.port.ok_or(anyhow!("port in dsn is null")).unwrap()
                )
            })
            .collect::<Vec<String>>();
        let topic = dsn.subject.ok_or(anyhow!("db in from dsn is null"))?;
        let batch_size: usize = dsn
            .params
            .remove("batch_size")
            .unwrap_or("1".to_string())
            .parse()?;
        let ack_timeout: u64 = dsn
            .params
            .remove("ack_timeout")
            .unwrap_or("1".to_string())
            .parse()?;

        Ok(KafkaProducer {
            topic,
            kafka_server,
            ack_timeout,
            receiver,
            batch_size,
        })
    }

    async fn sink(self) -> Result<JoinHandle<Result<()>>> {
        let receiver = self.receiver;
        let kafka_server = self.kafka_server.clone();
        let topic = self.topic;
        let ack_timeout = self.ack_timeout;
        let batch_size = self.batch_size;

        let handler = tokio::spawn(async move {
            KafkaProducer::deal_message(receiver, kafka_server, topic, ack_timeout, batch_size)
                .await
        });
        Ok(handler)
    }

    async fn deal_message(
        mut receiver: Receiver<String>,
        kafka_server: Vec<String>,
        topic: String,
        ack_timeout: u64,
        batch_size: usize,
    ) -> Result<()> {
        let mut producer = Producer::from_hosts(kafka_server)
            .with_required_acks(RequiredAcks::One)
            .with_ack_timeout(Duration::from_secs(ack_timeout))
            .create()?;

        let mut messages = Vec::with_capacity(batch_size + 2);

        while let Some(message) = receiver.recv().await {
            messages.push(message);

            if messages.len() >= batch_size {
                let records = messages
                    .into_iter()
                    .map(|r| Record::from_value(topic.as_str(), r))
                    .collect::<Vec<Record<_, _>>>();

                producer.send_all(&records)?;
                messages = Vec::with_capacity(batch_size + 2);
            }
        }

        Ok(())
    }
}

impl KafkaSinker {
    // from dsn: tmq|tmq+ws://user:password@host:port/db?table=table&[start=start&][end=end&]cols=cols&tags=tags
    // to dsn: kafka://host:port/topic?ack_timeout=1
    fn new(from: Dsn, to: Dsn) -> Result<KafkaSinker> {
        let (tx, rx) = mpsc::channel(10);
        let source = TMQSource::new(from, tx)?;
        let producer = KafkaProducer::new(to, rx)?;

        let sinker = KafkaSinker { source, producer };
        Ok(sinker)
    }

    async fn sink(self) -> Result<()> {
        log::info!("start to sink data from tmq to kafka");
        let source_futures = self.source.read().await?;
        let sink_future = self.producer.sink().await?;

        for future in source_futures {
            future.await??;
        }
        sink_future.await??;

        Ok(())
    }
}

// tmq topic is base on task id
fn tmq_topic_name(task_id: String) -> String {
    format!("taosx_kafka_sink_{}", task_id)
}
