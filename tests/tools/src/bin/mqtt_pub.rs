use std::{
    future::pending,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::Parser;
use crossterm::event::EventStream;
use futures::{future::Either, StreamExt};
use rand::distributions::{Alphanumeric, DistString};
use rumqttc::{
    v5::{
        mqttbytes::{
            qos,
            v5::{ConnectReturnCode, PubAckReason, PubRecReason},
        },
        Event, Incoming, MqttOptions,
    },
    Outgoing,
};
use serde_with::serde_as;
use tokio::{signal::ctrl_c, task::JoinSet};
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;

use taosx_tools::{
    codec::{Compression, Encoding, Processor},
    csv_reader,
    topic::TopicFaker,
};

#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct SchemaFaker {
    pub schema: Vec<Schema>,
}

impl SchemaFaker {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let buf = std::fs::read_to_string(path).context("read schema file error")?;
        toml::from_str(&buf).context("parse schema file error")
    }
}

#[serde_as]
#[derive(Debug, PartialEq, serde::Deserialize)]
pub struct Schema {
    #[serde_as(as = "Vec<serde_with::DisplayFromStr>")]
    pub topics: Vec<TopicFaker>,
    pub qos: Option<u8>,
    pub payload: taosx_tools::fake_json::DataFakeSchema,
}

#[derive(Debug, clap::Parser)]
struct Args {
    #[command(flatten)]
    connect: ConnectArgs,
    /// [default: CPU cores]
    #[arg(long = "parallel", short = 'l')]
    parallel: Option<usize>,
    /// message data source
    #[command(flatten)]
    source: DataSourceArgs,
    /// interval to send message
    #[command(flatten)]
    frequency: FrequencyArgs,
    /// compression and encoding
    #[command(flatten)]
    payload: PayloadArgs,
    /// total messages count will send
    #[arg(long)]
    total_count: Option<usize>,
    #[arg(long, value_parser = fundu::parse_duration)]
    exec_duration: Option<Duration>,
    #[arg(long = "csv-header", value_delimiter = ',', requires = "csv_file")]
    csv_headers: Option<Vec<String>>,
}

#[derive(Debug, clap::Args)]
struct ConnectArgs {
    #[arg(long = "host", default_value = "localhost")]
    broker_host: String,
    #[arg(long = "port", default_value_t = 1883)]
    broker_port: u16,
    #[arg(long = "username", short = 'u')]
    username: Option<String>,
    #[arg(long = "password", short = 'p')]
    password: Option<String>,
    #[arg(long = "keep_alive", short = 'k', default_value = "5s", value_parser = fundu::parse_duration)]
    keep_alive: Duration,
    /// [default: `mqtt_pub_tool_` + random string]
    #[arg(long = "client_id", short = 'c')]
    client_id: Option<String>,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
struct DataSourceArgs {
    #[arg(long = "schema")]
    schema: Option<PathBuf>,
    #[arg(long = "csv-file")]
    csv_file: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
struct PayloadArgs {
    /// payload compression, support: gzip, lz4, snappy, zstd
    #[arg(long = "compress")]
    compress: Option<Compression>,
    /// payload encoding, support GBK, GB18030, BIG5
    #[arg(long = "encoding")]
    encoding: Option<Encoding>,
}

#[derive(Debug, clap::Args)]
#[group(required = false, multiple = false)]
struct FrequencyArgs {
    /// The interval time for sending data, supporting units such as s/ms/Ms/ns
    #[arg(long = "interval", default_value = "100ms", value_parser = fundu::parse_duration)]
    interval: Duration,
    #[arg(long = "stdin", action = clap::ArgAction::SetTrue)]
    stdin: bool,
}

/// TODO: qos=0 大数据量 publish 时，偶发性报错 I/O Connection reset by peer，订阅端会收到不合法的 json 字符串
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let client_id = args
        .connect
        .client_id
        .unwrap_or_else(|| format!("mqtt_pub_tool_{}", generate_random_string(10)));
    println!("client_id: {}", client_id);

    enum WatchEvent {
        ConnAck,
    }

    let mut opts = MqttOptions::new(
        client_id,
        args.connect.broker_host,
        args.connect.broker_port,
    );
    if let (Some(username), Some(password)) = (args.connect.username, args.connect.password) {
        opts.set_credentials(username, password);
    }
    opts.set_keep_alive(args.connect.keep_alive);

    let parallel = args
        .parallel
        .unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Data {
        topic: String,
        payload: Vec<u8>,
        qos: u8,
    }

    impl Data {
        fn new(topic: String, payload: Vec<u8>, qos: u8) -> Self {
            Self {
                topic,
                payload,
                qos,
            }
        }
    }

    let mut join_set = JoinSet::new();

    let (data_tx, data_rx) = flume::bounded(parallel);
    match (args.source.schema, args.source.csv_file) {
        (None, Some(csv)) => {
            #[derive(Debug, serde::Serialize, serde::Deserialize)]
            struct CsvData {
                topic: String,
                payload: String,
                qos: u8,
            }
            let headers = args.csv_headers;
            let reader =
                csv_reader::new_reader(headers.is_none(), csv).context("build csv reader error")?;
            join_set.spawn_blocking(move || {
                let headers = headers.map(csv::StringRecord::from);
                for data in reader {
                    if data_tx.is_disconnected() {
                        break;
                    }
                    match data {
                        Ok(data) => {
                            let data: CsvData = data
                                .deserialize(headers.as_ref())
                                .context("get csv data error")?;
                            let data = Data::new(data.topic, data.payload.into_bytes(), data.qos);
                            if data_tx.send(data).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            println!("{e:#}")
                        }
                    }
                }
                println!("stop read csv file");
                anyhow::Ok(())
            });
        }
        (Some(schema), None) => {
            let faker = SchemaFaker::from_file(schema)?;
            let processor = (args.payload.encoding, args.payload.compress);
            for schema in faker.schema {
                let payload = Arc::new(schema.payload);
                for topic in schema.topics {
                    for _ in 0..parallel {
                        let topic = topic.clone();
                        let data_tx = data_tx.clone();
                        let payload = payload.clone();
                        join_set.spawn(async move {
                            loop {
                                if data_tx.is_disconnected() {
                                    break;
                                }
                                let payload = payload.clone();
                                let payload = tokio::task::spawn_blocking(move || {
                                    anyhow::Ok(
                                        payload
                                            .rand_json_value()
                                            .context("gen fake data error")
                                            .and_then(|value| {
                                                serde_json::to_vec(&value)
                                                    .context("serialize json error")
                                            })
                                            .and_then(|value| {
                                                processor
                                                    .process(value)
                                                    .context("processer process error")
                                            })
                                            .context("get value error")?,
                                    )
                                })
                                .await??;
                                let data = Data::new(
                                    topic.next()?,
                                    payload,
                                    schema.qos.unwrap_or_default(),
                                );
                                if data_tx.send_async(data).await.is_err() {
                                    break;
                                }
                            }
                            Ok(())
                        });
                    }
                }
            }
            drop(data_tx);
        }
        (Some(_), Some(_)) => unreachable!(),
        (None, None) => anyhow::bail!("schema or csv file is required"),
    }

    let (client, mut event_loop) = rumqttc::v5::AsyncClient::new(opts, 1000);

    let token = CancellationToken::new();
    let (tx, rx) = flume::bounded(parallel);
    let total_notifier = Arc::new(tokio::sync::Notify::new());
    join_set.spawn({
        let token = token.clone();
        let total_notifier = total_notifier.clone();
        async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut published = 0;
            let start = Instant::now();
            loop {
                tokio::select! {
                    res = event_loop.poll() => match res {
                        Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                            if matches!(ack.code, ConnectReturnCode::Success) {
                                println!("client connect sucessfully");
                                tx.try_send(WatchEvent::ConnAck).ok();
                            } else {
                                println!("connect error: {:?}", ack.code);
                                break
                            }
                        },
                        Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                            if matches!(ack.reason, PubAckReason::Success) {
                                published += 1;
                            } else {
                                println!("publish error: {:?}", ack.reason);
                            }
                        },
                        Ok(Event::Incoming(Incoming::PubRec(ack))) => {
                            if matches!(ack.reason, PubRecReason::Success) {
                                published += 1;
                            } else {
                                println!("publish error: {:?}", ack.reason);
                            }
                        },
                        Ok(Event::Outgoing(Outgoing::Publish(0))) => {
                                published += 1;
                        }
                        Ok(_) => {},
                        Err(e) => {
                            println!("polling error: {e}");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        },
                    },
                    _ = ticker.tick() => {
                        let duration = start.elapsed().as_secs();
                        if duration == 0 {
                            continue;
                        }
                        println!("published {published}, speed: {}/s", published / duration);
                    },
                    _ = token.cancelled() => break
                }

                if args
                    .total_count
                    .is_some_and(|total| published as usize >= total)
                {
                    total_notifier.notify_waiters();
                    break;
                }
            }
            println!("total published: {published}");
            Ok(())
        }
    });

    let Ok(WatchEvent::ConnAck) = rx.recv_async().await else {
        anyhow::bail!("client connect error, exit")
    };

    join_set.spawn({
        let token = token.clone();
        let client = client.clone();
        let total_notifier = total_notifier.clone();
        async move {
            let _guard = token.clone().drop_guard();
            let mut stream = {
                if args.frequency.stdin {
                    EventStream::new().map(|_| ()).boxed()
                } else if !args.frequency.interval.is_zero() {
                    let mut interval = tokio::time::interval(args.frequency.interval);
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    IntervalStream::new(interval).map(|_| ()).boxed()
                } else {
                    futures::stream::repeat(()).boxed()
                }
            };
            let mut total = 0;
            loop {
                tokio::select! {
                    _ = stream.next() => {
                        let data = tokio::select! {
                            data = data_rx.recv_async() => match data {
                                Ok(data) => data,
                                Err(_) => break,
                            },
                            _ = token.cancelled() => break,
                        };
                        let qos = qos(data.qos).unwrap();
                        tokio::select! {
                            res = client.publish(&data.topic, qos, false, data.payload) => {
                                if let Err(e) = res {
                                    println!("publish message error: {e}")
                                }
                            },
                            _ = token.cancelled() => break,
                        }
                        total += 1;
                        if args.total_count.is_some_and(|c| total >= c) {
                            total_notifier.notified().await;
                            break
                        }
                    },
                    _ = token.cancelled() => break,
                }
            }
            Ok(())
        }
    });

    let deadline = args.exec_duration.map(|v| tokio::time::Instant::now() + v);
    loop {
        tokio::select! {
            _ = ctrl_c() => {
                println!("received ctrl_c signal");
                break
            },
            _ = timeout_or_never(deadline) => {
                println!("time expired");
                break
            },
            res = join_set.join_next() => match res {
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    println!("consumer panic: {e}");
                }
                None => {
                    println!("all consumer exist");
                    break
                },
            }
        }
    }

    match tokio::time::timeout(Duration::from_secs(5), client.disconnect()).await {
        Ok(_) => println!("mqtt client disconnected"),
        Err(_) => println!("mqtt client disconnected timeout"),
    }
    token.cancel();
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}

    Ok(())
}

fn generate_random_string(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), length)
}

fn timeout_or_never(
    fut: Option<tokio::time::Instant>,
) -> Either<tokio::time::Sleep, std::future::Pending<()>> {
    match fut {
        Some(instant) => Either::Left(tokio::time::sleep_until(instant)),
        None => Either::Right(pending()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_schema_faker_test() -> anyhow::Result<()> {
        let schema: SchemaFaker = toml::from_str(
            r#"
[[schema]]
topics = [
    "ems/site/{::60}/root/{::60}/string",
    "ems/site/{::60}/{::60}/{::60}/{::60}/string",
    "ems/site/{::60}/unit/{::60}/root/{::60}/string",
    "ems/site/{::60}/unit/{::60}/{::60}/{::60}/{::60}/string",
]
qos = 0

[schema.payload]
type = "object"

[schema.payload.properties]
ts = { type = "timestamp", start_time = 2025-10-01T00:00:00.888888888, interval = "1ns" }
value = { type = "option", value = { type = "string", length = { range = { min = 10, max = 1000 } } } }
        "#,
        )?;
        assert_eq!(schema.schema.len(), 1);
        assert_eq!(
            schema.schema[0].topics,
            [
                "ems/site/{::60}/root/{::60}/string".parse()?,
                "ems/site/{::60}/{::60}/{::60}/{::60}/string".parse()?,
                "ems/site/{::60}/unit/{::60}/root/{::60}/string".parse()?,
                "ems/site/{::60}/unit/{::60}/{::60}/{::60}/{::60}/string".parse()?,
            ]
        );
        assert_eq!(schema.schema[0].qos, Some(0));
        assert_eq!(
            schema.schema[0].payload,
            toml::from_str(
                r#"
type = "object"
[properties.ts]
type = "timestamp"
start_time = 2025-10-01T00:00:00.888888888
interval = "1ns"

[properties.value]
type = "option"

[properties.value.value]
type = "string"
length = { range = { min = 10, max = 1000 } }
        "#
            )?
        );
        Ok(())
    }
}
