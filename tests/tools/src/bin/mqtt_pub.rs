use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::Parser;
use crossterm::event::EventStream;
use futures::StreamExt;
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
use tokio::{signal::ctrl_c, task::JoinSet};
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;

use taosx_tools::{
    codec::{Compression, Encoding, Processor},
    csv_reader,
    faker::SchemaFaker,
};

#[derive(Debug, clap::Parser)]
struct Args {
    #[clap(long = "schema")]
    schema: Option<PathBuf>,
    #[clap(long = "host", default_value = "localhost")]
    broker_host: String,
    #[clap(long = "port", default_value_t = 1883)]
    broker_port: u16,
    #[clap(long = "username", short = 'u')]
    username: Option<String>,
    #[clap(long = "password", short = 'p')]
    password: Option<String>,
    #[clap(long = "keep_alive", short = 'k', default_value = "5s", value_parser = fundu::parse_duration)]
    keep_alive: Duration,
    /// [default: `mqtt_pub_tool_` + random string]
    #[clap(long = "client_id", short = 'c')]
    client_id: Option<String>,
    /// [default: CPU cores]
    #[clap(long = "perallel", short = 'l')]
    perallel: Option<usize>,
    /// The interval time for sending data, supporting units such as s/ms/Ms/ns
    #[clap(long = "interval", default_value = "100ms", value_parser = fundu::parse_duration)]
    interval: Duration,
    #[clap(long = "stdin", action = clap::ArgAction::SetTrue, conflicts_with = "interval")]
    stdin: bool,
    /// payload compression, support: gzip, lz4, snappy, zstd
    #[clap(long = "compress")]
    compress: Option<Compression>,
    /// payload encoding, support GBK, GB18030, BIG5
    #[clap(long = "encoding")]
    encoding: Option<Encoding>,
    #[clap(long = "csv-header", value_delimiter = ',')]
    csv_headers: Option<Vec<String>>,
    #[clap(long = "csv", conflicts_with = "schema")]
    csv_file: Option<PathBuf>,
}

/// TODO: qos=0 大数据量 publish 时，偶发性报错 I/O Connection reset by peer，订阅端会收到不合法的 json 字符串
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let client_id = args
        .client_id
        .unwrap_or_else(|| format!("mqtt_pub_tool_{}", generate_random_string(10)));
    println!("client_id: {}", client_id);

    enum WatchEvent {
        ConnAck,
    }

    let mut opts = MqttOptions::new(client_id, args.broker_host, args.broker_port);
    if let (Some(username), Some(password)) = (args.username, args.password) {
        opts.set_credentials(username, password);
    }
    opts.set_keep_alive(args.keep_alive);

    let perallel = args
        .perallel
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

    let (data_tx, data_rx) = flume::bounded(perallel);
    match (args.schema, args.csv_file) {
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
            let faker = SchemaFaker::from_file(schema).map_err(anyhow::Error::new)?;
            let processor = (args.encoding, args.compress);
            for schema in faker.schema.into_iter() {
                let payload = Arc::new(schema.payload);
                for topic in schema.topics.into_iter() {
                    let data_tx = data_tx.clone();
                    let payload = payload.clone();
                    join_set.spawn(async move {
                        loop {
                            if data_tx.is_disconnected() {
                                break;
                            }
                            let payload = payload
                                .clone()
                                .rand_json()
                                .context("gen fake data error")
                                .and_then(|value| {
                                    serde_json::to_vec(&value).context("serialize json error")
                                })
                                .and_then(|value| {
                                    processor.process(value).context("processer process error")
                                })
                                .context("get value error")?;
                            let data =
                                Data::new(topic.next()?, payload, schema.qos.unwrap_or_default());
                            if data_tx.send_async(data).await.is_err() {
                                break;
                            }
                        }
                        Ok(())
                    });
                }
            }
        }
        (Some(_), Some(_)) => unreachable!(),
        (None, None) => anyhow::bail!("schema or csv file is required"),
    }

    let (client, mut event_loop) = rumqttc::v5::AsyncClient::new(opts, 1000);

    let token = CancellationToken::new();
    let (tx, rx) = flume::bounded(perallel);
    let (consumer_exit_tx, consumer_exit_rx) = flume::bounded::<()>(0);
    join_set.spawn({
        let token = token.clone();
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
                    _ = consumer_exit_rx.recv_async() => break,
                    _ = token.cancelled() => break
                }
            }
            println!("total published: {published}");
            Ok(())
        }
    });

    let Ok(WatchEvent::ConnAck) = rx.recv_async().await else {
        anyhow::bail!("client connect error, exit")
    };

    let stdin = args.stdin;

    for _ in 0..perallel {
        join_set.spawn({
            let token = token.clone();
            let client = client.clone();
            let data_rx = data_rx.clone();
            let consumer_exit_tx = consumer_exit_tx.clone();
            async move {
                let mut stream = {
                    if stdin {
                        EventStream::new().map(|_| ()).boxed()
                    } else if !args.interval.is_zero() {
                        let mut interval = tokio::time::interval(args.interval);
                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        IntervalStream::new(interval).map(|_| ()).boxed()
                    } else {
                        futures::stream::repeat(()).boxed()
                    }
                };

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
                        },
                        _ = token.cancelled() => break,
                    }
                }
                drop(consumer_exit_tx);
                Ok(())
            }
        });
    }
    drop(data_rx);
    drop(consumer_exit_tx);

    loop {
        tokio::select! {
            _ = ctrl_c() => {
                println!("received ctrl_c signal");
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
