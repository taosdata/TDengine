use std::{
    future::pending,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::Parser;
use crossterm::event::EventStream;
use faststr::FastStr;
use futures::{future::Either, StreamExt};
use rumqttc::{
    v5::{
        mqttbytes::{
            qos,
            v5::{ConnAck, ConnectReturnCode, PubAckReason, PubRecReason},
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
    csv_reader, generate_random_string,
    topic::TopicFaker,
    topic_fuzzy::TopicFuzzer,
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

#[derive(Debug, Clone, clap::Parser)]
struct Args {
    #[command(flatten)]
    connect: ConnectArgs,
    /// [default: CPU cores]
    #[arg(long, short = 'l')]
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
    #[arg(long = "report-interval", default_value = "5s", value_parser = fundu::parse_duration)]
    report_interval: Duration,
}

#[derive(Debug, Clone, clap::Args)]
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
}

#[derive(Debug, Clone, clap::Args)]
#[group(required = true)]
struct DataSourceArgs {
    #[arg(long = "schema")]
    schema: Option<PathBuf>,
    #[arg(long = "csv-file")]
    csv_file: Option<PathBuf>,
}

#[derive(Debug, Clone, clap::Args)]
struct PayloadArgs {
    /// payload compression, support: gzip, lz4, snappy, zstd
    #[arg(long = "compress")]
    compress: Option<Compression>,
    /// payload encoding, support GBK, GB18030, BIG5
    #[arg(long = "encoding")]
    encoding: Option<Encoding>,
}

#[derive(Debug, Clone, clap::Args)]
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

    if args.report_interval < Duration::from_secs(1) {
        anyhow::bail!("report interval should > 1s");
    }

    let parallel = args
        .parallel
        .unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();

    let (data_tx, data_rx) = flume::bounded(parallel);
    match (&args.source.schema, args.source.csv_file.clone()) {
        (schema, Some(csv)) => {
            let has_schema = schema.is_some();
            #[derive(serde::Serialize, serde::Deserialize)]
            struct Schema<T> {
                base: FastStr,
                topic_patterns: Vec<FastStr>,
                parser: T,
            }
            let (mut parser, topic_fuzzer): (Option<fractal::PayloadParser>, Option<TopicFuzzer>) =
                match schema.as_ref() {
                    Some(schema) => {
                        let schema = tokio::fs::read_to_string(schema)
                            .await
                            .context("read schema file error")?;
                        let schema: Schema<fractal::PayloadParser> =
                            toml::from_str(&schema).context("parse schema file error")?;
                        Some((
                            schema.parser,
                            TopicFuzzer::new(schema.base, schema.topic_patterns),
                        ))
                        .unzip()
                    }
                    None => (None, None),
                };

            let headers = args.csv_headers.clone();

            #[derive(Debug, serde::Deserialize)]
            struct CsvData {
                topic: String,
                payload: String,
                qos: u8,
            }

            tasks.spawn_blocking({
                let cancel = cancel.clone();
                move || {
                    'outer: loop {
                        println!("===start read csv===");
                        let reader = csv_reader::new_reader(headers.is_none(), csv.clone())
                            .context("build csv reader error")?;
                        let headers = headers.clone().map(csv::StringRecord::from);
                        for data in reader {
                            if cancel.is_cancelled() {
                                break 'outer;
                            }
                            if data_tx.is_disconnected() {
                                break 'outer;
                            }
                            match data {
                                Ok(data) => {
                                    let mut data: CsvData = data
                                        .deserialize(headers.as_ref())
                                        .context("get csv data error")?;
                                    data.payload = data.payload.replace('\n', "\\n");
                                    let mut payload = match serde_json::from_str::<fractal::Payload>(
                                        &data.payload,
                                    ) {
                                        Ok(payload) => payload,
                                        Err(_) => continue,
                                    };
                                    if let Some(parser) = parser.as_ref() {
                                        parser.next_payload(&mut payload);
                                    }

                                    let topic = match topic_fuzzer.as_ref() {
                                        Some(fuzzer) => match fuzzer.fuzzy(&data.topic) {
                                            Ok(topic) => topic,
                                            Err(_) => continue,
                                        },
                                        None => data.topic,
                                    };
                                    let payload = serde_json::to_vec(&payload)
                                        .context("serialize data error")?;
                                    let data = Data::new(topic, payload, data.qos);
                                    if data_tx.send(data).is_err() {
                                        break 'outer;
                                    }
                                }
                                Err(e) => {
                                    println!("{e:#}")
                                }
                            }
                        }
                        if let Some(parser) = parser.as_mut() {
                            parser.update();
                        }
                        if !has_schema {
                            break
                        }
                    }
                    anyhow::Ok(())
                }
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
                        tasks.spawn(async move {
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
        (None, None) => anyhow::bail!("schema or csv file is required"),
    }

    let published = Arc::new(AtomicU64::default());
    for _ in 0..parallel {
        start_publisher(
            args.clone(),
            &mut tasks,
            data_rx.clone(),
            published.clone(),
            cancel.child_token(),
        )
        .await?;
    }
    drop(data_rx);

    tasks.spawn({
        let mut ticker = tokio::time::interval(args.report_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let cancel = cancel.clone();
        async move {
            let mut pre_published = published.load(Ordering::SeqCst);
            let start = Instant::now();
            let mut pre_inst = Instant::now();
            loop {
                if cancel.run_until_cancelled(ticker.tick()).await.is_none() {
                    break;
                };
                let duration = start.elapsed().as_millis();
                if duration == 0 {
                    continue;
                }
                let published = published.load(Ordering::SeqCst);
                let published_delta = published - pre_published;
                let mut duration_delta = pre_inst.elapsed().as_millis();
                if duration_delta == 0 {
                    duration_delta = duration;
                }
                pre_published = published;
                pre_inst = Instant::now();
                println!(
                    "published {published}, speed: {:.0}/s, avg speed: {:.0}/s",
                    (published_delta as f64 / duration_delta as f64) * 1000.0,
                    (published as f64 / duration as f64) * 1000.0
                );
                if args
                    .total_count
                    .is_some_and(|total| published as usize >= total)
                {
                    cancel.cancel();
                    break;
                }
            }
            println!("total published: {}", published.load(Ordering::SeqCst));
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
            res = tasks.join_next() => match res {
                Some(Ok(Err(e))) => {
                    println!("consumer error: {e:#}");
                },
                Some(Err(e)) => {
                    println!("consumer panic: {e}");
                }
                Some(Ok(_)) => continue,
                None => {
                    println!("all consumer exit");
                    break
                },
            }
        }
    }

    cancel.cancel();
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}

    Ok(())
}

async fn start_publisher(
    args: Args,
    tasks: &mut JoinSet<anyhow::Result<()>>,
    data_rx: flume::Receiver<Data<Vec<u8>>>,
    published: Arc<AtomicU64>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let client_id = format!("mqtt_pub_tool_{}", generate_random_string(10));
    println!("client_id: {}", client_id);

    let mut opts = MqttOptions::new(
        client_id,
        &args.connect.broker_host,
        args.connect.broker_port,
    );
    if let (Some(username), Some(password)) = (&args.connect.username, &args.connect.password) {
        opts.set_credentials(username, password);
    }
    opts.set_keep_alive(args.connect.keep_alive);
    let (client, mut event_loop) = rumqttc::v5::AsyncClient::new(opts, 1024);
    match event_loop
        .poll()
        .await
        .context("poll connect event error")?
    {
        Event::Incoming(Incoming::ConnAck(ConnAck {
            code: ConnectReturnCode::Success,
            ..
        })) => {}
        Event::Incoming(Incoming::ConnAck(ConnAck { code, .. })) => {
            anyhow::bail!("connect error, code: {code:?}");
        }
        _ => anyhow::bail!("expected connect packet"),
    }
    tasks.spawn({
        let token = cancel.clone();
        async move {
            loop {
                let Some(res) = token.run_until_cancelled(event_loop.poll()).await else {
                    break;
                };
                match res {
                    Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                        if matches!(ack.code, ConnectReturnCode::Success) {
                            println!("client connect sucessfully");
                        } else {
                            println!("connect error: {:?}", ack.code);
                            break;
                        }
                    }
                    Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                        if matches!(ack.reason, PubAckReason::Success) {
                            published.fetch_add(1, Ordering::SeqCst);
                        } else {
                            println!("publish error: {:?}", ack.reason);
                        }
                    }
                    Ok(Event::Incoming(Incoming::PubRec(ack))) => {
                        if matches!(ack.reason, PubRecReason::Success) {
                            published.fetch_add(1, Ordering::SeqCst);
                        } else {
                            println!("publish error: {:?}", ack.reason);
                        }
                    }
                    Ok(Event::Outgoing(Outgoing::Publish(0))) => {
                        published.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        println!("polling error: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }

            Ok(())
        }
    });

    tasks.spawn({
        let token = cancel.clone();
        let client = client.clone();
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
            while token.run_until_cancelled(stream.next()).await.is_some() {
                let Some(Ok(data)) = token.run_until_cancelled(data_rx.recv_async()).await else {
                    break;
                };

                let qos = qos(data.qos).unwrap();
                match token
                    .run_until_cancelled(client.publish(&data.topic, qos, false, data.payload))
                    .await
                {
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        println!("publish message error: {e:#}")
                    }
                    _ => break,
                }
            }

            Ok(())
        }
    });

    Ok(())
}

fn timeout_or_never(
    fut: Option<tokio::time::Instant>,
) -> Either<tokio::time::Sleep, std::future::Pending<()>> {
    match fut {
        Some(instant) => Either::Left(tokio::time::sleep_until(instant)),
        None => Either::Right(pending()),
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Data<T> {
    topic: String,
    payload: T,
    qos: u8,
}

impl<T> Data<T> {
    fn new(topic: String, payload: T, qos: u8) -> Self {
        Self {
            topic,
            payload,
            qos,
        }
    }
}

trait PayloadParser: Sized {
    type Payload;

    fn update(&mut self);

    fn next_payload(&self, payload: &mut Self::Payload);
}

mod fractal {
    use anyhow::Context;
    use chrono::{DateTime, TimeDelta, Utc};

    #[derive(Debug, serde::Deserialize)]
    pub struct PayloadParser {
        delta: Delta,
        #[serde(skip)]
        cur_delta: Delta,
    }

    #[derive(Debug, Default, serde_with::DeserializeFromStr)]
    struct Delta(TimeDelta);

    impl std::str::FromStr for Delta {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let duration = fundu::parse_duration(s).context("parse delta duration error")?;
            let delta = TimeDelta::from_std(duration).context("convert to timedelta error")?;
            Ok(Self(delta))
        }
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Payload {
        ts: DateTime<Utc>,
        value: Box<serde_json::value::RawValue>,
    }

    impl super::PayloadParser for PayloadParser {
        type Payload = Payload;

        fn update(&mut self) {
            self.cur_delta.0 += self.delta.0;
        }

        fn next_payload(&self, payload: &mut Self::Payload) {
            payload.ts += self.cur_delta.0
        }
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
value = { type = "option", value = { type = "string", random = { length = { range = { min = 10, max = 1000 } } } } }
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
random = { length = { range = { min = 10, max = 1000 } } }
        "#
            )?
        );
        Ok(())
    }
}
