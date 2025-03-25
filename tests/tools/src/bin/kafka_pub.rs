use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;
use std::time::Instant;
use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use clap::Parser;
use crossterm::event::EventStream;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use taosx_tools::codec::{Encoding, Processor};
use taosx_tools::fake_json::DataFakeSchema;
use tokio::signal::ctrl_c;
use tokio::task::JoinSet;
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;

#[derive(Debug, clap::Parser)]
struct Args {
    #[clap(long = "schema", short = 'f')]
    schema: PathBuf,
    #[clap(long = "servers", short = 's', default_value = "localhost:9092")]
    servers: String,
    #[clap(long = "topic", short = 't')]
    topic: String,
    #[clap(long = "perallel", short = 'l', default_value_t = std::thread::available_parallelism().unwrap().get())]
    parallel: usize,
    #[clap(long = "interval", default_value = "100ms", value_parser = fundu::parse_duration)]
    interval: Duration,
    #[clap(long = "stdin", action = clap::ArgAction::SetTrue, conflicts_with = "interval")]
    stdin: bool,
    #[clap(
        long = "compress",
        help = "payload compression, support: gzip, lz4, snappy, zstd"
    )]
    compress: Option<String>,
    #[clap(
        long = "encoding",
        help = "payload encoding, support GBK, GB18030, BIG5"
    )]
    encoding: Option<Encoding>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let token = CancellationToken::new();

    let faker = Arc::new(DataFakeSchema::from_file(args.schema).unwrap());

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", args.servers);
    if let Some(compress) = args.compress {
        config.set("compression.type", compress);
    }

    let processor = args.encoding;
    let stdin = args.stdin;

    let published = Arc::new(AtomicU64::default());

    let mut join_set = JoinSet::new();

    join_set.spawn({
        let published = published.clone();
        let token = token.clone();
        async move {
            let start = Instant::now();
            loop {
                tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    let duration = start.elapsed().as_secs();
                    if duration == 0 {
                        continue;
                    }
                    let published = published.load(atomic::Ordering::Acquire);
                    println!("published {published}, speed: {}/s", published / duration);
                },
                _ = token.cancelled() => break,
                }
            }
        }
    });

    for _ in 0..args.parallel {
        join_set.spawn({
            let producer: FutureProducer = match  config.create() {
                Ok(p) => {
                    println!("create producer ok");
                    p
                },
                Err(e) => {
                    println!("create producer error: {e:#}");
                    return
                },
            };
            let topic = args.topic.clone();
            let faker = faker.clone();
            let token = token.clone();
            let published = published.clone();
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
                            let (tx, rx) = tokio::sync::oneshot::channel();
                            rayon::spawn({
                                let faker = faker.clone();
                                move || {
                                    let value = faker
                                        .rand_json_value()
                                        .context("gen fake data error")
                                        .and_then(|value| serde_json::to_vec(&value).context("serialize json error"))
                                        .and_then(|value| processor.process(value).context("processer process error"))
                                        .inspect_err(|e| println!("get value error: {e:#}"))
                                        .unwrap();
                                    tx.send(value).ok();
                                }
                            });
                            let Ok(value) = rx.await else {
                                continue
                            };
                            let record = FutureRecord::to(&topic).payload(&value).key("key");
                            tokio::select! {
                                _ = producer.send(record, Timeout::Never) => {
                                    published.fetch_add(1, atomic::Ordering::Release);
                                },
                                _ = token.cancelled() => break,
                            }
                        },
                        _ = token.cancelled() => break,
                    }
                }
                producer.flush(std::time::Duration::from_secs(5)).unwrap();
            }
        });
    }

    ctrl_c().await.ok();
    println!("received ctrl_c signal");
    token.cancel();
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}
}
