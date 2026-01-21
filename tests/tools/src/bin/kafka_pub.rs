use std::sync::Arc;
use std::sync::atomic::{self, AtomicU64};
use std::time::Instant;
use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use clap::Parser;
use crossterm::event::EventStream;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, FutureProducer, FutureRecord, Producer};
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
    #[clap(long = "parallel", short = 'l', default_value_t = std::thread::available_parallelism().unwrap().get())]
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

    let faker = Arc::new(DataFakeSchema::from_file(args.schema).expect("parse schema from file"));

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
            let mut start = Instant::now();
            let mut last_published = 0;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        let duration = start.elapsed().as_secs();
                        if duration == 0 {
                            continue;
                        }
                        start = Instant::now();
                        let published = published.load(atomic::Ordering::Acquire);
                        let delta = published - last_published;
                        last_published = published;
                        println!("published {published}, speed: {}/s", delta / duration);
                    },
                    _ = token.cancelled() => break,
                }
            }
        }
    });
    let topic_name = &args.topic;
    let partitions = {
        let producer = config
            .create::<BaseProducer>()
            .expect("create base producer");
        let meta = producer
            .client()
            .fetch_metadata(Some(topic_name), Timeout::After(Duration::from_secs(10)))
            .expect("fetch metadata");
        let mut partitions = None;
        for topic in meta.topics() {
            if topic_name != topic.name() {
                continue;
            }
            partitions = Some(topic.partitions().len());
        }
        let Some(partitions) = partitions else {
            println!("no partitions found");
            return;
        };
        partitions
    };
    println!("partitions: {partitions}");

    for i in 0..args.parallel {
        join_set.spawn({
            let producer: FutureProducer = match config.create() {
                Ok(p) => {
                    println!("create producer ok");
                    p
                }
                Err(e) => {
                    println!("create producer error: {e:#}");
                    return;
                }
            };
            let topic_name = args.topic.clone();

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

                let (msg_tx, msg_rx) = flume::bounded(10000);
                std::thread::spawn(move || {
                    loop {
                        let value = faker
                            .rand_json_value()
                            .context("gen fake data error")
                            .and_then(|value| {
                                serde_json::to_vec(&value).context("serialize json error")
                            })
                            .and_then(|value| {
                                processor.process(value).context("processor process error")
                            })
                            .inspect_err(|e| println!("get value error: {e:#}"));
                        msg_tx.send(value).ok();
                    }
                });

                loop {
                    if token.run_until_cancelled(stream.next()).await.is_none() {
                        break;
                    }
                    let Some(Ok(value)) = token.run_until_cancelled(msg_rx.recv_async()).await
                    else {
                        break;
                    };
                    let value = match value {
                        Err(e) => {
                            println!("get value error: {e:#}");
                            continue;
                        }
                        Ok(value) => value,
                    };
                    let record = FutureRecord::to(&topic_name)
                        .payload(&value)
                        .partition((i % partitions) as _)
                        .key("key");
                    let Some(res) = token
                        .run_until_cancelled(producer.send(record, Timeout::Never))
                        .await
                    else {
                        break;
                    };
                    match res {
                        Ok(_) => {
                            published.fetch_add(1, atomic::Ordering::AcqRel);
                        }
                        Err((e, _)) => {
                            println!("publish message error: {e:#}");
                        }
                    }
                }
                if let Err(e) = producer.flush(std::time::Duration::from_secs(5)) {
                    println!("flush error: {e:#}");
                };
            }
        });
    }

    ctrl_c().await.ok();
    println!("received ctrl_c signal");
    token.cancel();
    while join_set.join_next().await.is_some() {}
}
