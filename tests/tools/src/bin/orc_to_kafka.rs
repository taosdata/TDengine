use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use orc_rust::ArrowReaderBuilder;
use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
    ClientConfig,
};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(short, long)]
    input: PathBuf,
    #[arg(short, long)]
    broker: String,
    #[arg(short, long)]
    topic: String,
    #[arg(long, default_value = "1000")]
    batch_size: usize,
    #[arg(long)]
    projection: Option<String>,
    #[arg(long, short = 'l')]
    parallel: Option<usize>,
    #[arg(long, short)]
    producers: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cancel = CancellationToken::new();

    let parallel = args
        .parallel
        .unwrap_or_else(|| std::thread::available_parallelism().unwrap().get());
    let producers = args.producers.unwrap_or(parallel * 10);

    let (message_tx, message_rx) = flume::bounded(parallel * 10000);

    let mut tasks = JoinSet::new();

    let input = args.input.clone();
    tasks.spawn(async move {
        let file = tokio::fs::File::open(&input)
            .await
            .context("open orc file error")?;
        let mut builder = ArrowReaderBuilder::try_new_async(file)
            .await
            .context("build orc async reader error")?;
        builder = builder.with_batch_size(args.batch_size);

        let mut reader = builder.build_async();

        let permits = Arc::new(Semaphore::new(parallel * 10));
        while let Some(batch) = reader.next().await {
            let permit = permits
                .clone()
                .acquire_owned()
                .await
                .context("acquire permit error")?;
            let message_tx = message_tx.clone();
            rayon::spawn(move || {
                let handle = move || {
                    let batch = batch.context("read batch error")?;
                    let mut writer = arrow::json::ArrayWriter::new(Vec::new());
                    writer.write(&batch).context("write batch json error")?;
                    writer.finish().context("finish json writer error")?;
                    let buf = writer.into_inner();
                    let rows: Vec<serde_json::Value> =
                        serde_json::from_slice(&buf).context("deserialize json batch error")?;
                    for row in rows {
                        if message_tx.send(row).is_err() {
                            break;
                        }
                    }
                    drop(permit);
                    anyhow::Ok(())
                };
                if let Err(e) = handle() {
                    println!("rayon error: {e}")
                }
            });
        }
        anyhow::Ok(())
    });

    let count = Arc::new(AtomicU64::default());

    tokio::spawn({
        let count = count.clone();
        let cancel = cancel.clone();
        async move {
            loop {
                if cancel
                    .run_until_cancelled(tokio::time::sleep(Duration::from_secs(5)))
                    .await
                    .is_none()
                {
                    break;
                } else {
                    println!("published {} messages", count.load(Ordering::SeqCst));
                }
            }
        }
    });

    for idx in 0..producers {
        let count = count.clone();
        let broker = args.broker.clone();
        let topic = args.topic.clone();
        let message_rx = message_rx.clone();
        tasks.spawn(async move {
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", broker);
            let producer: FutureProducer = match config.create() {
                Ok(p) => {
                    println!("create producer ok");
                    p
                }
                Err(e) => {
                    println!("create producer error: {e:#}");
                    return Ok(());
                }
            };
            let metadata = producer
                .client()
                .fetch_metadata(Some(&topic), Timeout::Never)
                .context("fetch metadata error")?;
            let partitions = metadata
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .map(|v| v.partitions().len())
                .context("no partitions found")?;
            let partition = (idx % partitions) as i32;
            println!("start send to partition {partition}");
            while let Ok(row) = message_rx.recv_async().await {
                let payload = row.to_string();
                let record = FutureRecord::to(&topic)
                    .partition(partition)
                    .key("a")
                    .payload(&payload);
                match producer.send(record, Timeout::Never).await {
                    Ok(_) => {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                    Err((e, _)) => {
                        println!("publish error: {e}")
                    }
                }
            }
            Ok(())
        });
    }

    for res in tasks.join_all().await {
        if let Err(e) = res {
            println!("task exit with error: {e}")
        }
    }

    Ok(())
}
