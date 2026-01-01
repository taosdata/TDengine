use std::{
    io::BufWriter,
    net::SocketAddr,
    num::NonZero,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
    time::Duration,
};

use anyhow::Context;
use arrow::{
    array::{BinaryArray, Int32Array},
    ipc::{
        CompressionType,
        reader::StreamReader,
        writer::{IpcWriteOptions, StreamWriter},
    },
};
use clap::Parser;

use signal_hook::{
    consts::{SIGHUP, SIGINT, SIGQUIT, SIGTERM},
    iterator::Signals,
};
use taosx_tools::fake_arrow;

#[derive(Debug, clap::Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Proxy(Proxy),
    Generate(Generate),
}

#[derive(Debug, clap::Args)]
struct Proxy {
    /// socket listen on
    #[arg(long)]
    from: SocketAddr,
    /// socket send bytes to
    #[arg(long)]
    to: SocketAddr,
}

#[derive(Debug, clap::Args)]
struct Generate {
    #[arg(long)]
    schema: PathBuf,
    #[arg(long)]
    to: SocketAddr,
    #[arg(long)]
    batch_size: usize,
    #[arg(long)]
    batch_parallel: Option<usize>,
    #[arg(long)]
    parallel: Option<usize>,
    #[arg(long, value_parser = fundu::parse_duration)]
    interval: Option<Duration>,
    #[arg(long, value_parser = fundu::parse_duration)]
    stat_interval: Option<Duration>,
    #[arg(long, action = clap::ArgAction::SetTrue)]
    compression: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Proxy(args) => proxy(args)?,
        Command::Generate(args) => generate(args)?,
    }

    Ok(())
}

fn proxy(args: Proxy) -> anyhow::Result<()> {
    let listener = std::net::TcpListener::bind(args.from)
        .with_context(|| format!("bind listener on {} error", args.from))?;
    println!("start listening on {}", args.from);

    for res in listener.incoming() {
        let mut batch_from_stream = res.context("accept connection error")?;
        let mut ack_to_stream = batch_from_stream
            .try_clone()
            .context("clone ack to stream error")?;
        let mut batch_to_stream = std::net::TcpStream::connect(args.to)
            .with_context(|| format!("connect to {} error", args.to))?;
        let mut ack_from_stream = batch_to_stream
            .try_clone()
            .context("clone ack from stream error")?;
        println!("connected to {}", args.to);
        std::thread::spawn(move || {
            if std::io::copy(&mut batch_from_stream, &mut batch_to_stream).is_err() {
                println!("batch stream copy exit");
                batch_from_stream.shutdown(std::net::Shutdown::Both).ok();
                batch_to_stream.shutdown(std::net::Shutdown::Both).ok();
            }
        });
        std::thread::spawn(move || {
            if std::io::copy(&mut ack_from_stream, &mut ack_to_stream).is_err() {
                println!("ack stream copy exit");
                ack_from_stream.shutdown(std::net::Shutdown::Both).ok();
                ack_to_stream.shutdown(std::net::Shutdown::Both).ok();
            }
        });
    }

    Ok(())
}

fn generate(args: Generate) -> anyhow::Result<()> {
    let stream = std::net::TcpStream::connect(args.to)
        .with_context(|| format!("connect {} error", args.to))?;
    stream
        .set_read_timeout(None)
        .context("set read timeout error")?;
    stream
        .set_write_timeout(None)
        .context("set write timeout error")?;
    set_tcp_keepalive(&stream).context("set tcp keepalive error")?;
    println!("connected to {}", args.to);

    let faker = Arc::new(
        fake_arrow::DataFaker::from_file(args.batch_size, args.schema)
            .context("create data faker error")?,
    );
    let parallel = args
        .parallel
        .or_else(|| {
            std::thread::available_parallelism()
                .map(NonZero::<usize>::get)
                .ok()
        })
        .context("cannot get parallel value")?;

    let (tx, rx) = flume::bounded(parallel);

    let (ack_stream, write_stream) = (
        stream.try_clone().context("clone ack stream error")?,
        stream.try_clone().context("clone write stream error")?,
    );

    let ack_count = Arc::new(AtomicU64::default());
    let batch_count = Arc::new(AtomicU64::default());
    let gen_count = Arc::new(AtomicU64::default());
    std::thread::spawn({
        let ack_count = ack_count.clone();
        let batch_count = batch_count.clone();
        let gen_count = gen_count.clone();
        move || {
            let mut last_ack = 0;
            let mut last_batch = 0;
            let mut last_gen = 0;
            let duration = args.stat_interval.unwrap_or_else(|| Duration::from_secs(5));
            loop {
                std::thread::sleep(duration);
                let curr_gen = gen_count.load(atomic::Ordering::SeqCst);
                let curr_batch = batch_count.load(atomic::Ordering::SeqCst);
                let curr_ack = ack_count.load(atomic::Ordering::SeqCst);
                println!(
                    "total    => gen: {}, sent: {}, acks: {}",
                    curr_gen, curr_batch, curr_ack
                );
                println!(
                    "interval => gen: {}, sent: {}, acks: {}",
                    curr_gen - last_gen,
                    curr_batch - last_batch,
                    curr_ack - last_ack,
                );
                println!();
                (last_gen, last_batch, last_ack) = (curr_gen, curr_batch, curr_ack);
            }
        }
    });

    let (permit_tx, permit_rx) = args
        .batch_parallel
        .filter(|v| *v > 0)
        .map(flume::bounded)
        .unzip();

    std::thread::spawn(move || {
        let Ok(ack_reader) = StreamReader::try_new_buffered(ack_stream, None) else {
            return;
        };
        for ack in ack_reader {
            if permit_rx.as_ref().is_some_and(|rx| rx.recv().is_err()) {
                break;
            }
            let ack = match ack {
                Ok(ack) => ack,
                Err(e) => {
                    println!("read ack error: {e:#}");
                    return;
                }
            };
            ack_count.fetch_add(1, atomic::Ordering::SeqCst);
            let code_array = {
                let code_column = ack.column_by_name("code").expect("code column not found");
                code_column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("code array not int32")
            };
            let message_array = {
                let message_column = ack
                    .column_by_name("message")
                    .expect("message column not found");
                message_column
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("message array not binary")
            };
            if code_array.value(0) != 0 {
                println!(
                    "ack failed: {}",
                    String::from_utf8(message_array.value(0).to_vec())
                        .expect("message invalid utf8")
                )
            }
        }
    });

    let mut opts = IpcWriteOptions::default();
    if args.compression {
        opts = opts
            .try_with_compression(Some(CompressionType::ZSTD))
            .context("try new ipc writer options error")?;
    }
    let mut writer =
        StreamWriter::try_new_with_options(BufWriter::new(write_stream), &faker.get_schema(), opts)
            .context("create stream writer error")?;
    let interval = args.interval;
    std::thread::spawn(move || {
        loop {
            if permit_tx.as_ref().is_some_and(|tx| tx.send(()).is_err()) {
                break;
            }
            let Ok(batch) = rx.recv() else {
                break;
            };
            if let Err(e) = writer.write(&batch) {
                println!("write batch error: {e}");
                break;
            }
            batch_count.fetch_add(1, atomic::Ordering::SeqCst);
            if let Some(interval) = interval {
                std::thread::sleep(interval);
            }
        }
        if let Err(e) = writer.finish() {
            println!("finish record batch writer error: {e}")
        }
    });

    for _ in 0..parallel {
        let faker = faker.clone();
        let batch_tx = tx.clone();
        let gen_count = gen_count.clone();
        std::thread::spawn(move || {
            loop {
                let batch = faker
                    .rand_record_batch()
                    .expect("generate record batch error");
                gen_count.fetch_add(1, atomic::Ordering::SeqCst);
                if let Some(interval) = interval {
                    std::thread::sleep(interval);
                }
                if batch_tx.send(batch).is_err() {
                    break;
                }
            }
        });
    }
    drop(tx);

    let mut signals = Signals::new([SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
    for signal in signals.wait() {
        println!("received signal: {signal}");
        stream.shutdown(std::net::Shutdown::Both).ok();
    }

    Ok(())
}

pub fn set_tcp_keepalive(stream: &std::net::TcpStream) -> anyhow::Result<()> {
    let sock_ref = socket2::SockRef::from(stream);
    let keep_alive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(10))
        .with_interval(Duration::from_secs(10));
    sock_ref.set_tcp_keepalive(&keep_alive)?;

    Ok(())
}
