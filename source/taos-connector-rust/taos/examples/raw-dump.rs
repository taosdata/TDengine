use std::ops::Deref;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::bail;
use clap::Parser;
use sync::MessageSet;
use taos::*;

#[derive(Debug, Parser)]
struct Opts {
    /// User
    #[clap(short, long, default_value = "root")]
    user: String,
    /// Password
    #[clap(short = 'P', long, default_value = "taosdata")]
    pass: String,
    /// The target to connect to.
    #[clap(long, default_value = "localhost")]
    host: String,
    #[clap(long, default_value = "6030")]
    port: u16,
    /// The topic to consume from.
    #[clap(short, long, required = true)]
    topic: String,

    /// Poll timeout
    #[clap(long)]
    timeout: Option<String>,
    /// Consumer group
    #[clap(long)]
    group: Option<String>,

    /// Set options.
    ///
    /// By default, we already set
    /// 1. experimental.snapshot.enable=true
    /// 2. auto.offset.reset=latest
    /// 3. client.id=raw-dump .
    #[clap(short, long)]
    set: Vec<String>,

    /// Interactive mode.
    #[clap(short, long)]
    interactive: bool,

    /// Read raw data from directory.
    #[clap(short, long, default_value = "./")]
    raw_dir: PathBuf,

    /// Print data block
    #[clap(short, long)]
    print_block: bool,

    /// Do not dump meta message.
    #[clap(long)]
    no_meta: bool,

    /// Do not dump metadata message (`insert into .. using ...`).
    #[clap(long)]
    no_metadata: bool,

    /// Do not commit any offset
    #[clap(long)]
    no_commit: bool,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // std::env::set_var("RUST_LOG", "trace");
    pretty_env_logger::formatted_timed_builder()
        .parse_default_env()
        .try_init()?;

    let mut args = Opts::parse();
    if args.raw_dir.exists() {
        if !args.raw_dir.is_dir() {
            bail!("{} is not a directory", args.raw_dir.display());
        }
    } else {
        std::fs::create_dir_all(&args.raw_dir)?;
    }

    let mut dsn = Dsn::default();
    dsn.driver = "tmq".to_string();
    dsn.username = Some(args.user);
    dsn.password = Some(args.pass);
    dsn.subject = Some(args.topic);

    dsn.addresses.push(Address::new(args.host, args.port));

    if let Some(timeout) = &args.timeout {
        dsn.set("timeout", timeout);
    }
    if let Some(group) = &args.group {
        dsn.set("group.id", group);
    }

    for s in &args.set {
        tracing::debug!("set: {}", s);
        if let Some((k, v)) = s.split_once('=') {
            dsn.set(k.trim(), v.trim());
        } else {
            dsn.set(s.trim(), "");
        }
    }

    let topic = dsn
        .subject
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing topic"))?;

    // subscribe
    let group = chrono::Local::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow::anyhow!("can't get nanosecond timestamp"))?;
    if dsn.get("group.id").is_none() {
        dsn.params.insert("group.id".to_string(), group.to_string());
    }
    eprintln!("group id: {}", dsn.get("group.id").unwrap());

    tracing::debug!("Full dsn: {dsn}");

    let tmq = TmqBuilder::from_dsn(&dsn)?;

    let mut consumer = tmq.build().await?;
    consumer.subscribe([&topic]).await?;
    // let assignment = consumer.assignments().await;
    // println!("assignments: {:?}", assignment);

    let mut last_offset = None;
    let blocks_cost = Duration::ZERO;
    if args.interactive {
        println!("> Interactive mode, press q to quit, c or nothing to continue, m to commit, off to continue and set interactive mode off:");

        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf)?;
        let buf = buf.trim();
        match buf {
            "q" | "quit" => return Ok(()),
            "m" => {
                println!("no offset to commit");
            }
            "off" => {
                args.interactive = false;
            }
            _ => {}
        }
    }
    {
        // let mut stream = consumer.stream();
        eprintln!("start consuming");

        let begin = Instant::now();
        let u = consumer.default_timeout();
        tracing::info!("{:?}", u);

        let mut mid = 0;
        println!("id,type,size");
        while let Some((offset, message)) = consumer.recv_timeout(u).await? {
            // println!("{mid} offset: {:?}", offset);
            // get information from offset
            match message {
                MessageSet::Meta(meta) => {
                    if args.no_meta {
                        continue;
                    }
                    let raw = meta.as_raw_meta().await?;
                    let bytes = raw.to_bytes();
                    println!("{mid},meta,{}", bytes.len());
                    let path = args.raw_dir.join(format!("raw_{}_meta.bin", mid));
                    std::fs::write(path, bytes.deref())?;
                }
                MessageSet::Data(data) => {
                    // println!("{mid} data: {:?}", data);
                    let raw = data.as_raw_data().await?;
                    let bytes = raw.to_bytes();
                    println!("{mid},data,{}", bytes.len());

                    if args.print_block {
                        while let Some(block) = data.fetch_raw_block().await? {
                            // one block for one table, get table name if needed
                            let name = block.table_name();
                            let nrows = block.nrows();
                            let ncols = block.ncols();
                            if let Some(name) = name {
                                eprintln!(
                                    "data[{mid}] block with table name {}, cols: {}, rows: {}",
                                    name, ncols, nrows
                                );
                                eprintln!("{}", block.pretty_format());
                            } else {
                                eprintln!(
                                    "data[{mid}] block without table name, cols: {}, rows: {}",
                                    ncols, nrows
                                );
                                eprintln!("{}", block.pretty_format());
                            }
                        }
                    }
                    let path = args.raw_dir.join(format!("raw_{}_data.bin", mid));
                    std::fs::write(path, bytes.deref())?;
                }
                MessageSet::MetaData(meta, data) => {
                    if args.no_metadata {
                        continue;
                    }
                    // println!("{mid} meta data: {:?}", meta);
                    let raw = meta.as_raw_meta().await?;
                    let bytes = raw.to_bytes();
                    println!("{mid},metadata,{}", bytes.len());
                    if args.print_block {
                        while let Some(block) = data.fetch_raw_block().await? {
                            // one block for one table, get table name if needed
                            let name = block.table_name();
                            let nrows = block.nrows();
                            let ncols = block.ncols();
                            if let Some(name) = name {
                                eprintln!(
                                    "data[{mid}] block with table name {}, cols: {}, rows: {}",
                                    name, ncols, nrows
                                );
                                eprintln!("{}", block.pretty_format());
                            } else {
                                eprintln!(
                                    "data[{mid}] block without table name, cols: {}, rows: {}",
                                    ncols, nrows
                                );
                                eprintln!("{}", block.pretty_format());
                            }
                        }
                    }
                    let path = args.raw_dir.join(format!("raw_{}_metadata.bin", mid));
                    std::fs::write(path, bytes.deref())?;
                }
            }
            mid += 1;

            if args.interactive {
                println!("> Interactive mode, press q to quit, c or nothing to continue, m to commit, off to continue and set interactive mode off:");
                let mut buf = String::new();
                std::io::stdin().read_line(&mut buf)?;
                let buf = buf.trim();
                match buf {
                    "q" | "quit" => break,
                    "m" => {
                        consumer.commit(offset).await?;
                    }
                    "off" => {
                        args.interactive = false;
                    }
                    _ => {}
                }
                tracing::info!(mid, "polling next message");
            } else {
                if !args.no_commit {
                    consumer.commit(offset).await?;
                } else {
                    last_offset = Some(offset);
                }
            }
        }
        drop(last_offset);
        eprintln!("total cost: {:?}", begin.elapsed());
    }
    eprintln!("blocks cost: {:?}", blocks_cost);

    consumer.unsubscribe().await;
    Ok(())
}
