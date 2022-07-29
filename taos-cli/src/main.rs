use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use taos_cli::taoz::*;
use taos_sys::*;
use tokio::io::*;

use clap::Parser;

async fn sync<C: AsAsyncConsumer, Q: AsyncQueryable>(
    consumer: C,
    taos: Q,
    timeout: Timeout,
) -> Result<()>
where
    C::Error: std::error::Error + Sync + Send,
    <C::Meta as IsAsyncMeta>::Error: std::error::Error + Send + Sync + 'static,
    <C::Data as IsAsyncData>::Error: std::error::Error + Send + Sync + 'static,
    Q::Error: std::error::Error + Sync + Send + 'static,
    anyhow::Error: From<C::Error>,
{
    let mut stream = consumer.stream();

    while let Some((offset, message)) = stream.try_next().await? {
        match message {
            MessageSet::Meta(meta) => {
                dbg!(meta.as_json_meta().await?);
                taos.write_raw_meta(meta.as_raw_meta().await?).await?;
            }
            MessageSet::Data(data) => {
                while let Some(block) = data.fetch_raw_block().await? {
                    taos.write_raw_block(&block).await?;
                    dbg!(&block.nrows(), block.ncols());
                }
            }
        }
        consumer.commit(offset).await?;
    }
    Ok(())
}

async fn backup<T: AsyncWrite + Unpin + Send>(writer: ZCodec<T>, dsn: impl IntoDsn) -> Result<()> {
    let dsn = dsn.into_dsn()?;
    let mut tmq = TmqBuilder::from_dsn(&dsn)?.build()?;
    let db = dsn.database.unwrap();
    tmq.subscribe([db]).await?;
    let writer = Arc::new(Mutex::new(writer));

    let rows = tmq
        .stream()
        .map_err(anyhow::Error::from)
        .map_ok(|(offset, message)| async {
            let mut rows = 0;
            let mut writer = writer.lock().unwrap();
            match message {
                MessageSet::Meta(meta) => {
                    // dbg!(meta.as_json_meta().await?);
                    writer
                        .write_meta_async(&meta.as_raw_meta().await?)
                        .await
                        .unwrap();
                }
                MessageSet::Data(data) => {
                    writer.start_data_async().await.unwrap();
                    while let Some(block) = data.fetch_raw_block().await.unwrap() {
                        // dbg!(&block);
                        let len = writer.write_data_async(&block).await.unwrap();
                        rows += block.nrows();
                        // dbg!(len);
                        // log::info!("");
                        log::info!(
                            "table {} rows: {}",
                            block.table_name().unwrap(),
                            block.nrows()
                        );
                    }
                    writer.finish_data_async().await.unwrap();
                }
            }
            writer.flush().await.unwrap();
            tmq.commit(offset).await?;
            Result::<usize>::Ok(rows)
        })
        .try_fold(0, |sum, n| async move { Ok(n.await? + sum) })
        .await?;
    let mut writer = writer.lock().unwrap();
    writer.flush().await?;
    writer.shutdown().await?;
    log::info!("total backup {} rows", rows);
    Ok(())
}

async fn restore<T: AsyncRead + Unpin + Send>(mut reader: ZCodec<T>, taos: &Taos) -> Result<()> {
    let header = reader.header_async().await?;
    dbg!(header);

    // let mut rows = AtomicU64::new(0);
    let mut rows = 0;

    loop {
        let res = reader.read_message_async().await;
        match res {
            Ok(message) => match message {
                MessageSet::Meta(meta) => {
                    // dbg!(&meta);
                    taos.write_raw_meta(meta).await?
                }
                MessageSet::Data(data) => {
                    // dbg!(&data);
                    for raw in data {
                        rows += raw.nrows();
                        taos.write_raw_block(&raw).await?;
                    }
                    println!("rows: {}", rows);
                    // taos.write_raw_data(data[0]).await?
                }
            },
            Err(err) => {
                // dbg!(&err);
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                dbg!(&err);
                break;
            }
        }
    }
    println!("total {} rows", rows);
    Ok(())
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Algorithm {
    Brotli,
    Bzip2,
    Deflate,
    Gzip,
    Lzma,
    Xz,
    Zlib,
    Zstd,
}

// impl FromStr for Algorithm {
//     type Err = anyhow::Error;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s.to_lowercase() {
//             "brotli" => Algorithm::Brotli,
//             "bz" | "bzip2" => Algorithm::Bzip2,
//             "deflate" => Algorithm::Deflate,
//             "gz" | "gzip" => Algorithm::Gzip,
//             _ => todo!(),
//         }
//     }
// }
/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Input DSN(Data Source Name) string.
    ///
    /// Supported:
    ///
    /// - TMQ: TDengine message queue data stream, use as:
    ///
    ///     * `tmq://host:port/topics?group.id=STR&client.id=STR&timeout`.
    ///
    /// - Legacy query, use as:
    ///
    ///     * Database input: `taos://localhost:6030/database`
    ///
    ///     * Table input: `taos://host:port/database?from=Stb1&select=c1,c2,c3`, this will be queried as:
    ///       'select c1,c2,c3 from `database`.'
    ///
    /// - Local backup, use as `local:./path`.
    ///
    /// - CSV: `csv:/path/to/file.csv`.
    ///
    /// - Parquet: `parquet:/path/to/*.parq`.
    ///
    #[clap(short, long, value_parser)]
    from: Dsn,

    /// Output DSN.
    #[clap(short, long, value_parser)]
    to: Dsn,

    /// Algorithm
    #[clap(short, long, value_enum, default_value = "zstd")]
    algorithm: Algorithm,

    /// For verbosity print.
    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity,

    /// Number of jobs, default to 0, will use `jobs` number of works for TMQ.
    #[clap(short, long, value_parser, default_value = "0")]
    jobs: usize,

    /// When `endless` flag set, we'll re-write tmq timeout as `never` to wait messages
    /// without an ending, but it will still abort when there's error in the process.
    #[clap(short, long)]
    endless: bool,

    /// Override default TDengine connection protocol to websocket, both `from` and `to` will be affected.
    ///
    /// So that you don't need to append `+ws` in DSN.
    #[clap(short, long)]
    websocket: bool,

    /// Be careful to use this, we suggest only use it when failed at first time.
    ///
    /// We'll warn you various kind of risks before really running a task.
    #[clap(short, long)]
    yes_i_really_mean_it: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    pretty_env_logger::formatted_builder()
        .filter_level(args.verbose.log_level_filter())
        .init();

    match (args.from.driver.as_str(), args.to.driver.as_str()) {
        ("tmq", "taos") => {
            // td to td
            let database = args.from.database.take().expect("database not specified");
            let tmq = TmqBuilder::from_dsn(&args.from)?;
            let builder = TaosBuilder::from_dsn(&args.to)?;
            let mut consumer = tmq.build()?;
            consumer.subscribe([database]).await?;

            let taos = builder.build()?;

            sync(consumer, taos, Timeout::from_millis(500)).await?;
        }
        ("tmq", "local") => {
            // tmq to local backup
            todo!()
        }
        ("tmq", "csv") => {
            // tmq table to csv, write table records to csv format.
            todo!()
        }
        ("tmq", "parquet") => {
            // tmq table to parquet
            todo!()
        }
        ("csv", "taos") => {
            // CSV to TDengine
            todo!()
        }
        ("parquet", "taos") => {
            // parquet to TDengine
            todo!()
        }
        ("taos", "local") => {
            let taos = TaosBuilder::from_dsn(&args.from)?.build()?;
            taos.exec_many([
                "drop topic if exists abc1",
                "create topic abc1 with meta as database abc1",
                "use abc1",
            ])
            .await?;
            // backup
            match args.algorithm {
                Algorithm::Brotli => {
                    let writer = tokio::fs::File::create("abc1.test.brotli").await?;
                    let writer = async_compression::tokio::write::BrotliEncoder::new(writer);
                    let mut writer = ZCodec::new(writer);
                    writer
                        .write_head_async(&Header::new("db".to_string()))
                        .await?;

                    backup(writer, "taos:///abc1?group.id=1").await?;
                }
                Algorithm::Bzip2 => {
                    let writer = tokio::fs::File::create("abc1.test.bzip2").await?;
                    let writer = async_compression::tokio::write::BzEncoder::new(writer);
                    let mut writer = ZCodec::new(writer);
                    writer
                        .write_head_async(&Header::new("db".to_string()))
                        .await?;

                    backup(writer, "taos:///abc1?group.id=1").await?;
                }
                Algorithm::Deflate => {
                    let writer = tokio::fs::File::create("abc1.test.deflate").await?;
                    let writer = async_compression::tokio::write::DeflateEncoder::new(writer);
                    let mut writer = ZCodec::new(writer);
                    writer
                        .write_head_async(&Header::new("db".to_string()))
                        .await?;

                    backup(writer, "taos:///abc1?group.id=1").await?;
                }
                Algorithm::Gzip => {
                    let writer = tokio::fs::File::create("abc1.test.gzip").await?;
                    let writer = async_compression::tokio::write::GzipEncoder::new(writer);
                    let mut writer = ZCodec::new(writer);
                    writer
                        .write_head_async(&Header::new("db".to_string()))
                        .await?;

                    backup(writer, "taos:///abc1?group.id=1").await?;
                }
                Algorithm::Lzma => {
                    let writer = tokio::fs::File::create("abc1.test.lzma").await?;
                    let writer = async_compression::tokio::write::LzmaEncoder::new(writer);
                    let mut writer = ZCodec::new(writer);
                    writer
                        .write_head_async(&Header::new("db".to_string()))
                        .await?;

                    backup(writer, "taos:///abc1?group.id=1").await?;
                }
                Algorithm::Xz => {
                    let writer = tokio::fs::File::create("abc1.test.xz").await?;
                    let writer = async_compression::tokio::write::XzEncoder::new(writer);
                    let mut writer = ZCodec::new(writer);
                    writer
                        .write_head_async(&Header::new("db".to_string()))
                        .await?;

                    backup(writer, "taos:///abc1?group.id=1").await?;
                }
                Algorithm::Zlib => {
                    let writer = tokio::fs::File::create("abc1.test.zlib").await?;
                    let writer = async_compression::tokio::write::ZlibEncoder::new(writer);
                    let mut writer = ZCodec::new(writer);
                    writer
                        .write_head_async(&Header::new("db".to_string()))
                        .await?;

                    backup(writer, "taos:///abc1?group.id=1").await?;
                }
                Algorithm::Zstd => {
                    let writer = tokio::fs::File::create("abc1.test.zstd").await?;
                    let writer = async_compression::tokio::write::ZstdEncoder::new(writer);
                    let mut writer = ZCodec::new(writer);
                    writer
                        .write_head_async(&Header::new("db".to_string()))
                        .await?;

                    backup(writer, "taos:///abc1?group.id=1").await?;
                }
                _ => todo!(),
            }
        }
        ("local", "taos") => {
            // restore
            let database = args.to.database.take();
            log::info!("dsn: {}", args.to);
            let taos = TaosBuilder::from_dsn(&args.to)?.build()?;
            match args.algorithm {
                Algorithm::Brotli => {
                    let suffix = "brotli";
                    taos.exec_many([
                        format!("drop database if exists {}", suffix),
                        format!("create database {}", suffix),
                        format!("use {}", suffix),
                    ])
                    .await?;
                    let reader = tokio::fs::File::open(format!("abc1.test.{}", suffix)).await?;
                    let reader = tokio::io::BufReader::new(reader);
                    let reader = async_compression::tokio::bufread::BrotliDecoder::new(reader);
                    let reader = ZCodec::new(reader);
                    restore(reader, &taos).await?;
                }
                Algorithm::Bzip2 => {
                    let suffix = "bzip2";
                    taos.exec_many([
                        format!("drop database if exists {}", suffix),
                        format!("create database {}", suffix),
                        format!("use {}", suffix),
                    ])
                    .await?;
                    let reader = tokio::fs::File::open(format!("abc1.test.{}", suffix)).await?;
                    let reader = tokio::io::BufReader::new(reader);
                    let reader = async_compression::tokio::bufread::BzDecoder::new(reader);
                    let reader = ZCodec::new(reader);
                    restore(reader, &taos).await?;
                }
                Algorithm::Deflate => {
                    let suffix = "deflate";
                    taos.exec_many([
                        format!("drop database if exists {}", suffix),
                        format!("create database {}", suffix),
                        format!("use {}", suffix),
                    ])
                    .await?;
                    let reader = tokio::fs::File::open(format!("abc1.test.{}", suffix)).await?;
                    let reader = tokio::io::BufReader::new(reader);
                    let reader = async_compression::tokio::bufread::DeflateDecoder::new(reader);
                    let reader = ZCodec::new(reader);
                    restore(reader, &taos).await?;
                }
                Algorithm::Gzip => {
                    let suffix = "gzip";
                    taos.exec_many([
                        format!("drop database if exists {}", suffix),
                        format!("create database {}", suffix),
                        format!("use {}", suffix),
                    ])
                    .await?;
                    let reader = tokio::fs::File::open(format!("abc1.test.{}", suffix)).await?;
                    let reader = tokio::io::BufReader::new(reader);
                    let reader = async_compression::tokio::bufread::GzipDecoder::new(reader);
                    let reader = ZCodec::new(reader);
                    restore(reader, &taos).await?;
                }
                Algorithm::Lzma => {
                    let suffix = "lzma";
                    taos.exec_many([
                        format!("drop database if exists {}", suffix),
                        format!("create database {}", suffix),
                        format!("use {}", suffix),
                    ])
                    .await?;
                    let reader = tokio::fs::File::open(format!("abc1.test.{}", suffix)).await?;
                    let reader = tokio::io::BufReader::new(reader);
                    let reader = async_compression::tokio::bufread::LzmaDecoder::new(reader);
                    let reader = ZCodec::new(reader);
                    restore(reader, &taos).await?;
                }
                Algorithm::Xz => {
                    let suffix = "xz";
                    taos.exec_many([
                        format!("drop database if exists {}", suffix),
                        format!("create database {}", suffix),
                        format!("use {}", suffix),
                    ])
                    .await?;
                    let reader = tokio::fs::File::open(format!("abc1.test.{}", suffix)).await?;
                    let reader = tokio::io::BufReader::new(reader);
                    let reader = async_compression::tokio::bufread::XzDecoder::new(reader);
                    let reader = ZCodec::new(reader);
                    restore(reader, &taos).await?;
                }
                Algorithm::Zlib => {
                    let suffix = "zlib";
                    taos.exec_many([
                        format!("drop database if exists {}", suffix),
                        format!("create database {}", suffix),
                        format!("use {}", suffix),
                    ])
                    .await?;
                    let reader = tokio::fs::File::open(format!("abc1.test.{}", suffix)).await?;
                    let reader = tokio::io::BufReader::new(reader);
                    let reader = async_compression::tokio::bufread::ZlibDecoder::new(reader);
                    let reader = ZCodec::new(reader);
                    restore(reader, &taos).await?;
                }
                Algorithm::Zstd => {
                    let suffix = "zstd";
                    taos.exec_many([
                        format!("drop database if exists {}", suffix),
                        format!("create database {}", suffix),
                        format!("use {}", suffix),
                    ])
                    .await?;
                    let reader = tokio::fs::File::open(format!("abc1.test.{}", suffix)).await?;
                    let reader = tokio::io::BufReader::new(reader);
                    let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);
                    let reader = ZCodec::new(reader);
                    restore(reader, &taos).await?;
                }
                _ => todo!(),
            }
        }
        _ => todo!(),
    }

    Ok(())
}
