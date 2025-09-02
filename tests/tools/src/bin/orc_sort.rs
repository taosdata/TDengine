use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use anyhow::Context;
use clap::Parser;
use orc_rust::projection::ProjectionMask;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(short, long)]
    input: PathBuf,
    #[arg(short, long)]
    output: PathBuf,
    #[arg(long, default_value = "1000")]
    batch_size: usize,
    #[arg(long)]
    projection: Option<String>,
    #[arg(long)]
    create_table: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if !args.input.is_dir() {
        anyhow::bail!("dir {} not found", args.input.display())
    }
    let projection = args.projection.map(|s| {
        s.split(',')
            .map(|f| f.trim())
            .filter(|f| !f.is_empty())
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
    });

    let files = find_files_in_dir(&args.input)?;

    let schema = Arc::new(OnceLock::new());

    let (input_tx, input_rx) = std::sync::mpsc::sync_channel(64);
    for input in files {
        let input_tx = input_tx.clone();
        let schema = schema.clone();
        let projection = projection.clone();
        std::thread::spawn(move || {
            let handle = move || {
                let file = std::fs::File::open(&input).context("open orc file error")?;
                let mut builder = orc_rust::ArrowReaderBuilder::try_new(file)
                    .context("build orc reader error")?;
                builder = builder.with_batch_size(args.batch_size);

                let root_data_type = builder.file_metadata().root_data_type();
                let projection = match &projection {
                    Some(names) => ProjectionMask::named_roots(root_data_type, names),
                    None => ProjectionMask::all(),
                };
                builder = builder.with_projection(projection);
                schema.get_or_init(|| builder.schema());
                let reader = builder.build();

                for batch in reader {
                    let batch = batch.context("read batch error")?;
                    // arrow::util::pretty::print_batches(&[batch.clone()]).ok();
                    if input_tx.send(batch).is_err() {
                        break;
                    }
                }
                anyhow::Ok(())
            };
            if let Err(e) = handle() {
                println!("read batch task error: {e:#}")
            }
        });
    }
    drop(input_tx);

    let duck_path = args.input.join("data.duck");

    let conn = duckdb::Connection::open(duck_path).context("open duckdb error")?;
    conn.execute(&args.create_table, [])
        .context("create table error")?;

    let mut appender = conn.appender("orc").context("open duck appender error")?;
    for batch in input_rx {
        appender
            .append_record_batch(batch)
            .context("append batch error")?;
    }
    appender.flush().context("flush duckdb error")?;

    println!("write batches to duckdb done");

    let schema = schema.get().context("schema not found")?;

    let (output_tx, output_rx) = std::sync::mpsc::sync_channel(64);

    let schema_clone = schema.clone();
    let write_handle = std::thread::spawn(move || {
        let handle = move || {
            let output = std::fs::File::options()
                .create(true)
                .truncate(true)
                .write(true)
                .open(args.output)
                .context("open output file error")?;

            let mut writer = orc_rust::ArrowWriterBuilder::new(output, schema_clone)
                .with_batch_size(args.batch_size)
                .try_build()
                .context("build output writer error")?;
            for batch in output_rx {
                writer.write(&batch).context("write batch error")?;
            }

            writer.close().context("close output file error")?;
            anyhow::Ok(())
        };
        if let Err(e) = handle() {
            println!("write orc task error: {e:#}")
        }
    });

    let mut query_stmt = conn
        .prepare("SELECT * from orc order by gn")
        .context("duck prepare error")?;
    let stream = query_stmt
        .stream_arrow([], schema.clone())
        .context("stream arrow error")?;
    for batch in stream {
        if output_tx.send(batch).is_err() {
            break;
        }
    }
    drop(output_tx);

    write_handle.join().unwrap();

    Ok(())
}

fn find_files_in_dir(dir: impl AsRef<Path>) -> anyhow::Result<Vec<PathBuf>> {
    let read_dir = std::fs::read_dir(dir).context("read dir error")?;
    Ok(read_dir
        .filter_map(|entry| {
            entry.ok().and_then(|entry| {
                entry
                    .metadata()
                    .ok()
                    .filter(|metadata| metadata.is_file())
                    .map(|_| entry.path())
            })
        })
        .collect::<Vec<_>>())
}
