use std::path::PathBuf;

use clap::Parser;
use persist_queue::{
    RawReader,
    fs::{FsQueue, ReadFrom},
};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(long)]
    count: usize,
    #[arg(long)]
    batch_size: usize,
    #[arg(long)]
    dir: Option<PathBuf>,
    #[arg(long)]
    segment_size: Option<usize>,
    #[arg(long)]
    buffer_size: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cancel = CancellationToken::new();

    let (_temp, dir) = args.dir.map(|v| (None, v)).unwrap_or_else(|| {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        (Some(dir), path)
    });

    let mut queue_builder = FsQueue::builder(&dir);
    if let Some(segment_size) = args.segment_size {
        queue_builder = queue_builder.segment_size(segment_size);
    }
    if let Some(buffer_size) = args.buffer_size {
        queue_builder = queue_builder.buffer_size(buffer_size);
    }
    let queue = queue_builder.build().await?;

    let mut reader = queue.new_reader(ReadFrom::Earliest).await?;

    let start = std::time::Instant::now();
    let mut count = 0;
    loop {
        let entries = reader
            .read_util(1, args.batch_size.min(args.count - count), None, &cancel)
            .await?;
        if entries.is_empty() {
            continue;
        }
        count += entries.len();
        if count >= args.count {
            break;
        }
    }
    println!("used {:?}", start.elapsed());

    Ok(())
}
