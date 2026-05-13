use std::path::{Path, PathBuf};

use bytes::Bytes;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use persist_queue::{RawWriter, fs::FsQueue};
use rand::distributions::{Alphanumeric, DistString};
use tempfile::tempdir;
use tokio_stream::wrappers::ReadDirStream;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(long)]
    count: usize,
    #[arg(long)]
    payload_size: usize,
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
    let mut queue = queue_builder.build().await?;

    let mut writer = queue.new_writer().await?;

    let payload = generate_random_string(args.payload_size);
    let mut payloads = futures::stream::iter(std::iter::repeat_n(payload.as_bytes(), args.count))
        .map(Bytes::copy_from_slice)
        .ready_chunks(args.batch_size);

    let start = std::time::Instant::now();
    while let Some(chunk) = payloads.next().await {
        if chunk.is_empty() {
            break;
        }
        writer.write(chunk).await?;
    }
    writer.sync_data().await?;
    println!("used {:?}", start.elapsed());

    let read_dir = tokio::fs::read_dir(&dir).await?;
    let mut files = ReadDirStream::new(read_dir)
        .try_filter_map(|entry| async move {
            if entry.path().extension().is_none_or(|v| v != "seg") {
                return Ok(None);
            }
            let Some(segment_id) = parse_segment_id(entry.path()) else {
                return Ok(None);
            };
            let meta = entry.metadata().await?;
            Ok(Some((segment_id, meta.len(), entry.path())))
        })
        .try_collect::<Vec<_>>()
        .await?;
    files.sort_by(|(lid, _, _), (rid, _, _)| lid.cmp(rid));
    for (_, len, path) in files {
        println!("{}: {}", path.display(), format_file_size(len));
    }

    Ok(())
}

fn parse_segment_id(path: impl AsRef<Path>) -> Option<u64> {
    let path = path.as_ref();
    let filename = path.file_name()?.to_str()?;
    let segment_id_str = filename.strip_suffix(".seg")?;
    segment_id_str.parse().ok()
}

fn generate_random_string(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), length)
}

fn format_file_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.2} TiB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
