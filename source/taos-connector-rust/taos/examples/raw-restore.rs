use std::time::Instant;

use clap::Parser;
use taos::*;
use taos_query::common::RawData;

#[derive(Debug, Parser)]
struct Opts {
    /// The target to connect to.
    #[clap(short, long, default_value = "taos://localhost:6030/ts5250")]
    target: String,

    /// Read raw data from directory.
    #[clap(short, long, default_value = "./")]
    raw_dir: String,

    /// Repeat the last raw data by NUMBER of times.
    #[clap(short = 'N', long, default_value = "0")]
    repeats: usize,

    /// Interactive mode.
    #[clap(short, long)]
    interactive: bool,

    /// Number of workers.
    #[clap(short, long, default_value = "1")]
    workers: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // std::env::set_var("RUST_LOG", "trace");
    pretty_env_logger::init();
    let opts = Opts::parse();
    let pool = TaosBuilder::from_dsn(&opts.target)?.pool()?;

    let conn = pool.get().await?;
    eprintln!("Connected to {}", opts.target);

    let dir = std::fs::read_dir(&opts.raw_dir)?;

    let raws = dir
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() {
                let name = path.file_name().unwrap();
                if name
                    .to_str()
                    .map(|s| s.starts_with("raw_"))
                    .unwrap_or(false)
                {
                    Some(path)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .sorted_by(|a, b| {
            let na = a
                .file_name()
                .unwrap()
                .to_string_lossy()
                .split('_')
                .nth(1)
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let nb = b
                .file_name()
                .unwrap()
                .to_string_lossy()
                .split('_')
                .nth(1)
                .unwrap()
                .parse::<usize>()
                .unwrap();

            na.cmp(&nb)
        })
        .collect::<Vec<_>>();

    tracing::debug!("raws: {:?}", raws);

    let (meta, data): (Vec<_>, Vec<_>) = raws.into_iter().partition(|path| {
        let name = path.file_name().unwrap();
        name.to_str().map(|s| s.contains("meta")).unwrap_or(false)
    });

    let mut idx = 0;
    for path in meta {
        if opts.interactive {
            eprintln!("{}: press enter to write raw data from {:?}", idx, path);
            std::io::stdin().read_line(&mut String::new())?;
        }
        let mut file = std::fs::File::open(&path)?;
        let raw: RawMeta = Inlinable::read_inlined(&mut file)?;
        let instant = Instant::now();
        conn.write_raw_meta(&raw).await?;
        eprintln!("{}: write raw data from {:?} done", idx, path);
        println!(
            "{},{},{},{}",
            idx,
            path.display(),
            raw.raw_len(),
            instant.elapsed().as_millis()
        );
        idx += 1;
    }

    let mut set = tokio::task::JoinSet::new();
    let mut senders = Vec::with_capacity(opts.workers);
    for wid in 0..opts.workers {
        let conn = pool.get().await?;
        type Item = (usize, std::path::PathBuf, RawMeta, usize);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Item>(1024);
        set.spawn(async move {
            while let Some((idx, path, raw, size)) = rx.recv().await {
                let instant = Instant::now();
                if let Err(err) = conn.write_raw_meta(&raw).await {
                    eprintln!(
                        "w{wid:02}:i{:08}: write raw data from {:?} failed: {}",
                        idx, path, err
                    );
                    break;
                }
                // eprintln!("{}: write raw data from {:?} done", idx, path);
                println!(
                    "{},{},{},{},{}",
                    wid,
                    idx,
                    path.display(),
                    size,
                    instant.elapsed().as_millis()
                );
            }
        });
        senders.push(tx);
    }

    for rid in 0..=opts.repeats {
        if rid > 0 {
            eprintln!("repeat {} times", rid);
        }
        for path in data.iter() {
            if opts.interactive {
                eprintln!("{}: press enter to write raw data from {:?}", idx, path);
                std::io::stdin().read_line(&mut String::new())?;
            }
            let mut file = std::fs::File::open(path)?;
            let raw: RawData = Inlinable::read_inlined(&mut file)?;
            let size = raw.raw_len() as usize + 6;
            senders[idx % opts.workers]
                .send((idx, path.clone(), raw, size))
                .await?;
            idx += 1;
        }
    }

    drop(senders);

    while let Some(res) = set.join_next().await {
        res?;
        // eprintln!("{}: write raw data from {:?} done", idx, path);
    }

    Ok(())
}
