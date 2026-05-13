use clap::Parser;
use taos::*;

#[derive(Debug, Parser)]
struct Opts {
    /// The target to connect to.
    #[clap(short, long, default_value = "taos://localhost:6030/ts5250")]
    target: String,

    /// Read raw data from directory.
    #[clap(short, long, default_value = "./")]
    raw_dir: String,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    unsafe { std::env::set_var("RUST_LOG", "trace") };
    pretty_env_logger::init();
    let opts = Opts::parse();
    let pool = TaosBuilder::from_dsn(&opts.target)?.pool()?;

    let conn = pool.get().await?;
    println!("Connected to {}", opts.target);

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
        .sorted()
        .collect::<Vec<_>>();

    for (idx, path) in raws.into_iter().enumerate() {
        println!("{}: press enter to write raw data from {:?}", idx, path);
        std::io::stdin().read_line(&mut String::new())?;
        let mut file = std::fs::File::open(&path)?;

        let raw: RawMeta = Inlinable::read_inlined(&mut file)?;
        conn.write_raw_meta(&raw).await?;
        println!("{}: write raw data from {:?} done", idx, path);
    }
    Ok(())
}
