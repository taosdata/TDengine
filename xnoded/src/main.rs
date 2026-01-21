mod api;
mod controller;
mod entrypoint;
mod log;
mod monitor;
mod rebalancer;
mod utils;

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use tracing::level_filters::LevelFilter;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

shadow_rs::shadow!(build);

const CLAP_SHORT_VERSION: &str = if build::GIT_CLEAN {
    const_format::concatcp!(
        "version: ",
        build::TD_VERSION,
        " (core-",
        build::PKG_VERSION,
        ")\ngit: ",
        build::COMMIT_HASH,
        "\nbuild: ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
} else {
    const_format::concatcp!(
        "version: ",
        build::TD_VERSION,
        " (core-dirty-",
        build::PKG_VERSION,
        ")\ngit: ",
        build::COMMIT_HASH,
        "\nbuild: ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
};

#[derive(Debug, clap::Parser)]
#[clap(version = CLAP_SHORT_VERSION)]
struct Args {
    #[arg(env = "XNODED_CFG_DIR")]
    cfg_dir: String,
    #[arg(env = "XNODED_LEADER_EP")]
    leader_ep: String,
    #[arg(env = "XNODED_USER_PASS")]
    user_pass: String,
    #[arg(env = "XNODED_CLUSTER_ID")]
    cluster_id: String,
    #[command(flatten)]
    log: LogOpts,
    #[arg(env = "XNODED_LISTEN")]
    listen: Option<String>,
    #[arg(env = "XNODED_ENGINE_DSN")]
    taos_dsn: Option<String>,
}

#[derive(Parser, Debug, Clone, Default)]
struct LogOpts {
    /// Log path.
    #[arg(env = "XNODED_LOG_DIR")]
    path: PathBuf,

    /// Log level.
    #[arg(env = "XNODED_LOG_LEVEL")]
    level: Option<LevelFilter>,

    /// Enable compress for log files.
    #[arg(env = "XNODED_LOG_COMPRESS")]
    compress: Option<bool>,

    /// Rotation count for log files.
    #[arg(env = "XNODED_LOG_ROTATION_COUNT")]
    rotation_count: Option<u16>,

    /// Keep days for log files.
    #[arg(env = "XNODED_LOG_KEEP_DAYS")]
    keep_days: Option<u16>,

    /// Rotation size for log files.
    #[arg(env = "XNODED_LOG_ROTATION_SIZE")]
    rotation_size: Option<String>,

    /// Reserved disk size for log files.
    #[arg(env = "XNODED_LOG_RESERVED_DISK_SIZE")]
    reserved_disk_size: Option<String>,
}

fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();

    // Set a panic hook
    std::panic::set_hook(Box::new(|info| {
        // 正常打印 backtrace, 需要设置环境变量: RUST_BACKTRACE=1
        let backtrace = std::backtrace::Backtrace::capture();
        tracing::error!("panic occurred. {} {}", info, backtrace);
    }));

    log::init(&args).context("init logger error")?;
    print_args(&args);

    let _guard = utils::defer::defer(|| tracing::info!("main exited"));

    if let Err(e) = entrypoint::run(args) {
        tracing::error!("failed to run entrypoint: {e:#}");
        return Err(e);
    }

    Ok(())
}

fn print_args(args: &Args) {
    tracing::info!("================config================");
    tracing::info!("cfg_dir: {}", args.cfg_dir);
    tracing::info!("leader_ep: {}", args.leader_ep);
    tracing::info!("cluster_id: {}", args.cluster_id);
    tracing::info!(
        "listen: {}",
        args.listen.as_deref().unwrap_or("0.0.0.0:6051")
    );
    if let Some(taos_dsn) = args.taos_dsn.as_ref() {
        tracing::info!("engine dsn: {}", taos_dsn);
    }
    tracing::info!("log.path: {}", args.log.path.display());
    if let Some(level) = args.log.level.as_ref() {
        tracing::info!("log.level: {}", level);
    }
    if let Some(compress) = args.log.compress.as_ref() {
        tracing::info!("log.compress: {}", compress);
    }
    if let Some(rotation_count) = args.log.rotation_count.as_ref() {
        tracing::info!("log.rotation_count: {}", rotation_count);
    }
    if let Some(keep_days) = args.log.keep_days.as_ref() {
        tracing::info!("log.keep_days: {}", keep_days);
    }
    if let Some(rotation_size) = args.log.rotation_size.as_ref() {
        tracing::info!("log.rotation_size: {}", rotation_size);
    }
    if let Some(reserved_disk_size) = args.log.reserved_disk_size.as_ref() {
        tracing::info!("log.reserved_disk_size: {}", reserved_disk_size);
    }
    tracing::info!("================config================");
}
