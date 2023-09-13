use anyhow::{bail, Result};
use chrono::Local;
use clap::{Parser, Subcommand};
use const_format::concatcp;
use file_rotate::{
    compression::Compression,
    suffix::{AppendTimestamp, DateFrom, FileLimit},
    ContentLimit, FileRotate, TimeFrequency,
};
use metrics_tracing_context::MetricsLayer;
use shadow_rs::shadow;

use taosx_core::{
    get_log_dir, get_log_keep_days, valid_env_log_keep_days, ENV_TAOSX_LOGS_KEEP_DAYS,
};
#[cfg(feature = "tikv_jemallocator")]
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(feature = "tikv_jemallocator")]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

use time::{macros::format_description, UtcOffset};
use tracing_subscriber::{
    fmt::{format::FmtSpan, time::OffsetTime},
    prelude::*,
    EnvFilter,
};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

shadow!(build);

const CLAP_SHORT_VERSION: &str = if build::GIT_CLEAN {
    concatcp!(
        build::PKG_VERSION,
        "-",
        build::SHORT_COMMIT,
        " (built ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME,
        ")"
    )
} else {
    concatcp!(
        build::PKG_VERSION,
        "-",
        build::SHORT_COMMIT,
        "-dirty",
        " (built ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME,
        ")"
    )
};

mod run;
mod serve;

fn fmt_span_from_str(s: &str) -> Result<FmtSpan, String> {
    match s {
        "none" => Ok(FmtSpan::NONE),
        "full" => Ok(FmtSpan::FULL),
        "new" => Ok(FmtSpan::NEW),
        "enter" => Ok(FmtSpan::ENTER),
        "exit" => Ok(FmtSpan::EXIT),
        "active" => Ok(FmtSpan::ACTIVE),
        _ => Err(s.to_string()),
    }
}

#[derive(Debug, Parser, Clone)]
pub(crate) struct GlobalOpts {
    /// For verbosity print.
    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::WarnLevel>,

    /// Number of jobs, default to 0, will use `jobs` number of works for TMQ.
    #[clap(short, long, value_parser, default_value = "0", global = true)]
    jobs: usize,

    /// Enable debug will set the mod path as `file:line`.
    #[clap(short, long, global = true)]
    debug: bool,

    /// Log keep days.
    #[clap(long, global = true, env = "LOG_KEEP_DAYS")]
    log_keep_days: Option<i64>,

    /// Be careful to use this, we suggest only use it when failed at first time.
    ///
    /// We'll warn you various kind of risks before really running a task.
    #[clap(short, long, global = true)]
    yes_i_really_mean_it: bool,

    /// Enable OpenTelemetry tracing and metrics exporter.
    #[clap(long, global = true, action = clap::ArgAction::SetTrue, env = "ENABLE_OTEL")]
    otel: Option<bool>,

    #[clap(
        // short = 'e',
        long,
        global = true,
        default_value = "enter",
        value_parser = fmt_span_from_str,
        env = "TRACING_EVENTS"
    )]
    tracing_events: FmtSpan,
}

impl GlobalOpts {
    pub fn executor_worker_threads(&self) -> usize {
        let min = std::thread::available_parallelism()
            .map(|v| v.get() * 2)
            .unwrap_or(16)
            .max(16);

        if self.jobs + 2 > min {
            self.jobs + 2
        } else {
            min
        }
    }
}

#[derive(Subcommand, Debug)]
enum Commands {
    Run(run::Cli),
    Serve(serve::Cli),
    #[clap(external_subcommand)]
    External(Vec<String>),
}
fn set_env_log_keep_days(config: Option<i64>) {
    if let Some(log_keep_days) = config {
        if log_keep_days > 0 && valid_env_log_keep_days().is_none() {
            std::env::set_var(ENV_TAOSX_LOGS_KEEP_DAYS, log_keep_days.to_string());
        }
    }
}
#[derive(Parser, Debug)]
#[clap(
    name = build::CUS_CLI_NAME,
    author, version = CLAP_SHORT_VERSION,
    about = build::CUS_CLI_ABOUT,
    long_about = build::CUS_CLI_ABOUT)]
struct Args {
    #[clap(flatten)]
    globals: GlobalOpts,
    #[clap(subcommand)]
    commands: Option<Commands>,
    #[clap(last = true, value_parser)]
    slop: Vec<String>,
}

const ENV_PLUGINS_HOME: &'static str = "PLUGINS_HOME";
const ENV_TAOSX_PLUGINS_HOME: &'static str = concatcp!(build::CUS_PROMPT, "_PLUGINS_HOME");

fn build_runtime(
    worker_threads: usize,
) -> std::result::Result<tokio::runtime::Runtime, std::io::Error> {
    tokio::runtime::Builder::new_multi_thread()
        .disable_lifo_slot()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(b"taosx rng seed"))
        .global_queue_interval(31)
        .max_blocking_threads(4096)
        .thread_name("taosx")
        .worker_threads(worker_threads)
        .enable_all()
        .build()
}
fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    let version = build::PKG_VERSION;
    let commit_id = build::COMMIT_HASH;
    let build_time = build::BUILD_TIME;
    // println!("taosx version: {CLAP_SHORT_VERSION}");
    println!("taosx version: {version}");
    println!("commit id: {commit_id}");
    println!("build time: {build_time}");

    let plugins_home = std::env::var(ENV_PLUGINS_HOME).or(std::env::var(ENV_TAOSX_PLUGINS_HOME));
    match plugins_home {
        Ok(home) => std::env::set_var(ENV_PLUGINS_HOME, home),
        Err(_) => {
            #[cfg(unix)]
            {
                let default = "/usr/local/taosx/plugins";
                let path = std::path::Path::new(default);
                if path.exists() {
                    std::env::set_var(ENV_PLUGINS_HOME, default);
                } else {
                    let default = "/usr/local/taos/xplugins";
                    let path = std::path::Path::new(default);
                    if path.exists() {
                        std::env::set_var(ENV_PLUGINS_HOME, default);
                    }
                }
            }
        }
    }

    set_env_log_keep_days(args.globals.log_keep_days);

    let mut log_path = get_log_dir("server");

    log_path.push("server.log");

    let log_keep_days = get_log_keep_days();

    println!("log keep days: {}", &log_keep_days);

    let log_rotation = FileRotate::new(
        &log_path,
        AppendTimestamp::with_format(
            "%Y-%m-%d",
            FileLimit::Age(chrono::Duration::days(log_keep_days)),
            DateFrom::DateYesterday,
        ),
        ContentLimit::Time(TimeFrequency::Daily),
        Compression::None,
        #[cfg(unix)]
        None,
    );

    let (non_blocking, _guard) = tracing_appender::non_blocking(log_rotation);

    // let timer = LocalTime::new(format_description!(
    //     "[month]/[day] [hour]:[minute]:[second].[subsecond digits:6]"
    // ));

    let chrono_local = Local::now();
    let timezone_offset = (chrono_local.offset().local_minus_utc()
        / chrono::Duration::hours(1).num_seconds() as i32) as i8;

    println!("local timezone offset: {}", timezone_offset);

    let timer = OffsetTime::new(
        UtcOffset::from_hms(timezone_offset, 0, 0).unwrap(),
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]"),
    );

    let level_filter = args.globals.verbose.log_level_filter();

    use tracing_subscriber::filter::LevelFilter;
    let level_filter = match level_filter {
        clap_verbosity_flag::LevelFilter::Off => LevelFilter::OFF,
        clap_verbosity_flag::LevelFilter::Error => LevelFilter::ERROR,
        clap_verbosity_flag::LevelFilter::Warn => LevelFilter::WARN,
        clap_verbosity_flag::LevelFilter::Info => LevelFilter::INFO,
        clap_verbosity_flag::LevelFilter::Debug => LevelFilter::DEBUG,
        clap_verbosity_flag::LevelFilter::Trace => LevelFilter::TRACE,
    };
    let span_events = args.globals.tracing_events.clone();
    let worker_threads = args.globals.executor_worker_threads();
    let runtime = build_runtime(worker_threads)?;

    runtime.block_on(async move {
        let mut layers = Vec::new();

        layers.push(
            tracing_subscriber::fmt::layer()
                .with_timer(timer.clone())
                .with_level(true)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_span_events(span_events.clone())
                .with_ansi(false)
                .with_writer(non_blocking)
                .with_file(true)
                .with_line_number(true)
                // .compact()
                .with_filter(level_filter)
                .boxed(),
        );
        let filter = EnvFilter::builder()
            .with_default_directive(level_filter.into())
            .with_regex(true)
            .from_env_lossy()
            .add_directive("tungstenite=warn".parse()?)
            .add_directive("tokio=warn".parse()?)
            .add_directive("runtime=warn".parse()?)
            // .add_directive("actix_server=info".parse()?)
            .add_directive("actix_http=info".parse()?)
            .add_directive("tokio_tungstenite=info".parse()?)
            .add_directive("mio=warn".parse()?);

        if atty::is(atty::Stream::Stderr) {
            layers.push(
                tracing_subscriber::fmt::layer()
                    .with_timer(timer.clone())
                    .with_level(true)
                    .with_thread_ids(true)
                    .with_writer(std::io::stderr)
                    .with_span_events(span_events)
                    .with_ansi(true)
                    .pretty()
                    .with_filter(filter)
                    .boxed(),
            );
        } else {
            layers.push(
                tracing_subscriber::fmt::layer()
                    .with_timer(timer.clone())
                    .with_level(true)
                    .with_thread_ids(true)
                    .with_writer(std::io::stderr)
                    .with_span_events(span_events)
                    .with_ansi(false)
                    .compact()
                    .with_filter(filter)
                    .boxed(),
            );
        }

        let metrics_layer = MetricsLayer::new();
        let console_layer = console_subscriber::spawn();
        if args.globals.otel.unwrap_or(false) {
            let tracer = opentelemetry_jaeger::new_agent_pipeline()
                .with_service_name("x")
                // .install_simple()?;
                .with_auto_split_batch(true)
                .install_batch(opentelemetry::runtime::Tokio)?;

            // Create a tracing layer with the configured tracer
            let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(console_layer)
                .with(metrics_layer)
                .with(layers)
                .with(telemetry)
                .init();
        } else {
            tracing_subscriber::registry()
                .with(console_layer)
                .with(metrics_layer)
                .with(layers)
                .init();
        }

        tracing::info!("version: {version}");
        tracing::info!("commit id: {commit_id}");
        tracing::info!("build time: {build_time}");
        anyhow::Ok(())
    })?;

    match args
        .commands
        .unwrap_or(Commands::Serve(serve::Cli::default()))
    {
        Commands::Run(cli) => {
            let _ = tracing::info_span!("cli").entered();
            runtime.block_on(cli.run_with(args.globals))?;
        }
        Commands::Serve(serve) => {
            let _ = tracing::info_span!("serve").entered();
            let grpc_rt = build_runtime(worker_threads)?;
            // let api_rt = build_runtime(worker_threads)?;
            let ctl = runtime.block_on(serve.controller())?;
            let api_ctl = ctl.clone();
            let serve_api = serve.clone();
            let grpc_handle = grpc_rt.spawn(serve_api.grpc(ctl.clone()));
            runtime.block_on(async move {
                // rest api
                serve.api(api_ctl, grpc_handle).await
            })?;
        }
        Commands::External(_) => bail!("unknown subcommand"),
    }
    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}
