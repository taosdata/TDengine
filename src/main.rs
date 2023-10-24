use anyhow::{bail, Result};
use chrono::Local;
use clap::{CommandFactory, Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use const_format::concatcp;
use file_rotate::{
    compression::Compression,
    suffix::{AppendTimestamp, DateFrom, FileLimit},
    ContentLimit, FileRotate, TimeFrequency,
};
use metrics_tracing_context::MetricsLayer;
use opentelemetry::trace::Tracer;
use shadow_rs::shadow;

use taosx_core::{
    Action,
    ENV_PLUGINS_HOME, ENV_TAOSX_DATA_DIR, ENV_TAOSX_LOGS_HOME, ENV_TAOSX_LOGS_KEEP_DAYS,
    get_log_dir, get_log_keep_days, valid_env_log_keep_days};
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
use tracing::{Instrument, Level, log::LevelFilter};
use tracing_subscriber::{
    fmt::{format::FmtSpan, time::OffsetTime},
    prelude::*,
    EnvFilter,
};
use std::{path::PathBuf};
use taos::Dsn;
use thiserror::Error;
use twelf::{config, Layer};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

shadow!(build);

const CLAP_SHORT_VERSION: &str = if build::GIT_CLEAN {
    concatcp!(
        "version: ",
        build::TD_VERSION,
        "\ngit: ",
        build::BRANCH,
        "-",
        build::COMMIT_HASH,
        "\nbuild: core-",
        build::PKG_VERSION,
        " ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
} else {
    concatcp!(
        "version: ",
        build::TD_VERSION,
        "\ngit: ",
        build::BRANCH,
        "-",
        build::COMMIT_HASH,
        "\nbuild: core-dirty-",
        build::PKG_VERSION,
        " ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
};

mod run;
mod serve;

#[derive(Subcommand, Debug)]
enum Commands {
    Run(run::Cli),
    Serve(serve::Cli),
    #[clap(external_subcommand)]
    External(Vec<String>),
}

#[derive(Parser, Debug)]
struct OptArgs {
    #[clap(short = 'c', long, global = true)]
    config: Option<PathBuf>,

    /// For verbosity print.
    #[clap(flatten)]
    verbose: Option<Verbosity<InfoLevel>>,

    /// Be careful to use this, we suggest only use it when failed at first time.
    ///
    /// We'll warn you various kind of risks before really running a task.
    #[clap(short, long, global = true)]
    yes_i_really_mean_it: bool,

    #[clap(long)]
    do_not_resume: bool,

    #[clap(short = 'L', long)]
    log_dir: Option<PathBuf>,

    #[clap(
        long,
        global = true,
        default_value = "none",
        value_parser = fmt_span_from_str,
        env = "TRACING_EVENTS"
    )]
    tracing_events: FmtSpan,
}

#[config]
#[derive(Parser, Debug)]
struct ConfigArgs {
    #[clap(long, env = "PLUGINS_HOME")]
    plugins_home: Option<String>,

    #[clap(long, env = "TAOSX_DATA_DIR")]
    data_dir: Option<String>,

    #[clap(long, env = "TAOSX_LOGS_HOME")]
    logs_home: Option<String>,

    #[clap(long, env = "UPLOAD_FILE_HOME")]
    upload_file_home: Option<String>,

    /// For environment variable wised log level.
    #[clap(hide = true, env = "LOG_LEVEL")]
    log_level: Option<LevelFilter>,

    /// Enable debug will set the mod path as `file:line`.
    #[clap(short, long, global = true)]
    debug: bool,

    /// Log keep days.
    #[clap(long, global = true, env = "LOG_KEEP_DAYS")]
    log_keep_days: Option<i64>,

    /// Number of jobs, default to 0, will use `jobs` number of works for TMQ.
    #[clap(short, long, value_parser, default_value = "0", global = true)]
    jobs: usize,

    /// Enable OpenTelemetry tracing and metrics exporter.
    #[clap(long, global = true, action = clap::ArgAction::SetTrue, env = "ENABLE_OTEL")]
    otel: Option<bool>,
}

#[derive(Parser, Debug)]
#[clap(name = build::CUS_CLI_NAME, author, version = CLAP_SHORT_VERSION, about = build::CUS_CLI_ABOUT, long_about = build::CUS_CLI_ABOUT)]
struct Args {
    #[clap(subcommand)]
    commands: Option<Commands>,
    #[clap(flatten)]
    opt_args: OptArgs,
    #[clap(flatten)]
    config_args: ConfigArgs,
    #[clap(last = true, value_parser)]
    slop: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ArgsError {
    #[error("Config file is set but seems not exist: {0}")]
    ConfigNotFound(String),
    #[error("Missing required argument: {0}")]
    MissingRequiredArgument(String),
    #[error("Argument parsing error: {0}")]
    ParseError(#[from] twelf::Error),
}

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

impl Args {
    pub fn init() -> Result<Args, ArgsError> {
        let path = if let Ok(c) = Args::try_parse() {
            c.opt_args.config.map(|p| {
                if p.exists() {
                    Ok(p)
                } else {
                    Err(ArgsError::ConfigNotFound(p.display().to_string()))
                }
            }).transpose()?
        } else {
            None
        }.unwrap_or_else(|| {
            if cfg!(windows) {
                std::path::Path::new("C:\\")
                    .join("Program Files")
                    .join("taos")
                    .join("config")
                    .join("taosx.toml")
            } else {
                std::path::Path::new("/etc")
                    .join(build::CUS_PROMPT)
                    .join("taosx.toml")
            }
        });
        let mut layers = vec![];
        if path.exists() {
            layers.push(Layer::Toml(path))
        }
        layers.push(Layer::Env(Some(format!("{}X_", build::CUS_PROMPT.to_uppercase()))));
        layers.push(Layer::Clap(Args::command().get_matches()));

        let ConfigArgs {
            plugins_home,
            data_dir,
            logs_home,
            upload_file_home,
            log_level,
            debug,
            log_keep_days,
            jobs,
            otel,
            ..
        } = ConfigArgs::with_layers(&layers)?;

        let mut args = Args::parse();

        let jobs = executor_worker_threads(jobs);

        Ok(Args {
            commands: args.commands,
            opt_args: args.opt_args,
            config_args: ConfigArgs {
                plugins_home,
                data_dir,
                logs_home,
                upload_file_home,
                log_level,
                debug,
                log_keep_days,
                jobs,
                otel
            },
            slop: args.slop,
        })
    }
}

pub fn executor_worker_threads(jobs: usize) -> usize {
    let min = std::thread::available_parallelism()
        .map(|v| v.get() * 2)
        .unwrap_or(16)
        .max(16);
    if &jobs + 2 > min {
        jobs + 2
    } else {
        min
    }
}

fn set_env_plugins_home_dir(config: Option<String>) {
    if let Some(plugins_home_dir) = config {
        std::env::set_var(ENV_PLUGINS_HOME, plugins_home_dir);
    }
}

fn set_env_data_dir(config: Option<String>) {
    if let Some(data_dir) = config {
        std::env::set_var(ENV_TAOSX_DATA_DIR, data_dir);
    }
}

fn set_env_log_home_dir(config: Option<String>) {
    if let Some(log_home_dir) = config {
        std::env::set_var(ENV_TAOSX_LOGS_HOME, log_home_dir);
    }
}

fn set_env_log_keep_days(config: Option<i64>) {
    if let Some(log_keep_days) = config {
        if log_keep_days > 0 && valid_env_log_keep_days().is_none() {
            std::env::set_var(ENV_TAOSX_LOGS_KEEP_DAYS, log_keep_days.to_string());
        }
    }
}

fn log_level_to_tracing_level(level: LevelFilter) -> Option<Level> {
    match level {
        LevelFilter::Off => None,
        LevelFilter::Error => Some(Level::ERROR),
        LevelFilter::Warn => Some(Level::WARN),
        LevelFilter::Info => Some(Level::INFO),
        LevelFilter::Debug => Some(Level::DEBUG),
        LevelFilter::Trace => Some(Level::TRACE),
    }
}

fn build_runtime(worker_threads: usize) -> std::result::Result<tokio::runtime::Runtime, std::io::Error> {
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
    let version = build::PKG_VERSION;
    let commit_id = build::COMMIT_HASH;
    let build_time = build::BUILD_TIME;
    // println!("taosx version: {CLAP_SHORT_VERSION}");
    println!("taosx version: {version}");
    println!("commit id: {commit_id}");
    println!("build time: {build_time}");

    let args = Args::init()?;

    println!("configs: {:?}", args);

    set_env_plugins_home_dir(args.config_args.plugins_home.clone());
    set_env_data_dir(args.config_args.data_dir.clone());
    set_env_log_home_dir(args.config_args.logs_home.clone());
    set_env_log_keep_days(args.config_args.log_keep_days.clone());

    let log_level = log_level_to_tracing_level(
        args.config_args.log_level
            .clone()
            .or(args.opt_args.verbose.clone().map(|v| v.log_level_filter()))
            .unwrap_or(log::LevelFilter::Info),
    );

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
    let timezone_offset = (chrono_local.offset().local_minus_utc() / chrono::Duration::hours(1).num_seconds() as i32) as i8;

    println!("local timezone offset: {}", timezone_offset);

    let timer = OffsetTime::new(
        UtcOffset::from_hms(timezone_offset, 0, 0).unwrap(),
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]"),
    );

    let level_filter = tracing_subscriber::filter::LevelFilter::from_level(log_level.unwrap_or(Level::INFO));
    use tracing_subscriber::filter::LevelFilter;
    let span_events = args.opt_args.tracing_events.clone();
    let worker_threads = args.config_args.jobs.clone();
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
            .add_directive("actix_server=info".parse()?)
            .add_directive("actix_http=info".parse()?)
            .add_directive("tokio_tungstenite=info".parse()?)
            .add_directive("mio=info".parse()?)
            .add_directive("h2=info".parse()?);

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

        let metrics_layer = MetricsLayer::new().boxed();
        layers.push(metrics_layer);
        let layered = tracing_subscriber::registry().with(layers);
        #[cfg(feature = "tokio-tracing")]
        {
            layers.push(console_subscriber::spawn().boxed());
        }
        if args.config_args.otel.clone().unwrap_or(false) {
            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(opentelemetry_otlp::new_exporter().tonic())
                .with_trace_config(
                    opentelemetry::sdk::trace::config()
                        .with_sampler(opentelemetry::sdk::trace::Sampler::AlwaysOn)
                        .with_id_generator(opentelemetry::sdk::trace::RandomIdGenerator::default())
                        .with_max_events_per_span(64)
                        .with_max_attributes_per_span(16)
                        .with_max_events_per_span(16)
                        .with_resource(opentelemetry::sdk::Resource::new(vec![
                            opentelemetry::KeyValue::new("service.name", build::CUS_CLI_NAME),
                        ])),
                )
                .install_simple()?;
            // .install_batch(opentelemetry::runtime::Tokio)?;

            tracer.in_span("init", |_cx| _cx.attach());
            // Create a tracing layer with the configured tracer
            let telemetry = tracing_opentelemetry::layer::<_>()
                .with_tracer(tracer)
                .with_filter(
                    EnvFilter::builder()
                        .with_default_directive(level_filter.into())
                        .with_regex(true)
                        .from_env_lossy()
                        .add_directive("tungstenite=warn".parse()?)
                        .add_directive("tokio=warn".parse()?)
                        .add_directive("runtime=warn".parse()?)
                        .add_directive("actix_server=info".parse()?)
                        .add_directive("actix_http=info".parse()?)
                        .add_directive("tokio_tungstenite=info".parse()?)
                        .add_directive("mio=info".parse()?)
                        .add_directive("h2=info".parse()?),
                );
            layered.with(telemetry).try_init()?;
        } else {
            layered.try_init()?;
        }

        let span = tracing::info_span!("info", version, commit_id = build::SHORT_COMMIT);
        span.in_scope(|| {
            tracing::info!("version: {version}");
            tracing::info!("commit id: {commit_id}");
            tracing::info!("build time: {build_time}");
        });
        span.entered().exit();
        anyhow::Ok(())
    })?;

    match args
        .commands
        .unwrap_or(Commands::Serve(serve::Cli::default()))
    {
        Commands::Run(cli) => {
            // let _ = tracing::info_span!("main").entered();

            let span = tracing::info_span!("main");
            let _ = span.enter();
            runtime.block_on(cli.run_with(args.opt_args, args.config_args).instrument(span))?;
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
    runtime.block_on(async move {
        opentelemetry::global::shutdown_tracer_provider();
    });
    println!("wait for runtime shutdown");
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser, Subcommand};
    use std::env;
    use std::error::Error;

    /// set plugins_home、data_dir、logs_home in server.toml
    /// set data_dir、logs_home in env
    /// set logs_home in cli
    #[test]
    fn test_config_from_toml() -> Result<(), anyhow::Error> {
        env::set_var("TAOSX_DATA_DIR", "from-env");
        env::set_var("TAOSX_LOGS_HOME", "from-env");

        let args = tests::parse()?;
        println!("configs: {:?}", args);

        assert_eq!(args.config_args.plugins_home.unwrap_or("".to_string()), "from-config".to_string());
        assert_eq!(args.config_args.data_dir.unwrap_or("".to_string()), "from-env".to_string());
        // assert_eq!(args.config_args.logs_home.unwrap_or("".to_string()), "from-cli".to_string());
        Ok(())
    }
}
