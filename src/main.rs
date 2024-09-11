use std::backtrace::Backtrace;
use std::path::{Path, PathBuf};

use serve::monitor::MonitorCfg;
use taosx_core::runners::{
    ENV_LOGS_HOME, ENV_PLUGINS_HOME, ENV_TAOSX_DATA_DIR, ENV_TAOSX_LOGS_HOME,
    ENV_TAOSX_PLUGINS_HOME,
};
use tracing::debug;
use tracing_appender::rolling::{RollingFileAppender, Rotation};

use anyhow::Result;
use chrono::Local;
use clap::{parser::ValueSource, CommandFactory, Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use const_format::concatcp;
use opentelemetry::trace::Tracer;
use serde::{Deserialize, Serialize};
use shadow_rs::shadow;
use thiserror::Error;
use time::{macros::format_description, UtcOffset};
use tracing::{log::LevelFilter, Instrument};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{
    fmt::{format::FmtSpan, time::OffsetTime},
    prelude::*,
    EnvFilter,
};
use twelf::{config, Layer};

use crate::serve::monitor;
use taosx_core::utils::timeout::Timeout;
use taosx_core::{
    get_data_dir,
    runners::{get_logs_home_dir, get_plugins_home_dir},
    utils::trace::TaosXLayer,
};
use taosx_core::{
    get_log_dir, get_log_keep_days, set_env_data_dir, set_env_log_home_dir, set_env_log_keep_days,
    set_env_plugins_home_dir,
};

#[cfg(all(feature = "mimalloc", feature = "jemallocator"))]
compile_error!("Only one allocator can be specified");

#[cfg(feature = "tikv-jemallocator")]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

shadow!(build);

const CLAP_SHORT_VERSION: &str = if build::GIT_CLEAN {
    concatcp!(
        "version: ",
        build::TD_VERSION,
        " (core-",
        build::PKG_VERSION,
        if build::IS_DEBUG { " debug" } else { "" },
        ")\ngit: ",
        build::COMMIT_HASH,
        "\nbuild: ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
} else {
    concatcp!(
        "version: ",
        build::TD_VERSION,
        " (core-dirty-",
        build::PKG_VERSION,
        if build::IS_DEBUG { " debug" } else { "" },
        ")\ngit: ",
        build::COMMIT_HASH,
        "\nbuild: ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
};

mod privileges;
mod replica;
mod run;
mod serve;

#[derive(Subcommand, Debug)]
enum Commands {
    Run(run::Cli),
    Privileges(privileges::Cli),
    Serve(serve::Cli),
    #[clap(hide = true)]
    Replica(replica::Cli),
}

#[derive(Parser, Debug)]
struct OptArgs {
    #[clap(short = 'c', long, global = true)]
    config: Option<PathBuf>,

    /// For verbosity print.
    #[clap(flatten)]
    verbose: Verbosity<InfoLevel>,

    /// Be careful to use this, we suggest only use it when failed at first time.
    ///
    /// We'll warn you various kind of risks before really running a task.
    #[clap(short, long, global = true)]
    yes_i_really_mean_it: bool,

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
struct ConfigurableOpts {
    #[clap(flatten)]
    #[serde(flatten)]
    global: Global,

    #[clap(flatten)]
    serve: Option<serve::Cli>,

    #[clap(flatten)]
    monitor: Option<MonitorCfg>,
}

#[derive(Parser, Debug, Deserialize, Serialize, Default)]
#[serde(default)]
struct Global {
    #[clap(long, env = "PLUGINS_HOME", global = true)]
    plugins_home: Option<String>,

    #[clap(long, env = "TAOSX_DATA_DIR", global = true)]
    data_dir: Option<String>,

    #[clap(long, env = "LOGS_HOME", global = true)]
    logs_home: Option<String>,

    /// For environment variable wised log level.
    #[clap(long, hide = true, env = "LOG_LEVEL", global = true)]
    log_level: Option<LevelFilter>,

    /// Enable debug will set the mod path as `file:line`.
    #[clap(short, long, global = true)]
    debug: bool,

    /// Log keep days.
    #[clap(long, env = "LOG_KEEP_DAYS", global = true)]
    log_keep_days: Option<i64>,

    /// Not log to files.
    #[clap(long, global = true)]
    no_log_to_files: bool,

    /// Disable non-blocking writer for log file appender.
    #[clap(long, global = true)]
    no_async_log: bool,

    /// Number of jobs, default to 0, will use `jobs` number of works for TMQ.
    #[clap(short, long, value_parser, default_value = "0", global = true)]
    jobs: usize,

    /// Enable OpenTelemetry tracing and metrics exporter.
    #[clap(long, action = clap::ArgAction::SetTrue, env = "ENABLE_OTEL", global = true)]
    otel: Option<bool>,

    /// Max activities per entity.
    max_activities_per_entity: Option<usize>,

    #[clap(long, action = clap::ArgAction::SetTrue, env = "DRY_RUN", global = true, hide = true)]
    dry_run: Option<bool>,
}

#[derive(Parser, Debug)]
#[clap(name = build::CUS_CLI_NAME, author, version = CLAP_SHORT_VERSION, about = build::CUS_CLI_ABOUT, long_about = build::CUS_CLI_ABOUT)]
struct Args {
    #[clap(subcommand)]
    commands: Option<Commands>,
    #[clap(flatten)]
    opt_args: OptArgs,
    #[clap(flatten)]
    global: Global,
    #[clap(flatten)]
    monitor: MonitorCfg,
}

#[derive(Debug, Error)]
pub enum ArgsError {
    #[error("Config file is set but seems not exist: {0}")]
    ConfigNotFound(String),
    #[error("Missing required argument: {0}")]
    MissingRequiredArgument(String),
    #[error("Argument parsing error: {0}")]
    ParseError(#[from] twelf::Error),
    // #[error("Argument parsing error: {0}")]
    // ClapError(#[from] clap::Error),
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

#[cfg(windows)]
fn get_default_config_path() -> PathBuf {
    std::path::Path::new("C:\\")
        .join("TDengine")
        .join("cfg")
        .join("taosx.toml")
}

#[cfg(not(windows))]
fn get_default_config_path() -> PathBuf {
    std::path::Path::new("/etc")
        .join(build::CUS_PROMPT)
        .join(format!("{}x.toml", build::CUS_PROMPT))
}

#[inline]
fn get_effective_config_path(args: &Args) -> PathBuf {
    args.opt_args
        .config
        .clone()
        .unwrap_or_else(|| get_default_config_path())
}

impl Args {
    pub fn init() -> Result<Args, ArgsError> {
        let mut args = Args::parse();
        let path = get_effective_config_path(&args);
        // let path = if let Ok(c) = Args::try_parse() {
        //     c.opt_args
        //         .config
        //         .map(|p| {
        //             if p.exists() {
        //                 Ok(p)
        //             } else {
        //                 Err(ArgsError::ConfigNotFound(p.display().to_string()))
        //             }
        //         })
        //         .transpose()?
        // } else {
        //     None
        // }
        // .unwrap_or_else(|| {
        //     if cfg!(windows) {
        //         std::path::Path::new("C:\\")
        //             .join("TDengine")
        //             .join("cfg")
        //             .join("taosx.toml")
        //     } else {
        //         std::path::Path::new("/etc")
        //             .join(build::CUS_PROMPT)
        //             .join("taosx.toml")
        //     }
        // });
        let mut layers = vec![];
        if path.exists() {
            layers.push(Layer::Toml(path))
        }
        layers.push(Layer::Env(Some(format!(
            "{}X_",
            build::CUS_PROMPT.to_uppercase()
        ))));
        layers.push(Layer::Clap(Args::command().get_matches()));

        let configurable_opts = ConfigurableOpts::with_layers(&layers)?;
        args.global.merge_from(configurable_opts.global);
        if let Some(monitor_cfg) = configurable_opts.monitor.as_ref() {
            args.monitor.merge_from(monitor_cfg);
        }
        args.global.jobs = executor_worker_threads(args.global.jobs);
        let matches = Args::command().get_matches();

        match &mut args.commands {
            Some(Commands::Serve(cli)) => {
                let mut serve = configurable_opts.serve.unwrap_or_default();

                if let Some(matches) = matches.subcommand_matches("serve") {
                    macro_rules! take_or_not {
                        ($f:ident) => {
                            match matches.value_source(stringify!($f)) {
                                Some(ValueSource::DefaultValue) | None => {}
                                _ => {
                                    serve.$f.take();
                                }
                            }
                        };
                    }
                    take_or_not!(listen);
                    take_or_not!(grpc);
                    take_or_not!(database_url);
                    take_or_not!(secret_prefix);
                    take_or_not!(request_timeout);
                    take_or_not!(do_not_resume);
                }
                cli.merge_from(serve);
            }
            _ => {}
        }
        Ok(args)
    }
}

impl Global {
    pub fn merge_from(&mut self, rhs: Self) -> &mut Self {
        let matches = Args::command().get_matches();
        macro_rules! update_if_none {
            ($f:ident) => {
                match matches.value_source(stringify!($f)) {
                    Some(ValueSource::DefaultValue) | None => {
                        self.$f = rhs.$f;
                    }
                    _ => {}
                }
            };
        }
        update_if_none!(plugins_home);
        update_if_none!(data_dir);
        update_if_none!(logs_home);
        update_if_none!(log_level);
        update_if_none!(debug);
        update_if_none!(log_keep_days);
        update_if_none!(jobs);
        update_if_none!(otel);
        update_if_none!(dry_run);
        self
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

fn build_runtime(
    thread_name: &str,
    worker_threads: usize,
) -> std::result::Result<tokio::runtime::Runtime, std::io::Error> {
    tokio::runtime::Builder::new_multi_thread()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            format!("{}x rng seed", build::CUS_PROMPT).as_bytes(),
        ))
        .global_queue_interval(61)
        .max_blocking_threads(4096)
        .disable_lifo_slot()
        .thread_name(thread_name)
        .worker_threads(worker_threads)
        .enable_all()
        .build()
}

fn create_rolling_file_appender(log_dir: &Path) -> RollingFileAppender {
    let max_files = get_log_keep_days() + 1;
    RollingFileAppender::builder()
        .max_log_files(max_files as usize)
        .filename_prefix(format!("{}x.log", build::CUS_PROMPT))
        .rotation(Rotation::DAILY)
        .build(log_dir)
        .expect("failed to initialize rolling file appender")
}

fn init_tracing_layers<W>(
    args: &Args,
    span_events: FmtSpan,
    level_filter: LevelFilter,
    make_writer: W,
) -> Result<(), anyhow::Error>
where
    W: for<'writer> MakeWriter<'writer> + 'static + Send + Sync,
{
    let mut layers = Vec::new();
    use tracing_subscriber::filter::LevelFilter as TracingLevelFilter;
    let tracing_level_filter = match level_filter {
        log::LevelFilter::Off => TracingLevelFilter::OFF,
        log::LevelFilter::Error => TracingLevelFilter::ERROR,
        log::LevelFilter::Warn => TracingLevelFilter::WARN,
        log::LevelFilter::Info => TracingLevelFilter::INFO,
        log::LevelFilter::Debug => TracingLevelFilter::DEBUG,
        log::LevelFilter::Trace => TracingLevelFilter::TRACE,
    };
    fn env_filter_from(tracing_level_filter: &TracingLevelFilter) -> anyhow::Result<EnvFilter> {
        let event_filter = EnvFilter::builder()
            .with_default_directive(tracing_level_filter.clone().into())
            .with_regex(true)
            .from_env_lossy();
        let event_filter = if *tracing_level_filter > TracingLevelFilter::INFO {
            event_filter
                .add_directive("tungstenite=warn".parse()?)
                .add_directive("tokio=warn".parse()?)
                .add_directive("runtime=warn".parse()?)
                .add_directive("actix_server=info".parse()?)
                .add_directive("actix_http=info".parse()?)
                .add_directive("tokio_tungstenite=warn".parse()?)
                .add_directive("mio=warn".parse()?)
                .add_directive("h2=warn".parse()?)
                .add_directive("hyper=warn".parse()?)
                .add_directive("reqwest=warn".parse()?)
                .add_directive("sled=warn".parse()?)
        } else {
            event_filter
        };
        Ok(event_filter)
    }
    if !args.global.no_log_to_files {
        // Add layer for rotating logs
        layers.push(
            TaosXLayer::new()
                .with_writer(make_writer)
                .with_filter(env_filter_from(&tracing_level_filter)?)
                .boxed(),
        );
    }

    let chrono_local = Local::now();
    let timezone_offset = (chrono_local.offset().local_minus_utc()
        / chrono::Duration::hours(1).num_seconds() as i32) as i8;
    let timer = OffsetTime::new(
        UtcOffset::from_hms(timezone_offset, 0, 0).unwrap(),
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]"),
    );

    // Add layer for printing log to terminal.
    if atty::is(atty::Stream::Stderr) {
        layers.push(
            tracing_subscriber::fmt::layer()
                .with_timer(timer.clone())
                .with_thread_names(true)
                .with_writer(std::io::stderr)
                .with_span_events(span_events)
                .with_ansi(true)
                .pretty()
                .with_filter(env_filter_from(&tracing_level_filter)?)
                .boxed(),
        );
    }

    // Enable console subscriber
    #[cfg(feature = "tokio-tracing")]
    {
        layers.push(console_subscriber::spawn().boxed());
    }
    // Create event subscriber
    let layered = tracing_subscriber::registry().with(layers);

    // Enable opentelemetry layer
    if otel_enabled(args) {
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(opentelemetry_otlp::new_exporter().tonic())
            .with_trace_config(
                opentelemetry_sdk::trace::config()
                    .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
                    .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default())
                    .with_max_events_per_span(64)
                    .with_max_attributes_per_span(16)
                    .with_max_events_per_span(16)
                    .with_resource(opentelemetry_sdk::Resource::new(vec![
                        opentelemetry::KeyValue::new("service.name", build::CUS_CLI_NAME),
                    ])),
            )
            .install_simple()?;
        tracer.in_span("init", |_cx| _cx.attach());
        // Create a tracing layer with the configured tracer
        let telemetry = tracing_opentelemetry::layer::<_>()
            .with_tracer(tracer)
            .with_filter(
                EnvFilter::builder()
                    .with_default_directive(tracing_level_filter.into())
                    .with_regex(true)
                    .from_env_lossy()
                    .add_directive("tungstenite=warn".parse()?)
                    .add_directive("tokio=warn".parse()?)
                    .add_directive("runtime=warn".parse()?)
                    .add_directive("actix_server=info".parse()?)
                    .add_directive("actix_http=info".parse()?)
                    .add_directive("tokio_tungstenite=info".parse()?)
                    .add_directive("mio=info".parse()?)
                    .add_directive("h2=warn".parse()?),
            );
        layered.with(telemetry).try_init()?;
    } else {
        layered.try_init()?;
    }
    Ok(())
}

fn level_upgrade(level: LevelFilter, num: i8) -> LevelFilter {
    if num == 0 {
        return level;
    }
    if num < 0 {
        let level = match level {
            LevelFilter::Off => return LevelFilter::Off,
            LevelFilter::Error => LevelFilter::Off,
            LevelFilter::Warn => LevelFilter::Error,
            LevelFilter::Info => LevelFilter::Warn,
            LevelFilter::Debug => LevelFilter::Info,
            LevelFilter::Trace => LevelFilter::Debug,
        };
        return level_upgrade(level, num + 1);
    }
    let level = match level {
        LevelFilter::Off => LevelFilter::Error,
        LevelFilter::Error => LevelFilter::Warn,
        LevelFilter::Warn => LevelFilter::Info,
        LevelFilter::Info => LevelFilter::Debug,
        LevelFilter::Debug => LevelFilter::Trace,
        LevelFilter::Trace => LevelFilter::Trace,
    };
    return level_upgrade(level, num - 1);
}

fn get_log_path() -> PathBuf {
    let mut log_path = get_log_dir("");
    log_path.push(format!("{}x.log", build::CUS_PROMPT));
    log_path
}

fn get_env_log_dir() -> String {
    if let Some(dir) = std::env::var(ENV_LOGS_HOME).ok() {
        return dir;
    }
    if let Some(dir) = std::env::var(ENV_TAOSX_LOGS_HOME).ok() {
        return dir;
    }

    if cfg!(windows) {
        "C:\\TDengine\\log".to_string()
    } else {
        format!("/var/log/{}", build::CUS_PROMPT)
    }
}

fn get_env_data_dir() -> String {
    if let Some(dir) = std::env::var(ENV_TAOSX_DATA_DIR).ok() {
        return dir;
    }

    if cfg!(windows) {
        "C:\\TDengine\\data\\taosx".to_string()
    } else {
        format!("/var/lib/{0}/{0}x", build::CUS_PROMPT)
    }
}

fn get_env_plugin_dir() -> String {
    if let Some(dir) = std::env::var(ENV_PLUGINS_HOME).ok() {
        return dir;
    }
    if let Some(dir) = std::env::var(ENV_TAOSX_PLUGINS_HOME).ok() {
        return dir;
    }

    if cfg!(windows) {
        "C:\\TDengine\\plugins".to_string()
    } else {
        format!("/usr/local/{}/plugins", build::CUS_PROMPT)
    }
}

#[inline]
fn otel_enabled(args: &Args) -> bool {
    args.global.otel.unwrap_or(false)
}

/// Gether all effective environment variables and options, and join them with \n .
/// This method can only be called after all env variables and options were determined.
#[rustfmt::skip]
fn print_effective_config(level_filter: &LevelFilter, args: &Args) {
    let w = 18;
    let w2 = 22;
    let mut s = String::new();
    s += "       global config\n";
    s += "===================================================================================\n";
    s += format!("{:<w$}{:<w2$}{}\n", ' ', "config file", get_effective_config_path(args).display()).as_str();
    s += format!("{:<w$}{:<w2$}{}\n", ' ', "plugins_home", get_plugins_home_dir().display()).as_str();
    s += format!("{:<w$}{:<w2$}{}\n",' ',"data_dir", get_data_dir().display()).as_str();
    s += format!("{:<w$}{:<w2$}{}\n", ' ', "logs_home",get_logs_home_dir().display()).as_str();
    s += format!("{:<w$}{:<w2$}{}\n", ' ', "log_path", get_log_path().display()).as_str();
    s += format!("{:<w$}{:<w2$}{}\n", ' ', "log_level", level_filter).as_str();
    s += format!("{:<w$}{:<w2$}{}\n", ' ', "log_keep_days", get_log_keep_days()).as_str();
    s += format!("{:<w$}{:<w2$}{}\n", ' ', "jobs", args.global.jobs).as_str();
    if let Commands::Serve(cli) = args.commands.as_ref().unwrap_or(&Commands::Serve(Default::default())) {
        s += format!("{:<w$}{:<w2$}{}\n", ' ', "server.listen", cli.get_listen_address()).as_str();
        s += format!("{:<w$}{:<w2$}{}\n", ' ', "server.grpc", cli.get_grpc_address()).as_str();
        s += format!("{:<w$}{:<w2$}{}\n", ' ', "server.database_url", cli.get_database_url()).as_str();
        s += format!("{:<w$}{:<w2$}{}\n", ' ', "monitor.fqdn", args.monitor.fqdn.as_ref().unwrap_or(&"".to_string())).as_str();
        s += format!("{:<w$}{:<w2$}{}\n", ' ', "monitor.port", args.monitor.port).as_str();
        s += format!("{:<w$}{:<w2$}{}\n", ' ', "monitor.interval", args.monitor.interval).as_str();
    }
    s += "===================================================================================";
    tracing::info!("{}", s);
}

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let version = build::PKG_VERSION;
    let commit_id = build::COMMIT_HASH;
    let build_time = build::BUILD_TIME;
    let args = Args::init()?;
    set_env_data_dir(
        args.global
            .data_dir
            .clone()
            .unwrap_or_else(|| get_env_data_dir()),
    );
    set_env_log_home_dir(
        args.global
            .logs_home
            .clone()
            .unwrap_or_else(|| get_env_log_dir()),
    );
    set_env_plugins_home_dir(
        args.global
            .plugins_home
            .clone()
            .unwrap_or_else(|| get_env_plugin_dir()),
    );
    set_env_log_keep_days(args.global.log_keep_days.clone());

    // Set a panic hook
    std::panic::set_hook(Box::new(|info| {
        // 正常打印 backtrace, 需要设置环境变量: RUST_BACKTRACE=1
        let backtrace = Backtrace::capture();
        tracing::error!("panic occurred. {} {}", info, backtrace);
    }));
    // Initialize tracing layers
    let mut level_filter = if matches!(args.commands, Some(Commands::Replica(_))) {
        LevelFilter::Warn
    } else {
        args.global.log_level.clone().unwrap_or(LevelFilter::Info)
    };
    let matches = Args::command().get_matches();
    let level_num = matches.get_count("verbose") as i8 - matches.get_count("quiet") as i8;
    level_filter = level_upgrade(level_filter, level_num);

    let span_events = args.opt_args.tracing_events.clone();
    let worker_threads = args.global.jobs.clone();
    let runtime = build_runtime(&format!("{}x", build::CUS_PROMPT), worker_threads)?;
    let log_dir = get_log_dir("");
    let rolling_file_appender = create_rolling_file_appender(&log_dir);

    let _guard = if !args.global.no_async_log {
        let (non_blocking, guard) = tracing_appender::non_blocking(rolling_file_appender);
        init_tracing_layers(&args, span_events, level_filter, non_blocking)?;
        Some(guard)
    } else {
        init_tracing_layers(&args, span_events, level_filter, rolling_file_appender)?;
        None
    };
    tracing::info!("{}x version: {version}", build::CUS_PROMPT);
    tracing::info!("commit id: {commit_id}");
    tracing::info!("build time: {build_time}");
    tracing::info!(
        "connector version: {}-{}",
        taos::build::PKG_VERSION,
        taos::build::SHORT_COMMIT
    );

    if args.global.dry_run.unwrap_or(false) {
        tracing::info!("dry run mode enabled");
        unsafe { taosx_core::global::DRY_RUN = true };
    }

    print_effective_config(&level_filter, &args);
    let res = match args.commands.unwrap_or(Commands::Serve(Default::default())) {
        Commands::Run(cli) => {
            let span = tracing::info_span!("main");
            let _ = span.enter();
            runtime.block_on(cli.run_with(args.opt_args, args.global).instrument(span))
        }
        Commands::Privileges(privileges) => runtime.block_on(privileges.run(args.opt_args)),
        Commands::Replica(replica) => runtime.block_on(replica.run(args.opt_args)),
        Commands::Serve(serve) => {
            Timeout::set_default_timeout(serve.request_timeout);

            let serve = || {
                let _ = tracing::info_span!("serve").entered();
                let addr = serve.get_listen_address();
                let port = addr.split(':').last().unwrap();
                let scheduler_rt = build_runtime(
                    &format!("{}x-server", build::CUS_PROMPT),
                    worker_threads * 2,
                )?;
                let (
                    agent_integration_channel,
                    agent_rpc_channel,
                    agent_spawn_sender,
                    scheduler_notifier,
                ) = scheduler_rt.block_on(serve.channels());

                debug!("Starting scheduler");
                let scheduler = scheduler_rt
                    .block_on(serve.scheduler(scheduler_notifier, agent_integration_channel))?;

                let grpc_rt = build_runtime("grpc-server", worker_threads)?;

                // let api_rt = build_runtime(worker_threads)?;
                let max_activities_per_entity =
                    args.global.max_activities_per_entity.unwrap_or(100);

                debug!("Starting controller");
                let ctl =
                    runtime.block_on(serve.controller(scheduler, max_activities_per_entity))?;

                debug!("Starting monitor");
                let monitor = monitor::Monitor::new(args.monitor.clone(), port, ctl.clone());
                let api_ctl = ctl.clone();
                let serve_api = serve.clone();
                debug!("Starting gRPC server");
                let grpc_handle = grpc_rt.spawn(serve_api.grpc(
                    ctl.clone(),
                    agent_rpc_channel,
                    agent_spawn_sender,
                    monitor.clone(),
                ));
                debug!("Starting API server");
                runtime.block_on(async move {
                    // rest api
                    serve.api(api_ctl, grpc_handle, monitor).await
                })?;
                Ok(())
            };
            serve()
        }
    };
    runtime.block_on(async move {
        opentelemetry::global::shutdown_tracer_provider();
    });
    tracing::trace!("Shutdown main runtime");
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    /// set plugins_home、data_dir、logs_home in server.toml
    /// set data_dir、logs_home in env
    /// set logs_home in cli
    #[test]
    #[ignore]
    fn test_config_from_toml() -> Result<(), anyhow::Error> {
        env::set_var("TAOSX_DATA_DIR", "from-env");
        env::set_var("TAOSX_LOGS_HOME", "from-env");

        let args = Args::parse();
        println!("configs: {:?}", args);

        assert_eq!(
            args.global.plugins_home.unwrap_or("".to_string()),
            "from-config".to_string()
        );
        assert_eq!(
            args.global.data_dir.unwrap_or("".to_string()),
            "from-env".to_string()
        );
        // assert_eq!(args.config_args.logs_home.unwrap_or("".to_string()), "from-cli".to_string());
        Ok(())
    }
}
