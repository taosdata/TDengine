use agent::listen_task_metrics;
use anyhow::bail;
use chrono::Utc;
use clap::{CommandFactory, Parser};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use const_format::concatcp;
use flume::{Receiver, Sender};
use metrics::gauge;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use taoslog::{layer::TaosLayer, writer::RollingFileAppender};
use taosx_metrics::{MetricEvent, MetricsEvents};
use thiserror::Error;
use tokio::task::JoinHandle;

use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, Layer as _,
};
use twelf::{config, Layer};

use taosx_core::{
    get_data_dir,
    runners::{
        get_plugins_home_dir, ENV_LOGS_HOME, ENV_PLUGINS_HOME, ENV_TAOSX_DATA_DIR,
        ENV_TAOSX_LOGS_HOME, ENV_TAOSX_PLUGINS_HOME,
    },
    utils::{
        monitor::update_sub_connector_process_metrics,
        trace::{Qid, INSTANCE_ID},
    },
};
use taosx_core::{
    get_log_dir, get_log_keep_days, set_env_data_dir, set_env_log_home_dir, set_env_log_keep_days,
    set_env_plugins_home_dir, Activity, RespAction, AGENT_COMPRESSION,
};
use tracing::{log::LevelFilter, Instrument};

const LOG_FILE: &str = "agent.log";

shadow_rs::shadow!(build);

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

fn log_level_to_tracing_level(level: LevelFilter) -> Option<tracing::level_filters::LevelFilter> {
    use tracing::level_filters::LevelFilter as Level;
    match level {
        LevelFilter::Off => None,
        LevelFilter::Error => Some(Level::ERROR),
        LevelFilter::Warn => Some(Level::WARN),
        LevelFilter::Info => Some(Level::INFO),
        LevelFilter::Debug => Some(Level::DEBUG),
        LevelFilter::Trace => Some(Level::TRACE),
    }
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

#[derive(Debug)]
pub struct Args {
    plugins_home: Option<String>,

    data_dir: Option<String>,

    log: Option<LogOpts>,

    /// Listen to ip:port address.
    endpoint: String,

    token: String,

    log_keep_days: Option<i64>,
}

#[config]
#[derive(Parser, Debug)]
#[clap(
    name = build::CUS_CLI_NAME,
    author, version = CLAP_SHORT_VERSION,
    about = build::CUS_CLI_ABOUT,
    long_about = build::CUS_CLI_ABOUT)]
pub struct ConfigArgs {
    #[clap(long, env = "PLUGINS_HOME")]
    plugins_home: Option<String>,

    #[clap(long, env = "TAOSX_DATA_DIR")]
    data_dir: Option<String>,

    #[clap(long, global = true, env = "INSTANCE_ID")]
    #[serde(rename = "instanceId")]
    instance_id: Option<u8>,

    #[clap(long, env = "LOGS_HOME")]
    logs_home: Option<String>,

    #[clap(flatten)]
    log: Option<LogOpts>,

    /// Listen to ip:port address.
    #[clap(short = 'e', long)]
    endpoint: Option<String>,

    #[clap(short = 't', long)]
    token: Option<String>,

    #[clap(long)]
    compression: Option<bool>,

    /// For environment variable wised log level.
    #[clap(hide = true, env = "LOG_LEVEL")]
    log_level: Option<LevelFilter>,

    #[clap(long, env = "LOG_KEEP_DAYS")]
    log_keep_days: Option<i64>,
}

#[derive(Parser, Debug)]
pub struct ArgsParser {
    /// Config file.
    #[clap(short = 'c', long)]
    config: Option<PathBuf>,

    #[clap(flatten)]
    config_args: ConfigArgs,

    /// For verbosity logging.
    #[clap(flatten)]
    verbose: Option<Verbosity<InfoLevel>>,
}

#[config]
#[derive(Parser, Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct LogOpts {
    #[clap(id = "log.path", long = "log.path", env = "LOG_PATH")]
    path: Option<PathBuf>,
    #[clap(id = "log.level", long = "log.level", env = "LOG_LEVEL")]
    level: Option<LevelFilter>,
    #[clap(
        id = "log.compress",
        long = "log.compress",
        env = "LOG_COMPRESS",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = compress_arg_parser,
    )]
    compress: Option<CompressType>,
    #[clap(
        id = "log.rotationCount",
        long = "log.rotationCount",
        env = "LOG_ROTATION_COUNT"
    )]
    rotation_count: Option<u16>,
    #[clap(id = "log.keepDays", long = "log.keepDays", env = "LOG_KEEP_DAYS")]
    keep_days: Option<u16>,
    #[clap(
        id = "log.rotationSize",
        long = "log.rotationSize",
        env = "LOG_ROTATION_SIZE"
    )]
    rotation_size: Option<String>,
    #[clap(
        id = "log.reservedDiskSize",
        long = "log.reservedDiskSize",
        env = "LOG_RESERVED_DISK_SIZE"
    )]
    reserved_disk_size: Option<String>,
}
fn compress_arg_parser(value: &str) -> Result<CompressType, clap::Error> {
    match value.to_lowercase().as_str() {
        "0" | "false" => Ok(CompressType::B(false)),
        _ => Ok(CompressType::B(false)),
    }
}

impl Default for LogOpts {
    fn default() -> Self {
        Self {
            path: Some(PathBuf::from(get_env_log_dir())),
            level: Some(LevelFilter::Info),
            compress: Some(CompressType::B(false)),
            rotation_count: Some(30),
            keep_days: Some(30),
            rotation_size: Some("1GB".to_string()),
            reserved_disk_size: Some("1GB".to_string()),
        }
    }
}

impl LogOpts {
    fn merge_from(&mut self, rhs: Self) {
        macro_rules! update_if_none {
            ($field: ident) => {
                if self.$field.is_none() {
                    self.$field = rhs.$field
                }
            };
        }
        update_if_none!(path);
        update_if_none!(level);
        update_if_none!(compress);
        update_if_none!(rotation_count);
        update_if_none!(keep_days);
        update_if_none!(rotation_size);
        update_if_none!(reserved_disk_size);
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
#[serde(untagged)]
enum CompressType {
    B(bool),
    N(u8),
}

impl CompressType {
    fn to_bool(self) -> anyhow::Result<bool> {
        match self {
            CompressType::B(b) => Ok(b),
            CompressType::N(1) => Ok(true),
            CompressType::N(0) => Ok(false),
            _ => bail!("invalid log compress type"),
        }
    }
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

#[inline]
fn get_effective_config_path(args: &ArgsParser) -> PathBuf {
    args.config
        .clone()
        .unwrap_or_else(|| get_default_config_path())
}

#[cfg(windows)]
fn get_default_config_path() -> PathBuf {
    std::path::Path::new("C:\\")
        .join("TDengine")
        .join("cfg")
        .join("agent.toml")
}

#[cfg(not(windows))]
fn get_default_config_path() -> PathBuf {
    std::path::Path::new("/etc")
        .join(build::CUS_PROMPT)
        .join("agent.toml")
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

impl Args {
    pub fn init() -> Result<Args, ArgsError> {
        let args = ArgsParser::parse();
        let path = get_effective_config_path(&args);
        // let path = if let Ok(c) = ArgsParser::try_parse() {
        //     c.config
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
        //             .join("agent.toml")
        //     } else {
        //         std::path::Path::new("/etc")
        //             .join(build::CUS_PROMPT)
        //             .join("agent.toml")
        //     }
        // });

        let mut layers = vec![];

        if path.exists() {
            layers.push(Layer::Toml(path))
        }
        layers.push(Layer::Env(Some(format!(
            "{}X_AGENT_",
            build::CUS_PROMPT.to_uppercase()
        ))));
        layers.push(Layer::Clap(ArgsParser::command().get_matches()));

        let ConfigArgs {
            plugins_home,
            data_dir,
            logs_home,
            endpoint,
            token,
            compression,
            log_level,
            log_keep_days,
            instance_id,
            mut log,
            ..
        } = ConfigArgs::with_layers(&layers)?;

        let mut level_filter = log
            .as_ref()
            .and_then(|opts| opts.level)
            .or(log_level)
            .unwrap_or(LevelFilter::Info);

        if let Some(_) = &args.verbose.as_ref() {
            let matches = ArgsParser::command().get_matches();
            let level_num = matches.get_count("verbose") as i8 - matches.get_count("quiet") as i8;
            level_filter = level_upgrade(level_filter, level_num);
        }

        let log_home = log
            .as_ref()
            .and_then(|opts| opts.path.clone())
            .or(logs_home.map(PathBuf::from));

        match log.as_mut() {
            Some(opts) => {
                opts.level = Some(level_filter);
                opts.path = log_home;
                opts.merge_from(LogOpts::default());
            }
            None => {
                let mut opts = LogOpts::default();
                opts.level = Some(level_filter);
                opts.path = log_home;
                log = Some(opts);
            }
        }

        INSTANCE_ID.get_or_init(|| instance_id.unwrap_or(64));

        AGENT_COMPRESSION.set(compression.unwrap_or(false)).unwrap();

        Ok(Args {
            plugins_home,
            data_dir,
            endpoint: endpoint
                .ok_or_else(|| ArgsError::MissingRequiredArgument("endpoint".to_string()))?,
            token: token.ok_or_else(|| ArgsError::MissingRequiredArgument("token".to_string()))?,
            log_keep_days,
            log,
        })
    }
}

mod agent;
mod runner;

async fn main_agent_service(args: Args) -> anyhow::Result<()> {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut client = agent::Client::new(&args.endpoint, &args.token).await?;
    let mut client2 = agent::Client::new(&args.endpoint, &args.token).await?;
    let mut client3 = agent::Client::new(&args.endpoint, &args.token).await?;

    let agent = client.agent();
    let agent_id = agent.id;
    let (resp_tx, resp_rx) = flume::unbounded::<RespAction>();

    let (runner, tasks, sender, status) =
        runner::spawn_runner(agent_id, &args.endpoint, &args.token, resp_tx.clone());

    let (metrics_tx, metrics_rx) = flume::bounded(1000);
    taosx_metrics::ChannelRecorder::new(Arc::new(metrics_tx)).install();

    let monitor_config = client.get_taosx_monitor_config().await;
    let monitor_enabled: bool = get_monitor_enabled(monitor_config.as_ref());
    let monitor_interval: u64 = get_monitor_interval(monitor_config.as_ref());
    let taosx_id = get_taosx_id(monitor_config.as_ref());

    if monitor_enabled {
        start_collect_agent_metrics(monitor_interval, taosx_id, agent_id);
        export_metrics(metrics_rx.clone(), resp_tx.clone(), monitor_interval);
    }

    let task_metrics_listener = tokio::spawn(listen_task_metrics(resp_tx.clone()));

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("SIGINT triggered");
            for task in tasks.iter() {
                let status = Activity::new::<String>(
                    *task.key(),
                    Utc::now(),
                    taosx_core::LevelFilter::Warn,
                    format!("{}x-agent is suspended by SIGINT", build::CUS_PROMPT),
                    "waiting".to_string(),
                    None,
                );
                if let Err(err) = client3.push_status(&status).await {
                    tracing::error!("Push status error: {err}");
                }
                if let Err(err) = sender.send_async(runner::Action::Interrupt(*task.key())).await {
                    tracing::error!("Send interrupt action to runner error: {err}");
                }
            }
            task_metrics_listener.abort();
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        _ = runner => {
            tracing::info!("Runner stopped");
            task_metrics_listener.abort();
        }
        err = async {
            let ret: anyhow::Result<()>;
            struct ErrorGate {
                error_queue: std::collections::VecDeque<(Instant, anyhow::Error)>,
                duration: Duration,
                limit: usize,
            }
            impl ErrorGate {
                fn new(limit: usize, duration: Duration) -> Self {
                    Self {
                        error_queue: std::collections::VecDeque::with_capacity(limit),
                        duration,
                        limit,
                    }
                }
                fn tick(&mut self, err: impl Into<anyhow::Error>) -> anyhow::Result<()> {
                    let now = std::time::Instant::now();
                    let err = err.into();
                    if self.error_queue.len() >= self.limit {
                        let (first_err_time, first_err) = self.error_queue.pop_front().unwrap();
                        if now.duration_since(first_err_time) < self.duration {
                            anyhow::bail!("Too many errors in {:?}, first error: {:#}; last error: {:#}",
                                self.duration, first_err, err
                            );
                        }
                    }
                    self.error_queue.push_back((now, err));
                    Ok(())
                }
            }
            let mut error_gate = ErrorGate::new(12, Duration::from_secs(60 * 2));
            loop {
                let sender = sender.clone();
                let heartbeat = spawn_heartbeat_task(resp_tx.clone());
                if let Err(err) = client.wait_tasks(sender, resp_tx.clone(), resp_rx.clone()).await {
                    heartbeat.abort();
                    tracing::debug!("Heartbeat task aborted");
                    let err_str = format!("{err:#}");
                    if err_str.contains("code: Aborted") {
                        tracing::info!("Connection aborted, error: {err:?}");
                        ret = Err(err);
                        break;
                    } else {
                        tracing::error!("Connection closed. Retry in 5 seconds");
                        if let Err(err) = error_gate.tick(err) {
                            tracing::info!("Connection failed: {err:#}");
                            ret = Err(err);
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            ret
         } => {
            tracing::error!("Task listener failed");
            err?;
        }
        _ = async {
            loop {
                match status.recv_async().await {
                    Ok(status) => {
                        for _ in 0..5 {
                            if let Err(err) = client2.push_status(&status).await {
                                tracing::error!("Push status error: {err}");
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            } else {
                                break;
                            }
                        }
                    },
                    Err(err) => {
                        tracing::error!("Status channel is disconnected: {err}");
                    }
                }
            }
        } => {
            tracing::info!("")
        }
    }
    task_metrics_listener.abort();
    Ok(())
}

fn get_monitor_interval(monitor_config: Option<&HashMap<String, String>>) -> u64 {
    if monitor_config.is_none() {
        return 30;
    } else {
        let monitor_config = monitor_config.unwrap();
        if let Some(interval) = monitor_config.get("interval") {
            if let Ok(interval) = interval.parse::<u64>() {
                return interval;
            }
        }
        return 30;
    }
}

fn get_monitor_enabled(monitor_config: Option<&HashMap<String, String>>) -> bool {
    if monitor_config.is_none() {
        return false;
    } else {
        let taosx_config = monitor_config.unwrap();
        if taosx_config.get("fqdn").is_some() {
            return true;
        }
        return false;
    }
}

fn get_taosx_id(monitor_config: Option<&HashMap<String, String>>) -> &'static str {
    if monitor_config.is_none() || monitor_config.unwrap().get("taosx_id").is_none() {
        "unknown"
    } else {
        let taosx_id = monitor_config.unwrap().get("taosx_id").unwrap();
        Box::leak(taosx_id.clone().into_boxed_str())
    }
}

fn start_collect_agent_metrics(monitor_interval: u64, taosx_id: &'static str, agent_id: i64) {
    use sysinfo::*;
    tracing::info!("Start collect agent metrics");
    let mut sys = System::new_all();
    let process_id = get_current_pid();
    if process_id.is_err() {
        let err = process_id.unwrap_err();
        tracing::error!("Get process id error: {err}");
        return;
    }
    let process_id = process_id.unwrap();
    let agent_id = agent_id.to_string();
    let agent_id = Box::leak(agent_id.into_boxed_str());
    tokio::spawn(async move {
        let mut collect_interval = tokio::time::interval(Duration::from_secs(monitor_interval));
        loop {
            let _ = process_metrics(
                &mut sys,
                taosx_id,
                agent_id,
                process_id,
                monitor_interval as f64,
            );
            collect_interval.tick().await;
        }
    });
}

pub fn process_metrics(
    sys: &mut sysinfo::System,
    taosx_id: &'static str,
    agent_id: &'static str,
    process_id: sysinfo::Pid,
    monitor_interval: f64,
) -> anyhow::Result<()> {
    sys.refresh_all();
    let labels = [
        ("stable", "taosx_agent"),
        ("taosx_id", taosx_id),
        ("agent_id", agent_id),
    ];
    // system metrics
    let cpu_cores = sys.cpus().len() as f64;
    gauge!("sys_cpu_cores", &labels).set(cpu_cores);
    gauge!("sys_total_memory", &labels).set(sys.total_memory() as f64);
    gauge!("sys_used_memory", &labels).set(sys.used_memory() as f64);
    gauge!("sys_available_memory", &labels).set(sys.available_memory() as f64);
    // process metrics
    gauge!("process_id", &labels).set(process_id.as_u32() as f64);
    if let Some(ps) = sys.process(process_id) {
        let cpu = ps.cpu_usage();
        gauge!("process_cpu_percent", &labels).set(cpu as f64 / cpu_cores);
        let mem = ps.memory() as f64 / sys.total_memory() as f64 * 100.0;
        gauge!("process_memory_percent", &labels).set(mem);
        let disk = ps.disk_usage();
        gauge!("process_disk_read_bytes", &labels).set(disk.read_bytes as f64 / monitor_interval);
        gauge!("process_disk_written_bytes", &labels)
            .set(disk.written_bytes as f64 / monitor_interval);
        gauge!("process_uptime", &labels).set(ps.run_time() as f64);
    }
    // connecotor process metrics
    update_sub_connector_process_metrics(
        sys,
        taosx_id.to_string(),
        process_id,
        monitor_interval,
        cpu_cores,
    );
    Ok(())
}

fn spawn_heartbeat_task(resp_tx: Sender<RespAction>) -> JoinHandle<()> {
    tracing::debug!("Spawn heartbeat task");
    tokio::spawn(async move {
        let mut heart_beat_interval = tokio::time::interval(Duration::from_secs(61));
        loop {
            heart_beat_interval.tick().await;
            if resp_tx.send(RespAction::Heartbeat).is_err() {
                tracing::warn!("Send heartbeat action error");
                break;
            }
        }
    })
}

fn export_metrics(
    metrics_rx: Receiver<MetricEvent>,
    resp_tx: Sender<RespAction>,
    monitor_interval: u64,
) -> JoinHandle<()> {
    tracing::info!("Start export metrics via rpc");
    tokio::spawn(async move {
        let mut export_interval = tokio::time::interval(Duration::from_secs(monitor_interval));
        loop {
            let mut metrics_events = MetricsEvents::new();
            loop {
                match metrics_rx.try_recv() {
                    Ok(event) => metrics_events.push(event),
                    Err(_) => break,
                }
            }
            if !metrics_events.is_empty() {
                tracing::debug!("Export metric events, total: {}", metrics_events.len());
                if let Err(err) = resp_tx.send(RespAction::Metrics(metrics_events)) {
                    tracing::warn!("Send metrics action error: {err}");
                    break;
                }
            } else {
                tracing::warn!("No metric events to export");
            }
            export_interval.tick().await;
        }
    })
}

#[rustfmt::skip]
fn print_effictive_config(log_keep_days: i64, args: &Args) {
    let log_opts = serde_json::to_vec(&args.log).unwrap();
    let log_opts_map = serde_json::from_slice::<HashMap<String, serde_json::Value>>(&log_opts).unwrap();
    let w = 18;
    let w2 = 20;
    let compression = *(AGENT_COMPRESSION.get().unwrap_or(&false));
    let mut s = String::new();
    s += "global config\n";
    s += "================================================================\n";
    s += &format!("{:<w$}{:<w2$}{}\n", ' ', "endpoint",  args.endpoint);
    s += &format!("{:<w$}{:<w2$}{}\n", ' ', "plugins_home",  get_plugins_home_dir().display());
    s += &format!("{:<w$}{:<w2$}{}\n", ' ', "data_dir",  get_data_dir().display());
    for (k, v) in log_opts_map {
        if v.is_null() {
            continue;
        }
        s += &format!("{:<w$}{:<w2$}{}\n", ' ', k,  v)
    }
    s += &format!("{:<w$}{:<w2$}{}\n", ' ', "log_keep_days",  log_keep_days);
    s += &format!("{:<w$}{:<w2$}{}\n", ' ', "compression",  compression);
    s += "================================================================";
    tracing::info!("{s}");
}

fn main() -> anyhow::Result<()> {
    let args = Args::init()?;
    set_env_plugins_home_dir(
        args.plugins_home
            .clone()
            .unwrap_or_else(|| get_env_plugin_dir()),
    );
    set_env_data_dir(args.data_dir.clone().unwrap_or_else(|| get_env_data_dir()));
    set_env_log_home_dir(
        args.log
            .as_ref()
            .and_then(|opts| opts.path.clone())
            .and_then(|p| p.to_str().map(ToString::to_string))
            .unwrap_or_else(|| get_env_log_dir()),
    );
    set_env_log_keep_days(args.log_keep_days.clone());

    let mut log_path = get_log_dir("");
    log_path.push(LOG_FILE);

    let log_keep_days = get_log_keep_days();

    // let (_non_blocking, _guard) = tracing_appender::non_blocking(rolling_file_appender);

    // let timer = LocalTime::new(format_description!(
    //     "[month]/[day] [hour]:[minute]:[second].[subsecond digits:6]"
    // ));

    // let chrono_local = Local::now();
    // let timezone_offset = (chrono_local.offset().local_minus_utc()
    //     / chrono::Duration::hours(1).num_seconds() as i32) as i8;

    // println!("local timezone offset: {}", timezone_offset);

    // let timer = OffsetTime::new(
    //     UtcOffset::from_hms(timezone_offset, 0, 0).unwrap(),
    //     format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]"),
    // );

    // let log_level = args.log_level.unwrap_or(Level::INFO);

    // let log_level_directive = match log_level {
    //     Level::ERROR => "error",
    //     Level::WARN => "warn",
    //     Level::INFO => "info",
    //     Level::DEBUG => "debug",
    //     Level::TRACE => "trace",
    // };
    // let _default_directive = format!("tungstenite=warn,tokio_tungstenite=warn,mio=warn,h2=warn,runtime=warn,actix_server={log_level_directive},actix_http={log_level_directive},{log_level_directive}", log_level_directive = log_level_directive);

    let mut layers = Vec::new();

    let LogOpts {
        level,
        compress,
        rotation_count,
        rotation_size,
        reserved_disk_size,
        ..
    } = args.log.as_ref().unwrap();

    let appender = RollingFileAppender::builder(
        get_env_log_dir(),
        format!("{}x_agent", build::CUS_PROMPT),
        *INSTANCE_ID.get().unwrap(),
    )
    .compress(compress.unwrap().to_bool()?)
    .reserved_disk_size(&reserved_disk_size.as_ref().unwrap())
    .rotation_count(rotation_count.unwrap())
    .rotation_size(&rotation_size.as_ref().unwrap())
    .build()
    .unwrap();

    layers.push(
        TaosLayer::<Qid>::new(appender)
            .with_filter(log_level_to_tracing_level(level.unwrap()))
            .boxed(),
    );

    #[cfg(debug_assertions)]
    layers.push(
        TaosLayer::<Qid, _, _>::new(std::io::stdout)
            .with_ansi()
            .with_location()
            .with_filter(log_level_to_tracing_level(level.unwrap()))
            .boxed(),
    );

    tracing_subscriber::registry().with(layers).init();

    let _span = tracing::info_span!("main").entered();

    let version = build::PKG_VERSION;
    let commit_id = build::COMMIT_HASH;
    let build_time = build::BUILD_TIME;
    tracing::info!("version: {version}");
    tracing::info!("commit id: {commit_id}");
    tracing::info!("build time: {build_time}");
    print_effictive_config(log_keep_days, &args);
    tracing::info!("Start");

    // todo: arrow flight rpc client.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(4096)
        .thread_name("taosx-agent")
        .enable_all()
        .build()?;

    rt.block_on(main_agent_service(args).in_current_span())?;
    rt.shutdown_timeout(Duration::from_secs(5));

    Ok(())
}
