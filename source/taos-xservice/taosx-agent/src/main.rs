use std::{
    collections::HashMap, ops::RangeInclusive, path::PathBuf, pin::Pin, sync::Arc, time::Duration,
};

use agent::listen_task_metrics;
use anyhow::{Context, bail};
use arrow_flight::error::FlightError;
use clap::{CommandFactory, Parser};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use const_format::concatcp;
use flume::{Receiver, Sender};
use futures_ext::OptionFuture;
use metrics::gauge;
use taoslog::{layer::TaosLayer, writer::RollingFileAppender};
use taosx_metrics::{MetricEvent, MetricsEvents};
use taosx_utils::signal::wait_signal;
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tonic::{Code, transport::Certificate};

use tracing::instrument;
use tracing::{Instrument, log::LevelFilter};
use tracing_subscriber::{
    Layer as _, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
};
use twelf::{Layer, config};

use ha_core::{activity::Activity, utils::next_req_id};
use taosx_core::{
    AGENT_COMPRESSION, RespAction, get_log_dir, get_log_keep_days, set_env_data_dir,
    set_env_log_home_dir, set_env_log_keep_days, set_env_plugins_home_dir,
};
use taosx_core::{
    get_data_dir,
    runners::{
        ENV_LOGS_HOME, ENV_PLUGINS_HOME, ENV_TAOSX_DATA_DIR, ENV_TAOSX_LOGS_HOME,
        ENV_TAOSX_PLUGINS_HOME, get_plugins_home_dir,
    },
    utils::{
        monitor::update_sub_connector_process_metrics,
        trace::{self, INSTANCE_ID, Qid},
    },
};
use taosx_core::{global::GLOBAL_LOG_OPTS, utils::trace::DEFAULT_AGENT_INSTANCE_ID};

use crate::agent::client::Client;

const LOG_FILE: &str = "agent.log";
const AGENT_SERVICE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

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
    level_upgrade(level, num - 1)
}

fn finish_rustls_provider_install(
    result: std::result::Result<(), Arc<rustls::crypto::CryptoProvider>>,
) -> anyhow::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(_) => {
            tracing::debug!("rustls crypto provider already installed");
            Ok(())
        }
    }
}

fn install_rustls_provider() -> anyhow::Result<()> {
    finish_rustls_provider_install(rustls::crypto::ring::default_provider().install_default())
}

#[derive(Debug)]
pub struct Args {
    plugins_home: Option<String>,

    data_dir: Option<String>,

    log: Option<LogOpts>,

    /// Listen to ip:port address.
    endpoint: String,

    token: String,

    ca: Option<String>,

    log_keep_days: Option<i64>,

    /// For in-memory cache queue capacity.
    in_memory_cache_capacity: Option<usize>,

    // manually specified port range, eg. 9000-9099
    ports: Option<RangeInclusive<u16>>,

    keep_online: bool,
}

#[config]
#[derive(Parser, Debug)]
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

    /// Token for authentication.
    #[clap(short = 't', long)]
    token: Option<String>,

    /// For TLS CA certificate.
    #[clap(long)]
    ca: Option<String>,

    /// To enable compression.
    #[clap(long)]
    compression: Option<bool>,

    /// For in-memory cache queue capacity.
    #[clap(long)]
    in_memory_cache_capacity: Option<usize>,

    /// For environment variable wised log level.
    #[clap(hide = true, env = "LOG_LEVEL")]
    log_level: Option<LevelFilter>,

    #[clap(long, env = "LOG_KEEP_DAYS")]
    log_keep_days: Option<i64>,

    #[clap(flatten)]
    client_port_range: Option<ClientPortRange>,

    #[clap(long, default_value = "true", action = clap::ArgAction::SetTrue)]
    keep_online: Option<bool>,
}

#[config]
#[derive(Parser, Debug, Clone, serde::Serialize)]
pub struct ClientPortRange {
    min: Option<u16>,
    max: Option<u16>,
}

#[derive(Parser, Debug)]
#[clap(
    name = build::CUS_CLI_NAME,
    author, version = CLAP_SHORT_VERSION,
    about = build::CUS_CLI_ABOUT,
    long_about = build::CUS_CLI_ABOUT)]
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
            rotation_count: Some(3),
            keep_days: Some(3),
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
    #[error("Reading {0} from {1} error: {2}")]
    ReadCertError(&'static str, String, std::io::Error),
}

#[inline]
fn get_effective_config_path(args: &ArgsParser) -> PathBuf {
    args.config.clone().unwrap_or_else(get_default_config_path)
}

#[cfg(windows)]
fn get_default_config_path() -> PathBuf {
    std::path::Path::new("C:\\")
        .join(build::CANONICAL_CUS_NAME)
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
    if let Ok(dir) = std::env::var(ENV_LOGS_HOME) {
        return dir;
    }
    if let Ok(dir) = std::env::var(ENV_TAOSX_LOGS_HOME) {
        return dir;
    }

    if cfg!(windows) {
        format!("C:\\{}\\log", build::CANONICAL_CUS_NAME)
    } else {
        format!("/var/log/{}", build::CUS_PROMPT)
    }
}

fn get_env_data_dir() -> String {
    if let Ok(dir) = std::env::var(ENV_TAOSX_DATA_DIR) {
        return dir;
    }

    if cfg!(windows) {
        format!(
            "C:\\{}\\data\\{}xagent",
            build::CANONICAL_CUS_NAME,
            build::CUS_PROMPT
        )
    } else {
        format!("/var/lib/{0}/{0}xagent", build::CUS_PROMPT)
    }
}

fn get_env_plugin_dir() -> String {
    if let Ok(dir) = std::env::var(ENV_PLUGINS_HOME) {
        return dir;
    }
    if let Ok(dir) = std::env::var(ENV_TAOSX_PLUGINS_HOME) {
        return dir;
    }

    if cfg!(windows) {
        format!("C:\\{}\\plugins", build::CANONICAL_CUS_NAME)
    } else {
        format!("/usr/local/{}/plugins", build::CUS_PROMPT)
    }
}

impl Args {
    pub fn init() -> Result<Args, ArgsError> {
        let args = ArgsParser::parse();
        let path = get_effective_config_path(&args);

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
            in_memory_cache_capacity,
            mut log,
            client_port_range,
            ca,
            keep_online,
            ..
        } = ConfigArgs::with_layers(&layers)?;

        let mut level_filter = log
            .as_ref()
            .and_then(|opts| opts.level)
            .or(log_level)
            .unwrap_or(LevelFilter::Info);

        if args.verbose.as_ref().is_some() {
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
                let opts = LogOpts {
                    level: Some(level_filter),
                    path: log_home,
                    ..Default::default()
                };
                log = Some(opts);
            }
        }

        INSTANCE_ID.get_or_init(|| instance_id.unwrap_or(DEFAULT_AGENT_INSTANCE_ID));

        AGENT_COMPRESSION.set(compression.unwrap_or(false)).unwrap();

        // set the range of client port
        let ports = client_port_range.and_then(|client_port_range| {
            if client_port_range.min.is_some() || client_port_range.max.is_some() {
                let port_min = client_port_range
                    .min
                    .map_or(49152, |port| port.clamp(49152, 65535));
                let port_max = client_port_range
                    .max
                    .map_or(65535, |port| port.clamp(port_min, 65535));
                Some(port_min..=port_max)
            } else {
                None
            }
        });

        Ok(Args {
            plugins_home,
            data_dir,
            endpoint: endpoint
                .ok_or_else(|| ArgsError::MissingRequiredArgument("endpoint".to_string()))?,
            token: token.ok_or_else(|| ArgsError::MissingRequiredArgument("token".to_string()))?,
            ca,
            log_keep_days,
            log,
            in_memory_cache_capacity,
            ports,
            keep_online: keep_online.unwrap_or(true),
        })
    }

    fn ca(&self) -> Result<Option<Certificate>, ArgsError> {
        if let Some(ca) = &self.ca {
            let cert = if ca.starts_with("-----BEGIN") {
                Certificate::from_pem(ca)
            } else {
                Certificate::from_pem(
                    std::fs::read_to_string(ca)
                        .map_err(|p| ArgsError::ReadCertError("ca", ca.to_string(), p))?
                        .trim(),
                )
            };
            return Ok(Some(cert));
        }
        Ok(None)
    }
}

mod agent;
mod runner;

async fn main_agent_service(args: Args) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let mut handle = JoinSet::new();
    let (metrics_tx, metrics_rx) = flume::bounded(1000);
    let (metrics_trigger_tx, metrics_trigger_rx) = flume::bounded::<()>(1);
    taosx_metrics::ChannelRecorder::new(Arc::new(metrics_tx)).install();
    for endpoint in args.endpoint.split(",").map(|v| v.trim()) {
        handle.spawn(main_agent_service_inner(
            endpoint.to_string(),
            args.token.clone(),
            args.ports.clone(),
            args.ca()?,
            metrics_rx.clone(),
            metrics_trigger_tx.clone(),
            metrics_trigger_rx.clone(),
            args.keep_online,
            cancel.child_token(),
        ));
    }

    let mut wait_for_shutdown_signal = Box::pin(wait_signal());
    let mut shutdown_requested = false;
    let mut shutdown_timer: Pin<Box<OptionFuture<tokio::time::Sleep>>> =
        Box::pin(OptionFuture::from(None::<tokio::time::Sleep>));
    loop {
        tokio::select! {
            sig = &mut wait_for_shutdown_signal => {
                match sig {
                    Ok(sig) => {
                        if shutdown_requested {
                            tracing::warn!("received second {sig}, aborting remaining agent tasks");
                            handle.abort_all();
                            break;
                        }
                        tracing::info!("received {sig}, shutting down agent service");
                    }
                    Err(e) => {
                        if shutdown_requested {
                            tracing::error!("failed while waiting for follow-up shutdown signal: {e:#}, aborting remaining agent tasks");
                            handle.abort_all();
                            break;
                        }
                        handle.abort_all();
                        return Err(e).context("failed while waiting for shutdown signal");
                    }
                }
                if !shutdown_requested {
                    shutdown_requested = true;
                    shutdown_timer = Box::pin(OptionFuture::from(Some(tokio::time::sleep_until(
                        tokio::time::Instant::now() + AGENT_SERVICE_SHUTDOWN_TIMEOUT,
                    ))));
                    wait_for_shutdown_signal = Box::pin(wait_signal());
                    cancel.cancel();
                }
            }
            _ = &mut shutdown_timer => {
                tracing::error!(
                    "agent service shutdown timed out after {} seconds, aborting remaining tasks",
                    AGENT_SERVICE_SHUTDOWN_TIMEOUT.as_secs()
                );
                handle.abort_all();
                break;
            }
            res = handle.join_next() => {
                let Some(res) = res else {
                    break
                };
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::error!("Agent service failed: {e:#}");
                    }
                    Err(e) => {
                        tracing::error!("Agent service panicked: {}", e);
                    }
                }
            }
        }
    }

    tracing::info!("Agent service stopped");
    Ok(())
}

#[instrument(skip_all)]
async fn main_agent_service_inner(
    endpoint: String,
    token: String,
    ports: Option<RangeInclusive<u16>>,
    ca: Option<Certificate>,
    metrics_rx: Receiver<MetricEvent>,
    metrics_trigger_tx: Sender<()>,
    metrics_trigger_rx: Receiver<()>,
    keep_online: bool,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    if let Some(ca) = &ca {
        taosx_core::global::set_agent_client_ca(ca.clone());
    }
    const INIT_RETRY_INTERVAL: Duration = Duration::from_millis(500);
    let mut retry_interval = INIT_RETRY_INTERVAL;
    const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(10);
    macro_rules! retry {
        () => {{
            tokio::time::sleep(retry_interval).await;
            retry_interval = (retry_interval * 2).min(MAX_RETRY_INTERVAL);
            continue;
        }};
    }

    // Long-lived runner: survives across reconnections so that data tasks
    // (especially those with persist_data_enable=true) keep running during
    // network outages.  Only cancelled on agent shutdown.
    let runner_cancel = cancel.child_token();
    let mut runner_handle = JoinSet::new();
    // runner_tx / activity_rx are lazily initialized on first successful
    // connection and reused across reconnections.
    let mut runner_tx: Option<flume::Sender<runner::Action>> = None;
    let mut activity_rx: Option<flume::Receiver<ha_core::activity::Activity>> = None;
    // Shared resp channel: the runner and long-lived tasks send status
    // messages here; each process_actions() call consumes from a clone of
    // resp_rx.  Messages buffered during disconnection are drained on
    // the next connection.
    let (resp_tx, resp_rx) = flume::bounded::<RespAction>(1000);

    // Per-connection state: heartbeat, activity push, metrics export.
    // Cancelled and re-created on each reconnection.
    let mut conn_handle = JoinSet::new();
    let mut conn_cancel = cancel.child_token();

    macro_rules! wait_conn_handle {
        () => {
            if !conn_handle.is_empty() {
                conn_cancel.cancel();
                tracing::info!("Waiting for connection tasks to complete");
            }
            while let Some(res) = conn_handle.join_next().await {
                match res {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => tracing::error!("Connection task error: {err}"),
                    Err(err) => tracing::error!("Connection task panic: {err}"),
                }
            }
        };
    }

    loop {
        if cancel.is_cancelled() {
            break;
        }
        if !keep_online {
            break;
        }

        // Stop only connection-level tasks; data tasks in runner_handle
        // continue running.
        wait_conn_handle!();

        // Drain completed runner tasks (non-blocking) to avoid stale entries.
        // If all runner tasks have exited (e.g. due to internal error),
        // reset runner_tx so the runner is re-initialized on next connection.
        while let Some(res) = runner_handle.try_join_next() {
            match res {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => tracing::error!("Runner task error: {err}"),
                Err(err) => tracing::error!("Runner task panic: {err}"),
            }
        }
        if runner_tx.is_some() && runner_handle.is_empty() {
            tracing::warn!("All runner tasks exited unexpectedly, resetting runner state");
            runner_tx = None;
            activity_rx = None;
        }

        let mut client = match cancel
            .run_until_cancelled(Client::new(&endpoint, &token, ca.clone(), &ports))
            .await
        {
            Some(Ok(client)) => client,
            Some(Err(agent::client::Error::Handshake {
                source: FlightError::Tonic(status),
            })) if matches!(
                status.code(),
                Code::Aborted | Code::PermissionDenied | Code::InvalidArgument
            ) =>
            {
                tracing::error!("Handshake invalid: {status}");
                break;
            }
            Some(Err(e)) => {
                tracing::error!("Failed to create client: {:#}", anyhow::Error::new(e));
                retry!();
            }
            None => break,
        };
        tracing::info!(endpoint, "connect to xnode successfully");
        retry_interval = INIT_RETRY_INTERVAL;

        let agent_id = client.agent_id();

        conn_cancel = cancel.child_token();

        // Initialize the long-lived runner on first successful connection.
        if runner_tx.is_none() {
            let (sender, activities) = runner::spawn_runner(
                agent_id,
                &endpoint,
                &token,
                resp_tx.clone(),
                &mut runner_handle,
                runner_cancel.clone(),
            );
            runner_tx = Some(sender);
            activity_rx = Some(activities);
        }

        // 给 taosx 发送 activity（per-connection: recreated on reconnect）
        if let Some(ref activities) = activity_rx {
            conn_handle.spawn({
                let client = Client::new(&endpoint, &token, ca.clone(), &ports)
                    .await
                    .context("build agent client error")?;
                let cancel = conn_cancel.clone();
                send_activity(activities.clone(), client, cancel)
            });
        }

        // 给 tasox 发送 agent 运行时 metrics
        let monitor_config = client.get_taosx_monitor_config().await;
        let monitor_enabled: bool = get_monitor_enabled(monitor_config.as_ref());
        if monitor_enabled {
            let monitor_interval: u64 = get_monitor_interval(monitor_config.as_ref());
            let taosx_id = get_taosx_id(monitor_config.as_ref());
            conn_handle.spawn(start_collect_agent_metrics(
                monitor_interval,
                taosx_id,
                agent_id,
                metrics_trigger_tx.clone(),
                metrics_trigger_rx.clone(),
                conn_cancel.clone(),
            ));
            conn_handle.spawn(export_metrics(
                metrics_rx.clone(),
                resp_tx.clone(),
                monitor_interval,
                conn_cancel.clone(),
            ));
        }
        // 给 taosx 发送任务 metrics（per-connection）
        conn_handle.spawn(listen_task_metrics(resp_tx.clone(), conn_cancel.clone()));
        // 给 tasox 发送心跳（per-connection）
        conn_handle.spawn(heartbeat_task(resp_tx.clone(), conn_cancel.clone()));

        // do exchange 接收 taosx 发送来的消息
        let sender = runner_tx.clone().expect("runner_tx should be initialized");
        let Some(res) = cancel
            .run_until_cancelled(conn_cancel.run_until_cancelled(client.process_actions(
                sender,
                resp_tx.clone(),
                resp_rx.clone(),
            )))
            .await
        else {
            continue;
        };
        let Some(res) = res else {
            // conn_cancel was triggered (e.g. connection-level timeout),
            // retry the connection while keeping runner tasks alive.
            tracing::info!("connection cancelled, reconnecting...");
            retry!();
        };
        match res {
            Ok(_) => continue,
            Err(agent::client::Error::DoExchange { source }) => {
                tracing::error!("Process actions do exchange error: {source}");
                if let FlightError::Tonic(status) = source
                    && matches!(status.code(), Code::Unknown | Code::Unavailable)
                {
                    tracing::info!("flight connection disconnected, retry...");
                    retry!();
                }
            }
            Err(e) => {
                tracing::error!("Failed to process actions: {:#}", anyhow::Error::new(e))
            }
        }
    }

    // Shutdown: cancel runner (and all data tasks), then wait.
    runner_cancel.cancel();
    conn_cancel.cancel();
    while let Some(res) = conn_handle.join_next().await {
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => tracing::error!("Connection task error on shutdown: {err}"),
            Err(err) => tracing::error!("Connection task panic on shutdown: {err}"),
        }
    }
    while let Some(res) = runner_handle.join_next().await {
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => tracing::error!("Runner task error on shutdown: {err}"),
            Err(err) => tracing::error!("Runner task panic on shutdown: {err}"),
        }
    }
    tracing::info!(endpoint, "Agent runner exited");
    Ok(())
}

#[instrument(skip_all)]
async fn send_activity(
    receiver: flume::Receiver<Activity>,
    mut client: Client,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let _guard = cancel.drop_guard_ref();
    while let Some(Ok(activity)) = cancel.run_until_cancelled(receiver.recv_async()).await {
        if let Err(e) = client.push_status(&activity).await {
            if let agent::client::Error::DoExchange { source } = &e
                && let FlightError::Tonic(status) = source
                && matches!(status.code(), Code::Unknown | Code::Unavailable)
            {
                tracing::info!("flight connection disconnected, retry...");
                return Ok(());
            }
            tracing::error!("Push activity error: {:#}", anyhow::Error::new(e));
        }
    }
    Ok(())
}

fn get_monitor_interval(monitor_config: Option<&HashMap<String, String>>) -> u64 {
    if monitor_config.is_none() {
        30
    } else {
        if let Some(interval) = monitor_config.and_then(|c| c.get("interval"))
            && let Ok(interval) = interval.parse::<u64>()
        {
            return interval;
        }
        30
    }
}

fn get_monitor_enabled(monitor_config: Option<&HashMap<String, String>>) -> bool {
    if monitor_config.is_none() {
        false
    } else {
        monitor_config.and_then(|c| c.get("fqdn")).is_some()
    }
}

fn get_taosx_id(monitor_config: Option<&HashMap<String, String>>) -> &'static str {
    monitor_config
        .and_then(|map| map.get("taosx_id"))
        .map_or("unknown", |taosx_id| {
            Box::leak(taosx_id.clone().into_boxed_str())
        })
}

#[instrument(skip_all)]
async fn start_collect_agent_metrics(
    monitor_interval: u64,
    taosx_id: &'static str,
    agent_id: i64,
    metrics_trigger_tx: Sender<()>,
    metrics_trigger_rx: Receiver<()>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    use sysinfo::*;
    tracing::info!("Start collect agent metrics");
    let kind = RefreshKind::nothing()
        .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
        .with_memory(MemoryRefreshKind::nothing().with_ram());
    let mut sys = System::new_with_specifics(kind);
    let process_id = match get_current_pid() {
        Ok(pid) => pid,
        Err(err) => {
            tracing::error!("Get process id error: {err}");
            return Ok(());
        }
    };
    let agent_id = agent_id.to_string();
    let agent_id = Box::leak(agent_id.into_boxed_str());
    let mut collect_interval = tokio::time::interval(Duration::from_secs(monitor_interval));
    loop {
        if cancel
            .run_until_cancelled(metrics_trigger_rx.recv_async())
            .await
            .is_none()
        {
            break;
        }
        let _ = process_metrics(
            &mut sys,
            kind,
            taosx_id,
            agent_id,
            process_id,
            monitor_interval as f64,
        )
        .await;
        if cancel
            .run_until_cancelled(collect_interval.tick())
            .await
            .is_none()
        {
            break;
        }
        if cancel
            .run_until_cancelled(metrics_trigger_tx.send_async(()))
            .await
            .is_none()
        {
            break;
        }
    }

    Ok(())
}

pub async fn process_metrics(
    sys: &mut sysinfo::System,
    kind: sysinfo::RefreshKind,
    taosx_id: &'static str,
    agent_id: &'static str,
    process_id: sysinfo::Pid,
    monitor_interval: f64,
) -> anyhow::Result<()> {
    sys.refresh_specifics(kind);
    sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[process_id]),
        false,
        sysinfo::ProcessRefreshKind::nothing()
            .with_cpu()
            .with_memory()
            .with_disk_usage()
            .with_tasks(),
    );
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
    )
    .await;
    Ok(())
}

async fn heartbeat_task(
    resp_tx: Sender<RespAction>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::debug!("Spawn heartbeat task");
    let _guard = cancel.drop_guard_ref();
    let mut heart_beat_interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        if cancel
            .run_until_cancelled(heart_beat_interval.tick())
            .await
            .is_none()
        {
            return Ok(());
        }
        if cancel
            .run_until_cancelled(resp_tx.send_async(RespAction::Heartbeat(next_req_id())))
            .await
            .is_none_or(|v| v.is_err())
        {
            tracing::warn!("Send heartbeat action error");
            return Ok(());
        }
    }
}

async fn export_metrics(
    metrics_rx: Receiver<MetricEvent>,
    resp_tx: Sender<RespAction>,
    monitor_interval: u64,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut export_interval = tokio::time::interval(Duration::from_secs(monitor_interval));
    loop {
        let mut metrics_events = MetricsEvents::new();
        while let Ok(event) = metrics_rx.try_recv() {
            metrics_events.push(event);
        }
        if !metrics_events.is_empty() {
            tracing::debug!("Export metric events, total: {}", metrics_events.len());
            if cancel
                .run_until_cancelled(resp_tx.send_async(RespAction::Metrics(metrics_events)))
                .await
                .is_none_or(|v| v.is_err())
            {
                break;
            }
        }
        if cancel
            .run_until_cancelled(export_interval.tick())
            .await
            .is_none()
        {
            break;
        }
    }
    Ok(())
}

#[rustfmt::skip]
fn print_effective_config(log_keep_days: i64, args: &Args) -> Result<(), std::io::Error> {
    let log_opts = serde_json::to_vec(&args.log).expect("serialize log options failed");
    let log_opts_map = serde_json::from_slice::<HashMap<String, serde_json::Value>>(&log_opts).expect("deserialize log options failed");
    let w = 18;
    let w2 = 20;
    let compression = *(AGENT_COMPRESSION.get().unwrap_or(&false));
    let mut cache: Vec::<u8> = Vec::new();
    let mut cursor = std::io::Cursor::new(&mut cache);
    use std::io::Write;

    writeln!(cursor, "global config")?;
    writeln!(cursor, "================================================================")?;
    writeln!(cursor, "{:<w$}{:<w2$}{}", ' ', "endpoint",  args.endpoint)?;
    writeln!(cursor, "{:<w$}{:<w2$}{}", ' ', "plugins_home",  get_plugins_home_dir().display())?;
    writeln!(cursor, "{:<w$}{:<w2$}{}", ' ', "data_dir",  get_data_dir().display())?;
    for (k, v) in log_opts_map {
        if v.is_null() {
            continue;
        }
        writeln!(cursor, "{:<w$}{:<w2$}{}", ' ', k,  v)?;
    }
    writeln!(cursor, "{:<w$}{:<w2$}{}", ' ', "log_keep_days",  log_keep_days)?;
    writeln!(cursor, "{:<w$}{:<w2$}{}", ' ', "compression",  compression)?;
    writeln!(cursor, "{:<w$}{:<w2$}{}", ' ', "keep_online",  args.keep_online)?;
    write!(cursor, "================================================================")?;
    tracing::info!("{}", String::from_utf8_lossy(&cache));
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = Args::init()?;
    set_env_plugins_home_dir(args.plugins_home.clone().unwrap_or_else(get_env_plugin_dir));
    set_env_data_dir(args.data_dir.clone().unwrap_or_else(get_env_data_dir));
    set_env_log_home_dir(
        args.log
            .as_ref()
            .and_then(|opts| opts.path.clone())
            .and_then(|p| p.to_str().map(ToString::to_string))
            .unwrap_or_else(get_env_log_dir),
    );
    set_env_log_keep_days(args.log_keep_days);

    let mut log_path = get_log_dir("");
    log_path.push(LOG_FILE);

    let log_keep_days = get_log_keep_days();

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
        *INSTANCE_ID.get_or_init(|| DEFAULT_AGENT_INSTANCE_ID),
    )
    .compress(compress.unwrap().to_bool()?)
    .reserved_disk_size(reserved_disk_size.as_ref().unwrap())
    .rotation_count(rotation_count.unwrap())
    .rotation_size(rotation_size.as_ref().unwrap())
    .build()
    .unwrap();

    GLOBAL_LOG_OPTS
        .set(taosx_core::global::LogOpts {
            instance_id: *INSTANCE_ID.get().unwrap(),
            compress: compress.map(|c| c.to_bool().unwrap_or(false)),
            rotation_count: *rotation_count,
            keep_days: None,
            rotation_size: rotation_size.clone(),
            reserved_disk_size: reserved_disk_size.clone(),
        })
        .expect("set global log options failed");

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
    install_rustls_provider()?;

    let _span = tracing::info_span!("main").entered();

    // init qid batch_id db
    trace::qid_db_init(INSTANCE_ID.get_or_init(|| DEFAULT_AGENT_INSTANCE_ID))?;

    let version = build::PKG_VERSION;
    let commit_id = build::COMMIT_HASH;
    let build_time = build::BUILD_TIME;
    tracing::info!("version: {version}");
    tracing::info!("commit id: {commit_id}");
    tracing::info!("build time: {build_time}");
    print_effective_config(log_keep_days, &args)?;

    if let Some(capacity) = args.in_memory_cache_capacity {
        taosx_core::global::set_agent_in_memory_cache_capacity(capacity);
    }

    tracing::info!("Start");

    // Set a panic hook
    std::panic::set_hook(Box::new(|info| {
        // 正常打印 backtrace, 需要设置环境变量: RUST_BACKTRACE=1
        let backtrace = std::backtrace::Backtrace::capture();
        tracing::error!("panic occurred. {} {}", info, backtrace);
    }));

    // Register KingHistorian datasets lister to avoid taosx-core <-> kinghistorian circular deps
    // Safe to call once; subsequent calls are ignored by OnceLock
    #[allow(clippy::redundant_closure)]
    {
        taosx_core::plugins::register_datasets_lister(
            source_kinghistorian::KING_HIST_ID,
            source_kinghistorian::kinghist_datasets_lister,
        );
        tracing::info!("Registered KingHistorian datasets lister");

        taosx_core::plugins::register_datasets_lister(
            source_pspace::PSPACE_ID,
            source_pspace::pspace_datasets_lister,
        );
        tracing::info!("Registered PSPACE datasets lister");
    }

    // todo: arrow flight rpc client.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(4096)
        .thread_name("taosx-agent")
        .enable_all()
        .build()?;

    rt.block_on(main_agent_service(args).in_current_span())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[test]
    fn install_rustls_provider_is_idempotent() {
        super::install_rustls_provider().expect("first install should succeed");
        super::install_rustls_provider().expect("second install should be treated as success");
    }

    #[test]
    fn finish_rustls_provider_install_treats_already_installed_as_success() {
        let provider = rustls::crypto::ring::default_provider();
        let result = super::finish_rustls_provider_install(Err(Arc::new(provider)));
        assert!(
            result.is_ok(),
            "already-installed should not be treated as an error"
        );
    }
}
