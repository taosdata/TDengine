use chrono::{Local, Utc};
use clap::{CommandFactory, Parser};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use const_format::concatcp;
use flume::{Receiver, Sender};
use metrics::counter;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use taosx_metrics::{MetricEvent, MetricsEvents};
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing_appender::rolling::{RollingFileAppender, Rotation};

use time::macros::format_description;
use time::UtcOffset;
use tracing_subscriber::{
    fmt::time::OffsetTime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    Layer as _,
};
use twelf::{config, Layer};

use taosx_core::{
    get_data_dir,
    runners::{get_logs_home_dir, get_plugins_home_dir},
    utils::trace::TaosXLayer,
};
use taosx_core::{
    get_log_dir, get_log_keep_days, set_env_data_dir, set_env_log_home_dir, set_env_log_keep_days,
    set_env_plugins_home_dir, Activity, RespAction, AGENT_COMPRESSION,
};
use tracing::{log::LevelFilter, Level};

const LOG_FILE: &str = "agent.log";

shadow_rs::shadow!(build);

const CLAP_SHORT_VERSION: &str = if build::GIT_CLEAN {
    concatcp!(
        "version: ",
        build::TD_VERSION,
        "\ngit: ",
        build::COMMIT_HASH,
        "\nbuild: core-",
        build::PKG_VERSION,
        if build::IS_DEBUG { " debug " } else { " " },
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
} else {
    concatcp!(
        "version: ",
        build::TD_VERSION,
        "\ngit: ",
        build::COMMIT_HASH,
        "\nbuild: core-dirty-",
        build::PKG_VERSION,
        if build::IS_DEBUG { " debug " } else { " " },
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
};

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

#[derive(Debug)]
pub struct Args {
    plugins_home: Option<String>,

    data_dir: Option<String>,

    logs_home: Option<String>,

    /// Listen to ip:port address.
    endpoint: String,

    token: String,

    log_level: Option<Level>,

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

    #[clap(long, env = "LOGS_HOME")]
    logs_home: Option<String>,

    /// Listen to ip:port address.
    #[clap(short = 'e', long)]
    endpoint: Option<String>,

    #[clap(short = 't', long)]
    token: Option<String>,

    #[clap(long)]
    compression: Option<bool>,

    /// For verbosity logging.
    #[clap(flatten)]
    #[serde(skip)]
    verbose: Option<Verbosity<InfoLevel>>,

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
impl Args {
    pub fn init() -> Result<Args, ArgsError> {
        let path = if let Ok(c) = ArgsParser::try_parse() {
            c.config
                .map(|p| {
                    if p.exists() {
                        Ok(p)
                    } else {
                        Err(ArgsError::ConfigNotFound(p.display().to_string()))
                    }
                })
                .transpose()?
        } else {
            None
        }
        .unwrap_or_else(|| {
            if cfg!(windows) {
                std::path::Path::new("C:\\")
                    .join("TDengine")
                    .join("cfg")
                    .join("agent.toml")
            } else {
                std::path::Path::new("/etc")
                    .join(build::CUS_PROMPT)
                    .join("agent.toml")
            }
        });

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
            verbose,
            log_keep_days,
            ..
        } = ConfigArgs::with_layers(&layers)?;
        let log_level = log_level_to_tracing_level(
            log_level
                .clone()
                .or(verbose.clone().map(|v| v.log_level_filter()))
                .unwrap_or(log::LevelFilter::Info),
        );

        AGENT_COMPRESSION.set(compression.unwrap_or(false)).unwrap();

        Ok(Args {
            plugins_home,
            data_dir,
            logs_home,
            endpoint: endpoint
                .ok_or_else(|| ArgsError::MissingRequiredArgument("endpoint".to_string()))?,
            token: token.ok_or_else(|| ArgsError::MissingRequiredArgument("token".to_string()))?,
            log_level,
            log_keep_days,
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

    let (resp_tx, resp_rx) = flume::unbounded::<RespAction>();

    let (runner, tasks, sender, status) =
        runner::spawn_runner(agent.id, &args.endpoint, &args.token, resp_tx.clone());

    let (metrics_tx, metrics_rx) = flume::bounded(1000);
    taosx_metrics::ChannelRecorder::new(Arc::new(metrics_tx)).install();

    let monitor_config = client.get_taosx_monitor_config().await;
    let monitor_enabled: bool = get_monitor_enabled(monitor_config.as_ref());
    let monitor_interval: u64 = get_monitor_interval(monitor_config.as_ref());

    if monitor_enabled {
        start_collect_agent_metrics(monitor_interval);
        export_metrics(metrics_rx.clone(), resp_tx.clone());
    }

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("SIGINT triggered");
            for task in tasks.iter() {
                let status = Activity::new::<String>(
                    *task.key(),
                    Utc::now(),
                    taosx_core::LevelFilter::Warn,
                    "taosx-agent is suspended by SIGINT".to_string(),
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
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        _ = runner => {
            tracing::info!("Runner stopped");
        }
        err = async {
            let ret: anyhow::Result<()>;
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
                        tracing::error!("Connection closed, error: {err:?}. Retry in 5 seconds");
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
    Ok(())
}

fn get_monitor_interval(taosx_config: Option<&HashMap<String, String>>) -> u64 {
    if taosx_config.is_none() {
        return 30;
    } else {
        let taosx_config = taosx_config.unwrap();
        if let Some(interval) = taosx_config.get("monitor_interval") {
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

fn start_collect_agent_metrics(monitor_interval: u64) {
    tracing::info!("Start collect agent metrics");
    tokio::spawn(async move {
        let mut collect_interval = tokio::time::interval(Duration::from_secs(monitor_interval));
        loop {
            counter!("hello-taosx", "version" => "0.1").increment(1);
            collect_interval.tick().await;
        }
    });
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
) -> JoinHandle<()> {
    tracing::debug!("Start export metrics via rpc");
    tokio::spawn(async move {
        let mut export_interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            let mut metrics_events = MetricsEvents::new();
            loop {
                match metrics_rx.try_recv() {
                    Ok(event) => metrics_events.push(event),
                    Err(_) => break,
                }
            }
            if !metrics_events.is_empty() {
                if let Err(err) = resp_tx.send(RespAction::Metrics(metrics_events)) {
                    tracing::warn!("Send metrics action error: {err}");
                    break;
                }
            }
            export_interval.tick().await;
        }
    })
}

#[rustfmt::skip]
fn print_effictive_config(log_level: Level, log_path: PathBuf, log_keep_days: i64, args: &Args) {
    let w = 18;
    let w2 = 20;
    tracing::info!("                           global config");
    tracing::info!("================================================================");
    tracing::info!("{:<w$}{:<w2$}{}", ' ', "endpoint",  args.endpoint);
    tracing::info!("{:<w$}{:<w2$}{}", ' ', "plugins_home",  get_plugins_home_dir().display());
    tracing::info!("{:<w$}{:<w2$}{}", ' ', "data_dir",  get_data_dir().display());
    tracing::info!("{:<w$}{:<w2$}{}", ' ', "log_home",  get_logs_home_dir().display());
    tracing::info!("{:<w$}{:<w2$}{}", ' ', "log_path",  log_path.display());
    tracing::info!("{:<w$}{:<w2$}{}", ' ', "log_level",  log_level);
    tracing::info!("{:<w$}{:<w2$}{}", ' ', "log_keep_days",  log_keep_days);
    tracing::info!("================================================================");
}

fn main() -> anyhow::Result<()> {
    let args = Args::init()?;
    set_env_plugins_home_dir(args.plugins_home.clone());
    set_env_data_dir(args.data_dir.clone());
    set_env_log_home_dir(args.logs_home.clone());
    set_env_log_keep_days(args.log_keep_days.clone());

    let mut log_path = get_log_dir("");
    log_path.push(LOG_FILE);

    let log_keep_days = get_log_keep_days();
    let rolling_file_appender = RollingFileAppender::builder()
        .max_log_files((log_keep_days + 1) as usize)
        .filename_prefix(LOG_FILE)
        .rotation(Rotation::DAILY)
        .build(get_log_dir(""))
        .expect("failed to initialize rolling file appender");
    let (non_blocking, _guard) = tracing_appender::non_blocking(rolling_file_appender);

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

    let log_level = args.log_level.unwrap_or(Level::INFO);
    let level_filter = tracing_subscriber::filter::LevelFilter::from_level(log_level);
    let mut layers = Vec::new();
    // Add layer for rotating logs
    layers.push(
        TaosXLayer::new()
            .with_writer(non_blocking)
            .with_filter(level_filter)
            .boxed(),
    );

    if atty::is(atty::Stream::Stdout) {
        cfg_if::cfg_if! {
            if #[cfg(windows)] {
               let ansi = true;
            } else {
               let ansi = true;
            }
        };
        layers.push(
            tracing_subscriber::fmt::layer()
                .with_timer(timer.clone())
                .with_level(true)
                .with_writer(std::io::stdout)
                .pretty()
                .with_ansi(ansi)
                .with_filter(level_filter)
                .boxed(),
        );
    }
    tracing_subscriber::registry().with(layers).init();

    let version = build::PKG_VERSION;
    let commit_id = build::COMMIT_HASH;
    let build_time = build::BUILD_TIME;
    tracing::info!("version: {version}");
    tracing::info!("commit id: {commit_id}");
    tracing::info!("build time: {build_time}");
    print_effictive_config(log_level, log_path, log_keep_days, &args);
    tracing::info!("Start");

    // todo: arrow flight rpc client.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(4096)
        .thread_name("taosx-agent")
        .enable_all()
        .build()?;

    rt.block_on(main_agent_service(args))?;
    rt.shutdown_timeout(Duration::from_secs(5));

    Ok(())
}
