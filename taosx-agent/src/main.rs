use std::{path::PathBuf, time::Duration};

use chrono::{Local, Utc};
use clap::{CommandFactory, Parser};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use thiserror::Error;

use file_rotate::{
    compression::Compression,
    suffix::{AppendTimestamp, DateFrom, FileLimit},
    ContentLimit, FileRotate, TimeFrequency,
};
use time::macros::format_description;
use time::UtcOffset;
use tracing_subscriber::{
    fmt::time::OffsetTime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    Layer as _,
};
use twelf::{config, Layer};

use taosx_core::utils::trace::TaosXLayer;
use taosx_core::{
    get_log_dir, get_log_keep_days, set_env_data_dir, set_env_log_home_dir, set_env_log_keep_days,
    set_env_plugins_home_dir, Activity, RespAction,
};
use tracing::{log::LevelFilter, Level};

const LOG_FILE: &str = "agent.log";

shadow_rs::shadow!(build);

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
    author, version = build::VERBOSE_VERSION,
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
                if let Err(err) = client.wait_tasks(sender, resp_tx.clone(), resp_rx.clone()).await {
                    let err_str = format!("{err:#}");
                    if err_str.contains("code: Aborted") {
                        tracing::info!("Connection aborted, error: {err:?}");
                        ret = Err(err);
                        break;
                    } else {
                        tracing::error!("Connection closed, error: {err:?}. Retry in 5 seconds");
                    }
                    // tracing::error!("Connection closed, error: {err}. Retry in 5 seconds");
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

fn main() -> anyhow::Result<()> {
    let args = Args::init()?;
    println!(
        "Serve agent with endpoint: {} via token ******",
        args.endpoint
    );
    set_env_plugins_home_dir(args.plugins_home.clone());
    set_env_data_dir(args.data_dir.clone());
    set_env_log_home_dir(args.logs_home.clone());
    set_env_log_keep_days(args.log_keep_days.clone());

    let mut log_path = get_log_dir("");

    log_path.push(LOG_FILE);

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
        Compression::OnRotate(2),
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

    let level_filter =
        tracing_subscriber::filter::LevelFilter::from_level(args.log_level.unwrap_or(Level::INFO));

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
               let ansi = false;
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

    tracing::info!("log keep days: {}", &log_keep_days);

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
