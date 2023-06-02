use std::{path::PathBuf, time::Duration};

use clap::{CommandFactory, Parser};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use thiserror::Error;
use tracing_subscriber::fmt::{
    format::FmtSpan, 
    time::LocalTime
};
use time::macros::format_description;
use twelf::{config, Layer};

use tracing::{log::LevelFilter, Level};

use taosx_core::get_log_dir;

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
    /// Listen to ip:port address.
    endpoint: String,

    token: String,

    log_level: Option<Level>,
}
#[config]
#[derive(Parser, Debug)]
#[clap(
    name = build::CUS_CLI_NAME,
    author, version = build::VERBOSE_VERSION,
    about = build::CUS_CLI_ABOUT,
    long_about = build::CUS_CLI_ABOUT)]
pub struct ArgsParser {
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
    #[clap(hide = true)]
    log_level: Option<LevelFilter>,
}

#[derive(Parser, Debug)]
pub struct Config {
    /// Config file.
    #[clap(short = 'c', long)]
    config: Option<PathBuf>,

    #[clap(flatten)]
    args: ArgsParser,
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
        let path = if let Ok(c) = Config::try_parse() {
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
                    .join(build::CUS_NAME)
                    .join("cfg")
                    .join("agent.toml")
            } else {
                std::path::Path::new("/etc")
                    .join(build::CUS_PROMPT)
                    .join("agent.toml")
            }
        });

        let matches = Config::command().get_matches();

        let mut layers = vec![];

        if path.exists() {
            layers.push(Layer::Toml(path))
        }
        layers.push(Layer::Env(Some(format!(
            "{}X_AGENT_",
            build::CUS_PROMPT.to_uppercase()
        ))));
        layers.push(Layer::Clap(matches));

        let ArgsParser {
            endpoint,
            token,
            log_level,
            verbose,
            ..
        } = ArgsParser::with_layers(&layers)?;
        let log_level = log_level_to_tracing_level(
            log_level
                .clone()
                .or(verbose.clone().map(|v| v.log_level_filter()))
                .unwrap_or(log::LevelFilter::Info),
        );
        Ok(Args {
            endpoint: endpoint
                .ok_or_else(|| ArgsError::MissingRequiredArgument("endpoint".to_string()))?,
            token: token.ok_or_else(|| ArgsError::MissingRequiredArgument("token".to_string()))?,
            log_level,
        })
    }
}

mod agent;
mod runner;

async fn main_agent_service(args: Args) -> anyhow::Result<()> {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut client = agent::Client::new(&args.endpoint, &args.token).await?;
    let mut client2 = agent::Client::new(&args.endpoint, &args.token).await?;
    let (runner, sender, status) = runner::spawn_runner(&args.endpoint, &args.token);

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("SIGINT triggered");
        }
        _ = runner => {
            tracing::info!("Runner stopped");
        }
        _ = async {
            loop {
                let sender = sender.clone();
                if let Err(err) = client.wait_tasks(sender).await {
                    tracing::error!("Connection closed, error: {err}. Retry in 5 seconds");
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            // Ok::<_, anyhow::Error>(())
         } => {
            tracing::info!("Task listener stopped");
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
        "Serve agent with endpoint: {} via token {}",
        args.endpoint, args.token
    );

    let log_dir = get_log_dir("agent");

    let file_appender = tracing_appender::rolling::daily(
        log_dir, 
        LOG_FILE
    );
    
    let (
        non_blocking, 
        _guard
    ) = tracing_appender::non_blocking(
        file_appender
    );

    let timer = LocalTime::new(
        format_description!(
            "[month]/[day] [hour]:[minute]:[second].[subsecond digits:6]"
        )
    );

    let subscriber = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::ACTIVE)
        .with_max_level(args.log_level)
        .with_timer(timer)
        .with_writer(non_blocking)
        .compact();
    // if atty::is(atty::Stream::Stdout) {
    //     subscriber.pretty().init();
    // } else {
    //     subscriber.with_ansi(false).init();
    // }
    subscriber.with_ansi(false).init();

    log::info!("Start");

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
