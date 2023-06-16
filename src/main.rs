use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use const_format::concatcp;
use log::Level;
use pretty_env_logger::env_logger::fmt::{Color, StyledValue};
use shadow_rs::shadow;

#[cfg(feature = "tikv_jemallocator")]
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(feature = "tikv_jemallocator")]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

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

    /// Be careful to use this, we suggest only use it when failed at first time.
    ///
    /// We'll warn you various kind of risks before really running a task.
    #[clap(short, long, global = true)]
    yes_i_really_mean_it: bool,
}

impl GlobalOpts {
    pub fn executor_worker_threads(&self) -> usize {
        let min = std::thread::available_parallelism()
            .map(|v| v.get() * 2)
            .unwrap_or(16)
            .max(8);

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
                let default = "/usr/local/taosX/plugins";
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

    let mut builder = pretty_env_logger::formatted_timed_builder();
    builder.filter_level(args.globals.verbose.log_level_filter());
    builder.filter_module("tokio", log::LevelFilter::Warn);
    builder.filter_module("tungstenite", log::LevelFilter::Warn);
    builder.filter_module("tokio_tungstenite", log::LevelFilter::Warn);
    builder.filter_module("mio", log::LevelFilter::Warn);
    let debug = args.globals.debug;
    builder
        .format_module_path(true)
        .format(
            move |buf, record| -> std::result::Result<(), std::io::Error> {
                fn colored_level<'a>(
                    style: &'a mut pretty_env_logger::env_logger::fmt::Style,
                    level: Level,
                ) -> StyledValue<'a, &'static str> {
                    match level {
                        Level::Trace => style.set_color(Color::Magenta).value("TRACE"),
                        Level::Debug => style.set_color(Color::Blue).value("DEBUG"),
                        Level::Info => style.set_color(Color::Green).value("INFO "),
                        Level::Warn => style.set_color(Color::Yellow).value("WARN "),
                        Level::Error => style.set_color(Color::Red).value("ERROR"),
                    }
                }
                let mut style = buf.style();
                let level = colored_level(&mut style, record.level());
                let mut mod_path = buf.style();

                let mod_path = if debug {
                    mod_path.set_bold(true).value(format!(
                        "{}:{}",
                        record.file().unwrap_or("unknown"),
                        record.line().unwrap_or(0),
                    ))
                } else {
                    mod_path
                        .set_bold(true)
                        .value(format!("{}", record.module_path().unwrap_or_default(),))
                };

                use std::io::Write;
                writeln!(
                    buf,
                    "[{:29}] {: <5} {} > {}",
                    chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                    level,
                    mod_path,
                    record.args()
                )
            },
        )
        .is_test(false)
        .init();

    let worker_threads = args.globals.executor_worker_threads();

    log::info!("version: {version}");
    log::info!("commit id: {commit_id}");
    log::info!("build time: {build_time}");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(4096)
        .thread_name("taosx")
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;
    if let Some(cmd) = args.commands {
        match cmd {
            Commands::Run(cmd) => runtime.block_on(cmd.run_with(args.globals))?,
            Commands::Serve(cli) => {
                // let rt = tokio::runtime::Builder::new_multi_thread()
                //     .max_blocking_threads(4096)
                //     .thread_name("runner")
                //     .worker_threads(worker_threads)
                //     .enable_all()
                //     .build()?;
                runtime.block_on(cli.run_with(args.globals, None))?;
            }
            Commands::External(_) => bail!("unknown subcommand"),
        }
    } else {
        // let rt = tokio::runtime::Builder::new_multi_thread()
        //     .max_blocking_threads(4096)
        //     .thread_name("runner")
        //     .worker_threads(worker_threads)
        //     .enable_all()
        //     .build()?;
        runtime.block_on(serve::Cli::default().run_with(args.globals, None))?;
    }
    Ok(())
}
