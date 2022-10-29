use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use log::Level;
use pretty_env_logger::env_logger::fmt::{Color, StyledValue};
use const_format::concatcp;
use shadow_rs::shadow;

shadow!(build);

const CLAP_SHORT_VERSION: &str = concatcp!(build::PKG_VERSION, "-", build::SHORT_COMMIT, " (",build::BUILD_OS,  " ", build::COMMIT_DATE_3339, ")");

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

#[derive(Subcommand, Debug)]
enum Commands {
    Run(run::Cli),
    Serve(serve::Cli),
    #[clap(external_subcommand)]
    External(Vec<String>),
}

/// TDengine streaming data transfer tool.
///
/// Service mode:
///
/// $ taosx serve --help
///
/// Batch mode:
///
/// $ taosx run -f <FROM> -t <TO>
#[derive(Parser, Debug)]
#[clap(author, version = CLAP_SHORT_VERSION, about)]
struct Args {
    #[clap(flatten)]
    globals: GlobalOpts,
    #[clap(subcommand)]
    commands: Option<Commands>,
    #[clap(last = true, value_parser)]
    slop: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut builder = pretty_env_logger::formatted_timed_builder();
    builder.filter_level(args.globals.verbose.log_level_filter());
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
                        Level::Info => style.set_color(Color::Green).value("INFO"),
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
        .init();
    if let Some(cmd) = args.commands {
        match cmd {
            Commands::Run(cmd) => cmd.run_with(args.globals).await?,
            Commands::Serve(cli) => cli.run_with(args.globals).await?,
            Commands::External(_) => bail!("unknown subcommand"),
        }
    } else {
        //  service mode
        // dbg!(&args);
        serve::Cli::default().run_with(args.globals).await?;
    }
    Ok(())
}
