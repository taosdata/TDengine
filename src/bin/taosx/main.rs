use std::ffi::OsString;

use anyhow::Result;
use taos::*;

use clap::{Parser, Subcommand};

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

// #[derive(clap::ValueEnum, Clone, Debug)]
// enum Algorithm {
//     Brotli,
//     Bzip2,
//     Deflate,
//     Gzip,
//     Lzma,
//     Xz,
//     Zlib,
//     Zstd,
// }

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
#[clap(author, version, about)]
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

    pretty_env_logger::formatted_builder()
        .filter_level(args.globals.verbose.log_level_filter())
        .init();

    if let Some(cmd) = args.commands {
        match cmd {
            Commands::Run(cmd) => cmd.run_with(args.globals).await?,
            Commands::Serve(cli) => cli.run_with(args.globals).await?,
            Commands::External(cli) => todo!(),
        }
    } else {
        //  service mode
        dbg!(&args);
        serve::Cli::default().run_with(args.globals).await?;
    }
    Ok(())
}
