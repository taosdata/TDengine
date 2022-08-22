use anyhow::Result;
use taos::*;

use taosx::{local_to_taos, query_to_csv, query_to_parquet, tmq_to_local, tmq_to_td};

use clap::{Parser, Subcommand};

mod run;
mod serve;

#[derive(Debug, Parser, Clone)]
pub(crate) struct GlobalOpts {
    /// For verbosity print.
    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::WarnLevel>,

    /// Number of jobs, default to 0, will use `jobs` number of works for TMQ.
    #[clap(short, long, value_parser, default_value = "0")]
    jobs: usize,

    /// Be careful to use this, we suggest only use it when failed at first time.
    ///
    /// We'll warn you various kind of risks before really running a task.
    #[clap(short, long)]
    yes_i_really_mean_it: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Run(run::Cli),
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
/// $ taosx -d
///
/// One-shot mode:
///
/// $ taosx -f <FROM> -t <TO>
#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(flatten)]
    globals: GlobalOpts,
    #[clap(subcommand)]
    commands: Option<Commands>,
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
            _ => todo!(),
        }
    } else {
        //  service mode
        serve::Cli::default().run_with(args.globals).await?;
    }
    Ok(())
}
