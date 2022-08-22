use anyhow::Result;
use taos::*;

use taosx::{local_to_taos, query_to_csv, query_to_parquet, tmq_to_local, tmq_to_td};

use clap::Parser;

#[derive(Parser, Debug)]
pub(super) struct Cli {
    /// Input DSN(Data Source Name) string.
    ///
    /// Supported:
    ///
    /// ─ TMQ: TDengine message queue data stream, use as:
    ///  ** `tmq://host:port/topics?group.id=STR&client.id=STR&timeout`.
    ///
    /// ─ Legacy query, use as:
    ///
    /// └── a) database input: `taos://localhost:6030/database`, this will output stable schemas and child tables.
    ///
    /// └── b) table input: `taos://host:port/database?from=Stb1&select=c1,c2,c3`, this will be queried as:
    ///       'select c1,c2,c3 from `database`.'
    ///
    /// ─ Local backup, use as `local:./path`.
    ///
    /// ─ CSV: `csv:/path/to/file.csv`.
    ///
    /// ─ Parquet: `parquet:/path/to/*.parq`.
    ///
    #[clap(short, long, value_parser)]
    from: Dsn,

    /// Output DSN.
    #[clap(short, long, value_parser)]
    to: Dsn,

    // /// Algorithm
    // #[clap(short, long, value_enum, default_value = "zstd")]
    // #[doc(hidden)]
    // algorithm: Algorithm,
    /// For verbosity print.
    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::WarnLevel>,

    /// Number of jobs, default to 0, will use `jobs` number of works for TMQ.
    #[clap(short, long, value_parser, default_value = "0")]
    jobs: usize,

    /// When `endless` flag set, we'll re-write tmq timeout as `never` to wait messages
    /// without an ending, but it will still abort when there's error in the process.
    #[clap(short, long)]
    endless: bool,

    /// Override default TDengine connection protocol to websocket, both `from` and `to` will be affected.
    ///
    /// So that you don't need to append `+ws` in DSN.
    #[clap(short, long)]
    websocket: bool,
}

impl Cli {
    pub(super) async fn run_with(self, opts: super::GlobalOpts) -> Result<()> {
        let mut args = self;
        if args.websocket {
            if args.from.protocol.is_none() {
                args.from.protocol = Some("ws".to_string());
            }
            if args.to.protocol.is_none() {
                args.to.protocol = Some("ws".to_string());
            }
        }

        match (args.from.driver.as_str(), args.to.driver.as_str()) {
            ("tmq", "taos") => {
                tmq_to_td(args.from, args.to, args.jobs).await?;
            }
            ("tmq", "local") => {
                tmq_to_local(args.from, args.to, args.jobs, opts.yes_i_really_mean_it).await?;
            }
            ("local", "taos") => {
                local_to_taos(args.from, args.to, args.jobs, opts.yes_i_really_mean_it).await?;
            }
            ("taos", "csv") => {
                query_to_csv(args.from, args.to).await?;
            }
            ("taos", "parquet") => {
                query_to_parquet(args.from, args.to, opts.yes_i_really_mean_it).await?;
            }
            // ("tmq", "csv") => {
            //     // tmq table to csv, write table records to csv format.
            //     todo!()
            // }
            // ("tmq", "parquet") => {
            //     // tmq table to parquet
            //     todo!()
            // }
            // ("csv", "taos") => {
            //     // CSV to TDengine
            //     todo!()
            // }
            // ("parquet", "taos") => {
            //     // parquet to TDengine
            //     todo!()
            // }
            // ("taos", "local") => {
            //     todo!()
            // }
            (_, _) => panic!(
                "unsupported source or dest: from {} to {}",
                args.from, args.to
            ),
        }

        Ok(())
    }
}
