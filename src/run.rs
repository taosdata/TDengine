use crate::serve::check_parser_timestamp_precision;
use anyhow::{bail, Result};
use clap::Parser;
use taos::*;
use taosx_core::utils::{self};
use taosx_core::Action;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

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
    /// └── b) table input: `taos://host:port/db?query=select c1,c2,c3 from stb1`, this will be queried by sql `select c1,c2,c3 from stb1` and output as a plain table.
    ///
    /// ─ Local backup, use as `local:./path`.
    ///
    /// ─ CSV: `csv:/path/to/file.csv`.
    ///
    /// ─ Parquet: `parquet:/path/to/*.parquet`.
    ///
    #[clap(short, long, value_parser)]
    from: Dsn,

    /// Output DSN.
    #[clap(short, long, value_parser)]
    to: Dsn,

    /// Parser.
    #[clap(short, long)]
    parser: Option<String>,
    // parser: Option<taosx_core::Parser>,
    /// Transformer actions.
    ///
    /// Supported action format:
    ///
    /// - 'add-tag:tag1=value1': add a tag named `tag1`, and valued `value1`.
    ///
    /// - 'rename-table:prefix:v1_': rename all tables as `v1_{{ name }}`
    ///
    /// - 'rename-super-table:suffix:_stb': rename all super tables as suffixed '_stb'
    ///
    /// - 'rename-child-table:template:prefix_{{ name }}_stb': rename all super tables with prefix 'prefix_' and suffix '_stb'
    ///
    /// - 'rename-replace-with-regex:replace_with_regex:prefix(?<old>)::newprefix_$old': replace all tables prefix with new prefix
    #[clap(short = 'T', long)]
    transform: Vec<Action>,

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
    #[tracing::instrument(skip(self, opts), name = "cli")]
    pub(super) async fn run_with(self, opts: super::GlobalOpts) -> Result<()> {
        // let _ = span.entered();
        tracing::info!("start cli");
        let args = self;
        let parser = args.parser.as_ref().map(|p| {
            let content = utils::get_string_content_from_file_path(p);
            let content = content.is_none().then(|| p.clone()).or(content);
            let content = content.map(|p| serde_json::from_str(&p)).unwrap();
            content
        });
        let parser = if parser.is_some() {
            parser.unwrap().map_err(|_err| anyhow::Error::msg(format!("parser config should be a valid json or a file path with '@' as prefix and a valid json content")))?
        } else {
            None
        };
        // validate parser
        if let Some(parser) = args.parser.as_ref() {
            if !check_parser_timestamp_precision(&parser) {
                bail!("parser should have same timestamp precision");
            }
        }
        let cancel = CancellationToken::new();
        let span = tracing::Span::current();
        // let _ = span.clone().entered();
        // let _ = span.enter();
        let task_opt = taosx_core::TaskOpts {
            from: args.from,
            transform: args.transform,
            to: args.to,
            parser: parser,
            jobs: args.jobs,
            compression_level: None,
            force: opts.yes_i_really_mean_it,
            cancel: cancel.clone(),
            with_agent: None,
            offsets: Default::default(),
            transferred: Default::default(),
            span: span.clone(),
            task_id: None,
        };
        let port_pool = Default::default();
        tokio::select! {
            res = task_opt.run(&port_pool).in_current_span() => {
                res?;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("ctrl-c received, exiting...");
                cancel.cancel();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }

        Ok(())
    }
}
