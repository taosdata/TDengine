use anyhow::{bail, Result};
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use taos::*;
use taosx_core::core_metrics::init_task_metrics;
use taosx_core::utils::license::validate_enterprise_license;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use twelf::config;

use taosx_core::utils::{self};
use taosx_core::Action;

use crate::serve::check_parser_timestamp_precision;

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
    /// - 'rename-child-table:map:oldname1,newname1|oldname2::newname2': rename all child tables with oldname1 to newname1, oldname2 to newname2
    ///
    /// - 'rename-child-table:map:@./rename-old-new.csv': rename all child tables with oldname,newname pairs in csv file
    ///
    /// - 'rename-replace-with-regex:replace_with_regex:prefix(?<old>)::newprefix_$old': replace all tables prefix with new prefix
    #[clap(short = 'T', long)]
    transform: Vec<Action>,

    #[clap(flatten)]
    config_args: ConfigArgs,

    #[clap(flatten)]
    pub verbose: Option<Verbosity<InfoLevel>>,

    /// Task id, default is -1.
    #[clap(long, hide = true)]
    task_id: Option<i64>,
}

#[config]
#[derive(Parser, Debug)]
struct ConfigArgs {
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
    #[tracing::instrument(skip(self, opt_args, config_args), name = "cli")]
    pub(super) async fn run_with(
        self,
        opt_args: super::OptArgs,
        config_args: super::Global,
    ) -> Result<()> {
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
        // let span = tracing::info_span!("cli");
        let cancel = CancellationToken::new();

        let (notify, receiver) = flume::unbounded();

        tokio::spawn(async move {
            while let Ok(notify) = receiver.recv_async().await {
                match notify {
                    taosx_core::TaskNotify::Info(info) => {
                        tracing::info!("{}", info);
                    }
                    taosx_core::TaskNotify::Error(error) => {
                        tracing::error!("{}", error);
                    }
                    taosx_core::TaskNotify::Warn(warn) => {
                        tracing::warn!("{}", warn);
                    }
                    _ => {}
                }
            }
        });
        #[cfg(not(feature = "disable-enterprise-only-validation"))]
        {
            validate_enterprise_license(&args.from, &args.to).await?;
        }
        // let _ = span.clone().entered();
        // let _ = span.enter();
        let task_opt = taosx_core::TaskOpts {
            from: args.from,
            transform: args.transform,
            to: args.to,
            parser,
            jobs: config_args.jobs,
            compression_level: None,
            force: opt_args.yes_i_really_mean_it,
            cancel: cancel.clone(),
            with_agent: None,
            breakpoints: None,
            transferred: Default::default(),
            task_id: args.task_id.clone().map(|v| v.to_string()),
            notify,
        };

        // start metrics print schedular
        let debugging_recorder = metrics_util::debugging::DebuggingRecorder::new();
        debugging_recorder.install()?;

        let timer_run = Arc::new(AtomicBool::new(true));
        let _metrics = init_task_metrics(
            &task_opt.from,
            &task_opt.to,
            args.task_id.unwrap_or(-1),
            None,
        )
        .await;
        let port_pool = Default::default();
        let task = tokio::spawn(
            async move { task_opt.run(&port_pool).in_current_span().await }.in_current_span(),
        );
        tokio::pin!(task);
        tokio::select! {
            res = &mut task => {
                res??;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("ctrl-c received, exiting...");
                cancel.cancel();
                match tokio::time::timeout(std::time::Duration::from_secs(5), task).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        tracing::error!("task error: {:?}", e);
                    }
                    Err(_) => {
                        tracing::error!("task timeout after 5s, force exit...");
                    }
                }
            }
        }
        timer_run.store(false, Ordering::SeqCst);
        Ok(())
    }
}
