use anyhow::Result;
use clap::Args;
use futures::prelude::*;
use taos::query::Dsn;
use taosx::plugins::sink::taos::TaosSinkBuilder;
use taosx::plugins::source::taos::TaosSourceBuilder;
use taosx::stream::source::XSourceBuilder;
use taosx::stream::stream::XSinkBuilder;

use taosx::TaosOpts;

#[derive(Debug, Args)]
/// Import external files to TDengine.
pub(crate) struct App {
    #[clap(short, long)]
    /// A DSN(database source name) format string for source TDengine: taos:///db1, for eg.
    from: Dsn,
    #[clap(short, long)]
    /// A DSN(database source name) format string for target TDengine: taos:///db2, for eg.
    to: Dsn,
    /// Number of workers for TMQ consumers.
    #[clap(short = 'j', long)]
    workers: Option<usize>,
}

impl App {
    pub async fn run_with_taos_opts(self, _opts: &TaosOpts) -> Result<()> {
        log::debug!("app: {self:?}");

        let mut source_builder = TaosSourceBuilder::from_dsn(self.from)?;
        let max_workers = source_builder.max_workers();

        let sink_builder = TaosSinkBuilder::from_dsn(self.to)?;

        let mut workers = self.workers.unwrap_or(max_workers);
        if max_workers != 0 && workers > max_workers {
            log::warn!("maximum workers for the stream is {max_workers} while you want {workers}, reduce to limit");
            workers = max_workers;
        }
        if workers == 0 {
            workers = 1;
        }
        let mut handlers = Vec::new();
        log::info!("use {workers} workers (max: {max_workers})");

        for _ in 0..workers {
            let source = source_builder.build_source()?;
            let sink = sink_builder.build_sink()?;
            handlers.push(tokio::spawn(async move { source.forward(sink).await }));
        }

        for hd in handlers {
            hd.await??;
        }

        let summary = sink_builder.summary();

        log::info!(
            "Summary: total synced {} blocks with {} rows",
            summary.blocks(),
            summary.rows()
        );
        Ok(())
    }
}
