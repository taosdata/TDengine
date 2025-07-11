use std::{cell::UnsafeCell, collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use taos::*;
use tokio::{runtime::Runtime, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use anyhow::Result;

use crate::{LegacyMetrics, TargetOpts, TimeRange};

use super::{scheduler::Scheduler, sync_single_table, TableRecord};

pub struct TablesHandle {
    source: TaosPool,
    target: TaosPool,
    target_opts: TargetOpts,
    target_is_v3: bool,
    opts: TableOpts,
    handles: HashMap<String, JoinHandle<Result<()>>>,
    cancellation: Option<CancellationToken>,
    metrics: Arc<LegacyMetrics>,
    scheduler: Arc<Scheduler>,
    // sender: tokio::sync::mpsc::UnboundedSender<(String, TimeRange)>,
    // worker: Option<WorkerHandler>,
}

impl Drop for TablesHandle {
    fn drop(&mut self) {
        self.cancellation.take().map(CancellationToken::drop_guard);
    }
}

async fn process_sync_with(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<(String, TimeRange)>,
    source: TaosPool,
    target: TaosPool,
    target_opts: TargetOpts,
    target_is_v3: bool,
    cancellation: CancellationToken,
    metrics: &LegacyMetrics,
) -> anyhow::Result<()> {
    // let start = now - chrono::Duration::from_std(opts_cloned.restro)?;
    let from = source.get()?;
    let to = target.get()?;

    // let target_is_v3 = self.target_is_v3;
    // let table = self.table.clone();
    // let target_opts = self.target_opts.clone();
    // tracing::debug!("spawn sync task for range: {:?}", opts.time_range);
    // let h = tokio::spawn(async move {
    loop {
        let (table, time_range) = receiver.recv().await.unwrap();

        let opts = crate::QueryOpts {
            time_range,
            ..Default::default()
        };
        sync_single_table(
            &from,
            None, // todo
            &table,
            &to,
            &opts,
            &target_opts,
            target_is_v3,
            metrics,
        )
        .await?;
    }
    Ok(())
}

type WorkerHandler = JoinHandle<()>;
impl TablesHandle {
    pub async fn new(
        scheduler: Scheduler,
        source: TaosPool,
        target: TaosPool,
        opts: TableOpts,
        target_opts: TargetOpts,
        metrics: Arc<LegacyMetrics>,
    ) -> Result<Self> {
        // let source = source.pool()?;
        // let target = target.pool()?;
        let version: String = target
            .get()?
            .query_one("SELECT server_version()")
            .await?
            .unwrap();
        let target_is_v3 = version.starts_with("3");
        let token = CancellationToken::new();
        // let (sender, todo) = tokio::sync::mpsc::unbounded_channel();

        // // let opts_cloned = opts.clone();
        // let (source2, target2) = (source.clone(), target.clone());
        // let target_opts_cloned = target_opts.clone();
        // let token_cloned = token.clone();
        // let sender_cloned = sender.clone();
        // let metrics_cloned = metrics.clone();
        // let worker = tokio::spawn(async move {
        //     if let Err(err) = process_sync_with(
        //         todo,
        //         source2,
        //         target2,
        //         target_opts_cloned,
        //         target_is_v3,
        //         token_cloned,
        //         &metrics_cloned,
        //     )
        //     .await
        //     {
        //         tracing::warn!("syncing error: {err:?}");
        //     }
        //     let _ = sender_cloned;
        // });
        // let runtime = Runtime::new()?;
        Ok(Self {
            source,
            target,
            target_opts,
            target_is_v3,
            opts,
            metrics,
            // sender,
            scheduler: Arc::new(scheduler),
            handles: Default::default(),
            cancellation: Some(token),
            // worker: Some(worker),
        })
    }
    fn push_table(&mut self, table: String) -> Result<()> {
        let mut handle = TableHandler {
            source: self.source.clone(),
            target: self.target.clone(),
            target_opts: self.target_opts.clone(),
            target_is_v3: self.target_is_v3,
            opts: self.opts.clone(),
            table: table.clone(),
            handles: Default::default(),
            cancellation: CancellationToken::new(),
            metrics: self.metrics.clone(),
        };
        // handle.run().await;
        let handle = tokio::spawn(async move { handle.run().await });
        self.handles.insert(table, handle);
        Ok(())
    }

    pub async fn spawn(&mut self) -> Result<()> {
        let from = self.source.get()?;

        let v1: String = from.query_one("SELECT server_version()").await?.unwrap();

        let tables: Vec<_> = from
            .query("SHOW TABLES")
            .await?
            .deserialize::<String>()
            .try_collect()
            .await?;
        // let sender = self.sender.clone();
        let opts = self.opts.clone();
        let h = tokio::spawn(async move {
            let mut now = Utc::now();
            let excursion = chrono::Duration::from_std(opts.excursion)?;
            if !opts.excursion.is_zero() {
                now -= excursion;
            }
            // now is the separator of history and future data.

            // check if need retro back.
            if opts.restro.is_zero() {
                // trace back to some duration.
                let start = now - chrono::Duration::from_std(opts.restro)?;
                let time_range = TimeRange::new().start(start).end(now);

                for table in &tables {
                    sender
                        .send_async((table.clone(), time_range.clone()))
                        .await
                        .unwrap();
                }
            }

            let tick_duration = chrono::Duration::from_std(opts.interval)?;
            let mut interval = tokio::time::interval(opts.interval);
            interval.tick().await;
            let mut start = now;
            loop {
                let end = Utc::now() - tick_duration;
                let time_range = TimeRange::new().start(start).end(end);
                tracing::debug!("spawn sync task for range: {:?}", time_range);
                for table in &tables {
                    sender
                        .send_async((table.clone(), time_range.clone()))
                        .await
                        .unwrap();
                }
                start = end;
                let _ = interval.tick().await;
            }
            Ok::<(), anyhow::Error>(())
        });
        Ok(())
    }
    pub async fn join(&mut self) -> Result<()> {
        // if let Some(worker) = self.worker.take() {
        //     worker.await;
        // }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TableOpts {
    /// A retrospective duration to sync.
    pub(super) restro: Duration,
    /// An internal to add a task.
    pub(super) interval: Duration,
    /// Time duration for possible server/client clock excursion.
    pub(super) excursion: Duration,
}

impl Default for TableOpts {
    fn default() -> Self {
        Self {
            restro: Duration::ZERO,
            interval: Duration::from_secs(1),
            excursion: Duration::from_millis(500),
        }
    }
}

impl TableOpts {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn restro(&mut self, value: Duration) -> &mut Self {
        self.restro = value;
        self
    }
    pub fn interval(&mut self, value: Duration) -> &mut Self {
        self.interval = value;
        self
    }
    pub fn excursion(&mut self, value: Duration) -> &mut Self {
        self.excursion = value;
        self
    }

    pub fn from_params(dsn: &mut Dsn) -> Result<Self> {
        let mut opts = Self::new();
        if let Some(value) = dsn.remove("restro") {
            opts.restro = utils::parse_duration(&value)?;
        }
        if let Some(value) = dsn.remove("interval") {
            opts.interval = utils::parse_duration(&value)?;
        }
        if let Some(value) = dsn.remove("excursion") {
            opts.excursion = utils::parse_duration(&value)?;
        }
        Ok(opts)
    }
}

pub struct TableHandler {
    source: TaosPool,
    target: TaosPool,
    target_opts: TargetOpts,
    target_is_v3: bool,
    table: String,
    opts: TableOpts,
    handles: Vec<JoinHandle<Result<()>>>,
    metrics: Arc<LegacyMetrics>,
    cancellation: CancellationToken,
}

impl TableHandler {
    pub async fn run(&mut self) -> Result<()> {
        let mut now = Utc::now();
        let excursion = chrono::Duration::from_std(self.opts.excursion)?;
        if !self.opts.excursion.is_zero() {
            now -= excursion;
        }
        // now is the separator of history and future data.

        // check if need retro back.
        if self.opts.restro.is_zero() {
            // trace back to some duration.
            let start = now - chrono::Duration::from_std(self.opts.restro)?;
            let from = self.source.get()?;
            let to = self.target.get()?;

            let opts = crate::QueryOpts {
                time_range: TimeRange::new().start(start).end(now),
                ..Default::default()
            };
            let target_is_v3 = self.target_is_v3;
            let table = self.table.clone();
            let target_opts = self.target_opts.clone();
            tracing::debug!("spawn sync task for range: {:?}", opts.time_range);
            let metrics = self.metrics.clone();
            let h = tokio::spawn(async move {
                sync_single_table(
                    &from,
                    None,
                    &table,
                    &to,
                    &opts,
                    &target_opts,
                    target_is_v3,
                    &metrics,
                )
                .await
            });
            self.handles.push(h);
            // let h = tokio::spawn(async move {
            // sync_single_table(
            //     &from,
            //     None,
            //     &table,
            //     &to,
            //     &opts,
            //     &target_opts,
            //     target_is_v3,
            //     &metrics,
            // )
            // .await;
            // });
            // self.handles.push(h);
        }

        let tick_duration = chrono::Duration::from_std(self.opts.interval)?;
        let mut interval = tokio::time::interval(self.opts.interval);
        interval.tick().await;
        let mut start = now;
        loop {
            let _ = interval.tick().await;
            let end = Utc::now() - tick_duration;
            let target_is_v3 = self.target_is_v3;
            let table = self.table.clone();
            let from = self.source.get()?;
            let to = self.target.get()?;
            let opts = crate::QueryOpts {
                time_range: TimeRange::new().start(start).end(end),
                ..Default::default()
            };
            tracing::debug!("spawn sync task for range: {:?}", opts.time_range);
            let target_opts = self.target_opts.clone();
            let metrics = self.metrics.clone();
            let h = tokio::spawn(async move {
                sync_single_table(
                    &from,
                    None,
                    &table,
                    &to,
                    &opts,
                    &target_opts,
                    target_is_v3,
                    &metrics,
                )
                .await
            });
            self.handles.push(h);
            // sync_single_table(
            //     &from,
            //     None,
            //     &table,
            //     &to,
            //     &opts,
            //     &target_opts,
            //     target_is_v3,
            //     &metrics,
            // )
            // .await;
            // let h = tokio::spawn(async move {
            //     sync_single_table(&from, &table, &to, &opts, &target_opts, target_is_v3).await
            // });
            // self.handles.push(h);
            start = end;
        }
    }

    pub async fn join(&mut self) -> Result<()> {
        for h in &mut self.handles {
            h.await??;
        }
        Ok(())
    }

    pub async fn abort(&mut self) -> Result<()> {
        for h in &mut self.handles {
            h.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use taos::{AsyncQueryable, TBuilder};

    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn all_tables() -> Result<()> {
        pretty_env_logger::formatted_timed_builder()
            .filter_level(log::LevelFilter::Debug)
            .init();
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop database if exists ts2031f",
            "create database ts2031f",
            "create table ts2031f.ntb1 (ts timestamp, v1 int)",
            "drop database if exists ts2031t",
            "create database ts2031t",
            "create table ts2031t.ntb1 (ts timestamp, v1 int)",
        ])
        .await?;
        let source = TaosBuilder::from_dsn("taos:///ts2031f")?;
        let target = TaosBuilder::from_dsn("taos:///ts2031t")?;
        let target_opts = TargetOpts::default();
        let mut opts = TableOpts::new();
        opts.restro(Duration::from_secs(60))
            .excursion(Duration::from_secs(2));

        let mut tables_handle = TablesHandle::new(
            source.pool()?,
            target.pool()?,
            opts,
            target_opts,
            Arc::new(Default::default()),
        )
        .await?;
        tables_handle.spawn().await?;

        let sleep = tokio::time::sleep(Duration::from_secs(10));

        tokio::select! {
            _ = sleep => {
                tracing::warn!("timer elapsed");
            }
            _ = tables_handle.join() => {
            },
            _ = async {
                loop {
                    taos.exec("insert into ts2031f.ntb1 values(now, 1)").await.unwrap();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } => {}
        }
        // table_task.join().await?;

        let wrote: u32 = taos
            .query_one("select count(*) from ts2031t.ntb1")
            .await?
            .unwrap();
        tracing::info!("we've synced {wrote} records");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn child_table() -> Result<()> {
        pretty_env_logger::formatted_timed_builder()
            .filter_level(log::LevelFilter::Debug)
            .init();
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop database if exists ts2031f",
            "create database ts2031f",
            "create table ts2031f.ntb1 (ts timestamp, v1 int)",
            "drop database if exists ts2031t",
            "create database ts2031t",
            "create table ts2031t.ntb1 (ts timestamp, v1 int)",
        ])
        .await?;
        let source = TaosBuilder::from_dsn("taos:///ts2031f")?.pool()?;
        let target = TaosBuilder::from_dsn("taos:///ts2031t")?.pool()?;
        let mut opts = TableOpts::new();
        let target_opts = TargetOpts::default();
        opts.restro(Duration::from_secs(60))
            .excursion(Duration::from_secs(2));

        let mut table_task = TableHandler {
            source,
            target,
            target_opts,
            target_is_v3: true,
            table: "ntb1".to_string(),
            opts,
            handles: Default::default(),
            cancellation: CancellationToken::new(),
            metrics: Default::default(),
        };

        let sleep = tokio::time::sleep(Duration::from_secs(10));

        tokio::select! {
            _ = sleep => {
                tracing::warn!("timer elapsed");
            }
            _ = table_task.run() => {
            },
            _ = async {
                loop {
                    taos.exec("insert into ts2031f.ntb1 values(now, 1)").await.unwrap();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } => {}
        }
        table_task.join().await?;

        let wrote: u32 = taos
            .query_one("select count(*) from ts2031t.ntb1")
            .await?
            .unwrap();
        tracing::info!("we've synced {wrote} records");
        Ok(())
    }
}
