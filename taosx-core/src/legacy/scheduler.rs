use std::{
    collections::HashMap,
    fmt::Debug,
    io::Write,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use anyhow::Context as _;
use chrono::{DateTime, Utc};
use flume::{Receiver, Sender};
use futures::FutureExt;
use itertools::Itertools;
use metrics::counter;
use taos::{AsyncQueryable, Taos, TaosPool};
use tokio::{
    sync::oneshot,
    task::{JoinError, JoinHandle},
};
use tracing::{instrument, Instrument};

use crate::{
    legacy::{
        split_table_into_time_range_chunks, sync_single_table_partial, sync_super_table_schema,
        transform_sql_with_remap,
    },
    utils::{breakpoints, metrics_db::MetricsDb},
    Action, LegacyMetrics, QueryOpts, TargetOpts, TimeRange, METRICS_LEGACY_CREATED_TABLES,
    METRICS_LEGACY_TABLES,
};

use super::{sync_normal_table_schema, sync_super_table_schema_with_subs};

pub enum Todo {
    STable(Arc<String>, oneshot::Sender<anyhow::Result<()>>),
    Meta(
        Option<Arc<String>>,
        Vec<String>,
        Option<oneshot::Sender<anyhow::Result<()>>>,
    ),
    Data(
        Option<Arc<String>>,
        Arc<String>,
        TimeRange,
        Option<oneshot::Sender<anyhow::Result<()>>>,
    ),
}

/// Legacy table synchronization scheduler.
pub struct Scheduler {
    workers: u32,
    #[allow(dead_code)]
    source: TaosPool,
    #[allow(dead_code)]
    target: TaosPool,
    opts: Arc<TargetOpts>,
    sender: Sender<Todo>,
    receiver: Receiver<Todo>,
    handles: Vec<JoinHandle<anyhow::Result<()>>>,
    #[allow(dead_code)]
    task_id: Option<String>,
}

impl Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("workers", &self.workers)
            .field("source", &"..")
            .field("target", &"..")
            .field("opts", &self.opts)
            .field("sender", &self.sender)
            .field("receiver", &self.receiver)
            .field("handles", &self.handles)
            .finish()
    }
}

#[instrument(skip_all)]
async fn worker(
    worker: u32,
    source: TaosPool,
    target: TaosPool,
    receiver: Receiver<Todo>,
    query: Arc<QueryOpts>,
    opts: Arc<TargetOpts>,
    metrics: Arc<LegacyMetrics>,
    metrics_db: Option<Arc<MetricsDb>>,
    actions: Vec<Action>,
    source_is_v3: bool,
    target_is_v3: bool,
    task_id: Option<String>,
    file_mutex: Arc<std::sync::Mutex<()>>,
) -> anyhow::Result<()> {
    const MAX_WS_RETRIES: usize = 5;
    let mut from = source.get().await?;
    let mut to = target.get().await?;
    from.exec("select 1")
        .await
        .map_err(|err| anyhow::format_err!("check source connection error: {err:?}"))?;
    to.exec("select 1")
        .await
        .map_err(|err| anyhow::format_err!("check target connection error: {err:?}"))?;
    let smooth_fold = (worker as f64 + 1.0).log2() as u32;
    tokio::time::sleep(query.smooth_init * smooth_fold).await;
    loop {
        let todo = receiver.recv_async().await?;
        match todo {
            Todo::STable(stable, sender) => {
                let mut retries = MAX_WS_RETRIES;
                let remap = opts
                    .remap
                    .as_ref()
                    .and_then(|v| v.get(stable.as_str()).map(Clone::clone));
                loop {
                    match sync_super_table_schema(
                        &from,
                        &stable,
                        &to,
                        remap.as_ref(),
                        &opts,
                        &actions,
                    )
                    .await
                    {
                        Ok(_) => {
                            let _ = sender.send(Ok(()));
                            break;
                        }
                        Err(err) => {
                            tracing::warn!("sync STable schema {stable} err: {err:#}");
                            let err_string = err.to_string();
                            if (err_string.contains("0xE00")
                                || err_string.contains("channel closed"))
                                && retries > 0
                            {
                                from = source.get().await?;
                                to = target.get().await?;
                                retries -= 1;
                                tracing::warn!(
                                            "[worker:{worker}] sync stable {stable} error: {err}, retrying ... {retries} times left"
                                        );
                                // wait 5 seconds to avoid too many retries
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                continue;
                            }

                            tracing::error!(
                                        "[worker:{worker}] sync stable schema {stable} error: {err:#}, continue next"
                                    );

                            if let Some(path) = opts.fails_to.as_ref() {
                                path.lock().unwrap().write_fmt(format_args!(
                                    "meta\t{}:\t\t{}\n",
                                    stable.as_str(),
                                    format!("{err:#}").replace("\n", " ")
                                ))?;
                            } else {
                                println!(
                                    "meta\t{}:\t\t{}",
                                    stable.as_str(),
                                    format!("{err:#}").replace("\n", " ")
                                );
                            }

                            let _ = sender.send(Err(err));
                            break;
                        }
                    }
                }
            }
            Todo::Meta(stable, tables, sender) => {
                match stable {
                    Some(stable) => {
                        let mut retries = MAX_WS_RETRIES;
                        let remap = opts
                            .remap
                            .as_ref()
                            .and_then(|v| v.get(stable.as_str()).map(Clone::clone));
                        loop {
                            //todo
                            match sync_super_table_schema_with_subs(
                                &from,
                                &stable,
                                &tables,
                                &to,
                                remap.as_ref(),
                                &opts,
                                source_is_v3,
                                &actions,
                                &metrics,
                            )
                            .await
                            {
                                Ok(_) => {
                                    if let Some(sender) = sender {
                                        let _ = sender.send(Ok(()));
                                    }
                                    break;
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        "sync_super_table_schema_with_subs {stable} err: {err:#}"
                                    );
                                    let table_count = tables.len();
                                    let err_string = err.to_string();
                                    if (err_string.contains("0xE00")
                                        || err_string.contains("channel closed"))
                                        && retries > 0
                                    {
                                        from = source.get().await?;
                                        to = target.get().await?;
                                        retries -= 1;
                                        tracing::warn!(
                                            "[worker:{worker}] sync stable {stable} error: {err}, retrying ... {retries} times left"
                                        );
                                        // wait 5 seconds to avoid too many retries
                                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                        continue;
                                    }

                                    tracing::error!(
                                        "[worker:{worker}] sync stable schema {stable} with {table_count} sub tables error: {err:#}, continue next"
                                    );

                                    if let Some(path) = opts.fails_to.as_ref() {
                                        path.lock().unwrap().write_fmt(format_args!(
                                            "meta\t{}:{}\t\t{}\n",
                                            stable.as_str(),
                                            tables.join(","),
                                            format!("{err:#}").replace("\n", " ")
                                        ))?;
                                    } else {
                                        println!(
                                            "meta\t{}:{}\t\t{}",
                                            stable.as_str(),
                                            tables.join(","),
                                            format!("{err:#}").replace("\n", " ")
                                        );
                                    }

                                    if let Some(sender) = sender {
                                        let _ = sender.send(Err(err));
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    None => {
                        //normal
                        let mut errors = String::new();
                        for table in &tables {
                            let remap = opts.remap.as_ref().and_then(|v| v.get(table));
                            if let Err(err) =
                                sync_normal_table_schema(&from, &table, &actions, remap, &to).await
                            {
                                tracing::error!("Syncing table `{table}` error: {err:?}");
                                if let Some(path) = opts.fails_to.as_ref() {
                                    path.lock().unwrap().write_fmt(format_args!(
                                        "meta\t{}\t\t{}\n",
                                        table.as_str(),
                                        format!("{err:?}").replace("\n", " ")
                                    ))?;
                                } else {
                                    println!(
                                        "meta\t{}\t\t{}",
                                        table.as_str(),
                                        format!("{err:?}").replace("\n", " ")
                                    );
                                }
                                errors.extend(format!("- Error of table {table}: {err}\n").chars());
                            } else {
                                counter!(METRICS_LEGACY_CREATED_TABLES, 1);
                                metrics.created_tables.fetch_add(1, Ordering::SeqCst);
                            }
                        }

                        if let Some(sender) = sender {
                            if errors.is_empty() {
                                let _ = sender.send(Ok(()));
                            } else {
                                let _ = sender.send(Err(anyhow::format_err!(
                                    "Syncing {} ordinary tables error:\n{errors}",
                                    tables.len()
                                )));
                            }
                        }
                    }
                }
            }
            Todo::Data(stable, table, mut time_range, sender) => {
                // get breakpoints use breakpoints_get
                if let Some(task_id) = task_id.clone() {
                    const MAX_RETRIES: usize = 5;
                    let mut retries = MAX_RETRIES;
                    loop {
                        let _lock = file_mutex.lock().unwrap();
                        match breakpoints::breakpoints_get(&task_id, &table).and_then(|bp| {
                            bp.map(|bp| bp.parse::<DateTime<Utc>>().context("Parse datetime error"))
                                .transpose()
                        }) {
                            Ok(Some(breakpoint)) => {
                                time_range.start = Some(breakpoint);
                                tracing::debug!("load breakpoint success set time_range: {time_range} table: {table}");
                                break;
                            }
                            Ok(None) => {
                                tracing::debug!("load breakpoint no breakpoint, table: {table}");
                                break;
                            }
                            Err(err) => {
                                tracing::debug!(
                                    "load breakpoint failed, err: {err} table: {table}, retrying ... {retries} times left"
                                );

                                if retries > 0 {
                                    retries -= 1;
                                    drop(_lock);
                                    std::thread::sleep(std::time::Duration::from_secs(1));
                                    continue;
                                } else {
                                    tracing::debug!(
                                        "load breakpoint failed finally, err: {err} table: {table}"
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }

                let query = QueryOpts {
                    time_range,
                    unit: query.unit,
                    limit: query.limit,
                    select_from_stable: query.select_from_stable,
                    smooth_init: query.smooth_init,
                };
                let mut retries = MAX_WS_RETRIES;

                let remap = opts.remap.as_ref().and_then(|v| {
                    v.get(
                        stable
                            .as_ref()
                            .map(|s| s.as_str())
                            .unwrap_or(table.as_str()),
                    )
                    .map(Clone::clone)
                });

                let chunks = split_table_into_time_range_chunks(&from, &table, &query).await;
                match chunks {
                    Ok(chunks) => {
                        let mut chunk_err: Option<String> = None;
                        // chunks
                        'chunks: for chunk in chunks {
                            let mut query = query.clone();
                            query.time_range = chunk;
                            let table_inner = table.clone();
                            loop {
                                let partial_metrics = Arc::new(LegacyMetrics::default());
                                match sync_single_table_partial(
                                    source.clone(),
                                    target.clone(),
                                    &from,
                                    &stable,
                                    &table,
                                    &to,
                                    &actions,
                                    &query,
                                    remap.as_ref(),
                                    &opts,
                                    target_is_v3,
                                    partial_metrics.clone(),
                                )
                                .await
                                {
                                    Ok(_) => {
                                        // set breakpoint async
                                        if let Some(task_id) = task_id.clone() {
                                            if let Some(end) = chunk.end {
                                                let breakpoint = end.to_string();
                                                // dbg!(&breakpoint);
                                                tokio::spawn(async move {
                                                    let _ = breakpoints::breakpoints_set(
                                                        &task_id,
                                                        &table_inner,
                                                        &breakpoint,
                                                    );
                                                });
                                            }
                                        }
                                        // metrics
                                        log::debug!(
                                            "sync table {table} time_range {time_range} partial metrics: {partial_metrics:#}",
                                            table = table.as_str(),
                                            time_range = query.time_range,
                                            partial_metrics = &partial_metrics,
                                        );

                                        let _ = metrics.merge(&partial_metrics);

                                        log::debug!(
                                            "sync table {table} time_range {time_range} total metrics: {metrics:#}",
                                            table = table.as_str(),
                                            time_range = query.time_range,
                                            metrics = metrics,
                                        );

                                        if let Some(metrics_db) = metrics_db.as_ref() {
                                            let str_metrics = metrics.to_json();
                                            log::debug!("str_metrics: {str_metrics}");
                                            let r = metrics_db.set(&str_metrics);
                                            if let Err(err) = r {
                                                log::error!("metrics_db::metrics_set error: {err}");
                                            }
                                        }

                                        break;
                                    }
                                    Err(err) => {
                                        let err_string = err.to_string();
                                        // tracing::error!("err_string: {err_string}");
                                        if (err_string.contains("0xE00")
                                            || err_string.contains("channel closed"))
                                            && retries > 0
                                        {
                                            from = source.get().await?;
                                            to = target.get().await?;
                                            retries -= 1;
                                            tracing::warn!(
                                    "[worker:{worker}] sync table {table} error: {err}, retrying ... {retries} times left"
                                );
                                            continue;
                                        } else if err_string.contains("0x263F")
                                            || err_string.contains("Column does not exist")
                                        {
                                            tracing::info!(
                                    "[worker:{worker}] sync table {table} err 0x263F: {err:?}, add column"
                                );
                                            let st = stable.as_ref().map(|s| s.as_str());
                                            if let Some(stable) = st {
                                                sync_add_column(&from, &to, stable, remap.as_ref())
                                                    .await?;
                                            } else {
                                                sync_add_column(&from, &to, &table, remap.as_ref())
                                                    .await?;
                                            }
                                            continue;
                                        }

                                        tracing::error!(
                                "[worker:{worker}] sync table {table} error: {err:?}, continue next"
                                        );
                                        if let Some(path) = opts.fails_to.as_ref() {
                                            path.lock().unwrap().write_fmt(format_args!(
                                                "data\t{}\t{:?}\t{}\n",
                                                table.as_str(),
                                                query.time_range,
                                                format!("{err:?}").replace("\n", " ")
                                            ))?;
                                        } else {
                                            println!(
                                                "data\t{}\t{:?}\t{}",
                                                table.as_str(),
                                                query.time_range,
                                                format!("{err:?}").replace("\n", " ")
                                            );

                                            chunk_err = Some(format!("{err:?}").to_string());
                                        }

                                        break 'chunks;
                                    }
                                };
                            }
                        }

                        match chunk_err {
                            Some(err) => {
                                if let Some(sender) = sender {
                                    let _ = sender.send(Err(anyhow::format_err!("Syncing table failed: {err}",)));
                                }
                            }
                            None => {
                                counter!(METRICS_LEGACY_TABLES, 1);
                                if let Some(sender) = sender {
                                    let _ = sender.send(Ok(()));
                                } 
                            }
                        }
                    }
                    Err(err) => {
                        tracing::error!(
                            "[worker:{worker}] sync table {table} error: {err:?}, continue next"
                        );
                        if let Some(path) = opts.fails_to.as_ref() {
                            path.lock().unwrap().write_fmt(format_args!(
                                "data\t{}\t{:?}\t{}\n",
                                table.as_str(),
                                query.time_range,
                                format!("{err:?}").replace("\n", " ")
                            ))?;
                        } else {
                            println!(
                                "data\t{}\t{:?}\t{}",
                                table.as_str(),
                                query.time_range,
                                format!("{err:?}").replace("\n", " ")
                            );
                        }
                    }
                }
            }
        }
    }
}

pub async fn sync_add_column(
    from: &Taos,
    to: &Taos,
    table: &str,
    remap: Option<&Arc<HashMap<String, String>>>,
) -> anyhow::Result<()> {
    let l_desc = from.describe(table).await?;
    let r_desc = to.describe(table).await?;
    let mut add_columns = Vec::new();
    for l in l_desc.iter() {
        if !l.is_tag()
            && !r_desc
                .iter()
                .any(|r| r.field() == remap.and_then(|map| map.get(l.field())).unwrap_or(&l.field))
        {
            add_columns.push(l);
        }
    }
    for col in add_columns {
        let sql = format!(
            "ALTER TABLE `{}` ADD COLUMN {}",
            table,
            transform_sql_with_remap(col.sql_repr(), remap)
        );
        tracing::info!("add column sql: {sql}");
        if let Err(err) = to.exec(sql.as_str()).await {
            tracing::error!("Add column error: {err:#}");
        }
    }

    Ok(())
}

impl Scheduler {
    #[instrument(skip_all)]
    pub async fn new(
        source: TaosPool,
        target: TaosPool,
        query: Arc<QueryOpts>,
        opts: Arc<TargetOpts>,
        workers: u32,
        actions: &Vec<Action>,
        metrics: Arc<LegacyMetrics>,
        metrics_db: Option<Arc<MetricsDb>>,
        source_is_v3: bool,
        target_is_v3: bool,
        task_id: Option<String>,
    ) -> Self {
        let workers = std::cmp::max(1, workers);
        let (sender, receiver) = flume::bounded((workers * 4) as usize);
        let file_mutex = Arc::new(std::sync::Mutex::new(()));
        let handles = (0..workers)
            .map(|i| {
                tokio::spawn(
                    worker(
                        i,
                        source.clone(),
                        target.clone(),
                        receiver.clone(),
                        query.clone(),
                        opts.clone(),
                        metrics.clone(),
                        metrics_db.clone(),
                        actions.clone(),
                        source_is_v3,
                        target_is_v3,
                        task_id.clone(),
                        file_mutex.clone(),
                    )
                    .in_current_span(),
                )
            })
            .collect_vec();

        Self {
            workers,
            source,
            target,
            opts,
            sender,
            receiver,
            handles,
            task_id,
        }
    }
    pub async fn send(&self, todo: Todo) -> Result<(), flume::SendError<Todo>> {
        self.sender.send_async(todo).await
    }

    // pub fn abort(&self) {
    //     for h in self.handles.iter() {
    //         h.abort();
    //     }
    // }

    // pub fn is_empty(&self) -> bool {
    //     self.receiver.is_empty()
    // }
}
impl std::future::Future for Scheduler {
    type Output = Result<(), JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for h in self.handles.iter_mut() {
            match h.poll_unpin(cx) {
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(Ok(_)) => {
                    continue;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}
