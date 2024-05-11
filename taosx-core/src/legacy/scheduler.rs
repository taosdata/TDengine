use std::{
    collections::HashMap,
    fmt::Debug,
    io::Write,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Context as _;
use chrono::{DateTime, Utc};
use flume::{Receiver, Sender};
use futures::FutureExt;
use futures_util::{StreamExt, TryStreamExt};
use itertools::Itertools;
use taos::{AsyncFetchable, AsyncQueryable, AsyncRows, BorrowedValue, ResultSet, Taos, TaosPool};
use tokio::{
    sync::oneshot,
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

use crate::{
    core_metrics::{CoreMetrics, TaskMetrics},
    legacy::{
        split_table_into_time_range_chunks, sync_single_table_partial, sync_super_table_schema,
        transform_sql_with_remap, transform_tbname_with_actions,
    },
    utils::breakpoints,
    Action, QueryOpts, TargetOpts, TimeRange,
};

use super::{sync_normal_table_schema, sync_super_table_schema_with_subs};

type TodoResp = oneshot::Sender<anyhow::Result<()>>;
pub enum Todo {
    STable(Arc<String>, TodoResp),
    Meta(Option<Arc<String>>, Vec<String>, Option<TodoResp>),
    Data(
        Option<Arc<String>>,
        Arc<String>,
        TimeRange,
        Option<TodoResp>,
    ),
    /// Multiple-tables-low-frequency kind of todo item.
    Sparse(Arc<String>, TimeRange, Option<TodoResp>),
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

#[instrument(skip_all, fields(worker = worker))]
async fn worker(
    worker: u32,
    source: TaosPool,
    target: TaosPool,
    receiver: Receiver<Todo>,
    query: Arc<QueryOpts>,
    opts: Arc<TargetOpts>,
    metrics_arc: Arc<CoreMetrics>,
    actions: Vec<Action>,
    source_is_v3: bool,
    target_is_v3: bool,
    task_id: Option<String>,
    file_mutex: Arc<tokio::sync::Mutex<()>>,
) -> anyhow::Result<()> {
    let metrics = metrics_arc.legacy();
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
                                metrics_arc.clone(),
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
                                metrics.add_created_tables(1);
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
                let span =
                    tracing::info_span!("sync_data", table = table.as_str(), range = ?time_range);
                let _entered = span.enter();
                // get breakpoints use breakpoints_get
                if let Some(task_id) = task_id.clone() {
                    const MAX_RETRIES: usize = 5;
                    let mut retries = MAX_RETRIES;
                    loop {
                        let _lock = file_mutex.lock().await;
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
                let _entered = span.enter();
                match chunks {
                    Ok(chunks) => {
                        let chunks_len = chunks.len();
                        span.record("chunks", &chunks_len);
                        tracing::info!("Syncing start with {} chunks", chunks_len);
                        let mut chunk_err: Option<String> = None;
                        // chunks
                        'chunks: for (idx, chunk) in chunks.into_iter().enumerate() {
                            let mut query = query.clone();
                            query.time_range = chunk;
                            let table_inner = table.clone();
                            loop {
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
                                    metrics_arc.clone(),
                                )
                                .in_current_span()
                                .await
                                {
                                    Ok(_) => {
                                        let _entered = span.enter();
                                        tracing::debug!(
                                            chunk.id = idx,
                                            chunk.range = ?chunk,
                                            "synced table {table} time_range {time_range}",
                                            table = table.as_str(),
                                            time_range = query.time_range,
                                        );
                                        // set breakpoint
                                        if let Some(task_id) = task_id.clone() {
                                            if let Some(end) = chunk.end {
                                                let breakpoint = end.to_string();
                                                // dbg!(&breakpoint);
                                                tokio::task::spawn_blocking(move || {
                                                    let max_retries = 5;
                                                    let mut retries = 0;
                                                    loop {
                                                        if let Err(err) =
                                                            breakpoints::breakpoints_set(
                                                                &task_id,
                                                                &table_inner,
                                                                &breakpoint,
                                                            )
                                                        {
                                                            retries += 1;
                                                            if retries >= max_retries {
                                                                tracing::warn!(
                                                                    task.id = task_id.as_str(),
                                                                    breakpoints.key = table_inner.as_str(),
                                                                    breakpoints.value = breakpoint.as_str(),
                                                                    "set breakpoint failed, err: {err:#}"
                                                                );
                                                            }
                                                        };
                                                    }
                                                })
                                                .instrument(
                                                    tracing::info_span!("set_breakpoint", chunk.id = idx, chunk.range = ?chunk))
                                                .await?;
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
                                            tracing::warn!("[worker:{worker}] sync table {table} error: {err}, retrying ... {retries} times left");
                                            continue;
                                        } else if err_string.contains("0x263F")
                                            || err_string.contains("Column does not exist")
                                        {
                                            tracing::info!("[worker:{worker}] sync table {table} error 0x263F: {err:?}, add column");
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

                                        tracing::error!("[worker:{worker}] sync table {table} with range {chunk} error: {err:?}, continue next");
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

                        let _entered = span.enter();
                        match chunk_err {
                            Some(err) => {
                                if let Some(sender) = sender {
                                    let _ = sender.send(Err(anyhow::format_err!(
                                        "Syncing table failed: {err}",
                                    )));
                                }
                            }
                            None => {
                                metrics.add_finished_tables(1);
                                tracing::info!(
                                    finished = metrics.finished_tables(),
                                    total = metrics.total_tables(),
                                    "Syncing partially done with table {table}"
                                );
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

            Todo::Sparse(table, time_range, sender) => {
                let query = QueryOpts {
                    time_range,
                    unit: query.unit,
                    limit: query.limit,
                    select_from_stable: query.select_from_stable,
                    smooth_init: query.smooth_init,
                };
                let mut retries = MAX_WS_RETRIES;

                let remap = opts
                    .remap
                    .as_ref()
                    .and_then(|v| v.get(table.as_str()).map(Clone::clone));
                let mut chunk_err: Option<String> = None;
                loop {
                    let partial_metrics = metrics_arc.clone();
                    match sync_sparse_stable(
                        source.clone(),
                        target.clone(),
                        &from,
                        &table,
                        &to,
                        &actions,
                        &query,
                        remap.as_ref(),
                        &opts,
                        target_is_v3,
                        partial_metrics,
                    )
                    .await
                    {
                        Ok(_) => {
                            // metrics
                            log::debug!(
                                            "sync table {table} time_range {time_range} total metrics: {metrics:#}",
                                            table = table.as_str(),
                                            time_range = query.time_range,
                                            metrics = metrics,
                                        );

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
                                sync_add_column(&from, &to, &table, remap.as_ref()).await?;
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
                            break;
                        }
                    };
                }

                match chunk_err {
                    Some(err) => {
                        if let Some(sender) = sender {
                            let _ = sender
                                .send(Err(anyhow::format_err!("Syncing table failed: {err}",)));
                        }
                    }
                    None => {
                        metrics.add_finished_tables(1);
                        if let Some(sender) = sender {
                            let _ = sender.send(Ok(()));
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
    pub async fn new(
        source: TaosPool,
        target: TaosPool,
        query: Arc<QueryOpts>,
        opts: Arc<TargetOpts>,
        workers: u32,
        actions: &Vec<Action>,
        metrics: Arc<CoreMetrics>,
        source_is_v3: bool,
        target_is_v3: bool,
        task_id: Option<String>,
        cancellation: CancellationToken,
        file_mutex: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        let workers = std::cmp::max(1, workers);
        let (sender, receiver) = flume::bounded((workers * 4) as usize);

        let handles = (0..workers)
            .map(|i| {
                let cancellation = cancellation.clone();
                let future = worker(
                    i,
                    source.clone(),
                    target.clone(),
                    receiver.clone(),
                    query.clone(),
                    opts.clone(),
                    metrics.clone(),
                    actions.clone(),
                    source_is_v3,
                    target_is_v3,
                    task_id.clone(),
                    file_mutex.clone(),
                )
                .in_current_span();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = future => {
                            Ok(())
                        }
                        _ = cancellation.cancelled() => {
                            Ok(())
                        }
                    }
                })
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

    pub fn abort(&self) {
        self.handles.iter().for_each(|h| h.abort());
    }

    pub async fn send(&self, todo: Todo) -> Result<(), flume::SendError<Todo>> {
        self.sender.send_async(todo).await
    }

    pub fn send_blocking(&self, todo: Todo) -> Result<(), flume::SendError<Todo>> {
        self.sender.send(todo)
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

#[async_backtrace::framed]
async fn sync_sparse_stable(
    _source: TaosPool,
    target: TaosPool,
    from: &Taos,
    table: &Arc<String>,
    _to: &Taos,
    actions: &Vec<Action>,
    opts: &QueryOpts,
    remap: Option<&Arc<HashMap<String, String>>>,
    target_opts: &TargetOpts,
    _target_is_v3: bool,
    metrics: Arc<CoreMetrics>,
) -> anyhow::Result<()> {
    tracing::info!(
        "Syncing sparse table {table} with range: {}",
        opts.time_range
    );
    let stable_schema = from.describe(table).await?;
    debug_assert!(
        { stable_schema.iter().any(|f| f.is_tag()) },
        "{table} must be a stable"
    );
    let sql = if opts.is_none() {
        format!("SELECT tbname, * FROM `{table}`")
    } else {
        format!("SELECT tbname, * FROM `{table}` WHERE {opts}")
    };
    let tag_idx = stable_schema
        .iter()
        .position(|f| f.is_tag())
        .expect("stable must have a tag");

    // let mut blocks = res.blocks();
    let new_table_name = if actions.is_empty() {
        table.clone()
    } else {
        Arc::new(transform_tbname_with_actions(&table, actions, true)?.to_string())
    };

    let mut res = from
        .query(&sql)
        .await
        .context(format!("query with {sql}"))?;

    let concurrent_limit = target_opts.concurrent_limit.get();

    const MAX_SQL_LEN: usize = 1000 * 1000; // 800kb.
    let max_sql_length = target_opts.max_sql_length.unwrap_or(MAX_SQL_LEN);
    {
        let (add_tag_names, add_tag_values) = {
            actions
                .iter()
                .filter(|a| matches!(a, Action::AddTag(_)))
                .fold(
                    (String::new(), String::new()),
                    |(mut names, mut values), a| {
                        if let Action::AddTag(n) = a {
                            let (_, value) = n.entry();
                            names.push(',');
                            names.push('`');
                            names.push_str(n.name.as_str());
                            names.push('`');

                            values.push(',');
                            values.push('\'');
                            values.push_str(value);
                            values.push('\'');
                        }
                        (names, values)
                    },
                )
        };
        let sqls = futures::stream::unfold(
            Some((
                res.rows(),
                new_table_name.clone(),
                add_tag_names,
                add_tag_values,
                actions,
                remap.clone(),
                String::with_capacity(1024),
            )),
            |context| async move {
                type TaosRows<'a> = AsyncRows<'a, ResultSet>;

                let (mut rows, new_table_name, add_tag_names, add_tag_values, actions, remap, tmp) =
                    context?;

                let mut sql = String::with_capacity(MAX_SQL_LEN);
                sql.push_str("INSERT INTO");

                async fn yield_sql_from<'a>(
                    rows: &mut TaosRows<'a>,
                    mut sql: String,
                    mut tmp: String,
                    stable: &str,

                    add_tag_names: &str,
                    add_tag_values: &str,
                    tag_idx: usize,
                    max_sql_length: usize,
                    actions: &Vec<Action>,
                    remap: Option<&Arc<HashMap<String, String>>>,
                ) -> Result<Option<(usize, String, String)>, taos::Error> {
                    let mut contains = 0;
                    if !tmp.is_empty() {
                        sql.push_str(tmp.as_str());
                        contains += 1;
                        tmp.clear();
                    }
                    while let Some(row) = rows.next().await.and_then(|r| match r {
                        Ok(r) => Some(Ok(r)),
                        Err(err) => {
                            if err.message().contains("result is nil") {
                                // taosAdapter returns `result is nil` error when polled too fast after end.
                                // We should ignore this error and treat as end of stream.
                                None
                            } else {
                                Some(Err(err))
                            }
                        }
                    }) {
                        let row = row.map_err(|err| err.context("sparse row view error"))?;
                        // dbg!(&row);
                        let values = row.collect_vec();
                        let name = match &values[0].1 {
                            BorrowedValue::VarChar(s) => *s,
                            _ => unreachable!(),
                        };
                        let name = transform_tbname_with_actions(&name, actions, false)?;
                        tmp.push_str(&format!(" `{}` using `{}` ", name, stable));

                        let tags = &values[tag_idx + 1..];
                        let values = &values[1..=tag_idx];
                        if let Some(remap) = remap {
                            if add_tag_names.is_empty() {
                                tmp.push_str(&format!(
                                    "({}) tags({}) ({}) values({})",
                                    tags.iter()
                                        .map(|(n, _)| format!(
                                            "`{}`",
                                            remap.get(*n).map(|s| s.as_str()).unwrap_or(n)
                                        ))
                                        .join(","),
                                    tags.iter().map(|(_, v)| v.to_sql_value()).join(","),
                                    values
                                        .iter()
                                        .map(|(n, _)| format!(
                                            "`{}`",
                                            remap.get(*n).map(|s| s.as_str()).unwrap_or(n)
                                        ))
                                        .join(","),
                                    values.iter().map(|(_, v)| v.to_sql_value()).join(","),
                                ));
                            } else {
                                tmp.push_str(&format!(
                                    "({}{}) tags({}{}) ({}) values({})",
                                    tags.iter()
                                        .map(|(n, _)| format!(
                                            "`{}`",
                                            remap.get(*n).map(|s| s.as_str()).unwrap_or(n)
                                        ))
                                        .join(","),
                                    add_tag_names,
                                    tags.iter().map(|(_, v)| v.to_sql_value()).join(","),
                                    add_tag_values,
                                    values
                                        .iter()
                                        .map(|(n, _)| format!(
                                            "`{}`",
                                            remap.get(*n).map(|s| s.as_str()).unwrap_or(n)
                                        ))
                                        .join(","),
                                    values.iter().map(|(_, v)| v.to_sql_value()).join(","),
                                ));
                            }
                        } else {
                            if add_tag_names.is_empty() {
                                tmp.push_str(&format!(
                                    "({}) tags({}) ({}) values({})",
                                    tags.iter().map(|(n, _)| format!("`{}`", n)).join(","),
                                    tags.iter().map(|(_, v)| v.to_sql_value()).join(","),
                                    values.iter().map(|(n, _)| format!("`{}`", n)).join(","),
                                    values.iter().map(|(_, v)| v.to_sql_value()).join(","),
                                ));
                            } else {
                                tmp.push_str(&format!(
                                    "({}{}) tags({}{}) ({}) values({})",
                                    tags.iter().map(|(n, _)| format!("`{}`", n)).join(","),
                                    add_tag_names,
                                    tags.iter().map(|(_, v)| v.to_sql_value()).join(","),
                                    add_tag_values,
                                    values.iter().map(|(n, _)| format!("`{}`", n)).join(","),
                                    values.iter().map(|(_, v)| v.to_sql_value()).join(","),
                                ));
                            }
                        }

                        if sql.len() + tmp.len() > max_sql_length {
                            debug_assert!(contains > 0);
                            return Ok(Some((contains, sql, tmp)));
                        } else {
                            sql.push_str(tmp.as_str());
                            contains += 1;
                            tmp.clear();
                        }
                    }
                    if contains > 0 {
                        Ok(Some((contains, sql, tmp)))
                    } else {
                        Ok(None)
                    }
                }
                yield_sql_from(
                    &mut rows,
                    sql,
                    tmp,
                    &new_table_name,
                    &add_tag_names,
                    &add_tag_values,
                    tag_idx,
                    max_sql_length,
                    actions,
                    remap,
                )
                .await
                .transpose()
                .and_then(|v| match v {
                    Ok((records, sql, tmp)) => {
                        if records == 0 {
                            None
                        } else {
                            Some((
                                Ok((records, sql)),
                                Some((
                                    rows,
                                    new_table_name,
                                    add_tag_names,
                                    add_tag_values,
                                    actions,
                                    remap,
                                    tmp,
                                )),
                            ))
                        }
                    }
                    Err(err) => Some((Err(err), None)),
                })
            },
        );
        tokio::pin!(sqls);

        async fn sparse_concurrent_runner(
            target: TaosPool,
            tag_idx: usize,
            recv: flume::Receiver<(usize, String)>,
            metrics: Arc<CoreMetrics>,
        ) -> anyhow::Result<()> {
            let metrics = metrics.legacy();
            let to = target.get().await?;
            while let Ok((records, sql)) = recv.recv_async().await {
                to.exec(sql.as_str()).await?;
                metrics.add_success_blocks(1);
                metrics.add_written_rows(records as _);
                metrics.add_written_points(records as u64 * tag_idx as u64);
            }
            Ok(())
        }

        let mut set = tokio::task::JoinSet::new();

        let (tx, rx) = flume::bounded(concurrent_limit as usize * 8);
        for _ in 0..concurrent_limit {
            set.spawn(sparse_concurrent_runner(
                target.clone(),
                tag_idx,
                rx.clone(),
                metrics.clone(),
            ));
        }

        sqls.try_for_each_concurrent(concurrent_limit, |(records, sql)| {
            tx.send_async((records, sql)).map(|_| Ok(()))
        })
        .await?;
        drop(tx);

        while let Some(res) = set.join_next().await {
            res??;
        }
    }
    let (blocks, rows) = res.summary();
    tracing::info!(
        "Synced sparse table {table} with {blocks} blocks, {rows} rows",
        table = table.as_str(),
        blocks = blocks,
        rows = rows
    );
    Ok(())
}
