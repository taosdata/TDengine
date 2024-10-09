use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Display},
    io::Write,
    num::NonZeroUsize,
    path::Path,
    str::FromStr,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use anyhow::{bail, Context};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::FutureExt;
use rand::seq::SliceRandom;
use serde::Deserialize;
use serde_with::serde_as;
use taos::*;
use tokio::{sync::oneshot, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn, Instrument};

use crate::{
    core_metrics::{get_metrics_arc, CoreMetrics, TaskMetrics},
    legacy::scheduler::Todo,
    utils::{breakpoints::BreakpointDb, constants::VERSION_3_3_0, sql::get_v2_precision},
    Action,
};

use self::chunks::TimeChunks;
use self::scheduler::Scheduler;

mod chunks;
pub mod legacy_metric;
mod scheduler;
mod verify;
// mod tasks;

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

    pub fn from_params(dsn: &mut Dsn) -> Result<Self, parse_duration::parse::Error> {
        let mut opts = Self::new();
        if let Some(value) = dsn
            .remove("retro")
            .or(dsn.remove("restro"))
            .or(dsn.remove("retrospect"))
        {
            opts.restro = parse_duration::parse(&value)?;
        }
        if let Some(value) = dsn.remove("interval") {
            opts.interval = parse_duration::parse(&value)?;
        }
        if let Some(value) = dsn.remove("excursion") {
            opts.excursion = parse_duration::parse(&value)?;
        }
        Ok(opts)
    }
}

/// A paging expression.
///
/// It will be append to query with `LIMIT {limit} OFFSET {offset}`.
#[derive(Debug, Default, Clone, Copy)]
struct Limit {
    limit: u32,
    offset: Option<u32>,
}

impl Limit {
    #[cfg(test)]
    pub const fn new(limit: (u32, Option<u32>)) -> Self {
        Self {
            limit: limit.0,
            offset: limit.1,
        }
    }

    // #[cfg(test)]
    // pub const fn limit(mut self, limit: u32) -> Self {
    //     self.limit = limit;
    //     self
    // }
    // #[cfg(test)]
    // pub const fn offset(mut self, offset: u32) -> Self {
    //     self.offset = Some(offset);
    //     self
    // }

    pub fn is_none(&self) -> bool {
        matches!((self.limit, self.offset), (0 | u32::MAX, None))
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.limit, self.offset) {
            (0 | u32::MAX, None) => Ok(()),
            (0 | u32::MAX, Some(offset)) => write!(f, " OFFSET {offset}"),
            (limit, None) => write!(f, " LIMIT {limit}"),
            (limit, Some(offset)) => write!(f, " LIMIT {limit} OFFSET {offset}"),
        }
    }
}

/// A (half-open) time range bounded inclusively below and exclusively above (`start..end`).
///
/// The range `start..end` is equivalent to TDengine SQL condition `_c0 >= {start} AND _c0 < {end}`.
#[derive(Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct TimeRange {
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
}

impl TimeRange {
    pub const fn new() -> Self {
        Self {
            start: None,
            end: None,
        }
    }

    pub fn get_start(&self) -> Option<DateTime<Utc>> {
        self.start
    }

    pub fn get_end(&self) -> Option<DateTime<Utc>> {
        self.end
    }

    pub const fn start(mut self, start: DateTime<Utc>) -> Self {
        self.start = Some(start);
        self
    }

    pub const fn end(mut self, end: DateTime<Utc>) -> Self {
        self.end = Some(end);
        self
    }

    pub const fn has_start(&self) -> bool {
        self.start.is_some()
    }

    pub const fn has_end(&self) -> bool {
        self.end.is_some()
    }

    pub const fn is_none(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }

    pub fn to_chunks(&self, duration: Duration) -> Vec<Self> {
        let duration = if duration.is_zero() {
            chrono::Duration::days(1)
        } else {
            chrono::Duration::from_std(duration).unwrap()
        };
        match (self.start, self.end) {
            (Some(mut start), Some(end)) => {
                let mut chunks = vec![];
                loop {
                    let chunk_end = start + duration;
                    if chunk_end >= end {
                        chunks.push(Self {
                            start: Some(start),
                            end: Some(end),
                        });
                        break;
                    }
                    chunks.push(Self {
                        start: Some(start),
                        end: Some(chunk_end),
                    });
                    start = chunk_end;
                }

                chunks
            }
            _ => vec![*self],
        }
    }

    pub fn to_chunks_iter(&self, duration: Duration) -> TimeChunks {
        let duration = if duration.is_zero() {
            chrono::Duration::days(1)
        } else {
            chrono::Duration::from_std(duration).unwrap()
        };
        TimeChunks::new(*self, duration)
    }

    pub fn till_now() -> Self {
        let end = chrono::Utc::now();
        Self {
            start: None,
            end: Some(end),
        }
    }
}

impl Debug for TimeRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.start, self.end) {
            (None, None) => f.write_str(".."),
            (Some(start), None) => f.write_fmt(format_args!("{}..", start.to_rfc3339())),
            (None, Some(end)) => f.write_fmt(format_args!("..{}", end.to_rfc3339())),
            (Some(start), Some(end)) => {
                f.write_fmt(format_args!("{}..{}", start.to_rfc3339(), end.to_rfc3339()))
            }
        }
    }
}

impl Display for TimeRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.start, self.end) {
            (None, None) => Ok(()),
            (Some(start), None) => f.write_fmt(format_args!(" _c0 >= '{}'", start.to_rfc3339())),
            (None, Some(end)) => f.write_fmt(format_args!(" _c0 < '{}'", end.to_rfc3339())),
            (Some(start), Some(end)) => f.write_fmt(format_args!(
                " _c0 >= '{}' and _c0 < '{}'",
                start.to_rfc3339(),
                end.to_rfc3339()
            )),
        }
    }
}

#[test]
fn test_time_range() {
    let ts_range = TimeRange::new();
    dbg!(ts_range.start);
    dbg!(ts_range);

    let chunks = ts_range.to_chunks(Duration::from_secs(5));
    assert!(chunks[0] == ts_range);

    let range = TimeRange::new()
        .start(Utc::now())
        .end(Utc::now() + chrono::Duration::days(3));

    let chunks = range.to_chunks(chrono::Duration::days(1).to_std().unwrap());

    dbg!(&chunks);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct QueryOpts {
    time_range: TimeRange,
    unit: Duration,
    limit: Limit,
    select_from_stable: bool,
    smooth_init: Duration,
}

impl Display for QueryOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.time_range, self.limit)
    }
}

impl QueryOpts {
    pub fn is_none(&self) -> bool {
        self.time_range.is_none() && self.limit.is_none()
    }
    pub fn time_range_chunks(&self) -> Vec<TimeRange> {
        self.time_range.to_chunks(self.unit)
    }
}

#[instrument(skip_all)]
#[async_backtrace::framed]
async fn split_table_into_time_range_chunks(
    from: &Taos,
    table: &str,
    opts: &QueryOpts,
) -> anyhow::Result<TimeChunks> {
    tracing::debug!("Split table `{table}` into chunks");

    let mut time_range = opts.time_range;
    async fn query_ts_with(
        taos: &Taos,
        sql: impl AsRef<str>,
    ) -> Result<chrono::DateTime<Utc>, taos::Error> {
        let sql = sql.as_ref();
        let mut set = taos.query(sql).await?;
        let mut records = set.to_records().await?;
        if let Some(Value::Timestamp(ts)) = records.pop().and_then(|mut v| v.pop()) {
            Ok(Utc.from_local_datetime(&ts.to_naive_datetime()).unwrap())
        } else {
            Err(taos::Error::from_string("Invalid sql for timestamp: {sql}"))
        }
    }
    match (time_range.has_start(), time_range.has_end()) {
        (true, true) => (),
        (true, false) => {
            if let Ok(ts) = query_ts_with(from, format!("select last(_c0) from `{table}`")).await {
                time_range.end.replace(ts + chrono::Duration::seconds(1));
                tracing::debug!("Replace end: {:?}", time_range.end);
            }
        }
        (false, true) => {
            if let Ok(ts) = query_ts_with(from, format!("select first(_c0) from `{table}`")).await {
                time_range.start.replace(ts);
                tracing::debug!("Replace start: {:?}", time_range.start);
            }
        }
        (false, false) => {
            if let Ok(ts) = query_ts_with(from, format!("select first(_c0) from `{table}`")).await {
                time_range.start.replace(ts);
                tracing::debug!("Replace start: {:?}", time_range.start);
            }
            if let Ok(ts) = query_ts_with(from, format!("select last(_c0) from `{table}`")).await {
                time_range.end.replace(ts + chrono::Duration::seconds(1));
                tracing::debug!("Replace end: {:?}", time_range.end);
            }
        }
    }
    Ok(time_range.to_chunks_iter(opts.unit))
}

struct WriteContext {
    from: (TaosPool, TableTodo),
    to: (TaosPool, TableTodo),
    actions: Vec<Action>,
    target_opts: TargetOpts,
    metrics_arc: Arc<CoreMetrics>,
    remap: Option<Arc<HashMap<String, String>>>,
    with_precision: Option<Precision>,
}

// #[instrument(skip_all, fields(precision = ?context.target_precision))]
async fn write_block(mut block: RawBlock, context: Arc<WriteContext>) -> RawResult<()> {
    // write block
    let to = &context
        .to
        .0
        .get()
        .await
        .context("Get source connection error")?;
    let stable = context.from.1.stable.as_deref();
    let table = &context.from.1.name;
    let actions = &context.actions;
    let new_table_name = context.to.1.name.as_str();
    let metrics_arc = &context.metrics_arc;
    let target_opts = &context.target_opts;
    let remap = &context.remap;
    let metrics = metrics_arc.legacy();

    if let Some(remap) = remap {
        let names = block
            .field_names()
            .iter()
            .map(|s| remap.get(s).unwrap_or(s))
            .map(Clone::clone)
            .collect_vec();
        block.with_field_names(names);
    }
    block.with_table_name(new_table_name);
    if let Some(precision) = context.with_precision {
        // block.with_precision(precision);
        block = block.cast_precision(precision);
    }

    loop {
        let ok = to.write_raw_block(&block).await;
        if let Err(err) = ok {
            let code: i32 = err.code().into();
            let err_str = err.to_string();
            tracing::debug!("sync_single_table_partial write raw block error: {err:#}",);
            let from = &context
                .from
                .0
                .get()
                .await
                .context("Get source connection error")?;
            if code == 0x2603 || code == 0x0618 {
                if let Some(stable) = stable {
                    sync_super_table_schema(from, stable, to, remap.as_ref(), target_opts, actions)
                        .in_current_span()
                        .await?;
                    sync_super_table_schema_with_subs(
                        from,
                        stable,
                        &[table.as_str()],
                        to,
                        remap.as_ref(),
                        target_opts,
                        true,
                        actions,
                        metrics_arc.clone(),
                    )
                    .in_current_span()
                    .await?;
                } else {
                    sync_normal_table_schema(from, table, actions, remap.as_ref(), to)
                        .in_current_span()
                        .await?;
                }
                continue;
            } else if code == 0x263F || code == 0x061B {
                tracing::info!("sync table {table} error with: {err:#}");
                if let Some(stable) = stable {
                    scheduler::sync_add_column(from, to, stable, remap.as_ref()).await?;
                } else {
                    scheduler::sync_add_column(from, to, table, remap.as_ref()).await?;
                }
                continue;
            } else if err_str.contains("0x0911") {
                // TSDB_CODE_SYN_PROPOSE_NOT_READY： Sync not ready to propose
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            } else if err_str.contains("0x0118") {
                let desc = to
                    .describe(table)
                    .await
                    .map_err(|err| anyhow::format_err!("Describe table {table} error: {err}"))?;
                let fields: HashMap<_, Ty> = block
                    .field_names()
                    .iter()
                    .map(|name| {
                        desc.iter()
                            .find(|f| f.field() == name)
                            .map(|f| (name, f.ty()))
                            .ok_or_else(|| anyhow::format_err!("Column does not exist {name}"))
                    })
                    .try_collect()?;
                let views: Vec<ColumnView> = block
                    .column_views()
                    .iter()
                    .zip(block.field_names())
                    .map(|(view, name)| view.cast(fields[name]))
                    .try_collect()
                    .map_err(RawError::from_any)?;
                let mut new = RawBlock::from_views(views.as_slice(), block.precision());
                new.with_table_name(new_table_name);
                new.with_field_names(block.field_names());
                // dbg!(&new);
                // new.pretty_format();
                to.write_raw_block(&new)
                    .await
                    .with_context(|| new.pretty_format().to_string())
                    .with_context(|| {
                        anyhow::format_err!(
                            "[{}:{}]write raw block of table {table} ({} rows)",
                            std::file!(),
                            std::line!(),
                            new.nrows(),
                        )
                    })?;
            } else {
                return Err(err)
                    .with_context(|| block.pretty_format().to_string())
                    .with_context(|| {
                        format!(
                            "[{}:{}]write raw block of table {table} ({} rows): {}",
                            std::file!(),
                            std::line!(),
                            block.nrows(),
                            err_str
                        )
                    })?;
            }
        }
        break;
    }

    metrics.add_success_blocks(1);
    metrics.add_written_rows(block.nrows() as _);
    metrics.add_written_points((block.nrows() * block.ncols()) as _);

    if let Some(duration) = target_opts.interval {
        tokio::time::sleep(duration).await;
    }
    RawResult::Ok(())
}

// #[async_backtrace::framed]
// async fn sync_single_table(
//     from: &Taos,
//     stable: Option<&str>,
//     table: &str,
//     to: &Taos,
//     actions: &Vec<Action>,
//     opts: &QueryOpts,
//     target_opts: &TargetOpts,
//     target_is_v3: bool,
//     metrics: &LegacyMetrics,
// ) -> anyhow::Result<()> {
//     log::debug!("Migrate data from table `{table}`");

//     let mut time_range = opts.time_range;
//     async fn query_ts_with(
//         taos: &Taos,
//         sql: impl AsRef<str>,
//     ) -> Result<chrono::DateTime<Utc>, taos::Error> {
//         let sql = sql.as_ref();
//         let mut set = taos.query(&sql).await?;
//         let mut records = set.to_records().await?;
//         if let Some(Value::Timestamp(ts)) = records.pop().and_then(|mut v| v.pop()) {
//             Ok(Utc.from_local_datetime(&ts.to_naive_datetime()).unwrap())
//         } else {
//             Err(taos::Error::from_string("Invalid sql for timestamp: {sql}"))
//         }
//     }
//     match (time_range.has_start(), time_range.has_end()) {
//         (true, true) => (),
//         (true, false) => {
//             if let Ok(ts) = query_ts_with(from, format!("select last(_c0) from {table}")).await {
//                 time_range.end.replace(ts + chrono::Duration::seconds(1));
//             }
//         }
//         (false, true) => {
//             if let Ok(ts) = query_ts_with(from, format!("select first(_c0) from {table}")).await {
//                 time_range.start.replace(ts);
//             }
//         }
//         (false, false) => {
//             if let Ok(ts) = query_ts_with(from, format!("select first(_c0) from {table}")).await {
//                 time_range.start.replace(ts);
//             }
//             if let Ok(ts) = query_ts_with(from, format!("select last(_c0) from {table}")).await {
//                 time_range.end.replace(ts + chrono::Duration::seconds(1));
//             }
//         }
//     }
//     for ts in time_range.to_chunks(opts.unit) {
//         let mut opts = opts.clone();
//         opts.time_range = ts;
//         sync_single_table_partial(
//             from,
//             stable,
//             table,
//             to,
//             actions,
//             &opts,
//             target_opts,
//             target_is_v3,
//             metrics,
//         )
//         .await?;
//     }
//     Ok(())
// }

#[instrument(skip_all)]
#[async_backtrace::framed]
async fn sync_single_table_partial(
    source: TaosPool,
    target: TaosPool,
    from: &Taos,
    stable: &Option<Arc<String>>,
    table: &Arc<String>,
    to: &Taos,
    actions: &[Action],
    opts: &QueryOpts,
    remap: Option<&Arc<HashMap<String, String>>>,
    target_opts: &TargetOpts,
    target_is_v3: bool,
    with_precision: Option<Precision>,
    metrics_arc: &Arc<CoreMetrics>,
) -> anyhow::Result<()> {
    tracing::debug!("Syncing table {table} with range: {}", opts.time_range);
    let metrics = metrics_arc.legacy();
    let (table, sql) = if opts.select_from_stable {
        if let Some(stable) = stable {
            let stable_schema = from.describe(stable).await?;
            let fields = stable_schema
                .iter()
                .filter(|f| !f.is_tag())
                .map(|f| format!("`{}`", f.field()))
                .join(",");
            let sql = if opts.is_none() {
                format!("SELECT {fields} FROM `{stable}` WHERE tbname = '{table}'")
            } else {
                format!("SELECT {fields} FROM `{stable}` WHERE tbname = '{table}' AND {opts}")
            };
            (table, sql)
        } else {
            let sql = if opts.is_none() {
                format!("SELECT * FROM `{table}`")
            } else {
                format!("SELECT * FROM `{table}` WHERE {opts}")
            };
            (table, sql)
        }
    } else {
        let sql = if opts.is_none() {
            format!("SELECT * FROM `{table}`")
        } else {
            format!("SELECT * FROM `{table}` WHERE {opts}")
        };
        (table, sql)
    };

    let mut res = from
        .query(&sql)
        .await
        .with_context(|| format!("SQL: {sql}"))
        .with_context(|| "query from source error")?;
    let fields = res.num_of_fields();
    let mut blocks = res.blocks();
    let new_table_name = if actions.is_empty() {
        table.clone()
    } else {
        Arc::new(transform_tbname_with_actions(table, actions, false)?.to_string())
    };

    let concurrent_limit = target_opts.concurrent_limit.get();
    let span = tracing::info_span!("sync_single_table_partial", table = %table, target = %new_table_name, concurrent_limit, blocks_chunk_size = target_opts.blocks_chunk_size.get());
    let _guard = span.enter();

    if target_is_v3 && !target_opts.force_stmt {
        let context = Arc::new(WriteContext {
            from: (
                source.clone(),
                TableTodo::new(table.clone(), stable.clone()),
            ),
            to: (
                target.clone(),
                TableTodo::new(new_table_name.clone(), stable.clone()),
            ),
            actions: actions.to_vec(),
            target_opts: target_opts.clone(),
            metrics_arc: metrics_arc.clone(),
            remap: remap.map(Clone::clone),
            with_precision,
        });

        if target_opts.blocks_chunk_size.get() == 1 {
            if concurrent_limit == 1 {
                blocks
                    .try_for_each(|block| write_block(block, context.clone()))
                    .await?;
            } else {
                blocks
                    .try_for_each_concurrent(concurrent_limit, |block| {
                        write_block(block, context.clone())
                    })
                    .await?;
            }
        } else {
            let blocks = blocks
                .chunks(target_opts.blocks_chunk_size.get())
                .map(|chunk| {
                    chunk
                        .into_iter()
                        .reduce(|a, b| match (a, b) {
                            (Ok(a), Ok(b)) => Ok(a.concat(&b)),
                            (Err(err), _) => Err(err),
                            (_, Err(err)) => Err(err),
                        })
                        .unwrap()
                });

            if concurrent_limit == 1 {
                blocks
                    .try_for_each(|block| write_block(block, context.clone()))
                    .await?;
            } else {
                blocks
                    .try_for_each_concurrent(concurrent_limit, |block| {
                        write_block(block, context.clone())
                    })
                    .await?;
            }
        }
    } else {
        let question_masks = std::iter::repeat('?').take(fields).join(",");
        let sql = format!("INSERT INTO `{new_table_name}` VALUES({question_masks})");

        let mut stmt = Stmt::init(to).await.context("initialize stmt")?;
        let mut prepare = false;
        while let Some(mut block) = blocks.try_next().await? {
            if let Some(precision) = with_precision {
                block = block.cast_precision(precision);
            }
            // dbg!(res.summary());
            if !prepare {
                stmt.prepare(&sql)
                    .await
                    .with_context(|| format!("[{new_table_name}] prepare statement error"))?;
                prepare = true;
            }
            let views = block.column_views();
            if let Some(batch_size) = target_opts.batch_size {
                if batch_size < block.nrows() {
                    for i in 0..block.nrows().div_ceil(batch_size) {
                        let range =
                            batch_size * i..std::cmp::min(batch_size * (i + 1), block.nrows());
                        let params: Vec<_> = views
                            .iter()
                            .map(|view| view.slice(range.clone()).unwrap())
                            .collect();
                        tracing::debug!(
                            "[{table}] write {}..{} rows with max batch size: {batch_size}",
                            range.start,
                            range.end,
                        );
                        stmt.bind(&params)
                            .await
                            .context(format!("[{new_table_name}] bind by chunk {batch_size}"))?;
                        stmt.add_batch().await.context(format!(
                            "[{new_table_name}] add batch by chunk {batch_size}"
                        ))?;
                        stmt.execute().await
                            .with_context(|| format!("[{new_table_name}] execute {} rows insertion with batch size limit {batch_size}", range.len()))?;

                        metrics.add_success_blocks(1);
                        metrics.add_written_rows(params.len() as _);
                        metrics.add_written_points((params.len() * fields) as _);
                        if let Some(duration) = target_opts.interval {
                            tokio::time::sleep(duration).await;
                        }
                    }
                    continue;
                }
            }
            stmt.bind(views)
                .await
                .with_context(|| format!("[{table}] bind error"))?;
            stmt.add_batch()
                .await
                .with_context(|| format!("[{table}] add batch"))?;

            let res = stmt.execute().await;

            if res.is_err() {
                let err = res.unwrap_err();
                tracing::warn!("Write block error: {err}");
                let err_str = err.to_string();
                if err_str.contains("0x1002") {
                    let mut chunks = 4;
                    let views = block.column_views();
                    let mut success = true;
                    // re-bind from start of the block for each loop until success
                    for _ in 0..4 {
                        if target_is_v3 {
                            stmt.prepare(&sql).await.context("re-prepare statement")?;
                        } else {
                            stmt = Stmt::init(to).await.context("re-initialize stmt")?;
                            stmt.prepare(&sql)
                                .await
                                .with_context(|| format!("[{table}] re-prepare statement error"))?;
                        }
                        let mut batch_size = block.nrows() / chunks;
                        if batch_size == 0 {
                            batch_size = 1;
                        }
                        // split chunks by batch size
                        for i in 0..block.nrows().div_ceil(batch_size) {
                            let range = batch_size * i..batch_size * (i + 1);
                            let params: Vec<_> = views
                                .iter()
                                .map(|view| view.slice(range.clone()).unwrap())
                                .collect();
                            stmt.bind(&params)
                                .await
                                .context(format!("[{table}] bind by batch limit {batch_size}"))?;
                            stmt.add_batch()
                                .await
                                .context(format!("[{table}] add batch with limit {batch_size}"))?;
                            // stmt.execute().context(format!(
                            //     "[{table}] execute with batch limit {batch_size}"
                            // ))?;

                            // if still error, go ahead to next loop.
                            if stmt.execute().await.is_err() {
                                success = false;
                                break;
                            }
                            metrics.add_success_blocks(1);
                            metrics.add_written_rows(params.len() as _);
                            metrics.add_written_points((params.len() * fields) as _);
                        }
                        if success {
                            break;
                        }
                        chunks *= 4;
                        if batch_size == 1 {
                            break;
                        }
                    }

                    if !success {
                        Err(err).with_context(|| format!("[{table}] execute error and unable to auto choose a batch size limit"))?;
                    }
                } else if err_str.contains("0x0020") {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    stmt.execute()
                        .await
                        .with_context(|| format!("[table: {table}] insert error: {err}"))?;
                } else {
                    Err(err)
                        .with_context(|| format!("[table: {table}] insert error: {err_str}"))?;
                }
            } else {
                let rows = res.unwrap();
                metrics.add_success_blocks(1);
                metrics.add_written_rows(rows as _);
                metrics.add_written_points((rows * fields) as _);
            }

            if let Some(duration) = target_opts.interval {
                tokio::time::sleep(duration).await;
            } else {
                tokio::time::sleep(Duration::ZERO).await;
            }
        }
    }
    Ok(())
}

pub async fn sync_super_table_schema(
    from: &Taos,
    name: &str,
    to: &Taos,
    remap: Option<&Arc<HashMap<String, String>>>,
    target_opts: &TargetOpts,
    actions: &[Action],
) -> anyhow::Result<()> {
    debug_assert!(!name.is_empty());
    let (_, sql): ((), String) = from
        .query_one(format!("show create table `{name}`"))
        .await?
        .unwrap();
    let sql = sql
        .replace("VARCHAR", "BINARY")
        .replace("IF NOT EXISTS", "")
        .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        .replace("CREATE STABLE", "CREATE STABLE IF NOT EXISTS")
        .replace("create table", "CREATE TABLE IF NOT EXISTS")
        .replace("create stable", "CREATE STABLE IF NOT EXISTS")
        .replace("IF NOT EXISTS IF NOT EXISTS ", "IF NOT EXISTS ");

    let target_name: Cow<str> = if actions.is_empty() {
        name.into()
    } else {
        let mut target: Cow<str> = name.into();
        for action in actions {
            match action {
                Action::RenameTable(action) => {
                    target = action.apply(name)?.into();
                    break;
                }
                Action::RenameSuperTable(action) => {
                    target = action.apply(name)?.into();
                    break;
                }
                _ => (),
            }
        }
        target
    };

    let sql = transform_sql_with_actions(sql, name, actions, true, remap)?;

    loop {
        tracing::debug!(stable.sql = sql, stable.name = name, "sync schema");
        if let Err(err) = to.exec(&sql).await {
            let code: i32 = err.code().into();

            match code {
                0x000B => {
                    break;
                }
                0x032C => {
                    from.exec(format!("desc `{target_name}`")).await?;
                    continue;
                }
                0x03D3 => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
                0x2600 | 0x2601 => {
                    // 0x2600: Syntax error
                    // 0x2601: Incomplete SQL statement
                    sync_super_table_schema_only_fallback(from, name, to, remap, actions).await?;
                    break;
                }
                _ => {
                    Err(err).with_context(|| format!("sql: [{}] exec error", &sql))?;
                    break;
                }
            }
        } else {
            break;
        }
    }

    // Compare fields metadata and synchronize if not match.
    let desc = from.describe(name).await?;
    let target_desc = to.describe(&target_name).await?;
    let fields: BTreeMap<_, _> = target_desc.iter().map(|f| (f.field(), f)).collect();

    let desc_first = desc.first().context("Error data: empty fields")?;
    let target_desc_first = target_desc.first();
    // check if the first field is timestamp
    if desc_first.ty() == Ty::Timestamp {
        if let Some(target_desc_first) = target_desc_first {
            if !(target_desc_first.ty() == Ty::Timestamp
                && desc_first.field() == target_desc_first.field())
            {
                bail!(
                    "Mismatch the first field: expect `{:?}`, but got `{:?}`",
                    target_desc_first,
                    desc_first
                );
            }
        }
    } else {
        bail!(
            "Error data: expect timestamp as first field, but got `{}`",
            desc_first.ty()
        );
    }

    for l in desc.iter() {
        let r_name = remap.and_then(|m| m.get(l.field())).unwrap_or(&l.field);
        if let Some(r) = fields.get(&r_name.as_str()) {
            // check if the field is equal.

            if r.is_tag() != l.is_tag() {
                bail!("Target field is not match the source");
            }
            if r.ty() != l.ty() {
                warn!(
                    "Target field ({}) is not equal to source({})",
                    r.sql_repr(),
                    l.sql_repr()
                );
            } else if r.length() < l.length() {
                let c_or_t = if r.is_tag() { "TAG" } else { "COLUMN" };
                if let Err(err) = to
                    .exec(transform_sql_with_remap(
                        format!(
                            "ALTER TABLE `{}` MODIFY {} {}",
                            target_name,
                            c_or_t,
                            l.sql_repr(),
                        ),
                        remap,
                    ))
                    .await
                {
                    warn!(
                        "Modify column {} of table {target_name} error: {err:#}",
                        l.field()
                    );
                }
            }
        } else {
            // field does not exist in right side.
            let c_or_t = if l.is_tag() { "TAG" } else { "COLUMN" };
            if let Err(err) = to
                .exec(transform_sql_with_remap(
                    format!(
                        "ALTER TABLE `{}` ADD {} {}",
                        target_name,
                        c_or_t,
                        l.sql_repr(),
                    ),
                    remap,
                ))
                .await
            {
                warn!(
                    "Add column {} for table {target_name} error: {err:#}",
                    l.field()
                );
            }
        }
    }
    if let Some(duration) = target_opts.interval {
        tokio::time::sleep(duration).await;
    }
    Ok(())
}

pub async fn sync_super_table_schema_only_fallback(
    from: &Taos,
    name: &str,
    to: &Taos,
    remap: Option<&Arc<HashMap<String, String>>>,
    actions: &[Action],
) -> anyhow::Result<()> {
    debug_assert!(!name.is_empty());
    let desc = from.describe(name).await?;
    let sql = desc.to_create_table_sql(name);
    let target_name: Cow<str> = if actions.is_empty() {
        name.into()
    } else {
        let mut target: Cow<str> = name.into();
        for action in actions {
            match action {
                Action::RenameTable(action) => {
                    target = action.apply(name)?.into();
                    break;
                }
                Action::RenameSuperTable(action) => {
                    target = action.apply(name)?.into();
                    break;
                }
                _ => (),
            }
        }
        target
    };

    let sql = transform_sql_with_actions(sql, name, actions, true, remap)?;

    loop {
        tracing::info!("sync schema sql: {sql}");
        if let Err(err) = to.exec(&sql).await {
            let code: i32 = err.code().into();

            match code {
                0x000B => {
                    break;
                }
                0x032C => {
                    from.exec(format!("desc `{target_name}`")).await?;
                    continue;
                }
                _ => {
                    Err(err).with_context(|| format!("sql: [{}] exec error", &sql))?;
                    break;
                }
            }
        } else {
            break;
        }
    }
    Ok(())
}

pub async fn sync_super_table_schema_with_subs(
    from: &Taos,
    name: &str,
    subs: &[impl AsRef<str>],
    to: &Taos,
    remap: Option<&Arc<HashMap<String, String>>>,
    target_opts: &TargetOpts,
    is_v3: bool,
    actions: &[Action],
    metrics_arc: Arc<CoreMetrics>,
) -> anyhow::Result<()> {
    debug_assert!(!name.is_empty());
    let metrics = metrics_arc.legacy();
    let desc = from.describe(name).await?;
    let tag_name_vec = desc.tag_names().collect_vec();
    let tag_names = tag_name_vec.iter().map(|s| format!("`{s}`")).join(",");

    let cond_for_to = subs
        .iter()
        .map(|n| {
            format!(
                "'{}'",
                transform_tbname_with_actions(n.as_ref(), actions, false).unwrap()
            )
        })
        .join(",");

    let stable_name_for_to = transform_tbname_with_actions(name, actions, true)?;
    let sql = if is_v3 {
        format!("SELECT distinct tbname, {tag_names} FROM `{stable_name_for_to}` WHERE tbname IN ({cond_for_to})")
    } else {
        format!("SELECT tbname, {tag_names} FROM `{stable_name_for_to}` WHERE tbname IN ({cond_for_to})")
    };
    let res_to: HashMap<_, _> = to
        .query(transform_sql_with_remap(sql, remap))
        .await?
        .to_records()
        .await?
        .into_iter()
        .map(|mut v| (format!("{}", v.remove(0)), v))
        .collect();
    let (exists, non_exists): (Vec<_>, Vec<_>) =
        query_sub_tables_from_source(from, is_v3, subs, name, &tag_names)
            .await?
            .into_iter()
            .map(|mut v| (format!("{}", v.remove(0)), v))
            .partition(|v| res_to.contains_key(&v.0));
    if target_opts.update_tags {
        let mut updated_tags = 0;
        for (n, l) in &exists {
            let r = res_to.get(n).unwrap();

            for (tag, l, _r) in l
                .iter()
                .zip(r)
                .zip(&tag_name_vec)
                .filter_map(|((l, r), tag)| if l == r { None } else { Some((tag, l, r)) })
            {
                let sql = format!(
                    "alter table `{n}` set tag `{tag}` = {}",
                    l.to_sql_value_with_rfc3339()
                );
                let sql = transform_sql_with_remap(sql, remap);
                if let Err(err) = to.exec(&sql).await {
                    tracing::error!(
                        "Altering table `{n}` tag `{tag}` to {} error: {err:?}",
                        l.to_sql_value_with_rfc3339()
                    );
                } else {
                    updated_tags += 1;
                    metrics.total_updated_tags.fetch_add(1, Ordering::SeqCst);
                    metrics.updated_tags.fetch_add(1, Ordering::SeqCst);
                }
            }
        }

        tracing::info!("Totally updated {} tags in this chunk", updated_tags);
    }
    const MAX_SQL_LEN: usize = 1000 * 1000; // 800kb.
    let max_sql_length = target_opts.max_sql_length.unwrap_or(MAX_SQL_LEN);
    let mut tables = 0;
    let mut batch = 0;
    let mut sql = "CREATE TABLE".to_string();
    let new_stable_name = transform_tbname_with_actions(name, actions, true)?;
    for (child, row) in non_exists {
        let new_table_name = transform_tbname_with_actions(&child, actions, false)?;
        let tags = row
            .into_iter()
            .map(|v| v.to_sql_value_with_rfc3339())
            .join(",");
        // let tag_names = tag_name_vec.iter().map(|s| format!("`{s}`")).join(",");
        let e = transform_sql_with_remap(
            format!("  IF NOT EXISTS `{new_table_name}` USING `{new_stable_name}` ({tag_names}) TAGS({tags})"),
            remap,
        );
        batch += 1;
        tables += 1;

        if sql.len() + e.len() > max_sql_length {
            to.exec(&sql).await?;

            if let Some(duration) = target_opts.interval {
                tokio::time::sleep(duration).await;
            }

            tracing::debug!("Already created {} tables, {} in batch", tables, batch);
            sql = "CREATE TABLE".to_string();
            batch = 0;
        }
        sql.push_str(&e);
    }
    if tables > 0 {
        tracing::debug!("Create child tables with sql: {sql}");
        to.exec(&sql).await?;
        tracing::info!(
            "Created {} tables in stable {} in this chunk",
            tables,
            new_stable_name
        );
        metrics
            .total_created_tables
            .fetch_add(tables, Ordering::SeqCst);
        metrics.created_tables.fetch_add(tables, Ordering::SeqCst);
    }

    Ok(())
}

async fn query_sub_tables_from_source(
    from: &Taos,
    is_v3: bool,
    subs: &[impl AsRef<str>],
    name: &str,
    tag_names: &String,
) -> Result<Vec<Vec<Value>>, anyhow::Error> {
    if is_v3 {
        let cond = subs.iter().map(|n| format!("'{}'", n.as_ref())).join(",");
        let sql =
            format!("SELECT distinct tbname, {tag_names} FROM `{name}` WHERE tbname IN ({cond})");
        Ok(from.query(sql).await?.to_records().await?)
    } else {
        let mut sub_tables: Vec<Vec<Value>> = Vec::new();
        for sub in subs {
            let sql = format!("SELECT tbname, {tag_names} FROM `{}`", sub.as_ref());
            tracing::trace!(sql, "query sub tables from source");
            let result = from.query(sql).await;
            match result {
                Ok(mut rs) => {
                    let mut tmp = rs.to_records().await?;
                    sub_tables.append(&mut tmp);
                }
                Err(error) => {
                    tracing::warn!("select sub table {} error: {}", sub.as_ref(), error);
                }
            }
        }
        Ok(sub_tables)
    }
}

/// transform create sql based on actions
fn transform_sql_with_actions(
    sql: String,
    table_name: &str,
    actions: &[Action],
    is_stable: bool,
    remap: Option<&Arc<HashMap<String, String>>>,
) -> anyhow::Result<String> {
    let mut sql = transform_sql_with_remap(sql, remap);
    if actions.is_empty() {
        return Ok(sql);
    }
    if is_stable {
        for action in actions {
            match action {
                Action::Select(_) => {
                    bail!("unsupported transform action: {:?}", action)
                }
                Action::AddTag(action) => {
                    let len = match action.len {
                        0 => 100,
                        16374.. => 16374,
                        a => a,
                    };
                    sql.pop();
                    sql.push_str(&format!(", `{}` VARCHAR({}))", action.name, len));
                }
                Action::RenameTable(action) => {
                    let new = sql.replace(
                        &format!("`{table_name}`",),
                        &format!("`{}`", action.apply(table_name)?),
                    );
                    sql.clear();
                    sql.push_str(&new);
                }
                Action::RenameSuperTable(action) => {
                    let new = sql.replace(
                        &format!("`{table_name}`",),
                        &format!("`{}`", action.apply(table_name)?),
                    );
                    sql.clear();
                    sql.push_str(&new);
                }
                // Action::RenameReplaceWithRegex(action) => {
                //     let new = sql.replace(&format!("`{table_name}`",), &action.apply(table_name)?);
                //     sql.clear();
                //     sql.extend(new.chars());
                // }
                _ => (),
            }
        }
    } else {
        for action in actions {
            match action {
                Action::Select(_) => {
                    bail!("unsupported transform action: {:?}", action)
                }
                Action::RenameTable(action) => {
                    let new = action.apply(table_name)?;
                    if table_name != new {
                        sql = sql.replace(&format!("`{table_name}`",), &format!("`{}`", new));
                    }
                }
                Action::RenameChildTable(action) => {
                    let new = action.apply(table_name)?;
                    if table_name != new {
                        sql = sql.replace(&format!("`{table_name}`",), &format!("`{}`", new));
                    }
                }
                // Action::RenameReplaceWithRegex(action) => {
                //     let new = sql.replace(&format!("`{table_name}`",), &action.apply(table_name)?);
                //     sql.clear();
                //     sql.extend(new.chars());
                // }
                _ => (),
            }
        }
    }
    // tracing::debug!("sql transform after: {sql}");
    Ok(sql)
}

fn transform_sql_with_remap(
    mut sql: String,
    remap: Option<&Arc<HashMap<String, String>>>,
) -> String {
    if let Some(remap) = remap {
        for (l, r) in remap.iter() {
            sql = sql.replace(&format!("`{l}`"), &format!("`{r}`"));
        }
    }
    sql
}

fn transform_tbname_with_actions<'a>(
    table_name: &'a str,
    actions: &[Action],
    is_stable: bool,
) -> anyhow::Result<Cow<'a, str>> {
    if actions.is_empty() {
        return Ok(Cow::Borrowed(table_name));
    }
    let mut new_table_name = String::from(table_name);
    if is_stable {
        for action in actions {
            match action {
                Action::RenameTable(action) => {
                    new_table_name = action.apply(&new_table_name)?;
                }
                Action::RenameSuperTable(action) => {
                    new_table_name = action.apply(&new_table_name)?;
                }
                _ => (),
            }
        }
    } else {
        for action in actions {
            match action {
                Action::RenameTable(action) => {
                    new_table_name = action.apply(&new_table_name)?;
                }
                Action::RenameChildTable(action) => {
                    new_table_name = action.apply(&new_table_name)?;
                }
                _ => (),
            }
        }
    }
    if table_name != new_table_name {
        tracing::trace!("table name transform from {table_name} to: {new_table_name}");
    }
    Ok(new_table_name.into())
}

pub async fn sync_normal_table_schema(
    from: &Taos,
    name: &str,
    actions: &[Action],
    remap: Option<&Arc<HashMap<String, String>>>,
    to: &Taos,
) -> anyhow::Result<()> {
    tracing::info!("Sync normal table schema of {name}");
    let (_, sql): ((), String) = from
        .query_one(format!("show create table `{name}`"))
        .await
        .context("Show create table error")?
        .unwrap();
    // todo: here will produce error: [0x000B] Unable to establish connection
    let mut sql = sql
        .replace("VARCHAR", "BINARY")
        .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

    if let Err(err) = from.exec(&sql).await {
        if err.code() == 0x2600 || err.code() == 0x2601 {
            let desc = from.describe(name).await?;
            sql = desc.to_create_table_sql(name);
        }
    }

    sql = transform_sql_with_actions(sql, name, actions, false, remap)?;

    tracing::info!(sql, name, "sync normal table");
    if let Err(err) = to.exec(&sql).await {
        let code: i32 = err.code().into();

        match code {
            0x000B => {
                warn!("Cannot create table: {name} since {:#}", err);
            }
            0x2600 | 0x2601 => {
                sync_normal_table_schema_fallback(from, name, actions, remap, to).await?;
            }
            _ => {
                Err(err).with_context(|| format!("sql: [{}] exec error", &sql))?;
            }
        }
    }
    Ok(())
}

async fn sync_normal_table_schema_fallback(
    from: &Taos,
    name: &str,
    actions: &[Action],
    remap: Option<&Arc<HashMap<String, String>>>,
    to: &Taos,
) -> anyhow::Result<()> {
    tracing::info!("Sync normal table schema of {name}");
    let desc = from.describe(name).await?;
    let mut sql = desc.to_create_table_sql(name);

    sql = transform_sql_with_actions(sql, name, actions, false, remap)?;
    if let Err(err) = to.exec(&sql).await {
        if !err.to_string().contains("[0x000B]") {
            Err(err).with_context(|| format!("normal table create error, sql: [{sql}]"))?;
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct STableRecord {
    name: String,
    #[allow(dead_code)]
    tables: usize,
}

#[serde_as]
#[derive(Debug, Deserialize)]
struct TableRecord {
    table_name: String,
    #[serde_as(as = "serde_with::NoneAsEmptyString")]
    stable_name: Option<String>,
    #[serde(rename = "vgId")]
    vgroup_id: u32,
}

impl TableRecord {
    #[allow(dead_code)]
    fn is_normal_table(&self) -> bool {
        self.stable_name
            .as_deref()
            .map(|s| s.is_empty())
            .unwrap_or(true)
    }
}

#[instrument(skip_all)]
#[async_backtrace::framed]
async fn sync_schema(
    scheduler: &Scheduler,
    todo: &Arc<LegacyTodo>,
    concurrency: usize,
) -> anyhow::Result<()> {
    // Step B: sync STables

    // tasks listener
    let mut todo_join_set = JoinSet::new();
    let todo_sender = scheduler.sender.clone();
    let mut readers = futures::stream::FuturesUnordered::new();
    todo.stables
        .scan_async(|stable| {
            let (sender, reader) = oneshot::channel();
            let stable = stable.clone();
            let todo_sender = todo_sender.clone();
            todo_join_set
                .spawn(async move { todo_sender.send_async(Todo::STable(stable, sender)).await });
            readers.push(reader);
        })
        .await;

    let mut tables = 0;
    while let Some(res) = todo_join_set.join_next().await {
        tables += 1;
        if let Err(err) = res {
            tracing::error!("Send  error: {err:#}",);
        }
    }
    if tables > 0 {
        tracing::info!("Sent {tables} stable tasks");
    }

    // wait for all stable tasks done
    while readers.try_next().await?.transpose()?.is_some() {}

    if !todo.stables.is_empty() {
        info!(tables, "STables syncing done");
    }
    if todo.tables.is_empty() {
        tracing::info!("No tables to sync");
        return Ok(());
    }
    tracing::info!("Sent {tables} tables task with {concurrency} workers");

    // Step C: sync tables

    let chunk_size = 400;
    let mut chunk_num = 0;
    let mut chunks = Vec::with_capacity(chunk_size);
    let mut stable = None;

    let mut tables_join_set = JoinSet::new();
    // let mut readers = Vec::new();

    todo.tables
        .scan_async(|table| {
            if table.stable.is_none() {
                // Send ordinary table task
                let (sender, reader) = oneshot::channel();
                let table = table.table.as_ref().clone();
                let todo_sender = todo_sender.clone();
                todo_join_set.spawn(async move {
                    todo_sender
                        .send_async(Todo::Meta(None, vec![table], Some(sender)))
                        .await
                });
                tables_join_set.spawn(async move { reader.await.map(|_| 1usize) });
            } else if stable == table.stable {
                // Push table to chunk
                chunks.push(table.table.as_ref().clone());
                if chunks.len() == chunk_size {
                    // Send chunk only when it's full
                    let (sender, reader) = oneshot::channel();
                    let tables = chunks.drain(..).collect_vec();
                    let stable = stable.clone();

                    tracing::debug!(tables = tables.len(), chunk.id = chunk_num, "Send chunk");

                    let todo_sender = todo_sender.clone();
                    todo_join_set.spawn(async move {
                        todo_sender
                            .send_async(Todo::Meta(stable, tables, Some(sender)))
                            .await
                    });
                    tables_join_set.spawn(async move { reader.await.map(|_| chunk_size) });

                    chunk_num += 1;
                }
            } else {
                if !chunks.is_empty() {
                    let (sender, reader) = oneshot::channel();
                    let tables = chunks.drain(..).collect_vec();
                    let stable = stable.clone();
                    let len = tables.len();

                    let todo_sender = todo_sender.clone();
                    todo_join_set.spawn(async move {
                        todo_sender
                            .send_async(Todo::Meta(stable, tables, Some(sender)))
                            .await
                    });
                    tables_join_set.spawn(async move { reader.await.map(|_| len) });
                    chunk_num += 1;
                }
                stable = table.stable.clone();
                chunks.push(table.table.as_ref().clone());
            }
        })
        .await;

    if !chunks.is_empty() {
        let (sender, reader) = oneshot::channel();
        let tables = chunks.drain(..).collect_vec();
        let stable = stable.clone();
        let len = tables.len();

        todo_join_set.spawn(async move {
            todo_sender
                .send_async(Todo::Meta(stable, tables, Some(sender)))
                .await
        });
        tables_join_set.spawn(async move { reader.await.map(|_| len) });
    }

    let mut tables_tasks = 0;
    // wait for all stable tasks sent
    while let Some(res) = todo_join_set.join_next().await {
        tables_tasks += 1;
        if let Err(err) = res {
            tracing::error!("Send  error: {err:#}",);
        }
    }
    tracing::info!(tables_tasks, "Sent all table tasks");

    // wait for all table tasks done
    let mut fails = 0;
    let mut completed = 0;
    let total = todo.tables.len();
    let dot = (total / 100).max(1);
    // wait for all stable tasks done
    while let Some(v) = tables_join_set.join_next().await.transpose()? {
        match v {
            Ok(num) => {
                completed += num;
                if completed % dot == 0 {
                    if fails == 0 {
                        tracing::info!(
                            "Synchronized {:.2}% of tables ({} of {}) for schema.",
                            completed as f64 * 100.0 / total as f64,
                            completed,
                            total,
                        )
                    } else {
                        tracing::info!(
                            "Synchronized {:.2}% of tables ({} of {}) for schema, {} failed.",
                            completed as f64 * 100.0 / total as f64,
                            completed,
                            total,
                            fails,
                        );
                    }
                }
            }
            Err(err) => {
                tracing::error!(completed, total, fails, "Error: {err:#}",);
                fails += 1;
            }
        }
    }

    tracing::info!("Synchronizing {completed} tables metadata with {concurrency} workers finished");
    Ok(())
}

#[instrument(skip_all)]
async fn sync_specified_tables_with_workers(
    scheduler: &Scheduler,
    from: &TaosPool,
    mut opts: QueryOpts,
    todo: &Arc<LegacyTodo>,
    target_opts: TargetOpts,
    workers: usize,
    task_id: &Option<String>,
) -> anyhow::Result<()> {
    tracing::info!(
        tables = todo.tables_todo(),
        task_id,
        "Synchronize table data with {} workers",
        workers
    );
    let mut count = 0;
    let (tx, rx) = flume::unbounded::<(
        Option<(Arc<String>, TimeRange)>,
        oneshot::Receiver<anyhow::Result<()>>,
    )>();
    let task_id = task_id.clone().map(Arc::new);
    let task_id_cloned = task_id.clone();
    let breakpoints = scheduler.breakpoints();
    let handle = tokio::spawn(async move {
        let mut fails = 0;
        let task_id = task_id_cloned;
        while let Ok((sparse, reader)) = rx.recv_async().await {
            count += 1;
            match reader.await? {
                Ok(_) => {
                    if let Some((table, time_range)) = sparse {
                        // set breakpoint async
                        if let Some(breakpoints) = breakpoints.as_ref() {
                            if let Some(end) = time_range.end {
                                let breakpoint = end.to_string();
                                if let Err(err) = breakpoints.set(&table, &breakpoint).await {
                                    tracing::warn!(
                                        task.id = task_id.as_deref(),
                                        "Set breakpoint error: {err:#}"
                                    );
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(task.id = task_id.as_deref(), "Syncing error: {err:#}",);
                    fails += 1;
                    if target_opts.fails_to.is_none() {
                        return Err(err);
                    }
                }
            }
        }
        if fails > 0 {
            tracing::info!(
                "Synchronizing {count} tables with {workers} workers finished, {fails} failed"
            );
        } else {
            tracing::info!("Synchronizing {count} tables with {workers} workers finished");
        }
        anyhow::Ok(())
    });
    let from = from.get().await?;
    let (items_tx, items_rx) = flume::bounded(1024);
    let todo = todo.clone();
    tokio::task::spawn(
        async move {
            tracing::info!(tables = todo.tables.len(), "Scanning new tables ...");
            let mut scanned = std::collections::HashSet::new();
            let mut futures = futures::stream::FuturesUnordered::new();
            todo.tables
                .scan_async(|item: &LegacyTableItem| {
                    if scanned.contains(item) {
                        tracing::warn!("table {} is already scanned.", item.table);
                        return;
                    }
                    scanned.insert(item.clone());
                    futures.push(items_tx.send_async(item.clone()));
                })
                .await;

            while let Some(res) = futures.next().await {
                if let Err(err) = res {
                    tracing::trace!("Send table error: {err:#}",);
                }
            }
            tracing::info!(tables = todo.tables.len(), "Scanning tables done");
        }
        .in_current_span(),
    );

    let breakpoints = scheduler.breakpoints_ref();
    while let Ok(item) = items_rx.recv_async().await {
        let stable = &item.stable;
        let table = &item.table;

        if item.mtlf {
            // get breakpoints use breakpoints_get
            if let Some(breakpoints) = breakpoints {
                const MAX_RETRIES: usize = 5;
                let mut retries = MAX_RETRIES;
                loop {
                    match breakpoints.get(table).await.and_then(|bp| {
                        bp.map(|bp| bp.parse::<DateTime<Utc>>().context("Parse datetime error"))
                            .transpose()
                    }) {
                        Ok(Some(breakpoint)) => {
                            opts.time_range.start = Some(breakpoint);
                            tracing::debug!(
                                "load breakpoint success set time_range: {} table: {table}",
                                opts.time_range
                            );
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
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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

            let chunks = split_table_into_time_range_chunks(&from, table, &opts).await?;
            for chunk in chunks {
                let (sender, reader) = oneshot::channel();
                scheduler
                    .send(Todo::Sparse(item.table.clone(), chunk, Some(sender)))
                    .await?;
                tx.send_async((Some((item.table.clone(), chunk)), reader))
                    .await?;
            }
        } else {
            let (sender, reader) = oneshot::channel();
            scheduler
                .send(Todo::Data(
                    stable.clone(),
                    item.table.clone(),
                    opts.time_range,
                    Some(sender),
                ))
                .await?;
            tx.send_async((None, reader)).await?;
        }
    }
    drop(tx); // drop tx to close rx
    drop(items_rx); // drop items_rx to close items_tx
    handle.await??; // wait for rx handle
    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
pub enum SyncMode {
    /// Synchronize history data as it currently is (at this query time).
    #[default]
    AsIs,
    /// Synchronize (almost) realtime series and run forever (like a service).
    Realtime,
    /// Synchronize all data, include both historical or realtime data.
    ///
    /// It means sync history data, monitor the latest, and run forever.
    All,
}
impl FromStr for SyncMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "" | "history" | "asis" | "as-is" => Ok(Self::AsIs),
            "realtime" => Ok(Self::Realtime),
            "all" => Ok(Self::All),
            _ => bail!("Invalid schema mode: {s}"),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SourceOpts {
    /// SQL query options.
    query: QueryOpts,
    /// Create database automatically if not exists.
    assert: bool,
    schema: SchemaMode,
    mode: SyncMode,
    table: TableOpts,
    forever: bool,
    tables: Option<Vec<String>>,
    /// Specified stables to sync.
    stables: Option<Vec<String>>,
    /// The concurrent workers number for query.
    workers: usize,
    /// Shuffle the tables before sync to query different vgroups at one time.
    shuffle: bool,
    /// Enable sparse mode for multiple tables low frequency data.
    sparse: bool,
    /// Retrieve schema per interval.
    schema_polling_interval: Duration,
    /// Sleep before stop polling schema.
    schema_polling_wait_before_end: Option<Duration>,
    /// The overall maximum concurrency for writing to the target database.
    /// This option will affect the concurrent_limit in TargetOpts.
    write_concurrency: Option<usize>,
    /// This option will overwrite the same option in TargetOpts.
    fails_to: Option<String>,
}

impl SourceOpts {
    pub fn from_params(dsn: &mut Dsn) -> anyhow::Result<Self> {
        let mut opts = Self::default();
        if let Some(schema) = dsn.remove("schema") {
            opts.schema = schema.parse()?;
        }

        if let Some(assert) = dsn.remove("assert") {
            match assert.as_str() {
                "false" => opts.assert = false,
                "" | "true" => opts.assert = true,
                _ => anyhow::bail!("assert in source dsn should be only empty, or true/false"),
            }
        }
        if let Some(value) = dsn.remove("forever") {
            match value.as_str() {
                "false" => opts.forever = false,
                "" | "true" => opts.forever = true,
                _ => anyhow::bail!("forever in source dsn should be only empty, or true/false"),
            }
        }
        if let Some(limit) = dsn.remove("limit") {
            let limit: u32 = limit.parse()?;
            opts.query.limit.limit = limit;
        }
        if let Some(offset) = dsn.remove("offset") {
            let offset: u32 = offset.parse()?;
            opts.query.limit.offset.replace(offset);
        }
        if let Some(value) = dsn.remove("workers") {
            let value: usize = value.parse()?;
            opts.workers = value;
        }

        if let Some(value) = dsn.remove("start") {
            let value = DateTime::<Utc>::from_str(&value)?;
            opts.query.time_range.start.replace(value);
        }
        if let Some(value) = dsn.remove("end") {
            let value = DateTime::<Utc>::from_str(&value)?;
            opts.query.time_range.end.replace(value);
        }
        if let Some(value) = dsn.remove("unit") {
            let value = parse_duration::parse(&value).map_err(|err| {
                anyhow::format_err!(
                    "Can not parse duration for `unit` from value: {value} (Error: {err})"
                )
            })?;
            opts.query.unit = value;
        }

        if let Some(value) = dsn.remove("smooth-init") {
            let value = parse_duration::parse(&value).map_err(|err| {
                anyhow::format_err!(
                    "Can not parse duration for `smooth-init` from value: {value} (Error: {err})"
                )
            })?;
            opts.query.smooth_init = value;
        } else {
            opts.query.smooth_init = Duration::ZERO;
        }

        if let Some(value) = dsn.remove("mode") {
            let value = SyncMode::from_str(&value)?;
            opts.mode = value;
        }

        if let Some(value) = dsn.remove("tables") {
            let (files, mut tables): (Vec<_>, Vec<_>) = value
                .split(",")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .partition(|v| v.starts_with('@'));

            for file in files {
                let f = std::fs::File::open(&file[1..])?;
                let buf = std::io::BufReader::new(f);
                use std::io::prelude::*;
                #[allow(clippy::lines_filter_map_ok)]
                tables.extend(buf.lines().filter_map(Result::ok));
            }

            if !tables.is_empty() {
                opts.tables = Some(tables);
            }
        }
        if let Some(value) = dsn.remove("select-from-stable") {
            opts.query.select_from_stable = value != "false";
        }
        if let Some(value) = dsn.remove("shuffle") {
            opts.shuffle = value != "false";
        }
        if let Some(value) = dsn.remove("sparse") {
            opts.sparse = value != "false";
        }

        // schema_polling_interval
        if let Some(value) = dsn.remove("schema-polling-interval") {
            let value = parse_duration::parse(&value).map_err(|err| {
                anyhow::format_err!(
                    "Can not parse duration for `schema-polling-interval` from value: {value} (Error: {err})"
                )
            })?;
            opts.schema_polling_interval = value;
        } else {
            // default 1m=60s
            opts.schema_polling_interval = Duration::from_secs(60);
        }
        // schema_polling_wait_before_end
        if let Some(value) = dsn.remove("schema-polling-wait-before-end") {
            let value = parse_duration::parse(&value).map_err(|err| {
                anyhow::format_err!(
                    "Can not parse duration for `schema-polling-wait-before-end` from value: {value} (Error: {err})"
                )
            })?;
            opts.schema_polling_wait_before_end.replace(value);
        }

        // stables
        if let Some(value) = dsn.remove("stables") {
            let (files, mut tables): (Vec<_>, Vec<_>) = value
                .split(",")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .partition(|v| v.starts_with('@'));

            for file in files {
                let f = std::fs::File::open(&file[1..])?;
                let buf = std::io::BufReader::new(f);
                use std::io::prelude::*;
                #[allow(clippy::lines_filter_map_ok)]
                tables.extend(buf.lines().filter_map(|l| l.ok()));
            }
            if !tables.is_empty() {
                opts.stables = Some(tables);
            }
        }
        opts.table = TableOpts::from_params(dsn)?;

        // write_concurrency
        if let Some(value) = dsn.remove("write-concurrency") {
            let value: usize = value
                .parse()
                .with_context(|| format!("invalid write-concurrency value: {value}"))?;
            opts.write_concurrency = Some(value);
        }
        // fails_to
        if let Some(value) = dsn.remove("fails-to") {
            opts.fails_to.replace(value);
        }

        Ok(opts)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub enum SchemaMode {
    None,
    Only,
    #[default]
    Always,
}

impl SchemaMode {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    pub fn todo(&self) -> bool {
        !self.is_none()
    }
}

impl FromStr for SchemaMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "" | "always" | "true" => Ok(Self::Always),
            "false" | "none" => Ok(Self::None),
            "only" => Ok(Self::Only),
            _ => bail!("Invalid schema mode: {s}"),
        }
    }
}
#[derive(Debug, Clone)]
pub struct TargetOpts {
    assert: bool,
    schema: SchemaMode,
    database_options: Option<String>,
    batch_size: Option<usize>,
    interval: Option<Duration>,
    max_sql_length: Option<usize>,
    force_stmt: bool,
    fails_to: Option<Arc<tokio::sync::Mutex<std::fs::File>>>,
    timeout_per_table: Option<Duration>,
    update_tags: bool,
    concurrent_limit: NonZeroUsize,
    blocks_chunk_size: NonZeroUsize,
    /// Remap the field name to another.
    ///
    /// A map of table name to another map of field name to another.
    remap: Option<HashMap<String, Arc<HashMap<String, String>>>>,
}

impl Default for TargetOpts {
    fn default() -> Self {
        Self {
            assert: false,
            schema: SchemaMode::default(),
            database_options: None,
            batch_size: None,
            interval: None,
            max_sql_length: None,
            force_stmt: false,
            fails_to: None,
            timeout_per_table: None,
            update_tags: false,
            concurrent_limit: NonZeroUsize::new(1).unwrap(),
            blocks_chunk_size: NonZeroUsize::new(1).unwrap(),
            remap: None,
        }
    }
}

impl Drop for TargetOpts {
    fn drop(&mut self) {
        if let Some(file) = self.fails_to.take() {
            tokio::task::spawn(async move {
                let _ = file.lock().await.flush();
            });
        }
    }
}

impl TargetOpts {
    /// 从 from DSN 和 to DSN 获取最终的 TargeOpts。
    /// 在 taosX 1.6.0 之前，TargetOpts 对应 to DSN， 但是 taosX 1.6.0 版本将某些与目标端相关的高级选项加入到了 from DSN 中，因此 TargetOpts 也需要参考 from DSN。
    pub fn from_params(
        source_opts: &SourceOpts,
        to_dsn: &mut Dsn,
        workers: usize,
    ) -> anyhow::Result<Self> {
        let mut opts = Self::default();
        if let Some(value) = to_dsn.remove("schema") {
            opts.schema = value.parse().with_context(|| {
                format!(
                    "invalid schema value: {value}, \
                    only always|true, none|false, or only is supported "
                )
            })?;
        }

        if let Some(assert) = to_dsn.remove("assert") {
            match assert.as_str() {
                "false" => opts.assert = false,
                "" | "true" => opts.assert = true,
                _ => anyhow::bail!(
                    "assert in target dsn should be only empty, or true/false (default is false)"
                ),
            }
        }

        if let Some(value) = to_dsn.remove("database-options") {
            opts.database_options.replace(value);
        }
        if let Some(value) = to_dsn.remove("batch-size") {
            opts.batch_size.replace(
                value
                    .parse()
                    .with_context(|| format!("invalid batch-size value: {value}"))?,
            );
        }

        if let Some(value) = to_dsn.remove("concurrent-limit") {
            opts.concurrent_limit = value
                .parse()
                .with_context(|| format!("invalid concurrent-limit value: {value}"))?;
        }
        if let Some(write_concurrency) = source_opts.write_concurrency {
            // write-concurrency 是最大整体写并发，这里需要把它换算成 concurrent-limit
            let concurrent_limit = if write_concurrency != 0 && write_concurrency % workers == 0 {
                write_concurrency / workers
            } else {
                write_concurrency / workers + 1
            };
            opts.concurrent_limit = NonZeroUsize::new(concurrent_limit)
                .with_context(|| format!("invalid concurrent-limit value: {concurrent_limit}"))?;
            tracing::info!(
                "concurrent-limit is set to {concurrent_limit} based on write-concurrency {write_concurrency} and workers {workers}"
            );
        }

        if let Some(value) = to_dsn.remove("blocks-chunk-size") {
            opts.blocks_chunk_size = value
                .parse()
                .with_context(|| format!("invalid blocks-chunk-size value: {value}"))?;
        }
        if let Some(value) = to_dsn.remove("interval") {
            let value = parse_duration::parse(&value)?;
            opts.interval.replace(value);
        }
        if let Some(value) = to_dsn.remove("max-sql-length") {
            opts.max_sql_length.replace(value.parse()?);
        }
        if to_dsn.remove("force-stmt").is_some() {
            opts.force_stmt = true;
        }

        let mut fails_to = to_dsn.remove("fails-to");
        if fails_to.is_none() {
            fails_to = source_opts.fails_to.clone();
        }
        if let Some(value) = fails_to {
            let value = Path::new(&value);
            let file = std::fs::File::create(value)?;

            opts.fails_to
                .replace(Arc::new(tokio::sync::Mutex::new(file)));
        }

        if let Some(value) = to_dsn.remove("timeout-per-table") {
            let value = parse_duration::parse(&value)?;
            opts.timeout_per_table.replace(value);
        }
        if let Some(v) = to_dsn.remove("update-tags") {
            if v != "false" {
                opts.update_tags = true;
            }
        }

        if let Some(value) = to_dsn.remove("remap") {
            let in_lines = value.split(",").filter_map(|s| {
                if let Some((table, from, to)) = s.split("::").collect_tuple() {
                    Some((
                        table.trim().to_string(),
                        (from.trim().to_string(), to.trim().to_string()),
                    ))
                } else {
                    None
                }
            });
            opts.remap.replace(
                value
                    .split(",")
                    .filter_map(|s| {
                        s.strip_prefix('@').map(|s| {
                            std::fs::File::open(s)
                                .context("open remap file error")
                                .map(|f| {
                                    csv_lib::ReaderBuilder::new()
                                        .has_headers(false)
                                        .flexible(true)
                                        .from_reader(f)
                                })
                        })
                    })
                    .map_ok(|mut buf| {
                        let iter = buf.records();
                        iter.filter_map(|l| {
                            if let Ok(l) = l {
                                return l.iter().take(3).collect_tuple().map(
                                    |(table, from, to)| {
                                        (
                                            table.trim().to_string(),
                                            (from.trim().to_string(), to.trim().to_string()),
                                        )
                                    },
                                );
                            }
                            None
                        })
                        .collect_vec()
                    })
                    .flatten_ok()
                    .try_collect::<_, Vec<_>, _>()?
                    .into_iter()
                    .chain(in_lines)
                    .chunk_by(|(table, _)| table.clone())
                    .into_iter()
                    .map(|(group, v)| {
                        let map: HashMap<_, _> = v.map(|(_, v)| v).collect();
                        (group.to_string(), Arc::new(map))
                    })
                    .collect(),
            );
        }
        Ok(opts)
    }
}

pub struct TableTodo {
    name: Arc<String>,
    stable: Option<Arc<String>>,
}

impl TableTodo {
    pub fn new(name: impl Into<Arc<String>>, stable: Option<impl Into<Arc<String>>>) -> Self {
        Self {
            name: name.into(),
            stable: stable.map(Into::into),
        }
    }
    pub fn is_ordinary_table(&self) -> bool {
        self.stable.is_none()
    }
    pub fn is_child_of_stable(&self) -> bool {
        self.stable.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct LegacyTodo {
    stables: scc::HashSet<Arc<String>>,
    tables: scc::HashSet<LegacyTableItem>,
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct LegacyTableItem {
    vgroup_id: u32,
    stable: Option<Arc<String>>,
    table: Arc<String>,
    mtlf: bool,
}

impl std::hash::Hash for LegacyTableItem {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.stable.hash(state);
        self.table.hash(state);
    }
}

impl LegacyTableItem {
    pub fn new(vgroup_id: u32, stable: Option<Arc<String>>, table: Arc<String>) -> Self {
        Self {
            vgroup_id,
            stable,
            table,
            mtlf: false,
        }
    }

    /// Create a mtlf stable item. The table must be a stable name.
    pub fn new_mtlf(vgroup_id: u32, table: Arc<String>) -> Self {
        Self {
            vgroup_id,
            stable: Some(table.clone()),
            table,
            mtlf: true,
        }
    }

    pub fn is_ordinary_table(&self) -> bool {
        self.stable.is_none()
    }
}
impl Default for LegacyTodo {
    fn default() -> Self {
        Self::new()
    }
}

impl LegacyTodo {
    pub fn tables_todo(&self) -> usize {
        self.tables.len()
    }

    pub fn new() -> Self {
        Self {
            stables: Default::default(),
            tables: Default::default(),
        }
    }
}

type TodoListRef = Arc<LegacyTodo>;

/// The todo list for the scheduler.
#[instrument(skip_all)]
#[async_backtrace::framed]
pub async fn update_todo_list(
    pool: &TaosPool,
    opts: &SourceOpts,
    todo: TodoListRef,
) -> anyhow::Result<LegacyTodo> {
    async fn tables_from_vec<T>(tables: Vec<T>) -> scc::HashSet<T>
    where
        T: std::hash::Hash + Eq,
    {
        let set = scc::HashSet::with_capacity(tables.len());
        for item in tables {
            let _ = set.insert_async(item).await;
        }
        set
    }
    // let version =
    let taos = pool
        .get()
        .await
        .context("Connect to data source error with timeout")?;
    let version = taos.server_version().await?;
    let is_v2 = version.starts_with('2');
    debug!(version = version.as_ref(), "updating table list...");
    // dbg!(&opts.stables);
    if let Some(stables) = opts.stables.as_ref() {
        const MAX_DISPLAY_STABLES: usize = 5;
        let list = if stables.len() > MAX_DISPLAY_STABLES {
            format!("{},...", stables.iter().take(MAX_DISPLAY_STABLES).join(","))
        } else {
            stables.iter().take(MAX_DISPLAY_STABLES).join(",")
        };
        tracing::info!("Use stables list in data source parameters: {list}");

        // Get or update the stables list.

        let stables: Vec<_> = futures::stream::iter(stables.iter())
            .then(|s| {
                todo.stables
                    .read_async(s, Clone::clone)
                    .map(|stable| stable.unwrap_or_else(|| Arc::new(s.to_string())))
                // .unwrap_or_else(|| Arc::new(s.to_string()))
            })
            .collect()
            .await;

        let tables = scc::HashSet::new();
        if opts.sparse {
            // Sparse mode only need to sync the stables.
            for stable in &stables {
                if !todo.stables.contains_async(stable).await {
                    let _ = todo.stables.insert_async(stable.clone()).await;
                    let _ = todo
                        .tables
                        .insert_async(LegacyTableItem::new_mtlf(0, stable.clone()))
                        .await;
                    let _ = tables
                        .insert_async(LegacyTableItem::new_mtlf(0, stable.clone()))
                        .await;
                }
            }
            return Ok(LegacyTodo {
                stables: tables_from_vec(stables).await,
                tables,
            });
        }
        if is_v2 {
            for stable in &stables {
                let mut res = taos
                    .query(format!("select tbname from `{}`", stable))
                    .await?;
                let mut stream = res.deserialize().map_ok(|table_name| {
                    LegacyTableItem::new(0, Some(stable.clone()), Arc::new(table_name))
                });

                while let Some(table) = stream.try_next().await? {
                    if !todo.tables.contains_async(&table).await {
                        let _ = tables.insert_async(table.clone()).await;
                        let _ = todo.tables.insert_async(table).await;
                    }
                }
            }
        } else {
            // is v3
            let database: String = taos.query_one("SELECT database()").await?.unwrap();
            // note!: to make sure the information_schema is updated.
            taos.exec("use information_schema").await?;
            taos.exec(format!("use `{database}`")).await?;
            for stable in &stables {
                let mut set = taos
                    .query(format!("select vgroup_id, table_name from information_schema.ins_tables where db_name = '{}' and stable_name = '{}'", database, stable)).await?;
                let mut stream = set.deserialize().map_ok(|(vgroup_id, name)| {
                    LegacyTableItem::new(vgroup_id, Some(stable.clone()), Arc::new(name))
                });
                while let Some(table) = stream.try_next().await? {
                    if !todo.tables.contains_async(&table).await {
                        let _ = tables.insert_async(table.clone()).await;
                        let _ = todo.tables.insert_async(table).await;
                    }
                }
            }
        }
        tracing::info!(
            "Try to synchronize {} tables in {} stables",
            tables.len(),
            stables.len()
        );

        Ok(LegacyTodo {
            stables: tables_from_vec(stables).await,
            tables,
        })
    } else if let Some(tables) = opts.tables.as_ref() {
        // 有 tables 选项情况下，只需初始化一次
        if !todo.tables.is_empty() {
            return Ok(LegacyTodo::new());
        }
        let stables = scc::HashSet::new();

        let mut table_items = Vec::with_capacity(tables.len());

        for s in tables.iter() {
            let item = if let Some((stable_name, table)) = s.split_once('.') {
                if let Some(stable) = stables
                    .read_async(&stable_name.to_string(), Clone::clone)
                    .await
                {
                    LegacyTableItem::new(0, Some(stable), Arc::new(table.to_string()))
                } else {
                    let stable = Arc::new(stable_name.to_string());
                    let _ = stables.insert_async(stable.clone()).await;
                    LegacyTableItem::new(0, Some(stable.clone()), Arc::new(table.to_string()))
                }
            } else {
                LegacyTableItem::new(0, None, Arc::new(s.to_string()))
            };
            table_items.push(item);
        }

        for LegacyTableItem { stable, table, .. } in
            &mut table_items.iter_mut().filter(|s| s.is_ordinary_table())
        {
            if is_v2 {
                let table_record: Option<TableRecord> = taos
                    .query_one(format!("show tables like '{table}'"))
                    .await?;
                if let Some(record) = table_record {
                    *stable = record.stable_name.map(Arc::new);
                } else {
                    tracing::warn!("Table todo not found: {table}");
                }
            } else {
                let database: String = taos.query_one("SELECT database()").await?.unwrap();
                // note!: to make sure the information_schema is updated.
                taos.exec("use information_schema").await?;
                taos.exec(format!("use `{database}`")).await?;
                if let Some(stable_name) = taos.query_one::<_, Option<String>>(format!("select stable_name from information_schema.ins_tables where db_name = '{database}' and table_name = '{table}'")).await? {
                    if let Some(stable_name) = stable_name {
                        if let Some(arc) = stables.read_async(&stable_name, Clone::clone).await {
                            stable.replace(arc);
                        } else {
                            stable.replace(Arc::new(stable_name));
                        }
                    }
                } else {
                    tracing::warn!("Table todo not found: {table}");
                }
            }
        }
        tracing::info!(
            "Try to synchronize {} tables in {} stables",
            tables.len(),
            stables.len()
        );
        Ok(LegacyTodo {
            stables,
            tables: tables_from_vec(table_items).await,
        })
    } else {
        #[allow(clippy::collapsible_else_if)]
        if version.starts_with('2') {
            let mut res = taos
                .query("SHOW STABLES")
                .await
                .context("Get stable list from source")?;
            let stables: Vec<Arc<String>> = res
                .deserialize()
                .map_ok(|stable: STableRecord| Arc::new(stable.name))
                .try_collect()
                .await
                .context("Deserialize stable list from source error")?;

            let mut tables = Vec::new();
            if opts.sparse {
                // Sparse stables
                let show = stables
                    .iter()
                    .map(|stable| LegacyTableItem::new_mtlf(0, stable.clone()))
                    .collect_vec();
                for table in show {
                    if !todo.tables.contains_async(&table).await {
                        tables.push(table.clone());
                        let _ = todo.tables.insert_async(table).await;
                    }
                }

                // Ordinary tables
                let mut set = taos
                    .query("show tables")
                    .await
                    .context("Show tables error in sparse mode")?;
                let mut stream = set.deserialize::<TableRecord>().try_filter_map(
                    |TableRecord {
                         table_name,
                         stable_name,
                         vgroup_id,
                     }| {
                        let filter = if stable_name.is_some() {
                            None
                        } else {
                            Some(LegacyTableItem::new(vgroup_id, None, Arc::new(table_name)))
                        };
                        futures::future::ready(Ok(filter))
                    },
                );
                while let Some(table) = stream
                    .try_next()
                    .await
                    .context("Deserialize stable list from source error")?
                {
                    if !todo.tables.contains_async(&table).await {
                        tables.push(table.clone());
                        let _ = todo.tables.insert(table);
                    }
                }
            } else {
                let mut set = taos
                    .query("show tables")
                    .await
                    .context("Show tables error")?;
                let mut stream = set.deserialize::<TableRecord>().map_ok(
                    |TableRecord {
                         table_name,
                         stable_name,
                         vgroup_id,
                     }| {
                        if let Some(stable_name) = stable_name {
                            if let Some(stable) = todo.stables.read(&stable_name, |s| s.clone()) {
                                LegacyTableItem::new(vgroup_id, Some(stable), Arc::new(table_name))
                            } else {
                                let stable = Arc::new(stable_name.clone());
                                // stables.push(stable.clone());
                                todo.stables.insert(stable.clone()).unwrap();
                                LegacyTableItem::new(vgroup_id, Some(stable), Arc::new(table_name))
                            }
                        } else {
                            LegacyTableItem::new(vgroup_id, None, Arc::new(table_name))
                        }
                    },
                );

                while let Some(table) = stream
                    .try_next()
                    .await
                    .context("Deserialize stable list from source error")?
                {
                    if !todo.tables.contains_async(&table).await {
                        tables.push(table.clone());
                        let _ = todo.tables.insert_async(table).await;
                    }
                }
            };

            if opts.shuffle {
                tables.shuffle(&mut rand::thread_rng());
            }

            info!(
                version = version.as_ref(),
                tables = tables.len(),
                stables = stables.len(),
                "Try to synchronize {} tables in {} stables and {} ordinary tables",
                tables.len(),
                stables.len(),
                0
            );
            Ok(LegacyTodo {
                stables: tables_from_vec(stables).await,
                tables: tables_from_vec(tables).await,
            })
        } else {
            let database: String = taos.query_one("SELECT database()").await?.unwrap();
            let mut stables = Vec::new();
            // note!: to make sure the information_schema is updated.
            taos.exec("use information_schema").await?;
            taos.exec(format!("use `{database}`")).await?;
            let mut set = taos.query("show stables").await?;
            let mut stream = set.deserialize::<String>();

            while let Some(stable) = stream
                .try_next()
                .await
                .context("Deserialize stable list from source error")?
            {
                if !todo.stables.contains_async(&stable).await {
                    let stable = Arc::new(stable.clone());
                    stables.push(stable.clone());
                    let _ = todo.stables.insert_async(stable).await;
                }
            }

            let mut tables = Vec::new();
            if opts.sparse {
                // Sparse stables
                todo.stables
                    .scan_async(|stable| {
                        let stable = stable.clone();
                        let table = LegacyTableItem::new_mtlf(0, stable);
                        if !todo.tables.contains(&table) {
                            tables.push(table.clone());
                            let _ = todo.tables.insert(table.clone());
                        }
                    })
                    .await;

                // Ordinary tables
                let mut set = taos
                .query(format!("select vgroup_id, stable_name, table_name from information_schema.ins_tables where db_name = '{database}' order by stable_name, table_name"))
                .await
                .context("Get stable list from source error")?;
                let mut stream = set
                    .deserialize::<(u32, Option<String>, String)>()
                    .try_filter_map(|(vgroup_id, stable, table)| {
                        let filter = if stable.is_some() {
                            None
                        } else {
                            Some(LegacyTableItem::new(vgroup_id, None, Arc::new(table)))
                        };
                        futures::future::ready(Ok(filter))
                    });
                while let Some(table) = stream
                    .try_next()
                    .await
                    .context("Deserialize stable list from source error")?
                {
                    if !todo.tables.contains_async(&table).await {
                        tables.push(table.clone());
                        let _ = todo.tables.insert_async(table).await;
                    }
                }
            } else {
                // get stable list.
                let mut res = taos
                .query(format!("select vgroup_id, stable_name, table_name from information_schema.ins_tables where db_name = '{database}' order by stable_name, table_name"))
                .await
                .context("Get stable list from source error")?;
                let mut records = res.deserialize::<(u32, Option<String>, String)>();
                while let Some((vgroup_id, stable, table)) = records
                    .try_next()
                    .await
                    .context("Deserialize stable list from source error")?
                {
                    let table = if let Some(stable_name) = stable {
                        if let Some(stable) =
                            todo.stables.read_async(&stable_name, Clone::clone).await
                        {
                            LegacyTableItem::new(vgroup_id, Some(stable.clone()), Arc::new(table))
                        } else {
                            let stable = Arc::new(stable_name.clone());
                            let _ = todo.stables.insert_async(stable.clone()).await;
                            LegacyTableItem::new(vgroup_id, Some(stable), Arc::new(table))
                        }
                    } else {
                        LegacyTableItem::new(vgroup_id, None, Arc::new(table))
                    };
                    if !todo.tables.contains_async(&table).await {
                        tables.push(table.clone());
                        let _ = todo.tables.insert_async(table).await;
                    }
                }
            };
            if opts.shuffle {
                tables.shuffle(&mut rand::thread_rng());
            }
            if !tables.is_empty() {
                tracing::info!("Try to synchronize {} tables in {} tables", tables.len(), 0);
            }
            Ok(LegacyTodo {
                stables: tables_from_vec(stables).await,
                tables: tables_from_vec(tables).await,
            })
        }
    }
}

#[instrument(skip_all)]
#[async_backtrace::framed]
pub async fn parse_todo_list(pool: &TaosPool, opts: &SourceOpts) -> anyhow::Result<LegacyTodo> {
    let todo = Arc::new(LegacyTodo {
        stables: Default::default(),
        tables: Default::default(),
    });
    update_todo_list(pool, opts, todo).await
}

async fn realtime(
    scheduler: &Scheduler,
    start: DateTime<Utc>,
    _from: &Taos,
    _to: &Taos,
    opts: &TableOpts,
    _source_is_v3: bool,
    _target_is_v3: bool,
    todo: &LegacyTodo,
) -> anyhow::Result<()> {
    let mut now = start;
    let excursion = chrono::Duration::from_std(opts.excursion)?;
    if !opts.excursion.is_zero() {
        now -= excursion;
    }
    // now is the separator of history and future data.
    // check if need retro back.
    if !opts.restro.is_zero() {
        // trace back to some duration.
        info!("Retrospect to {:?} ago.", opts.restro);
        let start = now - chrono::Duration::from_std(opts.restro)?;
        let time_range = TimeRange::new().start(start).end(now);

        info!(
            mode = "retrospect",
            ?start,
            end = ?now,
            "spawning retro task for range: {:?}.",
            time_range
        );
        let mut scanned = std::collections::HashSet::<LegacyTableItem>::new();
        let todo_sender = &scheduler.sender;
        let futures = futures::stream::FuturesUnordered::new();
        todo.tables
            .scan_async(|table| {
                if scanned.contains(table) {
                    tracing::warn!("table {} is already scanned.", table.table);
                    return;
                }
                scanned.insert(table.clone());
                futures.push(todo_sender.send_async(Todo::Data(
                    table.stable.clone(),
                    table.table.clone(),
                    time_range,
                    None,
                )));
            })
            .await;
        futures
            .try_for_each(|v| futures::future::ready(Ok(v)))
            .await
            .inspect_err(|err| tracing::warn!(error = %err, "restro task sent error"))?;
        info!(
            mode = "retrospect",
            "restro tasks are all spawned. waiting..."
        );
    }

    // let tick_duration = chrono::Duration::from_std(opts.interval)?;
    let mut interval = tokio::time::interval(opts.interval);
    interval.tick().await;
    let mut start = now;
    let mut scanned = std::collections::HashSet::<LegacyTableItem>::new();
    loop {
        scanned.clear();
        let end = Utc::now() - excursion;
        let time_range = TimeRange::new().start(start).end(end);
        info!(
            mode = "realtime",
            ?start,
            ?end,
            "spawn sync task for range: {:?}.",
            time_range
        );
        let futures = futures::stream::FuturesUnordered::new();
        todo.tables
            .scan_async(|item| {
                if scanned.contains(item) {
                    tracing::warn!("table {} is already scanned.", item.table);
                    return;
                }
                scanned.insert(item.clone());
                let LegacyTableItem {
                    stable,
                    table,
                    vgroup_id: _,
                    mtlf,
                } = item;
                if *mtlf {
                    futures.push(scheduler.send(Todo::Sparse(table.clone(), time_range, None)));
                } else {
                    futures.push(scheduler.send(Todo::Data(
                        stable.clone(),
                        table.clone(),
                        time_range,
                        None,
                    )));
                }
            })
            .await;
        futures
            .try_for_each(|v| futures::future::ready(Ok(v)))
            .await
            .inspect_err(|err| tracing::warn!(error = %err, "restro task sent error"))?;
        start = end;
        info!(
            mode = "realtime",
            "tick tasks are all spawned. waiting for next interval tick..."
        );
        let _ = interval.tick().await;
    }
}

#[instrument(skip_all)]
pub async fn legacy_to_taos(
    from: Dsn,
    actions: Vec<Action>,
    to: Dsn,
    concurrency: usize,
    cancel: CancellationToken,
    task_id: Option<String>,
) -> anyhow::Result<()> {
    legacy_to_taos_impl(from, actions, to, concurrency, cancel, task_id)
        .in_current_span()
        .await
}

async fn legacy_to_taos_impl(
    mut from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    concurrency: usize,
    cancel: CancellationToken,
    task_id: Option<String>,
) -> anyhow::Result<()> {
    tracing::info!("synchronization started in legacy mode");
    let cancel = cancel.child_token();
    let guard = cancel.clone().drop_guard();

    let _ = tracing::info_span!("check parameters").entered();
    // let span = span.entered();

    let concurrent = if concurrency > 0 {
        concurrency
    } else {
        std::thread::available_parallelism()
            .map(|v| v.get())
            .unwrap_or(20)
    };

    let metrics_arc = get_metrics_arc(task_id.clone()).await;
    let metrics = metrics_arc.as_ref().legacy();

    let from_database = from
        .subject
        .clone()
        .ok_or_else(|| anyhow::format_err!("Source database should be set"))?;
    let mut source_opts: SourceOpts = SourceOpts::from_params(&mut from)?;
    if source_opts.workers == 0 {
        source_opts.workers = concurrent;
    }

    verify::verify_dsn(&from)
        .map_err(|err| anyhow::format_err!("Cannot parse source DSN params: {err}"))?;

    let target_db = to.subject.take();

    let from_builder = TaosBuilder::from_dsn(&from)?;
    let to_builder = TaosBuilder::from_dsn(&to)?;

    let target_opts = TargetOpts::from_params(&source_opts, &mut to, source_opts.workers)?;
    tracing::debug!("target options: {:?}", target_opts); // debug
    verify::verify_dsn(&to)
        .map_err(|err| anyhow::format_err!("Cannot parse target DSN params: {err}"))?;
    // let connect_timeout = Duration::from_secs(10);
    tracing::debug!("Building source connection pool...");
    let from_pool = from_builder
        .pool()
        .context("Source connection pool error")?;
    tracing::debug!("Getting connection from source connection pool...");
    let source_taos = from_pool.get().await.context("Source connection error")?;

    // let source_taos = from_pool.get().await.context("Target connection error")?;
    if target_opts.assert {
        // use take there to avoid [Error: [0x0383] Invalid database name] when execute sql with no database
        if let Some(db) = target_db {
            let target = to_builder.build().await?;
            if target.exec(format!("use `{db}`")).await.is_err() {
                if let Some(database_options) = target_opts.database_options.clone() {
                    target
                        .exec(format!(
                            "create database if not exists `{db}` {}",
                            database_options
                        ))
                        .await?;
                } else {
                    // param mapping
                    let (_, sql): ((), String) = source_taos
                        .query_one(format!("show create database `{}`", from_database.clone()))
                        .await?
                        .unwrap();
                    let from_version: String = source_taos
                        .query_one("select server_version()")
                        .await?
                        .unwrap();
                    let to_version: String =
                        target.query_one("select server_version()").await?.unwrap();

                    let option_iter: Vec<&str> = sql.split(' ').collect();
                    let mut option_str = String::new();
                    for (i, s) in option_iter.iter().enumerate() {
                        if i > 2 {
                            // ignore create database `dbname`
                            option_str.push_str(s);
                            option_str.push(' ');
                        }
                    }
                    let ultimate_database_option =
                        if from_version.starts_with("2") && to_version.starts_with("3") {
                            database_options_2to3(option_str.as_str()).unwrap()
                        } else if from_version.starts_with("3") && to_version.starts_with("2") {
                            database_options_3to2(option_str.as_str()).unwrap()
                        } else {
                            // same version or version out of consider
                            option_str.clone()
                        };
                    tracing::info!(
                        "original data option:{}, ultimate database option: {}",
                        option_str,
                        ultimate_database_option
                    );
                    target
                        .exec(format!(
                            "create database if not exists `{db}` {}",
                            ultimate_database_option
                        ))
                        .await?;
                }
            };
            to.subject = Some(db);
        } else {
            anyhow::bail!("Target database should be set!");
        }
    } else {
        to.subject = target_db;
    }

    tracing::debug!("Building target connection pool...");
    let to_pool = TaosBuilder::from_dsn(&to)?.pool()?;
    tracing::debug!("Getting connection from target connection pool...");
    let target_taos = to_pool.get().await?;

    let v1: String = source_taos.server_version().await?.to_string();
    let source_is_v3 = !v1.starts_with("2");
    let v2: String = target_taos.server_version().await?.to_string();
    let target_is_v3 = !v2.starts_with('2');

    {
        let (source_version, target_version) = (&v1, &v2);
        let source_version = semver::Version::parse(&source_version.split('.').take(3).join("."))?;
        let target_version = semver::Version::parse(&target_version.split('.').take(3).join("."))?;
        if source_version >= VERSION_3_3_0 && target_version < VERSION_3_3_0 {
            bail!("Source version is 3.3.0 or later, but target version is earlier than 3.3.0, which is not supported.");
        }
    }

    tracing::debug!("Checking precisions...");
    const SQL_PRECISION: &str =
        "select `precision` from information_schema.ins_databases where name = database()";
    let precision_of_from: Precision = if source_is_v3 {
        source_taos
            .query_one(SQL_PRECISION)
            .await
            .context("Get precision from source error")?
            .ok_or_else(|| anyhow::format_err!("Cannot get precision from source"))?
    } else {
        get_v2_precision(&source_taos)
            .await
            .with_context(|| anyhow::anyhow!("Source (2.x) precision could no be detected"))?
    };
    let precision_of_to: Precision = if target_is_v3 {
        target_taos
            .query_one(SQL_PRECISION)
            .await
            .context("Get precision from target error")?
            .ok_or_else(|| anyhow::format_err!("Cannot get precision from target"))?
    } else {
        get_v2_precision(&target_taos)
            .await
            .with_context(|| anyhow::anyhow!("Target (2.x) precision could no be detected"))?
    };
    if precision_of_from > precision_of_to {
        anyhow::bail!(
            "Cannot convert from source to target precision: {} -> {}",
            precision_of_from,
            precision_of_to
        );
    }
    let with_precision = if precision_of_from < precision_of_to {
        tracing::warn!(
            "Cast precision from {} to {}",
            precision_of_from,
            precision_of_to
        );
        Some(precision_of_to)
    } else {
        tracing::debug!("Use precision: {}", precision_of_from);
        None
    };

    metrics
        .read_concurrency
        .store(source_opts.workers as _, Ordering::SeqCst);

    // span.exit();

    let todo = tokio::select! {
        todo = parse_todo_list(&from_pool, &source_opts) => { todo? },
        _ = cancel.cancelled() => {
            tracing::debug!("Parsing table list break by cancellation");
            return Ok(());
        }
    };
    // dbg!(&todo.stables);
    let todo = Arc::new(todo);

    metrics
        .total_tables
        .store(todo.tables.len() as _, Ordering::SeqCst);
    metrics
        .total_stables
        .store(todo.stables.len() as _, Ordering::SeqCst);

    tracing::info!(
        tables = todo.tables_todo(),
        "Prepare for {} worker scheduler",
        source_opts.workers
    );

    let breakpoints = if let Some(id) = task_id.as_deref() {
        Some(
            BreakpointDb::new_with_task(id)
                .await
                .context("create breakpoint db failed")?, // TODO: handle error
        )
    } else {
        None
    };
    let scheduler = scheduler::Scheduler::new(
        from_pool.clone(),
        to_pool.clone(),
        Arc::new(source_opts.query),
        Arc::new(target_opts.clone()),
        source_opts.workers as _,
        &actions,
        metrics_arc.clone(),
        source_is_v3,
        target_is_v3,
        with_precision,
        cancel.clone(),
        breakpoints,
    )
    // .instrument(tracing::info_span!("scheduler"))
    .await;
    let scheduler = Arc::new(scheduler);

    let scheduler_clone = scheduler.clone();
    let cancel_clone = cancel.clone();
    tokio::spawn(
        async move {
            tokio::select! {
                _ = cancel_clone.cancelled() => {
                    tracing::debug!("Abort scheduler task by cancellation");
                    scheduler_clone.abort();
                }
                _ = scheduler_clone.wait() => {
                    tracing::debug!("Scheduler workers finished, stop task manager");
                    cancel_clone.cancel();
                }
            }
        }
        .in_current_span(),
    );

    let metrics_arc_clone = metrics_arc.clone();
    let cancel_clone = cancel.clone();
    tokio::spawn(
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            let metrics_tracing = || {
                let metrics = metrics_arc_clone.as_ref().legacy();
                tracing::info!(
                    "Processed {} parts for total {} tables, metrics detail:\n{}",
                    metrics.total_finished_tables.load(Ordering::SeqCst),
                    metrics.total_tables.load(Ordering::SeqCst),
                    metrics
                );
            };
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        metrics_tracing();
                    }
                    _ = cancel_clone.cancelled() => {
                        tracing::debug!("metrics task cancelled");
                        metrics_tracing();
                        break;
                    }
                }
            }
        }
        .in_current_span(),
    );

    if !matches!(source_opts.schema, SchemaMode::None) {
        const BREAKPOINT_KEY_SCHEMA: &str = "...schema...";
        let mut schema_synced = false;
        // Step A: check breakpoints for schema
        if let Some(breakpoints) = scheduler.breakpoints_ref() {
            if let Some(v) = breakpoints
                .get(BREAKPOINT_KEY_SCHEMA)
                .await
                .ok()
                .and_then(|v| v)
            {
                tracing::info!("Schema is synced at {v}, skipped");
                schema_synced = true;
            }
        }

        if !schema_synced {
            tracing::info!("synchronize schemas");
            let future = sync_schema(&scheduler, &todo, source_opts.workers as _);
            tokio::select! {
                _ = future => {}
                _ = cancel.cancelled() => {
                    tracing::debug!("Schema task queue cancelled");
                }
            };
        }

        if let Some(breakpoints) = scheduler.breakpoints_ref() {
            if let Err(err) = breakpoints
                .set(BREAKPOINT_KEY_SCHEMA, &chrono::Utc::now().to_string())
                .await
            {
                tracing::warn!("Set schema breakpoint error: {err:#}");
            }
        }
    }

    if matches!(source_opts.schema, SchemaMode::Only) {
        println!("{}", metrics);
        return Ok(());
    }

    let restro_mark = std::time::Instant::now();

    tracing::info!("monitoring for schema changes");
    let now = Utc::now();
    let schema_polling_scheduler = scheduler.clone();
    let schema_polling_todo = todo.clone();
    let schema_polling_source_opts = source_opts.clone();
    let schema_polling_target_opts = target_opts.clone();
    let schema_polling_pool = from_pool.clone();
    let schema_polling_task_id = task_id.clone();
    let schema_polling_metrics = metrics_arc.clone();
    let schema_polling_cancellation = cancel.clone();
    let schema_polling_task = if matches!(source_opts.mode, SyncMode::All | SyncMode::AsIs) {
        let todo_non_changed = Arc::new(todo.as_ref().clone());
        let schema_polling_task = tokio::spawn(
            async move {
                let handle = async move {
                    let mut interval =
                        tokio::time::interval(schema_polling_source_opts.schema_polling_interval);
                    loop {
                        interval.tick().await;
                        let updates = update_todo_list(
                            &schema_polling_pool,
                            &schema_polling_source_opts,
                            schema_polling_todo.clone(),
                        )
                        .await?;
                        if updates.stables.is_empty() && updates.tables.is_empty() {
                            continue;
                        }
                        let updates = Arc::new(updates);
                        let _ = async {
                            tracing::info!(
                                "Schema updated, spawning sync schema task for {} tables",
                                updates.tables.len()
                            );
                            schema_polling_metrics
                                .as_ref()
                                .legacy()
                                .total_tables
                                .fetch_add(updates.tables.len() as _, Ordering::SeqCst);

                            if !matches!(schema_polling_source_opts.schema, SchemaMode::None) {
                                // sync schema of the updated stables.
                                sync_schema(&schema_polling_scheduler, &updates, concurrency)
                                    .await
                                    .context("Spawn schema syncing of the updated stables error")?;
                            }
                            if updates.tables.is_empty() {
                                return Ok::<_, anyhow::Error>(());
                            }
                            // sync data of the updated tables.
                            sync_specified_tables_with_workers(
                                &schema_polling_scheduler,
                                &schema_polling_pool,
                                schema_polling_source_opts.query,
                                &updates,
                                schema_polling_target_opts.clone(),
                                schema_polling_source_opts.workers as _,
                                &schema_polling_task_id,
                            )
                            .await
                            .context("Spawn data syncing of the updated tables error")?;
                            Ok::<_, anyhow::Error>(())
                        }
                        .await
                        .inspect_err(|err| {
                            tracing::warn!(error = format!("{err:#}"), "Sync updated tables error");
                        });
                    }
                    #[allow(unreachable_code)]
                    anyhow::Ok(())
                };
                tokio::select! {
                    _ = schema_polling_cancellation.cancelled() => {
                        tracing::debug!("schema polling task cancelled");
                    }
                    res = handle => {
                        res?;
                    }
                }
                anyhow::Ok(())
            }
            .in_current_span(),
        );

        tracing::info!("synchronize all tables");

        // sync all tables
        let future = sync_specified_tables_with_workers(
            &scheduler,
            &from_pool,
            source_opts.query,
            &todo_non_changed,
            target_opts,
            source_opts.workers as _,
            &task_id,
        );
        tokio::select! {
            _ = future => {}
            _ = cancel.cancelled() => {
                tracing::debug!("Scheduler task queue cancelled");
            }
        };

        schema_polling_task
    } else {
        tokio::spawn(async move {
            let handle = async move {
                let mut interval =
                    tokio::time::interval(schema_polling_source_opts.schema_polling_interval);
                loop {
                    interval.tick().await;
                    let updates = update_todo_list(
                        &schema_polling_pool,
                        &schema_polling_source_opts,
                        schema_polling_todo.clone(),
                    )
                    .await
                    .inspect_err(|err| {
                        tracing::warn!(error = %err, "update todo list error, break schema polling loop");
                    })?;
                    if updates.stables.is_empty() && updates.tables.is_empty() {
                        continue;
                    }
                    let updates = Arc::new(updates);
                    let _ = async {
                        tracing::info!(
                            "Schema updated, spawning sync schema task for {} tables",
                            updates.tables.len()
                        );
                        schema_polling_metrics
                            .as_ref()
                            .legacy()
                            .total_tables
                            .fetch_add(updates.tables.len() as _, Ordering::SeqCst);

                        if !matches!(schema_polling_source_opts.schema, SchemaMode::None) {
                            // sync schema of the updated tables.
                            sync_schema(&schema_polling_scheduler, &updates, concurrency)
                                .await
                                .context("Spawn schema syncing of the updated tables error")?;
                        }
                        if updates.tables.is_empty() {
                            return Ok::<_, anyhow::Error>(());
                        }

                        sync_specified_tables_with_workers(
                            &schema_polling_scheduler,
                            &schema_polling_pool,
                            schema_polling_source_opts.query,
                            &updates,
                            schema_polling_target_opts.clone(),
                            schema_polling_source_opts.workers as _,
                            &schema_polling_task_id,
                        )
                        .await?;
                        Ok::<_, anyhow::Error>(())
                    }
                    .await
                    .inspect_err(|err| {
                        tracing::warn!(error = format!("{err:#}"), "Sync updated tables error");
                    });
                }
                #[allow(unreachable_code)]
                anyhow::Ok(())
            };
            tokio::select! {
                _ = schema_polling_cancellation.cancelled() => {
                    tracing::debug!("schema polling task cancelled");
                }
                res = handle => {
                    res?;
                }
            }
            anyhow::Ok(())
        }.in_current_span())
    };

    if matches!(source_opts.mode, SyncMode::All | SyncMode::Realtime) {
        if source_opts.table.restro.is_zero() {
            source_opts.table.restro = restro_mark.elapsed();
            tracing::info!(
                "Override restro duration to {:?} for historical data sync",
                source_opts.table.restro
            );
        };
        tracing::info!("monitoring for data changes");
        let future = realtime(
            &scheduler,
            now,
            &source_taos,
            &target_taos,
            &source_opts.table,
            source_is_v3,
            target_is_v3,
            &todo,
        )
        .in_current_span();
        tokio::select! {
            _ = future => {}
            _ = cancel.cancelled() => {
                tracing::debug!("Realtime task queue cancelled");
            }
        };
    }

    tracing::info!("close schema monitoring task");
    if let Some(duration) = source_opts.schema_polling_wait_before_end {
        tokio::time::sleep(duration).await;
    }
    drop(guard);
    schema_polling_task.await??;

    info!("syncing done, wait to release resources");
    println!("{}", metrics);
    Ok(())
}

fn database_options_2to3(options: &str) -> Option<String> {
    let mut result = String::new();
    let vec: Vec<&str> = options.split(" ").collect();
    let mut index = 0;
    let options_2 = vec![
        "CACHE",
        "BLOCKS",
        "DAYS",
        "KEEP",
        "MINROWS",
        "MAXROWS",
        "WAL",
        "FSYNC",
        "UPDATE",
        "CACHELAST",
        "REPLICA",
        "QUORUM",
        "COMP",
        "PRECISION",
    ];
    let options_3 = vec![
        "BUFFER",
        "DURATION",
        "KEEP",
        "MINROWS",
        "MAXROWS",
        "WAL_LEVEL",
        "WAL_FSYNC_PERIOD",
        "CACHEMODEL",
        "CACHESIZE",
        "REPLICA",
        "COMP",
        "PRECISION",
        "PAGES",
        "PAGESIZE",
        "RETENTIONS",
        "VGROUPS",
        "SINGLE_STABLE",
        "STT_TRIGGER",
        "TABLE_PREFIX",
        "TABLE_SUFFIX",
        "TSDB_PAGESIZE",
        "WAL_RETENTION_PERIOD",
        "WAL_RETENTION_SIZE",
        "WAL_ROLL_PERIOD",
        "WAL_SEGMENT_SIZE",
    ];
    let mut cache = 0;
    let mut blocks = 0;
    while index < vec.len() {
        if options_2.contains(&vec[index]) {
            index += 1;
            if index < vec.len()
                && !options_2.contains(&vec[index])
                && !options_3.contains(&vec[index])
            // 是一个值
            {
                if "CACHE".eq_ignore_ascii_case(vec[index - 1]) {
                    let cache_result = String::from(vec[index]).parse::<u32>();
                    if cache_result.is_ok() {
                        cache = cache_result.unwrap();
                    }
                }
                if "BLOCKS".eq_ignore_ascii_case(vec[index - 1]) {
                    let blocks_result = String::from(vec[index]).parse::<u32>();
                    if blocks_result.is_ok() {
                        blocks = blocks_result.unwrap();
                    }
                }
                result.push_str(&process_option2to3_pair(vec[index - 1], vec[index]));
            } else {
                index -= 1;
                result.push_str(&process_option2to3_pair(vec[index], ""));
            }
        } else {
            result.push_str(&process_option2to3_pair(vec[index], ""));
        }
        index += 1;
    }
    if cache == 0 && blocks != 0 {
        cache = 1;
    }
    if blocks == 0 && cache != 0 {
        blocks = 1;
    }
    if cache != 0 && blocks != 0 {
        result.push_str(" BUFFER ");
        result.push_str((cache * blocks).to_string().as_str());
    }
    Some(result)
}

fn process_option2to3_pair(option: &str, option_value: &str) -> String {
    match option {
        "DAYS" => {
            let mut new_option = String::from(" DURATION ");
            new_option.push_str(option_value);
            new_option
        }
        "CACHELAST" => {
            let mut new_option = String::from(" CACHEMODEL ");
            let parse_result = String::from(option_value).parse::<u32>();
            if parse_result.is_ok() {
                let cache_last = parse_result.unwrap();
                match cache_last {
                    0 => new_option.push_str("'none'"),
                    1 => new_option.push_str("'last_row'"),
                    2 => new_option.push_str("'last_value'"),
                    3 => new_option.push_str("'both'"),
                    _ => new_option.push_str(option_value),
                }
            } else {
                new_option.push_str(option_value);
            }
            new_option
        }
        "KEEP" | "MINROWS" | "MAXROWS" | "REPLICA" | "COMP" | "PRECISION" => {
            same_option(option, option_value)
        }
        "WAL" => {
            let mut new_option = String::from(" WAL_LEVEL ");
            new_option.push_str(option_value);
            new_option
        }
        "FSYNC" => {
            let mut new_option = String::from(" WAL_FSYNC_PERIOD ");
            new_option.push_str(option_value);
            new_option
        }
        // ignore
        "UPDATE" | "QUORUM" | "CACHE" | "BLOCKS" => String::new(),
        _ => String::new(),
    }
}

fn same_option(option: &str, option_value: &str) -> String {
    let mut same_option = String::new();
    same_option.push(' ');
    same_option.push_str(option);
    if !option_value.is_empty() {
        same_option.push(' ');
    }
    same_option.push_str(option_value);
    same_option
}

fn database_options_3to2(options: &str) -> Option<String> {
    let mut result = String::new();
    let vec: Vec<&str> = options.split(" ").collect();
    let options_2 = vec![
        "CACHE",
        "BLOCKS",
        "DAYS",
        "KEEP",
        "MINROWS",
        "MAXROWS",
        "WAL",
        "FSYNC",
        "UPDATE",
        "CACHELAST",
        "REPLICA",
        "QUORUM",
        "COMP",
        "PRECISION",
    ];
    let options_3 = vec![
        "BUFFER",
        "DURATION",
        "KEEP",
        "MINROWS",
        "MAXROWS",
        "WAL_LEVEL",
        "WAL_FSYNC_PERIOD",
        "CACHEMODEL",
        "CACHESIZE",
        "REPLICA",
        "COMP",
        "PRECISION",
        "PAGES",
        "PAGESIZE",
        "RETENTIONS",
        "VGROUPS",
        "SINGLE_STABLE",
        "STT_TRIGGER",
        "TABLE_PREFIX",
        "TABLE_SUFFIX",
        "TSDB_PAGESIZE",
        "WAL_RETENTION_PERIOD",
        "WAL_RETENTION_SIZE",
        "WAL_ROLL_PERIOD",
        "WAL_SEGMENT_SIZE",
    ];
    // let map :HashMap<String, String>= HashMap::new();
    let mut index = 0;
    while index < vec.len() {
        if options_3.contains(&vec[index]) {
            index += 1;
            if index < vec.len()
                && !options_2.contains(&vec[index])
                && !options_3.contains(&vec[index])
            // 是一个值
            {
                result.push_str(&process_option_pair(vec[index - 1], vec[index]));
            } else {
                index -= 1;
                result.push_str(&process_option_pair(vec[index], ""));
            }
        } else {
            result.push_str(&process_option_pair(vec[index], ""));
        }

        index += 1;
    }
    Option::Some(result)
}

fn process_option_pair(option: &str, option_value: &str) -> String {
    match option {
        "BUFFER" => {
            let value_result = String::from(option_value).parse::<u32>();
            if value_result.is_ok() {
                let mut new_option = String::from(" CACHE ");
                let buffer = value_result.unwrap();
                new_option.push_str("16 ");
                new_option.push_str("BLOCKS ");
                new_option.push_str((buffer / 16).to_string().as_str());
                new_option
            } else {
                println!("option_value not a number");
                same_option(option, option_value)
            }
        }
        "DURATION" => {
            let mut new_option = String::from(" DAYS ");
            new_option.push_str(&process_unit_value(option_value));
            new_option
        }
        "KEEP" => {
            let mut new_option = String::from(" KEEP ");
            let value_array: Vec<&str> = option_value.split(",").collect();
            if value_array.first().is_some() {
                new_option.push_str(&process_unit_value(value_array.first().unwrap()));
            }
            new_option
        }
        "MINROWS" | "MAXROWS" | "REPLICA" | "COMP" | "PRECISION" => {
            same_option(option, option_value)
        }
        "WAL_LEVEL" => {
            let mut new_option = String::from(" WAL ");
            new_option.push_str(option_value);
            new_option
        }
        "WAL_FSYNC_PERIOD" => {
            let mut new_option = String::from(" FSYNC ");
            new_option.push_str(option_value);
            new_option
        }
        "CACHEMODEL" => {
            let mut new_option = String::from(" CACHELAST ");
            match option_value {
                "'none'" => new_option.push('0'),
                "'last_row'" => new_option.push('1'),
                "'last_value'" => new_option.push('2'),
                "'both'" => new_option.push('3'),
                _ => new_option.push_str(""),
            }
            new_option
        }
        // ignore
        "CACHESIZE"
        | "PAGES"
        | "PAGESIZE"
        | "RETENTIONS"
        | "VGROUPS"
        | "SINGLE_STABLE"
        | "TABLE_PREFIX"
        | "TABLE_SUFFIX"
        | "TSDB_PAGESIZE"
        | "WAL_RETENTION_PERIOD"
        | "WAL_RETENTION_SIZE"
        | "WAL_ROLL_PERIOD"
        | "WAL_SEGMENT_SIZE" => String::new(),
        _ => String::new(),
    }
}

fn process_unit_value(option_value: &str) -> String {
    let mut unit = "d";
    let option_len = option_value.len();
    if option_len > 1 {
        unit = &option_value[option_len - 1..option_len];
    }
    match unit {
        "d" => String::from(option_value),
        "m" => {
            let minutes_str = &option_value[0..option_len - 1];
            let minutes: u32 = minutes_str.parse().expect("need a number");
            let days = minutes / (24 * 60);
            days.to_string()
        }
        "h" => {
            let hours: u32 = option_value[0..option_len - 1]
                .parse()
                .expect("need a number");
            let days = hours / 24;
            days.to_string()
        }
        _ => String::from(option_value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    //
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn sync() -> anyhow::Result<()> {
        let _ = pretty_env_logger::formatted_timed_builder()
            .filter_level(log::LevelFilter::Debug)
            .try_init();
        // prepare
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        taos.exec_many([
            "drop database if exists `x-sync-2`",
            "create database `x-sync-2`",
            "drop database if exists `x-sync`",
            "create database `x-sync`",
            "use `x-sync`",
            "create table stb1 (ts timestamp, v1 int) tags (t1 int)",
            "create table ctb1 using stb1 tags(1)",
            "insert into ctb1 values('2022-12-12T08:00:00Z', 1)",
            "insert into ctb1 values('2022-12-13T08:00:00Z', 2)",
            "insert into ctb1 values('2022-12-22T08:00:00Z', 3)",
            "create table ntb1 (ts timestamp, v1 int)",
            "insert into ntb1 values('2022-12-12T08:00:00Z', 1)",
            "insert into ntb1 values('2022-12-13T08:00:00Z', 2)",
            "insert into ntb1 values('2022-12-22T08:00:00Z', 3)",
        ])
        .await?;

        let v3: Dsn = "taos:///x-sync".parse()?;

        let v2: Dsn = "taos:///x-sync-2".parse()?;

        // let v2: Dsn = "taos://localhost:16030/db1?libraryPath=\
        //     /home/huolinhe/Projects/taosdata/TDengine2.0/debug/build/lib/libtaos.so.2.6.0.0\
        //     &configDir=\
        //     /home/huolinhe/Projects/taosdata/taos-connector-rust/taos-optin/tests/cfg/v2"
        //     .parse()?;

        let _ = QueryOpts {
            time_range: TimeRange::new()
                .start(DateTime::parse_from_rfc3339("2022-12-12T08:00:00Z")?.with_timezone(&Utc)),
            limit: Limit::new((1, Some(1))),
            ..Default::default()
        };
        legacy_to_taos(v3, vec![], v2, 1, CancellationToken::new(), None).await?;
        Ok(())
    }

    /// Test synchronize schema with large columns of table.
    ///
    /// Close https://jira.taosdata.com:18080/browse/TS-4323
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn sync_large_table() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt::fmt().with_level(true).try_init();
        // prepare
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        let db_prefix = "test_large_stable";
        let db1 = format!("{}1", db_prefix);
        let db2 = format!("{}2", db_prefix);
        taos.exec_many([
            format!("drop database if exists `{db2}`"),
            format!("create database `{db2}`"),
            format!("use {db2}"),
        ])
        .await?;
        taos.exec_many([
            format!("drop database if exists `{db1}`"),
            format!("create database `{db1}`"),
            format!("use {db1}"),
        ])
        .await?;

        let stable = "sTb1";
        let types = [
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "TINYINT UNSIGNED",
            "SMALLINT UNSIGNED",
            "INT UNSIGNED",
            "BIGINT UNSIGNED",
            "FLOAT",
            "DOUBLE",
            "BINARY(16)",
            "NCHAR(4)",
        ];
        let table_prefix = "tB";
        let table_num = 1000;

        let columns = 3600;
        let mut create_table_sql = format!("CREATE TABLE `{}` (`ts` TIMESTAMP", stable);
        for i in 0..columns {
            let column_name = format!("a_longer_column_name_{}", i);
            let column_type = types[i % types.len()];
            create_table_sql.push_str(format!(", {} {}", column_name, column_type).as_str());
        }
        create_table_sql.push_str(") tags (`t1` INT)");

        std::fs::write("tests/large_table.sql", create_table_sql.as_bytes())?;

        taos.exec(&create_table_sql).await?;

        let show_create: (String, String) = taos
            .query_one(format!("show create table `{}`", stable))
            .await?
            .unwrap();
        let show_sql = show_create.1;
        tracing::info!(
            truncated_len = show_sql.len(),
            "show create table sql: {}",
            &show_sql.as_str()[(show_sql.len() - 100)..show_sql.len()]
        );

        for table_idx in 0..table_num {
            let table_name = format!("{}_{}", table_prefix, table_idx);
            taos.exec(format!(
                "create table `{}` using `{}` tags({})",
                table_name, stable, table_idx
            ))
            .await?;
        }

        let v3: Dsn = format!("taos:///{db1}?schema=only").parse()?;

        let v2: Dsn = format!("taos:///{db2}?assert").parse()?;
        let _ = QueryOpts {
            time_range: TimeRange::new()
                .start(DateTime::parse_from_rfc3339("2022-12-12T08:00:00Z")?.with_timezone(&Utc)),
            limit: Limit::new((1, Some(1))),
            ..Default::default()
        };
        legacy_to_taos(v3, vec![], v2, 1, CancellationToken::new(), None).await?;

        taos.exec(format!("use `{}`", db2)).await?;
        let _ = taos.describe(stable).await?;

        taos.exec_many([
            format!("drop database if exists `{db1}`"),
            format!("drop database if exists `{db2}`"),
        ])
        .await?;
        Ok(())
    }

    /// Test synchronize schema with large columns of table.
    ///
    /// Close https://jira.taosdata.com:18080/browse/TS-4323
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn sync_large_normal_table() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt::fmt().with_level(true).try_init();
        // prepare
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        let db_prefix = "test_large_normal_table";
        let db1 = format!("{}1", db_prefix);
        let db2 = format!("{}2", db_prefix);
        taos.exec_many([
            format!("drop database if exists `{db2}`"),
            format!("create database `{db2}`"),
            format!("use {db2}"),
        ])
        .await?;
        taos.exec_many([
            format!("drop database if exists `{db1}`"),
            format!("create database `{db1}`"),
            format!("use {db1}"),
        ])
        .await?;

        let name = "nTb1";
        let types = [
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "TINYINT UNSIGNED",
            "SMALLINT UNSIGNED",
            "INT UNSIGNED",
            "BIGINT UNSIGNED",
            "FLOAT",
            "DOUBLE",
            "BINARY(16)",
            "NCHAR(4)",
        ];

        let columns = 3600;
        let mut create_table_sql = format!("CREATE TABLE `{}` (`ts` TIMESTAMP", name);
        for i in 0..columns {
            let column_name = format!("a_longer_column_name_{}", i);
            let column_type = types[i % types.len()];
            create_table_sql.push_str(format!(", {} {}", column_name, column_type).as_str());
        }
        create_table_sql.push(')');

        std::fs::write("tests/large_normal_table.sql", create_table_sql.as_bytes())?;

        taos.exec(&create_table_sql).await?;

        let show_create: (String, String) = taos
            .query_one(format!("show create table `{}`", name))
            .await?
            .unwrap();
        let show_sql = show_create.1;
        tracing::info!(
            truncated_len = show_sql.len(),
            "show create table sql: {}",
            &show_sql.as_str()[(show_sql.len() - 100)..show_sql.len()]
        );

        let v3: Dsn = format!("taos:///{db1}?schema=only").parse()?;

        let v2: Dsn = format!("taos:///{db2}?assert").parse()?;
        let _ = QueryOpts {
            time_range: TimeRange::new()
                .start(DateTime::parse_from_rfc3339("2022-12-12T08:00:00Z")?.with_timezone(&Utc)),
            limit: Limit::new((1, Some(1))),
            ..Default::default()
        };
        legacy_to_taos(v3, vec![], v2, 1, CancellationToken::new(), None).await?;

        taos.exec(format!("use `{}`", db2)).await?;

        let _desc = taos.describe(name).await?;

        taos.exec_many([
            format!("drop database if exists `{db1}`"),
            format!("drop database if exists `{db2}`"),
        ])
        .await?;
        Ok(())
    }

    #[test]
    fn test_database_options_2to3() {
        let options2_1 = "REPLICA 1 QUORUM 1 DAYS 10 KEEP 3650 CACHE 16 BLOCKS 6 MINROWS 100 MAXROWS 4096 WAL 1 FSYNC 3000 COMP 2 CACHELAST 0 PRECISION 'ms' UPDATE 0";
        assert_eq!(" REPLICA 1 DURATION 10 KEEP 3650 MINROWS 100 MAXROWS 4096 WAL_LEVEL 1 WAL_FSYNC_PERIOD 3000 COMP 2 CACHEMODEL 'none' PRECISION 'ms' BUFFER 96", database_options_2to3(options2_1).unwrap());
        let option2_2 = "REPLICA 1 QUORUM 1";
        assert_eq!(" REPLICA 1", database_options_2to3(option2_2).unwrap());
        let option2_3 = "REPLICA QUORUM DAYS 10 KEEP";
        assert_eq!(
            " REPLICA DURATION 10 KEEP",
            database_options_2to3(option2_3).unwrap()
        );
    }

    #[test]
    fn test_database_options_3to2() {
        let options3_1 = "BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 14400m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 1 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0";
        assert_eq!(" CACHE 16 BLOCKS 16 CACHELAST 0 COMP 2 DAYS 10 FSYNC 3000 MAXROWS 4096 MINROWS 100 KEEP 3650 PRECISION 'ms' REPLICA 1 WAL 1", database_options_3to2(options3_1).unwrap());
        let option3_2 = "BUFFER CACHESIZE 1 CACHEMODEL";
        assert_eq!(
            " BUFFER CACHELAST ",
            database_options_3to2(option3_2).unwrap()
        );
    }

    #[test]
    fn test_get_precision() {
        fn get_precision(database_create_sql: String) -> Option<String> {
            let vec: Vec<&str> = database_create_sql.split(' ').collect();
            let mut index = 0;
            while index < vec.len() {
                if "PRECISION".eq_ignore_ascii_case(vec[index]) {
                    index += 1;
                    return Some(String::from(vec[index]));
                }
                index += 1;
            }
            // it should't return this
            None
        }

        assert_eq!(get_precision(String::from("PRECISION 'ms' REPLICA 1")), get_precision(String::from("CREATE DATABASE `test2` REPLICA 1 QUORUM 1 DAYS 10 KEEP 3650 CACHE 16 BLOCKS 6 MINROWS 100 MAXROWS 4096 WAL 1 FSYNC 3000 COMP 2 CACHELAST 0 PRECISION 'ms' UPDATE 0")));
        assert_ne!(get_precision(String::from("CREATE DATABASE `test2` REPLICA 1 QUORUM 1 DAYS 10 KEEP 3650 CACHE 16 BLOCKS 6 MINROWS 100 MAXROWS 4096 WAL 1 FSYNC 3000 COMP 2 CACHELAST 0 PRECISION 'us' UPDATE 0")), get_precision(String::from("CREATE DATABASE `test2` REPLICA 1 QUORUM 1 DAYS 10 KEEP 3650 CACHE 16 BLOCKS 6 MINROWS 100 MAXROWS 4096 WAL 1 FSYNC 3000 COMP 2 CACHELAST 0 PRECISION 'ms' UPDATE 0")));
    }

    #[tokio::test]
    async fn test_incomplete_sqls() -> anyhow::Result<()> {
        let sqls = [
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIME"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, "),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1`"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` I"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT)"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) T"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS("),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1`"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` I"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` IN"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT,"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `t2` INT U"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `t2` INT UNSIGNED"),
            (0x2600, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `t2` INT UNSIGNED, `t3` VAR"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `t2` INT UNSIGNED, `t3` VARCHAR"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `t2` INT UNSIGNED, `t3` VARCHAR("),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `t2` INT UNSIGNED, `t3` VARCHAR(1"),
            (0x2601, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `t2` INT UNSIGNED, `t3` VARCHAR(100)"),
            (0, "CREATE STABLE `sTb1` (`ts` TIMESTAMP, `v1` INT) TAGS(`t1` INT, `t2` INT UNSIGNED, `t3` VARCHAR(100))"),
        ];

        tracing_subscriber::fmt::fmt().with_level(true).init();
        // prepare
        let taos = TaosBuilder::from_dsn("taos:///")?.build().await?;
        let db2 = "test_incomplete_sqls";
        taos.exec_many([
            format!("drop database if exists `{db2}`"),
            format!("create database `{db2}`"),
            format!("use {db2}"),
        ])
        .await?;
        for (idx, (code, sql)) in sqls.iter().enumerate() {
            match taos.exec(sql).await {
                Ok(_) => {
                    tracing::info!("{}: {}", idx, sql);
                    assert!(*code == 0, "[{idx}] SQL({sql}) must run ok");
                }
                Err(e) => {
                    tracing::info!("{}: {}", idx, e);
                    assert!(e.code() == *code, "[{idx}] SQL({sql}) must run err");
                }
            }
        }
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_legacy_advance_options() {
        use taos::Dsn;
        let from = "taos+ws://localhost:6041/db1?schema=only&fails-to=./fails-to.log&write-concurrency=10&workers=10";
        let to = "taow+ws://localhost:6041/db2";
        let mut from_dsn = Dsn::from_str(from).unwrap();
        let source_opts = SourceOpts::from_params(&mut from_dsn).unwrap();
        assert_eq!(source_opts.workers, 10);
        assert_eq!(source_opts.write_concurrency, Some(10));
        let mut to_dsn = Dsn::from_str(to).unwrap();
        let targe_opts =
            TargetOpts::from_params(&source_opts, &mut to_dsn, source_opts.workers).unwrap();
        assert_eq!(targe_opts.concurrent_limit, NonZeroUsize::new(1).unwrap());
        assert!(targe_opts.fails_to.is_some());
    }

    #[tokio::test]
    async fn ts5124() -> anyhow::Result<()> {
        let builder = TaosBuilder::from_dsn("taos:///")?;
        let taos1 = builder.build().await?;
        let taos2 = builder.build().await?;
        let db_prefix = "test_ts5124";
        let db1 = format!("{}1", db_prefix);
        let db2 = format!("{}2", db_prefix);
        let tid = 5124;
        taos1
            .exec_many([
                format!("drop database if exists `{db1}`"),
                format!("create database `{db1}`"),
                format!("use {db1}"),
                "create table `nTb1` (ts timestamp, v1 int)".to_string(),
                "insert into `nTb1` values(now, 1)".to_string(),
                "create table `>♑1` (ts timestamp, v1 int)".to_string(),
                "insert into `>♑1` values(now, 1)".to_string(),
            ])
            .await?;
        taos2
            .exec_many([
                format!("drop database if exists `{db2}`"),
                format!("create database `{db2}`"),
                format!("use {db2}"),
            ])
            .await?;

        let source: Dsn = format!("taos:///{db1}").parse()?;

        let sink: Dsn = format!("taos:///{db2}").parse()?;
        let _ = QueryOpts {
            ..Default::default()
        };
        let actions = vec![Action::from_str("rename-table:map:nTb1,nTb2").unwrap(); 1];

        crate::core_metrics::clear_metrics(tid).await;
        let _ = crate::core_metrics::init_task_metrics(&source, &sink, tid, None).await;

        legacy_to_taos(
            source,
            actions,
            sink,
            1,
            CancellationToken::new(),
            Some(tid.to_string()),
        )
        .await?;

        // table nTb2 should be created
        let _ = taos2.exec(format!("desc `{}`.`nTb2`", db2)).await?;
        let _ = taos2.exec(format!("desc `{}`.`>♑1`", db2)).await?;

        taos1
            .exec_many([
                format!("drop database if exists `{db1}`"),
                format!("drop database if exists `{db2}`"),
            ])
            .await?;
        crate::core_metrics::clear_metrics(tid).await;
        Ok(())
    }

    // start: 2024-07-31T01:04:39.316816912Z, end: 2024-07-31T01:04:39.437430018Z
    // start: 2024-07-31T01:04:39.437430018Z, end: 2024-07-31T01:04:39.560152320Z
    // start: 2024-07-31T01:04:39.560152320Z, end: 2024-07-31T01:04:40.560887126Z

    #[test]
    fn test_to_chunks() {
        let start = "2024-07-31T01:04:39.560152320Z";
        let end = "2024-07-31T01:04:40.560887126Z";
        let start: DateTime<Utc> = start.parse().unwrap();
        let end: DateTime<Utc> = end.parse().unwrap();
        let range = TimeRange::new().start(start).end(end);
        let unit = Duration::from_secs(1);
        let chunks = range.to_chunks(unit);

        dbg!(&chunks);
    }
}
