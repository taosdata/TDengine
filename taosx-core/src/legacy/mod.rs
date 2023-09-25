use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Display},
    io::Write,
    num::NonZeroUsize,
    path::Path,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use chrono::{DateTime, TimeZone, Utc};
use metrics::counter;
use rand::seq::SliceRandom;
use serde::Deserialize;
use serde_with::serde_as;
use taos::*;
use tokio::sync::oneshot;
use tracing::{info, instrument, warn};

use crate::{legacy::scheduler::Todo, Action};

use self::scheduler::Scheduler;

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

// legacy metrics
pub const METRICS_LEGACY_WORKERS: &str = "metrics.legacy.workers";
pub const METRICS_LEGACY_STABLES: &str = "metrics.legacy.stables";
pub const METRICS_LEGACY_UPDATED_TAGS: &str = "metrics.legacy.updated_tags";
pub const METRICS_LEGACY_UPDATED_TABLES: &str = "metrics.legacy.updated_tables";
pub const METRICS_LEGACY_CREATED_TABLES: &str = "metrics.legacy.created_tables";
pub const METRICS_LEGACY_TABLES: &str = "metrics.legacy.tables";
pub const METRICS_LEGACY_BLOCKS: &str = "metrics.legacy.blocks";
pub const METRICS_LEGACY_RECORDS: &str = "metrics.legacy.records";
pub const METRICS_LEGACY_POINTS: &str = "metrics.legacy.points";

#[derive(Debug)]
pub struct LegacyMetrics {
    pub workers: AtomicU16,
    pub stables: AtomicU32,
    pub updated_tags: AtomicU32,
    pub updated_tables: AtomicU32,
    pub created_tables: AtomicU32,
    pub tables: AtomicU32,
    pub blocks: AtomicU64,
    pub records: AtomicU64,
    pub points: AtomicU64,
    pub time_cost: Instant,
}

impl Default for LegacyMetrics {
    fn default() -> Self {
        Self {
            workers: Default::default(),
            stables: Default::default(),
            tables: Default::default(),
            blocks: Default::default(),
            records: Default::default(),
            points: Default::default(),
            updated_tags: Default::default(),
            updated_tables: Default::default(),
            created_tables: Default::default(),
            time_cost: Instant::now(),
        }
    }
}
impl Display for LegacyMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering::SeqCst;
        let records = self.records.load(SeqCst);
        let points = self.points.load(SeqCst);
        let cost = self.time_cost.elapsed();
        let mut cons_as_secs = cost.as_secs();
        if cons_as_secs == 0 {
            cons_as_secs = 1;
        }
        write!(
            f,
            "# Metrics\n\
            workers: {}\n\
            created tables: {}\n\
            updated tags: {}\n\
            stables: {}\n\
            tables: {}\n\
            blocks: {}\n\
            records: {} ({} r/s)\n\
            points: {} ({} p/s)\n\
            time cost: {:?}",
            self.workers.load(std::sync::atomic::Ordering::SeqCst),
            self.created_tables
                .load(std::sync::atomic::Ordering::SeqCst),
            self.updated_tags.load(std::sync::atomic::Ordering::SeqCst),
            self.stables.load(std::sync::atomic::Ordering::SeqCst),
            self.tables.load(std::sync::atomic::Ordering::SeqCst),
            self.blocks.load(std::sync::atomic::Ordering::SeqCst),
            records,
            records / cons_as_secs,
            points,
            points / cons_as_secs,
            self.time_cost.elapsed()
        )?;
        Ok(())
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
        match (self.limit, self.offset) {
            (0 | u32::MAX, None) => true,
            _ => false,
        }
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
            (Some(start), None) => f.write_fmt(format_args!("{start}..")),
            (None, Some(end)) => f.write_fmt(format_args!("..{end}")),
            (Some(start), Some(end)) => f.write_fmt(format_args!("{start}..{end}")),
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

#[async_backtrace::framed]
async fn split_table_into_time_range_chunks(
    from: &Taos,
    table: &str,
    opts: &QueryOpts,
) -> anyhow::Result<Vec<TimeRange>> {
    tracing::debug!("Migrate data from table `{table}`");

    let mut time_range = opts.time_range;
    async fn query_ts_with(
        taos: &Taos,
        sql: impl AsRef<str>,
    ) -> Result<chrono::DateTime<Utc>, taos::Error> {
        let sql = sql.as_ref();
        let mut set = taos.query(&sql).await?;
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
            }
        }
        (false, true) => {
            if let Ok(ts) = query_ts_with(from, format!("select first(_c0) from `{table}`")).await {
                time_range.start.replace(ts);
            }
        }
        (false, false) => {
            if let Ok(ts) = query_ts_with(from, format!("select first(_c0) from `{table}`")).await {
                time_range.start.replace(ts);
            }
            if let Ok(ts) = query_ts_with(from, format!("select last(_c0) from `{table}`")).await {
                time_range.end.replace(ts + chrono::Duration::seconds(1));
            }
        }
    }

    Ok(time_range.to_chunks(opts.unit))
}

struct WriteContext {
    from: (TaosPool, TableTodo),
    to: (TaosPool, TableTodo),
    actions: Vec<Action>,
    target_opts: TargetOpts,
    metrics: Arc<LegacyMetrics>,
    remap: Option<Arc<HashMap<String, String>>>,
}
async fn write_block(mut block: RawBlock, context: Arc<WriteContext>) -> RawResult<()> {
    // write block

    let from = &context
        .from
        .0
        .get()
        .await
        .context("Get source connection error")?;
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
    let metrics = &context.metrics;
    let target_opts = &context.target_opts;
    let remap = &context.remap;

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

    loop {
        let ok = to.write_raw_block(&block).await;
        if let Err(err) = ok {
            let code: i32 = err.code().into();
            let err_str = err.to_string();
            tracing::debug!("sync_single_table_partial write raw block error: {err:#}",);
            if code == 0x2603 || code == 0x0618 {
                if let Some(stable) = stable {
                    sync_super_table_schema(
                        &from,
                        stable,
                        &to,
                        remap.as_ref(),
                        target_opts,
                        actions,
                    )
                    .await?;
                    sync_super_table_schema_with_subs(
                        &from,
                        stable,
                        &[table.as_str()],
                        &to,
                        remap.as_ref(),
                        target_opts,
                        true,
                        actions,
                        metrics,
                    )
                    .await?;
                } else {
                    sync_normal_table_schema(from, table, actions, remap.as_ref(), to).await?;
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
                to.write_raw_block(&new).await.map_err(|err| {
                    anyhow::format_err!(
                        "[{}:{}]write raw block of table {table} ({} rows): {}\nData:{}",
                        std::file!(),
                        std::line!(),
                        new.nrows(),
                        err,
                        new.pretty_format()
                    )
                })?;
            } else {
                return Err(err).with_context(|| {
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

    counter!(METRICS_LEGACY_BLOCKS, 1);
    metrics.blocks.fetch_add(1, Ordering::AcqRel);
    counter!(METRICS_LEGACY_RECORDS, block.nrows() as u64);
    metrics
        .records
        .fetch_add(block.nrows() as _, Ordering::AcqRel);
    counter!(
        METRICS_LEGACY_POINTS,
        block.nrows() as u64 * block.ncols() as u64
    );
    metrics
        .points
        .fetch_add((block.nrows() * block.ncols()) as _, Ordering::AcqRel);

    // metrics.fetch_add()

    if let Some(duration) = target_opts.interval {
        tokio::time::sleep(duration).await;
    }
    RawResult::Ok(())
}

#[async_backtrace::framed]
async fn sync_single_table_partial(
    source: TaosPool,
    target: TaosPool,
    from: &Taos,
    stable: &Option<Arc<String>>,
    table: &Arc<String>,
    to: &Taos,
    actions: &Vec<Action>,
    opts: &QueryOpts,
    remap: Option<&Arc<HashMap<String, String>>>,
    target_opts: &TargetOpts,
    target_is_v3: bool,
    metrics: Arc<LegacyMetrics>,
) -> anyhow::Result<()> {
    tracing::info!("Syncing table {table} with range: {}", opts.time_range);
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
        .context(format!("query with {sql}"))?;
    let fields = res.num_of_fields();
    let mut blocks = res.blocks();
    let new_table_name = if actions.is_empty() {
        table.clone()
    } else {
        Arc::new(transform_tbname_with_actions(&table, actions, false)?.to_string())
    };

    let concurrent_limit = target_opts.concurrent_limit.get();

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
            actions: actions.clone(),
            target_opts: target_opts.clone(),
            metrics: metrics.clone(),
            remap: remap.map(Clone::clone),
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
        while let Some(block) = blocks.try_next().await? {
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
                    for i in 0..(block.nrows() + batch_size - 1) / batch_size {
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
                        counter!(METRICS_LEGACY_BLOCKS, 1);
                        metrics.blocks.fetch_add(1, Ordering::SeqCst);
                        counter!(METRICS_LEGACY_RECORDS, params.len() as u64);
                        metrics
                            .records
                            .fetch_add(params.len() as _, Ordering::SeqCst);
                        counter!(METRICS_LEGACY_POINTS, params.len() as u64 * fields as u64);
                        metrics
                            .points
                            .fetch_add((params.len() * fields) as _, Ordering::SeqCst);
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
                        for i in 0..(block.nrows() + batch_size - 1) / batch_size {
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
                            counter!(METRICS_LEGACY_BLOCKS, 1);
                            metrics.blocks.fetch_add(1, Ordering::SeqCst);
                            counter!(METRICS_LEGACY_RECORDS, params.len() as u64);
                            metrics
                                .records
                                .fetch_add(params.len() as _, Ordering::SeqCst);
                            counter!(METRICS_LEGACY_POINTS, params.len() as u64 * fields as u64);
                            metrics
                                .points
                                .fetch_add((params.len() * fields) as _, Ordering::SeqCst);
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
                } else if err_str.contains("0x0x0020") {
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
                counter!(METRICS_LEGACY_BLOCKS, 1);
                metrics.blocks.fetch_add(1, Ordering::SeqCst);
                counter!(METRICS_LEGACY_RECORDS, rows as u64);
                metrics.records.fetch_add(rows as _, Ordering::SeqCst);
                counter!(METRICS_LEGACY_POINTS, rows as u64 * fields as u64);
                metrics
                    .points
                    .fetch_add((rows * fields) as _, Ordering::SeqCst);
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

async fn sync_super_table_schema(
    from: &Taos,
    name: &str,
    to: &Taos,
    remap: Option<&Arc<HashMap<String, String>>>,
    target_opts: &TargetOpts,
    actions: &Vec<Action>,
) -> anyhow::Result<()> {
    debug_assert!(!name.is_empty());
    let (_, sql): ((), String) = from
        .query_one(format!("show create table `{name}`"))
        .await?
        .unwrap();
    let sql = sql
        .replace("VARCHAR", "BINARY")
        .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        .replace("CREATE STABLE", "CREATE STABLE IF NOT EXISTS")
        .replace("create table", "CREATE TABLE IF NOT EXISTS")
        .replace("create stable", "CREATE TABLE IF NOT EXISTS");

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
            if code == 0x000B {
                from.exec(format!("desc `{name}`")).await?;
                break;
            } else if code == 0x032C {
                continue;
            } else {
                Err(err).with_context(|| format!("sql: [{}] exec error", &sql))?;
                break;
            }
        } else {
            break;
        }
    }

    // Compare fields metadata and synchronize if not match.
    let desc = from.describe(name).await?;
    let target_desc = to.describe(&target_name).await?;
    let fields: BTreeMap<_, _> = target_desc.iter().map(|f| (f.field(), f)).collect();

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
            } else {
                if r.length() < l.length() {
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

async fn sync_super_table_schema_with_subs(
    from: &Taos,
    name: &str,
    subs: &[impl AsRef<str>],
    to: &Taos,
    remap: Option<&Arc<HashMap<String, String>>>,
    target_opts: &TargetOpts,
    is_v3: bool,
    actions: &Vec<Action>,
    metrics: &Arc<LegacyMetrics>,
) -> anyhow::Result<()> {
    debug_assert!(!name.is_empty());
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
                .into_iter()
                .zip(r)
                .zip(&tag_name_vec)
                .filter_map(|((l, r), tag)| if l == r { None } else { Some((tag, l, r)) })
            {
                let sql = format!("alter table `{n}` set tag `{tag}` = {}", l.to_sql_value());
                let sql = transform_sql_with_remap(sql, remap);
                if let Err(err) = to.exec(&sql).await {
                    tracing::error!(
                        "Altering table `{n}` tag `{tag}` to {} error: {err:?}",
                        l.to_sql_value()
                    );
                } else {
                    updated_tags += 1;
                    counter!(METRICS_LEGACY_UPDATED_TAGS, 1);
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
    let mut sql = format!("CREATE TABLE");
    let new_stable_name = transform_tbname_with_actions(name, actions, true)?;
    for (child, row) in non_exists {
        let new_table_name = transform_tbname_with_actions(&child, actions, false)?;
        let tags = row.into_iter().map(|v| v.to_sql_value()).join(",");
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
            sql = format!("CREATE TABLE");
            batch = 0;
        }
        sql.extend(e.chars());
    }
    if tables > 0 {
        tracing::debug!("Create child tables with sql: {sql}");
        to.exec(&sql).await?;
        tracing::info!(
            "Created {} tables in stable {} in this chunk",
            tables,
            new_stable_name
        );
        metrics.created_tables.fetch_add(tables, Ordering::SeqCst);
        counter!(METRICS_LEGACY_CREATED_TABLES, tables as u64);
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
            tracing::info!("{}", &sql);
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

// transfrom create sql based on actions
fn transform_sql_with_actions(
    sql: String,
    table_name: &str,
    actions: &Vec<Action>,
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
                    let new = sql.replace(&format!("`{table_name}`",), &action.apply(table_name)?);
                    sql.clear();
                    sql.extend(new.chars());
                }
                Action::RenameSuperTable(action) => {
                    let new = sql.replace(&format!("`{table_name}`",), &action.apply(table_name)?);
                    sql.clear();
                    sql.extend(new.chars());
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
                    let new = sql.replace(&format!("`{table_name}`",), &action.apply(table_name)?);
                    sql.clear();
                    sql.extend(new.chars());
                }
                Action::RenameChildTable(action) => {
                    let new = sql.replace(&format!("`{table_name}`",), &action.apply(table_name)?);
                    sql.clear();
                    sql.extend(new.chars());
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
    actions: &Vec<Action>,
    is_stable: bool,
) -> anyhow::Result<Cow<'a, str>> {
    tracing::debug!("table name transform before: {table_name}");
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
    tracing::debug!("table name transform after: {new_table_name}");
    Ok(new_table_name.into())
}

async fn sync_normal_table_schema(
    from: &Taos,
    name: &str,
    actions: &Vec<Action>,
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
    sql = transform_sql_with_actions(sql, name, actions, false, remap)?;
    if let Err(err) = to.exec(sql.clone()).await {
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

#[async_backtrace::framed]
async fn sync_schema(
    scheduler: &Scheduler,
    _from_pool: &TaosPool,
    _to_pool: &TaosPool,
    _connect_timeout: Duration,
    _opts: SourceOpts,
    _target_opts: TargetOpts,
    todo: Arc<LegacyTodo>,
    _actions: &Vec<Action>,
    concurrency: usize,
    _metrics: &Arc<LegacyMetrics>,
    _source_is_v3: bool,
    _target_is_v3: bool,
) -> anyhow::Result<()> {
    // tasks listener
    let mut readers = Vec::new();

    for stable in &todo.stables {
        let (sender, reader) = oneshot::channel();
        // let (stable, tables);
        scheduler.send(Todo::STable(stable.clone(), sender)).await?;
        readers.push((0, reader));
    }
    for (_, reader) in readers.drain(..) {
        reader.await??;
    }
    info!("STables syncing done");

    // if opts.query.select_from_stable {
    let chunk_size = 400;
    let chunks = todo
        .tables
        .iter()
        .group_by(|item| item.stable.as_deref().map(|s| s.as_str()))
        .into_iter()
        .flat_map(|(stable, group)| {
            if let Some(stable) = stable {
                group
                    .map(|item| item.table.as_str())
                    .chunks(chunk_size)
                    .into_iter()
                    .map(|chunk| (Some(stable), chunk.collect_vec()))
                    .collect_vec()
            } else {
                vec![(None, group.map(|item| item.table.as_str()).collect_vec())]
            }
        })
        .collect_vec();
    let concurrency = if concurrency > 0 {
        concurrency
    } else {
        std::thread::available_parallelism()
            .map(|v| v.get())
            .unwrap_or(8)
    };
    for chunk in chunks {
        let (sender, reader) = oneshot::channel();
        let (stable, tables) = chunk;
        scheduler
            .send(Todo::Meta(
                stable.map(|s| Arc::new(s.to_string())),
                tables.iter().map(|s| s.to_string()).collect_vec(),
                Some(sender),
            ))
            .await?;

        readers.push((tables.len(), reader));
    }
    let total = todo.tables.len();
    let mut dot = total / 100;
    if dot == 0 {
        dot = 1;
    }
    let mut count = 0;
    let mut fails = 0;
    for (tables, reader) in readers {
        count += tables;

        match reader.await? {
            Ok(_) => {}
            Err(err) => {
                tracing::error!("Error: {err:#}",);
                fails += 1;
            }
        }

        if count % dot == 0 {
            if fails == 0 {
                tracing::info!(
                    "Synchronized {:.2}% of tables ({} of {}) for schema.",
                    count as f64 * 100.0 / total as f64,
                    count,
                    total,
                )
            } else {
                tracing::info!(
                    "Synchronized {:.2}% of tables ({} of {}) for schema, {} failed.",
                    count as f64 * 100.0 / total as f64,
                    count,
                    total,
                    fails,
                );
            }
        }
    }
    tracing::info!("Synchronizing {count} tables metadata with {concurrency} workers finished");

    Ok(())
}

async fn sync_specified_tables_with_workers(
    scheduler: &Scheduler,
    _from: TaosPool,
    _to: TaosPool,
    opts: QueryOpts,
    tables: &[LegacyTableItem],
    _target_opts: TargetOpts,
    workers: usize,
    metrics: Arc<LegacyMetrics>,
    _source_is_v3: bool,
    _target_is_v3: bool,
) -> anyhow::Result<()> {
    tracing::info!("Synchronize table data with {} workers", workers);
    let mut count = 0;
    let mut dot = tables.len() / 100;
    if dot == 0 {
        dot = 1;
    }
    let total_tables = tables.len();

    let mut readers = Vec::new();
    for item in tables {
        let stable = &item.stable;
        let table = &item.table;
        let (sender, reader) = oneshot::channel();
        scheduler
            .send(Todo::Data(
                stable.clone(),
                table.clone(),
                opts.time_range,
                Some(sender),
            ))
            .await?;
        readers.push(reader);
    }
    let mut fails = 0;
    for reader in readers {
        count += 1;
        counter!(METRICS_LEGACY_TABLES, 1);
        metrics.tables.fetch_add(1, Ordering::SeqCst);
        match reader.await? {
            Ok(_) => {}
            Err(err) => {
                tracing::error!("Syncing error: {err:#}",);
                fails += 1;
            }
        }

        if count % dot == 0 {
            if fails == 0 {
                tracing::info!(
                    "Synchronized {:.2}% of tables ({} of {}).",
                    count as f64 * 100.0 / total_tables as f64,
                    count,
                    total_tables,
                )
            } else {
                tracing::info!(
                    "Synchronized {:.2}% of tables ({} of {}), {} failed.",
                    count as f64 * 100.0 / total_tables as f64,
                    count,
                    total_tables,
                    fails,
                );
            }
        }
    }
    tracing::info!("Synchronizing {count} tables with {workers} workers finished");
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
                tables.extend(buf.lines().filter_map(|l| l.ok()));
            }

            if tables.len() > 0 {
                opts.tables = Some(tables);
            }
        }
        if let Some(value) = dsn.remove("select-from-stable") {
            opts.query.select_from_stable = value != "false";
        }
        if let Some(value) = dsn.remove("shuffle") {
            opts.shuffle = value != "false";
        }
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
                tables.extend(buf.lines().filter_map(|l| l.ok()));
            }
            if tables.len() > 0 {
                opts.stables = Some(tables);
            }
        }
        opts.table = TableOpts::from_params(dsn)?;

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
    fails_to: Option<Arc<std::sync::Mutex<std::fs::File>>>,
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
        if let Some(file) = self.fails_to.as_mut() {
            let _ = file.lock().unwrap().flush();
        }
    }
}

impl TargetOpts {
    pub fn from_params(dsn: &mut Dsn) -> anyhow::Result<Self> {
        let mut opts = Self::default();
        if let Some(value) = dsn.remove("schema") {
            opts.schema = value.parse().with_context(|| {
                format!(
                    "invalid schema value: {value}, \
                    only always|true, none|false, or only is supported "
                )
            })?;
        }

        if let Some(assert) = dsn.remove("assert") {
            match assert.as_str() {
                "false" => opts.assert = false,
                "" | "true" => opts.assert = true,
                _ => anyhow::bail!(
                    "assert in target dsn should be only empty, or true/false (default is false)"
                ),
            }
        }

        if let Some(value) = dsn.remove("database-options") {
            opts.database_options.replace(value);
        }
        if let Some(value) = dsn.remove("batch-size") {
            opts.batch_size.replace(
                value
                    .parse()
                    .with_context(|| format!("invalid batch-size value: {value}"))?,
            );
        }
        if let Some(value) = dsn.remove("concurrent-limit") {
            opts.concurrent_limit = value
                .parse()
                .with_context(|| format!("invalid concurrent-limit value: {value}"))?;
        }
        if let Some(value) = dsn.remove("blocks-chunk-size") {
            opts.blocks_chunk_size = value
                .parse()
                .with_context(|| format!("invalid blocks-chunk-size value: {value}"))?;
        }
        if let Some(value) = dsn.remove("interval") {
            let value = parse_duration::parse(&value)?;
            opts.interval.replace(value);
        }
        if let Some(value) = dsn.remove("max-sql-length") {
            opts.max_sql_length.replace(value.parse()?);
        }
        if let Some(_) = dsn.remove("force-stmt") {
            opts.force_stmt = true;
        }
        if let Some(value) = dsn.remove("fails-to") {
            let value = Path::new(&value);
            let file = std::fs::File::create(&value)?;

            opts.fails_to.replace(Arc::new(std::sync::Mutex::new(file)));
        }
        if let Some(value) = dsn.remove("timeout-per-table") {
            let value = parse_duration::parse(&value)?;
            opts.timeout_per_table.replace(value);
        }
        if let Some(v) = dsn.remove("update-tags") {
            if v != "false" {
                opts.update_tags = true;
            }
        }

        if let Some(value) = dsn.remove("remap") {
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
                        if s.starts_with('@') {
                            Some(
                                std::fs::File::open(&s[1..])
                                    .context("open remap file error")
                                    .map(|f| {
                                        csv_lib::ReaderBuilder::new()
                                            .has_headers(false)
                                            .flexible(true)
                                            .from_reader(f)
                                    }),
                            )
                        } else {
                            None
                        }
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
                    .group_by(|(table, _)| table.clone())
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
pub struct LegacyTodo {
    stables: Vec<Arc<String>>,
    tables: Vec<LegacyTableItem>,
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct LegacyTableItem {
    vgroup_id: u32,
    stable: Option<Arc<String>>,
    table: Arc<String>,
}

impl LegacyTableItem {
    pub fn new(vgroup_id: u32, stable: Option<Arc<String>>, table: Arc<String>) -> Self {
        Self {
            vgroup_id,
            stable,
            table,
        }
    }

    pub fn is_ordinary_table(&self) -> bool {
        self.stable.is_none()
    }
}
impl LegacyTodo {
    pub fn tables_todo(&self) -> usize {
        self.tables.len()
    }
}

#[async_backtrace::framed]
pub async fn parse_todo_list(pool: &TaosPool, opts: &SourceOpts) -> anyhow::Result<LegacyTodo> {
    // let version =
    let taos = pool
        .get()
        .await
        .context("Connect to data source error with timeout")?;
    let version = taos.server_version().await?;
    let is_v2 = version.starts_with('2');
    info!(version = version.as_ref(), "Retrieving table list...");
    // dbg!(&opts.stables);
    if let Some(stables) = opts.stables.as_ref() {
        const MAX_DISPLAY_STABLES: usize = 5;
        let list = if stables.len() > MAX_DISPLAY_STABLES {
            format!("{},...", stables.iter().take(MAX_DISPLAY_STABLES).join(","))
        } else {
            stables.iter().take(MAX_DISPLAY_STABLES).join(",")
        };
        tracing::info!("Use stables list in data source parameters: {list}");

        let stables = stables
            .iter()
            .map(|s| Arc::new(s.to_string()))
            .collect_vec();
        // let mut tables = vec![];
        let mut tables: Vec<_>;
        if is_v2 {
            if opts.shuffle {
                tables = taos
                    .query("show tables")
                    .await?
                    .deserialize::<TableRecord>()
                    .try_filter_map(|x| {
                        let filter = if let Some(stable_name) = &x.stable_name {
                            stables
                                .iter()
                                .find(|x| x.as_str() == stable_name)
                                .map(|item| {
                                    LegacyTableItem::new(
                                        x.vgroup_id,
                                        Some(item.clone()),
                                        Arc::new(x.table_name),
                                    )
                                })
                        } else {
                            None
                        };
                        futures::future::ready(Ok(filter))
                    })
                    .try_collect()
                    .await
                    .context("Deserialize stable list from source error")?;

                let mut rng = rand::thread_rng();
                tables.shuffle(&mut rng);
            } else {
                tables = Vec::new();
                for stable in &stables {
                    let mut res = taos
                        .query(format!("select tbname from `{}`", stable))
                        .await?;
                    tables.extend(
                        res.deserialize()
                            .map_ok(|table_name| {
                                LegacyTableItem::new(0, Some(stable.clone()), Arc::new(table_name))
                            })
                            .try_collect::<Vec<_>>()
                            .await?,
                    );
                }
            }
        } else {
            // is v3
            let database: String = taos.query_one("SELECT database()").await?.unwrap();
            // note!: to make sure the information_schema is updated.
            taos.exec("use information_schema").await?;
            taos.exec(format!("use `{database}`")).await?;
            tables = Vec::new();
            for stable in &stables {
                let mut res = taos.query(format!("select vgroup_id, table_name from information_schema.ins_tables where db_name = '{}' and stable_name = '{}'", database, stable)).await?;
                tables.extend(
                    res.deserialize()
                        .map_ok(|(vgroup_id, name)| {
                            LegacyTableItem::new(vgroup_id, Some(stable.clone()), Arc::new(name))
                        })
                        .try_collect::<Vec<_>>()
                        .await?,
                );
            }
            if opts.shuffle {
                tables.shuffle(&mut rand::thread_rng());
            }
        }
        tracing::info!(
            "Try to synchronize {} tables in {} stables",
            tables.len(),
            stables.len()
        );

        Ok(LegacyTodo { stables, tables })
    } else if let Some(tables) = opts.tables.as_ref() {
        let mut stable_set = BTreeMap::new();
        let mut stables = vec![];

        let mut tables: Vec<_> = tables
            .iter()
            .map(|s| {
                if let Some((stable_name, table)) = s.split_once('.') {
                    if stable_set.contains_key(stable_name) {
                        let stable: &Arc<String> = stable_set.get(stable_name).unwrap();
                        LegacyTableItem::new(0, Some(stable.clone()), Arc::new(table.to_string()))
                    } else {
                        let stable = Arc::new(stable_name.to_string());
                        stables.push(stable.clone());
                        stable_set.insert(stable_name, stable.clone());
                        LegacyTableItem::new(0, Some(stable.clone()), Arc::new(table.to_string()))
                    }
                } else {
                    LegacyTableItem::new(0, None, Arc::new(s.to_string()))
                }
            })
            .collect();

        for LegacyTableItem { stable, table, .. } in
            &mut tables.iter_mut().filter(|s| s.is_ordinary_table())
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
                        if let Some(arc) = stable_set.get(stable_name.as_str()) {
                            stable.replace(arc.clone());
                        } else {
                            stable.replace(Arc::new(stable_name));
                        }
                    }
                } else {
                    tracing::warn!("Table todo not found: {table}");
                }
            }
        }
        if opts.shuffle {
            tables.shuffle(&mut rand::thread_rng());
        }
        tracing::info!(
            "Try to synchronize {} tables in {} stables",
            tables.len(),
            stables.len()
        );
        Ok(LegacyTodo { stables, tables })
    } else {
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
            let mut stable_set: BTreeMap<String, Arc<String>> = BTreeMap::new();
            stable_set.extend(stables.iter().map(|s| (s.to_string(), s.clone())));

            let mut stable_set: BTreeMap<String, Arc<String>> = BTreeMap::new();

            let mut tables: Vec<_> = taos
                .query("show tables")
                .await?
                .deserialize::<TableRecord>()
                .map_ok(
                    |TableRecord {
                         table_name,
                         stable_name,
                         vgroup_id,
                     }| {
                        if let Some(stable_name) = stable_name {
                            if let Some(stable) = stable_set.get(&stable_name) {
                                LegacyTableItem::new(
                                    vgroup_id,
                                    Some(stable.clone()),
                                    Arc::new(table_name),
                                )
                            } else {
                                let stable = Arc::new(stable_name.clone());
                                // stables.push(stable.clone());
                                stable_set.insert(stable_name, stable.clone());
                                LegacyTableItem::new(vgroup_id, Some(stable), Arc::new(table_name))
                            }
                        } else {
                            LegacyTableItem::new(vgroup_id, None, Arc::new(table_name))
                        }
                    },
                )
                .try_collect()
                .await
                .context("Deserialize stable list from source error")?;

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
            Ok(LegacyTodo { stables, tables })
        } else {
            let database: String = taos.query_one("SELECT database()").await?.unwrap();
            // note!: to make sure the information_schema is updated.
            taos.exec("use information_schema").await?;
            taos.exec(format!("use `{database}`")).await?;

            // let mut stables = vec![];
            let mut stables: Vec<Arc<String>> = taos
                .query("show stables")
                .await?
                .deserialize()
                .map_ok(|stable: String| Arc::new(stable))
                .try_collect()
                .await
                .context("Deserialize stable list from source error")?;
            let mut stable_set: BTreeMap<String, Arc<String>> = BTreeMap::new();
            stable_set.extend(stables.iter().map(|s| (s.to_string(), s.clone())));

            // get stable list.
            let mut res = taos
                .query(&format!("select vgroup_id, stable_name, table_name from information_schema.ins_tables where db_name = '{database}' order by stable_name, table_name"))
                .await
                .context("Get stable list from source error")?;
            let mut tables: Vec<_> = res
                .deserialize::<(u32, Option<String>, String)>()
                .map_ok(|(vgroup_id, stable, table)| {
                    if let Some(stable_name) = stable {
                        if let Some(stable) = stable_set.get(&stable_name) {
                            LegacyTableItem::new(vgroup_id, Some(stable.clone()), Arc::new(table))
                        } else {
                            let stable = Arc::new(stable_name.clone());
                            stables.push(stable.clone());
                            stable_set.insert(stable_name, stable.clone());
                            LegacyTableItem::new(vgroup_id, Some(stable), Arc::new(table))
                        }
                    } else {
                        LegacyTableItem::new(vgroup_id, None, Arc::new(table))
                    }
                })
                .try_collect()
                .await
                .context("Deserialize stable list from source error")?;
            if opts.shuffle {
                tables.shuffle(&mut rand::thread_rng());
            }

            tracing::info!(
                "Try to synchronize {} tables in {} stables and {} ordinary tables",
                tables.len(),
                stables.len(),
                0
            );
            Ok(LegacyTodo { stables, tables })
        }
    }
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
        for LegacyTableItem { stable, table, .. } in &todo.tables {
            scheduler
                .send(Todo::Data(
                    stable.clone(),
                    table.clone(),
                    time_range.clone(),
                    None,
                ))
                .await
                .unwrap();
        }
        info!(
            mode = "retrospect",
            "restro tasks are all spawned. waiting..."
        );
    }

    // let tick_duration = chrono::Duration::from_std(opts.interval)?;
    let mut interval = tokio::time::interval(opts.interval);
    interval.tick().await;
    let mut start = now;
    loop {
        let end = Utc::now() - excursion;
        let time_range = TimeRange::new().start(start).end(end);
        info!(
            mode = "realtime",
            ?start,
            ?end,
            "spawn sync task for range: {:?}.",
            time_range
        );
        for LegacyTableItem { stable, table, .. } in &todo.tables {
            scheduler
                .send(Todo::Data(
                    stable.clone(),
                    table.clone(),
                    time_range.clone(),
                    None,
                ))
                .await
                .unwrap();
        }
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
    mut from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    concurrency: usize,
) -> anyhow::Result<()> {
    tracing::info!("synchronization started in legacy mode");

    let concurrent = if concurrency > 0 {
        concurrency
    } else {
        std::thread::available_parallelism()
            .map(|v| v.get())
            .unwrap_or(20)
    };
    let metrics = Arc::new(LegacyMetrics::default());
    let from_database = from.subject.clone().unwrap();
    let mut source_opts = SourceOpts::from_params(&mut from)?;
    if source_opts.workers == 0 {
        source_opts.workers = concurrent;
    }

    verify::verify_dsn(&from)
        .map_err(|err| anyhow::format_err!("Cannot parse source DSN params: {err}"))?;

    let target_db = to.subject.take();

    let from_builder = TaosBuilder::from_dsn(&from)?;
    let to_builder = TaosBuilder::from_dsn(&to)?;

    let target_opts = TargetOpts::from_params(&mut to)?;
    verify::verify_dsn(&to)
        .map_err(|err| anyhow::format_err!("Cannot parse target DSN params: {err}"))?;
    let connect_timeout = Duration::from_secs(10);
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
                            option_str.push_str(" ");
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

    tracing::debug!("Checking precisions...");
    let precision_of_from = source_taos
        .query("select 1")
        .await
        .context("Get precision from source error")?
        .precision();
    let precision_of_to = target_taos
        .query("select 1")
        .await
        .context("Get precision from target error")?
        .precision();
    if precision_of_from != precision_of_to {
        anyhow::bail!("from and to databases have different precision");
    }
    tracing::debug!("Use precision: {}", precision_of_from);

    let v1: String = source_taos.server_version().await?.to_string();
    let source_is_v3 = !v1.starts_with("2");
    let v2: String = target_taos.server_version().await?.to_string();
    let target_is_v3 = !v2.starts_with('2');

    counter!(METRICS_LEGACY_WORKERS, source_opts.workers as u64);
    metrics
        .workers
        .store(source_opts.workers as _, Ordering::SeqCst);

    let todo = parse_todo_list(&from_pool, &source_opts).await?;
    // dbg!(&todo.stables);
    let todo = Arc::new(todo);

    metrics
        .stables
        .store(todo.stables.len() as _, Ordering::SeqCst);
    counter!(METRICS_LEGACY_STABLES, todo.stables.len() as u64);
    let metrics_inner = metrics.clone();
    let todo_inner = todo.clone();

    tracing::info!("Prepare for {} worker scheduler", source_opts.workers);
    let scheduler = scheduler::Scheduler::new(
        from_pool.clone(),
        to_pool.clone(),
        Arc::new(source_opts.query),
        Arc::new(target_opts.clone()),
        source_opts.workers as _,
        &actions,
        metrics.clone(),
        source_is_v3,
        target_is_v3,
    )
    .await;

    let task_done = AtomicBool::new(false);
    let rc = Arc::new(task_done);
    let task_done_clone = rc.clone();
    std::thread::spawn(move || loop {
        if task_done_clone.load(Ordering::Relaxed) {
            tracing::debug!("stop timer");
            break;
        }
        std::thread::sleep(Duration::from_secs(5));
        tracing::info!(
            "Processed {}/{}, metrics detail:\n{}",
            metrics_inner.tables.load(Ordering::SeqCst),
            todo_inner.tables.len(),
            metrics_inner
        );
    });

    match (source_opts.mode, source_opts.schema) {
        (_, SchemaMode::Only) => {
            sync_schema(
                &scheduler,
                &from_pool,
                &to_pool,
                connect_timeout,
                source_opts.clone(),
                target_opts.clone(),
                todo.clone(),
                &actions,
                source_opts.workers as _,
                &metrics,
                source_is_v3,
                target_is_v3,
            )
            .await?
        }
        (SyncMode::AsIs, SchemaMode::None) => {
            sync_specified_tables_with_workers(
                &scheduler,
                from_pool,
                to_pool,
                source_opts.query,
                &todo.tables,
                target_opts,
                source_opts.workers as _,
                metrics.clone(),
                source_is_v3,
                target_is_v3,
            )
            .await?;
        }
        (SyncMode::AsIs, SchemaMode::Always) => {
            sync_schema(
                &scheduler,
                &from_pool,
                &to_pool,
                connect_timeout,
                source_opts.clone(),
                target_opts.clone(),
                todo.clone(),
                &actions,
                source_opts.workers as _,
                &metrics,
                source_is_v3,
                target_is_v3,
            )
            .await?;
            tracing::info!("synchronize all tables");
            // sync_tables_only(&from, &to, source_opts.query).await?;
            sync_specified_tables_with_workers(
                &scheduler,
                from_pool,
                to_pool,
                source_opts.query,
                &todo.tables,
                target_opts,
                source_opts.workers as _,
                metrics.clone(),
                source_is_v3,
                target_is_v3,
            )
            .await?;
            tracing::info!("synchronize finished.");
        }
        (SyncMode::Realtime, _) => {
            realtime(
                &scheduler,
                Utc::now(),
                &source_taos,
                &target_taos,
                &source_opts.table,
                source_is_v3,
                target_is_v3,
                &todo,
            )
            .await?;
        }
        (SyncMode::All, schema) => {
            match schema {
                SchemaMode::None => (),
                _ => {
                    sync_schema(
                        &scheduler,
                        &from_pool,
                        &to_pool,
                        connect_timeout,
                        source_opts.clone(),
                        target_opts.clone(),
                        todo.clone(),
                        &actions,
                        source_opts.workers as _,
                        &metrics,
                        source_is_v3,
                        target_is_v3,
                    )
                    .await?
                }
            }
            let restro_mark = Instant::now();

            sync_specified_tables_with_workers(
                &scheduler,
                from_pool.clone(),
                to_pool.clone(),
                source_opts.query.clone(),
                &todo.tables,
                target_opts.clone(),
                source_opts.workers as _,
                metrics.clone(),
                source_is_v3,
                target_is_v3,
            )
            .await?;
            // sync_tables_only(&from, &to, source_opts.query, target_opts.clone()).await?;

            if source_opts.table.restro.is_zero() {
                source_opts.table.restro = restro_mark.elapsed();
                tracing::info!(
                    "Override restro duration to {:?} for historical data sync",
                    source_opts.table.restro
                );
            }
            realtime(
                &scheduler,
                Utc::now(),
                &source_taos,
                &target_taos,
                &source_opts.table,
                source_is_v3,
                target_is_v3,
                &todo,
            )
            .await?;
        }
    }

    info!("syncing done, wait to release resources");
    rc.store(true, Ordering::Relaxed);
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
                if "CACHE".eq_ignore_ascii_case(&vec[index - 1]) {
                    let cache_result = String::from(vec[index]).parse::<u32>();
                    if cache_result.is_ok() {
                        cache = cache_result.unwrap();
                    }
                }
                if "BLOCKS".eq_ignore_ascii_case(&vec[index - 1]) {
                    let blocks_result = String::from(vec[index]).parse::<u32>();
                    if blocks_result.is_ok() {
                        blocks = blocks_result.unwrap();
                    }
                }
                result.push_str(&process_option2to3_pair(&vec[index - 1], &vec[index]));
            } else {
                index -= 1;
                result.push_str(&process_option2to3_pair(&vec[index], ""));
            }
        } else {
            result.push_str(&process_option2to3_pair(&vec[index], ""));
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
    same_option.push_str(" ");
    same_option.push_str(option);
    if !option_value.is_empty() {
        same_option.push_str(" ");
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
                result.push_str(&process_option_pair(&vec[index - 1], &vec[index]));
            } else {
                index -= 1;
                result.push_str(&process_option_pair(&vec[index], ""));
            }
        } else {
            result.push_str(&process_option_pair(&vec[index], ""));
        }

        index += 1;
    }
    Option::Some(result)
}

fn process_option_pair<'a>(option: &str, option_value: &str) -> String {
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
            if value_array.get(0).is_some() {
                new_option.push_str(&process_unit_value(value_array.get(0).unwrap()));
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
                "'none'" => new_option.push_str("0"),
                "'last_row'" => new_option.push_str("1"),
                "'last_value'" => new_option.push_str("2"),
                "'both'" => new_option.push_str("3"),
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
            let hours: u32 = (&option_value[0..option_len - 1])
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
    async fn sync() -> anyhow::Result<()> {
        pretty_env_logger::formatted_timed_builder()
            .filter_level(log::LevelFilter::Debug)
            .init();
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
        legacy_to_taos(v3, vec![], v2, 1).await?;
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
                if "PRECISION".eq_ignore_ascii_case(&vec[index]) {
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
}
