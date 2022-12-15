use std::{
    fmt::{Debug, Display},
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use taos::*;

use crate::{legacy::tasks::TablesHandle, Action};

use self::tasks::TableOpts;

mod tasks;

/// A paging expression.
///
/// It will be append to query with `LIMIT {limit} OFFSET {offset}`.
#[derive(Debug, Default, Clone, Copy)]
struct Limit {
    limit: u32,
    offset: Option<u32>,
}

impl Limit {
    pub const fn new(limit: (u32, Option<u32>)) -> Self {
        Self {
            limit: limit.0,
            offset: limit.1,
        }
    }

    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    pub const fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

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
#[derive(Default, Clone, Copy)]
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

    pub const fn is_none(&self) -> bool {
        self.start.is_none() && self.end.is_none()
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
}

#[derive(Debug, Default, Clone, Copy)]
pub struct QueryOpts {
    time_range: TimeRange,
    limit: Limit,
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
}

async fn sync_single_table(
    from: &Taos,
    table: &str,
    to: &Taos,
    opts: &QueryOpts,
    target_is_v3: bool,
) -> anyhow::Result<()> {
    let sql = if opts.is_none() {
        format!("SELECT * FROM `{table}`")
    } else {
        format!("SELECT * FROM `{table}` WHERE {opts}")
    };
    let mut res = from
        .query(&sql)
        .await
        .context(format!("query with {sql}"))?;
    let fields = res.num_of_fields();
    let mut blocks = res.blocks();
    if target_is_v3 {
        while let Some(mut block) = blocks.try_next().await? {
            block.with_table_name(table);
            to.write_raw_block(&block).await.context(format!(
                "write raw block of table {table} ({} rows)",
                block.nrows()
            ))?;
        }
    } else {
        let mut stmt = Stmt::init(to).context("initialize stmt")?;
        let question_masks = std::iter::repeat('?').take(fields).join(",");
        stmt.prepare(format!("INSERT INTO `{table}` VALUES({question_masks})"))
            .context("prepare statement")?;
        while let Some(block) = blocks.try_next().await? {
            // let views = block.columns().collect_vec();
            stmt.bind(block.column_views()).context("bind")?;
            stmt.add_batch().context("add batch")?;
            stmt.execute().context("execute")?;
        }
    }
    Ok(())
}

async fn sync_super_table_schema(
    from: &Taos,
    name: &str,
    to: &Taos,
    tables: usize,
    is_v3: bool,
) -> anyhow::Result<()> {
    // let version: String = from.query_one("SELECT server_version()").await?.unwrap();
    // if version.starts_with('2') {
    // create stable
    let (_, sql): ((), String) = from
        .query_one(format!("show create table `{name}`"))
        .await?
        .unwrap();
    if let Err(err) = to
        .exec(
            sql.replace("VARCHAR", "BINARY")
                .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
                .replace("CREATE STABLE", "CREATE STABLE IF NOT EXISTS"),
        )
        .await
    {
        if err.to_string().contains("0x000B") {
            from.exec(format!("desc `{name}`")).await?;
        } else {
            Err(err)?;
        }
    }

    if tables == 0 {
        return Ok(());
    }

    let desc = from.describe(name).await?;
    let tag_names = desc.tag_names().map(|s| format!("`{s}`")).join(",");

    let sql = if is_v3 {
        format!("SELECT distinct tbname, {tag_names} FROM `{name}`")
    } else {
        format!("SELECT tbname, {tag_names} FROM {name}")
    };
    let mut res = from.query(sql).await?;

    let mut blocks = res.blocks();
    const MAX_SQL_LEN: usize = 1000 * 1000; // 800kb.
    let mut tables = 0;
    let mut batch = 0;
    let mut sql = format!("CREATE TABLE");
    while let Some(block) = blocks.try_next().await? {
        for mut row in block.rows() {
            let child = row.next().unwrap().1.to_string().unwrap();
            tables += 1;
            batch += 0;

            let tags = row.map(|(_, v)| v.to_value().to_sql_value()).join(",");
            let e = format!("  IF NOT EXISTS `{child}` USING `{name}` TAGS({tags})");

            if sql.len() + e.len() > MAX_SQL_LEN {
                to.exec(&sql).await?;
                log::info!("Already created {} tables, {} in batch", tables, batch);
                sql = format!("CREATE TABLE");
                batch = 0;
            }
            sql.extend(e.chars());
        }
        // }
    }

        log::debug!("create child tables with sql length {}", sql.len());
        to.exec(&sql).await?;
    log::info!("Finally created {} tables", tables);
    Ok(())
}

async fn sync_normal_table_schema(from: &Taos, name: &str, to: &Taos) -> anyhow::Result<()> {
    let (_, sql): ((), String) = from
        .query_one(format!("show create table `{name}`"))
        .await?
        .unwrap();
    // todo: here will produce error: [0x000B] Unable to establish connection
    if let Err(err) = to
        .exec(
            sql.replace("VARCHAR", "BINARY")
                .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"),
        )
        .await
    {
        if !err.to_string().contains("[0x000B]") {
            Err(err)?;
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct STableRecord {
    name: String,
    tables: usize,
}
#[derive(Debug, Deserialize)]
struct TableRecord {
    table_name: String,
    stable_name: Option<String>,
}

impl TableRecord {
    fn is_normal_table(&self) -> bool {
        self.stable_name
            .as_deref()
            .map(|s| s.is_empty())
            .unwrap_or(true)
    }
}

async fn sync_schema(from: &Taos, to: &Taos) -> anyhow::Result<()> {
    let v1: String = from.query_one("SELECT server_version()").await?.unwrap();
    // let v2: String = to.query_one("SELECT server_version()").await?.unwrap();
    if v1.starts_with('2') {
        // get stable list.
        let mut res = from.query("SHOW STABLES").await?;
        res.deserialize()
            .try_for_each(|stable: STableRecord| async move {
                // let name = stable.name.to_string();
                sync_super_table_schema(from, &stable.name, to, stable.tables, false)
                    .await
                    .map_err(Error::Any)
            })
            .await?;

        //  get normal tables.
        let mut res = from.query("SHOW TABLES").await?;
        res.deserialize()
            .try_for_each(|row: TableRecord| async move {
                if row.is_normal_table() {
                    sync_normal_table_schema(from, row.table_name.as_str(), to)
                        .await
                        .map_err(Error::Any)
                } else {
                    Ok(())
                }
            })
            .await?;
    } else {
        let database: String = from.query_one("SELECT database()").await?.unwrap();
        // note!: to make sure the information_schema is updated.
        from.exec("use information_schema").await?;
        from.exec(format!("use `{database}`")).await?;
        // get stable list.
        let mut res = from.query("SHOW STABLES").await?;
        res.deserialize()
            .try_for_each(|name: String| async move {
                sync_super_table_schema(from, &name, to, 1, true)
                    .await
                    .map_err(Error::Any)
            })
            .await?;
        //  get normal tables.
        let mut res = from.query(format!("select `table_name` from information_schema.ins_tables where db_name = '{database}' and stable_name is null;")).await?;

        res.deserialize()
            .try_for_each(|row: String| async move {
                log::debug!("sync normal table schema: {}", row);
                sync_normal_table_schema(from, row.as_str(), to)
                    .await
                    .map_err(Error::Any)
            })
            .await?;
    }

    Ok(())
}

async fn sync_tables_only(from: &Taos, to: &Taos, opts: QueryOpts) -> anyhow::Result<()> {
    let v1: String = from.query_one("SELECT server_version()").await?.unwrap();
    let v2: String = to.query_one("SELECT server_version()").await?.unwrap();
    let to_is_v3 = v2.starts_with('3');
    if v1.starts_with('2') {
        let mut res = from.query("SHOW TABLES").await?;
        let tables: Vec<TableRecord> = res.deserialize().try_collect().await?;
        drop(res);
        for row in tables {
            sync_single_table(from, &row.table_name, to, &opts, to_is_v3)
                .await
                .map_err(Error::Any)?;
        }
    } else {
        //  get normal tables.
        let mut res = from.query("SHOW TABLES").await?;
        let tables: Vec<String> = res.deserialize().try_collect().await?;
        drop(res);

        for table in tables {
            sync_single_table(from, table.as_str(), to, &opts, to_is_v3)
                .await
                .map_err(Error::Any)?;
        }
    }
    Ok(())
}
pub async fn sync(from: Taos, to: Taos, opts: QueryOpts, schema: bool) -> anyhow::Result<()> {
    if schema {
        sync_schema(&from, &to).await?;
    }
    sync_tables_only(&from, &to, opts).await?;
    Ok(())
}
// async fn sync(from: Taos, to: Taos, opts: QueryOpts, schema: bool) -> anyhow::Result<()> {
//     if schema {
//         sync_schema(&from, &to).await?;
//     }
//     sync_tables_only(&from, &to, opts).await?;
//     Ok(())
// }

#[derive(Debug, Default)]
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

#[derive(Debug, Default)]
pub struct SourceOpts {
    query: QueryOpts,
    assert: bool,
    schema: SchemaMode,
    mode: SyncMode,
    table: TableOpts,
    forever: bool,
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
                _ => anyhow::bail!("assert in target dsn should be only empty, or true/false"),
            }
        }
        if let Some(value) = dsn.remove("forever") {
            match value.as_str() {
                "false" => opts.forever = false,
                "" | "true" => opts.forever = true,
                _ => anyhow::bail!("assert in target dsn should be only empty, or true/false"),
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

        if let Some(value) = dsn.remove("start") {
            let value = DateTime::<Utc>::from_str(&value)?;
            opts.query.time_range.start.replace(value);
        }
        if let Some(value) = dsn.remove("end") {
            let value = DateTime::<Utc>::from_str(&value)?;
            opts.query.time_range.end.replace(value);
        }

        if let Some(value) = dsn.remove("mode") {
            let value = SyncMode::from_str(&value)?;
            opts.mode = value;
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

impl FromStr for SchemaMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "" | "always" | "true" => Ok(Self::Always),
            "false" | "none" => Ok(Self::None),
            "only" => Ok(Self::Only),
            _ => bail!("Invalid schema mode: {s}"),
        }
    }
}
#[derive(Debug, Default)]
pub struct TargetOpts {
    assert: bool,
    schema: SchemaMode,
    database_options: Option<String>,
}

impl TargetOpts {
    pub fn from_params(dsn: &mut Dsn) -> anyhow::Result<Self> {
        let mut opts = Self::default();
        if let Some(schema) = dsn.remove("schema") {
            opts.schema = schema.parse()?;
        }

        if let Some(assert) = dsn.remove("assert") {
            match assert.as_str() {
                "false" => opts.assert = false,
                "" | "true" => opts.assert = true,
                _ => anyhow::bail!("assert in target dsn should be only empty, or true/false"),
            }
        }

        if let Some(value) = dsn.remove("database-options") {
            opts.database_options.replace(value);
        }
        Ok(opts)
    }
}

pub async fn legacy_to_taos(
    mut from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
) -> anyhow::Result<()> {
    let _ = (actions, jobs);
    log::info!("synchronization started in legacy mode");
    let mut source_opts = SourceOpts::from_params(&mut from)?;
    let target_opts = TargetOpts::from_params(&mut to)?;

    let from_builder = TaosBuilder::from_dsn(&from)?;
    let from = from_builder.build()?;

    if target_opts.assert {
        if let Some(db) = to.subject.take() {
            let target = TaosBuilder::from_dsn(&to)?.build()?;
            if target.exec(format!("use `{db}`")).await.is_err() {
                target
                    .exec(format!(
                        "create database if not exists `{db}` {}",
                        target_opts.database_options.as_deref().unwrap_or("")
                    ))
                    .await?;
            };
            to.subject = Some(db);
        } else {
            anyhow::bail!("Target database should be set!");
        }
    }
    let to_builder = TaosBuilder::from_dsn(&to)?;
    let to = to_builder.build()?;

    // let to_is_v3 = to
    //     .query_one::<_, String>("SELECT server_version()")
    //     .await?
    //     .unwrap()
    //     .starts_with('3');

    match (source_opts.mode, source_opts.schema) {
        (_, SchemaMode::Only) => sync_schema(&from, &to).await?,
        (SyncMode::AsIs, SchemaMode::None) => {
            sync_tables_only(&from, &to, source_opts.query).await?;
        }
        (SyncMode::AsIs, SchemaMode::Always) => {
            log::info!("synchronize schema");
            sync_schema(&from, &to).await?;
            log::info!("synchronize all tables");
            sync_tables_only(&from, &to, source_opts.query).await?;
            log::info!("synchronize finished.");
        }
        (SyncMode::Realtime, _) => {
            let mut tables = TablesHandle::new(from_builder, to_builder, source_opts.table).await?;
            tables.spawn().await?;
            tables.join().await?;
        }
        (SyncMode::All, schema) => {
            match schema {
                SchemaMode::None => (),
                SchemaMode::Only => unreachable!(),
                SchemaMode::Always => sync_schema(&from, &to).await?,
            }
            let restro_mark = Instant::now();
            sync_tables_only(&from, &to, source_opts.query).await?;
            if source_opts.table.restro.is_zero() {
                source_opts.table.restro = restro_mark.elapsed();
                log::info!(
                    "Override restro duration to {:?} for historical data sync",
                    source_opts.table.restro
                );
            }
            let mut tables = TablesHandle::new(from_builder, to_builder, source_opts.table).await?;
            tables.spawn().await?;
            tables.join().await?;
        }

        _ => unreachable!(),
    }

    log::info!("syncing done, wait to release resources");
    // if to_is_v3 {
    //     drop(to_builder);
    // }
    // drop(from_builder);

    log::info!("done");
    Ok(())
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
        let taos = TaosBuilder::from_dsn("taos:///")?.build()?;
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

        let opts = QueryOpts {
            time_range: TimeRange::new()
                .start(DateTime::parse_from_rfc3339("2022-12-12T08:00:00Z")?.with_timezone(&Utc)),
            limit: Limit::new((1, Some(1))),
        };
        legacy_to_taos(v3, vec![], v2, 1).await?;
        Ok(())
    }
}
