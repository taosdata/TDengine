use anyhow::{Context, Result};
use deadpool::managed::{Metrics, RecycleResult};
use faststr::FastStr;
use serde::Serialize;
use std::collections::HashMap;
use std::{
    ops::Deref,
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};
use taos::*;
use taosx_core::tmq::Topic;
use taosx_core::{
    Action,
    core_metrics::{CoreMetrics, TaskMetrics},
    legacy_metric::LegacyToTaosMetrics,
    tmq::tmq_metric::TmqMetrics,
    utils::sql::BlockPartitionBy,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use legacy_to_taos::{
    sync_normal_table_schema, sync_super_table_schema, sync_super_table_schema_with_subs,
};

use super::{TaosConnection, execute_many_sql, migrate_data_schema};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum MessageType {
    DataOnly = 1,
    MetaOnly,
    MetaData,
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (*self as u8).fmt(f)
    }
}

pub struct RawMessage {
    /// Message ID.
    pub mid: usize,
    /// Message type.
    ///
    /// 0: unexpected.
    /// 1: meta only.
    /// 2: data only.
    /// 3: meta and data with schema changes.
    pub mty: MessageType,
    /// TMQ raw data.
    pub raw: RawMeta,
    /// TMQ schema changes in deserialized JSON format.
    pub meta: Option<JsonMeta>,
    /// TMQ data blocks in current message.
    pub data: Option<Vec<RawBlock>>,
}

impl RawMessage {
    pub fn raw_only(mid: usize, mty: MessageType, raw: RawMeta) -> Self {
        Self {
            mid,
            mty,
            raw,
            meta: None,
            data: None,
        }
    }
    pub fn meta_only(mid: usize, raw: RawMeta, meta: Option<JsonMeta>) -> Self {
        Self {
            mid,
            mty: MessageType::MetaOnly,
            raw,
            meta,
            data: None,
        }
    }

    pub fn data_only(mid: usize, raw: RawMeta, data: Vec<RawBlock>) -> Self {
        Self {
            mid,
            mty: MessageType::DataOnly,
            raw,
            meta: None,
            data: Some(data),
        }
    }

    pub fn meta_data(
        mid: usize,
        raw: RawMeta,
        meta: Option<JsonMeta>,
        data: Vec<RawBlock>,
    ) -> Self {
        Self {
            mid,
            mty: MessageType::MetaData,
            raw,
            meta,
            data: Some(data),
        }
    }

    pub fn rows(&self) -> Option<usize> {
        self.data
            .as_ref()
            .map(|v| v.iter().map(|b| b.nrows()).sum())
    }
}

// async fn parse_data_only(
//     data: &Data,
//     actions: &[Action],
//     metrics: &TmqMetrics,
// ) -> Result<RawMessage> {
//     let raw = data.as_raw_data().await?;
//     if !actions.is_empty() {
//         let mut vec = Vec::new();
//         while let Some(raw) = data
//             .fetch_raw_block()
//             .await
//             .context("Fetch raw block error")?
//         {
//             vec.push(raw);
//         }
//         metrics.add_messages_of_data(1);
//         Ok(RawMessage::data_only(
//             unsafe { std::mem::transmute(raw) },
//             vec,
//         ))
//     } else {
//         metrics.add_messages_of_data(1);
//         Ok(RawMessage::raw_only(unsafe { std::mem::transmute(raw) }))
//     }
// }

// pub(super) async fn parse_message(
//     message: &MessageSet<Meta, Data>,
//     actions: &[Action],
//     metrics: &TmqMetrics,
// ) -> Result<RawMessage> {
//     match message {
//         MessageSet::Meta(meta) => parse_meta_only(meta, metrics).in_current_span().await,
//         MessageSet::Data(data) => {
//             parse_data_only(data, actions, metrics)
//                 .in_current_span()
//                 .await
//         }
//         MessageSet::MetaData(meta, data) => {
//             parse_meta_data(meta, data, metrics).in_current_span().await
//         }
//     }
// }

#[derive(Debug, Serialize, Default, Clone, Copy)]
pub enum WriteStrategy {
    /// Default strategy, prefer raw data if possible.
    #[default]
    Auto,
    /// Use raw2raw only for best performance.
    Raw,
    /// Prefer interlace mode.
    Interlace,
    /// Prefer stmt, fallback to block-by-block if raw data failed.
    Stmt,
    /// Prefer sql, fallback to block-by-block if failed.
    Sql,
    /// Prefer block-by-block, fallback to sql if raw data failed.
    Block,
}

impl From<&str> for WriteStrategy {
    fn from(s: &str) -> Self {
        match s {
            "raw" => WriteStrategy::Raw,
            "stmt" => WriteStrategy::Stmt,
            "sql" => WriteStrategy::Sql,
            "interlace" => WriteStrategy::Interlace,
            "block" => WriteStrategy::Block,
            _ => WriteStrategy::Auto,
        }
    }
}

impl From<String> for WriteStrategy {
    fn from(s: String) -> Self {
        s.as_str().into()
    }
}

impl From<&WriteStrategy> for &str {
    fn from(s: &WriteStrategy) -> Self {
        match s {
            WriteStrategy::Interlace => "interlace",
            WriteStrategy::Stmt => "stmt",
            WriteStrategy::Sql => "sql",
            WriteStrategy::Block => "block",
            WriteStrategy::Raw => "raw",
            WriteStrategy::Auto => "auto",
        }
    }
}

impl WriteStrategy {
    #[inline]
    pub fn as_str(&self) -> &str {
        self.into()
    }

    pub fn from_env() -> Self {
        std::env::var("TMQ_MESSAGE_WRITE_STRATEGY")
            .map(|s| s.into())
            .unwrap_or(WriteStrategy::Auto)
    }

    #[inline]
    pub fn by_block(&self) -> bool {
        matches!(self, WriteStrategy::Block)
    }

    #[inline]
    pub fn prefer_interlace(&self) -> bool {
        matches!(self, WriteStrategy::Interlace)
    }

    #[inline]
    pub fn prefer_raw(&self) -> bool {
        matches!(self, WriteStrategy::Raw)
    }

    #[inline]
    pub fn is_default(&self) -> bool {
        matches!(self, WriteStrategy::Auto)
    }

    #[inline]
    pub fn require_blocks(&self) -> bool {
        !matches!(self, WriteStrategy::Raw)
    }

    #[inline]
    pub fn without_json_meta(&self) -> bool {
        matches!(self, WriteStrategy::Raw)
    }
}
#[derive(Clone)]
pub(super) struct WriteOptions {
    pub actions: Arc<Vec<Action>>,
    pub with_meta_delete: bool,
    pub with_meta_drop: bool,
    pub strategy: WriteStrategy,
    pub concurrency: usize,

    pub commit_chunk_size: usize,
    pub commit_interval_ms: u64,
    pub max_polling_timeout: Duration,
    pub mid: Arc<AtomicUsize>,
}

impl WriteOptions {
    fn next_mid(&self) -> usize {
        self.mid.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    async fn parse_meta_only(&self, meta: &Meta, metrics: &TmqMetrics) -> Result<RawMessage> {
        let raw = meta.as_raw_meta().await?;
        metrics.add_message_bytes(raw.raw_len() as _);
        let json_meta = if !self.actions.is_empty() || !self.strategy.without_json_meta() {
            meta.as_json_meta()
                .in_current_span()
                .await
                .context("Fetch json meta error")
                .ok()
        } else {
            None
        };
        metrics.add_messages_of_meta(1);
        Ok(RawMessage::meta_only(self.next_mid(), raw, json_meta))
    }
    async fn parse_data(&self, data: &Data, metrics: &TmqMetrics) -> Result<RawMessage> {
        let raw = data.as_raw_data().await?;
        metrics.add_message_bytes(raw.raw_len() as _);

        if self.actions.is_empty() && !self.strategy.require_blocks() {
            return Ok(RawMessage::raw_only(
                self.next_mid(),
                MessageType::DataOnly,
                raw,
            ));
        }
        let mut vec = Vec::new();
        while let Some(block) = data
            .fetch_raw_block()
            .await
            .context("Fetch raw block error")?
        {
            vec.push(block);
        }
        metrics.add_messages_of_data(1);
        Ok(RawMessage::data_only(self.next_mid(), raw, vec))
    }
    async fn parse_meta_data(
        &self,
        meta: &Meta,
        data: &Data,
        metrics: &TmqMetrics,
    ) -> Result<RawMessage> {
        let raw = meta.as_raw_meta().await?;
        metrics.add_message_bytes(raw.raw_len() as _);
        if self.actions.is_empty() && self.strategy.without_json_meta() {
            return Ok(RawMessage::raw_only(
                self.next_mid(),
                MessageType::MetaData,
                raw,
            ));
        }
        let json_meta = meta
            .as_json_meta()
            .in_current_span()
            .await
            .context("Fetch json meta error")
            .ok();
        metrics.add_messages_of_meta(1);
        let mut vec = Vec::new();
        while let Some(raw) = data
            .fetch_raw_block()
            .await
            .context("Fetch raw block error")?
        {
            vec.push(raw);
        }
        metrics.add_messages_of_data(1);
        Ok(RawMessage::meta_data(self.next_mid(), raw, json_meta, vec))
    }

    pub(super) async fn parse_message(
        &self,
        message: &MessageSet<Meta, Data>,
        metrics: &TmqMetrics,
    ) -> Result<RawMessage> {
        match message {
            MessageSet::Meta(meta) => self.parse_meta_only(meta, metrics).in_current_span().await,
            MessageSet::Data(data) => self.parse_data(data, metrics).in_current_span().await,
            MessageSet::MetaData(meta, data) => {
                self.parse_meta_data(meta, data, metrics)
                    .in_current_span()
                    .await
            }
        }
    }
}
pub(super) struct Worker {
    pub source: TaosPool,
    pub target: TaosPool,
    pub target_connection: Option<TaosConnection>,
    pub table: Option<FastStr>,
    pub sender: flume::Sender<anyhow::Result<()>>,

    pub topic: Arc<taosx_core::tmq::Topic>,
    pub metrics: Arc<CoreMetrics>,
    pub options: WriteOptions,
}
impl Clone for Worker {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            target: self.target.clone(),
            target_connection: None,
            table: self.table.clone(),
            sender: self.sender.clone(),
            options: self.options.clone(),
            metrics: self.metrics.clone(),
            topic: self.topic.clone(),
        }
    }
}

/// handle write raw block error, error message as fellow:
/// Invalid parameters,detail:table:stb, err:column type not equal, name:val, schema type:DOUBLE, data type:DECIMAL
async fn handle_unequal_column_type(
    taos: &Taos,
    table_name: &str,
    raw: &RawBlock,
) -> anyhow::Result<()> {
    let desc = taos.describe(table_name).await?;
    let fields: HashMap<_, _> = raw
        .field_names()
        .iter()
        .map(|name| {
            desc.iter()
                .find(|f| f.field() == name)
                .map(|f| (name, f.data_type()))
                .ok_or_else(|| anyhow::format_err!("Column does not exist {name}"))
        })
        .try_collect()?;
    let views: Vec<ColumnView> = raw
        .column_views()
        .iter()
        .zip(raw.field_names())
        .map(|(view, name)| view.cast_with_schema(fields[name]))
        .try_collect()
        .map_err(RawError::from_any)?;
    let mut new = RawBlock::from_views(views.as_slice(), raw.precision());
    new.with_table_name(table_name);
    new.with_field_names(raw.field_names());
    tracing::debug!("new block: {}", new.pretty_format().to_string());
    taos.write_raw_block(&new)
        .await
        .with_context(|| new.pretty_format().to_string())
        .with_context(|| {
            anyhow::format_err!(
                "[{}:{}]write raw block of table {table_name} ({} rows)",
                std::file!(),
                std::line!(),
                new.nrows(),
            )
        })?;
    Ok(())
}

#[tracing::instrument(skip_all, fields(target.table = raw.table_name()))]
pub(super) async fn write_with_raw_block(
    actions: &[Action],
    topic: &Topic,
    source: &TaosPool,
    target: &TaosPool,
    taos: &Taos,
    raw: &RawBlock,
    source_table_name: Option<&str>,
    metrics: &TmqMetrics,
) -> anyhow::Result<()> {
    loop {
        if let Err(err) = taos.write_raw_block(raw).await {
            let code = *err.code().deref();
            tracing::debug!("Try to recover from error str: {}", err.to_string());
            if let Some(source_table_name) = source_table_name {
                match code {
                    0x0118 => {
                        let err_str = err.to_string();
                        if err_str.contains("column type not equal") {
                            return handle_unequal_column_type(taos, source_table_name, raw).await;
                        }
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0x0118: the table does not exist
                        let from = source.get().await?;
                        // sync schema
                        // let source_stable_name = from.query_one::<_, String>(format!("select stable_name from information_schema.ins_tables where db_name = database() and table_name = '{source_table_name}'")).await?;
                        let source_stable_name =
                            get_stable_name(&from, None, source_table_name).await?;
                        if let Some(mut source_stable_name) = source_stable_name {
                            if actions.is_empty() {
                                migrate_data_schema(&raw.fields(), taos, source_table_name).await?;
                            } else {
                                for action in actions {
                                    match action {
                                        Action::RenameTable(rename)
                                        | Action::RenameSuperTable(rename) => {
                                            rename.apply_in_place(&mut source_stable_name)?
                                        }
                                        _ => (),
                                    }
                                }
                                migrate_data_schema(&raw.fields(), taos, &source_stable_name)
                                    .await?;
                            }
                        } else {
                            let table = raw.table_name().unwrap();
                            migrate_data_schema(&raw.fields(), taos, table).await?;
                        }
                        tracing::debug!("Write raw block into target after 0x0118 fix");
                        continue;
                    }
                    0x0218 | 0x2603 | 0x2662 | 0x036D | 0x0618 => {
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0x0218: the table does not exist
                        // 0x2603: the table does not exist
                        // 0x2662: the table does not exist
                        // 0x036D: the table does not exist
                        // 0x0618: the table does not exist
                        let from = source.get().await?;
                        let database = topic.database.as_str();
                        if topic.is_query() {
                            // sync as normal table.
                            sync_normal_table_schema(&from, source_table_name, actions, None, taos)
                                .await
                                .context("Create table error")?;
                        }
                        let super_table_name =
                            get_stable_name(&from, Some(database), source_table_name).await?;
                        // if let Some(stable) = from.query_one::<_, String>(format!("select stable_name from information_schema.ins_tables where db_name = '{database}' and table_name = '{source_table_name}'")).await?.and_then(|s| if s.is_empty() { None } else { Some(s) }) {
                        if let Some(stable) = super_table_name {
                            let from = source.get().await?;
                            let target_opts = Default::default();
                            sync_super_table_schema(
                                &from,
                                &stable,
                                taos,
                                None,
                                &target_opts,
                                actions,
                            )
                            .await
                            .context("Create super table error")?;
                            // 临时代码，保证编译通过
                            let metrics_arc =
                                Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::default()));
                            sync_super_table_schema_with_subs(
                                &from,
                                &stable,
                                &[source_table_name],
                                taos,
                                None,
                                &target_opts,
                                true,
                                true,
                                actions,
                                &metrics_arc,
                            )
                            .await
                            .context("Create sub table error")?;
                        } else {
                            // normal table
                            sync_normal_table_schema(&from, source_table_name, actions, None, taos)
                                .await
                                .context("Create table error")?;
                        }
                        continue;
                    }
                    0x060B => {
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0x060B: the primary timestamp is out of range
                        let cancel = CancellationToken::new();
                        let _guard = cancel.clone().drop_guard();
                        let mut target_conn = Some(target.get().await?);
                        let (_precision, min, max) = taosx_core::utils::sql::get_timestamp_range(
                            target,
                            &mut target_conn,
                            5,
                            &cancel,
                        )
                        .await?;

                        let valid = unsafe { raw.column_views()[0].as_timestamp_view() }
                            .iter()
                            .map(|v| {
                                v.map(|ts| {
                                    let ts = ts.to_datetime_with_tz();
                                    ts > min && ts < max
                                })
                                .unwrap_or(false)
                            })
                            .collect_vec();

                        let (valid, invalid) = raw.partition_by(&valid);
                        if let Some(invalid) = invalid {
                            tracing::warn!(
                                "Timestamp out of range block: {}",
                                invalid.pretty_format()
                            );
                            metrics.add_out_of_range_rows(invalid.nrows() as _);
                        }
                        if let Some(valid) = valid {
                            taos.write_raw_block(&valid).await?;
                        }
                    }
                    0x061B => {
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0x061B: invalid table schema version
                        let _ = taos.describe(raw.table_name().unwrap()).await;
                        let mut max_retries = 5;
                        loop {
                            if let Err(err) = taos.write_raw_block(raw).await {
                                if max_retries == 0 {
                                    Err(err).context("Try to fix 0x061B error failed")?;
                                } else {
                                    max_retries -= 1;
                                }
                            }
                        }
                    }
                    _ => Err(err)?,
                }
            } else if let Some(meta) = raw.to_create() {
                let sql = meta.to_string();
                taos.exec(&sql)
                    .await
                    .with_context(|| format!("SQL: {sql}"))?;
            } else {
                Err(err)?
            }
        };

        break;
    }
    anyhow::Ok(())
}

impl Worker {
    async fn write_meta_fallback(&mut self, message: &RawMessage) -> Result<()> {
        let conn = self.target_connection.as_ref().unwrap();
        let metrics = self.metrics.as_ref().tmq();
        let meta = message.meta.as_ref().unwrap();
        let sqls = meta.iter().map(ToString::to_string).collect_vec();
        execute_many_sql(conn, sqls)
            .in_current_span()
            .await
            .context("Write raw meta with sql error")?;
        metrics.add_suc_blocks(1);
        Ok(())
    }

    fn try_mutate_meta(&mut self, json_meta: &mut JsonMeta) -> Result<bool> {
        let mut meta_changed = false;
        match (self.options.with_meta_delete, self.options.with_meta_drop) {
            (true, true) => {
                // do nothing, all kinds of meta are allowed.
            }
            (true, false) => {
                if json_meta
                    .iter()
                    .any(|unit| matches!(unit, MetaUnit::Drop(_)))
                {
                    // skip drop meta
                    match json_meta {
                        JsonMeta::Single(_) => {
                            *json_meta = JsonMeta::Plural {
                                metas: vec![],
                                tmq_meta_version: FastStr::from_static_str("1"),
                            };
                        }
                        JsonMeta::Plural { metas, .. } => {
                            metas.retain(|unit| matches!(unit, MetaUnit::Drop(_)));
                        }
                    }

                    meta_changed = true;
                }
            }
            (false, true) => {
                if json_meta
                    .iter()
                    .any(|unit| matches!(unit, MetaUnit::Delete(_)))
                {
                    match json_meta {
                        JsonMeta::Single(_) => {
                            *json_meta = JsonMeta::Plural {
                                metas: vec![],
                                tmq_meta_version: FastStr::from_static_str("1"),
                            };
                        }
                        JsonMeta::Plural { metas, .. } => {
                            metas.retain(|unit| matches!(unit, MetaUnit::Delete(_)));
                        }
                    }

                    meta_changed = true;
                }
            }
            (false, false) => {
                if json_meta
                    .iter()
                    .any(|unit| matches!(unit, MetaUnit::Delete(_) | MetaUnit::Drop(_)))
                {
                    match json_meta {
                        JsonMeta::Single(_) => {
                            *json_meta = JsonMeta::Plural {
                                metas: vec![],
                                tmq_meta_version: FastStr::from_static_str("1"),
                            };
                        }
                        JsonMeta::Plural { metas, .. } => {
                            metas.retain(|unit| {
                                matches!(unit, MetaUnit::Drop(_) | MetaUnit::Delete(_))
                            });
                        }
                    }

                    meta_changed = true;
                }
            }
        }

        for action in self.options.actions.as_ref() {
            action.mutate_meta(json_meta)?;
            if !meta_changed {
                meta_changed = true;
            }
        }
        Ok(meta_changed)
    }

    fn try_mutate_data(&mut self, data: &mut Vec<RawBlock>) -> Result<()> {
        for raw in data {
            if let Some(name) = self.table.as_ref() {
                if self.options.actions.is_empty() {
                    raw.with_table_name(name.to_string());
                    tracing::debug!(
                        "Write into {name} {} rows with {} columns",
                        raw.nrows(),
                        raw.ncols()
                    );
                } else {
                    let mut name = name.to_string();
                    for action in self.options.actions.iter() {
                        match action {
                            Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
                                rename.apply_in_place(&mut name)?
                            }
                            _ => (),
                        }
                    }
                    raw.with_table_name(&name);
                    tracing::debug!(
                        "Write into {name} {} rows with {} columns",
                        raw.nrows(),
                        raw.ncols()
                    );
                }
            } else if let Some(name) = raw.table_name() {
                if !self.options.actions.is_empty() {
                    let mut name = name.to_string();
                    for action in self.options.actions.iter() {
                        match action {
                            Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
                                rename.apply_in_place(&mut name)?
                            }
                            _ => (),
                        }
                    }
                    raw.with_table_name(&name);
                    tracing::debug!(
                        "Write into {name} {} rows with {} columns",
                        raw.nrows(),
                        raw.ncols()
                    );
                }
            } else {
                // 会走到这里吗？
                tracing::debug!("write {} rows with {} columns", raw.nrows(), raw.ncols());
            }
        }
        Ok(())
    }
    async fn write_meta_only(&mut self, message: &RawMessage) -> Result<()> {
        let conn = self.target_connection.as_ref().unwrap();
        let now = std::time::Instant::now();
        let res = conn.write_raw_meta(&message.raw).await;
        let elapsed = now.elapsed();
        tracing::debug!(
            elapsed = elapsed.as_millis(),
            bytes = message.raw.raw_len(),
            "Write raw meta finished"
        );
        self.metrics
            .as_ref()
            .tmq()
            .add_write_cost_ms(elapsed.as_millis() as _);

        if let Err(err) = res {
            // metrics.add_write_raw_fails(1);
            // Print error no matter how we will deal with it, so that we can know what happened.
            tracing::debug!("Write raw meta error: {err:#}");
            let code = *err.code().deref();
            let meta = message.meta.as_ref().unwrap();
            match code {
                // Table not exist error codes.
                0x0218 | 0x2603 | 0x036D | 0x0618 => {
                    // 0x0218: the table does not exist
                    // 0x2603: the table does not exist
                    // 0x036D: the table does not exist
                    // 0x0618: the table does not exist
                    for json_meta in meta {
                        match json_meta {
                            MetaUnit::Create(create) => match create {
                                MetaCreate::Super {
                                    table_name: _,
                                    columns: _,
                                    tags: _,
                                } => {
                                    // Stable should never not exist.
                                    continue;
                                }
                                MetaCreate::Child {
                                    table_name,
                                    using,
                                    tags: _,
                                    tag_num: _,
                                } => {
                                    // Create child table error means stable not exist.
                                    tracing::warn!(
                                        "Table does not exist: {using} while create child {table_name}. Sync super table schema."
                                    );
                                    let from = self.source.get().await?;
                                    sync_super_table_schema(
                                        &from,
                                        using,
                                        conn,
                                        None,
                                        &Default::default(),
                                        &[],
                                    )
                                    .await?;
                                    if let Err(err) = conn.write_raw_meta(&message.raw).await {
                                        // metrics.add_write_raw_fails(1);
                                        Err(err.context(format!(
                                            "Write raw meta error with table {table_name}"
                                        )))?;
                                    }
                                }
                                MetaCreate::Normal {
                                    table_name: _,
                                    columns: _,
                                } => {
                                    // Normal table should never not exist.
                                    continue;
                                }
                            },
                            // Do nothing if not create
                            MetaUnit::Alter(_) | MetaUnit::Drop(_) | MetaUnit::Delete(_) => {
                                tracing::warn!(
                                    "Unexpected error {err:#} for meta: ```{json_meta}```, do nothing."
                                );
                            }
                        }
                    }
                }
                0x032C | 0x0115 | 0x03C7 | 0x03D3 => {
                    // 0x032C: object is creating
                    // 0x0115: invalid msg
                    // 0x03C7: stable uid not match
                    // 0x03D3: conflict transaction not completed
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let _ = conn.write_raw_meta(&message.raw).await.inspect_err(|err| {
                        tracing::debug!(
                            error = format!("{err:#}"),
                            "retry write raw with code {code}"
                        );
                    });
                }
                _ => {
                    self.write_meta_fallback(message).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn write_blocks(&mut self, blocks: &mut [RawBlock]) -> Result<()> {
        let taos = self.target_connection.as_ref().unwrap();
        let metrics = self.metrics.as_ref().tmq();
        let mut rows = 0;
        let actions = self.options.actions.as_ref();
        for raw in blocks {
            let source_table_name = raw
                .table_name()
                .filter(|name| !name.is_empty())
                .map(|name| name.to_owned());
            tracing::debug!(
                "worker try to write raw block: {}",
                raw.pretty_format().to_string()
            );
            if let Some(name) = self.table.as_ref().map(|s| s.as_str()) {
                let mut name = name.to_string();
                for action in actions {
                    match action {
                        Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
                            rename.apply_in_place(&mut name)?
                        }
                        _ => (),
                    }
                }
                raw.with_table_name(&name);
                tracing::debug!(
                    "Write into {name} {} rows(total {}) with {} columns",
                    raw.nrows(),
                    rows,
                    raw.ncols()
                );
            } else if let Some(name) = raw.table_name() {
                let mut name = name.to_string();
                for action in actions {
                    match action {
                        Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
                            rename.apply_in_place(&mut name)?
                        }
                        _ => (),
                    }
                }
                if !actions.is_empty() {
                    raw.with_table_name(&name);
                }
                tracing::debug!(
                    "write into {name} {} rows with {} columns",
                    raw.nrows(),
                    raw.ncols()
                );
            } else {
                // 会走到这里吗？
                tracing::debug!(
                    "write {} rows(total {}) with {} columns",
                    raw.nrows(),
                    rows,
                    raw.ncols()
                );
            }
            rows += raw.nrows();
            let raw_block_context = || format!("Error with block: {}", raw.pretty_format());

            let with_raw = true;
            if with_raw {
                if let Err(err) = write_with_raw_block(
                    actions,
                    &self.topic,
                    &self.source,
                    &self.target,
                    taos,
                    raw,
                    source_table_name.as_deref(),
                    metrics,
                )
                .await
                {
                    tracing::warn!(
                        table = raw.table_name(),
                        "Write raw block error: {err:#}, try STMT mode"
                    );
                    let with_stmt = async {
                        let mut stmt = Stmt::init(taos)
                            .await
                            .context("Write with stmt init error")?;
                        let fields = raw.fields();
                        let question_masks = std::iter::repeat_n('?', fields.len()).join(",");
                        let table = raw.table_name().unwrap();
                        stmt.prepare(&format!("INSERT INTO `{table}` VALUES({question_masks})"))
                            .await
                            .context("Write with stmt prepare error")?;

                        stmt.bind(raw.column_views())
                            .await
                            .context("Write with stmt bind error")?;
                        stmt.add_batch()
                            .await
                            .context("Write with stmt add_batch error")?;
                        stmt.execute()
                            .await
                            .context("Write with stmt execute error")?;
                        anyhow::Ok(())
                    };
                    if let Err(err) = with_stmt.await {
                        tracing::warn!(
                            table = raw.table_name(),
                            "Write with stmt error: {err}, try SQL mode"
                        );
                        pub trait RawBlockToSql {
                            fn to_sql(&self) -> String;
                        }
                        impl RawBlockToSql for RawBlock {
                            fn to_sql(&self) -> String {
                                let table = self.table_name().unwrap();
                                let fields = self
                                    .field_names()
                                    .iter()
                                    .map(|s| format!("`{s}`"))
                                    .join(",");
                                let values = self
                                    .rows()
                                    .map(|row| {
                                        let row = row.map(|(_name, v)| v.to_sql_value()).join(",");
                                        format!("({row})")
                                    })
                                    .join(" ");

                                format!("INSERT INTO `{table}` ({fields}) VALUES {values}")
                            }
                        }

                        let sql = raw.to_sql();
                        if let Err(err) = taos.exec(&sql).await {
                            tracing::warn!(table = raw.table_name(), "Write with sql error: {err}");
                            metrics.add_write_raw_fails(1);
                            Err(err)
                                .with_context(raw_block_context)
                                .context("Write raw block into target error")?;
                        }
                    }
                }
            } else {
                let with_stmt = async {
                    let mut stmt = Stmt::init(taos)
                        .await
                        .context("Write with stmt init error")?;
                    let fields = raw.fields();
                    let question_masks = std::iter::repeat_n('?', fields.len()).join(",");
                    let table = raw.table_name().unwrap();
                    stmt.prepare(&format!("INSERT INTO `{table}` VALUES({question_masks})"))
                        .await
                        .context("Write with stmt prepare error")?;

                    stmt.bind(raw.column_views())
                        .await
                        .context("Write with stmt bind error")?;
                    stmt.add_batch()
                        .await
                        .context("Write with stmt add_batch error")?;
                    stmt.execute()
                        .await
                        .context("Write with stmt execute error")?;
                    anyhow::Ok(())
                };
                with_stmt
                    .await
                    .inspect_err(|_| metrics.add_write_raw_fails(1))
                    .with_context(raw_block_context)
                    .context("write table with stmt error")?;
            }
            metrics.add_suc_blocks(1);
            metrics.add_written_rows(raw.nrows() as _);
            metrics.add_written_points((raw.nrows() * raw.ncols()) as _);
        }
        tracing::debug!(
            "End writing data, current written rows {}",
            metrics.written_rows()
        );
        Ok(())
    }

    pub async fn write_block(&mut self, raw: &mut RawBlock) -> Result<()> {
        if self.target_connection.is_none() {
            self.target_connection = Some(self.target.get().await?);
        }
        let taos = self.target_connection.as_ref().unwrap();
        let metrics = self.metrics.as_ref().tmq();

        let actions = self.options.actions.as_ref();

        let source_table_name = raw.table_name().and_then(|name| {
            if name.is_empty() {
                None
            } else {
                Some(name.to_owned())
            }
        });
        tracing::trace!(
            source.table = source_table_name,
            "sync block with {} rows {} cols",
            raw.nrows(),
            raw.ncols()
        );
        if let Some(name) = self.table.as_ref().map(|s| s.as_str()) {
            if actions.is_empty() {
                raw.with_table_name(name);
                tracing::debug!(
                    "Write into {name} {} rows with {} columns",
                    raw.nrows(),
                    raw.ncols()
                );
            } else {
                let mut name = name.to_string();
                for action in actions {
                    match action {
                        Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
                            rename.apply_in_place(&mut name)?
                        }
                        _ => (),
                    }
                }
                raw.with_table_name(&name);
                tracing::debug!(
                    "Write into {name} {} rows with {} columns",
                    raw.nrows(),
                    raw.ncols()
                );
            }
        } else if let Some(name) = raw.table_name() {
            if !actions.is_empty() {
                let mut name = name.to_string();
                for action in actions {
                    match action {
                        Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
                            rename.apply_in_place(&mut name)?
                        }
                        _ => (),
                    }
                }
                raw.with_table_name(&name);
                tracing::debug!(
                    "write into {name} {} rows with {} columns",
                    raw.nrows(),
                    raw.ncols()
                );
            }
        } else {
            // 会走到这里吗？
            tracing::debug!("write {} rows with {} columns", raw.nrows(), raw.ncols());
        }
        let raw_block_context = || format!("Error with block: {}", raw.pretty_format());

        let with_raw = true;
        if with_raw {
            let with_raw_block = async {
                if let Err(err) = taos.write_raw_block(raw).await {
                    let code = *err.code().deref();
                    tracing::debug!("Try to recover from error: {err}");
                    if let Some(source_table_name) = source_table_name {
                        match code {
                            0x0118 => {
                                // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                // 0x0118: invalid parameter
                                let from = self.source.get().await?;
                                // sync schema
                                // let source_stable_name = from.query_one::<_, String>(format!("select stable_name from information_schema.ins_tables where db_name = database() and table_name = '{source_table_name}'")).await?;
                                let source_stable_name =
                                    get_stable_name(&from, None, &source_table_name).await?;
                                if let Some(mut source_stable_name) = source_stable_name {
                                    if actions.is_empty() {
                                        migrate_data_schema(
                                            &raw.fields(),
                                            taos,
                                            &source_table_name,
                                        )
                                        .await?;
                                    } else {
                                        for action in actions {
                                            match action {
                                                Action::RenameTable(rename)
                                                | Action::RenameSuperTable(rename) => rename
                                                    .apply_in_place(&mut source_stable_name)?,
                                                _ => (),
                                            }
                                        }
                                        migrate_data_schema(
                                            &raw.fields(),
                                            taos,
                                            &source_stable_name,
                                        )
                                        .await?;
                                    }
                                } else {
                                    let table = raw.table_name().unwrap();
                                    migrate_data_schema(&raw.fields(), taos, table).await?;
                                }
                                taos.write_raw_block(raw).await.context(
                                    "Write raw block into target error after 0x0118 fix",
                                )?;
                            }
                            0x0218 | 0x2603 | 0x2662 | 0x036D | 0x0618 => {
                                // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                // 0x0218: the table does not exist
                                // 0x2603: the table does not exist
                                // 0x2662: the table does not exist
                                // 0x036D: the table does not exist
                                // 0x0618: the table does not exist
                                let from = self.source.get().await?;
                                let database = self.topic.database.as_str();
                                if self.topic.is_query() {
                                    // sync as normal table.
                                    sync_normal_table_schema(
                                        &from,
                                        &source_table_name,
                                        actions,
                                        None,
                                        taos,
                                    )
                                    .await
                                    .context("Create table error")?;
                                }
                                let super_table_name =
                                    get_stable_name(&from, Some(database), &source_table_name)
                                        .await?;
                                // if let Some(stable) = from.query_one::<_, String>(format!("select stable_name from information_schema.ins_tables where db_name = '{database}' and table_name = '{source_table_name}'")).await?.and_then(|s| if s.is_empty() { None } else { Some(s) }) {
                                if let Some(stable) = super_table_name {
                                    let from = self.source.get().await?;
                                    let target_opts = Default::default();
                                    sync_super_table_schema(
                                        &from,
                                        &stable,
                                        taos,
                                        None,
                                        &target_opts,
                                        actions,
                                    )
                                    .await
                                    .context("Create super table error")?;
                                    // 临时代码，保证编译通过
                                    let metrics_arc = Arc::new(CoreMetrics::Legacy(
                                        LegacyToTaosMetrics::default(),
                                    ));
                                    sync_super_table_schema_with_subs(
                                        &from,
                                        &stable,
                                        &[source_table_name],
                                        taos,
                                        None,
                                        &target_opts,
                                        true,
                                        true,
                                        &self.options.actions,
                                        &metrics_arc,
                                    )
                                    .await
                                    .context("Create sub table error")?;
                                    taos.write_raw_block(raw)
                                        .await
                                        .context("Write raw block into target error")?;
                                } else {
                                    // normal table
                                    sync_normal_table_schema(
                                        &from,
                                        &source_table_name,
                                        actions,
                                        None,
                                        taos,
                                    )
                                    .await
                                    .context("Create table error")?;
                                    taos.write_raw_block(raw)
                                        .await
                                        .context("Write raw block into target error")?;
                                }
                            }
                            0x061B => {
                                // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                // 0x061B: invalid table schema version
                                let _ = taos.describe(raw.table_name().unwrap()).await;
                                let mut max_retries = 5;
                                loop {
                                    if let Err(err) = taos.write_raw_block(raw).await {
                                        if max_retries == 0 {
                                            Err(err).context("Try to fix 0x061B error failed")?;
                                        } else {
                                            max_retries -= 1;
                                        }
                                    }
                                }
                            }
                            _ => Err(err)?,
                        }
                    } else if let Some(meta) = raw.to_create() {
                        let sql = meta.to_string();
                        taos.exec(&sql)
                            .in_current_span()
                            .await
                            .with_context(|| format!("SQL: {sql}"))?;
                    } else {
                        Err(err)?
                    }
                };
                anyhow::Ok(())
            };
            with_raw_block
                .await
                .inspect_err(|_| metrics.add_write_raw_fails(1))
                .with_context(raw_block_context)
                .context("Write raw block into target error")?
        } else {
            let with_stmt = async {
                let mut stmt = Stmt::init(taos)
                    .await
                    .context("Write with stmt init error")?;
                let fields = raw.fields();
                let question_masks = std::iter::repeat_n('?', fields.len()).join(",");
                let table = raw.table_name().unwrap();
                stmt.prepare(&format!("INSERT INTO `{table}` VALUES({question_masks})"))
                    .await
                    .context("Write with stmt prepare error")?;

                stmt.bind(raw.column_views())
                    .await
                    .context("Write with stmt bind error")?;
                stmt.add_batch()
                    .await
                    .context("Write with stmt add_batch error")?;
                stmt.execute()
                    .await
                    .context("Write with stmt execute error")?;
                anyhow::Ok(())
            };
            with_stmt
                .await
                .inspect_err(|_| metrics.add_write_raw_fails(1))
                .with_context(raw_block_context)
                .context("write table with stmt error")?;
        }
        metrics.add_suc_blocks(1);
        metrics.add_written_rows(raw.nrows() as _);
        metrics.add_written_points((raw.nrows() * raw.ncols()) as _);
        tracing::debug!(
            "End writing block, current written rows {}",
            metrics.written_rows()
        );
        Ok(())
    }

    pub async fn write(&mut self, message: &mut RawMessage) -> Result<()> {
        if self.target_connection.is_none() {
            self.target_connection = Some(self.target.get().await?);
        }
        let mid = message.mid;
        let rows = message.rows();

        let mut raw_changed = false;
        if message.meta.is_some() {
            // Meta only
            raw_changed = self.try_mutate_meta(message.meta.as_mut().unwrap())?;
            if raw_changed {
                self.write_meta_fallback(message).in_current_span().await?;
            } else {
                self.write_meta_only(message).in_current_span().await?;
            }
            if message.data.is_none() {
                return Ok(());
            }
        }

        if let Some(data) = message.data.as_mut() {
            if raw_changed || self.options.strategy.by_block() {
                self.try_mutate_data(data)?;
                self.write_blocks(data).in_current_span().await?;
                return Ok(());
            }
        }

        let conn = self.target_connection.as_ref().unwrap();
        let now = std::time::Instant::now();
        let res = conn.write_raw_meta(&message.raw).in_current_span().await;
        let elapsed = now.elapsed();
        tracing::debug!(
            mid,
            mty = message.mty as u8,
            elapsed = elapsed.as_millis(),
            rows,
            bytes = message.raw.raw_len(),
            "Write raw finished"
        );
        self.metrics
            .as_ref()
            .tmq()
            .add_write_raw_cost_ms(elapsed.as_millis() as _);
        if let Err(err) = res {
            // metrics.add_write_raw_fails(1);
            // Print error no matter how we will deal with it, so that we can know what happened.
            tracing::info!(error = format!("{err:#}"), "Write raw data error: {err:#}");
            let code = *err.code().deref();
            if message.meta.is_none() && message.data.is_none() {
                if code == 0x0118 {
                    // Invalid parameters,detail:table:t_1e2f01eda79dc691cf0e7e3f3730d43b, err:column var data bytes error, name:field2, schema type:VARCHAR, bytes:10, data type:VARCHAR, bytes:18
                    let errstr = err.to_string();
                    lazy_static::lazy_static! {
                        static ref RE: regex::Regex = regex::Regex::new(r"table:(?P<table>\S+),").unwrap();
                    }
                    if let Some(table) = RE.captures(&errstr).and_then(|caps| caps.name("table")) {
                        let source_table_name = table.as_str();
                        tracing::warn!(
                            "Invalid parameters for table {source_table_name}, try to sync schema."
                        );
                        let from = self.source.get().await?;
                        let database = self.topic.database.as_str();
                        if self.topic.is_query() {
                            // sync as normal table.
                            sync_normal_table_schema(&from, source_table_name, &[], None, conn)
                                .await
                                .context("Create table error")?;
                        }
                        let super_table_name =
                            get_stable_name(&from, Some(database), source_table_name).await?;
                        // if let Some(stable) = from.query_one::<_, String>(format!("select stable_name from information_schema.ins_tables where db_name = '{database}' and table_name = '{source_table_name}'")).await?.and_then(|s| if s.is_empty() { None } else { Some(s) }) {
                        if let Some(stable) = super_table_name {
                            let target_opts = Default::default();
                            sync_super_table_schema(&from, &stable, conn, None, &target_opts, &[])
                                .await
                                .context("Create super table error")?;
                            // 临时代码，保证编译通过
                            let metrics_arc =
                                Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::default()));
                            sync_super_table_schema_with_subs(
                                &from,
                                &stable,
                                &[source_table_name],
                                conn,
                                None,
                                &target_opts,
                                true,
                                true,
                                &[],
                                &metrics_arc,
                            )
                            .await
                            .context("Create sub table error")?;
                        } else {
                            // normal table
                            sync_normal_table_schema(&from, source_table_name, &[], None, conn)
                                .await
                                .context("Create table error")?;
                        }
                    }
                } else {
                    tracing::error!("Write raw into target error: {err:#}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                if let Err(err2) = conn.write_raw_meta(&message.raw).await.inspect_err(|err| {
                    tracing::debug!(
                        error = format!("{err:#}"),
                        "retry write raw with code {}",
                        err.code()
                    );
                }) {
                    return Err(err)
                        .with_context(|| format!("Retry error: {err2}"))
                        .context("Write raw message into target error");
                }
            }
            if let Some(meta) = &message.meta {
                match code {
                    // Table not exist error codes.
                    0x0218 | 0x2603 | 0x036D | 0x0618 => {
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0x0218: the table does not exist
                        // 0x2603: the table does not exist
                        // 0x036D: the table does not exist
                        // 0x0618: the table does not exist
                        for json_meta in meta {
                            match json_meta {
                                MetaUnit::Create(create) => match create {
                                    MetaCreate::Super {
                                        table_name: _,
                                        columns: _,
                                        tags: _,
                                    } => {
                                        // Stable should never not exist.
                                        continue;
                                    }
                                    MetaCreate::Child {
                                        table_name,
                                        using,
                                        tags: _,
                                        tag_num: _,
                                    } => {
                                        // Create child table error means stable not exist.
                                        tracing::warn!(
                                            "Table does not exist: {using} while create child {table_name}. Sync super table schema."
                                        );
                                        let from = self.source.get().await?;
                                        sync_super_table_schema(
                                            &from,
                                            using,
                                            conn,
                                            None,
                                            &Default::default(),
                                            &[],
                                        )
                                        .await?;
                                        if let Err(err) = conn.write_raw_meta(&message.raw).await {
                                            // metrics.add_write_raw_fails(1);
                                            Err(err.context(format!(
                                                "Write raw meta error with table {table_name}"
                                            )))?;
                                        }
                                    }
                                    MetaCreate::Normal {
                                        table_name: _,
                                        columns: _,
                                    } => {
                                        // Normal table should never not exist.
                                        continue;
                                    }
                                },
                                // Do nothing if not create
                                MetaUnit::Alter(_) | MetaUnit::Drop(_) | MetaUnit::Delete(_) => {
                                    tracing::warn!(
                                        "Unexpected error {err:#} for meta: ```{json_meta}```, do nothing."
                                    );
                                }
                            }
                        }
                    }
                    0x0603 => {
                        // 0x0603: table already exists
                        // Fallback to sql method.
                        tracing::debug!("Fallback to sql method due to: {err:#}.");
                        let sqls = meta.iter().map(ToString::to_string).collect_vec();
                        let _ = execute_many_sql(conn, sqls)
                            .in_current_span()
                            .await
                            .context("Write raw meta with sql error");
                    }
                    0x032C | 0x0115 | 0x03C7 | 0x03D3 | 0x0900..=0x09FF => {
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0x032C: object is creating
                        // 0x0115: invalid msg
                        // 0x03C7: stable uid not match
                        // 0x03D3: conflict transaction not completed
                        // 0x0900..=0x09FF: sync error
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        // let _ = conn;
                        // let _ = self.target_connection.take();
                        // self.target_connection.replace(self.target.get().await?);
                        // let conn = self.target_connection.as_ref().unwrap();
                        // // let _ = conn.exec("show tables").await;
                        let _ = conn.write_raw_meta(&message.raw).await.inspect_err(|err| {
                            tracing::debug!(
                                error = format!("{err:#}"),
                                "retry write raw with code {code}"
                            );
                        });
                    }
                    _ => {
                        // Fallback to sql method.
                        tracing::debug!("Fallback to sql method due to: {err:#}.");
                        let sqls = meta.iter().map(ToString::to_string).collect_vec();
                        execute_many_sql(conn, sqls)
                            .in_current_span()
                            .await
                            .context("Write raw meta with sql error")?;
                    }
                }
            }
            if let Some(data) = &mut message.data {
                match code {
                    // Table not exist error codes or invalid input.
                    0x070F | 0x0218 | 0x2603 | 0x036D | 0x0618 | 0x2662 | 0x0118 | 0x4000
                    | 0x0603 => {
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0x070F: invalid input
                        // 0x0218: the table does not exist
                        // 0x2603: the table does not exist
                        // 0x036D: the table does not exist
                        // 0x0618: the table does not exist
                        // 0x2662: the table does not exist
                        // 0x0118: invalid parameter
                        // 0x4000: invalid msg
                        // 0x0603: table already exists
                        tracing::debug!("Fallback to block-by-block method due to: {err:#}.");
                        self.try_mutate_data(data)?;
                        self.write_blocks(data).await?;
                    }
                    0x060B => {
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0x060B: the primary timestamp out of range
                        let cancel = CancellationToken::new();
                        let _guard = cancel.clone().drop_guard();
                        let (_precision, min, max) = taosx_core::utils::sql::get_timestamp_range(
                            &self.target,
                            &mut self.target_connection,
                            5,
                            &cancel,
                        )
                        .await?;

                        let valid = unsafe { data[0].column_views()[0].as_timestamp_view() }
                            .iter()
                            .map(|v| {
                                v.map(|ts| {
                                    let ts = ts.to_datetime_with_tz();
                                    ts > min && ts < max
                                })
                                .unwrap_or(false)
                            })
                            .collect_vec();

                        let mut blocks = data
                            .iter()
                            .filter_map(|block| {
                                let (valid, invalid) = block.partition_by(&valid);
                                if let Some(invalid) = invalid {
                                    tracing::warn!(
                                        "Timestamp out of range block: {}",
                                        invalid.pretty_format()
                                    );
                                    self.metrics
                                        .as_ref()
                                        .tmq()
                                        .add_out_of_range_rows(invalid.nrows() as _);
                                }
                                valid
                            })
                            .collect_vec();

                        if !blocks.is_empty() {
                            self.write_blocks(&mut blocks).await?;
                        }
                    }
                    _ => {
                        self.try_mutate_data(data)?;
                        // metrics.add_write_raw_fails(1);
                        tracing::error!("Write data error: {err}");
                        // let block = data.fetch_raw_block().await;
                        if let Some(block) = data.first() {
                            tracing::error!(
                                "Details about the failed data: {}",
                                block.pretty_format()
                            );
                        }
                        Err(err).context("Write raw data into target error")?;
                    }
                }
            }
        }

        Ok(())
    }
}

pub async fn get_stable_name(
    taos: &Taos,
    database: Option<&str>,
    tablename: &str,
) -> Result<Option<String>> {
    let database_name;
    if database.is_some() {
        database_name = database.unwrap().to_string();
    } else {
        let database: Option<String> = taos.query_one("select database()").await?;
        database_name = database.expect("get database name withe 'select database()'");
    }

    let show_create_table_result: Option<(String, String)> = taos
        .query_one(format!("show create table `{database_name}`.`{tablename}`"))
        .await?;
    if let Some((_, sql_create_table)) = show_create_table_result {
        let regex = regex::Regex::new(r"`\sUSING\s`(.+?)`\s").unwrap();
        for cap in regex.captures_iter(&sql_create_table) {
            let cap_str = cap.get(1);
            if let Some(cap_str) = cap_str {
                return Ok(Some(cap_str.as_str().to_string()));
            }
        }
        tracing::warn!("No stable name found in sql: {}", sql_create_table);
        Ok(None)
    } else {
        tracing::warn!("No table found in database: {database_name} with table name: {tablename}");
        Ok(None)
    }
}

// mod test {
//     use taos::*;
//     use super::get_stable_name;

//     #[tokio::test]
//     async fn test_query_stable_name_with_taos() -> anyhow::Result<()> {
//         let taos = TaosBuilder::from_dsn("taos+ws://192.168.0.201:6041/test")?.build().await?;

//         let stable_name = get_stable_name(&taos, None, "d1").await?;
//         println!("stablename: {:?}", stable_name);

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_query_stable_name_with_database_name_with_taos() -> anyhow::Result<()> {
//         let taos = TaosBuilder::from_dsn("taos+ws://192.168.0.201:6041")?.build().await?;

//         let stable_name = get_stable_name(&taos, Some("test"), "d1").await?;
//         println!("stablename: {:?}", stable_name);

//         Ok(())
//     }
// }

#[async_trait::async_trait]
impl deadpool::managed::Manager for Worker {
    type Type = Worker;
    type Error = ();

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Ok(self.clone())
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &Metrics,
    ) -> RecycleResult<Self::Error> {
        Ok(())
    }
}
