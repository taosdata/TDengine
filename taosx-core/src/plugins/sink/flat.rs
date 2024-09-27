use std::{
    any::Any,
    clone::Clone,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Context};
use arrow_schema::ArrowError;
use async_backtrace::framed;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use lazy_static::lazy_static;
use serde_json::json;
use taos::{taos_query::Manager, AsyncQueryable, Itertools, RawBlock, TaosBuilder, TaosPool, Ty};
use taoslog::{
    utils::{QidMetadataGetter, QidMetadataSetter, Span},
    QidManager,
};
use taosx_ipc::{
    ack::LushAck,
    stream::{flat::FlatMessage, reader::IpcMessage},
};
use thiserror::Error;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, trace, Instrument};

use crate::{
    core_metrics::{CoreMetrics, TaskMetrics},
    plugins::transform::MessageArrowRecords,
    sink::{consume_flat_record, DEFAULT_MAX_RETRIES_FOR_CONNECTION},
    utils::{
        sql::{
            describe_table_with_connection_retries, exec_sql_with_connection_retries,
            get_minimum_timestamp, values_to_sqls,
        },
        trace::{BatchCounter, Qid},
    },
    Parser,
};

use super::{ipc_metric::IpcMetrics, IpcErrorStrategy};

/// All the messages should be in the same stable.
pub(crate) fn message_to_sql<'a>(
    messages: impl IntoIterator<Item = &'a MessageArrowRecords>,
    precision: taos::Precision,
    with_meta: bool,
    with_field_names: bool,
) -> Vec<Records> {
    messages
        .into_iter()
        .chunk_by(|m| m.stable_name())
        .into_iter()
        .map(|(key, group)| {
            let values = group
                .into_iter()
                .flat_map(|m| m.sql_insert_part(precision, with_meta, with_field_names))
                .collect_vec();
            let stable_name_iter = std::iter::repeat(key);

            values_to_sqls(&values)
                .into_iter()
                .zip(stable_name_iter)
                .map(|((sql, tables, records), stable)| Records {
                    stable: stable.map(|s| s.to_string()),
                    sql,
                    tables,
                    records,
                })
        })
        .flatten()
        .collect_vec()
}

/// Write records to TDengine with `sql`, which contains `tables` num of tables,
/// and `records` number of records.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Records {
    pub stable: Option<String>,
    pub sql: String,
    pub tables: usize,
    pub records: usize,
}
impl Records {
    #[inline]
    pub fn sql(&self) -> &str {
        self.sql.as_str()
    }

    #[inline]
    pub fn records(&self) -> usize {
        self.records
    }

    #[inline]
    pub fn stable(&self) -> Option<&str> {
        self.stable.as_deref()
    }
}
impl<'a> AsRef<str> for Records {
    fn as_ref(&self) -> &str {
        self.sql.as_str()
    }
}

type TaosConnection = deadpool::managed::Object<Manager<TaosBuilder>>;

#[derive(Debug, Error)]
pub enum FlatWriteError {
    #[error("Connection error")]
    ConnectionPoolError(#[from] deadpool::managed::PoolError<taos::Error>),
    #[error("Table not exists")]
    TableNotExits(String),
    #[error("Container length too short: {0:#}")]
    ContainerLengthTooShort(String),
    #[error("Write SQL error: {0:#}")]
    Taos(#[from] taos::Error),
    #[error("Arrow internal error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

async fn assert_create_stable(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    sql: &str,
    req_id: u64,
    cancel: &CancellationToken,
) -> Result<(), FlatWriteError> {
    match exec_sql_with_connection_retries(
        pool,
        taos,
        sql,
        req_id,
        DEFAULT_MAX_RETRIES_FOR_CONNECTION,
        cancel,
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            let code = err.code();
            let errno: i32 = code.into();
            tracing::warn!(sql, "Exec SQL error: {err:#}");
            match errno {
                0x032C | 0x0603 | 0x03C7 | 0x03D3 | 0x0360 => Ok(()),
                _ => Err(err).context("Create stable error").map_err(Into::into),
            }
        }
    }
}

lazy_static! {
    static ref RE_0X2653: regex::Regex =
        regex::Regex::new(r"`Value too long for column/tag: (.*)`").unwrap();
}

#[instrument(skip_all)]
async fn write_stable_with_sql(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    req_id: u64,
    records: &Records,
    cancel: &CancellationToken,
) -> Result<usize, FlatWriteError> {
    let sql = records.sql();
    tracing::trace!("write stable with sql: {}", sql);
    let res = if sql.find(|c| c == '\0').is_some() {
        tracing::warn!("SQL contains null character, remove it");
        let sql: String = sql.chars().filter(|c| *c != '\0').collect();
        exec_sql_with_connection_retries(
            pool,
            taos,
            &sql,
            req_id,
            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
            cancel,
        )
        .await
    } else {
        exec_sql_with_connection_retries(
            pool,
            taos,
            sql,
            req_id,
            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
            cancel,
        )
        .await
    };

    match res {
        Ok(n) => Ok(n),
        Err(err) => {
            let code = err.code();
            let errno: i32 = code.into();
            tracing::warn!(
                sql,
                "flat message write sql encountered unrecoverable err: {err:#}"
            );
            match errno {
                0x2603 | 0x0618 => {
                    // stable/table not exists
                    Err(FlatWriteError::TableNotExits(
                        records.stable.as_deref().unwrap_or("unknown").to_string(),
                    ))
                }
                0x2653 => {
                    // Value too long for column/tag
                    let message = err.message();
                    if let Some(caps) = RE_0X2653.captures(&message) {
                        let field = caps.get(1).unwrap().as_str();
                        return Err(FlatWriteError::ContainerLengthTooShort(field.to_string()));
                    }
                    Err(err).map_err(Into::into)
                }
                0x267B => {
                    // TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL
                    // SQL internal error, ignore for now
                    Ok(0)
                }
                _ => Err(err)
                    .context("flat message write sql error")
                    .map_err(Into::into),
            }
        }
    }
}

#[instrument(skip_all)]
#[async_backtrace::framed]
pub async fn flat_write_with_sql(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    target_precision: taos::Precision,
    messages: &[MessageArrowRecords],
    metrics: &IpcMetrics,
    _notifier: Option<&crate::TaskNotifySender>,
    cancel: &CancellationToken,
) -> anyhow::Result<usize> {
    let mut count = 0;
    // Split messages into different stales.
    let cols = messages[0].records.num_columns();
    // group by stable name
    let groups = messages
        .into_iter()
        .into_group_map_by(|m| m.stable_name().map(|s| s.to_string()));
    // insert into stable
    for (stable, messages) in groups.into_iter() {
        let instant = std::time::Instant::now();
        let sqls = message_to_sql(messages.iter().map(|v| *v), target_precision, true, true);
        tracing::debug!(
            stable = stable.as_deref(),
            sqls = sqls.len(),
            cost = ?instant.elapsed(),
            "message to sql cost: {:#?}",
            instant.elapsed()
        );
        let mut qid = Span.get_qid().unwrap_or_else(Qid::init);
        // debug_assert!(qid.task_id() > 0);
        // debug_assert!(qid.batch_id() > 0);
        for records in sqls {
            loop {
                qid.add_sub_batch_id();
                match write_stable_with_sql(pool, taos, qid.get(), &records, cancel).await {
                    Ok(n) => {
                        tracing::debug!(stable, rows = n, "write stable success");

                        count += n;
                        metrics.add_inserted_sqls(1 as u64);
                        metrics.add_written_rows(n as u64);
                        metrics.add_written_points((n * cols) as u64);
                        break;
                    }
                    Err(err) => {
                        metrics.add_failed_sqls(1 as u64);
                        metrics.add_failed_rows(records.records() as u64);
                        metrics.add_failed_points((records.records() * cols) as u64);
                        error!(stable, "write stable with sql error: {err:#}");
                        match err {
                            FlatWriteError::TableNotExits(_) => {
                                if let Some(stable_sql) = messages[0].stable_sql() {
                                    qid.add_sub_batch_id();
                                    tracing::info!(
                                        sql = stable_sql,
                                        stable = stable.as_deref(),
                                        "stable not exists, create stable with sql: {stable_sql}"
                                    );
                                    assert_create_stable(
                                        pool,
                                        taos,
                                        &stable_sql,
                                        qid.get(),
                                        cancel,
                                    )
                                    .await?;
                                }

                                for m in &messages {
                                    let sql = m.table_sql();
                                    qid.add_sub_batch_id();
                                    assert_create_stable(pool, taos, &sql, qid.get(), cancel)
                                        .await?;
                                }
                            }
                            FlatWriteError::ContainerLengthTooShort(field) => {
                                if let Some(stable) = stable.as_deref() {
                                    let desc = taos.as_ref().unwrap().describe(stable).await?;
                                    let f = desc.iter().find(|f| f.field() == field).ok_or_else(
                                        || {
                                            anyhow::anyhow!(
                                                "field `{}` not found in table `{}`",
                                                field,
                                                stable
                                            )
                                        },
                                    )?;
                                    let length = messages
                                        .iter()
                                        .flat_map(|m| m.max_var_length(f.field()))
                                        .max();
                                    if f.is_tag() {
                                        let sql = format!(
                                            "alter table `{}` modify tag `{}` {}({})",
                                            stable,
                                            f.field(),
                                            f.ty(),
                                            length.unwrap_or_else(|| {
                                                let max = if f.ty() == Ty::VarChar {
                                                    16382
                                                } else {
                                                    4093
                                                };
                                                (f.length() * 2).min(max)
                                            })
                                        );
                                        let _ = taos.as_ref().unwrap().exec(&sql).await;
                                    } else {
                                        let sql = format!(
                                            "alter table `{}` modify column `{}` {}({})",
                                            stable,
                                            f.field(),
                                            f.ty(),
                                            length.unwrap_or_else(|| {
                                                let max = if f.ty() == Ty::VarChar {
                                                    65517
                                                } else {
                                                    16382
                                                };
                                                (f.length() * 2).min(max)
                                            })
                                        );
                                        let _ = taos.as_ref().unwrap().exec(&sql).await;
                                    }
                                }
                            }
                            _ => {
                                return Err(err)?;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(count)
}

pub async fn flat_write_with_raw_block(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    max_lengths: &mut HashMap<String, usize>,
    parser: &Parser,
    target_precision: taos::Precision,
    messages: &[MessageArrowRecords],
    metrics: &IpcMetrics,
    notifier: Option<&crate::TaskNotifySender>,
    cancel: &CancellationToken,
) -> anyhow::Result<usize> {
    let mut count = 0;
    let mut qid = Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    for records in messages {
        if records.records.num_rows() == 0 {
            continue;
        }
        metrics.add_processed_rows(records.records.num_rows() as u64);
        if records.records.column(0).null_count() > 0 {
            bail!("Timestamp field contains null or invalid values");
        }
        tracing::debug!("Write records with rows {}", records.records.num_rows());
        let views = taosx_ipc::stream::reader::record_batch_to_column_view(
            &records.records,
            target_precision,
        );
        // dbg!(&views);
        let schema = records.records.schema();
        let columns = schema.fields().iter().map(|f| f.name()).collect_vec();

        // replace dot in table_name
        let table_name = records
            .opts
            .canonical_table_name(records.table.name.as_str());

        let mut raw = RawBlock::from_views(&views, target_precision);
        raw.with_field_names(&columns)
            .with_table_name(table_name.clone());

        let mut write_retries = 0;
        loop {
            let var_views = views
                .iter()
                .zip(&columns)
                .filter(|(v, _)| v.as_ty().is_var_type())
                .map(|(view, name)| (name, view.as_ty(), view.max_variable_length()))
                .collect_vec();
            if var_views.len() > 0 {
                for (name, ty, length) in var_views {
                    if let Some(max) = max_lengths.get(*name) {
                        if *max >= length {
                            continue;
                        }
                    }
                    loop {
                        let res = describe_table_with_connection_retries(
                            pool,
                            taos,
                            &table_name,
                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                            cancel,
                        )
                        .in_current_span()
                        .await;
                        match res {
                            Ok(desc) => {
                                if let Some(col) = desc
                                    .iter()
                                    .find(|f| f.ty().is_var_type() && f.field() == name.as_str())
                                {
                                    // debug_assert!(ty == col.ty());
                                    if col.length() < length {
                                        let table =
                                            records.table.using.as_deref().unwrap_or(&table_name);
                                        let sql = format!(
                                            "alter table `{table}` modify column `{}` {}({})",
                                            name, ty, length
                                        );
                                        qid.add_sub_batch_id();
                                        let result = taos
                                            .as_ref()
                                            .unwrap()
                                            .exec_with_req_id(&sql, qid.get())
                                            .in_current_span()
                                            .await;
                                        match result {
                                            Ok(_) => {
                                                tracing::trace!("exec sql successfully");
                                                max_lengths.insert(name.to_string(), length);
                                            }
                                            Err(err) => {
                                                tracing::warn!("alter column failed: {err:#}");
                                            }
                                        }
                                        continue;
                                    }
                                }
                                break;
                            }
                            Err(err) => {
                                let code: i32 = err.code().into();
                                if !matches!(code, 0x0218 | 0x2603 | 0x0618 | 0x0362) {
                                    Err(err).with_context(|| {
                                        format!("Get table schema error for `{table_name}`")
                                    })?;
                                }
                                // Table not exists.
                                if let Some(sql) = records.stable_sql() {
                                    qid.add_sub_batch_id();
                                    tracing::debug!("flat message stable sql : {sql}");
                                    match taos
                                        .as_ref()
                                        .unwrap()
                                        .exec_with_req_id(&sql, qid.get())
                                        .in_current_span()
                                        .await
                                    {
                                        Ok(_) => {
                                            tracing::trace!("exec sql successfully");
                                            metrics.add_created_stables(1);
                                        }
                                        Err(err) => {
                                            let code: i32 = err.code().into();
                                            // STable already exists
                                            if code != 0x0360 {
                                                Err(err)?;
                                            }
                                        }
                                    }
                                    let sql = records.table_sql();

                                    loop {
                                        qid.add_sub_batch_id();
                                        match exec_sql_with_connection_retries(
                                            pool,
                                            taos,
                                            &sql,
                                            qid.get(),
                                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                            cancel,
                                        )
                                        .await
                                        {
                                            Ok(_n) => {
                                                metrics.add_created_tables(1);
                                            }
                                            Err(err) => {
                                                let code: i32 = err.code().into();

                                                if code == 0x2605 {
                                                    let table =
                                                        records.table.using.as_deref().unwrap();
                                                    let desc =
                                                        describe_table_with_connection_retries(
                                                            pool,
                                                            taos,
                                                            table,
                                                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                                            cancel,
                                                        )
                                                        .in_current_span()
                                                        .await?;
                                                    for f in desc.iter().filter(|f| {
                                                        f.is_tag() && f.ty().is_var_type()
                                                    }) {
                                                        let sql = format!(
                                                                        "alter table `{table}` modify tag `{}` {}({})",
                                                                        f.field(),
                                                                        f.ty(),
                                                                        f.length() * 2
                                                                    );

                                                        qid.add_sub_batch_id();
                                                        let _ = exec_sql_with_connection_retries(
                                                            pool,
                                                            taos,
                                                            &sql,
                                                            qid.get(),
                                                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                                            cancel,
                                                        )
                                                        .await;
                                                        continue;
                                                    }
                                                } else if code == 0x260D {
                                                    // Tags number not matched
                                                    // add Tag
                                                    let table =
                                                        records.table.using.as_deref().unwrap();
                                                    let tags = records.tag_meta().unwrap();
                                                    for tag_meta in tags {
                                                        let mut need_add = true;
                                                        let res =
                                                            describe_table_with_connection_retries(
                                                                pool,
                                                                taos,
                                                                table,
                                                                DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                                                cancel,
                                                            )
                                                            .in_current_span()
                                                            .await?;
                                                        res.into_iter().for_each(|tag_added| {
                                                            if tag_added.is_tag()
                                                                && tag_added.field()
                                                                    == tag_meta.field()
                                                            {
                                                                need_add = false;
                                                            }
                                                        });
                                                        if need_add {
                                                            qid.add_sub_batch_id();
                                                            let add_tag_sql = format!(
                                                                            "alter table `{table}` add tag `{}` {}",
                                                                            tag_meta.field(),
                                                                            parser.get_ipcdatatype_from_parser(tag_meta.field()).unwrap().sql_repr()
                                                                        );
                                                            tracing::info!("table {table} add tag sql: {add_tag_sql}");
                                                            exec_sql_with_connection_retries(
                                                                pool,
                                                                taos,
                                                                &add_tag_sql,
                                                                qid.get(),
                                                                DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                                                cancel,
                                                            )
                                                            .await?;
                                                        }
                                                    }
                                                } else {
                                                    Err(err)?;
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    //.inspect_err(|err| tracing::warn!("{}", err))?
                                } else {
                                    qid.add_sub_batch_id();
                                    let sql = records.table_sql();
                                    exec_sql_with_connection_retries(
                                        pool,
                                        taos,
                                        &sql,
                                        qid.get(),
                                        DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                        cancel,
                                    )
                                    .await
                                    .inspect(|_| metrics.add_created_tables(1))?;
                                }
                            }
                        }
                    }
                }
            }
            qid.add_sub_batch_id();
            trace!("write raw block");
            if let Err(err) = taos
                .as_ref()
                .unwrap()
                .write_raw_block_with_req_id(&raw, qid.get())
                .await
            {
                let code = err.code();
                let errno: i32 = code.into();
                write_retries += 1;
                if write_retries > DEFAULT_MAX_RETRIES_FOR_CONNECTION {
                    tracing::warn!(
                        "flat message write raw block encounter unrecoverable err: {err:#}"
                    );
                    metrics.add_failed_raw_blocks(1);
                    metrics.add_failed_rows(raw.nrows() as u64);
                    metrics.add_failed_points((raw.nrows() * raw.column_views().len()) as u64);
                    Err(err)?;
                    break;
                }
                if errno == 0x2603 || errno == 0x0618 {
                    if let Some(sql) = records.stable_sql() {
                        // dbg!(&sql);

                        qid.add_sub_batch_id();
                        match exec_sql_with_connection_retries(
                            pool,
                            taos,
                            &sql,
                            qid.get(),
                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                            cancel,
                        )
                        .await
                        {
                            Ok(_n) => {
                                metrics.add_created_stables(1);
                            }
                            Err(err) => {
                                let code: i32 = err.code().into();
                                match code {
                                    0x032C => {
                                        // Object is creating
                                        tracing::warn!("error code [0x032C] encountered, ignore");
                                        continue;
                                    }
                                    0x0360 | 0x0115 | 0x0603 | 0x03C7 | 0x03D3 => {
                                        // Table already exists, do nothing
                                        tracing::debug!("error encountered, ignore(table already exists): {err:#}",);
                                    }
                                    _ => {
                                        tracing::error!(sql, "create stable error: {err:#}");
                                        Err(err).context("create stable error")?;
                                    }
                                }
                            }
                        }

                        let sql = records.table_sql();

                        qid.add_sub_batch_id();
                        match exec_sql_with_connection_retries(
                            pool,
                            taos,
                            &sql,
                            qid.get(),
                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                            cancel,
                        )
                        .await
                        {
                            Ok(_n) => {
                                metrics.add_created_tables(1);
                            }
                            Err(err) => {
                                let code: i32 = err.code().into();
                                if code == 0x2605 {
                                    let table = records.table.using.as_deref().unwrap();
                                    let desc = describe_table_with_connection_retries(
                                        pool,
                                        taos,
                                        table,
                                        DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                        cancel,
                                    )
                                    .in_current_span()
                                    .await?;
                                    for f in
                                        desc.iter().filter(|f| f.is_tag() && f.ty().is_var_type())
                                    {
                                        let sql = format!(
                                            "alter table `{table}` modify tag `{}` {}({})",
                                            f.field(),
                                            f.ty(),
                                            f.length() * 2
                                        );
                                        qid.add_sub_batch_id();
                                        let _ = exec_sql_with_connection_retries(
                                            pool,
                                            taos,
                                            &sql,
                                            qid.get(),
                                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                            cancel,
                                        )
                                        .await;
                                    }
                                } else {
                                    Err(err)?;
                                }
                            }
                        }
                        //.inspect_err(|err| tracing::warn!("{}", err))?
                    } else {
                        qid.add_sub_batch_id();
                        let sql = records.table_sql();
                        exec_sql_with_connection_retries(
                            pool,
                            taos,
                            &sql,
                            qid.get(),
                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                            cancel,
                        )
                        .await
                        .inspect(|_| metrics.add_created_tables(1))?;
                    }

                    continue;
                } else if errno == 0x2605 {
                    // container length is too short.
                    let desc = describe_table_with_connection_retries(
                        pool,
                        taos,
                        &table_name,
                        DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                        cancel,
                    )
                    .in_current_span()
                    .await?;
                    let table = records.table.using.as_deref().unwrap_or(&table_name);
                    for f in desc.iter().filter(|f| !f.is_tag() && f.ty().is_var_type()) {
                        let sql = format!(
                            "alter table `{table}` modify column `{}` {}({})",
                            f.field(),
                            f.ty(),
                            f.length() * 2
                        );
                        qid.add_sub_batch_id();
                        let _ = exec_sql_with_connection_retries(
                            pool,
                            taos,
                            &sql,
                            qid.get(),
                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                            cancel,
                        )
                        .await;
                    }
                } else if errno == 0x0118 {
                    // Code([0x0118] Unknown or common error)
                    // column or tag not exists
                    let mut index = 0;
                    while index < columns.len() {
                        // let column_view = views.get(index).unwrap();
                        let column_name = columns.get(index).unwrap().as_str();
                        let desc = describe_table_with_connection_retries(
                            pool,
                            taos,
                            &table_name,
                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                            cancel,
                        )
                        .in_current_span()
                        .await?;
                        let mut need_add = true;
                        desc.into_iter().for_each(|column_meta| {
                            if column_meta.field() == column_name {
                                need_add = false;
                            }
                        });
                        if need_add {
                            let ipc_data_type = parser.get_ipcdatatype_from_parser(column_name);
                            if ipc_data_type.is_none() {
                                anyhow::bail!("column name {column_name} not config in parser");
                            }
                            let sql = format!(
                                "alter table `{}` add column `{}` {}",
                                records
                                    .table
                                    .using
                                    .as_ref()
                                    .unwrap_or(&table_name.to_string()),
                                &column_name,
                                ipc_data_type.unwrap(),
                            );
                            qid.add_sub_batch_id();
                            tracing::info!("alter table column sql: {}", sql);
                            exec_sql_with_connection_retries(
                                pool,
                                taos,
                                &sql,
                                qid.get(),
                                DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                cancel,
                            )
                            .await?;
                        }
                        index += 1;
                    }
                } else if errno == 0xE001 || errno == 0xE002 || errno == 0xE003 || errno == 0x000B {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if cancel.is_cancelled() {
                        return Err(err)?;
                    }
                    taos.replace(pool.get().await?);
                    continue;
                } else if errno == 0x2653 {
                    // retry with sql
                    let records_copy = MessageArrowRecords {
                        table: records.table.clone(),
                        records: records.records.clone(),
                        opts: records.opts.clone(),
                    };
                    let retry_messages = vec![records_copy];
                    count += flat_write_with_sql(
                        &pool,
                        taos,
                        target_precision,
                        &retry_messages,
                        metrics,
                        notifier,
                        cancel,
                    )
                    .await?;
                    break;
                } else if errno == 0x267B {
                    // TSDB_CODE_PAR_PRIMARY_KEY_IS_NULL
                    // SQL internal error, ignore for now
                    tracing::warn!("write raw block sql error: {err:#}",);
                    break;
                } else {
                    error!(table = table_name.as_ref(), code = %code, "write {} records failed: {err:?}", records.records.num_rows());
                    metrics.add_failed_raw_blocks(1);
                    metrics.add_failed_rows(raw.nrows() as u64);
                    metrics.add_failed_points((raw.nrows() * raw.column_views().len()) as u64);
                    Err(err)?;
                    break;
                }
                continue;
            } else {
                count += raw.nrows();
                metrics.add_written_raw_blocks(1);
                metrics.add_written_rows(raw.nrows() as u64);
                metrics.add_written_points((raw.nrows() * raw.column_views().len()) as u64);
                break;
            }
        }
    }
    Ok(count)
}

pub type FlatItem = (
    Vec<MessageArrowRecords>,
    Qid,
    tokio::sync::oneshot::Sender<usize>,
);
pub struct FlatSink {
    pool: TaosPool,
    taos: Option<TaosConnection>,
    parser: Arc<Parser>,
    target_precision: taos::Precision,
    db: String,
    senders: Vec<flume::Sender<FlatItem>>,
    set: Option<JoinSet<anyhow::Result<()>>>,
}

impl FlatSink {
    pub async fn new(
        pool: TaosPool,
        parser: Parser,
        target_precision: taos::Precision,
        metrics_arc: Arc<CoreMetrics>,
        notifier: crate::TaskNotifySender,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        let workers = parser.global().workers_per_vgroup();
        let taos = pool.get().await?;
        let db: String = taos.query_one("select database()").await?.unwrap();
        let vgroups: usize = taos
            .query_one(format!(
                "select `vgroups` from information_schema.ins_databases where name = '{db}'"
            ))
            .await
            .ok()
            .and_then(|s| s)
            .unwrap_or(2);
        let taos = Some(taos);
        let parser = Arc::new(parser);
        let mut set = JoinSet::new();
        let mut senders = Vec::new();
        for vgid in 0..vgroups {
            let (tx, rx) = flume::bounded(workers * 64);
            senders.push(tx);
            for wid in 0..workers.max(1) {
                let pool = pool.clone();
                let metrics_arc = metrics_arc.clone();
                let parser = parser.clone();
                let rx = rx.clone();
                let notifier = notifier.clone();
                let cancel = cancel.clone();
                set.spawn(
                    async move {
                        let metrics = metrics_arc.ipc();
                        let mut taos = Some(pool.get().await?);
                        let mut max_lengths = HashMap::new();
                        let mut total = 0;
                        loop {
                            let (mut messages, qid, sender): FlatItem = rx.recv_async().await?;
                            taoslog::utils::Span.set_qid(&qid);
                            if messages.len() == 0 {
                                continue;
                            }
                            let num_of_rows = messages
                                .iter()
                                .map(|message| message.records.num_rows())
                                .sum::<usize>();
                            let factor = num_of_rows / messages.len();

                            let res = if factor < 200 {
                                flat_write_with_sql(
                                    &pool,
                                    &mut taos,
                                    target_precision,
                                    &messages,
                                    metrics,
                                    Some(&notifier),
                                    &cancel,
                                )
                                .in_current_span()
                                .await
                            } else {
                                flat_write_with_raw_block(
                                    &pool,
                                    &mut taos,
                                    &mut max_lengths,
                                    &parser,
                                    target_precision,
                                    &messages,
                                    metrics,
                                    Some(&notifier),
                                    &cancel,
                                )
                                .in_current_span()
                                .await
                            };
                            match res {
                                Ok(written) => {
                                    total += written;
                                    metrics.add_processed_rows(num_of_rows as u64);

                                    tracing::debug!(
                                        count = total,
                                        written,
                                        "flat write in sink worker"
                                    );
                                    if let Err(_) = sender.send(written) {
                                        // tracing::warn!("send written failed");
                                    }
                                }
                                Err(err) => {
                                    let errstr = format!("{:#}", err);
                                    if errstr.contains("Timestamp data out of range") {
                                        tracing::warn!(
                                            "Contains invalid timestamp, filter out them"
                                        );
                                        // filter timestamp.
                                        let min = get_minimum_timestamp(
                                            &pool,
                                            &mut taos,
                                            DEFAULT_MAX_RETRIES_FOR_CONNECTION,
                                            &cancel,
                                        )
                                        .in_current_span()
                                        .await?;
                                        tracing::debug!("Minimus timestamp: {}", min.to_rfc3339());
                                        let rows: usize =
                                            messages.iter().map(|m| m.records.num_rows()).sum();
                                        messages = messages
                                            .into_iter()
                                            .flat_map(|item| item.filter_by_primary_timestamp(&min))
                                            .collect();

                                        let rows_after: usize =
                                            messages.iter().map(|m| m.records.num_rows()).sum();

                                        let filtered = rows - rows_after;
                                        tracing::info!(
                                            rows,
                                            filtered,
                                            after = rows_after,
                                            "Filter out records"
                                        );
                                        metrics.add_drained_rows(filtered as _);
                                        if messages.len() == 0 {
                                            continue;
                                        }
                                        let factor = messages
                                            .iter()
                                            .map(|message| message.records.num_rows())
                                            .sum::<usize>()
                                            / messages.len();
                                        let written = if factor < 200 {
                                            flat_write_with_sql(
                                                &pool,
                                                &mut taos,
                                                target_precision,
                                                &messages,
                                                metrics,
                                                Some(&notifier),
                                                &cancel,
                                            )
                                            .in_current_span()
                                            .await
                                        } else {
                                            flat_write_with_raw_block(
                                                &pool,
                                                &mut taos,
                                                &mut max_lengths,
                                                &parser,
                                                target_precision,
                                                &messages,
                                                metrics,
                                                Some(&notifier),
                                                &cancel,
                                            )
                                            .in_current_span()
                                            .await
                                        }?;
                                        total += written;
                                        metrics.add_processed_rows(num_of_rows as u64);

                                        tracing::debug!(
                                            count = total,
                                            written,
                                            "flat write in sink worker"
                                        );
                                        if let Err(_) = sender.send(written) {
                                            // tracing::warn!("send written failed");
                                        }
                                    } else {
                                        return Err(err);
                                    }
                                }
                            }
                        }
                    }
                    .instrument(tracing::info_span!(
                        "flat_sink_worker",
                        wid,
                        vgid
                    )),
                );
            }
        }
        Ok(Self {
            pool,
            taos,
            parser,
            target_precision,
            senders,
            db,
            set: Some(set),
        })
    }

    pub async fn wait(self) {
        let mut set = self.set.unwrap();
        while let Some(res) = set.join_next().await {
            if let Err(err) = res.unwrap() {
                error!("Flat sink worker error: {err:#}");
            }
        }
    }
    pub async fn cloned(&self) -> anyhow::Result<Self> {
        let taos = self.pool.get().await?;
        let taos = Some(taos);
        let pool = self.pool.clone();
        let parser = self.parser.clone();
        let target_precision = self.target_precision;
        // let workers = self.senders.len();
        Ok(Self {
            pool,
            taos,
            parser,
            target_precision,
            senders: self.senders.clone(),
            db: self.db.clone(),
            set: None,
        })
    }

    pub async fn write(&self, messages: Vec<MessageArrowRecords>) -> anyhow::Result<usize> {
        let qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
        let mut count = 0;
        let tables = messages.iter().map(|m| m.table.name.as_str()).collect_vec();
        if let Some(ids) = self
            .taos
            .as_ref()
            .unwrap()
            .tables_vgroup_ids(&self.db, &tables)
            .await
        {
            let groups = ids.into_iter().zip(messages.into_iter()).into_group_map();

            for (id, messages) in groups {
                let (tx, _) = tokio::sync::oneshot::channel();
                tracing::debug!(
                    vgid = id,
                    wid = id as usize % self.senders.len(),
                    table = messages[0].table.name.as_str(),
                    "send messages to vgroup"
                );
                self.senders[id as usize % self.senders.len()]
                    .send_async((messages, qid.clone(), tx))
                    .await?;
                // let written = rx.await?;
                // count += written;
            }
        } else {
            tracing::warn!("Can not fetch tables vgroup id");
            let id: usize = rand::random();
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.senders[id as usize % self.senders.len()]
                .send_async((messages, qid, tx))
                .await?;
            let written = rx.await?;
            count += written;
        }
        Ok(count)
    }
}

/// Write flat message to TDengine.
///
/// # Arguments
///
/// - `count` will be increased by the number of rows written. Note that the number of rows written may be less than the number of rows in the message.
#[framed]
#[instrument(skip_all, fields(writer.count = count))]
async fn consume_flat_record_with_sink(
    sink: &FlatSink,
    record: &FlatMessage,
    count: &mut usize,
    parser: &Parser,
) -> anyhow::Result<()> {
    for message in record.records() {
        let batch = message.record();
        let batch = parser.parse_message_from_records(batch, true)?;
        match batch {
            crate::plugins::transform::Message::Raw(_) => todo!(),
            crate::plugins::transform::Message::Tables(_) => todo!(),
            crate::plugins::transform::Message::ChildTables(_) => todo!(),
            crate::plugins::transform::Message::Records(messages) => {
                sink.write(messages).await?;
            }
        }
    }
    Ok(())
}

#[framed]
#[instrument(skip_all)]
pub async fn ipc_flat_stream_worker_vgroup(
    pool: &TaosPool,
    stream: impl Stream<Item = Result<Box<dyn IpcMessage>, ArrowError>> + Unpin,
    sink: impl Sink<LushAck, Error = ArrowError> + Send + 'static,
    parser: &Parser,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let flat_sink = FlatSink::new(
        pool.clone(),
        parser.clone(),
        target_precision,
        metrics_arc.clone(),
        notifier.clone(),
        cancel,
    )
    .await?;
    let count = Arc::new(AtomicUsize::new(0));
    let parser = Arc::new(parser.clone());

    let workers = parser.global().concurrent_limit();

    let (msg_tx, msg_rx) = flume::bounded(workers * 4);
    let (ack_tx, ack_rx) = flume::bounded(workers * 4);

    let mut writer_set = tokio::task::JoinSet::new();

    writer_set.spawn(async move {
        tokio::pin!(sink);
        while let Ok(ack) = ack_rx.recv_async().await {
            sink.send(ack).await?;
        }
        anyhow::Ok(())
    });

    let qid = Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    for i in 0..workers {
        let count = count.clone();
        let parser = parser.clone();
        let msg_rx = msg_rx.clone();
        let ack_tx = ack_tx.clone();
        let count = count.clone();
        let notifier = notifier.clone();
        let metrics_arc = metrics_arc.clone();
        let ipc_error_strategy = ipc_error_strategy.clone();
        let batch_counter = batch_counter.clone();
        let flat_sink = flat_sink.cloned().await?;
        let mut qid = qid.clone();
        let batch_counter = batch_counter.clone();
        writer_set.spawn(
            async move {
                let metrics = metrics_arc.ipc();
                let mut worker_written = 0;
                while let Ok(record) = msg_rx.recv_async().await {
                    let batch_number = if let Some(batch_counter) = batch_counter.as_ref() {
                        let batch_number = batch_counter.next().await?;
                        qid.set_batch_id(batch_number);
                        Some(batch_number)
                    } else {
                        None
                    };
                    trace!(batch_number, "Writing batch");
                    let mut written = 0;
                    let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
                        std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
                    })
                    .unwrap();
                    let res =
                        consume_flat_record_with_sink(&flat_sink, &record, &mut written, &parser)
                            .await;
                    worker_written += written;
                    count.fetch_add(written, Ordering::SeqCst);
                    match res {
                        Err(err) => {
                            metrics.add_failed_batches(1);
                            error!(batch_number, "Writing batch error: {err:#}");
                            let ack = LushAck {
                                code: 0,
                                message: Some(err.to_string()),
                                context: Some(
                                    json!({
                                        "stream": "flat",
                                        "written":  written,
                                    })
                                    .to_string(),
                                ),
                            };
                            ack_tx.send_async(ack).await.context("ACK writer error")?;
                            if ipc_error_strategy.will_stop() {
                                Err(err).context("write batch error")?;
                            } else if let Err(_) =
                                notifier.send(crate::TaskNotify::Error(format!("{:#}", err)))
                            {
                                Err(err).context("write batch error")?;
                            }
                        }
                        Ok(_) => {
                            metrics.add_processed_batches(1);
                            trace!(batch_number, written, "Writing batch done");
                            let ack = LushAck {
                                code: 0,
                                message: None,
                                context: Some(
                                    json!({
                                        "stream": "flat",
                                        "written":  written
                                    })
                                    .to_string(),
                                ),
                            };
                            ack_tx.send_async(ack).await.context("ACK writer error")?;
                        }
                    }
                }
                if worker_written > 0 {
                    info!(
                        worker.id = i,
                        worker.written = worker_written,
                        "Flat stream worker {} done",
                        i
                    );
                }
                return anyhow::Ok(());
            }
            .in_current_span(),
        );
    }

    tokio::pin!(stream);
    while let Some(record) = stream.next().await {
        metrics_arc.ipc().add_received_batches(1);
        match record {
            Ok(record) => {
                msg_tx.send_async(record).await?;
            }
            Err(err) => {
                ack_tx
                    .send_async(LushAck {
                        code: 0xFFFF,
                        message: Some(format!("Parse message error: {err:#}")),
                        context: Some(
                            json!({
                                "stream": "flat",
                            })
                            .to_string(),
                        ),
                    })
                    .await?;
            }
        };
    }

    // The workers will exit when all tx are dropped.
    drop(msg_tx);
    drop(ack_tx);

    while let Some(res) = writer_set.join_next().await {
        res.context("JoinSet spawn flat worker error")?
            .context("Flat stream worker error")?;
    }
    flat_sink.wait().await;
    info!(
        "IPC processing done, written totally {} records",
        count.load(Ordering::SeqCst)
    );
    Ok(())
}

#[framed]
#[instrument(skip_all)]
pub async fn ipc_flat_stream_worker_vgroup_sequential(
    pool: &TaosPool,
    stream: impl Stream<Item = Result<Box<dyn IpcMessage>, ArrowError>> + Unpin,
    sink: impl Sink<LushAck, Error = ArrowError> + Send + 'static,
    parser: &Parser,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let flat_sink = FlatSink::new(
        pool.clone(),
        parser.clone(),
        target_precision,
        metrics_arc.clone(),
        notifier.clone(),
        cancel,
    )
    .await?;
    let count = Arc::new(AtomicUsize::new(0));
    // let ipc_ack_writer = Arc::new(Mutex::new(ipc_ack_writer));

    let (ack_tx, ack_rx) = flume::bounded(4);

    let mut writer_set = tokio::task::JoinSet::new();

    writer_set.spawn(async move {
        tokio::pin!(sink);
        while let Ok(ack) = ack_rx.recv_async().await {
            sink.send(ack).await?;
        }
        anyhow::Ok(())
    });
    // let qid = Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);

    let metrics = metrics_arc.ipc();
    tokio::pin!(stream);
    while let Some(record) = stream.next().await {
        metrics.add_received_batches(1);
        match record {
            Ok(record) => {
                // msg_tx.send_async(record).await?;
                let batch_number = if let Some(batch_counter) = batch_counter.clone() {
                    Some(batch_counter.next().await?)
                } else {
                    None
                };
                trace!(batch_number, "Writing batch");
                let mut written = 0;
                let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
                    std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
                })
                .unwrap();
                let res =
                    consume_flat_record_with_sink(&flat_sink, &record, &mut written, &parser).await;
                count.fetch_add(written, Ordering::SeqCst);
                match res {
                    Err(err) => {
                        metrics.add_failed_batches(1);
                        error!("Writing batch error: {err:#}");
                        let ack = LushAck {
                            code: 0,
                            message: Some(err.to_string()),
                            context: Some(
                                json!({
                                    "stream": "flat",
                                    "written":  written,
                                })
                                .to_string(),
                            ),
                        };
                        ack_tx.send_async(ack).await.context("ACK writer error")?;
                        if ipc_error_strategy.will_stop() {
                            Err(err).context("write batch error")?;
                        } else if let Err(_) =
                            notifier.send(crate::TaskNotify::Error(format!("{:#}", err)))
                        {
                            Err(err).context("write batch error")?;
                        }
                    }
                    Ok(_) => {
                        metrics.add_processed_batches(1);
                        trace!(batch_number, written, "Writing batch done");
                        let ack = LushAck {
                            code: 0,
                            message: None,
                            context: Some(
                                json!({
                                    "stream": "flat",
                                    "written":  written
                                })
                                .to_string(),
                            ),
                        };
                        ack_tx.send_async(ack).await.context("ACK writer error")?;
                    }
                }
            }
            Err(err) => {
                ack_tx
                    .send_async(LushAck {
                        code: 0xFFFF,
                        message: Some(format!("Parse message error: {err:#}")),
                        context: Some(
                            json!({
                                "stream": "flat",
                            })
                            .to_string(),
                        ),
                    })
                    .await?;
            }
        };
    }

    // The workers will exit when all tx are dropped.
    drop(ack_tx);

    while let Some(res) = writer_set.join_next().await {
        res.context("JoinSet spawn flat worker error")?
            .context("Flat stream worker error")?;
    }
    flat_sink.wait().await;
    info!(
        "IPC processing done, written totally {} records",
        count.load(Ordering::SeqCst)
    );
    Ok(())
}

#[framed]
#[instrument(skip_all, fields(precision = %target_precision))]
pub async fn ipc_flat_stream_worker_concurrent(
    pool: &TaosPool,
    stream: impl Stream<Item = Result<Box<dyn IpcMessage>, ArrowError>> + Unpin,
    sink: impl Sink<LushAck, Error = ArrowError> + Send + 'static,
    cancel: CancellationToken,
    parser: &Parser,
    target_precision: taos::Precision,
    notifier: crate::TaskNotifySender,
    ipc_error_strategy: IpcErrorStrategy,
    metrics_arc: Arc<CoreMetrics>,
    batch_counter: Option<BatchCounter>,
) -> anyhow::Result<()> {
    tokio::pin!(stream);
    let count = Arc::new(AtomicUsize::new(0));
    let context = WriterContext {
        pool: pool.clone(),
        parser: Arc::new(parser.clone()),
        target_precision,
    };
    // let ipc_ack_writer = Arc::new(Mutex::new(ipc_ack_writer));
    let workers = parser.global().concurrent_limit();

    let (msg_tx, msg_rx) = flume::bounded(workers);
    let (ack_tx, ack_rx) = flume::bounded(workers);

    let (cancel, upstream) = (cancel.child_token(), cancel);
    let mut writer_set = tokio::task::JoinSet::new();

    writer_set.spawn(
        async move {
            tokio::pin!(sink);
            while let Ok(ack) = ack_rx.recv_async().await {
                sink.send(ack).await.inspect_err(|err| {
                    error!("ACK writer error: {err:#}");
                })?;
            }
            anyhow::Ok(())
        }
        .in_current_span(),
    );
    let qid = Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    for i in 0..workers {
        let count = count.clone();
        let context = context.clone();
        let msg_rx = msg_rx.clone();
        let ack_tx = ack_tx.clone();
        let notifier = notifier.clone();
        let metrics_arc = metrics_arc.clone();
        let ipc_error_strategy = ipc_error_strategy.clone();
        let batch_counter = batch_counter.clone();
        let mut qid = qid.clone();
        if cancel.is_cancelled() {
            writer_set.abort_all();
            return Ok(());
        }
        let cancel = cancel.clone();
        writer_set.spawn(
            async move {
                let mut taos = None;
                let metrics = metrics_arc.ipc();
                let mut worker_written = 0;
                while let Ok(record) = msg_rx.recv_async().await {
                    if let Some(batch_counter) = batch_counter.as_ref() {
                        let batch_number = batch_counter.next().await?;
                        qid.set_batch_id(batch_number);
                    }
                    tracing::trace!("Writing batch");
                    let mut written = 0;
                    let record = *Box::<dyn Any>::downcast::<FlatMessage>(unsafe {
                        std::mem::transmute::<Box<dyn IpcMessage>, Box<dyn Any>>(record)
                    })
                    .unwrap();
                    let res = consume_flat_record(
                        &context.pool,
                        &mut taos,
                        &record,
                        &mut written,
                        &cancel,
                        &context.parser,
                        context.target_precision,
                        metrics,
                        Some(&notifier),
                    )
                    .await;
                    worker_written += written;
                    count.fetch_add(written, Ordering::SeqCst);
                    match res {
                        Err(err) => {
                            metrics.add_failed_batches(1);
                            error!("Writing batch error: {err:#}");
                            let ack = LushAck {
                                code: 0xFFFF,
                                message: Some(err.to_string()),
                                context: Some(
                                    json!({
                                        "stream": "flat",
                                        "written":  written,
                                    })
                                    .to_string(),
                                ),
                            };
                            ack_tx
                                .send_async(ack)
                                .await
                                .inspect_err(|error| {
                                    error!(%error, "ACK send error");
                                })
                                .context("ACK writer error")?;
                            if let Err(error) =
                                notifier.send(crate::TaskNotify::Error(format!("{:#}", err)))
                            {
                                tracing::warn!(%error, "Send error notify failed");
                                cancel.cancel();
                                Err(err).context("write batch error")?;
                            } else if ipc_error_strategy.will_stop() {
                                tracing::warn!("Stop writing based on error strategy");
                                cancel.cancel();
                                Err(err).context("write batch error")?;
                            } else {
                                continue;
                            }
                        }
                        Ok(_) => {
                            metrics.add_processed_batches(1);
                            trace!(written, "Writing batch done");
                            let ack = LushAck {
                                code: 0,
                                message: None,
                                context: Some(
                                    json!({
                                        "stream": "flat",
                                        "written":  written
                                    })
                                    .to_string(),
                                ),
                            };
                            ack_tx
                                .send_async(ack)
                                .await
                                .inspect_err(|error| {
                                    error!(%error, "ACK send error");
                                })
                                .context("ACK writer error")?;
                        }
                    }
                }
                if worker_written > 0 {
                    info!(
                        worker.written = worker_written,
                        "Flat stream worker {} done", i
                    );
                }
                return anyhow::Ok(());
            }
            .instrument(tracing::info_span!("flat_stream_worker", worker.id = i,)),
        );
    }

    #[derive(Clone)]
    struct WriterContext {
        pool: TaosPool,
        parser: Arc<Parser>,
        target_precision: taos::Precision,
    }

    let mut batches = 0;

    loop {
        if cancel.is_cancelled() && !upstream.is_cancelled() {
            // Writer is failed
            tracing::warn!("Writer may be failed, try join all workers");
            break;
        }
        if let Some(Err(err)) = writer_set
            .try_join_next()
            .transpose()
            .context("IPC writer error")?
        {
            error!(error = "Writer error: {err:#}");
            Err(err).context("IPC writer fail with error")?;
        }
        if let Some(record) = stream.next().await {
            batches += 1;
            metrics_arc.ipc().add_received_batches(1);
            match record {
                Ok(record) => {
                    msg_tx.send_async(record).await?;
                }
                Err(err) => {
                    tracing::warn!("Consume message error: {err:#}");
                    ack_tx
                        .send_async(LushAck {
                            code: 0xFFFF,
                            message: Some(format!("Parse message error: {err:#}")),
                            context: Some(
                                json!({
                                    "stream": "flat",
                                })
                                .to_string(),
                            ),
                        })
                        .await?;
                }
            };
        } else {
            break;
        }
    }

    if batches == 0 {
        info!("None batches received");
        writer_set.abort_all();
        return Ok(());
    }
    info!("All messages received, totally {} batches", batches);

    // The workers will exit when all tx are dropped.
    drop(msg_tx);
    drop(ack_tx);

    while let Some(res) = writer_set.join_next().await {
        res.context("JoinSet spawn flat worker error")?
            .context("Flat stream worker error")?;
    }
    info!(
        "IPC processing done, written totally {} records",
        count.load(Ordering::SeqCst)
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::*;
    use arrow_schema::{Field, FieldRef, Schema};
    use serde_json::json;
    use taos::AsyncTBuilder;
    use taosx_ipc::prelude::IpcDataType;
    use IpcDataType::*;

    use crate::plugins::transform::MessageTableMeta;

    struct STableMessagesBuilder {
        /// The stable name of the table, if not set, use ordinary table instead.
        stable: String,
        /// The name of the primary key column in the table.
        primary_key: String,
        /// The data types of the columns in the table except for the primary key.
        column_types: Vec<IpcDataType>,
        /// The names of the columns in the table except for the primary key.
        column_names: Vec<String>,
        /// The number of columns in the table except for the primary key.
        column_num: Option<usize>,
        /// The data types of the tags in the table.
        tag_types: Vec<IpcDataType>,
        /// The names of the tags in the table.
        tag_names: Vec<String>,
        /// The number of tags in the table.
        tag_num: Option<usize>,

        /// The prefix of the table name.
        table_prefix: Option<String>,
        /// The suffix of the table name.
        table_suffix: Option<String>,
        /// The number of tables.
        table_num: usize,

        messages_per_table: usize,
        string_repeats: usize,
        /// TODO: The number of records in each message.
        #[allow(dead_code)]
        records_per_message: usize,
    }

    fn sequence_of(
        name: &str,
        ty: &IpcDataType,
        len: usize,
        var_len: usize,
    ) -> (FieldRef, ArrayRef) {
        let field = Arc::new(Field::new(name, ty.arrow_data_type(), true).with_metadata(
            HashMap::from_iter(vec![("type".to_string(), ty.to_string())]),
        ));

        let value = match ty {
            Null => Arc::new(NullArray::new(len)) as ArrayRef,
            Bool => Arc::new(BooleanArray::from_iter(
                (0..len).map(|i| i % 2 == 0).map(Some),
            )),
            UInt8 => Arc::new(UInt8Array::from_iter((0..len).map(|i| i as _).map(Some))),
            UInt16 => Arc::new(UInt16Array::from_iter((0..len).map(|i| i as _).map(Some))),
            UInt32 => Arc::new(UInt32Array::from_iter((0..len).map(|i| i as _).map(Some))),
            UInt64 => Arc::new(UInt64Array::from_iter((0..len).map(|i| i as _).map(Some))),
            Int8 => Arc::new(Int8Array::from_iter((0..len).map(|i| i as _).map(Some))),
            Int16 => Arc::new(Int16Array::from_iter((0..len).map(|i| i as _).map(Some))),
            Int32 => Arc::new(Int32Array::from_iter((0..len).map(|i| i as _).map(Some))),
            Int64 => Arc::new(Int64Array::from_iter((0..len).map(|i| i as _).map(Some))),
            Float32 => Arc::new(Float32Array::from_iter((0..len).map(|i| i as _).map(Some))),
            Float64 => Arc::new(Float64Array::from_iter((0..len).map(|i| i as _).map(Some))),
            Timestamp(unit) => match unit {
                arrow_schema::TimeUnit::Second => unreachable!("Second is not supported"),
                arrow_schema::TimeUnit::Millisecond => {
                    let now = chrono::Utc::now().timestamp_millis();
                    Arc::new(TimestampMillisecondArray::from_iter(
                        (0..len).map(|i| (now + i as i64 * 1000)).map(Some),
                    )) as ArrayRef
                }
                arrow_schema::TimeUnit::Microsecond => {
                    let now = chrono::Utc::now().timestamp_micros();
                    Arc::new(TimestampMicrosecondArray::from_iter(
                        (0..len).map(|i| (now + i as i64 * 1000_000)).map(Some),
                    )) as ArrayRef
                }
                arrow_schema::TimeUnit::Nanosecond => {
                    let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
                    Arc::new(TimestampNanosecondArray::from_iter(
                        (0..len).map(|i| (now + i as i64 * 1000_000_000)).map(Some),
                    )) as ArrayRef
                }
            },
            VarChar(_) => {
                let mut builder = StringBuilder::new();
                for i in 0..len {
                    builder.append_value(format!("varchar_{}", i).repeat(var_len));
                }
                Arc::new(builder.finish())
            }
            NChar(_) => {
                let mut builder = StringBuilder::new();
                for i in 0..len {
                    builder.append_value(format!("nchar_{}", i).repeat(var_len));
                }
                Arc::new(builder.finish())
            }
            Json => {
                let mut builder = StringBuilder::new();
                for i in 0..len {
                    builder.append_value(json!({ "json": i }).to_string());
                }
                Arc::new(builder.finish())
            }
            VarBinary(_) => {
                let mut builder = StringBuilder::new();
                for _ in 0..len {
                    builder.append_value(format!("\\x0102030405060708090a0b0c0d0e0f10"));
                }
                Arc::new(builder.finish())
            }
        };
        (field, value)
    }

    #[allow(dead_code)]
    impl STableMessagesBuilder {
        fn new() -> Self {
            Self {
                stable: String::from("stb1"),
                primary_key: String::from("ts"),

                column_types: vec![
                    Bool,
                    Int8,
                    Int16,
                    Int32,
                    Int64,
                    Float32,
                    Float64,
                    UInt8,
                    UInt16,
                    UInt32,
                    UInt64,
                    VarChar(2),
                    NChar(2),
                ],
                column_names: vec!["v".to_string()],
                column_num: None,

                tag_types: vec![
                    Bool,
                    Int8,
                    Int16,
                    Int32,
                    Int64,
                    Float32,
                    Float64,
                    UInt8,
                    UInt16,
                    UInt32,
                    UInt64,
                    VarChar(2),
                    NChar(2),
                ],
                tag_names: vec!["t".to_string()],
                tag_num: None,

                table_prefix: Some("tb_".to_string()),
                table_suffix: None,
                table_num: 1,

                messages_per_table: 1,
                records_per_message: 1,
                string_repeats: 12,
            }
        }

        fn stable(mut self, stable: &str) -> Self {
            self.stable = stable.to_string();
            self
        }

        fn column_types(mut self, types: Vec<IpcDataType>) -> Self {
            self.column_types = types;
            self
        }

        fn column_names(mut self, names: Vec<&str>) -> Self {
            self.column_names = names.iter().map(|s| s.to_string()).collect();
            self
        }
        fn column_num(mut self, num: usize) -> Self {
            self.table_num = num;
            self
        }

        fn table_prefix(mut self, prefix: &str) -> Self {
            self.table_prefix = Some(prefix.to_string());
            self
        }

        fn table_suffix(mut self, suffix: &str) -> Self {
            self.table_suffix = Some(suffix.to_string());
            self
        }

        fn table_num(mut self, num: usize) -> Self {
            self.table_num = num;
            self
        }

        fn string_repeats(mut self, repeats: usize) -> Self {
            self.string_repeats = repeats;
            self
        }

        fn build(&self) -> Vec<MessageArrowRecords> {
            let stable = self.stable.as_str();
            let table_prefix = self.table_prefix.as_deref().unwrap_or("");
            let table_suffix = self.table_suffix.as_deref().unwrap_or("");
            let table_num = self.table_num;

            let column_num = self.column_num.unwrap_or(self.column_types.len());
            let column_names: Vec<_> = Some(self.primary_key.clone())
                .into_iter()
                .chain(
                    (0..)
                        .zip(std::iter::repeat_with(|| &self.column_names))
                        .flat_map(|(i, names)| {
                            names.into_iter().map(move |name| format!("{}{}", name, i))
                        })
                        .take(column_num),
                )
                .collect();

            let column_types: Vec<_> =
                Some(&IpcDataType::Timestamp(arrow_schema::TimeUnit::Millisecond))
                    .into_iter()
                    .chain(
                        std::iter::repeat_with(|| &self.column_types)
                            .flat_map(|ty| ty.into_iter())
                            .take(column_num),
                    )
                    .collect();

            let tag_num = self.tag_num.unwrap_or(self.tag_types.len());
            let tag_names = (0..)
                .zip(std::iter::repeat(&self.tag_names))
                .flat_map(|(i, names)| names.into_iter().map(move |name| format!("{}{}", name, i)))
                .take(tag_num)
                .collect_vec();
            let tag_types = std::iter::repeat(&self.tag_types)
                .flat_map(|ty| ty.into_iter())
                .take(tag_num)
                .collect_vec();

            let (fields, columns): (Vec<_>, Vec<_>) = tag_names
                .iter()
                .zip(tag_types)
                .map(|(name, ty)| sequence_of(name, ty, table_num, self.string_repeats))
                .unzip();

            let table_tags = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();

            let table_meta = (0..table_num)
                .map(|i| {
                    let table_name = format!("{}{}{}", table_prefix, i, table_suffix);

                    MessageTableMeta {
                        name: Arc::new(table_name),
                        using: Some(stable.to_string()),
                        tags: Some(table_tags.slice(i, 1)),
                    }
                })
                .collect_vec();

            std::iter::repeat(table_meta)
                .zip(0..self.messages_per_table)
                .flat_map(|(meta, mid)| meta.into_iter().map(move |meta| (meta, mid)))
                .map(|(table, _)| MessageArrowRecords {
                    table,
                    records: {
                        let (fields, columns): (Vec<_>, Vec<_>) = column_names
                            .iter()
                            .zip(column_types.iter().cloned())
                            .map(|(name, ty)| sequence_of(name, ty, table_num, self.string_repeats))
                            .unzip();
                        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
                    },
                    opts: Default::default(),
                })
                .collect()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_stable_multiple_tables_small_record_batch() -> anyhow::Result<()> {
        // pretty_env_logger::init();
        let _ = tracing_subscriber::fmt()
            .with_level(true)
            .with_file(true)
            .with_max_level(tracing::Level::DEBUG)
            .pretty()
            .init();
        let builder = STableMessagesBuilder::new()
            .stable("meters")
            .table_num(10)
            .table_prefix("tb_")
            .table_suffix("_suffix")
            .column_names(vec!["v"])
            .table_num(10)
            .string_repeats(12);
        let messages = builder.build();

        let pool = TaosBuilder::from_dsn("taos+ws:///flat")?.pool()?;

        let mut taos = Some(pool.get().await?);

        taos.as_ref()
            .unwrap()
            .exec("drop stable if exists meters")
            .await?;
        let metrics = IpcMetrics::default();
        flat_write_with_sql(
            &pool,
            &mut taos,
            taos::Precision::Millisecond,
            &messages,
            &metrics,
            None,
            &CancellationToken::new(),
        )
        .await?;

        Ok(())
    }

    #[test]
    fn flat_sql_builder() {
        const MAX_SQL_LENGTH: usize = 1_000_000;

        fn valid_sql_or_none(
            slice: &[(
                String, // One table values SQL
                usize,  // One table records
            )],
        ) -> Option<(
            String, // SQL to insert into.
            usize,  // number of tables
            usize,  // number of records
        )> {
            if slice.len() == 1 {
                return Some((format!("INSERT INTO {}", slice[0].0), 1, slice[0].1));
            }
            let len = slice.iter().map(|(sql, _)| sql.len()).sum::<usize>();
            if len < MAX_SQL_LENGTH - 12 {
                let mut sql = String::with_capacity(len + 12);
                sql.push_str("INSERT INTO ");
                let (sql, records) = slice.iter().fold((sql, 0), |(mut sql, records), (s, n)| {
                    sql.push_str(s);
                    (sql, records + n)
                });
                Some((sql, slice.len(), records))
            } else {
                None
            }
        }

        fn values_to_sqls(slice: &[(String, usize)]) -> Vec<(String, usize, usize)> {
            if slice.len() == 0 {
                return vec![];
            }
            if let Some(sql) = valid_sql_or_none(slice) {
                return vec![sql];
            }
            let p = (slice.len() + 1) / 2;
            let (left, right) = slice.split_at(p);
            let mut sqls = values_to_sqls(left);
            sqls.extend(values_to_sqls(right));
            sqls
        }

        let value_items = 1000;
        let mut values = Vec::with_capacity(value_items);
        for i in 0..value_items {
            let s = (0..4096).map(|_| "NULL").join(","); // NULL * 4096
            values.push((s, i));
        }

        let sqls = values_to_sqls(&values);
        assert_eq!(sqls.len(), 32);

        for (sql, _, _) in sqls {
            assert!(sql.len() < MAX_SQL_LENGTH);
        }
    }
}
