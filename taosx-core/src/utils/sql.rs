use arrow::array::ListArray;
use arrow::{array::Array, record_batch::RecordBatch};
use arrow_schema::ArrowError;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::{fmt::Write, io::Write as _, time::Duration};
use taos::{
    taos_query::{common::Describe, Manager},
    AsyncFetchable, AsyncQueryable, AsyncTBuilder, Dsn, Error as TaosError, RawBlock, Taos,
    TaosBuilder, TaosPool,
};
use taos::{Precision, ResultSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::utils;

pub type TaosConnection = deadpool::managed::Object<Manager<TaosBuilder>>;

const SQL_CURRENT_DATABASE: &str = "select database()";
const SQL_SHOW_DATABASES: &str = "show databases";

pub async fn connect_taos(host: &str, ws_enable: bool) -> anyhow::Result<Taos, taos::Error> {
    if ws_enable {
        TaosBuilder::from_dsn(format!("taos+ws://{host}:6041"))?
            .build()
            .await
    } else {
        TaosBuilder::from_dsn(format!("taos://{host}"))?
            .build()
            .await
    }
}

pub async fn get_v2_precision(taos: &taos::Taos) -> Result<taos::Precision, TaosError> {
    let database = taos
        .query_one::<_, String>(SQL_CURRENT_DATABASE)
        .in_current_span()
        .await?
        .ok_or_else(|| TaosError::new(0xFFFF, "Database is not specified"))?;

    #[derive(Deserialize)]
    struct Database {
        name: String,
        precision: taos::Precision,
    }

    let mut databases = taos.query(SQL_SHOW_DATABASES).in_current_span().await?;

    use futures::stream::TryStreamExt;
    databases
        .deserialize::<Database>()
        .try_filter_map(|db| {
            std::future::ready(Ok(if db.name == database {
                Some(db.precision)
            } else {
                None
            }))
        })
        .try_next()
        .await?
        .ok_or_else(|| TaosError::new(0xFFFF, "Can't get precision"))
}

pub async fn get_current_precision(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    max_retries: u32,
    cancel: &CancellationToken,
) -> Result<taos::Precision, TaosError> {
    const SQL_PRECISION: &str =
        "select `precision` from information_schema.ins_databases where name = database()";
    if taos.is_none() {
        taos.replace(
            reconnect_with_max_retries(pool, max_retries, cancel)
                .in_current_span()
                .await?,
        );
    }

    let mut reconnected = false;
    loop {
        match taos
            .as_ref()
            .unwrap()
            .query_one::<_, taos::Precision>(SQL_PRECISION)
            .in_current_span()
            .await
        {
            Ok(n) => {
                return n.ok_or_else(|| TaosError::new(0xFFFF, "Can't get precision"));
            }
            Err(err) => {
                if max_retries == 0 {
                    return Err(err.context("Can't get precision"));
                }
                let code = err.code();
                let errno: i32 = code.into();
                match errno {
                    0xE001 | 0xE002 | 0xE003 | 0xE004 | 0x000B => {
                        // 0xE001: internal error
                        // 0xE002: connection closed
                        // 0xE003: send timeout
                        // 0xE004: receive timeout
                        // 0x000B: unable to establish connection
                        if reconnected {
                            return Err(err.context("Can't get precision"));
                        }
                        taos.replace(
                            reconnect_with_max_retries(pool, max_retries, cancel)
                                .in_current_span()
                                .await?,
                        );
                        reconnected = true;
                        continue;
                    }
                    _ => return Err(err.context("Can't get precision")),
                }
            }
        }
    }
}

#[tokio::test]
async fn test_precision_with_taos() {
    use taos::AsyncTBuilder;
    let dsn = "taos://";
    let pool = taos::TaosBuilder::from_dsn(dsn).unwrap().pool().unwrap();
    let taos = pool.get().await.unwrap();
    taos.exec_many([
        "drop database if exists test_precision",
        "create database if not exists test_precision precision 'ns'",
        "use test_precision",
        "create table if not exists test (ts timestamp, v int)",
        "insert into test values (now(), 1)",
    ])
    .await
    .unwrap();
    let mut taos = Some(taos);

    let t = get_current_precision(&pool, &mut taos, 0, &CancellationToken::new())
        .await
        .unwrap();
    assert!(t == taos::Precision::Nanosecond);
    taos.unwrap()
        .exec_many(["drop database if exists test_precision"])
        .await
        .unwrap();
}

// #[tracing::instrument(skip_all)]
// async fn get_maximum_timestamp(
//     _pool: &TaosPool,
//     _taos: &mut Option<TaosConnection>,
//     _max_retries: u32,
//     _cancel: &CancellationToken,
// ) -> Result<DateTime<Utc>, TaosError> {
//     Ok(chrono::Utc::now() + Duration::from_secs(365 * 24 * 3600))
// }

pub async fn get_database(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    max_retries: u32,
    cancel: &CancellationToken,
) -> Result<String, TaosError> {
    const SQL_SELECT_DATABASE: &str = "select database();";
    if taos.is_none() {
        taos.replace(
            reconnect_with_max_retries(pool, max_retries, cancel)
                .in_current_span()
                .await?,
        );
    }

    match taos
        .as_ref()
        .unwrap()
        .query_one::<_, String>(SQL_SELECT_DATABASE)
        .in_current_span()
        .await
    {
        Ok(n) => n.ok_or_else(|| TaosError::new(0xFFFF, "database name empty")),
        Err(err) => Err(err.context("get database error")),
    }
}

type TimestampRangeResult = Result<(Precision, Option<DateTime<Utc>>, DateTime<Utc>), TaosError>;

#[tracing::instrument(skip_all)]
pub async fn get_timestamp_range(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    max_retries: u32,
    cancel: &CancellationToken,
) -> TimestampRangeResult {
    let min = get_minimum_timestamp(pool, taos, max_retries, cancel).await?;
    let max = chrono::Utc::now() + Duration::from_secs(365 * 24 * 3600);
    Ok((min.0, min.1, max))
}

#[tracing::instrument(skip_all)]
pub async fn get_minimum_timestamp(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    max_retries: u32,
    cancel: &CancellationToken,
) -> Result<(Precision, Option<DateTime<Utc>>), TaosError> {
    let retries = max_retries;
    const SQL_KEEP: &str =
        "select `precision`, `keep` from information_schema.ins_databases where name = database()";
    if taos.is_none() {
        taos.replace(
            reconnect_with_max_retries(pool, max_retries, cancel)
                .in_current_span()
                .await?,
        );
    }

    let mut reconnected = false;
    loop {
        match taos
            .as_ref()
            .unwrap()
            .query_one::<_, (Precision, String)>(SQL_KEEP)
            .in_current_span()
            .await
        {
            Ok(n) => {
                return n
                    .map(|(precision, keep)| {
                        keep.split_once(',')
                            .map(|(keep1, _)| (precision, keep1.to_string()))
                            .unwrap_or((precision, keep))
                    })
                    .and_then(|(precision, keep)| {
                        utils::parse_duration(&keep).ok().map(|d| (precision, d))
                    })
                    .map(|(_precision, d)| (_precision, Some(chrono::Utc::now() - d)))
                    .ok_or_else(|| taos::Error::from_string("Empty precision/keep result"));
            }
            Err(err) => {
                if max_retries == 0 {
                    return Err(err.context("Can't get minimum timestamp"));
                }
                let code = err.code();
                let errno: i32 = code.into();
                match errno {
                    0xE001 | 0xE002 | 0xE003 | 0xE004 | 0x000B => {
                        // 0xE001: internal error
                        // 0xE002: connection closed
                        // 0xE003: send timeout
                        // 0xE004: receive timeout
                        // 0x000B: unable to establish connection
                        if reconnected {
                            return Err(err.context("Can't get minimum timestamp"));
                        }
                        taos.replace(
                            reconnect_with_max_retries(pool, max_retries, cancel)
                                .in_current_span()
                                .await?,
                        );
                        reconnected = true;
                        continue;
                    }
                    0x2602 => {
                        // 兼容云服务，表中没有 keep 字段
                        let precision = get_current_precision(pool, taos, retries, cancel).await?;
                        return Ok((precision, None));
                    }
                    _ => return Err(err.context("Can't get minimum timestamp")),
                }
            }
        }
    }
}

#[tokio::test]
async fn test_min_timestamp_with_taos() {
    use taos::AsyncTBuilder;
    let dsn = "taos://";
    let pool = taos::TaosBuilder::from_dsn(dsn).unwrap().pool().unwrap();
    let taos = pool.get().await.unwrap();
    taos.exec_many([
        "drop database if exists test_min_timestamp",
        "create database if not exists test_min_timestamp keep 365d",
        "use test_min_timestamp",
        "create table if not exists test (ts timestamp, v int)",
        "insert into test values (now(), 1)",
    ])
    .await
    .unwrap();
    let mut taos = Some(taos);

    let min = chrono::Utc::now();
    let (precision, t) = get_minimum_timestamp(&pool, &mut taos, 0, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(precision, Precision::Millisecond);
    assert!(t <= Some(min));
    taos.unwrap()
        .exec_many(["drop database if exists test_min_timestamp"])
        .await
        .unwrap();
}

pub async fn reconnect_with_max_retries(
    pool: &TaosPool,
    max_retries: u32,
    cancel: &CancellationToken,
) -> Result<TaosConnection, TaosError> {
    let mut backoff = 1;
    let mut retries = 0;
    loop {
        if cancel.is_cancelled() {
            return Err(TaosError::new(0x000B, "reconnection cancelled".to_string()));
        }
        match pool.get().await {
            Ok(taos) => {
                tracing::debug!(retries, "reconnected");
                break Ok(taos);
            }
            Err(err) => {
                if retries >= max_retries {
                    tracing::warn!(retries, "reconnect failed after retries");
                    break Err(TaosError::new(0x000B, format!("reconnect failed: {}", err)));
                }
                retries += 1;
                tokio::time::sleep(Duration::from_millis(backoff * 100)).await;
                tracing::trace!(retries, "retry reconnecting");
                if backoff < 64 {
                    backoff *= 2;
                }
            }
        }
    }
}

#[tracing::instrument(skip(pool, taos, cancel))]
pub async fn exec_sql_with_connection_retries(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    sql: &str,
    req_id: u64,
    max_retries: u32,
    cancel: &CancellationToken,
) -> Result<usize, TaosError> {
    if taos.is_none() {
        taos.replace(
            reconnect_with_max_retries(pool, max_retries, cancel)
                .in_current_span()
                .await?,
        );
    }

    match taos
        .as_ref()
        .unwrap()
        .exec_with_req_id(sql, req_id)
        .in_current_span()
        .await
    {
        Ok(n) => {
            tracing::trace!("exec sql successfully");
            Ok(n)
        }
        Err(err) => {
            if max_retries == 0 {
                return Err(err.context(format!("exec sql `{}`", sql)));
            }
            let code = err.code();
            let errno: i32 = code.into();
            tracing::debug!(%code, error = format!("{err:#}"), sql, "exec sql error");
            match errno {
                0xE001 | 0xE002 | 0xE003 | 0xE004 | 0x000B => {
                    // 0xE001: internal error
                    // 0xE002: connection closed
                    // 0xE003: send timeout
                    // 0xE004: receive timeout
                    // 0x000B: unable to establish connection
                    taos.replace(
                        reconnect_with_max_retries(pool, max_retries, cancel)
                            .in_current_span()
                            .await?,
                    );
                    taos.as_ref()
                        .unwrap()
                        .exec_with_req_id(sql, req_id)
                        .in_current_span()
                        .await
                }
                0x032C => {
                    // 0x032C: Object is creating
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    taos.as_ref()
                        .unwrap()
                        .exec_with_req_id(sql, req_id)
                        .in_current_span()
                        .await
                }
                _ => Err(err.context(format!("exec sql `{}`", sql))),
            }
        }
    }
}

#[tracing::instrument(skip(pool, taos, block), fields(table = block.table_name(), rows = block.nrows()))]
pub async fn write_raw_block_with_connection_retries(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    block: &RawBlock,
    req_id: u64,
    max_retries: u32,
    cancel: &CancellationToken,
) -> Result<(), TaosError> {
    if taos.is_none() {
        taos.replace(
            reconnect_with_max_retries(pool, max_retries, cancel)
                .in_current_span()
                .await?,
        );
    }

    match taos
        .as_ref()
        .unwrap()
        .write_raw_block_with_req_id(block, req_id)
        .in_current_span()
        .await
    {
        Ok(n) => Ok(n),
        Err(err) => {
            if max_retries == 0 {
                return Err(err.context(format!("Write block: {}", block.pretty_format())));
            }
            let code = err.code();
            let errno: i32 = code.into();
            match errno {
                0xE001 | 0xE002 | 0xE003 | 0xE004 | 0x000B => {
                    // 0xE001: internal error
                    // 0xE002: connection closed
                    // 0xE003: send timeout
                    // 0xE004: receive timeout
                    // 0x000B: unable to establish connection
                    taos.replace(
                        reconnect_with_max_retries(pool, max_retries, cancel)
                            .in_current_span()
                            .await?,
                    );
                    taos.as_ref()
                        .unwrap()
                        .write_raw_block_with_req_id(block, req_id)
                        .in_current_span()
                        .await
                }
                _ => Err(err.context(format!("Write block: {}", block.pretty_format()))),
            }
        }
    }
}

pub async fn query_sql_with_connection_retries(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    sql: &str,
    max_retries: u32,
    cancel: &CancellationToken,
) -> Result<ResultSet, TaosError> {
    if taos.is_none() {
        taos.replace(
            reconnect_with_max_retries(pool, max_retries, cancel)
                .in_current_span()
                .await?,
        );
    }

    match taos.as_ref().unwrap().query(sql).in_current_span().await {
        Ok(res) => Ok(res),
        Err(err) => {
            if max_retries == 0 {
                return Err(err.context(format!("query sql: {}", sql)));
            }
            let code = err.code();
            let errno: i32 = code.into();
            match errno {
                0xE001 | 0xE002 | 0xE003 | 0xE004 | 0x000B => {
                    taos.replace(
                        reconnect_with_max_retries(pool, max_retries, cancel)
                            .in_current_span()
                            .await?,
                    );
                    taos.as_ref().unwrap().query(sql).in_current_span().await
                }
                _ => Err(err.context(format!("query sql: {}", sql))),
            }
        }
    }
}

#[tracing::instrument(skip(pool, taos))]
pub async fn describe_table_with_connection_retries(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    table: &str,
    max_retries: u32,
    cancel: &CancellationToken,
) -> Result<Describe, TaosError> {
    if taos.is_none() {
        taos.replace(
            reconnect_with_max_retries(pool, max_retries, cancel)
                .in_current_span()
                .await?,
        );
    }

    match taos
        .as_ref()
        .unwrap()
        .describe(table)
        .in_current_span()
        .await
    {
        Ok(n) => Ok(n),
        Err(err) => {
            if max_retries == 0 {
                return Err(err.context(format!("describe `{table}`")));
            }
            let code = err.code();
            let errno: i32 = code.into();
            match errno {
                0xE001 | 0xE002 | 0xE003 | 0xE004 | 0x000B => {
                    // 0xE001: internal error
                    // 0xE002: connection closed
                    // 0xE003: send timeout
                    // 0xE004: receive timeout
                    // 0x000B: unable to establish connection
                    taos.replace(
                        reconnect_with_max_retries(pool, max_retries, cancel)
                            .in_current_span()
                            .await?,
                    );
                    taos.as_ref()
                        .unwrap()
                        .describe(table)
                        .in_current_span()
                        .await
                }
                _ => Err(err.context(format!("describe `{table}`"))),
            }
        }
    }
}

pub struct RetriableTaos {
    pool: TaosPool,
    taos: Option<TaosConnection>,
    max_retries: u32,
    cancel: CancellationToken,
}

impl RetriableTaos {
    pub fn new(pool: TaosPool, max_retries: u32, cancel: CancellationToken) -> Self {
        Self {
            pool,
            taos: None,
            max_retries,
            cancel,
        }
    }

    pub async fn exec(&mut self, sql: &str, req_id: u64) -> Result<usize, TaosError> {
        exec_sql_with_connection_retries(
            &self.pool,
            &mut self.taos,
            sql,
            req_id,
            self.max_retries,
            &self.cancel,
        )
        .await
    }

    pub async fn describe(&mut self, table: &str) -> Result<Describe, TaosError> {
        describe_table_with_connection_retries(
            &self.pool,
            &mut self.taos,
            table,
            self.max_retries,
            &self.cancel,
        )
        .await
    }

    pub async fn write_raw_block(
        &mut self,
        block: &RawBlock,
        req_id: u64,
    ) -> Result<(), TaosError> {
        write_raw_block_with_connection_retries(
            &self.pool,
            &mut self.taos,
            block,
            req_id,
            self.max_retries,
            &self.cancel,
        )
        .await
    }
}

/// Escape a string value for SQL.
pub struct SingleQuoteSqlValueEscaped<'a>(&'a str);

impl std::fmt::Display for SingleQuoteSqlValueEscaped<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = self.0;
        f.write_char('\'')?;

        for c in value.chars() {
            match c {
                '\0' => {
                    // taosc uses C escape syntax for SQL which not support null byte escape,
                    // so we need to ignore null byte.
                }
                '\'' => {
                    f.write_char('\'')?;
                    f.write_char('\'')?;
                }

                '\t' => {
                    f.write_char('\\')?;
                    f.write_char('t')?;
                }
                '\r' => {
                    f.write_char('\\')?;
                    f.write_char('r')?;
                }
                '\n' => {
                    f.write_char('\\')?;
                    f.write_char('n')?;
                }
                '\\' | '"' => {
                    f.write_char('\\')?;
                    f.write_char(c)?;
                }
                _ => {
                    f.write_char(c)?;
                }
            }
        }
        f.write_char('\'')
    }
}

pub fn sql_value_escaped_fmt(value: &str) -> SingleQuoteSqlValueEscaped {
    SingleQuoteSqlValueEscaped(value)
}
/// Escape a string value for SQL.
pub fn sql_value_escape(value: &str) -> String {
    SingleQuoteSqlValueEscaped(value).to_string()
}

pub fn sql_max_var_length(batch: &RecordBatch) -> Vec<usize> {
    let mut lengths = vec![0; batch.num_columns()];

    #[allow(clippy::needless_range_loop)]
    for i in 0..batch.num_columns() {
        let array = batch.column(i);

        match array.data_type() {
            arrow_schema::DataType::Binary => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .unwrap();
                if let Some(len) = array.iter().flatten().map(|v| v.len()).max() {
                    lengths[i] = len;
                }
            }
            arrow_schema::DataType::FixedSizeBinary(len) => {
                lengths[i] = *len as _;
            }
            arrow_schema::DataType::LargeBinary => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeBinaryArray>()
                    .unwrap();
                if let Some(len) = array.iter().flatten().map(|v| v.len()).max() {
                    lengths[i] = len;
                }
            }
            arrow_schema::DataType::Utf8 => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                if let Some(len) = array.iter().flatten().map(|v| v.len()).max() {
                    lengths[i] = len;
                }
            }
            arrow_schema::DataType::LargeUtf8 => {
                let array = array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                    .unwrap();
                if let Some(len) = array.iter().flatten().map(|v| v.len()).max() {
                    lengths[i] = len;
                }
            }
            _ => (),
        }
    }
    lengths
}

pub fn sql_values_from_record_batch(
    batch: &RecordBatch,
    precision: taos::Precision,
    with_field_names: bool,
) -> Result<Vec<(String, usize, usize, usize)>, arrow::error::ArrowError> {
    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }

    let mut column_has_value = vec![];
    let schema = batch.schema();
    let names = schema
        .fields()
        .iter()
        .filter(|f| {
            let col_index = schema.index_of(f.name()).unwrap();
            if !with_field_names || batch.column(col_index).null_count() < batch.num_rows() {
                column_has_value.push(col_index);
                true
            } else {
                false
            }
        })
        .map(|f| format!("`{}`", f.name()))
        .join(",");
    let columns = batch.columns();
    let schema = batch.schema_ref();
    let vec = Vec::with_capacity(1);
    let mut rows = 0;
    let mut start = 0;

    let (mut vec, cursor) =
        (0..batch.num_rows()).try_fold((vec, None), |(mut vec, cursor), row| {
            if columns[0].is_null(row) {
                return Ok((vec, cursor));
            }
            let mut cursor = cursor.unwrap_or_else(|| {
                let mut cursor = std::io::Cursor::new(Vec::<u8>::with_capacity(256));
                if with_field_names {
                    let _ = write!(cursor, "({}) values", names).inspect_err(|e| {
                        tracing::error!("Cursor io should never error: {}", e);
                    });
                } else {
                    let _ = cursor.write(b"values").inspect_err(|e| {
                        tracing::error!("Cursor io should never error: {}", e);
                    });
                }
                cursor
            });
            cursor.write_all(b"(")?;
            #[allow(clippy::needless_range_loop)]
            for col in column_has_value.iter() {
                let array = &columns[*col];
                if *col > 0 {
                    cursor.write_all(b",")?;
                }
                if array.is_null(row) {
                    cursor.write_all(b"NULL")?;
                    continue;
                }
                let field = schema.field(*col);
                let cast_to = field.metadata().get("cast_to").map(|s| s.as_str());
                match columns[*col].data_type() {
                    arrow_schema::DataType::Null => {
                        cursor.write_all(b"NULL")?;
                    }
                    arrow_schema::DataType::Boolean => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .unwrap();
                        cursor.write_all(if array.value(row) { b"true" } else { b"false" })?;
                    }
                    arrow_schema::DataType::Int8 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::Int8Array>()
                            .unwrap();
                        write!(cursor, "{}", array.value(row))?;
                    }
                    arrow_schema::DataType::Int16 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::Int16Array>()
                            .unwrap();
                        write!(cursor, "{}", array.value(row))?;
                    }
                    arrow_schema::DataType::Int32 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::Int32Array>()
                            .unwrap();
                        write!(cursor, "{}", array.value(row))?;
                    }
                    arrow_schema::DataType::Int64 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .unwrap();
                        write!(cursor, "{}", array.value(row))?;
                    }
                    arrow_schema::DataType::UInt8 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::UInt8Array>()
                            .unwrap();
                        write!(cursor, "{}", array.value(row))?;
                    }
                    arrow_schema::DataType::UInt16 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::UInt16Array>()
                            .unwrap();
                        write!(cursor, "{}", array.value(row))?;
                    }
                    arrow_schema::DataType::UInt32 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::UInt32Array>()
                            .unwrap();
                        write!(cursor, "{}", array.value(row))?;
                    }
                    arrow_schema::DataType::UInt64 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::UInt64Array>()
                            .unwrap();
                        write!(cursor, "{}", array.value(row))?;
                    }
                    arrow_schema::DataType::Decimal128(_, scale) => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::Decimal128Array>()
                            .unwrap();
                        let v = bigdecimal::BigDecimal::from_bigint(
                            array.value(row).into(),
                            *scale as _,
                        );
                        write!(cursor, "{}", v)?;
                    }
                    arrow_schema::DataType::Float16 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::Float16Array>()
                            .unwrap();
                        let v = array.value(row);
                        if v.is_nan() {
                            cursor.write_all(b"NULL")?;
                        } else {
                            write!(cursor, "{}", v)?;
                        }
                    }
                    arrow_schema::DataType::Float32 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::Float32Array>()
                            .unwrap();
                        let v = array.value(row);
                        if v.is_nan() {
                            cursor.write_all(b"NULL")?;
                        } else {
                            write!(cursor, "{}", v)?;
                        }
                    }
                    arrow_schema::DataType::Float64 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .unwrap();
                        let v = array.value(row);
                        if v.is_nan() {
                            cursor.write_all(b"NULL")?;
                        } else {
                            write!(cursor, "{}", v)?;
                        }
                    }
                    arrow_schema::DataType::Timestamp(unit, _) => match unit {
                        arrow_schema::TimeUnit::Second => {
                            let array = array
                                .as_any()
                                .downcast_ref::<arrow::array::TimestampSecondArray>()
                                .unwrap();
                            match precision {
                                taos::Precision::Millisecond => {
                                    write!(cursor, "{}", array.value(row) * 1000)?;
                                }
                                taos::Precision::Microsecond => {
                                    write!(cursor, "{}", array.value(row) * 1_000_000)?;
                                }
                                taos::Precision::Nanosecond => {
                                    write!(cursor, "{}", array.value(row) * 1_000_000_000)?;
                                }
                            }
                        }
                        arrow_schema::TimeUnit::Millisecond => {
                            let array = array
                                .as_any()
                                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                                .unwrap();

                            match precision {
                                taos::Precision::Millisecond => {
                                    write!(cursor, "{}", array.value(row))?;
                                }
                                taos::Precision::Microsecond => {
                                    write!(cursor, "{}", array.value(row) * 1000)?;
                                }
                                taos::Precision::Nanosecond => {
                                    write!(cursor, "{}", array.value(row) * 1_000_000)?;
                                }
                            }
                        }
                        arrow_schema::TimeUnit::Microsecond => {
                            let array = array
                                .as_any()
                                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                                .unwrap();

                            match precision {
                                taos::Precision::Millisecond => {
                                    write!(cursor, "{}", array.value(row) / 1000)?;
                                }
                                taos::Precision::Microsecond => {
                                    write!(cursor, "{}", array.value(row))?;
                                }
                                taos::Precision::Nanosecond => {
                                    write!(cursor, "{}", array.value(row) * 1000)?;
                                }
                            }
                        }
                        arrow_schema::TimeUnit::Nanosecond => {
                            let array = array
                                .as_any()
                                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                                .unwrap();

                            match precision {
                                taos::Precision::Millisecond => {
                                    write!(cursor, "{}", array.value(row) / 1_000_000)?;
                                }
                                taos::Precision::Microsecond => {
                                    write!(cursor, "{}", array.value(row) / 1000)?;
                                }
                                taos::Precision::Nanosecond => {
                                    write!(cursor, "{}", array.value(row))?;
                                }
                            }
                        }
                    },
                    arrow_schema::DataType::Binary if cast_to.is_some_and(|s| s == "VARBINARY") => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::BinaryArray>()
                            .unwrap();
                        let bytes = array.value(row);
                        write!(cursor, "'\\x{}'", hex::encode(bytes))?;
                    }
                    arrow_schema::DataType::Binary => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::BinaryArray>()
                            .unwrap();
                        write!(
                            cursor,
                            "{}",
                            sql_value_escaped_fmt(&String::from_utf8_lossy(array.value(row),))
                        )?;
                    }
                    arrow_schema::DataType::FixedSizeBinary(_)
                        if cast_to.is_some_and(|s| s == "VARBINARY") =>
                    {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                            .unwrap();
                        let bytes = array.value(row);
                        write!(cursor, "\\x{}", hex::encode(bytes))?;
                    }
                    arrow_schema::DataType::FixedSizeBinary(_) => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                            .unwrap();
                        write!(
                            cursor,
                            "{}",
                            sql_value_escaped_fmt(&String::from_utf8_lossy(array.value(row),))
                        )?;
                    }
                    arrow_schema::DataType::LargeBinary
                        if cast_to.is_some_and(|s| s == "VARBINARY") =>
                    {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::LargeBinaryArray>()
                            .unwrap();
                        let bytes = array.value(row);
                        write!(cursor, "\\x{}", hex::encode(bytes))?;
                    }
                    arrow_schema::DataType::LargeBinary => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::LargeBinaryArray>()
                            .unwrap();
                        write!(
                            cursor,
                            "{}",
                            sql_value_escaped_fmt(&String::from_utf8_lossy(array.value(row),))
                        )?;
                    }
                    arrow_schema::DataType::Utf8 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .unwrap();
                        write!(cursor, "{}", sql_value_escaped_fmt(array.value(row)))?;
                    }
                    arrow_schema::DataType::LargeUtf8 => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::LargeStringArray>()
                            .unwrap();
                        write!(cursor, "{}", sql_value_escaped_fmt(array.value(row)))?;
                    }
                    arrow_schema::DataType::List(field)
                        if cast_to.is_some_and(|s| s == "VARBINARY")
                            && field.data_type().is_numeric() =>
                    {
                        let array = array.as_any().downcast_ref::<ListArray>().unwrap();
                        // ListArray 每一行转换为一个 Bytes 数组
                        let row_data = array.value(row);
                        let u8_array =
                            arrow::compute::cast(&row_data, &arrow_schema::DataType::UInt8)?;
                        let u8_array = u8_array
                            .as_any()
                            .downcast_ref::<arrow::array::UInt8Array>()
                            .unwrap();
                        let bytes = u8_array.values();
                        let s = hex::encode(bytes);

                        write!(cursor, "'\\x{s}'")?;
                    }
                    dt => {
                        return Err(ArrowError::NotYetImplemented(format!(
                            "Convert `{dt:?}` to sql value"
                        )));
                    }
                }
            }
            cursor.write_all(b")")?;
            rows += 1;
            cursor.flush()?;
            if cursor.position() > 900_000 {
                let values = unsafe { String::from_utf8_unchecked(cursor.into_inner()) };
                vec.push((values, rows, start, row));
                rows = 0;
                start = row + 1;
                Ok((vec, None))
            } else {
                Ok((vec, Some(cursor)))
            }
        })?;
    if let Some(cursor) = cursor {
        let values = unsafe { String::from_utf8_unchecked(cursor.into_inner()) };
        vec.push((values, rows, start, batch.num_rows() - 1));
    }

    Ok(vec)
}

/// 对于属于同一个子表的一批 Records，生成一条 SQL 插入语句的 insert into 后面的部分，不使用自动建表语法。
/// 如果某个字段的值为 NULL，则跳过该字段。
/// 如：tablename (col_names) values (col_values) tablename (col_names) values (col_values) ...
pub fn sql_values_from_record_batch_skip_null(
    table_name: &str,
    batch: &RecordBatch,
    target_precision: taos::Precision,
) -> Result<Vec<(String, usize, usize, usize)>, arrow::error::ArrowError> {
    let mut vec = Vec::with_capacity(1);
    let schema = batch.schema();
    let col_names = schema
        .fields()
        .iter()
        .map(|f| format!("`{}`", f.name()))
        .collect::<Vec<_>>();
    let columns = batch.columns();
    let mut sql = String::with_capacity(256);
    let mut rows = 0;
    let mut start = 0;

    for row in 0..batch.num_rows() {
        if columns[0].is_null(row) {
            continue;
        }
        let mut insert_col_names = String::new();
        let mut insert_col_values = String::new();
        for col in 0..batch.num_columns() {
            let array = &columns[col];
            if array.is_null(row) {
                continue;
            }
            let column_data_type = columns[col].data_type();
            if column_data_type == &arrow_schema::DataType::Null {
                continue;
            }
            insert_col_names.push_str(&col_names[col]);
            match column_data_type {
                arrow_schema::DataType::Boolean => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .unwrap();
                    insert_col_values.push_str(if array.value(row) { "true" } else { "false" });
                }
                arrow_schema::DataType::Int8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int8Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int16 => {
                    let array: &arrow::array::PrimitiveArray<arrow::datatypes::Int16Type> = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int16Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Int64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt8Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt16Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt32Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::UInt64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .unwrap();
                    insert_col_values.push_str(&array.value(row).to_string());
                }
                arrow_schema::DataType::Decimal128(_, scale) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Decimal128Array>()
                        .unwrap();
                    let v =
                        bigdecimal::BigDecimal::from_bigint(array.value(row).into(), *scale as _);
                    insert_col_values.push_str(&v.to_string());
                }
                arrow_schema::DataType::Float16 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float16Array>()
                        .unwrap();
                    let v = array.value(row);
                    if v.is_nan() {
                        insert_col_values.push_str("NULL");
                    } else {
                        insert_col_values.push_str(&v.to_string());
                    }
                }
                arrow_schema::DataType::Float32 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .unwrap();
                    let v = array.value(row);
                    if v.is_nan() {
                        insert_col_values.push_str("NULL");
                    } else {
                        insert_col_values.push_str(&v.to_string());
                    }
                }
                arrow_schema::DataType::Float64 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .unwrap();
                    let v = array.value(row);
                    if v.is_nan() {
                        insert_col_values.push_str("NULL");
                    } else {
                        insert_col_values.push_str(&v.to_string());
                    }
                }
                arrow_schema::DataType::Timestamp(unit, _) => match unit {
                    arrow_schema::TimeUnit::Second => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampSecondArray>()
                            .unwrap();
                        match target_precision {
                            taos::Precision::Millisecond => {
                                insert_col_values.push_str(&(array.value(row) * 1000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                insert_col_values
                                    .push_str(&(array.value(row) * 1_000_000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                insert_col_values
                                    .push_str(&(array.value(row) * 1_000_000_000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Millisecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                            .unwrap();

                        match target_precision {
                            taos::Precision::Millisecond => {
                                insert_col_values.push_str(&(array.value(row)).to_string());
                            }
                            taos::Precision::Microsecond => {
                                insert_col_values.push_str(&(array.value(row) * 1000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                insert_col_values
                                    .push_str(&(array.value(row) * 1_000_000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Microsecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                            .unwrap();

                        match target_precision {
                            taos::Precision::Millisecond => {
                                insert_col_values.push_str(&(array.value(row) / 1000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                insert_col_values.push_str(&(array.value(row)).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                insert_col_values.push_str(&(array.value(row) * 1000).to_string());
                            }
                        }
                    }
                    arrow_schema::TimeUnit::Nanosecond => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                            .unwrap();

                        match target_precision {
                            taos::Precision::Millisecond => {
                                insert_col_values
                                    .push_str(&(array.value(row) / 1_000_000).to_string());
                            }
                            taos::Precision::Microsecond => {
                                insert_col_values.push_str(&(array.value(row) / 1000).to_string());
                            }
                            taos::Precision::Nanosecond => {
                                insert_col_values.push_str(&(array.value(row)).to_string());
                            }
                        }
                    }
                },
                arrow_schema::DataType::Binary => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::FixedSizeBinary(_) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::LargeBinary => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::LargeBinaryArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(&String::from_utf8_lossy(
                        array.value(row),
                    )));
                }
                arrow_schema::DataType::Utf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(array.value(row)));
                }
                arrow_schema::DataType::LargeUtf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .unwrap();
                    insert_col_values.push_str(&sql_value_escape(array.value(row)));
                }
                dt => {
                    return Err(ArrowError::NotYetImplemented(format!(
                        "Convert `{dt:?}` to sql value"
                    )));
                }
            }
            insert_col_values.push(',');
            insert_col_names.push(',');
        }
        insert_col_values.pop();
        insert_col_names.pop();

        rows += 1;
        sql.push_str(&format!(
            " `{}` ({}) values ({})",
            table_name, insert_col_names, insert_col_values
        ));
        if sql.len() > 900_000 {
            vec.push((sql, rows, start, row));
            sql = String::with_capacity(256);
            rows = 0;
            start = row + 1;
        }
    }
    if !sql.is_empty() {
        vec.push((sql, rows, start, batch.num_rows() - 1));
    }
    Ok(vec)
}

const MAX_SQL_LENGTH: usize = 1_000_000;

/// 二分法递归拼装 SQL 语句
/// 输入是一个元组数组，元组的第一个元素是一个 SQL 片段，只包含 insert into 后面的部分，第二个元素是记录数， 比如 ("table_name using super_table tags(a int) (c1, c2, c3) values (1,2,3) (4, 5, 6),(7, 8, 9)", 3)
/// 返回值是一个元组，第一个元素是完整的 SQL 语句，第二个元素是表的数量，第三个元素 SQL 的记录数
#[allow(clippy::type_complexity)]
pub fn values_to_sqls(
    slice: &[(String, usize, RecordBatch)],
) -> Vec<(String, usize, usize, Vec<RecordBatch>)> {
    if slice.is_empty() {
        return vec![];
    }
    if let Some(sql) = valid_sql_or_none(slice) {
        return vec![sql];
    }
    let p = slice.len().div_ceil(2);
    let (left, right) = slice.split_at(p);
    let mut sqls = values_to_sqls(left);
    sqls.extend(values_to_sqls(right));
    sqls
}

/// 拼装 SQL 语句
/// 尝试将多个 table 的 values 部分拼装到一起，如果长度超过 MAX_SQL_LENGTH，则返回 None
fn valid_sql_or_none(
    slice: &[(
        String,      // One table values SQL
        usize,       // One table records
        RecordBatch, // transformed from this RecordBatch
    )],
) -> Option<(
    String,           // SQL to insert into.
    usize,            // number of tables
    usize,            // number of records
    Vec<RecordBatch>, // transformed from these RecordBatches
)> {
    if slice.len() == 1 {
        return Some((
            format!("INSERT INTO {}", slice[0].0),
            1,
            slice[0].1,
            vec![slice[0].2.clone()],
        ));
    }
    let len = slice.iter().map(|(sql, _, _)| sql.len()).sum::<usize>();
    if len < MAX_SQL_LENGTH - 12 {
        let mut sql = String::with_capacity(len + 12);
        sql.push_str("INSERT INTO ");
        let (sql, records, batches) = slice.iter().fold(
            (sql, 0, Vec::new()),
            |(mut sql, records, mut batches), (s, n, batch)| {
                sql.push_str(s);
                batches.push(batch.clone());
                (sql, records + n, batches)
            },
        );
        Some((sql, slice.len(), records, batches))
    } else {
        None
    }
}

/// 通过 dsn 连接 taosd 且不指定 database 和任何参数
pub async fn connect_taos_root(dsn: &Dsn) -> anyhow::Result<Taos> {
    let from_cloned = Dsn {
        subject: None,
        params: BTreeMap::new(),
        ..dsn.clone()
    };

    let taos = TaosBuilder::from_dsn(&from_cloned)?
        .build()
        .await
        .map_err(|err| {
            anyhow::Error::from(err)
                .context(format!("failed to connect taos with dsn: {}", from_cloned))
        })?;

    Ok(taos)
}

pub trait BlockPartitionBy: Sized {
    fn partition_by(&self, slice: &[bool]) -> (Option<Self>, Option<Self>);
}
impl BlockPartitionBy for RawBlock {
    fn partition_by(&self, slice: &[bool]) -> (Option<Self>, Option<Self>) {
        let rle: Vec<(bool, usize, usize)> = Vec::new();
        let (left, right): (Vec<_>, Vec<_>) = slice
            .iter()
            .enumerate()
            .fold(rle, |mut acc, (idx, v)| {
                if let Some((state, _start, _end)) = acc.last_mut() {
                    if *state == *v {
                        *_end = idx;
                        return acc;
                    }
                }
                acc.push((*v, idx, idx));
                acc
            })
            .into_iter()
            .partition_map(|(v, start, end)| {
                if v {
                    either::Either::Left(start..end + 1)
                } else {
                    either::Either::Right(start..end + 1)
                }
            });

        let fn_take = |ranges: Vec<std::ops::Range<usize>>| -> Option<RawBlock> {
            let views = self.column_views();
            let precision = self.precision();
            ranges
                .into_iter()
                .filter_map(|range| {
                    let views = views
                        .iter()
                        .filter_map(|view| view.slice(range.clone()))
                        .collect_vec();
                    if views.is_empty() {
                        None
                    } else {
                        Some(RawBlock::from_views(&views, precision))
                    }
                })
                .reduce(|lhs, rhs| lhs.concat(&rhs))
                .map(|mut block| {
                    block.with_table_name(self.table_name().unwrap());
                    block.with_field_names(self.field_names().to_vec());
                    block
                })
        };
        (fn_take(left), fn_take(right))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::*;
    use std::sync::Arc;
    use taos::{AsyncQueryable, AsyncTBuilder};
    use taosx_ipc::prelude::IpcDataType;

    use super::*;

    const ROWS: usize = 10;

    fn valid_values_record() -> RecordBatch {
        let now = chrono::Utc::now().timestamp_millis();
        RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    (0..ROWS).map(|i| now + i as i64 * 100),
                )) as ArrayRef,
            ),
            ("null", Arc::new(NullArray::new(ROWS))),
            (
                "bool",
                Arc::new(BooleanArray::from_iter(
                    (0..ROWS).map(|i| i % 2 == 0).map(Some),
                )) as ArrayRef,
            ),
            (
                "i8",
                Arc::new(Int8Array::from_iter_values((0..ROWS).map(|i| i as i8))) as ArrayRef,
            ),
            (
                "i16",
                Arc::new(Int16Array::from_iter_values((0..ROWS).map(|i| i as i16))) as ArrayRef,
            ),
            (
                "i32",
                Arc::new(Int32Array::from_iter_values((0..ROWS).map(|i| i as i32))) as ArrayRef,
            ),
            (
                "i64",
                Arc::new(Int64Array::from_iter_values((0..ROWS).map(|i| i as i64))) as ArrayRef,
            ),
            (
                "u8",
                Arc::new(UInt8Array::from_iter_values((0..ROWS).map(|i| i as u8))) as ArrayRef,
            ),
            (
                "u16",
                Arc::new(UInt16Array::from_iter_values((0..ROWS).map(|i| i as u16))) as ArrayRef,
            ),
            (
                "u32",
                Arc::new(UInt32Array::from_iter_values((0..ROWS).map(|i| i as u32))) as ArrayRef,
            ),
            (
                "u64",
                Arc::new(UInt64Array::from_iter_values((0..ROWS).map(|i| i as u64))) as ArrayRef,
            ),
            (
                "f32",
                Arc::new(Float32Array::from_iter_values((0..ROWS).map(|i| i as f32))) as ArrayRef,
            ),
            (
                "f64",
                Arc::new(Float64Array::from_iter_values((0..ROWS).map(|i| i as f64))) as ArrayRef,
            ),
            (
                "str",
                Arc::new(StringArray::from_iter_values(
                    (0..ROWS).map(|i| format!("str{}", i)),
                )) as ArrayRef,
            ),
            (
                "binary",
                Arc::new(BinaryArray::from_iter_values(
                    (0..ROWS).map(|i| format!("binary{}", i).into_bytes()),
                )) as ArrayRef,
            ),
            (
                "string",
                Arc::new(BinaryArray::from_iter_values(
                    (0..ROWS).map(|i| format!("string'\"\t\n!@#$%^&*()_-+={}`/?.,:;", i)),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[ignore]
    #[tokio::test]
    async fn record_to_sql() {
        let batch = valid_values_record();
        let schema = batch.schema();
        let builder = taos::TaosBuilder::from_dsn("taos:///").unwrap();
        let taos = builder.build().await.unwrap();
        for precision in [
            taos::Precision::Millisecond,
            taos::Precision::Microsecond,
            taos::Precision::Nanosecond,
        ] {
            let db = format!("precision_{precision}");
            taos.exec_many([
                format!("drop database if exists {db}"),
                format!("create database {db} precision '{precision}'"),
                format!("use {db}"),
            ])
            .await
            .unwrap();

            let stable = "stb";

            let mut stable_create = format!("create stable {stable} (ts timestamp, `null` int");
            for i in 2..batch.num_columns() {
                let field = schema.field(i);
                let name = field.name();
                let ty: IpcDataType = field.data_type().into();
                stable_create.push_str(&format!(", `{name}` {ty}", name = name, ty = ty));
            }
            stable_create.push_str(") tags(t1 int)");
            taos.exec(&stable_create).await.unwrap();

            let table_prefix = "tb";
            let tables = 100;

            let (values, _size, _start, _end) =
                &sql_values_from_record_batch(&batch, precision, true).unwrap()[0];
            let mut sql = String::new();
            sql.push_str("insert into ");
            for i in 0..tables {
                sql.push_str(&format!(
                    "{table_prefix}_{i} using {stable} tags({i})",
                    table_prefix = table_prefix,
                    stable = stable,
                    i = i
                ));
                sql.push_str(values);
            }

            let n = taos.exec(&sql).await.unwrap();

            assert_eq!(n, tables * ROWS);

            // taos.query("select * from {}")
        }
    }

    #[test]
    fn test_sql_values_from_record_batch() -> anyhow::Result<()> {
        let ts_array: ArrayRef = Arc::new(Int64Array::from(vec![100, 101]));
        let value_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.1), None]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("ts", ts_array, false),
            ("value", value_array, true),
        ])?;

        let values = sql_values_from_record_batch(&batch, taos::Precision::Nanosecond, false)?;
        assert_eq!(
            values,
            vec![("values(100,0.1)(101,NULL)".to_string(), 2, 0, 1)]
        );
        Ok(())
    }

    #[test]
    fn test_sql_values_from_record_batch_skip_null() -> anyhow::Result<()> {
        let ts_array: ArrayRef = Arc::new(Int64Array::from(vec![100, 101]));
        let value_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.1), None]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("ts", ts_array, false),
            ("value", value_array, true),
        ])?;

        let values =
            sql_values_from_record_batch_skip_null("table", &batch, taos::Precision::Nanosecond)?;
        assert_eq!(
            values,
            vec![(
                " `table` (`ts`,`value`) values (100,0.1) `table` (`ts`) values (101)".to_string(),
                2,
                0,
                1
            )]
        );
        Ok(())
    }

    #[test]
    fn test_values_to_sqls() {
        let ts_array: ArrayRef = Arc::new(Int64Array::from(vec![100, 101]));
        let value_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.1), None]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("ts", ts_array, false),
            ("value", value_array, true),
        ])
        .unwrap();

        let values = sql_values_from_record_batch(&batch, taos::Precision::Nanosecond, false)
            .unwrap()
            .into_iter()
            .map(|(sql, size, start, end)| {
                let batch = batch.slice(start, end - start + 1);
                (sql, size, batch)
            })
            .collect::<Vec<_>>();

        let sqls = values_to_sqls(&values);
        assert_eq!(sqls.len(), 1);
        assert_eq!(
            sqls[0].0,
            "INSERT INTO values(100,0.1)(101,NULL)".to_string()
        );
        assert_eq!(sqls[0].1, 1);
        assert_eq!(sqls[0].2, 2);
        assert_eq!(sqls[0].3[0].num_rows(), 2);
    }

    #[test]
    fn test_valid_sql_or_none() {
        let ts_array: ArrayRef = Arc::new(Int64Array::from(vec![100, 101]));
        let value_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.1), None]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("ts", ts_array, false),
            ("value", value_array, true),
        ])
        .unwrap();

        let values = sql_values_from_record_batch(&batch, taos::Precision::Nanosecond, false)
            .unwrap()
            .into_iter()
            .map(|(sql, size, start, end)| {
                let batch = batch.slice(start, end - start + 1);
                (sql, size, batch)
            })
            .collect::<Vec<_>>();

        let sql = valid_sql_or_none(&values);
        assert!(sql.is_some());
        let sql = sql.unwrap();
        assert_eq!(sql.0, "INSERT INTO values(100,0.1)(101,NULL)".to_string());
        assert_eq!(sql.1, 1);
        assert_eq!(sql.2, 2);
        assert_eq!(sql.3[0].num_rows(), 2);
    }

    #[test]
    fn sql_values_from_record_batch_test() -> anyhow::Result<()> {
        let ts_array: ArrayRef = Arc::new(Int64Array::from(vec![100, 101]));
        let value_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.1), None]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("ts", ts_array, false),
            ("value", value_array, true),
        ])?;
        assert_eq!(
            sql_values_from_record_batch(&batch, taos::Precision::Nanosecond, true)?,
            vec![(
                "(`ts`,`value`) values(100,0.1)(101,NULL)".to_string(),
                2,
                0,
                1
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_connect_taos_root_with_taos() {
        // TODO: test_connect_taos_root
    }
}
