use anyhow::Context;
use lazy_static::lazy_static;
use log::warn;
use taos::{taos_query::Manager, AsyncQueryable, Itertools, TaosBuilder, TaosPool, Ty};
use thiserror::Error;

use tracing::{error, instrument};

use crate::{
    core_metrics::TaskMetrics, plugins::transform::MessageArrowRecords,
    sink::DEFAULT_MAX_RETRIES_FOR_CONNECTION, utils::trace::RequestID,
};

use super::ipc_metric::IpcMetrics;

/// All the messages should be in the same stable.
fn message_to_sql(
    messages: &[MessageArrowRecords],
    precision: taos::Precision,
    with_meta: bool,
) -> Vec<Records> {
    debug_assert!(
        messages
            .iter()
            .group_by(|m| m.stable_name())
            .into_iter()
            .count()
            == 1,
        "all messages should be in the same stable"
    );
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
        if len < MAX_SQL_LENGTH {
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

    messages
        .iter()
        .group_by(|m| m.stable_name())
        .into_iter()
        .map(|(key, group)| {
            let values = group
                .into_iter()
                .flat_map(|m| {
                    m.sql_insert_part(precision, with_meta).map(|sql| {
                        (
                            sql,                  // SQL to insert into.
                            m.records.num_rows(), // number of records
                        )
                    })
                })
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
struct Records {
    stable: Option<String>,
    sql: String,
    tables: usize,
    records: usize,
}
impl Records {
    fn sql(&self) -> &str {
        self.sql.as_str()
    }

    fn records(&self) -> usize {
        self.records
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
    req_id: &RequestID,
) -> Result<(), FlatWriteError> {
    let mut write_retries = 0;
    loop {
        if let Err(err) = taos
            .as_ref()
            .unwrap()
            .exec_with_req_id(sql, req_id.next())
            .await
        {
            let code = err.code();
            let errno: i32 = code.into();
            write_retries += 1;
            tracing::warn!(sql = sql, "Exec SQL error: {err:#}");
            if write_retries > DEFAULT_MAX_RETRIES_FOR_CONNECTION {
                // counter!(METRIC_STABLE_CREATED, 1);
                // TODO: add metrics
                break Err(err)
                    .context("Exec SQL error: Retries exceeded")
                    .map_err(Into::into);
            }
            match errno {
                0x032C | 0x0603 | 0x03C7 | 0x03D3 | 0x0360 => {
                    break Ok(());
                }
                0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                    taos.replace(pool.get().await?);
                }
                _ => {
                    break Err(err).context("Create stable error").map_err(Into::into);
                }
            }
        } else {
            break Ok(());
        }
    }
}

lazy_static! {
    static ref RE_0X2653: regex::Regex =
        regex::Regex::new(r"`Value too long for column/tag: (.*)`").unwrap();
}

/// TODO: maybe helpful for refactor
#[allow(dead_code)]
async fn assert_exec_sql(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    sql: &str,
    req_id: &RequestID,
) -> Result<(), FlatWriteError> {
    let mut write_retries = 0;
    loop {
        if let Err(err) = taos
            .as_ref()
            .unwrap()
            .exec_with_req_id(sql, req_id.next())
            .await
        {
            let code = err.code();
            let errno: i32 = code.into();
            write_retries += 1;
            tracing::warn!(sql = sql, "Exec SQL error: {err:#}");
            if write_retries > DEFAULT_MAX_RETRIES_FOR_CONNECTION {
                // counter!(METRIC_WRITE_RAW_BLOCK_FAILS, 1);
                // TODO: add metrics
                break Err(err)
                    .context("Exec SQL error: Retries exceeded")
                    .map_err(Into::into);
            }
            match errno {
                0x2603 | 0x0618 => {
                    // stable not exists
                    break Err(FlatWriteError::TableNotExits("unknown".to_string()));
                }
                0x2653 => {
                    // Value too long for column/tag
                    let message = err.message();
                    if let Some(caps) = RE_0X2653.captures(&message) {
                        let field = caps.get(1).unwrap().as_str();
                        break Err(FlatWriteError::ContainerLengthTooShort(field.to_string()));
                    }
                    break Err(err).map_err(Into::into);
                }
                // 0x2605 => {
                //     // container length is too short.
                //     // break Err(FlatWriteError::ContainerLengthTooShort(err));
                // }
                0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                    taos.replace(pool.get().await?);
                }
                _ => {
                    // counter!(METRIC_WRITE_RAW_BLOCK_FAILS, 1);
                    // TODO: add metrics
                    break Err(err)
                        .context("flat message write sql error")
                        .map_err(Into::into);
                }
            }
        } else {
            break Ok(());
        }
    }
}
async fn write_stable_with_sql(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    req_id: &RequestID,
    records: &Records,
) -> Result<usize, FlatWriteError> {
    let mut write_retries = 0;
    let sql = records.sql();

    loop {
        match taos
            .as_ref()
            .unwrap()
            .exec_with_req_id(sql, req_id.next())
            .await
        {
            Ok(n) => break Ok(n),
            Err(err) => {
                let code = err.code();
                let errno: i32 = code.into();
                write_retries += 1;
                tracing::warn!(
                    sql,
                    "flat message write sql encountered unrecoverable err: {err:#}"
                );
                if write_retries > DEFAULT_MAX_RETRIES_FOR_CONNECTION {
                    break Err(err)
                        .context("Write flat stream with SQL error: Retries exceeded")
                        .map_err(Into::into);
                }
                match errno {
                    0x2603 | 0x0618 => {
                        // stable/table not exists
                        break Err(FlatWriteError::TableNotExits(
                            records.stable.as_deref().unwrap_or("unknown").to_string(),
                        ));
                    }
                    0x2653 => {
                        // Value too long for column/tag
                        let message = err.message();
                        if let Some(caps) = RE_0X2653.captures(&message) {
                            let field = caps.get(1).unwrap().as_str();
                            break Err(FlatWriteError::ContainerLengthTooShort(field.to_string()));
                        }
                        break Err(err).map_err(Into::into);
                    }
                    0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                        taos.replace(pool.get().await?);
                    }
                    _ => {
                        break Err(err)
                            .context("flat message write sql error")
                            .map_err(Into::into);
                    }
                }
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
    req_id: &RequestID,
    messages: Vec<MessageArrowRecords>,
    metrics: &IpcMetrics,
) -> anyhow::Result<usize> {
    let mut count = 0;
    // Split messages into different stales.
    let cols = messages[0].records.num_columns();
    let groups = messages
        .into_iter()
        .into_group_map_by(|m| m.stable_name().map(|s| s.to_string()));
    for (stable, messages) in groups.into_iter() {
        let sqls = message_to_sql(&messages, target_precision, true);
        for records in sqls {
            loop {
                match write_stable_with_sql(pool, taos, req_id, &records).await {
                    Ok(n) => {
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
                        tracing::warn!(stable, "write stable with sql error: {err:#}");
                        match err {
                            FlatWriteError::TableNotExits(_) => {
                                if let Some(stable_sql) = messages[0].stable_sql() {
                                    tracing::info!(
                                        sql = stable_sql,
                                        stable = stable.as_deref(),
                                        "stable not exists, create stable with sql: {stable_sql}"
                                    );
                                    assert_create_stable(pool, taos, &stable_sql, req_id).await?;
                                }

                                for m in &messages {
                                    let sql = m.table_sql();
                                    assert_create_stable(pool, taos, &sql, req_id).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::*;
    use arrow_schema::{Field, FieldRef, Schema};
    use serde_json::json;
    use std::{collections::HashMap, sync::Arc};
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

        let req_id = RequestID::new(0);
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
            &req_id,
            messages,
            &metrics,
        )
        .await?;

        Ok(())
    }
}
