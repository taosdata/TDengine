use std::{collections::HashMap, ops::Range};

use super::transform::Parser;
use crate::{
    plugins::runners::pi::transform::{PIElementModelConfig, PIPointModelConfig, SuperTableConfig},
    runners::pi::transform::PiModelType,
    utils::sql::values_to_sqls,
};
use anyhow::{anyhow, Context};
use arrow::{array::Array, record_batch::RecordBatch};
use arrow::{
    array::{ArrayRef, StringArray},
    compute::concat_batches,
};
use arrow_schema::Field;
use arrow_schema::{DataType, Schema};
use faststr::FastStr;
use lazy_static::lazy_static;
use linked_hash_map::LinkedHashMap;
use serde::Serialize;
use std::sync::Arc;
use taos::Dsn;
use taos::{taos_query::Manager, AsyncQueryable, Itertools, TaosBuilder, TaosPool, Ty};
use taosx_ipc::stream::reader::LushInsertAttrs;
use thiserror::Error;

use tracing::{error, instrument};

use crate::{
    core_metrics::TaskMetrics, plugins::transform::MessageArrowRecords,
    sink::DEFAULT_MAX_RETRIES_FOR_CONNECTION, utils::trace::RequestID,
};

use super::ipc_metric::IpcMetrics;

#[derive(Clone, Debug, Serialize)]
pub struct LushModelConfig {
    /// The name of the column that represent sub-table name in the recived RecordBatch.
    pub table_name_column: String,

    /// key:  super-table name .
    /// value: parser for the super-table.
    pub super_table_parsers: HashMap<String, Parser>,

    /// key: sub-table name in point mode, default super table name in element mode.
    /// value: super-table name.
    pub super_table_name_mapping: HashMap<String, String>,
}

#[derive(Debug)]
pub struct TableTagCache(scc::HashMap<FastStr, LushInsertAttrs>);

impl TableTagCache {
    pub fn new() -> Self {
        TableTagCache(scc::HashMap::new())
    }

    pub fn get(&self, table_name: &str) -> Option<LushInsertAttrs> {
        // get the value from the cache
        let entry = self.0.get(table_name);
        match entry {
            Some(entry) => Some(entry.get().clone()),
            None => None,
        }
    }

    pub fn insert(&self, table_name: impl Into<FastStr>, value: LushInsertAttrs) {
        let _ = self.0.insert(table_name.into(), value);
    }

    pub async fn insert_async(&self, table_name: impl Into<FastStr>, value: LushInsertAttrs) {
        let (mut k, mut v) = (table_name.into(), value);
        let mut retry = 5;
        loop {
            match self.0.insert_async(k, v).await {
                Ok(_) => break,
                Err(entry) => {
                    if retry == 0 {
                        error!("Insert table tag cache failed: {:?}", entry);
                        break;
                    }
                    (k, v) = entry;
                    retry -= 1;
                }
            }
        }
    }
}

impl LushModelConfig {
    pub fn index_super_table_by_name(
        super_table: Vec<SuperTableConfig>,
    ) -> HashMap<String, SuperTableConfig> {
        let mut map = HashMap::new();
        for super_table in super_table {
            map.insert(super_table.super_table_name.clone(), super_table);
        }
        map
    }
}

impl TryFrom<Dsn> for LushModelConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: Dsn) -> Result<Self, Self::Error> {
        let driver = dsn.driver.as_str();
        match driver {
            "pi" | "pibackfill" => {
                let transform_config_file = dsn
                    .params
                    .get("transform_config_file")
                    .ok_or(anyhow!("Not found transform_config_file in DSN params"))?;
                let transform_config_file = transform_config_file.trim_start_matches('@');
                let model: PiModelType = dsn
                    .params
                    .get("model")
                    .ok_or(anyhow!("Not found model in DSN params"))?
                    .as_str()
                    .try_into()?;
                match model {
                    PiModelType::SingleColumn => {
                        let point_model_config: PIPointModelConfig = PIPointModelConfig::from_csv(
                            transform_config_file,
                        )
                        .with_context(|| {
                            format!(
                                "Failed to create PIPointModelConfig from {}",
                                transform_config_file
                            )
                        })?;
                        Ok(point_model_config.into())
                    }
                    PiModelType::MultiColumn => {
                        let element_model_config: PIElementModelConfig =
                            PIElementModelConfig::from_csv(transform_config_file).with_context(
                                || {
                                    format!(
                                        "Failed to create PIElementModelConfig from {}",
                                        transform_config_file
                                    )
                                },
                            )?;
                        Ok(element_model_config.into())
                    }
                }
            }
            _ => Err(anyhow!("Unsupported data source")),
        }
    }
}

impl From<PIPointModelConfig> for LushModelConfig {
    fn from(config: PIPointModelConfig) -> Self {
        let super_table_config: HashMap<String, SuperTableConfig> =
            LushModelConfig::index_super_table_by_name(config.super_tables);
        let mut super_table_parsers: HashMap<String, Parser> = HashMap::new();
        for (super_table_name, config) in super_table_config.iter() {
            super_table_parsers.insert(super_table_name.to_owned(), config.to_owned().into());
        }
        let mut sub_super_mapping: HashMap<String, String> = HashMap::new();
        for point in config.points {
            sub_super_mapping.insert(point.point_name, point.super_table);
        }
        LushModelConfig {
            table_name_column: "point_name".to_string(),
            super_table_parsers: super_table_parsers,
            super_table_name_mapping: sub_super_mapping,
        }
    }
}

impl From<PIElementModelConfig> for LushModelConfig {
    fn from(config: PIElementModelConfig) -> Self {
        let super_table_name_mapping = config
            .super_tables
            .iter()
            .map(|super_table| {
                let template = super_table.template_name.as_deref().unwrap(); // element model 的配置必须有模板名
                let stable_name_from_template_name =
                    PIElementModelConfig::default_stable_name(template);
                (
                    stable_name_from_template_name,
                    super_table.super_table_name.clone(),
                )
            })
            .collect();

        let super_table_config: HashMap<String, SuperTableConfig> =
            LushModelConfig::index_super_table_by_name(config.super_tables);
        let mut super_table_parsers: HashMap<String, Parser> = HashMap::new();
        for (super_table_name, config) in super_table_config.iter() {
            super_table_parsers.insert(super_table_name.to_owned(), config.to_owned().into());
        }
        // old code that use element_id to index super_table
        // let mut sub_super_mapping: HashMap<String, String> = HashMap::new();
        // for element in config.elements {
        //     sub_super_mapping.insert(element.element_id, element.super_table);
        // }

        LushModelConfig {
            table_name_column: "element_id".to_string(),
            super_table_parsers: super_table_parsers,
            super_table_name_mapping,
        }
    }
}

pub fn join_record_batch(tags_record: &RecordBatch, values_record: &RecordBatch) -> RecordBatch {
    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut added_name = std::collections::BTreeSet::<&str>::new();
    let tags_schema = tags_record.schema();
    let values_schema = values_record.schema();
    for i in 0..tags_schema.fields().len() {
        let name = tags_schema.field(i).name().as_str();
        if added_name.contains(name) {
            continue;
        }
        added_name.insert(name);
        fields.push(tags_schema.field(i).clone());
        columns.push(tags_record.column(i).clone());
    }
    for i in 0..values_schema.fields().len() {
        let name = values_schema.field(i).name().as_str();
        if added_name.contains(name) {
            continue;
        }
        added_name.insert(name);
        fields.push(values_schema.field(i).clone());
        columns.push(values_record.column(i).clone());
    }
    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

pub fn create_tags_record(
    table_name_column: &StringArray,
    table_cache: Arc<TableTagCache>,
) -> anyhow::Result<RecordBatch> {
    // 同一个超级表的 tag 列是相同的，只需遍历第一个表的 tags
    let mut fields: Vec<Field> = Vec::new();
    let table_name0 = table_name_column.value(0);
    let table0 = table_cache
        .get(table_name0)
        .ok_or_else(|| anyhow!("table_name {} not found in table_cache", table_name0))?;
    for tag in table0.tags().as_ref().unwrap() {
        fields.push(Field::new(tag.0.clone(), DataType::Utf8, true));
    }
    // 收集每一行 tag 值
    let mut tag_values: Vec<Vec<String>> = Vec::new();
    let mut stables = Vec::<String>::new();
    for table_name in table_name_column.iter() {
        let table_name = table_name.unwrap();
        let table = table_cache
            .get(table_name)
            .ok_or_else(|| anyhow!("table_name {} not found in table_cache", table_name))?;
        let tags = table.tags().as_ref().unwrap();
        let stable = table.stable_name().as_deref().unwrap().to_string();
        stables.push(stable);
        let values: Vec<String> = tags
            .iter()
            .map(|tag| {
                let value = &tag.1;
                match value {
                    taos::Value::VarChar(v) => v.clone(),
                    taos::Value::NChar(v) => v.clone(),
                    _ => unimplemented!(),
                }
            })
            .collect();
        tag_values.push(values);
    }
    // 行转列
    let mut columns: Vec<ArrayRef> = Vec::new();
    for i in 0..fields.len() {
        let values: Vec<String> = tag_values.iter().map(|v| v[i].clone()).collect();
        let array = StringArray::from(values);
        columns.push(Arc::new(array) as ArrayRef);
    }
    // 添加 _using 列
    fields.push(Field::new("_using".to_string(), DataType::Utf8, true));
    columns.push(Arc::new(StringArray::from(stables)) as ArrayRef);

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns).map_err(|err| err.into())
}

/// 单列模型，按 table_name 列（值是子表名）对应的超级表名分组
/// 为了避免过多的内存复制，这里不是每遍历一行就调 concat_batches 方法创建一个 RecordBatch， 而是先把连续属于同一超级表的行 slice 成一个 RecordBatch，暂存起来
/// 最后对于每个超级表，调用一次 concat_batches 合并成一个 RecordBatch
pub fn group_by_super_table_name(
    records: &RecordBatch,
    name_of_table_name_column: &str,
    sub_super_mapping: &HashMap<String, String>,
) -> LinkedHashMap<String, RecordBatch> {
    let table_name_column: &Arc<dyn Array> = records
        .column_by_name(name_of_table_name_column)
        .expect("table_name_column not found");
    let table_name_column: &StringArray = table_name_column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut super_table_ranges: LinkedHashMap<&str, Vec<Range<usize>>> = LinkedHashMap::new();
    for i in 0..table_name_column.len() {
        let table_name = table_name_column.value(i);
        let super_table = sub_super_mapping.get(table_name);
        if super_table.is_none() {
            error!("table_name {} not found in sub_super_mapping", table_name);
            continue;
        }
        let super_table = super_table.unwrap();
        if super_table_ranges.contains_key(super_table.as_str()) {
            let ranges = super_table_ranges.get_mut(super_table.as_str()).unwrap();
            let last_range = ranges.last_mut().unwrap();
            if last_range.end == i {
                last_range.end += 1;
            } else {
                ranges.push(i..i + 1);
            }
        } else {
            super_table_ranges.insert(super_table.as_str(), vec![i..i + 1]);
        }
    }
    let schema = records.schema();
    super_table_ranges
        .into_iter()
        .map(|(super_table, ranges)| {
            let mut record_batches: Vec<RecordBatch> = Vec::new();
            for range in ranges {
                let record = records.slice(range.start, range.end - range.start);
                record_batches.push(record);
            }
            let record_batch = concat_batches(&schema, record_batches.iter()).unwrap();
            (super_table.to_string(), record_batch)
        })
        .collect()
}

/// 多列模型，按 _using 列（值是默认超级表名）分组
pub fn group_by_super_table_name2(records: &RecordBatch) -> LinkedHashMap<&str, RecordBatch> {
    let stable_name_column: &Arc<dyn Array> =
        records.column_by_name("_using").expect("_using not found");
    let table_name_column: &StringArray = stable_name_column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut super_table_ranges: LinkedHashMap<&str, Vec<Range<usize>>> = LinkedHashMap::new();
    for i in 0..table_name_column.len() {
        let super_table = table_name_column.value(i);
        if super_table_ranges.contains_key(super_table) {
            let ranges = super_table_ranges.get_mut(super_table).unwrap();
            let last_range = ranges.last_mut().unwrap();
            if last_range.end == i {
                last_range.end += 1;
            } else {
                ranges.push(i..i + 1);
            }
        } else {
            super_table_ranges.insert(super_table, vec![i..i + 1]);
        }
    }
    let schema = records.schema();
    super_table_ranges
        .into_iter()
        .map(|(super_table, ranges)| {
            let mut record_batches: Vec<RecordBatch> = Vec::new();
            for range in ranges {
                let record = records.slice(range.start, range.end - range.start);
                record_batches.push(record);
            }
            let record_batch = concat_batches(&schema, record_batches.iter()).unwrap();
            (super_table, record_batch)
        })
        .collect()
}

/// 与 flat_write_with_sql 不同，这里的 messages 已经都属于一个超级表， 并且在写入的时候，会忽略值为 null 的列。
#[instrument(skip_all, fields(stable=super_table_name))]
#[async_backtrace::framed]
pub async fn write(
    pool: &TaosPool,
    super_table_name: &str,
    target_precision: taos::Precision,
    req_id: &RequestID,
    messages: Vec<MessageArrowRecords>,
    metrics: &IpcMetrics,
) -> anyhow::Result<usize> {
    let timer = std::time::Instant::now();
    let mut taos = Some(pool.get().await.context("Target connection error")?);
    let cols = messages[0].records.num_columns();
    let stable = messages[0]
        .stable_name()
        .ok_or_else(|| anyhow!("stable name not found in MessageArrowRecords"))?;
    let sqls = message_to_sql(super_table_name, &messages, target_precision);
    tracing::debug!(gensql.elapsed = ?timer.elapsed(), "Generate SQLs");
    let timer = std::time::Instant::now();
    // 写入成功返回的总行数
    let mut written_rows = 0;
    for records in sqls {
        let mut retry = 0;
        let taos = &mut taos;
        loop {
            retry += 1;
            match write_lush_stable_with_sql(pool, taos, req_id, &records, metrics).await {
                Ok(n) => {
                    metrics.add_written_points((n * cols) as u64);
                    written_rows += n;
                    break;
                }
                Err(err) => match err {
                    WriteError::TableNotExits(_) => {
                        if let Some(stable_sql) = messages[0].stable_sql() {
                            tracing::info!("Create stable sql={stable_sql}");
                            assert_create_table(pool, taos, &stable_sql, req_id, true, metrics)
                                .await?;
                        }
                        for m in &messages {
                            let sql = m.table_sql();
                            tracing::info!("Create table sql={sql}");
                            for _ in 0..6 {
                                if let Err(err) =
                                    assert_create_table(pool, taos, &sql, req_id, false, metrics)
                                        .await
                                {
                                    match err {
                                        WriteError::ContainerLengthTooShort(field) => {
                                            // 尝试修改超级表
                                            let _ = alter_table(
                                                pool, taos, stable, &field, &messages, req_id,
                                            )
                                            .await;
                                            // 无论成功失败都重试建表
                                        }
                                        _ => Err(err)?,
                                    }
                                } else {
                                    // 成功创建子表则退出循环
                                    break;
                                }
                            }
                        }
                    }
                    WriteError::ContainerLengthTooShort(field) => {
                        let _ = alter_table(pool, taos, stable, &field, &messages, req_id).await;
                    }
                    _ => {
                        return Err(err)?;
                    }
                },
            }
            if retry > 2 {
                tracing::error!(stable, "Retry insert exceeded {retry}");
                return Err(anyhow!("Retry insert exceeded {retry}"));
            }
        }
    }
    let total_tables = messages.len();
    tracing::debug!(write.elapsed = ?timer.elapsed(), "Wrote total {total_tables} talbes total {written_rows} rows");
    Ok(written_rows)
}

async fn alter_table(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    stable: &str,
    field: &str,
    messages: &[MessageArrowRecords],
    req_id: &RequestID,
) -> anyhow::Result<()> {
    let alter_table_max_retry = 3;
    let mut retry = 0;
    loop {
        retry += 1;
        let desc = taos.as_ref().unwrap().describe(stable).await;
        if let Err(err) = desc {
            let code = err.code();
            let errno: i32 = code.into();
            match errno {
                0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                    taos.replace(pool.get().await?);
                    if retry > alter_table_max_retry {
                        tracing::error!("Alter table retry execeeded {retry}, {err:#}");
                        return Err(err.into());
                    } else {
                        continue;
                    }
                }
                _ => {
                    tracing::error!(
                        req_id = req_id.trace_id_str(),
                        "Describe table error: {err:#}"
                    );
                    return Err(err.into());
                }
            }
        }
        let desc = desc.unwrap();
        let f = desc
            .iter()
            .find(|f| f.field() == field)
            .ok_or_else(|| anyhow::anyhow!("field `{}` not found in table `{}`", field, stable))?;
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
                    let max = if f.ty() == Ty::VarChar { 16382 } else { 4093 };
                    (f.length() * 2).min(max)
                })
            );
            tracing::info!(sql = sql, "Alter table");
            match taos
                .as_ref()
                .unwrap()
                .exec_with_req_id(&sql, req_id.next())
                .await
            {
                Err(err) => {
                    // Alter table error: [0x264B] Internal error: `Only varbinary/binary/nchar/geometry column length could be modified, and the length can only be increased, not decreased`
                    let code = err.code();
                    let errno: i32 = code.into();
                    match errno {
                        0x264B | 0x036F => {
                            tracing::warn!(
                                req_id = req_id.trace_id_str(),
                                sql,
                                "Ignore alter table error: {err:#}"
                            );
                            return Ok(());
                        }
                        0x03D3 => {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            return Ok(());
                        }
                        0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                            taos.replace(pool.get().await?);
                            if retry > alter_table_max_retry {
                                tracing::error!("Alter table retry execeeded {retry}, {err:#}");
                                return Err(err.into());
                            }
                        }
                        _ => {
                            tracing::error!(
                                req_id = req_id.trace_id_str(),
                                sql,
                                "Alter table error: {err:#}"
                            );
                            return Err(err.into());
                        }
                    }
                }
                _ => return Ok(()),
            }
        } else {
            let sql = format!(
                "alter table `{}` modify column `{}` {}({})",
                stable,
                f.field(),
                f.ty(),
                length.unwrap_or_else(|| {
                    let max = if f.ty() == Ty::VarChar { 65517 } else { 16382 };
                    (f.length() * 2).min(max)
                })
            );
            tracing::info!(sql = sql, "Alter table");
            match taos
                .as_ref()
                .unwrap()
                .exec_with_req_id(&sql, req_id.next())
                .await
            {
                Err(err) => {
                    // Alter table error: [0x264B] Internal error: `Only varbinary/binary/nchar/geometry column length could be modified, and the length can only be increased, not decreased`
                    let code = err.code();
                    let errno: i32 = code.into();
                    match errno {
                        0x264B | 0x036F => {
                            tracing::warn!(
                                req_id = req_id.trace_id_str(),
                                sql,
                                "Ignore alter table error: {err:#}"
                            );
                            return Ok(());
                        }
                        0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                            taos.replace(pool.get().await?);
                            if retry > alter_table_max_retry {
                                tracing::error!("Alter table retry execeeded {retry}, {err:#}");
                                return Err(err.into());
                            }
                        }
                        _ => {
                            tracing::error!(
                                req_id = req_id.trace_id_str(),
                                sql,
                                "Alter table error: {err:#}"
                            );
                            return Err(err.into());
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }
}

fn message_to_sql(
    super_table_name: &str,
    messages: &[MessageArrowRecords],
    target_precision: taos::Precision,
) -> Vec<Records> {
    // 一个子表对应一条 SQL，形如：
    // format!("`{}` using `{}` ({}) tags({}) {}", tbname, using, names, tag_values, col_values))
    let values = messages
        .into_iter()
        .flat_map(|m| {
            m.sql_insert_part_skip_null(target_precision)
                .into_iter()
                .map(|sql| (sql, 0))
        })
        .collect_vec();

    let stable_name_iter = std::iter::repeat(super_table_name);
    values_to_sqls(&values)
        .into_iter()
        .zip(stable_name_iter)
        .map(|((sql, tables, records), stable)| Records {
            stable: Some(stable.to_string()),
            sql,
            tables,
            records,
        })
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
}
impl<'a> AsRef<str> for Records {
    fn as_ref(&self) -> &str {
        self.sql.as_str()
    }
}

type TaosConnection = deadpool::managed::Object<Manager<TaosBuilder>>;

#[derive(Debug, Error)]
pub enum WriteError {
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

async fn assert_create_table(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    sql: &str,
    req_id: &RequestID,
    is_stable: bool,
    metrics: &IpcMetrics,
) -> Result<(), WriteError> {
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
            if write_retries > 5 {
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
                0x2653 => {
                    // Value too long for column/tag
                    let message = err.message();
                    if let Some(caps) = RE_0X2653.captures(&message) {
                        let field = caps.get(1).unwrap().as_str();
                        tracing::debug!("Create table error: {}", message);
                        break Err(WriteError::ContainerLengthTooShort(field.to_string()));
                    }
                    break Err(err)?;
                }
                // [0x2603] Internal error: `Table does not exist`
                0x2603 => {
                    // retry
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    tracing::warn!(retry = write_retries, "{:#}", err);
                }
                _ => {
                    tracing::error!(
                        req_id = req_id.trace_id_str(),
                        "Create {} error: {err:#}, sql={}",
                        if is_stable { "stable" } else { "table" },
                        sql
                    );
                    break Err(err)
                        .context(format!(
                            "Create {} error",
                            if is_stable { "stable" } else { "table" }
                        ))
                        .map_err(Into::into);
                }
            }
        } else {
            if is_stable {
                // stable
                metrics.add_created_stables(1);
            } else {
                // table
                metrics.add_created_tables(1);
            }
            break Ok(());
        }
    }
}

lazy_static! {
    static ref RE_0X2653: regex::Regex =
        regex::Regex::new(r"`Value too long for column/tag: (.*)`").unwrap();
}

#[instrument(skip_all)]
async fn write_lush_stable_with_sql(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    req_id: &RequestID,
    records: &Records,
    metrics: &IpcMetrics,
) -> Result<usize, WriteError> {
    let mut write_retries = 0;
    let sql = records.sql();
    // let debug_sql = if sql.len() > 300 { &sql[0..300] } else { sql };
    // tracing::trace!(req_id = req_id.get(), sql = debug_sql, "Write with SQL");
    loop {
        match taos
            .as_ref()
            .unwrap()
            .exec_with_req_id(sql, req_id.next())
            .await
        {
            Ok(n) => {
                metrics.add_inserted_sqls(1 as u64);
                metrics.add_written_rows(n as u64);
                break Ok(n);
            }
            Err(err) => {
                let code = err.code();
                let errno: i32 = code.into();
                write_retries += 1;
                if write_retries > DEFAULT_MAX_RETRIES_FOR_CONNECTION {
                    break Err(err)
                        .context("Write with SQL error: Retries exceeded")
                        .map_err(Into::into);
                }
                match errno {
                    0x2603 | 0x0618 => {
                        // stable/table not exists
                        break Err(WriteError::TableNotExits(
                            records.stable.as_deref().unwrap_or("unknown").to_string(),
                        ));
                    }
                    0x2653 => {
                        // Value too long for column/tag
                        let message = err.message();
                        tracing::debug!("Write stable error: {}", message);
                        if let Some(caps) = RE_0X2653.captures(&message) {
                            let field = caps.get(1).unwrap().as_str();
                            break Err(WriteError::ContainerLengthTooShort(field.to_string()));
                        }
                        break Err(err).map_err(Into::into);
                    }
                    0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                        let period = match write_retries {
                            errors if errors < 8 => 8,
                            errors if errors < 16 => 16,
                            errors if errors < 32 => 32,
                            errors if errors < 64 => 64,
                            _ => 128,
                        };
                        tokio::time::sleep(std::time::Duration::from_millis(period * 80)).await;
                        taos.replace(pool.get().await?);
                    }
                    _ => {
                        tracing::error!(
                            sql = truncate_sql_in_log_message(sql),
                            req_id = req_id.trace_id_str(),
                            "Write SQL error: {err:#}"
                        );
                        break Err(err).context("Write sql error").map_err(Into::into);
                    }
                }
            }
        }
    }
}

fn truncate_sql_in_log_message(sql: &str) -> &str {
    if sql.len() > 500 {
        &sql[0..500]
    } else {
        sql
    }
}
