use std::{collections::HashMap, time::Duration};

use super::{
    flat::Records,
    transform::{modeler::Modeler, Parser},
};
use crate::{
    plugins::runners::pi::transform::{PIElementModelConfig, PIPointModelConfig, SuperTableConfig},
    runners::pi::transform::PiModelType,
    utils::{breakpoints::BreakpointDb, sql::values_to_sqls, trace::Qid},
};
use anyhow::{anyhow, Context};
use arrow::array::{ArrayRef, StringArray, UInt16Builder};
use arrow::{array::Array, record_batch::RecordBatch};
use arrow_compute_ext::*;
use arrow_schema::Field;
use arrow_schema::{DataType, Schema};
use chrono::DateTime;
use faststr::FastStr;
use itertools::Itertools as _;
use lazy_static::lazy_static;
use linked_hash_map::LinkedHashMap;
use serde::Serialize;
use std::sync::Arc;
use taos::Dsn;
use taos::{taos_query::Manager, AsyncQueryable, TaosBuilder, TaosPool, Ty};
use taoslog::{
    utils::{QidMetadataGetter, Span},
    QidManager,
};
use taosx_ipc::stream::reader::LushInsertAttrs;
use thiserror::Error;

use tracing::{error, instrument, Instrument};

use crate::{
    core_metrics::TaskMetrics, plugins::transform::MessageArrowRecords,
    sink::DEFAULT_MAX_RETRIES_FOR_CONNECTION,
};

use super::ipc_metric::IpcMetrics;

/// 一批数据每个子表的最后时间戳，作为断点信息。
/// key 为子表的唯一标识，例如 PI 系统中的 element_id
/// value 为 DateTime<UTC> 的字符串表示
type TableBreakPoints = Vec<(String, String)>;
#[derive(Clone, Debug, Serialize)]
pub struct LushModelConfig {
    /// The name of the column that can uniquely represent a sub-table in the received RecordBatch.
    pub table_id_column: String,

    /// key:  super-table name .
    /// value: parser for the super-table.
    pub super_table_parsers: HashMap<String, Parser>,

    pub super_table_sqls: HashMap<String, String>,

    /// key: sub-table name in point mode, default super table name in element mode.
    /// value: super-table name.
    pub super_table_name_mapping: HashMap<String, String>,
    // 写入的时候是否跳过 null 值
    // 目前实现：PI backfill 不跳过 null 值，PI 实时数据跳过 null 值
    pub skip_null: bool,
}

#[derive(Debug)]
pub struct TableTagCache(scc::HashMap<FastStr, Arc<LushInsertAttrs>>);

impl Default for TableTagCache {
    fn default() -> Self {
        Self::new()
    }
}

impl TableTagCache {
    pub fn new() -> Self {
        TableTagCache(scc::HashMap::new())
    }

    pub fn get(&self, table_name: &str) -> Option<Arc<LushInsertAttrs>> {
        // get the value from the cache
        let entry = self.0.get(table_name);
        entry.map(|entry| entry.get().clone())
    }

    pub fn insert(&self, table_name: impl Into<FastStr>, value: impl Into<Arc<LushInsertAttrs>>) {
        let _ = self.0.insert(table_name.into(), value.into());
    }

    pub async fn insert_async(
        &self,
        table_name: impl Into<FastStr>,
        value: impl Into<Arc<LushInsertAttrs>>,
    ) {
        let (k, v) = (table_name.into(), value.into());
        let _ = self.0.insert_async(k, v).await;
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

    /// 判断一个超级表是不是只有标签列。
    /// _c1 是我们为了创建超级表，添加的伪列。
    pub fn is_labels_only_stable(modeler: &Modeler) -> bool {
        // 对于 LushModelConfig 的 Parser 的 modeler，有且仅有一个 table
        let table = modeler.first().unwrap();
        let columns = table.columns.as_ref().unwrap();
        if columns.len() == 2 {
            for column in columns {
                if column == "_c1" {
                    return true;
                }
            }
        }
        false
    }
}

impl TryFrom<Dsn> for LushModelConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: Dsn) -> Result<Self, Self::Error> {
        let driver = dsn.driver.as_str();
        let mut config: LushModelConfig = match driver {
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
        }?;
        match driver {
            "pi" => config.skip_null = true,
            "pibackfill" => config.skip_null = false,
            _ => {}
        }
        Ok(config)
    }
}

impl From<PIPointModelConfig> for LushModelConfig {
    fn from(config: PIPointModelConfig) -> Self {
        let super_table_config: HashMap<String, SuperTableConfig> =
            LushModelConfig::index_super_table_by_name(config.super_tables);
        let mut super_table_sqls: HashMap<String, String> = HashMap::new();
        let mut super_table_parsers: HashMap<String, Parser> = HashMap::new();
        for (super_table_name, config) in super_table_config.iter() {
            super_table_sqls.insert(super_table_name.to_owned(), config.get_sql());
            super_table_parsers.insert(super_table_name.to_owned(), config.to_owned().into());
        }
        let mut sub_super_mapping: HashMap<String, String> = HashMap::new();
        for point in config.points {
            //sub_super_mapping.insert(point.point_name, point.super_table);
            // 暂不支持点级别配对应的超级表
            sub_super_mapping.insert(point.super_table.clone(), point.super_table);
        }
        LushModelConfig {
            table_id_column: "point_name".to_string(),
            super_table_parsers,
            super_table_sqls,
            super_table_name_mapping: sub_super_mapping,
            skip_null: false,
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
        let mut super_table_sqls: HashMap<String, String> = HashMap::new();
        for (super_table_name, config) in super_table_config.iter() {
            super_table_sqls.insert(super_table_name.to_owned(), config.get_sql());
            super_table_parsers.insert(super_table_name.to_owned(), config.to_owned().into());
        }
        // old code that use element_id to index super_table
        // let mut sub_super_mapping: HashMap<String, String> = HashMap::new();
        // for element in config.elements {
        //     sub_super_mapping.insert(element.element_id, element.super_table);
        // }

        LushModelConfig {
            table_id_column: "element_id".to_string(),
            super_table_parsers,
            super_table_sqls,
            super_table_name_mapping,
            skip_null: true,
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
    name_of_table_id_column: &str,
    table_id_column: &StringArray,
    table_cache: Arc<TableTagCache>,
) -> anyhow::Result<RecordBatch> {
    // 同一个超级表的 tag 列是相同的，只需遍历第一个表的 tags
    let mut fields: Vec<Field> = Vec::new();
    let table_name0 = table_id_column.value(0);
    let table0 = table_cache
        .get(table_name0)
        .ok_or_else(|| anyhow!("table_name {} not found in table_cache", table_name0))?;
    for tag in table0.tags().as_ref().unwrap() {
        fields.push(Field::new(tag.0.clone(), DataType::Utf8, true));
    }
    // 收集每一行 tag 值
    let mut tag_values: Vec<Vec<String>> = Vec::new();
    let mut stables = Vec::new();
    for table_id in table_id_column.iter() {
        let table_id = table_id.unwrap();
        let table = table_cache
            .get(table_id)
            .ok_or_else(|| anyhow!("table_name {} not found in table_cache", table_id))?;
        let tags = table.tags().as_ref().unwrap();
        let stable = table.stable_name().unwrap().clone();
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
    columns.push(Arc::new(StringArray::from_iter_values(
        stables.iter().map(|fs| fs.as_str()),
    )) as ArrayRef);
    // 添加 table id 列
    fields.push(Field::new(
        name_of_table_id_column.to_string(),
        DataType::Utf8,
        true,
    ));
    columns.push(Arc::new(table_id_column.clone()) as ArrayRef);
    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns).map_err(|err| err.into())
}

/// 单列模型，按 table_name 列（值是子表名）对应的超级表名分组
/// 为了避免过多的内存复制，这里不是每遍历一行就调 concat_batches 方法创建一个 RecordBatch， 而是先把连续属于同一超级表的行 slice 成一个 RecordBatch，暂存起来
/// 最后对于每个超级表，调用一次 concat_batches 合并成一个 RecordBatch
pub fn group_by_super_table_name<'b>(
    records: &RecordBatch,
    name_of_table_name_column: &str,
    sub_super_mapping: &'b HashMap<String, String>,
) -> LinkedHashMap<&'b str, RecordBatch> {
    let table_name_column: &Arc<dyn Array> = records
        .column_by_name(name_of_table_name_column)
        .expect("table_name_column not found");
    let table_name_column: &StringArray = table_name_column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut super_table_ranges: LinkedHashMap<&str, UInt16Builder> = LinkedHashMap::new();
    for i in 0..table_name_column.len() {
        let table_name = table_name_column.value(i);
        let super_table = sub_super_mapping.get(table_name);
        if super_table.is_none() {
            error!("table_name {} not found in sub_super_mapping", table_name);
            continue;
        }
        let super_table = super_table.unwrap().as_str();
        if super_table_ranges.contains_key(super_table) {
            let builder = super_table_ranges.get_mut(super_table).unwrap();
            builder.append_value(i as _);
        } else {
            let mut builder = UInt16Builder::new();
            builder.append_value(i as _);
            super_table_ranges.insert(super_table, builder);
        }
    }
    super_table_ranges
        .into_iter()
        .map(|(super_table, mut builder)| {
            let indices = builder.finish();
            let record_batch = records.take(&indices).unwrap();
            (super_table, record_batch)
        })
        .collect()
}

/// 多列模型，按 _using 列（值是默认超级表名）分组
pub fn group_by_super_table_name2(records: &RecordBatch) -> LinkedHashMap<&str, RecordBatch> {
    let stable_name_column: &Arc<dyn Array> =
        records.column_by_name("_using").expect("_using not found");
    let stable_name_column: &StringArray = stable_name_column
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut super_table_ranges: LinkedHashMap<&str, UInt16Builder> = LinkedHashMap::new();
    for i in 0..stable_name_column.len() {
        let super_table = stable_name_column.value(i);
        if super_table_ranges.contains_key(super_table) {
            let builder = super_table_ranges.get_mut(super_table).unwrap();
            builder.append_value(i as _);
        } else {
            let mut builder = arrow::array::UInt16Builder::new();
            builder.append_value(i as _);
            super_table_ranges.insert(super_table, builder);
        }
    }
    super_table_ranges
        .into_iter()
        .map(|(super_table, mut builder)| {
            let indices = builder.finish();
            let record_batch = records.take(&indices).unwrap();
            (super_table, record_batch)
        })
        .collect()
}

/// 与 flat_write_with_sql 不同，这里的 messages 已经都属于一个超级表， 并且在写入的时候，会根据参数决定是否忽略值为 null 的列。
#[instrument(skip_all, fields(stable=super_table_name))]
#[async_backtrace::framed]

pub async fn write(
    pool: &TaosPool,
    super_table_name: &str,
    target_precision: taos::Precision,
    messages: Vec<MessageArrowRecords>,
    metrics: &IpcMetrics,
    skip_null: bool,
    table_id_column: &str,
    breakpoints: BreakpointDb,
) -> anyhow::Result<(usize, Duration, Duration)> {
    let table_break_points = get_break_point(&messages, table_id_column);
    let mut taos = Some(pool.get().await.context("Target connection error")?);
    let cols = messages[0].records.num_columns();
    let stable = messages[0]
        .stable_name()
        .ok_or_else(|| anyhow!("stable name not found in MessageArrowRecords"))?;
    let timer = std::time::Instant::now();
    let sqls = if skip_null {
        message_to_sql(super_table_name, &messages, target_precision)
    } else {
        super::flat::message_to_sql(&messages, target_precision, true, false)
    }; //
    let gen_sql_time = timer.elapsed();
    let timer = std::time::Instant::now();
    // 写入成功返回的总行数
    let mut written_rows = 0;
    for records in sqls {
        let mut retry = 0;
        let taos = &mut taos;
        let mut error = Ok(());
        loop {
            retry += 1;
            match write_lush_stable_with_sql(pool, taos, &records, metrics)
                .in_current_span()
                .await
            {
                Ok(n) => {
                    metrics.add_written_points((n * cols) as u64);
                    written_rows += n;
                    break;
                }
                Err(err) => match err {
                    WriteError::TableNotExits(_) => {
                        if let Some(stable_sql) = messages[0].stable_sql() {
                            tracing::info!("{stable_sql}");
                            assert_create_table(pool, taos, &stable_sql, true, metrics)
                                .in_current_span()
                                .await?;
                        }
                        for m in &messages {
                            let sql = m.table_sql();
                            tracing::info!("{sql}");
                            for retry in 0..12 {
                                if let Err(err) =
                                    assert_create_table(pool, taos, &sql, false, metrics)
                                        .in_current_span()
                                        .await
                                {
                                    match err {
                                        WriteError::ContainerLengthTooShort(field) => {
                                            // 尝试修改超级表
                                            if let Err(alter) =
                                                alter_stable(pool, taos, stable, &field, &messages)
                                                    .in_current_span()
                                                    .await
                                            {
                                                tracing::error!(
                                                    stable,
                                                    field,
                                                    "Alter table error: {alter:#}"
                                                );
                                                let context = format!("Try alter table {stable} field `{field}` round {retry} error: {alter:#}");
                                                if error.is_err() {
                                                    error = error.context(context);
                                                } else {
                                                    error = Err(
                                                        WriteError::ContainerLengthTooShort(field),
                                                    )
                                                    .context(context);
                                                }
                                            }
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
                        if let Err(alter) = alter_stable(pool, taos, stable, &field, &messages)
                            .in_current_span()
                            .await
                        {
                            tracing::error!(stable, field, "Alter table error: {alter:#}");
                            let context = format!("Try alter table {stable} field `{field}` round {retry} error: {alter:#}");
                            if error.is_err() {
                                error = error.context(context);
                            } else {
                                error = Err(WriteError::ContainerLengthTooShort(field))
                                    .context(context);
                            }
                        } else {
                            retry -= 1;
                            let context = format!(
                                "Try alter table {stable} field `{field}` round {retry} success"
                            );
                            if error.is_err() {
                                error = error.context(context);
                            } else {
                                error = Err(WriteError::ContainerLengthTooShort(field))
                                    .context(context);
                            }
                        }
                    }
                    _ => {
                        return Err(err)?;
                    }
                },
            }
            if retry > 5 {
                if let Err(err) = error {
                    tracing::error!(
                        stable,
                        error = format!("{err:#}"),
                        backtrace = ?err,
                        "Retry insert exceeded {retry}"
                    );
                    return Err(err)
                        .with_context(|| format!("Insert retries exceeded with {retry} times"))?;
                }
                tracing::error!(stable, "Retry insert exceeded {retry}");
                return Err(anyhow!("Retry insert exceeded {retry}"));
            }
        }
    }
    let write_time = timer.elapsed();
    breakpoints.batch_set(table_break_points).await?;
    Ok((written_rows, gen_sql_time, write_time))
}

// 获取每个子表的最后时间戳，作为断点信息
fn get_break_point(messages: &Vec<MessageArrowRecords>, table_id_column: &str) -> TableBreakPoints {
    let mut table_break_points = Vec::new();
    for m in messages {
        let table_key = m.table.get_tag_value_by_name(table_id_column);
        if table_key.is_none() {
            tracing::error!("table_id_column {} not found in tags", table_id_column);
            break;
        }
        let ts_col = m.get_ts_column();
        if ts_col.is_none() {
            tracing::error!(
                "Can't get primary key column. schema={}",
                m.records.schema()
            );
            break;
        }
        let table_key = table_key.unwrap();
        let ts_col = ts_col.unwrap();
        let last_ts = ts_col.value(ts_col.len() - 1);
        let last_date_time = DateTime::from_timestamp(last_ts / 1000, 0);
        if let Some(date_time) = last_date_time {
            let date_time = date_time.to_rfc3339();
            table_break_points.push((table_key.to_string(), date_time));
        } else {
            tracing::error!("Can't convert timestamp to DateTime {}", last_ts);
        }
    }
    table_break_points
}

pub async fn create_sub_tables(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    stable: &str,
    messages: &Vec<MessageArrowRecords>,
    metrics: &IpcMetrics,
) -> anyhow::Result<()> {
    for m in messages {
        let sql = m.table_sql();
        let table_name = m.table_name();
        tracing::info!("Creating table {}", table_name);
        let mut retry = 0;
        let mut error = Ok(());
        loop {
            if retry > 12 {
                tracing::error!("Create sub table {table_name} retry exceeded {retry}");
                return error;
            }
            if let Err(err) = assert_create_table(pool, taos, &sql, false, metrics)
                .in_current_span()
                .await
            {
                match err {
                    WriteError::ContainerLengthTooShort(field) => {
                        // 尝试修改超级表
                        if let Err(alter) = alter_stable(pool, taos, stable, &field, messages)
                            .in_current_span()
                            .await
                        {
                            tracing::error!(stable, field, "Alter table error: {alter:#}");
                            let context = format!("Try alter table {stable} field `{field}` round {retry} error: {alter:#}");
                            if error.is_err() {
                                error = error.context(context);
                            } else {
                                error = Err(WriteError::ContainerLengthTooShort(field))
                                    .context(context);
                            }
                        }
                        // 无论成功失败都重试建表
                    }
                    _ => Err(err)?,
                }
            } else {
                // 成功创建子表则退出循环
                tracing::info!("Created table {}", table_name);
                break;
            }
            retry += 1;
        }
    }
    Ok(())
}

async fn alter_stable(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    stable: &str,
    field: &str,
    messages: &[MessageArrowRecords],
) -> anyhow::Result<()> {
    let alter_table_max_retry = 3;
    let mut retry = 0;

    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    loop {
        retry += 1;
        let desc = taos.as_ref().unwrap().describe(stable).await;
        if let Err(err) = desc {
            let code = err.code();
            let errno: i32 = code.into();
            match errno {
                0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                    tokio::time::sleep(std::time::Duration::from_millis(50 * retry)).await;
                    if retry > alter_table_max_retry {
                        tracing::error!("Alter table retry exceeded {retry}, {err:#}");
                        return Err(err)
                            .with_context(|| "Describe {stable} error: Retries exceeded");
                    } else {
                        taos.replace(pool.get().await?);
                        continue;
                    }
                }
                _ => {
                    tracing::error!("Describe table error: {err:#}");
                    return Err(err).with_context(|| "Describe {stable} error: Retries exceeded");
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
            let length = length.unwrap_or_else(|| {
                let max = if f.ty() == Ty::VarChar { 16382 } else { 4093 };
                (f.length() * 2).min(max)
            });
            if f.length() >= length {
                tracing::debug!(
                    stable,
                    field,
                    "Expect tag length {length} is less than or equal to current length {}",
                    f.length()
                );
                return Ok(());
            }
            let sql = format!(
                "alter table `{}` modify tag `{}` {}({})",
                stable,
                f.field(),
                f.ty(),
                length
            );
            qid.add_sub_batch_id();
            tracing::info!(sql = sql, "Alter table");
            match taos
                .as_ref()
                .unwrap()
                .exec_with_req_id(&sql, qid.get())
                .in_current_span()
                .await
            {
                Err(err) => {
                    // Alter table error: [0x264B] Internal error: `Only varbinary/binary/nchar/geometry column length could be modified, and the length can only be increased, not decreased`
                    let code = err.code();
                    let errno: i32 = code.into();
                    match errno {
                        0x264B | 0x036F => {
                            tracing::warn!(sql, "Ignore alter table error: {err:#}");
                            return Ok(());
                        }
                        0x03D3 => {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            return Ok(());
                        }
                        0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                            tokio::time::sleep(std::time::Duration::from_millis(50 * retry)).await;
                            taos.replace(pool.get().await?);
                            if retry > alter_table_max_retry {
                                tracing::error!("Alter table retry exceeded {retry}, {err:#}");
                                return Err(err).with_context(|| sql).with_context(|| {
                                    format!(
                                        "Alter table {stable} tag `{field}` error: Retries exceeded"
                                    )
                                });
                            }
                        }
                        _ => {
                            tracing::error!(sql, "Alter table error: {err:#}");
                            return Err(err).with_context(|| sql).with_context(|| {
                                format!("Alter table {stable} tag `{field}` error")
                            });
                        }
                    }
                }
                _ => {
                    tracing::trace!("exec sql successfully");
                    return Ok(());
                }
            }
        } else {
            let length = length.unwrap_or_else(|| {
                let max = if f.ty() == Ty::VarChar { 65517 } else { 16382 };
                (f.length() * 2).min(max)
            });
            if f.length() >= length {
                tracing::debug!(
                    stable,
                    field,
                    "Expect column length {length} is less than or equal to current length {}",
                    f.length()
                );
                return Ok(());
            }
            let sql = format!(
                "alter table `{}` modify column `{}` {}({})",
                stable,
                f.field(),
                f.ty(),
                length,
            );
            qid.add_sub_batch_id();
            tracing::info!(sql = sql, "Alter table");
            match taos
                .as_ref()
                .unwrap()
                .exec_with_req_id(&sql, qid.get())
                .in_current_span()
                .await
            {
                Err(err) => {
                    // Alter table error: [0x264B] Internal error: `Only varbinary/binary/nchar/geometry column length could be modified, and the length can only be increased, not decreased`
                    let code = err.code();
                    let errno: i32 = code.into();
                    match errno {
                        0x264B | 0x036F => {
                            tracing::warn!(sql, "Ignore alter table error: {err:#}");
                            return Ok(());
                        }
                        0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                            taos.replace(pool.get().await?);
                            if retry > alter_table_max_retry {
                                tracing::error!("Alter table retry exceeded {retry}, {err:#}");
                                return Err(err).with_context(|| sql).with_context(|| {
                                    format!(
                                        "Alter table {stable} column `{field}` error: Retries exceeded"
                                    )
                                });
                            }
                        }
                        _ => {
                            tracing::error!(sql, "Alter table error: {err:#}");
                            return Err(err).with_context(|| sql).with_context(|| {
                                format!("Alter table {stable} column `{field}` error")
                            });
                        }
                    }
                }
                _ => {
                    tracing::trace!("exec sql successfully");
                    return Ok(());
                }
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
        .iter()
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

pub async fn assert_create_table(
    pool: &TaosPool,
    taos: &mut Option<TaosConnection>,
    sql: &str,
    is_stable: bool,
    metrics: &IpcMetrics,
) -> Result<(), WriteError> {
    let mut write_retries = 0;
    let mut qid = taoslog::utils::Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    loop {
        qid.add_sub_batch_id();
        if let Err(err) = taos
            .as_ref()
            .unwrap()
            .exec_with_req_id(sql, qid.get())
            .in_current_span()
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
            tracing::trace!("exec sql successfully");
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
    records: &Records,
    metrics: &IpcMetrics,
) -> Result<usize, WriteError> {
    let mut write_retries = 0;
    let sql = records.sql();
    let mut qid = Span.get_qid().unwrap_or_else(Qid::init);
    loop {
        qid.add_sub_batch_id();
        match taos
            .as_ref()
            .unwrap()
            .exec_with_req_id(sql, qid.get())
            .in_current_span()
            .await
        {
            Ok(n) => {
                tracing::trace!("exec sql successfully");
                metrics.add_inserted_sqls(1_u64);
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
                            records.stable().unwrap_or("unknown").to_string(),
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
                        break Err(Into::into(err));
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

#[instrument(skip_all)]
pub fn get_table_name_from_table_id(
    table_id: &str,
    table_cache: Arc<TableTagCache>,
    lush_model_config: Arc<LushModelConfig>,
) -> Option<String> {
    let table_id_column = StringArray::from(vec![table_id]);
    let tags_records: Result<RecordBatch, anyhow::Error> =
        create_tags_record("element_id", &table_id_column, table_cache.clone());
    if let Err(err) = tags_records {
        tracing::error!("{err:#}");
        return None;
    }
    let records: RecordBatch = tags_records.unwrap();
    let default_super_table = records
        .column_by_name("_using")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let super_table = lush_model_config
        .super_table_name_mapping
        .get(default_super_table);
    if super_table.is_none() {
        tracing::error!(
            "default_super_table {} not found in super_table_name_mapping",
            default_super_table
        );
        return None;
    }
    let super_table = super_table.unwrap();
    let parser = lush_model_config.super_table_parsers.get(super_table);
    if parser.is_none() {
        tracing::error!("super_table {super_table} not found in super_table_parsers");
        return None;
    }
    let parser = parser.unwrap();
    let modeler = parser.modeler();
    let table = modeler.table0();
    if table.is_none() {
        tracing::error!("no table in modeler");
        return None;
    }
    let table = table.unwrap();
    let table_name = table.eval_table_name(&records);
    if let Err(err) = table_name {
        tracing::error!("{err:#}");
        return None;
    }
    let table_name = table_name.unwrap();
    let table_name = table_name.value(0);
    let table_name = parser.global().canonical_table_name(table_name);
    Some(table_name.to_string())
}

#[instrument(skip_all)]
pub(crate) async fn drop_table(pool: &TaosPool, table_name: &str) -> anyhow::Result<()> {
    let sql = format!("DROP TABLE IF EXISTS `{}`", table_name);
    exec_sql(pool, &sql).await
}

#[instrument(skip_all)]
pub(crate) async fn delete_table_data(
    pool: &TaosPool,
    table_name: &str,
    condition: &str,
) -> anyhow::Result<()> {
    let sql = format!("DELETE FROM `{}` WHERE {}", table_name, condition);
    exec_sql(pool, &sql).await
}

#[instrument(skip_all)]
pub(crate) async fn alter_table(
    pool: &TaosPool,
    table_name: &str,
    alter_table_clause: &str,
) -> anyhow::Result<()> {
    let sql = format!("ALTER TABLE `{}` {}", table_name, alter_table_clause);
    exec_sql(pool, &sql).await
}

#[instrument(skip_all)]
pub(crate) async fn insert_into_table(
    pool: &TaosPool,
    table_name: &str,
    column_values: &str,
) -> anyhow::Result<()> {
    let sql = format!("INSERT INTO `{}` {}", table_name, column_values);
    exec_sql(pool, &sql).await
}

#[instrument(skip_all)]
pub(crate) async fn exec_sql(pool: &TaosPool, sql: &str) -> anyhow::Result<()> {
    tracing::debug!("exec_sql: {sql}");
    let mut taos = Some(pool.get().await.context("get connection error")?);
    let mut write_retries = 0;
    let mut qid = Span.get_qid().unwrap_or_else(Qid::init);
    // debug_assert!(qid.task_id() > 0);
    // debug_assert!(qid.batch_id() > 0);
    loop {
        qid.add_sub_batch_id();
        match taos
            .as_ref()
            .unwrap()
            .exec_with_req_id(sql, qid.get())
            .in_current_span()
            .await
        {
            Ok(_) => {
                tracing::trace!("exec sql successfully");
                break Ok(());
            }
            Err(err) => {
                let code = err.code();
                let errno: i32 = code.into();
                write_retries += 1;
                if write_retries > DEFAULT_MAX_RETRIES_FOR_CONNECTION {
                    break Err(err)
                        .context("Exec SQL error: Retries exceeded")
                        .map_err(Into::into);
                }
                match errno {
                    0x0E001 | 0x0E002 | 0x0E003 | 0x000B => {
                        taos.replace(pool.get().await?);
                    }
                    0x2603 | 0x0618 => {
                        tracing::warn!("Table not exists, sql={}", sql);
                        return Ok(());
                    }
                    _ => {
                        tracing::error!("Exec SQL error: {:#}, sql={}", err, sql,);
                        break Err(err).context("Exec sql error").map_err(Into::into);
                    }
                }
            }
        }
    }
}
