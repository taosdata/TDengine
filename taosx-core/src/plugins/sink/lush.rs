use std::{collections::HashMap, ops::Range};

use super::transform::Parser;
use crate::{
    plugins::runners::pi::transform::{PIElementModelConfig, PIPointModelConfig, SuperTableConfig},
    runners::pi::transform::PiModelType,
};
use anyhow::{anyhow, Context};
use arrow::{array::Array, record_batch::RecordBatch};
use arrow::{
    array::{ArrayRef, StringArray},
    compute::concat_batches,
};
use arrow_schema::Field;
use arrow_schema::{DataType, Schema};
use linked_hash_map::LinkedHashMap;
use serde::Serialize;
use std::sync::Arc;
use taos::Dsn;
use taosx_ipc::stream::reader::LushInsertAttrs;

#[derive(Clone, Debug, Serialize)]
pub struct LushModelConfig {
    /// The name of the column that represent sub-table name in the recived RecordBatch.
    pub table_name_column: String,
    /// key:  super-table name .
    /// value: parser for the super-table.
    pub super_table_parsers: HashMap<String, Parser>,
    /// key: sub-table name.
    /// value: super-table name.
    pub sub_super_mapping: HashMap<String, String>,
    #[serde(skip)]
    pub table_tags: Arc<TableTagCache>,
}

#[derive(Debug)]
pub struct TableTagCache(scc::HashMap<String, LushInsertAttrs>);

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

    pub fn insert(&self, table_name: String, value: LushInsertAttrs) {
        let _ = self.0.insert(table_name, value);
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
            sub_super_mapping: sub_super_mapping,
            table_tags: Arc::new(TableTagCache::new()),
        }
    }
}

impl From<PIElementModelConfig> for LushModelConfig {
    fn from(config: PIElementModelConfig) -> Self {
        let super_table_config: HashMap<String, SuperTableConfig> =
            LushModelConfig::index_super_table_by_name(config.super_tables);
        let mut super_table_parsers: HashMap<String, Parser> = HashMap::new();
        for (super_table_name, config) in super_table_config.iter() {
            super_table_parsers.insert(super_table_name.to_owned(), config.to_owned().into());
        }
        let mut sub_super_mapping: HashMap<String, String> = HashMap::new();
        for element in config.elements {
            sub_super_mapping.insert(element.element_id, element.super_table);
        }
        LushModelConfig {
            table_name_column: "element_id".to_string(),
            super_table_parsers: super_table_parsers,
            sub_super_mapping: sub_super_mapping,
            table_tags: Arc::new(TableTagCache::new()),
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
    table_cache: &TableTagCache,
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
    for table_name in table_name_column.iter() {
        let table_name = table_name.unwrap();
        let table = table_cache
            .get(table_name)
            .ok_or_else(|| anyhow!("table_name {} not found in table_cache", table_name))?;
        let tags = table.tags().as_ref().unwrap();
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

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), columns).map_err(|err| err.into())
}

/// 按 table_name 列（值是子表名）对应的超级表名分组
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
        let super_table = sub_super_mapping.get(table_name).unwrap();
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

#[cfg(test)]
mod test {
    use crate::runners::pi::transform::PIPointModelConfig;

    use super::LushModelConfig;

    #[test]
    fn test_table_cache() {
        use super::TableTagCache;
        let cache = TableTagCache::new();
        cache.insert("table1".to_string(), Default::default());
        assert!(cache.get("table1").is_some());
        assert!(cache.get("table2").is_none());
    }

    #[test]
    fn test_create_lush_model_config() {
        let point_model_config =
            PIPointModelConfig::from_csv("default_pi_config_1714435852.csv").unwrap();
        let super_tables = &point_model_config.super_tables;
        let super_table = super_tables.get(0).unwrap();
        let scheam = super_table.schema.clone();
        for row in scheam {
            println!("{}", row.column_map);
        }

        let lush_model_config = LushModelConfig::from(point_model_config);
        let parser = lush_model_config
            .super_table_parsers
            .get("volt_double")
            .unwrap();
        println!("{}", serde_json::to_string_pretty(parser).unwrap());
    }

    #[test]
    fn test_group_by_super_table_name() {
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::record_batch::RecordBatch;
        use arrow_schema::Field;
        use arrow_schema::{DataType, Schema};
        use std::sync::Arc;

        let schema = Schema::new(vec![
            Field::new("table_name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]);
        let table_name = StringArray::from(vec![
            "table1", "table1", "table2", "table2", "table1", "table1", "table2", "table2",
            "table1", "table1", "table2", "table2", "table1", "table1", "table2", "table2",
            "table1", "table1", "table2", "table2", "table1", "table1", "table2", "table2",
            "table1", "table1", "table2", "table2", "table1", "table1", "table2", "table2",
            "table1", "table1", "table2", "table2", "table1", "table1", "table2", "table2",
            "table1", "table1", "table2", "table2", "table1", "table1", "table2", "table2",
            "table1", "table1", "table2", "table2", "table1", "table1", "table2", "table2",
            "table1", "table1", "table2", "table2", "table1", "table1", "table2", "table2",
        ]);
        let value = Int64Array::from(vec![
            1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1,
            2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2,
            3, 4, 1, 2, 3, 4,
        ]);
        let record = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(table_name) as ArrayRef,
                Arc::new(value) as ArrayRef,
            ],
        )
        .unwrap();
        let sub_super_mapping: std::collections::HashMap<String, String> = vec![
            ("table1".to_string(), "super_table1".to_string()),
            ("table2".to_string(), "super_table2".to_string()),
        ]
        .iter()
        .cloned()
        .collect();

        // 统计耗时
        let start = std::time::Instant::now();
        let grouped_batches =
            super::group_by_super_table_name(&record, "table_name", &sub_super_mapping);
        let elapsed = start.elapsed();
        println!("elapsed: {:?}", elapsed);
        let super_table1 = grouped_batches.get("super_table1").unwrap();
        let super_table2 = grouped_batches.get("super_table2").unwrap();
        println!("{:?}", super_table1);
        println!("{:?}", super_table2);
    }
}
