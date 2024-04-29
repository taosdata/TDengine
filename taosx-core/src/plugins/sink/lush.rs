use std::collections::HashMap;

use super::transform::Parser;
use crate::{
    plugins::runners::pi::transform::{PIElementModelConfig, PIPointModelConfig, SuperTableConfig},
    runners::pi::transform::PiModelType,
};
use anyhow::{anyhow, Context};
use std::sync::Arc;
use taos::Dsn;
use taosx_ipc::stream::reader::LushInsertAttrs;

#[derive(Clone, Debug)]
pub struct LushModelConfig {
    /// The name of the column that represent sub-table name in the recived RecordBatch.
    pub table_name_column: String,
    /// key:  super-table name .
    /// value: parser for the super-table.
    pub super_table_parsers: HashMap<String, Parser>,
    /// key: sub-table name.
    /// value: super-table name.
    pub sub_super_mapping: HashMap<String, String>,
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

#[cfg(test)]
mod test {
    #[test]
    fn test_table_cache() {
        use super::TableTagCache;
        let cache = TableTagCache::new();
        cache.insert("table1".to_string(), Default::default());
        assert!(cache.get("table1").is_some());
        assert!(cache.get("table2").is_none());
    }
}
