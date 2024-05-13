use std::collections::HashMap;

use super::transform::Parser;
use crate::{
    plugins::runners::pi::transform::{PIElementModelConfig, PIPointModelConfig, SuperTableConfig},
    runners::pi::transform::PiModelType,
};
use anyhow::{anyhow, Context};
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
    fn test_parser() {
        let s = r#"{
            "global": {
              "replace_dot_in_table_name": "_"
            },
            "parse": null,
            "mutate": [
              {
                "filter": [
                  {
                    "Expr": {
                      "expr": ""
                    }
                  }
                ]
              },
              {
                "map": {
                  "value": {
                    "expr": "value",
                    "null_if_error": true,
                    "as": "double"
                  },
                  "status": {
                    "expr": "status",
                    "null_if_error": true,
                    "as": "int"
                  },
                  "path": {
                    "expr": "path",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "ptclassname": {
                    "expr": "ptclassname",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "sourcetag": {
                    "expr": "sourcetag",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "tag": {
                    "expr": "tag",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "descriptor": {
                    "expr": "descriptor",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "exdesc": {
                    "expr": "exdesc",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "engunits": {
                    "expr": "engunits",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "pointsource": {
                    "expr": "pointsource",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "step": {
                    "expr": "step",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "future": {
                    "expr": "future",
                    "null_if_error": true,
                    "as": "nchar(100)"
                  },
                  "element_paths": {
                    "expr": "element_paths.replace('\', 'b')",
                    "null_if_error": true,
                    "as": "nchar(512)"
                  }
                }
              }
            ],
            "model": [
              {
                "name": "${point_name}",
                "using": "volt_double",
                "tags": [
                  "path",
                  "ptclassname",
                  "sourcetag",
                  "tag",
                  "descriptor",
                  "exdesc",
                  "engunits",
                  "pointsource",
                  "step",
                  "future",
                  "element_paths"
                ],
                "columns": [
                  "ts",
                  "value",
                  "status"
                ],
                "where": null,
                "global": null
              }
            ]
          }"#;
        use crate::plugins::transform::Parser;

        let parse: Parser = serde_json::from_str(s).unwrap();
        println!("{:?}", serde_json::to_string(&parse).unwrap());
    }
}
