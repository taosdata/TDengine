use crate::pi::{PIElementModelConfig, PIPointModelConfig, SuperTableConfig};
use std::collections::HashMap;

use super::transform::Parser;

pub struct LushTransfromConfig {
    /// The name of the column that represent sub-table name in the recived RecordBatch.
    pub table_name_column: String,
    /// key: the value of sub-table name column.
    /// For PI point model, the column is point_id;
    /// For PI element model, the column is element_id;
    pub config: HashMap<String, Parser>,
}

impl LushTransfromConfig {
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

impl From<PIPointModelConfig> for LushTransfromConfig {
    fn from(config: PIPointModelConfig) -> Self {
        let table_map: HashMap<String, SuperTableConfig> =
            LushTransfromConfig::index_super_table_by_name(config.super_tables);
        let mut map: HashMap<String, Parser> = HashMap::new();
        for point in config.points {
            let super_table = table_map.get(point.super_table.as_str()).unwrap();
            map.insert(point.point_id.to_string(), super_table.to_owned().into());
        }
        LushTransfromConfig {
            table_name_column: "point_id".to_string(),
            config: map,
        }
    }
}

impl From<PIElementModelConfig> for LushTransfromConfig {
    fn from(config: PIElementModelConfig) -> Self {
        let table_map: HashMap<String, SuperTableConfig> =
            LushTransfromConfig::index_super_table_by_name(config.super_tables);
        let mut map: HashMap<String, Parser> = HashMap::new();
        for element in config.elements {
            let super_table = table_map.get(element.super_table.as_str()).unwrap();
            map.insert(element.element_id.clone(), super_table.to_owned().into());
        }
        LushTransfromConfig {
            table_name_column: "element_id".to_string(),
            config: map,
        }
    }
}
