use std::collections::HashMap;
use std::str::FromStr;

use anyhow::bail;
use csv_async::StringRecord;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use taos::{Dsn, Ty};

use taosx_ipc::prelude::IpcDataType;
use taosx_ipc::types::DataSet;

use crate::runners::opc::config::csv::header::CsvHeader;
use crate::runners::opc::config::csv::CsvParser;
use crate::runners::opc::config::OPCConfig;
use crate::runners::opc::{generate_stable_from_pattern, generate_tbname_from_pattern, OpcType};
use crate::utils::rhai_syntax_validator::check_math_expression;
use crate::utils::validate_table_column_name;

/// 点位映射规则的生成方式
/// Rule: 通过自定义的规则生成点位映射规则
/// Csv: 通过csv文件中的配置生成点位映射规则
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GeneratePointMappingBy {
    Rule(OpcPointMappingRule),
    Csv((Vec<String>, Option<String>)),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpcPointMappingRule {
    pub opc_type: OpcType,
    pub stable_expression: String,
    pub tbname_expression: String,
    pub primary_key: String,
    pub primary_key_alias: String,
}

impl OpcPointMappingRule {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let stable_expression = OPCConfig::parse_stable_expression(dsn)?;
        let tbname_expression = OPCConfig::parse_tbname_expression(dsn)?;
        let primary_key =
            OPCConfig::parse_primary_key(dsn)?.unwrap_or(ColumnConfig::ORIGINAL_TS.to_string());
        let primary_key_alias =
            OPCConfig::parse_primary_key_alias(dsn)?.unwrap_or("ts".to_string());

        Ok(Self {
            opc_type,
            stable_expression,
            tbname_expression,
            primary_key,
            primary_key_alias,
        })
    }

    pub fn generate(
        &self,
        data: Vec<DataSet>,
    ) -> anyhow::Result<(
        LinkedHashMap<String, PointConfig>,
        LinkedHashMap<String, TableConfig>,
    )> {
        let mut point_map = LinkedHashMap::new();
        let mut table_map = LinkedHashMap::new();

        for (index, p) in data.into_iter().enumerate() {
            let point_id = p.id;
            let point_type = p.r#type;

            let value_type = point_type
                .map(|t| {
                    IpcDataType::from_str(t.as_str()).map_err(|_err| {
                        anyhow::anyhow!("failed to convert point type: {} to IpcDataType", t)
                    })
                })
                .transpose()?;

            // point_config
            let point_config =
                self.gen_point_config(index, point_id.clone(), value_type.clone())?;
            point_map.insert(point_id.clone(), point_config);

            // table_config
            let table_config = self.gen_table_config(value_type.clone())?;
            table_map.insert(point_id.clone(), table_config);
        }

        Ok((point_map, table_map))
    }

    pub fn gen_point_config(
        &self,
        index: usize,
        point_id: String,
        point_type: Option<IpcDataType>,
    ) -> anyhow::Result<PointConfig> {
        let driver = self.opc_type.to_string();

        let tbname = generate_tbname_from_pattern(
            driver.as_str(),
            self.tbname_expression.as_str(),
            point_id.as_str(),
        );
        let stable = generate_stable_from_pattern(&self.stable_expression, &point_type);

        let point_config = PointConfig {
            row_index: index,
            code: tbname,
            stable: Some(stable),
            tag_values: None,
            value_type: point_type,
        };

        Ok(point_config)
    }

    pub fn gen_table_config(&self, point_type: Option<IpcDataType>) -> anyhow::Result<TableConfig> {
        let value_type = point_type.map(|t| t.ty());

        let mut column_configs = vec![];
        column_configs.push(ColumnConfig {
            name: ColumnConfig::VALUE.to_string(),
            r#type: value_type,
            alias: Some(String::from("val")),
            transform: None,
            is_primary_key: false,
        });
        column_configs.push(ColumnConfig {
            name: ColumnConfig::QUALITY.to_string(),
            r#type: Some(Ty::Int),
            alias: None,
            transform: None,
            is_primary_key: false,
        });
        match self.primary_key.as_str() {
            ColumnConfig::ORIGINAL_TS => {
                column_configs.push(ColumnConfig {
                    name: ColumnConfig::ORIGINAL_TS.to_string(),
                    r#type: Some(Ty::Timestamp),
                    alias: Some(self.primary_key_alias.clone()),
                    transform: None,
                    is_primary_key: true,
                });
            }
            ColumnConfig::RECEIVED_TS => {
                column_configs.push(ColumnConfig {
                    name: ColumnConfig::RECEIVED_TS.to_string(),
                    r#type: Some(Ty::Timestamp),
                    alias: Some(self.primary_key_alias.clone()),
                    transform: None,
                    is_primary_key: true,
                });
            }
            _ => {
                bail!("invalid primary key: {}", self.primary_key);
            }
        }

        let table_config = TableConfig {
            enabled: Some(1),
            stable_prefix: None,
            column_configs,
            tag_configs: None,
        };

        Ok(table_config)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpcModelConfig {
    pub opc_type: OpcType,
    pub generate_rule: Option<GeneratePointMappingBy>,
    pub point_config_map: LinkedHashMap<String, PointConfig>,
    pub table_config_map: LinkedHashMap<String, TableConfig>,
}

impl OpcModelConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        // check stable, stable is required
        for (point_id, point_config) in self.point_config_map.iter() {
            if point_config.stable.is_none() {
                bail!("stable is required for point_id: {}", point_id);
            }
        }

        // check ts_col/ received_ts_col
        for (point_id, table_config) in self.table_config_map.iter() {
            let mut has_primary_key = false;
            for col_config in table_config.column_configs.iter() {
                if col_config.is_primary_key {
                    has_primary_key = true;
                    break;
                }
            }
            if !has_primary_key {
                bail!(
                    "ts_col or received_ts_col is required for point_id: {}",
                    point_id
                );
            }
        }

        Ok(())
    }
    pub fn get_point_mapping(
        &self,
        point_id: &str,
    ) -> anyhow::Result<Option<(&PointConfig, &TableConfig)>> {
        let point_config = self.point_config_map.get(point_id);
        let table_config = self.table_config_map.get(point_id);

        match (point_config, table_config) {
            (Some(point_config), Some(table_config)) => Ok(Some((point_config, table_config))),
            (None, None) => Ok(None),
            _ => bail!(
                "point_id: {} not found in point_config_map or table_config_map",
                point_id
            ),
        }
    }

    pub async fn generate_point_mapping(
        &self,
        point_id: &str,
        value_type: &IpcDataType,
    ) -> anyhow::Result<(PointConfig, TableConfig)> {
        if self.point_config_map.len() != self.table_config_map.len() {
            bail!(
                "point_config_map length: {} not equal to table_config_map length: {}",
                self.point_config_map.len(),
                self.table_config_map.len()
            );
        }

        let generate_rule = self
            .generate_rule
            .clone()
            .ok_or(anyhow::anyhow!("generate_rule is required"))?;

        match &generate_rule {
            GeneratePointMappingBy::Rule(rule) => {
                let index = self.point_config_map.len();
                let p =
                    rule.gen_point_config(index, point_id.to_string(), Some(value_type.clone()))?;
                let t = rule.gen_table_config(Some(value_type.clone()))?;
                Ok((p, t))
            }
            GeneratePointMappingBy::Csv((csv_files, csv_origin)) => {
                let parser = match csv_origin {
                    None => CsvParser::try_new(self.opc_type.clone(), csv_files.clone())?,
                    Some(csv_origin) => {
                        CsvParser::try_new(self.opc_type.clone(), vec![format!("@{}", csv_origin)])?
                    }
                };

                let (p, t) = parser.parse_one(point_id).await?.ok_or(anyhow::anyhow!(
                    "point_id: {} not found in csv files: {:?}",
                    point_id,
                    csv_files
                ))?;
                Ok((p, t))
            }
        }
    }

    pub async fn generate_transform_map(&self, column_name: &str) -> HashMap<String, ColumnConfig> {
        let result = self.generate_transform_map_impl(column_name).await;
        match result {
            Ok(map) => map,
            Err(err) => {
                tracing::warn!("failed to generate transform map, use an empty HashMap instead, column: {}, err: {}",column_name,err.to_string());
                HashMap::new()
            }
        }
    }

    async fn generate_transform_map_impl(
        &self,
        column_name: &str,
    ) -> anyhow::Result<HashMap<String, ColumnConfig>> {
        match &self.generate_rule {
            None => {
                bail!("generate rule is required")
            }
            Some(GeneratePointMappingBy::Rule(_rule)) => {
                bail!("generate transform map by GeneratePointMappingBy::Rule is not supported")
            }
            Some(GeneratePointMappingBy::Csv((csv, csv_origin))) => {
                let parser = match csv_origin {
                    None => CsvParser::try_new(self.opc_type.clone(), csv.clone())?,
                    Some(csv_origin) => {
                        CsvParser::try_new(self.opc_type.clone(), vec![format!("@{}", csv_origin)])?
                    }
                };
                parser.parse_transform(column_name).await
            }
        }
    }

    pub fn get_column_config_map_by_name(&self, col_name: &str) -> HashMap<String, ColumnConfig> {
        let mut column_config_map = HashMap::new();

        for (point_id, table_config) in &self.table_config_map {
            let column_config = table_config.column_config(col_name);
            if let Some(column_config) = column_config {
                column_config_map.insert(point_id.clone(), column_config.clone());
            }
        }

        column_config_map
    }

    pub fn is_conflict(
        point_id: &str,
        point_config: &PointConfig,
        table_config: &TableConfig,
        point_config_map: &LinkedHashMap<String, PointConfig>,
        table_config_map: &LinkedHashMap<String, TableConfig>,
    ) -> anyhow::Result<()> {
        if table_config.enabled.is_some_and(|v| v == 0) {
            return Ok(());
        }

        let stable = point_config.stable.as_ref();
        let tbname = point_config.code.as_str();

        if let Some(stable) = stable {
            if stable.contains("{type}") {
                return Ok(());
            }
        }
        if tbname.contains("{id}") || tbname.contains("{ns}") || tbname.contains("{tag_name}") {
            return Ok(());
        }

        let value_col = table_config
            .column_config(ColumnConfig::VALUE)
            .and_then(|v| v.alias.as_ref());

        // 遍历 self.point_config_map 和 self.table_config_map，当 stable 和 tbname 时，value_col 应该不同，否则报错
        for (id, p_config) in point_config_map {
            if let Some(t_config) = table_config_map.get(id) {
                if p_config.stable.as_ref() == stable && p_config.code.as_str() == tbname {
                    if let Some(v_col) = t_config.column_config(ColumnConfig::VALUE) {
                        if v_col.alias.as_ref() == value_col {
                            bail!(
                                "point_id: {} and point_id: {} have same stable: {} and tbname: {}, value_col should be different",
                                id,
                                point_id,
                                stable.unwrap(),
                                tbname,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PointConfig {
    pub row_index: usize,
    pub code: String, // code is tbname
    pub stable: Option<String>,
    pub tag_values: Option<HashMap<String, String>>,
    pub value_type: Option<IpcDataType>,
}

impl PointConfig {
    pub fn from_csv(
        header: &CsvHeader,
        row: &StringRecord,
        row_index: usize,
    ) -> anyhow::Result<Self> {
        let code = CsvParser::parse_tbname(header, row)?;
        let value_type = parse_type(header, row)?;
        let stable = parse_stable(header, row);
        let tag_values = parse_tag_values(header, row);
        if stable.is_some() {
            validate_table_column_name("stable name", stable.as_ref().unwrap())?;
        }

        // 遍历tag_values，校验tag_values中的tag_name是否合法
        if tag_values.is_some() {
            for tag_name in tag_values.as_ref().unwrap().keys() {
                validate_table_column_name("tag name", tag_name)?;
            }
        }

        Ok(PointConfig {
            row_index,
            code,
            stable,
            tag_values,
            value_type,
        })
    }
}

fn parse_type(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Option<IpcDataType>> {
    header
        .get_column("type")
        .and_then(|col| row.get(col.index))
        .map(|val| {
            if val.is_empty() {
                return Ok(None);
            }
            match IpcDataType::from_str(val) {
                Err(_e) => {
                    bail!("invalid column data type: {}", val)
                }
                Ok(value_type) => Ok(Some(value_type)),
            }
        })
        .unwrap_or(Ok(None))
}

fn parse_raw_type(header: &CsvHeader, row: &StringRecord) -> Option<String> {
    header
        .get_column("type")
        .and_then(|col| row.get(col.index))
        .and_then(|val| {
            if val.is_empty() {
                return None;
            }
            match val.find("(") {
                Some(index) => Some(val[..index].to_string().replace(" ", "_")),
                None => Some(val.replace(" ", "_")),
            }
        })
}

fn parse_stable(header: &CsvHeader, row: &StringRecord) -> Option<String> {
    header
        .get_column("stable")
        .and_then(|col| row.get(col.index))
        .and_then(|val| {
            if val.is_empty() {
                return None;
            }
            let val = val.replace(".", "_");
            let val_type = parse_raw_type(header, row);
            let stable_name = match (val.contains("{type}"), val_type) {
                (true, Some(val_type)) => val.replace("{type}", &val_type),
                _ => val,
            };
            Some(stable_name)
        })
}

/// example:
///      tag::VARCHAR(200)::name
///      入库温度
/// tag_value map:
///      name => 入库温度
fn parse_tag_values(header: &CsvHeader, row: &StringRecord) -> Option<HashMap<String, String>> {
    let mut map = HashMap::new();

    for col in header.get_columns() {
        if !col.is_tag {
            continue;
        }
        let tag_name = col.name.clone();
        let tag_value = row.get(col.index).unwrap_or("").to_string();

        map.insert(tag_name, tag_value);
    }

    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

#[cfg(test)]
mod point_config_tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_stable() {
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["point1", "stable1"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("stable1".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["point1", ""]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, None);

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "meters_{type}"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("meters_{type}".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable", "type"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "meters_{type}", ""]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("meters_{type}".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable", "type"]),
        )
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "stable1_{type}", "varchar(200)"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("stable1_varchar".to_string()));
    }
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TableConfig {
    pub enabled: Option<i8>,
    pub stable_prefix: Option<String>,
    pub column_configs: Vec<ColumnConfig>,
    pub tag_configs: Option<Vec<TagConfig>>,
}

const DEFAULT_STABLE_PREFIX: &str = "opc";

impl TableConfig {
    pub fn empty() -> Self {
        TableConfig {
            enabled: None,
            stable_prefix: None,
            column_configs: vec![],
            tag_configs: None,
        }
    }

    pub fn from_csv(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Self> {
        let stable = parse_stable(header, row);
        let stable_prefix = match stable {
            None => Some(String::from(DEFAULT_STABLE_PREFIX)),
            Some(_stable) => None,
        };

        let enabled = CsvParser::parse_enabled(header, row)?;
        let column_configs = parse_columns(header, row)?;
        let tag_configs = parse_tags(header);
        let tag_configs = if tag_configs.is_empty() {
            None
        } else {
            Some(tag_configs)
        };

        Ok(Self {
            enabled,
            stable_prefix,
            column_configs,
            tag_configs,
        })
    }

    pub fn column_config(&self, name: &str) -> Option<&ColumnConfig> {
        self.column_configs.iter().find(|c| c.name == name)
    }
}

fn parse_columns(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Vec<ColumnConfig>> {
    let mut columns = Vec::new();

    // value => value_col
    let value = parse_value_col(header, row)?;
    columns.push(value);

    // quality => quality_col
    let quality = parse_quality_col(header, row)?;
    if let Some(quality) = quality {
        columns.push(quality);
    }

    // original_ts
    let original_ts = parse_original_ts_col(header, row)?;
    // received_ts
    let received_ts = parse_received_ts_col(header, row)?;

    match (original_ts, received_ts) {
        (Some(origin_ts), Some(received_ts)) => {
            columns.push(origin_ts);
            columns.push(received_ts);
        }
        (Some(origin_ts), None) => {
            columns.push(origin_ts);
        }
        (None, Some(received_ts)) => {
            columns.push(received_ts);
        }
        (None, None) => {
            // when received_ts and original_ts are both none, add original_ts
            columns.push(ColumnConfig {
                name: ColumnConfig::ORIGINAL_TS.to_string(),
                r#type: Some(Ty::Timestamp),
                alias: Some("ts".to_string()),
                transform: None,
                is_primary_key: true,
            });
        }
    }

    Ok(columns)
}

fn parse_tags(header: &CsvHeader) -> Vec<TagConfig> {
    let mut tags = Vec::new();

    for col in header.get_columns() {
        if !col.is_tag {
            continue;
        }

        let tag_name = col.name.clone();
        let tag_type = col.tag_type.clone().unwrap();
        let tag_config = TagConfig {
            name: tag_name,
            r#type: tag_type,
        };
        tags.push(tag_config);
    }

    tags
}

fn parse_value_col(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<ColumnConfig> {
    let value_name = header
        .get_column("value_col")
        .and_then(|col| row.get(col.index))
        .map_or(Some("val".to_string()), |val| {
            if val.is_empty() {
                Some("val".to_string())
            } else {
                Some(val.to_string())
            }
        });

    let value_type = header
        .get_column("type")
        .and_then(|col| row.get(col.index))
        .and_then(|val| {
            if val.is_empty() {
                None
            } else {
                let val_type = IpcDataType::from_str(val)
                    .map(|val_type| val_type.ty())
                    .map_err(|_err| anyhow::anyhow!("invalid column data type: {}", val));
                Some(val_type)
            }
        })
        .transpose()?;

    let value_transform = header
        .get_column("value_transform")
        .and_then(|col| row.get(col.index))
        .and_then(|val| {
            if val.is_empty() {
                None
            } else {
                Some(val.to_string())
            }
        });

    match (value_name.as_ref(), value_transform.as_ref()) {
        (Some(value_name), Some(value_transform)) => {
            // 校验列名
            validate_table_column_name("value column name", value_name)?;
            // 校验表达式
            check_math_expression(value_name, value_transform).map_err(|e| {
                anyhow::anyhow!(
                    "invalid value_transform: {}, cause: {}",
                    value_transform,
                    e.to_string()
                )
            })?;
        }
        (Some(value_name), None) => {
            // 校验列名
            validate_table_column_name("value column name", value_name)?;
        }
        (None, _) => {
            panic!("value column name cannot be None");
        }
    }

    Ok(ColumnConfig {
        name: ColumnConfig::VALUE.to_string(),
        r#type: value_type,
        alias: value_name,
        transform: value_transform,
        is_primary_key: false,
    })
}

fn parse_quality_col(
    header: &CsvHeader,
    row: &StringRecord,
) -> anyhow::Result<Option<ColumnConfig>> {
    let col = header
        .get_column("quality_col")
        .and_then(|col| row.get(col.index));

    if col.is_none() {
        return Ok(None);
    }

    let quality_col = col.unwrap();
    let quality_col = if quality_col.is_empty() {
        "quality".to_string()
    } else {
        quality_col.to_string()
    };

    // todo!("check column name")
    // if quality.is_some() {
    //     let quality_column = quality.unwrap();
    //     let quality_name = quality_column.alias.as_ref().unwrap();
    //     validate_table_column_name("quality column name", quality_name)?;
    // }

    Ok(Some(ColumnConfig {
        name: ColumnConfig::QUALITY.to_string(),
        r#type: Some(Ty::Int),
        alias: Some(quality_col),
        transform: None,
        is_primary_key: false,
    }))
}

fn parse_received_ts_col(
    header: &CsvHeader,
    row: &StringRecord,
) -> anyhow::Result<Option<ColumnConfig>> {
    let rts_col = header
        .get_column("received_ts_col")
        .or(header.get_column("received_time_col"));
    if rts_col.is_none() {
        return Ok(None);
    }

    let col = rts_col.unwrap();
    let col_name = row.get(col.index).and_then(|v| {
        if v.is_empty() {
            None
        } else {
            Some(v.to_string())
        }
    });

    if let Some(col_name) = col_name {
        validate_table_column_name("received_ts column name", &col_name)?;

        let received_ts_transform = header
            .get_column("received_ts_transform")
            .and_then(|col| row.get(col.index))
            .and_then(|val| {
                if val.is_empty() {
                    None
                } else {
                    Some(val.to_string())
                }
            });

        if let Some(rts_transform) = received_ts_transform.as_ref() {
            // 校验表达式
            check_math_expression(&col_name, rts_transform).map_err(|e| {
                anyhow::anyhow!(
                    "invalid received_ts_transform: {}, cause: {}",
                    rts_transform,
                    e.to_string()
                )
            })?;
        }

        return Ok(Some(ColumnConfig {
            name: ColumnConfig::RECEIVED_TS.to_string(),
            r#type: Some(Ty::Timestamp),
            alias: Some(col_name),
            transform: received_ts_transform,
            is_primary_key: col.is_primary_key,
        }));
    }

    Ok(None)
}

fn parse_original_ts_col(
    header: &CsvHeader,
    row: &StringRecord,
) -> anyhow::Result<Option<ColumnConfig>> {
    let ts_col = header.get_column("ts_col");
    if ts_col.is_none() {
        return Ok(None);
    }

    let col = ts_col.unwrap();
    let col_name = row.get(col.index).and_then(|val| {
        if val.is_empty() {
            None
        } else {
            Some(val.to_string())
        }
    });

    if let Some(origin_ts_name) = col_name {
        validate_table_column_name("original_ts column name", &origin_ts_name)?;

        let origin_ts_transform = header
            .get_column("ts_transform")
            .and_then(|col| row.get(col.index))
            .and_then(|val| {
                if val.is_empty() {
                    None
                } else {
                    Some(val.to_string())
                }
            });

        if let Some(ts_transform) = origin_ts_transform.as_ref() {
            // 校验表达式
            check_math_expression(&origin_ts_name, ts_transform).map_err(|e| {
                anyhow::anyhow!(
                    "invalid original_ts_transform: {}, cause: {}",
                    ts_transform,
                    e.to_string()
                )
            })?;
        }

        return Ok(Some(ColumnConfig {
            name: ColumnConfig::ORIGINAL_TS.to_string(),
            r#type: Some(Ty::Timestamp),
            alias: Some(origin_ts_name),
            transform: origin_ts_transform,
            is_primary_key: col.is_primary_key,
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod table_config_tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_value_col() {
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["value_col", "value_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["value", "value + 1"]);
        let value_col = parse_value_col(&header, &row).unwrap();
        assert_eq!(value_col.alias.unwrap(), "value");
        assert_eq!(value_col.transform.unwrap(), "value + 1");

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["value_col", "value_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["", "value + 1"]);
        let value_col = parse_value_col(&header, &row);
        assert!(value_col.is_err());
        assert_eq!(
            value_col.unwrap_err().to_string(),
            "invalid value_transform: value + 1, cause: Variable not found: value"
        );

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["value_col", "value_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["", "val + 1"]);
        let value_col = parse_value_col(&header, &row).unwrap();
        assert_eq!(value_col.alias.unwrap(), "val");
        assert_eq!(value_col.transform.unwrap(), "val + 1");
    }

    #[tokio::test]
    async fn test_parse_original_ts_col() {
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["ts_col", "ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ts", "ts + 1"]);
        let ts_col = parse_original_ts_col(&header, &row).unwrap().unwrap();
        assert_eq!(ts_col.alias.unwrap(), "ts");
        assert_eq!(ts_col.transform.unwrap(), "ts + 1");

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["ts_col", "ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["", "ts + 1"]);
        let ts_col = parse_original_ts_col(&header, &row).unwrap();
        assert!(ts_col.is_none());

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["ts_col", "ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ts", "origin_ts + 1"]);
        let ts_col = parse_original_ts_col(&header, &row);
        assert!(ts_col.is_err());
        assert_eq!(
            ts_col.unwrap_err().to_string(),
            "invalid original_ts_transform: origin_ts + 1, cause: Variable not found: origin_ts"
        );
    }

    #[tokio::test]
    async fn test_parse_received_ts_col() {
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["received_ts_col", "received_ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["rts", "rts + 1"]);
        let received_ts_col = parse_received_ts_col(&header, &row).unwrap().unwrap();
        assert_eq!(received_ts_col.alias.unwrap(), "rts");
        assert_eq!(received_ts_col.transform.unwrap(), "rts + 1");

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["received_ts_col", "received_ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["", "rts + 1"]);
        let received_ts_col = parse_received_ts_col(&header, &row).unwrap();
        assert!(received_ts_col.is_none());

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["received_ts_col", "received_ts_transform"]),
        )
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["rts", "received_ts + 1"]);
        let received_ts_col = parse_received_ts_col(&header, &row);
        assert!(received_ts_col.is_err());
        assert_eq!(
            received_ts_col.unwrap_err().to_string(),
            "invalid received_ts_transform: received_ts + 1, cause: Variable not found: received_ts"
        );
    }
}

#[derive(Clone, Deserialize, Debug, Serialize, PartialEq)]
pub struct ColumnConfig {
    pub name: String, // original_ts / received_ts / value / quality
    pub r#type: Option<Ty>,
    pub alias: Option<String>,
    pub transform: Option<String>,
    pub is_primary_key: bool,
}

impl ColumnConfig {
    pub const ORIGINAL_TS: &'static str = "original_ts";
    pub const RECEIVED_TS: &'static str = "received_ts";
    pub const VALUE: &'static str = "value";
    pub const QUALITY: &'static str = "quality";
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TagConfig {
    pub name: String,
    pub r#type: IpcDataType,
}
