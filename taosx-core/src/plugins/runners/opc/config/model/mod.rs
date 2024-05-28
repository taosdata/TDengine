use std::collections::HashMap;
use std::str::FromStr;

use anyhow::bail;
use csv_async::StringRecord;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use taos::Ty;

use taosx_ipc::prelude::IpcDataType;
use taosx_ipc::types::DataSet;

use crate::runners::opc::config::csv::header::CsvHeader;
use crate::runners::opc::config::OpcPointModelConfig;
use crate::runners::opc::{generate_stable_from_pattern, generate_tbname_from_pattern, OpcType};
use crate::utils::rhai_syntax_validator::check_math_expression;
use crate::utils::validate_table_column_name;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpcModelConfig {
    point_model_config: Option<OpcPointModelConfig>,
    pub point_config_map: LinkedHashMap<String, PointConfig>,
    pub table_config_map: LinkedHashMap<String, TableConfig>,
}

impl OpcModelConfig {
    pub fn new() -> Self {
        OpcModelConfig {
            point_model_config: None,
            point_config_map: LinkedHashMap::new(),
            table_config_map: LinkedHashMap::new(),
        }
    }

    pub fn set_point_model_config(&mut self, point_model_config: OpcPointModelConfig) {
        self.point_model_config = Some(point_model_config);
    }

    pub fn get_point_model_config(&self) -> Option<OpcPointModelConfig> {
        self.point_model_config.clone()
    }

    pub fn build_point_config(
        &self,
        index: usize,
        point_id: String,
        point_type: Option<IpcDataType>,
    ) -> anyhow::Result<PointConfig> {
        let point_model_config = self.point_model_config.clone().ok_or(anyhow::anyhow!(
            "super_table_expression and child_table_expression should be set before add points"
        ))?;

        let driver = point_model_config.opc_type.to_string();
        let stable_expr = point_model_config.stable_expression.clone();
        let tbname_expr = point_model_config.tbname_expression.clone();

        let tbname = generate_tbname_from_pattern(&driver, &tbname_expr, point_id.as_str());
        let stable = generate_stable_from_pattern(&stable_expr, &point_type);
        let point_config = PointConfig {
            row_index: index,
            code: tbname,
            stable: Some(stable),
            tag_values: None,
            value_type: point_type,
        };

        Ok(point_config)
    }

    pub fn build_table_config(
        &self,
        point_type: Option<IpcDataType>,
    ) -> anyhow::Result<TableConfig> {
        let point_model_config = self.point_model_config.clone().ok_or(anyhow::anyhow!(
            "super_table_expression and child_table_expression should be set before add points"
        ))?;

        let primary_key = point_model_config.primary_key.clone();
        let primary_key_alias = point_model_config.primary_key_alias.clone();
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
        match primary_key.as_str() {
            ColumnConfig::ORIGINAL_TS => {
                column_configs.push(ColumnConfig {
                    name: ColumnConfig::ORIGINAL_TS.to_string(),
                    r#type: Some(Ty::Timestamp),
                    alias: Some(primary_key_alias.clone()),
                    transform: None,
                    is_primary_key: true,
                });
            }
            ColumnConfig::RECEIVED_TS => {
                column_configs.push(ColumnConfig {
                    name: ColumnConfig::RECEIVED_TS.to_string(),
                    r#type: Some(Ty::Timestamp),
                    alias: Some(primary_key_alias.clone()),
                    transform: None,
                    is_primary_key: true,
                });
            }
            _ => {
                bail!("invalid primary key: {}", primary_key);
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

    pub fn add_points(&mut self, points: Vec<DataSet>) -> anyhow::Result<()> {
        let mut index: usize = 0;
        for p in points {
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
                self.build_point_config(index, point_id.clone(), value_type.clone())?;
            self.point_config_map.insert(point_id.clone(), point_config);

            // table_config
            let table_config = self.build_table_config(value_type.clone())?;
            self.table_config_map.insert(point_id.clone(), table_config);

            index += 1;
        }

        Ok(())
    }

    /// parse one row in csv file to a point config
    pub async fn add_csv_row(
        &mut self,
        header: &CsvHeader,
        row: StringRecord,
        row_index: usize,
    ) -> anyhow::Result<()> {
        let point_id = parse_point_id(header, &row)?;
        // check point_id duplicated
        match self.get_row_index(&point_id) {
            None => {}
            Some(index) => match header.get_opc_type() {
                OpcType::OPCUA => {
                    bail!("point_id: {} should be unique in one OPC DataIn Task, duplicated in CSV row: [{}, {}]", point_id,index,row_index);
                }
                OpcType::OPCDA => {
                    bail!("tag_name: {} should be unique in one OPC DataIn Task, duplicated in CSV row: [{}, {}]", point_id,index, row_index);
                }
                OpcType::FAKE => {
                    unimplemented!()
                }
            },
        }

        // parse point config and table config
        let point_config = PointConfig::from_csv(&header, &row, row_index)?;
        let table_config = TableConfig::from_csv(&header, &row)?;

        // check conflict
        match self.is_conflict(&point_id, &point_config, &table_config) {
            Ok(_) => {
                self.point_config_map.insert(point_id.clone(), point_config);
                self.table_config_map.insert(point_id.clone(), table_config);
            }
            Err(err) => {
                bail!(
                    "csv config conflict at row: {}, cause: {}",
                    row_index,
                    err.to_string()
                );
            }
        }

        Ok(())
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

    pub fn get_row_index(&self, point_id: &str) -> Option<usize> {
        self.point_config_map.get(point_id).map(|v| v.row_index)
    }

    fn is_conflict(
        &self,
        point_id: &String,
        point_config: &PointConfig,
        table_config: &TableConfig,
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
            .map(|v| v.alias.as_ref())
            .flatten();

        // 遍历 self.point_config_map 和 self.table_config_map，当 stable 和 tbname 时，value_col 应该不同，否则报错
        for (id, p_config) in &self.point_config_map {
            if let Some(t_config) = self.table_config_map.get(id) {
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

fn parse_point_id(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<String> {
    let opc_type = header.get_opc_type();

    let point_id_col = match opc_type {
        OpcType::OPCUA => header
            .get_column("point_id")
            .ok_or(anyhow::anyhow!("point_id not exist in csv header"))?,
        OpcType::OPCDA => header
            .get_column("tag_name")
            .or(header.get_column("TagName"))
            .ok_or(anyhow::anyhow!(
                "tag_name or TagName not exist in csv header"
            ))?,
        OpcType::FAKE => {
            bail!("fake opc type not supported");
        }
    };

    row.get(point_id_col.index)
        .map(|v| {
            if v.is_empty() {
                None
            } else {
                Some(v.to_string())
            }
        })
        .flatten()
        .ok_or(anyhow::anyhow!("point_id cannot be None in csv row"))
}

#[cfg(test)]
mod model_config_tests {
    use taos::IntoDsn;

    use super::*;

    #[test]
    fn test_add_points() {
        // given
        let dsn = format!(
            "opcua://?super_table_expression={}&child_table_expression={}",
            "opc_{type}", "t_{ns}_{id}"
        )
        .into_dsn()
        .unwrap();
        let points = vec![
            DataSet {
                id: "ns=3;i=1001".to_string(),
                name: Some("Constant".to_string()),
                category: None,
                r#type: Some("double".to_string()),
                options: None,
                format: None,
            },
            DataSet {
                id: "ns=3;i=1002".to_string(),
                name: Some("Counter".to_string()),
                category: None,
                r#type: Some("int".to_string()),
                options: None,
                format: None,
            },
        ];

        // when
        let mut config = OpcModelConfig::new();
        config.set_point_model_config(OpcPointModelConfig::from_dsn(&dsn).unwrap());
        config.add_points(points).unwrap();

        // then
        assert_eq!(config.point_config_map.len(), 2);
        config.point_config_map.get("ns=3;i=1001").map(|v| {
            assert_eq!(v.code, "t_3_1001");
            assert_eq!(v.stable, Some("opc_double".to_string()));
            assert_eq!(v.value_type, Some(IpcDataType::Float64));
        });
        config.point_config_map.get("ns=3;i=1002").map(|v| {
            assert_eq!(v.code, "t_3_1002");
            assert_eq!(v.stable, Some("opc_int".to_string()));
            assert_eq!(v.value_type, Some(IpcDataType::Int32));
        });

        assert_eq!(config.table_config_map.len(), 2);
        config.table_config_map.get("ns=3;i-1001").map(|v| {
            assert_eq!(v.enabled, Some(1));
            assert_eq!(v.stable_prefix, None);
            assert_eq!(v.column_configs.len(), 3);

            assert_eq!(v.column_configs[0].name, ColumnConfig::VALUE);
            assert_eq!(v.column_configs[0].r#type, None);
            assert_eq!(v.column_configs[0].alias, Some("val".to_string()));

            assert_eq!(v.column_configs[1].name, ColumnConfig::QUALITY);
            assert_eq!(v.column_configs[1].r#type, Some(Ty::Int));
            assert_eq!(v.column_configs[1].alias, None);

            assert_eq!(v.column_configs[2].name, ColumnConfig::ORIGINAL_TS);
            assert_eq!(v.column_configs[2].r#type, Some(Ty::Timestamp));
            assert_eq!(v.column_configs[2].alias, Some("ts".to_string()));
        });
    }

    #[tokio::test]
    async fn test_add_csv_row() {
        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable", "tbname", "value_col", "type"]),
        )
        .await
        .unwrap();
        let mut model_config = OpcModelConfig::new();
        let first_line = StringRecord::from(vec!["ns=3;i=1001", "stb1", "tb1", "val", "double"]);
        let second_line = StringRecord::from(vec!["ns=3;i=1002", "stb1", "tb1", "val", "int"]);

        let result = model_config.add_csv_row(&header, first_line, 1).await;
        assert!(result.is_ok());

        let result = model_config.add_csv_row(&header, second_line, 2).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "csv config conflict at row: 2, cause: point_id: ns=3;i=1001 and point_id: ns=3;i=1002 have same stable: stb1 and tbname: tb1, value_col should be different"
        );
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
        let code = parse_tbname(header, row)?;
        let value_type = parse_type(header, row)?;
        let stable = parse_stable(header, row);
        let tag_values = parse_tag_values(header, row);
        if stable.is_some() {
            validate_table_column_name("stable name", stable.as_ref().unwrap())?;
        }

        // 遍历tag_values，校验tag_values中的tag_name是否合法
        if tag_values.is_some() {
            for (tag_name, _) in tag_values.as_ref().unwrap() {
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

fn parse_tbname(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<String> {
    let point_id = parse_point_id(header, &row)?;

    let column = header
        .get_column("tbname")
        .ok_or(anyhow::anyhow!("tbname not exist in csv header"))?;

    let value = row
        .get(column.index)
        .ok_or(anyhow::anyhow!("tbname not exist in csv row"))?;

    if value.is_empty() {
        bail!("tbname cannot be empty");
    }

    let tbname = if value.contains("{") {
        // replace {tag_name} or {TagName} in tbname
        let opc_type = header.get_opc_type();
        generate_tbname_from_pattern(opc_type.to_string().as_str(), value, &point_id)
    } else {
        value.to_string()
    };
    validate_table_column_name("table name", &tbname)?;

    match tbname.is_empty() {
        true => bail!("tbname cannot be empty"),
        false => Ok(tbname),
    }
}

fn parse_type(header: &CsvHeader, row: &StringRecord) -> anyhow::Result<Option<IpcDataType>> {
    header
        .get_column("type")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| {
            if val.is_empty() {
                return Ok(None);
            }
            let value_type = IpcDataType::from_str(val);
            if value_type.is_err() {
                bail!("invalid column data type: {}", val)
            } else {
                Ok(Some(value_type.unwrap()))
            }
        })
        .unwrap_or(Ok(None))
}

fn parse_raw_type(header: &CsvHeader, row: &StringRecord) -> Option<String> {
    header
        .get_column("type")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| {
            if val.is_empty() {
                return None;
            }
            match val.find("(") {
                Some(index) => Some(val[..index].to_string().replace(" ", "_")),
                None => Some(val.replace(" ", "_")),
            }
        })
        .flatten()
}

fn parse_stable(header: &CsvHeader, row: &StringRecord) -> Option<String> {
    header
        .get_column("stable")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| {
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
        .flatten()
}

/// example:
///      tag::VARCHAR(200)::name
///      入库温度
/// tag_value map:
///      name => 入库温度
fn parse_tag_values(
    header: &CsvHeader,
    row: &csv_async::StringRecord,
) -> Option<HashMap<String, String>> {
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
        .await
        .unwrap();
        let row = StringRecord::from(vec!["point1", "stable1"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("stable1".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .await
        .unwrap();
        let row = StringRecord::from(vec!["point1", ""]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, None);

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable"]),
        )
        .await
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "meters_{type}"]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("meters_{type}".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable", "type"]),
        )
        .await
        .unwrap();
        let row = StringRecord::from(vec!["ns=3;i=1001", "meters_{type}", ""]);
        let stable = parse_stable(&header, &row);
        assert_eq!(stable, Some("meters_{type}".to_string()));

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &StringRecord::from(vec!["point_id", "stable", "type"]),
        )
        .await
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

const DEFAULT_STABLE_PREFIX: &'static str = "opc";

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
        let enabled = parse_enabled(header, row)?;
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

fn parse_enabled(header: &CsvHeader, row: &csv_async::StringRecord) -> anyhow::Result<Option<i8>> {
    let enabled = header
        .get_column("enabled")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| if val.is_empty() { None } else { Some(val) })
        .flatten()
        .map(|v| {
            if v != "0" && v != "1" {
                return Err(anyhow::anyhow!(
                    "invalid enabled: {} in csv row, must be 0 or 1",
                    v
                ));
            }
            v.parse::<i8>()
                .map_err(|_| anyhow::anyhow!("invalid enabled: {} in csv row, must be 0 or 1", v))
        })
        .transpose()?;
    Ok(enabled)
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
        .map(|col| row.get(col.index))
        .flatten();

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

    return Ok(None);
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

    return Ok(None);
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
        .await
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["value", "value + 1"]);
        let value_col = parse_value_col(&header, &row).unwrap();
        assert_eq!(value_col.alias.unwrap(), "value");
        assert_eq!(value_col.transform.unwrap(), "value + 1");

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["value_col", "value_transform"]),
        )
        .await
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
        .await
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
        .await
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["ts", "ts + 1"]);
        let ts_col = parse_original_ts_col(&header, &row).unwrap().unwrap();
        assert_eq!(ts_col.alias.unwrap(), "ts");
        assert_eq!(ts_col.transform.unwrap(), "ts + 1");

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["ts_col", "ts_transform"]),
        )
        .await
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["", "ts + 1"]);
        let ts_col = parse_original_ts_col(&header, &row).unwrap();
        assert!(ts_col.is_none());

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["ts_col", "ts_transform"]),
        )
        .await
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
        .await
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["rts", "rts + 1"]);
        let received_ts_col = parse_received_ts_col(&header, &row).unwrap().unwrap();
        assert_eq!(received_ts_col.alias.unwrap(), "rts");
        assert_eq!(received_ts_col.transform.unwrap(), "rts + 1");

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["received_ts_col", "received_ts_transform"]),
        )
        .await
        .unwrap();
        let row = csv_async::StringRecord::from(vec!["", "rts + 1"]);
        let received_ts_col = parse_received_ts_col(&header, &row).unwrap();
        assert!(received_ts_col.is_none());

        let header = CsvHeader::try_new(
            OpcType::OPCUA,
            &csv_async::StringRecord::from(vec!["received_ts_col", "received_ts_transform"]),
        )
        .await
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
