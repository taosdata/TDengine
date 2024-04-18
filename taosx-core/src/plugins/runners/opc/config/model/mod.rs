use std::collections::HashMap;
use std::str::FromStr;

use anyhow::bail;
use csv_async::StringRecord;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use taos::Ty;

use taosx_ipc::prelude::IpcDataType;

use crate::runners::opc::config::csv::header::CsvHeader;
use crate::runners::opc::{generate_tbname_from_pattern, OpcType};
use crate::utils::rhai_syntax_validator::check_math_expression;
use crate::utils::validate_table_column_name;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpcModelConfig {
    /// id, (code, stable, enabled)
    /// code for child table name, stable maybe none when use ui config, cause stable_prefix exists
    /// when stable is none stable_prefix will be enabled
    pub point_config_map: LinkedHashMap<String, PointConfig>,
    pub table_config_map: LinkedHashMap<String, TableConfig>,
}

impl OpcModelConfig {
    pub fn new() -> Self {
        OpcModelConfig {
            point_config_map: LinkedHashMap::new(),
            table_config_map: LinkedHashMap::new(),
        }
    }

    /// parse one row in csv file to a point config
    pub async fn append(
        &mut self,
        header: &CsvHeader,
        row: StringRecord,
        row_index: usize,
    ) -> anyhow::Result<()> {
        let point_id = parse_point_id(header, &row)?;

        // add point config
        let is_duplicated = self.point_config_map.insert(
            point_id.clone(),
            PointConfig::from_csv(&header, &row, row_index)?,
        );

        // check point_id duplicated
        if let Some(point_config) = is_duplicated {
            match header.get_opc_type() {
                OpcType::OPCUA => {
                    bail!("point_id: {} should be unique in one OPC DataIn Task, duplicated in CSV row: [{}, {}]", point_id,point_config.row_index,row_index);
                }
                OpcType::OPCDA => {
                    bail!("tag_name: {} should be unique in one OPC DataIn Task, duplicated in CSV row: [{}, {}]", point_id,point_config.row_index, row_index);
                }
                OpcType::FAKE => {
                    unimplemented!()
                }
            }
        }

        // add table config
        self.table_config_map
            .insert(point_id.clone(), TableConfig::from_csv(&header, &row)?);

        Ok(())
    }

    pub fn get_column_config_map_by_name(&self, col_name: &str) -> HashMap<String, ColumnConfig> {
        let mut transform_map = HashMap::new();

        for (point_id, table_config) in &self.table_config_map {
            let column_config = table_config.column_config(col_name);
            if column_config.is_none() {
                continue;
            }
            let column_config = column_config.unwrap().clone();
            transform_map.insert(point_id.clone(), column_config);
        }

        transform_map
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

fn parse_tbname(header: &CsvHeader, row: &csv_async::StringRecord) -> anyhow::Result<String> {
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

fn parse_type(
    header: &CsvHeader,
    row: &csv_async::StringRecord,
) -> anyhow::Result<Option<IpcDataType>> {
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
                bail!("invalid column data type: [{}]", val)
            } else {
                Ok(Some(value_type.unwrap()))
            }
        })
        .unwrap_or(Ok(None))
}

fn get_raw_type(header: &CsvHeader, row: &csv_async::StringRecord) -> Option<String> {
    header
        .get_column("type")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| match val.find("(") {
            Some(index) => val[..index].to_string(),
            None => val.to_string(),
        })
}

fn parse_stable(header: &CsvHeader, row: &csv_async::StringRecord) -> Option<String> {
    header
        .get_column("stable")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| {
            let val_type = get_raw_type(header, row);
            if val.contains("{type}") && val_type.is_none() {
                tracing::warn!("stable contains '{{type}}' but type is None, use None");
                return None;
            }

            let stable_name = if val_type.is_some() {
                Some(val.replace("{type}", &val_type.unwrap()))
            } else {
                Some(val.to_string())
            };
            stable_name
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
        let stable_prefix = if stable.is_none() {
            Some(String::from(DEFAULT_STABLE_PREFIX))
        } else {
            None
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
            v.parse::<i8>()
                .map_err(|_| anyhow::anyhow!("invalid enabled: {} in csv row, must be 0 or 1", v))
        })
        .transpose()?;
    Ok(enabled)
}

fn parse_columns(
    header: &CsvHeader,
    row: &csv_async::StringRecord,
) -> anyhow::Result<Vec<ColumnConfig>> {
    let mut columns = Vec::new();

    // value => value_col
    let value = parse_value_col(header, row);
    let value_name = value.alias.as_ref().unwrap();
    validate_table_column_name("column name", value_name)?;

    if value.transform.is_some() {
        // 校验表达式
        let value_transform = value.transform.as_ref().unwrap();
        check_math_expression(value_name, value_transform).map_err(|e| {
            anyhow::anyhow!(
                "invalid value_transform: {}, cause: {}",
                value_transform,
                e.to_string()
            )
        })?;
    }
    columns.push(value);

    // quality => quality_col
    let quality = parse_quality_col(header, row);
    if quality.is_some() {
        let quality_column = quality.unwrap();
        let quality_name = quality_column.alias.as_ref().unwrap();
        validate_table_column_name("quality column name", quality_name)?;
        columns.push(quality_column);
    }

    // received_ts => received_ts_col/received_time_col
    let received_ts = parse_received_ts_col(header, row);
    let original_ts = parse_original_ts_col(header, row);
    // when received_ts and original_ts are both none, add original_ts
    if received_ts.is_none() && original_ts.is_none() {
        columns.push(ColumnConfig {
            name: "original_ts".to_string(),
            r#type: Some(Ty::Timestamp),
            alias: Some("ts".to_string()),
            transform: None,
            is_primary_key: true,
        });
    }

    if received_ts.is_some() {
        let received_ts_column = received_ts.unwrap();
        let ts_name = received_ts_column.alias.as_ref().unwrap();
        validate_table_column_name("received_ts column name", ts_name)?;

        if received_ts_column.transform.is_some() {
            // 校验表达式
            let ts_transform = received_ts_column.transform.as_ref().unwrap();
            check_math_expression(ts_name, ts_transform).map_err(|e| {
                anyhow::anyhow!(
                    "invalid received_ts_transform: {}, cause: {}",
                    ts_transform,
                    e.to_string()
                )
            })?;
        }
        columns.push(received_ts_column);
    }

    if original_ts.is_some() {
        let original_ts_column = original_ts.unwrap();
        let ts_name = original_ts_column.alias.as_ref().unwrap();
        validate_table_column_name("original_ts column name", ts_name)?;

        if original_ts_column.transform.is_some() {
            // 校验表达式
            let ts_transform = original_ts_column.transform.as_ref().unwrap();
            check_math_expression(ts_name, ts_transform).map_err(|e| {
                anyhow::anyhow!(
                    "invalid original_ts_transform: {}, cause: {}",
                    ts_transform,
                    e.to_string()
                )
            })?;
        }
        columns.push(original_ts_column);
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

fn parse_value_col(header: &CsvHeader, row: &csv_async::StringRecord) -> ColumnConfig {
    let value_name = header
        .get_column("value_col")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| {
            if val.is_empty() {
                Some("val".to_string())
            } else {
                Some(val.to_string())
            }
        })
        .flatten();

    let value_transform = header
        .get_column("value_transform")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| {
            if val.is_empty() {
                None
            } else {
                Some(val.to_string())
            }
        })
        .flatten();

    ColumnConfig {
        name: ColumnConfig::VALUE.to_string(),
        r#type: None,
        alias: value_name,
        transform: value_transform,
        is_primary_key: false,
    }
}

fn parse_quality_col(header: &CsvHeader, row: &csv_async::StringRecord) -> Option<ColumnConfig> {
    let col = header
        .get_column("quality_col")
        .map(|col| row.get(col.index))
        .flatten();

    if col.is_none() {
        return None;
    }

    let quality_col = col.unwrap();
    let quality_col = if quality_col.is_empty() {
        "quality".to_string()
    } else {
        quality_col.to_string()
    };

    Some(ColumnConfig {
        name: ColumnConfig::QUALITY.to_string(),
        r#type: Some(Ty::Int),
        alias: Some(quality_col),
        transform: None,
        is_primary_key: false,
    })
}

fn parse_received_ts_col(
    header: &CsvHeader,
    row: &csv_async::StringRecord,
) -> Option<ColumnConfig> {
    let col = header
        .get_column("received_ts_col")
        .or(header.get_column("received_time_col"));
    if col.is_none() {
        return None;
    }

    let col = col.unwrap();
    let col_name = row
        .get(col.index)
        .map(|v| {
            if v.is_empty() {
                None
            } else {
                Some(v.to_string())
            }
        })
        .flatten();
    if col_name.is_none() {
        return None;
    }

    let received_ts_transform = header
        .get_column("received_ts_transform")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| {
            if val.is_empty() {
                None
            } else {
                Some(val.to_string())
            }
        })
        .flatten();

    Some(ColumnConfig {
        name: ColumnConfig::RECEIVED_TS.to_string(),
        r#type: Some(Ty::Timestamp),
        alias: col_name,
        transform: received_ts_transform,
        is_primary_key: col.is_primary_key,
    })
}

fn parse_original_ts_col(
    header: &CsvHeader,
    row: &csv_async::StringRecord,
) -> Option<ColumnConfig> {
    let col = header.get_column("ts_col");
    if col.is_none() {
        return None;
    }

    let col = col.unwrap();
    let col_name = row
        .get(col.index)
        .map(|v| {
            if v.is_empty() {
                None
            } else {
                Some(v.to_string())
            }
        })
        .flatten();
    if col_name.is_none() {
        return None;
    }

    let original_ts_transform = header
        .get_column("ts_transform")
        .map(|col| row.get(col.index))
        .flatten()
        .map(|val| {
            if val.is_empty() {
                None
            } else {
                Some(val.to_string())
            }
        })
        .flatten();

    Some(ColumnConfig {
        name: ColumnConfig::ORIGINAL_TS.to_string(),
        r#type: Some(Ty::Timestamp),
        alias: col_name,
        transform: original_ts_transform,
        is_primary_key: col.is_primary_key,
    })
}

#[derive(Clone, Deserialize, Debug, Serialize)]
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
