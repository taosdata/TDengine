use std::io::BufRead;

use anyhow::bail;
use csv_lib::ReaderBuilder;
use itertools::Itertools;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use taos::{AsyncQueryable, Dsn, Taos};

use crate::runners::opc::config::collect::CollectConfig;
use crate::runners::opc::config::connect::ConnectConfig;
use crate::runners::opc::config::csv::CsvParser;
use crate::runners::opc::config::model::{OpcModelConfig, PointConfig, TableConfig};
use crate::runners::opc::config::points::PointsConfig;
use crate::runners::opc::config::report::ReportConfig;
use crate::runners::opc::OpcType;

mod collect;
mod connect;
pub mod csv;
pub mod model;
pub mod points;
mod report;

#[derive(Debug, Serialize)]
pub struct OPCConfig {
    pub opc_type: OpcType,
    pub debug: bool,
    connect: ConnectConfig,
    pub report: ReportConfig,

    pub points: Option<PointsConfig>,
    collect: CollectConfig,

    #[serde(skip)]
    pub param_mapping: LinkedHashMap<String, PointConfig>,
    #[serde(skip)]
    pub opc_table_config: Option<TableConfig>,
}

impl OPCConfig {
    pub async fn from_dsn_collect_mode(
        dsn: &Dsn,
        ipc_port: u16,
        taos: &Taos,
        id: Option<i64>,
    ) -> anyhow::Result<Self> {
        if dsn.driver != "opc" && dsn.driver != "opcua" && dsn.driver != "opcda" {
            bail!("invalid opc driver");
        }

        let opc_type = OpcType::from_dsn(dsn)?;
        let debug = Self::parse_debug(dsn)?;
        let connect = ConnectConfig::from_dsn(dsn)?;
        let report = ReportConfig::from_dsn(dsn, ipc_port)?;

        let csv_config_file = Self::parse_csv_config_file(dsn);

        if csv_config_file.is_some() {
            let parser = CsvParser::from_dsn(dsn).await?;
            let table_to_drop = parser.get_tables_to_drop();

            for child_table_name in table_to_drop.iter() {
                let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                tracing::info!("drop sql: {drop_sql}");
                taos.exec(drop_sql).await.map_err(|err| {
                    anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                })?;
            }
        }

        Ok(Self {
            opc_type,
            debug,
            connect,
            report,
            points: None,
            collect: CollectConfig::from_dsn(dsn, id).await?,
            param_mapping: Self::build_param_mapping(dsn).await?,
            // opc_table_config: TableConfig::from_dsn(dsn).await?,
            opc_table_config: None,
        })
    }

    pub async fn from_dsn_point_mode(dsn: &Dsn) -> anyhow::Result<Self> {
        if dsn.driver != "opc" && dsn.driver != "opcua" && dsn.driver != "opcda" {
            bail!("invalid opc driver");
        }

        // keep_raw_data is not needed in point mode
        let mut dsn = dsn.clone();
        if dsn.params.contains_key("enable") {
            dsn.params.remove("enable");
        }
        if dsn.params.contains_key("keep_raw_data") {
            dsn.params.remove("keep_raw_data");
        }

        Ok(Self {
            opc_type: OpcType::from_dsn(&dsn)?,
            debug: Self::parse_debug(&dsn)?,
            connect: ConnectConfig::from_dsn(&dsn)?,
            points: None,
            collect: if dsn.get("csv_config_file").is_some() {
                CollectConfig::from_dsn(&dsn, None).await?
            } else {
                CollectConfig::new_empty()
            },
            report: ReportConfig::from_dsn(&dsn, 0)?,
            param_mapping: LinkedHashMap::new(),
            opc_table_config: None,
        })
    }

    pub async fn from_dsn_for_validate(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(OPCConfig {
            opc_type: OpcType::from_dsn(dsn)?,
            debug: Self::parse_debug(dsn)?,
            connect: ConnectConfig::from_dsn(dsn)?,
            points: None,
            collect: CollectConfig::new_empty(),
            report: ReportConfig::from_dsn(dsn, 0)?,
            param_mapping: LinkedHashMap::new(),
            opc_table_config: None,
        })
    }

    fn parse_debug(dsn: &Dsn) -> anyhow::Result<bool> {
        let debug = dsn.params.get("debug");
        if debug.is_some() {
            return Ok(debug.unwrap().parse::<bool>().unwrap_or(false));
        }

        Ok(dsn
            .params
            .get("log_level")
            .map(|v| match v.as_str() {
                "error" | "warn" | "info" => Ok(false),
                "debug" | "trace" => Ok(true),
                _ => Err(anyhow::anyhow!("invalid log_level: {}", v.to_string())),
            })
            .transpose()?
            .unwrap_or(false))
    }

    fn parse_csv_config_file(dsn: &Dsn) -> Option<String> {
        dsn.params.get("csv_config_file").map(|v| v.to_string())
    }

    async fn build_param_mapping(dsn: &Dsn) -> anyhow::Result<LinkedHashMap<String, PointConfig>> {
        let csv_config_file = Self::parse_csv_config_file(dsn);

        if csv_config_file.is_some() {
            let parser = CsvParser::from_dsn(dsn).await?;
            let point_config_map = parser.get_model_config().point_config_map;
            return Ok(point_config_map);
        }

        let opc_type = OpcType::from_dsn(dsn)?;

        let param_mapping = match opc_type {
            OpcType::OPCUA => {
                let mut param_mapping = LinkedHashMap::new();

                let ua_nodes =
                    get_string_vec_from_param_or_file_for_opc(&mut dsn.clone(), "ua.nodes")
                        .map_err(|s| anyhow::anyhow!("file parse error: {}", s))?;

                for i in 0..ua_nodes.len() {
                    let pair = ua_nodes[i].split("::").collect_vec();
                    if pair.len() != 2 {
                        let pair = pair.join("::");
                        bail!(
                            "failed to parse node: {}, cause: split result len is not 2",
                            pair
                        );
                    }
                    let tag = String::from(pair[0]);
                    let code = String::from(pair[1]);
                    param_mapping.insert(
                        tag,
                        PointConfig {
                            row_index: i + 1,
                            code,
                            stable: None,
                            tag_values: None,
                            value_type: None,
                        },
                    );
                }
                param_mapping
            }
            OpcType::OPCDA => {
                let mut param_mapping = LinkedHashMap::new();

                let node_vec =
                    get_string_vec_from_param_or_file_for_opc(&mut dsn.clone(), "da.tags")
                        .map_err(|s| anyhow::anyhow!("file parse error: {}", s))?;
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    if pair.len() != 2 {
                        let pair = pair.join("::");
                        bail!(
                            "node config error node config: {} split result len is not 2",
                            pair
                        );
                    }
                    let tag = String::from(pair[0]);
                    let code = String::from(pair[1]);
                    param_mapping.insert(
                        tag,
                        PointConfig {
                            row_index: i + 1,
                            code,
                            stable: None,
                            tag_values: None,
                            value_type: None,
                        },
                    );
                }
                param_mapping
            }
            _ => bail!("invalid opc type: {}", opc_type),
        };

        Ok(param_mapping)
    }

    pub fn parse_select_all_points(dsn: &Dsn) -> bool {
        dsn.params
            .get("select_all_points")
            .map(|v| v.parse::<bool>().ok().unwrap_or(true))
            .unwrap_or(false)
    }

    pub async fn with_table_config_map(
        &self,
        table_config_map: LinkedHashMap<String, TableConfig>,
    ) -> anyhow::Result<OpcModelConfig> {
        let id_code_map = self
            .param_mapping
            .iter()
            .map(|(id, code)| (id.clone(), code.clone()))
            .collect();
        let table_config = if self.opc_table_config.clone().is_some() {
            self.opc_table_config.clone().unwrap()
        } else {
            TableConfig::empty()
        };

        let c = OpcModelConfig {
            point_config_map: id_code_map,
            table_config,
            table_config_map,
        };
        Ok(c)
    }
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub enum AuthMethod {
    Anonymous,
    UserName,
    #[default]
    Certificate,
}

/*
/// return opc table config, node_config, tables_to_drop
// #[async_backtrace::framed]
pub async fn generate_config_from_csv(
    opc_type: &str,
    csv_config_file: &str,
) -> anyhow::Result<(OpcModelConfig, Vec<String>, Vec<String>)> {
    let files_or_strings = csv_config_file
        .split(",")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let mut id_code_map = LinkedHashMap::new(); // id, (code for sub-table name, stable)
    let mut tag_config = Vec::new();
    let mut column_config = Vec::new();
    let mut node_config_old = Vec::new();
    let mut tables_to_drop = Vec::new();
    let mut current_tag_names = Vec::new();
    let mut stable_prefix = None;

    for mut file in files_or_strings {
        tracing::info!(
            "current log: {}",
            std::env::current_dir().unwrap().to_str().unwrap()
        );

        let mut rdr;
        if !file.starts_with("@") {
            // TODO use mime instead
            let decoded = general_purpose::STANDARD.decode(&file)?;
            let mut temp_file = tempfile::NamedTempFile::new()?;
            let res = String::from_utf8(decoded)?;
            write!(temp_file, "{}", res)?;
            file = format!("@{}", temp_file.path().to_str().unwrap());
            rdr = csv_async::AsyncReader::from_reader(tokio::fs::File::open(&file[1..]).await?);
            temp_file.into_temp_path();
        } else {
            rdr = csv_async::AsyncReader::from_reader(tokio::fs::File::open(&file[1..]).await?);
        }
        // the header is comment, skip it
        let mut records = rdr.records();
        let header = records.next().await;

        if header.is_none() {
            tracing::warn!("file {file} should have 3 lines at least");
            bail!("Config file {file} should not be empty");
        }
        let header = header.unwrap()?;
        // header parse
        let mut column_names = Vec::new();

        for column_name in header.iter() {
            column_names.push(column_name.to_string());
            if column_name.starts_with("tag::") {
                // is tag config tag::type::name e.g. tag::varchar(123)::unit
                let split_tag = column_name.split("::").collect_vec();
                if split_tag.len() != 3 {
                    bail!(
                        "file {file} column {column_name} config error, pattern is tag::type::name"
                    );
                }
                let column_type =
                    IpcDataType::from_str(split_tag.get(1).unwrap()).map_err(|err| {
                        anyhow::Error::msg(format!("{err} should be a valid Data Type"))
                    })?;
                let tag_name = split_tag.get(2).unwrap().to_string();
                check_duplicated(&current_tag_names, None, &tag_name)?;
                current_tag_names.push(tag_name.clone());
                tag_config.push(TagConfig {
                    name: tag_name,
                    r#type: column_type,
                });
            }
        }

        let mut line = 3;
        let mut column_config_init = false;
        while let Some(record) = records.next().await {
            match record {
                Ok(record) => {
                    let mut record_map = LinkedHashMap::new();
                    let mut tag_values_map = HashMap::new();
                    // tags
                    for (index, column_name) in column_names.iter().enumerate() {
                        let data = record.get(index).unwrap();
                        if column_name.starts_with("tag::") {
                            tag_values_map.insert(
                                column_name
                                    .split("::")
                                    .collect_vec()
                                    .get(2)
                                    .unwrap()
                                    .to_string(),
                                data.to_string(),
                            );
                        } else {
                            record_map.insert(column_name.to_string(), data.to_string());
                        }
                    }

                    // point_id or tag_name
                    let point_id = record_map
                        .get("point_id")
                        .or(record_map.get("tag_name"))
                        .ok_or(anyhow::anyhow!("point_id or tag_name not found"))?
                        .clone();

                    let tb_name = record_map.get_mut("tbname").unwrap();
                    if tb_name.contains("{") {
                        // replace {tag_name} or {TagName} in tbname
                        *tb_name = generate_tbname_from_pattern(opc_type, tb_name, &point_id);
                    }

                    // stable
                    let stable = if let Some(stable_name) = record_map.get("stable") {
                        let val_type = record_map.get("type").unwrap();

                        let stable = if !val_type.is_empty() {
                            stable_name.to_string().replace("{type}", &val_type)
                        } else {
                            stable_name.to_string()
                        };

                        Some(stable)
                    } else {
                        None
                    };

                    if stable.is_none() && stable_prefix.is_none() {
                        stable_prefix = Some(String::from("opc"));
                    }

                    // enabled
                    let code = record_map.get("tbname").unwrap();
                    let enabled_column = record_map.get("enabled");
                    if enabled_column.is_some() {
                        let enabled = enabled_column.unwrap();
                        if enabled == "0" {
                            // warn: should delete subtable (stable_code)
                            tables_to_drop.push(format!("{code}"));
                            continue;
                        }
                    }

                    // type
                    let column_type = if let Some(ty) = record_map.get("type") {
                        if ty.is_empty() {
                            None
                        } else {
                            Some(
                                IpcDataType::from_str(ty)
                                    .map_err(|err| anyhow::Error::msg(err.clone()))?,
                            )
                        }
                    } else {
                        None
                    };

                    // value_col
                    let mut current_columns: Vec<String> = Vec::new();
                    if !column_config_init {
                        let value_column_name = record_map
                            .get("value_col")
                            .and_then(|v| if v.is_empty() { None } else { Some(v) })
                            .unwrap_or(&"val".to_string())
                            .clone();
                        check_duplicated(
                            &current_tag_names,
                            Some(&current_columns),
                            &value_column_name,
                        )
                        .with_context(|| format!("Config error with {value_column_name}"))?;
                        current_columns.push(value_column_name.clone());
                        column_config.push(ColumnConfig {
                            name: ColumnConfig::VALUE.to_string(),
                            r#type: None,
                            alias: Some(value_column_name.clone()),
                            transform: None,
                            is_primary_key: false,
                        });
                        let quality_col_name = record_map
                            .get("quality_col")
                            .unwrap_or(&"quality".to_string())
                            .clone();
                        check_duplicated(
                            &current_tag_names,
                            Some(&current_columns),
                            &quality_col_name,
                        )?;
                        current_columns.push(quality_col_name.clone());
                        column_config.push(ColumnConfig {
                            name: ColumnConfig::QUALITY.to_string(),
                            r#type: Some(Ty::Int),
                            alias: Some(quality_col_name.clone()),
                            transform: None,
                            is_primary_key: false,
                        });

                        let mut has_primary_key = false;
                        record_map.iter().for_each(|(col_name, col_data)| {
                            match col_name.as_str() {
                                "received_ts_col" | "received_time_col" => {
                                    current_columns.push(col_data.clone());

                                    has_primary_key = !has_primary_key;
                                    let col_config = ColumnConfig {
                                        name: ColumnConfig::RECEIVED_TS.to_string(),
                                        r#type: Some(Ty::Timestamp),
                                        alias: Some(col_data.clone()),
                                        transform: None,
                                        is_primary_key: has_primary_key,
                                    };
                                    column_config.push(col_config);
                                }
                                "ts_col" => {
                                    current_columns.push(col_data.clone());

                                    has_primary_key = !has_primary_key;
                                    let col_config = ColumnConfig {
                                        name: ColumnConfig::ORIGINAL_TS.to_string(),
                                        r#type: Some(Ty::Timestamp),
                                        alias: Some(col_data.clone()),
                                        transform: None,
                                        is_primary_key: has_primary_key,
                                    };
                                    column_config.push(col_config);
                                }
                                _ => {}
                            };
                        });

                        let rts_col_num = column_config
                            .iter()
                            .filter(|col| col.name == "received_ts")
                            .count();
                        let ts_col_num = column_config
                            .iter()
                            .filter(|col| col.name == "original_ts")
                            .count();
                        if rts_col_num > 1 {
                            bail!("received_ts column exists more than once in csv file");
                        }
                        if ts_col_num > 1 {
                            bail!("original_ts column exists more than once in csv file");
                        }

                        if rts_col_num == 0 && ts_col_num == 0 {
                            let col_config = ColumnConfig {
                                name: ColumnConfig::ORIGINAL_TS.to_string(),
                                r#type: Some(Ty::Timestamp),
                                alias: Some("ts".to_string()),
                                transform: None,
                                is_primary_key: true,
                            };
                            column_config.push(col_config);
                        }

                        column_config_init = true;
                    }

                    let tag_values = if tag_values_map.len() == 0 {
                        None
                    } else {
                        Some(tag_values_map)
                    };

                    let point_id = record_map
                        .get("point_id")
                        .or(record_map.get("tag_name"))
                        .ok_or(anyhow::anyhow!("point_id or tag_name not found"))?
                        .clone();

                    id_code_map.insert(
                        point_id.clone(),
                        PointConfig {
                            code: code.clone(),
                            stable,
                            tag_values,
                            value_type: column_type,
                        },
                    );
                    node_config_old.push(format!("{}::{}", point_id, code));
                }
                Err(_e) => {
                    tracing::warn!("line {} have different with other previous lines ", line)
                }
            }
            line += 1;
        }
    }

    let tag_configs = if tag_config.len() == 0 {
        None
    } else {
        Some(tag_config)
    };

    return Ok((
        OpcModelConfig {
            point_config_map: id_code_map,
            table_config: TableConfig {
                enabled: None,
                stable_prefix,
                column_configs: column_config,
                tag_configs,
            },
            table_config_map: LinkedHashMap::new(),
        },
        node_config_old,
        tables_to_drop,
    ));
}
*/

/*
fn check_duplicated(
    current_tags: &Vec<String>,
    current_columns: Option<&Vec<String>>,
    column_name: &String,
) -> anyhow::Result<()> {
    if current_tags.contains(column_name) {
        bail!("duplicated tag: {column_name}")
    }
    if current_columns.is_some() && current_columns.unwrap().contains(column_name) {
        bail!("duplicated column: {column_name}")
    }
    Ok(())
}
*/

pub fn get_string_vec_from_param_or_file_for_opc(
    dsn: &mut Dsn,
    key: &str,
) -> Result<Vec<String>, String> {
    if let Some(nodes) = dsn.remove(key) {
        let mut rdr = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(nodes.as_bytes());
        let header = rdr.headers().map_err(|err| err.to_string())?;
        let (files, mut node_config): (Vec<_>, Vec<_>) = header
            .into_iter()
            // .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .partition(|v| v.starts_with("@"));
        // dbg!(&files, &node_config);
        for file in files {
            tracing::info!(
                "current log: {}",
                std::env::current_dir().unwrap().to_str().unwrap()
            );
            let f = std::fs::File::open(&file[1..]);
            if f.is_err() {
                tracing::warn!(
                    "file: {} read error, cause: {}",
                    &file[1..],
                    f.err().unwrap()
                );
                continue;
                // return Err("file read error".to_string());
            }
            let buf = std::io::BufReader::new(f.unwrap());
            let mut file_data = buf.lines().collect_vec();
            // remove header
            if file_data.remove(0).is_err() {
                tracing::warn!("file: {} content length < 1", file);
            }

            node_config.extend(
                file_data
                    .iter()
                    .filter_map(|r| r.as_ref().ok())
                    .map(|s| s.replace(",", "::")),
            );
        }
        if node_config.len() == 0 {
            tracing::warn!("node config is empty");
            // return Err(format!("node config set but is empty: {nodes}"));
        }
        return Ok(node_config);
    }
    // tracing::warn!("node config is empty");
    return Err("Nodes not set".to_string());
}

#[cfg(test)]
mod tests {
    use taos::IntoDsn;

    use super::*;

    #[test]
    fn test_parse_special_nodes() {
        let mut dsn = format!(
            "opcua://?ua.nodes={}",
            r#""ns=3;s=Special_""!§$%&/()=?`´\+~*'#_-:.;,<>|@^°€µ{[]}::meter_3_Special_""!§$%&/()=?_´\+~*'#_-:_;,<>|@^°€µ{[]}","a::b""#
        ).into_dsn().unwrap();

        let config = get_string_vec_from_param_or_file_for_opc(&mut dsn, "ua.nodes").unwrap();
        assert_eq!(config[0], "ns=3;s=Special_\"!§$%");
    }
}
