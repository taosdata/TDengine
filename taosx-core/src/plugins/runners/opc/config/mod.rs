use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose;
use csv_lib::ReaderBuilder;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sha2::digest::typenum::private::Trim;
use taos::{AsyncQueryable, Dsn, Taos, Ty};
use tokio_stream::StreamExt;

use taosx_ipc::prelude::IpcDataType;

use crate::runners::opc::config::collect::CollectConfig;
use crate::runners::opc::config::connect::ConnectConfig;
use crate::runners::opc::config::points::PointsConfig;
use crate::runners::opc::config::report::ReportConfig;
use crate::runners::opc::config::table::{ColumnConfig, TableConfig, TagConfig};
use crate::runners::opc::generate_tbname_from_pattern;
use crate::runners::opc::opc_type::OpcType;

mod connect;
mod collect;
mod report;
pub mod table;
mod points;

#[derive(Debug, Serialize)]
pub struct OPCConfig {
    pub opc_type: OpcType,
    pub debug: bool,
    connect: ConnectConfig,
    pub points: Option<PointsConfig>,
    collect: CollectConfig,
    pub report: ReportConfig,

    #[serde(skip)]
    pub param_mapping: HashMap<String, PointConfig>,
    #[serde(skip)]
    pub opc_table_config: Option<TableConfig>,
}

impl OPCConfig {
    pub async fn from_dsn_collect_mode(dsn: &Dsn, ipc_port: u16, taos: &Taos) -> anyhow::Result<Self> {
        if dsn.driver != "opc" && dsn.driver != "opcua" && dsn.driver != "opcda" {
            anyhow::bail!("invalid opc driver");
        }

        let config = Self {
            opc_type: OpcType::from_dsn(dsn)?,
            debug: Self::parse_debug(dsn)?,
            connect: ConnectConfig::from_dsn(dsn)?,
            points: None,
            collect: CollectConfig::from_dsn(dsn)?,
            report: ReportConfig::from_dsn(dsn, ipc_port)?,
            param_mapping: Self::build_param_mapping(dsn)?,     // TODO
            opc_table_config: Some(TableConfig::from_dsn(dsn)?),    // TODO
        };

        let csv_config_file = Self::parse_csv_config_file(dsn);
        if csv_config_file.is_some() {
            let table_to_drop = generate_opcconfig_from_csv(
                "opcua",
                csv_config_file.clone().unwrap().as_str(),
            )
                .await
                .map(|(a, b, c)| c)
                .map_err(|err| {
                    anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                })?;

            for child_table_name in table_to_drop.iter() {
                let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                tracing::info!("drop sql: {drop_sql}");
                taos.exec(drop_sql).await.map_err(|err| {
                    anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                })?;
            }
        }

        Ok(config)
    }

    fn build_param_mapping(dsn: &Dsn) -> anyhow::Result<HashMap<String, PointConfig>> {
        let param_mapping = HashMap::new();

        Ok(param_mapping)
    }
    pub async fn from_dsn_point_mode(dsn: &Dsn) -> anyhow::Result<Self> {
        if dsn.driver != "opc" && dsn.driver != "opcua" && dsn.driver != "opcda" {
            anyhow::bail!("invalid opc driver");
        }

        Ok(Self {
            opc_type: OpcType::from_dsn(dsn)?,
            debug: Self::parse_debug(dsn)?,
            connect: ConnectConfig::from_dsn(dsn)?,
            points: None,
            collect: CollectConfig::from_dsn(dsn)?,
            report: ReportConfig::from_dsn(dsn, 0)?,
            param_mapping: HashMap::new(),
            opc_table_config: None,
        })
    }

/*
    pub(crate) async fn new(mut dsn: Dsn, ipc_port: u16, config_mode: OPCConfigMode, taos: Option<&Taos>) -> anyhow::Result<Self> {
        if dsn.driver != "opc" && dsn.driver != "opcua" && dsn.driver != "opcda" {
            anyhow::bail!("invalid opc driver");
        }

        let collect;
        let mut param_mapping = HashMap::new();

        // let csv_config_file = dsn.remove("csv_config_file");
        let csv_config_file = Self::parse_csv_config_file(&dsn);

        let mut opc_table_config = None;

        let select_all_points = Self::parse_select_all_points(&dsn);

        match dsn.protocol.as_deref() {
            Some("ua") => {
                let node_vec: Vec<String> = if let OPCConfigMode::Points = config_mode {
                    vec![]
                } else if csv_config_file.is_some() {
                    let res = generate_opcconfig_from_csv(
                        "opcua",
                        csv_config_file.clone().unwrap().as_str(),
                    ).await.map_err(|err| anyhow::anyhow!("csv_config_file config error: {}", err.to_string()))?;
                    opc_table_config = Some(res.0);
                    for child_table_name in res.2.iter() {
                        let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                        tracing::info!("drop sql: {drop_sql}");
                        taos.unwrap().exec(drop_sql).await.map_err(|err| {
                            anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                        })?;
                    }
                    res.1
                } else if select_all_points {
                    // TODO: all points returns empty.
                    // warn!("select_all_points is not implemented");
                    Vec::new()
                } else {
                    get_string_vec_from_param_or_file_for_opc(&mut dsn, "ua.nodes")
                        .map_err(|s| anyhow::anyhow!("file parse error: {}", s))?
                };

                let mut ua_node_config_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    if pair.len() != 2 {
                        let pair = pair.join("::");
                        anyhow::bail!("node config error node config: {} split result len is not 2", pair);
                    }
                    let id = String::from(pair[0]);
                    let code = String::from(pair[1]);
                    let ua_node_config = UANodeConfig { id: id.clone() };
                    if csv_config_file.is_none() {
                        param_mapping.insert(
                            id,
                            PointConfig {
                                code,
                                stable: None,
                                tag_values: None,
                                value_type: None,
                            },
                        );
                    }
                    ua_node_config_vec.push(ua_node_config);
                }

                let collect_mode = dsn.remove("collect_mode").unwrap_or("observe".to_string());
                let collect_ua_config = UaCollectConfig {
                    collect_mode: collect_mode
                        .parse::<CollectMode>()
                        .map_err(|err|
                            anyhow::anyhow!("collect_mode config error: {}", err.to_string())
                        )?,
                    nodes: ua_node_config_vec,
                };
                collect = CollectConfig {
                    interval,
                    limit,
                    ua: Some(collect_ua_config),
                    da: None,
                    dump: dump_config,
                };
            }
            Some("da") => {
                let node_vec: Vec<String> = if let OPCConfigMode::Points = config_mode {
                    vec![]
                } else if csv_config_file.is_some() {
                    let res = generate_opcconfig_from_csv(
                        "opcda",
                        csv_config_file.clone().unwrap().as_str(),
                    )
                        .await
                        .map_err(|err|
                            anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                        )?;
                    opc_table_config = Some(res.0);
                    for child_table_name in res.2.iter() {
                        let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                        tracing::info!("drop sql: {drop_sql}");
                        taos.unwrap().exec(drop_sql).await.map_err(|err| {
                            anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                        })?;
                    }
                    res.1
                } else {
                    get_string_vec_from_param_or_file_for_opc(&mut dsn, "da.tags")
                        .map_err(|s|
                            anyhow::anyhow!("file parse error: {}", s)
                        )?
                };

                let mut da_nodes_vec = Vec::new();
                for i in 0..node_vec.len() {
                    let pair = node_vec[i].split("::").collect_vec();
                    if pair.len() != 2 {
                        let pair = pair.join("::");
                        anyhow::bail!("node config error node config: {} split result len is not 2", pair);
                    }
                    let tag = String::from(pair[0]);
                    let code = String::from(pair[1]);
                    da_nodes_vec.push(DaNodeConfig { tag: tag.clone() });
                    if csv_config_file.is_none() {
                        param_mapping.insert(
                            tag,
                            PointConfig {
                                code,
                                stable: None,
                                tag_values: None,
                                value_type: None,
                            },
                        );
                    }
                }
                collect = CollectConfig {
                    interval,
                    limit,
                    ua: None,
                    da: Some(DaCollectConfig { tags: da_nodes_vec }),
                    dump: dump_config,
                }
            }
            _ => {
                panic!()
                // bail!("opc config has wrong protocol");
            }
        }

        let table_config: Option<TableConfig>;
        if matches!(config_mode, OPCConfigMode::Points) {
            table_config = None;
        } else {
            if opc_table_config.is_none() {
                if select_all_points {
                    table_config = None;
                } else {
                    let config = dsn.remove("opc_table_config");
                    if config.is_none() {
                        anyhow::bail!("opc_table_config config error: should config opc_table_config or use csv config file");
                    }
                    table_config =
                        Some(serde_json::from_str(config.unwrap().as_str()).map_err(|v| {
                            anyhow::anyhow!("Parse param error from {} while parsing parameter opc_table_config", v.to_string())
                        })?);
                }
            } else {
                let opc_table_config = opc_table_config.unwrap();
                table_config = Some(opc_table_config.table_config.clone());
                param_mapping = opc_table_config.id_code_map.clone();
            }
        }

        Ok(OPCConfig {
            opc_type: OpcType::from_dsn(&dsn)?,
            debug: Self::parse_debug(&dsn)?,
            points: None,
            connect: ConnectConfig::from_dsn(&dsn)?,
            collect,
            report: ReportConfig::from_dsn(&dsn, ipc_port)?,
            param_mapping,
            opc_table_config: table_config,
        })
    }
*/
    fn parse_debug(dsn: &Dsn) -> anyhow::Result<bool> {
        Ok(dsn.params
            .get("debug")
            .map(|v| {
                v.parse::<bool>().map_err(|err| {
                    anyhow::anyhow!("parse debug failed, cause: {}", err.to_string())
                })
            })
            .transpose()?
            .unwrap_or(false)
        )
    }

    fn parse_csv_config_file(dsn: &Dsn) -> Option<String> {
        dsn.params
            .get("csv_config_file")
            .map(|v| v.to_string())
    }

    fn parse_select_all_points(dsn: &Dsn) -> bool {
        dsn.params
            .get("select_all_points")
            .map(|v| v.parse::<bool>().ok().unwrap_or(true))
            .unwrap_or(false)
    }

    pub async fn parse_tables_with(&self) -> anyhow::Result<OpcTableConfig> {
        let id_code_map = self
            .param_mapping
            .iter()
            .map(|(id, code)| (id.clone(), code.clone()))
            .collect();
        let c = OpcTableConfig {
            id_code_map,
            table_config: self.opc_table_config.clone().unwrap(),
        };
        Ok(c)
    }
}


#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
enum AuthMethod {
    Anonymous,
    UserName,
    #[default]
    Certificate,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpcTableConfig {
    /// id, (code, stable, enabled)
    /// code for child table name, stable maybe none when use ui config, casue stabel_prefix exists
    /// when stable is none stable_prefix will be enabled
    pub id_code_map: HashMap<String, PointConfig>,
    pub table_config: TableConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PointConfig {
    pub code: String,
    pub stable: Option<String>,
    pub tag_values: Option<HashMap<String, String>>,
    pub value_type: Option<IpcDataType>,
}

/// OPC connector mode
pub enum OPCConfigMode {
    /// just get points
    Points,
    /// collect point data
    Collect,
}

const CSV_CONFIG_COLUMNS: [&str; 2] = ["point_id", "tbname"];

/// return opctableconfig, node_config, tables_to_drop
pub async fn generate_opcconfig_from_csv(ty: &str, csv_config_file: &str) -> anyhow::Result<(OpcTableConfig, Vec<String>, Vec<String>)> {
    let files_or_strings = csv_config_file
        .split(",")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let mut id_code_map = HashMap::new(); // id, (code for sub-table name, stable)
    let mut tag_config = Vec::new();
    let mut column_config = Vec::new();
    let mut node_config_old = Vec::new();
    let mut tables_to_drop = Vec::new();
    let mut current_tag_names = Vec::new();
    let mut stable_prefix = None;
    for mut file in files_or_strings {
        tracing::info!("current log: {}",std::env::current_dir().unwrap().to_str().unwrap());

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
        // let mut
        let mut records = rdr.records();
        let header = records.next().await;
        // skip first line(desc)
        if header.is_none() {
            tracing::warn!("file {file} should have 2 lines at least");
            continue;
        }
        let header = header.unwrap()?;
        // header parse
        let mut column_map = HashMap::new();
        let mut column = 0;
        let temp_column = CSV_CONFIG_COLUMNS
            .iter()
            .map(|s| s.to_string())
            .collect_vec()
            .clone();
        let mut column_set: HashSet<&String> = HashSet::from_iter(temp_column.iter());
        for column_name in header.iter() {
            column_map.insert(column, column_name);
            if column_name.starts_with("tag") {
                // is tag config tag::type::name e.g. tag::varchar(123)::unit
                let split_tag = column_name.split("::").collect_vec();
                if split_tag.len() != 3 {
                    anyhow::bail!(
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
                    column_name: tag_name,
                    column_type,
                });
            }
            column += 1;
            column_set.remove(&column_name.to_string());
        }
        if column_set.len() != 0 {
            anyhow::bail!(
                "csv config miss column: {}",
                column_set.iter().next().unwrap()
            );
        }
        let mut line = 3;
        let mut column_config_init = false;
        while let Some(record) = records.next().await {
            match record {
                Ok(record) => {
                    let mut record_map = HashMap::new(); // column_name, column_data
                    let mut tag_values_map = HashMap::new();
                    for (index, column_name) in column_map.iter() {
                        let data = record.get(index.clone()).unwrap();
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

                    let point_id = record_map.get_mut("point_id").unwrap();
                    let pointid = point_id.clone();
                    let tb_name = record_map.get_mut("tbname").unwrap();
                    if tb_name.contains("{") {
                        // maybe should use pattern match?
                        *tb_name = generate_tbname_from_pattern(ty, tb_name, &pointid);
                    }
                    let point_id = record_map.get("point_id").unwrap();
                    let stable = if let Some(stable_name) = record_map.get("stable") {
                        Some(stable_name.clone())
                    } else {
                        if stable_prefix.is_none() {
                            stable_prefix = Some(String::from("opc"));
                        }
                        None
                    };
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
                    let column_type = if let Some(ty) = record_map.get("type") {
                        Some(
                            IpcDataType::from_str(ty)
                                .map_err(|err| anyhow::Error::msg(err.clone()))?,
                        )
                    } else {
                        None
                    };
                    let mut current_columns = Vec::new();
                    if !column_config_init {
                        let value_column_name = record_map
                            .get("value_col")
                            .unwrap_or(&"val".to_string())
                            .clone();
                        check_duplicated(
                            &current_tag_names,
                            Some(&current_columns),
                            &value_column_name,
                        )?;
                        current_columns.push(value_column_name.clone());
                        column_config.push(ColumnConfig {
                            column_name: "value".to_string(),
                            column_type: None,
                            column_alias: Some(value_column_name.clone()),
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
                            column_name: "quality".to_string(),
                            column_type: Some(Ty::Int),
                            column_alias: Some(quality_col_name.clone()),
                            is_primary_key: false,
                        });
                        let received_ts_col = record_map
                            .get("received_ts_col")
                            .or(record_map.get("received_time_col"));
                        let mut has_primary_key = false;
                        if received_ts_col.is_some() {
                            let received_ts_col_name = record_map
                                .get("received_ts_col")
                                .or(record_map.get("received_time_col"))
                                .unwrap_or(&"received_ts".to_string())
                                .clone();
                            check_duplicated(
                                &current_tag_names,
                                Some(&current_columns),
                                &received_ts_col_name,
                            )?;
                            current_columns.push(received_ts_col_name.clone());
                            has_primary_key = true;
                            column_config.push(ColumnConfig {
                                column_name: "received_ts".to_string(),
                                column_type: Some(Ty::Timestamp),
                                column_alias: Some(received_ts_col_name),
                                is_primary_key: has_primary_key,
                            });
                        }
                        let ts_col_name = record_map
                            .get("ts_col")
                            .unwrap_or(&"ts".to_string())
                            .clone();
                        check_duplicated(&current_tag_names, Some(&current_columns), &ts_col_name)?;
                        current_columns.push(ts_col_name.clone());
                        column_config.push(ColumnConfig {
                            column_name: "original_ts".to_string(),
                            column_type: Some(Ty::Timestamp),
                            column_alias: Some(ts_col_name),
                            is_primary_key: !has_primary_key,
                        });
                        column_config_init = true;
                    }

                    let tag_values = if tag_values_map.len() == 0 {
                        None
                    } else {
                        Some(tag_values_map)
                    };
                    id_code_map.insert(
                        point_id.clone(),
                        PointConfig {
                            code: code.clone(),
                            stable: stable,
                            tag_values,
                            value_type: column_type,
                        },
                    );
                    node_config_old.push(format!("{point_id}::{code}"))
                }
                Err(_e) => tracing::warn!("line {line} have different with other previous lines ",),
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
        OpcTableConfig {
            id_code_map,
            table_config: TableConfig {
                stable_prefix,
                column_configs: column_config,
                tag_configs,
            },
        },
        node_config_old,
        tables_to_drop,
    ));
}

fn check_duplicated(current_tags: &Vec<String>, current_columns: Option<&Vec<String>>, column_name: &String) -> anyhow::Result<()> {
    if current_tags.contains(column_name) {
        anyhow::bail!("duplicated column or tag: {column_name}")
    }
    if current_columns.is_some() && current_columns.unwrap().contains(column_name) {
        anyhow::bail!("duplicated column or tag: {column_name}")
    }
    Ok(())
}

pub fn get_string_vec_from_param_or_file_for_opc(dsn: &mut Dsn, key: &str) -> Result<Vec<String>, String> {
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
        return Result::Ok(node_config);
    }
    // tracing::warn!("node config is empty");
    return Err("Nodes not set".to_string());
}

#[cfg(test)]
mod tests {
    //     #[tokio::test]
//     async fn test_opc_config_to_toml() {
//         let mut map = HashMap::new();
//         map.insert(
//             String::from("123"),
//             PointConfig {
//                 code: "567".to_string(),
//                 stable: None,
//                 tag_values: None,
//                 value_type: None,
//             },
//         );
//         let mut column_configs = Vec::new();
//         let column_config = ColumnConfig {
//             column_name: String::from("received_time"),
//             column_type: Some(Ty::Timestamp),
//             column_alias: Some("ts".to_string()),
//             is_primary_key: true,
//         };
//         column_configs.push(column_config);
//         let column_config = ColumnConfig {
//             column_name: String::from("original_time"),
//             column_type: Some(Ty::Timestamp),
//             column_alias: None,
//             is_primary_key: false,
//         };
//         column_configs.push(column_config);
//         let column_config = ColumnConfig {
//             column_name: String::from("value"),
//             column_type: Some(Ty::Timestamp),
//             column_alias: None,
//             is_primary_key: true,
//         };
//         column_configs.push(column_config);
//         let opc_table_config = TableConfig {
//             stable_prefix: Some("meters".to_string()),
//             column_configs,
//             tag_configs: None,
//         };
//         let config = OPCConfig {
//             opc_type: OpcType::OPCUA,
//             debug: true,
//             points: Some(PointsConfig {
//                 limit: 32,
//                 regex: Some(String::from("123")),
//             }),
//             // use_received_time: true,
//             connect: ConnectConfig {
//                 ua: Some(UaConnectConfig {
//                     endpoint: String::from("endpoint.123"),
//                     connect_timeout: 10,
//                     request_timeout: 20,
//                     security_policy: String::from("None"),
//                     security_mode: String::from("None"),
//                     certificate: None,
//                     private_key: None,
//                     auth_method: AuthMethod::Anonymous,
//                     username: None,
//                     password: None,
//                 }),
//                 da: Some(DaConnectConfig {
//                     server: String::from("server.server"),
//                     nodes: vec![String::from("localhost")],
//                 }),
//             },
//             collect: CollectConfig {
//                 interval: Some(10),
//                 limit: Some(10),
//                 ua: Some(UaCollectConfig {
//                     collect_mode: CollectMode::OBSERVE,
//                     nodes: vec![UANodeConfig {
//                         id: String::from("1"),
//                         // value_type: String::from("DOUBLE"),
//                     }],
//                 }),
//                 da: Some(DaCollectConfig {
//                     tags: vec![DaNodeConfig {
//                         tag: String::from("123"),
//                         // value_type: String::from("VARCHAR"),
//                     }],
//                 }),
//                 dump: Some(DumpConfig {
//                     enable: true,
//                     path: Some("/usr/loacl/taosx/".to_string()),
//                     keep: Some(10 as usize),
//                 }),
//             },
//             report: ReportConfig {
//                 remote: String::from("remote.remote"),
//                 concurrent: Some(10),
//                 batch_size: None,
//                 batch_timeout: Some(100),
//             },
//             param_mapping: map,
//             // table_info: HashMap::new(),
//             opc_table_config: Some(opc_table_config),
//         };
//         let toml = toml::to_string(&config).unwrap();
//         assert_eq!(
//             r#"opc_type = "opcua"
// debug = true
//
// [connect.ua]
// endpoint = "endpoint.123"
// connect_timeout = 10
// request_timeout = 20
// security_policy = "None"
// security_mode = "None"
// auth_method = "Anonymous"
//
// [connect.da]
// server = "server.server"
// nodes = ["localhost"]
//
// [points]
// limit = 32
// regex = "123"
//
// [collect]
// interval = 10
// limit = 10
//
// [collect.ua]
// collect_mode = "observe"
//
// [[collect.ua.nodes]]
// id = "1"
//
// [[collect.da.tags]]
// tag = "123"
//
// [collect.dump]
// enable = true
// path = "/usr/loacl/taosx/"
// keep = 10
//
// [report]
// remote = "remote.remote"
// concurrent = 10
// batch_timeout = 100
// "#,
//             toml
//         );
//     }
}