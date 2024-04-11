use std::io::BufRead;

use crate::get_data_dir;
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
    pub connect: ConnectConfig,
    pub report: ReportConfig,
    pub points: Option<PointsConfig>,
    pub collect: CollectConfig,

    #[serde(skip)]
    model_config: Option<OpcModelConfig>,
}

impl OPCConfig {
    /// 从 dsn 中解析参数 select_all_points
    /// 1. dsn 没有参数，返回 None
    /// 2. dsn 有参数，且合法，true/false，返回 Some(true) or Some(false)
    /// 3. dsn 有参数，不合法，Error, return Error()
    pub fn parse_select_all_points(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        dsn.params
            .get("select_all_points")
            .map(|v| {
                v.parse::<bool>().map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse select_all_points: {}, cause: {}",
                        v,
                        err.to_string()
                    )
                })
            })
            .transpose()
    }

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

        let model_config = if csv_config_file.is_some() {
            let parser = CsvParser::from_dsn(dsn).await?;

            let table_to_drop = parser.get_tables_to_drop();
            for child_table_name in table_to_drop.iter() {
                let drop_sql = format!("DROP TABLE IF EXISTS {child_table_name}");
                tracing::info!("drop sql: {drop_sql}");
                taos.exec(drop_sql).await.map_err(|err| {
                    anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                })?;
            }

            Some(parser.get_model_config())
        } else {
            // 如果没有 csv_config_file 参数，那么, 从 dsn 中解析 point_config_map 和 table_config_map
            let point_config_map = Self::build_point_config_map(dsn)?;

            // 前端传递了 opc_table_config 参数
            let table_config = dsn
                .params
                .get("opc_table_config")
                .ok_or(anyhow::anyhow!("opc_table_config is required"))?;
            let table_config: TableConfig =
                serde_json::from_str(table_config.as_str()).map_err(|v| {
                    anyhow::anyhow!("failed to parse opc_table_config, cause: {}", v.to_string())
                })?;

            // all point_id share the same table_config
            let mut table_config_map = LinkedHashMap::new();
            for point_id in point_config_map.keys() {
                table_config_map.insert(point_id.clone(), table_config.clone());
            }

            Some(OpcModelConfig {
                point_config_map,
                table_config_map,
            })
        };

        Ok(Self {
            opc_type,
            debug,
            connect,
            report,
            points: None,
            collect: CollectConfig::from_dsn(dsn, id).await?,
            model_config,
        })
    }

    pub fn from_dsn_point_mode(dsn: &Dsn) -> anyhow::Result<Self> {
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
            points: PointsConfig::from_dsn(&dsn),
            collect: CollectConfig::new_empty(),
            report: ReportConfig::from_dsn(&dsn, 0)?,
            model_config: None,
        })
    }

    pub fn get_model_config(&self) -> Option<&OpcModelConfig> {
        self.model_config.as_ref()
    }

    pub async fn from_dsn_for_validate(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(OPCConfig {
            opc_type: OpcType::from_dsn(dsn)?,
            debug: Self::parse_debug(dsn)?,
            connect: ConnectConfig::from_dsn(dsn)?,
            points: None,
            collect: CollectConfig::new_empty(),
            report: ReportConfig::from_dsn(dsn, 0)?,
            model_config: None,
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

    /// parse point config map from dsn
    fn build_point_config_map(dsn: &Dsn) -> anyhow::Result<LinkedHashMap<String, PointConfig>> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let point_config_map = match opc_type {
            OpcType::OPCUA => {
                let mut point_config_map = LinkedHashMap::new();

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
                    point_config_map.insert(
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
                point_config_map
            }
            OpcType::OPCDA => {
                let mut point_config_map = LinkedHashMap::new();

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
                    point_config_map.insert(
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
                point_config_map
            }
            _ => bail!("invalid opc type: {}", opc_type),
        };

        Ok(point_config_map)
    }
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub enum AuthMethod {
    Anonymous,
    UserName,
    #[default]
    Certificate,
}

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
