use std::io::BufRead;

use anyhow::bail;
use csv_lib::ReaderBuilder;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;
use tempfile::NamedTempFile;

use crate::runners::opc::config::collect::CollectConfig;
use crate::runners::opc::config::connect::ConnectConfig;
use crate::runners::opc::config::csv::CsvParser;
use crate::runners::opc::config::model::{
    ColumnConfig, GeneratePointMappingBy, OpcModelConfig, OpcPointMappingRule,
};
use crate::runners::opc::config::points::PointsConfig;
use crate::runners::opc::config::report::ReportConfig;
use crate::runners::opc::{csv_string_record_from_iter, opc_datasets_impl, OpcType};
use crate::utils::validate_table_column_name;

pub mod collect;
mod connect;
pub mod csv;
pub mod model;
pub mod points;
mod report;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PointsMode {
    ByCsv,
    ByCommand,
}

impl PointsMode {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let csv_config_file = OPCConfig::parse_csv_config_file(dsn);
        if csv_config_file.is_some() {
            return Ok(Self::ByCsv);
        }
        Ok(Self::ByCommand)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OPCConfig {
    pub opc_type: OpcType,
    pub debug: bool,
    pub connect: ConnectConfig,
    pub report: ReportConfig,
    pub points: Option<PointsConfig>,
    pub collect: Option<CollectConfig>,

    #[serde(skip)]
    pub points_mode: Option<PointsMode>, // 数据点位的模式, csv 或 command
    #[serde(skip)]
    model_config: Option<OpcModelConfig>,
}

impl OPCConfig {
    pub async fn from_dsn_collect_mode(
        dsn: &Dsn,
        ipc_port: u16,
        task_id: Option<i64>,
    ) -> anyhow::Result<Self> {
        if dsn.driver != "opc" && dsn.driver != "opcua" && dsn.driver != "opcda" {
            bail!("invalid opc driver");
        }

        let opc_type = OpcType::from_dsn(dsn)?;
        let debug = Self::parse_debug(dsn)?;
        let connect = ConnectConfig::from_dsn(dsn)?;
        let report = ReportConfig::from_dsn(dsn, ipc_port)?;

        let points_mode = PointsMode::from_dsn(dsn)?;
        // OPC model config
        let model_config = match points_mode {
            PointsMode::ByCsv => {
                // 上传 csv 配置文件
                let mut parser = CsvParser::from_dsn(dsn)?;
                parser.set_csv_origin(Self::parse_csv_origin(dsn));
                parser.parse().await?
            }
            PointsMode::ByCommand => {
                // 选择数据点位
                // 1. 执行 taosx-opc point 查询点位
                let points = opc_datasets_impl(dsn.clone()).await?;
                // 2. 从 dsn 中解析点位到 TDengine 的映射规则
                let rule = OpcPointMappingRule::from_dsn(dsn)?;
                // 3. 生成 model_config
                let (point_map, table_map) = rule.generate(points)?;

                OpcModelConfig {
                    opc_type: opc_type.clone(),
                    generate_rule: Some(GeneratePointMappingBy::Rule(rule)),
                    point_config_map: point_map,
                    table_config_map: table_map,
                }
            }
        };

        // points config
        let points_config = PointsConfig::from_dsn(dsn)?;

        // 这里把 model_config 中的点位写到 dsn 中，是为了在 collect 中使用。
        // todo: 应该改造一下 collect 解析，直接使用 model_config 中的点位
        let mut dsn_clone = dsn.clone();
        let points = csv_string_record_from_iter(model_config.point_config_map.iter().map(
            |(point_id, point_config)| format!("{}::{}", point_id, point_config.code.clone()),
        ));
        if dsn.driver.as_str() == "opcua" {
            dsn_clone.set("ua.nodes", points);
        } else {
            dsn_clone.set("da.tags", points);
        }
        let collect = CollectConfig::from_dsn(&dsn_clone, task_id).await?;

        Ok(Self {
            opc_type,
            debug,
            connect,
            report,
            points: Some(points_config),
            collect: Some(collect),
            points_mode: Some(points_mode),
            model_config: Some(model_config),
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
            points: Some(PointsConfig::from_dsn(&dsn)?),
            collect: None,
            report: ReportConfig::from_dsn(&dsn, 0)?,
            model_config: None,
            points_mode: None,
        })
    }

    pub async fn from_dsn_check_mode(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self {
            opc_type: OpcType::from_dsn(dsn)?,
            debug: Self::parse_debug(dsn)?,
            connect: ConnectConfig::from_dsn(dsn)?,
            points: None,
            collect: None,
            report: ReportConfig::from_dsn(dsn, 0)?,
            model_config: None,
            points_mode: None,
        })
    }

    pub fn get_model_config(&self) -> Option<&OpcModelConfig> {
        self.model_config.as_ref()
    }

    pub fn set_temp_filepath(
        &mut self,
        key: &str,
        temp_file: Option<&NamedTempFile>,
    ) -> anyhow::Result<()> {
        match temp_file {
            None => Ok(()),
            Some(temp_file) => {
                let file_path = temp_file
                    .path()
                    .canonicalize()
                    .map(|p| p.display().to_string())
                    .map_err(|err| anyhow::anyhow!("failed to get temp file path: {}", err))?;

                match key {
                    "certificate" | "private_key" | "auth_certificate" | "auth_private_key" => {
                        let connect = self.connect.ua.as_mut();
                        match connect {
                            None => {
                                bail!("connect is None");
                            }
                            Some(connect) => connect.set_temp_filepath(key, file_path.as_str()),
                        }
                    }
                    _ => {
                        bail!("invalid key: {}, v", key);
                    }
                }
            }
        }
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

    pub fn parse_csv_config_file(dsn: &Dsn) -> Option<String> {
        dsn.params.get("csv_config_file").and_then(|v| {
            if v.is_empty() {
                return None;
            }
            Some(v.to_string())
        })
    }

    pub fn parse_csv_config_files(dsn: &Dsn) -> Option<Vec<String>> {
        dsn.params.get("csv_config_file").and_then(|v| {
            if v.is_empty() {
                return None;
            }

            let csv_files = v
                .split(",")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect_vec();

            Some(csv_files)
        })
    }

    /// 从 dsn 中解析参数 select_all_points 参数
    /// 1. dsn 没有参数，返回 None
    /// 2. dsn 有参数，且合法，true/false，返回 Some(true) or Some(false)
    /// 3. dsn 有参数，不合法，Error, return Error()
    pub fn parse_select_all_points(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        dsn.params.get("select_all_points").map_or(Ok(None), |v| {
            if v.is_empty() {
                return Ok(None);
            }
            match v.as_str() {
                "true" => Ok(Some(true)),
                "false" => Ok(Some(false)),
                _ => {
                    bail!(
                        "invalid select_all_points: {}, must be true or false",
                        v.to_string()
                    );
                }
            }
        })
    }

    /// 从 dsn 中解析参数 stable_expression 参数：超级表名的表达式
    /// “选择数据点位”时，super_table_expression 参数是必须的
    pub fn parse_stable_expression(dsn: &Dsn) -> anyhow::Result<String> {
        // TODO: 使用 opc_{type} 作为默认值，是为了兼容之前的任务
        let stable_expression = dsn
            .params
            .get("super_table_expression")
            .map(|v| {
                if v.is_empty() {
                    "opc_{type}".to_string()
                } else {
                    v.to_string()
                }
            })
            .unwrap_or("opc_{type}".to_string());

        // let expr = dsn
        //     .params
        //     .get("super_table_expression")
        //     .ok_or(anyhow::anyhow!("super_table_expression is required"))?;
        // if expr.is_empty() {
        //     bail!("super_table_expression cannot be empty");
        // }
        // let stable_expression = expr.to_string();

        Ok(stable_expression)
    }

    /// 从 dsn 中解析 child_table_expression 参数：子表名的表达式
    /// "选择数据点位"时，child_table_expression 参数是必须的
    pub fn parse_tbname_expression(dsn: &Dsn) -> anyhow::Result<String> {
        let expr = dsn
            .params
            .get("child_table_expression")
            .ok_or(anyhow::anyhow!("child_table_expression is required"))?;

        if expr.is_empty() {
            bail!("child_table_expression cannot be empty");
        }

        let tbname_expression = expr.to_string();
        // TODO: validate tbname_expression
        Ok(tbname_expression)
    }

    /// 从 dsn 中解析 table_primary_key 参数：主键列
    /// "选择数据点位"时，table_primary_key 参数是可选的
    pub fn parse_primary_key(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        dsn.params.get("table_primary_key").map_or(Ok(None), |v| {
            if v.is_empty() {
                return Ok(None);
            }
            match v.as_str() {
                ColumnConfig::ORIGINAL_TS | ColumnConfig::RECEIVED_TS => Ok(Some(v.to_string())),
                _ => {
                    bail!(
                        "invalid table_primary_key: {}, must be {} or {}",
                        v.to_string(),
                        ColumnConfig::ORIGINAL_TS,
                        ColumnConfig::RECEIVED_TS
                    );
                }
            }
        })
    }

    /// 从 dsn 中解析 table_primary_key_alias 参数：主键列名
    /// "选择数据点位"时，table_primary_key_alias 参数是可选的
    pub fn parse_primary_key_alias(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        Ok(dsn.params.get("table_primary_key_alias").and_then(|v| {
            if v.is_empty() {
                return None;
            }
            let primary_key_alias = v.to_string();
            validate_table_column_name("primary_key", &primary_key_alias).ok()?;
            Some(primary_key_alias)
        }))
    }

    pub fn parse_csv_origin(dsn: &Dsn) -> Option<String> {
        dsn.params.get("csv_config_file_origin").and_then(|v| {
            if v.is_empty() {
                return None;
            }
            Some(v.to_string())
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
        if node_config.is_empty() {
            tracing::warn!("node config is empty");
            // return Err(format!("node config set but is empty: {nodes}"));
        }
        return Ok(node_config);
    }
    // tracing::warn!("node config is empty");
    Err("Nodes not set".to_string())
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

    #[test]
    fn test_parse_select_all_points() {
        let dsn = "opcua://?select_all_points=true"
            .to_string()
            .into_dsn()
            .unwrap();
        let select_all_points = OPCConfig::parse_select_all_points(&dsn).unwrap();
        assert_eq!(select_all_points, Some(true));

        let dsn = "opcua://?select_all_points=false"
            .to_string()
            .into_dsn()
            .unwrap();
        let select_all_points = OPCConfig::parse_select_all_points(&dsn).unwrap();
        assert_eq!(select_all_points, Some(false));

        let dsn = "opcua://?select_all_points="
            .to_string()
            .into_dsn()
            .unwrap();
        let select_all_points = OPCConfig::parse_select_all_points(&dsn).unwrap();
        assert_eq!(select_all_points, None);

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let select_all_points = OPCConfig::parse_select_all_points(&dsn).unwrap();
        assert_eq!(select_all_points, None);
    }

    #[test]
    fn test_parse_stable_expression() {
        let dsn = "opcua://?super_table_expression=abc_{type}"
            .to_string()
            .into_dsn()
            .unwrap();
        let stable_expression = OPCConfig::parse_stable_expression(&dsn).unwrap();
        assert_eq!(stable_expression, "abc_{type}");

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let stable_expression = OPCConfig::parse_stable_expression(&dsn).unwrap();
        assert_eq!(stable_expression, "opc_{type}");

        // let result = OPCConfig::parse_stable_expression(&dsn);
        // assert!(result.is_err());
        // assert_eq!(
        //     "super_table_expression is required",
        //     result.err().unwrap().to_string()
        // );
    }

    #[test]
    fn test_parse_tbname_expression() {
        let dsn = "opcua://?child_table_expression=t_{ns}_{id}"
            .to_string()
            .into_dsn()
            .unwrap();
        let tbname_expression = OPCConfig::parse_tbname_expression(&dsn).unwrap();
        assert_eq!(tbname_expression, "t_{ns}_{id}");

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let result = OPCConfig::parse_tbname_expression(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "child_table_expression is required",
            result.err().unwrap().to_string()
        );
    }

    #[test]
    fn test_parse_primary_key() {
        let dsn = "opcua://?table_primary_key=original_ts"
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key = OPCConfig::parse_primary_key(&dsn).unwrap();
        assert_eq!(primary_key, Some("original_ts".to_string()));

        let dsn = "opcua://?table_primary_key=received_ts"
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key = OPCConfig::parse_primary_key(&dsn).unwrap();
        assert_eq!(primary_key, Some("received_ts".to_string()));

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let primary_key = OPCConfig::parse_primary_key(&dsn).unwrap();
        assert_eq!(primary_key, None);

        let dsn = "opcua://?table_primary_key="
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key = OPCConfig::parse_primary_key(&dsn).unwrap();
        assert_eq!(primary_key, None);

        let dsn = "opcua://?table_primary_key=invalid"
            .to_string()
            .into_dsn()
            .unwrap();
        let result = OPCConfig::parse_primary_key(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid table_primary_key: invalid, must be original_ts or received_ts",
            result.err().unwrap().to_string()
        );
    }

    #[test]
    fn parse_primary_key_alias() {
        let dsn = "opcua://?table_primary_key_alias=ts"
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key_alias = OPCConfig::parse_primary_key_alias(&dsn).unwrap();
        assert_eq!(primary_key_alias, Some("ts".to_string()));

        let dsn = "opcua://".to_string().into_dsn().unwrap();
        let primary_key_alias = OPCConfig::parse_primary_key_alias(&dsn).unwrap();
        assert_eq!(primary_key_alias, None);

        let dsn = "opcua://?table_primary_key_alias="
            .to_string()
            .into_dsn()
            .unwrap();
        let primary_key_alias = OPCConfig::parse_primary_key_alias(&dsn).unwrap();
        assert_eq!(primary_key_alias, None);
    }
}
