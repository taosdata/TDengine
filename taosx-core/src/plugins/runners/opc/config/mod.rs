use std::sync::Arc;

use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;
use tempfile::NamedTempFile;

use crate::plugins::sink::point::csv::CsvParser;
use crate::plugins::sink::point::model::{
    GeneratePointMappingBy, PointMappingRule, PointModelConfig,
};
use crate::runners::opc::config::collect::CollectConfig;
use crate::runners::opc::config::connect::ConnectConfig;
use crate::runners::opc::config::points::PointsConfig;
use crate::runners::opc::config::report::ReportConfig;
use crate::runners::opc::points::OpcNode;
use crate::runners::opc::{OpcType, opc_datasets_impl};
use crate::sink::point::model::SourceType;
use crate::utils::parse_key_in_dsn;

pub mod collect;
pub mod connect;
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
    pub points_mode: Option<PointsMode>, // 配置数据点位的方式：csv 或 command
    #[serde(skip)]
    model_config: Option<Arc<PointModelConfig>>,
}

impl OPCConfig {
    /// taosx-opc collect
    pub async fn from_dsn_collect_mode(
        dsn: &Dsn,
        ipc_port: u16,
        task_id: Option<i64>,
    ) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let debug = Self::parse_debug(dsn)?;
        let connect = ConnectConfig::from_dsn(dsn)?;
        let report = ReportConfig::from_dsn(dsn, ipc_port)?;

        let points_mode = PointsMode::from_dsn(dsn)?;
        // OPC model config
        let mut model_config = match points_mode {
            PointsMode::ByCsv => {
                // 上传 csv 配置文件
                let mut parser = CsvParser::from_dsn(dsn)?;
                parser.set_csv_origin(Self::parse_csv_origin(dsn));
                parser.parse().await?
            }
            PointsMode::ByCommand => {
                // 选择数据点位

                // 1. 执行 taosx-opc points 查询点位
                let opc_points_mode = parse_key_in_dsn(dsn, "opc_points_mode")
                    .unwrap_or(Some("variable".to_string()))
                    .unwrap_or("variable".to_string());
                let filter = match opc_points_mode.as_str() {
                    "variable" => Some(OpcNode::variable_node_filter()),
                    "object" => Some(OpcNode::object_node_filter()),
                    "all" => None,
                    _ => {
                        bail!("unsupported opc_points_mode: {}", opc_points_mode);
                    }
                };
                let points = opc_datasets_impl(dsn.clone(), filter).await?;
                // 2. 从 dsn 中解析点位到 TDengine 的映射规则
                let rule: PointMappingRule = PointMappingRule::from_dsn(dsn)?;
                // 3. 生成 point_map 和 table_map
                let (point_map, table_map) = rule.generate(points.clone())?;
                // 4. 生成 object node 的配置
                let node_config_map = rule.generate_node_config_map(points)?;
                let node_config_map = if node_config_map.is_empty() {
                    None
                } else {
                    Some(node_config_map)
                };

                PointModelConfig {
                    source_type: SourceType::try_from(opc_type.as_static_str())?,
                    generate_rule: Some(GeneratePointMappingBy::Rule(rule)),
                    point_config_map: point_map,
                    table_config_map: table_map,
                    update_mode: None,
                    node_config_map,
                }
            }
        };

        // points config
        let points_config = PointsConfig::from_dsn(dsn)?;
        // 设置动态点位更新的模式
        model_config.update_mode = points_config.update_mode;

        // 这里把 model_config 中的点位写到 dsn 中，是为了在 collect 中使用。
        let mut dsn_clone = dsn.clone();
        let points = model_config
            .point_config_map
            .iter()
            .map(|(point_id, point_config)| format!("{}::{}", point_id, point_config.code.clone()))
            .join(",");

        match opc_type {
            OpcType::OPCUA => {
                dsn_clone.set("ua.nodes", points);
            }
            OpcType::OPCDA => {
                dsn_clone.set("da.tags", points);
            }
            _ => {
                bail!("unsupported opc_type: {:?}", opc_type);
            }
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
            model_config: Some(Arc::new(model_config)),
        })
    }

    /// taosx-opc points
    pub fn from_dsn_point_mode(dsn: &Dsn) -> anyhow::Result<Self> {
        // enable/keep_raw_data is not needed in point mode
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

    /// taosx-opc check
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

    pub fn get_model_config(&self) -> Option<&Arc<PointModelConfig>> {
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
        if let Some(debug) = debug {
            return Ok(debug.parse::<bool>().unwrap_or(false));
        }

        Ok(dsn
            .params
            .get("log_level")
            .map(|v| match v.as_str() {
                "error" | "warn" | "info" => Ok(false),
                "debug" | "trace" => Ok(true),
                _ => Err(anyhow::anyhow!("invalid log_level: {}", v)),
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use taos::IntoDsn;

    use super::*;

    /// 测试从 dsn 解析参数，生成一个 taosx-opc 的配置文件
    #[tokio::test]
    async fn test_dsn_to_toml_in_check_mode() {
        // given
        let dsn = Dsn::from_str("opcua://192.168.2.16:53530/OPCUA/SimulationServer").unwrap();
        // when
        let config = OPCConfig::from_dsn_check_mode(&dsn).await.unwrap();
        let toml = toml::to_string(&config).unwrap();
        // then
        assert_eq!(
            toml,
            r#"opc_type = "opcua"
debug = false

[connect.ua]
endpoint = "opc.tcp://192.168.2.16:53530/OPCUA/SimulationServer"
connect_timeout = 10
request_timeout = 10
security_policy = "None"
security_mode = "None"
auth_method = "Anonymous"
auto_reconnect = true

[report]
remote = "127.0.0.1:0"
batch_size = 1000
batch_timeout = 1
"#
        );
    }

    #[tokio::test]
    async fn test_dsn_to_toml_in_point_mode() {
        // given
        let dsn = format!(
            "opcua://{}?node_id_pattern={}&browse_name_pattern={}",
            "192.168.2.16:53530/OPCUA/SimulationServer", "^(?!.*_Error).+$", "^(?!.*_Error).+$"
        )
        .into_dsn()
        .unwrap();
        // when
        let config = OPCConfig::from_dsn_point_mode(&dsn).unwrap();
        let toml = toml::to_string(&config).unwrap();
        // then
        assert_eq!(
            toml,
            r#"opc_type = "opcua"
debug = false

[connect.ua]
endpoint = "opc.tcp://192.168.2.16:53530/OPCUA/SimulationServer"
connect_timeout = 10
request_timeout = 10
security_policy = "None"
security_mode = "None"
auth_method = "Anonymous"
auto_reconnect = true

[report]
remote = "127.0.0.1:0"
batch_size = 1000
batch_timeout = 1

[points]
limit = 0
regex_name = "^(?!.*_Error).+$"
regex_id = "^(?!.*_Error).+$"

[points.ua]
"#
        );
    }

    #[tokio::test]
    async fn test_dsn_to_toml_in_collect_mode() {
        unsafe {
            std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());
        }

        let dsn = "opcua://192.168.2.16:53530?csv_config_file=@./tests/opc/opcua-3.3.6.0.csv"
            .into_dsn()
            .unwrap();
        let config = OPCConfig::from_dsn_collect_mode(&dsn, 0, None)
            .await
            .unwrap();
        let toml = toml::to_string(&config).unwrap();
        assert_eq!(
            toml,
            r#"opc_type = "opcua"
debug = false

[connect.ua]
endpoint = "opc.tcp://192.168.2.16:53530/"
connect_timeout = 10
request_timeout = 10
security_policy = "None"
security_mode = "None"
auth_method = "Anonymous"
auto_reconnect = true

[report]
remote = "127.0.0.1:0"
batch_size = 1000
batch_timeout = 1

[points]
limit = 0

[points.ua]

[collect.ua]
collect_mode = "observe"

[[collect.ua.nodes]]
id = "ns=3;i=1005"

[[collect.ua.nodes]]
id = "ns=3;i=1006"

[[collect.ua.nodes]]
id = 'ns=3;s="数据块_1"."Tag101"'
"#
        );
    }
}
