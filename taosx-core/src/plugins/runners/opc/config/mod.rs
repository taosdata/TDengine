use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;
use tempfile::NamedTempFile;

use crate::plugins::runners::opc::csv::CsvParser;
use crate::plugins::runners::opc::model::{
    ColumnConfig, GeneratePointMappingBy, OpcModelConfig, OpcPointMappingRule,
};
use crate::runners::opc::config::collect::CollectConfig;
use crate::runners::opc::config::connect::ConnectConfig;
use crate::runners::opc::config::points::PointsConfig;
use crate::runners::opc::config::report::ReportConfig;
use crate::runners::opc::{opc_datasets_impl, OpcType};
use crate::utils::validate_table_column_name;

pub mod collect;
mod connect;
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
    /// taosx-opc collect
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
                    opc_type,
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
        let points = model_config
            .point_config_map
            .iter()
            .map(|(point_id, point_config)| format!("{}::{}", point_id, point_config.code.clone()))
            .join(",");

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

    /// taosx-opc points
    pub fn from_dsn_point_mode(dsn: &Dsn) -> anyhow::Result<Self> {
        if dsn.driver != "opc" && dsn.driver != "opcua" && dsn.driver != "opcda" {
            bail!("invalid opc driver");
        }

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
        if let Some(debug) = debug {
            return Ok(debug.parse::<bool>().unwrap_or(false));
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

    /// 从 dsn 中解析参数 stable_expression 参数：超级表名的表达式。
    /// “选择数据点位”时，super_table_expression 参数是必须的
    pub fn parse_stable_expression(dsn: &Dsn) -> anyhow::Result<String> {
        let stable_expression = dsn
            .params
            .get("super_table_expression")
            .map(|v| {
                if v.is_empty() {
                    // 使用 opc_{type} 作为默认值，是为了兼容之前的任务
                    "opc_{type}".to_string()
                } else {
                    v.to_string()
                }
            })
            .unwrap_or("opc_{type}".to_string());

        // TODO: validate stable_expression
        Ok(stable_expression)
    }

    /// 从 dsn 中解析 child_table_expression 参数：子表名的表达式。
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

    /// 从 dsn 中解析 table_primary_key 参数：主键列。
    /// "选择数据点位"时，table_primary_key 参数指定主键列，只能是 original_ts/request_ts/received_ts。
    pub fn parse_primary_key(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        dsn.params.get("table_primary_key").map_or(Ok(None), |v| {
            if v.is_empty() {
                return Ok(None);
            }
            match v.as_str() {
                ColumnConfig::ORIGINAL_TS
                | ColumnConfig::REQUEST_TS
                | ColumnConfig::RECEIVED_TS => Ok(Some(v.to_string())),
                _ => {
                    bail!(
                        "invalid table_primary_key: {}, must be {} or {} or {}",
                        v.to_string(),
                        ColumnConfig::ORIGINAL_TS,
                        ColumnConfig::REQUEST_TS,
                        ColumnConfig::RECEIVED_TS
                    );
                }
            }
        })
    }

    /// 从 dsn 中解析 table_primary_key_alias 参数：主键列名。
    /// "选择数据点位"时，table_primary_key_alias 参数指定主键的 name。
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

[report]
remote = "127.0.0.1:0"
batch_size = 1000
batch_timeout = 1

[points]
regex_id = "^(?!.*_Error).+$"
regex_name = "^(?!.*_Error).+$"
limit = 0

[points.ua]
"#
        );
    }

    #[tokio::test]
    async fn test_dsn_to_toml_in_collect_mode() {
        std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());

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

[report]
remote = "127.0.0.1:0"
batch_size = 1000
batch_timeout = 1

[points]
limit = 0
update_mode = "Append"
update_interval = 60

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
            "invalid table_primary_key: invalid, must be original_ts or request_ts or received_ts",
            result.err().unwrap().to_string()
        );
    }

    #[test]
    fn test_parse_primary_key_alias() {
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
