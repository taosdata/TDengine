use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::runners::opc::config::collect::CollectMode;
use crate::runners::opc::config::csv::CsvParser;
use crate::runners::opc::config::{
    get_string_vec_from_param_or_file_for_opc, OPCConfig, PointsMode,
};
use crate::runners::opc::OpcType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UaCollectConfig {
    collect_mode: CollectMode,
    pub(crate) nodes: Vec<UANodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UANodeConfig {
    id: String,
    // value_type: String,
}

impl UANodeConfig {
    pub fn new(id: String) -> Self {
        Self { id }
    }
}

impl UaCollectConfig {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self {
            collect_mode: Self::parse_collect_mode(dsn)?,
            nodes: Self::parse_nodes(dsn).await?,
        })
    }

    fn parse_collect_mode(dsn: &Dsn) -> anyhow::Result<CollectMode> {
        Ok(dsn
            .params
            .get("collect_mode")
            .map(|v| {
                v.parse::<CollectMode>()
                    .map_err(|_err| anyhow::anyhow!("invalid collect_mode: {}", v))
            })
            .transpose()?
            .unwrap_or(CollectMode::Observe))
    }

    async fn parse_nodes(dsn: &Dsn) -> anyhow::Result<Vec<UANodeConfig>> {
        let points_mode = PointsMode::from_dsn(dsn)?;
        let node_vec = match points_mode {
            PointsMode::ByCsv => {
                let csv_files = OPCConfig::parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
                    "csv_config_file is required for PointsMode::ByCsv"
                ))?;
                let parser = CsvParser::try_new(OpcType::OPCUA, csv_files)?;
                let node_ids = parser.parse_all_point_id_and_tbname().await?;
                node_ids
                    .into_iter()
                    .map(|(point_id, tbname)| format!("{}::{}", point_id, tbname))
                    .collect_vec()
            }
            PointsMode::ByCommand => {
                get_string_vec_from_param_or_file_for_opc(&mut dsn.clone(), "ua.nodes")
                    .map_err(|s| anyhow::anyhow!("file parse error: {}", s))?
            }
        };

        let mut ua_node_config_vec = Vec::new();
        for node in node_vec {
            tracing::info!("Node: {}", node);
            if let Some((id, _)) = node.split_once("::") {
                ua_node_config_vec.push(UANodeConfig { id: id.to_string() });
            } else {
                tracing::warn!("Node: {} is not regular(with \"::\")", node);
                ua_node_config_vec.push(UANodeConfig {
                    id: node.to_string(),
                });
            }
        }

        Ok(ua_node_config_vec)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_parse_nodes() {
        std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());

        let dsn =
            Dsn::from_str("opcua://?ua.nodes=ns=3;i=1002::d1dfao123,ns=3;i=1007::dns31007double")
                .unwrap();
        let nodes = UaCollectConfig::parse_nodes(&dsn).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, "ns=3;i=1002");
        assert_eq!(nodes[1].id, "ns=3;i=1007");

        let dsn = Dsn::from_str("opcua://?ua.nodes=@./tests/opc/ua.nodes").unwrap();
        let nodes = UaCollectConfig::parse_nodes(&dsn).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, "ns=3;i=1002");
        assert_eq!(nodes[1].id, "ns=3;i=1007");

        let dsn =
            Dsn::from_str("opcua://?csv_config_file=@./tests/opc/ua_collect_config.csv").unwrap();
        let nodes = UaCollectConfig::parse_nodes(&dsn).await.unwrap();
        assert_eq!(nodes.len(), 29);
        assert_eq!("ns=3;i=1008", nodes[0].id);
        assert_eq!("ns=3;i=1012", nodes[4].id);
    }

    #[test]
    fn test_parse_collect_mode() {
        let dsn = Dsn::from_str("opcua://").unwrap();
        let mode = UaCollectConfig::parse_collect_mode(&dsn).unwrap();
        assert_eq!(mode, CollectMode::Observe);

        let dsn = Dsn::from_str("opcua://?collect_mode=observe").unwrap();
        let mode = UaCollectConfig::parse_collect_mode(&dsn).unwrap();
        assert_eq!(mode, CollectMode::Observe);

        let dsn = Dsn::from_str("opcua://?collect_mode=subscribe").unwrap();
        let mode = UaCollectConfig::parse_collect_mode(&dsn).unwrap();
        assert_eq!(mode, CollectMode::Subscribe);

        let dsn = Dsn::from_str("opcua://?collect_mode=xxx").unwrap();
        let mode = UaCollectConfig::parse_collect_mode(&dsn);
        assert!(mode.is_err());
        assert_eq!("invalid collect_mode: xxx", mode.unwrap_err().to_string());
    }
}
