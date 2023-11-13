use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::runners::opc::config::{generate_config_from_csv, get_string_vec_from_param_or_file_for_opc, OPCConfig};
use crate::runners::opc::config::collect::CollectMode;

#[derive(Debug, Serialize, Deserialize)]
pub struct UaCollectConfig {
    collect_mode: CollectMode,
    nodes: Vec<UANodeConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UANodeConfig {
    id: String,
    // value_type: String,
}

impl UaCollectConfig {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self {
            collect_mode: Self::parse_collect_mode(dsn)?,
            nodes: Self::parse_nodes(dsn).await?,
        })
    }

    fn parse_collect_mode(dsn: &Dsn) -> anyhow::Result<CollectMode> {
        Ok(dsn.params
            .get("collect_mode")
            .map(|v| {
                v.parse::<CollectMode>().map_err(|err| {
                    anyhow::anyhow!("parse collect_mode failed, cause: {}", err.to_string())
                })
            })
            .transpose()?
            .unwrap_or(CollectMode::OBSERVE)
        )
    }

    async fn parse_nodes(dsn: &Dsn) -> anyhow::Result<Vec<UANodeConfig>> {
        let csv_config_file = OPCConfig::parse_csv_config_file(dsn);

        let node_vec = match csv_config_file {
            Some(csv) => {
                generate_config_from_csv("opcua", csv.as_str())
                    .await
                    .map(|(_a, b, _c)| b)
                    .map_err(|err| {
                        anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                    })?
            }
            None => {
                get_string_vec_from_param_or_file_for_opc(&mut dsn.clone(), "ua.nodes")
                    .map_err(|s| {
                        anyhow::anyhow!("file parse error: {}", s)
                    })?
            }
        };

        let mut ua_node_config_vec = Vec::new();
        for i in 0..node_vec.len() {
            let pair = node_vec[i].split("::").collect_vec();
            if pair.len() != 2 {
                let pair = pair.join("::");
                anyhow::bail!("failed to parse node: {}, cause: split result len is not 2", pair);
            }
            let id = String::from(pair[0]);
            ua_node_config_vec.push(UANodeConfig {
                id: id.clone()
            });
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
        let dsn = Dsn::from_str("opcua://?ua.nodes=ns=3;i=1002::d1dfao123,ns=3;i=1007::dns31007double").unwrap();
        let nodes = UaCollectConfig::parse_nodes(&dsn).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, "ns=3;i=1002");
        assert_eq!(nodes[1].id, "ns=3;i=1007");

        let dsn = Dsn::from_str("opcua://?ua.nodes=@../tests/opc/ua.nodes").unwrap();
        let nodes = UaCollectConfig::parse_nodes(&dsn).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, "ns=3;i=1002");
        assert_eq!(nodes[1].id, "ns=3;i=1007");

        let dsn = Dsn::from_str("opcua://?csv_config_file=@../tests/opc/opc_point_config_simple.csv").unwrap();
        let nodes = UaCollectConfig::parse_nodes(&dsn).await.unwrap();
        assert_eq!(nodes.len(), 29);
        assert_eq!("ns=3;i=1008", nodes[0].id);
        assert_eq!("ns=3;i=1012", nodes[4].id);
    }

    #[test]
    fn test_parse_collect_mode() {
        let dsn = Dsn::from_str("opcua://").unwrap();
        let collect_mode = UaCollectConfig::parse_collect_mode(&dsn).unwrap();
        assert_eq!(collect_mode, CollectMode::OBSERVE);

        let dsn = Dsn::from_str("opcua://?collect_mode=observe").unwrap();
        let collect_mode = UaCollectConfig::parse_collect_mode(&dsn).unwrap();
        assert_eq!(collect_mode, CollectMode::OBSERVE);

        let dsn = Dsn::from_str("opcua://?collect_mode=subscribe").unwrap();
        let collect_mode = UaCollectConfig::parse_collect_mode(&dsn).unwrap();
        assert_eq!(collect_mode, CollectMode::SUBSCRIBE);

        let dsn = Dsn::from_str("opcua://?collect_mode=xxx").unwrap();
        let collect_mode = UaCollectConfig::parse_collect_mode(&dsn);
        assert!(collect_mode.is_err());
        assert_eq!("parse collect_mode failed, cause: ", collect_mode.unwrap_err().to_string());
    }
}