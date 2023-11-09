use itertools::Itertools;
use serde::Serialize;
use taos::{AsyncQueryable, Dsn};

use crate::runners::opc::config::{generate_opcconfig_from_csv, get_string_vec_from_param_or_file_for_opc, OPCConfig};
use crate::runners::opc::config::collect::CollectMode;

#[derive(Debug, Serialize)]
pub struct UaCollectConfig {
    collect_mode: CollectMode,
    nodes: Vec<UANodeConfig>,
}

#[derive(Debug, Serialize)]
pub struct UANodeConfig {
    id: String,
    // value_type: String,
}

impl UaCollectConfig {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self {
            collect_mode: Self::parse_collect_mode(dsn)?,
            nodes: Self::parse_nodes(dsn)?,
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
                generate_opcconfig_from_csv("opcua", csv.as_str())
                    .await
                    .map(|(a, b, c)| b)
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
                anyhow::bail!("node config error node config: {} split result len is not 2", pair);
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