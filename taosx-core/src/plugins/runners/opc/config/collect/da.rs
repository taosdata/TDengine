use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::runners::opc::config::csv::CsvParser;
use crate::runners::opc::config::{get_string_vec_from_param_or_file_for_opc, OPCConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaCollectConfig {
    pub(crate) tags: Vec<DaNodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaNodeConfig {
    tag: String,
}

impl DaNodeConfig {
    pub fn new(tag: String) -> Self {
        Self { tag }
    }
}

impl DaCollectConfig {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let csv_config_file = OPCConfig::parse_csv_config_file(dsn);
        let node_vec = match csv_config_file {
            Some(_csv) => {
                let parser = CsvParser::from_dsn(dsn).await?;
                let node_ids = parser.get_point_ids();
                node_ids
            }
            None => get_string_vec_from_param_or_file_for_opc(&mut dsn.clone(), "da.tags")
                .map_err(|s| anyhow::anyhow!("file parse error: {}", s))?,
        };

        let mut tags = Vec::new();
        for i in 0..node_vec.len() {
            let pair = node_vec[i].split("::").collect_vec();
            if pair.len() != 2 {
                let pair = pair.join("::");
                anyhow::bail!(
                    "node config error node config: {} split result len is not 2",
                    pair
                );
            }
            let tag = String::from(pair[0]);
            tags.push(DaNodeConfig { tag: tag.clone() });
        }

        Ok(Self { tags })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_from_dsn() {
        let dsn = Dsn::from_str("opcda://?da.tags=tag1::tb1,tag2::tb2").unwrap();
        let config = DaCollectConfig::from_dsn(&dsn).await.unwrap();
        assert_eq!(config.tags.len(), 2);
        assert_eq!("tag1", config.tags[0].tag);
        assert_eq!("tag2", config.tags[1].tag);

        let dsn = Dsn::from_str("opcda://?da.tags=@../tests/opc/da.tags").unwrap();
        let config = DaCollectConfig::from_dsn(&dsn).await.unwrap();
        assert_eq!(config.tags.len(), 2);
        assert_eq!("tag1", config.tags[0].tag);
        assert_eq!("tag2", config.tags[1].tag);

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@../tests/opc/da_collect_config.csv").unwrap();
        let config = DaCollectConfig::from_dsn(&dsn).await.unwrap();
        assert_eq!(1, config.tags.len());
        assert_eq!("tag1", config.tags[0].tag);
    }
}
