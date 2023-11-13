use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::runners::opc::config::{
    generate_config_from_csv, get_string_vec_from_param_or_file_for_opc, OPCConfig,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct DaCollectConfig {
    tags: Vec<DaNodeConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DaNodeConfig {
    tag: String,
}

impl DaCollectConfig {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let csv_config_file = OPCConfig::parse_csv_config_file(dsn);
        let node_vec = match csv_config_file {
            Some(csv) => generate_config_from_csv("opcda", csv.as_str())
                .await
                .map(|(_a, b, _c)| b)
                .map_err(|err| {
                    anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                })?,
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
    use super::*;
    use std::str::FromStr;

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

        let dsn = Dsn::from_str(
            "opcda://?csv_config_file=@../tests/opc/opc_point_config_complicated.csv",
        )
        .unwrap();
        let config = DaCollectConfig::from_dsn(&dsn).await.unwrap();
        assert_eq!(1, config.tags.len());
        assert_eq!("ns=3;i=1002", config.tags[0].tag);
    }
}
