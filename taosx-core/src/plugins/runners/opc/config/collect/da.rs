use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::runners::opc::config::csv::CsvParser;
use crate::runners::opc::config::{
    get_string_vec_from_param_or_file_for_opc, OPCConfig, PointsMode,
};
use crate::runners::opc::OpcType;

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
        let points_mode = PointsMode::from_dsn(dsn)?;

        let node_vec = match points_mode {
            PointsMode::ByCsv => {
                let csv_files = OPCConfig::parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
                    "csv_config_file is required for PointsMode::ByCsv"
                ))?;
                let parser = CsvParser::try_new(OpcType::OPCDA, csv_files)?;
                let node_ids = parser.parse_all_point_id_and_tbname().await?;

                node_ids
                    .into_iter()
                    .map(|(point_id, tbname)| format!("{}::{}", point_id, tbname))
                    .collect_vec()
            }
            // parse from dsn.da.tags
            PointsMode::ByCommand => {
                get_string_vec_from_param_or_file_for_opc(&mut dsn.clone(), "da.tags")
                    .map_err(|s| anyhow::anyhow!("file parse error: {}", s))?
            }
        };

        let mut tags = Vec::new();
        for node in node_vec {
            let pair = node.split("::").collect_vec();
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
        std::env::set_var("TAOSX_DATA_DIR", std::env::current_dir().unwrap());

        let dsn = Dsn::from_str("opcda://?da.tags=tag1::tb1,tag2::tb2").unwrap();
        let config = DaCollectConfig::from_dsn(&dsn).await.unwrap();
        assert_eq!(config.tags.len(), 2);
        assert_eq!("tag1", config.tags[0].tag);
        assert_eq!("tag2", config.tags[1].tag);

        let dsn = Dsn::from_str("opcda://?da.tags=@./tests/opc/da.tags").unwrap();
        let config = DaCollectConfig::from_dsn(&dsn).await.unwrap();
        assert_eq!(config.tags.len(), 2);
        assert_eq!("tag1", config.tags[0].tag);
        assert_eq!("tag2", config.tags[1].tag);

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@./tests/opc/da_collect_config.csv").unwrap();
        let config = DaCollectConfig::from_dsn(&dsn).await.unwrap();
        assert_eq!(1, config.tags.len());
        assert_eq!("tag1", config.tags[0].tag);
    }
}
