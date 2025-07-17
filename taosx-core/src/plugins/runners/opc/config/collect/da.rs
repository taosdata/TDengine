use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::plugins::runners::opc::csv::CsvParser;
use crate::runners::opc::config::collect::parse_opc_node_ids;
use crate::runners::opc::config::{OPCConfig, PointsMode};
use crate::runners::opc::OpcType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaCollectConfig {
    pub tags: Vec<DaNodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaNodeConfig {
    pub tag: String,
}

impl DaCollectConfig {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let points_mode = PointsMode::from_dsn(dsn)?;

        let node_ids = match points_mode {
            PointsMode::ByCsv => {
                let csv_files = OPCConfig::parse_csv_config_files(dsn).ok_or(anyhow::anyhow!(
                    "csv_config_file is required for PointsMode::ByCsv"
                ))?;
                let parser = CsvParser::try_new(OpcType::OPCDA, csv_files)?;
                let node_ids = parser.parse_point_id_and_tbname().await?;

                node_ids
                    .iter()
                    .map(|(point_id, _)| point_id.clone())
                    .collect_vec()
            }
            // parse from dsn.da.tags
            PointsMode::ByCommand => {
                // get_string_vec_from_param_or_file_for_opc(&mut dsn.clone(), "da.tags")
                //     .map_err(|s| anyhow::anyhow!("file parse error: {}", s))?
                parse_opc_node_ids(dsn, "da.tags").await?
            }
        };

        let tags = node_ids
            .into_iter()
            .map(|tag| DaNodeConfig { tag })
            .collect_vec();

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

        let dsn =
            Dsn::from_str("opcda://?csv_config_file=@./tests/opc/da_collect_config.csv").unwrap();
        let config = DaCollectConfig::from_dsn(&dsn).await.unwrap();
        assert_eq!(1, config.tags.len());
        assert_eq!("tag1", config.tags[0].tag);
    }
}
