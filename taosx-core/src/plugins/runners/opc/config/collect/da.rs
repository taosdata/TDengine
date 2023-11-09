use itertools::Itertools;
use serde::Serialize;
use taos::Dsn;

use crate::runners::opc::config::{generate_opcconfig_from_csv, get_string_vec_from_param_or_file_for_opc, OPCConfig};

#[derive(Debug, Serialize)]
pub struct DaCollectConfig {
    tags: Vec<DaNodeConfig>,
}

#[derive(Debug, Serialize)]
pub struct DaNodeConfig {
    tag: String,
}

impl DaCollectConfig {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let csv_config_file = OPCConfig::parse_csv_config_file(dsn);
        let node_vec = match csv_config_file {
            Some(csv) => {
                generate_opcconfig_from_csv("opcda", csv.as_str())
                    .await
                    .map(|(a, b, c)| b)
                    .map_err(|err|
                        anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                    )?
            }
            None => {
                get_string_vec_from_param_or_file_for_opc(&mut dsn.clone(), "da.tags")
                    .map_err(|s|
                        anyhow::anyhow!("file parse error: {}", s)
                    )?
            }
        };

        let mut tags = Vec::new();
        for i in 0..node_vec.len() {
            let pair = node_vec[i].split("::").collect_vec();
            if pair.len() != 2 {
                let pair = pair.join("::");
                anyhow::bail!("node config error node config: {} split result len is not 2", pair);
            }
            let tag = String::from(pair[0]);
            tags.push(DaNodeConfig { tag: tag.clone() });
        }

        Ok(Self {
            tags,
        })
    }
}

#[cfg(test)]
mod tests{

}