use anyhow::bail;
use serde::{Deserialize, Serialize};
use taos::{Dsn, Ty};

use taosx_ipc::prelude::IpcDataType;

use crate::runners::opc::config::{generate_config_from_csv, OPCConfig};
use crate::runners::opc::opc_type::OpcType;

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TableConfig {
    pub stable_prefix: Option<String>,
    pub column_configs: Vec<ColumnConfig>,
    pub tag_configs: Option<Vec<TagConfig>>,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct ColumnConfig {
    pub column_name: String,
    pub column_type: Option<Ty>,
    pub column_alias: Option<String>,
    pub is_primary_key: bool,
}

#[derive(Clone, Deserialize, Debug, Serialize)]
pub struct TagConfig {
    pub column_name: String,
    pub column_type: IpcDataType,
}

impl TableConfig {
    pub async fn from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Self>> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let csv_config_file = OPCConfig::parse_csv_config_file(dsn);
        let opc_table_config = match opc_type {
            OpcType::OPCUA => match csv_config_file {
                Some(csv) => {
                    let config = generate_config_from_csv("opcua", csv.as_str())
                        .await
                        .map(|(a, _b, _c)| a)
                        .map_err(|err| {
                            anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                        })?;
                    Some(config)
                }
                None => None,
            },
            OpcType::OPCDA => match csv_config_file {
                Some(csv) => {
                    let config = generate_config_from_csv("opcda", csv.as_str())
                        .await
                        .map(|(a, _b, _c)| a)
                        .map_err(|err| {
                            anyhow::anyhow!("csv_config_file config error: {}", err.to_string())
                        })?;
                    Some(config)
                }
                None => None,
            },
            OpcType::FAKE => None,
        };

        let select_all_points = OPCConfig::parse_select_all_points(dsn);
        let table_config = match opc_table_config {
            Some(table_config) => Some(table_config.table_config),
            None => {
                if select_all_points {
                    None
                } else {
                    let config = dsn.params.get("opc_table_config");
                    if config.is_none() {
                        bail!("opc_table_config is required");
                    }
                    Some(serde_json::from_str(config.unwrap().as_str()).map_err(|v| {
                        anyhow::anyhow!(
                            "failed to parse opc_table_config, cause: {}",
                            v.to_string()
                        )
                    })?)
                }
            }
        };

        Ok(table_config)
    }
}
