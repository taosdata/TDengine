use crate::runners::opc::opc_type::OpcType;
use serde::{Deserialize, Serialize};
use taos::Dsn;

#[derive(Debug, Serialize, Deserialize)]
pub struct PointUaConfig {
    root: Option<String>,
    namespaces: Option<Vec<u16>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PointDaConfig {
    access_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PointsConfig {
    pub limit: usize,
    pub regex: Option<String>,
    pub ua: Option<PointUaConfig>,
    pub da: Option<PointDaConfig>,
}

impl PointsConfig {
    pub fn from_dsn(dsn: &Dsn) -> Option<Self> {
        let opc_type = OpcType::from_dsn(dsn);
        let mut points_config = Self {
            limit: 0,
            regex: dsn.params.get("regex").map(|v| v.to_string()),
            ua: None,
            da: None,
        };

        match opc_type.unwrap_or(OpcType::FAKE) {
            OpcType::OPCUA => {
                let root = dsn.params.get("root").map(|v| v.to_string());
                let namespaces = dsn.params.get("namespaces").map(|v| {
                    v.split(',')
                        .map(|v| v.parse::<u16>().unwrap())
                        .collect::<Vec<u16>>()
                });
                points_config.ua = Some(PointUaConfig { root, namespaces });
            }
            OpcType::OPCDA => {
                let access_path = dsn.params.get("access_path").map(|v| v.to_string());
                points_config.da = Some(PointDaConfig { access_path });
            }
            OpcType::FAKE => {
                // do nothing
            }
        };

        Some(points_config)
    }
}
