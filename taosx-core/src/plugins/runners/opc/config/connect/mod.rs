use serde::{Deserialize, Serialize};
use taos::Dsn;
use crate::runners::opc::config::connect::da::DaConnectConfig;
use crate::runners::opc::config::connect::ua::UaConnectConfig;
use crate::runners::opc::opc_type::OpcType;

mod da;
mod ua;

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectConfig {
    ua: Option<UaConnectConfig>,
    da: Option<DaConnectConfig>,
}

impl ConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let connect_config = match opc_type {
            OpcType::OPCUA => Self {
                ua: Some(UaConnectConfig::from_dsn(&dsn)?),
                da: None,
            },
            OpcType::OPCDA => Self {
                ua: None,
                da: Some(DaConnectConfig::from_dsn(&dsn)?),
            },
            OpcType::FAKE => Self {
                ua: None,
                da: None,
            }
        };
        Ok(connect_config)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use super::*;
    use taos::Dsn;

    #[test]
    fn test_connect_config_from_dsn() {
        let dsn = Dsn::from_str("opc+ua://").unwrap();
    }
}