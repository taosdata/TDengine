use crate::runners::opc::config::connect::da::DaConnectConfig;
use crate::runners::opc::config::connect::ua::UaConnectConfig;
use crate::runners::opc::OpcType;
use serde::{Deserialize, Serialize};
use taos::Dsn;

mod da;
mod ua;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectConfig {
    pub ua: Option<UaConnectConfig>,
    da: Option<DaConnectConfig>,
}

impl ConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let connect_config = match opc_type {
            OpcType::OPCUA => Self {
                ua: Some(UaConnectConfig::from_dsn(dsn)?),
                da: None,
            },
            OpcType::OPCDA => Self {
                ua: None,
                da: Some(DaConnectConfig::from_dsn(dsn)?),
            },
            OpcType::FAKE => Self { ua: None, da: None },
        };
        Ok(connect_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("opcua://root:taosdata@localhost:1234").unwrap();
        let connect = ConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("opc.tcp://localhost:1234/", connect.ua.unwrap().endpoint);
        assert_eq!(None, connect.da);

        let dsn = Dsn::from_str("opcda://192.168.1.10,192.168.1.11,192.168.1.12/subject").unwrap();
        let connect = ConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!(None, connect.ua);
        let da = connect.da.unwrap();
        assert_eq!("subject", da.server);
        assert_eq!(
            vec!["192.168.1.10", "192.168.1.11", "192.168.1.12"],
            da.nodes
        );
    }
}
