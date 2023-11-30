use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DaConnectConfig {
    pub server: String,
    pub nodes: Vec<String>,
}

impl DaConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let server = dsn
            .subject
            .clone()
            .ok_or(anyhow::anyhow!("subject is required for opc da"))?;
        let nodes = dsn.addresses.clone();
        if nodes.is_empty() {
            bail!("host config error: should config at least one host");
        }
        let nodes = nodes
            .into_iter()
            .filter(|addr| addr.host.is_some())
            .map(|addr| addr.host.unwrap().clone())
            .collect_vec();
        Ok(Self { server, nodes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let config = DaConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "subject is required for opc da",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("opc:///subject").unwrap();
        let config = DaConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "host config error: should config at least one host",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("opc://localhost/subject").unwrap();
        let config = DaConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("subject", config.server);
        assert_eq!(vec!["localhost"], config.nodes);

        let dsn = Dsn::from_str("opc://192.168.1.10,192.168.1.11,192.168.1.12/subject").unwrap();
        let config = DaConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("subject", config.server);
        assert_eq!(
            vec!["192.168.1.10", "192.168.1.11", "192.168.1.12"],
            config.nodes
        );
    }
}
