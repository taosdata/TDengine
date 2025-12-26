use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::utils;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DaConnectConfig {
    pub server: String,
    pub nodes: Vec<String>,
    pub reconnect_times: Option<u32>, // 重连尝试次数，不设置默认 100
    pub reconnect_interval: Option<u32>, // 重连间隔，单位毫秒，不设置默认 1000
    pub add_tag_retry_times: Option<u32>, // 重连后重新添加点位尝试次数，不设置默认 100
    pub add_tag_retry_interval: Option<u32>, // 重新添加点位失败时下次重试间隔，单位毫秒，不设置默认 500
    pub failed_reads_to_force_reconnect: Option<u32>, // 累计读取失败达到次数将强制重连，不设置默认 50
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

        let reconnect_times = utils::parse_key_in_dsn::<u32>(dsn, "reconnect_times")?;
        let reconnect_interval = utils::parse_key_in_dsn::<u32>(dsn, "reconnect_interval")?;
        let add_tag_retry_times = utils::parse_key_in_dsn::<u32>(dsn, "add_tag_retry_times")?;
        let add_tag_retry_interval = utils::parse_key_in_dsn::<u32>(dsn, "add_tag_retry_interval")?;
        let failed_reads_to_force_reconnect =
            utils::parse_key_in_dsn::<u32>(dsn, "failed_reads_to_force_reconnect")?;

        Ok(Self {
            server,
            nodes,
            reconnect_times,
            reconnect_interval,
            add_tag_retry_times,
            add_tag_retry_interval,
            failed_reads_to_force_reconnect,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

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

        let dsn = Dsn::from_str("opc://localhost/subject?reconnect_times=10&reconnect_interval=2000&add_tag_retry_times=20&add_tag_retry_interval=1000&failed_reads_to_force_reconnect=30").unwrap();
        let config = DaConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("subject", config.server);
        assert_eq!(vec!["localhost"], config.nodes);
        assert_eq!(Some(10), config.reconnect_times);
        assert_eq!(Some(2000), config.reconnect_interval);
        assert_eq!(Some(20), config.add_tag_retry_times);
        assert_eq!(Some(1000), config.add_tag_retry_interval);
        assert_eq!(Some(30), config.failed_reads_to_force_reconnect);
    }
}
