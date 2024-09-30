use std::str::FromStr;

use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::runners::opc::config::collect::da::DaCollectConfig;
use crate::runners::opc::config::collect::dump::DumpConfig;
use crate::runners::opc::config::collect::ua::UaCollectConfig;
use crate::runners::opc::config::OpcType;

pub mod da;
pub mod dump;
pub mod ua;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum CollectMode {
    OBSERVE,
    SUBSCRIBE,
}

impl FromStr for CollectMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "observe" => Ok(Self::OBSERVE),
            "subscribe" => Ok(Self::SUBSCRIBE),
            _ => Err(s.to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectConfig {
    pub interval: Option<i64>,
    pub limit: Option<i64>,
    pub ua: Option<UaCollectConfig>,
    pub da: Option<DaCollectConfig>,
    pub dump: Option<DumpConfig>,
}

impl CollectConfig {
    pub async fn from_dsn(dsn: &Dsn, task_id: Option<i64>) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;
        let collect_config = match opc_type {
            OpcType::OPCUA => Self {
                interval: Self::parse_interval(dsn)?,
                limit: Self::parse_limit(dsn)?,
                ua: Some(UaCollectConfig::from_dsn(dsn).await?),
                da: None,
                dump: DumpConfig::from_dsn(dsn, task_id)?,
            },
            OpcType::OPCDA => Self {
                interval: Self::parse_interval(dsn)?,
                limit: Self::parse_limit(dsn)?,
                ua: None,
                da: Some(DaCollectConfig::from_dsn(dsn).await?),
                dump: DumpConfig::from_dsn(dsn, task_id)?,
            },
            OpcType::FAKE => Self {
                interval: None,
                limit: None,
                ua: None,
                da: None,
                dump: None,
            },
        };
        Ok(collect_config)
    }

    fn parse_interval(dsn: &Dsn) -> anyhow::Result<Option<i64>> {
        dsn.params
            .get("interval")
            .map(|v| {
                v.parse::<i64>().map_err(|err| {
                    anyhow::anyhow!("invalid interval: {}, cause: {}", v, err.to_string())
                })
            })
            .transpose()
    }

    fn parse_limit(dsn: &Dsn) -> anyhow::Result<Option<i64>> {
        dsn.params
            .get("limit")
            .map(|v| {
                v.parse::<i64>().map_err(|err| {
                    anyhow::anyhow!("invalid limit: {}, cause: {}", v, err.to_string())
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let interval = CollectConfig::parse_interval(&dsn).unwrap();
        assert_eq!(None, interval);

        let dsn = Dsn::from_str("opc://?interval=123").unwrap();
        let interval = CollectConfig::parse_interval(&dsn).unwrap();
        assert_eq!(123, interval.unwrap());

        let dsn = Dsn::from_str("opc://?interval=abc").unwrap();
        let interval = CollectConfig::parse_interval(&dsn);
        assert!(interval.is_err());
        assert_eq!(
            "invalid interval: abc, cause: invalid digit found in string",
            interval.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_limit() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let limit = CollectConfig::parse_limit(&dsn).unwrap();
        assert_eq!(None, limit);

        let dsn = Dsn::from_str("opc://?limit=123").unwrap();
        let limit = CollectConfig::parse_limit(&dsn).unwrap();
        assert_eq!(123, limit.unwrap());

        let dsn = Dsn::from_str("opc://?limit=abc").unwrap();
        let limit = CollectConfig::parse_limit(&dsn);
        assert!(limit.is_err());
        assert_eq!(
            "invalid limit: abc, cause: invalid digit found in string",
            limit.unwrap_err().to_string()
        );
    }
}
