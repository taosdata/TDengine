use csv_lib::ReaderBuilder;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::io::BufRead;
use std::str::FromStr;
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
    Observe,
    Subscribe,
}

impl FromStr for CollectMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "observe" => Ok(Self::Observe),
            "subscribe" => Ok(Self::Subscribe),
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

pub fn get_string_vec_from_param_or_file_for_opc(
    dsn: &mut Dsn,
    key: &str,
) -> Result<Vec<String>, String> {
    if let Some(nodes) = dsn.remove(key) {
        let mut rdr = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(nodes.as_bytes());
        let header = rdr.headers().map_err(|err| err.to_string())?;
        let (files, mut node_config): (Vec<_>, Vec<_>) = header
            .into_iter()
            // .split(",")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .partition(|v| v.starts_with("@"));
        // dbg!(&files, &node_config);
        for file in files {
            tracing::info!(
                "current log: {}",
                std::env::current_dir().unwrap().to_str().unwrap()
            );
            let f = std::fs::File::open(&file[1..]);
            if f.is_err() {
                tracing::warn!(
                    "file: {} read error, cause: {}",
                    &file[1..],
                    f.err().unwrap()
                );
                continue;
                // return Err("file read error".to_string());
            }
            let buf = std::io::BufReader::new(f.unwrap());
            let mut file_data = buf.lines().collect_vec();
            // remove header
            if file_data.remove(0).is_err() {
                tracing::warn!("file: {} content length < 1", file);
            }

            node_config.extend(
                file_data
                    .iter()
                    .filter_map(|r| r.as_ref().ok())
                    .map(|s| s.replace(",", "::")),
            );
        }
        if node_config.is_empty() {
            tracing::warn!("node config is empty");
            // return Err(format!("node config set but is empty: {nodes}"));
        }
        return Ok(node_config);
    }
    // tracing::warn!("node config is empty");
    Err("Nodes not set".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::IntoDsn;

    #[test]
    fn test_parse_special_nodes() {
        let mut dsn = format!(
            "opcua://?ua.nodes={}",
            r#""ns=3;s=Special_""!§$%&/()=?`´\+~*'#_-:.;,<>|@^°€µ{[]}::meter_3_Special_""!§$%&/()=?_´\+~*'#_-:_;,<>|@^°€µ{[]}","a::b""#
        ).into_dsn().unwrap();

        let config = get_string_vec_from_param_or_file_for_opc(&mut dsn, "ua.nodes").unwrap();
        assert_eq!(config[0], "ns=3;s=Special_\"!§$%");
    }

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
