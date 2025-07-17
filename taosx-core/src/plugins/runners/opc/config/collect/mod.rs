use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PersistDataConfig {
    pub dir: Option<PathBuf>,
}

impl PersistDataConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Self>> {
        let enable = parse_simple_params(dsn, "persist_data_enable")?.is_some_and(|v| v);
        if !enable {
            return Ok(None);
        }

        let dir = parse_simple_params(dsn, "persist_data_dir")?;
        Ok(Some(Self { dir }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectConfig {
    pub interval: Option<i64>,
    pub limit: Option<i64>,
    pub ua: Option<UaCollectConfig>,
    pub da: Option<DaCollectConfig>,
    pub persist_data: Option<PersistDataConfig>,
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
                persist_data: PersistDataConfig::from_dsn(dsn)?,
                dump: DumpConfig::from_dsn(dsn, task_id)?,
            },
            OpcType::OPCDA => Self {
                interval: Self::parse_interval(dsn)?,
                limit: Self::parse_limit(dsn)?,
                ua: None,
                da: Some(DaCollectConfig::from_dsn(dsn).await?),
                persist_data: PersistDataConfig::from_dsn(dsn)?,
                dump: DumpConfig::from_dsn(dsn, task_id)?,
            },
            OpcType::FAKE => Self {
                interval: None,
                limit: None,
                ua: None,
                da: None,
                persist_data: None,
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

fn parse_simple_params<T>(dsn: &Dsn, key: &str) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    dsn.get(key)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse::<T>()
                .with_context(|| format!("invalid {key}: `{v}`"))
        })
        .transpose()
}

/// 从 dsn 的参数 ua.nodes/da.tags 中解析出
pub async fn parse_opc_node_ids(dsn: &Dsn, param_key: &str) -> anyhow::Result<Vec<String>> {
    let param_val = dsn
        .params
        .get(param_key)
        .and_then(|s| if s.is_empty() { None } else { Some(s) })
        .ok_or(anyhow::anyhow!(""))?;

    let mut node_ids = vec![];
    for node in param_val
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let node_id = node.split_once("::").map(|(id, _)| id).unwrap_or(node);
        node_ids.push(node_id.to_string());
    }

    Ok(node_ids)
}

// pub fn get_string_vec_from_param_or_file_for_opc(
//     dsn: &mut Dsn,
//     key: &str,
// ) -> Result<Vec<String>, String> {
//     if let Some(nodes) = dsn.remove(key) {
//         let mut rdr = ReaderBuilder::new()
//             .delimiter(b',')
//             .from_reader(nodes.as_bytes());
//         let header = rdr.headers().map_err(|err| err.to_string())?;
//         let (files, mut node_config): (Vec<_>, Vec<_>) = header
//             .into_iter()
//             // .split(",")
//             .map(|s| s.trim())
//             .filter(|s| !s.is_empty())
//             .map(|s| s.to_string())
//             .partition(|v| v.starts_with("@"));
//         // dbg!(&files, &node_config);
//         for file in files {
//             tracing::info!(
//                 "current log: {}",
//                 std::env::current_dir().unwrap().to_str().unwrap()
//             );
//             let f = std::fs::File::open(&file[1..]);
//             if f.is_err() {
//                 tracing::warn!(
//                     "file: {} read error, cause: {}",
//                     &file[1..],
//                     f.err().unwrap()
//                 );
//                 continue;
//                 // return Err("file read error".to_string());
//             }
//             let buf = std::io::BufReader::new(f.unwrap());
//             let mut file_data = buf.lines().collect_vec();
//             // remove header
//             if file_data.remove(0).is_err() {
//                 tracing::warn!("file: {} content length < 1", file);
//             }
//
//             node_config.extend(
//                 file_data
//                     .iter()
//                     .filter_map(|r| r.as_ref().ok())
//                     .map(|s| s.replace(",", "::")),
//             );
//         }
//         if node_config.is_empty() {
//             tracing::warn!("node config is empty");
//             // return Err(format!("node config set but is empty: {nodes}"));
//         }
//         return Ok(node_config);
//     }
//     // tracing::warn!("node config is empty");
//     Err("Nodes not set".to_string())
// }

#[cfg(test)]
mod tests {
    use super::*;
    use taos::IntoDsn;

    #[tokio::test]
    async fn test_parse_opc_node_ids_in_dsn() {
        // given
        let dsn = "opcda://?ua.nodes=ns=3;i=1001,ns=3;i=1003"
            .into_dsn()
            .unwrap();
        // when
        let node_ids = parse_opc_node_ids(&dsn, "ua.nodes").await.unwrap();
        // then
        assert_eq!(node_ids.len(), 2);
        assert_eq!(node_ids[0], "ns=3;i=1001");
        assert_eq!(node_ids[1], "ns=3;i=1003");

        let points = r#"ns=3;s="数据块_1"."Tag1"::t_3_"数据块_1"_"Tag1""#;
        let mut dsn = Dsn::from_str("opcua://").unwrap();
        dsn.set("ua.nodes", points);
        // when
        let nods = parse_opc_node_ids(&dsn, "ua.nodes").await.unwrap();
        // then
        assert_eq!(nods.len(), 1);
        assert_eq!(nods[0], r#"ns=3;s="数据块_1"."Tag1""#);

        // given
        let dsn = "opcda://?da.tags=tag3::tb3,tag4::tb4".into_dsn().unwrap();
        // when
        let node_ids = parse_opc_node_ids(&dsn, "da.tags").await.unwrap();
        // then
        assert_eq!(node_ids.len(), 2);
        assert_eq!(node_ids[0], "tag3");
        assert_eq!(node_ids[1], "tag4");
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
