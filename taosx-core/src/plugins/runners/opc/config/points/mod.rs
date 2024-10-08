use std::str::FromStr;

use crate::runners::opc::config::PointsMode;
use crate::runners::opc::OpcType;
use serde::{Deserialize, Serialize};
use taos::Dsn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointsConfig {
    pub regex: Option<String>,
    pub limit: usize, // always 0
    pub update_mode: Option<UpdateMode>,
    pub update_interval: Option<usize>,

    pub ua: Option<PointsUaConfig>,
    pub da: Option<PointsDaConfig>,
}

impl PointsConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let opc_type = OpcType::from_dsn(dsn)?;

        let (ua, da) = match opc_type {
            OpcType::OPCUA => {
                let ua = PointsUaConfig::from_dsn(dsn)?;
                (Some(ua), None)
            }
            OpcType::OPCDA => {
                let da = PointsDaConfig::from_dsn(dsn);
                (None, da)
            }
            OpcType::FAKE => (None, None),
        };

        let points_mode = PointsMode::from_dsn(dsn)?;
        let (update_mode, update_interval) = match points_mode {
            PointsMode::ByCsv => {
                // default update_mode is Append, default update_interval is 60 seconds
                (Some(UpdateMode::Append), Some(60usize))
            }
            PointsMode::ByCommand => {
                let update_mode = Self::parse_update_mode(dsn)?;
                let update_interval = Self::parse_update_interval(dsn)?;
                (update_mode, update_interval)
            }
        };

        Ok(Self {
            regex: Self::parse_regex(dsn),
            limit: 0,
            update_mode,
            update_interval,
            ua,
            da,
        })
    }

    fn parse_regex(dsn: &Dsn) -> Option<String> {
        dsn.params.get("pattern").map(|v| v.to_string())
    }

    pub fn parse_update_mode(dsn: &Dsn) -> anyhow::Result<Option<UpdateMode>> {
        dsn.params
            .get("update_mode")
            .map(|v| v.parse::<UpdateMode>())
            .transpose()
    }

    fn parse_update_interval(dsn: &Dsn) -> anyhow::Result<Option<usize>> {
        dsn.params
            .get("update_interval")
            .map(|v| {
                v.parse::<usize>().map_err(|err| {
                    anyhow::anyhow!("invalid update_interval: {}, cause: {}", v, err.to_string())
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod points_config_tests {
    use super::*;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("opcua://?").unwrap();
        let config = PointsConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.regex, None);
        assert_eq!(config.limit, 0);
        assert_eq!(config.update_mode, None);
        assert_eq!(config.update_interval, None);
        assert!(config.da.is_none());

        let ua = config.ua.unwrap();
        assert_eq!(ua.root, None);
        assert_eq!(ua.namespaces, None);
    }

    #[test]
    fn test_parse_update_mode() {
        let dsn = Dsn::from_str("opc://?update_mode=none").unwrap();
        let points_config = PointsConfig::parse_update_mode(&dsn).unwrap();
        assert_eq!(points_config, Some(UpdateMode::None));

        let dsn = Dsn::from_str("opc://?update_mode=append").unwrap();
        let points_config = PointsConfig::parse_update_mode(&dsn).unwrap();
        assert_eq!(points_config, Some(UpdateMode::Append));

        let dsn = Dsn::from_str("opc://?update_mode=update").unwrap();
        let points_config = PointsConfig::parse_update_mode(&dsn).unwrap();
        assert_eq!(points_config, Some(UpdateMode::Update));

        let dsn = Dsn::from_str("opc://").unwrap();
        let points_config = PointsConfig::parse_update_mode(&dsn).unwrap();
        assert_eq!(points_config, None);

        let dsn = Dsn::from_str("opc://?update_mode=invalid").unwrap();
        let points_config = PointsConfig::parse_update_mode(&dsn);
        assert!(points_config.is_err());
        assert_eq!(
            points_config.err().unwrap().to_string(),
            "invalid update mode: invalid, must be none/append/update".to_string()
        );
    }

    #[test]
    fn test_parse_update_interval() {
        let dsn = Dsn::from_str("opc://?update_interval=10").unwrap();
        let points_config = PointsConfig::parse_update_interval(&dsn).unwrap();
        assert_eq!(points_config, Some(10));

        let dsn = Dsn::from_str("opc://").unwrap();
        let points_config = PointsConfig::parse_update_interval(&dsn).unwrap();
        assert_eq!(points_config, None);

        let dsn = Dsn::from_str("opc://?update_interval=invalid").unwrap();
        let points_config = PointsConfig::parse_update_interval(&dsn);
        assert!(points_config.is_err());
        assert_eq!(
            points_config.err().unwrap().to_string(),
            "invalid update_interval: invalid, cause: invalid digit found in string".to_string()
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointsUaConfig {
    root: Option<String>,
    namespaces: Option<Vec<u16>>,
}

impl PointsUaConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let root = dsn.params.get("root").and_then(|v| {
            if v.is_empty() {
                return None;
            }
            Some(v.to_string())
        });

        let namespaces = dsn
            .params
            .get("namespaces")
            .and_then(|v| {
                if v.is_empty() {
                    return None;
                }
                let namespaces = v
                    .split(',')
                    .map(|v| {
                        v.parse::<u16>().map_err(|err| {
                            anyhow::anyhow!("invalid namespaces: {}, cause: {}", v, err.to_string())
                        })
                    })
                    .collect::<Result<Vec<u16>, anyhow::Error>>();
                Some(namespaces)
            })
            .transpose()?;

        Ok(Self { root, namespaces })
    }
}

#[cfg(test)]
mod points_ua_config_tests {
    use super::*;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("opc://?root=Root&namespaces=1,2,3").unwrap();
        let points_ua_config = PointsUaConfig::from_dsn(&dsn).unwrap();
        assert_eq!(points_ua_config.root, Some("Root".to_string()));
        assert_eq!(points_ua_config.namespaces, Some(vec![1, 2, 3]));

        let dsn = Dsn::from_str("opc://?root=Root&namespaces=").unwrap();
        let points_ua_config = PointsUaConfig::from_dsn(&dsn).unwrap();
        assert_eq!(points_ua_config.root, Some("Root".to_string()));
        assert_eq!(points_ua_config.namespaces, None);

        let dsn = Dsn::from_str("opc://?root=Root").unwrap();
        let points_ua_config = PointsUaConfig::from_dsn(&dsn).unwrap();
        assert_eq!(points_ua_config.root, Some("Root".to_string()));
        assert_eq!(points_ua_config.namespaces, None);

        let dsn = Dsn::from_str("opc://?root=&namespaces=1,2,3").unwrap();
        let points_ua_config = PointsUaConfig::from_dsn(&dsn).unwrap();
        assert_eq!(points_ua_config.root, None);
        assert_eq!(points_ua_config.namespaces, Some(vec![1, 2, 3]));

        let dsn = Dsn::from_str("opc://?root=&namespaces=").unwrap();
        let points_ua_config = PointsUaConfig::from_dsn(&dsn).unwrap();
        assert_eq!(points_ua_config.root, None);
        assert_eq!(points_ua_config.namespaces, None);

        let dsn = Dsn::from_str("opc://?root=&namespaces=invalid").unwrap();
        let points_ua_config = PointsUaConfig::from_dsn(&dsn);
        assert!(points_ua_config.is_err());
        assert_eq!(
            points_ua_config.err().unwrap().to_string(),
            "invalid namespaces: invalid, cause: invalid digit found in string".to_string()
        );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointsDaConfig {
    access_path: Option<Vec<String>>,
}

impl PointsDaConfig {
    pub fn from_dsn(dsn: &Dsn) -> Option<Self> {
        let access_path = dsn.params.get("root").and_then(|v| {
            if v.is_empty() {
                return None;
            }
            let access_path = v.split(".").map(|v| v.to_string()).collect::<Vec<String>>();
            Some(access_path)
        });

        Some(Self { access_path })
    }
}

#[cfg(test)]
mod points_da_config_tests {
    use super::*;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("opc://?root=Root").unwrap();
        let points_da_config = PointsDaConfig::from_dsn(&dsn).unwrap();
        assert_eq!(points_da_config.access_path, Some(vec!["Root".to_string()]));

        let dsn = Dsn::from_str("opc://?root=Root.Child").unwrap();
        let points_da_config = PointsDaConfig::from_dsn(&dsn).unwrap();
        assert_eq!(
            points_da_config.access_path,
            Some(vec!["Root".to_string(), "Child".to_string()])
        );

        let dsn = Dsn::from_str("opc://?root=").unwrap();
        let points_da_config = PointsDaConfig::from_dsn(&dsn).unwrap();
        assert_eq!(points_da_config.access_path, None);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UpdateMode {
    None,
    Append,
    Update,
}

impl FromStr for UpdateMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(UpdateMode::None),
            "append" => Ok(UpdateMode::Append),
            "update" => Ok(UpdateMode::Update),
            _ => Err(anyhow::anyhow!(
                "invalid update mode: {}, must be none/append/update",
                s
            )),
        }
    }
}
