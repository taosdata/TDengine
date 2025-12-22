use crate::{runners::opc::OpcType, sink::point::UpdateMode};
use serde::{Deserialize, Serialize};
use taos::Dsn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointsConfig {
    pub limit: usize,               // always 0
    pub regex: Option<String>,      // 正则表达式，匹配 NodeId || BrowseName，兼容旧版本
    pub regex_name: Option<String>, // 正则表达式，匹配 BrowseName
    pub regex_id: Option<String>,   // 正则表达式，匹配 NodeId
    pub ua: Option<PointsUaConfig>, // OPC UA 配置
    pub da: Option<PointsDaConfig>, // OPC DA 配置

    pub update_mode: Option<UpdateMode>, // 点位更新模式
    pub update_interval: Option<usize>,  // 点位更新间隔
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

        let update_mode = dsn
            .params
            .get("update_mode")
            .map(|v| v.parse::<UpdateMode>())
            .transpose()?;
        let update_interval = dsn
            .params
            .get("update_interval")
            .map(|v| {
                v.parse::<usize>().map_err(|err| {
                    anyhow::anyhow!("invalid update_interval: {}, cause: {}", v, err.to_string())
                })
            })
            .transpose()?;

        Ok(Self {
            regex: Self::parse_regex("pattern", dsn),
            regex_id: Self::parse_regex("node_id_pattern", dsn),
            regex_name: Self::parse_regex("browse_name_pattern", dsn),
            limit: 0,
            update_mode,
            update_interval,
            ua,
            da,
        })
    }

    /// 从 dsn 中解析正则表达式类型的参数
    fn parse_regex(param: &str, dsn: &Dsn) -> Option<String> {
        dsn.params.get(param).map(|v| v.to_string())
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
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_from_str_of_update_mode() {
        assert_eq!("none".parse::<UpdateMode>().unwrap(), UpdateMode::None);
        assert_eq!("None".parse::<UpdateMode>().unwrap(), UpdateMode::None);

        assert_eq!("append".parse::<UpdateMode>().unwrap(), UpdateMode::Append);
        assert_eq!("Append".parse::<UpdateMode>().unwrap(), UpdateMode::Append);

        assert_eq!("update".parse::<UpdateMode>().unwrap(), UpdateMode::Update);
        assert_eq!("Update".parse::<UpdateMode>().unwrap(), UpdateMode::Update);

        let update_mode = "invalid".parse::<UpdateMode>();
        assert!(update_mode.is_err());
        assert_eq!(
            update_mode.err().unwrap().to_string(),
            "invalid update mode: invalid, must be none/append/update".to_string()
        );
    }

    #[test]
    fn test_from_dsn_of_points_config() {
        // pattern 不以 _Error 结尾
        let dsn = Dsn::from_str("opcua://?pattern=%5E%28%3F%21%2e_Error%2e%24%29").unwrap();
        let config = PointsConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.regex, Some("^(?!._Error.$)".to_string()));
        assert_eq!(config.regex_id, None);
        assert_eq!(config.regex_name, None);

        // browse_name_pattern 不以 _Error 结尾
        let dsn =
            Dsn::from_str("opcua://?browse_name_pattern=%5E%28%3F%21%2e_Error%2e%24%29").unwrap();
        let config = PointsConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.regex, None);
        assert_eq!(config.regex_id, None);
        assert_eq!(config.regex_name, Some("^(?!._Error.$)".to_string()));

        // node_id_pattern 不以 _Error 结尾
        let dsn = Dsn::from_str("opcua://?node_id_pattern=%5E%28%3F%21%2e_Error%2e%24%29").unwrap();
        let config = PointsConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.regex, None);
        assert_eq!(config.regex_id, Some("^(?!._Error.$)".to_string()));
        assert_eq!(config.regex_name, None);
    }

    #[test]
    fn test_from_dsn_update_mode_and_interval() {
        let dsn = Dsn::from_str("opcda://?update_mode=append&update_interval=60").unwrap();
        let config = PointsConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.update_mode, Some(UpdateMode::Append));
        assert_eq!(config.update_interval, Some(60));

        let dsn = Dsn::from_str("opcda://?update_mode=update&update_interval=bad").unwrap();
        let err = PointsConfig::from_dsn(&dsn).unwrap_err();
        assert!(err.to_string().contains("invalid update_interval"));
    }

    #[test]
    fn test_from_dsn_of_points_ua_config() {
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

    #[test]
    fn test_from_dsn_of_points_da_config() {
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
