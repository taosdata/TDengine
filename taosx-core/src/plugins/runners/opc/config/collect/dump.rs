use serde::{Deserialize, Serialize};
use taos::Dsn;

use crate::get_data_dir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpConfig {
    pub enable: bool,
    pub path: Option<String>,
    pub keep: Option<usize>,
}

impl DumpConfig {
    pub fn from_dsn(dsn: &Dsn, task_id: Option<i64>) -> anyhow::Result<Option<Self>> {
        let enable = Self::parse_enable(dsn)?;
        let dump_config = match enable {
            None => None,
            Some(dump_enable) => {
                if dump_enable {
                    let path = dsn
                        .params
                        .get("path")
                        .or(dsn.get("keep_raw_data_dir"))
                        .map(|v| v.to_string())
                        .or_else(|| {
                            task_id.map(|id| {
                                let path = get_data_dir()
                                    .join("tasks")
                                    .join(format!("{id}"))
                                    .join("rawdata");
                                path.display().to_string()
                            })
                        })
                        .ok_or_else(|| anyhow::anyhow!("path is required if dump is enabled"))?;
                    let keep = dsn
                        .params
                        .get("keep")
                        .or(dsn.get("keep_raw_data_days"))
                        .map(|v| {
                            v.parse::<usize>().map_err(|err| {
                                anyhow::anyhow!("parse keep failed, cause: {}", err.to_string())
                            })
                        })
                        .transpose()?
                        .unwrap_or(1); // Default keep 1 day.
                    Some(DumpConfig {
                        enable: dump_enable,
                        path: Some(path),
                        keep: Some(keep),
                    })
                } else {
                    Some(DumpConfig {
                        enable: dump_enable,
                        path: None,
                        keep: None,
                    })
                }
            }
        };
        Ok(dump_config)
    }

    fn parse_enable(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        dsn.params
            .get("enable")
            .or(dsn.params.get("keep_raw_data"))
            .map(|v| {
                v.parse::<bool>().map_err(|err| {
                    anyhow::anyhow!("parse enable failed, cause: {}", err.to_string())
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let config = DumpConfig::from_dsn(&dsn, None).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("opc://?enable=false").unwrap();
        let config = DumpConfig::from_dsn(&dsn, None).unwrap().unwrap();
        assert!(!config.enable);
        assert_eq!(None, config.path);
        assert_eq!(None, config.keep);

        let dsn = Dsn::from_str("opc://?enable=true").unwrap();
        let config = DumpConfig::from_dsn(&dsn, None);
        assert!(config.is_err());
        assert_eq!(
            "path is required if dump is enabled",
            config.unwrap_err().to_string()
        );

        #[cfg(unix)]
        {
            let dsn = Dsn::from_str("opc://?enable=true").unwrap();
            let config = DumpConfig::from_dsn(&dsn, Some(1)).unwrap().unwrap();
            assert_eq!(config.path.unwrap(), "/var/lib/taos/taosx/tasks/1/rawdata");
        }
        let dsn = Dsn::from_str("opc://?enable=true&path=abc").unwrap();
        let config = DumpConfig::from_dsn(&dsn, None);
        assert_eq!(config.unwrap().unwrap().keep, Some(1));

        let dsn = Dsn::from_str("opc://?enable=true&path=abc&keep=abc").unwrap();
        let config = DumpConfig::from_dsn(&dsn, None);
        assert!(config.is_err());
        assert_eq!(
            "parse keep failed, cause: invalid digit found in string",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("opc://?enable=true&path=abc&keep=123").unwrap();
        let config = DumpConfig::from_dsn(&dsn, None).unwrap().unwrap();
        assert!(config.enable);
        assert_eq!("abc", config.path.unwrap());
        assert_eq!(123, config.keep.unwrap());
    }
}
