use serde::{Deserialize, Serialize};
use taos::Dsn;

#[derive(Debug, Serialize, Deserialize)]
pub struct DumpConfig {
    pub enable: bool,
    pub path: Option<String>,
    pub keep: Option<usize>,
}

impl DumpConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Self>> {
        let enable = Self::parse_enable(dsn)?;
        let dump_config = match enable {
            None => { None }
            Some(dump_enable) => {
                if dump_enable {
                    let path = dsn.params
                        .get("path")
                        .map(|v| v.to_string())
                        .ok_or(anyhow::anyhow!("path is required if dump is enabled"))?;
                    let keep = dsn.params
                        .get("keep")
                        .map(|v| v.parse::<usize>().map_err(|err| {
                            anyhow::anyhow!("parse keep failed, cause: {}", err.to_string())
                        }))
                        .transpose()?
                        .ok_or(anyhow::anyhow!("keep is required if dump is enabled"))?;
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
        Ok(dsn.params
            .get("enable")
            .map(|v| {
                v.parse::<bool>().map_err(|err| anyhow::anyhow!("parse enable failed, cause: {}", err.to_string()))
            })
            .transpose()?
        )
    }
}

#[cfg(test)]
mod tests{
    use std::str::FromStr;
    use taos::Dsn;
    use super::*;

    #[test]
    fn test_from_dsn(){
        let dsn = Dsn::from_str("opc://").unwrap();
        let config = DumpConfig::from_dsn(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("opc://?enable=false").unwrap();
        let config = DumpConfig::from_dsn(&dsn).unwrap().unwrap();
        assert_eq!(false, config.enable);
        assert_eq!(None, config.path);
        assert_eq!(None, config.keep);

        let dsn = Dsn::from_str("opc://?enable=true").unwrap();
        let config = DumpConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("path is required if dump is enabled", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("opc://?enable=true&path=abc").unwrap();
        let config = DumpConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("keep is required if dump is enabled", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("opc://?enable=true&path=abc&keep=abc").unwrap();
        let config = DumpConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("parse keep failed, cause: invalid digit found in string", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("opc://?enable=true&path=abc&keep=123").unwrap();
        let config = DumpConfig::from_dsn(&dsn).unwrap().unwrap();
        assert_eq!(true, config.enable);
        assert_eq!("abc",config.path.unwrap());
        assert_eq!(123, config.keep.unwrap());
    }
}