use serde::{Deserialize, Serialize};
use taos::Dsn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportConfig {
    pub remote: String,
    pub concurrent: Option<i64>,
    pub batch_size: Option<i64>,
    pub batch_timeout: Option<i64>,
}

impl ReportConfig {
    pub fn from_dsn(dsn: &Dsn, ipc_port: u16) -> anyhow::Result<Self> {
        Ok(Self {
            remote: format!("127.0.0.1:{ipc_port}"),
            concurrent: Self::parse_concurrent(dsn)?,
            batch_size: Self::parse_batch_size(dsn)?,
            batch_timeout: Self::parse_batch_timeout(dsn)?,
        })
    }

    fn parse_concurrent(dsn: &Dsn) -> anyhow::Result<Option<i64>> {
        dsn.get("concurrent")
            .or(dsn.get("write_concurrency"))
            .map(|v| {
                v.parse::<i64>().map_err(|err| {
                    anyhow::anyhow!("parse concurrent failed, cause: {}", err.to_string())
                })
            })
            .transpose()
    }

    fn parse_batch_size(dsn: &Dsn) -> anyhow::Result<Option<i64>> {
        Ok(dsn
            .params
            .get("batch_size")
            .map(|v| {
                v.parse::<i64>().map_err(|err| {
                    anyhow::anyhow!("parse batch_size failed, cause: {}", err.to_string())
                })
            })
            .transpose()?
            .or(Some(1000)))
    }

    fn parse_batch_timeout(dsn: &Dsn) -> anyhow::Result<Option<i64>> {
        Ok(dsn
            .params
            .get("batch_timeout")
            .map(|v| {
                v.parse::<i64>().map_err(|err| {
                    anyhow::anyhow!("parse batch_timeout failed, cause: {}", err.to_string())
                })
            })
            .transpose()?
            .or(Some(1)))
    }
}

#[cfg(test)]
mod report_config_tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("opc://").unwrap();
        let config = ReportConfig::from_dsn(&dsn, 1234).unwrap();
        assert_eq!("127.0.0.1:1234", config.remote);
        assert_eq!(None, config.concurrent);
        assert_eq!(Some(1000), config.batch_size);
        assert_eq!(Some(1), config.batch_timeout);

        let dsn = Dsn::from_str("opc://?concurrent=10&batch_size=11&batch_timeout=12").unwrap();
        let config = ReportConfig::from_dsn(&dsn, 1234).unwrap();
        assert_eq!("127.0.0.1:1234", config.remote);
        assert_eq!(10, config.concurrent.unwrap());
        assert_eq!(11, config.batch_size.unwrap());
        assert_eq!(12, config.batch_timeout.unwrap());
    }
}
