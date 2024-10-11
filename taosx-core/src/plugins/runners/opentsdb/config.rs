use crate::runners::config::PerformanceConfig;
use itertools::Itertools;
use taos::Dsn;

#[derive(Debug, serde::Serialize)]
pub struct OpentsdbConfig {
    // the datasource config
    pub opents: ConnectionConfig,
    // the addr for connector to agent
    pub taosx: Option<TaosxConfig>,
    // the task config
    pub task: Option<TaskConfig>,
    // the performance config
    pub performance: Option<PerformanceConfig>,
}

impl OpentsdbConfig {
    pub fn from(dsn: &Dsn, ipc: u16) -> anyhow::Result<Self> {
        if dsn.driver != "opentsdb" {
            return Err(anyhow::anyhow!("invalid driver: {}", dsn.driver));
        }

        Ok(Self {
            opents: ConnectionConfig::from_dsn(dsn)?,
            taosx: Some(TaosxConfig::new(ipc)),
            task: Some(TaskConfig::from_dsn(dsn)?),
            performance: Some(PerformanceConfig::from_dsn(dsn)?),
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ConnectionConfig {
    pub url: String,
}

impl ConnectionConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let host = dsn
            .addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or_else(|| anyhow::anyhow!("host is required"))?;
        let port = dsn
            .addresses
            .first()
            .and_then(|addr| addr.port)
            .ok_or_else(|| anyhow::anyhow!("port is required"))?;
        let protocol = dsn.protocol.as_deref().unwrap_or("http");
        if protocol != "http" && protocol != "https" {
            return Err(anyhow::anyhow!("invalid protocol: {}", protocol));
        }

        Ok(ConnectionConfig {
            url: format!("{}://{}:{}/", protocol, host, port),
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct TaosxConfig {
    pub host: String,
    pub port: u16,
}

impl TaosxConfig {
    pub fn new(port: u16) -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct TaskConfig {
    pub mode: String,
    pub metrics: Vec<String>,
    #[serde(rename = "beginTime")]
    pub begin_time: String,
    #[serde(rename = "endTime")]
    pub end_time: Option<String>,
    pub breakpoints: Option<String>,
    #[serde(rename = "logLevel")]
    log_level: Option<String>,
}

impl TaskConfig {
    fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self {
            mode: dsn
                .params
                .get("mode")
                .unwrap_or(&"normal".to_string())
                .to_string(),
            metrics: dsn
                .params
                .get("metrics")
                .unwrap_or(&"".to_string())
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect_vec(),
            begin_time: dsn
                .params
                .get("beginTime")
                .ok_or(anyhow::anyhow!("beginTime is required"))?
                .to_string(),
            end_time: dsn.params.get("endTime").map(|s| s.to_string()),
            breakpoints: dsn.params.get("breakpoints").map(|s| s.to_string()),
            log_level: dsn.get("log_level").map(|s| s.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_opentsdb_config_from() {
        let dsn =
            Dsn::from_str("opentsdb://127.0.0.1:6060/?beginTime=2023-11-22T12:11:22Z").unwrap();
        let config = OpentsdbConfig::from(&dsn, 6061).unwrap();
        assert_eq!("http://127.0.0.1:6060/", config.opents.url);
        let taosx = config.taosx.unwrap();
        assert_eq!("127.0.0.1:6061", format!("{}:{}", taosx.host, taosx.port));
        let task = config.task.unwrap();
        assert_eq!("normal", task.mode);
        assert_eq!(Vec::<String>::new(), task.metrics);
        assert_eq!("2023-11-22T12:11:22Z", task.begin_time);
        assert_eq!(None, task.end_time);

        let dsn = Dsn::from_str("invalid://").unwrap();
        let config = OpentsdbConfig::from(&dsn, 6061);
        assert!(config.is_err());
        assert_eq!("invalid driver: invalid", config.err().unwrap().to_string());
    }

    #[test]
    fn test_connection_config_from_dsn() {
        let dsn = Dsn::from_str("opentsdb://127.0.0.1:6060").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn).unwrap();
        assert_eq!("http://127.0.0.1:6060/", config.url);

        let dsn = Dsn::from_str("opentsdb://").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("host is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("opentsdb://127.0.0.1").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("port is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("opentsdb+https://127.0.0.1:6060").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn).unwrap();
        assert_eq!("https://127.0.0.1:6060/", config.url);
    }

    #[test]
    fn test_taosx_config_new() {
        let config = TaosxConfig::new(6060);
        assert_eq!("127.0.0.1", config.host);
        assert_eq!(6060, config.port);
    }

    #[test]
    fn test_task_config_from_dsn() {
        let dsn = Dsn::from_str("opentsdb://?mode=abc&metrics=a,b,c&beginTime=2023-10-10 12:01:02.123&endTime=2023-10-11 12:00:00.123").unwrap();
        let config = TaskConfig::from_dsn(&dsn).unwrap();
        assert_eq!("abc", config.mode);
        assert_eq!(vec!["a", "b", "c"], config.metrics);
        assert_eq!("2023-10-10 12:01:02.123", config.begin_time);
        assert_eq!(Some("2023-10-11 12:00:00.123".to_string()), config.end_time);

        let dsn = Dsn::from_str("opentsdb://").unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("beginTime is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("opentsdb://?beginTime=2023-10-10 12:01:02.123").unwrap();
        let config = TaskConfig::from_dsn(&dsn).unwrap();
        assert_eq!("normal", config.mode);
        assert_eq!(Vec::<String>::new(), config.metrics);
        assert_eq!("2023-10-10 12:01:02.123", config.begin_time);
        assert_eq!(None, config.end_time);
    }
}
