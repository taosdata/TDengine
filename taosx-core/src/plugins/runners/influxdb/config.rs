use itertools::Itertools;
use taos::Dsn;

use crate::runners::config::PerformanceConfig;

pub const INFLUXDB_V1: [&str; 2] = ["1.7", "1.8"];
pub const INFLUXDB_V2: [&str; 8] = ["2.0", "2.1", "2.2", "2.3", "2.4", "2.5", "2.6", "2.7"];

#[derive(Debug, serde::Serialize)]
pub struct InfluxdbConfig {
    // the datasource config
    pub influx: ConnectionConfig,
    // the addr for connector to agent
    taosx: Option<TaosxConfig>,
    // the task config
    task: Option<TaskConfig>,
    // the performance config
    pub performance: Option<PerformanceConfig>,
}

impl InfluxdbConfig {
    pub fn from(dsn: &Dsn, ipc: u16) -> anyhow::Result<Self> {
        if dsn.driver != "influxdb" {
            return Err(anyhow::anyhow!("invalid driver: {}", dsn.driver));
        }
        // the datasource config
        let connect = ConnectionConfig::from_dsn(dsn)?;
        // the addr for connector to agent
        let taosx_host = String::from("127.0.0.1");
        let taosx_port = ipc;
        let taosx = TaosxConfig {
            taosx_host,
            taosx_port,
        };

        // the task config
        let task = TaskConfig::from_dsn(dsn)?;

        // the performance config
        let performance = PerformanceConfig::from_dsn(dsn)?;

        Ok(Self {
            influx: connect,
            taosx: Some(taosx),
            task: Some(task),
            performance: Some(performance),
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ConnectionConfig {
    pub url: String,
    pub version: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    #[serde(rename = "orgId")]
    pub org_id: Option<String>,
    #[serde(rename = "addDbrp")]
    pub add_dbrp: bool,
}

impl ConnectionConfig {
    /// On version 1.x, only username/password mode can be used
    /// On version 2.x, only access token mode can be used.
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        if dsn.driver != "influxdb" {
            return Err(anyhow::anyhow!("invalid driver: {}", dsn.driver));
        }

        let version = dsn
            .params
            .get("version")
            .ok_or(anyhow::anyhow!("version is required"))?;

        let mut username = None;
        let mut password = None;
        let mut org_id = None;
        let mut token = None;
        if INFLUXDB_V1.contains(&version.as_str()) {
            username = Option::from(
                dsn.params
                    .get("username")
                    .ok_or(anyhow::anyhow!("username is required"))?
                    .to_string(),
            );
            password = Option::from(
                dsn.params
                    .get("password")
                    .ok_or(anyhow::anyhow!("password is required"))?
                    .to_string(),
            );
        } else if INFLUXDB_V2.contains(&version.as_str()) {
            org_id = Option::from(
                dsn.params
                    .get("orgId")
                    .ok_or(anyhow::anyhow!("orgId is required"))?
                    .to_string(),
            );
            token = Option::from(
                dsn.params
                    .get("token")
                    .ok_or(anyhow::anyhow!("token is required"))?
                    .to_string(),
            );
        } else {
            return Err(anyhow::anyhow!("invalid version: {}", version));
        }
        let add_dbrp = dsn
            .params
            .get("addDbrp")
            .map(|s| s.as_str() == "true")
            .unwrap_or(false);

        let influx = ConnectionConfig {
            url: Self::parse_url(dsn)?,
            version: version.to_string(),
            username,
            password,
            token,
            org_id,
            add_dbrp,
        };

        Ok(influx)
    }
    fn parse_url(dsn: &Dsn) -> anyhow::Result<String> {
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

        Ok(format!("{}://{}:{}/", protocol, host, port))
    }
}

#[derive(Debug, serde::Serialize)]
struct TaosxConfig {
    #[serde(rename = "host")]
    pub taosx_host: String,
    #[serde(rename = "port")]
    pub taosx_port: u16,
}

#[derive(Debug, serde::Serialize)]
struct TaskConfig {
    mode: String,
    bucket: String,
    measurements: Vec<String>,
    #[serde(rename = "beginTime")]
    begin_time: String,
    #[serde(rename = "endTime")]
    end_time: Option<String>,
    breakpoints: Option<String>,
    #[serde(rename = "logLevel")]
    log_level: Option<String>,
}

impl TaskConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(TaskConfig {
            mode: dsn
                .params
                .get("mode")
                .unwrap_or(&"normal".to_string())
                .to_string(),
            bucket: dsn
                .params
                .get("bucket")
                .ok_or(anyhow::anyhow!("bucket is required"))?
                .to_string(),
            measurements: dsn
                .params
                .get("measurements")
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
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_influxdb_config_from_dsn() {
        let dsn = Dsn::from_str("invalid://").unwrap();
        let config = InfluxdbConfig::from(&dsn, 0);
        assert!(config.is_err());
        assert_eq!("invalid driver: invalid", config.unwrap_err().to_string());
    }

    #[test]
    fn test_task_config_from_dsn() {
        let dsn = Dsn::from_str("influxdb://").unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("bucket is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("influxdb://?bucket=abc").unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("beginTime is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("influxdb://?bucket=abc&beginTime=2023-11-11T12:00:00Z").unwrap();
        let config = TaskConfig::from_dsn(&dsn).unwrap();
        assert_eq!("normal", config.mode);
        assert_eq!("abc", config.bucket);
        assert_eq!(Vec::<String>::new(), config.measurements);
        assert_eq!("2023-11-11T12:00:00Z", config.begin_time);
        assert_eq!(None, config.end_time);
        assert_eq!(None, config.breakpoints);

        let dsn = Dsn::from_str(
            "influxdb://?bucket=abc&beginTime=2023-11-11T12:00:00Z&measurements=m1,m2,m3&breakpoints=abc",
        ).unwrap();
        let config = TaskConfig::from_dsn(&dsn).unwrap();
        assert_eq!("normal", config.mode);
        assert_eq!("abc", config.bucket);
        assert_eq!(vec!["m1", "m2", "m3"], config.measurements);
        assert_eq!("2023-11-11T12:00:00Z", config.begin_time);
        assert_eq!(None, config.end_time);
        assert_eq!("abc", config.breakpoints.unwrap());
    }

    #[test]
    fn test_parse_url() {
        let dsn = Dsn::from_str("influxdb://").unwrap();
        let url = ConnectionConfig::parse_url(&dsn);
        assert!(url.is_err());
        assert_eq!("host is required", url.unwrap_err().to_string());

        let dsn = Dsn::from_str("influxdb://192.168.1.107").unwrap();
        let url = ConnectionConfig::parse_url(&dsn);
        assert!(url.is_err());
        assert_eq!("port is required", url.unwrap_err().to_string());

        let dsn = Dsn::from_str("influxdb://192.168.1.107:8086").unwrap();
        let url = ConnectionConfig::parse_url(&dsn).unwrap();
        assert_eq!("http://192.168.1.107:8086/", url);

        let dsn = Dsn::from_str("influxdb+https://192.168.1.107:8086").unwrap();
        let url = ConnectionConfig::parse_url(&dsn).unwrap();
        assert_eq!("https://192.168.1.107:8086/", url);

        let dsn = Dsn::from_str("influxdb+invalid://192.168.1.107:8086").unwrap();
        let url = ConnectionConfig::parse_url(&dsn);
        assert!(url.is_err());
        assert_eq!("invalid protocol: invalid", url.unwrap_err().to_string());
    }

    #[test]
    fn test_connection_config_from_dsn() {
        let dsn = Dsn::from_str("influxdb://127.0.0.1:8086").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("version is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("influxdb://127.0.0.1:8086?version=1.7").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("username is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("influxdb://127.0.0.1:8086/?version=1.7&username=root").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("password is required", config.unwrap_err().to_string());

        let dsn =
            Dsn::from_str("influxdb://127.0.0.1:8086/?version=1.7&username=root&password=abc")
                .unwrap();
        let config = ConnectionConfig::from_dsn(&dsn).unwrap();
        assert_eq!("1.7", config.version);
        assert_eq!("root", config.username.unwrap());
        assert_eq!("abc", config.password.unwrap());
        assert_eq!(None, config.org_id);
        assert_eq!(None, config.token);

        let dsn = Dsn::from_str("influxdb://127.0.0.1:8086/?version=2.7").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("orgId is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("influxdb://127.0.0.1:8086/?version=2.7&orgId=123").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("token is required", config.unwrap_err().to_string());

        let dsn =
            Dsn::from_str("influxdb://127.0.0.1:8086/?version=2.7&orgId=123&token=abc").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn).unwrap();
        assert_eq!("2.7", config.version);
        assert_eq!(None, config.username);
        assert_eq!(None, config.password);
        assert_eq!("123", config.org_id.unwrap());
        assert_eq!("abc", config.token.unwrap());

        let dsn =
            Dsn::from_str("influxdb://127.0.0.1:8086/?version=3.0&orgId=123&token=abc").unwrap();
        let config = ConnectionConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("invalid version: 3.0", config.unwrap_err().to_string());
    }
}
