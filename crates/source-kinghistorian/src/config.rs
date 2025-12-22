use anyhow::Ok;
use chrono::{DateTime, Local};
use faststr::FastStr;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use taos::Dsn;
use taosx_core::utils::{parse_duration_in_dsn, parse_key_in_dsn, parse_local_datetime_in_dsn};

#[derive(Clone, Serialize, Deserialize)]
pub struct KingHistConfig {
    pub connect: KingHistConnectConfig, // KingHistorian 连接配置
    pub csv_path: Option<PathBuf>,      // csv 配置文件路径
    pub csv_content: Option<FastStr>,   // csv 配置文件内容
    pub mode: KingHistMode,             // history or realtime
    #[serde(flatten)]
    pub query_criteria: Option<HistQueryCriteria>, // KingHistorian 查询条件，mode 为 History 时有效
    pub min_elapsed: Option<usize>,     // KingHistorian 订阅时的最小间隔时间，单位毫秒
    pub max_retries: usize,             // 最大重试次数
    pub retry_interval_sec: usize,      // 重试间隔时间，单位秒
}

impl std::fmt::Debug for KingHistConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("KingHistConfig");
        if let Some(ref csv_path) = self.csv_path {
            ds.field("csv_path", csv_path);
        }
        ds.field("mode", &self.mode);
        if let Some(ref history) = self.query_criteria {
            ds.field("start", &history.start)
                .field("end", &history.end)
                .field("time_range", &history.time_range)
                .field("restro", &history.restro)
                .field("interval", &history.interval);
        }
        if let Some(ref min_elapsed) = self.min_elapsed {
            ds.field("min_elapsed", min_elapsed);
        }
        ds.field("max_retries", &self.max_retries)
            .field("retry_interval", &self.retry_interval_sec);
        ds.finish()
    }
}

const DEFAULT_TIME_RANGE: Duration = Duration::from_secs(24 * 3600); // 1 day
const DEFAULT_RESTRO: Duration = Duration::ZERO; // no restro
const DEFAULT_INTERVAL: usize = 1000; // 1000 ms

impl KingHistConfig {
    pub fn try_from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let connect = KingHistConnectConfig::try_from(dsn)?;
        // csv config
        let (csv_path, csv_content) = crate::csv::parse_csv(dsn)?;
        // max_retries
        let max_retries = parse_key_in_dsn::<usize>(dsn, "max_retries")?.unwrap_or(10);
        // retry_interval
        let retry_interval_sec = parse_key_in_dsn::<usize>(dsn, "retry_interval")?.unwrap_or(5);
        // mode
        let mode = parse_key_in_dsn::<String>(dsn, "mode")?
            .map(|m| KingHistMode::try_from(m.as_str()))
            .transpose()?
            .ok_or(anyhow::anyhow!("mode is required"))?;

        match mode {
            KingHistMode::History => {
                let history = HistQueryCriteria::from_dsn(dsn)?;
                Ok(Self {
                    connect,
                    csv_path,
                    csv_content,
                    mode,
                    query_criteria: Some(history),
                    min_elapsed: None,
                    max_retries,
                    retry_interval_sec,
                })
            }
            KingHistMode::RealTime => {
                let min_elapsed = parse_key_in_dsn::<usize>(dsn, "min_elapsed")?.unwrap_or(1000);
                Ok(Self {
                    connect,
                    csv_path,
                    csv_content,
                    mode,
                    query_criteria: None,
                    min_elapsed: Some(min_elapsed),
                    max_retries,
                    retry_interval_sec,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KingHistConnectConfig {
    pub host: String,            // kinghistorian 主机地址
    pub port: u16,               // kinghistorian 端口
    pub username: String,        // kinghistorian 用户名
    pub password: String,        // kinghistorian 密码
    pub timeout_ms: Option<u32>, // kinghistorian 的连接超时，单位毫秒
}

impl TryFrom<&Dsn> for KingHistConnectConfig {
    type Error = anyhow::Error;

    /// Extract connection info from DSN
    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        let addr = dsn
            .addresses
            .first()
            .ok_or_else(|| anyhow::anyhow!("invalid dsn (missing host/port): {}", dsn))?;
        let host = addr
            .host
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("invalid dsn (missing host): {}", dsn))?
            .to_string();
        let port = addr.port.unwrap_or(5678);
        let username = dsn
            .username
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("invalid dsn (missing username): {}", dsn))?
            .to_string();
        let password = dsn
            .password
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("invalid dsn (missing password): {}", dsn))?
            .to_string();
        let timeout_ms = parse_key_in_dsn::<u32>(dsn, "conn_timeout")?.map(|sec| sec * 1000);
        Ok(Self {
            host,
            port,
            username,
            password,
            timeout_ms,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistQueryCriteria {
    pub start: DateTime<Local>, // 开始时间
    pub end: DateTime<Local>,   // 结束时间
    pub time_range: Duration,   // 每次查询的步长
    pub restro: Duration,       // 回溯时间
    pub interval: usize,        // 两次查询之间的间隔时间，单位毫秒
}

impl HistQueryCriteria {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let start = parse_local_datetime_in_dsn(dsn, "start")?
            .ok_or(anyhow::anyhow!("start is required"))?;
        let end = parse_local_datetime_in_dsn(dsn, "end")?.unwrap_or(Local::now());
        let time_range = parse_duration_in_dsn(dsn, "time_range")?.unwrap_or(DEFAULT_TIME_RANGE);
        let restro = parse_duration_in_dsn(dsn, "restro")?.unwrap_or(DEFAULT_RESTRO);
        let interval = parse_key_in_dsn::<usize>(dsn, "interval")?.unwrap_or(DEFAULT_INTERVAL);
        Ok(Self {
            start,
            end,
            time_range,
            restro,
            interval,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KingHistMode {
    History,
    RealTime,
}

impl TryFrom<&str> for KingHistMode {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "history" => Ok(KingHistMode::History),
            "realtime" => Ok(KingHistMode::RealTime),
            _ => Err(anyhow::anyhow!("invalid mode: {}", value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use taos::{Dsn, IntoDsn};

    #[test]
    fn test_kinghist_config_from_dsn() {
        // given
        let dsn: Dsn = "kinghist://sa:sa@127.0.0.1:5678?conn_timeout=5&mode=history&start=2023-10-01T00:00:00Z&end=2023-10-02T00:00:00Z&time_range=1h&restro=10m&interval=500"
            .into_dsn().unwrap();

        // when
        let config = KingHistConfig::try_from_dsn(&dsn).unwrap();

        // then
        assert_eq!(config.mode, KingHistMode::History);
        assert_eq!(config.connect.timeout_ms, Some(5000));
        let history = config.query_criteria.unwrap();
        assert_eq!(history.interval, 500);
        assert_eq!(history.time_range, Duration::from_secs(3600));
        assert_eq!(history.restro, Duration::from_secs(600));
        assert_eq!(
            history.start,
            DateTime::<Local>::from_str("2023-10-01T00:00:00Z").unwrap()
        );
        assert_eq!(
            history.end,
            DateTime::<Local>::from_str("2023-10-02T00:00:00Z").unwrap()
        );
    }
}
