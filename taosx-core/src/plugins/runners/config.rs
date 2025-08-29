use taos::Dsn;

use crate::{plugins::config::AdvancedOptions, utils::dsn::parse_simple_params};

#[derive(Debug, serde::Serialize)]
pub struct PerformanceConfig {
    #[serde(rename = "readWindow")]
    pub(crate) read_window: u32,
    #[serde(rename = "delay")]
    pub(crate) delay: u32,
    #[serde(rename = "maxThread")]
    pub(crate) max_thread: usize,
    #[serde(rename = "limitConnect")]
    pub(crate) limit_connect: usize,
    #[serde(rename = "limitBatch")]
    pub(crate) limit_batch: usize,
    #[serde(rename = "limitTimeout")]
    pub(crate) limit_timeout: usize,
    #[serde(rename = "queueSizeT")]
    pub(crate) queue_size_thread: u32,
    #[serde(rename = "queueSizeD")]
    pub(crate) queue_size_data: u32,
    #[serde(rename = "limitSpeed")]
    pub(crate) limit_speed: i32,
    #[serde(rename = "rowsPerRead")]
    pub(crate) rows_per_read: u32,
    #[serde(skip)]
    pub java_opts: Option<String>,
}

impl PerformanceConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let advanced_options = AdvancedOptions::from_dsn(dsn).unwrap();
        Ok(PerformanceConfig {
            read_window: parse_simple_params(dsn, "readWindow")?.unwrap_or(60),
            delay: parse_simple_params(dsn, "delay")?.unwrap_or(10),
            max_thread: advanced_options.read_concurrency.unwrap_or(50),
            limit_connect: advanced_options.write_concurrency.unwrap_or(50),
            limit_batch: advanced_options.batch_size.unwrap_or(5000),
            limit_timeout: advanced_options.batch_timeout.unwrap_or(1000),
            queue_size_thread: parse_simple_params(dsn, "queue_size_t")?.unwrap_or(1000),
            queue_size_data: parse_simple_params(dsn, "cache_queue_size")?.unwrap_or(200000),
            limit_speed: parse_simple_params(dsn, "limit_speed")?.unwrap_or(-1),
            rows_per_read: parse_simple_params(dsn, "rows_per_read")?.unwrap_or(1000),
            java_opts: parse_simple_params(dsn, "jvm_opts")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("influxdb://").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn).unwrap();
        assert_eq!(60, config.read_window);
        assert_eq!(10, config.delay);
        assert_eq!(50, config.max_thread);
        assert_eq!(50, config.limit_connect);
        assert_eq!(5000, config.limit_batch);
        assert_eq!(1000, config.limit_timeout);
        assert_eq!(1000, config.queue_size_thread);
        assert_eq!(200000, config.queue_size_data);
        assert_eq!(-1, config.limit_speed);

        let dsn = Dsn::from_str("influxdb://?readWindow=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("invalid readWindow: `abc`", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("influxdb://?delay=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("invalid delay: `abc`", config.unwrap_err().to_string());

        // let dsn = Dsn::from_str("influxdb://?maxThread=abc").unwrap();
        // let config = PerformanceConfig::from_dsn(&dsn);
        // assert!(config.is_err());
        // assert_eq!(
        //     "invalid maxThread, cause: ParseIntError { kind: InvalidDigit }",
        //     config.unwrap_err().to_string()
        // );

        let dsn = Dsn::from_str("influxdb://?queue_size_t=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid queue_size_t: `abc`",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?cache_queue_size=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid cache_queue_size: `abc`",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?limit_speed=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid limit_speed: `abc`",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?rows_per_read=abc").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid rows_per_read: `abc`",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("influxdb://?jvm_opts=-Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2").unwrap();
        let config = PerformanceConfig::from_dsn(&dsn).unwrap();
        assert_eq!(
            "-Xms4g -Xmx4g -XX:+UseG1GC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2",
            config.java_opts.unwrap()
        );
    }
}
