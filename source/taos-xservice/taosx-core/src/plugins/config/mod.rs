use serde::{Deserialize, Serialize};
use std::str::FromStr;
use taos::Dsn;

/// advanced options for all data sources. Option value is None when data source not support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedOptions {
    #[allow(dead_code)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_level: Option<LogLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_concurrency: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_concurrency: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,
    /// Batch timeout in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_timeout: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_raw_data: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_raw_data_days: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_raw_data_dir: Option<String>,
}

impl AdvancedOptions {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self {
            log_level: Self::parse_log_level(dsn)?,
            read_concurrency: Self::parse_read_concurrency(dsn)?,
            write_concurrency: Self::parse_write_concurrency(dsn)?,
            batch_size: Self::parse_batch_size(dsn)?,
            batch_timeout: Self::parse_batch_timeout(dsn)?,
            keep_raw_data: Self::parse_keep_raw_data(dsn)?,
            keep_raw_data_days: Self::parse_keep_raw_data_days(dsn)?,
            keep_raw_data_dir: Self::parse_keep_raw_data_dir(dsn)?,
        })
    }

    fn parse_log_level(dsn: &Dsn) -> anyhow::Result<Option<LogLevel>> {
        if let Some(log_level) = dsn.get("log_level") {
            let log_level = LogLevel::from_str(log_level)?;
            Ok(Some(log_level))
        } else {
            Ok(None)
        }
    }

    fn parse_read_concurrency(dsn: &Dsn) -> anyhow::Result<Option<usize>> {
        if let Some(read_concurrency) = dsn.get("read_concurrency") {
            let read_concurrency = read_concurrency.parse::<usize>().map_err(|e| {
                anyhow::anyhow!(
                    "invalid read_concurrency: {}, cause: {}",
                    read_concurrency,
                    e
                )
            })?;
            Ok(Some(read_concurrency))
        } else {
            Ok(None)
        }
    }

    fn parse_write_concurrency(dsn: &Dsn) -> anyhow::Result<Option<usize>> {
        if let Some(write_concurrency) = dsn.get("write_concurrency") {
            let write_concurrency = write_concurrency.parse::<usize>().map_err(|e| {
                anyhow::anyhow!(
                    "invalid write_concurrency: {}, cause: {}",
                    write_concurrency,
                    e
                )
            })?;
            Ok(Some(write_concurrency))
        } else {
            Ok(None)
        }
    }

    fn parse_batch_size(dsn: &Dsn) -> anyhow::Result<Option<usize>> {
        if let Some(batch_size) = dsn.get("batch_size") {
            let batch_size = batch_size
                .parse::<usize>()
                .map_err(|e| anyhow::anyhow!("invalid batch_size: {}, cause: {}", batch_size, e))?;
            Ok(Some(batch_size))
        } else {
            Ok(None)
        }
    }

    fn parse_batch_timeout(dsn: &Dsn) -> anyhow::Result<Option<usize>> {
        if let Some(batch_timeout) = dsn.get("batch_timeout") {
            let batch_timeout = batch_timeout.parse::<usize>().map_err(|e| {
                anyhow::anyhow!("invalid batch_timeout: {}, cause: {}", batch_timeout, e)
            })?;
            Ok(Some(batch_timeout))
        } else {
            Ok(None)
        }
    }

    fn parse_keep_raw_data(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        if let Some(keep_raw_data) = dsn.get("keep_raw_data") {
            let keep_raw_data = keep_raw_data.parse::<bool>().map_err(|e| {
                anyhow::anyhow!("invalid keep_raw_data: {}, cause: {}", keep_raw_data, e)
            })?;
            Ok(Some(keep_raw_data))
        } else {
            Ok(None)
        }
    }

    fn parse_keep_raw_data_days(dsn: &Dsn) -> anyhow::Result<Option<usize>> {
        if let Some(keep_raw_data_days) = dsn.get("keep_raw_data_days") {
            let keep_raw_data_days = keep_raw_data_days.parse::<usize>().map_err(|e| {
                anyhow::anyhow!(
                    "invalid keep_raw_data_days: {}, cause: {}",
                    keep_raw_data_days,
                    e
                )
            })?;
            Ok(Some(keep_raw_data_days))
        } else {
            Ok(None)
        }
    }

    fn parse_keep_raw_data_dir(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        if let Some(keep_raw_data_dir) = dsn.get("keep_raw_data_dir") {
            Ok(Some(keep_raw_data_dir.to_string()))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl FromStr for LogLevel {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Err(anyhow::anyhow!("invalid log_level: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_log_level() {
        let dsn = Dsn::from_str("ds://?log_level=error").unwrap();
        let log_level = AdvancedOptions::parse_log_level(&dsn).unwrap().unwrap();
        assert_eq!(log_level, LogLevel::Error);

        let dsn = Dsn::from_str("ds://?log_level=warn").unwrap();
        let log_level = AdvancedOptions::parse_log_level(&dsn).unwrap().unwrap();
        assert_eq!(log_level, LogLevel::Warn);

        let dsn = Dsn::from_str("ds://?log_level=info").unwrap();
        let log_level = AdvancedOptions::parse_log_level(&dsn).unwrap().unwrap();
        assert_eq!(log_level, LogLevel::Info);

        let dsn = Dsn::from_str("ds://?log_level=debug").unwrap();
        let log_level = AdvancedOptions::parse_log_level(&dsn).unwrap().unwrap();
        assert_eq!(log_level, LogLevel::Debug);

        let dsn = Dsn::from_str("ds://?log_level=trace").unwrap();
        let log_level = AdvancedOptions::parse_log_level(&dsn).unwrap().unwrap();
        assert_eq!(log_level, LogLevel::Trace);

        let dsn = Dsn::from_str("ds://?log_level=invalid").unwrap();
        let result = AdvancedOptions::parse_log_level(&dsn);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid log_level: invalid"
        );
    }

    #[test]
    fn test_parse_read_concurrency() {
        let dsn = Dsn::from_str("ds://?read_concurrency=10").unwrap();
        let read_concurrency = AdvancedOptions::parse_read_concurrency(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!(read_concurrency, 10);

        let dsn = Dsn::from_str("ds://?read_concurrency=1000").unwrap();
        let read_concurrency = AdvancedOptions::parse_read_concurrency(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!(read_concurrency, 1000);

        let dsn = Dsn::from_str("ds://?read_concurrency=invalid").unwrap();
        let result = AdvancedOptions::parse_read_concurrency(&dsn);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid read_concurrency: invalid, cause: invalid digit found in string"
        );
    }

    #[test]
    fn test_parse_write_concurrency() {
        let dsn = Dsn::from_str("ds://?write_concurrency=10").unwrap();
        let write_concurrency = AdvancedOptions::parse_write_concurrency(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!(write_concurrency, 10);

        let dsn = Dsn::from_str("ds://?write_concurrency=1000").unwrap();
        let write_concurrency = AdvancedOptions::parse_write_concurrency(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!(write_concurrency, 1000);

        let dsn = Dsn::from_str("ds://?write_concurrency=invalid").unwrap();
        let result = AdvancedOptions::parse_write_concurrency(&dsn);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid write_concurrency: invalid, cause: invalid digit found in string"
        );
    }

    #[test]
    fn test_parse_batch_size() {
        let dsn = Dsn::from_str("ds://?batch_size=10").unwrap();
        let batch_size = AdvancedOptions::parse_batch_size(&dsn).unwrap().unwrap();
        assert_eq!(batch_size, 10);

        let dsn = Dsn::from_str("ds://?batch_size=10000").unwrap();
        let batch_size = AdvancedOptions::parse_batch_size(&dsn).unwrap().unwrap();
        assert_eq!(batch_size, 10000);

        let dsn = Dsn::from_str("ds://?batch_size=invalid").unwrap();
        let result = AdvancedOptions::parse_batch_size(&dsn);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid batch_size: invalid, cause: invalid digit found in string"
        );
    }

    #[test]
    fn test_parse_batch_timeout() {
        let dsn = Dsn::from_str("ds://?batch_timeout=10").unwrap();
        let batch_timeout = AdvancedOptions::parse_batch_timeout(&dsn).unwrap().unwrap();
        assert_eq!(batch_timeout, 10);

        let dsn = Dsn::from_str("ds://?batch_timeout=60").unwrap();
        let batch_timeout = AdvancedOptions::parse_batch_timeout(&dsn).unwrap().unwrap();
        assert_eq!(batch_timeout, 60);

        let dsn = Dsn::from_str("ds://?batch_timeout=invalid").unwrap();
        let result = AdvancedOptions::parse_batch_timeout(&dsn);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid batch_timeout: invalid, cause: invalid digit found in string"
        );
    }

    #[test]
    fn test_parse_keep_raw_data() {
        let dsn = Dsn::from_str("ds://?keep_raw_data=true").unwrap();
        let keep_raw_data = AdvancedOptions::parse_keep_raw_data(&dsn).unwrap().unwrap();
        assert!(keep_raw_data);

        let dsn = Dsn::from_str("ds://?keep_raw_data=false").unwrap();
        let keep_raw_data = AdvancedOptions::parse_keep_raw_data(&dsn).unwrap().unwrap();
        assert!(!keep_raw_data);

        let dsn = Dsn::from_str("ds://?keep_raw_data=invalid").unwrap();
        let result = AdvancedOptions::parse_keep_raw_data(&dsn);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid keep_raw_data: invalid, cause: provided string was not `true` or `false`"
        );
    }

    #[test]
    fn test_parse_keep_raw_data_days() {
        let dsn = Dsn::from_str("ds://?keep_raw_data_days=10").unwrap();
        let keep_raw_data_days = AdvancedOptions::parse_keep_raw_data_days(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!(keep_raw_data_days, 10);

        let dsn = Dsn::from_str("ds://?keep_raw_data_days=365").unwrap();
        let keep_raw_data_days = AdvancedOptions::parse_keep_raw_data_days(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!(keep_raw_data_days, 365);

        let dsn = Dsn::from_str("ds://?keep_raw_data_days=invalid").unwrap();
        let result = AdvancedOptions::parse_keep_raw_data_days(&dsn);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid keep_raw_data_days: invalid, cause: invalid digit found in string"
        );
    }

    #[test]
    fn test_parse_keep_raw_data_dir() {
        let dsn = Dsn::from_str("ds://?keep_raw_data_dir=/tmp").unwrap();
        let keep_raw_data_dir = AdvancedOptions::parse_keep_raw_data_dir(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!(keep_raw_data_dir, "/tmp");
    }

    #[test]
    fn test_from_dsn_all_options() {
        let dsn = Dsn::from_str(
            "ds://?log_level=debug&read_concurrency=5&write_concurrency=10&batch_size=1000&batch_timeout=500&keep_raw_data=true&keep_raw_data_days=30&keep_raw_data_dir=/data"
        ).unwrap();

        let options = AdvancedOptions::from_dsn(&dsn).unwrap();

        assert_eq!(options.log_level, Some(LogLevel::Debug));
        assert_eq!(options.read_concurrency, Some(5));
        assert_eq!(options.write_concurrency, Some(10));
        assert_eq!(options.batch_size, Some(1000));
        assert_eq!(options.batch_timeout, Some(500));
        assert_eq!(options.keep_raw_data, Some(true));
        assert_eq!(options.keep_raw_data_days, Some(30));
        assert_eq!(options.keep_raw_data_dir, Some("/data".to_string()));
    }

    #[test]
    fn test_from_dsn_no_options() {
        let dsn = Dsn::from_str("ds://").unwrap();

        let options = AdvancedOptions::from_dsn(&dsn).unwrap();

        assert_eq!(options.log_level, None);
        assert_eq!(options.read_concurrency, None);
        assert_eq!(options.write_concurrency, None);
        assert_eq!(options.batch_size, None);
        assert_eq!(options.batch_timeout, None);
        assert_eq!(options.keep_raw_data, None);
        assert_eq!(options.keep_raw_data_days, None);
        assert_eq!(options.keep_raw_data_dir, None);
    }

    #[test]
    fn test_from_dsn_partial_options() {
        let dsn = Dsn::from_str("ds://?batch_size=500&keep_raw_data=false").unwrap();

        let options = AdvancedOptions::from_dsn(&dsn).unwrap();

        assert_eq!(options.log_level, None);
        assert_eq!(options.read_concurrency, None);
        assert_eq!(options.write_concurrency, None);
        assert_eq!(options.batch_size, Some(500));
        assert_eq!(options.batch_timeout, None);
        assert_eq!(options.keep_raw_data, Some(false));
        assert_eq!(options.keep_raw_data_days, None);
        assert_eq!(options.keep_raw_data_dir, None);
    }

    #[test]
    fn test_from_dsn_invalid_log_level() {
        let dsn = Dsn::from_str("ds://?log_level=invalid_level").unwrap();
        let result = AdvancedOptions::from_dsn(&dsn);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid log_level")
        );
    }

    #[test]
    fn test_from_dsn_invalid_read_concurrency() {
        let dsn = Dsn::from_str("ds://?read_concurrency=-1").unwrap();
        let result = AdvancedOptions::from_dsn(&dsn);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid read_concurrency")
        );
    }

    #[test]
    fn test_from_dsn_invalid_write_concurrency() {
        let dsn = Dsn::from_str("ds://?write_concurrency=-2").unwrap();
        let result = AdvancedOptions::from_dsn(&dsn);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid write_concurrency")
        );
    }

    #[test]
    fn test_from_dsn_invalid_keep_raw_data() {
        let dsn = Dsn::from_str("ds://?keep_raw_data=maybe").unwrap();
        let result = AdvancedOptions::from_dsn(&dsn);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid keep_raw_data")
        );
    }

    #[test]
    fn test_from_dsn_invalid_keep_raw_data_days() {
        let dsn = Dsn::from_str("ds://?keep_raw_data_days=-5").unwrap();
        let result = AdvancedOptions::from_dsn(&dsn);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid keep_raw_data_days")
        );
    }

    #[test]
    fn test_parse_options_absent_return_none() {
        let dsn = Dsn::from_str("ds://").unwrap();
        assert!(AdvancedOptions::parse_log_level(&dsn).unwrap().is_none());
        assert!(
            AdvancedOptions::parse_read_concurrency(&dsn)
                .unwrap()
                .is_none()
        );
        assert!(
            AdvancedOptions::parse_write_concurrency(&dsn)
                .unwrap()
                .is_none()
        );
        assert!(AdvancedOptions::parse_batch_size(&dsn).unwrap().is_none());
        assert!(
            AdvancedOptions::parse_batch_timeout(&dsn)
                .unwrap()
                .is_none()
        );
        assert!(
            AdvancedOptions::parse_keep_raw_data(&dsn)
                .unwrap()
                .is_none()
        );
        assert!(
            AdvancedOptions::parse_keep_raw_data_days(&dsn)
                .unwrap()
                .is_none()
        );
        assert!(
            AdvancedOptions::parse_keep_raw_data_dir(&dsn)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_from_dsn_zero_values() {
        let dsn = Dsn::from_str("ds://?batch_size=0&batch_timeout=0&keep_raw_data_days=0").unwrap();

        let options = AdvancedOptions::from_dsn(&dsn).unwrap();

        assert_eq!(options.batch_size, Some(0));
        assert_eq!(options.batch_timeout, Some(0));
        assert_eq!(options.keep_raw_data_days, Some(0));
    }

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from_str("error").unwrap(), LogLevel::Error);
        assert_eq!(LogLevel::from_str("warn").unwrap(), LogLevel::Warn);
        assert_eq!(LogLevel::from_str("info").unwrap(), LogLevel::Info);
        assert_eq!(LogLevel::from_str("debug").unwrap(), LogLevel::Debug);
        assert_eq!(LogLevel::from_str("trace").unwrap(), LogLevel::Trace);

        let result = LogLevel::from_str("unknown");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "invalid log_level: unknown"
        );
    }
}
