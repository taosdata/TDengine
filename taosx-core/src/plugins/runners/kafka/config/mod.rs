use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use faststr::FastStr;
use taos::{Dsn, Itertools};
use tracing::debug;

use crate::plugins::config::AdvancedOptions;
use crate::runners::kafka::config::connect::KafkaConnectConfig;

pub mod connect;

#[derive(Debug, Clone)]
pub struct KafkaTaskConfig {
    pub connect: KafkaConnectConfig,

    pub timeout: i64,
    pub group: String,
    pub topics: Vec<String>,

    pub fallback_offset: String,
    pub fetch_max_wait_time: Option<Duration>,
    pub fetch_min_bytes: Option<i32>,
    pub fetch_max_bytes_per_partition: Option<i32>,
    pub fetch_crc_validation: Option<bool>,
    pub connection_idle_timeout: Option<Duration>,
    pub client_id: Option<String>,
    pub commit_interval: Option<Duration>,
    pub enable_group_instance_id: bool,

    pub advanced_options: AdvancedOptions,

    pub extras: Option<HashMap<FastStr, FastStr>>,
}

impl KafkaTaskConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let config = KafkaTaskConfig {
            connect: KafkaConnectConfig::from_dsn(dsn)?,
            timeout: Self::parse_timeout(dsn)?,
            group: Self::parse_group(dsn),
            topics: Self::parse_topics(dsn)?,
            fallback_offset: Self::parse_fallback_offset(dsn)?,
            fetch_max_wait_time: Self::parse_fetch_max_wait_time(dsn)?,
            fetch_min_bytes: Self::parse_fetch_min_bytes(dsn)?,
            fetch_max_bytes_per_partition: Self::parse_fetch_max_bytes_per_partition(dsn)?,
            fetch_crc_validation: Self::parse_fetch_crc_validation(dsn)?,
            connection_idle_timeout: Self::parse_connection_idle_timeout(dsn)?,
            commit_interval: Self::parse_commit_interval(dsn)?,
            client_id: Self::parse_client_id(dsn)?,
            enable_group_instance_id: Self::parse_enable_group_instance_id(dsn),
            advanced_options: AdvancedOptions::from_dsn(dsn)?,
            extras: Self::parse_extras(dsn)?,
        };
        Ok(config)
    }

    fn parse_group(dsn: &Dsn) -> String {
        dsn.params
            .get("group")
            .unwrap_or(&"".to_string())
            .to_string()
    }

    pub fn parse_topics(dsn: &Dsn) -> anyhow::Result<Vec<String>> {
        Ok(dsn
            .get("topics")
            .map(|s| s.split(",").map(|s| s.to_string()).collect::<Vec<String>>())
            .ok_or(anyhow::anyhow!("topics is required"))?)
    }

    pub fn parse_fallback_offset(dsn: &Dsn) -> anyhow::Result<String> {
        let fallback_offset = dsn.params.get("fallback_offset").map(String::as_str);

        match fallback_offset {
            Some("Smallest") => Ok(String::from("smallest")),
            Some("Earliest") => Ok(String::from("earliest")),
            Some("Beginning") => Ok(String::from("beginning")),
            Some("Largest") => Ok(String::from("largest")),
            Some("Latest") => Ok(String::from("latest")),
            Some("End") => Ok(String::from("end")),
            Some("Error") => Ok(String::from("error")),
            Some(_) | None => Ok(String::from("largest")),
        }
    }

    fn parse_fetch_max_wait_time(dsn: &Dsn) -> anyhow::Result<Option<Duration>> {
        dsn.params
            .get("fetch_max_wait_time")
            .map(String::as_str)
            .map(|s| {
                let result = parse_duration::parse(s);
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid fetch_max_wait_time: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_fetch_min_bytes(dsn: &Dsn) -> anyhow::Result<Option<i32>> {
        dsn.params
            .get("fetch_min_bytes")
            .map(String::as_str)
            .map(|s| {
                let result = s.parse::<i32>();
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid fetch_min_bytes: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_fetch_max_bytes_per_partition(dsn: &Dsn) -> anyhow::Result<Option<i32>> {
        dsn.params
            .get("fetch_max_bytes_per_partition")
            .map(String::as_str)
            .map(|s| {
                let result = s.parse::<i32>();
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid fetch_max_bytes_per_partition: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(Some(1024 * 1024)))
    }

    fn parse_fetch_crc_validation(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
        dsn.params
            .get("fetch_crc_validation")
            .map(String::as_str)
            .map(|s| {
                let result = s.parse::<bool>();
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid fetch_crc_validation: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_connection_idle_timeout(dsn: &Dsn) -> anyhow::Result<Option<Duration>> {
        dsn.params
            .get("connection_idle_timeout")
            .map(String::as_str)
            .map(|s| {
                let result = parse_duration::parse(s);
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid connection_idle_timeout: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_commit_interval(dsn: &Dsn) -> anyhow::Result<Option<Duration>> {
        dsn.params
            .get("commit_interval")
            .map(String::as_str)
            .map(|s| {
                let result = parse_duration::parse(s);
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid commit_interval: {}, cause: {}",
                        s,
                        e
                    )),
                }
            })
            .unwrap_or(Ok(None))
    }

    fn parse_client_id(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        let client_id = dsn
            .params
            .get("client_id")
            .map(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            })
            .flatten();
        Ok(client_id)
    }

    fn parse_enable_group_instance_id(dsn: &Dsn) -> bool {
        dsn.params
            .get("enable_group_instance_id")
            .map(String::as_str)
            .map(|s| s.parse::<bool>().unwrap_or(false))
            .unwrap_or(false)
    }

    fn parse_timeout(dsn: &Dsn) -> anyhow::Result<i64> {
        let timeout = dsn
            .params
            .get("timeout")
            .map(String::as_str)
            .unwrap_or("0ms");
        if timeout.eq("never") || timeout.starts_with("0") || timeout.starts_with("-1") {
            return Ok(-1);
        }

        let result = parse_duration::parse(timeout);
        return match result {
            Ok(d) => Ok(d.as_millis() as i64),
            Err(e) => Err(anyhow::anyhow!(
                "invalid timeout: {}, cause: {}",
                timeout,
                e
            )),
        };
    }

    fn parse_extras(dsn: &Dsn) -> anyhow::Result<Option<HashMap<FastStr, FastStr>>> {
        let mut extras = HashMap::new();
        for (k, v) in dsn
            .params
            .iter()
            .filter(|(k, _)| !k.is_empty())
            .filter(|(k, _)| k.contains('.'))
            .map(|(k, v)| (k.trim(), v.trim()))
        {
            extras.insert(FastStr::from_str(k)?, FastStr::from_str(v)?);
        }
        if let Some(str) = std::env::var("KAFKA_CONSUMER_EXTRAS").ok() {
            debug!("use env KAFKA_CONSUMER_EXTRAS: {}", str);
            for (k, v) in str
                .split(',')
                .flat_map(|s| s.split('=').collect_tuple::<(_, _)>())
                .map(|(k, v)| (k.trim(), v.trim()))
            {
                extras.insert(FastStr::from_str(k)?, FastStr::from_str(v)?);
            }
        }
        Ok(if extras.is_empty() {
            None
        } else {
            Some(extras)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_group() {
        let dsn = Dsn::from_str("kafka://:?group=group1").unwrap();
        let group = KafkaTaskConfig::parse_group(&dsn);
        assert_eq!("group1", group);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let group = KafkaTaskConfig::parse_group(&dsn);
        assert_eq!("", group);

        let dsn = Dsn::from_str("kafka://:?group=&topics=tp1").unwrap();
        let group = KafkaTaskConfig::parse_group(&dsn);
        assert_eq!("", group);
    }

    #[test]
    fn test_parse_topics() {
        let dsn = Dsn::from_str("kafka://:?topics=tp1,tp2").unwrap();
        let topics = KafkaTaskConfig::parse_topics(&dsn);
        assert_eq!("tp1", topics.as_ref().unwrap()[0]);
        assert_eq!("tp2", topics.as_ref().unwrap()[1]);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let topics = KafkaTaskConfig::parse_topics(&dsn);
        assert!(topics.is_err());
        assert_eq!("topics is required", topics.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_fallback_offset() {
        // Smallest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Smallest").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("smallest", result);

        // Earliest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Earliest").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("earliest", result);

        // Beginning
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Beginning").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("beginning", result);

        // Largest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Largest").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("largest", result);

        // Latest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Latest").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("latest", result);

        // End
        let dsn = Dsn::from_str("kafka://:?fallback_offset=End").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("end", result);

        // Error
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Error").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("error", result);

        // default 1
        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("largest", result);

        // default 2
        let dsn = Dsn::from_str("kafka://:?fallback_offset=xx").unwrap();
        let result = KafkaTaskConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("largest", result);
    }

    #[test]
    fn test_parse_fetch_max_wait_time() {
        let dsn = Dsn::from_str("kafka://?fetch_max_wait_time=1h").unwrap();
        let result = KafkaTaskConfig::parse_fetch_max_wait_time(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!(3600, result.unwrap().as_secs());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_fetch_max_wait_time(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_max_wait_time=invalid").unwrap();
        let result = KafkaTaskConfig::parse_fetch_max_wait_time(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid fetch_max_wait_time: invalid, cause: NoValueFoundError: no value found in the string \"invalid\"", result.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_fetch_min_bytes() {
        let dsn = Dsn::from_str("kafka://?fetch_min_bytes=100").unwrap();
        let result = KafkaTaskConfig::parse_fetch_min_bytes(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!(100, result.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_fetch_min_bytes(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_min_bytes=invalid").unwrap();
        let result = KafkaTaskConfig::parse_fetch_min_bytes(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid fetch_min_bytes: invalid, cause: invalid digit found in string",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_fetch_max_bytes_per_partition() {
        let dsn = Dsn::from_str("kafka://?fetch_max_bytes_per_partition=100").unwrap();
        let config = KafkaTaskConfig::parse_fetch_max_bytes_per_partition(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(100, config.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let config = KafkaTaskConfig::parse_fetch_max_bytes_per_partition(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(1024 * 1024, config.unwrap());

        let dsn = Dsn::from_str("kafka://?fetch_max_bytes_per_partition=invalid").unwrap();
        let result = KafkaTaskConfig::parse_fetch_max_bytes_per_partition(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid fetch_max_bytes_per_partition: invalid, cause: invalid digit found in string",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_fetch_crc_validation() {
        let dsn = Dsn::from_str("kafka://?fetch_crc_validation=true").unwrap();
        let config = KafkaTaskConfig::parse_fetch_crc_validation(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(true, config.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let config = KafkaTaskConfig::parse_fetch_crc_validation(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_crc_validation=invalid").unwrap();
        let result = KafkaTaskConfig::parse_fetch_crc_validation(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid fetch_crc_validation: invalid, cause: provided string was not `true` or `false`", result.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_connection_idle_timeout() {
        let dsn = Dsn::from_str("kafka://?connection_idle_timeout=1h").unwrap();
        let result = KafkaTaskConfig::parse_connection_idle_timeout(&dsn).unwrap();
        assert!(result.is_some());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_connection_idle_timeout(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?connection_idle_timeout=invalid").unwrap();
        let result = KafkaTaskConfig::parse_connection_idle_timeout(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid connection_idle_timeout: invalid, cause: NoValueFoundError: no value found in the string \"invalid\"", result.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_client_id() {
        let dsn = Dsn::from_str("kafka://?client_id=client1").unwrap();
        let result = KafkaTaskConfig::parse_client_id(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!("client1", result.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = KafkaTaskConfig::parse_client_id(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?client_id=").unwrap();
        let result = KafkaTaskConfig::parse_client_id(&dsn).unwrap();
        assert_eq!("", result.unwrap().as_str());
    }

    #[test]
    fn test_parse_timeout() {
        let dsn = Dsn::from_str("kafka://?timeout=5s").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(5000, result);

        let dsn = Dsn::from_str("kafka://?timeout=30s").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(30 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=5min").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(5 * 60 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=6h").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(6 * 3600 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=1d").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(24 * 3600 * 1000, result);

        let dsn = Dsn::from_str("kafka://?").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(500, result);

        let dsn = Dsn::from_str("kafka://?timeout=never").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(-1, result);

        let dsn = Dsn::from_str("kafka://?timeout=invalid").unwrap();
        let result = KafkaTaskConfig::parse_timeout(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid timeout: invalid, cause: NoValueFoundError: no value found in the string \"invalid\"", result.unwrap_err().to_string());
    }
}
