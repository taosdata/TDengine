use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use kafka::consumer::{FetchOffset, GroupOffsetStorage};
use taos::Dsn;

#[derive(Debug)]
pub struct SourceConfig {
    // kafka brokers
    pub bootstrap_servers: Vec<String>,

    pub group: String,
    pub topics: Option<Vec<String>>,
    pub topic_partitions: Option<HashMap<String, Vec<i32>>>,

    // certification file path
    pub cert: Option<PathBuf>,
    // certification key file path
    pub cert_key: Option<PathBuf>,
    // use SSL or not
    pub use_ssl: bool,

    pub fallback_offset: FetchOffset,
    pub fetch_max_wait_time: Option<Duration>,
    pub fetch_min_bytes: Option<i32>,
    pub fetch_max_bytes_per_partition: Option<i32>,
    pub fetch_crc_validation: Option<bool>,
    pub offset_storage: Option<GroupOffsetStorage>,
    pub retry_max_bytes_limit: Option<i32>,
    pub connection_idle_timeout: Option<Duration>,
    pub client_id: Option<String>,

    pub timeout: i64,
}

impl SourceConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let use_ssl = Self::parse_use_ssl(dsn)?;
        let (cert, cert_key) = if use_ssl {
            Self::parse_certification(dsn)?
        } else {
            (None, None)
        };

        let config = SourceConfig {
            bootstrap_servers: Self::parse_bootstrap_servers(dsn),
            group: Self::parse_group(dsn),
            topics: Self::parse_topics(dsn),
            topic_partitions: Self::parse_topic_partitions(dsn)?,
            use_ssl,
            cert,
            cert_key,
            fallback_offset: Self::parse_fallback_offset(dsn)?,
            fetch_max_wait_time: Self::parse_fetch_max_wait_time(dsn)?,
            fetch_min_bytes: Self::parse_fetch_min_bytes(dsn)?,
            fetch_max_bytes_per_partition: Self::parse_fetch_max_bytes_per_partition(dsn)?,
            fetch_crc_validation: Self::parse_fetch_crc_validation(dsn)?,
            offset_storage: Self::parse_offset_storage(dsn)?,
            retry_max_bytes_limit: Self::parse_retry_max_bytes_limit(dsn)?,
            connection_idle_timeout: Self::parse_connection_idle_timeout(dsn)?,
            client_id: Self::parse_client_id(dsn)?,
            timeout: Self::parse_timeout(dsn)?,
        };
        Ok(config)
    }

    fn parse_bootstrap_servers(dsn: &Dsn) -> Vec<String> {
        let mut bootstrap_servers = Vec::new();
        for address in dsn.addresses.iter() {
            bootstrap_servers.push(format!(
                "{}:{}",
                address.host.clone().unwrap(),
                address.port.clone().unwrap()
            ));
        }
        bootstrap_servers
    }

    fn parse_use_ssl(dsn: &Dsn) -> anyhow::Result<bool> {
        dsn.params
            .get("use_ssl")
            .unwrap_or(&"false".to_string())
            .parse()
            .map_err(|e| {
                anyhow::anyhow!(
                    "invalid use_ssl: {}, cause: {}",
                    dsn.params.get("use_ssl").unwrap(),
                    e
                )
            })
    }

    fn parse_certification(dsn: &Dsn) -> anyhow::Result<(Option<PathBuf>, Option<PathBuf>)> {
        let cert = dsn.params.get("cert").map(|s| Path::new(s).to_path_buf());
        let cert_key = dsn
            .params
            .get("cert_key")
            .map(|s| Path::new(s).to_path_buf());

        if cert.is_none() || !cert.clone().unwrap().exists() {
            return Err(anyhow::anyhow!(
                "Kafka source CA config read error, cause: cert file not found"
            ));
        }
        if cert_key.is_none() || !cert_key.clone().unwrap().exists() {
            return Err(anyhow::anyhow!(
                "Kafka source CA config read error, cause: cert_key file not found"
            ));
        }

        Ok((cert, cert_key))
    }

    fn parse_group(dsn: &Dsn) -> String {
        dsn.params
            .get("group")
            .unwrap_or(&"".to_string())
            .to_string()
    }

    fn parse_topics(dsn: &Dsn) -> Option<Vec<String>> {
        let topics = dsn
            .params
            .get("topics")
            .map(|s| s.split(",").map(|s| s.to_string()).collect::<Vec<String>>());
        topics
    }

    fn parse_topic_partitions(dsn: &Dsn) -> anyhow::Result<Option<HashMap<String, Vec<i32>>>> {
        let topic_partitions = dsn.params.get("topic_partitions");
        if topic_partitions.is_none() {
            return Ok(None);
        }

        let mut topic_map = HashMap::new();

        for tp in topic_partitions.unwrap().split(",") {
            if tp.contains(":") {
                let topic_partition = tp.split(":").collect::<Vec<&str>>();
                let topic = topic_partition[0];
                let partition = topic_partition[1];
                if partition.contains("..") {
                    let partition_range = partition.split("..").collect::<Vec<&str>>();
                    let start = partition_range[0].parse::<i32>()?;
                    let end = partition_range[1].parse::<i32>()?;
                    if start > end {
                        return Err(anyhow::anyhow!("invalid partition range: {}", partition));
                    }
                    let partitions = (start..=end).collect::<Vec<i32>>();
                    topic_map
                        .entry(topic.to_string())
                        .or_insert(vec![])
                        .extend(partitions);
                } else {
                    let partition = partition.parse::<i32>()?;
                    topic_map
                        .entry(topic.to_string())
                        .or_insert(vec![])
                        .push(partition);
                }
            } else {
                let topic = tp;
                topic_map.insert(topic.to_string(), vec![]);
            }
        }

        Ok(Some(topic_map))
    }

    fn parse_fallback_offset(dsn: &Dsn) -> anyhow::Result<FetchOffset> {
        let fallback_offset = dsn.params.get("fallback_offset").map(String::as_str);

        match fallback_offset {
            Some("Earliest") | None => Ok(FetchOffset::Earliest),
            Some("Latest") => Ok(FetchOffset::Latest),
            Some(s) => s
                .parse::<i64>()
                .map(FetchOffset::ByTime)
                .map_err(|e| anyhow::anyhow!("invalid fallback_offset: {}, cause: {}", s, e)),
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

    fn parse_offset_storage(dsn: &Dsn) -> anyhow::Result<Option<GroupOffsetStorage>> {
        dsn.params.get("offset_storage").map(String::as_str).map(|s| {
            match s {
                "Zookeeper" => Ok(Some(GroupOffsetStorage::Zookeeper)),
                "Kafka" => Ok(Some(GroupOffsetStorage::Kafka)),
                _ => {
                    Err(anyhow::anyhow!(
                        "invalid offset_storage: {}, cause: provided string was not `Zookeeper` or `Kafka`",
                        s
                    ))
                }
            }
        }).unwrap_or(Ok(None))
    }

    fn parse_retry_max_bytes_limit(dsn: &Dsn) -> anyhow::Result<Option<i32>> {
        dsn.params
            .get("retry_max_bytes_limit")
            .map(String::as_str)
            .map(|s| {
                let result = s.parse::<i32>();
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!(
                        "invalid retry_max_bytes_limit: {}, cause: {}",
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

    fn parse_client_id(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        dsn.params
            .get("client_id")
            .map(String::as_str)
            .map(|s| {
                let result = s.parse::<String>();
                match result {
                    Ok(d) => Ok(Some(d)),
                    Err(e) => Err(anyhow::anyhow!("invalid client_id: {}, cause: {}", s, e)),
                }
            })
            .unwrap_or(Ok(None))
    }

    pub fn parse_timeout(dsn: &Dsn) -> anyhow::Result<i64> {
        let timeout = dsn
            .params
            .get("timeout")
            .map(String::as_str)
            .unwrap_or("500ms");
        if timeout.eq("never") {
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
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_bootstrap_servers() {
        let dsn = Dsn::from_str("kafka://localhost:9092,192.168.1.92:9092").unwrap();
        let bootstrap_servers = SourceConfig::parse_bootstrap_servers(&dsn);
        assert_eq!("localhost:9092", bootstrap_servers[0]);
        assert_eq!("192.168.1.92:9092", bootstrap_servers[1]);
    }

    #[test]
    fn test_parse_use_ssl() {
        let dsn = Dsn::from_str("kafka://?use_ssl=true").unwrap();
        let use_ssl = SourceConfig::parse_use_ssl(&dsn).unwrap();
        assert_eq!(true, use_ssl);

        let dsn = Dsn::from_str("kafka://?use_ssl=false").unwrap();
        let use_ssl = SourceConfig::parse_use_ssl(&dsn).unwrap();
        assert_eq!(false, use_ssl);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let use_ssl = SourceConfig::parse_use_ssl(&dsn).unwrap();
        assert_eq!(false, use_ssl);

        let dsn = Dsn::from_str("kafka://?use_ssl=invalid").unwrap();
        let result = SourceConfig::parse_use_ssl(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid use_ssl: invalid, cause: provided string was not `true` or `false`",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_certification() {
        dbg!(std::env::current_dir().unwrap());
        let dsn =
            Dsn::from_str("kafka://?cert=../tests/kafka/ca.pem&cert_key=../tests/kafka/ca.key")
                .unwrap();
        let (cert, cert_key) = SourceConfig::parse_certification(&dsn).unwrap();
        assert_eq!(Path::new("../tests/kafka/ca.pem"), cert.unwrap());
        assert_eq!(Path::new("../tests/kafka/ca.key"), cert_key.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = SourceConfig::parse_certification(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "Kafka source CA config read error, cause: cert file not found",
            result.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("kafka://?cert=../tests/kafka/ca.pem").unwrap();
        let result = SourceConfig::parse_certification(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "Kafka source CA config read error, cause: cert_key file not found",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_group() {
        let dsn = Dsn::from_str("kafka://:?group=group1").unwrap();
        let group = SourceConfig::parse_group(&dsn);
        assert_eq!("group1", group);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let group = SourceConfig::parse_group(&dsn);
        assert_eq!("", group);

        let dsn = Dsn::from_str("kafka://:?group=&topics=tp1").unwrap();
        let group = SourceConfig::parse_group(&dsn);
        assert_eq!("", group);
    }

    #[test]
    fn test_parse_topics() {
        let dsn = Dsn::from_str("kafka://:?topics=tp1,tp2").unwrap();
        let topics = SourceConfig::parse_topics(&dsn);
        assert_eq!("tp1", topics.as_ref().unwrap()[0]);
        assert_eq!("tp2", topics.as_ref().unwrap()[1]);
    }

    #[test]
    fn test_parse_topic_partitions() {
        let dsn = Dsn::from_str("kafka://:?topic_partitions=tp1").unwrap();
        let topic_partitions = SourceConfig::parse_topic_partitions(&dsn).unwrap().unwrap();
        assert_eq!(1, topic_partitions.len());
        assert_eq!(true, topic_partitions.get("tp1").unwrap().is_empty());

        let dsn = Dsn::from_str("kafka://:?topic_partitions=tp2:0").unwrap();
        let topic_partitions = SourceConfig::parse_topic_partitions(&dsn).unwrap().unwrap();
        assert_eq!(1, topic_partitions.len());
        assert_eq!(1, topic_partitions.get("tp2").unwrap().len());
        assert_eq!(0, topic_partitions.get("tp2").unwrap()[0]);

        let dsn = Dsn::from_str("kafka://:?topic_partitions=tp3:0..9").unwrap();
        let topic_partitions = SourceConfig::parse_topic_partitions(&dsn).unwrap().unwrap();
        assert_eq!(1, topic_partitions.len());
        assert_eq!(10, topic_partitions.get("tp3").unwrap().len());
        assert_eq!(5, topic_partitions.get("tp3").unwrap()[5]);

        let dsn = Dsn::from_str("kafka://:?topic_partitions=tp4:0..5,tp4:7,tp5").unwrap();
        let topic_partitions = SourceConfig::parse_topic_partitions(&dsn).unwrap().unwrap();
        assert_eq!(2, topic_partitions.len());
        assert_eq!(7, topic_partitions.get("tp4").unwrap().len());
        assert_eq!(7, topic_partitions.get("tp4").unwrap()[6]);
        assert_eq!(true, topic_partitions.get("tp5").unwrap().is_empty());

        let dsn = Dsn::from_str("kafka://:?topic_partitions=tp6:0..5,tp6:7,tp6").unwrap();
        let topic_partitions = SourceConfig::parse_topic_partitions(&dsn).unwrap().unwrap();
        assert_eq!(1, topic_partitions.len());
        assert_eq!(true, topic_partitions.get("tp6").unwrap().is_empty());

        let dsn = Dsn::from_str("kafka://:?topic_partitions=tp7:5..2").unwrap();
        let topic_partitions = SourceConfig::parse_topic_partitions(&dsn);
        assert!(topic_partitions.is_err());
        assert_eq!(
            "invalid partition range: 5..2",
            topic_partitions.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_fallback_offset() {
        // Earliest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Earliest").unwrap();
        let result = SourceConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("Earliest", format!("{result:?}"));

        // Latest
        let dsn = Dsn::from_str("kafka://:?fallback_offset=Latest").unwrap();
        let result = SourceConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("Latest", format!("{result:?}"));

        // ByTime
        let dsn = Dsn::from_str("kafka://:?fallback_offset=1600000000000").unwrap();
        let result = SourceConfig::parse_fallback_offset(&dsn).unwrap();
        assert_eq!("ByTime(1600000000000)", format!("{result:?}"));

        // invalid
        let dsn = Dsn::from_str("kafka://:?fallback_offset=invalid").unwrap();
        let result = SourceConfig::parse_fallback_offset(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid fallback_offset: invalid, cause: invalid digit found in string",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_fetch_max_wait_time() {
        let dsn = Dsn::from_str("kafka://?fetch_max_wait_time=1h").unwrap();
        let result = SourceConfig::parse_fetch_max_wait_time(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!(3600, result.unwrap().as_secs());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = SourceConfig::parse_fetch_max_wait_time(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_max_wait_time=invalid").unwrap();
        let result = SourceConfig::parse_fetch_max_wait_time(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid fetch_max_wait_time: invalid, cause: NoValueFoundError: no value found in the string \"invalid\"", result.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_fetch_min_bytes() {
        let dsn = Dsn::from_str("kafka://?fetch_min_bytes=100").unwrap();
        let result = SourceConfig::parse_fetch_min_bytes(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!(100, result.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = SourceConfig::parse_fetch_min_bytes(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_min_bytes=invalid").unwrap();
        let result = SourceConfig::parse_fetch_min_bytes(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid fetch_min_bytes: invalid, cause: invalid digit found in string",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_fetch_max_bytes_per_partition() {
        let dsn = Dsn::from_str("kafka://?fetch_max_bytes_per_partition=100").unwrap();
        let config = SourceConfig::parse_fetch_max_bytes_per_partition(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(100, config.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let config = SourceConfig::parse_fetch_max_bytes_per_partition(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(1024 * 1024, config.unwrap());

        let dsn = Dsn::from_str("kafka://?fetch_max_bytes_per_partition=invalid").unwrap();
        let result = SourceConfig::parse_fetch_max_bytes_per_partition(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid fetch_max_bytes_per_partition: invalid, cause: invalid digit found in string",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_fetch_crc_validation() {
        let dsn = Dsn::from_str("kafka://?fetch_crc_validation=true").unwrap();
        let config = SourceConfig::parse_fetch_crc_validation(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(true, config.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let config = SourceConfig::parse_fetch_crc_validation(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("kafka://?fetch_crc_validation=invalid").unwrap();
        let result = SourceConfig::parse_fetch_crc_validation(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid fetch_crc_validation: invalid, cause: provided string was not `true` or `false`", result.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_offset_storage() {
        let dsn = Dsn::from_str("kafka://?offset_storage=Kafka").unwrap();
        let config = SourceConfig::parse_offset_storage(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!("Kafka", format!("{:?}", config.unwrap()));

        let dsn = Dsn::from_str("kafka://").unwrap();
        let config = SourceConfig::parse_offset_storage(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("kafka://?offset_storage=invalid").unwrap();
        let result = SourceConfig::parse_offset_storage(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid offset_storage: invalid, cause: provided string was not `Zookeeper` or `Kafka`", result.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_retry_max_bytes_limit() {
        let dsn = Dsn::from_str("kafka://?retry_max_bytes_limit=100").unwrap();
        let config = SourceConfig::parse_retry_max_bytes_limit(&dsn).unwrap();
        assert!(config.is_some());
        assert_eq!(100, config.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let config = SourceConfig::parse_retry_max_bytes_limit(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("kafka://?retry_max_bytes_limit=invalid").unwrap();
        let result = SourceConfig::parse_retry_max_bytes_limit(&dsn);
        assert!(result.is_err());
        assert_eq!(
            "invalid retry_max_bytes_limit: invalid, cause: invalid digit found in string",
            result.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_connection_idle_timeout() {
        let dsn = Dsn::from_str("kafka://?connection_idle_timeout=1h").unwrap();
        let result = SourceConfig::parse_connection_idle_timeout(&dsn).unwrap();
        assert!(result.is_some());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = SourceConfig::parse_connection_idle_timeout(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?connection_idle_timeout=invalid").unwrap();
        let result = SourceConfig::parse_connection_idle_timeout(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid connection_idle_timeout: invalid, cause: NoValueFoundError: no value found in the string \"invalid\"", result.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_client_id() {
        let dsn = Dsn::from_str("kafka://?client_id=client1").unwrap();
        let result = SourceConfig::parse_client_id(&dsn).unwrap();
        assert!(result.is_some());
        assert_eq!("client1", result.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = SourceConfig::parse_client_id(&dsn).unwrap();
        assert!(result.is_none());

        let dsn = Dsn::from_str("kafka://?client_id=").unwrap();
        let result = SourceConfig::parse_client_id(&dsn).unwrap();
        assert_eq!("", result.unwrap().as_str());
    }

    #[test]
    fn test_parse_timeout() {
        let dsn = Dsn::from_str("kafka://?timeout=5s").unwrap();
        let result = SourceConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(5000, result);

        let dsn = Dsn::from_str("kafka://?timeout=30s").unwrap();
        let result = SourceConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(30 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=5min").unwrap();
        let result = SourceConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(5 * 60 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=6h").unwrap();
        let result = SourceConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(6 * 3600 * 1000, result);

        let dsn = Dsn::from_str("kafka://?timeout=1d").unwrap();
        let result = SourceConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(24 * 3600 * 1000, result);

        let dsn = Dsn::from_str("kafka://?").unwrap();
        let result = SourceConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(500, result);

        let dsn = Dsn::from_str("kafka://?timeout=never").unwrap();
        let result = SourceConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(-1, result);

        let dsn = Dsn::from_str("kafka://?timeout=invalid").unwrap();
        let result = SourceConfig::parse_timeout(&dsn);
        assert!(result.is_err());
        assert_eq!("invalid timeout: invalid, cause: NoValueFoundError: no value found in the string \"invalid\"", result.unwrap_err().to_string());
    }
}
