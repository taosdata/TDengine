use std::collections::HashMap;
use std::path::{Path, PathBuf};

use kafka::consumer::FetchOffset;
use taos::Dsn;

#[derive(Debug)]
pub struct SourceConfig {
    // kafka brokers
    pub bootstrap_servers: Vec<String>,
    // use SSL or not
    pub use_ssl: bool,
    // certification file path
    pub cert: Option<PathBuf>,
    // certification key file path
    pub cert_key: Option<PathBuf>,

    pub group: String,
    pub topics: Option<Vec<String>>,
    pub topic_partitions: Option<HashMap<String, Vec<i32>>>,
    pub fallback_offset: FetchOffset,
    pub timeout: i64,
}

impl SourceConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let bootstrap_servers = Self::parse_bootstrap_servers(dsn);
        let use_ssl = Self::parse_use_ssl(dsn)?;
        let (cert, cert_key) = if use_ssl {
            Self::parse_certification(dsn)?
        } else {
            (None, None)
        };

        let group = Self::parse_group(dsn);
        let topics = Self::parse_topics(dsn);
        let topic_partitions = Self::parse_topic_partitions(dsn)?;
        let fallback_offset = Self::parse_fallback_offset(dsn)?;
        let timeout = Self::parse_timeout(dsn)?;

        let config = SourceConfig {
            bootstrap_servers,
            use_ssl,
            cert,
            cert_key,
            group,
            topics,
            topic_partitions,
            fallback_offset,
            timeout,
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
        dsn.params.get("use_ssl")
            .unwrap_or(&"false".to_string())
            .parse()
            .map_err(|e| {
                anyhow::anyhow!("invalid use_ssl: {}, cause: {}", dsn.params.get("use_ssl").unwrap(), e)
            })
    }

    fn parse_certification(dsn: &Dsn) -> anyhow::Result<(Option<PathBuf>, Option<PathBuf>)> {
        let cert = dsn.params.get("cert").map(|s| Path::new(s).to_path_buf());
        let cert_key = dsn.params.get("cert_key").map(|s| Path::new(s).to_path_buf());

        if cert.is_none() || !cert.clone().unwrap().exists() {
            return Err(anyhow::anyhow!("Kafka source CA config read error, cause: cert file not found"));
        }
        if cert_key.is_none() || !cert_key.clone().unwrap().exists() {
            return Err(anyhow::anyhow!("Kafka source CA config read error, cause: cert_key file not found"));
        }

        Ok((cert, cert_key))
    }

    fn parse_group(dsn: &Dsn) -> String {
        dsn.params.get("group").unwrap_or(&"".to_string()).to_string()
    }

    fn parse_topics(dsn: &Dsn) -> Option<Vec<String>> {
        let topics = dsn.params.get("topics").map(|s| {
            s.split(",").map(|s| s.to_string()).collect::<Vec<String>>()
        });
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
                    topic_map.entry(topic.to_string()).or_insert(vec![]).extend(partitions);
                } else {
                    let partition = partition.parse::<i32>()?;
                    topic_map.entry(topic.to_string()).or_insert(vec![]).push(partition);
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
            Some(s) => s.parse::<i64>()
                .map(FetchOffset::ByTime)
                .map_err(|e| anyhow::anyhow!("invalid fallback_offset: {}, cause: {}", s, e)),
        }
    }

    pub fn parse_timeout(dsn: &Dsn) -> anyhow::Result<i64> {
        let timeout = dsn.params.get("timeout").map(String::as_str).unwrap_or("500ms");
        if timeout.eq("never") {
            return Ok(-1);
        }

        let result = parse_duration::parse(timeout);
        return match result {
            Ok(d) => Ok(d.as_millis() as i64),
            Err(e) => Err(anyhow::anyhow!("invalid timeout: {}, cause: {}", timeout, e))
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
        assert_eq!("invalid use_ssl: invalid, cause: provided string was not `true` or `false`", result.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_certification() {
        dbg!(std::env::current_dir().unwrap());
        let dsn = Dsn::from_str("kafka://?cert=../tests/kafka/ca.pem&cert_key=../tests/kafka/ca.key").unwrap();
        let (cert, cert_key) = SourceConfig::parse_certification(&dsn).unwrap();
        assert_eq!(Path::new("../tests/kafka/ca.pem"), cert.unwrap());
        assert_eq!(Path::new("../tests/kafka/ca.key"), cert_key.unwrap());

        let dsn = Dsn::from_str("kafka://").unwrap();
        let result = SourceConfig::parse_certification(&dsn);
        assert!(result.is_err());
        assert_eq!("Kafka source CA config read error, cause: cert file not found", result.unwrap_err().to_string());

        let dsn = Dsn::from_str("kafka://?cert=../tests/kafka/ca.pem").unwrap();
        let result = SourceConfig::parse_certification(&dsn);
        assert!(result.is_err());
        assert_eq!("Kafka source CA config read error, cause: cert_key file not found", result.unwrap_err().to_string());
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
        assert_eq!("invalid partition range: 5..2", topic_partitions.unwrap_err().to_string());
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
        assert_eq!("invalid fallback_offset: invalid, cause: invalid digit found in string", result.unwrap_err().to_string());
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