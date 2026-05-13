use crate::config::connect::{DataVendor, PulsarConnectConfig};
use anyhow::Context;
use faststr::FastStr;
use itertools::Itertools;
use pulsar::consumer::InitialPosition;
use std::{collections::HashMap, str::FromStr};
use taos::Dsn;
use taosx_core::{
    config::AdvancedOptions,
    utils::{self, codec::StringDecoder},
};
use taosx_utils::dsn::parse_simple_params;

#[derive(Debug, Clone)]
pub struct PulsarTaskConfig {
    pub connect: PulsarConnectConfig,

    pub timeout: i64,
    pub consumer_name: String,
    pub subscription: String,
    pub topics: Vec<String>,
    pub seek_to_end: Option<bool>,

    pub initial_position: InitialPosition,

    pub codec_processor: Option<StringDecoder>,
    pub advanced_options: AdvancedOptions,

    pub extras: Option<HashMap<FastStr, FastStr>>,
}

impl PulsarTaskConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let connect = PulsarConnectConfig::from_dsn(dsn)?;
        let (consumer_name, subscription, topics) = match connect.data_vendor {
            DataVendor::Tuya => {
                let access_id = connect
                    .tuya_access_id
                    .as_ref()
                    .ok_or(anyhow::anyhow!("tuya access_id is empty"))?;
                let consumer_name = format!("taosx-{}", access_id);
                let subscription = format!("{}-sub", access_id);
                let topics = vec![format!(
                    "{}/out/{}",
                    access_id,
                    connect
                        .tuya_env
                        .as_ref()
                        .ok_or(anyhow::anyhow!("tuya env is empty"))?
                        .get_value()
                )];
                (consumer_name, subscription, topics)
            }
            DataVendor::Standard => {
                let consumer_name = parse_simple_params(dsn, "consumer_name")?
                    .ok_or(anyhow::anyhow!("consumer_name is required"))?;
                let subscription = parse_simple_params(dsn, "subscription")?
                    .ok_or(anyhow::anyhow!("subscription is required"))?;
                let topics = Self::parse_topics(dsn)?;
                (consumer_name, subscription, topics)
            }
        };
        let config = PulsarTaskConfig {
            connect,
            timeout: Self::parse_timeout(dsn)?,
            consumer_name,
            subscription,
            topics,
            seek_to_end: parse_simple_params::<bool>(dsn, "seek_to_end")?,
            initial_position: Self::parse_initial_position(dsn)?,
            advanced_options: AdvancedOptions::from_dsn(dsn)?,
            extras: Self::parse_extras(dsn)?,
            codec_processor: Self::parse_codec_processor(dsn)?,
        };
        tracing::debug!("pulsar task config: {:?}", config);
        Ok(config)
    }

    pub fn parse_codec_processor(dsn: &Dsn) -> anyhow::Result<Option<StringDecoder>> {
        dsn.params
            .get("char_encoding")
            .map(|s| {
                s.parse()
                    .with_context(|| format!("invalid char_encoding: {s}"))
            })
            .transpose()
    }

    fn parse_topics(dsn: &Dsn) -> anyhow::Result<Vec<String>> {
        let topics = dsn
            .get("topics")
            .map(|s| {
                s.split(",")
                    .filter_map(|s| {
                        let s = s.trim();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s.to_string())
                        }
                    })
                    .collect::<Vec<String>>()
            })
            .ok_or(anyhow::anyhow!("topics is required"))?;
        if topics.is_empty() {
            anyhow::bail!("pulsar task config must have at least one topic");
        }
        Ok(topics)
    }

    pub fn parse_initial_position(dsn: &Dsn) -> anyhow::Result<InitialPosition> {
        Ok(parse_simple_params(dsn, "initial_position")?
            .map(|s: String| match s.to_lowercase().as_str() {
                "earliest" => Ok(InitialPosition::Earliest),
                "latest" => Ok(InitialPosition::Latest),
                _ => Err(anyhow::anyhow!("invalid initial_position: {}", s)),
            })
            .transpose()?
            .unwrap_or(InitialPosition::Earliest))
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

        let result = utils::parse_duration(timeout);
        match result {
            Ok(d) => Ok(d.as_millis() as i64),
            Err(e) => Err(anyhow::anyhow!(
                "invalid timeout: {}, cause: {}",
                timeout,
                e
            )),
        }
    }

    fn parse_extras(dsn: &Dsn) -> anyhow::Result<Option<HashMap<FastStr, FastStr>>> {
        let mut extras = HashMap::new();
        for (k, v) in dsn
            .params
            .iter()
            .filter(|(k, _)| !k.is_empty())
            .filter(|(k, _)| k.contains('.') && !utils::contains_uppercase(k))
            .map(|(k, v)| (k.trim(), v.trim()))
        {
            extras.insert(FastStr::from_str(k)?, FastStr::from_str(v)?);
        }
        if let Ok(str) = std::env::var("PULSAR_CONSUMER_EXTRAS") {
            tracing::debug!("use env PULSAR_CONSUMER_EXTRAS: {}", str);
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
    use taos::IntoDsn;

    use super::*;
    use crate::config::tuya::TuyaEnv;

    #[test]
    fn test_parse_seek_to() {
        let dsn = Dsn::from_str("pulsar://localhost:6650?topics=persistent://public/default/pt-zgc&consumer_name=c1&subscription=s1&seek_to_end=true").unwrap();
        let seek_to_end = PulsarTaskConfig::from_dsn(&dsn)
            .unwrap()
            .seek_to_end
            .unwrap();
        assert!(seek_to_end);

        let dsn = Dsn::from_str("pulsar://192.168.2.131:6650?topics=persistent://public/default/pt-zgc&consumer_name=c1&subscription=s1&seek_to_end=false").unwrap();
        let seek_to_end = PulsarTaskConfig::from_dsn(&dsn)
            .unwrap()
            .seek_to_end
            .unwrap();
        assert!(!seek_to_end);

        let dsn =
            Dsn::from_str("pulsar://localhost:6650?topics=t&consumer_name=c1&subscription=s1")
                .unwrap();
        let seek_to_end = PulsarTaskConfig::from_dsn(&dsn).unwrap().seek_to_end;
        assert!(seek_to_end.is_none());
    }

    #[test]
    fn test_consumer_name_subscription() {
        let dsn = Dsn::from_str(
            "pulsar://localhost:6650?subscription=taosx-sub&consumer_name=c1&topics=tp1,tp2",
        )
        .unwrap();
        let config = PulsarTaskConfig::from_dsn(&dsn).unwrap();
        assert_eq!("taosx-sub", &config.subscription);
        assert_eq!("c1", &config.consumer_name);
        assert_eq!("tp1", &config.topics[0]);
        assert_eq!("tp2", &config.topics[1]);

        let dsn =
            Dsn::from_str("pulsar://localhost:6650?subscription=&consumer_name=c1&topics=tp1,tp2")
                .unwrap();
        let config = PulsarTaskConfig::from_dsn(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str("pulsar://localhost:6650?consumer_name=c1&topics=tp1").unwrap();
        let config = PulsarTaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
    }

    #[test]
    fn test_tuya_consumer_name_subscription() {
        let access_id = "acsid";
        let tuya_env = "test";
        let expect_topic = format!(
            "{}/out/{}",
            access_id,
            TuyaEnv::try_from(tuya_env).unwrap().get_value()
        );

        let dsn = Dsn::from_str(&format!(
            "pulsarTuya://localhost:6650?tuya_access_id={}&tuya_access_key=tuyakey&tuya_env={}",
            access_id, tuya_env
        ))
        .unwrap();
        let config = PulsarTaskConfig::from_dsn(&dsn).unwrap();
        assert_eq!("taosx-acsid", &config.consumer_name);
        assert_eq!("acsid-sub", &config.subscription);
        assert_eq!(expect_topic, config.topics[0]);

        let dsn = Dsn::from_str(&format!(
            "pulsarTuya://localhost:6650?tuya_access_id={}&tuya_access_key=tuyakey",
            access_id
        ))
        .unwrap();
        let config = PulsarTaskConfig::from_dsn(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str(&format!(
            "pulsarTuya://localhost:6650?tuya_access_key=tuyakey&tuya_env={}",
            tuya_env
        ))
        .unwrap();
        let config = PulsarTaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
    }

    #[test]
    fn test_parse_timeout() {
        let dsn = Dsn::from_str("pulsar://localhost:6650?timeout=5s").unwrap();
        let result = PulsarTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(5000, result);

        let dsn = Dsn::from_str("pulsar://localhost:6650?timeout=30s").unwrap();
        let result = PulsarTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(30 * 1000, result);

        let dsn = Dsn::from_str("pulsar://localhost:6650?timeout=5m").unwrap();
        let result = PulsarTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(5 * 60 * 1000, result);

        let dsn = Dsn::from_str("pulsar://localhost:6650?timeout=6h").unwrap();
        let result = PulsarTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(6 * 3600 * 1000, result);

        let dsn = Dsn::from_str("pulsar://localhost:6650?timeout=1d").unwrap();
        let result = PulsarTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(24 * 3600 * 1000, result);

        let dsn = Dsn::from_str("pulsar://localhost:6650?").unwrap();
        let result = PulsarTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(-1, result);

        let dsn = Dsn::from_str("pulsar://localhost:6650?timeout=never").unwrap();
        let result = PulsarTaskConfig::parse_timeout(&dsn).unwrap();
        assert_eq!(-1, result);

        let dsn = Dsn::from_str("pulsar://localhost:6650?timeout=invalid").unwrap();
        let result = PulsarTaskConfig::parse_timeout(&dsn);
        assert!(result.is_err());
    }

    #[test]
    pub fn test_initial_position() {
        let dsn = Dsn::from_str("pulsar://localhost:6650?topics=tp1&consumer_name=c1&subscription=s1&initial_position=Latest").unwrap();
        let config = PulsarTaskConfig::from_dsn(&dsn).unwrap();
        dbg!(config.initial_position);
        let dsn =
            Dsn::from_str("pulsar://localhost:6650?topics=tp1&consumer_name=c1&subscription=s1")
                .unwrap();
        let config = PulsarTaskConfig::from_dsn(&dsn).unwrap();
        dbg!(config.initial_position);
    }

    #[tokio::test]
    async fn test_use_ssl() {
        let tmppath = tempfile::tempdir().unwrap();
        let cert_file = tmppath.path().join("cert.pem");
        let key_file = tmppath.path().join("key.pem");
        std::fs::write(&cert_file, "abc").unwrap();
        std::fs::write(&key_file, "def").unwrap();

        let dsn = format!(
            "pulsar://{}?cert=@{}&cert_key=@{}",
            "192.168.2.131:6650",
            cert_file.display(),
            key_file.display(),
        )
        .into_dsn()
        .expect("ssl dsn should be valid");

        let config = PulsarConnectConfig::from_dsn(&dsn).expect("config should success in test");
        let cert_chain = config.get_cert_chain();
        assert_eq!(&cert_chain, "abcdef".as_bytes());
    }
}
