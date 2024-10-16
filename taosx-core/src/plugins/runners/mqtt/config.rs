use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, Context};
use itertools::Itertools;
use rumqttc::{tokio_rustls::rustls, TlsConfiguration};
use taos::Dsn;

use crate::runners::{
    get_string_from_param_or_file, get_string_vec_from_param_or_file, NoCertificateVerification,
};

#[derive(Debug)]
pub struct MqttConfig {
    pub task: TaskConfig,
    pub mqtt: MqttConnectConfig,
    pub topics: HashMap<String, u8>,
    pub dump: Option<DumpConfig>,
}

#[derive(Debug)]
pub struct TaskConfig {
    pub batch_size: usize,
    pub batch_timeout: usize,
    pub unprocessed_messages_buffer_size: usize,
    pub maximum_processing_batch: usize,
}

impl TryFrom<&Dsn> for TaskConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        let parser = |key: &str| -> anyhow::Result<Option<usize>> {
            dsn.get(key)
                .map(|v| {
                    v.parse::<usize>()
                        .with_context(|| format!("invalid {key} number `{v}`"))
                })
                .transpose()
        };

        Ok(Self {
            batch_size: parser("batch_size")?.unwrap_or(1000),
            batch_timeout: parser("batch_timeout")?.unwrap_or(500),
            unprocessed_messages_buffer_size: parser("unprocessed_messages_buffer_size")?
                .unwrap_or(50000),
            maximum_processing_batch: parser("maximum_processing_batch")?.unwrap_or(100),
        })
    }
}

#[derive(Debug)]
pub struct DumpConfig {
    pub enable: bool,
    pub path: Option<String>,
    pub keep: usize,
}

impl DumpConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Self>> {
        let enable = dsn
            .params
            .get("keep_raw_data")
            .map(|v| {
                let v = v.trim();
                if v.is_empty() {
                    return Ok(true);
                }
                v.trim()
                    .parse::<bool>()
                    .with_context(|| format!("invalid keep_raw_data: `{v}`, require boolean value"))
            })
            .transpose()?
            .unwrap_or(false);
        if !enable {
            return Ok(None);
        }
        let path = dsn.params.get("keep_raw_data_dir").cloned();
        let keep = dsn
            .params
            .get("keep_raw_data_days")
            .map(|v| {
                v.parse::<usize>()
                    .context("parse keep_raw_data_days failed, which requires integer value")
            })
            .transpose()?
            .unwrap_or(1); // Default keep 1 day.

        Ok(Some(DumpConfig { enable, path, keep }))
    }
}

impl TryFrom<&Dsn> for MqttConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> anyhow::Result<Self> {
        let connect_config: MqttConnectConfig = MqttConnectConfig::try_from(dsn)?;
        let dump = DumpConfig::from_dsn(dsn)?;
        let topics_vec = get_string_vec_from_param_or_file(&mut dsn.clone(), "topics")
            .map_err(|err| anyhow::anyhow!("invalid topics, cause: {}", err.to_string()))?;

        let mut topics = HashMap::new();
        for topic in topics_vec {
            let pair = topic.split("::").collect_vec();
            if pair.len() != 2 {
                bail!("invalid topic: {topic}, cause: the format of topic is name::qos",);
            }
            let topic = String::from(pair[0]);
            let qos = pair[1]
                .parse::<u8>()
                .with_context(|| format!("invalid qos: {} in topic", pair[1]))?;
            topics.insert(topic, qos);
        }

        Ok(MqttConfig {
            task: dsn.try_into()?,
            mqtt: connect_config,
            topics,
            dump,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MqttConnectConfig {
    pub(crate) address: String,
    pub(crate) version: Version,
    pub(crate) client_id: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) keep_alive: Option<u64>,
    pub(crate) clean_session: Option<bool>,
    pub(crate) ca: Option<String>,
    pub(crate) cert: Option<String>,
    pub(crate) cert_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Version {
    V3,
    V5,
}

impl TryFrom<Dsn> for MqttConnectConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: Dsn) -> Result<Self, Self::Error> {
        (&dsn).try_into()
    }
}

impl TryFrom<&Dsn> for MqttConnectConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        let ca = dsn.get("ca");
        let ca = if ca.is_none() || ca.unwrap().is_empty() {
            Ok(None)
        } else {
            let ca = ca.unwrap();
            if ca.starts_with('@') {
                get_string_from_param_or_file(&mut dsn.clone(), "ca", true, None).map_err(|err| {
                    anyhow::anyhow!("failed to read ca config, cause: {}", err.to_string())
                })
            } else {
                Ok(Some(ca.to_string()))
            }
        }?;

        let cert = dsn.get("cert");
        let cert = if cert.is_none() || cert.unwrap().is_empty() {
            Ok(None)
        } else {
            let cert = cert.unwrap();
            if cert.starts_with('@') {
                get_string_from_param_or_file(&mut dsn.clone(), "cert", true, None).map_err(|err| {
                    anyhow::anyhow!("failed to read cert config, cause: {}", err.to_string())
                })
            } else {
                Ok(Some(cert.to_string()))
            }
        }?;

        let cert_key = dsn.get("cert_key");
        let cert_key = if cert_key.is_none() || cert_key.unwrap().is_empty() {
            Ok(None)
        } else {
            let cert_key = cert_key.unwrap();
            if cert_key.starts_with('@') {
                get_string_from_param_or_file(&mut dsn.clone(), "cert_key", true, None).map_err(
                    |err| anyhow::anyhow!("failed to read cert config, cause: {}", err.to_string()),
                )
            } else {
                Ok(Some(cert_key.to_string()))
            }
        }?;

        let host = dsn
            .addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or(anyhow::anyhow!("host is required"))?;
        let port = dsn
            .addresses
            .first()
            .and_then(|addr| addr.port)
            .ok_or(anyhow::anyhow!("port is required"))?;
        let address = if ca.is_some() {
            format!("ssl://{host}:{port}")
        } else {
            format!("tcp://{host}:{port}")
        };

        Ok(MqttConnectConfig {
            address,
            version: parse_version(dsn)?,
            client_id: parse_client_id(dsn)?,
            username: dsn.username.clone(),
            password: dsn.password.clone(),
            keep_alive: parse_keep_alive(dsn)?,
            clean_session: parse_clean_session(dsn)?,
            ca,
            cert,
            cert_key,
        })
    }
}

impl MqttConnectConfig {
    pub fn host_port(&self) -> anyhow::Result<(String, u16)> {
        let parts = self.address.split(':').collect::<Vec<&str>>();
        let host = parts
            .get(1)
            .context("MQTT host not found")?
            .trim_start_matches("//")
            .to_string();
        let port = parts
            .get(2)
            .context("MQTT port not found")?
            .parse::<u16>()
            .context("MQTT port invalid")?;
        Ok((host, port))
    }

    /// Default keep alive is 5 seconds
    pub fn keep_alive(&self) -> core::time::Duration {
        core::time::Duration::from_secs(self.keep_alive.unwrap_or(5))
    }

    /// Default clean session is true
    pub fn clean_session(&self) -> bool {
        self.clean_session.unwrap_or(true)
    }

    pub fn ssl(&self) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let ca = self.ca.as_ref().map(|v| v.as_bytes().to_vec())?;
        let cert = self.cert.as_ref().map(|v| v.as_bytes().to_vec())?;
        let cert_key = self.cert_key.as_ref().map(|v| v.as_bytes().to_vec())?;
        Some((ca, cert, cert_key))
    }
}

pub fn build_tls_config(
    ca: Vec<u8>,
    _client_pem: Vec<u8>,
    _client_key: Vec<u8>,
) -> anyhow::Result<TlsConfiguration> {
    let mut ca = std::io::Cursor::new(ca);

    use itertools::Itertools;
    let certs: Vec<_> = rustls_pemfile::certs(&mut ca).try_collect().unwrap();
    let mut root_cert_store = rustls::RootCertStore::empty();
    root_cert_store.add_parsable_certificates(
        rustls_native_certs::load_native_certs().expect("could not load platform certs"),
    );
    root_cert_store.add_parsable_certificates(certs);
    let mut rustls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();
    rustls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification()));
    let tls_config = TlsConfiguration::Rustls(Arc::new(rustls_config));

    Ok(tls_config)
}

fn parse_keep_alive(dsn: &Dsn) -> anyhow::Result<Option<u64>> {
    let keep_alive = dsn
        .params
        .get("keep_alive")
        .map(|v| {
            let v = v
                .parse::<u64>()
                .with_context(|| format!("invalid keep_alive value: {v}"))?;
            anyhow::ensure!(v >= 5, "The value of keep_alive must be at least 5");
            Ok(v)
        })
        .transpose()?;
    Ok(keep_alive)
}

fn parse_version(dsn: &Dsn) -> anyhow::Result<Version> {
    dsn.params
        .get("version")
        .filter(|s| !s.is_empty())
        .map(|v| match v.as_str() {
            "3.1" | "3.1.1" => Ok(Version::V3),
            "5.0" | "5" => Ok(Version::V5),
            _ => bail!("Invalid MQTT version: {v}"),
        })
        .transpose()?
        .context("MQTT version is required")
}

fn parse_clean_session(dsn: &Dsn) -> anyhow::Result<Option<bool>> {
    dsn.params
        .get("clean_session")
        .map(|v| {
            v.parse::<bool>()
                .with_context(|| format!("invalid clean session: {v}"))
        })
        .transpose()
}

fn parse_client_id(dsn: &Dsn) -> anyhow::Result<String> {
    dsn.params
        .get("client_id")
        .cloned()
        .context("MQTT client id is requeired")
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_version() {
        let dsn = Dsn::from_str("mqtt://").unwrap();
        let version = parse_version(&dsn);
        assert!(version.is_err());

        let dsn = Dsn::from_str("mqtt://?version=3.0").unwrap();
        let version = parse_version(&dsn);
        assert!(version.is_err());

        let dsn = Dsn::from_str("mqtt://?version=3.1").unwrap();
        let version = parse_version(&dsn).unwrap();
        assert_eq!(Version::V3, version);

        let dsn = Dsn::from_str("mqtt://?version=3.1.1").unwrap();
        let version = parse_version(&dsn).unwrap();
        assert_eq!(Version::V3, version);

        let dsn = Dsn::from_str("mqtt://?version=5.0").unwrap();
        let version = parse_version(&dsn).unwrap();
        assert_eq!(Version::V5, version);

        let dsn = Dsn::from_str("mqtt://?version=5").unwrap();
        let version = parse_version(&dsn).unwrap();
        assert_eq!(Version::V5, version);
    }

    #[test]
    fn test_host_port() {
        let dsn = Dsn::from_str("mqtt://127.0.0.1:1884?version=3.1&client_id=1").unwrap();
        let config = MqttConnectConfig::try_from(&dsn).unwrap();
        assert_eq!(config.host_port().unwrap(), ("127.0.0.1".to_string(), 1884));
    }

    #[test]
    fn test_parse_keep_alive() {
        let dsn = Dsn::from_str("mqtt://?keep_alive=30").unwrap();
        let keep_alive = parse_keep_alive(&dsn).unwrap();
        assert_eq!(Some(30), keep_alive);

        let dsn = Dsn::from_str("mqtt://?keep_alive=abc").unwrap();
        let keep_alive = parse_keep_alive(&dsn);
        assert!(keep_alive.is_err());

        let dsn = Dsn::from_str("mqtt://").unwrap();
        let keep_alive = parse_keep_alive(&dsn);
        assert!(keep_alive.is_ok_and(|s| s.is_none()));

        let dsn = Dsn::from_str("mqtt://?keep_alive=").unwrap();
        let keep_alive = parse_keep_alive(&dsn);
        assert!(keep_alive.is_err());
    }

    #[test]
    #[ignore]
    fn test_mqtt_connect_config_from_dsn() {
        let dsn = Dsn::from_str("mqtt://").unwrap();
        let config = MqttConnectConfig::try_from(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str("mqtt://127.0.0.1").unwrap();
        let config = MqttConnectConfig::try_from(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833").unwrap();
        let config = MqttConnectConfig::try_from(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.1.1&client_id=1").unwrap();
        let config = MqttConnectConfig::try_from(&dsn).unwrap();
        assert_eq!(config.version, Version::V3);

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60").unwrap();
        let config = MqttConnectConfig::try_from(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.1&keep_alive=abc").unwrap();
        let config = MqttConnectConfig::try_from(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str(
            "mqtt://127.0.0.1:1833?client_id=123&version=3.1&keep_alive=60&clean_session=true",
        )
        .unwrap();
        let config = MqttConnectConfig::try_from(&dsn).unwrap();
        assert_eq!("tcp://127.0.0.1:1833", config.address);
        assert_eq!(Version::V3, config.version);
        assert_eq!("123", config.client_id);
        assert_eq!(None, config.username);
        assert_eq!(None, config.password);
        assert_eq!(60, config.keep_alive.unwrap());
        assert!(config.clean_session.unwrap());
        assert_eq!(None, config.ca);
        assert_eq!(None, config.cert);
        assert_eq!(None, config.cert_key);
    }

    #[test]
    fn test_mqtt_config_from() {
        let dsn =
            Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true")
                .unwrap();
        let config = MqttConfig::try_from(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str(
            "mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=a,b,c",
        )
        .unwrap();
        let config = MqttConfig::try_from(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str(
            "mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::abc",
        )
        .unwrap();
        let config = MqttConfig::try_from(&dsn);
        assert!(config.is_err());

        let dsn = Dsn::from_str(
            "mqtt://127.0.0.1:1833?client_id=1&version=3.1&keep_alive=60&clean_session=true&topics=tp1::0",
        )
        .unwrap();
        let config = MqttConfig::try_from(&dsn);
        assert!(config.is_ok());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?client_id=1&version=3.1.1&topics=tp1::0,tp2::1,tp3::2&keep_alive=60&clean_session=true").unwrap();
        let config = MqttConfig::try_from(&dsn).unwrap();
        assert_eq!(3, config.topics.len());
        assert_eq!(0, *config.topics.get("tp1").unwrap());
        assert_eq!(1, *config.topics.get("tp2").unwrap());
        assert_eq!(2, *config.topics.get("tp3").unwrap());
    }

    #[test]
    fn test_mqtt_dump() {
        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?client_id=1&version=3.1.1&keep_alive=60&clean_session=true&topics=tp1::0,tp2::1,tp3::2").unwrap();
        let config = MqttConfig::try_from(&dsn).unwrap();
        assert_eq!(3, config.topics.len());
        assert_eq!(0, *config.topics.get("tp1").unwrap());
        assert_eq!(1, *config.topics.get("tp2").unwrap());
        assert_eq!(2, *config.topics.get("tp3").unwrap());
        assert!(config.dump.is_none());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?client_id&version=3.1&keep_alive=60&clean_session=true&topics=tp1::0,tp2::1,tp3::2&keep_raw_data&keep_raw_data_dir=./abc").unwrap();
        let config = MqttConfig::try_from(&dsn).unwrap();
        let dump = config.dump.unwrap();
        assert!(dump.enable);
        assert_eq!(dump.path, Some("./abc".to_owned()));
        assert_eq!(dump.keep, 1);
    }

    #[test]
    fn task_config_test() {
        let dsn = Dsn::from_str("mqtt://127.0.0.1:1883?client_id=1&version=5&topics=tp1::0&batch_size=1&batch_timeout=2&unprocessed_message_buffer_size=3&maximum_processing_batch=4").unwrap();
        let config = MqttConfig::try_from(&dsn).unwrap();
        let task = config.task;
        assert_eq!(task.batch_size, 1);
        assert_eq!(task.batch_timeout, 2);
        assert_eq!(task.unprocessed_messages_buffer_size, 3);
        assert_eq!(task.maximum_processing_batch, 4);
    }

    #[test]
    fn parse_task_config() {
        {
            let dsn = Dsn::from_str("mqtt://localhost:1883?client=123&version=5&topics=tp1::0&batch_size=1&batch_timeout=2&unprocessed_messages_buffer_size=3&maximum_processing_batch=4").unwrap();
            let config = TaskConfig::try_from(&dsn).unwrap();
            assert_eq!(config.batch_size, 1);
            assert_eq!(config.batch_timeout, 2);
            assert_eq!(config.unprocessed_messages_buffer_size, 3);
            assert_eq!(config.maximum_processing_batch, 4);
        }
        {
            let dsn =
                Dsn::from_str("mqtt://localhost:1883?client=123&version=5&topics=tp1::0").unwrap();
            let config = TaskConfig::try_from(&dsn).unwrap();
            assert_eq!(config.batch_size, 1000);
            assert_eq!(config.batch_timeout, 500);
            assert_eq!(config.unprocessed_messages_buffer_size, 50000);
            assert_eq!(config.maximum_processing_batch, 100);
        }
    }
}
