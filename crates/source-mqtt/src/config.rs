use std::{collections::HashMap, io::Read, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use itertools::Itertools;
use rumqttc::{TlsConfiguration, tokio_rustls::rustls};
use taos::Dsn;

use taosx_core::{
    runners::NoCertificateVerification,
    utils::{
        codec::{Decompressor, StringDecoder},
        dsn::parse_simple_params,
    },
};

use super::{client::Version, topic::TopicPattern};

#[derive(Debug)]
pub struct MqttConfig {
    pub task: TaskConfig,
    pub mqtt: MqttConnectConfig,
    pub topics: HashMap<String, u8>,
    pub topic_pattern: Option<TopicPattern>,
    pub dump: Option<DumpConfig>,
    pub persist_data: Option<PersistDataConfig>,
    pub codec_processor: (Option<Decompressor>, Option<StringDecoder>),
}

#[derive(Debug, PartialEq)]
pub struct TaskConfig {
    pub batch_size: usize,
    /// timeout unit: ms
    pub batch_timeout: usize,
    pub unprocessed_messages_buffer_size: usize,
    pub maximum_processing_batch: usize,
}

impl TryFrom<&Dsn> for TaskConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> Result<Self, Self::Error> {
        Ok(Self {
            batch_size: parse_simple_params(dsn, "batch_size")?.unwrap_or(1000),
            batch_timeout: parse_simple_params(dsn, "batch_timeout")?.unwrap_or(500),
            unprocessed_messages_buffer_size: parse_simple_params(
                dsn,
                "unprocessed_messages_buffer_size",
            )?
            .unwrap_or(50000),
            maximum_processing_batch: parse_simple_params(dsn, "maximum_processing_batch")?
                .unwrap_or(100),
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct DumpConfig {
    pub enable: bool,
    pub path: Option<PathBuf>,
    pub keep: usize,
}

impl DumpConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Self>> {
        let enable = parse_simple_params(dsn, "keep_raw_data")?.is_some_and(|v| v);
        if !enable {
            return Ok(None);
        }
        let path = parse_simple_params(dsn, "keep_raw_data_dir")?;
        let keep = parse_simple_params(dsn, "keep_raw_data_days")?.unwrap_or(1); // Default keep 1 day.

        Ok(Some(DumpConfig { enable, path, keep }))
    }
}

#[derive(Debug, PartialEq)]
pub struct PersistDataConfig {
    pub dir: Option<PathBuf>,
}

impl PersistDataConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Self>> {
        let enable = parse_simple_params(dsn, "persist_data_enable")?.is_some_and(|v| v);
        if !enable {
            return Ok(None);
        }

        let dir = parse_simple_params(dsn, "persist_data_dir")?;
        Ok(Some(Self { dir }))
    }
}

impl TryFrom<&Dsn> for MqttConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(MqttConfig {
            task: dsn.try_into()?,
            mqtt: dsn.try_into()?,
            topics: parse_topics(dsn)?,
            dump: DumpConfig::from_dsn(dsn)?,
            persist_data: PersistDataConfig::from_dsn(dsn)?,
            codec_processor: parse_codec_processor(dsn)?,
            topic_pattern: parse_simple_params(dsn, "topic_pattern")?,
        })
    }
}

impl TryFrom<Dsn> for MqttConfig {
    type Error = anyhow::Error;

    fn try_from(dsn: Dsn) -> anyhow::Result<Self> {
        (&dsn).try_into()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct MqttConnectConfig {
    pub host: String,
    pub port: u16,
    pub version: Version,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub certificates: Option<Certificates>,
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
        let (host, port) = parse_host_port(dsn)?;

        Ok(MqttConnectConfig {
            host,
            port,
            version: parse_version(dsn)?,
            client_id: parse_client_id(dsn)?,
            username: dsn.username.clone(),
            password: dsn.password.clone(),
            keep_alive: parse_keep_alive(dsn)?,
            clean_session: parse_clean_session(dsn)?,
            certificates: parse_tls_certificates(dsn)?,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Certificates {
    ca: Vec<u8>,
    cert: Option<Vec<u8>>,
    cert_key: Option<Vec<u8>>,
}

pub fn parse_tls_certificates(dsn: &Dsn) -> anyhow::Result<Option<Certificates>> {
    let ca = parse_from_param_or_file(dsn, "ca")?;
    let cert = parse_from_param_or_file(dsn, "cert")?;
    let cert_key = parse_from_param_or_file(dsn, "cert_key")?;

    Ok(ca.map(|ca| Certificates { ca, cert, cert_key }))
}

pub fn build_tls_config(certificates: Option<&Certificates>) -> anyhow::Result<TlsConfiguration> {
    let mut root_cert_store = rustls::RootCertStore::empty();
    // 添加机器上的 ca
    let result = rustls_native_certs::load_native_certs();
    if !result.certs.is_empty() {
        root_cert_store.add_parsable_certificates(result.certs);
    }

    let tls_config = match certificates {
        Some(certificates) => {
            // 添加用户 ca
            let mut ca = std::io::Cursor::new(certificates.ca.clone());
            let root_certs = rustls_pemfile::certs(&mut ca)
                .collect::<Result<Vec<_>, _>>()
                .context("Parse CA file error")?;
            if root_certs.is_empty() {
                anyhow::bail!("No valid CA cert in chain");
            }
            root_cert_store.add_parsable_certificates(root_certs);
            let rustls_config =
                rustls::ClientConfig::builder().with_root_certificates(root_cert_store);
            // 添加用户 cert 和 key
            match certificates
                .cert
                .as_ref()
                .and_then(|cert| certificates.cert_key.as_ref().map(|key| (cert, key)))
            {
                Some((cert, key)) => {
                    let client_certs = rustls_pemfile::certs(&mut std::io::Cursor::new(cert))
                        .collect::<Result<Vec<_>, _>>()?;
                    if client_certs.is_empty() {
                        anyhow::bail!("No valid client cert in chain");
                    }
                    let mut keys_reader = std::io::Cursor::new(key);

                    let key = loop {
                        let item = rustls_pemfile::read_one(&mut keys_reader)
                            .context("Read one ca key error")?;
                        match item {
                            Some(rustls_pemfile::Item::Sec1Key(key)) => {
                                break key.into();
                            }
                            Some(rustls_pemfile::Item::Pkcs1Key(key)) => {
                                break key.into();
                            }
                            Some(rustls_pemfile::Item::Pkcs8Key(key)) => {
                                break key.into();
                            }
                            None => anyhow::bail!("No valid key in chain"),
                            _ => {}
                        }
                    };
                    rustls_config
                        .with_client_auth_cert(client_certs, key)
                        .context("Build with client auth error")?
                }
                None => {
                    // 没有客户端 cert，设置单向认证
                    rustls_config.with_no_client_auth()
                }
            }
        }
        None => {
            // 没有ca，设置为不认证
            let mut rustls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();
            rustls_config
                .dangerous()
                .set_certificate_verifier(Arc::new(NoCertificateVerification));
            rustls_config
        }
    };

    let tls_config = TlsConfiguration::Rustls(Arc::new(tls_config));

    Ok(tls_config)
}

pub fn parse_host_port(dsn: &Dsn) -> anyhow::Result<(String, u16)> {
    dsn.addresses
        .first()
        .and_then(|addr| addr.host.clone().zip(addr.port))
        .context("mqtt invalid address")
}

pub fn parse_keep_alive(dsn: &Dsn) -> anyhow::Result<Duration> {
    let keep_alive = parse_simple_params::<u64>(dsn, "keep_alive")?.unwrap_or(5);
    anyhow::ensure!(
        keep_alive >= 5,
        "The value of keep_alive must be at least 5"
    );
    Ok(Duration::from_secs(keep_alive))
}

pub fn parse_version(dsn: &Dsn) -> anyhow::Result<Version> {
    parse_simple_params(dsn, "version")?.context("MQTT version is required")
}

pub fn parse_client_id(dsn: &Dsn) -> anyhow::Result<String> {
    dsn.get("client_id")
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .context("MQTT client id is requeired")
}

pub fn parse_clean_session(dsn: &Dsn) -> anyhow::Result<bool> {
    Ok(parse_simple_params(dsn, "clean_session")?.unwrap_or(true))
}

pub fn parse_topics(dsn: &Dsn) -> anyhow::Result<HashMap<String, u8>> {
    dsn.get("topics")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .context("topics requeired")?
        .split(',')
        .map(|topic| {
            topic
                .split_once("::")
                .with_context(|| format!("invalid topic: {topic}, format should be: `topic::qos`"))
                .and_then(|(topic, qos)| {
                    qos.parse::<u8>()
                        .context("invalid qos")
                        .map(|qos| (topic.to_string(), qos))
                })
        })
        .try_collect()
}

fn parse_codec_processor(
    dsn: &Dsn,
) -> anyhow::Result<(Option<Decompressor>, Option<StringDecoder>)> {
    Ok((
        parse_simple_params(dsn, "compression")?,
        parse_simple_params(dsn, "char_encoding")?,
    ))
}

fn parse_from_param_or_file(dsn: &Dsn, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
    fn read_from_file(path: &str) -> std::io::Result<Vec<u8>> {
        let path = std::fs::canonicalize(path)?;
        let mut file = std::fs::File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }
    dsn.get(key)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.strip_prefix('@')
                .map(|path| read_from_file(path).with_context(|| format!("read {key} file error")))
                .unwrap_or(Ok(v.as_bytes().to_vec()))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::{io::Write, str::FromStr};

    use super::*;

    #[test]
    fn test_parse_version() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://")?;
        let version = parse_version(&dsn);
        assert!(version.is_err());

        let dsn = Dsn::from_str("mqtt://?version=3.0")?;
        let version = parse_version(&dsn);
        assert!(version.is_err());

        let dsn = Dsn::from_str("mqtt://?version=3.1")?;
        let version = parse_version(&dsn)?;
        assert_eq!(Version::V3, version);

        let dsn = Dsn::from_str("mqtt://?version=3.1.1")?;
        let version = parse_version(&dsn)?;
        assert_eq!(Version::V3, version);

        let dsn = Dsn::from_str("mqtt://?version=5.0")?;
        let version = parse_version(&dsn)?;
        assert_eq!(Version::V5, version);

        let dsn = Dsn::from_str("mqtt://?version=5")?;
        let version = parse_version(&dsn)?;
        assert_eq!(Version::V5, version);

        Ok(())
    }

    #[test]
    fn test_host_port() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://127.0.0.1:1884")?;
        let (host, port) = parse_host_port(&dsn)?;
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 1884);

        let dsn = Dsn::from_str("mqtt://127.0.0.1:")?;
        assert!(parse_host_port(&dsn).is_err());
        let dsn = Dsn::from_str("mqtt://:1883")?;
        assert!(parse_host_port(&dsn).is_err());

        let dsn = Dsn::from_str("mqtt://:")?;
        assert!(parse_host_port(&dsn).is_err());
        Ok(())
    }

    #[test]
    fn test_parse_keep_alive() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://?keep_alive=30")?;
        let keep_alive = parse_keep_alive(&dsn)?;
        assert_eq!(keep_alive, Duration::from_secs(30));

        let dsn = Dsn::from_str("mqtt://?keep_alive=abc")?;
        let keep_alive = parse_keep_alive(&dsn);
        assert!(keep_alive.is_err());

        let dsn = Dsn::from_str("mqtt://")?;
        let keep_alive = parse_keep_alive(&dsn)?;
        assert_eq!(keep_alive, Duration::from_secs(5));

        let dsn = Dsn::from_str("mqtt://?keep_alive=")?;
        let keep_alive = parse_keep_alive(&dsn)?;
        assert_eq!(keep_alive, Duration::from_secs(5));

        let dsn = Dsn::from_str("mqtt://?keep_alive=1")?;
        assert!(parse_keep_alive(&dsn).is_err());
        Ok(())
    }

    #[test]
    fn parse_client_id_test() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://?client_id=")?;
        assert!(parse_client_id(&dsn).is_err());

        let dsn = Dsn::from_str("mqtt://?")?;
        assert!(parse_client_id(&dsn).is_err());

        let dsn = Dsn::from_str("mqtt://?client_id=abc")?;
        assert_eq!(&parse_client_id(&dsn)?, "abc");
        Ok(())
    }

    #[test]
    fn parse_params_or_file_test() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://?ca=abc")?;
        let ca = parse_from_param_or_file(&dsn, "ca")?.context("ca not found")?;
        assert_eq!(ca, b"abc");

        let mut ca_file = tempfile::NamedTempFile::new()?;
        ca_file.write_all(b"abc")?;
        let dsn = Dsn::from_str(&format!("mqtt://?ca=@{}", ca_file.path().display()))?;
        let ca = parse_from_param_or_file(&dsn, "ca")?.context("ca not found")?;
        assert_eq!(ca, b"abc");

        Ok(())
    }

    #[test]
    fn parse_tls_test() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://?")?;
        let certs = parse_tls_certificates(&dsn)?;
        assert!(certs.is_none());

        let dsn = Dsn::from_str("mqtt://?ca=")?;
        let certs = parse_tls_certificates(&dsn)?;
        assert!(certs.is_none());

        let dsn = Dsn::from_str("mqtt://?ca=abc&cert=def&cert_key=ghi")?;
        let certs = parse_tls_certificates(&dsn)?.context("certs not found")?;
        assert_eq!(certs.ca, b"abc");
        assert_eq!(certs.cert, Some(b"def".into()));
        assert_eq!(certs.cert_key, Some(b"ghi".into()));

        let dsn = Dsn::from_str("mqtt://?ca=abc&cert=&cert_key=ghi")?;
        let certs = parse_tls_certificates(&dsn)?.context("certs not found")?;
        assert_eq!(certs.ca, b"abc");
        assert_eq!(certs.cert, None);
        assert_eq!(certs.cert_key, Some(b"ghi".into()));

        let dsn = Dsn::from_str("mqtt://?ca=abc&cert_key=ghi")?;
        let certs = parse_tls_certificates(&dsn)?.context("certs not found")?;
        assert_eq!(certs.ca, b"abc");
        assert_eq!(certs.cert, None);
        assert_eq!(certs.cert_key, Some(b"ghi".into()));
        Ok(())
    }

    #[test]
    fn parse_task_config_test() -> anyhow::Result<()> {
        let dsn = Dsn::from_str(
            "mqtt://?batch_size=1&batch_timeout=2&unprocessed_messages_buffer_size=3&maximum_processing_batch=4",
        )?;
        let config = TaskConfig::try_from(&dsn)?;
        assert_eq!(config.batch_size, 1);
        assert_eq!(config.batch_timeout, 2);
        assert_eq!(config.unprocessed_messages_buffer_size, 3);
        assert_eq!(config.maximum_processing_batch, 4);

        let dsn = Dsn::from_str("mqtt://")?;
        let config = TaskConfig::try_from(&dsn)?;
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.batch_timeout, 500);
        assert_eq!(config.unprocessed_messages_buffer_size, 50000);
        assert_eq!(config.maximum_processing_batch, 100);

        Ok(())
    }

    #[test]
    fn parse_dump_config() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://?keep_raw_data=true")?;
        let dump = DumpConfig::from_dsn(&dsn)?.context("dump not found")?;
        assert!(dump.enable);
        assert_eq!(dump.keep, 1);
        assert_eq!(dump.path, None);

        let dsn = Dsn::from_str(
            "mqtt://?keep_raw_data=true&keep_raw_data_dir=/a/b/c&keep_raw_data_days=4",
        )?;
        let dump = DumpConfig::from_dsn(&dsn)?.context("dump not found")?;
        assert!(dump.enable);
        assert_eq!(dump.keep, 4);
        assert_eq!(dump.path, Some(PathBuf::from("/a/b/c")));

        let dsn = Dsn::from_str("mqtt://")?;
        let dump = DumpConfig::from_dsn(&dsn)?;
        assert!(dump.is_none());

        let dsn = Dsn::from_str("mqtt://?keep_raw_data=false")?;
        let dump = DumpConfig::from_dsn(&dsn)?;
        assert!(dump.is_none());

        Ok(())
    }

    #[test]
    fn parse_clean_session_test() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://")?;
        assert!(parse_clean_session(&dsn)?);

        let dsn = Dsn::from_str("mqtt://?clean_session=")?;
        assert!(parse_clean_session(&dsn)?);

        let dsn = Dsn::from_str("mqtt://?clean_session=true")?;
        assert!(parse_clean_session(&dsn)?);

        let dsn = Dsn::from_str("mqtt://?clean_session=false")?;
        assert!(!parse_clean_session(&dsn)?);
        Ok(())
    }

    #[test]
    fn parse_topics_test() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://?topics=")?;
        assert!(parse_topics(&dsn).is_err());

        let dsn = Dsn::from_str("mqtt://")?;
        assert!(parse_topics(&dsn).is_err());

        let dsn = Dsn::from_str("mqtt://?topics=tp1")?;
        assert!(parse_topics(&dsn).is_err());

        let dsn = Dsn::from_str("mqtt://?topics=tp1:0")?;
        assert!(parse_topics(&dsn).is_err());

        let dsn = Dsn::from_str("mqtt://?topics=tp1,tp2")?;
        assert!(parse_topics(&dsn).is_err());

        let dsn = Dsn::from_str("mqtt://?topics=tp1::0,tp2::1")?;
        let topics = parse_topics(&dsn)?;
        assert_eq!(
            topics,
            HashMap::from_iter([("tp1".into(), 0), ("tp2".into(), 1)])
        );
        Ok(())
    }

    #[test]
    fn parse_codec_processor_test() -> anyhow::Result<()> {
        let dsn = Dsn::from_str("mqtt://")?;
        assert_eq!(parse_codec_processor(&dsn)?, (None, None));

        let dsn = Dsn::from_str("mqtt://?compression=gzip")?;
        assert_eq!(
            parse_codec_processor(&dsn)?,
            (Some(Decompressor::Gzip), None)
        );

        let dsn = Dsn::from_str("mqtt://?char_encoding=UTF_8")?;
        assert_eq!(
            parse_codec_processor(&dsn)?,
            (None, Some(StringDecoder::Utf8))
        );
        Ok(())
    }

    #[test]
    fn build_tls_config_test() -> anyhow::Result<()> {
        let res = build_tls_config(Some(&Certificates {
            ca: b"
-----BEGIN CERTIFICATE-----
MIIFDzCCA3egAwIBAgIQSL1JEpBqVfNDYePUWb6m3DANBgkqhkiG9w0BAQsFADCB
nzEeMBwGA1UEChMVbWtjZXJ0IGRldmVsb3BtZW50IENBMTowOAYDVQQLDDF5YW55
dXhpbmdAeWFueXV4aW5nZGVNYWMtU3R1ZGlvLmxvY2FsICjpl6vlrofmmJ8pMUEw
PwYDVQQDDDhta2NlcnQgeWFueXV4aW5nQHlhbnl1eGluZ2RlTWFjLVN0dWRpby5s
b2NhbCAo6Zer5a6H5pifKTAeFw0yNDAyMjUxNDEzNTJaFw0zNDAyMjUxNDEzNTJa
MIGfMR4wHAYDVQQKExVta2NlcnQgZGV2ZWxvcG1lbnQgQ0ExOjA4BgNVBAsMMXlh
bnl1eGluZ0B5YW55dXhpbmdkZU1hYy1TdHVkaW8ubG9jYWwgKOmXq+Wuh+aYnykx
QTA/BgNVBAMMOG1rY2VydCB5YW55dXhpbmdAeWFueXV4aW5nZGVNYWMtU3R1ZGlv
LmxvY2FsICjpl6vlrofmmJ8pMIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKC
AYEA0mjpCN7OeJhKTXKAsEFZ2GenuD16sFwPtR3XWJSpdUj2lwKYaSF4sRD5/fb9
zFj9q1nvRp2tBwRFG/QIlXPzLiba/+7yhJvH5Iredg/tN3iwOJPwRXCHGUQH+c3B
U31PSirl0VDpe/LQzG2iaud5UqdNkOOKOxk21spqbCJe6dccFizi5Y9Q+fYFPGfj
6QU55ZX72ZRRkIbengSKj3yrOwmwUGICRytk6I3+DJNOi9DRTW0bDDO77AjhB6e1
4Z7Un3R7A1emr38LRygrNF3T4/l9npS9K2SSYbQK1cIpG+2KoiMTCbaCMgD9rNZp
0wgwVM2LOQg4SARsgAeLQFxPpnD2O5f1tFgelP+Fh0laOKpZCZLSWv4G20x710Xh
lTr00vQ64CI0++VDsNtVi3bZQ+YROrMepv/huqA2WWnZj5JGSgx0nob5NbVTOqAj
aFLBgjpA4aOlrLD9J2K1f4fhPOFiZud6CUKNMEiSoMQyHOpGiriE/juxVJ3s0LdI
ev5rAgMBAAGjRTBDMA4GA1UdDwEB/wQEAwICBDASBgNVHRMBAf8ECDAGAQH/AgEA
MB0GA1UdDgQWBBQJu/tU1aRs8H0ShzlD0bkWQbRhMTANBgkqhkiG9w0BAQsFAAOC
AYEAmOY8sTs0D7P0P0avdaDV40/Crfexet0i16DKqjT1w9V7P9VueewA6qZF5sbk
yugRCxCJARzrZJKn58aa3nAM9VvcoXA/iwrdbP83vKLs3Nq83pw5OzQWbFPtpfzs
LVr9ZMcoOibDQD6AocW+BWaIn3CcYNlzBM8GOc3iLHNRHdduDl2z9k+rtSo6hLbT
oidjYS6an7gYAbbYWoQZfBscFMjs7yyj8K//GUnmsVl285rkeOo6BiJyqjrbZkGr
cpY3WfTfoCear1jGxu5rUJJy1GbBu36pTzNLFObyvVWFzz1t5xtoQeu8rtXq0lUy
42uQY1HyFjwDNIXxl044R46hTCjY41bc/5cX8TbT7kSfZ3gE9H4njFYSBcb7Nv3J
n6LGgw+tpZuDUpQXxG12TbLa8N5i2ScLm4SCeHQs1nXEfc7uUZcqKGKnUStUheeD
9LgliGorTlSHG/7WbqHHNGB2VKsXuFJ3YkM5f+yj3v6wLeW/iM4fhkajHpJzMEGg
JGMv
-----END CERTIFICATE-----
        "
            .to_vec(),
            cert: None,
            cert_key: None,
        }));
        assert!(res.is_ok());
        Ok(())
    }

    #[test]
    fn parse_mqtt_config_test() -> anyhow::Result<()> {
        let params = vec![
            "batch_size=100",
            "batch_timeout=200",
            "unprocessed_messages_buffer_size=300",
            "maximum_processing_batch=400",
            "keep_raw_data=true",
            "keep_raw_data_dir=/a/b/c",
            "keep_raw_data_days=7",
            "ca=abc",
            "cert=def",
            "cert_key=ghi",
            "keep_alive=5",
            "version=5",
            "client_id=aaa",
            "clean_session=false",
            "topics=tp1::0,tp2::1",
            "compression=none",
            "char_encoding=GBK",
        ]
        .join("&");
        let dsn_str = format!("mqtt://root:taosdata@localhost:1883?{}", params);
        let dsn = Dsn::from_str(&dsn_str)?;
        let config = MqttConfig::try_from(&dsn)?;
        assert_eq!(
            config.codec_processor,
            (Some(Decompressor::Noop), Some(StringDecoder::GBK))
        );
        let dump = config.dump.context("dump")?;
        assert_eq!(
            dump,
            DumpConfig {
                enable: true,
                path: Some(PathBuf::from("/a/b/c")),
                keep: 7
            }
        );

        let conn = config.mqtt;
        assert_eq!(
            conn,
            MqttConnectConfig {
                host: "localhost".to_string(),
                port: 1883,
                version: Version::V5,
                client_id: "aaa".to_string(),
                username: Some("root".to_string()),
                password: Some("taosdata".to_string()),
                keep_alive: Duration::from_secs(5),
                clean_session: false,
                certificates: Some(Certificates {
                    ca: b"abc".to_vec(),
                    cert: Some(b"def".to_vec()),
                    cert_key: Some(b"ghi".to_vec())
                })
            }
        );

        assert_eq!(
            config.task,
            TaskConfig {
                batch_size: 100,
                batch_timeout: 200,
                unprocessed_messages_buffer_size: 300,
                maximum_processing_batch: 400
            }
        );

        assert_eq!(
            config.topics,
            HashMap::from_iter([("tp1".to_string(), 0), ("tp2".to_string(), 1)])
        );

        Ok(())
    }
}
