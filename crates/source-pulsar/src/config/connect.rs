use crate::{PULSAR_TUYA_ID, config::tuya::TuyaEnv};
use taos::Dsn;
use taosx_core::{runners::get_string_from_param_or_file, utils::dsn::parse_simple_params};

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum DataVendor {
    #[default]
    Standard,
    Tuya,
}

#[derive(Debug, Clone)]
pub struct PulsarConnectConfig {
    pub broker_url: String,
    pub use_ssl: bool,
    pub cert: Option<String>,     // the client certificate
    pub cert_key: Option<String>, // key for the client certificate
    pub jwt_token: Option<String>,
    pub ba_username: Option<String>,
    pub ba_password: Option<String>,
    pub custom_auth_name: Option<String>,
    pub custom_auth_data: Option<String>,
    pub custom_is_ssl: Option<bool>,
    pub data_vendor: DataVendor,
    pub tuya_access_id: Option<String>,
    pub tuya_access_key: Option<String>,
    pub tuya_env: Option<TuyaEnv>,
}

impl PulsarConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let data_vendor = Self::parse_data_vendor(dsn)?;
        Ok(Self {
            broker_url: Self::parse_broker_url(dsn)?,
            use_ssl: Self::parse_use_ssl(dsn)?,
            cert: Self::parse_ssl_param(dsn, "cert")?,
            cert_key: Self::parse_ssl_param(dsn, "cert_key")?,
            jwt_token: parse_simple_params(dsn, "jwt_token")?,
            ba_username: parse_simple_params(dsn, "ba_username")?,
            ba_password: parse_simple_params(dsn, "ba_password")?,
            custom_auth_name: parse_simple_params(dsn, "custom_auth_name")?,
            custom_auth_data: parse_simple_params(dsn, "custom_auth_data")?,
            custom_is_ssl: parse_simple_params(dsn, "custom_is_ssl")?,
            data_vendor,
            tuya_access_id: if data_vendor == DataVendor::Tuya {
                Some(
                    parse_simple_params(dsn, "tuya_access_id")?
                        .ok_or(anyhow::anyhow!("tuya access_id is required"))?,
                )
            } else {
                None
            },
            tuya_access_key: if data_vendor == DataVendor::Tuya {
                Some(
                    parse_simple_params(dsn, "tuya_access_key")?
                        .ok_or(anyhow::anyhow!("tuya access_key is required"))?,
                )
            } else {
                None
            },
            tuya_env: if data_vendor == DataVendor::Tuya {
                Some(TuyaEnv::try_from(
                    parse_simple_params::<String>(dsn, "tuya_env")?
                        .ok_or(anyhow::anyhow!("tuya_env is required"))?
                        .as_str(),
                )?)
            } else {
                None
            },
        })
    }

    fn parse_data_vendor(dsn: &Dsn) -> anyhow::Result<DataVendor> {
        match dsn.driver.as_str() {
            PULSAR_TUYA_ID => Ok(DataVendor::Tuya),
            _ => Ok(DataVendor::default()),
        }
    }

    /// use client `cert` to determine whether to use ssl
    fn parse_use_ssl(dsn: &Dsn) -> anyhow::Result<bool> {
        match dsn.get("cert") {
            Some(ca) => {
                if ca.is_empty() {
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }

    fn parse_broker_url(dsn: &Dsn) -> anyhow::Result<String> {
        let endpoints = dsn
            .addresses
            .iter()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>()
            .join(",");
        if endpoints.is_empty() {
            anyhow::bail!("dsn addr is empty");
        }
        let use_ssl = Self::parse_use_ssl(dsn)?
            || parse_simple_params(dsn, "custom_is_ssl")?.unwrap_or(false)
            || Self::parse_data_vendor(dsn)? == DataVendor::Tuya;

        let schema = if use_ssl { "pulsar+ssl" } else { "pulsar" };

        Ok(format!("{}://{}", schema, endpoints))
    }

    fn parse_ssl_param(dsn: &Dsn, key: &str) -> anyhow::Result<Option<String>> {
        dsn.get(key)
            .and_then(|cert| {
                if cert.is_empty() {
                    None
                } else if cert.starts_with('@') {
                    get_string_from_param_or_file(&mut dsn.clone(), key, true, None)
                        .map_err(|err| {
                            anyhow::anyhow!("failed to read {} config, cause: {}", key, err)
                        })
                        .transpose()
                } else {
                    Some(Ok(cert.to_string()))
                }
            })
            .transpose()
    }

    pub fn get_cert_chain(&self) -> Vec<u8> {
        let cert = self
            .cert
            .as_deref()
            .map_or(Vec::new(), |cert| cert.as_bytes().to_vec());
        let key = self
            .cert_key
            .as_deref()
            .map_or(Vec::new(), |key| key.as_bytes().to_vec());
        [cert.as_slice(), key.as_slice()].concat()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::IntoDsn;
    use taosx_core::utils::dsn::json_to_dsn;

    #[test]
    fn test_parse_broker_url() {
        let dsn = Dsn::from_str("pulsar://localhost:6650").unwrap();
        let broker_url = PulsarConnectConfig::from_dsn(&dsn).unwrap().broker_url;
        assert_eq!("pulsar://localhost:6650", broker_url);

        let dsn = Dsn::from_str("pulsar://localhost:6651?cert=pub_key").unwrap();
        let broker_url = PulsarConnectConfig::from_dsn(&dsn).unwrap().broker_url;
        assert_eq!("pulsar+ssl://localhost:6651", broker_url);

        let dsn = Dsn::from_str("pulsar+ssl://localhost:6651?cert=").unwrap();
        let broker_url = PulsarConnectConfig::from_dsn(&dsn).unwrap().broker_url;
        assert_eq!("pulsar://localhost:6651", broker_url);

        let dsn = json_to_dsn(&serde_json::json!({
            "type": "pulsar",
            "endpoint": "192.168.2.131:6651",
            "cert": "pub_key"
        }))
        .unwrap();
        let broker_url = PulsarConnectConfig::from_dsn(&dsn).unwrap().broker_url;
        assert_eq!("pulsar+ssl://192.168.2.131:6651", broker_url);

        let dsn = Dsn::from_str("pulsar://localhost:6651?custom_is_ssl=true").unwrap();
        let broker_url = PulsarConnectConfig::from_dsn(&dsn).unwrap().broker_url;
        assert_eq!("pulsar+ssl://localhost:6651", broker_url);
    }

    #[test]
    fn test_parse_use_ssl() {
        let dsn = Dsn::from_str("pulsar://?cert=file").unwrap();
        let use_ssl = PulsarConnectConfig::parse_use_ssl(&dsn).unwrap();
        assert!(use_ssl);

        let dsn = Dsn::from_str("pulsar://").unwrap();
        let use_ssl = PulsarConnectConfig::parse_use_ssl(&dsn).unwrap();
        assert!(!use_ssl);

        let dsn = Dsn::from_str("pulsar://?cert=").unwrap();
        let use_ssl = PulsarConnectConfig::parse_use_ssl(&dsn).unwrap();
        assert!(!use_ssl);
    }

    #[test]
    fn test_parse_auth_mtls() {
        let dsn = format!(
            "pulsar://{}?&cert={}&cert_key={}",
            "127.0.0.1:6650", "../tests/kafka/client.cert", "../tests/kafka/client.key",
        )
        .into_dsn()
        .unwrap();

        let config = PulsarConnectConfig::from_dsn(&dsn).unwrap();
        dbg!(&config);
        assert_eq!("../tests/kafka/client.key", config.cert_key.unwrap());
        assert_eq!("../tests/kafka/client.cert", config.cert.unwrap());
    }

    #[test]
    fn test_parse_auth_mechanism() {
        let dsn = format!(
            "pulsar://{}?&ba_username={}&ba_password={}&endpoint=192.168.2.131:6650",
            "192.168.2.131:6650", "root", "taosdata",
        )
        .into_dsn()
        .unwrap();

        let config = PulsarConnectConfig::from_dsn(&dsn).unwrap();
        dbg!(&config);
        assert_eq!("root", config.ba_username.unwrap());
        assert_eq!("taosdata", config.ba_password.unwrap());

        let dsn = format!(
            "pulsar://{}?&jwt_token={}",
            "127.0.0.1:6650", "this_is_token",
        )
        .into_dsn()
        .unwrap();

        let config = PulsarConnectConfig::from_dsn(&dsn).unwrap();
        dbg!(&config);
        assert_eq!("this_is_token", config.jwt_token.unwrap());
    }

    #[test]
    fn test_custom_auth() {
        let dsn = Dsn::from_str("pulsar://localhost:6651?custom_is_ssl=true&custom_auth_name=auth1&custom_auth_data=auth_data").unwrap();
        let config = PulsarConnectConfig::from_dsn(&dsn).unwrap();
        dbg!(&config);
        assert_eq!("auth1", config.custom_auth_name.unwrap());
        assert_eq!("auth_data", config.custom_auth_data.unwrap());
        assert_eq!("pulsar+ssl://localhost:6651", config.broker_url);
    }

    #[test]
    fn test_data_vendor() {
        let dsn = Dsn::from_str("pulsar://localhost:6650").unwrap();
        let data_vendor = PulsarConnectConfig::parse_data_vendor(&dsn).unwrap();
        assert_eq!(DataVendor::Standard, data_vendor);

        let dsn = Dsn::from_str("pulsarTuya://localhost:7285").unwrap();
        let data_vendor = PulsarConnectConfig::parse_data_vendor(&dsn).unwrap();
        assert_eq!(DataVendor::Tuya, data_vendor);
    }

    #[test]
    fn test_tuya_broker_url() {
        let dsn = Dsn::from_str(
            "pulsarTuya://mqe.tuyaus.com:7285?tuya_access_id=123&tuya_access_key=456&tuya_env=prod",
        )
        .unwrap();
        dbg!(&dsn);
        let broker_url = PulsarConnectConfig::from_dsn(&dsn).unwrap().broker_url;
        assert_eq!("pulsar+ssl://mqe.tuyaus.com:7285", broker_url);
    }
}
