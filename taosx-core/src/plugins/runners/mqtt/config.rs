use std::collections::HashMap;

use itertools::Itertools;
use taos::Dsn;
use uuid::Uuid;

use crate::{
    get_data_dir,
    runners::{get_string_from_param_or_file, get_string_vec_from_param_or_file},
};

#[derive(Debug, serde::Serialize)]
pub struct MqttConfig {
    pub log_level: String,
    pub remote: String,
    pub mqtt: MqttConnectConfig,
    pub topics: HashMap<String, u8>,
    pub dump: Option<Dump>,
}

#[derive(Debug, serde::Serialize)]
pub struct Dump {
    pub enable: bool,
    pub path: String,
    pub keep: usize,
}

impl Dump {
    pub fn from_dsn(dsn: &Dsn, id: Option<i64>) -> anyhow::Result<Option<Self>> {
        let enable = dsn
            .params
            .get("keep_raw_data")
            .map(|v| {
                let v = v.trim();
                if v.is_empty() {
                    return Ok(true);
                }
                v.trim().parse::<bool>().map_err(|err| {
                    tracing::error!(
                        "invalid keep_raw_data: `{}`, require boolean value, cause: {}",
                        v,
                        err
                    );
                    err
                })
            })
            .transpose()?
            .unwrap_or(false);
        if !enable {
            return Ok(None);
        }
        let path = dsn
            .params
            .get("keep_raw_data_dir")
            .map(|v| v.to_string())
            .or_else(|| {
                id.map(|id| {
                    let path = get_data_dir()
                        .join("tasks")
                        .join(format!("{id}"))
                        .join("rawdata");
                    path.display().to_string()
                })
            })
            .ok_or_else(|| anyhow::anyhow!("path is required if keep_raw_data is enabled"))?;
        let keep = dsn
            .params
            .get("keep_raw_data_days")
            .map(|v| {
                v.parse::<usize>().map_err(|err| {
                    anyhow::anyhow!(
                        "parse keep_raw_data_days failed, which requires integer value: {}",
                        err.to_string()
                    )
                })
            })
            .transpose()?
            .unwrap_or(1); // Default keep 1 day.

        Ok(Some(Dump { enable, path, keep }))
    }
}

impl MqttConfig {
    pub fn from(dsn: &Dsn, ipc_port: Option<u16>, task_id: Option<i64>) -> anyhow::Result<Self> {
        let connect_config = MqttConnectConfig::from_dsn(dsn)?;
        let dump = Dump::from_dsn(dsn, task_id)?;
        let topics_vec = get_string_vec_from_param_or_file(&mut dsn.clone(), "topics")
            .map_err(|err| anyhow::anyhow!("invalid topics, cause: {}", err.to_string()))?;

        let mut topics = HashMap::new();
        for i in 0..topics_vec.len() {
            let pair = topics_vec[i].split("::").collect_vec();
            if pair.len() != 2 {
                return Err(anyhow::anyhow!(
                    "invalid topic: {}, cause: the format of topic is name::qos",
                    topics_vec[i]
                ));
            }
            let topic = String::from(pair[0]);
            let qos = pair[1].parse::<u8>().map_err(|err| {
                anyhow::anyhow!(
                    "invalid qos: {} in topic, cause: {}",
                    pair[1].to_string(),
                    err.to_string()
                )
            })?;
            topics.insert(topic, qos);
        }

        Ok(MqttConfig {
            log_level: dsn
                .params
                .get("log_level")
                .cloned()
                .unwrap_or("info".to_string())
                .to_string(),
            remote: format!("127.0.0.1:{}", ipc_port.unwrap_or(0)),
            mqtt: connect_config,
            topics,
            dump,
        })
    }
}

#[derive(Debug, serde::Serialize, Default)]
pub struct MqttConnectConfig {
    address: String,
    version: String,
    client_id: Option<String>,
    username: Option<String>,
    password: Option<String>,
    keep_alive: Option<u64>,
    clean_session: Option<bool>,
    ca: Option<String>,
    cert: Option<String>,
    cert_key: Option<String>,
}

impl MqttConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
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
            version: dsn
                .params
                .get("version")
                .ok_or(anyhow::anyhow!("version is required"))?
                .to_string(),
            client_id: dsn.params.get("client_id").map(|v| v.to_string()),
            username: dsn.username.clone(),
            password: dsn.password.clone(),
            keep_alive: Self::parse_keep_alive(dsn)?,
            clean_session: dsn
                .params
                .get("clean_session")
                .map(|v| {
                    v.parse::<bool>().map_err(|err| {
                        anyhow::anyhow!(
                            "invalid clean_session: {}, cause: {}",
                            v.to_string(),
                            err.to_string()
                        )
                    })
                })
                .transpose()?,
            ca,
            cert,
            cert_key,
        })
    }

    pub fn parse_version(dsn: &Dsn) -> anyhow::Result<String> {
        let version = dsn
            .params
            .get("version")
            .map(|v| {
                if v.is_empty() {
                    return Err(anyhow::anyhow!("version is required"));
                }
                let version = match v.as_str() {
                    "3.1" | "3.1.1" | "5.0" | "5" => Ok(v.to_string()),
                    _ => Err(anyhow::anyhow!("invalid version: {}", v.to_string())),
                };
                version
            })
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("version is required"))?;
        Ok(version)
    }

    pub fn host_port(&self) -> (String, u16) {
        let parts = self.address.split(':').collect::<Vec<&str>>();
        let host = parts[1].trim_start_matches("//").to_string();
        let port = parts[2].parse::<u16>().unwrap();
        (host, port)
    }

    /// use UUID as client_id if not set
    pub fn client_id(&self) -> impl Into<String> {
        self.client_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string().replace("-", ""))
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// Default keep alive is 5 seconds
    pub fn keep_alive(&self) -> core::time::Duration {
        core::time::Duration::from_secs(self.keep_alive.unwrap_or(5))
    }

    fn parse_keep_alive(dsn: &Dsn) -> anyhow::Result<Option<u64>> {
        let keep_alive = dsn
            .params
            .get("keep_alive")
            .map(|v| {
                v.parse::<u64>().map_err(|err| {
                    anyhow::anyhow!(
                        "invalid keep_alive: {}, cause: {}",
                        v.to_string(),
                        err.to_string()
                    )
                })
            })
            .transpose()?;
        Ok(keep_alive)
    }

    /// Default clean session is true
    pub fn clean_session(&self) -> bool {
        self.clean_session.unwrap_or(true)
    }

    pub fn ssl_enabled(dsn: &Dsn) -> bool {
        dsn.params.get("ca").is_some()
    }

    pub fn ssl(&self) -> anyhow::Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let ca = self
            .ca
            .as_ref()
            .map(|v| v.as_bytes().to_vec())
            .ok_or(anyhow::anyhow!(
                "ca is required if ssl is enabled, please set ca or ca file path"
            ))?;
        let cert = self
            .cert
            .as_ref()
            .map(|v| v.as_bytes().to_vec())
            .ok_or(anyhow::anyhow!(
                "cert is required if ssl is enabled, please set cert or cert file path"
            ))?;
        let cert_key = self
            .cert_key
            .as_ref()
            .map(|v| v.as_bytes().to_vec())
            .ok_or(anyhow::anyhow!(
                "cert_key is required if ssl is enabled, please set cert_key or cert_key file path"
            ))?;
        Ok((ca, cert, cert_key))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_version() {
        let dsn = Dsn::from_str("mqtt://").unwrap();
        let version = MqttConnectConfig::parse_version(&dsn);
        assert!(version.is_err());
        assert_eq!("version is required", version.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://?version=3.0").unwrap();
        let version = MqttConnectConfig::parse_version(&dsn);
        assert!(version.is_err());
        assert_eq!("invalid version: 3.0", version.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://?version=3.1").unwrap();
        let version = MqttConnectConfig::parse_version(&dsn).unwrap();
        assert_eq!("3.1", version);

        let dsn = Dsn::from_str("mqtt://?version=3.1.1").unwrap();
        let version = MqttConnectConfig::parse_version(&dsn).unwrap();
        assert_eq!("3.1.1", version);

        let dsn = Dsn::from_str("mqtt://?version=5.0").unwrap();
        let version = MqttConnectConfig::parse_version(&dsn).unwrap();
        assert_eq!("5.0", version);

        let dsn = Dsn::from_str("mqtt://?version=5").unwrap();
        let version = MqttConnectConfig::parse_version(&dsn).unwrap();
        assert_eq!("5", version);
    }

    #[test]
    fn test_host_port() {
        let dsn = Dsn::from_str("mqtt://127.0.0.1:1884?version=3.1").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn).unwrap();
        let (host, port) = config.host_port();
        assert_eq!("127.0.0.1", host);
        assert_eq!(1884, port);
    }

    #[test]
    fn test_client_id() {
        let config = MqttConnectConfig::default();
        assert_eq!(32, config.client_id().into().len());
    }

    #[test]
    fn test_parse_keep_alive() {
        let dsn = Dsn::from_str("mqtt://?keep_alive=30").unwrap();
        let keep_alive = MqttConnectConfig::parse_keep_alive(&dsn).unwrap();
        assert_eq!(Some(30), keep_alive);

        let dsn = Dsn::from_str("mqtt://?keep_alive=abc").unwrap();
        let keep_alive = MqttConnectConfig::parse_keep_alive(&dsn);
        assert!(keep_alive.is_err());
        assert_eq!(
            "invalid keep_alive: abc, cause: invalid digit found in string",
            keep_alive.err().unwrap().to_string()
        );

        let dsn = Dsn::from_str("mqtt://").unwrap();
        let keep_alive = MqttConnectConfig::parse_keep_alive(&dsn);
        assert_eq!(None, keep_alive.unwrap());

        let dsn = Dsn::from_str("mqtt://?keep_alive=").unwrap();
        let keep_alive = MqttConnectConfig::parse_keep_alive(&dsn);
        assert_eq!(None, keep_alive.unwrap());
    }

    #[test]
    #[ignore]
    fn test_mqtt_connect_config_from_dsn() {
        let dsn = Dsn::from_str("mqtt://").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("host is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("port is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("version is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.version, "3.0");

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "clean_session is required",
            config.err().unwrap().to_string()
        );

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=abc").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "invalid keep_alive: abc, cause: invalid digit found in string",
            config.err().unwrap().to_string()
        );

        let dsn =
            Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true")
                .unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("tcp://127.0.0.1:1833", config.address);
        assert_eq!("3.0", config.version);
        assert_eq!(None, config.client_id);
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
        let config = MqttConfig::from(&dsn, Some(10086), None);
        assert!(config.is_err());
        assert_eq!(
            "invalid topics, cause: Nodes not set",
            config.err().unwrap().to_string()
        );

        let dsn = Dsn::from_str(
            "mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=a,b,c",
        )
        .unwrap();
        let config = MqttConfig::from(&dsn, Some(10086), None);
        assert!(config.is_err());
        assert_eq!(
            "invalid topic: a, cause: the format of topic is name::qos",
            config.err().unwrap().to_string()
        );

        let dsn = Dsn::from_str(
            "mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::abc",
        )
        .unwrap();
        let config = MqttConfig::from(&dsn, Some(10086), None);
        assert!(config.is_err());
        assert_eq!(
            "invalid qos: abc in topic, cause: invalid digit found in string",
            config.err().unwrap().to_string()
        );

        let dsn = Dsn::from_str(
            "mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::0",
        )
        .unwrap();
        let config = MqttConfig::from(&dsn, Some(10086), None);
        assert!(config.is_ok());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::0,tp2::1,tp3::2&log_level=debug&version=3.0&keep_alive=60&clean_session=true").unwrap();
        let config = MqttConfig::from(&dsn, Some(10086), None).unwrap();
        assert_eq!("debug", config.log_level);
        assert_eq!("127.0.0.1:10086", config.remote);
        assert_eq!(3, config.topics.len());
        assert_eq!(0, *config.topics.get("tp1").unwrap());
        assert_eq!(1, *config.topics.get("tp2").unwrap());
        assert_eq!(2, *config.topics.get("tp3").unwrap());
    }

    #[test]
    #[ignore]
    fn test_mqtt_config_from_with_file() {
        let dsn = Dsn::from_str("mqtt://192.168.1.42:1833?version=3.0").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn).unwrap();
        let toml = toml::to_string(&config);
        assert_eq!(
            "address = \"tcp://192.168.1.42:1833\"\\nversion = \"3.0\"",
            toml.unwrap()
        );
    }

    #[test]
    fn test_mqtt_dump() {
        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::0,tp2::1,tp3::2&log_level=debug&version=3.0&keep_alive=60&clean_session=true").unwrap();
        let config = MqttConfig::from(&dsn, Some(10086), None).unwrap();
        assert_eq!("debug", config.log_level);
        assert_eq!("127.0.0.1:10086", config.remote);
        assert_eq!(3, config.topics.len());
        assert_eq!(0, *config.topics.get("tp1").unwrap());
        assert_eq!(1, *config.topics.get("tp2").unwrap());
        assert_eq!(2, *config.topics.get("tp3").unwrap());
        assert!(config.dump.is_none());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::0,tp2::1,tp3::2&log_level=debug&version=3.0&keep_alive=60&clean_session=true&keep_raw_data&keep_raw_data_dir=./abc").unwrap();
        let config = MqttConfig::from(&dsn, Some(10086), None).unwrap();
        let dump = config.dump.unwrap();
        assert!(dump.enable);
        assert_eq!(dump.path, "./abc");
        assert_eq!(dump.keep, 1);
    }
}
