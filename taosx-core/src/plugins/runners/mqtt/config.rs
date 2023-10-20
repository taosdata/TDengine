use std::collections::HashMap;

use itertools::Itertools;
use taos::Dsn;

use crate::runners::{get_string_from_param_or_file, get_string_vec_from_param_or_file};

#[derive(Debug, serde::Serialize)]
pub struct MqttConfig {
    log_level: String,
    pub(crate) remote: String,
    mqtt: MqttConnectConfig,
    topics: HashMap<String, u8>,
}

impl MqttConfig {
    pub fn from(dsn: &Dsn, ipc_port: Option<u16>) -> anyhow::Result<Self> {
        let connect_config = MqttConnectConfig::from_dsn(&dsn)?;
        let topics_vec = get_string_vec_from_param_or_file(&mut dsn.clone(), "topics")
            .map_err(|err| anyhow::anyhow!("invalid topics, cause: {}", err.to_string()))?;

        let mut topics = HashMap::new();
        for i in 0..topics_vec.len() {
            let pair = topics_vec[i].split("::").collect_vec();
            if pair.len() != 2 {
                return Err(anyhow::anyhow!("invalid topic: {}, cause: the format of topic is name::qos",topics_vec[i]));
            }
            let topic = String::from(pair[0]);
            let qos = pair[1]
                .parse::<u8>()
                .map_err(|err| anyhow::anyhow!("invalid qos: {} in topic, cause: {}", pair[1].to_string(), err.to_string()))?;
            topics.insert(topic, qos);
        }

        Ok(MqttConfig {
            log_level: dsn
                .params
                .get("log_level")
                .ok_or(anyhow::anyhow!("log_level is required"))?
                .to_string(),
            remote: format!("127.0.0.1:{}", ipc_port.unwrap_or(0)),
            mqtt: connect_config,
            topics,
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct MqttConnectConfig {
    address: String,
    version: String,
    client_id: String,
    username: String,
    password: String,
    keep_alive: usize,
    clean_session: bool,
    ca: String,
    cert: String,
    cert_key: String,
}

impl MqttConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let ca = get_string_from_param_or_file(&mut dsn.clone(), "ca", true, None)
            .map_err(|err| anyhow::anyhow!("failed to read ca config, cause: {}", err.to_string()))?;
        let cert = get_string_from_param_or_file(&mut dsn.clone(), "cert", true, None)
            .map_err(|err| anyhow::anyhow!("failed to read cert config, cause: {}", err.to_string()))?;
        let cert_key = get_string_from_param_or_file(&mut dsn.clone(), "cert_key", true, None)
            .map_err(|err| anyhow::anyhow!("failed to read cert_key config, cause: {}", err.to_string()))?;

        let host = dsn.addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or(anyhow::anyhow!("host is required"))?;
        let port = dsn.addresses
            .first()
            .and_then(|addr| addr.port.clone())
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
            client_id: dsn
                .params
                .get("client_id")
                .unwrap_or(&"".to_string())
                .to_string(),
            username: dsn.username.clone().unwrap_or("".to_string()),
            password: dsn.password.clone().unwrap_or("".to_string()),
            keep_alive: dsn
                .params
                .get("keep_alive")
                .map(|v| {
                    v.parse::<usize>()
                        .map_err(|err| anyhow::anyhow!("invalid keep_alive: {}, cause: {}", v.to_string(), err.to_string()))
                })
                .transpose()?
                .ok_or(anyhow::anyhow!("keep_alive is required"))?,
            clean_session: dsn
                .params
                .get("clean_session")
                .map(|v| {
                    v.parse::<bool>()
                        .map_err(|err| anyhow::anyhow!("invalid clean_session: {}, cause: {}", v.to_string(), err.to_string()))
                })
                .transpose()?
                .ok_or(anyhow::anyhow!("clean_session is required"))?,
            ca: ca.unwrap_or("".to_string()),
            cert: cert.unwrap_or("".to_string()),
            cert_key: cert_key.unwrap_or("".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
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
        let config = MqttConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("keep_alive is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("clean_session is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=abc").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("invalid keep_alive: abc, cause: invalid digit found in string", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true").unwrap();
        let config = MqttConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("tcp://127.0.0.1:1833", config.address);
        assert_eq!("3.0", config.version);
        assert_eq!("", config.client_id);
        assert_eq!("", config.username);
        assert_eq!("", config.password);
        assert_eq!(60, config.keep_alive);
        assert_eq!(true, config.clean_session);
        assert_eq!("", config.ca);
        assert_eq!("", config.cert);
        assert_eq!("", config.cert_key);
    }

    #[test]
    fn test_mqtt_config_from() {
        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true").unwrap();
        let config = MqttConfig::from(&dsn, Some(10086));
        assert!(config.is_err());
        assert_eq!("invalid topics, cause: Nodes not set", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=a,b,c").unwrap();
        let config = MqttConfig::from(&dsn, Some(10086));
        assert!(config.is_err());
        assert_eq!("invalid topic: a, cause: the format of topic is name::qos", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::abc").unwrap();
        let config = MqttConfig::from(&dsn, Some(10086));
        assert!(config.is_err());
        assert_eq!("invalid qos: abc in topic, cause: invalid digit found in string", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::0").unwrap();
        let config = MqttConfig::from(&dsn, Some(10086));
        assert!(config.is_err());
        assert_eq!("log_level is required", config.err().unwrap().to_string());

        let dsn = Dsn::from_str("mqtt://127.0.0.1:1833?version=3.0&keep_alive=60&clean_session=true&topics=tp1::0,tp2::1,tp3::2&log_level=debug&version=3.0&keep_alive=60&clean_session=true").unwrap();
        let config = MqttConfig::from(&dsn, Some(10086)).unwrap();
        assert_eq!("debug", config.log_level);
        assert_eq!("127.0.0.1:10086", config.remote);
        assert_eq!(3, config.topics.len());
        assert_eq!(0, *config.topics.get("tp1").unwrap());
        assert_eq!(1, *config.topics.get("tp2").unwrap());
        assert_eq!(2, *config.topics.get("tp3").unwrap());
    }
}