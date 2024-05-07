use taos::Dsn;

#[derive(Debug, Clone)]
pub struct KafkaConnectConfig {
    pub bootstrap_servers: Vec<String>,
    pub use_ssl: bool,
    pub ca_cert: Option<String>,     // the trusted CA certificates
    pub client_cert: Option<String>, // the client certificate
    pub client_key: Option<String>,  // key for the client certificate
    pub use_sasl: bool,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

impl KafkaConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self {
            bootstrap_servers: Self::parse_bootstrap_servers(dsn)?,
            use_ssl: Self::parse_use_ssl(dsn)?,
            ca_cert: dsn.get("ca").map(|s| s.to_string()),
            client_cert: dsn.get("cert").map(|s| s.to_string()),
            client_key: dsn.get("cert_key").map(|s| s.to_string()),
            use_sasl: Self::parse_use_sasl(dsn)?,
            sasl_mechanism: dsn.get("sasl_mechanism").map(|s| s.to_string()),
            sasl_username: dsn.get("sasl_username").map(|s| s.to_string()),
            sasl_password: dsn.get("sasl_password").map(|s| s.to_string()),
        })
    }

    fn parse_bootstrap_servers(dsn: &Dsn) -> anyhow::Result<Vec<String>> {
        let mut bootstrap_servers = Vec::new();
        for address in dsn.addresses.iter() {
            if address.host.is_none() || address.port.is_none() {
                return Err(anyhow::anyhow!(
                    "invalid bootstrap_servers, cause: host or port is none"
                ));
            }
            bootstrap_servers.push(format!(
                "{}:{}",
                address.host.clone().unwrap(),
                address.port.clone().unwrap()
            ));
        }
        Ok(bootstrap_servers)
    }

    /// use `ca` to determine whether to use ssl
    fn parse_use_ssl(dsn: &Dsn) -> anyhow::Result<bool> {
        match dsn.get("ca") {
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

    /// use `sasl_mechanism` to determine whether to use sasl
    fn parse_use_sasl(dsn: &Dsn) -> anyhow::Result<bool> {
        match dsn.get("sasl_mechanism") {
            Some(sasl_mechanism) => {
                if sasl_mechanism.is_empty() {
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use taos::IntoDsn;

    use super::*;

    #[test]
    fn test_parse_bootstrap_servers() {
        let dsn = Dsn::from_str("kafka://localhost:9092,192.168.1.92:9092").unwrap();
        let bootstrap_servers = KafkaConnectConfig::parse_bootstrap_servers(&dsn).unwrap();
        assert_eq!("localhost:9092", bootstrap_servers[0]);
        assert_eq!("192.168.1.92:9092", bootstrap_servers[1]);

        let dsn = Dsn::from_str("kafka://localhost").unwrap();
        let bootstrap_servers = KafkaConnectConfig::parse_bootstrap_servers(&dsn);
        assert!(bootstrap_servers.is_err());
        assert_eq!(
            "invalid bootstrap_servers, cause: host or port is none",
            bootstrap_servers.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("kafka://:9092").unwrap();
        let bootstrap_servers = KafkaConnectConfig::parse_bootstrap_servers(&dsn);
        assert!(bootstrap_servers.is_err());
        assert_eq!(
            "invalid bootstrap_servers, cause: host or port is none",
            bootstrap_servers.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_use_ssl() {
        let dsn = Dsn::from_str("kafka://?ca=file").unwrap();
        let use_ssl = KafkaConnectConfig::parse_use_ssl(&dsn).unwrap();
        assert_eq!(true, use_ssl);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let use_ssl = KafkaConnectConfig::parse_use_ssl(&dsn).unwrap();
        assert_eq!(false, use_ssl);

        let dsn = Dsn::from_str("kafka://?ca=").unwrap();
        let use_ssl = KafkaConnectConfig::parse_use_ssl(&dsn).unwrap();
        assert_eq!(false, use_ssl);
    }

    #[test]
    fn test_parse_certification() {
        let dsn = format!(
            "kafka://{}?use_ssl=true&ca={}&cert={}&cert_key={}",
            "127.0.0.1:9092",
            "../tests/kafka/ca.cert",
            "../tests/kafka/client.cert",
            "../tests/kafka/client.key",
        )
        .into_dsn()
        .unwrap();

        let config = KafkaConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("../tests/kafka/ca.cert", config.ca_cert.unwrap());
        assert_eq!("../tests/kafka/client.key", config.client_key.unwrap());
        assert_eq!("../tests/kafka/client.cert", config.client_cert.unwrap());
    }

    #[test]
    fn test_parse_use_sasl() {
        let dsn = Dsn::from_str("kafka://?sasl_mechanism=PLAIN").unwrap();
        let use_sasl = KafkaConnectConfig::parse_use_sasl(&dsn).unwrap();
        assert_eq!(true, use_sasl);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let use_sasl = KafkaConnectConfig::parse_use_sasl(&dsn).unwrap();
        assert_eq!(false, use_sasl);

        let dsn = Dsn::from_str("kafka://?sasl_mechanism=").unwrap();
        let use_sasl = KafkaConnectConfig::parse_use_sasl(&dsn).unwrap();
        assert_eq!(false, use_sasl);
    }
}
