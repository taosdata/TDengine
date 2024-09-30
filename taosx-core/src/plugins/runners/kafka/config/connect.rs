use crate::{get_data_dir, runners::get_string_from_param_or_file};
use taos::Dsn;

#[derive(Debug, Clone)]
pub struct KafkaConnectConfig {
    pub bootstrap_servers: Vec<String>,
    pub use_ssl: bool,
    pub ca_cert: Option<String>,          // the trusted CA certificates
    pub ca_cert_password: Option<String>, // the password of the CA certificate
    pub client_cert: Option<String>,      // the client certificate
    pub client_key: Option<String>,       // key for the client certificate
    pub use_sasl: bool,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub sasl_kerberos_service_name: Option<String>,
    pub sasl_kerberos_principal: Option<String>,
    pub sasl_kerberos_kinit_cmd: Option<String>,
    pub sasl_kerberos_keytab: Option<String>,
}

impl KafkaConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(Self {
            bootstrap_servers: Self::parse_bootstrap_servers(dsn)?,
            use_ssl: Self::parse_use_ssl(dsn)?,
            ca_cert: Self::parse_ssl_ca(dsn)?,
            ca_cert_password: dsn.get("ca_password").map(|s| s.to_string()),
            client_cert: Self::parse_ssl_cert(dsn)?,
            client_key: Self::parse_ssl_cert_key(dsn)?,
            use_sasl: Self::parse_use_sasl(dsn)?,
            sasl_mechanism: dsn.get("sasl_mechanism").map(|s| s.to_string()),
            sasl_username: dsn.get("sasl_username").map(|s| s.to_string()),
            sasl_password: dsn.get("sasl_password").map(|s| s.to_string()),
            sasl_kerberos_service_name: dsn
                .get("sasl_kerberos_service_name")
                .map(|s| s.to_string()),
            sasl_kerberos_principal: dsn.get("sasl_kerberos_principal").map(|s| s.to_string()),
            sasl_kerberos_kinit_cmd: dsn.get("sasl_kerberos_kinit_cmd").map(|s| s.to_string()),
            sasl_kerberos_keytab: Self::parse_sasl_kerberos_keytab(dsn)?,
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
                address.port.unwrap()
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

    fn parse_ssl_ca(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        let ca = dsn.get("ca");
        if ca.is_none() || ca.unwrap().is_empty() {
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
        }
    }

    fn parse_ssl_cert(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        let cert = dsn.get("cert");
        if cert.is_none() || cert.unwrap().is_empty() {
            Ok(None)
        } else {
            let cert = cert.unwrap();
            if cert.starts_with('@') {
                get_string_from_param_or_file(&mut dsn.clone(), "cert", true, None).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to read client certificate config, cause: {}",
                        err.to_string()
                    )
                })
            } else {
                Ok(Some(cert.to_string()))
            }
        }
    }

    fn parse_ssl_cert_key(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        let cert_key = dsn.get("cert_key");
        if cert_key.is_none() || cert_key.unwrap().is_empty() {
            Ok(None)
        } else {
            let cert_key = cert_key.unwrap();
            if cert_key.starts_with('@') {
                get_string_from_param_or_file(&mut dsn.clone(), "cert_key", true, None).map_err(
                    |err| {
                        anyhow::anyhow!(
                            "failed to read client key config, cause: {}",
                            err.to_string()
                        )
                    },
                )
            } else {
                Ok(Some(cert_key.to_string()))
            }
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

    fn parse_sasl_kerberos_keytab(dsn: &Dsn) -> anyhow::Result<Option<String>> {
        let sasl_kerberos_keytab = dsn.get("sasl_kerberos_keytab");
        if sasl_kerberos_keytab.is_none() || sasl_kerberos_keytab.unwrap().is_empty() {
            Ok(None)
        } else {
            let sasl_kerberos_keytab = sasl_kerberos_keytab.unwrap();
            if sasl_kerberos_keytab.starts_with('@') {
                Ok(Some(
                    get_data_dir()
                        .join(sasl_kerberos_keytab.trim_start_matches("@"))
                        .display()
                        .to_string(),
                ))
            } else {
                Ok(Some(sasl_kerberos_keytab.to_string()))
            }
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
        assert!(use_ssl);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let use_ssl = KafkaConnectConfig::parse_use_ssl(&dsn).unwrap();
        assert!(!use_ssl);

        let dsn = Dsn::from_str("kafka://?ca=").unwrap();
        let use_ssl = KafkaConnectConfig::parse_use_ssl(&dsn).unwrap();
        assert!(!use_ssl);
    }

    #[test]
    fn test_parse_certification() {
        let dsn = format!(
            "kafka://{}?use_ssl=true&ca={}&ca_password={}&cert={}&cert_key={}",
            "127.0.0.1:9092",
            "../tests/kafka/ca.cert",
            "abcdefgh",
            "../tests/kafka/client.cert",
            "../tests/kafka/client.key",
        )
        .into_dsn()
        .unwrap();

        let config = KafkaConnectConfig::from_dsn(&dsn).unwrap();
        dbg!(&config);
        assert_eq!("../tests/kafka/ca.cert", config.ca_cert.unwrap());
        assert_eq!("abcdefgh", config.ca_cert_password.unwrap());
        assert_eq!("../tests/kafka/client.key", config.client_key.unwrap());
        assert_eq!("../tests/kafka/client.cert", config.client_cert.unwrap());
    }

    #[test]
    fn test_parse_use_sasl() {
        let dsn = Dsn::from_str("kafka://?sasl_mechanism=PLAIN").unwrap();
        let use_sasl = KafkaConnectConfig::parse_use_sasl(&dsn).unwrap();
        assert!(use_sasl);

        let dsn = Dsn::from_str("kafka://").unwrap();
        let use_sasl = KafkaConnectConfig::parse_use_sasl(&dsn).unwrap();
        assert!(!use_sasl);

        let dsn = Dsn::from_str("kafka://?sasl_mechanism=").unwrap();
        let use_sasl = KafkaConnectConfig::parse_use_sasl(&dsn).unwrap();
        assert!(!use_sasl);
    }
}
