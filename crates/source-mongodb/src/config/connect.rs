use std::time::Duration;
use taos::Dsn;
use taosx_core::{get_data_dir, utils};

#[derive(Debug, Clone)]
pub struct ConnectConfig {
    // connection
    pub host: String,
    pub port: u16,
    pub load_balanced: bool,
    pub direct_connection: bool,
    pub repl_set_name: Option<String>,
    pub local_threshold: Duration,
    // authentication
    pub username: Option<String>,
    pub password: Option<String>,
    pub mechanism: Option<String>,
    pub source: Option<String>,
    // other options
    pub app_name: Option<String>,
    pub compressors: Option<String>,
    pub tls: bool,
    pub ca_file_path: Option<String>,
    pub cert_key_file_path: Option<String>,
}

impl ConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(ConnectConfig {
            host: Self::parse_host(dsn)?,
            port: Self::parse_port(dsn)?,
            load_balanced: Self::parse_load_balanced(dsn),
            direct_connection: Self::parse_direct_connection(dsn),
            repl_set_name: Self::parse_repl_set_name(dsn),
            local_threshold: Self::parse_local_threshold(dsn)?,
            username: Self::parse_username(dsn),
            password: Self::parse_password(dsn),
            mechanism: Self::parse_mechanism(dsn),
            source: Self::parse_source(dsn),
            app_name: Self::parse_app_name(dsn),
            compressors: Self::parse_compressors(dsn),
            tls: Self::parse_tls(dsn),
            ca_file_path: Self::parse_ca_file_path(dsn),
            cert_key_file_path: Self::parse_cert_key_file_path(dsn),
        })
    }

    fn parse_host(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.addresses
            .first()
            .map(|addr| {
                anyhow::Ok(
                    addr.host
                        .clone()
                        .ok_or(anyhow::anyhow!("host is required"))?,
                )
            })
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("host is required"))
    }

    fn parse_port(dsn: &Dsn) -> anyhow::Result<u16> {
        dsn.addresses
            .first()
            .map(|addr| anyhow::Ok(addr.port.ok_or(anyhow::anyhow!("port is required"))?))
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("port is required"))
    }

    fn parse_load_balanced(dsn: &Dsn) -> bool {
        dsn.params
            .get("load_balanced")
            .map(|load_balanced| load_balanced.to_lowercase() == "true")
            .unwrap_or(false)
    }

    fn parse_direct_connection(dsn: &Dsn) -> bool {
        dsn.params
            .get("direct_connection")
            .map(|direct_connection| direct_connection.to_lowercase() == "true")
            .unwrap_or(false)
    }

    fn parse_repl_set_name(dsn: &Dsn) -> Option<String> {
        dsn.params.get("repl_set_name").cloned()
    }

    fn parse_local_threshold(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn
            .params
            .get("local_threshold")
            .map(|s| {
                let duration = utils::parse_duration(s).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse local_threshold: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;
                anyhow::Ok(duration)
            })
            .transpose()?
            .unwrap_or(Duration::from_millis(15)))
    }

    fn parse_username(dsn: &Dsn) -> Option<String> {
        dsn.username.clone()
    }

    fn parse_password(dsn: &Dsn) -> Option<String> {
        dsn.password.clone()
    }

    fn parse_mechanism(dsn: &Dsn) -> Option<String> {
        dsn.params.get("mechanism").cloned()
    }

    fn parse_source(dsn: &Dsn) -> Option<String> {
        dsn.params.get("source").cloned()
    }

    fn parse_app_name(dsn: &Dsn) -> Option<String> {
        dsn.params.get("app_name").cloned()
    }

    fn parse_compressors(dsn: &Dsn) -> Option<String> {
        dsn.params.get("compressors").cloned()
    }

    fn parse_tls(dsn: &Dsn) -> bool {
        dsn.params
            .get("tls")
            .map(|tls| tls.to_lowercase() == "true")
            .unwrap_or(false)
    }

    fn parse_ca_file_path(dsn: &Dsn) -> Option<String> {
        let ca_file_path = dsn.get("ca_file_path");
        if ca_file_path.is_none() || ca_file_path.unwrap().is_empty() {
            None
        } else {
            let ca_file_path = ca_file_path.unwrap();
            if ca_file_path.starts_with('@') {
                Some(
                    get_data_dir()
                        .join(ca_file_path.trim_start_matches("@"))
                        .display()
                        .to_string(),
                )
            } else {
                Some(ca_file_path.to_string())
            }
        }
    }

    fn parse_cert_key_file_path(dsn: &Dsn) -> Option<String> {
        let cert_key_file_path = dsn.get("cert_key_file_path");
        if cert_key_file_path.is_none() || cert_key_file_path.unwrap().is_empty() {
            None
        } else {
            let cert_key_file_path = cert_key_file_path.unwrap();
            if cert_key_file_path.starts_with('@') {
                Some(
                    get_data_dir()
                        .join(cert_key_file_path.trim_start_matches("@"))
                        .display()
                        .to_string(),
                )
            } else {
                Some(cert_key_file_path.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("mongodb://").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("host is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mongodb://localhost").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("port is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mongodb://admin:123456@localhost:27017?load_balanced=true&direct_connection=true&repl_set_name=repl&local_threshold=10ms&mechanism=MongoDbCr&source=admin&app_name=appname&compressors=zstd&tls=true&ca_file_path=@./file/ca.pem&cert_key_file_path=@./file/cert.pem").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("localhost", config.host);
        assert_eq!(27017, config.port);
        assert!(config.load_balanced);
        assert!(config.direct_connection);
        assert_eq!(Some("repl".to_string()), config.repl_set_name);
        assert_eq!(Duration::from_millis(10), config.local_threshold);
        assert_eq!(Some("admin".to_string()), config.username);
        assert_eq!(Some("123456".to_string()), config.password);
        assert_eq!(Some("MongoDbCr".to_string()), config.mechanism);
        assert_eq!(Some("admin".to_string()), config.source);
        assert_eq!(Some("appname".to_string()), config.app_name);
        assert_eq!(Some("zstd".to_string()), config.compressors);
        assert!(config.tls);
        assert_eq!(
            Some(get_data_dir().join("./file/ca.pem").display().to_string()),
            config.ca_file_path
        );
        assert_eq!(
            Some(get_data_dir().join("./file/cert.pem").display().to_string()),
            config.cert_key_file_path
        );
    }
}
