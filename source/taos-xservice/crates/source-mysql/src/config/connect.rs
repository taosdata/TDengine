use taos::Dsn;

#[derive(Debug, Clone)]
pub struct ConnectConfig {
    // connection
    pub host: String,
    pub port: u16,
    pub subject: String,
    // authentication
    pub username: String,
    pub password: String,
    // other options
    pub charset: String,
    pub ssl_mode: String,
    pub ssl_ca: Option<String>,
    pub ssl_client_cert: Option<String>,
    pub ssl_client_key: Option<String>,
}

impl ConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(ConnectConfig {
            host: Self::parse_host(dsn)?,
            port: Self::parse_port(dsn)?,
            subject: Self::parse_subject(dsn)?,
            username: Self::parse_username(dsn)?,
            password: Self::parse_password(dsn)?,
            charset: Self::parse_charset(dsn)?,
            ssl_mode: Self::parse_ssl_mode(dsn)?,
            ssl_ca: Self::parse_ssl_ca(dsn),
            ssl_client_cert: Self::parse_ssl_client_cert(dsn),
            ssl_client_key: Self::parse_ssl_client_key(dsn),
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

    fn parse_subject(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.subject
            .clone()
            .ok_or_else(|| anyhow::anyhow!("subject is required"))
    }

    fn parse_username(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.username
            .clone()
            .ok_or_else(|| anyhow::anyhow!("username is required"))
    }

    fn parse_password(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.password
            .clone()
            .ok_or_else(|| anyhow::anyhow!("password is required"))
    }

    fn parse_charset(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.params
            .get("charset")
            .map(|charset| charset.to_lowercase().clone())
            .unwrap_or_else(|| "utf8mb4".to_string())
            .parse()
            .map_err(|_| anyhow::anyhow!("charset is invalid"))
    }

    fn parse_ssl_mode(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.params
            .get("ssl_mode")
            .map(|ssl_mode| ssl_mode.to_uppercase().clone())
            .unwrap_or_else(|| "PREFERRED".to_string())
            .parse()
            .map_err(|_| anyhow::anyhow!("ssl_mode is invalid"))
    }

    fn parse_ssl_ca(dsn: &Dsn) -> Option<String> {
        dsn.params.get("ssl_ca").cloned()
    }

    fn parse_ssl_client_cert(dsn: &Dsn) -> Option<String> {
        dsn.params.get("ssl_client_cert").cloned()
    }

    fn parse_ssl_client_key(dsn: &Dsn) -> Option<String> {
        dsn.params.get("ssl_client_key").cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("mysql://").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("host is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mysql://localhost").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("port is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mysql://localhost:3306").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("subject is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mysql://localhost:3306/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("username is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mysql://aaAdmin@localhost:3306/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("password is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mysql://aaAdmin:aaAdmin@localhost:3306/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("localhost", config.host);
        assert_eq!(3306, config.port);
        assert_eq!("db1", config.subject);
        assert_eq!("aaAdmin", config.username);
        assert_eq!("aaAdmin", config.password);
    }
}
