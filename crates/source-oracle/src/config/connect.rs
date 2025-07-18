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
}

impl ConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(ConnectConfig {
            host: Self::parse_host(dsn)?,
            port: Self::parse_port(dsn)?,
            subject: Self::parse_subject(dsn)?,
            username: Self::parse_username(dsn)?,
            password: Self::parse_password(dsn)?,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("oracle://").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("host is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("oracle://localhost").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("port is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("oracle://localhost:1521").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("subject is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("oracle://localhost:1521/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("username is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("oracle://aaAdmin@localhost:1521/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("password is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("oracle://aaAdmin:aaAdmin@localhost:1521/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("localhost", config.host);
        assert_eq!(1521, config.port);
        assert_eq!("db1", config.subject);
        assert_eq!("aaAdmin", config.username);
        assert_eq!("aaAdmin", config.password);
    }
}
