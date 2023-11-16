use taos::Dsn;

#[derive(Debug)]
pub struct ConnectConfig {
    // connection
    pub host: String,
    pub port: u16,
    // authentication
    pub username: String,
    pub password: String,
}

impl ConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(ConnectConfig {
            host: Self::parse_host(dsn)?,
            port: Self::parse_port(dsn),
            username: Self::parse_username(dsn)?,
            password: Self::parse_password(dsn)?,
        })
    }

    fn parse_host(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.addresses
            .first()
            .map(|addr| {
                anyhow::Ok(addr.host.clone().ok_or(anyhow::anyhow!("host is required"))?)
            })
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("host is required"))
    }

    fn parse_port(dsn: &Dsn) -> u16 {
        dsn.addresses
            .first()
            .map(|addr| {
                addr.port.unwrap_or(1433)
            })
            .unwrap_or(1433)
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
    use std::str::FromStr;

    use taos::Dsn;

    use super::*;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("historian://").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("host is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://localhost").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("username is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://aaAdmin@localhost").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("password is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@localhost").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("localhost", config.host);
        assert_eq!(1433, config.port);
        assert_eq!("aaAdmin", config.username);
        assert_eq!("aaAdmin", config.password);

        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@localhost:1234").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("localhost", config.host);
        assert_eq!(1234, config.port);
        assert_eq!("aaAdmin", config.username);
        assert_eq!("aaAdmin", config.password);
    }
}