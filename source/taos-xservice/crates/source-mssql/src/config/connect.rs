use taos::Dsn;

#[derive(Debug, Clone)]
pub struct ConnectConfig {
    // connection
    pub host: String,
    pub port: u16,
    pub database: String,
    // authentication
    pub username: String,
    pub password: String,
    // other options
    pub instance_name: String,
    pub application_name: String,
    pub encryption: String,
    pub trust_cert: bool,
    pub trust_cert_ca: Option<String>,
}

impl ConnectConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(ConnectConfig {
            host: Self::parse_host(dsn)?,
            port: Self::parse_port(dsn)?,
            database: Self::parse_database(dsn)?,
            username: Self::parse_username(dsn)?,
            password: Self::parse_password(dsn)?,
            instance_name: Self::parse_instance_name(dsn),
            application_name: Self::parse_application_name(dsn),
            encryption: Self::parse_encryption(dsn),
            trust_cert: Self::parse_trust_cert(dsn),
            trust_cert_ca: Self::parse_trust_cert_ca(dsn),
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

    fn parse_database(dsn: &Dsn) -> anyhow::Result<String> {
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

    fn parse_instance_name(dsn: &Dsn) -> String {
        dsn.params.get("instance_name").cloned().unwrap_or_default()
    }

    fn parse_application_name(dsn: &Dsn) -> String {
        dsn.params
            .get("application_name")
            .cloned()
            .unwrap_or_default()
    }

    fn parse_encryption(dsn: &Dsn) -> String {
        dsn.params
            .get("encryption")
            .cloned()
            .unwrap_or_else(|| "NotSupported".to_string())
    }

    fn parse_trust_cert(dsn: &Dsn) -> bool {
        dsn.params
            .get("trust_cert")
            .map(|trust_cert| trust_cert.to_lowercase() == "true")
            .unwrap_or(false)
    }

    fn parse_trust_cert_ca(dsn: &Dsn) -> Option<String> {
        dsn.params.get("trust_cert_ca").cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_dsn() {
        let dsn = Dsn::from_str("mssql://").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("host is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mssql://localhost").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("port is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mssql://localhost:1433").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("subject is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mssql://localhost:1433/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("username is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mssql://aaAdmin@localhost:1433/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("password is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("mssql://aaAdmin:aaAdmin@localhost:1433/db1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        assert_eq!("localhost", config.host);
        assert_eq!(1433, config.port);
        assert_eq!("db1", config.database);
        assert_eq!("aaAdmin", config.username);
        assert_eq!("aaAdmin", config.password);
    }
}
