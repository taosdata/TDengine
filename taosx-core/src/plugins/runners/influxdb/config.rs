use taos::Dsn;

pub const INFLUXDB_V1: [&str; 2] = ["1.7", "1.8"];
pub const INFLUXDB_V2: [&str; 8] = ["2.0", "2.1", "2.2", "2.3", "2.4", "2.5", "2.6", "2.7"];

#[derive(Debug, serde::Serialize)]
pub struct ConnectionConfig {
    pub url: String,
    pub version: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
    #[serde(rename = "orgId")]
    pub org_id: Option<String>,
    #[serde(rename = "addDbrp")]
    pub add_dbrp: bool,
}

impl ConnectionConfig {
    /// On version 1.x, only username/password mode can be used
    /// On version 2.x, only access token mode can be used.
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        if dsn.driver != "influxdb" {
            return Err(anyhow::anyhow!("invalid driver: {}", dsn.driver));
        }

        let version = dsn
            .params
            .get("version")
            .ok_or(anyhow::anyhow!("version is required"))?;

        let mut username = None;
        let mut password = None;
        let mut org_id = None;
        let mut token = None;
        if INFLUXDB_V1.contains(&version.as_str()) {
            username = Option::from(
                dsn.params
                    .get("username")
                    .or(dsn.username.as_ref())
                    .ok_or(anyhow::anyhow!("username is required"))?
                    .to_string(),
            );
            password = Option::from(
                dsn.params
                    .get("password")
                    .or(dsn.password.as_ref())
                    .ok_or(anyhow::anyhow!("password is required"))?
                    .to_string(),
            );
        } else if INFLUXDB_V2.contains(&version.as_str()) {
            org_id = Option::from(
                dsn.params
                    .get("orgId")
                    .ok_or(anyhow::anyhow!("orgId is required"))?
                    .to_string(),
            );
            token = Option::from(
                dsn.params
                    .get("token")
                    .ok_or(anyhow::anyhow!("token is required"))?
                    .to_string(),
            );
        } else {
            return Err(anyhow::anyhow!("invalid version: {}", version));
        }
        let add_dbrp = dsn
            .params
            .get("addDbrp")
            .map(|s| s.as_str() == "true")
            .unwrap_or(false);

        let influx = ConnectionConfig {
            url: Self::parse_url(dsn)?,
            version: version.to_string(),
            username,
            password,
            token,
            org_id,
            add_dbrp,
        };

        Ok(influx)
    }
    fn parse_url(dsn: &Dsn) -> anyhow::Result<String> {
        let host = dsn
            .addresses
            .first()
            .and_then(|addr| addr.host.clone())
            .ok_or_else(|| anyhow::anyhow!("host is required"))?;

        let port = dsn
            .addresses
            .first()
            .and_then(|addr| addr.port)
            .ok_or_else(|| anyhow::anyhow!("port is required"))?;

        let protocol = dsn.protocol.as_deref().unwrap_or("http");
        if protocol != "http" && protocol != "https" {
            return Err(anyhow::anyhow!("invalid protocol: {}", protocol));
        }

        Ok(format!("{}://{}:{}/", protocol, host, port))
    }
}
