use taos::Dsn;

#[derive(Debug, serde::Serialize)]
pub struct ConnectionConfig {
    pub url: String,
}

impl ConnectionConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
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

        Ok(ConnectionConfig {
            url: format!("{}://{}:{}/", protocol, host, port),
        })
    }
}
