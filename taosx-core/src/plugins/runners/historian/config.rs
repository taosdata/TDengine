use chrono::{DateTime, Duration, Timelike, Utc};
use taos::Dsn;

#[derive(Debug, serde::Serialize)]
pub struct SourceConfig {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub table: String,
    pub tags: Vec<String>,
    pub begin_date_time: DateTime<Utc>,
    pub end_date_time: DateTime<Utc>,
    pub retrieve_mode: String,
}

impl SourceConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let begin_date_time = match dsn.params.get("beginDateTime") {
            None => DateTime::parse_from_rfc3339(&"1970-01-01T00:00:00.0Z")?.with_timezone(&Utc),
            Some(dt) => DateTime::parse_from_rfc3339(dt)?.with_timezone(&Utc)
        };

        let end_date_time = match dsn.params.get("endDateTime") {
            None => default_end_datetime(),
            Some(dt) => DateTime::parse_from_rfc3339(dt)?.with_timezone(&Utc)
        };

        let config = SourceConfig {
            username: dsn.username.clone().ok_or_else(|| anyhow::anyhow!("username is required, dsn: {:?}", &dsn))?,
            password: dsn.password.clone().ok_or_else(|| anyhow::anyhow!("password is required, dsn: {:?}", &dsn))?,
            host: dsn.addresses[0].host.clone().ok_or_else(|| anyhow::anyhow!("host is required, dsn: {:?}", &dsn))?,
            port: dsn.addresses[0].port.clone().unwrap_or(1433),
            table: dsn.params.get("table").unwrap_or(&"dbo.History".to_string()).to_string(),
            tags: dsn.params.get("tags").unwrap_or(&"*".to_string()).split(",").map(|s| s.to_string()).collect(),
            begin_date_time,
            end_date_time,
            retrieve_mode: dsn.params.get("retrieveMode").unwrap_or(&"full".to_string()).to_string(),
        };

        Ok(config)
    }
}

fn default_end_datetime() -> DateTime<Utc> {
    let tomorrow_midnight = (Utc::now() + Duration::days(1))
        .with_hour(0).unwrap()
        .with_minute(0).unwrap()
        .with_second(0).unwrap()
        .with_nanosecond(0).unwrap();
    tomorrow_midnight
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_source_config() {
        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@localhost").unwrap();
        let config = SourceConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.username, "aaAdmin");
        assert_eq!(config.password, "aaAdmin");
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1433);
        assert_eq!(config.table, "dbo.History");
        assert_eq!(config.tags, vec!["*"]);
        assert_eq!(config.begin_date_time, DateTime::parse_from_rfc3339("1970-01-01T00:00:00.0Z").unwrap());
        assert_eq!(config.end_date_time, default_end_datetime());
        assert_eq!(config.retrieve_mode, "full");

        let dsn = Dsn::from_str("historian://taosdata:taosdata@192.168.1.92:1234?\
        table=dbo.AnalogHistory\
        &tags=TAG1,TAG2,TAG3\
        &beginDateTime=2023-08-01T08:01:02.52+08:00\
        &endDateTime=2023-08-30T08:01:02.52+08:00\
        &retrieveMode=full").unwrap();
        let config = SourceConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.username, "taosdata");
        assert_eq!(config.password, "taosdata");
        assert_eq!(config.host, "192.168.1.92");
        assert_eq!(config.port, 1234);
        assert_eq!(config.table, "dbo.AnalogHistory");
        assert_eq!(config.tags, vec!["TAG1", "TAG2", "TAG3"]);
        assert_eq!(config.begin_date_time, DateTime::parse_from_rfc3339("2023-08-01T08:01:02.52+08:00").unwrap());
        assert_eq!(config.end_date_time, DateTime::parse_from_rfc3339("2023-08-30T08:01:02.52+08:00").unwrap());
        assert_eq!(config.retrieve_mode, "full");
    }
}