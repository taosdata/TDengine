use chrono::{DateTime, Duration, Utc};
use taos::Dsn;

use crate::runners::historian::config::connect::ConnectConfig;

pub mod connect;

#[derive(Debug)]
pub struct TaskConfig {
    pub connect: ConnectConfig,

    pub mode: String,
    pub table: String,
    pub tags: Vec<String>,
    pub begin_datetime: DateTime<Utc>,
    pub end_datetime: Option<DateTime<Utc>>,
    pub time_window: Duration,
    pub retrieve_interval: Duration,
    pub tolerance: Duration,
}

impl TaskConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(TaskConfig {
            connect: ConnectConfig::from_dsn(dsn)?,
            mode: Self::parse_mode(dsn)?,
            table: Self::parse_table(dsn)?,
            tags: Self::parse_tags(dsn),
            begin_datetime: Self::parse_begin_datetime(dsn)?,
            end_datetime: Self::parse_end_datetime(dsn)?,
            time_window: Self::parse_time_window(dsn)?,
            retrieve_interval: Self::parse_retrieve_interval(dsn)?,
            tolerance: Self::parse_tolerance(dsn)?,
        })
    }

    fn parse_mode(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.params
            .get("mode")
            .map(|s| {
                match s.as_str() {
                    "synchronize" => Ok("synchronize".to_string()),
                    "migrate" => Ok("migrate".to_string()),
                    _ => Err(anyhow::anyhow!("mode must be synchronize or migrate"))
                }
            })
            .transpose()?
            .ok_or(anyhow::anyhow!("mode is required"))
    }

    fn parse_table(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.params
            .get("table")
            .map(|s| {
                match s.as_str() {
                    "Runtime.dbo.History" => Ok("Runtime.dbo.History".to_string()),
                    "Runtime.dbo.Live" => Ok("Runtime.dbo.Live".to_string()),
                    _ => Err(anyhow::anyhow!("table must be Runtime.dbo.History or Runtime.dbo.Live"))
                }
            })
            .transpose()?
            .ok_or(anyhow::anyhow!("table is required"))
    }

    fn parse_tags(dsn: &Dsn) -> Vec<String> {
        dsn.params
            .get("tags")
            .map(|s| {
                s.split(",").map(|s| s.to_string()).collect::<Vec<String>>()
            })
            .unwrap_or(vec!["*".to_string()])
    }

    fn parse_begin_datetime(dsn: &Dsn) -> anyhow::Result<DateTime<Utc>> {
        dsn.params
            .get("beginDateTime")
            .map(|s| {
                let date_time = DateTime::parse_from_rfc3339(s)
                    .map_err(|e| {
                        anyhow::anyhow!("failed to parse beginDateTime: {}, cause: {}", s.to_string(),e.to_string())
                    })?
                    .into();
                anyhow::Ok(date_time)
            })
            .transpose()?
            .ok_or(anyhow::anyhow!("beginDateTime is required"))
    }

    fn parse_end_datetime(dsn: &Dsn) -> anyhow::Result<Option<DateTime<Utc>>> {
        let mode = Self::parse_mode(dsn)?;
        if mode.as_str() == "synchronize" {
            return Ok(None);
        }

        let end_date_time = dsn.params
            .get("endDateTime")
            .map(|s| {
                anyhow::Ok(DateTime::parse_from_rfc3339(s)
                    .map_err(|e| {
                        anyhow::anyhow!("failed to parse endDateTime: {}, cause: {}",s.to_string(), e.to_string())
                    })?
                    .into()
                )
            })
            .transpose()?;

        if mode.as_str() == "migrate" && end_date_time.is_none() {
            anyhow::bail!("endDateTime is required when mode is migrate");
        }
        Ok(end_date_time)
    }

    fn parse_time_window(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn.params
            .get("timeWindow")
            .map(|s| {
                let duration = parse_duration::parse(s)
                    .map_err(|err| {
                        anyhow::anyhow!("failed to parse timeWindow: {}, cause: {}", s.to_string(), err.to_string())
                    })?;

                if duration.as_secs() < 60 * 60 {
                    anyhow::bail!("timeWindow must be greater than 1h");
                }

                let duration = Duration::from_std(duration).map_err(|err| {
                    anyhow::anyhow!("failed parse timeWindow: {}, cause: {}", s.to_string(), err.to_string())
                })?;

                Ok(duration)
            })
            .transpose()?
            .unwrap_or(Duration::days(1)))
    }

    fn parse_retrieve_interval(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn.params
            .get("retrieveInterval")
            .map(|s| {
                let duration = parse_duration::parse(s)
                    .map_err(|err| {
                        anyhow::anyhow!("failed to parse retrieveInterval: {}, cause: {}", s.to_string(), err.to_string())
                    })?;

                if duration.as_secs() < 1 {
                    anyhow::bail!("retrieveInterval must be greater than 1s");
                }

                let duration = Duration::from_std(duration)
                    .map_err(|err| {
                        anyhow::anyhow!("failed to parse retrieveInterval: {}, cause: {}", s.to_string(), err.to_string())
                    })?;

                Ok(duration)
            })
            .transpose()?
            .unwrap_or(Duration::seconds(10)))
    }

    fn parse_tolerance(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn.params
            .get("tolerance")
            .map(|s| {
                let duration = parse_duration::parse(s)
                    .map_err(|err| {
                        anyhow::anyhow!("failed to parse tolerance: {}, cause: {}", s.to_string(), err.to_string())
                    })?;

                if duration.as_millis() < 1 {
                    anyhow::bail!("tolerance must be greater than 1ms");
                }

                let duration = Duration::from_std(duration)
                    .map_err(|err| {
                        anyhow::anyhow!("failed to parse tolerance: {}, cause: {}", s.to_string(), err.to_string())
                    })?;

                Ok(duration)
            })
            .transpose()?
            .unwrap_or(Duration::milliseconds(0)))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_mode() {
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_mode(&dsn);
        assert!(config.is_err());
        assert_eq!("mode is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://?mode=synchronize").unwrap();
        let config = TaskConfig::parse_mode(&dsn).unwrap();
        assert_eq!("synchronize", config);

        let dsn = Dsn::from_str("historian://?mode=migrate").unwrap();
        let config = TaskConfig::parse_mode(&dsn).unwrap();
        assert_eq!("migrate", config);

        let dsn = Dsn::from_str("historian://?mode=xxx").unwrap();
        let config = TaskConfig::parse_mode(&dsn);
        assert!(config.is_err());
        assert_eq!("mode must be synchronize or migrate", config.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_table() {
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_table(&dsn);
        assert!(config.is_err());
        assert_eq!("table is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://?table=Runtime.dbo.History").unwrap();
        let config = TaskConfig::parse_table(&dsn).unwrap();
        assert_eq!("Runtime.dbo.History", config);

        let dsn = Dsn::from_str("historian://?table=Runtime.dbo.Live").unwrap();
        let config = TaskConfig::parse_table(&dsn).unwrap();
        assert_eq!("Runtime.dbo.Live", config);

        let dsn = Dsn::from_str("historian://?table=xxx").unwrap();
        let config = TaskConfig::parse_table(&dsn);
        assert!(config.is_err());
        assert_eq!("table must be Runtime.dbo.History or Runtime.dbo.Live", config.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_tags() {
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_tags(&dsn);
        assert_eq!(vec!["*".to_string()], config);

        let dsn = Dsn::from_str("historian://?tags=tag1").unwrap();
        let config = TaskConfig::parse_tags(&dsn);
        assert_eq!(vec!["tag1".to_string()], config);

        let dsn = Dsn::from_str("historian://?tags=tag1,tag2").unwrap();
        let config = TaskConfig::parse_tags(&dsn);
        assert_eq!(vec!["tag1".to_string(), "tag2".to_string()], config);
    }

    #[test]
    fn test_parse_begin_datetime() {
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_begin_datetime(&dsn);
        assert!(config.is_err());
        assert_eq!("beginDateTime is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://?beginDateTime=2021-01-01T00:00:00Z").unwrap();
        let config = TaskConfig::parse_begin_datetime(&dsn).unwrap();
        assert_eq!("2021-01-01T00:00:00+00:00", config.to_rfc3339());

        let dsn = Dsn::from_str("historian://?beginDateTime=xxx").unwrap();
        let config = TaskConfig::parse_begin_datetime(&dsn);
        assert!(config.is_err());
        assert_eq!("failed to parse beginDateTime: xxx, cause: premature end of input", config.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_end_datetime() {
        let dsn = Dsn::from_str("historian://?mode=synchronize").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("historian://?mode=migrate").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn);
        assert!(config.is_err());
        assert_eq!("endDateTime is required when mode is migrate", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://?mode=migrate&endDateTime=2021-01-01T00:00:00Z").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn).unwrap();
        assert_eq!("2021-01-01T00:00:00+00:00", config.unwrap().to_rfc3339());

        let dsn = Dsn::from_str("historian://?mode=migrate&endDateTime=xxx").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn);
        assert!(config.is_err());
        assert_eq!("failed to parse endDateTime: xxx, cause: premature end of input", config.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_time_window(){
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_time_window(&dsn).unwrap();
        assert_eq!(Duration::days(1), config);

        let dsn = Dsn::from_str("historian://?timeWindow=1h").unwrap();
        let config = TaskConfig::parse_time_window(&dsn).unwrap();
        assert_eq!(Duration::hours(1), config);

        let dsn = Dsn::from_str("historian://?timeWindow=xxx").unwrap();
        let config = TaskConfig::parse_time_window(&dsn);
        assert!(config.is_err());
        assert_eq!("failed to parse timeWindow: xxx, cause: NoValueFoundError: no value found in the string \"xxx\"", config.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_retrieve_interval(){
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_retrieve_interval(&dsn).unwrap();
        assert_eq!(Duration::seconds(10), config);

        let dsn = Dsn::from_str("historian://?retrieveInterval=1s").unwrap();
        let config = TaskConfig::parse_retrieve_interval(&dsn).unwrap();
        assert_eq!(Duration::seconds(1), config);

        let dsn = Dsn::from_str("historian://?retrieveInterval=xxx").unwrap();
        let config = TaskConfig::parse_retrieve_interval(&dsn);
        assert!(config.is_err());
        assert_eq!("failed to parse retrieveInterval: xxx, cause: NoValueFoundError: no value found in the string \"xxx\"", config.unwrap_err().to_string());
    }

    #[test]
    fn test_parse_tolerance(){
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_tolerance(&dsn).unwrap();
        assert_eq!(Duration::milliseconds(0), config);

        let dsn = Dsn::from_str("historian://?tolerance=1ms").unwrap();
        let config = TaskConfig::parse_tolerance(&dsn).unwrap();
        assert_eq!(Duration::milliseconds(1), config);

        let dsn = Dsn::from_str("historian://?tolerance=xxx").unwrap();
        let config = TaskConfig::parse_tolerance(&dsn);
        assert!(config.is_err());
        assert_eq!("failed to parse tolerance: xxx, cause: NoValueFoundError: no value found in the string \"xxx\"", config.unwrap_err().to_string());
    }
}
