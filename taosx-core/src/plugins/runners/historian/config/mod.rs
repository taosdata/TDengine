use crate::plugins::config::AdvancedOptions;
use anyhow::bail;
use chrono::{DateTime, Duration, Utc};
use std::str::FromStr;
use taos::Dsn;

use crate::runners::historian::config::connect::ConnectConfig;

pub mod connect;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum TaskMode {
    Synchronize,
    Migrate,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum HistorianTable {
    History,
    Live,
}

impl FromStr for HistorianTable {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Runtime.dbo.History" => Ok(Self::History),
            "Runtime.dbo.Live" => Ok(Self::Live),
            _ => Err(anyhow::anyhow!(
                "invalid historian table: {}, must be Runtime.dbo.History or Runtime.dbo.Live",
                s
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub connect: ConnectConfig,
    pub ipc_port: Option<u16>,

    // collect
    pub mode: TaskMode,
    pub table: HistorianTable,
    pub tags: Vec<String>,
    pub tag_list_size: usize, // split tags into multiple lists, each list contains tag_list_size tags
    pub begin_datetime: Option<DateTime<Utc>>,
    pub end_datetime: Option<DateTime<Utc>>,
    pub time_window: Duration,
    pub retrieve_interval: Duration,
    pub tolerance: Duration,

    // advanced options
    pub advanced_options: AdvancedOptions,
}

impl TaskConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(TaskConfig {
            connect: ConnectConfig::from_dsn(dsn)?,
            ipc_port: None,
            mode: Self::parse_mode(dsn)?,
            table: Self::parse_table(dsn)?,
            tags: Self::parse_tags(dsn),
            tag_list_size: Self::parse_tag_list_size(dsn)?,
            begin_datetime: Self::parse_begin_datetime(dsn)?,
            end_datetime: Self::parse_end_datetime(dsn)?,
            time_window: Self::parse_time_window(dsn)?,
            retrieve_interval: Self::parse_retrieve_interval(dsn)?,
            tolerance: Self::parse_tolerance(dsn)?,
            advanced_options: AdvancedOptions::from_dsn(dsn)?,
        })
    }

    fn parse_mode(dsn: &Dsn) -> anyhow::Result<TaskMode> {
        dsn.params
            .get("mode")
            .map(|s| match s.as_str() {
                "synchronize" => Ok(TaskMode::Synchronize),
                "migrate" => Ok(TaskMode::Migrate),
                _ => Err(anyhow::anyhow!("mode must be synchronize or migrate")),
            })
            .transpose()?
            .ok_or(anyhow::anyhow!("mode is required"))
    }

    fn parse_table(dsn: &Dsn) -> anyhow::Result<HistorianTable> {
        let table = dsn
            .params
            .get("table")
            .map(|s| HistorianTable::from_str(s))
            .transpose()?
            .ok_or(anyhow::anyhow!("table is required"))?;

        let mode = Self::parse_mode(dsn)?;
        if mode == TaskMode::Migrate && table == HistorianTable::Live {
            bail!("table must be Runtime.dbo.History when mode is migrate");
        }
        Ok(table)
    }

    fn parse_tags(dsn: &Dsn) -> Vec<String> {
        dsn.params
            .get("tags")
            .map(|s| s.split(",").map(|s| s.to_string()).collect::<Vec<String>>())
            .unwrap_or(vec!["*".to_string()])
    }

    fn parse_tag_list_size(dsn: &Dsn) -> anyhow::Result<usize> {
        Ok(dsn
            .params
            .get("tagListSize")
            .map(|s| {
                let tag_list_size = s.parse::<usize>().map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse tagListSize: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;
                if tag_list_size < 1 {
                    bail!("tagListSize must be greater than 1");
                }
                Ok(tag_list_size)
            })
            .transpose()?
            .unwrap_or(10))
    }

    fn parse_begin_datetime(dsn: &Dsn) -> anyhow::Result<Option<DateTime<Utc>>> {
        let begin_datetime = dsn
            .params
            .get("beginDateTime")
            .map(|s| {
                let date_time = DateTime::parse_from_rfc3339(s)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "failed to parse beginDateTime: {}, cause: {}",
                            s.to_string(),
                            e.to_string()
                        )
                    })?
                    .into();
                anyhow::Ok(date_time)
            })
            .transpose()?;

        let table = Self::parse_table(dsn);
        if table.is_ok() && begin_datetime.is_none() && table.unwrap() == HistorianTable::History {
            bail!("beginDateTime is required when table is Runtime.dbo.History");
        }

        Ok(begin_datetime)
    }

    fn parse_end_datetime(dsn: &Dsn) -> anyhow::Result<Option<DateTime<Utc>>> {
        let end_date_time = dsn
            .params
            .get("endDateTime")
            .map(|s| {
                anyhow::Ok(
                    DateTime::parse_from_rfc3339(s)
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "failed to parse endDateTime: {}, cause: {}",
                                s.to_string(),
                                e.to_string()
                            )
                        })?
                        .into(),
                )
            })
            .transpose()?;

        let mode = Self::parse_mode(dsn);
        let table = Self::parse_table(dsn);
        if mode.is_err() || table.is_err() {
            return Ok(end_date_time);
        }

        if end_date_time.is_none()
            && mode.unwrap() == TaskMode::Migrate
            && table.unwrap() == HistorianTable::History
        {
            bail!("endDateTime is required when mode is migrate and table is Runtime.dbo.History");
        }

        Ok(end_date_time)
    }

    fn parse_time_window(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn
            .params
            .get("timeWindow")
            .map(|s| {
                let duration = parse_duration::parse(s).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse timeWindow: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;

                let duration = Duration::from_std(duration).map_err(|err| {
                    anyhow::anyhow!(
                        "failed parse timeWindow: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;

                anyhow::Ok(duration)
            })
            .transpose()?
            .unwrap_or(Duration::days(1)))
    }

    fn parse_retrieve_interval(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn
            .params
            .get("retrieveInterval")
            .map(|s| {
                let duration = parse_duration::parse(s).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse retrieveInterval: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;

                if duration.as_secs() < 1 {
                    anyhow::bail!("retrieveInterval must be greater than 1s");
                }

                let duration = Duration::from_std(duration).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse retrieveInterval: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;

                Ok(duration)
            })
            .transpose()?
            .unwrap_or(Duration::seconds(10)))
    }

    fn parse_tolerance(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn
            .params
            .get("tolerance")
            .map(|s| {
                let duration = parse_duration::parse(s).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse tolerance: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;

                let duration = Duration::from_std(duration).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse tolerance: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;

                anyhow::Ok(duration)
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
        assert_eq!(TaskMode::Synchronize, config);

        let dsn = Dsn::from_str("historian://?mode=migrate").unwrap();
        let config = TaskConfig::parse_mode(&dsn).unwrap();
        assert_eq!(TaskMode::Migrate, config);

        let dsn = Dsn::from_str("historian://?mode=xxx").unwrap();
        let config = TaskConfig::parse_mode(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "mode must be synchronize or migrate",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_table() {
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_table(&dsn);
        assert!(config.is_err());
        assert_eq!("table is required", config.unwrap_err().to_string());

        let dsn =
            Dsn::from_str("historian://?mode=synchronize&&table=Runtime.dbo.History").unwrap();
        let config = TaskConfig::parse_table(&dsn).unwrap();
        assert_eq!(HistorianTable::History, config);

        let dsn = Dsn::from_str("historian://?mode=synchronize&&table=Runtime.dbo.Live").unwrap();
        let config = TaskConfig::parse_table(&dsn).unwrap();
        assert_eq!(HistorianTable::Live, config);

        let dsn = Dsn::from_str("historian://?mode=synchronize&&table=xxx").unwrap();
        let config = TaskConfig::parse_table(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "table must be Runtime.dbo.History or Runtime.dbo.Live",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("historian://?mode=migrate&table=Runtime.dbo.History").unwrap();
        let config = TaskConfig::parse_table(&dsn).unwrap();
        assert_eq!(HistorianTable::History, config);

        let dsn = Dsn::from_str("historian://?mode=migrate&table=Runtime.dbo.Live").unwrap();
        let config = TaskConfig::parse_table(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "table must be Runtime.dbo.History when mode is migrate",
            config.unwrap_err().to_string()
        );
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
    fn test_parse_tag_list_size() {
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_tag_list_size(&dsn).unwrap();
        assert_eq!(10, config);

        let dsn = Dsn::from_str("historian://?tagListSize=1").unwrap();
        let config = TaskConfig::parse_tag_list_size(&dsn).unwrap();
        assert_eq!(1, config);

        let dsn = Dsn::from_str("historian://?tagListSize=xxx").unwrap();
        let config = TaskConfig::parse_tag_list_size(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "failed to parse tagListSize: xxx, cause: invalid digit found in string",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_begin_datetime() {
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_begin_datetime(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("historian://?beginDateTime=2021-01-01T00:00:00Z").unwrap();
        let config = TaskConfig::parse_begin_datetime(&dsn).unwrap();
        assert_eq!("2021-01-01T00:00:00+00:00", config.unwrap().to_rfc3339());

        let dsn = Dsn::from_str("historian://?beginDateTime=xxx").unwrap();
        let config = TaskConfig::parse_begin_datetime(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "failed to parse beginDateTime: xxx, cause: premature end of input",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_end_datetime() {
        let dsn = Dsn::from_str("historian://").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("historian://?mode=synchronize").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn).unwrap();
        assert!(config.is_none());

        let dsn = Dsn::from_str("historian://?mode=migrate").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn).unwrap();
        assert!(config.is_none());

        let dsn =
            Dsn::from_str("historian://?mode=migrate&endDateTime=2021-01-01T00:00:00Z").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn).unwrap();
        assert_eq!("2021-01-01T00:00:00+00:00", config.unwrap().to_rfc3339());

        let dsn = Dsn::from_str("historian://?mode=migrate&table=Runtime.dbo.History").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "endDateTime is required when mode is migrate and table is Runtime.dbo.History",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str("historian://?mode=migrate&endDateTime=xxx").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "failed to parse endDateTime: xxx, cause: premature end of input",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_parse_time_window() {
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
    fn test_parse_retrieve_interval() {
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
    fn test_parse_tolerance() {
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
