use anyhow::bail;
use chrono::{DateTime, Duration, Utc};
use std::str::FromStr;
use taos::Dsn;

use taosx_core::plugins::config::AdvancedOptions;
use taosx_core::utils;

use super::{HistorianTable, TaskMode};

#[derive(Debug, Clone)]
pub struct TaskConfig {
    // task info
    pub task_id: Option<i64>,
    pub sub_task_id: Option<String>,
    // communication
    pub connect: ConnectConfig,
    pub ipc_port: Option<u16>,
    // collect
    pub mode: TaskMode,
    pub table: HistorianTable,
    pub tags: Vec<String>,
    pub tag_list_size: usize,
    // split tags into multiple lists, each list contains tag_list_size tags
    pub begin_datetime: Option<DateTime<Utc>>,
    pub end_datetime: Option<DateTime<Utc>>,
    pub time_window: Duration,
    pub retrieve_interval: Duration,
    pub tolerance: Duration,
    pub sample_data_limit: usize,
    // advanced options
    pub advanced_options: AdvancedOptions,
}

impl TaskConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let task_id = Self::parse_task_id(dsn);
        let connect = ConnectConfig::from_dsn(dsn)?;
        let mode = Self::parse_mode(dsn)?;
        let table = Self::parse_table(dsn)?;
        if mode == TaskMode::Migrate && table == HistorianTable::Live {
            bail!("table must be Runtime.dbo.History when mode is migrate");
        }

        let tags = Self::parse_tags(dsn);
        let tag_list_size = Self::parse_tag_list_size(dsn)?;

        let begin_datetime = Self::parse_begin_datetime(dsn)?;
        if begin_datetime.is_none() && table == HistorianTable::History {
            bail!("beginDateTime is required when table is Runtime.dbo.History");
        }

        let end_datetime = Self::parse_end_datetime(dsn)?;
        if end_datetime.is_none() && mode == TaskMode::Migrate && table == HistorianTable::History {
            bail!("endDateTime is required when mode is migrate and table is Runtime.dbo.History");
        }

        let time_window = Self::parse_time_window(dsn)?;
        let retrieve_interval = Self::parse_retrieve_interval(dsn)?;
        let tolerance = Self::parse_tolerance(dsn)?;
        let sample_data_limit = Self::parse_sample_data_limit(dsn)?;
        let advanced_options = AdvancedOptions::from_dsn(dsn)?;

        Ok(TaskConfig {
            task_id,
            sub_task_id: None,
            connect,
            ipc_port: None,
            mode,
            table,
            tags,
            tag_list_size,
            begin_datetime,
            end_datetime,
            time_window,
            retrieve_interval,
            tolerance,
            sample_data_limit,
            advanced_options,
        })
    }

    fn parse_task_id(dsn: &Dsn) -> Option<i64> {
        dsn.params.get("taskId").and_then(|s| {
            s.parse::<i64>()
                .map(Some)
                .inspect_err(|err| {
                    tracing::warn!("failed to parse taskId: {} ({err}), use None", s);
                })
                .unwrap_or(None)
        })
    }

    fn parse_mode(dsn: &Dsn) -> anyhow::Result<TaskMode> {
        dsn.params
            .get("mode")
            .map(|s| TaskMode::from_str(s))
            .transpose()?
            .ok_or(anyhow::anyhow!("mode is required"))
    }

    pub fn parse_table(dsn: &Dsn) -> anyhow::Result<HistorianTable> {
        let table = dsn
            .params
            .get("table")
            .map(|s| HistorianTable::from_str(s))
            .transpose()?
            .ok_or(anyhow::anyhow!("table is required"))?;

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

        Ok(end_date_time)
    }

    fn parse_time_window(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn
            .params
            .get("timeWindow")
            .map(|s| {
                let duration = utils::parse_duration(s).map_err(|err| {
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
                let duration = utils::parse_duration(s).map_err(|err| {
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
                let duration = utils::parse_duration(s).map_err(|err| {
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

    fn parse_sample_data_limit(dsn: &Dsn) -> anyhow::Result<usize> {
        Ok(dsn
            .params
            .get("sample_data_limit")
            .map(|s| {
                let sample_data_limit = s.parse::<usize>().map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse sample_data_limit: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;
                if sample_data_limit == 0 {
                    bail!("sample_data_limit must be greater than 0");
                }
                Ok(sample_data_limit)
            })
            .transpose()?
            .unwrap_or(3))
    }
}

#[derive(Debug, Clone)]
pub struct ConnectConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: String,
}

impl ConnectConfig {
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
            .unwrap_or(1433);

        let username = dsn
            .username
            .clone()
            .ok_or_else(|| anyhow::anyhow!("username is required"))?;

        let password = dsn
            .password
            .clone()
            .ok_or_else(|| anyhow::anyhow!("password is required"))?;

        Ok(ConnectConfig {
            host,
            port,
            username,
            password,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_connect_config_from_dsn() {
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

    #[test]
    fn test_task_config_from_dsn() {
        let dsn = Dsn::from_str("historian://").unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("host is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://localhost").unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("username is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://aaAdmin@localhost").unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("password is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@localhost").unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("mode is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str("historian://aaAdmin:aaAdmin@localhost?mode=migrate").unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!("table is required", config.unwrap_err().to_string());

        let dsn = Dsn::from_str(
            "historian://aaAdmin:aaAdmin@localhost?mode=migrate&table=Runtime.dbo.Live",
        )
        .unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "table must be Runtime.dbo.History when mode is migrate",
            config.unwrap_err().to_string()
        );

        let dsn = Dsn::from_str(
            "historian://aaAdmin:aaAdmin@host?mode=migrate&table=Runtime.dbo.History&beginDateTime=2024-01-01T00:00:00Z",
        )
        .unwrap();
        let config = TaskConfig::from_dsn(&dsn);
        assert!(config.is_err());
        assert_eq!(
            "endDateTime is required when mode is migrate and table is Runtime.dbo.History",
            config.unwrap_err().to_string()
        );
    }

    #[test]
    fn test_historian_table_fmt() {
        assert_eq!(
            "Runtime.dbo.Live".to_string(),
            HistorianTable::Live.to_string()
        );
        assert_eq!(
            "Runtime.dbo.History".to_string(),
            HistorianTable::History.to_string()
        );
    }

    #[test]
    fn test_parse_task_id() {
        let dsn = Dsn::from_str("historian://?").unwrap();
        let config = TaskConfig::parse_task_id(&dsn);
        assert!(config.is_none());

        let dsn = Dsn::from_str("historian://?taskId=1").unwrap();
        let config = TaskConfig::parse_task_id(&dsn).unwrap();
        assert_eq!(1, config);

        let dsn = Dsn::from_str("historian://?taskId=xxx").unwrap();
        let config = TaskConfig::parse_task_id(&dsn);
        assert!(config.is_none());
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

        let dsn = Dsn::from_str("historian://?endDateTime=2021-01-01T00:00:00Z").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn).unwrap();
        assert_eq!("2021-01-01T00:00:00+00:00", config.unwrap().to_rfc3339());

        let dsn = Dsn::from_str("historian://?endDateTime=xxx").unwrap();
        let config = TaskConfig::parse_end_datetime(&dsn);
        assert!(config.is_err());
        assert!(
            config
                .unwrap_err()
                .to_string()
                .starts_with("failed to parse endDateTime: xxx, cause:")
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
    }
}
