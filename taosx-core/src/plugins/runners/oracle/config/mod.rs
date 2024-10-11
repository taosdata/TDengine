use crate::runners::oracle::config::connect::ConnectConfig;
use crate::utils::replace_date_placeholder;
use crate::{plugins::config::AdvancedOptions, utils};
use anyhow::Ok;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use std::str::FromStr;
use taos::Dsn;

pub mod connect;

#[derive(Debug, Clone)]
pub struct OracleConfig {
    // task info
    pub task_id: Option<i64>,
    pub sub_task_id: Option<String>,
    pub ipc_port: Option<u16>,
    // the datasource config
    pub connect: ConnectConfig,
    // the task config
    pub task: TaskConfig,
    // the advanced options
    pub advanced: AdvancedOptions,
}

impl OracleConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        if dsn.driver != "oracle" {
            return Err(anyhow::anyhow!("invalid driver: {}", dsn.driver));
        }
        Ok(OracleConfig {
            task_id: Self::parse_task_id(dsn),
            sub_task_id: None,
            ipc_port: None,
            connect: ConnectConfig::from_dsn(dsn)?,
            task: TaskConfig::from_dsn(dsn)?,
            advanced: AdvancedOptions::from_dsn(dsn)?,
        })
    }

    fn parse_task_id(dsn: &Dsn) -> Option<i64> {
        dsn.params.get("taskId").and_then(|s| {
            s.parse::<i64>()
                .map(Some)
                .inspect_err(|_err| {
                    tracing::warn!("failed to parse taskId: {}, use None", s);
                })
                .unwrap_or(None)
        })
    }
}

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub subtable_fields: Option<String>,
    pub sql: String,
    pub start: DateTime<Utc>,
    pub end: Option<DateTime<Utc>>,
    pub time_zone: String,
    pub interval: Duration,
    pub delay: Duration,
    pub sample_data_limit: u32,
}

impl TaskConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        Ok(TaskConfig {
            subtable_fields: Self::parse_subtable_fields(dsn),
            sql: Self::parse_sql(dsn)?,
            start: Self::parse_start(dsn)?,
            end: Self::parse_end(dsn)?,
            time_zone: Self::parse_time_zone(dsn)?,
            interval: Self::parse_interval(dsn)?,
            delay: Self::parse_delay(dsn)?,
            sample_data_limit: Self::parse_sample_data_limit(dsn)?,
        })
    }

    fn parse_subtable_fields(dsn: &Dsn) -> Option<String> {
        dsn.params.get("subtable_fields").map(|s| s.to_string())
    }

    fn parse_sql(dsn: &Dsn) -> anyhow::Result<String> {
        Ok(dsn
            .params
            .get("sql")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("sql is required"))?)
    }

    fn parse_start(dsn: &Dsn) -> anyhow::Result<DateTime<Utc>> {
        let start = dsn
            .params
            .get("start")
            .map(|s| {
                let start_time = DateTime::parse_from_rfc3339(s)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "failed to parse start: {}, cause: {}",
                            s.to_string(),
                            e.to_string()
                        )
                    })?
                    .into();
                anyhow::Ok(start_time)
            })
            .transpose()?
            .expect("start is required");
        Ok(start)
    }

    fn parse_end(dsn: &Dsn) -> anyhow::Result<Option<DateTime<Utc>>> {
        let end = dsn
            .params
            .get("end")
            .map(|s| {
                let end_time = DateTime::parse_from_rfc3339(s)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "failed to parse end: {}, cause: {}",
                            s.to_string(),
                            e.to_string()
                        )
                    })?
                    .into();
                anyhow::Ok(Some(end_time))
            })
            .transpose()?
            .unwrap_or(None);
        Ok(end)
    }

    fn parse_time_zone(dsn: &Dsn) -> anyhow::Result<String> {
        // try to parse from start time
        let start = dsn.params.get("start");
        let time_zone = match start {
            Some(start) => {
                if !start.is_empty() {
                    let start_time = DateTime::parse_from_rfc3339(start);
                    match start_time {
                        Result::Ok(start_time) => start_time.format("%Z").to_string(),
                        Err(_) => "+00:00".to_string(),
                    }
                } else {
                    "+00:00".to_string()
                }
            }
            None => "+00:00".to_string(),
        };
        // get time_zone from params or use the time_zone in start time
        Ok(dsn
            .params
            .get("time_zone")
            .unwrap_or(&time_zone)
            .to_string())
    }

    fn parse_interval(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn
            .params
            .get("interval")
            .map(|s| {
                let duration = utils::parse_duration(s).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse interval: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;
                let duration = Duration::from_std(duration).map_err(|err| {
                    anyhow::anyhow!(
                        "failed parse interval: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;
                anyhow::Ok(duration)
            })
            .transpose()?
            .unwrap_or(Duration::try_days(1).unwrap()))
    }

    fn parse_delay(dsn: &Dsn) -> anyhow::Result<Duration> {
        Ok(dsn
            .params
            .get("delay")
            .map(|s| {
                let delay = utils::parse_duration(s).map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse delay: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;
                let delay = Duration::from_std(delay).map_err(|err| {
                    anyhow::anyhow!(
                        "failed parse delay: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;
                anyhow::Ok(delay)
            })
            .transpose()?
            .unwrap_or(Duration::try_seconds(5).unwrap()))
    }

    fn parse_sample_data_limit(dsn: &Dsn) -> anyhow::Result<u32> {
        Ok(dsn
            .params
            .get("sample_data_limit")
            .map(|s| {
                let limit = s.parse::<u32>().map_err(|err| {
                    anyhow::anyhow!(
                        "failed to parse sample_data_limit: {}, cause: {}",
                        s.to_string(),
                        err.to_string()
                    )
                })?;
                anyhow::Ok(limit)
            })
            .transpose()?
            .unwrap_or(5))
    }

    pub fn generate_distinct_sql(&self) -> anyhow::Result<String> {
        // generate sql
        let sql = self.subtable_fields.clone().unwrap_or("".to_string());

        // task start time with time zone
        let start = self.start;
        let time_zone = FixedOffset::from_str(&self.time_zone.to_string())?;
        let start_tz = start.with_timezone(&time_zone);

        // replace the placeholders
        anyhow::Ok(replace_date_placeholder(sql.clone(), start_tz))
    }

    pub fn generate_sql(&self) -> anyhow::Result<String> {
        // replace ${start} and ${end} with the actual start and end time
        let start = self.start;
        let end = self.end.unwrap_or(Utc::now());
        let time_zone = FixedOffset::from_str(&self.time_zone.to_string())?;

        let start_tz = start.with_timezone(&time_zone);
        let end_tz = end.with_timezone(&time_zone);

        let mut sql = self.sql.clone();

        // whether the sql contains time range
        let mut time_range_exist = false;

        if sql.contains("${start}") && sql.contains("${end}") {
            let query_start = format!(
                "TO_DATE('{}','YYYY-MM-DD HH24:MI:SS')",
                start_tz.format("%Y-%m-%d %H:%M:%S")
            );
            let query_end = format!(
                "TO_DATE('{}','YYYY-MM-DD HH24:MI:SS')",
                end_tz.format("%Y-%m-%d %H:%M:%S")
            );
            sql = sql
                .replace("${start}", &query_start)
                .replace("${end}", &query_end);
            time_range_exist = true;
        }
        if sql.contains("${start_no_tz}") && sql.contains("${end_no_tz}") {
            let query_start = format!(
                "TO_DATE('{}','YYYY-MM-DD HH24:MI:SS')",
                start_tz.format("%Y-%m-%d %H:%M:%S")
            );
            let query_end = format!(
                "TO_DATE('{}','YYYY-MM-DD HH24:MI:SS')",
                end_tz.format("%Y-%m-%d %H:%M:%S")
            );
            sql = sql
                .replace("${start_no_tz}", &query_start)
                .replace("${end_no_tz}", &query_end);
            time_range_exist = true;
        }
        if sql.contains("${start_date}") && sql.contains("${end_date}") {
            let query_start = format!("TO_DATE('{}','YYYY-MM-DD')", start_tz.format("%Y-%m-%d"));
            let query_end = format!("TO_DATE('{}','YYYY-MM-DD')", end_tz.format("%Y-%m-%d"));
            sql = sql
                .replace("${start_date}", &query_start)
                .replace("${end_date}", &query_end);
            time_range_exist = true;
        }
        if sql.contains("${start_time}") && sql.contains("${end_time}") {
            let query_start = format!("'{}'", start_tz.format("%H:%M:%S"));
            let mut query_end = format!("'{}'", end_tz.format("%H:%M:%S"));
            // modify endtime to 24:00:00 instead of 00:00:00
            if query_end == "'00:00:00'" || end_tz.date_naive() > start_tz.date_naive() {
                query_end = String::from("'24:00:00'");
            }
            sql = sql
                .replace("${start_time}", &query_start)
                .replace("${end_time}", &query_end);
            time_range_exist = true;
        }
        if !time_range_exist {
            anyhow::bail!("invalid sql template, missing start and end");
        }

        // sharding by time
        anyhow::Ok(replace_date_placeholder(sql.clone(), start_tz))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config_invalid_driver() {
        let dsn = Dsn::from_str("oraclex://root:password@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric&start=2021-01-01T00:00:00Z&end=2021-01-02T00:00:00Z&interval=1d&delay=0")
            .unwrap();
        let config = OracleConfig::from_dsn(&dsn);
        dbg!(&config);
        assert!(config.is_err());
    }

    #[test]
    fn test_parse_config() {
        let dsn = Dsn::from_str("oracle://root:password@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric&start=2021-01-01T00:00:00Z&end=2021-01-02T00:00:00Z&interval=1d&delay=5")
            .unwrap();
        let config = OracleConfig::from_dsn(&dsn).unwrap();
        dbg!(&config);
        assert_eq!(config.connect.host, "192.168.1.40");
        assert_eq!(config.connect.port, 1521);
        assert_eq!(config.connect.username, "root");
        assert_eq!(config.connect.password, "password");
        assert_eq!(config.connect.subject, "ORCLPDB1");
        assert_eq!(config.task.sql, "select * from t_metric");
        assert_eq!(
            config.task.start,
            "2021-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            config.task.end,
            Some("2021-01-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert_eq!(config.task.interval, Duration::try_days(1).unwrap());
        assert_eq!(config.task.delay, Duration::try_seconds(5).unwrap());
    }

    #[test]
    fn test_generate_sql() {
        let dsn = Dsn::from_str("oracle://root:password@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric where ts>=${start} and ts<${end}&start=2021-01-01T00:00:00Z&end=2021-01-02T00:00:00Z&interval=1d&delay=0")
            .unwrap();
        let config = OracleConfig::from_dsn(&dsn).unwrap();
        let sql = config.task.generate_sql().unwrap();
        dbg!(&sql);
        assert!(sql.contains("TO_DATE('2021-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')"));
        assert!(sql.contains("TO_DATE('2021-01-02 00:00:00','YYYY-MM-DD HH24:MI:SS')"));

        let dsn = Dsn::from_str("oracle://root:password@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric where ts>=${start_date} and ts<${end_date}&start=2021-01-01T00:00:00Z&end=2021-01-02T00:00:00Z&interval=1d&delay=0")
            .unwrap();
        let config = OracleConfig::from_dsn(&dsn).unwrap();
        let sql = config.task.generate_sql().unwrap();
        dbg!(&sql);
        assert!(sql.contains("'2021-01-01'"));
        assert!(sql.contains("'2021-01-02'"));

        let dsn = Dsn::from_str("oracle://root:password@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric where ts>=${start_time} and ts<${end_time}&start=2021-01-01T00:00:00Z&end=2021-01-02T00:00:00Z&interval=1d&delay=0")
            .unwrap();
        let config = OracleConfig::from_dsn(&dsn).unwrap();
        let sql = config.task.generate_sql().unwrap();
        dbg!(&sql);
        assert!(sql.contains("'00:00:00'"));
        assert!(sql.contains("'24:00:00'"));
    }
}
