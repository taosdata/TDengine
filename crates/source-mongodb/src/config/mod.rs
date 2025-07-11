use std::{collections::BTreeMap, str::FromStr};

use chrono::{DateTime, Duration, FixedOffset, Utc};

use mongodb::bson::{Bson, Document};
use std::result::Result::Ok;
use taos::Dsn;
use taosx_core::utils::replace_date_placeholder;
use taosx_core::{plugins::config::AdvancedOptions, utils};

use connect::ConnectConfig;

pub mod connect;

#[derive(Debug, Clone)]
pub struct MongoDBConfig {
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

impl MongoDBConfig {
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        if dsn.driver != "mongodb" {
            return Err(anyhow::anyhow!("invalid driver: {}", dsn.driver));
        }
        Ok(MongoDBConfig {
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
    pub database: String,
    pub collection: String,
    pub subtable_fields: BTreeMap<String, String>,
    pub sql: String,
    pub sort: Option<String>,
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
            database: Self::parse_database(dsn)?,
            collection: Self::parse_collection(dsn)?,
            subtable_fields: Self::parse_subtable_fields(dsn),
            sql: Self::parse_sql(dsn)?,
            sort: Self::parse_sort(dsn),
            start: Self::parse_start(dsn)?,
            end: Self::parse_end(dsn)?,
            time_zone: Self::parse_time_zone(dsn)?,
            interval: Self::parse_interval(dsn)?,
            delay: Self::parse_delay(dsn)?,
            sample_data_limit: Self::parse_sample_data_limit(dsn)?,
        })
    }

    fn parse_database(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.params
            .get("database")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("database is required"))
    }

    fn parse_collection(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.params
            .get("collection")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("collection is required"))
    }

    fn parse_subtable_fields(dsn: &Dsn) -> BTreeMap<String, String> {
        let subtable_fields = dsn.params.get("subtable_fields");
        // transform "name,sn" to BTreeMap<String, String>
        if let Some(subtable_fields) = subtable_fields {
            if !subtable_fields.is_empty() {
                return subtable_fields
                    .split(",")
                    .map(|s| (s.to_string(), format!("\"{}\":${{v}}", s)))
                    .collect::<BTreeMap<String, String>>();
            }
        }
        BTreeMap::new()
    }

    fn parse_sql(dsn: &Dsn) -> anyhow::Result<String> {
        dsn.params
            .get("sql")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("sql is required"))
    }

    fn parse_sort(dsn: &Dsn) -> Option<String> {
        dsn.params.get("sort").cloned()
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

    pub fn generate_database(&self) -> anyhow::Result<String> {
        // replace ${start} and ${end} with the actual start and end time
        let time_zone = FixedOffset::from_str(&self.time_zone.to_string())?;
        let start_tz = self.start.with_timezone(&time_zone);

        // sharding by time
        anyhow::Ok(replace_date_placeholder(self.database.clone(), start_tz))
    }

    pub fn generate_collection(&self) -> anyhow::Result<String> {
        // replace ${start} and ${end} with the actual start and end time
        let time_zone = FixedOffset::from_str(&self.time_zone.to_string())?;
        let start_tz = self.start.with_timezone(&time_zone);

        // sharding by time
        anyhow::Ok(replace_date_placeholder(self.collection.clone(), start_tz))
    }

    pub fn generate_filter(&self) -> anyhow::Result<Document> {
        // replace ${start} and ${end} with the actual start and end time
        let start = self.start;
        let end = self.end.unwrap_or(Utc::now());
        let time_zone = FixedOffset::from_str(&self.time_zone.to_string())?;

        let start_tz = start.with_timezone(&time_zone);
        let end_tz = end.with_timezone(&time_zone);

        let mut sql = self.sql.clone();

        // whether the sql contains time range
        let mut time_range_exist = false;

        if sql.contains("${start_datetime}") && sql.contains("${end_datetime}") {
            let query_start = Bson::DateTime(mongodb::bson::DateTime::from_millis(
                start_tz.timestamp_millis(),
            ));
            let query_end = Bson::DateTime(mongodb::bson::DateTime::from_millis(
                end_tz.timestamp_millis(),
            ));
            sql = sql
                .replace(
                    "${start_datetime}",
                    serde_json::to_string(&query_start).unwrap().as_str(),
                )
                .replace(
                    "${end_datetime}",
                    serde_json::to_string(&query_end).unwrap().as_str(),
                );
            time_range_exist = true;
        }
        if sql.contains("${start_timestamp}") && sql.contains("${end_timestamp}") {
            let query_start = Bson::Timestamp(mongodb::bson::Timestamp {
                time: start_tz.timestamp() as u32,
                increment: 0,
            });
            let query_end = Bson::Timestamp(mongodb::bson::Timestamp {
                time: end_tz.timestamp() as u32,
                increment: 0,
            });
            sql = sql
                .replace(
                    "${start_timestamp}",
                    serde_json::to_string(&query_start).unwrap().as_str(),
                )
                .replace(
                    "${end_timestamp}",
                    serde_json::to_string(&query_end).unwrap().as_str(),
                );
            time_range_exist = true;
        }
        if !time_range_exist {
            if !sql.contains("${start_datetime}") && sql.contains("${end_datetime}") {
                anyhow::bail!("invalid query template, missing start_datetime");
            } else if sql.contains("${start_datetime}") && !sql.contains("${end_datetime}") {
                anyhow::bail!("invalid query template, missing end_datetime");
            } else if !sql.contains("${start_timestamp}") && sql.contains("${end_timestamp}") {
                anyhow::bail!("invalid query template, missing start_timestamp");
            } else if sql.contains("${start_timestamp}") && !sql.contains("${end_timestamp}") {
                anyhow::bail!("invalid query template, missing end_timestamp");
            } else {
                anyhow::bail!("invalid query template, missing start and end");
            }
        }
        let document: Result<Document, serde_json::Error> = serde_json::from_str(sql.as_str());
        match document {
            Ok(document) => anyhow::Ok(document),
            Err(e) => anyhow::bail!("parsing query template failed, cause: {}, sql: {}", e, sql),
        }
    }

    pub fn generate_sort(&self) -> anyhow::Result<Document> {
        let sort: Result<Document, serde_json::Error> = if let Some(sort) = self.sort.clone() {
            serde_json::from_str(sort.as_str())
        } else {
            Ok(Document::new())
        };
        match sort {
            Ok(sort) => anyhow::Ok(sort),
            Err(e) => anyhow::bail!("parsing sort failed: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use taosx_core::get_data_dir;

    use super::*;

    #[test]
    fn test_parse_config_invalid_driver() {
        let dsn = Dsn::from_str("mongodbx://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn);
        dbg!(&config);
        assert!(config.is_err());
    }

    #[test]
    fn test_parse_config() {
        let dsn = Dsn::from_str("mongodb://admin:123456@localhost:27017?load_balanced=true&direct_connection=true&repl_set_name=repl&local_threshold=10ms&mechanism=MongoDbCr&source=admin&app_name=appname&compressors=zstd&tls=true&ca_file_path=@./file/ca.pem&cert_key_file_path=@./file/cert.pem&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        dbg!(&config);
        assert_eq!(config.connect.host, "localhost");
        assert_eq!(config.connect.port, 27017);
        assert!(config.connect.load_balanced);
        assert!(config.connect.direct_connection);
        assert_eq!(config.connect.repl_set_name, Some("repl".to_string()));
        assert_eq!(
            config.connect.local_threshold,
            std::time::Duration::from_millis(10)
        );
        assert_eq!(config.connect.mechanism, Some("MongoDbCr".to_string()));
        assert_eq!(config.connect.source, Some("admin".to_string()));
        assert_eq!(config.connect.app_name, Some("appname".to_string()));
        assert_eq!(config.connect.compressors, Some("zstd".to_string()));
        assert!(config.connect.tls);
        assert_eq!(
            config.connect.ca_file_path,
            Some(get_data_dir().join("./file/ca.pem").display().to_string()),
        );
        assert_eq!(
            config.connect.cert_key_file_path,
            Some(get_data_dir().join("./file/cert.pem").display().to_string()),
        );
        assert_eq!(config.task.database, "test_taosx");
        assert_eq!(config.task.collection, "metrics");
        assert_eq!(
            config.task.sql,
            "{\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}"
        );
        assert_eq!(
            config.task.start,
            "2024-07-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            config.task.end,
            Some("2024-08-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert_eq!(config.task.time_zone, "+00:00");
        assert_eq!(config.task.interval, Duration::try_hours(12).unwrap());
        assert_eq!(config.task.delay, Duration::try_seconds(0).unwrap());
        assert_eq!(config.task.sample_data_limit, 4);
    }

    #[test]
    fn test_parse_time_zone() {
        // time_zone exists
        let dsn = Dsn::from_str("mongodb://admin:123456@localhost:27017?database=test_taosx&collection=metrics&sql={}&time_zone=+02:00&start=2021-01-01T00:00:00Z")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.task.time_zone, "+02:00");

        // time_zone doesn't exists, start exists
        let dsn = Dsn::from_str(
            "mongodb://admin:123456@localhost:27017?database=test_taosx&collection=metrics&sql={}&start=2021-01-01T00:00:00+03:00",
        )
        .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.task.time_zone, "+03:00");

        // time_zone doesn't exists, start's time_zone is zero
        let dsn = Dsn::from_str(
            "mongodb://admin:123456@localhost:27017?database=test_taosx&collection=metrics&sql={}&start=2021-01-01T00:00:00Z",
        )
        .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.task.time_zone, "+00:00");
    }

    #[test]
    fn test_parse_subtable_fields() {
        let dsn = Dsn::from_str("mongodb://admin:123456@localhost:27017?database=test_taosx&collection=metrics&subtable_fields=sys_sn,sys_so&sql={}&start=2021-01-01T00:00:00Z")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.task.subtable_fields.len(), 2);
        assert_eq!(
            config.task.subtable_fields.get("sys_sn").unwrap(),
            "\"sys_sn\":${v}"
        );
        assert_eq!(
            config.task.subtable_fields.get("sys_so").unwrap(),
            "\"sys_so\":${v}"
        );

        let dsn = Dsn::from_str("mongodb://admin:123456@localhost:27017?database=test_taosx&collection=metrics&sql={}&start=2021-01-01T00:00:00Z")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        assert_eq!(config.task.subtable_fields.len(), 0);
    }

    #[test]
    fn test_generate_filter() {
        // with type datatime
        let dsn = Dsn::from_str("mongodb://admin:123456@localhost:27017?load_balanced=true&direct_connection=true&repl_set_name=repl&local_threshold=10ms&mechanism=MongoDbCr&source=admin&app_name=appname&compressors=zstd&tls=true&ca_file_path=@./file/ca.pem&cert_key_file_path=@./file/cert.pem&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        let filter = config.task.generate_filter().unwrap();
        dbg!(&filter);

        // with type timestamp
        let dsn = Dsn::from_str("mongodb://admin:123456@localhost:27017?load_balanced=true&direct_connection=true&repl_set_name=repl&local_threshold=10ms&mechanism=MongoDbCr&source=admin&app_name=appname&compressors=zstd&tls=true&ca_file_path=@./file/ca.pem&cert_key_file_path=@./file/cert.pem&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_timestamp},\"$lt\":${end_timestamp}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        let filter = config.task.generate_filter().unwrap();
        dbg!(&filter);
    }

    #[test]
    fn test_generate_sort() {
        let dsn = Dsn::from_str("mongodb://admin:123456@localhost:27017?load_balanced=true&direct_connection=true&repl_set_name=repl&local_threshold=10ms&mechanism=MongoDbCr&source=admin&app_name=appname&compressors=zstd&tls=true&ca_file_path=@./file/ca.pem&cert_key_file_path=@./file/cert.pem&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_timestamp},\"$lt\":${end_timestamp}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4&sort={\"datetime\":1}")
            .unwrap();
        let config = MongoDBConfig::from_dsn(&dsn).unwrap();
        let sort = config.task.generate_sort().unwrap();
        dbg!(&sort);
    }
}
