use std::{fmt::Debug, path::PathBuf, time::Duration};

use chrono::{DateTime, Utc};
use deadpool::managed::Pool;
use taos::{Dsn, TaosBuilder, taos_query::Manager};
use taosx_core::{
    s3::{self, S3Config},
    utils::{self, parse_datetime_in_dsn, parse_duration_in_dsn, parse_key_in_dsn},
};

use crate::{QueryObject, Schema};

#[derive(Clone)]
pub struct Td2LocalContext {
    pub task_id: Option<String>,
    pub raw_from: Dsn,
    pub raw_to: Dsn,
    pub config: Td2LocalConfig,
    pub pool: Option<Pool<Manager<TaosBuilder>>>,
    pub query_obj: Option<QueryObject>,
    pub schema: Option<Schema>,
}

impl Debug for Td2LocalContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Td2LocalContext")
            .field("task_id", &self.task_id)
            .field("raw_from", &self.raw_from.to_string())
            .field("raw_to", &self.raw_to.to_string())
            .field("config", &self.config)
            .finish()
    }
}

impl Td2LocalContext {
    pub fn new(
        task_id: Option<String>,
        raw_from: Dsn,
        raw_to: Dsn,
        config: Td2LocalConfig,
    ) -> Self {
        Self {
            task_id,
            raw_from,
            raw_to,
            config,
            pool: None,
            query_obj: None,
            schema: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Td2LocalConfig {
    // params in --from dsn
    pub upcoming: Option<DateTime<Utc>>,
    pub schema_only: bool,
    #[allow(unused)]
    pub stables: Vec<String>,
    #[allow(unused)]
    pub max_retry: usize,
    #[allow(unused)]
    pub retry_interval: Duration,
    pub concurrency: usize, // 并发数

    // params in --to DSN
    pub backup_dir: PathBuf,                         // 备份文件存放的目录
    pub backup_max_size: u64,                        // 备份文件的最大字节数
    pub backup_comp_level: async_compression::Level, // 备份文件的压缩等级

    #[allow(unused)]
    pub s3: Option<S3Config>,
}

pub struct Td2LocalConfigBuilder {
    task_id: Option<String>,
    from: Dsn,
    to: Dsn,
}

impl Td2LocalConfigBuilder {
    pub fn new(task_id: Option<String>, from: Dsn, to: Dsn) -> Self {
        Self { task_id, from, to }
    }

    pub fn build(&self) -> anyhow::Result<Td2LocalConfig> {
        // stables
        let stables = parse_key_in_dsn::<String>(&self.from, "stables")?
            .map(|s| s.split(",").map(|s| s.trim().to_string()).collect())
            .unwrap_or_default();
        // upcoming
        let upcoming = parse_datetime_in_dsn(&self.from, "upcoming")?;
        // schema_only
        let schema_only = parse_key_in_dsn::<bool>(&self.from, "schema_only")?.unwrap_or(false);
        // max_retry
        let max_retry = parse_key_in_dsn(&self.from, "max_retry")?.unwrap_or(10);
        // retry_interval
        let retry_interval =
            parse_duration_in_dsn(&self.from, "retry_interval")?.unwrap_or(Duration::from_secs(5));
        let concurrency =
            utils::parse_keys_in_dsn::<usize>(&self.from, &["concurrency", "workers"])?.unwrap_or(
                std::thread::available_parallelism()
                    .map(|n| n.get() * 2)
                    .unwrap_or(1),
            );
        // 存备份文件的目录
        let backup_dir = utils::parse_backup_dir(&self.to, self.task_id.as_deref())?;
        // 备份文件的最大字节数，默认1GB
        let backup_max_size = utils::parse_keys_in_dsn::<String>(
            &self.to,
            &["max_size", "backup_max_size", "max.file.size"],
        )?
        .map(|s| utils::parse_bytes(&s))
        .transpose()?
        .unwrap_or(1024 * 1024 * 1024);

        // 备份文件的压缩等级
        let backup_comp_level =
            utils::parse_compression_in_dsn(&self.to, &["compression.level", "compression_level"])?
                .unwrap_or(async_compression::Level::Fastest);
        // s3
        let s3_enable = utils::parse_key_in_dsn::<bool>(&self.to, s3::S3_ENABLE)?.unwrap_or(false);
        let s3 = if s3_enable {
            Some(S3Config::from_dsn(&self.to)?)
        } else {
            None
        };

        Ok(Td2LocalConfig {
            stables,
            upcoming,
            schema_only,
            max_retry,
            retry_interval,
            concurrency,
            backup_dir,
            backup_max_size,
            backup_comp_level,
            s3,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_parse_config() {
        let from = "taos://127.0.0.1:6030/test?stables=stb1,stb2&upcoming=2024-10-01T00:00:00Z&schema_only=true";
        let temp_dir = tempdir().unwrap();
        let to = format!("local:{}", temp_dir.path().display());

        let config = Td2LocalConfigBuilder::new(None, from.parse().unwrap(), to.parse().unwrap())
            .build()
            .unwrap();

        assert_eq!(config.stables.len(), 2);
        assert_eq!(config.stables[0], "stb1");
        assert_eq!(config.stables[1], "stb2");
        assert_eq!(
            config.upcoming,
            Some("2024-10-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert!(config.schema_only);
        assert_eq!(config.max_retry, 10);
        assert_eq!(config.retry_interval, Duration::from_secs(5));
    }
}
