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
    pub task_job_id: Option<(i64, i64)>,
    pub raw_from: Dsn,
    pub raw_to: Dsn,
    pub config: Td2LocalConfig,
    pub source_pool: Option<Pool<Manager<TaosBuilder>>>,
    pub server_version: Option<String>,
    pub query_obj: Option<QueryObject>,
    pub schema: Option<Schema>,
}

impl Debug for Td2LocalContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Td2LocalContext")
            .field("task_job_id", &self.task_job_id)
            .field("raw_from", &self.raw_from.to_string())
            .field("raw_to", &self.raw_to.to_string())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct Td2LocalConfig {
    // params in --from dsn
    pub upcoming: Option<DateTime<Utc>>, // 任务预定开始时间
    pub schema_only: bool,               // 是否只备份 schema
    pub stables: Vec<String>,            // 指定要备份的超级表列表
    pub start: Option<DateTime<Utc>>,    // 开始时间
    pub end: Option<DateTime<Utc>>,      // 结束时间
    pub max_retry: usize,                // 最大重试次数
    pub retry_interval: Duration,        // 重试间隔
    pub concurrency: usize,              // 并发工作线程数
    // params in --to DSN
    pub backup_dir: PathBuf,                         // 备份文件存放的目录
    pub backup_max_size: u64,                        // 备份文件的最大字节数
    pub backup_comp_level: async_compression::Level, // 备份文件的压缩等级
    pub pretty: bool,                                // 备份的 schema 是否pretty
    #[allow(unused)]
    pub s3: Option<S3Config>,    // 如果配置了 s3，则备份文件上传到 s3
}

pub struct Td2LocalConfigBuilder {
    task_job_id: Option<(i64, i64)>,
    from: Dsn,
    to: Dsn,
}

impl Td2LocalConfigBuilder {
    pub fn new(task_job_id: Option<(i64, i64)>, from: &Dsn, to: &Dsn) -> Self {
        Self {
            task_job_id,
            from: from.clone(),
            to: to.clone(),
        }
    }

    pub fn build(&self) -> anyhow::Result<Td2LocalConfig> {
        // upcoming
        let upcoming = parse_datetime_in_dsn(&self.from, "upcoming")?;
        // schema_only
        let schema_only = parse_key_in_dsn::<bool>(&self.from, "schema_only")?.unwrap_or(false);
        // stables
        let stables = parse_key_in_dsn::<String>(&self.from, "stables")?
            .map(|s| s.split(",").map(|s| s.trim().to_string()).collect())
            .unwrap_or_default();
        // start
        let start = parse_datetime_in_dsn(&self.from, "start")?;
        // end
        let end = parse_datetime_in_dsn(&self.from, "end")?;
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
        let backup_dir = utils::parse_backup_dir(&self.to, self.task_job_id)?;
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

        // schema 是否 pretty
        let pretty = self
            .to
            .get("pretty")
            .map(|s| match s.as_str() {
                "" | "1" | "true" | "TRUE" | "yes" | "YES" => true,
                "0" | "false" | "FALSE" | "no" | "NO" => false,
                other => {
                    tracing::warn!("invalid value for pretty: {}, use default false", other);
                    false
                }
            })
            .unwrap_or(false);

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
            start,
            end,
            max_retry,
            retry_interval,
            concurrency,
            backup_dir,
            backup_max_size,
            backup_comp_level,
            pretty,
            s3,
        })
    }
}

#[cfg(test)]
mod tests {
    use taos::IntoDsn;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_parse_config() {
        // given
        let from = "taos://127.0.0.1:6030/test?stables=stb1,stb2&upcoming=2024-10-01T00:00:00Z&schema_only=true".into_dsn().unwrap();
        let temp_dir = tempdir().unwrap();
        let to = format!("local:{}", temp_dir.path().display())
            .into_dsn()
            .unwrap();

        // when
        let config = Td2LocalConfigBuilder::new(None, &from, &to)
            .build()
            .unwrap();

        // then
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
