use crate::tmq::generate_hash;
use crate::utils;
use crate::utils::sql::connect_taos_root;
use anyhow::{anyhow, bail, Context};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use taos::taos_query::tmq::VGroupId;
use taos::*;
use tracing::Instrument;

#[derive(Debug, Clone)]
pub struct BackupConfig {
    #[allow(unused)]
    pub task_id: Option<String>,
    raw_from: Dsn,
    #[allow(unused)]
    raw_to: Dsn,
    /// taosd 的版本
    pub server_version: String,
    /// 备份使用的topic名称，由 database，stable 和 创建时间生成
    pub topic: String,
    /// 备份对象：database
    pub database: String,
    /// 备份对象：stable
    pub stable: Option<String>,
    /// 是否自动重复
    pub self_repeat: bool,
    /// 下次执行时间
    pub upcoming: Option<DateTime<Utc>>,
    /// 备份周期
    pub interval: Option<Duration>,
    /// 备份点的生成方式
    pub backup_point_gen_mode: BackupPointGenMode,
    #[allow(dead_code)]
    /// 最大错误重试次数。默认为：10
    pub error_retry_max: u32,
    #[allow(dead_code)]
    /// 错误重试的间隔。默认为 5s。
    pub error_retry_interval: Duration,
    /// 备份文件的存储路径。默认值：$TAOSX_DATA_DIR/backup/$PLAN_ID
    pub backup_dir: PathBuf,
    /// 备份文件移动到的目录
    pub move_to: Option<PathBuf>,
    /// 单个备份文件的最大字节数，默认值为：1 GB
    pub backup_max_size: u64,
    /// 备份文件的压缩等级，默认为 fastest。
    pub backup_comp_level: async_compression::Level,
}

impl BackupConfig {}

impl BackupConfig {
    pub fn group_id(task_id: &Option<String>, from: &Dsn, to: &Dsn) -> String {
        if let Some(oneshot_topic) = from.get("use.topic.name") {
            return oneshot_topic.to_string();
        }

        let mut salt = vec![from.to_string(), to.to_string()];
        if let Some(task_id) = task_id {
            salt.push(task_id.to_string());
        }
        generate_hash(salt)
    }

    /// 如果 topic 在 taosd 中不存在，则是初始备份，反之则不是
    pub async fn is_initial_backup(&self) -> anyhow::Result<bool> {
        let taos = connect_taos_root(&self.raw_from).await?;
        let topics = taos.topics().await?;

        Ok(!topics.iter().any(|t| t.name() == self.topic))
    }

    /// 在 taosd 中创建 topic
    pub async fn create_topic(&self) -> anyhow::Result<()> {
        let sql = self.create_topic_sql();
        tracing::debug!("create topic with sql: {}", sql);

        let taos = connect_taos_root(&self.raw_from).await?;
        taos.exec(&sql).await.map_err(|err| {
            anyhow::Error::from(err).context(format!(
                "failed to create topic: {}, sql: {}",
                &self.topic, sql
            ))
        })?;

        Ok(())
    }

    /// 在本地创建备份目录
    pub async fn create_backup_dir(&self) -> anyhow::Result<()> {
        let dir = self.backup_dir.as_path();

        if !dir.exists() {
            std::fs::create_dir_all(dir).map_err(|err| {
                anyhow::Error::new(err)
                    .context(format!("failed to create backup dir: {}", dir.display()))
            })?;
            tracing::info!("create backup dir: {}", dir.display());
        }

        if !dir.is_dir() {
            bail!("{} is not a valid directory", dir.display());
        }

        Ok(())
    }

    fn create_topic_sql(&self) -> String {
        match self.stable.as_ref() {
            None => {
                format!(
                    "CREATE TOPIC `{}` with meta AS DATABASE `{}`",
                    &self.topic, &self.database
                )
            }
            Some(stable) => {
                format!(
                    "CREATE TOPIC `{}` with meta AS STABLE `{}`.`{}`",
                    &self.topic, &self.database, stable
                )
            }
        }
    }

    pub fn to_tmq_dsn(&self) -> Dsn {
        let mut dsn = Dsn {
            subject: Some(self.topic.clone()),
            ..self.raw_from.clone()
        };
        // 设置 group.id 为 topic
        dsn.params
            .insert("group.id".to_string(), self.topic.clone());
        if self.raw_from.get("auto.offset.reset").is_none() {
            dsn.set("auto.offset.reset", "earliest");
        }
        if self.raw_from.get("experimental.snapshot.enable").is_none() {
            dsn.set("experimental.snapshot.enable", "true");
        }
        if self.raw_from.get("self.repeat").is_some() {
            dsn.remove("self.repeat");
        }

        dsn
    }

    async fn get_vgroups(&self) -> anyhow::Result<usize> {
        let taos = connect_taos_root(&self.raw_from).await?;

        let sql = format!(
            "select `vgroups` from information_schema.ins_databases where name = '{}'",
            self.database
        );

        taos.query_one(&sql)
            .await
            .map_err(|err| {
                anyhow::Error::from(err).context(format!(
                    "failed to query database vgroups with sql: {}",
                    sql
                ))
            })?
            .ok_or(anyhow!("vgroups not found in database: {}", self.database))
    }

    /// 按照 jobs 数量创建 consumer
    pub async fn create_consumer(&self) -> anyhow::Result<Vec<Consumer>> {
        let from = self.to_tmq_dsn();
        tracing::info!("create consumer with dsn: {}", &from);

        // 使用 vgroups 数量创建 consumer
        let jobs = self.get_vgroups().await?;

        let mut handlers = Vec::with_capacity(jobs);
        for id in 0..jobs {
            let tmq = TmqBuilder::from_dsn(&from)?;
            let mut consumer = tmq.build().await.map_err(|err| {
                anyhow::Error::from(err)
                    .context(format!("failed to create consumer with dsn: {}", &from))
            })?;
            let topic = self.topic.clone();
            handlers.push(tokio::spawn(
                async move {
                    // 订阅 topic
                    tracing::info!("consumer {id} subscribe topic: {}", &topic);
                    consumer.subscribe([topic.as_str()]).await.map_err(|err| {
                        anyhow::Error::from(err)
                            .context(format!("failed to subscribe topic: {}", &topic))
                    })?;
                    anyhow::Ok(consumer)
                }
                .in_current_span(),
            ));
        }

        // 等待所有 consumer 创建完成
        let mut consumers = Vec::with_capacity(jobs);
        for h in handlers {
            let consumer = h.await??;
            consumers.push(consumer);
        }

        Ok(consumers)
    }

    /// 解析 dsn 中的备份目录 local:/<BACKUP_DIR>
    pub fn parse_backup_dir(dsn: &Dsn, task_id: Option<&str>) -> anyhow::Result<PathBuf> {
        let dir = utils::parse_dir_in_dsn(dsn, None)?;
        // TODO: 如果 dir 为空，使用 $TAOSX_DATA_DIR/backup

        let mut dir = dir.ok_or(anyhow!("backup dir is None in dsn"))?;

        if let Some(task_id) = task_id {
            dir = dir.join(task_id);
        }

        Ok(dir)
    }

    pub async fn position(
        &self,
        topic: &str,
        vg_id: VGroupId,
    ) -> anyhow::Result<Option<(i64, i64)>> {
        let taos = connect_taos_root(&self.raw_from).await?;

        let sql = format!(
            "SELECT `offset` FROM information_schema.ins_subscriptions WHERE topic_name = '{}' AND consumer_group = '{}' AND vgroup_id = {}",
            topic, topic, vg_id
        );
        tracing::trace!("query with sql: {}", sql);

        let sub: Option<String> = taos.query_one(sql).await?;
        if sub.is_none() {
            return Ok(None);
        }
        let offset = sub.unwrap();
        if !offset.starts_with("wal") {
            return Ok(None);
        }
        let loc = offset.split_once(':').unwrap().1;
        let (current, latest) = loc
            .split_once("/")
            .map(|(a, b)| {
                (
                    a.parse::<i64>().expect("invalid wal offset"),
                    b.parse::<i64>().expect("invalid wal offset"),
                )
            })
            .ok_or_else(|| anyhow!("invalid offset {}", offset))?;
        Ok(Some((current, latest)))
    }
}

/// 备份点生成的方式
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BackupPointGenMode {
    /// 备份计划：通过 tmq 订阅，先获取到 latest offset，当消费 latest offset 后停止，生成备份点，对应备份任务开始的时间戳
    ByOffset,
    /// 手动备份：通过 tmq 订阅，当达到 timeout 没有数据时停止，生成备份点，对应备份任务结束的时间戳
    ByTimeout,
}

impl BackupPointGenMode {
    pub fn try_from_dsn(dsn: impl IntoDsn) -> anyhow::Result<Self> {
        let dsn = dsn.into_dsn()?;
        let upcoming = utils::parse_datetime_in_dsn(&dsn, "upcoming")?;
        match upcoming {
            Some(_) => Ok(Self::ByOffset),
            None => Ok(Self::ByTimeout),
        }
    }
}

pub struct BackupConfigBuilder {
    task_id: Option<String>,
    from: Dsn,
    to: Dsn,
}

impl BackupConfigBuilder {
    pub fn new(task_id: Option<String>, from: &Dsn, to: &Dsn) -> Self {
        Self {
            task_id,
            from: from.clone(),
            to: to.clone(),
        }
    }

    pub async fn build(&self) -> anyhow::Result<BackupConfig> {
        let taos = connect_taos_root(&self.from).await?;

        let server_version = taos
            .server_version()
            .await
            .map_err(|err| anyhow::Error::from(err).context("failed to get server version"))?
            .to_string();

        // database
        let database = Self::parse_database(&self.from)?;
        let dbs = taos.databases().await?;
        if !dbs.iter().any(|db| db.name == database) {
            bail!("database `{}` not exists", database);
        }

        // stable
        let stable = Self::parse_stable(&self.from);
        if let Some(stable) = &stable {
            let sql = format!(
                "select stable_name from information_schema.ins_stables where db_name = '{}'",
                database.as_str()
            );
            tracing::debug!("query with sql: {}", sql);
            let stables: Vec<String> = taos.query(sql).await?.deserialize().try_collect().await?;
            if !stables.contains(stable) {
                bail!("stable `{}` not exists", stable);
            }
        }
        let self_repeat = utils::parse_key_in_dsn(&self.from, "self.repeat")?.unwrap_or(false);

        // upcoming
        let upcoming = utils::parse_datetime_in_dsn(&self.from, "upcoming")?;

        // interval
        let interval = utils::parse_duration_in_dsn(&self.from, "interval")?;
        if let Some(interval) = interval {
            // if interval < Duration::from_secs(10 * 60) {
            //     bail!("interval must be greater than 10 minutes");
            // }
            let sql = format!("SELECT `wal_retention_period` FROM information_schema.ins_databases WHERE name = '{}'", &database);
            tracing::debug!("query with sql: {}", sql);
            let wal_retention_period: u64 = taos.query_one(sql).await?.unwrap();
            if interval.as_secs() >= wal_retention_period {
                bail!(
                    "interval must be less than wal_retention_period: {}",
                    wal_retention_period
                );
            }
        }

        // backup_point_gen_mode
        let backup_point_gen_mode = BackupPointGenMode::try_from_dsn(&self.from)
            .context("failed to parse backup point generate mode")?;

        // backup dir
        let backup_dir = BackupConfig::parse_backup_dir(&self.to, self.task_id.as_deref())?;

        // topic 与 group.id 相同
        let topic = BackupConfig::group_id(&self.task_id, &self.from, &self.to);

        // error.retry.max
        let error_retry_max =
            utils::parse_keys_in_dsn::<u32>(&self.from, &["max_retry", "error.max.retry"])?
                .unwrap_or(10);

        // error.retry.interval
        let error_retry_interval = utils::parse_duration_in_dsn(&self.from, "retry_interval")?
            .unwrap_or(Duration::from_secs(5));

        // move_to
        let move_to = utils::parse_dir_in_dsn(&self.to, Some("move.to"))?;

        // backup_max_size
        let backup_max_size = utils::parse_keys_in_dsn::<String>(
            &self.to,
            &["max_size", "backup_max_size", "max.file.size"],
        )?
        .map(|s| utils::parse_bytes(&s))
        .transpose()?
        .unwrap_or(1024 * 1024 * 1024);

        // backup_comp_level
        let backup_comp_level =
            Self::parse_compression_level(&self.to)?.unwrap_or(async_compression::Level::Fastest);

        Ok(BackupConfig {
            task_id: self.task_id.clone(),
            raw_from: self.from.clone(),
            raw_to: self.to.clone(),
            server_version,
            topic,
            database,
            stable,
            self_repeat,
            upcoming,
            interval,
            backup_point_gen_mode,
            error_retry_max,
            error_retry_interval,
            backup_dir,
            move_to,
            backup_max_size,
            backup_comp_level,
        })
    }

    /// 从 dsn 中解析 database 参数，database 是必须的
    fn parse_database(from: &Dsn) -> anyhow::Result<String> {
        from.subject
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned()
            .ok_or_else(|| anyhow!("database is required"))
    }

    /// 从 dsn 中解析 stable 参数，如果 stable 为 * 或者为空，则返回 None
    fn parse_stable(from: &Dsn) -> Option<String> {
        from.get("stable").and_then(|s| {
            if s.is_empty() || s == "*" {
                return None;
            }
            Some(s.to_string())
        })
    }

    /// 解析 dsn 中的压缩等级参数
    fn parse_compression_level(dsn: &Dsn) -> anyhow::Result<Option<async_compression::Level>> {
        utils::parse_keys_in_dsn::<String>(dsn, &["compression.level", "compression_level"])?
            .map(|s| {
                let level = s.to_lowercase();
                match level.as_str() {
                    "fastest" => Ok(async_compression::Level::Fastest),
                    "best" => Ok(async_compression::Level::Best),
                    "default" | "balanced" => Ok(async_compression::Level::Default),
                    _ => level
                        .parse::<i32>()
                        .map_err(|err| {
                            anyhow::Error::from(err)
                                .context(format!("invalid compression level: {s}"))
                        })
                        .map(async_compression::Level::Precise),
                }
            })
            .transpose()
    }

    /// 解析 dsn 中的 move.to 参数
    pub fn parse_directory_param(dsn: &Dsn, param_key: &str) -> anyhow::Result<Option<PathBuf>> {
        dsn.get(param_key)
            .filter(|s| !s.is_empty())
            .map(|s| {
                Path::new(s).canonicalize().map_err(|err| {
                    anyhow::Error::new(err).context(format!("invalid {param_key}: {s}"))
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试解析备份文件的压缩等级
    /// 测试用例：
    /// 1. compression.level=fastest
    /// 2. compression.level=best
    /// 3. compression.level=default
    /// 4. compression.level=balanced
    /// 5. compression.level=5
    /// 6. compression.level=
    /// 7. compression.level=abc
    /// 8. 不包含 compression.level
    #[test]
    fn test_parse_compression_level() {
        let dsn = "local:/tmp?compression.level=fastest".into_dsn().unwrap();
        let level = BackupConfigBuilder::parse_compression_level(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!("Fastest", format!("{:?}", level));

        let dsn = "local:/tmp?compression.level=best".into_dsn().unwrap();
        let level = BackupConfigBuilder::parse_compression_level(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!("Best", format!("{:?}", level));

        let dsn = "local:/tmp?compression.level=default".into_dsn().unwrap();
        let level = BackupConfigBuilder::parse_compression_level(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!("Default", format!("{:?}", level));

        let dsn = "local:/tmp?compression.level=balanced".into_dsn().unwrap();
        let level = BackupConfigBuilder::parse_compression_level(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!("Default", format!("{:?}", level));

        let dsn = "local:/tmp?compression.level=5".into_dsn().unwrap();
        let level = BackupConfigBuilder::parse_compression_level(&dsn)
            .unwrap()
            .unwrap();
        assert_eq!("Precise(5)", format!("{:?}", level));

        let dsn = "local:/tmp".into_dsn().unwrap();
        let level = BackupConfigBuilder::parse_compression_level(&dsn).unwrap();
        assert!(level.is_none());

        let dsn = "local:/tmp?compression.level=".into_dsn().unwrap();
        let level = BackupConfigBuilder::parse_compression_level(&dsn).unwrap();
        assert!(level.is_none());

        let dsn = "local:/tmp?compression.level=abc".into_dsn().unwrap();
        let level = BackupConfigBuilder::parse_compression_level(&dsn);
        assert!(level.is_err());
        assert_eq!(
            "invalid compression level: abc",
            format!("{}", level.err().unwrap())
        );
    }

    #[test]
    fn test_parse_backup_dir() {
        let dsn = "local:/tmp".into_dsn().unwrap();
        let task_id = Some("123".to_string());
        let backup_dir = BackupConfig::parse_backup_dir(&dsn, task_id.as_deref()).unwrap();

        let cur_dir = Path::new("/tmp").canonicalize().unwrap().join("123");
        assert_eq!(backup_dir, cur_dir);
    }

    #[test]
    fn test_from_dsn_of_backup_point_gen_mode() {
        let dsn = "tmq://".into_dsn().unwrap();
        let mode = BackupPointGenMode::try_from_dsn(dsn).unwrap();
        assert_eq!(mode, BackupPointGenMode::ByTimeout);

        let dsn = "tmq://?timeout=10min".into_dsn().unwrap();
        let mode = BackupPointGenMode::try_from_dsn(dsn).unwrap();
        assert_eq!(mode, BackupPointGenMode::ByTimeout);

        let dsn = "tmq://?upcoming=2021-10-01T00:00:00Z".into_dsn().unwrap();
        let mode = BackupPointGenMode::try_from_dsn(dsn).unwrap();
        assert_eq!(mode, BackupPointGenMode::ByOffset);
    }
}
