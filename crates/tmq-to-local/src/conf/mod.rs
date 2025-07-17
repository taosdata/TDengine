use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use taos::taos_query::tmq::VGroupId;
use taos::*;
use taosx_core::s3::{S3_ENABLE, S3Config};
use taosx_core::tmq::generate_hash;
use taosx_core::utils::constants::VERSION_3_3_6;
use taosx_core::utils::sql::connect_taos_root;
use taosx_core::{get_data_dir, utils};
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
    /// 是否开启 S3 转储，默认为 false
    pub s3_enable: bool,
    /// S3 配置
    pub s3_config: Option<S3Config>,
    /// 本地备份的保留时间，所有早于now - backup_retention_period的文件都需要上传，默认为 0
    pub backup_retention_period: Option<Duration>,
    /// 本地备份文件的保留个数，本地只保留最新的backup_retention_size个备份文件，默认为 0
    pub backup_retention_size: Option<u64>,
}

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

    pub async fn to_tmq_dsn(&self) -> anyhow::Result<Dsn> {
        let mut dsn = Dsn {
            subject: Some(self.topic.clone()),
            ..self.raw_from.clone()
        };
        // 设置 group.id 为 topic
        dsn.params
            .insert("group.id".to_string(), self.topic.clone());
        // 默认从最早的 offset 开始消费
        if self.raw_from.get("auto.offset.reset").is_none() {
            dsn.set("auto.offset.reset", "earliest");
        }
        // 默认开始从 TSDB 快照开始消费
        if self.raw_from.get("experimental.snapshot.enable").is_none() {
            dsn.set("experimental.snapshot.enable", "true");
        }
        // tmq 不接受自定义的参数，删除 self.repeat
        if self.raw_from.get("self.repeat").is_some() {
            dsn.remove("self.repeat");
        }

        // 以下参数是创建备份计划时的参数，不可以传递给 tmq
        dsn.remove("stable");
        dsn.remove("upcoming");
        dsn.remove("interval");
        dsn.remove("max_retry");
        dsn.remove("retry_interval");
        dsn.remove("use.topic.name");

        // 如果是 ws 协议，则默认启用压缩
        if let Some(protocol) = dsn.protocol.as_ref() {
            if protocol == "ws" || protocol == "wss" || protocol == "http" || protocol == "https" {
                // 默认启用压缩
                if dsn.get("compression").is_none() {
                    dsn.set("compression", "true");
                }
            }
        }

        let version = semver::Version::parse(&self.server_version.split('.').take(3).join("."))
            .context(format!("invalid server version: {}", &self.server_version))?;
        if version < VERSION_3_3_6 && dsn.get("msg.consume.rawdata").is_some() {
            bail!("msg.consume.rawdata is not supported in server version < 3.3.6");
        }

        Ok(dsn)
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
        let from = self.to_tmq_dsn().await.context("failed to build tmq dsn")?;
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
        let mut dir = match utils::parse_dir_in_dsn(dsn, None)? {
            // dir 为空，使用默认路径: $TAOSX_DATA_DIR/backup
            None => {
                let default_dir = get_data_dir().join("backup");
                // 如果 $TAOSX_DATA_DIR/backup 不存在，则创建
                if !default_dir.exists() {
                    std::fs::create_dir_all(&default_dir).map_err(|err| {
                        anyhow::Error::new(err).context(format!(
                            "failed to create backup dir: {}",
                            default_dir.display()
                        ))
                    })?;
                    tracing::info!("create backup dir: {}", default_dir.display());
                }
                default_dir
            }
            // 用户指定的 dir
            Some(dir) => {
                // 如果 dir 不存在，则报错
                if !dir.exists() {
                    bail!("backup dir not exists: {}", dir.display());
                }
                dir
            }
        };

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

        // self.repeat
        let self_repeat = utils::parse_key_in_dsn(&self.from, "self.repeat")?.unwrap_or(false);

        // upcoming
        let upcoming = utils::parse_datetime_in_dsn(&self.from, "upcoming")?;

        // interval
        let interval = utils::parse_duration_in_dsn(&self.from, "interval")?;
        if let Some(interval) = interval {
            // if interval < Duration::from_secs(10 * 60) {
            //     bail!("interval must be greater than 10 minutes");
            // }
            let sql = format!(
                "SELECT `wal_retention_period` FROM information_schema.ins_databases WHERE name = '{}'",
                &database
            );
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
        // s3_enable
        let s3_enable = utils::parse_key_in_dsn::<bool>(&self.to, S3_ENABLE)?.unwrap_or(false);

        let (s3_config, backup_retention_period, backup_retention_size) = if s3_enable {
            // s3 config
            let s3_config = S3Config::from_dsn(&self.to)?;
            // 检查 s3 连通性
            s3_config
                .connect()
                .await
                .context(format!("failed to connect s3: {:?}", &s3_config))?;
            // backup_retention_period
            let backup_retention_period =
                utils::parse_duration_in_dsn(&self.to, "backup_retention_period")?;
            // backup_retention_size
            let backup_retention_size =
                utils::parse_key_in_dsn::<u64>(&self.to, "backup_retention_size")?
                    .and_then(|s| if s == 0 { None } else { Some(s) });
            (
                Some(s3_config),
                backup_retention_period,
                backup_retention_size,
            )
        } else {
            (None, None, None)
        };

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
            s3_enable,
            s3_config,
            backup_retention_period,
            backup_retention_size,
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
    use std::env;

    /// 创建一个备份任务所需要的最少的配置参数：
    /// from: 数据库名称，且通过连接信息可以连接到 taosd
    /// to: 备份文件的存储路径，且路径存在
    #[tokio::test]
    async fn test_backup_config_builder_with_taos() {
        let from = format!(
            "{}/log",
            env::var("TAOS_ADDR").unwrap_or("taos://".to_string())
        )
        .into_dsn()
        .unwrap();
        let to = "local:/tmp".into_dsn().unwrap();

        let config = BackupConfigBuilder::new(None, &from, &to)
            .build()
            .await
            .unwrap();

        assert_eq!(config.task_id, None);
        assert_eq!(config.database, "log");
        assert_eq!(config.backup_dir, Path::new("/tmp").canonicalize().unwrap());
    }

    #[tokio::test]
    async fn test_backup_config_to_tmq_dsn_remove_items() {
        let config = BackupConfig {
            raw_from:
                "tmq://host:6030/db?retry_interval=1&interval=2&max_retry=3&upcoming=2025-12-12"
                    .into_dsn()
                    .unwrap(),
            topic: "abc".to_string(),
            task_id: None,
            raw_to: Default::default(),
            server_version: "3.3.3.3".to_string(),
            database: Default::default(),
            stable: None,
            self_repeat: false,
            upcoming: None,
            interval: None,
            backup_point_gen_mode: BackupPointGenMode::ByOffset,
            error_retry_max: 0,
            error_retry_interval: Default::default(),
            backup_dir: Default::default(),
            move_to: None,
            backup_max_size: 0,
            backup_comp_level: async_compression::Level::Fastest,
            s3_enable: false,
            s3_config: None,
            backup_retention_period: None,
            backup_retention_size: None,
        };

        assert!(config.raw_from.get("retry_interval").is_some());
        assert!(config.raw_from.get("interval").is_some());
        assert!(config.raw_from.get("max_retry").is_some());
        assert!(config.raw_from.get("upcoming").is_some());
        // when
        let dsn = config.to_tmq_dsn().await.unwrap();
        assert!(dsn.get("retry_interval").is_none());
        assert!(dsn.get("interval").is_none());
        assert!(dsn.get("max_retry").is_none());
        assert!(dsn.get("upcoming").is_none());
    }

    #[tokio::test]
    async fn test_backup_config_to_tmq_dsn() {
        // 使用 tmq，不设置 compression 参数
        let config = BackupConfig {
            raw_from: "tmq://host:6030/db".into_dsn().unwrap(),
            topic: "abc".to_string(),
            task_id: None,
            raw_to: Default::default(),
            server_version: "3.3.3.3".to_string(),
            database: Default::default(),
            stable: None,
            self_repeat: false,
            upcoming: None,
            interval: None,
            backup_point_gen_mode: BackupPointGenMode::ByOffset,
            error_retry_max: 0,
            error_retry_interval: Default::default(),
            backup_dir: Default::default(),
            move_to: None,
            backup_max_size: 0,
            backup_comp_level: async_compression::Level::Fastest,
            s3_enable: false,
            s3_config: None,
            backup_retention_period: None,
            backup_retention_size: None,
        };
        // when
        let dsn = config.to_tmq_dsn().await.unwrap();
        // then
        assert_eq!("abc", dsn.get("group.id").unwrap());
        assert_eq!("earliest", dsn.get("auto.offset.reset").unwrap());
        assert_eq!("true", dsn.get("experimental.snapshot.enable").unwrap());
        assert!(dsn.get("compression").is_none());

        // 使用 ws 协议，默认启用压缩
        let config = BackupConfig {
            raw_from: "tmq+ws://host:6041/db".into_dsn().unwrap(),
            topic: "abc".to_string(),
            task_id: None,
            raw_to: Default::default(),
            server_version: "3.3.3.3".to_string(),
            database: Default::default(),
            stable: None,
            self_repeat: false,
            upcoming: None,
            interval: None,
            backup_point_gen_mode: BackupPointGenMode::ByOffset,
            error_retry_max: 0,
            error_retry_interval: Default::default(),
            backup_dir: Default::default(),
            move_to: None,
            backup_max_size: 0,
            backup_comp_level: async_compression::Level::Fastest,
            s3_enable: false,
            s3_config: None,
            backup_retention_period: None,
            backup_retention_size: None,
        };
        // when
        let dsn = config.to_tmq_dsn().await.unwrap();
        // then
        assert_eq!("abc", dsn.get("group.id").unwrap());
        assert_eq!("earliest", dsn.get("auto.offset.reset").unwrap());
        assert_eq!("true", dsn.get("experimental.snapshot.enable").unwrap());
        assert_eq!("true", dsn.get("compression").unwrap());

        // 使用 http 协议，默认启用压缩
        let config = BackupConfig {
            raw_from: "tmq+http://host:6041/db".into_dsn().unwrap(),
            topic: "abc".to_string(),
            task_id: None,
            raw_to: Default::default(),
            server_version: "3.3.3.3".to_string(),
            database: Default::default(),
            stable: None,
            self_repeat: false,
            upcoming: None,
            interval: None,
            backup_point_gen_mode: BackupPointGenMode::ByOffset,
            error_retry_max: 0,
            error_retry_interval: Default::default(),
            backup_dir: Default::default(),
            move_to: None,
            backup_max_size: 0,
            backup_comp_level: async_compression::Level::Fastest,
            s3_enable: false,
            s3_config: None,
            backup_retention_period: None,
            backup_retention_size: None,
        };
        // when
        let dsn = config.to_tmq_dsn().await.unwrap();
        // then
        assert_eq!("abc", dsn.get("group.id").unwrap());
        assert_eq!("earliest", dsn.get("auto.offset.reset").unwrap());
        assert_eq!("true", dsn.get("experimental.snapshot.enable").unwrap());
        assert_eq!("true", dsn.get("compression").unwrap());

        // 使用 wss 协议，默认启用压缩
        let config = BackupConfig {
            raw_from: "tmq+wss://host:6041/db".into_dsn().unwrap(),
            topic: "abc".to_string(),
            task_id: None,
            raw_to: Default::default(),
            server_version: "3.3.3.3".to_string(),
            database: Default::default(),
            stable: None,
            self_repeat: false,
            upcoming: None,
            interval: None,
            backup_point_gen_mode: BackupPointGenMode::ByOffset,
            error_retry_max: 0,
            error_retry_interval: Default::default(),
            backup_dir: Default::default(),
            move_to: None,
            backup_max_size: 0,
            backup_comp_level: async_compression::Level::Fastest,
            s3_enable: false,
            s3_config: None,
            backup_retention_period: None,
            backup_retention_size: None,
        };
        // when
        let dsn = config.to_tmq_dsn().await.unwrap();
        // then
        assert_eq!("abc", dsn.get("group.id").unwrap());
        assert_eq!("earliest", dsn.get("auto.offset.reset").unwrap());
        assert_eq!("true", dsn.get("experimental.snapshot.enable").unwrap());
        assert_eq!("true", dsn.get("compression").unwrap());

        // 使用 https 协议，默认启用压缩
        let config = BackupConfig {
            raw_from: "tmq+https://host:6041/db".into_dsn().unwrap(),
            topic: "abc".to_string(),
            task_id: None,
            raw_to: Default::default(),
            server_version: "3.3.3.3".to_string(),
            database: Default::default(),
            stable: None,
            self_repeat: false,
            upcoming: None,
            interval: None,
            backup_point_gen_mode: BackupPointGenMode::ByOffset,
            error_retry_max: 0,
            error_retry_interval: Default::default(),
            backup_dir: Default::default(),
            move_to: None,
            backup_max_size: 0,
            backup_comp_level: async_compression::Level::Fastest,
            s3_enable: false,
            s3_config: None,
            backup_retention_period: None,
            backup_retention_size: None,
        };
        // when
        let dsn = config.to_tmq_dsn().await.unwrap();
        // then
        assert_eq!("abc", dsn.get("group.id").unwrap());
        assert_eq!("earliest", dsn.get("auto.offset.reset").unwrap());
        assert_eq!("true", dsn.get("experimental.snapshot.enable").unwrap());
        assert_eq!("true", dsn.get("compression").unwrap());

        // 使用 ws 协议，不启用压缩
        let config = BackupConfig {
            raw_from: "tmq+ws://host:6041/db?compression=false"
                .into_dsn()
                .unwrap(),
            topic: "abc".to_string(),
            task_id: None,
            raw_to: Default::default(),
            server_version: "3.3.3.3".to_string(),
            database: Default::default(),
            stable: None,
            self_repeat: false,
            upcoming: None,
            interval: None,
            backup_point_gen_mode: BackupPointGenMode::ByOffset,
            error_retry_max: 0,
            error_retry_interval: Default::default(),
            backup_dir: Default::default(),
            move_to: None,
            backup_max_size: 0,
            backup_comp_level: async_compression::Level::Fastest,
            s3_enable: false,
            s3_config: None,
            backup_retention_period: None,
            backup_retention_size: None,
        };
        // when
        let dsn = config.to_tmq_dsn().await.unwrap();
        // then
        assert_eq!("abc", dsn.get("group.id").unwrap());
        assert_eq!("earliest", dsn.get("auto.offset.reset").unwrap());
        assert_eq!("true", dsn.get("experimental.snapshot.enable").unwrap());
        assert_eq!("false", dsn.get("compression").unwrap());
    }

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
