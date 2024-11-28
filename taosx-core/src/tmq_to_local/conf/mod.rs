use crate::tmq::generate_hash;
use anyhow::{anyhow, bail};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use taos::*;

#[derive(Debug, Clone)]
pub struct BackupConfig {
    #[allow(dead_code)]
    task_id: Option<String>,
    raw_from: Dsn,
    #[allow(dead_code)]
    raw_to: Dsn,
    /// taosd 的版本
    pub server_version: String,
    /// 备份使用的topic名称，由 database，stable 和 创建时间生成
    pub topic: String,
    /// 备份对象：database
    pub database: String,
    /// 备份对象：stable
    pub stable: Option<String>,
    /// 下次执行时间
    pub upcoming: Option<DateTime<Utc>>,
    /// 备份点的生成方式
    pub backup_point_gen_mode: BackupPointGenMode,
    #[allow(dead_code)]
    /// 备份执行的间隔
    pub interval: Option<Duration>,
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
    /// 是否是初始备份, 如果 topic 在 taosd 中不存在，则是初始备份，反之则不是
    pub async fn is_initial_backup(&self) -> anyhow::Result<bool> {
        let taos = connect(&self.raw_from).await?;
        let topics = taos.topics().await?;
        let t = topics.iter().find(|t| t.name() == self.topic).is_none();
        Ok(t)
    }

    /// 在 taosd 中创建 topic
    pub async fn create_topic(&self) -> anyhow::Result<()> {
        let sql = self.create_topic_sql();

        let taos = connect(&self.raw_from).await?;

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

        dsn.params
            .insert("group.id".to_string(), self.topic.clone());

        dsn
    }

    async fn get_vgroups(&self) -> anyhow::Result<usize> {
        let taos = connect(&self.raw_from).await?;

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
    pub async fn create_consumer(&self) -> anyhow::Result<Vec<BackupConsumer>> {
        let from = self.to_tmq_dsn();

        // 按照 vgroups 的数量，创建 consumer 并发起订阅
        let vgroups = self.get_vgroups().await?;
        let mut handlers = Vec::with_capacity(vgroups);
        for id in 0..vgroups {
            let tmq = TmqBuilder::from_dsn(&from)?;
            let mut consumer = tmq.build().await.map_err(|err| {
                anyhow::Error::from(err)
                    .context(format!("failed to create consumer with dsn: {}", &from))
            })?;
            let topic = self.topic.clone();
            handlers.push(tokio::spawn(async move {
                // 订阅 topic
                tracing::debug!("Subscribe consumer {id}");
                consumer.subscribe([topic.as_str()]).await.map_err(|err| {
                    anyhow::Error::from(err)
                        .context(format!("failed to subscribe topic: {}", &topic))
                })?;
                anyhow::Ok(consumer)
            }));
        }

        // 等待所有 consumer 创建完成，并检查 assignment
        let mut consumers = Vec::with_capacity(vgroups);
        for (idx, h) in handlers.into_iter().enumerate() {
            let consumer = h.await??;

            // check assignments
            let assign = consumer.assignments().await;
            if assign.is_none() {
                tracing::warn!("consumer {} no assignments", idx);
                continue;
            }
            let assign = assign.unwrap();
            let (topic, assign) = assign.first().unwrap();
            assert_eq!(assign.len(), 1);
            let assign = assign.first().unwrap();
            consumers.push(BackupConsumer {
                topic: topic.clone(),
                vgroup_id: assign.vgroup_id(),
                begin_offset: assign.begin(),
                end_offset: assign.end(),
                current_offset: assign.current_offset(),
                consumer,
            });
        }

        Ok(consumers)
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
        let upcoming = BackupConfigBuilder::parse_upcoming(&dsn)?;
        match upcoming {
            Some(_) => Ok(Self::ByOffset),
            None => Ok(Self::ByTimeout),
        }
    }
}

#[derive(Debug)]
pub struct BackupConsumer {
    pub topic: String,
    pub vgroup_id: i32,
    #[allow(unused)]
    pub begin_offset: i64,
    pub end_offset: i64,
    #[allow(unused)]
    pub current_offset: i64,
    pub consumer: Consumer,
}

/// 通过 dsn 连接 taosd 且不指定 database 和任何参数
async fn connect(dsn: &Dsn) -> anyhow::Result<Taos> {
    let from_cloned = Dsn {
        subject: None,
        params: BTreeMap::new(),
        ..dsn.clone()
    };

    let taos = TaosBuilder::from_dsn(&from_cloned)?
        .build()
        .await
        .map_err(|err| {
            anyhow::Error::from(err)
                .context(format!("failed to connect taos with dsn: {}", from_cloned))
        })?;

    Ok(taos)
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
        let taos = connect(&self.from).await?;

        let server_version = taos
            .server_version()
            .await
            .map_err(|err| anyhow::Error::from(err).context("failed to get server version"))?
            .to_string();

        // database
        let database = Self::parse_database(&self.from)?;
        let dbs = taos.databases().await?;
        if dbs.iter().find(|db| db.name == database).is_none() {
            bail!("database `{}` not exists", database);
        }

        // stable
        let stable = Self::parse_stable(&self.from);
        if let Some(stable) = &stable {
            let sql = format!(
                "select stable_name from information_schema.ins_stables where db_name = '{}'",
                database.as_str()
            );
            let stables: Vec<String> = taos.query(sql).await?.deserialize().try_collect().await?;
            if !stables.contains(&stable) {
                bail!("stable `{}` not exists", stable);
            }
        }

        let interval = Self::parse_duration_in_dsn(&self.from, "interval")?;
        // TODO: 如果 interval >= WAL_RETENTION_PERIOD 报错，可能会造成数据丢失

        let mut salt = vec![self.from.to_string(), self.to.to_string()];
        if let Some(task_id) = &self.task_id {
            salt.push(task_id.to_string());
        }
        let topic = generate_hash(salt);

        Ok(BackupConfig {
            task_id: self.task_id.clone(),
            raw_from: self.from.clone(),
            raw_to: self.to.clone(),
            server_version,
            topic,
            database,
            stable,
            upcoming: Self::parse_upcoming(&self.from)?,
            backup_point_gen_mode: BackupPointGenMode::try_from_dsn(&self.from).map_err(|err| {
                anyhow::Error::from(err).context("failed to parse backup point generate mode")
            })?,
            interval,
            error_retry_max: Self::parse_max_retry(&self.from)?.unwrap_or(10),
            error_retry_interval: Self::parse_duration_in_dsn(&self.from, "retry_interval")?
                .unwrap_or(Duration::from_secs(5)),
            backup_dir: Self::parse_backup_dir(&self.to)?,
            move_to: Self::parse_directory_param(&self.to, "move.to")?,
            backup_max_size: Self::parse_backup_max_size(&self.to)?.unwrap_or(1024 * 1024 * 1024),
            backup_comp_level: Self::parse_compression_level(&self.to)?
                .unwrap_or(async_compression::Level::Fastest),
        })
    }

    /// 从 dsn 中解析 database 参数，database 是必须的
    fn parse_database(from: &Dsn) -> anyhow::Result<String> {
        from.subject
            .as_ref()
            .filter(|s| !s.is_empty())
            .map(|s| s.clone())
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

    /// 从 dsn 中解析出 upcoming 时间，upcoming 是一个符合 rfc3339 格式的时间字符串
    fn parse_upcoming(dsn: &Dsn) -> anyhow::Result<Option<DateTime<Utc>>> {
        dsn.get("upcoming")
            .filter(|s| !s.is_empty())
            .map(|s| {
                DateTime::parse_from_rfc3339(s)
                    .map_err(|err| {
                        anyhow::Error::from(err).context(format!("invalid upcoming: {s}"))
                    })
                    .map(|dt| dt.with_timezone(&Utc))
            })
            .transpose()
    }

    /// 解析 dsn 中的 interval 参数，interval 是一个 Duraiton
    fn parse_duration_in_dsn(dsn: &Dsn, key: &str) -> anyhow::Result<Option<Duration>> {
        dsn.get(key)
            .filter(|s| !s.is_empty())
            .map(|s| {
                fundu::parse_duration(s.as_str())
                    .map_err(|err| anyhow::Error::from(err).context(format!("invalid {key}: {s}")))
            })
            .transpose()
    }

    /// 解析 dsn 中的 max_retry
    fn parse_max_retry(dsn: &Dsn) -> anyhow::Result<Option<u32>> {
        dsn.get("max_retry")
            .or_else(|| dsn.get("error.max.retry"))
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<u32>().map_err(|err| {
                    anyhow::Error::from(err).context(format!("invalid max_retry: {s}"))
                })
            })
            .transpose()
    }

    /// 解析 dsn 中的压缩等级参数
    fn parse_compression_level(dsn: &Dsn) -> anyhow::Result<Option<async_compression::Level>> {
        dsn.get("compression.level")
            .or_else(|| dsn.get("compression_level"))
            .filter(|s| !s.is_empty())
            .map(|s| {
                let level = s.to_lowercase();
                match level.as_str() {
                    "fastest" => Ok(async_compression::Level::Fastest),
                    "best" => Ok(async_compression::Level::Best),
                    "default" => Ok(async_compression::Level::Default),
                    _ => level
                        .parse::<i32>()
                        .map_err(|err| {
                            anyhow::Error::from(err)
                                .context(format!("invalid compression level: {s}"))
                        })
                        .map(|l| async_compression::Level::Precise(l)),
                }
            })
            .transpose()
    }

    /// 解析 dsn 中的备份目录 local:/<BACKUP_DIR>
    fn parse_backup_dir(dsn: &Dsn) -> anyhow::Result<PathBuf> {
        let p = dsn
            .path
            .as_ref()
            .ok_or(anyhow::anyhow!("backup dir is required"))?;

        let dir = Path::new(p)
            .canonicalize()
            .map_err(|err| anyhow::Error::new(err).context(format!("invalid backup dir: {p}")))?;

        Ok(dir)
    }

    /// 解析 dsn 中的备份文件最大字节数，默认为 1G
    fn parse_backup_max_size(dsn: &Dsn) -> anyhow::Result<Option<u64>> {
        dsn.get("backup_max_size")
            .or_else(|| dsn.get("max.file.size"))
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<u64>().map_err(|err| {
                    anyhow::Error::from(err).context(format!("invalid backup file size: {s}"))
                })
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
    use chrono::Local;

    #[test]
    fn test_parse_upcoming() {
        let now = Utc::now();
        let dsn = format!("tmq://?upcoming={}", now.to_rfc3339())
            .into_dsn()
            .unwrap();
        let upcoming = BackupConfigBuilder::parse_upcoming(&dsn).unwrap().unwrap();
        assert_eq!(upcoming, now);

        let now = Local::now();
        let dsn = format!("tmq://?upcoming={}", now.to_rfc3339())
            .into_dsn()
            .unwrap();
        let upcoming = BackupConfigBuilder::parse_upcoming(&dsn).unwrap().unwrap();
        assert_eq!(upcoming, now.with_timezone(&Utc));

        let dsn = "tmq://".into_dsn().unwrap();
        let upcoming = BackupConfigBuilder::parse_upcoming(&dsn).unwrap();
        assert!(upcoming.is_none());

        let dsn = "tmq://?upcoming=".into_dsn().unwrap();
        let upcoming = BackupConfigBuilder::parse_upcoming(&dsn).unwrap();
        assert!(upcoming.is_none());

        let dsn = "tmq://?upcoming=abc".into_dsn().unwrap();
        let upcoming = BackupConfigBuilder::parse_upcoming(&dsn);
        assert!(upcoming.is_err());
        assert!(upcoming
            .unwrap_err()
            .to_string()
            .starts_with("failed to parse upcoming: abc, cause: "));
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
