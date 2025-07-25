use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use std::path::PathBuf;
use std::time::Duration;
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, Taos, TaosBuilder, TaosPool};

use taosx_core::s3::{S3_ENABLE, S3Config};
use taosx_core::tmq::BackupObject;
use taosx_core::utils;
use taosx_core::utils::sql::connect_taos_root;

#[derive(Debug, Clone)]
pub struct LocalRestoreConfig {
    #[allow(unused)]
    task_id: Option<String>,
    #[allow(unused)]
    raw_from: Dsn,
    #[allow(unused)]
    raw_to: Dsn,
    /// 备份对象的元信息
    pub backup_obj: BackupObject,
    /// 备份文件的目录，默认是 $TAOSX_DATA_DIR/backup/$TASK_ID
    pub backup_dir: PathBuf,
    /// 指定从哪个备份点开始恢复，如果为 None，则从最早的备份点开始
    pub start_from: Option<DateTime<Utc>>,
    /// 指定到哪个备份点结束恢复，如果为 None，恢复任务永不停止，持续监听 backup_dir 下的新备份文件
    pub stop_at: Option<DateTime<Utc>>,
    #[allow(unused)]
    /// 写入 taosd 失败，将错误数据转存到日志，
    pub error_restore_dir: PathBuf,
    #[allow(unused)]
    /// 最大错误重试次数。默认为：10
    pub error_retry_max: u32,
    #[allow(unused)]
    /// 错误重试的间隔。默认为 5s。
    pub error_retry_interval: Duration,
    #[allow(unused)]
    /// 恢复到指定的数据库，如果为 None，则使用 topic_meta.db_name
    pub database: Option<String>,
    /// S3 存储配置
    pub s3_config: Option<S3Config>,
    /// 恢复成功后的操作
    pub post_action: Option<PostAction>,
    /// 是否持续监听
    pub watch: Option<bool>,
}

impl LocalRestoreConfig {
    #[allow(unused)]
    pub async fn connect_taos(&self) -> anyhow::Result<Taos> {
        let taos = connect_taos_root(&self.raw_to).await?;

        if let Some(db_name) = &self.database {
            let sql = format!("USE `{}`", db_name);
            tracing::info!("exec sql: {}", sql);
            taos.exec(sql).await?;
        }

        Ok(taos)
    }

    pub async fn connect_taos_pool(&self) -> anyhow::Result<TaosPool> {
        let pool = TaosBuilder::from_dsn(&self.raw_to)?
            .pool()
            .map_err(|e| anyhow::anyhow!("failed to build connect pool, cause: {:?}", e))?;

        Ok(pool)
    }
}

pub struct LocalRestoreConfigBuilder {
    /// 任务ID
    task_id: Option<String>,
    /// e.g. local:$TAOSX_DATA_DIR/backip/$TASK_ID?topic=x123&from=2021-01-01T00:00:00Z&to=2021-01-02T00:00:00Z
    from: Dsn,
    /// e.g. taos+ws://$HOST:$PORT/$DATABASE
    to: Dsn,
}

impl LocalRestoreConfigBuilder {
    pub fn new(task_id: &Option<String>, from: &Dsn, to: &Dsn) -> Self {
        Self {
            task_id: task_id.clone(),
            from: from.clone(),
            to: to.clone(),
        }
    }

    pub async fn build(&self) -> anyhow::Result<LocalRestoreConfig> {
        // backup object
        let backup_obj = BackupObject::try_from(&self.from).context(format!(
            "failed to parse backup object in dsn: {}",
            &self.from
        ))?;

        // backup directory
        let mut backup_dir = utils::parse_dir_in_dsn(&self.from, None)?
            .ok_or(anyhow::anyhow!("path not found in dsn"))?;
        if let Some(backup_task_id) = &backup_obj.task_id {
            backup_dir = backup_dir.join(backup_task_id);
        }

        // s3_enable
        let s3_enable = utils::parse_key_in_dsn::<bool>(&self.from, S3_ENABLE)?.unwrap_or(false);
        let s3_config = if s3_enable {
            // 解析 s3 配置参数
            let s3_config = S3Config::from_dsn(&self.from)
                .context(format!("failed to parse s3 config in dsn: {}", &self.from))?;
            // 检查 s3 连通性
            s3_config.connect().await?;
            Some(s3_config)
        } else {
            None
        };

        // from 备份点
        let start_from = utils::parse_datetime_in_dsn(&self.from, "from")?;
        // to 备份点
        let stop_at = utils::parse_datetime_in_dsn(&self.from, "to")?;
        // error_restore_dir
        let mut error_restore_dir = utils::parse_dir_in_dsn(&self.from, None)?
            .ok_or(anyhow::anyhow!("path not found in dsn"))?;
        if let Some(restore_task_id) = &self.task_id {
            error_restore_dir = error_restore_dir.join(restore_task_id);
        }

        // error.max.retry
        let error_retry_max =
            utils::parse_keys_in_dsn::<u32>(&self.to, &["error.max.retry", "error_retry_max"])?
                .unwrap_or(10u32);
        // error.retry.interval
        let error_retry_interval = utils::parse_duration_in_dsn(&self.to, "retry_interval")?
            .unwrap_or(Duration::from_secs(5));
        // watch
        let watch = utils::parse_keys_in_dsn::<bool>(&self.from, &["watch"])?;

        // post action
        let post_action = PostAction::try_from_dsn(&self.from)?;

        Ok(LocalRestoreConfig {
            task_id: self.task_id.clone(),
            raw_from: self.from.clone(),
            raw_to: self.to.clone(),
            backup_obj,
            backup_dir,
            start_from,
            stop_at,
            error_restore_dir,
            error_retry_max,
            error_retry_interval,
            database: self.to.subject.clone(),
            s3_config,
            post_action,
            watch,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PostAction {
    Delete,
    /// Move the restored data to a specified path.
    /// The path can contain a date-time pattern, which will be replaced with the backup file's timestamp.
    /// For example, if the path is "/data/tmp/%Y-%m-%dT%H:%M:%S",
    /// it will be replaced with "/data/restore/2023-10-01T12:00:00" if the backup file is x85c20042893-1749192649912-501-2.z
    Move(String),
}

impl PostAction {
    fn try_from_dsn(dsn: &Dsn) -> anyhow::Result<Option<Self>> {
        const POST_ACTION: &str = "post_action";
        const MOVE_TO: &str = "move_to";

        let post_action = dsn.get(POST_ACTION);
        if let Some(action) = post_action {
            match action.to_lowercase().as_str() {
                "delete" | "del" | "remove" | "rm" => Ok(Some(Self::Delete)),
                "move" | "mv" => {
                    let path = dsn
                        .get(MOVE_TO)
                        .ok_or(anyhow::anyhow!("move_to is required for post_action: MOVE"))?;
                    // Check the path
                    if path.is_empty() {
                        bail!("move_to cannot be empty for post_action: MOVE");
                    }
                    Ok(Some(Self::Move(path.to_string())))
                }
                _ => bail!("unknown post action: {}", action),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::IntoDsn;

    #[test]
    fn test_post_action() {
        let dsn = "local:/path/to/backup?post_action=delete"
            .into_dsn()
            .unwrap();
        let action = PostAction::try_from_dsn(&dsn).unwrap();
        assert_eq!(action, Some(PostAction::Delete));

        let dsn = "local:/path/to/backup?post_action=move&move_to=/path/to/move"
            .into_dsn()
            .unwrap();
        let action = PostAction::try_from_dsn(&dsn).unwrap();
        assert_eq!(action, Some(PostAction::Move("/path/to/move".to_string())));

        let dsn = "local:/path/to/backup?post_action=move".into_dsn().unwrap();
        assert!(
            PostAction::try_from_dsn(&dsn).is_err(),
            "move action without move_to should fail"
        );

        let dsn = "local:/path/to/backup?post_action=move&move_to=/home/taosx/tmp/s%Y-%m-%dT%H:%M:%S%.3f%:z"
            .into_dsn()
            .unwrap();
        let action = PostAction::try_from_dsn(&dsn).unwrap();
        assert_eq!(
            action,
            Some(PostAction::Move(
                "/home/taosx/tmp/s%Y-%m-%dT%H:%M:%S%.3f%:z".to_string()
            ))
        );

        let dsn = "local:/path/to/backup?post_action=unknown"
            .into_dsn()
            .unwrap();
        assert!(PostAction::try_from_dsn(&dsn).is_err());
    }
}
