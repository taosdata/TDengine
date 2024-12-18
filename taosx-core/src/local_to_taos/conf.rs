use crate::utils;
use chrono::{DateTime, Utc};
use std::path::PathBuf;
use std::time::Duration;
use taos::Dsn;

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
        // topic
        let topic = utils::parse_key_in_dsn(&self.from, "topic")?
            .ok_or(anyhow::anyhow!("topic is None in dsn"))?;

        // backup_dir
        let mut backup_dir = utils::parse_dir_in_dsn(&self.from, None)?
            .ok_or(anyhow::anyhow!("path is None in dsn"))?;
        if let Some(task_id) = &self.task_id {
            backup_dir.push(task_id.as_str());
        }
        // from 备份点
        let from = utils::parse_datetime_in_dsn(&self.from, "from")?;
        // to 备份点
        let to = utils::parse_datetime_in_dsn(&self.to, "to")?;
        // error.max.retry
        let error_retry_max =
            utils::parse_keys_in_dsn::<u32>(&self.to, &["error.max.retry", "error_retry_max"])?
                .unwrap_or(10u32);
        // error.retry.interval
        let error_retry_interval = utils::parse_duration_in_dsn(&self.to, "retry_interval")?
            .unwrap_or(Duration::from_secs(5));
        // database
        let database = &self
            .to
            .subject
            .clone()
            .ok_or(anyhow::anyhow!("database is None in dsn: {}", &self.to))?;

        Ok(LocalRestoreConfig {
            topic,
            backup_dir: backup_dir.clone(),
            from,
            to,
            error_restore_dir: backup_dir.clone(),
            error_retry_max,
            error_retry_interval,
            database: database.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct LocalRestoreConfig {
    /// 备份对象
    pub topic: String,
    /// 备份文件的目录，默认是 $TAOSX_DATA_DIR/backup/$TASK_ID
    pub backup_dir: PathBuf,
    /// 指定从哪个备份点开始恢复，如果为 None，则从最早的备份点开始
    pub from: Option<DateTime<Utc>>,
    /// 指定到哪个备份点结束恢复，如果为 None，恢复任务永不停止，持续监听 backup_dir 下的新备份文件
    pub to: Option<DateTime<Utc>>,
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
    /// database
    pub database: String,
}

impl LocalRestoreConfig {}

#[cfg(test)]
mod tests {}
