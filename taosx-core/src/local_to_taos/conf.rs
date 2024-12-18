use crate::tmq::BackupObject;
use crate::utils;
use crate::utils::sql::connect_taos_root;
use anyhow::Context;
use chrono::{DateTime, Utc};
use std::path::PathBuf;
use std::time::Duration;
use taos::{AsyncQueryable, Dsn};

#[derive(Debug, Clone)]
pub struct LocalRestoreConfig {
    /// 备份对象的元信息
    pub backup_obj: BackupObject,
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
    /// taos 的原始 dsn
    pub raw_to: Dsn,
    #[allow(unused)]
    /// 恢复到指定的数据库，如果为 None，则使用 topic_meta.db_name
    pub database: Option<String>,
}

impl LocalRestoreConfig {
    pub fn meta(&self) -> &BackupObject {
        &self.backup_obj
    }

    /// 从 taosd 中查询备份对象的元信息
    pub async fn query_meta(
        &self,
        _db_name: &str,
        _stable: Option<&String>,
    ) -> anyhow::Result<Option<BackupObject>> {
        todo!()
    }

    /// 删除备份对象的元信息
    pub async fn del_meta(&self, meta: &BackupObject) -> anyhow::Result<()> {
        let taos = connect_taos_root(&self.raw_to).await?;

        // drop topic
        let topic = self.backup_obj.topic.as_str();
        let sql = format!("DROP TOPIC IF EXISTS `{}`", topic);
        taos.exec(sql).await?;

        // drop database
        let sql = format!("DROP DATABASE IF EXISTS `{}`", meta.db_name);
        taos.exec(sql).await?;

        Ok(())
    }

    /// 向 taosd 中写入备份对象的元信息
    pub async fn write_meta(&self, meta: &BackupObject) -> anyhow::Result<()> {
        let taos = connect_taos_root(&self.raw_to).await?;
        // create database
        let sql = meta.db_sql.clone();
        tracing::info!("exec sql: {}", sql);
        taos.exec(sql).await?;

        // create stable
        if let Some(stable_sql) = &meta.stable_sql {
            let sql = stable_sql.clone();
            tracing::info!("exec sql: {}", sql);
            taos.exec(sql).await?;
        }

        Ok(())
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

        Ok(LocalRestoreConfig {
            backup_obj,
            backup_dir: backup_dir.clone(),
            from,
            to,
            error_restore_dir: backup_dir.clone(),
            error_retry_max,
            error_retry_interval,
            raw_to: self.to.clone(),
            database: self.to.subject.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    // TODO: add tests
}
