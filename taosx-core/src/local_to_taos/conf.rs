use crate::s3::{S3Config, S3_ENABLE};
use crate::tmq::BackupObject;
use crate::utils;
use crate::utils::sql::connect_taos_root;
use anyhow::Context;
use chrono::{DateTime, Utc};
use std::path::PathBuf;
use std::time::Duration;
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, Taos, TaosBuilder, TaosPool};

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
    // 强制恢复，如果为 true，则删除已存在的数据库或表。默认为 true
    // pub force: bool,
    /// S3 存储配置
    pub s3_config: Option<S3Config>,
}

impl LocalRestoreConfig {
    // 从目标 taosd 中查询备份对象
    // pub async fn is_obj_existed(&self) -> anyhow::Result<bool> {
    //     Ok(self.query_obj().await?.is_some())
    // }

    // pub async fn query_obj(&self) -> anyhow::Result<Option<BackupObject>> {
    //     // 如果target database 不为空，则使用target database; 否则使用backup object中的db_name
    //     let db_name = match &self.database {
    //         Some(db_name) => db_name.clone(),
    //         None => self.backup_obj.db_name,
    //     };
    //
    //     // taos://xxx/db2?stable=stb
    //     let mut dsn = Dsn {
    //         subject: Some(db_name),
    //         ..self.raw_to.clone()
    //     };
    //     if let Some(stable) = &self.backup_obj.stable_name {
    //         dsn.params.insert("stable".to_string(), stable.to_string());
    //     }
    //
    //     BackupObject::try_from_taos(&dsn)
    //         .await
    //         .context(format!("failed to query backup object from taos: {}", &dsn))
    // }

    // 删除备份对象的元信息
    // pub async fn delete_obj(&self) -> anyhow::Result<()> {
    //     let taos = connect_taos_root(&self.raw_to).await?;
    //
    //     let db_name = match &self.database {
    //         None => self.backup_obj.db_name.clone(),
    //         Some(db_name) => db_name.clone(),
    //     };
    //
    //     // drop topic
    //     // if let Some(topic) = &self.backup_obj.topic {
    //     //     let sql = format!("DROP TOPIC IF EXISTS `{}`", topic);
    //     //     tracing::info!("exec sql: {sql}");
    //     //     taos.exec(sql).await.context("failed to drop topic")?;
    //     // }
    //
    //     // drop stable
    //     match &self.backup_obj.stable_name {
    //         Some(stable_name) => {
    //             let sql = format!("DROP TABLE IF EXISTS `{}`.`{}`", db_name, stable_name);
    //             tracing::info!("exec sql: {sql}");
    //             taos.exec(sql).await.context("failed to drop stable")?;
    //         }
    //         None => {
    //             let sql = format!("DROP DATABASE IF EXISTS `{}`", db_name);
    //             tracing::info!("exec sql: {sql}");
    //             taos.exec(sql).await.context("failed to drop database")?;
    //         }
    //     }
    //
    //     Ok(())
    // }

    // 向 taosd 中写入备份对象的元信息
    // pub async fn restore_obj(&self) -> anyhow::Result<()> {
    //     let taos = connect_taos_root(&self.raw_to).await?;
    //
    //     let db_name = match &self.database {
    //         None => self.backup_obj.db_name.clone(),
    //         Some(db_name) => db_name.clone(),
    //     };
    //
    //     match &self.backup_obj.stable_sql {
    //         None => {
    //             // create database
    //             let sql = self.backup_obj.db_sql.clone();
    //             let sql = sql.replace(&self.backup_obj.db_name, &db_name);
    //             tracing::info!("exec sql: {}", sql);
    //             taos.exec(sql).await?;
    //         }
    //         Some(stable_sql) => {
    //             // create stable
    //             let sql = format!("USE `{}`", db_name);
    //             tracing::info!("exec sql: {}", sql);
    //             let res = taos.exec(sql.clone()).await;
    //             if let Err(e) = res {
    //                 if e.to_string().to_lowercase().contains("database not exist") {
    //                     let db_sql = self.backup_obj.db_sql.clone();
    //                     let db_sql = db_sql.replace(&self.backup_obj.db_name, &db_name);
    //                     tracing::info!("exec sql: {}", db_sql);
    //                     taos.exec(db_sql).await?;
    //                     taos.exec(sql).await?;
    //                 } else {
    //                     return Err(anyhow::Error::from(e).context("failed to use database"));
    //                 }
    //             }
    //
    //             let sql = stable_sql.clone();
    //             let sql = sql.replace(&self.backup_obj.db_name, &db_name);
    //             tracing::info!("exec sql: {}", sql);
    //             taos.exec(sql).await?;
    //         }
    //     }
    //
    //     Ok(())
    // }

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
        // if backup_obj.topic.is_none() {
        //     return Err(anyhow::anyhow!("topic not found in dsn"));
        // }

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

        // force
        // let force = utils::parse_key_in_dsn::<bool>(&self.to, "force")?.unwrap_or(true);

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
        })
    }
}

#[cfg(test)]
mod tests {
    // TODO: add tests
}
