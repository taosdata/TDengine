use anyhow::{Context as AnyhowContext, Result};
use chrono::{DateTime, Local, Utc};

use dashmap::DashMap;
use scc::HashMap;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::RwLock;
use std::sync::atomic::Ordering::SeqCst;
use std::time::{Duration, Instant};
use std::{path::PathBuf, sync::Arc};
use taos::sync::MessageSet;
use taos::taos_query::tmq::VGroupId;
use taos::*;
use taos_query::common::RawData;
use taosx_core::core_metrics::{CoreMetrics, get_metrics};
use taosx_core::s3::S3Dumper;
use taosx_core::{
    taoz::{RawType, ZFile},
    tmq::*,
};
use tokio::select;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};

use conf::{BackupConfig, BackupConfigBuilder, BackupPointGenMode};

pub mod conf;

/// 增量备份任务，通过 TDengine 的订阅，将数据备份到本地
#[instrument(skip_all, fields(task_id))]
#[async_backtrace::framed]
pub async fn tmq_to_local(
    task_id: Option<String>,
    from: Dsn,
    to: Dsn,
    cancel: CancellationToken,
) -> Result<()> {
    tracing::info!("tmq_to_local start");

    // 解析备份需要的参数
    let mut config = BackupConfigBuilder::new(task_id.clone(), &from, &to)
        .build()
        .await
        .context(format!(
            "parse backup config error, from: {}, to: {}",
            &from, &to
        ))?;

    if !config.self_repeat {
        return tmq_to_local_impl(config, cancel).await.inspect_err(|err| {
            tracing::error!("tmq_to_local error: {:#}", err);
        });
    }

    let interval = config.interval.unwrap_or(Duration::from_secs(60 * 10));
    loop {
        let next_upcoming = config.upcoming.unwrap_or(Utc::now()) + interval;

        tmq_to_local_impl(config.clone(), cancel.clone()).await?;
        // update upcoming
        config.upcoming = Some(next_upcoming);
    }
}

async fn tmq_to_local_impl(mut config: BackupConfig, cancel: CancellationToken) -> Result<()> {
    tracing::debug!("backup config: {:#?}", config);

    // 等待并更新 upcoming
    wait_for_upcoming_impl(config.upcoming).await?;
    if config.upcoming.is_some() {
        config.upcoming = Some(Utc::now());
    }

    // 如果是初始备份, 则创建备份计划使用的 topic，创建备份目录
    if config.is_initial_backup().await? {
        config.create_topic().await?;
        config.create_backup_dir().await?;
    }

    // 创建 consumer
    let consumers = config
        .create_consumer()
        .await
        .context("failed to create consumer")?;

    let file_timeout = config.interval.unwrap_or(Duration::from_secs(1));

    // 创建 ZFileMan
    let man = ZFileMan {
        api_version: crate::build::PKG_VERSION.to_owned(),
        server_version: config.server_version.clone(),
        backup_dir: config.backup_dir.clone(),
        topic: config.topic.clone(),
        ts: config.upcoming,
        compression_level: config.backup_comp_level,
        max_file_size: config.backup_max_size,
        move_to: config.move_to.clone(),
        timeout: file_timeout,
        sync: tokio::sync::Mutex::new(()),
        writers: Default::default(),
    };
    // load metrics
    let tid = config
        .task_id
        .clone()
        .and_then(|id| id.parse::<i64>().ok())
        .unwrap_or(-1);
    let metrics = get_metrics(tid).await;

    // 创建并启动 BackupWorker
    let man = Arc::new(man);
    let mut join_set = JoinSet::new();
    for (idx, consumer) in consumers.into_iter().enumerate() {
        // 创建 worker
        let mut task = BackupWorker {
            id: idx,
            config: config.clone(),
            consumer,
            assignments: Arc::new(RwLock::new(HashMap::new())),
            man: man.clone(),
            cancel: cancel.clone(),
            metrics: metrics.clone(),
        };

        // metrics 增加 consumer 数量
        if let Some(metrics) = &metrics {
            let metrics = metrics.tmq();
            metrics.consumers.fetch_add(1, SeqCst);
        }

        // 启动 BackupWorker
        join_set.spawn(async move { task.run().await }.in_current_span());
    }

    // 如果启用 S3，创建 S3 dumper
    let dumper_handler = match (&config.s3_enable, &config.s3_config) {
        (true, Some(s3_config)) => {
            // 创建 S3 dumper
            let dumper = S3Dumper::new(
                config.backup_dir.clone(),
                s3_config.clone(),
                config.backup_retention_period,
                config.backup_retention_size,
                cancel.clone(),
            )
            .await?;
            // 启动 S3 dumper
            let handler = tokio::spawn(async move { dumper.run().await });
            Some(handler)
        }
        _ => None,
    };

    // 等待所有 BackupWorker 完成
    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res.map_err(anyhow::Error::from).and_then(|r| r) {
            tracing::error!("abort all tmq_to_local workers since error: {:#}", err);
            join_set.abort_all();
            // TODO: 如果出现错误，要清理文件
            return Err(err);
        }
    }

    // 关闭 ZFileMan
    man.shutdown().await?;

    // 如果启用了S3Dumper，停止 S3Dumper，并等待 S3Dumper 完成
    if let Some(dumper_handler) = dumper_handler {
        cancel.cancel();
        let _ = dumper_handler.await?;
    }

    tracing::info!("tmq_to_local finish");
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct LocalConfig {
    pub(crate) created_at: DateTime<Local>,
    pub(crate) last_modified: DateTime<Local>,
    pub(crate) group_id: String,
    pub(crate) client_id: String,
    pub(crate) topics: Vec<Topic>,
}

#[derive(Debug)]
struct BackupWorker {
    id: usize,
    config: BackupConfig,
    consumer: Consumer,
    assignments: Arc<RwLock<HashMap<(String, VGroupId), i64>>>,
    man: Arc<ZFileMan>,
    cancel: CancellationToken,
    metrics: Option<Arc<CoreMetrics>>,
}

/// 如果当前时间比 upcoming 早，等待到 upcoming
async fn wait_for_upcoming_impl(upcoming: Option<DateTime<Utc>>) -> Result<()> {
    if let Some(upcoming) = upcoming {
        let now = Utc::now();
        if now < upcoming {
            let duration = upcoming - now;
            tracing::info!("tmq_to_local wait for upcoming: {}", upcoming);
            tokio::time::sleep(duration.to_std().map_err(|err| {
                anyhow::Error::from(err)
                    .context(format!("failed to convert: {:?} to std duration", duration))
            })?)
            .await;
        }
    }
    Ok(())
}

impl BackupWorker {
    async fn run(&mut self) -> Result<()> {
        let run_impl = self.run_impl().in_current_span();
        select! {
            _ = self.cancel.cancelled() => {
                tracing::warn!("tmq_to_local [{}] cancelled", self.id);
            }
            res = run_impl => {
                match res {
                    Ok(_) => {
                        tracing::info!("tmq_to_local [{}] completed", self.id);
                    }
                    Err(err) => {
                        tracing::error!(?err, "tmq_to_local [{}] exit with error: {:#}", self.id, err);
                        return Err(err);
                    }
                }
            }
        }

        Ok(())
    }

    async fn run_impl(&self) -> Result<()> {
        let timeout = self.consumer.default_timeout();
        tracing::debug!(
            "tmq_to_local consumer:[{}] start, use timeout: {:?}",
            self.id,
            timeout
        );
        let mut last = Instant::now();
        loop {
            tracing::trace!("tmq_to_local consumer:[{}] polling...", self.id);
            let res = self.consumer.recv_timeout(Timeout::from_millis(500)).await;
            tracing::trace!("tmq_to_local consumer:[{}] polled.", self.id);
            match res {
                Ok(None) => {
                    // 如果超过了 consumer.timeout 没有收到消息，则退出
                    match timeout {
                        Timeout::Duration(d) => {
                            if last.elapsed() > d {
                                tracing::info!("tmq_to_local consumer:[{}] timeout", self.id);
                                break;
                            }
                        }
                        Timeout::None => {
                            tracing::info!(
                                "tmq_to_local consumer:[{}] timeout is None, exit",
                                self.id
                            );
                            break;
                        }
                        Timeout::Never => {
                            tracing::trace!(
                                "tmq_to_local consumer[{}]: no messages received for {:?}",
                                self.id,
                                last.elapsed()
                            );
                            self.man.check_or_next().await?;
                            tracing::trace!(
                                "tmq_to_local consumer[{}]: check_or_next completed",
                                self.id
                            );
                        }
                    }
                }
                Ok(Some((offset, message))) => {
                    // 更新最后一次收到消息的时间
                    last = Instant::now();

                    // 通过 topic, vg_id 可以获取到当前的 offset
                    let vg_id = offset.vgroup_id();
                    if self.config.backup_point_gen_mode == BackupPointGenMode::ByOffset {
                        let topic = offset.topic();
                        // let cur_offset = self.consumer.position(topic, vg_id).await?;
                        let position = self.config.position(topic, vg_id).await?;

                        if let Some((current, latest)) = position {
                            // 获取 topic, vgroup 对应的 end_offset
                            let end_offset = self.get_end_offset(topic.to_string(), vg_id).await;

                            let end_offset = match end_offset {
                                Some(offset) => offset,
                                None => {
                                    // 如果 end_offset 不存在，设置为当前的 latest offset
                                    self.set_end_offset(topic.to_string(), vg_id, latest).await;
                                    latest
                                }
                            };

                            // 如果 cur_offset == end_offset，表示当前 vgroup 已经备份完成
                            if current == end_offset {
                                self.set_complete(topic.to_string(), vg_id).await;
                            }
                            // 如果所有 vgroup 都备份完成，退出
                            if self.is_all_complete().await {
                                break;
                            }
                        }
                    }

                    // 处理 message，写入本地文件
                    match message {
                        MessageSet::Meta(meta) => {
                            let raw = meta.as_raw_meta().await?;
                            tracing::debug!("backup meta, len: {}", raw.raw_len());
                            self.man
                                .write_vgroup_with_raw(vg_id, &raw, RawType::Meta)
                                .await
                                .context("Backup raw meta message failed")?;
                            self.man
                                .flush_vgroup(vg_id)
                                .await
                                .context("Flush vgroup error")?;
                            if let Some(metrics) = &self.metrics {
                                let metrics = metrics.tmq();
                                metrics.add_messages_of_meta(1);
                            }
                        }
                        MessageSet::Data(data) => {
                            let raw = data.as_raw_data().await?;
                            tracing::debug!("backup data, len: {}", raw.raw_len());
                            self.man
                                .write_vgroup_with_raw(vg_id, &raw, RawType::Data)
                                .await
                                .context("Backup raw data message failed")?;
                            self.man
                                .flush_vgroup(vg_id)
                                .await
                                .context("Flush vgroup error")?;
                            if let Some(metrics) = &self.metrics {
                                let metrics = metrics.tmq();
                                metrics.add_messages_of_data(1);
                            }
                        }
                        MessageSet::MetaData(_meta, data) => {
                            let raw = data.as_raw_data().await?;
                            tracing::debug!("backup raw data, len: {}", raw.raw_len());
                            self.man
                                .write_vgroup_with_raw(vg_id, &raw, RawType::Both)
                                .await
                                .context("Backup raw metadata message failed")?;
                            self.man
                                .flush_vgroup(vg_id)
                                .await
                                .context("Flush vgroup error")?;
                            if let Some(metrics) = &self.metrics {
                                let metrics = metrics.tmq();
                                metrics.add_messages_of_data(1);
                            }
                        }
                    }
                    self.consumer.commit(offset).await?;
                    if let Some(metrics) = &self.metrics {
                        let metrics = metrics.tmq();
                        metrics.add_messages(1);
                    }
                }
                Err(err) => {
                    tracing::error!(?err, "tmq_to_local consumer[{}] recv error", self.id);
                    return Err(err).context("tmq_to_local consumer recv error");
                }
            }
        }

        if let Some(metrics) = &self.metrics {
            let metrics = metrics.tmq();
            tracing::info!(
                "tmq_to_local processed messages: {}, total_messages: {} ",
                metrics.messages.load(SeqCst),
                metrics.total_messages.load(SeqCst),
            );
        }

        Ok(())
    }

    async fn set_end_offset(&self, topic: String, vg_id: VGroupId, offset: i64) {
        let assign_map = self.assignments.write().unwrap();
        assign_map.insert((topic, vg_id), offset).unwrap();
    }

    async fn get_end_offset(&self, topic: String, vg_id: VGroupId) -> Option<i64> {
        let assign_map = self.assignments.read().unwrap();
        assign_map.get(&(topic, vg_id)).map(|s| *s)
    }

    async fn set_complete(&self, topic: String, vg_id: VGroupId) {
        let assign_map = self.assignments.write().unwrap();
        assign_map.remove(&(topic, vg_id)).unwrap();
    }

    async fn is_all_complete(&self) -> bool {
        let assign_map = self.assignments.read().unwrap();
        assign_map.is_empty()
    }
}

struct ZFileMan {
    /// taosx 的版本号
    api_version: String,
    /// taosd 的版本号
    server_version: String,
    /// 存放备份文件的目录
    backup_dir: PathBuf,
    /// 增量备份对应的 topic
    topic: String,
    /// 备份点对应的时间，如果为 None，表示使用任务结束的时间作为备份点
    ts: Option<DateTime<Utc>>,
    /// 压缩级别
    compression_level: async_compression::Level,
    /// 文件的最大大小
    max_file_size: u64,
    /// 在文件写满后，将文件移动到 move_to 目录
    move_to: Option<PathBuf>,
    /// 写入超时，当文件不为空，且超过 timeout 没有写入数据，则关闭当前文件
    timeout: Duration,

    sync: tokio::sync::Mutex<()>,
    writers: DashMap<i32, tokio::sync::Mutex<ZFile>>,
}

impl Debug for ZFileMan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZFileMan")
            .field("api_version", &self.api_version)
            .field("server_version", &self.server_version)
            .field("path", &self.backup_dir)
            .field("topic", &self.topic)
            .field("compress_level", &self.compression_level)
            .field("max_file_size", &self.max_file_size)
            .field("move_to", &self.move_to)
            .finish()
    }
}

impl Drop for ZFileMan {
    fn drop(&mut self) {
        //self.writers.iter().for_each(|entry| {
        //    let _ = block_in_place(async { entry.value().lock().await.shutdown().await });
        //});
        self.writers.clear();
    }
}

impl ZFileMan {
    pub async fn shutdown(&self) -> Result<()> {
        for entry in self.writers.iter_mut() {
            let mut man = entry.value().lock().await;
            tracing::info!("Flush vgroup {}", entry.key());
            // Finish any open data block and ensure contents are flushed to disk first
            man.start_raw_block().await?;
            man.finish_raw_block().await?;
            man.flush().await?;
            man.shutdown().await?;
            // After the file has been closed, move it
            man.move_to().await?;
        }
        Ok(())
    }

    async fn assert_vgroup(&self, vgroup: i32) -> Result<()> {
        if !self.writers.contains_key(&vgroup) {
            let _ = self.sync.lock().await;
            if !self.writers.contains_key(&vgroup) {
                let file = ZFile::new(
                    &self.api_version,
                    &self.server_version,
                    &self.backup_dir,
                    (&self.topic, self.ts, vgroup, 1),
                    self.compression_level,
                    self.max_file_size,
                    self.move_to.clone(),
                    self.timeout,
                )
                .await
                .inspect_err(|error| {
                    tracing::error!(?error, "create new ZFile for vgroup {vgroup} error");
                })
                .with_context(|| format!("create new ZFile for vgroup: {}", vgroup))?;
                tracing::debug!("create new ZFile for vgroup: {}", vgroup);
                let _ = self.writers.insert(vgroup, tokio::sync::Mutex::new(file));
            }
        }
        Ok(())
    }

    async fn write_vgroup_with_raw(
        &self,
        vgroup: i32,
        raw: &RawData,
        raw_type: RawType,
    ) -> Result<()> {
        self.assert_vgroup(vgroup).await?;
        let entry = self.writers.get(&vgroup).expect("should always exist");
        let mut writer = entry.value().lock().await;
        writer.write_raw(raw, raw_type).await?;

        Ok(())
    }

    /// 遍历所有 writer check_or_next
    async fn check_or_next(&self) -> Result<()> {
        for entry in self.writers.iter() {
            let mut writer = entry.value().lock().await;
            writer.check_or_next().await?;
        }
        Ok(())
    }

    async fn flush_vgroup(&self, vgroup: i32) -> Result<()> {
        self.assert_vgroup(vgroup).await?;
        let entry = self.writers.get(&vgroup).expect("should always exist");
        let mut writer = entry.value().lock().await;
        writer.check().await?;
        writer.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_for_upcoming() {
        let now = Utc::now();
        wait_for_upcoming_impl(Some(now + chrono::Duration::seconds(2)))
            .await
            .unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 2);

        let now = Utc::now();
        wait_for_upcoming_impl(None).await.unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 0);

        let now = Utc::now();
        wait_for_upcoming_impl(Some(now - chrono::Duration::days(1)))
            .await
            .unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 0);
    }
}
