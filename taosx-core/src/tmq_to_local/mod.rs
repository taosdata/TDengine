use crate::s3::S3Dumper;
use crate::tmq_to_local::conf::{BackupConfig, BackupConfigBuilder, BackupPointGenMode};
use crate::{
    core_metrics::{get_metrics_arc, CoreMetrics, TaskMetrics},
    tmq::tmq_metric::TmqMetrics,
};
use crate::{
    taoz::{RawType, ZFile},
    tmq::*,
};
use anyhow::{bail, Context as AnyhowContext, Result};
use chrono::{DateTime, Local, Utc};
use dashmap::DashMap;
use scc::HashMap;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::RwLock;
use std::time::Duration;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use taos::taos_query::tmq::{Assignment, VGroupId};
use taos::*;
use taos_query::common::RawData;
use tokio::select;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

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
        return tmq_to_local_impl(config, cancel).await;
    }

    let interval = config.interval.unwrap_or(Duration::from_secs(60 * 10));
    loop {
        tmq_to_local_impl(config.clone(), cancel.clone()).await?;
        tokio::time::sleep(interval).await;

        // update upcoming
        config.upcoming = Some(Utc::now());
    }
}

async fn tmq_to_local_impl(mut config: BackupConfig, cancel: CancellationToken) -> Result<()> {
    tracing::debug!("backup config: {:#?}", config);

    // 如果是初始备份, 则创建备份计划使用的 topic，创建备份目录
    if config.is_initial_backup().await? {
        config.create_topic().await?;
        config.create_backup_dir().await?;
    }

    // 等待并更新 upcoming
    wait_for_upcoming_impl(config.upcoming).await?;
    if config.upcoming.is_some() {
        config.upcoming = Some(Utc::now());
    }

    // 创建 consumer
    let consumers = config.create_consumer().await?;
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
        sync: tokio::sync::Mutex::new(()),
        writers: Default::default(),
    };
    // load metrics
    let metrics = get_metrics_arc(config.task_id.clone()).await;

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
        let metrics = metrics.tmq();
        metrics.consumers.fetch_add(1, SeqCst);

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

impl LocalConfig {
    pub fn new(
        topics: Vec<Topic>,
        group_id: impl Into<String>,
        client_id: impl Into<String>,
    ) -> Self {
        Self {
            created_at: Local::now(),
            last_modified: Local::now(),
            group_id: group_id.into(),
            client_id: client_id.into(),
            topics,
        }
    }
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let config = config::Config::builder()
            .add_source(config::File::with_name(&path.display().to_string()))
            .build()?;
        let config: LocalConfig = config.try_deserialize()?;
        Ok(config)
    }

    pub fn write_to(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let bytes = toml::to_string(self)?;
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

#[derive(Debug)]
struct BackupWorker {
    id: usize,
    config: BackupConfig,
    consumer: Consumer,
    assignments: Arc<RwLock<HashMap<(String, VGroupId), i64>>>,
    man: Arc<ZFileMan>,
    cancel: CancellationToken,
    metrics: Arc<CoreMetrics>,
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
    #[allow(unused)]
    async fn assign(&self) -> Result<()> {
        let assignments = self.consumer.assignments().await;
        if let Some(assigns) = assignments {
            for (topic, assign) in assigns {
                for a in assign {
                    let vg_id = a.vgroup_id();
                    let end_offset = a.end();
                    tracing::info!(
                        "set end offset of topic: {}, vg_id: {}, offset: {}",
                        topic,
                        vg_id,
                        end_offset
                    );
                    self.set_end_offset(topic.clone(), vg_id, end_offset).await;
                }
            }
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        tracing::info!("tmq_to_local worker[{}] start", self.id);
        let run_impl = self.run_impl().in_current_span();
        select! {
            _ = self.cancel.cancelled() => {
                tracing::warn!("tmq_to_local worker[{}] cancelled", self.id);
            }
            res = run_impl => {
                match res {
                    Ok(_) => {
                        tracing::info!("tmq_to_local worker[{}] completed", self.id);
                    }
                    Err(err) => {
                        tracing::error!(?err, "tmq_to_local worker[{}] exit with error: {:#}", self.id, err);
                        return Err(err);
                    }
                }
            }
        }

        Ok(())
    }

    async fn run_impl(&self) -> Result<()> {
        let metrics = self.metrics.tmq();

        let mut stream = self.consumer.stream();

        while let Some((offset, message)) = stream.try_next().await? {
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
                    metrics.add_messages_of_meta(1);
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
                    metrics.add_messages_of_data(1);
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
                    metrics.add_messages_of_data(1);
                }
            }
            self.consumer.commit(offset).await?;
            metrics.add_messages(1);
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
            man.move_to().await?;
            man.start_raw_block().await?;
            man.finish_raw_block().await?;
            man.flush().await?;
            man.shutdown().await?;
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

    #[allow(unused /* previous version */)]
    async fn write_vgroup_with_meta(
        &self,
        vgroup: i32,
        meta: taos::Meta,
        metrics: &TmqMetrics,
    ) -> Result<()> {
        let raw = meta.as_raw_meta().await?;
        self.assert_vgroup(vgroup).await?;
        let entry = self.writers.get(&vgroup).expect("should always exist");
        entry.value().lock().await.write_meta(&raw).await?;
        metrics.add_messages_of_meta(1);
        Ok(())
    }

    #[allow(unused /* previous version */)]
    async fn write_vgroup_with_data(
        &self,
        vgroup: i32,
        data: taos::Data,
        metrics: &TmqMetrics,
        stop_at: Option<DateTime<Local>>,
    ) -> Result<(usize, bool)> {
        self.assert_vgroup(vgroup)
            .await
            .with_context(|| format!("vgroup {vgroup} error"))?;
        let entry = self.writers.get(&vgroup).expect("should always exist");
        let mut writer = entry.value().lock().await;
        writer
            .start_raw_block()
            .await
            .with_context(|| format!("vgroup {vgroup} start raw block error"))?;
        let mut nrows = 0;
        let mut last_ts = None;
        while let Some(block) = data.fetch_raw_block().await.unwrap() {
            // dbg!(&block);
            writer
                .write_raw_block(&block)
                .await
                .with_context(|| format!("vgroup {vgroup} write raw block"))?;
            if let Some(view) = block.column_views().first() {
                match view {
                    ColumnView::Timestamp(view) => {
                        last_ts = view.iter().last().unwrap();
                    }
                    _ => unreachable!("expect first column is timestamp"),
                }
            }
            nrows += block.nrows();
            tracing::debug!(
                "[vg:{vgroup}] table {} rows: {}",
                block.table_name().unwrap_or_default(),
                block.nrows()
            );
            metrics.add_suc_blocks(1);
            metrics.add_written_rows(block.nrows() as _);
            metrics.add_written_points((block.nrows() * block.ncols()) as _);
        }
        writer
            .finish_raw_block()
            .await
            .with_context(|| format!("vgroup {vgroup} flush raw blocks error"))?;

        let mut stop = false;
        if let (Some(stop_at), Some(last_ts)) = (stop_at, last_ts) {
            if last_ts.to_datetime_with_tz() >= stop_at {
                stop = true;
            }
        }
        metrics.add_messages_of_data(1);
        Ok((nrows, stop))
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

    /// write data to file and return the number of rows and whether to stop
    async fn stop_at(
        &self,
        vgroup: i32,
        data: taos::Data,
        metrics: &TmqMetrics,
        stop_at: &Option<StopAt>,
    ) -> Result<(usize, bool)> {
        let mut nrows = 0;
        let mut last_ts = None;

        while let Some(block) = data.fetch_raw_block().await? {
            if let Some(view) = block.column_views().first() {
                match view {
                    ColumnView::Timestamp(view) => {
                        last_ts = view.iter().last().unwrap();
                    }
                    _ => unreachable!("expect first column is timestamp"),
                }
            }
            nrows += block.nrows();

            tracing::debug!(
                "[vg:{vgroup}] table {} rows: {}",
                block.table_name().unwrap_or_default(),
                block.nrows()
            );
            metrics.add_suc_blocks(1);
            metrics.add_written_rows(block.nrows() as _);
            metrics.add_written_points((block.nrows() * block.ncols()) as _);
        }

        let mut stop = false;
        if let (Some(stop_at), Some(last_ts)) = (stop_at, last_ts) {
            match stop_at {
                StopAt::Rows(n) => {
                    if nrows >= *n {
                        stop = true;
                    }
                }
                StopAt::DateTime(stop_at) => {
                    if last_ts.to_datetime_with_tz() >= *stop_at {
                        stop = true;
                    }
                }
            }
        }
        metrics.add_messages_of_data(1);
        Ok((nrows, stop))
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

/*** DEPRECATED CODES START ***/
#[deprecated(note = "use the new tmq_to_local instead")]
pub async fn tmq_to_local_previous(
    from: Dsn,
    to: Dsn,
    _jobs: usize,
    _force: bool,
    cancel: CancellationToken,
    task_id: Option<String>,
) -> Result<()> {
    let (mut from, builder, topics, _, _) = check_tmq_dsn(from).await?;

    // FIXME(@zitsen)
    let jobs = 0;
    let force = true;

    let offsets = Arc::new(DashMap::new());
    let version = builder.server_version().await?.to_owned();
    // parameters
    let stop_at = parse_stop_at(&from)?;
    let max_file_size = parse_max_file_size(&to)?.unwrap_or(1024 * 1024 * 1024);
    let move_to = parse_move_to(&to)?;
    let compression_level = parse_compression_level(&to)?;

    // local backup dir
    let dir = to.path.clone().ok_or(anyhow::anyhow!(
        "invalid local backup dsn: {}, Please use a local path DSN like `local:./path/to/backup`",
        to
    ))?;
    let backup_dir = Path::new(&dir).to_path_buf();
    if !backup_dir.exists() {
        tracing::info!("create dir for backup: {}", backup_dir.display());
        std::fs::create_dir_all(backup_dir.clone())?;
    } else {
        tracing::info!("use existing dir for backup: {}", backup_dir.display());
    }

    // LocalConfig
    let local_config_path = backup_dir.join("local.toml");
    let config = if !local_config_path.exists() {
        // create a new local config
        new_local_config(&from, &to, topics)
    } else {
        let metadata = std::fs::metadata(local_config_path.clone())?;
        let mut config = if metadata.len() == 0 {
            // if the 'local.toml' is an empty file, remove it
            std::fs::remove_file(&local_config_path)?;
            // create a new local config
            new_local_config(&from, &to, topics)
        } else {
            tracing::info!("read configuration in: {}", local_config_path.display());
            LocalConfig::from_path(&local_config_path)?
        };
        // update last modified time
        config.last_modified = Local::now();
        // check group id
        if let Some(group_id) = from.params.get("group.id") {
            if config.group_id != group_id.as_str() {
                if force {
                    tracing::warn!(
                        "group id not match(`{}` vs `{}` in last operation), but use it by force",
                        group_id,
                        config.group_id
                    );
                    config.group_id = group_id.clone();
                } else {
                    anyhow::bail!(
                        "group id not match: will use `{}` but it's `{}` in last operation",
                        group_id,
                        config.group_id
                    );
                }
            }
        }
        config
    };

    // update group id in DSN
    from.params
        .insert("group.id".to_string(), config.group_id.clone());

    let metrics_arc = get_metrics_arc(task_id.clone()).await;
    let metrics = metrics_arc.tmq();
    metrics.topics.fetch_add(config.topics.len() as _, SeqCst);

    tracing::info!("create TMQ builder");
    let tmq = TmqBuilder::from_dsn(&from)?;
    tracing::info!("write to config file");
    config.write_to(local_config_path.clone())?;
    tracing::info!("write to config file done");

    // move the current file to a new path
    match &move_to {
        Some(new_dir) => {
            let file_path = &local_config_path;
            if let Some(file_name) = file_path.file_name() {
                let new_path = new_dir.clone().join(file_name);
                tokio::fs::rename(file_path, new_path).await?;
            }
        }
        None => {
            // nothing
        }
    }

    let mut join_set = JoinSet::new();
    let mut consumer_task_id = 0;
    let (consumers_tx, mut consumers_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut files_manager = Vec::new();
    for topic in config.topics.iter() {
        if jobs == 0 && topic.vgroups == 0 {
            bail!("unknown vgroups, use a thread number larger than 0 with -j");
        }
        let jobs = if jobs == 0 || jobs > topic.vgroups {
            topic.vgroups
        } else {
            jobs
        };

        // 按照 jobs 的数量创建 consumer
        let mut consumers = Vec::with_capacity(jobs);
        tracing::info!("create {jobs} consumers for topic {}", topic.name);
        metrics.consumers.fetch_add(jobs as _, SeqCst);
        let mut consumer_handles = Vec::with_capacity(jobs);
        for id in 0..jobs {
            let mut consumer = tmq.build().await?;
            let topic = topic.name.clone();
            consumer_handles.push(tokio::spawn(async move {
                tracing::debug!("Subscribe consumer {id}");
                consumer.subscribe([&topic]).await.with_context(|| {
                    format!("Subscribe consumer [{id}] with topic `{topic}` error")
                })?;
                anyhow::Ok(consumer)
            }));
        }
        for h in consumer_handles {
            let consumer = h.await??;
            consumers.push(consumer);
        }

        let barrier = Arc::new(Barrier::new(jobs));
        let man = Arc::new(ZFileMan {
            api_version: crate::build::PKG_VERSION.to_owned(),
            server_version: version.clone(),
            backup_dir: backup_dir.to_owned(),
            topic: topic.name.clone(),
            ts: None,
            sync: tokio::sync::Mutex::new(()),
            writers: Default::default(),
            max_file_size,
            move_to: move_to.clone(),
            compression_level,
        });
        tracing::info!("zfile: {:?}", man);

        for _ in 0..jobs {
            let consumer = consumers.pop().unwrap();
            let barrier = barrier.clone();
            let man = man.clone();
            let cancel = cancel.clone();
            let sender = consumers_tx.clone();
            let offsets = offsets.clone();
            let stop_at_clone = stop_at.clone();
            join_set.spawn(
                backup(
                    sender,
                    consumer,
                    man,
                    consumer_task_id,
                    barrier,
                    cancel,
                    metrics_arc.clone(),
                    stop_at_clone,
                    offsets,
                    version.clone(),
                )
                .in_current_span(),
            );
            consumer_task_id += 1;
        }

        files_manager.push(man);
    }

    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res.map_err(anyhow::Error::from).and_then(|r| r) {
            tracing::error!("Task error: {err}");
            join_set.abort_all();
            return Err(err);
        }
    }

    tracing::info!("all consumers done");
    for man in files_manager {
        man.shutdown().await?;
    }
    tracing::info!("stop all consumers({})", consumer_task_id);
    for _ in 0..consumer_task_id {
        let _ = consumers_rx.recv().await;
    }
    tracing::info!("all workers done for backup");
    tracing::debug!("metrics: {}", metrics);
    Ok(())
}

/// parse stopAt parameter from dsn
fn parse_stop_at(dsn: &Dsn) -> Result<Option<StopAt>> {
    dsn.params
        .get("stopAt")
        .map(|s| StopAt::from_str(s).context(format!("failed to parse stopAt: {}", s)))
        .transpose()
}

fn parse_max_file_size(dsn: &Dsn) -> Result<Option<u64>> {
    dsn.params
        .get("max.file.size")
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u64>().map_err(|err| {
                anyhow::anyhow!("failed to parse max.file.size: {}, cause: {:?}", s, err)
            })
        })
        .transpose()
}

fn parse_move_to(dsn: &Dsn) -> Result<Option<PathBuf>> {
    let move_to = dsn.params.get("move.to").filter(|s| !s.is_empty());
    if move_to.is_none() {
        return Ok(None);
    }
    let move_to = move_to.unwrap();
    let path = Path::new(&move_to);
    if !path.exists() {
        bail!("the move.to: {} not exists", path.display())
    }
    Ok(Some(path.to_path_buf()))
}

fn parse_compression_level(dsn: &Dsn) -> Result<async_compression::Level> {
    let level = dsn.params.get("compression.level").and_then(|s| {
        if s.is_empty() {
            return None;
        }
        Some(s.to_string())
    });

    let level = level.unwrap_or("best".to_string()).to_lowercase();
    match level.as_str() {
        "fastest" => Ok(async_compression::Level::Fastest),
        "best" => Ok(async_compression::Level::Best),
        "default" => Ok(async_compression::Level::Default),
        _ => {
            let level = level.parse::<i32>().map_err(|err| {
                anyhow::anyhow!(
                    "failed to parse compress.level: {}, cause: {:?}",
                    level,
                    err
                )
            })?;
            Ok(async_compression::Level::Precise(level))
        }
    }
}

fn new_local_config(from: &Dsn, to: &Dsn, topics: Vec<Topic>) -> LocalConfig {
    let group_id = from
        .params
        .get("group.id")
        .cloned()
        .unwrap_or_else(|| group_id_hash_by(from, to));
    let client_id = from
        .params
        .get("client.id")
        .cloned()
        .unwrap_or("taosx".to_string());
    LocalConfig::new(topics, group_id, client_id)
}

#[instrument(skip_all)]
async fn backup(
    sender: tokio::sync::mpsc::UnboundedSender<Consumer>,
    consumer: Consumer,
    man: Arc<ZFileMan>,
    id: usize,
    barrier: Arc<Barrier>,
    cancel: CancellationToken,
    metrics_arc: Arc<CoreMetrics>,
    stop_at: Option<StopAt>,
    _offsets: Arc<DashMap<String, Vec<Assignment>>>,
    _version: String,
) -> Result<()> {
    let mut stream = consumer.stream();
    let mut rows = 0;
    let mut messages = 0;
    let metrics = metrics_arc.tmq();

    loop {
        select! {
            _ = cancel.cancelled() => {
                tracing::warn!("[sync: {id}] cancelled");
                break;
            }
            next = stream.try_next() => {
                if let Some((offset, message)) = next? {
                    metrics.add_messages(1);
                    let total = metrics.messages.load(SeqCst);
                    messages += 1;
                    if messages % 2000 == 0 {
                        tracing::info!("[{id}] received {messages} messages ({:.2})", messages as f64 / total as f64);
                    }
                    let vgroup = offset.vgroup_id();

                    // handle message
                    match message {
                        MessageSet::Meta(meta) => {
                            let raw = meta.as_raw_meta().await?;
                            man.write_vgroup_with_raw(vgroup, &raw, RawType::Meta).await?;
                            man.flush_vgroup(vgroup).await?;
                            metrics.add_messages_of_meta(1);
                            consumer.commit(offset).await?;
                        }
                        MessageSet::Data(data) => {
                            let raw = data.as_raw_data().await?;
                            man.write_vgroup_with_raw(vgroup, &raw, RawType::Data).await?;
                            man.flush_vgroup(vgroup).await?;
                            metrics.add_messages_of_data(1);

                            let (size, stop) = man.stop_at(vgroup, data, metrics, &stop_at).await?;
                            rows += size;
                            consumer.commit(offset).await?;
                            if stop {
                                break;
                            }
                        }
                        MessageSet::MetaData(_meta, data) => {
                            let raw = data.as_raw_data().await?;
                            man.write_vgroup_with_raw(vgroup, &raw, RawType::Both).await?;
                            man.flush_vgroup(vgroup).await?;
                            metrics.add_messages_of_data(1);

                            let (size, stop) = man.stop_at(vgroup, data, metrics, &stop_at).await?;
                            rows += size;
                            consumer.commit(offset).await?;
                            if stop {
                                break;
                            }
                        }
                    }
                } else {
                    tracing::info!("[sync: {id}] polling stopped");
                    break;
                }
            }
        }
    }

    barrier.wait().await;
    tracing::info!("[{id}] total backup {} rows", rows);
    drop(stream);
    let _ = sender.send(consumer);
    // consumer.unsubscribe().await;
    tracing::info!("[{id}] backup done");
    Ok(())
}
/*** DEPRECATED CODES START ***/

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
