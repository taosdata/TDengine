use crate::tmq_to_local::conf::{
    BackupConfig, BackupConfigBuilder, BackupConsumer, BackupPointGenMode,
};
use crate::{
    core_metrics::{get_metrics_arc, CoreMetrics, TaskMetrics},
    tmq::tmq_metric::TmqMetrics,
};
use crate::{
    taoz::{RawType, ZFile},
    tmq::*,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Local, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Debug;
use std::sync::atomic::Ordering::SeqCst;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use taos::taos_query::tmq::Assignment;
use taos::*;
use taos_query::common::RawData;
use tokio::select;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

mod conf;

struct ZFileMan {
    api_version: String,
    server_version: String,
    path: PathBuf,
    topic: String,
    sync: tokio::sync::Mutex<()>,
    writers: DashMap<i32, tokio::sync::Mutex<ZFile>>,
    max_file_size: u64,
    move_to: Option<PathBuf>,
    compression_level: async_compression::Level,
}

impl Debug for ZFileMan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZFileMan")
            .field("api_version", &self.api_version)
            .field("server_version", &self.server_version)
            .field("path", &self.path)
            .field("topic", &self.topic)
            .field("max_file_size", &self.max_file_size)
            .field("move_to", &self.move_to)
            .field("compress_level", &self.compression_level)
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
                let prefix = self.path.join(format!("{}-{}", self.topic, vgroup));
                let mut file = ZFile::new(
                    prefix,
                    self.compression_level,
                    &self.api_version,
                    &self.server_version,
                )
                .await?;
                file.set_max_file_size(self.max_file_size);
                file.set_move_to(self.move_to.clone());
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
        self.assert_vgroup(vgroup).await?;
        let entry = self.writers.get(&vgroup).expect("should always exist");
        let mut writer = entry.value().lock().await;
        writer.start_raw_block().await?;
        let mut nrows = 0;
        let mut last_ts = None;
        while let Some(block) = data.fetch_raw_block().await.unwrap() {
            // dbg!(&block);
            writer.write_raw_block(&block).await?;
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
        writer.finish_raw_block().await?;

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
        entry.value().lock().await.flush().await?;

        Ok(())
    }
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
    consumer: BackupConsumer,
    man: Arc<ZFileMan>,
    cancel: CancellationToken,
}

impl BackupWorker {
    async fn run(&self) -> Result<()> {
        self.wait_for_upcoming().await?;
        tracing::info!("tmq_to_local worker: {} start", self.id);
        let run_impl = self.run_impl();
        loop {
            select! {
                _ = self.cancel.cancelled() => {
                    tracing::warn!("tmq_to_local worker: {} cancelled", self.id);
                    break;
                }
                res = run_impl => {
                    match res {
                        Ok(_) => {
                            tracing::info!("tmq_to_local worker: {} completed", self.id);
                            break;
                        }
                        Err(err) => {
                            tracing::error!("tmq_to_local worker: {} exit with error: {:#}", self.id, err);
                            bail!(err);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn wait_for_upcoming(&self) -> Result<()> {
        Self::wait_for_upcoming_impl(self.config.upcoming.clone()).await
    }

    async fn wait_for_upcoming_impl(upcoming: Option<DateTime<Utc>>) -> Result<()> {
        if let Some(upcoming) = upcoming {
            let now = Utc::now();
            if now < upcoming.clone() {
                let duration = upcoming - now;
                tracing::info!("tmq_to_local worker wait for upcoming: {}", upcoming);
                tokio::time::sleep(duration.to_std().map_err(|err| {
                    anyhow::Error::from(err)
                        .context(format!("failed to convert: {:?} to std duration", duration))
                })?)
                .await;
            }
        }
        Ok(())
    }

    async fn run_impl(&self) -> Result<()> {
        let mut stream = self.consumer.consumer.stream();

        let end_offset = self.consumer.end_offset;

        while let Some((offset, message)) = stream.try_next().await? {
            let vg_id = offset.vgroup_id();

            // handle message
            match message {
                MessageSet::Meta(meta) => {
                    let raw = meta.as_raw_meta().await?;
                    self.man
                        .write_vgroup_with_raw(vg_id, &raw, RawType::Meta)
                        .await?;
                    self.man.flush_vgroup(vg_id).await?;
                }
                MessageSet::Data(data) => {
                    let raw = data.as_raw_data().await?;
                    self.man
                        .write_vgroup_with_raw(vg_id, &raw, RawType::Data)
                        .await?;
                    self.man.flush_vgroup(vg_id).await?;
                }
                MessageSet::MetaData(_meta, data) => {
                    let raw = data.as_raw_data().await?;
                    self.man
                        .write_vgroup_with_raw(vg_id, &raw, RawType::Both)
                        .await?;
                    self.man.flush_vgroup(vg_id).await?;
                }
            }
            self.consumer.consumer.commit(offset).await?;

            // 通过 topic, vg_id 可以获取到当前的 offset
            if self.config.backup_point_gen_mode == BackupPointGenMode::ByOffset {
                assert_eq!(vg_id, self.consumer.vgroup_id);

                let cur_offset = self
                    .consumer
                    .consumer
                    .position(self.consumer.topic.as_str(), vg_id)
                    .await?;
                if cur_offset == end_offset {
                    break;
                }
            }
        }

        Ok(())
    }
}

/// full or incremental backup from TMQ to local file
/// @param from: DSN of TMQ
/// @param to: DSN of local
/// @param jobs: 子任务的数量，如果
/// @param force: 如果 force 为 true，表示强制使用 from 中配置的 group_id；如果 force 为 false，
/// 检查 from 中的 group_id 是否和上次备份的 group_id 一致，不一致则报错。
/// @param cancel: 取消信号，用于取消备份任务
/// @param task_id: task id
pub async fn tmq_to_local(
    from: Dsn,
    to: Dsn,
    _jobs: usize,
    _force: bool,
    cancel: CancellationToken,
    task_id: Option<String>,
) -> Result<()> {
    tracing::info!(
        "tmq_to_local task: {:?}, from: {}, to: {}",
        task_id,
        from,
        to
    );

    let config = BackupConfigBuilder::new(task_id.clone(), &from, &to)
        .build()
        .await
        .context(format!(
            "parse backup config error, from: {}, to: {}",
            &from, &to
        ))?;

    if config.is_initial_backup().await? {
        // 在 TDengine 创建备份的 topic
        config.create_topic().await?;
        // 在本地创建备份目录
        config.create_backup_dir().await?;
    }

    let consumers = config.create_consumer().await?;

    let man = Arc::new(ZFileMan {
        api_version: crate::build::PKG_VERSION.to_owned(),
        server_version: config.server_version.clone(),
        path: config.backup_dir.clone(),
        topic: config.topic.clone(),
        sync: tokio::sync::Mutex::new(()),
        writers: Default::default(),
        max_file_size: config.backup_max_size,
        move_to: config.move_to.clone(),
        compression_level: config.backup_comp_level.clone(),
    });

    // 订阅 topic，按照 jobs 的数量创建 TmqConsumer, 每个 TmqConsumer 一个线程，负责从 Tmq 拉取数据，
    // 发送给 LocalWriter，LocalWriter 负责将数据写入本地文件
    let mut join_set = JoinSet::new();
    for (idx, consumer) in consumers.into_iter().enumerate() {
        let task = BackupWorker {
            id: idx,
            config: config.clone(),
            consumer,
            man: man.clone(),
            cancel: cancel.clone(),
        };
        join_set.spawn(async move { task.run().await });
    }

    // 等待所有 worker 完成
    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res.map_err(anyhow::Error::from).and_then(|r| r) {
            tracing::error!("abort all workers since error: {:#}", err);
            join_set.abort_all();
            // TODO: 清理文件
            return Err(err);
        }
    }

    tracing::info!("tmq_to_local task: {:?} finished", task_id);
    Ok(())
}

#[allow(unused)]
pub async fn tmq_to_local_v1(
    from: Dsn,
    to: Dsn,
    jobs: usize,
    force: bool,
    cancel: CancellationToken,
    task_id: Option<String>,
) -> Result<()> {
    let (mut from, builder, topics, _, _) = check_tmq_dsn(from).await?;

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
            path: backup_dir.to_owned(),
            topic: topic.name.clone(),
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
        .map(|s| {
            StopAt::from_str(s)
                .map_err(|err| anyhow::anyhow!("failed to parse stopAt: {}, cause: {:?}", s, err))
        })
        .transpose()
}

fn parse_max_file_size(dsn: &Dsn) -> Result<Option<u64>> {
    let max_file_size = dsn.params.get("max.file.size").and_then(|s| {
        if s.is_empty() {
            return None;
        }
        Some(s.to_string())
    });

    max_file_size
        .map(|s| {
            s.parse::<u64>().map_err(|err| {
                anyhow::anyhow!("failed to parse max.file.size: {}, cause: {:?}", s, err)
            })
        })
        .transpose()
}

fn parse_move_to(dsn: &Dsn) -> Result<Option<PathBuf>> {
    let move_to = dsn.params.get("move.to").and_then(|s| {
        if s.is_empty() {
            return None;
        }
        Some(s.to_string())
    });

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
        .unwrap_or_else(|| generate_group_id(from, to));
    let client_id = from
        .params
        .get("client.id")
        .cloned()
        .unwrap_or("taosx".to_string());
    LocalConfig::new(topics, group_id, client_id)
}

/// generate group.id by sha256(from + to)
fn generate_group_id(from: &Dsn, to: &Dsn) -> String {
    let mut hasher = Sha256::new();
    hasher.update(from.to_string());
    hasher.update(to.to_string());
    let id = hasher.finalize();
    let mut group_id = format!("x{:x}", id);
    group_id.truncate(12);
    group_id
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

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::EnvFilter;

    #[tokio::test]
    async fn test_wait_for_upcoming() {
        let now = Utc::now();
        BackupWorker::wait_for_upcoming_impl(Some(now + chrono::Duration::seconds(2)))
            .await
            .unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 2);

        let now = Utc::now();
        BackupWorker::wait_for_upcoming_impl(None).await.unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 0);

        let now = Utc::now();
        BackupWorker::wait_for_upcoming_impl(Some(now - chrono::Duration::days(1)))
            .await
            .unwrap();
        let current = Utc::now();
        assert_eq!(current.timestamp() - now.timestamp(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tmq_to_local_with_taos() {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse().unwrap()))
            .with_file(true)
            .pretty()
            .try_init()
            .unwrap();

        // let addr = "tmq:///";
        let addr = "tmq+http://192.168.0.201:6041";
        let database = "test_tmq_to_local_with_taos";
        let back_dir = "/tmp/test_tmq_to_local_with_taos";

        std::fs::create_dir_all(back_dir).unwrap();

        let taos = TaosBuilder::from_dsn(format!("{addr}"))
            .unwrap()
            .build()
            .await
            .unwrap();

        let from = format!("{addr}/{database}?").into_dsn().unwrap();
        let to = format!("local:{back_dir}").into_dsn().unwrap();

        let config = BackupConfigBuilder::new(None, &from, &to)
            .build()
            .await
            .unwrap();

        let topic = config.topic.clone();

        taos.exec_many([
            format!("DROP TOPIC IF EXISTS `{topic}`"),
            format!("DROP DATABASE IF EXISTS `{database}`"),
            format!("CREATE DATABASE `{database}` wal_retention_period 3600"),
            format!("CREATE STABLE `{database}`.`Stb` (ts TIMESTAMP, f1 double) TAGS(t1 int)"),
        ])
        .await
        .unwrap();

        let writer = tokio::spawn(async move {
            for table_idx in 0..10 {
                for idx in 0..10 {
                    let sql = format!(
                        "INSERT INTO `{database}`.`Tb{table_idx}` USING `{database}`.`Stb` tags({table_idx}) VALUES (now+{idx}s, {idx}.{idx})",
                    );
                    taos.exec(sql).await.unwrap();
                }
            }
        });

        let backup = tokio::spawn(async move {
            tmq_to_local(from, to, 1, true, Default::default(), None)
                .await
                .unwrap();
        });

        writer.await.unwrap();
        backup.await.unwrap();

        // clean up
    }
}
