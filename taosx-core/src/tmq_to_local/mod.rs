use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use taos::{sync::MessageSet, Consumer, *};
use tokio::sync::{Barrier, Mutex};
use tokio_util::sync::CancellationToken;

use crate::{
    taoz::ZFile,
    tmq::{check_tmq_dsn, StopAt, TmqMetrics, Topic},
};

use dashmap::DashMap;
use taos::taos_query::tmq::Assignment;

struct ZFileMan {
    path: PathBuf,
    // db: String,
    topic: String,
    sync: Mutex<()>,
    writers: dashmap::DashMap<i32, Mutex<ZFile>>,
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
            log::info!("Flush vgroup {}", entry.key());
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
                let file = ZFile::new(prefix, async_compression::Level::Best).await?;
                let _ = self.writers.insert(vgroup, Mutex::new(file));
            }
        }
        Ok(())
    }
    async fn write_vgroup_with_meta(
        &self,
        vgroup: i32,
        meta: taos::Meta,
        metrics: &Arc<TmqMetrics>,
    ) -> Result<()> {
        let raw = meta.as_raw_meta().await?;
        self.assert_vgroup(vgroup).await?;
        let entry = self.writers.get(&vgroup).expect("should always exist");
        entry.value().lock().await.write_meta(&raw).await?;

        metrics
            .messages_of_meta
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn write_vgroup_with_data(
        &self,
        vgroup: i32,
        data: taos::Data,
        metrics: &Arc<TmqMetrics>,
        stop_at: Option<DateTime<Local>>,
    ) -> Result<(usize, bool)> {
        // let raw = meta.as_raw_meta().await?;
        self.assert_vgroup(vgroup).await?;
        let entry = self.writers.get(&vgroup).expect("should always exist");
        let mut writer = entry.value().lock().await;
        writer.start_raw_block().await?;
        let mut nrows = 0;
        let mut last_ts = None;
        while let Some(block) = data.fetch_raw_block().await.unwrap() {
            // dbg!(&block);
            writer.write_raw_block(&block).await?;
            if let Some(view) = block.column_views().get(0) {
                match view {
                    ColumnView::Timestamp(view) => {
                        last_ts = view.iter().last().unwrap();
                    }
                    _ => unreachable!("expect first column is timestamp"),
                }
            }
            nrows += block.nrows();
            log::debug!(
                "[vg:{vgroup}] table {} rows: {}",
                block.table_name().unwrap_or_default(),
                block.nrows()
            );

            metrics
                .blocks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            metrics
                .records
                .fetch_add(block.nrows() as _, std::sync::atomic::Ordering::SeqCst);
            metrics.points.fetch_add(
                block.nrows() as u64 * block.ncols() as u64,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        writer.finish_raw_block().await?;

        let mut stop = false;
        match (stop_at, last_ts) {
            (Some(stop_at), Some(last_ts)) => {
                if last_ts.to_datetime_with_tz() >= stop_at {
                    stop = true;
                }
            }
            _ => (),
        }

        metrics
            .messages_of_data
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok((nrows, stop))
    }

    async fn flush_vgroup(&self, vgroup: i32) -> Result<()> {
        self.assert_vgroup(vgroup).await?;
        let entry = self.writers.get(&vgroup).expect("should always exist");
        entry.value().lock().await.flush().await?;

        Ok(())
    }
}

async fn backup(
    sender: tokio::sync::mpsc::UnboundedSender<Consumer>,
    consumer: Consumer,
    man: Arc<ZFileMan>,
    id: usize,
    barrier: Arc<Barrier>,
    cancel: CancellationToken,
    metrics: Arc<TmqMetrics>,
    stop_at: Option<DateTime<Local>>,
    offsets: Arc<DashMap<String, Vec<Assignment>>>,
) -> Result<()> {
    let mut stream = consumer.stream();
    let mut rows = 0;
    let mut messages = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                log::warn!("[sync: {id}] cancelled");
                break;
            }
            next = stream.try_next() => {
                let assignments = consumer.assignments().await.unwrap();
                log::debug!("assignment: {:?}", assignments);
                for (topic, assignment) in assignments {
                    offsets.insert(topic, assignment);
                }
                
                if let Some((offset, message)) = next? {
                    metrics
                        .messages
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let total = metrics.messages.load(std::sync::atomic::Ordering::SeqCst);
                    messages += 1;
                    if messages % 2000 == 0 {
                        log::info!("[{id}] received {messages} messages ({:.2})", messages as f64 / total as f64);
                    }
                    let vgroup = offset.vgroup_id();

                    match message {
                        MessageSet::Meta(meta) => {
                            man.write_vgroup_with_meta(vgroup, meta, &metrics).await?;

                            man.flush_vgroup(vgroup).await?;
                            consumer.commit(offset).await?;
                        }
                        MessageSet::Data(data) => {
                            let (size, stop) = man.write_vgroup_with_data(vgroup, data, &metrics, stop_at).await?;
                            rows += size;
                            man.flush_vgroup(vgroup).await?;
                            consumer.commit(offset).await?;
                            if stop {
                                break;
                            }
                        }
                        MessageSet::MetaData(meta, data) => {
                            // writer.write_meta(&meta.as_raw_meta().await?).await?;
                            man.write_vgroup_with_meta(vgroup, meta, &metrics).await?;
                            man.flush_vgroup(vgroup).await?;
                            let (size, stop) = man.write_vgroup_with_data(vgroup, data, &metrics, stop_at).await?;

                            man.flush_vgroup(vgroup).await?;
                            consumer.commit(offset).await?;
                            rows += size;
                            if stop {
                                break;
                            }
                        }
                    }
                } else {
                    log::info!("[sync: {id}] polling stopped");
                    break;
                }
            }
        }
    }

    barrier.wait().await;
    log::info!("[{id}] total backup {} rows", rows);
    drop(stream);
    let _ = sender.send(consumer);
    // consumer.unsubscribe().await;
    log::info!("[{id}] backup done");
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct LocalConfig {
    pub(crate) created_at: chrono::DateTime<Local>,
    pub(crate) last_modified: chrono::DateTime<Local>,
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

pub async fn tmq_to_local(
    from: Dsn,
    mut to: Dsn,
    jobs: usize,
    force: bool,
    cancel: CancellationToken,
    offsets: Arc<DashMap<String, Vec<Assignment>>>,
) -> Result<()> {
    let (mut from, builder, topics) = check_tmq_dsn(from).await?;

    #[cfg(not(feature = "disable-enterprise-only-validation"))]
    if !builder.is_enterprise_edition().await? {
        anyhow::bail!(
            "Only enterprise edition is supported. If it's not your case, please contact us."
        )
    }
    let mut from_params = from.drain_params();

    let stop_at = if let Some(stop_at) = from_params.remove("stopAt") {
        let ts = StopAt::from_str(&stop_at)
            .context(format!("Unsupported `stopAt` parameter: {stop_at}"))?;
        Some(ts.to_local_date_time())
    } else {
        None
    };
    // let (mut from, mut from_params) = from.split_params();
    let to_params = to.drain_params();

    if to.path.is_none() {
        anyhow::bail!(
            "invalid local backup dsn: {}\nPlease use a local path DSN like `local:./path/to/backup`",
            to
        );
    }
    let path: &Path = to.path.as_ref().unwrap().as_ref();
    if !path.exists() {
        log::info!("create directory for backup: {}", path.display());
        std::fs::create_dir_all(path)?;
    } else {
        log::info!("use existing directory for backup: {}", path.display());
    }

    let config_path = path.join("local.toml");
    let config = if config_path.exists() {
        log::info!("read configuration in: {}", config_path.display());
        let mut config = LocalConfig::from_path(&config_path)?;
        config.last_modified = Local::now();

        if let Some(group_id) = from_params.get("group.id") {
            if config.group_id != group_id.as_str() {
                if force {
                    log::warn!(
                        "group id not match(`{}` vs `{}` in last operation), but use it by force",
                        group_id,
                        config.group_id
                    );
                } else {
                    anyhow::bail!(
                        "group id not match: will use `{}` but it's `{}` in last operation",
                        group_id,
                        config.group_id
                    );
                }
            }
        } else {
            from_params.insert("group.id".to_string(), config.group_id.to_string());
        }
        config
    } else {
        let group_id = if let Some(group_id) = from_params.get("group.id") {
            group_id
        } else {
            use sha2::Digest;
            let mut hasher = Sha256::new();
            hasher.update(from.to_string());
            hasher.update(to.to_string());
            let id = hasher.finalize();
            let mut group_id = format!("x{:x}", id);
            group_id.truncate(12);
            from_params.insert("group.id".to_string(), group_id);
            from_params.get("group.id").unwrap()
        };
        let client_id = from_params
            .get("client.id")
            .map(|s| s.as_str())
            .unwrap_or(&"taosx");
        let config = LocalConfig::new(topics, group_id, client_id);
        config
    };

    from.params = from_params;
    to.params = to_params;

    let metrics = Arc::new(TmqMetrics {
        topics: config.topics.len(),
        ..Default::default()
    });

    let tmq = TmqBuilder::from_dsn(&from)?;
    log::info!("TMQ builder created");

    if to.path.is_none() {
        anyhow::bail!("invalid backup DSN: {}", to);
    }

    log::info!("write to config file");
    config.write_to(config_path)?;
    log::info!("write to config file done");

    let mut handles = Vec::new();

    let mut task_id = 0;
    let (consumers_sender, mut consumers_receiver) = tokio::sync::mpsc::unbounded_channel();

    let mut files_manager = Vec::new();

    for (_, topic) in config.topics.iter().enumerate() {
        if jobs == 0 && topic.vgroups == 0 {
            anyhow::bail!("unknown vgroups, use a thread number larger than 0 with -j");
        }
        let jobs = if jobs == 0 || jobs > topic.vgroups {
            topic.vgroups
        } else {
            jobs
        };

        let mut consumers = Vec::with_capacity(jobs);
        log::info!("create {jobs} consumers for topic {}", topic.name);
        metrics
            .workers
            .fetch_add(jobs as _, std::sync::atomic::Ordering::SeqCst);
        for _ in 0..jobs {
            let mut consumer = tmq.build().await?;
            consumer.subscribe([&topic.name]).await?;
            consumers.push(consumer);
        }

        let barrier = Arc::new(Barrier::new(jobs));

        let man = Arc::new(ZFileMan {
            path: path.to_owned(),
            // db: topic.database.clone(),
            topic: topic.name.clone(),
            sync: tokio::sync::Mutex::new(()),
            writers: Default::default(),
        });

        for _ in 0..jobs {
            let consumer = consumers.pop().unwrap();
            let barrier = barrier.clone();
            let man = man.clone();
            let cancel = cancel.clone();
            let metrics = metrics.clone();
            let sender = consumers_sender.clone();
            let offsets = offsets.clone();
            let handle = tokio::spawn(backup(
                sender, consumer, man, task_id, barrier, cancel, metrics, stop_at, offsets,
            ));
            handles.push(handle);
            task_id += 1;
        }

        files_manager.push(man);
    }
    for handle in handles {
        let _ = handle.await??;
        log::info!("worker done");
    }
    for man in files_manager {
        man.shutdown().await?;
    }
    log::info!("stop all consumers({})", task_id);
    for _ in 0..task_id {
        let _ = consumers_receiver.recv().await;
    }
    log::info!("all workers done for backup");

    println!("{}", metrics.as_ref());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tmq_to_local() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    pretty_env_logger::init();
    let taos = TaosBuilder::from_dsn("taos://")?.build().await?;
    taos.exec_many([
        "DROP TOPIC IF EXISTS tmq_to_local",
        "DROP DATABASE IF EXISTS tmq_to_local",
        "CREATE DATABASE tmq_to_local",
        "USE tmq_to_local",
        "CREATE STABLE stb1 (ts TIMESTAMP, v1 BOOL) TAGS(j1 json)",
        "CREATE TOPIC tmq_to_local WITH META AS DATABASE tmq_to_local",
    ])
    .await?;
    tmq_to_local(
        "tmq:///tmq_to_local".parse()?,
        "local:./tmq_to_local_out".parse()?,
        1,
        true,
        Default::default(),
        Default::default(),
    )
    .await?;
    std::fs::remove_dir_all("./tmq_to_local_out")?;
    Ok(())
}
