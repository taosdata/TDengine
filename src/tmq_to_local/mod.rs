use std::path::Path;

use anyhow::Result;
use chrono::Local;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use taos::{Consumer, *};

use crate::{
    taoz::ZFile,
    tmq::{check_tmq_dsn, Topic},
};

// #[derive(Debug, Deserialize, Serialize)]
// pub(crate) struct Topic {
//     name: String,
//     database: String,
//     vgroups: usize,
//     sql: String,
//     /// the topic is for single table
//     table: Option<String>,
// }

async fn backup(consumer: Consumer, mut writer: ZFile, id: usize) -> Result<()> {
    let mut stream = consumer.stream();
    let mut rows = 0;

    while let Some((offset, message)) = stream.try_next().await? {
        match message {
            MessageSet::Meta(meta) => {
                //dbg!(meta.as_json_meta().await?);
                writer.write_meta(&meta.as_raw_meta().await?).await?;
            }
            MessageSet::Data(data) => {
                writer.start_raw_block().await?;
                while let Some(block) = data.fetch_raw_block().await.unwrap() {
                    // dbg!(&block);
                    writer.write_raw_block(&block).await?;
                    rows += block.nrows();
                    log::info!(
                        "[{id}] table {} rows: {}",
                        block.table_name().unwrap_or_default(),
                        block.nrows()
                    );
                }
                writer.finish_raw_block().await?;
            }
        }
        writer.flush().await?;
        consumer.commit(offset).await?;
    }
    writer.shutdown().await?;
    log::info!("[{id}] total backup {} rows", rows);
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
        let bytes = toml::to_vec(self)?;
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

pub async fn tmq_to_local(from: Dsn, mut to: Dsn, jobs: usize, force: bool) -> Result<()> {
    let (mut from, topics) = check_tmq_dsn(from).await?;
    let mut from_params = from.drain_params();

    // let (mut from, mut from_params) = from.split_params();
    let to_params = to.drain_params();

    if to.fragment.is_none() {
        anyhow::bail!(
            "invalid local backup dsn: {}\nPlease use a local path DSN like `local:./path/to/backup`",
            to
        );
    }
    let path: &Path = to.fragment.as_ref().unwrap().as_ref();
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

    let tmq = TmqBuilder::from_dsn(&from)?;
    log::info!("TMQ builder created");

    if to.fragment.is_none() {
        anyhow::bail!("invalid backup DSN: {}", to);
    }

    log::info!("write to config file");
    config.write_to(config_path)?;
    log::info!("write to config file done");

    let mut handles = Vec::new();

    let mut task_id = 0;

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
        for _ in 0..jobs {
            let mut consumer = tmq.build()?;
            consumer.subscribe([&topic.name]).await?;
            consumers.push(consumer);
        }

        for id in 0..jobs {
            let consumer = consumers.pop().unwrap();
            let prefix = path.join(format!("{}-{}", topic.name, id));
            log::info!("start with {}", prefix.display());
            let file = ZFile::new(prefix, async_compression::Level::Best).await?;
            let handle = tokio::spawn(async move { backup(consumer, file, task_id).await });
            handles.push(handle);
            task_id += 1;
        }
    }
    for handle in handles {
        handle.await??;
    }
    Ok(())
}

#[tokio::test]
async fn test_tmq_to_local() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    pretty_env_logger::init();
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
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
    )
    .await?;
    Ok(())
}
