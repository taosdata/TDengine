use std::{path::Path, sync::Arc};

use anyhow::{bail, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use taos::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{taoz::ZCodec, Compression};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Topic {
    name: String,
    database: String,
    sql: String,
    vgroups: usize,
}

async fn restore(
    id: usize,
    path: impl AsRef<Path>,
    taos: Taos,
    sem: OwnedSemaphorePermit,
) -> Result<()> {
    let path = path.as_ref();
    log::info!("[{}] restore with file: {:?}", id, path.display());
    let reader = tokio::fs::File::open(path).await?;
    let reader = tokio::io::BufReader::new(reader);
    let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);
    let mut reader = ZCodec::new(reader);
    let header = reader.header_async().await?;
    log::debug!("parse header: {:?}", header);

    // let mut rows = AtomicU64::new(0);
    let mut rows = 0;

    loop {
        let res = reader.read_message_async().await;
        match res {
            Ok(message) => match message {
                MessageSet::Meta(meta) => {
                    // dbg!(&meta);
                    taos.write_raw_meta(meta).await?
                }
                MessageSet::Data(data) => {
                    // dbg!(&data);
                    for raw in data {
                        rows += raw.nrows();
                        taos.write_raw_block(&raw).await?;
                    }
                    log::debug!("rows: {}", rows);
                    // taos.write_raw_data(data[0]).await?
                }
            },
            Err(err) => {
                // dbg!(&err);
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                dbg!(&err);
                break;
            }
        }
    }
    let mut zo = path.to_path_buf();
    zo.set_extension("zo");
    tokio::fs::write(zo, "").await?;
    drop(sem);
    drop(taos);
    drop(reader);

    // barrier.wait().await;
    log::info!("[{}] totally write {} rows", id, rows);
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

pub async fn local_to_taos(from: Dsn, to: Dsn, jobs: usize, force: bool) -> Result<()> {
    if from.fragment.is_none() {
        anyhow::bail!(
            "invalid local dsn: {}\nPlease use a local path DSN like `local:./path/to/backup`",
            from
        );
    }
    let continuous = from.params.contains_key("continue");
    let path: &Path = from.fragment.as_ref().unwrap().as_ref();
    if !path.exists() {
        anyhow::bail!("invalid backup dsn `{}`: directory not exist", from);
    }
    let config_path = path.join("local.toml");
    if !config_path.exists() {
        anyhow::bail!(
            "invalid backup location: config file `{}` not found",
            config_path.display()
        );
    }

    let config = LocalConfig::from_path(&config_path)?;

    // check database
    if let Some(target) = to.database.as_ref() {
        for topic in &config.topics {
            if &topic.database != target {
                if force {
                    log::warn!("restore from {} to {} by force", topic.database, target);
                } else {
                    bail!("to restore from {} to a different database {}, please use --yes-i-really-mean-it", topic.database, target);
                }
            }
        }
    }

    let target = TaosBuilder::from_dsn(&to)?;
    let global_taos = target.build()?;

    let mut handles = Vec::new();
    let jobs = if jobs == 0 { 16 } else { jobs };
    let task_sem = Arc::new(Semaphore::new(jobs));
    // let barrier = Arc::new(Barrier::new(jobs));

    let mut task_id = 0;
    for topic in &config.topics {
        if !global_taos.database_exists(&topic.database).await? {
            global_taos
                .exec(
                    topic
                        .sql
                        .replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS"),
                )
                .await?;
        } else if !force {
            anyhow::bail!(
                "the database has already exists, please be sure to override it by force"
            );
        }

        let mut dir_entry = tokio::fs::read_dir(path).await?;
        while let Some(path) = dir_entry.next_entry().await? {
            let file_name = path.file_name().into_string().unwrap();
            if !file_name.starts_with(&topic.name) || !file_name.ends_with("z") {
                continue;
            }
            if continuous {
                let mut zo = path.path();
                zo.set_extension("zo");
                if zo.exists() {
                    continue;
                }
            }

            let sem = task_sem.clone().acquire_owned().await?;
            let taos = target.build()?;
            if to.database.is_none() {
                taos.exec(format!("use {}", topic.database)).await?;
            }
            // let barrier = barrier.clone();
            let handle =
                tokio::spawn(async move { restore(task_id, path.path(), taos, sem).await });
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
async fn test() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    pretty_env_logger::init();
    let out = Path::new("local_to_taos_out");
    if out.exists() {
        std::fs::remove_dir_all(out)?;
    }
    let local: Dsn = format!("local:./{}", out.display()).parse()?;
    let taos = TaosBuilder::from_dsn("taos://")?.build()?;
    taos.exec_many([
        "DROP TOPIC IF EXISTS local_to_taos",
        "DROP DATABASE IF EXISTS local_to_taos",
        "CREATE DATABASE local_to_taos",
        "USE local_to_taos",
        "CREATE STABLE stb1 (ts TIMESTAMP, v1 BOOL) TAGS(j1 json)",
        "CREATE TABLE tb1 USING stb1 TAGS('{\"id\":\"1\"}')",
        "INSERT INTO tb1 VALUES (now, true) (now+1s, false) (now+2s, NULL)",
        "CREATE TOPIC local_to_taos WITH META AS DATABASE local_to_taos",
    ])
    .await?;
    crate::tmq_to_local("tmq:///local_to_taos".parse()?, local.clone(), 1, true).await?;

    taos.exec_many([
        "DROP TOPIC local_to_taos",
        "DROP DATABASE local_to_taos",
        "CREATE DATABASE local_to_taos",
    ])
    .await?;

    local_to_taos(local.clone(), "taos:///".parse()?, 1, true).await?;

    let count: usize = taos.query_one("SELECT count(*) from tb1").await?.unwrap();
    assert_eq!(count, 3, "restored");

    std::fs::remove_dir_all(out)?;

    taos.exec_many(["DROP DATABASE local_to_taos"]).await?;

    Ok(())
}
