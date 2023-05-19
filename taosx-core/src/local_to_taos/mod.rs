use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use taos::*;
use tokio::sync::Semaphore;

use crate::{taoz::ZCodec, tmq_to_local::LocalConfig};

#[async_backtrace::framed]
async fn restore(
    id: usize,
    path: impl AsRef<Path>,
    taos: &Taos,
    table: Option<&str>,
) -> Result<()> {
    let path = path.as_ref();
    log::info!("[{}] restore with file: {:?}", id, path.display());
    let reader = tokio::fs::File::open(path).await?;
    let reader = tokio::io::BufReader::new(reader);
    let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);
    let mut reader = ZCodec::new(reader);
    let header = reader.header_async().await?;
    log::debug!("[{id}] parse header: {:?}", header);
    let mut rows = 0;

    loop {
        let res = reader.read_message_async().await;
        match res {
            Ok(message) => match message {
                MessageSet::Meta(meta) => {
                    // dbg!(&meta);
                    if let Err(err) = taos.write_raw_meta(&meta).await {
                        let err_str = err.to_string();
                        if err_str.contains("0x032C") {
                            log::warn!("found error 0x032C, retry once");
                            tokio::time::sleep(Duration::from_nanos(100)).await;
                            taos.write_raw_meta(&meta).await?;
                        } else if err_str.contains("0x2603") {
                            log::warn!("found error 0x2603, retry once");
                            taos.write_raw_meta(&meta).await?;
                        } else {
                            Err(err).context("create table error with write_raw")?;
                        }
                    };
                }
                MessageSet::Data(data) => {
                    for mut raw in data {
                        if let Some(name) = table {
                            raw.with_table_name(name);
                        }
                        rows += raw.nrows();
                        if let Err(err) = taos.write_raw_block(&raw).await {
                            if err.to_string().contains("[0x2603]") {
                                // table not exists
                                if let Some(meta) = raw.to_create() {
                                    if let Err(err) = taos.exec(format!("{}", meta)).await {
                                        if err.to_string().contains("0x032C") {
                                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                                        } else {
                                            Err(err).context("create table error")?;
                                        }
                                    };
                                    taos.write_raw_block(&raw)
                                        .await
                                        .context("write_raw block error")?;
                                } else {
                                    Err(err).context("write_raw block error")?;
                                }
                            } else {
                                Err(err).context("write raw block error")?;
                            }
                        };
                    }
                    log::debug!("[{id}] current rows: {}", rows);
                    // taos.write_raw_data(data[0]).await?
                }
                _ => unreachable!(),
            },
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::info!("[{id}] reading file {} done", path.display());
                    break;
                }
                log::debug!("[{id}] Reading data error: {}", &err);
                break;
            }
        }
    }
    let mut zo = path.to_path_buf();
    zo.set_extension("zo");
    tokio::fs::write(zo, "").await?;
    drop(reader);

    log::info!(
        "[{}] totally write {} rows from file {}",
        id,
        rows,
        path.display()
    );
    Ok(())
}

#[tracing::instrument]
#[async_backtrace::framed]
pub async fn local_to_taos(mut from: Dsn, mut to: Dsn, jobs: usize, force: bool) -> Result<()> {
    if from.path.is_none() {
        anyhow::bail!(
            "invalid local dsn: {}\nPlease use a local path DSN like `local:./path/to/backup`",
            from
        );
    }
    let continuous = from
        .params
        .remove("continue")
        .map(|s| s.is_empty() || s.to_lowercase() == "true")
        .unwrap_or(false);
    let path: &Path = from.path.as_ref().unwrap().as_ref();
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
    if let Some(target) = to.subject.as_mut() {
        let databases: Vec<_> = config
            .topics
            .iter()
            .map(|t| t.database.as_str())
            .dedup()
            .collect();

        if databases.len() > 1 {
            bail!("taosx does not support restore data from more than one databases to a single database");
        }

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

    let target_database = to.subject.take();
    let target = TaosBuilder::from_dsn(&to)?;
    let global_taos = target.build().await?;

    #[cfg(not(feature = "disable-enterprise-only-validation"))]
    if !target.is_enterprise_edition().await? {
        bail!("Only enterprise edition is supported. If it's not your case, please contact us.")
    }

    let mut handles = Vec::new();
    let jobs = if jobs == 0 { 16 } else { jobs };
    let task_sem = Arc::new(Semaphore::new(jobs));
    // let barrier = Arc::new(Barrier::new(jobs));

    let mut task_id = 0;
    for topic in &config.topics {
        if let Some(target) = target_database.as_ref() {
            if !global_taos.database_exists(&target).await? {
                log::info!(
                    "target database not exist, create database `{target}` with the same parameter in the backup"
                );
                if let Some(sql) = topic.database_sql.as_deref() {
                    let mut sql = sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS");
                    if &topic.database != target {
                        sql = sql.replace(&format!("`{}`", topic.database), &format!("`{target}`"));
                    }
                    global_taos.exec(sql).await?;
                }
            } else if !force {
                anyhow::bail!(
                    "the database has already exists, please be sure to override it by force"
                );
            }
        } else {
            if !global_taos.database_exists(&topic.database).await? {
                if let Some(sql) = topic.database_sql.as_deref() {
                    global_taos
                        .exec(sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS"))
                        .await?;
                }
            } else if !force {
                anyhow::bail!(
                    "the database has already exists, please be sure to override it by force"
                );
            }
        }
        if let Some(table) = topic.table.as_ref() {
            // schema rebuild
            let taos = target.build().await?;
            if let Some(target) = target_database.as_ref() {
                taos.exec(format!("use `{}`", target)).await?;
            } else {
                taos.exec(format!("use `{}`", topic.database)).await?;
            }

            if let Some(sql) = table.stable_sql.as_deref() {
                taos.exec(sql.replace("CREATE STABLE", "CREATE STABLE IF NOT EXISTS"))
                    .await?;
            }
            taos.exec(
                table
                    .table_sql
                    .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"),
            )
            .await?;
        }

        let mut dir_entry = tokio::fs::read_dir(path).await?;

        let mut files: BTreeMap<i64, BTreeMap<i64, tokio::fs::DirEntry>> = BTreeMap::new();

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
            let file_name_only = path.path().with_extension("");
            let items = file_name_only
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .split("-")
                .collect_vec();

            let (ts, vgroup) = items.iter().rev().take(2).collect_tuple().unwrap();
            let vgroup: i64 = vgroup.parse().unwrap();
            let ts: i64 = ts.parse().unwrap();

            if files.contains_key(&vgroup) {
                files.get_mut(&vgroup).unwrap().insert(ts, path);
            } else {
                let mut map = BTreeMap::new();
                map.insert(ts, path);
                files.insert(vgroup, map);
            }
        }

        for (_, files) in files {
            let sem = task_sem.clone().acquire_owned().await?;
            let taos = target.build().await?;
            if let Some(target) = target_database.as_ref() {
                taos.exec(format!("use `{}`", target)).await?;
            } else {
                taos.exec(format!("use `{}`", topic.database)).await?;
            }

            let table = topic.table.as_ref().map(|t| t.table.clone());
            let handle = tokio::spawn(async move {
                for (_, path) in files {
                    let res = restore(task_id, path.path(), &taos, table.as_deref()).await;
                    if res.is_err() {
                        drop(sem);
                        return res;
                    }
                }

                drop(sem);
                Ok(())
            });
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
    let taos = TaosBuilder::from_dsn("taos://")?.build().await?;
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
    crate::tmq_to_local(
        "tmq:///local_to_taos".parse()?,
        local.clone(),
        1,
        true,
        Default::default(),
        Default::default(),
    )
    .await?;

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
