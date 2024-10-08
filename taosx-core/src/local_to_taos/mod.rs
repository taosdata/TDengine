use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use taos::*;
use taosx_ipc::types::dsv::DataSourceValidation;
use tokio::sync::Semaphore;

use crate::{
    taoz::{ZCodec, ZMessage},
    tmq_to_local::LocalConfig,
    utils::constants::{VERSION_3_0_0, VERSION_3_3_0},
};

#[async_backtrace::framed]
async fn restore(
    id: usize,
    path: impl AsRef<Path>,
    taos: &Taos,
    table: Option<&str>,
) -> Result<()> {
    let path = path.as_ref();
    tracing::info!("[{}] restore with file: {:?}", id, path.display());
    let reader = tokio::fs::File::open(path).await?;
    let reader = tokio::io::BufReader::new(reader);
    let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);
    let mut reader = ZCodec::new(reader);
    let header = reader.header_async().await?;
    tracing::debug!("[{id}] parse header: {:?}", header);

    let target_version = taos
        .server_version()
        .await
        .context("get server version error")?;

    let target_version = semver::Version::parse(&target_version.split('.').take(3).join("."))?;
    if target_version < VERSION_3_0_0 {
        bail!("Backup source version is 3.3.0 or later, but target version is earlier than 3.3.0, which is not supported.");
    }
    if let Some(source_version) = header.server_version() {
        let source_version = semver::Version::parse(&source_version.split('.').take(3).join("."))?;
        if source_version >= VERSION_3_3_0 && target_version < VERSION_3_3_0 {
            bail!("Backup source version is 3.3.0 or later, but target version is earlier than 3.3.0, which is not supported.");
        }
    }

    let mut rows = 0;
    loop {
        let res = reader.read_message_async().await;
        match res {
            Ok(message) => {
                match message {
                    ZMessage::Meta(meta) => {
                        // dbg!(&meta);
                        if let Err(err) = taos.write_raw_meta(&meta).await {
                            let code: i32 = err.code().into();
                            match code {
                                0x0603 => {
                                    tracing::debug!("Table already exists");
                                }
                                0x032C | 0x0115 | 0x03C7 | 0x03D3 => {
                                    tracing::debug!("Found recoverable error: {err:#}, retry once");
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                    let res = taos.write_raw_meta(&meta).await;
                                    if res.is_ok() {
                                        tracing::debug!("Retry success");
                                    } else {
                                        tracing::debug!(
                                            "Retry failed: {:#}, continue",
                                            res.unwrap_err()
                                        );
                                    }
                                }
                                0x2603 => {
                                    tracing::debug!("Found 0x2603 error: {err:#}, retry once");
                                    taos.write_raw_meta(&meta)
                                        .await
                                        .context("restore meta error")?;
                                }
                                _ => {
                                    Err(err).context("write raw error while restore")?;
                                }
                            }
                        };
                    }
                    ZMessage::Data(data) => {
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
                        tracing::debug!("[{id}] current rows: {}", rows);
                    }
                    ZMessage::Raw(raw_type, raw) => {
                        let meta = raw.into();
                        if let Err(err) = taos.write_raw_meta(&meta).await {
                            let code: i32 = err.code().into();
                            match code {
                                0x032C | 0x0115 | 0x0603 | 0x03C7 | 0x03D3 => {
                                    tracing::debug!(raw.r#type = ?raw_type, "Found recoverable error: {}", err);
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                    let _ = taos.write_raw_meta(&meta).await;
                                }
                                0x2603 => {
                                    let mut tries = 0;
                                    let max_retries = 3;
                                    loop {
                                        tracing::debug!(raw.r#type = ?raw_type, "Found 0x2603 error: {}, retry", err);
                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                        match taos.write_raw_meta(&meta).await {
                                            Ok(_) => break,
                                            Err(err) => {
                                                if tries >= max_retries {
                                                    Err(err).with_context(|| {
                                                        format!(
                                                            "write raw({:?}) error while restore",
                                                            raw_type
                                                        )
                                                    })?;
                                                }
                                                tries += 1;
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    Err(err).context("write raw error while restore")?;
                                }
                            }
                        };
                    }
                }
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    tracing::info!("[{id}] reading file {} done", path.display());
                    break;
                }
                tracing::debug!("[{id}] Reading data error: {}", &err);
                break;
            }
        }
    }
    let mut zo = path.to_path_buf();
    zo.set_extension("zo");
    tokio::fs::write(zo, "").await?;
    drop(reader);

    tracing::info!(
        "[{}] totally write {} rows from file {}",
        id,
        rows,
        path.display()
    );
    Ok(())
}

#[tracing::instrument]
#[async_backtrace::framed]
pub async fn local_to_taos(from: Dsn, mut to: Dsn, jobs: usize, force: bool) -> Result<()> {
    // local dir
    let local_dir = from
        .path
        .as_ref()
        .map(PathBuf::from)
        .ok_or(anyhow::anyhow!(
            "invalid local dsn: {}, Please use a local path DSN like `local:./path/to/backup`",
            from
        ))?;
    if !local_dir.exists() {
        bail!("local path: {} not found", from);
    }

    // local.toml
    let local_toml_path = local_dir.join("local.toml");
    if !local_toml_path.exists() {
        bail!("local config: {} not found", local_toml_path.display());
    }
    // LocalConfig
    let config = LocalConfig::from_path(&local_toml_path)?;

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
                    tracing::warn!("restore from {} to {} by force", topic.database, target);
                } else {
                    bail!("to restore from {} to a different database {}, please use --yes-i-really-mean-it", topic.database, target);
                }
            }
        }
    }

    // parameters
    let continuous = from
        .params
        .get("continue")
        .map(|s| s.is_empty() || s.to_lowercase() == "true")
        .unwrap_or(false);

    let target_database = to.subject.take();
    let target = TaosBuilder::from_dsn(&to)?;
    let global_taos = target.build().await?;

    let mut handles = Vec::new();
    let jobs = if jobs == 0 { 16 } else { jobs };
    let task_sem = Arc::new(Semaphore::new(jobs));

    let mut task_id = 0;
    for topic in &config.topics {
        if let Some(target) = target_database.as_ref() {
            if !global_taos.database_exists(target).await? {
                tracing::info!(
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
                bail!("the database has already exists, please be sure to override it by force");
            }
        } else if !global_taos.database_exists(&topic.database).await? {
            if let Some(sql) = topic.database_sql.as_deref() {
                global_taos
                    .exec(sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS"))
                    .await?;
            }
        } else if !force {
            bail!("the database has already exists, please be sure to override it by force");
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

        let mut dir_entry = tokio::fs::read_dir(&local_dir).await?;

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

            if let std::collections::btree_map::Entry::Vacant(e) = files.entry(vgroup) {
                let mut map = BTreeMap::new();
                map.insert(ts, path);
                e.insert(map);
            } else {
                files.get_mut(&vgroup).unwrap().insert(ts, path);
            }
        }

        for (_vgroup_id, files) in files {
            let sem = task_sem.clone().acquire_owned().await?;
            let taos = target.build().await?;

            if let Some(target) = target_database.as_ref() {
                taos.exec(format!("use `{}`", target)).await?;
            } else {
                taos.exec(format!("use `{}`", topic.database)).await?;
            }

            let table = topic.table.as_ref().map(|t| t.table.clone());
            let handle = tokio::spawn(async move {
                for (_ts, path) in files {
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

pub async fn is_local_valid(dsn: &Dsn) -> DataSourceValidation {
    if dsn.driver != "local" {
        return DataSourceValidation::invalid(
            "local".to_string(),
            "backup data source".to_string(),
        );
    }
    if dsn.path.is_none() {
        return DataSourceValidation::invalid(
            "local".to_string(),
            "No backup directory specified".to_string(),
        );
    }
    let path: &Path = dsn.path.as_ref().unwrap().as_ref();
    if !path.exists() {
        return DataSourceValidation::invalid(
            "local".to_string(),
            "Backup directory does not exist".to_string(),
        );
    }
    let config_path = path.join("local.toml");
    if !config_path.exists() {
        return DataSourceValidation::invalid(
            "local".to_string(),
            "Backup directory may not be correct".to_string(),
        );
    }

    DataSourceValidation {
        valid: true,
        support: true,
        data_source: "local".to_string(),
        version: None,
        message: None,
        namespaces: None,
    }
}

#[tokio::test]
#[ignore]
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
        None,
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
