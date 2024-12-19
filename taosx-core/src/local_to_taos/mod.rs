use anyhow::{bail, Context, Result};
use flume::Receiver;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use taos::{AsyncQueryable, AsyncTBuilder, Dsn, Taos, TaosBuilder};
use taosx_ipc::types::dsv::DataSourceValidation;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::local_to_taos::conf::{LocalRestoreConfig, LocalRestoreConfigBuilder};
use crate::local_to_taos::file_watcher::FileWatcher;
use crate::taoz::{ZCodec, ZFile, ZMessage};
use crate::tmq::BackupObject;
use crate::tmq_to_local::LocalConfig;
use crate::utils;
use crate::utils::constants::{VERSION_3_0_0, VERSION_3_3_0};

mod conf;
mod file_watcher;

/// 从本地备份恢复到 taos
/// 1. 备份文件对应某个备份对象：database 或 database.stable，在 target 中：
/// * 如果 backup object 存在
///     - 如果 @param force 为 true，删除旧的 backup object，创建新的 backup object
///     - 如果 @param force 为 false，报错 target 已存在，退出
/// * 如果 backup object 不存在，创建新的 backup object
/// 2. 恢复任务需要按照 backup point 的时间戳顺序，依次恢复；同一个 vg_id 的备份文件，按照 index 顺序恢复
/// 3. 如果 stop.at 为 None，则持续监听 backup_dir 下的新备份文件
/// 4. local_to_tmq 任务可以被中断 @param cancel
#[tracing::instrument]
#[async_backtrace::framed]
pub async fn local_to_taos(
    task_id: Option<String>,
    from: Dsn,
    to: Dsn,
    jobs: usize,
    force: bool,
    cancel: CancellationToken,
) -> Result<()> {
    tracing::info!("local_to_taos start");

    // 解析参数
    let config = LocalRestoreConfigBuilder::new(&task_id, &from, &to)
        .build()
        .await
        .context("parse local_to_taos config error")?;
    tracing::debug!("local_to_taos config: {:?}", config);

    // 处理 backup object
    if config.is_obj_existed().await? {
        if force {
            tracing::warn!("restore target exists, force to delete and recreate");
            config.delete_obj().await?;
        } else {
            bail!("restore target already exists, please use -y/--yes-i-really-mean-it to delete and recreate");
        }
    }
    tracing::warn!("recreate backup object");
    config.restore_obj().await?;

    // 创建 watcher
    let watcher = FileWatcher::from(config.clone());
    let stop_flag = watcher.get_stop_flag();

    let (tx, rx) = flume::unbounded();
    // 创建 RestoreWorker
    let mut join_set = JoinSet::new();
    let jobs = if jobs > 0 { jobs } else { 16 };
    for i in 0..jobs {
        let rx = rx.clone();
        let mut worker = RestoreWorker {
            id: i,
            rx,
            backup_config: config.clone(),
            backup_obj: config.backup_obj.clone(),
        };
        join_set.spawn(async move { worker.run().await }.in_current_span());
    }

    let cancel_clone = cancel.clone();
    let stop_flag_clone = stop_flag.clone();
    tokio::spawn(async move {
        cancel_clone.cancelled().await;
        tracing::warn!("local_to_taos task: {:?} cancelled", task_id);
        stop_flag_clone.store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!("set stop flag true since task cancelled");
    });

    // 读取备份目录下的文件
    let stream = watcher.into_stream();
    tokio::pin!(stream);
    tracing::debug!("local_to_taos start read files");
    while let Some(files) = stream.next().await {
        for f in files {
            match config.stop_at.as_ref() {
                None => {
                    tracing::debug!("local_to_taos send file: {:?} to worker", f);
                    // TODO handle send error
                    tx.send(f)?;
                }
                Some(stop_at) => {
                    let file_name = f.file_name().unwrap().to_string_lossy();
                    let (_, ts, _, _idx) = ZFile::parse_file_name(file_name.as_ref())?;
                    tracing::debug!("compare current point: {} with stop point: {}", ts, to);
                    match ts.cmp(stop_at) {
                        std::cmp::Ordering::Less => {
                            tracing::debug!("local_to_taos send file: {:?} to worker", f);
                            // TODO handle send error
                            tx.send(f)?;
                        }
                        std::cmp::Ordering::Equal => {
                            tracing::debug!("local_to_taos send file: {:?} to worker", f);
                            // TODO handle send error
                            tx.send(f)?;
                            stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                            tracing::debug!("set stop flag true since stop point reached");
                        }
                        std::cmp::Ordering::Greater => {
                            tracing::debug!("local_to_taos skip file: {:?}", f);
                            // skip
                            stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                            tracing::debug!("set stop flag true since stop point reached");
                        }
                    }
                }
            }
        }
        if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!("local_to_taos stop reading files");
            break;
        }
    }
    drop(tx);
    tracing::info!("local_to_taos read files done");

    // 等待所有 worker 完成
    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res.map_err(anyhow::Error::from).and_then(|r| r) {
            tracing::error!("abort all local_to_taos workers since error: {:#}", err);
            join_set.abort_all();
            // TODO: 如果出现错误，要清理文件
            return Err(err);
        }
    }
    tracing::info!("local_to_taos completed");
    Ok(())
}

struct RestoreWorker {
    id: usize,
    backup_config: LocalRestoreConfig,
    backup_obj: BackupObject,
    rx: Receiver<PathBuf>,
}

impl RestoreWorker {
    async fn run(&mut self) -> Result<()> {
        tracing::info!("RestoreWorker {} started", self.id);
        let taos = self.backup_config.connect_taos().await?;

        while let Ok(file) = self.rx.recv() {
            tracing::info!("restore file: {:?}", file);
            restore(self.id, file, &taos, self.backup_obj.stable_name.as_deref()).await?;
        }
        tracing::info!("RestoreWorker {} stopped", self.id);
        Ok(())
    }
}

#[allow(unused)]
#[deprecated(note = "use new local_to_taos")]
pub async fn local_to_taos_previous(from: Dsn, mut to: Dsn) -> Result<()> {
    // FIXME(@zitsen)
    let jobs = 0;
    let force = true;

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

pub async fn is_local_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_local_valid_impl(dsn).await {
        Ok(_) => DataSourceValidation {
            valid: true,
            support: true,
            data_source: "local".to_string(),
            version: None,
            message: None,
            namespaces: None,
        },
        Err(err) => DataSourceValidation::invalid("local".to_string(), err.to_string()),
    }
}

pub async fn is_local_valid_impl(dsn: &Dsn) -> Result<()> {
    if dsn.driver != "local" {
        bail!("invalid driver: {}", dsn.driver);
    }
    if dsn.path.is_none() {
        bail!("no backup directory specified");
    }
    utils::parse_dir_in_dsn(dsn, None)?;

    Ok(())
}

#[allow(unused /* previous version */)]
pub async fn is_local_valid_previous(dsn: &Dsn) -> DataSourceValidation {
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

#[cfg(test)]
mod tests {
    use super::*;
    use taos::{AsyncTBuilder, TaosBuilder};

    #[tokio::test]
    #[ignore]
    async fn test_local_to_taos_with_taos() -> Result<()> {
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

        local_to_taos(
            None,
            local.clone(),
            "taos:///".parse()?,
            1,
            true,
            CancellationToken::new(),
        )
        .await?;

        let count: usize = taos.query_one("SELECT count(*) from tb1").await?.unwrap();
        assert_eq!(count, 3, "restored");

        std::fs::remove_dir_all(out)?;

        taos.exec_many(["DROP DATABASE local_to_taos"]).await?;

        Ok(())
    }
}
