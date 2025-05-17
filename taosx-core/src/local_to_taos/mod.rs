use anyhow::{bail, Context, Result};
use flume::Receiver;
use itertools::Itertools;
use std::path::{Path, PathBuf};
use std::time::Duration;
use taos::{AsyncQueryable, Dsn, Taos, TaosPool};
use taosx_ipc::types::dsv::DataSourceValidation;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

use crate::core_metrics::get_metrics_arc;
use crate::local_to_taos::conf::{LocalRestoreConfig, LocalRestoreConfigBuilder};
use crate::local_to_taos::file_watcher::FileWatcher;
use crate::local_to_taos::metrics::LocalToTaosMetrics;
use crate::s3::{S3Config, S3Loader};
use crate::taoz::{ZCodec, ZFile, ZMessage};
use crate::tmq::BackupObject;
use crate::utils::constants::{VERSION_3_0_0, VERSION_3_3_0};
use crate::{s3, utils};

mod conf;
mod file_watcher;
mod metrics;

/// # 从本地备份恢复到 taos
/// 1. 备份文件对应某个备份对象：database 或 database.stable，在 target 中：
/// * 如果 backup object 存在
///     - 如果 @param force 为 true，删除旧的 backup object，创建新的 backup object
///     - 如果 @param force 为 false，报错 target 已存在，退出
/// * 如果 backup object 不存在，创建新的 backup object
/// 2. 恢复任务需要按照 backup point 的时间戳顺序，依次恢复；同一个 vg_id 的备份文件，按照 index 顺序恢复
/// 3. 如果 stop.at 为 None，则持续监听 backup_dir 下的新备份文件
/// 4. local_to_tmq 任务可以被中断 @param cancel
#[instrument(skip_all, fields(task_id))]
#[async_backtrace::framed]
pub async fn local_to_taos(
    task_id: Option<String>,
    from: Dsn,
    to: Dsn,
    cancel: CancellationToken,
) -> Result<()> {
    tracing::info!("local_to_taos start");

    // 解析参数
    let config = LocalRestoreConfigBuilder::new(&task_id, &from, &to)
        .build()
        .await
        .context("parse local_to_taos config error")?;
    tracing::debug!("local_to_taos config: {:#?}", config);

    // 如果配置了 S3 转储，则先从 S3 下载备份文件到本地
    if let Some(s3_config) = &config.s3_config {
        let s3_loader = S3Loader::try_from(s3_config).await?;
        s3_loader.load_to(config.backup_dir.as_path()).await?;
    }

    // 处理 backup object
    // if config.is_obj_existed().await? {
    //     if config.force {
    //         tracing::warn!("restore target exists, force to delete and recreate");
    //         config.delete_obj().await?;
    //     } else {
    //         bail!("restore target already exists, please delete and recreate");
    //     }
    // }
    // tracing::warn!("recreate backup object");
    // config.restore_obj().await?;

    // 创建 watcher
    let watcher = FileWatcher::from(config.clone());
    let stop_flag = watcher.get_stop_flag();

    // load metrics
    let metrics_arc = get_metrics_arc(task_id.clone()).await;

    let (tx, rx) = flume::unbounded();
    let taos_pool = config.connect_taos_pool().await?;
    // 创建 RestoreWorker
    let rx = rx.clone();
    let worker = RestoreWorker {
        rx,
        backup_config: config.clone(),
        backup_obj: config.backup_obj.clone(),
        metrics: LocalToTaosMetrics::new(metrics_arc.clone()),
        pool: taos_pool,
    };
    let restore_worker = tokio::spawn(async move { worker.run().await }.in_current_span());

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
        let mut files_to_send = Vec::with_capacity(files.len());
        for f in files {
            match config.stop_at.as_ref() {
                None => {
                    files_to_send.push(f);
                }
                Some(stop_at) => {
                    let file_name = f.file_name().unwrap().to_string_lossy();
                    let (_, ts, _, _idx) = ZFile::parse_file_name(file_name.as_ref())?;
                    tracing::debug!(
                        "compare current point: {} with stop point: {}",
                        ts.to_rfc3339(),
                        stop_at.to_rfc3339()
                    );
                    match ts.cmp(stop_at) {
                        std::cmp::Ordering::Less => {
                            files_to_send.push(f);
                        }
                        std::cmp::Ordering::Equal => {
                            files_to_send.push(f);
                            stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        std::cmp::Ordering::Greater => {
                            tracing::debug!("local_to_taos skip file: {:?}", f);
                            // skip
                            stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        }

        if files_to_send.is_empty() {
            if let Some(stop_at) = config.stop_at.as_ref() {
                if stop_at < &chrono::Utc::now() {
                    tracing::debug!("local_to_taos has no files to read");
                    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        } else {
            tracing::debug!("local_to_taos send files: {:?} to worker", files_to_send);
            tx.send(files_to_send).inspect_err(|err| {
                tracing::error!("failed to send files to worker: {:#}", err);
            })?;
        }

        // 检查是否需要停止
        if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!("local_to_taos stop reading files");
            break;
        }
    }
    drop(tx);
    tracing::info!("local_to_taos completed");

    // 等待所有 worker 完成
    restore_worker.await?
}

struct RestoreWorker {
    backup_config: LocalRestoreConfig,
    backup_obj: BackupObject,
    rx: Receiver<Vec<PathBuf>>,
    metrics: LocalToTaosMetrics,
    pool: TaosPool,
}

impl RestoreWorker {
    /// 从 channel 中获取备份文件的路径，然后恢复到 taos
    async fn run(&self) -> Result<()> {
        tracing::info!("RestoreWorker started");

        while let Ok(files) = self.rx.recv() {
            let file_count = files.len();

            // 按照 ts 分组，按照 ts 的先后顺序执行
            let files = files
                .into_iter()
                .chunk_by(|f| {
                    let file_name = f.file_name().unwrap().to_str().unwrap();
                    let (_, ts, _, _) = ZFile::parse_file_name(file_name).unwrap();
                    ts
                })
                .into_iter()
                .map(|(_ts, chunk)| chunk.collect_vec())
                .collect_vec();

            for files_of_point in files {
                // 按照 vg_id 分组，可以并行执行
                let files_of_vgroup = files_of_point
                    .into_iter()
                    .chunk_by(|f| {
                        let file_name = f.file_name().unwrap().to_str().unwrap();
                        let (_, _, vg_id, _) = ZFile::parse_file_name(file_name).unwrap();
                        vg_id
                    })
                    .into_iter()
                    .map(|(_vg_id, chunk)| chunk.collect_vec())
                    .collect_vec();

                let mut join_set: JoinSet<Result<()>> = JoinSet::new();
                // 每个 vgroup 一个 worker
                for (idx, files) in files_of_vgroup.into_iter().enumerate() {
                    // let taos = self.backup_config.connect_taos().await?;
                    let taos = self.pool.get().await?;
                    let stable = self.backup_obj.stable_name.clone();
                    let metrics = self.metrics.clone();
                    let retry_interval = self.backup_config.error_retry_interval;
                    let retry_max = self.backup_config.error_retry_max;

                    join_set.spawn(async move {
                        for f in files {
                            tracing::debug!("worker[{idx}] restore files: {:?}", f);
                            restore(
                                idx,
                                f,
                                &taos,
                                stable.clone(),
                                metrics.clone(),
                                retry_max,
                                retry_interval,
                            )
                            .await?;
                        }
                        Ok(())
                    });
                }
                // 等待所有 worker 完成
                while let Some(res) = join_set.join_next().await {
                    if let Err(err) = res.map_err(anyhow::Error::from) {
                        tracing::error!("restore worker error: {:#}", err);
                        join_set.abort_all();
                        return Err(err);
                    }
                }
            }

            self.metrics.add_processed_files(file_count as u64);
        }
        tracing::info!("RestoreWorker stopped");
        Ok(())
    }
}

/*
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

            /// TODO: invalid metrics
            let metrics = get_metrics_arc(None).await;

            let table = topic.table.as_ref().map(|t| t.table.clone());
            let handle = tokio::spawn(async move {
                for (_ts, path) in files {
                    let res =
                        restore(task_id, path.path(), &taos, table.clone(), metrics.clone()).await;
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
*/

#[async_backtrace::framed]
async fn restore(
    idx: usize,
    path: impl AsRef<Path>,
    taos: &Taos,
    table: Option<String>,
    metrics: LocalToTaosMetrics,
    retry_max: u32,
    retry_interval: Duration,
) -> Result<()> {
    let path = path.as_ref();
    tracing::info!("[{idx}] restore with file: {:?}", path.display());
    let reader = tokio::fs::File::open(path).await?;
    let reader = tokio::io::BufReader::new(reader);
    let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);
    let mut reader = ZCodec::new(reader);
    let header = reader.header_async().await?;
    tracing::debug!("[{idx}] parse header: {:?}", header);

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

    'READ_LOOP: loop {
        let res = reader.read_message_async().await;
        match res {
            Ok(message) => {
                metrics.add_received_batch();
                match message {
                    ZMessage::Meta(meta) => {
                        tracing::debug!("[{idx}] restore meta, len: {}", meta.raw_len());

                        if let Err(err) = taos.write_raw_meta(&meta).await {
                            let code: i32 = err.code().into();
                            match code {
                                0x0603 => {
                                    // 0x0603: table already exists
                                    tracing::debug!("Table already exists");
                                    // do nothing and continue
                                }
                                0x032C | 0x0115 | 0x03C7 | 0x03D3 | 0x2603 => {
                                    // 0x032C: object is creating
                                    // 0x0115: invalid msg
                                    // 0x03C7: stable uid not match
                                    // 0x03D3: conflict transaction not completed
                                    // 0x2603: the table does not exist: 写 meta 时报表不存在，重试
                                    tracing::debug!("Found recoverable error: {err:#}, retry once");
                                    let mut retry = retry_max;
                                    loop {
                                        if retry == 0 {
                                            tracing::error!("Retry failed: {:#}, skip", err);
                                            metrics.add_failed_batch();
                                            continue 'READ_LOOP;
                                        }
                                        retry -= 1;

                                        tokio::time::sleep(retry_interval).await;
                                        tracing::debug!("Retrying... {retry} times left");
                                        let res = taos.write_raw_meta(&meta).await;
                                        if res.is_ok() {
                                            tracing::debug!("Retry success");
                                            break;
                                        } else {
                                            tracing::warn!("Retry failed: {:#}", res.unwrap_err());
                                        }
                                    }
                                }
                                _ => {
                                    tracing::error!("restore meta error: {:#}", err);
                                    metrics.add_failed_batch();
                                    continue 'READ_LOOP;
                                }
                            }
                        };
                        metrics.add_processed_batch();
                        metrics.add_processed_bytes(meta.raw_len() as u64);
                    }
                    ZMessage::Data(data) => {
                        tracing::debug!("[{idx}] restore data, len: {}", data.len());
                        for mut raw in data {
                            if let Some(name) = &table {
                                raw.with_table_name(name.as_str());
                            }
                            let bytes = raw.as_raw_bytes().len() as u64;
                            if let Err(err) = taos.write_raw_block(&raw).await {
                                let code: i32 = err.code().into();
                                match code {
                                    0x2603 => {
                                        // 0x2603: the table does not exist：写 data 时表不存在，尝试建表+重试
                                        if let Some(meta) = raw.to_create() {
                                            if let Err(err) = taos.exec(format!("{}", meta)).await {
                                                tracing::error!("restore data error: {:#}", err);
                                                metrics.add_failed_batch();
                                                continue 'READ_LOOP;
                                            };
                                        } else {
                                            tracing::error!("restore data error: {:#}", err);
                                            metrics.add_failed_batch();
                                            continue 'READ_LOOP;
                                        }

                                        let mut retry = retry_max;
                                        loop {
                                            if retry == 0 {
                                                tracing::error!("Retry failed: {:#}, skip", err);
                                                metrics.add_failed_batch();
                                                continue 'READ_LOOP;
                                            }
                                            retry -= 1;

                                            match taos.write_raw_block(&raw).await {
                                                Ok(_) => {
                                                    tracing::debug!("Retry success");
                                                    break;
                                                }
                                                Err(err) => {
                                                    tracing::warn!("Retry failed: {:#}", err);
                                                }
                                            }
                                            tokio::time::sleep(retry_interval).await;
                                        }
                                    }
                                    _ => {
                                        tracing::error!("restore data err: {:#}", err);
                                        metrics.add_failed_batch();
                                        continue 'READ_LOOP;
                                    }
                                }
                            };
                            metrics.add_processed_batch();
                            metrics.add_processed_bytes(bytes);
                        }
                    }
                    ZMessage::Raw(raw_type, raw) => {
                        tracing::debug!("[{idx}] restore raw, len: {}", raw.raw_len());
                        if let Err(err) = taos.write_raw_meta(&raw).await {
                            let code: i32 = err.code().into();
                            match code {
                                0x032C | 0x0115 | 0x0603 | 0x03C7 | 0x03D3 | 0x2603 => {
                                    // 0x032C: object is creating
                                    // 0x0115: invalid msg
                                    // 0x0603: table already exists
                                    // 0x03C7: stable uid not match
                                    // 0x03D3: conflict transaction not completed
                                    // 0x2603: the table does not exist
                                    tracing::debug!(raw.r#type = ?raw_type, "Found recoverable error: {:#}, retry", err);
                                    let mut retry = retry_max;
                                    'RETRY_LOOP: loop {
                                        if retry == 0 {
                                            tracing::error!("Retry failed: {:#}, skip", err);
                                            metrics.add_failed_batch();
                                            continue 'READ_LOOP;
                                        }
                                        retry -= 1;

                                        tokio::time::sleep(retry_interval).await;
                                        tracing::debug!("Retrying... {retry} times left");
                                        match taos.write_raw_meta(&raw).await {
                                            Ok(_) => {
                                                tracing::debug!("Retry success");
                                                break 'RETRY_LOOP;
                                            }
                                            Err(err) => {
                                                tracing::warn!("Retry failed: {:#}", err);
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    tracing::error!("restore raw error: {:#}", err);
                                    metrics.add_failed_batch();
                                    continue 'READ_LOOP;
                                }
                            }
                        };
                        metrics.add_processed_batch();
                        metrics.add_processed_bytes(raw.raw_len() as u64);
                    }
                }
            }
            Err(err) => {
                // 如果是 EOF，表示文件读取完成
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    tracing::info!("[{idx}] reading file {} done", path.display());
                    break;
                }
                // 其他错误，打印错误信息
                tracing::debug!("[{idx}] reading data error: {}", &err);
                break;
            }
        }

        tracing::debug!("local_to_taos metrics detail\n{}", metrics);
    }

    drop(reader);
    Ok(())
}

/// 检查 local 数据源是否有效
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

    if let Some(true) = utils::parse_key_in_dsn(dsn, s3::S3_ENABLE)? {
        let config = S3Config::from_dsn(dsn)?;
        config.connect().await?;
    }

    Ok(())
}

/*
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
*/

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
            None,
            "tmq:///local_to_taos".parse()?,
            local.clone(),
            Default::default(),
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
