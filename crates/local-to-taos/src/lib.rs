use anyhow::{Context, bail};
use flume::Receiver;
use itertools::Itertools;
use std::path::{Path, PathBuf};
use std::time::Duration;
use taos::{AsyncQueryable, Dsn, Taos, TaosPool};
use taosx_ipc::types::dsv::DataSourceValidation;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};

use taosx_core::core_metrics::get_metrics;
use taosx_core::s3::{S3Config, S3Loader};
use taosx_core::taoz::{ZCodec, ZFile, ZMessage};
use taosx_core::tmq::BackupObject;
use taosx_core::utils::constants::{VERSION_3_0_0, VERSION_3_3_0};
use taosx_core::{s3, utils};

use conf::{LocalRestoreConfig, LocalRestoreConfigBuilder, PostAction};
use file_watcher::FileWatcher;
use metrics::LocalToTaosMetrics;

pub mod conf;
pub mod file_watcher;
pub mod metrics;

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
) -> anyhow::Result<()> {
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

    // 创建 watcher
    let watcher = FileWatcher::from(config.clone());
    let stop_flag = watcher.get_stop_flag();

    // load metrics
    let tid = task_id
        .clone()
        .and_then(|id| id.parse::<i64>().ok())
        .unwrap_or(-1);
    let metrics = get_metrics(tid).await.map(LocalToTaosMetrics::new);

    let (tx, rx) = flume::unbounded();
    let taos_pool = config.connect_taos_pool().await?;
    // 创建 RestoreWorker
    let rx = rx.clone();
    let worker = RestoreWorker {
        rx,
        backup_config: config.clone(),
        backup_obj: config.backup_obj.clone(),
        metrics: metrics.clone(),
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

        if let Some(false) = config.watch {
            stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
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
    metrics: Option<LocalToTaosMetrics>,
    pool: TaosPool,
}

impl RestoreWorker {
    /// 从 channel 中获取备份文件的路径，然后恢复到 taos
    async fn run(&self) -> anyhow::Result<()> {
        tracing::info!("RestoreWorker started");
        while let Ok(files) = self.rx.recv_async().await {
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

                let mut join_set: JoinSet<anyhow::Result<()>> = JoinSet::new();
                // 每个 vgroup 一个 worker
                for (idx, files) in files_of_vgroup.into_iter().enumerate() {
                    // let taos = self.backup_config.connect_taos().await?;
                    let taos = self.pool.get().await?;
                    let stable = self.backup_obj.stable_name.clone();
                    let metrics = self.metrics.clone();
                    let retry_interval = self.backup_config.error_retry_interval;
                    let retry_max = self.backup_config.error_retry_max;

                    let post_action = self.backup_config.post_action.clone();
                    join_set.spawn(async move {
                        for f in files {
                            tracing::debug!("worker[{idx}] restore files: {:?}", f);
                            let res = restore(
                                idx,
                                f.clone(),
                                &taos,
                                stable.clone(),
                                metrics.clone(),
                                retry_max,
                                retry_interval,
                            )
                            .await;
                            if let Err(err) = res {
                                tracing::error!("worker[{idx}] restore file error: {:#}", err);
                                return Err(err);
                            }

                            match post_action {
                                None => {
                                    // do nothing
                                }
                                Some(PostAction::Delete) => {
                                    // delete file
                                    tokio::fs::remove_file(f).await?;
                                }
                                Some(PostAction::Move(ref move_to)) => {
                                    // move file, use the specified path
                                    let file_name = f.file_name().unwrap().to_str().unwrap();
                                    let (_, ts, _, _) = ZFile::parse_file_name(file_name).unwrap();
                                    let new_path = PathBuf::from(ts.format(move_to).to_string())
                                        .join(file_name);
                                    tracing::debug!(
                                        "worker[{idx}] move file: {:?} to: {:?}",
                                        f,
                                        new_path
                                    );
                                    tokio::fs::rename(f.clone(), new_path.clone())
                                        .await
                                        .context(format!(
                                            "failed to move file: {:?} to: {:?}",
                                            f, new_path
                                        ))?;
                                }
                            }
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

            if let Some(metrics) = &self.metrics {
                metrics.add_processed_files(file_count as u64);
                tracing::debug!("local_to_taos metrics detail\n{}", metrics);
            }
        }
        tracing::info!("RestoreWorker stopped");
        Ok(())
    }
}

async fn open_file_with_retry(
    path: impl AsRef<Path>,
    retry_max: u32,
    retry_interval: Duration,
) -> anyhow::Result<tokio::fs::File> {
    let path = path.as_ref();
    let mut retry = retry_max;
    loop {
        match tokio::fs::File::open(path).await {
            Ok(file) => {
                tracing::debug!("Opened file: {:?}", path.display());
                return Ok(file);
            }
            Err(err) => {
                if retry == 0 {
                    tracing::error!(
                        "Failed to open file: {:?}, error: {:#}",
                        path.display(),
                        err
                    );
                    return Err(err.into());
                }
                retry -= 1;
                tracing::warn!(
                    "Failed to open file: {:?}, retrying... {} times left",
                    path.display(),
                    retry
                );
                tokio::time::sleep(retry_interval).await;
            }
        }
    }
}

#[async_backtrace::framed]
async fn restore(
    idx: usize,
    path: impl AsRef<Path>,
    taos: &Taos,
    table: Option<String>,
    metrics: Option<LocalToTaosMetrics>,
    retry_max: u32,
    retry_interval: Duration,
) -> anyhow::Result<()> {
    let path = path.as_ref();
    tracing::info!("[{idx}] restore with file: {:?}", path.display());

    let reader = open_file_with_retry(path, retry_max, retry_interval).await?;
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
        bail!(
            "Backup source version is 3.3.0 or later, but target version is earlier than 3.3.0, which is not supported."
        );
    }
    if let Some(source_version) = header.server_version() {
        let source_version = semver::Version::parse(&source_version.split('.').take(3).join("."))?;
        if source_version >= VERSION_3_3_0 && target_version < VERSION_3_3_0 {
            bail!(
                "Backup source version is 3.3.0 or later, but target version is earlier than 3.3.0, which is not supported."
            );
        }
    }

    'READ_LOOP: loop {
        let res = reader.read_message_async().await;
        match res {
            Ok(message) => {
                if let Some(metrics) = &metrics {
                    metrics.add_received_batch();
                }
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
                                            if let Some(metrics) = &metrics {
                                                metrics.add_failed_batch();
                                            }
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
                                    if let Some(metrics) = &metrics {
                                        metrics.add_failed_batch();
                                    }
                                    continue 'READ_LOOP;
                                }
                            }
                        };
                        if let Some(metrics) = &metrics {
                            metrics.add_processed_batch();
                            metrics.add_processed_bytes(meta.raw_len() as u64);
                        }
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
                                                if let Some(metrics) = &metrics {
                                                    metrics.add_failed_batch();
                                                }
                                                continue 'READ_LOOP;
                                            };
                                        } else {
                                            tracing::error!("restore data error: {:#}", err);
                                            if let Some(metrics) = &metrics {
                                                metrics.add_failed_batch();
                                            }
                                            continue 'READ_LOOP;
                                        }

                                        let mut retry = retry_max;
                                        loop {
                                            if retry == 0 {
                                                tracing::error!("Retry failed: {:#}, skip", err);
                                                if let Some(metrics) = &metrics {
                                                    metrics.add_failed_batch();
                                                }
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
                                        if let Some(metrics) = &metrics {
                                            metrics.add_failed_batch();
                                        }
                                        continue 'READ_LOOP;
                                    }
                                }
                            };
                            if let Some(metrics) = &metrics {
                                metrics.add_processed_batch();
                                metrics.add_processed_bytes(bytes);
                            }
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
                                            if let Some(metrics) = &metrics {
                                                metrics.add_failed_batch();
                                            }
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
                                    if let Some(metrics) = &metrics {
                                        metrics.add_failed_batch();
                                    }
                                    continue 'READ_LOOP;
                                }
                            }
                        };
                        if let Some(metrics) = &metrics {
                            metrics.add_processed_batch();
                            metrics.add_processed_bytes(raw.raw_len() as u64);
                        }
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

        if let Some(metrics) = &metrics {
            tracing::debug!("local_to_taos metrics detail\n{}", metrics);
        }
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

pub async fn is_local_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::sql::connect_taos;
    use taos::IntoDsn;

    /// # description_cn
    /// 本地恢复, post_action=move
    /// 1. 创建数据库：DB_SRC 和 DB_DST，向 DB_SRC 中创建超级表，并插入 5 行数据
    /// 2. 启动 tmq_to_local 任务，将 DB_SRC 中的数据备份到本地
    /// 3. 启动 local_to_taos 任务，将备份的数据恢复到 DB_DST 中
    /// 4. 检查 DB_SRC 和 DB_DST 中的数据，一致则用例通过，否则失败
    /// 5. 检查本地备份目录下的文件，应该为空
    /// 6. 检查 move_to 目录下的文件，应该不为空
    /// # jira
    /// close https://jira.taosdata.com:18080/browse/TS-6456
    /// # example
    /// ```shell
    /// cargo nextest run -p taosx-core test_move_post_action_with_taos --no-capture --retries 0
    /// ```
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_move_post_action_with_taos() -> anyhow::Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = std::env::var("WS_ENABLE")
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(false);

        const DB_SRC: &str = "ts6456_mov_src";
        const DB_DST: &str = "ts6456_mov_dst";
        const TOPIC: &str = "ts6456_mov";
        const ROWS: i64 = 5;

        let taos = connect_taos(&host, ws_enable).await?;
        let temp_dir = tempfile::tempdir()?;
        let move_to_dir = tempfile::tempdir()?;

        // create database and stable, insert 5 rows
        init_database(&taos, TOPIC, DB_SRC, DB_DST, ROWS).await?;

        // start a tmq_to_local task to generate backup files
        run_tmq_to_local(
            ws_enable,
            &host,
            DB_SRC,
            TOPIC,
            temp_dir.path().display().to_string().as_str(),
        )
        .await?;

        // start a local_to_taos task to restore data
        let (from, to) = if ws_enable {
            let from = format!(
                "local:{}?to=now&post_action=move&move_to={}",
                temp_dir.path().display(),
                move_to_dir.path().display()
            )
            .into_dsn()?;
            let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
            (from, to)
        } else {
            let from = format!(
                "local:{}?to=now&post_action=move&move_to={}",
                temp_dir.path().display(),
                move_to_dir.path().display()
            )
            .into_dsn()?;
            let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
            (from, to)
        };
        local_to_taos(None, from, to, CancellationToken::new()).await?;

        // check data
        let count_dst: i64 = taos
            .query_one(format!("select count(*) from `{DB_DST}`.stb"))
            .await?
            .unwrap_or(0);
        assert_eq!(count_dst, ROWS);

        // check files
        let mut files = vec![];
        let mut entries = tokio::fs::read_dir(temp_dir.path()).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                files.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        assert!(
            files.is_empty(),
            "backup dir should be empty after move post action",
        );

        let mut files = vec![];
        let mut entries = tokio::fs::read_dir(move_to_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                files.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        assert!(
            !files.is_empty(),
            "move_to dir should not be empty after move post action",
        );

        // clean
        temp_dir.close()?;
        taos.exec_many(vec![
            format!("DROP TOPIC IF EXISTS force {TOPIC}"),
            format!("DROP DATABASE IF EXISTS {DB_SRC}"),
            format!("DROP DATABASE IF EXISTS {DB_DST}"),
        ])
        .await?;

        Ok(())
    }

    /// # description_cn
    /// 本地恢复, post_action=delete
    /// 1. 创建数据库：DB_SRC 和 DB_DST，向 DB_SRC 中创建超级表，并插入 5 行数据
    /// 2. 启动 tmq_to_local 任务，将 DB_SRC 中的数据备份到本地
    /// 3. 启动 local_to_taos 任务，将备份的数据恢复到 DB_DST 中
    /// 4. 检查 DB_SRC 和 DB_DST 中的数据，一致则用例通过，否则失败
    /// 5. 检查本地备份目录下的文件，应该为空
    /// # jira
    /// close https://jira.taosdata.com:18080/browse/TS-6456
    /// # example
    /// ```shell
    /// cargo nextest run -p taosx-core test_delete_post_action_with_taos --no-capture --retries 0
    /// ```
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_delete_post_action_with_taos() -> anyhow::Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = std::env::var("WS_ENABLE")
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(false);
        const DB_SRC: &str = "ts6456_del_src";
        const DB_DST: &str = "ts6456_del_dst";
        const TOPIC: &str = "ts6456_del";
        const ROWS: i64 = 5;

        let taos = connect_taos(&host, ws_enable).await?;
        let temp_dir = tempfile::tempdir()?;

        // create database and stable, insert 3 rows
        init_database(&taos, TOPIC, DB_SRC, DB_DST, ROWS).await?;

        // start a tmq_to_local task to generate backup files
        run_tmq_to_local(
            ws_enable,
            &host,
            DB_SRC,
            TOPIC,
            temp_dir.path().display().to_string().as_str(),
        )
        .await?;

        // start a local_to_taos task to restore data
        let (from, to) = if ws_enable {
            let from =
                format!("local:{}?to=now&post_action=del", temp_dir.path().display()).into_dsn()?;
            let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
            (from, to)
        } else {
            let from =
                format!("local:{}?to=now&post_action=del", temp_dir.path().display()).into_dsn()?;
            let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
            (from, to)
        };
        local_to_taos(None, from, to, CancellationToken::new()).await?;

        // check data
        let count_dst: i64 = taos
            .query_one(format!("select count(*) from `{DB_DST}`.stb"))
            .await?
            .unwrap_or(0);
        assert_eq!(count_dst, ROWS);

        // check files
        let mut files = vec![];
        let mut entries = tokio::fs::read_dir(temp_dir.path()).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                files.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        assert!(
            files.is_empty(),
            "backup files should be empty after delete post action"
        );

        // clean
        temp_dir.close()?;
        taos.exec_many(vec![
            format!("DROP TOPIC IF EXISTS force {TOPIC}"),
            format!("DROP DATABASE IF EXISTS {DB_SRC}"),
            format!("DROP DATABASE IF EXISTS {DB_DST}"),
        ])
        .await?;

        Ok(())
    }

    /// # description_cn
    /// 本地恢复
    /// 1. 创建数据库：DB_SRC 和 DB_DST，向 DB_SRC 中创建超级表，并插入 5 行数据
    /// 2. 启动 tmq_to_local 任务，将 DB_SRC 中的数据备份到本地
    /// 3. 启动 local_to_taos 任务，将备份的数据恢复到 DB_DST 中
    /// 4. 检查 DB_SRC 和 DB_DST 中的数据，一致则用例通过，否则失败。
    /// 5. 检查本地备份目录下的文件，应该不为空，否则失败。
    /// # example
    /// ```shell
    /// cargo nextest run -p taosx-core test_local_to_taos_with_taos --no-capture --retries 0
    /// ```
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_local_to_taos_with_taos() -> anyhow::Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = std::env::var("WS_ENABLE")
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(false);
        const DB_SRC: &str = "test_local_to_taos_src";
        const DB_DST: &str = "test_local_to_taos_dst";
        const TOPIC: &str = "test_local_to_taos";
        const ROWS: i64 = 5;

        let taos = connect_taos(&host, ws_enable).await?;
        let temp_dir = tempfile::tempdir()?;

        // create database and stable, insert 5 rows
        init_database(&taos, TOPIC, DB_SRC, DB_DST, ROWS).await?;

        // start a tmq_to_local task to generate backup files
        run_tmq_to_local(
            ws_enable,
            &host,
            DB_SRC,
            TOPIC,
            temp_dir.path().display().to_string().as_str(),
        )
        .await?;

        // start a local_to_taos task to restore data
        let (from, to) = if ws_enable {
            let from = format!("local:{}?to=now", temp_dir.path().display()).into_dsn()?;
            let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
            (from, to)
        } else {
            let from = format!("local:{}?to=now", temp_dir.path().display()).into_dsn()?;
            let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
            (from, to)
        };
        local_to_taos(None, from, to, CancellationToken::new()).await?;

        // check data
        let count_dst: i64 = taos
            .query_one(format!("select count(*) from `{DB_DST}`.stb"))
            .await?
            .unwrap_or(0);
        assert_eq!(
            count_dst, ROWS,
            "count of rows in destination database should match source"
        );

        // check files
        let mut files = vec![];
        let mut entries = tokio::fs::read_dir(temp_dir.path()).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                files.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        assert!(!files.is_empty(), "backup files should not be empty");

        // clean
        temp_dir.close()?;
        taos.exec_many(vec![
            format!("DROP TOPIC IF EXISTS force {TOPIC}"),
            format!("DROP DATABASE IF EXISTS {DB_SRC}"),
            format!("DROP DATABASE IF EXISTS {DB_DST}"),
        ])
        .await?;

        Ok(())
    }

    /// # description_cn
    /// 本地恢复
    /// 1. 创建数据库：DB_SRC 和 DB_DST，向 DB_SRC 中创建超级表，并插入 5 行数据
    /// 2. 启动 tmq_to_local 任务，将 DB_SRC 中的数据备份到本地
    /// 3. 启动 local_to_taos 任务，设置 watch 为 false，只执行一次，将备份的数据恢复到 DB_DST 中
    /// 4. 检查 DB_SRC 和 DB_DST 中的数据，一致则用例通过，否则失败。
    /// 5. 检查本地备份目录下的文件，应该不为空，否则失败。
    /// # example
    /// ```shell
    /// cargo nextest run -p taosx-core test_local_to_taos_with_taos --no-capture --retries 0
    /// ```
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_watch_local_to_taos_with_taos() -> anyhow::Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = std::env::var("WS_ENABLE")
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(false);
        const DB_SRC: &str = "test_watch_ts_6896_src";
        const DB_DST: &str = "test_watch_ts_6896_dst";
        const TOPIC: &str = "test_watch_ts_6896";
        const ROWS: i64 = 5;

        let taos = connect_taos(&host, ws_enable).await?;
        let temp_dir = tempfile::tempdir()?;

        // create database and stable, insert 5 rows
        init_database(&taos, TOPIC, DB_SRC, DB_DST, ROWS).await?;

        // start a tmq_to_local task to generate backup files
        run_tmq_to_local(
            ws_enable,
            &host,
            DB_SRC,
            TOPIC,
            temp_dir.path().display().to_string().as_str(),
        )
        .await?;

        // start a local_to_taos task to restore data
        let (from, to) = if ws_enable {
            let from = format!("local:{}?watch=false", temp_dir.path().display()).into_dsn()?;
            let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
            (from, to)
        } else {
            let from = format!("local:{}?watch=false", temp_dir.path().display()).into_dsn()?;
            let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
            (from, to)
        };
        local_to_taos(None, from, to, CancellationToken::new()).await?;

        // check data
        let count_dst: i64 = taos
            .query_one(format!("select count(*) from `{DB_DST}`.stb"))
            .await?
            .unwrap_or(0);
        assert_eq!(
            count_dst, ROWS,
            "count of rows in destination database should match source"
        );

        // check files
        let mut files = vec![];
        let mut entries = tokio::fs::read_dir(temp_dir.path()).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                files.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        assert!(!files.is_empty(), "backup files should not be empty");

        // clean
        temp_dir.close()?;
        taos.exec_many(vec![
            format!("DROP TOPIC IF EXISTS force {TOPIC}"),
            format!("DROP DATABASE IF EXISTS {DB_SRC}"),
            format!("DROP DATABASE IF EXISTS {DB_DST}"),
        ])
        .await?;

        Ok(())
    }

    async fn init_database(
        taos: &Taos,
        topic: &str,
        db_src: &str,
        db_dst: &str,
        rows: i64,
    ) -> anyhow::Result<()> {
        taos.exec_many([
            format!("DROP TOPIC IF EXISTS force {topic}"),
            format!("DROP DATABASE IF EXISTS {db_src}"),
            format!("DROP DATABASE IF EXISTS {db_dst}"),
            format!("CREATE DATABASE IF NOT EXISTS {db_src}"),
            format!("CREATE DATABASE IF NOT EXISTS {db_dst}"),
            format!("CREATE STABLE `{db_src}`.stb (ts timestamp, val float) TAGS(id int)"),
        ])
        .await?;
        for i in 1..=rows {
            taos.exec(format!(
                "INSERT INTO `{db_src}`.t{i} USING `{db_src}`.stb TAGS({i}) VALUES (now, {i}.0)"
            ))
            .await?;
        }
        Ok(())
    }

    async fn run_tmq_to_local(
        ws_enable: bool,
        host: &str,
        db_src: &str,
        topic: &str,
        backup_dir: &str,
    ) -> anyhow::Result<()> {
        // start a tmq_to_local task to generate backup files
        let (from, to) = if ws_enable {
            let from =
                format!("tmq+ws://{host}:6041/{db_src}?use.topic.name={topic}").into_dsn()?;
            let to = format!("local:{backup_dir}",).into_dsn()?;
            (from, to)
        } else {
            let from = format!("tmq://{host}/{db_src}?use.topic.name={topic}").into_dsn()?;
            let to = format!("local:{backup_dir}",).into_dsn()?;
            (from, to)
        };
        tmq_to_local::tmq_to_local(None, from, to, CancellationToken::new()).await
    }
}
