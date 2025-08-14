use crate::conf::LocalRestoreConfig;

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use taosx_core::taoz::ZFile;
use tokio::sync::Mutex;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::{Stream, StreamExt};

type NameFilter = Box<dyn Fn(&str) -> bool + Send + Sync>;
type FileSorter = Box<dyn Fn(&Path, &Path) -> std::cmp::Ordering + Send + Sync>;

pub struct FileWatcher {
    /// 文件路径
    dir: PathBuf,
    /// 监视新文件的间隔
    interval: Duration,
    /// 文件名的过滤条件
    name_filter: Arc<Option<NameFilter>>,
    /// 文件名的排序规则
    sorter: Arc<Option<FileSorter>>,

    /// 已经确认稳定并发送过的文件 (hash of path)
    seen_files: Arc<Mutex<HashSet<u64>>>,
    /// 正在观察稳定性的文件: hash -> (size, modified_time)
    pending_files: Arc<Mutex<HashMap<u64, (u64, SystemTime)>>>,
    /// 停止标志
    stop_flag: Arc<AtomicBool>,
    /// 首次扫描标志：首次扫描时直接返回已有文件
    initial_bootstrap: Arc<AtomicBool>,
}

/// 使用 LocalRestoreConfig 创建 FileWatcher，默认使用 500 ms 的间隔
/// ### 文件名称的过滤条件：
/// 1. 文件名以 topic 开头，以 .z 结尾
/// 2. 如果 config.start_from 有值，那么，文件的时间戳必须大于等于 start_from 条件
/// ### 文件名称的排序规则：
/// 按照备份点 ts 时间戳 + 文件序号 idx 排序
impl From<LocalRestoreConfig> for FileWatcher {
    fn from(config: LocalRestoreConfig) -> Self {
        let backup_dir = config.backup_dir.clone();

        let name_filter = Some(Box::new(move |file_name: &str| {
            // 文件名以.z结尾
            if !file_name.ends_with(".z") {
                return false;
            }
            // 如果有from条件，那么，文件的时间戳必须大于等于start_from条件
            if let Some(from) = config.start_from {
                return match ZFile::parse_file_name(file_name) {
                    Err(_) => false,
                    Ok((_, ts, _, _)) => ts >= from,
                };
            }
            true
        }) as NameFilter);

        let sorter = Some(Box::new(|a: &Path, b: &Path| compare_file_name(a, b)) as FileSorter);

        Self::new(backup_dir, Duration::from_millis(500), name_filter, sorter)
    }
}

fn compare_file_name(a: &Path, b: &Path) -> std::cmp::Ordering {
    let parse = |path: &Path| -> Result<(_, _, _, _), ()> {
        let name = path.file_name().unwrap().to_string_lossy();
        ZFile::parse_file_name(&name).map_err(|_| ())
    };

    match (parse(a), parse(b)) {
        (Ok((_, a_ts, a_vg_id, a_idx)), Ok((_, b_ts, b_vg_id, b_idx))) => match a_ts.cmp(&b_ts) {
            std::cmp::Ordering::Less => std::cmp::Ordering::Less,
            std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => match a_vg_id.cmp(&b_vg_id) {
                std::cmp::Ordering::Less => std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                std::cmp::Ordering::Equal => a_idx.cmp(&b_idx),
            },
        },
        (Ok(_), Err(_)) => std::cmp::Ordering::Less, // 有效文件在前
        (Err(_), Ok(_)) => std::cmp::Ordering::Greater, // 无效文件在后
        _ => std::cmp::Ordering::Equal,              // 都无效
    }
}

impl FileWatcher {
    pub fn new(
        dir: PathBuf,
        interval: Duration,
        name_filter: Option<NameFilter>,
        sorter: Option<FileSorter>,
    ) -> Self {
        Self {
            dir,
            seen_files: Arc::new(Mutex::new(HashSet::new())),
            interval,
            name_filter: Arc::new(name_filter),
            sorter: Arc::new(sorter),
            stop_flag: Arc::new(AtomicBool::new(false)),
            pending_files: Arc::new(Mutex::new(HashMap::new())),
            initial_bootstrap: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn get_stop_flag(&self) -> Arc<AtomicBool> {
        self.stop_flag.clone()
    }

    pub fn into_stream(self) -> impl Stream<Item = Vec<PathBuf>> {
        IntervalStream::new(tokio::time::interval(self.interval))
            .then(move |_| {
                let dir = self.dir.clone();
                // Downgraded to debug to reduce noise (was info)
                tracing::debug!("file watcher started: {:?}", dir);
                let seen_files = self.seen_files.clone();
                let pending_files = self.pending_files.clone();
                let stop_flag = self.stop_flag.clone();
                let name_filter = self.name_filter.clone();
                let sorter = self.sorter.clone();
                let initial_bootstrap = self.initial_bootstrap.clone();

                async move {
                    if stop_flag.load(Ordering::Relaxed) {
                        tracing::debug!("stop flag is set, stop watching files");
                        return None;
                    }

                    let mut new_files = vec![];
                    let mut seen_files = seen_files.lock().await;
                    let mut pending_files = pending_files.lock().await;
                    let first_scan = initial_bootstrap.load(Ordering::Relaxed);

                    // 读取目录下的所有文件
                    let mut dir_entries = tokio::fs::read_dir(&dir)
                        .await
                        .inspect_err(|err| {
                            tracing::error!("failed to read dir: {:?}, err: {:?}", dir, err);
                        })
                        .ok()?;
                    tracing::trace!("read dir: {:?}", dir);
                    while let Some(entry) = dir_entries
                        .next_entry()
                        .await
                        .inspect_err(|err| {
                            tracing::error!("failed to read entry, err: {:?}", err);
                        })
                        .ok()? {
                        // 过滤文件
                        if let Some(ref name_filter) = *name_filter {
                            let file_name = entry.file_name();
                            if !name_filter(file_name.to_string_lossy().as_ref()) {
                                continue;
                            }
                        }
                        let path = entry.path();
                        // 为每个文件单独创建 hasher，避免多个文件累加 hash 状态导致冲突
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        path.as_path().hash(&mut hasher);
                        let path_hash = hasher.finish();
                        // 如果是文件，且没有看到过，那么，处理
                        if path.is_file() && !seen_files.contains(&path_hash) {
                            if first_scan {
                                // 首次扫描：直接返回
                                new_files.push(path.clone());
                                seen_files.insert(path_hash);
                                continue;
                            }
                            match tokio::fs::metadata(&path).await {
                                Ok(meta) => {
                                    let size = meta.len();
                                    let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                                    match pending_files.get(&path_hash) {
                                        None => {
                                            // 第一次看到：记录，等待下次确认稳定
                                            pending_files.insert(path_hash, (size, modified));
                                        }
                                        Some((prev_size, prev_mod)) => {
                                            if *prev_size == size && *prev_mod == modified {
                                                // 稳定：接受
                                                new_files.push(path.clone());
                                                seen_files.insert(path_hash);
                                                pending_files.remove(&path_hash);
                                            } else {
                                                // 还在变化：刷新基线
                                                pending_files.insert(path_hash, (size, modified));
                                            }
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::debug!("file metadata not available (may have vanished): {:?}, err: {:?}", path, err);
                                    pending_files.remove(&path_hash);
                                }
                            }
                        }
                    }
                    if first_scan {
                        initial_bootstrap.store(false, Ordering::Relaxed);
                    }
                    drop(seen_files);
                    drop(pending_files);

                    // 排序
                    if let Some(sorter) = sorter.deref() {
                        new_files.sort_by(|a, b| sorter(a, b));
                    }

                    tracing::trace!("read files: {:?}", new_files);
                    Some(new_files)
                }
            })
            .map_while(|x| x)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use std::sync::atomic::Ordering;
    use tokio_stream::StreamExt;

    #[test]
    fn test_sort_file_name() {
        let files = vec![
            "xb4a5d2d8954-1735784520-6179-1.z",
            "xb4a5d2d8954-1735784520-6179-10.z",
            "xb4a5d2d8954-1735784520-6179-2.z",
            "xb4a5d2d8954-1735784520-6179-3.z",
            "xb4a5d2d8954-1735784520-6179-4.z",
            "xb4a5d2d8954-1735784520-6179-5.z",
            "xb4a5d2d8954-1735784520-6179-6.z",
            "xb4a5d2d8954-1735784520-6179-7.z",
            "xb4a5d2d8954-1735784520-6179-8.z",
            "xb4a5d2d8954-1735784520-6179-9.z",
            "xb4a5d2d8954-1735784520-6180-1.z",
            "xb4a5d2d8954-1735784520-6180-10.z",
            "xb4a5d2d8954-1735784520-6180-2.z",
            "xb4a5d2d8954-1735784520-6180-3.z",
            "xb4a5d2d8954-1735784520-6180-4.z",
            "xb4a5d2d8954-1735784520-6180-5.z",
            "xb4a5d2d8954-1735784520-6180-6.z",
            "xb4a5d2d8954-1735784520-6180-7.z",
            "xb4a5d2d8954-1735784520-6180-8.z",
            "xb4a5d2d8954-1735784520-6180-9.z",
            "abc",
            "abcde",
        ];
        let files = files
            .iter()
            .sorted_by(|a, b| compare_file_name(Path::new(a), Path::new(b)))
            .map(|s| s.to_string())
            .collect_vec();
        assert_eq!(files[0], "xb4a5d2d8954-1735784520-6179-1.z");
        assert_eq!(files[9], "xb4a5d2d8954-1735784520-6179-10.z");
        assert_eq!(files[10], "xb4a5d2d8954-1735784520-6180-1.z");
        assert_eq!(files[19], "xb4a5d2d8954-1735784520-6180-10.z");
        assert_eq!(files[20], "abc");
        assert_eq!(files[21], "abcde");
    }

    #[tokio::test]
    async fn test_file_watcher() {
        // 在临时目录下创建 4 个文件
        let dir = tempfile::tempdir().unwrap();
        let file1 = dir.path().join("topic-20210901-1-1.z");
        let file4 = dir.path().join("topic-20210902-1-2.z");
        let file2 = dir.path().join("topic-20210901-1-2.z");
        let file3 = dir.path().join("topic-20210902-1-1.z");
        std::fs::File::create(&file1).unwrap();
        std::fs::File::create(&file2).unwrap();
        std::fs::File::create(&file3).unwrap();
        std::fs::File::create(&file4).unwrap();

        // 创建 watcher
        let watcher = FileWatcher::new(
            dir.keep(),
            Duration::from_secs(2),
            Some(Box::new(move |file_name: &str| {
                file_name.starts_with("topic") && file_name.ends_with(".z")
            }) as NameFilter),
            Some(Box::new(move |a: &Path, b: &Path| compare_file_name(a, b)) as FileSorter),
        );

        let stop_flag = watcher.stop_flag.clone();
        // 停止条件：运行 5 秒后停止
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            stop_flag.store(true, Ordering::Relaxed);
        });

        // 转换为 stream
        let watcher = watcher.into_stream();
        tokio::pin!(watcher);
        // 读取文件
        let mut count = 0;
        while let Some(files) = watcher.next().await {
            println!("{:?}", files);
            if count == 0 {
                assert_eq!(files.len(), 4);
            }
            if count == 1 {
                assert_eq!(files.len(), 0);
            }
            count += 1;
        }
        assert_eq!(count, 3);
    }
}
