use crate::local_to_taos::conf::LocalRestoreConfig;
use crate::taoz::ZFile;
use std::collections::HashSet;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::{Stream, StreamExt};

pub struct FileWatcher {
    /// 文件路径
    dir: PathBuf,
    /// 监视新文件的间隔
    interval: Duration,
    /// 已经看到的文件
    seen_files: Arc<Mutex<HashSet<PathBuf>>>,
    /// 文件名的过滤条件
    name_filter: Arc<Option<Box<dyn Fn(&str) -> bool + Send + Sync>>>,
    /// 文件名的排序规则
    sorter: Arc<Option<Box<dyn Fn(&PathBuf, &PathBuf) -> std::cmp::Ordering + Send + Sync>>>,
    /// 停止标志
    stop_flag: Arc<AtomicBool>,
}

impl From<LocalRestoreConfig> for FileWatcher {
    fn from(config: LocalRestoreConfig) -> Self {
        let backup_dir = config.backup_dir.clone();

        let name_filter = Some(Box::new(move |file_name: &str| {
            // 文件名以topic开头，以.z结尾
            if !file_name.starts_with(config.topic.as_str()) || !file_name.ends_with(".z") {
                return false;
            }
            // 如果有from条件，那么，文件的时间戳必须大于等于from条件
            if let Some(from) = config.from {
                return match ZFile::parse_file_name(file_name) {
                    Err(_) => false,
                    Ok((_, ts, _, _)) => ts >= from,
                };
            }
            true
        }) as Box<dyn Fn(&str) -> bool + Send + Sync>);

        let sorter = Some(Box::new(|a: &PathBuf, b: &PathBuf| compare_file_name(a, b))
            as Box<dyn Fn(&PathBuf, &PathBuf) -> std::cmp::Ordering + Send + Sync>);

        Self::new(backup_dir, Duration::from_millis(500), name_filter, sorter)
    }
}

fn compare_file_name(a: &PathBuf, b: &PathBuf) -> std::cmp::Ordering {
    let a = a.file_name().unwrap().to_string_lossy();
    let b = b.file_name().unwrap().to_string_lossy();
    let (_, a_ts, _, a_idx) = ZFile::parse_file_name(a.as_ref()).unwrap();
    let (_, b_ts, _, b_idx) = ZFile::parse_file_name(b.as_ref()).unwrap();

    // 按照备份点和文件序号排序
    match a_ts.cmp(&b_ts) {
        std::cmp::Ordering::Less => std::cmp::Ordering::Less,
        std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
        std::cmp::Ordering::Equal => a_idx.cmp(&b_idx),
    }
}

impl FileWatcher {
    pub fn new(
        dir: PathBuf,
        interval: Duration,
        name_filter: Option<Box<dyn Fn(&str) -> bool + Send + Sync>>,
        sorter: Option<Box<dyn Fn(&PathBuf, &PathBuf) -> std::cmp::Ordering + Send + Sync>>,
    ) -> Self {
        Self {
            dir,
            seen_files: Arc::new(Mutex::new(HashSet::new())),
            interval,
            name_filter: Arc::new(name_filter),
            sorter: Arc::new(sorter),
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn get_stop_flag(&self) -> Arc<AtomicBool> {
        self.stop_flag.clone()
    }

    pub fn into_stream(self) -> impl Stream<Item = Vec<PathBuf>> {
        IntervalStream::new(tokio::time::interval(self.interval))
            .then(move |_| {
                let dir = self.dir.clone();
                let seen_files = self.seen_files.clone();
                let stop_flag = self.stop_flag.clone();
                let name_filter = self.name_filter.clone();
                let sorter = self.sorter.clone();

                async move {
                    if stop_flag.load(Ordering::Relaxed) {
                        return None;
                    }

                    let mut new_files = vec![];
                    let mut seen_files = seen_files.lock().await;

                    // 读取目录下的所有文件
                    let mut dir_entries = tokio::fs::read_dir(&dir).await.ok()?;
                    while let Some(entry) = dir_entries.next_entry().await.ok()? {
                        // 过滤文件
                        if let Some(ref name_filter) = *name_filter {
                            let file_name = entry.file_name();
                            if !name_filter(file_name.to_string_lossy().as_ref()) {
                                continue;
                            }
                        }
                        // 如果是文件，且没有看到过，那么，加入到新文件列表中
                        let path = entry.path();
                        if path.is_file() && !seen_files.contains(&path) {
                            new_files.push(path.clone());
                            seen_files.insert(path);
                        }
                    }
                    drop(seen_files);

                    // 排序
                    if let Some(sorter) = sorter.deref() {
                        new_files.sort_by(|a, b| sorter(a, b));
                    }

                    Some(new_files)
                }
            })
            .map_while(|x| x)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio_stream::StreamExt;

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
            PathBuf::from(dir.into_path()),
            Duration::from_secs(2),
            Some(Box::new(move |file_name: &str| {
                file_name.starts_with("topic") && file_name.ends_with(".z")
            }) as Box<dyn Fn(&str) -> bool + Send + Sync>),
            Some(
                Box::new(move |a: &PathBuf, b: &PathBuf| compare_file_name(a, b))
                    as Box<dyn Fn(&PathBuf, &PathBuf) -> std::cmp::Ordering + Send + Sync>,
            ),
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
