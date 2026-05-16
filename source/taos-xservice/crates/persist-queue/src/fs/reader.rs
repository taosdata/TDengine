use std::{
    collections::BTreeMap,
    fs::TryLockError,
    future::Future,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::StreamExt;
use notify::Watcher;
use parking_lot::Mutex;
use snafu::ResultExt;
use tokio::{io::AsyncSeekExt, time::timeout_at};
use tokio_util::{codec::FramedRead, sync::CancellationToken};

use crate::{
    AddWatchSnafu, DelWatchSnafu, Entry, ExclusiveLockFileSnafu, FileLockedSnafu, OpenFileSnafu,
    RawReader, RemoveFileSnafu, Result, SeekFileSnafu, SharedLockFileSnafu, fs::format_segment_id,
};

use super::{EntryPosition, LOCK_FILE_EXTENSION, ReadFrom, codec::ReadCodec};

enum ReadState {
    Position(ReadFrom),
    Reader(InnerReader),
}

impl std::fmt::Display for ReadState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadState::Position(read_from) => write!(f, "POSITION-{}", read_from),
            ReadState::Reader(inner_reader) => {
                write!(f, "READER-{}", inner_reader.framed.decoder().position)
            }
        }
    }
}

impl ReadState {
    fn reader(
        path: PathBuf,
        file: tokio::fs::File,
        lock_file: std::fs::File,
        position: EntryPosition,
        buffer_size: Option<usize>,
    ) -> Self {
        let framed = match buffer_size {
            Some(capacity) => FramedRead::with_capacity(file, ReadCodec::new(position), capacity),
            None => FramedRead::new(file, ReadCodec::new(position)),
        };
        Self::Reader(InnerReader {
            segment: path,
            _lock_file: lock_file,
            framed,
        })
    }

    async fn new(
        dir: impl AsRef<Path>,
        segment_size: usize,
        buffer_size: Option<usize>,
        from: ReadFrom,
        segments: Arc<Mutex<BTreeMap<u64, PathBuf>>>,
    ) -> Result<Self> {
        let dir = dir.as_ref();
        match from {
            ReadFrom::Earliest => {
                let first_entry = {
                    segments
                        .lock()
                        .first_key_value()
                        .map(|(id, path)| (*id, path.clone()))
                };
                match first_entry {
                    Some((segment_id, path)) => {
                        let (lock_file, (file, offset)) =
                            open_file_with_lock(dir, segment_id, &path, None).await?;
                        Ok(Self::reader(
                            path,
                            file,
                            lock_file,
                            EntryPosition::new(segment_id, offset),
                            buffer_size,
                        ))
                    }
                    None => Ok(Self::Position(from)),
                }
            }
            ReadFrom::Latest => {
                let max = {
                    segments
                        .lock()
                        .last_key_value()
                        .map(|(id, path)| (*id, path.clone()))
                };
                // 当前目录中没有数据文件
                let Some((segment_id, path)) = max else {
                    return Ok(Self::Position(from));
                };
                let (lock_file, (file, offset)) =
                    open_file_with_lock(dir, segment_id, &path, Some(SeekFrom::End(0))).await?;
                Ok(Self::reader(
                    path,
                    file,
                    lock_file,
                    EntryPosition::new(segment_id, offset),
                    buffer_size,
                ))
            }
            ReadFrom::LastPosition(last_position) => {
                let end_offset = last_position.end_offset;
                let (segment_id, end_offset) = if end_offset >= segment_size as u64 {
                    // 触发文件滚动
                    (last_position.segment_id + 1, 0)
                } else if segments.lock().contains_key(&last_position.segment_id) {
                    // 当前文件
                    (last_position.segment_id, last_position.end_offset)
                } else {
                    // 文件不存在，从头开始读
                    (last_position.segment_id, 0)
                };

                // 从前往后找大于或等于当前 segment id 的
                let entry = {
                    let segments = segments.lock();
                    segments
                        .iter()
                        .find(|(id, _)| **id >= segment_id)
                        .map(|(id, path)| (*id, path.clone()))
                };
                match entry {
                    Some((id, path)) => {
                        let (lock_file, (file, offset)) = if id == segment_id {
                            open_file_with_lock(
                                dir,
                                segment_id,
                                &path,
                                Some(SeekFrom::Start(end_offset)),
                            )
                            .await?
                        } else {
                            open_file_with_lock(dir, id, &path, None).await?
                        };
                        Ok(Self::reader(
                            path,
                            file,
                            lock_file,
                            EntryPosition::new(id, offset),
                            buffer_size,
                        ))
                    }
                    None => {
                        // 找当前目录最大的文件，从下一个开始读
                        // offset 设置为文件尾，确保下次读取时从新文件开始读
                        // 如果当前目录下没有文件，则从 Earliest 开始读
                        let entry = {
                            let segments = segments.lock();
                            segments
                                .last_key_value()
                                .map(|(id, path)| (*id, path.clone()))
                        };
                        match entry {
                            Some((segment_id, _)) => Ok(Self::Position(ReadFrom::LastPosition(
                                EntryPosition::new(segment_id, segment_size as u64),
                            ))),
                            None => Ok(Self::Position(ReadFrom::Earliest)),
                        }
                    }
                }
            }
        }
    }

    fn position(&self) -> ReadState {
        match self {
            ReadState::Position(read_from) => ReadState::Position(*read_from),
            ReadState::Reader(inner) => {
                ReadState::Position(ReadFrom::LastPosition(inner.framed.decoder().position))
            }
        }
    }
}

async fn open_file_with_lock(
    dir: &Path,
    segment_id: u64,
    path: &Path,
    seek_from: Option<SeekFrom>,
) -> Result<(std::fs::File, (tokio::fs::File, u64))> {
    let lock_file_path = dir.join(format_segment_id(segment_id) + LOCK_FILE_EXTENSION);
    let lock_file = std::fs::File::create(&lock_file_path).context(OpenFileSnafu {
        path: &lock_file_path,
    })?;
    match lock_file.try_lock_shared() {
        Ok(_) => {}
        Err(TryLockError::WouldBlock) => {
            return FileLockedSnafu {
                path: &lock_file_path,
            }
            .fail();
        }
        Err(TryLockError::Error(e)) => Err(e).context(SharedLockFileSnafu {
            path: lock_file_path,
        })?,
    }
    let mut file = tokio::fs::File::open(&path)
        .await
        .context(OpenFileSnafu { path: &path })?;
    let offset = match seek_from {
        Some(from) => file
            .seek(from)
            .await
            .context(SeekFileSnafu { path: &path })?,
        None => 0,
    };

    Ok((lock_file, (file, offset)))
}

struct InnerReader {
    segment: PathBuf,
    _lock_file: std::fs::File,
    framed: FramedRead<tokio::fs::File, ReadCodec>,
}

impl Drop for InnerReader {
    fn drop(&mut self) {
        // file drop 时会自动 unlock，但这里加一层保险
        // TODO: use std file lock instead after stable
        #[allow(unstable_name_collisions)]
        let _ = self._lock_file.unlock();
    }
}

pub struct Reader {
    dir: PathBuf,
    segment_size: usize,
    buffer_size: Option<usize>,

    state: ReadState,

    file_watcher: notify::RecommendedWatcher,
    file_modify_notifier: Arc<tokio::sync::Notify>,

    _dir_watcher: Arc<notify::RecommendedWatcher>,
    segments: Arc<Mutex<BTreeMap<u64, PathBuf>>>,
    dir_modify_notifier: Arc<tokio::sync::Notify>,
}

impl Reader {
    pub async fn new(
        dir: impl AsRef<Path>,
        segment_size: usize,
        buffer_size: Option<usize>,
        from: ReadFrom,
        segments: Arc<Mutex<BTreeMap<u64, PathBuf>>>,
        dir_watcher: Arc<notify::RecommendedWatcher>,
        dir_modify_notifier: Arc<tokio::sync::Notify>,
    ) -> Result<Self> {
        let state = ReadState::new(&dir, segment_size, buffer_size, from, segments.clone()).await?;

        let file_modify_notifier = Arc::new(tokio::sync::Notify::new());

        let mut watcher = {
            use crate::BuildWatcherSnafu;
            use notify::event::ModifyKind;
            let handler = {
                let file_modify_notifier = file_modify_notifier.clone();
                move |event: std::result::Result<notify::Event, notify::Error>| {
                    let Ok(event) = event else { return };
                    if matches!(
                        event.kind,
                        notify::EventKind::Modify(ModifyKind::Data(_))
                            | notify::EventKind::Modify(ModifyKind::Any)
                    ) {
                        file_modify_notifier.notify_one();
                    }
                }
            };
            notify::RecommendedWatcher::new(handler, notify::Config::default())
                .context(BuildWatcherSnafu)?
        };
        if let ReadState::Reader(inner) = &state {
            watcher
                .watch(&inner.segment, notify::RecursiveMode::NonRecursive)
                .context(AddWatchSnafu {
                    path: &inner.segment,
                })?;
        }

        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            segment_size,
            buffer_size,
            state,
            file_watcher: watcher,
            file_modify_notifier,
            segments,
            _dir_watcher: dir_watcher,
            dir_modify_notifier,
        })
    }

    async fn rotate(&mut self, state: ReadState) -> Result<ReadState> {
        if let ReadState::Reader(inner) = &state {
            self.file_watcher
                .unwatch(&inner.segment)
                .context(DelWatchSnafu {
                    path: &inner.segment,
                })?;
        }

        let from = match state {
            ReadState::Position(from) => from,
            ReadState::Reader(inner) => ReadFrom::LastPosition(inner.framed.decoder().position),
        };

        match self.new_state(from).await? {
            state @ ReadState::Position(_) => Ok(state),
            ReadState::Reader(inner) => {
                self.file_watcher
                    .watch(&inner.segment, notify::RecursiveMode::NonRecursive)
                    .context(AddWatchSnafu {
                        path: &inner.segment,
                    })?;
                Ok(ReadState::Reader(inner))
            }
        }
    }

    async fn rotate_timeout_at(
        &mut self,
        state: ReadState,
        deadline: Option<tokio::time::Instant>,
        cancel: &CancellationToken,
    ) -> Result<ReadState> {
        if let ReadState::Reader(inner) = &state {
            self.file_watcher
                .unwatch(&inner.segment)
                .context(DelWatchSnafu {
                    path: &inner.segment,
                })?;
        }
        let from = match state {
            ReadState::Position(from) => from,
            ReadState::Reader(inner) => ReadFrom::LastPosition(inner.framed.decoder().position),
        };

        loop {
            match self.new_state(from).await? {
                state @ ReadState::Position(_) => {
                    match wait(deadline, cancel, self.dir_modify_notifier.notified()).await {
                        Some(_) => continue,
                        None => return Ok(state),
                    }
                }
                ReadState::Reader(inner) => {
                    self.file_watcher
                        .watch(&inner.segment, notify::RecursiveMode::NonRecursive)
                        .context(AddWatchSnafu {
                            path: &inner.segment,
                        })?;
                    return Ok(ReadState::Reader(inner));
                }
            }
        }
    }

    fn need_rotate(&self, inner: &InnerReader) -> bool {
        inner.framed.decoder().position.end_offset >= self.segment_size as u64
    }

    async fn new_state(&self, from: ReadFrom) -> Result<ReadState> {
        ReadState::new(
            &self.dir,
            self.segment_size,
            self.buffer_size,
            from,
            self.segments.clone(),
        )
        .await
    }
}

impl RawReader for Reader {
    type EntryPosition = super::EntryPosition;

    async fn read(&mut self, max_batch_size: usize) -> Result<Vec<Entry<Self::EntryPosition>>> {
        let current_position = self.state.position();
        let mut inner = match std::mem::replace(&mut self.state, current_position) {
            ReadState::Reader(inner) => inner,
            ReadState::Position(from) => match self.rotate(ReadState::Position(from)).await? {
                ReadState::Position(_) => {
                    return Ok(vec![]);
                }
                ReadState::Reader(inner) => inner,
            },
        };

        let mut entries = Vec::with_capacity(max_batch_size);
        let mut last_instant = Instant::now();
        const LOG_DURATION: Duration = Duration::from_secs(5);
        loop {
            match inner.framed.next().await.transpose()? {
                Some(entry) => {
                    entries.push(entry);
                    if entries.len() >= max_batch_size {
                        break;
                    }
                }
                None => {
                    if !self.need_rotate(&inner) {
                        break;
                    }
                    if last_instant.elapsed() >= LOG_DURATION {
                        tracing::info!("perssit file reader will rotate to next file");
                    }
                    match self.rotate(ReadState::Reader(inner)).await? {
                        ReadState::Position(from) => {
                            if last_instant.elapsed() >= LOG_DURATION {
                                tracing::info!(
                                    "perssit file reader next rotate file from {from} not found"
                                );
                            }
                            self.state = ReadState::Position(from);
                            return Ok(entries);
                        }
                        ReadState::Reader(new_inner) => {
                            if last_instant.elapsed() >= LOG_DURATION {
                                tracing::info!(
                                    seg = ?new_inner.segment,
                                    "perssit file reader rotate to next file"
                                );
                            }
                            inner = new_inner
                        }
                    }
                }
            }
            if last_instant.elapsed() >= LOG_DURATION {
                last_instant = Instant::now();
            }
        }

        self.state = ReadState::Reader(inner);

        Ok(entries)
    }

    async fn read_util(
        &mut self,
        min_batch_size: usize,
        max_batch_size: usize,
        timeout: Option<std::time::Duration>,
        cancel: &CancellationToken,
    ) -> Result<Vec<Entry<Self::EntryPosition>>> {
        let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);

        let current_position = self.state.position();
        let mut inner = match std::mem::replace(&mut self.state, current_position) {
            ReadState::Reader(inner) => inner,
            ReadState::Position(from) => {
                let Some(res) = cancel
                    .run_until_cancelled(self.rotate_timeout_at(
                        ReadState::Position(from),
                        deadline,
                        cancel,
                    ))
                    .await
                else {
                    return Ok(vec![]);
                };
                match res? {
                    ReadState::Position(_) => {
                        return Ok(vec![]);
                    }
                    ReadState::Reader(inner) => inner,
                }
            }
        };

        let mut entries = Vec::with_capacity(max_batch_size);
        while let Some(res) = wait(deadline, cancel, inner.framed.next()).await {
            match res.transpose()? {
                Some(entry) => {
                    entries.push(entry);
                    if entries.len() >= max_batch_size {
                        break;
                    }
                }
                None => {
                    if entries.len() >= min_batch_size {
                        break;
                    }
                    // 文件被删除，或者是读到了文件结尾，则切换下一个文件
                    if self.need_rotate(&inner) {
                        tracing::info!(path = ?inner.segment, "perssit file reader will rotate to next file");
                        match self
                            .rotate_timeout_at(ReadState::Reader(inner), deadline, cancel)
                            .await?
                        {
                            ReadState::Position(from) => {
                                tracing::info!(
                                    "perssit file reader next rotate file from {from} not found"
                                );
                                self.state = ReadState::Position(from);
                                return Ok(entries);
                            }
                            ReadState::Reader(new_inner) => {
                                tracing::info!(
                                    seg = ?new_inner.segment,
                                    "perssit file reader rotate to next file"
                                );
                                inner = new_inner
                            }
                        }
                        continue;
                    }

                    // 等待文件新内容，超时返回
                    if wait(deadline, cancel, self.file_modify_notifier.notified())
                        .await
                        .is_none()
                    {
                        break;
                    }
                }
            }
        }

        self.state = ReadState::Reader(inner);

        Ok(entries)
    }

    async fn vacuum(&mut self) -> Result<()> {
        let segments = { self.segments.lock().clone() };

        let segment_id = match &self.state {
            ReadState::Position(from) => match from {
                ReadFrom::Earliest => return Ok(()),
                ReadFrom::Latest => match segments.last_key_value() {
                    Some((&id, _)) => id,
                    None => return Ok(()),
                },
                ReadFrom::LastPosition(position) => position.segment_id,
            },
            ReadState::Reader(inner) => inner.framed.decoder().position.segment_id,
        };

        for (&id, path) in segments.range(..segment_id) {
            let lock_file_path = self.dir.join(format_segment_id(id) + LOCK_FILE_EXTENSION);
            // 这里在文件不存在时直接创建文件，避免在判断文件不存在后有其他进程创建了文件导致数据文件误删
            let lock_file = std::fs::File::create(&lock_file_path).context(OpenFileSnafu {
                path: &lock_file_path,
            })?;
            match lock_file.try_lock() {
                Ok(_) => {}
                Err(TryLockError::WouldBlock) => {
                    tracing::warn!("vacuum {} failed, file is locked, ignore", path.display());
                    continue;
                }
                Err(TryLockError::Error(e)) => Err(e).context(ExclusiveLockFileSnafu {
                    path: &lock_file_path,
                })?,
            }
            tokio::fs::remove_file(&path)
                .await
                .context(RemoveFileSnafu { path: &path })?;
            tokio::fs::remove_file(&lock_file_path)
                .await
                .context(RemoveFileSnafu {
                    path: &lock_file_path,
                })?;
            self.segments.lock().remove(&id);
        }

        Ok(())
    }
}

async fn wait<F, T>(
    deadline: Option<tokio::time::Instant>,
    cancel: &CancellationToken,
    future: F,
) -> Option<T>
where
    F: Future<Output = T>,
{
    match deadline {
        Some(deadline) => {
            let Some(Ok(res)) = cancel
                .run_until_cancelled(timeout_at(deadline, future))
                .await
            else {
                return None;
            };
            Some(res)
        }
        None => cancel.run_until_cancelled(future).await,
    }
}
