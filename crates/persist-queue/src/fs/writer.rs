use std::{
    collections::BTreeMap,
    fs::{File, TryLockError},
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::SinkExt;
use parking_lot::Mutex;
use snafu::ResultExt;
use tokio::io::AsyncSeekExt;
use tokio_util::codec::FramedWrite;

use crate::{
    DirLockedSnafu, ExclusiveLockFileSnafu, OpenFileSnafu, RawWriter, Result, SeekFileSnafu,
    SyncFileDataSnafu,
};

use super::{EntryPosition, LOCK_FILE_EXTENSION, codec::WriteCodec, format_segment_id};

pub struct Writer<B> {
    dir: PathBuf,
    segment_size: usize,
    _lock_file: File,

    _dir_watcher: Arc<notify::RecommendedWatcher>,

    framed: FramedWrite<tokio::fs::File, WriteCodec<B>>,
}

impl<B> Writer<B> {
    pub async fn new(
        dir: impl AsRef<Path>,
        segment_size: usize,
        segments: Arc<Mutex<BTreeMap<u64, PathBuf>>>,
        dir_watcher: Arc<notify::RecommendedWatcher>,
    ) -> Result<Self> {
        let dir = dir.as_ref();
        // 全局锁
        let lock_file_path = dir.join(format!("global_writer{LOCK_FILE_EXTENSION}"));
        let _lock_file = std::fs::File::create(&lock_file_path).context(OpenFileSnafu {
            path: &lock_file_path,
        })?;
        match _lock_file.try_lock() {
            Ok(_) => {}
            Err(TryLockError::WouldBlock) => return DirLockedSnafu.fail(),
            Err(TryLockError::Error(e)) => {
                return Err(e).context(ExclusiveLockFileSnafu {
                    path: &lock_file_path,
                })?;
            }
        }

        let max = {
            segments
                .lock()
                .last_key_value()
                .map(|(id, path)| (*id, path.clone()))
        };
        let framed = match max {
            Some((segment_id, path)) => {
                let mut file = tokio::fs::File::options()
                    .append(true)
                    .open(&path)
                    .await
                    .context(OpenFileSnafu { path: &path })?;
                let offset = file
                    .seek(SeekFrom::End(0))
                    .await
                    .context(SeekFileSnafu { path: &path })?;
                FramedWrite::new(
                    file,
                    WriteCodec::new(path, EntryPosition::new(segment_id, offset)),
                )
            }
            None => {
                // 创建新文件
                let path = dir.join(format_segment_id(0));
                let file = tokio::fs::File::options()
                    .append(true)
                    .create(true)
                    .open(&path)
                    .await
                    .context(OpenFileSnafu { path: &path })?;
                FramedWrite::new(file, WriteCodec::new(path, EntryPosition::new(0, 0)))
            }
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            segment_size,
            _lock_file,
            _dir_watcher: dir_watcher,
            framed,
        })
    }
}

impl<B> RawWriter<B> for Writer<B>
where
    B: AsRef<[u8]> + Send,
{
    type EntryPosition = EntryPosition;

    fn position(&self) -> Self::EntryPosition {
        self.framed.encoder().position
    }

    async fn write(&mut self, payloads: Vec<B>) -> Result<()> {
        for payload in payloads {
            if self.framed.encoder().position.end_offset >= self.segment_size as u64 {
                self.sync_data().await?;
                // rotate new file
                let new_segment_id = self.framed.encoder().position.segment_id + 1;
                let path = self.dir.join(format_segment_id(new_segment_id));
                let file = tokio::fs::File::options()
                    .append(true)
                    .create_new(true)
                    .open(&path)
                    .await
                    .context(OpenFileSnafu { path: &path })?;
                self.framed = FramedWrite::new(
                    file,
                    WriteCodec::new(path, EntryPosition::new(new_segment_id, 0)),
                );
            }

            self.framed.feed(payload).await?;
        }

        self.framed.flush().await?;
        Ok(())
    }

    async fn sync_data(&mut self) -> Result<()> {
        self.framed.flush().await?;
        self.framed
            .get_ref()
            .sync_all()
            .await
            .context(SyncFileDataSnafu {
                path: &self.framed.encoder().path,
            })?;
        Ok(())
    }
}
