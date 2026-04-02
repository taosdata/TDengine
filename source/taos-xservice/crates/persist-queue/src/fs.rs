use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use bitfield::bitfield;
use futures::TryStreamExt;

use notify::{
    Watcher,
    event::{CreateKind, RemoveKind},
};
use parking_lot::Mutex;
use snafu::ResultExt;
use tokio_stream::wrappers::ReadDirStream;

use crate::{
    AddWatchSnafu, BuildWatcherSnafu, CreateDirSnafu, DirExistsSnafu, MultipleWriterSnafu,
    ReadDirSnafu, WalkDirSnafu,
};

mod codec;
pub mod reader;
pub mod writer;

type Result<T> = std::result::Result<T, crate::Error>;

const DEFAULT_SEGMENT_SIZE: usize = 1024 * 1024 * 1024; // 1GB
const LOCK_FILE_EXTENSION: &str = ".lock";
const SEGMENT_FILE_EXTENSION: &str = ".seg";

pub struct FsQueueBuilder {
    dir: PathBuf,
    segment_size: Option<usize>,
    buffer_size: Option<usize>,
}

impl FsQueueBuilder {
    pub fn segment_size(self, segment_size: usize) -> Self {
        Self {
            segment_size: Some(segment_size),
            ..self
        }
    }

    pub fn buffer_size(self, buffer_size: usize) -> Self {
        Self {
            buffer_size: Some(buffer_size),
            ..self
        }
    }

    pub async fn build(self) -> Result<FsQueue> {
        if !tokio::fs::try_exists(&self.dir)
            .await
            .context(DirExistsSnafu)?
        {
            tokio::fs::create_dir_all(&self.dir)
                .await
                .context(CreateDirSnafu)?;
        }

        let segments = find_all_segment_id(&self.dir).await?;
        let segments = Arc::new(Mutex::new(segments));

        let dir_modify_notifier = Arc::new(tokio::sync::Notify::new());
        let handler = {
            let segments = segments.clone();
            let dir_modify_notifier = dir_modify_notifier.clone();
            move |event: std::result::Result<notify::Event, notify::Error>| {
                let Ok(event) = event else { return };
                match event.kind {
                    notify::EventKind::Create(CreateKind::File)
                    | notify::EventKind::Create(CreateKind::Any) => {
                        let Some(path) = event.paths.into_iter().next() else {
                            return;
                        };
                        let Some(id) = parse_segment_id(&path) else {
                            return;
                        };
                        segments.lock().insert(id, path);
                        dir_modify_notifier.notify_waiters();
                    }
                    notify::EventKind::Remove(RemoveKind::File)
                    | notify::EventKind::Remove(RemoveKind::Any) => {
                        let Some(path) = event.paths.into_iter().next() else {
                            return;
                        };
                        let Some(id) = parse_segment_id(&path) else {
                            return;
                        };
                        segments.lock().remove(&id);
                    }
                    _ => {}
                }
            }
        };
        let mut watcher = notify::recommended_watcher(handler).context(BuildWatcherSnafu)?;
        watcher
            .watch(self.dir.as_ref(), notify::RecursiveMode::NonRecursive)
            .context(AddWatchSnafu { path: &self.dir })?;
        Ok(FsQueue {
            dir: self.dir,
            segment_size: self.segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE),
            buffer_size: self.buffer_size,
            has_writer: false,
            segments,
            _dir_watcher: Arc::new(watcher),
            dir_modify_notifier,
        })
    }
}

pub struct FsQueue {
    dir: PathBuf,
    segment_size: usize,
    buffer_size: Option<usize>,

    has_writer: bool,

    /// watch current dir
    _dir_watcher: Arc<notify::RecommendedWatcher>,
    dir_modify_notifier: Arc<tokio::sync::Notify>,
    segments: Arc<Mutex<BTreeMap<u64, PathBuf>>>,
}

impl FsQueue {
    pub fn builder(dir: impl AsRef<Path>) -> FsQueueBuilder {
        FsQueueBuilder {
            dir: dir.as_ref().to_path_buf(),
            segment_size: None,
            buffer_size: None,
        }
    }

    pub async fn new_reader(&self, from: ReadFrom) -> Result<reader::Reader> {
        reader::Reader::new(
            &self.dir,
            self.segment_size,
            self.buffer_size,
            from,
            self.segments.clone(),
            self._dir_watcher.clone(),
            self.dir_modify_notifier.clone(),
        )
        .await
    }

    pub async fn new_writer<B>(&mut self) -> Result<writer::Writer<B>> {
        snafu::ensure!(!self.has_writer, MultipleWriterSnafu);
        self.has_writer = true;
        writer::Writer::new(
            &self.dir,
            self.segment_size,
            self.segments.clone(),
            self._dir_watcher.clone(),
        )
        .await
    }

    pub fn segments(&self) -> BTreeMap<u64, PathBuf> {
        self.segments.lock().clone()
    }
}

pub(crate) async fn find_all_segment_id(dir: impl AsRef<Path>) -> Result<BTreeMap<u64, PathBuf>> {
    let read_dir = tokio::fs::read_dir(dir).await.context(ReadDirSnafu)?;
    ReadDirStream::new(read_dir)
        .try_filter_map(|entry| async move {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                return Ok(None);
            }
            let path = entry.path();
            Ok(parse_segment_id(&path).map(|id| (id, path)))
        })
        .try_collect()
        .await
        .context(WalkDirSnafu)
}

#[derive(Debug, Clone, Copy)]
pub enum ReadFrom {
    Earliest,
    Latest,
    LastPosition(EntryPosition),
}

impl std::fmt::Display for ReadFrom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadFrom::Earliest => write!(f, "EARLIEST"),
            ReadFrom::Latest => write!(f, "LATEST"),
            ReadFrom::LastPosition(position) => {
                write!(f, "({}:{})", position.segment_id, position.end_offset)
            }
        }
    }
}

/// Entry 在文件系统上的位置
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EntryPosition {
    /// WAL 文件序号
    segment_id: u64,
    /// 当前 Entry 在文件中结束的偏移量
    end_offset: u64,
}

impl crate::EntryPosition for EntryPosition {
    fn offset(&self) -> u64 {
        self.into()
    }
}

bitfield! {
    struct EntryOffset(u64);
    impl Debug;
    impl new;

    u16, segment_id, set_segment_id: 63, 48;
    u64, end_offset, set_end_offset: 47,0;
}

impl From<&EntryPosition> for u64 {
    fn from(value: &EntryPosition) -> Self {
        EntryOffset::new(value.segment_id as u16, value.end_offset).0
    }
}

impl From<EntryPosition> for u64 {
    fn from(value: EntryPosition) -> Self {
        EntryOffset::new(value.segment_id as u16, value.end_offset).0
    }
}

impl From<u64> for EntryPosition {
    fn from(value: u64) -> Self {
        let offset = EntryOffset(value);
        Self {
            segment_id: offset.segment_id() as u64,
            // start_offset: offset.end_offset(),
            end_offset: offset.end_offset(),
        }
    }
}

impl std::fmt::Display for EntryPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}:{})", self.segment_id, self.end_offset)
    }
}

impl EntryPosition {
    pub fn new(segment_id: u64, end_offset: u64) -> Self {
        Self {
            segment_id,
            // start_offset,
            end_offset,
        }
    }

    pub fn advance(&mut self, offset: u64) {
        // self.start_offset = self.end_offset;
        self.end_offset += offset;
    }
}

pub(crate) fn parse_segment_id(path: impl AsRef<Path>) -> Option<u64> {
    let path = path.as_ref();
    let filename = path.file_name()?.to_str()?;
    let segment_id_str = filename.strip_suffix(SEGMENT_FILE_EXTENSION)?;
    segment_id_str.parse().ok()
}

pub(crate) fn format_segment_id(segment_id: u64) -> String {
    format!("{segment_id:020}{SEGMENT_FILE_EXTENSION}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::tempdir;

    use crate::EntryPosition as _;

    use super::*;

    #[test]
    fn position_test() -> anyhow::Result<()> {
        let mut position = EntryPosition::new(1, 30);
        position.advance(12);
        assert_eq!(position.segment_id, 1);
        // assert_eq!(position.start_offset, 30);
        assert_eq!(position.end_offset, 42);
        Ok(())
    }

    #[test]
    fn parse_segment_id_test() -> anyhow::Result<()> {
        assert!(parse_segment_id("./aa.txt").is_none());
        assert!(parse_segment_id("a.seg").is_none());
        assert!(parse_segment_id("123.txt").is_none());
        assert!(parse_segment_id("123.txt.seg").is_none());
        assert_eq!(parse_segment_id("123.seg"), Some(123));
        assert_eq!(parse_segment_id("00000000000000000001.seg"), Some(1));
        Ok(())
    }

    #[test]
    fn format_segment_id_test() -> anyhow::Result<()> {
        assert_eq!(format_segment_id(0), "00000000000000000000.seg");
        assert_eq!(format_segment_id(123), "00000000000000000123.seg");
        Ok(())
    }

    #[tokio::test]
    async fn find_segment_id_test() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let dir_path = dir.path();
        for path in [
            ".seg",
            "ab00001.seg",
            "00000000000000000000.seg",
            "0123.seg",
            "1.sega",
            "1.seg",
            "2.seg",
            "5.seg",
        ] {
            tokio::fs::File::create(dir_path.join(path)).await?;
        }

        for path in ["00000000000000000001.seg", "00000000000000000002.seg"] {
            tokio::fs::create_dir(dir_path.join(path)).await?;
        }

        assert_eq!(
            find_all_segment_id(dir_path).await?,
            BTreeMap::from_iter([
                (0, dir_path.join("00000000000000000000.seg")),
                (1, dir_path.join("1.seg")),
                (2, dir_path.join("2.seg")),
                (5, dir_path.join("5.seg")),
                (123, dir_path.join("0123.seg"))
            ])
        );

        Ok(())
    }

    #[tokio::test]
    async fn dir_file_notify_test() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let dir_path = dir.path();

        let queue = FsQueue::builder(dir_path).build().await?;

        assert_eq!(queue.segments(), BTreeMap::new());

        // add new files
        for path in [
            ".seg",
            "ab00001.seg",
            "00000000000000000000.seg",
            "0123.seg",
            "1.sega",
            "1.seg",
            "2.seg",
            "5.seg",
        ] {
            tokio::fs::File::create(dir_path.join(path)).await?;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            queue.segments(),
            BTreeMap::from_iter([
                (0, dir_path.join("00000000000000000000.seg")),
                (1, dir_path.join("1.seg")),
                (2, dir_path.join("2.seg")),
                (5, dir_path.join("5.seg")),
                (123, dir_path.join("0123.seg"))
            ])
        );

        // remove files
        for path in ["1.seg", "2.seg", ".seg", "ab00001.seg"] {
            tokio::fs::remove_file(dir_path.join(path)).await?;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            queue.segments(),
            BTreeMap::from_iter([
                (0, dir_path.join("00000000000000000000.seg")),
                (5, dir_path.join("5.seg")),
                (123, dir_path.join("0123.seg"))
            ])
        );

        Ok(())
    }

    #[test]
    fn position_offset_test() -> anyhow::Result<()> {
        assert_eq!(EntryPosition::new(0, 1).offset(), 0x0000000000000001u64);
        assert_eq!(EntryPosition::new(1, 1).offset(), 0x0001000000000001u64);

        assert_eq!(
            EntryPosition::from(0x0000000000000001u64),
            EntryPosition::new(0, 1)
        );
        assert_eq!(
            EntryPosition::from(0x0001000000000001u64),
            EntryPosition::new(1, 1)
        );
        Ok(())
    }

    #[test]
    fn parse_position_test() -> anyhow::Result<()> {
        println!("{:?}", EntryPosition::from(1970325600306716));
        println!("{:016x}", 1970325600306716_u64);
        Ok(())
    }
}
