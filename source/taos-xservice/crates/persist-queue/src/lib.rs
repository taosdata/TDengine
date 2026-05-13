pub mod fs;
pub mod reader;
pub mod writer;

use std::{future::Future, path::PathBuf, time::Duration};

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Frame codec error"))]
    Codec { source: std::io::Error },
    #[snafu(display("Dir has been locked"))]
    DirLocked,
    #[snafu(display("File {} has been locked", path.display()))]
    FileLocked { path: PathBuf },
    #[snafu(display("Must have only one writer"))]
    MultipleWriter,
    #[snafu(display("Frame invalid crc checksum, file damaged"))]
    BadChecksum,
    #[snafu(display("Lock file {} error", path.display()))]
    SharedLockFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Lock file {} error", path.display()))]
    ExclusiveLockFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Open file {} error", path.display()))]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Seek file {} error", path.display()))]
    SeekFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Remove file {} error", path.display()))]
    RemoveFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Fetch file {} metadata error", path.display()))]
    FileMetadata {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Check dir exist status error"))]
    DirExists { source: std::io::Error },
    #[snafu(display("Fsync file {} data error", path.display()))]
    SyncFileData {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Build notify watcher error"))]
    BuildWatcher { source: notify::Error },
    #[snafu(display("Add watch {} to notify error", path.display()))]
    AddWatch {
        path: PathBuf,
        source: notify::Error,
    },
    #[snafu(display("Del watch {} in notify error", path.display()))]
    DelWatch {
        path: PathBuf,
        source: notify::Error,
    },
    #[snafu(display("Read dir error"))]
    ReadDir { source: std::io::Error },
    #[snafu(display("Scan dir entries error"))]
    WalkDir { source: std::io::Error },
    #[snafu(display("Invalid payload length bytes, file damaged"))]
    InvalidPayloadLengthBytes,
    #[snafu(display("Create dir error"))]
    CreateDir { source: std::io::Error },
    #[snafu(display("Payload must not be empty"))]
    EmptyPayload,
    #[snafu(display("Payload is too long"))]
    PayloadTooLong { len: usize },
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Error::Codec { source }
    }
}

type Result<T> = std::result::Result<T, Error>;

pub trait EntryPosition: serde::Serialize + for<'a> serde::Deserialize<'a> + From<u64> {
    fn offset(&self) -> u64;
}

/// 用户读取到的的 WAL 日志
#[derive(Debug, PartialEq, Eq)]
pub struct Entry<P> {
    /// 当前日志的 id
    pub position: P,
    /// 用户存储到 WAL 的日志
    pub payload: Bytes,
}

pub trait RawReader {
    type EntryPosition;

    /// read at most `max_batch_size` entries and return immediately
    fn read(
        &mut self,
        max_batch_size: usize,
    ) -> impl Future<Output = Result<Vec<Entry<Self::EntryPosition>>>> + Send;

    /// read until batch_size entries or reach the timeout
    fn read_util(
        &mut self,
        min_batch_size: usize,
        max_batch_size: usize,
        timeout: Option<Duration>,
        cancel: &CancellationToken,
    ) -> impl Future<Output = Result<Vec<Entry<Self::EntryPosition>>>> + Send;

    fn vacuum(&mut self) -> impl Future<Output = Result<()>> + Send;
}

pub trait RawWriter<B>
where
    B: AsRef<[u8]> + Send,
{
    type EntryPosition;

    fn position(&self) -> Self::EntryPosition;

    fn write(&mut self, payloads: Vec<B>) -> impl Future<Output = Result<()>> + Send;

    fn sync_data(&mut self) -> impl Future<Output = Result<()>> + Send;
}
