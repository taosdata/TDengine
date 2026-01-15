pub mod utils;
pub mod writer;

use crate::writer::{FileName, InnerRotateWriter};
use faststr::FastStr;
use futures::Sink;
use std::{fmt::Debug, fs::create_dir_all, path::PathBuf, pin::Pin};
use thiserror::Error;
use tokio::sync::oneshot;

const SEP: char = '.';
const SNAPSHOT_PREFIX: &str = "snapshot";
pub type SinkFn<Item, E> = Pin<Box<dyn Sink<Item, Error = E> + Sync + Send>>;
type SinkGenFn<Item, E1, E2> = Box<dyn Fn(PathBuf) -> Result<SinkFn<Item, E2>, E1> + Sync + Send>;

pub enum RequestMsg<Item> {
    Write(WriteData<Item>),
    Snapshot(SnapshotData),
    ForceRotate(ForceRotateData),
    Close,
}

pub struct WriteData<Item> {
    data: Item,
    resp_tx: oneshot::Sender<Result<(), RotateFileError>>,
}

pub struct SnapshotData {
    resp_tx: oneshot::Sender<Result<Vec<PathBuf>, RotateFileError>>,
}

pub struct ForceRotateData {
    resp_tx: oneshot::Sender<Result<(), RotateFileError>>,
}

#[derive(Clone)]
pub struct RotateWriter<Item> {
    id: i64,
    sender: flume::Sender<RequestMsg<Item>>,
}

impl<Item> RotateWriter<Item> {
    pub async fn write(&self, data: Item) -> Result<(), RotateFileError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send_async(RequestMsg::Write(WriteData { data, resp_tx }))
            .await
            .map_err(|e| RotateFileError::InnerSendError(self.id, e.to_string()))?;
        resp_rx
            .await
            .map_err(|e| RotateFileError::InnerReceiveError(self.id, e.to_string()))?
    }

    pub async fn snapshot(&self) -> Result<Vec<PathBuf>, RotateFileError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send_async(RequestMsg::Snapshot(SnapshotData { resp_tx }))
            .await
            .map_err(|e| RotateFileError::InnerSendError(self.id, e.to_string()))?;
        resp_rx
            .await
            .map_err(|e| RotateFileError::InnerReceiveError(self.id, e.to_string()))?
    }

    pub async fn close(&self) -> Result<(), RotateFileError> {
        if !self.sender.is_disconnected() {
            self.sender
                .send_async(RequestMsg::Close)
                .await
                .map_err(|e| RotateFileError::InnerSendError(self.id, e.to_string()))?;
        }
        Ok(())
    }

    pub async fn force_rotate(&self) -> Result<(), RotateFileError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send_async(RequestMsg::ForceRotate(ForceRotateData { resp_tx }))
            .await
            .map_err(|e| RotateFileError::InnerSendError(self.id, e.to_string()))?;
        resp_rx
            .await
            .map_err(|e| RotateFileError::InnerReceiveError(self.id, e.to_string()))?
    }
}

pub struct RotateWriterBuilder<Item, E1, E2> {
    pub id: Option<i64>,
    pub dir: Option<FastStr>,
    pub rotate_count: Option<usize>,
    pub max_size_value: Option<usize>,
    pub max_size_unit: Option<FastStr>,
    pub keep_time_value: Option<usize>,
    pub keep_time_unit: Option<FastStr>,
    pub prefix: Option<FastStr>,
    pub suffix: Option<FastStr>,
    pub file_dt_fmt: Option<FastStr>,
    pub gen_sink: Option<SinkGenFn<Item, E1, E2>>,
}

impl<Item, E1, E2> Default for RotateWriterBuilder<Item, E1, E2>
where
    E1: Into<anyhow::Error>,
    E2: Into<anyhow::Error>,
    Item: Send + 'static,
{
    fn default() -> Self {
        Self {
            id: None,
            dir: None,
            rotate_count: None,
            max_size_value: None,
            max_size_unit: None,
            keep_time_value: None,
            keep_time_unit: None,
            prefix: None,
            suffix: None,
            file_dt_fmt: None,
            gen_sink: None,
        }
    }
}

impl<Item, E1, E2> RotateWriterBuilder<Item, E1, E2>
where
    E1: Into<anyhow::Error> + 'static,
    E2: Into<anyhow::Error> + 'static,
    Item: Send + 'static,
{
    pub fn new() -> Self {
        Default::default()
    }

    pub fn build(self) -> Result<RotateWriter<Item>, RotateFileError> {
        // check params
        macro_rules! not_allow_none {
            () => {};
            ($($param: ident),+ $(,)?) => {
                $(
                    if self.$param.is_none() {
                        return Err(RotateFileError::MissingRequiredField {
                            field: stringify!($param).to_string(),
                        });
                    }
                )+
            };
        }
        macro_rules! not_allow_empty {
            () => {};
            ($($param: ident),+ $(,)?) => {
                $(
                    if $param.is_empty() {
                        return Err(RotateFileError::ParamIsEmpty {
                            param: stringify!($param).to_string(),
                        });
                    }
                )+
            };
        }
        not_allow_none!(
            id,
            dir,
            prefix,
            file_dt_fmt,
            gen_sink,
            rotate_count,
            max_size_value,
            max_size_unit,
            keep_time_value,
            keep_time_unit
        );
        let id = self.id.unwrap();
        let dir = self.dir.unwrap();
        let prefix = self.prefix.unwrap();
        let file_dt_fmt = self.file_dt_fmt.unwrap();
        let gen_sink = self.gen_sink.unwrap();
        let rotate_count = self.rotate_count.unwrap();
        let max_size_value = self.max_size_value.unwrap();
        let max_size_unit = self.max_size_unit.unwrap();
        let keep_time_value = self.keep_time_value.unwrap();
        let keep_time_unit = self.keep_time_unit.unwrap();
        let suffix = self.suffix.unwrap_or(FastStr::from(""));

        not_allow_empty!(dir, prefix, file_dt_fmt);

        let rs = prefix
            .contains(SEP)
            .then(|| RotateFileError::PrefixContainsSeparator {
                prefix: prefix.as_str().to_string(),
                sep: SEP,
            });
        if let Some(e) = rs {
            return Err(e);
        }
        if keep_time_value < 1 {
            return Err(RotateFileError::InvalidKeepTimeValue {
                id,
                keep_time_value,
            });
        }
        if max_size_value < 1 {
            return Err(RotateFileError::InvalidMaxSizeValue { id, max_size_value });
        }
        if rotate_count < 1 {
            return Err(RotateFileError::InvalidRotateCount { id, rotate_count });
        }

        // create dir if not exists
        let dir = std::path::PathBuf::from(dir.as_str());
        if !dir.exists() {
            create_dir_all(dir.clone()).map_err(|error| RotateFileError::CreateDirError {
                id,
                dir: dir.to_string_lossy().to_string(),
                error,
            })?;
        }
        // file name point to the last one
        let truncated_ts = InnerRotateWriter::<Item, E1, E2>::truncate_ts_now(keep_time_value);
        let file_name = FileName::new(prefix, truncated_ts, file_dt_fmt.clone(), 1, suffix.clone());
        let files = file_name.scan_sort_files(&dir)?;
        let file_path = match files.last() {
            Some(v) => v.to_owned(),
            None => dir.join(file_name.to_string()),
        };
        let mut file_name = FileName::from_str(
            file_path.file_name().and_then(|s| s.to_str()).unwrap_or(""),
            file_dt_fmt.as_str(),
        )?;
        // it may be a snapshot file, so we need to keep the suffix
        file_name.suffix = suffix;
        // gen sink
        let sink = Box::pin(
            gen_sink(file_path).map_err(|e| RotateFileError::GenSinkError {
                id,
                error: e.into().into(),
            })?,
        );

        let max_size =
            max_size_value * InnerRotateWriter::<Item, E1, E2>::size_unit_to_bytes(&max_size_unit)?;
        let time_unit = InnerRotateWriter::<Item, E1, E2>::time_unit_to_secs(&keep_time_unit)?;

        let writer = InnerRotateWriter {
            id,
            max_num_file: rotate_count,
            max_size,
            num_of_time_unit: keep_time_value,
            time_unit,
            dir,
            file_name,
            gen_sink,
            sink,
        };
        let tx = writer.spawn()?;

        Ok(RotateWriter { id, sender: tx })
    }

    pub fn id(self, id: i64) -> Self {
        Self {
            id: Some(id),
            ..self
        }
    }

    pub fn rotate_count(self, rotate_count: usize) -> Self {
        Self {
            rotate_count: Some(rotate_count),
            ..self
        }
    }

    pub fn max_size_value(self, max_size_value: usize) -> Self {
        Self {
            max_size_value: Some(max_size_value),
            ..self
        }
    }

    pub fn max_size_unit<S: Into<FastStr>>(self, max_size_unit: S) -> Self {
        Self {
            max_size_unit: Some(max_size_unit.into()),
            ..self
        }
    }

    pub fn keep_time_value(self, keep_time_value: usize) -> Self {
        Self {
            keep_time_value: Some(keep_time_value),
            ..self
        }
    }

    pub fn keep_time_unit<S: Into<FastStr>>(self, keep_time_unit: S) -> Self {
        Self {
            keep_time_unit: Some(keep_time_unit.into()),
            ..self
        }
    }

    pub fn dir<S: Into<FastStr>>(self, dir: S) -> Self {
        Self {
            dir: Some(dir.into()),
            ..self
        }
    }

    pub fn prefix<S: Into<FastStr>>(self, prefix: S) -> Self {
        Self {
            prefix: Some(prefix.into()),
            ..self
        }
    }

    pub fn file_dt_fmt<S: Into<FastStr>>(self, file_dt_fmt: S) -> Self {
        Self {
            file_dt_fmt: Some(file_dt_fmt.into()),
            ..self
        }
    }

    pub fn suffix<S: Into<FastStr>>(self, suffix: S) -> Self {
        Self {
            suffix: Some(suffix.into()),
            ..self
        }
    }

    pub fn gen_sink(self, gen_sink: SinkGenFn<Item, E1, E2>) -> Self {
        Self {
            gen_sink: Some(gen_sink),
            ..self
        }
    }
}

#[derive(Debug, Error)]
pub enum RotateFileError {
    #[error("RotateWriter field check error: {field} is missing")]
    MissingRequiredField { field: String },
    #[error("RotateWriter prefix {prefix} can't contain {sep}")]
    PrefixContainsSeparator { prefix: String, sep: char },
    #[error("RotateWriter {param} is empty")]
    ParamIsEmpty { param: String },
    #[error("RotateWriter Close error: {0}")]
    CloseError(String),
    #[error("RotateWriter {id} create {dir} meet error: {error}")]
    CreateDirError {
        id: i64,
        dir: String,
        error: std::io::Error,
    },
    #[error("RotateWriter time unit {time_unit} not support, only support m, h, d")]
    TimeUnitNotSupport { time_unit: String },
    #[error("RotateWriter size unit {size_unit} not support, only support KB, MB, GB")]
    SizeUnitNotSupport { size_unit: String },
    #[error("RotateWriter {0} inner send error: {1}")]
    InnerSendError(i64, String),
    #[error("RotateWriter {0} inner receive error: {1}")]
    InnerReceiveError(i64, String),
    #[error("RotateWriter {id} gen sink error: {error}")]
    GenSinkError {
        id: i64,
        error: Box<dyn std::error::Error + Sync + Send + 'static>,
    },
    #[error("RotateWriter {id} exec sink fn error: {error}")]
    ExecSinkFnError {
        id: i64,
        error: Box<dyn std::error::Error + Sync + Send + 'static>,
    },
    #[error("RotateWriter {id} close sink error: {error}")]
    CloseSinkError {
        id: i64,
        error: Box<dyn std::error::Error + Sync + Send + 'static>,
    },
    #[error("RotateWriter {id} create file {file} meet error: {error}")]
    CreateFileError {
        id: i64,
        file: String,
        error: std::io::Error,
    },
    #[error("RotateWriter {id} create thread meet error: {error}")]
    CreateThreadError { id: i64, error: std::io::Error },
    #[error("RotateWriter {id} create tokio runtime error: {error}")]
    CreateTokioRuntimeError { id: i64, error: std::io::Error },
    #[error("RotateWriter read dir: {dir}, meet error: {error}")]
    ReadDirError { dir: String, error: std::io::Error },
    #[error("RotateWriter {id} remove file {file} meet error: {error}")]
    RemoveFileError {
        id: i64,
        file: String,
        error: std::io::Error,
    },
    #[error("RotateWriter file name {file_name} is invalid, error: {error}")]
    InvalidFileName { file_name: String, error: String },
    #[error("RotateWriter {id} keep time value {keep_time_value} must be greater than 0")]
    InvalidKeepTimeValue { id: i64, keep_time_value: usize },
    #[error("RotateWriter {id} max size value {max_size_value} must be greater than 0")]
    InvalidMaxSizeValue { id: i64, max_size_value: usize },
    #[error("RotateWriter {id} rotate count {rotate_count} must be greater than 0")]
    InvalidRotateCount { id: i64, rotate_count: usize },
    #[error("RotateWriter {id} rename file {from} to {to} meet error: {error}")]
    RenameFileError {
        id: i64,
        from: String,
        to: String,
        error: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use crate::{RotateWriterBuilder, SinkFn, utils::time_unit_dt_fmt, writer::FileName};
    use faststr::FastStr;
    use futures::sink;
    use std::{
        fs::{File, OpenOptions},
        io::Write,
        path::PathBuf,
    };

    // #[ignore]
    #[tokio::test]
    pub async fn test_snapshot() -> anyhow::Result<()> {
        let dt_fmt = time_unit_dt_fmt("m")?;
        let cache_writer = RotateWriterBuilder::new()
            .id(999)
            .dir("/tmp/taosx/cache")
            .prefix("cache")
            .file_dt_fmt(dt_fmt)
            .rotate_count(5)
            .max_size_value(2)
            .max_size_unit("KB")
            .keep_time_value(10)
            .keep_time_unit("m")
            .gen_sink(Box::new(
                |file_path: PathBuf| -> Result<SinkFn<FastStr, std::io::Error>, anyhow::Error> {
                    let file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&file_path)
                        .map_err(|e| anyhow::anyhow!("open file error: {:?}", e))?;

                    let sink = sink::unfold(file, |mut file: File, line: FastStr| async move {
                        file.write_all(line.as_bytes())?;
                        file.flush()?;
                        Ok(file)
                    });
                    Ok(Box::pin(sink))
                },
            ))
            .build()?;

        let writer2 = cache_writer.clone();
        tokio::spawn(async move {
            let mut cnt = 1;
            loop {
                cnt += 1;
                if cnt > 120 {
                    break;
                }

                for i in 0..1000 {
                    let _ = writer2.write(format!("test {}\n", i).into()).await;
                }

                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let mut files = cache_writer.snapshot().await?;
        assert!(!files.is_empty());
        files.sort_by(|path1, path2| {
            let name1 = FileName::from_str(
                path1.file_name().and_then(|s| s.to_str()).unwrap_or(""),
                dt_fmt,
            )
            .ok();
            let name2 = FileName::from_str(
                path2.file_name().and_then(|s| s.to_str()).unwrap_or(""),
                dt_fmt,
            )
            .ok();
            name1.cmp(&name2)
        });
        println!("snapshot files: {:?}", files);

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    pub async fn test_force_rotate() -> anyhow::Result<()> {
        let dt_fmt = time_unit_dt_fmt("m")?;
        let dir = "/tmp/taosx/cache";
        let prefix = "cache";
        let suffix = "log";
        let cache_writer = RotateWriterBuilder::new()
            .id(999)
            .dir(dir)
            .prefix(prefix)
            .suffix(suffix)
            .file_dt_fmt(dt_fmt)
            .rotate_count(5)
            .max_size_value(2)
            .max_size_unit("KB")
            .keep_time_value(10)
            .keep_time_unit("m")
            .gen_sink(Box::new(
                |file_path: PathBuf| -> Result<SinkFn<FastStr, anyhow::Error>, anyhow::Error> {
                    let sink =
                        sink::unfold(file_path, |file_path: PathBuf, line: FastStr| async move {
                            let mut file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(&file_path)
                                .map_err(|e| anyhow::anyhow!("open file error: {:?}", e))?;
                            file.write_all(line.as_bytes())?;
                            file.flush()?;
                            Ok(file_path)
                        });
                    Ok(Box::pin(sink))
                },
            ))
            .build()?;

        let writer2 = cache_writer.clone();
        tokio::spawn(async move {
            let mut cnt = 1;
            loop {
                cnt += 1;
                if cnt > 120 {
                    break;
                }

                for i in 0..20 {
                    let _ = writer2.write(format!("test {}\n", i).into()).await;
                }

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let file_name = FileName {
            prefix: prefix.into(),
            dt_fmt: dt_fmt.into(),
            num: 1,
            suffix: suffix.into(),
            truncated_ts: 1750907340,
        };
        let before_files = file_name.scan_sort_files(&PathBuf::from(dir))?;
        cache_writer.force_rotate().await?;
        let files = file_name.scan_sort_files(&PathBuf::from(dir))?;

        println!("force rotate files before: {:?}", before_files);
        println!("force rotate files after: {:?}", files);
        assert!(before_files.len() >= files.len());
        Ok(())
    }
}
