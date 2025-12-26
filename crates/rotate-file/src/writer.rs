use crate::{
    RequestMsg, RotateFileError, SEP, SNAPSHOT_PREFIX, SinkFn, SinkGenFn,
    utils::{self, scan_files_with},
};
use chrono::{Local, TimeZone};
use faststr::FastStr;
use futures::SinkExt;
use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
    path::PathBuf,
    time::Duration,
};
use tokio::time::timeout;

const DEF_QUEUE_CAP: usize = 32;

#[derive(Debug, Clone)]
pub struct FileName {
    pub prefix: FastStr,
    pub truncated_ts: i64,
    pub dt_fmt: FastStr,
    pub num: usize,
    pub suffix: FastStr,
}

impl FileName {
    pub fn new(
        prefix: FastStr,
        truncated_ts: i64,
        dt_fmt: FastStr,
        num: usize,
        suffix: FastStr,
    ) -> Self {
        Self {
            prefix,
            truncated_ts,
            dt_fmt,
            num,
            suffix,
        }
    }

    pub fn from_str(s: &str, dt_fmt: &str) -> Result<Self, RotateFileError> {
        let parts: Vec<&str> = s.split(SEP).collect();
        if parts.len() < 3 {
            return Err(RotateFileError::InvalidFileName {
                file_name: s.to_string(),
                error: "file name part less than 3".to_string(),
            });
        }
        let prefix = parts[0].to_string().into();
        let dt = parts[1];
        let dt =
            utils::parse_from_str(dt, dt_fmt).map_err(|e| RotateFileError::InvalidFileName {
                file_name: s.to_string(),
                error: e.to_string(),
            })?;
        let truncated_ts = dt.timestamp();
        let num = parts[2]
            .parse::<usize>()
            .map_err(|e| RotateFileError::InvalidFileName {
                file_name: s.to_string(),
                error: e.to_string(),
            })?;
        let suffix = if parts.len() == 3 {
            FastStr::from("")
        } else if parts[3] == SNAPSHOT_PREFIX {
            FastStr::from(parts[4..].join(&SEP.to_string()))
        } else {
            FastStr::from(parts[3..].join(&SEP.to_string()))
        };
        Ok(Self {
            prefix,
            truncated_ts,
            dt_fmt: dt_fmt.to_string().into(),
            num,
            suffix,
        })
    }
}

impl FileName {
    pub fn scan_files(&self, dir: &PathBuf) -> Result<Vec<PathBuf>, RotateFileError> {
        let files = scan_files_with(dir, |path| {
            let name = path.file_name().and_then(|s| s.to_str());
            match name {
                Some(name) => {
                    if name.is_empty() {
                        false
                    } else {
                        name.starts_with(self.prefix.as_str())
                            && name.ends_with(self.suffix.as_str())
                    }
                }
                None => false,
            }
        })
        .map_err(|e| RotateFileError::ReadDirError {
            dir: dir.to_string_lossy().to_string(),
            error: e,
        })?;
        Ok(files)
    }

    pub fn scan_sort_files(&self, dir: &PathBuf) -> Result<Vec<PathBuf>, RotateFileError> {
        let mut files = self.scan_files(dir)?;
        files.sort_by(|path1, path2| {
            let name1 = FileName::from_str(
                path1.file_name().and_then(|s| s.to_str()).unwrap_or(""),
                self.dt_fmt.as_str(),
            )
            .ok();
            let name2 = FileName::from_str(
                path2.file_name().and_then(|s| s.to_str()).unwrap_or(""),
                self.dt_fmt.as_str(),
            )
            .ok();
            name1.cmp(&name2)
        });
        Ok(files)
    }

    fn snapshot(&self) -> String {
        let local_dt = match Local.timestamp_opt(self.truncated_ts, 0) {
            chrono::offset::LocalResult::Single(t) => t,
            _ => Local::now(),
        };
        let fmt = local_dt.format(&self.dt_fmt).to_string();
        if self.suffix.is_empty() {
            return format!(
                "{}{SEP}{}{SEP}{}{SEP}{}",
                self.prefix, fmt, self.num, SNAPSHOT_PREFIX
            );
        }
        format!(
            "{}{SEP}{}{SEP}{}{SEP}{}{SEP}{}",
            self.prefix, fmt, self.num, SNAPSHOT_PREFIX, self.suffix
        )
    }
}

impl Display for FileName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let local_dt = match Local.timestamp_opt(self.truncated_ts, 0) {
            chrono::offset::LocalResult::Single(t) => t,
            _ => Local::now(),
        };
        let fmt = local_dt.format(&self.dt_fmt).to_string();
        if self.suffix.is_empty() {
            write!(f, "{}{SEP}{}{SEP}{}", self.prefix, fmt, self.num)
        } else {
            write!(
                f,
                "{}{SEP}{}{SEP}{}{SEP}{}",
                self.prefix, fmt, self.num, self.suffix
            )
        }
    }
}

impl PartialEq for FileName {
    fn eq(&self, other: &Self) -> bool {
        self.truncated_ts == other.truncated_ts && self.num == other.num
    }
}

impl Eq for FileName {}

impl PartialOrd for FileName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileName {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.truncated_ts.cmp(&other.truncated_ts) {
            Ordering::Equal => self.num.cmp(&other.num),
            ord => ord,
        }
    }
}

pub struct InnerRotateWriter<Item, E1, E2> {
    pub id: i64,
    pub job_id: i64,
    pub max_num_file: usize,
    pub max_size: usize,
    pub num_of_time_unit: usize,
    pub time_unit: usize,
    pub dir: PathBuf,
    pub file_name: FileName,
    pub gen_sink: SinkGenFn<Item, E1, E2>,
    pub sink: SinkFn<Item, E2>,
}

impl<Item, E1, E2> InnerRotateWriter<Item, E1, E2> {
    pub fn time_unit_to_secs(time_unit: &str) -> Result<usize, RotateFileError> {
        match time_unit.to_lowercase().as_str() {
            "m" => Ok(60),
            "h" => Ok(60 * 60),
            "d" => Ok(60 * 60 * 24),
            u => Err(RotateFileError::TimeUnitNotSupport {
                time_unit: u.to_string(),
            }),
        }
    }

    pub fn size_unit_to_bytes(size_unit: &str) -> Result<usize, RotateFileError> {
        match size_unit.to_uppercase().as_str() {
            "KB" => Ok(1024),
            "MB" => Ok(1024 * 1024),
            "GB" => Ok(1024 * 1024 * 1024),
            u => Err(RotateFileError::SizeUnitNotSupport {
                size_unit: u.to_string(),
            }),
        }
    }

    pub fn truncate_ts(time_unit: usize, ts: i64) -> i64 {
        ts - ts % time_unit as i64
    }

    pub fn truncate_ts_now(time_unit: usize) -> i64 {
        let now = chrono::Local::now();
        Self::truncate_ts(time_unit, now.timestamp())
    }
}

impl<Item, E1, E2> InnerRotateWriter<Item, E1, E2>
where
    Item: Send + 'static,
    E1: Into<anyhow::Error> + 'static,
    E2: Into<anyhow::Error> + 'static,
{
    pub fn init(&mut self) -> Result<(), RotateFileError> {
        if self.is_fail_time_limit() {
            self.rotate_by_time()?;
        }
        if self.is_fail_size_limit() {
            self.rotate_by_size()?;
        }
        Ok(())
    }

    pub async fn write(&mut self, data: Item) -> Result<(), RotateFileError> {
        if self.is_fail_time_limit() {
            self.rotate_by_time()?;
        }
        if self.is_fail_size_limit() {
            self.rotate_by_size()?;
        }
        self.sink
            .send(data)
            .await
            .map_err(|e| RotateFileError::ExecSinkFnError {
                id: self.id,
                job_id: self.job_id,
                error: e.into().into(),
            })?;
        Ok(())
    }

    pub fn snapshot(&mut self) -> Result<Vec<PathBuf>, RotateFileError> {
        let files = self
            .file_name
            .scan_files(&self.dir)?
            .into_iter()
            .filter(|f| f.exists() && utils::file_size(f).unwrap_or(0) > 0)
            .collect::<Vec<_>>();
        if files.is_empty() {
            return Ok(vec![]);
        }

        let old_file = self.dir.join(self.file_name.to_string());
        let sp_file = self.dir.join(self.file_name.snapshot());

        self.file_name.num += 1;
        self.new_file_sink()?;
        // safe rename
        if old_file.exists() {
            std::fs::rename(&old_file, &sp_file).map_err(|e| RotateFileError::RenameFileError {
                id: self.id,
                job_id: self.job_id,
                from: old_file.to_string_lossy().to_string(),
                to: sp_file.to_string_lossy().to_string(),
                error: e,
            })?;
        }

        let current_name = self.file_name.to_string();
        let files = self
            .file_name
            .scan_files(&self.dir)?
            .into_iter()
            .filter(|p| {
                p.file_name()
                    .map(|name| name.to_string_lossy() != current_name)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        Ok(files)
    }

    pub async fn close(&mut self) -> Result<(), RotateFileError> {
        self.sink
            .close()
            .await
            .map_err(|e| RotateFileError::CloseSinkError {
                id: self.id,
                job_id: self.job_id,
                error: e.into().into(),
            })?;
        Ok(())
    }

    pub fn new_file_sink(&mut self) -> Result<(), RotateFileError> {
        let file_path = self.dir.join(self.file_name.to_string());
        let sink =
            self.gen_sink.as_ref()(file_path).map_err(|e| RotateFileError::GenSinkError {
                id: self.id,
                job_id: self.job_id,
                error: e.into().into(),
            })?;
        self.sink = sink;
        Ok(())
    }

    pub fn is_fail_time_limit(&self) -> bool {
        let truncated_ts = Self::truncate_ts_now(self.time_unit);
        self.file_name.truncated_ts != truncated_ts
    }

    pub fn set_truncated_ts(&mut self, ts: i64) {
        self.file_name.truncated_ts = ts;
    }

    pub fn is_fail_size_limit(&self) -> bool {
        let file_path = self.dir.join(self.file_name.to_string());
        let size = utils::file_size(&file_path).unwrap_or(0) as usize;
        size > self.max_size
    }

    pub fn rotate_by_time(&mut self) -> Result<(), RotateFileError> {
        self.set_truncated_ts(Self::truncate_ts_now(self.time_unit));
        self.file_name.num = 1;

        self.clean_overdue_files()?;

        self.new_file_sink()?;
        Ok(())
    }

    pub fn rotate_by_size(&mut self) -> Result<(), RotateFileError> {
        self.clean_overdue_files()?;

        self.file_name.num += 1;
        self.new_file_sink()?;
        Ok(())
    }

    pub fn clean_overdue_files(&mut self) -> Result<(), RotateFileError> {
        let files = self.file_name.scan_files(&self.dir)?;

        // delete files matching truncated_ts < start_ts
        let start_ts =
            self.file_name.truncated_ts - self.num_of_time_unit as i64 * self.time_unit as i64;
        let (overtime_files, mut files): (Vec<_>, Vec<_>) = files.into_iter().partition(|path| {
            let is_delete = path
                .file_name()
                .and_then(|name| name.to_str().and_then(|s| s.split(SEP).nth(1)))
                .and_then(|file_dt| {
                    utils::parse_from_str(file_dt, self.file_name.dt_fmt.as_str()).ok()
                })
                .map(|file_dt| {
                    let file_ts = file_dt.timestamp();
                    let file_ts = Self::truncate_ts(self.time_unit, file_ts);
                    file_ts <= start_ts
                });
            is_delete.unwrap_or(false)
        });

        // delete files that exceed max_num_file
        let old_n = files.len() as i32 - self.max_num_file as i32;
        let overcount_files = if old_n > 0 {
            files.sort_by(|path1, path2| {
                let name1 = FileName::from_str(
                    path1.file_name().and_then(|s| s.to_str()).unwrap_or(""),
                    self.file_name.dt_fmt.as_str(),
                )
                .ok();
                let name2 = FileName::from_str(
                    path2.file_name().and_then(|s| s.to_str()).unwrap_or(""),
                    self.file_name.dt_fmt.as_str(),
                )
                .ok();
                name1.cmp(&name2)
            });
            files.into_iter().take(old_n as usize).collect::<Vec<_>>()
        } else {
            vec![]
        };

        overtime_files
            .into_iter()
            .chain(overcount_files)
            .for_each(|path| {
                self.remove_file(&path);
            });
        Ok(())
    }

    pub fn remove_file(&mut self, path: &PathBuf) {
        if !path.exists() {
            return;
        }
        if let Err(e) = std::fs::remove_file(path).map_err(|e| RotateFileError::RemoveFileError {
            id: self.id,
            job_id: self.job_id,
            file: path.to_string_lossy().to_string(),
            error: e,
        }) {
            tracing::warn!("Remove file meet error: {:?}", e);
        }
    }

    pub fn force_rotate(&mut self) -> Result<(), RotateFileError> {
        let mut files = self.file_name.scan_sort_files(&self.dir)?;
        // exclude the current file
        let current_name = self.file_name.to_string();
        files.retain(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy() != current_name)
                .unwrap_or(false)
        });
        // delete empty files and first not empty file
        for file in files {
            if utils::file_size(&file).unwrap_or(0) == 0 {
                self.remove_file(&file);
                continue;
            } else {
                self.remove_file(&file);
                break;
            }
        }
        self.rotate_by_size()
    }

    pub fn spawn(mut self) -> Result<flume::Sender<RequestMsg<Item>>, RotateFileError> {
        self.init()?;

        let id = self.id;
        let job_id = self.job_id;
        let cap = std::thread::available_parallelism()
            .map(|v| v.get() * 2)
            .unwrap_or(DEF_QUEUE_CAP);
        let (tx, rx) = flume::bounded::<RequestMsg<Item>>(cap);
        std::thread::Builder::new()
            .name("rotate-file".to_owned())
            .spawn(move || -> Result<(), RotateFileError> {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!("Rotate {id} failed to create tokio runtime: {:?}", e);
                        return Err(RotateFileError::CreateTokioRuntimeError {
                            id,
                            job_id,
                            error: e,
                        });
                    }
                };
                rt.block_on(async move {
                    loop {
                        let msg = timeout(Duration::from_secs(5), rx.recv_async()).await;
                        let msg = if let Ok(v) = msg {
                            v
                        } else {
                            if self.is_fail_time_limit()
                                && let Err(e) = self.rotate_by_time()
                            {
                                tracing::error!("Rotate {id} failed to rotate by time: {e:?}");
                            }
                            continue;
                        };

                        let msg = match msg {
                            Ok(msg) => msg,
                            Err(e) => {
                                tracing::info!("Disconnect from sender: {:?}", e);
                                break;
                            }
                        };

                        match msg {
                            RequestMsg::Write(data) => {
                                let rs = self.write(data.data).await;
                                if let Err(e) = data.resp_tx.send(rs) {
                                    tracing::warn!("Rotate {id} receiver dropped, response: {e:?}");
                                }
                            }
                            RequestMsg::Snapshot(data) => {
                                let rs = self.snapshot();
                                if let Err(e) = data.resp_tx.send(rs) {
                                    tracing::warn!("Rotate {id} receiver dropped, response: {e:?}");
                                }
                            }
                            RequestMsg::Close => {
                                tracing::info!("Receive close msg from sender");
                                match self.close().await {
                                    Ok(_) => tracing::info!("Rotate {id} close writer success"),
                                    Err(e) => tracing::error!("Rotate {id} close error: {e:?}"),
                                }
                                break;
                            }
                            RequestMsg::ForceRotate(data) => {
                                let rs = self.force_rotate();
                                if let Err(e) = data.resp_tx.send(rs) {
                                    tracing::warn!("Rotate {id} receiver dropped, response: {e:?}");
                                }
                            }
                        }
                    }
                });
                Ok(())
            })
            .map_err(|e| RotateFileError::CreateThreadError {
                id,
                job_id,
                error: e,
            })?;
        Ok(tx)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::utils::YYMMDD;

    #[test]
    pub fn test_file_name_from_str() {
        let file_name = FileName::from_str("cache.20250626.1", YYMMDD).unwrap();
        assert_eq!(file_name.prefix, "cache");
        assert_eq!(file_name.dt_fmt, YYMMDD);
        assert_eq!(file_name.truncated_ts, 1750867200);
        assert_eq!(file_name.suffix, "");
        assert_eq!(file_name.num, 1);
        let file_name = FileName::from_str("cache.20250626.1.snapshot", YYMMDD).unwrap();
        assert_eq!(file_name.prefix, "cache");
        assert_eq!(file_name.dt_fmt, YYMMDD);
        assert_eq!(file_name.truncated_ts, 1750867200);
        assert_eq!(file_name.suffix, "");
        assert_eq!(file_name.num, 1);
    }
}
