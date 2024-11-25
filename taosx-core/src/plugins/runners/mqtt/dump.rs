use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use anyhow::Context;
use chrono::{
    format::{DelayedFormat, StrftimeItems},
    DateTime, Local, NaiveDateTime, TimeDelta, TimeZone, Timelike,
};
use parking_lot::{RwLock, RwLockReadGuard};
use regex::Regex;

const DATE_TIME_FORMAT: &str = "%Y%m%d%H%M";

pub struct RollingWriter<'a>(RwLockReadGuard<'a, File>);

impl std::io::Write for RollingWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        (&*self.0).write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (&*self.0).flush()
    }
}

#[derive(Clone)]
pub struct Config {
    dir: PathBuf,
    keep_days: TimeDelta,
}

impl Config {
    fn handle_old_files(&self) -> anyhow::Result<()> {
        // 删除多余的旧文件
        let delete_files = std::fs::read_dir(&self.dir)
            .with_context(|| format!("read dir {} error", self.dir.display()))?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let metadata = entry.metadata().ok()?;

                if !metadata.is_file() {
                    return None;
                }

                let filename = entry.file_name().to_str()?.to_string();
                let date = parse_filename(&filename)?;

                let need_delete = date < Local::now() - self.keep_days;

                need_delete.then_some(filename)
            })
            .map(|f| self.dir.join(f));

        delete_files.for_each(|path| {
            std::fs::remove_file(path).ok();
        });

        Ok(())
    }
}

struct State {
    file_path: PathBuf,
}

pub struct RollingFileAppender {
    config: Config,

    event_tx: flume::Sender<()>,

    state: RwLock<State>,
    writer: RwLock<File>,
}

impl std::io::Write for RollingFileAppender {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.make_writer().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.make_writer().flush()
    }
}

impl RollingFileAppender {
    pub fn new(mut dir: PathBuf, keep_days: i64) -> anyhow::Result<Self> {
        if !dir.is_absolute() {
            dir = dir
                .canonicalize()
                .context("Get dump dir absolute path error")?;
        }
        // init log dir
        if !dir.is_dir() {
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("create dump dir {dir:?} error"))?;
        }

        // open dump file
        let (writer, state) = {
            // open the max file in dir or open new file
            let file_path = match latest_filename_date(&dir)? {
                Some((date, filename)) if !is_date_expired(date) => dir.join(filename),
                _ => {
                    let this_hour = time_format(Local::now());
                    let filename = format!("mqtt.dump.{this_hour}.csv");
                    dir.join(filename)
                }
            };
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .with_context(|| format!("Open file {file_path:?} error"))?;
            (RwLock::new(file), RwLock::new(State { file_path }))
        };

        let config = Config {
            dir,
            keep_days: TimeDelta::days(keep_days),
        };

        let (event_tx, event_rx) = flume::bounded(1);
        std::thread::spawn({
            let config = config.clone();
            move || {
                while event_rx.recv().is_ok() {
                    config.handle_old_files().ok();
                }
            }
        });

        Ok(Self {
            config,
            event_tx,
            state,
            writer,
        })
    }

    pub fn make_writer(&self) -> RollingWriter<'_> {
        if let Ok(Some(file)) = self.rotate() {
            let mut writer = self.writer.write();
            *writer = file;
        }
        RollingWriter(self.writer.read())
    }

    fn rotate(&self) -> anyhow::Result<Option<File>> {
        let mut state = self.state.write();

        if let Some(file) = self.rotate_by_time(&mut state)? {
            return Ok(Some(file));
        }

        if !state.file_path.is_file() {
            return self.new_csv_file(&mut state);
        }

        Ok(None)
    }

    fn rotate_by_time(&self, state: &mut State) -> anyhow::Result<Option<File>> {
        let filename = state
            .file_path
            .file_name()
            .and_then(|f| f.to_str())
            .context("file name not found")?;
        let date = parse_filename(filename).context("valid filename not found")?;
        if !is_date_expired(date) {
            return Ok(None);
        }

        self.new_csv_file(state)
    }

    fn new_csv_file(&self, state: &mut State) -> anyhow::Result<Option<File>> {
        let this_hour = time_format(Local::now());
        let filename = format!("mqtt.dump.{this_hour}.csv");
        let file_path = self.config.dir.join(filename);
        match create_file(&file_path)? {
            Some(file) => {
                self.event_tx.try_send(()).ok();
                state.file_path = file_path;
                Ok(Some(file))
            }
            None => Ok(None),
        }
    }
}

fn parse_filename(name: &str) -> Option<DateTime<Local>> {
    static LOG_FILE_NAME_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"mqtt.dump.(?<date>\d{12}).csv$").unwrap());
    let caps = LOG_FILE_NAME_RE.captures(name)?;
    let date = caps.name("date").and_then(|m| parse_date_str(m.as_str()))?;

    Some(date)
}

fn is_date_expired(date: DateTime<Local>) -> bool {
    let now = Local::now();
    date + TimeDelta::hours(1) < now.with_second(0).unwrap()
}

fn latest_filename_date(
    dir: impl AsRef<Path>,
) -> anyhow::Result<Option<(DateTime<Local>, String)>> {
    Ok(std::fs::read_dir(dir)
        .context("read dump dir error")?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let metadata = entry.metadata().ok()?;

            if !metadata.is_file() {
                return None;
            }

            let filename = entry.file_name();
            let date = parse_filename(filename.to_str()?)?;

            Some((date, filename.into_string().ok()?))
        })
        .max_by_key(|s| s.0))
}

fn parse_date_str(date: &str) -> Option<DateTime<Local>> {
    let dt = NaiveDateTime::parse_from_str(date, DATE_TIME_FORMAT).ok()?;
    Local.from_local_datetime(&dt).single()
}

fn time_format<'a>(datetime: DateTime<Local>) -> DelayedFormat<StrftimeItems<'a>> {
    datetime.format(DATE_TIME_FORMAT)
}

fn create_file(name: impl AsRef<Path>) -> anyhow::Result<Option<File>> {
    let path = name.as_ref();
    match std::fs::OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => Ok(Some(file)),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
        e @ Err(_) => Ok(Some(
            e.with_context(|| format!("open file {path:?} error"))?,
        )),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parse_filename_test() {
        assert_eq!(
            parse_date_str("202410221338").map(|s| s.timestamp()),
            Some(1729575480)
        );
        assert_eq!(
            parse_filename("mqtt.dump.202410221338.csv").map(|s| s.timestamp()),
            Some(1729575480)
        );
    }

    #[test]
    fn parse_time_test() {
        assert_eq!(
            time_format(parse_date_str("202410221338").unwrap()).to_string(),
            "202410221338"
        );
    }
}
