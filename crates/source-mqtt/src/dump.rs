use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use anyhow::Context;
use chrono::{
    DateTime, Local, NaiveDateTime, TimeDelta, TimeZone, Timelike,
    format::{DelayedFormat, StrftimeItems},
};
use parking_lot::{RwLock, RwLockReadGuard};
use regex::Regex;

const DATE_TIME_FORMAT: &str = "%Y%m%d%H%M";

pub trait SystemClock: Clone + Send + 'static {
    fn now(&self) -> DateTime<Local>;
}

impl<F> SystemClock for F
where
    F: Fn() -> DateTime<Local> + Clone + Send + 'static,
{
    fn now(&self) -> DateTime<Local> {
        self()
    }
}

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
    fn handle_old_files<C>(&self, clock: &C) -> anyhow::Result<()>
    where
        C: SystemClock,
    {
        // 删除多余的旧文件
        let now = clock.now();
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

                let need_delete = date <= now - self.keep_days;

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

pub struct RollingFileAppender<C> {
    config: Config,

    clock: C,

    event_tx: flume::Sender<()>,

    state: RwLock<State>,
    writer: RwLock<File>,
}

impl<C> std::io::Write for RollingFileAppender<C>
where
    C: SystemClock,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.make_writer().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.make_writer().flush()
    }
}

impl<C> RollingFileAppender<C>
where
    C: SystemClock,
{
    pub fn new(mut dir: PathBuf, keep_days: i64, clock: C) -> anyhow::Result<Self> {
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
                Some((date, filename)) if !is_date_expired(date, clock.clone()) => {
                    dir.join(filename)
                }
                _ => {
                    let this_hour = time_format(clock.now());
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

        config
            .handle_old_files(&clock)
            .context("remove old files error")?;

        let (event_tx, event_rx) = flume::bounded(1);
        std::thread::spawn({
            let config = config.clone();
            let clock = clock.clone();
            move || {
                while event_rx.recv().is_ok() {
                    config.handle_old_files(&clock).ok();
                }
            }
        });

        Ok(Self {
            config,
            event_tx,
            state,
            writer,
            clock,
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
        if !is_date_expired(date, self.clock.clone()) {
            return Ok(None);
        }

        self.new_csv_file(state)
    }

    fn new_csv_file(&self, state: &mut State) -> anyhow::Result<Option<File>> {
        let this_hour = time_format(self.clock.now());
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

fn is_date_expired<C>(date: DateTime<Local>, clock: C) -> bool
where
    C: SystemClock,
{
    let now = clock.now();
    date + TimeDelta::hours(1) <= now.with_second(0).unwrap()
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

    use std::{io::Write, sync::Arc, time::Duration};

    use parking_lot::Mutex;
    use tempfile::TempDir;

    use super::*;

    impl SystemClock for chrono::DateTime<Local> {
        fn now(&self) -> DateTime<Local> {
            *self
        }
    }

    #[derive(Clone)]
    struct TestClock(Arc<Mutex<chrono::DateTime<Local>>>);

    impl TestClock {
        fn set(&self, s: &str) {
            *self.0.lock() = dt(s)
        }
    }

    impl SystemClock for TestClock {
        fn now(&self) -> DateTime<Local> {
            *self.0.lock()
        }
    }

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

    fn dt(s: &str) -> DateTime<Local> {
        DateTime::parse_from_rfc3339(s).unwrap().into()
    }

    #[test]
    fn is_date_expired_test() -> anyhow::Result<()> {
        assert!(is_date_expired(
            dt("2024-11-18T08:21:33+08:00"),
            dt("2024-11-18T10:21:33+08:00")
        ));
        Ok(())
    }

    #[test]
    fn latest_filename_date_test() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let dir_path = dir.path();

        assert!(latest_filename_date(dir_path)?.is_none());

        std::fs::create_dir(dir_path.join("test_dir"))?;

        assert!(latest_filename_date(dir_path)?.is_none());

        for path in [
            "mqtt.dump.202410221335.csv",
            "mqtt.dump.202410221337.csv",
            "mqtt.dump.202410221339.csv",
            "mqtt.202410221340.csv",
            "mqtt.dump.csv",
            "mqtt.dump.202410221340",
            "dump.202410221340.csv",
        ] {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir_path.join(path))?;
        }

        let (date, filename) = latest_filename_date(dir_path)?.context("filename not found")?;
        assert_eq!(date, dt("2024-10-22T13:39:00+08:00"));
        assert_eq!(&filename, "mqtt.dump.202410221339.csv");
        Ok(())
    }

    #[test]
    fn create_file_test() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let dir_path = dir.path();

        let path = dir_path.join("mqtt.dump.202410221335.csv");
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        assert!(create_file(&path)?.is_none());

        assert!(create_file(dir_path.join("mqtt.dump.202410221339.csv"))?.is_some());
        Ok(())
    }

    #[test]
    fn handle_old_files_test() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let dir_path = dir.path();
        let config = Config {
            dir: dir_path.to_path_buf(),
            keep_days: TimeDelta::days(1),
        };

        std::fs::create_dir(dir_path.join("test_dir"))?;

        for path in [
            "mqtt.dump.202410221335.csv",
            "mqtt.dump.202410231337.csv",
            "mqtt.dump.202410241339.csv",
        ] {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir_path.join(path))?;
        }

        config.handle_old_files(&dt("2024-10-23T14:39:00+08:00"))?;

        assert!(std::fs::exists(
            dir_path.join("mqtt.dump.202410241339.csv")
        )?);
        assert!(std::fs::exists(
            dir_path.join("mqtt.dump.202410231337.csv")
        )?);
        assert!(!std::fs::exists(
            dir_path.join("mqtt.dump.202410221335.csv")
        )?);
        Ok(())
    }

    #[test]
    fn write_csv_test() -> anyhow::Result<()> {
        let clock = TestClock(Arc::new(Mutex::new(dt("2024-10-22T13:39:00+08:00"))));

        let dir = TempDir::new()?;
        let dir_path = dir.path().join("csv");

        // 初始化
        let mut appender = RollingFileAppender::new(dir_path.to_path_buf(), 1, clock.clone())?;

        let file_path1 = dir_path.join("mqtt.dump.202410221339.csv");

        // 初始化时会创建文件1和目录
        assert!(dir_path.is_dir());
        assert!(file_path1.is_file());

        // 写入一条数据
        appender.write_all(b"hello,world")?;
        appender.flush()?;

        // 读取数据
        assert_eq!(&std::fs::read_to_string(&file_path1)?, "hello,world");

        // 修改系统时间，向前推进 1 小时
        clock.set("2024-10-22T14:39:00+08:00");

        // 写入一条数据
        appender.write_all(b"hello,world")?;
        appender.flush()?;

        // 文件1和目录存在
        assert!(dir_path.is_dir());
        assert!(file_path1.is_file());

        // 新文件2被创建
        let file_path2 = dir_path.join("mqtt.dump.202410221439.csv");
        assert!(file_path2.is_file());

        // 读取文件2数据
        assert_eq!(&std::fs::read_to_string(&file_path2)?, "hello,world");

        // 修改系统时间，向前推进 1 天
        clock.set("2024-10-23T13:39:00+08:00");

        // 文件 1，文件 2 和目录存在
        assert!(dir_path.is_dir());
        assert!(file_path1.is_file());
        assert!(file_path2.is_file());

        // 写入一条数据
        appender.write_all(b"hello,world")?;
        appender.flush()?;

        // 新文件 3 被创建
        let file_path3 = dir_path.join("mqtt.dump.202410231339.csv");
        assert!(file_path3.is_file());

        // 读取文件 3 数据
        assert_eq!(&std::fs::read_to_string(&file_path2)?, "hello,world");

        // 等待后台线程删除文件
        std::thread::sleep(Duration::from_secs(1));

        // 文件 2,文件 3 和目录存在，文件 1 被删除
        assert!(dir_path.is_dir());
        assert!(!file_path1.is_file());
        assert!(file_path2.is_file());
        assert!(file_path3.is_file());

        Ok(())
    }

    #[test]
    fn init_use_old_file_test() -> anyhow::Result<()> {
        let clock = TestClock(Arc::new(Mutex::new(dt("2024-10-22T13:40:00+08:00"))));

        let dir = TempDir::new()?;
        let dir_path = dir.path();

        // 提前创建好文件
        let file_path = dir_path.join("mqtt.dump.202410221339.csv");
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;
        assert!(file_path.is_file());

        // 初始化
        let mut appender = RollingFileAppender::new(dir_path.to_path_buf(), 1, clock.clone())?;
        appender.write_all(b"hello,world")?;
        appender.flush()?;

        assert!(file_path.is_file());
        assert_eq!(&std::fs::read_to_string(&file_path)?, "hello,world");

        // 中途删除文件
        std::fs::remove_file(&file_path)?;
        assert!(!file_path.is_file());

        // 再次写入，文件自动创建
        appender.write_all(b"hello,world")?;
        appender.flush()?;

        let file_path = dir_path.join("mqtt.dump.202410221340.csv");
        assert!(file_path.is_file());
        assert_eq!(&std::fs::read_to_string(&file_path)?, "hello,world");

        Ok(())
    }
}
