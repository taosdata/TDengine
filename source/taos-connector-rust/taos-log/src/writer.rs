use std::cmp::Reverse;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use chrono::{Local, NaiveTime, TimeDelta};
use flate2::write::GzEncoder;
use parking_lot::{RwLock, RwLockReadGuard};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use regex::Regex;
use snafu::{ensure, OptionExt, ResultExt};
use sysinfo::Disks;
use tracing::Level;

use crate::{
    CompressSnafu, CreateLogDirSnafu, DiskMountPointNotFoundSnafu, FileLockSnafu, GetFileSizeSnafu,
    GetLogAbsolutePathSnafu, InvalidRotationSizeSnafu, OpenLogFileSnafu, ReadDirSnafu, Result,
};

#[derive(Debug, Clone)]
struct Rotation {
    file_size: u64,
}

struct State {
    x: u32,
    y: u8,
    file_path: PathBuf,
}

pub struct RollingFileAppenderBuilder<'a> {
    log_dir: PathBuf,
    log_keep_days: TimeDelta,
    rotation_size: &'a str,
    compress: bool,
    reserved_disk_size: &'a str,
    stop_logging_threshold: usize,
}

impl<'a> RollingFileAppenderBuilder<'a> {
    pub fn keep_days(self, log_keep_days: u16) -> Self {
        Self {
            log_keep_days: TimeDelta::days(log_keep_days as _),
            ..self
        }
    }

    pub fn rotation_size(self, rotation_size: &'a str) -> Self {
        Self {
            rotation_size,
            ..self
        }
    }

    pub fn compress(self, compress: bool) -> Self {
        Self { compress, ..self }
    }

    pub fn reserved_disk_size(self, reserved_disk_size: &'a str) -> Self {
        Self {
            reserved_disk_size,
            ..self
        }
    }

    pub fn stop_logging_threadhold(self, stop_logging_threshold: usize) -> Self {
        Self {
            stop_logging_threshold,
            ..self
        }
    }

    pub fn build(mut self) -> Result<RollingFileAppender> {
        if !self.log_dir.is_absolute() {
            self.log_dir = self
                .log_dir
                .canonicalize()
                .context(GetLogAbsolutePathSnafu)?;
        }

        if !self.log_dir.is_dir() {
            fs::create_dir_all(&self.log_dir).context(CreateLogDirSnafu {
                path: &self.log_dir,
            })?;
        }

        let (x, y, file, file_path) = process_log_filename(&self.log_dir);

        let rotation = Rotation {
            file_size: parse_unit_size(self.rotation_size)?,
        };

        let disk_available_space = calc_disk_available_space(&self.log_dir)?;

        let config = Config {
            log_dir: self.log_dir,
            rotation,
            reserved_disk_size: parse_unit_size(self.reserved_disk_size)?,
            compress: self.compress,
            log_keep_days: self.log_keep_days,
            stop_logging_threshold: self.stop_logging_threshold as f64 / 100f64,
        };

        let (event_tx, event_rx) = flume::bounded(1);

        thread::spawn({
            let config = config.clone();
            move || {
                while event_rx.recv().is_ok() {
                    config.handle_old_files().ok();
                }
            }
        });

        event_tx.send(()).ok();

        Ok(RollingFileAppender {
            config,
            disk_available_space,
            level_downgrade: AtomicBool::default(),
            event_tx,
            state: RwLock::new(State { x, y, file_path }),
            writer: RwLock::new(file),
        })
    }
}

fn process_log_filename(log_dir: &Path) -> (u32, u8, File, PathBuf) {
    for x in 0u32.. {
        let p0 = log_dir.join(format!("taoswslog{x}.0"));
        let p1 = log_dir.join(format!("taoswslog{x}.1"));

        let mut f0_opt = if p0.exists() {
            match OpenOptions::new().read(true).append(true).open(&p0) {
                Ok(file) => match file.try_lock() {
                    Ok(()) => Some(file),
                    Err(_) => continue,
                },
                Err(_) => continue,
            }
        } else {
            None
        };

        let mut f1_opt = if p1.exists() {
            match OpenOptions::new().read(true).append(true).open(&p1) {
                Ok(file) => match file.try_lock() {
                    Ok(()) => Some(file),
                    Err(_) => continue,
                },
                Err(_) => continue,
            }
        } else {
            None
        };

        match (f0_opt.take(), f1_opt.take()) {
            (Some(f0), None) => return (x, 0, f0, p0),
            (None, Some(f1)) => return (x, 1, f1, p1),
            (None, None) => {
                if let Ok(f0) = OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .append(true)
                    .open(&p0)
                {
                    if f0.try_lock().is_ok() {
                        return (x, 0, f0, p0);
                    }
                }
            }
            (Some(f0), Some(f1)) => {
                let (m0, m1) = match (
                    f0.metadata().and_then(|md| md.modified()),
                    f1.metadata().and_then(|md| md.modified()),
                ) {
                    (Ok(m0), Ok(m1)) => (m0, m1),
                    _ => continue,
                };

                if m0 >= m1 {
                    return (x, 0, f0, p0);
                }
                return (x, 1, f1, p1);
            }
        }
    }

    unreachable!("u32 range exhausted");
}

pub struct RollingFileAppender {
    config: Config,
    disk_available_space: Arc<AtomicU64>,
    level_downgrade: AtomicBool,
    event_tx: flume::Sender<()>,
    state: RwLock<State>,
    writer: RwLock<File>,
}

impl RollingFileAppender {
    pub fn builder<'a, P: AsRef<Path>>(log_dir: P) -> RollingFileAppenderBuilder<'a> {
        RollingFileAppenderBuilder {
            log_dir: log_dir.as_ref().to_path_buf(),
            rotation_size: "1GB",
            compress: true,
            log_keep_days: TimeDelta::days(30),
            reserved_disk_size: "2GB",
            stop_logging_threshold: 50,
        }
    }

    fn rotate(&self) -> Result<Option<File>> {
        let mut state = self.state.write();

        let cur_size = self
            .writer
            .read()
            .metadata()
            .context(GetFileSizeSnafu {
                path: &state.file_path,
            })?
            .len();

        if cur_size >= self.config.rotation.file_size {
            let y = 1 - state.y;
            let new_filename = format!("taoswslog{}.{}", state.x, y);
            let new_file_path = self.config.log_dir.join(new_filename);
            let new_file = create_or_open_file(&new_file_path)?;

            if self.config.compress {
                let old_filename = format!("taoswslog{}.{}", state.x, state.y);
                let old_file_path = self.config.log_dir.join(old_filename);
                compress(&old_file_path).ok();
            }

            self.event_tx.try_send(()).ok();

            state.y = y;
            state.file_path = new_file_path;
            return Ok(Some(new_file));
        }

        if !state.file_path.is_file() {
            let file = create_or_open_file(&state.file_path)?;
            return Ok(Some(file));
        }

        Ok(None)
    }
}

#[derive(Debug, Clone)]
struct Config {
    log_dir: PathBuf,
    rotation: Rotation,
    reserved_disk_size: u64,
    compress: bool,
    log_keep_days: TimeDelta,
    stop_logging_threshold: f64,
}

impl Config {
    fn handle_old_files(&self) -> Result<()> {
        let mut files = fs::read_dir(&self.log_dir)
            .context(ReadDirSnafu {
                path: &self.log_dir,
            })?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let md = entry.metadata().ok()?;
                if !md.is_file() {
                    return None;
                }

                let filename = entry.file_name().to_str()?.to_string();
                if is_active_log_file(&filename) {
                    return None;
                }

                let ts = parse_compressed_filename(&filename)?;
                Some((filename, ts))
            })
            .collect::<Vec<_>>();

        if files.is_empty() {
            return Ok(());
        }

        files.sort_by(|a, b| b.1.cmp(&a.1));

        if !self.log_keep_days.is_zero() {
            let cutoff_time = Local::now().with_time(NaiveTime::MIN).unwrap() - self.log_keep_days;
            let cutoff_ts = cutoff_time.timestamp_nanos_opt().unwrap();
            let idx = files.partition_point(|(_, ts)| *ts >= cutoff_ts);
            files[idx..].into_par_iter().for_each(|(filename, _)| {
                fs::remove_file(self.log_dir.join(filename)).ok();
            });
        }

        Ok(())
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

pub enum TaosLogWriter<'a> {
    Rolling(RollingWriter<'a>),
    Null(std::io::Empty),
}

impl std::io::Write for TaosLogWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            TaosLogWriter::Rolling(w) => w.write(buf),
            TaosLogWriter::Null(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            TaosLogWriter::Rolling(w) => w.flush(),
            TaosLogWriter::Null(w) => w.flush(),
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RollingFileAppender {
    type Writer = TaosLogWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        if let Ok(Some(file)) = self.rotate() {
            *self.writer.write() = file;
        }
        TaosLogWriter::Rolling(RollingWriter(self.writer.read()))
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        let level = meta.level();
        let cur_disk_space = self.disk_available_space.load(Ordering::SeqCst);
        if cur_disk_space as f64 / self.config.reserved_disk_size as f64
            <= self.config.stop_logging_threshold
        {
            return TaosLogWriter::Null(std::io::empty());
        }

        let level_downgrade = cur_disk_space <= self.config.reserved_disk_size;
        if level_downgrade
            && self
                .level_downgrade
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok_and(|x| !x)
        {
            let mut writer = self.make_writer();
            writer.write_all(b"======= level downgrade =======\n").ok();
            writer.flush().ok();
        }
        if !level_downgrade
            && self
                .level_downgrade
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok_and(|x| x)
        {
            let mut writer = self.make_writer();
            writer.write_all(b"======= level upgrade =======\n").ok();
            writer.flush().ok();
        }

        if level_downgrade && level > &Level::ERROR {
            TaosLogWriter::Null(std::io::empty())
        } else {
            self.make_writer()
        }
    }
}

fn create_or_open_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .context(OpenLogFileSnafu { path })?;
    file.try_lock().context(FileLockSnafu { path })?;
    Ok(file)
}

fn calc_disk_available_space(log_dir: &Path) -> Result<Arc<AtomicU64>> {
    let mut disks = Disks::new();
    disks.refresh_list();
    let mut disks = Vec::from(disks);
    disks.sort_by_key(|d| Reverse(d.mount_point().to_str().map(|s| s.len())));
    let mut disk = disks
        .into_iter()
        .find(|d| log_dir.starts_with(d.mount_point()))
        .context(DiskMountPointNotFoundSnafu)?;
    disk.refresh();
    let disk_available_space = Arc::new(AtomicU64::new(disk.available_space()));

    thread::spawn({
        let disk_available_space = disk_available_space.clone();
        move || -> ! {
            loop {
                disk.refresh();
                disk_available_space.store(disk.available_space(), Ordering::SeqCst);
                std::thread::sleep(Duration::from_secs(30));
            }
        }
    });

    Ok(disk_available_space)
}

fn compress(path: &Path) -> Result<()> {
    let ts = Local::now().timestamp_nanos_opt().unwrap();
    let compressed_name = format!("taoswslog.{ts}.gz");
    let dest_path = path.parent().unwrap().join(compressed_name);

    let mut src_file = File::open(path).context(CompressSnafu { path })?;
    let dest_file = match OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&dest_path)
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Ok(());
        }
        e @ Err(_) => e.context(OpenLogFileSnafu { path })?,
    };

    let mut encoder = GzEncoder::new(dest_file, flate2::Compression::default());
    std::io::copy(&mut src_file, &mut encoder).context(CompressSnafu { path })?;
    fs::remove_file(path).context(CompressSnafu { path })?;
    Ok(())
}

fn is_active_log_file(filename: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^taoswslog\d+\.[01]$").unwrap());
    re.is_match(filename)
}

fn parse_compressed_filename(filename: &str) -> Option<i64> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^taoswslog\.(?<timestamp>\d+)\.gz$").unwrap());
    let caps = re.captures(filename)?;
    caps.name("timestamp")?.as_str().parse().ok()
}

fn parse_unit_size(size: &str) -> Result<u64> {
    ensure!(size.len() >= 3, InvalidRotationSizeSnafu { size });
    ensure!(size.is_ascii(), InvalidRotationSizeSnafu { size });
    let (count, unit) = size.split_at(size.len() - 2);
    let count = count
        .parse::<u64>()
        .ok()
        .context(InvalidRotationSizeSnafu { size })?;
    match unit.to_ascii_uppercase().as_str() {
        "KB" => Ok(count * 1024),
        "MB" => Ok(count * 1024 * 1024),
        "GB" => Ok(count * 1024 * 1024 * 1024),
        _ => InvalidRotationSizeSnafu { size }.fail(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::sync::Arc;
    use std::thread;

    use tempfile::tempdir;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[test]
    fn test_is_active_log_file() {
        assert!(is_active_log_file("taoswslog0.0"));
        assert!(is_active_log_file("taoswslog1.1"));
        assert!(!is_active_log_file("taoswslog.123"));
        assert!(!is_active_log_file("taoswslog.123.gz"));
    }

    #[test]
    fn test_parse_compressed_filename() {
        assert_eq!(parse_compressed_filename("taoswslog.0.gz"), Some(0));
        assert_eq!(
            parse_compressed_filename("taoswslog.1692600000.gz"),
            Some(1692600000)
        );

        assert_eq!(parse_compressed_filename("taoswslog.gz"), None);
        assert_eq!(parse_compressed_filename("taoswslog.abc.gz"), None);
        assert_eq!(parse_compressed_filename("taoswslog.123.txt"), None);
        assert_eq!(parse_compressed_filename("randomfile.gz"), None);
        assert_eq!(parse_compressed_filename("taoswslog.1234567890"), None);
    }

    #[test]
    fn test_parse_unit_size() {
        assert_eq!(parse_unit_size("5KB").unwrap(), 5 * 1024);
        assert_eq!(parse_unit_size("5MB").unwrap(), 5 * 1024 * 1024);
        assert_eq!(parse_unit_size("5GB").unwrap(), 5 * 1024 * 1024 * 1024);

        assert!(parse_unit_size("5GBK").is_err());
        assert!(parse_unit_size("GB").is_err());
    }

    #[test]
    fn test_log_file_naming_and_rotation() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let appender = RollingFileAppender::builder(log_dir)
            .rotation_size("1KB")
            .keep_days(1)
            .compress(false)
            .build()
            .unwrap();

        {
            let mut writer = appender.make_writer();
            for _ in 0..1100 {
                writer.write_all(b"x").unwrap();
            }
            writer.flush().unwrap();
        }

        {
            let mut writer = appender.make_writer();
            for _ in 0..1100 {
                writer.write_all(b"y").unwrap();
            }
            writer.flush().unwrap();
        }

        let files: Vec<_> = fs::read_dir(log_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .collect();

        assert!(files.contains(&"taoswslog0.0".to_string()));
        assert!(files.contains(&"taoswslog0.1".to_string()));
    }

    #[test]
    fn test_compress_and_compressed_filename() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();
        let log_path = log_dir.join("taoswslog0.0");

        fs::write(&log_path, b"hello world").unwrap();
        compress(&log_path).unwrap();
        assert!(!log_path.exists());

        let files: Vec<_> = fs::read_dir(log_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .collect();

        let gz_file = files.iter().find(|f| f.ends_with(".gz")).unwrap();
        assert!(parse_compressed_filename(gz_file).is_some());
    }

    #[test]
    fn test_keep_days_removes_old_files() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let now = Local::now().timestamp_nanos_opt().unwrap();
        let two_days_ago = (Local::now() - Duration::from_secs(2 * 24 * 60 * 60))
            .timestamp_nanos_opt()
            .unwrap();

        let today_file = log_dir.join(format!("taoswslog.{now}.gz"));
        let old_file = log_dir.join(format!("taoswslog.{two_days_ago}.gz"));
        fs::write(&today_file, b"today").unwrap();
        fs::write(&old_file, b"two_days_ago").unwrap();

        let config = Config {
            log_dir: log_dir.to_path_buf(),
            rotation: Rotation { file_size: 1024 },
            reserved_disk_size: 1024 * 1024,
            compress: true,
            log_keep_days: TimeDelta::days(1),
            stop_logging_threshold: 0.1,
        };
        config.handle_old_files().unwrap();

        let files: Vec<_> = fs::read_dir(log_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .collect();

        assert!(files.iter().any(|f| f == &format!("taoswslog.{now}.gz")));
        assert!(!files
            .iter()
            .any(|f| f == &format!("taoswslog.{two_days_ago}.gz")));
    }

    #[test]
    fn test_log_file_auto_recreate_after_delete() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let appender = RollingFileAppender::builder(log_dir)
            .rotation_size("1KB")
            .build()
            .unwrap();

        {
            let mut writer = appender.make_writer();
            writer.write_all(b"abc").unwrap();
            writer.flush().unwrap();
        }

        let state = appender.state.read();
        let file_path = state.file_path.clone();
        drop(state);
        std::fs::remove_file(&file_path).unwrap();

        {
            let mut writer = appender.make_writer();
            writer.write_all(b"def").unwrap();
            writer.flush().unwrap();
        }

        assert!(file_path.exists());
    }

    #[test]
    fn test_log_content_integrity_after_rotation_and_compress() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let appender = RollingFileAppender::builder(log_dir)
            .rotation_size("1KB")
            .compress(true)
            .build()
            .unwrap();

        let content = vec![b'a'; 1030];

        {
            let mut writer = appender.make_writer();
            writer.write_all(&content).unwrap();
            writer.flush().unwrap();
        }
        {
            let mut writer = appender.make_writer();
            writer.write_all(&content).unwrap();
            writer.flush().unwrap();
        }

        let files: Vec<_> = std::fs::read_dir(log_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .collect();
        let gz_file = files.iter().find(|f| f.ends_with(".gz")).unwrap();
        let gz_path = log_dir.join(gz_file);

        let gz_file = std::fs::File::open(&gz_path).unwrap();
        let mut decoder = flate2::read::GzDecoder::new(gz_file);
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, content);
    }

    #[test]
    fn test_parallel_write_and_rotate() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let appender = Arc::new(
            RollingFileAppender::builder(log_dir)
                .rotation_size("1KB")
                .compress(false)
                .build()
                .unwrap(),
        );

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let appender = appender.clone();
                thread::spawn(move || {
                    let mut writer = appender.make_writer();
                    for _ in 0..600 {
                        writer.write_all(b"x").unwrap();
                    }
                    writer.flush().unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let files: Vec<_> = std::fs::read_dir(log_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .collect();
        let count = files
            .iter()
            .filter(|&f| f == "taoswslog0.0" || f == "taoswslog0.1")
            .count();
        assert!(count <= 2);
    }

    #[test]
    fn test_lock_exclusivity_same_process() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let appender = RollingFileAppender::builder(log_dir)
            .rotation_size("4KB")
            .build()
            .unwrap();

        {
            let mut writer = appender.make_writer();
            writer.write_all(b"hello").unwrap();
            writer.flush().unwrap();
        }

        let path = { appender.state.read().file_path.clone() };
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .unwrap();
        assert!(file.try_lock().is_err());
    }

    #[test]
    fn test_multiple_appenders_allocate_distinct_x() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let a1 = RollingFileAppender::builder(log_dir)
            .rotation_size("4KB")
            .build()
            .unwrap();
        let a2 = RollingFileAppender::builder(log_dir)
            .rotation_size("4KB")
            .build()
            .unwrap();

        let x1 = a1.state.read().x;
        let x2 = a2.state.read().x;
        assert_ne!(x1, x2);
    }

    #[test]
    fn test_rotation_preserves_new_lock() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let appender = RollingFileAppender::builder(log_dir)
            .rotation_size("1KB")
            .build()
            .unwrap();

        let (old_path, old_y) = {
            let st = appender.state.read();
            (st.file_path.clone(), st.y)
        };

        for _ in 0..1100 {
            let mut writer = appender.make_writer();
            writer.write_all(b"x").unwrap();
        }

        let (new_path, new_y) = {
            let st = appender.state.read();
            (st.file_path.clone(), st.y)
        };

        assert_ne!(old_y, new_y);
        assert_ne!(old_path, new_path);

        let new_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&new_path)
            .unwrap();
        assert!(new_file.try_lock().is_err());

        std::thread::sleep(Duration::from_millis(100));

        assert!(!old_path.exists())
    }

    #[test]
    fn test_no_creation_during_probe_missing_files() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let log_path = log_dir.join("taoswslog0.0");
        fs::write(&log_path, b"seed").unwrap();
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&log_path)
            .unwrap();
        file.try_lock().unwrap();

        let appender = RollingFileAppender::builder(log_dir)
            .rotation_size("4KB")
            .build()
            .unwrap();
        let st = appender.state.read();
        assert_eq!(st.x, 1);
    }

    #[test]
    fn test_second_appender_rotates_independently() {
        let dir = tempdir().unwrap();
        let log_dir = dir.path();

        let a1 = RollingFileAppender::builder(log_dir)
            .rotation_size("1KB")
            .build()
            .unwrap();
        let a2 = RollingFileAppender::builder(log_dir)
            .rotation_size("1KB")
            .build()
            .unwrap();

        for _ in 0..1200 {
            let mut w1 = a1.make_writer();
            w1.write_all(b"x").unwrap();
            let mut w2 = a2.make_writer();
            w2.write_all(b"y").unwrap();
        }

        let s1 = a1.state.read();
        let s2 = a2.state.read();
        assert_ne!(s1.x, s2.x);

        let files: Vec<_> = fs::read_dir(log_dir)
            .unwrap()
            .filter_map(|res| res.ok())
            .map(|de| de.file_name().into_string().unwrap())
            .collect();

        let count_x1 = files
            .iter()
            .filter(|f| f.starts_with(&format!("taoswslog{}.", s1.x)))
            .count();
        let count_x2 = files
            .iter()
            .filter(|f| f.starts_with(&format!("taoswslog{}.", s2.x)))
            .count();
        assert!(count_x1 <= 2);
        assert!(count_x2 <= 2);
    }
}
