use std::io::Write;
use std::path::Path;

use crate::global::GLOBAL_LOG_OPTS;
use flume::Receiver;
use taoslog::writer::RollingFileAppender;
use tracing_subscriber::fmt::MakeWriter;

pub struct RawDataLogger {
    task_id: i64,
    job_id: i64,
    keep_raw_data: bool,
    data_dir: String,
    keep_days: usize,
    rx: Receiver<String>,
}

impl RawDataLogger {
    pub fn new(
        task_id: i64,
        job_id: i64,
        keep_raw_data: bool,
        data_dir: String,
        keep_days: usize,
        rx: Receiver<String>,
    ) -> Self {
        Self {
            task_id,
            job_id,
            keep_raw_data,
            data_dir,
            keep_days,
            rx,
        }
    }

    pub fn start(&self) {
        let keep_raw_data = self.keep_raw_data;
        if !keep_raw_data {
            let rx = self.rx.clone();
            tokio::spawn(async move { while rx.recv_async().await.is_ok() {} });
        } else {
            let log_dir = format!("{}/tasks/{}/{}", self.data_dir, self.task_id, self.job_id);
            let appender = raw_data_log(Path::new(&log_dir), self.keep_days)
                .expect("failed to create raw data logger");
            tracing::debug!(
                "keep task:({},{}) raw data in dir: {}",
                self.task_id,
                self.job_id,
                log_dir
            );

            let rx = self.rx.clone();

            let task_id = self.task_id;
            tokio::spawn(async move {
                while let Ok(raw_data) = rx.recv_async().await {
                    let mut w = appender.make_writer();
                    // writeln!(w, "{}", raw_data).expect("failed to write raw data");
                    if let Err(e) = writeln!(w, "{}", raw_data) {
                        eprintln!(
                            "[raw_data_logger] failed to write raw data for task {}: {}",
                            task_id, e
                        );
                    }
                    if let Err(e) = w.flush() {
                        eprintln!(
                            "[raw_data_logger] failed to flush raw data for task {}: {}",
                            task_id, e
                        );
                    }
                }
            });
        }
    }
}

fn raw_data_log(log_dir: &Path, keep_files: usize) -> anyhow::Result<RollingFileAppender> {
    let log_opts = GLOBAL_LOG_OPTS
        .get()
        .ok_or(anyhow::anyhow!("log opts not set"))?;

    let mut builder = RollingFileAppender::builder(log_dir, "rawdata", log_opts.instance_id);

    if let Some(compression) = log_opts.compress {
        builder = builder.compress(compression);
    }
    if let Some(reserved_disk_size) = &log_opts.reserved_disk_size {
        builder = builder.reserved_disk_size(reserved_disk_size);
    }
    if keep_files > 0 {
        builder = builder.rotation_count(keep_files as u16);
    } else if let Some(rotation_count) = log_opts.rotation_count {
        builder = builder.rotation_count(rotation_count);
    }
    if let Some(rotation_size) = &log_opts.rotation_size {
        builder = builder.rotation_size(rotation_size);
    }
    // We don't set keep_days here; raw data retention by file count only.
    let appender = builder.build()?;
    Ok(appender)
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_raw_data_log_builder_with_opts() {
        // Prepare log options once
        let _ = GLOBAL_LOG_OPTS.set(crate::global::LogOpts {
            instance_id: 1,
            compress: Some(false),
            rotation_count: Some(5),
            keep_days: Some(0),
            rotation_size: Some("10MB".to_string()),
            reserved_disk_size: Some("100MB".to_string()),
        });

        // Use a temporary directory within system temp dir
        let tmp = std::env::temp_dir().join("taosx_raw_data_test");
        let _ = std::fs::create_dir_all(&tmp);

        // Build appender with explicit keep_files override
        let appender = raw_data_log(&tmp, 3).expect("should build raw_data appender");

        // Write a small line to ensure writer works
        let mut w = appender.make_writer();
        writeln!(w, "hello").expect("write should succeed");
        w.flush().expect("flush should succeed");

        // Clean up directory best-effort
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    #[ignore]
    async fn test_raw_data_logger() {
        let (tx, rx) = flume::bounded(0);

        let logger = RawDataLogger::new(0, 0, true, "logs".to_string(), 1, rx);
        logger.start();

        let senders = (0..10)
            .map(|thread_index| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    for i in 0..100 {
                        let msg = format!("thread: {}, msg: {}", thread_index, i);
                        let _ = tx.send_async(msg).await;
                    }
                })
            })
            .collect_vec();

        for t in senders {
            t.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_raw_data_logger_without_keep() {
        // When keep_raw_data is false, start() should spawn a drain task that simply consumes messages.
        let (tx, rx) = flume::bounded::<String>(16);
        let logger =
            RawDataLogger::new(42, false, std::env::temp_dir().display().to_string(), 0, rx);
        logger.start();

        // Send a few messages, then drop sender so the background task can exit.
        for i in 0..10 {
            tx.send_async(format!("msg-{}", i)).await.unwrap();
        }
        drop(tx);

        // Give the spawned task a moment to drain; this test primarily ensures no panic on start/drain path.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn test_raw_data_logger_with_keep_minimal() {
        // Prepare log opts once
        let _ = GLOBAL_LOG_OPTS.set(crate::global::LogOpts {
            instance_id: 1,
            compress: Some(false),
            rotation_count: Some(2),
            keep_days: Some(0),
            rotation_size: Some("1MB".to_string()),
            reserved_disk_size: Some("10MB".to_string()),
        });

        let tmp = std::env::temp_dir().join("taosx_raw_data_keep_test");
        let _ = std::fs::create_dir_all(&tmp);

        let (tx, rx) = flume::bounded::<String>(16);
        let logger = RawDataLogger::new(100, true, tmp.display().to_string(), 1, rx);
        logger.start();

        // Send a handful of messages to exercise write path
        for i in 0..5 {
            tx.send_async(format!("raw-{}", i)).await.unwrap();
        }
        drop(tx);
        // Allow background task to process messages
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Best-effort cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
