use std::io::Write;

use file_rotate::compression::Compression;
use file_rotate::suffix::{AppendTimestamp, DateFrom, FileLimit};
use file_rotate::{ContentLimit, FileRotate, TimeFrequency};
use flume::Receiver;

pub struct RawDataLogger {
    task_id: i64,
    keep_raw_data: bool,
    data_dir: String,
    keep_days: usize,
    rx: Receiver<String>,
}

impl RawDataLogger {
    pub fn new(
        task_id: i64,
        keep_raw_data: bool,
        data_dir: String,
        keep_days: usize,
        rx: Receiver<String>,
    ) -> Self {
        Self {
            task_id,
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
            tokio::spawn(async move { while let Ok(_) = rx.recv_async().await {} });
        } else {
            let log_name = format!("{}/tasks/{}/rawdata", self.data_dir, self.task_id);
            let mut log = raw_data_log(log_name.clone(), self.keep_days);
            tracing::debug!(
                "keep task:{} raw data in files: {:?}",
                self.task_id,
                log_name
            );

            let rx = self.rx.clone();

            tokio::spawn(async move {
                while let Ok(raw_data) = rx.recv_async().await {
                    writeln!(log, "{}", raw_data).expect("failed to write raw data");
                }
            });
        }
    }
}

fn raw_data_log(log_file: String, keep_days: usize) -> FileRotate<AppendTimestamp> {
    FileRotate::new(
        log_file,
        AppendTimestamp::with_format(
            "%Y-%m-%d",
            FileLimit::MaxFiles(keep_days),
            DateFrom::DateYesterday,
        ),
        ContentLimit::Time(TimeFrequency::Daily),
        Compression::None,
        #[cfg(unix)]
        None,
    )
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[tokio::test]
    async fn test_raw_data_logger() {
        let (tx, rx) = flume::bounded(0);

        let logger = RawDataLogger::new(0, true, "logs".to_string(), 1, rx);
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
}
