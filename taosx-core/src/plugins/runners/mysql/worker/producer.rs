use std::cmp::min;

use chrono::{Days, Utc};
use flume::Sender;

use crate::runners::mysql::config::MySqlConfig;

pub struct Producer {
    config: MySqlConfig,
}

impl Producer {
    pub fn new(config: &MySqlConfig) -> Self {
        Producer {
            config: config.clone(),
        }
    }

    pub async fn produce(&self, tx: &Sender<MySqlConfig>) -> anyhow::Result<()> {
        let start = self.config.task.start;
        let end = match self.config.task.end {
            Some(end) => end,
            None => Utc::now(),
        };
        let interval = self.config.task.interval;
        tracing::debug!(
            "produce tasks, start: {}, end: {}, interval: {}",
            start,
            end,
            interval
        );

        // split the task into multiple windows
        let mut window_start = start.clone();
        while window_start < end {
            // calculate the end of the window
            let mut window_end = min(window_start + interval, end);

            // when the window across days, we need to adjust the end to the start of the next day
            if window_start.date_naive() != window_end.date_naive() {
                window_end = window_start
                    .date_naive()
                    .checked_add_days(Days::new(1))
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc();
            }

            // create a new task
            let mut config = self.config.clone();
            config.task.start = window_start;
            config.task.end = Some(window_end);
            let _ = tx.send_async(config).await;
            tracing::debug!(
                "produce task, window_start: {}, window_end: {}, end: {}, next: {}",
                window_start,
                window_end,
                end,
                window_start < end
            );

            // move the window
            window_start = window_end;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_produce() {
        let dsn = Dsn::from_str("mysql://root:password@localhost:3306/dbname?sql=select * from table&start=2021-01-01T00:00:00Z&end=2021-02-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let config = MySqlConfig::from_dsn(&dsn).unwrap();

        let (tx, rx) = flume::bounded(4);

        let consumer = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for msg in rx.iter() {
                tasks.push(msg);
            }
            tasks
        });

        let producer = Producer::new(&config);
        producer.produce(&tx).await.unwrap();

        // let tasks = consumer.await.unwrap();

        // assert_eq!(62, tasks.len());
        // dbg!(tasks.get(0).unwrap());
        // drop(tx);
    }
}
