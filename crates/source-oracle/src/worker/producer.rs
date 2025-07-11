use std::cmp::min;

use chrono::{Days, FixedOffset, Utc};
use flume::Sender;
use std::str::FromStr;

use crate::config::OracleConfig;

pub struct Producer {
    config: OracleConfig,
}

impl Producer {
    pub fn new(config: &OracleConfig) -> Self {
        Producer {
            config: config.clone(),
        }
    }

    pub async fn produce(&self, tx: Sender<OracleConfig>) -> anyhow::Result<()> {
        let start = self.config.task.start;
        let end = match self.config.task.end {
            Some(end) => end,
            None => Utc::now(),
        };
        let time_zone = FixedOffset::from_str(&self.config.task.time_zone.to_string())?;
        let interval = self.config.task.interval;
        tracing::debug!(
            "produce tasks, start: {}, end: {}, interval: {}",
            start,
            end,
            interval
        );

        // split the task into multiple windows
        let window_start = start;

        // with time zone
        let mut window_start_with_tz = window_start.with_timezone(&time_zone);
        let end_with_tz = end.with_timezone(&time_zone);

        while window_start_with_tz < end_with_tz {
            // calculate the end of the window
            let mut window_end_with_tz = min(window_start_with_tz + interval, end_with_tz);

            // when the window across days, we need to adjust the end to the start of the next day
            if window_end_with_tz.date_naive() > window_start_with_tz.date_naive() {
                window_end_with_tz = window_start_with_tz
                    .date_naive()
                    .checked_add_days(Days::new(1))
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_local_timezone(time_zone)
                    .unwrap();
            }

            // create a new task
            let mut config = self.config.clone();
            config.task.start = window_start_with_tz.with_timezone(&Utc);
            config.task.end = Some(window_end_with_tz.with_timezone(&Utc));
            let _ = tx.send_async(config).await;
            tracing::debug!(
                "produce task, window_start_with_tz: {}, window_end_with_tz: {}, end_with_tz: {}, next: {}",
                window_start_with_tz,
                window_end_with_tz,
                end_with_tz,
                window_start_with_tz < end_with_tz
            );

            // move the window
            window_start_with_tz = window_end_with_tz;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taos::Dsn;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_produce() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric&start=2021-01-01T00:00:00Z&end=2021-02-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let config = OracleConfig::from_dsn(&dsn).unwrap();

        let (tx, rx) = flume::bounded(4);

        let consumer = tokio::spawn(async move {
            let mut tasks = Vec::new();
            for msg in rx.iter() {
                tasks.push(msg);
            }
            tasks
        });

        let producer = Producer::new(&config);
        producer.produce(tx).await.unwrap();

        let tasks = consumer.await.unwrap();

        assert_eq!(62, tasks.len());
        dbg!(tasks.first().unwrap());
    }
}
