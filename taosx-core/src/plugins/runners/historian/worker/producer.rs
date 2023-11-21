use std::cmp::min;

use flume::Sender;

use crate::runners::historian::config::TaskConfig;

pub struct Producer {
    config: TaskConfig,
}

impl Producer {
    pub fn new(config: &TaskConfig) -> Self {
        Producer {
            config: config.clone(),
        }
    }

    pub async fn produce(&self, tx: Sender<TaskConfig>) -> anyhow::Result<()> {
        let mut window_start = self.config.begin_datetime;
        let end = self
            .config
            .end_datetime
            .ok_or(anyhow::anyhow!("endDateTime cannot be None"))?;
        let time_window = self.config.time_window;

        while window_start < end {
            let window_end = min(window_start + time_window, end);
            tracing::debug!(
                "create migrate task, from: {}, to: {}",
                window_start,
                window_end
            );

            let mut task = self.config.clone();
            task.begin_datetime = window_start;
            task.end_datetime = Some(window_end);
            tx.send(task).unwrap();

            window_start = window_start + time_window;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use taos::IntoDsn;

    use super::*;

    #[tokio::test]
    async fn test_produce() {
        let dsn = format!(
            "historian://aaAdmin:aaAdmin@localhost:1433/?mode={}&table={}&beginDateTime={}&endDateTime={}&timeWindow={}",
            "migrate",
            "Runtime.dbo.History",
            "2021-08-01T00:00:00Z",
            "2021-08-04T12:00:00Z",
            "1d"
        ).into_dsn().unwrap();
        let config = TaskConfig::from_dsn(&dsn).unwrap();

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

        assert_eq!(4, tasks.len());
        let t = tasks.get(0).unwrap();
        assert_eq!("2021-08-01T00:00:00+00:00", t.begin_datetime.to_rfc3339());
        assert_eq!(
            "2021-08-02T00:00:00+00:00",
            t.end_datetime.unwrap().to_rfc3339()
        );
        let t = tasks.get(1).unwrap();
        assert_eq!("2021-08-02T00:00:00+00:00", t.begin_datetime.to_rfc3339());
        assert_eq!(
            "2021-08-03T00:00:00+00:00",
            t.end_datetime.unwrap().to_rfc3339()
        );
        let t = tasks.get(2).unwrap();
        assert_eq!("2021-08-03T00:00:00+00:00", t.begin_datetime.to_rfc3339());
        assert_eq!(
            "2021-08-04T00:00:00+00:00",
            t.end_datetime.unwrap().to_rfc3339()
        );
        let t = tasks.get(3).unwrap();
        assert_eq!("2021-08-04T00:00:00+00:00", t.begin_datetime.to_rfc3339());
        assert_eq!(
            "2021-08-04T12:00:00+00:00",
            t.end_datetime.unwrap().to_rfc3339()
        );
    }
}
