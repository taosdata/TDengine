use std::cmp::min;

use flume::Sender;
use itertools::Itertools;

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
        let mut window_start = self
            .config
            .begin_datetime
            .ok_or(anyhow::anyhow!("beginDateTime cannot be None"))?;
        let end = self
            .config
            .end_datetime
            .ok_or(anyhow::anyhow!("endDateTime cannot be None"))?;
        let time_window = self.config.time_window;
        tracing::debug!(
            "produce task, begin: {}, end: {}, timeWindow: {}",
            window_start,
            end,
            time_window
        );

        while window_start < end {
            let window_end = min(window_start + time_window, end);

            let tasks = self
                .config
                .tags
                .iter()
                .chunks(self.config.tag_list_size)
                .into_iter()
                .map(|list| {
                    let mut task = self.config.clone();

                    task.begin_datetime = Some(window_start);
                    task.end_datetime = Some(window_end);
                    task.tags = list.map(|s| s.to_string()).collect::<Vec<_>>();

                    task
                })
                .collect_vec();

            for task in tasks {
                tx.send_async(task).await.unwrap();
            }

            window_start = window_end;
        }

        tracing::debug!("produce task finished");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use taos::IntoDsn;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_produce() {
        let dsn = format!(
            "historian://aaAdmin:aaAdmin@192.168.3.40:1433/?mode={}&table={}&tags={}&tagListSize={}&beginDateTime={}&endDateTime={}&timeWindow={}",
            "migrate",
            "Runtime.dbo.History",
            "tag0,tag1,tag2,tag3,tag4,tag5,tag6,tag7,tag8,tag9",
            "3",
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

        assert_eq!(16, tasks.len());
        let t = tasks.first().unwrap();
        assert_eq!(
            "2021-08-01T00:00:00+00:00",
            t.begin_datetime.unwrap().to_rfc3339()
        );
        assert_eq!(
            "2021-08-02T00:00:00+00:00",
            t.end_datetime.unwrap().to_rfc3339()
        );
        assert_eq!(3, t.tags.len());
        assert_eq!("tag0", t.tags.first().unwrap());
        assert_eq!("tag1", t.tags.get(1).unwrap());
        assert_eq!("tag2", t.tags.get(2).unwrap());
    }
}
