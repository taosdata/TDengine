use arrow::ipc::writer::StreamWriter;
use flume::Receiver;
use futures_util::TryStreamExt;
use tiberius::QueryItem;

use crate::runners::historian::arrow::ArrowDataAppender;
use crate::runners::historian::config::TaskConfig;
use crate::runners::historian::query::HistorianQuery;
use crate::utils::port_pool::PortPool;

pub struct Consumer {
    port_pool: PortPool,
    query: HistorianQuery,
}

impl Consumer {
    pub fn new(query: HistorianQuery, port_pool: PortPool) -> Self {
        Consumer {
            port_pool,
            query,
        }
    }

    pub async fn consume(&mut self, receiver: Receiver<TaskConfig>) -> anyhow::Result<()> {
        for task in receiver.iter() {

            let port = self.port_pool
                .get()
                .await
                .ok_or_else(|| anyhow::format_err!("No available port for Historian source"))?;
            let socket = format!("127.0.0.1:{}", port);
            let stream = std::net::TcpStream::connect(socket)?;
            let mut appender = ArrowDataAppender::new(&task)?;
            let mut writer = StreamWriter::try_new(&stream, appender.schema())?;

            let start = task.begin_datetime;
            let end = task.end_datetime
                .ok_or(anyhow::anyhow!("endDateTime cannot be None"))?;
            tracing::debug!("execute migrate query, from: {}, to: {}", start, end);

            // query
            let mut rows = self.query.query_history(task.tags, start, end).await?;
            while let Some(row) = rows.try_next().await? {
                match row {
                    QueryItem::Row(row) => {
                        appender.append_history_row(row)?;
                    }
                    QueryItem::Metadata(_) => {
                        continue;
                    }
                }
            }

            // write batch
            let batch = appender.finish()?;
            writer.write(&batch)?;
            writer.finish()?;

            self.port_pool.put(port).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_consume() {}
}