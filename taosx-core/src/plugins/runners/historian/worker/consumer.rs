use arrow::ipc::writer::StreamWriter;
use flume::Receiver;
use futures_util::TryStreamExt;
use tiberius::QueryItem;

use crate::runners::historian::arrow::ArrowDataAppender;
use crate::runners::historian::config::{HistorianTable, TaskConfig};
use crate::runners::historian::query::HistorianQuery;

pub struct Consumer {
    query: HistorianQuery,
    port: u16,
}

impl Consumer {
    pub fn new(query: HistorianQuery, port: u16) -> Self {
        Self { query, port }
    }

    pub async fn consume(&mut self, receiver: Receiver<TaskConfig>) -> anyhow::Result<()> {
        let socket = format!("127.0.0.1:{}", self.port);
        let stream = std::net::TcpStream::connect(socket)?;
        let mut appender = ArrowDataAppender::try_new(HistorianTable::History)?;
        let mut writer = StreamWriter::try_new(&stream, appender.schema())?;

        while let Ok(task) = receiver.recv_async().await {
            let start = task
                .begin_datetime
                .ok_or(anyhow::anyhow!("beginDateTime cannot be None"))?;
            let end = task
                .end_datetime
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
        }

        writer.finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_consume() {}
}
