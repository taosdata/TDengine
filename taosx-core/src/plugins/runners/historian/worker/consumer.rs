use arrow::ipc::writer::StreamWriter;
use flume::Receiver;
use futures_util::TryStreamExt;
use tiberius::QueryItem;

use taosx_ipc::ack::AckReaderBuilder;

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
        let mut appender = ArrowDataAppender::try_new(HistorianTable::History)?;
        let schema = appender.schema().clone();

        let socket = format!("127.0.0.1:{}", self.port);
        let stream = std::net::TcpStream::connect(socket)?;
        let ack_stream = stream.try_clone()?;

        let (tx, rx) = flume::bounded(0);
        let writer_handler = tokio::task::spawn_blocking(move || {
            stream.set_nonblocking(false)?;
            let mut writer = StreamWriter::try_new(stream, &schema)?;
            while let Ok(batch) = rx.recv() {
                writer.write(&batch)?;
            }
            let _ = writer.finish()?;
            anyhow::Ok(())
        });

        let ack = tokio::task::spawn_blocking(move || {
            let ack_reader =
                AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
            for ack in ack_reader {
                if !ack.success() {
                    tracing::warn!("write records error: {ack:?}",);
                    if let Some(message) = ack.message() {
                        anyhow::bail!("IPC writer error: {message}")
                    }
                }
            }
            tracing::info!("ACK reader finished");
            Ok(())
        });

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
            tracing::debug!("rows fetched");
            while let Some(row) = rows.try_next().await? {
                match row {
                    QueryItem::Row(row) => {
                        appender.append_history_row(row).map_err(|err| {
                            let err_msg = format!("append history row error: {}", err.to_string());
                            tracing::error!(err_msg);
                            anyhow::anyhow!(err_msg)
                        })?;
                    }
                    QueryItem::Metadata(_) => {
                        continue;
                    }
                }
            }
            tracing::debug!("record batch prepared");
            // write batch
            let batch = appender.finish()?;
            tracing::debug!("appender finish");
            tx.send_async(batch.clone()).await?;
            tracing::debug!("historian source write batch to ipc: {}", batch.num_rows());
        }

        writer_handler.await??;
        ack.await??;
        Ok(())
    }
}
