use arrow::ipc::writer::StreamWriter;
use flume::{Receiver, Sender};
use futures_util::TryStreamExt;

use taosx_ipc::ack::AckReaderBuilder;

use crate::runners::historian::appender::ArrowDataAppender;
use crate::runners::historian::config::{HistorianTable, TaskConfig};
use crate::runners::historian::query::HistorianQuery;
use crate::runners::set_tcp_keepalive;

pub struct Consumer {
    query: HistorianQuery,
    ipc_port: u16,
}

impl Consumer {
    pub fn new(query: HistorianQuery, ipc_port: u16) -> Self {
        Self { query, ipc_port }
    }

    pub async fn consume(
        &mut self,
        receiver: Receiver<TaskConfig>,
        logger_tx: Sender<String>,
    ) -> anyhow::Result<()> {
        let mut appender = ArrowDataAppender::try_new(HistorianTable::History)?;
        let schema = appender.schema().clone();

        // IPC Tcp stream
        let socket = format!("127.0.0.1:{}", self.ipc_port);
        let stream = std::net::TcpStream::connect(socket)?;
        set_tcp_keepalive(&stream)?;
        stream.set_nonblocking(false)?;

        // ack reader stream
        let ack_stream = stream.try_clone()?;
        set_tcp_keepalive(&ack_stream)?;
        ack_stream.set_read_timeout(None)?;

        // write batch to IPC
        let (tx, rx) = flume::bounded(100);
        let writer_handler = tokio::task::spawn_blocking(move || {
            let mut writer = StreamWriter::try_new(stream, &schema)?;
            let mut row_count = 0;
            let mut batches = 0;

            while let Ok(batch) = rx.recv() {
                writer.write(&batch)?;
                tracing::debug!("migrate history write {} rows to ipc", batch.num_rows());

                row_count += batch.num_rows();
                batches += 1;
            }

            tracing::debug!(
                send.batches = batches,
                send.records = row_count,
                "sending finished, waiting for persisting"
            );
            let _ = writer.finish()?;
            anyhow::Ok(())
        });

        // receive ACK from IPC
        let ack = tokio::task::spawn_blocking(move || {
            let ack_reader =
                AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
            for ack in ack_reader {
                if !ack.success() {
                    tracing::error!("migrate history write records error: {ack:?}",);
                    if let Some(message) = ack.message() {
                        anyhow::bail!("IPC writer error: {message}")
                    }
                }
            }
            tracing::info!("migrate history ACK reader finished");
            Ok(())
        });

        // query database and send to writer
        let mut batch_count: u64 = 1;
        while let Ok(task) = receiver.recv_async().await {
            let start = task
                .begin_datetime
                .ok_or(anyhow::anyhow!("beginDateTime cannot be None"))?;
            let end = task
                .end_datetime
                .ok_or(anyhow::anyhow!("endDateTime cannot be None"))?;

            // query
            tracing::debug!(
                "migrate history batch:{}, execute query from: {}, to: {}",
                batch_count,
                start,
                end
            );
            let mut rows = self
                .query
                .select_from_history(task.tags, start, end)
                .await?
                .into_row_stream();

            let batch_size = task.advanced_options.batch_size.unwrap_or(10000);
            let mut row_count = 0;
            while let Some(row) = rows.try_next().await? {
                let raw_data = appender.append_history_row(&row).map_err(|err| {
                    let err_msg = format!(
                        "migrate history batch: {}, append row error: {}",
                        batch_count,
                        err.to_string()
                    );
                    tracing::error!(err_msg);
                    anyhow::anyhow!(err_msg)
                })?;

                let _ = logger_tx.send_async(raw_data.to_string()).await;

                row_count += 1;

                if row_count == batch_size {
                    let batch = appender.finish()?;
                    tracing::debug!(
                        "migrate history batch: {}, send batch records to writer",
                        batch_count
                    );
                    tx.send_async(batch.clone()).await?;
                    batch_count += 1;

                    row_count = 0;
                }
            }

            let batch = appender.finish()?;
            tracing::debug!(
                "migrate history batch: {}, send batch records to writer",
                batch_count
            );
            tx.send_async(batch.clone()).await?;
            batch_count += 1;
        }
        drop(tx);

        tracing::debug!("migrate history query finished");
        writer_handler.await??;
        tracing::debug!("migrate history writer finished");
        ack.await??;
        tracing::debug!("migrate history consumer finished");
        Ok(())
    }
}
