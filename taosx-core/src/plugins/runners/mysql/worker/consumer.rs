use arrow::ipc::writer::StreamWriter;
use arrow_schema::Schema;
use chrono::Utc;
use flume::Receiver;
use futures::StreamExt;

use taosx_ipc::ack::AckReaderBuilder;

use crate::runners::mysql::appender;
use crate::runners::mysql::config::MySqlConfig;
use crate::runners::mysql::query::MySqlQuery;
use crate::runners::mysql::worker::set_breakpoint;
use crate::runners::set_tcp_keepalive;

pub struct Consumer {
    config: MySqlConfig,
    schema: Schema,
}

impl Consumer {
    pub fn new(config: MySqlConfig, schema: Schema) -> Self {
        Self { config, schema }
    }

    pub async fn consume(&mut self, receiver: Receiver<MySqlConfig>) -> anyhow::Result<()> {
        // connect to database
        let mut query = MySqlQuery::try_new(
            self.config.connect.clone(),
            self.config.task.time_zone.clone(),
        )
        .await?;

        // IPC Tcp stream
        let socket = format!("127.0.0.1:{}", &self.config.ipc_port.unwrap_or(0));
        let stream = std::net::TcpStream::connect(socket)?;
        set_tcp_keepalive(&stream)?;
        stream.set_nonblocking(false)?;

        // ack reader stream
        let ack_stream = stream.try_clone()?;
        set_tcp_keepalive(&ack_stream)?;
        ack_stream.set_read_timeout(None)?;

        let schema = self.schema.clone();

        // write batch to IPC
        let (tx, rx) = flume::bounded(100);
        let writer_handler = tokio::task::spawn_blocking(move || {
            // IPC writer
            let mut writer = StreamWriter::try_new(stream, &schema)?;
            // stastics
            let mut row_count = 0;
            let mut batch_count = 0;

            while let Ok(batch) = rx.recv() {
                writer.write(&batch)?;
                tracing::debug!("migrate mysql write {} rows to ipc", batch.num_rows());
                row_count += batch.num_rows();
                batch_count += 1;
            }

            tracing::debug!(
                send.batches = batch_count,
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
                    tracing::error!("migrate mysql write records error: {ack:?}",);
                    if let Some(message) = ack.message() {
                        anyhow::bail!("IPC writer error: {message}")
                    }
                }
            }
            tracing::info!("migrate mysql ACK reader finished");
            Ok(())
        });

        // query database and send to writer
        let mut batch_count: u64 = 0;
        while let Ok(mut config) = receiver.recv_async().await {
            let end = config.task.end.unwrap_or_else(Utc::now);
            let sql = config.task.generate_sql()?;
            let batch_size = config.advanced.batch_size.unwrap_or(10000);

            // set sub task id
            config.sub_task_id = self.config.sub_task_id.clone();

            tracing::debug!("consume task, config:{:?}, sql:{:?}", &config, &sql);

            let mut stream = query.select_by_stream(&sql);
            let mut rows = Vec::new();
            while let Some(result) = stream.next().await {
                match result {
                    Ok(row) => {
                        rows.push(row);
                    }
                    Err(e) => {
                        tracing::warn!("migrate mysql query error: {e:?}",);
                        // anyhow::bail!("migrate mysql query error: {e}")
                    }
                }
                if rows.len() >= batch_size {
                    // copy rows
                    let rows_cloned = rows.splice(.., Vec::new()).collect::<Vec<_>>();
                    // transform to record batch
                    let batch =
                        appender::to_record_batch(rows_cloned, self.config.task.time_zone.clone())
                            .await?;
                    // send to IPC
                    tx.send_async(batch.clone()).await?;
                    // clear rows
                    rows.clear();
                    // stastics
                    batch_count += 1;
                }
            }
            if !rows.is_empty() {
                // transform to record batch
                let batch =
                    appender::to_record_batch(rows, self.config.task.time_zone.clone()).await?;
                // send to IPC
                tx.send_async(batch.clone()).await?;
                // stastics
                batch_count += 1;
            }
            // set breakpoint
            set_breakpoint(&config, &end).await?;
        }
        drop(tx);

        tracing::debug!("migrate mysql query finished, total batch: {}", batch_count);
        writer_handler.await??;
        tracing::debug!(
            "migrate mysql writer finished, total batch: {}",
            batch_count
        );
        ack.await??;
        tracing::debug!(
            "migrate mysql consumer finished, total batch: {}",
            batch_count
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::runners::mysql::worker::producer::Producer;

    use super::*;
    use std::str::FromStr;
    use taos::Dsn;
    use tests::appender::to_schema;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_consumer() {
        // config
        let dsn = Dsn::from_str("mysql://root:123456@192.168.1.40:3306/test_taosx?sql=select * from t_metric&start=2021-01-01T00:00:00Z&end=2021-02-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let mut config = MySqlConfig::from_dsn(&dsn).unwrap();
        config.task_id = Some(1);
        config.sub_task_id = Some(format!("mig-1"));
        config.ipc_port = Some(6666);

        // query for schema
        let mut query = MySqlQuery::try_new(config.connect.clone(), config.task.time_zone.clone())
            .await
            .unwrap();
        let row = query
            .select_one_for_schema("select * from t_metric")
            .await
            .unwrap();
        let schema = match row {
            Some(row) => to_schema(row).await.unwrap(),
            None => return,
        };

        // ipc
        let (tx, rx) = flume::bounded(4);

        // consumer
        let config_clone = config.clone();
        let consumer =
            tokio::spawn(async move { Consumer::new(config_clone, schema).consume(rx).await });

        // produce task
        let producer = Producer::new(&config);
        producer.produce(tx).await.unwrap();

        // wait for consumer
        let _ = consumer.await;
        // match result {
        //     Ok(r) => match r {
        //         Ok(_) => {
        //             tracing::info!("test_consumer finished");
        //         }
        //         Err(e) => {
        //             tracing::error!("test_consumer error: {e:?}",);
        //             panic!("test_consumer error: {e}")
        //         }
        //     },
        //     Err(e) => {
        //         tracing::error!("test_consumer error: {e:?}",);
        //         panic!("test_consumer error: {e}")
        //     }
        // }
    }
}
