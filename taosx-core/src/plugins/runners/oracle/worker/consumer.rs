use arrow::ipc::writer::StreamWriter;
use arrow_schema::Schema;
use chrono::Utc;
use flume::Receiver;

use taosx_ipc::ack::AckReaderBuilder;

use crate::runners::oracle::config::OracleConfig;
use crate::runners::oracle::query::OracleQuery;
use crate::runners::oracle::worker::set_breakpoint;
use crate::runners::set_tcp_keepalive;

pub struct Consumer {
    config: OracleConfig,
    schema: Schema,
}

impl Consumer {
    pub fn new(config: OracleConfig, schema: Schema) -> Self {
        Self { config, schema }
    }

    pub async fn consume(&mut self, receiver: Receiver<OracleConfig>) -> anyhow::Result<()> {
        // connect to database
        let mut query = OracleQuery::try_new(
            self.config.connect.clone(),
            self.config.task.time_zone.clone(),
        )?;

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
                tracing::debug!("migrate oracle write {} rows to ipc", batch.num_rows());
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
                    tracing::error!("migrate oracle write records error: {ack:?}",);
                    if let Some(message) = ack.message() {
                        anyhow::bail!("IPC writer error: {message}")
                    }
                }
            }
            tracing::info!("migrate oracle ACK reader finished");
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

            let result = query.select_all_and_to_record_batches(&sql, batch_size);

            match result {
                Ok(batches) => {
                    for batch in batches {
                        // send to IPC
                        tx.send_async(batch.clone()).await?;
                        // stastics
                        batch_count += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!("migrate oracle query error: {e:?}",);
                    // anyhow::bail!("migrate oracle query error: {e}")
                }
            }

            // let (col_map, _) = query.select_all(&sql)?;

            // let mut batch_rows = Vec::new();

            // for row in rows {
            //     batch_rows.push(row);
            //     if batch_rows.len() % batch_size == 0 {
            //         // copy rows
            //         let rows_cloned = batch_rows.splice(.., Vec::new()).collect::<Vec<_>>();
            //         // transform to record batch
            //         let batch = appender::to_record_batch(col_map.clone(), rows_cloned).await?;
            //         // send to IPC
            //         tx.send_async(batch.clone()).await?;
            //         // clear rows
            //         batch_rows.clear();
            //         // stastics
            //         batch_count += 1;
            //     }
            // }
            // if !rows.is_empty() {
            //     // transform to record batch
            //     let batch = appender::to_record_batch(col_map, rows).await?;
            //     // send to IPC
            //     tx.send_async(batch.clone()).await?;
            //     // stastics
            //     batch_count += 1;
            // }
            // set breakpoint
            set_breakpoint(&config, &end).await?;
        }
        drop(tx);

        tracing::debug!(
            "migrate oracle query finished, total batch: {}",
            batch_count
        );
        writer_handler.await??;
        tracing::debug!(
            "migrate oracle writer finished, total batch: {}",
            batch_count
        );
        ack.await??;
        tracing::debug!(
            "migrate oracle consumer finished, total batch: {}",
            batch_count
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::runners::oracle::{appender::to_schema, worker::producer::Producer};

    use super::*;
    use std::str::FromStr;
    use taos::Dsn;
    // use tests::appender::to_schema;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_consumer() {
        // config
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1?sql=select * from t_metric&start=2021-01-01T00:00:00Z&end=2021-02-01T00:00:00Z&interval=12h&delay=0")
            .unwrap();
        let mut config = OracleConfig::from_dsn(&dsn).unwrap();
        config.task_id = Some(1);
        config.sub_task_id = Some(format!("mig-1"));
        config.ipc_port = Some(6666);

        // query for schema
        let mut query =
            OracleQuery::try_new(config.connect.clone(), config.task.time_zone.clone()).unwrap();
        let col_map = query.select_for_schema("select * from TEST").unwrap();
        let schema = to_schema(col_map.clone()).unwrap();

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
