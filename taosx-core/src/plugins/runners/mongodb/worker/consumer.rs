use arrow::ipc::writer::StreamWriter;
use arrow_schema::Schema;
use chrono::Utc;
use flume::Receiver;
use std::time::Duration;

use taosx_ipc::ack::AckReaderBuilder;

use crate::runners::mongodb::config::MongoDBConfig;
use crate::runners::mongodb::query::MongoDBQuery;
use crate::runners::mongodb::worker::set_breakpoint;
use crate::runners::set_tcp_keepalive;

pub struct Consumer {
    config: MongoDBConfig,
    schema: Schema,
    query: MongoDBQuery,
}

impl Consumer {
    pub fn new(config: MongoDBConfig, schema: Schema, query: MongoDBQuery) -> Self {
        Self {
            config,
            schema,
            query,
        }
    }

    pub async fn consume(&mut self, receiver: Receiver<MongoDBConfig>) -> anyhow::Result<()> {
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
        let (tx, rx) = flume::bounded(0);
        let (ack_tx, ack_rx) = flume::bounded(100);
        let writer_handler = tokio::task::spawn_blocking(move || {
            // IPC writer
            let mut writer = StreamWriter::try_new(stream, &schema)?;
            // stastics
            let mut row_count = 0;
            let mut batch_count = 0;

            while let Ok(batch) = rx.recv() {
                // if the sending fails, retry 3 times by sleeping 1 second each time
                for i in 1..4 {
                    let write_result = writer.write(&batch);
                    match write_result {
                        Ok(_) => {
                            tracing::debug!(
                                "migrate mongodb, write {} rows to ipc",
                                batch.num_rows()
                            );
                            row_count += batch.num_rows();
                            batch_count += 1;
                            ack_tx.send(batch.num_rows())?;
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "migrate mongodb, failed to write to ipc, cause: {}, retrying {i} times...",
                                e
                            );
                            std::thread::sleep(Duration::from_secs(1));
                        }
                    }
                }
            }
            let finish_result = writer.finish();
            match finish_result {
                Ok(_) => {
                    tracing::debug!(
                        send.batches = batch_count,
                        send.records = row_count,
                        "migrate mongodb, sending finished, waiting for persisting"
                    );
                }
                Err(e) => {
                    tracing::warn!("migrate mongodb, sending finished error, cause: {e:?}",);
                }
            }
            anyhow::Ok(())
        });

        // receive ACK from IPC
        let ack_handler = tokio::task::spawn_blocking(move || {
            let ack_reader =
                AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush).open(&ack_stream);
            for ack in ack_reader {
                let _ = ack_rx.recv();
                if !ack.success() {
                    tracing::error!("migrate mongodb, ipc ack, write records error: {ack:?}");
                }
            }
            tracing::info!("migrate mongodb ACK reader finished");
            anyhow::Ok(())
        });

        // query database and send to writer
        let mut batch_count: u64 = 0;
        while let Ok(mut config) = receiver.recv_async().await {
            let end = config.task.end.unwrap_or_else(Utc::now);
            let database = config.task.generate_database()?;
            let collection = config.task.generate_collection()?;
            let filter = config.task.generate_filter()?;
            let sort = config.task.generate_sort()?;
            let batch_size = config.advanced.batch_size.unwrap_or(10000);

            // set sub task id
            config.sub_task_id = self.config.sub_task_id.clone();

            tracing::debug!(
                "consume task, config:{:?}, filter:{:?}, sort:{:?}",
                &config,
                &filter,
                &sort
            );

            // query database, oom occurs when rows are too large
            // let result = query.select_all_and_to_record_batches(&database, &collection, filter, batch_size).await;

            let run_start = Utc::now().timestamp_millis();
            let result = self
                .query
                .select_all_and_send(&database, &collection, filter, sort, batch_size, tx.clone())
                .await;
            let run_end = Utc::now().timestamp_millis();

            match result {
                Ok(amount) => {
                    tracing::info!(
                        "migrate mongodb sub task finished, total rows: {}, used {} ms.",
                        amount,
                        (run_end - run_start)
                    );
                    batch_count += amount;
                    // set breakpoint
                    set_breakpoint(&config, &end).await?;
                }
                Err(e) => {
                    tracing::error!("migrate mongodb query error: {e:?}");
                }
            }
        }
        drop(tx);
        tracing::debug!(
            "migrate mongodb query finished, total batch: {}",
            batch_count
        );

        writer_handler.await??;
        tracing::debug!(
            "migrate mongodb writer finished, total batch: {}",
            batch_count
        );

        ack_handler.await??;
        tracing::debug!(
            "migrate mongodb consumer finished, total batch: {}",
            batch_count
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::runners::mongodb::{appender::to_schema, worker::producer::Producer};

    use super::*;
    use std::str::FromStr;
    use taos::Dsn;
    // use tests::appender::to_schema;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn test_consumer() {
        // config
        let dsn = Dsn::from_str("mongodb://admin:tbase125!@192.168.1.40:27017?source=admin&database=test_taosx&collection=metrics&sql={\"datetime\":{\"$gte\":${start_datetime},\"$lt\":${end_datetime}}}&start=2024-07-01T00:00:00+00:00&end=2024-08-01T00:00:00+00:00&interval=12h&delay=0&sample_data_limit=4")
            .unwrap();
        let mut config = MongoDBConfig::from_dsn(&dsn).unwrap();
        config.task_id = Some(1);
        config.sub_task_id = Some(format!("mig-1"));
        config.ipc_port = Some(6666);

        // query for schema
        let schema = to_schema().unwrap();

        // ipc
        let (tx, rx) = flume::bounded(4);

        // consumer
        let config_clone = config.clone();
        let mut query = MongoDBQuery::try_new(config.connect.clone()).await.unwrap();
        let consumer = tokio::spawn(async move {
            Consumer::new(config_clone, schema, query.clone())
                .consume(rx)
                .await
        });

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
