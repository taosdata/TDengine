use arrow::array::RecordBatch;
use flume::Receiver;
use futures::sink;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    errors::ParquetError,
    file::properties::WriterProperties,
};
use rotate_file::{utils::time_unit_dt_fmt, RotateFileError, RotateWriterBuilder, SinkFn};
use std::{fs::OpenOptions, marker::PhantomData, path::PathBuf};
use tokio::sync::oneshot;

use crate::{Archive, Cache, ARCHIVE_PREFIX, CACHE_PREFIX};

#[derive(Debug)]
pub struct RewriteMsg {
    pub resp_tx: oneshot::Sender<Result<Vec<PathBuf>, ArchiveError>>,
}

#[derive(Debug)]
pub enum ArchiveType {
    Cache(RecordBatch),
    Archive(RecordBatch),
    CacheRewrite(RewriteMsg),
}

pub struct ArchiveConsumer<F, T>
where
    F: Fn(T) -> anyhow::Result<()>,
{
    task_id: i64,
    cache: Cache,
    archive: Archive,
    update_metrics: F,
    _phantom_data: PhantomData<T>,
}

impl<F, T> ArchiveConsumer<F, T>
where
    T: From<u64>,
    F: Fn(T) -> anyhow::Result<()>,
{
    pub fn new(task_id: i64, cache: Cache, archive: Archive, update_metrics: F) -> Self {
        Self {
            task_id,
            cache,
            archive,
            update_metrics,
            _phantom_data: PhantomData,
        }
    }

    pub async fn consume(&mut self, receiver: Receiver<ArchiveType>) -> Result<(), ArchiveError> {
        tracing::debug!(
            "start the 'cache & archive' thread, task id: {}, cache: {:?}, archive: {:?}",
            self.task_id,
            self.cache,
            self.archive
        );

        let cache_writer = RotateWriterBuilder::new()
            .id(self.task_id)
            .dir(self.cache.location.to_string())
            .prefix(
                self.cache
                    .prefix
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or(CACHE_PREFIX.to_string()),
            )
            .file_dt_fmt(
                time_unit_dt_fmt(&self.cache.keep_days_unit)
                    .map_err(ArchiveError::TimeUnitError)?,
            )
            .rotate_count(self.cache.rotate_count)
            .max_size_value(self.cache.max_size_value)
            .max_size_unit(self.cache.max_size_unit.to_string())
            .keep_time_value(self.cache.keep_days_value)
            .keep_time_unit(self.cache.keep_days_unit.to_string())
            .gen_sink(Box::new(
                |file_path: PathBuf| -> Result<SinkFn<RecordBatch, ParquetError>, anyhow::Error> {
                    let sink = sink::unfold(
                        file_path,
                        |file_path: PathBuf, record: RecordBatch| async move {
                            let file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(&file_path)?;
                            let schema = record.schema();
                            let props = WriterProperties::builder()
                                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                                .build();
                            let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
                            writer.write(&record)?;
                            writer.close()?;
                            Ok(file_path)
                        },
                    );
                    Ok(Box::pin(sink))
                },
            ))
            .build()
            .map_err(ArchiveError::BuildRotateFileError)?;
        let archive_writer = RotateWriterBuilder::new()
            .id(self.task_id)
            .dir(self.archive.location.to_string())
            .prefix(
                self.archive
                    .prefix
                    .as_ref()
                    .map(|x| x.to_string())
                    .unwrap_or(ARCHIVE_PREFIX.to_string()),
            )
            .file_dt_fmt(
                time_unit_dt_fmt(&self.archive.keep_days_unit)
                    .map_err(ArchiveError::TimeUnitError)?,
            )
            .rotate_count(self.archive.rotate_count)
            .max_size_value(self.archive.max_size_value)
            .max_size_unit(self.archive.max_size_unit.to_string())
            .keep_time_value(self.archive.keep_days_value)
            .keep_time_unit(self.archive.keep_days_unit.to_string())
            .gen_sink(Box::new(
                |file_path: PathBuf| -> Result<SinkFn<RecordBatch, ParquetError>, anyhow::Error> {
                    let sink = sink::unfold(
                        file_path,
                        |file_path: PathBuf, record: RecordBatch| async move {
                            let file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(&file_path)?;
                            let schema = record.schema();
                            let props = WriterProperties::builder()
                                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                                .build();
                            let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
                            writer.write(&record)?;
                            writer.close()?;
                            Ok(file_path)
                        },
                    );
                    Ok(Box::pin(sink))
                },
            ))
            .build()
            .map_err(ArchiveError::BuildRotateFileError)?;
        // get metrics
        let update_metrics = &self.update_metrics;

        let id = self.task_id;
        while let Ok(archive_type) = receiver.recv_async().await {
            match archive_type {
                ArchiveType::Cache(batch) => {
                    let num_rows = batch.num_rows();
                    match cache_writer.write(batch).await {
                        Ok(_) => {
                            tracing::debug!("Task {} cache records success, {} rows", id, num_rows);
                        }
                        Err(e) => match self.cache.on_fail.handle(format!("{e:#}")) {
                            Ok(_) => {}
                            Err(e) => return Err(ArchiveError::HandleCacheFailError(e)),
                        },
                    }
                }
                ArchiveType::Archive(batch) => {
                    let num_rows = batch.num_rows();
                    match archive_writer.write(batch.clone()).await {
                        Ok(_) => {
                            if let Err(e) = update_metrics((num_rows as u64).into())
                                .map_err(ArchiveError::UpdateMetricsError)
                            {
                                tracing::error!("Task {id} update metrics error: {:?}", e);
                            }
                            tracing::debug!(
                                "Task {} archive records success, {} rows",
                                id,
                                num_rows
                            );
                        }
                        Err(e) => match self.archive.on_fail.handle(format!("{e:#}")) {
                            Ok(retry) if retry => {
                                if let Err(e) = archive_writer.force_rotate().await {
                                    tracing::error!(
                                        "Task {id} rotate archive file failed, err: {e:#}"
                                    );
                                }
                                if let Err(e) = archive_writer.write(batch).await {
                                    tracing::error!(
                                        "Task {id} retry archive records failed, {} rows, err: {e:#}",
                                        num_rows
                                    );
                                }
                            }
                            Ok(_) => {
                                unreachable!()
                            }
                            Err(e) => return Err(ArchiveError::HandleArchiveFailError(e)),
                        },
                    }
                }
                ArchiveType::CacheRewrite(rewrite_msg) => {
                    let RewriteMsg { resp_tx } = rewrite_msg;
                    let files = cache_writer
                        .snapshot()
                        .await
                        .map_err(ArchiveError::CacheRewriteError);
                    if let Err(e) = resp_tx.send(files) {
                        tracing::error!("Task {id} send cache rewrite files error: {:?}", e);
                    }
                }
            }
        }
        cache_writer
            .close()
            .await
            .map_err(ArchiveError::CacheWriterCloseError)?;
        archive_writer
            .close()
            .await
            .map_err(ArchiveError::ArchiveWriterCloseError)?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("Build rotate file error: {0}")]
    BuildRotateFileError(RotateFileError),
    #[error("Cache writer close error: {0}")]
    CacheWriterCloseError(RotateFileError),
    #[error("Archive writer close error: {0}")]
    ArchiveWriterCloseError(RotateFileError),
    #[error("Handle cache failed: {0}")]
    HandleCacheFailError(anyhow::Error),
    #[error("Handle archive failed: {0}")]
    HandleArchiveFailError(anyhow::Error),
    #[error("Update metrics error: {0}")]
    UpdateMetricsError(anyhow::Error),
    #[error("Cache rewrite error: {0}")]
    CacheRewriteError(RotateFileError),
    #[error("Oneshot send error: {0}")]
    OneshotSendError(String),
    #[error("Oneshot recv error: {0}")]
    OneshotRecvError(String),
    #[error("Time unit error: {0}")]
    TimeUnitError(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use crate::{get_rewrite_files, Archive, ArchiveConsumer, ArchiveType, Cache};
    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field},
    };
    use std::sync::Arc;

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_consumer() {
        let task_id = 1;
        let cache = Cache {
            location: "/tmp/taosx/cache".to_string(),
            keep_days: "10m".to_string(),
            keep_days_value: 10,
            keep_days_unit: "m".to_string(),
            max_size: "1MB".to_string(),
            max_size_value: 1,
            max_size_unit: "MB".to_string(),
            ..Default::default()
        };
        let archive = Archive {
            location: "/tmp/taosx/cache".to_string(),
            keep_days: "10m".to_string(),
            keep_days_value: 10,
            keep_days_unit: "m".to_string(),
            max_size: "1MB".to_string(),
            max_size_value: 1,
            max_size_unit: "MB".to_string(),
            ..Default::default()
        };

        let (archive_tx, archive_rx) = flume::bounded(10);

        tokio::spawn(async move {
            let _ = ArchiveConsumer::new(task_id, cache, archive, |num_rows: u64| {
                println!("exec update metrics: {}", num_rows);
                Ok::<_, anyhow::Error>(())
            })
            .consume(archive_rx)
            .await;
        });

        tokio::spawn({
            let archive_tx = archive_tx.clone();
            async move {
                loop {
                    let batch = RecordBatch::try_new(
                        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                            "ts",
                            DataType::Int64,
                            true,
                        )])),
                        vec![Arc::new(Int64Array::from(vec![
                            1750867200, 1750867201, 1750867202, 1750867203,
                        ]))],
                    )
                    .unwrap();

                    if let Err(e) = archive_tx.send_async(ArchiveType::Cache(batch)).await {
                        println!("archive consumer write batch error: {e:?}")
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        });
        // send cache rewrite msg
        loop {
            let rewrite_files = get_rewrite_files(&archive_tx).await;
            println!("get rewrite files: {:?}", rewrite_files);
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
        }
    }
}
