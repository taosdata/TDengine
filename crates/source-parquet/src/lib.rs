mod config;

use anyhow::Context;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use taos::{
    AsyncTBuilder, Dsn,
    tokio::{self, task::JoinSet},
};
use tokio_util::sync::CancellationToken;

use taosx_core::{Parser, TaskNotifySender, sink::channel_based_transformer};

use crate::config::{Config, Projection};

pub async fn parquet_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    task_id: Option<i64>,
    cancel: CancellationToken,
    notifier: TaskNotifySender,
) -> anyhow::Result<()> {
    tracing::info!("Parquet to taos, from: {from}, to: {to}");

    let task_cancel = cancel.child_token();

    let config: Config = from.try_into()?;

    let pool = taos::TaosBuilder::from_dsn(to)
        .context("taos builder from `to` dsn error")?
        .pool()
        .context("get taos pool error")?;
    let (sender, ack) = channel_based_transformer(
        pool,
        &cancel,
        parser,
        Some("parquet"),
        task_id,
        notifier,
        config.unprocessed_batches.unwrap_or(64),
    )
    .await?;

    let mut tasks = JoinSet::new();

    // ack
    tasks.spawn({
        let cancel = task_cancel.child_token();
        async move {
            let _guard = cancel.clone().drop_guard();
            let mut count = 0;
            while let Some(Ok(ack)) = cancel.run_until_cancelled(ack.recv_async()).await {
                if ack.success() {
                    count += 1;
                    if count % 1000 == 0 {
                        tracing::info!("receive successful ack {:?}", ack);
                    }
                    continue;
                }
                if let Some(msg) = ack.message() {
                    tracing::error!("receive failed ack: {msg}");
                } else {
                    tracing::error!("receive failed ack")
                }
            }
            tracing::info!("ack task exiting");
            anyhow::Ok(())
        }
    });

    // reader
    for path in config.paths {
        let projection = config.projection.clone();
        let batch_size = config.batch_size;
        tasks.spawn({
            let cancel = task_cancel.child_token();
            let sender = sender.clone();
            async move {
                // Use tokio::task::spawn_blocking for synchronous I/O
                tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    let file = std::fs::File::open(&path)
                        .context(format!("open parquet file error: {}", path))?;

                    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                        .context("build parquet reader error")?;

                    let parquet_schema = builder.parquet_schema();
                    let arrow_schema = builder.schema();

                    // Apply projection if specified
                    let builder = if let Some(proj) = &projection.as_deref() {
                        match proj {
                            Projection::Indices(indices) => {
                                let mask =
                                    ProjectionMask::roots(parquet_schema, indices.iter().cloned());
                                builder.with_projection(mask)
                            }
                            Projection::Names(names) => {
                                // Find column indices by name
                                let indices: Vec<usize> = names
                                    .iter()
                                    .filter_map(|name| {
                                        arrow_schema.fields().iter().position(|f| f.name() == name)
                                    })
                                    .collect();
                                let mask = ProjectionMask::roots(parquet_schema, indices);
                                builder.with_projection(mask)
                            }
                        }
                    } else {
                        builder
                    };

                    let builder = builder.with_batch_size(batch_size);
                    let reader = builder
                        .build()
                        .context("build parquet batch reader error")?;

                    for (index, batch) in reader.enumerate() {
                        let batch =
                            batch.with_context(|| format!("read parquet batch {} error", index))?;
                        if cancel.is_cancelled() {
                            break;
                        }
                        if sender.send(Ok(batch)).is_err() {
                            break;
                        }
                    }
                    tracing::info!("finished reading parquet file: {}", path);
                    Ok(())
                })
                .await
                .context("spawn parquet reader error")?
            }
        });
    }
    drop(sender);

    let mut has_error = false;
    let mut errors = vec![];
    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(Ok(_)) => {
                continue;
            }
            Ok(Err(e)) => {
                tracing::error!("parquet task exit with error: {e:#}");
                has_error = true;
                errors.push(e);
            }
            Err(e) => {
                tracing::error!("parquet task panicked: {e}");
                has_error = true;
                errors.push(anyhow::anyhow!(format!("tokio runtime join error: {e:#}")));
            }
        }
    }

    if has_error {
        return Err(errors
            .into_iter()
            .rev()
            .reduce(|acc, e| acc.context(format!("{e:#}")))
            .context("multiple errors occurred")?);
    }
    tracing::info!("parquet to taos completed successfully");

    Ok(())
}
