mod config;

use anyhow::Context;
use futures::StreamExt;
use orc_rust::{ArrowReaderBuilder, projection::ProjectionMask};
use taos::{
    AsyncTBuilder, Dsn,
    tokio::{self, task::JoinSet},
};
use tokio_util::sync::CancellationToken;

use taosx_core::{Parser, TaskNotifySender, sink::channel_based_transformer};

use crate::config::{Config, Projection};

pub async fn orc_to_taos(
    from: Dsn,
    parser: Option<Parser>,
    to: Dsn,
    task_id: Option<i64>,
    cancel: CancellationToken,
    notifier: TaskNotifySender,
) -> anyhow::Result<()> {
    tracing::info!("ORC to taos, from: {from}, to: {to}");

    let task_cancel = cancel.child_token();

    let config: Config = from.try_into()?;

    let pool = taos::TaosBuilder::from_dsn(to)
        .context("taos builder from `to` dsn error")?
        .pool()
        .context("get taos pool error")?;
    let (sender, ack) = channel_based_transformer(
        pool,
        task_cancel.child_token(),
        parser,
        Some("orc"),
        task_id,
        notifier,
        config.unprocessed_batches.unwrap_or(64),
    )
    .await?;

    let mut tasks = JoinSet::new();

    // ack
    tasks.spawn({
        let cancel = task_cancel.clone();
        async move {
            let _guard = cancel.clone().drop_guard();
            while let Some(Ok(ack)) = cancel.run_until_cancelled(ack.recv_async()).await {
                if ack.success() {
                    continue;
                }
                if let Some(msg) = ack.message() {
                    tracing::error!("receive failed ack: {msg}");
                } else {
                    tracing::error!("receive failed ack")
                }
            }
            anyhow::Ok(())
        }
    });

    // reader
    for path in config.paths {
        let projection = config.projection.clone();
        tasks.spawn({
            let cancel = task_cancel.clone();
            let sender = sender.clone();
            async move {
                let file = tokio::fs::File::open(path)
                    .await
                    .context("open orc file error")?;
                let mut builder = ArrowReaderBuilder::try_new_async(file)
                    .await
                    .context("build orc async reader error")?;
                builder = builder.with_batch_size(config.batch_size);

                let root_data_type = builder.file_metadata().root_data_type();
                let projection = match projection {
                    Some(Projection::Indices(indices)) => {
                        ProjectionMask::roots(root_data_type, indices)
                    }
                    Some(Projection::Names(names)) => {
                        ProjectionMask::named_roots(root_data_type, &names)
                    }
                    None => ProjectionMask::all(),
                };
                builder = builder.with_projection(projection);
                let mut reader = builder.build_async();
                while let Some(batch) = cancel.run_until_cancelled(reader.next()).await.flatten() {
                    if sender.send_async(batch).await.is_err() {
                        break;
                    }
                }

                Ok(())
            }
        });
    }
    drop(sender);

    let mut has_error = false;
    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::error!("orc task exit with error: {e:#}");
                has_error = true;
            }
            Err(e) => {
                tracing::error!("orc task panicked: {e}");
                has_error = true;
            }
        }
    }
    if has_error {
        anyhow::bail!("task exit with error, waiting to restart");
    }

    Ok(())
}
