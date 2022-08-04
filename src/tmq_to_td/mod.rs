use anyhow::Result;
use taos::{tmq::Consumer, *};

use crate::tmq::check_tmq_dsn;

async fn sync(id: usize, consumer: Consumer, taos: Taos) -> Result<()> {
    let mut stream = consumer.stream();
    let mut rows = 0;

    while let Some((offset, message)) = stream.try_next().await? {
        match message {
            MessageSet::Meta(meta) => {
                log::debug!("[{id} meta");
                taos.write_raw_meta(meta.as_raw_meta().await?).await?;
            }
            MessageSet::Data(data) => {
                while let Some(block) = data.fetch_raw_block().await? {
                    rows += block.nrows();
                    log::debug!(
                        "[{id}] write {} rows(total {}) with {} columns",
                        block.nrows(),
                        rows,
                        block.ncols()
                    );
                    taos.write_raw_block(&block).await?;
                }
            }
        }
        // consumer.commit(offset).await?;
    }
    Ok(())
}

pub async fn tmq_to_td(from: Dsn, mut to: Dsn, jobs: usize) -> Result<()> {
    let (from, topics, jobs) = check_tmq_dsn(from, jobs).await?;

    let tmq = TmqBuilder::from_dsn(&from)?;

    if let Some(database) = to.database.take() {
        let taos = TaosBuilder::from_dsn(&to)?.build()?;
        if !taos.database_exists(&database).await? {
            log::warn!(
                "Target database name `{database}` does not exist, create it with default option"
            );
            taos.exec(format!("create database if not exists {database}"))
                .await?;
        }
        to.database = Some(database);
        dbg!(&to);
    } else {
        anyhow::bail!("Database not specified in DSN: {}", to);
    }
    let target = TaosBuilder::from_dsn(to)?;

    let mut consumers = Vec::with_capacity(jobs);
    for id in 0..jobs {
        let mut consumer = tmq.build()?;
        consumer.subscribe(&topics).await?;
        consumers.push(consumer);
    }

    log::info!("created {jobs} consumers");

    let mut handles = Vec::new();
    for id in 0..jobs {
        let consumer = consumers.pop().unwrap();

        let taos = target.build()?;
        let handle = tokio::spawn(async move { sync(id, consumer, taos).await });
        handles.push(handle);
    }
    for handle in handles {
        handle.await??;
    }
    Ok(())
}
