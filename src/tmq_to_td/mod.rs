use std::time::Duration;

use anyhow::{Context, Result};
use taos::{Consumer, *};

use crate::tmq::{check_tmq_dsn, group_id_hash};

async fn sync(id: usize, consumer: Consumer, taos: Taos, table: Option<String>) -> Result<()> {
    let mut stream = consumer.stream();
    let mut rows = 0;

    while let Some((offset, message)) = stream.try_next().await? {
        match message {
            MessageSet::Meta(meta) => {
                log::debug!("[{id}] meta: {}", meta.as_json_meta().await?);
                if let Err(err) = taos.write_raw_meta(meta.as_raw_meta().await?).await {
                    if err.to_string().contains("[0x032C") {
                        tokio::time::sleep(Duration::from_nanos(1000)).await;
                    } else {
                        Err(err).context("write raw meta error")?;
                    }
                }
            }
            MessageSet::Data(data) => {
                while let Some(mut raw) = data.fetch_raw_block().await? {
                    if let Some(name) = table.as_ref() {
                        raw.with_table_name(name);
                    }
                    rows += raw.nrows();
                    log::debug!(
                        "[{id}] write {} rows(total {}) with {} columns",
                        raw.nrows(),
                        rows,
                        raw.ncols()
                    );
                    if let Err(err) = taos.write_raw_block(&raw).await {
                        if err.to_string().contains("[0x2603]") {
                            // table not exists
                            if let Some(meta) = raw.to_create() {
                                if let Err(err) = taos.exec(format!("{}", meta)).await {
                                    if err.to_string().contains("0x032C") {
                                        tokio::time::sleep(Duration::from_nanos(1000)).await;
                                    } else {
                                        Err(err).context("create table error")?;
                                    }
                                };
                                taos.write_raw_block(&raw)
                                    .await
                                    .context("write table data failed")?;
                            } else {
                                Err(err).context("write table failed")?;
                            }
                        } else {
                            Err(err).context("write table failed")?;
                        }
                    };
                }
            }
        }
        consumer.commit(offset).await?;
    }
    Ok(())
}

pub async fn tmq_to_td(from: Dsn, mut to: Dsn, jobs: usize) -> Result<()> {
    let (mut from, topics) = check_tmq_dsn(from).await?;

    if let Some(database) = to.database.take() {
        let taos = TaosBuilder::from_dsn(&to)?.build()?;
        if !taos.database_exists(&database).await? {
            log::warn!(
                "Target database name `{database}` does not exist, create it with default option"
            );
            taos.exec(format!("create database if not exists `{database}`"))
                .await?;
        }
        to.database = Some(database);
    } else {
        anyhow::bail!("Database not specified in DSN: {}", to);
    }

    // auto generate group.id if not exists
    let mut from_params = from.drain_params();
    if from_params.get("group.id").is_none() {
        let to_params = to.drain_params();
        let group_id = group_id_hash(&from, &to);
        log::info!(
            "group.id not set, will use automatically generated group id: {}",
            group_id
        );
        from_params.insert("group.id".to_string(), group_id);
        to.params = to_params;
    }
    from.params = from_params;

    let mut handles = Vec::new();
    let mut task_id = 0;
    let target = TaosBuilder::from_dsn(to)?;
    for topic in topics {
        let jobs = if jobs == 0 || jobs >= topic.vgroups {
            topic.vgroups
        } else {
            jobs
        };
        dbg!(&topic);

        // let mut from = from.clone();
        let tmq = TmqBuilder::from_dsn(&from)?;

        let mut consumers = Vec::with_capacity(jobs);
        for _ in 0..jobs {
            let mut consumer = tmq.build()?;
            consumer.subscribe([&topic.name]).await?;
            consumers.push(consumer);
        }

        for _ in 0..jobs {
            let consumer = consumers.pop().unwrap();
            let taos = target.build()?;
            let table = topic.table.clone();
            let handle = tokio::spawn(async move { sync(task_id, consumer, taos, table).await });
            handles.push(handle);
            task_id += 1;
        }
    }
    for handle in handles {
        handle.await??;
    }

    // let tmq = TmqBuilder::from_dsn(&from)?;
    // let target = TaosBuilder::from_dsn(to)?;

    // let mut consumers = Vec::with_capacity(jobs);
    // for _id in 0..jobs {
    //     let mut consumer = tmq.build()?;
    //     consumer.subscribe(&topics).await?;
    //     consumers.push(consumer);
    // }

    // log::info!("created {jobs} consumers");

    // let mut handles = Vec::new();
    // for id in 0..jobs {
    //     let consumer = consumers.pop().unwrap();

    //     let taos = target.build()?;
    //     let handle = tokio::spawn(async move { sync(id, consumer, taos).await });
    //     handles.push(handle);
    // }
    // for handle in handles {
    //     handle.await??;
    // }
    Ok(())
}
