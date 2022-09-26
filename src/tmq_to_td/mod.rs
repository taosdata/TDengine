use std::time::Duration;

use anyhow::{bail, Context, Result};
use taos::{Consumer, *};

use crate::{
    tmq::{check_tmq_dsn, group_id_hash},
    Action,
};

async fn sync(
    id: usize,
    consumer: Consumer,
    taos: Taos,
    table: Option<String>,
    actions: Vec<Action>,
) -> Result<()> {
    let mut stream = consumer.stream();
    let mut rows = 0;

    while let Some((offset, message)) = stream.try_next().await? {
        match message {
            MessageSet::Meta(meta) => {
                // log::debug!("[{id}] meta: {}", meta.as_json_meta().await?);
                if actions.is_empty() {
                    if let Err(err) = taos.write_raw_meta(meta.as_raw_meta().await?).await {
                        let errstr = err.to_string();
                        if errstr.contains("[0x032C]") {
                            log::warn!("there's a same object is creating and expected to be done in some time, so we'll continue");
                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                        } else if errstr.contains("[0x03C7]") {
                            log::warn!("write raw meta error with stable, but we'll continue");
                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                        } else {
                            Err(err).context("write raw meta error")?;
                        }
                    }
                } else {
                    let mut meta = meta.as_json_meta().await?;
                    // dbg!(&meta);

                    for action in &actions {
                        action.mutate_meta(&mut meta)?;
                    }
                    dbg!(&meta);
                    let sql = meta.to_string();
                    if let Err(err) = taos.exec(&sql).await {
                        let errstr = err.to_string();
                        if errstr.contains("[0x032C]") {
                            log::warn!("there's a same object is creating and expected to be done in some time, so we'll continue");
                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                        } else if errstr.contains("[0x03C7]") {
                            log::warn!("write raw meta error with stable, but we'll continue");
                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                        } else {
                            Err(err).context("write raw meta error")?;
                        }
                    }
                }
            }
            MessageSet::Data(data) => {
                while let Some(mut raw) = data.fetch_raw_block().await? {
                    if let Some(name) = table.as_ref() {
                        if actions.is_empty() {
                            raw.with_table_name(name);
                            log::debug!(
                                "[{id}] write into {name} {} rows(total {}) with {} columns",
                                raw.nrows(),
                                rows,
                                raw.ncols()
                            );
                        } else {
                            let mut name = name.to_string();
                            for action in &actions {
                                match action {
                                    Action::RenameTable(rename)
                                    | Action::RenameChildTable(rename) => {
                                        rename.apply_in_place(&mut name)
                                    }
                                    _ => (),
                                }
                            }
                            raw.with_table_name(&name);
                            log::debug!(
                                "[{id}] write into {name} {} rows(total {}) with {} columns",
                                raw.nrows(),
                                rows,
                                raw.ncols()
                            );
                        }
                    } else if let Some(name) = raw.table_name().as_deref() {
                        if !actions.is_empty() {
                            let mut name = name.to_string();
                            for action in &actions {
                                match action {
                                    Action::RenameTable(rename)
                                    | Action::RenameChildTable(rename) => {
                                        rename.apply_in_place(&mut name)
                                    }
                                    _ => (),
                                }
                            }
                            raw.with_table_name(&name);
                            log::debug!(
                                "[{id}] write into {name} {} rows(total {}) with {} columns",
                                raw.nrows(),
                                rows,
                                raw.ncols()
                            );
                        }
                    } else {
                        log::debug!(
                            "[{id}] write {} rows(total {}) with {} columns",
                            raw.nrows(),
                            rows,
                            raw.ncols()
                        );
                    }
                    rows += raw.nrows();
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
            MessageSet::MetaData(meta, data) => {
                // log::debug!("[{id}] meta: {}", meta.as_json_meta().await?);
                if actions.is_empty() {
                    if let Err(err) = taos.write_raw_meta(meta.as_raw_meta().await?).await {
                        let errstr = err.to_string();
                        if errstr.contains("[0x032C]") {
                            log::warn!("there's a same object is creating and expected to be done in some time, so we'll continue");
                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                        } else if errstr.contains("[0x03C7]") {
                            log::warn!("write raw meta error with stable, but we'll continue");
                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                        } else {
                            Err(err).context("write raw meta error")?;
                        }
                        continue;
                    }
                } else {
                    let mut meta = meta.as_json_meta().await?;

                    for action in &actions {
                        action.mutate_meta(&mut meta)?;
                    }
                    let sql = meta.to_string();
                    if let Err(err) = taos.exec(&sql).await {
                        let errstr = err.to_string();
                        if errstr.contains("[0x032C]") {
                            log::warn!("there's a same object is creating and expected to be done in some time, so we'll continue");
                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                        } else if errstr.contains("[0x03C7]") {
                            log::warn!("write raw meta error with stable, but we'll continue");
                            // tokio::time::sleep(Duration::from_nanos(1000)).await;
                        } else {
                            Err(err).context("write raw meta error")?;
                        }
                    }
                }

                while let Some(mut raw) = data.fetch_raw_block().await? {
                    if let Some(name) = table.as_ref() {
                        if actions.is_empty() {
                            raw.with_table_name(name);
                        } else {
                            let mut name = name.to_string();
                            for action in &actions {
                                match action {
                                    Action::RenameTable(rename) => rename.apply_in_place(&mut name),
                                    Action::RenameChildTable(rename) => {
                                        rename.apply_in_place(&mut name)
                                    }
                                    _ => (),
                                }
                            }
                            raw.with_table_name(name);
                        }
                    } else if let Some(name) = raw.table_name().as_deref() {
                        if !actions.is_empty() {
                            let mut name = name.to_string();
                            for action in &actions {
                                match action {
                                    Action::RenameTable(rename)
                                    | Action::RenameChildTable(rename) => {
                                        rename.apply_in_place(&mut name)
                                    }
                                    _ => (),
                                }
                            }
                            raw.with_table_name(&name);
                            log::debug!(
                                "[{id}] write into {name} {} rows(total {}) with {} columns",
                                raw.nrows(),
                                rows,
                                raw.ncols()
                            );
                        }
                    } else {
                        log::debug!(
                            "[{id}] write {} rows(total {}) with {} columns",
                            raw.nrows(),
                            rows,
                            raw.ncols()
                        );
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

pub async fn tmq_to_td(from: Dsn, actions: Vec<Action>, mut to: Dsn, jobs: usize) -> Result<()> {
    let (mut from, topics) = check_tmq_dsn(from).await?;

    if let Some(database) = to.subject.take() {
        let taos = TaosBuilder::from_dsn(&to)?.build()?;
        if !taos.database_exists(&database).await? {
            log::warn!(
                "Target database name `{database}` does not exist, create it with default option"
            );
            taos.exec(format!("create database if not exists `{database}`"))
                .await?;
        }
        to.subject = Some(database);
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
    let target = TaosBuilder::from_dsn(&to)?;
    for topic in topics {
        let jobs = if jobs == 0 || jobs >= topic.vgroups {
            topic.vgroups
        } else {
            jobs
        };
        if let Some(table) = topic.table.as_ref() {
            // schema rebuild
            let taos = target.build()?;
            if to.subject.is_none() {
                taos.exec(format!("use {}", topic.database)).await?;
            }

            if let Some(sql) = table.stable_sql.as_deref() {
                let mut sql = sql.replace("CREATE STABLE", "CREATE STABLE IF NOT EXISTS");

                for action in &actions {
                    match action {
                        Action::Select(_) => {
                            bail!("unsupported transform action: {:?}", action)
                        }
                        Action::AddTag(action) => {
                            let len = match action.len {
                                0 => 100,
                                16374.. => 16374,
                                a => a,
                            };
                            sql.pop();
                            sql.push_str(&format!(", `{}` VARCHAR({}))", action.name, len));
                        }
                        Action::RenameTable(action) => {
                            let name = table.stable.as_deref().unwrap();
                            let new = sql.replace(&format!("`{name}`",), &action.apply(name));
                            sql.clear();
                            sql.extend(new.chars());
                        }
                        Action::RenameSuperTable(action) => {
                            let name = table.stable.as_deref().unwrap();
                            let new = sql.replace(&format!("`{name}`",), &action.apply(name));
                            sql.clear();
                            sql.extend(new.chars());
                        }
                        _ => (),
                    }
                }
                taos.exec(sql).await?;
            }
            let mut sql = table
                .table_sql
                .replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

            for action in &actions {
                match action {
                    Action::Select(_) => {
                        bail!("unsupported transform action: {:?}", action)
                    }
                    Action::RenameTable(action) => {
                        let name = table.stable.as_deref().unwrap();
                        let new = sql.replace(&format!("`{name}`",), &action.apply(name));
                        sql.clear();
                        sql.extend(new.chars());
                    }
                    _ => (),
                }
            }
            taos.exec(sql).await?;
        }
        // dbg!(&topic);

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
            let table = topic.table.as_ref().map(|t| t.table.clone());
            let actions = actions.to_vec();
            let handle =
                tokio::spawn(async move { sync(task_id, consumer, taos, table, actions).await });
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
