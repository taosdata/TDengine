use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use taos::{Consumer, *};
use tokio_util::sync::CancellationToken;

use crate::{
    tmq::{check_tmq_dsn, group_id_hash, TmqMetrics},
    Action,
};

async fn sync(
    id: usize,
    consumer: Consumer,
    taos: Taos,
    table: Option<String>,
    actions: Vec<Action>,
    cancel: CancellationToken,
    metrics: Arc<TmqMetrics>,
) -> Result<()> {
    let mut stream = consumer.stream();
    let mut rows = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                log::warn!("[sync: {id}] cancelled");
                break;
            }
            next = stream.try_next() => {
                if let Some((offset, message)) = next? {
                    metrics.messages.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    match message {
                        MessageSet::Meta(meta) => {
                            metrics.messages_of_meta.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                                        bail!("write raw meta error: {err}");
                                    }
                                }
                            } else {
                                let mut meta = meta.as_json_meta().await?;
                                // dbg!(&meta);

                                for action in &actions {
                                    action.mutate_meta(&mut meta)?;
                                }
                                // dbg!(&meta);
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
                                        bail!("write raw meta error: {err}");
                                    }
                                }
                            }
                        }
                        MessageSet::Data(data) => {
                            metrics.messages_of_data.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                                                    bail!("create table error: {err}");
                                                }
                                            };
                                            taos.write_raw_block(&raw).await?;
                                        } else {
                                            bail!("write table failed: {err}",);
                                        }
                                    } else {
                                        bail!("write table failed: {err}",);
                                    }
                                };
                                metrics.blocks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                metrics.records.fetch_add(raw.nrows() as _, std::sync::atomic::Ordering::SeqCst);
                                metrics.points.fetch_add(raw.nrows() as u64 * raw.ncols() as u64, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                        MessageSet::MetaData(meta, data) => {
                            // log::debug!("[{id}] meta: {}", meta.as_json_meta().await?);
                            metrics.messages_of_meta.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            metrics.messages_of_data.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
                                        bail!("write raw meta failed: {err}",);
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
                                        bail!("write raw meta failed: {err}",);
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
                                                    bail!("write to table failed: {err}");
                                                }
                                            };
                                            taos.write_raw_block(&raw).await?;
                                        } else {
                                            bail!(
                                                "write to table {:?} but not exists: {err}",
                                                raw.table_name()
                                            );
                                        }
                                    } else {
                                        bail!("write to table failed: {err}",);
                                    }
                                };

                                metrics.blocks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                metrics.records.fetch_add(raw.nrows() as _, std::sync::atomic::Ordering::SeqCst);
                                metrics.points.fetch_add(raw.nrows() as u64 * raw.ncols() as u64, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                    }
                    consumer.commit(offset).await?;
                } else {
                    break;
                }
            }
        }
    }

    // while let Some((offset, message)) = stream.try_next().await? {}
    Ok(())
}

pub async fn tmq_to_td(
    from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
    cancel: CancellationToken,
) -> Result<()> {
    let (mut from, builder, topics) = check_tmq_dsn(from).await?;

    // auto generate group.id if not exists
    let mut from_params = from.drain_params();
    if from_params.get("group.id").is_none() {
        let to_params = to.drain_params();
        if let Some(v) = to_params.get("token") {
            to.set("token", v);
        }

        let group_id = group_id_hash(&from, &to);
        log::info!(
            "group.id not set, will use automatically generated group id: {}",
            group_id
        );
        from_params.insert("group.id".to_string(), group_id);
        to.params = to_params;
    }
    from.params = from_params;

    let metrics = Arc::new(TmqMetrics {
        topics: topics.len(),
        ..Default::default()
    });

    let mut handles = Vec::new();
    let mut task_id = 0;

    let target_database = to.subject.take();
    let target = TaosBuilder::from_dsn(&to)?;

    let global_taos = target.build()?;

    for topic in topics {
        let target_database = if let Some(target) = target_database.as_ref() {
            if !global_taos.database_exists(&target).await? {
                if let Some(sql) = topic.database_sql.as_deref() {
                    log::info!(
                        "target database not exist, try create database `{target}` with the same parameter from the source"
                    );
                    let mut sql = sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS");
                    if &topic.database != target {
                        sql = sql.replace(&format!("`{}`", topic.database), &format!("`{target}`"));
                    }
                    global_taos.exec(sql).await?;
                } else {
                    anyhow::bail!("can not get database params to create a same one");
                }
            }
            target
        } else {
            if !global_taos.database_exists(&topic.database).await? {
                if let Some(sql) = topic.database_sql.as_deref() {
                    global_taos
                        .exec(sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS"))
                        .await?;
                } else {
                    anyhow::bail!("can not get database params to create a same one");
                }
            }
            &topic.database
        };

        let jobs = if jobs == 0 || jobs >= topic.vgroups {
            topic.vgroups
        } else {
            jobs
        };
        metrics
            .workers
            .fetch_add(jobs as _, std::sync::atomic::Ordering::SeqCst);
        if let Some(table) = topic.table.as_ref() {
            // schema rebuild
            let taos = target.build()?;
            taos.exec(format!("use `{target_database}`")).await?;

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
                        if let Some(name) = table.stable.as_deref() {
                            let new = sql.replace(&format!("`{name}`",), &action.apply(name));
                            sql.clear();
                            sql.extend(new.chars());
                        }
                        let name = &table.table;
                        let new = sql.replace(&format!("`{name}`",), &action.apply(name));
                        sql.clear();
                        sql.extend(new.chars());
                    }
                    _ => (),
                }
            }
            taos.exec(sql).await?;
        }

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
            taos.exec(format!("use `{target_database}`")).await?;
            let table = topic.table.as_ref().map(|t| t.table.clone());
            let actions = actions.to_vec();
            let cancellation = cancel.clone();
            let metrics = metrics.clone();
            let handle = tokio::spawn(async move {
                sync(
                    task_id,
                    consumer,
                    taos,
                    table,
                    actions,
                    cancellation,
                    metrics,
                )
                .await
            });
            handles.push(handle);
            task_id += 1;
        }
    }
    for handle in handles {
        let _ = handle.await?;
    }

    drop(global_taos);
    drop(target);
    drop(builder);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    log::info!("replication done.");
    println!("{}", metrics.as_ref());

    Ok(())
}
