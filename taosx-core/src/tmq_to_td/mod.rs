use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use taos::{Consumer, *};
use tokio_util::sync::CancellationToken;

use crate::{
    tmq::{check_tmq_dsn, group_id_hash, TmqMetrics},
    Action,
};
use dashmap::DashMap;
use taos::taos_query::tmq::Assignment;

async fn write_data(
    id: usize,
    rows: &mut usize,
    taos: &Taos,
    table: Option<&str>,
    actions: &[Action],
    data: &Data,
    target_is_v3: bool,
    metrics: &TmqMetrics,
) -> Result<u64> {
    log::debug!("[{id}] start writing data");
    metrics
        .messages_of_data
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let mut has_blocks = false;
    if target_is_v3 && actions.is_empty() {
        let raw = data.as_raw_data().await?;
        taos.write_raw_meta(&unsafe { std::mem::transmute(raw) })
            .await?;
        while let Some(raw) = data.fetch_raw_block().await? {
            *rows += raw.nrows();
            metrics
                .blocks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            metrics
                .records
                .fetch_add(raw.nrows() as _, std::sync::atomic::Ordering::SeqCst);
            metrics.points.fetch_add(
                raw.nrows() as u64 * raw.ncols() as u64,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        return Ok(0);
    }
    while let Some(mut raw) = data.fetch_raw_block().await? {
        has_blocks = true;
        if let Some(name) = table {
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
                for action in actions {
                    match action {
                        Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
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
                for action in actions {
                    match action {
                        Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
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
        *rows += raw.nrows();

        if target_is_v3 {
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
                        bail!(
                            "write table failed: {err}, with block: {}",
                            raw.pretty_format()
                        );
                    }
                } else {
                    bail!(
                        "write table failed: {err}, with block: {}",
                        raw.pretty_format()
                    );
                }
            };
        } else {
            let mut stmt = Stmt::init(taos)?;
            let fields = raw.fields();
            let question_masks = std::iter::repeat('?').take(fields.len()).join(",");
            let table = raw.table_name().unwrap();
            stmt.prepare(format!("INSERT INTO `{table}` VALUES({question_masks})"))?;

            stmt.bind(raw.column_views())?;
            stmt.add_batch()?;
            stmt.execute()?;
        }
        metrics
            .blocks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        metrics
            .records
            .fetch_add(raw.nrows() as _, std::sync::atomic::Ordering::SeqCst);
        metrics.points.fetch_add(
            raw.nrows() as u64 * raw.ncols() as u64,
            std::sync::atomic::Ordering::SeqCst,
        );
    }
    if !has_blocks {
        if actions.is_empty() {
            if target_is_v3 {
                if let Err(err) = taos
                    .write_raw_meta(&unsafe { std::mem::transmute(data.as_raw_data().await?) })
                    .await
                {
                    let errstr = err.to_string();
                    if errstr.contains("[0x032C]")
                        || errstr.contains("[0x0115]")
                        || errstr.contains("[0x0603]")
                        || errstr.contains("[0x03C7]")
                    {
                        log::warn!("[{id}] {errstr}");
                    } else {
                        bail!("write raw meta error: {err}");
                    }
                }
            } else {
                log::warn!("[{id}] v2 target does not support delete data");
            }
        } else {
            log::warn!(
                "[{id}] there's older version delete message, you must delete data manually"
            );
        }
    }
    log::debug!(
        "[{id}] end writing data, current records {}",
        metrics.records.load(std::sync::atomic::Ordering::SeqCst)
    );
    Ok(0)
}

async fn write_meta(
    id: usize,
    taos: &Taos,
    actions: &[Action],
    meta: &Meta,
    target_is_v3: bool,
    metrics: &TmqMetrics,
) -> Result<()> {
    let order = std::sync::atomic::Ordering::SeqCst;

    let cur = metrics
        .messages_of_meta
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    log::debug!("[{id}] start writing meta {cur}");
    // log::debug!("[{id}] meta: {}", meta.as_json_meta().await?);
    if actions.is_empty() {
        if target_is_v3 {
            let jm = meta.as_json_meta().await?;
            log::debug!("meta: {}", jm);
            if let Err(err) = taos.write_raw_meta(&meta.as_raw_meta().await?).await {
                let errstr = err.to_string();
                if errstr.contains("[0x032C]")
                    || errstr.contains("[0x0115]")
                    || errstr.contains("[0x0603]")
                    || errstr.contains("[0x03C7]")
                {
                    log::warn!("[{id}] {errstr}");
                } else {
                    bail!("write raw meta error: {err}");
                }
            }
        } else {
            let meta = meta.as_json_meta().await?;
            taos.exec(meta.to_string()).await?;
        }
    } else {
        let mut meta = meta.as_json_meta().await?;
        // dbg!(&meta);

        for action in actions {
            action.mutate_meta(&mut meta)?;
        }
        // dbg!(&meta);
        let sql = meta.to_string();
        if let Err(err) = taos.exec(&sql).await {
            let errstr = err.to_string();
            if errstr.contains("[0x032C]")
                || errstr.contains("[0x0115]")
                || errstr.contains("[0x0603]")
                || errstr.contains("[0x03C7]")
            {
                log::warn!("{errstr}");
            } else {
                bail!("[{id}] write raw meta error: {err}");
            }
        }
    }
    log::debug!("[{id}] end writing meta {cur}");
    Ok(())
}

async fn sync(
    id: usize,
    sender: tokio::sync::mpsc::UnboundedSender<Consumer>,
    consumer: Consumer,
    taos: &Taos,
    table: Option<String>,
    actions: Vec<Action>,
    cancel: CancellationToken,
    metrics: Arc<TmqMetrics>,
    offsets: Arc<DashMap<String, Vec<Assignment>>>,
) -> Result<()> {
    log::info!("[{id}] task start");
    let mut stream = consumer.stream();
    let mut rows = 0;
    let mut messages = 0;
    let target_is_v3 = taos
        .exec("desc information_schema.ins_databases")
        .await
        .is_ok();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                log::warn!("[sync: {id}] cancelled");
                break;
            }
            next = stream.try_next() => {
                let assignments = consumer.assignments().await.unwrap();
                log::debug!("assignment: {:?}", assignments);
                for (topic, assignment) in assignments {
                    offsets.insert(topic, assignment);
                }

                if let Some((offset, message)) = next? {
                    metrics.messages.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let total = metrics.messages.load(std::sync::atomic::Ordering::SeqCst);
                    messages += 1;
                    if messages % 2000 == 0 {
                        log::info!("[{id}] received {messages} messages ({:.2})", messages as f64 / total as f64);
                    }
                    match message {
                        MessageSet::Meta(meta) => {
                            write_meta(id, taos, &actions, &meta, target_is_v3, &metrics).await?;
                        }
                        MessageSet::Data(data) => {
                            write_data(id, &mut rows, taos, table.as_deref(), &actions, &data, target_is_v3, &metrics).await?;
                        }
                        MessageSet::MetaData(meta, data) => {
                            write_meta(id, taos, &actions, &meta, target_is_v3, &metrics).await?;
                            if !actions.is_empty() {
                                write_data(id, &mut rows, taos, table.as_deref(), &actions, &data, target_is_v3, &metrics).await?;
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
    log::info!("[{id}] task done");

    // do not drop consumer when single task done.
    drop(stream);
    let _ = sender.send(consumer);
    Ok(())
}

pub async fn tmq_to_td(
    from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
    cancel: CancellationToken,
    offsets: Arc<DashMap<String, Vec<Assignment>>>,
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

    let target_builder = TaosBuilder::from_dsn(&to)?;

    #[cfg(not(feature = "disable-enterprise-only-validation"))]
    {
        if !builder.is_enterprise_edition().await?
            && !target_builder.is_enterprise_edition().await?
        {
            bail!("Only enterprise edition is supported. If it's not your case, please contact us.")
        }
    }
    let target = target_builder.pool()?;
    let target_taos = target.get().await?;

    let (consumers_sender, mut consumers_receiver) = tokio::sync::mpsc::unbounded_channel();

    for topic in topics {
        let target_database = if let Some(target) = target_database.as_ref() {
            if let Some(sql) = topic.database_sql.as_deref() {
                let mut sql = sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS");
                if &topic.database != target {
                    sql = sql.replace(&format!("`{}`", topic.database), &format!("`{target}`"));
                }
                let _ = target_taos.exec(sql).await;
            }
            // target_taos.database_exists(&target);
            // if !target_taos.database_exists(&target).await? {
            //     if let Some(sql) = topic.database_sql.as_deref() {
            //         log::info!(
            //             "target database not exist, try create database `{target}` with the same parameter from the source"
            //         );
            //         let mut sql = sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS");
            //         if &topic.database != target {
            //             sql = sql.replace(&format!("`{}`", topic.database), &format!("`{target}`"));
            //         }
            //         target_taos.exec(sql).await?;
            //     } else {
            //         anyhow::bail!("can not get database params to create a same one");
            //     }
            // }
            target
        } else {
            if let Some(sql) = topic.database_sql.as_deref() {
                let _ = target_taos
                    .exec(sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS"))
                    .await;
            }
            // if !target_taos.database_exists(&topic.database).await? {
            //     if let Some(sql) = topic.database_sql.as_deref() {
            //         target_taos
            //             .exec(sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS"))
            //             .await?;
            //     } else {
            //         anyhow::bail!("can not get database params to create a same one");
            //     }
            // }
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
        let mut target_dsn = to.clone();
        target_dsn.subject.replace(target_database.to_string());
        let target = TaosBuilder::from_dsn(target_dsn)?.pool()?;

        if let Some(table) = topic.table.as_ref() {
            // schema rebuild
            let taos = target.get().await?;
            // taos.exec(format!("use `{target_database}`")).await?;

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

        let consumer_timer = std::time::Instant::now();

        let (tx, rx) = flume::bounded(jobs);

        for _ in 0..jobs {
            let tx = tx.clone();
            let mut consumer = tmq.build().await?;
            let topic = topic.name.clone();
            tokio::spawn(async move {
                consumer.subscribe([&topic]).await?;
                tx.send(consumer)?;
                Ok::<(), anyhow::Error>(())
            });
        }
        for _ in 0..jobs {
            let consumer = rx.recv_async().await?;
            consumers.push(consumer);
        }
        let duration = consumer_timer.elapsed();
        log::info!("Setup {} consumers in {:?}", jobs, duration);

        for _ in 0..jobs {
            let consumer = consumers.pop().unwrap();
            let taos = target.get().await?;
            // taos.exec(format!("use `{target_database}`")).await?;
            let mut table = topic.table.as_ref().map(|t| t.table.clone());
            if topic.is_query() {
                if let Some(name) = topic.use_table_name.as_ref() {
                    table.replace(name.to_string());
                } else if table.is_none() {
                    table.replace(topic.name.clone());
                }
            }
            let actions = actions.to_vec();
            let cancellation = cancel.clone();
            let metrics = metrics.clone();
            let sender = consumers_sender.clone();
            let offsets = offsets.clone();
            let handle = tokio::spawn(async move {
                sync(
                    task_id,
                    sender,
                    consumer,
                    &taos,
                    table,
                    actions,
                    cancellation,
                    metrics,
                    offsets,
                )
                .await
            });
            handles.push(handle);
            log::info!("spawn consuming task with id {task_id}",);

            task_id += 1;
        }
    }

    log::info!("spawn consuming tasks {}", handles.len());
    for handle in handles {
        let _ = handle.await??;
    }
    log::debug!("consumers tasks offsets: {:?}", offsets);

    log::info!("stop all consumers({})", task_id);
    for _ in 0..task_id {
        let consumer = consumers_receiver.recv().await;
        tokio::spawn(async move {
            if let Some(consumer) = consumer {
                consumer.unsubscribe().await;
            }
        });
    }
    // for consumers in consumers_receiver.

    drop(target_taos);
    drop(target);
    drop(builder);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    log::info!("replication done.");
    println!("{}", metrics.as_ref());

    Ok(())
}
