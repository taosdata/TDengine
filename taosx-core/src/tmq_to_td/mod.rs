use std::{ops::Deref, sync::Arc, time::Duration};

use crate::{
    core_metrics::{get_metrics_arc, CoreMetrics, TaosXMetrics},
    legacy_metric::LegacyToTaosMetrics,
    sync_super_table_schema, sync_super_table_schema_with_subs,
    tmq::{tmq_metric::TmqMetrics, *},
    Action,
};
use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use linked_hash_map::LinkedHashMap;
use metrics::counter;
use std::sync::atomic::Ordering::SeqCst;
use taos::taos_query::tmq::Assignment;
use taos::{Consumer, *};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

#[instrument(skip_all, fields(table, rows))]
async fn write_data(
    id: usize,
    rows: &mut usize,
    source: &TaosPool,
    taos: &Taos,
    table: Option<&str>,
    actions: &[Action],
    data: &Data,
    target_is_v3: bool,
    metrics: &TmqMetrics,
) -> Result<u64> {
    tracing::debug!("[{id}] start writing data");
    metrics.add_messages_of_data(1);
    let mut has_blocks = false;
    if target_is_v3 && actions.is_empty() {
        let raw = data
            .as_raw_data()
            .await
            .context("Data source raw data error")?;
        if let Err(err) = taos
            .write_raw_meta(&unsafe { std::mem::transmute(raw) })
            .await
        {
            let code = *err.code().deref();
            match code {
                // Table not exist error codes or invalid input.
                0x070F | 0x0218 | 0x2603 | 0x036D | 0x0618 | 0x2662 => {
                    // fallback to block-by-block method.
                }
                _ => {
                    counter!(METRIC_TMQ_WRITE_META_FAILS, 1);
                    Err(err).context("Write raw data into target error")?;
                }
            }
        } else {
            while let Some(raw) = data
                .fetch_raw_block()
                .await
                .context("Fetch raw block error")?
            {
                *rows += raw.nrows();
                metrics.add_written_rows(raw.nrows() as _);
                metrics.add_written_points((raw.nrows() * raw.ncols()) as _);
                metrics.add_suc_blocks(1);
            }
            return Ok(0);
        }
    }
    while let Some(mut raw) = data
        .fetch_raw_block()
        .await
        .context("Fetch raw block error")?
    {
        has_blocks = true;
        let source_table_name = raw
            .table_name()
            .ok_or_else(|| anyhow::anyhow!("Table name not found while subscribing from source"))?
            .to_string();
        if let Some(name) = table {
            if actions.is_empty() {
                raw.with_table_name(name);
                tracing::debug!(
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
                            rename.apply_in_place(&mut name)?
                        }
                        _ => (),
                    }
                }
                raw.with_table_name(&name);
                tracing::debug!(
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
                            rename.apply_in_place(&mut name)?
                        }
                        _ => (),
                    }
                }
                raw.with_table_name(&name);
                tracing::debug!(
                    "[{id}] write into {name} {} rows(total {}) with {} columns",
                    raw.nrows(),
                    rows,
                    raw.ncols()
                );
            }
        } else {
            tracing::debug!(
                "[{id}] write {} rows(total {}) with {} columns",
                raw.nrows(),
                rows,
                raw.ncols()
            );
        }
        *rows += raw.nrows();

        if target_is_v3 {
            if let Err(err) = taos.write_raw_block(&raw).await {
                let code = *err.code().deref();
                match code {
                    0x0218 | 0x2603 | 0x2662 | 0x036D | 0x0618 => {
                        let from = source.get().await?;
                        let database = from
                            .query_one::<_, String>("select database()")
                            .await?
                            .unwrap();
                        if let Some(stable) = from.query_one::<_, String>(format!("select stable_name from information_schema.ins_tables where db_name = '{database}' and table_name = '{source_table_name}'")).await?.and_then(|s| if s.is_empty() { None } else { Some(s) }) {
                            let from = source.get().await?;
                            let target_opts = Default::default();
                            sync_super_table_schema(&from, &stable, taos, None, &target_opts, actions).await.context("Create sub table error")?;
                            // 临时代码，保证编译通过
                            let metrics_arc = Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::default()));
                            sync_super_table_schema_with_subs(&from, &stable, &[source_table_name], taos, None, &target_opts, true,actions, metrics_arc).await.context("Create sub table error")?;
                            taos.write_raw_block(&raw)
                                .await
                                .context("Write raw block into target error")?;
                        } else if let Some(meta) = raw.to_create() {
                            if let Err(err) = taos.exec(format!("{}", meta)).await {
                                if err.to_string().contains("0x032C") {
                                    tokio::time::sleep(Duration::from_nanos(1000)).await;
                                } else {
                                    bail!("create table error: {err}");
                                }
                            };
                            taos.write_raw_block(&raw)
                                .await
                                .context("Write raw block into target error")?;
                        } else {
                            bail!(
                                "write table failed: {err}, with block: {}",
                                raw.pretty_format()
                            );
                        }
                    }
                    _ => {
                        bail!(
                            "write table failed: {err}, with block: {}",
                            raw.pretty_format()
                        );
                    }
                }
            };
        } else {
            let mut stmt = Stmt::init(taos)
                .await
                .context("Write with stmt init error")?;
            let fields = raw.fields();
            let question_masks = std::iter::repeat('?').take(fields.len()).join(",");
            let table = raw.table_name().unwrap();
            stmt.prepare(&format!("INSERT INTO `{table}` VALUES({question_masks})"))
                .await
                .context("Write with stmt prepare error")?;

            stmt.bind(raw.column_views())
                .await
                .context("Write with stmt bind error")?;
            stmt.add_batch()
                .await
                .context("Write with stmt add_batch error")?;
            stmt.execute()
                .await
                .context("Write with stmt execute error")?;
        }
        metrics.add_suc_blocks(1);
        metrics.add_written_rows(raw.nrows() as _);
        metrics.add_written_points((raw.nrows() * raw.ncols()) as _);
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
                        counter!(METRIC_TMQ_WRITE_META_FAILS, 1);
                        tracing::warn!("[{id}] {errstr}");
                    } else {
                        counter!(METRIC_TMQ_WRITE_META_FAILS, 1);
                        bail!("write raw data error: {err}");
                    }
                }
            } else {
                tracing::warn!("[{id}] v2 target does not support delete data");
            }
        } else {
            tracing::warn!(
                "[{id}] there's older version delete message, you must delete data manually"
            );
        }
    }
    tracing::debug!(
        "[{id}] end writing data, current records {}",
        metrics.written_rows()
    );
    Ok(0)
}

#[instrument(skip_all, fields(consumer.id = id))]
async fn write_meta(
    id: usize,
    source: &TaosPool,
    taos: &Taos,
    actions: &[Action],
    meta: &Meta,
    target_is_v3: bool,
    metrics: &TmqMetrics,
) -> Result<()> {
    let cur = metrics.add_messages_of_meta(1);
    tracing::debug!("[{id}] start writing meta {cur}");
    if actions.is_empty() {
        if target_is_v3 {
            let jm = meta.as_json_meta().await.context("Fetch json meta error");
            match &jm {
                Ok(meta) => tracing::debug!("meta: {:?}", meta),
                Err(err) => tracing::warn!("meta: {:#}", err),
            };
            let raw_meta = meta.as_raw_meta().await?;
            if let Err(err) = taos.write_raw_meta(&raw_meta).await {
                let code = *err.code().deref();
                match code {
                    // Table not exist error codes.
                    0x0218 | 0x2603 | 0x036D | 0x0618 => {
                        let meta = jm.context("Can't parse meta")?;
                        match meta {
                            JsonMeta::Create(create) => match create {
                                MetaCreate::Super {
                                    table_name,
                                    columns: _,
                                    tags: _,
                                } => {
                                    // Stable should never not exist.
                                    Err(err.context(format!(
                                        "Write raw meta error with stable {table_name}"
                                    )))?;
                                }
                                MetaCreate::Child {
                                    table_name,
                                    using,
                                    tags: _,
                                    tag_num: _,
                                } => {
                                    // Create child table error means stable not exist.
                                    tracing::warn!("Table does not exist: {using} while create child {table_name}");
                                    let from = source.get().await?;
                                    sync_super_table_schema(
                                        &from,
                                        &using,
                                        &taos,
                                        None,
                                        &Default::default(),
                                        &[],
                                    )
                                    .await?;
                                    taos.write_raw_meta(&raw_meta).await.map_err(|err| {
                                        err.context(format!(
                                            "Write raw meta error with table {table_name}"
                                        ))
                                    })?;
                                }
                                MetaCreate::Normal {
                                    table_name,
                                    columns: _,
                                } => {
                                    // Normal table should never not exist.
                                    Err(err.context(format!(
                                        "Write raw meta error with table {table_name}"
                                    )))?;
                                }
                            },
                            // Do nothing if not create
                            JsonMeta::Alter(_) | JsonMeta::Drop(_) | JsonMeta::Delete(_) => {
                                tracing::warn!(
                                    "Unexpected error {err:#} for meta: ```{meta}```, do nothing."
                                );
                            }
                        }
                    }
                    0x032C | 0x0115 | 0x0603 | 0x03C7 => {
                        metrics.add_write_raw_fails(1);
                        tracing::warn!(consumer.id = id, "Write raw meta: {err:#}");
                    }
                    _ => {
                        metrics.add_write_raw_fails(1);
                        Err(err.context("Write raw meta error"))?;
                    }
                }
            }
        } else {
            let meta = meta
                .as_json_meta()
                .await
                .context("Fetch json meta error for v2 target")?;
            taos.exec(meta.to_string()).await?;
        }
    } else {
        let mut meta = meta
            .as_json_meta()
            .await
            .context("Fetch json meta error for transform")?;
        // dbg!(&meta);

        for action in actions {
            action.mutate_meta(&mut meta)?;
        }
        // dbg!(&meta);
        let sql = meta.to_string();
        if let Err(err) = taos.exec(&sql).await {
            metrics.add_write_raw_fails(1);
            let errstr = err.to_string();
            if errstr.contains("[0x032C]")
                || errstr.contains("[0x0115]")
                || errstr.contains("[0x0603]")
                || errstr.contains("[0x03C7]")
            {
                tracing::warn!("{errstr}");
            } else {
                bail!("[{id}] write raw meta error: {err}");
            }
        }
    }
    tracing::debug!("[{id}] end writing meta {cur}");
    Ok(())
}

#[instrument(skip(sender, consumer, taos, cancel, source_pool, metrics_arc))]
async fn sync(
    id: usize,
    sender: tokio::sync::mpsc::UnboundedSender<Consumer>,
    consumer: Consumer,
    source_pool: TaosPool,
    taos: &Taos,
    table: Option<String>,
    actions: Vec<Action>,
    cancel: CancellationToken,
    metrics_arc: Arc<CoreMetrics>,
    offsets: Arc<DashMap<String, Vec<Assignment>>>,
    version: String,
) -> Result<()> {
    tracing::info!("[{id}] task start");
    let mut stream = consumer.stream();
    let mut rows = 0;
    let mut messages = 0;
    let target_is_v3 = taos
        .exec("desc information_schema.ins_databases")
        .await
        .is_ok();
    let metrics = metrics_arc.tmq();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::warn!("[sync: {id}] cancelled");
                break;
            }
            next = stream.try_next() => {
                if let Some((offset, message)) = next.with_context(|| format!("[{id}] polling next message error"))? {
                    metrics.add_messages(1);
                    let total = metrics.messages.load(SeqCst);
                    messages += 1;
                    if messages % 2000 == 0 {
                        tracing::info!("[{id}] received {messages} messages ({:.2})", messages as f64 / total as f64);
                    }
                    match message {
                        MessageSet::Meta(meta) => {
                            write_meta(id, &source_pool, taos, &actions, &meta, target_is_v3, metrics).await.with_context(|| format!("[{id}] writing meta-only message error"))?;
                        }
                        MessageSet::Data(data) => {
                            write_data(id, &mut rows, &source_pool,  taos, table.as_deref(), &actions, &data, target_is_v3, metrics).await.with_context(|| format!("[{id}] writing data message error"))?;
                        }
                        MessageSet::MetaData(meta, data) => {
                            write_meta(id, &source_pool,taos, &actions, &meta, target_is_v3, metrics).await.with_context(|| format!("[{id}] writing metadata message message error"))?;
                            if !actions.is_empty() {
                                write_data(id, &mut rows, &source_pool, taos, table.as_deref(), &actions, &data, target_is_v3, metrics).await.with_context(|| format!("[{id}] writing data message error"))?;
                            }
                        }
                    }
                    if let Err(err) = consumer.commit(offset).await {
                        tracing::warn!(
                            consumer.worker.id = id,
                            "[{id}] commit error: {err:?}"
                        );
                    }
                } else {
                    break;
                }
            }
        }
    }
    tracing::info!("[{id}] task done");

    // do not drop consumer when single task done.
    drop(stream);
    let _ = sender.send(consumer); // tokio send
    Ok(())
}

#[instrument(skip_all)]
pub async fn tmq_to_td(
    from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
    cancel: CancellationToken,
    offsets: Arc<DashMap<String, Vec<Assignment>>>,
    task_id: Option<String>,
) -> Result<()> {
    let (mut from, builder, topics) = check_tmq_dsn(from).await?;

    let version = builder.server_version().await?.to_owned();

    // auto generate group.id if not exists
    let mut from_params = from.drain_params();
    if from_params.get("group.id").is_none() {
        let to_params = to.drain_params();
        if let Some(v) = to_params.get("token") {
            to.set("token", v);
        }

        let group_id = group_id_hash(&from, &to);
        tracing::info!(
            "group.id not set, will use automatically generated group id: {}",
            group_id
        );
        from_params.insert("group.id".to_string(), group_id);
        to.params = to_params;
    }
    from.params = from_params;
    let metrics_arc = get_metrics_arc(task_id.clone());
    let metrics = metrics_arc.tmq();
    metrics.topics.fetch_add(topics.len() as _, SeqCst);

    let mut handles = Vec::new();
    let mut consumer_task_id = 0;
    let target_database = to.subject.take();

    let target_builder = TaosBuilder::from_dsn(&to)?;

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
            target
        } else {
            if let Some(sql) = topic.database_sql.as_deref() {
                let _ = target_taos
                    .exec(sql.replace("CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS"))
                    .await;
            }
            &topic.database
        };

        let jobs = if jobs == 0 || jobs >= topic.vgroups {
            topic.vgroups
        } else {
            jobs
        };
        metrics.consumers.fetch_add(jobs as _, SeqCst);
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
                            let new = sql.replace(&format!("`{name}`",), &action.apply(name)?);
                            sql.clear();
                            sql.extend(new.chars());
                        }
                        Action::RenameSuperTable(action) => {
                            let name = table.stable.as_deref().unwrap();
                            let new = sql.replace(&format!("`{name}`",), &action.apply(name)?);
                            sql.clear();
                            sql.extend(new.chars());
                        }
                        // Action::RenameReplaceWithRegex(action) => {
                        //     let name = table.stable.as_deref().unwrap();
                        //     let new = sql.replace(&format!("`{name}`",), &action.apply(name));
                        //     sql.clear();
                        //     sql.extend(new.chars());
                        // }
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
                            let new = sql.replace(&format!("`{name}`",), &action.apply(name)?);
                            sql.clear();
                            sql.extend(new.chars());
                        }
                        let name = &table.table;
                        let new = sql.replace(&format!("`{name}`",), &action.apply(name)?);
                        sql.clear();
                        sql.extend(new.chars());
                    }
                    _ => (),
                }
            }
            taos.exec(sql).await?;
        }

        let tmq = TmqBuilder::from_dsn(&from)?;
        let mut from = from.clone();
        from.subject.replace(topic.database.clone());
        let source_pool = TaosBuilder::from_dsn(&from)?.pool()?;

        let mut consumers = Vec::with_capacity(jobs);

        let consumer_timer = std::time::Instant::now();

        let mut consumer_handles = Vec::with_capacity(jobs);
        for id in 0..jobs {
            let mut consumer = tmq.build().await?;
            let topic = topic.name.clone();
            consumer_handles.push(tokio::spawn(async move {
                tracing::debug!("Subscribe consumer {id}");
                consumer.subscribe([&topic]).await.with_context(|| {
                    format!("Subscribe consumer [{id}] with topic `{topic}` error")
                })?;
                anyhow::Ok(consumer)
            }));
        }

        for h in consumer_handles {
            let consumer = h.await??;
            consumers.push(consumer);
        }
        let duration = consumer_timer.elapsed();
        tracing::info!("Setup {} consumers in {:?}", jobs, duration);

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
            let sender = consumers_sender.clone();
            let offsets = offsets.clone();
            let version = version.clone();
            let source_pool = source_pool.clone();
            let metrics_arc = metrics_arc.clone();
            let handle = tokio::spawn(
                async move {
                    sync(
                        consumer_task_id,
                        sender,
                        consumer,
                        source_pool,
                        &taos,
                        table,
                        actions,
                        cancellation,
                        metrics_arc.clone(),
                        offsets,
                        version,
                    )
                    .await
                }
                .in_current_span(),
            );
            handles.push(handle);
            tracing::info!("spawn consuming task with id {consumer_task_id}",);

            consumer_task_id += 1;
        }
    }

    tracing::info!("spawn consuming tasks {}", handles.len());
    for handle in handles {
        let _ = handle.await??;
    }
    tracing::debug!("consumers tasks offsets: {:?}", offsets);

    tracing::info!("stop all consumers({})", consumer_task_id);
    for _ in 0..consumer_task_id {
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
    tracing::info!("replication done.");
    println!("{}", metrics);

    Ok(())
}

#[instrument(skip_all)]
pub async fn tmq_offsets(from: Dsn) -> anyhow::Result<LinkedHashMap<String, Vec<Assignment>>> {
    let (from, _, topics) = check_tmq_dsn(from).await?;
    let tmq = TmqBuilder::from_dsn(&from)?;
    let mut consumer = tmq.build().await?;
    consumer
        .subscribe(&topics.iter().map(|t| t.name.to_string()).collect_vec())
        .await?;
    Ok(consumer
        .assignments()
        .await
        .unwrap_or_default()
        .into_iter()
        .collect())
}
