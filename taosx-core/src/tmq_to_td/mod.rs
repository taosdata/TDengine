mod worker;

use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use crate::{
    core_metrics::{get_metrics_arc, CoreMetrics, TaskMetrics},
    legacy_metric::LegacyToTaosMetrics,
    sync_normal_table_schema, sync_super_table_schema, sync_super_table_schema_with_subs,
    tmq::{tmq_metric::TmqMetrics, *},
    utils::{constants::VERSION_3_3_0, interval::IntervalLimit},
    Action,
};
use anyhow::{anyhow, bail, Context, Result};
use faststr::FastStr;
use humantime::parse_duration;
use linked_hash_map::LinkedHashMap;
use serde::Serialize;
use std::sync::atomic::Ordering::SeqCst;
use taos::taos_query::tmq::Assignment;
use taos::*;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};
use worker::{Worker, WriteOptions};

use worker::*;

async fn migrate_data_schema(desc: &[Field], to: &Taos, table: &str) -> Result<bool> {
    let target_desc = to.describe(table).await?;
    let fields: BTreeMap<_, _> = target_desc.iter().map(|f| (f.field(), f)).collect();

    let desc_first = &desc[0];
    let target_desc_first = target_desc.first();
    // check if the first field is timestamp
    if desc_first.ty() == Ty::Timestamp {
        if let Some(target_desc_first) = target_desc_first {
            if !(target_desc_first.ty() == Ty::Timestamp
                && desc_first.name() == target_desc_first.field())
            {
                tracing::error!(
                    "Mismatch the first field: expect `{:?}`, but got `{:?}`",
                    target_desc_first,
                    desc_first
                );
                return Ok(false);
            }
        }
    } else {
        tracing::error!(
            "Error data: expect timestamp as first field, but got `{}`",
            desc_first.ty()
        );
        return Ok(false);
    }

    for l in desc {
        if let Some(r) = fields.get(l.name()) {
            // check if the field is equal.
            if r.is_tag() {
                tracing::error!(
                    "Target field is not match the source: expect `{}` as column, but got tag",
                    l.name()
                );
                return Ok(false);
            }
            if r.ty() != l.ty() {
                tracing::warn!(
                    "Target field ({}) is not equal to source({})",
                    r.sql_repr(),
                    l.sql_repr()
                );
            } else if r.length() < l.bytes() as usize {
                if let Err(err) = to
                    .exec(format!(
                        "ALTER TABLE `{}` MODIFY COLUMN {}",
                        table,
                        l.sql_repr(),
                    ))
                    .await
                {
                    tracing::warn!(
                        "Modify column `{}` of table `{table}` error: {err:#}, try continue",
                        l.name()
                    );
                }
            }
        } else {
            // field does not exist in right side.
            if let Err(err) = to
                .exec(format!(
                    "ALTER TABLE `{}` ADD COLUMN {}",
                    table,
                    l.sql_repr(),
                ))
                .await
            {
                tracing::warn!(
                    "Add column {} for table {table} error: {err:#}, try continue",
                    l.name()
                );
            }
        }
    }
    Ok(true)
}

async fn write_data(
    topic: &Topic,
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
    tracing::trace!("Start writing data");
    metrics.add_messages_of_data(1);
    let mut has_blocks = false;
    let mut last_error = None;
    if target_is_v3 && !topic.is_query() && actions.is_empty() {
        let raw = data
            .as_raw_data()
            .await
            .context("Data source raw data error")?;
        if let Err(err) = taos
            .write_raw_meta(&unsafe {
                std::mem::transmute::<taos::taos_query::common::RawData, taos::RawMeta>(raw)
            })
            .await
        {
            let code = *err.code().deref();
            match code {
                // Table not exist error codes or invalid input.
                0x070F | 0x0218 | 0x2603 | 0x036D | 0x0618 | 0x2662 | 0x0118 | 0x4000 => {
                    tracing::debug!("Fallback to block-by-block method due to: {err:#}.");
                    last_error.replace(err);
                }
                _ => {
                    metrics.add_write_raw_fails(1);
                    tracing::error!("Write data error: {err}");
                    let block = data.fetch_raw_block().await;
                    if let Ok(Some(block)) = block {
                        tracing::error!("Details about the failed data: {}", block.pretty_format());
                    } else {
                        tracing::warn!("Failed to fetch raw block");
                    }
                    Err(err).context("Write raw data into target error")?;
                }
            }
        } else {
            return Ok(0);
        }
    }

    while let Some(mut raw) = data
        .fetch_raw_block()
        .await
        .context("Fetch raw block error")?
    {
        has_blocks = true;

        let source_table_name = raw.table_name().and_then(|name| {
            if name.is_empty() {
                None
            } else {
                Some(name.to_owned())
            }
        });
        tracing::trace!(
            source.table = source_table_name,
            "sync block with {} rows {} cols",
            raw.nrows(),
            raw.ncols()
        );
        if let Some(name) = table {
            if actions.is_empty() {
                raw.with_table_name(name);
                tracing::debug!(
                    "Write into {name} {} rows(total {}) with {} columns",
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
                    "Write into {name} {} rows(total {}) with {} columns",
                    raw.nrows(),
                    rows,
                    raw.ncols()
                );
            }
        } else if let Some(name) = raw.table_name() {
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
                    "write into {name} {} rows(total {}) with {} columns",
                    raw.nrows(),
                    rows,
                    raw.ncols()
                );
            }
        } else {
            // 会走到这里吗？
            tracing::debug!(
                "write {} rows(total {}) with {} columns",
                raw.nrows(),
                rows,
                raw.ncols()
            );
        }
        *rows += raw.nrows();

        let last_error_context = || {
            last_error.as_ref().map_or_else(
                || {
                    if actions.is_empty() {
                        "Write blocks to older version".to_string()
                    } else {
                        "Write blocks with transform actions".to_string()
                    }
                },
                |last_error| format!("Fallback while write_raw: {:#}", last_error),
            )
        };

        let raw_block_context = || format!("Error with block: {}", raw.pretty_format());

        if target_is_v3 {
            let with_raw_block = async {
                if let Err(err) = taos.write_raw_block(&raw).await {
                    let code = *err.code().deref();
                    tracing::debug!("Try to recover from error: {err}");
                    if let Some(source_table_name) = source_table_name {
                        match code {
                            // invalid parameters
                            0x0118 => {
                                let from = source.get().await?;
                                let database = topic.database.as_str();
                                // sync schema
                                let source_stable_name = from.query_one::<_, String>(format!("select stable_name from information_schema.ins_tables where db_name = '{database}' and table_name = '{source_table_name}'")).await?;
                                if let Some(mut source_stable_name) = source_stable_name {
                                    if actions.is_empty() {
                                        let result = migrate_data_schema(
                                            &raw.fields(),
                                            taos,
                                            &source_table_name,
                                        )
                                        .await?;
                                        // if failed, do not retry again
                                        if !result {
                                            return anyhow::Ok(());
                                        }
                                    } else {
                                        for action in actions {
                                            match action {
                                                Action::RenameTable(rename)
                                                | Action::RenameSuperTable(rename) => rename
                                                    .apply_in_place(&mut source_stable_name)?,
                                                _ => (),
                                            }
                                        }
                                        let result = migrate_data_schema(
                                            &raw.fields(),
                                            taos,
                                            &source_stable_name,
                                        )
                                        .await?;
                                        // if failed, do not retry again
                                        if !result {
                                            return anyhow::Ok(());
                                        }
                                    }
                                } else {
                                    let table = raw.table_name().unwrap();
                                    let result =
                                        migrate_data_schema(&raw.fields(), taos, table).await?;
                                    // if failed, do not retry again
                                    if !result {
                                        return anyhow::Ok(());
                                    }
                                }
                                taos.write_raw_block(&raw).await.context(
                                    "Write raw block into target error after 0x0118 fix",
                                )?;
                            }
                            // table not exist
                            0x0218 | 0x2603 | 0x2662 | 0x036D | 0x0618 => {
                                let from = source.get().await?;
                                let database = topic.database.as_str();
                                if topic.is_query() {
                                    // sync as normal table.
                                    sync_normal_table_schema(
                                        &from,
                                        &source_table_name,
                                        actions,
                                        None,
                                        taos,
                                    )
                                    .await
                                    .context("Create table error")?;
                                }
                                if let Some(stable) = from.query_one::<_, String>(format!("select stable_name from information_schema.ins_tables where db_name = '{database}' and table_name = '{source_table_name}'")).await?.and_then(|s| if s.is_empty() { None } else { Some(s) }) {
                                    let from = source.get().await?;
                                    let target_opts = Default::default();
                                    sync_super_table_schema(&from, &stable, taos, None, &target_opts, actions).await.context("Create super table error")?;
                                    // 临时代码，保证编译通过
                                    let metrics_arc = Arc::new(CoreMetrics::Legacy(LegacyToTaosMetrics::default()));
                                    sync_super_table_schema_with_subs(&from, &stable, &[source_table_name], taos, None, &target_opts, true,actions, metrics_arc).await.context("Create sub table error")?;
                                    taos.write_raw_block(&raw)
                                        .await
                                        .context("Write raw block into target error")?;
                                } else {
                                    // normal table
                                    sync_normal_table_schema(&from, &source_table_name, actions, None, taos).await.context("Create table error")?;
                                    taos.write_raw_block(&raw)
                                        .await
                                        .context("Write raw block into target error")?;
                                }
                            }
                            // table has been modified
                            0x061B => {
                                // Table schema is old.
                                let _ = taos.describe(raw.table_name().unwrap()).await;
                                let mut max_retries = 5;
                                loop {
                                    if let Err(err) = taos.write_raw_block(&raw).await {
                                        if max_retries == 0 {
                                            Err(err).context("Try to fix 0x061B error failed")?;
                                        } else {
                                            max_retries -= 1;
                                        }
                                    }
                                }
                            }
                            _ => Err(err)?,
                        }
                    } else if let Some(meta) = raw.to_create() {
                        let sql = meta.to_string();
                        taos.exec(&sql)
                            .await
                            .with_context(|| format!("SQL: {sql}"))?;
                    } else {
                        Err(err)?
                    }
                };
                anyhow::Ok(())
            };
            with_raw_block
                .await
                .inspect_err(|_| metrics.add_write_raw_fails(1))
                .with_context(raw_block_context)
                .with_context(last_error_context)
                .context("Write raw block into target error")?
        } else {
            let with_stmt = async {
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
                anyhow::Ok(())
            };
            with_stmt
                .await
                .inspect_err(|_| metrics.add_write_raw_fails(1))
                .with_context(raw_block_context)
                .with_context(last_error_context)
                .context("write table with stmt error")?;
        }
        metrics.add_suc_blocks(1);
        metrics.add_written_rows(raw.nrows() as _);
        metrics.add_written_points((raw.nrows() * raw.ncols()) as _);
    }
    if !has_blocks {
        if actions.is_empty() {
            if target_is_v3 {
                if let Err(err) = taos
                    .write_raw_meta(&unsafe {
                        std::mem::transmute::<taos::taos_query::common::RawData, taos::RawMeta>(
                            data.as_raw_data().await?,
                        )
                    })
                    .await
                {
                    let errstr = err.to_string();
                    if errstr.contains("[0x032C]")
                        || errstr.contains("[0x0115]")
                        || errstr.contains("[0x0603]")
                        || errstr.contains("[0x03C7]")
                    {
                        // counter!(METRIC_TMQ_WRITE_META_FAILS, 1);
                        tracing::warn!("[{id}] {errstr}");
                    } else {
                        // counter!(METRIC_TMQ_WRITE_META_FAILS, 1);
                        bail!("write raw data error: {err}");
                    }
                }
            } else {
                tracing::warn!("v2 target does not support delete data");
            }
        } else {
            tracing::warn!("there's older version delete message, you must delete data manually");
        }
    }
    tracing::debug!(
        "End writing data, current written rows {}",
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
    with_meta_delete: bool,
    with_meta_drop: bool,
    with_data: bool,
) -> Result<()> {
    let cur = metrics.add_messages_of_meta(1);
    let mut json_meta = match meta.as_json_meta().await.context("Fetch json meta error") {
        Ok(json_meta) => json_meta,
        Err(err) => {
            // Without fallback.
            tracing::debug!("Can't get json meta: {err}");

            if actions.is_empty() {
                if target_is_v3 {
                    let raw_meta = meta.as_raw_meta().await?;
                    taos.write_raw_meta(&raw_meta)
                        .await
                        .context("Write raw meta without fallback error")?;
                } else {
                    tracing::warn!("v2 target does not support raw meta");
                }
            } else {
                tracing::warn!("Can't get json meta, skip the meta transform");
            }
            return Ok(());
        }
    };
    tracing::debug!(meta.sql = %json_meta.iter().join(";"), meta.idx = cur, "Start writing meta");

    let mut meta_changed = false;
    match (with_meta_delete, with_meta_drop) {
        (true, true) => {
            // do nothing, all kinds of meta are allowed.
        }
        (true, false) => {
            if json_meta
                .iter()
                .any(|unit| matches!(unit, MetaUnit::Drop(_)))
            {
                // skip drop meta
                match &mut json_meta {
                    JsonMeta::Single(_) => return Ok(()),
                    JsonMeta::Plural { metas, .. } => {
                        metas.retain(|unit| matches!(unit, MetaUnit::Drop(_)));
                    }
                }

                meta_changed = true;
            }
        }
        (false, true) => {
            if json_meta
                .iter()
                .any(|unit| matches!(unit, MetaUnit::Delete(_)))
            {
                match &mut json_meta {
                    JsonMeta::Single(_) => return Ok(()),
                    JsonMeta::Plural { metas, .. } => {
                        metas.retain(|unit| matches!(unit, MetaUnit::Delete(_)));
                    }
                }

                meta_changed = true;
            }
        }
        (false, false) => {
            if json_meta
                .iter()
                .any(|unit| matches!(unit, MetaUnit::Delete(_) | MetaUnit::Drop(_)))
            {
                match &mut json_meta {
                    JsonMeta::Single(_) => return Ok(()),
                    JsonMeta::Plural { metas, .. } => {
                        metas
                            .retain(|unit| matches!(unit, MetaUnit::Drop(_) | MetaUnit::Delete(_)));
                    }
                }

                meta_changed = true;
            }
        }
    }
    if actions.is_empty() || meta_changed {
        if target_is_v3 {
            let raw_meta = meta.as_raw_meta().await?;
            if let Err(err) = taos.write_raw_meta(&raw_meta).await {
                metrics.add_write_raw_fails(1);
                // Print error no matter how we will deal with it, so that we can know what happened.
                tracing::debug!("Write raw meta error: {err:#}");
                let code = *err.code().deref();
                match code {
                    // Table not exist error codes.
                    0x0218 | 0x2603 | 0x036D | 0x0618 => {
                        for json_meta in &json_meta {
                            match json_meta {
                                MetaUnit::Create(create) => match create {
                                    MetaCreate::Super {
                                        table_name: _,
                                        columns: _,
                                        tags: _,
                                    } => {
                                        // Stable should never not exist.
                                        continue;
                                    }
                                    MetaCreate::Child {
                                        table_name,
                                        using,
                                        tags: _,
                                        tag_num: _,
                                    } => {
                                        // Create child table error means stable not exist.
                                        tracing::warn!("Table does not exist: {using} while create child {table_name}. Sync super table schema.");
                                        let from = source.get().await?;
                                        sync_super_table_schema(
                                            &from,
                                            using,
                                            taos,
                                            None,
                                            &Default::default(),
                                            &[],
                                        )
                                        .await?;
                                        if let Err(err) = taos.write_raw_meta(&raw_meta).await {
                                            metrics.add_write_raw_fails(1);
                                            Err(err.context(format!(
                                                "Write raw meta error with table {table_name}"
                                            )))?;
                                        }
                                    }
                                    MetaCreate::Normal {
                                        table_name: _,
                                        columns: _,
                                    } => {
                                        // Normal table should never not exist.
                                        continue;
                                    }
                                },
                                // Do nothing if not create
                                MetaUnit::Alter(_) | MetaUnit::Drop(_) | MetaUnit::Delete(_) => {
                                    tracing::warn!(
                                    "Unexpected error {err:#} for meta: ```{json_meta}```, do nothing."
                                );
                                }
                            }
                        }
                    }
                    0x032C | 0x0115 | 0x0603 | 0x03C7 | 0x03D3 => {
                        // do nothing
                    }
                    _ => {
                        // Fallback to sql method.
                        tracing::debug!("Fallback to sql method due to: {err:#}.");
                        let sqls = json_meta.iter().map(ToString::to_string).collect_vec();
                        taos.exec_many(&sqls)
                            .in_current_span()
                            .await
                            .context("Write raw meta with sql error")?;
                    }
                }
            } else {
                // Write raw meta success, no need to check if with data.
                return Ok(());
            }
        } else {
            let sqls = json_meta.iter().map(ToString::to_string).collect_vec();
            taos.exec_many(&sqls).await?;
        }
    } else {
        for action in actions {
            action.mutate_meta(&mut json_meta)?;
        }
        let sqls = json_meta.iter().map(ToString::to_string).collect_vec();
        for sql in &sqls {
            if let Err(err) = taos.exec(sql).await {
                metrics.add_write_raw_fails(1);
                let errstr = err.to_string();
                if errstr.contains("[0x032C]")
                    || errstr.contains("[0x03D3]")
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
    }
    if with_data {
        bail!("raw bytes contains data blocks, should not write meta only");
    }
    tracing::trace!("End writing meta {cur}");
    Ok(())
}

type TaosConnection = deadpool::managed::Object<taos::taos_query::Manager<taos::TaosBuilder>>;

async fn sync_msg(
    topic: &Topic,
    consumer: &Consumer,
    id: usize,
    offset: Offset,
    message: MessageSet<Meta, Data>,
    messages: &mut usize,
    rows: &mut usize,
    metrics: &TmqMetrics,
    source_pool: &TaosPool,
    target_pool: &TaosPool,
    taos: &mut TaosConnection,
    table: Option<&str>,
    actions: &[Action],
    with_meta_delete: bool,
    with_meta_drop: bool,
    target_is_v3: bool,
) -> Result<()> {
    metrics.add_messages(1);
    let total = metrics.messages.load(SeqCst);
    *messages += 1;
    if *messages % 2000 == 0 {
        tracing::info!(
            "Received {messages} messages ({:.2})",
            *messages as f64 / total as f64
        );
    }
    let mut retries = 0;
    let max_retries = 3;
    match message {
        MessageSet::Meta(meta) => loop {
            let write_meta_result = write_meta(
                id,
                source_pool,
                taos,
                actions,
                &meta,
                target_is_v3,
                metrics,
                with_meta_delete,
                with_meta_drop,
                false,
            )
            .in_current_span()
            .await;
            if let Err(err) = write_meta_result {
                let msg = format!("{:#}", err);
                if msg.contains("0xE00") && retries < max_retries {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    *taos = target_pool.get().await.context("Target connection error")?;
                    retries += 1;
                    continue;
                }
                tracing::warn!("Ignore error: {}", msg);
            }
            retries = 0;
            break;
        },
        MessageSet::Data(data) => loop {
            if let Err(err) = write_data(
                topic,
                id,
                rows,
                source_pool,
                taos,
                table,
                actions,
                &data,
                target_is_v3,
                metrics,
            )
            .in_current_span()
            .await
            {
                let msg = format!("{:#}", err);
                if msg.contains("0xE00") && retries < max_retries {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    *taos = target_pool.get().await.context("Target connection error")?;
                    retries += 1;
                    continue;
                }
                return Err(err).with_context(|| format!("[{id}] writing data message error"));
            }
            retries = 0;
            break;
        },
        MessageSet::MetaData(meta, data) => loop {
            let write_meta_result = write_meta(
                id,
                source_pool,
                taos,
                actions,
                &meta,
                target_is_v3,
                metrics,
                with_meta_delete,
                with_meta_drop,
                true,
            )
            .in_current_span()
            .await;
            let mut meta_skipped = false;
            if let Err(err) = write_meta_result {
                let msg = format!("{:#}", err);
                if msg.contains("0xE00") && retries < max_retries {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    *taos = target_pool.get().await.context("Target connection error")?;
                    retries += 1;
                    continue;
                }
                tracing::warn!("Ignore error: {}", err);
                meta_skipped = true;
            }
            if !actions.is_empty() || meta_skipped {
                if let Err(err) = write_data(
                    topic,
                    id,
                    rows,
                    source_pool,
                    taos,
                    table,
                    actions,
                    &data,
                    target_is_v3,
                    metrics,
                )
                .in_current_span()
                .await
                {
                    let msg = format!("{:#}", err);
                    if msg.contains("0xE00") && retries < max_retries {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        *taos = target_pool.get().await.context("Target connection error")?;
                        retries += 1;
                        continue;
                    }
                    return Err(err)
                        .with_context(|| format!("[{id}] writing metadata message error"));
                }
            }
            retries = 0;
            break;
        },
    }
    if let Err(err) = consumer.commit(offset).await {
        tracing::warn!(retries, "Commit error: {err:?}");
    }
    anyhow::Ok(())
}

#[instrument(skip_all, fields(consumer.id = id, table))]

async fn sync(
    topic: &Topic,
    id: usize,
    consumer: Consumer,
    source_pool: &TaosPool,
    target_pool: &TaosPool,
    table: Option<&str>,
    actions: &[Action],
    cancel: CancellationToken,
    metrics_arc: Arc<CoreMetrics>,
    with_meta_delete: bool,
    with_meta_drop: bool,
) -> Result<Consumer> {
    tracing::info!("Task start");
    let mut stream = consumer.stream();
    let mut rows = 0;
    let mut messages = 0;
    let mut taos = target_pool.get().await?;
    let target_is_v3 = taos
        .exec("desc information_schema.ins_databases")
        .await
        .is_ok();
    let metrics = metrics_arc.tmq();
    let refresh_progress_interval =
        crate::utils::interval::IntervalLimit::new(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::warn!("Sync cancelled");
                break;
            }
            next = stream.try_next() => {
                if let Some((offset, message)) = next.with_context(|| format!("[{id}] polling next message error"))? {
                    sync_msg(
                        topic,
                        &consumer,
                        id,
                        offset,
                        message,
                        &mut messages,
                        &mut rows,
                        metrics,
                        source_pool,
                        target_pool,
                        &mut taos,
                        table,
                        actions,
                        with_meta_delete,
                        with_meta_drop,
                        target_is_v3
                    )
                        .in_current_span()
                        .await?;
                    if refresh_progress_interval.ticked() {
                        update_progress(&consumer, metrics).await;
                    }

                } else {
                    break;
                }
            }
        }
    }
    update_progress(&consumer, metrics).await;
    tracing::info!("Task done");

    // do not drop consumer when single task done.
    drop(stream);
    // let _ = sender.send(consumer); // tokio send
    Ok(consumer)
}

enum InterlaceItem {
    Message(RawMessage),
    Block(RawBlock),
    Commit,
}
#[instrument(skip_all, fields(consumer.id = id, table))]
async fn sync_interlace(
    topic: &Arc<Topic>,
    id: usize,
    consumer: Consumer,
    source_pool: &TaosPool,
    target_pool: &TaosPool,
    table: Option<&str>,
    cancel: CancellationToken,
    metrics_arc: Arc<CoreMetrics>,
    options: &WriteOptions,
) -> Result<Consumer> {
    tracing::info!("Task start");
    // let mut stream = consumer.stream_with_timeout(Timeout::from_secs(5).into());
    let taos = target_pool.get().await?;
    let _target_is_v3 = taos
        .exec("desc information_schema.ins_databases")
        .await
        .is_ok();
    let metrics = metrics_arc.tmq();

    let chunk_size = std::env::var("TMQ_COMMIT_CHUNK_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);
    let mut last_type = 0;

    use std::str::FromStr;
    let mut join_set = tokio::task::JoinSet::new();
    let (msg_tx, msg_rx) = flume::bounded::<InterlaceItem>(1024 * 8);
    let (res_tx, res_rx) = flume::bounded(chunk_size);

    let table = table.map(|str| FastStr::from_str(str).unwrap());
    for wid in 0..options.concurrency {
        let worker = Worker {
            source: source_pool.clone(),
            target: target_pool.clone(),
            target_connection: None,
            table: table.clone(),
            sender: res_tx.clone(),
            options: options.clone(),
            metrics: metrics_arc.clone(),
            topic: topic.clone(),
        };
        let msg_rx = msg_rx.clone();
        join_set.spawn(
            async move {
                let mut worker = worker;
                let mut mid = 0;
                let mut last_error = None;
                while let Ok(message) = msg_rx.recv_async().await {
                    match message {
                        InterlaceItem::Message(mut message) => {
                            mid += 1;
                            let rows = message.rows();
                            let bytes = message.raw.raw_len();
                            tracing::debug!(mid, rows, bytes, "Received message");
                            let res = worker
                                .write(&mut message)
                                .instrument(tracing::debug_span!("write", mid))
                                .await;
                            let _ = worker.sender.send_async(res).await;
                        }
                        InterlaceItem::Block(mut block) => {
                            mid += 1;
                            tracing::debug!(mid, rows = block.nrows(), "Received block");
                            let res = worker
                                .write_block(&mut block)
                                .instrument(tracing::debug_span!("write_block", mid))
                                .await;
                            // let _ = worker.sender.send_async(res).await;
                            if let Err(err) = res {
                                last_error.replace(err);
                            }
                        }
                        InterlaceItem::Commit => {
                            tracing::debug!("Received commit item");
                            let res = last_error.take().map_or_else(|| Ok(()), Err);
                            let _ = worker.sender.send_async(res).await;
                        }
                    }
                }
                tracing::info!("Worker done");
            }
            .instrument(tracing::info_span!("worker", wid)),
        );
    }

    let mut last_offset = None;
    let mut chunk_len = 0;
    let mut table_blocks: HashMap<String, RawBlock> = HashMap::with_capacity(1024);

    macro_rules! clean_cache {
        () => {
            for (_, raw) in table_blocks.drain() {
                msg_tx.send_async(InterlaceItem::Block(raw)).await?;
            }
            msg_tx.send_async(InterlaceItem::Commit).await?;
            res_rx.recv_async().await??;

            #[allow(unused_assignments)]
            {
                chunk_len = 0;
            }
        };
    }

    let now = std::time::Instant::now();
    // println!("# [{id}] Start consuming at {}", chrono::Local::now());
    tracing::info!("Start consuming");
    // For message timeout
    let mut last_message = std::time::Instant::now();
    // For commit timeout
    let mut last_commit = std::time::Instant::now();
    let max_interlace_rows = 1024;
    // let mut blocks
    loop {
        if cancel.is_cancelled() {
            tracing::info!("Sync cancelled");
            break;
        }
        if let Some((offset, message)) = consumer.recv_timeout(Timeout::from_secs(1)).await? {
            let message_type = match &message {
                MessageSet::Meta(_) => 1,
                MessageSet::Data(_) => 2,
                MessageSet::MetaData(_, _) => 3,
            };
            metrics.add_messages(1);
            let message = options
                .parse_message(&message, metrics)
                .in_current_span()
                .await?;
            if message_type == 1 {
                clean_cache!();
                // meta only, sync immediately.
                msg_tx.send_async(InterlaceItem::Message(message)).await?;
                res_rx.recv_async().await??;
                consumer.commit(offset).await?;
                println!("{}", metrics);
                last_commit = std::time::Instant::now();
                last_message = std::time::Instant::now();
                continue;
            }

            if (last_type != 0 && last_type != message_type)
                || last_commit.elapsed() > Duration::from_secs(15)
            {
                clean_cache!();
                // meta only, sync immediately.
                msg_tx.send_async(InterlaceItem::Message(message)).await?;
                res_rx.recv_async().await??;
                consumer.commit(offset).await?;
                last_commit = std::time::Instant::now();
            } else {
                let data = message
                    .data
                    .ok_or_else(|| anyhow::anyhow!("No blocks found in data message"))?;
                for raw in data {
                    let table = raw.table_name().ok_or_else(|| {
                        anyhow::anyhow!("Query topic does not support interlace mode")
                    })?;

                    if raw.nrows() >= max_interlace_rows {
                        // todo: send block to worker
                        msg_tx.send_async(InterlaceItem::Block(raw)).await?;
                        chunk_len += 1;
                        continue; // next block
                    }

                    if !table_blocks.contains_key(table) {
                        table_blocks.insert(table.to_string(), raw);
                        continue; // next block
                    }

                    let (name, lhs) = table_blocks.remove_entry(table).unwrap();
                    let raw = lhs.concat(&raw);
                    if raw.nrows() >= max_interlace_rows {
                        // todo: send block to worker

                        msg_tx.send_async(InterlaceItem::Block(raw)).await?;
                        chunk_len += 1;
                        continue; // next block
                    } else {
                        table_blocks.insert(name, raw);
                    }
                }
                last_offset.replace(offset);
            }
            last_type = message_type;
            last_message = std::time::Instant::now();
        } else {
            if last_message.elapsed() > Duration::from_secs(5) {
                clean_cache!();
            }
            if last_commit.elapsed() > Duration::from_secs(5) {
                tracing::info!("Polling timeout, commit immediately");
                break;
            }
        }
    }
    println!(
        "# [{id}] Consume done after {:?}, waiting for writers to finish",
        now.elapsed()
    );
    tracing::info!(elapse = ?now.elapsed(), "Consume done, waiting for writers to finish");
    if chunk_len > 0 {
        clean_cache!();
        consumer.commit(last_offset.unwrap()).await?;
    }
    update_progress(&consumer, metrics).await;
    tracing::info!("Task done");

    // do not drop consumer when single task done.
    // drop(stream);
    // let _ = sender.send(consumer); // tokio send
    Ok(consumer)
}

#[instrument(skip_all, fields(consumer.id = id, table))]

async fn sync_concurrently(
    topic: &Arc<Topic>,
    id: usize,
    consumer: Consumer,
    source_pool: &TaosPool,
    target_pool: &TaosPool,
    table: Option<&str>,
    cancel: CancellationToken,
    metrics_arc: Arc<CoreMetrics>,
    options: &WriteOptions,
) -> Result<Consumer> {
    tracing::info!("Task start");
    let taos = target_pool.get().await?;
    let _target_is_v3 = taos
        .exec("desc information_schema.ins_databases")
        .await
        .is_ok();
    let metrics = metrics_arc.tmq();

    let chunk_size = options.commit_chunk_size;
    let mut last_type = 0;

    use std::str::FromStr;
    let mut join_set = tokio::task::JoinSet::new();
    let (msg_tx, msg_rx) = flume::bounded::<RawMessage>(options.concurrency * 2);
    let (res_tx, res_rx) = flume::bounded(chunk_size);

    let table = table.map(|str| FastStr::from_str(str).unwrap());
    for wid in 0..options.concurrency {
        let worker = Worker {
            source: source_pool.clone(),
            target: target_pool.clone(),
            target_connection: None,
            table: table.clone(),
            sender: res_tx.clone(),
            options: options.clone(),
            metrics: metrics_arc.clone(),
            topic: topic.clone(),
        };
        let msg_rx = msg_rx.clone();
        join_set.spawn(
            async move {
                let mut worker = worker;

                while let Ok(mut message) = msg_rx.recv_async().await {
                    let rows = message.rows();
                    let bytes = message.raw.raw_len();
                    tracing::debug!(
                        mid = message.mid,
                        metadata = message.meta.is_some() && message.data.is_some(),
                        rows,
                        bytes,
                        "Received message"
                    );
                    let now = std::time::Instant::now();
                    let res = worker.write(&mut message).in_current_span().await;
                    let elapse = now.elapsed();
                    worker
                        .metrics
                        .as_ref()
                        .tmq()
                        .add_write_cost_ms(elapse.as_millis() as _);
                    let _ = worker.sender.send_async(res).await;
                }
                tracing::info!("Worker done");
            }
            .instrument(tracing::info_span!("worker", wid)),
        );
    }

    let mut last_offset = None;
    let mut chunk_len = 0;

    macro_rules! clean_cache {
        () => {
            for _ in 0..chunk_len {
                res_rx.recv_async().await??;
            }

            #[allow(unused_assignments)]
            {
                chunk_len = 0;
            }
        };
    }

    let now = std::time::Instant::now();
    // println!("# [{id}] Start consuming at {}", chrono::Local::now());
    tracing::info!("Start consuming");
    let mut per_message_instant = std::time::Instant::now();

    const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
    let poll_interval = if options.max_polling_timeout < DEFAULT_POLL_INTERVAL {
        options.max_polling_timeout
    } else {
        DEFAULT_POLL_INTERVAL
    };
    let refresh_progress_interval =
        crate::utils::interval::IntervalLimit::new(Duration::from_secs(1));
    loop {
        if cancel.is_cancelled() {
            tracing::info!("Sync cancelled");
            break;
        }
        drop(last_offset.take());
        if let Some((offset, message)) = consumer
            .recv_timeout(Timeout::from_millis(poll_interval.as_millis() as _))
            .await?
        {
            if refresh_progress_interval.ticked() {
                update_progress(&consumer, metrics).await;
            }
            metrics.add_consume_cost_ms(per_message_instant.elapsed().as_millis() as _);

            let message_type = match &message {
                MessageSet::Meta(_) => 1,
                MessageSet::Data(_) => 2,
                MessageSet::MetaData(_, _) => 3,
            };
            metrics.add_messages(1);
            let raw = options
                .parse_message(&message, metrics)
                .in_current_span()
                .await?;
            drop(message);
            if message_type == 1 {
                clean_cache!();
                // meta only, sync immediately.
                msg_tx.send_async(raw).await?;
                res_rx.recv_async().await??;
                consumer.commit(offset).await?;
                per_message_instant = std::time::Instant::now();
                continue;
            }

            if (last_type != 0 && last_type != message_type) || chunk_len >= chunk_size {
                clean_cache!();

                // meta only, sync immediately.
                msg_tx.send_async(raw).await?;
                res_rx.recv_async().await??;
                consumer.commit(offset).await?;
            } else {
                msg_tx.send_async(raw).await?;
                chunk_len += 1;
                last_offset.replace(offset);
            }
            last_type = message_type;
            per_message_instant = std::time::Instant::now();
        } else {
            // No message received
            let elapsed = per_message_instant.elapsed();
            if elapsed.as_millis() as u64 >= options.commit_interval_ms {
                clean_cache!();
                if let Some(offset) = last_offset.take() {
                    consumer.commit(offset).await?;
                }
            }

            if elapsed > options.max_polling_timeout {
                tracing::info!(
                    "Polling timeout ({:?} > {:?}), commit immediately",
                    elapsed,
                    options.max_polling_timeout
                );
                break;
            }
        }
    }
    println!(
        "# [{id}] Consume done after {:?}, waiting for writers to finish",
        now.elapsed()
    );
    tracing::info!(elapse = ?now.elapsed(), "Consume done, waiting for writers to finish");
    if chunk_len > 0 {
        clean_cache!();
        if let Some(last_offset) = last_offset {
            consumer.commit(last_offset).await?;
        }
    }
    update_progress(&consumer, metrics).await;
    tracing::info!("Task done");
    // let _ = sender.send(consumer); // tokio send
    Ok(consumer)
}

async fn update_progress(consumer: &Consumer, metrics: &TmqMetrics) {
    let assignments = consumer.assignments().await;
    match assignments {
        Some(assignments) => {
            if !assignments.is_empty() {
                metrics.update_progress(assignments);
            }
        }
        None => {
            tracing::warn!("Failed to get assignments");
        }
    }
}

#[instrument(skip_all)]
pub async fn tmq_to_td(
    from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    jobs: usize,
    cancel: CancellationToken,
    task_id: Option<String>,
    notify: crate::TaskNotifySender,
) -> Result<()> {
    let (mut from, builder, topics, with_meta_delete, with_meta_drop) = check_tmq_dsn(from).await?;

    let jobs = from
        .remove("read_concurrency")
        .or(from.remove("num.of.consumers"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(jobs); // 0 means auto
    let strategy = from
        .remove("prefer")
        .map(|s| s.into())
        .unwrap_or_else(WriteStrategy::from_env);
    let concurrency = from
        .remove("write_concurrency")
        .or(from.remove("num.of.writers"))
        .or(std::env::var("TMQ_WRITE_CONCURRENCY").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0); // 0 means auto, should be set after.
    let commit_chunk_size = from
        .remove("commit.chunk.size")
        .or(std::env::var("TMQ_COMMIT_CHUNK_SIZE").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(512);
    let commit_interval_ms = from
        .remove("commit.interval.ms")
        .or(std::env::var("TMQ_COMMIT_INTERVAL_MS").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);

    let max_polling_timeout = from
        .remove("max.polling.timeout")
        .or(from.remove("timeout")) // for compatibility
        .or(std::env::var("TMQ_MAX_POLLING_TIMEOUT").ok())
        .map(|s| parse_timeout_duration(&s))
        .transpose()?
        .unwrap_or_else(|| Duration::from_secs(5));
    let mut options = WriteOptions {
        with_meta_delete,
        with_meta_drop,
        strategy,
        concurrency,
        commit_chunk_size,
        commit_interval_ms,
        max_polling_timeout,
        actions: Arc::new(actions.to_owned()),
        mid: Arc::new(AtomicUsize::new(0)),
    };

    let version = builder.server_version().await?.to_owned();
    tracing::debug!("Source version: {version}");
    // auto generate group.id if not exists
    let mut from_params = from.drain_params();
    if !from_params.contains_key("group.id") {
        let to_params = to.drain_params();
        if let Some(v) = to_params.get("token") {
            to.set("token", v);
        }
        let group_id = group_id_hash(&from, &to);
        tracing::info!("group.id not set, will use automatically generated group id: {group_id}");
        from_params.insert("group.id".to_string(), group_id);
        to.params = to_params;
    }
    from.params = from_params;
    let metrics_arc = get_metrics_arc(task_id.clone()).await;
    let metrics = metrics_arc.tmq();
    metrics.topics.fetch_add(topics.len() as _, SeqCst);

    let mut join_set = tokio::task::JoinSet::new();
    let mut consumer_task_id = 0;
    let target_database = to.subject.take();

    let target_builder = TaosBuilder::from_dsn(&to)?;
    let target_version = target_builder.server_version().await?.to_owned();
    {
        let source_version = semver::Version::parse(&version.split('.').take(3).join("."))?;
        let target_version = semver::Version::parse(&target_version.split('.').take(3).join("."))?;
        if source_version >= VERSION_3_3_0 && target_version < VERSION_3_3_0 {
            bail!("Source version is 3.3.0 or later, but target version is earlier than 3.3.0, which is not supported.");
        }
    }

    let source_pool = TaosBuilder::from_dsn(&from)?.pool()?;
    let target_pool = target_builder.pool()?;

    // check if the from database and the targe database have the same precision for each topic
    tracing::debug!("Check precision of source and target database.");
    for topic in &topics {
        let source_database = &topic.database;
        let target_database = if let Some(target) = target_database.as_ref() {
            target
        } else {
            source_database
        };
        let target_taos = target_pool.get().await?;
        let precision_of_to = target_taos.query_one::<String, String>(format!("select `precision` from information_schema.ins_databases where name='{target_database}'")).await;
        if let Err(err) = precision_of_to {
            // 可能因为当前用户没有权限查询 information_schema, 此时忽略错误。
            tracing::debug!(err = ?err, "Get precision of target database {target_database} failed.");
            continue;
        }
        let precision_of_to = precision_of_to.unwrap();
        if precision_of_to.is_none() {
            // 可能因为目标数据库不存在，此时会自动创建和源库相同精度的数据库,也忽略。
            tracing::debug!("Get precision of target database {target_database} failed: None");
            continue;
        }
        let precision_of_to = precision_of_to.unwrap();
        tracing::debug!("Precision of target database {target_database}: {precision_of_to}");
        let source_taos = source_pool.get().await?;
        let precision_of_from = source_taos.query_one::<String, String>(format!("select `precision` from information_schema.ins_databases where name='{source_database}'")).await;
        if let Err(err) = precision_of_from {
            tracing::debug!(err = ?err, "Get precision of source database {source_database} failed.");
            continue;
        }
        let precision_of_from = precision_of_from.unwrap();
        if precision_of_from.is_none() {
            tracing::debug!("Get precision of source database {source_database} failed: None");
            continue;
        }
        let precision_of_from = precision_of_from.unwrap();
        tracing::debug!("precision of source database {source_database}: {precision_of_from}");
        if precision_of_from != precision_of_to {
            bail!("The precision of the source database {source_database} and the target database {target_database} are different: source={precision_of_from}, to={precision_of_to}");
        }
    }
    tracing::debug!("Precision check done");

    let target_taos = target_pool.get().await?;
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

        // Jobs should be less than or equal to vgroups and greater than 0.
        let jobs = if jobs == 0 || jobs >= topic.vgroups {
            topic.vgroups
        } else {
            jobs
        };

        // If concurrency is 0, use available_parallelism * 2 / jobs
        if options.concurrency == 0 {
            options.concurrency = std::thread::available_parallelism()
                .map_or_else(|_| 8, |n| n.get() * 2 / jobs)
                .max(2);
        }
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
                            sql.push_str(&new);
                        }
                        Action::RenameSuperTable(action) => {
                            let name = table.stable.as_deref().unwrap();
                            let new = sql.replace(&format!("`{name}`",), &action.apply(name)?);
                            sql.clear();
                            sql.push_str(&new);
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
                            sql.push_str(&new);
                        }
                        let name = &table.table;
                        let new = sql.replace(&format!("`{name}`",), &action.apply(name)?);
                        sql.clear();
                        sql.push_str(&new);
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

        let tmq = Arc::new(tmq);
        let topic = Arc::new(topic);
        for mut consumer in consumers {
            let tmq = tmq.clone();
            let topic = topic.clone();
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
            let sender: tokio::sync::mpsc::UnboundedSender<Consumer> = consumers_sender.clone();
            let source_pool = source_pool.clone();
            let metrics_arc = metrics_arc.clone();
            let notify = notify.clone();
            let target = target.clone();
            let options = options.clone();
            join_set.spawn(
                async move {
                    let mut retries = 0;
                    let max_retries = 5; // max retries in 1m
                    let tick = IntervalLimit::new(Duration::from_secs(60));
                    loop {
                        // let taos = target.get().await?;
                        match if strategy.prefer_interlace() {
                            sync_interlace(
                                &topic,
                                consumer_task_id,
                                consumer,
                                &source_pool,
                                &target,
                                table.as_deref(),
                                cancellation.clone(),
                                metrics_arc.clone(),
                                &options,
                            )
                            .await
                        } else if actions.is_empty() {
                            sync_concurrently(
                                &topic,
                                consumer_task_id,
                                consumer,
                                &source_pool,
                                &target,
                                table.as_deref(),
                                cancellation.clone(),
                                metrics_arc.clone(),
                                &options,
                            )
                            .await
                        } else {
                            sync(
                                &topic,
                                consumer_task_id,
                                consumer,
                                &source_pool,
                                &target,
                                table.as_deref(),
                                &actions,
                                cancellation.clone(),
                                metrics_arc.clone(),
                                with_meta_delete,
                                with_meta_drop,
                            )
                            .await
                        } {
                            Ok(consumer) => {
                                let _ = sender.send(consumer);
                                break;
                            }
                            Err(err) => {
                                let err_str = format!("{err:#}");
                                if !(err_str.contains("0xE001")
                                    || err_str.contains("0xE002")
                                    || err_str.contains("0xE003"))
                                {
                                    // 0xE001 is the error code for "Connection refused"
                                    // 0xE002 is the error code for "Connection reset without closing handshake"
                                    return Err(err);
                                }
                                if retries > max_retries {
                                    tracing::error!("Consumer error: {err:#}");
                                    return Err(err);
                                }
                                let _ = notify
                                    .send_async(crate::TaskNotify::Warn(format!(
                                        "Consuming task {consumer_task_id} error: {err:#}"
                                    )))
                                    .await;
                                consumer = tmq.build().await?;
                                consumer.subscribe([topic.name.as_str()]).await?;
                                tokio::time::sleep(Duration::from_secs(retries * 2)).await;
                                tracing::warn!(retries, "Consumer error: {err:#}, retrying...");
                                if tick.ticked() {
                                    retries = 0;
                                } else {
                                    retries += 1;
                                }
                            }
                        }
                    }
                    anyhow::Ok(())
                }
                .in_current_span(),
            );
            tracing::info!("Spawn consuming task with id {consumer_task_id}",);
            consumer_task_id += 1;
        }
    }

    tracing::info!("Spawn consuming tasks {}", join_set.len());
    while let Some(res) = join_set.join_next().await {
        if let Err(err) = res.map_err(anyhow::Error::from).and_then(|r| r) {
            tracing::error!("Task error: {err:#}");
            join_set.abort_all();
            println!("{}", metrics);
            return Err(err);
        }
    }
    println!("# Syncing done at {}", chrono::Local::now());
    tracing::info!("Stop all consumers({})", consumer_task_id);
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
    drop(target_pool);
    drop(source_pool);
    drop(builder);
    tokio::time::sleep(Duration::from_millis(1000)).await;
    tracing::info!("replication done.");
    println!("{}", metrics);
    Ok(())
}

#[instrument(skip_all)]
pub async fn tmq_offsets(from: Dsn) -> anyhow::Result<LinkedHashMap<String, Vec<Assignment>>> {
    let (from, _, topics, _, _) = check_tmq_dsn(from).await?;
    let tmq = TmqBuilder::from_dsn(&from)?;
    let mut consumer = tmq.build().await?;
    consumer
        .subscribe(topics.iter().map(|t| t.name.to_string()).collect_vec())
        .await?;
    Ok(consumer
        .assignments()
        .await
        .unwrap_or_default()
        .into_iter()
        .collect())
}

#[derive(Debug, Serialize)]
pub struct TableProgress {
    pub table_name: String,
    pub from_last_ts: Option<u64>,
    pub to_last_ts: Option<u64>,
    pub from_count: u64,
    pub to_count: u64,
}
#[instrument(skip_all)]
pub async fn get_table_progress(
    from: &str,
    to: &str,
    // format db.table
    table: &str,
    start: Option<&String>,
    end: Option<&String>,
) -> anyhow::Result<TableProgress> {
    let mut from: Dsn = from.parse()?;
    let _ = from.remove("use.topic.name");
    let _ = from.remove("use.table.name");
    let _ = from.remove("with.meta.delete");
    let _ = from.remove("with.meta.drop");
    let (from_db, table) = table
        .split_once('.')
        .ok_or(anyhow!("Invalid table format"))?;
    from.subject.replace(from_db.to_string());
    let to: Dsn = to.parse()?;
    let to_db = to
        .subject
        .clone()
        .ok_or(anyhow!("No database found in target dsn"))?;
    let from_builder = TaosBuilder::from_dsn(&from)?;
    let to_builder = TaosBuilder::from_dsn(&to)?;
    let from_taos = from_builder.build().await?;
    from_taos.use_database(from_db).await?;
    let to_taos = to_builder.build().await?;

    let (from_sql, to_sql) = if let Some(start) = start {
        if let Some(end) = end {
            (format!("SELECT last(_c0), count(*) FROM `{from_db}`.`{table}` where _c0 > '{start}' and _c0 < '{end}'"),
            format!("SELECT last(_c0), count(*) FROM `{to_db}`.`{table}` where _c0 > '{start}' and _c0 < '{end}'"))
        } else {
            (
                format!(
                    "SELECT last(_c0), count(*) FROM `{from_db}`.`{table}` where _c0 > '{start}'"
                ),
                format!(
                    "SELECT last(_c0), count(*) FROM `{to_db}`.`{table}` where _c0 > '{start}'"
                ),
            )
        }
    } else if let Some(end) = end {
        (
            format!("SELECT last(_c0), count(*) FROM `{from_db}`.`{table}` where _c0 < '{end}'"),
            format!("SELECT last(_c0), count(*) FROM `{to_db}`.`{table}` where _c0 < '{end}'"),
        )
    } else {
        (
            format!("SELECT last(_c0), count(*) FROM `{from_db}`.`{table}`"),
            format!("SELECT last(_c0), count(*) FROM `{to_db}`.`{table}`"),
        )
    };
    tracing::debug!("\nfrom_sql: {from_sql}\nto_sql: {to_sql}");
    let from_result = from_taos
        .query_one::<String, (Option<u64>, u64)>(from_sql)
        .await;
    if let Err(err) = from_result {
        tracing::error!("Query from source database error: {err}");
        bail!(err);
    }
    let to_result = to_taos
        .query_one::<String, (Option<u64>, u64)>(to_sql)
        .await;
    if let Err(err) = to_result {
        tracing::error!("Query to target database error: {err}");
        bail!(err);
    }
    let from_result = from_result.unwrap().unwrap();
    let to_result = to_result.unwrap().unwrap();
    Ok(TableProgress {
        table_name: table.to_string(),
        from_last_ts: from_result.0,
        to_last_ts: to_result.0,
        from_count: from_result.1,
        to_count: to_result.1,
    })
}

fn parse_timeout_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    if matches!(s, "never" | "0" | "-1") {
        Ok(Duration::MAX)
    } else {
        parse_duration(s)
            .inspect_err(|e| {
                tracing::warn!(
                    key = "max.polling.timeout",
                    value = s,
                    "parse max.polling.timeout error: {e}",
                );
            })
            .context("parse max.polling.timeout error")
            .map(|d| if d.is_zero() { Duration::MAX } else { d })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_parse_duration_test() {
        assert_eq!(parse_timeout_duration("0").unwrap(), Duration::MAX);
        assert_eq!(parse_timeout_duration("never").unwrap(), Duration::MAX);
        assert_eq!(parse_timeout_duration("-1").unwrap(), Duration::MAX);

        assert_eq!(parse_timeout_duration("0s").unwrap(), Duration::MAX);
        assert_eq!(parse_timeout_duration("0ms").unwrap(), Duration::MAX);
        assert_eq!(parse_timeout_duration("0ns").unwrap(), Duration::MAX);
        assert_eq!(parse_timeout_duration("0m").unwrap(), Duration::MAX);

        assert_eq!(
            parse_timeout_duration("10s").unwrap(),
            Duration::from_secs(10)
        );
        assert_eq!(
            parse_timeout_duration("10ms").unwrap(),
            Duration::from_millis(10)
        );
        assert_eq!(
            parse_timeout_duration("10m").unwrap(),
            Duration::from_secs(10 * 60)
        );
    }
}
