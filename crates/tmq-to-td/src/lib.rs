mod worker;

use anyhow::{Context, anyhow, bail};
use faststr::FastStr;
use humantime::parse_duration;
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering::SeqCst;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};
use taos::taos_query::tmq::Assignment;
use taos::*;
use taosx_core::{TaskNotify, TaskNotifySender};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, instrument};
use worker::*;
use worker::{Worker, WriteOptions};

use taosx_core::{
    Action,
    core_metrics::{CoreMetrics, TaskMetrics, get_metrics_arc_or},
    tmq::{tmq_metric::TmqMetrics, *},
    utils::{
        constants::{VERSION_3_3_0, VERSION_3_3_6},
        dsn::json_to_dsn,
        interval::IntervalLimit,
    },
};

use legacy_to_taos::sync_super_table_schema;

async fn migrate_data_schema(desc: &[Field], to: &Taos, table: &str) -> anyhow::Result<bool> {
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
    target: &TaosPool,
    taos: &Taos,
    table: Option<&str>,
    actions: &[Action],
    data: &Data,
    target_is_v3: bool,
    metrics: &TmqMetrics,
) -> anyhow::Result<u64> {
    tracing::debug!("Start writing data");
    metrics.add_messages_of_data(1);
    let mut last_error = None;
    if target_is_v3 && !topic.is_query() && actions.is_empty() {
        let raw = data
            .as_raw_data()
            .await
            .context("Data source raw data error")?;
        metrics.add_message_bytes(raw.raw_len() as _);
        if let Err(err) = taos.write_raw_meta(&raw).await {
            let code = *err.code().deref();
            match code {
                // Table not exist error codes or invalid input.
                0x070F | 0x0218 | 0x2603 | 0x036D | 0x0618 | 0x2662 | 0x0118 | 0x4000 | 0x060B => {
                    // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                    // 0x070F: invalid input
                    // 0x0218: the table does not exist
                    // 0x2603: the table does not exist
                    // 0x036D: the table does not exist
                    // 0x0618: the table does not exist
                    // 0x2662: the table does not exist
                    // 0x0118: invalid parameter
                    // 0x4000: invalid msg
                    // 0x060B: the primary timestamp out of range
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

    // fallback to block-by-block method
    let mut has_blocks = false;
    while let Some(mut raw) = data
        .fetch_raw_block()
        .await
        .context("Fetch raw block error")?
    {
        has_blocks = true;

        let source_table_name = raw
            .table_name()
            .filter(|name| !name.is_empty())
            .map(|s| s.to_owned());
        tracing::debug!(
            "try to write raw block: {}",
            raw.pretty_format().to_string()
        );
        if let Some(name) = table {
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
        } else if let Some(name) = raw.table_name() {
            let mut name = name.to_string();
            for action in actions {
                match action {
                    Action::RenameTable(rename) | Action::RenameChildTable(rename) => {
                        rename.apply_in_place(&mut name)?
                    }
                    _ => (),
                }
            }
            if !actions.is_empty() {
                raw.with_table_name(&name);
            }
            tracing::debug!(
                "write into {name} {} rows(total {}) with {} columns",
                raw.nrows(),
                rows,
                raw.ncols()
            );
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
            worker::write_with_raw_block(
                actions,
                topic,
                source,
                target,
                taos,
                &raw,
                source_table_name.as_deref(),
                metrics,
            )
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
                let question_masks = std::iter::repeat_n('?', fields.len()).join(",");
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
                let raw = data.as_raw_data().await?;
                metrics.add_message_bytes(raw.raw_len() as _);
                if let Err(err) = taos.write_raw_meta(&raw).await {
                    let errstr = err.to_string();
                    if errstr.contains("[0x032C]")
                        || errstr.contains("[0x0115]")
                        || errstr.contains("[0x0603]")
                        || errstr.contains("[0x03C7]")
                    {
                        // 0x032C: object is creating
                        // 0x0115: invalid msg
                        // 0x0603: table already exists
                        // 0x03C7: stable uid not match
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
) -> anyhow::Result<()> {
    let cur = metrics.add_messages_of_meta(1);
    let mut json_meta = match meta.as_json_meta().await.context("Fetch json meta error") {
        Ok(json_meta) => json_meta,
        Err(err) => {
            // Without fallback.
            tracing::debug!("Can't get json meta: {err}");

            if actions.is_empty() {
                if target_is_v3 {
                    let raw_meta = meta.as_raw_meta().await?;
                    metrics.add_message_bytes(raw_meta.raw_len() as _);
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
            metrics.add_message_bytes(raw_meta.raw_len() as _);
            if let Err(err) = taos.write_raw_meta(&raw_meta).await {
                metrics.add_write_raw_fails(1);
                // Print error no matter how we will deal with it, so that we can know what happened.
                tracing::debug!("Write raw meta error: {err:#}");
                let code = *err.code().deref();
                match code {
                    // Table not exist error codes.
                    0x0218 | 0x2603 | 0x036D | 0x0618 => {
                        // 0x0218: the table does not exist
                        // 0x2603: the table does not exist
                        // 0x036D: the table does not exist
                        // 0x0618: the table does not exist
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
                                        tracing::warn!(
                                            "Table does not exist: {using} while create child {table_name}. Sync super table schema."
                                        );
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
                        // 0x032C: object is creating
                        // 0x0115: invalid msg
                        // 0x0603: table already exists
                        // 0x03C7: stable uid not match
                        // 0x03D3: conflict transaction not completed
                        // do nothing
                    }
                    _ => {
                        // Fallback to sql method.
                        tracing::debug!("Fallback to sql method due to: {err:#}.");
                        let sqls = json_meta.iter().map(ToString::to_string).collect_vec();
                        execute_many_sql(taos, sqls)
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
                    // 0x032C: object is creating
                    // 0x03D3: conflict transaction not completed
                    // 0x0115: invalid msg
                    // 0x0603: table already exists
                    // 0x03C7: stable uid not match
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
) -> anyhow::Result<()> {
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
                    // 0xE00: connection error
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
                target_pool,
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
                    // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                    // 0xE00: connection error
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
                    // 0xE00: connection error
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
                    target_pool,
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
                        // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                        // 0xE00: connection error
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
    metrics.add_commits(1);
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
) -> anyhow::Result<Consumer> {
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

                } else {
                    break;
                }
            }
        }
    }
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
) -> anyhow::Result<Consumer> {
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
    println!("# [{id}] Start consuming at {}", chrono::Local::now());
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
) -> anyhow::Result<Consumer> {
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
                    let metrics = worker.metrics.as_ref().tmq();
                    metrics.add_write_cost_ms(elapse.as_millis() as _);
                    if res.is_ok() {
                        metrics.add_success_messages(1);
                    } else {
                        metrics.add_write_raw_fails(1);
                    }

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
    println!("# [{id}] Start consuming at {}", chrono::Local::now());
    tracing::info!("Start consuming");
    let mut per_message_instant = std::time::Instant::now();

    const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
    let poll_interval = if options.max_polling_timeout < DEFAULT_POLL_INTERVAL {
        options.max_polling_timeout
    } else {
        DEFAULT_POLL_INTERVAL
    };
    let timeout = Timeout::from_millis(poll_interval.as_millis() as _);
    let async_loop = async {
        loop {
            if cancel.is_cancelled() {
                tracing::info!("Sync cancelled");
                break;
            }
            drop(last_offset.take());
            if let Some((offset, message)) = tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("Sync cancelled");
                    break;
                }
                res = consumer.recv_timeout(timeout) => {
                    res?
                }
            } {
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
                    metrics.add_commits(1);
                    per_message_instant = std::time::Instant::now();
                    continue;
                }

                if (last_type != 0 && last_type != message_type) || chunk_len >= chunk_size {
                    clean_cache!();

                    // meta only, sync immediately.
                    msg_tx.send_async(raw).await?;
                    res_rx.recv_async().await??;
                    consumer.commit(offset).await?;
                    metrics.add_commits(1);
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
                if elapsed.as_millis() as u64 >= options.commit_interval_ms && chunk_len > 0 {
                    clean_cache!();
                    if let Some(offset) = last_offset.take() {
                        if let Err(err) = consumer.commit(offset).await {
                            tracing::warn!(?err, "Commit error: {err}");
                        };
                        metrics.add_commits(1);
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
                if let Err(err) = consumer.commit(last_offset).await {
                    tracing::warn!(?err, "Final commit error: {err}");
                };
                metrics.add_commits(1);
            }
        }
        Ok(())
    }
    .in_current_span();

    match async_loop.await {
        Ok(_) => {
            tracing::info!("Task done");
            Ok(consumer)
        }
        Err(err) => {
            tracing::info!("Task failed, unsubscribe: {err}");
            consumer.unsubscribe().await;
            Err(err)
        }
    }
}

async fn update_progress(
    source_pool: &TaosPool,
    metrics: &TmqMetrics,
    topic: &String,
    group_id: &String,
) -> anyhow::Result<()> {
    let Ok(taos) = source_pool.get().await.inspect_err(|err| {
        tracing::error!("Failed to get taos connection by source pool, {:?}", err);
    }) else {
        return Ok(());
    };
    if let Ok(mut res) = taos
        .query(format!(
            "select * from information_schema.ins_subscriptions \
            where topic_name = '{topic}' and consumer_group = '{group_id}' \
            and consumer_id is not NULL"
        ))
        .await
        .inspect_err(|err| {
            tracing::warn!(cause = %err, "execute sql 'show subscriptions' error");
        })
    {
        #[derive(Deserialize)]
        struct SubscriptionInformation {
            consumer_id: Option<String>,
            vgroup_id: i32,
            user: Option<String>,
            fqdn: Option<String>,
            offset: Option<String>,
            rows: i64,
        }
        if let Ok(records) = res.deserialize().try_collect().await {
            let records: Vec<SubscriptionInformation> = records;

            if tracing::enabled!(tracing::Level::DEBUG) {
                let span = tracing::debug_span!("subscriptions", topic, group = group_id);
                let _entered = span.enter();
                tracing::debug!(
                    "| consumer_id | vgroup_id |   user   |   fqdn   |   offset   | rows |"
                );
                for r in &records {
                    tracing::debug!(
                        "| {:11} | {:8} | {:8} | {:8} | {:10} | {:4} |",
                        r.consumer_id.as_deref().unwrap_or(""),
                        r.vgroup_id,
                        r.user.as_deref().unwrap_or(""),
                        r.fqdn.as_deref().unwrap_or(""),
                        r.offset.as_deref().unwrap_or(""),
                        r.rows
                    );
                }
            }

            if records.is_empty() && metrics.progress.contains_key(topic.as_str()) {
                bail!("Consumer all dead for topic {topic} group {group_id}");
            }

            let assignments = records
                .iter()
                .filter_map(|r| {
                    if let Some(offset) = r.offset.as_deref().filter(|s| s.starts_with("wal:")) {
                        let parts: Vec<&str> =
                            offset.trim_start_matches("wal:").split('/').collect();
                        if parts.len() == 2 {
                            let part1 = parts[0];
                            let part2 = parts[1];
                            let offset = part1.parse::<i64>().unwrap_or(0);
                            let end = part2.parse::<i64>().unwrap_or(0);
                            return Some(Assignment::new(r.vgroup_id, offset, 0, end));
                        }
                    }
                    None
                })
                .collect_vec();
            if !assignments.is_empty() {
                metrics.update_progress_of_topic(topic, assignments);
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
pub async fn tmq_to_td(
    from: Dsn,
    actions: Vec<Action>,
    mut to: Dsn,
    cancel: CancellationToken,
    task_id: Option<String>,
    notify: TaskNotifySender,
) -> anyhow::Result<()> {
    let cancel = cancel.child_token();
    let _drop_guard = cancel.clone().drop_guard();
    let (mut from, builder, topics, with_meta_delete, with_meta_drop) = check_tmq_dsn(from).await?;

    // check if the source database has enabled wal
    if let Err(err) = check_wal_enabled(&builder, &topics).await {
        tracing::error!("check wal failed: {:#}", err);
        bail!(format!("check wal failed: {}", err));
    }

    let jobs = from
        .remove("read_concurrency")
        .or(from.remove("num.of.consumers"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0); // 0 means auto
    let strategy = from
        .remove("prefer")
        .map(|s| s.into())
        .unwrap_or_else(WriteStrategy::from_env);
    let concurrency = from
        .remove("write_concurrency")
        .or(from.remove("num.of.writers"))
        .or(std::env::var("TMQ_WRITE_CONCURRENCY").ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1); // 0 means auto, should be set after.
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
    let refresh_progress_interval = Duration::from_secs(
        from.remove("refresh.progress.interval")
            .or(std::env::var("REFRESH_PROGRESS_INTERVAL").ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(7),
    );

    const DEFAULT_ENABLE_CONCURRENT_POLLING: bool = true;
    let concurrent_polling = from
        .remove("enable.concurrent.polling")
        .or(std::env::var("TMQ_CONCURRENT_POLLING").ok())
        .and_then(|s| match s.as_str() {
            "" | "true" | "TRUE" | "T" | "1" => Some(true),
            "false" | "FALSE" | "F" | "0" => Some(false),
            _ => {
                tracing::warn!(
                    "Invalid value for enable.concurrent.polling: {s}, use default value: {}",
                    DEFAULT_ENABLE_CONCURRENT_POLLING
                );
                None
            }
        })
        .unwrap_or(DEFAULT_ENABLE_CONCURRENT_POLLING);
    if let Some(v) = from.remove("timeout") {
        let d = parse_timeout_duration(&v).context("parse timeout error")?;
        if d == Duration::MAX {
            from.set("timeout", "never");
        } else {
            from.set("timeout", v);
        }
    }
    let max_polling_timeout = from
        .remove("max.polling.timeout")
        .or(from.get("timeout").cloned()) // for compatibility
        .or(std::env::var("TMQ_MAX_POLLING_TIMEOUT").ok())
        .map(|s| parse_timeout_duration(&s))
        .transpose()?
        .unwrap_or_else(|| Duration::from_secs(60));
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
        let group_id = group_id_hash_by(&from, &to);
        tracing::info!("group.id not set, will use automatically generated group id: {group_id}");
        from_params.insert("group.id".to_string(), group_id);
        to.params = to_params;
    }
    from.params = from_params;
    let metrics_arc = get_metrics_arc_or(task_id.as_deref().and_then(|s| s.parse().ok()), || {
        Arc::new(CoreMetrics::TMQ(TmqMetrics::default()))
    })
    .await;
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
            bail!(
                "Source version is 3.3.0 or later, but target version is earlier than 3.3.0, which is not supported."
            );
        }

        // @huolinhe: Keep the code here, it's dangerous if source schema changes.
        // Jira: [TS-6672](https://jira.taosdata.com:18080/browse/TS-6672)

        if strategy.prefer_raw() && from.get("msg.consume.rawdata").is_some() {
            if source_version < VERSION_3_3_6 {
                tracing::warn!(
                    "Source version is earlier than 3.3.6, which does not support msg.consume.rawdata, will remove it from dsn."
                );
                from.remove("msg.consume.rawdata");
            }
            if target_version < VERSION_3_3_6 {
                tracing::warn!(
                    "Target version is earlier than 3.3.6, which does not support msg.consume.rawdata, will remove it from dsn."
                );
                from.remove("msg.consume.rawdata");
            }
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
            bail!(
                "The precision of the source database {source_database} and the target database {target_database} are different: source={precision_of_from}, to={precision_of_to}"
            );
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
                let mut retries = 20;
                loop {
                    match consumer.subscribe([&topic]).await {
                        Ok(_) => break,
                        Err(err) => {
                            tracing::warn!("Subscribe consumer {id} error: {err:#}");
                            if retries == 0 {
                                Err(err)?;
                            }
                            retries -= 1;
                            tokio::time::sleep(Duration::from_secs(3)).await;
                        }
                    }
                }
                anyhow::Ok(consumer)
            }));
        }

        for h in consumer_handles {
            let consumer = h.await??;
            consumers.push(consumer);
        }
        let duration = consumer_timer.elapsed();
        tracing::info!("Setup {} consumers in {:?}", jobs, duration);

        let from_params = from.params;
        let group_id = Arc::new(from_params.get("group.id").cloned().unwrap());

        let tmq = Arc::new(tmq);
        let topic = Arc::new(topic);
        let mut wg = awaitgroup::WaitGroup::new();
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
            let worker = wg.worker();
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
                        } else if concurrent_polling && actions.is_empty() {
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
                                    || err_str.contains("0xE003")
                                    || err_str.contains("0xE004")
                                    || err_str.contains("0xE00B"))
                                {
                                    // NOTICE 此方法不涉及 transform 配置，所以不进行“写入异常处理”
                                    // 0xE001: internal error
                                    // 0xE002: connection closed
                                    // 0xE003: send timeout
                                    // 0xE004: receive timeout
                                    // 0x000B: unable to establish connection
                                    return Err(err);
                                }
                                if retries > max_retries {
                                    tracing::error!("Consumer error: {err:#}");
                                    return Err(err);
                                }
                                let _ = notify
                                    .send_async(TaskNotify::source_error(format!(
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
                    tracing::info!("Consumer task {consumer_task_id} done",);
                    worker.done();
                    anyhow::Ok(())
                }
                .in_current_span(),
            );
            tracing::info!("Spawn consuming task with id {consumer_task_id}",);
            consumer_task_id += 1;
        }

        join_set.spawn({
            let group_id = group_id.clone();
            let topic = topic.clone();
            let metrics = metrics_arc.clone();
            let source_pool = source_pool.clone();
            let cancel = cancel.clone();
            async move {
                let mut interval = tokio::time::interval(refresh_progress_interval);
                let metrics = metrics.tmq();
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            update_progress(&source_pool, metrics, &topic.name, &group_id)
                                .await
                                .inspect_err(|err| {
                                    tracing::error!("TMQ process error: {err:#}");
                                })?;
                        }
                        _ = cancel.cancelled() => {
                            let _ = update_progress(&source_pool, metrics, &topic.name, &group_id)
                                .await
                                .inspect_err(|err| {
                                    tracing::error!("TMQ process error: {err:#}");
                                });
                            break Ok(());
                        }
                        _ = wg.wait() => {
                            tracing::info!("All consumers done");
                            let _ = update_progress(&source_pool, metrics, &topic.name, &group_id)
                                .await
                                .inspect_err(|err| {
                                    tracing::error!("TMQ process error: {err:#}");
                                });
                            break Ok(());
                        }
                    }
                }
            }
        });
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
    // let mut from: Dsn = from.parse()?;
    let mut from = json_to_dsn(&serde_json::Value::String(from.to_string()))?;
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
            (
                format!(
                    "SELECT last(_c0), count(*) FROM `{from_db}`.`{table}` where _c0 > '{start}' and _c0 < '{end}'"
                ),
                format!(
                    "SELECT last(_c0), count(*) FROM `{to_db}`.`{table}` where _c0 > '{start}' and _c0 < '{end}'"
                ),
            )
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

async fn execute_many_sql(conn: &Taos, sqls: Vec<String>) -> anyhow::Result<()> {
    for sql in sqls.iter() {
        conn.exec(sql)
            .in_current_span()
            .await
            .with_context(|| format!("failed to execute sql: {}", sql))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use taosx_core::utils::sql::connect_taos;

    /// # description
    /// Test case for real-time synchronization of a database using TMQ and TD.
    /// 1. Create DB_SRC and DB_DST databases, and create topic which subscribe the DB_SRC
    /// 2. Create a thread to start the sync task, which syncs DB_SRC to DB_DST
    /// 3. Create N threads, each thread writes T tables, each table writes BATCH_NUM times, and each time writes BATCH_SIZE rows of data
    /// 4. Check if the data in DB_SRC and DB_DST is consistent, if consistent, the test case passes, otherwise it fails
    /// # description_cn
    /// 实时同步数据库，指定写入模式
    /// 1. 创建数据库 DB_SRC 和 DB_DST；创建 topic，订阅 DB_SRC
    /// 2. 创建一个线程，启动同步任务，将 DB_SRC 同步到 DB_DST
    /// 3. 创建 N 个线程，每个线程写入 T 张表，每张表写入 BATCH_NUM 次，每次写入 BATCH_SIZE 条数据
    /// 4. 检查 DB_SRC 和 DB_DST 中的数据是否一致，一致则用例通过，否则用例失败
    /// # jira
    /// Close https://jira.taosdata.com:18080/browse/TD-32960
    /// # example
    /// ```shell
    /// cargo nextest run -p taosx_core test_realtime_sync_with_taos --no-capture --retries 0
    /// ```
    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn test_realtime_sync_with_taos() -> anyhow::Result<()> {
        let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = std::env::var("WS_ENABLE")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        const DB_SRC: &str = "td32960_005_src";
        const DB_DST: &str = "td32960_005_dst";
        const TOPIC: &str = "test_realtime_sync";
        const TID: u64 = 32960005;
        const THREADS: u64 = 10; // 写入的并发
        const TABLES: u64 = 100; // 每个线程写入 T 张表, 不要修改
        const BATCH_NUM: u64 = 250; // 每个表写入 BATCH_NUM 次
        const BATCH_SIZE: u64 = 400; // 每个 BATCH 写入 BATCH_SIZE 条数据, 不要修改

        // create database
        let taos = taosx_core::utils::sql::connect_taos(&host, ws_enable).await?;
        taos.exec_many(vec![
            format!("DROP TOPIC IF EXISTS force `{TOPIC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
            format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}`"),
            format!("CREATE DATABASE IF NOT EXISTS `{DB_DST}`"),
            format!("CREATE TOPIC `{TOPIC}` WITH META AS DATABASE `{DB_SRC}`"),
            format!("CREATE TABLE `{DB_SRC}`.stb (ts timestamp, val float) TAGS (id int)"),
        ])
        .await?;

        // create a realtime sync task
        let (tx, _rx) = flume::unbounded();
        let host_clone = host.clone();
        let sync_task = tokio::spawn(async move {
            let (from, to) = if ws_enable {
                let from = format!("tmq+ws://{host_clone}:6041/{TOPIC}")
                    .into_dsn()
                    .unwrap();
                let to = format!("taos+ws://{host_clone}:6041/{DB_DST}")
                    .into_dsn()
                    .unwrap();
                (from, to)
            } else {
                let from = format!("tmq://{host_clone}/{TOPIC}").into_dsn().unwrap();
                let to = format!("taos://{host_clone}/{DB_DST}").into_dsn().unwrap();
                (from, to)
            };
            tmq_to_td(
                from,
                vec![],
                to,
                CancellationToken::new(),
                Some(TID.to_string()),
                tx,
            )
            .await
        });

        // write data to DB_SRC concurrently
        let mut writers = vec![];
        let ts0 = Utc::now() - chrono::Duration::seconds((BATCH_NUM * BATCH_SIZE) as i64);
        for thres_idx in 0..THREADS {
            let host_clone = host.clone();
            let w = tokio::spawn(async move {
                let taos = taosx_core::utils::sql::connect_taos(&host_clone, ws_enable).await?;

                for batch_idx in 0..BATCH_NUM {
                    let mut sql = "insert into".to_string();
                    for t in 0..TABLES {
                        let table_idx = (thres_idx * TABLES) + t;
                        sql.push_str(&format!(" `{DB_SRC}`.t{table_idx} using `{DB_SRC}`.stb tags({table_idx}) values "));

                        for n in 0..BATCH_SIZE {
                            let ts = (ts0
                                + chrono::Duration::seconds((batch_idx * BATCH_SIZE + n) as i64))
                            .timestamp_millis();
                            sql.push_str(&format!("({ts}, {n}.{n})"));
                        }
                    }
                    // println!("batch idx: {batch_idx}, sql.len: {}", sql.len());
                    taos.exec(&sql).await?;
                }

                Ok::<(), anyhow::Error>(())
            });
            writers.push(w);
        }
        // wait for all writers to finish
        for w in writers {
            if let Err(err) = w.await? {
                tracing::error!("Write data error: {err:#}");
                return Err(err);
            }
        }

        // wait for the task to end
        sync_task.await??;

        // check data
        let count_src: u64 = taos
            .query_one(format!("select count(*) from `{DB_SRC}`.stb"))
            .await?
            .unwrap_or_default();
        let count_dst: u64 = taos
            .query_one(format!("select count(*) from `{DB_DST}`.stb"))
            .await?
            .unwrap_or_default();
        assert_eq!(count_src, THREADS * TABLES * BATCH_NUM * BATCH_SIZE);
        assert_eq!(count_dst, THREADS * TABLES * BATCH_NUM * BATCH_SIZE);

        // clean
        taos.exec_many(vec![
            format!("DROP TOPIC IF EXISTS force `{TOPIC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
        ])
        .await?;

        Ok(())
    }

    /// # description
    /// The 'timestamp out of range' rows should be lost when the DB_SRC's keep value is greater than the DB_DST's keep value.
    /// # description_cn
    /// 目标数据库的 keep 值小于源数据库的 keep，写入 timestamp out of range 的数据
    /// 1. 创建数据库 DB_SRC 和 DB_DST，DB_SRC 的 keep 为 10d，DB_DST 的 keep 为 7d；
    /// 2. 向 DB_SRC 中写入 10 行数据，每天一条；
    /// 3. 运行 tmq_to_td 任务；
    /// 4. 检查 DB_SRC 和 DB_DST 中的数据，DB_SRC 最早为 10d 前，DB_DST 最早为 7d 前，正确则用例通过，否则失败。
    /// # example
    /// ```shell
    /// cargo nextest run -p taosx-core test_timestamp_out_of_range_with_taos --no-capture --retries 0
    /// ```
    #[tokio::test]
    async fn test_timestamp_out_of_range_with_taos() -> anyhow::Result<()> {
        unsafe {
            std::env::set_var("RUST_LOG", "debug");
        }
        tracing_subscriber::fmt()
            .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
            .init();

        let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = std::env::var("WS_ENABLE").is_ok_and(|w| w.eq_ignore_ascii_case("true"));
        const DB_SRC: &str = "test_timestamp_out_of_range_1";
        const DB_DST: &str = "test_timestamp_out_of_range_2";
        const TID: u64 = 32960000;
        const DAYS: i64 = 10;

        let now = Utc::now();
        let taos = connect_taos(&host, ws_enable).await?;
        taos.exec_many([
            format!("DROP TOPIC IF EXISTS force `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
            format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}` keep {DAYS}d duration 1h",),
            format!(
                "CREATE DATABASE IF NOT EXISTS `{DB_DST}` keep {}d duration 1h",
                (DAYS - 3)
            ),
            format!("CREATE TABLE IF NOT EXISTS `{DB_SRC}`.t1 (ts TIMESTAMP, d DOUBLE)"),
        ])
        .await?;
        for i in 0..DAYS {
            taos.exec(format!(
                "INSERT INTO `{DB_SRC}`.t1 VALUES ({}, {i}.0)",
                (now - chrono::Duration::days(i)).timestamp_millis()
            ))
            .await?;
        }

        let (from, to) = if ws_enable {
            let from = format!(
                "tmq+ws://{host}:6041/{DB_SRC}?snapshot=false&group.id={}&enable.concurrent.polling=true",
                now.timestamp_millis()
            ).into_dsn()?;
            let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
            (from, to)
        } else {
            let from = format!(
                "tmq://{host}/{DB_SRC}?snapshot=false&group.id={}&enable.concurrent.polling=true",
                now.timestamp_millis()
            )
            .into_dsn()?;
            let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
            (from, to)
        };
        let (tx, rx) = flume::unbounded();
        tmq_to_td(
            from,
            vec![],
            to,
            CancellationToken::new(),
            Some(TID.to_string()),
            tx.clone(),
        )
        .await?;

        drop(tx);
        assert_eq!(rx.sender_count(), 0, "Sender should be dropped");
        while let Ok(msg) = rx.recv() {
            tracing::info!("Received: {msg:?}");
        }

        // check the data in source and destination databases
        let count_src: u64 = taos
            .query_one(format!("select first(ts) from `{DB_SRC}`.t1"))
            .await?
            .unwrap_or_default();
        assert_eq!(
            count_src,
            (now - chrono::Duration::days(DAYS - 1)).timestamp_millis() as u64
        );
        let count_dst: u64 = taos
            .query_one(format!("select first(ts) from `{DB_DST}`.t1"))
            .await?
            .unwrap_or_default();
        assert_eq!(
            count_dst,
            (now - chrono::Duration::days(DAYS - 4)).timestamp_millis() as u64
        );

        // clean
        taos.exec_many([
            format!("DROP TOPIC IF EXISTS force `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
        ])
        .await?;

        Ok(())
    }

    /// # description
    /// test tmq_to_td task sync stream tables
    /// 1. create databases, stable and stream
    /// 2. create a replication task(tmq_to_td) from DB_SRC to DB_DST
    /// 3. write data in DB_SRC, and stream will generate new tables and data
    /// 4. run for 20 seconds, then stop the replication task
    /// 5. check the result
    /// # description_cn
    /// tmq 同步数据库中写入的数据以及 stream 产生的数据
    /// 1. 创建数据库 DB_SRC 和 DB_DST，在 DB_SRC 中创建超级表和 stream；
    /// 2. 创建数据复制任务，timeout=never
    /// 3. 向 DB_SRC 中写入数据，同时 stream 会产生新表和新数据；
    /// 4. 运行 20 秒后，停止数据复制任务；
    /// 5. 检查 DB_SRC 和 DB_DST 中的数据，表和 stream 的数据都完成了同步，则用例通过，否则失败。
    /// # jira
    /// Close https://jira.taosdata.com:18080/browse/TD-34829
    /// # example
    /// ```shell
    /// cargo nextest run -p taosx-core test_td34829_with_taos --nocapture --retries 0
    /// ```
    #[ignore]
    #[tokio::test]
    async fn test_td34829_with_taos() -> anyhow::Result<()> {
        let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = std::env::var("WS_ENABLE")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        const DB_SRC: &str = "td34829_src";
        const DB_DST: &str = "td34829_dst";
        const TID: i32 = 34829;
        const STREAM: &str = "current_state_window";
        let group_id = format!("test_td{TID}");
        let topic_name = format!("test_replica_td{TID}");

        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();

        // 1. create two database： td34829_src & td34829_dst
        println!("====== create databases and stream =====");
        let taos = if ws_enable {
            TaosBuilder::from_dsn(format!("taos+ws://{host}:6041").into_dsn()?)?
                .build()
                .await?
        } else {
            TaosBuilder::from_dsn(format!("taos://{host}").into_dsn()?)?
                .build()
                .await?
        };

        loop {
            let result = taos
                .exec_many(vec![
                    format!("drop topic if exists `{topic_name}`"),
                    format!("drop stream if exists `{STREAM}`"),
                ])
                .await;
            if result.is_ok() {
                break;
            }
        }

        taos.exec_many(vec![
            format!("drop database if exists `{DB_SRC}`"),
            format!("drop database if exists `{DB_DST}`"),
            format!("create database if not exists `{DB_SRC}`"),
            format!("create database if not exists `{DB_DST}`"),
            format!("create table `{DB_SRC}`.`meters`(ts timestamp, val float) tags(id int)"),
        ])
        .await?;

        loop {
            let sql = format!(
                "create stream `{STREAM}` into `{DB_SRC}`.`{STREAM}` as select tbname,_wstart,avg(val) from `{DB_SRC}`.meters partition by tbname state_window(cast(val as int))"
            );

            let result = taos.exec(&sql).await;
            if result.is_ok() {
                break;
            }
        }

        // 2. create a replication task(tmq_to_td) from td34829_src to td34829_dst
        println!("====== start replication task =====");
        let (from, to) = if ws_enable {
            let from = format!(
                "tmq+ws://{host}:6041/{DB_SRC}?group.id={group_id}&timeout=never&use.topic.name={topic_name}"
            ).into_dsn()?;
            let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
            (from, to)
        } else {
            let from = format!(
                "tmq://{host}/{DB_SRC}?group.id={group_id}&timeout=never&use.topic.name={topic_name}"
            )
                .into_dsn()?;
            let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
            (from, to)
        };

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let (tx, rx) = flume::unbounded();
        let tx_clone = tx.clone();
        let h = tokio::spawn(async move {
            tmq_to_td(
                from,
                vec![],
                to,
                cancel_clone,
                Some(TID.to_string()),
                tx_clone,
            )
            .await
        });

        // 3. write data in DB_SRC
        println!("======= write data =====");
        for _ in 0..20 {
            taos.exec_many(vec![
                format!("insert into {DB_SRC}.t1 using {DB_SRC}.meters tags(1) values(now, 11.1),(now+1s,11.2),(now+2s,11.3),(now+3s,10.1),(now+4s,10.2),(now+5s,10.3)"),
                format!("insert into {DB_SRC}.t2 using {DB_SRC}.meters tags(2) values(now, 22.1),(now+1s,22.2),(now+2s,22.3),(now+3s,21.1),(now+4s,21.2),(now+5s,21.3)"),
                format!("insert into {DB_SRC}.t3 using {DB_SRC}.meters tags(3) values(now, 33.1),(now+1s,33.2),(now+2s,33.3),(now+3s,32.1),(now+4s,32.2),(now+5s,32.3)"),
                format!("insert into {DB_SRC}.t4 using {DB_SRC}.meters tags(4) values(now, 44.1),(now+1s,44.2),(now+2s,44.3),(now+3s,43.1),(now+4s,43.2),(now+5s,43.3)"),
                format!("insert into {DB_SRC}.t5 using {DB_SRC}.meters tags(5) values(now, 55.1),(now+1s,55.2),(now+2s,55.3),(now+3s,54.1),(now+4s,54.2),(now+5s,55.3)"),
            ]).await?;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;

        drop(tx);
        cancel.cancel();
        h.await??;
        while let Ok(msg) = rx.recv() {
            println!("{msg:?}");
        }

        // 4. check the result
        println!("====== check the result =====");
        let count_src: u64 = taos
            .query_one(format!("select count(*) from `{DB_SRC}`.meters"))
            .await?
            .unwrap();
        let count_dst: u64 = taos
            .query_one(format!("select count(*) from `{DB_DST}`.meters"))
            .await?
            .unwrap();
        assert_eq!(count_src, count_dst);

        let stream_src: u64 = taos
            .query_one(format!("select count(*) from `{DB_SRC}`.`{STREAM}`"))
            .await?
            .unwrap();
        let stream_dst: u64 = taos
            .query_one(format!("select count(*) from `{DB_DST}`.`{STREAM}`"))
            .await?
            .unwrap();
        assert_eq!(stream_src, stream_dst);

        // 5. clean
        println!("====== clean up =====");
        loop {
            let result = taos
                .exec_many(vec![
                    format!("drop topic if exists `{topic_name}`"),
                    format!("drop stream if exists `{STREAM}`"),
                ])
                .await;
            if result.is_ok() {
                break;
            }
        }
        taos.exec_many(vec![
            format!("drop database if exists `{DB_SRC}`"),
            format!("drop database if exists `{DB_DST}`"),
        ])
        .await?;

        Ok(())
    }

    /// # description
    /// test tmq_to_td task with auto create table
    /// # description_cn
    /// 目标端表不存在可自动建表
    /// 1. 创建数据库 DB_SRC 和 DB_DST，在 DB_SRC 中创建超级表 meters；
    /// 2. 创建数据复制任务，timeout=10s
    /// 3. 向 DB_SRC 中写入数据，写 100 个表，每个表写 100 行数据；
    /// 4. 写入到一半时，删除 DB_DST 中的 t1, t2, t3 三个表；
    /// 5. 等待数据复制任务结束，检查 DB_SRC 和 DB_DST 中的表，数量一致，则用例通过，否则失败。
    /// # jira
    /// close https://jira.taosdata.com:18080/browse/TD-33080
    /// # example
    /// ```shell
    /// cargo nextest run -p taosx-core test_td33080_with_taos --nocapture --retries 0
    /// ```
    #[ignore]
    #[tokio::test]
    async fn test_td33080_with_taos() -> anyhow::Result<()> {
        tracing_subscriber::fmt::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();

        let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_string());
        let ws_enable = std::env::var("WS_ENABLE")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        const DB_SRC: &str = "td33080_src";
        const DB_DST: &str = "td33080_dst";
        const TID: u64 = 33080;
        const TABLES: u64 = 100;
        const ROWS: usize = 100;

        let taos = connect_taos(&host, ws_enable).await?;
        // create database and topic
        taos.exec_many(vec![
            format!("DROP TOPIC IF EXISTS force `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
            format!("CREATE DATABASE IF NOT EXISTS `{DB_SRC}`"),
            format!("CREATE DATABASE IF NOT EXISTS `{DB_DST}`"),
            format!("CREATE TABLE `{DB_SRC}`.meters(ts timestamp, val float) tags(id int)"),
            format!("CREATE TOPIC `{DB_SRC}` as DATABASE `{DB_SRC}`"),
        ])
        .await?;

        let now = Utc::now();
        // start replication task
        let (from, to) = if ws_enable {
            let from = format!(
                "tmq+ws://{host}:6041/{DB_SRC}?group.id={}&timeout=10s&enable.concurrent.polling=false",
                now.timestamp_millis()
            )
                .into_dsn()?;
            let to = format!("taos+ws://{host}:6041/{DB_DST}").into_dsn()?;
            (from, to)
        } else {
            let from = format!(
                "tmq://{host}/{DB_SRC}?group.id={}&timeout=10s&enable.concurrent.polling=false",
                now.timestamp_millis()
            )
            .into_dsn()?;
            let to = format!("taos://{host}/{DB_DST}").into_dsn()?;
            (from, to)
        };
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let (tx, rx) = flume::unbounded();
        let tx_clone = tx.clone();
        let h = tokio::spawn(async move {
            tmq_to_td(
                from,
                vec![],
                to,
                cancel_clone,
                Some(TID.to_string()),
                tx_clone,
            )
            .await
        });

        // start writing data
        let host_clone = host.clone();
        let write_handler = tokio::spawn(async move {
            let taos = connect_taos(host_clone.as_str(), ws_enable).await.unwrap();

            for _ in 1..=ROWS {
                let mut sql = "INSERT INTO".to_string();
                for table_idx in 1..=TABLES {
                    sql.push_str(&format!(
                        "`{DB_SRC}`.t{table_idx} USING `{DB_SRC}`.meters TAGS({table_idx}) VALUES ({}, {table_idx}.0) ",
                        (now + chrono::Duration::seconds(table_idx as i64)).timestamp_millis()
                    ));
                }
                taos.exec(sql).await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        // drop table meters in sink database
        tokio::time::sleep(Duration::from_millis(TABLES / 2 * 100)).await;
        taos.exec_many(vec![
            format!("DROP TABLE IF EXISTS `{DB_DST}`.t1"),
            format!("DROP TABLE IF EXISTS `{DB_DST}`.t2"),
            format!("DROP TABLE IF EXISTS `{DB_DST}`.t3"),
        ])
        .await?;

        // wait for the write task to finish
        write_handler.await?;

        // wait for the task to end
        drop(tx);
        h.await??;
        while let Ok(msg) = rx.recv() {
            println!("{msg:?}");
        }

        let res_src: u64 = taos
            .query_one(format!(
                "select count(*) from information_schema.ins_tables where db_name = '{DB_SRC}'"
            ))
            .await?
            .unwrap_or(0);
        assert_eq!(res_src, TABLES);
        let res_dst: u64 = taos
            .query_one(format!(
                "select count(*) from information_schema.ins_tables where db_name = '{DB_DST}'"
            ))
            .await?
            .unwrap_or(0);
        assert_eq!(res_dst, TABLES);

        // clean
        taos.exec_many(vec![
            format!("DROP TOPIC IF EXISTS force `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_SRC}`"),
            format!("DROP DATABASE IF EXISTS `{DB_DST}`"),
        ])
        .await?;

        Ok(())
    }

    #[test]
    fn test_dsn_parse_duration() {
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
