use async_compression::tokio::bufread::ZstdDecoder;
use std::path::Path;
use taos::*;
use taos_to_local::meta::Schema;
use taosx_core::taoz::{ZCodec, ZFile, ZMessage};
use tokio::io::BufReader;

use crate::conf::LocalRestoreConfig;

// 检查目录下，如果有 schema.meta 或 data.bin.* 文件，则认为是 t2l 备份文件
pub async fn is_t2l(dir: &Path) -> anyhow::Result<bool> {
    tracing::info!("check dir: {}", dir.display());

    if !dir.exists() || !dir.is_dir() {
        return Ok(false);
    }

    let mut has_meta = false;
    let mut has_data = false;
    // 遍历目录
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        tracing::debug!("found entry: {}", path.display());
        if path.is_file() {
            let file_name = path.file_name().unwrap().to_string_lossy();
            if file_name.starts_with("schema.meta") {
                has_meta = true;
            } else if file_name.starts_with("data.bin") {
                has_data = true;
            }
        }
    }

    if has_meta || has_data {
        tracing::info!("found t2l backup files in dir: {}", dir.display());
        return Ok(true);
    }
    Ok(false)
}

// 执行本地文件到 taos 的恢复
pub async fn restore_t2l(config: LocalRestoreConfig) -> anyhow::Result<()> {
    tracing::info!(
        "start restore t2l backup files from dir: {}",
        config.backup_dir.display()
    );

    // 1. 创建 root 级 taos 连接（不指定 database），以便在目标库不存在时能先建库
    let taos = config.connect_taos_root().await?;
    tracing::info!("connected to taos server (root mode, no database selected)");

    // 2. 恢复 schema
    restore_schema(&taos, &config).await?;

    // 3. 恢复数据
    restore_data(&taos, &config).await?;

    Ok(())
}

async fn restore_schema(taos: &Taos, config: &LocalRestoreConfig) -> anyhow::Result<()> {
    let backup_dir = config.backup_dir.clone();
    let schema_file = backup_dir.join("schema.meta");
    if !schema_file.exists() || !schema_file.is_file() {
        return Err(anyhow::anyhow!(
            "schema.meta file not found in dir: {}",
            backup_dir.display()
        ));
    }
    tracing::info!("found schema file: {}", schema_file.display());

    // 读取 schema.meta
    let mut schema: Schema = crate::t2l::read_schema(&schema_file).await?;

    // 目标数据库名：优先使用 config.database（即用户在 DSN 中指定的），否则使用备份中的原库名。
    if let Some(db) = &config.database {
        schema.db_meta.name = db.clone();
    };
    let target_db = schema.db_meta.name.as_str();

    // 检查数据库是否存在
    if taos.database_exists(target_db).await? {
        tracing::debug!(
            "target database `{}` already exists, skip create",
            target_db
        );
    } else {
        let create_sql = schema.db_meta.to_string();
        tracing::debug!("create database sql: {}", create_sql);
        if let Err(err) = taos.exec(&create_sql).await {
            tracing::error!("create database `{}` failed: {:#}", target_db, err);
            return Err(err.into());
        }
    }

    // 切换数据库，后续表创建语句若未包含库前缀可正常执行。
    let use_sql = format!("USE `{}`", target_db);
    tracing::debug!("exec sql: {}", use_sql);
    taos.exec(&use_sql).await?;

    // 创建超级表和普通表，MetaCreate::Super 和 MetaCreate::Normal 逐个执行
    let mut child_creates: Vec<taos::MetaCreate> = Vec::with_capacity(schema.metas.len());
    for meta in &schema.metas {
        if let MetaUnit::Create(meta) = meta {
            match meta {
                taos::MetaCreate::Super { .. } | taos::MetaCreate::Normal { .. } => {
                    let table_sql = meta.to_string();
                    tracing::debug!("exec sql: {}", table_sql);
                    match taos.exec(&table_sql).await {
                        Ok(_) => {}
                        Err(err) => {
                            let code: i32 = err.code().into();
                            match code {
                            0x0603 /* table already exists */ => {
                                tracing::debug!("table already exists, skip: {:#}", err);
                            }
                            _ => {
                                tracing::error!("create table failed: {:#}", err);
                                return Err(err.into());
                            }
                        }
                        }
                    }
                }
                taos::MetaCreate::Child { .. } => {
                    // 收集子表建表信息，稍后批量建表
                    child_creates.push(meta.clone());
                }
            }
        }
    }

    // MetaCreate::Child 使用批量建表语句
    if !child_creates.is_empty() {
        create_child_tables(taos, &child_creates).await?;
    }

    Ok(())
}

async fn create_child_tables(
    taos: &Taos,
    child_creates: &[taos::MetaCreate],
) -> anyhow::Result<()> {
    // TDengine 支持多子表批量建表：
    // CREATE TABLE IF NOT EXISTS tb1 USING stb ... TAGS(...) tb2 USING stb ... TAGS(...)
    // 这里将多个 MetaCreate::Child 的 to_string() 合并，首个保留前缀，后续移除前缀。
    const MAX_SQL_LEN: usize = 1024 * 1024; // 1 MiB 上限
    const PREFIX: &str = "CREATE TABLE IF NOT EXISTS ";

    let mut batches: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut current_len: usize = 0;

    for m in child_creates {
        let full = m.to_string();
        let full_trimmed = full.trim().to_string();
        if !full_trimmed.starts_with(PREFIX) {
            // 不符合预期前缀的，将其视为无法批量合并，单独成批。
            if !current.is_empty() {
                batches.push(std::mem::take(&mut current));
                current_len = 0;
            }
            batches.push(vec![full_trimmed]);
            continue;
        }
        let piece = full_trimmed;

        // 预估加入当前批后的长度（含一个空格或首部前缀）
        let added_len = if current.is_empty() {
            piece.len() + 1 // 结尾可能再补一个分号
        } else {
            // 去掉重复的前缀时的长度：
            piece.len() - PREFIX.len() + 1
        };

        if current_len + added_len > MAX_SQL_LEN && !current.is_empty() {
            // 现有批次已达到长度限制，先提交此批
            batches.push(std::mem::take(&mut current));
            current_len = 0;
        }

        current.push(piece);
        current_len += added_len;
    }

    if !current.is_empty() {
        batches.push(current);
    }

    // 执行每一个批量建表 SQL
    for group in batches.into_iter() {
        let mut sql = String::new();
        for (i, item) in group.iter().enumerate() {
            if i == 0 {
                // 首个完整保留
                sql.push_str(item);
            } else {
                // 后续如果包含前缀则移除
                if let Some(stripped) = item.strip_prefix(PREFIX) {
                    sql.push(' ');
                    sql.push_str(stripped);
                } else {
                    sql.push(' ');
                    sql.push_str(item);
                }
            }
        }
        sql.push(';');
        if let Err(err) = taos.exec(&sql).await {
            tracing::error!("create child tables failed, sql: {}", sql);
            tracing::warn!("fallback to single execution, err: {:#}", err);
            // Fallback: 单条执行
            for sql in group {
                if let Err(e2) = taos.exec(&sql).await {
                    let c2: i32 = e2.code().into();
                    match c2 {
                        0x0603 /* table already exists */ => {
                            tracing::debug!("child table already exists (fallback single), skip: {:#}", e2);
                        }
                        _ => {
                            tracing::error!("create child table failed (fallback single): {:#}", e2);
                            return Err(e2.into());
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn read_schema(path: &Path) -> anyhow::Result<Schema> {
    let data = tokio::fs::read(path).await?;
    let schema: Schema = serde_json::from_slice(&data)?;
    Ok(schema)
}

async fn restore_data(_taos: &Taos, _config: &LocalRestoreConfig) -> anyhow::Result<()> {
    let taos = _taos; // keep names to match signature
    let config = _config;

    // 列出备份目录下按时间顺序排序的 .z 备份文件
    let files = ZFile::list_in_dir(&config.backup_dir).await?;
    if files.is_empty() {
        tracing::info!("no zfiles found in dir: {}", config.backup_dir.display());
        return Ok(());
    }

    for f in files {
        let Some(path) = f.raw_path.as_ref() else {
            continue;
        };
        tracing::info!("restore t2l data from file: {}", path.display());

        // 打开并解压 zstd，再用 ZCodec 读取
        let file = match tokio::fs::File::open(path).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("skip unreadable file {}: {:#}", path.display(), e);
                continue;
            }
        };
        let reader = BufReader::new(file);
        let decoder = ZstdDecoder::new(reader);
        let mut codec = ZCodec::new(decoder);

        // 读取头信息（并不强依赖）
        let _header = match codec.header_async().await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!("skip file (invalid header) {}: {:#}", path.display(), e);
                continue;
            }
        };

        // 按消息流读取并写回 taos
        loop {
            match codec.read_message_async().await {
                Ok(ZMessage::Meta(meta)) => {
                    // schema 在 restore_schema 已经创建，这里可跳过或尝试写入（忽略已存在错误）
                    if let Err(err) = taos.write_raw_meta(&meta).await {
                        let code: i32 = err.code().into();
                        if code != 0x0603 {
                            // 0x0603: table already exists
                            tracing::warn!("write_raw_meta ignored error: {:#}", err);
                        }
                    }
                }
                Ok(ZMessage::Data(data)) => {
                    for raw in data {
                        // 如果需要强制目标库名，可在 restore_schema 中 USE 之后，这里直接写入即可
                        // raw.with_table_name("..."); // 可选

                        // 记录目标表名（尽力而为）：优先从 to_create() 中提取
                        let table_guess: Option<String> = raw.to_create().map(|mc| match mc {
                            taos::MetaCreate::Super { table_name, .. } => table_name,
                            taos::MetaCreate::Normal { table_name, .. } => table_name,
                            taos::MetaCreate::Child { table_name, .. } => table_name,
                        });
                        if let Some(ref name) = table_guess {
                            tracing::debug!("restore_data: writing raw block to table `{}`", name);
                        } else {
                            tracing::debug!("restore_data: writing raw block to <unknown-table>");
                        }

                        if let Err(err) = taos.write_raw_block(&raw).await {
                            let code: i32 = err.code().into();
                            if code == 0x2603 {
                                // the table does not exist
                                if let Some(create) = raw.to_create() {
                                    if let Err(e2) = taos.exec(format!("{}", create)).await {
                                        tracing::error!("create table failed: {:#}", e2);
                                        return Err(e2.into());
                                    }
                                    tracing::info!(
                                        "restore_data: created missing table, retry write_raw_block"
                                    );
                                    // retry once
                                    if let Err(e3) = taos.write_raw_block(&raw).await {
                                        tracing::error!("retry write_raw_block failed: {:#}", e3);
                                        return Err(e3.into());
                                    }
                                    tracing::debug!("restore_data: retry write_raw_block success");
                                } else {
                                    tracing::error!(
                                        "no create DDL in raw block, cannot recover table"
                                    );
                                    return Err(err.into());
                                }
                            } else {
                                tracing::error!("write_raw_block failed: {:#}", err);
                                return Err(err.into());
                            }
                        } else if let Some(name) = table_guess {
                            tracing::debug!("restore_data: wrote raw block to table `{}`", name);
                        }
                    }
                }
                Ok(ZMessage::Raw(_ty, raw)) => {
                    // 保底处理：当出现 Raw 消息时按 meta 看待
                    if let Err(err) = taos.write_raw_meta(&raw).await {
                        tracing::debug!("write_raw_meta (raw) ignored error: {:#}", err);
                    }
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    tracing::warn!("read_message_async error on {}: {:#}", path.display(), e);
                    break;
                }
            }
        }
    }

    Ok(())
}
