use std::path::Path;

use crate::conf::LocalRestoreConfig;
use taos::*;
use taos_to_local::Schema;

// 检查目录下，如果有 schema.meta 和 data.bin.* 文件，则认为是 t2l 备份文件
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
        tracing::info!("found entry: {}", path.display());
        if path.is_file() {
            let file_name = path.file_name().unwrap().to_string_lossy();
            if file_name.starts_with("schema.meta") {
                has_meta = true;
            } else if file_name.starts_with("data.bin") {
                has_data = true;
            }
        }
    }
    if has_meta && has_data {
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

    // 创建 taos 连接
    let taos_pool = config.connect_taos_pool().await?;
    let taos = taos_pool.get().await?;
    tracing::info!("connected to taos server");

    restore_schema(&taos, &config).await?;

    restore_data(&taos, &config).await?;

    Ok(())
}

async fn restore_schema(taos: &Taos, config: &LocalRestoreConfig) -> anyhow::Result<()> {
    let backup_dir = config.backup_dir.clone();
    // find schema.meta
    let schema_file = backup_dir.join("schema.meta");
    if !schema_file.exists() || !schema_file.is_file() {
        return Err(anyhow::anyhow!(
            "schema.meta file not found in dir: {}",
            backup_dir.display()
        ));
    }
    tracing::info!("found schema file: {}", schema_file.display());

    // 读取 schema.meta 文件
    let mut schema: Schema = crate::t2l::read_schema(&schema_file).await?;
    tracing::info!("read schema: {:#?}", schema);

    // 创建数据库
    if let Some(db) = &config.database {
        schema.db_meta.name = db.clone();
    }
    let sql = schema.db_meta.to_string();
    tracing::info!("exec sql: {}", sql);
    taos.exec(sql).await?;

    // 创建表
    for meta in &mut schema.metas {
        let sql = meta.to_string();
        tracing::info!("exec sql: {}", sql);
        taos.exec(sql).await?;
    }

    Ok(())
}

async fn read_schema(path: &Path) -> anyhow::Result<Schema> {
    let data = tokio::fs::read(path).await?;
    let schema: Schema = serde_json::from_slice(&data)?;
    Ok(schema)
}

async fn restore_data(_taos: &Taos, _config: &LocalRestoreConfig) -> anyhow::Result<()> {
    Ok(())
}
