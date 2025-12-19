use anyhow::{Context, bail};
use async_compression::Level;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::sync::Arc;
use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    time::Duration,
    vec,
};
use taos::*;
use taosx_core::{
    taoz::{RawType, ZFile},
    utils::{parse_key_in_dsn, wait_for_upcoming},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::{Td2LocalConfigBuilder, Td2LocalContext};
use crate::meta::{Schema, fetch_database_meta, fetch_tables_meta};
use crate::worker::{TaskProducer, Worker};

mod config;
pub mod meta;
mod worker;

/// 基于 TDengine 查询的备份
pub async fn taos_to_local(
    task_id: Option<String>,
    from: Dsn,
    to: Dsn,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(
        "taos_to_local started, task_id: {:?}, from: {}, to: {}",
        task_id,
        from,
        to
    );

    // parse Td2LocalConfig
    let config = Td2LocalConfigBuilder::new(task_id.as_deref(), &from, &to)
        .build()
        .context("failed to parse taos_to_local configuration")?;
    tracing::info!("taos_to_local config: {:#?}", config);

    let mut ctx = Td2LocalContext {
        task_id: task_id.clone(),
        raw_from: from.clone(),
        raw_to: to.clone(),
        config: config.clone(),
        source_pool: None,
        server_version: None,
        query_obj: None,
        schema: None,
    };
    // 建立连接池
    let pool = TaosBuilder::from_dsn(&ctx.raw_from)?
        .pool()
        .map_err(|e| anyhow::anyhow!("failed to build connect pool, cause: {:?}", e))?;
    tracing::info!("taos_to_local connect pool established");
    ctx.source_pool = Some(pool.clone());

    // 获取源 TDengine 服务器版本
    let taos = pool.get().await?;
    let server_version = taos
        .server_version()
        .await
        .map(|s| s.to_string())
        .context("failed to get server_version")?;
    tracing::info!("taos_to_local source server version: {}", server_version);
    ctx.server_version = Some(server_version);

    // 检查备份对象是否存在
    let query_obj = QueryObject::try_from_dsn(&ctx.raw_from)?;
    if !query_obj.exists(&taos).await? {
        bail!("backup object: {:?} not exist", query_obj);
    }
    ctx.query_obj = Some(query_obj.clone());

    // 等待 upcoming
    if let Err(_e) = wait_for_upcoming(config.upcoming, cancel.clone()).await {
        tracing::info!("taos_to_local cancelled before upcoming time");
        return Ok(());
    }

    // 获取备份对象的 schema
    let schema1 = query_obj.fetch_schema(&taos).await?;
    let backup_dir = ctx.config.backup_dir.clone();
    write_schema_to_local(&schema1, &backup_dir, "schema.meta", ctx.config.pretty).await?;
    ctx.schema = Some(schema1.clone());

    if ctx.config.schema_only {
        tracing::info!("taos_to_local backup schema only, done.");
        return Ok(());
    }

    // 创建 ZWriter，用于写 ZFile 备份文件
    let zwriter = ZWriter::new(&ctx)?;
    // start workers
    let worker_num = ctx.config.concurrency;
    let (tx, rx) = flume::bounded(worker_num);
    let mut workers = vec![];
    for id in 0..worker_num {
        let worker = Worker::new(
            id as i32,
            ctx.clone(),
            rx.clone(),
            zwriter.clone(),
            cancel.clone(),
        );
        let handle: tokio::task::JoinHandle<Result<(), anyhow::Error>> =
            tokio::spawn(async move { worker.run().await });
        workers.push(handle);
    }

    // start task producer
    let producer = TaskProducer::new(ctx.clone(), tx, cancel.clone());
    let producer_handle = tokio::spawn(async move { producer.run().await });

    // wait for producer finished
    let _ = producer_handle.await;

    // wait for all workers finished
    for handle in workers {
        let _ = handle.await;
    }

    // 确保所有 ZFile 正常关闭并刷盘
    zwriter.shutdown().await?;

    // 再次检查备份对象的 schema
    let schema2 = query_obj.fetch_schema(&taos).await?;
    write_schema_to_local(&schema2, &backup_dir, "schema.meta.1", ctx.config.pretty).await?;

    Ok(())
}

/// 为 taos-to-local 提供 ZFile 写入能力的轻量封装。
///
/// 设计目标：
/// - 复用 taosx_core::taoz::ZFile，产出的备份文件可被 local-to-taos 直接识别。
/// - 接口简单：当前版本先采用“单全局文件”模型，后续可以按库/表扩展 key。
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ZKey {
    /// 所有任务写入同一个 ZFile（简单模式）。
    Global,
}

pub struct ZWriter {
    /// taosx 的版本号，用于写入 ZFile 头部。
    api_version: String,
    /// 源 taosd 的版本号。
    server_version: String,
    /// 备份文件存放目录。
    backup_dir: PathBuf,
    /// 逻辑“topic”名称：对 taos-to-local 来说，可以用数据库名等替代。
    topic: String,
    /// 备份点时间戳。
    ts: Option<DateTime<Utc>>,
    /// 压缩级别。
    compression_level: Level,
    /// 单个文件的最大大小。
    max_file_size: u64,
    /// 写满后的移动目录（可选）。
    move_to: Option<PathBuf>,
    /// 写入超时时间。
    timeout: Duration,
    /// 管理不同逻辑 key 对应的 ZFile。
    writers: DashMap<ZKey, Arc<Mutex<ZFile>>>,
}

impl Debug for ZWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZWriter")
            .field("api_version", &self.api_version)
            .field("server_version", &self.server_version)
            .field("backup_dir", &self.backup_dir)
            .field("topic", &self.topic)
            .field("ts", &self.ts)
            .field("compression_level", &self.compression_level)
            .field("max_file_size", &self.max_file_size)
            .field("move_to", &self.move_to)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl ZWriter {
    /// 基于上下文创建一个 ZWriter 实例。
    pub fn new(ctx: &Td2LocalContext) -> anyhow::Result<std::sync::Arc<Self>> {
        let cfg = &ctx.config;

        let backup_dir = cfg.backup_dir.clone();
        // 备份文件名称：database-timestamp-vgid-sequence
        let topic = match &ctx.query_obj {
            Some(QueryObject::Database(db)) => db.clone(),
            Some(QueryObject::SuperTables((db, _))) => db.clone(),
            Some(QueryObject::Select((db, ..))) => db.clone(),
            None => "t2lbackup".to_string(), // 默认的文件 PREFIX
        };
        let api_version = crate::build::PKG_VERSION.to_owned();
        let server_version = ctx.server_version.clone().unwrap_or_default();
        // 以当前时间作为备份时间戳
        let ts: Option<DateTime<Utc>> = Some(Utc::now());

        Ok(std::sync::Arc::new(Self {
            api_version,
            server_version,
            backup_dir,
            topic,
            ts,
            compression_level: cfg.backup_comp_level,
            max_file_size: cfg.backup_max_size,
            move_to: None,
            timeout: Duration::from_secs(60),
            writers: DashMap::new(),
        }))
    }

    /// 获取或为给定 key 创建一个 ZFile，并返回其可变引用锁。
    async fn get_or_create_arc(&self, key: &ZKey) -> anyhow::Result<Arc<Mutex<ZFile>>> {
        if let Some(entry) = self.writers.get(key) {
            return Ok(entry.value().clone());
        }
        let zfile = ZFile::new(
            &self.api_version,
            &self.server_version,
            &self.backup_dir,
            (self.topic.as_str(), self.ts, 0, 0),
            self.compression_level,
            self.max_file_size,
            self.move_to.clone(),
            self.timeout,
        )
        .await?;
        let arc = Arc::new(Mutex::new(zfile));
        self.writers.insert(key.clone(), arc.clone());
        Ok(arc)
    }

    /// 将一组原始块写入指定 key 对应的 ZFile。这里暂时只支持 Data 类型，后续可扩展 Meta/Raw。
    pub async fn write_raw_blocks<I>(&self, key: &ZKey, blocks: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = taos_query::common::RawData>,
    {
        let arc = self.get_or_create_arc(key).await?;
        let mut zf = arc.lock().await;
        for raw in blocks {
            zf.write_raw(&raw, RawType::Data).await?;
        }
        zf.flush().await?;
        Ok(())
    }

    /// 开始一个 Data(Vec<RawBlock>) 消息的写入（对应 ZMessage::Data），写头标记。
    pub async fn start_raw_block(&self, key: &ZKey) -> anyhow::Result<()> {
        let arc = self.get_or_create_arc(key).await?;
        let mut zf = arc.lock().await;
        zf.start_raw_block().await?;
        Ok(())
    }

    /// 向当前 Data 序列写入一个 RawBlock。
    pub async fn write_raw_block(&self, key: &ZKey, block: &RawBlock) -> anyhow::Result<()> {
        let arc = self.get_or_create_arc(key).await?;
        let mut zf = arc.lock().await;
        zf.write_raw_block(block).await?;
        Ok(())
    }

    /// 结束一个 Data(Vec<RawBlock>) 消息（写结束标记并做轮转检查）。
    pub async fn finish_raw_block(&self, key: &ZKey) -> anyhow::Result<()> {
        let arc = self.get_or_create_arc(key).await?;
        let mut zf = arc.lock().await;
        zf.finish_raw_block().await?;
        zf.flush().await?;
        Ok(())
    }

    /// 直接写入一批 RawBlock，内部自动包裹 start/finish，返回写入块数量。
    pub async fn write_raw_block_sequence<I>(&self, key: &ZKey, blocks: I) -> anyhow::Result<usize>
    where
        I: IntoIterator<Item = RawBlock>,
    {
        let arc = self.get_or_create_arc(key).await?;
        let mut zf = arc.lock().await;
        zf.start_raw_block().await?;
        let mut count = 0usize;
        for b in blocks.into_iter() {
            zf.write_raw_block(&b).await?;
            count += 1;
        }
        zf.finish_raw_block().await?;
        zf.flush().await?;
        Ok(count)
    }

    /// 关闭所有打开的 ZFile，确保数据落盘并执行必要的 move_to。
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        for entry in self.writers.iter() {
            let mut file = entry.value().lock().await;
            // 如果正在写 Data 段，这里假定调用方已经 finish；不再强制补一个空 Data。
            file.flush().await?;
            file.shutdown().await?;
            file.move_to().await?;
        }
        Ok(())
    }
}

// 将 Schema 序列化并写入本地文件
async fn write_schema_to_local(
    schema: &Schema,
    dir: &Path,
    name: &str,
    pretty: bool,
) -> anyhow::Result<()> {
    // 创建目录
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("failed to create directory: {}", dir.display()))?;

    // 文件名，使用数据库名
    let file = dir.join(name);

    let context = if pretty {
        serde_json::to_vec_pretty(schema)?
    } else {
        serde_json::to_vec(schema)?
    };

    tokio::fs::write(&file, context)
        .await
        .with_context(|| format!("failed to write schema to: {}", file.display()))?;

    tracing::info!("write schema to {}", file.display());

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryObject {
    Database(String),                   // 备份某个数据库
    SuperTables((String, Vec<String>)), // 备份某个数据库下的部分超级表
    Select((String, String, String)), // 备份某个数据库下 select 语句的查询结果，第三个参数为结果表名
}

impl QueryObject {
    // 从 dsn 解析出备份对象
    fn try_from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        if dsn.subject.is_none() {
            bail!("dsn subject is required");
        }

        let db = dsn.subject.as_ref().unwrap().to_string();
        let stables: Option<Vec<String>> = parse_key_in_dsn::<String>(dsn, "stables")?
            .map(|s| s.split(",").map(|s| s.trim().to_string()).collect());
        if let Some(stables) = stables {
            if stables.is_empty() {
                bail!("stables cannot be empty if specified");
            }
            return Ok(QueryObject::SuperTables((db, stables)));
        }

        let select: Option<String> = parse_key_in_dsn::<String>(dsn, "select")?;
        let tbname: String =
            parse_key_in_dsn::<String>(dsn, "select_name")?.unwrap_or_else(|| "result".to_string());
        if let Some(sql) = select {
            if sql.trim().is_empty() {
                bail!("select cannot be empty if specified");
            }
            return Ok(QueryObject::Select((db, sql, tbname)));
        }

        Ok(QueryObject::Database(db))
    }

    // 检查备份对象在 TDengine 中是否存在
    async fn exists(&self, taos: &Taos) -> anyhow::Result<bool> {
        match self {
            QueryObject::Database(db) => {
                return taos
                    .database_exists(db)
                    .await
                    .context("failed to check database exists");
            }
            QueryObject::SuperTables((db, stables)) => {
                for stb in stables {
                    let sql = format!(
                        "select stable_name from information_schema.ins_stables where db_name = '{db}' and stable_name = '{stb}'"
                    );
                    let res: Option<String> = taos
                        .query_one(sql)
                        .await
                        .context("failed to query stable exists")?;
                    if res.is_none() {
                        bail!("stable '{}' not exists in database '{}'", stb, db);
                    }
                }
                Ok(true)
            }
            QueryObject::Select((db, select, _tbname)) => {
                let sql = format!("use `{db}`");
                taos.exec(sql)
                    .await
                    .context(format!("failed to use database '{}'", db))?;

                let res = taos.query(select).await;
                if let Err(e) = res {
                    return Err(anyhow::anyhow!("invalid select sql, cause: {:?}", e));
                }
                Ok(true)
            }
        }
    }

    // 获取备份对象的 schema 信息
    async fn fetch_schema(&self, taos: &Taos) -> anyhow::Result<Schema> {
        let (db_meta, metas) = match self {
            Self::Database(db) | Self::Select((db, ..)) => {
                // 整个数据库的 schema，包括：数据库，超级表，子表，普通表
                let db_meta = fetch_database_meta(taos, db).await?;
                let metas = fetch_tables_meta(taos, db, &[]).await?;
                (db_meta, metas)
            }
            Self::SuperTables((db, stables)) => {
                // 指定数据库的某些超级表的 schema
                let db_meta = fetch_database_meta(taos, db).await?;
                let metas = fetch_tables_meta(taos, db, stables).await?;
                (db_meta, metas)
            }
        };

        Ok(Schema { db_meta, metas })
    }
}

#[cfg(test)]
mod tests {

    use taos::IntoDsn;

    use super::*;

    #[test]
    fn test_parse_query_obj() {
        let dsn = "taos://:/test".into_dsn().unwrap();
        let obj = QueryObject::try_from_dsn(&dsn).unwrap();
        assert_eq!(obj, QueryObject::Database("test".to_string()));

        let dsn = "taos://:/test?stables=stb1,stb2".into_dsn().unwrap();
        let obj = QueryObject::try_from_dsn(&dsn).unwrap();
        assert_eq!(
            obj,
            QueryObject::SuperTables((
                "test".to_string(),
                vec!["stb1".to_string(), "stb2".to_string()]
            ))
        );

        let dsn = "taos://:/test?select=select * from stb1"
            .into_dsn()
            .unwrap();
        let obj = QueryObject::try_from_dsn(&dsn).unwrap();
        assert_eq!(
            obj,
            QueryObject::Select((
                "test".to_string(),
                "select * from stb1".to_string(),
                "result".to_string()
            ))
        );

        let dsn = "taos:///".into_dsn().unwrap();
        let res = QueryObject::try_from_dsn(&dsn);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "dsn subject is required");
    }

    #[tokio::test]
    async fn test_query_obj_exists_with_taos() {
        let dsn = "taos:///";

        let taos = TaosBuilder::from_dsn(dsn.into_dsn().unwrap())
            .unwrap()
            .build()
            .await
            .unwrap();

        taos.exec_many(vec![
            "drop database if exists non_exist_db",
            "create database if not exists db",
            "create stable if not exists db.stb (ts timestamp, val int) tags(t int)",
        ])
        .await
        .unwrap();

        let obj = QueryObject::Database("db".to_string());
        let res = obj.exists(&taos).await.unwrap();
        assert!(res);

        let obj = QueryObject::Database("non_exist_db".to_string());
        let res = obj.exists(&taos).await.unwrap();
        assert!(!res);

        let obj = QueryObject::SuperTables((
            "db".to_string(),
            vec!["stb1".to_string(), "stb2".to_string()],
        ));
        let res = obj.exists(&taos).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().to_string(),
            "stable 'stb1' not exists in database 'db'"
        );

        let obj = QueryObject::Select((
            "db".to_string(),
            "select * from stb".to_string(),
            "result".to_string(),
        ));
        let res = obj.exists(&taos).await.unwrap();
        assert!(res);

        taos.exec("drop database if exists db").await.unwrap();
    }
}
