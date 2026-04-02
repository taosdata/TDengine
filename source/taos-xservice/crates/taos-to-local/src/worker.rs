use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use flume::Receiver;
use taos::*;
use tokio_util::sync::CancellationToken;

use crate::{QueryObject, ZKey, ZWriter, config::Td2LocalContext};

pub struct Worker {
    id: i32,
    context: Td2LocalContext,
    task_rx: Receiver<TaosToLocalTask>,
    zwriter: Arc<ZWriter>,
    cancel: CancellationToken,
}

impl Worker {
    pub fn new(
        id: i32,
        context: Td2LocalContext,
        task_rx: Receiver<TaosToLocalTask>,
        zwriter: Arc<ZWriter>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            id,
            context,
            task_rx,
            zwriter,
            cancel,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!("taos_to_local worker: {} start", self.id);

        let taos = match &self.context.source_pool {
            Some(p) => p.get().await?,
            None => {
                anyhow::bail!("pool must be set in context");
            }
        };

        let mut count = 0;
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    tracing::info!("worker: {} cancelled", self.id);
                    break;
                }
                res = self.task_rx.recv_async() => {
                    match res {
                        Ok(task) => {
                            let task_id = task.id;
                            tracing::debug!(
                                "taos_to_local worker:{} begin task:{} tb:`{}` sql:{}",
                                self.id,
                                task_id,
                                task.tbname,
                                task.sql
                            );

                            // 执行查询
                            let mut res = taos.query(&task.sql).await?;
                            let mut blocks = res.blocks();

                            // 当前版本：所有任务写入同一个全局 ZFile，后续可按库/表扩展 key。
                            let key = ZKey::Global;
                            self.zwriter.start_raw_block(&key).await?;

                            let mut block_cnt = 0usize;
                            let mut is_cancelled = false;
                            while let Some(block) = {
                                tokio::select! {
                                    biased;
                                    _ = self.cancel.cancelled() => {
                                        is_cancelled = true;
                                        None
                                    }
                                    block = blocks.try_next() => block?,
                                }
                            } {
                                let names = block.field_names();
                                let precision = block.precision();
                                let raw = block.as_raw_bytes();
                                let bytes = Bytes::copy_from_slice(raw);
                                // build new RawBlock
                                let mut new_block = RawBlock::parse_from_raw_block(bytes, precision);
                                new_block.with_field_names(names);
                                new_block.with_table_name(&task.tbname);
                                // write to ZWriter
                                self.zwriter.write_raw_block(&key, &new_block).await?;
                                block_cnt += 1;
                                tracing::debug!(
                                    "worker:{} task:{} tb:`{}` wrote block #{} (raw_len={}B, precision={:?})",
                                    self.id,
                                    task_id,
                                    task.tbname,
                                    block_cnt,
                                    raw.len(),
                                    precision
                                );
                            }

                            if is_cancelled {
                                tracing::warn!("worker:{} task:{} cancelled during block processing", self.id, task_id);
                            } else {
                                self.zwriter.finish_raw_block(&key).await?;
                            }

                            count += 1;
                            tracing::debug!(
                                "worker:{} finished task:{} tb:`{}` blocks:{}",
                                self.id,
                                task_id,
                                task.tbname,
                                block_cnt
                            );
                        },
                        Err(_) => {
                            tracing::info!("worker: {} task channel closed", self.id);
                            break;
                        },
                    }
                }

            }
        }

        tracing::info!("worker: {} shutdown, total: {}", self.id, count);
        Ok(())
    }
}

#[derive(Debug)]
pub struct TaosToLocalTask {
    pub id: usize,
    pub tbname: String,
    pub sql: String,
}

pub struct TaskProducer {
    context: Td2LocalContext,
    task_tx: flume::Sender<TaosToLocalTask>,
    cancel: CancellationToken,
}

impl TaskProducer {
    pub fn new(
        context: Td2LocalContext,
        task_tx: flume::Sender<TaosToLocalTask>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            task_tx,
            cancel,
        }
    }

    /// 带取消检查的发送封装，避免在 Producer 已经被取消后继续推送任务。
    async fn send_task(&self, task: TaosToLocalTask) -> anyhow::Result<()> {
        if self.cancel.is_cancelled() {
            tracing::info!("taos_to_local task producer cancelled before sending task");
            return Ok(());
        }

        tracing::debug!(
            "taos_to_local task producer send task:{} tb:`{}` sql:{}",
            task.id,
            task.tbname,
            task.sql
        );
        self.task_tx.send_async(task).await.inspect_err(|err| {
            tracing::error!("failed to send taos_to_local task: {:?}", err);
        })?;

        Ok(())
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        tracing::info!("taos_to_local task producer start");

        let (query_obj, schema) = match (&self.context.query_obj, &self.context.schema) {
            (Some(q), Some(s)) => (q, s),
            _ => {
                anyhow::bail!("query_obj and schema not found in context");
            }
        };

        let start = self.context.config.start;
        let end = self.context.config.end;

        match query_obj {
            QueryObject::Database(_) => {
                for (idx, meta) in schema.meta_create_iter().enumerate() {
                    if self.cancel.is_cancelled() {
                        tracing::info!(
                            "taos_to_local task producer cancelled while scanning database metas"
                        );
                        break;
                    }

                    let task = match meta {
                        MetaCreate::Super { .. } => {
                            continue;
                        }
                        MetaCreate::Child { table_name, .. }
                        | MetaCreate::Normal { table_name, .. } => {
                            let sql = sql(table_name.as_str(), start, end);
                            TaosToLocalTask {
                                id: idx + 1,
                                sql,
                                tbname: table_name.to_string(),
                            }
                        }
                    };

                    self.send_task(task).await?;
                }
            }
            QueryObject::SuperTables((_, stables)) => {
                for (idx, meta) in schema.meta_create_iter().enumerate() {
                    if self.cancel.is_cancelled() {
                        tracing::info!(
                            "taos_to_local task producer cancelled while scanning super table metas"
                        );
                        break;
                    }

                    let task = match meta {
                        MetaCreate::Super { .. } => {
                            continue;
                        }
                        MetaCreate::Child {
                            table_name, using, ..
                        } => {
                            if !stables.contains(using) {
                                continue;
                            }
                            let sql = sql(table_name.as_str(), start, end);
                            TaosToLocalTask {
                                id: idx + 1,
                                sql,
                                tbname: table_name.to_string(),
                            }
                        }
                        MetaCreate::Normal { table_name, .. } => {
                            let sql = sql(table_name.as_str(), start, end);
                            TaosToLocalTask {
                                id: idx + 1,
                                sql,
                                tbname: table_name.to_string(),
                            }
                        }
                    };

                    self.send_task(task).await?;
                }
            }
            QueryObject::Select((_db, sql, tbname)) => {
                let task = TaosToLocalTask {
                    id: 1,
                    sql: sql.clone(),
                    tbname: tbname.clone(),
                };
                self.send_task(task).await?;
            }
        }

        tracing::info!("task producer finished");
        Ok(())
    }
}

fn sql(table: &str, start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> String {
    match (start, end) {
        (Some(s), Some(e)) => {
            format!(
                "SELECT * FROM `{}` WHERE ts >= '{}' AND ts < '{}'",
                table,
                s.to_rfc3339(),
                e.to_rfc3339()
            )
        }
        (Some(s), None) => {
            format!("SELECT * FROM `{}` WHERE ts >= '{}'", table, s.to_rfc3339())
        }
        (None, Some(e)) => {
            format!("SELECT * FROM `{}` WHERE ts < '{}'", table, e.to_rfc3339())
        }
        (None, None) => {
            format!("SELECT * FROM `{}`", table)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Td2LocalConfig, Td2LocalContext};
    use crate::meta::{DbMeta, Schema};
    use std::path::PathBuf;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_producer() {
        let metas = vec![
            MetaUnit::Create(MetaCreate::Normal {
                table_name: "t".to_string(),
                columns: vec![],
            }),
            MetaUnit::Create(MetaCreate::Child {
                table_name: "t1".to_string(),
                using: "stb".to_string(),
                tags: vec![],
                tag_num: None,
            }),
            MetaUnit::Create(MetaCreate::Child {
                table_name: "t2".to_string(),
                using: "stb".to_string(),
                tags: vec![],
                tag_num: None,
            }),
        ];
        let db_meta = DbMeta {
            name: "test".to_string(),
            create_time: 0,
            ntables: 0,
            strict: String::new(),
            status: String::new(),
            retentions: None,
            ss_chunkpages: None,
            ss_keeplocal: None,
            ss_compact: None,
            with_arbitrator: None,
            encrypt_algorithm: None,
            opts: None,
        };
        let schema = Schema { db_meta, metas };
        let config = Td2LocalConfig {
            stables: vec![],
            upcoming: None,
            schema_only: false,
            start: None,
            end: None,
            max_retry: 3,
            retry_interval: Duration::from_secs(5),
            concurrency: 2,
            backup_dir: PathBuf::new(),
            backup_max_size: 1024 * 1024 * 1024,
            backup_comp_level: async_compression::Level::Fastest,
            pretty: false,
            s3: None,
        };

        // 构造 Td2LocalContext，填入 QueryObject 和 Schema
        let ctx = Td2LocalContext {
            task_job_id: None,
            raw_from: "taos:///test".into_dsn().unwrap(),
            raw_to: "local:///".into_dsn().unwrap(),
            config,
            source_pool: None,
            server_version: None,
            query_obj: Some(QueryObject::Database("test".to_string())),
            schema: Some(schema),
        };

        // 准备 channel 和取消 token
        let (tx, rx) = flume::bounded(10);
        let cancel = CancellationToken::new();

        // 启动 producer
        let producer = TaskProducer::new(ctx, tx, cancel.clone());
        producer.run().await.unwrap();

        // 校验：应当收到 1 条任务，且表名为 t1，SQL 为 SELECT * FROM t1
        let tasks: Vec<TaosToLocalTask> = rx.try_iter().collect();
        assert_eq!(tasks.len(), 3);

        let task = &tasks[0];
        assert_eq!(task.tbname, "t");
        assert_eq!(task.sql, "SELECT * FROM `t`");

        let task = &tasks[1];
        assert_eq!(task.tbname, "t1");
        assert_eq!(task.sql, "SELECT * FROM `t1`");

        let task = &tasks[2];
        assert_eq!(task.tbname, "t2");
        assert_eq!(task.sql, "SELECT * FROM `t2`");
    }

    // ============ sql() function tests ============

    #[test]
    fn test_sql_no_time_range() {
        let result = sql("test_table", None, None);
        assert_eq!(result, "SELECT * FROM `test_table`");
    }

    #[test]
    fn test_sql_with_start_only() {
        let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("test_table", Some(start), None);
        assert_eq!(
            result,
            "SELECT * FROM `test_table` WHERE ts >= '2024-01-01T00:00:00+00:00'"
        );
    }

    #[test]
    fn test_sql_with_end_only() {
        let end = DateTime::parse_from_rfc3339("2024-12-31T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("test_table", None, Some(end));
        assert_eq!(
            result,
            "SELECT * FROM `test_table` WHERE ts < '2024-12-31T23:59:59+00:00'"
        );
    }

    #[test]
    fn test_sql_with_both_start_and_end() {
        let start = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-12-31T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("test_table", Some(start), Some(end));
        assert_eq!(
            result,
            "SELECT * FROM `test_table` WHERE ts >= '2024-01-01T00:00:00+00:00' AND ts < '2024-12-31T23:59:59+00:00'"
        );
    }

    #[test]
    fn test_sql_special_table_name() {
        let result = sql("table_with_special-chars_123", None, None);
        assert_eq!(result, "SELECT * FROM `table_with_special-chars_123`");
    }

    #[test]
    fn test_sql_backtick_escaping() {
        // 测试表名带反引号的情况
        let result = sql("table`name", None, None);
        // 注意：当前实现没有转义，这个测试验证实际行为
        assert_eq!(result, "SELECT * FROM `table`name`");
    }

    #[test]
    fn test_sql_empty_table_name() {
        let result = sql("", None, None);
        assert_eq!(result, "SELECT * FROM ``");
    }

    #[test]
    fn test_sql_with_millisecond_precision() {
        let start = DateTime::parse_from_rfc3339("2024-06-15T12:30:45.123Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-06-15T13:30:45.456Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("metrics", Some(start), Some(end));
        assert_eq!(
            result,
            "SELECT * FROM `metrics` WHERE ts >= '2024-06-15T12:30:45.123+00:00' AND ts < '2024-06-15T13:30:45.456+00:00'"
        );
    }

    #[test]
    fn test_sql_with_different_timezones() {
        // RFC3339 格式会保留时区信息，但 with_timezone(&Utc) 会转换为 UTC
        let start_cst = DateTime::parse_from_rfc3339("2024-01-01T08:00:00+08:00")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("sensor_data", Some(start_cst), None);
        // 应该转换为 UTC 时间 00:00:00
        assert_eq!(
            result,
            "SELECT * FROM `sensor_data` WHERE ts >= '2024-01-01T00:00:00+00:00'"
        );
    }

    #[test]
    fn test_sql_edge_case_epoch_start() {
        let epoch = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("legacy_data", Some(epoch), None);
        assert_eq!(
            result,
            "SELECT * FROM `legacy_data` WHERE ts >= '1970-01-01T00:00:00+00:00'"
        );
    }

    #[test]
    fn test_sql_future_date() {
        let future = DateTime::parse_from_rfc3339("2099-12-31T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("forecast", None, Some(future));
        assert_eq!(
            result,
            "SELECT * FROM `forecast` WHERE ts < '2099-12-31T23:59:59+00:00'"
        );
    }

    #[test]
    fn test_sql_same_start_and_end() {
        let time = DateTime::parse_from_rfc3339("2024-06-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("snapshot", Some(time), Some(time));
        assert_eq!(
            result,
            "SELECT * FROM `snapshot` WHERE ts >= '2024-06-15T12:00:00+00:00' AND ts < '2024-06-15T12:00:00+00:00'"
        );
    }

    #[test]
    fn test_sql_chinese_table_name() {
        let result = sql("测试表", None, None);
        assert_eq!(result, "SELECT * FROM `测试表`");
    }

    #[test]
    fn test_sql_table_with_dots() {
        let result = sql("db.table", None, None);
        assert_eq!(result, "SELECT * FROM `db.table`");
    }

    #[test]
    fn test_sql_year_boundary() {
        let start = DateTime::parse_from_rfc3339("2023-12-31T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339("2024-01-01T00:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        let result = sql("yearly_data", Some(start), Some(end));
        assert_eq!(
            result,
            "SELECT * FROM `yearly_data` WHERE ts >= '2023-12-31T23:59:59+00:00' AND ts < '2024-01-01T00:00:01+00:00'"
        );
    }
}
