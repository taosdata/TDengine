use anyhow::Ok;
use chrono::Local;
use deadpool::managed::Pool;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap; // added for CommonLoadParams
use std::{
    fmt,
    time::{Duration, Instant},
};
use sysinfo::System;
use taos::{AsyncFetchable, AsyncQueryable, Taos, TaosBuilder, taos_query::Manager};
use tokio::time::sleep;

pub mod csv2td;
pub mod td2td;
pub mod tmq2local;
pub mod tmq2td;
pub mod utils;
pub use csv2td::csv2td;
pub use td2td::td2td_history;
pub use td2td::td2td_realtime;
pub use tmq2local::tmq2local;
pub use tmq2td::tmq2td;

use crate::performance::utils::{detect_max_buffer, local_ipv4};

// taosd 相关的参数
#[derive(Debug, Clone)]
pub struct TaosdFactors {
    pub host: String,
    pub ws_enable: bool,
    pub database_options: BTreeMap<String, String>,
}

impl TaosdFactors {
    pub fn csv_header(&self) -> String {
        let db_opts = self
            .database_options
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        format!("host,ws_enable,{}", db_opts)
    }
    pub fn csv_row(&self) -> String {
        format!(
            "{},{},{}",
            self.host,
            self.ws_enable,
            self.database_options
                .values()
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

pub struct TaosdFactorBaseLine {
    pub host: String,
    pub ws_enable: bool,
    pub vgroups: Vec<usize>,
    pub buffers: Vec<usize>,
}

impl TaosdFactorBaseLine {
    pub fn new() -> Self {
        let host = std::env::var("HOST").unwrap_or(local_ipv4());
        let ws_enable = std::env::var("WS_ENABLE")
            .ok()
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        // vgroups 可根据需要后续扩展为读取环境变量或配置文件
        let vgroups: Vec<usize> = vec![32, 64];
        let max_vgroups = *vgroups.iter().max().unwrap();

        // buffer
        let max_buffer = detect_max_buffer(max_vgroups);
        let buffers: Vec<usize> = if max_buffer <= 3 {
            vec![3]
        } else {
            vec![3, max_buffer]
        };

        TaosdFactorBaseLine {
            host,
            ws_enable,
            vgroups,
            buffers,
        }
    }

    pub fn min_vgroups(&self) -> usize {
        *self.vgroups.iter().min().unwrap()
    }

    pub fn max_buffer(&self) -> usize {
        *self.buffers.iter().max().unwrap()
    }
}

// 数据规模相关的参数
#[derive(Debug, Clone, Default)]
pub struct DataFactors {
    // 子表数量
    pub tables: usize,
    // 每个子表写多少行
    pub rows: usize,
    // 每行数据的列数，不包括 timestamp 列
    pub cols: usize,
    // 两行数据之间的时间戳间隔，单位毫秒
    pub interval: i64,
}

impl DataFactors {
    pub fn csv_header(&self) -> &'static str {
        "tables,rows,cols,interval"
    }
    pub fn csv_row(&self) -> String {
        format!(
            "{},{},{},{}",
            self.tables, self.rows, self.cols, self.interval
        )
    }
}

#[derive(Debug, Clone)]
pub struct Writer {
    /// 线程编号
    gid: usize,
    // database
    db: String,
    /// 子表的数量
    table_size: usize,
    /// 每个子表写多少行
    rows: usize,
    /// 写入数据的开始时间戳（ms）
    ts: i64,
    /// 每行数据的步长（ms）
    step: i64,
    /// 子表前缀
    table_prefix: String,
    /// 每条 SQL 写入的行数
    rows_per_sql: usize,
}

impl Writer {
    /// 根据 Simulation 配置创建 writers, 并均匀分配子表数量
    pub fn from_simluation(sim: &Simulation) -> Vec<Self> {
        assert!(sim.writers > 0, "writers must be > 0");
        let mut writers = Vec::with_capacity(sim.writers);
        let base_per_writer = sim.tables / sim.writers;
        let remainder = sim.tables % sim.writers;
        for gid in 0..sim.writers {
            let table_size = base_per_writer + if gid < remainder { 1 } else { 0 };
            writers.push(Writer {
                gid,
                db: sim.db.clone(),
                table_size,
                rows: sim.rows_per_table,
                ts: sim.ts,
                step: sim.step,
                table_prefix: sim.table_prefix.clone().unwrap_or_else(|| "t".to_string()),
                rows_per_sql: sim.rows_per_sql,
            });
        }
        writers
    }

    pub async fn create_tables(&self, taos: &Taos) -> anyhow::Result<()> {
        const MAX_SQL_LENGTH: usize = 1024 * 1024;
        const PREFIX: &str = "t";

        let mut sql = "create table ".to_string();
        let mut sql_len = sql.len();
        let mut first = true;

        for tid in 0..self.table_size {
            let table_def = format!(
                "`{}`.{PREFIX}_{}_{} using `{}`.stb tags({}, {})",
                self.db, self.gid, tid, self.db, self.gid, tid
            );
            let part = if first {
                first = false;
                table_def.clone()
            } else {
                format!(" {}", table_def)
            };

            if sql_len + part.len() > MAX_SQL_LENGTH {
                taos.exec(&sql).await?;
                sql = format!("create table {}", table_def);
                sql_len = sql.len();
                first = false;
            } else {
                sql.push_str(&part);
                sql_len += part.len();
            }
        }
        if sql_len > "create table ".len() {
            taos.exec(&sql).await?;
        }
        Ok(())
    }

    /// taos: connection to TDengine
    /// rate_limit: 每秒写入的行数限制，None表示不限制
    pub async fn write(&self, taos: &Taos, rate_limit: Option<u64>) -> anyhow::Result<()> {
        const MAX_SQL_LENGTH: usize = 1024 * 1024;

        let mut sql = String::new();
        let mut rows_written = vec![0; self.table_size];
        let mut table_ts = vec![self.ts; self.table_size]; // 每个表独立的 timestamp
        let mut table_idx = 0;

        let mut last_tick = Instant::now();
        let mut rows_this_sec = 0;

        loop {
            for tid in table_idx..self.table_size {
                if rows_written[tid] >= self.rows {
                    continue;
                }
                // 拼接表名
                let mut table_sql = format!(
                    "`{}`.{}_{}_{} VALUES",
                    self.db, self.table_prefix, self.gid, tid
                );
                let mut values = Vec::new();

                for _i in 0..self.rows_per_sql {
                    if rows_written[tid] >= self.rows {
                        break;
                    }
                    let value = ((rand::random::<f32>() * 100.0) * 100.0).round() / 100.0;
                    values.push(format!("({}, {})", table_ts[tid], value));

                    rows_written[tid] += 1;
                    table_ts[tid] += self.step;
                    rows_this_sec += 1;

                    // 限速控制
                    if let Some(rate) = rate_limit {
                        if rows_this_sec >= rate {
                            let elapsed = last_tick.elapsed();
                            if elapsed < Duration::from_secs(1) {
                                sleep(Duration::from_secs(1) - elapsed).await;
                            }
                            last_tick = Instant::now();
                            rows_this_sec = 0;
                        }
                    }
                }

                if !values.is_empty() {
                    table_sql.push_str(&values.join(","));
                    if !sql.is_empty() && !sql.ends_with("INSERT INTO ") {
                        table_sql = format!(" {}", table_sql);
                    }
                    // 检查拼接后是否超长
                    if sql.len() + table_sql.len() > MAX_SQL_LENGTH {
                        // 先执行/清空
                        if sql.len() > "INSERT INTO ".len() {
                            taos.exec(&sql).await?;
                        }
                        sql.clear();
                        sql.push_str("INSERT INTO ");
                    }
                    if sql.is_empty() {
                        sql.push_str("INSERT INTO ");
                    }
                    sql.push_str(&table_sql);
                }
            }

            // 如果有剩余SQL或所有表都写完了，执行一次写入
            let all_done = rows_written.iter().all(|&n| n >= self.rows);
            if sql.len() > "INSERT INTO ".len() && (sql.len() >= MAX_SQL_LENGTH || all_done) {
                taos.exec(&sql).await?;
                sql.clear();
            }

            if all_done {
                break;
            }

            // 下次从第一个表继续
            table_idx = 0;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Simulation {
    db: String,
    writers: usize,               // 线程数
    tables: usize,                // 总共创建多少个子表
    rows_per_table: usize,        // 每个子表插入多少行数据
    ts: i64,                      // 写入数据的开始时间戳（ms）
    step: i64,                    // 写入数据的步长（ms）
    table_prefix: Option<String>, // 子表前缀
    rows_per_sql: usize,          // 每条 SQL 写入的行数，默认为 50
    speed_limit: Option<u64>,     // 每秒写入的行数限制，None表示不限制
}

// 仅创建超级表与所有子表 schema，不写入数据
pub async fn simluate_create_tables(
    pool: &Pool<Manager<TaosBuilder>>,
    sim: &Simulation,
) -> anyhow::Result<()> {
    let taos = pool.get().await?;

    // 创建超级表
    taos.exec(format!(
        "CREATE TABLE `{}`.stb(ts TIMESTAMP, val FLOAT) TAGS(gid INT, id INT)",
        sim.db
    ))
    .await?;

    // 创建 writer
    let writers = Writer::from_simluation(sim);

    // 创建表
    for writer in &writers {
        writer.create_tables(&taos).await?;
    }

    Ok(())
}

// 仅执行数据写入，假定 schema 已经创建完成
pub async fn simulate_write_only(
    pool: &Pool<Manager<TaosBuilder>>,
    sim: &Simulation,
) -> anyhow::Result<BasicMetrics> {
    let writers = Writer::from_simluation(sim);

    let start = Instant::now();
    let mut handlers = Vec::new();
    for writer in &writers {
        let taos = pool.get().await?;
        let writer = writer.clone();
        let limit = sim.speed_limit;
        let h = tokio::spawn(async move {
            writer.write(&taos, limit).await.unwrap();
        });
        handlers.push(h);
    }
    futures::future::join_all(handlers).await;

    // 计算性能指标
    let total_rows = sim.tables * sim.rows_per_table;
    let time_cost = start.elapsed().as_secs_f64();
    let rate = total_rows as f64 / time_cost.max(1e-6);

    // 验证写入的总行数
    let taos = pool.get().await?;
    let rows: u64 = taos
        .query_one(format!("SELECT count(*) FROM `{}`.stb", sim.db))
        .await?
        .unwrap_or(0);
    assert_eq!(rows, total_rows as u64);

    Ok(BasicMetrics {
        total_rows,
        time_cost,
        rate,
    })
}

// 模拟写入 (包含 schema 创建 + 数据写入)
pub async fn simulate_write(
    pool: &Pool<Manager<TaosBuilder>>,
    sim: &Simulation,
) -> anyhow::Result<BasicMetrics> {
    simluate_create_tables(pool, sim).await?;

    let output = simulate_write_only(pool, sim).await?;

    Ok(output)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BasicMetrics {
    total_rows: usize, // 总行数
    time_cost: f64,    // 总耗时
    rate: f64,         // 速度
}

impl fmt::Display for BasicMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "write rows: {}, time_cost: {:.2}s, rate: {:.2} rows/s }}",
            self.total_rows, self.time_cost, self.rate
        )
    }
}

impl BasicMetrics {
    pub fn csv_header_with_prefix(prefix: &str) -> String {
        format!("{prefix}_total_rows,{prefix}_time_cost(sec),{prefix}_rate")
    }

    pub fn csv_header() -> String {
        "total_rows,time_cost(sec),rate".to_string()
    }

    pub fn csv_row(&self) -> String {
        format!("{},{:.2},{:.2}", self.total_rows, self.time_cost, self.rate)
    }
}

fn collect_sysinfo(sys: &mut System) -> (f32, f32) {
    sys.refresh_cpu_all();
    sys.refresh_memory();
    let cpu_usage = sys.cpus().iter().map(|c| c.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;
    let mem_usage = sys.used_memory() as f32 / sys.total_memory() as f32;

    // TODO: 磁盘，统计写和读的速度
    // TODO: 网络，统计入站和出站流量

    (cpu_usage, mem_usage)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SysMetrics {
    pub duration: Duration,
    pub cpu_min: f32,
    pub cpu_max: f32,
    pub cpu_p80: f32,
    pub cpu_p90: f32,
    pub mem_min: f32,
    pub mem_max: f32,
    pub mem_p80: f32,
    pub mem_p90: f32,
}

impl SysMetrics {
    pub fn csv_header() -> String {
        "duration(sec),cpu_min,cpu_max,cpu_p80,cpu_p90,mem_min,mem_max,mem_p80,mem_p90".to_string()
    }

    pub fn csv_row(&self) -> String {
        format!(
            "{:.2},{:.2}%,{:.2}%,{:.2}%,{:.2}%,{:.2}%,{:.2}%,{:.2}%,{:.2}%",
            self.duration.as_secs_f64(),
            self.cpu_min,
            self.cpu_max,
            self.cpu_p80,
            self.cpu_p90,
            self.mem_min,
            self.mem_max,
            self.mem_p80,
            self.mem_p90
        )
    }
}

fn summarize_percentiles(mut values: Vec<f32>) -> (f32, f32, f32, f32) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min = *values.first().unwrap();
    let max = *values.last().unwrap();
    let p80_idx = ((values.len() as f32) * 0.8).ceil() as usize - 1;
    let p90_idx = ((values.len() as f32) * 0.9).ceil() as usize - 1;
    let p80 = values
        .get(p80_idx.min(values.len() - 1))
        .cloned()
        .unwrap_or(max);
    let p90 = values
        .get(p90_idx.min(values.len() - 1))
        .cloned()
        .unwrap_or(max);
    (min, max, p80, p90)
}

/// 通用性能测试包装器，自动采集系统负载
pub async fn collect_system_metrics<F, Fut>(f: F) -> SysMetrics
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    use std::sync::{Arc, Mutex};
    use tokio::sync::watch;

    let cpu_samples = Arc::new(Mutex::new(Vec::new()));
    let mem_samples = Arc::new(Mutex::new(Vec::new()));

    let mut sys = System::new_all();
    sys.refresh_all();

    let start = Instant::now();
    let (tx, rx) = watch::channel(false);
    let cpu_samples_clone = cpu_samples.clone();
    let mem_samples_clone = mem_samples.clone();

    // 采集任务
    let collector = tokio::spawn(async move {
        let mut sys = System::new_all();
        sys.refresh_all();
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // 让 sysinfo 有基线

        loop {
            let (cpu_usage, mem_usage) = collect_sysinfo(&mut sys);
            cpu_samples_clone.lock().unwrap().push(cpu_usage);
            mem_samples_clone.lock().unwrap().push(mem_usage);

            // 检查是否收到停止信号
            if *rx.borrow() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    });

    // 执行被测函数
    f().await;

    let duration = start.elapsed();
    // 通知采集任务退出
    let _ = tx.send(true);
    let _ = collector.await;

    // 统计指标
    let cpu_samples = Arc::try_unwrap(cpu_samples).unwrap().into_inner().unwrap();
    let mem_samples = Arc::try_unwrap(mem_samples).unwrap().into_inner().unwrap();
    let (cpu_min, cpu_max, cpu_p80, cpu_p90) = summarize_percentiles(cpu_samples);
    let (mem_min, mem_max, mem_p80, mem_p90) = summarize_percentiles(mem_samples);

    SysMetrics {
        duration,
        cpu_min,
        cpu_max,
        cpu_p80,
        cpu_p90,
        mem_min,
        mem_max,
        mem_p80,
        mem_p90,
    }
}

pub async fn recreate_databases(
    taos: &Taos,
    db_names: &[&str],
    db_opts: &BTreeMap<String, String>,
) -> anyhow::Result<()> {
    // drop databases if exists
    let drop_sqls: Vec<String> = db_names
        .iter()
        .map(|db| format!("DROP DATABASE IF EXISTS `{}`", db))
        .collect();
    taos.exec_many(drop_sqls).await?;

    // create databases
    for db in db_names {
        let mut sql = format!("CREATE DATABASE `{}`", db);
        for (k, v) in db_opts.iter() {
            sql.push_str(&format!(" {} {}", k, v));
        }
        taos.exec(sql).await?;
    }
    Ok(())
}

pub async fn drop_related_topics(taos: &Taos, db_names: &[&str]) -> anyhow::Result<()> {
    for db in db_names {
        let topics: Vec<String> = taos
            .query(format!(
                "select topic_name from information_schema.ins_topics where db_name = '{db}'"
            ))
            .await?
            .deserialize()
            .try_collect()
            .await?;

        // drop topics
        for t in topics {
            taos.exec(format!("DROP TOPIC IF EXISTS force `{t}`"))
                .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use taosx_core::utils::sql::connect_taos_pool;

    use super::*;

    #[tokio::test]
    async fn test_collect_sysinfo() {
        let mut sys = System::new_all();
        sys.refresh_all();
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // 让 sysinfo 有基线

        for _ in 0..5 {
            let (cpu_usage, mem_usage) = collect_sysinfo(&mut sys);
            println!(
                "CPU Usage: {:.2}%, Memory Usage: {:.2}%",
                cpu_usage, mem_usage
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    #[test]
    fn test_create_writers() {
        let sim = Simulation {
            db: "db".into(),
            writers: 24,
            tables: 10000,
            rows_per_table: 10000,
            ts: 0,
            step: 1000,
            table_prefix: None,
            rows_per_sql: 50,
            speed_limit: None,
        };
        let writers = Writer::from_simluation(&sim);
        assert_eq!(writers.len(), 24);
        let total_tables: usize = writers.iter().map(|w| w.table_size).sum();
        assert_eq!(total_tables, 10000);
        assert_eq!(writers[0].gid, 0);
        assert_eq!(writers[0].table_size, 417);
        assert_eq!(writers[15].gid, 15);
        assert_eq!(writers[15].table_size, 417);
        assert_eq!(writers[16].gid, 16);
        assert_eq!(writers[16].table_size, 416);
        assert_eq!(writers[23].gid, 23);
        assert_eq!(writers[23].table_size, 416);
    }

    #[ignore]
    #[tokio::test]
    async fn test_simulate_write() {
        let pool = connect_taos_pool("192.168.2.139", true).await.unwrap();

        let sim = Simulation {
            db: "test_simulate_write".to_string(),
            writers: 10,
            tables: 10000,
            table_prefix: Some("t".to_string()),
            rows_per_table: 1000,
            rows_per_sql: 50,
            speed_limit: None,
            ts: Local::now().timestamp_millis() - (1000 * 1000) as i64,
            step: 1000, // 每行数据间隔1秒
        };

        simulate_write(&pool, &sim).await.unwrap();
    }
}
