use crate::performance::{
    BasicMetrics, DataFactors, Simulation, SysMetrics, TaosdFactorBaseLine, TaosdFactors,
    recreate_databases, simluate_create_tables, simulate_write_only,
    utils::{taosd_version, taosx_version},
};
use anyhow::anyhow;
use chrono::Local;
use itertools::iproduct;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;
use std::{collections::BTreeMap, fs::OpenOptions, io::Write};
use taos::IntoDsn;
use taos::{AsyncQueryable, Dsn};
use taosx_core::utils::sql::connect_taos_pool;
use tokio::time::{Duration as TokioDuration, sleep};
use tokio_util::sync::CancellationToken; // for overall sync duration

/// tmq2td 数据同步的负载性能测试
pub async fn tmq2td() -> anyhow::Result<()> {
    tracing::info!("TMQ -> TD Performance Test...");

    run_cases(gen_cases().await?).await?;

    Ok(())
}

/// 生成 tmq2td 测试用例
async fn gen_cases() -> anyhow::Result<Vec<Tmq2TdFactors>> {
    let base = TaosdFactorBaseLine::new();

    // baseline
    const TABLES_BASELINE: usize = 10000;
    const ROWS_BASELINE: usize = 10000;
    const INTERVAL_BASELINE: i64 = 1000;
    const PREFER_BASELINE: &str = "auto";
    let writers_baseline = std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(10);

    let mut cases = Vec::new();

    // 1. 基准的测试用例，确定 VGROUPS 和 BUFFER 对性能的影响
    for (&v, &b) in iproduct!(&base.vgroups, &base.buffers) {
        let mut database_options = BTreeMap::new();
        database_options.insert("VGROUPS".to_string(), v.to_string());
        database_options.insert("BUFFER".to_string(), b.to_string());

        let c = Tmq2TdFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options,
            },
            data_factors: DataFactors {
                tables: TABLES_BASELINE,
                rows: ROWS_BASELINE,
                cols: 1,
                interval: INTERVAL_BASELINE,
            },
            prefer: PREFER_BASELINE.to_string(),
            read_concurrency: v,
            write_concurrency: writers_baseline,
        };
        cases.push(c);
    }

    let mut db_options = BTreeMap::new();
    db_options.insert("VGROUPS".to_string(), base.min_vgroups().to_string());
    db_options.insert("BUFFER".to_string(), base.max_buffer().to_string());

    // 2. 数据规模: 多表，少行
    let c = Tmq2TdFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLES_BASELINE * 100,
            rows: ROWS_BASELINE / 10,
            cols: 1,
            interval: INTERVAL_BASELINE,
        },
        prefer: PREFER_BASELINE.to_string(),
        read_concurrency: base.min_vgroups(),
        write_concurrency: writers_baseline,
    };
    cases.push(c);

    // 3. 数据规模: 少表，多行
    let c = Tmq2TdFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLES_BASELINE / 10,
            rows: ROWS_BASELINE * 100,
            cols: 1,
            interval: INTERVAL_BASELINE,
        },
        prefer: PREFER_BASELINE.to_string(),
        read_concurrency: base.min_vgroups(),
        write_concurrency: writers_baseline,
    };
    cases.push(c);

    // 4. 数据规模: 多表，多行
    let c = Tmq2TdFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLES_BASELINE * 100,
            rows: ROWS_BASELINE,
            cols: 1,
            interval: INTERVAL_BASELINE,
        },
        prefer: PREFER_BASELINE.to_string(),
        read_concurrency: base.min_vgroups(),
        write_concurrency: writers_baseline,
    };
    cases.push(c);

    // 5. interval = 60 sec
    let c = Tmq2TdFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLES_BASELINE,
            rows: ROWS_BASELINE,
            cols: 1,
            interval: INTERVAL_BASELINE * 60,
        },
        prefer: PREFER_BASELINE.to_string(),
        read_concurrency: base.min_vgroups(),
        write_concurrency: writers_baseline,
    };
    cases.push(c);

    // 6. 比较不同 prefer 写入模式
    for mode in ["raw", "interlace", "stmt", "sql", "block"] {
        let c = Tmq2TdFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_factors: DataFactors {
                tables: TABLES_BASELINE,
                rows: ROWS_BASELINE,
                cols: 1,
                interval: INTERVAL_BASELINE,
            },
            prefer: mode.to_string(),
            read_concurrency: base.min_vgroups(),
            write_concurrency: writers_baseline,
        };

        cases.push(c);
    }

    // 7. 比较 write_concurrency 对性能的影响
    for w in [writers_baseline / 2, writers_baseline * 2] {
        let c = Tmq2TdFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_factors: DataFactors {
                tables: TABLES_BASELINE,
                rows: ROWS_BASELINE,
                cols: 1,
                interval: INTERVAL_BASELINE,
            },
            prefer: PREFER_BASELINE.to_string(),
            read_concurrency: base.min_vgroups(),
            write_concurrency: w,
        };
        cases.push(c);
    }

    Ok(cases)
}

async fn run_cases(factors: Vec<Tmq2TdFactors>) -> anyhow::Result<()> {
    // Get CSV output directory from environment variable, fallback to current directory
    let csv_output_dir = std::env::var("CSV_OUTPUT_DIR").unwrap_or_else(|_| ".".to_string());
    let report_path = std::path::Path::new(&csv_output_dir).join("tmq2td.csv");

    // 测试生成的 metrics 写入到 tmq2td.csv
    let mut file: std::fs::File = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&report_path)?;
    let need_header = file.metadata()?.len() == 0;
    // 获取 taosd 的版本信息
    let taosd_ver = taosd_version().await.unwrap_or("unknown".to_string());
    // 获取 taosx 的版本信息
    let taosx_ver = taosx_version().await.unwrap_or("unknown".to_string());

    for (idx, f) in factors.iter().enumerate() {
        let m = run_tmq2td(f.clone()).await?;

        if idx == 0 && need_header {
            // 写入csv表头
            let header = format!(
                "ts,TDengine Version,TaosX Version,{},{},total_rows,time_cost,rate,latency(sec),{}\n",
                f.csv_header(),
                BasicMetrics::csv_header_with_prefix("write"),
                SysMetrics::csv_header(),
            );
            file.write_all(header.as_bytes())?;
            file.flush()?;
        }

        let now = Local::now().to_rfc3339();
        let csv_line = format!(
            "{},{},{},{},{},{},{:.2},{:.2},{:.2},{}\n",
            now,
            taosd_ver,
            taosx_ver,
            f.csv_row(),
            m.write.csv_row(),
            m.total_rows,
            m.time_cost,
            m.rate,
            m.latency,
            m.sys.csv_row()
        );

        file.write_all(csv_line.as_bytes())?;
        file.flush()?;
    }
    Ok(())
}

type Tmq2TdResult = Arc<Mutex<Option<(BasicMetrics, usize, f64, f64, f64)>>>;

/// 单条 tmq2td 测试执行 (当前占位: 复用 write 模拟 + 占位迁移指标)
async fn run_tmq2td(f: Tmq2TdFactors) -> anyhow::Result<Tmq2TdMetrics> {
    let pool: deadpool::managed::Pool<taos::taos_query::Manager<taos::TaosBuilder>> =
        connect_taos_pool(&f.taosd_factors.host, f.taosd_factors.ws_enable).await?;

    const DB_SRC: &str = "pt_tmq2td_src";
    const DB_DST: &str = "pt_tmq2td_dst";
    const TOPIC: &str = "pt_tmq2td";

    // 准备环境: topic + src/dst databases
    let taos = pool.get().await?;
    taos.exec(format!("DROP TOPIC IF EXISTS force `{TOPIC}`"))
        .await?;
    recreate_databases(&taos, &[DB_SRC, DB_DST], &f.taosd_factors.database_options).await?;
    taos.exec(format!(
        "CREATE TOPIC `{TOPIC}` WITH META AS DATABASE `{DB_SRC}`"
    ))
    .await?;
    drop(taos);

    // 构造 from/to dsn
    let (from, to) = build_tmq2td_dsn(&f, TOPIC, DB_DST)?;

    let sim = Simulation {
        db: DB_SRC.to_string(),
        writers: 10,
        tables: f.data_factors.tables,
        rows_per_table: f.data_factors.rows,
        table_prefix: None,
        ts: chrono::Utc::now().timestamp_millis() / 60000 * 60000,
        step: f.data_factors.interval,
        rows_per_sql: 50,
        speed_limit: None,
    };

    // 先创建表结构
    simluate_create_tables(&pool, &sim).await?;

    // 采集系统指标期间执行整个同步流程
    let result_holder: Tmq2TdResult = Arc::new(Mutex::new(None));
    let result_holder_closure = result_holder.clone();
    let from_closure = from.clone();
    let to_closure = to.clone();
    let pool_closure = pool.clone();
    let sys_metrics = crate::performance::collect_system_metrics(|| async move {
        // A: 启动同步消费任务
        let consumer_done = Arc::new(AtomicBool::new(false));
        let consumer_done_flag = consumer_done.clone();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let (tx_progress, _rx_progress) = flume::unbounded();
        let consumer_handle = {
            let from = from_closure.clone();
            let to = to_closure.clone();
            let consumer_done_set = consumer_done_flag.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    tmq_to_td::tmq_to_td(from, vec![], to, cancel_clone, None, tx_progress).await
                {
                    eprintln!("tmq_to_td task error: {e}");
                }
                // 标记消费者已结束 (正常或异常退出)
                consumer_done_set.store(true, Ordering::SeqCst);
            })
        };

        // B: 写入任务
        let sync_start = Instant::now();
        let write_finished = Arc::new(AtomicBool::new(false));
        let write_finished_flag = write_finished.clone();
        let pool_for_writer = pool_closure.clone();
        let writer_handle = tokio::spawn(async move {
            // let m = simulate_write(&pool_for_writer, &sim).await;
            // todo: 目前是建表和写入分离
            let m = simulate_write_only(&pool_for_writer, &sim).await;
            write_finished_flag.store(true, Ordering::SeqCst);
            m
        });

        // C: 监控任务
        let pool_for_monitor = pool_closure.clone();
        let consumer_done_check = consumer_done.clone();
        let write_finished_check = write_finished.clone();
        let monitor_handle = tokio::spawn(async move {
            let mut samples: Vec<(u64, u64)> = Vec::new();
            let mut last_diff: Option<u64> = None;
            let mut stable_ticks: usize = 0; // diff 未变化计数
            const STABLE_THRESHOLD: usize = 30; // 连续 30 秒无进展则退出 (写已完成)
            const MAX_DURATION_SECS: u64 = 30 * 60; // 30 分钟上限
            let start = Instant::now();
            loop {
                // 超时保护
                if start.elapsed().as_secs() >= MAX_DURATION_SECS {
                    tracing::warn!("monitor timeout reached ({}s)", MAX_DURATION_SECS);
                    break;
                }
                let taos = match pool_for_monitor.get().await {
                    Ok(c) => c,
                    Err(_) => {
                        sleep(TokioDuration::from_millis(500)).await;
                        continue;
                    }
                };
                let src: u64 = taos
                    .query_one(format!("SELECT count(*) FROM `{}`.stb", DB_SRC))
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                let dst: u64 = taos
                    .query_one(format!("SELECT count(*) FROM `{}`.stb", DB_DST))
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                samples.push((src, dst));

                let diff = src.saturating_sub(dst);

                // 退出条件 1: 已追平
                if src > 0 && dst == src {
                    tracing::info!(src, dst, diff, "tmq2td monitor exit: dst caught up");
                    break;
                }
                // 退出条件 2: 消费者任务自行结束 (异常或正常) 防止等待悬挂
                if consumer_done_check.load(Ordering::SeqCst) {
                    tracing::warn!(src, dst, diff, "tmq2td monitor exit: consumer task ended");
                    break;
                }
                // 退出条件 3: 写入已结束且 diff 长期无变化 (认为下游追平已停滞或接近稳定)
                if write_finished_check.load(Ordering::SeqCst) {
                    match last_diff {
                        Some(ld) if ld == diff => {
                            stable_ticks += 1;
                            if stable_ticks >= STABLE_THRESHOLD {
                                tracing::info!(
                                    src,
                                    dst,
                                    diff,
                                    stable_ticks,
                                    "tmq2td monitor exit: stable diff after write finished"
                                );
                                break;
                            }
                        }
                        _ => {
                            stable_ticks = 0;
                            last_diff = Some(diff);
                        }
                    }
                }
                sleep(TokioDuration::from_secs(1)).await;
            }
            samples
        });

        // 等待写入完成
        let write_metrics = writer_handle
            .await
            .expect("writer join")
            .expect("writer result");

        // 等待监控完成
        let samples = monitor_handle.await.expect("monitor join");

        // 取消 tmq_to_td
        cancel.cancel();
        let _ = tokio::time::timeout(TokioDuration::from_secs(2), consumer_handle).await;

        // 计算 latency
        let latency_sec = calc_median_latency(&samples, write_metrics.rate);

        let sync_duration = sync_start.elapsed().as_secs_f64();
        let total_rows = write_metrics.total_rows;
        let sync_rate = if sync_duration > 0.0 {
            total_rows as f64 / sync_duration
        } else {
            0.0
        };

        *result_holder_closure.lock().unwrap() = Some((
            write_metrics,
            total_rows,
            sync_duration,
            sync_rate,
            latency_sec,
        ));
    })
    .await;

    let (write_metrics, total_rows, sync_duration, sync_rate, latency_sec) = result_holder
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| anyhow!("tmq2td result missing"))?;

    let m = Tmq2TdMetrics {
        write: write_metrics,
        total_rows,
        time_cost: sync_duration,
        rate: sync_rate,
        latency: latency_sec,
        sys: sys_metrics,
    };

    Ok(m)
}

/// 根据 (src,dst) 样本计算同步延迟 (秒)
/// 样本中的差值 diff = src - dst (行), 取中位数, 再除以写入速率(行/秒)
fn calc_median_latency(samples: &[(u64, u64)], write_rate: f64) -> f64 {
    if write_rate <= 0.0 || samples.is_empty() {
        return 0.0;
    }
    let mut diffs: Vec<u64> = samples.iter().map(|(s, d)| s.saturating_sub(*d)).collect();
    if diffs.is_empty() {
        return 0.0;
    }
    diffs.sort_unstable();
    let mid = diffs.len() / 2;
    let median = if diffs.len() % 2 == 1 {
        diffs[mid] as f64
    } else {
        (diffs[mid - 1] as f64 + diffs[mid] as f64) / 2.0
    };
    median / write_rate
}

/// 构造 tmq2td 的 from / to DSN 字符串 (尚未 into_dsn)
fn build_tmq2td_dsn(f: &Tmq2TdFactors, topic: &str, db_dst: &str) -> anyhow::Result<(Dsn, Dsn)> {
    let (from, to) = if f.taosd_factors.ws_enable {
        let from = format!(
            "tmq+ws://{}:6041/{topic}?timeout=never&prefer={}&read_concurrency={}&write_concurrency={}",
            f.taosd_factors.host, f.prefer, f.read_concurrency, f.write_concurrency
        );
        let to = format!("taos+ws://{}:6041/{db_dst}", f.taosd_factors.host);
        (from, to)
    } else {
        let from = format!(
            "tmq://{}:6030/{topic}?timeout=never&prefer={}&read_concurrency={}&write_concurrency={}",
            f.taosd_factors.host, f.prefer, f.read_concurrency, f.write_concurrency
        );
        let to = format!("taos://{}:6030/{db_dst}", f.taosd_factors.host);
        (from, to)
    };

    Ok((from.into_dsn()?, to.into_dsn()?))
}

/// tmq2td 的影响因子
#[derive(Debug, Clone)]
struct Tmq2TdFactors {
    // TDengine 的参数
    taosd_factors: TaosdFactors,
    // 数据规模
    data_factors: DataFactors,
    // 写入模式: auto, raw, interlace, stmt, sql, block
    prefer: String,
    // tmq 订阅的消费者数量
    read_concurrency: usize,
    // 写入线程数
    write_concurrency: usize,
}

impl Tmq2TdFactors {
    fn csv_header(&self) -> String {
        format!(
            "{},{},{},{},{}",
            self.taosd_factors.csv_header(),
            self.data_factors.csv_header(),
            "prefer",
            "read_concurrency",
            "write_concurrency"
        )
    }

    fn csv_row(&self) -> String {
        format!(
            "{},{},{},{},{}",
            self.taosd_factors.csv_row(),
            self.data_factors.csv_row(),
            self.prefer,
            self.read_concurrency,
            self.write_concurrency
        )
    }
}

/// tmq2td 的性能指标
#[derive(Debug, Clone)]
struct Tmq2TdMetrics {
    write: BasicMetrics,

    total_rows: usize, // 总行数
    time_cost: f64,    // 总耗时
    rate: f64,         // 速度
    latency: f64,      // 延迟

    sys: SysMetrics,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gen_cases() {
        let cases = gen_cases().await.unwrap();
        assert!(!cases.is_empty());
        for (idx, case) in cases.iter().enumerate() {
            if idx == 0 {
                println!("#,{}", case.csv_header());
            }
            println!("{},{}", idx + 1, case.csv_row());
        }
    }

    #[test]
    fn test_build_tmq2td_dsn() {
        // given
        let f = Tmq2TdFactors {
            taosd_factors: TaosdFactors {
                host: "192.168.0.11".to_string(),
                ws_enable: true,
                database_options: BTreeMap::new(),
            },
            data_factors: DataFactors::default(),
            prefer: "auto".to_string(),
            read_concurrency: 2,
            write_concurrency: 4,
        };
        const TOPIC: &str = "ABC";
        const DB_DST: &str = "DB_DST";

        // when
        let (from, to) = build_tmq2td_dsn(&f, TOPIC, DB_DST).unwrap();

        // then
        assert_eq!(
            from,
            "tmq+ws://192.168.0.11:6041/ABC?timeout=never&prefer=auto&read_concurrency=2&write_concurrency=4"
                .into_dsn()
                .unwrap()
        );
        assert_eq!(to, "taos+ws://192.168.0.11:6041/DB_DST".into_dsn().unwrap());
    }

    #[test]
    fn test_calc_median_latency() {
        let samples: Vec<(u64, u64)> = vec![];
        assert_eq!(calc_median_latency(&samples, 100.0), 0.0);

        let samples = vec![(10, 5), (20, 10)];
        assert_eq!(calc_median_latency(&samples, 0.0), 0.0);

        // diffs: 3,6,1 -> median 3 -> latency 3/3 = 1.0
        let samples = vec![(10, 7), (20, 14), (30, 29)];
        let latency = calc_median_latency(&samples, 3.0);
        assert!((latency - 1.0).abs() < 1e-9, "latency={latency}");

        // diffs: (10-7)=3, (20-19)=1, (30-25)=5, (40-31)=9 -> sorted 1,3,5,9 median (3+5)/2=4 -> 4/2=2
        let samples = vec![(10, 7), (20, 19), (30, 25), (40, 31)];
        let latency = calc_median_latency(&samples, 2.0);
        assert!((latency - 2.0).abs() < 1e-9, "latency={latency}");

        // diffs all 0 -> latency 0
        let samples = vec![(10, 10), (20, 20), (30, 30)];
        let latency = calc_median_latency(&samples, 5.0);
        assert_eq!(latency, 0.0);
    }
}
