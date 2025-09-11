use anyhow::Ok;
use chrono::Local;
use chrono::Utc;
use itertools::iproduct;
use legacy_to_taos::legacy_to_taos;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;
use taos::IntoDsn;
use taosx_core::utils::sql::connect_taos_pool;
use tokio_util::sync::CancellationToken;

use crate::performance::SysMetrics;
use crate::performance::TaosdFactorBaseLine;
use crate::performance::utils::taosd_version;
use crate::performance::utils::taosx_version;
use crate::performance::{
    BasicMetrics, DataFactors, Simulation, TaosdFactors, collect_system_metrics,
    recreate_databases, simulate_write,
};

/// TD -> TD 历史数据迁移的负载性能测试
pub async fn td2td_history() -> anyhow::Result<()> {
    tracing::info!("TD -> TD History Mode Performance Test...");

    run_hist_cases(gen_hist_cases().await?).await
}

/// 构造一组 td2td_history 的测试用例
/// 设计原则 (按对迁移速率 mig_rate 影响的当前优先级):
/// 1. VGROUPS × BUFFER: 首要关注 (影响 taosd 读/写能力)。提供 2×2 主效应矩阵: VGROUPS {32,64} × BUFFER {3,max_buffer(若>3)}
/// 2. STEP: 影响 taosx 处理效率。提供稠密(1s) vs 稀疏(60s) 主效应，固定在中等规模 (1e4 tables × 1e4 rows) 基线下。
/// 3. 数据规模 (tables, rows): 区分“多表+少行”(1e6×100 ~1e8 rows) 、“少表+中等行”(1e4×5e4 ~5e8 rows)、极高行 (1e4×1e5 ~1e9 rows)
/// 4. WORKERS: 迁移工作线程 (baseline = CPU*2)。比较 {baseline/2, baseline, baseline*2} 在基线 (buffer=3, vgroups=32, step=1s) 场景的影响。
async fn gen_hist_cases() -> anyhow::Result<Vec<TD2TDFactors>> {
    let base = TaosdFactorBaseLine::new();

    // 统一 baseline
    const TABLES_BASE_LINE: usize = 10000;
    const ROWS_BASE_LINE: usize = 10000;
    const INTERVAL_BASE_LINE: i64 = 1000;
    // workers: 工作线程数 baseline = CPU * 2
    let workers_baseline = std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(10);

    let mut cases = vec![];
    // 1) 基线 VGROUPS × BUFFER 主效应矩阵
    for (vg, buf) in iproduct!(&base.vgroups, &base.buffers) {
        // 构造 database_options
        let mut database_options = BTreeMap::new();
        database_options.insert("VGROUPS".to_string(), vg.to_string());
        database_options.insert("BUFFER".to_string(), buf.to_string());

        let c = TD2TDFactors {
            taosd_params: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options,
            },
            data_params: DataFactors {
                tables: TABLES_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: 1,
                interval: INTERVAL_BASE_LINE,
            },
            workers: workers_baseline,
        };

        cases.push(c);
    }

    let mut db_options = BTreeMap::new();
    db_options.insert("VGROUPS".to_string(), base.min_vgroups().to_string());
    db_options.insert("BUFFER".to_string(), base.max_buffer().to_string());

    // 2) 数据规模: 多表，少行
    let c = TD2TDFactors {
        taosd_params: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_params: DataFactors {
            tables: TABLES_BASE_LINE * 10,
            rows: ROWS_BASE_LINE / 10,
            cols: 1,
            interval: INTERVAL_BASE_LINE,
        },
        workers: workers_baseline,
    };
    cases.push(c);

    // 3) 数据规模: 少表多行
    let c = TD2TDFactors {
        taosd_params: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_params: DataFactors {
            tables: TABLES_BASE_LINE / 10,
            rows: ROWS_BASE_LINE * 100,
            cols: 1,
            interval: INTERVAL_BASE_LINE,
        },
        workers: workers_baseline,
    };
    cases.push(c);

    // 4) 数据规模: 多表多行
    // NOTICE：实际测试发现，多表情况下，性能很差，暂时屏蔽
    // let c = TD2TDFactors {
    //     taosd_params: TaosdFactors {
    //         host: base.host.clone(),
    //         ws_enable: base.ws_enable,
    //         database_options: db_options.clone(),
    //     },
    //     data_params: DataFactors {
    //         tables: TABLES_BASE_LINE * 100,
    //         rows: ROWS_BASE_LINE,
    //         cols: 1,
    //         interval: INTERVAL_BASE_LINE,
    //     },
    //     workers: workers_baseline,
    // };
    // cases.push(c);

    // 5) 数据规模: interval
    for i in [60_000, 300_000] {
        let c: TD2TDFactors = TD2TDFactors {
            taosd_params: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_params: DataFactors {
                tables: TABLES_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: 1,
                interval: i,
            },
            workers: workers_baseline,
        };
        cases.push(c);
    }

    // 6) td2td workers
    for w in [workers_baseline / 2, workers_baseline * 2] {
        let c: TD2TDFactors = TD2TDFactors {
            taosd_params: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_params: DataFactors {
                tables: TABLES_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: 1,
                interval: INTERVAL_BASE_LINE,
            },
            workers: w,
        };
        cases.push(c);
    }

    Ok(cases)
}

/// 执行一组 td2td_history 的测试用例，结果输出到 td2td_history.csv
async fn run_hist_cases(params: Vec<TD2TDFactors>) -> anyhow::Result<()> {
    // Get CSV output directory from environment variable, fallback to current directory
    let csv_output_dir = std::env::var("CSV_OUTPUT_DIR").unwrap_or_else(|_| ".".to_string());
    let report_path = std::path::Path::new(&csv_output_dir).join("td2td_history.csv");

    // 测试生成的 metrics 写入到 td2td_history.csv
    let mut file: std::fs::File = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&report_path)?;

    // 只有当文件为空（新建或被清空）时才写入表头
    let need_header = file.metadata()?.len() == 0;
    // 获取 taosd 的版本信息
    let taosd_ver = taosd_version().await.unwrap_or("unknown".to_string());
    // 获取 taosx 的版本信息
    let taosx_ver = taosx_version().await.unwrap_or("unknown".to_string());

    for (idx, param) in params.iter().enumerate() {
        tracing::info!("Running with params: {:?}", param);
        let m = run_td2td_history(param.clone()).await?;

        if idx == 0 && need_header {
            // 写入csv表头
            let header = format!(
                "ts,TDengine Version,TaosX Version,{},{},{},{}\n",
                param.to_csv_header(),
                BasicMetrics::csv_header_with_prefix("write"),
                BasicMetrics::csv_header(),
                SysMetrics::csv_header()
            );
            file.write_all(header.as_bytes())?;
        }

        let now = Local::now().to_rfc3339();
        let csv_line = format!(
            "{},{},{},{},{},{},{}\n",
            now,
            taosd_ver,
            taosx_ver,
            param.to_csv_row(),
            m.write.csv_row(),
            m.mig.csv_row(),
            m.sys.csv_row()
        );

        file.write_all(csv_line.as_bytes())?;
        file.flush()?;
    }
    Ok(())
}

/// td2td_history 的影响因子
#[derive(Debug, Clone)]
struct TD2TDFactors {
    // taosd 相关的参数
    taosd_params: TaosdFactors,

    // 数据规模相关的参数
    data_params: DataFactors,

    // 迁移的工作线程数
    workers: usize,
}

impl TD2TDFactors {
    pub fn to_csv_header(&self) -> String {
        format!(
            "{},{},workers",
            self.taosd_params.csv_header(),
            self.data_params.csv_header()
        )
    }

    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{}",
            self.taosd_params.csv_row(),
            self.data_params.csv_row(),
            self.workers
        )
    }
}

/// td2td_history 输出的性能指标
#[derive(Debug, Clone)]
struct TD2TDHistryMetrics {
    write: BasicMetrics, // 写入的性能指标
    mig: BasicMetrics,   // 迁移的性能指标
    sys: SysMetrics,     // 系统负载
}

async fn run_td2td_history(params: TD2TDFactors) -> anyhow::Result<TD2TDHistryMetrics> {
    let pool = connect_taos_pool(&params.taosd_params.host, params.taosd_params.ws_enable).await?;

    // create DB_SRC and DB_DST
    const DB_SRC: &str = "pt_td2td_src";
    const DB_DST: &str = "pt_td2td_dst";

    // create DB_SRC and DB_DST
    let taos = pool.get().await?;
    recreate_databases(
        &taos,
        &[DB_SRC, DB_DST],
        &params.taosd_params.database_options,
    )
    .await?;

    let start = Utc::now().timestamp_millis() / 60000 * 60000;
    // simuate write
    let sim: Simulation = Simulation {
        db: DB_SRC.to_string(),
        writers: std::thread::available_parallelism()
            .map(|n| n.get() * 2)
            .unwrap_or(10),
        tables: params.data_params.tables,
        rows_per_table: params.data_params.rows,
        table_prefix: None,
        ts: start,
        step: params.data_params.interval,
        rows_per_sql: 50, // TODO: make it configurable
        speed_limit: None,
    };
    let write_metrics = simulate_write(&pool, &sim).await?;
    tracing::info!("simulate_write, {}", write_metrics);

    // legacy_to_taos
    let cancel = CancellationToken::new();
    let (from, to) = if params.taosd_params.ws_enable {
        let from = format!(
            "taos+ws://{}:6041/{DB_SRC}?mode=history&schema=always&workers={}",
            params.taosd_params.host, params.workers,
        ) // use scenario-specific workers
        .into_dsn()?;
        let to = format!("taos+ws://{}:6041/{DB_DST}", params.taosd_params.host).into_dsn()?;
        (from, to)
    } else {
        let from = format!(
            "taos://{}:6030/{DB_SRC}?mode=history&schema=always&workers={}",
            params.taosd_params.host, params.workers,
        ) // use scenario-specific workers
        .into_dsn()?;
        let to = format!("taos://{}:6030/{DB_DST}", params.taosd_params.host).into_dsn()?;
        (from, to)
    };
    let cancel_clone = cancel.clone();

    let t0 = Instant::now();
    let sys_metrics = collect_system_metrics(|| async {
        legacy_to_taos(from, vec![], to, cancel_clone, None)
            .await
            .unwrap();
    })
    .await;

    // 统计性能指标
    let time_cost = t0.elapsed().as_secs_f64();
    let total_rows = sim.tables * sim.rows_per_table;
    let rate = total_rows as f64 / time_cost;

    Ok(TD2TDHistryMetrics {
        write: write_metrics,
        mig: BasicMetrics {
            total_rows,
            time_cost,
            rate,
        },
        sys: sys_metrics,
    })
}

/// TD -> TD 实时数据迁移的负载性能测试
pub async fn td2td_realtime() -> anyhow::Result<()> {
    tracing::info!("TD -> TD Realtime Mode Performance Test");

    run_realtime_cases(gen_realtime_cases().await?).await
}

async fn gen_realtime_cases() -> anyhow::Result<Vec<TD2TDFactors>> {
    let cases = vec![];

    // todo

    Ok(cases)
}

async fn run_realtime_cases(_cases: Vec<TD2TDFactors>) -> anyhow::Result<()> {
    // todo

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gen_hist_cases() {
        let params = gen_hist_cases().await.unwrap();
        assert!(!params.is_empty());
        for (idx, p) in params.iter().enumerate() {
            if idx == 0 {
                println!("#,{}", p.to_csv_header());
            }
            println!("{},{}", idx + 1, p.to_csv_row());
        }
    }
}
