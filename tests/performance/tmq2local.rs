use std::{
    collections::{BTreeMap, HashSet},
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::Ok;
use chrono::{Local, Utc};
use itertools::iproduct;
use taos::{Dsn, IntoDsn};
use taosx_core::utils::sql::connect_taos_pool;
use tempfile::tempdir;
use tmq_to_local::tmq_to_local;
use tokio_util::sync::CancellationToken;

use crate::performance::{
    BasicMetrics, DataFactors, Simulation, SysMetrics, TaosdFactorBaseLine, TaosdFactors,
    collect_system_metrics, drop_related_topics, recreate_databases, simulate_write,
    utils::{taosd_version, taosx_version},
};

// TMQ -> Local 数据备份的性能测试
pub async fn tmq2local() -> anyhow::Result<()> {
    tracing::info!("TMQ -> Local Performance Test");

    run_cases(gen_cases().await?).await
}

// 生成测试用例
async fn gen_cases() -> anyhow::Result<Vec<Tmq2LocalFactors>> {
    let mut cases = vec![];

    const TABLES_BASE_LINE: usize = 10000;
    const ROWS_BASE_LINE: usize = 10000;
    const COLS_BASE_LINE: usize = 1;
    const INTERVAL_BASE_LINE: i64 = 1000;
    const TIMEOUT_BASE_LINE: usize = 5000; // 5s
    const MAX_SIZE_BASE_LINE: &str = "1GB";
    const COMPRESS_BASE_LINE: &str = "fastest";

    // 1. taosd 的参数，测试 vgroups 和 buffer 的组合
    let base = TaosdFactorBaseLine::new();
    for (v, b) in iproduct!(&base.vgroups, &base.buffers) {
        let mut database_options = BTreeMap::new();
        database_options.insert("VGROUPS".to_string(), v.to_string());
        database_options.insert("BUFFER".to_string(), b.to_string());

        let c = Tmq2LocalFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options,
            },
            data_factors: DataFactors {
                tables: TABLES_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: COLS_BASE_LINE,
                interval: INTERVAL_BASE_LINE,
            },
            timeout: Some(TIMEOUT_BASE_LINE),
            max_size: MAX_SIZE_BASE_LINE.to_string(),
            compress: COMPRESS_BASE_LINE.to_string(),
        };
        cases.push(c);
    }

    // 通过历史的测试记录可知，大 BUFFER，小 VGROUPS 的组合性能较好
    let mut db_options = BTreeMap::new();
    db_options.insert("VGROUPS".to_string(), base.min_vgroups().to_string());
    db_options.insert("BUFFER".to_string(), base.max_buffer().to_string());

    // 2. 数据规模: 少表，多行
    let c = Tmq2LocalFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLES_BASE_LINE,
            rows: ROWS_BASE_LINE * 10,
            cols: COLS_BASE_LINE,
            interval: INTERVAL_BASE_LINE,
        },
        timeout: Some(TIMEOUT_BASE_LINE),
        max_size: MAX_SIZE_BASE_LINE.to_string(),
        compress: COMPRESS_BASE_LINE.to_string(),
    };
    cases.push(c);

    // 3. 数据规模: 多表，少行
    let c = Tmq2LocalFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLES_BASE_LINE * 100,
            rows: ROWS_BASE_LINE / 10,
            cols: COLS_BASE_LINE,
            interval: INTERVAL_BASE_LINE,
        },
        timeout: Some(TIMEOUT_BASE_LINE),
        max_size: MAX_SIZE_BASE_LINE.to_string(),
        compress: COMPRESS_BASE_LINE.to_string(),
    };
    cases.push(c);

    // 4. 数据规模: 多表，多行
    let c = Tmq2LocalFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLES_BASE_LINE * 100,
            rows: ROWS_BASE_LINE,
            cols: COLS_BASE_LINE,
            interval: INTERVAL_BASE_LINE,
        },
        timeout: Some(TIMEOUT_BASE_LINE),
        max_size: MAX_SIZE_BASE_LINE.to_string(),
        compress: COMPRESS_BASE_LINE.to_string(),
    };
    cases.push(c);

    // 5. tmq2local 的参数: backup.max.size
    let max_sizes = vec!["5MB", "500MB", "2GB"];
    for size in max_sizes {
        let c = Tmq2LocalFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_factors: DataFactors {
                tables: TABLES_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: COLS_BASE_LINE,
                interval: INTERVAL_BASE_LINE,
            },
            timeout: Some(TIMEOUT_BASE_LINE),
            max_size: size.to_string(),
            compress: COMPRESS_BASE_LINE.to_string(),
        };
        cases.push(c);
    }

    // 6. tmq2local 的参数: backup.comp.level
    let compress_levels = vec!["best", "balanced"];
    for level in compress_levels {
        let c = Tmq2LocalFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_factors: DataFactors {
                tables: TABLES_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: COLS_BASE_LINE,
                interval: INTERVAL_BASE_LINE,
            },
            timeout: Some(TIMEOUT_BASE_LINE),
            max_size: MAX_SIZE_BASE_LINE.to_string(),
            compress: level.to_string(),
        };
        cases.push(c);
    }

    // 去重（保持原有顺序）
    let mut deduped = Vec::with_capacity(cases.len());
    let mut seen = HashSet::new();
    for c in cases.into_iter() {
        let key = c.to_csv_row(); // 该行包含所有影响唯一性的参数
        if seen.insert(key) {
            deduped.push(c);
        }
    }

    Ok(deduped)
}

// 运行所有测试用例
async fn run_cases(cases: Vec<Tmq2LocalFactors>) -> anyhow::Result<()> {
    // 用例的性能指标写入到 tmq2local.csv
    const REPORT: &str = "tmq2local.csv";
    let csv_output_dir = std::env::var("CSV_OUTPUT_DIR").unwrap_or(".".to_string());
    let report_path = std::path::Path::new(&csv_output_dir).join(REPORT);

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

    for (idx, case) in cases.iter().enumerate() {
        let m = run_tmq2local(case.clone()).await?;

        if idx == 0 && need_header {
            // 写入csv表头
            let header = format!(
                "ts,TDengine Version,TaosX Version,{},{},{},{}\n",
                case.to_csv_header(),
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
            case.to_csv_row(),
            m.write.csv_row(),
            m.backup.csv_row(),
            m.sys.csv_row()
        );

        file.write_all(csv_line.as_bytes())?;
        file.flush()?;
    }
    Ok(())
}

// 运行单个测试用例
async fn run_tmq2local(f: Tmq2LocalFactors) -> anyhow::Result<Tmq2LocalMetrics> {
    tracing::info!("Run tmq2local case: {:?}", f);
    const DB_SRC: &str = "tmq2local";

    let temp_dir = tempdir()?;
    let backup_dir = std::env::var("TMQ2LOCAL_BACKUP_DIR")
        .map(PathBuf::from)
        .unwrap_or(temp_dir.path().to_path_buf());

    // 初始化 backup_dir
    if backup_dir.exists() {
        std::fs::remove_dir_all(&backup_dir)?;
    }
    std::fs::create_dir_all(&backup_dir)?;
    tracing::info!("TMQ -> Local use backup_dir: {:?}", backup_dir);

    // 初始化 database
    let pool = connect_taos_pool(&f.taosd_factors.host, f.taosd_factors.ws_enable).await?;
    let taos = pool.get().await?;
    drop_related_topics(&taos, &[DB_SRC]).await?;
    recreate_databases(&taos, &[DB_SRC], &f.taosd_factors.database_options).await?;

    // 模拟数据，写入到 TDengine
    let start = Utc::now().timestamp_millis() / 60000 * 60000;
    let sim = Simulation {
        db: DB_SRC.to_string(),
        writers: 10,
        tables: f.data_factors.tables,
        rows_per_table: f.data_factors.rows,
        table_prefix: None,
        ts: start,
        step: f.data_factors.interval,
        rows_per_sql: 50,
        speed_limit: None,
    };
    let write_metrics = simulate_write(&pool, &sim).await?;
    tracing::info!("simluate write for tmq2local test, {}", write_metrics);

    // 执行 tmq2local 备份
    let t0 = Instant::now();
    let cancel = CancellationToken::new();
    let (from, to) = build_dsn(DB_SRC, backup_dir.as_path(), &f)?;
    let sys_metrics = collect_system_metrics(|| async {
        tmq_to_local(None, from, to, cancel).await.unwrap();
    })
    .await;

    drop(temp_dir);

    // 统计性能指标
    let time_cost = t0.elapsed().as_secs_f64();
    let total_rows = sim.tables * sim.rows_per_table;
    let rate = total_rows as f64 / time_cost;

    Ok(Tmq2LocalMetrics {
        write: write_metrics,
        backup: BasicMetrics {
            total_rows,
            time_cost,
            rate,
        },
        sys: sys_metrics,
    })
}

fn build_dsn(
    database: &str,
    backup_dir: &Path,
    f: &Tmq2LocalFactors,
) -> anyhow::Result<(Dsn, Dsn)> {
    let mut from = if f.taosd_factors.ws_enable {
        format!(
            "tmq+ws://{}:6041/{}?upcoming=now",
            f.taosd_factors.host, database
        )
    } else {
        format!(
            "tmq://{}:6030/{}?upcoming=now",
            f.taosd_factors.host, database
        )
    };
    if let Some(timeout) = f.timeout {
        from.push_str(format!("&timeout={timeout}ms").as_str());
    }
    let from = from.into_dsn()?;

    let to = format!(
        "local:{}?backup_max_size={}&compression_level={}",
        backup_dir.display(),
        f.max_size,
        f.compress
    )
    .into_dsn()?;

    Ok((from, to))
}

#[derive(Clone, Debug)]
struct Tmq2LocalFactors {
    taosd_factors: TaosdFactors,
    data_factors: DataFactors,

    // 超时时间，毫秒单位
    timeout: Option<usize>,
    // 文件大小
    max_size: String,
    // 压缩等级
    compress: String,
}

impl Tmq2LocalFactors {
    fn to_csv_header(&self) -> String {
        format!(
            "{},{},timeout(ms),backup_max_size,compression_level",
            self.taosd_factors.csv_header(),
            self.data_factors.csv_header(),
        )
    }

    fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{}",
            self.taosd_factors.csv_row(),
            self.data_factors.csv_row(),
            self.timeout
                .map(|t| format!("{t}"))
                .unwrap_or("None".to_string()),
            self.max_size,
            self.compress,
        )
    }
}

#[derive(Clone, Debug)]
struct Tmq2LocalMetrics {
    write: BasicMetrics,  // 写入的性能指标
    backup: BasicMetrics, // 备份的性能指标
    sys: SysMetrics,      // 系统负载
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_gen_cases() {
        let cases = gen_cases().await.unwrap();
        assert!(!cases.is_empty());
        for (idx, case) in cases.iter().enumerate() {
            if idx == 0 {
                println!("#,{}", case.to_csv_header());
            }
            println!("{},{}", idx + 1, case.to_csv_row());
        }
    }

    #[test]
    fn test_build_dsn() {
        // given
        let f = &Tmq2LocalFactors {
            taosd_factors: TaosdFactors {
                host: "127.0.0.1".to_string(),
                ws_enable: true,
                database_options: BTreeMap::new(),
            },
            data_factors: DataFactors::default(),
            timeout: Some(5000),
            max_size: "1GB".to_string(),
            compress: "fastest".to_string(),
        };

        // when
        let (from, to) = build_dsn("test", PathBuf::from_str("./").unwrap().as_path(), f).unwrap();

        // then
        assert_eq!(
            from,
            "tmq+ws://127.0.0.1:6041/test?upcoming=now&timeout=5000ms"
                .into_dsn()
                .unwrap()
        );
        assert_eq!(
            to,
            "local:./?compression_level=fastest&backup_max_size=1GB"
                .into_dsn()
                .unwrap()
        );
    }
}
