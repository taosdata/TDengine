use anyhow::Ok;
use chrono::{Local, Timelike};
use itertools::iproduct;
use rand::{Rng, SeedableRng, rngs::StdRng};
use source_csv::csv_to_taos;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    path::{Path, PathBuf},
    time::Instant,
};
use taos::{AsyncQueryable, Dsn, IntoDsn};
use taosx_core::{
    Parser,
    utils::{port_pool::PortPool, sql::connect_taos_pool},
};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

use crate::performance::TaosdFactorBaseLine;
use crate::performance::{
    BasicMetrics, DataFactors, SysMetrics, TaosdFactors, collect_system_metrics,
    recreate_databases,
    utils::{taosd_version, taosx_version},
};

// CSV -> TDengine 的负载性能测试
pub async fn csv2td() -> anyhow::Result<()> {
    tracing::info!("CSV -> TDengine Performance Test...");

    run_cases(gen_cases().await?).await
}

// 生成测试用例
async fn gen_cases() -> anyhow::Result<Vec<CSV2TDFactors>> {
    let mut cases = vec![];

    const TABLE_BASE_LINE: usize = 10000;
    const ROWS_BASE_LINE: usize = 10000;
    const COLS_BASE_LINE: usize = 1;
    const INTERVAL_BASE_LINE: i64 = 1000;
    const FILES_BASE_LINE: usize = 10;
    const BATCH_BASE_LINE: usize = 1000;
    const READ_CONCURRENCY_BASE_LINE: usize = 10;

    // 1. taosd 的参数，测试 vgroups 和 buffer 的组合
    let base = TaosdFactorBaseLine::new();
    for (v, b) in iproduct!(&base.vgroups, &base.buffers) {
        let mut database_options = BTreeMap::new();
        database_options.insert("VGROUPS".to_string(), v.to_string());
        database_options.insert("BUFFER".to_string(), b.to_string());

        let c = CSV2TDFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options,
            },
            data_factors: DataFactors {
                tables: TABLE_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: COLS_BASE_LINE,
                interval: INTERVAL_BASE_LINE,
            },
            files: FILES_BASE_LINE,
            batch_size: BATCH_BASE_LINE,
            null_value: None,
            read_concurrency: READ_CONCURRENCY_BASE_LINE,
        };
        cases.push(c);
    }

    let mut db_options = BTreeMap::new();
    db_options.insert("VGROUPS".to_string(), base.min_vgroups().to_string());
    db_options.insert("BUFFER".to_string(), base.max_buffer().to_string());

    // 2. 数据规模的参数：多表，少行
    let c = CSV2TDFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLE_BASE_LINE * 100,
            rows: ROWS_BASE_LINE / 10,
            cols: COLS_BASE_LINE,
            interval: INTERVAL_BASE_LINE,
        },
        files: FILES_BASE_LINE,
        batch_size: BATCH_BASE_LINE,
        null_value: None,
        read_concurrency: READ_CONCURRENCY_BASE_LINE,
    };
    cases.push(c);

    // 3. 数据规模：少表，多行，
    let c = CSV2TDFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLE_BASE_LINE,
            rows: ROWS_BASE_LINE * 10,
            cols: COLS_BASE_LINE,
            interval: INTERVAL_BASE_LINE,
        },
        files: FILES_BASE_LINE,
        batch_size: BATCH_BASE_LINE,
        null_value: None,
        read_concurrency: READ_CONCURRENCY_BASE_LINE,
    };
    cases.push(c);

    // 4. 数据规模：多表，多行
    let c = CSV2TDFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLE_BASE_LINE * 100,
            rows: ROWS_BASE_LINE,
            cols: COLS_BASE_LINE,
            interval: INTERVAL_BASE_LINE,
        },
        files: FILES_BASE_LINE,
        batch_size: BATCH_BASE_LINE,
        null_value: None,
        read_concurrency: READ_CONCURRENCY_BASE_LINE,
    };
    cases.push(c);

    // 5. 数据规模：step
    let c = CSV2TDFactors {
        taosd_factors: TaosdFactors {
            host: base.host.clone(),
            ws_enable: base.ws_enable,
            database_options: db_options.clone(),
        },
        data_factors: DataFactors {
            tables: TABLE_BASE_LINE,
            rows: ROWS_BASE_LINE,
            cols: COLS_BASE_LINE,
            interval: INTERVAL_BASE_LINE * 300, // 5分钟步长
        },
        files: FILES_BASE_LINE,
        batch_size: BATCH_BASE_LINE,
        null_value: None,
        read_concurrency: READ_CONCURRENCY_BASE_LINE,
    };
    cases.push(c);

    // 6. 数据规模：null_ratio
    for null_ratio in [0.1f32, 0.5f32, 0.9f32] {
        let c = CSV2TDFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_factors: DataFactors {
                tables: TABLE_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: COLS_BASE_LINE,
                interval: INTERVAL_BASE_LINE,
            },
            files: FILES_BASE_LINE,
            batch_size: BATCH_BASE_LINE,
            null_value: Some((null_ratio, "NaN".to_string())),
            read_concurrency: READ_CONCURRENCY_BASE_LINE,
        };
        cases.push(c);
    }

    // 7. csv2td 的参数: files
    for files in [1, 100] {
        let c = CSV2TDFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_factors: DataFactors {
                tables: TABLE_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: COLS_BASE_LINE,
                interval: INTERVAL_BASE_LINE,
            },
            files,
            batch_size: BATCH_BASE_LINE,
            null_value: None,
            read_concurrency: READ_CONCURRENCY_BASE_LINE,
        };
        cases.push(c);
    }

    // 8. csv2td 的参数: batch_size
    for batch_size in [100, 2000] {
        let c = CSV2TDFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_factors: DataFactors {
                tables: TABLE_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: COLS_BASE_LINE,
                interval: INTERVAL_BASE_LINE,
            },
            files: FILES_BASE_LINE,
            batch_size,
            null_value: None,
            read_concurrency: READ_CONCURRENCY_BASE_LINE,
        };
        cases.push(c);
    }

    // 9. csv2td 的参数: read_concurrency
    for read_concurrency in [2, 50] {
        let c = CSV2TDFactors {
            taosd_factors: TaosdFactors {
                host: base.host.clone(),
                ws_enable: base.ws_enable,
                database_options: db_options.clone(),
            },
            data_factors: DataFactors {
                tables: TABLE_BASE_LINE,
                rows: ROWS_BASE_LINE,
                cols: COLS_BASE_LINE,
                interval: INTERVAL_BASE_LINE,
            },
            files: FILES_BASE_LINE,
            batch_size: BATCH_BASE_LINE,
            null_value: None,
            read_concurrency,
        };
        cases.push(c);
    }

    Ok(cases)
}

// 运行所有测试用例
async fn run_cases(cases: Vec<CSV2TDFactors>) -> anyhow::Result<()> {
    // 用例的性能指标写入到 csv2td.csv
    const REPORT: &str = "csv2td.csv";
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
        let m = run_csv2td(case).await?;

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
            "{},{},{},{},{},{},{:.2},{:.2},{}\n",
            now,
            taosd_ver,
            taosx_ver,
            case.to_csv_row(),
            m.write.csv_row(),
            m.total_rows,
            m.time_cost,
            m.rate,
            m.sys.csv_row()
        );

        file.write_all(csv_line.as_bytes())?;
        file.flush()?;
    }
    Ok(())
}

// 运行单个 csv2td 用例，返回性能指标
async fn run_csv2td(f: &CSV2TDFactors) -> anyhow::Result<CSV2TDMetrics> {
    tracing::info!("Run csv2td case: {:?}", f);
    const DB: &str = "pt_csv2td";

    // 初始化 database
    let pool = connect_taos_pool(&f.taosd_factors.host, f.taosd_factors.ws_enable).await?;
    let taos = pool.get().await?;
    recreate_databases(&taos, &[DB], &f.taosd_factors.database_options).await?;
    // 创建超级表
    taos.query(format!(
        "CREATE STABLE IF NOT EXISTS `{DB}`.stb (
            ts TIMESTAMP,
            {}
        ) TAGS (gid INT, tid INT)",
        (1..=f.data_factors.cols)
            .map(|i| format!("col{} FLOAT", i))
            .collect::<Vec<_>>()
            .join(", ")
    ))
    .await?;

    let csv_dir = std::env::var("CSV_DIR")
        .map(PathBuf::from)
        .unwrap_or(tempdir()?.path().to_path_buf());
    tracing::info!("CSV dir: {}", csv_dir.display());

    // 模拟数据，写入到 csv 中
    let write_metrics = simulate_write_csv(csv_dir.as_path(), f).await?;

    // 执行 csv2td
    let (from, parser, to) = build_dsn(csv_dir.as_path(), f, DB)?;
    let cancel = CancellationToken::new();
    let (tx, rx) = flume::unbounded();
    let t0 = Instant::now();
    let drain = tokio::spawn(async move { while rx.recv_async().await.is_ok() {} });
    let sys_metrics = collect_system_metrics(|| async {
        if let Err(err) = csv_to_taos(
            from,
            parser,
            to,
            &PortPool::default(),
            cancel,
            None,
            None,
            None,
            tx,
        )
        .await
        {
            tracing::error!("csv2td failed: {:?}", err);
        }
    })
    .await;
    drain.await.ok();

    // 统计性能指标
    let time_cost = t0.elapsed().as_secs_f64();
    let total_rows = f.data_factors.tables * f.data_factors.rows;
    let rate = total_rows as f64 / time_cost;

    // 检查数据行数
    let rows: u64 = taos
        .query_one(format!("SELECT count(*) FROM `{DB}`.stb"))
        .await?
        .unwrap_or(0);
    assert_eq!(rows, total_rows as u64);

    Ok(CSV2TDMetrics {
        write: write_metrics,
        total_rows,
        time_cost,
        rate,
        sys: sys_metrics,
    })
}

// 在 csv_dir 下生成 CSV 文件，返回写入的性能指标
async fn simulate_write_csv(csv_dir: &Path, f: &CSV2TDFactors) -> anyhow::Result<BasicMetrics> {
    let tables = f.data_factors.tables.max(1);
    let rows_per_table = f.data_factors.rows.max(1);
    let cols = f.data_factors.cols.max(1);
    let interval_ms = f.data_factors.interval; // 毫秒步长
    let files = f.files.max(1); // 分组 (= 文件个数)

    fs::create_dir_all(csv_dir)?;
    // 先清空目录下的所有文件
    for entry in fs::read_dir(csv_dir)? {
        let entry = entry?;
        if entry.path().is_file() {
            fs::remove_file(entry.path())?;
        }
    }

    // 计算每个 group(文件) 分配的表数量：先平均，再把余数 1 个分配给前 remainder 个 group。
    let mut group_table_counts = vec![tables / files; files];
    for i in group_table_counts.iter_mut().take(tables % files) {
        *i += 1;
    }

    // 空值注入配置
    let (null_ratio, null_pattern) = f
        .null_value
        .as_ref()
        .map(|(r, p)| (*r, p.clone()))
        .unwrap_or((0.0f32, String::new()));
    let enable_null = null_ratio > 0.0 && !null_pattern.is_empty();

    let base_ts = Local::now().with_second(0).unwrap();
    let t0 = Instant::now();

    // 并发写入：每个 group(文件) 一个 blocking 任务
    let mut handles = Vec::with_capacity(group_table_counts.len());
    for (group_id, &tables_in_group) in group_table_counts.iter().enumerate() {
        if tables_in_group == 0 {
            continue;
        }
        let file_path = csv_dir.join(format!("part_{}.csv", group_id));
        let null_pattern_cloned = null_pattern.clone();
        let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            // 为每个任务单独创建 RNG，保证并发安全 & 基本可重复性
            let mut rng = StdRng::seed_from_u64(0x5454_4f58 ^ (group_id as u64));
            let fh = File::create(&file_path)?;
            let mut w = BufWriter::new(fh);

            // 表头
            write!(w, "tbname,ts")?;
            for i in 1..=cols {
                write!(w, ",col{}", i)?;
            }
            writeln!(w, ",gid,tid")?;

            let gid = group_id + 1; // gid 从 1 开始
            for table_idx in 0..tables_in_group {
                let tid = table_idx + 1; // tid 从 1 开始
                let tbname = format!("t_{}_{}", gid, tid);
                for row_idx in 0..rows_per_table {
                    let ts = base_ts + chrono::Duration::milliseconds(interval_ms * row_idx as i64);
                    let ts_str = ts.format("%Y-%m-%dT%H:%M:%S%:z");
                    write!(w, "{},{},", tbname, ts_str)?; // tbname, ts
                    for c in 0..cols {
                        let r: f32 = rng.gen_range(0.0..1.0);
                        if enable_null && r < null_ratio {
                            write!(w, "{}", null_pattern_cloned)?;
                        } else {
                            let base = ((gid * 10_000 + tid * 1_000 + row_idx * 10 + (c + 1))
                                % 10_000) as f64
                                / 100.0;
                            let noise: f64 = rng.gen_range(0.0..1.0);
                            let val = base + noise;
                            write!(w, "{:.2}", val)?;
                        }
                        if c + 1 < cols {
                            write!(w, ",")?;
                        }
                    }
                    writeln!(w, ",{},{}", gid, tid)?; // gid, tid
                }
            }
            w.flush()?;
            Ok(())
        });
        handles.push(handle);
    }

    // 等待所有文件写入完成
    for h in handles {
        h.await??;
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let total_rows = tables * rows_per_table; // 不含表头
    let rate = if elapsed > 0.0 {
        total_rows as f64 / elapsed
    } else {
        0.0
    };

    Ok(BasicMetrics {
        total_rows,
        time_cost: elapsed,
        rate,
    })
}

fn build_dsn(
    csv_dir: &Path,
    f: &CSV2TDFactors,
    db: &str,
) -> anyhow::Result<(Dsn, Option<Parser>, Dsn)> {
    // build from DSN
    let mut from = format!(
        "csv:{}/*.csv?batch_size={}&read_concurrency={}",
        csv_dir.display(),
        f.batch_size,
        f.read_concurrency
    );
    if let Some((_, null_pattern)) = &f.null_value {
        from.push_str(format!("&null_pattern={null_pattern}").as_str());
    }
    let from = from.into_dsn()?;

    // build parser
    let cols = f.data_factors.cols;
    use serde_json::Value as JsonValue;
    use serde_json::json;
    // build parse section
    let mut parse_obj = serde_json::Map::new();
    parse_obj.insert("ts".to_string(), json!({"as": "TIMESTAMP(ms)"}));
    for i in 1..=cols {
        parse_obj.insert(format!("col{i}"), json!({"as": "FLOAT"}));
    }
    parse_obj.insert("gid".to_string(), json!({"as": "INT"}));
    parse_obj.insert("tid".to_string(), json!({"as": "INT"}));

    // model columns list
    let mut model_columns: Vec<JsonValue> = Vec::with_capacity(cols + 1);
    model_columns.push(json!("ts"));
    for i in 1..=cols {
        model_columns.push(json!(format!("col{i}")));
    }

    let parser_value = json!({
        "parse": JsonValue::Object(parse_obj),
        "model": {
            "name": "${tbname}",
            "using": "stb",
            "tags": ["gid", "tid"],
            "columns": model_columns,
        }
    });
    let parser: Parser = serde_json::from_value(parser_value)?;

    // build to DSN
    let to = if f.taosd_factors.ws_enable {
        format!("taos+ws://{}:6041/{}", f.taosd_factors.host, db).into_dsn()?
    } else {
        format!("taos://{}:6030/{}", f.taosd_factors.host, db).into_dsn()?
    };

    Ok((from, Some(parser), to))
}

/// csv2td 的影响因子
#[derive(Debug, Clone)]
struct CSV2TDFactors {
    // taosd 的参数：VGROUPS, BUFFER
    taosd_factors: TaosdFactors,
    // 写入的数据规模：
    data_factors: DataFactors,

    // CSV 文件个数: 1, 10, 100
    files: usize,
    // 批次大小: 100, 1000, 2000
    batch_size: usize,
    // (空值比率，空值 pattern)
    null_value: Option<(f32, String)>,
    // 读取 CSV 的并发，默认是 2
    read_concurrency: usize,
}

impl CSV2TDFactors {
    fn to_csv_header(&self) -> String {
        format!(
            "{},{},{},{},{},{},{}",
            self.taosd_factors.csv_header(),
            self.data_factors.csv_header(),
            "files",
            "batch_size",
            "null_ratio",
            "null_pattern",
            "read_concurrency"
        )
    }

    fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{},{},{}",
            self.taosd_factors.csv_row(),
            self.data_factors.csv_row(),
            self.files,
            self.batch_size,
            self.null_value
                .as_ref()
                .map(|(r, _)| r.to_string())
                .unwrap_or_else(|| "0".to_string()),
            self.null_value
                .as_ref()
                .map(|(_, p)| p.clone())
                .unwrap_or("".to_string()),
            self.read_concurrency
        )
    }
}

/// csv2td 的性能指标
#[derive(Debug, Clone)]
struct CSV2TDMetrics {
    write: BasicMetrics, // 写入的性能指标

    total_rows: usize, // 总行数
    time_cost: f64,    // 总耗时
    rate: f64,         // 速度

    sys: SysMetrics, // 系统负载
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
                println!("#,{}", case.to_csv_header());
            }
            println!("{},{}", idx + 1, case.to_csv_row());
        }
    }

    #[test]
    fn test_build_dsn() {
        // given
        let csv_dir = tempdir().unwrap();
        let f = CSV2TDFactors {
            taosd_factors: TaosdFactors {
                host: "127.0.0.1".to_string(),
                ws_enable: false,
                database_options: BTreeMap::new(),
            },
            data_factors: DataFactors {
                tables: 10000,
                rows: 10000,
                cols: 3,
                interval: 1000,
            },
            files: 2,
            batch_size: 10,
            null_value: Some((0.1, "NaN".to_string())),
            read_concurrency: 2,
        };

        // when
        let (from, parser, to) = build_dsn(csv_dir.path(), &f, "db").unwrap();

        // then
        assert_eq!(
            from,
            format!(
                "csv:{}/*.csv?batch_size=10&read_concurrency=2&null_pattern=NaN",
                csv_dir.path().display()
            )
            .into_dsn()
            .unwrap()
        );
        let s = r#"{
        "parse":{
            "ts":{"as":"TIMESTAMP(ms)"},
            "col1":{"as":"FLOAT"},
            "col2":{"as":"FLOAT"},
            "col3":{"as":"FLOAT"},
            "gid":{"as":"INT"},
            "tid":{"as":"INT"}
        },
        "model":{
            "name":"${tbname}",
            "using":"stb",
            "tags":["gid","tid"],
            "columns":["ts","col1","col2","col3"]
        }}"#;
        let expected: Parser = serde_json::from_str(s).unwrap();
        assert_eq!(parser.unwrap(), expected);
        assert_eq!(to, "taos://127.0.0.1:6030/db".into_dsn().unwrap());
    }

    #[tokio::test]
    async fn test_simulate_write_csv() {
        // given
        let csv_dir = Path::new("./tttt");
        let f = CSV2TDFactors {
            taosd_factors: TaosdFactors {
                host: "127.0.0.1".to_string(),
                ws_enable: false,
                database_options: BTreeMap::new(),
            },
            data_factors: DataFactors {
                tables: 1000,
                rows: 1000,
                cols: 5,
                interval: 1000,
            },
            files: 5,
            batch_size: 10,
            null_value: Some((0.1, "NaN".to_string())),
            read_concurrency: 2,
        };

        // when
        let m = simulate_write_csv(csv_dir, &f).await.unwrap();
        println!("metrics: {:?}", m);

        // then
        let csv_files = std::fs::read_dir(csv_dir)
            .unwrap()
            .filter_map(|e| {
                let p = e.unwrap().path();
                if p.extension().map(|ext| ext == "csv").unwrap_or(false) {
                    Some(p)
                } else {
                    None
                }
            })
            .collect::<Vec<PathBuf>>();
        // 应该在目录下生成 files 个 csv 文件
        assert_eq!(f.files, csv_files.len());
        // 所有文件的总行数应该是 tables * rows + files (表头)
        let total_rows: usize = csv_files
            .iter()
            .map(|p| {
                let content = std::fs::read_to_string(p).unwrap();
                content.lines().count()
            })
            .sum();
        assert_eq!(
            f.data_factors.tables * f.data_factors.rows + f.files,
            total_rows
        );
        // 所有的NaN 数量应该接近 10% (允许有偏差)
        let total_nans: usize = csv_files
            .iter()
            .map(|p| {
                let content = std::fs::read_to_string(p).unwrap();
                content.matches("NaN").count()
            })
            .sum();
        let ratio =
            total_nans as f32 / (f.data_factors.tables * f.data_factors.rows * f.files) as f32;
        assert!((0.05..0.15).contains(&ratio));
    }
}
