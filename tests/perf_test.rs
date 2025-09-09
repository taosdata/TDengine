use performance::*;

mod performance;

/// 运行性能测试用例，输出性能指标
/// 1. 通过环境变量 PERF_CASES 指定要运行的用例 (逗号分隔)
/// PERF_CASES 支持的用例包括: td2td_history, tmq2td, csv2td
/// 2. 通过环境变量 CSV_OUTPUT_DIR 指定要输出的目录，默认是当前目录
/// example:
/// ```
/// # 运行所有测试用例
/// HOST=192.168.2.139 WS_ENABLE=false cargo nextest run test_performance --retries 0 --nocapture
///
/// # 指定 PERF_CASES 运行部分测试用例
/// PERF_CASES=tmq2td CSV_OUTPUT_DIR=/root/perf_test/records HOST=192.168.2.125 WS_ENABLE=false cargo nextest run test_performance --retries 0 --nocapture
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_performance() -> anyhow::Result<()> {
    // 设置日志
    // tracing_subscriber::fmt::fmt().with_level(true).init();
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    const CASES: &[&str] = &[
        "td2td_history",
        "td2td_realtime",
        "tmq2td",
        "csv2td",
        "tmq2local",
    ];

    // 如果设置 PERF_CASES，则按其出现的顺序执行；忽略不在 CASES 中的项；去重但保持首次出现顺序
    let mut selected_cases: Vec<String> = Vec::new();
    if let Ok(list) = std::env::var("PERF_CASES") {
        for item in list
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
        {
            if CASES.contains(&item.as_str()) && !selected_cases.iter().any(|c| c == &item) {
                selected_cases.push(item);
            }
        }
    }

    for case in selected_cases {
        match case.as_str() {
            "td2td_history" => td2td_history().await?,
            "td2td_realtime" => td2td_realtime().await?,
            "tmq2td" => tmq2td().await?,
            "csv2td" => csv2td().await?,
            "tmq2local" => tmq2local().await?,
            _ => {}
        }
    }

    Ok(())
}
