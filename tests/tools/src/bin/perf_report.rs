use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(name = "perf-report", about = "生成性能测试报告主页与详情页")]
struct Args {
    /// 输入目录，包含 perf_cases.toml 以及对应的 CSV 文件
    #[arg(short = 'i', long = "input", default_value = "tests/tools/sample")]
    input: PathBuf,
    /// 输出目录 (会创建)
    #[arg(
        short = 'o',
        long = "output",
        default_value = "tests/tools/dist/perf-report-gen"
    )]
    output: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Config {
    #[allow(dead_code)]
    version: Option<u32>,
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize, Clone)]
struct Case {
    id: String,
    name: String,
    description: Option<String>,
    file: String,
    #[serde(default)]
    analysis: Option<AnalysisCfg>,
}

#[derive(Debug, Deserialize, Clone)]
struct AnalysisCfg {
    // 指标列（一个或多个）；兼容旧字段 target 已在上层配置更新后不再使用
    #[serde(default)]
    metrics: Vec<String>,
    #[serde(default)]
    factors: Vec<String>,
}

#[derive(Debug, Default)]
struct ColumnStats {
    min: f64,
    max: f64,
    count: usize,
}

#[derive(Clone)]
struct ScatterPoint {
    row: usize, // 行号 (1-based)
    ts: i64,
    val: f64,
    td_raw: String, // full raw TDengine Version cell
    tx_raw: String, // full raw TaosX Version cell
}

fn main() {
    if let Err(e) = real_main() {
        eprintln!("生成报告失败: {e:#}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<()> {
    let args = Args::parse();
    run(args)
}

fn run(args: Args) -> Result<()> {
    let config_path = args.input.join("perf_cases.toml");
    let cfg_text = fs::read_to_string(&config_path)
        .with_context(|| format!("读取配置文件失败: {}", config_path.display()))?;
    let cfg: Config = toml::from_str(&cfg_text).context("解析配置文件 TOML 失败")?;
    if cfg.cases.is_empty() {
        bail!("配置中没有 cases");
    }

    fs::create_dir_all(&args.output).context("创建输出目录失败")?;

    // 生成每个用例详情
    for case in &cfg.cases {
        generate_case_page(case, &args.input, &args.output)?;
    }

    // 主页
    generate_index_page(&cfg.cases, &args.output)?;

    println!("报告生成完成: {}", args.output.display());
    Ok(())
}

fn generate_case_page(case: &Case, data_dir: &Path, output: &Path) -> Result<()> {
    let csv_path = data_dir.join(&case.file);
    let mut html = String::new();
    html.push_str(&format!("<!doctype html><html lang=\"zh\"><head><meta charset=\"utf-8\"><title>{} - 性能测试报告</title><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />{}</head><body>", escape(&case.name), STYLE));
    html.push_str(&format!("<h1>{}</h1>", escape(&case.name)));
    if let Some(desc) = &case.description {
        html.push_str(&format!("<p class=desc>{}</p>", escape(desc)));
    }
    html.push_str("<p><a href=\"index.html\">&larr; 返回主页</a></p>");

    match fs::read(&csv_path) {
        Ok(bytes) => {
            let delim = detect_delimiter(&bytes);
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(true)
                .delimiter(delim)
                .from_reader(&bytes[..]);
            let headers = rdr.headers()?.clone();
            let mut records: Vec<csv::StringRecord> = Vec::new();
            let mut stats: HashMap<String, ColumnStats> = HashMap::new();
            let mut scatter_points: Vec<ScatterPoint> = Vec::new();

            let mut tdengine_idx: Option<usize> = None;
            let mut taosx_idx: Option<usize> = None;
            let mut ts_idx: Option<usize> = None;
            let mut target_idx: Option<usize> = None;

            for (i, h) in headers.iter().enumerate() {
                let low = h.to_ascii_lowercase();
                if low.replace(' ', "") == "tdengineversion" {
                    tdengine_idx = Some(i);
                } else if low.replace(' ', "") == "taosxversion" {
                    taosx_idx = Some(i);
                } else if low == "ts" {
                    ts_idx = Some(i);
                }
            }
            if let Some(ana) = &case.analysis {
                if let Some(first_metric) = ana.metrics.first() {
                    for (i, h) in headers.iter().enumerate() {
                        if h.eq_ignore_ascii_case(first_metric) {
                            target_idx = Some(i);
                            break;
                        }
                    }
                }
            } else {
                for (i, h) in headers.iter().enumerate() {
                    if h.eq_ignore_ascii_case("mig_rate") {
                        target_idx = Some(i);
                        break;
                    }
                }
            }

            let mut trend_min_ts: Option<i64> = None;
            let mut trend_max_ts: Option<i64> = None;
            let mut trend_min_v: Option<f64> = None;
            let mut trend_max_v: Option<f64> = None;

            for rec in rdr.records() {
                let rec = match rec {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("跳过损坏记录: {e}");
                        continue;
                    }
                };
                if let (Some(ts_i), Some(val_i)) = (ts_idx, target_idx) {
                    if let (Some(ts_raw), Some(val_raw)) = (rec.get(ts_i), rec.get(val_i)) {
                        let ts_raw_trim = ts_raw.trim();
                        if !ts_raw_trim.is_empty() {
                            if let Ok(val) = val_raw.trim().parse::<f64>() {
                                if let Some(ts_epoch) = parse_ts_to_epoch(ts_raw_trim) {
                                    if trend_min_ts.map(|m| ts_epoch < m).unwrap_or(true) {
                                        trend_min_ts = Some(ts_epoch);
                                    }
                                    if trend_max_ts.map(|m| ts_epoch > m).unwrap_or(true) {
                                        trend_max_ts = Some(ts_epoch);
                                    }
                                    if trend_min_v.map(|m| val < m).unwrap_or(true) {
                                        trend_min_v = Some(val);
                                    }
                                    if trend_max_v.map(|m| val > m).unwrap_or(true) {
                                        trend_max_v = Some(val);
                                    }
                                    let td_full = tdengine_idx
                                        .and_then(|idx| rec.get(idx))
                                        .map(|s| s.trim().to_string())
                                        .unwrap_or_default();
                                    let tx_full = taosx_idx
                                        .and_then(|idx| rec.get(idx))
                                        .map(|s| s.trim().to_string())
                                        .unwrap_or_default();
                                    scatter_points.push(ScatterPoint {
                                        row: records.len() + 1,
                                        ts: ts_epoch,
                                        val,
                                        td_raw: td_full,
                                        tx_raw: tx_full,
                                    });
                                }
                            }
                        }
                    }
                }
                for (idx, val) in rec.iter().enumerate() {
                    if val.is_empty() {
                        continue;
                    }
                    if let Ok(num) = val.parse::<f64>() {
                        let key = headers.get(idx).unwrap_or("").to_string();
                        let entry = stats.entry(key).or_insert_with(|| ColumnStats {
                            min: num,
                            max: num,
                            count: 0,
                        });
                        if num < entry.min {
                            entry.min = num;
                        }
                        if num > entry.max {
                            entry.max = num;
                        }
                        entry.count += 1;
                    }
                }
                records.push(rec);
            }

            // 1. 相关性分析
            html.push_str(&render_factor_analysis_section(
                case, &headers, &records, &stats,
            ));
            // 2. 趋势分析
            html.push_str(&render_trend_section(
                case,
                &scatter_points,
                trend_min_ts,
                trend_max_ts,
                trend_min_v,
                trend_max_v,
            ));
            // 3. 测试记录
            html.push_str(&render_records_section(
                &headers,
                &records,
                tdengine_idx,
                taosx_idx,
            ));
        }
        Err(e) => {
            html.push_str(&format!(
                "<p style=\"color:#c00\">读取 CSV 失败: {} ({})</p>",
                escape(&csv_path.display().to_string()),
                escape(&e.to_string())
            ));
        }
    }

    html.push_str("<footer><p>生成时间: ");
    html.push_str(
        &chrono::Local::now()
            .format("%Y-%m-%d %H:%M:%S %z")
            .to_string(),
    );
    html.push_str(" | 自动生成 | <a href=\"index.html\">主页</a></p></footer></body></html>");
    let outfile = output.join(format!("{}.html", case.id));
    fs::write(&outfile, html).with_context(|| format!("写入详情页失败: {}", outfile.display()))?;
    Ok(())
}

fn render_factor_analysis_section(
    case: &Case,
    headers: &csv::StringRecord,
    records: &Vec<csv::StringRecord>,
    stats: &HashMap<String, ColumnStats>,
) -> String {
    // 如果有分析配置，则对每个 metric 单独生成一小节
    if let Some(ana) = &case.analysis {
        let mut s = String::new();
        s.push_str("<section class=sec><h2>相关性分析</h2>");
        if ana.metrics.is_empty() {
            s.push_str("<p>metrics 为空。</p></section>");
            return s;
        }
        if ana.metrics.len() > 1 {
            s.push_str(&format!(
                "<p style='font-size:12px;color:#555'>分析指标: {}</p>",
                escape(&ana.metrics.join(", "))
            ));
        }
        for metric in &ana.metrics {
            let conclusions = compute_metric_correlations(headers, records, metric, &ana.factors);
            s.push_str(&format!("<div class='metric-block' style='margin-top:18px'><h3 style='margin:8px 0 4px'>指标: {}</h3>", escape(metric)));
            if conclusions.is_empty() {
                s.push_str("<p style='font-size:12px;color:#666'>没有可分析的有效因子。</p>");
            } else {
                for (i, c) in conclusions.iter().enumerate() {
                    s.push_str(&format!("<div class=conclusion><strong>{}.</strong> {} <br/><span class=metric>{}</span></div>", i+1, escape(&c.title), escape(&c.detail)));
                }
            }
            s.push_str("</div>");
        }
        s.push_str("</section>");
        return s;
    }
    // 无分析配置时维持兼容逻辑
    let conclusions = if case.id == "td2td_history" {
        derive_mig_rate_param_analysis(headers, records)
    } else {
        derive_conclusions_for_case(case, stats)
    };
    let mut s = String::new();
    s.push_str("<section class=sec><h2>相关性分析</h2>");
    if conclusions.is_empty() {
        s.push_str("<p>暂无可生成的相关性分析结果。</p>");
    } else {
        for (i, c) in conclusions.iter().enumerate() {
            s.push_str(&format!("<div class=conclusion><strong>{}.</strong> {} <br/><span class=metric>{}</span></div>", i+1, escape(&c.title), escape(&c.detail)));
        }
    }
    s.push_str("</section>");
    s
}

// 针对单个 metric 计算各 factor 与其的 Pearson 相关性，并生成结论列表
fn compute_metric_correlations(
    headers: &csv::StringRecord,
    records: &Vec<csv::StringRecord>,
    metric: &str,
    factors: &Vec<String>,
) -> Vec<Conclusion> {
    let target_idx = headers.iter().position(|h| h.eq_ignore_ascii_case(metric));
    let Some(target_idx) = target_idx else {
        return vec![Conclusion {
            title: format!("未找到列 {metric}"),
            detail: String::from("请检查配置中的 metrics"),
        }];
    };
    let mut y_vals: Vec<f64> = Vec::new();
    let mut row_valid: Vec<bool> = Vec::new();
    for rec in records {
        if let Some(v) = rec.get(target_idx) {
            if let Ok(num) = v.trim().parse::<f64>() {
                y_vals.push(num);
                row_valid.push(true);
                continue;
            }
        }
        row_valid.push(false);
    }
    if y_vals.len() < 3 {
        return vec![Conclusion {
            title: format!("{metric} 数据不足"),
            detail: "有效行数 < 3".into(),
        }];
    }
    #[derive(Debug)]
    struct FactorR {
        r: f64,
        title: String,
        detail: String,
    }
    let mut out: Vec<FactorR> = Vec::new();
    for fname in factors {
        let fidx = headers.iter().position(|h| h.eq_ignore_ascii_case(fname));
        let Some(fidx) = fidx else { continue }; // 忽略未找到列
        let mut xs: Vec<f64> = Vec::new();
        let mut ys: Vec<f64> = Vec::new();
        for (row, rec) in records.iter().enumerate() {
            if !row_valid[row] {
                continue;
            }
            let fx = rec.get(fidx).unwrap_or("").trim();
            let fy = rec.get(target_idx).unwrap_or("").trim();
            if fx.is_empty() || fy.is_empty() {
                continue;
            }
            if let (Ok(x), Ok(y)) = (fx.parse::<f64>(), fy.parse::<f64>()) {
                xs.push(x);
                ys.push(y);
            }
        }
        if xs.len() < 3 {
            continue;
        }
        let mut uniq = std::collections::BTreeSet::new();
        for &x in &xs {
            uniq.insert(x.to_bits());
        }
        if uniq.len() < 2 {
            continue;
        }
        let n = xs.len() as f64;
        let (mut sx, mut sy, mut sx2, mut sy2, mut sxy) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for (&x, &y) in xs.iter().zip(&ys) {
            sx += x;
            sy += y;
            sx2 += x * x;
            sy2 += y * y;
            sxy += x * y;
        }
        let num = n * sxy - sx * sy;
        let den = ((n * sx2 - sx * sx) * (n * sy2 - sy * sy)).sqrt();
        if den <= 0.0 {
            continue;
        }
        let r = (num / den).clamp(-1.0, 1.0);
        use std::collections::HashMap;
        #[derive(Default)]
        struct Agg {
            sum: f64,
            cnt: usize,
        }
        let mut map: HashMap<String, Agg> = HashMap::new();
        for (&x, &y) in xs.iter().zip(&ys) {
            let k = trim_float(x);
            let a = map.entry(k).or_default();
            a.sum += y;
            a.cnt += 1;
        }
        let mut avg: Vec<(String, f64, usize)> = map
            .into_iter()
            .map(|(k, a)| (k.clone(), a.sum / a.cnt as f64, a.cnt))
            .collect();
        avg.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let mut best_worst = avg.clone();
        best_worst.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let ratio = if let (Some(w), Some(b)) = (best_worst.first(), best_worst.last()) {
            if w.1 > 0.0 {
                b.1 / w.1
            } else {
                0.0
            }
        } else {
            0.0
        };
        let mut detail = format!("metric={metric}; r={:.3}; 平均 {metric}: ", r);
        for (i, (k, v, c)) in avg.iter().enumerate() {
            if i > 0 {
                detail.push_str(", ");
            }
            detail.push_str(&format!("{}={:.2} (n={})", k, v, c));
        }
        if ratio > 1.0 {
            detail.push_str(&format!("; 最佳/最差≈{:.2}x", ratio));
        }
        let corr_sign = if r >= 0.0 { "正相关" } else { "负相关" };
        let title = format!("{} -> {} ({} |r|={:.3})", fname, metric, corr_sign, r.abs());
        out.push(FactorR { r, title, detail });
    }
    if out.is_empty() {
        return vec![];
    }
    out.sort_by(|a, b| {
        b.r.abs()
            .partial_cmp(&a.r.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out.into_iter()
        .take(10)
        .map(|f| Conclusion {
            title: f.title,
            detail: f.detail,
        })
        .collect()
}

fn render_trend_section(
    case: &Case,
    scatter_points: &Vec<ScatterPoint>,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    min_v: Option<f64>,
    max_v: Option<f64>,
) -> String {
    if scatter_points.is_empty()
        || min_ts.is_none()
        || max_ts.is_none()
        || min_v.is_none()
        || max_v.is_none()
    {
        return String::new();
    }
    // 按天聚合取最大
    let mut by_day: HashMap<i64, ScatterPoint> = HashMap::new();
    for p in scatter_points {
        let day = p.ts / 86_400;
        by_day
            .entry(day)
            .and_modify(|e| {
                if p.val > e.val {
                    *e = p.clone();
                }
            })
            .or_insert_with(|| p.clone());
    }
    let mut daily: Vec<ScatterPoint> = by_day.into_values().collect();
    daily.sort_by_key(|p| p.ts);
    let (mut min_ts_d, mut max_ts_d, mut min_v_d, mut max_v_d) =
        (i64::MAX, i64::MIN, f64::MAX, f64::MIN);
    for p in &daily {
        if p.ts < min_ts_d {
            min_ts_d = p.ts;
        }
        if p.ts > max_ts_d {
            max_ts_d = p.ts;
        }
        if p.val < min_v_d {
            min_v_d = p.val;
        }
        if p.val > max_v_d {
            max_v_d = p.val;
        }
    }
    if min_v_d == f64::MAX {
        min_v_d = 0.0;
    }
    if max_v_d == f64::MIN {
        max_v_d = 0.0;
    }
    let mut s = String::new();
    s.push_str("<section class=sec><h2>趋势分析</h2>");
    let target_name = case
        .analysis
        .as_ref()
        .and_then(|a| a.metrics.first().map(|s| s.as_str()))
        .unwrap_or("mig_rate");
    s.push_str(&build_scatter_svg(
        "性能趋势",
        &daily,
        min_ts_d,
        max_ts_d,
        min_v_d,
        max_v_d,
        target_name,
    ));
    s.push_str("<p style='font-size:12px;color:#555'>按天聚合：仅展示每天最高的 ");
    s.push_str(&escape(target_name));
    s.push_str(" 值。</p></section>");
    s
}

fn render_records_section(
    headers: &csv::StringRecord,
    records: &[csv::StringRecord],
    td_idx: Option<usize>,
    tx_idx: Option<usize>,
) -> String {
    let render_version_multiline = |raw: &str| -> String {
        if raw.contains(';') {
            raw.split(';')
                .map(|s| escape(s.trim()))
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join("<br/>")
        } else {
            escape(raw.trim())
        }
    };
    let mut s = String::new();
    s.push_str(&format!(
        "<section class=sec><h2>测试记录 ({} 条)</h2>",
        records.len()
    ));
    s.push_str("<div style=\"overflow:auto;max-width:100%;border:1px solid #ccc\"><table><thead><tr><th>#</th>");
    for h in headers.iter() {
        s.push_str(&format!("<th>{}</th>", escape(h)));
    }
    s.push_str("</tr></thead><tbody>");
    for (ridx, rec) in records.iter().enumerate() {
        s.push_str("<tr>");
        s.push_str(&format!("<td>{}</td>", ridx + 1));
        for (cidx, v) in rec.iter().enumerate() {
            if Some(cidx) == td_idx || Some(cidx) == tx_idx {
                s.push_str(&format!("<td>{}</td>", render_version_multiline(v)));
            } else {
                s.push_str(&format!("<td>{}</td>", escape(v)));
            }
        }
        s.push_str("</tr>");
    }
    s.push_str("</tbody></table></div></section>");
    s
}

fn generate_index_page(cases: &[Case], output: &Path) -> Result<()> {
    let mut html = String::new();
    html.push_str(&format!("<!doctype html><html lang=\"zh\"><head><meta charset=\"utf-8\"><title>性能测试报告主页</title><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />{}</head><body><h1>性能测试用例总览</h1>", STYLE));
    html.push_str("<section class=sec><ul class=case-list>");
    for case in cases {
        let desc = case.description.clone().unwrap_or_default();
        html.push_str(&format!(
            "<li><a href=\"{}.html\"><strong>{}</strong></a><br/><span class=desc>{}</span></li>",
            escape(&case.id),
            escape(&case.name),
            escape(&desc)
        ));
    }
    html.push_str("</ul></section><footer><p>生成时间: ");
    html.push_str(
        &chrono::Local::now()
            .format("%Y-%m-%d %H:%M:%S %z")
            .to_string(),
    );
    html.push_str(" | 自动生成 | </p></footer></body></html>");
    let outfile = output.join("index.html");
    fs::write(&outfile, html).with_context(|| format!("写入主页失败: {}", outfile.display()))?;
    Ok(())
}

struct Conclusion {
    title: String,
    detail: String,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Factor {
    name: String,
    r: f64,
    detail: String,
}

fn derive_conclusions_for_case(
    case: &Case,
    stats: &HashMap<String, ColumnStats>,
) -> Vec<Conclusion> {
    if case.id == "td2td_history" {
        return vec![];
    }
    derive_conclusions(stats)
}

fn derive_conclusions(stats: &HashMap<String, ColumnStats>) -> Vec<Conclusion> {
    if stats.is_empty() {
        return vec![];
    }
    const KEYWORDS: &[&str] = &["rate", "qps", "throughput", "duration", "latency", "time"];
    let mut interesting: Vec<(String, &ColumnStats)> = stats
        .iter()
        .filter(|(k, v)| v.count > 0 && KEYWORDS.iter().any(|kw| k.to_lowercase().contains(kw)))
        .map(|(k, v)| (k.clone(), v))
        .collect();
    if interesting.is_empty() {
        interesting = stats.iter().map(|(k, v)| (k.clone(), v)).collect();
    }
    interesting.sort_by(|a, b| {
        b.1.max
            .partial_cmp(&a.1.max)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    interesting
        .into_iter()
        .take(5)
        .map(|(k, v)| {
            let title = format!("{} 指标", k);
            let detail = format!("max = {:.4}, min = {:.4} ({} 条)", v.max, v.min, v.count);
            Conclusion { title, detail }
        })
        .collect()
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

const STYLE: &str = r#"<style>
body{font-family:Arial,Helvetica,sans-serif;margin:40px;line-height:1.5}
h1,h2{margin-top:1.4em}
.case-list{list-style:none;padding:0}
.case-list li{margin:12px 0;padding:8px 12px;border:1px solid #ddd;border-radius:6px;background:#fafafa}
.case-list a{text-decoration:none;color:#2563eb}
.case-list a:hover{text-decoration:underline}
.sec{margin-top:32px}
.conclusion{border-left:4px solid #2563eb;background:#f0f7ff;padding:8px 12px;margin:8px 0;border-radius:4px}
.metric{color:#555;font-size:12px}
table{border-collapse:collapse;margin:1em 0;font-size:12px}
th,td{border:1px solid #999;padding:4px 6px;vertical-align:top;white-space:nowrap}
th{background:#eee}
footer{margin-top:60px;font-size:11px;color:#666}
.desc{color:#555;font-size:13px}
.trend{margin-top:24px;position:relative}
.tip{position:fixed;z-index:9999;background:rgba(0,0,0,0.88);color:#fff;padding:6px 8px;font-size:12px;border-radius:4px;line-height:1.4;max-width:420px;box-shadow:0 2px 6px rgba(0,0,0,0.3);white-space:pre;pointer-events:auto;user-select:text;font-family:monospace;}
</style>"#;

fn detect_delimiter(data: &[u8]) -> u8 {
    let mut has_tab = false;
    for &b in data.iter().take(8192) {
        if b == b'\n' {
            break;
        }
        if b == b'\t' {
            has_tab = true;
            break;
        }
    }
    if has_tab {
        b'\t'
    } else {
        b','
    }
}

fn derive_mig_rate_param_analysis(
    headers: &csv::StringRecord,
    records: &Vec<csv::StringRecord>,
) -> Vec<Conclusion> {
    let mut mig_idx: Option<usize> = None;
    for (i, h) in headers.iter().enumerate() {
        if h.trim().eq_ignore_ascii_case("mig_rate") {
            mig_idx = Some(i);
            break;
        }
    }
    let mig_idx = match mig_idx {
        Some(i) => i,
        None => return vec![],
    };

    const PARAM_CANDIDATES: &[&str] = &[
        "BUFFER",
        "MINROWS",
        "STT_TRIGGER",
        "VGROUPS",
        "tables",
        "rows",
        "cols",
        "step",
        "writers",
        "workers",
    ];
    let mut param_indices: Vec<(usize, &str)> = Vec::new();
    for &p in PARAM_CANDIDATES {
        for (i, h) in headers.iter().enumerate() {
            if h.eq_ignore_ascii_case(p) {
                param_indices.push((i, p));
                break;
            }
        }
    }

    #[derive(Debug)]
    struct FactorResult {
        r: f64,
        title: String,
        detail: String,
    }

    let mut factors: Vec<FactorResult> = Vec::new();

    for (pidx, pname) in param_indices {
        let mut xs: Vec<f64> = Vec::new();
        let mut ys: Vec<f64> = Vec::new();
        for rec in records {
            let px = rec.get(pidx).unwrap_or("").trim();
            let py = rec.get(mig_idx).unwrap_or("").trim();
            if px.is_empty() || py.is_empty() {
                continue;
            }
            if let (Ok(x), Ok(y)) = (px.parse::<f64>(), py.parse::<f64>()) {
                xs.push(x);
                ys.push(y);
            }
        }
        if xs.len() < 3 {
            continue;
        }
        let mut distinct = std::collections::BTreeSet::new();
        for &x in &xs {
            distinct.insert(x.to_bits());
        }
        if distinct.len() < 2 {
            continue;
        }

        let n = xs.len() as f64;
        let (mut sum_x, mut sum_y, mut sum_x2, mut sum_y2, mut sum_xy) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            sum_x += x;
            sum_y += y;
            sum_x2 += x * x;
            sum_y2 += y * y;
            sum_xy += x * y;
        }
        let num = n * sum_xy - sum_x * sum_y;
        let den = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)).sqrt();
        if den <= 0.0 {
            continue;
        }
        let r = (num / den).clamp(-1.0, 1.0);

        use std::collections::HashMap;
        #[derive(Default)]
        struct Agg {
            sum: f64,
            cnt: usize,
        }
        let mut by_val: HashMap<String, Agg> = HashMap::new();
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            let key = trim_float(x);
            let a = by_val.entry(key).or_default();
            a.sum += y;
            a.cnt += 1;
        }
        let mut avg_entries: Vec<(String, f64, usize)> = by_val
            .into_iter()
            .map(|(k, a)| {
                let avg = a.sum / a.cnt as f64;
                (k, avg, a.cnt)
            })
            .collect();
        avg_entries.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let mut avg_by_rate = avg_entries
            .iter()
            .map(|e| (e.0.clone(), e.1))
            .collect::<Vec<_>>();
        avg_by_rate.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let ratio_part =
            if let (Some(worst), Some(best)) = (avg_by_rate.first(), avg_by_rate.last()) {
                if worst.1 > 0.0 {
                    format!("; 最佳/最差≈{:.2}x", best.1 / worst.1)
                } else {
                    String::new()
                }
            } else {
                String::new()
            };

        let corr_sign = if r >= 0.0 { "正相关" } else { "负相关" };
        let title = format!("参数 {} {} |r|={:.3}", pname, corr_sign, r.abs());
        let mut detail = format!("r={:.3}; 平均 mig_rate: ", r);
        for (i, (val, avg, cnt)) in avg_entries.iter().enumerate() {
            if i > 0 {
                detail.push_str(", ");
            }
            detail.push_str(&format!("{}={:.2} (n={})", val, avg, cnt));
        }
        detail.push_str(&ratio_part);
        factors.push(FactorResult { r, title, detail });
    }

    factors.sort_by(|a, b| {
        b.r.abs()
            .partial_cmp(&a.r.abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    factors
        .into_iter()
        .take(10)
        .map(|f| Conclusion {
            title: f.title,
            detail: f.detail,
        })
        .collect()
}

fn trim_float(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{:.0}", v)
    } else {
        format!("{:.2}", v)
    }
}

fn parse_ts_to_epoch(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }
    if let Ok(sec) = s.parse::<i64>() {
        return Some(sec);
    }
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(naive.and_utc().timestamp());
    }
    None
}

fn build_scatter_svg(
    title: &str,
    points: &Vec<ScatterPoint>,
    min_ts: i64,
    max_ts: i64,
    min_v: f64,
    max_v: f64,
    target: &str,
) -> String {
    if points.is_empty() {
        return format!(
            "<div class=trend><h3>{}</h3><p>无数据</p></div>",
            escape(title)
        );
    }
    let min_day = min_ts / 86_400;
    let max_day = max_ts / 86_400;
    let day_span = (max_day - min_day).max(1) as f64;
    let width = 900f64;
    let height = 300f64;
    let pl = 60f64; // left padding
    let pr = 20f64; // right padding
    let pt = 20f64; // top padding
    let pb = 50f64; // bottom padding
    let plot_w = width - pl - pr;
    let plot_h = height - pt - pb;
    let span_v = (max_v - min_v).max(1e-9);

    let mut svg = String::new();
    svg.push_str(&format!(
        "<div class=trend style='position:relative'><h3>{}</h3>",
        escape(title)
    ));
    svg.push_str(&format!("<svg viewBox='0 0 {} {}' width='100%' height='{}' style='background:#fafafa;border:1px solid #ddd' class='scatter'>", width as i32, height as i32, height as i32));

    // axes
    svg.push_str(&format!(
        "<line x1='{pl}' y1='{pt}' x2='{pl}' y2='{}' stroke='#333' stroke-width='1' />",
        pt + plot_h
    ));
    svg.push_str(&format!(
        "<line x1='{pl}' y1='{}' x2='{}' y2='{}' stroke='#333' stroke-width='1' />",
        pt + plot_h,
        pl + plot_w,
        pt + plot_h
    ));

    // y labels (min / max)
    svg.push_str(&format!(
        "<text x='{}' y='{}' font-size='10' text-anchor='end'>{:.2}</text>",
        pl - 4.0,
        pt + 4.0,
        max_v
    ));
    svg.push_str(&format!(
        "<text x='{}' y='{}' font-size='10' text-anchor='end'>{:.2}</text>",
        pl - 4.0,
        pt + plot_h,
        min_v
    ));

    // vertical grid + x ticks
    let total_days = (max_day - min_day + 1).max(1);
    let tick_step = if total_days <= 14 {
        1
    } else {
        (total_days / 10).max(1)
    };
    let mut day = min_day;
    while day <= max_day {
        let x = pl + ((day - min_day) as f64 / day_span) * plot_w;
        svg.push_str(&format!(
            "<line x1='{:.2}' y1='{}' x2='{:.2}' y2='{}' stroke='#eee' stroke-width='1' />",
            x,
            pt,
            x,
            pt + plot_h
        ));
        if (day - min_day) % tick_step == 0 || day == max_day {
            let day_ts = day * 86_400;
            let label = chrono::DateTime::from_timestamp(day_ts, 0)
                .map(|d| d.format("%m-%d").to_string())
                .unwrap_or_else(|| day.to_string());
            svg.push_str(&format!(
                "<text x='{:.2}' y='{}' font-size='10' text-anchor='middle'>{}</text>",
                x,
                pt + plot_h + 14.0,
                label
            ));
        }
        day += 1;
    }

    // y axis label
    svg.push_str(&format!("<text x='{}' y='{}' font-size='10' text-anchor='middle' transform='rotate(-90 {} {} )'>{}</text>", 14.0, pt + plot_h / 2.0, 14.0, pt + plot_h / 2.0, escape(target)));

    // points
    let color = "#2563eb";
    for p in points {
        let day = p.ts / 86_400;
        let x = pl + ((day - min_day) as f64 / day_span) * plot_w;
        let y = pt + plot_h - ((p.val - min_v) / span_v) * plot_h;
        let ts_fmt = chrono::DateTime::from_timestamp(p.ts, 0)
            .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| p.ts.to_string());
        let mut lines: Vec<String> = Vec::new();
        lines.push(format!("row: {}", p.row));
        lines.push(format!("{}: {:.4}", target, p.val));
        lines.push(format!("ts: {}", ts_fmt));
        lines.push("TDengine Version:".into());
        for seg in p.td_raw.split(';') {
            let seg = seg.trim();
            if !seg.is_empty() {
                lines.push(seg.to_string());
            }
        }
        lines.push("TaosX Version:".into());
        for seg in p.tx_raw.split(';') {
            let seg = seg.trim();
            if !seg.is_empty() {
                lines.push(seg.to_string());
            }
        }
        let info_txt = escape(&lines.join("\n"));
        svg.push_str(&format!("<circle class='pt' cx='{:.2}' cy='{:.2}' r='5' fill='{color}' data-info='{}'></circle>", x, y, info_txt));
    }

    svg.push_str("</svg>");
    // tooltip script (initialized once)
    svg.push_str(r#"<script>(function(){if(window.__perfTooltipInit)return;window.__perfTooltipInit=true;const tip=document.createElement('div');tip.className='tip';tip.id='perf-tip';tip.style.display='none';document.body.appendChild(tip);let showTimer=null,hideTimer=null;function position(e){const r=tip.getBoundingClientRect();let x=e.clientX+16,y=e.clientY+16;if(x+r.width>window.innerWidth-8)x=window.innerWidth-8-r.width;if(y+r.height>window.innerHeight-8)y=window.innerHeight-8-r.height;tip.style.left=x+'px';tip.style.top=y+'px';}function scheduleHide(){if(hideTimer)clearTimeout(hideTimer);hideTimer=setTimeout(()=>{if(!tip.matches(':hover')){tip.style.display='none';}},250);}document.addEventListener('mouseover',e=>{const el=e.target;if(el instanceof SVGCircleElement && el.classList.contains('pt')){if(hideTimer)clearTimeout(hideTimer);if(showTimer)clearTimeout(showTimer);showTimer=setTimeout(()=>{const info=el.getAttribute('data-info');if(!info)return;tip.textContent=info;tip.style.display='block';position(e);},100);}else if(!tip.contains(el)){scheduleHide();}});document.addEventListener('mousemove',e=>{if(tip.style.display==='block')position(e);});tip.addEventListener('mouseleave',scheduleHide);tip.addEventListener('mouseenter',()=>{if(hideTimer)clearTimeout(hideTimer);});})();</script>"#);
    svg.push_str("</div>");
    svg
}
