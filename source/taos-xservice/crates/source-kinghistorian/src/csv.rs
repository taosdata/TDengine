use std::path::PathBuf;

use anyhow::Context;
use faststr::FastStr;
use taos::Dsn;
use taosx_core::plugins::sink::point::csv::CsvParser;
use taosx_core::utils::{self, parse_key_in_dsn};

pub const DEFAULT_CSV_HEADERS: [&str; 14] = [
    "tag_name",
    "stable",
    "tbname",
    "enabled",
    "value_col",
    "value_transform",
    "type",
    "quality_col",
    "ts_col",
    "ts_transform",
    "request_ts_col",
    "request_ts_transform",
    "received_ts_col",
    "received_ts_transform",
];

// 解析 csv_config_file 参数
pub fn parse_csv(dsn: &Dsn) -> anyhow::Result<(Option<PathBuf>, Option<FastStr>)> {
    let csv = parse_key_in_dsn::<String>(dsn, "csv_config_file")?;
    let csv = if let Some(csv) = csv {
        csv
    } else {
        return Ok((None, None));
    };

    // 如果以 @ 开头，则表示是文件路径
    if let Some(file_path) = csv.strip_prefix("@") {
        let path = PathBuf::from(file_path);

        // check file exists
        if !path.exists() {
            return Err(anyhow::anyhow!("csv_config_file not exists: {:?}", csv));
        }

        // read file content
        let context = std::fs::read_to_string(&path).map_err(|err| {
            anyhow::anyhow!("failed to read csv_config_file: {:?}, cause: {}", csv, err)
        })?;

        Ok((Some(path), Some(context.into())))
    } else {
        // dsn 中直接包含 csv 内容，且 csv 是 URL encoded
        let content = utils::files::decode_csv_content(&csv, true)?;
        let content = String::from_utf8(content)
            .context("failed to convert DSN's csv_config_file to string")?;

        Ok((None, Some(content.into())))
    }
}

/// 检查 csv 配置是否合法
pub async fn is_csv_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
    // DSN 中的 csv_config_file：
    // 1. 以 @ 开头，表示是文件路径
    // 2. URL encoded 的 csv 内容，在 agent 情况下使用
    let parser = CsvParser::from_dsn(dsn)
        .map_err(|err| anyhow::anyhow!("failed to parse dsn: {}, cause: {:?}", dsn, err))?;

    let model_config = parser
        .parse()
        .await
        .map_err(|err| anyhow::anyhow!("failed to parse dsn: {}, cause: {:?}", dsn, err))?;

    // 检查 csv 文件是否满足合法性
    model_config
        .validate()
        .map_err(|err| anyhow::anyhow!("failed to validate csv file, cause: {:?}", err))?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn test_is_csv_valid_impl() {
        let csv_path = match std::env::var("KINGHIST_CSV").ok() {
            Some(path) => path,
            None => concat!(env!("CARGO_MANIFEST_DIR"), "/example/kinghist.csv").to_string(),
        };
        let dsn = format!("kinghist:///?csv_config_file=@{}", csv_path)
            .parse::<Dsn>()
            .unwrap();

        let instant = Instant::now();
        let result = is_csv_valid_impl(&dsn).await;

        println!(
            "is_csv_valid_impl time cost: {} ms",
            instant.elapsed().as_millis()
        );
        assert!(result.is_ok(), "CSV should be valid: {:?}", result);
    }
}
