use anyhow::Context;
use base64::{Engine, engine::general_purpose};

pub async fn encode_csv_config_file(csv_path: String) -> anyhow::Result<String> {
    let mut new_value = String::new();

    // TODO use mime instead
    let (files, strs): (Vec<String>, Vec<String>) = csv_path
        .split(",")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .partition(|v| v.starts_with("@"));
    let file_len = files.len();
    for file in files {
        tracing::debug!(
            "current dir: {}",
            std::env::current_dir().unwrap().to_str().unwrap()
        );
        let file_data = tokio::fs::read(&file[1..])
            .await
            .with_context(|| anyhow::format_err!("Failed to read file: {}", &file[1..]))?;
        new_value.push_str(general_purpose::STANDARD.encode(file_data).as_str());
        new_value.push(',');
    }
    if file_len > 0 {
        new_value.pop();
    }
    let str_len = strs.len();
    for content in strs {
        new_value.push_str(content.as_str());
        new_value.push(',');
    }
    if str_len > 0 {
        new_value.pop();
    }

    Ok(new_value)
}
