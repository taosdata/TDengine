use std::fs;
use std::path::Path;
use anyhow::anyhow;

pub fn get_files_in_dir(dir: &str, ext: &str) -> Result<Vec<String>, anyhow::Error> {
    let path = Path::new(dir);
    if !path.is_dir() {
        return Err(anyhow!(format!("path {} is not dir", dir)));
    }

    let mut files = vec![];
    let mut stack = vec![path.to_path_buf()];

    while let Some(p) = stack.pop() {
        let dir_files = fs::read_dir(p)?;
        for entry in dir_files {
            let entry_path = entry?.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
                continue;
            }
            if let Some(file) = entry_path.to_str().filter(|f| ext.is_empty() || f.ends_with(ext)) {
                files.push(file.to_owned());
            }
        }
    }

    Ok(files)
}
