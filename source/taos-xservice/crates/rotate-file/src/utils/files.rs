use std::{fs, path::PathBuf};

pub fn list_dir_files(dir: &PathBuf) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let dir = fs::read_dir(dir)?;
    for entry in dir {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.exists() {
            files.push(path);
        }
    }
    Ok(files)
}

pub fn scan_files_with<F>(dir: &PathBuf, predicate: F) -> std::io::Result<Vec<PathBuf>>
where
    F: Fn(&PathBuf) -> bool,
{
    let files = list_dir_files(dir)?;
    let files = files.into_iter().filter(|f| predicate(f)).collect();
    Ok(files)
}

pub fn file_size(path: &PathBuf) -> std::io::Result<u64> {
    let meta = fs::metadata(path)?;
    Ok(meta.len())
}
