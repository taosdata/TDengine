use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::anyhow;
use chardetng::EncodingDetector;
use encoding_rs::Encoding;

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
            if let Some(file) = entry_path
                .to_str()
                .filter(|f| ext.is_empty() || f.ends_with(ext))
            {
                files.push(file.to_owned());
            }
        }
    }

    Ok(files)
}

pub fn get_encode(file_path: &str) -> anyhow::Result<&'static Encoding> {
    let mut file = File::open(file_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to open file: {}, cause: {}",
            file_path,
            e.to_string()
        )
    })?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).map_err(|e| {
        anyhow::anyhow!(
            "failed to read file: {}, cause: {}",
            file_path,
            e.to_string()
        )
    })?;

    get_encode_from_buffer(buffer.as_slice())
}

pub fn get_encode_from_buffer(buffer: &[u8]) -> anyhow::Result<&'static Encoding> {
    let mut detector = EncodingDetector::new();
    detector.feed(buffer, true);

    let encoding = detector.guess(None, true);

    Ok(encoding)
}

pub fn decompress_and_write_file(
    path: &std::path::PathBuf,
    data: &[u8],
) -> Result<(), std::io::Error> {
    use std::io::Write;
    let decode_buf = Vec::new();
    let mut decoder = flate2::write::GzDecoder::new(decode_buf);
    decoder.write_all(data)?;
    let writer = decoder.finish()?;
    let mut file = File::create(path)?;
    file.write_all(&writer)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_file_encode() {
        let file_path = "./tests/opc/opcua-utf8.csv";
        let encode = get_encode(file_path).unwrap();
        assert_eq!(encode.name(), "UTF-8");

        let file_path = "./tests/opc/opcua-utf8bom.csv";
        let encode = get_encode(file_path).unwrap();
        assert_eq!(encode.name(), "UTF-8");

        let file_path = "./tests/opc/opcua-gbk.csv";
        let encode = get_encode(file_path).unwrap();
        assert_eq!(encode.name(), "GBK");
    }
}
