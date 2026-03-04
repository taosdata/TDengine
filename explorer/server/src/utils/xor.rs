use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::Local;
use thiserror::Error;

pub struct TimeBasedXor {
    allowed_duration_in_seconds: u64,
}

impl TimeBasedXor {
    pub const fn new(allowed_duration_in_seconds: u64) -> Self {
        Self {
            allowed_duration_in_seconds,
        }
    }

    pub fn decrypt(&self, data: &str) -> Result<String, XorError> {
        let (timestamp, encrypted_data) = data.split_once('.').ok_or(XorError::InvalidData)?;
        let timestamp = timestamp
            .parse::<i64>()
            .map_err(|_| XorError::InvalidTimestamp)?;
        let current_time = Local::now().timestamp();
        if current_time - timestamp > self.allowed_duration_in_seconds as i64 {
            return Err(XorError::Expired);
        }
        let bytes = BASE64_STANDARD.decode(encrypted_data)?;
        let key = Self::gen_key(timestamp);
        let decrypted = decrypt_xor(&key, &bytes);
        String::from_utf8(decrypted).map_err(Into::into)
    }

    #[cfg(test)]
    pub fn encrypt<T: AsRef<[u8]>>(&self, data: T) -> Result<String, XorError> {
        let timestamp = Local::now().timestamp();
        let key = Self::gen_key(timestamp);
        let encrypted = encrypt_xor(&key, data.as_ref());
        let encrypted_data = BASE64_STANDARD.encode(encrypted);
        Ok(format!("{}.{}", timestamp, encrypted_data))
    }

    #[inline]
    fn gen_key(timestamp: i64) -> [u8; 24] {
        let mut key = [0; 24];
        let prefix = litcrypt::lc!("taosdataexplorer");
        key[0..16].copy_from_slice(prefix.as_bytes());
        key[16..24].copy_from_slice(&timestamp.to_be_bytes());
        key
    }
}

#[derive(Debug, Error)]
pub enum XorError {
    #[error("Base64 decoding data error: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("Xor decoding caused UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Invalid data")]
    InvalidData,
    #[error("Invalid timestamp")]
    InvalidTimestamp,
    #[error("Time-based xor decoding expired")]
    Expired,
}

pub fn decrypt_xor(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(data.len());
    let mut key_index = 0;

    for byte in data {
        result.push(byte ^ key[key_index]);
        key_index = (key_index + 1) % key.len();
    }

    result
}

#[cfg(test)]
pub fn encrypt_xor(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(data.len());
    let mut key_index = 0;

    for byte in data {
        result.push(byte ^ key[key_index]);
        key_index = (key_index + 1) % key.len();
    }

    result
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_xor() {
        let key = b"secret";
        let data = b"hello";
        let encrypted = encrypt_xor(key, data);
        let decrypted = decrypt_xor(key, &encrypted);

        assert_eq!(decrypted, data);
    }

    #[test]
    fn test_time_based_xor() {
        let xor = TimeBasedXor::new(0);
        let data = b"hello";
        let encrypted = xor.encrypt(data).unwrap();
        std::thread::sleep(Duration::from_secs(1));
        let expired = xor.decrypt(&encrypted).expect_err("expired");
        assert!(matches!(expired, XorError::Expired));

        let invalid_data = "invalid";
        let invalid_data_err = xor.decrypt(invalid_data).expect_err("invalid data");
        assert!(matches!(invalid_data_err, XorError::InvalidData));

        let invalid_timestamp = "abc.nothing";
        let invalid_timestamp_err = xor
            .decrypt(invalid_timestamp)
            .expect_err("invalid timestamp");
        assert!(matches!(invalid_timestamp_err, XorError::InvalidTimestamp));

        let xor = TimeBasedXor::new(5);
        let base64_decoding_error = format!("{}.{}", Local::now().timestamp(), "invalid");
        let base64_error = xor
            .decrypt(&base64_decoding_error)
            .expect_err("base64 decoding error");
        match base64_error {
            XorError::Base64(_) => (),
            _ => panic!("unexpected error"),
        }

        let decrypted = xor.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted.as_bytes(), data);
    }

    #[test]
    fn test_time_based_xor_sql() {
        let xor = TimeBasedXor::new(60); // 60 seconds validity
        let sql = "SELECT * FROM test.meters LIMIT 10";
        let encrypted = xor.encrypt(sql).unwrap();
        let decrypted = xor.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, sql);
    }

    #[test]
    fn test_time_based_xor_unicode() {
        let xor = TimeBasedXor::new(60);
        let sql = "SELECT * FROM 测试.电表 WHERE 名称='传感器'";
        let encrypted = xor.encrypt(sql).unwrap();
        let decrypted = xor.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, sql);
    }

    #[test]
    fn test_time_based_xor_long_sql() {
        let xor = TimeBasedXor::new(60);
        let sql = "SELECT * FROM test.meters WHERE ts > NOW() - 1d ".repeat(100);
        let encrypted = xor.encrypt(&sql).unwrap();
        let decrypted = xor.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, sql);
    }
}
