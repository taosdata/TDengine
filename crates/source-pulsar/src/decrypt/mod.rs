use snafu::Snafu;
use std::string::FromUtf8Error;
mod aesecb;
mod aesgcm;

#[derive(Debug, Snafu)]
pub enum DecryptionError {
    #[snafu(display("Base64 decoding failed: {source}"))]
    Base64 { source: base64::DecodeError },
    #[snafu(display("Invalid data length: {desc}"))]
    InvalidLength { desc: String },
    #[snafu(display("Invalid key length: must be 16, 24, or 32 bytes, but got {key_len}"))]
    InvalidKeyLength { key_len: usize },
    #[snafu(display("GCM Decrypt failed: {desc}"))]
    GcmError { desc: String },
    #[snafu(display("Invalid padding: {desc}"))]
    InvalidPadding { desc: String },
    #[snafu(display("Decrypted data is not valid UTF-8: {source}"))]
    Utf8 { source: FromUtf8Error },
}

#[derive(Debug, Clone, Copy)]
pub enum Decryptor {
    AesEcb,
    AesGcm,
}

impl Decryptor {
    pub fn decrypt(&self, data: &str, key: &str) -> Result<String, DecryptionError> {
        match self {
            Decryptor::AesEcb => aesecb::decrypt(data, key),
            Decryptor::AesGcm => aesgcm::decrypt(data, key),
        }
    }
}

impl From<&str> for Decryptor {
    fn from(value: &str) -> Self {
        match value {
            "aes_ecb" => Decryptor::AesEcb,
            "aes_gcm" => Decryptor::AesGcm,
            _ => {
                tracing::debug!("invalid decryptor type: {}, use default AesEcb", value);
                Decryptor::AesEcb
            }
        }
    }
}
