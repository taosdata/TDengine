use crate::decrypt::{Base64Snafu, DecryptionError, Utf8Snafu};
use aes_gcm::aead::{Aead, KeyInit, Nonce};
use aes_gcm::{Aes128Gcm, Aes256Gcm};
use base64::{Engine as _, engine::general_purpose::STANDARD as base64_engine};
use snafu::ResultExt;

const GCM_NONCE_LENGTH: usize = 12;
const GCM_TAG_LENGTH: usize = 16; // 128 bits

pub fn decrypt(data: &str, key: &str) -> Result<String, DecryptionError> {
    let key_bytes = key.as_bytes();
    let message = base64_engine.decode(data).context(Base64Snafu)?;

    if message.len() < GCM_NONCE_LENGTH + GCM_TAG_LENGTH {
        return Err(DecryptionError::InvalidLength {
            desc: format!(
                "aesgcm ciphertext len must be at least {} bytes, got {}",
                GCM_NONCE_LENGTH + GCM_TAG_LENGTH,
                message.len()
            ),
        });
    }

    let (nonce_bytes, ciphertext_and_tag) = message.split_at(GCM_NONCE_LENGTH);

    let decrypted_bytes = match key_bytes.len() {
        16 => {
            // 128-bit key
            let cipher = Aes128Gcm::new(key_bytes.into());
            let nonce = Nonce::<Aes128Gcm>::from_slice(nonce_bytes);
            cipher.decrypt(nonce, ciphertext_and_tag).map_err(|e| {
                tracing::error!("gcm decrypt error: {:?}", e);
                DecryptionError::GcmError {
                    desc: e.to_string(),
                }
            })?
        }
        32 => {
            // 256-bit key
            let cipher = Aes256Gcm::new(key_bytes.into());
            let nonce = Nonce::<Aes256Gcm>::from_slice(nonce_bytes);
            cipher
                .decrypt(nonce, ciphertext_and_tag)
                .map_err(|e| DecryptionError::GcmError {
                    desc: e.to_string(),
                })?
        }
        _ => {
            return Err(DecryptionError::InvalidKeyLength {
                key_len: key_bytes.len(),
            });
        }
    };

    String::from_utf8(decrypted_bytes).context(Utf8Snafu)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tuya() -> anyhow::Result<()> {
        let key = "62cc4527a90e7829";
        let data = "qzGAKa00XbTK4HPjElvpkuYN/fXYyj2BhdTbE6+l6cONQLctSXTwwsSlkrNo+30mlJQeaNZ73vh/NuSeyf4HQgmHrdb3bonBWkxjdbD+bGrDUAr77zAj2RUTyR8inKwqJWaSfnva4UEUW2xRUfWCTRYjyyLJsHO5m8Plg+lW8q5Rg83yEPQniHi1UjEOL34c7fz88PBaNm7MD+5deyG4czT4ZsO+VpwZ2yB6CXDwgGtZhspEHF6EaiNvzo+Rxr0kL+UW+f/dmCkGjxmcHlqpDqdUrrI0ZPc=";

        println!("data len: {}", data.len());
        println!("key len: {}", key.len());
        let plaintext = decrypt(data, key)?;
        assert_eq!(
            plaintext,
            r#"{"dataId":"000642BB7075D85F7DA5A0BF6807233B","devId":"ebc778f3c5d9908ff6plgl","productKey":"9exm2qiar0dvqoxv","status":[{"3":"40","code":"humidity_current","t":1762222673354,"value":40}]}"#
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_data2() -> anyhow::Result<()> {
        let key128 = "7fddb57453c241d03efbed3ac44e371c";
        let data128 = "ee283a3fc75575e33efd48872ccda4a5415cb91e135c2a0f78c9b2fd";
        let rs = decrypt(data128, key128);
        println!("{:?}", rs);
        assert!(rs.is_err());
        Ok(())
    }
}
