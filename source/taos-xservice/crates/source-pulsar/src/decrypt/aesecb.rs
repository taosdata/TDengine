use crate::decrypt::{Base64Snafu, DecryptionError, Utf8Snafu};
use aes::cipher::{BlockDecrypt, KeyInit};
use aes::{Aes128, Aes192, Aes256};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cipher::block_padding::Pkcs7;
use cipher::generic_array::GenericArray;
use cipher::{BlockCipher, BlockDecryptMut};
use snafu::ResultExt;

// 解密 AES/ECB/PKCS7, key 必须是 16/24/32 字节的 UTF-8 字符串, warning: 不建议在生产环境使用，当前 tuya 要求
pub fn decrypt(data: &str, key: &str) -> Result<String, DecryptionError> {
    let key_bytes = key.as_bytes();
    let cipher_bytes = STANDARD.decode(data).context(Base64Snafu)?;

    if cipher_bytes.len() % 16 != 0 {
        return Err(DecryptionError::InvalidLength {
            desc: "aesecb ciphertext len must be multiple of 16".to_string(),
        });
    }

    let rs = match key_bytes.len() {
        16 => decrypt_blocks::<Aes128>(key_bytes, &cipher_bytes)?,
        24 => decrypt_blocks::<Aes192>(key_bytes, &cipher_bytes)?,
        32 => decrypt_blocks::<Aes256>(key_bytes, &cipher_bytes)?,
        _ => {
            return Err(DecryptionError::InvalidKeyLength {
                key_len: key_bytes.len(),
            });
        }
    };

    String::from_utf8(rs).context(Utf8Snafu)
}

fn decrypt_blocks<A>(key: &[u8], ct: &[u8]) -> Result<Vec<u8>, DecryptionError>
where
    A: BlockDecrypt + KeyInit + BlockCipher,
{
    let cipher = ecb::Decryptor::<A>::new(GenericArray::from_slice(key));
    let mut buf = ct.to_vec();
    let pt = cipher.decrypt_padded_mut::<Pkcs7>(&mut buf).map_err(|e| {
        DecryptionError::InvalidPadding {
            desc: format!("aesecb padding error: {}", e),
        }
    })?;

    Ok(pt.to_vec())
}

#[cfg(test)]
mod tests {
    use super::decrypt;
    use aes::Aes128;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use cipher::{BlockEncryptMut, KeyInit, block_padding::Pkcs7};

    #[tokio::test]
    async fn test_decrypt() {
        let key = *b"1234567890abcdef";
        let plaintext = *b"hello cipher! this is taosx.";
        let mut buf = [0u8; 48];
        let pt_len = plaintext.len();
        buf[..pt_len].copy_from_slice(&plaintext);
        let ct = ecb::Encryptor::<Aes128>::new(&key.into())
            .encrypt_padded_mut::<Pkcs7>(&mut buf, pt_len)
            .unwrap();
        let b64ct = STANDARD.encode(ct);
        dbg!(&b64ct);
        let decripted_txt = decrypt(&b64ct, core::str::from_utf8(&key).unwrap()).unwrap();
        assert_eq!(decripted_txt, String::from_utf8_lossy(&plaintext));
    }
}
