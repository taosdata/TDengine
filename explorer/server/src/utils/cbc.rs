use aes::Aes256;
use base64::prelude::{Engine, BASE64_STANDARD};
use cbc::{
    cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit},
    Decryptor, Encryptor,
};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;

type Aes256CbcEnc = Encryptor<Aes256>;
type Aes256CbcDec = Decryptor<Aes256>;
type HmacSha256 = Hmac<Sha256>;

const KEY_LEN: usize = 64; // 32B AES key + 32B HMAC key
const AES_KEY_LEN: usize = 32;
const HMAC_KEY_LEN: usize = 32;
const IV_LEN: usize = 16;
const TAG_LEN: usize = 32;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid CBC key format")]
    InvalidKeyFormat,
    #[error("Invalid key length: expect {KEY_LEN}, got {0}")]
    InvalidKeyLength(usize),
    #[error("Invalid IV length")]
    InvalidIvLength,
    #[error("Ciphertext too short")]
    CiphertextTooShort,
    #[error("MAC verification failed")]
    MacVerificationFailed,
    #[error("Decryption failed")]
    DecryptionFailed,
}

fn split_key(key_b64: &str) -> Result<([u8; AES_KEY_LEN], [u8; HMAC_KEY_LEN]), Error> {
    let key = BASE64_STANDARD
        .decode(key_b64)
        .map_err(|_| Error::InvalidKeyFormat)?;
    if key.len() != KEY_LEN {
        return Err(Error::InvalidKeyLength(key.len()));
    }
    let mut aes_key = [0u8; AES_KEY_LEN];
    let mut hmac_key = [0u8; HMAC_KEY_LEN];
    aes_key.copy_from_slice(&key[..AES_KEY_LEN]);
    hmac_key.copy_from_slice(&key[AES_KEY_LEN..]);
    Ok((aes_key, hmac_key))
}

fn compute_tag(
    hmac_key: &[u8; HMAC_KEY_LEN],
    iv: &[u8],
    ciphertext: &[u8],
) -> Result<[u8; TAG_LEN], Error> {
    let mut mac = HmacSha256::new_from_slice(hmac_key)
        .map_err(|_| Error::InvalidKeyLength(hmac_key.len()))?;
    mac.update(iv);
    mac.update(ciphertext);
    let bytes = mac.finalize().into_bytes();
    Ok(bytes.into())
}

/// Encrypts plaintext and returns base64(iv || ciphertext || mac).
pub fn encrypt_cbc_mac_b64(plaintext: &[u8], key_b64: &str) -> Result<String, Error> {
    let (aes_key, hmac_key) = split_key(key_b64)?;
    let mut iv = [0u8; IV_LEN];
    rand::thread_rng().fill_bytes(&mut iv);

    let cipher = Aes256CbcEnc::new_from_slices(&aes_key, &iv)
        .map_err(|_| Error::InvalidKeyLength(aes_key.len()))?;
    let ciphertext = cipher.encrypt_padded_vec_mut::<Pkcs7>(plaintext);

    let tag = compute_tag(&hmac_key, &iv, &ciphertext)?;

    let mut out = Vec::with_capacity(IV_LEN + ciphertext.len() + TAG_LEN);
    out.extend_from_slice(&iv);
    out.extend_from_slice(&ciphertext);
    out.extend_from_slice(&tag);

    Ok(BASE64_STANDARD.encode(out))
}

/// Decrypts base64(iv || ciphertext || mac), verifying MAC first.
pub fn decrypt_cbc_mac_b64(payload_b64: &str, key_b64: &str) -> Result<Vec<u8>, Error> {
    let data = BASE64_STANDARD
        .decode(payload_b64)
        .map_err(|_| Error::InvalidKeyFormat)?;

    if data.len() < IV_LEN + TAG_LEN {
        return Err(Error::CiphertextTooShort);
    }

    let (aes_key, hmac_key) = split_key(key_b64)?;
    let (iv, rest) = data.split_at(IV_LEN);
    let (ciphertext, tag) = rest.split_at(rest.len() - TAG_LEN);

    let mut mac = HmacSha256::new_from_slice(&hmac_key)
        .map_err(|_| Error::InvalidKeyLength(hmac_key.len()))?;
    mac.update(iv);
    mac.update(ciphertext);
    mac.verify_slice(tag)
        .map_err(|_| Error::MacVerificationFailed)?;

    let cipher = Aes256CbcDec::new_from_slices(&aes_key, iv).map_err(|_| Error::InvalidIvLength)?;
    cipher
        .decrypt_padded_vec_mut::<Pkcs7>(ciphertext)
        .map_err(|_| Error::DecryptionFailed)
}

/// Derive a 64-byte (base64) key and User-Agent.
/// Returns (derived_key_b64, salt_b64). You must persist salt_b64 alongside ciphertext.
pub fn derive_key_from_user_agent(
    secret: &[u8],
    user_agent: &str,
    salt: Option<&[u8; 16]>,
) -> Result<(String, Option<[u8; 16]>), Error> {
    let mut rand_salt = [0u8; 16];
    let with_salt = salt.unwrap_or_else(|| {
        rand::thread_rng().fill_bytes(&mut rand_salt);
        &rand_salt
    });

    let hk = Hkdf::<Sha256>::new(Some(with_salt), secret);
    let mut okm = [0u8; KEY_LEN];
    hk.expand(format!("ua:{user_agent}").as_bytes(), &mut okm)
        .map_err(|_| Error::InvalidKeyLength(with_salt.len()))?;

    if salt.is_none() {
        Ok((BASE64_STANDARD.encode(okm), Some(*with_salt)))
    } else {
        Ok((BASE64_STANDARD.encode(okm), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_key_from_user_agent() {
        const FAKE_SECRET_ENV: &str = "SERVER_HKDF_SECRET"; // base64-encoded, at least 32 random bytes
        let user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3";
        let (key_b64, salt_b64) =
            derive_key_from_user_agent(FAKE_SECRET_ENV.as_bytes(), user_agent, None).unwrap();
        assert_eq!(key_b64.len(), 88);
        assert!(salt_b64.is_some());

        let salt = uuid::Uuid::new_v4();
        let (key_b64, salt_b64) = derive_key_from_user_agent(
            FAKE_SECRET_ENV.as_bytes(),
            user_agent,
            Some(salt.as_bytes()),
        )
        .unwrap();
        assert!(salt_b64.is_none());
        let (key1_b64, _salt1_b64) = derive_key_from_user_agent(
            FAKE_SECRET_ENV.as_bytes(),
            user_agent,
            Some(salt.as_bytes()),
        )
        .unwrap();
        assert_eq!(key1_b64.len(), 88);
        assert_eq!(key1_b64, key_b64);

        let value = "hello taosx – AES-CBC+HMAC";
        let encrypted = encrypt_cbc_mac_b64(value.as_bytes(), &key_b64).unwrap();
        let decrypted = decrypt_cbc_mac_b64(&encrypted, &key_b64).unwrap();
        assert_eq!(decrypted, value.as_bytes());
    }

    #[test]
    fn test_encrypt_decrypt() {
        let encrypted = "ozo4Wxt7wsRhEvkY7sWSqdFiM4Va1Av9D4I/Xs89y0o6oICnGxHhnNVqwTjp92/zK3UNM44YYeAUzBD+jaF/55N8Rfl4NK13yyDTPvFcgkw=";
        let plaintext = "hello taosx – AES-CBC+HMAC";
        let key = "WrQKXN+tJkr/PJWbJDswU/SrikmLK04YKc4NW6jX5hT6W3oIEldHUj8AulIHZ01oO4nxG9FSQRD0pzOpyQZxKQ==";
        let decrypted = decrypt_cbc_mac_b64(encrypted, key).unwrap();
        assert_eq!(decrypted, plaintext.as_bytes());
        let encrypted = encrypt_cbc_mac_b64(plaintext.as_bytes(), key).unwrap();
        let decrypted = decrypt_cbc_mac_b64(&encrypted, key).unwrap();
        let string = String::from_utf8(decrypted).unwrap();
        assert_eq!(string, plaintext);
    }
}
