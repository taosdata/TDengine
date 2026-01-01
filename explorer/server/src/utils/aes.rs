use aes_gcm::{Aes256Gcm, KeyInit, Nonce, aead::Aead};
use base64::Engine;

/// AES-256-GCM encrypted data, concatenated with nonce at the front.
pub fn aes_encrypt_base64(data: &[u8], key: &[u8; 32]) -> Result<String, aes_gcm::Error> {
    let encrypted = aes_encrypt(data, key)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(&encrypted))
}

pub fn aes_decrypt_base64(data_b64: &str, key: &[u8]) -> Result<Vec<u8>, aes_gcm::Error> {
    let data = base64::engine::general_purpose::STANDARD
        .decode(data_b64)
        .map_err(|_| aes_gcm::Error)?;
    aes_decrypt(&data, key)
}
/// AES-256-GCM encrypted data, concatenated with nonce at the front.
pub fn aes_encrypt(data: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, aes_gcm::Error> {
    let cipher = Aes256Gcm::new_from_slice(key).unwrap();
    let mut nonce = [0u8; 12];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut nonce);
    let nonce_slice = Nonce::from_slice(&nonce); // 96-bits; unique per message
    let mut result = nonce.to_vec();
    let ciphertext = cipher.encrypt(nonce_slice, data)?;
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// AES-256-GCM decrypt data, assuming nonce is concatenated at the front.
pub fn aes_decrypt(data: &[u8], key: &[u8]) -> Result<Vec<u8>, aes_gcm::Error> {
    if data.len() < 12 {
        return Err(aes_gcm::Error);
    }
    let cipher = Aes256Gcm::new_from_slice(key).unwrap();
    let iv = Nonce::from_slice(&data[..12]); // 96-bits; unique per message
    let ciphertext = &data[12..];
    cipher.decrypt(iv, ciphertext)
}
#[cfg(test)]
mod tests {
    use super::*;

    pub fn base64_encode(data: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(data)
    }
    pub fn aes_decrypt_with_nonce(
        data: &[u8],
        key: &[u8; 32],
        iv: &[u8; 12],
    ) -> Result<Vec<u8>, aes_gcm::Error> {
        let cipher = Aes256Gcm::new_from_slice(key).unwrap();
        let nonce = Nonce::from_slice(iv); // 96-bits; unique per message

        cipher.decrypt(nonce, data)
    }
    pub fn aes_encrypt_with_nonce(
        data: &[u8],
        key: &[u8; 32],
        nonce: &[u8; 12],
    ) -> Result<Vec<u8>, aes_gcm::Error> {
        let cipher = Aes256Gcm::new_from_slice(key).unwrap();
        let nonce = Nonce::from_slice(nonce); // 96-bits; unique per message

        cipher.encrypt(nonce, data)
    }

    pub fn generate_aes_key() -> [u8; 32] {
        use rand::RngCore;
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }

    #[test]
    fn test_aes_encrypt_decrypt() {
        let key = generate_aes_key();
        let raw = b"Hello, AES-256-GCM!";
        let encrypted = aes_encrypt(raw, &key).expect("Encryption failed");
        let decrypted = aes_decrypt(&encrypted, &key).expect("Decryption failed");

        let data = &encrypted[12..];
        let nonce = &encrypted[..12].try_into().unwrap();
        let decrypted_with_nonce = aes_decrypt_with_nonce(&encrypted[12..], &key, nonce)
            .expect("Decryption with nonce failed");

        assert_eq!(raw.to_vec(), decrypted_with_nonce);
        let encrypted_with_nonce =
            aes_encrypt_with_nonce(&decrypted, &key, nonce).expect("Decryption with nonce failed");
        assert_eq!(encrypted_with_nonce, data);
    }

    #[test]
    fn test_aes_encrypt_decrypt_base64() {
        let key = generate_aes_key();
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
        dbg!(&key_b64);
        let data = b"Hello, AES-256-GCM with Base64!";
        let encrypted_b64 = aes_encrypt_base64(data, &key).expect("Encryption failed");
        dbg!(&encrypted_b64);
        let decrypted = aes_decrypt_base64(&encrypted_b64, &key).expect("Decryption failed");
        let decrypted_str = String::from_utf8(decrypted.clone()).expect("UTF-8 conversion failed");
        dbg!(&decrypted_str);

        assert_eq!(data.to_vec(), decrypted);
    }

    #[test]
    fn test_aes_decrypt_with_imported_key() {
        let b64key = "Ioc0q7sVElGXOaBFDPEHrjgLZeIFbm55Ol5HOTiNqg8=";
        let key = base64::engine::general_purpose::STANDARD
            .decode(b64key)
            .expect("Base64 decode failed");

        let encrypted_b64 =
            "/QCRjlIA7VMUa/trgd0L0Sz5DNWflZCvWOUh3BC1VKdHUIfHc87OtlCPi3xUJ9Z1vmo9HVojmMAHV9Q=";
        let decrypted = aes_decrypt_base64(encrypted_b64, &key).expect("Decryption failed");
        let decrypted_str = String::from_utf8(decrypted.clone()).expect("UTF-8 conversion failed");
        dbg!(&decrypted_str);
        assert_eq!(decrypted_str, "Hello, AES-256-GCM with Base64!");
    }

    #[test]
    fn test_token_uuid_encryption() {
        let uuid = uuid::Uuid::new_v4();
        dbg!(&uuid);
        let key_0 = generate_aes_key();
        let key_b64 = base64_encode(&key_0);
        let key = base64::engine::general_purpose::STANDARD
            .decode(&key_b64)
            .expect("Base64 decode failed");
        assert_eq!(key_0, key[..]);
        let key_array: [u8; 32] = key.try_into().expect("Key length incorrect");

        // Use specific uuid to make test deterministic in both server and client sides.
        let token_uuid = "550e8400-e29b-41d4-a716-446655440000";
        let encrypted_b64 =
            aes_encrypt_base64(token_uuid.as_bytes(), &key_array).expect("Encryption failed");
        dbg!(&encrypted_b64);

        let decrypted_bytes =
            aes_decrypt_base64(&encrypted_b64, &key_array).expect("Decryption failed");
        let decrypted_str = String::from_utf8(decrypted_bytes).expect("UTF-8 conversion failed");
        dbg!(&decrypted_str);

        assert_eq!(token_uuid, decrypted_str);
    }
}
