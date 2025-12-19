use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser, Default, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct SecurityConfig {
    /// Encryption key for encrypting and decrypting sensitive data
    #[clap(long, env = "EXPLORER_SECURITY_ENCRYPTION_KEY")]
    #[serde(skip_serializing)]
    encryption_key: Option<String>,
}

impl SecurityConfig {
    /// Load encryption key from environment variable or generate a default one
    /// In production, this MUST be loaded from a secure key management system
    pub fn load_encryption_key(&self) -> [u8; 32] {
        if let Some(key_64) = self.encryption_key.as_ref() {
            use base64::Engine;
            match base64::engine::general_purpose::STANDARD.decode(key_64) {
                Ok(key_bytes) if key_bytes.len() == 32 => {
                    let mut key = [0u8; 32];
                    key.copy_from_slice(&key_bytes);
                    tracing::info!("Loaded encryption key from EXPLORER_SECURITY_ENCRYPTION_KEY");
                    return key;
                }
                Ok(_) => {
                    tracing::warn!(
                        "EXPLORER_SECURITY_ENCRYPTION_KEY has invalid length, using default key"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to decode EXPLORER_SECURITY_ENCRYPTION_KEY: {}, using default key",
                        e
                    );
                }
            }
        }

        // WARNING: Using a hardcoded key is NOT secure for production!
        // This is only for development/testing purposes
        tracing::warn!("Using default encryption key - NOT SECURE FOR PRODUCTION!");
        tracing::warn!(
            "Set EXPLORER_SECURITY_ENCRYPTION_KEY environment variable with a Base64-encoded 32-byte key"
        );

        // Default key derived from a known string (NOT SECURE)
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"taos-explorer-oauth-default-key-do-not-use-in-production");
        let result = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&result);
        key
    }
}
