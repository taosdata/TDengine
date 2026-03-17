use clap::Parser;
use serde::{Deserialize, Serialize};

const XOR_ALLOWED_DURATION_SECS_MIN: u64 = 10;
const XOR_ALLOWED_DURATION_SECS_MAX: u64 = 60 * 60 * 24;
const XOR_ALLOWED_DURATION_SECS_DEFAULT: u64 = 300;

#[derive(Debug, Default, Parser, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct SecurityConfig {
    /// Encryption key for encrypting and decrypting sensitive data
    #[clap(long, env = "EXPLORER_SECURITY_ENCRYPTION_KEY")]
    #[serde(skip_serializing)]
    encryption_key: Option<String>,

    /// Enable CAPTCHA for every Explorer login.
    ///
    /// Default: false.
    ///
    /// Config file (explorer.toml):
    ///
    ///   [security]
    ///   login_captcha = true
    #[clap(long, env = "EXPLORER_SECURITY_LOGIN_CAPTCHA")]
    #[serde(skip_serializing_if = "Option::is_none")]
    login_captcha: Option<bool>,

    /// Allowed duration in seconds for decrypting time-based XOR encrypted passwords during login.
    ///
    /// Range: 10..=60*60*24, default: 300.
    #[clap(
        long,
        env = "EXPLORER_SECURITY_XOR_ALLOWED_DURATION_SECS",
        value_parser = xor_allowed_duration_secs_parser,
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    xor_allowed_duration_secs: Option<u64>,
}

fn xor_allowed_duration_secs_parser(s: &str) -> Result<u64, String> {
    let v = s.parse::<u64>().map_err(|e| format!("{e}"))?;
    if (XOR_ALLOWED_DURATION_SECS_MIN..=XOR_ALLOWED_DURATION_SECS_MAX).contains(&v) {
        Ok(v)
    } else {
        Err(format!(
            "xor allowed duration must be in {XOR_ALLOWED_DURATION_SECS_MIN}..={XOR_ALLOWED_DURATION_SECS_MAX} seconds"
        ))
    }
}

impl SecurityConfig {
    pub fn login_captcha_enabled(&self) -> bool {
        self.login_captcha.unwrap_or(false)
    }

    pub fn xor_allowed_duration_secs(&self) -> u64 {
        self.xor_allowed_duration_secs
                .map_or(XOR_ALLOWED_DURATION_SECS_DEFAULT, |v| {
                    let clamped = v.clamp(XOR_ALLOWED_DURATION_SECS_MIN, XOR_ALLOWED_DURATION_SECS_MAX);
                    if clamped != v {
                        tracing::warn!(
                            "security.xor_allowed_duration_secs ({}) is outside the allowed range [{}..={}] and has been clamped to {}",
                            v, XOR_ALLOWED_DURATION_SECS_MIN, XOR_ALLOWED_DURATION_SECS_MAX, clamped
                        );
                    }
                    clamped
                })
    }

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
