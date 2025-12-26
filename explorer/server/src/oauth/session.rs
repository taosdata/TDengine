use super::client::UserInfo;
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Months, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use taos::*;
use tracing::instrument;
use uuid::Uuid;

use crate::Args;
use crate::oauth::middleware::{AuthType, TsdbCredential};
use crate::utils::aes::{aes_decrypt_base64, aes_encrypt_base64};
use crate::utils::cbc::derive_key_from_user_agent;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct OAuthUser {
    pub user_id: i64,
    pub username: String,
    pub tsdb_username: Option<String>,
    pub tsdb_password: Option<String>,
    pub email: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthSession {
    #[serde(flatten)]
    pub user: OAuthUser,
    #[serde(flatten)]
    pub details: OAuthSessionDetails,
}

impl OAuthSession {
    /// Get the TDengine username associated with this OAuth session
    pub fn get_tsdb_username(&self) -> Option<&str> {
        self.user.tsdb_username.as_deref()
    }

    /// Get the TDengine password associated with this OAuth session
    pub fn get_tsdb_password(&self) -> Option<&str> {
        self.user.tsdb_password.as_deref()
    }

    pub fn session_id(&self) -> &str {
        &self.details.session_id
    }

    /// Get the OAuth username
    pub fn username(&self) -> &str {
        &self.user.username
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthSyncSummary {
    pub imported: u64,
    pub updated: u64,
    pub skipped: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TsdbSyncPassword {
    Random,
    Constant(String),
}
impl Default for TsdbSyncPassword {
    fn default() -> Self {
        Self::Random
    }
}

impl TsdbSyncPassword {
    pub fn generate_password(&self) -> Cow<'_, str> {
        match self {
            TsdbSyncPassword::Random => {
                fn random_password() -> String {
                    let mut rng = rand::thread_rng();
                    use rand::Rng;

                    let dist =
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!?$&*#-+";
                    let password: String = (0..16)
                        .map(|_| {
                            rng.sample(rand::distributions::Uniform::new_inclusive(
                                0,
                                dist.len() - 1,
                            ))
                        })
                        .map(|idx| dist.chars().nth(idx).unwrap())
                        .collect();
                    password
                }

                Cow::Owned(random_password())
            }
            TsdbSyncPassword::Constant(password) => Cow::Borrowed(password),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_password() {
        let random: TsdbSyncPassword = serde_json::from_str(r#""random""#).unwrap();
        assert_eq!(random, TsdbSyncPassword::Random);

        let password: TsdbSyncPassword = serde_json::from_str(r#"{"constant":"abc"}"#).unwrap();
        assert_eq!(password, TsdbSyncPassword::Constant("abc".to_string()));
    }

    #[test]
    fn test_tsdb_sync_password_default() {
        let password = TsdbSyncPassword::default();
        assert_eq!(password, TsdbSyncPassword::Random);
    }

    #[test]
    fn test_tsdb_sync_password_random_generation() {
        let password = TsdbSyncPassword::Random;
        let generated1 = password.generate_password();
        let generated2 = password.generate_password();

        // Random passwords should be 16 characters
        assert_eq!(generated1.len(), 16);
        assert_eq!(generated2.len(), 16);

        // Random passwords should be different each time
        assert_ne!(generated1.as_ref(), generated2.as_ref());

        // Should only contain valid characters
        let valid_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!?$&*#-+";
        for c in generated1.chars() {
            assert!(valid_chars.contains(c));
        }
    }

    #[test]
    fn test_tsdb_sync_password_constant() {
        let password = TsdbSyncPassword::Constant("my_password_123".to_string());
        let generated = password.generate_password();

        assert_eq!(generated, "my_password_123");

        // Should return the same value every time
        assert_eq!(password.generate_password(), "my_password_123");
    }

    #[test]
    fn test_tsdb_sync_password_constant_empty() {
        let password = TsdbSyncPassword::Constant("".to_string());
        let generated = password.generate_password();

        assert_eq!(generated, "");
    }

    #[test]
    fn test_tsdb_sync_password_serialization() {
        let random = TsdbSyncPassword::Random;
        let json = serde_json::to_string(&random).unwrap();
        assert_eq!(json, r#""random""#);

        let constant = TsdbSyncPassword::Constant("test123".to_string());
        let json = serde_json::to_string(&constant).unwrap();
        assert_eq!(json, r#"{"constant":"test123"}"#);
    }

    #[test]
    fn test_tsdb_sync_password_deserialization() {
        let random: TsdbSyncPassword = serde_json::from_str(r#""random""#).unwrap();
        assert_eq!(random, TsdbSyncPassword::Random);

        let constant: TsdbSyncPassword = serde_json::from_str(r#"{"constant":"secret"}"#).unwrap();
        assert_eq!(constant, TsdbSyncPassword::Constant("secret".to_string()));
    }

    #[test]
    fn test_tsdb_sync_username_default() {
        let username = TsdbSyncUsername::Default;

        // Test with different providers
        let generated = username.generate_username("oidc", "user123");
        assert!(generated.starts_with("oi_user123_") || generated.starts_with("oi_user123"));

        let generated = username.generate_username("custom", "user456");
        assert!(generated.starts_with("oc_user456_") || generated.starts_with("oc_user456"));
    }

    #[test]
    fn test_tsdb_sync_username_default_length_limit() {
        let username = TsdbSyncUsername::Default;

        // Test with very long user_id (should be truncated to fit 23 char limit)
        let long_user_id = "very_long_user_id_that_exceeds_limit";
        let generated = username.generate_username("oidc", long_user_id);

        // TSDB username limit is 23 characters
        assert!(generated.len() <= 23);
    }

    #[test]
    fn test_tsdb_sync_username_constant() {
        let username = TsdbSyncUsername::Constant("root".to_string());
        let generated = username.generate_username("oidc", "user123");

        assert_eq!(generated, "root");

        // Should always return the same value regardless of provider/user_id
        assert_eq!(username.generate_username("custom", "another_user"), "root");
    }

    #[test]
    fn test_tsdb_sync_username_pattern() {
        let username = TsdbSyncUsername::Pattern("oauth_{provider}_{user_id}".to_string());
        let generated = username.generate_username("oidc", "user123");

        assert_eq!(generated, "oauth_oidc_user123");

        let generated = username.generate_username("custom", "admin");
        assert_eq!(generated, "oauth_custom_admin");
    }

    #[test]
    fn test_tsdb_sync_username_pattern_with_uuid() {
        let username = TsdbSyncUsername::Pattern("user_{uuid}".to_string());
        let generated1 = username.generate_username("oidc", "user123");
        let generated2 = username.generate_username("oidc", "user123");

        // Both should start with "user_" but have different UUIDs
        assert!(generated1.starts_with("user_"));
        assert!(generated2.starts_with("user_"));
        assert_ne!(generated1.as_ref(), generated2.as_ref());
    }

    #[test]
    fn test_tsdb_sync_username_pattern_with_suffix() {
        let username = TsdbSyncUsername::Pattern("oauth_{provider}_{suffix}".to_string());
        let generated = username.generate_username("oidc", "user123");

        assert!(generated.starts_with("oauth_oidc_"));
        assert_eq!(generated.len(), "oauth_oidc_".len() + 4); // suffix is 4 chars
    }

    #[test]
    fn test_tsdb_sync_username_usermap() {
        let mut usermap = HashMap::new();
        usermap.insert("user123".to_string(), "mapped_user1".to_string());
        usermap.insert("user456".to_string(), "mapped_user2".to_string());

        let username = TsdbSyncUsername::Usermap(usermap);

        // Mapped users should return their mapped names
        assert_eq!(
            username.generate_username("oidc", "user123"),
            "mapped_user1"
        );
        assert_eq!(
            username.generate_username("custom", "user456"),
            "mapped_user2"
        );

        // Unmapped users should get default format
        let unmapped = username.generate_username("oidc", "unknown_user");
        assert_eq!(unmapped, "oauth_oidc_unknown_user");
    }

    #[test]
    fn test_tsdb_sync_username_usermap_empty() {
        let usermap = HashMap::new();
        let username = TsdbSyncUsername::Usermap(usermap);

        // All users should get default format
        assert_eq!(
            username.generate_username("oidc", "user123"),
            "oauth_oidc_user123"
        );
    }

    #[test]
    fn test_tsdb_sync_username_default_trait() {
        let username = TsdbSyncUsername::default();
        assert_eq!(username, TsdbSyncUsername::Default);
    }

    #[test]
    fn test_tsdb_sync_username_serialization() {
        let default = TsdbSyncUsername::Default;
        let json = serde_json::to_string(&default).unwrap();
        assert_eq!(json, r#""default""#);

        let constant = TsdbSyncUsername::Constant("admin".to_string());
        let json = serde_json::to_string(&constant).unwrap();
        assert_eq!(json, r#"{"constant":"admin"}"#);

        let pattern = TsdbSyncUsername::Pattern("oauth_{provider}".to_string());
        let json = serde_json::to_string(&pattern).unwrap();
        assert_eq!(json, r#"{"pattern":"oauth_{provider}"}"#);
    }

    #[test]
    fn test_tsdb_sync_username_deserialization() {
        let default: TsdbSyncUsername = serde_json::from_str(r#""default""#).unwrap();
        assert_eq!(default, TsdbSyncUsername::Default);

        let constant: TsdbSyncUsername = serde_json::from_str(r#"{"constant":"root"}"#).unwrap();
        assert_eq!(constant, TsdbSyncUsername::Constant("root".to_string()));

        let pattern: TsdbSyncUsername =
            serde_json::from_str(r#"{"pattern":"oauth_{user_id}"}"#).unwrap();
        assert_eq!(
            pattern,
            TsdbSyncUsername::Pattern("oauth_{user_id}".to_string())
        );
    }

    #[test]
    fn test_tsdb_sync_options_default() {
        let options = TsdbSyncOptions::default();
        assert_eq!(options.username, TsdbSyncUsername::Default);
        assert_eq!(options.password, TsdbSyncPassword::Random);
    }

    #[test]
    fn test_tsdb_sync_options_get_user_pass() {
        let options = TsdbSyncOptions {
            username: TsdbSyncUsername::Constant("test_user".to_string()),
            password: TsdbSyncPassword::Constant("test_pass".to_string()),
        };

        let (username, password) = options.get_user_pass("oidc", "user123");
        assert_eq!(username, "test_user");
        assert_eq!(password, "test_pass");
    }

    #[test]
    fn test_tsdb_sync_options_get_user_pass_random() {
        let options = TsdbSyncOptions::default();

        let (_username1, password1) = options.get_user_pass("oidc", "user123");
        let (_username2, password2) = options.get_user_pass("oidc", "user123");

        // Passwords should be different (random)
        assert_ne!(password1.as_ref(), password2.as_ref());

        // Passwords should be 16 characters
        assert_eq!(password1.len(), 16);
        assert_eq!(password2.len(), 16);
    }

    #[test]
    fn test_tsdb_sync_options_serialization() {
        let options = TsdbSyncOptions {
            username: TsdbSyncUsername::Constant("admin".to_string()),
            password: TsdbSyncPassword::Constant("secret".to_string()),
        };

        let json = serde_json::to_string(&options).unwrap();
        assert!(json.contains(r#""username""#));
        assert!(json.contains(r#""password""#));
        assert!(json.contains(r#""admin""#));
        assert!(json.contains(r#""secret""#));
    }

    #[test]
    fn test_tsdb_sync_options_deserialization() {
        let json = r#"{
            "username": {"constant": "test_user"},
            "password": {"constant": "test_password"}
        }"#;

        let options: TsdbSyncOptions = serde_json::from_str(json).unwrap();
        assert_eq!(
            options.username,
            TsdbSyncUsername::Constant("test_user".to_string())
        );
        assert_eq!(
            options.password,
            TsdbSyncPassword::Constant("test_password".to_string())
        );
    }

    #[test]
    fn test_oauth_sync_summary() {
        let summary = OAuthSyncSummary {
            imported: 10,
            updated: 5,
            skipped: 2,
        };

        assert_eq!(summary.imported, 10);
        assert_eq!(summary.updated, 5);
        assert_eq!(summary.skipped, 2);
    }

    #[test]
    fn test_tsdb_sync_username_provider_abbreviation() {
        let username = TsdbSyncUsername::Default;

        // Test provider abbreviations
        let oidc = username.generate_username("oidc", "u");
        assert!(oidc.starts_with("oi_u"));

        let custom = username.generate_username("custom", "u");
        assert!(custom.starts_with("oc_u"));

        // Other providers should use their full name
        let plain = username.generate_username("plain", "u");
        assert!(plain.starts_with("oplain_u"));
    }

    #[test]
    fn test_tsdb_sync_username_pattern_all_placeholders() {
        let username = TsdbSyncUsername::Pattern(
            "p:{provider}_u:{user_id}_uuid:{uuid}_s:{suffix}".to_string(),
        );

        let generated = username.generate_username("oidc", "user123");

        assert!(generated.starts_with("p:oidc_u:user123_uuid:"));
        assert!(generated.contains("_s:"));
    }

    #[test]
    fn test_password_clone_and_debug() {
        let password = TsdbSyncPassword::Constant("test".to_string());
        let cloned = password.clone();
        assert_eq!(password, cloned);

        let debug_str = format!("{:?}", password);
        assert!(debug_str.contains("Constant"));
    }

    #[test]
    fn test_username_clone_and_debug() {
        let username = TsdbSyncUsername::Constant("admin".to_string());
        let cloned = username.clone();
        assert_eq!(username, cloned);

        let debug_str = format!("{:?}", username);
        assert!(debug_str.contains("Constant"));
    }

    #[test]
    fn test_tsdb_sync_options_clone() {
        let options = TsdbSyncOptions {
            username: TsdbSyncUsername::Constant("user".to_string()),
            password: TsdbSyncPassword::Constant("pass".to_string()),
        };

        let cloned = options.clone();
        assert_eq!(options, cloned);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TsdbSyncUsername {
    // Using default pattern oauth_{provider}_{user_id}_{username}
    Default,
    // Use constant username, like 'root'
    Constant(String),
    // User custom name pattern
    Pattern(String),
    // User custom usermap
    Usermap(HashMap<String, String>),
}
impl TsdbSyncUsername {
    pub fn generate_username<'a>(&'a self, provider: &str, user_id: &str) -> Cow<'a, str> {
        match self {
            TsdbSyncUsername::Default => {
                let mut buf = [0u8; 4];
                use rand::Rng;
                let mut rng = rand::thread_rng();
                for i in &mut buf {
                    *i = rng.sample(rand::distributions::Alphanumeric);
                }
                let suffix = unsafe { std::str::from_utf8_unchecked(&buf) };
                // TSDB username has a limit of 23 characters.
                let provider = match provider {
                    "oidc" => "i",
                    "custom" => "c",
                    s => s,
                };

                if provider.len() + user_id.len() <= 23 - 4 - 3 {
                    return Cow::Owned(format!("o{}_{}_{}", provider, user_id, suffix));
                }
                if provider.len() + user_id.len() <= 23 - 2 {
                    return Cow::Owned(format!("o{}_{}", provider, user_id));
                }
                let user_id = &user_id[0..23 - 4 - 3 - provider.len()];
                Cow::Owned(format!("o{}_{}_{}", provider, user_id, suffix))
            }
            TsdbSyncUsername::Constant(username) => username.as_str().into(),
            TsdbSyncUsername::Pattern(pattern) => {
                let mut buf = [0u8; 4];
                use rand::Rng;
                let mut rng = rand::thread_rng();
                for i in &mut buf {
                    *i = rng.sample(rand::distributions::Alphanumeric);
                }

                let suffix = unsafe { std::str::from_utf8_unchecked(&buf) };
                pattern
                    .replace("{provider}", provider)
                    .replace("{user_id}", user_id)
                    .replace("{uuid}", &Uuid::new_v4().as_simple().to_string())
                    .replace("{suffix}", suffix)
                    .into()
            }
            TsdbSyncUsername::Usermap(usermap) => usermap
                .get(user_id)
                .map(|s| Cow::Owned(s.to_string())) // can't use s.to_str, wired.
                .unwrap_or_else(|| Cow::Owned(format!("oauth_{}_{}", provider, user_id))),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TsdbSyncOptions {
    pub username: TsdbSyncUsername,
    pub password: TsdbSyncPassword,
}

impl Default for TsdbSyncOptions {
    fn default() -> Self {
        Self {
            username: TsdbSyncUsername::Default,
            password: TsdbSyncPassword::Random,
        }
    }
}

impl TsdbSyncOptions {
    pub fn get_user_pass(&self, provider: &str, user_id: &str) -> (Cow<'_, str>, Cow<'_, str>) {
        let username = self.username.generate_username(provider, user_id);
        let password = self.password.generate_password();
        (username, password)
    }
}

impl Default for TsdbSyncUsername {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct OAuthSessionDetails {
    /// Unique session ID, generated on session creation with UUIDv4
    pub session_id: String,
    /// Associated user ID
    pub user_id: i64,
    /// Access token from OAuth provider
    pub access_token: Option<String>,
    /// Refresh token from OAuth provider
    pub refresh_token: Option<String>,
    /// ID token from OAuth provider (if applicable)
    pub id_token: Option<String>,
    /// Expiration timestamp of the access token
    pub access_token_expires_at: Option<DateTime<Utc>>,
    /// Expiration timestamp of the session
    pub expires_at: DateTime<Utc>,
    /// Timestamp when the session was created
    pub login_at: DateTime<Utc>,
    /// Timestamp of the last activity in this session
    pub last_active: DateTime<Utc>,
}

#[derive(Clone)]
pub struct SessionManager {
    /// Explorer configuration arguments
    args: Arc<Args>,

    /// Explorer database connection pool
    pool: SqlitePool,
    /// Encryption key for TDengine passwords (32 bytes)
    /// This should be loaded from environment variable or secure key management
    encryption_key: [u8; 32],

    /// Pool of TDengine connections.
    ///
    /// - key: `session_id` string.
    /// - value: [TaosPool] object.
    connections: scc::HashMap<String, TaosPool>,
}
impl std::fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionManager")
            .field("pool", &"SqlitePool")
            .field("encryption_key", &"[REDACTED]")
            .finish()
    }
}

impl SessionManager {
    pub fn new(args: Arc<Args>, pool: SqlitePool, encryption_key: [u8; 32]) -> Self {
        Self {
            args,
            pool,
            encryption_key,
            connections: scc::HashMap::new(),
        }
    }

    /// Encrypt TDengine password using AES-256-GCM
    fn encrypt_password(&self, password: &str) -> Result<String> {
        aes_encrypt_base64(password.as_bytes(), &self.encryption_key)
            .map_err(|e| anyhow::anyhow!("Failed to encrypt password: {:?}", e))
    }

    /// Decrypt TDengine password using AES-256-GCM
    pub fn decrypt_password(&self, encrypted_password: &str) -> Result<String> {
        let decrypted_bytes = aes_decrypt_base64(encrypted_password, &self.encryption_key)
            .map_err(|e| anyhow::anyhow!("Failed to decrypt password: {:?}", e))?;
        String::from_utf8(decrypted_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to convert decrypted password to UTF-8: {}", e))
    }

    /// Encrypt arbitrary token (access/refresh/id token) using the same AES key
    fn encrypt_token(&self, token: &str) -> Result<String> {
        aes_encrypt_base64(token.as_bytes(), &self.encryption_key)
            .map_err(|e| anyhow::anyhow!("Failed to encrypt token: {:?}", e))
    }

    /// Decrypt token previously encrypted by `encrypt_token`
    fn decrypt_token(&self, encrypted_token: &str) -> Result<String> {
        let decrypted_bytes = aes_decrypt_base64(encrypted_token, &self.encryption_key)
            .map_err(|e| anyhow::anyhow!("Failed to decrypt token: {:?}", e))?;
        String::from_utf8(decrypted_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to convert decrypted token to UTF-8: {}", e))
    }

    /// Get decrypted TDengine password from session
    pub fn get_decrypted_tsdb_password(&self, session: &OAuthSession) -> Result<Option<String>> {
        match session.user.tsdb_password.as_ref() {
            Some(encrypted_password) => {
                let decrypted = self
                    .decrypt_password(encrypted_password)
                    .context("Failed to decrypt TDengine password")?;
                Ok(Some(decrypted))
            }
            None => Ok(None),
        }
    }

    pub async fn derive_client_encrypt_key_with_user_agent(
        &self,
        session_id: &str,
        user_agent: &str,
    ) -> Result<String> {
        let uuid = uuid::Uuid::from_str(session_id)
            .map_err(|e| anyhow::anyhow!("Failed to parse session ID: {}", e))?;
        derive_key_from_user_agent(&self.encryption_key, user_agent, Some(uuid.as_bytes()))
            .context("Failed to derive client encryption key")
            .map(|(key, _)| key)
    }

    /// Create a self-provided OAuth session
    pub async fn create_self_provided_session(
        &self,
        tsdb: &TsdbCredential,
        expires_in: Option<i64>,
    ) -> Result<OAuthSession> {
        let mut tx = self.pool.begin().await?;
        const SELF_PROVIDED: &str = "__self__";
        let username = &tsdb.username;
        let expires_in = expires_in.unwrap_or(i64::MAX);
        let expires_in = chrono::Duration::try_seconds(expires_in).unwrap_or(chrono::Duration::MAX);
        let now = Utc::now();
        let expires_at = now
            .checked_add_signed(expires_in)
            .or_else(|| now.checked_add_months(Months::new(100 * 12)))
            .context("Failed to calculate expiration time")?;

        let user = sqlx::query_as::<_, OAuthUser>(
            r#"
            SELECT user_id, username, tsdb_username, tsdb_password, email, created_at, updated_at
            FROM oauth_users
            WHERE provider = ? AND username = ?
            "#,
        )
        .bind(SELF_PROVIDED)
        .bind(username)
        .fetch_optional(tx.as_mut())
        .await
        .context("Failed to fetch OAuth user")?;
        // Encrypt password
        let encrypted_password = self
            .encrypt_password(&tsdb.password)
            .context("Failed to encrypt password in self-provided session")?;

        let user = if let Some(user) = user {
            // Check if password needs update by comparing decrypted version
            let password_match = match user.tsdb_password.as_ref() {
                Some(stored_encrypted) => {
                    match self.decrypt_password(stored_encrypted) {
                        Ok(decrypted) => decrypted == tsdb.password,
                        Err(_) => false, // If decryption fails, update password
                    }
                }
                None => false,
            };

            if !password_match {
                // Update user's TSDB credentials
                let _ = sqlx::query(
                    r#"
                    UPDATE oauth_users
                    SET tsdb_username = ?, tsdb_password = ?, updated_at = ?
                    WHERE user_id = ?
                    "#,
                )
                .bind(username)
                .bind(&encrypted_password)
                .bind(now)
                .bind(user.user_id)
                .execute(tx.as_mut())
                .await
                .context("Failed to update OAuth user")?;
            }
            user
        } else {
            // Create new user
            let res = sqlx::query(
                r#"
                INSERT INTO oauth_users (provider, username, tsdb_username, tsdb_password, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(SELF_PROVIDED)
            .bind(username)
            .bind(username)
            .bind(&encrypted_password)
            .bind(now)
            .bind(now)
            .execute(tx.as_mut())
            .await
            .context("Failed to create OAuth user")?;

            OAuthUser {
                user_id: res.last_insert_rowid(),
                username: username.clone(),
                email: None,
                tsdb_username: None,
                tsdb_password: None,
                created_at: now,
                updated_at: now,
            }
        };

        let session_id = Uuid::new_v4().to_string();

        let session = OAuthSession {
            user: user.clone(),
            details: OAuthSessionDetails {
                session_id: session_id.clone(),
                user_id: user.user_id,
                access_token: None,
                refresh_token: None,
                id_token: None,
                access_token_expires_at: None,
                expires_at,
                login_at: now,
                last_active: now,
            },
        };

        sqlx::query(
            r#"
            INSERT INTO oauth_sessions
            (session_id, user_id, expires_at, login_at, last_active)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(session_id)
        .bind(user.user_id)
        .bind(expires_at)
        .bind(now)
        .bind(now)
        .execute(tx.as_mut())
        .await
        .context("Failed to create OAuth session")?;

        tx.commit()
            .await
            .context("Failed to commit oauth session creation changes")?;

        tracing::info!("Created OAuth session for user: {}", username);

        Ok(session)
    }

    /// Create a new OAuth session
    pub async fn create_session(
        &self,
        provider: String,
        username: String,
        email: Option<String>,
        access_token: Option<String>,
        refresh_token: Option<String>,
        id_token: Option<String>,
        expires_in: i64,                      // session expiration in seconds
        access_token_expires_in: Option<i64>, // access token expiration in seconds
    ) -> Result<OAuthSession> {
        let mut tx = self.pool.begin().await?;
        let user = sqlx::query_as::<_, OAuthUser>(
            r#"
            SELECT user_id, username, tsdb_username, tsdb_password, email, created_at, updated_at
            FROM oauth_users
            WHERE provider = ? AND username = ?
            "#,
        )
        .bind(&provider)
        .bind(&username)
        .fetch_optional(tx.as_mut())
        .await
        .context("Failed to fetch OAuth user")?;

        let session_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now();
        let user = if let Some(user) = user {
            user
        } else {
            // Create new user
            let res = sqlx::query(
                r#"
                INSERT INTO oauth_users (provider, username, email, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
            .bind(&provider)
            .bind(&username)
            .bind(&email)
            .bind(now)
            .bind(now)
            .execute(tx.as_mut())
            .await
            .context("Failed to create OAuth user")?;

            OAuthUser {
                user_id: res.last_insert_rowid(),
                username: username.clone(),
                email: email.clone(),
                tsdb_username: None,
                tsdb_password: None,
                created_at: now,
                updated_at: now,
            }
        };

        let expires_at = now + chrono::Duration::seconds(expires_in);
        let access_token_expires_at =
            access_token_expires_in.map(|secs| now + chrono::Duration::seconds(secs));

        let session = OAuthSession {
            user: user.clone(),
            details: OAuthSessionDetails {
                session_id: session_id.clone(),
                user_id: user.user_id,
                access_token: access_token.clone(),
                refresh_token: refresh_token.clone(),
                id_token: id_token.clone(),
                access_token_expires_at,
                expires_at,
                login_at: now,
                last_active: now,
            },
        };

        // Encrypt tokens before storing in DB (encrypt_token uses same AES key as TDengine password)
        let encrypted_access = match access_token.as_ref() {
            Some(t) => Some(
                self.encrypt_token(t)
                    .context("Failed to encrypt access token")?,
            ),
            None => None,
        };
        let encrypted_refresh = match refresh_token.as_ref() {
            Some(t) => Some(
                self.encrypt_token(t)
                    .context("Failed to encrypt refresh token")?,
            ),
            None => None,
        };
        let encrypted_id = match id_token.as_ref() {
            Some(t) => Some(
                self.encrypt_token(t)
                    .context("Failed to encrypt id token")?,
            ),
            None => None,
        };

        sqlx::query(
            r#"
            INSERT INTO oauth_sessions
            (session_id, user_id, access_token, refresh_token, id_token, access_token_expires_at, expires_at, login_at, last_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#)
            .bind(session_id)
            .bind(user.user_id)
            .bind(encrypted_access)
            .bind(encrypted_refresh)
            .bind(encrypted_id)
            .bind(access_token_expires_at)
            .bind(expires_at)
            .bind(now)
            .bind(now)
        .execute(tx.as_mut())
        .await
        .context("Failed to create OAuth session")?;

        tx.commit()
            .await
            .context("Failed to commit oauth session creation changes")?;

        tracing::info!("Created OAuth session for user: {}", username);

        Ok(session)
    }

    /// Get a session by session_id
    pub async fn get_session(&self, session_id: &str) -> Result<Option<OAuthSession>> {
        let mut tx = self.pool.begin().await?;
        let user = sqlx::query_as::<_, OAuthUser>(
            r#"
            SELECT user_id, username, tsdb_username, tsdb_password, email, created_at, updated_at
            FROM oauth_users
            WHERE user_id = (
                SELECT user_id FROM oauth_sessions WHERE session_id = ?
            )
            "#,
        )
        .bind(session_id)
        .fetch_optional(tx.as_mut())
        .await
        .context("Failed to fetch OAuth user for session")?;
        if user.is_none() {
            return Ok(None);
        }
        let session = sqlx::query_as::<_, OAuthSessionDetails>(
            r#"
            SELECT session_id, user_id, access_token, refresh_token, id_token,
                   access_token_expires_at, expires_at, login_at, last_active
            FROM oauth_sessions
            WHERE session_id = ?
            "#,
        )
        .bind(session_id)
        .fetch_optional(tx.as_mut())
        .await
        .context("Failed to fetch OAuth session")?;
        if session.is_none() {
            return Ok(None);
        }

        // Decrypt tokens retrieved from storage before returning the session
        let mut details = session.unwrap();
        if let Some(enc) = &details.access_token {
            match self.decrypt_token(enc) {
                Ok(dec) => details.access_token = Some(dec),
                Err(e) => {
                    tracing::warn!(
                        "Failed to decrypt access_token for session {}: {:?}",
                        session_id,
                        e
                    );
                    // best-effort: keep encrypted value if decryption fails
                }
            }
        }
        if let Some(enc) = &details.refresh_token {
            match self.decrypt_token(enc) {
                Ok(dec) => details.refresh_token = Some(dec),
                Err(e) => {
                    tracing::warn!(
                        "Failed to decrypt refresh_token for session {}: {:?}",
                        session_id,
                        e
                    );
                }
            }
        }
        if let Some(enc) = &details.id_token {
            match self.decrypt_token(enc) {
                Ok(dec) => details.id_token = Some(dec),
                Err(e) => {
                    tracing::warn!(
                        "Failed to decrypt id_token for session {}: {:?}",
                        session_id,
                        e
                    );
                }
            }
        }

        let result = OAuthSession {
            user: user.unwrap(),
            details,
        };

        tx.commit()
            .await
            .context("Failed to commit oauth get session changes")?;

        Ok(Some(result))
    }

    /// Bind TDengine credentials to an OAuth user by session ID.
    #[instrument(skip(self), fields(session_id = session_id))]
    pub async fn bind_tsdb_credentials(
        &self,
        session_id: &str,
        tsdb_username: &str,
        tsdb_password: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now();

        // Encrypt password before storing
        let encrypted_password = self
            .encrypt_password(tsdb_password)
            .context("Failed to encrypt TDengine password")?;
        let res = sqlx::query(
            r#"
            UPDATE oauth_users
            SET tsdb_username = ?, tsdb_password = ?, updated_at = ?
            WHERE user_id = (
                SELECT user_id FROM oauth_sessions WHERE session_id = ?
            )
            "#,
        )
        .bind(tsdb_username)
        .bind(&encrypted_password)
        .bind(now)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("Failed to fetch OAuth username for session")?;
        let (rowid, affected) = (res.last_insert_rowid(), res.rows_affected());
        tracing::info!(
            "Bound TDengine credentials by session {}, rows affected: {}, rowid: {}",
            session_id,
            affected,
            rowid
        );
        let session = self.get_session(session_id).await?.ok_or_else(|| {
            anyhow::anyhow!("No user session found for session_id: {}", session_id)
        })?;

        tracing::info!(
            "Bound TDengine credentials for OAuth user: {}",
            session.username()
        );

        if session.get_tsdb_password().is_none() {
            return Err(anyhow::anyhow!(
                "Failed to bind TDengine credentials: password is None"
            ));
        }

        Ok(())
    }

    /// Verify if a session is valid (exists and not expired)
    #[instrument(skip(self), fields(session_id = session_id))]
    pub async fn verify_session(&self, session_id: &str) -> Result<Option<OAuthSession>> {
        let session = self.get_session(session_id).await?;

        if let Some(ref sess) = session {
            let now = chrono::Utc::now();

            if sess.details.expires_at < now {
                tracing::debug!("Session {} has expired", session_id);
                // Clean up expired session
                let _ = self.delete_session(session_id).await;
                return Ok(None);
            }

            // Update last_active timestamp
            let _ = self.update_last_active(session_id).await;
        }

        Ok(session)
    }

    /// Update last_active timestamp
    async fn update_last_active(&self, session_id: &str) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        sqlx::query(
            r#"
            UPDATE oauth_sessions
            SET last_active = ?
            WHERE session_id = ?
            "#,
        )
        .bind(now)
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("Failed to update session last_active")?;

        Ok(())
    }

    /// Delete a session
    #[instrument(skip(self), fields(session_id = session_id))]
    pub async fn delete_session(&self, session_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM oauth_sessions
            WHERE session_id = ?
            "#,
        )
        .bind(session_id)
        .execute(&self.pool)
        .await
        .context("Failed to delete OAuth session")?;

        let _ = self.connections.remove_async(session_id).await;

        tracing::info!("Deleted OAuth session: {}", session_id);

        Ok(())
    }

    /// Delete a user from database
    #[instrument(skip(self), fields(user_id))]
    pub async fn delete_user(&self, user_id: i64) -> Result<()> {
        let sessions: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT session_id FROM oauth_sessions
            WHERE user_id = ?
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .inspect_err(|err| {
            tracing::error!("Failed to fetch OAuth sessions to delete user: {}", err);
        })
        .unwrap_or_default();

        for session in sessions {
            let _ = self.delete_session(&session).await;
        }
        sqlx::query(
            r#"
                    DELETE FROM oauth_users
                    WHERE user_id = ?
                    "#,
        )
        .bind(user_id)
        .execute(&self.pool)
        .await
        .context("Failed to delete OAuth user")?;

        tracing::info!("Deleted OAuth user: {}", user_id);

        Ok(())
    }

    /// Refresh access token for a session
    /// Updates the access_token, refresh_token (if provided), and access_token_expires_at
    #[instrument(skip(self), fields(session_id = session_id))]
    pub async fn refresh_session_token(
        &self,
        session_id: &str,
        new_access_token: &str,
        new_refresh_token: Option<&str>,
        expires_in_seconds: Option<i64>,
    ) -> Result<()> {
        let now = chrono::Utc::now();
        let access_token_expires_at =
            expires_in_seconds.map(|secs| now + chrono::Duration::seconds(secs));

        // Encrypt tokens before storing, using the same AES key as TDengine password
        let encrypted_access = self
            .encrypt_token(new_access_token)
            .context("Failed to encrypt access token for storage")?;

        let query = if let Some(new_refresh) = new_refresh_token {
            let encrypted_refresh = self
                .encrypt_token(new_refresh)
                .context("Failed to encrypt refresh token for storage")?;

            sqlx::query(
                r#"
                UPDATE oauth_sessions
                SET access_token = ?, refresh_token = ?, access_token_expires_at = ?, last_active = ?
                WHERE session_id = ?
                "#,
            )
            .bind(encrypted_access)
            .bind(encrypted_refresh)
            .bind(access_token_expires_at)
            .bind(now)
            .bind(session_id)
        } else {
            sqlx::query(
                r#"
                UPDATE oauth_sessions
                SET access_token = ?, access_token_expires_at = ?, last_active = ?
                WHERE session_id = ?
                "#,
            )
            .bind(encrypted_access)
            .bind(access_token_expires_at)
            .bind(now)
            .bind(session_id)
        };

        query
            .execute(&self.pool)
            .await
            .context("Failed to update session tokens")?;

        tracing::info!("Refreshed access token for session: {}", session_id);

        Ok(())
    }

    /// Check if access token is about to expire (within 5 minutes)
    /// Returns true if token will expire in less than 5 minutes or is already expired
    pub fn is_access_token_expiring_soon(&self, session: &OAuthSession) -> bool {
        if let Some(expires_at) = session.details.access_token_expires_at {
            let now = chrono::Utc::now();
            let threshold = now + chrono::Duration::minutes(5);
            expires_at < threshold
        } else {
            // If no expiration time, assume it's not expiring
            false
        }
    }

    /// Sync OAuth users into oauth_users table (idempotent upsert)
    #[instrument(skip(self), fields(session_id = %session.session_id()))]
    pub async fn sync_users(
        &self,
        session: &OAuthSession,
        provider: &str,
        users: &[UserInfo],
        tsdb_options: &TsdbSyncOptions,
    ) -> Result<OAuthSyncSummary> {
        let pool = self.get_connection_pool(session).await?;
        let conn = pool.get().await.with_context(|| {
            format!(
                "Failed to get connection from pool for user {}",
                session.username()
            )
        })?;
        let mut tx = self.pool.begin().await?;
        let mut summary = OAuthSyncSummary {
            imported: 0,
            updated: 0,
            skipped: 0,
        };
        let now = chrono::Utc::now();

        for user in users {
            let existing = sqlx::query_as::<_, OAuthUser>(
                r#"
                SELECT user_id, username, tsdb_username, tsdb_password, email, created_at, updated_at
                FROM oauth_users
                WHERE provider = ? AND username = ?
                "#,
            )
            .bind(provider)
            .bind(&user.username)
            .fetch_optional(tx.as_mut())
            .await
            .context("Failed to fetch OAuth user during sync")?;

            if let Some(existing) = existing {
                let needs_update = user.email.is_some() && user.email != existing.email;
                if needs_update {
                    sqlx::query(
                        r#"
                        UPDATE oauth_users
                        SET email = ?, updated_at = ?
                        WHERE user_id = ?
                        "#,
                    )
                    .bind(&user.email)
                    .bind(now)
                    .bind(existing.user_id)
                    .execute(tx.as_mut())
                    .await
                    .context("Failed to update OAuth user during sync")?;
                    summary.updated += 1;
                } else {
                    summary.skipped += 1;
                }
            } else {
                let (tsdb_username, tsdb_password) =
                    tsdb_options.get_user_pass(provider, &user.username);
                if tsdb_password.contains('\'') {
                    return Err(anyhow!("Password contains single quote!"));
                }
                // let taos = self.get_connection();
                let encrypted_password = self
                    .encrypt_password(&tsdb_password)
                    .context("Try encrypt password while syncing users error")?;
                conn.exec_many([
                    format!(
                        "CREATE USER `{}` PASS '{}' SYSINFO 1",
                        tsdb_username, tsdb_password
                    ),
                    format!("GRANT read on *.* TO `{}`", tsdb_username),
                ])
                .await
                .context("Failed to create user in TDengine")?;
                sqlx::query(
                    r#"
                    INSERT INTO oauth_users (provider, username, email, tsdb_username, tsdb_password, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    "#,
                )
                .bind(provider)
                .bind(&user.username)
                .bind(&user.email)
                .bind(&tsdb_username)
                .bind(&encrypted_password)
                .bind(now)
                .bind(now)
                .execute(tx.as_mut())
                .await
                .context("Failed to create OAuth user during sync")?;
                summary.imported += 1;
            }
        }

        tx.commit()
            .await
            .context("Failed to commit OAuth user sync")?;
        Ok(summary)
    }

    /// Get connection pool by oauth session object
    pub async fn get_connection_pool(&self, session: &OAuthSession) -> Result<TaosPool> {
        if let Some(pool) = self.connections.get_async(session.session_id()).await {
            return Ok(pool.clone());
        }
        let username = session
            .get_tsdb_username()
            .ok_or_else(|| anyhow!("There's no TSDB username bound for {}", session.username()))?;
        let password = session
            .get_tsdb_password()
            .ok_or_else(|| anyhow!("There's no TSDB password bound for {}", session.username()))?;

        let decrypted_password = self.decrypt_password(password)?;
        let dsn = self
            .args
            .build_dsn(&TsdbCredential {
                auth_type: AuthType::Basic,
                username: username.to_string(),
                password: decrypted_password.clone(),
            })
            .map_err(|e| anyhow!("Failed to build DSN: {}", e.desc))?;

        let pool = TaosBuilder::from_dsn(dsn)?
            .pool()
            .context("Failed to create Taos connection pool")?;

        let _ = self
            .connections
            .insert(session.session_id().to_owned(), pool.clone());
        Ok(pool)
    }

    /// List existing OAuth users, optionally filtered by provider
    pub async fn list_oauth_users(&self, provider: Option<&str>) -> Result<Vec<OAuthUser>> {
        if let Some(provider) = provider {
            sqlx::query_as::<_, OAuthUser>(
                r#"
                SELECT user_id, username, tsdb_username, tsdb_password, email, created_at, updated_at
                FROM oauth_users
                WHERE provider = ?
                ORDER BY username
                "#,
            )
            .bind(provider)
            .fetch_all(&self.pool)
            .await
            .context("Failed to list OAuth users by provider")
        } else {
            sqlx::query_as::<_, OAuthUser>(
                r#"
                SELECT user_id, username, tsdb_username, tsdb_password, email, created_at, updated_at
                FROM oauth_users
                ORDER BY provider, username
                "#,
            )
            .fetch_all(&self.pool)
            .await
            .context("Failed to list all OAuth users")
        }
    }

    /// Clean up expired sessions
    #[instrument(skip(self))]
    pub async fn cleanup_expired_sessions(&self) -> Result<u64> {
        let now = Utc::now();

        let mut txn = self
            .pool
            .begin()
            .await
            .context("Failed to start transaction")?;

        let expired_sessions: Vec<String> =
            sqlx::query_scalar("SELECT session_id FROM oauth_sessions WHERE expires_at < ?")
                .bind(now)
                .fetch_all(txn.as_mut())
                .await
                .context("Failed to fetch expired sessions")?;
        if expired_sessions.is_empty() {
            let _ = txn.commit().await.inspect_err(|err| {
                tracing::error!("Failed to commit transaction: {}", err);
            });
            tracing::info!("No expired OAuth sessions found");
            return Ok(0);
        }
        for session_id in expired_sessions {
            let _ = self.connections.remove(&session_id);
        }

        let result = sqlx::query(
            r#"
            DELETE FROM oauth_sessions
            WHERE expires_at < ?
            "#,
        )
        .bind(now)
        .execute(txn.as_mut())
        .await
        .context("Failed to cleanup expired sessions")?;

        let count = result.rows_affected();
        if count > 0 {
            tracing::info!("Cleaned up {} expired OAuth sessions", count);
        }

        txn.commit().await.context("Failed to commit transaction")?;

        Ok(count)
    }

    // /// Get all sessions for a user
    // pub async fn get_user_sessions(&self, username: &str) -> Result<Vec<OAuthSession>> {
    //     let sessions = sqlx::query_as::<_, OAuthSession>(
    //         r#"
    //         SELECT session_id, username, email, access_token, refresh_token, id_token,
    //                tdengine_password, expires_at, created_at, last_active
    //         FROM oauth_sessions
    //         WHERE username = ?
    //         ORDER BY created_at DESC
    //         "#,
    //     )
    //     .bind(username)
    //     .fetch_all(&self.pool)
    //     .await
    //     .context("Failed to fetch user sessions")?;

    //     Ok(sessions)
    // }
}
