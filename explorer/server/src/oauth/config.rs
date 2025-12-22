use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct OAuthConfig {
    /// Whether OAuth is enabled
    pub enabled: bool,

    /// Provider type: "oidc" (OpenID Connect), "plain" (standard OAuth 2.0), "custom" (TSDB custom)
    pub provider: String,

    /// Provider display name (i18n support)
    pub provider_display_name: ProviderDisplayName,

    /// Customized fallback redirect URI. If not set, use `/`.
    #[cfg(debug_assertions)]
    pub fallback_redirect_uri: Option<String>,

    /// OIDC configuration (used when provider = "oidc")
    pub oidc: OidcConfig,

    /// Standard OAuth 2.0 configuration (used when provider = "plain")
    pub plain: PlainOAuthConfig,

    /// Custom OAuth configuration (used when provider = "custom")
    pub custom: CustomOAuthConfig,

    /// User attribute mapping from id_token claims or profile response
    pub user_mapping: UserMapping,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ProviderDisplayName {
    /// English display name
    pub en: String,

    /// Chinese display name
    pub zh: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct OidcConfig {
    /// OAuth 2.0 client ID
    #[serde(skip_serializing)]
    pub client_id: String,

    /// OAuth 2.0 client secret
    #[serde(skip_serializing)]
    pub client_secret: String,

    /// OIDC issuer URL (e.g., https://idp.example.com)
    pub issuer_url: String,

    /// OAuth 2.0 redirect URI
    pub redirect_uri: String,

    /// OAuth 2.0 scopes
    pub scopes: Vec<String>,

    /// Optional: Authorization endpoint (will be discovered if not set)
    pub authorization_endpoint: Option<String>,

    /// Optional: Token endpoint (will be discovered if not set)
    pub token_endpoint: Option<String>,

    /// Optional: UserInfo endpoint (will be discovered if not set)
    pub userinfo_endpoint: Option<String>,

    /// Optional: JWKS URI (will be discovered if not set)
    pub jwks_uri: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PlainOAuthConfig {
    /// OAuth 2.0 client ID
    #[serde(skip_serializing)]
    pub client_id: String,

    /// OAuth 2.0 client secret
    #[serde(skip_serializing)]
    pub client_secret: String,

    /// Authorization endpoint URL
    pub authorize_url: String,

    /// Token endpoint URL
    pub token_url: String,

    /// Profile/UserInfo endpoint URL
    pub profile_url: String,

    /// OAuth 2.0 redirect URI
    pub redirect_uri: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct CustomOAuthConfig {
    /// OAuth 2.0 client ID
    #[serde(skip_serializing)]
    pub client_id: String,

    /// OAuth 2.0 client secret
    #[serde(skip_serializing)]
    pub client_secret: String,

    /// Authorization endpoint URL
    pub authorize_url: String,

    /// Token endpoint URL
    pub token_url: String,

    /// Login endpoint URL
    pub login_url: Option<String>,

    /// Profile/UserInfo endpoint URL
    pub profile_url: String,

    /// Sync users endpoint URL (custom)
    pub fetch_users_url: Option<String>,

    /// OAuth 2.0 redirect URI
    pub redirect_uri: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct UserMapping {
    /// Claim name for username (default: "preferred_username")
    pub username: String,

    /// Claim name for email (default: "email")
    pub email: String,

    /// Claim name for first name (default: "given_name")
    pub first_name: String,

    /// Claim name for last name (default: "family_name")
    pub last_name: String,

    /// Claim name for roles/groups (default: "groups")
    pub roles: String,
}

impl Default for ProviderDisplayName {
    fn default() -> Self {
        Self {
            en: "OAuth".to_string(),
            zh: "OAuth".to_string(),
        }
    }
}

impl Default for OAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "oidc".to_string(),
            provider_display_name: ProviderDisplayName::default(),
            #[cfg(debug_assertions)]
            fallback_redirect_uri: None,
            oidc: OidcConfig::default(),
            plain: PlainOAuthConfig::default(),
            custom: CustomOAuthConfig::default(),
            user_mapping: UserMapping::default(),
        }
    }
}

impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            issuer_url: String::new(),
            redirect_uri: "http://localhost:6060/api/-/oauth/callback".to_string(),
            scopes: vec![
                "openid".to_string(),
                "profile".to_string(),
                "email".to_string(),
            ],
            authorization_endpoint: None,
            token_endpoint: None,
            userinfo_endpoint: None,
            jwks_uri: None,
        }
    }
}

impl Default for PlainOAuthConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            authorize_url: String::new(),
            token_url: String::new(),
            profile_url: String::new(),
            redirect_uri: "http://localhost:6060/api/-/oauth/callback".to_string(),
        }
    }
}

impl Default for CustomOAuthConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            authorize_url: String::new(),
            token_url: String::new(),
            profile_url: String::new(),
            login_url: None,
            fetch_users_url: None,
            redirect_uri: "http://localhost:6060/api/-/oauth/callback".to_string(),
        }
    }
}

impl Default for UserMapping {
    fn default() -> Self {
        Self {
            username: "preferred_username".to_string(),
            email: "email".to_string(),
            first_name: "given_name".to_string(),
            last_name: "family_name".to_string(),
            roles: "groups".to_string(),
        }
    }
}

impl OAuthConfig {
    /// Load OAuth config from environment variables (currently OIDC only)
    pub fn update_by_env(&mut self) {
        if let Ok(val) = std::env::var("EXPLORER_OAUTH_ENABLED") {
            self.enabled = matches!(val.as_str(), "1" | "true" | "True" | "TRUE" | "yes" | "Yes");
        }

        if let Ok(val) = std::env::var("EXPLORER_OAUTH_CLIENT_ID") {
            self.oidc.client_id = val;
        }

        if let Ok(val) = std::env::var("EXPLORER_OAUTH_CLIENT_SECRET") {
            self.oidc.client_secret = val;
        }

        if let Ok(val) = std::env::var("EXPLORER_OAUTH_ISSUER_URL") {
            self.oidc.issuer_url = val;
        }

        if let Ok(val) = std::env::var("EXPLORER_OAUTH_REDIRECT_URI") {
            self.oidc.redirect_uri = val;
        }

        if let Ok(val) = std::env::var("EXPLORER_OAUTH_SCOPES") {
            self.oidc.scopes = val.split(',').map(|s| s.trim().to_string()).collect();
        }
    }

    /// Validate OAuth configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        match self.provider.as_str() {
            "oidc" => self.validate_oidc(),
            "plain" => self.validate_plain(),
            "custom" => self.validate_custom(),
            _ => anyhow::bail!(
                "Unknown OAuth provider type: '{}'. Supported: 'oidc', 'plain', 'custom'",
                self.provider
            ),
        }
    }

    pub fn plain_config(&self) -> PlainOAuthConfig {
        if self.plain.authorize_url.is_empty() && !self.custom.authorize_url.is_empty() {
            // Fallback to legacy custom config when plain section is not provided
            PlainOAuthConfig {
                client_id: self.custom.client_id.clone(),
                client_secret: self.custom.client_secret.clone(),
                authorize_url: self.custom.authorize_url.clone(),
                token_url: self.custom.token_url.clone(),
                profile_url: self.custom.profile_url.clone(),
                redirect_uri: self.custom.redirect_uri.clone(),
            }
        } else {
            self.plain.clone()
        }
    }

    /// Validate OIDC configuration
    fn validate_oidc(&self) -> anyhow::Result<()> {
        Self::validate_required(&self.oidc.client_id, "OIDC client_id", "oidc")?;
        Self::validate_required(&self.oidc.client_secret, "OIDC client_secret", "oidc")?;
        Self::validate_required(&self.oidc.issuer_url, "OIDC issuer_url", "oidc")?;
        url::Url::parse(&self.oidc.issuer_url)
            .map_err(|e| anyhow::anyhow!("Invalid OIDC issuer_url: {}", e))?;
        Ok(())
    }

    /// Validate plain OAuth configuration
    fn validate_plain(&self) -> anyhow::Result<()> {
        let plain = self.plain_config();

        Self::validate_required(&plain.client_id, "plain OAuth client_id", "plain")?;
        Self::validate_required(&plain.client_secret, "plain OAuth client_secret", "plain")?;
        Self::validate_required(&plain.authorize_url, "plain OAuth authorize_url", "plain")?;
        Self::validate_required(&plain.token_url, "plain OAuth token_url", "plain")?;
        Self::validate_required(&plain.profile_url, "plain OAuth profile_url", "plain")?;
        Self::validate_url(&plain.authorize_url, "plain OAuth authorize_url")?;
        Self::validate_url(&plain.token_url, "plain OAuth token_url")?;
        Self::validate_url(&plain.profile_url, "plain OAuth profile_url")?;
        Self::validate_url(&plain.redirect_uri, "plain OAuth redirect_uri")?;
        Ok(())
    }

    /// Validate custom OAuth configuration
    fn validate_custom(&self) -> anyhow::Result<()> {
        Self::validate_required(&self.custom.client_id, "Custom OAuth client_id", "custom")?;
        Self::validate_required(
            &self.custom.client_secret,
            "Custom OAuth client_secret",
            "custom",
        )?;
        Self::validate_required(
            &self.custom.authorize_url,
            "Custom OAuth authorize_url",
            "custom",
        )?;
        Self::validate_required(&self.custom.token_url, "Custom OAuth token_url", "custom")?;
        Self::validate_required(
            &self.custom.profile_url,
            "Custom OAuth profile_url",
            "custom",
        )?;
        Self::validate_url(&self.custom.authorize_url, "Custom OAuth authorize_url")?;
        Self::validate_url(&self.custom.token_url, "Custom OAuth token_url")?;
        Self::validate_url(&self.custom.profile_url, "Custom OAuth profile_url")?;
        Self::validate_url(&self.custom.redirect_uri, "Custom OAuth redirect_uri")?;
        if let Some(url) = self.custom.fetch_users_url.as_deref() {
            Self::validate_url(url, "Custom OAuth fetch_users")?;
        }
        Ok(())
    }

    fn validate_required(value: &str, field: &str, provider: &str) -> anyhow::Result<()> {
        if value.is_empty() {
            anyhow::bail!("{} is required when provider is '{}'", field, provider);
        }
        Ok(())
    }

    fn validate_url(value: &str, field: &str) -> anyhow::Result<()> {
        url::Url::parse(value)
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("Invalid {}: {}", field, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_default_oauth_config() {
        let config = OAuthConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.provider, "oidc");
        assert_eq!(config.provider_display_name.en, "OAuth");
        assert_eq!(config.provider_display_name.zh, "OAuth");
    }

    #[test]
    fn test_default_oidc_config() {
        let config = OidcConfig::default();
        assert_eq!(config.client_id, "");
        assert_eq!(config.client_secret, "");
        assert_eq!(config.issuer_url, "");
        assert_eq!(
            config.redirect_uri,
            "http://localhost:6060/api/-/oauth/callback"
        );
        assert_eq!(config.scopes, vec!["openid", "profile", "email"]);
        assert!(config.authorization_endpoint.is_none());
        assert!(config.token_endpoint.is_none());
        assert!(config.userinfo_endpoint.is_none());
        assert!(config.jwks_uri.is_none());
    }

    #[test]
    fn test_default_plain_oauth_config() {
        let config = PlainOAuthConfig::default();
        assert_eq!(config.client_id, "");
        assert_eq!(config.client_secret, "");
        assert_eq!(config.authorize_url, "");
        assert_eq!(config.token_url, "");
        assert_eq!(config.profile_url, "");
        assert_eq!(
            config.redirect_uri,
            "http://localhost:6060/api/-/oauth/callback"
        );
    }

    #[test]
    fn test_default_custom_oauth_config() {
        let config = CustomOAuthConfig::default();
        assert_eq!(config.client_id, "");
        assert_eq!(config.client_secret, "");
        assert_eq!(config.authorize_url, "");
        assert_eq!(config.token_url, "");
        assert_eq!(config.profile_url, "");
        assert!(config.login_url.is_none());
        assert!(config.fetch_users_url.is_none());
        assert_eq!(
            config.redirect_uri,
            "http://localhost:6060/api/-/oauth/callback"
        );
    }

    #[test]
    fn test_default_user_mapping() {
        let mapping = UserMapping::default();
        assert_eq!(mapping.username, "preferred_username");
        assert_eq!(mapping.email, "email");
        assert_eq!(mapping.first_name, "given_name");
        assert_eq!(mapping.last_name, "family_name");
        assert_eq!(mapping.roles, "groups");
    }

    #[test]
    fn test_default_provider_display_name() {
        let name = ProviderDisplayName::default();
        assert_eq!(name.en, "OAuth");
        assert_eq!(name.zh, "OAuth");
    }

    #[test]
    fn test_validate_disabled_config() {
        let config = OAuthConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_oidc_missing_client_id() {
        let config = OAuthConfig {
            enabled: true,
            provider: "oidc".to_string(),
            oidc: OidcConfig {
                issuer_url: "https://example.com".to_string(),
                client_secret: "secret".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("OIDC client_id is required"));
    }

    #[test]
    fn test_validate_oidc_missing_client_secret() {
        let config = OAuthConfig {
            enabled: true,
            provider: "oidc".to_string(),
            oidc: OidcConfig {
                issuer_url: "https://example.com".to_string(),
                client_id: "client123".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("OIDC client_secret is required"));
    }

    #[test]
    fn test_validate_oidc_missing_issuer_url() {
        let config = OAuthConfig {
            enabled: true,
            provider: "oidc".to_string(),
            oidc: OidcConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("OIDC issuer_url is required"));
    }

    #[test]
    fn test_validate_oidc_invalid_issuer_url() {
        let config = OAuthConfig {
            enabled: true,
            provider: "oidc".to_string(),
            oidc: OidcConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                issuer_url: "not-a-valid-url".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid OIDC issuer_url"));
    }

    #[test]
    fn test_validate_oidc_success() {
        let config = OAuthConfig {
            enabled: true,
            provider: "oidc".to_string(),
            oidc: OidcConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                issuer_url: "https://example.com".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_plain_missing_client_id() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("plain OAuth client_id is required"));
    }

    #[test]
    fn test_validate_plain_missing_authorize_url() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("plain OAuth authorize_url is required"));
    }

    #[test]
    fn test_validate_plain_invalid_token_url() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "not-a-url".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid plain OAuth token_url"));
    }

    #[test]
    fn test_validate_plain_success() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_custom_missing_fields() {
        let config = OAuthConfig {
            enabled: true,
            provider: "custom".to_string(),
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_custom_invalid_profile_url() {
        let config = OAuthConfig {
            enabled: true,
            provider: "custom".to_string(),
            custom: CustomOAuthConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "invalid-url".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid Custom OAuth profile_url"));
    }

    #[test]
    fn test_validate_custom_success() {
        let config = OAuthConfig {
            enabled: true,
            provider: "custom".to_string(),
            custom: CustomOAuthConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_custom_with_optional_urls() {
        let config = OAuthConfig {
            enabled: true,
            provider: "custom".to_string(),
            custom: CustomOAuthConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                login_url: Some("https://example.com/login".to_string()),
                fetch_users_url: Some("https://example.com/users".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_custom_with_invalid_fetch_users_url() {
        let config = OAuthConfig {
            enabled: true,
            provider: "custom".to_string(),
            custom: CustomOAuthConfig {
                client_id: "client123".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                fetch_users_url: Some("not-a-url".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid Custom OAuth fetch_users"));
    }

    #[test]
    fn test_validate_unknown_provider() {
        let config = OAuthConfig {
            enabled: true,
            provider: "unknown".to_string(),
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown OAuth provider type"));
    }

    #[test]
    #[serial]
    fn test_update_by_env_enabled() {
        // Save current state
        let prev_val = std::env::var("EXPLORER_OAUTH_ENABLED").ok();

        std::env::set_var("EXPLORER_OAUTH_ENABLED", "true");
        let mut config = OAuthConfig::default();
        config.update_by_env();
        assert!(config.enabled);

        // Restore previous state
        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_ENABLED", val),
            None => std::env::remove_var("EXPLORER_OAUTH_ENABLED"),
        }
    }

    #[test]
    #[serial]
    fn test_update_by_env_enabled_variants() {
        // Save current state
        let prev_val = std::env::var("EXPLORER_OAUTH_ENABLED").ok();

        for val in ["1", "true", "True", "TRUE", "yes", "Yes"] {
            std::env::set_var("EXPLORER_OAUTH_ENABLED", val);
            let mut config = OAuthConfig::default();
            config.update_by_env();
            assert!(config.enabled, "Failed for value: {}", val);
        }

        // Restore previous state
        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_ENABLED", val),
            None => std::env::remove_var("EXPLORER_OAUTH_ENABLED"),
        }
    }

    #[test]
    #[serial]
    fn test_update_by_env_disabled() {
        // Save current state
        let prev_val = std::env::var("EXPLORER_OAUTH_ENABLED").ok();

        std::env::set_var("EXPLORER_OAUTH_ENABLED", "false");
        let mut config = OAuthConfig::default();
        config.update_by_env();
        assert!(!config.enabled);

        // Restore previous state
        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_ENABLED", val),
            None => std::env::remove_var("EXPLORER_OAUTH_ENABLED"),
        }
    }

    #[test]
    #[serial]
    fn test_update_by_env_client_id() {
        let prev_val = std::env::var("EXPLORER_OAUTH_CLIENT_ID").ok();

        std::env::set_var("EXPLORER_OAUTH_CLIENT_ID", "test_client");
        let mut config = OAuthConfig::default();
        config.update_by_env();
        assert_eq!(config.oidc.client_id, "test_client");

        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_CLIENT_ID", val),
            None => std::env::remove_var("EXPLORER_OAUTH_CLIENT_ID"),
        }
    }

    #[test]
    #[serial]
    fn test_update_by_env_client_secret() {
        let prev_val = std::env::var("EXPLORER_OAUTH_CLIENT_SECRET").ok();

        std::env::set_var("EXPLORER_OAUTH_CLIENT_SECRET", "test_secret");
        let mut config = OAuthConfig::default();
        config.update_by_env();
        assert_eq!(config.oidc.client_secret, "test_secret");

        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_CLIENT_SECRET", val),
            None => std::env::remove_var("EXPLORER_OAUTH_CLIENT_SECRET"),
        }
    }

    #[test]
    #[serial]
    fn test_update_by_env_issuer_url() {
        let prev_val = std::env::var("EXPLORER_OAUTH_ISSUER_URL").ok();

        std::env::set_var("EXPLORER_OAUTH_ISSUER_URL", "https://issuer.example.com");
        let mut config = OAuthConfig::default();
        config.update_by_env();
        assert_eq!(config.oidc.issuer_url, "https://issuer.example.com");

        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_ISSUER_URL", val),
            None => std::env::remove_var("EXPLORER_OAUTH_ISSUER_URL"),
        }
    }

    #[test]
    #[serial]
    fn test_update_by_env_redirect_uri() {
        let prev_val = std::env::var("EXPLORER_OAUTH_REDIRECT_URI").ok();

        std::env::set_var(
            "EXPLORER_OAUTH_REDIRECT_URI",
            "https://app.example.com/callback",
        );
        let mut config = OAuthConfig::default();
        config.update_by_env();
        assert_eq!(config.oidc.redirect_uri, "https://app.example.com/callback");

        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_REDIRECT_URI", val),
            None => std::env::remove_var("EXPLORER_OAUTH_REDIRECT_URI"),
        }
    }

    #[test]
    #[serial]
    fn test_update_by_env_scopes() {
        let prev_val = std::env::var("EXPLORER_OAUTH_SCOPES").ok();

        std::env::set_var("EXPLORER_OAUTH_SCOPES", "openid,profile,email,groups");
        let mut config = OAuthConfig::default();
        config.update_by_env();
        assert_eq!(
            config.oidc.scopes,
            vec!["openid", "profile", "email", "groups"]
        );

        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_SCOPES", val),
            None => std::env::remove_var("EXPLORER_OAUTH_SCOPES"),
        }
    }

    #[test]
    #[serial]
    fn test_update_by_env_scopes_with_spaces() {
        let prev_val = std::env::var("EXPLORER_OAUTH_SCOPES").ok();

        std::env::set_var("EXPLORER_OAUTH_SCOPES", "openid, profile, email");
        let mut config = OAuthConfig::default();
        config.update_by_env();
        assert_eq!(config.oidc.scopes, vec!["openid", "profile", "email"]);

        match prev_val {
            Some(val) => std::env::set_var("EXPLORER_OAUTH_SCOPES", val),
            None => std::env::remove_var("EXPLORER_OAUTH_SCOPES"),
        }
    }

    #[test]
    fn test_plain_config_fallback_to_custom() {
        let mut config = OAuthConfig::default();
        config.custom.client_id = "custom_client".to_string();
        config.custom.client_secret = "custom_secret".to_string();
        config.custom.authorize_url = "https://custom.com/auth".to_string();
        config.custom.token_url = "https://custom.com/token".to_string();
        config.custom.profile_url = "https://custom.com/profile".to_string();
        config.custom.redirect_uri = "https://custom.com/callback".to_string();

        let plain = config.plain_config();
        assert_eq!(plain.client_id, "custom_client");
        assert_eq!(plain.client_secret, "custom_secret");
        assert_eq!(plain.authorize_url, "https://custom.com/auth");
        assert_eq!(plain.token_url, "https://custom.com/token");
        assert_eq!(plain.profile_url, "https://custom.com/profile");
        assert_eq!(plain.redirect_uri, "https://custom.com/callback");
    }

    #[test]
    fn test_plain_config_prefers_plain_over_custom() {
        let mut config = OAuthConfig::default();
        config.plain.client_id = "plain_client".to_string();
        config.plain.authorize_url = "https://plain.com/auth".to_string();
        config.custom.client_id = "custom_client".to_string();
        config.custom.authorize_url = "https://custom.com/auth".to_string();

        let plain = config.plain_config();
        assert_eq!(plain.client_id, "plain_client");
        assert_eq!(plain.authorize_url, "https://plain.com/auth");
    }

    #[test]
    fn test_serde_serialization() {
        let config = OAuthConfig {
            enabled: true,
            provider: "oidc".to_string(),
            provider_display_name: ProviderDisplayName {
                en: "Test OAuth".to_string(),
                zh: "测试OAuth".to_string(),
            },
            #[cfg(debug_assertions)]
            fallback_redirect_uri: Some("/dashboard".to_string()),
            oidc: OidcConfig {
                client_id: "client123".to_string(),
                client_secret: "secret456".to_string(),
                issuer_url: "https://example.com".to_string(),
                redirect_uri: "https://app.com/callback".to_string(),
                scopes: vec!["openid".to_string(), "profile".to_string()],
                authorization_endpoint: Some("https://example.com/auth".to_string()),
                token_endpoint: Some("https://example.com/token".to_string()),
                userinfo_endpoint: Some("https://example.com/userinfo".to_string()),
                jwks_uri: Some("https://example.com/jwks".to_string()),
            },
            plain: PlainOAuthConfig::default(),
            custom: CustomOAuthConfig::default(),
            user_mapping: UserMapping::default(),
        };

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"enabled\":true"));
        assert!(json.contains("\"provider\":\"oidc\""));
        // Client secret should be skipped in serialization
        assert!(!json.contains("secret456"));
    }

    #[test]
    fn test_serde_deserialization() {
        let json = r#"{
            "enabled": true,
            "provider": "plain",
            "provider_display_name": {
                "en": "GitHub",
                "zh": "GitHub"
            },
            "oidc": {
                "client_id": "",
                "client_secret": "",
                "issuer_url": "",
                "redirect_uri": "http://localhost:6060/api/-/oauth/callback",
                "scopes": ["openid", "profile", "email"]
            },
            "plain": {
                "client_id": "github_client",
                "client_secret": "github_secret",
                "authorize_url": "https://github.com/login/oauth/authorize",
                "token_url": "https://github.com/login/oauth/access_token",
                "profile_url": "https://api.github.com/user",
                "redirect_uri": "http://localhost:6060/api/-/oauth/callback"
            },
            "custom": {
                "client_id": "",
                "client_secret": "",
                "authorize_url": "",
                "token_url": "",
                "profile_url": "",
                "redirect_uri": "http://localhost:6060/api/-/oauth/callback"
            },
            "user_mapping": {
                "username": "login",
                "email": "email",
                "first_name": "name",
                "last_name": "name",
                "roles": "groups"
            }
        }"#;

        let config: OAuthConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.provider, "plain");
        assert_eq!(config.plain.client_id, "github_client");
        assert_eq!(config.user_mapping.username, "login");
    }

    #[test]
    fn test_custom_oauth_config_with_optional_fields() {
        let config = CustomOAuthConfig {
            client_id: "client".to_string(),
            client_secret: "secret".to_string(),
            authorize_url: "https://example.com/auth".to_string(),
            token_url: "https://example.com/token".to_string(),
            login_url: Some("https://example.com/login".to_string()),
            profile_url: "https://example.com/profile".to_string(),
            fetch_users_url: Some("https://example.com/sync".to_string()),
            redirect_uri: "https://example.com/callback".to_string(),
        };

        assert!(config.login_url.is_some());
        assert!(config.fetch_users_url.is_some());
        assert_eq!(config.login_url.unwrap(), "https://example.com/login");
        assert_eq!(config.fetch_users_url.unwrap(), "https://example.com/sync");
    }

    #[test]
    fn test_user_mapping_custom_claims() {
        let mapping = UserMapping {
            username: "sub".to_string(),
            email: "mail".to_string(),
            first_name: "firstName".to_string(),
            last_name: "lastName".to_string(),
            roles: "memberOf".to_string(),
        };

        assert_eq!(mapping.username, "sub");
        assert_eq!(mapping.email, "mail");
        assert_eq!(mapping.first_name, "firstName");
        assert_eq!(mapping.last_name, "lastName");
        assert_eq!(mapping.roles, "memberOf");
    }

    #[test]
    fn test_oidc_config_with_manual_endpoints() {
        let config = OidcConfig {
            client_id: "client".to_string(),
            client_secret: "secret".to_string(),
            issuer_url: "https://example.com".to_string(),
            redirect_uri: "https://app.com/callback".to_string(),
            scopes: vec!["openid".to_string()],
            authorization_endpoint: Some("https://example.com/authorize".to_string()),
            token_endpoint: Some("https://example.com/token".to_string()),
            userinfo_endpoint: Some("https://example.com/userinfo".to_string()),
            jwks_uri: Some("https://example.com/.well-known/jwks.json".to_string()),
        };

        assert!(config.authorization_endpoint.is_some());
        assert!(config.token_endpoint.is_some());
        assert!(config.userinfo_endpoint.is_some());
        assert!(config.jwks_uri.is_some());
    }

    #[test]
    fn test_provider_display_name_i18n() {
        let name = ProviderDisplayName {
            en: "Google Workspace".to_string(),
            zh: "谷歌工作区".to_string(),
        };

        assert_eq!(name.en, "Google Workspace");
        assert_eq!(name.zh, "谷歌工作区");
    }

    #[test]
    fn test_validate_url_helper() {
        assert!(OAuthConfig::validate_url("https://example.com", "test").is_ok());
        assert!(OAuthConfig::validate_url("http://localhost:8080", "test").is_ok());
        assert!(OAuthConfig::validate_url("https://example.com/path?query=value", "test").is_ok());
        assert!(OAuthConfig::validate_url("not-a-url", "test").is_err());
        assert!(OAuthConfig::validate_url("", "test").is_err());
    }

    #[test]
    fn test_validate_required_helper() {
        assert!(OAuthConfig::validate_required("value", "field", "provider").is_ok());
        assert!(OAuthConfig::validate_required("", "field", "provider").is_err());
    }

    #[test]
    fn test_clone_configs() {
        let config = OAuthConfig::default();
        let cloned = config.clone();
        assert_eq!(config.enabled, cloned.enabled);
        assert_eq!(config.provider, cloned.provider);

        let oidc = OidcConfig::default();
        let cloned_oidc = oidc.clone();
        assert_eq!(oidc.client_id, cloned_oidc.client_id);

        let plain = PlainOAuthConfig::default();
        let cloned_plain = plain.clone();
        assert_eq!(plain.redirect_uri, cloned_plain.redirect_uri);

        let custom = CustomOAuthConfig::default();
        let cloned_custom = custom.clone();
        assert_eq!(custom.profile_url, cloned_custom.profile_url);

        let mapping = UserMapping::default();
        let cloned_mapping = mapping.clone();
        assert_eq!(mapping.username, cloned_mapping.username);
    }

    #[test]
    fn test_debug_format() {
        let config = OAuthConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("OAuthConfig"));
        assert!(debug_str.contains("enabled"));

        let oidc = OidcConfig::default();
        let debug_str = format!("{:?}", oidc);
        assert!(debug_str.contains("OidcConfig"));

        let plain = PlainOAuthConfig::default();
        let debug_str = format!("{:?}", plain);
        assert!(debug_str.contains("PlainOAuthConfig"));
    }
}
