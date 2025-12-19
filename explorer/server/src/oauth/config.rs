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
