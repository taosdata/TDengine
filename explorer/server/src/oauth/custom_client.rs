use super::client::UserInfo;
use super::config::OAuthConfig;
use crate::utils::deserialize_non_empty_string;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

/// OAuth client for TSDB “custom” provider.
///
/// It reuses standard OAuth 2.0 logic (authorization, token exchange,
/// profile fetch, refresh) while keeping the custom-only user sync
/// endpoints and flexible profile parsing.
#[derive(Clone)]
pub struct CustomOAuthClient {
    config: OAuthConfig,
    http_client: reqwest::Client,
    plain: PlainDelegate,
}

#[derive(Debug, Clone)]
pub struct AuthorizationRequest {
    pub auth_url: String,
    pub csrf_token: String,
}

/// Token response from custom OAuth (supports both direct and wrapped forms)
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TokenResponse {
    Direct {
        access_token: String,
        #[serde(default)]
        refresh_token: Option<String>,
        #[serde(default)]
        expires_in: Option<i64>,
        #[serde(default)]
        #[allow(dead_code)]
        token_type: Option<String>,
    },
    Wrapped {
        data: TokenData,
    },
}

#[derive(Debug, Deserialize)]
struct TokenData {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
}

/// Flexible profile response structure (standard + custom fields)
#[derive(Debug, Deserialize)]
struct ProfileResponse {
    // Standard OAuth 2.0 / OIDC fields
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    given_name: Option<String>,
    #[serde(default)]
    family_name: Option<String>,

    // Custom provider fields (for backward compatibility)
    #[serde(default)]
    attributes: ProfileAttributes,

    #[serde(flatten)]
    #[allow(dead_code)]
    additional: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Default)]
#[allow(dead_code)]
struct ProfileAttributes {
    #[serde(default)]
    token_expired: Option<i64>,
    #[serde(default)]
    token_time: Option<i64>,
    #[serde(default)]
    roles: Vec<RoleInfo>,
    #[serde(default)]
    orgs: Vec<OrgInfo>,
}

#[derive(Debug, Deserialize)]
struct RoleInfo {
    role_name: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct OrgInfo {
    org_name: String,
    #[serde(default)]
    org_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SyncUserOrg {
    #[serde(default)]
    org_name: Option<String>,
    #[serde(default)]
    org_display: Option<String>,
    #[serde(default)]
    org_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SyncUserRole {
    #[serde(default)]
    role_name: Option<String>,
    #[serde(default)]
    role_display_name: Option<String>,
    #[serde(default)]
    org: Option<SyncUserOrg>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SyncUserItem {
    #[serde(default)]
    user_name: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    user_display_name: Option<String>,
    #[serde(default)]
    user_org_path: Option<String>,
    #[serde(default)]
    org: Option<SyncUserOrg>,
    #[serde(default)]
    roles: Option<Vec<SyncUserRole>>,
}

#[derive(Debug, Deserialize)]
struct ErrorDetails {
    success: bool,
    code: i32,
    msg: String,
    details: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LoginData {
    data: AccessToken,
}

#[derive(Debug, Deserialize)]
struct AccessToken {
    access_token: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LoginSuccess {
    success: bool,
    code: i32,
    data: AccessToken,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum LoginResponse {
    Success(LoginSuccess),
    Error(ErrorDetails),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchUsersCredentials {
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub username: String,
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    pub password: String,
}

impl TokenResponse {
    fn access_token(&self) -> String {
        match self {
            TokenResponse::Direct { access_token, .. } => access_token.trim().to_string(),
            TokenResponse::Wrapped { data } => data.access_token.trim().to_string(),
        }
    }

    fn refresh_token(&self) -> Option<String> {
        match self {
            TokenResponse::Direct { refresh_token, .. } => {
                refresh_token.as_ref().map(|t| t.trim().to_string())
            }
            TokenResponse::Wrapped { data } => {
                data.refresh_token.as_ref().map(|t| t.trim().to_string())
            }
        }
    }

    fn expires_in(&self) -> Option<i64> {
        match self {
            TokenResponse::Direct { expires_in, .. } => *expires_in,
            TokenResponse::Wrapped { data } => data.expires_in,
        }
    }
}

impl CustomOAuthClient {
    /// Create a new custom OAuth client (uses standard flow + custom sync).
    pub fn new(config: OAuthConfig) -> Result<Self> {
        config.validate()?;

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        let plain = PlainDelegate::new(&config, http_client.clone());

        Ok(Self {
            config,
            http_client,
            plain,
        })
    }

    /// Generate authorization URL (standard OAuth 2.0).
    pub fn generate_auth_url(&self) -> AuthorizationRequest {
        self.plain.generate_auth_url()
    }

    /// Exchange authorization code for tokens.
    /// Returns (access_token, refresh_token, expires_in_seconds)
    pub async fn exchange_code(&self, code: &str) -> Result<(String, Option<String>, Option<i64>)> {
        self.plain.exchange_code(code).await
    }

    /// Fetch user info (standard with custom-compatible parsing).
    pub async fn fetch_user_info(&self, access_token: &str) -> Result<UserInfo> {
        self.plain.fetch_user_info(access_token).await
    }

    /// Logs in to the custom OAuth provider and returns the access token.
    async fn login(&self, credentials: &FetchUsersCredentials) -> Result<String> {
        let login_url = self
            .config
            .custom
            .login_url
            .as_ref()
            .context("Login URL not configured")?;
        tracing::debug!("Logging in to custom OAuth provider");
        let response = self
            .http_client
            .post(login_url)
            .json(credentials)
            .send()
            .await
            .context("Failed to login")?;

        let response_text = response
            .text()
            .await
            .context("Failed to read token response body")?;

        tracing::debug!("Token response body: {}", response_text);

        let login_response: LoginResponse =
            serde_json::from_str(&response_text).context("Failed to parse token response")?;

        match login_response {
            LoginResponse::Success(success) => {
                let access_token = success.data.access_token;

                Ok(access_token)
            }
            LoginResponse::Error(error) => Err(anyhow!(
                "Failed to login: {}, code: {}, details: {}",
                error.msg,
                error.code,
                error.details
            )),
        }
    }

    /// Fetch all users from custom sync endpoint (custom-only).
    pub async fn fetch_users(&self, credentials: &FetchUsersCredentials) -> Result<Vec<UserInfo>> {
        let access_token = self.login(credentials).await?;

        let url = if let Some(url) = &self.config.custom.fetch_users_url {
            url
        } else {
            anyhow::bail!("Custom fetch users URL is not configured");
        };

        tracing::debug!(
            "Fetching users from custom OAuth sync endpoint with access token: {}",
            access_token
        );

        let response = self
            .http_client
            .get(url)
            .query(&[("clientId", self.config.custom.client_id.as_str())])
            .header("Access_token", access_token)
            // .bearer_auth(access_token)
            .send()
            .await
            .context("Failed to call sync users endpoint")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Sync users endpoint returned error: {} - {}", status, body);
        }

        let body = response
            .text()
            .await
            .context("Failed to read sync users response body")?;
        #[cfg(debug_assertions)]
        tracing::debug!("Fetch remote user list: {body}");

        // Test if error
        //
        // {"success":false,"code":500,"msg":"系统异常","details":"Index 1 out of bounds for length 1","returnStructureFix":true}
        if let Ok(error) = serde_json::from_str::<ErrorDetails>(&body) {
            if !error.success {
                anyhow::bail!(
                    "Sync users endpoint returned error: {}, code: {}, details: {}",
                    error.msg,
                    error.code,
                    error.details
                );
            }
        }

        let items: Vec<SyncUserItem> = serde_json::from_str(&body)
            .map_err(|e| {
                tracing::error!(
                    "Failed to parse sync users response. Body: '{}', Error: {}",
                    body,
                    e
                );
                e
            })
            .with_context(|| format!("Body: {body}"))
            .context("Failed to parse sync users response")?;

        let users: Vec<UserInfo> = items
            .into_iter()
            .filter_map(|item| {
                let username = item
                    .user_name
                    .or(item.user_display_name.clone())
                    .filter(|s| !s.trim().is_empty())?;
                let email = item.email;
                let roles: Vec<String> = item
                    .roles
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|r| r.role_name)
                    .collect();
                Some(UserInfo {
                    username,
                    email,
                    first_name: None,
                    last_name: None,
                    roles,
                })
            })
            .collect();

        tracing::info!("Fetched {} users from custom OAuth provider", users.len());

        Ok(users)
    }

    /// Refresh access token using refresh token.
    /// Returns (new_access_token, new_refresh_token, expires_in_seconds)
    pub async fn refresh_access_token(
        &self,
        refresh_token: &str,
    ) -> Result<(String, Option<String>, Option<i64>)> {
        self.plain.refresh_access_token(refresh_token).await
    }
}

/// Internal helper that performs the standard OAuth 2.0 flow using the
/// `oauth.custom` endpoints/credentials.
#[derive(Clone)]
struct PlainDelegate {
    client_id: String,
    client_secret: String,
    authorize_url: String,
    token_url: String,
    #[allow(dead_code)]
    login_url: Option<String>,
    profile_url: String,
    redirect_uri: String,
    http_client: reqwest::Client,
}

impl PlainDelegate {
    fn new(config: &OAuthConfig, http_client: reqwest::Client) -> Self {
        Self {
            client_id: config.custom.client_id.clone(),
            client_secret: config.custom.client_secret.clone(),
            authorize_url: config.custom.authorize_url.clone(),
            token_url: config.custom.token_url.clone(),
            login_url: config.custom.login_url.clone(),
            profile_url: config.custom.profile_url.clone(),
            redirect_uri: config.custom.redirect_uri.clone(),
            http_client,
        }
    }

    fn generate_auth_url(&self) -> AuthorizationRequest {
        use rand::Rng;

        let csrf_token: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();

        let auth_url = format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&state={}",
            self.authorize_url,
            urlencoding::encode(&self.client_id),
            urlencoding::encode(&self.redirect_uri),
            urlencoding::encode(&csrf_token)
        );

        AuthorizationRequest {
            auth_url,
            csrf_token,
        }
    }

    async fn exchange_code(&self, code: &str) -> Result<(String, Option<String>, Option<i64>)> {
        tracing::debug!("Exchanging authorization code for access token (custom)");

        let body = serde_json::json!({
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri
        });

        let response = self
            .http_client
            .post(&self.token_url)
            .json(&body)
            .send()
            .await
            .context("Failed to send token request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Token endpoint returned error: {} - {}", status, body);
        }

        let response_text = response
            .text()
            .await
            .context("Failed to read token response body")?;

        tracing::debug!("Token response body: {}", response_text);

        let token_response: TokenResponse =
            serde_json::from_str(&response_text).context("Failed to parse token response")?;

        #[cfg(debug_assertions)]
        tracing::debug!("Token response: {:?}", token_response);

        let access_token = token_response.access_token();
        let refresh_token = token_response.refresh_token();
        let expires_in = token_response.expires_in().or(Some(7200)); // default 2h

        Ok((access_token, refresh_token, expires_in))
    }

    /// Fetch user info (standard with custom-compatible parsing).
    async fn fetch_user_info(&self, access_token: &str) -> Result<UserInfo> {
        tracing::debug!("Fetching user profile from custom OAuth provider");

        let response = self
            .http_client
            .get(&self.profile_url)
            .query(&[("access_token", access_token)])
            .send()
            .await
            .context("Failed to fetch user profile")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Profile endpoint returned error: {} - {}", status, body);
        }

        let profile: ProfileResponse = response
            .json()
            .await
            .context("Failed to parse profile response")?;
        tracing::debug!("Got profile: {profile:#?}");

        let username = profile
            .username
            .or(profile.preferred_username)
            .or_else(|| profile.sub.as_ref().and_then(|s| s.split('@').next().map(|s| s.to_string())))
            .or(profile.name)
            .context(
                "No username field found in profile response (tried: username, preferred_username, sub, name)",
            )?;

        let email = profile.email;
        let first_name = profile.given_name;
        let last_name = profile.family_name;

        let roles: Vec<String> = profile
            .attributes
            .roles
            .iter()
            .map(|r| r.role_name.clone())
            .collect();

        Ok(UserInfo {
            username,
            email,
            first_name,
            last_name,
            roles,
        })
    }

    async fn refresh_access_token(
        &self,
        refresh_token: &str,
    ) -> Result<(String, Option<String>, Option<i64>)> {
        tracing::debug!("Refreshing access token using refresh token");

        let body = serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        });

        let response = self
            .http_client
            .post(&self.token_url)
            .json(&body)
            .send()
            .await
            .context("Failed to send refresh token request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "Token refresh endpoint returned error: {} - {}",
                status,
                body
            );
        }

        let response_text = response
            .text()
            .await
            .context("Failed to read refresh token response body")?;

        tracing::debug!("Refresh token response body: {}", response_text);

        let token_response: TokenResponse = serde_json::from_str(&response_text)
            .context("Failed to parse refresh token response")?;

        tracing::debug!("Refresh token response: {:?}", token_response);

        let new_access_token = token_response.access_token();
        let new_refresh_token = token_response.refresh_token();
        let expires_in = token_response.expires_in().or(Some(7200));

        Ok((new_access_token, new_refresh_token, expires_in))
    }
}
