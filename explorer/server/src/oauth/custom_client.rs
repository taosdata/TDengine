use super::client::UserInfo;
use super::config::OAuthConfig;
use crate::utils::deserialize_non_empty_string;
use anyhow::{Context, Result, anyhow};
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
        if let Ok(error) = serde_json::from_str::<ErrorDetails>(&body)
            && !error.success
        {
            anyhow::bail!(
                "Sync users endpoint returned error: {}, code: {}, details: {}",
                error.msg,
                error.code,
                error.details
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth::config::CustomOAuthConfig;

    #[test]
    fn test_authorization_request_structure() {
        let auth_req = AuthorizationRequest {
            auth_url: "https://example.com/auth".to_string(),
            csrf_token: "csrf_token_123".to_string(),
        };

        assert_eq!(auth_req.auth_url, "https://example.com/auth");
        assert_eq!(auth_req.csrf_token, "csrf_token_123");
    }

    #[test]
    fn test_authorization_request_clone() {
        let auth_req = AuthorizationRequest {
            auth_url: "https://example.com/auth".to_string(),
            csrf_token: "csrf_token_123".to_string(),
        };

        let cloned = auth_req.clone();
        assert_eq!(auth_req.auth_url, cloned.auth_url);
        assert_eq!(auth_req.csrf_token, cloned.csrf_token);
    }

    #[test]
    fn test_authorization_request_debug() {
        let auth_req = AuthorizationRequest {
            auth_url: "https://example.com/auth".to_string(),
            csrf_token: "test_csrf".to_string(),
        };

        let debug_str = format!("{:?}", auth_req);
        assert!(debug_str.contains("AuthorizationRequest"));
        assert!(debug_str.contains("https://example.com/auth"));
    }

    #[test]
    fn test_token_response_direct_deserialization() {
        let json = r#"{
            "access_token": "test_access_token",
            "token_type": "Bearer",
            "refresh_token": "test_refresh_token",
            "expires_in": 3600
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token(), "test_access_token");
        assert_eq!(
            token.refresh_token(),
            Some("test_refresh_token".to_string())
        );
        assert_eq!(token.expires_in(), Some(3600));
    }

    #[test]
    fn test_token_response_direct_minimal() {
        let json = r#"{
            "access_token": "test_token"
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token(), "test_token");
        assert!(token.refresh_token().is_none());
        assert!(token.expires_in().is_none());
    }

    #[test]
    fn test_token_response_wrapped_deserialization() {
        let json = r#"{
            "data": {
                "access_token": "wrapped_token",
                "refresh_token": "wrapped_refresh",
                "expires_in": 7200
            }
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token(), "wrapped_token");
        assert_eq!(token.refresh_token(), Some("wrapped_refresh".to_string()));
        assert_eq!(token.expires_in(), Some(7200));
    }

    #[test]
    fn test_token_response_wrapped_minimal() {
        let json = r#"{
            "data": {
                "access_token": "wrapped_token"
            }
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token(), "wrapped_token");
        assert!(token.refresh_token().is_none());
        assert!(token.expires_in().is_none());
    }

    #[test]
    fn test_token_response_trimming() {
        let json = r#"{
            "access_token": "  token_with_spaces  ",
            "refresh_token": "  refresh_with_spaces  "
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token(), "token_with_spaces");
        assert_eq!(
            token.refresh_token(),
            Some("refresh_with_spaces".to_string())
        );
    }

    #[test]
    fn test_profile_response_standard_fields() {
        let json = r#"{
            "sub": "12345",
            "preferred_username": "testuser",
            "username": "test",
            "name": "Test User",
            "email": "test@example.com",
            "given_name": "Test",
            "family_name": "User"
        }"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(profile.sub, Some("12345".to_string()));
        assert_eq!(profile.preferred_username, Some("testuser".to_string()));
        assert_eq!(profile.username, Some("test".to_string()));
        assert_eq!(profile.name, Some("Test User".to_string()));
        assert_eq!(profile.email, Some("test@example.com".to_string()));
        assert_eq!(profile.given_name, Some("Test".to_string()));
        assert_eq!(profile.family_name, Some("User".to_string()));
    }

    #[test]
    fn test_profile_response_custom_attributes() {
        let json = r#"{
            "username": "testuser",
            "attributes": {
                "token_expired": 1234567890,
                "token_time": 3600,
                "roles": [
                    {"role_name": "admin"},
                    {"role_name": "user"}
                ],
                "orgs": [
                    {"org_name": "org1", "org_path": "/org1"}
                ]
            }
        }"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(profile.username, Some("testuser".to_string()));
        assert_eq!(profile.attributes.token_expired, Some(1234567890));
        assert_eq!(profile.attributes.token_time, Some(3600));
        assert_eq!(profile.attributes.roles.len(), 2);
        assert_eq!(profile.attributes.roles[0].role_name, "admin");
        assert_eq!(profile.attributes.orgs.len(), 1);
        assert_eq!(profile.attributes.orgs[0].org_name, "org1");
    }

    #[test]
    fn test_profile_response_minimal() {
        let json = r#"{}"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert!(profile.sub.is_none());
        assert!(profile.preferred_username.is_none());
        assert!(profile.username.is_none());
        assert!(profile.name.is_none());
        assert!(profile.email.is_none());
    }

    #[test]
    fn test_sync_user_item_full() {
        let json = r#"{
            "user_name": "testuser",
            "email": "test@example.com",
            "user_display_name": "Test User",
            "user_org_path": "/org/path",
            "org": {
                "org_name": "TestOrg",
                "org_display": "Test Organization",
                "org_path": "/org/test"
            },
            "roles": [
                {
                    "role_name": "admin",
                    "role_display_name": "Administrator",
                    "org": {
                        "org_name": "TestOrg"
                    }
                }
            ]
        }"#;

        let user: SyncUserItem = serde_json::from_str(json).unwrap();
        assert_eq!(user.user_name, Some("testuser".to_string()));
        assert_eq!(user.email, Some("test@example.com".to_string()));
        assert_eq!(user.user_display_name, Some("Test User".to_string()));
    }

    #[test]
    fn test_sync_user_item_minimal() {
        let json = r#"{}"#;

        let user: SyncUserItem = serde_json::from_str(json).unwrap();
        assert!(user.user_name.is_none());
        assert!(user.email.is_none());
        assert!(user.user_display_name.is_none());
        assert!(user.roles.is_none());
    }

    #[test]
    fn test_error_details_deserialization() {
        let json = r#"{
            "success": false,
            "code": 401,
            "msg": "Unauthorized",
            "details": "Invalid credentials"
        }"#;

        let error: ErrorDetails = serde_json::from_str(json).unwrap();
        assert!(!error.success);
        assert_eq!(error.code, 401);
        assert_eq!(error.msg, "Unauthorized");
        assert_eq!(error.details, "Invalid credentials");
    }

    #[test]
    fn test_login_success_deserialization() {
        let json = r#"{
            "success": true,
            "code": 200,
            "data": {
                "access_token": "test_token_123"
            }
        }"#;

        let login: LoginSuccess = serde_json::from_str(json).unwrap();
        assert!(login.success);
        assert_eq!(login.code, 200);
        assert_eq!(login.data.access_token, "test_token_123");
    }

    #[test]
    fn test_login_response_success() {
        let json = r#"{
            "success": true,
            "code": 200,
            "data": {
                "access_token": "test_token"
            }
        }"#;

        let response: LoginResponse = serde_json::from_str(json).unwrap();
        match response {
            LoginResponse::Success(success) => {
                assert!(success.success);
                assert_eq!(success.code, 200);
            }
            LoginResponse::Error(_) => panic!("Expected success response"),
        }
    }

    #[test]
    fn test_login_response_error() {
        let json = r#"{
            "success": false,
            "code": 401,
            "msg": "Login failed",
            "details": "Invalid password"
        }"#;

        let response: LoginResponse = serde_json::from_str(json).unwrap();
        match response {
            LoginResponse::Success(_) => panic!("Expected error response"),
            LoginResponse::Error(error) => {
                assert!(!error.success);
                assert_eq!(error.code, 401);
                assert_eq!(error.msg, "Login failed");
            }
        }
    }

    #[test]
    fn test_fetch_users_credentials_serialization() {
        let creds = FetchUsersCredentials {
            username: "admin".to_string(),
            password: "secret".to_string(),
        };

        let json = serde_json::to_string(&creds).unwrap();
        assert!(json.contains("admin"));
        assert!(json.contains("secret"));
    }

    #[test]
    fn test_fetch_users_credentials_deserialization() {
        let json = r#"{
            "username": "admin",
            "password": "password123"
        }"#;

        let creds: FetchUsersCredentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.username, "admin");
        assert_eq!(creds.password, "password123");
    }

    #[test]
    fn test_fetch_users_credentials_debug() {
        let creds = FetchUsersCredentials {
            username: "testuser".to_string(),
            password: "testpass".to_string(),
        };

        let debug_str = format!("{:?}", creds);
        assert!(debug_str.contains("FetchUsersCredentials"));
    }

    #[test]
    fn test_custom_oauth_client_new_success() {
        let config = OAuthConfig {
            enabled: true,
            provider: "custom".to_string(),
            custom: CustomOAuthConfig {
                client_id: "test_client".to_string(),
                client_secret: "test_secret".to_string(),
                authorize_url: "https://example.com/oauth/authorize".to_string(),
                token_url: "https://example.com/oauth/token".to_string(),
                profile_url: "https://example.com/api/user".to_string(),
                redirect_uri: "https://app.example.com/callback".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = CustomOAuthClient::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_custom_oauth_client_new_invalid_config() {
        let config = OAuthConfig {
            enabled: true,
            provider: "custom".to_string(),
            // Leave required fields empty to trigger validation error
            ..Default::default()
        };
        let result = CustomOAuthClient::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_oauth_client_clone() {
        let config = OAuthConfig {
            enabled: true,
            provider: "custom".to_string(),
            custom: CustomOAuthConfig {
                client_id: "test_client".to_string(),
                client_secret: "test_secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = CustomOAuthClient::new(config).unwrap();
        let cloned = client.clone();

        // Verify clone works (can't compare directly due to private fields)
        let _ = cloned;
    }

    #[test]
    fn test_role_info_deserialization() {
        let json = r#"{"role_name": "admin"}"#;
        let role: RoleInfo = serde_json::from_str(json).unwrap();
        assert_eq!(role.role_name, "admin");
    }

    #[test]
    fn test_org_info_deserialization() {
        let json = r#"{
            "org_name": "TestOrg",
            "org_path": "/org/test"
        }"#;

        let org: OrgInfo = serde_json::from_str(json).unwrap();
        assert_eq!(org.org_name, "TestOrg");
        assert_eq!(org.org_path, Some("/org/test".to_string()));
    }

    #[test]
    fn test_org_info_without_path() {
        let json = r#"{"org_name": "TestOrg"}"#;
        let org: OrgInfo = serde_json::from_str(json).unwrap();
        assert_eq!(org.org_name, "TestOrg");
        assert!(org.org_path.is_none());
    }

    #[test]
    fn test_sync_user_org_deserialization() {
        let json = r#"{
            "org_name": "TestOrg",
            "org_display": "Test Organization",
            "org_path": "/test/org"
        }"#;

        let org: SyncUserOrg = serde_json::from_str(json).unwrap();
        assert_eq!(org.org_name, Some("TestOrg".to_string()));
        assert_eq!(org.org_display, Some("Test Organization".to_string()));
        assert_eq!(org.org_path, Some("/test/org".to_string()));
    }

    #[test]
    fn test_sync_user_role_deserialization() {
        let json = r#"{
            "role_name": "admin",
            "role_display_name": "Administrator",
            "org": {
                "org_name": "TestOrg"
            }
        }"#;

        let role: SyncUserRole = serde_json::from_str(json).unwrap();
        assert_eq!(role.role_name, Some("admin".to_string()));
        assert_eq!(role.role_display_name, Some("Administrator".to_string()));
        assert!(role.org.is_some());
    }

    #[test]
    fn test_profile_attributes_default() {
        let attrs = ProfileAttributes::default();
        assert!(attrs.token_expired.is_none());
        assert!(attrs.token_time.is_none());
        assert!(attrs.roles.is_empty());
        assert!(attrs.orgs.is_empty());
    }

    #[test]
    fn test_token_response_debug() {
        let json = r#"{"access_token": "test_token"}"#;
        let token: TokenResponse = serde_json::from_str(json).unwrap();
        let debug_str = format!("{:?}", token);
        // TokenResponse is an enum, so debug will show the variant (Direct or Wrapped)
        assert!(debug_str.contains("Direct") || debug_str.contains("Wrapped"));
    }

    #[test]
    fn test_profile_response_debug() {
        let json = r#"{"username": "testuser"}"#;
        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        let debug_str = format!("{:?}", profile);
        assert!(debug_str.contains("ProfileResponse"));
    }

    #[test]
    fn test_token_response_zero_expires() {
        let json = r#"{
            "access_token": "test_token",
            "expires_in": 0
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.expires_in(), Some(0));
    }

    #[test]
    fn test_profile_response_with_additional_fields() {
        let json = r#"{
            "username": "testuser",
            "custom_field": "custom_value",
            "another_field": 123
        }"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(profile.username, Some("testuser".to_string()));
    }

    #[test]
    fn test_sync_user_item_empty_roles() {
        let json = r#"{
            "user_name": "testuser",
            "roles": []
        }"#;

        let user: SyncUserItem = serde_json::from_str(json).unwrap();
        assert_eq!(user.user_name, Some("testuser".to_string()));
        assert!(user.roles.is_some());
        assert!(user.roles.unwrap().is_empty());
    }

    #[test]
    fn test_profile_response_unicode() {
        let json = r#"{
            "username": "用户名",
            "name": "测试用户",
            "email": "test@例え.jp"
        }"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(profile.username, Some("用户名".to_string()));
        assert_eq!(profile.name, Some("测试用户".to_string()));
    }

    #[test]
    fn test_error_details_debug() {
        let json = r#"{
            "success": false,
            "code": 404,
            "msg": "Not found",
            "details": "Resource not found"
        }"#;

        let error: ErrorDetails = serde_json::from_str(json).unwrap();
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("ErrorDetails"));
        assert!(debug_str.contains("Not found"));
    }

    #[test]
    fn test_access_token_deserialization() {
        let json = r#"{"access_token": "token123"}"#;
        let token: AccessToken = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token, "token123");
    }

    #[test]
    fn test_login_data_deserialization() {
        let json = r#"{
            "data": {
                "access_token": "test_token"
            }
        }"#;

        let login_data: LoginData = serde_json::from_str(json).unwrap();
        assert_eq!(login_data.data.access_token, "test_token");
    }

    #[test]
    fn test_token_data_deserialization() {
        let json = r#"{
            "access_token": "token",
            "refresh_token": "refresh",
            "expires_in": 3600
        }"#;

        let data: TokenData = serde_json::from_str(json).unwrap();
        assert_eq!(data.access_token, "token");
        assert_eq!(data.refresh_token, Some("refresh".to_string()));
        assert_eq!(data.expires_in, Some(3600));
    }

    #[test]
    fn test_profile_attributes_with_empty_arrays() {
        let json = r#"{
            "roles": [],
            "orgs": []
        }"#;

        let attrs: ProfileAttributes = serde_json::from_str(json).unwrap();
        assert!(attrs.roles.is_empty());
        assert!(attrs.orgs.is_empty());
    }

    #[test]
    fn test_sync_user_item_debug() {
        let json = r#"{"user_name": "testuser"}"#;
        let user: SyncUserItem = serde_json::from_str(json).unwrap();
        let debug_str = format!("{:?}", user);
        assert!(debug_str.contains("SyncUserItem"));
    }
}
