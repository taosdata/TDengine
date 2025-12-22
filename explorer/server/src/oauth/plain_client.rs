use super::client::UserInfo;
use super::config::OAuthConfig;
use anyhow::{Context, Result};
use serde::Deserialize;

/// OAuth 2.0 client for standard providers (RFC 6749 / 6750)
#[derive(Clone)]
pub struct PlainOAuthClient {
    config: OAuthConfig,
    http_client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct AuthorizationRequest {
    pub auth_url: String,
    pub csrf_token: String,
}

/// Standard OAuth 2.0 token response
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    #[allow(dead_code)]
    token_type: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(flatten)]
    #[allow(dead_code)]
    additional: serde_json::Map<String, serde_json::Value>,
}

/// Standard UserInfo response (extend as needed)
#[derive(Debug, Deserialize)]
struct ProfileResponse {
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
    #[serde(default)]
    roles: Option<Vec<String>>,
    #[serde(flatten)]
    #[allow(dead_code)]
    additional: serde_json::Map<String, serde_json::Value>,
}

impl PlainOAuthClient {
    /// Create a new plain OAuth client
    pub fn new(config: OAuthConfig) -> Result<Self> {
        config.validate()?;

        const USER_AGENT: &str =
            const_format::concatcp!("taos-explorer-server ", crate::build::PKG_VERSION);

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent(USER_AGENT)
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            config,
            http_client,
        })
    }

    /// Generate an authorization URL with CSRF state
    pub fn generate_auth_url(&self) -> AuthorizationRequest {
        use rand::Rng;

        let csrf_token: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();

        let plain = self.config.plain_config();

        let auth_url = format!(
            "{}?response_type=code&client_id={}&redirect_uri={}&state={}",
            plain.authorize_url,
            urlencoding::encode(&plain.client_id),
            urlencoding::encode(&plain.redirect_uri),
            urlencoding::encode(&csrf_token)
        );

        AuthorizationRequest {
            auth_url,
            csrf_token,
        }
    }

    /// Exchange authorization code for access/refresh tokens
    pub async fn exchange_code(&self, code: &str) -> Result<(String, Option<String>, Option<i64>)> {
        tracing::debug!("Exchanging authorization code for access token (plain)");

        let plain = self.config.plain_config();

        let body = serde_json::json!({
            "client_id": plain.client_id,
            "client_secret": plain.client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": plain.redirect_uri,
        });

        let resp = self
            .http_client
            .post(&plain.token_url)
            .json(&body)
            .send()
            .await
            .context("Failed to send token request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Token endpoint returned error: {} - {}", status, body);
        }

        let text = resp
            .text()
            .await
            .context("Failed to read token response body")?;
        #[cfg(debug_assertions)]
        tracing::debug!("Token response body (plain): {}", text);

        let token: TokenResponse = match serde_json::from_str(&text) {
            Ok(token) => token,
            Err(json_parse_error) => {
                let token: actix_web::web::Query<TokenResponse> =
                    actix_web::web::Query::from_query(&text)
                    .map_err(|query_parse_error| {
                        anyhow::anyhow!(
                            "Failed to parse token response as JSON or query parameters: 1. JSON: {:#}, 2. Query: {:#}",
                            json_parse_error,
                            query_parse_error
                        )
                    })?;
                token.into_inner()
            }
        };

        let access_token = token.access_token.trim().to_string();
        let refresh_token = token.refresh_token.map(|t| t.trim().to_string());
        let expires_in = token.expires_in;

        Ok((access_token, refresh_token, expires_in))
    }

    /// Fetch user info from the provider using Bearer token
    pub async fn fetch_user_info(&self, access_token: &str) -> Result<UserInfo> {
        tracing::debug!("Fetching user info (plain)");

        let plain = self.config.plain_config();

        let resp = self
            .http_client
            .get(&plain.profile_url)
            .bearer_auth(access_token)
            .send()
            .await
            .context("Failed to fetch user profile")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Profile endpoint returned error: {} - {}", status, body);
        }

        let profile: ProfileResponse = resp
            .json()
            .await
            .context("Failed to parse profile response")?;
        tracing::debug!("Profile response (plain): {profile:#?}");

        let username = profile
            .username
            .or(profile.preferred_username)
            .or_else(|| {
                profile
                    .sub
                    .as_ref()
                    .and_then(|s| s.split('@').next().map(|p| p.to_string()))
            })
            .or(profile.name)
            .context("No username field found in profile response")?;

        let email = profile.email;
        let first_name = profile.given_name;
        let last_name = profile.family_name;
        let roles = profile.roles.unwrap_or_default();

        Ok(UserInfo {
            username,
            email,
            first_name,
            last_name,
            roles,
        })
    }

    /// Refresh access token using the refresh token
    pub async fn refresh_access_token(
        &self,
        refresh_token: &str,
    ) -> Result<(String, Option<String>, Option<i64>)> {
        tracing::debug!("Refreshing access token (plain)");

        let plain = self.config.plain_config();

        let body = serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": plain.client_id,
            "client_secret": plain.client_secret,
        });

        let resp = self
            .http_client
            .post(&plain.token_url)
            .json(&body)
            .send()
            .await
            .context("Failed to send refresh token request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Refresh token endpoint returned error: {} - {}",
                status,
                body
            );
        }

        let text = resp
            .text()
            .await
            .context("Failed to read refresh token response body")?;
        tracing::debug!("Refresh token response body (plain): {}", text);

        let token: TokenResponse =
            serde_json::from_str(&text).context("Failed to parse refresh token response")?;

        let access_token = token.access_token.trim().to_string();
        let new_refresh_token = token.refresh_token.map(|t| t.trim().to_string());
        let expires_in = token.expires_in;

        Ok((access_token, new_refresh_token, expires_in))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth::config::{CustomOAuthConfig, PlainOAuthConfig};

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
    fn test_plain_oauth_client_new_success() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "test_client".to_string(),
                client_secret: "test_secret".to_string(),
                authorize_url: "https://example.com/oauth/authorize".to_string(),
                token_url: "https://example.com/oauth/token".to_string(),
                profile_url: "https://example.com/api/user".to_string(),
                redirect_uri: "https://app.example.com/callback".to_string(),
            },
            ..Default::default()
        };

        let result = PlainOAuthClient::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_plain_oauth_client_new_invalid_config() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            // Leave required fields empty to trigger validation error
            ..Default::default()
        };
        let result = PlainOAuthClient::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_plain_oauth_client_clone() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "test_client".to_string(),
                client_secret: "test_secret".to_string(),
                authorize_url: "https://example.com/oauth/authorize".to_string(),
                token_url: "https://example.com/oauth/token".to_string(),
                profile_url: "https://example.com/api/user".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = PlainOAuthClient::new(config).unwrap();
        let cloned = client.clone();

        // Verify clone works (can't compare directly due to private fields)
        let _ = cloned;
    }

    #[test]
    fn test_generate_auth_url_format() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "test_client".to_string(),
                client_secret: "test_secret".to_string(),
                authorize_url: "https://example.com/oauth/authorize".to_string(),
                token_url: "https://example.com/oauth/token".to_string(),
                profile_url: "https://example.com/api/user".to_string(),
                redirect_uri: "https://app.example.com/callback".to_string(),
            },
            ..Default::default()
        };

        let client = PlainOAuthClient::new(config).unwrap();
        let auth_req = client.generate_auth_url();

        assert!(auth_req
            .auth_url
            .starts_with("https://example.com/oauth/authorize"));
        assert!(auth_req.auth_url.contains("response_type=code"));
        assert!(auth_req.auth_url.contains("client_id="));
        assert!(auth_req.auth_url.contains("redirect_uri="));
        assert!(auth_req.auth_url.contains("state="));
        assert_eq!(auth_req.csrf_token.len(), 32);
    }

    #[test]
    fn test_generate_auth_url_unique_csrf() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "test_client".to_string(),
                client_secret: "test_secret".to_string(),
                authorize_url: "https://example.com/oauth/authorize".to_string(),
                token_url: "https://example.com/oauth/token".to_string(),
                profile_url: "https://example.com/api/user".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = PlainOAuthClient::new(config).unwrap();
        let auth_req1 = client.generate_auth_url();
        let auth_req2 = client.generate_auth_url();

        // CSRF tokens should be unique
        assert_ne!(auth_req1.csrf_token, auth_req2.csrf_token);
        assert_ne!(auth_req1.auth_url, auth_req2.auth_url);
    }

    #[test]
    fn test_generate_auth_url_url_encoding() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "test client with spaces".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                redirect_uri: "https://app.com/callback?param=value".to_string(),
            },
            ..Default::default()
        };

        let client = PlainOAuthClient::new(config).unwrap();
        let auth_req = client.generate_auth_url();

        // URL should be properly encoded
        assert!(auth_req
            .auth_url
            .contains("client_id=test%20client%20with%20spaces"));
    }

    #[test]
    fn test_csrf_token_alphanumeric() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "client".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = PlainOAuthClient::new(config).unwrap();
        let auth_req = client.generate_auth_url();

        // CSRF token should only contain alphanumeric characters
        assert!(auth_req
            .csrf_token
            .chars()
            .all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_token_response_deserialization() {
        let json = r#"{
            "access_token": "test_access_token",
            "token_type": "Bearer",
            "refresh_token": "test_refresh_token",
            "expires_in": 3600
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token, "test_access_token");
        assert_eq!(token.token_type, Some("Bearer".to_string()));
        assert_eq!(token.refresh_token, Some("test_refresh_token".to_string()));
        assert_eq!(token.expires_in, Some(3600));
    }

    #[test]
    fn test_token_response_deserialization_minimal() {
        let json = r#"{
            "access_token": "test_token"
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token, "test_token");
        assert!(token.token_type.is_none());
        assert!(token.refresh_token.is_none());
        assert!(token.expires_in.is_none());
    }

    #[test]
    fn test_token_response_with_additional_fields() {
        let json = r#"{
            "access_token": "test_token",
            "refresh_token": "refresh",
            "expires_in": 7200,
            "custom_field": "custom_value",
            "another_field": 123
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token, "test_token");
        assert_eq!(token.refresh_token, Some("refresh".to_string()));
        assert_eq!(token.expires_in, Some(7200));
    }

    #[test]
    fn test_profile_response_deserialization_full() {
        let json = r#"{
            "sub": "12345",
            "preferred_username": "testuser",
            "username": "test",
            "name": "Test User",
            "email": "test@example.com",
            "given_name": "Test",
            "family_name": "User",
            "roles": ["admin", "user"]
        }"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(profile.sub, Some("12345".to_string()));
        assert_eq!(profile.preferred_username, Some("testuser".to_string()));
        assert_eq!(profile.username, Some("test".to_string()));
        assert_eq!(profile.name, Some("Test User".to_string()));
        assert_eq!(profile.email, Some("test@example.com".to_string()));
        assert_eq!(profile.given_name, Some("Test".to_string()));
        assert_eq!(profile.family_name, Some("User".to_string()));
        assert_eq!(
            profile.roles,
            Some(vec!["admin".to_string(), "user".to_string()])
        );
    }

    #[test]
    fn test_profile_response_deserialization_minimal() {
        let json = r#"{}"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert!(profile.sub.is_none());
        assert!(profile.preferred_username.is_none());
        assert!(profile.username.is_none());
        assert!(profile.name.is_none());
        assert!(profile.email.is_none());
        assert!(profile.given_name.is_none());
        assert!(profile.family_name.is_none());
        assert!(profile.roles.is_none());
    }

    #[test]
    fn test_profile_response_with_additional_fields() {
        let json = r#"{
            "sub": "12345",
            "username": "testuser",
            "custom_field": "custom_value",
            "another_field": {"nested": "value"}
        }"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(profile.sub, Some("12345".to_string()));
        assert_eq!(profile.username, Some("testuser".to_string()));
    }

    #[test]
    fn test_profile_response_debug() {
        let profile = ProfileResponse {
            sub: Some("12345".to_string()),
            preferred_username: Some("testuser".to_string()),
            username: None,
            name: None,
            email: Some("test@example.com".to_string()),
            given_name: None,
            family_name: None,
            roles: None,
            additional: serde_json::Map::new(),
        };

        let debug_str = format!("{:?}", profile);
        assert!(debug_str.contains("ProfileResponse"));
        assert!(debug_str.contains("12345"));
        assert!(debug_str.contains("testuser"));
    }

    #[test]
    fn test_token_response_debug() {
        let token = TokenResponse {
            access_token: "test_token".to_string(),
            token_type: Some("Bearer".to_string()),
            refresh_token: Some("refresh".to_string()),
            expires_in: Some(3600),
            additional: serde_json::Map::new(),
        };

        let debug_str = format!("{:?}", token);
        assert!(debug_str.contains("TokenResponse"));
        assert!(debug_str.contains("test_token"));
    }

    #[test]
    fn test_client_with_fallback_config() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            // Set custom config instead of plain
            custom: CustomOAuthConfig {
                client_id: "custom_client".to_string(),
                client_secret: "custom_secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let result = PlainOAuthClient::new(config);
        // Should succeed using fallback to custom config
        assert!(result.is_ok());
    }

    #[test]
    fn test_auth_url_with_special_redirect_uri() {
        let config = OAuthConfig {
            enabled: true,
            provider: "plain".to_string(),
            plain: PlainOAuthConfig {
                client_id: "client".to_string(),
                client_secret: "secret".to_string(),
                authorize_url: "https://example.com/auth".to_string(),
                token_url: "https://example.com/token".to_string(),
                profile_url: "https://example.com/profile".to_string(),
                redirect_uri: "https://app.com/callback?mode=oauth".to_string(),
            },
            ..Default::default()
        };

        let client = PlainOAuthClient::new(config).unwrap();
        let auth_req = client.generate_auth_url();

        // Should properly encode the redirect_uri with query params
        assert!(auth_req.auth_url.contains("redirect_uri="));
    }

    #[test]
    fn test_profile_response_empty_roles() {
        let json = r#"{
            "username": "testuser",
            "roles": []
        }"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(profile.username, Some("testuser".to_string()));
        assert_eq!(profile.roles, Some(vec![]));
    }

    #[test]
    fn test_token_response_zero_expires_in() {
        let json = r#"{
            "access_token": "test_token",
            "expires_in": 0
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token, "test_token");
        assert_eq!(token.expires_in, Some(0));
    }

    #[test]
    fn test_token_response_negative_expires_in() {
        let json = r#"{
            "access_token": "test_token",
            "expires_in": -1
        }"#;

        let token: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(token.access_token, "test_token");
        assert_eq!(token.expires_in, Some(-1));
    }

    #[test]
    fn test_profile_response_unicode_names() {
        let json = r#"{
            "username": "用户名",
            "name": "测试用户",
            "given_name": "测试",
            "family_name": "用户",
            "email": "test@例え.jp"
        }"#;

        let profile: ProfileResponse = serde_json::from_str(json).unwrap();
        assert_eq!(profile.username, Some("用户名".to_string()));
        assert_eq!(profile.name, Some("测试用户".to_string()));
        assert_eq!(profile.given_name, Some("测试".to_string()));
        assert_eq!(profile.family_name, Some("用户".to_string()));
    }
}
