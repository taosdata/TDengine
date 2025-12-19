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
