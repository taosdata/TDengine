use super::config::OAuthConfig;
use super::custom_client;
use super::plain_client;
use anyhow::{Context, Result};
use openidconnect::{
    core::{
        CoreAuthenticationFlow, CoreClient, CoreIdTokenClaims, CoreIdTokenVerifier,
        CoreProviderMetadata, CoreUserInfoClaims,
    },
    reqwest::async_http_client,
    AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce, OAuth2TokenResponse,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenResponse,
};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct OidcClient {
    client: CoreClient,
    config: OAuthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub username: String,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AuthorizationRequest {
    pub auth_url: String,
    pub csrf_token: String,
    pub nonce: String,
    pub pkce_verifier: String,
}

trait UserClaimsExtractor {
    fn get(&self, claim: &str) -> Option<String>;
}
impl UserClaimsExtractor for CoreIdTokenClaims {
    fn get(&self, claim: &str) -> Option<String> {
        match claim {
            "sub" => Some(self.subject().as_str().to_string()),
            "preferred_username" => self.preferred_username().map(|n| n.to_string()),
            "name" => self.name().and_then(|n| n.get(None).map(|s| s.to_string())),

            "email" => self.email().map(|e| e.as_str().to_string()),
            "given_name" => self
                .given_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "family_name" => self
                .family_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "middle_name" => self
                .middle_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "nickname" => self
                .nickname()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "first_name" => self
                .given_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "last_name" => self
                .family_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),

            "picture" => self
                .picture()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "locale" => self.locale().map(|n| n.to_string()),
            "zoneinfo" => self.zoneinfo().map(|n| n.to_string()),
            "phone_number" => self.phone_number().map(|n| n.to_string()),
            "address" => self
                .address()
                .and_then(|s| s.formatted.as_ref().map(|addr| addr.to_string())),
            _ => {
                tracing::warn!("Claim '{}' not found in ID token claims", claim);
                None
            }
        }
    }
}
impl UserClaimsExtractor for CoreUserInfoClaims {
    fn get(&self, claim: &str) -> Option<String> {
        match claim {
            "sub" => Some(self.subject().as_str().to_string()),
            "preferred_username" => self.preferred_username().map(|n| n.to_string()),
            "name" => self.name().and_then(|n| n.get(None).map(|s| s.to_string())),

            "email" => self.email().map(|e| e.as_str().to_string()),
            "given_name" => self
                .given_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "family_name" => self
                .family_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "middle_name" => self
                .middle_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "nickname" => self
                .nickname()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "first_name" => self
                .given_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "last_name" => self
                .family_name()
                .and_then(|n| n.get(None).map(|s| s.to_string())),

            "picture" => self
                .picture()
                .and_then(|n| n.get(None).map(|s| s.to_string())),
            "locale" => self.locale().map(|n| n.to_string()),
            "zoneinfo" => self.zoneinfo().map(|n| n.to_string()),
            "phone_number" => self.phone_number().map(|n| n.to_string()),
            "address" => self
                .address()
                .and_then(|s| s.formatted.as_ref().map(|addr| addr.to_string())),
            _ => {
                tracing::warn!("Claim '{}' not found in ID token claims", claim);
                None
            }
        }
    }
}
impl OidcClient {
    /// Create a new OIDC client with automatic discovery
    pub async fn new(config: OAuthConfig) -> Result<Self> {
        config.validate()?;

        // Parse issuer URL
        let issuer_url =
            IssuerUrl::new(config.oidc.issuer_url.clone()).context("Invalid issuer URL")?;

        tracing::info!(
            "Discovering OIDC provider metadata from: {}",
            issuer_url.as_str()
        );

        // Perform OIDC Discovery
        let provider_metadata = CoreProviderMetadata::discover_async(issuer_url, async_http_client)
            .await
            .context("Failed to discover OIDC provider metadata")?;

        tracing::info!(
            "OIDC discovery successful. Issuer: {}",
            provider_metadata.issuer().as_str()
        );

        // Create OIDC client
        let client = CoreClient::from_provider_metadata(
            provider_metadata,
            ClientId::new(config.oidc.client_id.clone()),
            Some(ClientSecret::new(config.oidc.client_secret.clone())),
        )
        .set_redirect_uri(
            RedirectUrl::new(config.oidc.redirect_uri.clone()).context("Invalid redirect URI")?,
        );

        Ok(Self { client, config })
    }

    /// Generate an authorization URL with PKCE and nonce
    pub fn generate_auth_url(&self) -> AuthorizationRequest {
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let mut auth_request = self
            .client
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            .set_pkce_challenge(pkce_challenge);

        // Add configured scopes
        for scope in &self.config.oidc.scopes {
            auth_request = auth_request.add_scope(Scope::new(scope.clone()));
        }

        let (auth_url, csrf_token, nonce) = auth_request.url();

        AuthorizationRequest {
            auth_url: auth_url.to_string(),
            csrf_token: csrf_token.secret().clone(),
            nonce: nonce.secret().clone(),
            pkce_verifier: pkce_verifier.secret().clone(),
        }
    }

    /// Exchange authorization code for tokens
    /// Returns (id_token_claims, access_token, refresh_token, expires_in_seconds)
    pub async fn exchange_code(
        &self,
        code: &str,
        pkce_verifier: &str,
        nonce: &str,
    ) -> Result<(
        CoreIdTokenClaims,
        Option<String>,
        Option<String>,
        Option<i64>,
    )> {
        let code = AuthorizationCode::new(code.to_string());
        let verifier = PkceCodeVerifier::new(pkce_verifier.to_string());
        let expected_nonce = Nonce::new(nonce.to_string());

        tracing::debug!("Exchanging authorization code for tokens");

        // Exchange the code for tokens
        let token_response = self
            .client
            .exchange_code(code)
            .set_pkce_verifier(verifier)
            .request_async(async_http_client)
            .await
            .context("Failed to exchange authorization code for tokens")?;

        // Get the ID token verifier
        let id_token_verifier: CoreIdTokenVerifier = self.client.id_token_verifier();

        // Verify the ID token
        let id_token_claims = token_response
            .id_token()
            .context("No ID token in response")?
            .claims(&id_token_verifier, &expected_nonce)
            .context("Failed to verify ID token")?
            .clone();

        tracing::info!(
            "Successfully verified ID token for subject: {}",
            id_token_claims.subject().as_str()
        );

        // Extract access token and refresh token
        let access_token = token_response.access_token().secret().clone();
        let refresh_token = token_response.refresh_token().map(|t| t.secret().clone());
        let expires_in = token_response.expires_in().map(|d| d.as_secs() as i64);

        Ok((
            id_token_claims,
            Some(access_token),
            refresh_token,
            expires_in,
        ))
    }

    /// Refresh access token using refresh token
    /// Returns (new_access_token, new_refresh_token, expires_in_seconds)
    pub async fn refresh_access_token(
        &self,
        refresh_token: &str,
    ) -> Result<(String, Option<String>, Option<i64>)> {
        let refresh_token = openidconnect::RefreshToken::new(refresh_token.to_string());

        tracing::debug!("Refreshing access token");

        // Exchange refresh token for new access token
        let token_response = self
            .client
            .exchange_refresh_token(&refresh_token)
            .request_async(async_http_client)
            .await
            .context("Failed to refresh access token")?;

        // Extract new tokens
        let new_access_token = token_response.access_token().secret().clone();
        let new_refresh_token = token_response.refresh_token().map(|t| t.secret().clone());
        let expires_in = token_response.expires_in().map(|d| d.as_secs() as i64);

        tracing::info!("Successfully refreshed access token");

        Ok((new_access_token, new_refresh_token, expires_in))
    }

    /// Extract user info from ID token claims
    pub fn extract_user_info_from_claims(&self, claims: &CoreIdTokenClaims) -> Result<UserInfo> {
        let _additional_claims = claims.additional_claims();

        // Extract username
        let username = claims
            .get(&self.config.user_mapping.username)
            .or_else(|| {
                claims
                    .subject()
                    .as_str()
                    .split('@')
                    .next()
                    .map(|s| s.to_string())
            })
            .context("Username claim not found")?
            .to_string();

        // Extract email
        let email = claims
            .email()
            .map(|e| e.as_str().to_string())
            .or_else(|| claims.get(&self.config.user_mapping.email));

        // Extract first name
        let first_name = claims
            .get(&self.config.user_mapping.first_name)
            .map(|s| s.to_string());

        // Extract last name
        let last_name = claims
            .get(&self.config.user_mapping.last_name)
            .map(|s| s.to_string());

        // Extract roles/groups
        // let roles = claims
        //     .get(&self.config.user_mapping.roles)
        //     .unwrap_or_default();

        Ok(UserInfo {
            username,
            email,
            first_name,
            last_name,
            roles: vec![],
            // roles,
        })
    }

    /// Fetch user info from UserInfo endpoint
    #[allow(dead_code)]
    pub async fn fetch_user_info(&self, access_token: &str) -> Result<CoreUserInfoClaims> {
        let access_token = openidconnect::AccessToken::new(access_token.to_string());

        let userinfo_claims = self
            .client
            .user_info(access_token, None)
            .context("UserInfo endpoint not available")?
            .request_async(async_http_client)
            .await
            .context("Failed to fetch user info from UserInfo endpoint")?;

        Ok(userinfo_claims)
    }

    /// Extract user info from UserInfo endpoint response
    #[allow(dead_code)]
    pub fn extract_user_info_from_userinfo(
        &self,
        userinfo: &CoreUserInfoClaims,
    ) -> Result<UserInfo> {
        // let additional_claims = userinfo.additional_claims();

        // Extract username
        let username = userinfo
            .get(&self.config.user_mapping.username)
            .or_else(|| {
                userinfo
                    .subject()
                    .as_str()
                    .split('@')
                    .next()
                    .map(|s| s.to_string())
            })
            .context("Username claim not found")?
            .to_string();

        // Extract email
        let email = userinfo
            .email()
            .map(|e| e.as_str().to_string())
            .or_else(|| {
                userinfo
                    .get(&self.config.user_mapping.email)
                    .map(|s| s.to_string())
            });

        // Extract first name and last name
        let first_name = userinfo
            .get(&self.config.user_mapping.first_name)
            .map(|s| s.to_string());

        let last_name = userinfo
            .get(&self.config.user_mapping.last_name)
            .map(|s| s.to_string());

        // Extract roles
        let roles = userinfo
            .get(&self.config.user_mapping.roles)
            .and_then(|v| {
                v.as_str()
                    .split(",")
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
                    .into()
            })
            .map(|arr| {
                arr.iter()
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default();

        Ok(UserInfo {
            username,
            email,
            first_name,
            last_name,
            roles,
        })
    }
}

/// Unified OAuth client that supports both OIDC and custom OAuth
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum OAuthClientEnum {
    Oidc(OidcClient),
    Plain(plain_client::PlainOAuthClient),
    Custom(custom_client::CustomOAuthClient),
}

impl OAuthClientEnum {
    /// Create OAuth client based on provider configuration
    pub async fn new(config: OAuthConfig) -> Result<Self> {
        match config.provider.as_str() {
            "oidc" => {
                let client = OidcClient::new(config).await?;
                Ok(OAuthClientEnum::Oidc(client))
            }
            "plain" => {
                let client = plain_client::PlainOAuthClient::new(config)?;
                Ok(OAuthClientEnum::Plain(client))
            }
            "custom" => {
                let client = custom_client::CustomOAuthClient::new(config)?;
                Ok(OAuthClientEnum::Custom(client))
            }
            _ => anyhow::bail!("Unsupported OAuth provider: {}", config.provider),
        }
    }
}
