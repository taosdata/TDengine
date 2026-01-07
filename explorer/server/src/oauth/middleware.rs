// OAuth authentication middleware
// This module provides authentication middleware that supports both:
// 1. Traditional HTTP Basic Authentication
// 2. OAuth Bearer token authentication

use std::borrow::Cow;

use super::{client::OAuthClientEnum, session::SessionManager};
use actix_web::web;
use http::header::AUTHORIZATION;
use http_auth_basic::Credentials;

pub enum AuthType {
    Basic,
    Bearer,
}
impl AuthType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthType::Basic => "Basic",
            AuthType::Bearer => "Bearer",
        }
    }
}
/// Authentication result containing username and password for TDengine connection
pub struct TsdbCredential {
    pub auth_type: AuthType,
    pub username: String,
    pub password: String,
}

impl TsdbCredential {
    pub fn basic(username: String, password: String) -> Self {
        Self {
            auth_type: AuthType::Basic,
            username,
            password,
        }
    }
}

impl std::fmt::Display for TsdbCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TsdbCredential {{ username: {}, password: **** }}",
            self.username
        )
    }
}

/// Extract authentication information from Authorization header
/// Returns AuthResult if authenticated, None if no auth header, or Error
pub async fn extract_auth(
    auth_header: Option<&str>,
    session_manager: Option<&SessionManager>,
    oauth_client: Option<&OAuthClientEnum>,
) -> Result<Option<TsdbCredential>, String> {
    let auth = match auth_header {
        Some(a) => a,
        None => return Ok(None),
    };

    if auth.starts_with("Basic ") {
        // Handle HTTP Basic Authentication
        match Credentials::from_header(auth.to_string()) {
            Ok(creds) => Ok(Some(TsdbCredential {
                auth_type: AuthType::Basic,
                username: creds.user_id,
                password: creds.password,
            })),
            Err(e) => Err(format!("Invalid Basic Auth credentials: {}", e)),
        }
    } else {
        let token = auth.trim_start_matches("Bearer ");

        let session_mgr = match session_manager {
            Some(mgr) => mgr,
            None => {
                tracing::warn!("Bearer token provided but OAuth is not enabled");
                return Err("OAuth is not enabled".to_string());
            }
        };

        // Verify session token
        match session_mgr.verify_session(token).await {
            Ok(Some(mut session)) => {
                // Check if access token is about to expire and refresh it
                if session_mgr.is_access_token_expiring_soon(&session) {
                    if let Some(refresh_token) = &session.details.refresh_token {
                        if let Some(client) = oauth_client {
                            tracing::info!(
                                "Access token expiring soon for session {}, attempting refresh",
                                token
                            );

                            let refresh_result = match client {
                                OAuthClientEnum::Oidc(oidc_client) => {
                                    oidc_client.refresh_access_token(refresh_token).await
                                }
                                OAuthClientEnum::Plain(plain_client) => {
                                    plain_client.refresh_access_token(refresh_token).await
                                }
                                OAuthClientEnum::Custom(custom_client) => {
                                    custom_client.refresh_access_token(refresh_token).await
                                }
                            };

                            match refresh_result {
                                Ok((new_access_token, new_refresh_token, expires_in)) => {
                                    // Update session with new tokens
                                    if let Err(e) = session_mgr
                                        .refresh_session_token(
                                            token,
                                            &new_access_token,
                                            new_refresh_token.as_deref(),
                                            expires_in,
                                        )
                                        .await
                                    {
                                        tracing::error!("Failed to update session tokens: {:#}", e);
                                    } else {
                                        tracing::info!(
                                            "Successfully refreshed access token for session {}",
                                            token
                                        );
                                        // Update the session object with new token
                                        session.details.access_token = Some(new_access_token);
                                        if let Some(new_refresh) = new_refresh_token {
                                            session.details.refresh_token = Some(new_refresh);
                                        }
                                        if let Some(exp_secs) = expires_in {
                                            session.details.access_token_expires_at = Some(
                                                chrono::Utc::now()
                                                    + chrono::Duration::seconds(exp_secs),
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to refresh access token: {:#}", e);
                                    // Continue with existing token - it might still be valid
                                }
                            }
                        } else {
                            tracing::warn!("Access token expiring but OAuth client not available");
                        }
                    } else {
                        tracing::warn!("Access token expiring but no refresh token available");
                    }
                }

                // Get decrypted TDengine credentials from session
                let password = session_mgr
                    .get_decrypted_tsdb_password(&session)
                    .map_err(|e| format!("Failed to decrypt password: {}", e))?
                    .ok_or_else(|| "No TDengine password in session".to_string())?;

                Ok(Some(TsdbCredential {
                    auth_type: AuthType::Bearer,
                    // Safety: unwrap is safe here because session must have a user when password exists
                    username: session.get_tsdb_username().unwrap().to_string(),
                    password,
                }))
            }
            Ok(None) => Err("Invalid or expired session".to_string()),
            Err(e) => {
                tracing::error!("Failed to verify session: {:#}", e);
                Err("Failed to verify session".to_string())
            }
        }
    }
}

/// Helper function to extract auth from actix-web request
pub async fn extract_auth_from_request(
    req: &actix_web::HttpRequest,
) -> Result<Option<TsdbCredential>, String> {
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .map(Cow::Borrowed)
        .or(req
            .cookie("session_id")
            .map(|c| c.value().to_string())
            .map(Cow::Owned));

    // Try to get SessionManager and OAuthClientEnum from app data
    let session_manager = req
        .app_data::<web::Data<SessionManager>>()
        .map(|data| data.get_ref());

    let oauth_client = req
        .app_data::<web::Data<OAuthClientEnum>>()
        .map(|data| data.get_ref());

    extract_auth(auth_header.as_deref(), session_manager, oauth_client).await
}
/// Helper function to extract session id from actix-web request
pub fn extract_session_id_from_request(req: &actix_web::HttpRequest) -> Option<String> {
    if let Some(session_id) = req.cookie("session_id") {
        return Some(session_id.value().to_string());
    }
    if let Some(header) = req.headers().get(AUTHORIZATION) {
        let s = header.to_str().ok()?;
        if let Some(token) = s.strip_prefix("Bearer ") {
            return Some(token.to_string());
        }
    }
    None
}
