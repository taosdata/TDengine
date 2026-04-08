// OAuth authentication middleware
// This module provides authentication middleware that supports both:
// 1. Traditional HTTP Basic Authentication
// 2. OAuth Bearer token authentication

use std::borrow::Cow;

use super::{client::OAuthClientEnum, session::SessionManager};
use actix_web::{cookie::Cookie, dev::ServiceRequest, web};
use http::header::AUTHORIZATION;
use http_auth_basic::Credentials;

#[derive(Clone)]
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
#[derive(Clone)]
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

/// Static asset file extensions that carry no session semantics.
/// Requests for these paths skip the DB lookup entirely.
const STATIC_EXTENSIONS: &[&str] = &[
    "js", "css", "png", "jpg", "jpeg", "gif", "svg", "ico", "webp", "woff", "woff2", "ttf", "eot",
    "map", "txt", "json", "html", "htm",
];

/// Returns `true` if the request path points to a static asset that does not
/// need a session renewal check (e.g. JS/CSS/image files).
fn is_static_asset(path: &str) -> bool {
    path.rsplit('.')
        .next()
        .is_some_and(|ext| STATIC_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()))
}

/// Extract the session ID from a `ServiceRequest` (cookie first, then Bearer header).
/// Only returns IDs that look like taosx sessions (prefix "xt-").
fn extract_session_id_for_renewal(req: &ServiceRequest) -> Option<String> {
    req.cookie("session_id")
        .map(|c| c.value().to_string())
        .or_else(|| {
            req.headers()
                .get(AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.strip_prefix("Bearer "))
                .filter(|v| v.starts_with("xt-"))
                .map(|s| s.to_string())
        })
}

/// Actix-web middleware that renews the server-side session TTL on every request
/// and re-issues the `session_id` browser-session cookie when renewal fires.
///
/// When OAuth is enabled the session TTL is extended by calling
/// `verify_session_with_renewal`, which applies the 5-minute renewal interval
/// guard internally so that the database is only written when enough time has
/// elapsed.  When the server-side TTL is renewed the `session_id` cookie is
/// re-sent as a browser-session cookie (no `Max-Age`) — matching how it was
/// originally set at login — so the browser keeps it alive for the new session
/// window without introducing a hard expiry.
///
/// For all other auth modes (Basic, no session) this middleware is a no-op
/// pass-through.
pub async fn session_renewal_middleware(
    req: ServiceRequest,
    next: actix_web::middleware::Next<impl actix_web::body::MessageBody + 'static>,
) -> Result<actix_web::dev::ServiceResponse<impl actix_web::body::MessageBody>, actix_web::Error> {
    let path = req.path().to_owned();

    // Skip DB lookup entirely for static assets — they are fetched frequently
    // and carry no session semantics.
    if is_static_asset(&path) {
        return next.call(req).await;
    }

    // Determine whether the server-side TTL was renewed so we can refresh the cookie.
    let renewed_sid: Option<String> = match extract_session_id_for_renewal(&req) {
        None => {
            tracing::trace!(
                path,
                "Session renewal is skipped because no session ID was found in the request."
            );
            None
        }
        Some(sid) => match req.app_data::<web::Data<SessionManager>>() {
            None => {
                tracing::debug!(path, session_id = %sid, "Session renewal is skipped because OAuth is not enabled.");
                None
            }
            Some(mgr) => {
                tracing::trace!(path, session_id = %sid, "Initiating session verification for renewal.");
                match mgr.verify_session_with_renewal(&sid).await {
                    Ok((Some(_), true)) => {
                        tracing::debug!(path, session_id = %sid, "Session TTL renewed successfully, triggering browser cookie refresh.");
                        Some(sid)
                    }
                    Ok((Some(_), false)) => {
                        tracing::trace!(path, session_id = %sid, "Session verified successfully but renewal is not required at this time.");
                        None
                    }
                    Ok((None, _)) => {
                        tracing::trace!(path, session_id = %sid, "Session verification completed with no active session found (may be expired or missing).");
                        None
                    }
                    Err(e) => {
                        tracing::warn!(path, session_id = %sid, "Failed to verify session for renewal: {e:#}");
                        None
                    }
                }
            }
        },
    };

    let mut res = next.call(req).await?;

    // Re-issue the session_id cookie as a browser-session cookie (no Max-Age)
    // so the browser keeps it alive for the new server-side window.
    if let Some(sid) = renewed_sid {
        let cookie = Cookie::build("session_id", sid)
            .path("/")
            .http_only(true)
            .same_site(actix_web::cookie::SameSite::Lax)
            .finish();
        res.response_mut().add_cookie(&cookie).ok();
    }

    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test::TestRequest;

    // ── is_static_asset ────────────────────────────────────────────────────

    #[test]
    fn test_is_static_asset_js() {
        assert!(is_static_asset("/app/main.js"));
    }

    #[test]
    fn test_is_static_asset_css() {
        assert!(is_static_asset("/assets/style.css"));
    }

    #[test]
    fn test_is_static_asset_png() {
        assert!(is_static_asset("/images/logo.png"));
    }

    #[test]
    fn test_is_static_asset_woff2() {
        assert!(is_static_asset("/fonts/inter.woff2"));
    }

    #[test]
    fn test_is_static_asset_map() {
        assert!(is_static_asset("/dist/app.js.map"));
    }

    #[test]
    fn test_is_static_asset_uppercase_extension() {
        // Extension comparison must be case-insensitive.
        assert!(is_static_asset("/images/photo.PNG"));
        assert!(is_static_asset("/app/bundle.JS"));
    }

    #[test]
    fn test_is_static_asset_api_path() {
        assert!(!is_static_asset("/api/x/jobs"));
    }

    #[test]
    fn test_is_static_asset_no_extension() {
        assert!(!is_static_asset("/api/auth/login"));
        assert!(!is_static_asset("/health"));
    }

    #[test]
    fn test_is_static_asset_dot_in_segment_not_extension() {
        // "/v1.0/status" – the last segment has no dot, so this is not a static asset.
        assert!(!is_static_asset("/v1.0/status"));
    }

    // ── extract_session_id_for_renewal ─────────────────────────────────────

    #[test]
    fn test_extract_session_id_bearer_with_xt_prefix() {
        let req = TestRequest::default()
            .insert_header((AUTHORIZATION, "Bearer xt-abc123"))
            .to_srv_request();
        assert_eq!(
            extract_session_id_for_renewal(&req),
            Some("xt-abc123".to_string())
        );
    }

    #[test]
    fn test_extract_session_id_bearer_without_xt_prefix() {
        // Only `xt-` prefixed values are returned; plain tokens are ignored.
        let req = TestRequest::default()
            .insert_header((AUTHORIZATION, "Bearer some-other-token"))
            .to_srv_request();
        assert_eq!(extract_session_id_for_renewal(&req), None);
    }

    #[test]
    fn test_extract_session_id_cookie_takes_precedence_over_bearer() {
        // Cookie value is returned first, regardless of any Bearer header.
        let req = TestRequest::default()
            .cookie(actix_web::cookie::Cookie::new(
                "session_id",
                "xt-from-cookie",
            ))
            .insert_header((AUTHORIZATION, "Bearer xt-from-header"))
            .to_srv_request();
        assert_eq!(
            extract_session_id_for_renewal(&req),
            Some("xt-from-cookie".to_string())
        );
    }

    #[test]
    fn test_extract_session_id_cookie_without_xt_prefix() {
        // Cookie values are returned as-is (no prefix filter for cookies).
        let req = TestRequest::default()
            .cookie(actix_web::cookie::Cookie::new(
                "session_id",
                "legacy-uuid-value",
            ))
            .to_srv_request();
        assert_eq!(
            extract_session_id_for_renewal(&req),
            Some("legacy-uuid-value".to_string())
        );
    }

    #[test]
    fn test_extract_session_id_no_auth() {
        let req = TestRequest::default().to_srv_request();
        assert_eq!(extract_session_id_for_renewal(&req), None);
    }
}
