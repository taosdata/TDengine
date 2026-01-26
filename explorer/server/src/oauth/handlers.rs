use crate::{
    R, RestErrResponse,
    oauth::{
        custom_client::FetchUsersCredentials, middleware::extract_session_id_from_request,
        session::TsdbSyncOptions,
    },
    utils::{
        cbc::{decrypt_cbc_mac_b64, encrypt_cbc_mac_b64},
        deserialize_non_empty_string,
    },
};

use super::{
    client::{OAuthClientEnum, UserInfo},
    config::OAuthConfig,
    session::{OAuthSyncSummary, SessionManager},
};
use actix_web::{
    HttpRequest, HttpResponse, Responder,
    cookie::{Cookie, SameSite},
    web,
};
use awc::cookie::CookieBuilder;
use chrono::{DateTime, Utc};
use http::header::USER_AGENT;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const OAUTH_STATE_COOKIE: &str = "oauth_state";
const OAUTH_NONCE_COOKIE: &str = "oauth_nonce";
const OAUTH_VERIFIER_COOKIE: &str = "oauth_verifier";
const ENCRYPT_KEY: &str = "encrypt_key";
const SESSION_ID_COOKIE: &str = "session_id";
const COOKIE_MAX_AGE: Duration = Duration::from_secs(600); // 10 minutes
const NON_USER_AGENT: &str = "non-user-agent";

#[derive(Serialize)]
pub struct OAuthStatusResponse {
    pub enabled: bool,
    pub support_sync_users: bool,
    pub provider: Option<String>,
    pub provider_display_name: Option<ProviderDisplayName>,
}

#[derive(Serialize, Clone)]
pub struct ProviderDisplayName {
    pub en: String,
    pub zh: String,
}

impl From<&super::config::ProviderDisplayName> for ProviderDisplayName {
    fn from(config: &super::config::ProviderDisplayName) -> Self {
        Self {
            en: config.en.clone(),
            zh: config.zh.clone(),
        }
    }
}

/// GET /api/-/oauth/status
/// Returns whether OAuth is enabled and the provider type
pub async fn oauth_status(config: web::Data<OAuthConfig>) -> impl Responder {
    let response = OAuthStatusResponse {
        enabled: config.enabled,
        support_sync_users: config.enabled
            && config.provider == "custom"
            && config.custom.fetch_users_url.is_some(),
        provider: if config.enabled {
            Some(config.provider.clone())
        } else {
            None
        },
        provider_display_name: if config.enabled {
            Some(ProviderDisplayName::from(&config.provider_display_name))
        } else {
            None
        },
    };

    HttpResponse::Ok().json(response)
}

/// GET /api/-/oauth/authorize
/// Redirects user to OAuth provider for authorization
pub async fn oauth_authorize(
    oauth_client: web::Data<OAuthClientEnum>,
    config: web::Data<OAuthConfig>,
) -> impl Responder {
    if !config.enabled {
        return HttpResponse::NotFound().body("OAuth is not enabled");
    }

    match oauth_client.as_ref() {
        OAuthClientEnum::Oidc(oidc_client) => {
            // Generate authorization request with PKCE and nonce for OIDC
            let auth_req = oidc_client.generate_auth_url();

            tracing::info!(
                "Initiating OIDC authorization flow. Redirecting to: {}",
                auth_req.auth_url
            );

            // Store state, nonce, and verifier in HTTP-only cookies
            let state_cookie = Cookie::build(OAUTH_STATE_COOKIE, auth_req.csrf_token.clone())
                .path("/")
                .max_age(actix_web::cookie::time::Duration::seconds(
                    COOKIE_MAX_AGE.as_secs() as i64,
                ))
                .http_only(true)
                .same_site(SameSite::Lax)
                .finish();

            let nonce_cookie = Cookie::build(OAUTH_NONCE_COOKIE, auth_req.nonce.clone())
                .path("/")
                .max_age(actix_web::cookie::time::Duration::seconds(
                    COOKIE_MAX_AGE.as_secs() as i64,
                ))
                .http_only(true)
                .same_site(SameSite::Lax)
                .finish();

            let verifier_cookie =
                Cookie::build(OAUTH_VERIFIER_COOKIE, auth_req.pkce_verifier.clone())
                    .path("/")
                    .max_age(actix_web::cookie::time::Duration::seconds(
                        COOKIE_MAX_AGE.as_secs() as i64,
                    ))
                    .http_only(true)
                    .same_site(SameSite::Lax)
                    .finish();

            // Redirect to IdP
            HttpResponse::Found()
                .append_header(("Location", auth_req.auth_url))
                .cookie(state_cookie)
                .cookie(nonce_cookie)
                .cookie(verifier_cookie)
                .finish()
        }
        OAuthClientEnum::Plain(plain_client) => {
            // Generate authorization request without PKCE for plain OAuth
            let auth_req = plain_client.generate_auth_url();

            tracing::info!(
                "Initiating plain OAuth authorization flow. Redirecting to: {}",
                auth_req.auth_url
            );

            // Store only state in HTTP-only cookie (no PKCE/nonce for plain OAuth)
            let state_cookie = Cookie::build(OAUTH_STATE_COOKIE, auth_req.csrf_token.clone())
                .path("/")
                .max_age(actix_web::cookie::time::Duration::seconds(
                    COOKIE_MAX_AGE.as_secs() as i64,
                ))
                .http_only(true)
                .same_site(SameSite::Lax)
                .finish();

            // Redirect to plain OAuth provider
            HttpResponse::Found()
                .append_header(("Location", auth_req.auth_url))
                .cookie(state_cookie)
                .finish()
        }
        OAuthClientEnum::Custom(custom_client) => {
            // Generate authorization request without PKCE for custom OAuth
            let auth_req = custom_client.generate_auth_url();

            tracing::info!(
                "Initiating custom OAuth authorization flow. Redirecting to: {}",
                auth_req.auth_url
            );

            // Store only state in HTTP-only cookie (no PKCE/nonce for custom OAuth)
            let state_cookie = Cookie::build(OAUTH_STATE_COOKIE, auth_req.csrf_token.clone())
                .path("/")
                .max_age(actix_web::cookie::time::Duration::seconds(
                    COOKIE_MAX_AGE.as_secs() as i64,
                ))
                .http_only(true)
                .same_site(SameSite::Lax)
                .finish();

            // Redirect to custom OAuth provider
            HttpResponse::Found()
                .append_header(("Location", auth_req.auth_url))
                .cookie(state_cookie)
                .finish()
        }
    }
}

#[derive(Deserialize)]
pub struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

/// GET /api/-/oauth/callback
/// Handles OAuth callback from provider
pub async fn oauth_callback(
    query: web::Query<CallbackQuery>,
    req: HttpRequest,
    oauth_client: web::Data<OAuthClientEnum>,
    session_manager: web::Data<SessionManager>,
    config: web::Data<OAuthConfig>,
) -> impl Responder {
    if !config.enabled {
        return HttpResponse::NotFound().body("OAuth is not enabled");
    }

    // Check for errors from provider
    if let Some(error) = &query.error {
        let description = query
            .error_description
            .as_deref()
            .unwrap_or("Unknown error");
        tracing::error!("OAuth error from provider: {} - {}", error, description);
        return HttpResponse::BadRequest().body(format!("OAuth error: {}", description));
    }

    // Extract code and state from query
    let code = match &query.code {
        Some(c) => c,
        None => {
            return HttpResponse::BadRequest().body("Missing authorization code");
        }
    };
    // Get state from query parameter, or fall back to cookie value for custom OAuth providers
    // that may not echo back the state parameter
    match query.state.as_deref() {
        Some(state) => {
            match req.cookie(OAUTH_STATE_COOKIE) {
                Some(cookie) => {
                    let expected_state = cookie.value();
                    if state == expected_state {
                        tracing::info!("OAuth state matches");
                    } else {
                        tracing::warn!(
                            "OAuth state mismatch: query/fallback='{}', expected='{}'",
                            state,
                            expected_state
                        );
                        return HttpResponse::BadRequest().body("Invalid state parameter");
                    }
                }
                None => {
                    tracing::warn!("OAuth state cookie not found, do not validate csrf token");
                    // return HttpResponse::BadRequest().body("Invalid state: cookie not found");
                }
            }
        }
        None => {
            tracing::info!("State parameter missing in callback URL, no csrf validation proceed");
        }
    };
    // Process callback based on provider type
    let (user_info, access_token, refresh_token, access_token_expires_in) = match oauth_client
        .as_ref()
    {
        OAuthClientEnum::Oidc(oidc_client) => {
            // Extract nonce and verifier from cookies for OIDC
            let nonce = match req.cookie(OAUTH_NONCE_COOKIE) {
                Some(cookie) => cookie.value().to_string(),
                None => {
                    tracing::warn!("OAuth nonce cookie not found");
                    return HttpResponse::BadRequest().body("Invalid request: nonce not found");
                }
            };

            let verifier = match req.cookie(OAUTH_VERIFIER_COOKIE) {
                Some(cookie) => cookie.value().to_string(),
                None => {
                    tracing::warn!("OAuth verifier cookie not found");
                    return HttpResponse::BadRequest().body("Invalid request: verifier not found");
                }
            };

            // Exchange code for tokens with PKCE
            let (id_token_claims, access_token, refresh_token, access_token_expires_in) =
                match oidc_client.exchange_code(code, &verifier, &nonce).await {
                    Ok(tokens) => tokens,
                    Err(e) => {
                        tracing::error!("Failed to exchange authorization code: {:#}", e);
                        return HttpResponse::InternalServerError()
                            .body("Failed to exchange authorization code");
                    }
                };

            // Extract user info from ID token
            let user_info = match oidc_client.extract_user_info_from_claims(&id_token_claims) {
                Ok(info) => info,
                Err(e) => {
                    tracing::error!("Failed to extract user info from ID token: {:#}", e);
                    return HttpResponse::InternalServerError()
                        .body("Failed to extract user information");
                }
            };

            tracing::info!(
                "OIDC login successful for user: {} (email: {:?})",
                user_info.username,
                user_info.email
            );

            (
                user_info,
                access_token,
                refresh_token,
                access_token_expires_in,
            )
        }
        OAuthClientEnum::Plain(plain_client) => {
            // Exchange code for access token and refresh token
            let (access_token, refresh_token, access_token_expires_in) =
                match plain_client.exchange_code(code).await {
                    Ok(tokens) => tokens,
                    Err(e) => {
                        tracing::error!("Failed to exchange authorization code: {:#}", e);
                        return HttpResponse::InternalServerError()
                            .body("Failed to exchange authorization code");
                    }
                };

            // Fetch user info from profile endpoint
            let user_info = match plain_client.fetch_user_info(&access_token).await {
                Ok(info) => info,
                Err(e) => {
                    tracing::error!("Failed to fetch user profile: {:#}", e);
                    return HttpResponse::InternalServerError()
                        .body("Failed to fetch user information");
                }
            };

            tracing::info!(
                "Plain OAuth login successful for user: {} (roles: {}, refresh_token: {})",
                user_info.username,
                user_info.roles.len(),
                refresh_token.is_some()
            );

            (
                user_info,
                Some(access_token),
                refresh_token,
                access_token_expires_in,
            )
        }
        OAuthClientEnum::Custom(custom_client) => {
            // Exchange code for access token and refresh token
            let (access_token, refresh_token, access_token_expires_in) =
                match custom_client.exchange_code(code).await {
                    Ok(tokens) => tokens,
                    Err(e) => {
                        tracing::error!("Failed to exchange authorization code: {:#}", e);
                        return HttpResponse::InternalServerError()
                            .body("Failed to exchange authorization code");
                    }
                };

            // Fetch user info from profile endpoint
            let user_info = match custom_client.fetch_user_info(&access_token).await {
                Ok(info) => info,
                Err(e) => {
                    tracing::error!("Failed to fetch user profile: {:#}", e);
                    return HttpResponse::InternalServerError()
                        .body("Failed to fetch user information");
                }
            };

            tracing::info!(
                "Custom OAuth login successful for user: {} (roles: {}, refresh_token: {})",
                user_info.username,
                user_info.roles.len(),
                refresh_token.is_some()
            );

            (
                user_info,
                Some(access_token),
                refresh_token,
                access_token_expires_in,
            )
        }
    };

    // Create OAuth session (default 8 hours)
    let session = match session_manager
        .create_session(
            config.provider.clone(),
            user_info.username.clone(),
            user_info.email.clone(),
            access_token,
            refresh_token,
            None,                    // id_token - we don't need to store it
            28800,                   // 8 hours session expiration
            access_token_expires_in, // access token expiration from IdP
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to create OAuth session: {:#}", e);
            return HttpResponse::InternalServerError().body("Failed to create session");
        }
    };

    let session_id = session.session_id().to_string();

    tracing::info!("Created OAuth session: {}", session_id);

    // Clear OAuth cookies (state is always present, nonce/verifier only for OIDC)
    let clear_state = Cookie::build(OAUTH_STATE_COOKIE, "")
        .path("/")
        .max_age(actix_web::cookie::time::Duration::seconds(0))
        .finish();
    let clear_nonce = Cookie::build(OAUTH_NONCE_COOKIE, "")
        .path("/")
        .max_age(actix_web::cookie::time::Duration::seconds(0))
        .finish();
    let clear_verifier = Cookie::build(OAUTH_VERIFIER_COOKIE, "")
        .path("/")
        .max_age(actix_web::cookie::time::Duration::seconds(0))
        .finish();

    // Create a HttpOnly, Secure session cookie for session_id to avoid exposing it in URLs
    let session_cookie = Cookie::build("session_id", session_id.clone())
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .max_age(actix_web::cookie::time::Duration::seconds(3600)) // 1 hour
        .finish();
    let user_agent = req
        .headers()
        .get(USER_AGENT)
        .and_then(|ua| ua.to_str().ok())
        .unwrap_or(NON_USER_AGENT);

    let key_b64 = match session_manager
        .derive_client_encrypt_key_with_user_agent(&session_id, user_agent)
        .await
    {
        Ok(key) => key,
        Err(err) => {
            tracing::error!("Failed to derive client encrypt key: {:#}", err);
            return HttpResponse::InternalServerError().body("Failed to derive client encrypt key");
        }
    };
    let encrypt_key = Cookie::build(ENCRYPT_KEY, key_b64)
        .path("/")
        .max_age(actix_web::cookie::time::Duration::seconds(360))
        .finish();

    #[cfg(debug_assertions)]
    let redirect_url = {
        // Redirect to frontend without token in URL
        let fallback_redirect = config.fallback_redirect_uri.clone().unwrap_or_default();
        // User needs to bind TDengine credentials
        format!("{}/login", fallback_redirect)
    };

    #[cfg(not(debug_assertions))]
    let redirect_url = {
        // User needs to bind TDengine credentials
        "/login"
    };

    HttpResponse::Found()
        .append_header(("Location", redirect_url))
        .cookie(clear_state)
        .cookie(clear_nonce)
        .cookie(clear_verifier)
        .cookie(encrypt_key)
        .cookie(session_cookie)
        .finish()
}

/// Request body for binding TDengine credential.
///
/// token: OAuth session token
/// credential: TDengine username and password in the format "username:password"
#[derive(Deserialize)]
pub struct BindRequest {
    username: String,
    credential: String,
}
/// POST /api/-/oauth/bind
/// Binds TDengine credentials to the OAuth session
pub async fn oauth_bind(
    req: HttpRequest,
    body: web::Json<BindRequest>,
    session_manager: web::Data<SessionManager>,
) -> impl Responder {
    let token = if let Some(cookie) = req.cookie(SESSION_ID_COOKIE) {
        cookie.value().to_string()
    } else {
        return HttpResponse::BadRequest().json(RestErrResponse::new("Missing session ID"));
    };

    let user_agent = req
        .headers()
        .get(USER_AGENT)
        .and_then(|ua| ua.to_str().ok())
        .unwrap_or(NON_USER_AGENT);
    let client_encrypt_key = match session_manager
        .derive_client_encrypt_key_with_user_agent(&token, user_agent)
        .await
    {
        Ok(key) => key,
        Err(e) => {
            tracing::error!("Failed to derive client encrypt key: {:#}", e);
            return HttpResponse::InternalServerError()
                .json(RestErrResponse::new("Failed to derive client encrypt key"));
        }
    };

    let credential_decrypted = match decrypt_cbc_mac_b64(&body.credential, &client_encrypt_key) {
        Ok(cred) => cred,
        Err(e) => {
            tracing::error!("Failed to decrypt TDengine credential: {:#}", e);
            return HttpResponse::BadRequest()
                .json(RestErrResponse::new("Failed to decrypt credential"));
        }
    };
    let credential_decrypted = match String::from_utf8(credential_decrypted) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Decrypted credential is not valid password: {:#}", e);
            return HttpResponse::BadRequest()
                .json(RestErrResponse::new("Invalid credential format"));
        }
    };
    let session_id = &token;
    let tdengine_username = &body.username;
    let tdengine_password = &credential_decrypted;
    match session_manager
        .bind_tsdb_credentials(session_id, tdengine_username, tdengine_password)
        .await
    {
        Ok(_) => {
            tracing::info!(
                "Bound TDengine credentials to OAuth session: {}",
                session_id
            );
            HttpResponse::Ok().json(R::success(serde_json::json!({
                "status": "bound"
            })))
        }
        Err(e) => {
            tracing::error!(
                "Failed to bind TDengine credentials to OAuth session {}: {:#}",
                session_id,
                e
            );
            HttpResponse::InternalServerError()
                .json(RestErrResponse::new("Failed to bind credentials"))
        }
    }
}
/// POST /api/-/oauth/logout
/// Logs out the user (clears session)
pub async fn oauth_logout(
    req: HttpRequest,
    session_manager: web::Data<SessionManager>,
) -> impl Responder {
    if let Some(session_id) = extract_session_id_from_request(&req) {
        match session_manager.delete_session(&session_id).await {
            Ok(_) => {
                tracing::info!("OAuth session deleted: {}", session_id);
                return HttpResponse::Ok().json(serde_json::json!({
                    "status": "logged_out"
                }));
            }
            Err(e) => {
                tracing::error!("Failed to delete OAuth session: {e:#}");
            }
        }
    }

    // Return success anyway - the session might not exist
    let clear_session = Cookie::build(SESSION_ID_COOKIE, "")
        .path("/")
        .http_only(true)
        .max_age(actix_web::cookie::time::Duration::seconds(0))
        .finish();
    HttpResponse::Ok()
        .cookie(clear_session)
        .json(serde_json::json!({
            "status": "logged_out"
        }))
}

/// Create self-provided SSO token
pub async fn self_provided_token(
    req: HttpRequest,
    session_manager: web::Data<SessionManager>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let tsdb_credencial = crate::oauth::middleware::extract_auth_from_request(&req).await;
    let expires_in = query
        .get("expires_in")
        .map(|v| v.parse::<i64>().unwrap_or(i64::MAX));
    let redirect_to = query.get("redirect_to");
    match tsdb_credencial {
        Ok(Some(tsdb)) => {
            let token = session_manager
                .create_self_provided_session(&tsdb, expires_in)
                .await;
            match token {
                Ok(token) => {
                    // TODO: Implement redirect_to logic
                    if let Some(redirect_to) = redirect_to {
                        let cookie = CookieBuilder::new("session_id", token.session_id())
                            .http_only(true)
                            .same_site(SameSite::Strict)
                            .path("/")
                            .finish();
                        HttpResponse::Found()
                            .append_header(("Location", redirect_to.as_str()))
                            .cookie(cookie)
                            .finish()
                    } else {
                        HttpResponse::Ok()
                            .append_header(("Content-Type", "application/json"))
                            .json(serde_json::json!({
                                "code": 0,
                                "message": "Token generated successfully",
                                "token": token.session_id()
                            }))
                    }
                }
                Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": format!("Failed to create self-provided token: {:#}", e)
                })),
            }
        }
        _ => HttpResponse::Unauthorized().json(serde_json::json!({
            "error": "Invalid credentials"
        })),
    }
}

pub async fn oauth_me(
    req: HttpRequest,
    session_manager: web::Data<SessionManager>,
) -> HttpResponse {
    let session_id = if let Some(session_id) = extract_session_id_from_request(&req) {
        session_id
    } else {
        return HttpResponse::Unauthorized().json(RestErrResponse::new("Session not found"));
    };
    match session_manager.get_session(&session_id).await {
        Ok(Some(session)) => {
            let user = session.user;
            let user_agent = req
                .headers()
                .get(USER_AGENT)
                .and_then(|ua| ua.to_str().ok())
                .unwrap_or(NON_USER_AGENT);

            let key = match session_manager
                .derive_client_encrypt_key_with_user_agent(&session_id, user_agent)
                .await
            {
                Ok(key) => key,
                Err(err) => {
                    return HttpResponse::InternalServerError().json(RestErrResponse::new(
                        format!("Error deriving client encrypt key: {}", err),
                    ));
                }
            };
            let password = user.tsdb_password.as_deref().and_then(|password| {
                if let Ok(password) =
                    session_manager
                        .decrypt_password(password)
                        .inspect_err(|err| {
                            tracing::error!(
                                "Error decrypting password for user {}({}): {}",
                                user.user_id,
                                user.username,
                                err
                            );
                        })
                {
                    encrypt_cbc_mac_b64(password.as_bytes(), &key).ok()
                } else {
                    None
                }
            });

            let cookie_encrypt_key = Cookie::build(ENCRYPT_KEY, &key)
                .path("/")
                .same_site(SameSite::Lax)
                .max_age(actix_web::cookie::time::Duration::seconds(360))
                .finish();
            let cookie_session = Cookie::build(SESSION_ID_COOKIE, session_id)
                .path("/")
                .http_only(true)
                .same_site(SameSite::Lax)
                .max_age(actix_web::cookie::time::Duration::seconds(3600))
                .finish();
            HttpResponse::Ok()
                .cookie(cookie_encrypt_key)
                .cookie(cookie_session)
                .json(serde_json::json!({
                    "user_id": user.user_id,
                    "email": user.email,
                    "username": user.username,
                    "tsdb_username": user.tsdb_username,
                    "tsdb_password": password,
                    "provider": user.provider,
                    "is_self_provided": user.is_self_provided()
                }))
        }
        Ok(None) => HttpResponse::Unauthorized().json(RestErrResponse::new("Session not found")),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to retrieve user information: {:#}", e)
        })),
    }
}

async fn fetch_users_from_provider(
    req: &HttpRequest,
    oauth_client: &web::Data<OAuthClientEnum>,
    session_manager: &web::Data<SessionManager>,
    config: &web::Data<OAuthConfig>,
    credentials: &FetchUsersCredentials,
) -> Result<Vec<UserInfo>, HttpResponse> {
    if !config.enabled {
        return Err(HttpResponse::NotFound().body("OAuth is not enabled"));
    }
    if config.provider != "custom" {
        return Err(HttpResponse::BadRequest().json(RestErrResponse::new(
            "User sync is only supported for custom OAuth",
        )));
    }

    let session_id = if let Some(session_id) = extract_session_id_from_request(req) {
        session_id
    } else {
        return Err(HttpResponse::Unauthorized().json(RestErrResponse::new("Invalid session ID")));
    };

    let session = match session_manager.verify_session(&session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return Err(
                HttpResponse::Unauthorized().json(RestErrResponse::new("Session not found"))
            );
        }
        Err(e) => {
            tracing::error!("Failed to verify session for sync: {:#}", e);
            return Err(HttpResponse::InternalServerError()
                .json(RestErrResponse::new("Failed to verify session")));
        }
    };

    let _access_token = if let Some(token) = session.details.access_token.as_ref() {
        token.clone()
    } else {
        return Err(HttpResponse::BadRequest()
            .json(RestErrResponse::new("No access token in current session")));
    };

    let users = match oauth_client.as_ref() {
        OAuthClientEnum::Custom(client) => match client.fetch_users(credentials).await {
            Ok(users) => users,
            Err(e) => {
                tracing::error!("Failed to fetch users from custom OAuth provider: {:#}", e);
                return Err(
                    HttpResponse::InternalServerError().json(RestErrResponse::new(format!(
                        "Failed to fetch users from provider: {:#}",
                        e
                    ))),
                );
            }
        },
        _ => {
            return Err(HttpResponse::BadRequest().json(RestErrResponse::new(
                "User sync not supported for this provider",
            )));
        }
    };

    Ok(users)
}

#[derive(Debug, Deserialize)]
pub struct FetchUsersBody {
    #[serde(deserialize_with = "deserialize_non_empty_string")]
    password: String,
}

/// GET /api/-/oauth/users
/// Fetch user list from the custom OAuth provider using current session access token
pub async fn oauth_fetch_users(
    req: HttpRequest,
    oauth_client: web::Data<OAuthClientEnum>,
    session_manager: web::Data<SessionManager>,
    config: web::Data<OAuthConfig>,
    credentials: web::Json<FetchUsersBody>,
) -> impl Responder {
    let session_id = if let Some(session_id) = extract_session_id_from_request(&req) {
        session_id
    } else {
        return HttpResponse::Unauthorized().json(RestErrResponse::new("Invalid session ID"));
    };

    let session = match session_manager.verify_session(&session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return HttpResponse::Unauthorized().json(RestErrResponse::new("Session not found"));
        }
        Err(e) => {
            tracing::error!("Failed to verify session for sync: {:#}", e);
            return HttpResponse::InternalServerError()
                .json(RestErrResponse::new("Failed to verify session"));
        }
    };
    let credentials = FetchUsersCredentials {
        username: session.username().to_owned(),
        password: credentials.into_inner().password,
    };
    match fetch_users_from_provider(&req, &oauth_client, &session_manager, &config, &credentials)
        .await
    {
        Ok(users) => HttpResponse::Ok().json(users),
        Err(resp) => resp,
    }
}

/// POST /api/-/oauth/sync-users
/// Sync user list from the custom OAuth provider into oauth_users table
pub async fn oauth_sync_users(
    req: HttpRequest,
    oauth_client: web::Data<OAuthClientEnum>,
    session_manager: web::Data<SessionManager>,
    config: web::Data<OAuthConfig>,
    query: web::Query<TsdbSyncOptions>,
    credentials: web::Json<FetchUsersBody>,
) -> impl Responder {
    let session_id = if let Some(session_id) = extract_session_id_from_request(&req) {
        session_id
    } else {
        return HttpResponse::Unauthorized().json(RestErrResponse::new("Invalid session ID"));
    };

    let session = match session_manager.verify_session(&session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return HttpResponse::Unauthorized().json(RestErrResponse::new("Session not found"));
        }
        Err(e) => {
            tracing::error!("Failed to verify session for sync: {:#}", e);
            return HttpResponse::InternalServerError()
                .json(RestErrResponse::new("Failed to verify session"));
        }
    };
    let credentials = FetchUsersCredentials {
        username: session.username().to_owned(),
        password: credentials.into_inner().password,
    };
    if credentials.username.trim().is_empty() || credentials.password.trim().is_empty() {
        return HttpResponse::BadRequest().json(RestErrResponse::new(
            "SSO username and password are required for user sync",
        ));
    }
    let users = match fetch_users_from_provider(
        &req,
        &oauth_client,
        &session_manager,
        &config,
        &credentials,
    )
    .await
    {
        Ok(users) => users,
        Err(resp) => return resp,
    };

    let summary: OAuthSyncSummary = match session_manager
        .sync_users(&session, &config.provider, &users, &query)
        .await
    {
        Ok(summary) => summary,
        Err(e) => {
            tracing::error!("Failed to sync users into database: {:#}", e);
            return HttpResponse::InternalServerError().json(RestErrResponse::new(format!(
                "Failed to sync users: {:#}",
                e
            )));
        }
    };
    tracing::info!(
        "Synced oauth users into TSDB, imported: {}, updated: {}, skipped: {}",
        summary.imported,
        summary.updated,
        summary.skipped,
    );

    HttpResponse::Ok().json(summary)
}

/// Query params for listing existing OAuth users
#[derive(Deserialize)]
pub struct ListUsersQuery {
    provider: Option<String>,
}

#[derive(Serialize)]
pub struct OAuthUserView {
    pub user_id: i64,
    pub username: String,
    pub email: Option<String>,
    pub tsdb_username: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// GET /api/-/oauth/exist-users
/// List OAuth users already stored in database (oauth_users table)
pub async fn oauth_exist_users(
    req: HttpRequest,
    session_manager: web::Data<SessionManager>,
    config: web::Data<OAuthConfig>,
    query: web::Query<ListUsersQuery>,
) -> impl Responder {
    if !config.enabled {
        return HttpResponse::NotFound().body("OAuth is not enabled");
    }

    let session_id = if let Some(session_id) = extract_session_id_from_request(&req) {
        session_id
    } else {
        return HttpResponse::Unauthorized().json(RestErrResponse::new("Invalid session ID"));
    };

    let _session = match session_manager.verify_session(&session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return HttpResponse::Unauthorized().json(RestErrResponse::new("Session not found"));
        }
        Err(e) => {
            tracing::error!("Failed to verify session for list users: {:#}", e);
            return HttpResponse::InternalServerError()
                .json(RestErrResponse::new("Failed to verify session"));
        }
    };

    let provider = query.provider.as_deref().or(Some(config.provider.as_str()));
    match session_manager.list_oauth_users(provider).await {
        Ok(users) => {
            let view: Vec<OAuthUserView> = users
                .into_iter()
                .map(|u| OAuthUserView {
                    user_id: u.user_id,
                    username: u.username,
                    email: u.email,
                    tsdb_username: u.tsdb_username,
                    created_at: u.created_at,
                    updated_at: u.updated_at,
                })
                .collect();
            HttpResponse::Ok().json(view)
        }
        Err(e) => {
            tracing::error!("Failed to list OAuth users: {:#}", e);
            HttpResponse::InternalServerError()
                .json(RestErrResponse::new("Failed to list OAuth users"))
        }
    }
}

#[derive(Deserialize)]
pub struct UserId {
    pub id: i64,
}

/// Admin(root) user can revoke OAuth user
pub async fn oauth_revoke(
    req: HttpRequest,
    session_manager: web::Data<SessionManager>,
    user_id: web::Json<UserId>,
) -> HttpResponse {
    let session_id = if let Some(session_id) = extract_session_id_from_request(&req) {
        session_id
    } else {
        return HttpResponse::Unauthorized().json(RestErrResponse::new("Invalid session ID"));
    };
    let _session = match session_manager.verify_session(&session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return HttpResponse::Unauthorized().json(RestErrResponse::new("Session not found"));
        }
        Err(e) => {
            tracing::error!("Failed to verify session for list users: {:#}", e);
            return HttpResponse::InternalServerError().json(RestErrResponse::new(format!(
                "Failed to verify session: {e:#}"
            )));
        }
    };
    match session_manager.delete_user(user_id.id).await {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(e) => {
            tracing::error!("Failed to delete user: {:#}", e);
            HttpResponse::InternalServerError().json(RestErrResponse::new(format!(
                "Failed to delete user: {e:#}"
            )))
        }
    }
}

/// Handles OAuth disabled requests.
pub async fn oauth_disabled() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "error": "OAuth is disabled"
    }))
}
