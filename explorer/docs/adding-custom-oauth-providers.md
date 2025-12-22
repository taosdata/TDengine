# Adding Custom OAuth Providers to TSDB Explorer

This guide explains how to add support for OAuth 2.0 providers beyond the built-in OIDC implementation, and clarifies the split between **plain** (standard OAuth 2.0) and **custom** (TSDB-specific) clients.

## Current Provider Types

TSDB Explorer now supports three OAuth provider types:

1. **`oidc`** – Standard OpenID Connect with automatic discovery.
2. **`plain`** – Standard OAuth 2.0 (RFC 6749/6750) without discovery.
3. **`custom`** – TSDB-specific OAuth 2.0 with optional extra endpoints (e.g., user sync).

The provider is chosen via `oauth.provider`, and dispatched by the backend `OAuthClientEnum`.

## Configuration Structure

### Plain (Standard OAuth 2.0)

Use `provider = "plain"` for standard OAuth 2.0 flows (authorization code, token exchange, profile):

```toml
[oauth]
enabled = true
provider = "plain"

[oauth.plain]
client_id = "your_client_id"
client_secret = "your_client_secret"
authorize_url = "https://provider.com/oauth/authorize"
token_url = "https://provider.com/oauth/token"
profile_url = "https://provider.com/oauth/userinfo"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[oauth.user_mapping]
username = "preferred_username"
email = "email"
first_name = "given_name"
last_name = "family_name"
roles = "groups"
```

### Custom (GaoXin-Specific)

Use `provider = "custom"` only when you need the TSDB-specific extras (e.g., `fetch_users_url` or legacy payloads):

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.custom]
client_id = "your_client_id"
client_secret = "your_client_secret"
authorize_url = "https://provider.com/sso/oauth2.0/authorize"
token_url = "https://provider.com/sso/oauth2.0/accessToken"
profile_url = "https://provider.com/sso/oauth2.0/profile"
fetch_users_url = "https://provider.com/sso/oauth2.0/getUsers"  # optional
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[oauth.user_mapping]
username = "username"
email = "email"
roles = "attributes.roles[].role_name"
```

### OIDC (Existing)

Use `provider = "oidc"` for discovery-based OpenID Connect.

## Choosing Between `plain` and `custom`

- **Use `plain`** for standard OAuth 2.0 providers (GitHub, GitLab, Google, Azure AD, Okta, Keycloak, Auth0, etc.).
- **Use `custom`** only if you rely on:
  - The TSDB-specific **user sync endpoint** (`fetch_users_url`).
  - Legacy or non-standard profile/token responses already handled by the custom client.
  - Existing deployments that depend on the previous “custom” behavior.

If you were previously using `provider = "custom"` for a standard OAuth 2.0 provider, switch to `provider = "plain"`.

## Migration Notes

1. Change `provider = "custom"` to `provider = "plain"` when the provider is standard OAuth 2.0.
2. Move the endpoints into `[oauth.plain]` (same field names as before).
3. If you need user sync or legacy parsing, keep `provider = "custom"` and `[oauth.custom]`.

The backend will automatically fall back to `[oauth.custom]` values when `[oauth.plain]` is empty, but explicitly configuring `[oauth.plain]` is recommended.

## Validation Rules

- `plain` requires: `client_id`, `client_secret`, `authorize_url`, `token_url`, `profile_url`, `redirect_uri` (all valid URLs).
- `custom` requires the same, plus optional `fetch_users_url`.
- `oidc` requires: `client_id`, `client_secret`, `issuer_url` (valid URL).

## User Mapping

`[oauth.user_mapping]` applies to all providers:

- `username`: e.g., `preferred_username`, `login`, or `sub` (email prefix fallback).
- `email`: e.g., `email`.
- `first_name`, `last_name`: e.g., `given_name`, `family_name`.
- `roles`: e.g., `groups` or `attributes.roles[].role_name` (for custom).

## Flow Differences

- **OIDC**: Uses PKCE + nonce + discovery.
- **Plain**: Standard OAuth 2.0 authorization code with Bearer profile fetch.
- **Custom**: Same as plain for auth/token/profile, plus:
  - Query-parameter fallback for profile (`?access_token=`).
  - Optional `fetch_users_url` for bulk sync.
  - Flexible profile parsing for legacy attributes.

## Examples

### GitHub (use `plain`)
```toml
[oauth]
enabled = true
provider = "plain"

[oauth.plain]
client_id = "github_client_id"
client_secret = "github_client_secret"
authorize_url = "https://github.com/login/oauth/authorize"
token_url = "https://github.com/login/oauth/access_token"
profile_url = "https://api.github.com/user"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[oauth.user_mapping]
username = "login"
email = "email"
```

### Google (use `plain`)
```toml
[oauth]
enabled = true
provider = "plain"

[oauth.plain]
client_id = "google_client_id.apps.googleusercontent.com"
client_secret = "google_client_secret"
authorize_url = "https://accounts.google.com/o/oauth2/v2/auth"
token_url = "https://oauth2.googleapis.com/token"
profile_url = "https://www.googleapis.com/oauth2/v2/userinfo"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[oauth.user_mapping]
username = "email"
email = "email"
first_name = "given_name"
last_name = "family_name"
```

### TSDB Legacy with User Sync (keep `custom`)
```toml
[oauth]
enabled = true
provider = "custom"

[oauth.custom]
client_id = "client_id"
client_secret = "client_secret"
authorize_url = "https://sso.example.com/oauth2.0/authorize"
token_url = "https://sso.example.com/oauth2.0/accessToken"
profile_url = "https://sso.example.com/oauth2.0/profile"
fetch_users_url = "https://sso.example.com/oauth2.0/getUsers"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[oauth.user_mapping]
username = "username"
email = "email"
roles = "attributes.roles[].role_name"
```

## Testing Checklist

- `oauth.provider` set to one of `oidc`, `plain`, `custom`.
- Required fields present and valid URLs.
- `redirect_uri` matches the provider registration.
- Login flow: `/api/-/oauth/authorize` → provider login → `/api/-/oauth/callback`.
- For `custom`: verify profile fetch and (if used) `/api/-/oauth/users` or `/api/-/oauth/sync-users`.

## References

-- [OAuth 2.0 RFC](https://datatracker.ietf.org/doc/html/rfc6749)
-- [OpenID Connect Specification](https://openid.net/specs/openid-connect-core-1_0.html)
-- [Custom OAuth Documentation](custom-oauth.md)
-- [OIDC Configuration Example](../oidc-test.toml)
-- [Custom OAuth Configuration Example](../custom-oauth-test.toml)
