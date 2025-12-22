# OAuth 2.0 Standard Compliance

## Overview

The `custom` OAuth provider in TSDB Explorer has been updated to support **standard OAuth 2.0** providers (RFC 6749) in addition to the original custom implementation.

## ✅ Supported OAuth 2.0 Providers

The custom OAuth client now works with most standard OAuth 2.0 services, including:

- **GitHub** - https://github.com/settings/developers
- **GitLab** - https://gitlab.com/-/profile/applications
- **Google** - https://console.cloud.google.com/apis/credentials
- **Microsoft Azure AD** - https://portal.azure.com/
- **Okta** - https://www.okta.com/
- **Auth0** - https://auth0.com/
- **Keycloak** - https://www.keycloak.org/
- **GitLab self-hosted**
- **Generic OAuth 2.0 providers**

## Standard OAuth 2.0 Flow Support

### Authorization Request

**Standard compliant:**
```
GET /oauth/authorize?
  response_type=code&
  client_id={client_id}&
  redirect_uri={redirect_uri}&    ✅ Uses standard parameter name
  state={state}                   ✅ CSRF protection
```

### Token Exchange

**Standard compliant (RFC 6749 Section 4.1.3):**
```json
{
  "client_id": "...",
  "client_secret": "...",
  "grant_type": "authorization_code",
  "code": "...",
  "redirect_uri": "..."           ✅ Required by standard
}
```

### User Info Request

**Two methods supported:**

1. **Standard (preferred):**
   ```
   GET /userinfo
   Authorization: Bearer {access_token}    ✅ OAuth 2.0 standard
   ```

2. **Fallback (legacy/non-standard):**
   ```
   GET /userinfo?access_token={token}      ⚠️ For compatibility only
   ```

The implementation tries Bearer token first, then falls back to query parameter if needed.

## Profile Response Compatibility

The client handles multiple user profile field formats:

### Standard OAuth 2.0 / OIDC Fields

```json
{
  "sub": "user123",                    // Subject (unique ID)
  "preferred_username": "john.doe",    // Preferred username
  "email": "john@example.com",
  "given_name": "John",
  "family_name": "Doe",
  "name": "John Doe"
}
```

### Custom Format (Backward Compatible)

```json
{
  "username": "john.doe",
  "attributes": {
    "roles": [
      {"role_name": "admin"}
    ]
  }
}
```

### Field Priority for Username

The client tries to extract username in this order:
1. `username`
2. `preferred_username`
3. `sub` (strips domain if email format)
4. `name`

## Token Response Formats

### Standard Format
```json
{
  "access_token": "ya29.xxx...",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

### Wrapped Format (Supported)
```json
{
  "data": {
    "access_token": "token_here"
  }
}
```

### Legacy Format with Space
```json
{
  "access_token": " token_with_leading_space"
}
```
*Automatically trimmed*

## Configuration Examples

### GitHub OAuth

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.custom]
client_id = "your_github_client_id"
client_secret = "your_github_client_secret"
authorize_url = "https://github.com/login/oauth/authorize"
token_url = "https://github.com/login/oauth/access_token"
profile_url = "https://api.github.com/user"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[oauth.user_mapping]
username = "login"
email = "email"
```

### Google OAuth

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.custom]
client_id = "your_google_client_id.apps.googleusercontent.com"
client_secret = "your_google_client_secret"
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

### GitLab Self-Hosted

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.custom]
client_id = "your_gitlab_app_id"
client_secret = "your_gitlab_secret"
authorize_url = "https://gitlab.company.com/oauth/authorize"
token_url = "https://gitlab.company.com/oauth/token"
profile_url = "https://gitlab.company.com/api/v4/user"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[oauth.user_mapping]
username = "username"
email = "email"
```

### Generic OAuth 2.0 Provider

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.custom]
client_id = "your_client_id"
client_secret = "your_client_secret"
authorize_url = "https://provider.com/oauth2/authorize"
token_url = "https://provider.com/oauth2/token"
profile_url = "https://provider.com/oauth2/userinfo"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[oauth.user_mapping]
username = "sub"
email = "email"
```

## Known Limitations

### 1. No Scope Support in Authorization
Currently, the authorization URL does not include a `scope` parameter. This works for many providers that have default scopes, but some providers may require explicit scopes.

**Workaround:** For OIDC providers that require scopes, use `provider = "oidc"` instead.

### 2. ~~No Refresh Token Support~~ ✅ **Now Supported**
Refresh tokens are **fully supported** for both OIDC and custom OAuth providers. Access tokens are automatically refreshed before expiration, providing seamless, uninterrupted user sessions.

**See**: [OAuth Refresh Token Support](oauth-refresh-tokens.md) for detailed documentation.

### 3. No Client Authentication Methods
Only client_secret in the request body is supported. Client authentication via Basic Auth header or JWT is not supported.

### 4. No PKCE Support
The custom OAuth flow does not use PKCE (Proof Key for Code Exchange). For OIDC with PKCE, use `provider = "oidc"`.

### 5. Limited Token Response Parsing
Only `access_token` is extracted from the token response. Other fields like `token_type`, `expires_in`, `refresh_token` are ignored.

## Migration from Non-Standard Implementation

If you were using a previous version with non-standard parameters:

### Old (Non-Standard)
- Used `redirect_url` instead of `redirect_uri` ❌
- Omitted `redirect_uri` from token exchange ❌
- Only supported query parameter auth ❌

### New (Standard + Backward Compatible)
- Uses `redirect_uri` (OAuth 2.0 standard) ✅
- Includes `redirect_uri` in token exchange ✅
- Tries Bearer token first, falls back to query param ✅
- Flexible profile response parsing ✅

## Testing with Popular Providers

### GitHub
1. Register OAuth app: https://github.com/settings/developers
2. Set Authorization callback URL: `http://localhost:6060/api/-/oauth/callback`
3. Use the generated Client ID and Client Secret

### Google
1. Create OAuth 2.0 credentials: https://console.cloud.google.com/apis/credentials
2. Add redirect URI: `http://localhost:6060/api/-/oauth/callback`
3. Enable Google+ API or OAuth consent screen
4. Use Client ID and Client Secret

### GitLab
1. Create application: https://gitlab.com/-/profile/applications
2. Set Redirect URI: `http://localhost:6060/api/-/oauth/callback`
3. Select scopes: `read_user` minimum
4. Use Application ID and Secret

## Troubleshooting

### "redirect_uri_mismatch" Error
**Cause:** The redirect_uri in your config doesn't exactly match what's registered with the provider.

**Solution:** Make sure the redirect_uri matches exactly (including http vs https, trailing slash, port number).

### "invalid_grant" Error During Token Exchange
**Cause:** Authorization code has expired or already been used.

**Solution:** Authorization codes are single-use and typically expire in 10 minutes. Generate a new code by restarting the OAuth flow.

### "unauthorized_client" Error
**Cause:** Client ID or secret is incorrect, or the OAuth application is not properly configured.

**Solution:** Verify credentials and check that the OAuth app is enabled/active on the provider side.

### Profile Fetch Returns 401 Unauthorized
**Cause:** Access token is invalid or the profile endpoint requires different authentication.

**Solution:** Check provider documentation for the correct user info endpoint URL. Some providers use different paths like `/api/user` vs `/userinfo`.

## References

- [RFC 6749 - OAuth 2.0 Authorization Framework](https://datatracker.ietf.org/doc/html/rfc6749)
- [RFC 6750 - Bearer Token Usage](https://datatracker.ietf.org/doc/html/rfc6750)
- [OpenID Connect Core 1.0](https://openid.net/specs/openid-connect-core-1_0.html)
- [GitHub OAuth Documentation](https://docs.github.com/en/developers/apps/building-oauth-apps)
- [Google OAuth 2.0 Documentation](https://developers.google.com/identity/protocols/oauth2)
- [GitLab OAuth Documentation](https://docs.gitlab.com/ee/api/oauth2.html)
