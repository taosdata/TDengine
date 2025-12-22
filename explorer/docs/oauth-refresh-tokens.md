# OAuth Refresh Token Support

## Overview

TSDB Explorer now supports OAuth 2.0 refresh tokens for both OIDC and custom OAuth providers. This feature enables seamless, uninterrupted user sessions by automatically refreshing expired access tokens without requiring users to re-authenticate.

## What are Refresh Tokens?

### OAuth 2.0 Token Flow

When a user logs in via OAuth, they receive two types of tokens:

1. **Access Token** - Short-lived token (typically 1-2 hours) used to access protected resources
2. **Refresh Token** - Long-lived token (days/weeks/months) used to obtain new access tokens

### The Problem Without Refresh Tokens

Without refresh token support:
- Users are logged out when their access token expires (1-2 hours)
- Users must manually re-authenticate through the OAuth provider
- Interrupts workflows and reduces productivity
- Poor user experience for long-running operations

### The Solution With Refresh Tokens

With refresh token support:
- Access tokens are automatically refreshed before they expire
- Users stay logged in for extended periods (session lifetime)
- Seamless, uninterrupted experience
- Background refresh happens transparently

## How It Works

### 1. Initial Authentication

```
User → OAuth Provider → TSDB Explorer
                     ↓
           Receives both tokens:
           - access_token (expires in 2h)
           - refresh_token (expires in 30d)
```

### 2. Token Storage

Both tokens are encrypted and stored securely in the session database:

```sql
CREATE TABLE oauth_sessions (
    session_id TEXT PRIMARY KEY,
    access_token TEXT,           -- AES-256-GCM encrypted
    refresh_token TEXT,          -- AES-256-GCM encrypted
    access_token_expires_at TIMESTAMP,
    expires_at TIMESTAMP,
    ...
);
```

### 3. Automatic Token Refresh

When a user makes a request:

```
1. Middleware checks if access_token expires within 5 minutes
2. If yes → automatically refresh using refresh_token
3. Store new tokens in database
4. Continue with request using new access_token
5. User never notices the refresh happened
```

### Token Refresh Flow Diagram

```
User Request
    ↓
[Middleware Check]
    ↓
Access token expiring? ───No──→ Continue with request
    │
   Yes
    ↓
[Token Refresh]
    │
    ├─→ POST /oauth/token
    │   {
    │     "grant_type": "refresh_token",
    │     "refresh_token": "...",
    │     "client_id": "...",
    │     "client_secret": "..."
    │   }
    ↓
[Update Session]
    │
    ├─→ new_access_token
    ├─→ new_refresh_token (if rotated)
    ├─→ new_expires_at
    ↓
Continue with request
```

## User Benefits

### 1. Uninterrupted Sessions

**Without Refresh Tokens:**
```
9:00 AM  - User logs in
11:00 AM - Access token expires
11:01 AM - User gets logged out
         - Must click "Login" again
         - Redirected to OAuth provider
         - Lose current work context
```

**With Refresh Tokens:**
```
9:00 AM  - User logs in
11:00 AM - Access token expires
11:01 AM - Token automatically refreshed
         - User continues working
         - No interruption
```

### 2. Improved Productivity

- No need to re-authenticate every 1-2 hours
- Long-running queries/operations complete without interruption
- Batch operations can run for extended periods
- Better experience for data analysis workflows

### 3. Security Without Friction

- Short-lived access tokens limit exposure if compromised
- Long-lived refresh tokens enable convenience
- Automatic rotation of refresh tokens (if provider supports it)
- Encrypted storage of all tokens

### 4. Better User Experience

- Transparent to users
- No "session expired" popups
- Seamless across browser tabs
- Works even when user is idle

## Configuration

### For OIDC Providers

OIDC providers typically return refresh tokens by default if you request the `offline_access` scope:

```toml
[oauth]
enabled = true
provider = "oidc"

[oauth.oidc]
client_id = "your_client_id"
client_secret = "your_client_secret"
issuer_url = "https://auth.example.com"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"
scopes = ["openid", "profile", "email", "offline_access"]  # Add offline_access
```

### For Custom OAuth Providers

Custom OAuth providers must return `refresh_token` in the token response:

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.custom]
client_id = "your_client_id"
client_secret = "your_client_secret"
authorize_url = "https://provider.com/oauth/authorize"
token_url = "https://provider.com/oauth/token"
profile_url = "https://provider.com/oauth/userinfo"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"
```

**Expected Token Response:**
```json
{
  "access_token": "eyJhbGc...",
  "refresh_token": "def5020...",
  "token_type": "Bearer",
  "expires_in": 7200
}
```

## Provider-Specific Configuration

### GitHub

GitHub OAuth **does not expire** access tokens by default, so refresh tokens are not needed or provided.

### Google

```toml
[oauth.oidc]
scopes = ["openid", "profile", "email", "offline_access"]
```

Refresh tokens expire after 6 months of inactivity.

### Microsoft Azure AD

```toml
[oauth.oidc]
scopes = ["openid", "profile", "email", "offline_access"]
```

Refresh tokens can be configured with various lifetime policies.

### Okta

```toml
[oauth.oidc]
scopes = ["openid", "profile", "email", "offline_access"]
```

Default refresh token lifetime: 90 days (configurable).

### GitLab

GitLab provides refresh tokens with a default lifetime of 2 hours for access tokens.

```toml
[oauth.custom]
# GitLab configuration
```

### Auth0

```toml
[oauth.oidc]
scopes = ["openid", "profile", "email", "offline_access"]
```

Refresh token rotation is enabled by default.

## Implementation Details

### Refresh Trigger

Tokens are automatically refreshed when:
- Access token expires within 5 minutes
- User makes any authenticated request
- Middleware detects expiring token

### Refresh Token Rotation

Some providers (Auth0, Azure AD) support refresh token rotation:
- Each refresh request returns a **new** refresh token
- Old refresh token becomes invalid
- Increases security by limiting token lifetime

TSDB Explorer automatically handles rotation:
```rust
pub async fn refresh_session_token(
    &self,
    session_id: &str,
    new_access_token: &str,
    new_refresh_token: Option<&str>,  // New refresh token if rotated
    expires_in_seconds: Option<i64>,
) -> Result<()>
```

### Token Storage Security

All tokens are encrypted before storage:

```rust
// Encryption using AES-256-GCM
let encrypted_access = self.encrypt_token(access_token)?;
let encrypted_refresh = self.encrypt_token(refresh_token)?;

// Storage
INSERT INTO oauth_sessions (
    access_token,
    refresh_token,
    ...
) VALUES (?, ?, ...)
```

### Error Handling

If refresh fails:
1. Log warning message
2. Continue with existing access token (may still be valid)
3. If access token is truly expired, user will get authentication error
4. User can manually re-authenticate

```rust
match client.refresh_access_token(refresh_token).await {
    Ok((new_token, new_refresh, expires_in)) => {
        // Update session
    }
    Err(e) => {
        tracing::warn!("Failed to refresh: {}", e);
        // Continue with existing token
    }
}
```

## Session Lifetimes

### Access Token Lifetime
- **Short-lived**: 1-2 hours (provider-specific)
- **Purpose**: Minimize security risk
- **Automatically refreshed**: Yes

### Refresh Token Lifetime
- **Long-lived**: Days to months (provider-specific)
- **Purpose**: Enable long-running sessions
- **Automatically rotated**: If provider supports it

### Session Lifetime
- **Default**: 8 hours
- **Configurable**: Can be extended
- **Independent of**: Access/refresh token lifetimes

Example timeline:
```
Session Created: 9:00 AM
├─ Access Token:  9:00 AM - 11:00 AM (2h)
│  ├─ Refreshed:  10:55 AM
│  ├─ New Token:  10:55 AM - 12:55 PM
│  └─ Refreshed:  12:50 PM
├─ Refresh Token: 9:00 AM - 10:00 AM next day (30d)
└─ Session:       9:00 AM - 5:00 PM (8h)
```

## Monitoring and Logging

### Successful Refresh

```log
INFO Successfully refreshed access token for session abc123 
     (new_refresh_token: true, expires_in: 7200s)
```

### Failed Refresh

```log
WARN Failed to refresh access token: invalid_grant - 
     Refresh token expired
```

### Token Expiration Detection

```log
INFO Access token expiring soon for session abc123, 
     attempting refresh
```

## Troubleshooting

### Refresh Token Not Received

**Problem**: OAuth provider doesn't return `refresh_token`

**Solutions**:
1. **OIDC**: Add `offline_access` scope
2. **Custom OAuth**: Check provider documentation for required parameters
3. **Verify** provider supports refresh tokens
4. **Check** token response in logs

### Refresh Fails with "invalid_grant"

**Problem**: Refresh token is expired or invalid

**Possible Causes**:
- Refresh token expired (check provider lifetime policy)
- Refresh token was revoked
- User changed password
- Client credentials changed

**Solution**: User must re-authenticate

### Tokens Not Refreshing Automatically

**Problem**: Users still getting logged out

**Check**:
1. Verify `access_token_expires_at` is set correctly
2. Check middleware is configured properly
3. Ensure `OAuthClientEnum` is in app data
4. Review logs for refresh attempts

### Refresh Token Rotation Issues

**Problem**: Refresh fails after first use

**Cause**: Provider rotates tokens but new token not saved

**Solution**: Verify `new_refresh_token` parameter handling in code

## Best Practices

### 1. Always Request Refresh Tokens

For OIDC, always include `offline_access`:
```toml
scopes = ["openid", "profile", "email", "offline_access"]
```

### 2. Set Appropriate Session Lifetimes

Balance security and convenience:
- **Short sessions** (4-8 hours): High security environments
- **Long sessions** (24+ hours): Development/internal tools
- **Consider** user activity patterns

### 3. Monitor Token Refresh Rates

High refresh rates may indicate:
- Access tokens too short
- Multiple concurrent sessions
- Configuration issues

### 4. Handle Refresh Failures Gracefully

Don't panic if refresh fails:
- Existing token may still work
- User can re-authenticate if needed
- Log errors for investigation

### 5. Secure Token Storage

- Use strong encryption (AES-256-GCM)
- Rotate encryption keys periodically
- Never log tokens in plaintext
- Store encryption key securely (environment variable)

## Migration Guide

### From No Refresh Tokens

If upgrading from a version without refresh token support:

1. **Database**: No migration needed (schema already supports it)
2. **Configuration**: Add `offline_access` scope for OIDC
3. **Testing**: Verify tokens refresh automatically
4. **Users**: Existing sessions continue to work

### From OIDC-Only Refresh

If upgrading to add custom OAuth refresh support:

1. **No changes needed**: Custom OAuth automatically uses refresh tokens if provided
2. **Provider**: Ensure provider returns `refresh_token`
3. **Testing**: Monitor logs for successful refreshes

## Security Considerations

### Token Theft Protection

- **Encrypted storage**: Tokens encrypted at rest
- **Short access tokens**: Limit damage if stolen
- **Refresh rotation**: Invalidates old refresh tokens
- **HTTPS required**: All OAuth traffic over TLS

### Revocation

To revoke a session:
```rust
session_manager.delete_session(session_id).await?;
```

This removes both access and refresh tokens.

### Audit Trail

All refresh operations are logged:
- Session ID
- Timestamp
- Success/failure
- Token expiration times

## Performance Impact

### Minimal Overhead

- Refresh happens **once every 2 hours** (not per request)
- Database update is fast (~10ms)
- HTTP request to provider (~100-500ms)
- **Total impact**: Negligible for users

### Database Load

- One UPDATE per refresh (~1 request every 2 hours per active user)
- Encrypted token storage adds minimal overhead
- Indexes on `session_id` ensure fast lookups

## Comparison: With vs Without Refresh Tokens

| Aspect | Without Refresh Tokens | With Refresh Tokens |
|--------|----------------------|-------------------|
| **Session Duration** | Limited to access token lifetime (1-2h) | Extended to session lifetime (8h+) |
| **User Experience** | Frequent re-authentication required | Seamless, uninterrupted |
| **Security** | Less secure (long access tokens) OR Poor UX | Secure (short access) + Good UX |
| **Long Operations** | May fail mid-operation | Complete successfully |
| **Productivity** | Interrupted workflows | Continuous workflows |
| **Implementation** | Simpler | More complex (but automated) |

## Conclusion

Refresh token support is a critical feature for production OAuth deployments. It provides:

✅ **Better user experience** - No interruptions  
✅ **Improved security** - Short-lived access tokens  
✅ **Higher productivity** - Uninterrupted workflows  
✅ **Transparent operation** - Users never notice  
✅ **Standard compliance** - OAuth 2.0 RFC 6749  

For production deployments, always enable refresh token support by:
1. Adding `offline_access` scope (OIDC)
2. Ensuring provider returns `refresh_token`
3. Monitoring refresh operations in logs
4. Setting appropriate session lifetimes

## References

- [RFC 6749 - OAuth 2.0](https://datatracker.ietf.org/doc/html/rfc6749#section-6)
- [OpenID Connect Core - Refresh Tokens](https://openid.net/specs/openid-connect-core-1_0.html#RefreshTokens)
- [OAuth 2.0 Security Best Practices](https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics)
