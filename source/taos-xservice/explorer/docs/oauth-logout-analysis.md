# OAuth Logout and Token Revocation Analysis

## Question

Should we keep the refresh_token when user calls `/oauth/logout`, or delete everything including the refresh_token?

## Answer: Delete Everything (Current Implementation is Correct)

### Current Behavior ✅

```rust
// POST /api/-/oauth/logout
pub async fn oauth_logout() {
    match session_manager.delete_session(&session_id).await {
        // Deletes:
        // - session_id
        // - access_token
        // - refresh_token  ← This is CORRECT
        // - id_token
        // - tsdb credentials
    }
}
```

## Reasoning

### 1. OAuth 2.0 Best Practices

According to [RFC 6749](https://datatracker.ietf.org/doc/html/rfc6749) and [OAuth 2.0 Security Best Practices](https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics):

> When a user explicitly logs out, all tokens associated with that session should be invalidated.

**Why?**
- **Security**: Refresh tokens are powerful - they can create new access tokens
- **User expectation**: "Logout" means "end all access"
- **Compliance**: Many security standards require complete session termination

### 2. User Expectations

When a user clicks "Logout," they expect:
- ❌ **NOT** "pause my session temporarily"
- ✅ **YES** "completely end my session and revoke all access"

**Analogy**: Logging out of Gmail revokes all access, not just the current tab.

### 3. Security Considerations

**Scenario: User on shared computer**
```
1. User logs in → receives refresh_token
2. User works for a while
3. User clicks "Logout" → only clears session_id
4. Refresh token still in database
5. Attacker with database access → steals refresh_token
6. Attacker can create new session without username/password
```

**With full deletion:**
```
1. User logs in → receives refresh_token
2. User works for a while
3. User clicks "Logout" → DELETES refresh_token
4. Refresh token no longer exists
5. Attacker cannot use it
```

### 4. Session Expiration vs Logout

These are **different concepts**:

| Event | Session Expired | User Logout |
|-------|----------------|-------------|
| **Cause** | Time passed, no activity | User clicks "Logout" |
| **Intent** | Automatic timeout | Explicit action |
| **Action** | Keep refresh_token? Maybe | Delete everything? YES |
| **Re-login** | Could use refresh_token | Must fully authenticate |

## Alternative: Enhanced Logout with Provider Revocation

The current implementation is correct, but we can **enhance** it by also revoking the refresh token at the OAuth provider (if supported).

### Enhanced Logout Flow

```rust
pub async fn oauth_logout_enhanced(
    req: HttpRequest,
    session_manager: web::Data<SessionManager>,
    oauth_client: Option<web::Data<OAuthClientEnum>>,
) -> impl Responder {
    let session_id = extract_session_id_from_request(&req)?;
    
    // 1. Get session to retrieve refresh_token
    if let Ok(Some(session)) = session_manager.get_session(&session_id).await {
        // 2. Revoke refresh_token at OAuth provider (if supported)
        if let Some(refresh_token) = &session.details.refresh_token {
            if let Some(client) = oauth_client {
                // Attempt to revoke at provider
                let _ = client.revoke_token(refresh_token).await;
                // Note: Ignore errors - provider might not support revocation
            }
        }
    }
    
    // 3. Delete local session (current implementation)
    session_manager.delete_session(&session_id).await?;
    
    HttpResponse::Ok().json({
        "status": "logged_out"
    })
}
```

## When to Keep Refresh Tokens?

Refresh tokens should be kept (not deleted) in these scenarios:

### 1. Session Expiration (Not Logout)
- **Trigger**: `expires_at` timestamp reached
- **Action**: Auto-cleanup expired sessions
- **Refresh token**: Already expired naturally

### 2. Token Refresh
- **Trigger**: Access token expiring soon
- **Action**: Exchange refresh_token for new access_token
- **Refresh token**: Keep (or replace if rotated)

### 3. Background Cleanup
- **Trigger**: Scheduled job (hourly)
- **Action**: Remove sessions where `expires_at < now()`
- **Refresh token**: Removed with session

## Comparison Table

| Scenario | Keep Refresh Token? | Reasoning |
|----------|-------------------|-----------|
| **User clicks Logout** | ❌ NO | Explicit action to end session |
| **Session expires naturally** | ❌ NO | Session lifetime ended |
| **Token refresh (auto)** | ✅ YES | Part of normal operation |
| **Access token refresh** | ✅ YES | Expected behavior |
| **Database cleanup (expired)** | ❌ NO | Already expired |

## Security Standards

### OWASP Recommendations

[OWASP Session Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html):

> Logout functionality should completely destroy the session on both client and server sides.

### PCI DSS Compliance

For systems handling sensitive data:
- Requirement 8.1.8: After 15 minutes of inactivity, require the user to re-authenticate
- Logout must completely terminate the session

### GDPR Considerations

- User has "right to be forgotten"
- Logout is a form of data deletion request
- Keeping refresh tokens after logout could be considered retaining user data without consent

## Implementation Recommendations

### Current Implementation: Keep As-Is ✅

The current implementation is **correct and secure**:

```rust
pub async fn oauth_logout() {
    session_manager.delete_session(&session_id).await?;
    // This deletes everything including refresh_token
}
```

### Optional Enhancement: Add Provider Revocation

For extra security, revoke refresh token at provider:

**Pros:**
- More thorough cleanup
- Prevents token reuse even if database is compromised
- Follows OAuth 2.0 Token Revocation RFC 7009

**Cons:**
- Not all providers support revocation endpoint
- Adds network latency to logout
- May fail silently

**Implementation:**
- Make it optional/best-effort
- Don't block logout if revocation fails
- Log success/failure for monitoring

## Real-World Examples

### Google OAuth
- Logout: Deletes refresh token locally
- Optional: Call revocation endpoint
- User must re-authenticate fully

### Microsoft Azure AD
- Logout: Clears all tokens
- Calls `end_session_endpoint` to revoke tokens
- User redirected to provider logout page

### Auth0
- Logout: Deletes local session
- Revoke endpoint available for refresh tokens
- User must re-authenticate

### GitHub
- Logout: Deletes session
- OAuth tokens don't expire, but session ends
- User must re-authorize

## Conclusion

### ✅ Current Implementation is Correct

Deleting everything including refresh_token on logout is:
1. **Secure** - No lingering access
2. **Standard** - Follows OAuth 2.0 best practices
3. **Expected** - Matches user expectations
4. **Compliant** - Meets security standards

### Optional Enhancement

Consider adding OAuth provider revocation:
```rust
// Before deleting session
if let Some(refresh_token) = session.refresh_token {
    oauth_client.revoke_token(refresh_token).await?;
}
// Then delete session (current implementation)
session_manager.delete_session(session_id).await?;
```

### Do NOT Keep Refresh Tokens After Logout

Keeping refresh tokens after logout would:
- ❌ Violate user expectations
- ❌ Create security risks
- ❌ Violate OAuth 2.0 best practices
- ❌ Potentially violate compliance requirements

## References

- [RFC 6749 - OAuth 2.0 Authorization Framework](https://datatracker.ietf.org/doc/html/rfc6749)
- [RFC 7009 - OAuth 2.0 Token Revocation](https://datatracker.ietf.org/doc/html/rfc7009)
- [OAuth 2.0 Security Best Practices](https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics)
- [OWASP Session Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)
- [OpenID Connect Session Management](https://openid.net/specs/openid-connect-session-1_0.html)
