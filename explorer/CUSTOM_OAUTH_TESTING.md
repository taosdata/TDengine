# Custom OAuth 2.0 Integration Testing Guide

This guide provides instructions for testing the custom OAuth 2.0 SSO implementation.

## Test Environment

- **SSO Provider**: http://www.dodocloud.cn:43391/
- **Client ID**: jRYp8CqZ
- **Client Secret**: jRYp8CqZ
- **Test Accounts**:
  - Account 1: `admin/admin`
  - Account 2: `test/12345`

## Quick Start

### 1. Start the Server with Test Configuration

```bash
cd server
cargo run -- --cfg-path ../custom-oauth-test.toml
```

The server will start on `http://localhost:6060` with custom OAuth enabled.

### 2. Run Automated Tests

In a separate terminal:

```bash
cd explorer
./test_custom_oauth.sh
```

This will run automated tests that verify:
- OAuth status endpoint
- Authorization URL generation
- Correct endpoint configuration

## Manual Integration Testing

### Test 1: Complete OAuth Flow (Browser)

1. **Initiate OAuth Flow**
   ```bash
   # Open in your browser:
   http://localhost:6060/api/-/oauth/authorize
   ```

2. **SSO Login**
   - You'll be redirected to: `http://www.dodocloud.cn:43391/sso/oauth2.0/authorize`
   - Login with:
     - Username: `admin` (or `test`)
     - Password: `admin` (or `12345`)

3. **Callback**
   - After successful login, you'll be redirected back to:
     `http://localhost:6060/api/-/oauth/callback?code=...&state=...`
   - Then redirected to: `http://localhost:6060/login`

4. **Verify Session**
   - Check browser cookies for `session_id`
   - Test the authenticated endpoint:
     ```bash
     curl -b "session_id=<your_session_id>" http://localhost:6060/api/-/oauth/me
     ```

### Test 2: Direct API Testing

#### Get Authorization URL
```bash
curl -I http://localhost:6060/api/-/oauth/authorize
```
Expected: 302 redirect to SSO provider

#### Check OAuth Status
```bash
curl http://localhost:6060/api/-/oauth/status
```
Expected response:
```json
{
  "enabled": true,
  "provider": "custom"
}
```

#### Token Exchange (after getting code from callback)
```bash
curl -X POST 'http://www.dodocloud.cn:43391/sso/oauth2.0/accessToken' \
  -H 'Content-Type: application/json' \
  -d '{
    "client_id": "jRYp8CqZ",
    "client_secret": "jRYp8CqZ",
    "grant_type": "authorization_code",
    "code": "<CODE_FROM_CALLBACK>"
  }'
```
Expected response:
```json
{
  "access_token": "..."
}
```

#### Get User Profile
```bash
curl 'http://www.dodocloud.cn:43391/sso/oauth2.0/profile?access_token=<ACCESS_TOKEN>'
```
Expected response:
```json
{
  "username": "admin",
  "attributes": {
    "token_expired": 7200,
    "token_time": 1638253419364,
    "roles": [
      {
        "role_name": "管理员"
      }
    ],
    "orgs": [
      {
        "org_name": "xx部门",
        "org_path": "/xx总部/xx中心/xx部门"
      }
    ]
  }
}
```

## Test Scenarios

### Scenario 1: Successful Login Flow
1. User visits `/api/-/oauth/authorize`
2. User is redirected to SSO login page
3. User enters credentials and logs in
4. SSO redirects back with authorization code
5. Backend exchanges code for access token
6. Backend fetches user profile
7. Backend creates session
8. User is redirected to `/login` with session cookie

**Expected Result**: User is authenticated with valid session

### Scenario 2: Invalid Credentials
1. User visits SSO login page
2. User enters invalid credentials
3. SSO shows error message

**Expected Result**: Login fails at SSO level, no callback to application

### Scenario 3: Session Verification
1. User completes OAuth flow
2. User makes authenticated request with session cookie
3. Backend validates session

**Expected Result**: User data returned from `/api/-/oauth/me`

### Scenario 4: Logout
1. User has active session
2. User calls `/api/-/oauth/logout`
3. Backend deletes session

**Expected Result**: Session is invalidated, subsequent requests fail

## Debugging

### Enable Detailed Logging

Set environment variable before starting server:
```bash
export RUST_LOG=debug
cd server
cargo run -- --cfg-path ../custom-oauth-test.toml
```

### Check Server Logs

Look for these log messages:
- `OAuth client initialized successfully (provider: custom)`
- `Initiating custom OAuth authorization flow`
- `Custom OAuth login successful for user: ...`
- `Created OAuth session: ...`

### Common Issues

**Issue**: OAuth not enabled
- **Solution**: Verify `custom-oauth-test.toml` is being loaded
- Check logs for "OAuth is not enabled"

**Issue**: Invalid client credentials
- **Solution**: Verify client_id and client_secret in config
- Check SSO provider settings

**Issue**: Redirect URI mismatch
- **Solution**: Ensure redirect_uri in config matches registered URI with SSO provider
- Default: `http://localhost:6060/api/-/oauth/callback`

**Issue**: Network connectivity
- **Solution**: Verify SSO provider is accessible:
  ```bash
  curl http://www.dodocloud.cn:43391/sso/oauth2.0/authorize
  ```

## Test Checklist

- [ ] Server starts with custom OAuth configuration
- [ ] `/api/-/oauth/status` returns enabled=true, provider=custom
- [ ] `/api/-/oauth/authorize` redirects to SSO login page
- [ ] SSO login page loads correctly
- [ ] Can login with test account (admin/admin)
- [ ] Callback receives authorization code
- [ ] Token exchange succeeds
- [ ] User profile fetch succeeds
- [ ] Session is created
- [ ] `/api/-/oauth/me` returns user info
- [ ] `/api/-/oauth/logout` invalidates session

## Configuration Reference

See `custom-oauth-test.toml` for the complete test configuration.

Key settings:
- `oauth.enabled = true`
- `oauth.provider = "custom"`
- `oauth.custom.authorize_url` - SSO authorization endpoint
- `oauth.custom.token_url` - Token exchange endpoint
- `oauth.custom.profile_url` - User profile endpoint

## Next Steps

After successful testing:
1. Update configuration for production environment
2. Register production redirect URIs with SSO provider
3. Update client credentials for production
4. Configure proper session expiration times
5. Set up monitoring and logging
