# Custom OAuth 2.0 Testing - Quick Start

## Prerequisites

- Rust toolchain installed
- Access to SSO provider: http://www.dodocloud.cn:43391/
- Test credentials available

## Quick Test (30 seconds)

### 1. Start the Test Server
```bash
./start_test_server.sh
```

### 2. Run Automated Tests (in another terminal)
```bash
./test_custom_oauth.sh
```

Expected output:
```
✓ OAuth is enabled
✓ Provider is set to 'custom'
✓ Authorization endpoint is correct
✓ Client ID is present
✓ Redirect URL is present
✓ Response type is correct
```

### 3. Browser Test (Manual)

Open browser to:
```
http://localhost:6060/api/-/oauth/authorize
```

Login with:
- **Username**: `admin`
- **Password**: `admin`

After login, you should be redirected to `http://localhost:6060/login` with a session cookie.

## Verification

Check if OAuth is working:
```bash
# 1. Get OAuth status
curl http://localhost:6060/api/-/oauth/status

# 2. Check authorization redirect
curl -I http://localhost:6060/api/-/oauth/authorize

# 3. After browser login, test session (replace SESSION_ID)
curl -b "session_id=SESSION_ID" http://localhost:6060/api/-/oauth/me
```

## Test Accounts

| Username | Password | Role |
|----------|----------|------|
| admin    | admin    | Administrator |
| test     | 12345    | User |

## Test Scripts

- `./start_test_server.sh` - Start server with test config
- `./test_custom_oauth.sh` - Automated endpoint tests
- `./test_oauth_flow.py` - Python-based flow testing (optional)

## Configuration

Test configuration in: `custom-oauth-test.toml`

Key settings:
```toml
[oauth]
enabled = true
provider = "custom"

[oauth.custom]
client_id = "jRYp8CqZ"
client_secret = "jRYp8CqZ"
authorize_url = "http://www.dodocloud.cn:43391/sso/oauth2.0/authorize"
token_url = "http://www.dodocloud.cn:43391/sso/oauth2.0/accessToken"
profile_url = "http://www.dodocloud.cn:43391/sso/oauth2.0/profile"
```

## Troubleshooting

### Server won't start
```bash
# Check if port 6060 is in use
lsof -i :6060

# Check configuration
cat custom-oauth-test.toml
```

### OAuth not enabled
```bash
# Check server logs
export RUST_LOG=debug
./start_test_server.sh
```

### SSO redirect fails
```bash
# Verify SSO is accessible
curl http://www.dodocloud.cn:43391/sso/oauth2.0/authorize
```

## Next Steps

For detailed testing instructions, see:
- **CUSTOM_OAUTH_TESTING.md** - Comprehensive testing guide
- **custom-oauth-example.toml** - Production config template

## Support

For issues or questions:
1. Check server logs with `RUST_LOG=debug`
2. Review CUSTOM_OAUTH_TESTING.md
3. Verify SSO provider is accessible
