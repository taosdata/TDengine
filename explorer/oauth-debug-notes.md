# OAuth Token Exchange Error - Diagnosis and Fix

## Problem
The OAuth callback was failing with error:
```
Failed to exchange authorization code: Failed to parse token response: error decoding response body: missing field `access_token` at line 1 column 41
```

## Root Cause
The custom OAuth provider's token endpoint is likely returning a response that doesn't match the expected format, or the response is wrapped in an additional structure.

## Changes Made

### 1. Enhanced Token Response Parsing (`custom_client.rs`)
- Changed `TokenResponse` from a simple struct to an enum that can handle multiple response formats:
  - **Direct format**: `{"access_token": "..."}`
  - **Wrapped format**: `{"data": {"access_token": "..."}}`
- Added `.trim()` to handle potential whitespace in token values (documentation showed space before token)
- Added comprehensive debug logging to capture the actual response body

### 2. Improved Error Logging
- Added debug logging that prints the raw HTTP response before parsing
- Added error logging that shows both the response body and the parsing error
- Enhanced RUST_LOG to include debug level for OAuth client module

## Testing Instructions

1. **Start the test server:**
   ```bash
   cd /mnt/home/Projects/taosdata/taosx/explorer
   ./start_test_server.sh
   ```

2. **Perform OAuth login flow:**
   - Open browser to http://localhost:6060
   - Click login/OAuth button
   - Complete SSO authentication
   - Check the server logs for the token response

3. **Look for these log lines:**
   ```
   Token response body: <actual JSON response>
   Failed to parse token response. Body: '...', Error: ...
   ```

## Next Steps

If the error persists:

1. **Check the actual response format** in the debug logs
2. **Possible response formats from the provider:**
   - Standard OAuth: `{"access_token": "...", "token_type": "Bearer", "expires_in": 7200}`
   - Error response: `{"error": "invalid_grant", "error_description": "..."}`
   - Custom wrapped: `{"success": true, "data": {"access_token": "..."}}`
   - Plain text instead of JSON

3. **Common issues:**
   - Authorization code already used (codes are single-use)
   - Authorization code expired (typically 10 minutes)
   - Client credentials mismatch
   - Redirect URI mismatch
   - Network/firewall issues reaching the token endpoint

## Configuration Reference

Current test configuration (`custom-oauth-test.toml`):
- **Token URL**: http://www.dodocloud.cn:43391/sso/oauth2.0/accessToken
- **Client ID**: NtT4ey1C
- **Redirect URI**: http://localhost:6060/api/-/oauth/callback

## Documentation Reference

According to `docs/custom-oauth.md` (Table 4), the expected response is:
```json
{
  "access_token": " b1d508f3-32e7-4d6d-ac62-87836406704c"
}
```

Note: The documentation example shows a space before the token value, which our code now handles with `.trim()`.
