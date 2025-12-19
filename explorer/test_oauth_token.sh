#!/bin/bash
# Test script for custom OAuth token endpoint

TOKEN_URL="http://www.dodocloud.cn:43391/sso/oauth2.0/accessToken"

# You need to replace this with an actual authorization code from a real OAuth flow
CODE="${1:-test-code}"
CLIENT_ID="${2:-NtT4ey1C}"
CLIENT_SECRET="${3:-mKeMWqF3}"

echo "Testing token endpoint: $TOKEN_URL"
echo "Code: $CODE"
echo ""

curl -v -X POST "$TOKEN_URL" \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "'"$CLIENT_ID"'",
    "client_secret": "'"$CLIENT_SECRET"'",
    "grant_type": "authorization_code",
    "code": "'"$CODE"'"
  }' 2>&1 | grep -A 100 "^< HTTP"

echo ""
echo "Note: You need to provide a valid authorization code from the OAuth flow"
echo "Usage: $0 <authorization_code> [client_id] [client_secret]"
