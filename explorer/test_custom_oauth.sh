#!/bin/bash
# Integration test script for Custom OAuth 2.0 SSO

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/custom-oauth-test.toml"
BASE_URL="http://localhost:6060"
SSO_BASE="http://www.dodocloud.cn:43391"

echo "=========================================="
echo "Custom OAuth 2.0 Integration Test"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Check if server is running
check_server() {
    print_info "Checking if server is running..."
    if curl -s "$BASE_URL/api/-/oauth/status" > /dev/null 2>&1; then
        print_success "Server is running at $BASE_URL"
        return 0
    else
        print_error "Server is not running at $BASE_URL"
        return 1
    fi
}

# Test 1: OAuth Status Endpoint
test_oauth_status() {
    echo ""
    echo "Test 1: OAuth Status Endpoint"
    echo "------------------------------"
    
    RESPONSE=$(curl -s "$BASE_URL/api/-/oauth/status")
    
    # Check if OAuth is enabled
    ENABLED=$(echo "$RESPONSE" | grep -o '"enabled":[^,}]*' | cut -d':' -f2)
    PROVIDER=$(echo "$RESPONSE" | grep -o '"provider":"[^"]*"' | cut -d'"' -f4)
    
    echo "Response: $RESPONSE"
    
    if [[ "$ENABLED" == *"true"* ]]; then
        print_success "OAuth is enabled"
    else
        print_error "OAuth is not enabled"
        return 1
    fi
    
    if [[ "$PROVIDER" == "custom" ]]; then
        print_success "Provider is set to 'custom'"
    else
        print_error "Provider is not 'custom', got: $PROVIDER"
        return 1
    fi
}

# Test 2: Authorization URL Generation
test_authorization_url() {
    echo ""
    echo "Test 2: Authorization URL Generation"
    echo "-------------------------------------"
    
    print_info "Fetching authorization URL..."
    
    # Follow redirect to get authorization URL
    AUTH_URL=$(curl -s -I -L "$BASE_URL/api/-/oauth/authorize" | grep -i "^location:" | tail -1 | cut -d' ' -f2 | tr -d '\r')
    
    if [[ -z "$AUTH_URL" ]]; then
        print_error "Failed to get authorization URL"
        return 1
    fi
    
    echo "Authorization URL: $AUTH_URL"
    
    # Check URL components
    if [[ "$AUTH_URL" == *"$SSO_BASE/sso/oauth2.0/authorize"* ]]; then
        print_success "Authorization endpoint is correct"
    else
        print_error "Authorization endpoint is incorrect"
        return 1
    fi
    
    if [[ "$AUTH_URL" == *"client_id=jRYp8CqZ"* ]]; then
        print_success "Client ID is present"
    else
        print_error "Client ID is missing"
        return 1
    fi
    
    if [[ "$AUTH_URL" == *"redirect_url="* ]]; then
        print_success "Redirect URL is present"
    else
        print_error "Redirect URL is missing"
        return 1
    fi
    
    if [[ "$AUTH_URL" == *"response_type=code"* ]]; then
        print_success "Response type is correct"
    else
        print_error "Response type is incorrect"
        return 1
    fi
}

# Test 3: Manual OAuth Flow Test (requires browser)
test_manual_flow() {
    echo ""
    echo "Test 3: Manual OAuth Flow"
    echo "--------------------------"
    
    print_info "To test the complete OAuth flow manually:"
    echo ""
    echo "1. Open your browser and navigate to:"
    echo "   $BASE_URL/api/-/oauth/authorize"
    echo ""
    echo "2. You will be redirected to the SSO login page"
    echo ""
    echo "3. Login with test credentials:"
    echo "   - Account 1: admin/admin"
    echo "   - Account 2: test/12345"
    echo ""
    echo "4. After successful login, you should be redirected back to:"
    echo "   $BASE_URL/login"
    echo ""
    echo "5. Check browser cookies for 'session_id'"
    echo ""
    echo "6. Test the /api/-/oauth/me endpoint with the session:"
    echo "   curl -b 'session_id=<your_session_id>' $BASE_URL/api/-/oauth/me"
    echo ""
}

# Test 4: Direct Token Exchange Test (requires manual code)
test_token_exchange() {
    echo ""
    echo "Test 4: Token Exchange (Manual)"
    echo "--------------------------------"
    
    print_info "To test token exchange, you need an authorization code from SSO"
    echo ""
    echo "After completing the OAuth flow in browser:"
    echo "1. Intercept the callback URL to get the 'code' parameter"
    echo "2. Use the code to test token exchange:"
    echo ""
    echo "   curl -X POST '$SSO_BASE/sso/oauth2.0/accessToken' \\"
    echo "     -H 'Content-Type: application/json' \\"
    echo "     -d '{"
    echo '       "client_id": "jRYp8CqZ",'
    echo '       "client_secret": "jRYp8CqZ",'
    echo '       "grant_type": "authorization_code",'
    echo '       "code": "<YOUR_CODE_HERE>"'
    echo "     }'"
    echo ""
}

# Test 5: Profile Endpoint Test (requires access token)
test_profile_endpoint() {
    echo ""
    echo "Test 5: Profile Endpoint (Manual)"
    echo "----------------------------------"
    
    print_info "To test profile endpoint, you need an access token"
    echo ""
    echo "After getting the access token from token exchange:"
    echo ""
    echo "   curl '$SSO_BASE/sso/oauth2.0/profile?access_token=<YOUR_TOKEN>'"
    echo ""
}

# Main test execution
main() {
    echo "Starting Custom OAuth Integration Tests..."
    echo ""
    
    if ! check_server; then
        print_error "Server must be running to execute tests"
        echo ""
        print_info "To start the server with test configuration:"
        echo "  cd server"
        echo "  cargo run -- --cfg-path ../custom-oauth-test.toml"
        exit 1
    fi
    
    FAILED=0
    
    test_oauth_status || FAILED=$((FAILED + 1))
    test_authorization_url || FAILED=$((FAILED + 1))
    test_manual_flow
    test_token_exchange
    test_profile_endpoint
    
    echo ""
    echo "=========================================="
    if [ $FAILED -eq 0 ]; then
        print_success "All automated tests passed!"
        echo ""
        print_info "Manual tests are documented above for complete flow verification"
    else
        print_error "$FAILED automated test(s) failed"
    fi
    echo "=========================================="
    
    return $FAILED
}

# Run tests
main
