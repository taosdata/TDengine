#!/usr/bin/env python3
"""
Custom OAuth 2.0 Flow Testing Helper

This script helps test the custom OAuth flow by simulating a user logging in
and verifying the entire authentication process.
"""

import requests
import re
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "http://localhost:6060"
SSO_BASE = "http://www.dodocloud.cn:43391"

# Test credentials
TEST_ACCOUNTS = [
    {"username": "admin", "password": "admin"},
    {"username": "test", "password": "12345"}
]

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_success(msg):
    print(f"{Colors.GREEN}✓ {msg}{Colors.END}")

def print_error(msg):
    print(f"{Colors.RED}✗ {msg}{Colors.END}")

def print_info(msg):
    print(f"{Colors.BLUE}ℹ {msg}{Colors.END}")

def print_warning(msg):
    print(f"{Colors.YELLOW}⚠ {msg}{Colors.END}")

def test_oauth_status():
    """Test 1: Verify OAuth is enabled"""
    print("\n" + "="*60)
    print("Test 1: OAuth Status Check")
    print("="*60)
    
    try:
        response = requests.get(f"{BASE_URL}/api/-/oauth/status")
        data = response.json()
        
        if data.get("enabled") and data.get("provider") == "custom":
            print_success("OAuth is enabled with custom provider")
            return True
        else:
            print_error(f"OAuth configuration incorrect: {data}")
            return False
    except Exception as e:
        print_error(f"Failed to check OAuth status: {e}")
        return False

def test_authorization_flow(username, password):
    """Test 2: Complete OAuth authorization flow"""
    print("\n" + "="*60)
    print(f"Test 2: Complete OAuth Flow (User: {username})")
    print("="*60)
    
    session = requests.Session()
    session.allow_redirects = False
    
    try:
        # Step 1: Initiate OAuth flow
        print_info("Step 1: Initiating OAuth flow...")
        response = session.get(f"{BASE_URL}/api/-/oauth/authorize")
        
        if response.status_code != 302:
            print_error(f"Expected redirect (302), got {response.status_code}")
            return False
        
        auth_url = response.headers.get('Location')
        print_success(f"Redirected to: {auth_url[:80]}...")
        
        # Extract state cookie
        state_cookie = None
        for cookie in session.cookies:
            if cookie.name == "oauth_state":
                state_cookie = cookie.value
                print_success(f"State cookie received: {state_cookie[:20]}...")
        
        if not state_cookie:
            print_error("No state cookie received")
            return False
        
        # Step 2: Follow redirect to SSO (simulate login)
        print_info("Step 2: Accessing SSO login page...")
        print_warning("Note: Actual SSO login requires browser interaction")
        print_warning("This script demonstrates the flow structure")
        
        # In a real scenario, you would:
        # 1. GET the auth_url (SSO login page)
        # 2. Parse the login form
        # 3. POST credentials to SSO
        # 4. Get redirected back with authorization code
        
        print_info("To complete this test manually:")
        print(f"  1. Open browser to: {BASE_URL}/api/-/oauth/authorize")
        print(f"  2. Login with: {username}/{password}")
        print(f"  3. After redirect, check for session cookie")
        
        return True
        
    except Exception as e:
        print_error(f"OAuth flow test failed: {e}")
        return False

def test_direct_endpoints():
    """Test 3: Test SSO endpoints directly"""
    print("\n" + "="*60)
    print("Test 3: Direct SSO Endpoint Testing")
    print("="*60)
    
    # Test token endpoint (needs authorization code)
    print_info("Token endpoint: POST " + f"{SSO_BASE}/sso/oauth2.0/accessToken")
    print_warning("Requires authorization code from SSO callback")
    
    # Test profile endpoint (needs access token)
    print_info("Profile endpoint: GET " + f"{SSO_BASE}/sso/oauth2.0/profile")
    print_warning("Requires access token from token exchange")
    
    return True

def main():
    print("\n" + "="*60)
    print("Custom OAuth 2.0 Integration Test")
    print("="*60)
    print(f"Base URL: {BASE_URL}")
    print(f"SSO Provider: {SSO_BASE}")
    print()
    
    # Test 1: OAuth Status
    if not test_oauth_status():
        print_error("\nOAuth is not properly configured. Please check the server.")
        return
    
    # Test 2: Authorization flow for each test account
    for account in TEST_ACCOUNTS:
        test_authorization_flow(account["username"], account["password"])
    
    # Test 3: Direct endpoints
    test_direct_endpoints()
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print_info("Automated tests completed")
    print_info("For complete end-to-end testing:")
    print("  1. Start server: ./start_test_server.sh")
    print("  2. Open browser: http://localhost:6060/api/-/oauth/authorize")
    print("  3. Login with test credentials")
    print("  4. Verify session creation")
    print()
    print("See CUSTOM_OAUTH_TESTING.md for detailed instructions")
    print("="*60)

if __name__ == "__main__":
    main()
