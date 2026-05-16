# OAuth Provider Display Name

## Overview

The OAuth configuration now supports customizable provider display names with internationalization (i18n) support for English and Chinese. This allows you to customize the text shown on the login button instead of the generic "OAuth Login" text.

## Configuration

### Backend Configuration (TOML)

Add the `provider_display_name` section to your OAuth configuration:

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.provider_display_name]
en = "Company SSO"        # English display name
zh = "公司单点登录"       # Chinese display name
```

### Configuration Examples

#### Example 1: Generic SSO Provider

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.provider_display_name]
en = "Enterprise SSO"
zh = "企业单点登录"

[oauth.custom]
client_id = "your_client_id"
client_secret = "your_client_secret"
authorize_url = "https://sso.company.com/oauth/authorize"
token_url = "https://sso.company.com/oauth/token"
profile_url = "https://sso.company.com/oauth/userinfo"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"
```

#### Example 2: GitHub OAuth

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.provider_display_name]
en = "GitHub"
zh = "GitHub"

[oauth.custom]
client_id = "your_github_client_id"
client_secret = "your_github_client_secret"
authorize_url = "https://github.com/login/oauth/authorize"
token_url = "https://github.com/login/oauth/access_token"
profile_url = "https://api.github.com/user"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"
```

#### Example 3: GitLab Self-Hosted

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.provider_display_name]
en = "GitLab"
zh = "GitLab"

[oauth.custom]
client_id = "your_gitlab_app_id"
client_secret = "your_gitlab_secret"
authorize_url = "https://gitlab.company.com/oauth/authorize"
token_url = "https://gitlab.company.com/oauth/token"
profile_url = "https://gitlab.company.com/api/v4/user"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"
```

#### Example 4: Company-Specific Branding

```toml
[oauth]
enabled = true
provider = "custom"

[oauth.provider_display_name]
en = "Acme Corp ID"
zh = "Acme 公司账号"

[oauth.custom]
# ... OAuth configuration
```

## Default Values

If `provider_display_name` is not specified, it defaults to:

```toml
[oauth.provider_display_name]
en = "OAuth"
zh = "OAuth"
```

## Frontend Display

The login button text will automatically change based on the user's selected language:

### English UI
```
┌─────────────────────────────┐
│  Login with Company SSO     │
└─────────────────────────────┘
```

### Chinese UI
```
┌─────────────────────────────┐
│  使用 公司单点登录 登录     │
└─────────────────────────────┘
```

## API Response

The OAuth status endpoint (`/api/-/oauth/status`) returns the display name:

### Request
```bash
curl http://localhost:6060/api/-/oauth/status
```

### Response
```json
{
  "enabled": true,
  "provider": "custom",
  "provider_display_name": {
    "en": "Company SSO",
    "zh": "公司单点登录"
  }
}
```

## Implementation Details

### Backend (Rust)

The configuration structure:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OAuthConfig {
    pub enabled: bool,
    pub provider: String,
    pub provider_display_name: ProviderDisplayName,
    // ... other fields
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProviderDisplayName {
    pub en: String,
    pub zh: String,
}
```

### Frontend (Vue.js)

The login button implementation:

```vue
<el-button class="oauth-button" type="success" @click="loginWithOAuth">
  {{ $t('login.loginWith', { 
    provider: getLocalLang() === 'zh' 
      ? oauthProviderDisplayName.zh 
      : oauthProviderDisplayName.en 
  }) }}
</el-button>
```

### i18n Translation Keys

**English** (`src/lang/en/login.ts`):
```typescript
loginWith: "Login with {provider}"
```

**Chinese** (`src/lang/zh/login.ts`):
```typescript
loginWith: "使用 {provider} 登录"
```

## Best Practices

### 1. Keep Names Concise
Use short, recognizable names that fit on a button:

✅ Good:
- "GitHub"
- "Google"
- "Company SSO"

❌ Too long:
- "Company Enterprise Single Sign-On System"

### 2. Consistent Branding
If your OAuth provider has an official brand name, use it:

```toml
[oauth.provider_display_name]
en = "Okta"
zh = "Okta"
```

### 3. Localization Guidelines

For Chinese translations:
- Use simplified Chinese (简体中文)
- Keep technical terms in English when commonly used (e.g., "GitHub", "OAuth")
- For company-specific systems, translate descriptively

Examples:
```toml
# Technical service - keep English
[oauth.provider_display_name]
en = "Auth0"
zh = "Auth0"

# Generic SSO - translate
[oauth.provider_display_name]
en = "Single Sign-On"
zh = "单点登录"

# Company-specific - customize
[oauth.provider_display_name]
en = "Employee Portal"
zh = "员工门户"
```

## Troubleshooting

### Display Name Not Showing

**Problem**: Login button still shows "OAuth Login" instead of custom name.

**Solutions**:
1. Check that `provider_display_name` is correctly configured in TOML
2. Restart the explorer server after configuration changes
3. Clear browser cache and reload the page
4. Verify the OAuth status API returns the display name:
   ```bash
   curl http://localhost:6060/api/-/oauth/status
   ```

### Wrong Language Displayed

**Problem**: Display name shows in wrong language.

**Solutions**:
1. Check browser language settings
2. Use the language switcher (中/EN button) on login page
3. Verify both `en` and `zh` fields are configured

### Special Characters Not Displaying

**Problem**: Chinese characters appear as boxes or question marks.

**Solutions**:
1. Ensure your TOML file is saved with UTF-8 encoding
2. Verify the editor/IDE supports UTF-8
3. Check that no BOM (Byte Order Mark) is present

## Migration Guide

### From Generic "OAuth Login"

**Before** (no custom display name):
```toml
[oauth]
enabled = true
provider = "custom"
```

Button text: "OAuth Login" (EN) / "OAuth 登录" (ZH)

**After** (with custom display name):
```toml
[oauth]
enabled = true
provider = "custom"

[oauth.provider_display_name]
en = "Company SSO"
zh = "公司单点登录"
```

Button text: "Login with Company SSO" (EN) / "使用 公司单点登录 登录" (ZH)

## Related Documentation

- [OAuth Configuration Guide](oauth-standard-compliance.md)
- [Adding Custom OAuth Providers](adding-custom-oauth-providers.md)
- [Custom OAuth Specification](custom-oauth.md)
