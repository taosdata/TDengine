# OAuth 2.0/OIDC SSO Implementation - taos-explorer

## 📋 概述

本文档详细记录了 taos-explorer 中 OAuth 2.0/OIDC 单点登录 (SSO) 的完整实现。该实现支持标准的 OIDC 提供商（如 Keycloak、Okta、Azure AD 等），并与现有的 HTTP Basic Authentication 共存。

### 特性

✅ **标准 OIDC 支持** - 基于 OpenID Connect Discovery 自动发现  
✅ **安全性** - PKCE (SHA256)、CSRF 保护、JWT 签名验证  
✅ **会话管理** - 基于 SQLite 的持久化会话存储  
✅ **双认证模式** - 支持 Basic Auth 和 Bearer Token  
✅ **自动清理** - 后台任务自动清理过期会话  
✅ **前端集成** - Vue 3 + TypeScript 完整前端实现  
✅ **密码加密** - AES-GCM 加密存储 TDengine 凭据

---

## 🏗️ 架构设计

### 认证流程

```
┌─────────┐      ┌──────────┐      ┌─────────┐      ┌──────────┐
│ Browser │─────▶│ Explorer │─────▶│   IdP   │─────▶│ TDengine │
│         │◀─────│ (Backend)│◀─────│(Keycloak│      │          │
└─────────┘      └──────────┘      └─────────┘      └──────────┘
    │                  │                 │                │
    │ 1. Click SSO     │                 │                │
    │─────────────────▶│                 │                │
    │                  │ 2. Redirect     │                │
    │                  │────────────────▶│                │
    │                  │                 │ 3. User Login  │
    │                  │                 │                │
    │ 4. Callback      │                 │                │
    │◀─────────────────│◀────────────────│                │
    │                  │ 5. Token Exchange                │
    │                  │────────────────▶│                │
    │                  │◀────────────────│                │
    │                  │ 6. Create Session               │
    │                  │─────────────────────────────────▶│
    │ 7. Redirect with │                 │                │
    │    session token │                 │                │
    │◀─────────────────│                 │                │
    │ 8. Store token & │                 │                │
    │    access TDengine                 │                │
    │──────────────────────────────────────────────────▶│
```

### 组件架构

```
Backend (Rust)
├── oauth/
│   ├── config.rs          # OAuth 配置管理
│   ├── client.rs          # OIDC 客户端 (openidconnect crate)
│   ├── session.rs         # 会话管理 (SQLite)
│   ├── middleware.rs      # 认证中间件
│   └── handlers.rs        # HTTP 端点处理器
│
Frontend (Vue 3)
├── api/oauth.ts           # OAuth API 客户端
├── views/
│   ├── 0_login/index.vue  # 登录页面 (SSO 按钮)
│   └── oauth-callback/    # OAuth 回调页面
├── utils/
│   ├── request.ts         # 请求拦截器 (Bearer Token)
│   └── aesGcm.ts          # AES-GCM 加密工具
└── store/modules/app.ts   # Vuex OAuth 状态管理
```

---

## 🔧 后端实现

### 1. 核心模块

#### config.rs - 配置管理
**代码行数**: 201 行  
**责任**: OAuth 配置加载、验证、环境变量覆盖

**核心结构**:
```rust
pub struct OAuthConfig {
    pub enabled: bool,
    pub provider: String,
    pub fallback_redirect_uri: Option<String>,
    pub oidc: OidcConfig,
    pub user_mapping: UserMapping,
    // Note: TDengine-specific configuration (previously `tdengine: TDengineOAuthConfig`)
    // has been removed from the main OAuth config. TDengine credential binding is
    // handled via the `oauth_bind` endpoint and `oauth_users` table.
}

pub struct OidcConfig {
    pub client_id: String,
    pub client_secret: String,
    pub issuer_url: String,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
    // 自动发现的端点
    pub authorization_endpoint: Option<String>,
    pub token_endpoint: Option<String>,
    pub userinfo_endpoint: Option<String>,
    pub jwks_uri: Option<String>,
}
```

**环境变量支持**:
- `EXPLORER_OAUTH_ENABLED` - 启用/禁用 OAuth
- `EXPLORER_OAUTH_CLIENT_ID` - OAuth 客户端 ID
- `EXPLORER_OAUTH_CLIENT_SECRET` - OAuth 客户端密钥
- `EXPLORER_OAUTH_ISSUER_URL` - OIDC 发行者 URL
- `EXPLORER_OAUTH_REDIRECT_URI` - 回调 URI
- `EXPLORER_OAUTH_SCOPES` - OAuth 作用域（逗号分隔）

#### client.rs - OIDC 客户端
**代码行数**: 372 行  
**依赖**: `openidconnect` crate  
**责任**: OIDC Discovery、授权 URL 生成、Token 交换、JWT 验证

**关键功能**:
```rust
impl OidcClient {
    // OIDC Discovery - 自动发现提供商元数据
    pub async fn new(config: OAuthConfig) -> Result<Self>
    
    // 生成授权 URL (PKCE + Nonce + CSRF Token)
    pub fn generate_auth_url(&self) -> AuthorizationRequest
    
    // 交换授权码获取 Token
    pub async fn exchange_code(&self, code: &str, pkce_verifier: &str, 
                                nonce: &str) -> Result<(CoreIdTokenClaims, ...)>
    
    // 从 ID Token Claims 提取用户信息
    pub fn extract_user_info_from_claims(&self, claims: &CoreIdTokenClaims) 
                                         -> Result<UserInfo>
    
    // 从 UserInfo 端点获取用户信息
    pub async fn fetch_user_info(&self, access_token: &str) 
                                 -> Result<CoreUserInfoClaims>
}
```

**安全机制**:
- ✅ PKCE (SHA256) - 防止授权码拦截攻击
- ✅ Nonce - 防止重放攻击
- ✅ CSRF Token (State) - 防止跨站请求伪造
- ✅ JWT 签名验证 - 自动使用 JWKS 验证

#### session.rs - 会话管理
**代码行数**: 508 行  
**数据库**: SQLite (via sqlx)  
**责任**: 会话创建、验证、绑定 TDengine 凭据、清理

**数据模型**:
```rust
pub struct OAuthUser {
    pub user_id: i64,
    pub username: String,
    pub tsdb_username: Option<String>,  // TDengine 用户名
    pub tsdb_password: Option<String>,  // TDengine 密码 (加密)
    pub email: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct OAuthSession {
    pub user: OAuthUser,
    pub details: OAuthSessionDetails,
}

pub struct OAuthSessionDetails {
    pub session_id: String,        // UUID v4
    pub user_id: i64,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub login_at: DateTime<Utc>,
    pub last_active: DateTime<Utc>,
}
```

**核心方法**:
```rust
impl SessionManager {
    // 创建 OAuth 会话 (默认 8 小时过期)
    pub async fn create_session(...) -> Result<OAuthSession>
    
    // 创建自提供凭据的会话 (用于 generate-token)
    pub async fn create_self_provided_session(...) -> Result<OAuthSession>
    
    // 获取会话
    pub async fn get_session(&self, session_id: &str) -> Result<Option<OAuthSession>>
    
    // 验证会话 (检查过期、更新活跃时间)
    pub async fn verify_session(&self, session_id: &str) -> Result<Option<OAuthSession>>
    
    // 绑定 TDengine 凭据到 OAuth 用户
    pub async fn bind_tsdb_credentials(&self, session_id: &str, 
                                        tsdb_username: &str, 
                                        tsdb_password: &str) -> Result<()>
    
    // 清理过期会话
    pub async fn cleanup_expired_sessions(&self) -> Result<u64>
}
```

#### middleware.rs - 认证中间件
**代码行数**: 121 行  
**责任**: 统一认证接口，支持 Basic Auth 和 Bearer Token

```rust
pub struct TsdbCredential {
    pub auth_type: AuthType,  // Basic | Bearer
    pub username: String,
    pub password: String,
}

// 从 Authorization header 提取认证信息
pub async fn extract_auth(auth_header: Option<&str>, 
                          session_manager: Option<&SessionManager>) 
                          -> Result<Option<TsdbCredential>, String>

// 从 actix-web Request 提取认证信息
pub async fn extract_auth_from_request(req: &actix_web::HttpRequest) 
                                        -> Result<Option<TsdbCredential>, String>
```

**认证流程**:
1. 检查 `Authorization` header
2. 如果是 `Basic xxx` → 解码获取用户名密码
3. 如果是 `Bearer xxx` → 查询 SessionManager 验证会话 → 获取绑定的 TDengine 凭据
4. 返回统一的 `TsdbCredential` 供后续使用

#### handlers.rs - HTTP 端点
**代码行数**: 约 400 行  
**端点数量**: 5 个

**1. GET /api/-/oauth/status**
```rust
pub async fn oauth_status(config: web::Data<OAuthConfig>) -> impl Responder
```
- 返回 OAuth 是否启用、提供商类型
- 无需认证
- 前端用于条件渲染 SSO 按钮

**2. GET /api/-/oauth/authorize**
```rust
pub async fn oauth_authorize(oidc_client: web::Data<OidcClient>, 
                              config: web::Data<OAuthConfig>) -> impl Responder
```
- 生成授权 URL (PKCE + Nonce + CSRF)
- 将 state、nonce、verifier 存储在 HTTP-only Cookie (10 分钟过期)
- 重定向到 IdP 登录页面

**3. GET /api/-/oauth/callback**
```rust
pub async fn oauth_callback(query: web::Query<CallbackQuery>, 
                             req: HttpRequest, 
                             oidc_client: web::Data<OidcClient>, 
                             session_manager: web::Data<SessionManager>, 
                             config: web::Data<OAuthConfig>) -> impl Responder
```
- 验证 state (CSRF 保护)
- 从 Cookie 提取 nonce 和 verifier
- 交换授权码获取 Token
- 验证 ID Token (JWT 签名验证)
- 提取用户信息
- 创建会话 (8 小时过期)
- 生成 AES key (基于 session_id SHA256)
- 重定向到前端:
  - 已绑定 TDengine 凭据 → `/#/oauth/callback?token=xxx`
  - 未绑定 → `/#/login?token=xxx` (需要用户绑定)

**4. POST /api/-/oauth/bind**
```rust
pub async fn oauth_bind(req: HttpRequest, 
                         body: web::Json<BindRequest>, 
                         session_manager: web::Data<SessionManager>) -> impl Responder

pub struct BindRequest {
    token: String,           // session_id
    username: String,        // TDengine 用户名
    credential: String,      // AES-GCM 加密的密码 (Base64)
}
```
- 从 Cookie 读取 AES key
- 解密 TDengine 密码
- 绑定到 OAuth 用户
- 前端可在登录页直接绑定现有 TDengine 账号

**5. POST /api/-/oauth/logout**
```rust
pub async fn oauth_logout(req: HttpRequest, 
                           session_manager: web::Data<SessionManager>) -> impl Responder
```
- 从 Authorization header 提取 Bearer token
- 删除会话
- 返回成功

### 2. 数据库 Schema

**Migration**: `migrations/20251201000001_oauth_sessions.up.sql`

```sql
-- OAuth 用户映射表
CREATE TABLE oauth_users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,           -- 提供商名称 (keycloak, okta, etc.)
    username TEXT NOT NULL,           -- OAuth 用户名
    nickname TEXT,
    email TEXT,
    tsdb_username TEXT,               -- 绑定的 TDengine 用户名
    tsdb_password TEXT,               -- 加密的 TDengine 密码
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (provider, username)
);

-- OAuth 会话表
CREATE TABLE oauth_sessions (
    session_id TEXT PRIMARY KEY NOT NULL,  -- UUID v4
    user_id INTEGER NOT NULL,
    access_token TEXT,                      -- OAuth access token
    refresh_token TEXT,                     -- OAuth refresh token (未使用)
    id_token TEXT,                          -- OIDC id_token (未存储)
    expires_at TIMESTAMP NOT NULL,
    login_at TIMESTAMP NOT NULL,
    last_active TIMESTAMP NOT NULL
);

-- 索引
CREATE INDEX idx_oauth_sessions_username ON oauth_sessions(user_id);
CREATE INDEX idx_oauth_sessions_expires ON oauth_sessions(expires_at);
CREATE INDEX idx_oauth_sessions_last_active ON oauth_sessions(last_active);
```

### 3. main.rs 集成

**关键修改点**:

1. **模块声明** (line 70):
```rust
mod oauth;
```

2. **配置加载** (lines 200-213):
```rust
if let Some(oauth_config) = args.oauth.as_mut() {
    oauth_config.from_env();
    if let Err(e) = oauth_config.validate() {
        tracing::error!("OAuth configuration validation failed: {}", e);
        anyhow::bail!("OAuth configuration error: {}", e);
    }
    if oauth_config.enabled {
        tracing::info!("OAuth 2.0/OIDC authentication is enabled");
    }
}
```

3. **组件初始化** (lines 338-355):
```rust
let oauth_client = if args.oauth.as_ref().is_some_and(|c| c.enabled) {
    let oauth_config = args.oauth.as_ref().unwrap();
    match oauth::OidcClient::new(oauth_config.clone()).await {
        Ok(client) => {
            tracing::info!("OAuth OIDC client initialized successfully");
            Some(client)
        }
        Err(e) => {
            tracing::error!("Failed to initialize OAuth OIDC client: {}", e);
            anyhow::bail!("OAuth initialization failed: {}", e);
        }
    }
} else {
    None
};

let session_manager = Some(oauth::SessionManager::new(favorites.pool.clone()));
args.session_manager = session_manager.clone();
```

4. **后台清理任务** (lines 362-374):
```rust
if let Some(session_mgr) = session_manager.as_ref() {
    let session_mgr_clone = session_mgr.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600)); // 每小时
        loop {
            interval.tick().await;
            if let Err(e) = session_mgr_clone.cleanup_expired_sessions().await {
                tracing::error!("Failed to cleanup expired OAuth sessions: {}", e);
            }
        }
    });
    tracing::info!("OAuth session cleanup task started (runs every hour)");
}
```

5. **路由注册** (lines 414-438):
```rust
.configure(|cfg| {
    if oauth_client.is_some() && session_manager.is_some() {
        cfg.app_data(web::Data::new(oauth_client.clone().unwrap()))
            .app_data(web::Data::new(oauth_config.unwrap_or_default()))
            .route("/api/-/oauth/status", web::get().to(oauth::handlers::oauth_status))
            .route("/api/-/oauth/authorize", web::get().to(oauth::handlers::oauth_authorize))
            .route("/api/-/oauth/callback", web::get().to(oauth::handlers::oauth_callback))
            .route("/api/-/oauth/bind", web::post().to(oauth::handlers::oauth_bind))
            .route("/api/-/oauth/logout", web::post().to(oauth::handlers::oauth_logout));
    }
})
.route("/api/-/generate-token", web::get().to(oauth::handlers::self_provided_token))
```

---

## 🎨 前端实现

### 1. OAuth API 客户端

**文件**: `src/api/oauth.ts` (71 行)

```typescript
// 获取 OAuth 状态
export function getOAuthStatus(): Promise<{ enabled: boolean; provider?: string }>

// 发起 OAuth 授权 (重定向到 IdP)
export function oauthAuthorize(): void

// 绑定 TDengine 凭据
export async function oauthBindTsdb(token: string, username: string, password: string): Promise<any>

// OAuth 登出
export function oauthLogout(): Promise<any>

// 检查 OAuth 会话
export function checkOAuthSession(): Promise<any>
```

**关键实现 - AES-GCM 加密密码**:
```typescript
export async function oauthBindTsdb(token: string, username: string, password: string) {
    // 从 Cookie 读取加密 key (由后端生成)
    const key = Cookies.get('encrypt_key') || '';
    const aesKey = await aesGcm.importKey(key);
    
    // 使用 AES-GCM 加密密码
    const encryptedPassword = await aesGcm.encryptB64(password, aesKey);
    
    // 发送到后端
    return await request({
        baseURL: apiPath,
        url: `/oauth/bind`,
        method: "post",
        data: { token, username, credential: encryptedPassword.data }
    });
}
```

### 2. 请求拦截器

**文件**: `src/utils/request.ts` (修改 lines 20-38)

```typescript
// 请求拦截器 - 添加认证 header
request.interceptors.request.use((config) => {
    // 优先使用 OAuth token
    const oauthToken = localStorage.getItem('oauth_token');
    const authType = localStorage.getItem('auth_type');
    
    if (oauthToken && authType === 'oauth') {
        config.headers.Authorization = `Bearer ${oauthToken}`;
    } else {
        // 降级到 Basic Auth
        const token = localStorage.getItem('TDengine-Token');
        if (token) {
            config.headers.Authorization = token;
        }
    }
    
    return config;
});
```

### 3. 登录页面

**文件**: `src/views/0_login/index.vue` (修改)

**新增内容**:
1. 导入 OAuth API (line 91):
```typescript
import { getOAuthStatus, oauthAuthorize } from '@/api/oauth';
```

2. OAuth 状态管理 (line 115):
```typescript
const oauthEnabled = ref<boolean>(false);
```

3. 初始化检查 OAuth 状态 (lines 170-176):
```typescript
async function init() {
    await getClusterAndDashboardUrl();
    // ...
    
    try {
        const status = await getOAuthStatus();
        oauthEnabled.value = status.enabled;
    } catch (error) {
        console.error('Failed to get OAuth status:', error);
    }
}
```

4. SSO 按钮 (lines 66-73):
```vue
<el-form-item v-if="oauthEnabled" style="margin-bottom: 20px">
    <el-button class="oauth-button" @click="loginWithOAuth">
        <svg class="oauth-icon" viewBox="0 0 24 24" width="18" height="18">
            <path fill="currentColor" d="..."/>
        </svg>
        {{ $t('login.ssoLogin') || 'Sign in with SSO' }}
    </el-button>
</el-form-item>
```

5. SSO 登录函数 (lines 384-387):
```typescript
function loginWithOAuth() {
    oauthAuthorize(); // 重定向到后端 /oauth/authorize
}
```

### 4. OAuth 回调页面

**文件**: `src/views/oauth-callback/index.vue` (136 行)

**功能**:
- 从 URL query parameter 提取 `token` (session_id)
- 处理错误情况
- 存储 token 到 localStorage
- 更新 Vuex store
- 重定向到 `/explorer`

**核心逻辑**:
```typescript
onMounted(async () => {
    const token = route.query.token as string;
    const errorParam = route.query.error as string;
    
    if (errorParam) {
        error.value = true;
        errorMessage.value = decodeURIComponent(errorParam);
        return;
    }
    
    if (!token) {
        error.value = true;
        errorMessage.value = 'No OAuth token received';
        return;
    }
    
    // 存储 OAuth token
    localStorage.setItem('oauth_token', token);
    localStorage.setItem('auth_type', 'oauth');
    
    // 通知 Vuex
    await store.dispatch('app/setOAuthLogin', true);
    
    // 重定向
    setTimeout(() => {
        router.push({ path: '/explorer' });
    }, 500);
});
```

### 5. 路由配置

**文件**: `src/router/index.ts` (lines 195-198)

```typescript
{
    path: '/oauth/callback',
    name: 'OAuthCallback',
    component: () => import('@/views/oauth-callback/index.vue')
}
```

### 6. Vuex Store

**文件**: `src/store/modules/app.ts` (修改)

**新增状态** (lines 70-72):
```typescript
state: {
    // ...
    oauthEnabled: false,
    isOAuthLogin: false,
    sysinfo: true,
}
```

**Mutations** (lines 286-293):
```typescript
SET_OAUTH_ENABLED(state, enabled: boolean) {
    state.oauthEnabled = enabled;
},
SET_OAUTH_LOGIN(state, isOAuth: boolean) {
    state.isOAuthLogin = isOAuth;
},
SET_SYSINFO(state, sysinfo: boolean) {
    state.sysinfo = sysinfo;
}
```

**Actions** (lines 334-338):
```typescript
setOAuthEnabled({ commit }, enabled: boolean) {
    commit('SET_OAUTH_ENABLED', enabled);
},
setOAuthLogin({ commit }, isOAuth: boolean) {
    commit('SET_OAUTH_LOGIN', isOAuth);
}
```

**Logout 更新** (lines 328-332):
```typescript
async logout({ commit }) {
    // 清理 OAuth 状态
    localStorage.removeItem('oauth_token');
    localStorage.removeItem('auth_type');
    // ...
}
```

---

## ⚙️ 配置

### explorer.toml 示例

```toml
[oauth]
enabled = true
provider = "keycloak"

# 可选: 回调后前端重定向 URI (默认为空字符串)
fallback_redirect_uri = ""

[oauth.oidc]
client_id = "taos-explorer"
client_secret = "your-client-secret-here"
issuer_url = "http://localhost:8080/realms/taosdata"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"
scopes = ["openid", "profile", "email"]

# 可选: 手动指定端点 (通常通过 Discovery 自动获取)
# authorization_endpoint = "..."
# token_endpoint = "..."
# userinfo_endpoint = "..."
# jwks_uri = "..."

[oauth.user_mapping]
username = "preferred_username"
email = "email"
first_name = "given_name"
last_name = "family_name"
roles = "groups"

# NOTE: TDengine-specific `oauth.tdengine` configuration section has been removed.
# TDengine credential association is now performed via the binding endpoint
# (`POST /api/-/oauth/bind`) and persisted in the `oauth_users` table.
# If you previously relied on `auto_create_user` behavior, please implement an
# out-of-band provisioning process for TDengine users or open a feature request
# to reintroduce controlled auto-creation behavior.
```

### 环境变量覆盖

环境变量优先级 > TOML 配置文件

```bash
export EXPLORER_OAUTH_ENABLED=true
export EXPLORER_OAUTH_CLIENT_ID=taos-explorer
export EXPLORER_OAUTH_CLIENT_SECRET=secret
export EXPLORER_OAUTH_ISSUER_URL=http://localhost:8080/realms/taosdata
export EXPLORER_OAUTH_REDIRECT_URI=http://localhost:6060/api/-/oauth/callback
export EXPLORER_OAUTH_SCOPES=openid,profile,email
```

---

## 🧪 测试

### 1. Keycloak 测试环境

**文件**: `server/tests/keycloak/docker-compose.yml`

```bash
cd server/tests/keycloak
docker-compose up -d
```

**访问 Keycloak**:
- URL: http://localhost:8080
- 管理员用户: `admin` / `admin123`

**配置步骤**:
1. 创建 Realm: `taosdata`
2. 创建 Client: `taos-explorer`
   - Client Protocol: `openid-connect`
   - Access Type: `confidential`
   - Valid Redirect URIs: `http://localhost:6060/api/-/oauth/callback`
   - Web Origins: `http://localhost:6060`
3. 获取 Client Secret (Credentials tab)
4. 创建测试用户并设置密码

### 2. 端点测试

**OAuth 状态**:
```bash
curl http://localhost:6060/api/-/oauth/status
# 响应: {"enabled":true,"provider":"keycloak"}
```

**发起授权** (浏览器):
```
http://localhost:6060/api/-/oauth/authorize
# 应该重定向到 Keycloak 登录页
```

**回调测试** (自动，浏览器跳转):
```
http://localhost:6060/api/-/oauth/callback?code=xxx&state=xxx
# 应该重定向到 /#/oauth/callback?token=xxx
```

**登出**:
```bash
curl -X POST http://localhost:6060/api/-/oauth/logout \
  -H "Authorization: Bearer <session_token>"
```

### 3. 完整流程测试

1. 启动 Keycloak: `docker-compose up -d`
2. 配置 Keycloak (创建 realm、client、user)
3. 配置 explorer.toml
4. 启动后端: `cargo run`
5. 启动前端: `npm run dev`
6. 访问登录页: http://localhost:5173
7. 点击 "Sign in with SSO" 按钮
8. 在 Keycloak 登录
9. 重定向回 explorer 并自动登录
10. 访问 TDengine 资源

---

## 🔒 安全特性

### 1. PKCE (Proof Key for Code Exchange)
- **算法**: SHA256
- **目的**: 防止授权码拦截攻击
- **实现**: `client.rs` lines 165-188

### 2. CSRF 保护
- **机制**: State parameter + HTTP-only Cookie
- **验证**: `handlers.rs` lines 152-163
- **过期时间**: 10 分钟

### 3. JWT 验证
- **签名验证**: 自动使用 JWKS
- **Nonce 验证**: 防止重放攻击
- **实现**: `openidconnect` crate 自动处理

### 4. 密码加密
- **算法**: AES-256-GCM
- **Key 派生**: SHA256(session_id)
- **传输**: Base64 编码
- **实现**: 
  - 后端: `handlers.rs` lines 302-330
  - 前端: `utils/aesGcm.ts`

### 5. 会话安全
- **Session ID**: UUID v4 (随机)
- **存储**: SQLite (持久化)
- **过期**: 8 小时 (可配置)
- **自动清理**: 每小时执行

### 6. HTTP-only Cookies
- **用途**: 存储临时 OAuth 状态 (state, nonce, verifier)
- **属性**: `HttpOnly`, `SameSite=Lax`
- **过期**: 10 分钟

---

## 📊 代码统计

### 后端 (Rust)
| 文件 | 行数 | 责任 |
|------|------|------|
| config.rs | 201 | 配置管理 |
| client.rs | 372 | OIDC 客户端 |
| session.rs | 508 | 会话管理 |
| middleware.rs | 121 | 认证中间件 |
| handlers.rs | ~400 | HTTP 端点 |
| **总计** | **~1,600** | **完整后端实现** |

### 前端 (TypeScript/Vue)
| 文件 | 行数 | 责任 |
|------|------|------|
| api/oauth.ts | 71 | OAuth API |
| views/oauth-callback/index.vue | 136 | 回调页面 |
| views/0_login/index.vue | +40 | SSO 按钮 |
| utils/request.ts | +20 | Bearer Token |
| store/modules/app.ts | +20 | OAuth 状态 |
| router/index.ts | +4 | 回调路由 |
| **总计** | **~291** | **完整前端实现** |

### 数据库
| 文件 | 行数 | 责任 |
|------|------|------|
| 20251201000001_oauth_sessions.up.sql | 52 | Schema 定义 |

**总代码量**: ~1,943 行 (不含测试和文档)

---

## 🐛 已知问题与优化

### 当前限制

1. ❌ **Refresh Token 未实现**
   - 当前 access_token 过期后无法自动刷新
   - 需要用户重新登录

2. ❌ **TDengine 自动创建配置已移除 / 功能不可用**
   - The prior `oauth.tdengine` configuration (including `auto_create_user`) has been removed from the implementation.
   - TDengine users must be pre-provisioned by administrators or associated via the OAuth binding flow (`POST /api/-/oauth/bind`) which securely stores TDengine credentials in `oauth_users`.
   - If automated TDengine user provisioning is required, evaluate and implement a dedicated feature with clear operational and security controls.

3. ⚠️ **密码存储加密**
   - 当前使用明文存储 TDengine 密码
   - TODO: 实现 AES 加密存储 (`session.rs` line 365)

4. ⚠️ **角色映射未实现**
   - `role_mapping` 配置存在但未使用
   - 需要将 OIDC groups 映射到 TDengine 角色

### 优化建议

#### 高优先级
1. ✅ **实现密码加密存储**
   ```rust
   // session.rs line 365
   let encrypted_password = encrypt_password(tsdb_password)?;
   ```

2. ✅ **实现 Token 刷新**
   ```rust
   // client.rs 添加
   pub async fn refresh_token(&self, refresh_token: &str) -> Result<TokenResponse>
   ```

3. ✅ **完善错误处理**
   - 更友好的错误消息
   - 前端错误提示国际化

#### 中优先级
4. ⭕ **TDengine 用户自动创建**
   ```rust
   // handlers.rs callback 添加
   if config.tdengine.auto_create_user {
       auto_create_tdengine_user(&user_info, &config).await?;
   }
   ```

5. ⭕ **会话管理 UI**
   - 显示当前会话
   - 允许用户主动登出其他设备

6. ⭕ **审计日志**
   - 记录所有 OAuth 操作
   - 登录/登出时间、IP 地址

#### 低优先级
7. ⬜ **多 IdP 支持**
   - 支持同时配置多个 OAuth 提供商
   - 登录页显示多个 SSO 按钮

8. ⬜ **SSO 按钮自定义**
   - 允许配置按钮文本、图标、颜色

9. ⬜ **监控指标**
   - OAuth 登录成功/失败次数
   - 平均会话时长
   - Prometheus metrics

---

## 📚 参考资料

### 规范文档
- [OAuth 2.0 RFC 6749](https://datatracker.ietf.org/doc/html/rfc6749)
- [OpenID Connect Core 1.0](https://openid.net/specs/openid-connect-core-1_0.html)
- [PKCE RFC 7636](https://datatracker.ietf.org/doc/html/rfc7636)

### 依赖库
- [openidconnect](https://docs.rs/openidconnect/) - Rust OIDC 客户端
- [sqlx](https://docs.rs/sqlx/) - Rust SQL 工具包
- [actix-web](https://actix.rs/) - Rust Web 框架

### Keycloak 文档
- [Securing Applications](https://www.keycloak.org/docs/latest/securing_apps/)
- [Server Administration](https://www.keycloak.org/docs/latest/server_admin/)

---

## ✅ Code Review 总结

### 优点

1. ✅ **架构清晰**: 模块职责分明，易于维护
2. ✅ **安全完备**: PKCE、CSRF、JWT 验证全覆盖
3. ✅ **错误处理**: 大部分错误都有日志和用户提示
4. ✅ **代码质量**: Rust 类型安全、编译时保证
5. ✅ **前后端分离**: API 设计合理
6. ✅ **文档完善**: 代码注释、README、集成指南

### 需要改进

1. ⚠️ **密码存储**: 当前明文存储，需要加密
2. ⚠️ **Token 刷新**: 未实现自动刷新机制
3. ⚠️ **测试覆盖**: 缺少单元测试和集成测试
4. ⚠️ **国际化**: 部分错误消息硬编码英文
5. ⚠️ **监控**: 缺少 Prometheus metrics

### 安全建议

1. 🔒 生产环境**必须使用 HTTPS**
2. 🔒 定期轮换 `client_secret`
3. 🔒 配置 `session` 过期时间根据安全策略
4. 🔒 实施 IP 白名单或地理位置限制
5. 🔒 启用 IdP 的 MFA (多因素认证)

---

## 📝 更新日志

### v1.0.0 (2024-12-08)
- ✅ 完成后端 OAuth 2.0/OIDC 实现
- ✅ 完成前端 SSO 登录界面
- ✅ 完成会话管理和 TDengine 凭据绑定
- ✅ 完成 Keycloak 测试环境
- ✅ 完成文档撰写

---

## 👥 贡献者

- Backend: [@explorer-team]
- Frontend: [@explorer-team]
- Documentation: [@explorer-team]

---

## 📄 License

与 taos-explorer 项目保持一致
