# OAuth Token 自动刷新功能

## 📋 概述

实现了 OAuth 2.0 access token 自动刷新功能，在 token 即将过期时（5分钟内）自动使用 refresh_token 获取新的 access_token，避免用户频繁重新登录。

## 🔑 核心功能

### 1. 自动检测 Token 过期
- **检测时机**: 每次 Bearer token 认证时
- **过期阈值**: Token 剩余有效期 < 5 分钟
- **检测位置**: `middleware.rs` 中的 `extract_auth` 函数

### 2. 自动刷新流程
```
用户请求 → 验证 Bearer Token → 检查过期 → 即将过期? 
                                              ↓ Yes
                                    使用 refresh_token 获取新 token
                                              ↓
                                    更新数据库中的 token 信息
                                              ↓
                                    使用新 token 完成请求
```

### 3. 实现细节

#### 数据库变更
新增字段: `oauth_sessions.access_token_expires_at` (timestamp)
- 记录 access_token 的过期时间
- 用于判断是否需要刷新

#### 新增方法

**client.rs**:
```rust
/// 刷新 access token
pub async fn refresh_access_token(
    &self,
    refresh_token: &str,
) -> Result<(String, Option<String>, Option<i64>)>
```

**session.rs**:
```rust
/// 刷新会话的 access token
pub async fn refresh_session_token(
    &self,
    session_id: &str,
    new_access_token: &str,
    new_refresh_token: Option<&str>,
    expires_in_seconds: Option<i64>,
) -> Result<()>

/// 检查 token 是否即将过期（5分钟内）
pub fn is_access_token_expiring_soon(&self, session: &OAuthSession) -> bool
```

**middleware.rs**:
- 更新 `extract_auth` 函数添加自动刷新逻辑
- 接受 `oidc_client` 参数用于调用刷新 API

## 🧪 测试方法

### 方法 1: 修改过期阈值测试

修改 `session.rs` 中的过期检查阈值（临时用于测试）:

```rust
// session.rs line 529 附近
pub fn is_access_token_expiring_soon(&self, session: &OAuthSession) -> bool {
    if let Some(expires_at) = session.details.access_token_expires_at {
        let now = chrono::Utc::now();
        // 改为 30 分钟用于测试（原值 5 分钟）
        let threshold = now + chrono::Duration::minutes(30);
        expires_at < threshold
    } else {
        false
    }
}
```

**测试步骤**:
1. 配置 Keycloak access token lifetime 为 5 分钟
2. 修改阈值为 30 分钟
3. OAuth 登录
4. 立即发起 API 请求（任意需要认证的 API）
5. 查看后端日志，应该看到 "Access token expiring soon" 和 "Successfully refreshed access token"

### 方法 2: 配置短期 Token 测试

**配置 Keycloak**:
1. 进入 Keycloak Admin Console
2. Clients → taos-explorer → Settings
3. Advanced Settings:
   - Access Token Lifespan: `2 minutes` (2分钟)
   - Client Session Idle: `5 minutes`
   - Client Session Max: `10 minutes`
4. Save

**测试步骤**:
1. 启动 explorer 后端和前端
2. OAuth 登录成功
3. 等待 1 分钟（token 还有 1 分钟过期）
4. 在前端执行任意操作触发 API 请求
5. 查看后端日志:
```log
[INFO] Access token expiring soon for session xxx, attempting refresh
[INFO] Successfully refreshed access token for session xxx
```
6. 继续使用，验证不需要重新登录

### 方法 3: 直接测试 Token 刷新 API

使用工具（如 Postman）直接测试刷新逻辑:

```bash
# 1. OAuth 登录获取 session_token (从回调 URL 提取)

# 2. 等待 token 即将过期（或修改阈值）

# 3. 发起需要认证的请求
curl -X GET http://localhost:6060/api/-/profile \
  -H "Authorization: Bearer YOUR_SESSION_TOKEN"

# 4. 检查日志确认刷新成功
```

### 方法 4: 数据库验证

**查询 token 过期时间**:
```sql
SELECT 
    session_id,
    user_id,
    access_token_expires_at,
    expires_at,
    datetime(access_token_expires_at) as token_expires,
    datetime(expires_at) as session_expires,
    datetime('now') as now
FROM oauth_sessions
WHERE session_id = 'YOUR_SESSION_ID';
```

**刷新后验证**:
```sql
-- 刷新前记录 access_token 前10个字符
SELECT substr(access_token, 1, 10) as token_prefix FROM oauth_sessions 
WHERE session_id = 'YOUR_SESSION_ID';

-- 触发刷新

-- 刷新后验证 token 已更新
SELECT substr(access_token, 1, 10) as token_prefix FROM oauth_sessions 
WHERE session_id = 'YOUR_SESSION_ID';
-- token_prefix 应该不同
```

## 🔍 调试技巧

### 1. 启用详细日志

在 `explorer.toml` 或环境变量中设置:
```toml
[log]
level = "debug"
```

### 2. 关键日志输出

**刷新开始**:
```log
[INFO] Access token expiring soon for session {session_id}, attempting refresh
```

**刷新成功**:
```log
[INFO] Successfully refreshed access token
[INFO] Successfully refreshed access token for session {session_id}
```

**刷新失败**:
```log
[WARN] Failed to refresh access token: {error}
```

**无 refresh_token**:
```log
[WARN] Access token expiring but no refresh token available
```

**无 OIDC client**:
```log
[WARN] Access token expiring but OIDC client not available
```

### 3. 验证刷新逻辑是否执行

在 `middleware.rs` 的 `extract_auth` 函数中添加临时日志:

```rust
// Line 79 附近
if session_mgr.is_access_token_expiring_soon(&session) {
    tracing::info!("DEBUG: Token expires at: {:?}", session.details.access_token_expires_at);
    tracing::info!("DEBUG: Has refresh token: {}", session.details.refresh_token.is_some());
    tracing::info!("DEBUG: Has OIDC client: {}", oidc_client.is_some());
    // ...
}
```

## 📊 性能考虑

### 1. 刷新频率
- **阈值**: 5 分钟内过期才刷新
- **影响**: 如果 token lifetime = 1 小时，约每 55 分钟刷新一次
- **并发**: 多个并发请求可能触发多次刷新（可以优化为加锁）

### 2. 刷新失败处理
- **策略**: 失败后继续使用旧 token
- **原因**: 旧 token 可能还有效，避免立即中断用户会话
- **日志**: WARN 级别记录失败原因

### 3. 数据库负载
- **更新频率**: 每次刷新写入一次数据库
- **查询频率**: 每次 Bearer token 认证读取一次
- **优化建议**: 可以考虑添加内存缓存（Redis）

## ⚙️ 配置选项

### 1. 修改过期检查阈值

编辑 `session.rs`:
```rust
pub fn is_access_token_expiring_soon(&self, session: &OAuthSession) -> bool {
    if let Some(expires_at) = session.details.access_token_expires_at {
        let now = chrono::Utc::now();
        let threshold = now + chrono::Duration::minutes(5); // 修改这里
        expires_at < threshold
    } else {
        false
    }
}
```

### 2. 修改 Session 过期时间

编辑 `handlers.rs`:
```rust
// Line 225 附近
28800, // 8 hours = 8 * 3600 seconds
// 修改为其他值，如 24 小时 = 86400
```

### 3. Keycloak Token Lifetime 配置

**推荐配置**:
- Access Token Lifespan: `5-15 minutes` (生产环境)
- Refresh Token Lifespan: `30 days`
- SSO Session Idle: `30 minutes`
- SSO Session Max: `10 hours`

**测试配置**:
- Access Token Lifespan: `2 minutes`
- Refresh Token Lifespan: `10 minutes`

## 🐛 常见问题

### Q1: Token 没有自动刷新

**可能原因**:
1. IdP 没有返回 `expires_in` → 数据库中 `access_token_expires_at` 为 NULL
2. IdP 没有返回 `refresh_token`
3. OIDC client 未正确注册到 actix-web app_data
4. Token 过期时间 > 5 分钟（还未到刷新阈值）

**排查方法**:
```sql
-- 检查 token 过期时间是否存储
SELECT access_token_expires_at FROM oauth_sessions WHERE session_id = 'xxx';

-- 检查是否有 refresh_token
SELECT refresh_token IS NOT NULL as has_refresh FROM oauth_sessions WHERE session_id = 'xxx';
```

### Q2: Token 刷新失败

**错误日志示例**:
```log
[WARN] Failed to refresh access token: Failed to refresh access token
```

**可能原因**:
1. refresh_token 已过期
2. IdP refresh token endpoint 不可用
3. Client secret 错误
4. refresh_token 已被撤销

**解决方法**:
- 检查 IdP 日志
- 验证 client credentials
- 用户重新登录获取新的 refresh_token

### Q3: 并发请求导致重复刷新

**现象**: 多个并发请求同时触发刷新，导致日志中出现多次 "Successfully refreshed"

**影响**: 一般无害，但会产生多余的 IdP 请求

**优化方案** (可选):
```rust
// 在 SessionManager 中添加刷新锁
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SessionManager {
    pool: SqlitePool,
    refresh_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

// 刷新前获取会话锁
let locks = self.refresh_locks.lock().await;
let session_lock = locks.entry(session_id.to_string())
    .or_insert_with(|| Arc::new(Mutex::new(())));
let _guard = session_lock.lock().await;
// 执行刷新...
```

## 📝 更新日志

### v1.1.0 (2024-12-08)
- ✅ 实现 access token 自动刷新
- ✅ 添加 `access_token_expires_at` 字段
- ✅ 支持在中间件中自动检测和刷新
- ✅ 添加数据库迁移
- ✅ 完整的错误处理和日志记录

---

## 📚 相关文档

- [OAUTH_IMPLEMENTATION.md](./OAUTH_IMPLEMENTATION.md) - 完整实现文档
- [OAUTH_README.md](./OAUTH_README.md) - 快速开始指南
- [OAUTH_INTEGRATION_GUIDE.md](./OAUTH_INTEGRATION_GUIDE.md) - 集成指南
