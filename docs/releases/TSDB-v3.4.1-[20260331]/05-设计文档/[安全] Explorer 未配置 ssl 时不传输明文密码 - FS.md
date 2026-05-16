# [安全] Explorer 未配置 ssl 时不传输明文密码 - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-07 | 2026-01-07 | 1.0 | Linhe Huo | 初始版本 |

## 2. 背景

当前 TDengine Explorer 使用客户端可读取的 Cookie 存储认证 Token，存在安全风险。为提高系统安全性，需要实现基于服务端会话的认证机制，使用 HttpOnly Cookie 存储会话 ID，并对密码进行时间限制加密传输。
本优化目标：
1. 消除客户端 Token 存储的安全隐患
2. 实现密码加密传输
3. 统一 OAuth 和 Basic Auth 的会话管理

## 3. 定义

- **session_id**: 服务端生成的会话标识符，存储在 HttpOnly Cookie 中
- **TimeBasedXor**: 基于时间戳的 XOR 加密算法，用于密码的短期加密传输
- **encrypt_key**: 用于加密 TSDB 密码的 AES 密钥，存储在客户端可读 Cookie 中
- **Self-provided User**: 通过 Basic Auth 创建的本地用户，provider 为 `__self__`

## 4. 行为说明

### 4.1 登录流程变化

#### 4.1.1 前端登录请求

**原有行为**：
```typescript
// 使用 Basic Auth Header
headers: {
  Authorization: 'Basic ' + base64(username + ':' + password)
}
```

**新行为**：
```typescript
// 使用 JSON Body 传递加密密码
import { TimeBasedXor } from '@/utils/timeBasedXor';

const xor = new TimeBasedXor(60); // 60秒有效期
const encrypted_password = xor.encrypt(password);

await request({
  url: '/api/-/login',
  method: 'post',
  data: {
    username,
    encrypted_password,
    sql: 'select server_version()'
  }
});
```

**加密算法示例**：
```typescript
// 密码加密过程
const timestamp = Math.floor(Date.now() / 1000);
const key = generateKey(timestamp); // 16字节固定密钥 + 8字节时间戳
const encrypted = xorEncrypt(key, password);
const result = `${timestamp}.${base64(encrypted)}`;
```

#### 4.1.2 后端登录处理

**新增配置**：
```rust
// 在 main.rs 中配置加密密钥
litcrypt::use_litcrypt!("AeRohyohKee4saih9se7cu6ieHagh1ko");
```

**登录接口变化**：
```rust
#[derive(Debug, Deserialize)]
struct LoginBody {
    username: String,
    encrypted_password: String,
}

async fn login(
    db: web::Data<Storage>,
    session_manager: web::Data<SessionManager>,
    body: web::Json<LoginBody>,
) -> impl Responder {
    // 1. 解密密码（60秒有效期）
    const XOR_DECODER: TimeBasedXor = TimeBasedXor::new(60);
    let password = XOR_DECODER.decrypt(&body.encrypted_password)?;
    
    // 2. 创建认证凭证
    let auth = TsdbCredential::basic(body.username, password);
    
    // 3. 验证数据库连接
    let result = execute_sql(&auth, "select server_version()").await?;
    
    // 4. 创建会话
    let session = session_manager
        .create_self_provided_session(&auth, Some(3600))
        .await?;
    
    // 5. 设置 HttpOnly Cookie
    let session_cookie = Cookie::build("session_id", session.session_id())
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .max_age(Duration::seconds(3600))
        .finish();
    
    HttpResponse::Ok()
        .cookie(session_cookie)
        .json(ok)
}
```

**错误处理**：
- `[0x0357]` 错误码（认证失败）返回 401 Unauthorized
- 密码过期或解密失败返回 401 Unauthorized
- 其他错误返回 500 Internal Server Error

### 4.2 新增 API 端点

#### 4.2.1 获取当前用户信息

**端点**: `GET /api/-/me` 或 `GET /api/-/oauth/me`
**请求**：
```bash
curl -X GET https://explorer.example.com/api/-/me \
  --cookie "session_id=<session_id>"
```

**响应**（成功 200）：
```json
{
  "user_id": 12345,
  "email": "user@example.com",
  "username": "oauth_user",
  "tsdb_username": "root",
  "tsdb_password": "<encrypted_password>",
  "provider": "google",
  "is_self_provided": false
}
```

**响应**（未登录 401）：
```json
{
  "code": 401,
  "desc": "Session not found"
}
```

#### 4.2.2 登出

**端点**: `POST /api/-/logout` 或 `POST /api/-/oauth/logout`
**请求**：
```bash
curl -X POST https://explorer.example.com/api/-/logout \
  --cookie "session_id=<session_id>"
```

**响应**（成功 200）：
```json
{
  "code": 0,
  "desc": "Logged out successfully"
}
```

### 4.3 前端状态管理变化

#### 4.3.1 移除 Token 管理

**移除的函数**：
- `removeToken()` - 不再需要移除客户端 Cookie Token
- `setLoginSign()` - 不再使用登录标志位
- `isLogin()` - 不再通过 Cookie 判断登录状态
- `clearLoginStateWhenReopen()` - 不再需要清理登录状态
- `deleteCookieItem()` - 不再需要删除 TDengine-Token Cookie

#### 4.3.2 会话状态管理

**新增状态**：
```typescript
// store/modules/app.ts
state: {
  loginWithSession: false, // 标识使用会话登录
  // ... 其他状态
}

mutations: {
  SET_LOGIN_WITH_SESSION: (state, loginWithSession) => {
    state.loginWithSession = loginWithSession;
  }
}
```

#### 4.3.3 路由守卫变化

**新的认证逻辑**：
```typescript
// permission.ts
router.beforeEach(async (to, from, next) => {
  // 检查会话状态
  if (!store.state.app.loginWithSession) {
    // 尝试从服务端获取会话信息
    const user = await oauthMe(false);
    if (user.tsdb_username) {
      // 恢复会话状态
      await store.dispatch('app/setLoginWithSession', true);
      return next();
    }
    // 无会话，跳转登录页
    next('/login');
  }
});
```

#### 4.3.4 请求拦截器变化

**原有行为**：
```typescript
// 根据 OAuth 状态决定是否添加 Authorization Header
if (isOAuthLogin) {
  config.withCredentials = true;
} else if (hasToken) {
  headers['Authorization'] = hasToken;
}
```

**新行为**：
```typescript
// 所有请求都携带 Cookie
config.withCredentials = true;
```

### 4.4 会话管理数据库变化

#### 4.4.1 会话查询变化

添加 `provider` 字段，使用 `__self__` 标识用户名密码认证。
**原有查询**：
```sql
SELECT user_id, username, tsdb_username, tsdb_password, email, created_at, updated_at
FROM oauth_users
WHERE ...
```

**新查询**：
```sql
SELECT user_id, username, provider, tsdb_username, tsdb_password, email, created_at, updated_at
FROM oauth_users
WHERE ...
```

### 4.5 配置参数

无新增配置参数。使用固定的会话过期时间 3600 秒（1 小时）。

## 5. 性能

- **登录性能**: 增加了 XOR 解密和会话创建操作，预计增加 5-10ms 延迟
- **请求性能**: 每次请求需要验证会话，增加数据库查询，预计增加 2-5ms 延迟
- **优化措施**: 
  - 对 session_id 字段建立索引
  - 可考虑使用 Redis 缓存会话数据（未来优化）

## 6. 安全

### 6.1 防御 XSS 攻击

- **HttpOnly Cookie**: session_id 使用 HttpOnly 标记，JavaScript 无法读取
- **加密密钥混淆**: 使用 litcrypt 库混淆硬编码密钥

### 6.2 防御 CSRF 攻击

- **SameSite Cookie**: 使用 SameSite=Lax 属性，限制跨站请求

### 6.3 防御重放攻击

- **时间限制加密**: 加密密码仅在 60 秒内有效
- **时间戳验证**: 服务端验证时间戳有效性

### 6.4 数据机密性

- **密码不明文传输**: 密码在前端加密后传输
- **会话加密存储**: TSDB 密码使用 AES-CBC-MAC 加密存储

### 6.5 会话管理

- **会话过期**: 会话 1 小时后自动过期
- **主动登出**: 用户可主动登出并清理会话

## 7. 兼容性

### 7.1 破坏性变化

1. **登录 API 变化**: `/api/-/login` 接口不再接受 Basic Auth Header，必须使用 JSON Body
  - **影响：**新旧版本 taos-explorer 后端不兼容，仅影响开发侧，不影响生产
1. **Cookie 名称变化**: 不再使用 `TDengine-Token` Cookie，改用 `session_id`
  - **影响**: 旧版本的 Cookie 将失效
  - **迁移方案**: 用户需要重新登录

### 7.2 向后兼容

- OAuth 登录流程保持不变
- `/api/-/oauth/me` 和 `/api/-/oauth/logout` 端点保留，映射到新端点
- 用户数据库表结构向后兼容，仅查询字段修改

## 8. 运维

### 8.1 会话清理

定期清理过期会话：
```sql
DELETE FROM oauth_sessions WHERE expires_at < NOW();
```

在服务启动时已配置定时任务执行清理，用户侧不需要关注。

### 8.2 监控建议

- 监控会话创建速率
- 监控登录失败率（区分密码错误和解密失败）
- 监控会话表大小

## 9. 使用场景

### 9.1 场景 1: 新用户首次登录

1. 用户访问登录页面
2. 输入用户名和密码
3. 前端使用 TimeBasedXor 加密密码
4. 发送 JSON 请求到 `/api/-/login`
5. 后端验证密码并创建会话
6. 返回 session_id Cookie
7. 前端跳转到主页

### 9.2 场景 2: 已登录用户刷新页面

1. 用户刷新页面
2. 调用 `/api/-/me` 检查会话
3. 后端验证 session_id Cookie
4. 返回用户信息
5. 前端恢复登录状态

### 9.3 场景 3: 用户主动登出

1. 用户点击登出按钮
2. 前端调用 `/api/-/logout`
3. 后端删除会话记录
4. 清除 session_id Cookie
5. 前端清理状态并跳转到登录页

### 9.4 场景 4: 会话过期

1. 用户会话超过 1 小时
2. 用户发起请求
3. 后端验证会话失败，返回 401
4. 前端拦截器捕获 401，清理状态
5. 跳转到登录页

### 9.5 场景 5: OAuth 用户登录

1. 用户通过 OAuth 登录
2. 后端创建 OAuth 会话（provider 为 OAuth 提供商）
3. 返回 session_id 和 encrypt_key Cookie
4. 前端正常使用会话

## 10. 约束和限制

### 10.1 约束

1. 必须启用 Cookie 才能使用本功能
2. 前端和后端时钟偏差不能超过 60 秒（影响密码解密）
3. 会话依赖数据库存储，数据库不可用时无法创建/验证会话

### 10.2 限制

1. 会话过期时间固定为 1 小时，暂不支持自定义
2. 密码加密仅防御被动监听，不防御主动中间人攻击（需 HTTPS）

## 11. 常见错误和排查

### 11.1 错误 1: "Invalid password"

**原因**：密码解密失败
**排查**：
1. 检查前端和后端时间是否同步
2. 确认加密密钥一致（litcrypt 密钥）
3. 查看前端加密实现是否正确

### 11.2 错误 2: "Session not found"

**原因**：会话不存在或已过期
**排查**：
1. 检查 Cookie 是否携带 session_id
2. 查询数据库确认会话是否存在
3. 确认会话未超过 1 小时

### 11.3 错误 3: "Time-based xor decoding expired"

**原因**：加密密码超过 60 秒
**排查**：
1. 检查前端和后端时钟偏差
2. 确认网络延迟是否过高
3. 考虑适当增加有效期（需权衡安全性）

### 11.4 错误 4: 登录后立即被登出

**原因**：Cookie 未正确设置或浏览器阻止 Cookie
**排查**：
1. 检查浏览器 Cookie 设置
2. 确认 SameSite 属性配置正确
3. 确认域名和路径匹配

## 12. 可观测性

### 12.1 taosExplorer

1. **登录页面**: 无明显变化，用户无感知
2. **用户信息**: `/api/-/me` 返回 `is_self_provided` 字段，区分用户类型
3. **错误提示**: 登录失败时显示具体错误信息（密码错误、服务异常等）

### 12.2 日志记录

新增日志：
- 会话创建：`Created basic auth session: {session_id}`
- 会话创建失败：`Failed to create basic auth session: {error}`
- 认证失败：`Failed to authenticate user: {error}`
- 密码解密失败：`Invalid login: {username}`

## 13. 安装和卸载

无变化。

## 14. 文档

用户侧无感知，无需修改文档。

## 15. 参考文档

- OAuth 2.0 RFC 6749: https://datatracker.ietf.org/doc/html/rfc6749
- OWASP Session Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html
- MDN HttpOnly Cookie: https://developer.mozilla.org/en-US/docs/Web/HTTP/Cookies#restrict_access_to_cookies

## 16. 附录

### 16.1 A. TimeBasedXor 实现细节

#### 16.1.1 Rust 实现

密钥生成：
```rust
fn gen_key(timestamp: i64) -> [u8; 24] {
    let mut key = [0; 24];
    let prefix = litcrypt::lc!("taosdataexplorer"); // 16字节
    key[0..16].copy_from_slice(prefix.as_bytes());
    key[16..24].copy_from_slice(&timestamp.to_be_bytes());
    key
}
```

解密：
```rust
pub fn decrypt(&self, data: &str) -> Result<String, XorError> {
    let (timestamp, encrypted_data) = data.split_once('.').ok_or(XorError::InvalidData)?;
    let timestamp = timestamp.parse::<i64>()?;
    
    let current_time = Local::now().timestamp();
    if current_time - timestamp > self.allowed_duration_in_seconds as i64 {
        return Err(XorError::Expired);
    }
    
    let bytes = BASE64_STANDARD.decode(encrypted_data)?;
    let key = Self::gen_key(timestamp);
    let decrypted = decrypt_xor(&key, &bytes);
    String::from_utf8(decrypted).map_err(Into::into)
}
```

#### 16.1.2 TypeScript 实现（客户端）

密钥生成：
```typescript
private static genKey(timestamp: number): Uint8Array {
  const key = new Uint8Array(24);
  const prefix = new TextEncoder().encode('taosdataexplorer'); // 16字节
  key.set(prefix, 0);
  
  const timestampBuffer = new ArrayBuffer(8);
  const timestampView = new DataView(timestampBuffer);
  timestampView.setBigUint64(0, BigInt(timestamp), false); // 大端序
  key.set(new Uint8Array(timestampBuffer), 16);
  
  return key;
}
```

加密：
```typescript
public encrypt(data: string): string {
  const timestamp = Math.floor(Date.now() / 1000);
  const key = TimeBasedXor.genKey(timestamp);
  const dataBytes = Buffer.from(data);
  const encrypted = this.encryptXor(key, dataBytes);
  const encryptedData = Buffer.from(encrypted).toString('base64');
  return `${timestamp}.${encryptedData}`;
}
```

### 16.2 B. 会话数据结构

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthSession {
    #[serde(flatten)]
    pub user: OAuthUser,
    pub session_id: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthUser {
    pub user_id: i64,
    pub username: String,
    pub provider: String,
    pub tsdb_username: Option<String>,
    pub tsdb_password: Option<String>,
    pub email: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```
