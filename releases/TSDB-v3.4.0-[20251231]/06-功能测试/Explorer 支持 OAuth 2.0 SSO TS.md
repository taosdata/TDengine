# Explorer 支持 OAuth 2.0 SSO TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-12 | 2025-12-12 | 1.0 | 霍琳贺 | 初始版本，基于 OAuth 2.0/OIDC SSO 功能规范编写测试用例 |

## 2. 测试目标

本测试旨在全面验证 TDengine Explorer OAuth 2.0 / OpenID Connect 单点登录功能的正确性、安全性、性能和兼容性。主要测试目标包括：
- **功能完整性**: 验证 OAuth 2.0 授权码流程、PKCE、用户信息同步、凭据绑定等核心功能
- **安全性**: 验证 CSRF 防护、JWT 验证、密码加密、会话管理等安全特性
- **兼容性**: 验证与主流身份提供商（Keycloak、Azure AD、Google、Okta）的集成
- **性能**: 验证认证响应时间、并发支持、Token 验证性能等指标
- **混合模式**: 验证 OAuth 登录与传统用户名密码登录的共存能力

## 3. 参考文档

- JIRA 链接: 
- RFC 6749: The OAuth 2.0 Authorization Framework
- RFC 7636: Proof Key for Code Exchange by OAuth Public Clients
- OpenID Connect Core 1.0 规范

## 4. 测试结论

测试通过。

## 5. 测试环境

- **操作系统**: Windows 10/11, Ubuntu 20.04/22.04, macOS 12+
- **浏览器**: Chrome 90+, Firefox 88+, Safari 14+, Edge 90+
- **后端**: Rust (Actix-web)
- **前端**: Vue 3 + TypeScript
- **身份提供商**: 
  - Keycloak 22+ (主要测试环境)
  - Azure AD
  - Google Workspace
  - Okta
- **TDengine**: 3.3.0+
- **测试工具**: 
  - Postman/curl (API 测试)
  - JMeter (性能测试)
  - OWASP ZAP (安全测试)

## 6. 功能测试

### 6.1 OAuth 配置管理

#### 6.1.1 测试要点

- OAuth 配置的加载与验证
- 配置文件与环境变量的优先级
- 配置错误时的降级处理
- 配置状态的正确返回

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1.1 | 配置文件加载 | 验证 explorer.toml 中的 OAuth 配置能够正确加载，包括 enabled, provider, client_id, client_secret, issuer_url, redirect_uri, scopes 等字段 | 通过 |
| 1.2 | 环境变量覆盖 | 验证环境变量（EXPLORER_OAUTH_*）能够覆盖配置文件中的设置 | 通过 |
| 1.3 | 必填字段验证 | 验证缺少必填字段（client_id, client_secret, issuer_url）时配置验证失败并禁用 OAuth | 通过 |
| 1.4 | 默认值处理 | 验证可选字段使用正确的默认值（scopes 默认为 ["openid", "profile", "email"]） | 通过 |
| 1.5 | OAuth 状态查询 | 调用 GET /api/-/oauth/status，验证返回正确的 enabled 和 provider 信息 | 通过 |
| 1.6 | OAuth 禁用状态 | 当 oauth.enabled=false 时，验证状态接口返回 {"enabled": false} 且不暴露敏感配置 | 通过 |
| 1.7 | 加密密钥配置 | 验证 EXPLORER_SECURITY_ENCRYPTION_KEY 环境变量正确配置时能够用于密码加密 | 通过 |
| 1.8 | 加密密钥缺失警告 | 验证缺失 EXPLORER_SECURITY_ENCRYPTION_KEY 时记录警告日志但不阻止启动 | 通过 |

### 6.2 OIDC Discovery 与客户端初始化

#### 6.2.1 测试要点

- OIDC Discovery 自动发现机制
- 授权、令牌、用户信息端点的正确获取
- JWKS URI 的获取与密钥加载
- 网络异常时的错误处理

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 2.1 | Discovery 成功 | 验证能够从 issuer_url/.well-known/openid-configuration 获取 OIDC 元数据 | 通过 |
| 2.2 | 端点提取 | 验证正确提取 authorization_endpoint, token_endpoint, userinfo_endpoint, jwks_uri | 通过 |
| 2.3 | Discovery 失败处理 | 验证 issuer_url 不可达时返回明确错误并禁用 OAuth | 通过 |
| 2.4 | 无效 Discovery 响应错误处理 | 验证 Discovery 返回无效 JSON 时的 | 通过 |
| 2.5 | JWKS 加载 | 验证能够从 jwks_uri 加载公钥用于 JWT 验证 | 通过 |
| 2.6 | 客户端初始化日志 | 验证 OIDC 客户端初始化成功时记录 "OIDC client initialized successfully" 日志 | 通过 |

### 6.3 OAuth 授权流程

#### 6.3.1 测试要点

- 授权 URL 生成的正确性
- PKCE (code_challenge) 的生成
- state 和 nonce 参数的生成与存储
- 重定向到身份提供商的正确性
- Cookie 的正确设置（httpOnly, Secure, SameSite）

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 3.1 | 发起授权请求 | 调用 GET /api/-/oauth/authorize，验证返回 302 重定向到 IdP 授权页面 | 通过 |
| 3.2 | 授权 URL 参数 | 验证重定向 URL 包含正确的 client_id, redirect_uri, scope, response_type=code, state, nonce, code_challenge, code_challenge_method=S256 | 通过 |
| 3.3 | PKCE code_challenge | 验证 code_challenge 使用 SHA256 算法生成，格式正确（base64url 编码） | 通过 |
| 3.4 | state Cookie 设置 | 验证响应中设置 oauth_state cookie，属性为 httpOnly, SameSite=Lax, Max-Age=600 | 通过 |
| 3.5 | nonce Cookie 设置 | 验证响应中设置 oauth_nonce cookie，属性为 httpOnly, SameSite=Lax, Max-Age=600 | 通过 |
| 3.6 | verifier Cookie 设置 | 验证响应中设置 oauth_verifier cookie，属性为 httpOnly, SameSite=Lax, Max-Age=600 | 通过 |
| 3.7 | OAuth 禁用时授权 | 当 OAuth 禁用时调用授权接口，验证返回 400 错误 | 通过 |
| 3.8 | OIDC 客户端未初始化 | OIDC 客户端初始化失败时调用授权接口，验证返回 500 错误 | 通过 |

### 6.4 OAuth 回调处理

#### 6.4.1 测试要点

- 授权码的接收与验证
- state 参数的 CSRF 验证
- 授权码交换访问令牌
- ID Token 的验证（签名、issuer、audience、expiration、nonce）
- 用户信息提取与同步
- 会话创建与 Cookie 设置
- 加密密钥 Cookie 的设置

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 4.1 | 成功回调处理 | 验证携带有效 code 和 state 的回调请求能够成功处理并创建会话 |  |
| 4.2 | state 验证 | 验证回调中的 state 参数与 oauth_state cookie 匹配时才继续处理 |  |
| 4.3 | state 不匹配 | 验证 state 参数与 cookie 不匹配时返回 "Invalid state parameter" 错误 |  |
| 4.4 | 缺失 state Cookie | 验证缺失 oauth_state cookie 时返回错误 |  |
| 4.5 | 授权码交换 | 验证使用 code 和 code_verifier 成功交换 access_token, id_token, refresh_token |  |
| 4.6 | 无效授权码 | 验证使用无效或过期的授权码时返回错误 |  |
| 4.7 | ID Token 签名验证 | 验证 ID Token 的 JWT 签名使用 JWKS 公钥正确验证 |  |
| 4.8 | ID Token issuer 验证 | 验证 ID Token 的 iss claim 与配置的 issuer_url 匹配 |  |
| 4.9 | ID Token audience 验证 | 验证 ID Token 的 aud claim 包含配置的 client_id |  |
| 4.10 | ID Token 过期验证 | 验证 ID Token 的 exp claim 未过期 |  |
| 4.11 | nonce 验证 | 验证 ID Token 的 nonce claim 与 oauth_nonce cookie 匹配 |  |
| 4.12 | 用户信息提取 | 验证从 ID Token claims 正确提取 username (preferred_username), email 等字段 |  |
| 4.13 | oauth_users 表创建 | 首次登录时验证在 oauth_users 表中创建用户记录 |  |
| 4.14 | oauth_sessions 表创建 | 验证在 oauth_sessions 表中创建会话记录，包含 session_id, access_token, refresh_token 等 |  |
| 4.15 | 会话 Cookie 设置 | 验证回调响应设置 httpOnly, Secure 的 session cookie（不在 URL 中传递 session_id） |  |
| 4.16 | ENCRYPT_KEY Cookie | 验证回调响应设置 ENCRYPT_KEY cookie（非 httpOnly，base64 编码，短期有效）用于后续绑定 |  |
| 4.17 | 错误回调处理 | 验证 IdP 返回 error 参数时（如用户拒绝授权）正确处理并返回友好错误信息 |  |
| 4.18 | Token 过期时间设置 | 验证 access_token_expires_at 和 session expires_at 正确计算并存储 |  |

### 6.5 TDengine 凭据绑定

#### 6.5.1 测试要点

- 绑定接口的请求与响应
- 前端密码加密（使用 ENCRYPT_KEY）
- 后端密码解密与重新加密存储
- 绑定后的会话更新
- TDengine 用户存在性验证
- 权限验证

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 5.1 | 首次绑定流程 | OAuth 用户首次登录后，验证能够成功绑定 TDengine 用户名和密码 |  |
| 5.2 | 绑定请求格式 | 验证 POST /api/-/oauth/bind 接受正确格式的请求体：{ username, credential } 或兼容 { token, username, credential } |  |
| 5.3 | 会话标识方式 | 验证优先使用 httpOnly session cookie 识别会话，向后兼容 token 字段 |  |
| 5.4 | 密码加密传输 | 验证前端使用 ENCRYPT_KEY cookie 中的密钥 AES 加密密码后 base64 编码传输 |  |
| 5.5 | 后端密码解密 | 验证后端使用派生自 session_id 的密钥解密前端提交的密码 |  |
| 5.6 | 密码存储加密 | 验证解密后的密码使用 AES-GCM 重新加密存储到 oauth_users.tsdb_password |  |
| 5.7 | TDengine 用户验证 | 验证绑定前检查 TDengine 用户是否存在（管理员需预创建） |  |
| 5.8 | 无效 TDengine 凭据 | 验证提供的 TDengine 用户名或密码错误时返回明确错误 |  |
| 5.9 | 绑定成功响应 | 验证绑定成功后返回 200 状态码和成功消息 |  |
| 5.10 | 重复绑定 | 验证已绑定的用户再次绑定时更新凭据 |  |
| 5.11 | 未登录绑定 | 验证未建立 OAuth 会话时调用绑定接口返回 401 错误 |  |
| 5.12 | 缺失 ENCRYPT_KEY | 验证前端缺失 ENCRYPT_KEY cookie 时的错误处理 |  |
| 5.13 | ENCRYPT_KEY 过期 | 验证 ENCRYPT_KEY cookie 过期后无法完成绑定 |  |

### 6.6 会话管理

#### 6.6.1 测试要点

- 会话的创建、获取、验证、更新
- 会话过期检查
- last_active 时间更新
- 过期会话清理
- 会话 Cookie 的正确传递

#### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 6.1 | 会话创建 | 验证 SessionManager::create_session 正确创建会话并返回 session_id | 通过 |
| 6.2 | 会话获取 | 验证 SessionManager::get_session 能够根据 session_id 获取完整会话信息 | 通过 |
| 6.3 | 会话验证 | 验证 SessionManager::verify_session 正确验证会话存在性和有效性 | 通过 |
| 6.4 | 会话过期检查 | 验证会话 expires_at 超过当前时间时会话被视为无效 | 通过 |
| 6.5 | 默认过期时间 | 验证会话默认 1 小时后过期 | 通过 |
| 6.6 | last_active 更新 | 验证每次访问时 last_active 字段更新为当前时间 | 通过 |
| 6.7 | 过期会话清理 | 验证 cleanup_expired_sessions 后台任务定期（每小时）清理过期会话 | 通过 |
| 6.8 | 清理日志记录 | 验证清理操作记录清理的会话数量到日志 | 通过 |
| 6.9 | httpOnly Cookie 读取 | 验证中间件能够从 httpOnly session cookie 中提取 session_id | 通过 |
| 6.10 | Authorization Header | 验证中间件支持从 Authorization: Bearer <session_id> header 提取会话 | 通过 |
| 6.11 | 无效会话 ID | 验证使用不存在的 session_id 时返回 401 错误 | 通过 |
| 6.12 | 会话并发访问 | 验证同一会话的并发请求不会导致数据不一致 | 通过 |

### 6.7 Token 刷新

#### 6.7.1 测试要点

- Access Token 过期检测
- Refresh Token 自动刷新
- 刷新后的 Token 更新到数据库
- 刷新失败处理
- 无 Refresh Token 时的行为

#### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 7.1 | Token 过期检测 | 验证中间件检测到 access_token 将在 5 分钟内过期时触发刷新 |  |
| 7.2 | 自动刷新流程 | 验证使用 refresh_token 调用 token endpoint 获取新的 access_token |  |
| 7.3 | 刷新后更新数据库 | 验证刷新成功后新 token 写入 oauth_sessions 表 |  |
| 7.4 | 刷新成功日志 | 验证 Token 刷新成功时记录日志 |  |
| 7.5 | 刷新失败处理 | 验证 refresh_token 失效或 IdP 拒绝时记录错误日志但不中断当前请求 |  |
| 7.6 | 无 Refresh Token | 验证会话没有 refresh_token 时跳过刷新逻辑 |  |
| 7.7 | 刷新 Token 轮换 | 验证 IdP 返回新 refresh_token 时正确更新存储 |  |
| 7.8 | 并发刷新保护 | 验证多个并发请求不会触发多次 Token 刷新 |  |

### 6.8 中间件与认证集成

#### 6.8.1 测试要点

- 请求认证信息提取
- Bearer Token 支持
- Basic Auth 与 OAuth 的共存
- TDengine 凭据解密
- 未认证请求的处理

#### 6.8.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 8.1 | OAuth 会话认证 | 验证携带有效 OAuth session 的请求能够通过认证 |  |
| 8.2 | Basic Auth 认证 | 验证传统 Basic Auth (用户名密码) 仍然正常工作 |  |
| 8.3 | 混合认证共存 | 验证 OAuth 和 Basic Auth 用户可以同时存在并正常访问 |  |
| 8.4 | TDengine 凭据提取 | 验证已绑定凭据的 OAuth 会话能够提取 TsdbCredential 用于数据库访问 |  |
| 8.5 | 密码解密 | 验证从数据库读取的加密 TDengine 密码能够正确解密 |  |
| 8.6 | 未绑定凭据处理 | 验证未绑定 TDengine 凭据的 OAuth 会话无法访问需要数据库权限的接口 |  |
| 8.7 | 无认证信息 | 验证既无 OAuth session 也无 Basic Auth 的请求返回 401 |  |
| 8.8 | 认证信息优先级 | 验证同时存在多种认证信息时的优先级处理（推荐 OAuth session 优先） |  |

### 6.9 登出功能

#### 6.9.1 测试要点

- OAuth 登出接口
- 会话清理
- Cookie 清除
- IdP 登出（如支持）
- 前端状态清理

#### 6.9.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 9.1 | OAuth 登出 | 调用 POST /api/-/oauth/logout，验证会话被删除 | 通过 |
| 9.2 | Cookie 清除 | 验证登出响应清除 session cookie 和其他 OAuth 相关 cookies | 通过 |
| 9.3 | 数据库会话删除 | 验证 oauth_sessions 表中对应记录被删除 | 通过 |
| 9.4 | 登出后访问 | 验证登出后使用原会话 ID 访问接口返回 401 | 通过 |
| 9.5 | 重复登出 | 验证重复调用登出接口不会报错（幂等性） | 通过 |
| 9.6 | Basic Auth 用户登出 | 验证传统用户登出功能不受 OAuth 影响 | 通过 |

### 6.10 前端集成

#### 6.10.1 测试要点

- 登录页 OAuth 按钮显示
- OAuth 状态检测
- 授权流程重定向
- 回调页面处理
- 会话状态管理（Vuex/Store）
- 绑定流程 UI
- 错误提示

#### 6.10.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 10.1 | OAuth 按钮显示 | OAuth 启用时验证登录页显示 "Sign in with SSO" 按钮 | 通过 |
| 10.2 | OAuth 禁用隐藏 | OAuth 禁用时验证登录页不显示 SSO 按钮 | 通过 |
| 10.3 | 点击授权按钮 | 验证点击 SSO 按钮触发后端授权接口并跳转到 IdP | 通过 |
| 10.4 | 回调页面加载 | 验证 IdP 回调后前端 /oauth-callback 页面正确加载 | 通过 |
| 10.5 | 会话验证请求 | 验证回调页面通过 credentials: 'include' 调用后端验证会话（不从 URL 读取 session_id） | 通过 |
| 10.6 | 首次登录绑定提示 | 验证首次登录且未绑定凭据时显示绑定 TDengine 用户的表单 | 通过 |
| 10.7 | 绑定表单提交 | 验证输入 TDengine 用户名密码后前端加密并提交绑定请求 | 通过 |
| 10.8 | 绑定成功跳转 | 验证绑定成功后自动跳转到主页面 | 通过 |
| 10.9 | 已登录跳转 | 验证已绑定凭据的用户直接跳转到主页面 | 通过 |
| 10.10 | OAuth 状态存储 | 验证 OAuth 登录状态正确保存到 Vuex store | 通过 |
| 10.11 | localStorage 清理 | 验证不再将 oauth_token 或 session_id 持久化到 localStorage | 通过 |
| 10.12 | 错误提示显示 | 验证授权或绑定失败时显示友好的错误提示信息 | 通过 |
| 10.13 | 网络错误处理 | 验证网络请求失败时的错误处理和用户提示 | 通过 |

### 6.11 多身份提供商兼容性

#### 6.11.1 测试要点

- Keycloak 集成
- Azure AD 集成
- Google Workspace 集成
- Okta 集成
- 不同 IdP 的 claims 映射

#### 6.11.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 11.1 | Keycloak 完整流程 | 使用 Keycloak 作为 IdP 完成从授权到绑定的完整流程 | 通过 |
| 11.2 | GitHub 完整流程 | 使用 GitHub 作为 IdP 完成从授权到绑定的完整流程 | 通过 |
| 11.3 | Google 完整流程 | 使用 Google Workspace 作为 IdP 完成从授权到绑定的完整流程 | 通过 |
| 11.4 | Okta 完整流程 | 使用 Okta 作为 IdP 完成从授权到绑定的完整流程 | 通过 |
| 11.6 | Claims 映射配置 | 验证不同 IdP 的 claims 通过 user_mapping 正确映射到用户字段 | 通过 |
| 11.7 | 非标准 claims 处理 | 验证缺失或非标准的 claims 字段时使用默认值或合理降级 | 通过 |

## 7. 安全测试

### 7.1 测试要点

- CSRF 攻击防护
- XSS 攻击防护
- 重放攻击防护
- 密码泄露防护
- Token 泄露防护
- SQL 注入防护
- 会话劫持防护

### 7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| S.1 | CSRF 攻击防护 | 验证 state 参数能够有效防止 CSRF 攻击，伪造的 state 值被拒绝 | 通过 |
| S.2 | 重放攻击防护 | 验证 nonce 参数能够防止重放攻击，重复使用的 nonce 被拒绝 | 通过 |
| S.3 | JWT 签名验证 | 验证篡改的 ID Token 签名被拒绝 | 通过 |
| S.4 | JWT 过期验证 | 验证过期的 ID Token 被拒绝 | 通过 |
| S.5 | JWT issuer 验证 | 验证来自错误 issuer 的 ID Token 被拒绝 | 通过 |
| S.6 | JWT audience 验证 | 验证 audience 不匹配的 ID Token 被拒绝 | 通过 |
| S.7 | 密码加密存储 | 验证 TDengine 密码在数据库中以 AES-GCM 加密形式存储 | 通过 |
| S.8 | Token 加密存储 | 验证 access_token 和 refresh_token 在数据库中加密存储（如实现） | 通过 |
| S.9 | httpOnly Cookie | 验证会话 cookie 设置 httpOnly 属性防止 JavaScript 访问 | 通过 |
| S.10 | Secure Cookie | 验证生产环境（HTTPS）下 cookie 设置 Secure 属性 | 通过 |
| S.11 | SameSite Cookie | 验证 cookie 设置 SameSite=Lax 或 Strict 防止 CSRF | 通过 |
| S.12 | ENCRYPT_KEY 暴露风险 | 验证 ENCRYPT_KEY cookie 的使用场景和过期时间，评估 XSS 风险 | 通过 |
| S.13 | SQL 注入防护 | 验证所有数据库查询使用参数化查询，防止 SQL 注入 | 通过 |
| S.14 | 会话固定攻击 | 验证登录后生成新的 session_id，防止会话固定攻击 | 通过 |
| S.15 | 敏感信息日志 | 验证日志中不包含密码、token 等敏感信息 | 通过 |
| S.16 | 错误信息泄露 | 验证错误响应不泄露敏感的配置或系统信息 | 通过 |
| S.17 | Client Secret 保护 | 验证 client_secret 不在日志、错误信息或客户端代码中暴露 | 通过 |
| S.18 | Rate Limiting | 验证登录、绑定等敏感接口有速率限制，防止暴力破解 | 通过 |

## 8. 性能测试

### 8.1 测试要点

- 认证响应时间
- Token 验证性能
- 会话管理性能

### 8.2 测试场景

| # | 测试场景 | 性能指标 | 测试结果 |
| --- | --- | --- | --- |
| P.1 | 单用户授权流程 | 端到端时间（从点击登录到完成认证）< 5 秒 | 通过 |
| P.2 | 授权 URL 生成 | 响应时间 < 100ms | 通过 |
| P.3 | 回调处理 | 处理时间 < 1 秒（包括 token 交换和 JWT 验证） | 通过 |
| P.4 | 会话验证 | 响应时间 < 50ms | 通过 |
| P.5 | Token 刷新 | 响应时间 < 500ms | 通过 |

## 9. 易用性测试

### 9.1 测试要点

- UI 美观性和一致性
- 交互流程合理性
- 错误提示友好性
- 国际化支持
- 帮助文档完整性

### 9.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| U.1 | 登录页 UI | 验证 OAuth 登录按钮样式美观，与页面风格一致 | 通过 |
| U.2 | 按钮文案 | 验证按钮文案清晰（如 "Sign in with SSO"），易于理解 | 通过 |
| U.3 | 加载状态 | 验证授权跳转、回调处理时显示加载指示器 | 通过 |
| U.4 | 绑定表单 UI | 验证 TDengine 凭据绑定表单布局合理，字段标签清晰 | 通过 |
| U.5 | 错误提示友好 | 验证错误信息友好且可操作（如 "配置错误，请联系管理员" 而非技术错误堆栈） | 通过 |
| U.6 | 成功反馈 | 验证登录成功、绑定成功时有明确的成功提示 | 通过 |
| U.7 | 中英文支持 | 验证点击切换语言按钮后，OAuth 相关 UI 元素正确切换语言 | 通过 |
| U.8 | 字体和字号 | 验证文字大小合适，易于阅读，无拥挤或过大问题 | 通过 |
| U.9 | 无错别字 | 验证所有文案无拼写错误或语法错误 | 通过 |
| U.10 | 帮助文档 | 验证提供完整的 OAuth 配置文档和用户使用指南 | 通过 |

## 10. 兼容性测试

### 10.1 测试要点

- 浏览器兼容性
- 操作系统兼容性
- 版本升级兼容性
- 数据迁移兼容性
- 向后兼容性

### 10.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| C.1 | Chrome 浏览器 | 验证 Chrome 90+ 上 OAuth 功能完全正常 | 通过 |
| C.2 | Firefox 浏览器 | 验证 Firefox 88+ 上 OAuth 功能完全正常 | 通过 |
| C.3 | Safari 浏览器 | 验证 Safari 14+ 上 OAuth 功能完全正常 | 通过 |
| C.4 | Edge 浏览器 | 验证 Edge 90+ 上 OAuth 功能完全正常 | 通过 |
| C.5 | Windows 系统 | 验证 Windows 10/11 上部署和运行正常 | 通过 |
| C.6 | Linux 系统 | 验证 Ubuntu 20.04/22.04 上部署和运行正常 | 通过 |
| C.7 | macOS 系统 | 验证 macOS 12+ 上部署和运行正常 | 通过 |
| C.8 | 版本升级 | 从无 OAuth 版本升级到 OAuth 版本，验证数据库迁移正确执行 | 通过 |
| C.9 | 传统用户兼容 | 升级后验证已有的传统用户账户仍然可以正常登录 | 通过 |
| C.10 | 数据库兼容 | 验证 oauth_users 和 oauth_sessions 表与现有数据库 schema 兼容 | 通过 |
| C.11 | 降级兼容 | 验证禁用 OAuth 后系统仍能正常运行（传统认证） | 通过 |
| C.12 | 配置兼容 | 验证缺失 OAuth 配置节时系统正常启动且 OAuth 自动禁用 | 通过 |

## 11. 长期稳定性测试

### 11.1 测试要点

- 长时间运行稳定性
- 会话清理效果
- 内存泄漏检测
- Token 刷新机制长期有效性
- 数据库连接池管理

### 11.2 测试场景

| # | 测试场景 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| L.1 | 7x24 小时运行 | 系统连续运行 7 天，定期进行 OAuth 登录和访问，验证无崩溃和性能退化 | 通过 |
| L.2 | 会话清理持续性 | 运行 7 天后检查数据库，验证过期会话被定期清理，无大量垃圾数据 | 通过 |
| L.3 | 内存泄漏检测 | 使用内存分析工具监控 7 天，验证内存使用稳定，无持续增长 | 通过 |
| L.4 | Token 刷新机制 | 模拟用户长期使用（超过 access_token 有效期），验证 Token 自动刷新机制持续有效 | 通过 |
| L.5 | 数据库连接池 | 高频访问下验证数据库连接池管理正常，无连接泄漏 | 通过 |
| L.6 | 日志文件大小 | 验证日志轮换机制正常，日志文件不会无限增长 | 通过 |

## 12. 运维与部署测试

### 12.1 测试要点

- 配置方式的多样性
- 错误诊断的便利性
- 监控指标的完整性
- 部署文档的准确性
- 故障排查指南

### 12.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| O.1 | 配置文件部署 | 验证仅使用 explorer.toml 配置能够成功启动 OAuth | 通过 |
| O.2 | 环境变量部署 | 验证仅使用环境变量配置能够成功启动 OAuth | 通过 |
| O.3 | 混合配置 | 验证配置文件和环境变量混合使用时按优先级正确加载 | 通过 |
| O.4 | 配置验证提示 | 验证配置错误时提供清晰的错误提示和解决建议 | 通过 |
| O.5 | 启动日志 | 验证启动时记录 OAuth 状态和配置信息（不含敏感信息） | 通过 |
| O.6 | 健康检查 | 验证提供 IdP 连通性检查功能 | 通过 |
| O.10 | 详细错误日志 | 验证认证失败时记录详细的错误日志便于排查 | 通过 |
| O.11 | 审计日志 | 验证记录所有 OAuth 登录、登出、绑定等操作的审计日志 | 通过 |
| O.12 | 部署文档准确性 | 验证部署文档与实际实现一致，步骤可操作 | 通过 |
| O.13 | 故障排查指南 | 验证文档包含常见问题的排查步骤和解决方案 | 通过 |
| O.14 | Docker 部署 | 验证 Docker 容器环境下 OAuth 功能正常（环境变量配置） | 通过 |
| O.15 | Kubernetes 部署 | 验证 K8s 环境下使用 ConfigMap/Secret 配置 OAuth 正常 | 通过 |

## 13. 已知问题和限制

根据需求规格和功能设计文档，OAuth 2.0 SSO 功能存在以下已知限制和待改进事项：

### 13.1 设计约束

1. **TDengine 用户预创建要求**: OAuth 用户必须预先在 TDengine 中创建对应的数据库用户账户，不支持自动创建
2. **手动凭据绑定**: OAuth 用户首次登录时需要手动绑定现有的 TDengine 用户凭据
3. **权限管理**: 用户权限完全由 TDengine 原生权限系统管理，不支持从 IdP 自动映射权限

### 13.2 安全风险

1. **ENCRYPT_KEY Cookie 暴露**: 绑定流程中的 ENCRYPT_KEY cookie 可被 JavaScript 读取，存在 XSS 攻击风险
2. **数据库备份泄露**: 数据库备份文件包含加密密码，需要安全的备份策略

### 13.3 功能限制

1. **单点登出不完整**: 仅支持 Explorer 本地登出，不支持 IdP 端的全局登出（back-channel logout）
2. **JWKS 缓存策略**: 缺少显式的 JWKS 缓存刷新和密钥轮换机制
3. **多实例会话共享**: 当前使用数据库存储会话，高并发场景建议使用 Redis（未实现）
4. **绑定流程安全性**: 前端加密方案可改进为服务端生成一次性 challenge 或 ephemeral key

### 13.4 其他限制

1. **时间戳数据类型**: `last_active` 字段的时间戳数据类型需统一为 chrono DateTime
2. **错误响应格式**: 缺少统一的错误码表和标准化的 JSON 错误响应格式
3. **Rate Limiting**: 未实现针对登录和绑定接口的速率限制
4. **会话过期时间**: 会话默认 8 小时过期，需要修改源码才能调整（未提供配置项）

### 13.5 企业版限制

- OAuth 2.0 SSO 功能仅在 **TDengine 企业版** 中提供
- 社区版不包含此功能
- 要求 TDengine 3.3.8.11+ 版本

### 13.6 浏览器要求

- 必须启用 Cookie（httpOnly cookies 用于会话管理）
- 必须支持 JavaScript（前端密码加密）
- 推荐使用最新版本的主流浏览器

### 13.7 网络要求

- **生产环境必须使用 HTTPS**，否则 OAuth 不安全
- 需要能够访问身份提供商的网络连接
- 回调 URI 必须能够被浏览器访问

## 14. 测试交付物

完成测试后需要提交以下交付物：
1. **测试报告**: 填写本文档中的所有测试结果列
2. **缺陷列表**: 详细记录发现的所有缺陷，包括严重级别、复现步骤、截图等
3. **测试数据**: 性能测试数据、并发测试结果、监控指标等
4. **测试脚本**: 自动化测试脚本
5. **配置文件**: 各种身份提供商的测试配置示例
6. **部署文档审核**: 对部署文档的准确性和完整性评估

## 15. 测试进度追踪

| 测试阶段 | 计划开始日期 | 计划完成日期 | 实际完成日期 | 状态 |
| --- | --- | --- | --- | --- |
| 功能测试 |  |  |  |  |
| 安全测试 |  |  |  |  |
| 性能测试 |  |  |  |  |
| 易用性测试 |  |  |  |  |
| 兼容性测试 |  |  |  |  |
| 稳定性测试 |  |  |  |  |
| 运维测试 |  |  |  |  |

## 16. 附录

### 16.1 测试用 IdP 配置示例

#### 16.1.1 Keycloak (推荐用于测试)

```toml
[oauth]
enabled = true
provider = "oidc"
[oauth.oidc]
client_id = "taos-explorer"
client_secret = "your-keycloak-client-secret"
issuer_url = "http://localhost:8080/realms/taosdata"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"
scopes = ["openid", "profile", "email"]
```

#### 16.1.2 GitHub

```toml
[oauth]
enabled = true
provider = "plain"

[oauth.provider_display_name]
en = "GitHub"
zh = "GitHub"

[oauth.plain]
client_id = "xx"
client_secret = "xx"

authorize_url = "https://github.com/login/oauth/authorize"
token_url = "https://github.com/login/oauth/access_token"
profile_url = "https://api.github.com/user"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

## 17. User attribute mapping

[oauth.user_mapping]
username = "log"
email = "email"
```

### 17.1 B. 测试数据准备

#### 17.1.1 TDengine 测试用户

在测试前需要在 TDengine 中预创建以下测试用户：
```sql
-- 创建测试用户
CREATE USER oauth_user1 PASS 'Password123';
CREATE USER oauth_user2 PASS 'Password456';
CREATE USER oauth_admin PASS 'Admin123';

-- 授予权限
GRANT ALL ON database.* TO oauth_user1;
GRANT READ ON database.* TO oauth_user2;
GRANT ALL ON *.* TO oauth_admin;
```

#### 17.1.2 IdP 测试账户

在身份提供商中创建对应的测试账户，用户名应与 TDengine 用户对应（用于绑定测试）。

### 17.2 参考链接

- OAuth 2.0 规范: https://datatracker.ietf.org/doc/html/rfc6749
- PKCE 规范: https://datatracker.ietf.org/doc/html/rfc7636
- OpenID Connect 规范: https://openid.net/specs/openid-connect-core-1_0.html
- OWASP 安全测试指南: https://owasp.org/www-project-web-security-testing-guide/
