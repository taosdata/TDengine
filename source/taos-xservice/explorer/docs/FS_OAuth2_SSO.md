# 概要设计说明书（Functional Spec）- OAuth 2.0 / OIDC SSO

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
|---|---:|---:|---|---|
| 2025-12-09 | 2025-12-09 | 1.0 | 作者 | 初版：基于需求整理 OAuth2/OIDC SSO 功能规范 |
| 2025-12-12 | 2025-12-12 | 1.1 | 作者 | 与 `server/src/oauth` 实现对齐：端点、cookie 行为、加密 key、Token 存储与未实现项 |
| 2025-12-12 | 2025-12-12 | 1.2 | 作者 | 更新：按完成的重构，前端不再持久化 `oauth_token` 到 localStorage；会话通过后端 httpOnly Secure cookie 传递，回调不再将 `session_id` 放入 URL。 |

# 背景

本文档定义 TDengine Explorer 中 OAuth 2.0 / OpenID Connect 单点登录（SSO）的产品行为与运维要点，并与当前后端实现（`server/src/oauth`）保持一致。以实现代码为准；任何行为变动必须同时更新实现与本文件。

# 定义

- OAuth 2.0：授权框架（RFC 6749）。
- OIDC：OpenID Connect（基于 OAuth 2.0 的身份层）。
- IdP：Identity Provider（例如 Keycloak、Azure AD、Google）。
- PKCE：Proof Key for Code Exchange。
- JWT：JSON Web Token。
- TDengine/TSDB 用户：用于访问 TDengine 的数据库账户与密码。
- session_id：后端为会话生成的 UUIDv4，用作 Explorer 会话凭据（实现上由后端持有并通过 httpOnly Secure cookie 提供给浏览器）；客户端不应将其持久化到 localStorage 或 URL。

# 范围与设计原则

- 描述目前实现（`server/src/oauth`）的行为细节：API 路径、cookie 策略、会话与 token 存储行为、TDengine 绑定流程以及未实现/有风险的点。
- 安全原则：网络传输必须使用 HTTPS；敏感数据持久化存储时应加密；尽量避免在 URL 或 localStorage 中暴露凭据。
- 实现优先：以代码实际行为为准。文档旨在准确反映实现并指出需要改进之处。

# 行为说明（实现对齐）

以下内容基于当前实现（`server/src/oauth`）逐项说明，包含开发/运维在使用与扩展时必须知悉的约束与行为。

## 授权流程（实现细节）

1. 前端触发：用户在登录页选择 OAuth 登录 → 前端调用后端：
   - GET `/api/-/oauth/authorize`（实现中使用 `/api/-/` 前缀）。

2. 后端处理（`oauth_authorize`）：
   - 使用 OIDC 客户端自动发现（Discovery）。
   - 生成 PKCE code_challenge / code_verifier、CSRF `state`（CsrfToken）与 `nonce`。
   - 在响应中通过 HTTP-only cookies 写入临时参数：
     - `oauth_state`（httpOnly, SameSite=Lax, TTL 10 分钟）
     - `oauth_nonce`（httpOnly, SameSite=Lax, TTL 10 分钟）
     - `oauth_verifier`（httpOnly, SameSite=Lax, TTL 10 分钟）
   - 返回 302 重定向到 IdP 的授权 URL（带 state、nonce、PKCE challenge 等）。
   - 参考：`server/src/oauth/handlers.rs::oauth_authorize`。

3. IdP 回调：IdP 完成认证后 redirect 返回：
   - GET `/api/-/oauth/callback?code=...&state=...`（或 error 参数）。

4. 回调处理（`oauth_callback`）：
   - 从 cookies 读取并验证 `state`、`nonce`、`verifier`。若缺失或不匹配则返回错误。
   - 使用 authorization `code` 与 `code_verifier` 调用 token endpoint，交换得到 `access_token`、`id_token`、`refresh_token`（如有）。
   - 使用 `openidconnect` 库校验 `id_token`（签名、iss、aud、exp、nonce 等）。
   - 从 `id_token` claims（或 userinfo）提取用户信息并在 `oauth_users` 表中查找或创建用户记录（首次见到某 provider+username 组合时会创建 `oauth_users`）。
   - 创建 `oauth_sessions` 记录，保存 `session_id`（UUIDv4）、access/refresh/id tokens（当前实现写入 DB）、过期时间、登录时间、last_active 等。
   - 会话传递方式（已完成的重构）：
     - 回调响应不再把 `session_id` 通过 URL 返回给前端。
     - 后端在回调响应中设置一个 httpOnly、Secure 的 session cookie（浏览器自动携带作为后续请求凭证）。
     - 前端应调用后端 profile/session 端点（例如 `GET /api/-/oauth/me`）并使用 `credentials: 'include'` 来验证会话是否建立，然后在应用内设置 OAuth 登录状态（Vuex 等）。
   - 回调会设置一个名为 `ENCRYPT_KEY` 的 cookie（非 httpOnly，base64 编码，短期有效），供前端在绑定 TDengine 凭据时使用（见绑定流程）。该 cookie 可被 JS 读取，因而存在 XSS 风险。
   - 参考：`server/src/oauth/handlers.rs::oauth_callback`。

## 会话与中间件（实现）

- `oauth_sessions` 中保存的字段包含：session_id、user_id、access_token、refresh_token、id_token、access_token_expires_at、expires_at、login_at、last_active 等。
- 中间件行为：
  - 中间件会尝试从 Authorization header（Bearer <session_id>）或 cookie 中解析会话标识并使用 `SessionManager::verify_session` 验证会话是否存在且未过期；在浏览器 SSO 流程中，优先使用 httpOnly session cookie。
  - 若 access_token 将在 5 分钟内过期，并且存在 refresh_token，则尝试调用 `OidcClient::refresh_access_token` 更新令牌并调用 `SessionManager::refresh_session_token` 将新 tokens 写回 DB；刷新失败会记录日志，但不会自动中断正在处理的请求（实现会记录刷新失败并继续）。
  - 若会话包含已绑定的 TDengine 凭据（加密存储），中间件解密后将 `TsdbCredential` 提供给上层使用（方便在内部服务调用时使用 TDengine 凭据进行鉴权）。
  - 参考：`server/src/oauth/middleware.rs`。

## TDengine 绑定行为（实现）

- 当前实现不再包含 TDengine 特定的自动创建逻辑（`TDengineOAuthConfig` 已在最近提交中移除）。绑定凭据由后端会话与 `oauth_users` 表管理；管理员需预先在 TDengine 中创建用户或用户通过绑定流程提交凭据以完成关联。

- 绑定流程（实现现状与重构后的推荐行为）：
  - 若首次登录且 `oauth_users` 未包含 TDengine 凭据，前端引导用户进行绑定。
  - 前端使用回调设置的 `ENCRYPT_KEY` cookie（短期、可被 JS 读取）在浏览器端 AES 加密 TDengine 密码（后端预期 base64 编码的密文），并调用：
    - POST `/api/-/oauth/bind`，推荐请求体为 `{ username: "td_user", credential: "<base64(ciphertext)>" }`。
  - 后端优先根据 httpOnly session cookie（或 Authorization header）来识别会话并将凭据绑定到该会话。为了向后兼容，后端仍可接受 `token: "<session_id>"` 字段，但该字段在浏览器端不推荐使用或持久化（不应写入 localStorage 或 URL）。推荐仅依赖服务端会话 cookie。
  - 后端在 `oauth_bind` 中使用派生自 session_id 的 AES key 解密前端提交的 payload，并将 TDengine 密码以服务端 AES-GCM 加密后写入 `oauth_users.tsdb_password`。
  - 参考：`server/src/oauth/handlers.rs::oauth_bind`、`server/src/oauth/session.rs::bind_tsdb_credentials`。

# 配置（实现对应）

- 配置可通过 `explorer.toml` 或环境变量注入。关键配置项（实现中）：
  - `oauth.enabled` (bool)
  - `oauth.provider` (string)
  - `oauth.fallback_redirect_uri` (string)
  - `oauth.oidc.client_id`
  - `oauth.oidc.client_secret`
  - `oauth.oidc.issuer_url`
  - `oauth.oidc.redirect_uri` (实现默认值见代码)
  - `oauth.oidc.scopes` (默认 ["openid","profile","email"])
  - `oauth.user_mapping`（claims 映射）
- 重要环境变量：
  - `EXPLORER_SECURITY_ENCRYPTION_KEY`：Base64 编码的 32 字节 key，用于服务端加密 TDengine 密码（必须在生产中设置）。若缺失，代码会退回到硬编码或派生的默认 key 并记录警告，**不可用于生产**。

# API（以实现为准）

重要端点（当前实现路径以 `/api/-/` 为准）：

- GET `/api/-/oauth/status`  
  - 返回 OAuth 是否启用及 provider 信息（实现：`oauth_status`）。

- GET `/api/-/oauth/authorize`  
  - 后端生成授权 URL 并 302 重定向到 IdP，同时设置 `oauth_state`, `oauth_nonce`, `oauth_verifier` cookies。

- GET `/api/-/oauth/callback`  
  - 处理 IdP 回调、交换 tokens、校验 id_token、创建 `oauth_users` 与 `oauth_sessions`。
  - 已完成的重构行为：回调不再通过 URL 将 `session_id` 返回给前端；而是由后端设置 httpOnly、Secure 的 session cookie。前端必须通过 `GET /api/-/oauth/me`（并包含凭证）来确认会话建立。
  - 回调仍会设置 `ENCRYPT_KEY` cookie（非 httpOnly、短期有效），供前端在绑定 TDengine 凭据时使用。

- POST `/api/-/oauth/bind`  
  - 请求体建议为 `{ username: "td_tsdb_user", credential: "<base64(aes_ciphertext)>" }`。后端优先通过 httpOnly session cookie 或 Authorization header 来识别会话并绑定凭据。浏览器端不应把 session_id 写入 URL 或 localStorage。

- POST `/api/-/oauth/logout`  
  - 从 Authorization header（Bearer session_id）或 httpOnly session cookie 中删除会话（实现会删除 `oauth_sessions` 记录并使 cookie 失效）。

备注：实现中还包含 `self_provided_token` 的逻辑，用于基于 TDengine 凭据创建自带凭据的会话（`__self__` provider）。

# 前端与集成注意（重构后行为）

- 前端不应将 session_id 或 OAuth access token 持久化到 localStorage. 客户端认证状态应以后端 httpOnly session cookie 为准。
- 在 OAuth 回调页或登录页收到来自后端的回调后，应：
  - 使用 fetch / XHR 调用 `GET /api/-/oauth/me`（并设置 `credentials: 'include'`）来验证服务端会话是否已建立；
  - 仅在服务端返回成功（已认证）的情况下才将 Vuex/应用状态标记为 OAuth 登录（例如 `store.dispatch('app/setOAuthLogin', true)`）。
- 绑定流程：如果 IdP 在回调 URL 中仍包含一个短期 token（legacy），前端可以临时读取该参数用于一次性绑定操作，但不得将其写入 localStorage 或其他持久化存储；优先使用 httpOnly cookie 驱动的绑定流程。
- 对于客户端 SDK 或自动化脚本，如果无法使用浏览器 cookie，应改用 Authorization Bearer header（session_id）方式，但要注意保护该 token 的存储与传输（不要写入可被 XSS 读取的存储）。

# 安全（实现现状与注意事项）

> 本节列出当前实现的已知安全设计与必须关注的风险点 —— 开发/运维必须理解并在发布前评估与缓解。

已实现的安全措施：
- 使用 PKCE、state、nonce 避免 CSRF 与授权码重放。
- ID Token 通过 `openidconnect` 库进行签名和 claims 验证（iss/aud/exp/nonce）。
- TDengine 密码在 DB 中使用 AES-GCM 加密（密钥来源由 `SessionManager::load_encryption_key` 控制）。
- `oauth_state`/`oauth_nonce`/`oauth_verifier` cookies 默认设置为 httpOnly、SameSite=Lax，有 10 分钟 TTL。
- 回调已完成重构：不再将 `session_id` 放入 URL，改为 httpOnly Secure cookie。

需要注意 / 风险（当前实现）：

1. access_token / refresh_token 在 DB 中已加密（自最近实现起）：
   - 说明：access_token、refresh_token 和 id_token 在写入 `oauth_sessions` 之前会使用 AES-256-GCM 加密并以 Base64 文本形式存储，使用与 `tsdb_password` 相同的服务端对称密钥（参见下文的 `EXPLORER_SECURITY_ENCRYPTION_KEY`）。在内存或网络传输环节仍以明文形式使用，但持久化在数据库中是加密的。
   - 迁移与兼容性：若数据库中存在旧的明文 token，需要在升级部署时运行一次迁移脚本将这些明文 token 加密（迁移示例见下文）。迁移策略示例：对每个 token 字段尝试用当前密钥解密；若解密成功则跳过（已加密），若解密失败则视为明文并加密后更新数据库。
   - 风险与缓解：加密能降低因 DB/备份泄露导致的滥用风险，但若加密密钥被泄露，所有加密数据将面临风险。因此必须对密钥实行严格的管理、审计与轮换策略。

2. `ENCRYPT_KEY` cookie（非 httpOnly）：
   - 风险：该 cookie 可被前端 JavaScript 读取，用于客户端在绑定流程中对 TDengine 密码进行一次性加密。如果前端存在 XSS 漏洞，攻击者可能读取该 key 并伪造/窃取绑定凭据。
   - 建议：（短期）在前端强制启用 Content Security Policy (CSP) 与其他 XSS 保护措施，并尽量缩短 `ENCRYPT_KEY` 的有效期；（长期）考虑改为一次性 server-side challenge 或采用不会将 raw key 暴露给 JS 的绑定方案。

3. 加密密钥名称与加载优先级（标准化为 `EXPLORER_SECURITY_ENCRYPTION_KEY`）：
   - 推荐变量名：`EXPLORER_SECURITY_ENCRYPTION_KEY`（Base64 编码，解码后应为 32 字节）。
   - 优先级：环境变量 > 配置文件（`explorer.toml` 中 `security.encryption_key`，可选）> 不允许使用内置默认。也就是说，若环境变量存在则优先使用；否则尝试从配置文件读取；若两者均未设置，推荐服务在启动时失败并输出明确错误（不要回退到不安全默认）。
   - 验证：启动时必须验证 Base64 解码长度为 32 字节；若校验失败则拒绝启动并给出运维可操作的错误信息（例如“EXPLORER_SECURITY_ENCRYPTION_KEY must be base64 of 32 bytes”）。
   - 运维要点：强烈建议将该密钥由 Secret Manager / KMS / Vault 管理，并通过运行时注入环境变量的方式提供给服务。避免将密钥写入代码仓库或 VCS 中的配置文件。记录并测试密钥轮换流程（包括迁移或重新加密已存数据的步骤）。

4. 时间/类型不一致风险：
   - 部分更新操作会使用不同时间类型（建议统一为 DATETIME 并在代码中使用 `chrono::Utc::now()`）。

# 兼容性与限制（实现对应）

- 与本地用户名/密码认证并存（实现未移除本地登录）。
- TDengine 相关的配置（`oauth.tdengine` / `TDengineOAuthConfig`）已被移除；自动创建 TDengine 用户的功能不再可用。请预先在 TDengine 中创建用户或使用前端绑定流程将凭据与 OAuth 会话关联。
- IdP 必须遵循 OIDC 标准；对不完全遵循标准的 IdP 可能需要 provider-specific 调整。

# 运维与部署注意事项（实现对应）

- 强制在生产中设置 `EXPLORER_SECURITY_ENCRYPTION_KEY`（Base64(32 bytes)），并使用 Secret Manager/KMS 管理。
- 部署必须使用 HTTPS，并保证与 IdP 的网络连通性。
- 多实例部署：当前 state/nonce 是通过 cookies 携带；如需更稳健，可考虑将临时 state 存入共享存储（Redis）。
- 监控：token_exchange latency、token_refresh failures、oauth.auth_success/failure 计数、jwks fetch 状态应纳入监控。
- 安全事件：若怀疑 session 或 token 泄露，应尽快撤销会话（可实现批量 session delete、token revoke 脚本）。

# DB Schema（与当前实现对应 / 建议）

当前实现使用 sqlite/sqlx 的表（下列为建议/反映实现的字段名与类型）：

- `oauth_providers`
  - id INTEGER PRIMARY KEY AUTOINCREMENT
  - name TEXT
  - issuer_url TEXT
  - authorization_endpoint TEXT
  - token_endpoint TEXT
  - userinfo_endpoint TEXT
  - jwks_uri TEXT
  - created_at DATETIME
  - updated_at DATETIME

- `oauth_users`
  - user_id INTEGER PRIMARY KEY AUTOINCREMENT
  - provider TEXT
  - username TEXT
  - email TEXT
  - tsdb_username TEXT
  - tsdb_password TEXT  -- AES-GCM 加密的 base64 文本
  - created_at DATETIME
  - updated_at DATETIME

- `oauth_sessions`
  - id INTEGER PRIMARY KEY AUTOINCREMENT
  - session_id TEXT UNIQUE
  - user_id INTEGER
  - access_token TEXT       -- (当前实现：AES-256-GCM 加密后以 Base64 存储)
  - refresh_token TEXT      -- (当前实现：AES-256-GCM 加密后以 Base64 存储)
  - id_token TEXT           -- (当前实现：如存储则同样经 AES-256-GCM 加密并以 Base64 存储)
  - access_token_expires_at DATETIME
  - expires_at DATETIME
  - login_at DATETIME
  - last_active DATETIME

> 注：实现中存在对 `last_active` 使用 integer epoch 秒的情况，建议统一为 DATETIME 类型并在代码中使用 `chrono::Utc::now()`。

# 测试需求（实现对应）

- 单元测试：
  - `OidcClient` 的 discovery、exchange_code、refresh_access_token、id_token 验证逻辑。
  - `SessionManager` 的 encrypt/decrypt、create_session、get_session、bind_tsdb_credentials、refresh_session_token、cleanup_expired_sessions。
  - `handlers` 中 `oauth_authorize`、`oauth_callback`、`oauth_bind` 的行为（cookie 设置/读取、错误分支）。

- 集成测试：
  - 使用 Keycloak 或 mock OIDC server 执行完整授权码 + PKCE 流程（含首次绑定流程）。
  - 验证回调后**不**在 URL 中携带 session_id，而是通过 httpOnly Secure session cookie 传递；前端必须通过 `GET /api/-/oauth/me`（包含凭证）来确认会话建立。
  - Token 刷新流程（middleware）在 access token 到期前后行为。

- 安全测试：
  - XSS 漏洞检查（特别验证 `ENCRYPT_KEY` cookie 读取风险）。
  - DB/备份泄露场景下 tokens 与密码的影响与恢复策略测试。
  - CSRF 检查（state 的生成与校验）。

- 性能测试：
  - 并发授权链路（至少 1000 并发请求）并测量 token_exchange latency。

# 实现中的未完成事项 & 改进建议（优先级）

基于当前实现（server/src/oauth），列出需尽快处理或在文档中明确的事项。

高优先级：
1. 已完成：不要将 `session_id` 放到回调 URL；改为由后端在回调响应中设置 httpOnly Secure cookie（降低泄露风险）。
2. 将 access_token / refresh_token 在数据库中加密存储（与 TDengine 密码一样加密或使用 KMS）。
3. 完善或实现 TDengine 自动创建用户逻辑，或在 FS 中明确标注该功能为未实现并提供管理员预建流程指导。

中优先级：
4. 改善绑定流程以避免将 raw encryption key 暴露给 JS（例如服务器生成一次性 challenge / ephemeral key）。
5. 统一时间戳数据类型，确保 `last_active` 使用 chrono DateTime 并与 DB schema 对齐。
6. 统一所有 handler 的错误 JSON 格式，列出统一错误码表，便于前端/自动化处理。

低优先级：
7. 明确 JWKS 缓存/刷新/轮换策略（或在实现中加入显式缓存与刷新逻辑）。
8. 为多实例部署提供 Redis-backed 临时 state 存储的实现示例。

# 文档变更记录与发布注意

- 在发布说明中明确：当前实现使用 `/api/-/oauth/*` 路径前缀；已完成的重构改为通过 httpOnly Secure session cookie 传递会话（回调不再将 session_id 放入 URL）。并提醒运维设置 `EXPLORER_SECURITY_ENCRYPTION_KEY`。
- 明确前端必须停止将 oauth token/session_id 写入 localStorage 与 URL；必须通过后端 profile/session 端点确认会话。
- 在企业版文档中增加专门章节，解释 `ENCRYPT_KEY` 的用途、风险与绑定操作步骤。

# 参考（实现文件）

- `server/src/oauth/client.rs` — OIDC client/discovery/exchange/refresh
- `server/src/oauth/handlers.rs` — authorize / callback / bind / logout handlers
- `server/src/oauth/session.rs` — session persistence / encryption helpers / bind logic
- `server/src/oauth/middleware.rs` — token refresh & session verification logic

---

此文档已按当前后端实现进行校准（包含最近完成的会话/回调重构与前端不再依赖 localStorage 持久化 oauth token 的行为）。若计划变更实现（例如更改会话 cookie 名称、tokens 存储策略、实现 auto_create_user），请先在代码与本 FS 中同时更新，并在发布说明里列出兼容性与迁移步骤（如何回滚、如何迁移现有会话数据）。
