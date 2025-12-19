# 详细设计说明书（Design Spec）- OAuth 2.0 / OIDC SSO

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
|---|---:|---:|---|---|
| 2025-12-09 | 2025-12-09 | 1.0 | 作者 | 初版：基于需求整理 OAuth2/OIDC SSO 功能规范 |
| 2025-12-12 | 2025-12-12 | 1.1 | 作者 | 与后端实现（server/src/oauth）对齐：端点、cookie 行为、会话管理 |
| 2025-12-12 | 2025-12-12 | 1.2 | 作者 | 重构后端/前端会话行为：不再将 session_id 放到 URL，前端不再持久化 oauth token 到 localStorage；增加服务端 token 加密说明 |
| 2025-12-15 | 2025-12-15 | 1.3 | 作者 | 将文档格式迁移为详细设计模板，补充加密、迁移与运维指导 |

# 引言

1. 目的  
   本详细设计说明（Design Spec）描述 TDengine Explorer 中 OAuth 2.0 / OIDC SSO 子系统的架构与实现细节，面向后端/前端实现工程师与运维人员，包含组件职责、数据库设计、接口规范、安全与运维指南、迁移步骤等。  
2. 范围  
   - 后端 OAuth 模块（OidcClient、handlers、SessionManager、middleware）  
   - 前端集成点（登录页、回调页、绑定流程）  
   - 持久化设计与加密策略（TDengine 凭据与 OAuth tokens 的加密）  
   - 迁移、部署与监控指南  
3. 受众  
   后端/前端开发工程师、运维工程师、安全负责人、文档维护者。

# 术语

- IdP：Identity Provider（例如 Keycloak、Okta）。  
- PKCE：Proof Key for Code Exchange。  
- JWT：JSON Web Token。  
- session_id：由后端生成的 UUIDv4，会话标识，后端通过 httpOnly、Secure cookie 提供给浏览器。  
- ENCRYPT_KEY：回调阶段后端设置的短期 cookie（非 httpOnly），用于前端对 TDengine 密码做一次性客户端加密以提交绑定请求。  
- EXPLORER_SECURITY_ENCRYPTION_KEY：服务端用于在数据库中加密 TDengine 密码与 OAuth tokens 的对称密钥（Base64 编码，解码后 32 字节）。

# 概述

1. 架构（高层）  
   - 主要组件：`OidcClient`（与 IdP 交互）、`handlers`（authorize/callback/bind/logout/me/status）、`SessionManager`（会话与加密存储）、`middleware`（请求鉴权与 token 刷新）、前端（登录页面、回调页、绑定 UI）、DB（`oauth_providers`、`oauth_users`、`oauth_sessions`）。  
   - 流程示意（高层）：浏览器 -> GET /api/-/oauth/authorize -> IdP -> GET /api/-/oauth/callback -> 后端交换 tokens -> 后端创建会话并设置 httpOnly session cookie -> 前端调用 GET /api/-/oauth/me (credentials: 'include') -> 应用就绪。  
2. 技术栈  
   - 后端：Rust + actix-web + sqlx。  
   - OIDC：openidconnect crate。  
   - 加密：AES-256-GCM（util 提供 aes_encrypt_base64 / aes_decrypt_base64）。  
   - 数据库：sqlite（开发/小型部署）或可替换为 Postgres/MySQL（生产）。  
3. 依赖  
   - openidconnect, actix-session, sqlx, uuid, chrono, anyhows 等。

# 设计考虑

1. 假设与限制  
   - IdP 遵循标准 OIDC 授权码 + PKCE 流。  
   - 前端不在 localStorage 中持久化 session_id 或 oauth 授权 token。  
   - TDengine 自动创建用户功能已从核心实现删除；凭据绑定通过 `/api/-/oauth/bind` 完成，或需管理员预先在 TDengine 中创建账户。  
2. 设计原则  
   - 安全优先：敏感数据在持久化存储中加密，避免在日志中记录敏感信息。  
   - 后端为主：后端返回的 httpOnly cookie 为会话的来源；前端通过后端确认 session（/api/-/oauth/me）。  
   - 可运维性：提供迁移脚本、启动时校验、迁移/轮换指南。  
3. 风险与缓解  
   - ENCRYPT_KEY（非 httpOnly）被 XSS 窃取 -> 缓解：短期有效、CSP、避免在页面中暴露其他注入点；长期目标改为 server-side challenge。  
   - 加密密钥泄露 -> 严格密钥管理、KMS/Secret Manager、密钥轮换计划。

# 详细设计

## 组件设计（职责与接口）

1. OidcClient  
   - 职责：Discovery、授权 URL 生成（含 state/nonce/PKCE）、使用 code + verifier 交换 tokens、刷新 tokens。  
   - 关键输出：`id_token_claims`、`access_token`、`refresh_token`、`expires_in`。  

2. handlers（HTTP 端点）  
   - `GET /api/-/oauth/authorize`  
     - 生成 PKCE verifier/challenge、state、nonce；写入 httpOnly cookies（`oauth_state`, `oauth_nonce`, `oauth_verifier`），302 重定向到 IdP 授权 URL。  
   - `GET /api/-/oauth/callback`  
     - 校验 cookies 中的 state/nonce/verifier；使用 code+verifier 与 IdP 交换 tokens；验证 id_token（签名/issuer/aud/nonce）；在 DB 中创建/查找 `oauth_users`；创建 `oauth_sessions`（存储加密后的 tokens）；设置 httpOnly Secure session cookie；设置短期 `ENCRYPT_KEY` cookie（非 httpOnly，用于前端绑定期间的客户端加密）；最后重定向到前端页面或返回 JSON（实现可配置）。  
   - `POST /api/-/oauth/bind`  
     - 请求体示例：`{ credential: "<base64(aes_gcm_ciphertext)>", username?: "td_user" }`。后端使用派生自 session_id 的 key 解密 credential（客户端用 `ENCRYPT_KEY` 加密），然后使用服务端 `EXPLORER_SECURITY_ENCRYPTION_KEY` 对 TDengine 密码进行 AES-GCM 加密并存入 `oauth_users.tsdb_password`。认证：使用 httpOnly cookie 或 Authorization Bearer。  
   - `POST /api/-/oauth/logout`  
     - 删除会话（从 Authorization header 或 cookie 中识别），使 cookie 失效，并删除 `oauth_sessions`。  
   - `GET /api/-/oauth/status`  
     - 无需认证，返回 `{ enabled: bool, provider?: string }`。  
   - `GET /api/-/oauth/me`  
     - 需要携带 cookie（或 Bearer），返回当前会话的用户元数据（不包含敏感凭据）。  

3. SessionManager  
   - 职责：会话 CRUD、encrypt/decrypt tokens & TDengine passwords、token 刷新写回、定期清理过期 session。  
   - `encryption_key`：从 `EXPLORER_SECURITY_ENCRYPTION_KEY`（env 优先）加载并验证（Base64->32 bytes）。  
   - 写入 DB 前：对 `access_token`/`refresh_token`/`id_token` 进行 AES-GCM 加密并 Base64 编码存储。  
   - 读取 DB 时：解密上述字段并填充返回的 `OAuthSession` 对象（在服务端进程内保持明文）。  

4. middleware  
   - 职责：从请求中提取会话（cookie 或 Bearer token），若 access token 快到期，使用 `OidcClient.refresh_access_token` 获取新 tokens 并调用 `SessionManager.refresh_session_token`（写入时加密），并在内存中更新 session 的 tokens。

## 数据模型（数据库）

- `oauth_providers`  
  - id, name, issuer_url, authorization_endpoint, token_endpoint, userinfo_endpoint, jwks_uri, created_at, updated_at

- `oauth_users`  
  - user_id INTEGER PRIMARY KEY AUTOINCREMENT  
  - provider TEXT  
  - username TEXT  
  - email TEXT  
  - tsdb_username TEXT  
  - tsdb_password TEXT  -- AES-256-GCM 加密后以 Base64 存储  
  - created_at DATETIME  
  - updated_at DATETIME

- `oauth_sessions`  
  - id INTEGER PRIMARY KEY AUTOINCREMENT  
  - session_id TEXT UNIQUE  
  - user_id INTEGER  
  - access_token TEXT       -- AES-256-GCM 加密后以 Base64 存储  
  - refresh_token TEXT      -- AES-256-GCM 加密后以 Base64 存储  
  - id_token TEXT           -- 如存储，亦为 AES-256-GCM Base64  
  - access_token_expires_at DATETIME  
  - expires_at DATETIME  
  - login_at DATETIME  
  - last_active DATETIME

注意：表中加密字段为 Base64 编码文本；字段长度/索引按实际需求配置。`last_active` 建议统一为 DATETIME 类型并使用 `chrono::Utc::now()`。

## 数据库交互细节

- Insert/Create Session：  
  - 在 `SessionManager.create_session()` 中，使用 `encrypt_token()` 将 `access_token`/`refresh_token`/`id_token` 加密后存入 DB。  
- Get Session：  
  - 在 `SessionManager.get_session()` 中，从 DB 读取加密字段并 `decrypt_token()`，然后返回包含解密后 tokens 的 `OAuthSession` 对象。  
- Refresh Token：  
  - 在 `SessionManager.refresh_session_token()` 中，接收明文新的 tokens，调用 `encrypt_token()` 写回 DB，并更新 `access_token_expires_at` 与 `last_active`。  
- Bind TDengine Credential：  
  - `/api/-/oauth/bind` 接收由客户端用 `ENCRYPT_KEY` 加密的 ciphertext，后端用派生 key 解密后再使用 `encrypt_password()`（服务端 key）保存到 `oauth_users.tsdb_password`。

## 时序 / 建议图表（建议插入到文档或 PR 中）
- 授权流程时序图：Browser -> /authorize -> IdP -> /callback -> backend exchange -> create session -> set cookies -> Browser -> /me  
- Token 刷新时序图：middleware 检测快过期 -> OidcClient.refresh_access_token -> SessionManager.refresh_session_token（写入加密 tokens）  
- 绑定流程序列图：callback 设置 ENCRYPT_KEY -> 前端收集 TDengine 凭据 -> 前端用 ENCRYPT_KEY 加密 -> POST /oauth/bind -> 后端解密并以服务端 key 加密存储。

# 接口规范（细节）

1. GET /api/-/oauth/status  
   - 返回：`{ enabled: true|false, provider?: string }`  
   - Auth：无需

2. GET /api/-/oauth/authorize  
   - 行为：设置临时 httpOnly cookies (`oauth_state`,`oauth_nonce`,`oauth_verifier`)，302 到 IdP 授权 URL。  
   - Auth：无需

3. GET /api/-/oauth/callback?code=...&state=...  
   - 行为：校验 cookies；交换 tokens；创建/更新 `oauth_users`/`oauth_sessions`；设置 httpOnly Secure session cookie 与短期 `ENCRYPT_KEY`（非 httpOnly）；重定向到前端页面或返回 JSON。  
   - Errors：401/4xx on invalid state/code，5xx on server errors。

4. POST /api/-/oauth/bind  
   - 请求体：`{ credential: "<base64(aes_gcm_ciphertext)>", username?: "td_user" }`  
   - 行为：通过 session（cookie/Authorization）识别会话，使用派生 key 解密 credential，然后使用服务端 `EXPLORER_SECURITY_ENCRYPTION_KEY` 对 TDengine 密码再次加密存储进 `oauth_users.tsdb_password`。  
   - Auth：需要 cookie 或 Bearer token

5. POST /api/-/oauth/logout  
   - 行为：删除会话并让 cookie 失效。  
   - Auth：cookie 或 Bearer token

6. GET /api/-/oauth/me  
   - 行为：返回当前用户信息（不包含凭据）。  
   - Auth：cookie 或 Bearer token（推荐浏览器调用时使用 credentials: 'include'）

# 安全考虑

1. 数据加密策略  
   - `EXPLORER_SECURITY_ENCRYPTION_KEY`：服务端必须持有 Base64(32 bytes) 的对称密钥，用于 AES-256-GCM 加密 TDengine 密码与 OAuth tokens。优先从环境变量读取（`EXPLORER_SECURITY_ENCRYPTION_KEY`），可选 fallback 至 `explorer.toml` 中的 `oauth.encryption_key`（不推荐）。服务启动时必须验证密钥长度为 32 字节，校验失败则拒绝启动。  
   - 数据在持久化层（DB）为加密后的 Base64 文本。运行时内存中可能存在明文（仅在服务进程内）。  
2. CSRF / PKCE / Nonce  
   - 使用 state、nonce、PKCE verifier 保证授权流程安全，并通过 httpOnly cookies 存储这些临时数据（TTL 推荐 10 分钟）。callback 时严格校验。  
3. ENCRYPT_KEY（回调短期 cookie）  
   - 该 cookie 为短期、非 httpOnly 用于前端在绑定时加密凭据；应缩短 TTL，并注意 XSS 风险（CSP、输入校验、最小化页面中可注入点）。长期建议替换为 server-side 一次性 challenge。  
4. 日志与审计  
   - 禁止在日志中打印明文 tokens / 密码 / 密钥。只记录事件（login/logout/bind/refresh）与元数据（timestamp, user_id, client IP）。  
5. 密钥管理和轮换  
   - 使用 Secret Manager（Vault、AWS Secrets Manager、GCP Secret Manager）管理 `EXPLORER_SECURITY_ENCRYPTION_KEY`。密钥轮换策略应支持：双密钥解密兼容（读时可尝试旧 key），以及逐行重加密步骤。  

# 性能与可扩展性

1. Token 刷新抑制  
   - 中间件在刷新 token 时应避免并发触发（例如基于 session 加锁或基于时间窗合并刷新请求），避免对 IdP 产生突发并发。  
2. 会话存储与扩展  
   - 支持将 sqlite 替换为 RDBMS（Postgres/MySQL）以提供多实例支持；可在前端缓存或 Redis 中缓存 session 元数据以降低 DB 压力，但保留持久化和加密字段在关系 DB 中。  
3. 多 IdP / 多租户支持  
   - 设计时应留出 provider 标识字段并在 OIDC 配置与路由层支持多 provider 的扩展。

# 部署与配置

1. 配置项（优先级）  
   - `EXPLORER_SECURITY_ENCRYPTION_KEY`（env） — Base64(32 bytes)（优先）  
   - `explorer.toml`（可选 fallback）：`[oauth] encryption_key = "<base64-32>"`（不推荐在生产中存放明文）  
   - 其他：`oauth.enabled`, `oauth.oidc.client_id`, `oauth.oidc.client_secret`, `oauth.oidc.issuer_url`, `oauth.oidc.redirect_uri`, `oauth.user_mapping` 等。  
2. 启动校验  
   - 服务启动时对 `EXPLORER_SECURITY_ENCRYPTION_KEY` 做 Base64 解码并验证长度为 32 字节；如果失败，应退出并提示运维。  
3. 数据库迁移（已有明文 token 的迁移）  
   - 在升级部署时，如果 DB 可能包含旧版的明文 tokens，需要运行迁移脚本：逐行检查 `oauth_sessions` 的 token 字段，尝试使用当前 key 解密：若解密成功则跳过（表示已加密）；解密失败则视为明文并 encrypt 后写回。务必在迁移前备份数据库。  
4. 回滚策略  
   - 部署前备份 DB 与配置；若密钥配置错误导致服务无法启动，可回滚到备份或将正确密钥注入环境变量并重启服务。

# 监控与维护

1. 指标（推荐）  
   - oauth.auth_success, oauth.auth_failure, oauth.refresh_success, oauth.refresh_failure, oauth.bind_success, oauth.bind_failure, token_exchange_latency, jwks_fetch_errors。  
2. 日志策略  
   - 保留操作性/错误日志，不记录敏感字段（tokens/passwords/keys）。审计日志记录事件时间、user_id、IP、结果（成功/失败）。  
3. 运维脚本  
   - 建议提供：迁移脚本（加密旧明文）；会话清理脚本；批量使 session 失效脚本（用于安全响应）。

# 测试需求

1. 单元测试  
   - OidcClient：discovery、exchange_code、refresh_access_token、id_token 验证。  
   - SessionManager：encrypt/decrypt、create_session（写入加密）、get_session（解密）、refresh_session_token（加密写入）、bind_tsdb_credentials（解密 client ciphertext 并重新加密保存）。  
   - handlers：authorize/callback/bind/logout/me 的错误分支和边界条件（cookies 缺失、state mismatch、id_token invalid）。  
2. 集成测试  
   - 使用 Keycloak 或 mock OIDC server 测试完整流程（authorize -> callback -> bind -> me）；验证回调后不会在 URL 中携带 session_id；验证加密存储与解密行为。  
3. 安全测试  
   - XSS 漏洞检测（重点：ENCRYPT_KEY 可被脚本读取的风险）。  
   - DB/备份泄露下的加密 token 抗风险验证（在密钥被保护的前提下，验证泄露恢复流程与失效策略）。

# 实现中的未完成事项 & 改进建议

- 将绑定流程改为 server-side 一次性 challenge（避免把 raw ENCRYPT_KEY 暴露给前端 JavaScript）。  
- 支持密钥轮换（双密钥读取兼容 + 后台逐条重加密）。  
- 多 IdP / 多租户支持与 UI 管理界面。  
- 在部署时提供官方迁移脚本（Rust 可执行）并在 CI 中运行静态检查与集成测试。

# 参考资料

- 实现文件（代码为准）：  
  - `server/src/oauth/client.rs`  
  - `server/src/oauth/handlers.rs`  
  - `server/src/oauth/session.rs`  
  - `server/src/oauth/middleware.rs`  
- 设计规范：OIDC Core、RFC 6749（OAuth 2.0）

---

此详细设计基于当前后端实现与最近完成的前端重构（不再依赖 localStorage 持久化 oauth token，回调不再将 session_id 放到 URL）。如有实现或配置更改（例如密钥名称、会话 cookie 名称、重新引入 TDengine 自动创建功能），请在代码与本文档中同步更新，并在发布说明中记录迁移与兼容步骤（如何回滚、如何迁移现有会话数据）。