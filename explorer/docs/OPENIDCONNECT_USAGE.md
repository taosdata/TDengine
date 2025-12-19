# 使用 openidconnect crate 实现 OAuth 2.0/OIDC

## 为什么选择 openidconnect？

我们选择使用 `openidconnect` crate 而不是直接使用 `oauth2` crate，原因如下：

### 主要优势

1. **专为 OpenID Connect 设计**
   - `openidconnect` 是专门为 OIDC 协议设计的高级封装
   - 提供了完整的 OIDC 功能，而 `oauth2` 只是通用的 OAuth 2.0 实现

2. **自动处理 OIDC Discovery**
   ```rust
   // 只需一行代码即可完成 OIDC Discovery
   let provider_metadata = CoreProviderMetadata::discover_async(
       issuer_url,
       async_http_client,
   ).await?;
   ```
   - 自动从 `/.well-known/openid-configuration` 获取所有端点配置
   - 无需手动配置 authorization_endpoint, token_endpoint, jwks_uri 等

3. **内置完整的 JWT 验证**
   ```rust
   // 自动验证 ID token 的所有安全属性
   let id_token_claims = token_response
       .id_token()?
       .claims(&id_token_verifier, &expected_nonce)?;
   ```
   验证内容包括：
   - ✅ JWT 签名验证（使用 JWKS）
   - ✅ Issuer (iss) 验证
   - ✅ Audience (aud) 验证
   - ✅ Nonce 验证（防止重放攻击）
   - ✅ Expiration (exp) 验证
   - ✅ Issued At (iat) 验证

4. **自动 JWKS 处理**
   - 自动从 JWKS URI 获取公钥
   - 自动缓存和更新密钥
   - 支持密钥轮换
   - 无需手动管理 JWT 验证密钥

5. **类型安全**
   - 强类型的 Claims 结构
   - 编译时类型检查
   - 减少运行时错误

6. **标准兼容**
   - 完全符合 OpenID Connect Core 1.0 规范
   - 通过了 OpenID Foundation 的兼容性测试

## 与直接使用 oauth2 的对比

### 使用 oauth2 + jsonwebtoken (旧方案)
```rust
// 需要大量手动代码
// 1. 手动构建 discovery URL
// 2. 手动解析 discovery 文档
// 3. 手动配置各个端点
// 4. 手动从 JWKS URI 获取公钥
// 5. 手动解码 JWT
// 6. 手动验证每个 claim (iss, aud, exp, nonce...)
// 7. 手动处理密钥轮换
// 需要约 200+ 行代码和多个依赖
```

### 使用 openidconnect (新方案)
```rust
// 简洁且安全
// 1. 一行代码完成 discovery
let provider_metadata = CoreProviderMetadata::discover_async(
    issuer_url,
    async_http_client,
).await?;

// 2. 自动创建客户端
let client = CoreClient::from_provider_metadata(
    provider_metadata,
    client_id,
    Some(client_secret),
);

// 3. 自动验证 ID token
let id_token_claims = token_response
    .id_token()?
    .claims(&id_token_verifier, &nonce)?;

// 只需约 50 行核心代码，所有安全验证自动完成
```

## 代码示例

### 完整的 OIDC 流程

```rust
use openidconnect::{
    core::{CoreClient, CoreProviderMetadata, CoreIdTokenClaims},
    IssuerUrl, ClientId, ClientSecret, RedirectUrl,
    PkceCodeChallenge, CsrfToken, Nonce,
};

// 1. 创建客户端（自动 discovery）
let issuer_url = IssuerUrl::new("https://idp.example.com".to_string())?;
let provider_metadata = CoreProviderMetadata::discover_async(
    issuer_url,
    async_http_client,
).await?;

let client = CoreClient::from_provider_metadata(
    provider_metadata,
    ClientId::new("client_id".to_string()),
    Some(ClientSecret::new("client_secret".to_string())),
)
.set_redirect_uri(
    RedirectUrl::new("http://localhost:6060/callback".to_string())?
);

// 2. 生成授权 URL（带 PKCE）
let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
let (auth_url, csrf_token, nonce) = client
    .authorize_url(
        CoreAuthenticationFlow::AuthorizationCode,
        CsrfToken::new_random,
        Nonce::new_random,
    )
    .add_scope(Scope::new("openid".to_string()))
    .add_scope(Scope::new("profile".to_string()))
    .set_pkce_challenge(pkce_challenge)
    .url();

// 3. 用户在 IdP 完成认证后，处理回调
// 验证 CSRF token...

// 4. 交换 code 换取 tokens（自动验证 ID token）
let token_response = client
    .exchange_code(AuthorizationCode::new(code))
    .set_pkce_verifier(pkce_verifier)
    .request_async(async_http_client)
    .await?;

// 5. 验证并提取 ID token claims
let id_token_verifier = client.id_token_verifier();
let id_token_claims = token_response
    .id_token()?
    .claims(&id_token_verifier, &nonce)?;

// 6. 使用 claims
println!("User: {}", id_token_claims.subject().as_str());
println!("Email: {:?}", id_token_claims.email());
```

## 安全性保证

使用 `openidconnect` crate 自动提供以下安全保证：

### ID Token 验证
- ✅ **签名验证**: 使用 IdP 的公钥验证 JWT 签名
- ✅ **Issuer 验证**: 确保 token 来自预期的 IdP
- ✅ **Audience 验证**: 确保 token 是为我们的应用颁发的
- ✅ **Nonce 验证**: 防止重放攻击
- ✅ **过期时间验证**: 确保 token 未过期
- ✅ **颁发时间验证**: 确保 token 不是来自未来

### PKCE 支持
- ✅ 自动生成和验证 code_challenge
- ✅ 使用 SHA256 算法
- ✅ 防止授权码拦截攻击

### 状态管理
- ✅ CSRF token (state) 生成
- ✅ Nonce 生成和验证
- ✅ PKCE verifier 管理

## 集成到 taos-explorer

### 1. 配置结构保持不变
我们现有的 `OAuthConfig` 结构仍然适用，只是内部使用 `openidconnect` 来处理。

### 2. 更简洁的客户端实现
`client.rs` 现在只有约 300 行代码，而如果使用 `oauth2` + 手动 JWT 验证需要 500+ 行。

### 3. 更好的错误处理
`openidconnect` 提供了详细的错误类型，便于调试和用户反馈。

### 4. 易于测试
- 可以使用 mock HTTP client 进行单元测试
- 无需真实的 IdP 即可测试大部分逻辑
- 内置的类型系统帮助捕获错误

## 依赖关系

```toml
[dependencies]
openidconnect = "3.5"  # 这一个依赖就够了
```

`openidconnect` 内部使用：
- `oauth2` - OAuth 2.0 基础功能
- `jsonwebtoken` - JWT 处理
- `reqwest` - HTTP 客户端（异步）
- `serde` - 序列化/反序列化

所以我们只需添加一个依赖，就能获得完整的 OIDC 功能栈。

## 支持的 IdP

`openidconnect` crate 与所有符合 OIDC 标准的 IdP 兼容，包括：

- ✅ Keycloak
- ✅ Okta
- ✅ Azure AD (Microsoft Entra ID)
- ✅ Google Identity Platform
- ✅ Auth0
- ✅ AWS Cognito
- ✅ GitLab
- ✅ GitHub (通过 OIDC)
- ✅ 任何符合 OpenID Connect Core 1.0 的 IdP

## 性能考虑

1. **Discovery 缓存**: Provider metadata 应该在启动时获取一次，然后缓存
2. **JWKS 缓存**: 公钥会被自动缓存，只在需要时更新
3. **Token 验证**: JWT 验证是 CPU 密集型操作，但 `openidconnect` 已经优化过

## 最佳实践

### 1. 在启动时初始化客户端
```rust
// 在 main.rs 中
let oidc_client = OidcClient::new(oauth_config).await?;
let oidc_client = web::Data::new(oidc_client);
```

### 2. 复用客户端实例
```rust
// 在 handler 中
async fn oauth_authorize(
    oidc_client: web::Data<OidcClient>,
) -> impl Responder {
    let auth_req = oidc_client.generate_auth_url();
    // ...
}
```

### 3. 安全地存储 state 和 verifier
```rust
// 使用 Cookie 或 Redis 存储临时数据
cookie_jar.add(
    Cookie::build("oauth_state", state)
        .http_only(true)
        .secure(true)
        .same_site(SameSite::Lax)
);
```

## 故障排查

### Discovery 失败
```rust
// 错误: Failed to discover OIDC provider metadata
// 解决: 检查 issuer_url 是否正确，是否可访问
```

### JWT 验证失败
```rust
// 错误: Failed to verify ID token
// 可能原因:
// 1. Nonce 不匹配 - 检查 nonce 是否正确传递
// 2. Token 过期 - 检查系统时间
// 3. Audience 不匹配 - 检查 client_id 配置
```

## 迁移指南

如果您之前使用的是 `oauth2` + `jsonwebtoken`，迁移步骤：

1. **替换依赖**
   ```toml
   # 移除
   # oauth2 = "4.4"
   # jsonwebtoken = "9.2"
   
   # 添加
   openidconnect = "3.5"
   ```

2. **更新导入**
   ```rust
   use openidconnect::{
       core::{CoreClient, CoreProviderMetadata, ...},
       ...
   };
   ```

3. **简化代码**
   - 删除手动 JWT 验证代码
   - 删除手动 JWKS 获取代码
   - 使用 `discover_async` 替代手动 discovery

## 参考资源

- [openidconnect crate 文档](https://docs.rs/openidconnect/)
- [OpenID Connect Core 1.0 规范](https://openid.net/specs/openid-connect-core-1_0.html)
- [OAuth 2.0 规范](https://tools.ietf.org/html/rfc6749)
- [PKCE 规范](https://tools.ietf.org/html/rfc7636)

---

**结论**: 使用 `openidconnect` crate 是实现 OIDC 的最佳选择，它提供了完整、安全、易用的 API，显著减少了样板代码和潜在的安全漏洞。
