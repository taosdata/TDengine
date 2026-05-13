# OAuth 集成到 main.rs 指南

本文档说明如何将 OAuth 模块集成到 `server/src/main.rs`。

## 步骤 1: 添加模块声明

在 `main.rs` 文件顶部的模块声明部分（约第 67-71 行），添加 OAuth 模块：

```rust
mod favorites;
mod monitor;
mod qid;
mod sql;
mod verification;
mod oauth;  // 添加这一行
```

## 步骤 2: 加载 OAuth 配置

在 `main()` 函数中，加载配置后（约第 193 行之后），添加 OAuth 配置加载：

```rust
// 在 args.cfg_path = Some(path); 之后添加

// Load OAuth configuration from args
let mut oauth_config = args.oauth.clone().unwrap_or_default();
oauth_config.from_env(); // Load from environment variables

// Validate OAuth configuration
if oauth_config.enabled {
    if let Err(e) = oauth_config.validate() {
        tracing::error!("Invalid OAuth configuration: {:#}", e);
        tracing::warn!("OAuth will be disabled");
        oauth_config.enabled = false;
    }
}

tracing::info!("OAuth enabled: {}", oauth_config.enabled);
```

## 步骤 3: 初始化 OAuth 组件

在创建 `favorites` 之后（约第 317 行后），添加 OAuth 组件初始化：

```rust
let favorites = FavoritesSql::new(&data_dir).await?;

// Initialize OAuth components if enabled
let oidc_client = if oauth_config.enabled {
    match oauth::OidcClient::new(oauth_config.clone()).await {
        Ok(client) => {
            tracing::info!("OIDC client initialized successfully");
            Some(client)
        }
        Err(e) => {
            tracing::error!("Failed to initialize OIDC client: {:#}", e);
            tracing::warn!("OAuth will be disabled");
            oauth_config.enabled = false;
            None
        }
    }
} else {
    None
};

// Create SessionManager (always create, even if OAuth is disabled)
let session_manager = oauth::SessionManager::new(favorites.pool().clone());

// Wrap in web::Data for sharing across handlers
let oauth_config_data = web::Data::new(oauth_config.clone());
let session_manager_data = web::Data::new(session_manager);
let oidc_client_data = oidc_client.map(web::Data::new);
```

## 步骤 4: 注册 OAuth 到 App Data

在 `HttpServer::new` 闭包中（约第 323 行），更新 app_data 注册：

```rust
let server = HttpServer::new(move || {
    // ... 现有的 cors 和 app 设置 ...
    
    let mut app = App::new()
        .wrap(TracingLogger::<TaosRootSpanBuilder<Qid>>::new())
        .wrap(cors)
        .wrap(Compress::default())
        .app_data(web::Data::new(
            reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .http1_only()
                .build()
                .expect("Failed to create reqwest client"),
        ))
        .app_data(app_args.clone())
        .app_data(web::Data::new(favorites.clone()))
        .app_data(oauth_config_data.clone())      // 添加 OAuth 配置
        .app_data(session_manager_data.clone());  // 添加 SessionManager
    
    // 如果 OIDC client 存在，也添加它
    if let Some(ref client) = oidc_client_data {
        app = app.app_data(client.clone());
    }
    
    // 继续注册路由...
    app = app
        .route("/api/-/rest/{path:.*}", web::to(rest_proxy))
        // ... 其他路由 ...
```

## 步骤 5: 注册 OAuth 路由

在现有路由注册之后（约第 359 行后，`/api/-/login` 附近），添加 OAuth 路由：

```rust
.route("/api/-/login", web::to(login))
.route("/api/-/oauth/status", web::get().to(oauth::oauth_status))
.route("/api/-/oauth/authorize", web::get().to(oauth::oauth_authorize))
.route("/api/-/oauth/callback", web::get().to(oauth::oauth_callback))
.route("/api/-/oauth/logout", web::post().to(oauth::oauth_logout))
.route("/api/-/import", web::to(import))
// ... 其他路由继续 ...
```

## 步骤 6: 更新 Args 结构添加 OAuth 配置字段

在 `Args` 结构定义中（约第 1517 行附近），添加 OAuth 配置字段：

```rust
#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
struct Args {
    // ... 现有字段 ...
    
    /// OAuth configuration
    #[clap(flatten)]
    #[serde(default)]
    pub oauth: Option<oauth::OAuthConfig>,
    
    // ... 其他字段继续 ...
}
```

## 步骤 7: 启动后台任务清理过期 Session

在服务器启动之前（约第 463 行，server.bind 之前），启动清理任务：

```rust
// Start background task to cleanup expired OAuth sessions
if oauth_config.enabled {
    let session_manager_clone = session_manager_data.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Every hour
        loop {
            interval.tick().await;
            match session_manager_clone.cleanup_expired_sessions().await {
                Ok(count) if count > 0 => {
                    tracing::info!("Cleaned up {} expired OAuth sessions", count);
                }
                Err(e) => {
                    tracing::error!("Failed to cleanup expired sessions: {:#}", e);
                }
                _ => {}
            }
        }
    });
}
```

## 完整的代码片段示例

### 在 main() 函数中的完整集成（伪代码）

```rust
#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // ... 现有的配置加载代码 ...
    
    // Step 2: Load OAuth config
    let mut oauth_config = args.oauth.clone().unwrap_or_default();
    oauth_config.from_env();
    if oauth_config.enabled {
        if let Err(e) = oauth_config.validate() {
            tracing::error!("Invalid OAuth configuration: {:#}", e);
            oauth_config.enabled = false;
        }
    }
    
    // ... 日志初始化等 ...
    
    let favorites = FavoritesSql::new(&data_dir).await?;
    
    // Step 3: Initialize OAuth components
    let oidc_client = if oauth_config.enabled {
        match oauth::OidcClient::new(oauth_config.clone()).await {
            Ok(client) => Some(client),
            Err(e) => {
                tracing::error!("Failed to initialize OIDC client: {:#}", e);
                oauth_config.enabled = false;
                None
            }
        }
    } else {
        None
    };
    
    let session_manager = oauth::SessionManager::new(favorites.pool().clone());
    let oauth_config_data = web::Data::new(oauth_config.clone());
    let session_manager_data = web::Data::new(session_manager);
    let oidc_client_data = oidc_client.map(web::Data::new);
    
    // Step 7: Start cleanup task
    if oauth_config.enabled {
        let sm = session_manager_data.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                interval.tick().await;
                let _ = sm.cleanup_expired_sessions().await;
            }
        });
    }
    
    // Step 4 & 5: Create server with OAuth routes
    let server = HttpServer::new(move || {
        let mut app = App::new()
            .wrap(TracingLogger::<TaosRootSpanBuilder<Qid>>::new())
            .wrap(cors)
            .wrap(Compress::default())
            .app_data(app_args.clone())
            .app_data(web::Data::new(favorites.clone()))
            .app_data(oauth_config_data.clone())
            .app_data(session_manager_data.clone());
        
        if let Some(ref client) = oidc_client_data {
            app = app.app_data(client.clone());
        }
        
        app = app
            .route("/api/-/login", web::to(login))
            .route("/api/-/oauth/status", web::get().to(oauth::oauth_status))
            .route("/api/-/oauth/authorize", web::get().to(oauth::oauth_authorize))
            .route("/api/-/oauth/callback", web::get().to(oauth::oauth_callback))
            .route("/api/-/oauth/logout", web::post().to(oauth::oauth_logout))
            // ... 其他路由 ...
            ;
        
        // ... 继续其他配置 ...
        app
    });
    
    // ... 绑定和运行服务器 ...
}
```

## 配置示例

### explorer.toml

```toml
# 现有配置...

[oauth]
enabled = true
provider = "keycloak"

[oauth.oidc]
client_id = "taos-explorer"
client_secret = "your-secret-here"
issuer_url = "https://keycloak.example.com/realms/your-realm"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"
scopes = ["openid", "profile", "email"]

[oauth.user_mapping]
username = "preferred_username"
email = "email"
first_name = "given_name"
last_name = "family_name"
roles = "groups"

# NOTE: The `oauth.tdengine` configuration section has been removed in a recent implementation change.
# TDengine credential association is now handled via the binding endpoint:
#   POST /api/-/oauth/bind
# Administrators should pre-provision TDengine users, or users may associate credentials
# via the binding flow. If automated TDengine user provisioning is required, open a feature
# request and implement it as a dedicated, secure provisioning workflow.
```

## 测试步骤

1. **编译项目**
   ```bash
   cd explorer/server
   cargo build
   ```

2. **启动服务器**
   ```bash
   cargo run
   ```

3. **检查日志**
   - 应该看到 "OAuth enabled: true" 或 "OAuth enabled: false"
   - 如果启用，应该看到 "OIDC client initialized successfully"

4. **测试 OAuth 状态端点**
   ```bash
   curl http://localhost:6060/api/-/oauth/status
   ```
   应该返回：
   ```json
   {"enabled": true, "provider": "keycloak"}
   ```

5. **测试授权流程**
   - 访问 http://localhost:6060/api/-/oauth/authorize
   - 应该被重定向到 IdP 登录页面

## 注意事项

1. **数据库迁移**: 确保在启动前运行数据库迁移以创建 oauth_sessions 和 oauth_config 表

2. **HTTPS**: 生产环境中必须使用 HTTPS，否则 OAuth 不安全

3. **密码管理**: 当前实现使用占位符密码，需要实现：
   - TDengine 用户存在性检查
   - 自动创建用户（如果启用）
   - 密码加密存储

4. **错误处理**: OAuth 初始化失败不应该导致服务器无法启动，只是禁用 OAuth 功能

5. **Session 清理**: 后台任务每小时清理一次过期 session，可以根据需要调整频率

## 后续优化

1. **密码加密**: 实现 AES 加密存储 TDengine 密码
2. **Token 刷新**: 实现 refresh_token 自动刷新 access_token
3. **用户凭据绑定（替代自动创建）**: `oauth.tdengine` 配置已被移除，自动创建 TDengine 用户功能不可用。请使用 `POST /api/-/oauth/bind` 将 TDengine 凭据与 OAuth 会话绑定，或由管理员在 TDengine 中预先创建用户。如需恢复自动创建能力，请提出单独的实现计划并在代码与文档中同步更新。
4. **监控指标**: 添加 OAuth 相关的监控指标（登录次数、失败率等）
5. **审计日志**: 记录所有 OAuth 操作以供审计

---

完成以上步骤后，OAuth 功能将完全集成到 taos-explorer 中，可以同时支持传统的 Basic Authentication 和 OAuth 2.0/OIDC 单点登录。
