# 可视化管理工具-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-28 | 2026-01-28 | 1.0 | 霍琳贺 | 重新整理 |

## 2. 引言

### 2.1 目的

本文档详细描述 TDengine Explorer 的技术架构、设计决策和实现细节。本文档面向开发人员、架构师和技术负责人,提供完整的技术视图。

### 2.2 范围

本文档涵盖 TDengine Explorer 后端服务的完整架构,包括:
- 系统架构和技术栈
- 核心模块设计
- 数据库设计
- API 接口规范
- 安全机制
- 部署和配置

### 2.3 受众

- 后端开发工程师
- 系统架构师
- DevOps 工程师
- 技术负责人

## 3. 术语

| 术语 | 定义 |
| --- | --- |
| TDengine | 高性能时序数据库 |
| taosX | TDengine 的数据同步和迁移工具 |
| Explorer | TDengine 的 Web 管理界面 |
| OAuth 2.0 | 开放授权标准 |
| OIDC | OpenID Connect,基于 OAuth 2.0 的身份认证协议 |
| actix-web | Rust 的高性能异步 Web 框架 |
| SQLite | 嵌入式关系型数据库 |
| WebSocket | 全双工通信协议 |
| CORS | 跨域资源共享 |

## 4. 概述

### 4.1 架构

TDengine Explorer 采用前后端分离的单页应用(SPA)架构,后端使用 **Rust** 和 **actix-web** 框架实现:

```plaintext
┌─────────────────────────────────────────────────────────────┐
│                      浏览器客户端 (SPA)                       │
└─────────────────────────┬───────────────────────────────────┘
                          │ HTTP/WebSocket
┌─────────────────────────▼───────────────────────────────────┐
│         Explorer Backend (Rust + actix-web v1.10.0)        │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐  │
│  │   HTTP   │  OAuth   │   SQL    │  taosX   │  Static  │  │
│  │  Router  │  Module  │  Proxy   │   API    │  Assets  │  │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         SQLite Database (sqlx + WAL mode)            │  │
│  │  - sql_favorites (SQL 收藏)                          │  │
│  │  - oauth_sessions (OAuth 会话加密存储)                │  │
│  │  - registration (taosX 验证信息)                      │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────┬──────────────────────┬───────────────────┘
                  │ taos native          │ HTTP/GRPC
       ┌──────────▼──────────┐   ┌───────▼────────────┐
       │   TDengine Cluster  │   │  taosX Service   │
       │  (deadpool管理连接)  │   │  (REST/GRPC)     │
       └─────────────────────┘   └──────────────────┘
```

### 4.2 技术栈

#### 4.2.1 核心框架

| 技术组件 | 版本/说明 | 用途 |
| --- | --- | --- |
| **Rust** | 2024 Edition | 编程语言,保证内存安全和高性能 |
| **actix-web** | workspace | 异步 HTTP 服务器框架 |
| **tokio** | full features | 异步运行时,多线程调度 |
| **sqlx** | SQLite support | 编译期类型安全的数据库访问 |
| **taos** | workspace | TDengine 原生连接器 (非REST) |
| **deadpool** | workspace | 连接池管理 (TDengine连接) |

#### 4.2.2 认证和安全

| 组件 | 版本 | 用途 |
| --- | --- | --- |
| **openidconnect** | 3.5.0 | OAuth 2.0/OIDC 标准客户端 |
| **actix-session** | 0.11.0 | 会话管理中间件 |
| **aes-gcm** | 0.10 | AES-256-GCM 加密 (会话密码) |
| **aes** | 0.8.4 | AES 算法实现 |
| **cbc** | 0.1.2 | CBC 加密模式 |
| **sha2** | 0.10 | SHA-256 哈希 |
| **hkdf** | 0.12.4 | 密钥派生函数 |
| **hmac** | 0.12.1 | HMAC 消息认证 |
| **rustls** | ring provider | TLS 1.2+ 支持 |

#### 4.2.3 序列化和工具

| 组件 | 用途 |
| --- | --- |
| serde/serde_json | JSON 序列化,支持任意精度数字 |
| tracing/tracing-actix-web | 结构化日志和分布式追踪 |
| rust-embed | 编译时嵌入静态资源 |
| mime_guess | MIME 类型自动检测 |

### 4.3 依赖项

#### 4.3.1 核心依赖

1. **TDengine 集群**: 
  - 原生协议连接 (通过 `taos` crate)
  - REST API 代理 (可选)
  - 连接池管理 (128 连接上限)
1. **taosX 服务** (可选): 
  - REST API (默认 6050 端口)
  - GRPC 接口 (代理管理)
1. **SQLite**: 
  - WAL (Write-Ahead Logging) 模式
  - 自动迁移 (sqlx migrations)
  - 连接池: 4-128 连接

#### 4.3.2 外部集成

1. **OAuth 2.0/OIDC 提供商**:
  - **OIDC**: Google, Azure AD, Keycloak
  - **Plain OAuth**: GitHub, GitLab
  - **Custom**: TDengine Cloud
1. **Grafana** (可选):
  - API 代理转发
  - Dashboard 集成

## 5. 设计考虑

### 5.1 假设和限制

#### 5.1.1 假设

1. Explorer 部署在受信任的网络环境
2. TDengine 集群稳定可用
3. 现代浏览器 (支持 ES6+, WebSocket)
4. 充足的磁盘空间 (日志 + SQLite)

#### 5.1.2 限制

1. **单实例架构**: 不支持水平扩展,会话存储在本地 SQLite
2. **用户名长度**: TDengine 限制 23 字符
3. **并发连接**: 受连接池配置限制 (默认 128)
4. **文件上传**: 受 actix-web multipart 配置限制

### 5.2 设计模式和原则

#### 5.2.1 架构模式

1. **分层架构**:
  - 路由层: actix-web HTTP 路由
  - 业务层: OAuth, SQL, taosX 模块
  - 数据层: SQLite (元数据) + TDengine (业务数据)
1. **代理模式**:
  - TDengine REST API 透传
  - taosX API 代理
  - Grafana API 转发
1. **中间件链**:
  - CORS → 日志追踪 → 压缩 → OAuth 认证 → 路由

#### 5.2.2 Rust 设计原则

1. **所有权系统**: 零拷贝数据传递,编译期内存安全
2. **类型安全**: sqlx 编译期查询验证,避免运行时错误
3. **异步优先**: tokio + async/await,高并发低延迟
4. **错误传播**: Result<T, E> + anyhow 统一错误处理

### 5.3 风险和缓解措施

| 风险 | 缓解措施 |
| --- | --- |
| TDengine 连接耗尽 | deadpool 连接池管理,超时自动回收,最大 128 连接 |
| SQLite 文件损坏 | WAL 模式,定期备份,VACUUM 维护 |
| OAuth 会话泄露 | AES-256-GCM 加密存储,HTTPS 传输,定期清理过期会话 |
| 并发写入冲突 | SQLite busy_timeout=10s,WAL 模式提升并发 |
| 内存泄漏 | Rust 内存安全保证,actix-web 自动资源管理 |

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 HTTP 服务器 (main.rs)

**框架**: actix-web 4.x
**启动流程**:
```rust
#[actix_web::main]
async fn main() -> Result<()> {
    // 1. 解析配置 (TOML + 环境变量)
    // 2. 初始化日志 (taoslog + tracing)
    // 3. 创建 SQLite 连接池
    // 4. 初始化 OAuth 客户端
    // 5. 创建 SessionManager
    // 6. 启动后台会话清理任务 (每小时)
    // 7. 启动 HTTP 服务器
    HttpServer::new(|| {
        App::new()
            .wrap(TracingLogger)  // 分布式追踪
            .wrap(Cors)           // CORS 配置
            .wrap(Compress)       // Gzip 压缩
            .service(routes)      // 注册路由
    })
    .bind(("0.0.0.0", 6060))?
    .run()
    .await
}
```

**配置参数**:
```rust
struct Args {
    port: Option<u16>,              // 默认 6060
    addr: Option<String>,           
    data_dir: Option<String>,       // SQLite 数据目录
    profile: Profile {              // TDengine 配置
        cluster: Option<String>,    // REST API
        cluster_native: Option<String>, // 原生连接 DSN
        x_api: Option<String>,      // taosX API
        grpc: Option<String>,       // taosX GRPC
    },
    oauth: Option<OAuthConfig>,
    security: SecurityConfig,
    monitor: MonitorCfg,
}
```

#### 6.1.2 OAuth 认证模块 (oauth/)

**模块结构**:
```plaintext
oauth/
├── mod.rs           // 模块导出
├── config.rs        // OAuth 配置和验证 (83 tests)
├── session.rs       // 会话管理和 TDengine 同步 (46 tests)
├── client.rs        // OIDC 客户端 (25 tests)
├── plain_client.rs  // 标准 OAuth 2.0 (34 tests)
├── custom_client.rs // TDengine Cloud (43 tests)
├── handlers.rs      // HTTP 处理器
└── middleware.rs    // 认证中间件
```

##### 6.1.2.1 会话管理 (session.rs)

**SessionManager 核心功能:**
```rust
pub struct SessionManager {
    args: Args,
    pool: SqlitePool,
    encryption_key: Vec<u8>,  // AES-256 密钥
}

impl SessionManager {
    // 创建会话并同步 TDengine 用户
    pub async fn create_session(&self, user_info: UserInfo) -> Result<Session> {
        // 1. 生成 TDengine 用户名 (23字符限制)
        let tsdb_username = self.generate_username(&user_info);
        
        // 2. 生成随机密码 (32字符,大小写+数字+特殊字符)
        let tsdb_password = generate_random_password(32);
        
        // 3. 在 TDengine 创建或更新用户
        taos_create_user(&tsdb_username, &tsdb_password).await?;
        
        // 4. AES-GCM 加密密码
        let encrypted = self.encrypt_password(&tsdb_password)?;
        
        // 5. 存储到 SQLite oauth_sessions 表
        sqlx::query("INSERT INTO oauth_sessions ...")
            .bind(&session_id)
            .bind(&encrypted)
            .execute(&self.pool).await?;
        
        Ok(Session { session_id, tsdb_username, ... })
    }
    
    // 每小时清理过期会话
    pub async fn cleanup_expired_sessions(&self) -> Result<()> {
        sqlx::query("DELETE FROM oauth_sessions WHERE expires_at < datetime('now')")
            .execute(&self.pool).await?;
        Ok(())
    }
}
```


**用户名映射算法**:
```rust
// 模式: {provider}_{user_id}_{random_suffix}
// 示例: i_user@example.com_a1b2
// provider 缩写: oidc→i, plain→p, custom→c

fn generate_username(provider: &str, user_id: &str) -> String {
    let abbr = match provider {
        "oidc" => "i",
        "plain" => "p",
        "custom" => "c",
        _ => "u",
    };
    
    let suffix = random_alphanumeric(4);
    let full = format!("{}_{}_{}",  abbr, user_id, suffix);
    
    // 截断到 23 字符 (TDengine 限制)
    full.chars().take(23).collect()
}
```


##### 6.1.2.2 OAuth 客户端 (client.rs)

**客户端枚举**:
```rust
pub enum OAuthClientEnum {
    Oidc(OidcClient),           // openidconnect crate
    Plain(PlainOAuthClient),    // 手动实现 OAuth 2.0
    Custom(CustomOAuthClient),  // TDengine Cloud 特殊流程
}

impl OAuthClientEnum {
    pub async fn new(config: OAuthConfig) -> Result<Self> {
        match config.provider.as_str() {
            "oidc" => {
                let provider_metadata = CoreProviderMetadata::discover_async(
                    IssuerUrl::new(config.oidc.issuer_url)?,
                    async_http_client
                ).await?;
                
                let client = CoreClient::from_provider_metadata(
                    provider_metadata,
                    ClientId::new(config.oidc.client_id),
                    Some(ClientSecret::new(config.oidc.client_secret)),
                )
                .set_redirect_uri(RedirectUrl::new(config.oidc.redirect_uri)?);
                
                Ok(Self::Oidc(client))
            },
            // ...
        }
    }
}
```


##### 6.1.2.3 HTTP 处理器 (handlers.rs)

**API 端点**:
```rust
// GET /api/-/oauth/login?provider=oidc
async fn oauth_login(provider: Query<String>, client: Data<OAuthClientEnum>) -> HttpResponse {
    let (auth_url, csrf_state) = client.authorization_url();
    // 存储 state 到 session
    HttpResponse::Found()
        .append_header(("Location", auth_url.to_string()))
        .finish()
}

// GET /api/-/oauth/callback?code=xxx&state=yyy
async fn oauth_callback(
    params: Query<CallbackParams>,
    client: Data<OAuthClientEnum>,
    session_mgr: Data<SessionManager>,
) -> Result<HttpResponse> {
    // 1. 验证 state (CSRF 防护)
    // 2. 交换 code 获取 token
    let token = client.exchange_code(params.code).await?;
    
    // 3. 获取用户信息
    let user_info = client.get_user_info(&token).await?;
    
    // 4. 创建会话 (同步 TDengine 用户)
    let session = session_mgr.create_session(user_info).await?;
    
    // 5. 设置 Cookie
    Ok(HttpResponse::Found()
        .cookie(Cookie::new("session_id", session.id))
        .append_header(("Location", "/"))
        .finish())
}

// GET /api/-/oauth/me
async fn oauth_me(session: AuthenticatedSession) -> Result<Json<UserInfo>> {
    Ok(Json(session.user_info))
}

// POST /api/-/oauth/logout
async fn oauth_logout(
    session_mgr: Data<SessionManager>,
    session_id: Cookie,
) -> Result<HttpResponse> {
    session_mgr.delete_session(&session_id).await?;
    Ok(HttpResponse::Ok().finish())
}
```


#### 6.1.3 SQL 执行模块 (sql.rs)

**核心功能**:
```rust
// 自动添加 LIMIT (防止大结果集)
pub fn need_limit(sql: &str) -> bool {
    let sql = sql.trim().to_uppercase();
    if !sql.starts_with("SELECT") {
        return false;
    }
    static LIMIT_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"LIMIT\s+(\d+)(?:\s*(?:,\s*(\d+)|OFFSET\s+(\d+)))?\b").unwrap()
    });
    !LIMIT_RE.is_match(&sql)
}

// 执行查询并反序列化
pub async fn query<T: DeserializeOwned>(dsn: &Dsn, sql: &str) -> Result<Vec<T>> {
    let conn = get_connection(dsn).await?;
    conn.query(sql)
        .await
        .context(format!("query sql `{}`", sql))?
        .deserialize::<T>()
        .try_collect::<Vec<_>>()
        .await
        .context(format!("deserialize fetch `{}` data error", sql))
}

// TDengine 连接池管理
static POOL: LazyLock<HashMap<String, Pool<TaosBuilder>>> = LazyLock::new(HashMap::new);

async fn get_connection(dsn: &Dsn) -> Result<Taos> {
    let key = dsn.to_string();
    let pool = POOL.entry(key.clone()).or_insert_with(|| {
        deadpool::managed::Pool::builder(TaosBuilder::from_dsn(&key).unwrap())
            .max_size(128)
            .build()
            .unwrap()
    });
    pool.get().await.context("get connection from pool")
}
```


#### 6.1.4 SQL 收藏模块 (favorites.rs)

**Storage 实现**:
```rust
pub struct Storage {
    pub pool: SqlitePool,
}

impl Storage {
    pub async fn new(data_dir: &str) -> Result<Self> {
        let connect_options = SqliteConnectOptions::from_str(
            &format!("sqlite:{}/explorer.db", data_dir)
        )?
        .create_if_missing(true)
        .busy_timeout(Duration::from_secs(10))
        .journal_mode(SqliteJournalMode::Wal)
        .auto_vacuum(SqliteAutoVacuum::Incremental);
        
        let pool = PoolOptions::new()
            .min_connections(4)
            .max_connections(128)
            .acquire_timeout(Duration::from_secs(60))
            .idle_timeout(Some(Duration::from_secs(3600)))
            .max_lifetime(Some(Duration::from_secs(86400)))
            .connect_with(connect_options)
            .await?;
        
        // 运行数据库迁移
        MIGRATOR.run(&pool).await?;
        
        Ok(Self { pool })
    }
    
    pub async fn add_favorites_sql(
        &self,
        username: &str,
        sql: &str,
        description: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO sql_favorites (username, sql, description) VALUES (?, ?, ?)"
        )
        .bind(username)
        .bind(sql)
        .bind(description)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
```


#### 6.1.5 taosX API 代理模块 (x_api/)

**模块结构**:
```plaintext
x_api/
├── mod.rs
├── tasks.rs       // 任务管理 (CRUD, 启动/停止, 导入/导出)
├── datasource.rs  // 数据源验证和采样
├── agent.rs       // 代理管理
├── transform.rs   // 数据转换 (扁平化, 超级表预览)
├── ws.rs          // WebSocket 实时推送
├── types.rs       // 共享类型定义
└── proxy.rs       // 通用代理逻辑
```


**任务管理示例** (tasks.rs):
```rust
// GET /api/x/tasks
pub async fn get_tasks(
    args: web::Data<Args>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let x_api = args.profile.x_api.as_ref()
        .ok_or(anyhow!("taosX API not configured"))?;
    
    // 代理到 taosX
    let resp = reqwest::get(format!("{}/tasks", x_api))
        .await?
        .json::<Vec<Task>>()
        .await?;
    
    Ok(HttpResponse::Ok().json(resp))
}

// POST /api/x/tasks/{id}/start
pub async fn start_task(
    path: web::Path<String>,
    args: web::Data<Args>,
) -> Result<HttpResponse> {
    let task_id = path.into_inner();
    let x_api = args.profile.x_api.as_ref().unwrap();
    
    reqwest::Client::new()
        .post(format!("{}/tasks/{}/start", x_api, task_id))
        .send()
        .await?;
    
    Ok(HttpResponse::Ok().finish())
}
```


**WebSocket 推送** (ws.rs):
```rust
// GET /api/x/activities/tasks/{cluster_id}/{token}
pub async fn get_ws_tasks_activities(
    path: web::Path<(String, String)>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse> {
    let (cluster_id, token) = path.into_inner();
    
    // 升级到 WebSocket
    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;
    
    // 订阅 taosX 任务活动
    let taosx_ws = connect_to_taosx_ws(cluster_id, token).await?;
    
    // 转发消息
    spawn(async move {
        while let Some(msg) = taosx_ws.next().await {
            session.text(msg).await.ok();
        }
    });
    
    Ok(res)
}
```


#### 6.1.6 监控模块 (monitor.rs)

```rust
pub struct Monitor {
    cfg: MonitorCfg,
    port: u16,
}

impl Monitor {
    pub fn init(&self) {
        if !self.cfg.enabled {
            return;
        }
        
        // 注册 Prometheus 指标
        metrics::counter!("explorer_http_requests_total");
        metrics::histogram!("explorer_http_request_duration_seconds");
        metrics::gauge!("explorer_active_connections");
    }
}
```


#### 6.1.7 安全模块 (security/)

**加密密钥管理**:
```rust
pub struct SecurityConfig {
    encryption_key_file: Option<String>,
}

impl SecurityConfig {
    pub fn load_encryption_key(&self) -> Vec<u8> {
        if let Some(path) = &self.encryption_key_file {
            fs::read(path).expect("read encryption key file")
        } else {
            // 从环境变量生成
            let salt = env::var("EXPLORER_SALT").unwrap_or_default();
            let mut key = [0u8; 32];
            hkdf::Hkdf::<sha2::Sha256>::new(None, salt.as_bytes())
                .expand(b"explorer-session-key", &mut key)
                .unwrap();
            key.to_vec()
        }
    }
}
```


#### 6.1.8 工具模块 (utils/)

**AES-GCM 加密** (utils/aes.rs):
```rust
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

pub fn encrypt(key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key)?;
    let nonce = Nonce::from_slice(b"unique nonce");
    let ciphertext = cipher.encrypt(nonce, plaintext)?;
    Ok(ciphertext)
}

pub fn decrypt(key: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key)?;
    let nonce = Nonce::from_slice(b"unique nonce");
    let plaintext = cipher.decrypt(nonce, ciphertext)?;
    Ok(plaintext)
}
```

#### 6.1.9 静态资源服务

**rust-embed 嵌入**:
```rust
#[derive(RustEmbed)]
#[folder = "dist/"]
struct Assets;

async fn static_assets(path: web::Path<String>) -> impl Responder {
    let file_path = path.as_str();
    
    if let Some(file) = Assets::get(file_path) {
        let mime = mime_guess::from_path(file_path).first_or_octet_stream();
        HttpResponse::Ok()
            .content_type(mime.essence_str())
            .body(file.data.into_owned())
    } else {
        // Fallback to index.html (SPA routing)
        let index = Assets::get("index.html").unwrap();
        HttpResponse::Ok()
            .content_type("text/html")
            .body(index.data.into_owned())
    }
}
```

### 6.2 数据库设计

#### 6.2.1 SQLite 数据库

**位置**: `{data_dir}/explorer.db`
**配置**:
```rust
SqliteConnectOptions::new()
    .journal_mode(SqliteJournalMode::Wal)         // WAL 模式
    .auto_vacuum(SqliteAutoVacuum::Incremental)   // 增量清理
    .busy_timeout(Duration::from_secs(10))        // 锁超时
    .optimize_on_close(true, None)                // 关闭时优化
```

#### 6.2.2 表结构

##### 6.2.2.1 sql_favorites

```sql
CREATE TABLE sql_favorites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    sql TEXT NOT NULL,
    description TEXT,
    is_public BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(username, sql)
);

CREATE INDEX idx_favorites_username ON sql_favorites(username);
CREATE INDEX idx_favorites_public ON sql_favorites(is_public) WHERE is_public = 1;
```

##### 6.2.2.2 oauth_sessions

```sql
CREATE TABLE oauth_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    username TEXT NOT NULL,
    tsdb_username TEXT NOT NULL,
    tsdb_password_encrypted BLOB NOT NULL,  -- AES-GCM 加密
    provider TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_sessions_user_id ON oauth_sessions(user_id);
CREATE INDEX idx_sessions_expires_at ON oauth_sessions(expires_at);
```

##### 6.2.2.3 registration

```sql
CREATE TABLE registration (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject TEXT NOT NULL UNIQUE,
    cid TEXT NOT NULL,
    version TEXT NOT NULL,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 6.2.3 数据访问层

- **ORM**: sqlx (编译期类型检查)
- **迁移**: `sqlx::migrate!()` 自动执行 migrations/
- **连接池**: 4-128 连接,60秒超时
- **事务支持**: 是

### 6.3 数据流图

#### 6.3.1 OAuth 登录流程

```plaintext
Browser          Explorer          OAuth Provider     TDengine
   │                 │                     │               │
   │─1.GET /login──▶│                     │               │
   │◀─2.Redirect────│                     │               │
   │─3.Authorize────────────────────────▶│               │
   │◀─4.Callback+code───────────────────│               │
   │────────────────▶│─5.Exchange token─▶│               │
   │                 │◀─────────────────│               │
   │                 │─6.Get user info──▶│               │
   │                 │◀─────────────────│               │
   │                 │─7.CREATE USER user_xxx PWD '...'─▶│
   │                 │◀──────────────────────────────────│
   │                 │─8.AES encrypt password            │
   │                 │─9.INSERT INTO oauth_sessions      │
   │◀─10.Set Cookie──│                     │               │
```

#### 6.3.2 SQL 查询流程 (原生连接)

```plaintext
Browser  →  Explorer  →  deadpool::Pool<Taos>  →  TDengine (原生协议)
           ↓
         sqlx::Pool<Sqlite>  (收藏/会话)
```

## 7. 接口规范

### 7.1 API 路由表

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| **认证** |  |  |
| POST | /api/-/login | Basic Auth 登录 |
| GET | /api/-/oauth/me | 获取当前用户 |
| POST | /api/-/oauth/logout | 登出 |
| **TDengine 代理** |  |  |
| * | /api/-/rest/{path} | 透传到 TDengine REST API |
| **taosX 任务** |  |  |
| GET | /api/x/tasks | 任务列表 |
| POST | /api/x/tasks | 创建任务 |
| GET | /api/x/tasks/{id} | 任务详情 |
| PATCH | /api/x/tasks/{id} | 更新任务 |
| DELETE | /api/x/tasks/{id} | 删除任务 |
| POST | /api/x/tasks/{id}/start | 启动任务 |
| POST | /api/x/tasks/{id}/stop | 停止任务 |
| POST | /api/x/tasks/start | 批量启动 |
| POST | /api/x/tasks/stop | 批量停止 |
| **WebSocket** |  |  |
| WS | /api/x/activities/tasks/{cluster_id}/{token} | 任务活动推送 |
| WS | /api/x/activities/agents/{cluster_id}/{token} | 代理活动推送 |
| WS | /api/x/metrics/task/{task_id}/{token} | 任务指标推送 |
| **静态资源** |  |  |
| GET | /docs/{path} | 中文文档 |
| GET | /docs-en/{path} | 英文文档 |
| GET | /{path} | SPA 静态资源 (rust-embed) |

## 8. 安全考虑

### 8.1 认证机制

#### 8.1.1 Basic Auth

- 直接透传到 TDengine
- 支持 HTTPS 加密

#### 8.1.2 OAuth 2.0/OIDC

- **CSRF 防护**: state 参数验证
- **Token 加密**: AES-256-GCM 加密存储在 SQLite
- **会话管理**: 定期清理过期会话 (每小时)
- **TDengine 同步**: 为每个 OAuth 用户创建独立 TDengine 用户

### 8.2 数据加密

#### 8.2.1 传输层

- TLS 1.2+ (rustls)
- HTTPS 重定向 (可选)

#### 8.2.2 存储层

```rust
// AES-256-GCM 加密 TDengine 密码
let cipher = Aes256Gcm::new(&key);
let encrypted = cipher.encrypt(&nonce, password.as_bytes())?;

// 存储到 SQLite
sqlx::query("INSERT INTO oauth_sessions (tsdb_password_encrypted) VALUES (?)")
    .bind(&encrypted)
    .execute(&pool)
    .await?;
```

### 8.3 CORS 配置

**严格模式** (默认):
```rust
Cors::default()
    .allowed_origin_fn(|origin, req_head| {
        req_head.headers()
            .get("Host")
            .map(|host| origin.as_bytes().ends_with(host.as_bytes()))
            .unwrap_or(false)
    })
```

**宽松模式** (开发环境):
```rust
Cors::default().allow_any_origin()
```

## 9. 性能和可扩展性

### 9.1 性能指标

| 指标 | 目标 |
| --- | --- |
| API 响应时间 | < 100ms |
| SQL 代理开销 | < 10ms |
| WebSocket 延迟 | < 50ms |
| 并发连接 | 1000+ |
| 内存占用 | < 500MB |

### 9.2 优化措施

#### 9.2.1 连接池

- **TDengine**: deadpool 管理,最大 128 连接
- **SQLite**: sqlx 管理,4-128 连接

#### 9.2.2 异步 I/O

- tokio 多线程运行时
- 非阻塞网络和磁盘 I/O

#### 9.2.3 零拷贝

- Rust 所有权系统
- 引用传递避免拷贝

#### 9.2.4 压缩

- HTTP 响应 Gzip 压缩

### 9.3 可扩展性

#### 9.3.1 当前限制

- 单实例架构
- 会话存储在本地 SQLite

#### 9.3.2 未来改进

- 会话迁移到 TSDB (支持水平扩展)
- Nginx 负载均衡

## 10. 部署和配置

### 10.1 配置文件

**TOML 格式**:
```toml
port = 6060
data_dir = "/var/lib/taos/explorer"
[profile]
cluster = "http://localhost:6041"
cluster_native = "taos://localhost:6030"
x_api = "http://localhost:6050"

[log]
path = "/var/log/taos"
level = "info"
rotation_size = "1GB"

[oauth]
enabled = true
provider = "oidc"

[oauth.oidc]
issuer_url = "https://accounts.google.com"
client_id = "xxx"
client_secret = "yyy"
redirect_uri = "http://localhost:6060/api/-/oauth/callback"

[security]
encryption_key_file = "/etc/taos/explorer.key"
```

### 10.2 环境变量

| 变量 | 默认值 |
| --- | --- |
| EXPLORER_PORT | 6060 |
| EXPLORER_CLUSTER | http://localhost:6041 |
| EXPLORER_X_API | - |
| EXPLORER_DATA_DIR | /var/lib/taos/explorer |
| EXPLORER_LOG_LEVEL | info |

### 10.3 Docker 部署

随 TSDB 启动（`tdengine/tsdb:latest`）。

## 11. 监控和维护

### 11.1 日志

**结构化日志**:
```plaintext
2026-01-29 10:00:00.123 INFO [explorer] [qid:abc123] GET /api/x/tasks 200 15ms
```

**日志轮转**:
- 单文件 1GB
- 保留 30 天
- Gzip 压缩

### 11.2 维护任务

**每小时**:
- 清理过期 OAuth 会话 (自动)
**每周**:
- 备份 SQLite 数据库
- 清理旧日志 (自动)
**每月**:
- VACUUM SQLite
- 安全更新检查

### 11.3 故障排查

**服务无法启动**:
```bash

## 12. 检查端口

lsof -i:6060

## 13. 检查日志

journalctl -u taos-explorer -n 100

## 14. 检查配置

taos-explorer -c config.toml --validate
```

**连接 TDengine 失败**:
```bash

## 15. 测试连接

curl http://tdengine:6041
taos -h localhost -P 6030

## 16. 检查连接池

grep "connection pool" /var/log/taos/explorer.log
```

## 17. 参考资料

1. **源码**: 仓库 taosx，实现：explorer/server
2. **功能需求规格**: [可视化管理工具 - Requirement Spec](https://taosdata.feishu.cn/wiki/XB90wO7VjiFOT1kgA2Mc2Y0Xnhb)
3. **概要设计**：[可视化管理工具 - Functional Spec](https://taosdata.feishu.cn/wiki/HI4HwWAIxiYBMJkuzH4cCzbTnjg)
4. **TDengine**: https://docs.taosdata.com
5. **Rust 文档**:
  - actix-web: https://actix.rs
  - sqlx: https://github.com/launchbadge/sqlx
  - openidconnect: https://docs.rs/openidconnect
