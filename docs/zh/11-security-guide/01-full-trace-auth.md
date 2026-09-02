---
sidebar_label: 全链路认证
title: 全链路认证
description: 从入口、接入、采集链路到集群与落盘的 TDengine 认证与访问控制
toc_max_heading_level: 3
---

## 概述

TDengine TSDB 的认证体系按六层分层架构逐层展开——从用户侧的入口（应用、Web UI、CLI），经接入层的两条路径（WebSocket/REST 与 taosc 私有协议），延伸到数据采集链路、集群内部，再到运维侧的可观测性接入，最后落到存储与审计访问控制。本文档按此分层逐一介绍认证机制：

```mermaid
graph LR
    classDef app   fill:#f39c12,color:#000,stroke:#d68910,font-weight:bold
    classDef svc   fill:#1a6ea8,color:#fff,stroke:#0d4f7d
    classDef core  fill:#2c3e50,color:#fff,stroke:#1a252f
    classDef agent fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef src   fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ops   fill:#16a085,color:#fff,stroke:#0e6655
    classDef idp   fill:#f5b7b1,color:#000,stroke:#e74c3c

    APP(["应用 / 各语言连接器"])
    CLI(["CLI 工具<br/>taos / taosX / taosBenchmark / taosdump"])

    subgraph TSDB["⚙️ TSDB 集群"]
        TA(["taosAdapter<br/>:6041"])
        TAOSC(["taosc<br/>（原生客户端库）"])
        TD[("taosd<br/>:6030")]
        TX(["taosX<br/>:6050 / :6055"])
        TD2[("taosd :6030<br/>(其他 DNode)")]
    end

    subgraph OPS["运维观测"]
        TE(["taosExplorer<br/>:6060"])
        TK(["taosKeeper<br/>:6043"])
    end

    AGT(["taosX-Agent"])
    subgraph DS["外部数据源"]
        S1["MQTT · Kafka · MySQL · PG<br/>MSSQL · Pulsar · OPC-UA"]
        S2["OPC-DA · KingHistorian<br/>pSpace · PI"]
    end
    IDP(["OAuth/OIDC IdP"])

    APP -->|"① 路径 A: HTTP/WS"| TA
    APP -.->|"① 路径 B: 嵌入 taosc"| TAOSC
    CLI -->|"① CLI 入口"| TA
    CLI -.->|"① CLI 入口 (嵌入 taosc)"| TAOSC
    TA  -->|"② 内部调用 taosc"| TAOSC
    TAOSC -->|"私有协议 :6030 + SASL/SCRAM"| TD
    TX  -->|"③ WebSocket"| TA
    AGT -->|"③ gRPC"| TX
    S2  -.->|"taosX-Agent 主动拉取"| AGT
    S1  -->|"内置连接器"| TX
    TD  <-->|"④ 集群内部"| TD2
    TE  -->|"① Web UI"| TA
    TE  -->|"① Web UI"| TX
    TK  -->|"⑤ WebSocket"| TA
    IDP -->|"① OIDC"| TE

    class APP,CLI app
    class TA,TX,TAOSC svc
    class TD,TD2 core
    class AGT agent
    class S1,S2 src
    class TK,TE ops
    class IDP idp
```

### 令牌类型对照表

TDengine 生态中涉及多种 Token，容易混淆。以下对照表明确各 Token 的签发方、适用链路和使用方式：

| 令牌类型 | 签发方 | 适用链路 | HTTP Header | 默认启用 | 说明 |
|----------|--------|---------|-------------|---------|------|
| Bearer Token | taosd `CREATE TOKEN` | App → taosAdapter | `Authorization: Bearer <token>` | 否（需手动创建）| 企业版长期令牌，避免明文密码 |
| Agent Token | taosd MNode（SQL `CREATE XNODE AGENT`）| taosX-Agent → taosX（gRPC Arrow Flight）| gRPC metadata `x-token` + Handshake Payload | 否（需手动创建）| JWT(HS256)，Agent 注册与心跳的身份凭据 |
| Cloud Token | TDengine Cloud 平台 | App → Cloud | DSN `?token=<token>` | — | 云服务专用 |
| OAuth Token | 外部 IdP | 用户 → taosExplorer | Cookie（httpOnly）| 否（需配置 OAuth）| SSO 单点登录 |

### 各层认证概览

| 层级 | 范围 | 认证机制 | 支持加密 | 默认配置 |
|------|------|----------|----------|----------|
| ① 入口层 · 程序化 | App / 各语言连接器 → taosAdapter 或 taosd | Basic / Bearer Token / 密码 / TOTP | 是（TLS / WSS） | 默认不加密 |
| ① 入口层 · Web UI | 浏览器 → taosExplorer | 用户名/密码；OAuth 2.0/OIDC SSO | 是（HTTPS） | 默认不加密 |
| ① 入口层 · CLI | taos / taosX / taosBenchmark / taosdump | 用户名/密码、DSN、Bearer Token、环境变量 | 是（TLS / WSS） | 默认不加密 |
| ② 接入层 · 路径 A | App → taosAdapter :6041 → taosc → taosd | Basic / Bearer Token | 是（TLS） | 默认不加密 |
| ② 接入层 · 路径 B | 应用嵌入 taosc → taosd :6030（私有协议）| SCRAM-SHA-256 / MD5 / TOTP / TOKEN | 是（TLS） | 默认不加密 |
| ③ 数据采集 | taosX / taosX-Agent / 外部源 | DSN、Bearer Token、Agent 注册 Token、源侧凭据 | 是（HTTPS / gRPC TLS / 源侧 TLS） | 默认不加密 |
| ④ 集群内部 | DNode ↔ DNode / MNode | SCRAM-SHA-256 内部密钥、TLS 动态加载 | 是（TLS） | 默认不加密 |
| ⑤ 可观测性接入 | taosKeeper | 配置文件密码、网络隔离、写回链路认证 | 是（HTTPS） | 默认不加密 |
| ⑥ 存储与审计 | TDE 密钥、落盘数据访问、审计日志 | 查询认证透传、TDE 密钥管理人员权限、`SYSAUDIT` 审计库读权限 | 是（TDE） | 默认不启用 |

---

## 1. 入口层认证

入口层是用户侧访问 TDengine 的三类入口：程序化连接（应用/连接器）、Web UI（taosExplorer）和命令行工具（运维 CLI）。本层关注凭据在用户侧的呈现形式和传递方式，**认证的最终校验发生在 ② 接入层**（taosAdapter 透传或 taosc 直连）。

### 1.1 程序化入口（应用 / 各语言连接器）

应用程序可通过两条路径接入：经 taosAdapter 的 WebSocket/REST（推荐）或通过嵌入 taosc 直连 taosd 的**私有协议**（TCP :6030）。支持的认证方式：

| 方式 | HTTP Header / 用法 | 说明 |
|------|---------------------|------|
| Basic Auth | `Authorization: Basic base64(user:pass)` | 标准 HTTP 基础认证 |
| Bearer Token | `Authorization: Bearer <token>` | 企业版 `CREATE TOKEN` 生成的长期令牌 |

> **注意**：REST API（`/rest/sql`）**不支持** `?user=&password=` 查询参数认证。InfluxDB 写入接口（`/influxdb/v1/write`）支持 `?u=&p=` 参数。

**Bearer Token 由 taosd 签发**（企业版），taosAdapter 透传到 taosd 验证：

```sql
-- 在 taosd 中创建 Bearer Token
CREATE TOKEN
    auth_code 'my_auth_code'
    client_id  'app_server_01'
    expire_time '2026-12-31T23:59:59';

-- 查询 token 值
SELECT token FROM information_schema.ins_tokens WHERE auth_code = 'my_auth_code';
```

**HTTP 示例：**

```bash
# Basic Auth
curl -u tduser:SecurePass123! http://localhost:6041/rest/sql -d "SELECT SERVER_VERSION()"

# Bearer Token（企业版，CREATE TOKEN 生成）
curl -H "Authorization: Bearer eyJ..." http://localhost:6041/rest/sql -d "SHOW DATABASES"
```

多语言连接器的 Token / 客户端 TLS / 动态轮换长示例见 [客户端与连接器安全](./05-client-connector-security.md)。

#### 1.1.1 各语言连接器认证配置

所有连接器都支持用户名/密码认证，高层连接器（WebSocket 模式）经由 taosAdapter 网关。

##### 1.1.1.1 C/C++ 连接器

```c
// 密码认证（嵌入 taosc，经私有协议直连 taosd，见 2.2 节）
TAOS *taos_connect("localhost", "tduser", "SecurePass123!",
                   "mydb", 6030);

// TOTP 认证（企业版，直连 taosd）
TAOS *taos_connect_totp("localhost", "tduser", "SecurePass123!",
                        "123456", "mydb", 6030);
```

##### 1.1.1.2 Python 连接器

```python
import taosws  # WebSocket 连接（经由 taosAdapter）

# 用户名/密码认证
conn = taosws.connect("taos+ws://tduser:SecurePass123!@localhost:6041/mydb")

# Bearer Token 认证（企业版，令牌由 CREATE TOKEN 生成）
conn = taosws.connect("taos+ws://localhost:6041/mydb?bearer_token=<bearer_token>")

# Token 认证（TDengine Cloud）
conn = taosws.connect("taos+wss://cloud.tdengine.com/mydb?token=<cloud_token>")
```

##### 1.1.1.3 Go 连接器

```go
import "github.com/taosdata/driver-go/v3/taosWS"

// WebSocket + 密码（经由 taosAdapter）
dsn := "tduser:SecurePass123!@ws(localhost:6041)/mydb"
db, err := sql.Open("taosWS", dsn)

// Bearer Token 认证（企业版，bearerToken 驼峰命名）
dsn = "ws(localhost:6041)/mydb?bearerToken=<bearer_token>"
db, err = sql.Open("taosWS", dsn)

// Token 认证（TDengine Cloud，wss 协议）
dsn = "wss(gw.cloud.taosdata.com:443)/mydb?token=<cloud_token>"
db, err = sql.Open("taosWS", dsn)
```

##### 1.1.1.4 JDBC 连接器

```java
// WebSocket 认证（经由 taosAdapter）
String wsUrl = "jdbc:TAOS-WS://localhost:6041/mydb?user=tduser&password=SecurePass123!";
Connection conn = DriverManager.getConnection(wsUrl);

// Bearer Token 认证（企业版）
String tokenUrl = "jdbc:TAOS-WS://localhost:6041/mydb?bearerToken=<bearer_token>";

// Token 认证（TDengine Cloud）
String cloudUrl = "jdbc:TAOS-WS://cloud.tdengine.com/mydb?useSSL=true&token=<cloud_token>";
```

##### 1.1.1.5 Rust 连接器

```rust
use taos::*;

// WebSocket 认证（经由 taosAdapter）
let taos = TaosBuilder::from_dsn("taos+ws://tduser:SecurePass123!@localhost:6041/mydb")?.build()?;

// Bearer Token 认证（企业版）
let taos = TaosBuilder::from_dsn("ws://localhost:6041/mydb?bearer_token=<bearer_token>")?.build()?;

// Token 认证（TDengine Cloud）
let taos = TaosBuilder::from_dsn("wss://gw.cloud.taosdata.com/mydb?token=<cloud_token>")?.build()?;
```

##### 1.1.1.6 CSharp (.NET) 连接器

```csharp
// WebSocket 密码认证（经由 taosAdapter）
string connStr = "protocol=WebSocket;host=localhost;port=6041;username=tduser;password=SecurePass123!";
using var conn = new TDengineDriver.TDengineConnection(connStr);

// Bearer Token 认证（企业版，TDengine.Connector >= 3.1.10）
string tokenStr = "protocol=WebSocket;host=localhost;port=6041;bearerToken=<bearer_token>";

// Token 认证（TDengine Cloud）
string cloudStr = "protocol=WebSocket;host=gw.cloud.taosdata.com;useSSL=true;token=<cloud_token>";
```

##### 1.1.1.7 Node.js 连接器

```javascript
const taos = require('@tdengine/websocket');

// WebSocket 密码认证（通过 WSConfig）
const conf = new taos.WSConfig('ws://localhost:6041');
conf.setUser('tduser');
conf.setPwd('SecurePass123!');
conf.setDb('mydb');
const wsSql = await taos.sqlConnect(conf);

// Bearer Token 认证（企业版）
const tokenConf = new taos.WSConfig('ws://localhost:6041');
tokenConf.setDb('mydb');
tokenConf.setBearerToken('<bearer_token>');
const tokenSql = await taos.sqlConnect(tokenConf);

// Token 认证（TDengine Cloud）
const cloudConf = new taos.WSConfig('wss://gw.cloud.taosdata.com');
cloudConf.setDb('mydb');
cloudConf.setToken('<cloud_token>');
const cloudSql = await taos.sqlConnect(cloudConf);
```

> **注**：Node.js 连接器不支持 `taos.connect(url)` 形式的直连 API，需通过 `WSConfig` + `sqlConnect()` 方式连接。

##### 1.1.1.8 ODBC 连接器

ODBC 连接器（Windows）通过 ODBC 数据源管理器配置：

**WebSocket 连接（自建部署，用户名/密码）：**

- 连接类型：WebSocket
- URL：`http://localhost:6041`
- 用户名 / 密码：填写对应凭证

**WebSocket 连接（TDengine Cloud，Token 认证）：**

- 连接类型：WebSocket
- URL：`https://gw.cloud.taosdata.com?token=<cloud_token>`
- 用户名 / 密码：留空

> **注**：ODBC 连接器不支持 `bearerToken`（自建部署 Token 认证），请使用用户名/密码方式。

#### 1.1.2 taosAdapter 插件服务账户（外部系统写入）

外部系统（collectd、StatsD、OpenTSDB）通过 taosAdapter 的协议兼容端点写入时，taosAdapter **不透传来源凭据**，而是用配置文件中声明的独立账户代写。这些账户属于应用入口层的一部分，建议为每个插件创建专用只写账户：

```toml
# /etc/taos/taosadapter.toml
# 安全建议：为每个插件创建专用只写账户，避免使用默认账户

[collectd]
user = "collectd_writer"
password = "CollectdPass123!"
# token = ""               # v3.4.0.0+ 企业版支持 token 认证

[statsd]
user = "statsd_writer"
password = "StatsdPass123!"

[opentsdb_telnet]
user = "opentsdb_writer"
password = "OpentsdbPass123!"
```

### 1.2 Web UI 入口（taosExplorer 认证）

taosExplorer 是 Web 管理界面（默认 :6060），支持两种登录方式。

**方式一：TDengine 用户名/密码登录**

直接使用 taosd 中创建的用户凭据登录 Web 界面。

**方式二：OAuth 2.0 / OIDC 单点登录（SSO）**

taosExplorer 支持对接企业身份提供商（Keycloak、Azure AD、Okta 等）。**注意：taosd 本身不支持 OAuth，Explorer 充当 OIDC Relying Party，用户通过 IdP 认证后由 Explorer 桥接到 taosd 用户/密码认证。**

#### 1.2.1 支持的 Provider 类型

| 类型 | 适用场景 | 认证流程 |
|------|---------|---------|
| `oidc` | 标准 OIDC 提供商（Keycloak、Azure AD、Okta） | Authorization Code + **PKCE** + Nonce |
| `plain` | 标准 OAuth 2.0 提供商（GitHub 等） | Authorization Code + State |
| `custom` | 非标准/遗留 OAuth 服务 | Authorization Code + State |

> **选型建议：** 优先使用 `oidc`（自动发现端点、PKCE 安全增强）。

#### 1.2.2 OIDC 配置示例

```toml
# /etc/taos/explorer.toml
[oauth]
enabled = true
provider = "oidc"

[oauth.provider_display_name]
en = "Enterprise SSO"
zh = "企业单点登录"

[oauth.oidc]
client_id     = "your-client-id"
client_secret = "your-client-secret"
issuer_url    = "https://idp.example.com/realms/taosdata"
redirect_uri  = "https://explorer.example.com:6060/api/-/oauth/callback"
scopes        = ["openid", "profile", "email"]
```

OIDC 配置支持环境变量覆盖（**仅 OIDC 模式**，plain/custom 需使用 TOML）：

| 环境变量 | 说明 |
|----------|------|
| `EXPLORER_OAUTH_ENABLED` | 启用/禁用 OAuth |
| `EXPLORER_OAUTH_CLIENT_ID` | OAuth Client ID |
| `EXPLORER_OAUTH_CLIENT_SECRET` | OAuth Client Secret |
| `EXPLORER_OAUTH_ISSUER_URL` | OIDC Issuer URL |
| `EXPLORER_OAUTH_REDIRECT_URI` | 回调地址 |
| `EXPLORER_OAUTH_SCOPES` | 逗号分隔的 Scope 列表 |

#### 1.2.3 Custom OAuth 配置示例

```toml
# /etc/taos/explorer.toml
[oauth]
enabled = true
provider = "custom"

[oauth.provider_display_name]
en = "Company SSO"
zh = "公司单点登录"

[oauth.custom]
client_id     = "your_client_id"
client_secret = "your_client_secret"
authorize_url = "https://sso.company.com/oauth2/authorize"
token_url     = "https://sso.company.com/oauth2/token"
profile_url   = "https://sso.company.com/oauth2/userinfo"
redirect_uri  = "https://explorer.example.com:6060/api/-/oauth/callback"
```

#### 1.2.4 加密密钥（生产必须）

OAuth Token 在数据库中以 AES-256-GCM 加密存储。**生产环境必须配置加密密钥**：

```bash
export EXPLORER_SECURITY_ENCRYPTION_KEY=$(openssl rand -base64 32)
```

#### 1.2.5 OAuth 认证流程

```text
用户 → Explorer UI "SSO 登录"
  → Explorer 生成 PKCE challenge + state + nonce（OIDC 模式）
  → 302 跳转到 IdP 授权页面
  → 用户在 IdP 完成认证
  → IdP 回调 /api/-/oauth/callback?code=...&state=...
  → Explorer 验证 state，用 code 换取 token
  → 验证 id_token 签名（JWKS）与 claims
  → 创建 Explorer 会话（httpOnly cookie，默认 8 小时）
  → 首次登录需绑定 taosd 用户/密码（POST /api/-/oauth/bind）
```

#### 1.2.6 API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/-/oauth/status` | GET | 查询 OAuth 是否启用 |
| `/api/-/oauth/authorize` | GET | 发起 OAuth 授权流程 |
| `/api/-/oauth/callback` | GET | IdP 回调处理 |
| `/api/-/oauth/bind` | POST | 绑定 taosd 用户凭据 |
| `/api/-/oauth/logout` | POST | 注销 OAuth 会话 |
| `/api/-/oauth/me` | GET | 获取当前会话信息 |

#### 1.2.7 启用 HTTPS（taosExplorer）

```toml
# /etc/taos/explorer.toml
[ssl]
certificate     = "/path/to/certificate.crt"
certificate_key = "/path/to/private.key"
```

### 1.3 命令行工具入口（运维 CLI 认证）

运维场景常用 4 个 CLI 工具：`taos`（交互式 shell）、`taosX`（数据管道 CLI）、`taosBenchmark`（压测）和 `taosdump`（备份/恢复）。所有 CLI 都通过命令行参数或环境变量携带凭据，最终落到 ② 接入层（taosAdapter 或 taosd）验证。

> **凭据暴露风险**：`-p'<password>'` 这类命令行参数会被写入 shell history、进程列表（`ps`）、`/proc/<pid>/cmdline` 等位置。生产运维建议：
>
> 1. 使用环境变量 `TDENGINE_USER` / `TDENGINE_PASSWORD`，或 `-p` 不带值由 CLI 交互式读取；
> 2. 企业版优先使用 Bearer Token（可吊销、可审计、无明文密码）；
> 3. 配置文件需设置 `chmod 600`，避免被其他用户读取；
> 4. 在 shell 前加一个空格（多数发行版 `HISTCONTROL=ignorespace`）可避免写入历史。

#### 1.3.1 `taos`（交互式 SQL shell）

`taos` 支持两种连接方式，由 `-Z/--driver` 切换：**原生直连**（`-Z 0`，默认，taosc 走私有协议到 taosd :6030，接入层路径 B）与 **WebSocket 连接**（`-Z 1`，经 taosAdapter :6041，路径 A）。连接 TDengine Cloud 使用 `-E/--dsn`，形如 `https://gw.cloud.taosdata.com?token=<cloud_token>`。

```bash
# 方式一：原生直连 taosd :6030（-Z 0 默认，可省略）
taos -h host -P 6030 -u tduser -p'SecurePass123!' -d mydb

# 方式一：交互式输入密码（推荐，不在命令行暴露）
taos -h host -P 6030 -u tduser -p -d mydb
# 回车后提示 "Enter password:"

# 方式二：WebSocket 经 taosAdapter :6041
taos -Z 1 -h host -P 6041 -u tduser -p -d mydb

# 方式三：连接 TDengine Cloud（-E/--dsn，仅限云服务）
taos --dsn "https://gw.cloud.taosdata.com?token=<cloud_token>"

# 方式四：环境变量（避免命令行暴露）
export TDENGINE_USER=tduser
export TDENGINE_PASSWORD='SecurePass123!'
taos -h host -P 6030 -d mydb
```

#### 1.3.2 `taosX`（数据管道 CLI）

`taosX` 通过 DSN 连接串工作，源端（`--from` / `-f`）和目的端（`--to` / `-t`）各需一个 DSN，走 WebSocket/WSS（taosAdapter）或原生 TCP 均可。

```bash
# 方式一：用户名/密码（WebSocket 经 taosAdapter）
taosX run \
  -f "taos+ws://tduser:SecurePass123!@src-host:6041/srcdb" \
  -t "taos+ws://tduser:SecurePass123!@dst-host:6041/dstdb"

# 方式一：用户名/密码 + WSS（加密传输）
taosX run \
  -f "taos+wss://tduser:SecurePass123!@src-host:6041/srcdb" \
  -t "taos+wss://tduser:SecurePass123!@dst-host:6041/dstdb"

# 方式二：Bearer Token（企业版）
taosX run \
  -f "taos+ws://src-host:6041/srcdb?bearer_token=<bearer_token>" \
  -t "taos+wss://dst-host:6041/dstdb?bearer_token=<bearer_token>"

# 方式二：TDengine Cloud Token
taosX run \
  -f "taos+wss://cloud.tdengine.com/srcdb?token=<cloud_token>" \
  -t "taos+ws://tduser:SecurePass123!@on-prem:6041/dstdb"

# 方式三：环境变量 + 精简 DSN
export TDENGINE_USER=tduser
export TDENGINE_PASSWORD='SecurePass123!'
taosX run -f "taos+ws://src-host:6041/srcdb" -t "taos+ws://dst-host:6041/dstdb"
```

> 常见子命令：`taosX run`（迁移/订阅/导出）、`taosX check`、`taosX plugin`。所有子命令的连接参数均接受上述 DSN。

#### 1.3.3 `taosBenchmark`（压测工具）

```bash
# 方式一：用户名/密码（命令行参数）
taosBenchmark -h host -P 6030 -u tduser -p'SecurePass123!' -d testdb -t 100 -n 10000

# 方式一：配置文件（user/password 写入 JSON，文件权限 600）
cat > bench.json <<'EOF'

{
  "host": "host",
  "port": 6030,
  "user": "tduser",
  "password": "SecurePass123!",
  "databases": [ { "dbinfo": { "name": "testdb" } } ]
}
EOF
chmod 600 bench.json
taosBenchmark -f bench.json

# 方式二：Bearer Token（WebSocket 模式，企业版；通过 -T/--taos-dsn 或配置项指定）
taosBenchmark -h host -P 6041 -T "taos+ws://host:6041?bearer_token=<bearer_token>" -d testdb

# 方式三：环境变量
export TDENGINE_USER=tduser
export TDENGINE_PASSWORD='SecurePass123!'
taosBenchmark -h host -P 6030 -d testdb
```

#### 1.3.4 `taosdump`（备份/恢复）

```bash
# 方式一：用户名/密码导出（默认 taosc 私有协议）
taosdump -h host -P 6030 -u tduser -p'SecurePass123!' -D mydb -o /backup/mydb

# 方式一：导入
taosdump -h host -P 6030 -u tduser -p'SecurePass123!' -i /backup/mydb

# 方式一：不在命令行暴露密码（交互式输入）
taosdump -h host -P 6030 -u tduser -p -D mydb -o /backup/mydb

# 方式二：Bearer Token（WebSocket 模式，企业版）
taosdump -R -h host -P 6041 \
  --cloud="taos+ws://host:6041/mydb?bearer_token=<bearer_token>" \
  -D mydb -o /backup/mydb

# 方式二：TDengine Cloud Token
taosdump -R --cloud="taos+wss://cloud.tdengine.com/mydb?token=<cloud_token>" \
  -D mydb -o /backup/mydb

# 方式三：环境变量
export TDENGINE_USER=tduser
export TDENGINE_PASSWORD='SecurePass123!'
taosdump -h host -P 6030 -D mydb -o /backup/mydb
```

---

## 2. 接入层认证

> **关于 taosc**：taosc 是 TDengine 的原生客户端库，作为独立组件向上提供 C 语言 API 和 DSN 连接接口，向下通过**私有协议**（TCP :6030）与 taosd 集群通信并完成认证（SASL/SCRAM、TOTP、TOKEN 等协议层握手）。路径 A（WebSocket/REST）由 taosAdapter 在服务端内部调用 taosc 完成到 taosd 的最后一跳；路径 B（原生连接）则由应用/CLI 通过嵌入 taosc 动态库直连 taosd。两条路径最终都经 taosc 进入 taosd，共用同一套用户账号与认证机制。

接入层是入口层流量在 TDengine 服务端的第一跳。它有 **两条并列路径**，由客户端选择——但最终都通过**同一套 taosd 用户账号体系**进行认证与授权：

- **路径 A：WebSocket / REST** — App/CLI → **taosAdapter :6041** → 内部 taosc → taosd（私有协议 :6030）
- **路径 B：taosc → taosd 私有协议** — App/CLI 将 taosc 作为动态库嵌入 → **taosd :6030**

> **共享账号**：无论从路径 A 还是路径 B 进入，用户账号、密码、IP 白名单、锁定策略、TOKEN 等都存储在 taosd 中；taosAdapter 只做协议转换与凭据透传，不维护独立账号；taosc（无论是 Adapter 内部调用还是嵌入在连接器中）执行最终的私有协议认证握手。

### 2.1 路径 A：WebSocket / REST 经 taosAdapter

taosAdapter 是应用程序通过 RESTful API 和 WebSocket 接入 TDengine 的统一网关。所有高层语言连接器（Python/Java/Go/Rust/CSharp/Node.js/ODBC）的 WebSocket 模式均通过此链路（入口层示例见 1.1.1 节）。

#### 2.1.1 服务端支持的认证方式

| 方式 | HTTP Header | 说明 |
|------|-------------|------|
| Basic Auth | `Authorization: Basic base64(user:pass)` | 标准 HTTP 基础认证，taosAdapter 解包后以用户名/密码送给 taosc |
| Bearer Token | `Authorization: Bearer <token>` | 企业版长期令牌，taosAdapter 透传到 taosd 验证 |

taosAdapter **不透传来源 IP 作为登录 IP**（除非配置 `X-Forwarded-For` 解析），因此 taosd 账户级别的 `HOST` 白名单看到的是 taosAdapter 所在的主机 IP。如需按客户端 IP 做访问控制，应在 taosAdapter 前端（API 网关 / Nginx）执行。

#### 2.1.2 启用 HTTPS（taosAdapter）

taosAdapter 默认不加密。启用 HTTPS/WSS：

```toml
# /etc/taos/taosadapter.toml
[ssl]
enable   = true
certFile = "/etc/taos/certs/adapter.pem"
keyFile  = "/etc/taos/certs/adapter.key"
```

> **注意：** 仅支持 PEM 格式证书。详细 TLS 配置参见 [全链路传输安全](./02-full-trace-transport.md) 第 1 节。

### 2.2 路径 B：taosc → taosd 私有协议

taosc 是 TDengine 的原生客户端库，作为**独立组件**通过 TCP :6030 直连 taosd，使用 TDengine **私有协议**（内含 SASL/SCRAM 握手与 RPC 帧）。应用可通过 C API 或原生 DSN（如 Go 的 `taosSql`、JDBC 的 `jdbc:TAOS://`）使用此链路；原生连接器（C/Java JNI/Python native/Go cgo/Rust native/CSharp）均将 taosc 作为动态库嵌入，其传输与认证行为与 taosAdapter 内部调用 taosc 完全一致。

#### 2.2.1 支持的认证方式

| 认证方式 | 协议层 | 版本要求 |
|----------|--------|----------|
| 用户名 + 密码（SCRAM-SHA-256）| TCP/TLS | 企业版 |
| 用户名 + 密码（MD5，向后兼容）| TCP | 社区版 |
| TOTP 多因素认证 | TCP/TLS | 企业版 |
| TOKEN 令牌认证 | TCP/TLS/HTTP | 企业版 |
| IP 白名单/黑名单 | 网络层 | 企业版 |
| 时间段限制登录 | 应用层 | 企业版 |

#### 2.2.2 用户管理

完整语法与参数说明以 SQL 手册 [用户管理](../05-tdengine-sql/07-user-and-privilege/01-user.md) 为准；下文为全链路场景下的摘录。

**创建用户（完整语法）：**

```sql
CREATE USER [IF NOT EXISTS] <用户名>
    PASS '<口令>'
    [TOTPSEED '<totpseed>']          -- 指定 TOTP 密钥种子（企业版）
    [ACCOUNT LOCK | ACCOUNT UNLOCK]  -- 初始锁定状态
    [
        SYSINFO { 0 | 1 }            -- 是否可查看系统信息（默认 1）
        CREATEDB { 0 | 1 }           -- 是否可建库（默认 0）
        CHANGEPASS { 0 | 1 | 2 }     -- 0=不可改，1=必须改，2=可改（默认 2）
        TOTP { 0 | 1 }               -- 是否启用 TOTP（默认 0）企业版
    ]
    [
        SESSIONS <n>                 -- 最大并发连接数（默认 10）

        CONNECTIONS <n>              -- 最大总连接数（默认 100）

        QUERIES <n>                  -- 最大并发查询数（默认 10）

        FAILED_LOGIN_ATTEMPTS <n>    -- 最大失败次数（默认 3）

        PASSWORD_LOCK_TIME <min>     -- 锁定时长分钟（默认 1440）
        PASSWORD_LIFE_TIME <days>    -- 密码有效期天（默认 90）
        PASSWORD_GRACE_TIME <days>   -- 宽限期天（默认 7）
        PASSWORD_REUSE_TIME <days>   -- 不可重用旧密码的天数（默认 30）
        PASSWORD_REUSE_MAX <n>       -- 修改多少次后方可重用（默认 5）
        INACTIVE_ACCOUNT_TIME <days> -- 不活动锁定天数（默认 90）
        CONNECT_IDLE_TIME <min>      -- 空闲连接超时分钟
        CONNECT_TIME <min>           -- 会话最长持续分钟
    ]
    [HOST '<CIDR>' | NOT_ALLOW_HOST '<CIDR>']         -- IP 白名单/黑名单
    [ALLOWED_TIME '<cron>' | DENIED_TIME '<cron>'];   -- 时间段限制
```

**修改用户（ALTER USER）：**

```sql
ALTER USER alice PASS 'NewP@ssw0rd!';
ALTER USER alice ACCOUNT LOCK;
ALTER USER alice ACCOUNT UNLOCK;
ALTER USER alice FAILED_LOGIN_ATTEMPTS 5;
ALTER USER alice ADD HOST '192.168.1.0/24';
```

#### 2.2.3 强口令策略

企业版对密码设置以下强制约束（社区版无此限制）：

- 长度：8~128 位
- 必须包含大写字母、小写字母、数字、特殊字符（`! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`）中的至少三类
- 不允许与用户名相同
- 历史记录检查：受 `PASSWORD_REUSE_TIME` 和 `PASSWORD_REUSE_MAX` 约束

#### 2.2.4 登录失败锁定

连续登录失败 `FAILED_LOGIN_ATTEMPTS` 次后，账户自动锁定：

| 锁定原因 | 自动解锁 | 手动解锁 | 影响已有会话 |
|----------|----------|----------|-------------|
| 失败次数超限 | `PASSWORD_LOCK_TIME` 后自动解锁 | `ALTER USER xxx ACCOUNT UNLOCK` | 不影响 |
| 永久手动锁定 | 不支持 | `ALTER USER xxx ACCOUNT UNLOCK` | 立即断开 |
| 不活动超时 | 不支持 | `ALTER USER xxx ACCOUNT UNLOCK` | 不影响 |
| 密码过期 | 修改密码后解锁 | — | 不影响 |

> **注意**：失败次数超限后，TOKEN 方式登录仍可用；永久锁定后所有方式均不可用。

#### 2.2.5 TOTP 多因素认证

TDengine 实现了基于 RFC 6238 标准的时间型一次性密码（TOTP）。

**配置步骤：**

```sql
-- 1. 创建用户时指定 TOTP 种子
CREATE USER alice PASS 'P@ssw0rd!' TOTPSEED 'my-seed-string' TOTP 1;

-- 2. 获取实际 TOTP 密钥（给 Authenticator App 扫描）
SELECT GENERATE_TOTP_SECRET('my-seed-string');

-- 3. 更新 TOTP 种子（轮换密钥）
ALTER USER alice TOTPSEED 'new-seed-string';
```

**登录流程（启用 TOTP 后）：**

1. 应用调用 `taos_connect_totp(host, user, password, totp_code, db, port)`
2. taosd 先验证密码，再验证 TOTP 验证码（30 秒时间窗口）
3. 两者均通过才建立会话

> 若用户已启用 TOTP 但尚未生成密钥，仍可用密码登录，但只能执行 `UPDATE TOTP` 命令。

#### 2.2.6 TOKEN 令牌认证

TOKEN 方式适合应用程序长期使用，避免在配置中明文存储用户密码。

**创建 TOKEN：**

```sql
CREATE TOKEN
    auth_code 'my_auth_code'
    client_id  'sensor_gateway'
    expire_time '2026-12-31T23:59:59'
    other_info  'production environment';

-- 通过 auth_code 换取实际 token
SELECT token FROM information_schema.ins_tokens WHERE auth_code = 'my_auth_code';
```

**使用 TOKEN 登录：**

```c
// C API（TOKEN 认证，直接传 token 字符串）
TAOS *taos_connect_auth(host, user, token, db, port);
```

#### 2.2.7 原生连接器配置示例

```go
// Go 原生连接（直连 taosd :6030，不经过 taosAdapter）
dsn := "tduser:SecurePass123!@tcp(localhost:6030)/mydb"
db, err := sql.Open("taosSql", dsn)
```

```java
// JDBC 原生连接（直连 taosd :6030）
String url = "jdbc:TAOS://localhost:6030/mydb?user=tduser&password=SecurePass123!";
Connection conn = DriverManager.getConnection(url);
```

---

## 3. 数据采集链路认证

数据采集链路是运行在集群旁的后台数据管道，主要涉及 `taosX`（数据管道引擎）和 `taosX-Agent`（边缘采集代理）。本层要区分 **三种凭据**：

1. **taosX → taosAdapter（Sink 写入）**：taosX 作为客户端向目标 TDengine 写入数据；
2. **taosX 自身接口**：供 taosExplorer 管理和 Agent 连接；
3. **taosX-Agent → taosX**：taosX-Agent 端通过注册 Token 接入；
4. **外部源 → taosX / Agent**：源系统各自的认证协议。

### 3.1 taosX Sink DSN 认证

taosX 通过 DSN 指定连接目标和认证凭证。**推荐使用 WebSocket 方式**，以复用 taosAdapter 的 SSL 加密、IP 白名单等安全能力：

```bash
# 推荐：WebSocket 连接（经由 taosAdapter :6041）
taos+ws://tduser:SecurePass123!@hostname:6041/db1

# 推荐：WebSocket + TLS（需先启用 taosAdapter SSL）
taos+wss://tduser:SecurePass123!@hostname:6041/db1

# Bearer Token 认证（企业版，令牌由 CREATE TOKEN 生成）
taos+ws://hostname:6041/db1?bearer_token=<bearer_token>

# TDengine Cloud（Token 认证）
taos+wss://cloud.tdengine.com/db1?token=<cloud_token>

# 备用：原生 taosc 直连 taosd（:6030），适用于本机部署场景
taos://tduser:SecurePass123!@hostname:6030/db1
```

### 3.2 taosX 自身接口认证

taosX 对外暴露两个接口：

| 接口 | 协议 | 默认端口 | 支持加密 | 默认配置 | 用途 |
|------|------|---------|---------|---------|------|
| REST API | HTTP 1.1 | 6050 | 是（HTTPS） | 不加密 | 供 taosExplorer / 管理工具调用 |
| gRPC | HTTP/2 | 6055 | 是（gRPC TLS）| 不加密 | 供 taosX-Agent 连接 |

**taosX 自身不提供独立的认证机制**。安全建议：

1. **绑定本机地址**（taosX 与 Explorer 在同一主机时）：

```toml
# /etc/taos/taosX.toml
[serve]
listen = "127.0.0.1:6050"
grpc   = "127.0.0.1:6055"
```

2. **启用 HTTPS**（对外暴露时必须，v3.3.6.0+）：

```toml
# /etc/taos/taosX.toml
[serve]
listen   = "0.0.0.0:6050"
grpc     = "0.0.0.0:6055"
ssl_cert = "/etc/taos/certs/taosX.pem"
ssl_key  = "/etc/taos/certs/taosX.key"
ssl_ca   = "/etc/taos/certs/ca.pem"
grpc_ssl_cert = "/etc/taos/certs/grpc.pem"
grpc_ssl_key  = "/etc/taos/certs/grpc.key"
grpc_ssl_ca   = "/etc/taos/certs/ca.pem"
```

启用后同步更新 taosExplorer 连接地址：

```toml
# /etc/taos/explorer.toml
x_api = "https://127.0.0.1:6050"
grpc  = "https://public.domain.name:6055"
```

### 3.3 taosX-Agent 注册认证

taosX-Agent 部署在外部数据源侧（如 OPC-DA、KingHistorian、pSpace、PI 等工控系统旁），负责将数据采集后通过 gRPC 推送给 taosX。采用 **注册制 Agent 凭证**：仅经过 taosX 注册的 Agent 才能建立连接。

1. 在 taosExplorer 中创建 Agent，获取注册 Token
2. 在 Agent 配置中填入 Token 和 taosX gRPC 地址

```toml
# /etc/taos/agent.toml
endpoint = "grpc://taosX-host:6055"
token    = "<agent_registration_token>"
```

3. taosX 启用 HTTPS 后，Agent 与 taosX 之间自动升级为 gRPC TLS 加密连接

> **建议**：在不安全网络（公共网络、跨 IDC）环境下，始终为 taosX 启用 HTTPS，确保 Agent 传输链路安全。

### 3.4 外部数据源认证

不同外部数据源的认证方式各异，在 taosExplorer 的数据源配置页面中配置。taosX / Agent 仅作为客户端，按源协议送出凭据：

| 数据源 | 常用认证机制 |
|--------|-------------|
| MQTT Broker | 用户名/密码；TLS 客户端证书 |
| Kafka | SASL/PLAIN、SASL/SCRAM-SHA-256、SASL/SCRAM-SHA-512；mTLS |
| OPC-UA | 用户名/密码；X.509 证书 |
| OPC-DA | Windows 域账号（DCOM） |
| MySQL / PostgreSQL / MSSQL | 用户名/密码；SSL 证书 |
| Pulsar | JWT Token；OAuth 2.0；TLS |
| InfluxDB / OpenTSDB | HTTP Basic Auth；Token |

> 源侧凭据建议在 Explorer 中以加密字段存储，并为 taosX / Agent 单独开通只读账号。

---

## 4. 集群内部认证（DNode ↔ DNode）

TDengine 集群内所有节点（包括 DNode 与 DNode、MNode 与 DNode、MNode 与 MNode）之间通过 **SASL/SCRAM-SHA-256** 互相认证。

### 4.1 节点间认证机制

- 密钥通过 `taosk` 工具生成并分发
- 节点间加密密钥通过加密通道分发，防止中间人劫持
- 所有节点间通信使用相同的 SCRAM 认证机制，不区分 MNode 和 DNode

### 4.2 TLS 证书动态加载

TLS 证书轮换无需重启服务：

```sql
-- 在线重载 TLS 证书（所有 DNode）
ALTER DNODES RELOAD TLS;

-- 单个 DNode 重载
ALTER DNODE <dnode_id> RELOAD TLS;
```

---

## 5. 可观测性接入认证

可观测性接入层的认证对象主要是 `taosKeeper`。它把各组件上报的监控指标写回 `log` 库；在默认路径（`auditSaveInSelf = 0`）下，企业版审计日志也经 Keeper 写入带 `IS_AUDIT` 的审计库（与 `log` 库分离）。`v3.4.1.0+` 可将 `auditSaveInSelf = 1`，使审计本集群直写、不经 Keeper。详见 [审计与合规](./07-audit-and-compliance.md)。

### 5.1 taosKeeper 认证

taosKeeper 使用 WebSocket 连接 taosAdapter，将指标写入 TDengine `log` 库；经典部署下也可接收 `taosd` 按 `auditInterval` 上报的审计数据并写入审计库。taosd、taosX、taosExplorer 等组件通过 HTTP 将自身指标推送给 taosKeeper。

taosKeeper 对外暴露 HTTP :6043 端口，接收各组件的指标（及默认路径下的审计）上报。当前版本中，taosKeeper 不验证上报方身份，依赖网络隔离保护。经 Keeper 的审计上报可由 `taosd` 侧 `auditHttps` / `auditUseToken` 加强（Token 写入侧通常需 `SYSAUDIT_LOG`）。

**taosKeeper 连接 taosAdapter 的认证配置**：

```toml
# /etc/taos/taoskeeper.toml
[tdengine]
host     = "localhost"
port     = 6041
username = "keeper_writer"    # 请创建专用低权限账户
password = "KeeperPass123!"   # 明文存储，需严格控制文件权限
usessl   = false
```

**已知安全风险与缓解措施：**

| 风险 | 缓解措施 |
|------|---------|
| :6043 端口暴露 | 防火墙限制仅允许集群内部 IP 访问 |
| 配置文件明文密码 | `chmod 600 /etc/taos/taoskeeper.toml` |
| 无上报方认证 | 部署在可信内网，禁止对外暴露；审计上报优先 `auditUseToken = 1` |

---

## 6. 存储与审计访问控制

业务数据落在普通业务库；监控指标默认在 `log` 库；审计日志落在带 `IS_AUDIT` 的**审计库**（默认名常为 `audit`）。三者都不是独立暴露的认证入口：对存储文件的访问依赖前面各层已经完成的认证，而对审计日志的读取通过 `SYSAUDIT` 等角色控制（不是普通“只读业务账号”）。

### 6.1 存储层访问控制

存储层本身不直接面向用户认证——所有落盘数据（vnode WAL、TSDB 数据文件、元数据）都只能通过 ②/③ 层已认证的查询/写入请求访问。与认证相关的要点：

- **落盘数据访问**：查询 / 写入在 taosd 内部已完成用户认证（SCRAM-SHA-256 / Token / TOTP），存储层按用户的 RBAC/MAC 权限（见 7.2 节）过滤数据，**不存在绕过认证直接读写数据文件的合法路径**。
- **TDE 密钥管理**：落盘加密（TDE）使用分级密钥，由企业版 `taosk` 生成；可更新的 `SVR_KEY` / `DB_KEY` 亦可在运行期由管理员通过 `ALTER SYSTEM SET` 轮换。建议将密钥操作与业务读写账号分离。
- **详细内容**：密钥分层、建加密库、安全删除等见 [静态数据保护](./06-data-security.md)。

### 6.2 审计日志访问控制

审计日志写入带 `IS_AUDIT` 标识的**审计库**（默认名常为 `audit`，**不是**监控用的 `log` 库）。落库路径为：经 `taosKeeper`（默认），或 `v3.4.1.0+` 的 `auditSaveInSelf` 本集群直写。配置、级别与表结构见 [审计与合规](./07-audit-and-compliance.md)；权限模型见 [权限管理 · 审计数据库](../05-tdengine-sql/07-user-and-privilege/02-grant.md#审计数据库)。

- 查看审计数据须具备 `SYSAUDIT`；写入由 `SYSAUDIT_LOG` / 系统上报路径完成，业务账号不应持有审计库写权限。
- `auditLevel ≥ 5` 时，对审计表的 `select` 等也可能产生新的审计事件。

---

## 7. 跨层安全控制

以下控制跨越多个层级生效，是对上述分层认证的横向增强。

### 7.1 IP 访问控制 {#ip-访问控制}

IP 访问控制在 taosd 账户级别生效，支持 CIDR 表示法，可同时配置白名单和黑名单。完整语法、增删查与注意事项见 [用户管理 · IP 白名单与黑名单](../05-tdengine-sql/07-user-and-privilege/01-user.md#ip-白名单与黑名单)。

```sql
-- 只允许来自指定网段的连接
ALTER USER alice ADD HOST '192.168.10.0/24';
ALTER USER alice ADD HOST '10.0.0.0/8';

-- 拒绝来自指定 IP 的连接
ALTER USER alice ADD NOT_ALLOW_HOST '203.0.113.5/32';
```

> 规则变更立即生效，立即断开不符合规则的已有连接。

须将 `enableWhiteList` 设为 `1` 后黑白名单才会生效（参数说明见 [taosd](../12-operations-and-tooling/03-components/01-taosd.md)）：

```ini
# taos.cfg
enableWhiteList = 1
```

### 7.2 权限管理（RBAC + MAC）

权限矩阵与 `GRANT`/`REVOKE`/`ROLE` 完整语法见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

TDengine 企业版支持两种权限模型：

- **RBAC（基于角色的访问控制）**：用户 → 角色 → 权限
- **MAC（强制访问控制）**：密级标签强制管控

| 权限范围 | 说明 |
|----------|------|
| 数据库级 | READ / WRITE / ALL |
| 表级 | SELECT / INSERT / UPDATE / DELETE |
| 列级 | 列黑名单（禁止访问指定列） |
| 系统级 | SYSINFO（查看集群信息）、CREATEDB（建库权限） |

```sql
GRANT READ ON db_name.* TO user1;
GRANT WRITE ON db_name.stb1 TO user2;
CREATE ROLE analyst;
GRANT READ ON sensor_db.* TO analyst;
GRANT ROLE analyst TO alice;
REVOKE WRITE ON db_name.* FROM user2;
```

用户 CRUD、强密码、IP 黑白名单、TOTP / Token 等语法见 [用户管理](../05-tdengine-sql/07-user-and-privilege/01-user.md)；授权矩阵与三权分立见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md)。

#### 7.2.1 实践建议 {#实践建议}

1. **最小权限**：按应用拆分账号或 Token，只授予所需库、表与主题权限；避免共享 `root` 凭据。
2. **职责分离**：在企业版 `v3.4.0.0+` 启用三权分立（`SYSDBA` / `SYSSEC` / `SYSAUDIT`），避免单一超级账号同时负责建库、授权与审计。
3. **凭据生命周期**：定期轮换密码与 Token。密码变更后，使用旧密码的连接会被踢除（Token 连接除外），详见 [用户管理](../05-tdengine-sql/07-user-and-privilege/01-user.md)。
4. **审计联动**：关键授权与用户变更应纳入审计，见 [审计与合规](./07-audit-and-compliance.md)。

### 7.3 API 网关认证增强

建议在局域网内部使用 TDengine TSDB。如果必须对外暴露，推荐在前端配置 API 网关：

#### 7.3.1 Nginx 负载均衡 + SSL

```nginx
http {
    upstream tdengine_adapter {
        server 192.168.11.61:6041;
        server 192.168.11.62:6041;
        server 192.168.11.63:6041;
    }

    server {
        listen 443 ssl;
        ssl_certificate     /path/to/certificate.crt;
        ssl_certificate_key /path/to/private.key;

        location / {
            proxy_pass http://tdengine_adapter;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto  $scheme;
            proxy_set_header X-Real-IP          $remote_addr;
        }
    }
}
```

#### 7.3.2 Traefik 安全网关

```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.tdengine.rule=Host(`api.tdengine.example.com`)"
  - "traefik.http.routers.tdengine.entrypoints=https"
  - "traefik.http.routers.tdengine.tls.certresolver=default"
  - "traefik.http.services.tdengine.loadbalancer.server.port=6041"
  - "traefik.http.middlewares.redirect-to-https.redirectscheme.scheme=https"
  - "traefik.http.middlewares.tdengine-ipwhitelist.ipwhitelist.sourcerange=127.0.0.1/32,192.168.1.0/24"
  - "traefik.http.routers.tdengine.middlewares=redirect-to-https,tdengine-ipwhitelist"
```

---

## 8. 安全审计

完整配置、操作级别与查看方式见 [审计与合规](./07-audit-and-compliance.md)。企业版需先存在符合约束的 `IS_AUDIT` 审计库；默认经 `taosKeeper` 落库，`auditSaveInSelf = 1`（`v3.4.1.0+`）时可本集群直写。与认证相关的常见审计项包括：

| 事件类型 | 说明 |
|----------|------|
| 登录 | `auditLevel ≥ 2` 时记录 `login`（Details 含 appName 等） |
| 用户 / 权限变更 | `createUser` / `alterUser` / `dropUser`、`grantPrivileges` / `revokePrivileges`（级别 2） |
| 其他认证相关 DDL | 以 [操作列表](./07-audit-and-compliance.md) 为准；并非所有登录失败 / 锁定细节均单独成行 |

开启示例（`taos.cfg` 或动态 SQL）：

```ini
audit             = 1
auditInterval     = 5000
auditLevel        = 3
auditCreateTable  = 1
# auditHttps / auditUseToken 仅经 Keeper 路径生效
# auditSaveInSelf = 1   # v3.4.1.0+ 本集群直写，可不经 taosKeeper
```

---

## 9. 运维排查

### 9.1 常见错误码

| 错误码 | 含义 | 排查建议 |
|--------|------|----------|
| `0x0375` | 来源 IP 不在白名单 | 检查 `HOST` 配置 |
| `0x0376` | 来源 IP 在黑名单 | 检查 `NOT_ALLOW_HOST` 配置 |
| `0x0379` | 会话数量超限 | 调整 `SESSIONS` 限制 |
| `0x037A` | 并发查询超限 | 调整 `QUERIES` 限制 |
| `0x037B` | 账户锁定（失败次数超限）| `ALTER USER xxx ACCOUNT UNLOCK` |
| `0x037C` | 账户被手动锁定 | `ALTER USER xxx ACCOUNT UNLOCK` |
| `0x037D` | 密码已过期 | 修改密码后重新登录 |
| `0x037E` | TOTP 验证码错误 | 检查时间同步，重新获取验证码 |
| `0x037F` | 在禁止登录时间段内 | 检查 `DENIED_TIME` 配置 |

### 9.2 查询当前用户状态

```sql
SHOW USERS;
SELECT * FROM information_schema.ins_users WHERE name = 'alice';
SELECT * FROM information_schema.ins_tokens;
SHOW CONNECTIONS;
```

---

## 10. 认证部署清单

### 10.1 上线前检查

- [ ] 所有用户已配置强密码（8+ 字符，含大小写/数字/特殊字符）
- [ ] 管理员账户已启用 TOTP 二次认证
- [ ] `root` 默认密码已修改
- [ ] 应用程序使用 Token 认证（非明文密码）
- [ ] IP 白名单已配置（`HOST`），限制可信来源
- [ ] 登录失败锁定策略已配置（`FAILED_LOGIN_ATTEMPTS`）
- [ ] taosKeeper 配置文件权限设为 600
- [ ] taosX Agent Token 已正确配置（未使用该组件则不适用）
- [ ] 企业版已启用审计（`audit = 1`）、已创建 `IS_AUDIT` 审计库，并用 `SYSAUDIT` 账号查看
- [ ] CLI 工具运维使用环境变量或交互式密码，不在命令行直接暴露

### 10.2 OAuth SSO（如启用）

- [ ] `EXPLORER_SECURITY_ENCRYPTION_KEY` 已配置（生产必须，勿使用默认值）
- [ ] OAuth redirect_uri 使用 HTTPS
- [ ] IdP 中已注册正确的 redirect_uri 和 client_id/secret
- [ ] 首次 SSO 登录后用户完成 taosd 凭据绑定
- [ ] OAuth 会话超时策略合理（默认 8 小时）

### 10.3 日常运维

- [ ] 定期检查 `SHOW CONNECTIONS` 清理异常会话
- [ ] 定期轮换 Token（`DROP TOKEN` + 重新生成）
- [ ] 监控认证失败日志，异常增长时排查
- [ ] 密码过期策略已配置（`PASSWORD_LIFE_TIME`）
