---
sidebar_label: 全链路传输安全与压缩
title: 全链路传输安全与压缩
description: 各层 TLS/SASL、传输压缩与落盘压缩概览
toc_max_heading_level: 3
---

## 概述

TDengine TSDB 的传输安全（TLS/SASL）与压缩（WebSocket/gRPC/TCP/落盘）按六层分层架构逐层展开——从用户侧的入口（应用、Web UI、CLI），经接入层的两条路径（WebSocket/REST 与 taosc 私有协议），延伸到数据采集链路、集群内部，再到运维侧的可观测性接入，最后进入存储层落盘压缩。传输安全与传输压缩共享同一条物理链路，本文按链路统一讲解。

:::note 应用层 / 客户端
客户端 SSL/TLS（TrustStore、`wss`/`useSSL`、REST HTTPS）、Token 与连接池等应用层实践见 [客户端与连接器安全](./05-client-connector-security.md)。自签名证书生成步骤见文末 [附录](#11-appendix-self-signed-cert)。
:::

:::note
WebSocket / REST 对外加密在 **taosAdapter** 上配置（`[ssl]`，企业版）。原生协议（`taosc` ↔ `taosd`）见下文第 2.2 节，部署加固见 [安全部署与加固建议](./08-security-hardening.md)。
:::

```mermaid
graph LR
    classDef app   fill:#f39c12,color:#000,stroke:#d68910
    classDef svc   fill:#1a6ea8,color:#fff,stroke:#0d4f7d
    classDef core  fill:#2c3e50,color:#fff,stroke:#1a252f
    classDef agent fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef ops   fill:#16a085,color:#fff,stroke:#0e6655
    classDef disk  fill:#7f8c8d,color:#fff,stroke:#566573

    APP(["应用 / 连接器"])
    CLI(["CLI 工具"])
    TE(["taosExplorer :6060"])
    TA(["taosAdapter :6041<br/>HTTPS/WSS + deflate"])
    TAOSC(["taosc<br/>（原生客户端库）<br/>TLS + SASL + zlib"])
    TD[("taosd :6030<br/>TLS + SASL + zlib")]
    TX(["taosX :6050/:6055"])
    AGT(["taosX-Agent"])
    DN(["DNode ↔ DNode"])
    TK(["taosKeeper :6043"])
    DSK[("存储<br/>三级压缩+TDE")]

    APP -->|"① 入口/接入 TLS+deflate"| TA
    APP -.->|"① 嵌入 taosc"| TAOSC
    CLI -->|"① CLI TLS/WSS"| TA
    CLI -.->|"① 嵌入 taosc"| TAOSC
    TE  -->|"① Web UI HTTPS"| TA
    TA  -->|"② 内部调用 taosc"| TAOSC
    TAOSC -->|"私有协议 :6030<br/>TLS + SASL/SCRAM + zlib"| TD
    TX  -->|"③ WSS + deflate"| TA
    AGT -->|"③ gRPC TLS + gzip"| TX
    TD  <-->|"④ 节点间 TLS+SCRAM"| DN
    TK  -->|"⑤ HTTPS"| TA
    TD  -->|"⑥ 存储压缩"| DSK

    class APP,CLI app
    class TA,TX,TE,TAOSC svc
    class TD,DN core
    class AGT agent
    class TK ops
    class DSK disk
```

### 各层传输特性概览

| 层级 | 关键机制 |
|------|---------|
| ① 入口层 | 连接器 `useSSL` + `enableCompression`；Explorer HTTPS；CLI `taos+wss://?compression` |
| ② 接入层 | 路径 A：taosAdapter HTTPS/WSS + permessage-deflate；路径 B：taosc（独立原生客户端库）→ taosd **私有协议** TLS + SASL/SCRAM + zlib (`compressMsgSize`) |
| ③ 数据采集链路 | taosX→Adapter WSS + deflate；taosX-Agent→taosX gRPC TLS + gzip；外部源 TLS + 消息体压缩（gzip/snappy/lz4/zstd）|
| ④ 集群内部 | DNode ↔ DNode TCP TLS + SCRAM 互认证 |
| ⑤ 可观测性接入 | taosKeeper → taosAdapter HTTPS + gzip |
| ⑥ 存储层 | 三级落盘压缩（列编码 + lz4/zstd + COMPACT） + TDE 数据加密 |

### 链路明细

| 链路 | TLS 协议 | 压缩协议 | TLS 默认 | 压缩默认 |
|------|---------|---------|---------|---------|
| App/连接器 → taosAdapter | HTTPS / WSS（TLS 1.2/1.3）| WebSocket permessage-deflate | 不启用 | 不启用 |
| CLI → taosAdapter / taosd | HTTPS/WSS 或原生 TLS | deflate / zlib | 不启用 | 不启用 |
| Explorer 浏览器 → Explorer | HTTPS | HTTP gzip | 不启用 | 启用 |
| taosc → taosd | TCP TLS + SASL/SCRAM-SHA-256 | zlib | 不启用 | 不启用 |
| taosX → taosAdapter | HTTPS / WSS | WebSocket permessage-deflate | 不启用 | 不启用 |
| taosX-Agent → taosX | gRPC TLS | gzip | 不启用 | 不启用 |
| 外部源 → taosX / taosX-Agent | TLS（视源端协议）| gzip/snappy/lz4/zstd（消息体）| 视源端 | 视源端 |
| DNode ↔ DNode | TCP TLS + SCRAM 互认证 | — | 不启用 | — |
| taosd → 存储（落盘）| — | 三级压缩（编码+lz4/zstd+COMPACT）| — | **启用**（lz4 medium）|
| taosKeeper → taosAdapter | HTTPS | gzip | 不启用 | 启用 |
| taosAdapter HTTP 响应 | — | gzip（服务端自动）| — | **启用** |

---

## 1. 入口层传输

入口层覆盖三类触达点：程序化入口（应用与各语言连接器）、Web UI 入口（taosExplorer）、命令行入口（taos / taosX / taosBenchmark / taosdump）。在入口层需要统一解决 **TLS（加密）** 与 **传输压缩** 两个问题。

### 1.1 程序化入口（应用 / 各语言连接器）

App / 连接器侧的 TLS 与压缩都落在 **App ↔ taosAdapter** 这一跳（详见 2.1 节的服务端配置），客户端侧只需在 DSN / 连接字符串中开启。更多 SSL/Token 长示例见 [客户端与连接器安全](./05-client-connector-security.md)。

> permessage-deflate 压缩会增加 CPU 负载，建议仅在带宽受限场景（跨 WAN / 云连接）启用。

#### 1.1.1 Python (taosws)

```python
import taosws
# TLS + 压缩
conn = taosws.connect("taos+wss://tduser:SecurePass123!@hostname:6041/db?compression")
# 仅压缩
conn = taosws.connect("taos+ws://tduser:SecurePass123!@hostname:6041/db?compression")
```

#### 1.1.2 Go

```go
// TLS
dsn := "tduser:SecurePass123!@wss(hostname:6041)/db"
// 压缩
dsn := "tduser:SecurePass123!@ws(hostname:6041)/db?enableCompression=true"
// TLS + 压缩
dsn := "tduser:SecurePass123!@wss(hostname:6041)/db?enableCompression=true"
db, _ := sql.Open("taosWS", dsn)
```

#### 1.1.3 JDBC

```java
// TLS
String url = "jdbc:TAOS-WS://hostname:6041/db?user=tduser&password=SecurePass123!&useSSL=true";

// TLS + 压缩
String url2 = "jdbc:TAOS-WS://hostname:6041/db?user=tduser&password=SecurePass123!&useSSL=true&enableCompression=true";

// 自签名证书（仅测试）
String url3 = "jdbc:TAOS-WS://hostname:6041/db?user=tduser&password=SecurePass123!&useSSL=true&disableSSLCertValidation=true";

System.setProperty("javax.net.ssl.trustStore", "/path/to/truststore.jks");
```

#### 1.1.4 Rust

```rust
let taos = TaosBuilder::from_dsn(
    "taos+wss://tduser:SecurePass123!@hostname:6041/db?compression"
)?.build()?;
```

#### 1.1.5 CSharp (.NET)

```csharp
string connStr = "protocol=WebSocket;host=hostname;port=6041;" +
                 "username=tduser;password=SecurePass123!;" +
                 "useSSL=true;enableCompression=true";
```

#### 1.1.6 Node.js

```javascript
const taos = require('@tdengine/websocket');
const conf = new taos.WSConfig('wss://hostname:6041');
conf.setUser('tduser');
conf.setPwd('SecurePass123!');
conf.setDb('db');
const wsSql = await taos.sqlConnect(conf);
// 注：Node.js 连接器当前不支持传输压缩参数
```

#### 1.1.7 ODBC

```text
URL=https://hostname:6041
```

> ODBC 原生模式的 TLS 由 `taos.cfg` 的 `enableTLS=1` 控制，DSN 不支持 `EnableTLS` 参数。

### 1.2 Web UI 入口（taosExplorer）

taosExplorer 在入口层承担两段 TLS：**浏览器 → Explorer** 与 **Explorer → taosAdapter / taosX**。

```toml
# /etc/taos/explorer.toml
[ssl]
certificate     = "/etc/taos/certs/explorer.pem"
certificate_key = "/etc/taos/certs/explorer-key.pem"
```

配置完成后浏览器需使用 `https://explorer-host:6060` 访问；HTTP 响应由 Explorer 自动 gzip 压缩。

向后对接时（Explorer → taosAdapter / taosX），在 Explorer 的数据源/后端配置中直接指向 `https://taosadapter-host:6041` / `https://taosX-host:6050` 即可复用后端证书链。

### 1.3 命令行工具入口（CLI 传输）

4 个 CLI 工具在传输层都落到第 2 节的两条接入路径之一。统一的思路：

- **`taos` shell** 用 `-Z/--driver` 切换：`-Z 0` 走 taosc 原生私有协议（TLS 由 `taos.cfg` 控制），`-Z 1` 走 WebSocket（TLS 由 taosAdapter 决定）。`-E/--dsn` 仅用于 TDengine Cloud（`https://...?token=`）。
- **`taosX` / `taosBenchmark` / `taosdump`** 以 DSN 连接：WebSocket/WSS 路径用 `taos+ws://` / `taos+wss://`，开启压缩追加 `?compression`；原生 TCP 路径由 `taos.cfg` 的 `enableTLS=1` + `tlsCaPath/tlsCliCertPath` 控制 TLS。

> **⚠️ 证书路径与凭据同样有暴露风险**：证书文件应 `chmod 600`；命令行传入密码请使用环境变量或交互式输入，避免进入 shell history / `ps` / `/proc/<pid>/cmdline`。

#### 1.3.1 `taos`（交互式 SQL shell）

`taos` 通过 `-Z/--driver` 切换连接驱动：`-Z 0`（默认）走 taosc 原生私有协议到 taosd :6030，TLS 由 `/etc/taos/taos.cfg` 控制；`-Z 1` 走 WebSocket 经 taosAdapter :6041，TLS 由 taosAdapter 的 HTTPS/WSS 配置决定。`-E/--dsn` 仅用于 TDengine Cloud（形如 `https://gw.cloud.taosdata.com?token=...`，强制 TLS）。

```bash
# 方式一：原生 + TLS（taosc 直连 taosd :6030，路径 B）
# 在 /etc/taos/taos.cfg 启用：
#   enableTLS        1
#   tlsCaPath        /etc/taos/certs/ca.pem
#   tlsCliCertPath   /etc/taos/certs/client.pem   # mTLS
#   tlsCliKeyPath    /etc/taos/certs/client-key.pem
taos -h host -P 6030 -u tduser -p -d mydb

# 方式二：WebSocket + TLS（-Z 1 经 taosAdapter :6041，路径 A）
# taosAdapter 启用 HTTPS/WSS 后，taos -Z 1 自动走 WSS
taos -Z 1 -h host -P 6041 -u tduser -p -d mydb

# 方式三：TDengine Cloud（--dsn，强制 HTTPS + token）
taos --dsn "https://gw.cloud.taosdata.com?token=<cloud_token>"
```

#### 1.3.2 `taosX`（数据管道 CLI）

`taosX` 的源端（`-f`/`--from`）与目的端（`-t`/`--to`）均为 DSN，TLS 与压缩在 DSN 中声明。

```bash
# TLS：两端均使用 wss
taosX run \
  -f "taos+wss://tduser:SecurePass123!@src-host:6041/srcdb" \
  -t "taos+wss://tduser:SecurePass123!@dst-host:6041/dstdb"

# TLS + 压缩：追加 ?compression
taosX run \
  -f "taos+wss://tduser:SecurePass123!@src-host:6041/srcdb?compression" \
  -t "taos+wss://tduser:SecurePass123!@dst-host:6041/dstdb?compression"

# 自签名 CA：通过环境变量指向证书路径
export TAOS_SSL_CA=/etc/taos/certs/ca.pem
taosX run -f "taos+wss://..." -t "taos+wss://..."
```

#### 1.3.3 `taosBenchmark`（压测工具）

```bash
# 方式一：WebSocket + TLS + 压缩（通过 DSN）
taosBenchmark -T "taos+wss://tduser:SecurePass123!@host:6041/testdb?compression" \
  -d testdb -t 100 -n 10000

# 方式二：配置文件（设置 host/port/ssl 相关字段，文件权限 600）
cat > bench.json <<'EOF'

{
  "host": "host",
  "port": 6041,
  "user": "tduser",
  "password": "SecurePass123!",
  "connection_mode": "ws",
  "ssl": true,
  "compression": true,
  "databases": [ { "dbinfo": { "name": "testdb" } } ]
}
EOF
chmod 600 bench.json
taosBenchmark -f bench.json
```

#### 1.3.4 `taosdump`（备份/恢复）

```bash
# 方式一：WebSocket + TLS（经 taosAdapter）
taosdump -R --cloud="taos+wss://tduser:SecurePass123!@host:6041/mydb?compression" \
  -D mydb -o /backup/mydb

# 方式二：原生 TCP + TLS（由 /etc/taos/taos.cfg 的 enableTLS=1 + tlsCaPath 控制）
taosdump -h host -P 6030 -u tduser -p -D mydb -o /backup/mydb
```

---

## 2. 接入层传输

> **关于 taosc**：taosc 是 TDengine 的原生客户端库，作为**独立组件**向上提供 C 语言 API 和 DSN 连接接口，向下通过**私有协议**（TCP :6030）与 taosd 集群通信，私有协议上承载 TLS + SASL/SCRAM 与 zlib 传输压缩。路径 A（WebSocket/REST）由 taosAdapter 在服务端内部调用 taosc 完成到 taosd 的最后一跳；路径 B 则由应用/CLI 通过嵌入 taosc 动态库直连 taosd。两条路径最终都经 taosc 进入 taosd，因此 `enableTLS` / `compressMsgSize` 等 taosc 参数对两条路径同时生效（Adapter 侧配置其内部的 taosc，原生连接器则配置嵌入进程内的 taosc）。

接入层是入口层流量在 TDengine 服务端的第一跳，两条路径并列：

- **路径 A**：WebSocket / REST → taosAdapter :6041 → 内部 taosc → taosd（私有协议 :6030）
- **路径 B**：应用/CLI 嵌入 taosc（原生连接器）→ taosd :6030 **私有协议**

两条路径的 TLS / 压缩独立配置，但 taosc → taosd 段共用同一套私有协议栈（TLS + SASL/SCRAM + zlib）。

### 2.1 路径 A：taosAdapter（HTTPS/WSS + permessage-deflate）

#### 2.1.1 服务端 HTTPS/WSS {#211-server-httpswss}

**taosAdapter 服务端**（`/etc/taos/taosadapter.toml`）。`[ssl]` 为企业版能力（示例配置注释中为 `Applicable for the Enterprise Edition`）。参数说明见 [taosAdapter · SSL](../12-operations-and-tooling/03-components/03-taosadapter.md#ssl)。自签名证书可用文末 [附录](#11-appendix-self-signed-cert) 生成。

```toml
[ssl]
# Enable SSL. Applicable for the Enterprise Edition.
enable   = true
# 证书与私钥路径须与实际文件一致（附录默认复制到 /etc/taos/）
certFile = "/etc/taos/server.crt"
keyFile  = "/etc/taos/server.key"
```

:::tip 路径与权限

- 若将证书放在其他目录（如 `/etc/taos/certs/adapter.pem`），请同步修改 `certFile` / `keyFile`。
- 私钥文件权限建议 `600`，属主为运行 taosAdapter 的用户（常见为 `taos`）。
:::

启用后重启并确认：

```bash
sudo systemctl restart taosadapter
sudo systemctl status taosadapter
journalctl -u taosadapter -n 50
# 正常时应能看到类似：SSL is enabled
```

客户端侧 `wss` / `useSSL` / TrustStore 配置见 [客户端与连接器安全 · SSL/TLS](./05-client-connector-security.md#2-ssltls-配置)。

#### 2.1.2 传输压缩（permessage-deflate）

taosAdapter 服务端默认接受客户端的 permessage-deflate 协商请求（硬编码启用），并对所有 HTTP 响应自动 gzip 压缩。客户端按需在 DSN 中开启即可（参见 1.1 节各连接器示例）。

> 压缩会增加 CPU 负载，建议仅在带宽受限场景（跨 WAN / 云连接）启用。

#### 2.1.3 Nginx 反向代理 TLS 卸载

```nginx
server {
    listen 80;
    server_name tdengine.example.com;
    return 301 https://$host$request_uri;
}
server {
    listen 443 ssl http2;
    ssl_certificate     /etc/nginx/certs/server.pem;
    ssl_certificate_key /etc/nginx/certs/server-key.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_ciphers         HIGH:!aNULL:!MD5;
    gzip on;
    location / {
        proxy_pass http://127.0.0.1:6041;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### 2.2 路径 B：taosc → taosd 私有协议（TLS + SASL + zlib）

taosc 作为独立的原生客户端库，使用 TDengine **私有协议**（TCP :6030）与 taosd 通信；私有协议之上承载 TLS 传输加密、SASL/SCRAM-SHA-256 认证与 zlib 消息压缩。原生连接器（C/Java JNI/Python native/Go cgo/Rust native/CSharp）将 taosc 作为动态库嵌入应用进程，其传输行为与 taosAdapter 内部调用的 taosc 完全一致。

#### 2.2.1 证书要求

| 证书类型 | 最低密钥长度 | 说明 |
|---------|-------------|------|
| RSA | 2048 位 | 推荐 4096 位 |
| ECC | 256 位 | 推荐 P-256 / P-384 |
| CA | — | 仅支持 PEM 格式 |

#### 2.2.2 双向 TLS（mTLS）

**taosd 服务端**（`/etc/taos/taos.cfg`）：

```ini
enableTLS         1
tlsCaPath         /etc/taos/certs/ca.pem
tlsSvrCertPath    /etc/taos/certs/server.pem
tlsSvrKeyPath     /etc/taos/certs/server-key.pem
# 双向 TLS（mTLS）
# tlsVerifyClient 1
```

**taosc 客户端**（`/etc/taos/taos.cfg`）：

```ini
enableTLS   1
tlsCaPath   /etc/taos/certs/ca.pem
# mTLS
# tlsCliCertPath /etc/taos/certs/client.pem
# tlsCliKeyPath  /etc/taos/certs/client-key.pem
```

#### 2.2.3 SASL/SCRAM-SHA-256 加固

在 TLS 通道之上叠加 SASL/SCRAM-SHA-256：

- 密码从不在网络上传输（仅传输 HMAC 推导值）
- 防止证书被盗时的中间人攻击
- 客户端同时验证服务端真实性（双向认证）

#### 2.2.4 证书热重载

```sql
ALTER DNODES RELOAD TLS;
ALTER DNODE <dnode_id> RELOAD TLS;

```

部署新证书 → 执行 RELOAD → 新连接自动使用新证书，无需停服。

#### 2.2.5 TCP 压缩（zlib）

```ini
# taos.cfg
# 0=不压缩  1=固定压缩  2=自适应压缩
compressMsgSize 0
```

默认关闭，适合带宽极度受限场景。开启后写入和查询均有一定延迟影响。

#### 2.2.6 原生连接器示例

**C/C++：**

```c
// taos.cfg 的 enableTLS=1 后自动启用 TLS
TAOS *taos = taos_connect("hostname", "tduser", "SecurePass123!", "db", 6030);
```

**Go 原生：**

```go
dsn := "tduser:SecurePass123!@tcp(hostname:6030)/db"
db, _ := sql.Open("taosSql", dsn)
```

**ODBC 原生：**

```text
[TDengine-TLS]
Driver=/usr/lib/libtaos_odbc.so
Server=hostname:6030
UID=tduser
PWD=SecurePass123!
```

---

## 3. 数据采集链路传输

数据采集链路是由 SQL（在 taosd 中）触发的后台数据流任务，链路自外而内：**外部源 → taosX / Agent → taosAdapter → taosd**。

### 3.1 taosX → taosAdapter (WSS Sink + deflate)

taosX 通过 WebSocket 写入 taosAdapter，TLS 与压缩通过 Sink DSN 指定：

```text
# TLS
taos+wss://tduser:SecurePass123!@taosadapter:6041/db1

# TLS + 压缩
taos+wss://tduser:SecurePass123!@taosadapter:6041/db1?compression
```

在 Explorer 创建数据接入任务时，Sink 配置支持勾选 `Enable WebSocket compression`。

### 3.2 taosX-Agent → taosX (gRPC TLS + gzip)

taosX-Agent 通过 Arrow Flight RPC（gRPC / HTTP2）上报数据。

**taosX 服务端**（`/etc/taos/taosX.toml`）：

```toml
[serve]
listen   = "0.0.0.0:6050"
grpc     = "0.0.0.0:6055"
# REST API TLS
ssl_cert = "/etc/taos/cert.pem"
ssl_key  = "/etc/taos/key.pem"
ssl_ca   = "/etc/taos/ca.pem"
# gRPC TLS（不配置则复用 REST 证书）
grpc_ssl_cert = "/etc/taos/grpc-cert.pem"
grpc_ssl_key  = "/etc/taos/grpc-key.pem"
grpc_ssl_ca   = "/etc/taos/grpc-ca.pem"
```

**taosX-Agent**（`/etc/taos/agent.toml`）：

```toml
endpoint    = "https://taosX-server:6055"
token       = "<Explorer 生成的 JWT Token>"
ca          = "/etc/taos/ca.pem"
compression = true
```

`compression = true` 启用 gRPC gzip 压缩，适合 Agent 到 taosX 跨 WAN / 公网回传场景。仅支持 gzip 算法（gRPC 原生）。

### 3.3 外部数据源传输

#### 3.3.1 源端 TLS 连接配置

**MQTT：**

```json
{
    "broker": "mqtts://broker.example.com:8883",
    "tls": {
        "ca_cert": "/path/to/ca.pem",
        "client_cert": "/path/to/client.pem",
        "client_key": "/path/to/client-key.pem"
    }
}
```

**Kafka mTLS:**

```json
{
    "bootstrap_servers": "broker:9093",
    "security_protocol": "SSL",
    "ssl_cafile": "/path/to/ca.pem",
    "ssl_certfile": "/path/to/client.pem",
    "ssl_keyfile": "/path/to/client-key.pem"
}
```

**MySQL / PostgreSQL / MSSQL / Pulsar / OPC-UA：** 在 Explorer 任务配置中统一通过 `tls.enabled = true` + `tls.ca_cert` / `tls.client_cert` / `tls.client_key` 指定（OPC-UA 使用 `SignAndEncrypt` 安全策略时自动启用 TLS）。

#### 3.3.2 MQTT / Kafka 消息体解压

发布方已压缩的消息，taosX 支持自动解压：

| 格式 | 说明 |
|------|------|
| `gzip` | 标准 gzip |
| `zlib` | zlib/deflate |
| `lz4` | LZ4 块压缩 |
| `zstd` | Zstandard |
| `snappy` | Google Snappy |

在 Explorer 任务的 **高级选项** 中指定。

---

## 4. 集群内部传输（DNode ↔ DNode）

MNode、DNode、VNode 之间的内部通信与客户端 → taosd 连接共享同一套 `taos.cfg` 证书配置，`enableTLS 1` 全局生效（对外对内同步启用）。

### 4.1 加密密钥分发

存储加密密钥（DB_KEY）通过 SASL 加密通道在 MNode 与 DNode 间分发：

1. MNode 使用 SVR_KEY 加密 DB_KEY
2. 通过已建立的 TLS + SASL 通道发送到目标 DNode
3. DNode 使用本机 SVR_KEY 解密，仅在内存中使用 DB_KEY

### 4.2 内部通信压缩

DNode 间的 RPC 复用 `compressMsgSize` 配置（参见 2.2.5 节）——一旦在 `taos.cfg` 中打开，客户端 ↔ taosd 与 DNode ↔ DNode 共同受控。集群内部通常走万兆内网，建议保持默认关闭以避免 CPU 开销。

---

## 5. 可观测性接入传输（taosKeeper）

taosKeeper 作为被动接收方汇聚 taosd / taosAdapter / taosX / taosExplorer 推送的监控指标，再经 WebSocket 写回 TSDB 的 `log` 库。默认审计路径（`auditSaveInSelf = 0`）下，企业版 `taosd` 也将审计 JSON 按 `auditInterval` 上报给 Keeper，由 Keeper 写入带 `IS_AUDIT` 的审计库（与 `log` 库分离）；`auditSaveInSelf = 1`（`v3.4.1.0+`）时审计不经 Keeper。详见 [审计与合规](./07-audit-and-compliance.md)。

taosKeeper 自身可暴露 HTTPS 接收端口，并通过 HTTPS 向 taosAdapter 回写。经 Keeper 的审计上报另受 `taosd` 的 `auditHttps` / `auditUseToken` 控制。

**taosKeeper 服务端**（`/etc/taos/taoskeeper.toml`）：

```toml
[ssl]
enable   = true
certFile = "/etc/taos/keeper-cert.pem"
keyFile  = "/etc/taos/keeper-key.pem"
```

**taosKeeper → taosAdapter**：

```toml
[tdengine]
host     = "taosadapter-host"
port     = 6041
username = "tduser"
password = "SecurePass123!"
usessl   = true
```

> Explorer 自身的 HTTPS 配置已归入第 1.2 节（因为它是 Web UI **入口**，而非“向内”的运维接入）。

---

## 6. 存储层：落盘压缩

> 经过第 1～5 节的链路传输之后，数据最终写入 TSDB 存储，在这里再经过一轮落盘压缩以显著降低占用。存储压缩独立于传输链路，对客户端透明。完整 SQL 语法见 [压缩配置](../05-tdengine-sql/03-data-write/03-compress.md)。

### 6.1 三级压缩架构

| 层次 | 名称 | 作用对象 | 配置粒度 |
|------|------|---------|---------|
| 一级 | 列编码（Encoding）| 单列数据，减少数据熵 | 列级 |
| 二级 | 块压缩（Compression）| 数据块，通用压缩算法 | 列级 |
| 三级 | 多块压缩 | 多数据块组合压缩 | COMPACT 时指定 |

### 6.2 列编码算法（一级）

| 算法 | 适用数据类型 | 原理 |
|------|-------------|------|
| `delta-i` | TIMESTAMP、BIGINT | 相邻值差分 |
| `delta-d` | FLOAT、DOUBLE | 浮点差分 |
| `simple8b` | TINYINT/SMALLINT/INT/BIGINT | 紧凑整数打包 |
| `bit-packing` | BOOL | 8 布尔值打入 1 字节 |
| `disabled` | VARCHAR/NCHAR/BINARY | 不编码，直走二级 |

### 6.3 块压缩算法（二级）

| 算法 | 特点 | 适用场景 |
|------|------|---------|
| `lz4` | 速度最快 | **默认**，高频写入 |
| `zstd` | 压缩率高 | 需要更高压缩比 |
| `zlib` | 通用型 | 对 CPU 不敏感 |
| `xz` | 最高压缩率 | 冷数据归档 |
| `tsz` | 时序有损 | 浮点传感器，允许精度损失 |
| `disabled` | 不压缩 | 极低延迟场景 |

### 6.4 SQL 配置

```sql
CREATE STABLE meters (
    ts        TIMESTAMP ENCODE 'delta-i'  COMPRESS 'lz4'  LEVEL 'medium',
    current   FLOAT     ENCODE 'delta-d'  COMPRESS 'zstd' LEVEL 'high',
    voltage   INT       ENCODE 'simple8b' COMPRESS 'lz4'  LEVEL 'low',
    note      VARCHAR(64)                 COMPRESS 'lz4'
) TAGS (location VARCHAR(64), groupId INT);

ALTER TABLE meters MODIFY COLUMN current COMPRESS 'zstd' LEVEL 'high';

CREATE DATABASE sensor_db COMP 2;     -- 0=关 1=一级 2=两级（默认）
COMPACT DATABASE sensor_db;            -- 触发三级压缩
```

### 6.5 按数据特征选择

| 数据特征 | 推荐配置 | 压缩率参考 |
|---------|---------|-----------|
| 整数传感器 | `delta-i` + `lz4` + `medium` | ~20%（节省 80%）|
| 浮点传感器 | `delta-d` + `lz4` + `medium` | ~30%（节省 70%）|
| 浮点有损 | `delta-d` + `tsz` + `medium` | ~10%（节省 90%）|
| 字符串标签 | `disabled` + `zstd` + `medium` | ~50% |
| 冷归档 | `delta-i` + `xz` + `high` | ~15% |

### 6.6 限制

- 压缩配置仅对普通列有效，TAG 列不支持
- 子表继承超级表压缩配置
- 已有数据需 COMPACT 后使用新算法
- 修改压缩算法后不支持回退到旧版本

---

## 7. 会话安全控制

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `sessionPerUser` | 32 | 单用户最大并发会话数 |
| `sessionConnTime` | 480 min | 会话最长持续时间 |
| `sessionConnIdleTime` | 30 min | 空闲超时 |
| `sessionMaxConcurrency` | 10 | 用户最大并发请求数 |
| `sessionMaxCallVnodeNum` | 10 | 单请求最多涉及 VNode 数 |

```sql
ALTER USER alice SESSIONS 5 CONNECTIONS 20 QUERIES 5;
```

---

## 8. 按网络场景配置建议

| 场景 | TLS | App→Adapter 压缩 | taosc→taosd 压缩 | taosX→Adapter 压缩 | taosX-Agent→taosX 压缩 |
|------|-----|------------------|------------------|---------------------|--------------------|
| 本地 / 局域网 | 可选 | 不启用 | 不启用 | 不启用 | 不启用 |
| 跨机房 WAN | **启用** | 视带宽决定 | 视带宽决定 | 建议启用 | 建议启用 |
| 云端远程 | **启用** | 建议启用 | — | 建议启用 | 建议启用 |

> 压缩会消耗 CPU。基准测试：1 亿行查询，不压缩 ~12 秒，开启 `compression` ~38 秒。局域网不建议开启传输压缩。

---

## 9. 监控与故障排查

### 9.1 存储压缩效果

```sql
SHOW TABLE DISTRIBUTED <db_name>.<table_name>;
SHOW DATABASES;
```

### 9.2 传输压缩验证

```bash
curl -s -D - -H "Accept-Encoding: gzip" \
  "http://localhost:6041/rest/sql" \
  -u tduser:SecurePass123! -d "SELECT server_status()" \
  | grep -i "content-encoding"
# 期望输出：Content-Encoding: gzip
```

### 9.3 错误码

| 错误码 | 含义 | 排查建议 |
|--------|------|---------|
| `0x0375` | TLS 握手失败 | 检查证书有效期和格式 |
| `0x0376` | 证书验证失败 | 检查 CA 证书配置 |
| `0x0377` | SASL 认证失败 | 检查密码，确认 SCRAM-SHA-256 |
| `0x0378` | 会话数量超限 | 调整 `sessionPerUser` |
| `0x0380` | 证书已过期 | 执行 `ALTER DNODES RELOAD TLS` |
| `0x0381` | TLS 版本不兼容 | 确认双端 TLSv1.2+ |

### 9.4 常见问题

| 现象 | 原因 | 解决 |
|------|------|------|
| 存储空间未减小 | 未指定压缩参数 | `ALTER TABLE ... COMPRESS 'lz4'` |
| WebSocket 未压缩 | 客户端未开启 | DSN 添加 `compression` |
| 压缩后写入变慢 | 压缩级别过高 | 降 `LEVEL` 或换 lz4 |
| Agent gRPC 未压缩 | 配置未启用 | agent.toml `compression = true` |

---

## 10. 部署清单（按层次）

### 10.1 入口层

- [ ] 各语言连接器已按需启用 `wss://` + `compression`（跨 WAN 场景）
- [ ] JDBC 已配置 `useSSL=true`，自签名证书已加入信任库
- [ ] taosExplorer 已启用 HTTPS（`explorer.toml` 的 `[ssl]`）
- [ ] CLI（taos / taosX / taosBenchmark / taosdump）的 DSN 已使用 `taos+wss://...?compression`
- [ ] CLI 所用证书与配置文件权限 600

### 10.2 接入层

- [ ] taosAdapter 已启用 HTTPS（`[ssl] enable = true`）
- [ ] taosAdapter 证书文件权限 600
- [ ] 如使用反向代理：Nginx TLS 卸载 + `proxy_pass` 到 6041 已验证
- [ ] taosd `enableTLS 1` 在服务端和客户端均配置
- [ ] 证书为 RSA ≥ 2048 或 ECC ≥ 256，PEM 格式
- [ ] CA 证书已分发到所有 taosc 客户端
- [ ] 熟悉 `ALTER DNODES RELOAD TLS` 热重载
- [ ] 跨 WAN 场景已评估 `compressMsgSize`

### 10.3 数据采集链路

- [ ] taosX Sink DSN 使用 `taos+wss://...?compression`
- [ ] taosX gRPC TLS 证书（`grpc_ssl_*`）已配置
- [ ] taosX-Agent `ca` 参数指向正确 CA
- [ ] taosX-Agent `compression = true`（跨 WAN 场景）
- [ ] 外部数据源（MQTT/Kafka/MySQL/PG/MSSQL/Pulsar/OPC-UA）TLS 已启用
- [ ] MQTT/Kafka 消息体压缩格式（gzip/snappy/lz4/zstd）已在任务高级选项中声明

### 10.4 集群内部

- [ ] 集群内部 TLS 随 `enableTLS` 全局生效
- [ ] 防火墙限制 6030 端口来源
- [ ] 证书过期告警已配置（到期前 30 天）

### 10.5 可观测性接入

- [ ] taosKeeper 已启用 HTTPS
- [ ] taosKeeper → taosAdapter `usessl = true`
- [ ] 敏感配置文件权限 600

### 10.6 存储层

- [ ] 数据库已指定压缩（`COMP 2`，lz4 或 zstd）
- [ ] `ENCODE` 已按数据类型选择
- [ ] `COMPRESS` / `LEVEL` 已按 CPU/空间需求调优
- [ ] 压缩比已验证（`SHOW TABLE DISTRIBUTED`）

## 11 附录：生成自签名证书 {#11-appendix-self-signed-cert}

以下步骤生成测试用自签名证书，供 taosAdapter `[ssl]`（及同类 HTTPS 入口）引用。生产环境请使用受信任 CA 签发的证书。

### 11.1 生成私钥

```bash
# 生成 RSA 2048 位私钥
openssl genrsa -out server.key 2048
```

### 11.2 生成证书签名请求（CSR）

```bash
# 交互式生成 CSR
openssl req -new -key server.key -out server.csr

# 按照提示填写信息（重要：Common Name 必须是你的服务器 IP 或域名）
# 以下是示例值，请根据实际情况修改：
#
# Country Name (2 letter code) [AU]: <YOUR_COUNTRY_CODE>           # 示例: CN
# State or Province Name (full name) [Some-State]: <YOUR_STATE>   # 示例: Beijing
# Locality Name (eg, city) []: <YOUR_CITY>                        # 示例: Beijing
# Organization Name (eg, company) [Internet Widgets Pty Ltd]: <YOUR_ORG>  # 示例: YourCompany
# Organizational Unit Name (eg, section) []: <YOUR_UNIT>           # 示例: IT Department
# Common Name (e.g. server FQDN or YOUR name) []: <YOUR_SERVER_IP_OR_DOMAIN>  # 重要！示例: 192.168.1.100 或 tdserver.example.com
# Email Address []: <YOUR_EMAIL>                                   # 示例: admin@example.com
```

:::tip 关键配置项

- **Common Name (CN)**：必须填入客户端连接时使用的服务器 IP 地址或域名。
- **Subject Alternative Name (SAN)**：必须包含客户端实际连接使用的域名 / IP（现代 TLS 客户端通常优先校验 SAN）。
- 如果客户端使用 `192.168.1.100` 连接，CN / SAN 应包含 `192.168.1.100`。
- 如果客户端使用 `tdserver.example.com` 连接，CN / SAN 应包含 `tdserver.example.com`。
:::

### 11.3 生成自签名证书（有效期 365 天）

```bash
# 推荐：显式添加 SAN（将示例域名/IP 替换为你的实际连接地址）
cat > san.ext <<'EOF'
subjectAltName=DNS:tdserver.example.com,IP:192.168.1.100
EOF

openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt -extfile san.ext
```

### 11.4 将证书和密钥复制到 TDengine 配置目录

```bash
# 假设 TDengine 配置目录为 /etc/taos
sudo cp server.crt /etc/taos/
sudo cp server.key /etc/taos/
sudo chown taos:taos /etc/taos/server.crt /etc/taos/server.key
sudo chmod 600 /etc/taos/server.key
```

### 11.5 在 taosAdapter 中启用 SSL

编辑 `/etc/taos/taosadapter.toml`：

```toml
[ssl]
enable   = true
certFile = "/etc/taos/server.crt"
keyFile  = "/etc/taos/server.key"
```

```bash
sudo systemctl restart taosadapter
sudo systemctl status taosadapter
journalctl -u taosadapter -n 50   # 确认出现 SSL is enabled
```

完整接入层说明见上文 [2.1.1 服务端 HTTPS/WSS](#211-server-httpswss)；客户端配置见 [客户端与连接器安全](./05-client-connector-security.md)。
