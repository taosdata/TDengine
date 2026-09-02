---
sidebar_label: Full-Trace Transport Security and Compression
title: Full-Trace Transport Security and Compression
description: Overview of TLS/SASL, transport compression, and on-disk compression by layer
toc_max_heading_level: 3
---

## Overview

TDengine TSDB transport security (TLS/SASL) and compression (WebSocket/gRPC/TCP/on-disk) are organized by a six-layer architecture. The path starts from user-facing entry points (applications, Web UI, and CLI), goes through two access-layer routes (WebSocket/REST and the taosc private protocol), extends to data ingestion links, cluster-internal links, and observability ingress, and finally reaches storage-layer on-disk compression. Transport security and transport compression share the same physical links, so this document explains them by link.

:::note Application Layer / Client
Application-layer practices such as client SSL/TLS (TrustStore, `wss`/`useSSL`, REST HTTPS), tokens, and connection pools are described in [Client and Connector Security](./05-client-connector-security.md). For steps to generate a self-signed certificate, see the [appendix](#11-appendix-generate-a-self-signed-certificate).
:::
:::note
Public WebSocket / REST encryption is configured on **taosAdapter** (`[ssl]`, Enterprise Edition). For the native protocol (`taosc` to `taosd`), see section 2.2 below. For deployment hardening, see [Security Deployment and Hardening Recommendations](./08-security-hardening.md).
:::

```mermaid
graph LR
    classDef app   fill:#f39c12,color:#000,stroke:#d68910
    classDef svc   fill:#1a6ea8,color:#fff,stroke:#0d4f7d
    classDef core  fill:#2c3e50,color:#fff,stroke:#1a252f
    classDef agent fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef ops   fill:#16a085,color:#fff,stroke:#0e6655
    classDef disk  fill:#7f8c8d,color:#fff,stroke:#566573

    APP(["Applications / Connectors"])
    CLI(["CLI Tools"])
    TE(["taosExplorer :6060"])
    TA(["taosAdapter :6041<br/>HTTPS/WSS + deflate"])
    TAOSC(["taosc<br/>(Native Client Library)<br/>TLS + SASL + zlib"])
    TD[("taosd :6030<br/>TLS + SASL + zlib")]
    TX(["taosX :6050/:6055"])
    AGT(["taosX-Agent"])
    DN(["DNode to DNode"])
    TK(["taosKeeper :6043"])
    DSK[("Storage<br/>Three-level compression + TDE")]

    APP -->|"1 Entry/access TLS + deflate"| TA
    APP -.->|"1 Embedded taosc"| TAOSC
    CLI -->|"1 CLI TLS/WSS"| TA
    CLI -.->|"1 Embedded taosc"| TAOSC
    TE  -->|"1 Web UI HTTPS"| TA
    TA  -->|"2 Internal taosc call"| TAOSC
    TAOSC -->|"Private protocol :6030<br/>TLS + SASL/SCRAM + zlib"| TD
    TX  -->|"3 WSS + deflate"| TA
    AGT -->|"3 gRPC TLS + gzip"| TX
    TD  <-->|"4 Inter-node TLS + SCRAM"| DN
    TK  -->|"5 HTTPS"| TA
    TD  -->|"6 Storage compression"| DSK

    class APP,CLI app
    class TA,TX,TE,TAOSC svc
    class TD,DN core
    class AGT agent
    class TK ops
    class DSK disk
```

### Transport Feature Overview By Layer

| Layer | Key mechanisms |
|------|---------|
| 1 Entry layer | Connector `useSSL` + `enableCompression`; Explorer HTTPS; CLI `taos+wss://?compression` |
| 2 Access layer | Path A: taosAdapter HTTPS/WSS + permessage-deflate; Path B: taosc (standalone native client library) to taosd **private protocol** TLS + SASL/SCRAM + zlib (`compressMsgSize`) |
| 3 Data ingestion links | taosX to Adapter WSS + deflate; taosX-Agent to taosX gRPC TLS + gzip; external-source TLS + message-body compression (gzip/snappy/lz4/zstd) |
| 4 Cluster-internal links | DNode to DNode TCP TLS + SCRAM mutual authentication |
| 5 Observability ingress | taosKeeper to taosAdapter HTTPS + gzip |
| 6 Storage layer | Three-level on-disk compression (column encoding + lz4/zstd + COMPACT) + TDE data encryption |

### Link Details

| Link | TLS protocol | Compression protocol | TLS default | Compression default |
|------|---------|---------|---------|---------|
| App/connector to taosAdapter | HTTPS / WSS (TLS 1.2/1.3) | WebSocket permessage-deflate | Disabled | Disabled |
| CLI to taosAdapter / taosd | HTTPS/WSS or native TLS | deflate / zlib | Disabled | Disabled |
| Explorer browser to Explorer | HTTPS | HTTP gzip | Disabled | Enabled |
| taosc to taosd | TCP TLS + SASL/SCRAM-SHA-256 | zlib | Disabled | Disabled |
| taosX to taosAdapter | HTTPS / WSS | WebSocket permessage-deflate | Disabled | Disabled |
| taosX-Agent to taosX | gRPC TLS | gzip | Disabled | Disabled |
| External source to taosX / taosX-Agent | TLS (depending on source protocol) | gzip/snappy/lz4/zstd (message body) | Source-dependent | Source-dependent |
| DNode to DNode | TCP TLS + SCRAM mutual authentication | - | Disabled | - |
| taosd to storage (on disk) | - | Three-level compression (encoding + lz4/zstd + COMPACT) | - | **Enabled** (lz4 medium) |
| taosKeeper to taosAdapter | HTTPS | gzip | Disabled | Enabled |
| taosAdapter HTTP responses | - | gzip (automatic on the server) | - | **Enabled** |

---

## 1. Entry-Layer Transport

The entry layer covers three kinds of touchpoints: programmatic entry points (applications and language connectors), Web UI entry points (taosExplorer), and command-line entry points (`taos`, `taosX`, `taosBenchmark`, and `taosdump`). At the entry layer, **TLS (encryption)** and **transport compression** should be handled consistently.

### 1.1 Programmatic Entry Points (Applications / Language Connectors)

TLS and compression on the app / connector side both apply to the **App to taosAdapter** hop. For server-side configuration, see section 2.1. On the client side, enable them in the DSN / connection string. For longer SSL/token examples, see [Client and Connector Security](./05-client-connector-security.md).

> permessage-deflate compression increases CPU load. Enable it only in bandwidth-constrained scenarios, such as cross-WAN or cloud connections.

#### 1.1.1 Python (taosws)

```python
import taosws
# TLS + compression
conn = taosws.connect("taos+wss://tduser:SecurePass123!@hostname:6041/db?compression")
# Compression only
conn = taosws.connect("taos+ws://tduser:SecurePass123!@hostname:6041/db?compression")
```

#### 1.1.2 Go

```go
// TLS
dsn := "tduser:SecurePass123!@wss(hostname:6041)/db"
// Compression
dsn := "tduser:SecurePass123!@ws(hostname:6041)/db?enableCompression=true"
// TLS + compression
dsn := "tduser:SecurePass123!@wss(hostname:6041)/db?enableCompression=true"
db, _ := sql.Open("taosWS", dsn)
```

#### 1.1.3 JDBC

```java
// TLS
String url = "jdbc:TAOS-WS://hostname:6041/db?user=tduser&password=SecurePass123!&useSSL=true";

// TLS + compression
String url2 = "jdbc:TAOS-WS://hostname:6041/db?user=tduser&password=SecurePass123!&useSSL=true&enableCompression=true";

// Self-signed certificate (testing only)
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
// Note: The Node.js connector does not currently support a transport compression parameter.
```

#### 1.1.7 ODBC

```text
URL=https://hostname:6041
```

> TLS in ODBC native mode is controlled by `enableTLS=1` in `taos.cfg`. The DSN does not support an `EnableTLS` parameter.

### 1.2 Web UI Entry Point (taosExplorer)

At the entry layer, taosExplorer handles two TLS segments: **browser to Explorer** and **Explorer to taosAdapter / taosX**.

```toml
# /etc/taos/explorer.toml
[ssl]
certificate     = "/etc/taos/certs/explorer.pem"
certificate_key = "/etc/taos/certs/explorer-key.pem"
```

After configuration, access Explorer from the browser at `https://explorer-host:6060`. Explorer automatically gzip-compresses HTTP responses.

For downstream connections (Explorer to taosAdapter / taosX), point the Explorer data source / backend configuration directly to `https://taosadapter-host:6041` / `https://taosX-host:6050` to reuse the backend certificate chain.

### 1.3 Command-Line Tool Entry Points (CLI Transport)

All four CLI tools use one of the two access-layer paths described in section 2. The unified approach is:

- **`taos` shell** switches drivers with `-Z/--driver`: `-Z 0` uses the taosc native private protocol (TLS controlled by `taos.cfg`), and `-Z 1` uses WebSocket (TLS determined by taosAdapter). `-E/--dsn` is only for TDengine Cloud (`https://...?token=`).
- **`taosX` / `taosBenchmark` / `taosdump`** connect by DSN: use `taos+ws://` / `taos+wss://` for WebSocket/WSS paths and append `?compression` to enable compression. For the native TCP path, TLS is controlled by `enableTLS=1` + `tlsCaPath/tlsCliCertPath` in `taos.cfg`.

> **Certificate paths and credentials can also be exposed**: certificate files should be `chmod 600`; pass command-line passwords through environment variables or interactive input to avoid exposing them in shell history, `ps`, or `/proc/<pid>/cmdline`.

#### 1.3.1 `taos` (Interactive SQL Shell)

`taos` switches connection drivers with `-Z/--driver`: `-Z 0` (default) uses the taosc native private protocol to connect to taosd :6030, with TLS controlled by `/etc/taos/taos.cfg`; `-Z 1` uses WebSocket through taosAdapter :6041, with TLS determined by the taosAdapter HTTPS/WSS configuration. `-E/--dsn` is only for TDengine Cloud, for example `https://gw.cloud.taosdata.com?token=...`, which enforces TLS.

```bash
# Method 1: native + TLS (taosc directly connects to taosd :6030, path B)
# Enable the following in /etc/taos/taos.cfg:
#   enableTLS        1
#   tlsCaPath        /etc/taos/certs/ca.pem
#   tlsCliCertPath   /etc/taos/certs/client.pem   # mTLS
#   tlsCliKeyPath    /etc/taos/certs/client-key.pem
taos -h host -P 6030 -u tduser -p -d mydb

# Method 2: WebSocket + TLS (-Z 1 through taosAdapter :6041, path A)
# After taosAdapter enables HTTPS/WSS, taos -Z 1 automatically uses WSS.
taos -Z 1 -h host -P 6041 -u tduser -p -d mydb

# Method 3: TDengine Cloud (--dsn, enforced HTTPS + token)
taos --dsn "https://gw.cloud.taosdata.com?token=<cloud_token>"
```

#### 1.3.2 `taosX` (Data Pipeline CLI)

Both source (`-f`/`--from`) and destination (`-t`/`--to`) in `taosX` are DSNs. TLS and compression are declared in the DSN.

```bash
# TLS: use wss on both ends.
taosX run \
  -f "taos+wss://tduser:SecurePass123!@src-host:6041/srcdb" \
  -t "taos+wss://tduser:SecurePass123!@dst-host:6041/dstdb"

# TLS + compression: append ?compression.
taosX run \
  -f "taos+wss://tduser:SecurePass123!@src-host:6041/srcdb?compression" \
  -t "taos+wss://tduser:SecurePass123!@dst-host:6041/dstdb?compression"

# Self-signed CA: point to the certificate path through an environment variable.
export TAOS_SSL_CA=/etc/taos/certs/ca.pem
taosX run -f "taos+wss://..." -t "taos+wss://..."
```

#### 1.3.3 `taosBenchmark` (Benchmark Tool)

```bash
# Method 1: WebSocket + TLS + compression (through DSN)
taosBenchmark -T "taos+wss://tduser:SecurePass123!@host:6041/testdb?compression" \
  -d testdb -t 100 -n 10000

# Method 2: configuration file (set host/port/ssl-related fields, file mode 600)
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

#### 1.3.4 `taosdump` (Backup/Restore)

```bash
# Method 1: WebSocket + TLS (through taosAdapter)
taosdump -R --cloud="taos+wss://tduser:SecurePass123!@host:6041/mydb?compression" \
  -D mydb -o /backup/mydb

# Method 2: native TCP + TLS (controlled by enableTLS=1 + tlsCaPath in /etc/taos/taos.cfg)
taosdump -h host -P 6030 -u tduser -p -D mydb -o /backup/mydb
```

---

## 2. Access-Layer Transport

> **About taosc**: taosc is the TDengine native client library. As a **standalone component**, it provides C APIs and DSN connection interfaces upward, and communicates downward with the taosd cluster over the **private protocol** (TCP :6030). The private protocol carries TLS + SASL/SCRAM and zlib transport compression. In path A (WebSocket/REST), taosAdapter internally calls taosc on the server side to complete the last hop to taosd. In path B, an application/CLI embeds the taosc dynamic library and connects directly to taosd. Both paths eventually enter taosd through taosc, so taosc parameters such as `enableTLS` / `compressMsgSize` take effect on both paths. On the Adapter side, configure the internal taosc used by taosAdapter; for native connectors, configure the taosc embedded in the application process.

The access layer is the first TDengine server-side hop for entry-layer traffic. It has two parallel paths:

- **Path A**: WebSocket / REST to taosAdapter :6041 to internal taosc to taosd (private protocol :6030)
- **Path B**: application/CLI embeds taosc (native connector) to taosd :6030 **private protocol**

TLS / compression for the two paths are configured independently, but the taosc to taosd segment shares the same private protocol stack (TLS + SASL/SCRAM + zlib).

### 2.1 Path A: taosAdapter (HTTPS/WSS + permessage-deflate)

#### 2.1.1 Server-Side HTTPS/WSS {#211-server-side-httpswss}

**taosAdapter server side** (`/etc/taos/taosadapter.toml`). `[ssl]` is an Enterprise Edition capability. The sample configuration comments mark it as `Applicable for the Enterprise Edition`. For parameter descriptions, see [taosAdapter: SSL](../12-operations-and-tooling/03-components/03-taosadapter.md#ssl). You can generate a self-signed certificate with the [appendix](#11-appendix-generate-a-self-signed-certificate).

```toml
[ssl]
# Enable SSL. Applicable for the Enterprise Edition.
enable   = true
# The certificate and private-key paths must match the actual files.
# The appendix copies them to /etc/taos/ by default.
certFile = "/etc/taos/server.crt"
keyFile  = "/etc/taos/server.key"
```

:::tip Paths and Permissions

- If you place the certificate in another directory, such as `/etc/taos/certs/adapter.pem`, update `certFile` / `keyFile` accordingly.
- The recommended permission for the private-key file is `600`, owned by the user that runs taosAdapter, commonly `taos`.
:::
Restart and verify after enabling it:

```bash
sudo systemctl restart taosadapter
sudo systemctl status taosadapter
journalctl -u taosadapter -n 50
# In normal cases, you should see a message similar to: SSL is enabled
```

For client-side `wss` / `useSSL` / TrustStore configuration, see [Client and Connector Security: SSL/TLS Configuration](./05-client-connector-security.md#2-ssltls-configuration).

#### 2.1.2 Transport Compression (permessage-deflate)

By default, the taosAdapter server accepts client permessage-deflate negotiation requests (hard-coded as enabled) and automatically gzip-compresses all HTTP responses. Enable compression in the client DSN as needed. See the connector examples in section 1.1.

> Compression increases CPU load. Enable it only in bandwidth-constrained scenarios, such as cross-WAN or cloud connections.

#### 2.1.3 Nginx Reverse-Proxy TLS Offloading

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

### 2.2 Path B: taosc to taosd Private Protocol (TLS + SASL + zlib)

As a standalone native client library, taosc communicates with taosd over the TDengine **private protocol** (TCP :6030). The private protocol carries TLS transport encryption, SASL/SCRAM-SHA-256 authentication, and zlib message compression. Native connectors (C/Java JNI/Python native/Go cgo/Rust native/CSharp) embed taosc as a dynamic library in the application process, and their transport behavior is exactly the same as taosAdapter's internal taosc calls.

#### 2.2.1 Certificate Requirements

| Certificate type | Minimum key length | Description |
|---------|-------------|------|
| RSA | 2048 bits | 4096 bits recommended |
| ECC | 256 bits | P-256 / P-384 recommended |
| CA | - | Only PEM format is supported |

#### 2.2.2 Mutual TLS (mTLS)

**taosd server side** (`/etc/taos/taos.cfg`):

```ini
enableTLS         1
tlsCaPath         /etc/taos/certs/ca.pem
tlsSvrCertPath    /etc/taos/certs/server.pem
tlsSvrKeyPath     /etc/taos/certs/server-key.pem
# Mutual TLS (mTLS)
# tlsVerifyClient 1
```

**taosc client side** (`/etc/taos/taos.cfg`):

```ini
enableTLS   1
tlsCaPath   /etc/taos/certs/ca.pem
# mTLS
# tlsCliCertPath /etc/taos/certs/client.pem
# tlsCliKeyPath  /etc/taos/certs/client-key.pem
```

#### 2.2.3 SASL/SCRAM-SHA-256 Hardening

Add SASL/SCRAM-SHA-256 on top of the TLS channel:

- Passwords are never transmitted over the network. Only HMAC-derived values are transmitted.
- This helps prevent man-in-the-middle attacks when certificates are stolen.
- The client also verifies server authenticity, enabling mutual authentication.

#### 2.2.4 Certificate Hot Reload

```sql
ALTER DNODES RELOAD TLS;
ALTER DNODE <dnode_id> RELOAD TLS;

```

Deploy the new certificate, run `RELOAD`, and new connections automatically use the new certificate without stopping the service.

#### 2.2.5 TCP Compression (zlib)

```ini
# taos.cfg
# 0 = no compression  1 = fixed compression  2 = adaptive compression
compressMsgSize 0
```

It is disabled by default and is suitable for extremely bandwidth-constrained scenarios. After it is enabled, both writes and queries experience some latency impact.

#### 2.2.6 Native Connector Examples

**C/C++:**

```c
// TLS is automatically enabled after enableTLS=1 is configured in taos.cfg.
TAOS *taos = taos_connect("hostname", "tduser", "SecurePass123!", "db", 6030);
```

**Go native:**

```go
dsn := "tduser:SecurePass123!@tcp(hostname:6030)/db"
db, _ := sql.Open("taosSql", dsn)
```

**ODBC native:**

```text
[TDengine-TLS]
Driver=/usr/lib/libtaos_odbc.so
Server=hostname:6030
UID=tduser
PWD=SecurePass123!
```

---

## 3. Data Ingestion Link Transport

Data ingestion links are background data-flow tasks triggered by SQL in taosd. The link runs from outside to inside: **external source to taosX / Agent to taosAdapter to taosd**.

### 3.1 taosX to taosAdapter (WSS Sink + deflate)

taosX writes to taosAdapter through WebSocket. TLS and compression are specified by the Sink DSN:

```text
# TLS
taos+wss://tduser:SecurePass123!@taosadapter:6041/db1

# TLS + compression
taos+wss://tduser:SecurePass123!@taosadapter:6041/db1?compression
```

When you create a data ingestion task in Explorer, the Sink configuration supports selecting `Enable WebSocket compression`.

### 3.2 taosX-Agent to taosX (gRPC TLS + gzip)

taosX-Agent reports data through Arrow Flight RPC (gRPC / HTTP2).

**taosX server side** (`/etc/taos/taosX.toml`):

```toml
[serve]
listen   = "0.0.0.0:6050"
grpc     = "0.0.0.0:6055"
# REST API TLS
ssl_cert = "/etc/taos/cert.pem"
ssl_key  = "/etc/taos/key.pem"
ssl_ca   = "/etc/taos/ca.pem"
# gRPC TLS (if not configured, the REST certificate is reused)
grpc_ssl_cert = "/etc/taos/grpc-cert.pem"
grpc_ssl_key  = "/etc/taos/grpc-key.pem"
grpc_ssl_ca   = "/etc/taos/grpc-ca.pem"
```

**taosX-Agent** (`/etc/taos/agent.toml`):

```toml
endpoint    = "https://taosX-server:6055"
token       = "<JWT Token generated by Explorer>"
ca          = "/etc/taos/ca.pem"
compression = true
```

`compression = true` enables gRPC gzip compression, which is suitable for cross-WAN / public-network backhaul from Agent to taosX. Only the gzip algorithm is supported because it is native to gRPC.

### 3.3 External Data Source Transport

#### 3.3.1 Source-Side TLS Connection Configuration

**MQTT:**

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

**MySQL / PostgreSQL / MSSQL / Pulsar / OPC-UA:** In Explorer task configuration, specify `tls.enabled = true` + `tls.ca_cert` / `tls.client_cert` / `tls.client_key` uniformly. OPC-UA automatically enables TLS when the `SignAndEncrypt` security policy is used.

#### 3.3.2 MQTT / Kafka Message-Body Decompression

For messages already compressed by the publisher, taosX supports automatic decompression:

| Format | Description |
|------|------|
| `gzip` | Standard gzip |
| `zlib` | zlib/deflate |
| `lz4` | LZ4 block compression |
| `zstd` | Zstandard |
| `snappy` | Google Snappy |

Specify the format in **Advanced Options** for the Explorer task.

---

## 4. Cluster-Internal Transport (DNode to DNode)

Internal communication among MNode, DNode, and VNode shares the same `taos.cfg` certificate configuration as client to taosd connections. `enableTLS 1` takes effect globally, enabling TLS for both external and internal traffic at the same time.

### 4.1 Encryption Key Distribution

The storage encryption key (DB_KEY) is distributed between MNode and DNode through the SASL encrypted channel:

1. MNode encrypts DB_KEY with SVR_KEY.
2. MNode sends the encrypted DB_KEY to the target DNode through the established TLS + SASL channel.
3. DNode decrypts it with the local SVR_KEY and uses DB_KEY only in memory.

### 4.2 Internal Communication Compression

RPC between DNodes reuses the `compressMsgSize` configuration. See section 2.2.5. Once enabled in `taos.cfg`, client to taosd traffic and DNode to DNode traffic are controlled together. Cluster-internal traffic usually runs on a 10 GbE intranet, so keep the default disabled setting to avoid CPU overhead.

---

## 5. Observability Ingress Transport (taosKeeper)

As a passive receiver, taosKeeper aggregates monitoring metrics pushed by taosd / taosAdapter / taosX / taosExplorer, and then writes them back to the TSDB `log` database through WebSocket. Under the default audit path (`auditSaveInSelf = 0`), Enterprise Edition `taosd` also reports audit JSON to Keeper according to `auditInterval`; Keeper writes it into an audit database with `IS_AUDIT`, separate from the `log` database. When `auditSaveInSelf = 1` (`v3.4.1.0+`), audit data does not go through Keeper. For details, see [Audit and Compliance](./07-audit-and-compliance.md).

taosKeeper itself can expose an HTTPS receiving port and write back to taosAdapter through HTTPS. Audit reporting through Keeper is also controlled by `auditHttps` / `auditUseToken` in `taosd`.

**taosKeeper server side** (`/etc/taos/taoskeeper.toml`):

```toml
[ssl]
enable   = true
certFile = "/etc/taos/keeper-cert.pem"
keyFile  = "/etc/taos/keeper-key.pem"
```

**taosKeeper to taosAdapter**:

```toml
[tdengine]
host     = "taosadapter-host"
port     = 6041
username = "tduser"
password = "SecurePass123!"
usessl   = true
```

> Explorer's own HTTPS configuration is included in section 1.2 because Explorer is a Web UI **entry point**, not an inward-facing operations ingress.

---

## 6. Storage Layer: On-Disk Compression

> After the link transport in sections 1 through 5, data is finally written into TSDB storage, where another round of on-disk compression significantly reduces space usage. Storage compression is independent of transport links and is transparent to clients. For the complete SQL syntax, see [Compression Configuration](../05-tdengine-sql/03-data-write/03-compress.md).

### 6.1 Three-Level Compression Architecture

| Level | Name | Target | Configuration granularity |
|------|------|---------|---------|
| Level 1 | Column encoding | Single-column data, reducing data entropy | Column level |
| Level 2 | Block compression | Data blocks, general-purpose compression algorithms | Column level |
| Level 3 | Multi-block compression | Combined compression of multiple data blocks | Specified during COMPACT |

### 6.2 Column Encoding Algorithms (Level 1)

| Algorithm | Applicable data types | Principle |
|------|-------------|------|
| `delta-i` | TIMESTAMP, BIGINT | Delta between adjacent values |
| `delta-d` | FLOAT, DOUBLE | Floating-point delta |
| `simple8b` | TINYINT/SMALLINT/INT/BIGINT | Compact integer packing |
| `bit-packing` | BOOL | Packs 8 Boolean values into 1 byte |
| `disabled` | VARCHAR/NCHAR/BINARY | No encoding; goes directly to level 2 |

### 6.3 Block Compression Algorithms (Level 2)

| Algorithm | Characteristics | Scenario |
|------|------|---------|
| `lz4` | Fastest | **Default**, high-frequency writes |
| `zstd` | High compression ratio | Higher compression ratio required |
| `zlib` | General purpose | CPU-insensitive workloads |
| `xz` | Highest compression ratio | Cold-data archiving |
| `tsz` | Lossy time-series compression | Floating-point sensors where precision loss is acceptable |
| `disabled` | No compression | Extremely low-latency scenarios |

### 6.4 SQL Configuration

```sql
CREATE STABLE meters (
    ts        TIMESTAMP ENCODE 'delta-i'  COMPRESS 'lz4'  LEVEL 'medium',
    current   FLOAT     ENCODE 'delta-d'  COMPRESS 'zstd' LEVEL 'high',
    voltage   INT       ENCODE 'simple8b' COMPRESS 'lz4'  LEVEL 'low',
    note      VARCHAR(64)                 COMPRESS 'lz4'
) TAGS (location VARCHAR(64), groupId INT);

ALTER TABLE meters MODIFY COLUMN current COMPRESS 'zstd' LEVEL 'high';

CREATE DATABASE sensor_db COMP 2;     -- 0 = off  1 = level 1  2 = two levels (default)
COMPACT DATABASE sensor_db;            -- Triggers level-3 compression.
```

### 6.5 Choosing By Data Characteristics

| Data characteristics | Recommended configuration | Compression ratio reference |
|---------|---------|-----------|
| Integer sensors | `delta-i` + `lz4` + `medium` | ~20% (80% saved) |
| Floating-point sensors | `delta-d` + `lz4` + `medium` | ~30% (70% saved) |
| Lossy floating-point data | `delta-d` + `tsz` + `medium` | ~10% (90% saved) |
| String tags | `disabled` + `zstd` + `medium` | ~50% |
| Cold archive | `delta-i` + `xz` + `high` | ~15% |

### 6.6 Limitations

- Compression configuration is valid only for ordinary columns. TAG columns are not supported.
- Subtables inherit the compression configuration of their supertable.
- Existing data must be compacted with COMPACT before the new algorithm is used.
- After the compression algorithm is modified, rollback to an earlier version is not supported.

---

## 7. Session Security Controls

| Parameter | Default | Description |
|------|--------|------|
| `sessionPerUser` | 32 | Maximum concurrent sessions per user |
| `sessionConnTime` | 480 min | Maximum session duration |
| `sessionConnIdleTime` | 30 min | Idle timeout |
| `sessionMaxConcurrency` | 10 | Maximum concurrent requests per user |
| `sessionMaxCallVnodeNum` | 10 | Maximum number of VNodes involved in a single request |

```sql
ALTER USER alice SESSIONS 5 CONNECTIONS 20 QUERIES 5;
```

---

## 8. Configuration Recommendations By Network Scenario

| Scenario | TLS | App to Adapter compression | taosc to taosd compression | taosX to Adapter compression | taosX-Agent to taosX compression |
|------|-----|------------------|------------------|---------------------|--------------------|
| Local / LAN | Optional | Disabled | Disabled | Disabled | Disabled |
| Cross-data-center WAN | **Enabled** | Depends on bandwidth | Depends on bandwidth | Recommended | Recommended |
| Cloud remote | **Enabled** | Recommended | - | Recommended | Recommended |

> Compression consumes CPU. Benchmark: a 100-million-row query takes about 12 seconds without compression and about 38 seconds with `compression` enabled. Transport compression is not recommended on a LAN.

---

## 9. Monitoring and Troubleshooting

### 9.1 Storage Compression Effect

```sql
SHOW TABLE DISTRIBUTED <db_name>.<table_name>;
SHOW DATABASES;
```

### 9.2 Transport Compression Verification

```bash
curl -s -D - -H "Accept-Encoding: gzip" \
  "http://localhost:6041/rest/sql" \
  -u tduser:SecurePass123! -d "SELECT server_status()" \
  | grep -i "content-encoding"
# Expected output: Content-Encoding: gzip
```

### 9.3 Error Codes

| Error code | Meaning | Troubleshooting recommendation |
|--------|------|---------|
| `0x0375` | TLS handshake failed | Check certificate validity period and format |
| `0x0376` | Certificate verification failed | Check CA certificate configuration |
| `0x0377` | SASL authentication failed | Check the password and confirm SCRAM-SHA-256 |
| `0x0378` | Session count exceeded | Adjust `sessionPerUser` |
| `0x0380` | Certificate expired | Run `ALTER DNODES RELOAD TLS` |
| `0x0381` | TLS version incompatible | Confirm TLSv1.2+ on both ends |

### 9.4 Frequently Asked Questions

| Symptom | Cause | Solution |
|------|------|------|
| Storage space did not decrease | Compression parameters were not specified | `ALTER TABLE ... COMPRESS 'lz4'` |
| WebSocket is not compressed | The client did not enable it | Add `compression` to the DSN |
| Writes become slower after compression | Compression level is too high | Lower `LEVEL` or switch to lz4 |
| Agent gRPC is not compressed | The configuration is not enabled | Set `compression = true` in `agent.toml` |

---

## 10. Deployment Checklist (By Layer)

### 10.1 Entry Layer

- [ ] Language connectors enable `wss://` + `compression` as needed, especially in cross-WAN scenarios.
- [ ] JDBC is configured with `useSSL=true`, and the self-signed certificate has been added to the truststore.
- [ ] taosExplorer has HTTPS enabled (`[ssl]` in `explorer.toml`).
- [ ] CLI (`taos` / `taosX` / `taosBenchmark` / `taosdump`) DSNs use `taos+wss://...?compression`.
- [ ] Certificate and configuration files used by CLI tools have mode 600.

### 10.2 Access Layer

- [ ] taosAdapter has HTTPS enabled (`[ssl] enable = true`).
- [ ] taosAdapter certificate files have mode 600.
- [ ] If using a reverse proxy, Nginx TLS offloading + `proxy_pass` to 6041 has been verified.
- [ ] `enableTLS 1` for taosd is configured on both server and client sides.
- [ ] Certificates are RSA &gt;= 2048 or ECC &gt;= 256 and in PEM format.
- [ ] The CA certificate has been distributed to all taosc clients.
- [ ] The `ALTER DNODES RELOAD TLS` hot reload operation is understood.
- [ ] `compressMsgSize` has been evaluated for cross-WAN scenarios.

### 10.3 Data Ingestion Links

- [ ] taosX Sink DSN uses `taos+wss://...?compression`.
- [ ] taosX gRPC TLS certificates (`grpc_ssl_*`) have been configured.
- [ ] The taosX-Agent `ca` parameter points to the correct CA.
- [ ] taosX-Agent has `compression = true` in cross-WAN scenarios.
- [ ] TLS is enabled for external data sources (MQTT/Kafka/MySQL/PG/MSSQL/Pulsar/OPC-UA).
- [ ] MQTT/Kafka message-body compression formats (gzip/snappy/lz4/zstd) have been declared in task advanced options.

### 10.4 Cluster Internal

- [ ] Cluster-internal TLS takes effect globally with `enableTLS`.
- [ ] The firewall restricts the source of traffic to port 6030.
- [ ] Certificate expiration alerts have been configured for 30 days before expiration.

### 10.5 Observability Ingress

- [ ] taosKeeper has HTTPS enabled.
- [ ] taosKeeper to taosAdapter has `usessl = true`.
- [ ] Sensitive configuration files have mode 600.

### 10.6 Storage Layer

- [ ] The database specifies compression (`COMP 2`, lz4 or zstd).
- [ ] `ENCODE` has been selected by data type.
- [ ] `COMPRESS` / `LEVEL` has been tuned by CPU and space requirements.
- [ ] The compression ratio has been verified with `SHOW TABLE DISTRIBUTED`.

## 11. Appendix: Generate a Self-Signed Certificate {#11-appendix-generate-a-self-signed-certificate}

The following steps generate a self-signed certificate for testing, which can be referenced by taosAdapter `[ssl]` and similar HTTPS entry points. In production environments, use a certificate issued by a trusted CA.

### 11.1 Generate a Private Key

```bash
# Generate an RSA 2048-bit private key.
openssl genrsa -out server.key 2048
```

### 11.2 Generate a Certificate Signing Request (CSR)

```bash
# Generate a CSR interactively.
openssl req -new -key server.key -out server.csr

# Fill in the prompted information.
# Important: Common Name must be your server IP address or domain name.
# The following values are examples. Modify them for your environment:
#
# Country Name (2 letter code) [AU]: <YOUR_COUNTRY_CODE>           # Example: CN
# State or Province Name (full name) [Some-State]: <YOUR_STATE>   # Example: Beijing
# Locality Name (eg, city) []: <YOUR_CITY>                        # Example: Beijing
# Organization Name (eg, company) [Internet Widgets Pty Ltd]: <YOUR_ORG>  # Example: YourCompany
# Organizational Unit Name (eg, section) []: <YOUR_UNIT>           # Example: IT Department
# Common Name (e.g. server FQDN or YOUR name) []: <YOUR_SERVER_IP_OR_DOMAIN>  # Important. Example: 192.168.1.100 or tdserver.example.com
# Email Address []: <YOUR_EMAIL>                                   # Example: admin@example.com
```

:::tip Key Configuration Items

- **Common Name (CN)**: Must contain the server IP address or domain name used by clients to connect.
- **Subject Alternative Name (SAN)**: Must contain the domain name / IP address actually used by clients to connect. Modern TLS clients usually verify SAN first.
- If the client connects with `192.168.1.100`, CN / SAN should contain `192.168.1.100`.
- If the client connects with `tdserver.example.com`, CN / SAN should contain `tdserver.example.com`.
:::

### 11.3 Generate a Self-Signed Certificate (Valid for 365 Days)

```bash
# Recommended: explicitly add SAN.
# Replace the example domain/IP with your actual connection address.
cat > san.ext <<'EOF'
subjectAltName=DNS:tdserver.example.com,IP:192.168.1.100
EOF

openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt -extfile san.ext
```

### 11.4 Copy the Certificate and Key to the TDengine Configuration Directory

```bash
# Assume the TDengine configuration directory is /etc/taos.
sudo cp server.crt /etc/taos/
sudo cp server.key /etc/taos/
sudo chown taos:taos /etc/taos/server.crt /etc/taos/server.key
sudo chmod 600 /etc/taos/server.key
```

### 11.5 Enable SSL in taosAdapter

Edit `/etc/taos/taosadapter.toml`:

```toml
[ssl]
enable   = true
certFile = "/etc/taos/server.crt"
keyFile  = "/etc/taos/server.key"
```

```bash
sudo systemctl restart taosadapter
sudo systemctl status taosadapter
journalctl -u taosadapter -n 50   # Confirm that SSL is enabled appears.
```

For the complete access-layer description, see [2.1.1 Server-Side HTTPS/WSS](#211-server-side-httpswss). For client configuration, see [Client and Connector Security](./05-client-connector-security.md).
