---
sidebar_label: Full-Trace Authentication
title: Full-Trace Authentication
description: TDengine authentication and access control from entry, access, ingestion, cluster, to storage
toc_max_heading_level: 3
---

## Overview

TDengine TSDB uses a six-layer authentication architecture that unfolds layer by layer, starting from user-side entry points (applications, Web UI, and CLI), passing through two access-layer paths (WebSocket/REST and the taosc private protocol), extending to data ingestion pipelines and internal cluster communication, then to observability access on the operations side, and finally to storage and audit access control. This document describes the authentication mechanisms by layer:

```mermaid
graph LR
    classDef app   fill:#f39c12,color:#000,stroke:#d68910,font-weight:bold
    classDef svc   fill:#1a6ea8,color:#fff,stroke:#0d4f7d
    classDef core  fill:#2c3e50,color:#fff,stroke:#1a252f
    classDef agent fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef src   fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ops   fill:#16a085,color:#fff,stroke:#0e6655
    classDef idp   fill:#f5b7b1,color:#000,stroke:#e74c3c

    APP(["Applications / language connectors"])
    CLI(["CLI tools<br/>taos / taosX / taosBenchmark / taosdump"])

    subgraph TSDB["TSDB Cluster"]
        TA(["taosAdapter<br/>:6041"])
        TAOSC(["taosc<br/>(native client library)"])
        TD[("taosd<br/>:6030")]
        TX(["taosX<br/>:6050 / :6055"])
        TD2[("taosd :6030<br/>(other DNodes)")]
    end

    subgraph OPS["Operations Observability"]
        TE(["taosExplorer<br/>:6060"])
        TK(["taosKeeper<br/>:6043"])
    end

    AGT(["taosX-Agent"])
    subgraph DS["External Data Sources"]
        S1["MQTT · Kafka · MySQL · PG<br/>MSSQL · Pulsar · OPC-UA"]
        S2["OPC-DA · KingHistorian<br/>pSpace · PI"]
    end
    IDP(["OAuth/OIDC IdP"])

    APP -->|"1 Path A: HTTP/WS"| TA
    APP -.->|"1 Path B: embedded taosc"| TAOSC
    CLI -->|"1 CLI entry"| TA
    CLI -.->|"1 CLI entry (embedded taosc)"| TAOSC
    TA  -->|"2 Internal taosc call"| TAOSC
    TAOSC -->|"Private protocol :6030 + SASL/SCRAM"| TD
    TX  -->|"3 WebSocket"| TA
    AGT -->|"3 gRPC"| TX
    S2  -.->|"taosX-Agent active pull"| AGT
    S1  -->|"Built-in connectors"| TX
    TD  <-->|"4 Cluster internal"| TD2
    TE  -->|"1 Web UI"| TA
    TE  -->|"1 Web UI"| TX
    TK  -->|"5 WebSocket"| TA
    IDP -->|"1 OIDC"| TE

    class APP,CLI app
    class TA,TX,TAOSC svc
    class TD,TD2 core
    class AGT agent
    class S1,S2 src
    class TK,TE ops
    class IDP idp
```

### Token Type Reference

The TDengine ecosystem uses several types of tokens that are easy to confuse. The following table clarifies each token's issuer, applicable path, and usage:

| Token type | Issuer | Applicable path | HTTP Header | Enabled by default | Description |
|------------|--------|-----------------|-------------|--------------------|-------------|
| Bearer Token | taosd `CREATE TOKEN` | App -> taosAdapter | `Authorization: Bearer <token>` | No (must be created manually) | Enterprise Edition long-lived token that avoids plaintext passwords |
| Agent Token | taosd MNode (SQL `CREATE XNODE AGENT`) | taosX-Agent -> taosX (gRPC Arrow Flight) | gRPC metadata `x-token` + Handshake Payload | No (must be created manually) | JWT (HS256), identity credential for Agent registration and heartbeat |
| Cloud Token | TDengine Cloud platform | App -> Cloud | DSN `?token=<token>` | - | Cloud service only |
| OAuth Token | External IdP | User -> taosExplorer | Cookie (httpOnly) | No (OAuth must be configured) | SSO single sign-on |

### Authentication Overview by Layer

| Layer | Scope | Authentication mechanism | Encryption supported | Default configuration |
|-------|-------|--------------------------|----------------------|-----------------------|
| 1 Entry layer · programmatic | App / language connectors -> taosAdapter or taosd | Basic / Bearer Token / password / TOTP | Yes (TLS / WSS) | Unencrypted by default |
| 1 Entry layer · Web UI | Browser -> taosExplorer | Username/password; OAuth 2.0/OIDC SSO | Yes (HTTPS) | Unencrypted by default |
| 1 Entry layer · CLI | taos / taosX / taosBenchmark / taosdump | Username/password, DSN, Bearer Token, environment variables | Yes (TLS / WSS) | Unencrypted by default |
| 2 Access layer · Path A | App -> taosAdapter :6041 -> taosc -> taosd | Basic / Bearer Token | Yes (TLS) | Unencrypted by default |
| 2 Access layer · Path B | Application embedding taosc -> taosd :6030 (private protocol) | SCRAM-SHA-256 / MD5 / TOTP / TOKEN | Yes (TLS) | Unencrypted by default |
| 3 Data ingestion | taosX / taosX-Agent / external sources | DSN, Bearer Token, Agent registration Token, source-side credentials | Yes (HTTPS / gRPC TLS / source-side TLS) | Unencrypted by default |
| 4 Cluster internal | DNode ↔ DNode / MNode | SCRAM-SHA-256 internal key, dynamic TLS loading | Yes (TLS) | Unencrypted by default |
| 5 Observability access | taosKeeper | Configuration file password, network isolation, write-back path authentication | Yes (HTTPS) | Unencrypted by default |
| 6 Storage and audit | TDE keys, on-disk data access, audit logs | Query authentication passthrough, TDE key operator privileges, `SYSAUDIT` audit database read permission | Yes (TDE) | Disabled by default |

---

## 1. Entry Layer Authentication

The entry layer consists of three user-side access points to TDengine: programmatic connections (applications/connectors), Web UI (taosExplorer), and command-line tools (operations CLI). This layer focuses on how credentials are presented and transmitted on the user side. **Final authentication verification happens in the 2 access layer** (through taosAdapter passthrough or direct taosc connection).

### 1.1 Programmatic Entry (Applications / Language Connectors)

Applications can access TDengine through two paths: WebSocket/REST via taosAdapter (recommended), or the **private protocol** (TCP :6030) by embedding taosc to connect directly to taosd. Supported authentication methods:

| Method | HTTP Header / usage | Description |
|--------|---------------------|-------------|
| Basic Auth | `Authorization: Basic base64(user:pass)` | Standard HTTP Basic authentication |
| Bearer Token | `Authorization: Bearer <token>` | Long-lived token generated by Enterprise Edition `CREATE TOKEN` |

> **Note**: The REST API (`/rest/sql`) **does not support** authentication through `?user=&password=` query parameters. The InfluxDB write API (`/influxdb/v1/write`) supports `?u=&p=` parameters.

**Bearer Tokens are issued by taosd** (Enterprise Edition), and taosAdapter passes them through to taosd for verification:

```sql
-- Create a Bearer Token in taosd
CREATE TOKEN
    auth_code 'my_auth_code'
    client_id  'app_server_01'
    expire_time '2026-12-31T23:59:59';

-- Query the token value
SELECT token FROM information_schema.ins_tokens WHERE auth_code = 'my_auth_code';
```

**HTTP examples:**

```bash
# Basic Auth
curl -u tduser:SecurePass123! http://localhost:6041/rest/sql -d "SELECT SERVER_VERSION()"

# Bearer Token (Enterprise Edition, generated by CREATE TOKEN)
curl -H "Authorization: Bearer eyJ..." http://localhost:6041/rest/sql -d "SHOW DATABASES"
```

For longer examples of tokens, client TLS, and dynamic rotation across language connectors, see [Client and Connector Security](./05-client-connector-security.md).

#### 1.1.1 Authentication Configuration for Language Connectors

All connectors support username/password authentication. Higher-level connectors (WebSocket mode) go through the taosAdapter gateway.

##### 1.1.1.1 C/C++ Connector

```c
// Password authentication (embedded taosc, direct private-protocol connection to taosd; see section 2.2)
TAOS *taos_connect("localhost", "tduser", "SecurePass123!",
                   "mydb", 6030);

// TOTP authentication (Enterprise Edition, direct connection to taosd)
TAOS *taos_connect_totp("localhost", "tduser", "SecurePass123!",
                        "123456", "mydb", 6030);
```

##### 1.1.1.2 Python Connector

```python
import taosws  # WebSocket connection (via taosAdapter)

# Username/password authentication
conn = taosws.connect("taos+ws://tduser:SecurePass123!@localhost:6041/mydb")

# Bearer Token authentication (Enterprise Edition, token generated by CREATE TOKEN)
conn = taosws.connect("taos+ws://localhost:6041/mydb?bearer_token=<bearer_token>")

# Token authentication (TDengine Cloud)
conn = taosws.connect("taos+wss://cloud.tdengine.com/mydb?token=<cloud_token>")
```

##### 1.1.1.3 Go Connector

```go
import "github.com/taosdata/driver-go/v3/taosWS"

// WebSocket + password (via taosAdapter)
dsn := "tduser:SecurePass123!@ws(localhost:6041)/mydb"
db, err := sql.Open("taosWS", dsn)

// Bearer Token authentication (Enterprise Edition, camelCase bearerToken)
dsn = "ws(localhost:6041)/mydb?bearerToken=<bearer_token>"
db, err = sql.Open("taosWS", dsn)

// Token authentication (TDengine Cloud, wss protocol)
dsn = "wss(gw.cloud.taosdata.com:443)/mydb?token=<cloud_token>"
db, err = sql.Open("taosWS", dsn)
```

##### 1.1.1.4 JDBC Connector

```java
// WebSocket authentication (via taosAdapter)
String wsUrl = "jdbc:TAOS-WS://localhost:6041/mydb?user=tduser&password=SecurePass123!";
Connection conn = DriverManager.getConnection(wsUrl);

// Bearer Token authentication (Enterprise Edition)
String tokenUrl = "jdbc:TAOS-WS://localhost:6041/mydb?bearerToken=<bearer_token>";

// Token authentication (TDengine Cloud)
String cloudUrl = "jdbc:TAOS-WS://cloud.tdengine.com/mydb?useSSL=true&token=<cloud_token>";
```

##### 1.1.1.5 Rust Connector

```rust
use taos::*;

// WebSocket authentication (via taosAdapter)
let taos = TaosBuilder::from_dsn("taos+ws://tduser:SecurePass123!@localhost:6041/mydb")?.build()?;

// Bearer Token authentication (Enterprise Edition)
let taos = TaosBuilder::from_dsn("ws://localhost:6041/mydb?bearer_token=<bearer_token>")?.build()?;

// Token authentication (TDengine Cloud)
let taos = TaosBuilder::from_dsn("wss://gw.cloud.taosdata.com/mydb?token=<cloud_token>")?.build()?;
```

##### 1.1.1.6 CSharp (.NET) Connector

```csharp
// WebSocket password authentication (via taosAdapter)
string connStr = "protocol=WebSocket;host=localhost;port=6041;username=tduser;password=SecurePass123!";
using var conn = new TDengineDriver.TDengineConnection(connStr);

// Bearer Token authentication (Enterprise Edition, TDengine.Connector >= 3.1.10)
string tokenStr = "protocol=WebSocket;host=localhost;port=6041;bearerToken=<bearer_token>";

// Token authentication (TDengine Cloud)
string cloudStr = "protocol=WebSocket;host=gw.cloud.taosdata.com;useSSL=true;token=<cloud_token>";
```

##### 1.1.1.7 Node.js Connector

```javascript
const taos = require('@tdengine/websocket');

// WebSocket password authentication (through WSConfig)
const conf = new taos.WSConfig('ws://localhost:6041');
conf.setUser('tduser');
conf.setPwd('SecurePass123!');
conf.setDb('mydb');
const wsSql = await taos.sqlConnect(conf);

// Bearer Token authentication (Enterprise Edition)
const tokenConf = new taos.WSConfig('ws://localhost:6041');
tokenConf.setDb('mydb');
tokenConf.setBearerToken('<bearer_token>');
const tokenSql = await taos.sqlConnect(tokenConf);

// Token authentication (TDengine Cloud)
const cloudConf = new taos.WSConfig('wss://gw.cloud.taosdata.com');
cloudConf.setDb('mydb');
cloudConf.setToken('<cloud_token>');
const cloudSql = await taos.sqlConnect(cloudConf);
```

> **Note**: The Node.js connector does not support a direct `taos.connect(url)` API. Use `WSConfig` + `sqlConnect()` instead.

##### 1.1.1.8 ODBC Connector

The ODBC connector (Windows) is configured through the ODBC Data Source Administrator:

**WebSocket connection (self-managed deployment, username/password):**

- Connection type: WebSocket
- URL: `http://localhost:6041`
- Username / password: enter the corresponding credentials

**WebSocket connection (TDengine Cloud, Token authentication):**

- Connection type: WebSocket
- URL: `https://gw.cloud.taosdata.com?token=<cloud_token>`
- Username / password: leave empty

> **Note**: The ODBC connector does not support `bearerToken` (self-managed Token authentication). Use username/password instead.

#### 1.1.2 taosAdapter Plugin Service Accounts (External System Writes)

When external systems (collectd, StatsD, OpenTSDB) write through taosAdapter protocol-compatible endpoints, taosAdapter **does not pass through source credentials**. Instead, it writes on behalf of independent accounts declared in its configuration file. These accounts are part of the application entry layer. We recommend creating a dedicated write-only account for each plugin:

```toml
# /etc/taos/taosadapter.toml
# Security recommendation: create a dedicated write-only account for each plugin and avoid using the default account

[collectd]
user = "collectd_writer"
password = "CollectdPass123!"
# token = ""               # Enterprise Edition v3.4.0.0+ supports token authentication

[statsd]
user = "statsd_writer"
password = "StatsdPass123!"

[opentsdb_telnet]
user = "opentsdb_writer"
password = "OpentsdbPass123!"
```

### 1.2 Web UI Entry (taosExplorer Authentication)

taosExplorer is the Web management interface (default :6060) and supports two login methods.

**Method 1: TDengine username/password login**

Log in to the Web interface directly with user credentials created in taosd.

**Method 2: OAuth 2.0 / OIDC single sign-on (SSO)**

taosExplorer supports integration with enterprise identity providers (Keycloak, Azure AD, Okta, and others). **Note: taosd itself does not support OAuth. Explorer acts as an OIDC Relying Party. After a user authenticates through the IdP, Explorer bridges the session to taosd username/password authentication.**

#### 1.2.1 Supported Provider Types

| Type | Applicable scenario | Authentication flow |
|------|---------------------|---------------------|
| `oidc` | Standard OIDC providers (Keycloak, Azure AD, Okta) | Authorization Code + **PKCE** + Nonce |
| `plain` | Standard OAuth 2.0 providers (GitHub, etc.) | Authorization Code + State |
| `custom` | Non-standard/legacy OAuth services | Authorization Code + State |

> **Selection recommendation:** Prefer `oidc` (automatic endpoint discovery and PKCE security enhancement).

#### 1.2.2 OIDC Configuration Example

```toml
# /etc/taos/explorer.toml
[oauth]
enabled = true
provider = "oidc"

[oauth.provider_display_name]
en = "Enterprise SSO"
zh = "Enterprise SSO"

[oauth.oidc]
client_id     = "your-client-id"
client_secret = "your-client-secret"
issuer_url    = "https://idp.example.com/realms/taosdata"
redirect_uri  = "https://explorer.example.com:6060/api/-/oauth/callback"
scopes        = ["openid", "profile", "email"]
```

OIDC configuration supports environment-variable overrides (**OIDC mode only**; plain/custom must use TOML):

| Environment variable | Description |
|----------------------|-------------|
| `EXPLORER_OAUTH_ENABLED` | Enable/disable OAuth |
| `EXPLORER_OAUTH_CLIENT_ID` | OAuth Client ID |
| `EXPLORER_OAUTH_CLIENT_SECRET` | OAuth Client Secret |
| `EXPLORER_OAUTH_ISSUER_URL` | OIDC Issuer URL |
| `EXPLORER_OAUTH_REDIRECT_URI` | Callback URL |
| `EXPLORER_OAUTH_SCOPES` | Comma-separated Scope list |

#### 1.2.3 Custom OAuth Configuration Example

```toml
# /etc/taos/explorer.toml
[oauth]
enabled = true
provider = "custom"

[oauth.provider_display_name]
en = "Company SSO"
zh = "Company SSO"

[oauth.custom]
client_id     = "your_client_id"
client_secret = "your_client_secret"
authorize_url = "https://sso.company.com/oauth2/authorize"
token_url     = "https://sso.company.com/oauth2/token"
profile_url   = "https://sso.company.com/oauth2/userinfo"
redirect_uri  = "https://explorer.example.com:6060/api/-/oauth/callback"
```

#### 1.2.4 Encryption Key (Required in Production)

OAuth Tokens are stored in the database encrypted with AES-256-GCM. **An encryption key must be configured in production**:

```bash
export EXPLORER_SECURITY_ENCRYPTION_KEY=$(openssl rand -base64 32)
```

#### 1.2.5 OAuth Authentication Flow

```text
User -> Explorer UI "SSO Login"
  -> Explorer generates PKCE challenge + state + nonce (OIDC mode)
  -> 302 redirect to the IdP authorization page
  -> User completes authentication at the IdP
  -> IdP callback /api/-/oauth/callback?code=...&state=...
  -> Explorer verifies state and exchanges code for token
  -> Verify id_token signature (JWKS) and claims
  -> Create Explorer session (httpOnly cookie, default 8 hours)
  -> First login requires binding taosd username/password (POST /api/-/oauth/bind)
```

#### 1.2.6 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/-/oauth/status` | GET | Query whether OAuth is enabled |
| `/api/-/oauth/authorize` | GET | Start the OAuth authorization flow |
| `/api/-/oauth/callback` | GET | Handle the IdP callback |
| `/api/-/oauth/bind` | POST | Bind taosd user credentials |
| `/api/-/oauth/logout` | POST | Log out of the OAuth session |
| `/api/-/oauth/me` | GET | Get current session information |

#### 1.2.7 Enable HTTPS (taosExplorer)

```toml
# /etc/taos/explorer.toml
[ssl]
certificate     = "/path/to/certificate.crt"
certificate_key = "/path/to/private.key"
```

### 1.3 Command-Line Tool Entry (Operations CLI Authentication)

Operations scenarios commonly use four CLI tools: `taos` (interactive shell), `taosX` (data pipeline CLI), `taosBenchmark` (benchmarking), and `taosdump` (backup/restore). All CLIs carry credentials through command-line arguments or environment variables, and final verification happens in the 2 access layer (taosAdapter or taosd).

> **Credential exposure risk**: Command-line arguments such as `-p'<password>'` are written to shell history, process lists (`ps`), `/proc/<pid>/cmdline`, and similar locations. For production operations, we recommend:
>
> 1. Use the environment variables `TDENGINE_USER` / `TDENGINE_PASSWORD`, or pass `-p` without a value so the CLI reads the password interactively;
> 2. Prefer Bearer Tokens in Enterprise Edition (revocable, auditable, and no plaintext password);
> 3. Set configuration file permissions to `chmod 600` to prevent other users from reading them;
> 4. Prefix a shell command with a space to avoid writing it to history on most distributions with `HISTCONTROL=ignorespace`.

#### 1.3.1 `taos` (Interactive SQL Shell)

`taos` supports two connection methods, switched by `-Z/--driver`: **native direct connection** (`-Z 0`, default, taosc uses the private protocol to taosd :6030, access-layer Path B) and **WebSocket connection** (`-Z 1`, via taosAdapter :6041, Path A). To connect to TDengine Cloud, use `-E/--dsn` in the form `https://gw.cloud.taosdata.com?token=<cloud_token>`.

```bash
# Method 1: native direct connection to taosd :6030 (-Z 0 is the default and can be omitted)
taos -h host -P 6030 -u tduser -p'SecurePass123!' -d mydb

# Method 1: enter password interactively (recommended; not exposed on the command line)
taos -h host -P 6030 -u tduser -p -d mydb
# Press Enter, then the prompt displays "Enter password:"

# Method 2: WebSocket via taosAdapter :6041
taos -Z 1 -h host -P 6041 -u tduser -p -d mydb

# Method 3: connect to TDengine Cloud (-E/--dsn, cloud service only)
taos --dsn "https://gw.cloud.taosdata.com?token=<cloud_token>"

# Method 4: environment variables (avoid command-line exposure)
export TDENGINE_USER=tduser
export TDENGINE_PASSWORD='SecurePass123!'
taos -h host -P 6030 -d mydb
```

#### 1.3.2 `taosX` (Data Pipeline CLI)

`taosX` works through DSN connection strings. The source side (`--from` / `-f`) and destination side (`--to` / `-t`) each require a DSN, and can use WebSocket/WSS (taosAdapter) or native TCP.

```bash
# Method 1: username/password (WebSocket via taosAdapter)
taosX run \
  -f "taos+ws://tduser:SecurePass123!@src-host:6041/srcdb" \
  -t "taos+ws://tduser:SecurePass123!@dst-host:6041/dstdb"

# Method 1: username/password + WSS (encrypted transport)
taosX run \
  -f "taos+wss://tduser:SecurePass123!@src-host:6041/srcdb" \
  -t "taos+wss://tduser:SecurePass123!@dst-host:6041/dstdb"

# Method 2: Bearer Token (Enterprise Edition)
taosX run \
  -f "taos+ws://src-host:6041/srcdb?bearer_token=<bearer_token>" \
  -t "taos+wss://dst-host:6041/dstdb?bearer_token=<bearer_token>"

# Method 2: TDengine Cloud Token
taosX run \
  -f "taos+wss://cloud.tdengine.com/srcdb?token=<cloud_token>" \
  -t "taos+ws://tduser:SecurePass123!@on-prem:6041/dstdb"

# Method 3: environment variables + simplified DSN
export TDENGINE_USER=tduser
export TDENGINE_PASSWORD='SecurePass123!'
taosX run -f "taos+ws://src-host:6041/srcdb" -t "taos+ws://dst-host:6041/dstdb"
```

> Common subcommands: `taosX run` (migration/subscription/export), `taosX check`, and `taosX plugin`. All subcommands accept the DSN parameters above.

#### 1.3.3 `taosBenchmark` (Benchmarking Tool)

```bash
# Method 1: username/password (command-line arguments)
taosBenchmark -h host -P 6030 -u tduser -p'SecurePass123!' -d testdb -t 100 -n 10000

# Method 1: configuration file (user/password written to JSON; file permissions 600)
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

# Method 2: Bearer Token (WebSocket mode, Enterprise Edition; specified through -T/--taos-dsn or configuration)
taosBenchmark -h host -P 6041 -T "taos+ws://host:6041?bearer_token=<bearer_token>" -d testdb

# Method 3: environment variables
export TDENGINE_USER=tduser
export TDENGINE_PASSWORD='SecurePass123!'
taosBenchmark -h host -P 6030 -d testdb
```

#### 1.3.4 `taosdump` (Backup/Restore)

```bash
# Method 1: export with username/password (default taosc private protocol)
taosdump -h host -P 6030 -u tduser -p'SecurePass123!' -D mydb -o /backup/mydb

# Method 1: import
taosdump -h host -P 6030 -u tduser -p'SecurePass123!' -i /backup/mydb

# Method 1: do not expose password on the command line (interactive input)
taosdump -h host -P 6030 -u tduser -p -D mydb -o /backup/mydb

# Method 2: Bearer Token (WebSocket mode, Enterprise Edition)
taosdump -R -h host -P 6041 \
  --cloud="taos+ws://host:6041/mydb?bearer_token=<bearer_token>" \
  -D mydb -o /backup/mydb

# Method 2: TDengine Cloud Token
taosdump -R --cloud="taos+wss://cloud.tdengine.com/mydb?token=<cloud_token>" \
  -D mydb -o /backup/mydb

# Method 3: environment variables
export TDENGINE_USER=tduser
export TDENGINE_PASSWORD='SecurePass123!'
taosdump -h host -P 6030 -D mydb -o /backup/mydb
```

---

## 2. Access Layer Authentication

> **About taosc**: taosc is the native TDengine client library. As an independent component, it provides the C API and DSN connection interface upward, and communicates with the taosd cluster downward through the **private protocol** (TCP :6030) to complete authentication (SASL/SCRAM, TOTP, TOKEN, and other protocol-layer handshakes). Path A (WebSocket/REST) is implemented by taosAdapter internally calling taosc on the server side to complete the final hop to taosd. Path B (native connection) is implemented by the application/CLI embedding the taosc dynamic library and connecting directly to taosd. Both paths ultimately enter taosd through taosc and share the same user account and authentication mechanisms.

The access layer is the first server-side hop for entry-layer traffic in TDengine. It has **two parallel paths**, selected by the client, but both ultimately authenticate and authorize through **the same taosd user account system**:

- **Path A: WebSocket / REST** - App/CLI -> **taosAdapter :6041** -> internal taosc -> taosd (private protocol :6030)
- **Path B: taosc -> taosd private protocol** - App/CLI embeds taosc as a dynamic library -> **taosd :6030**

> **Shared accounts**: Whether traffic enters through Path A or Path B, user accounts, passwords, IP whitelists, lockout policies, TOKENs, and related state are stored in taosd. taosAdapter only performs protocol conversion and credential passthrough. taosc, whether called internally by Adapter or embedded in a connector, performs the final private-protocol authentication handshake.

### 2.1 Path A: WebSocket / REST via taosAdapter

taosAdapter is the unified gateway for applications to access TDengine through RESTful APIs and WebSocket. The WebSocket mode of all higher-level language connectors (Python/Java/Go/Rust/CSharp/Node.js/ODBC) uses this path (see the entry-layer examples in section 1.1.1).

#### 2.1.1 Server-Side Supported Authentication Methods

| Method | HTTP Header | Description |
|--------|-------------|-------------|
| Basic Auth | `Authorization: Basic base64(user:pass)` | Standard HTTP Basic authentication. taosAdapter unwraps it and sends username/password to taosc |
| Bearer Token | `Authorization: Bearer <token>` | Enterprise Edition long-lived token. taosAdapter passes it through to taosd for verification |

taosAdapter **does not pass the source IP through as the login IP** unless `X-Forwarded-For` parsing is configured. Therefore, the account-level `HOST` whitelist in taosd sees the host IP where taosAdapter runs. If access control must be based on client IP, enforce it in front of taosAdapter (API gateway / Nginx).

#### 2.1.2 Enable HTTPS (taosAdapter)

taosAdapter is unencrypted by default. To enable HTTPS/WSS:

```toml
# /etc/taos/taosadapter.toml
[ssl]
enable   = true
certFile = "/etc/taos/certs/adapter.pem"
keyFile  = "/etc/taos/certs/adapter.key"
```

> **Note:** Only PEM certificates are supported. For detailed TLS configuration, see section 1 of [Full-Trace Transport Security](./02-full-trace-transport.md).

### 2.2 Path B: taosc -> taosd Private Protocol

taosc is the native TDengine client library. As an **independent component**, it connects directly to taosd over TCP :6030 and uses the TDengine **private protocol** (including SASL/SCRAM handshake and RPC frames). Applications can use this path through the C API or native DSNs (for example, Go `taosSql` and JDBC `jdbc:TAOS://`). Native connectors (C/Java JNI/Python native/Go cgo/Rust native/CSharp) all embed taosc as a dynamic library, and their transport and authentication behavior is identical to taosAdapter's internal calls to taosc.

#### 2.2.1 Supported Authentication Methods

| Authentication method | Protocol layer | Version requirement |
|-----------------------|----------------|---------------------|
| Username + password (SCRAM-SHA-256) | TCP/TLS | Enterprise Edition |
| Username + password (MD5, backward compatible) | TCP | Community Edition |
| TOTP multi-factor authentication | TCP/TLS | Enterprise Edition |
| TOKEN authentication | TCP/TLS/HTTP | Enterprise Edition |
| IP whitelist/blacklist | Network layer | Enterprise Edition |
| Login time-window restrictions | Application layer | Enterprise Edition |

#### 2.2.2 User Management

For complete syntax and parameter descriptions, see the SQL manual [Users](../05-tdengine-sql/07-user-and-privilege/01-user.md). The following is an excerpt for full-trace authentication scenarios.

**Create a user (complete syntax):**

```sql
CREATE USER [IF NOT EXISTS] <username>
    PASS '<password>'
    [TOTPSEED '<totpseed>']          -- Specify the TOTP key seed (Enterprise Edition)
    [ACCOUNT LOCK | ACCOUNT UNLOCK]  -- Initial lock state
    [
        SYSINFO { 0 | 1 }            -- Whether system information can be viewed (default 1)
        CREATEDB { 0 | 1 }           -- Whether databases can be created (default 0)
        CHANGEPASS { 0 | 1 | 2 }     -- 0=cannot change, 1=must change, 2=can change (default 2)
        TOTP { 0 | 1 }               -- Whether TOTP is enabled (default 0), Enterprise Edition
    ]
    [
        SESSIONS <n>                 -- Maximum concurrent sessions (default 10)

        CONNECTIONS <n>              -- Maximum total connections (default 100)

        QUERIES <n>                  -- Maximum concurrent queries (default 10)

        FAILED_LOGIN_ATTEMPTS <n>    -- Maximum failed attempts (default 3)

        PASSWORD_LOCK_TIME <min>     -- Lock duration in minutes (default 1440)
        PASSWORD_LIFE_TIME <days>    -- Password validity in days (default 90)
        PASSWORD_GRACE_TIME <days>   -- Grace period in days (default 7)
        PASSWORD_REUSE_TIME <days>   -- Days during which old passwords cannot be reused (default 30)
        PASSWORD_REUSE_MAX <n>       -- Number of changes required before reuse is allowed (default 5)
        INACTIVE_ACCOUNT_TIME <days> -- Inactivity lockout days (default 90)
        CONNECT_IDLE_TIME <min>      -- Idle connection timeout in minutes
        CONNECT_TIME <min>           -- Maximum session duration in minutes
    ]
    [HOST '<CIDR>' | NOT_ALLOW_HOST '<CIDR>']         -- IP whitelist/blacklist
    [ALLOWED_TIME '<cron>' | DENIED_TIME '<cron>'];   -- Time-window restriction
```

**Modify a user (ALTER USER):**

```sql
ALTER USER alice PASS 'NewP@ssw0rd!';
ALTER USER alice ACCOUNT LOCK;
ALTER USER alice ACCOUNT UNLOCK;
ALTER USER alice FAILED_LOGIN_ATTEMPTS 5;
ALTER USER alice ADD HOST '192.168.1.0/24';
```

#### 2.2.3 Strong Password Policy

Enterprise Edition enforces the following password constraints (Community Edition does not enforce these constraints):

- Length: 8 to 128 characters
- Must contain at least three of the following categories: uppercase letters, lowercase letters, digits, and special characters (`! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`)
- Must not be identical to the username
- History check: constrained by `PASSWORD_REUSE_TIME` and `PASSWORD_REUSE_MAX`

#### 2.2.4 Login Failure Lockout

After `FAILED_LOGIN_ATTEMPTS` consecutive failed login attempts, the account is locked automatically:

| Lock reason | Automatic unlock | Manual unlock | Effect on existing sessions |
|-------------|------------------|---------------|-----------------------------|
| Failed attempts exceeded | Automatically unlocked after `PASSWORD_LOCK_TIME` | `ALTER USER xxx ACCOUNT UNLOCK` | No effect |
| Permanent manual lock | Not supported | `ALTER USER xxx ACCOUNT UNLOCK` | Disconnected immediately |
| Inactivity timeout | Not supported | `ALTER USER xxx ACCOUNT UNLOCK` | No effect |
| Password expired | Unlocked after password change | - | No effect |

> **Note**: After failed attempts are exceeded, TOKEN login remains available. After a permanent lock, all login methods are unavailable.

#### 2.2.5 TOTP Multi-Factor Authentication

TDengine implements time-based one-time passwords (TOTP) based on RFC 6238.

**Configuration steps:**

```sql
-- 1. Specify the TOTP seed when creating the user
CREATE USER alice PASS 'P@ssw0rd!' TOTPSEED 'my-seed-string' TOTP 1;

-- 2. Obtain the actual TOTP secret (for scanning by an Authenticator App)
SELECT GENERATE_TOTP_SECRET('my-seed-string');

-- 3. Update the TOTP seed (rotate the key)
ALTER USER alice TOTPSEED 'new-seed-string';
```

**Login flow (after TOTP is enabled):**

1. The application calls `taos_connect_totp(host, user, password, totp_code, db, port)`.
2. taosd verifies the password first, then verifies the TOTP code (30-second time window).
3. The session is established only after both checks pass.

> If a user has enabled TOTP but has not generated a key yet, password login is still available, but the user can only execute the `UPDATE TOTP` command.

#### 2.2.6 TOKEN Authentication

TOKEN authentication is suitable for long-running applications and avoids storing user passwords in plaintext configuration.

**Create a TOKEN:**

```sql
CREATE TOKEN
    auth_code 'my_auth_code'
    client_id  'sensor_gateway'
    expire_time '2026-12-31T23:59:59'
    other_info  'production environment';

-- Exchange auth_code for the actual token
SELECT token FROM information_schema.ins_tokens WHERE auth_code = 'my_auth_code';
```

**Use TOKEN to log in:**

```c
// C API (TOKEN authentication, passing the token string directly)
TAOS *taos_connect_auth(host, user, token, db, port);
```

#### 2.2.7 Native Connector Configuration Examples

```go
// Go native connection (directly to taosd :6030, without taosAdapter)
dsn := "tduser:SecurePass123!@tcp(localhost:6030)/mydb"
db, err := sql.Open("taosSql", dsn)
```

```java
// JDBC native connection (directly to taosd :6030)
String url = "jdbc:TAOS://localhost:6030/mydb?user=tduser&password=SecurePass123!";
Connection conn = DriverManager.getConnection(url);
```

---

## 3. Data Ingestion Pipeline Authentication

The data ingestion pipeline is a background data pipeline that runs alongside the cluster and mainly involves `taosX` (data pipeline engine) and `taosX-Agent` (edge ingestion agent). This layer must distinguish **four credential types**:

1. **taosX -> taosAdapter (Sink writes)**: taosX writes data to the target TDengine as a client;
2. **taosX's own interfaces**: used by taosExplorer for management and by Agent connections;
3. **taosX-Agent -> taosX**: the taosX-Agent side connects through a registration Token;
4. **External source -> taosX / Agent**: source systems use their own authentication protocols.

### 3.1 taosX Sink DSN Authentication

taosX specifies connection targets and credentials through DSNs. **The WebSocket method is recommended** to reuse taosAdapter security capabilities such as SSL encryption and IP whitelists:

```bash
# Recommended: WebSocket connection (via taosAdapter :6041)
taos+ws://tduser:SecurePass123!@hostname:6041/db1

# Recommended: WebSocket + TLS (taosAdapter SSL must be enabled first)
taos+wss://tduser:SecurePass123!@hostname:6041/db1

# Bearer Token authentication (Enterprise Edition, token generated by CREATE TOKEN)
taos+ws://hostname:6041/db1?bearer_token=<bearer_token>

# TDengine Cloud (Token authentication)
taos+wss://cloud.tdengine.com/db1?token=<cloud_token>

# Fallback: native taosc direct connection to taosd (:6030), suitable for local deployments
taos://tduser:SecurePass123!@hostname:6030/db1
```

### 3.2 taosX Interface Authentication

taosX exposes two interfaces:

| Interface | Protocol | Default port | Encryption supported | Default configuration | Purpose |
|-----------|----------|--------------|----------------------|-----------------------|---------|
| REST API | HTTP 1.1 | 6050 | Yes (HTTPS) | Unencrypted | Called by taosExplorer / management tools |
| gRPC | HTTP/2 | 6055 | Yes (gRPC TLS) | Unencrypted | Used by taosX-Agent connections |

**taosX itself does not provide an independent authentication mechanism**. Security recommendations:

1. **Bind to the local address** (when taosX and Explorer are on the same host):

```toml
# /etc/taos/taosX.toml
[serve]
listen = "127.0.0.1:6050"
grpc   = "127.0.0.1:6055"
```

2. **Enable HTTPS** (required when exposed externally, v3.3.6.0+):

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

After enabling HTTPS, update the taosExplorer connection addresses accordingly:

```toml
# /etc/taos/explorer.toml
x_api = "https://127.0.0.1:6050"
grpc  = "https://public.domain.name:6055"
```

### 3.3 taosX-Agent Registration Authentication

taosX-Agent is deployed near external data sources, such as OPC-DA, KingHistorian, pSpace, PI, and other industrial control systems. It collects data and pushes it to taosX through gRPC. It uses **registered Agent credentials**: only Agents registered with taosX can establish connections.

1. Create an Agent in taosExplorer and obtain the registration Token.
2. Enter the Token and taosX gRPC address in the Agent configuration.

```toml
# /etc/taos/agent.toml
endpoint = "grpc://taosX-host:6055"
token    = "<agent_registration_token>"
```

3. After HTTPS is enabled on taosX, the connection between Agent and taosX is automatically upgraded to encrypted gRPC TLS.

> **Recommendation**: Always enable HTTPS for taosX in insecure networks (public networks, cross-IDC links) to secure the Agent transport path.

### 3.4 External Data Source Authentication

Different external data sources use different authentication methods, configured on the data source configuration page in taosExplorer. taosX / Agent acts only as a client and sends credentials according to the source protocol:

| Data source | Common authentication mechanisms |
|-------------|----------------------------------|
| MQTT Broker | Username/password; TLS client certificate |
| Kafka | SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512; mTLS |
| OPC-UA | Username/password; X.509 certificate |
| OPC-DA | Windows domain account (DCOM) |
| MySQL / PostgreSQL / MSSQL | Username/password; SSL certificate |
| Pulsar | JWT Token; OAuth 2.0; TLS |
| InfluxDB / OpenTSDB | HTTP Basic Auth; Token |

> Source-side credentials should be stored as encrypted fields in Explorer, and dedicated read-only accounts should be created for taosX / Agent.

---

## 4. Cluster Internal Authentication (DNode ↔ DNode)

All nodes in a TDengine cluster (including DNode-to-DNode, MNode-to-DNode, and MNode-to-MNode communication) mutually authenticate through **SASL/SCRAM-SHA-256**.

### 4.1 Inter-Node Authentication Mechanism

- Keys are generated and distributed through the `taosk` tool.
- Inter-node encryption keys are distributed through encrypted channels to prevent man-in-the-middle attacks.
- All inter-node communication uses the same SCRAM authentication mechanism, with no distinction between MNode and DNode.

### 4.2 Dynamic TLS Certificate Loading

TLS certificates can be rotated without restarting services:

```sql
-- Reload TLS certificates online (all DNodes)
ALTER DNODES RELOAD TLS;

-- Reload a single DNode
ALTER DNODE <dnode_id> RELOAD TLS;
```

---

## 5. Observability Access Authentication

The main authentication target in the observability access layer is `taosKeeper`. It writes monitoring metrics reported by components back to the `log` database. On the default path (`auditSaveInSelf = 0`), Enterprise Edition audit logs are also written through Keeper to an audit database marked with `IS_AUDIT` (separate from the `log` database). In `v3.4.1.0+`, `auditSaveInSelf = 1` allows audit data to be written directly by the local cluster without Keeper. For details, see [Audit and Compliance](./07-audit-and-compliance.md).

### 5.1 taosKeeper Authentication

taosKeeper connects to taosAdapter through WebSocket and writes metrics to the TDengine `log` database. In classic deployments, it can also receive audit data reported by `taosd` according to `auditInterval` and write it to the audit database. Components such as taosd, taosX, and taosExplorer push their own metrics to taosKeeper over HTTP.

taosKeeper exposes HTTP port :6043 to receive metrics (and audit data on the default path) from components. In the current version, taosKeeper does not authenticate reporters and relies on network isolation for protection. Audit reporting through Keeper can be strengthened on the `taosd` side by `auditHttps` / `auditUseToken` (Token-based write-side access usually requires `SYSAUDIT_LOG`).

**Authentication configuration for taosKeeper connecting to taosAdapter**:

```toml
# /etc/taos/taoskeeper.toml
[tdengine]
host     = "localhost"
port     = 6041
username = "keeper_writer"    # Create a dedicated low-privilege account
password = "KeeperPass123!"   # Stored in plaintext; strictly control file permissions
usessl   = false
```

**Known security risks and mitigations:**

| Risk | Mitigation |
|------|------------|
| Exposed :6043 port | Restrict firewall access to internal cluster IPs only |
| Plaintext password in configuration file | `chmod 600 /etc/taos/taoskeeper.toml` |
| No reporter authentication | Deploy on a trusted intranet and do not expose externally; prefer `auditUseToken = 1` for audit reporting |

---

## 6. Storage and Audit Access Control

Business data resides in normal business databases; monitoring metrics reside in the `log` database by default; audit logs reside in an **audit database** marked with `IS_AUDIT` (the default name is often `audit`). These are not independently exposed authentication entry points. Access to storage files depends on authentication completed by earlier layers, while audit log reads are controlled through roles such as `SYSAUDIT` (not ordinary "read-only business accounts").

### 6.1 Storage Layer Access Control

The storage layer itself does not directly authenticate users. All on-disk data (vnode WAL, TSDB data files, and metadata) can be accessed only through query/write requests authenticated by layers 2/3. Authentication-related points:

- **On-disk data access**: Queries and writes complete user authentication inside taosd (SCRAM-SHA-256 / Token / TOTP). The storage layer filters data according to the user's RBAC/MAC permissions (see section 7.2). **There is no legitimate path that bypasses authentication to directly read or write data files**.
- **TDE key management**: Transparent Data Encryption (TDE) uses hierarchical keys generated by Enterprise Edition `taosk`; mutable `SVR_KEY` / `DB_KEY` can also be rotated at runtime by administrators through `ALTER SYSTEM SET`. Key operations should be separated from business read/write accounts.
- **Details**: For key hierarchy, encrypted databases, secure deletion, and related topics, see [Data at Rest Protection](./06-data-security.md).

### 6.2 Audit Log Access Control

Audit logs are written to an **audit database** marked with `IS_AUDIT` (the default name is often `audit`, and it is **not** the monitoring `log` database). The write path is through `taosKeeper` by default, or direct local-cluster writes through `auditSaveInSelf` in `v3.4.1.0+`. For configuration, levels, and table schema, see [Audit and Compliance](./07-audit-and-compliance.md). For the permission model, see [Privileges · Audit Database](../05-tdengine-sql/07-user-and-privilege/02-grant.md#audit-database).

- Viewing audit data requires `SYSAUDIT`; writes are completed by `SYSAUDIT_LOG` / system reporting paths. Business accounts should not hold write permissions on the audit database.
- When `auditLevel >= 5`, operations such as `select` on audit tables may also generate new audit events.

---

## 7. Cross-Layer Security Controls

The following controls take effect across multiple layers and horizontally strengthen the layered authentication described above.

### 7.1 IP Access Control {#ip-access-control}

IP access control takes effect at the taosd account level. It supports CIDR notation and can configure whitelists and blacklists at the same time. For complete syntax, add/delete/query operations, and notes, see [Users](../05-tdengine-sql/07-user-and-privilege/01-user.md).

```sql
-- Allow connections only from specified network segments
ALTER USER alice ADD HOST '192.168.10.0/24';
ALTER USER alice ADD HOST '10.0.0.0/8';

-- Deny connections from a specified IP
ALTER USER alice ADD NOT_ALLOW_HOST '203.0.113.5/32';
```

> Rule changes take effect immediately and immediately disconnect existing connections that no longer comply.

Whitelists and blacklists take effect only after `enableWhiteList` is set to `1` (for parameter descriptions, see [taosd](../12-operations-and-tooling/03-components/01-taosd.md)):

```ini
# taos.cfg
enableWhiteList = 1
```

### 7.2 Privilege Management (RBAC + MAC)

For the privilege matrix and complete `GRANT`/`REVOKE`/`ROLE` syntax, see [Privileges](../05-tdengine-sql/07-user-and-privilege/02-grant.md).

TDengine Enterprise Edition supports two privilege models:

- **RBAC (Role-Based Access Control)**: user -> role -> privilege
- **MAC (Mandatory Access Control)**: security labels enforce access control

| Privilege scope | Description |
|-----------------|-------------|
| Database level | READ / WRITE / ALL |
| Table level | SELECT / INSERT / UPDATE / DELETE |
| Column level | Column blacklist (deny access to specified columns) |
| System level | SYSINFO (view cluster information), CREATEDB (database creation privilege) |

```sql
GRANT READ ON db_name.* TO user1;
GRANT WRITE ON db_name.stb1 TO user2;
CREATE ROLE analyst;
GRANT READ ON sensor_db.* TO analyst;
GRANT ROLE analyst TO alice;
REVOKE WRITE ON db_name.* FROM user2;
```

For user CRUD, strong passwords, IP whitelists/blacklists, TOTP / Token, and related syntax, see [Users](../05-tdengine-sql/07-user-and-privilege/01-user.md). For the authorization matrix and separation of duties, see [Privileges](../05-tdengine-sql/07-user-and-privilege/02-grant.md).

#### 7.2.1 Practice Suggestions {#practice-suggestions}

1. **Least privilege**: Split accounts or Tokens by application and grant only the required database, table, and topic privileges. Avoid shared `root` credentials.
2. **Separation of duties**: In Enterprise Edition `v3.4.0.0+`, enable separation of duties (`SYSDBA` / `SYSSEC` / `SYSAUDIT`) to avoid a single super account being responsible for database creation, authorization, and auditing at the same time.
3. **Credential lifecycle**: Rotate passwords and Tokens regularly. After a password changes, connections using the old password are kicked out (except Token connections). See [Users](../05-tdengine-sql/07-user-and-privilege/01-user.md).
4. **Audit linkage**: Key authorization and user changes should be included in audit coverage. See [Audit and Compliance](./07-audit-and-compliance.md).

### 7.3 API Gateway Authentication Enhancement

We recommend using TDengine TSDB inside a LAN. If it must be exposed externally, configure an API gateway in front:

#### 7.3.1 Nginx Load Balancing + SSL

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

#### 7.3.2 Traefik Security Gateway

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

## 8. Security Audit

For complete configuration, operation levels, and viewing methods, see [Audit and Compliance](./07-audit-and-compliance.md). Enterprise Edition requires an `IS_AUDIT` audit database that meets the constraints. By default, audit data is written through `taosKeeper`; when `auditSaveInSelf = 1` (`v3.4.1.0+`), the local cluster can write audit data directly without taosKeeper. Common audit items related to authentication include:

| Event type | Description |
|------------|-------------|
| Login | Records `login` when `auditLevel >= 2` (Details include appName and other information) |
| User / privilege changes | `createUser` / `alterUser` / `dropUser`, `grantPrivileges` / `revokePrivileges` (level 2) |
| Other authentication-related DDL | See the [operation list](./07-audit-and-compliance.md); not all login failure / lockout details are recorded as separate rows |

Enablement example (`taos.cfg` or dynamic SQL):

```ini
audit             = 1
auditInterval     = 5000
auditLevel        = 3
auditCreateTable  = 1
# auditHttps / auditUseToken take effect only on the Keeper path
# auditSaveInSelf = 1   # v3.4.1.0+ local-cluster direct write, can bypass taosKeeper
```

---

## 9. Operations Troubleshooting

### 9.1 Common Error Codes

| Error code | Meaning | Troubleshooting suggestion |
|------------|---------|----------------------------|
| `0x0375` | Source IP is not in the whitelist | Check `HOST` configuration |
| `0x0376` | Source IP is in the blacklist | Check `NOT_ALLOW_HOST` configuration |
| `0x0379` | Session count exceeded | Adjust the `SESSIONS` limit |
| `0x037A` | Concurrent query limit exceeded | Adjust the `QUERIES` limit |
| `0x037B` | Account locked (failed attempts exceeded) | `ALTER USER xxx ACCOUNT UNLOCK` |
| `0x037C` | Account manually locked | `ALTER USER xxx ACCOUNT UNLOCK` |
| `0x037D` | Password expired | Change the password and log in again |
| `0x037E` | Incorrect TOTP code | Check time synchronization and obtain a new code |
| `0x037F` | Login attempted during a denied time window | Check `DENIED_TIME` configuration |

### 9.2 Query Current User Status

```sql
SHOW USERS;
SELECT * FROM information_schema.ins_users WHERE name = 'alice';
SELECT * FROM information_schema.ins_tokens;
SHOW CONNECTIONS;
```

---

## 10. Authentication Deployment Checklist

### 10.1 Pre-Production Checks

- [ ] All users have strong passwords configured (8+ characters, including uppercase/lowercase letters, digits, and special characters)
- [ ] Administrator accounts have TOTP second-factor authentication enabled
- [ ] The default `root` password has been changed
- [ ] Applications use Token authentication (not plaintext passwords)
- [ ] IP whitelists are configured (`HOST`) to restrict trusted sources
- [ ] Login failure lockout policies are configured (`FAILED_LOGIN_ATTEMPTS`)
- [ ] taosKeeper configuration file permissions are set to 600
- [ ] taosX Agent Token is correctly configured (not applicable if the component is not used)
- [ ] Enterprise Edition audit is enabled (`audit = 1`), an `IS_AUDIT` audit database has been created, and audit data is viewed with a `SYSAUDIT` account
- [ ] CLI operations use environment variables or interactive passwords and do not expose passwords directly on the command line

### 10.2 OAuth SSO (If Enabled)

- [ ] `EXPLORER_SECURITY_ENCRYPTION_KEY` is configured (required in production; do not use the default value)
- [ ] OAuth `redirect_uri` uses HTTPS
- [ ] The correct `redirect_uri` and `client_id`/`secret` are registered in the IdP
- [ ] Users complete taosd credential binding after first SSO login
- [ ] OAuth session timeout policy is reasonable (default 8 hours)

### 10.3 Routine Operations

- [ ] Regularly check `SHOW CONNECTIONS` and clean up abnormal sessions
- [ ] Rotate Tokens regularly (`DROP TOKEN` + regenerate)
- [ ] Monitor authentication failure logs and investigate abnormal growth
- [ ] Password expiration policy is configured (`PASSWORD_LIFE_TIME`)
