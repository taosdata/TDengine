---
sidebar_label: 安全部署配置建议
title: 安全部署配置建议
toc_max_heading_level: 4
---

## 背景

TDengine 的分布式、多组件特性导致 TDengine 的安全配置是生产系统中比较关注的问题。本文档旨在对 TDengine 各组件及在不同部署方式下的安全问题进行说明，并提供部署和配置建议，为用户的数据安全提供支持。

## 安全配置涉及组件

TDengine 包含多个组件，有：

- `taosd`：内核组件。
- `taosc`：客户端库。
- `taosAdapter`：REST API 和 WebSocket 服务。
- `taosKeeper`：监控服务组件。
- `taosX`：数据管道和备份恢复组件。
- `taosxAgent`：外部数据源数据接入辅助组件。
- `taosExplorer`：Web 可视化管理界面。

与 TDengine 部署和应用相关，还会存在以下组件：

- 通过各种连接器接入并使用 TDengine 数据库的应用。
- 外部数据源：指接入 TDengine 的其他数据源，如 MQTT、OPC、Kafka 等。

各组件关系如下：

![TDengine 产品生态拓扑架构](./tdengine-topology.png)

关于各组件的详细介绍，请参考 [组件介绍](../intro)。

## TDengine 安全设置

### `taosd`

taosd 集群间使用 TCP 连接基于自有协议进行数据交换，风险较低，但传输过程不是加密的，仍有一定安全风险。

启用压缩可能对 TCP 数据混淆有帮助。

- **compressMsgSize**：是否对 RPC 消息进行压缩，整数，可选：-1：所有消息都不压缩；0：所有消息都压缩；N (N>0)：只有大于 N 个字节的消息才压缩。

为了保证数据库操作可追溯，建议启用审计功能。

- **audit**：审计功能开关，0 为关，1 为开。默认打开。
- **auditInterval**：上报间隔，单位为毫秒。默认 5000。
- **auditCreateTable**：是否针对创建子表开启申计功能。0 为关，1 为开。默认打开。

为保证数据文件安全，可启用数据库加密。

- **encryptAlgorithm**：数据加密算法。
- **encryptScope**：数据加密范围。

启用白名单可限制访问地址，进一步增强私密性。

- **enableWhiteList**：白名单功能开关，0 为关，1 为开；默认关闭。

### `taosc`

用户和其他组件与 `taosd` 之间使用原生客户端库（taosc）和自有协议进行连接，数据安全风险较低，但传输过程仍然不是加密的，有一定安全风险。

### `taosAdapter`

taosadapter 与 taosd 之间使用原生客户端库（taosc）和自有协议进行连接，同样支持 RPC 消息压缩，不会造成数据安全问题。

应用和其他组件通过各语言连接器与 taosadapter 进行连接。默认情况下，连接是基于 HTTP 1.1 且不加密的。要保证 taosadapter 与其他组件之间的数据传输安全，需要配置 SSL 加密连接。在 `/etc/taos/taosadapter.toml` 配置文件中修改如下配置：

```toml
[ssl]
enable = true
certFile = "/path/to/certificate-file"
keyFile = "/path/to/private-key"
```

在连接器中配置 HTTPS/SSL 访问方式，完成加密访问。

为进一步增强安全性，可启用白名单功能，在 `taosd` 中配置，对 taosdapter 组件同样生效。

### `taosX`

`taosX` 对外包括 REST API 接口和 gRPC 接口，其中 gRPC 接口用于 taos-agent 连接。

- REST API 接口是基于 HTTP 1.1 且不加密的，有安全风险。
- gRPC 接口基于 HTTP 2 且不加密，有安全风险。

为了保证数据安全，建议 taosX API 接口仅限内部访问。在 `/etc/taos/taosx.toml` 配置文件中修改如下配置：

```toml
[serve]
listen = "127.0.0.1:6050"
grpc = "127.0.0.1:6055"
```

从 TDengine 3.3.6.0 开始，taosX 支持 HTTPS 连接，在 `/etc/taos/taosx.toml` 文件中添加如下配置：

```toml
[serve]
ssl_cert = "/path/to/server.pem"
ssl_key =  "/path/to/server.key"
ssl_ca =   "/path/to/ca.pem"
```

并在 Explorer 中修改 API 地址为 HTTPS 连接：

```toml
# taosX API 本地连接
x_api = "https://127.0.01:6050"
# Public IP 或者域名地址
grpc = "https://public.domain.name:6055"
```

### `taosExplorer`

与 `taosAdapter` 组件相似，`taosExplorer` 组件提供 HTTP 服务对外访问。在 `/etc/taos/explorer.toml` 配置文件中修改如下配置：

```toml
[ssl]
# SSL certificate file
certificate = "/path/to/ca.file"

# SSL certificate private key
certificate_key = "/path/to/key.file"
```

之后，使用 HTTPS 进行 Explorer 访问，如 [https://192.168.12.34](https://192.168.12.34:6060) 。

### `taosxAgent`

taosX 启用 HTTPS 后，Agent 组件与 taosx 之间使用 HTTP 2 加密连接，使用 Arrow-Flight RPC 进行数据交换，传输内容是二进制格式，且仅注册过的 Agent 连接有效，保障数据安全。

建议在不安全网络或公共网络环境下的 Agent 服务，始终开启 HTTPS 连接。

### `taosKeeper`

taosKeeper 使用 WebSocket 连接与 taosadpater 通信，将其他组件上报的监控信息写入 TDengine。

`taosKeeper` 当前版本存在安全风险：

- 监控地址不可限制在本机，默认监控 所有地址的 6043 端口，存在网络攻击风险。使用 Docker 或 Kubernetes 部署不暴露 taosKeeper 端口时，此风险可忽略。
- 配置文件中配置明文密码，需要降低配置文件可见性。在 `/etc/taos/taoskeeper.toml` 中存在：

```toml
[tdengine]
host = "localhost"
port = 6041
username = "root"
password = "taosdata"
usessl = false
```

## 安全增强

我们建议使用在局域网内部使用 TDengine。

如果必须在局域网外部提供访问，请考虑添加以下配置：

### 负载均衡

使用负载均衡对外提供 taosAdapter 服务。

以 Nginx 为例，配置多节点负载均衡：

```nginx
http {
    server {
        listen 6041;
        
        location / {
            proxy_pass http://websocket;
            # Headers for websocket compatible
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            # Forwarded headers
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-Host $host;
            proxy_set_header X-Forwarded-Port $server_port;
            proxy_set_header X-Forwarded-Server $hostname;
            proxy_set_header X-Real-IP $remote_addr;
        }
    }
 
    upstream websocket {
        server 192.168.11.61:6041;
        server 192.168.11.62:6041;
        server 192.168.11.63:6041;
   }
}
```

如果 taosAdapter 组件未配置 SSL 安全连接，还需要配置 SSL 才能保证安全访问。SSL 可以配置在更上层的 API Gateway，也可以配置在 Nginx 中；如果你对各组件之间的安全性有更强的要求，您可以在所有组件中都配置 SSL。Nginx 配置如下：

```nginx
http {
    server {
        listen 443 ssl;

        ssl_certificate /path/to/your/certificate.crt;
        ssl_certificate_key /path/to/your/private.key;
    }
}
```

### 安全网关

在现在互联网生产系统中，安全网关使用也很普遍。[traefik](https://traefik.io/) 是一个很好的开源选择，我们以 traefik 为例，解释在 API 网关中的安全配置。

Traefik 中通过 middleware 中间件提供多种安全配置，包括：

1. 认证（Authentication）：Traefik 提供 BasicAuth、DigestAuth、自定义认证中间件、OAuth 2.0 等多种认证方式。
2. IP 白名单（IPWhitelist）：限制允许访问的客户端 IP。
3. 频率限制（RateLimit）：控制发送到服务的请求数。
4. 自定义 Headers：通过自定义 Headers 添加 `allowedHosts` 等配置，提高安全性。

一个常见的中间件示例如下：

```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.tdengine.rule=Host(`api.tdengine.example.com`)"
  - "traefik.http.routers.tdengine.entrypoints=https"
  - "traefik.http.routers.tdengine.tls.certresolver=default"
  - "traefik.http.routers.tdengine.service=tdengine"
  - "traefik.http.services.tdengine.loadbalancer.server.port=6041"
  - "traefik.http.middlewares.redirect-to-https.redirectscheme.scheme=https"
  - "traefik.http.middlewares.check-header.headers.customrequestheaders.X-Secret-Header=SecretValue"
  - "traefik.http.middlewares.check-header.headers.customresponseheaders.X-Header-Check=true"
  - "traefik.http.middlewares.tdengine-ipwhitelist.ipwhitelist.sourcerange=127.0.0.1/32, 192.168.1.7"
  - "traefik.http.routers.tdengine.middlewares=redirect-to-https,check-header,tdengine-ipwhitelist"
```

上面的示例完成以下配置：

- TLS 认证使用 `default` 配置，这个配置可使用配置文件或 traefik 启动参数中配置，如下：

    ```yaml
    traefik:
    image: "traefik:v2.3.2"
    hostname: "traefik"
    networks:
    - traefik
    command:
    - "--log.level=INFO"
    - "--api.insecure=true"
    - "--providers.docker=true"
    - "--providers.docker.exposedbydefault=false"
    - "--providers.docker.swarmmode=true"
    - "--providers.docker.network=traefik"
    - "--providers.docker.watch=true"
    - "--entrypoints.http.address=:80"
    - "--entrypoints.https.address=:443"
    - "--certificatesresolvers.default.acme.dnschallenge=true"
    - "--certificatesresolvers.default.acme.dnschallenge.provider=alidns"
    - "--certificatesresolvers.default.acme.dnschallenge.resolvers=ns1.alidns.com"
    - "--certificatesresolvers.default.acme.email=linhehuo@gmail.com"
    - "--certificatesresolvers.default.acme.storage=/letsencrypt/acme.json"
    ```

上面的启动参数配置了 `default` TSL 证书解析器和自动 acme 认证（自动证书申请和延期）。

- 中间件 `redirect-to-https`：配置从 HTTP 到 HTTPS 的转发，强制使用安全连接。

    ```yaml
    - "traefik.http.middlewares.redirect-to-https.redirectscheme.scheme=https"
    ```

- 中间件 `check-header`：配置自定义 Headers 检查。外部访问必须添加自定义 Header 并匹配 Header 值，避免非法访问。这在提供 API 访问时是一个非常简单有效的安全机制。
- 中间件 `tdengine-ipwhitelist`：配置 IP 白名单。仅允许指定 IP 访问，使用 CIDR 路由规则进行匹配，可以设置内网及外网 IP 地址。

## 总结

数据安全是 TDengine 产品的一项关键指标，这些措施旨在保护 TDengine 部署免受未经授权的访问和数据泄露，同时保持性能和功能。但 TDengine 自身的安全配置不是生产中的唯一保障，结合用户业务系统制定更加匹配客户需求的解决方案更加重要。
