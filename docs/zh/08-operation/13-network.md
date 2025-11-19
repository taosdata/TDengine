---
sidebar_label: 网络配置
title: 网络配置 
toc_max_heading_level: 4
---

### IPv6

TDengine 支持 IPv6 网络环境。该功能允许用户在现代网络基础设施中部署和连接 TDengine，减少对 IPv4 的依赖，以满足日益增长的 IPv6 网络需求。

#### 支持范围

- 支持版本：TDengine Server 和 Client 版本均需 ≥ 3.3.7.0

- 支持组件：

  - taosd：TDengine 数据库服务端
  - taos：TDengine 命令行客户端 (CLI)
  - 各种连接器：如 JDBC, Go, Python, C#, Rust 等（需使用支持 IPv6 的版本）

- 网络环境：纯 IPv6 环境或 IPv4/IPv6 双栈环境均支持

#### 服务端 (taosd) 配置

启用 IPv6 支持需在 TDengine 服务端的配置文件 `taos.cfg` 中进行设置，步骤如下：

1. 定位配置文件：默认路径通常为 `/etc/taos/taos.cfg`
2. 修改配置参数：找到并修改以下关键参数

```bash
// 设置 TDengine 服务端在指定网络接口上监听 IPv6 地址，值为该接口对应的 IPv6 地址，或 "::" 表示监听所有可用 IPv6 接口

firstEp    ipv6_address1:port
secondEp   ipv6_address2:port
fqdn       ipv6_address1
enableIPv6 1

```

3. 重启服务：修改配置后，需要重启 TDengine 服务以使配置生效

```bash
sudo systemctl restart taosd
```

重要说明

- 强烈建议使用 FQDN（全限定域名）配置 firstEP 和 secondEP，而非直接使用 IP 地址。通过 DNS 解析可自动选 IPv6 地址，提升灵活性和兼容性。
- 默认端口 6030 同样适用于 IPv6 连接。

#### 客户端连接方式

客户端可通过以下两种方式连接到支持 IPv6 的 TDengine 服务端：

1. 使用 FQDN：在客户端的 taos.cfg 或连接字符串中，使用服务端的域名。若该域名的 AAAA 记录指向正确的 IPv6 地址，客户端会自动通过 IPv6 建立连接。示例：

```bash
taos -h your_server_fqdn -P 6030
```

2. 直接使用 IPv6 地址：连接时需直接指定服务端的 IPv6 地址，且在命令行或连接字符串中必须用中括号包裹。示例：

```bash
taos -h [2001:db8::1] -P 6030
```

3. 验证连接：成功连接后，可在 TDengine 日志中确认所有连接均通过 IPv6 进行。

#### 注意事项与故障排除

- 网络基础设施：确保服务器、客户端及中间的路由器、防火墙等网络设备均正确配置 IPv6，并允许 6030 端口（默认）的通信。
- DNS 配置：若使用 FQDN，需确保 DNS 服务器正确配置该域名的 AAAA 记录（指向 IPv6 地址），而非 A 记录（指向 IPv4 地址）。
- 双栈环境优先级：在同时支持 IPv4 和 IPv6 的双栈主机上，需显式配置 FQDN 对应 IPv6 地址，以优先使用 IPv6 连接。
- 连接器版本：确保所有 TDengine 客户端连接器（如 JDBC、Go、Python 等）为 3.3.7.0 或更高版本，以完全兼容 IPv6。

### TLS

TDengine 的传输层支持加密通信，保障数据在网络传输过程中的安全性。

#### 参数说明

1. `tlsCaPath`：CA 证书路径，客户端和服务端均需配置，不可动态调整。
2. `tlsSvrCertPath`：服务端证书路径，仅服务端配置，不可动态调整。
3. `tlsSvrKeyPath`：服务端私钥路径，仅服务端配置，不可动态调整。
4. `tlsCliCertPath`：客户端证书路径，客户端和服务端均需配置（服务端用于集群间通信，含单节点），不可动态调整。
5. `tlsCliKeyPath`：客户端私钥路径，客户端和服务端均需配置（服务端用于集群间通信，含单节点），不可动态调整。
6. `enableTLS`：客户端和服务端均需配置的开关参数。启用 TLS 前，必须先配置所有必需的证书路径参数；若配置错误或不完整，服务将无法启动。

#### 查看参数

通过以下命令可查看集群中各节点的 TLS 相关配置：

```sql
SHOW VARIABLES LIKE '%tls%'; 
```

#### 约束说明

1. 服务端启用 TLS 时，需完整配置上述 5 个证书路径参数，否则启动失败；若 5 个参数均未配置，则以非 TLS 模式启动。
2. 客户端启用 TLS 时，需配置 `tlsCaPath`、`tlsCliCertPath`、`tlsCliKeyPath` 3 个参数，否则启动失败；若 3 个参数均未配置，则以非 TLS 模式启动。
3. 集群所有节点需统一以 TLS 模式或非 TLS 模式启动，否则集群间访问会失败。
4. 非 TLS 模式的客户端无法访问 TLS 模式的集群，会返回连接失败。
5. TLS 模式的客户端无法访问非 TLS 模式的集群，会返回连接失败。
6. 启用 TLS 后，所有网络传输数据均需双向鉴权认证；即使仅部署单个 taosd，其内部模块通过 RPC 通信时也需双向鉴权。

#### 部署用例

1. 生成相关证书文件

```bash
# 生成 CA 证书和私钥
openssl req -newkey rsa:2048 -nodes -keyout ca.key -x509 -days 365 -out ca.crt -subj "/CN=MyCA"

# 生成服务器私钥和证书签名请求(CSR)
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/CN=localhost"  # CN 通常设为服务器域名或 IP

# 用 CA 证书签发服务器证书
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365

# 生成客户端私钥和证书签名请求(CSR)
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr -subj "/CN=Client"

# 用 CA 证书签发客户端证书
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365
```

2. 客户端配置（taos.cfg）

```bash
tlsCliKeyPath  /path/client.key
tlsCliCertPath /path/client.crt
tlsCaPath      /path/ca.crt
enableTLS      1 
```

3. 服务端配置（taos.cfg）

```bash
tlsCliKeyPath  /path/client.key
tlsCliCertPath /path/client.crt
tlsSvrKeyPath  /path/server.key
tlsSvrCertPath /path/server.crt
tlsCaPath      /path/ca.crt
enableTLS      1
```

4. 启动服务端后，使用客户端访问即可

#### 性能说明

启用 TLS 会对性能产生一定影响，整体性能下降约 1%~4%，实际性能为非 TLS 模式的 96%~99%。

#### 运维和升级

不支持动态升级（需重启服务使配置生效）。
