# Ignition OPC UA Server Guide

> 飞书原文（含截图）：<https://taosdata.feishu.cn/wiki/HE7KwgtzVi0NWokc6rjchHv1nMd>

## 概述

本文档介绍如何通过 TDengine Explorer 以证书加密模式（SignAndEncrypt）连接 Ignition OPC UA Server。

OPC UA 安全体系分为两层：

### 安全通道（Secure Channel）

负责 **传输层加密**，保护客户端与服务器之间的通信不被窃听或篡改。

需要配置：

- **Secure Channel Certificate**：taosx 客户端自己的证书，发送给 OPC UA 服务器用于身份识别
- **Certificate's Private Key**：上述证书对应的私钥，用于签名和解密

### 用户认证（Authentication）

负责 **用户身份验证**，确认连接者的身份。

支持三种方式：

- **Anonymous**：匿名访问（需服务器允许）
- **Username**：用户名 + 密码
- **Certificate**：证书认证（需服务器配置证书到用户的映射）

> **💡 提示**：从 Ignition 下载的服务器证书（如 `ignition-server.der`）是 Ignition OPC UA 服务器自身的证书，不能用作客户端证书。你需要自行生成客户端证书和私钥。

## Ignition OPC UA Server 配置

### Endpoint 配置

进入 Ignition Gateway → **Config** → **Connections** → **OPC** → **OPC UA Server Settings** → **General Settings**。

关键配置项：

| 配置项             | 推荐值             | 说明                                                           |
| ------------------ | ------------------ | -------------------------------------------------------------- |
| Bind Port          | `62541`            | OPC UA 服务监听端口                                            |
| Bind Addresses     | `0.0.0.0`          | 如果 TDengine 与 Ignition 不在同一台服务器，必须改为 `0.0.0.0` |
| Endpoint Addresses | 添加服务器 IP      | 例如 `192.168.1.100`，确保客户端可通过此地址访问               |
| Security Policies  | ☑ `Basic256Sha256` | 勾选所需的安全策略                                             |
| Security Mode      | ☑ `SignAndEncrypt` | 勾选签名并加密模式                                             |

### 认证配置

在同一页面的 **AUTHENTICATION** 部分：

- 如果使用 **Username 认证**：确保 User Source 设为 `default`（推荐），而非 `opcua-module`

> **⚠️ 注意**：User Source 建议使用 `default`。默认的 `opcua-module` 是一个独立的用户源，需要额外配置用户和权限，容易导致 `StatusBadUserAccessDenied` 错误。

### 权限配置

切换到 **Permissions** 标签页，确认 `AuthenticatedUser` 角色拥有所需权限：

| 角色              | Browse | Read | Write | Call |
| ----------------- | ------ | ---- | ----- | ---- |
| AuthenticatedUser | ☑      | ☑    | ☑     | ☑    |

**Default Tag Provider Permissions** 也需要同样配置。

## 生成客户端证书

OPC UA 插件使用 `tls.LoadX509KeyPair()` 加载证书，要求 **PEM 格式**。客户端 Application URI 固定为 `urn:taosx-opc:client`，证书的 SAN（Subject Alternative Name）中必须包含此 URI。

### Windows（PowerShell）

如果系统未安装 OpenSSL，可使用 Git for Windows 自带的 OpenSSL（路径通常为 `C:\Program Files\Git\usr\bin\openssl.exe`）。

**步骤 1：创建证书配置文件**

```powershell
@"
[req]
distinguished_name = req_dn
x509_extensions = v3_ext
prompt = no

[req_dn]
CN = taosx-opc-client
O = TDengine

[v3_ext]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = URI:urn:taosx-opc:client
"@ | Out-File -Encoding ascii $env:TEMP\opcua_client_ext.cnf
```

**步骤 2：生成证书和私钥**

```powershell
mkdir C:\taosx_certs -Force
openssl req -x509 -newkey rsa:2048 -nodes `
  -keyout C:\taosx_certs\client_key.pem `
  -out C:\taosx_certs\client_cert.pem `
  -days 3650 `
  -config $env:TEMP\opcua_client_ext.cnf
```

**步骤 3：验证证书**

```powershell
openssl x509 -in C:\taosx_certs\client_cert.pem -noout -subject -ext subjectAltName
```

预期输出：

```
subject=CN=taosx-opc-client, O=TDengine
X509v3 Subject Alternative Name:
    URI:urn:taosx-opc:client
```

### Linux / macOS

```bash
# 创建配置文件
cat > /tmp/opcua_client_ext.cnf << 'EOF'
[req]
distinguished_name = req_dn
x509_extensions = v3_ext
prompt = no

[req_dn]
CN = taosx-opc-client
O = TDengine

[v3_ext]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth, serverAuth
subjectAltName = URI:urn:taosx-opc:client
EOF

# 生成证书和私钥（有效期 10 年）
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout client_key.pem \
  -out client_cert.pem \
  -days 3650 \
  -config /tmp/opcua_client_ext.cnf
```

> **💡 提示**：证书可以在任意机器上生成，只要最终把 `client_cert.pem` 和 `client_key.pem` 上传到 Explorer 即可。

## 在 Ignition 中信任客户端证书

生成证书后，需要让 Ignition 信任该客户端证书：

1. 在 Explorer 中先用该证书进行一次连通性检查（会失败，这是正常的）
2. 进入 Ignition Gateway → **Config** → **Connections** → **OPC** → **Security** → **Server** 标签页
3. 在 **Quarantined Certificates** 中找到 `taosx-opc-client` 证书
4. 点击右侧 **⋮** → **Trust**
5. 确认证书已移至 **Trusted Certificates** 列表中

## 在 Explorer 中配置连接

进入 TDengine Explorer → **Data In** → **Create New Data In Task**，数据源类型选择 **OPC-UA**。

### 连接配置

| 配置项                     | 值                     | 说明                      |
| -------------------------- | ---------------------- | ------------------------- |
| Server Endpoint            | `192.168.1.100:62541`  | Ignition 服务器 IP + 端口 |
| Security Mode              | `SignAndEncrypt`       | 与 Ignition 端配置一致    |
| Security Policy            | `Basic256Sha256`       | 与 Ignition 端配置一致    |
| Secure Channel Certificate | 上传 `client_cert.pem` | 客户端证书                |
| Certificate's Private Key  | 上传 `client_key.pem`  | 客户端私钥                |

### 认证配置

选择 **Username** 标签页：

| 配置项   | 值              | 说明                     |
| -------- | --------------- | ------------------------ |
| Username | Ignition 用户名 | User Source 中已有的用户 |
| Password | 对应密码        |                          |

点击 **Check Connection** 验证连通性。

## 常见错误排查

| 错误信息                                     | 原因                                                               | 解决方法                                                                                   |
| -------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `StatusBadIdentityTokenInvalid (0x80200000)` | 用户身份令牌无效。通常是认证方式不匹配或证书未被服务器接受。       | 如果使用 Certificate 认证，改为 Username 认证；确认 Ignition User Source 配置正确。        |
| `StatusBadUserAccessDenied (0x801F0000)`     | 用户名密码正确但无权限。通常是用户不在指定 User Source 中。        | 将 Ignition User Source 改为 `default`，确保用户存在于该用户源中。                         |
| `StatusBadSecurityChecksFailed`              | 安全通道建立失败。通常是证书未被 Trust 或 Security Policy 不匹配。 | 在 Ignition Security 页面 Trust 客户端证书；确认 Security Policy 两端一致。                |
| `StatusBadCertificateUriInvalid`             | 证书 SAN 中的 URI 与客户端 Application URI 不匹配。                | 重新生成证书，确保 SAN 包含 `URI:urn:taosx-opc:client`。                                   |
| 连接超时                                     | 网络不通或 Ignition 未监听在正确的地址。                           | 确认 Bind Address 为 `0.0.0.0`，Endpoint Addresses 包含服务器 IP，防火墙放通端口 `62541`。 |

## 参考文档

- [Ignition OPC UA server Demo](ignition-opcua-server-demo.md)
