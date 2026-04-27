---
title: TDengine SSL 配置指南
sidebar_label: SSL 配置指南
---

# TDengine SSL 配置指南

本指南专注于 TDengine 服务端的 SSL/TLS 基础设施配置，包括证书生成与 taosAdapter 服务端配置。

:::note 应用层安全
如需了解客户端 SSL/TLS（TrustStore、`wss/useSSL`、REST HTTPS）、Token 认证、动态轮换、连接池管理等应用层安全实践，请参考 [连接器安全最佳实践](./connector-security-best-practices.md)。
:::

## 1. 生成自签名证书

### 1.1 生成私钥

```bash
# 生成 RSA 2048 位私钥
openssl genrsa -out server.key 2048
```

### 1.2 生成证书签名请求 (CSR)

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

- **Common Name (CN)**：必须填入客户端连接时使用的服务器 IP 地址或域名
- **Subject Alternative Name (SAN)**：必须包含客户端实际连接使用的域名/IP（现代 TLS 客户端通常优先校验 SAN）
- 如果客户端使用 `192.168.1.100` 连接，CN 应填 `192.168.1.100`
- 如果客户端使用 `tdserver.example.com` 连接，CN 应填 `tdserver.example.com`
:::

### 1.3 生成自签名证书（有效期 365 天）

```bash
# 推荐：显式添加 SAN（将示例域名/IP替换为你的实际连接地址）
cat > san.ext <<'EOF'
subjectAltName=DNS:tdserver.example.com,IP:192.168.1.100
EOF

openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt -extfile san.ext
```

### 1.4 将证书和密钥复制到 TDengine 配置目录

```bash
# 假设 TDengine 配置目录为 /etc/taos
sudo cp server.crt /etc/taos/
sudo cp server.key /etc/taos/
sudo chown taos:taos /etc/taos/server.crt /etc/taos/server.key
sudo chmod 600 /etc/taos/server.key
```

---

## 2. 手动配置 TDengine 服务端

:::info WebSocket SSL 配置
本文档适用于 WebSocket 连接，SSL 配置在 **taosAdapter** 服务上进行。
:::

### 2.1 编辑 taosadapter.toml

```bash
sudo vi /etc/taos/taosadapter.toml
```

### 2.2 在配置文件中启用 SSL

```toml
[ssl]
# Enable SSL. Applicable for the Enterprise Edition.
enable   = true
# 证书文件路径（根据实际位置修改）
certFile = "/path/to/your/server.crt"    # 示例：/etc/taos/server.crt
keyFile  = "/path/to/your/server.key"     # 示例：/etc/taos/server.key
```

:::tip 路径说明

- 如果您在 1.4 步中将证书复制到了 `/etc/taos/`，则使用上述示例路径
- 如果您使用了其他路径，请相应修改 `certFile` 和 `keyFile` 的值
- 确保证书和私钥文件具有正确的权限（私钥文件应为 600）
:::

### 2.3 重启 taosAdapter 服务

```bash
sudo systemctl restart taosadapter

# 验证服务是否正常启动
sudo systemctl status taosadapter
```

### 2.4 查看日志确认 SSL 已启用

```bash
journalctl -u taosadapter -n 50

# 应该看到类似的日志：
# SSL is enabled
```

---

## 3. 配置客户端

客户端 SSL/TLS（TrustStore、`wss/useSSL`、REST HTTPS）已统一收敛到 [连接器安全最佳实践](./connector-security-best-practices.md#2-ssltls-配置)。

如需 Token 认证、动态轮换、连接池安全实践，也请参考该文档。
