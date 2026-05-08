---
title: "GE Cimplicity OPC UA Server 集成指南"
sidebar_label: "GE Cimplicity"
---

本页介绍当数据源是 **GE Cimplicity OPC UA Server** 时，如何在 taosX Explorer 中配置 OPC UA 连接。

OPC UA 安全分为两个相互独立的层级，连接 GE Cimplicity OPC Server 时两者都必须满足：

### 安全通道（Secure Channel）

- 当 Security Mode 为 `SignAndEncrypt` 时**始终必需**。
- 在 taosX Explorer 中通过 **Secure Channel Certificate** + **Certificate's Private Key** 配置。

### 用户认证（Authentication）

- 由 GE OPC UA Server 端实际开启的认证方式决定，taosX Explorer 中 Authentication 标签页的选择必须与之一致。常见三种：
  - **Anonymous**：本指南验证过的典型配置，GE OPC UA Server 默认即支持。
  - **Username**：当服务端启用了用户名/密码认证时使用。
  - **Certificates**：仅当服务端为 OPC UA 用户认证额外配置了用户证书白名单时使用；通常情况下，证书只用于保护安全通道，**不要** 误把安全通道证书直接复用到这里。
- 如果不确定服务端开了哪种用户认证，可以先用`Anonymous`方式试一下。

## 1. 生成 taosX OPC UA 客户端证书

按 [生成 taosX OPC UA 客户端证书](./01-client-certificate.md) 中给出的脚本（推荐使用 Windows cmd.exe 版本），生成 `client_cert.pem` 与 `client_key.pem`，并记录证书的 **SHA1 指纹**——后面在 GE 服务端配置时需要用到：

```bash
openssl x509 -in client_cert.pem -noout -fingerprint -sha1
```

## 2. 在 taosX Explorer 中配置连接

在 taosX Explorer 中进入 **Data In → Create New Data In Task**，选择 **OPC UA**，按下表设置。

### 2.1 连接配置

| Explorer 字段              | 值                                                 |
| -------------------------- | -------------------------------------------------- |
| Server Endpoint            | `<host>:<port>/GeCssOpcUaServer`（按实际部署填写） |
| Security Mode              | `SignAndEncrypt`                                   |
| Security Policy            | `Basic128Rsa15`                                    |
| Secure Channel Certificate | 上传 `client_cert.pem`                             |
| Certificate's Private Key  | 上传 `client_key.pem`                              |

### 2.2 用户认证

按服务端实际开启的方式选择：

| 服务端开启的认证方式          | Explorer 设置                                                                                               |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------- |
| 匿名（默认 / 本指南验证场景） | Authentication 选 `Anonymous`                                                                               |
| 用户名/密码                   | Authentication 选 `Username`，填写服务端创建的用户名密码                                                    |
| OPC UA 用户证书               | Authentication 选 `Certificates`，上传服务端用户证书白名单中对应的证书与私钥（**不必** 与安全通道证书相同） |

:::note
**Authentication → Certificates** 不是用来"再传一次安全通道证书"。除非 GE OPC UA Server 明确开启了 OPC UA 用户证书认证并把某张证书加入了用户白名单，否则保持 **Anonymous** 即可。
:::

## 3. 在 GE OPC UA Server 上信任并映射 taosX 客户端证书

在 taosX Explorer 中第一次执行 **Check Connection** 通常会失败 —— 这是预期行为，因为 GE OPC UA Server 还没有信任、也没有把新的 taosX 客户端证书映射到任何用户。

在 GE OPC UA Server 主机上打开 OPC UA Server 配置，进入：

```text
OPC UA Server -> Security Certificate / User Association
```

添加或选择 taosX 客户端证书的标识，并把它关联到一个有权限的用户，例如：

| 字段                   | 值                                              |
| ---------------------- | ----------------------------------------------- |
| Certificate Identifier | `client_cert.pem` 的 SHA1 指纹（去掉冒号）      |
| Privileges User Name   | 一个有权限的 GE Cimplicity 用户（例如 `admin`） |

把证书与该用户关联完成后，回到 taosX Explorer 再次执行 **Check Connection**。

## 4. 常见错误排查

| 现象                                                 | 可能原因                                                                                    | 处理方式                                                                                                                 |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| 第一次 Check Connection 报 `read header failed: EOF` | GE OPC UA Server 拒绝/关闭了安全通道握手，因为 taosX 客户端证书还未被信任或还未映射到用户。 | 在服务端添加 taosX 证书标识并关联到一个有权限的用户，再重试。                                                            |
| `StatusBadSecurityChecksFailed`                      | Security Mode、Security Policy、证书或私钥错误。                                            | 确认为 `SignAndEncrypt`、`Basic128Rsa15`，并使用正确的 `client_cert.pem` 与 `client_key.pem`。                           |
| `StatusBadCertificateUriInvalid`                     | 证书中没有 taosX OPC UA Application URI。                                                   | 重新生成证书，确认 SAN 中包含 `URI:urn:taosx-opc:client`。                                                               |
| 用户认证相关错误                                     | taosX 的 Authentication 标签页与服务器侧实际开启的认证方式不匹配。                          | 与服务端管理员核对开启的是 Anonymous / Username / OPC UA 用户证书 中的哪一种，再在 Explorer 中切到对应的标签页填写凭据。 |

## 5. 关键要点

- 推荐参考配置为 `SignAndEncrypt + Basic128Rsa15 + Anonymous`；用户认证若服务端配置了其他方式，按实际配置选 Username 或 Certificates。
- 当 Security Mode 为 `SignAndEncrypt` 时，taosX 仍然需要一个安全通道客户端证书与私钥。
- 安全通道证书是在 **Connection Configuration** 区域上传，**不是** 在 **Authentication → Certificates** 标签页。
- GE OPC UA Server 必须把 taosX 证书标识映射到一个有权限的用户。
- 服务端证书关联完成后，重新在 taosX Explorer 中执行 **Check Connection**。
