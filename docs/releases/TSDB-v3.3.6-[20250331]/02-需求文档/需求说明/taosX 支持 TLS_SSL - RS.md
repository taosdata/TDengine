# taosX 支持 TLS/SSL - RS

## 1. 引言

### 1.1 术语与缩写名词

- TLS：TLS（传输层安全，Transport Layer Security）是一种用于在计算机网络上提供安全通信的协议。它的前身是 SSL（安全套接字层，Secure Sockets Layer）。TLS 和 SSL 是应用层协议（如 HTTP、SMTP、FTP 等）的基础，确保数据在客户端和服务器之间传输时的机密性和完整性。

### 1.2 相关文档资料

JIRA: 
TS-6016

### 1.3 优先级要求

中

### 1.4 版本要求

企业版支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/03/19 | 0.1 | 霍琳贺 | 新建 |

## 3. 需求目标

1. 指定 TLS 服务端证书，可在 taosX 和 Agent 间启用 HTTPS 支持。

## 4. 具体说明

taosX、Explorer 、Agent 之间的连接可根据需要启用 HTTPS。
1. taosX 与 Explorer：使用 REST API 连接，需支持启用 HTTPS。
2. taosX 与 Agent：之间可能穿过外网，安全性要求更高，需支持启用 HTTPS 连接。

## 5. 性能需求

无。

## 6. 其他需求和说明

无。
