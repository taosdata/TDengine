---
sidebar_label: 安全指南
title: 安全指南
description: TDengine 安全能力概述：认证授权、传输加密、数据安全、审计、部署加固与安全公告
---

TDengine 提供面向生产环境的多层安全能力，覆盖身份认证与授权、传输加密、访问控制、静态数据保护、操作审计，以及各组件的部署加固建议。企业版在用户权限（含 RBAC / 三权分立）、IP 白名单、审计、透明加密与 Token 等方面提供完整能力；社区版仅提供基础能力，具体以各功能说明为准。

本章按能力分层组织：

- [认证与授权](./01-user.md)：用户、权限与 RBAC 的安全侧概述，语法与权限矩阵以 SQL 手册为准。
- [传输安全](./02-transport-security.md)：服务端 SSL/TLS 证书与 taosAdapter 配置。
- [数据安全](./03-data-security.md)：IP 白名单、安全删除（`SECURE_DELETE`）与透明数据加密（TDE）。
- [连接器安全](./04-connector-security.md)：客户端 SSL/TLS、Token 认证与动态轮换实践。
- [审计与合规](./05-audit-and-compliance.md)：审计日志配置、查看方式，以及与安全公告的关系。
- [安全部署配置建议](./06-security-suggestions.md)：各组件暴露面、加固与网关部署建议。
- [安全公告](./07-security-advisories.md)：已知漏洞、受影响版本与修复版本。

配置与加固见前几节；漏洞披露与修复版本见 [安全公告](./07-security-advisories.md)。
