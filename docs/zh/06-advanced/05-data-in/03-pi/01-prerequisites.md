---
title: "PI 数据接入前置条件"
sidebar_label: "前置条件"
---

本页列出从 PI 系统接入数据到 TDengine 之前需要确认的所有前置条件，包括网络连通性、端口与协议、认证与权限、软件依赖等。建议在创建 PI 数据接入任务之前，由 PI 管理员和网络管理员共同完成检查。

## 1. PI 系统连接要求

### 1.1 PI Data Archive

| 项目 | 说明 |
| --- | --- |
| 服务器地址 | PI Data Archive Server 的主机名或 IP 地址 |
| 默认端口 | **5450**（PI Data Archive 标准端口） |
| 协议 | PI SDK / PI AF SDK 私有协议 |

### 1.2 PI AF Server

使用 AF 模式（PI Data Archive + AF Server）时，还需要：

| 项目 | 说明 |
| --- | --- |
| AF Server 地址 | PI AF Server 的主机名 |
| AF 数据库名称 | 要连接的 AF Database 名称 |
| 默认端口 | **5457**（PI AF Server，通过 SQL Server） |
| 协议 | PI AF SDK，底层使用 SQL Server 连接 |

:::note
以上端口为 PI 系统的默认值。PI 连接器（taosx-pi.exe）通过 PI AF SDK 与 PI 系统通信，端口由 SDK 内部管理，无需在连接器中手动配置。但防火墙必须放行这些端口，否则 SDK 连接会失败。如果你的 PI 系统使用了非标准端口，请与 PI 管理员确认实际端口号。
:::

## 2. 网络与防火墙要求

taosX（或 taosx-agent）所在主机必须能够访问 PI 系统的以下端口：

| 源 | 目标 | 端口 | 协议 | 说明 |
| --- | --- | --- | --- | --- |
| taosX / taosx-agent | PI Data Archive Server | 5450/TCP | PI SDK | 必须，读取 PI Point 数据 |
| taosX / taosx-agent | PI AF Server | 5457/TCP | SQL Server (TDS) | 使用 AF 模式时必须 |
| taosx-agent | taosX | taosX 配置端口 | HTTPS/gRPC | 使用 Agent 代理模式时必须 |

如果你的网络环境中存在防火墙或网络隔离，请确保以上端口已放通。

:::tip
**如何验证端口连通性：**
在 taosX / agent 所在主机上，可以使用以下命令快速验证：

```powershell
# Windows PowerShell
Test-NetConnection -ComputerName <PI_SERVER_HOST> -Port 5450
Test-NetConnection -ComputerName <AF_SERVER_HOST> -Port 5457
```

```bash
# Linux（仅验证 taosX ↔ agent 连通性时使用）
nc -zv <HOST> <PORT>
```

:::

## 3. 认证与服务账户要求

PI 连接器使用 PI AF SDK 连接 PI 系统，认证基于运行 taosX（或 taosx-agent）进程的 **Windows 服务账户**。

### 3.1 PI Data Archive 权限

运行连接器的服务账户需要对 PI Data Archive 具有以下权限：

- 读取 PI Point 数据的权限（PI Identity 或 PI Mapping）
- 读取 PI Point 属性（Point Attributes）的权限

建议由 PI 管理员在 PI Data Archive 中为服务账户创建专用的 PI Mapping。

### 3.2 PI AF Server 权限

使用 AF 模式时，服务账户还需要：

- 对目标 AF 数据库的**读取权限**
- 对 AF 元素（Element）及其属性（Attribute）的读取权限

### 3.3 服务账户建议

| 建议 | 说明 |
| --- | --- |
| 创建专用服务账户 | 避免使用个人账户或高权限管理员账户 |
| 最小权限原则 | 仅授予 PI 数据的读取权限，无需写入权限 |
| 域账户 | 如果 PI 系统使用 Windows 域认证，服务账户应为域账户 |
| 密码策略 | 建议设置为密码不过期，或配合密码轮换策略 |

## 4. 软件依赖

PI 连接器依赖 PI AF SDK（PI AF Client），必须安装在运行 taosX 或 taosx-agent 的主机上。

| 依赖项 | 最低版本 | 说明 |
| --- | --- | --- |
| 操作系统 | Windows Server 2016+ / Windows 10+ | PI AF SDK 仅支持 Windows |
| PI AF SDK（PI AF Client） | 2018+ | 从 OSIsoft / AVEVA 官方获取安装包 |
| .NET Framework | 4.8+ | PI AF SDK 的运行时依赖 |

:::warning
PI AF SDK **仅支持 Windows**。如果 taosX 部署在 Linux 环境，则必须通过 taosx-agent（部署在 Windows 主机上）代理连接 PI 系统。
:::

## 5. 验证清单

在创建 PI 数据接入任务之前，请逐项确认以下检查项：

### 网络与连通性

- [ ] PI Data Archive Server 主机名/IP 已确认：`_______________`
- [ ] PI AF Server 主机名已确认（如使用 AF 模式）：`_______________`
- [ ] AF 数据库名称已确认（如使用 AF 模式）：`_______________`
- [ ] 从 taosX/agent 主机可以访问 PI Data Archive（端口 5450）
- [ ] 从 taosX/agent 主机可以访问 PI AF Server（端口 5457，如使用 AF 模式）
- [ ] 如使用 Agent 代理模式，taosX ↔ agent 网络已连通

### 认证与权限

- [ ] 已创建专用 Windows 服务账户
- [ ] 服务账户已在 PI Data Archive 中配置 PI Mapping（读取权限）
- [ ] 服务账户已获得 AF 数据库读取权限（如使用 AF 模式）

### 软件环境

- [ ] taosX/agent 主机操作系统为 Windows
- [ ] PI AF SDK（PI AF Client）已安装
- [ ] .NET Framework 4.8+ 已安装

### TDengine 侧

- [ ] TDengine 集群已部署并正常运行
- [ ] 目标数据库已创建（或准备在 Explorer 中创建）
- [ ] taosX 已安装并可通过 Explorer 访问
