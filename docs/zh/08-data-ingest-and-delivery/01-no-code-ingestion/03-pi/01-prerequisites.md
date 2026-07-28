---
title: PI 数据接入前置条件
sidebar_label: 前置条件
---

本页列出从 PI 系统接入数据到 TDengine 之前需要确认的所有前置条件，包括网络连通性、端口与协议、认证与权限、软件依赖等。建议在创建 PI 数据接入任务之前，由 PI 管理员和网络管理员共同完成检查。

## 1. PI 系统连接要求

### 1.1 PI Data Archive

| 项目       | 说明                                      |
| ---------- | ----------------------------------------- |
| 服务器地址 | PI Data Archive Server 的主机名或 IP 地址 |
| 默认端口   | **5450**（PI Data Archive 标准端口）      |
| 协议       | PI SDK / PI AF SDK 私有协议               |

### 1.2 PI AF Server

使用 AF 模式（PI Data Archive + AF Server）时，还需要：

| 项目           | 说明                              |
| -------------- | --------------------------------- |
| AF Server 地址 | PI AF Server 的主机名             |
| AF 数据库名称  | 要连接的 AF Database 名称         |
| 默认端口       | **5457**（PI AF Server 标准端口） |
| 协议           | PI AF SDK 私有协议                |

:::note
以上端口为 PI 系统的默认值。PI 连接器（taosx-pi.exe）通过 PI AF SDK 与 PI 系统通信，端口由 SDK 内部管理，无需在连接器中手动配置。但防火墙必须放行这些端口，否则 SDK 连接会失败。如果你的 PI 系统使用了非标准端口，请与 PI 管理员确认实际端口号。
:::

## 2. 网络与防火墙要求

taosX（或 taosx-agent）所在主机必须能够访问 PI 系统的以下端口：

| 源                                  | 目标                   | 端口           | 协议               | 说明                      |
| ----------------------------------- | ---------------------- | -------------- | ------------------ | ------------------------- |
| PI 连接器（taosX/taosx-agent 主机） | PI Data Archive Server | 5450/TCP       | PI AF SDK 私有协议 | 必须，读取 PI Point 数据  |
| PI 连接器（taosX/taosx-agent 主机） | PI AF Server           | 5457/TCP       | PI AF SDK 私有协议 | 使用 AF 模式时必须        |
| taosx-agent                         | taosX                  | taosX 配置端口 | gRPC               | 使用 Agent 代理模式时必须 |

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

PI 连接器（taosx-pi.exe）作为 taosX 或 taosx-agent 的子进程运行，通过 PI AF SDK 连接 PI 系统。**认证由 Windows 操作系统完成**（Kerberos 或 NTLM 集成认证），不依赖 PI 内部账号密码体系。

### 3.1 认证模式说明

连接配置中的 Username/Password/Domain 三个字段（可选）控制认证方式：

| 字段填写方式                         | 认证方式             | 说明                                                                    |
| ------------------------------------ | -------------------- | ----------------------------------------------------------------------- |
| 全部留空（推荐）                     | **Windows 集成认证** | 连接器以 taosx-agent 服务账户的 Windows 身份（Kerberos 或 NTLM）访问 PI |
| 填写 Username + Password（± Domain） | **显式凭据登录**     | 以指定的 Windows 账户访问 PI，忽略服务账户身份                          |

:::tip
**默认情况下，不需要填写 Username、Password、Domain**。留空时走 Windows 集成认证，连接器以 taosx-agent 服务账户的 Windows 身份访问 PI，这是生产环境的推荐方式。详细说明请参阅 [连接配置与认证](./06-connection-config.md)。
:::

taosx-agent 服务默认以 Windows **Local System** 账户运行，在域环境中对应机器账号（如 `DOMAIN\计算机名$`）。建议在生产环境中将 taosx-agent 配置为运行在专用域服务账号下，以便在 PI 侧统一管理权限。

### 3.2 PI Data Archive 权限

运行连接器的服务账户需要对 PI Data Archive 具有以下权限：

- 读取 PI Point 数据的权限（通过 PI Identity 或 PI Mapping）
- 读取 PI Point 属性（Point Attributes）的权限

由 PI 管理员在 PI System Management Tools（SMT）中，将该 Windows 账户的 SID 通过 **Mappings** 配置到具有读取权限的 **PI Identity**。

### 3.3 PI AF Server 权限

使用 AF 模式时，服务账户还需要：

- 对目标 AF 数据库的**读取权限**
- 对 AF 元素（Element）及其属性（Attribute）的读取权限

在 PI System Explorer（PSE）中，将该 Windows 账户添加到 AF 数据库的访问控制，授予 Read 和 Read Data 权限。

### 3.4 服务账户建议

| 建议               | 说明                                                          |
| ------------------ | ------------------------------------------------------------- |
| 创建专用域服务账户 | 避免使用 Local System（机器账号）、个人账户或高权限管理员账户 |
| 最小权限原则       | 仅授予 PI 数据的读取权限，无需写入权限                        |
| 域账户             | PI 系统使用域认证时，服务账户应为域账户，以支持 Kerberos      |
| 密码策略           | 建议设置为密码不过期，或配合密码轮换策略                      |

## 4. 软件依赖

PI 连接器依赖 PI AF SDK（PI AF Client），必须安装在运行 taosX 或 taosx-agent 的主机上。

| 依赖项                    | 最低版本                           | 说明                              |
| ------------------------- | ---------------------------------- | --------------------------------- |
| 操作系统                  | Windows Server 2016+ / Windows 10+ | PI AF SDK 仅支持 Windows          |
| PI AF SDK（PI AF Client） | 2018+                              | 从 OSIsoft / AVEVA 官方获取安装包 |
| .NET Framework            | 4.8+                               | PI AF SDK 的运行时依赖            |

:::warning
PI AF SDK **仅支持 Windows**。如果 taosX 部署在 Linux 环境，则必须通过 taosx-agent（部署在 Windows 主机上）代理连接 PI 系统。
:::

## 5. 验证清单

在创建 PI 数据接入任务之前，请逐项确认以下检查项：

### 网络与连通性

- [ ] PI Data Archive Server **主机名**已确认（推荐使用主机名而非 IP，使用 IP 可能影响 Kerberos 认证）
- [ ] PI AF Server 主机名已确认（如使用 AF 模式）
- [ ] AF 数据库名称已确认（如使用 AF 模式）
- [ ] 从 taosX/agent 主机可以访问 PI Data Archive（端口 5450）
- [ ] 从 taosX/agent 主机可以访问 PI AF Server（端口 5457，如使用 AF 模式）
- [ ] 在 taosx-agent 主机上，已通过 PI System Management Tools (SMT) 或 PI System Explorer (PSE) **手动验证**可连接 PI（先排除环境问题，再测试 taosX）
- [ ] 如使用 Agent 代理模式，taosx-agent 服务已启动，taosX ↔ agent 网络已连通，taosExplorer 中 agent 状态显示在线

### 认证与权限

- [ ] 已明确 taosx-agent 的运行身份（默认：Local System → 域中对应机器账号 `DOMAIN\计算机名$`；或自定义域服务账号）
- [ ] 该 Windows 身份已在 PI Data Archive 的 **Mappings** 中映射到对应 **PI Identity**（读取权限）
- [ ] AF 模式：该身份在 AF Server Security 中已获得 Read / Read Data 权限
- [ ] 连接配置中 Username/Password/Domain **优先留空**（使用 Windows 集成认证）

### 软件环境

- [ ] taosX/agent 主机操作系统为 Windows
- [ ] PI AF SDK（PI AF Client）已安装
- [ ] .NET Framework 4.8+ 已安装

### TDengine 侧

- [ ] TDengine 集群已部署并正常运行
- [ ] 目标数据库已创建（或准备在 taosExplorer 中创建）
- [ ] taosX 已安装并可通过 taosExplorer 访问

### 任务配置

- [ ] 连接配置中 PI Data Archive Server 使用主机名格式
- [ ] 点击 **连通性检查** 按钮，连接测试通过
