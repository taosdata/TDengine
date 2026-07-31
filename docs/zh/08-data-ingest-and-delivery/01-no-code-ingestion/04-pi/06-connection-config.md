---
title: PI 连接配置与认证
sidebar_label: 连接配置与认证
---

本页详细说明 PI 数据接入任务中"连接信息"部分各字段的含义，重点解释 Windows 认证机制、taosx-agent 的访问身份，以及如何在 PI 侧配置相应权限。

## 1. 连接信息字段说明

在 taosExplorer 中创建 PI 或 PI backfill 任务时，"连接信息"包含以下字段：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| PI 服务名 | 是 | PI Data Archive Server 的主机名。**推荐使用主机名而非 IP 地址**（见第 2 节） |
| PI 系统 (AF Server) 名称 | AF 模式必填 | PI AF Server 的主机名 |
| AF 数据库名 | AF 模式必填 | 要连接的 AF 数据库名称 |
| 用户名（Username） | 否 | Windows 账户用户名；**留空则使用集成认证（推荐）** |
| 密码（Password） | 否 | Windows 账户密码；仅在填写用户名时需要 |
| 域（Domain） | 否 | Windows 域名；仅在填写用户名时需要 |

## 2. 为什么推荐使用主机名而非 IP

PI Data Archive Server 地址应使用**主机名**，原因如下：

- **Kerberos 认证依赖 SPN**：Kerberos 票据绑定到目标服务主体名称（SPN），SPN 基于主机名注册。使用 IP 地址连接时，Windows 无法匹配 SPN，会自动降级为 NTLM 认证。
- **NTLM 可能受限**：部分 PI 服务器的安全策略要求使用 Kerberos，禁用 NTLM 或对 NTLM 有额外限制。
- **日志与审计更清晰**：PI 连接日志中显示主机名便于问题排查。

## 3. Windows 认证机制

### 3.1 认证由 Windows 完成，不是 PI 内部账号

PI 连接器（taosx-pi.exe）通过 PI AF SDK 连接 PI 系统，**认证由 Windows 操作系统完成**（使用 Kerberos 或 NTLM 集成认证协议）。PI AF SDK 本身不参与认证判断。

这意味着：

- 连接器**不使用 PI 内部账号密码**——PI 有独立的用户管理，但此处配置的凭据是 **Windows 账户**，不是 PI 账户。
- 认证通过与否，取决于 taosx-agent 主机上的 Windows 身份是否被 PI 系统信任，以及 PI 侧是否配置了对应的 Mapping（见第 4 节）。

### 3.2 连接器以哪个 Windows 身份访问 PI

PI 连接器以 **taosx-agent 服务的 Windows 运行身份**发起连接。taosx-agent 默认以 **Local System** 账户运行。

| taosx-agent 运行身份 | 连接器呈现给 PI 的 Windows 身份 |
| --- | --- |
| Local System（默认） | 域环境：机器账号 `DOMAIN\计算机名$`；工作组：本机系统账号（Kerberos 不可用） |
| 域服务账号（推荐） | 该域服务账号，如 `DOMAIN\svc-taosx` |
| 本地账号 | `计算机名\账号名` |

可在 Windows"服务"管理器中查看和修改 taosx-agent 的运行身份：**服务 → taosx-agent → 属性 → 登录**。

### 3.3 集成认证与显式凭据

连接配置中 Username/Password/Domain 三字段控制认证方式：

| 字段填写方式 | 认证方式 | 说明 |
| --- | --- | --- |
| **全部留空（推荐）** | Windows 集成认证 | 连接器以 taosx-agent 服务账户的 Windows 身份（Kerberos 或 NTLM）访问 PI |
| 填写 Username + Password（± Domain） | 显式凭据登录 | 以指定的 Windows 账户访问 PI，忽略服务账户身份 |

:::tip
**默认情况下，留空即可，不需要填写 Username、Password、Domain。**

在大多数企业域环境中，taosx-agent 的服务账户已通过 Windows 集成认证与 PI 系统建立信任关系，只需在 PI 侧为其 Windows 身份配置好 Mapping（见第 4 节），连接即可成功。
:::

:::warning
若使用显式凭据（填写用户名密码），密码会以加密形式存储在 taosX 的任务配置中。生产环境优先使用集成认证（留空），仅在服务账户无法访问 PI 且需要临时使用特定账户时才考虑显式凭据。
:::

## 4. PI 侧权限配置

PI 采用**两层访问控制**：**认证层（Mapping）** 决定"是谁"，**授权层（ACL/Security）** 决定"能干什么"。两层缺一不可。

### 4.1 第一步：确认连接器的 Windows 身份

在 PI Data Archive 或 AF Server 的管理工具（SMT 或 PSE）的 **Connections** 标签页，找到来自 taosx-agent 主机的连接，记录其 Username。

如果尚未建立过连接，可以先用以下方式推断：

- taosx-agent 默认以 Local System 运行 → 域环境中为 `DOMAIN\计算机名$`
- 查看 Windows 服务管理器中 taosx-agent 的"登录"标签页，即为实际运行身份

### 4.2 PI Data Archive 权限配置

#### 第一层：认证（Mapping）

在 **PI System Management Tools (SMT)** 中：**Security → Mappings**

新增一条 Mapping，将该 Windows 身份（Windows Identity / SID）映射到一个具有读取权限的 **PI Identity**。

#### 第二层：授权（ACL）

在 SMT → **Security → Identities** 中，为该 PI Identity 配置相应的 PI Point 读取权限（Read、Read Data）。

### 4.3 PI AF Server 权限配置（AF 模式）

在 **PI System Explorer (PSE)** 中：

1. 打开目标 AF 数据库的 **Database Security** 或 **Server Security**
2. 将该 Windows 账户添加为 AF Identity（或映射到已有 AF Identity）
3. 授予目标 AF Database 及 Element/Attribute 的 **Read** 和 **Read Data** 权限

### 4.4 权限配置速查

| 需求 | 操作位置 |
| --- | --- |
| 允许连接器读取所有 Point | SMT → Mappings（新增）+ Identities（授 Read/Read Data 权限） |
| 限制只能读取部分 Point | 使用更细粒度的点位级 ACL，或创建专用 PI Identity |
| 只读，禁止写回 PI | 对应 PI Identity 去掉 Write/Write Data 权限 |
| AF 模式读取 Element/Attribute | PSE → Database Security → 授 Read + Read Data |
| 禁止该 agent 访问 PI | 删除对应 Mapping（会导致连接时认证失败） |

:::note
原则：**Mapping 管"身份识别"，ACL 管"权限"**。如需收紧权限，修改 ACL；不要通过删除 Mapping 来限制权限（删 Mapping 会导致认证失败，无法区分"认证失败"和"无权限"）。
:::

## 5. 验证连接

完成认证与权限配置后：

1. 在 taosExplorer 的 PI 任务创建页面，填写连接信息（**Username/Password/Domain 留空**）
2. 点击 **连通性检查** 按钮
3. 检查通过即表示连接器可以正常访问 PI

## 6. 连接问题排查

### 连通性检查失败的排查顺序

1. **网络连通性**：在 taosx-agent 主机上执行：

   ```powershell
   Test-NetConnection -ComputerName <PI_SERVER_HOST> -Port 5450
   Test-NetConnection -ComputerName <AF_SERVER_HOST> -Port 5457  # AF 模式
   ```

2. **先用 PI 工具手工验证**：在 agent 主机上打开 PI System Management Tools (SMT) 或 PI System Explorer (PSE)，直接连接 PI。如果手工连接也失败，说明是环境问题（网络/权限/SDK），而非 taosX 配置问题。

3. **认证问题**：
   - 确认 taosx-agent 的运行身份（Windows 服务管理器 → taosx-agent → 登录）
   - 确认该 Windows 身份在 PI Data Archive 的 Mappings 中有对应配置
   - 如果使用了显式凭据，确认用户名、密码、域名填写正确

4. **PI AF SDK 未安装**：确认 taosx-agent 主机已安装 PI AF SDK（PI AF Client 2018+）

:::note
当前版本的 PI 连接器在连接失败时可能只返回通用错误信息，不详细指明失败原因（如认证失败的具体类型）。可将连接器日志级别调整为 `debug` 获取更详细的信息。
:::
