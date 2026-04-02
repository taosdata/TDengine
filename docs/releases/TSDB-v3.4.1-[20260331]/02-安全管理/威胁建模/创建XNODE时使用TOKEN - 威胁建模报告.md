# 创建XNODE时使用TOKEN - 威胁建模报告

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-31 | 2026-01-31 | 1.0 | 霍琳贺 | 初稿 |

### 2. 基本信息

| 项目信息 | 内容 |
| --- | --- |
| 报告编号 | `TM-TSDB-XNTKN-001` |
| 需求名称 | 创建XNODE时使用当前用户或指定用户的TOKEN |
| 设计文档链接 | docs/releases/TSDB-v3.4.1-[20260331]/05-设计文档/创建 XNODE 时使用当前用户或指定用户的 TOKEN-FS.md |
| 版本编号 | 3.4.1 |
| 业务负责人 | 张贵川 |
| 发起人 | 张贵川 |
| 安全负责人 | 霍琳贺 |
| 参会人员 | 霍琳贺、关胜亮、肖波、张心治、张贵川 |
| 报告日期 | 2026-01-31 |
| 总体评价 | 通过 |

### 3. 分析报告

本报告针对「创建XNODE时使用当前用户或指定用户的TOKEN」功能进行了威胁建模分析。该功能旨在增强 XNODE 的认证方式，支持以 Token 替代用户名/密码进行 xnoded→taosd 连接认证，并在未指定认证信息时自动创建默认 Token；同时新增 ALTER XNODE 语法支持动态修改认证凭证。

- **核心发现：** 共识别出 `6` 个潜在威胁，其中 `高危` `0` 个，`中危` `3` 个，`低危` `3` 个。
- **主要风险场景：**
  - **环境变量信息泄露：** Token 通过环境变量 `XNODED_TOKEN` 传递给 xnoded 子进程，在同宿主机上具有 `/proc` 读取权限的进程可能读取该环境变量。
  - **默认 Token 可见性：** 自动创建的默认 Token 可通过 `SHOW TOKENS` 查看，该命令的访问权限决定了 Token 的潜在暴露面。
  - **ALTER XNODE 权限未显式约束：** FS 未明确说明执行 CREATE/ALTER XNODE 所需的权限等级，依赖 TDengine SQL 层通用权限控制。
- **结论：** 该功能设计在提升 XNODE 部署自动化和认证灵活性方面价值显著，整体安全风险较低。识别出的威胁均为中低风险，核心安全保障（Token 不可打印显示、加密创建、凭证轮换机制）在 FS 中均有明确设计，可正常发布。

### 4. 威胁识别与分析（STRIDE）

本功能扩展了 XNODE 管理 SQL 语法：`CREATE XNODE` 新增 `TOKEN 'token'` 参数；`ALTER XNODE SET TOKEN/USER PASS` 支持动态修改凭证；未指定认证信息时 mnode 自动调用 `mndStoreXnodeUserPassToken()` 创建默认 Token（账户名 `xnode`）；认证信息通过 `SMCreateXnodeReq`/`SMUpdateXnodeReq` 结构体在节点间序列化传输；xnoded 启动时通过环境变量 `XNODED_TOKEN` 或 `XNODED_USER_PASS` 接收凭证。

#### 4.1 关键实体与数据

| 实体/数据 | 描述 | 敏感性 |
| --- | --- | --- |
| Token 参数值 | CREATE/ALTER XNODE 语句中传入的 Token 字符串 | 高（连接凭证） |
| 默认 Token | 未指定认证信息时 mnode 自动创建、账户名为 `xnode` 的 Token | 高（服务凭证） |
| XNODED_TOKEN 环境变量 | mnode 启动 xnoded 时传递 Token 的进程间通信参数 | 高（凭证明文传递路径） |
| SXnodeUserPassObj 结构体 | 存储 xnoded 凭证（user/pass/token）的内存结构，含读写锁 | 高（驻留内存的凭证） |
| SMCreateXnodeReq / SMUpdateXnodeReq | 在 taosd 节点间序列化传输的 XNODE 创建/更新请求结构体 | 中（含凭证字段） |
| SHOW TOKENS 输出 | 展示系统中所有 Token 的 SQL 命令结果，包含默认 `xnode` Token | 中（凭证可见性入口） |

#### 4.2 威胁评估

| 威胁ID | 威胁描述/攻击场景 | STRIDE | 相关组件/数据流 | 风险等级 |
| --- | --- | --- | --- | --- |
| T-XNTKN-01 | **Token 通过环境变量传递的信息泄露：** mnode 启动 xnoded 时，通过环境变量 `XNODED_TOKEN` 传递 Token 明文。在 Linux 系统上，宿主机上具有 `/proc/<pid>/environ` 读取权限的本地用户或进程（如系统管理员、容器逃逸场景）可能读取该环境变量，从而获取 xnoded 的连接凭证，进而冒充 xnoded 访问 taosd。 | I（信息泄露） | XNODED_TOKEN 环境变量 → /proc 文件系统 | 中 |
| T-XNTKN-02 | **默认 Token 通过 SHOW TOKENS 暴露：** 未指定认证信息时自动创建的默认 Token 可通过 `SHOW TOKENS` 命令查看。若 `SHOW TOKENS` 对普通用户开放（TDengine 权限策略决定），则任意具有连接权限的用户均可获取默认 xnode Token，进而在外部模拟 xnoded 的身份连接 taosd。 | I（信息泄露） | 默认 Token → SHOW TOKENS 命令 | 中 |
| T-XNTKN-03 | **CREATE/ALTER XNODE 权限未显式约束：** FS 未在功能描述中明确 CREATE XNODE 和 ALTER XNODE 所需的最低权限等级（如仅超级管理员可执行）。若 TDengine SQL 层未对这两条语句实施严格权限检查，普通账户可能通过 ALTER XNODE 替换 xnoded 的认证凭证，导致 xnoded 使用恶意凭证重连，进而影响整个 XNODE 节点的可用性或安全性。 | E（权限提升） | ALTER XNODE → mndProcessUpdateXnodeReq | 中 |
| T-XNTKN-04 | **ALTER XNODE 触发 xnoded 重启的 DoS：** 每次执行 ALTER XNODE 修改认证信息均会触发 xnoded 重启，期间该节点约 2s 不可用（FS 5.2 节）。具有 ALTER XNODE 执行权限的用户若持续反复执行该命令，可导致目标 XNODE 节点持续处于重启状态，影响该节点上的数据同步任务。 | D（拒绝服务） | ALTER XNODE → mndRestartXnoded → xnoded 重启循环 | 低 |
| T-XNTKN-05 | **Token 格式约束枚举：** 错误消息明确区分了"Token 为空"和"Token 长度非法"两类错误（FS 4.5.1、4.5.2），攻击者可通过探测不同长度的输入，反向推断系统要求的有效 Token 长度范围，辅助构造符合格式的伪造 Token 进行暴力猜测。 | I（信息泄露） | 错误处理 → 客户端错误消息 | 低 |
| T-XNTKN-06 | **凭证变更过渡期的短暂窗口：** ALTER XNODE 修改认证信息后，xnoded 重启（约 2s）期间存在凭证过渡时间窗口。若旧 Token 未被同步吊销而新 xnoded 实例尚未建立连接，理论上存在短暂的双凭证并存状态，可能被用于在凭证轮换期间尝试重放旧 Token。 | S（仿冒） | 旧 Token → ALTER XNODE 过渡期 → 新 Token 生效 | 低 |

### 5. 安全需求与设计约束

| 威胁ID | 转化后的安全需求/设计约束 | 类型 | 优先级 |
| --- | --- | --- | --- |
| T-XNTKN-01 | SEC-XNTKN-001（环境变量传递说明）：在运维文档中说明 `XNODED_TOKEN` 环境变量的安全使用建议，包括：限制宿主机上对 `/proc/<xnoded-pid>/environ` 的访问权限；在容器化部署场景下，使用 Kubernetes Secret 等安全机制管理环境变量；避免在不受信任的共享主机上部署 XNODE。 | 文档/运维规范需求 | 中 |
| T-XNTKN-02 | SEC-XNTKN-002（SHOW TOKENS 权限说明）：在文档中明确 `SHOW TOKENS` 命令所需的权限等级，并建议生产环境对该命令实施严格的访问控制，避免普通用户查看系统级服务 Token。 | 文档/设计约束 | 中 |
| T-XNTKN-03 | SEC-XNTKN-003（CREATE/ALTER XNODE 权限要求）：确认并在文档中明确 CREATE XNODE 和 ALTER XNODE 要求超级管理员（sysinfo 或 superuser）权限执行，非特权用户执行时应返回权限拒绝错误。 | 设计约束 | 中 |

### 6. 后续行动与验证

| 行动项 | 描述 | 责任方 | 完成标准 |
| --- | --- | --- | --- |
| 1. 补充运维文档 | 在官网运维手册中增加 XNODED_TOKEN 环境变量安全使用说明（对应 SEC-XNTKN-001）及 SHOW TOKENS 权限控制建议（对应 SEC-XNTKN-002）。 | 张贵川 | 文档发布并经安全代表确认。 |
| 2. 确认 ALTER XNODE 权限控制 | 确认 CREATE XNODE 和 ALTER XNODE 语句的 SQL 层权限检查实现，确保仅超级管理员可执行（对应 SEC-XNTKN-003）。 | 张贵川 | 技术确认完成，必要时补充测试用例。 |

### 7. 审批意见

| 角色 | 意见 | 签字 | 日期 |
| --- | --- | --- | --- |
| 产品负责人 | 确认理解安全需求的重要性，并将其纳入开发优先级。 | | 2026-01-31 |
| 技术负责人 | 确认上述安全约束在技术上是可行且必要的，将在架构设计中落实。 | | 2026-01-31 |
| 安全负责人 | 确认威胁分析全面，安全需求是保障该功能顺利上线的关键。 | | 2026-01-31 |
