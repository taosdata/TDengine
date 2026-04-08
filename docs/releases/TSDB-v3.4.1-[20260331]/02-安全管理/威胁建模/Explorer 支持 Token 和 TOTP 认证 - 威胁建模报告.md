# Explorer 支持 Token 和 TOTP 认证 - 威胁建模报告

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-10 | 2025-12-10 | 1.0 | 霍琳贺 | 初始版本 |

### 2. 基本信息

| 项目信息 | 内容 |
| --- | --- |
| 报告编号 | `TM-TSDB-EXPAUTH-001` |
| 需求名称 | Explorer 支持 Token 和 TOTP 认证 |
| 设计文档链接 | [Explorer 支持 Token 和 TOTP 认证 FS](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd) |
| 版本编号 | 3.4.1 |
| 业务负责人 | 杨志宇 |
| 发起人 | 杨志宇 |
| 安全负责人 | 霍琳贺 |
| 参会人员 | 霍琳贺、关胜亮、肖波、张心治、杨志宇 |
| 报告日期 | 2025-12-10 |
| 总体评价 | 通过 |

### 3. 分析报告

本报告针对「Explorer 支持 Token 和 TOTP 认证」功能进行了威胁建模分析。该功能旨在为 taos Explorer Web 管理界面增加 Token 登录和 TOTP 双因素认证支持，提升用户身份验证的安全性和灵活性。

- 核心发现：共识别出 `8` 个潜在威胁，其中 `高危` `2` 个，`中危` `4` 个，`低危` `2` 个。
- 主要风险场景：
   - Token 窃取与滥用：认证 Token 在传输或存储过程中被窃取，攻击者可绕过密码和 TOTP 直接登录。
   - TOTP 暴力破解：6位数字验证码在时间窗口内可能被暴力枚举。
   - 越权 Token 管理：权限不足的用户可能通过 SQL 命令创建或修改其他用户的 Token。
- 结论：该功能设计显著提升了 Explorer 的身份认证安全性，引入了双因素认证和 Token 机制两种增强手段。核心认证逻辑依赖 TSDB 服务端（身份鉴别 FS）的统一实现，Explorer 前端主要负责 UI 交互和流程编排，整体攻击面可控。

### 4. 威胁识别与分析（STRIDE）

Explorer 支持 Token 和 TOTP 认证功能为 Explorer Web 界面新增了两种认证方式：（1）Token 直接登录——用户使用系统签发的 Token 替代用户名密码进行认证；（2）TOTP 二次验证——用户在密码登录后，需额外输入基于时间的一次性验证码。同时提供了 TOTP 管理（启用/禁用/更新 seed）和 Token 管理（创建/修改过期时间/删除）的账号设置功能。

#### 4.1 关键实体与数据

| 实体/数据 | 描述 | 敏感性 |
| --- | --- | --- |
| 认证 Token | 系统为用户签发的认证凭据，具有 expire_time 过期控制，可用于直接登录 Explorer | 高 |
| TOTP Seed | 用户的 TOTP 共享密钥，用于生成基于时间的一次性验证码 | 高 |
| TOTP 验证码 | 基于 TOTP seed 和当前时间生成的6位数字一次性密码 | 中（有效期短） |
| Explorer 会话 | 用户登录后的 Web 会话凭据（Session/Cookie） | 高 |
| 用户凭证 | 用户名和密码，用于密码登录方式 | 高 |
| Token 管理 SQL 接口 | CREATE/ALTER/DROP token 等 SQL 命令接口 | 中 |

#### 4.2 威胁评估

| 威胁ID | **威胁描述/攻击场景** | **STRIDE** | **相关组件/数据流** | **风险等级** |
| --- | --- | --- | --- | --- |
| T-EXPAUTH-01 | Token 窃取冒充登录：攻击者通过网络嗅探（未启用 HTTPS 场景）、浏览器本地存储读取、或应用日志泄露等途径获取用户的认证 Token，使用窃取的 Token 直接登录 Explorer，完全绕过密码和 TOTP 验证。 | S (仿冒) | 认证 Token → Explorer 登录接口 | 高 |
| T-EXPAUTH-02 | 越权创建/管理 Token：非管理员用户通过 SQL 接口尝试执行 `CREATE token FROM USER` 为其他用户创建 Token，或通过 `ALTER token` 修改非本人 Token 的过期时间，从而获取对其他账户的访问能力。 | E (权限提升) | Token 管理 SQL 接口 | 高 |
| T-EXPAUTH-03 | TOTP 验证码暴力猜测：攻击者在已获取用户名密码的前提下，对 TOTP 验证接口进行暴力枚举攻击。TOTP 码为6位数字（100万种可能），在30秒时间窗口内（考虑前后窗口容差可达90秒）尝试穷举。 | S (仿冒) | TOTP 验证码 → 认证流程 | 中 |
| T-EXPAUTH-04 | TOTP Seed 信息泄露：具有数据库查询权限的用户通过 `SHOW USERS` 命令或直接查询系统表，可能获取到其他用户的 TOTP seed 信息，进而生成有效的 TOTP 验证码。 | I (信息泄露) | TOTP Seed → SHOW USERS | 中 |
| T-EXPAUTH-05 | Token 值传输泄露：Token 在 Explorer 登录请求中传输时，可能通过浏览器历史记录（如 GET 参数）、代理服务器访问日志、Referer 请求头等途径被记录和泄露。 | I (信息泄露) | 认证 Token → HTTP 请求 | 中 |
| T-EXPAUTH-06 | 登录接口拒绝服务：攻击者对 Explorer 的密码登录、Token 登录、TOTP 验证等认证接口发起大量无效请求，消耗服务端认证处理资源，导致合法用户无法正常登录。 | D (拒绝服务) | Explorer 登录接口 → TSDB 认证服务 | 中 |
| T-EXPAUTH-07 | Token 操作抵赖：使用 Token 方式登录的用户执行敏感操作后，由于 Token 可能被多人共享或传递，难以明确追溯实际操作者身份，操作者可抵赖其行为。 | R (抵赖) | 认证 Token → Explorer 操作 | 低 |
| T-EXPAUTH-08 | 过期 Token 时间窗口利用：Token 过期检查如果仅在登录时执行而非持续验证，攻击者可在 Token 临近过期时登录，获取的会话可能在 Token 过期后仍然有效，延长了实际可用时间窗口。 | T (篡改) | 认证 Token → Explorer 会话 | 低 |

### 5. 安全需求与设计约束

| 威胁ID | **转化后的安全需求/设计约束** | **类型** | **优先级** |
| --- | --- | --- | --- |
| T-EXPAUTH-01 | SEC-EXPAUTH-001（Token 传输保护）：Token 登录请求必须通过 POST 方法提交（而非 GET），避免 Token 出现在 URL 中。生产环境应强制 HTTPS，确保 Token 在传输层加密。 | 安全设计约束 | 高 |
| T-EXPAUTH-02 | SEC-EXPAUTH-002（Token 操作权限控制）：Token 管理 SQL 命令必须实施严格的权限校验——普通用户仅能管理自己的 Token，管理员可管理所有用户的 Token。服务端需在执行 CREATE/ALTER/DROP token 前验证操作者权限。 | 安全功能需求 | 高 |
| T-EXPAUTH-03 | SEC-EXPAUTH-003（TOTP 暴力破解防护）：TOTP 验证接口应实施速率限制，如连续5次验证失败后临时锁定该账户的 TOTP 验证（锁定5分钟），防止暴力枚举。 | 安全设计约束 | 中 |
| T-EXPAUTH-04 | SEC-EXPAUTH-004（TOTP Seed 访问控制）：`SHOW USERS` 命令不应返回 TOTP seed 明文，仅显示 TOTP 是否启用的状态标识。TOTP seed 在数据库中应以不可逆方式存储或严格限制访问。 | 安全设计约束 | 中 |
| T-EXPAUTH-05 | SEC-EXPAUTH-005（Token 防泄露）：Explorer 前端在处理 Token 时应避免将其写入浏览器 localStorage（优先使用 httpOnly Cookie 或 sessionStorage），登录请求不应在 URL 中携带 Token。 | 安全设计约束 | 中 |
| T-EXPAUTH-06 | SEC-EXPAUTH-006（认证接口限流）：Explorer 认证接口应实施基于 IP 的速率限制，防止暴力攻击导致服务不可用。 | 非功能安全需求 | 中 |

### 6. 后续行动与验证

| 行动项 | 描述 | 责任方 | 完成标准 |
| --- | --- | --- | --- |
| 1. 确认 Token 传输方式 | 确认 Explorer Token 登录接口使用 POST 方法提交 Token，不在 URL 中暴露 Token 值。 | 杨志宇 | Token 仅通过请求体传输。 |
| 2. 确认 Token 权限校验 | 确认 TSDB 服务端对 Token 管理 SQL 命令实施了用户权限校验，普通用户无法操作他人 Token。 | 杨志宇 | 权限校验通过安全测试。 |
| 3. 确认 TOTP seed 不可见 | 确认 `SHOW USERS` 命令不返回 TOTP seed 明文。 | 杨志宇 | SHOW USERS 仅显示 TOTP 启用状态。 |

### 7. 审批意见

| 角色 | 意见 | 签字 | 日期 |
| --- | --- | --- | --- |
| 产品负责人 | 确认理解安全需求的重要性，并将其纳入开发优先级。 |  |  |
| 技术负责人 | 确认上述安全约束在技术上是可行且必要的，将在架构设计中落实。 |  |  |
| 安全负责人 | 确认威胁分析全面，安全需求是保障该功能顺利上线的关键。 |  |  |
