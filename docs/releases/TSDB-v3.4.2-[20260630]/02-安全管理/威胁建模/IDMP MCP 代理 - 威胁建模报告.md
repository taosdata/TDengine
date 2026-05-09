# IDMP MCP 代理 - 威胁建模报告

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-07 | 2026-05-07 | 1.0 | Linhe Huo | 基于 FS 完成首版 STRIDE 威胁建模 |

### 2. 基本信息

| 项目信息 | 内容 |
| --- | --- |
| 报告编号 | `TM-TSDB-PROXY-001` |
| 需求名称 | IDMP MCP 代理 |
| 设计文档 | `05-设计文档/IDMP MCP 代理 FS.md` |
| 版本编号 | 3.4.2 |
| 报告日期 | 2026-05-07 |
| 总体评价 | 附带条件通过 |

### 3. 分析报告

IDMP MCP 代理在 IDMP REST 服务内新增 HTTP 反向代理，将 MCP 流量转发到后端 MCP Server。核心风险在于代理层不做独立鉴权，Authorization 头无条件透传。

核心发现：共识别出 `5` 个潜在威胁，其中 `高危` `1` 个，`中危` `2` 个，`低危` `2` 个。

### 4. 威胁识别与分析（STRIDE）

#### 4.1 关键实体与数据

| 实体/数据 | 描述 | 敏感性 |
| --- | --- | --- |
| Authorization Bearer Token | 调用方传入的认证 Token，代理层透传 | 高 |
| X-Forwarded-For | 客户端 IP 地址，用于审计和速率限制 | 中 |
| MCP 会话 ID | mcp-session-id，用于会话管理 | 中 |
| MCP 工具调用请求 | JSON-RPC 格式的工具调用参数 | 中 |
| SSE 事件流 | Server-Sent Events 格式的增量结果 | 低 |

#### 4.2 威胁评估

| 威胁ID | 威胁描述/攻击场景 | STRIDE | 相关组件/数据流 | 风险等级 |
| --- | --- | --- | --- | --- |
| T-PROXY-01 | 代理层无条件透传 Authorization 头，不做任何校验。攻击者构造 MCP 可接受但 IDMP 不校验的 Token 绕过认证；或在可信内网中伪造任意身份 | S（仿冒）/E（权限提升） | /api/v1/mcp/stream 入口 | 高 |
| T-PROXY-02 | X-Forwarded-For 可被客户端伪造（§4.2 明确'若调用方已提供则沿用'），攻击者可绕过基于 IP 的审计或速率限制 | S（仿冒） | 代理层 -> 审计/速率限制 | 中 |
| T-PROXY-03 | hop-by-hop 头过滤列表缺少 cookie 头。如果上游 MCP 使用 Cookie 认证，代理会透传调用方的 Cookie | I（信息泄露） | 代理层 -> 上游 MCP | 中 |
| T-PROXY-04 | MCP 工具调用请求中的参数（如 SQL 语句、文件路径等）直接透传到后端 MCP Server，如果 MCP Server 缺乏输入校验，可导致注入攻击 | T（篡改） | MCP 工具请求 -> MCP Server | 低 |
| T-PROXY-05 | SSE 事件流中可能包含敏感数据（如查询结果），代理层不做内容过滤 | I（信息泄露） | MCP Server -> SSE 流 -> 客户端 | 低 |

### 5. 风险评估与应对措施

#### 5.1 高危威胁应对

##### T-PROXY-01：认证绕过

| 项目 | 内容 |
| --- | --- |
| 威胁 | MCP 代理层无条件透传 Authorization 头，无独立鉴权 |
| 影响 | 未授权访问 MCP 能力，审计来源伪造 |
| 现有防护 | hop-by-hop 头过滤、X-Original-URI 补充 |
| 残余风险 | 代理层不做 Token 校验 |
| 应对措施 | ① 代理层增加可选 Token 格式校验（至少验证 JWT 格式和签名）；② 补充 cookie 到不转发列表；③ X-Forwarded-For 仅在可信代理链中追加，不可信来源应丢弃 |
| 安全需求编号 | SEC-PROXY-001 |
| 优先级 | 高 |
| 验证方式 | 认证绕过渗透测试 + hop-by-hop 头过滤验证 |

#### 5.2 中低危威胁应对

| 威胁ID | 应对措施 | 安全需求编号 | 优先级 |
| --- | --- | --- | --- |
| T-PROXY-02 | X-Forwarded-For 仅在可信代理链中追加，不可信来源应丢弃或覆盖 | SEC-PROXY-002 | 中 |
| T-PROXY-03 | 补充 cookie 到 hop-by-hop 不转发列表 | SEC-PROXY-003 | 中 |
| T-PROXY-04 | MCP Server 端增加输入校验，代理层可增加请求参数白名单过滤 | SEC-PROXY-004 | 低 |
| T-PROXY-05 | 评估 SSE 事件流中是否包含敏感数据，必要时增加内容过滤 | SEC-PROXY-005 | 低 |

### 6. 安全需求追踪表

| 需求编号 | 来源威胁 | 安全需求描述 | 类型 | 优先级 |
| --- | --- | --- | --- | --- |
| SEC-PROXY-001 | T-PROXY-01 | 代理层 Token 格式校验 + cookie 过滤 | 安全功能需求 | 高 |
| SEC-PROXY-002 | T-PROXY-02 | X-Forwarded-For 可信代理链限制 | 安全设计约束 | 中 |
| SEC-PROXY-003 | T-PROXY-03 | cookie 加入 hop-by-hop 过滤列表 | 安全设计约束 | 中 |
| SEC-PROXY-004 | T-PROXY-04 | MCP Server 输入校验 | 安全设计约束 | 低 |
| SEC-PROXY-005 | T-PROXY-05 | SSE 事件流内容过滤 | 安全设计约束 | 低 |

### 7. 总结

MCP 代理的认证边界设计是本特性的核心安全风险。在'可信内网'假设下可接受，但缺乏明确的信任边界声明。建议代理层增加至少一层 Token 格式校验，并明确信任边界文档。

---

*本威胁建模报告基于 AI 辅助分析生成，需安全委员会复核确认后归档。*
