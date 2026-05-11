# Explorer XNode 管理页面 - 威胁建模报告

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-07 | 2026-05-07 | 1.0 | Linhe Huo | 基于 FS 完成首版 STRIDE 威胁建模 |

### 2. 基本信息

| 项目信息 | 内容 |
| --- | --- |
| 报告编号 | `TM-TSDB-XNODE-001` |
| 需求名称 | Explorer XNode 管理页面 |
| 设计文档 | `05-设计文档/Explorer XNode 管理页面 FS.md` |
| 版本编号 | 3.4.2 |
| 报告日期 | 2026-05-07 |
| 总体评价 | 附带条件通过 |

### 3. 分析报告

Explorer XNode 管理页面通过前端 SQL 代理执行 CREATE/DROP XNODE 等 SQL 语句。核心风险是 SQL 注入和认证凭据泄露。

核心发现：共识别出 `4` 个潜在威胁，其中 `中危` `2` 个，`低危` `2` 个。

### 4. 威胁识别与分析（STRIDE）

#### 4.1 威胁评估

| 威胁ID | 威胁描述/攻击场景 | STRIDE | 风险等级 |
| --- | --- | --- | --- |
| T-XNODE-01 | CREATE XNODE SQL 依赖前端转义防注入，Token 字段无任何校验，攻击者可构造恶意 Token 注入 SQL | T（篡改） | 中 |
| T-XNODE-02 | XNode 的 Password 和 Token 存储在 TDengine 系统表中，如果系统表访问控制不严，可被非管理员读取 | I（信息泄露） | 中 |
| T-XNODE-03 | DROP XNODE 操作无二次确认，误操作可导致数据源配置丢失 | D（拒绝服务） | 低 |
| T-XNODE-04 | Explorer SQL 代理层如果未做权限校验，低权限用户可执行管理员操作 | E（权限提升） | 低 |

### 5. 风险评估与应对措施

| 威胁ID | 应对措施 | 安全需求编号 | 优先级 |
| --- | --- | --- | --- |
| T-XNODE-01 | Explorer SQL 代理改用参数化查询，Token 字段增加格式校验 | SEC-XNODE-001 | 中 |
| T-XNODE-02 | XNode 凭据在系统表中设为 sysInfo=true 保护 | SEC-XNODE-002 | 中 |
| T-XNODE-03 | DROP XNODE 增加二次确认机制 | SEC-XNODE-003 | 低 |
| T-XNODE-04 | Explorer SQL 代理增加权限校验 | SEC-XNODE-004 | 低 |

### 6. 安全需求追踪表

| 需求编号 | 来源威胁 | 安全需求描述 | 类型 | 优先级 |
| --- | --- | --- | --- | --- |
| SEC-XNODE-001 | T-XNODE-01 | SQL 参数化查询 | 安全设计约束 | 中 |
| SEC-XNODE-002 | T-XNODE-02 | 凭据系统表保护 | 安全配置需求 | 中 |
| SEC-XNODE-003 | T-XNODE-03 | DROP 操作二次确认 | 健壮性需求 | 低 |
| SEC-XNODE-004 | T-XNODE-04 | SQL 代理权限校验 | 安全功能需求 | 低 |

### 7. 总结

Explorer XNode 管理页面的 SQL 注入防护应依赖服务端而非前端转义。

---

*本威胁建模报告基于 AI 辅助分析生成，需安全委员会复核确认后归档。*
