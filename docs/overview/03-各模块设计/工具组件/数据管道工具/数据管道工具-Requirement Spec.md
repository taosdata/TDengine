# 数据管道工具-Requirement Spec

## 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-07 | 2025-01-07 | 1.0 | 霍琳贺 | 从 [Confluence](https://jira.taosdata.com:18090/pages/viewpage.action?pageId=133399357) 导入需求文档并适配文档模板 |
| 2026-02-06 | 2025-02-06 | 1.1 | 霍琳贺 | 升级为高可用架构版本，增加 XNode、MNode、分片、负载均衡等分布式功能 |

## 引言

### 2.1 术语与缩写名词

1. `**taosX**` 为产品名，在高可用架构中指 XNode 服务。
2. `**XNode**`：taosX 的高可用分布式节点，是 taosX 在 TDengine 集群中的工作节点。
3. `**MNode**`：TDengine 的管理节点，在高可用架构中负责 taosX 元数据存储和任务调度。
4. `**xnoded**`：运行在 MNode Leader 节点上的守护进程，负责管理 XNode 节点状态、任务分片和负载均衡。
5. `**Stream**` **数据流：**是由一个或多个数据源持续生成的数据。时间戳是其附带属性，但不一定是完全按时间顺序产生的（即：允许乱序）。数据流可以是多种形式，如文件，SOCKET，TCP/HTTP数据流，消息队列，S3对象存储，关系型数据库或其他时序数据库数据流等。taosX 作为消费者消费数据流写入 TDengine 集群，或将 TDengine 数据流写入其他端点。
6. `**Source**` ：数据源或输入端。
7. `**Sink**`：数据目标或输出端。
8. `**Transform**`：taosX 的输入流可以通过变换来进行诸如过滤、转换、聚合等数据处理操作，处理后的结果仍然是一个所支持数据类型的数据流。但不一定与原始输入流的类型一致。其中每经过一次变换的操作称为 `Transformer`，`Transformer` 可以链式叠加。`Transfrom` 之后的数据流入 `Sink` 。
9. `**Shard**` **分片：**在高可用架构中，一个任务可被划分为多个子任务（分片），分布在不同 XNode 上并行执行。
10. `**2.x**` `**3.x**`：文档中出现 2.x 3.x 表示主版本号为 2 或 3 的多个版本。

### 2.2 相关文档资料

功能和执行形式上，参考以下两种开源工具：
1. [DataX - 阿里云DataWorks数据集成的开源版本](https://github.com/alibaba/DataX)
2. [SeaTunnel Docs](https://interestinglab.github.io/seatunnel-docs/)

### 2.3 优先级要求

此工具为高优先级开发项目，随 TDengine 3.3+ 版本一起发布。

### 2.4 版本要求

此工具作为企业版的特有功能，随 TDengine 企业版一起发布。高可用功能需要 TDengine 3.3.0.0 及以上版本支持。

## 需求目标

### 3.1 产品形式

高可用架构下，taosX 以 XNode 组件形式集成到 TDengine 平台，成为 TDengine 平台的数据同步项目。
产品的主要内容变化：
1. **XNode 服务**：`taosx` 可执行文件作为 XNode 工作节点运行，通过 `CREATE XNODE` SQL 命令注册到 TDengine 集群。
2. **SQL 管理接口**：所有任务管理操作通过 SQL 命令完成，不再依赖独立的 REST API 创建任务：
  - `CREATE XNODE`：添加 XNode 节点
  - `CREATE XNODE TASK`：创建数据接入任务
  - `START/STOP XNODE TASK`：启停任务
  - `SHOW XNODES/XNODE TASKS/XNODE JOBS`：查看节点、任务、分片状态
1. **高可用保障**：通过 MNode 存储元数据，利用 TDengine 的 sdb 保证数据一致性；当 XNode 节点故障时，任务自动迁移到其他可用节点。
2. **负载均衡**：任务支持分片（Sharding），分片可分布在多个 XNode 上并行执行，由 xnoded 统一调度。
3. **共享存储**：多节点共享同一数据目录，用于存储任务检查点、缓存数据、归档文件等。
4. **Agent 模式**：`taosx-agent` 作为代理服务，可通过 SQL 命令 `CREATE XNODE AGENT` 管理。

### 3.2 运行环境

**taosX** 除了需要运行在 TDengine 支持的架构和系统之外，还要求可以提供边云协同的能力。需要支持的平台和 CPU 架构如下：
1. CPU 架构：amd64/x86_64, arm64, 以及国产芯片架构包括龙芯等。
2. OS 操作系统: Linux/Windows/MacOS，以及国产操作系统包括凝思、麒麟等。
**高可用附加要求**：
1. 需要配置共享存储路径（如 NAS），所有 XNode 节点挂载同一数据目录。
2. TDengine 集群需正常运行，MNode 至少包含 3 个节点以保证高可用。

## 功能需求

| 序号 | **功能类别** | **功能名称** | 功能描述 |
| --- | --- | --- | --- |
| 1 | 数据迁移 | 2.x 版本历史数据迁移到 3.x 集群 | 将 TDengine 2.x 版本数据库、超级表或普通表数据迁移到 3.x 版本新集群 |
|  |  | 3.x 版本历史数据迁移到 3.x 集群 | 将 TDengine 3.x 版本数据库、超级表或普通表数据迁移到另一个 3.x 版本新集群 |
|  |  | 2.x 版本实时查询数据迁移到 3.x 集群 | 将 TDengine 2.x 版本数据库、超级表或普通表根据查询条件按照一定周期持续查询最新写入的数据并将数据写入 3.x 新集群 |
|  |  | 3.x 版本实时查询数据迁移到 3.x 集群 | 将 TDengine 3.x 版本数据库、超级表或普通表根据查询条件按照一定周期持续查询最新写入的数据并将数据写入 3.x 新集群 |
|  |  | 数据迁移支持新增常量标签 | 在数据迁移过程中支持新增常量标签写入新集群 |
|  |  | 数据迁移支持表名变换 | 在数据迁移过程中支持超级表、子表的表名重命名 |
|  |  | 数据迁移支持列名变换 | 在数据迁移过程中支持按照一定的规则将名重命名 |
| 2 | 数据同步 | 订阅一个数据库并同步到其他集群 |  |
|  |  | 订阅一个超级表并同步到其他集群 |  |
|  |  | 订阅一个查询语句并同步到其他集群 |  |
|  |  | 订阅多个主题并同步到其他集群 |  |
| 3 | 双活 | 使用订阅保证双集群间同步数据 |  |
| 4 | 数据备份 | 备份 3.x 数据到本地文件 |  |
| 5 | 数据恢复 | 将备份文件恢复到 3.x 集群 |  |
| 6 | 数据导出 | 导出 TDengine 查询结果到 CSV 文件 |  |
| 7 | 数据导入 | 导入 CSV 文件到 TDengine |  |
|  |  | 导入 PI Archive 数据到 TDengine |  |
|  |  | 导入 PI Asset Framework 数据到 TDengine |  |
|  |  | 导入 OpenTSDB 数据到 TDengine |  |
|  |  | 导入 InfluxDB 数据到 TDengine |  |
|  |  | 导入 Kafka 数据到 TDengine |  |
|  |  | 导入 MQTT 数据到 TDengine |  |
| 8 | 节点管理 | 添加 XNode 节点 | 通过 CREATE XNODE SQL 命令将 taosX 节点注册到 TDengine 集群 |
|  |  | 删除 XNode 节点 | 通过 DROP XNODE SQL 命令移除节点，支持强制删除和优雅下线（DRAIN 模式） |
|  |  | 查看 XNode 状态 | 通过 SHOW XNODES 查看节点列表、状态、任务数等 |
|  |  | 节点状态管理 | 支持设置节点为 DRAIN 模式，停止接收新任务，已有任务迁移到其他节点 |
| 9 | 任务分片 | 任务自动分片 | 根据数据源特征自动将任务划分为多个分片（Shard） |
|  |  | 消费者策略分片 | 适用于 Kafka/TMQ/MQTT 等订阅类数据源，按分区消费 |
|  |  | 标记切割策略分片 | 适用于 TDengine 查询/关系型数据库等，按时间范围或标记切分 |
| 10 | 负载均衡 | 分片负载均衡 | 分片按照 RoundRobin/Range 策略分布在不同 XNode 上执行 |
|  |  | 故障自动迁移 | XNode 宕机时，自动将故障节点上的分片迁移到其他可用节点 |
|  |  | 手动重新平衡 | 通过 REBALANCE XNODE JOBS SQL 命令手动触发负载重平衡 |
| 11 | 任务管理 | SQL 创建任务 | 通过 CREATE XNODE TASK SQL 命令创建数据接入任务 |
|  |  | SQL 启停任务 | 通过 START/STOP XNODE TASK SQL 命令启停任务 |
|  |  | SQL 删除任务 | 通过 DROP XNODE TASK SQL 命令删除任务 |
|  |  | 查看任务和分片 | 通过 SHOW XNODE TASKS/JOBS 查看任务状态和分片执行情况 |
| 12 | Agent 管理 | SQL 管理 Agent | 通过 CREATE/DROP/SHOW XNODE AGENT SQL 命令管理 Agent |
| 13 | 服务降级 | 只读模式 | 当 MNode 无法满足 RAFT 协议要求时，服务降级为只读模式，支持已有任务运行但禁止新任务创建 |
| 14 | 数据迁移 | 旧版本数据迁移 | 支持通过 taosx migrate 命令将旧版本 SQLite 数据迁移到 MNode |

## 性能需求

1. 数据迁移在不做任何变换下，支持 1000 万数据点每秒迁移速度。
2. 数据同步支持 100 万点每秒同步速度。
3. 数据备份文件磁盘占用不高于 TSDB，落盘数据支持压缩。
4. CSV 数据导入支持 100 万点每秒导入速度。
5. **高可用附加要求**：
  - 任务分片切换时间：XNode 故障后，分片迁移到其他节点的时间应小于 30 秒。
  - MNode 切换时间：MNode Leader 切换后，xnoded 应在 10 秒内恢复服务。

## 安全需求

### 6.1 认证授权

1. **XNode 与 MNode 认证**
  - XNode 使用独立用户（`__xnode__`）与 MNode 通信
  - 该用户密码随机生成，仅用于 XNode 内部通信，不对外暴露
  - XNode 与 MNode 之间使用 JWT Token 进行双向认证
1. **Agent 与 XNode 认证**
  - Agent 必须使用有效的 JWT Token 才能连接 XNode
  - Token 应支持自动轮换机制
  - Agent 连接应支持 IP 白名单限制
1. **用户权限控制**
  - 只有管理员（root 或具有相应权限的用户）才能执行 `CREATE/DROP/ALTER XNODE` 操作
  - 普通用户可以创建、管理自己的任务，但不能操作其他用户的任务
  - 任务写入权限自动由系统管理，无需手动授权

### 6.2 通信安全

1. **传输加密**
  - XNode 与 MNode 之间使用 gRPC + Arrow Flight 通信，必须启用 TLS 加密
  - Agent 与 XNode 之间的数据传输必须支持 TLS 加密
  - 支持 TLS 1.2 及以上版本，优先使用 TLS 1.3
1. **双向认证**
  - XNode 与 MNode 之间支持双向 TLS (mTLS) 认证
  - 证书应支持定期轮换
1. **数据源连接安全**
  - 外部数据源连接应优先使用加密协议（SSL/TLS）
  - 敏感数据源（如数据库）连接应验证服务器证书

### 6.3 数据安全

1. **DSN 密码保护**
  - DSN 中的密码必须加密存储，不得明文保存在配置文件或共享存储中
  - 支持使用环境变量或密钥管理系统注入密码
  - 内存中的密码应使用安全字符串保护，防止核心转储泄露
1. **共享存储安全**
  - 各任务的检查点和缓存数据在共享存储中按任务 ID 隔离存储
  - 敏感任务数据支持加密存储
  - 共享存储访问应配置适当的访问控制列表（ACL）
1. **数据完整性**
  - 传输中的数据应具有完整性校验机制
  - 关键配置文件应具有完整性保护

## 其他需求

### 7.1 兼容性需求

- **TDengine 版本**：高可用功能需要 TDengine 3.4.0.0 及以上版本。
- **数据迁移**：数据源支持 2.0.20 即以上版本、2.2、2.4、2.6 版本，目标端支持 3.0、3.1、3.2 及以上版本。
- **数据订阅**：支持任意 3.0 以上低版本数据订阅写入到高版本集群。
- **数据导入**：支持 Windows、Linux、macOS 下的 CSV 文件。
- **备份恢复**：支持备份文件复制到另一个机器上读取并恢复到新集群，与运行系统无关。
- **数据导入**：支持 InfluxDB 1.7 及以上版本。

### 7.2 接口需求

**SQL 接口**：
所有管理操作通过 TDengine SQL 接口完成，主要命令包括：
- `CREATE XNODE 'url' [USER user PASS 'password']`：创建 XNode
- `DROP XNODE [FORCE] url|id`：删除 XNode
- `SHOW XNODES`：查看 XNode 列表
- `CREATE XNODE TASK 'name' FROM 'dsn' TO database [WITH options]`：创建任务
- `START XNODE TASK id|'name'`：启动任务
- `STOP XNODE TASK id|'name' [WITH timeout = 10s]`：停止任务
- `DROP XNODE TASK [FORCE] id|'name'`：删除任务
- `SHOW XNODE TASKS`：查看任务列表
- `SHOW XNODE JOBS`：查看分片任务列表
- `CREATE XNODE AGENT 'name' [WITH options]`：创建 Agent
- `DROP XNODE AGENT [FORCE] 'name'`：删除 Agent
- `SHOW XNODE AGENTS`：查看 Agent 列表
- `DRAIN XNODE id`：设置节点为 DRAIN 模式
- `REBALANCE XNODE JOBS WHERE conditions`：重新平衡负载
**保留 API 接口**：
为保持兼容性，REST API 和 Arrow Flight gRPC 接口仍然可用，但元数据操作将转发到 MNode 处理。

### 7.3 运维需求

- **服务管理**：XNode 服务由 systemd 管理（Linux）或服务管理器（Windows）管理。
- **共享存储**：所有 XNode 节点必须配置相同的共享存储路径（`taosxDataDir`）。
- **自动备份**：支持配置 CRON 表达式自动备份数据文件到指定目录或 S3。
- **监控指标**：XNode 和任务指标通过 taosKeeper 写入 `log` 库，支持通过 `SHOW XNODE TASKS/JOBS` 查看实时状态。
- **日志管理**：日志存储在共享存储的 `logs` 目录下，支持通过 `task.id` 过滤特定任务日志。

### 7.4 易用性需求

- **命令行工具**：`taosx` 命令行工具保留，支持 `migrate` 子命令进行数据迁移。
- **可视化管理**：Explorer 通过 SQL 命令与集群交互，提供图形化任务管理界面。
- **错误提示**：SQL 命令执行失败时返回清晰的中文/英文错误信息。

### 7.5 测试需求（不含测试例）

- XNode 功能可使用 SQL 命令进行功能和性能测试。
- 需要测试故障场景：XNode 宕机、MNode 切换、网络分区等。
- 需要测试负载均衡场景：任务分片是否正确分布、故障迁移是否及时。
- 需要测试兼容性：旧版本数据迁移、API 兼容性等。
