# taosX 高可用 - DS v3

## 引言

1. 目的：本文档对 taosX 高可用架构进行完整阐述，说明架构变更、设计实现、安装部署与运行维护等问题。
2. 范围
3. 受众：taosX 开发者。

## 修订记录

注：版本变更规则，初始版本为 0.1，中间若经过几次较大修改要增加版本号为 0.2， 0.3，最后定稿时的版本号为 1.0，以下为示例

| 日期 | 版本 | 负责人 | 主要修改内容 |
| :--- | :--- | :--- | :--- |
| 2025/05/24 | 0.1 | 霍琳贺 | 初稿 |
| 2025/06/16 | 0.2 | 霍琳贺 | 使用 MNode + XNode 模式 |
| 2025/07/03 | 0.3 | 霍琳贺 | 细化 SQL 命令，覆盖 taosx 已有功能 |
|  | 1.0 |  |  |

## 术语

- **ETL**：抽取（Extract）、转换（Transform）、加载（Load）的数据处理过程。
- **高可用**：系统在大部分时间内可运行，即使在部分节点发生故障时。
- **服务降级**：在系统资源不足或出现故障时，降低服务质量，保证核心功能可用。对于 taosX 服务，服务降级可能发生在主节点宕机，且无法满足 RAFT 协议要求的节点数量时；此时 taosX 服务降级，仅支持已有任务的运行和可用性。
- **负载均衡**：在节点正常服务时，任务节点可在指定数量的节点间均衡分布执行其子任务（本文档中称作 Shard）。
- **分片（Sharding）**：一个 taosX 运行任务，可配置其分片机制，将一个 taosX 运行任务分成多个子任务，以便执行多线程调优或在多节点中分别执行。
- **XNode**：taosX 节点

## 概述

## 架构

本次架构优化中，通过复用 TDengine 的 mnode 来保证数据一致性并进行主体任务的调度，变更如下：

1. MNode 在 taosX 运行时主要提供两个功能：

   1. 任务元数据的存储一致性和持久化：通过 taosd 自身的 mnode 高可用，实现存储的分布式一致性。
   2. 任务的分片与负载均衡：在任务启动后，由 MNode 生成分片任务，并将分片按照负载均衡策略派发到不同的 XNode 节点执行。
2. 所有元数据操作均通过 MNode 执行，包括：

   1. 添加或删除节点：`CREATE/DROP XNODE`；
   2. 修改节点状态：`ALTER XNODE set key value`；
   3. 创建数据接入任务：`CREATE XNODE TASK 'name' FROM 'dsn://source' TO database db`；
   4. 更新数据接入任务：`ALTER XNODE TASK SET FROM 'dsn://source' TO database db`；
   5. 删除任务：DROP XNODE TASK [FORCE] id | 'name';
   6. 创建或删除 Agent：`CREATE XNODE AGENT 'name' WITH options`；
   7. （内部使用）为指定数据接入任务创建负载均衡工作分片：`CREATE XNODE JOB ON <xnodeId>.<taskId> WITH '{json}'`
   8. 为负载均衡工作分片手动切换运行节点：`ALTER XNODE JOB <jid> SET XNODE 1`
   9. 手动结束负载均衡工作分片任务：`DROP XNODE JOB <jid>`
   10. 重新平衡相关工作负载：`REBALANCE XNODE JOBS WHERE job_conditions`
3. XNode 节点之间是平等的，无论 TDengine 包含几个节点，Xnode 都可以任意添加或删除一个或多一个节点。
4. 所有 XNode 节点皆定期上报状态到 MNode。当 MNode 启动后，与 XNode 建立 RPC 连接，XNode 会在 MNode 注册当前节点信息，并定期发送更新消息。节点存活消息过期后，视为节点离线。

## 技术

该分布式架构的核心在实现集群节点管理和任务分片：

1. 使用 MNode 进行 taosx 元数据存储，并使用 TDengine  SQL 命令进行节点管理；
2. 提供任务分片（Sharding）接口，并为支持的数据源添加分片能力；

## 依赖项

无新增依赖项。

# 设计考虑

## 假设和限制

- 假设​：

  - TDengine 依赖：taosX 必须在 TDengine 正常工作时才允许配置数据接入任务。
  - 一个 TDengine 集群仅允许一套 taosX 实例：这意味着 TDengine 与 taosX 必须绑定部署。
  - 共享存储：多个节点 taosX 必须配置数据目录为同一共享存储路径，且存储可靠。
  - 数据一致性：数据可以重复写入 TDengine ，不会影响查询结果和数据一致性。
- 限制​：

  - TDengine 宕机时，taosX 不能创建、修改、或启停任务。删除 TDengine 数据，会影响 taosX 集群使用。

## 设计模式和原则

- 分层架构（Layered Architecture）​​：

  - 关注点分离​：上层模块不跨层调用下层模块。
- 模块化​：

  - 任务、执行器、调度器、管理器模块严格分离，使用接口进行调用。
- 可观测性​：全链路集成 `OpenTelemetry`，遵循可观测性原则。

## 风险和缓解措施

- 风险 2：任务调度过载

  - 表现​：高并发任务导致任务执行和状态同步阻塞。
  - 缓解​：
    - 任务执行器和调度器进程隔离。
- 风险 4：安全漏洞

  - 表现​：REST API 暴露攻击面。
  - 缓解​：
    - 强制 TLS 加密（HTTPS）。

# 详细设计

## 节点管理

通过添加 XNODE 节点，我们将 taosX 扩展为集群架构，每个 XNODE 即是一个 taosX 工作节点，多个节点组成 taosX 集群，可以在多个节点上执行某项数据接入任务。

### 创建 XNode

管理员通过 SQL 语句 `CREATE XNODE` 添加一个 taosX 节点，非管理员不允许操作。示例如下：

```sql
create xnode 'x1:6050' user __xnode__ pass 'Ab123456';
create xnode 'x2:6050';
```

以上 SQL 语句将添加两个 taosX 节点到 TDengine 集群。

在添加第一个 XNode 节点时，需要指定 XNode 所使用的用户：

- 如果用户不存在，则创建用户：

  - 如果用户密码未指定，则创建随机密码；
  - 如果指定用户密码，则使用指定密码创建用户；
- 如果用户已存在，必须输入密码，否则报错（密码将用于 XNode 登录和写入）。

创建第二个及之后的 XNode，不需要指定用户，系统将使用第一次添加 XNode 时的用户密码作为 XNode 的用户。

> [!TIP]
> 该用户将用于：

1. 与 MNode 保持连接；
2. 向 TDengine 写入数据；
3. 将 XNode 及任务运行指标写入 log 库；
   解决的问题：
4. 旧版本用户密码变更后 taosX 写入会停止，新版本生产环境建议使用随机密码，这个密码创建后不会更改，且仅在 XNode 中使用；
5. 其他用户创建 XNode 数据接入任务后，自动赋予 XNode 用户数据库写入权限（相当于 root）。
   未来的变化：
6. 如果之后的版本增加了 Auth Token 方式，可以迁移到 root 用户的授权 TOKEN 方式，就不需要单独的用户了；

XNode 节点添加后，将在 mnode 中创建 ins_xnodes 系统表，包含如下字段：

- ~~id~~：节点 ID。
- ~~url~~：通用的 `host:port` 形式的节点表示。
- ~~create_time~~：XNode 的创建时间。
- ~~update_time~~：XNode 最近一次启动的时间。
- ~~last_ping~~：XNode 最后一次心跳时间。
- ~~jobs~~：在该节点执行的分片任务数。

ins_xnodes 系统表：

| 名称 | 类型 | 默认值 | 取值说明 | 功能描述 |
| :--- | :--- | :--- | :--- | :--- |
| id | int | 必填 |  | xnode 的 id |
| endpoint | varchar | 必填 | 通用的 `host:port` 形式的节点表示。 | 通信地址，xnode 开启的 rpc 服务 |
| status | varchar | offline | online | offline | 节点状态 |
| ~~xnoded~~ | ~~varchar~~ | ~~必填~~ | ~~xnoded 的 host，比如：x1~~ | ~~表示当前链接的 xnoded，应与 taosd 的 host 相同~~ |
| last_ping | datetime | 入库时间 |  | XNode 最后一次心跳时间（可按需保存为时间戳） |
| create_time | datetime | 创建时间 |  | XNode 创建时间 |
| update_time | datetime | 入库时间 |  | XNode 最近一次启动的时间 |

注意：需要展示 xnode 下的 task, job 数量，可以从任务分片表等获取。

### 删除 XNode

同理，使用 DROP XNODE 进行节点删除操作（仅管理员可操作）：

```sql
drop xnode [force] url | id;
```

- `force`：支持强制删除节点（忽略 XNODE 状态）；
- `url`：支持使用节点 URL 删除，如：`drop xnode 'x1:6050'`；
- `id`：支持通过节点 ID 删除，如：`drop xnode 1`；

删除流程如下：

1. 管理员发起 DROP XNode 请求；
2. MNode 接收删除 XNode 请求后，需检查节点可用性：

   1. 如果当前节点已经是最后一个可用的 XNode，需检查是否有任务正在运行：
      1. 如果有正在运行的任务，则直接报错：请停止正在运行的任务后再删除 XNode！
      2. 如果没有正在运行的任务，可在系统表中删除该节点，taosX 删除所有任务且不再接受任务变更。
   2. 如果当前节点不是最后一个可用节点：
      1. 先向 XNode 发起清理操作。XNode 接收删除请求后，按顺序执行以下操作：
         1. 设置当前节点为 DRAIN 模式，表示该节点不接收新增任务执行请求；
         2. 如果有正在运行的任务，获取任务列表，依次执行任务清理操作：
            1. 循环停止所有任务，由 MNode 节点上的负载均衡器重新分发该分片任务到其他可用节点；
            2. 任务清理完毕后，下线节点，表示节点已移除；
         3. 如果没有任务，则直接下线该节点。
      2. 确保 XNode 没有任务正在运行，可以在系统表中删除该节点。

### 查看 XNode

使用 `SHOW XNODES` 查看当前集群中的 xnode 列表和状态。

## 任务管理

### 系统表 ins_xnode_tasks

为进行任务数据的持久化，新增系统表 `ins_xnode_tasks`：

| 名称 | 类型 | 默认值 | 取值说明 | 功能描述 |
| :--- | :--- | :--- | :--- | :--- |
| id | int |  | 自增 | 任务 id |
| name | varchar |  | 非空 | 任务名称，用于标识任务 |
| xnode_id | int | -1 | -1 表示尚未分配到 xnode 执行 |  |
| agent_id | int | -1 | agent 的 id；如果没有 agent 取值 -1 |  |
| from | str |  |  |  |
| to | str |  |  |  |
| parser | str |  |  |  |
| jobs | int | 0 | >0 表示有分片任务 | 分片任务的个数统计 |
| options | varchar |  | 键值对列表 `k=v[,k2[=v2]]` |  |
| status | str |  | 与当前保持一致 | 状态定义：应为所有分片任务的最终状态；首次创建任务为 stopped，并在 reason 说明初次创建 |
| ~~retries~~ | ~~int~~ |  |  | ~~重试次数（迁移至 activity）~~ |
| reason | str |  |  | failed/stopped 需填写此内容：最后出错消息或人为停止说明；首次创建未启动需注明 |
| ~~metrics~~ | ~~str~~ |  | ~~JSON 格式~~ | ~~最近运行指标（指标项另行定义）~~ |
| ~~is_deleted~~ | ~~bool~~ |  |  | ~~软删除标识~~ |
| created_at | datetime |  |  |  |
| finished_at | datetime |  |  | 完成时间 |
| update_at | datetime |  |  | 最近一次更新时间 |

### 新建任务

新建任务可使用 SQL 命令：

```sql
-- 数据写入
CREATE XNODE TASK 'name' FROM 'influxdb://xxx' TO database
  WITH parser 'parser_json' ... ;

-- 数据导出
-- 1. 订阅导出
CREATE XNODE TASK 'name' FROM topic topc1 TO 'kafka://xxx'
  WITH `group.id` = 'g1', `client.id` = 'c1';
-- 2. 数据库导出
CREATE XNODE TASK 'name' FROM database db1 TO 'any://sink'
  WITH op1 = 'v1', op2 = 'v2';
```

Explorer 使用此种方式与集群进行交互，不再依赖 taosX REST API。

为保持 taosX API 的兼容性，集群模式下 taosX 的任意节点 API 都可接收新建任务请求，但需要转发到 MNode 才能新建任务：

1. 用户侧通过 API 发起新建任务请求：

   1. `CreateNewTask` 请求​：用户发起任务创建指令，请求体与单机 taosX 保持不变。
   2. `Validation` 自检​：`XNode` 对请求格式、权限、License 等做基础校验：
      1. ✅ 校验通过​：进入任务创建子流程。
      2. ❌ 校验失败​：直接返回错误（`Invalid NewTask request`）
2. 任务创建流程：

   1. 校验通过后，请求转发至 `MNode`。
      1. 任务转发失败或 MNode 状态异常，直接返回具体错误原因（`MNode is not ready`） 。
   2. `MNode` 获取元数据写锁，​避免并发冲突​。
   3. 进行任务查重（`name` 字段是 UNIQUE 约束的）：
      1. 任务不存在，初始化任务元数据，并进行持久化；
      2. 任务已存在，返回 `Task has been exist`，终止创建流程；
      3. 持久化失败，返回具体错误信息，终止创建流程；
3. 任务调度与结果反馈

   1. 如果创建任务要求即时启动，则触发任务执行流程（见下一节：启动任务）。
   2. 成功反馈​：`MNode` → `XNode` → `User` 逐层返回创建成功。

#### 异常处理机制

### 执行任务

执行任务可使用 SQL 命令：

```sql
START XNODE TASK id | 'name'; -- suggested

-- 或者使用函数形式？
select start_xtask(id); -- 哪种更好些？
```

用户可以在创建任务时即时执行，或通过 API 或 UI 启动任务，触发任务执行流程：

1. 发送请求

   1. 用户发送任务之前请求，所有执行请求均转发到 MNode 执行；
   2. MNode 根据任务 ID 标记任务为执行状态，执行状态无法进行任务修改、再执行、删除操作。
   3. MNode taosX 调度器将指定任务加入执行队列：
      1. 当执行队列不可用时，标记任务失败，并发送失败消息到 MNode。
      2. 加入队列成功后，标记任务启动，并发送成功消息到 MNode。
   4. 根据请求结果：
      1. 如果请求失败，继续触发重试流程，直到达到最大重试次数。
      2. 最终失败后，标记任务发送执行请求失败，发送系统通知和告警。
2. 分片生成

   1. MNode 调度器后台开始执行任务，首先进行分片生成：
      1. 根据数据特征，生成均匀的分片信息；
   2. 根据负载均衡策略，将分片派发到不同的 XNode 上。
      1. 任务派发时，使用异步发送，不阻塞派发线程；
      2. 如果任务派发成功，标记该分片的执行节点、开始时间。
      3. 如果任务派发失败，将分片派发到其他 XNode 节点，重试直到任务派发成功。
3. 分片执行

   1. 运行分片过程中，持续、异步上报运行指标
   2. 分片执行完毕，返回分片执行结果（shardId、指标、成功信息）
   3. 分片执行失败，触发重试：
      1. 达到最大重试次数，视为分片执行失败；
      2. 返回分片执行信息（shardId、指标、失败原因）
4. 结果聚合

   1. 主节点异步等待所有分片状态：
      1. 成功：标记分片执行成功（shardId、指标），异步更新元数据，并同步存储到数据目录。
      2. 失败：标记分片执行失败，记录失败原因，重新派发分片，直到达到最大重试次数后仍然失败，则停止其他分片任务，将该任务标记为运行失败（对应原 taosx 任务状态 `Failed`）。

### 启动/停止任务

启动任务可使用 SQL 命令：

```sql
START XNODE TASK id | 'name' [WITH option_key = option_val];
```

停止任务可使用 SQL 命令：

```sql
STOP XNODE TASK id | 'name' [WITH timeout = 10s];
```

### 删除任务

删除任务可使用 SQL 命令：

```sql
DROP XNODE TASK [FORCE] id | 'name';
```

删除任务时，隐含执行任务停止流程，然后在 MNode 中删除该任务。

1. 用户发起删除任务请求；
2. XNode 转发删除请求到 MNode:

   1. 请求失败则直接报错退出；
3. MNode 申请写锁：

   1. 超时无法申请到写锁，直接退出并向 XNode 回复错误；
4. 在写锁保护范围内，执行以下操作：

   1. 锁定任务，停止接收新的任务调度操作；
   2. 启动停止任务流程，直到所有子任务停止。
   3. 删除任务元数据，删除失败则报错。
5. 发送删除结果（成功、失败）-> XNode -> User。

## taosX Agent 管理

### 新增 Agent

```sql
CREATE XNODE AGENT 'name' [WITH ttl 1y, ipwhitelist x.x.x.x, ...];
```

### 移除 Agent

```sql
DROP XNODE AGENT [FORCE] 'name';
```

## 负载均衡

负载均衡是 taosX 分布式优化的关键特性。不同的任务主节点及其分片都可以运行在不同的 XNode 上。

### 分片策略

分片策略是区分不同类型任务的关键设计之一， taosX 将要实现以下两种分片策略：

1. 消费者策略：基于数据源的能力，提供发布-订阅模式的消费者模型，按照分区分别进行消费的策略；在 taosX 中，TDengine 数据订阅、备份恢复、Kafka 、MQTT、OPC 等适用此策略。其特点是：

   1. 任务启动后，消费者分区基本不变（或变动频率较低）；
   2. 每个消费者分区可能包含所有时间的时序数据；
   3. 分区的进度使用偏移量进行标记，通常可以使用偏移量可允许重复消费；
   4. 一旦偏移量更新，可能无法回退（TMQ）；
   5. 每个分区可能都会持续运行，并不结束；
2. 标记切割策略：基于数据源的特征，对数据集使用可见的标记或标记组合进行切割，按照标记组织方式分别执行的策略；在 taosX 中，TDengine 基于查询的数据迁移、关系型数据库、PI 等；其特点是：

   1. 每个分区只包含一部分时序数据；
   2. 分区可以通过标记进行重复拉取；
   3. 分区随着任务进行逐步增多；
   4. 每个分区预期将在有限的时间内结束；

分片策略使用如下接口：

```rust
/// 分片 ID
pub enum ShardId {
    Consumer {
        id: FastStr,
        parameters: LinkedHashMap<FastStr,FastStr>,
    }
    Markup {
        values: LinkedHashMap<FastStr,FastStr> // A hashmap can be sorted by insert order.
    },
}

/// 分片
pub struct Shard {
    pub task: Arc<Task>,             // 父任务
    pub id: ShardId,             // 消费者策略可能仅一个元素，标记范围策略可能包含多个
    pub checkpoint: Option<Bytes>, // 支持偏移量（消费者策略）或标记范围（切割策略）
}

/// 待执行的任务
pub type AsyncJob = Box<dyn Future<Output=Result<Summary, Error>>>;

/// 数据源定义接口
pub trait ShardingTask {
    async fn shards(&self) -> impl FusedStream<Item=Result<Shard, ShardError>>;
    fn shard_job(&self, context: &Context, shard: &Shard) -> Result<AsyncJob, ShardError>;
}
```

### 负载均衡策略

每个分片的子任务，按照配置负载均衡策略 RoundRobin/Range 在每个可用节点上分配。

- RoundRobin

  - 假设集群 3 个节点（X1-X3），任务 A 生成 7 个分片（A1 - A7），按照 RoundRobin 方式分配如下：
- Range

  - 假设集群 3 个节点（X1-X3），任务 A 生成 7 个分片（A1 - A7），按照 Range 方式分配如下：

### 异常处理

在 XNode 节点宕机后，任务将失败的任务或分片重新分配到可用节点上执行。

1. 节点宕机的确认：

   1. 节点宕机必须在 MNode 和至少一个 XNode 节点确认同时无法连接才会确认为宕机，触发任务重新平衡；
   2. 当无法找到至少一个 XNode 节点时，视为系统异常，报警；
   3. 当 XNode 无法连接到 MNode 时，不做任何操作，当前任务继续执行；
2. 重新平衡：

   1. 首先将 MNode 中的节点状态置为 Offline，不再进行新任务的消费；
   2. 宕机节点的分片任务视为失败，重新分配到其他节点上执行。

### 负载均衡 SQL 指令

1. 为指定数据接入任务创建负载均衡工作分片：

```sql
CREATE XNODE JOB ON <xnodeId>.<taskId> WITH '{json}'
```

其中 <xnodeId> 表示 XNode ID，<taskId> 表示任务 ID，该语法用于在 XNODE 中动态创建新的分片任务，仅限 XNODE 绑定用户使用。

创建分片任务后，可以通过 `SHOW XNODE JOBS` 查询当前所有分片任务和其运行所在节点。

1. 为负载均衡工作分片手动切换运行节点：

```sql
ALTER XNODE JOB <jid> SET XNODE 1;
```

2. 手动结束负载均衡工作分片任务：

```sql
DELETE XNODE JOB WHERE JID = <jid>;
```

3. 重新平衡相关工作负载：

```sql
REBALANCE XNODE JOBS WHERE job_conditions;
```

## 可观测性

### SQL：`SHOW XNODE TASKS`

添加系统表 XNODE_TASKS，用于存储和查看当前所有任务状态。包含如下字段：

- `id`：自动生成的任务 ID；

- `name`：用户自定义的任务名称；

- `source`：数据来源；

- `sink`：数据端点；

- `transform`：JSON 类型的 Transform 表达式；

- `created_at`：创建时间；

- `status`：当前任务状态；

- `metrics`：当前任务运行指标；

- `shards`：当前任务分片数量；

- `shards_status`：`成功/失败/总数` 形式的分片状态；

### SQL：`SHOW XNODE JOBS`

查看当前所有分片任务。包含如下字段：

- `tid`：整型，任务 ID；

- `jid`：字符串类型，任务调度器为此分片分配的 ID；

- `sid`：字符串类型，分片标识；

- `status`：字符串类型，分片任务执行状态；

- `retries`：字符串类型，已重试次数 / 最大重试次数，如 `1/5` 表示最大重试次数为 `5`，已执行 `1`次重试。

- `xnode`：分片任务所在的节点 ID；

- `metrics`：JSON 类型的任务运行指标；

### SQL：`DRAIN XNODE id`

设置当前节点为 DRAIN 模式，不接收任务执行，已有任务重新分配到其他节点中执行。

## XNODE 服务

原来的 taosx 服务模式转为 xnode 服务。

新的 XNODE 服务不存储元数据，仅接收 mnode 的指令并进行交互。

### 数据库

为原有的数据库提供任务迁移命令：

```bash
taosx migrate -i /var/lib/taos/taosx/taosx.db -o "taos://root:taosdata@localhost:6030"
```

将数据库中的任务迁移到目标集群中。

当数据库中的任务包含多个集群时报错，此时可指定需要迁移的集群 ID：

```bash
taosx migrate \
 -i /var/lib/taos/taosx/taosx.db \
 -o "taos://root:taosdata@localhost:6030" \
 --cid "1092920786939788088"
```

### RPC

MNode 启动后，将与 xnodes 列表中的节点建立 RPC 连接。RPC 连接使用当前 taosx 的 Arrow Flight RPC 方案。

功能列表包括：

1. 获取当前 XNODE 状态和运行的任务。
2. 检查数据源在当前节点的可用性。
3. 发送指定任务。
4. KeepAlive。

具体实现在开发时再补充。

### API

当前 API 仍然可用，但将 MNode 作为元数据存储和指令入口。

## Explorer

Explorer 不再依赖 taosX ，现有数据源功能将完全由 SQL 命令取代（Explorer 调用 API 不变，后端实现改为 SQL 语句）。

## 共享存储

### 目录结构

taos.cfg 中添加 taosxDataDir 参数，等同于 /etc/taos/taosx.toml 的 `data_dir`，表示 taosX 数据目录。

将 TDengine 和 XNode 各节点的 taosX 数据目录设置为同一目录，如 `/nas/taosx/data/`，目录结构如下：

![](static/XoE5b6vvloMnP8xGWppcMvtbnPd.png)

其中：

- `files`：为上传文件的目录；
- `tasks`：为任务目录，其下是以顺序任务 ID 生成的任务目录 `tasks/:id`，每个任务可能包含以下目录或文件：

  - `x.lock`：当主任务执行时，创建文件锁 `x.lock` 以防止冲突和重复执行。
  - `checkpoints`：为任务或分片生成和记录检查点。
  - `metrics`：运行指标（缓存），会定时刷新写入到 TDengine。
  - `shards`：每个分片有独立的数据文件目录，可能包含：
    - `dump`：MQTT、OPC 开启 dump  功能后的默认数据写入路径。
    - `archived`：当有数据被归档时，创建此目录，并生成 `prefix.%Y%m%d.%N` 形式的归档文件。
    - `cache`：当有数据暂时无法入库，创建此目录，并生成 `prefix.%Y%m%d.%N` 形式的缓存文件，待 TDengine 恢复正常后继续入库。
    - `metrics`：运行指标。

需要注意：

- 除了 shards 独立的数据文件目录外，其他文件均 MNode 上的 XNode 管理模块生成和管理，分片任务不能操作其他文件，以避免冲突。
- 主任务进行数据的聚合工作，可能存在一定的延迟。

### 定时备份

在 taos.cfg 中：

- 添加数据备份 CRON 表达式（`xNodeBackupCron`），taosX 将自动定时备份数据文件。
   - 其格式如下：

      ```text
      sec  min  hour  day-of-month  month  day-of-week  year
      ```

	- 示例：
		- `0 15 10 ? * MON-FRI` 表示周一到周五每天上午 10:15 执行作业
		- `0 0/10 * * * ?` 表示每 10 分钟执行任务

- 添加数据备份路径参数（`xNodeBackupDir`），作为数据备份存储路径（支持S3？）。

## 待解决问题

## Mnode 上的 taosX 管理模块是否采用独立进程？

独立线程的好处：

- 出现问题不阻塞 TDengine

## taosX 各个数据源下分片如何实现？

### Kafka/TMQ

数据订阅类型的任务，根据最大并发数 + VGROUPS/Partitions 数量进行分片。

### MQTT

按照节点分片，每个节点最大并发数为 1。假设 

### TDengine Query

基于查询的同步或数据迁移，需要支持动态的分片生成，支持：

1. 子表名 + 时间范围：用于历史数据迁移

2. 超级表名 + 时间范围：用于实时查询同步

两种分片生成方式。

### MySQL/PG/Oracle

关系型数据库，按照 标签 + 时间范围 的方式动态生成分片，类似于 TDengine 查询同步。

即：将现在使用多任务并发同步的方式改成多节点 + 多任务的方式。

### PI

受限于仅允许使用安装了授权的 AF SDK 的 Windows 系统，保持原样，不分片。

### InfluxDB/OpenTSDB

计划将 InfluxDB/OpenTSDB 外置数据源迁移到 Rust 实现，暂保持原样，不分片。

### OPC-UA/DA

数据源暂时不具备分片条件，保持原样，不分片。

## 接口规范

1. API 文档（如果有超过 20 个API，应将其拆分为 API 组，可以使用工具生成 API 文档）

2. 用户界面（如适用）：概述 UI/UX 设计考虑

## 安全考虑（如适用）

1. 安全要求，如数据加密、用户认证、授权和其他在设计模块时考虑的安全措施

2. 概述如何缓解漏洞以及如何处理敏感数据

## 性能和可扩展性（如适用）

1. 性能要求：定义模块的性能期望（例如响应时间、吞吐量）

2. 可扩展性：讨论模块如何处理增加的负载，是否有任何特定策略确保模块水平或垂直扩展

## 部署和配置

## 兼容性

### 升级

taosX 服务启动时，进行数据迁移操作，保证旧版本的配置文件可平滑迁移到新版本，但以下情况除外：

1. 多个节点数据不会自动迁移。需要在需要迁移数据的节点上手动执行数据迁移命令：`taosx migrate`

### 降级

taosX 不删除旧数据，因此允许降级，降级后原数据保持不变，但新增数据不会体现。

用户可手动删除升级过程新增的数据文件。

## 监控和维护

1. 监控：概述任何监控机制，以跟踪模块的健康状况和性能

2. 日志记录和诊断：讨论日志策略，包括将记录哪些数据以及如何诊断和追踪问题

3. 维护：描述随着时间的推移维护模块的方法，包括更新、修复漏洞和支持新需求

## 参考资料

1. 功能规格说明

2. 测试规格说明



```
