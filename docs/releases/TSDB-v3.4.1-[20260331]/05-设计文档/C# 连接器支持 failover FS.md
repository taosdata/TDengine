# C# 连接器支持 failover FS

## 1 修订记录

| 编写日期 | 发布日期 | 版本  | 修订人 | 主要修改内容 |
| --- | --- |-----|-----|--------|
| 2026-03-27 | 2026-03-27 | 1.0 | 谭雪峰 | 初稿     |

## 2 背景

当前 `taos-connector-dotnet` 在 WebSocket 和 TMQ 场景下主要依赖单地址访问 taosAdapter。单地址部署在适配器重启、节点故障、网络抖动或维护窗口期间容易成为可用性瓶颈，也不利于在多个 adapter 实例之间做客户端侧分流。

本次功能的目标是为 WebSocket 查询/写入场景和 TMQ 消费场景增加多地址接入、自动故障转移和有限的客户端侧负载分配能力，同时保持 Native 协议行为清晰可控：Native 继续只支持单地址，不引入多地址故障转移。

此外，本次变更还补充了多地址解析相关的非法配置前置校验。对于 WebSocket 连接，空 `host` 会在地址解析阶段报错；对于 Native 连接，仍保留空 `host` 透传到底层 C 库的原有语义。

## 3 定义

- **故障转移（Failover）**：当前连接不可用时，客户端自动切换到同一配置中的其他可用地址继续建立连接。
- **多地址配置**：在一个连接配置中提供多个地址，地址之间用英文逗号分隔。
- **首选地址**：最近一次成功建立连接的地址。重连时优先尝试该地址，再尝试其他地址。
- **地址租约（Lease）**：客户端在成功选中某个地址后持有的占用记录，用于维护当前进程内的地址连接计数。
- **最少连接选择**：在当前进程内优先选择活动连接数最少的地址，用于初始连接和非首选地址回退时的负载分配。
- **至少一次语义**：在 TMQ 故障转移过程中，offset 自动提交或手动提交可能失败，重复消费是允许的，业务侧需要自行保证幂等。

## 4 行为说明

### 4.1 连接配置

#### 4.1.1 WebSocket / 通用连接字符串

`ConnectionStringBuilder` 的 `host` 参数支持多个地址，地址之间使用英文逗号分隔。文档约定每个地址使用以下格式：

- `host`
- `host:port`
- `[ipv6]`
- `[ipv6]:port`

示例：

```csharp
var builder = new ConnectionStringBuilder(
    "protocol=WebSocket;" +
    "host=adapter-a:6041,adapter-b:6041;" +
    "username=root;" +
    "password=taosdata;" +
    "autoReconnect=true;" +
    "reconnectRetryCount=3;" +
    "reconnectIntervalMs=2000;");
```

端口解析规则如下：

- 地址本身显式带端口时，优先使用地址上的端口。
- 地址不带端口时，若全局 `port` 已设置且大于 0，则使用全局 `port` 作为该地址的兜底端口。
- `protocol=WebSocket` 且仍未得到端口时：
  - `useSSL=true` 时默认端口为 `443`
  - `useSSL=false` 时默认端口为 `6041`
- `protocol=Native` 且没有显式端口时，不自动补全 WebSocket 默认端口。

`port` 的作用可以概括为：

- 给**没有显式端口**的地址统一补端口
- 不覆盖已经写在地址里的端口
- 在多地址配置下，对每个未写端口的地址分别生效

例如：

```csharp
var builder = new ConnectionStringBuilder(
    "protocol=WebSocket;" +
    "host=adapter-a,adapter-b:6050,adapter-c;" +
    "port=6041;" +
    "useSSL=false;");
```

上面的实际端口效果是：

- `adapter-a` -> `6041`
- `adapter-b` -> `6050`
- `adapter-c` -> `6041`

如果没有配置 `port`，则未写端口的地址按协议默认值补齐；对于 WebSocket，默认值由 `useSSL` 决定。

重复地址会按标准化后的主机和最终端口去重；在 WebSocket 场景下，`useSSL` 不同也会被视为不同地址键。

#### 4.1.2 TMQ 配置

TMQ 使用 `ConsumerConfig` 中的 `td.connect.ip` 提供多个地址，格式与 WebSocket `host` 保持一致，仍然使用英文逗号分隔。

相关参数如下：

| 参数 | 说明 | 默认值 | 约束 |
| --- | --- | --- | --- |
| `td.connect.ip` | TMQ 连接地址列表 | 无 | 必须为合法地址或地址列表 |
| `td.connect.port` | 兜底端口，补给未显式写端口的地址 | 无 | `0` 到 `65535` |
| `useSSL` | 是否使用 `wss` | `false` | `true` / `false` |
| `ws.autoReconnect` | 是否在连接异常后自动重连 | `false` | `true` / `false` |
| `ws.reconnect.retry.count` | 自动重连轮数 | 仅在开启重连时生效 | 非负整数 |
| `ws.reconnect.interval.ms` | 自动重连间隔（毫秒） | 仅在开启重连时生效 | 非负整数 |

TMQ 端口解析规则如下：

- 地址显式带端口时使用该端口。
- 地址未显式带端口时，使用 `td.connect.port` 作为兜底端口。
- 若仍未指定：
  - `useSSL=true` 时默认 `443`
  - `useSSL=false` 时默认 `6041`

例如：

```csharp
var config = new ConsumerConfig
{
    TDConnectIp = "adapter-a,adapter-b:6050,adapter-c",
    TDConnectPort = "6041",
    TDUseSSL = "false"
};
```

上面的实际端口效果是：

- `adapter-a` -> `6041`
- `adapter-b` -> `6050`
- `adapter-c` -> `6041`

#### 4.1.3 Native 配置

Native 仍然只支持单地址，不支持多地址故障转移。若 `host` 中配置多个地址，则在打开连接时直接抛出参数异常，而不是进入底层连接逻辑。

示例：

```csharp
var builder = new ConnectionStringBuilder(
    "protocol=Native;" +
    "host=localhost:6030,localhost:6031;" +
    "username=root;" +
    "password=taosdata;");

// 该配置会在 Open 时抛出 ArgumentException
using var client = DbDriver.Open(builder);
```

#### 4.1.4 非法配置的前置校验

对于 `ConnectionStringBuilder`：

- `protocol=WebSocket` 且 `host` 为空串、连接字符串中的空白串或 `null` 时，会抛出 `ArgumentException("host value cannot be empty", "host")`
- `protocol=Native` 且 `host` 为空串、连接字符串中的空白串或 `null` 时，不会在 connector 层被改写为 `localhost`；空 `host` 会继续传递到底层 Native C 库，由其按配置文件解析地址
- `port` 超出 `0` 到 `65535` 范围时，会抛出参数异常
- `reconnectRetryCount < 0` 或 `reconnectIntervalMs < 0` 时，会抛出参数异常

这意味着 WebSocket 的无效地址会在连接前失败，而 Native 会保留既有的空 `host` 兼容语义。

### 4.2 WebSocket 客户端行为

#### 4.2.1 初始连接

`WSClient` 创建时会先将 `host` 解析为候选地址列表，然后调用故障转移连接器进行首次连接尝试。首次连接具有以下行为：

- 候选地址为空时视为配置错误
- 当多个地址都可用时，按当前进程内“最少连接”策略选出优先地址
- 当第一个地址不可用时，会继续尝试其余地址
- 首次打开连接失败且所有候选地址均不可达时，抛出 `TDengineError`

示例：

```csharp
var connStr =
    "protocol=WebSocket;" +
    "host=localhost:6041,localhost:6042;" +
    "useSSL=false;" +
    "username=root;" +
    "password=taosdata;" +
    "autoReconnect=true;" +
    "reconnectRetryCount=10;" +
    "reconnectIntervalMs=200;";

using var client = DbDriver.Open(new ConnectionStringBuilder(connStr));
using var rows = client.Query("select server_version()");
```

#### 4.2.2 自动重连与故障转移

当 `autoReconnect=true` 时，WebSocket 客户端在检测到当前连接不可用后，会按以下顺序执行重连：

1. 读取最近一次成功连接的地址作为首选地址
2. 先尝试首选地址
3. 首选地址失败后，再尝试其余候选地址
4. 若本轮仍失败，则按 `reconnectRetryCount` 和 `reconnectIntervalMs` 进入下一轮

参数定义如下：

| 参数 | 说明 | 默认值 | 约束 |
| --- | --- | --- | --- |
| `autoReconnect` | 是否自动重连 | `false` | `true` / `false` |
| `reconnectRetryCount` | 重连轮数 | `3` | 非负整数 |
| `reconnectIntervalMs` | 重连间隔毫秒数 | `2000` | 非负整数 |

如果 `autoReconnect=false`，则客户端不会自动切换到其他地址，调用方直接收到原始异常或连接异常。

#### 4.2.3 受影响的接口

WebSocket 故障转移会影响同一 `WSClient` 实例上的以下行为：

- `Query`
- `Exec`
- `SchemalessInsert`
- `StmtInit` 及后续基于该客户端的 WebSocket Statement 操作

这些接口共享同一个底层连接和重连逻辑。对调用方而言，成功的故障转移不需要重新构造 `WSClient`。

#### 4.2.4 错误码与异常

WebSocket 相关错误通过异常暴露，而不是单独的返回值。

| 类型 / 错误码 | 含义 | 说明 |
| --- | --- | --- |
| `TDengineError` / `0xf005` (`WS_CONNEC_FAILED`) | 初始连接失败 | 所有候选地址都无法建立 WebSocket 连接 |
| `TDengineError` / `0xf001` (`WS_RECONNECT_FAILED`) | 自动重连失败 | 在配置的轮数和间隔内仍无法恢复连接 |
| `ArgumentException` | 参数配置非法 | 例如 WebSocket 空 `host`、非法端口、Native 多地址 |
| `ObjectDisposedException` | 对已释放对象继续操作 | `WSClient.Dispose()` 后再次调用接口 |

服务端返回的数据库错误仍保持原有 `TDengineError` 行为，不因引入故障转移而改变错误码体系。

### 4.3 TMQ 消费者行为

#### 4.3.1 初始连接与订阅

TMQ 消费者在构造时通过 `td.connect.ip` 解析候选地址列表，并使用与 WebSocket 相同的故障转移连接器完成首次连接。建立连接后，`Subscribe`、`Consume`、`Commit` 等操作都在当前活动连接上执行。

示例：

```csharp
var config = new ConsumerConfig
{
    GroupId = "g1",
    ClientId = "c1",
    TDConnectIp = "adapter-a:6041,adapter-b:6041",
    TDConnectUser = "root",
    TDConnectPasswd = "taosdata",
    TDReconnect = "true",
    TDReconnectRetryCount = "10",
    TDReconnectIntervalMs = "200",
    EnableAutoCommit = "true",
    AutoCommitIntervalMs = "5000",
};

using var consumer = new ConsumerBuilder<Dictionary<string, object>>(config).Build();
consumer.Subscribe("topic_demo");
var result = consumer.Consume(1000);
```

#### 4.3.2 自动重连与重新订阅

当 `ws.autoReconnect=true` 时，TMQ 消费者检测到连接不可用后会尝试故障转移：

- 优先尝试最近一次成功连接的地址
- 首选地址不可用时再尝试其他地址
- 连接恢复后，如果消费者之前已经订阅了 topic，会自动重新订阅原有 topic 列表

相关配置如下：

| 参数 | 说明 | 默认值 | 约束 |
| --- | --- | --- | --- |
| `ws.autoReconnect` | 是否允许自动重连 | `false` | `true` / `false` |
| `ws.reconnect.retry.count` | 重连轮数 | 无固定默认值，未开启重连时不使用 | 非负整数 |
| `ws.reconnect.interval.ms` | 重连间隔毫秒数 | 无固定默认值，未开启重连时不使用 | 非负整数 |

如果配置未开启 TMQ 自动重连，则连接中断后不会自动切换到其他地址。

#### 4.3.3 Commit 与消费语义

TMQ 在故障转移场景下保持**至少一次**消费语义，而不是精确一次语义：

- 故障发生时，自动提交可能失败
- 手动 `Commit()` 也可能因为当前连接切换、分组重新分配或网络异常而失败
- 重连后消费者所在 group 的分配结果可能变化
- 因此重复消费是允许的、且属于预期行为

使用方需要基于消息 key、业务主键或幂等写入机制自行去重，不应将“故障转移后绝不重复消费”视为本特性的保证。

#### 4.3.4 错误码与异常

TMQ 与 WebSocket 共享同类错误模型：

| 类型 / 错误码 | 含义 | 说明 |
| --- | --- | --- |
| `TDengineError` / `0xf005` (`WS_CONNEC_FAILED`) | 初始连接失败 | 所有候选地址都不可用 |
| `TDengineError` / `0xf001` (`WS_RECONNECT_FAILED`) | 重连失败 | 达到配置的重连轮数后仍未恢复 |
| `ArgumentException` | 参数配置非法 | 例如非法重连参数、非法端口 |
| `ObjectDisposedException` | 已关闭消费者继续操作 | `Close()` 或释放之后继续调用 |

### 4.4 SQL 行为

本特性**不引入新的 SQL 语法、SQL 关键字或 SQL 处理语义**。故障转移仅影响客户端如何建立和恢复连接，不改变 SQL 本身的执行方式。

例如，在故障转移前后，以下语句的写法保持不变：

```sql
select server_version();
```

```sql
insert into power.meters values (now, 220.1, 10.2, 1);
```

### 4.5 发布与部署行为

本特性不要求新增安装脚本、部署脚本或新的二进制组件。用户侧部署变化体现在：

- WebSocket/TMQ 推荐至少配置两个可访问的 taosAdapter 地址
- 若需要 TLS，应同步在所有候选地址上启用 `wss`
- 若多个地址指向不同 adapter 实例，应保证这些实例后端连接的是相同的数据服务与元数据环境

## 5 性能

正常无故障场景下，本特性对查询、写入和消费的主路径开销很小：

- 地址解析仅发生在客户端初始化或重连时，不在每条 SQL 上重复执行
- “最少连接”选择只在建立连接时访问一次进程内缓存
- 连接计数使用进程内锁保护，适用于常规客户端并发规模

在故障场景下，恢复时间主要受以下因素影响：

- 候选地址数量
- 单次连接超时时间
- `reconnectRetryCount`
- `reconnectIntervalMs`

因此，故障恢复的最坏时延大致与“每轮候选地址尝试总耗时 + 重试间隔 × 重试轮数”成正比。该代价换取的是连接可用性和客户端侧的多实例容灾能力。

本特性还会在同一进程内尽量把新连接分散到多个地址，从而缓解单一 adapter 的热点连接压力，但它不是跨进程、跨机器的全局负载均衡。

## 6 安全

本特性不改变认证与授权模型，仍沿用现有的：

- 用户名 / 密码认证
- bearer token
- WebSocket token 参数
- `useSSL=true` 时的 TLS 传输保护

安全注意事项如下：

- 多地址配置中的所有地址都将被视为可信候选节点，客户端可能向这些地址发送认证信息，因此只能配置同一信任域内的 taosAdapter 实例
- 跨公网或不可信网络场景建议开启 `useSSL=true`
- 故障转移不会绕过服务端权限控制，也不会降低数据库对象级权限校验

## 7 兼容性

本特性对现有单地址使用方式保持兼容：

- WebSocket 单地址连接保持原有行为
- TMQ 单地址连接保持原有行为
- 未开启自动重连时，WebSocket/TMQ 仍按原有方式在异常时直接返回失败

需要明确的兼容性变化如下：

- Native 协议继续不支持多地址，但现在会更早、更明确地拒绝多地址配置
- WebSocket 的空 `host` 现在会在地址解析阶段直接报错，而不是把空地址继续传递到后续网络层
- Native 的空 `host` 继续透传到底层连接库，不改写为 `localhost`

上述变化只影响原本就非法或不受支持的配置，不影响合法配置的兼容性。

## 8 运维

为了让故障转移真正生效，运维侧需要满足以下条件：

- 至少部署两个可访问的 taosAdapter 实例
- 所有候选地址都能访问相同的后端 TDengine 服务
- TMQ 场景下，所有候选地址看到的 topic、database 和 group 状态应保持一致
- `reconnectRetryCount` 与 `reconnectIntervalMs` 需要结合网络状况、进程探活和维护窗口策略调优

如果候选地址背后的服务并不等价，本特性只会做到“连接切换”，不能保证切换后的行为一致性。

## 9 使用场景

本特性适用于以下场景：

1. WebSocket 查询客户端需要在多个 taosAdapter 实例之间容灾
2. WebSocket 写入客户端需要在一个 adapter 不可用时自动切到另一个实例
3. TMQ 消费者需要在 adapter 重启、升级或短时故障后继续消费
4. 业务方没有额外的外部负载均衡器，希望由客户端直接配置多个地址
5. 多个客户端实例运行在同一进程或服务内，希望在若干 adapter 节点之间做粗粒度连接分散
6. 计划维护期间，先启动新 adapter、再下线旧 adapter，希望客户端能够自动恢复

## 10 约束和限制

约束：

- Native 协议不支持多地址故障转移
- WebSocket 多地址通过 `host` 配置，TMQ 多地址通过 `td.connect.ip` 配置
- `reconnectRetryCount`、`reconnectIntervalMs`、`td.connect.port` 必须为合法非负整数
- 使用多地址时，各地址应对应同一业务环境

限制：

- “最少连接”只在**当前进程内**生效，不是全局负载均衡
- 客户端不会探测地址背后的业务一致性，只按“能否建立连接”做选择
- TMQ 故障转移后不保证精确一次消费，重复消费允许出现
- Native 不参与 failover
- 文档描述的故障转移主要覆盖 WebSocket 与 TMQ 路径，不改变其他非 WebSocket 通信协议的高可用能力

## 11 常见错误和排查

| 现象 / 提示 | 可能原因 | 排查建议 |
| --- | --- | --- |
| `ArgumentException: host value cannot be empty` | WebSocket 的 `host` 为空、只有空格或被置为 `null` | 检查连接字符串或代码里对 `Host` 的赋值 |
| `ArgumentException: native protocol does not support multiple host addresses` | Native 使用了多地址 | 将 Native 改为单地址，或改用 WebSocket 协议 |
| `invalid reconnect retry count value` / `invalid reconnect interval value` | 重连参数不是非负整数 | 校验配置来源和配置中心值 |
| `code:[0xf005],error:websocket connection failed` | 首次连接时所有候选地址都不可达 | 逐个检查端口、网络和 taosAdapter 进程状态 |
| `code:[0xf001],error:websocket connection reconnect failed...` | 运行中连接断开后，重连轮数耗尽 | 检查目标地址是否都不可用，或适当增加重试轮数与间隔 |
| TMQ 在故障转移后出现重复消息 | offset 提交失败或 group 重新分配 | 这是至少一次语义下的预期行为，业务侧应保证幂等 |

建议的排查顺序：

1. 先确认候选地址列表是否正确、端口是否匹配
2. 再确认 `useSSL` 与实际服务端协议是否一致
3. 之后确认 taosAdapter 是否已启动并可从客户端网络访问
4. 最后根据异常类型判断是“首次连接失败”还是“运行中重连失败”

## 12 可观测性

本特性不直接修改 taos shell、taos Explorer、TDinsight 或其他 UI 组件行为。

对用户而言，可观测性变化主要体现在：

- 连接失败或重连失败时，异常信息会体现 WebSocket 初始连接失败或重连失败
- TMQ 故障转移后的行为可以通过消费是否继续推进、是否出现重复消费来侧面判断

当前实现没有新增独立的 UI 展示项，也没有新增专门的可视化指标输出接口。

## 13 安装和卸载

本特性不要求修改安装或卸载脚本，也不要求新增额外组件。

使用要求如下：

- 安装包含该功能版本的连接器
- 按需把连接配置改为多地址形式
- 若需要自动重连，显式开启对应参数

卸载行为与现有版本一致，无额外清理步骤。

## 14 文档

是否需要修改企业版文档：需要。

## 15 参考文档

## 16 附录

本次实现的核心机制如下：

1. `HostEndpointParser` 负责把 `host` / `td.connect.ip` 解析为标准化地址
2. `FailoverAddress` 保存地址信息与缓存键
3. `FailoverAddressLease` 在连接持有期间维护当前进程内的地址连接计数
4. `FailoverConnector.TryOpen(...)` 负责在候选地址之间尝试连接，并结合重试参数执行故障转移
5. `WSClient` 和 `TMQ Consumer` 基于同一套故障转移机制进行首次连接和重连
6. Native 客户端不接入该故障转移机制，只做单地址校验和单地址连接

从实现角度看，本功能属于**客户端侧连接管理增强**，不改变 SQL、协议包格式或服务端元数据模型。
