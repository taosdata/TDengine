# JDBC Connector adapter HA FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-15 | 2026-05-20 | 1.0 | Sheyanjie | 初稿，新增 JDBC WebSocket SQL 与 TMQ adapter HA 动态实例发现和故障切换行为说明 |

## 2. 背景

taosAdapter 在 TSDB v3.4.2 中支持在 WebSocket SQL 连接和 TMQ 订阅响应中按需返回 adapter 实例列表。JDBC Connector 需要利用该能力，在用户仅配置一个种子 adapter 地址时发现同一 adapter 集群内的其他实例，并在连接建立、后续新连接和自动重连场景中使用完整 adapter 节点列表。

本次优化目标：

1. JDBC WebSocket SQL 连接和 WebSocket TMQ 订阅支持通过配置项启用 adapter HA。
2. 启用后，请求 adapter 返回 `list_instances`，并将返回的实例地址合并到当前连接的 endpoint 列表。
3. 将发现到的完整 adapter 集群信息登记到 JDBC 现有 `RebalanceManager`，使后续仅配置种子节点的新连接在真正建连前扩展为完整集群节点列表，并继续使用最小连接数算法选择目标节点。
4. 连接或订阅命中的种子 adapter 下线后，结合既有 `enableAutoReconnect` 能力切换到已发现的其他 adapter 实例。

## 3. 定义

1. **adapter HA**：JDBC Connector 通过 adapter 返回的实例列表进行 adapter 侧服务发现、故障切换和连接分布优化的能力。
2. **种子 endpoint**：用户在 JDBC URL、`endpoints` 或 TMQ `bootstrap.servers` 中显式配置的 adapter 地址。
3. **发现 endpoint**：adapter 成功响应中的 `list_instances` 字段返回的 `host:port` 地址。
4. **完整 cluster**：由种子 endpoint 与发现 endpoint 合并得到的 adapter 实例集合。
5. **`adapterHA` 配置项**：JDBC Connector 新增布尔配置项，控制是否启用 adapter HA，默认值为 `false`。

## 4. 行为说明

### 4.1 配置项

| 配置项 | 类型 | 默认值 | 适用范围 | 说明 |
| --- | --- | --- | --- | --- |
| `adapterHA` | boolean | `false` | JDBC WebSocket SQL、WebSocket TMQ | 是否请求 adapter 返回实例列表并启用 JDBC 侧 adapter HA |

SQL WebSocket URL 示例：

```text
jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata&adapterHA=true&enableAutoReconnect=true
```

SQL WebSocket Properties 示例：

```java
Properties properties = new Properties();
properties.setProperty("user", "root");
properties.setProperty("password", "taosdata");
properties.setProperty("adapterHA", "true");
properties.setProperty("enableAutoReconnect", "true");
Connection connection = DriverManager.getConnection("jdbc:TAOS-WS://localhost:6041/", properties);
```

TMQ Properties 示例：

```java
Properties properties = new Properties();
properties.setProperty("td.connect.type", "ws");
properties.setProperty("bootstrap.servers", "localhost:6041");
properties.setProperty("group.id", "g1");
properties.setProperty("td.connect.user", "root");
properties.setProperty("td.connect.pass", "taosdata");
properties.setProperty("adapterHA", "true");
properties.setProperty("enableAutoReconnect", "true");
TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties);
```

### 4.2 SQL WebSocket 连接行为

启用 `adapterHA=true` 后，JDBC 构造 WebSocket `conn` 请求时在 `args` 中增加：

```json
{
  "list_instances": true
}
```

连接认证成功后，JDBC 从 `ConnectResp.list_instances` 读取 adapter 返回的实例列表，并执行以下处理：

1. 如果 `adapterHA=false`，忽略响应中的实例列表，保持旧行为。
2. 如果 `adapter` 调用 `list_instances` 失败，则不会返回列表，仍然返回连接成功，此时 JDBC 仍可以正常连接，但是无法获取到 `list_instances`，但不影响后续连接使用。
3. 如果响应缺少 `list_instances` 或列表为空，连接保持成功，JDBC 记录告警日志并关闭动态 endpoint 发现能力。
4. 如果列表中包含当前已存在的 endpoint，跳过重复项。
5. 如果列表中包含空字符串、无法解析的地址或一次解析出多个 endpoint 的字符串，跳过该项并记录 debug 日志。
6. 对新增 endpoint 创建对应 `WSClient`，追加到当前 `WSConnectionManager` 的 client 列表。
7. 合并后的完整 endpoint 列表写回 `ConnectionParam.endpoints`，并通过 `RebalanceManager.expandCluster` 登记完整 adapter cluster。

### 4.3 TMQ WebSocket 订阅行为

启用 `adapterHA=true` 后，JDBC 构造 TMQ `subscribe` 请求时在 `args` 中增加：

```json
{
  "list_instances": true
}
```

订阅成功后，JDBC 从 `SubscribeResp.list_instances` 读取 adapter 返回的实例列表，并复用 SQL WebSocket 的 endpoint 合并逻辑。TMQ 配置解析中将 `adapterHA` 识别为 JDBC 内部配置项，不会转发到 TMQ `config` 自定义参数中。

TMQ 重连场景沿用既有逻辑：底层 WebSocket 重连成功后重新执行 `subscribe`，重新订阅成功时可再次合并 adapter 返回的实例列表。

### 4.4 已知 cluster 扩展行为

`RebalanceManager` 新增两类能力：

1. `expandCluster(List<Endpoint>)`：当某个连接发现完整 adapter 列表后，将原有种子 cluster 扩展为完整 cluster，并保留已有 endpoint 的连接计数、在线状态和 rebalancing 状态。
2. `expandEndpointsIfKnown(List<Endpoint>)`：后续新连接构造 `WSConnectionManager` 时，如果用户配置的种子 endpoint 已属于某个更大的已知 cluster，则在建连前将当前连接的 endpoint 列表扩展为该完整 cluster。
3. 目前没有做复杂的 shrink 逻辑，后续会考虑两个优化方向：定期轮训 adapter 端点列表或者采用订阅方式订阅 adapter 端点列表变化，避免后台探活的复杂性和集群节点变化。

该流程保证：第一个启用 adapter HA 的连接完成实例发现后，后续仅配置种子 endpoint 的连接也会在 `connectWithMinimumCount()` 前获得完整 endpoint 列表，并继续按现有最小连接数算法选择负载较低的 adapter。已下线节点不会增加连接开销，因为下线后其节点状态改变，后面新建连接就不会选取已下线节点。



### 4.5 故障切换行为

adapter HA 本身只负责 endpoint 发现和 cluster 扩展；连接故障切换依赖既有自动重连能力。

推荐同时配置：

```text
adapterHA=true
enableAutoReconnect=true
reconnectIntervalMs=500
reconnectRetryCount=3
messageWaitTimeout=2000
httpConnectTimeout=2000
```

在 SQL WebSocket 写入场景中，种子 adapter 下线后，JDBC 可在后续请求触发连接异常时从完整 endpoint 列表中选择其他在线 adapter 进行重连和认证。

在 TMQ 订阅场景中，种子 adapter 下线后，`poll` 捕获连接关闭或查询超时后触发 TMQ 重连，并在重连成功后重新订阅 topic。

### 4.6 错误处理

| 场景 | 行为 |
| --- | --- |
| `adapterHA=false` | 不发送 `list_instances`，不解析响应实例列表 |
| 服务端为旧版本，响应无 `list_instances` | 当前连接或订阅保持成功，记录告警，动态发现不生效 |
| 响应实例列表为空 | 当前连接或订阅保持成功，记录告警，动态发现不生效 |
| 响应中某个实例地址无法解析 | 忽略该实例，继续处理其他合法实例 |
| 响应实例与现有 endpoint 重复 | 忽略重复实例 |
| 所有发现实例均无效 | 当前连接或订阅保持成功，endpoint 列表不变化 |
| 自动重连失败 | 沿用既有 WebSocket/TMQ 连接关闭错误 |

## 5. 性能

1. 默认路径无额外开销：`adapterHA=false` 时不发送 `list_instances`，不做动态 endpoint 合并。
2. 启用后，每次 SQL 连接认证或 TMQ 订阅请求会多携带一个布尔字段，响应解析多处理一个字符串数组字段。
3. endpoint 合并仅发生在连接或订阅成功路径，复杂度与 adapter 实例数量线性相关。
4. 后续新连接在构造阶段查询已知 cluster 并扩展 endpoint 列表，复杂度与 endpoint 数量线性相关。
5. 查询、写入、poll、fetch 和 commit 的正常执行路径不额外调用 adapter 实例查询接口。

## 6. 安全

1. 不新增认证方式，不改变用户密码、Bearer Token、白名单、SSL 和连接选项校验。
2. `list_instances` 返回的是 adapter 实例访问地址，不包含用户数据、SQL 文本、topic 数据或凭据信息。
3. 默认关闭 adapter HA，只有用户显式设置 `adapterHA=true` 时才请求返回实例列表。
4. 日志仅记录实例发现缺失或非法 endpoint 解析情况，不记录密码或 token。

## 7. 兼容性

1. **默认兼容**：`adapterHA` 默认值为 `false`，旧应用不受影响。
2. **旧 adapter 兼容**：旧 adapter 忽略未知请求字段或不返回 `list_instances` 时，JDBC 连接和订阅仍成功，只是不启用动态 endpoint 发现。
3. **新 adapter 兼容旧客户端**：旧 JDBC 不发送 `list_instances`，adapter 按默认行为不返回实例列表。
4. **配置兼容**：JDBC 配置项名称为 `adapterHA`。通过 URL 参数启用时也应使用 `adapterHA=true`。
5. **TMQ 兼容**：`adapterHA` 被识别为 JDBC 内部配置，不作为 TMQ 自定义 config 下发。

## 8. 运维

1. 不需要修改 TDengine 服务端配置。
2. 需要 adapter 侧支持 `list_instances` 响应扩展，并保证 adapter 实例注册信息中包含可访问的 `host:port`。
3. 如果只配置一个种子 adapter，建议确保该种子 adapter 在首次连接时可用，以便完成实例发现。
4. 生产环境建议同时开启 `enableAutoReconnect`，否则 adapter HA 只能扩展连接候选列表，不能在连接断开后自动恢复。

## 9. 使用场景

1. **SQL 写入故障切换**：应用只配置 `localhost:6041`，连接成功后发现 `localhost:6042`、`localhost:6044`。当 `localhost:6041` 停止后，后续写入触发自动重连并切换到其他 adapter。
2. **TMQ 订阅故障切换**：消费者通过种子 adapter 订阅 topic，订阅成功后发现其他 adapter。种子 adapter 停止后，`poll` 触发重连并重新订阅，消费继续进行。
3. **后续连接负载均衡**：第一个连接发现完整 cluster 后，后续连接即使仍只配置种子 endpoint，也会在建连前扩展为完整 endpoint 列表，并按最小连接数选择目标 adapter。
4. **旧版本灰度**：在混部旧 adapter 时，命中旧版本 adapter 的连接仍可成功，只是响应无 `list_instances`，JDBC 记录告警后按普通连接运行。

## 10. 约束和限制

1. 仅支持 JDBC WebSocket SQL 和 WebSocket TMQ 路径，不影响原生连接、REST 或其他连接方式。
2. JDBC `Connection` 和 `TaosConsumer` 不要求支持多线程并发访问；同一连接或消费者实例应由调用方串行使用。
3. `list_instances` 是连接或订阅时刻的快照，不提供持续推送；后续 adapter 拓扑变化需要通过新的连接或订阅响应再次发现。
4. `expandCluster` 保守地拒绝 shrink：如果传入 endpoint 列表不是已有 cluster 的超集，不会缩小已知 cluster。
5. TMQ 不支持 `slaveClusterHost`；SQL WebSocket 在配置 slave cluster 时不执行已知 cluster 扩展。

## 11. 常见错误和排查

| 现象 | 可能原因 | 排查建议 |
| --- | --- | --- |
| endpoint 列表没有扩展 | 未设置 `adapterHA=true` | 检查 JDBC URL 或 Properties 中配置项名称和值 |
| endpoint 列表没有扩展 | adapter 响应没有 `list_instances` | 确认 adapter 版本是否支持实例列表返回 |
| endpoint 列表没有扩展 | adapter 返回空列表 | 检查 adapter 实例注册状态和注册地址 |
| 发现列表中部分节点缺失 | 返回地址格式非法或重复 | 查看 JDBC debug 日志中的非法 endpoint 解析信息 |
| 种子 adapter 停止后连接未恢复 | 未开启 `enableAutoReconnect` 或重试次数不足 | 检查自动重连相关参数 |
| TMQ 消费中断后未继续 | 重连后重新订阅失败 | 查看 TMQ subscribe 返回码和异常信息 |

## 12. 可观测性

1. 当 `adapterHA=true` 但响应未包含 `list_instances` 时，`Transport` 会记录 warn 日志。
2. 非法 adapter HA endpoint 会记录 debug 日志。
3. RebalanceManager 仍按既有逻辑维护 endpoint 连接数、在线状态和 rebalancing 状态。
4. 本功能不改变 taos shell、taos Explorer、TDinsight 的 UI 行为。

## 13. 安装和卸载

无。该能力随 JDBC Connector 发布，不涉及安装脚本、卸载脚本、系统表或数据迁移。

## 14. 文档

1. 需要在 JDBC Connector WebSocket 连接参数文档中补充 `adapterHA`。
2. 需要在 JDBC Connector WebSocket TMQ 配置说明中补充 `adapterHA`。
3. 需要说明 `adapterHA` 依赖 adapter 支持 `list_instances`，旧版本 adapter 可兼容但不提供动态发现。

## 15. 参考文档

1. `TSDB-v3.4.2-[20260630]/05-设计文档/taosadapter-list-instances-FS.md`
2. `src/main/java/com/taosdata/jdbc/TSDBDriver.java`
3. `src/main/java/com/taosdata/jdbc/common/ConnectionParam.java`
4. `src/main/java/com/taosdata/jdbc/ws/WSConnectionManager.java`
5. `src/main/java/com/taosdata/jdbc/ws/loadbalance/RebalanceManager.java`
6. `src/main/java/com/taosdata/jdbc/ws/entity/ConnectReq.java`
7. `src/main/java/com/taosdata/jdbc/ws/entity/ConnectResp.java`
8. `src/main/java/com/taosdata/jdbc/ws/tmq/entity/TMQRequestFactory.java`
9. `src/main/java/com/taosdata/jdbc/ws/tmq/entity/SubscribeReq.java`
10. `src/main/java/com/taosdata/jdbc/ws/tmq/entity/SubscribeResp.java`

## 16. 附录

### 16.1 关键流程

```text
用户配置 adapterHA=true
  -> JDBC conn/subscribe 请求携带 list_instances=true
  -> adapter 成功响应 list_instances
  -> JDBC 解析并合并 endpoint
  -> RebalanceManager.expandCluster 记录完整 cluster
  -> 后续新连接 expandEndpointsIfKnown
  -> connectWithMinimumCount 按完整 endpoint 列表选择连接数最小节点
```
