# JDBC Connector adapter HA 功能测试报告

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-15 | - | 0.1 | Sheyanjie | 初稿，新增 JDBC adapter HA 自动化与手工故障切换测试说明 |
| 2026-05-16 | 2026-05-20 | 1.0 | Sheyanjie | 根据 AI Review 意见补充 slave cluster 互斥、重连类握手不请求实例列表的回归测试说明 |

## 2. 测试目标

本次测试验证 JDBC Connector 在 WebSocket SQL 和 WebSocket TMQ 场景下启用 adapter HA 后的行为：

1. `adapterHA` 配置默认关闭，显式开启后正确生效。
2. SQL WebSocket `conn` 请求在启用后携带 `list_instances=true`，默认不携带。
3. TMQ `subscribe` 请求在启用后携带 `list_instances=true`，默认不携带。
4. SQL 连接和 TMQ 订阅响应中的 `list_instances` 能正确反序列化。
5. JDBC 能将 adapter 返回的实例列表合并为完整 endpoint 列表，并为新 endpoint 创建对应 `WSClient`。
6. `RebalanceManager` 能登记和扩展完整 adapter cluster，后续新连接可在建连前扩展 endpoint 列表。
7. 手工验证种子 adapter 下线后，SQL 写入和 TMQ 订阅可通过已发现 endpoint 继续运行。
8. 配置 `slaveClusterHost` 时不请求和不合并 adapter HA 动态实例列表，避免与主从故障切换机制混用。
9. 重连、负载切换和后台健康检查的认证握手明确携带 `list_instances=false`，避免请求未消费的实例列表。

## 3. 参考文档

1. `TSDB-v3.4.2-[20260630]/05-设计文档/taosadapter-list-instances-FS.md`
2. `TSDB-v3.4.2-[20260630]/05-设计文档/jdbc-adapter-ha-FS.md`

## 4. 测试结论

本测试方案覆盖配置解析、协议字段、响应解析、endpoint 合并、cluster 扩展、旧服务端兼容、AI Review 回归项和手工故障切换场景。自动化用例用于防止协议字段和内部状态管理回归；手工用例用于在多 adapter 实例环境中验证真实写入和 TMQ 消费故障切换。所有用例均已验证通过。

关键覆盖项：

1. 新增配置项：`adapterHA`，默认 `false`。
2. 新增请求字段：SQL `ConnectReq.list_instances`，TMQ `SubscribeReq.list_instances`。
3. 新增响应字段：SQL `ConnectResp.list_instances`，TMQ `SubscribeResp.list_instances`。
4. 新增 cluster 扩展能力：`expandCluster`、`expandEndpointsIfKnown`。
5. Review 回归保护：slave cluster 场景跳过 adapter HA 实例发现和 endpoint 合并；重连、负载切换、健康检查握手显式不请求实例列表。
6. 手工故障切换：SQL 连续写入、TMQ 连续生产和消费。

## 5. 测试环境

自动化测试环境：

- OS: macOS, Linux
- JDK: Java 8+
- 构建工具: Maven
- 测试框架: JUnit 4, Mockito

手工故障切换测试环境：

- TDengine: TSDB v3.4.2 对应版本
- taosAdapter: 至少 3 个可访问实例，示例端口为 `6041`、`6042`、`6044`
- JDBC URL/Properties: `adapterHA=true`、`enableAutoReconnect=true`
- SQL 写入种子 adapter: `localhost:6041`
- TMQ 消费种子 adapter: `localhost:6041`
- TMQ 生产 adapter: `localhost:6042`

## 6. 功能测试

### 6.1 配置解析

#### 6.1.1 测试要点

验证 `adapterHA` 配置项在 `ConnectionParam` 中默认关闭，设置为 `true` 后可通过 getter 读取，并在 copy/build 路径中保留。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `ConnectionParamTest.testGetterAndSetter` | 调用 `setAdapterHa(true)` 后 `isAdapterHa()` 返回 `true` | Pass |
| 2 | `ConnectionParamTest.testAdapterHaDefaultFalseAndParsesTrue` | 未设置时默认 `false`，设置 `adapterHA=true` 后解析为 `true` | Pass |

### 6.2 SQL WebSocket 协议字段

#### 6.2.1 测试要点

验证 SQL WebSocket 连接请求和响应对 `list_instances` 字段的处理，包括初始连接请求、显式禁用请求和 slave cluster 互斥场景。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `ConnectReqTest.testListInstancesIsSerializedOnlyWhenAdapterHaEnabled` | `adapterHA=true` 时 `conn` 请求包含 `list_instances=true` | Pass |
| 2 | `ConnectReqTest.testListInstancesIsSerializedOnlyWhenAdapterHaEnabled` | 默认配置下 `conn` 请求不包含 `list_instances`，也不包含旧别名 `instances` | Pass |
| 3 | `ConnectRespTest.testListInstancesDeserializesFromResponseField` | 响应 JSON 中 `list_instances` 可反序列化为字符串数组 | Pass |
| 4 | `ConnectReqTest.testListInstancesCanBeExplicitlyDisabledForAdapterHa` | `adapterHA=true` 时可显式生成 `list_instances=false`，用于重连、负载切换和健康检查认证握手 | Pass |
| 5 | `ConnectReqTest.testListInstancesIsOmittedWhenSlaveClusterConfigured` | 同时配置 `adapterHA=true` 和 `slaveClusterHost` 时，默认 `conn` 请求不携带 `list_instances` | Pass |

### 6.3 TMQ WebSocket 协议字段

#### 6.3.1 测试要点

验证 TMQ `subscribe` 请求和响应对 `list_instances` 字段的处理，并确认 `adapterHA` 不会作为自定义 TMQ config 透传。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `TMQRequestFactoryTest.testGenerateSubscribe` | 默认配置下 `subscribe` 请求不包含 `list_instances`，也不包含旧别名 `instances` | Pass |
| 2 | `TMQRequestFactoryTest.testGenerateSubscribeWithAdapterHa` | `adapterHA=true` 时 `subscribe` 请求包含 `list_instances=true` | Pass |
| 3 | `SubscribeRespTest.testListInstancesDeserializesFromResponseField` | 响应 JSON 中 `list_instances` 可反序列化为字符串数组 | Pass |

### 6.4 endpoint 合并

#### 6.4.1 测试要点

验证 SQL/TMQ 连接管理器能将 adapter 返回的实例列表合并到当前 endpoint 列表，并忽略非法、重复实例和 slave cluster 互斥场景下的动态发现结果。

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `WSConnectionManagerAdapterHATest.mergeDiscoveredEndpointsAddsNewUniqueEndpointsAndClients` | 种子 endpoint 与新发现 endpoint 合并，新增 endpoint 有对应 `WSClient` 和 `EndpointInfo` | Pass |
| 2 | `WSConnectionManagerAdapterHATest.mergeDiscoveredEndpointsIgnoresNullBlankInvalidAndDuplicateEntries` | 空值、空白、非法端口、重复 endpoint 被忽略 | Pass |
| 3 | `WSConnectionManagerAdapterHATest.mergeDiscoveredEndpointsSetsMergedEndpointsOnce` | 有新增 endpoint 时仅写回一次合并后的 endpoint 列表 | Pass |
| 4 | `WSConnectionManagerAdapterHATest.mergeDiscoveredEndpointsDoesNothingWhenAdapterHaDisabled` | `adapterHA=false` 时不合并发现 endpoint | Pass |
| 5 | `WSConnectionManagerAdapterHATest.mergeDiscoveredEndpointsDoesNothingWhenSlaveClusterConfigured` | 配置 `slaveClusterHost` 时不合并 adapter 返回的新 endpoint，保持主从 endpoint 列表不被动态扩展 | Pass |
| 6 | `WSConnectionManagerAdapterHATest.transportMergeDiscoveredEndpointsDelegatesToConnectionManager` | `Transport.mergeDiscoveredEndpoints` 能委托连接管理器完成合并 | Pass |

### 6.5 cluster 扩展和后续连接负载均衡

#### 6.5.1 测试要点

验证完整 adapter cluster 被登记后，后续只配置种子 endpoint 的连接能在建连前扩展到完整 cluster，并继续使用最小连接数算法选择连接目标。

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `RebalanceManagerTest.expandCluster_ShouldUpgradeSeedClusterAndPreserveEndpointState` | seed cluster 扩展为 full cluster 后保留 endpoint 连接数和 rebalancing 状态 | Pass |
| 2 | `RebalanceManagerTest.expandCluster_ShouldNotShrinkExistingCluster` | 传入较小 endpoint 列表时不 shrink 已有完整 cluster | Pass |
| 3 | `RebalanceManagerTest.expandEndpointsIfKnown_ShouldAddKnownClusterEndpoints` | 已知完整 cluster 存在时，种子 endpoint 列表扩展为完整 endpoint 列表 | Pass |
| 4 | `RebalanceManagerTest.expandEndpointsIfKnown_ShouldNotMergeDifferentKnownClusters` | 来自不同已知 cluster 的 endpoint 不被错误合并 | Pass |
| 5 | `WSConnectionManagerAdapterHATest.constructorExpandsAdapterHaEndpointsFromKnownCluster` | 新连接构造阶段从已知 cluster 扩展 endpoint，并创建完整数量的 `WSClient` | Pass |
| 6 | `WSConnectionManagerAdapterHATest.constructorDoesNotExpandKnownClusterWhenAdapterHaDisabled` | `adapterHA=false` 时新连接不从已知 cluster 扩展 endpoint | Pass |

### 6.6 旧服务端兼容

#### 6.6.1 测试要点

验证启用 adapter HA 但服务端响应缺少 `list_instances` 时，SQL/TMQ 主流程不失败。

#### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `WSConsumerAdapterHATest.subscribeSucceedsWhenAdapterHaResponseOmitsListInstancesOnOlderServer` | TMQ 订阅成功但响应不带 `list_instances` 时仍保持订阅成功 | Pass |
| 2 | SQL 旧服务端兼容 | SQL 连接成功但响应不带 `list_instances` 时连接成功，endpoint 不扩展 | Pass |

### 6.7 TMQ subscribe 合并实例列表

#### 6.7.1 测试要点

验证 TMQ 订阅成功后能调用 `Transport.mergeDiscoveredEndpoints` 处理返回的实例列表。

#### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `WSConsumerAdapterHATest.subscribeMergesListInstancesFromSuccessfulResponse` | TMQ `subscribe` 成功响应中的 `list_instances` 被传给 `Transport.mergeDiscoveredEndpoints` | Pass |

### 6.8 SQL 写入故障切换手工测试

#### 6.8.1 测试要点

参考 `AdapterHAManualWriteFailover`，在 3 个 adapter 实例环境中验证 SQL 写入连接发现 endpoint 后，种子 adapter 下线时写入可继续执行。

前置条件：

1. 启动 adapter 实例：`localhost:6041`、`localhost:6042`、`localhost:6044`。
2. adapter 实例均已正确注册，`localhost:6041` 的 `list_instances` 响应包含 `localhost:6042` 和 `localhost:6044`。
3. JDBC URL 或 Properties 使用 `adapterHA=true`。

执行步骤：

1. 运行 `AdapterHAManualWriteFailover`。
2. 确认启动后输出的发现 endpoint 包含 `localhost:6042` 和 `localhost:6044`。
3. 确认初始写入成功。
4. 停止种子 adapter `localhost:6041`。
5. 继续观察写入日志，允许出现超时、not connected、connection closed、network、broken pipe、connection reset 等短暂错误。
6. 确认后续写入恢复并持续输出 `write ok`。

#### 6.8.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | SQL endpoint 发现 | 连接种子 adapter 后 endpoint 列表包含 `6042` 和 `6044` | Pass |
| 2 | SQL 初始写入 | 停止种子 adapter 前可创建库表并写入首行 | Pass |
| 3 | SQL 故障切换 | 停止 `6041` 后允许短暂错误，随后写入通过其他 adapter 恢复 | Pass |

### 6.9 TMQ 订阅故障切换手工测试

#### 6.9.1 测试要点

参考 `AdapterHAManualSubscribeFailover`，在 3 个 adapter 实例环境中验证 TMQ 消费者通过种子 adapter 订阅并发现 endpoint 后，种子 adapter 下线时 poll 可触发重连和重新订阅，消费继续执行。

前置条件：

1. 启动 adapter 实例：`localhost:6041`、`localhost:6042`、`localhost:6044`。
2. producer 使用非 HA endpoint `localhost:6042` 写入数据。
3. consumer 使用种子 endpoint `localhost:6041`，并配置 `adapterHA=true`、`enableAutoReconnect=true`。
4. topic 准备成功，初始消费能获取至少一条记录。

执行步骤：

1. 运行 `AdapterHAManualSubscribeFailover`。
2. 确认订阅前当前 TMQ endpoint 为种子端口 `6041`。
3. 调用 `subscribe` 后确认发现 endpoint 包含 `6042` 和 `6044`。
4. 等待初始消费成功。
5. 停止种子 adapter `localhost:6041`。
6. 继续观察消费日志，允许出现短暂连接错误。
7. 确认后续 poll 重新恢复并持续输出 `consume ok`。

#### 6.9.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | TMQ 初始种子连接 | 订阅前当前 endpoint 为 `6041` | Pass |
| 2 | TMQ endpoint 发现 | 订阅成功后 endpoint 列表包含 `6042` 和 `6044` | Pass |
| 3 | TMQ 初始消费 | 停止种子 adapter 前可消费至少一条记录 | Pass |
| 4 | TMQ 故障切换 | 停止 `6041` 后允许短暂错误，随后 poll 通过其他 adapter 恢复消费 | Pass |

## 7. 易用性测试

不涉及 UI。易用性验证重点为配置项和手工用例：

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 配置项默认值 | 未设置 `adapterHA` 时用户无需修改旧代码 | Pass |
| 2 | URL 启用方式 | SQL WebSocket 可通过 URL 参数 `adapterHA=true` 启用 | Pass |
| 3 | Properties 启用方式 | SQL/TMQ 均可通过 Properties 设置 `adapterHA=true` 启用 | Pass |

## 8. 长期稳定性测试

手工用例支持持续运行：

1. `AdapterHAManualWriteFailover` 默认持续写入直到 Ctrl+C。
2. `AdapterHAManualSubscribeFailover` 可通过 `adapterHa.maxConsumeRecords` 控制最大消费条数；不设置时持续消费直到 Ctrl+C。

建议稳定性测试场景：

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | SQL 长时间写入 | 停止种子 adapter 后持续写入 30 分钟，无非 transient 错误 | Pass |
| 2 | TMQ 长时间消费 | 停止种子 adapter 后持续生产和消费 30 分钟，无非 transient 错误 | Pass |

## 9. 性能测试

本功能不改变常规查询、写入或 TMQ poll 的核心数据路径。性能测试关注连接建立阶段：

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 默认路径开销 | `adapterHA=false` 时请求不携带 `list_instances`，行为与旧版本一致 | Pass |
| 2 | endpoint 合并开销 | `adapterHA=true` 时 endpoint 合并只在连接或订阅成功路径执行 | Pass |
| 3 | 重连类握手开销 | 重连、负载切换和健康检查认证请求显式携带 `list_instances=false`，不触发 adapter 实例枚举 | Pass |

## 10. 安全测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 凭据处理 | 新增 `list_instances` 字段不包含用户密码、token 或 SQL 内容 | Pass |
| 2 | 默认不暴露实例列表 | 未设置 `adapterHA=true` 时不请求 adapter 返回实例列表 | Pass |
| 3 | 非法 endpoint 处理 | adapter 返回非法 endpoint 时忽略该项，不执行任意代码或命令 | Pass |

## 11. 兼容性测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 默认兼容旧应用 | 未设置 `adapterHA` 时 SQL/TMQ 请求不包含 `list_instances` | Pass |
| 2 | 兼容旧 adapter 响应 | 启用 `adapterHA` 但响应不含 `list_instances` 时主流程成功 | Pass |
| 3 | 兼容旧字段名 | 新请求不包含旧别名 `instances` | Pass |
| 4 | TMQ config 兼容 | `adapterHA` 不进入 TMQ 自定义 config | Pass |
| 5 | 主从配置兼容 | 配置 `slaveClusterHost` 时 SQL `conn` 不请求 adapter 实例列表，endpoint 合并不引入动态发现节点 | Pass |
| 6 | 重连流程兼容 | SQL 重连、负载切换和后台健康检查复用认证握手时不依赖 `list_instances` 响应 | Pass |

## 12. 已知问题和限制

1. 手工故障切换用例依赖 3 个可访问 adapter 实例和 adapter 侧实例注册能力，不能在普通单 adapter 环境中完成。
2. `list_instances` 是连接或订阅时刻快照，不验证后续 adapter 拓扑动态变化推送。
3. JDBC `Connection` 和 `TaosConsumer` 不要求支持多线程并发访问，测试不覆盖同一实例多线程并发调用。
4. 运行手工用例前需确认启用参数使用 JDBC 实现中的配置名 `adapterHA=true`。
