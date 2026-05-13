# C# 连接器支持 failover TS

## 1 修订记录

| 编写日期 | 发布日期 | 版本  | 修订人     | 主要修改内容                                               |
| --- | --- |-----|---------|------------------------------------------------------|
| 2026-03-27 | 2026-03-27 | 1.0 | 谭雪峰 | 初稿 |

## 2 测试目标

本报告验证“支持故障转移”相关改动的实际测试覆盖与执行结果，重点确认以下内容：

- 多地址配置、WebSocket 空 `host` 拒绝、Native 空 `host` 兼容性、非法端口、Native 多地址拒绝等参数行为
- failover 连接器在地址失败、首选地址失败场景下的切换行为
- WebSocket 客户端的首次连接故障转移、运行中重连、负向配置行为与并发关闭安全性
- TMQ 消费者在故障转移后的订阅恢复、消费恢复、提交一致性与并发关闭安全性
- 当前实现遵循的 TMQ 至少一次语义是否与测试验收口径一致

## 3 参考文档

- [C# 连接器支持 failover FS.md](../05-设计文档/C%23%20连接器支持%20failover%20FS.md)

## 4 测试结论

本次按当前已有测试执行了 3 组与故障转移直接相关或强相关的自动化用例：

1. `TDengineConnectionStringBuilderTests` 全量：**14/14 通过**
2. `Driver.Test` 中 failover 核心、WebSocket failover（含 IPv6）、TMQ failover、并发关闭相关测试：**20/20 通过**
3. `Driver.Test` 中 WebSocket failover 负向集成测试：**3/3 通过**

合计执行结果如下：

- 总计：**37**
- 通过：**37**
- 失败：**0**
- 跳过：**0**

基于本次执行结果，可以确认：

- 当前代码已经具备多地址 failover 的核心自动化覆盖
- WebSocket 的首次故障转移、IPv6 首次故障转移、运行中重连、Exec、Schemaless、三地址多轮切换都已有现成测试且本次执行通过
- Native 对多地址的拒绝行为已被自动化验证
- TMQ 的订阅恢复、消费恢复、提交一致性与并发 `Close()` 行为已有现成测试且本次执行通过

此外，针对 2026-03-27 CI 失败根因补充执行了定向回归（不并入上述 37 项统计）：

- `Driver.Test.Client.TMQ.Consumer.SubscribeReconnect`
- `Driver.Test.Client.TMQ.Consumer.ConsumeReconnect`
- `Driver.Test.Client.Tools.TaosAdapterToolsTests`（2 项）

定向回归结果：**5/5 通过**（`net9`）。

## 5 测试环境

- OS: Linux
- .NET Target Framework: `net9`
- 测试框架: xUnit
- `taosadapter` 路径: `/usr/bin/taosadapter`
- 组件测试环境:
  - `MockWSServer` 用于 WebSocket failover 核心逻辑、连接计数与并发行为测试
- 集成测试环境:
  - 本地启动真实 `taosadapter`
  - 后端 TDengine 服务可正常访问
  - 使用本机回环地址 `127.0.0.1` / `localhost` / `::1` 与动态端口

本次执行命令如下：

```bash
dotnet test test/Data.Tests/Data.Tests.csproj -f net9 --filter "FullyQualifiedName~TDengineConnectionStringBuilderTests" --nologo --no-restore

dotnet test test/Driver.Test/Driver.Test.csproj -f net9 --filter "FullyQualifiedName~FailoverConnectorTests|FullyQualifiedName~FailoverAddressLeaseTests|FullyQualifiedName~Driver.Test.Client.Query.Failover|FullyQualifiedName~Driver.Test.Client.Query.Client.MultiAddress|FullyQualifiedName~Driver.Test.Client.TMQ.Consumer.MultiAddress|FullyQualifiedName~ConcurrentQueryAndDisposeShouldCompleteWithoutDeadlock|FullyQualifiedName~ConcurrentConsumeAndCloseShouldCompleteWithoutDeadlock" --nologo --no-restore

dotnet test test/Driver.Test/Driver.Test.csproj -f net9 --filter "FullyQualifiedName~AutoReconnectFalseShouldNotFailoverWhenPrimaryStops|FullyQualifiedName~ReconnectRetryCountZeroShouldNotFailoverWhenPrimaryStops|FullyQualifiedName~ReconnectIntervalZeroShouldStillFailoverWhenPrimaryStops" --nologo --no-restore
```

## 6 功能测试

### 6.1 配置解析与参数校验

#### 6.1.1 测试要点

- 多地址字符串可被正常接收和解析
- Native 协议拒绝多地址
- 非法端口和 WebSocket 空 `host` 会在连接前失败；Native 空 `host` / `null host` 不应在 connector 层抛出 `ArgumentException`
- 当前连接字符串相关回归未因本次改动被破坏

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `ParseHostList` | 验证 `host=127.0.0.1:6030,127.0.0.2:6031` 可作为多地址字符串保留下来 | 通过 |
| 2 | `NativeMultiHostShouldThrowWhenOpen` | 验证 Native 协议配置多地址时，`DbDriver.Open` 抛出参数异常 | 通过 |
| 3 | `NativeDuplicateHostShouldThrowWhenOpen` | 验证 Native 协议下重复地址也不会被接受 | 通过 |
| 4 | `ParseInvalidPort` | 验证非法端口值被拒绝 | 通过 |
| 5 | `WebSocketEmptyHostShouldThrowWhenOpen` | 验证 WebSocket 空白 `host` 被拒绝 | 通过 |
| 6 | `NativeEmptyHostShouldNotThrowArgumentExceptionWhenOpen` | 验证 Native 空串和空白 `host` 不会在 connector 层抛出 `ArgumentException` | 通过 |
| 7 | `NullNativeHostShouldNotThrowArgumentExceptionWhenOpen` | 验证 `builder.Host = null` 后 Native 打开连接不会在 connector 层抛出 `ArgumentException` | 通过 |

补充说明：

- 本次同时执行了 `TDengineConnectionStringBuilderTests` 全量 14 项，除上述与故障转移直接相关的 7 项外，其余连接串回归项也全部通过，说明本次改动未破坏该测试类现有行为。

### 6.2 故障转移核心机制

#### 6.2.1 测试要点

- 当前地址失败时继续尝试下一地址
- 首选地址失败时同轮可继续尝试其他地址
- 地址租约释放幂等
- 最少连接分配在并发场景下基本均衡
- 释放连接后计数能够回收并重新平衡

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `TryOpenShouldContinueToNextAddressWhenCurrentLeaseFails` | 第一个地址失败后继续尝试第二个地址并成功建立连接 | 通过 |
| 2 | `TryOpenShouldSkipFailedPreferredAddressWithinSameAttempt` | 首选地址失败后，同一轮不会卡在首选地址，能够继续尝试其他地址 | 通过 |
| 3 | `DisposeShouldRemainIdempotentWhenAddressIsNull` | `FailoverAddressLease(null)` 重复释放不抛异常 | 通过 |
| 4 | `MultiAddressConnectShouldSelectLeastConnectionAddress` | 两个客户端连接两个地址时，两个地址各分配到 1 个连接 | 通过 |
| 5 | `MultiAddressConnectShouldDistributeEvenlyUnderHighConcurrency` | 20 个并发连接下，2 个地址的连接分布差值不超过 2 | 通过 |
| 6 | `MultiAddressConnectShouldDistributeStrictlyUnderModerateConcurrency` | 10 个并发连接下，2 个地址的连接分布差值不超过 1 | 通过 |
| 7 | `MultiAddressConnectShouldNotUseAddressFromOtherConnection` | 一个连接失败时不会串用其他连接实例的地址状态 | 通过 |
| 8 | `MultiAddressReconnectShouldPreferPreviousAddressForTransientDisconnect` | 瞬时断连后优先回到上次成功地址 | 通过 |
| 9 | `MultiAddressDisposeShouldReleaseConnectionCountAndRebalance` | Dispose 后连接计数回收，后续连接重新均衡 | 通过 |

### 6.3 WebSocket 功能与集成测试

#### 6.3.1 测试要点

- 首次连接时可从不可用主地址切换到可用备地址
- IPv6 地址下的首次连接 failover 可通过真实 `taosadapter` 完成
- 运行中主地址失效后，开启自动重连时可切换到其他地址
- Query、Exec、Schemaless、Stmt 路径在 failover 下可继续工作
- `autoReconnect=false`、`reconnectRetryCount=0`、`reconnectIntervalMs=0` 的行为与预期一致
- Query 与 Dispose 并发执行时不死锁
- 三地址随机多轮切换后客户端仍可工作

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `MultiAddressConnectShouldFailoverToSecondRealAdapterWhenFirstUnavailable` | 第一个 adapter 不可用时，客户端能通过第二个 adapter 建连并查询 | 通过 |
| 2 | `MultiAddressIpv6ConnectShouldFailoverToSecondRealAdapterWhenFirstUnavailable` | 使用 `[::1]` 多地址配置时，第一个 IPv6 地址不可用，客户端能通过第二个 IPv6 地址建连并查询 | 通过 |
| 3 | `MultiAddressReconnectShouldFailoverToSecondRealAdapterWhenFirstStops` | 运行中停掉第一地址后，客户端自动切换到第二地址并继续查询 | 通过 |
| 4 | `MultiAddressExecShouldFailoverToSecondRealAdapterWhenFirstUnavailable` | 首地址不可用时，Exec 路径仍能建表、写入并校验数据 | 通过 |
| 5 | `MultiAddressSchemalessShouldFailoverToSecondRealAdapterWhenFirstUnavailable` | 首地址不可用时，Schemaless 写入和数据可见性正常 | 通过 |
| 6 | `ConcurrentQueryAndDisposeShouldCompleteWithoutDeadlock` | 查询与 Dispose 并发执行时无死锁、无 `TimeoutException` | 通过 |
| 7 | `AutoReconnectFalseShouldNotFailoverWhenPrimaryStops` | 关闭自动重连后，主地址失效时不会自动切换 | 通过 |
| 8 | `ReconnectRetryCountZeroShouldNotFailoverWhenPrimaryStops` | 重试次数为 0 时，主地址失效后不会自动恢复 | 通过 |
| 9 | `ReconnectIntervalZeroShouldStillFailoverWhenPrimaryStops` | 重连间隔为 0 时，只要有可用地址仍可恢复 | 通过 |
| 10 | `MultiAddressReconnectShouldSurviveMultipleRoundsAcrossThreeAdapters` | 3 个 adapter 多轮随机停启后仍能持续查询 | 通过 |

### 6.4 TMQ 功能与集成测试

#### 6.4.1 测试要点

- 首次订阅可从第二地址完成
- 消费过程中第一地址失效后可继续在第二地址消费
- `Consume` 与 `Close` 并发执行时不死锁
- failover 前后 offset 提交与查询一致
- 验收口径是至少一次，而不是精确一次

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | `MultiAddressSubscribeShouldFailoverToSecondAdapterWhenFirstUnavailable` | 第一地址不可用时，消费者可通过第二地址订阅并读到消息 | 通过 |
| 2 | `MultiAddressConsumeShouldReconnectToSecondAdapterWhenFirstStops` | 消费过程中停掉第一地址后，消费者可通过第二地址继续消费 | 通过 |
| 3 | `ConcurrentConsumeAndCloseShouldCompleteWithoutDeadlock` | 持续 `Consume` 时并发 `Close()` 不死锁、无 `TimeoutException` | 通过 |
| 4 | `MultiAddressConsumeCommitShouldRemainConsistentAfterFailover` | failover 前后的 offset 提交和查询保持一致 | 通过 |

补充说明：

- 当前现有测试对 TMQ 的验收重点是“故障转移后仍可消费并推进提交位点”。
- 对于重复消费，本报告按产品语义视为允许行为，不作为失败判定。

## 7 易用性测试（可选）

本特性不涉及 UI，本次未单独执行交互式易用性测试。

从现有自动化结果看，以下报错路径已具备基本可读性验证：

- WebSocket 空 `host`
- Native 多地址
- 非法端口
- 非法重连参数

## 8 长期稳定性测试（可选）

本次未单独执行长时间 soak 测试，但已执行一个现成的多轮随机切换稳定性用例：

- `MultiAddressReconnectShouldSurviveMultipleRoundsAcrossThreeAdapters`

该测试覆盖 3 个 adapter 的随机停启与反复重连，可作为当前自动化中的稳定性代表用例。

## 9 性能测试

本次未单独执行 benchmark 型性能测试。

但当前已有测试中，以下用例对连接分布行为提供了直接验证：

- `MultiAddressConnectShouldDistributeEvenlyUnderHighConcurrency`
  - 20 个并发客户端
  - 2 个地址
  - 分布差值不超过 2
- `MultiAddressConnectShouldDistributeStrictlyUnderModerateConcurrency`
  - 10 个并发客户端
  - 2 个地址
  - 分布差值不超过 1

因此，虽然没有单独输出吞吐和时延指标，但当前自动化已经验证了 failover 地址选择在并发场景下具备基本均衡性。

## 10 安全测试

本次未单独执行安全专项测试。

原因是当前故障转移改动主要影响客户端连接选择和重连逻辑，不改变认证、授权和权限模型。现有测试也未新增专门的安全攻击面用例。

## 11 兼容性测试

本次已通过现有自动化覆盖以下兼容性点：

- Native 单地址行为保持原有路径，Native 多地址继续明确失败
- `autoReconnect=false` 时保持“不自动切换”的旧语义
- `reconnectRetryCount=0` 时保持“不自动恢复”的旧语义
- `reconnectIntervalMs=0` 仍能在有可用地址时恢复
- 连接字符串相关旧能力所在测试类全量 14 项通过，未观察到同类配置能力回归

补充兼容性验证（2026-03-27）：

- `test/Driver.Test/Client/Tools/TaosAdapterTools.cs`
- `test/Driver.Test/Client/Tools/TaosAdapterToolsTests.cs`

上述两个文件中的 `using` 声明语法已调整为 `using (...) {}` 形式，以兼容 `net46`（C# 7.3）编译链路。已通过 `dotnet test test/Driver.Test/Driver.Test.csproj -f net46 --no-restore --list-tests` 验证构建通过。

### 11.1 补充：CI 稳定性回归（2026-03-27）

#### 11.1.1 背景

`compatibility-3360` 工作流中 `test unit 6.0.x` 出现单测失败，失败用例为：

- `Driver.Test.Client.TMQ.Consumer.ConsumeReconnect`

日志显示 `taosadapter` 在固定端口 `36042` 启动时发生端口冲突（`bind: address already in use`），继而触发 `Failed to start taosadapter`。

同一轮 CI 中 `test windows unit net46` 存在 C# 8 `using` 声明语法与 `net46` 编译链路不兼容的问题。

#### 11.1.2 修复

- 将 `test/Driver.Test/Client/TMQ/Reconnect.cs` 中 `SubscribeReconnect`/`ConsumeReconnect` 从固定端口改为动态空闲端口。
- 将测试工具代码中的 C# 8 `using` 声明改为 `using (...) {}`，确保 `net46` 可编译。

#### 11.1.3 验证

```bash
dotnet test test/Driver.Test/Driver.Test.csproj -f net9 --filter "FullyQualifiedName~Driver.Test.Client.TMQ.Consumer.ConsumeReconnect|FullyQualifiedName~Driver.Test.Client.TMQ.Consumer.SubscribeReconnect|FullyQualifiedName~Driver.Test.Client.Tools.TaosAdapterToolsTests"

dotnet test test/Driver.Test/Driver.Test.csproj -f net46 --no-restore --list-tests
```

验证结果：

- `net9` 定向回归：`5/5` 通过
- `net46` 编译检查：通过

## 12 已知问题和限制（可选）

- 本报告基于**当前已有测试及本次实际执行结果**，不是未来测试规划
- 集成测试依赖本地 `taosadapter` 和可访问的 TDengine 后端；缺失环境时相关用例无法执行
