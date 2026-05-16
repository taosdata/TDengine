# taosx-opc Observe 模式 Batch Read 设计文档

## 1. 概述

本文档描述 taosx-opc 在 **observe（轮询）** 采集模式下，如何从 OPC UA Server 批量读取点位数据，以及数据如何流经各组件最终写入 TDengine。

taosx-opc 是一个独立的 Go 进程，由 taosx（Rust）以子进程方式启动。两者之间通过 **Apache Arrow IPC over TCP** 传输数据。taosx-opc 负责与 OPC UA Server 交互并将点位值序列化为 Arrow RecordBatch；taosx 负责接收 RecordBatch、执行 transform、生成 INSERT SQL 并写入 TDengine。

## 2. 端到端数据流总览

```
┌──────────────┐    OPC UA Read RPC     ┌──────────────┐   Arrow IPC/TCP   ┌──────────┐   SQL   ┌──────────┐
│  OPC UA      │ ◄──────────────────── │  taosx-opc   │ ────────────────► │  taosx   │ ─────► │ TDengine │
│  Server      │    (批量值读取)         │  (Go 进程)    │   (RecordBatch)   │ (Rust)   │        │          │
└──────────────┘                       └──────────────┘                   └──────────┘        └──────────┘
```

**关键角色：**

| 组件 | 语言 | 职责 |
|------|------|------|
| OPC UA Server | — | 提供点位数据，响应 Read RPC |
| taosx-opc (`UAClient`) | Go | 连接 OPC UA、周期性轮询读值、序列化为 Arrow |
| ArrowReporter | Go | 将 NodeValue 打包为 Arrow RecordBatch，通过 TCP 发送给 taosx |
| taosx (source-opc sink) | Rust | 接收 Arrow IPC、执行 transform、生成 SQL、写入 TDengine |

## 3. Observe 模式详细数据流

### 3.1 启动阶段

```
UAClient.Collect()
  ├── getServerLimit()        ← 从 OPC Server 读取 maxNodesPerRead (NodeID i=11705)
  ├── initNodeName()          ← 批量读取所有点位的 BrowseName
  └── observe()               ← 启动轮询 goroutine
```

`getServerLimit()` 向 OPC UA Server 查询以下能力参数：

| NodeID | 参数名 | 含义 | 典型值 |
|--------|--------|------|--------|
| `i=11705` | MaxNodesPerRead | 单次 Read RPC 最大节点数 | 1000 |
| `i=11714` | MaxMonitoredItemsPerCall | 单次订阅最大监控项数 | — |
| `i=11710` | MaxNodesPerBrowse | 单次浏览最大节点数 | — |

> 如果 Server 返回 0 或查询失败，`maxNodesPerRead` 回退到默认值 **1000**。
> 如果检测到 KEPServerEX，硬编码为 **10000**。

### 3.2 轮询循环 (observe)

`observe()` 启动一个 `time.Ticker`，每隔 `readInterval`（由配置 `interval` 决定，单位秒）触发一次完整的读取-上报周期。

```
observe() goroutine:
  每 readInterval 触发:
    ┌─ pollCount++
    ├─ if pollCount % probeInterval == 0 → probeBadNodes()    // 定期重探黑名单
    ├─ activeNodes = filterActiveNodes(c.nodes)                // 排除黑名单节点
    ├─ readAllValue(activeNodes)                               // 批量读值（核心）
    ├─ 遍历 activeNodes，收集有效的 NodeValue
    │    ├─ 跳过 ValueType 无效的节点
    │    ├─ 名称为空的节点 → 追加到 readNameList 延迟读名
    │    └─ Status != OK 且 containsBad == false → 跳过
    ├─ if readNameList 非空 → readAllNames(readNameList)       // 补读名称
    ├─ if dumper != nil → dumper.Dump(values)                  // 可选：本地落盘
    └─ onMessage(values)                                       // 上报给 ArrowReporter
```

### 3.3 批量值读取 (readAllValue → readValueBatch)

这是 observe 模式的核心 I/O 路径。由于 OPC UA Server 对单次 Read RPC 有节点数上限（`maxNodesPerRead`），大量点位必须分批读取。

#### 3.3.1 分批策略（跨周期拆分）

`readAllValue` 采用两阶段读取：先重试上一周期记录的失败批次（拆成更小的子批次），再按 `maxNodesPerRead` 正常分批读取。

```go
func readAllValue(nodes []*nodeValue) {
    // 阶段 1：重试上一周期的失败批次
    for _, fb := range c.failedBatches {
        subSize := fb.size / 2  // 二分拆成子批次
        retryFailedBatch(fb.startIdx, fb.endIdx, subSize)
    }
    c.failedBatches = nil

    // 阶段 2：正常分批读取
    readNormalBatches(nodes)
}

func retryFailedBatch(start, end, subSize int) {
    for i := start; i < end; i += subSize {
        batchEnd := min(i + subSize, end)
        result := readValueBatch(nodes[i:batchEnd])
        if result == batchReadRPCError {
            if batchEnd - i <= individualTestThreshold {
                // 子批次已经很小，逐节点测试定位坏节点
                testNodesIndividually(nodes[i:batchEnd])
            } else {
                // 记录到 failedBatches，下一周期继续拆分
                c.failedBatches = append(c.failedBatches, failedBatchInfo{...})
            }
            time.Sleep(postFailureDelay)
        }
    }
}
```

**为什么跨周期**：某些 OPC Server（如 KEPServerEX）在返回 `StatusBadEncodingError` 后会关闭 TCP 连接，同一周期内的后续 Read 都会因连接断开而失败。跨周期拆分利用 OPC UA 库的**自动重连机制**：下一个轮询周期开始时，连接已恢复，拆分后的子批次可以正常读取。

**收敛过程**：以 1966 个点位，`maxNodesPerRead=1000`，批次 2（966 节点）包含 1 个坏节点为例：

```
周期 1: 正常批次 [0:1000] OK, [1000:1966] FAIL → 记录 failedBatch
周期 2: 重试 [1000:1483] OK, [1483:1966] FAIL → 记录 failedBatch
         正常批次 [0:1000] OK, [1000:1966] OK（坏节点已从 failedBatch 路径处理）
周期 3: 重试 [1483:1724] OK, [1724:1966] FAIL → 继续拆分
...
周期 7-8: 子批次 ≤10 → testNodesIndividually() → 精确黑名单化坏节点
周期 9+: failedBatches 为空，恢复正常
```

**收敛速度**：log₂(966/10) ≈ 7，约 **7-8 个轮询周期**后坏节点被精确定位并黑名单化。

> **关键事实**：节点在 `c.nodes` 中的排序由 OPC UA 浏览树的遍历顺序决定，
> 通常按 NodeID 字符串的字典序排列。因此哪些节点落入哪个批次是稳定且可预测的。

#### 3.3.2 单批读取 (readValueBatch)

每个批次执行一次 OPC UA Read RPC 调用：

```go
resp, err := conn.Read(ctx, &ReadRequest{
    MaxAge:             maxAge,
    TimestampsToReturn: TimestampsToReturnBoth,
    NodesToRead:        valueReqs,        // [{NodeID, AttributeID=Value}, ...]
})
```

RPC 的结果分为两个层级：

| 层级 | 判断条件 | 含义 | 影响范围 |
|------|---------|------|---------|
| **RPC 层** | `err != nil` | 整个 Read 请求失败 | 本批次**所有**节点丢失数据 |
| **节点层** | `resp.Results[i].Status != OK` | 单个节点值异常 | 仅影响该节点，其他节点正常 |

`readValueBatch` 返回 `batchReadResult` 枚举：

| 返回值 | 含义 | 对自适应的影响 |
|--------|------|--------------|
| `batchReadOK` | 批次读取成功 | 无 |
| `batchReadConnectionError` | 连接级错误（Server 不可达） | 不调整批次大小 |
| `batchReadRPCError` | 非连接 RPC 错误（如 EncodingError） | 触发批次减半 |

**RPC 成功路径**（正常情况）：

```
对每个 resp.Results[i]:
  ├─ Status == OK
  │    ├─ 提取 Value、ValueType、Timestamp
  │    ├─ consecutiveFailures 重置为 0
  │    └─ 写入 nodeValue
  └─ Status != OK
       ├─ nodeValue.Value = nil
       ├─ consecutiveFailures++
       ├─ 如果 consecutiveFailures == 1 → Debug 日志（仅首次）
       └─ 如果 consecutiveFailures == statusFailThreshold(3)
            → Error 日志 + addBadNode（加入黑名单）
```

**RPC 失败路径**：

```
err != nil
  ├─ isConnectionError(err)?
  │    └─ 是 → 返回 batchReadConnectionError（不调整批次大小）
  └─ 非连接错误
       ├─ 返回 batchReadRPCError（触发自适应减半）
       └─ 如果 len(batch) <= 10 → testNodesIndividually()
            逐个读取定位并黑名单化坏节点
```

#### 3.3.3 批次失败后的连接恢复

某些 OPC UA Server（如 ADS 场景中的 KEPServerEX）在返回 `StatusBadEncodingError` 后会关闭 TCP 连接。这意味着同一周期内的后续 Read 请求也会失败（连接级错误）。

为了最大化数据采集，`readAllValue` 在批次 RPC 失败后等待 500ms（`postFailureDelay`），让 OPC UA 库的自动重连机制恢复连接，然后继续处理后续批次。

### 3.4 坏节点隔离与黑名单机制

#### 3.4.1 问题背景

OPC UA Server 在某些场景下，批次中只要包含一个"坏节点"（例如编码异常、元数据损坏），就会导致整个 Read RPC 返回错误（如 `StatusBadEncodingError`），而不是在节点级别返回错误。这意味着同批次中所有正常节点也无法获取数据。

更严重的是：由于节点的排序是稳定的，同一个坏节点每次都落在同一个批次中，导致该批次**每个轮询周期都失败**，造成持续性数据丢失。

#### 3.4.2 跨周期拆分隔离坏节点

**为什么不用二分定位**：二分定位需要在批次失败后立即进行多次 Read 请求。但某些 OPC Server 在 `StatusBadEncodingError` 后会关闭 TCP 连接，导致后续所有 Read 都失败。在 ADS 场景中实测发现，二分定位的第一次子批次 Read 在 7ms 内就遇到连接断开，永远无法完成定位。

**跨周期拆分**利用 TCP 自动重连：记录失败批次，在**下一个轮询周期**将其拆成更小的子批次重试。每个周期只拆一次（二分），逐步收敛直到子批次小到可以逐节点测试：

```
周期 N: [966 节点] 失败 → 记录 failedBatch
周期 N+1: 重试 [483][483] → 其中一个失败 → 记录 failedBatch
周期 N+2: 重试 [241][242] → 其中一个失败 → ...
...
周期 N+K: [≤10 节点] 失败 → testNodesIndividually() → 精确黑名单化
```

**边界条件**：

| 场景 | 处理 |
|------|------|
| 连接级错误（Server 不可达） | 不记录 failedBatch（不是节点问题） |
| 多个批次同时失败 | 各自独立记录和拆分 |
| 拆分后子批次也遇到连接错误 | 保留在 failedBatches 中，下一周期重试 |
| 节点列表变化（observeChange） | 清空 failedBatches（索引失效） |
| subSize 已经是 1 但仍失败 | 直接 addBadNode 黑名单化 |

#### 3.4.3 逐节点测试 (testNodesIndividually)

当自适应机制将批次缩小到 ≤10 个节点时，如果该批次仍然 RPC 失败，则逐个读取以精确定位坏节点：

```
testNodesIndividually(smallBatch):
    for each node in smallBatch:
        单独 Read 该节点
        if 连接错误 → 停止（等待下一周期重试）
        if RPC 错误 → addBadNode() → 精确黑名单化
        if 成功 → 该节点正常
```

> 注意：此函数在 RPC 错误之后调用，连接可能已断开。第一个节点的 Read 可能因连接错误而失败，此时立即停止，等待下一周期连接恢复后重试。

#### 3.4.4 黑名单管理

```go
type UAClient struct {
    badNodes      map[string]*badNodeInfo   // 黑名单
    badNodesMu    sync.RWMutex              // 并发保护
    probeInterval uint64                     // 重探间隔（轮询周期数）
    pollCount     uint64                     // 当前轮询计数
    failedBatches []failedBatchInfo          // 上一周期失败的批次，下一周期拆分重试
}
```

**入黑名单的两种路径：**

| 路径 | 触发条件 | 典型错误 |
|------|---------|---------|
| 逐节点测试 | 批次 ≤10 且 RPC 失败 → 单节点 Read 失败 | `StatusBadEncodingError` |
| 单节点 Status 失败 | `consecutiveFailures` 达到阈值 (3) | `StatusUncertainInitialValue` |

**定期重探 (probeBadNodes)**：

每隔 `probeInterval`（默认 60）个轮询周期，逐个单独读取黑名单中的节点。如果某节点读取成功（RPC 无错 + Status OK），则从黑名单移除，恢复到正常轮询。

```
probeBadNodes():
    for each badNode:
        单独 Read 该节点
        if 连接错误 → 中止探测（Server 不可达）
        if RPC 成功 && Status == OK:
            从 badNodes 移除
            consecutiveFailures 重置为 0
            Info 日志: "node recovered"
```

#### 3.4.5 observe 循环中的过滤

```go
// 每个轮询周期
activeNodes := c.filterActiveNodes(c.nodes)   // O(N)，排除黑名单节点
c.readAllValue(activeNodes)                    // 只读活跃节点，含跨周期拆分重试
```

`filterActiveNodes` 是 O(N) 的简单过滤，当黑名单为空时直接返回原始 slice（零开销）。

## 4. 从 taosx-opc 到 TDengine 的数据流

### 4.1 taosx-opc 内部：NodeValue → Arrow RecordBatch

```
observe()
  → onMessage(values []*NodeValue)
    → handleMessage()                           // cmd/collect/collect.go
      → 按 (IDStr, ValueType) 分组
      → 每组调用 ArrowReporter.Report(group)
        → MessageList.Add(group)                // 缓冲层
          → 达到 batchSize 或 batchTimeout 后
            → upload(list)
              → RecordBuilder 构建 Arrow Record
                  字段: [id, name, ts, now, value, status, request_ts]
              → ipc.Writer.Write(record)        // 通过 TCP 发送给 taosx
              → ipc.Reader.Next()               // 等待 taosx 的 ack
```

**Arrow Schema（每种 ValueType 一个 Reporter / TCP 连接）：**

| 列序 | 字段名 | 类型 | 来源 |
|------|--------|------|------|
| 0 | id | string | `nodeValue.IDStr` |
| 1 | name | string | `nodeValue.Name` (BrowseName) |
| 2 | ts | timestamp(ms) | `nodeValue.Timestamp`（优先 SourceTimestamp） |
| 3 | now | timestamp(ms) | `nodeValue.FinishTime`（Read 完成时间） |
| 4 | value | varies | `nodeValue.Value`（实际值，类型取决于 ValueType） |
| 5 | status | int64 | `nodeValue.Status`（OPC UA StatusCode） |
| 6 | request_ts | timestamp(ms) | `nodeValue.StartTime`（Read 发起时间） |

### 4.2 taosx 端：Arrow RecordBatch → INSERT SQL → TDengine

```
taosx (Rust)
  → source-opc plugin sink (taosx-core/src/plugins/sink/mod.rs)
    → 接收 Arrow IPC RecordBatch
    → transform 引擎处理（列映射、标签提取、超级表分类）
    → 生成 INSERT SQL
    → 执行 SQL 写入 TDengine
```

## 5. 时间戳处理

`readValueBatch` 中对每个节点的时间戳选择优先级：

```
1. SourceTimestamp  ← OPC UA Server 报告的数据源时间（优先）
2. ServerTimestamp  ← OPC UA Server 处理时间（备选）
3. time.Now()      ← 本地时间（兜底）
```

同时记录两个辅助时间戳：

- `StartTime`：本批次 `conn.Read()` 调用前的时间 → 对应 Arrow 的 `request_ts` 列
- `FinishTime`：本批次 `conn.Read()` 返回后的时间 → 对应 Arrow 的 `now` 列

这两个时间在 taosx 侧可配置为 `request_ts` 时间戳列的来源。

## 6. 错误处理与日志策略

### 6.1 日志分级

| 场景 | 日志级别 | 频率 | 说明 |
|------|---------|------|------|
| 批次 RPC 失败 → 自适应缩小 | Warn | 每次失败 | 记录批次大小和错误 |
| 自适应批次大小调整 | Info | 仅变化时 | 记录新的批次大小 |
| 逐节点测试定位到坏节点 | Error | 仅一次 | 精确到具体 NodeID |
| 单节点 Status 异常（首次） | Debug | 仅首次 | 不污染生产日志 |
| 单节点达到失败阈值 | Error | 仅一次 | 报告具体 NodeID + 连续失败次数 |
| 节点恢复 | Info | 仅一次 | 从黑名单移除时打印 |
| 正常轮询读取 | Debug | 每次 | 包含批次大小、耗时 |

### 6.2 与旧版本行为对比

| 行为 | 旧版本 | 当前版本 |
|------|--------|---------|
| 批次 RPC 失败 | 整批丢弃 + 尝试 reconnect（对非网络错误无效） | 自适应缩小批次 → 坏节点逐步隔离 → 其余节点正常读取 |
| 单节点 Status 异常 | 每轮每节点打一条 Error 日志 | 首次 Debug，达阈值一次 Error，之后跳过 |
| 日志量（以 5 个坏节点 × 1000 次/天为例） | ~5000 条 Error/天 | ~5 条 Error（总计） |

## 7. 性能特征

### 7.1 正常运行（无坏节点）

- `filterActiveNodes`: 黑名单为空时直接返回原 slice，**零额外开销**
- `adaptiveMaxNodes == maxNodesPerRead`，批次数与旧版本完全一致
- `pollCount++` 和模运算：可忽略
- 数据路径与旧版本完全一致

### 7.2 自适应缩小阶段

以 ADS 实际场景为例（1966 节点，`maxNodesPerRead=1000`，批次 2 固定失败）：

| 周期 | adaptiveMax | 批次数 | 失败批次 | 额外开销 |
|------|-------------|--------|---------|---------|
| 1 | 1000 | 2 | 1 | 500ms（重连等待）|
| 2 | 500 | 4 | 1 | 500ms |
| 3 | 250 | 8 | 1 | 500ms |
| 4 | 125 | 16 | 1 | 500ms |
| 5+ | 62 | 32 | 1 | 500ms |

**稳态**（adaptiveMax=62）：32 批次，其中 31 批次成功，1 批次失败后 500ms 等待重连。
额外开销 = 更多 Read RPC 调用 + 500ms 等待 ≈ 约 0.6s/周期。对于 1s observe interval，
仍然可以在每个周期内完成读取。

### 7.3 重探开销

每 60 个轮询周期，逐个读取黑名单节点。假设黑名单 5 个节点，每个单独 Read ~2ms，总开销 ~10ms/60 周期，**可忽略**。

## 8. 配置参数

| 参数 | 来源 | 默认值 | 说明 |
|------|------|--------|------|
| `maxNodesPerRead` | OPC UA Server (i=11705) | 1000 | 单次 Read RPC 最大节点数 |
| `interval` | Explorer 任务配置 | — | 轮询间隔（秒） |
| `batchSize` | Explorer 任务配置 (ReportConfig) | — | Arrow IPC 消息缓冲大小（与 OPC Read 批次无关） |
| `statusFailThreshold` | 代码常量 | 3 | 单节点连续 Status 失败多少次后加入黑名单 |
| `defaultProbeInterval` | 代码常量 | 60 | 每隔多少个轮询周期重探黑名单节点 |

> **重要区分**：`batchSize`（Explorer 配置）控制的是 taosx-opc → taosx 之间的 Arrow IPC
> 消息缓冲大小，与 OPC UA Read RPC 的 `maxNodesPerRead` 是完全不同的参数。

## 9. 关键数据结构

### 9.1 nodeValue

```go
type nodeValue struct {
    nodeID              *ua.NodeID        // OPC UA 节点标识
    nodeValue           *common.NodeValue // 承载读取结果的值对象
    clientHandle        uint32            // 订阅模式的客户端句柄
    subscribed          bool
    subscriptionID      *int
    monitoredItemID     uint32
    consecutiveFailures int               // 连续 Status 失败计数
}
```

### 9.2 common.NodeValue

```go
type NodeValue struct {
    IDStr      string          // 节点 ID 字符串 (如 "ns=2;s=G3.bb1")
    Name       string          // BrowseName
    Value      interface{}     // 实际值
    ValueType  types.ValueType // 值类型 (Float, Double, Int32, String, ...)
    Timestamp  time.Time       // 数据时间戳
    StartTime  time.Time       // Read 请求发起时间
    FinishTime time.Time       // Read 请求完成时间
    Status     int64           // OPC UA StatusCode
}
```

### 9.3 UAClient（黑名单相关字段）

```go
type UAClient struct {
    // ... 其他字段 ...
    badNodes      map[string]*badNodeInfo   // 黑名单映射
    badNodesMu    sync.RWMutex              // 读写锁
    probeInterval uint64                     // 重探间隔
    pollCount     uint64                     // 轮询计数器
}
```

## 10. 相关文件索引

| 文件 | 说明 |
|------|------|
| `plugins/opc/client/opcua/client.go` | UAClient 核心实现：observe、readAllValue、readValueBatch、locateBadNodes、probeBadNodes |
| `plugins/opc/client/client.go` | Client 接口定义、OnMessage 回调类型 |
| `plugins/opc/cmd/collect/collect.go` | 启动入口、handleMessage 桥接 ArrowReporter |
| `plugins/opc/reporter/arrow.go` | Arrow IPC 序列化与 TCP 传输 |
| `plugins/opc/reporter/manager.go` | 按 (IDStr, ValueType) 路由到对应的 ArrowReporter |
| `plugins/opc/buffer/message.go` | 消息缓冲层（batchSize / batchTimeout 触发上传） |
| `plugins/opc/config/config.go` | 配置结构定义 |
| `taosx-core/src/plugins/sink/mod.rs` | taosx 侧接收 Arrow IPC 并生成 INSERT SQL |
