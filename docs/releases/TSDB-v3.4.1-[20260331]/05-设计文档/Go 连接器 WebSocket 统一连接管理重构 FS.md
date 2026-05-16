# Go 连接器 WebSocket 统一连接管理重构 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-23 | - | 1.0 | 谭雪峰 | 编写文档 |

## 2. 背景

driver-go 现有 WebSocket 连接管理分散在多个包中（`taosWS`、`ws/stmt`、`ws/schemaless`、`ws/tmq`），各自独立实现连接建立、消息路由、超时处理和错误恢复逻辑，导致以下问题：

1. **代码重复**：每个包各自维护 WebSocket 连接生命周期管理，逻辑高度相似但实现不一致。
2. **缺乏高可用能力**：不支持多节点 failover 和自动重连，单节点故障时客户端无法自动恢复。
3. **维护成本高**：Bug 修复和功能增强需要在多个包中重复实施。
4. **stmt2 协议支持不完整**：WebSocket 通道上的 stmt2 二进制协议实现缺失。
本次重构引入 `ws/unified` 统一连接管理包，将所有 WebSocket 协议操作（Query、Stmt/Stmt2、Schemaless、TMQ）收归到同一个客户端实现中，同时保持所有现有外部接口向后兼容。

## 3. 定义

| 术语 | 说明 |
| --- | --- |
| Runtime | `ws/client.Client` 实例，代表一个活跃的底层 WebSocket 连接 |
| Client | `ws/unified.Client`，统一连接管理器，持有当前 Runtime 并负责 failover/reconnect |
| Generation | Runtime 的递增版本号，用于区分新旧连接、防止陈旧重连覆盖健康连接 |
| Bootstrap | 连接建立后的协议握手过程（发送认证信息、切换数据库等） |
| PendingRequest | 以 `req_id` 为键注册的等待响应请求，收到响应后通过 channel 通知调用方 |
| Failover | 当活跃节点不可用时，自动切换到候选节点重新建立连接 |
| Stmt2 | TDengine 第二代参数化写入协议，支持多表绑定和增强的类型系统 |
| DSN | Data Source Name，连接字符串，格式为 `ws(host1:port1,host2:port2)/db?params` |
| TMQ | TDengine Message Queue，数据订阅消费组件 |

## 4. 行为说明

### 4.1 统一连接管理（ws/unified.Client）

#### 4.1.1 连接建立

通过 `NewClient` + `Connect()` 建立连接。支持单节点和多节点配置。

```go
// 单节点连接（通过 Config）
cfg := unified.NewConfig([]string{"ws://localhost:6041"})
cfg.User = "root"
cfg.Passwd = "taosdata"
cfg.DbName = "test"
client, err := unified.NewClient(cfg, "/ws")
if err != nil { /* handle */ }
err = client.Connect()

// 多节点 failover 连接
cfg := unified.NewConfig([]string{
    "ws://node1:6041",
    "ws://node2:6041",
    "ws://node3:6041",
})
cfg.User = "root"
cfg.Passwd = "taosdata"
cfg.DbName = "test"
cfg.AutoReconnect = true
client, err := unified.NewClient(cfg, "/ws")
if err != nil { /* handle */ }
err = client.Connect()

// 通过 DSN 创建
client, err := unified.NewClientFromDSN(
    "root:taosdata@ws(node1:6041,node2:6041)/test?autoReconnect=true",
    "/ws",
)
if err != nil { /* handle */ }
err = client.Connect()
```

#### 4.1.2 DSN 解析

DSN 完整格式：

```plaintext
[user[:password]@][net[(addr[,addr...])]]/dbname[?param1=value1&paramN=valueN]
```

示例：

```plaintext
# 单节点
root:taosdata@ws(localhost:6041)/test

# 单节点 wss 加密
root:taosdata@wss(localhost:6041)/test?enableCompression=true

# 多节点 failover
root:taosdata@ws(node1:6041,node2:6041,node3:6041)/test?autoReconnect=true

# 使用 token（云服务）
root:taosdata@ws(cloud.tdengine.com:443)/test?token=your_cloud_token

# 完整参数
root:taosdata@ws(node1:6041,node2:6041)/test?autoReconnect=true&reconnectIntervalMs=3000&reconnectRetryCount=5&readTimeout=30s&writeTimeout=10s&enableCompression=true&timezone=Asia%2FShanghai
```

##### 4.1.2.1 完整 DSN 参数列表

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `interpolateParams` | bool | `true` | 是否启用参数插值 |
| `token` | string | `""` | 云服务认证 Token，会被附加到每个 Endpoint URL 的 query string 中 |
| `enableCompression` | bool | `false` | 是否启用 WebSocket 压缩 |
| `readTimeout` | duration | `5m` | 读取超时，支持 Go duration 格式（如 `30s`、`5m`） |
| `writeTimeout` | duration | `10s` | 写入超时，支持 Go duration 格式 |
| `timezone` | string | `""` | 时区，需 URL 编码（如 `Asia%2FShanghai`），用于时间戳转换 |
| `bearerToken` | string | `""` | Bearer Token 认证，与 `token` 互斥 |
| `totpCode` | string | `""` | TOTP 一次性验证码，用于双因素认证 |
| `autoReconnect` | bool | `false` | 是否启用自动重连和 Failover |
| `chanLength` | uint | `1` | 内部消息 channel 长度 |
| `reconnectIntervalMs` | int | `2000` | 重连间隔（毫秒），两轮候选节点遍历之间的等待时间 |
| `reconnectRetryCount` | int | `3` | 重连重试轮数，每轮遍历所有候选节点 |

> **说明**：未匹配到上述已知 key 的参数会被存入 `Config.Params` map 中，以键值对形式透传。
>
#### 4.1.3 消息路由机制

所有协议操作共享同一个消息处理器，通过 `req_id` 进行请求-响应匹配：

1. 发送请求时，分配唯一 `req_id` 并在 `pendingRequests` 中注册等待 channel。
2. 收到 WebSocket 文本消息时，`handleTextMessage` 从 JSON 中提取 `req_id`。
3. 通过 `pendingRequests` 查找对应 channel，将原始消息投递给等待者。
4. 调用方从 channel 收到响应后，反序列化并处理。
线程安全通过 `pendingLock`（RWMutex）保护 `pendingRequests` map。

#### 4.1.4 自动重连与 Failover

当操作遇到可重连错误（网络断开、连接关闭）时：

1. `sendWithReconnect` 捕获错误。
2. 调用 `reconnectWithBootstrap` 发起重连。
3. 通过 `failoverState` 获取候选节点列表：**先尝试当前活跃节点**，若失败再按最少连接数排序尝试其余节点。
4. 逐一尝试候选节点，成功后执行 Bootstrap 握手。
5. `swapRuntime` 原子替换 Runtime 指针并递增 Generation。
6. 在新 Runtime 上重试一次原操作。
**重连安全保证**：

- `reconnectLock` 确保同一时刻只有一个重连流程运行。
- Generation 机制防止陈旧重连覆盖更新的健康连接。
- 所有等待中的请求在连接断开时通过 Runtime 的 `Done()` channel 收到通知。

#### 4.1.5 错误模型

引入统一错误类型 `unified.Error`，包含：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `Type` | `ErrorType` | 错误分类：`protocol` / `message_timeout` / `connect_timeout` / `client_closed` / `invalid_state` / `reconnect_failed` / `invalid_config` / `invalid_dsn` |
| `Message` | `string` | 错误消息 |
| `Cause` | `error` | 原始底层错误 |
| `RequestSummary` | `string` | 惰性生成的请求上下文摘要，用于诊断 |
| `ConnectionRelated` | `bool` | 是否与连接相关 |
| `ConnectionDisconnected` | `bool` | 是否为连接断开 |
| `ReconnectFailed` | `bool` | 是否为重连失败 |

辅助判断函数：

```go
unified.ErrorTypeOf(err)                  // 提取错误类型
unified.IsErrorType(err, ErrorTypeProtocol) // 判断错误类型
unified.IsConnectionRelatedError(err)       // 是否连接相关
unified.IsConnectionDisconnectedError(err)  // 是否连接断开
unified.IsReconnectFailedError(err)         // 是否重连失败
```

### 4.2 Query 操作

```go
// Exec（写入/DDL）
affected, err := client.Exec(0, "CREATE DATABASE IF NOT EXISTS test")

// Query（查询）
resultSet, err := client.Query(0, "SELECT * FROM meters LIMIT 10")
if resultSet != nil {
    defer resultSet.Close()
    dest := make([]driver.Value, resultSet.FieldCount())
    for {
        err = resultSet.Next(dest)
        if err == io.EOF {
            break
        }
        // 处理 dest
    }
}
```

- `reqID` 参数传 0 时自动生成。
- `ResultSet` 绑定到特定 Runtime，不支持跨连接 failover（因为服务端结果句柄与连接绑定）。
- 支持后台预取（Prefetch），在用户处理当前数据块时提前拉取下一块。

### 4.3 Stmt/Stmt2 参数化操作

#### 4.3.1 Stmt2 绑定协议

完整实现 stmt2 二进制协议，支持通过 WebSocket 进行多表绑定写入：

```go
// 初始化 stmt（reqID 传 0 自动生成）
stmt, err := client.InitStmt(0)

// Prepare SQL
err = stmt.Prepare(0, "INSERT INTO ? USING meters TAGS(?) VALUES (?, ?, ?)")

// 方式一：Bind（推荐，直接使用 TaosStmt2BindData）
bindData := []*commonstmt.TaosStmt2BindData{
    {
        TableName: "d1001",
        Tags:      []driver.Value{"California.SanFrancisco"},
        Cols: [][]driver.Value{
            {time.Now()},
            {float32(23.5)},
            {int32(1)},
        },
    },
}
err = stmt.Bind(bindData)

// 方式二：兼容 API（Deprecated，逐步设置 TableName/Tags/Params/AddBatch）
err = stmt.SetTableName("d1001")
err = stmt.SetTags(tagParam, tagType)
err = stmt.BindParam(colParams, colType)
err = stmt.AddBatch()

// 执行
affected, err := stmt.Exec(0)

// 查询语句获取结果
resultSet, err := stmt.UseResult(0)

// 关闭
err = stmt.Close(0)
```

stmt2 二进制帧格式（固定头 28 字节）：

| 偏移 | 字段 | 大小 | 说明 |
| --- | --- | --- | --- |
| 0 | TotalLength | 4B | 整包长度 |
| 4 | Count | 4B | 表数量 |
| 8 | TagCount | 4B | Tag 列数 |
| 12 | ColCount | 4B | 数据列数 |
| 16 | TableNamesOffset | 4B | 表名段偏移 |
| 20 | TagsOffset | 4B | Tags 数据偏移 |
| 24 | ColsOffset | 4B | Cols 数据偏移 |

每个数据块（Tag/Col）布局：

| 字段 | 说明 |
| --- | --- |
| TotalLength (4B) | 本块总长 |
| DataType (4B) | 数据类型 |
| Num (4B) | 行数 |
| IsNull bitmap | 每行 1 字节 |
| HaveLength (1B) | 是否包含长度数组 |
| Length[] (4B×Num) | 变长类型的每行长度 |
| BufferLength (4B) | 数据区总长 |
| Buffer | 原始数据 |

#### 4.3.2 新增数据类型支持

| 类型 | 说明 |
| --- | --- |
| DECIMAL64 | 64 位定点小数 |
| BLOB | 二进制大对象 |

在 `types/taostype.go` 中新增对应 Go 自定义类型（`TaosDecimal`、`TaosBlob` 等），支持在 stmt2 绑定中明确指定目标类型。

#### 4.3.3 Stmt 兼容层

原 `ws/stmt` 包的 API 保持不变，内部委托给 `unified.Stmt`。支持自动重连后重新 Prepare，如果 schema 在断连期间发生变化则返回 `ErrStmtReprepareSchemaChanged`。

### 4.4 Schemaless 插入

```go
// 直接使用 unified.Client（推荐，支持多节点 failover）
client.SchemalessInsert(0, lineData, unified.InfluxDBLineProtocol, "ms", 0, "")

// 通过 ws/schemaless 包（向后兼容，Deprecated，仅单节点）
s, err := schemaless.NewSchemaless(schemaless.NewConfig("ws://localhost:6041", 0,
    schemaless.SetUser("root"),
    schemaless.SetPassword("taosdata"),
    schemaless.SetDb("test"),
))
err = s.Insert(lineData, schemaless.InfluxDBLineProtocol, "ms", 0, 0)
```

- 支持 InfluxDB Line Protocol、OpenTSDB Telnet、OpenTSDB JSON 三种协议。
- 多节点 failover 仅通过 `unified.Client.SchemalessInsert` 支持，兼容接口 `ws/schemaless` 仅支持单节点。

### 4.5 TMQ 消费

```go
conf := tmq.ConfigMap{
    "ws.url":            "ws://node1:6041,ws://node2:6041",
    "td.connect.user":   "root",
    "td.connect.pass":   "taosdata",
    "group.id":          "
my-group",
    "auto.offset.reset": "latest",
    "ws.autoReconnect":  true,
}
consumer, err := unified.NewTMQConsumer(&conf)
err = consumer.SubscribeTopics([]string{"topic1"}, nil)

event := consumer.Poll(500)
switch e := event.(type) {
case *tmq.DataMessage:
    // 处理数据
case tmq.Error:
    // 处理错误
}

offsets, err := consumer.Commit()
err = consumer.Close()
```

通过兼容层 `ws/tmq` 使用多节点 failover（内部委托 `unified.TMQConsumer`）：

```go
import (
    tmqcommon "github.com/taosdata/driver-go/v3/common/tmq"
    "github.com/taosdata/driver-go/v3/ws/tmq"
)

// ws.url 多端点用逗号分隔，故意将备用节点放前面以演示 failover
consumer, err := tmq.NewConsumer(&tmqcommon.ConfigMap{
    "ws.url":                  "ws://standby-node:6041,ws://primary-node:6041",
    "ws.autoReconnect":        true,
    "ws.reconnectIntervalMs":  2000,
    "ws.reconnectRetryCount":  3,
    "td.connect.user":         "root",
    "td.connect.pass":         "taosdata",
    "group.id":                "failover_group",
    "client.id":               "failover_client",
    "auto.offset.reset":       "earliest",
    "enable.auto.commit":      "false",
    "msg.with.table.name":     "true",
})
if err != nil {
    log.Fatal(err)
}
defer func() {
    _ = consumer.Unsubscribe()
    _ = consumer.Close()
}()

err = consumer.Subscribe("my_topic", nil)

for {
    ev := consumer.Poll(500)
    if ev == nil {
        continue
    }
    switch e := ev.(type) {
    case *tmqcommon.DataMessage:
        fmt.Printf("topic: %s, data: %v\n", e.TopicPartition.Topic, e.Value())
        consumer.CommitOffsets([]tmqcommon.TopicPartition{e.TopicPartition})
    case tmqcommon.Error:
        log.Printf("poll error: %s", e.Error())
    }
}
```

- 重连后自动重新订阅上次已知的 topic 列表。
- 配置验证增强，确保 `group.id` 等必选项在连接前就被校验。
- `Poll` 返回 `tmq.Event` 接口，通过类型断言区分消息和错误。

### 4.6 原有包的变化

#### 4.6.1 taosWS

| 组件 | 变化 |
| --- | --- |
| `connection.go` | 精简为 `database/sql/driver` 适配层，底层委托 `unified.Client` |
| `statement.go` | Stmt 包装 `unified.Stmt`，保留值转换逻辑 |
| `dsn.go` | `Config` 成为 `unified.Config` 的别名，解析委托 `unified.ParseDSN` |
| `rows.go` | 包装 `unified.ResultSet` |
| `proto.go` | 类型别名指向 `unified/proto` |

#### 4.6.2 ws/stmt

| 组件 | 变化 |
| --- | --- |
| `connector.go` | 标记为 Deprecated，内部创建 `unified.Client` |
| `stmt.go` | 包装 `unified.Stmt`，方法标记 Deprecated |
| `rows.go` | 包装 `unified.ResultSet` |

#### 4.6.3 ws/schemaless

| 组件 | 变化 |
| --- | --- |
| `schemaless.go` | 精简为包装 `unified.Client`，仅支持单节点 |
| `proto.go` | 删除（移至 `unified/proto`） |

#### 4.6.4 ws/tmq

| 组件 | 变化 |
| --- | --- |
| `consumer.go` | 精简为兼容外壳，内部委托 `unified.TMQConsumer` |
| `config.go` | 迁移至 `unified/tmq_config.go` |

### 4.7 ws/client 底层增强

| 增强项 | 说明 |
| --- | --- |
| 线程安全错误处理 | 并发安全的 handler 赋值和错误读取 |
| Envelope 池化 | 减少 GC 压力 |
| drainSendChan | 连接关闭时通知所有待发送消息 |
| HasConnection / LastError | 连接状态检查辅助方法 |

### 4.8 参数构建器（common/param）

新增 `param.ColumnType` 和 `param.Param` 构建器，简化 stmt2 绑定参数构造：

```go
colTypes := param.NewColumnType(3).
    AddTimestamp().
    AddFloat().
    AddInt()

p := param.NewParam(3).
    AddTimestamp(time.Now(), common.PrecisionMilliSecond).
    AddFloat(23.5).
    AddInt(1)
```

### 4.9 高性能内存复制（common/stmt/mem）

通过 `runtime.memmove`（`unsafe.Pointer`）实现高性能内存复制，用于 stmt2 二进制帧的高效组装。

### 4.10 ws/unified 对外接口

#### 4.10.1 配置与构建

| 类型/函数 | 签名 | 说明 |
| --- | --- | --- |
| `Config` | struct | 统一配置结构体 |
| `NewConfig` | `func(endpoints []string) *Config` | 创建带默认值的配置 |
| `ParseDSN` | `func(dsn string) (*Config, error)` | 解析 DSN 字符串为配置 |
| `NewConfigFromDSN` | `func(dsn string, defaultPath string) (*Config, error)` | 解析 DSN 并执行 Normalize |
| `NormalizeEndpoints` | `func(endpoints []string, defaultPath string) ([]string, error)` | 验证和归一化端点列表 |
| `BuildConnectionConfig` | `func(cfg *Config, defaults ConnectionConfigDefaults) *Config` | 填充默认值的兼容构建器 |

##### 4.10.1.1 Config 字段

```go
type Config struct {
    // 核心字段
    Endpoints           []string          // WebSocket 端点列表（ws://host:port 或 wss://host:port）
    User                string            // 用户名
    Passwd              string            // 密码
    DbName              string            // 默认数据库
    ChanLength          uint              // 内部消息 channel 长度，默认 1
    AutoReconnect       bool              // 是否启用自动重连
    ReconnectIntervalMs int               // 重连间隔（毫秒），默认 2000
    ReconnectRetryCount int               // 重连重试轮数，默认 3

    // 超时
    ReadTimeout         time.Duration     // 读取超时
    WriteTimeout        time.Duration     // 写入超时

    // 压缩
    EnableCompression   bool              // WebSocket 压缩

    // 认证
    Token               string            // 云服务 Token
    BearerToken         string            // Bearer Token
    TotpCode            string            // TOTP 验证码

    // 其他
    Timezone            *time.Location    // 时区
    InterpolateParams   bool              // 参数插值，默认 true
    Params              map[string]string // 透传参数

    // 向后兼容（Deprecated，用 Endpoints 替代）
    Net                 string            // "ws" 或 "wss"
    Addr                string            // 主机地址
    Port                int               // 端口
}
```

#### 4.10.2 Client（统一客户端）

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `NewClient` | `func(cfg *Config, defaultPath string, opts ...Option) (*Client, error)` | 创建客户端实例 |
| `NewClientFromDSN` | `func(dsn string, defaultPath string) (*Client, error)` | 从 DSN 创建客户端 |
| `Connect` | `func() error` | 建立连接并执行 Bootstrap 握手 |
| `Close` | `func()` | 关闭客户端及底层连接 |
| `IsClosed` | `func() bool` | 是否已关闭 |
| `Config` | `func() Config` | 返回当前配置副本 |
| `SetErrorHandler` | `func(handler func(error))` | 设置异步错误回调 |
| `Ping` | `func() error` | 发送 WebSocket Ping 帧 |
| `Exec` | `func(reqID int64, sql string) (int, error)` | 执行 SQL（返回 affected rows） |
| `Query` | `func(reqID int64, sql string) (*ResultSet, error)` | 执行 SQL 查询（返回结果集） |
| `InitStmt` | `func(reqID int64) (*Stmt, error)` | 初始化 stmt2 句柄 |
| `SchemalessInsert` | `func(reqID int64, lines string, protocol int, precision string, ttl int, tableNameKey string) error` | Schemaless 写入 |

> `reqID` 传 0 时自动通过 `common.GetReqID()` 生成。
>
##### 4.10.2.1 Option 函数

| Option | 说明 |
| --- | --- |
| `WithDialFunc(DialFunc)` | 覆盖 WebSocket 连接创建方式 |
| `WithClientFactory(ClientFactory)` | 覆盖 Runtime 构建方式 |

#### 4.10.3 Connector（连接工厂）

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `NewConnector` | `func(cfg *Config, defaultPath string) (*Connector, error)` | 从配置创建连接工厂 |
| `NewConnectorFromDSN` | `func(dsn string, defaultPath string) (*Connector, error)` | 从 DSN 创建连接工厂 |
| `Config` | `func() Config` | 返回配置副本 |
| `Connect` | `func() (*Client, error)` | 创建并连接新客户端 |

#### 4.10.4 Stmt（参数化语句）

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `Prepare` | `func(reqID int64, sql string) error` | 预编译 SQL，支持自动重连后 re-prepare |
| `Bind` | `func(params []*commonstmt.TaosStmt2BindData) error` | 绑定 stmt2 数据（推荐方式） |
| `BindParam` | `func(params []*param.Param, bindType *param.ColumnType) error` | 兼容绑定（Deprecated） |
| `SetTableName` | `func(name string) error` | 设置表名（Deprecated，用 Bind 替代） |
| `SetTags` | `func(tags *param.Param, bindType *param.ColumnType) error` | 设置 Tags（Deprecated，用 Bind 替代） |
| `AddBatch` | `func() error` | 缓存当前批次（Deprecated，用 Bind 替代） |
| `Exec` | `func(reqID int64) (int, error)` | 执行已绑定的数据，返回 affected rows |
| `UseResult` | `func(reqID int64) (*ResultSet, error)` | 获取查询语句的结果集 |
| `IsInsert` | `func() (bool, error)` | 是否为插入语句 |
| `ColFields` | `func() ([]*commonstmt.StmtField, error)` | 获取列字段元数据 |
| `AffectedRows` | `func() int` | 上次 Exec 的 affected rows |
| `Close` | `func(reqID int64) error` | 关闭 stmt2 句柄 |

> **绑定模式互斥**：`Bind`（Raw 模式）和 `SetTableName/SetTags/BindParam/AddBatch`（Compat 模式）在同一个 Prepare 周期内不可混用。
>
#### 4.10.5 ResultSet（查询结果集）

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `Next` | `func(dest []driver.Value) error` | 读取下一行，EOF 时返回 `io.EOF` |
| `Close` | `func() error` | 释放服务端结果资源 |
| `Columns` | `func() []string` | 返回列名列表 |
| `ColumnTypeDatabaseTypeName` | `func(index int) string` | 返回 TAOS 类型名 |
| `ColumnTypeLength` | `func(index int) (int64, bool)` | 返回列定长元数据 |
| `ColumnTypeScanType` | `func(index int) reflect.Type` | 返回扫描目标 Go 类型 |
| `ColumnTypePrecisionScale` | `func(index int) (precision, scale int64, ok bool)` | 返回 DECIMAL 精度和小数位 |

#### 4.10.6 TMQConsumer（消息队列消费者）

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `NewTMQConsumer` | `func(conf *tmq.ConfigMap) (*TMQConsumer, error)` | 创建消费者 |
| `Subscribe` | `func(topic string, rebalanceCb RebalanceCb) error` | 订阅单个 topic |
| `SubscribeTopics` | `func(topics []string, rebalanceCb RebalanceCb) error` | 订阅多个 topic |
| `Poll` | `func(timeoutMs int) tmq.Event` | 拉取消息 |
| `Commit` | `func() ([]tmq.TopicPartition, error)` | 提交消费位点 |
| `CommitOffsets` | `func(offsets []tmq.TopicPartition) ([]tmq.TopicPartition, error)` | 提交指定位点 |
| `Assignment` | `func() ([]tmq.TopicPartition, error)` | 获取分区分配 |
| `Seek` | `func(partition tmq.TopicPartition, ignoredTimeoutMs int) error` | 定位到指定 offset |
| `Committed` | `func(partitions []tmq.TopicPartition, timeoutMs int) ([]tmq.TopicPartition, error)` | 查询已提交位点 |
| `Position` | `func(partitions []tmq.TopicPartition) ([]tmq.TopicPartition, error)` | 查询当前消费位置 |
| `Unsubscribe` | `func() error` | 取消订阅 |
| `Close` | `func() error` | 关闭消费者 |

##### 4.10.6.1 TMQ ConfigMap 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `ws.url` | string | 必填 | 端点 URL，多节点用逗号分隔 |
| `td.connect.user` | string | `""` | 用户名 |
| `td.connect.pass` | string | `""` | 密码 |
| `group.id` | string | `""` | 消费组 ID |
| `client.id` | string | `""` | 客户端 ID |
| `auto.offset.reset` | string | `""` | 偏移重置策略 |
| `enable.auto.commit` | string | `""` | 是否自动提交 |
| `auto.commit.interval.ms` | string | `"5000"` | 自动提交间隔（毫秒） |
| `experimental.snapshot.enable` | string | `""` | 是否启用快照 |
| `msg.with.table.name` | string | `""` | 消息是否携带表名 |
| `session.timeout.ms` | string | `""` | 会话超时（毫秒） |
| `max.poll.interval.ms` | string | `""` | 最大 Poll 间隔（毫秒） |
| `timezone` | string | `""` | 时区 |
| `ws.message.channelLen` | uint | `0` | 消息 channel 长度 |
| `ws.message.timeout` | duration | `DefaultMessageTimeout` | 消息超时 |
| `ws.message.writeWait` | duration | `DefaultWriteWait` | 写入等待超时 |
| `ws.message.enableCompression` | bool | `false` | 是否启用压缩 |
| `ws.autoReconnect` | bool | `false` | 是否自动重连 |
| `ws.reconnectIntervalMs` | int | `2000` | 重连间隔（毫秒） |
| `ws.reconnectRetryCount` | int | `3` | 重连重试轮数 |

#### 4.10.7 Error（统一错误模型）

| 类型/函数 | 签名 | 说明 |
| --- | --- | --- |
| `Error` | struct | 统一错误结构体，实现 `error` 接口 |
| `ErrorType` | `string` 别名 | 错误分类标识 |
| `ErrorTypeOf` | `func(err error) ErrorType` | 提取错误类型 |
| `IsErrorType` | `func(err error, t ErrorType) bool` | 判断错误类型 |
| `IsConnectionRelatedError` | `func(err error) bool` | 是否连接相关 |
| `IsConnectionDisconnectedError` | `func(err error) bool` | 是否连接断开 |
| `IsReconnectFailedError` | `func(err error) bool` | 是否重连失败 |

##### 4.10.7.1 预定义错误常量

| 错误变量 | 类型 | 说明 |
| --- | --- | --- |
| `ErrNilConfig` | `invalid_config` | 配置为 nil |
| `ErrNoEndpoints` | `invalid_config` | 未配置端点 |
| `ErrConnectTimeout` | `connect_timeout` | 连接超时 |
| `ErrQueryMessageTimeout` | `message_timeout` | 查询消息超时 |
| `ErrStmtMessageTimeout` | `message_timeout` | Stmt 消息超时 |
| `ErrSchemalessMessageTimeout` | `message_timeout` | Schemaless 消息超时 |
| `ErrUnifiedClosed` | `client_closed` | 客户端已关闭 |
| `ErrUnifiedConnectFailed` | `reconnect_failed` | 连接/重连失败 |
| `ErrStmtConnectionLost` | `client_closed` | Stmt 连接丢失 |
| `ErrQueryResultConnectionLost` | `client_closed` | 结果集连接丢失 |
| `ErrQueryResultClosed` | `invalid_state` | 结果集已关闭 |
| `ErrStmtNotPrepared` | `invalid_state` | 语句未 Prepare |
| `ErrStmtReprepareSchemaChanged` | `invalid_state` | 重连后 Schema 变化 |
| `ErrStmtNoBatchAdded` | `invalid_state` | 未绑定数据 |
| `ErrStmtBindAfterCompatAPI` | `invalid_state` | Bind 和兼容 API 混用 |
| `ErrStmtCompatAPIAfterBind` | `invalid_state` | 兼容 API 和 Bind 混用 |

#### 4.10.8 Schemaless 协议常量

| 常量 | 值 | 说明 |
| --- | --- | --- |
| `InfluxDBLineProtocol` | 1 | InfluxDB Line Protocol |
| `OpenTSDBTelnetLineProtocol` | 2 | OpenTSDB Telnet Protocol |
| `OpenTSDBJsonFormatProtocol` | 3 | OpenTSDB JSON Protocol |

## 5. 性能

1. **消息路由**：`pendingRequests` 使用 `map[uint64]*pendingRequest` + RWMutex，O(1) 查找，热路径通过 `atomic.Value` 提供 Runtime 快照避免锁争用。
2. **结果集预取**：后台预取下一个数据块，减少用户遍历时的等待。
3. **Envelope 池化**：复用底层消息缓冲，减少内存分配和 GC 开销。
4. **stmt2 内存操作**：使用 `runtime.memmove` 替代逐字节拷贝，提升二进制帧组装性能。
5. **无性能退化**：原有功能通过薄包装层委托到 unified，额外开销仅为一次方法调用和错误映射。

## 6. 安全

1. **敏感信息保护**：错误消息中对 OTP（一次性密码）等敏感字段进行脱敏处理（`request_log_context.go` 中的 OTP redaction）。
2. **DSN 安全**：密码不出现在错误日志和 DSN 字符串回显中。
3. **连接认证**：Bootstrap 握手过程中通过安全通道传递认证凭据。

## 7. 兼容性

**完全向后兼容**。所有现有外部接口保持不变：

| 包 | 兼容策略 |
| --- | --- |
| `taosWS` | `Config` 为 `unified.Config` 别名，错误变量通过映射函数保留 |
| `ws/stmt` | API 标记 Deprecated 但仍可用，内部委托 unified |
| `ws/schemaless` | `NewSchemaless` 签名不变，仅单节点，多节点 failover 需使用 `unified.Client` |
| `ws/tmq` | `Consumer` 接口不变，内部委托 `unified.TMQConsumer` |
| `database/sql` | 标准 driver 接口不变，DSN 格式向后兼容 |

旧有 DSN 格式（单节点 `ws://host:port/db`）仍然支持。

## 8. 运维

1. **多节点部署**：通过 DSN 或 `Endpoints` 配置多个 taosAdapter 节点，实现客户端侧负载分散和故障自动恢复。
2. **自动重连**：通过 `autoReconnect=true` 启用，减少运维干预。
3. **诊断增强**：`RequestSummary` 提供失败请求的完整上下文（操作类型、目标地址、req_id 等），方便故障排查。

## 9. 使用场景

1. **单节点写入/查询**：与重构前行为一致，无需修改代码。
2. **多节点高可用写入**：配置多个 taosAdapter 端点，某节点故障时自动切换到其他节点继续写入。
3. **Stmt2 高性能批量写入**：通过 stmt2 协议实现多表绑定批量写入，配合自动重连保障长时间运行任务的稳定性。
4. **Schemaless 多节点采集**：IoT 场景下通过 InfluxDB Line Protocol 写入，配合 failover 提升数据采集可靠性。
5. **TMQ 消费高可用**：消费者断连后自动重连并重新订阅，避免消息处理中断。
6. **database/sql 标准接口**：通过 Go 标准 `database/sql` 接口使用，DSN 配置多节点即可获得 failover 能力。

## 10. 约束和限制

**约束：**

- WebSocket 协议要求 taosAdapter 服务在目标端口上可访问。
- 多节点 failover 要求所有节点的 taosAdapter 版本一致。
- `autoReconnect` 仅在配置为 `true` 时生效。
**限制：**
- `ResultSet`（查询结果集）绑定到特定连接，不支持跨连接 failover。如果在结果遍历过程中连接断开，需要重新执行查询。
- Stmt2 重连后若 schema 发生变化，将返回 `ErrStmtReprepareSchemaChanged`，需要用户重新初始化 statement。
- Failover 切换不保证零数据丢失——已发送但未确认的请求可能需要重试。
- TMQ 重连后的 offset 取决于服务端提交状态，可能出现少量重复消费。

## 11. 常见错误和排查

| 错误 | 原因 | 排查方法 |
| --- | --- | --- |
| `connection refused` | taosAdapter 不可用 | 检查目标节点 taosAdapter 服务状态和端口 |
| `all endpoints exhausted` | 所有候选节点均不可达 | 检查所有配置端点的网络连通性 |
| `ErrStmtReprepareSchemaChanged` | 重连后表 schema 已变化 | 关闭旧 Stmt，用新 schema 重新 Prepare |
| `timeout` | 请求超时未收到响应 | 检查网络延迟和服务端负载，调整超时参数 |
| `req_id mismatch` | 消息路由异常 | 检查是否存在并发安全问题或协议版本不匹配 |

## 12. 可观测性

本次重构不涉及 taos shell、taos Explorer、TDinsight 等 UI 组件的行为变化。
增强了 driver 侧的诊断能力：

- 每个失败请求附带 `RequestSummary`（包含操作类型、目标地址、req_id）。
- 连接状态可通过 `HasConnection()`、`LastError()` 方法检查。

## 13. 安装和卸载

无额外安装或卸载要求。本次变更为 driver-go 库内部重构，用户通过常规的 Go module 方式更新依赖即可：

```bash
go get github.com/taosdata/driver-go/v3@latest
```

## 14. 文档

需要更新官网文档

## 15. 参考文档

- `ws/ARCHITECTURE.md`：三层架构设计说明（Transport / Protocol / Recovery）
- `ws/RELIABILITY.md`：可靠性契约，确保同一时刻仅一个活跃 Runtime 被发布

## 16. 附录

### 16.1 A. 三层架构总览

```plaintext
┌─────────────────────────────────────────────────────┐
│                  Legacy Packages                     │
│  taosWS │ ws/stmt │ ws/schemaless │ ws/tmq          │
│  (thin wrappers / database/sql adapters)            │
├─────────────────────────────────────────────────────┤
│               ws/unified (Management)                │
│  Client · Failover · Reconnect · Message Routing    │
│  Query · Stmt/Stmt2 · Schemaless · TMQ Consumer     │
├─────────────────────────────────────────────────────┤
│               ws/client (Transport)                  │
│  WebSocket conn · Read/Write pumps · Envelope pool  │
└─────────────────────────────────────────────────────┘
```

### 16.2 B. Failover 流程

```plaintext
操作失败 (网络错误)
    │
    ▼
sendWithReconnect 捕获
    │
    ▼
reconnectWithBootstrap
    │
    ├─ 获取 reconnectLock
    ├─ 检查 Generation（防止陈旧重连）
    ├─ failoverState.reconnectCandidates()
    │   └─ 先尝试当前活跃节点；其余节点按最少连接数排序
    │
    ▼
  逐一尝试候选节点
    │
    ├─ 建立新 WebSocket 连接
    ├─ 执行 Bootstrap 握手
    ├─ swapRuntime（原子替换 + Generation++）
    │
    ▼
  在新 Runtime 上重试操作
```
