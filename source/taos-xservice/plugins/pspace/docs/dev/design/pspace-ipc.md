## 3. 按数据类型分组（Data Type Grouping）

### 功能描述

将数据点按其数据类型（`dataType`）进行分组，每种数据类型使用独立的 IPC 通道（TCP 连接）与 taosx 通信。这与 OPC 插件的实现模式一致。

### 分组原因

- taosx 端按 Arrow Schema 区分数据流，不同数据类型的 `value` 列对应不同的 Arrow `DataType`
- 每种数据类型有独立的 schema，需要独立的 TCP 连接来传输
- taosx 端按 schema 匹配 persist queue 和写入通道

### OPC 的分组方式（参考）

OPC 支持 13 种数据类型，每种类型对应独立的 Arrow Schema 和持久化目录：

| pSpace DataType | Arrow DataType          | 目录后缀     |
| --------------- | ----------------------- | ------------ |
| `BOOL`          | `Boolean`               | `_bool`      |
| `INT8`          | `Int8`                  | `_int8`      |
| `UINT8`         | `UInt8`                 | `_uint8`     |
| `INT16`         | `Int16`                 | `_int16`     |
| `UINT16`        | `UInt16`                | `_uint16`    |
| `INT32`         | `Int32`                 | `_int32`     |
| `UINT32`        | `UInt32`                | `_uint32`    |
| `INT64`         | `Int64`                 | `_int64`     |
| `UINT64`        | `UInt64`                | `_uint64`    |
| `FLOAT`         | `Float32`               | `_float`     |
| `DOUBLE`        | `Float64`               | `_double`    |
| `STRING`        | `Utf8`                  | `_str`       |
| `TIMESTAMP`     | `Timestamp(Nanosecond)` | `_timestamp` |

### Arrow Schema 模板

每种数据类型共享相同的 schema 结构，仅 `value` 列的类型不同：

```
fields:
  - id:       Utf8          (数据点 ID)
  - name:     Utf8          (数据点名称)
  - ts:       Timestamp(ms) (原始时间戳)
  - received: Timestamp(ms) (接收时间)
  - value:    <DataType>    (数据值，类型按分组决定)
  - status:   Int64         (数据质量)
  - request:  Timestamp(ms) (请求时间)

metadata:
  version: "1.0"
  stream:  "point"
  ack:     "lush"
```

### IPC 连接架构

```
                                    taosx (Rust)
                                   ┌──────────────┐
taosx-pspace (Java)                │  TCP Listener │
┌──────────────────┐               │  (build_ipc)  │
│                  │  TCP conn 1   │               │
│  Double 点位组   ├──────────────→│  schema_double│
│                  │               │               │
│  Int64 点位组    ├──────────────→│  schema_int64 │
│                  │  TCP conn 2   │               │
│  String 点位组   ├──────────────→│  schema_str   │
│                  │  TCP conn 3   │               │
│  ...             │               │  ...          │
└──────────────────┘               └──────────────┘
```

### pSpace 数据类型映射

pSpace SDK 的 `PsData.getDataType()` 返回 `PsDataTypeEnum`，需要映射到对应的 Arrow `DataType`。具体的枚举值待确认 SDK 文档，但核心逻辑为：

```java
// 按数据类型对点位进行分组
Map<PsDataTypeEnum, List<Long>> groupedPoints = points.stream()
    .collect(Collectors.groupingBy(
        Point::getDataType,
        Collectors.mapping(Point::getId, Collectors.toList())
    ));

// 每个分组建立独立的 TCP 连接和 ArrowWriter
for (Map.Entry<PsDataTypeEnum, List<Long>> entry : groupedPoints.entrySet()) {
    PsDataTypeEnum dataType = entry.getKey();
    List<Long> pointIds = entry.getValue();

    // 创建该数据类型的 schema 和 Netty 连接
    Schema schema = buildSchema(mapToArrowType(dataType));
    PSpaceNettyClient client = PSpaceNettyClient.fromRemote(remote);
    client.connect();
    // ... 发送表定义和数据
}
```

### 实现要点

1. **获取数据类型**：在点位查询阶段（`Points.getPoints`）需要获取每个点位的 `dataType`，参见 [pspace-points.md](pspace-points.md) 中 `includeDataType` 参数
2. **分组时机**：在查询/订阅数据前完成分组，每个分组独立建立连接
3. **连接管理**：需要管理多个 Netty TCP 连接的生命周期
4. **回调路由**：Subscribe 模式的回调数据需要按 `dataType` 路由到对应的连接
