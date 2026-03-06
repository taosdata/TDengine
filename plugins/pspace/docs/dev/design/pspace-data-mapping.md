# pSpace 数据映射与 Arrow Schema 设计

## 概述

本文档定义 pSpace 数据到 taosX Arrow IPC 协议的字段映射关系。该映射适用于所有运行模式（Query、QuerySync、Subscribe）。

## pSpace 数据源字段

从 pSpace SDK 查询/订阅到的数据包含以下属性：

| pSpace 属性 | Java 类型       | 来源                                | 说明                                        |
| ----------- | --------------- | ----------------------------------- | ------------------------------------------- |
| `tagId`     | `Long`          | `PsHisData.getTagId()`              | 数据点的唯一标识                            |
| `name`      | `String`        | `PsTagBase.getName()` / Points 查询 | 数据点的名称                                |
| `value`     | `Object`        | `PsData.getValue()`                 | 数据值，类型由 `dataType` 决定（如 DOUBLE） |
| `timestamp` | `long`          | `PsData.getTimestamp()`             | pSpace server 中的原始时间戳（epoch 毫秒）  |
| `quality`   | `PsQualityEnum` | `PsData.getQuality()`               | 数据质量枚举（如 GOOD、BAD 等）             |

## taosX Arrow Schema 字段

上报给 taosX 的 Arrow IPC RecordBatch 包含以下字段：

| Arrow 字段 | Arrow 类型              | 说明                      |
| ---------- | ----------------------- | ------------------------- |
| `id`       | `Binary`（String）      | 数据点 ID                 |
| `name`     | `Binary`（String）      | 数据点名称                |
| `ts`       | `Timestamp(NANOSECOND)` | 原始数据时间戳            |
| `received` | `Timestamp(NANOSECOND)` | 接收到数据的时间戳        |
| `value`    | `Float64`（Double）     | 数据值                    |
| `status`   | `Int64`（BigInt）       | 数据质量状态码            |
| `request`  | `Timestamp(NANOSECOND)` | 发起查询/订阅请求的时间戳 |

## 字段映射关系

```
pSpace SDK                          Arrow Schema (taosX)
───────────────                     ────────────────────
PsHisData.tagId (Long)         ──→  id       (String)       // Long 转 String
Point.name (String)            ──→  name     (String)       // 数据点名称
PsData.timestamp (long, ms)    ──→  ts       (Timestamp ns) // ms × 1_000_000 转纳秒
（收到数据时的系统时间）          ──→  received (Timestamp ns) // System.currentTimeMillis() × 1_000_000
PsData.value (Object)          ──→  value    (Double)       // Number 转 double
PsData.quality (PsQualityEnum) ──→  status   (Int64)        // 枚举转整数值
（发起请求时的系统时间）          ──→  request  (Timestamp ns) // 查询/订阅发起时刻
```

## 字段转换详细说明

### id: tagId → String

pSpace 中数据点 ID 为 `Long` 类型，需要通过 `String.valueOf(tagId)` 转为字符串发送给 taosX。

### name: 数据点名称

数据点名称来源于 Points 查询阶段获取的 `Point.name`（即 `PsTagBase.getName()`），需要在查询历史数据前预先加载并建立 `tagId → name` 的映射关系。

### ts: timestamp → 纳秒

pSpace 返回的 `PsData.getTimestamp()` 是 epoch 毫秒，需要乘以 `1_000_000` 转为纳秒，写入 `Timestamp(NANOSECOND)` 类型的 Arrow 向量。

### received: 接收时间戳

taosx-pspace 插件收到 pSpace SDK 回调/返回数据时的系统时间。

- 对于 Query/QuerySync（`hisReadRawAsync` 回调）：在回调函数中取 `System.currentTimeMillis()`
- 对于 Subscribe：在订阅回调中取 `System.currentTimeMillis()`，与 `request` 相同

同样需要乘以 `1_000_000` 转为纳秒。

### value: Object → Double

`PsData.getValue()` 返回 `Object`，需要转为 `Double`：

```java
if (ps.getValue() != null) {
    valVector.setSafe(i, ((Number) ps.getValue()).doubleValue());
} else {
    valVector.setNull(i);
}
```

### status: PsQualityEnum → Int64

`PsData.getQuality()` 返回 `com.sunwayland.pspace.enums.PsQualityEnum` 枚举，需要转为 `Int64` 整数值发送给 taosX。

可以使用 `quality.ordinal()` 或 `quality.getValue()`（取决于 SDK 提供的方法）获取整数值。

### request: 请求时间戳

taosx-pspace 插件发起查询/订阅请求的时间戳。

- 对于 Query/QuerySync：调用 `hisReadRawAsync` 之前记录 `System.currentTimeMillis()`，同一批次内的所有数据共享同一个 request 时间
- 对于 Subscribe：与 `received` 相同，都是订阅回调中收到数据时的 `System.currentTimeMillis()`（因为 Subscribe 模式是被动接收推送，没有独立的"请求"时刻）

同样需要乘以 `1_000_000` 转为纳秒。

## Arrow Schema 定义代码示意

```java
List<ArrowInitDto.Column> columns = new ArrayList<>();
columns.add(new ArrowInitDto.Column("id",       "string"));     // 数据点 ID
columns.add(new ArrowInitDto.Column("name",     "string"));     // 数据点名称
columns.add(new ArrowInitDto.Column("ts",       "timestamp"));  // 原始时间戳
columns.add(new ArrowInitDto.Column("received", "timestamp"));  // 接收时间
columns.add(new ArrowInitDto.Column("value",    "double"));     // 数据值
columns.add(new ArrowInitDto.Column("status",   "bigint"));     // 质量状态码
columns.add(new ArrowInitDto.Column("request",  "timestamp"));  // 请求时间
```

## 写入数据代码示意

```java

```

## 相关代码

- 点位信息：[Point.java](../../../src/main/java/com/taosdata/taosx/pspace/Point.java)
- 点位查询（获取 name）：[Points.java](../../../src/main/java/com/taosdata/taosx/pspace/Points.java)
