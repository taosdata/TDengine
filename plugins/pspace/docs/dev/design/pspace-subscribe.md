# pSpace 实时订阅模式设计（Subscribe）

## 概述

Subscribe 模式通过 pSpace SDK 的 `subscribe` 实时订阅接口，接收 pSpace 推送的实时数据，写入 TSDB 数据库，完成实时数据同步。

**当前状态**：尚未实现（`SubscribeTask` 为 placeholder），SDK 调用细节已确认。

## 功能描述

- **功能**：启动实时订阅，持续接收 pSpace 推送的数据变化，写入 TSDB 数据库
- **底层 SDK 方法**：`client.realNewSubscribeAndRead`
- **生命周期**：长期运行，直到连接断开或主动停止

## 预期执行流程

1. 获取点位列表（规则见 [pspace-points.md](pspace-points.md)）
2. 连接 pSpace 和 taosX（Netty TCP）
3. 发送表定义（Arrow IPC）
4. 定义 `IRealCallback` 回调函数
5. 调用 `client.realNewSubscribeAndRead(tagIds, callbackList)` 发起订阅
6. 解析返回的 `PsResult<PsSubRealData>`，获取初值并发送到 taosX
7. 后续数据变化通过回调函数推送，在回调中将数据序列化为 Arrow IPC 发送到 taosX
8. 持续运行（`Thread.sleep` 或阻塞等待），直到连接断开

## 配置参数

| 参数 | TOML 字段 | 类型   | 说明                     |
| ---- | --------- | ------ | ------------------------ |
| 模式 | `mode`    | String | 必填，值为 `"Subscribe"` |

> Subscribe 模式不需要 `start_time`、`end_time`、`time_window` 等时间参数，因为它是实时推送而非查询。

## TOML 配置示例

```toml
[run]
mode = "Subscribe"
```

## 底层 SDK 接口

### `client.realNewSubscribeAndRead` — 新增订阅并获取初值

```java
PsResult<PsSubRealData> result = client.realNewSubscribeAndRead(tagIds, callbackList);
```

| 参数           | 类型                  | 说明                         |
| -------------- | --------------------- | ---------------------------- |
| `tagIds`       | `List<Long>`          | 要订阅的点位 ID 列表         |
| `callbackList` | `List<IRealCallback>` | 回调函数列表，数据变化时触发 |

**返回值**：`PsResult<PsSubRealData>`

- `result.getCode()` 为 `PsErrorCodeEnum.PSRET_OK` 时订阅成功
- `result.getData()` 返回 `List<PsSubRealData>`，即各点位的**初值**（订阅时刻的当前值）
- 每个 `PsSubRealData` 中包含 `subId`（订阅号），后续推送的数据也携带相同的 `subId`

### `IRealCallback` — 实时数据回调接口

```java
IRealCallback callback = (int subId, List<PsSubRealData> subRealData) -> {
    // subId: 订阅号，与 realNewSubscribeAndRead 返回的 subId 对应
    // subRealData: 本次推送的数据列表，每个元素对应一个点位
};
```

- **推送时机**：当订阅的测点值发生变化时，pSpace 服务端主动推送数据到回调
- **推送粒度**：每次推送包含所有订阅点位的最新值（`List<PsSubRealData>`）
- **推送频率**：取决于 pSpace 服务端数据变化频率（示例中约 1 秒/次）

### `PsSubRealData` — 订阅数据结构

```
PsSubRealData
├── subId      (long)           // 订阅号
├── tagId      (long)           // 点位 ID
├── value      (Object)         // 数据值（类型由 dataType 决定）
├── dataType   (PsDataTypeEnum) // 数据类型（如 DOUBLE）
├── timestamp  (long)           // 时间戳（epoch 毫秒）
├── quality    (PsQualityEnum)  // 数据质量（如 GOOD）
└── code       (PsErrorCodeEnum)// 该点位的状态码（如 PSRET_OK）
```

与历史查询返回的 `PsData` 相比，`PsSubRealData` 多了 `subId` 和 `tagId` 字段，但核心数据属性（`value`、`timestamp`、`quality`）相同，字段映射规则见 [pspace-data-mapping.md](pspace-data-mapping.md)。

### 示例输出

```
订阅成功，订阅号: 3
初值：
  PsSubRealData(code=PSRET_OK, subId=3, tagId=150019, value=31.395547, dataType=DOUBLE, timestamp=1772597122874, quality=GOOD)
  PsSubRealData(code=PSRET_OK, subId=3, tagId=150021, value=31.559462, dataType=DOUBLE, timestamp=1772597122874, quality=GOOD)
  PsSubRealData(code=PSRET_OK, subId=3, tagId=150023, value=31.096435, dataType=DOUBLE, timestamp=1772597122874, quality=GOOD)
等待推送数据...
收到推送数据，订阅号: 3
  PsSubRealData(code=PSRET_OK, subId=3, tagId=150019, value=31.522111, dataType=DOUBLE, timestamp=1772597123874, quality=GOOD)
  PsSubRealData(code=PSRET_OK, subId=3, tagId=150021, value=32.028779, dataType=DOUBLE, timestamp=1772597123874, quality=GOOD)
  PsSubRealData(code=PSRET_OK, subId=3, tagId=150023, value=31.468154, dataType=DOUBLE, timestamp=1772597123874, quality=GOOD)
  ...（约 1 秒/次持续推送）
```

### 实现要点

1. **初值处理**：`realNewSubscribeAndRead` 返回的初值也需要作为数据发送到 taosX，不要丢弃
2. **回调线程安全**：回调在 SDK 内部线程中执行，操作共享资源（如 Netty client）时需注意线程安全
3. **阻塞主线程**：订阅后主线程需要保持存活（如 `Thread.sleep(Long.MAX_VALUE)` 或使用 `CountDownLatch`），否则进程退出订阅也会结束
4. **取消订阅**：通过 `client.disconnect()` 断开连接来结束订阅

## 与 Query/QuerySync 模式的对比

| 维度            | Query        | QuerySync        | Subscribe                 |
| --------------- | ------------ | ---------------- | ------------------------- |
| SDK 方法        | `histRead`   | `histRead`       | `realNewSubscribeAndRead` |
| 数据来源        | 历史数据查询 | 历史数据查询     | 实时推送                  |
| 运行方式        | 一次性       | 先回填再持续轮询 | 持续订阅                  |
| 需要 start_time | 是           | 是               | 否                        |
| 退出条件        | 查询完成     | 连接断开         | 连接断开                  |

## 相关代码

- Subscribe 实现：[SubscribeTask.java](../../../src/main/java/com/taosdata/taosx/pspace/run/SubscribeTask.java)（当前为 placeholder）
- 模式分发：[TaosXpSpaceMain.java](../../../src/main/java/com/taosdata/taosx/TaosXpSpaceMain.java)（`runTask` 方法）
