# 流计算事件通知机制 FS

## 1. 背景

JIRA: [TS-5469](https://jira.taosdata.com:18080/browse/TS-5469)
在流计算功能中，窗口(如时间窗口、状态窗口、会话窗口等)是数据聚合和处理的重要方式。当前， 流计算过程中，窗口的打开、关闭等事件仅在 taosd 内部处理，无法通知外部系统，难以满足外部系统的实时监控、报警及事件驱动需求。
因此，为了增强流计算的交互能力，本功能支持在窗口事件发生时向指定目标发送通知，涵盖多种窗口类型、事件类型。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/12/04 | 0.1 | 邝金清 | 初稿 |
| 2024/12/09 | 0.2 | 邝金清 | 根据线下需求讨论结果修改 |
| 2025/02/06 | 1.0 | 邝金清 | 根据最终实现修改部分细节描述 |

## 3. 定义

- **事件类型 (Event Types):** 指流计算过程中可能触发的特定事件，包括窗口打开、窗口关闭。
- **通知地址 (Notification Address):** 用于接收通知的目标地址，包括协议、IP 地址或域名、端口号，并允许包含路径。

## 4. 行为说明

### 4.1 创建通知

支持用户在创建流时指定通知地址和触发事件类型，通过 NOTIFY 和 ON 关键字配置：
```sql
CREATE STREAM [IF NOT EXISTS] stream_name [stream_options]
  INTO stb_name[(field1_name, field2_name [PRIMARY KEY], ...)]
  [TAGS (create_definition [, create_definition] ...)]
  SUBTABLE(expression) AS subquery
  [NOTIFY (url [, url] ...) ON (event_type [, event_type] ...) [notification_options]]
        
event_type: {
    'WINDOW_OPEN'
  | 'WINDOW_CLOSE'
}

notification_options: {
    NOTIFY_HISTORY [0|1]
    ON_FAILURE     [DROP|PAUSE]
}
```

1. `url`: 指定通知的目标地址，必须包括协议、IP 或域名、端口号，并允许包含路径、参数。目前仅支持 websocket 协议。例如：'ws://localhost:8080'，'ws://localhost:8080/notify'，'wss://localhost:8080/notify?key=foo'。
   - 如果 URL 中包含参数，在发送通知时会将 URL 中的所有参数截断，放入到通知消息中，这个功能可以用于外部程序的连接验证。
2. `event_type`: 定义需要通知的事件，支持的事件类型有：
   - 'WINDOW_OPEN'：窗口打开事件，所有类型的窗口打开时都会触发
   - 'WINDOW_CLOSE'：窗口关闭事件，所有类型的窗口关闭时都会触发
3. `NOTIFY_HISTORY`: 控制是否在计算历史数据时触发通知，默认不触发。
4. `ON_``FAILURE`: 向通知地址发送通知失败时(比如网络不佳场景)是否允许丢弃部分事件，默认值为 PAUSE：
   - PAUSE 表示发送通知失败时暂停流计算任务。taosd 会重试发送通知，直到发送成功后，任务自动恢复运行。
   - DROP 表示发送通知失败时直接丢弃事件信息，流计算任务继续运行，不受影响。
以下示例创建一个流，计算电表电流的每分钟平均值，并在窗口打开、关闭时向两个通知地址发送通知，计算历史数据时也强制发送通知，并且不允许在通知发送失败时丢弃通知：
```sql
CREATE STREAM avg_current_stream FILL_HISTORY 1
  AS SELECT _wstart, _wend, AVG(current) FROM meters
  INTERVAL (1m)
  NOTIFY ('ws://localhost:8080/notify', 'wss://192.168.1.1:8080/notify?key=foo')
  ON ('WINDOW_OPEN', 'WINDOW_CLOSE');
  NOTIFY_HISTORY 1
  ON_FAILURE PAUSE;
```

### 4.2 通知消息格式

当触发指定的事件时，taosd 会向指定的 URL 发送 POST 请求，消息体为 JSON 格式。一个请求可能包含若干个流的若干个事件，且事件类型不一定相同。
事件信息视窗口类型而定：
1. 时间窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。
2. 状态窗口：开始时发送起始时间、前一个窗口的状态值(没有为null)、当前窗口的状态值；结束时发送起始时间、结束时间、计算结果、当前窗口的状态值、下一个窗口的状态值。
3. 会话窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。
4. 事件窗口：开始时发送起始时间，触发窗口打开的数据值和对应条件编号；结束时发送起始时间、结束时间、计算结果、触发窗口关闭的数据值和对应条件编号。
5. 计数窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。

#### 4.2.1 消息结构示例

```json
{
  "messageId": "unique-message-id-12345",
  "timestamp": 1733284887203,
  "streams": [
    {
      "streamName": "avg_current_stream",
      "events": [
        {
          "tableName": "t_a667a16127d3b5a18988e32f3e76cd30",
          "eventType": "WINDOW_OPEN",
          "eventTime": 1733284887097,
          "triggerId": "window-id-67890",
          "triggerType": "Interval",
          "windowStart": 1733284800000
        },
        {
          "tableName": "t_a667a16127d3b5a18988e32f3e76cd30",
          "eventType": "WINDOW_CLOSE",
          "eventTime": 1733284887197,
          "triggerId": "window-id-67890",
          "triggerType":  "Interval",
          "windowStart": 1733284800000,
          "windowEnd": 1733284860000,
          "result": {
            "_wstart": 1733284800000,
            "avg(current)": 1.3
          }
        }
      ]
    },
    {
      "streamName": "max_voltage_stream",
      "events": [
        {
          "tableName": "t_96f62b752f36e9b16dc969fe45363748",
          "eventType": "WINDOW_OPEN",
          "eventTime": 1733284887231,
          "triggerId": "window-id-13579",
          "triggerType": "Event",
          "windowStart": 1733284800000,
          "triggerCondition": {
            "conditionIndex": 0,
            "fieldValue": {
              "c1": 10,
              "c2": 15
            }
          },
        },
        {
          "tableName": "t_96f62b752f36e9b16dc969fe45363748",
          "eventType": "WINDOW_CLOSE",
          "eventTime": 1733284887231,
          "triggerId": "window-id-13579",
          "triggerType": "Event",
          "windowStart": 1733284800000,
          "windowEnd": 1733284810000,
          "triggerCondition": {
            "conditionIndex": 1,
            "fieldValue": {
              "c1": 20
              "c2": 3
            }
          },
          "result": {
            "_wstart": 1733284800000,
            "max(voltage)": 220
          }
        }
      ]
    }
  ]
}
```

#### 4.2.2 字段说明

##### 4.2.2.1 根级字段

1. "messageId": 字符串类型，是通知消息的唯一标识符，确保整条消息可以被追踪和去重。
2. "timestamp": 长整型时间戳，表示通知消息生成的时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
3. "streams": 对象数组，包含多个流任务的事件信息。(详细信息见下节)

##### 4.2.2.2 stream 对象的字段

1. "streamName": 字符串类型，流任务的名称，用于标识事件所属的流。
2. "events": 对象数组，该流任务下的事件列表，包含一个或多个事件对象。(详细信息见下节)

##### 4.2.2.3 event 对象的字段

###### 4.2.2.3.1 通用字段

这部分是所有 event 对象所共有的字段。
1. "tableName": 字符串类型，是对应目标子表的表名。
2. "eventType": 字符串类型，表示事件类型 ("WINDOW_OPEN", "WINDOW_CLOSE" 或 "WINDOW_INVALIDATION")。
3. "eventTime": 长整型时间戳，表示事件生成时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
4. "triggerId": 字符串类型，触发事件/窗口的唯一标识符，确保打开和关闭事件的 ID 一致，便于外部系统将两者关联。如果 taosd 发生故障重启，部分事件可能会重复发送，会保证同一触发事件/窗口的 triggerId 保持不变。
5. "triggerType": 字符串类型，表示窗口类型 ("Time", "State", "Session", "Event", "Count")。

###### 4.2.2.3.2 时间窗口相关字段

这部分是 "triggerType" 为"Time" 时 event 对象才有的字段。
1. 如果 "eventType" 为 "WINDOW_OPEN"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
2. 如果 "eventType" 为 "WINDOW_CLOSE"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "windowEnd": 长整型时间戳，表示窗口的结束时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "result": 计算结果，为键值对形式，包含窗口计算的结果列及其值。
  ```json
  // 计算语句为 select max(voltage), avg(current), first(description) as comment from d1001 interval(1m)
  // 那么一个窗口关闭事件的样例为：
  "windowStart": 1733284800000,
  "windowEnd": 1733284860000,
  "result": {
    "max(voltage)": 223,
    "avg(current)": 1.7,
    "comment": "a sample data"
   }
  ```

这里计算结果中的数据类型 受 json 类型限制，所以计算结果中：
1. 布尔类型以 Boolean类型 表示
2. 整数类型(包括时间戳类型)、浮点数类型以 数字类型 表示
3. 其他类型均以 字符串类型 表示

###### 4.2.2.3.3 状态窗口相关字段

这部分是 "triggerType" 为"State" 时 event 对象才有的字段。
1. 如果 "eventType" 为 "WINDOW_OPEN"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "prevState": 与状态列的类型相同，表示上一个窗口的状态值。如果没有上一个窗口(即: 现在是第一个窗口)，则为 NULL。
   - "curState": 与状态列的类型相同，表示当前窗口的状态值。
2. 如果 "eventType" 为 "WINDOW_CLOSE"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "windowEnd": 长整型时间戳，表示窗口的结束时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "curState": 与状态列的类型相同，表示当前窗口的状态值。
   - "nextState": 与状态列的类型相同，表示下一个窗口的状态值。
   - "result": 计算结果，为键值对形式，包含窗口计算的结果列及其值。

###### 4.2.2.3.4 会话窗口相关字段

这部分是 "triggerType" 为"Session" 时 event 对象才有的字段。
1. 如果 "eventType" 为 "WINDOW_OPEN"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
2. 如果 "eventType" 为 "WINDOW_CLOSE"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "windowEnd": 长整型时间戳，表示窗口的结束时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "result": 计算结果，为键值对形式，包含窗口计算的结果列及其值。

###### 4.2.2.3.5 事件窗口相关字段

这部分是 "triggerType" 为"Event" 时 event 对象才有的字段。
1. 如果 "eventType" 为 "WINDOW_OPEN"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "triggerCondition": 触发窗口开始的条件信息，包括以下字段：
      - "conditionIndex": 整型，表示满足的触发窗口开始的条件的索引，从0开始编号。
      - "fieldValue": 键值对形式，表示相关字段的当前值。
  ```json
  // 语句条件为 start with (c1 > 9) OR (c2 > 20)
  "triggerCondition": {
    "conditionIndex": 0,
    "fieldValue": {
      "c1": 10,
      "c2": 15
    }
  },
  ```

1. 如果 "eventType" 为 "WINDOW_CLOSE"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "windowEnd": 长整型时间戳，表示窗口的结束时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "triggerCondition": 触发窗口关闭的条件信息，包括以下字段：
      - "conditionIndex": 整型，表示满足的触发窗口关闭的条件的索引，从0开始编号。
      - "fieldValue": 键值对形式，表示相关字段的当前值。
   - "result": 计算结果，为键值对形式，包含窗口计算的结果列及其值。

###### 4.2.2.3.6 计数窗口相关字段

这部分是 "triggerType" 为"Count" 时 event 对象才有的字段。
1. 如果 "eventType" 为 "WINDOW_OPEN"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
2. 如果 "eventType" 为 "WINDOW_CLOSE"，则包含如下字段：
   - "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "windowEnd": 长整型时间戳，表示窗口的结束时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
   - "result": 计算结果，为键值对形式，包含窗口计算的结果列及其值。

###### 4.2.2.3.7 窗口失效相关字段

说明：因为流计算过程中会遇到数据乱序、更新、删除等情况，可能造成已生成的窗口被删除，或者结果需要重新计算。此时会向通知地址发送一条 "WINDOW_INVALIDATION" 的通知，说明哪些窗口已经被删除。
这部分是 "eventType" 为 "WINDOW_INVALIDATION" 时，event 对象才有的字段。
1. "windowStart": 长整型时间戳，表示窗口的开始时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。
2. "windowEnd": 长整型时间戳，表示窗口的结束时间，精确到毫秒，即: '00:00, Jan 1 1970 UTC' 以来的毫秒数。

### 4.3 系统表

##### 4.3.0.1 INS_STREAM_TASKS

新增一列：

| # | 列名 | 数据类型 | 说明 |
| --- | --- | --- | --- |
| 25 | notify_event_stat | varchar(350) | 通知事件产生和发送的统计信息，包括事件数量和相关的时间开销 |

### 4.4 延迟和可靠性保证

1. 正常情况下，可以保证**事件发送延迟在 1s 以内**。但如果系统资源紧张，或是与通知地址的网络通信不畅时，延迟可能更高。
2. 在网络畅通的场景下，每个事件通知都会保证发送。即使 taosd 发生故障而重启，也会保证恢复未发送的事件信息。但是故障重启后，流计算是从上一个检查点恢复计算，所以可能产生一些已发送的重复事件通知，会**保证重新生成的事件ID与已发送的事件ID完全相同**。

### 4.5 配置参数

通知发送采用了 websocket 连接，而部分 websocket 框架对于通信时的消息大小和帧大小存在限制。为兼容与不同框架的通信，在 taosd 中添加如下开发者选项：
1. `streamNotifyMessageSize`:
   - 说明：控制事件通知的消息大小，单位 KB。
   - 取值范围：8 - 1*1024*1024 (即 8KB - 1GB)
   - 默认值：8192 (即 8MB)
2. `streamNotifyFrameSize`:
   - 说明：控制每个通知消息发送时的底层帧大小，单位KB。
   - 取值范围：8 - 1*1024*1024 (即 8KB - 1GB)
   - 默认值：256 (即 256KB)

## 5. 性能

通常情况下，不影响任何场景的查询/写入性能。网络瓶颈时，可能因为消息发送占用部分网络 I/O 而性能略微下降。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

外部系统需要根据流计算窗口事件触发后续业务逻辑的场景。

## 9. 约束和限制

无。

## 10. 常见错误和排查

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

需要修改官网文档。

## 14. 参考文档

## 15. 附录
