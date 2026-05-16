# TRUE_FOR 表达式增强功能 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-22 | 2026-01-22 | 1.0 | 邝金清 | 初始版本 |

## 2. 背景

TDengine 的 TRUE_FOR 表达式用于 STATE_WINDOW 和 EVENT_WINDOW 窗口类型，可应用于流计算查询（CREATE STREAM）和普通查询（SELECT）。原本 TRUE_FOR 仅支持基于时间长度的窗口触发条件（如 `TRUE_FOR(10s)`），这在某些场景下存在局限性：
1. **数据到达率不稳定**：当数据到达速率波动较大时，仅基于时间的触发可能导致窗口内数据量差异很大
2. **基于数据量的处理需求**：某些业务场景需要在收集到特定数量的数据后才进行处理，而不关心时间长度
3. **复合触发条件需求**：需要同时满足时间和数据量两个条件，或者满足其中任一条件即可触发
本特性旨在增强 TRUE_FOR 表达式的能力，使其能够：
- 支持单独指定行数限制（COUNT）
- 支持同时指定时间长度和行数限制（使用 AND 逻辑）
- 支持同时指定时间长度和行数限制（使用 OR 逻辑）
通过这些增强，用户可以更灵活地控制窗口的触发时机，满足不同业务场景的需求。本特性既适用于流计算场景，也适用于普通查询场景。

## 3. 定义

- **TRUE_FOR 表达式**：TDengine 中用于指定窗口触发条件的语法结构，可用于流计算查询和普通查询
- **STATE_WINDOW**：状态窗口，根据某列值的变化来划分窗口
- **EVENT_WINDOW**：事件窗口，根据起始和结束条件来划分窗口
- **COUNT**：行数限制，指定窗口需要累积的数据行数
- **AND 逻辑**：要求时间长度和行数两个条件都满足才触发窗口
- **OR 逻辑**：时间长度或行数任一条件满足即触发窗口
- **窗口触发**：当满足 TRUE_FOR 指定的条件时，输出窗口的聚合结果
- **流计算查询**：使用 CREATE STREAM 创建的持续查询，自动处理新到达的数据
- **普通查询**：使用 SELECT 语句对历史数据进行的一次性查询

## 4. 行为说明

### 4.1 语法扩展

本特性在保持向后兼容的前提下，扩展了 TRUE_FOR 表达式的语法。TRUE_FOR 可用于流计算查询（CREATE STREAM）和普通查询（SELECT）中的 STATE_WINDOW 和 EVENT_WINDOW。

#### 4.1.1 原有语法（保持不变）

**流计算查询示例**：
```sql
-- 仅基于时间长度触发
CREATE STREAM stream_name AS
SELECT _wstart, COUNT(*) FROM source_table
STATE_WINDOW(column_name)
TRUE_FOR(duration);
```

**普通查询示例**：
```sql
-- 对历史数据进行窗口查询
SELECT _wstart, COUNT(*) FROM source_table
STATE_WINDOW(column_name)
TRUE_FOR(duration);
```

**流计算具体示例**：
```sql
CREATE STREAM s1 AS
SELECT _wstart, COUNT(*) FROM sensors
STATE_WINDOW(status)
TRUE_FOR(10s);
```

**普通查询具体示例**：
```sql
SELECT _wstart, AVG(temperature) FROM sensors
WHERE ts >= '2024-01-01' AND ts < '2024-01-02'
STATE_WINDOW(status)
TRUE_FOR(10s);
```

#### 4.1.2 新增语法 - COUNT 单独指定

**流计算查询示例**：
```sql
-- 仅基于行数触发
CREATE STREAM stream_name AS
SELECT _wstart, COUNT(*) FROM source_table
STATE_WINDOW(column_name)
TRUE_FOR(COUNT n);
```

**普通查询示例**：
```sql
-- 对历史数据按行数触发窗口
SELECT _wstart, COUNT(*) FROM source_table
STATE_WINDOW(column_name)
TRUE_FOR(COUNT n);
```

**流计算具体示例**：
```sql
-- 每累积 100 行数据触发一次窗口
CREATE STREAM s2 AS
SELECT _wstart, AVG(temperature) FROM sensors
STATE_WINDOW(status)
TRUE_FOR(COUNT 100);
```

**普通查询具体示例**：
```sql
-- 查询历史数据，每 100 行输出一个窗口结果
SELECT _wstart, AVG(temperature) FROM sensors
WHERE ts >= '2024-01-01' AND ts < '2024-01-02'
STATE_WINDOW(status)
TRUE_FOR(COUNT 100);
```

#### 4.1.3 新增语法 - AND 逻辑

**流计算查询示例**：
```sql
-- 时间长度和行数都满足才触发
CREATE STREAM stream_name AS
SELECT _wstart, COUNT(*) FROM source_table
STATE_WINDOW(column_name)
TRUE_FOR(duration AND COUNT n);
```

**普通查询示例**：
```sql
-- 对历史数据应用 AND 逻辑
SELECT _wstart, COUNT(*) FROM source_table
STATE_WINDOW(column_name)
TRUE_FOR(duration AND COUNT n);
```

**流计算具体示例**：
```sql
-- 必须同时满足：10 秒已过且收到至少 5 行数据
CREATE STREAM s3 AS
SELECT _wstart, MAX(voltage) FROM meters
STATE_WINDOW(status)
TRUE_FOR(10s AND COUNT 5);
```

**普通查询具体示例**：
```sql
-- 查询历史数据，应用 AND 逻辑
SELECT _wstart, MAX(voltage) FROM meters
WHERE ts >= '2024-01-01' AND ts < '2024-01-02'
STATE_WINDOW(status)
TRUE_FOR(10s AND COUNT 5);
```

**AND 逻辑行为说明**：
- 窗口必须等待完整的时间长度，即使行数条件提前满足
- 如果时间长度已到但行数未满足，窗口继续保持打开状态，等待更多数据
- 只有当两个条件都满足时，窗口才会触发并输出结果
- 在流计算中，窗口会持续等待；在普通查询中，如果数据已全部扫描完但条件未满足，则该窗口不输出结果

#### 4.1.4 新增语法 - OR 逻辑

**流计算查询示例**：
```sql
-- 时间长度或行数任一满足即触发
CREATE STREAM stream_name AS
SELECT _wstart, COUNT(*) FROM source_table
EVENT_WINDOW(START WITH condition1 END WITH condition2)
TRUE_FOR(duration OR COUNT n);
```

**普通查询示例**：
```sql
-- 对历史数据应用 OR 逻辑
SELECT _wstart, COUNT(*) FROM source_table
EVENT_WINDOW(START WITH condition1 END WITH condition2)
TRUE_FOR(duration OR COUNT n);
```

**流计算具体示例**：
```sql
-- 满足以下任一条件即触发：10 秒已过 或 收到 5 行数据
CREATE STREAM s4 AS
SELECT _wstart, SUM(power) FROM meters
EVENT_WINDOW(START WITH voltage > 220 END WITH voltage < 200)
TRUE_FOR(10s OR COUNT 5);
```

**普通查询具体示例**：
```sql
-- 查询历史数据，应用 OR 逻辑
SELECT _wstart, SUM(power) FROM meters
WHERE ts >= '2024-01-01' AND ts < '2024-01-02'
EVENT_WINDOW(START WITH voltage > 220 END WITH voltage < 200)
TRUE_FOR(10s OR COUNT 5);
```

**OR 逻辑行为说明**：
- 哪个条件先满足，窗口就立即触发
- 触发后窗口关闭，开始新的窗口周期

### 4.2 参数说明

#### 4.2.1 COUNT 参数

- **类型**：非负整数
- **有效范围**：0 ≤ COUNT ≤ 2,147,483,647（INT32_MAX）
- **默认值**：无默认值，必须显式指定
- **特殊值**：
  - COUNT 0：条件始终满足，等同于不指定 TRUE_FOR 约束
  - COUNT 1：对于 STATE_WINDOW，等待状态稳定后触发，而不是收到第一行就立即触发

#### 4.2.2 duration 参数

- **类型**：时间长度
- **格式**：整数 + 时间单位（s=秒, m=分钟, h=小时, d=天, w=周）
- **示例**：10s, 5m, 1h, 2d
- **有效范围**：必须大于 0

### 4.3 窗口类型限制

TRUE_FOR 表达式仅支持以下窗口类型：
- **STATE_WINDOW**：状态窗口
- **EVENT_WINDOW**：事件窗口
**不支持**的窗口类型：
- INTERVAL（滑动窗口）
- SESSION（会话窗口）
如果在不支持的窗口类型上使用 TRUE_FOR，系统将返回语法错误。

### 4.4 窗口触发行为

#### 4.4.1 COUNT-only 模式

窗口在累积到指定行数时触发：
```plaintext
初始状态 (count=0) → 累积数据 (count++) → 触发 (count >= threshold)
```

示例：`TRUE_FOR(COUNT 100)`
- 窗口累积第 1-99 行：继续等待
- 窗口累积第 100 行：立即触发并输出结果
- 触发后：重置计数器，开始新窗口

#### 4.4.2 AND 模式

窗口必须同时满足时间和行数两个条件：
```plaintext
初始状态 → 累积数据 → 时间到达但行数未满足 → 继续等待 → 两个条件都满足 → 触发
```

示例：`TRUE_FOR(10s AND COUNT 5)`
**场景 1：行数先满足**
- 8 秒时收到第 5 行数据
- 系统不触发，继续等待
- 10 秒时间到达，触发窗口
**场景 2：时间先到达**
- 10 秒时间到达，但只收到 3 行数据
- 系统不触发，窗口保持打开
- 继续等待，直到收到第 5 行数据时触发
**场景 3：同时满足**
- 10 秒时间到达，恰好收到第 5 行数据
- 立即触发窗口

#### 4.4.3 OR 模式

窗口在时间或行数任一条件满足时触发：
```plaintext
初始状态 → 累积数据 → 任一条件满足 → 立即触发
```

示例：`TRUE_FOR(10s OR COUNT 5)`
**场景 1：行数先满足**
- 3 秒时收到第 5 行数据
- 立即触发窗口（不等待 10 秒）
**场景 2：时间先到达**
- 10 秒时间到达，但只收到 3 行数据
- 立即触发窗口（不等待更多数据）
**场景 3：同时满足**
- 10 秒时间到达，恰好收到第 5 行数据
- 立即触发窗口

### 4.5 出错处理

**错误 1：COUNT 值为负数**
```sql
CREATE STREAM s AS SELECT * FROM t STATE_WINDOW(col) TRUE_FOR(COUNT -1);
```

错误信息：`COUNT must be a non-negative integer not exceeding 2147483647`
**错误 2：COUNT 值超过上限**
```sql
CREATE STREAM s AS SELECT * FROM t STATE_WINDOW(col) TRUE_FOR(COUNT 3000000000);
```

错误信息：`COUNT value exceeds maximum limit (2147483647)`
**错误 3：在不支持的窗口类型上使用 TRUE_FOR**
```sql
CREATE STREAM s AS SELECT * FROM t INTERVAL(1s) TRUE_FOR(COUNT 10);
```

错误信息：`TRUE_FOR is only supported for STATE_WINDOW and EVENT_WINDOW`
**错误 4：语法格式错误**
```sql
CREATE STREAM s AS SELECT * FROM t STATE_WINDOW(col) TRUE_FOR(10s AND 5);
```

错误信息：`Syntax error: expected COUNT keyword before integer value`

## 5. 性能

无，不影响流计算和查询性能，因为窗口内的数据行数本来就会统计。

## 6. 安全

不涉及。

## 7. 兼容性

**完全兼容**：所有现有的 TRUE_FOR 语法保持不变，无需修改现有流和查询语句。

## 8. 运维

不涉及。

## 9. 使用场景

### 9.1 场景 1：告警去重和聚合

**业务需求**：
监控系统需要在 5 分钟内收到至少 10 次相同告警时才触发通知，避免误报。
**流计算解决方案**（实时告警）：
```sql
CREATE STREAM alert_aggregation AS
SELECT _wstart, COUNT(*) as alert_count, FIRST(message)
FROM alerts
STATE_WINDOW(alert_type)
TRUE_FOR(5m AND COUNT 10);
```

**普通查询解决方案**（历史告警分析）：
```sql
SELECT _wstart, COUNT(*) as alert_count, FIRST(message)
FROM alerts
WHERE ts >= '2024-01-01' AND ts < '2024-01-02'
STATE_WINDOW(alert_type)
TRUE_FOR(5m AND COUNT 10);
```

**优势**：
- 减少误报：必须同时满足时间和次数要求
- 提高告警质量
- 可用于实时告警和历史分析

### 9.2 场景 2：事件序列分析

**业务需求**：
分析用户行为序列，当检测到特定事件模式且累积至少 5 个事件时触发分析。
**流计算解决方案**（实时行为分析）：
```sql
CREATE STREAM user_behavior AS
SELECT _wstart, COLLECT_LIST(event_type) as event_sequence
FROM user_events
EVENT_WINDOW(START WITH event_type='session_start' END WITH event_type='session_end')
TRUE_FOR(COUNT 5);
```

**普通查询解决方案**（历史行为分析）：
```sql
SELECT _wstart, COLLECT_LIST(event_type) as event_sequence
FROM user_events
WHERE ts >= '2024-01-01' AND ts < '2024-01-02'
EVENT_WINDOW(START WITH event_type='session_start' END WITH event_type='session_end')
TRUE_FOR(COUNT 5);
```

**优势**：
- 过滤短会话：只分析包含足够事件的会话
- 提高分析质量
- 支持实时监控和历史回溯

## 10. 约束和限制

### 10.1 使用约束

1. **窗口类型限制**：
  - TRUE_FOR 仅支持 STATE_WINDOW 和 EVENT_WINDOW
  - 不支持 INTERVAL（滑动窗口）和 SESSION（会话窗口）
1. **COUNT 参数约束**：
  - 必须为非负整数：0 ≤ COUNT ≤ 2,147,483,647
  - COUNT 0 表示条件始终满足（等同于无 TRUE_FOR 约束）
1. **逻辑组合约束**：
  - 仅支持 AND 和 OR 两种逻辑运算符
  - 不支持 NOT、XOR 等其他逻辑运算符
  - 不支持多个 COUNT 条件组合

### 10.2 功能限制

1. **不支持动态修改**：
  - 已创建的流任务的 TRUE_FOR 条件不能动态修改
  - 需要删除并重新创建流任务
1. **不支持复杂表达式**：
  - COUNT 必须是常量，不支持表达式（如 `COUNT > 10`）
  - 不支持范围条件（如 `COUNT BETWEEN 5 AND 10`）
1. **不支持多条件组合**：
  - 不支持 `TRUE_FOR(10s AND COUNT 5 OR COUNT 10)` 这样的复杂组合
  - 仅支持单层 AND 或 OR 逻辑

## 11. 常见错误和排查

无。

## 12. 可观测性

不涉及。

## 13. 安装和卸载

不涉及。

## 14. 文档

1. 不需要修改企业版文档
2. 需要修改官网文档

## 15. 参考文档

无。

## 16. 附录

无。
