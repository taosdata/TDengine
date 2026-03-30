# TRUE_FOR 表达式增强功能 RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-16 | 2026-02-05 | 1.0 | 关胜亮 | 新建 |

## 2. 引言

### 2.1 术语与缩写名词

| TRUE_FOR 表达式 | TDengine 中用于指定窗口触发条件的语法结构，可用于流计算查询和普通查询，本次需求对其功能进行增强 |
| --- | --- |
| STATE_WINDOW | 状态窗口，根据某列值的变化来划分窗口，是TRUE_FOR表达式支持的窗口类型之一 |
| EVENT_WINDOW | 事件窗口，根据起始和结束条件来划分窗口，是TRUE_FOR表达式支持的窗口类型之一 |
| COUNT | 行数限制，本次新增的触发条件参数，指定窗口需要累积的数据行数 |
| AND 逻辑 | TRUE_FOR表达式新增的逻辑组合方式，要求时间长度和行数两个条件都满足才触发窗口 |
| OR 逻辑 | TRUE_FOR表达式新增的逻辑组合方式，时间长度或行数任一条件满足即触发窗口 |
| 窗口触发 | 当满足TRUE_FOR指定的条件时，输出窗口的聚合结果的操作 |
| 流计算查询 | 使用CREATE STREAM创建的持续查询，可自动处理新到达的数据，支持TRUE_FOR表达式 |
| 普通查询 | 使用SELECT语句对历史数据进行的一次性查询，支持TRUE_FOR表达式 |

### 2.2 相关文档资料

### 2.3 优先级要求

中（为现有TRUE_FOR表达式增强功能，解决业务场景局限性，不影响核心功能运行，需有序推进）

### 2.4 版本要求

企业版和社区版都支持

## 3. 需求目标

针对TDengine现有TRUE_FOR表达式仅支持基于时间长度的窗口触发条件，在数据到达率不稳定、需基于数据量处理、需复合触发条件等场景下存在局限性的问题，增强TRUE_FOR表达式的功能。实现支持单独指定行数限制（COUNT）、同时指定时间长度和行数限制（AND/OR逻辑），兼顾流计算查询和普通查询场景，保持向后兼容，让用户可更灵活地控制窗口触发时机，满足不同业务场景需求，为TRUE_FOR表达式增强功能的开发落地提供明确指导。

## 4. 功能需求

### 4.1 语法扩展

TRUE_FOR表达式可用于流计算查询（CREATE STREAM）和普通查询（SELECT）中的STATE_WINDOW和EVENT_WINDOW窗口类型，原有语法保持不变，新增3类语法，具体要求如下：

#### 4.1.1 原有语法（保持不变）

仅基于时间长度触发，适用于流计算和普通查询：
流计算查询示例：
```plaintext {wrap}
CREATE STREAM stream_name AS SELECT _wstart, COUNT(*) FROM source_table STATE_WINDOW(column_name) TRUE_FOR(duration);
```

普通查询示例：
```plaintext {wrap}
SELECT _wstart, COUNT(*) FROM source_table STATE_WINDOW(column_name) TRUE_FOR(duration);
```

#### 4.1.2 新增语法 - COUNT单独指定

仅基于行数触发，适用于流计算和普通查询，需显式指定COUNT及对应行数：
流计算查询示例：
```plaintext {wrap}
CREATE STREAM stream_name AS SELECT _wstart, COUNT(*) FROM source_table STATE_WINDOW(column_name) TRUE_FOR(COUNT n);
```

普通查询示例：
```plaintext {wrap}
SELECT _wstart, COUNT(*) FROM source_table STATE_WINDOW(column_name) TRUE_FOR(COUNT n);
```

#### 4.1.3 新增语法 - AND逻辑

时间长度和行数两个条件都满足才触发窗口，适用于流计算和普通查询：
流计算查询示例：
```plaintext {wrap}
CREATE STREAM stream_name AS SELECT _wstart, COUNT(*) FROM source_table STATE_WINDOW(column_name) TRUE_FOR(duration AND COUNT n);
```

普通查询示例：
```plaintext {wrap}
SELECT _wstart, COUNT(*) FROM source_table STATE_WINDOW(column_name) TRUE_FOR(duration AND COUNT n);
```

AND逻辑行为说明：窗口必须等待完整的时间长度，即使行数条件提前满足；若时间长度已到但行数未满足，窗口继续保持打开状态，等待更多数据；仅当两个条件都满足时，窗口才触发并输出结果；流计算中窗口持续等待，普通查询中若数据已全部扫描完但条件未满足，则该窗口不输出结果。

#### 4.1.4 新增语法 - OR逻辑

时间长度或行数任一条件满足即触发窗口，适用于流计算和普通查询：
流计算查询示例：
```plaintext {wrap}
CREATE STREAM stream_name AS SELECT _wstart, COUNT(*) FROM source_table EVENT_WINDOW(START WITH condition1 END WITH condition2) TRUE_FOR(duration OR COUNT n);
```

普通查询示例：
```plaintext {wrap}
SELECT _wstart, COUNT(*) FROM source_table EVENT_WINDOW(START WITH condition1 END WITH condition2) TRUE_FOR(duration OR COUNT n);
```

OR逻辑行为说明：哪个条件先满足，窗口就立即触发；触发后窗口关闭，开始新的窗口周期。

### 4.2 参数说明

#### 4.2.1 COUNT参数

- 类型：非负整数
- 有效范围：0 ≤ COUNT ≤ 2,147,483,647（INT32_MAX）
- 默认值：无默认值，必须显式指定
- 特殊值：COUNT 0表示条件始终满足，等同于不指定TRUE_FOR约束；COUNT 1表示对于STATE_WINDOW，等待状态稳定后触发，而非收到第一行就立即触发

#### 4.2.2 duration参数

- 类型：时间长度
- 格式：整数 + 时间单位（s=秒, m=分钟, h=小时, d=天, w=周），示例：10s, 5m, 1h, 2d
- 有效范围：必须大于0

### 4.3 窗口类型限制

TRUE_FOR表达式仅支持STATE_WINDOW（状态窗口）和EVENT_WINDOW（事件窗口）两种窗口类型；不支持INTERVAL（滑动窗口）和SESSION（会话窗口），若在不支持的窗口类型上使用TRUE_FOR，系统需返回语法错误。

### 4.4 窗口触发行为

#### 4.4.1 COUNT-only模式（仅COUNT参数）

窗口在累积到指定行数时触发，触发逻辑：初始状态(count=0) → 累积数据(count++) → 触发(count ≥ threshold)；触发后重置计数器，开始新窗口。

#### 4.4.2 AND模式（时间+行数）

窗口必须同时满足时间和行数两个条件，触发逻辑：初始状态 → 累积数据 → 时间到达但行数未满足 → 继续等待 → 两个条件都满足 → 触发，具体场景适配详见4.1.3相关说明。

#### 4.4.3 OR模式（时间+行数）

窗口在时间或行数任一条件满足时触发，触发逻辑：初始状态 → 累积数据 → 任一条件满足 → 立即触发，具体场景适配详见4.1.4相关说明。

### 4.5 出错处理

针对常见错误场景，需返回明确的错误信息，具体如下：
- 错误1：COUNT值为负数，示例：CREATE STREAM s AS SELECT * FROM t STATE_WINDOW(col) TRUE_FOR(COUNT -1)；错误信息：COUNT must be a non-negative integer not exceeding 2147483647
- 错误2：COUNT值超过上限，示例：CREATE STREAM s AS SELECT * FROM t STATE_WINDOW(col) TRUE_FOR(COUNT 3000000000)；错误信息：COUNT value exceeds maximum limit (2147483647)
- 错误3：在不支持的窗口类型上使用TRUE_FOR，示例：CREATE STREAM s AS SELECT * FROM t INTERVAL(1s) TRUE_FOR(COUNT 10)；错误信息：TRUE_FOR is only supported for STATE_WINDOW and EVENT_WINDOW
- 错误4：语法格式错误，示例：CREATE STREAM s AS SELECT * FROM t STATE_WINDOW(col) TRUE_FOR(10s AND 5)；错误信息：Syntax error: expected COUNT keyword before integer value

### 4.6 约束和限制

#### 4.6.1 使用约束

- 窗口类型约束：仅支持STATE_WINDOW和EVENT_WINDOW，不支持INTERVAL和SESSION窗口
- COUNT参数约束：必须为非负整数（0 ≤ COUNT ≤ 2,147,483,647），COUNT 0等同于无TRUE_FOR约束
- 逻辑组合约束：仅支持AND和OR两种逻辑运算符，不支持NOT、XOR等其他逻辑运算符，不支持多个COUNT条件组合

#### 4.6.2 功能限制

- 不支持动态修改：已创建的流任务的TRUE_FOR条件不能动态修改，需删除并重新创建流任务
- 不支持复杂表达式：COUNT必须是常量，不支持表达式（如COUNT > 10）和范围条件（如COUNT BETWEEN 5 AND 10）
- 不支持多条件组合：不支持TRUE_FOR(10s AND COUNT 5 OR COUNT 10)这类复杂组合，仅支持单层AND或OR逻辑

### 4.7 使用场景

增强后的TRUE_FOR表达式需适配以下典型业务场景，确保功能实用性：

#### 4.7.1 场景1：告警去重和聚合

业务需求：监控系统需在5分钟内收到至少10次相同告警时才触发通知，避免误报；支持实时告警和历史告警分析。
流计算解决方案（实时告警）：CREATE STREAM alert_aggregation AS SELECT _wstart, COUNT(*) as alert_count, FIRST(message) FROM alerts STATE_WINDOW(alert_type) TRUE_FOR(5m AND COUNT 10);
普通查询解决方案（历史告警分析）：SELECT _wstart, COUNT(*) as alert_count, FIRST(message) FROM alerts WHERE ts >= '2024-01-01' AND ts < '2024-01-02' STATE_WINDOW(alert_type) TRUE_FOR(5m AND COUNT 10);

#### 4.7.2 场景2：事件序列分析

业务需求：分析用户行为序列，当检测到特定事件模式且累积至少5个事件时触发分析；支持实时行为分析和历史行为回溯。
流计算解决方案（实时行为分析）：CREATE STREAM user_behavior AS SELECT _wstart, COLLECT_LIST(event_type) as event_sequence FROM user_events EVENT_WINDOW(START WITH event_type='session_start' END WITH event_type='session_end') TRUE_FOR(COUNT 5);
普通查询解决方案（历史行为分析）：SELECT _wstart, COLLECT_LIST(event_type) as event_sequence FROM user_events WHERE ts >= '2024-01-01' AND ts < '2024-01-02' EVENT_WINDOW(START WITH event_type='session_start' END WITH event_type='session_end') TRUE_FOR(COUNT 5);

## 5. 性能需求

不影响流计算和查询性能，窗口内的数据行数本就会进行统计，本次增强功能无需额外增加性能损耗。

## 6. 安全需求

不涉及

## 7. 兼容性需求

完全兼容现有版本，所有现有的TRUE_FOR语法保持不变，无需修改现有流任务和查询语句，确保现有业务场景不受影响。

## 8. 其他需求

- 文档需求：不需要修改企业版文档，需要修改官网文档，补充TRUE_FOR表达式新增语法、参数说明、使用场景等内容；
- 常见错误和排查：无特殊内容，按4.5出错处理相关说明执行即可；
- 运维、可观测性、安装和卸载：均不涉及特殊需求，遵循TDengine常规规范。
