---
sidebar_label: 建流语法
title: 建流语法
description: 流式计算 CREATE/SHOW/启停语法与参数说明
toc_max_heading_level: 4
---

## 创建流式计算

```sql
CREATE STREAM [IF NOT EXISTS] [db_name.]stream_name options [INTO [db_name.]table_name] [NODELAY_CREATE_SUBTABLE] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [COMPOSITE KEY][, ...])] [TAGS (tag_definition [, ...])] [AS subquery]

options: {
    trigger_type [FROM [db_name.]table_name] [{PARTITION BY col1 [, ...] | ROLLUP BY tag_name}] [STREAM_OPTIONS(stream_option [|...])] [notification_definition]
}
    
trigger_type: {
    PERIOD(period_time[, offset_time])
  | SLIDING(sliding_val[, offset_time]) 
  | INTERVAL(interval_val[, interval_offset]) SLIDING(sliding_val[, offset_time]) 
  | SESSION(ts_col, session_val)
  | STATE_WINDOW(state_expr [, state_expr ...]) [EXTEND(extend_val)] [ZEROTH_STATE(zeroth_val [, zeroth_val ...])] [TRUE_FOR(true_for_expr)]
  | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(true_for_expr)]
  | EVENT_WINDOW(START WITH (start_condition_1, start_condition_2 [,...]) [END WITH end_condition]) [TRUE_FOR(true_for_expr)]
  | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]])
  | WINDOW(named_nonleaf_window [, named_nonleaf_window ...], leaf_window)
}

named_nonleaf_window: nested_window_type AS layer_name

leaf_window: nested_window_type [AS layer_name]

nested_window_type: {
    SLIDING(sliding_val[, offset_time])
  | INTERVAL(interval_val[, interval_offset]) SLIDING(sliding_val[, offset_time])
  | SESSION(ts_col, session_val)
  | STATE_WINDOW(state_expr [, state_expr ...]) [EXTEND(extend_val)] [ZEROTH_STATE(zeroth_val [, zeroth_val ...])] [TRUE_FOR(true_for_expr)]
  | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(true_for_expr)]
  | EVENT_WINDOW(START WITH (start_condition_1, start_condition_2 [,...]) [END WITH end_condition]) [TRUE_FOR(true_for_expr)]
  | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]])
}

true_for_expr:
    true_for_arg [, true_for_arg [, true_for_arg]]

true_for_arg: {
    limit_expr
  | start(limit_expr)
  | end(limit_expr)
}

limit_expr: {
    duration_time
  | COUNT count_val
  | duration_time AND COUNT count_val
  | duration_time OR COUNT count_val
}

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types) | IGNORE_NODATA_TRIGGER | IDLE_TIMEOUT(duration_time) | FLUSH_ON_OUTER_CLOSE}

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

notify_option: NOTIFY_HISTORY

event_types:
    event_type [|event_type]

event_type: {WINDOW_OPEN | WINDOW_CLOSE | IDLE | RESUME}

tag_definition:
    tag_name type_name AS expr
```

流任务时区、自然时间单位（如 `PERIOD`/`SLIDING`/`INTERVAL` 中的周/月/季/年）在不同版本的支持范围，详见 [时区与自然时间单位](../05-tdengine-sql/10-time/01-timezone.md#流式计算时区)。

### 流式计算的触发方式

事件触发是流式计算的驱动方式，事件触发产生的来源可能多种多样，可以来自于某个表的数据写入，也可以来自于对某个表的计算分析结果，甚至可以不来自于任何表。当流式计算引擎检测到符合用户定义的触发条件时，就会触发计算，条件符合次数和计算触发次数是相同的，触发对象与计算对象彼此分离。用户可以灵活的定义和使用各种窗口来产生触发事件，支持在开窗、关窗以及开关窗同时进行触发，支持分组触发，支持对触发数据进行预先过滤处理。

#### 触发类型

触发类型通过 `trigger_type` 指定，包括定时触发、滑动触发、时间窗口触发、会话窗口触发、状态窗口触发、事件窗口触发、计数窗口触发。其中，状态窗口、事件窗口和计数窗口搭配超级表时，必须与 `PARTITION BY tbname` 一起使用。

##### 定时触发

```sql
PERIOD(period_time[, offset_time])
```

定时触发通过系统时间的固定间隔来驱动，本质上就是我们常说的定时任务。定时触发不属于窗口触发。各参数含义如下：

- period_time：定时间隔，支持的时间单位详见[时间单位](../05-tdengine-sql/01-datatype.md#时间单位)（支持毫秒至年），支持的时间范围为 `[10a, 3650d]`。
- offset_time：可选，定时偏移，支持的时间单位包括：毫秒 (a)、秒 (s)、分 (m)、小时 (h)、天 (d)。对于周/月/年单位，offset 必须严格小于触发周期；对于月单位，以 28 天/月为基准静态校验（如 `PERIOD(1n, 28d)` 非法）。

使用说明：

- 定时间隔小于 1 天时，基准时间点为每日零点加定时偏移，根据定时间隔来确定下次触发的时间点。基准时间点在每日零点重置。每日最后一次触发的时间点与下一日的基准时间点之间的间隔可能小于定时间隔。例如：
  - 定时间隔为 5 小时 30 分钟，那么当天的触发时刻为 `[00:00, 05:30, 11:00, 16:30, 22:00]`，后续每一天的触发时刻都是相同的。
  - 同样的定时间隔，如果指定时间偏移为 1 分钟，那么当天的触发时刻为 `[00:01, 05:31, 11:01, 16:31, 22:01]`，后续每一天的触发时刻都是相同的。
  - 同样条件下，如果建流时当前系统时间为 `12:00`，那么当天的触发时刻为 `[16:31, 22:01]`，后续每一天内的触发时刻为 `[00:01, 05:31, 11:01, 16:31, 22:01]`。
- 定时间隔大于等于 1 天时，基准时间点为服务端时区的 Unix epoch（1970-01-01 00:00:00）加定时偏移，按触发间隔整除对齐，保证所有任务触发时刻全局一致。例如：
  - 定时间隔为 2 天，所有使用该间隔的任务都会在距离 epoch 整数倍 2 天的时刻触发（如 1970-01-03 00:00:00, 1970-01-05 00:00:00, ...），确保全局对齐。
  - 定时间隔为 1 周（`PERIOD(1w)`），触发时刻对齐每周一 00:00:00；`PERIOD(1w, 1d)` 则在每周二 00:00:00 触发。
  - 定时间隔为 1 月（`PERIOD(1n)`），触发时刻对齐每月 1 日 00:00:00；`PERIOD(1n, 14d)` 则在每月 15 日 00:00:00 触发。
  - 定时间隔为 1 年（`PERIOD(1y)`），触发时刻对齐每年 1 月 1 日 00:00:00；`PERIOD(1y, 31d)` 则在每年 2 月 1 日 00:00:00 触发。

适用场景：需要按照系统时间连续定时驱动计算的场景，例如每小时计算生成一次当天的统计数据，每天定时发送统计报告等。

##### 滑动触发

```sql
SLIDING(sliding_val[, offset_time]) 
```

滑动触发是指对触发表的写入数据按照事件时间的固定间隔来驱动的触发。滑动触发不属于窗口触发，必须指定触发表。滑动触发的触发时刻、时间偏移规则和定时触发相同，唯一的区别是系统时间变更为事件时间。

各参数含义如下：

- sliding_val：必选，事件时间的滑动时长。
- offset_time：可选，指定滑动触发的时间偏移，支持的时间单位包括：毫秒 (a)、秒 (s)、分 (m)、小时 (h)。

使用说明：

- 必须指定触发表，触发表为超级表时支持按标签、子表分组，支持不分组。
- 支持对写入数据进行处理过滤后（有条件）的滑动触发。

适用场景：需要按照事件时间连续定时驱动计算的场景，例如每小时计算生成一次当天的统计数据，每天定时发送统计报告等场景。

##### 时间窗口触发

```sql
INTERVAL(interval_val[, interval_offset]) SLIDING(sliding_val[, offset_time])
```

时间窗口触发是指对触发表的写入数据按照事件时间和固定窗口大小滑动而形成的触发，必须指定 `INTERVAL` 窗口，属于窗口触发，必须指定触发表。与仅使用 `SLIDING` 的[滑动触发](#滑动触发)不同，此处 `INTERVAL` 为构成该触发类型的必需部分。

时间窗口触发的起始时间点是窗口的起始点，窗口默认是从 Unix time 0（1970-01-01 00:00:00 UTC）开始划分，可以通过指定窗口时间偏移的方式来改变窗口的划分起始点。各参数含义如下：

- interval_val：必选，时间窗口的时长。
- interval_offset：可选，时间窗口的划分偏移。
- sliding_val：必选，事件时间的滑动时长。
- offset_time：可选，滑动触发的时间偏移（含义同滑动触发）。

使用说明：

- 必须指定触发表，触发表为超级表时支持按标签、子表分组，支持不分组。
- 支持对写入数据进行处理过滤后（有条件）的时间窗口触发。

适用场景：需要按照事件时间定时窗口计算的场景，例如每小时计算生成该小时内的统计数据，每隔 1 小时计算最后 5 分钟窗口内的数据等场景。

##### 会话窗口触发

```sql
SESSION(ts_col, session_val)
```

会话窗口触发是指对触发表的写入数据按照会话窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。各参数含义如下：

- ts_col：主键列名。
- session_val：属于同一个会话的最大时间间隔，间隔小于等于 `session_val` 的记录都属于同一个会话。

使用说明：

- 必须指定触发表，触发表为超级表时支持按标签、子表分组，支持不分组。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。

适用场景：需要通过会话窗口驱动计算和（或）通知的场景。

##### 状态窗口触发

```sql
STATE_WINDOW(state_expr [, state_expr ...]) [EXTEND(extend_val)] [ZEROTH_STATE(zeroth_val [, zeroth_val ...])] [TRUE_FOR(true_for_expr)]
```

状态窗口触发是指对触发表的写入数据按照状态表达式的计算结果进行窗口划分，当窗口启动和（或）关闭时进行的触发。各参数含义如下：

- state_expr：一个或多个状态键。可以是列引用或标签，也可以是 `CASE WHEN`、`IF`、`CAST` 等表达式；返回类型必须是整数、布尔值或 `VARCHAR`。
- extend_val：可选，窗口开始结束时的扩展策略：`EXTEND(0)` 时，窗口开始、结束时间为该状态的第一条、最后一条数据对应的时间戳；`EXTEND(1)` 时，窗口开始时间不变，窗口结束时间向后扩展至下一个窗口开始之前；`EXTEND(2)` 时，窗口开始时间向前扩展至上一个窗口结束之后，窗口结束时间不变。
- zeroth_val：可选，指定“零状态”。参数个数必须与状态键个数一致；非 `NO_ZEROTH` 的参数必须是常量，并且可以转换为对应状态键的数据类型；`NO_ZEROTH` 表示对应位置不参与零状态判断。只有所有已配置零状态的位置都命中时，该窗口才会被计算为零状态窗口并被过滤。
- true_for_expr：可选，指定窗口的过滤条件，只有满足条件的窗口才会产生触发。支持以下四种模式：
  - `TRUE_FOR(duration_time)`：仅基于持续时长过滤，窗口持续时长必须大于等于 `duration_time`。
  - `TRUE_FOR(COUNT n)`：仅基于数据行数过滤，窗口数据行数必须大于等于 `n`。
  - `TRUE_FOR(duration_time AND COUNT n)`：同时满足持续时长和数据行数条件。
  - `TRUE_FOR(duration_time OR COUNT n)`：满足持续时长或数据行数条件之一即可。

  其中 `duration_time` 为时间范围正值，时间单位参见[时间单位](../05-tdengine-sql/01-datatype.md#时间单位)（仅支持毫秒至周），如 `TRUE_FOR(10m)`、`TRUE_FOR(COUNT 100)`、`TRUE_FOR(10m AND COUNT 100)`、`TRUE_FOR(10m OR COUNT 100)`。

使用说明：

- 必须指定触发表，触发表为超级表时支持按标签、子表分组，支持不分组。
- 状态窗口支持单列或多列状态键；当任一状态键变化时，会关闭当前窗口并自当前记录开启新窗口。
- 搭配超级表时，必须与 `PARTITION BY tbname` 一起使用。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。
- 当所有状态键列都是 `NULL` 时，该行按现有状态窗口的 `NULL` 规则处理；当只有部分状态键列为 `NULL` 时，连续的部分 `NULL` 行会作为一个整体，决定是并入前一个窗口、并入后一个窗口，还是独立成窗。
- 下面的表格展示了状态窗口触发里最常见的合并结果。表中“并入前窗 / 并入后窗 / 独立成窗”都指中间那段连续的部分 `NULL` 行：

| 输入序列（状态键） | `EXTEND(0)` | `EXTEND(1)` | `EXTEND(2)` |
| --- | --- | --- | --- |
| `(1, 10) -> (1, NULL) -> (1, 20)` | 并入前窗 | 并入前窗 | 并入后窗 |
| `(1, 'a') -> (1, NULL) -> (2, 'a')` | 并入前窗 | 并入前窗 | 独立成窗 |
| `(1, 'a') -> (NULL, 'b') -> (1, 'b')` | 并入后窗 | 独立成窗 | 并入后窗 |
| `(1, 'a') -> (NULL, 'b') -> (2, 'a')` | 独立成窗 | 独立成窗 | 独立成窗 |

- 如果一段连续的部分 `NULL` 行中夹杂全 `NULL` 行，夹在中间的全 `NULL` 行随这一段一起处理。例如 `(1, 'a') -> (1, NULL) -> (NULL, NULL) -> (1, NULL) -> (2, 'a')` 中间三行会一起处理：`EXTEND(0)` 和 `EXTEND(1)` 并入前窗，`EXTEND(2)` 独立成窗。
- `ZEROTH_STATE(...)` 按位置逐列判断；只有所有参与判断的位置都等于各自的零状态值时，该窗口才会被过滤。如果某个位置写成 `NO_ZEROTH`，该位置不参与零状态判断。
- 状态表达式可以引用触发表上下文中可见的 tag 列。例如：

```sql
CREATE STREAM s_tag_state
  STATE_WINDOW(voltage >= 220 + groupId)
  FROM meters
  PARTITION BY tbname
  INTO meters_state_out
  AS SELECT _twstart AS ts, _twend AS te, COUNT(*) AS cnt FROM %%trows;
```

多列状态窗口示例：

```sql
CREATE STREAM s_multi_state
  STATE_WINDOW(s1, s2) EXTEND(0) ZEROTH_STATE(1, NO_ZEROTH)
  FROM ntb
  PARTITION BY tbname
  INTO result_table
  AS
    SELECT _twstart AS ts, _twend AS te, COUNT(*) AS cnt FROM %%trows;
```

上面的流会在 `s1` 或 `s2` 任一变化时切窗，只对 `s1 = 1` 做零状态过滤，`s2` 不参与零状态判断。

适用场景：需要通过状态窗口驱动计算和（或）通知的场景。

##### 事件窗口触发

```sql
EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(true_for_expr)]
```

事件窗口触发是指对触发表的写入数据按照事件窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。各参数含义如下：

- start_condition：事件开始条件的定义，可以是任意合法条件表达式。
- end_condition：事件结束条件的定义，可以是任意合法条件表达式。
- true_for_expr：可选，指定窗口的过滤条件和开/关窗连续满足门限。三种参数均可选，顺序任意，最多各出现一次：
  - **窗口整体过滤（`limit_expr`）**：只有满足条件的窗口才会产生触发：
    - `TRUE_FOR(duration_time)`：仅基于持续时长过滤，窗口持续时长必须大于等于 `duration_time`。
    - `TRUE_FOR(COUNT n)`：仅基于数据行数过滤，窗口数据行数必须大于等于 `n`。
    - `TRUE_FOR(duration_time AND COUNT n)`：同时满足持续时长和数据行数条件。
    - `TRUE_FOR(duration_time OR COUNT n)`：满足持续时长或数据行数条件之一即可。
  - **开窗连续满足条件（`start(limit_expr)`）**：`START WITH` 条件连续满足 `limit_expr` 指定的行数或时长后，窗口才真正打开。`_wstart` 取 streak 第一行时间戳。streak 中断则重新计数。
  - **关窗连续满足条件（`end(limit_expr)`）**：`END WITH` 条件连续满足 `limit_expr` 指定的行数或时长后，窗口才真正关闭。`_wend` 取关窗 streak 第一行时间戳，streak 后续行不计入窗口。streak 中断则重新计数，窗口保持开启。

  其中 `duration_time` 为时间范围正值，时间单位参见[时间单位](../05-tdengine-sql/01-datatype.md#时间单位)（仅支持毫秒至周），如 `TRUE_FOR(10m)`、`TRUE_FOR(COUNT 100)`、`TRUE_FOR(10m AND COUNT 100)`、`TRUE_FOR(10m OR COUNT 100)`、`TRUE_FOR(start(COUNT 2))`、`TRUE_FOR(end(3s))`、`TRUE_FOR(5s, start(COUNT 2), end(COUNT 3))`。`start(...)` 和 `end(...)` 仅支持单开窗条件的 `EVENT_WINDOW`。

使用说明：

- 必须指定触发表，触发表为超级表时支持按标签、子表分组，支持不分组。
- 搭配超级表时，必须与 `PARTITION BY tbname` 一起使用。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。
- 开始/结束条件表达式可以引用触发表上下文中可见的 tag 列。例如：

```sql
CREATE STREAM s_tag_event
  EVENT_WINDOW(START WITH voltage >= 220 + groupId END WITH voltage < 220 + groupId)
  FROM meters
  PARTITION BY tbname
  INTO meters_event_out
  AS SELECT _twstart AS ts, _twend AS te, COUNT(*) AS cnt FROM %%trows;
```

适用场景：需要通过事件窗口驱动计算和（或）通知的场景。

##### 事件窗口触发 (支持子事件窗口)

```sql
EVENT_WINDOW(START WITH (start_condition_1, start_condition_2 [,...]) [END WITH end_condition]) [TRUE_FOR(true_for_expr)]
```

事件窗口触发是指对触发表的写入数据按照事件窗口的方式进行窗口划分，它现在支持指定多个开始条件，并能根据有效触发条件的变化，在原有的事件窗口内进一步划分和管理子事件窗口，同时引入父事件窗口的概念来聚合相关的子事件窗口。各参数含义如下：

- start_condition_1, start_condition_2 [,...]：定义多个事件开始条件。当任何一个条件满足时，事件窗口开启。系统会从前往后依次评估这些条件，第一个满足的条件即为“有效触发条件”。当所有 start_condition 都不满足时，父窗口和最后一个子窗口关闭。
- end_condition：事件结束条件的定义。当该条件满足时，当前父窗口和最后一个子窗口均关闭。该参数现在是可选的。
- true_for_expr：可选，指定窗口的过滤条件，只有满足条件的窗口才会产生触发。支持以下四种模式：
  - `TRUE_FOR(duration_time)`：仅基于持续时长过滤，窗口持续时长必须大于等于 `duration_time`。
  - `TRUE_FOR(COUNT n)`：仅基于数据行数过滤，窗口数据行数必须大于等于 `n`。
  - `TRUE_FOR(duration_time AND COUNT n)`：同时满足持续时长和数据行数条件。
  - `TRUE_FOR(duration_time OR COUNT n)`：满足持续时长或数据行数条件之一即可。

  其中 `duration_time` 为时间范围正值，时间单位参见[时间单位](../05-tdengine-sql/01-datatype.md#时间单位)（仅支持毫秒至周），如 `TRUE_FOR(10m)`、`TRUE_FOR(COUNT 100)`、`TRUE_FOR(10m AND COUNT 100)`、`TRUE_FOR(10m OR COUNT 100)`。

使用说明：

- 必须指定触发表，触发表为超级表时支持按标签、子表分组，支持不分组。
- 搭配超级表时，必须与 `PARTITION BY tbname` 一起使用。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。
- 多个 `start_condition` 以及可选的 `end_condition` 同样可以引用触发表上下文中可见的 tag 列。
- 父子窗口行为：
  - 没有父/子窗口：在事件窗口开启期间，如果有效触发条件没有变化，则只产生一个窗口，系统将其视为常规事件窗口，不产生父/子窗口的概念。
  - 子窗口：当某一个具体的 start_condition 成为有效触发条件时，会开启一个子窗口。如果有效触发条件发生变化，或者 end_condition 满足时，当前子窗口关闭。子窗口之间不重叠。
  - 父窗口：仅当第二个子窗口开启时，才会开启父窗口。父窗口的起始时间为第一个子窗口的起始时间，结束时间为最后一个子窗口的结束时间，当所有 start_condition 都不满足，或者 end_condition 满足时关闭。
- 通知消息扩展：在窗口开启（WINDOW_OPEN）的通知消息中，新增两个字段：
  - conditionIndex：触发当前窗口开启的开始条件的序号，从 0 开始计数。对于父窗口，其值与第一个子窗口的值相同。
  - windowIndex：子事件窗口在父窗口中的序号，从 0 开始计数。如果不是子窗口（即常规事件窗口或父窗口），该字段值为 -1。
- TRUE_FOR 选项对子窗口和父窗口均生效，即小于该时长限制的窗口（无论是子窗口还是父窗口）将直接被忽略。当父窗口下有部分子窗口不满足 TRUE_FOR 条件时，有效的子窗口可能不是连续的。如果父窗口下仅有 1 个子窗口满足 TRUE_FOR 条件，父/子窗口仍保留并触发通知和计算。

适用场景：需要通过事件窗口驱动计算和（或）通知的场景，尤其适用于需要根据多个动态变化的条件来精细化监控和分析事件的物联网、工业数据管理等领域。例如，设备故障告警，可以定义多个告警级别条件（如“负载高于 90”、“负载高于 60”），并在告警级别变化时，清晰地追踪告警状态的升级或降级。

##### 计数窗口触发

```sql
COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]]) 
```

计数窗口触发是指对触发表的写入数据按照计数窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。支持列的触发，只有当指定的列有数据写入时才触发。各参数含义如下：

- count_val：计数条数，当写入数据条目数达到 `count_val` 时触发，最小值为 1。
- sliding_val：可选，窗口滑动的条数。
- col1 [, ...]：可选，按列触发模式时的触发列列表，只支持普通列，列表中任一列有非空数据写入时才为有效条目，NULL 值视为无效值。

使用说明：

- 必须指定触发表，触发表为超级表时支持按标签、子表分组，支持不分组。
- 搭配超级表时，必须与 `PARTITION BY tbname` 一起使用。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。

适用场景：

- 需要对每条数据进行处理的场景，例如故障数据写入、采样数据写入等场景。
- 需要根据某些列特定值进行处理的场景，例如异常值写入场景。
- 需要批量处理数据的场景，例如每写入 1000 条电压数据求平均值场景。

##### 嵌套窗口触发

```sql
WINDOW (
  nested_window_type AS outer_layer,
  ...,
  nested_window_type [AS leaf_layer]
)
```

嵌套窗口触发将 2 至 8 个窗口按从最外层到最内层的顺序组成窗口链。根层和中间层只建立作用域，只有最内层（叶层）产生计算和通知事件。祖先作用域结束时，会先处理该作用域内的叶层实例，再重置子层并接收下一个作用域的数据。

所有非叶层必须命名，叶层名称可选。所有非空层名按 ASCII 字母大小写不敏感规则唯一，并且不能与计算查询中可见的表名、显式表别名或派生表别名冲突。

在计算查询中，未限定的触发占位符引用叶层。命名层可以通过层名限定占位符：

- `SLIDING` 层可使用 `layer_name._tprev_ts`、`layer_name._tcurrent_ts` 和 `layer_name._tnext_ts`。
- 其他受支持的窗口层可使用 `layer_name._twstart`、`layer_name._twend`、`layer_name._twduration` 和 `layer_name._twrownum`。
- `%%trows` 表示当前叶层事件中已被所有祖先层接纳的叶层触发数据集，并沿用普通 `%%trows` 的使用限制。

业务示例：按充电订单计算相邻采样的 SOC 变化

充电桩会持续上报电池荷电状态（SOC）。业务通常需要比较同一订单内相邻两次采样，计算 SOC 变化。单独使用 `COUNT_WINDOW(2, 1)` 会连续配对数据，订单切换时可能把上一订单的最后一次采样与下一订单的第一次采样配成一组。下面的示例先按 `order_id` 划分订单，再在每个订单内生成相邻采样对：

```sql
CREATE STREAM order_pairs
  WINDOW (
    STATE_WINDOW(order_id) EXTEND(1) AS order_scope,
    COUNT_WINDOW(2, 1) AS pair
  )
  FROM orders
  STREAM_OPTIONS(EVENT_TYPE(WINDOW_CLOSE))
  INTO order_pair_results
  AS
    SELECT _twstart AS ts,
           _twend AS window_end,
           order_scope._twstart AS order_start,
           FIRST(soc) AS previous_soc,
           LAST(soc) AS current_soc,
           LAST(soc) - FIRST(soc) AS soc_delta
    FROM %%trows;
```

外层 `STATE_WINDOW` 定义订单边界。`order_id` 变化时，内层窗口会重置，因此 `COUNT_WINDOW` 只会配对同一订单内的数据。下游可以直接使用 `soc_delta` 判断 SOC 跳变或统计充电进度。类似的结构也适用于在线会话内的相邻采样分析，以及生产批次内的定量质量检查：外层定义业务边界，叶层定义计算粒度。

使用说明：

- 嵌套窗口必须显式指定触发表，不支持外部触发表，也不支持具有复合主键的触发表。
- 层类型支持 `SLIDING`、`INTERVAL`、`SESSION`、`STATE_WINDOW`、`EVENT_WINDOW` 和 `COUNT_WINDOW`。层内不支持 `PERIOD` 或另一个嵌套 `WINDOW (...)`。
- 非叶 `INTERVAL` 窗口不能重叠；非叶 `STATE_WINDOW` 必须指定 `EXTEND(1)`，且不能指定 `ZEROTH_STATE` 或 `TRUE_FOR`；非叶 `COUNT_WINDOW` 不能重叠；非叶 `EVENT_WINDOW` 只支持一个 `START WITH` 条件，且不能指定 `TRUE_FOR`。
- 触发表为超级表时，只要窗口链包含 `STATE_WINDOW`、`EVENT_WINDOW` 或 `COUNT_WINDOW`，就必须使用 `PARTITION BY tbname`。只有所有层均为 `SLIDING`、`INTERVAL` 或 `SESSION` 时才支持 `ROLLUP BY`。
- 整条窗口链只声明一份流选项。`WATERMARK`、`EXPIRED_TIME` 和 `IGNORE_DISORDER` 等输入策略在数据进入根层前应用；事件、计算和输出策略作用于叶层事件。

#### 触发动作

触发后可以根据需要执行不同的动作，比如发送[事件通知](#流式计算的通知机制)、[执行计算](#流式计算的计算任务)或者两者同时进行。

- 只通知不计算：通过 `WebSocket` 方式向外部应用发送事件通知。
- 只计算不通知：执行任意一个查询并保存结果到流式计算的输出表中。
- 既通知又计算：执行任意一个查询，同时发送计算结果或事件通知给外部应用。

#### 触发表与分组

通常意义来说，一个流式计算只对应一个计算，比如根据一个子表触发和产生一个计算，结果保存到一张表中。根据 TDengine **一个设备一张表** 的设计理念，如果需要对所有设备分别计算，那就需要为每个子表创建一个流式计算，这会造成使用的不便和处理效率的降低。为了解决这个问题，TDengine 的流式计算支持触发分组，分组是流式计算的最小执行单元，从逻辑上可以认为每个分组对应一个单独的流式计算，每个分组对应一个输出表和单独的事件通知。如果未指定分组或未指定触发表（定时触发方式允许），那么整个流式计算将只产生一个计算，可以认为此时只有一个分组，最终对应一个输出表和通知。由于每个分组都具有独立的流式计算，所以每个分组的计算进度、输出频率等都是不同的。

**总结来说，一个流式计算输出表（子表或普通表）的个数与触发表的分组个数相同，未指定分组时只产生一个输出表（普通表）。**目前支持的触发方式与分组组合如下：

| 触发方式                           | 支持的分组类型              |
| --------------------------------- | -------------------------- |
| PERIOD、SLIDING、INTERVAL、SESSION | 按子表分组、按标签分组、层级标签汇总分组、不分组 |
| 其他窗口触发                       | 按子表分组                    |

##### 触发表

触发表可以为普通表、超级表、子表、虚拟表，不支持系统表、视图、查询。除定时触发可不指定触发表外，其他触发方式必须指定触发表。

```sql
[FROM [db_name.]table_name]
```

##### 触发分组

指定触发的分组列，支持多列，目前只支持按照子表和标签进行分组。

```sql
[PARTITION BY col1 [, ...]]
```

也可以指定一个层级标签汇总分组列。`ROLLUP BY` 与 `PARTITION BY` 互斥，只支持一个标签列。

```sql
[ROLLUP BY tag_name]
```

`ROLLUP BY` 用于标签值包含层级路径的场景，例如 `factory.workshop.line`。系统使用固定分隔符 `.` 将 `tag_name` 的字符串值展开为从根到当前节点的所有路径前缀，每个路径前缀都是一个独立的触发分组。例如标签值为 `A.B.C` 时，会展开为 `A`、`A.B`、`A.B.C` 三个分组；该子表的数据同时参与这三个分组的触发和计算。父级分组的数据集合包含本级路径以及所有后代路径的子表数据。

使用说明：

- `tag_name` 必须是触发超级表或虚拟超级表上的 `VARCHAR` 或 `NCHAR` 类型标签列。
- `ROLLUP BY` 支持 `PERIOD`、`SLIDING`、`INTERVAL`、`SESSION` 四类触发；不支持状态窗口、事件窗口和计数窗口。
- 使用 `ROLLUP BY` 时必须显式指定 `FROM <table_name>`，即使触发类型为 `PERIOD`。
- 标签值为 `NULL` 或空字符串时，不产生 rollup 分组，不触发、不计算。
- 标签值不能包含前导分隔符、尾随分隔符、连续分隔符、空路径段、控制字符或路径段首尾空白。检测到非法路径时，流进入错误状态。
- 被 `ROLLUP BY` 引用的标签列不允许修改、删除或重命名；删除源子表不会删除已经生成的输出子表。

### 流式计算的结果输出

流式计算的计算结果默认会保存到输出表中，每个输出表中的计算结果是截至当前时刻已经触发和计算完成的输出。可以指定输出表的结构定义，如果存在分组还可以指定子表的标签值。

```sql
[INTO [db_name.]table_name] [NODELAY_CREATE_SUBTABLE] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [COMPOSITE KEY][, ...])] [TAGS (tag_definition [, ...])] 

tag_definition:
    tag_name type_name AS expr
```

说明如下：

- INTO [db_name.]table_name：可选，指定输出表的表名为 `table_name` 和所在数据库名 `db_name`。
  - 存在触发分组时该表为超级表。
  - 不存在触发分组时该表为普通表。
  - 只触发通知不计算，或计算结果只通知不保存时，不需要指定。
- [NODELAY_CREATE_SUBTABLE]：可选，指定在建流的时候立即创建每个分组的计算输出子表/普通表，默认情况下计算输出子表在有一条计算数据写入时才创建。如果添加该选项，创建流之后，子表/普通表会异步的创建，如果未全部创建成功，则流的状态会是 `Idle` ；如果创建成功，则状态会变更为  `Running` 。输出表为普通表和超级表默认会在建流的时候自动建立，无需进行配置。
- [OUTPUT_SUBTABLE(tbname_expr)]：可选，指定每个触发分组的计算输出表（子表）名，没有触发分组时不可以指定。未指定时自动为每个分组生成唯一的输出表（子表）名。`tbname_expr` 为任意输出字符串的表达式，可根据需要选择触发表分组列（来自 `[PARTITION BY col1[, ...]]`）。使用 `ROLLUP BY` 时，可以使用 `%%1` 引用当前 rollup 节点完整路径，使用 `%%rollup_tag` 引用当前 rollup 节点本级标签值；不能使用 `_trollup_tbcount`。输出长度不能超过表名最大长度，超过时截断处理。如果不希望不同分组输出到同一子表中，用户需确保每个分组输出表名都是唯一的。
- [(column_name1, column_name2 [COMPOSITE KEY][, ...])]：可选，指定输出表的每列列名，未指定时每列列名与计算结果的每列列名相同。可以通过 `[COMPOSITE KEY]` 指定第二列为主键列，与第一列共同组成复合主键。
- [TAGS (tag_definition [, ...])]：可选，指定输出超级表的标签列定义与值的列表，只有存在触发分组时才可以指定。未指定时，标签列的定义和值来自于所有分组列，此时分组列中不可以存在相同的列名。当按子表分组时，默认产生的标签列名为 `tag_tbname`，类型为 `VARCHAR(270)`；当使用 `ROLLUP BY` 时，默认标签值为当前 rollup 节点完整路径。具体的 `tag_definition` 参数说明如下：
  - tag_name：标签列名
  - type_name：标签列类型
  - expr：标签值计算表达式，可根据需要选择任意触发表分组列（来自 `[PARTITION BY col1[, ...]]`）。使用 `ROLLUP BY` 时，可以使用 `%%1` 和 `%%rollup_tag`，不能使用 `_trollup_tbcount`。

### 流式计算的计算任务

```sql
[AS subquery]
```

计算任务是流在事件触发后执行的计算动作，可以是任意类型的查询语句，既可以对触发表进行计算，也可以对其他库表进行计算。计算任务的灵活度很高，需在建流前进行合理的设计。注意事项如下：

- **查询输出的第一列将作为输出表的主键列**：要求查询输出的第一列为合法的主键数值（Timestamp），如果列类型不符建流时会报错，如果运算过程中出现 NULL 值则对应的计算结果会被丢弃处理。
- **每个触发分组的计算结果会写入到该分组的同一个输出表（子表或普通表）**：如果查询语句也包含分组子句，分组结果中相同主键的记录会产生覆盖。如果需要使用分组，建议为输出表定义复合主键。

#### 占位符

计算时可能需要使用触发时的关联信息，这些信息在 SQL 语句中以占位符的形式出现，在每次计算时会被作为常量替换到 SQL 语句中。包括：

| 触发方式 | 占位符            | 含义与说明                        |
| ------- | -----------------| --------------------------------- |
| 定时触发 | _tprev_localtime | 上一次触发时刻的系统时间（精度：ns） |
| 定时触发 | _tnext_localtime | 下一次触发时刻的系统时间（精度：ns） |
| 滑动触发 | _tprev_ts        | 上一次触发的事件时间（精度同记录）   |
| 滑动触发 | _tcurrent_ts     | 本次触发的事件时间（精度同记录）     |
| 滑动触发 | _tnext_ts        | 下一次触发的事件时间（精度同记录）   |
| 窗口触发 | _twstart         | 本次触发窗口的起始时间戳            |
| 窗口触发 | _twend           | 本次触发窗口的结束时间戳，只适用于 `WINDOW_CLOSE` 触发使用 |
| 窗口触发 | _twduration      | 本次触发窗口的持续时间，只适用于 `WINDOW_CLOSE` 触发使用   |
| 窗口触发 | _twrownum        | 本次触发窗口的记录条数，只适用于 `WINDOW_CLOSE` 触发使用   |
| 空闲触发 | _tidlestart      | 分组进入空闲前最后一次收到数据的时间（processing time，精度：ns）。只适用于 `IDLE`/`RESUME` 触发使用，不可与 `_twstart/_twend` 混用。由于输出表通常为 ms 精度，建议使用 `cast(_tidlestart/1000000 as timestamp)` 进行转换。 |
| 空闲触发 | _tidleend        | IDLE 或 RESUME 事件的触发时间（精度：ns）。只适用于 `IDLE`/`RESUME` 触发使用，不可与 `_twstart/_twend` 混用。由于输出表通常为 ms 精度，建议使用 `cast(_tidleend/1000000 as timestamp)` 进行转换。 |
| 通用     | _tgrpid     | 触发分组的 ID 值，类型为 BIGINT         |
| 通用     | _tlocaltime | 本次触发时刻的系统时间（精度：ns）       |
| 通用     | %%n         | 触发分组列的引用<br/>n 为分组列（来自 `[PARTITION BY col1[, ...]]`）的下标（从 1 开始）<br/>使用 `ROLLUP BY` 时，`%%1` 表示当前 rollup 节点完整路径       |
| 通用     | %%tbname    | 触发表每个分组表名的引用<br/>只有触发分组含 tbname 时可用<br/>可作为查询表名使用（`FROM %%tbname`）  |
| 通用     | %%trows     | 触发表每个分组的触发数据集（满足本次触发的数据集）的引用<br/>定时触发时为上次与本次触发之间写入的触发表数据<br/>使用 `ROLLUP BY` 时，表示当前 rollup 节点本级路径及所有后代路径关联子表的触发数据集<br/>只可作为查询表名使用（`FROM %%trows`）<br/>只适用于 `WINDOW_CLOSE` 触发使用<br/>推荐在小数据量场景下使用|
| ROLLUP BY | %%rollup_tag | 当前 rollup 节点路径的最后一级标签值。路径不含 `.` 时取完整路径 |
| ROLLUP BY | _trollup_tbcount | 当前 rollup 节点在本次触发时关联的源子表数量 |

使用限制：

- %%trows：只能用于 FROM 子句，在使用 %%trows 的语句中不支持 where 条件过滤，不支持对 %%trows 进行关联查询。
- %%tbname：可以用于 FROM、SELECT 和 WHERE 子句。
- %%rollup_tag：只在使用 `ROLLUP BY` 时可用，可以用于 `OUTPUT_SUBTABLE`、`TAGS`、`AS subquery` 中现有触发占位符允许的位置。
- _trollup_tbcount：只在使用 `ROLLUP BY` 时可用，只能用于 `AS subquery`，不能用于 `OUTPUT_SUBTABLE` 或 `TAGS`。
- 其他占位符：只能用于 SELECT 和 WHERE 子句。
- 嵌套窗口中，未限定的触发占位符引用叶层；层名限定占位符只在计算查询及其子查询中可见。

### 流式计算的控制选项

```sql
[STREAM_OPTIONS(stream_option [|...])]

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types) | IGNORE_NODATA_TRIGGER | IDLE_TIMEOUT(duration_time) | FLUSH_ON_OUTER_CLOSE}
```

控制选项用于控制触发和计算行为，可以多选，同一个选项不可以多次指定。包括：

- WATERMARK(duration_time)：指定数据乱序的容忍时长，超过该时长的数据会被当做乱序数据，根据不同触发方式的乱序数据处理策略和用户配置进行处理，未指定时默认 `duration_time` 值为 0。
- EXPIRED_TIME(exp_time) ：指定过期数据间隔并忽略过期数据，未指定时无过期数据。不需要感知超过一定时间范围的数据写入或更新时可以指定。`exp_time` 为过期时间间隔，支持的时间单位包括：毫秒 (a)、秒 (s)、分 (m)、小时 (h)、天 (d)。
- IGNORE_DISORDER：指定忽略触发表的乱序数据，未指定时不忽略乱序数据。注重计算或通知的时效性、触发表乱序数据不影响计算结果等场景可以指定。乱序数据既包括新的乱序数据的写入，也包括对已写入数据的更新操作。对于滑动步长为 1 的计数窗口（例如 `COUNT_WINDOW(1)` 和 `COUNT_WINDOW(n, 1)`），未指定该选项时乱序数据和更新会触发自动重算，指定后仍会被忽略；滑动步长不为 1 的计数窗口会忽略乱序数据和更新。
- DELETE_RECALC：指定触发表的数据删除（包含触发子表被删除场景）需要自动重新计算，只有触发方式支持数据删除的自动重算才可以指定。未指定时忽略数据删除，只有触发表数据删除会影响计算结果的场景才需要指定。计数窗口中只有滑动步长为 1 的窗口（例如 `COUNT_WINDOW(1)` 和 `COUNT_WINDOW(n, 1)`）可以指定该选项；滑动步长不为 1 的计数窗口不支持。
- DELETE_OUTPUT_TABLE：指定触发子表被删除时其对应的输出子表也需要被删除，只适用于按子表分组的场景，不适用于 `PARTITION BY` 标签分组和 `ROLLUP BY` 层级标签汇总分组。未指定时触发子表被删除不会删除其输出子表。
- FILL_HISTORY[(start_time)]：指定需要从 `start_time`（事件时间）开始触发历史数据计算，未指定时从最早的记录开始触发计算。如果未指定 `FILL_HISTORY` 和 `FILL_HISTORY_FIRST`，则不进行历史数据的触发计算。该选项不能与 `FILL_HISTORY_FIRST` 同时指定。定时触发（PERIOD）模式下不支持历史计算。
- FILL_HISTORY_FIRST[(start_time)]：指定需要从 `start_time`（事件时间）开始优先触发历史数据计算，未指定时从最早的记录开始触发计算。该选项适合在需要按照时间顺序计算历史数据且历史数据计算完成前不需要实时计算的场景下指定，未指定时优先实时计算，不能与 `FILL_HISTORY` 同时指定。定时触发（PERIOD）模式下不支持历史计算。
- CALC_NOTIFY_ONLY：指定计算结果只发送通知，不保存到输出表，未指定时默认会保存到输出表。
- LOW_LATENCY_CALC：指定触发后需要低延迟的计算或通知，单次触发发生后会立即启动计算或通知。低延迟的计算或通知会保证实时流式计算任务的时效性，但是也会造成处理效率的降低，有可能需要更多的处理资源才能满足需求，因此只推荐在业务有强时效性要求时使用。未指定时单次触发发生后有可能不会立即进行计算，采用批量计算与通知的方式来达到较好的资源利用效率。
- PRE_FILTER(expr) ：指定在触发进行前对触发表进行数据过滤处理，只有符合条件的数据才会进入触发判断，`expr` 中可以包含列、标签、常量及其标量与逻辑运算。例如：`col1 > 0` 则只有 col1 为正数的数据行可以进行触发，未指定时无触发表数据过滤。
- FORCE_OUTPUT：指定计算结果强制输出选项，当某次触发没有计算结果时将强制输出一行数据，除常量外（含常量对待列）其他列的值都为 NULL，后续版本会增加更多填充策略。
- MAX_DELAY(delay_time)：指定在窗口未关闭时的最长等待的时长（处理时间），从窗口开启时每经过该时间段且窗口仍未关闭时产生触发，非窗口触发时自动忽略。当窗口触发存在 `TRUE_FOR` 条件且 `TRUE_FOR` 时长大于 `MAX_DELAY` 时，`MAX_DELAY` 仍然生效 (即使最终当前窗口未满足 `TRUE_FOR` 条件)。`delay_time` 为等待时长，支持的时间单位包括：秒 (s)、分 (m)、小时 (h)、天 (d)，最小允许的值为 3 秒，误差范围在 1 秒以内，当计算时长超过 `delay_time` 时忽略期间的 `MAX_DELAY` 触发。`WATERMARK` 的判断逻辑早于窗口判定，因此可能出现设定 `max_delay` 但仍未产生触发的情况，这是由于窗口并未真正开启。
- EVENT_TYPE(event_types)：指定窗口触发的事件类型，可以多选，未指定时默认值为 `WINDOW_CLOSE`。SLIDING 触发（不带 INTERVAL）和 PERIOD 触发不适用（自动忽略）。各选项含义如下：
  - WINDOW_OPEN：窗口启动事件。
  - WINDOW_CLOSE：窗口关闭事件。
  - IDLE：分组空闲事件，当某分组超过 `IDLE_TIMEOUT` 配置的时长未收到新数据时触发一次，需同时配置 `IDLE_TIMEOUT`。
  - RESUME：分组恢复事件，当处于空闲状态的分组重新收到新数据时立即触发一次，需同时配置 `IDLE_TIMEOUT`。
- IGNORE_NODATA_TRIGGER：指定忽略触发表无输入数据时的触发，适用于滑动触发（SLIDING）、时间窗口触发（INTERVAL）、定时触发（PERIOD）。
  - 滑动触发与定时触发：如果两次触发时刻中间触发表没有数据则忽略该次触发。
  - 时间窗口触发：如果窗口内触发表没有数据则忽略该次触发。
  - 未指定时：不忽略无输入数据时的触发。
- IDLE_TIMEOUT(duration_time)：开启分组空闲检测，指定空闲超时时长。当某个分组超过该时长未收到任何新数据时，视为进入空闲状态并触发 IDLE 事件；当空闲分组重新收到数据时触发 RESUME 事件。需与 `EVENT_TYPE(IDLE)` 和（或）`EVENT_TYPE(RESUME)` 配合使用。`duration_time` 支持的时间单位包括：毫秒 (a)、秒 (s)、分 (m)、小时 (h)、天 (d)，有效范围为 `[1s, 10d]`。空闲检测基于 processing time（数据到达并被处理的时间），使用单调时钟计算间隔，不受系统时钟跳变影响。
- FLUSH_ON_OUTER_CLOSE：只适用于嵌套窗口。默认情况下，祖先窗口关闭时会丢弃尚未完成的叶层窗口。启用该选项后，仅当 `EVENT_TYPE` 包含 `WINDOW_CLOSE` 时，才使用即将关闭的祖先上下文提前关闭每个未完成的叶层窗口。该选项不会增加事件类型，也不会绕过叶层的 `TRUE_FOR` 条件；仅启用 `WINDOW_OPEN` 时该选项不生效。

### 流式计算的通知机制

事件通知是流在事件触发后可选的执行动作，支持通过 `WebSocket` 协议发送事件通知到应用。用户通过 `notification_definition` 来指定需要通知的事件，以及用于接收通知消息的目标地址。通知内容可以包含计算结果，也可以在没有计算结果时只通知事件相关信息。

```sql
[notification_definition]

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

event_types:
    event_type [|event_type]

event_type: {WINDOW_OPEN | WINDOW_CLOSE | IDLE | RESUME}
```

详细说明如下：

- url [, ...]：指定通知的目标地址，必须包括协议、IP 或域名、端口号，并允许包含路径、参数，整个 url 需要包含在引号内。目前仅支持 WebSocket 协议。例如：`ws://localhost:8080`、`ws://localhost:8080/notify`、`ws://localhost:8080/notify?key=foo`。
- [ON (event_types)]：指定需要通知的事件类型，可多选，以 `|` 分隔关键字（不可写成字符串或逗号列表）。`SLIDING`（不带 `INTERVAL`）和 `PERIOD` 触发不需要指定 `ON`；其他触发必须指定。`ON (...)` 支持的事件类型有：
  - WINDOW_OPEN：窗口打开事件，在触发表分组窗口打开时发送通知。
  - WINDOW_CLOSE：窗口关闭事件，在触发表分组窗口关闭时发送通知。
  - IDLE：分组空闲事件，当分组进入空闲状态时发送通知，需同时在 `STREAM_OPTIONS` 中配置 `IDLE_TIMEOUT`。
  - RESUME：分组恢复事件，当空闲分组重新收到数据时发送通知，需同时在 `STREAM_OPTIONS` 中配置 `IDLE_TIMEOUT`。
  - 说明：`PERIOD` / 纯 `SLIDING` 触发时，通知消息中的 `eventType` 固定为 `ON_TIME`，该值仅出现在消息体中，不能写在 `ON (...)` 列表里。
- [WHERE condition]：指定通知需要满足的条件，`condition` 中只能指定含计算结果列和（或）常量的条件。
- [NOTIFY_OPTIONS(notify_option[|notify_option])]：可选，指定通知选项用于控制通知行为。当前语法仅支持：
  - NOTIFY_HISTORY：指定计算历史数据时是否发送通知，未指定时默认不发送。
  - 说明：`ON_FAILURE_PAUSE` 暂不支持，参见 [运维与限制](./02-instructions.md#规则和限制)。

当触发指定的事件时，taosd 会向指定的 URL 发送 POST 请求，消息体为 JSON 格式。一个请求可能包含若干个流的若干个事件，且事件类型不一定相同。
事件信息视窗口类型而定：

- 时间窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。
- 状态窗口：开始时发送起始时间、前一个窗口的状态键值、当前窗口的状态键值；结束时发送起始时间、结束时间、计算结果、当前窗口的状态键值、下一个窗口的状态键值。状态键值统一使用按 `STATE_WINDOW` 参数顺序排列的数组；单列状态窗口为长度 1 的数组，多列状态窗口为长度等于状态键个数的数组。
- 会话窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。
- 事件窗口：开始时发送起始时间，触发窗口打开的数据值和对应条件编号；结束时发送起始时间、结束时间、计算结果、触发窗口关闭的数据值和对应条件编号。
- 计数窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。

状态窗口通知示例如下：

```json
{"prevState":[1],"curState":[2]}
```

```json
{"curState":[2, "a"],"nextState":[2, "b"]}
```

通知消息的结构示例如下：

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
          "groupId": "2650968222368530754",
          "windowStart": 1733284800000
        },
        {
          "tableName": "t_a667a16127d3b5a18988e32f3e76cd30",
          "eventType": "WINDOW_CLOSE",
          "eventTime": 1733284887197,
          "triggerId": "window-id-67890",
          "triggerType": "Interval",
          "groupId": "2650968222368530754",
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
          "groupId": "7533998559487590581",
          "windowStart": 1733284800000,
          "triggerCondition": {
            "conditionIndex": 0,
            "fieldValue": {
              "c1": 10,
              "c2": 15
            }
          }
        },
        {
          "tableName": "t_96f62b752f36e9b16dc969fe45363748",
          "eventType": "WINDOW_CLOSE",
          "eventTime": 1733284887231,
          "triggerId": "window-id-13579",
          "triggerType": "Event",
          "groupId": "7533998559487590581",
          "windowStart": 1733284800000,
          "windowEnd": 1733284810000,
          "triggerCondition": {
            "conditionIndex": 1,
            "fieldValue": {
              "c1": 20,
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

后续小节是通知消息中各个字段的说明。

#### 根级字段说明

- messageId：字符串类型，是通知消息的唯一标识符，确保整条消息可以被追踪和去重。
- timestamp：长整型时间戳，表示通知消息生成的时间，精确到毫秒，即：'00:00, Jan 1 1970 UTC' 以来的毫秒数。
- streams：对象数组，包含多个流任务的事件信息。(详细信息见下节)

#### stream 对象的字段说明

- streamName：字符串类型，流任务的名称，用于标识事件所属的流。
- events：对象数组，该流任务下的事件列表，包含一个或多个事件对象。(详细信息见下节)

#### event 对象的字段说明

##### 通用字段

这部分是所有 event 对象所共有的字段。

- tableName：字符串类型，是对应目标子表的表名，当没有输出的时候，该字段不存在。
- eventType：字符串类型，表示事件类型，支持 ON_TIME、WINDOW_OPEN、WINDOW_CLOSE、IDLE、RESUME 五种类型。
- eventTime：长整型时间戳，表示事件生成时间，精确到毫秒，即：'00:00, Jan 1 1970 UTC' 以来的毫秒数。
- triggerId：字符串类型，触发事件的唯一标识符，确保打开和关闭事件（如果有的话）的 ID 一致，便于外部系统将两者关联。如果 taosd 发生故障重启，部分事件可能会重复发送，会保证同一事件的 triggerId 保持不变。
- triggerType：字符串类型，表示触发类型，支持 Period、SLIDING 两种非窗口触发类型以及 INTERVAL、State、Session、Event、Count 五种窗口类型。
- groupId：字符串类型，是对应分组的唯一标识符，如果是按子表分组，则与对应表的 uid 一致。若没有进行分组，该字段为 0.

##### 定时触发相关字段

这部分是 triggerType 为 Period 时 event 对象的关键字段。

- eventType 固定为 ON_TIME，包含如下字段：
  - result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

##### 滑动触发（Sliding）相关字段

这部分是 triggerType 为 Sliding 时 event 对象的关键字段。

- eventType 固定为 ON_TIME，包含如下字段：
  - result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

##### 时间窗口（Interval）相关字段

这部分是 triggerType 为 Interval 时 event 对象的关键字段。

- 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
- 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
  - windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。

##### 状态窗口相关字段

这部分是 triggerType 为 State 时 event 对象才有的字段。

- 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
  - prevState：表示上一个窗口的状态键值。如果存在上一个窗口，则为按 `STATE_WINDOW` 参数顺序排列的数组；单列状态窗口时该数组长度为 1。如果没有上一个窗口（即当前是第一个 `WINDOW_OPEN`），则为 JSON `NULL`。
  - curState：表示当前窗口的状态键值，为按 `STATE_WINDOW` 参数顺序排列的数组；单列状态窗口时该数组长度为 1。
- 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
  - windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
  - curState：表示当前窗口的状态键值，为按 `STATE_WINDOW` 参数顺序排列的数组；单列状态窗口时该数组长度为 1。
  - nextState：表示下一个窗口的状态键值，为按 `STATE_WINDOW` 参数顺序排列的数组；单列状态窗口时该数组长度为 1。
  - result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

##### 会话窗口相关字段

这部分是 triggerType 为 Session 时 event 对象才有的字段。

- 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
- 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
  - windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
  - result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

##### 事件窗口相关字段

这部分是 triggerType 为 Event 时 event 对象才有的字段。

- 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
  - windowIndex：整型，表示子事件窗口在父窗口中的序号，从 0 开始编号；常规事件窗口或父窗口的值为 -1。
  - triggerCondition：触发窗口开始的条件信息，包括以下字段：
    - conditionIndex：整型，表示满足的触发窗口开始的条件的索引，从 0 开始编号。
    - fieldValue：键值对形式，包含条件列列名及其对应的值。
- 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
  - windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
  - windowIndex：整型，表示子事件窗口在父窗口中的序号，从 0 开始编号；常规事件窗口或父窗口的值为 -1。
  - triggerCondition：触发窗口关闭的条件信息，包括以下字段：
    - conditionIndex：整型，表示满足的触发窗口关闭的条件的索引，从 0 开始编号。
    - fieldValue：键值对形式，包含条件列列名及其对应的值。
  - result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

##### 计数窗口相关字段

这部分是 triggerType 为 Count 时 event 对象才有的字段。

- 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
- 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
  - windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
  - windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
  - result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

##### 空闲触发相关字段

这部分是 eventType 为 IDLE 或 RESUME 时 event 对象才有的字段。

- 如果 eventType 为 IDLE，则包含如下字段：
  - idleStart：长整型，分组进入空闲前最后一次收到数据的时间，ns 精度 Unix epoch。
  - idleEnd：长整型，IDLE 事件触发时间，ns 精度 Unix epoch。
  - idleDurationMs：长整型，空闲持续时长（毫秒），使用单调时钟计算。
- 如果 eventType 为 RESUME，则包含如下字段：
  - idleStart：长整型，空闲周期开始时的时间戳（与对应 IDLE 事件的 idleStart 一致），ns 精度 Unix epoch。
  - idleEnd：长整型，RESUME 事件触发时间，ns 精度 Unix epoch。
  - idleDurationMs：长整型，从空闲开始到恢复的持续时长（毫秒），使用单调时钟计算。

同一分组的一次空闲周期内，IDLE 与对应的 RESUME 事件具有相同的 `triggerId`，便于外部系统关联两个事件。

## 删除流式计算

仅删除流式计算任务，由流式计算写入的数据不会被删除。

```sql
DROP STREAM [IF EXISTS] [db_name.]stream_name [, [db_name.]stream_name] ...
```

## 查看流式计算

### 查看流信息

显示当前数据库或指定数据库下的流；可用 `LIKE` 对流名模糊匹配。完整语法见 [SHOW STREAMS](../05-tdengine-sql/09-system-info/03-show.md#show-streams)。

```sql
SHOW [db_name.]STREAMS [LIKE 'pattern'];
```

查看指定流的创建语句（自 `v3.4.1.13`）：

```sql
SHOW CREATE STREAM [db_name.]stream_name;
```

更完整字段可查询 [`INS_STREAMS`](../05-tdengine-sql/09-system-info/01-meta.md#ins_streams)：

```sql
SELECT * FROM information_schema.`ins_streams`;
```

重算相关记录见 [`INS_STREAM_RECALCULATES`](../05-tdengine-sql/09-system-info/01-meta.md#ins_stream_recalculates)。

### 查看流任务

流式计算在执行时由多个任务组成，可从 [`INS_STREAM_TASKS`](../05-tdengine-sql/09-system-info/01-meta.md#ins_stream_tasks) 获取任务信息：

```sql
SELECT * FROM information_schema.`ins_stream_tasks`;
```

## 操作流式计算

### 启动操作

``` SQL
START STREAM [IF EXISTS] [IGNORE UNTREATED] [db_name.]stream_name; 
```

说明：

- 没有指定 `IF EXISTS` 时，如果该 stream 不存在，则报错；如果存在，则启动流式计算。
- 指定 `IF EXISTS` 时，如果 stream 不存在，则返回成功；如果存在，则启动流式计算。
- 建流后，流自动启动运行，不需要用户启动；只有在停止操作后才需要通过启动操作恢复运行。
- 未指定 `IGNORE UNTREATED` 时，启动后会将流停止期间写入、尚未处理的数据按历史数据补算；指定 `IGNORE UNTREATED` 时忽略这部分未处理数据。

### 停止操作

``` SQL
STOP STREAM [IF EXISTS] [db_name.]stream_name; 
```

说明：

- 没有指定 `IF EXISTS` 时，如果该 stream 不存在，则报错，如果存在，则停止流式计算。
- 指定 `IF EXISTS` 时，如果该 stream 不存在，则返回成功，如果存在，则停止流式计算。
- 停止操作是持久有效的，在用户重启流运行之前不会重新运行。

## 流式计算里使用外部源（类似联邦查询）

说明：

- 从 3.4.2.0 版本开始，流计算支持触发和计算里使用外部源。
- 历史计算暂不支持。
- 使用外部源性能没有本地库性能好，需根据具体使用场景具体优化。
- 外部源触发，不支持的流计算选项：EXPIRED_TIME/IGNORE_DISORDER/DELETE_RECALC。
- 外部源的变更（删表，改表，连接不可达等等），可能导致未定义的流计算行为。所以如果要变更外部源，最好把流先删除然后重建。
- tbname 支持
  - 对于 mysql/pg 外部源，流计算里不支持 partition by tbname 语法。
  - 对于 influxdb 外部源，支持 partition by tbname 语法，内部会转换为 partition by 所有的 tag。不支持 partition by function(tbname)。
  - 对于 influxdb 外部源，可以使用 %%tbname(将所有 tag 拼接起来作为 tbname，如果超过表名长度，则用拼接的字符串获取 hash 值标识)。
- influxdb 外部源，tag 名超过 64 个字符或者 tag 值超过 256 个字符，会报错。需要修改数据后，重新建流。
