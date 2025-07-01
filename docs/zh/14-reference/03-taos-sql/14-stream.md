---
sidebar_label: 流式计算
title: 流式计算
description: 流式计算的相关 SQL 的详细语法
---

与传统的流计算相比，TDengine 的流计算进行了功能和边界上的扩展。传统定义的流计算是一种以低延迟、持续性和事件时间驱动为核心，处理无界数据流的实时计算范式。TDengine 的流计算采用的是触发与计算分离的处理策略，处理的依然是持续的无界的数据流，但是进行了以下几个方面的扩展：

- 处理对象的扩展：传统的流计算其事件驱动对象与计算对象往往是统一的，根据同一份数据产生事件和计算。TDengine 的流计算支持触发（事件驱动）与计算的分离，也就意味着触发对象可以与计算对象进行分离。触发表与计算的数据源表可以不相同，甚至可以不需要触发表，处理的数据集合无论是列、时间范围都可以不相同。
- 触发方式的扩展：除了传统的数据写入触发方式外，TDengine 的流计算支持更多触发方式的扩展，例如 TDengine 支持的各种窗口计算。通过支持窗口触发，用户可以灵活的定义和使用各种方式的窗口来产生触发事件，并且窗口可以选择在开窗、关窗以及开关窗时进行触发。除了通过这些与触发表关联的事件时间驱动外，TDengine 还支持与事件时间无关的驱动，例如按照处理事件定义的定时触发等。在事件触发之前，TDengine 还支持对触发数据进行预先过滤处理，只有符合条件的数据才会进入触发。
- 计算的扩展：除了可以对触发表进行计算外，也可以对其他库表进行计算。计算的类型不受限制，任何查询语句都可以支持。计算结果的应用也可以根据需要进行选择，可以用来发送通知（不写入保存）、写入输出表，也可以两者同时使用。

除了对触发与计算的扩展外，TDengine 的流计算还对用户提供了其他使用上的便利。例如，针对不同用户对结果延迟的不同需求，可以支持用户在结果时效性与资源负载之间进行平衡。针对不同用户对非正常顺序写入场景的不同需求，可以支持用户灵活选择适合的处理方式与策略。

## 创建流式计算
```sql
CREATE STREAM [IF NOT EXISTS] [db_name.]stream_name stream_options [INTO [db_name.]table_name] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [PRIMARY KEY][, ...])] [TAGS (tag_definition [, ...])] [AS subquery]

stream_options: {
    trigger_type [FROM [db_name.]table_name] [PARTITION BY col1 [, ...]] [OPTIONS(stream_option [|...])] [notification_definition]
}
    
trigger_type: {
    PERIOD(period_time[, offset_time])
  | [INTERVAL(interval_val[, interval_offset])] SLIDING(sliding_val[, offset_time]) 
  | SESSION(ts_col, session_val)
  | STATE_WINDOW(col) [TRUE_FOR(duration_time)] 
  | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(duration_time)]
  | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]]) 
}
    
event_types:
    event_type [|event_type]    
    
event_type: {WINDOW_OPEN | WINDOW_CLOSE}    

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types)}

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]

notify_option: [NOTIFY_HISTORY | ON_FAILURE_PAUSE]

tag_definition:
    tag_name type_name [COMMENT 'string_value'] AS expr

```

### 流式计算的触发方式
流式计算支持事件触发和定时触发两种触发方式，触发对象与计算对象彼此分离。
- 事件触发：通过与触发表关联的事件时间驱动，可以灵活的定义和使用各种窗口来产生触发事件，支持在开窗、关窗以及开关窗时进行触发，支持对触发数据进行预先过滤处理，只有符合条件的数据才会进入触发。
- 定时触发：与事件时间无关，按照系统时间定时触发。

#### 触发类型
触发类型通过 `trigger_type` 指定，支持定时触发、滑动触发、会话窗口触发、状态窗口触发、事件窗口触发、计数窗口触发。其中，会话窗口触发、状态窗口触发、事件窗口触发和计数窗口触发搭配超级表时，必须与 `partition by tbname` 一起使用。对于数据源表是复合主键的流，不支持状态窗口、事件窗口、计数窗口的计算。

##### 定时触发
```sql
PERIOD(period_time[, offset_time])
```

定时触发通过系统时间的固定间隔来驱动，以建流当天系统时间的零点作为基准时间点，然后根据间隔来确定下次触发的时间点，可以通过指定时间偏移的方式来改变基准时间点。各参数含义如下：
- period_time：定时触发的系统时间间隔，支持的时间单位包括：毫秒(a)、秒(s)、分(m)、小时(h)、天(d)，支持的时间范围为 `[10a, 3650d]`。
- offset_time：可选，指定定时触发的时间偏移，支持的时间单位包括：毫秒(a)、秒(s)、分(m)、小时(h)。

使用说明：
- 定时间隔小于 1 天时，基准时间点的时间偏移在每天重置，表现为相对于每日零点的偏移，以前后两日的基准时间点为一个周期，在周期内按照间隔进行定时触发，最后一次定时触发的时间点与下一日的基准时间点之间的间隔可能小于固定的定时间隔。例如：
  - 定时间隔为 5 小时 30 分钟，那么在一天内的触发时刻为 `[00:00, 05:30, 11:00, 16:30, 22:00]`，后续每一天的触发时刻都是相同的。
  - 同样定时间隔，如果指定了偏移为 1 秒，那么在一天内的触发时刻为 `[00:01, 05:31, 11:01, 16:31, 22:01]`，后续每一天的触发时刻都是相同的。
  - 同样条件下，如果建流时当前系统时间为 `12:00`，那么在当天的触发时刻为 `[16:31, 22:01]`，后续每一天内的触发时刻同上。
- 定时间隔大于等于 1 天时，基准时间点只在第一次是相对于每日零点的偏移，后续不会进行重置。例如：
  - 定时间隔为 1 天 1 小时，建流时当前系统时间为 `05-01 12:00`，那么在当天及随后几天的触发时刻为 `[05-02 01:00, 05-03 02:00, 05-04 03:00, 05-05 04:00, ……]`。
  - 同样条件下，如果指定了偏移为 1 秒，那么在当天及随后几天的触发时刻为 `[05-02 01:01, 05-03 02:02, 05-04 03:03, 05-05 04:04, ……]`。

##### 滑动触发
```sql
[INTERVAL(interval_val[, interval_offset])] SLIDING(sliding_val[, offset_time]) 
```

滑动触发是指对触发表的写入数据按照事件时间的固定间隔来驱动的触发。可以有 INTERVAL 窗口，也可以没有。
- 存在 `INTERVAL` 窗口时，滑动触发的起始时间点是窗口的起始点，可以指定窗口的时间偏移，此时滑动的时间偏移不起作用。
- 不存在 `INTERVAL` 窗口时，滑动触发的触发时刻、时间偏移规则同定时触发相同，唯一的区别是系统时间变更为事件时间。

各参数含义如下：
- interval_val：可选，滑动窗口的时长。
- interval_offset：可选，滑动窗口的时间偏移。
- sliding_val：必选，事件时间的滑动时长。
- offset_time：可选，指定滑动触发的时间偏移，支持的时间单位包括：毫秒(a)、秒(s)、分(m)、小时(h)。

使用说明：
- 必须指定触发表，触发表为超级表时支持按 tag、子表分组，支持不分组。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。

##### 会话窗口触发
```sql
SESSION(ts_col, session_val)
```

会话窗口触发是指对触发表的写入数据按照会话窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。各参数含义如下：
- ts_col：主键列名。
- session_val：属于同一个会话的最大时间间隔，间隔小于等于session_val的记录都属于同一个会话。

使用说明：
- 必须指定触发表，触发表为超级表时支持按 tag、子表分组，支持不分组。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。

##### 状态窗口触发
```sql
STATE_WINDOW(col) [TRUE_FOR(duration_time)] 
```

状态窗口触发是指对触发表的写入数据按照状态窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。各参数含义如下：
- col：状态列的列名。
- duration_time：可选，指定窗口的最小持续时长，如果某个窗口的时长低于该设定值，则会自动舍弃，不产生触发。

使用说明：
- 必须指定触发表，触发表为超级表时支持按 tag、子表分组，支持不分组。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。
  
##### 事件窗口触发
```sql
EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(duration_time)]
```

状态窗口触发是指对触发表的写入数据按照状态窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。各参数含义如下：
- start_condition：事件开始条件的定义。
- end_condition：事件结束条件的定义。
- duration_time：可选，指定窗口的最小持续时长，如果某个窗口的时长低于该设定值，则会自动舍弃，不产生触发。

使用说明：
- 必须指定触发表，触发表为超级表时支持按 tag、子表分组，支持不分组。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。


##### 计数窗口触发
```sql
COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]]) 
```

计数窗口触发是指对触发表的写入数据按照计数窗口的方式进行窗口划分，当窗口启动和（或）关闭时进行的触发。支持列的触发，只有当指定的列有数据写入时才触发。各参数含义如下：
- count_val：计数条数，当写入数据条目数达到 count_val 时触发。
- sliding_val：可选，窗口滑动的条数。
- col1 [, ...]：可选，按列触发模式时的数据列列表，列表中任一列有非空数据写入时才为有效条目，NULL 值视为无效值。

使用说明：
- 必须指定触发表，触发表为超级表时支持按 tag、子表分组，支持不分组。
- 支持对写入数据进行处理过滤后（有条件）的窗口触发。


#### 触发表
```sql
[FROM [db_name.]table_name]
```

触发表可以为任意表类型，不支持系统表，不支持视图，不支持子查询，但支持虚拟表。除了定时触发可以不指定触发表外，其他触发方式必须指定触发表。

#### 触发分组
```sql
[PARTITION BY col1 [, ...]]
```

指定触发的分组列，支持多列，目前只支持按照 tbname（表）和 tag 进行分组。


### 流式计算的控制选项
```sql
[OPTIONS(stream_option [|...])]

stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY[(start_time)] | FILL_HISTORY_FIRST[(start_time)] | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types)}
```

指定流的控制选项用于控制触发和计算行为，可以多选，同一个选项不可以多次指定，目前支持的控制选项包括：
- WATERMARK(duration_time)：指定数据乱序的容忍时长，超过该时长的数据会被当做乱序数据，根据不同触发方式的乱序数据处理策略和用户配置进行处理，未指定时默认 duration_time 值为 0。
- EXPIRED_TIME(exp_time) ：指定过期数据间隔并忽略过期数据，未指定时无过期数据，如果业务不需要感知超过一定时间范围的数据写入或更新时可以指定。exp_time 为过期时间间隔，支持的时间单位包括：毫秒(a)、秒(s)、分(m)、小时(h)、天(d)，最小值为 1a。
- IGNORE_DISORDER：指定忽略触发表的乱序数据，未指定时不忽略乱序数据，对于业务非常注重计算或通知的时效性、触发表乱序数据不影响计算结果等场景可以指定。
- DELETE_RECALC: 指定触发表的数据删除（包含触发子表被删除场景）需要自动重新计算，只有触发方式支持数据删除的自动重算才可以指定。未指定时忽略数据删除，只有触发表数据删除会影响计算结果的场景才需要指定。
- DELETE_OUTPUT_TABLE：指定触发子表被删除时其对应的输出子表也需要被删除，只适用于按表分组的场景，未指定时触发子表被删除不会删除其输出子表。
- FILL_HISTORY[(start_time)]：指定需要从start_time（事件时间）开始触发历史数据计算，[(start_time)]可选，未指定时从最早的记录开始触发计算。如果未指定FILL_HISTORY和 FILL_HISTORY_FIRST则不进行历史数据的触发计算，该选项不能与FILL_HISTORY_FIRST同时指定 。定时触发（PERIOD）模式下不支持历史计算。
- FILL_HISTORY_FIRST[(start_time)]：指定需要从start_time（事件时间）开始优先触发历史数据计算，[(start_time)]可选，未指定时从最早的记录开始触发计算。该选项适合在需要按照时间顺序计算历史数据且历史数据计算完成前不需要实时计算的场景下指定，未指定时优先实时计算，不能与FILL_HISTORY同时指定。定时触发（PERIOD）模式下不支持历史计算。
- CALC_NOTIFY_ONLY：指定计算结果只发送通知，不保存到输出表，未指定时默认会保存到输出表。
- LOW_LATENCY_CALC：指定触发后需要低延迟的计算或通知，单次触发发生后会立即启动计算或通知。低延迟的计算或通知会保证实时流计算任务的时效性，但是也会造成处理效率的降低，有可能需要更多的处理资源才能满足需求，因此只推荐在业务有强时效性要求时使用。未指定时单次触发发生后有可能不会立即进行计算，采用批量计算与通知的方式来达到较好的资源利用效率。
- PRE_FILTER(expr) ：指定在触发进行前对触发表进行数据过滤处理，只有符合条件的数据才会进入触发判断，例如：col1 > 0 则只有 col1为正数的数据行可以进行触发，未指定时无触发表数据过滤。
- FORCE_OUTPUT：指定计算结果强制输出选项，当某次触发没有计算结果时将强制输出一行数据，除常量外（含常量对待列）其他列的值都为 NULL。
- MAX_DELAY(delay_time)：指定在窗口未关闭时最长等待的时长（处理时间），每超过该时间且窗口仍未关闭时产生触发，适用于所有窗口触发方式，SLIDING 触发（不带 INTERVAL）和 PERIOD 触发不适用（自动忽略）。当窗口触发存在TRUE_FOR条件且TRUE_FOR时长大于MAX_DELAY时，MAX_DELAY仍然生效，即使最终当前窗口未满足TRUE_FOR条件。delay_time为等待时长，支持的时间单位包括：毫秒(a)、秒(s)、分(m)、小时(h)、天(d)。
- EVENT_TYPE(event_types)：指定窗口触发的事件类型，可以多选，未指定时默认值为 WINDOW_CLOSE ，选项含义如下：
  - WINDOW_OPEN ：窗口启动事件。
  - WINDOW_CLOSE ：窗口关闭事件。
  SLIDING 触发（不带 INTERVAL）和 PERIOD 触发不适用（自动忽略）。

### 流式计算的通知机制
```sql
[notification_definition]

notification_definition:
    NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [NOTIFY_OPTIONS(notify_option[|notify_option])]
```

当需要发送事件通知时需要指定，详细说明如下：
- url [, ...]：指定通知的目标地址，必须包括协议、IP 或域名、端口号，并允许包含路径、参数，整个 url 需要包含在引号内。目前仅支持 websocket 协议。例如："ws://localhost:8080"、"ws://localhost:8080/notify"、"ws://localhost:8080/notify?key=foo"。
- [ON (event_types)]：指定需要通知的事件类型，可多选，对于SLIDING（不带 INTERVAL 窗口）和PERIOD触发不需要指定，其他触发必须指定，支持的事件类型有：
  - WINDOW_OPEN：窗口打开事件，在触发表分组窗口打开时发送通知。
  - WINDOW_CLOSE：窗口关闭事件，在触发表分组窗口关闭时发送通知。
- [WHERE condition]：指定通知需要满足的条件，condition中只能指定含计算结果列和（或）常量的条件。
- [NOTIFY_OPTIONS(notify_option[|notify_option)]]：可选，指定通知选项用于控制通知的行为，可以多选，目前支持的通知选项包括：
  - NOTIFY_HISTORY：指定计算历史数据时是否发送通知，未指定时默认不发送。
  - ON_FAILURE_PAUSE：指定在向通知地址发送通知失败时暂停流计算任务，循环重试发送通知，直至发送成功才恢复流计算运行，未指定时默认直接丢失事件通知（不影响流计算继续运行）。


### 流式计算的计算对象
```sql
[AS subquery]
```

指定流计算的计算任务语句，可以是任意类型的查询语句。既可以对触发表进行计算，也可以对其他库表进行计算。

#### 占位符
计算时可能需要使用触发时的一些关联信息，这些信息在 SQL 语句中以占位符的形式出现，在每次真实计数时会被作为常量替换到 SQL 语句中。这里定义和列举了计算 SQL 语句中可以使用的触发相关的一些占位符：

| 触发方式 | 占位符            | 含义与说明                        |
| ------- | -----------------| --------------------------------- |
| 滑动触发 | _tprev_ts        | 上一次触发的事件时间（精度同记录）   |
| 滑动触发 | _tcurrent_ts     | 本次触发的事件时间（精度同记录）     |
| 滑动触发 | _tnext_ts        | 下一次触发的事件时间（精度同记录）   |
| 窗口触发 | _twstart         | 本次触发窗口的起始时间戳            |
| 窗口触发 | _twend           | 本次触发窗口的结束时间戳            |
| 窗口触发 | _twduration      | 本次触发窗口的持续时间              |
| 窗口触发 | _twrownum        | 本次触发窗口的记录条数              |
| 定时触发 | _tprev_localtime | 上一次触发时刻的系统时间（精度：ns） |
| 定时触发 | _tnext_localtime | 下一次触发时刻的系统时间（精度：ns） |
| 通用     | _tgrpid     | 触发分组的 ID 值，类型为 BIGINT         |
| 通用     | _tlocaltime | 本次触发时刻的系统时间（精度：ns）       |
| 通用     | %%n         | 触发分组列的引用，n 为分组列（来自 [PARTITION BY col1[, ...]] ）的下标（从 1 开始）        |
| 通用     | %%tbname    | 触发表每个分组表名的引用，只有触发分组含 tbname 时可用，可作为查询表名使用（FROM %%tbname）  |
| 通用     | %%trows     | 触发表每个分组的触发数据集（满足本次触发的数据集）的引用，定时触发时为上次与本次触发之间写入的触发表数据 |

使用限制：
- %%trows：只能用于 FROM 子句，推荐在小数据量场景下使用。
- %%tbname：只能用于 FROM、SELECT 和 WHERE子 句。
- 其他占位符：只能用于 SELECT 和 WHERE 子句。

## 删除流式计算
仅删除流式计算任务，由流式计算写入的数据不会被删除。
```sql
DROP STREAM [IF EXISTS] [db_name.]stream_name;
```

## 查看流式计算
### 流的查看
显示当前数据库或指定数据库所属的流计算。
```sql
SHOW [db_name.]STREAMS;
```

如需查看所有流计算的信息或者流计算的更详细的信息，可以查询 `information_schema.ins_streams` 系统表。
``` SQL
SELECT * from information_schema.`ins_streams`;
```

### 任务查看
流计算在执行时由多个任务组成，用户或维护人员可以从系统表 `information_schema.ins_stream_tasks` 获取更详细的流的具体的任务信息。
``` SQL
SELECT * from information_schema.`ins_stream_tasks`;
```

## 操作流式计算
### 启动操作
``` SQL
START STREAM [IF EXISTS] [IGNORE UNTREATED] [db_name.]stream_name; 
```

说明：
- 没有指定 IF EXISTS，如果该 stream 不存在，则报错，如果存在，则启动流计算；指定了 IF EXISTS，如果 stream 不存在，则返回成功；如果存在，则启动流计算。
- 建流后流自动启动运行，不需要用户启动，只有在停止操作后才需要通过启动操作恢复流的运行。
- 启动流计算时，流计算停止期间写入的数据会被当做历史数据处理。

### 停止操作
``` SQL
STOP STREAM [IF EXISTS] [db_name.]stream_name; 
```

说明：
- 如果没有指定 IF EXISTS，如果该 stream 不存在，则报错；如果存在，则停止流计算。指定了 IF EXISTS，如果该 stream 不存在，则返回成功；如果存在，则停止流计算。
- 停止操作是持久有效的，在用户重启流运行之前不会重新运行。
  
### 手动重算
``` SQL
RECALCULATE STREAM [db_name.]stream_name FROM start_time [TO end_time];
```

说明：
- 用户可以根据需要来指定需要重算流的一段时间区间（事件时间）内的数据。
- 不适用于定时触发（PERIOD），适用于其他所有触发类型。



其中 subquery 是 select 普通查询语法的子集。




```sql
subquery: SELECT select_list
    from_clause
    [WHERE condition]
    [PARTITION BY tag_list]
    window_clause
```

支持会话窗口、状态窗口、滑动窗口、事件窗口和计数窗口。其中，状态窗口、事件窗口和计数窗口搭配超级表时必须与 partition by tbname 一起使用。对于数据源表是复合主键的流，不支持状态窗口、事件窗口、计数窗口的计算。

stb_name 是保存计算结果的超级表的表名，如果该超级表不存在，会自动创建；如果已存在，则检查列的 schema 信息。详见 [写入已存在的超级表](#写入已存在的超级表)。

TAGS 子句定义了流计算中创建 TAG 的规则，可以为每个 partition 对应的子表生成自定义的 TAG 值，详见 [自定义 TAG](#自定义-tag)
```sql
create_definition:
    col_name column_definition
column_definition:
    type_name [COMMENT 'string_value']
```

subtable 子句定义了流式计算中创建的子表的命名规则，详见 [流式计算的 partition](#流式计算的-partition)。

```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])
}
```

其中：

- SESSION 是会话窗口，tol_val 是时间间隔的最大范围。在 tol_val 时间间隔范围内的数据都属于同一个窗口，如果连续的两条数据的时间超过 tol_val，则自动开启下一个窗口。该窗口的 _wend 等于最后一条数据的时间加上 tol_val。

- STATE_WINDOW 是状态窗口，col 用来标识状态量，相同的状态量数值则归属于同一个状态窗口，col 数值改变后则当前窗口结束，自动开启下一个窗口。

- INTERVAL 是时间窗口，又可分为滑动时间窗口和翻转时间窗口。INTERVAL 子句用于指定窗口相等时间周期，SLIDING 字句用于指定窗口向前滑动的时间。当 interval_val 与 sliding_val 相等的时候，时间窗口即为翻转时间窗口，否则为滑动时间窗口，注意：sliding_val 必须小于等于 interval_val。

- EVENT_WINDOW 是事件窗口，根据开始条件和结束条件来划定窗口。当 start_trigger_condition 满足时则窗口开始，直到 end_trigger_condition 满足时窗口关闭。start_trigger_condition 和 end_trigger_condition 可以是任意 TDengine 支持的条件表达式，且可以包含不同的列。

- COUNT_WINDOW 是计数窗口，按固定的数据行数来划分窗口。count_val 是常量，是正整数，必须大于等于 2，小于 2147483648。count_val 表示每个 COUNT_WINDOW 包含的最大数据行数，总数据行数不能整除 count_val 时，最后一个窗口的行数会小于 count_val。sliding_val 是常量，表示窗口滑动的数量，类似于 INTERVAL 的 SLIDING。

窗口的定义与时序数据特色查询中的定义完全相同，详见 [TDengine 特色查询](../distinguished)

例如，如下语句创建流式计算。第一个流计算，自动创建名为 avg_vol 的超级表，以一分钟为时间窗口、30 秒为前向增量统计这些电表的平均电压，并将来自 meters 表的数据的计算结果写入 avg_vol 表，不同 partition 的数据会分别创建子表并写入不同子表。

第二个流计算，自动创建名为 streamt0 的超级表，将数据按时间戳的顺序，以 voltage < 0 作为窗口的开始条件，voltage > 9 作为窗口的结束条件，划分窗口做聚合运算，并将来自 meters 表的数据的计算结果写入 streamt0 表，不同 partition 的数据会分别创建子表并写入不同子表。

第三个流计算，自动创建名为 streamt1 的超级表，将数据按时间戳的顺序，以 10 条数据为一组，划分窗口做聚合运算，并将来自 meters 表的数据的计算结果写入 streamt1 表，不同 partition 的数据会分别创建子表并写入不同子表。

```sql
CREATE STREAM avg_vol_s INTO avg_vol AS
SELECT _wstart, count(*), avg(voltage) FROM meters PARTITION BY tbname INTERVAL(1m) SLIDING(30s);

CREATE STREAM streams0 INTO streamt0 AS
SELECT _wstart, count(*), avg(voltage) from meters PARTITION BY tbname EVENT_WINDOW START WITH voltage < 0 END WITH voltage > 9;

CREATE STREAM streams1 IGNORE EXPIRED 1 WATERMARK 100s INTO streamt1 AS
SELECT _wstart, count(*), avg(voltage) from meters PARTITION BY tbname COUNT_WINDOW(10);
```

notification_definition 子句定义了窗口计算过程中，在窗口打开/关闭等指定事件发生时，需要向哪些地址发送通知。详见 [流式计算的事件通知](#流式计算的事件通知)

## 流式计算的 partition

可以使用 PARTITION BY TBNAME，tag，普通列或者表达式，对一个流进行多分区的计算，每个分区的时间线与时间窗口是独立的，会各自聚合，并写入到目的表中的不同子表。

不带 PARTITION BY 子句时，所有的数据将写入到一张子表。

在创建流时不使用 SUBTABLE 子句时，流式计算创建的超级表有唯一的 tag 列 groupId，每个 partition 会被分配唯一 groupId。与 schemaless 写入一致，我们通过 MD5 计算子表名，并自动创建它。

若创建流的语句中包含 SUBTABLE 子句，用户可以为每个 partition 对应的子表生成自定义的表名，例如：

```sql
CREATE STREAM avg_vol_s INTO avg_vol SUBTABLE(CONCAT('new-', tname)) AS SELECT _wstart, count(*), avg(voltage) FROM meters PARTITION BY tbname tname INTERVAL(1m);
```

PARTITION 子句中，为 tbname 定义了一个别名 tname, 在 PARTITION 子句中的别名可以用于 SUBTABLE 子句中的表达式计算，在上述示例中，流新创建的子表将以前缀 'new-' 连接原表名作为表名（从 v3.2.3.0 开始，为了避免 SUBTABLE 中的表达式无法区分各个子表，即误将多个相同时间线写入一个子表，在指定的子表名后面加上 _stableName_groupId）。

注意，子表名的长度若超过 TDengine 的限制，将被截断。若要生成的子表名已经存在于另一超级表，由于 TDengine 的子表名是唯一的，因此对应新子表的创建以及数据的写入将会失败。

## 流式计算读取历史数据

正常情况下，流式计算不会处理创建前已经写入源表中的数据，若要处理已经写入的数据，可以在创建流时设置 fill_history 1 选项，这样创建的流式计算会自动处理创建前、创建中、创建后写入的数据。流计算处理历史数据的最大窗口数是 2000 万，超过限制会报错。例如：

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 interval(10s)
```

结合 fill_history 1 选项，可以实现只处理特定历史时间范围的数据，例如：只处理某历史时刻（2020 年 1 月 30 日）之后的数据

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 where ts > '2020-01-30' interval(10s)
```

再如，仅处理某时间段内的数据，结束时间可以是未来时间

```sql
create stream if not exists s1 fill_history 1 into st1  as select count(*) from t1 where ts > '2020-01-30' and ts < '2023-01-01' interval(10s)
```

如果该流任务已经彻底过期，并且您不再想让它检测或处理数据，您可以手动删除它，被计算出的数据仍会被保留。

注意：
- 开启 fill_history 时，创建流需要找到历史数据的分界点，如果历史数据很多，可能会导致创建流任务耗时较长，此时可以通过 fill_history 1 async（v3.3.6.0 开始支持）语法将创建流的任务放在后台处理，创建流的语句可立即返回，不阻塞后面的操作。async 只对 fill_history 1 起效，fill_history 0 时建流很快，不需要异步处理。

- 通过 show streams 可查看后台建流的进度（ready 状态表示成功，init 状态表示正在建流，failed 状态表示建流失败，失败时 message 列可以查看原因。对于建流失败的情况可以删除流重新建立）。

- 另外，不要同时异步创建多个流，可能由于事务冲突导致后面创建的流失败。

## 删除流式计算

```sql
DROP STREAM [IF EXISTS] stream_name;
```

仅删除流式计算任务，由流式计算写入的数据不会被删除。

## 展示流式计算

```sql
SHOW STREAMS;
```

若要展示更详细的信息，可以使用：

```sql
SELECT * from information_schema.`ins_streams`;
```

## 流式计算的触发模式

在创建流时，可以通过 TRIGGER 指令指定流式计算的触发模式。

对于非窗口计算，流式计算的触发是实时的；对于窗口计算，目前提供 4 种触发模式，默认为 WINDOW_CLOSE。

1. AT_ONCE：写入立即触发

2. WINDOW_CLOSE：窗口关闭时触发（窗口关闭由事件时间决定，可配合 watermark 使用）

3. MAX_DELAY time：若窗口关闭，则触发计算。若窗口未关闭，且未关闭时长超过 max delay 指定的时间，则触发计算。

4. FORCE_WINDOW_CLOSE：以操作系统当前时间为准，只计算当前关闭窗口的结果，并推送出去。窗口只会在被关闭的时刻计算一次，后续不会再重复计算。该模式当前只支持 INTERVAL 窗口（不支持滑动）；FILL_HISTORY 必须为 0，IGNORE EXPIRED 必须为 1，IGNORE UPDATE 必须为 1；FILL 只支持 PREV、NULL、NONE、VALUE。

5. CONTINUOUS_WINDOW_CLOSE：窗口关闭时输出结果。修改、删除数据，并不会立即触发重算，每等待 rec_time_val 时长，会进行周期性重算。如果不指定 rec_time_val，那么重算周期是 60 分钟。如果重算的时间长度超过 rec_time_val，在本次重算后，自动开启下一次重算。该模式当前只支持 INTERVAL 窗口。如果使用 FILL，需要配置 adapter 的相关信息：adapterFqdn、adapterPort、adapterToken。adapterToken 为 `{username}:{password}` 经过 Base64 编码之后的字符串，例如 `root:taosdata` 编码后为 `cm9vdDp0YW9zZGF0YQ==`

由于窗口关闭是由事件时间决定的，如事件流中断、或持续延迟，则事件时间无法更新，可能导致无法得到最新的计算结果。

因此，流式计算提供了以事件时间结合处理时间计算的 MAX_DELAY 触发模式。MAX_DELAY 最小时间是 5s，如果低于 5s，创建流计算时会报错。

MAX_DELAY 模式在窗口关闭时会立即触发计算。此外，当数据写入后，计算触发的时间超过 max delay 指定的时间，则立即触发计算

## 流式计算的窗口关闭

流式计算以事件时间（插入记录中的时间戳主键）为基准计算窗口关闭，而非以 TDengine 服务器的时间，以事件时间为基准，可以避免客户端与服务器时间不一致带来的问题，能够解决乱序数据写入等等问题。流式计算还提供了 watermark 来定义容忍的乱序程度。

在创建流时，可以在 stream_option 中指定 watermark，它定义了数据乱序的容忍上界。

流式计算通过 watermark 来度量对乱序数据的容忍程度，watermark 默认为 0。

T = 最新事件时间 - watermark

每次写入的数据都会以上述公式更新窗口关闭时间，并将窗口结束时间 < T 的所有打开的窗口关闭，若触发模式为 WINDOW_CLOSE 或 MAX_DELAY，则推送窗口聚合结果。


![TDengine 流式计算窗口关闭示意图](./pic/watermark.webp)


图中，纵轴表示不同时刻，对于不同时刻，我们画出其对应的 TDengine 收到的数据，即为横轴。

横轴上的数据点表示已经收到的数据，其中蓝色的点表示事件时间 (即数据中的时间戳主键) 最后的数据，该数据点减去定义的 watermark 时间，得到乱序容忍的上界 T。

所有结束时间小于 T 的窗口都将被关闭（图中以灰色方框标记）。

T2 时刻，乱序数据（黄色的点）到达 TDengine，由于有 watermark 的存在，这些数据进入的窗口并未被关闭，因此可以被正确处理。

T3 时刻，最新事件到达，T 向后推移超过了第二个窗口关闭的时间，该窗口被关闭，乱序数据被正确处理。

在 window_close 或 max_delay 模式下，窗口关闭直接影响推送结果。在 at_once 模式下，窗口关闭只与内存占用有关。


## 流式计算对于过期数据的处理策略

对于已关闭的窗口，再次落入该窗口中的数据被标记为过期数据。

TDengine 对于过期数据提供两种处理方式，由 IGNORE EXPIRED 选项指定：

1. 增量计算，即 IGNORE EXPIRED 0。

2. 直接丢弃，即 IGNORE EXPIRED 1：默认配置，忽略过期数据


无论在哪种模式下，watermark 都应该被妥善设置，来得到正确结果（直接丢弃模式）或避免频繁触发重算带来的性能开销（重新计算模式）。

## 流式计算对于修改数据的处理策略

TDengine 对于修改数据提供两种处理方式，由 IGNORE UPDATE 选项指定：

1. 检查数据是否被修改，即 IGNORE UPDATE 0，如果数据被修改，则重新计算对应窗口。

2. 不检查数据是否被修改，全部按增量数据计算，即 IGNORE UPDATE 1，默认配置。


## 写入已存在的超级表
```sql
[field1_name, ...]
```
在本页文档顶部的 [field1_name, ...] 是用来指定 stb_name 的列与 subquery 输出结果的对应关系的。如果 stb_name 的列与 subquery 输出结果的位置、数量全部匹配，则不需要显示指定对应关系。如果 stb_name 的列与 subquery 输出结果的数据类型不匹配，会把 subquery 输出结果的类型转换成对应的 stb_name 的列的类型。创建流计算时不能指定 stb_name 的列和 TAG 的数据类型，否则会报错。

对于已经存在的超级表，检查列的 schema 信息
1. 检查列的 schema 信息是否匹配，对于不匹配的，则自动进行类型转换，当前只有数据长度大于 4096byte 时才报错，其余场景都能进行类型转换。
2. 检查列的个数是否相同，如果不同，需要显示的指定超级表与 subquery 的列的对应关系，否则报错；如果相同，可以指定对应关系，也可以不指定，不指定则按位置顺序对应。

## 自定义 TAG

用户可以为每个 partition 对应的子表生成自定义的 TAG 值。
```sql
CREATE STREAM streams2 trigger at_once INTO st1 TAGS(cc varchar(100)) as select _wstart, count(*) c1 from st partition by concat("tag-", tbname) as cc interval(10s);
```

PARTITION 子句中，为 concat("tag-", tbname) 定义了一个别名 cc，对应超级表 st1 的自定义 TAG 的名字。在上述示例中，流新创建的子表的 TAG 将以前缀 'new-' 连接原表名作为 TAG 的值。

会对 TAG 信息进行如下检查
1. 检查 tag 的 schema 信息是否匹配，对于不匹配的，则自动进行数据类型转换，当前只有数据长度大于 4096byte 时才报错，其余场景都能进行类型转换。
2. 检查 tag 的个数是否相同，如果不同，需要显示的指定超级表与 subquery 的 tag 的对应关系，否则报错；如果相同，可以指定对应关系，也可以不指定，不指定则按位置顺序对应。

## 清理中间状态

```
DELETE_MARK time
```
DELETE_MARK 用于删除缓存的窗口状态，也就是删除流计算的中间结果。如果不设置，默认值是 10 年
T = 最新事件时间 - DELETE_MARK

## 流式计算支持的函数

1. 所有的 [单行函数](../function/#单行函数) 均可用于流计算。
2. 以下 19 个聚合/选择函数 <b>不能</b> 应用在创建流计算的 SQL 语句。此外的其他类型的函数均可用于流计算。

- [leastsquares](../function/#leastsquares)
- [percentile](../function/#percentile)
- [top](../function/#top)
- [bottom](../function/#bottom)
- [elapsed](../function/#elapsed)
- [interp](../function/#interp)
- [derivative](../function/#derivative)
- [irate](../function/#irate)
- [twa](../function/#twa)
- [histogram](../function/#histogram)
- [diff](../function/#diff)
- [statecount](../function/#statecount)
- [stateduration](../function/#stateduration)
- [csum](../function/#csum)
- [mavg](../function/#mavg)
- [sample](../function/#sample)
- [tail](../function/#tail)
- [unique](../function/#unique)
- [mode](../function/#mode)

## 暂停、恢复流计算
1.流计算暂停计算任务
PAUSE STREAM [IF EXISTS] stream_name;
没有指定 IF EXISTS，如果该 stream 不存在，则报错；如果存在，则暂停流计算。指定了 IF EXISTS，如果该 stream 不存在，则返回成功；如果存在，则暂停流计算。

2.流计算恢复计算任务
RESUME STREAM [IF EXISTS] [IGNORE UNTREATED] stream_name;
没有指定 IF EXISTS，如果该 stream 不存在，则报错，如果存在，则恢复流计算；指定了 IF EXISTS，如果 stream 不存在，则返回成功；如果存在，则恢复流计算。如果指定 IGNORE UNTREATED，则恢复流计算时，忽略流计算暂停期间写入的数据。

## 状态数据备份与同步
流计算的中间结果成为计算的状态数据，需要在流计算整个生命周期中进行持久化保存。为了确保流计算中间状态能够在集群环境下在不同的节点间可靠地同步和迁移，从 v3.3.2.1 开始，需要在运行环境中部署 rsync 软件，还需要增加以下的步骤：
1. 在配置文件中配置 snode 的地址（IP + 端口）和状态数据备份目录（该目录系 snode 所在的物理节点的目录）。
2. 然后创建 snode。
完成上述两个步骤以后才能创建流。
如果没有创建 snode 并正确配置 snode 的地址，流计算过程中将无法生成检查点（checkpoint），并可能导致后续的计算结果产生错误。

> snodeAddress           127.0.0.1:873
> 
> checkpointBackupDir    /home/user/stream/backup/checkpoint/


## 创建 snode 的方式
使用以下命令创建 snode（stream node），snode 是流计算中有状态的计算节点，可用于部署聚合任务，同时负责备份不同的流计算任务生成的检查点数据。
```sql
CREATE SNODE ON DNODE [id]
```
其中的 id 是集群中的 dnode 的序号。请注意选择的 dnode，流计算的中间状态将自动在其上进行备份。
从 v3.3.4.0 开始，在多副本环境中创建流会进行 snode 的**存在性检查**，要求首先创建 snode。如果 snode 不存在，无法创建流。

## 流式计算的事件通知

### 使用说明

流式计算支持在窗口打开/关闭时，向外部系统发送相关的事件通知。用户通过 `notification_definition` 来指定需要通知的事件，以及用于接收通知消息的目标地址。

```sql
notification_definition:
    NOTIFY (url [, url] ...) ON (event_type [, event_type] ...) [notification_options]

event_type:
    'WINDOW_OPEN'
  | 'WINDOW_CLOSE'

notification_options: {
    NOTIFY_HISTORY [0|1]
    ON_FAILURE [DROP|PAUSE]
}
```

上述语法中的相关规则含义如下：
1. `url`：指定通知的目标地址，必须包括协议、IP 或域名、端口号，并允许包含路径、参数。目前仅支持 websocket 协议。例如：`ws://localhost:8080`、`ws://localhost:8080/notify`、`wss://localhost:8080/notify?key=foo`。
1. `event_type`：定义需要通知的事件，支持的事件类型有：
    1. WINDOW_OPEN：窗口打开事件，所有类型的窗口打开时都会触发。
    1. WINDOW_CLOSE：窗口关闭事件，所有类型的窗口关闭时都会触发。
1. `NOTIFY_HISTORY`：控制是否在计算历史数据时触发通知，默认值为 0，即不触发。
1. `ON_FAILURE`：向通知地址发送通知失败时 (比如网络不佳场景) 是否允许丢弃部分事件，默认值为 `PAUSE`。
    1. PAUSE 表示发送通知失败时暂停流计算任务。taosd 会重试发送通知，直到发送成功后，任务自动恢复运行。
    1. DROP 表示发送通知失败时直接丢弃事件信息，流计算任务继续运行，不受影响。

比如，以下示例创建一个流，计算电表电流的每分钟平均值，并在窗口打开、关闭时向两个通知地址发送通知，计算历史数据时不发送通知，不允许在通知发送失败时丢弃通知：

```sql
CREATE STREAM avg_current_stream FILL_HISTORY 1
    AS SELECT _wstart, _wend, AVG(current) FROM meters
    INTERVAL (1m)
    NOTIFY ('ws://localhost:8080/notify', 'wss://192.168.1.1:8080/notify?key=foo')
    ON ('WINDOW_OPEN', 'WINDOW_CLOSE');
    NOTIFY_HISTORY 0
    ON_FAILURE PAUSE;
```

当触发指定的事件时，taosd 会向指定的 URL 发送 POST 请求，消息体为 JSON 格式。一个请求可能包含若干个流的若干个事件，且事件类型不一定相同。
事件信息视窗口类型而定：

1. 时间窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。
1. 状态窗口：开始时发送起始时间、前一个窗口的状态值、当前窗口的状态值；结束时发送起始时间、结束时间、计算结果、当前窗口的状态值、下一个窗口的状态值。
1. 会话窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。
1. 事件窗口：开始时发送起始时间，触发窗口打开的数据值和对应条件编号；结束时发送起始时间、结束时间、计算结果、触发窗口关闭的数据值和对应条件编号。
1. 计数窗口：开始时发送起始时间；结束时发送起始时间、结束时间、计算结果。

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
          "windowId": "window-id-67890",
          "windowType": "Time",
          "groupId": "2650968222368530754",
          "windowStart": 1733284800000
        },
        {
          "tableName": "t_a667a16127d3b5a18988e32f3e76cd30",
          "eventType": "WINDOW_CLOSE",
          "eventTime": 1733284887197,
          "windowId": "window-id-67890",
          "windowType": "Time",
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
          "windowId": "window-id-13579",
          "windowType": "Event",
          "groupId": "7533998559487590581",
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
          "windowId": "window-id-13579",
          "windowType": "Event",
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

### 根级字段说明

1. messageId：字符串类型，是通知消息的唯一标识符，确保整条消息可以被追踪和去重。
1. timestamp：长整型时间戳，表示通知消息生成的时间，精确到毫秒，即：'00:00, Jan 1 1970 UTC' 以来的毫秒数。
1. streams：对象数组，包含多个流任务的事件信息。(详细信息见下节)

### stream 对象的字段说明

1. streamName：字符串类型，流任务的名称，用于标识事件所属的流。
1. events：对象数组，该流任务下的事件列表，包含一个或多个事件对象。(详细信息见下节)

### event 对象的字段说明

#### 通用字段

这部分是所有 event 对象所共有的字段。
1. tableName：字符串类型，是对应目标子表的表名。
1. eventType：字符串类型，表示事件类型，支持 WINDOW_OPEN、WINDOW_CLOSE、WINDOW_INVALIDATION 三种类型。
1. eventTime：长整型时间戳，表示事件生成时间，精确到毫秒，即：'00:00, Jan 1 1970 UTC' 以来的毫秒数。
1. windowId：字符串类型，窗口的唯一标识符，确保打开和关闭事件的 ID 一致，便于外部系统将两者关联。如果 taosd 发生故障重启，部分事件可能会重复发送，会保证同一窗口的 windowId 保持不变。
1. windowType：字符串类型，表示窗口类型，支持 Time、State、Session、Event、Count 五种类型。
1. groupId: 字符串类型，是对应分组的唯一标识符，如果是按表分组，则与对应表的 uid 一致。

#### 时间窗口相关字段

这部分是 windowType 为 Time 时 event 对象才有的字段。
1. 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
1. 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
    1. windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
    1. result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

#### 状态窗口相关字段

这部分是 windowType 为 State 时 event 对象才有的字段。
1. 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
    1. prevState：与状态列的类型相同，表示上一个窗口的状态值。如果没有上一个窗口 (即：现在是第一个窗口)，则为 NULL。
    1. curState：与状态列的类型相同，表示当前窗口的状态值。
1. 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
    1. windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
    1. curState：与状态列的类型相同，表示当前窗口的状态值。
    1. nextState：与状态列的类型相同，表示下一个窗口的状态值。
    1. result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

#### 会话窗口相关字段

这部分是 windowType 为 Session 时 event 对象才有的字段。
1. 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
1. 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
    1. windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
    1. result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

#### 事件窗口相关字段

这部分是 windowType 为 Event 时 event 对象才有的字段。
1. 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
  1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
  1. triggerCondition：触发窗口开始的条件信息，包括以下字段：
    1. conditionIndex：整型，表示满足的触发窗口开始的条件的索引，从 0 开始编号。
    1. fieldValue：键值对形式，包含条件列列名及其对应的值。
1. 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
    1. windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
    1. triggerCondition：触发窗口关闭的条件信息，包括以下字段：
        1. conditionIndex：整型，表示满足的触发窗口关闭的条件的索引，从 0 开始编号。
        1. fieldValue：键值对形式，包含条件列列名及其对应的值。
    1. result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

#### 计数窗口相关字段

这部分是 windowType 为 Count 时 event 对象才有的字段。
1. 如果 eventType 为 WINDOW_OPEN，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
1. 如果 eventType 为 WINDOW_CLOSE，则包含如下字段：
    1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
    1. windowEnd：长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。
    1. result：计算结果，为键值对形式，包含窗口计算的结果列列名及其对应的值。

#### 窗口失效相关字段

因为流计算过程中会遇到数据乱序、更新、删除等情况，可能造成已生成的窗口被删除，或者结果需要重新计算。此时会向通知地址发送一条 WINDOW_INVALIDATION 的通知，说明哪些窗口已经被删除。

这部分是 eventType 为 WINDOW_INVALIDATION 时，event 对象才有的字段。
1. windowStart：长整型时间戳，表示窗口的开始时间，精度与结果表的时间精度一致。
1. windowEnd: 长整型时间戳，表示窗口的结束时间，精度与结果表的时间精度一致。

## 流式计算对虚拟表的支持

从 v3.3.6.0 开始，流计算能够使用虚拟表（包括虚拟普通表、虚拟子表、虚拟超级表）作为数据源进行计算，语法和非虚拟表完全一致。

但是虚拟表的行为与非虚拟表存在差异，所以目前在使用流计算对虚拟表进行计算时存在以下限制：

1. 流计算中涉及的虚拟普通表/虚拟子表的 schema 不允许更改。
1. 流计算过程中，如果修改虚拟表某一列对应的数据源，对流计算来说不生效。即：流计算仍只读取老的数据源。
1. 流计算过程中，如果虚拟表某一列对应的原始表被删除，之后新建了同名的表和同名的列，流计算不会读取新表的数据。
1. 流计算的 watermark 只能是 0，否则创建时就报错。
1. 如果流计算的数据源是虚拟超级表，流计算任务启动后新增的子表不参与计算。
1. 虚拟表的不同原始表的时间戳不完全一致，数据合并后可能会产生空值，暂不支持插值处理。
1. 不处理数据的乱序、更新或删除。即：流创建时不能指定 `ignore update 0` 或者 `ignore expired 0`，否则报错。
1. 不支持历史数据计算，即：流创建时不能指定 `fill_history 1`，否则报错。
1. 不支持触发模式：MAX_DELAY, FORCE_WINDOW_CLOSE, CONTINUOUS_WINDOW_CLOSE。
1. 不支持窗口类型：COUNT_WINDOW。
