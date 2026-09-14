---
sidebar_label: 时区与自然时间单位
title: 时区与自然时间单位
description: 时区语义、自然时间单位与相关 SQL / 流任务配置说明
toc_max_heading_level: 4
---

本文描述 TDengine 时区语义与自然时间单位。功能分版本交付，文中以版本号标注：

| 标记 | 含义 |
| --- | --- |
| （无标记） | `v3.4.1` 已支持 |
| `v3.4.2` | `v3.4.2` 起支持 |
| `v3.4.3` | `v3.4.3` 起支持（`v3.4.2` 中尚不可用） |

---

## 时区概述

TDengine 内部以 UTC 时间戳（int64）存储所有时间数据。时区仅在 **时间字符串与 UTC 之间的转换** 时起作用：写入时将本地时间字符串转为 UTC，读取时将 UTC 转为本地时间字符串展示。

### 支持的时区格式

#### IANA 名称

推荐使用。示例：`'Asia/Shanghai'`、`'America/New_York'`。

- **夏令时感知**：是。系统自动处理 DST 跳变与切换。

#### POSIX 固定偏移

示例：`'+08:00'`、`'-0500'`、`'Z'`、`'+10'`、`'UTC+08:00'`、
`'UTC8'`。

- **夏令时感知**：否。全年使用恒定偏移。
- **支持范围**：`-14:00` 至 `+14:00`。
- **符号约定**：`SET TIMEZONE`、`ALTER LOCAL`、`taos.cfg`、`TO_CHAR`、`TIMETRUNCATE` 均遵循 POSIX 符号约定（`+` = UTC 以西，即 `local = UTC − offset`）。**例外**：`TO_ISO8601` 的固定偏移参数使用 [ISO 8601 符号约定](#to_iso8601)。详见下文。

#### POSIX 固定偏移格式详解

TDengine 的固定偏移时区格式遵循 **POSIX `TZ` 环境变量规范** 的子集。完整的 POSIX 规范定义了如下格式：

```text
STD offset [ DST [ dstoffset ] [ , rule ] ]
```

其中 `STD` 是标准时间缩写，`offset` 是与 UTC 的偏移量，`DST` 和 `dstoffset` 用于夏令时定义，`rule` 用于夏令时切换规则。

**TDengine 支持的子集**：TDengine 仅支持该规范中 `STD offset` 部分，且 `STD` 只接受 `UTC` 一个值。不支持手动配置 `DST`、`dstoffset` 和 `rule`。如需夏令时支持，请使用 IANA 时区名称。TDengine 接受的固定偏移语法如下：

```text
UTC offset
```

**`offset` 字段格式**：带 `UTC` 前缀时，小时可以是一位或两位
数字，分钟（如使用）必须是两位数字；表示西向偏移的正号可以
省略，例如 `UTC8` 等价于 `UTC+8`，`UTC8:30` 等价于
`UTC+8:30`。不带 `UTC` 前缀时，正负号不能省略，支持 `±HH`、
`±HHMM` 和 `±HH:MM`。偏移量精确到分钟。`UTC` 前缀不区分
大小写，`utc+8` 与 `UTC+8` 等价。
特殊值 `Z` 等价于 `+00:00`。

**POSIX 符号约定**：POSIX 标准定义本地时间与 UTC 的关系为：

```text
local_time = UTC - offset
```

因此：正号 `+` 表示 UTC **以西**（西时区，本地时间比 UTC 慢），负号 `-` 表示 UTC **以东**（东时区，本地时间比 UTC 快）。这与 ISO 8601 的符号约定 **相反**。例如：

| 写法 | POSIX 含义 | 等效 IANA 时区 |
| --- | --- | --- |
| `'+08:00'` 或 `'UTC+08:00'` | UTC 以西 8 小时 | 接近 `Pacific/Pitcairn`（太平洋） |
| `'-08:00'` 或 `'UTC-08:00'` | UTC 以东 8 小时 | 接近 `Asia/Shanghai`（北京时间） |
| `'+05:30'` 或 `'UTC+05:30'` | UTC 以西 5.5 小时 | 接近 `America/Bogota`（哥伦比亚） |
| `'-05:30'` 或 `'UTC-05:30'` | UTC 以东 5.5 小时 | 接近 `Asia/Kolkata`（印度） |
| `'Z'` | UTC 本身 | `Etc/UTC` |

当省略 `UTC` 前缀时（如 `'+08:00'`），TDengine 仍按 POSIX 规则
解析偏移量，行为与带 `UTC` 前缀完全一致。此时正负号不能省略。

**支持范围**：偏移量的有效范围为 `-14:00` 至 `+14:00`（对应 UTC 以东 14 小时到 UTC 以西 14 小时）。

**与 IANA 时区的区别**：POSIX 固定偏移不包含任何夏令时信息，全年使用恒定偏移。如果目标地区存在夏令时（如美国、欧洲），应使用 IANA 名称以获得正确的 DST 自动切换。

### 时区优先级

TDengine 采用五层时区优先级体系，高层覆盖低层：

| 优先级 | 名称 | 设定方式 | 说明 |
| --- | --- | --- | --- |
| 最高 | SQL 级 | 函数时区参数（如 `TO_ISO8601(ts, '+09:00')`）；`TO_ISO8601` IANA 参数 `v3.4.2`；流任务 `STREAM_OPTIONS(CONFIG(...))` `v3.4.3` | 函数参数仅影响本条 SQL；流 CONFIG 在创建时固化到单个流 |
| 高 | 连接级 | C API `taos_options_connection`；`SET TIMEZONE` `v3.4.2` | 影响当前连接的所有 SQL |
| 中 | 客户端全局 | 客户端侧 `taos.cfg` 中 `timezone` | 仅影响客户端本地时间展示 |
| 低 | 服务端全局 | 服务端侧 `taos.cfg` 中 `timezone` | 连接未设时区时服务端计算的回退 |
| 最低 | 系统默认 | 操作系统自动检测 | 最终兜底 |

**重要**：客户端全局时区仅影响客户端本地展示（如 `SELECT ts` 的输出格式化），不影响服务端计算。未通过连接级设置时区的连接，服务端计算回退到服务端全局时区。

## 设置时区

### SET TIMEZONE（自 `v3.4.2`） {#set-timezone-v342}

设置当前连接的时区：

```sql
SET TIMEZONE 'Asia/Shanghai';
SET TIMEZONE '-08:00';      -- POSIX: local = UTC+8，效果同北京时间
SET TIMEZONE '+08:00';      -- POSIX: local = UTC-8，不是北京时间
SET TIMEZONE 'America/New_York';
```

固定偏移的正负号遵循 POSIX 符号约定，详见上文"POSIX 固定偏移格式详解"。

设置后，当前连接里的“当前时间显示”和大多数“和本地日历有关”的操作都会使用这个时区，例如：

- `SELECT ts` 这类时间列的显示
- `SELECT NOW()` / `SELECT NOW`
- `TO_ISO8601(ts)` 这类按时区格式化时间的函数
- `TODAY()`
- 带有自然时间边界的计算，如 `TIMETRUNCATE(..., 1d/1w/1n...)`、`INTERVAL`

也可通过 C API `taos_options_connection` 在建立连接时设置时区，效果等同于 `SET TIMEZONE`。

若希望当前连接按北京时间显示与计算，可直接执行：

```sql
SET TIMEZONE 'Asia/Shanghai';
```

### 查询当前时区

```sql
SELECT TIMEZONE();
```

返回当前连接当前生效的单个时区字符串，回退链为连接级 `SET TIMEZONE` / C API 设置值 → 连接创建时快照的客户端全局时区 → 系统默认时区。

`TIMEZONE()` 返回当前连接实际生效的时区。

### 配置文件设置

在 `taos.cfg` 中配置全局时区：

```text
timezone Asia/Shanghai
timezone UTC-8
timezone UTC8
timezone +08:00
```

支持 IANA 名称、Windows 标准时区名称（如 `China Standard Time`）
以及固定偏移格式 `Z`、`±HH`、`±HHMM`、`±HH:MM`、
`UTCH[:MM]`、`UTC+H[:MM]` 和 `UTC-H[:MM]`（`H` 为一位或两位数字，
`UTC` 前缀不区分大小写）。
`GMT` / `GMT±...` 不支持。未配置时使用操作系统检测的时区。

- **服务端侧** `taos.cfg`：连接未通过 `SET TIMEZONE` 设置时区时，服务端计算回退到此值。
- **客户端侧** `taos.cfg`：仅影响客户端本地时间展示（如 `SELECT ts` 的输出格式化），不影响服务端计算。

**注意**：固定偏移写法遵循 POSIX 符号约定（详见"POSIX 固定偏移格式详解"），所有入口（`SET TIMEZONE`、`ALTER LOCAL`、`taos.cfg`）的正负号含义一致。建议使用 IANA 名称以避免混淆。

`ALTER LOCAL 'timezone ...'` 和 `SET TIMEZONE ...` 的区别：

- `SET TIMEZONE` 只影响当前连接，断开重连后就没了。
- `ALTER LOCAL 'timezone ...'` 修改的是当前客户端进程里的全局配置，只会影响修改后新建的连接，已经打开的旧连接不会立刻改变。

## 一周起始日

### SET FIRST_DAY_OF_WEEK（自 `v3.4.2`） {#set-first_day_of_week-v342}

设置当前连接的一周起始日：

```sql
SET FIRST_DAY_OF_WEEK 0;  -- 周日起始
SET FIRST_DAY_OF_WEEK 1;  -- 周一起始
```

**说明**：客户端配置参数 `firstDayOfWeek` 的默认值为 `4`（周四），见下文配置表。上述 SQL 仅设置当前连接。

取值范围为 `0`–`6`：`0`=周日，`1`=周一，…，`6`=周六。

### 查询当前周起始日（自 `v3.4.2`）

```sql
SELECT FIRST_DAY_OF_WEEK();
```

返回当前连接生效的周起始日设置，结果为 `0..6` 的整数，其中 `0=周日`，`1=周一`，...，`6=周六`。

### 配置文件设置（自 `v3.4.2`）

在客户端侧 `taos.cfg` 中配置：

```text
firstDayOfWeek 4
```

也可通过 `ALTER LOCAL 'firstDayOfWeek' '<0..6>'` 在当前客户端进程内动态修改。该配置只影响修改后的新连接，已建立连接保持各自创建时的快照值。

默认值为 `4`（周四），与历史按 Unix epoch 取模的周对齐行为兼容。若客户端未显式配置，启动时会尝试从操作系统读取一周起始日；读取失败时回退到 `4`。

若希望按周统计从周一开始，可执行：

```sql
SET FIRST_DAY_OF_WEEK 1;
```

若希望从周日开始，设置为 `0`。

### 影响范围（自 `v3.4.2`）

`firstDayOfWeek` 影响所有以 `w`（周）为单位的操作：

- `TIMETRUNCATE(ts, 1w)` 的对齐日
- `INTERVAL(1w)` 的窗口起始日
- `PERIOD(1w)` 的触发日 `v3.4.3`
- `SLIDING(1w)` 的触发日 `v3.4.3`

## 时间函数

### TO_ISO8601

```sql
SELECT TO_ISO8601(ts) FROM t;                        -- 使用连接时区
SELECT TO_ISO8601(ts, '+09:00') FROM t;              -- 指定固定偏移（ISO 8601 符号）
SELECT TO_ISO8601(ts, 'UTC+09:00') FROM t;           -- 等价写法，'UTC' 前缀会被剥离
SELECT TO_ISO8601(ts, 'America/New_York') FROM t;    -- 指定 IANA 时区（自 `v3.4.2`）
```

**符号约定**：`TO_ISO8601` 是唯一使用 ISO 8601 符号约定的入口——`local = UTC + offset`，即 `'+08:00'` 表示东八区（北京时间）。以下写法完全等价：`'+0800'`、`'+08:00'`、`'UTC+8'`、`'UTC+0800'`、`'UTC+08:00'`。其余入口（`SET TIMEZONE`、`taos.cfg`、`TO_CHAR`、`TIMETRUNCATE` 等）均使用 POSIX 符号约定（`+` = 西区）。

使用 IANA 时区时，输出的偏移量随时刻的夏令时状态自动变化：

```sql
SET TIMEZONE 'America/New_York';           --（自 `v3.4.2`）
SELECT TO_ISO8601('2026-01-15 12:00:00');  -- ...T12:00:00-05:00 (EST, 冬令时)
SELECT TO_ISO8601('2026-07-15 12:00:00');  -- ...T12:00:00-04:00 (EDT, 夏令时)
```

### TIMETRUNCATE

将时间戳截断到指定单位边界。

```sql
SELECT TIMETRUNCATE(ts, 1d) FROM t;                          -- 截断到当天 00:00:00
SELECT TIMETRUNCATE(ts, 1w) FROM t;                          -- 截断到一周起始日 00:00:00
SELECT TIMETRUNCATE(ts, 1n) FROM t;                          -- 截断到当月 1 日（自 `v3.4.2`）
SELECT TIMETRUNCATE(ts, 1q) FROM t;                          -- 截断到当季首月 1 日（自 `v3.4.2`）
SELECT TIMETRUNCATE(ts, 1y) FROM t;                          -- 截断到当年 1 月 1 日（自 `v3.4.2`）
SELECT TIMETRUNCATE(ts, 1d, 'America/New_York') FROM t;      -- 指定时区（自 `v3.4.2`）
```

**支持的自然时间单位**

| 单位 | 含义 | 截断规则 | 版本 |
| --- | --- | --- | --- |
| `d` | 天 | 对齐到当天 00:00:00 | 已支持 |
| `w` | 周 | 对齐到一周起始日（由 `firstDayOfWeek` 决定）00:00:00 | 已支持，`v3.4.2` 起尊重 firstDayOfWeek |
| `n` | 月 | 对齐到当月 1 日 00:00:00 | `v3.4.2` |
| `q` | 季度 | 对齐到当季首月 1 日 00:00:00（Q1=1 月，Q2=4 月，Q3=7 月，Q4=10 月） | `v3.4.2` |
| `y` | 年 | 对齐到当年 1 月 1 日 00:00:00 | `v3.4.2` |

**示例**

```sql
SELECT TIMETRUNCATE('2026-03-15', 1n);   -- 2026-03-01 00:00:00（自 `v3.4.2`）
SELECT TIMETRUNCATE('2026-05-15', 1q);   -- 2026-04-01 00:00:00（自 `v3.4.2`）
SELECT TIMETRUNCATE('2026-08-15', 1y);   -- 2026-01-01 00:00:00（自 `v3.4.2`）
```

**第三参数**（时区）：

| 值 | 行为 | 版本 |
| --- | --- | --- |
| `0` | 使用 UTC（旧语义） | 已支持 |
| `1` | 使用连接时区（旧语义） | 已支持 |
| `'Asia/Shanghai'` | 使用指定 IANA 时区 | `v3.4.2` |
| `'+08:00'` | 使用指定固定偏移 | `v3.4.2` |
| 省略 | 使用连接时区 | 已支持 |

### TIMEZONE()

```sql
SELECT TIMEZONE();
```

返回当前连接当前使用的单个时区字符串。

- 当前连接执行过 `SET TIMEZONE` 时，优先返回连接级时区。
- 未设置连接级时区时，返回该连接创建时快照的客户端全局时区；若客户端也未配置，则回退到系统默认时区。
- `ALTER LOCAL 'timezone'` 只会影响修改后新建的连接，不会回写已有连接的 `TIMEZONE()` 结果。

若需确认 `SET TIMEZONE` 是否生效，可执行：

```sql
SELECT TIMEZONE();
SELECT TO_ISO8601(NOW());
```

例如：

- 执行 `SET TIMEZONE 'Asia/Shanghai'` 后，`TIMEZONE()` 返回 `Asia/Shanghai`。
- 未执行 `SET TIMEZONE` 时，返回连接创建时快照的客户端全局时区。
- 执行 `ALTER LOCAL 'timezone Asia/Shanghai'` 后，已有连接不受影响；新建连接才会使用新配置。

## INTERVAL 查询

`INTERVAL` 支持按自然时间单位切分窗口：

```sql
SELECT _wstart, COUNT(*) FROM meters
  INTERVAL(1n)                      -- 按月切分（自 `v3.4.2`）
  FILL(PREV);

SELECT _wstart, AVG(voltage) FROM meters
  INTERVAL(1q)                      -- 按季度切分（自 `v3.4.2`）
  FILL(NULL);

SELECT _wstart, SUM(energy) FROM meters
  INTERVAL(1w)                      -- 按周切分（尊重 firstDayOfWeek）[`v3.4.2`]
  FILL(LINEAR);
```

**支持的自然时间单位**

| 单位 | 窗口边界 | 版本 |
| --- | --- | --- |
| `d` | 本地时区每天 00:00:00 | 已支持 |
| `w` | 本地时区一周起始日 00:00:00（由 `firstDayOfWeek` 决定） | `v3.4.2` |
| `n` | 本地时区每月 1 日 00:00:00 | `v3.4.2` |
| `q` | 本地时区每季度首月 1 日 00:00:00 | `v3.4.2` |
| `y` | 本地时区每年 1 月 1 日 00:00:00 | `v3.4.2` |

**多倍数窗口**

```sql
INTERVAL(2q)   -- 半年窗口：[1 月，7 月), [7 月，次年 1 月)（自 `v3.4.2`）
INTERVAL(3n)   -- 季度窗口（等价 1q）：1/4/7/10 月（自 `v3.4.2`）
INTERVAL(2w)   -- 双周窗口（自 `v3.4.2`）
```

**夏令时处理**：窗口始终按本地挂钟时间对齐。DST 切换日窗口物理时长会变化（如春跳日 1d 窗口为 23 小时），这是正确行为。写入/查询在夏令时跳变与重叠区间的注意点，详见 [夏令时使用指南](./02-dst.md)。

**闰年/变长月**：窗口宽度自动适应实际天数（如 2 月窗口 28 或 29 天）。`FILL` 填充边界逐月/逐季推进。

## 流式计算时间配置

### 流 CONFIG（自 `v3.4.3`）

流不存在独立的 `TIMEZONE` 子句；只能使用 `STREAM_OPTIONS(CONFIG(...))` 配置流时间上下文：

```sql
-- PERIOD：季度对齐使用固化的完整配置。
CREATE STREAM quarterly_tokyo PERIOD(1q)
  STREAM_OPTIONS(CONFIG(timezone = 'Asia/Tokyo', firstDayOfWeek = 1))
  INTO tokyo_quarterly AS SELECT CAST(_tlocaltime / 1000000 AS TIMESTAMP) AS ts,
    AVG(current) FROM meters;

-- 独立 SLIDING：v3.4.3 起支持自然单位。
CREATE STREAM slide_ny SLIDING(1q) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'America/New_York', firstDayOfWeek = 0))
  INTO ny_quarterly AS SELECT _tprev_ts, _tcurrent_ts, AVG(current) FROM %%trows;

-- 翻滚 INTERVAL：省略 SLIDING 时以 interval_val 为步长。
CREATE STREAM monthly_uk INTERVAL(1n) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'Europe/London', firstDayOfWeek = 1))
  INTO uk_monthly AS SELECT _twstart, _twend, AVG(current) FROM %%trows;

-- 显式步长 INTERVAL：sliding 值可以大于 interval_val。
CREATE STREAM fixed_slide INTERVAL(1h) SLIDING(2h) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1))
  INTO fixed_out AS SELECT _twstart, _twend, AVG(current) FROM %%trows;
```

`timezone` 为字符串，`firstDayOfWeek` 为 `0..6` 的整数。CONFIG 键名大小写不敏感，但只接受 `timezone` 和 `firstDayOfWeek` 这两个规范名；别名和 `CONFIG()` 均被拒绝。重复出现时每个键独立地以最后一个值为准；缺少某个键时，该键继承当前连接的有效值。

创建时，完整键值对统一校验并规范化一次，然后持久化、固化。新流 SQL 恰好保存一个完整规范的 `CONFIG(timezone = '...', firstDayOfWeek = N)` 块。生命周期操作、重启、`SHOW CREATE`、`INS_STREAMS` SQL、导出和恢复都会保留它。要改变任一值必须重建流。旧流在离线升级后保留 NULL 时间元数据、未注入 CONFIG 的原始 SQL，以及原有动态运行时回退行为。

### 流时间配置的影响

固化的时区和每周首日会在适用处参与流日历对齐。PERIOD 和独立 SLIDING 仍各自使用原有调度器，不能认为二者使用 INTERVAL 的边界算法。`w` 的周边界由固化的 `firstDayOfWeek` 决定；`q` 会规范化为 `3n`。

### 流触发单位矩阵

| 位置 | 原有单位 | `v3.4.3` 正式支持或扩展规则 | 说明 |
| --- | --- | --- | --- |
| `PERIOD(period_time)` | `a/s/m/h/d/w/n/y`；源码已能解析 `q` | 将 `q` 纳入正式支持和回归保证 | 使用自己的调度器；`q` 为 `3n`。 |
| 独立 `SLIDING(sliding_val)` | `a/s/m/h/d/w` | 新增 `n/q/y` | 使用自己的调度器；`q` 为 `3n`。 |
| `INTERVAL(interval_val)` | `a/s/m/h` | 新增 `d/w/n/q/y` | 可省略 `SLIDING`，形成翻滚窗口。 |
| 显式 `INTERVAL ... SLIDING(sliding_val)` | `a/s/m/h/d/w` | 拒绝 `n/q/y` | 显式 sliding 值可以大于 interval_val。 |

`PERIOD(period_time[, offset_time])`、独立 `SLIDING(sliding_val[, offset_time])`
和 `INTERVAL(interval_val[, interval_offset])` 的 offset 均复用普通查询
INTERVAL 的接受范围、规范化和合法性比较。它们拒绝 `AUTO` 和负值，且 offset
必须严格小于对应基值。比较前先将 `q` 规范化为 `3n`；自然月与其他单位混合
比较时按每月 28 天保守换算。因此 `PERIOD(1n, 27d)` 合法，
`SLIDING(1q, 83d)` 合法，而 `INTERVAL(1n, 28d)`、
`PERIOD(1w, AUTO)` 和负 offset 非法。自然单位 INTERVAL 边界在适用处使用
固化时间配置；PERIOD 和独立 SLIDING 仍使用各自的调度器。

### 平台、TSMA 与升级约束

流只固化时区标识，不固化或分发 tzdb 规则。运行节点无法加载该标识时，流
进入失败状态且不回退到服务端全局时区、操作系统时区或 UTC；修复节点时区
数据后，由现有重试或重新部署机制恢复。完整 IANA/DST 语义沿用 Linux/macOS
已有时区后端，本特性不扩展 Windows 时区能力。

`CREATE TSMA` 生成的 backing stream 不使用本特性：其时间配置为空，
`ins_streams` 的 `timezone` 和 `first_day_of_week` 为 NULL，SQL 保持
`CREATE TSMA`。不能仅凭 NULL 时间列把它识别为旧用户流。

本版本只支持客户端和集群整体离线升级，不支持新旧版本混合运行。创建带新
时间元数据的流后，不支持直接降级并继续使用原数据；回滚必须恢复升级前备份。

### 查看流时间配置（自 `v3.4.3`）

```sql
SELECT stream_name, timezone, first_day_of_week, sql
FROM information_schema.ins_streams;
```

新流的 `sql` 包含规范 CONFIG 块，时间列保存固化值。旧流保留 NULL 时间列、原始 SQL 和动态回退行为。

## 各场景时区来源速查

| 场景 | 时区来源 | 版本说明 |
| --- | --- | --- |
| 写入 `INSERT`         | 连接 → 服务端全局 → OS | 将时间字符串转为 UTC，已支持 |
| 读取 `SELECT ts`      | 连接 → 客户端全局 → OS | 将 UTC 格式化为本地时间；连接级回退为 `v3.4.2` 起支持（此前仅用 OS 时区） |
| 函数（`TO_ISO8601` 等）| SQL 参数 → 连接 → 服务端全局 → OS | 固定偏移参数已支持；IANA 参数为 `v3.4.2` |
| `TIMETRUNCATE`        | SQL 参数 → 连接 → 服务端全局 → OS | `d`/`w` 已支持；`n`/`q`/`y` 为 `v3.4.2`；时区字符串参数为 `v3.4.2` |
| `INTERVAL` 查询窗口    | 连接 → 服务端全局 → OS | `d` 已支持；`w`/`n`/`q`/`y` 为 `v3.4.2` |
| `SHOW` / `EXPLAIN`    | 连接 → 客户端全局 → OS | 连接级回退为 `v3.4.2` 起支持（此前仅用 OS 时区） |
| 流式计算触发与计算      | `v3.4.3` 流 `CONFIG` → 缺少键时取当前连接值（创建时固化） | 新流持久化完整固化配置；旧流保留动态回退 |

## 配置参数一览

| 参数 | 配置文件 | 类型 | 默认值 | 说明 | 版本 |
| ---------------- | --- | --- | --- | --- | --- |
| `timezone`       | 服务端/客户端侧 `taos.cfg` | 字符串 | OS 检测 | 全局时区 | 已支持 |
| `firstDayOfWeek` | 客户端侧 `taos.cfg` | 整数 0-6 | 4（周四） | 一周起始日；也可用 `ALTER LOCAL` 动态修改 | `v3.4.2` |

## 错误信息

| 错误场景 | 错误信息 |
| --- | --- |
| 无效时区字符串 | `[0x26B2] Invalid timezone: '<value>'` |
| firstDayOfWeek 超出范围 | `[0x26B3] Invalid firstDayOfWeek: <value>, must be 0-6` |

## 版本支持矩阵

| 功能                                       | `v3.4.2` 之前 | `v3.4.2` | `v3.4.3` |
| ------------------------------------------ | :----------: | :-------: | :--------: |
| `timezone` 配置文件（服务端/客户端）         |      ✅      |    ✅     |    ✅     |
| `TO_ISO8601` 固定偏移参数                    |      ✅      |    ✅     |    ✅     |
| `TIMETRUNCATE` `d`/`w` 截断                  |      ✅      |    ✅     |    ✅     |
| `INTERVAL` 查询 `d` 窗口                      |      ✅      |    ✅     |    ✅     |
| `TIMEZONE()` 函数                            |      ✅      |  ✅（增强）   |    ✅     |
| PERIOD 触发 `a`/`s`/`m`/`h`/`d`/`w`/`n`/`y`  |      ✅      |    ✅     |    ✅     |
| SLIDING 触发 `a`/`s`/`m`/`h`/`d`/`w`         |      ✅      |    ✅     |    ✅     |
| INTERVAL 窗口触发 `a`/`s`/`m`/`h`            |      ✅      |    ✅     |    ✅     |
| `SET TIMEZONE`                             |      ❌      |    ✅     |    ✅     |
| `SET FIRST_DAY_OF_WEEK`                    |      ❌      |    ✅     |    ✅     |
| `firstDayOfWeek` 配置参数                   |      ❌      |    ✅     |    ✅     |
| `TO_ISO8601` IANA 时区参数                  |      ❌      |    ✅     |    ✅     |
| `TIMETRUNCATE` 时区字符串参数                |      ❌      |    ✅     |    ✅     |
| `TIMETRUNCATE` `n`/`q`/`y` 截断              |      ❌      |    ✅     |    ✅     |
| `INTERVAL` 查询 `w`/`n`/`q`/`y` 窗口         |      ❌      |    ✅     |    ✅     |
| 普通列读取使用连接时区                        |      ❌      |    ✅     |    ✅     |
| SHOW/EXPLAIN 使用连接时区                     |      ❌      |    ✅     |    ✅     |
| 流任务 `STREAM_OPTIONS(CONFIG(...))`         |      ❌      |    ❌     |    ✅     |
| 流任务时区/firstDayOfWeek 固化                |      ❌      |    ❌     |    ✅     |
| PERIOD 触发 `q` 季度                          |      ❌      |    ❌     |    ✅     |
| SLIDING 触发 `n`/`q`/`y`                     |      ❌      |    ❌     |    ✅     |
| INTERVAL 窗口触发 `d`/`w`/`n`/`q`/`y`        |      ❌      |    ❌     |    ✅     |
| `ins_streams` timezone/first_day_of_week 列  |      ❌      |    ❌     |    ✅     |
