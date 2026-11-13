---
sidebar_label: 时区与自然时间单位
title: 时区与自然时间单位
description: 时区语义、自然时间单位与相关 SQL / 流任务配置说明
toc_max_heading_level: 4
---

## 时区概述

TDengine 内部以 UTC 时间戳（int64）存储所有时间数据。连接级时区用于无偏移时间字符串的解析、当地日历边界计算和结果展示，不改变原始时间戳表示的绝对时刻。

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

**`offset` 字段格式**：带 `UTC` 前缀时，小时可以是一位或两位数字，分钟（如使用）必须是两位数字；表示西向偏移的正号可以省略，例如 `UTC8` 等价于 `UTC+8`，`UTC8:30` 等价于 `UTC+8:30`。不带 `UTC` 前缀时，正负号不能省略，小时必须为两位数字，支持 `±HH`、`±HHMM` 和 `±HH:MM`，不支持 `+8`、`-4`。偏移量精确到分钟；小时为 14 时分钟必须为 00。`UTC` 前缀不区分大小写，`utc+8` 与 `UTC+8` 等价。特殊值 `Z` / `z` 等价于 `+00:00`，即 UTC 时区。

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

当省略 `UTC` 前缀时（如 `'+08:00'`），TDengine 仍按 POSIX 规则解析偏移量，行为与带 `UTC` 前缀完全一致。此时正负号不能省略。

**支持范围**：偏移量的有效范围为 `-14:00` 至 `+14:00`（对应 UTC 以东 14 小时到 UTC 以西 14 小时）。

**与 IANA 时区的区别**：POSIX 固定偏移不包含任何夏令时信息，全年使用恒定偏移。如果目标地区存在夏令时（如美国、欧洲），应使用 IANA 名称以获得正确的 DST 自动切换。

### Windows 支持

从 `v3.5.0` 起，Windows 与 Linux、macOS 一样支持连接级时区，可通过 `SET TIMEZONE` 或 C API 独立设置各连接的时区。Windows 支持 IANA 名称、Windows 标准名称（如 `China Standard Time`，对应 `Asia/Shanghai`）及上述固定偏移格式，按目标时刻应用历史和 DST 规则。

Windows 继承的 `TZ` 必须使用支持的时区格式。`EST5EDT,M3.2.0,M11.1.0`、`EST5EDT` 和 `est5edt` 等输入不受支持；新建连接或清除连接级时区设置时遇到这些输入，将返回 `Invalid or unsupported timezone` 错误。清除失败时保留原连接级时区。支持的 IANA 名称及 Windows 系统默认时区仍按目标时刻应用历史和 DST 规则。

### 时区优先级

普通 SQL 优先使用函数显式指定的时区，否则使用连接级时区：

| 优先级 | 名称 | 设定方式 | 说明 |
| --- | --- | --- | --- |
| 最高 | SQL 级 | 函数时区参数（如 `TO_ISO8601(ts, '+09:00')`）；流任务 `STREAM_OPTIONS(CONFIG(...))` | 函数参数仅影响本条 SQL；流 `CONFIG` 在创建时写入该流的定义，之后只作用于这一条流 |
| 高 | 连接级 | C API `taos_options_connection`；`SET TIMEZONE` | 影响当前连接的所有 SQL |
| 默认来源 | 客户端默认 | 客户端侧 `taos.cfg timezone`、`taos_options(TSDB_OPTION_TIMEZONE)`、`ALTER LOCAL` | 新连接或重置连接级时区时保存该值 |
| 未配置时 | 系统默认 | 客户端初始化时检测 | 客户端未配置时区时使用 |

新连接即使未执行 `SET TIMEZONE`，也会保存客户端默认时区，用于解析、计算和展示。SQL 解析和生成计划时使用当前连接级时区；已有查询结果保留原时区，后续连接选项变更不影响其时间展示。服务端全局时区不覆盖 SQL 携带的连接级时区。

## 设置时区

### 设置当前时区

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

返回当前连接生效的时区，格式为 `名称 (缩写, ±HHMM)`，例如 `America/New_York (EST, -0500)`。缩写和偏移按每次查询的当前时刻刷新，跨 DST 切换无需重连。

### 配置文件设置

在 `taos.cfg` 中配置全局时区：

```text
timezone Asia/Shanghai
timezone UTC-8
timezone UTC8
timezone +08:00
```

支持 IANA 名称、Windows 标准时区名称（如 `China Standard Time`）以及固定偏移格式 `Z`、`±HH`、`±HHMM`、`±HH:MM`、`UTCH[:MM]`、`UTC+H[:MM]` 和 `UTC-H[:MM]`（`H` 为一位或两位数字，`UTC` 前缀不区分大小写）。`GMT` / `GMT±...` 不支持。未配置时使用操作系统检测的时区。

- **客户端侧** `taos.cfg`：提供新连接和时区重置时使用的默认值，影响该连接的解析、计算和展示。
- **服务端侧** `taos.cfg`：用于服务端缺少明确时区上下文时的默认处理，不覆盖请求携带的连接级时区。

**注意**：固定偏移写法遵循 POSIX 符号约定（详见"POSIX 固定偏移格式详解"），所有入口（`SET TIMEZONE`、`ALTER LOCAL`、`taos.cfg`）的正负号含义一致。建议使用 IANA 名称以避免混淆。

`ALTER LOCAL 'timezone' '<value>'` 和 `SET TIMEZONE '<value>'` 的区别：

- `SET TIMEZONE` 只修改当前连接的连接级时区；新建连接重新取默认值，连接器可按连接参数再次设置。
- `ALTER LOCAL 'timezone' 'Asia/Shanghai'` 同时更新客户端默认时区和执行语句的当前连接；其他已有连接保持原时区，之后新建或重置时区的连接使用新默认值。

### REST 与 WebSocket

- REST 通过 `conn_tz` 指定本次请求的连接级时区，使用 IANA 名称或 `UTC`。设置仅对本次请求生效；未指定 `conn_tz` 的请求使用 taosAdapter 的客户端默认时区。原有 `tz` 参数仅影响响应中的时间展示，详见 [REST API](../../10-developer-guide/08-connectors-reference/10-rest-api.mdx#http-请求格式)。
- WebSocket 通过连接器的时区参数或 `SET TIMEZONE` 设置会话的连接级时区，持续用于后续 SQL，直到显式修改、清除或重连。
- JDBC WebSocket 还受驱动的 IANA 名称校验限制，不能直接套用 Native C API 的全部格式，详见 [Java 连接器](../../10-developer-guide/08-connectors-reference/02-java.mdx#设置时区)。

Windows 链路是否支持连接级时区取决于 taosAdapter 所加载原生客户端库的平台和版本，与发起 HTTP / WebSocket 请求的应用所在操作系统无关。

DST 跳过或重复的本地时间应使用整数时间戳或带明确偏移的字符串，详见 [夏令时使用指南](./02-dst.md)。

## 一周起始日

### 设置当前周起始日

设置当前连接的一周起始日：

```sql
SET FIRST_DAY_OF_WEEK 0;  -- 周日起始
SET FIRST_DAY_OF_WEEK 1;  -- 周一起始
```

**说明**：客户端配置参数 `firstDayOfWeek` 的默认值为 `4`（周四），见下文配置表。上述 SQL 仅设置当前连接。

取值范围为 `0`–`6`：`0`=周日，`1`=周一，…，`6`=周六。

### 查询当前周起始日

```sql
SELECT FIRST_DAY_OF_WEEK();
```

返回当前连接生效的周起始日设置，结果为 `0..6` 的整数，其中 `0=周日`，`1=周一`，...，`6=周六`。

### 配置文件设置

在客户端侧 `taos.cfg` 中配置：

```text
firstDayOfWeek 4
```

也可通过 `ALTER LOCAL 'firstDayOfWeek' '<0..6>'` 在当前客户端进程内动态修改。该配置只影响修改后的新连接，已建立连接保持各自创建时的值。

默认值为 `4`（周四），与历史按 Unix epoch 取模的周对齐行为兼容。若客户端未显式配置，启动时会尝试从操作系统读取一周起始日；读取失败时回退到 `4`。

若希望按周统计从周一开始，可执行：

```sql
SET FIRST_DAY_OF_WEEK 1;
```

若希望从周日开始，设置为 `0`。

### 影响范围

`firstDayOfWeek` 影响所有以 `w`（周）为单位的操作：

- `TIMETRUNCATE(ts, 1w)` 的对齐日
- `INTERVAL(1w)` 的窗口起始日
- 流里 `PERIOD(1w)`、`SLIDING(1w)`、`INTERVAL(1w)` 的周边界（以该流定义中保存的一周起始日为准）

## 时间函数

### TO_ISO8601

```sql
SELECT TO_ISO8601(ts) FROM t;                        -- 使用连接时区
SELECT TO_ISO8601(ts, '+09:00') FROM t;              -- 指定固定偏移（ISO 8601 符号）
SELECT TO_ISO8601(ts, 'UTC+09:00') FROM t;           -- 等价写法，'UTC' 前缀会被剥离
SELECT TO_ISO8601(ts, 'America/New_York') FROM t;    -- 指定 IANA 时区
```

**符号约定**：`TO_ISO8601` 是唯一使用 ISO 8601 符号约定的入口——`local = UTC + offset`，即 `'+08:00'` 表示东八区（北京时间）。以下写法完全等价：`'+0800'`、`'+08:00'`、`'UTC+8'`、`'UTC+0800'`、`'UTC+08:00'`。其余入口（`SET TIMEZONE`、`taos.cfg`、`TO_CHAR`、`TIMETRUNCATE` 等）均使用 POSIX 符号约定（`+` = 西区）。

使用 IANA 时区时，输出的偏移量随时刻的夏令时状态自动变化：

```sql
SET TIMEZONE 'America/New_York'; 
SELECT TO_ISO8601('2026-01-15 12:00:00');  -- ...T12:00:00-05:00 (EST, 冬令时)
SELECT TO_ISO8601('2026-07-15 12:00:00');  -- ...T12:00:00-04:00 (EDT, 夏令时)
```

### TIMETRUNCATE

将时间戳截断到指定单位边界。

```sql
SELECT TIMETRUNCATE(ts, 1d) FROM t;                          -- 截断到当天 00:00:00
SELECT TIMETRUNCATE(ts, 1w) FROM t;                          -- 截断到一周起始日 00:00:00
SELECT TIMETRUNCATE(ts, 1n) FROM t;                          -- 截断到当月 1 日
SELECT TIMETRUNCATE(ts, 1q) FROM t;                          -- 截断到当季首月 1 日
SELECT TIMETRUNCATE(ts, 1y) FROM t;                          -- 截断到当年 1 月 1 日
SELECT TIMETRUNCATE(ts, 1d, 'America/New_York') FROM t;      -- 指定时区
```

**支持的自然时间单位**

| 单位 | 含义 | 截断规则 |
| --- | --- | --- |
| `d` | 天 | 对齐到当天 00:00:00 |
| `w` | 周 | 对齐到一周起始日（由 `firstDayOfWeek` 决定）00:00:00 |
| `n` | 月 | 对齐到当月 1 日 00:00:00 |
| `q` | 季度 | 对齐到当季首月 1 日 00:00:00（Q1=1 月，Q2=4 月，Q3=7 月，Q4=10 月） |
| `y` | 年 | 对齐到当年 1 月 1 日 00:00:00 |

**示例**

```sql
SELECT TIMETRUNCATE('2026-03-15', 1n);   -- 2026-03-01 00:00:00
SELECT TIMETRUNCATE('2026-05-15', 1q);   -- 2026-04-01 00:00:00
SELECT TIMETRUNCATE('2026-08-15', 1y);   -- 2026-01-01 00:00:00
```

**第三参数**（时区）：

| 值 | 行为 |
| --- | --- |
| `0` | 使用 UTC（旧语义） |
| `1` | 使用连接时区（旧语义） |
| `'Asia/Shanghai'` | 使用指定 IANA 时区 |
| `'+08:00'` | 使用指定固定偏移 |
| 省略 | 使用连接时区 |

### TIMEZONE()

```sql
SELECT TIMEZONE();
```

返回连接级时区的名称、当前缩写和偏移，格式为 `名称 (缩写, ±HHMM)`。未显式设置时，使用连接创建或重置时保存的客户端默认时区。

若需确认 `SET TIMEZONE` 是否生效，可执行：

```sql
SELECT TIMEZONE();
SELECT TO_ISO8601(NOW());
```

例如：

- 执行 `SET TIMEZONE 'Asia/Shanghai'` 后，`TIMEZONE()` 返回 `Asia/Shanghai (CST, +0800)`。
- 执行 `ALTER LOCAL 'timezone' 'Asia/Shanghai'` 后，当前连接和新建连接使用上海时区，其他已有连接保持原值。
- 使用 `America/New_York` 的同一连接，冬季返回 `EST, -0500`，夏季返回 `EDT, -0400`；历史时间戳的格式化仍按该时间戳对应的规则计算。

## INTERVAL 查询

`INTERVAL` 支持按自然时间单位切分窗口：

```sql
SELECT _wstart, COUNT(*) FROM meters
  INTERVAL(1n)                      -- 按月切分
  FILL(PREV);

SELECT _wstart, AVG(voltage) FROM meters
  INTERVAL(1q)                      -- 按季度切分
  FILL(NULL);

SELECT _wstart, SUM(energy) FROM meters
  INTERVAL(1w)                      -- 按周切分（尊重 firstDayOfWeek）
  FILL(LINEAR);
```

**支持的自然时间单位**

| 单位 | 窗口边界 |
| --- | --- |
| `d` | 本地时区每天 00:00:00 |
| `w` | 本地时区一周起始日 00:00:00（由 `firstDayOfWeek` 决定） |
| `n` | 本地时区每月 1 日 00:00:00 |
| `q` | 本地时区每季度首月 1 日 00:00:00 |
| `y` | 本地时区每年 1 月 1 日 00:00:00 |

**多倍数窗口**

```sql
INTERVAL(2q)   -- 半年窗口：[1 月，7 月), [7 月，次年 1 月)
INTERVAL(3n)   -- 季度窗口（等价 1q）：1/4/7/10 月
INTERVAL(2w)   -- 双周窗口
```

**夏令时处理**：窗口始终按本地挂钟时间对齐。夏令时切换当天，窗口的实际时长会变化（例如春季拨快当天的 `1d` 窗口可能只有 23 小时），这是预期行为。写入和查询在夏令时跳变与重叠区间的注意点，详见 [夏令时使用指南](./02-dst.md)。

**闰年/变长月**：窗口宽度自动适应实际天数（如 2 月窗口 28 或 29 天）。`FILL` 填充边界逐月/逐季推进。

## 流式计算时间配置

`CREATE STREAM` 会把时区和一周起始日写入流定义。之后日、周、月、季、年等自然时间边界都按这两项计算。停止后再启动、进程重启、故障切换、导出后再恢复，都继续使用创建时保存的值。修改当前连接、客户端或服务端的时区，不会改变已经创建的流。查询语句以及 `SET TIMEZONE`、`SET FIRST_DAY_OF_WEEK` 的既有行为不变。`CREATE TSMA` 不使用本节的配置方式。

建流语法见 [建流语法](../../07-stream-processing/01-syntax.md)。

### 为流指定时区和一周起始日

只能在 `STREAM_OPTIONS` 里用 `CONFIG` 指定，没有单独的流 `TIMEZONE` 子句，也不支持用 `ALTER STREAM` 修改。如需变更，请删除后重新创建。

```sql
STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1))
```

| 配置项 | 取值 | 未写出时 |
| --- | --- | --- |
| `timezone` | 字符串，格式与 `SET TIMEZONE` 相同 | 使用当前连接已经生效的时区 |
| `firstDayOfWeek` | 整数 `0`–`6`（`0` 为周日，`1` 为周一） | 使用当前连接已经生效的一周起始日 |

键名大小写不敏感，但只能写 `timezone` 和 `firstDayOfWeek`，不能写 `tz`、`first_day_of_week` 等别名。`CONFIG()` 不能为空。`timezone` 必须是字符串，`firstDayOfWeek` 必须是整数。

一条语句可以写多个 `CONFIG`，也可以在同一个块里重复同一键。同一键多次出现时，以最后一次为准。只有最终生效的值会做内容检查：前面写过无效时区、但后面被有效值覆盖时，创建仍可成功。未知键或值类型错误出现即失败，不能被后面的写法覆盖。

未写 `CONFIG`、或只写了其中一个键时，缺省项取创建该流时当前连接已经生效的值。系统仍会把完整的时区和一周起始日写入流，例如：

```sql
SET TIMEZONE 'Asia/Shanghai';
SET FIRST_DAY_OF_WEEK 1;

CREATE STREAM meters_monthly INTERVAL(1n) FROM meters
  INTO meters_monthly_out AS
  SELECT _twstart AS window_start, AVG(current) AS avg_current
  FROM %%trows;
```

上例创建后，流会保存 `timezone = 'Asia/Shanghai'` 和 `firstDayOfWeek = 1`。可用 `SHOW CREATE STREAM` 确认实际写入的 `CONFIG`。

固定偏移按 POSIX 符号保存。例如输入 `'+08:00'` 会保存为 `UTC+8`，表示本地时间等于 UTC 减 8 小时，不是北京时间。建议使用 `'Asia/Shanghai'` 这类 IANA 名称。

流计算语句里的 `TIMEZONE()` 和 `FIRST_DAY_OF_WEEK()` 使用流保存的值。如果时间函数自己带了时区参数，仍以函数参数为准。

### 触发参数可以使用的时间单位

`PERIOD`、不带 `INTERVAL` 的 `SLIDING`，以及 `INTERVAL`，都可以使用自然时间单位，但触发方式并不相同：

- `INTERVAL` 按事件时间划分窗口，窗口边界与查询语句中的 `INTERVAL` 一致。
- `PERIOD` 按系统时钟定时触发。
- 不带 `INTERVAL` 的 `SLIDING` 按事件时间滑动触发。
- 使用周单位 `w` 时，一周从哪一天开始，由该流保存的 `firstDayOfWeek` 决定。

因此，即使都写成 `1n` 或 `1w`，`PERIOD`、`SLIDING` 和 `INTERVAL` 的触发时刻也不一定相同。

`q`（季）按 3 个月处理，例如 `2q` 等于 `6n`。

| 写法 | 支持的时间单位 |
| --- | --- |
| `PERIOD(period_time)` | `a`/`s`/`m`/`h`/`d`/`w`/`n`/`q`/`y` |
| `SLIDING(sliding_val)`（不带 `INTERVAL`） | `a`/`s`/`m`/`h`/`d`/`w`/`n`/`q`/`y` |
| `INTERVAL(interval_val)` | `a`/`s`/`m`/`h`/`d`/`w`/`n`/`q`/`y`；可以不写 `SLIDING` |
| `INTERVAL(...) SLIDING(sliding_val)` | `a`/`s`/`m`/`h`/`d`/`w`，不支持 `n`/`q`/`y` |

不写 `SLIDING` 时，窗口步长等于窗口宽度，相邻窗口不重叠。例如 `INTERVAL(1n)` 表示按月划分，每月一个窗口。

写出 `SLIDING` 时，滑动步长只支持 `a`/`s`/`m`/`h`/`d`/`w`。因此 `INTERVAL(1n) SLIDING(1n)` 不合法；需要按自然月划分、窗口互不重叠时，写成 `INTERVAL(1n)`。流里显式写出的滑动步长只需大于 0，可以大于窗口宽度（窗口之间会有间隙），也不要求达到窗口宽度的 1%。这与查询语句中的 `INTERVAL ... SLIDING` 不同。

**示例**

```sql
-- 每季度按东京当地时间触发
CREATE STREAM meters_quarter_tokyo PERIOD(1q)
  STREAM_OPTIONS(CONFIG(timezone = 'Asia/Tokyo', firstDayOfWeek = 1))
  INTO meters_quarter_tokyo_out AS
  SELECT CAST(_tlocaltime / 1000000 AS TIMESTAMP) AS ts,
         AVG(current) AS avg_current
  FROM meters;

-- 按纽约当地年边界滑动触发
CREATE STREAM meters_slide_year SLIDING(1y) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'America/New_York', firstDayOfWeek = 0))
  INTO meters_slide_year_out AS
  SELECT _tprev_ts AS window_start, AVG(current) AS avg_current
  FROM %%trows;

-- 按伦敦当地自然月划分窗口
CREATE STREAM meters_monthly INTERVAL(1n) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'Europe/London', firstDayOfWeek = 1))
  INTO meters_monthly_out AS
  SELECT _twstart AS window_start, AVG(current) AS avg_current
  FROM %%trows;

-- 每 2 秒计算前 1 秒，窗口之间会有空隙
CREATE STREAM meters_gapped INTERVAL(1s) SLIDING(2s) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1))
  INTO meters_gapped_out AS
  SELECT _twstart AS window_start, AVG(current) AS avg_current
  FROM %%trows;
```

### 偏移

`PERIOD`、不带 `INTERVAL` 的 `SLIDING` 和 `INTERVAL` 都可以带偏移，规则与查询语句中 `INTERVAL` 的偏移相同：

- 不支持 `AUTO`，不支持负值。
- 偏移必须严格小于对应的周期、滑动间隔或窗口宽度。
- `q` 先按 3 个月计算，再参与比较。
- 月与天等不同单位一起比较时，按每月 28 天估算。例如 `PERIOD(1n, 27d)` 合法，`PERIOD(1n, 28d)` 非法。

写出 `INTERVAL(...) SLIDING(...)` 时，`SLIDING` 只能写滑动步长，不能在 `SLIDING` 里再写偏移。需要移动窗口起点时，把偏移写在 `INTERVAL` 里，例如 `INTERVAL(1d, 1h) SLIDING(1d)`。

| 合法写法 | 说明 |
| --- | --- |
| `PERIOD(1n, 27d)`、`SLIDING(1n, 27d)`、`INTERVAL(1n, 27d)` | 27 天小于按 28 天估算的一个月 |
| `PERIOD(29d, 1n)`、`SLIDING(29d, 1n)`、`INTERVAL(29d, 1n)` | 一个月按 28 天估算，小于 29 天 |
| `PERIOD(1q, 83d)`、`SLIDING(1q, 83d)`、`INTERVAL(1q, 83d)` | `1q` 按 3 个月、84 天估算，83 天合法 |

例如 `firstDayOfWeek = 1` 时，`PERIOD(1w, 1d)` 以周一为一周起点，偏移一天后在周二触发。

### 夏令时与节点时区数据

使用 IANA 时区时，自然日、周、月等边界按当地挂钟时间对齐。夏令时切换当天，`1d` 窗口可能是 23 小时或 25 小时，这是预期行为。

流只保存时区名称，不保存创建当时的时区规则。因此：

- 可能运行该流的节点都需要能识别该时区，并尽量使用同一套时区数据。
- 更新节点时区数据后，后续触发可能按新规则计算。固定 UTC 偏移不受 IANA 规则更新影响。
- 完整的 IANA 与夏令时行为在 Linux 和 macOS 上可用；Windows 不因本功能获得同等能力。

如果某个节点无法加载流保存的时区，该流会进入失败状态，不会改用服务端时区、操作系统时区或 UTC。补齐节点时区数据后，等待流按已有机制重新部署即可。不要通过修改全局时区试图让它改用别的时区继续运行。

### 升级前已有流、TSMA 与版本要求

`v3.4.3` 之前创建、没有把时区写入流定义的流，升级后不会自动补写 `CONFIG`：

- `information_schema.ins_streams` 中的 `timezone` 和 `first_day_of_week` 为 `NULL`。
- `SHOW CREATE STREAM` 和导出结果仍是原来的 SQL，不会插入 `CONFIG`。
- 运行时仍可能随当时环境变化。

若要把这类流改成创建时固定时区，需要删除后重新执行 `CREATE STREAM`。

`CREATE TSMA` 及其内部流不使用 `CONFIG`。系统表中的两个时间字段也可能是 `NULL`，但 SQL 是 `CREATE TSMA`，不要把它们当成升级前遗留的用户流。

升级时需要客户端和集群一起离线升级，不能新旧版本混跑。已经用新版本创建了带时间配置的流之后，不能直接降级继续使用这份数据；回滚需要恢复升级前的备份。

### 查看流的时区和一周起始日

```sql
SHOW CREATE STREAM db_name.stream_name;

SELECT stream_name, timezone, first_day_of_week, sql
FROM information_schema.ins_streams;
```

- 显示保存后的时区、`0`–`6` 的一周起始日，以及包含完整 `CONFIG` 的 `CREATE STREAM`。
- 升级前已有的用户流：两个时间字段为 `NULL`，SQL 中没有 `CONFIG`。
- `CREATE TSMA` 对象：时间字段也可以为 `NULL`，但 SQL 是 `CREATE TSMA`。

## 各场景时区来源速查

| 场景 | 时区来源 | 说明 |
| --- | --- | --- |
| 写入 `INSERT` | 连接 → 服务端全局 → OS | 将时间字符串转为 UTC |
| 读取 `SELECT ts` | 连接 → 客户端全局 → OS | 将 UTC 格式化为本地时间 |
| 函数（`TO_ISO8601` 等） | SQL 参数 → 连接 → 服务端全局 → OS | 可指定固定偏移或 IANA 时区 |
| `TIMETRUNCATE` | SQL 参数 → 连接 → 服务端全局 → OS | 支持 `d`/`w`/`n`/`q`/`y`，可指定时区 |
| `INTERVAL` 查询窗口 | 连接 → 服务端全局 → OS | 支持 `d`/`w`/`n`/`q`/`y` |
| `SHOW` / `EXPLAIN` | 连接 → 客户端全局 → OS | 按当前连接的时区展示 |
| 流式计算触发与计算 | 流 `CONFIG`；未写出的项取创建时当前连接的值 | 创建后按流定义中保存的值运行；未写入 `CONFIG` 的流仍可能随环境变化 |

## 配置参数一览

| 参数 | 配置文件 | 类型 | 默认值 | 说明 |
| ---------------- | --- | --- | --- | --- |
| `timezone`       | 服务端/客户端侧 `taos.cfg` | 字符串 | OS 检测 | 全局时区 |
| `firstDayOfWeek` | 客户端侧 `taos.cfg` | 整数 0-6 | 4（周四） | 一周起始日；也可用 `ALTER LOCAL` 动态修改 |

## 错误信息

| 错误场景 | 错误信息 |
| --- | --- |
| 无效或不受支持的时区字符串 | `[0x26B2] Invalid or unsupported timezone: '<value>'` |
| firstDayOfWeek 超出范围 | `[0x26B3] Invalid firstDayOfWeek: <value>, must be 0-6` |

## 版本支持矩阵

下表列出功能的引入版本；Windows 平台的连接级时区、历史和 DST 能力从 `v3.5.0` 起支持，详见 [Windows 支持](#windows-支持)。

| 功能                                       | `v3.4.2` 之前 | `v3.4.2` | `v3.4.3` | `v3.5.0` |
| ------------------------------------------ | :----------: | :-------: | :--------: | :--------: |
| `timezone` 配置文件（服务端/客户端）         |      ✅      |    ✅     |    ✅     |    ✅     |
| `TO_ISO8601` 固定偏移参数                    |      ✅      |    ✅     |    ✅     |    ✅     |
| `TIMETRUNCATE` `d`/`w` 截断                  |      ✅      |    ✅     |    ✅     |    ✅     |
| `INTERVAL` 查询 `d` 窗口                      |      ✅      |    ✅     |    ✅     |    ✅     |
| `TIMEZONE()` 函数                            |      ✅      |  ✅（增强）   |    ✅     |    ✅     |
| PERIOD 触发 `a`/`s`/`m`/`h`/`d`/`w`/`n`/`y`  |      ✅      |    ✅     |    ✅     |    ✅     |
| SLIDING 触发 `a`/`s`/`m`/`h`/`d`/`w`         |      ✅      |    ✅     |    ✅     |    ✅     |
| INTERVAL 窗口触发 `a`/`s`/`m`/`h`            |      ✅      |    ✅     |    ✅     |    ✅     |
| `SET TIMEZONE`                             |      ❌      |    ✅     |    ✅     |    ✅     |
| `SET FIRST_DAY_OF_WEEK`                    |      ❌      |    ✅     |    ✅     |    ✅     |
| `firstDayOfWeek` 配置参数                   |      ❌      |    ✅     |    ✅     |    ✅     |
| `TO_ISO8601` IANA 时区参数                  |      ❌      |    ✅     |    ✅     |    ✅     |
| `TIMETRUNCATE` 时区字符串参数                |      ❌      |    ✅     |    ✅     |    ✅     |
| `TIMETRUNCATE` `n`/`q`/`y` 截断              |      ❌      |    ✅     |    ✅     |    ✅     |
| `INTERVAL` 查询 `w`/`n`/`q`/`y` 窗口         |      ❌      |    ✅     |    ✅     |    ✅     |
| 普通列读取使用连接时区                        |      ❌      |    ✅     |    ✅     |    ✅     |
| SHOW/EXPLAIN 使用连接时区                     |      ❌      |    ✅     |    ✅     |    ✅     |
| 流任务 `STREAM_OPTIONS(CONFIG(...))`         |      ❌      |    ❌     |    ✅     |    ✅     |
| 流任务时区 / 一周起始日写入流定义              |      ❌      |    ❌     |    ✅     |    ✅     |
| PERIOD 触发 `q` 季度                          |      ❌      |    ❌     |    ✅     |    ✅     |
| SLIDING 触发 `n`/`q`/`y`                     |      ❌      |    ❌     |    ✅     |    ✅     |
| INTERVAL 窗口触发 `d`/`w`/`n`/`q`/`y`        |      ❌      |    ❌     |    ✅     |    ✅     |
| `ins_streams` timezone/first_day_of_week 列  |      ❌      |    ❌     |    ✅     |    ✅     |
| Windows 连接级时区及 IANA 历史 / DST 规则 |      ❌      |    ❌     |    ❌     |    ✅     |
