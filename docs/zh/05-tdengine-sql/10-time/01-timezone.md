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

TDengine 内部以 UTC 时间戳（int64）存储所有时间数据。时区仅在**时间字符串与 UTC 之间的转换**时起作用：写入时将本地时间字符串转为 UTC，读取时将 UTC 转为本地时间字符串展示。

### 支持的时区格式

#### IANA 名称

推荐使用。示例：`'Asia/Shanghai'`、`'America/New_York'`。

- **夏令时感知**：是。系统自动处理 DST 跳变与切换。

#### POSIX 固定偏移

示例：`'+08:00'`、`'-0500'`、`'Z'`、`'+10'`、`'UTC+08:00'`。

- **夏令时感知**：否。全年使用恒定偏移。
- **支持范围**：`-14:00` 至 `+14:00`。
- **符号约定**：`SET TIMEZONE`、`ALTER LOCAL`、`taos.cfg`、`TO_CHAR`、`TIMETRUNCATE` 均遵循 POSIX 符号约定（`+` = UTC 以西，即 `local = UTC − offset`）。**例外**：`TO_ISO8601` 的固定偏移参数使用 [ISO 8601 符号约定](#to_iso8601)。详见下文。

#### POSIX 固定偏移格式详解

TDengine 的固定偏移时区格式遵循**POSIX `TZ` 环境变量规范**的子集。完整的 POSIX 规范定义了如下格式：

```text
STD offset [ DST [ dstoffset ] [ , rule ] ]
```

其中 `STD` 是标准时间缩写，`offset` 是与 UTC 的偏移量，`DST` 和 `dstoffset` 用于夏令时定义，`rule` 用于夏令时切换规则。

**TDengine 支持的子集**：TDengine 仅支持该规范中 `STD offset` 部分，且 `STD` 只接受 `UTC` 一个值。不支持手动配置 `DST`、`dstoffset` 和 `rule`。如需夏令时支持，请使用 IANA 时区名称。TDengine 接受的固定偏移语法如下：

```text
UTC offset
```

**`offset` 字段格式**：`offset` 表示与 UTC 的偏移量，格式为 `[+|-] hh [ : mm [ : ss ] ]`，其中 `hh` 可以是一位或两位数字，`mm` 和 `ss`（如使用）必须是两位数字。也可写为无冒号分隔的紧凑形式（如 `+0800`、`-0530`）。特殊值 `Z` 等价于 `+00:00`。

**POSIX 符号约定**：POSIX 标准定义本地时间与 UTC 的关系为：

```text
local_time = UTC - offset
```

因此：正号 `+` 表示 UTC **以西**（西时区，本地时间比 UTC 慢），负号 `-` 表示 UTC **以东**（东时区，本地时间比 UTC 快）。这与 ISO 8601 的符号约定**相反**。例如：

| 写法 | POSIX 含义 | 等效 IANA 时区 |
| --- | --- | --- |
| `'+08:00'` 或 `'UTC+08:00'` | UTC 以西 8 小时 | 接近 `Pacific/Pitcairn`（太平洋） |
| `'-08:00'` 或 `'UTC-08:00'` | UTC 以东 8 小时 | 接近 `Asia/Shanghai`（北京时间） |
| `'+05:30'` 或 `'UTC+05:30'` | UTC 以西 5.5 小时 | 接近 `America/Bogota`（哥伦比亚） |
| `'-05:30'` 或 `'UTC-05:30'` | UTC 以东 5.5 小时 | 接近 `Asia/Kolkata`（印度） |
| `'Z'` | UTC 本身 | `Etc/UTC` |

当省略 `UTC` 前缀时（如 `'+08:00'`），TDengine 仍按 POSIX 规则解析偏移量，行为与带 `UTC` 前缀完全一致。

**支持范围**：偏移量的有效范围为 `-14:00` 至 `+14:00`（对应 UTC 以东 14 小时到 UTC 以西 14 小时）。

**与 IANA 时区的区别**：POSIX 固定偏移不包含任何夏令时信息，全年使用恒定偏移。如果目标地区存在夏令时（如美国、欧洲），应使用 IANA 名称以获得正确的 DST 自动切换。

### 时区优先级

TDengine 采用五层时区优先级体系，高层覆盖低层：

| 优先级 | 名称 | 设定方式 | 说明 |
| --- | --- | --- | --- |
| 最高 | SQL 级 | 函数时区参数（如 `TO_ISO8601(ts, '+09:00')`）；`TO_ISO8601` IANA 参数 `v3.4.2`；流任务 `TIMEZONE` 子句 `v3.4.3` | 仅影响本条 SQL 或本流任务 |
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
timezone +08:00
```

支持 IANA 名称、Windows 标准时区名称（如 `China Standard Time`）以及固定偏移格式 `Z`、`±HH`、`±HHMM`、`±HH:MM`、`UTC±H[:MM]`、`UTC±HH[:MM]`。`GMT` / `GMT±...` 不支持。未配置时使用操作系统检测的时区。

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

## 流式计算时区

### 流任务 TIMEZONE 子句（自 `v3.4.3`）

`v3.4.3` 之前，流式计算触发侧自然时间边界对齐始终使用服务端全局时区，无法为单个流任务指定独立时区。`v3.4.3` 起新增 `TIMEZONE` 子句，为流任务指定独立时区，适用于**所有触发类型**：

```sql
-- PERIOD 触发：东京时区每周触发
CREATE STREAM weekly_tokyo TRIGGER PERIOD(1w) TIMEZONE 'Asia/Tokyo'
  INTO tokyo_weekly AS SELECT AVG(current) FROM meters;

-- SLIDING 触发：纽约时区每季度滑动
CREATE STREAM slide_ny TRIGGER SLIDING(1q) TIMEZONE 'America/New_York'
  FROM meters
  INTO ny_quarterly AS SELECT _tprev_ts, _tcurrent_ts, AVG(current) FROM %%trows;

-- INTERVAL 触发：伦敦时区月窗口
CREATE STREAM monthly_uk TRIGGER INTERVAL(1n) SLIDING(1w) TIMEZONE 'Europe/London'
  FROM meters
  INTO uk_monthly AS SELECT _wstart, _wend, AVG(current) FROM %%trows;

-- EVENT 触发：计算侧使用东京时区
CREATE STREAM event_tokyo TRIGGER EVENT_WINDOW(START WITH voltage > 220 END WITH voltage <= 220)
  TIMEZONE 'Asia/Tokyo'
  FROM meters PARTITION BY tbname
  INTO event_out AS SELECT _twstart, _twend, AVG(current) FROM %%trows;
```

**固化行为**：`TIMEZONE` 在创建时固化到流元数据。后续修改全局时区不影响已有流任务。

**未指定 TIMEZONE 时**：按连接时区 → 服务端全局时区 → OS 时区的顺序解析后固化。

### 流式计算时区的影响

| 影响位置 | 说明 |
| --- | --- |
| 触发侧（PERIOD/SLIDING/INTERVAL） | 自然单位（d/w/n/q/y）的日历边界对齐使用固化时区 |
| 计算侧（AS subquery） | INTERVAL 自然单位窗口切分使用固化时区和 firstDayOfWeek |

### 流触发自然单位支持

以下表格列出 PERIOD、SLIDING、INTERVAL 三种触发类型支持的时间单位及其版本：

**PERIOD 触发**

| 单位 | 含义 | 版本 |
| --- | --- | --- |
| `a` | 毫秒 | 已支持 |
| `s` | 秒 | 已支持 |
| `m` | 分钟 | 已支持 |
| `h` | 小时 | 已支持 |
| `d` | 天 | 已支持 |
| `w` | 周 | 已支持 |
| `n` | 月 | 已支持 |
| `y` | 年 | 已支持 |
| `q` | 季度 | `v3.4.3` |

**offset 示例**

```sql
PERIOD(1w, 1d)       -- 每周二 00:00:00 触发
PERIOD(1n, 14d)      -- 每月 15 日 00:00:00 触发
PERIOD(1y, 31d)      -- 每年 2 月 1 日 00:00:00 触发
PERIOD(1q)           -- 每季度首月 1 日 00:00:00 触发（自 `v3.4.3`）
PERIOD(1q, 15d)      -- 每季度第 16 日触发（自 `v3.4.3`）
```

**SLIDING 触发**

| 单位 | 含义 | 版本 |
| --- | --- | --- |
| `a` | 毫秒 | 已支持 |
| `s` | 秒 | 已支持 |
| `m` | 分钟 | 已支持 |
| `h` | 小时 | 已支持 |
| `d` | 天 | 已支持 |
| `w` | 周 | 已支持 |
| `n` | 月 | `v3.4.3` |
| `q` | 季度 | `v3.4.3` |
| `y` | 年 | `v3.4.3` |

```sql
SLIDING(1n)          -- 每月滑动触发（自 `v3.4.3`）
SLIDING(1q)          -- 每季度滑动触发（自 `v3.4.3`）
SLIDING(1y)          -- 每年滑动触发（自 `v3.4.3`）
SLIDING(1q, 15d)     -- 每季度第 16 日滑动触发（自 `v3.4.3`）
```

**INTERVAL 窗口触发**（interval_val 和 sliding_val 均适用）：

| 单位 | 含义 | 版本 |
| --- | --- | --- |
| `a` | 毫秒 | 已支持 |
| `s` | 秒 | 已支持 |
| `m` | 分钟 | 已支持 |
| `h` | 小时 | 已支持 |
| `d` | 天 | `v3.4.3` |
| `w` | 周 | `v3.4.3` |
| `n` | 月 | `v3.4.3` |
| `q` | 季度 | `v3.4.3` |
| `y` | 年 | `v3.4.3` |

```sql
INTERVAL(1n) SLIDING(1w)    -- 月窗口，每周滑动（自 `v3.4.3`）
INTERVAL(1q) SLIDING(1n)    -- 季度窗口，每月滑动（自 `v3.4.3`）
INTERVAL(1y) SLIDING(1q)    -- 年窗口，每季度滑动（自 `v3.4.3`）
INTERVAL(1w) SLIDING(1d)    -- 周窗口，每天滑动（自 `v3.4.3`）
```

### 查看流任务时区（自 `v3.4.3`）

```sql
SELECT stream_name, timezone, first_day_of_week FROM information_schema.ins_streams;
```

## 各场景时区来源速查

| 场景 | 时区来源 | 版本说明 |
| --- | --- | --- |
| 写入 `INSERT`         | 连接 → 服务端全局 → OS | 将时间字符串转为 UTC，已支持 |
| 读取 `SELECT ts`      | 连接 → 客户端全局 → OS | 将 UTC 格式化为本地时间；连接级回退为 `v3.4.2` 起支持（此前仅用 OS 时区） |
| 函数（`TO_ISO8601` 等）| SQL 参数 → 连接 → 服务端全局 → OS | 固定偏移参数已支持；IANA 参数为 `v3.4.2` |
| `TIMETRUNCATE`        | SQL 参数 → 连接 → 服务端全局 → OS | `d`/`w` 已支持；`n`/`q`/`y` 为 `v3.4.2`；时区字符串参数为 `v3.4.2` |
| `INTERVAL` 查询窗口    | 连接 → 服务端全局 → OS | `d` 已支持；`w`/`n`/`q`/`y` 为 `v3.4.2` |
| `SHOW` / `EXPLAIN`    | 连接 → 客户端全局 → OS | 连接级回退为 `v3.4.2` 起支持（此前仅用 OS 时区） |
| 流式计算触发与计算      | 服务端全局 → OS；`v3.4.3` 起支持 `TIMEZONE` 子句 → 连接 → 服务端全局 → OS（创建时固化） | `v3.4.3` 前使用服务端时区；`v3.4.3` 起支持固化 |

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
| 流任务 `TIMEZONE` 子句                       |      ❌      |    ❌     |    ✅     |
| 流任务时区/firstDayOfWeek 固化                |      ❌      |    ❌     |    ✅     |
| PERIOD 触发 `q` 季度                          |      ❌      |    ❌     |    ✅     |
| SLIDING 触发 `n`/`q`/`y`                     |      ❌      |    ❌     |    ✅     |
| INTERVAL 窗口触发 `d`/`w`/`n`/`q`/`y`        |      ❌      |    ❌     |    ✅     |
| `ins_streams` timezone/first_day_of_week 列  |      ❌      |    ❌     |    ✅     |
