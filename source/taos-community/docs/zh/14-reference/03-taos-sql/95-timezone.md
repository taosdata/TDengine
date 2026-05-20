---
sidebar_label: 时区与自然时间单位
title: 时区与自然时间单位
description: TDengine 时区语义与自然时间单位使用说明
toc_max_heading_level: 4
---

本文档描述 TDengine 时区语义与自然时间单位的完整状态。功能分版本交付，文中以版本号标注：

| 标记 | 含义 |
| --- | --- |
| （无标记） | v3.4.1 已支持 |
| **[v3.4.2]** | v3.4.2 起支持 |
| **[v3.4.3]** | v3.4.3 起支持（v3.4.2 中尚不可用） |

---

## 时区概述

TDengine 内部以 UTC 时间戳（int64）存储所有时间数据。时区仅在**时间字符串与 UTC 之间的转换**时起作用：写入时将本地时间字符串转为 UTC，读取时将 UTC 转为本地时间字符串展示。

### 支持的时区格式

| 格式 | 示例 | 夏令时感知 |
| --- | --- | --- |
| IANA 名称 | `'Asia/Shanghai'`、`'America/New_York'` | ✅ 是（自动处理 DST 跳变） |
| 固定偏移 | `'+08:00'`、`'-05:00'`、`'Z'` | ❌ 否（恒定偏移） |

**建议**：涉及夏令时的地区使用 IANA 名称，系统自动处理 DST 切换。

### 时区优先级

TDengine 采用五层时区优先级体系，高层覆盖低层：

| 优先级 | 名称 | 设定方式 | 说明 |
| --- | --- | --- | --- |
| 最高 | SQL 级 | 函数时区参数（如 `TO_ISO8601(ts, '+09:00')`）；`TO_ISO8601` IANA 参数 **[v3.4.2]**；流任务 `TIMEZONE` 子句 **[v3.4.3]** | 仅影响本条 SQL 或本流任务 |
| 高 | 连接级 | C API `taos_options_connection`；`SET TIMEZONE` **[v3.4.2]** | 影响当前连接的所有 SQL |
| 中 | 客户端全局 | 客户端侧 `taos.cfg` 中 `timezone` | 仅影响客户端本地时间展示 |
| 低 | 服务端全局 | 服务端侧 `taos.cfg` 中 `timezone` | 连接未设时区时服务端计算的回退 |
| 最低 | 系统默认 | 操作系统自动检测 | 最终兜底 |

**重要**：客户端全局时区仅影响客户端本地展示（如 `SELECT ts` 的输出格式化），不影响服务端计算。未通过连接级设置时区的连接，服务端计算回退到服务端全局时区。

## 设置时区

### SET TIMEZONE [v3.4.2]

设置当前连接的时区：

```sql
SET TIMEZONE 'Asia/Shanghai';
SET TIMEZONE '+08:00';
SET TIMEZONE 'America/New_York';
```

设置后，该连接上所有读写操作和服务端计算均使用此时区。

也可通过 C API `taos_options_connection` 在建立连接时设置时区，效果等同于 `SET TIMEZONE`。

### 查询当前时区

```sql
SELECT TIMEZONE();
```

返回当前连接生效的时区字符串。**[v3.4.2]** 将同时返回连接级、客户端、服务端时区。

### 配置文件设置

在 `taos.cfg` 中配置全局时区：

```text
timezone Asia/Shanghai
timezone UTC-8
timezone GMT-8
```

支持 IANA 名称和 POSIX 偏移（`UTC±N`/`GMT±N`）两种格式。Windows 下不支持 `UTC-8` 写法，须使用 IANA 名称。未配置时使用操作系统检测的时区。

- **服务端侧** `taos.cfg`：连接未通过 `SET TIMEZONE` 设置时区时，服务端计算回退到此值。
- **客户端侧** `taos.cfg`：仅影响客户端本地时间展示（如 `SELECT ts` 的输出格式化），不影响服务端计算。

**注意**：配置文件不支持 `+08:00` 裸偏移格式（该格式仅限 `SET TIMEZONE` 和函数参数使用）。

## 一周起始日

### SET FIRST_DAY_OF_WEEK [v3.4.2]

设置当前连接的一周起始日：

```sql
SET FIRST_DAY_OF_WEEK 0;  -- 周日起始
SET FIRST_DAY_OF_WEEK 1;  -- 周一起始（默认）
```

取值范围 0-6:0=周日，1=周一，..., 6=周六。

### 配置文件设置 [v3.4.2]

在服务端侧 `taos.cfg` 中配置：

```text
firstDayOfWeek 1
```

默认值为 1（周一，遵循 ISO 8601）。仅提供服务端配置，不提供客户端配置。

### 影响范围 [v3.4.2]

`firstDayOfWeek` 影响所有以 `w`（周）为单位的操作：

- `TIMETRUNCATE(ts, 1w)` 的对齐日
- `INTERVAL(1w)` 的窗口起始日
- `PERIOD(1w)` 的触发日 **[v3.4.3]**
- `SLIDING(1w)` 的触发日 **[v3.4.3]**

## 时间函数

### TO_ISO8601

```sql
SELECT TO_ISO8601(ts) FROM t;                        -- 使用连接时区
SELECT TO_ISO8601(ts, '+09:00') FROM t;              -- 指定固定偏移
SELECT TO_ISO8601(ts, 'America/New_York') FROM t;    -- 指定 IANA 时区 [v3.4.2]
```

使用 IANA 时区时，输出的偏移量随时刻的夏令时状态自动变化：

```sql
SET TIMEZONE 'America/New_York';           -- [v3.4.2]
SELECT TO_ISO8601('2026-01-15 12:00:00');  -- ...T12:00:00-05:00 (EST, 冬令时)
SELECT TO_ISO8601('2026-07-15 12:00:00');  -- ...T12:00:00-04:00 (EDT, 夏令时)
```

### TIMETRUNCATE

将时间戳截断到指定单位边界。

```sql
SELECT TIMETRUNCATE(ts, 1d) FROM t;                          -- 截断到当天 00:00:00
SELECT TIMETRUNCATE(ts, 1w) FROM t;                          -- 截断到一周起始日 00:00:00
SELECT TIMETRUNCATE(ts, 1n) FROM t;                          -- 截断到当月 1 日 [v3.4.2]
SELECT TIMETRUNCATE(ts, 1q) FROM t;                          -- 截断到当季首月 1 日 [v3.4.2]
SELECT TIMETRUNCATE(ts, 1y) FROM t;                          -- 截断到当年 1 月 1 日 [v3.4.2]
SELECT TIMETRUNCATE(ts, 1d, 'America/New_York') FROM t;      -- 指定时区 [v3.4.2]
```

**支持的自然时间单位**：

| 单位 | 含义 | 截断规则 | 版本 |
| --- | --- | --- | --- |
| `d` | 天 | 对齐到当天 00:00:00 | 已支持 |
| `w` | 周 | 对齐到一周起始日（由 `firstDayOfWeek` 决定）00:00:00 | 已支持，v3.4.2 起尊重 firstDayOfWeek |
| `n` | 月 | 对齐到当月 1 日 00:00:00 | **v3.4.2** |
| `q` | 季度 | 对齐到当季首月 1 日 00:00:00（Q1=1 月，Q2=4 月，Q3=7 月，Q4=10 月） | **v3.4.2** |
| `y` | 年 | 对齐到当年 1 月 1 日 00:00:00 | **v3.4.2** |

**示例**：

```sql
SELECT TIMETRUNCATE('2026-03-15', 1n);   -- 2026-03-01 00:00:00 [v3.4.2]
SELECT TIMETRUNCATE('2026-05-15', 1q);   -- 2026-04-01 00:00:00 [v3.4.2]
SELECT TIMETRUNCATE('2026-08-15', 1y);   -- 2026-01-01 00:00:00 [v3.4.2]
```

**第三参数**（时区）：

| 值 | 行为 | 版本 |
| --- | --- | --- |
| `0` | 使用 UTC（旧语义） | 已支持 |
| `1` | 使用连接时区（旧语义） | 已支持 |
| `'Asia/Shanghai'` | 使用指定 IANA 时区 | **v3.4.2** |
| `'+08:00'` | 使用指定固定偏移 | **v3.4.2** |
| 省略 | 使用连接时区 | 已支持 |

### TIMEZONE()

```sql
SELECT TIMEZONE();
```

同时返回连接级、客户端、服务端三个时区，便于用户排查时区配置问题。**[v3.4.2]** 起增强为同时返回三个层级的时区信息：

- **连接级时区**：通过 `SET TIMEZONE` 或 C API 设置的值，未设置时为空
- **客户端时区**：客户端 `taos.cfg` 配置或系统检测值
- **服务端时区**：服务端 `taos.cfg` 配置或系统检测值

## INTERVAL 查询

`INTERVAL` 支持按自然时间单位切分窗口：

```sql
SELECT _wstart, COUNT(*) FROM meters
  INTERVAL(1n)                      -- 按月切分 [v3.4.2]
  FILL(PREV);

SELECT _wstart, AVG(voltage) FROM meters
  INTERVAL(1q)                      -- 按季度切分 [v3.4.2]
  FILL(NULL);

SELECT _wstart, SUM(energy) FROM meters
  INTERVAL(1w)                      -- 按周切分（尊重 firstDayOfWeek）[v3.4.2]
  FILL(LINEAR);
```

**支持的自然时间单位**：

| 单位 | 窗口边界 | 版本 |
| --- | --- | --- |
| `d` | 本地时区每天 00:00:00 | 已支持 |
| `w` | 本地时区一周起始日 00:00:00（由 `firstDayOfWeek` 决定） | **v3.4.2** |
| `n` | 本地时区每月 1 日 00:00:00 | **v3.4.2** |
| `q` | 本地时区每季度首月 1 日 00:00:00 | **v3.4.2** |
| `y` | 本地时区每年 1 月 1 日 00:00:00 | **v3.4.2** |

**多倍数窗口**：

```sql
INTERVAL(2q)   -- 半年窗口：[1 月，7 月), [7 月，次年 1 月) [v3.4.2]
INTERVAL(3n)   -- 季度窗口（等价 1q）：1/4/7/10 月 [v3.4.2]
INTERVAL(2w)   -- 双周窗口 [v3.4.2]
```

**夏令时处理**：窗口始终按本地挂钟时间对齐。DST 切换日窗口物理时长会变化（如春跳日 1d 窗口为 23 小时），这是正确行为。

**闰年/变长月**：窗口宽度自动适应实际天数（如 2 月窗口 28 或 29 天）。`FILL` 填充边界逐月/逐季推进。

## 流计算时区

### 流任务 TIMEZONE 子句 [v3.4.3]

v3.4.3 之前，流计算触发侧自然时间边界对齐始终使用服务端全局时区，无法为单个流任务指定独立时区。v3.4.3 起新增 `TIMEZONE` 子句，为流任务指定独立时区，适用于**所有触发类型**：

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

### 流计算时区的影响

| 影响位置 | 说明 |
| --- | --- |
| 触发侧（PERIOD/SLIDING/INTERVAL） | 自然单位（d/w/n/q/y）的日历边界对齐使用固化时区 |
| 计算侧（AS subquery） | INTERVAL 自然单位窗口切分使用固化时区和 firstDayOfWeek |

### 流触发自然单位支持

以下表格列出 PERIOD、SLIDING、INTERVAL 三种触发类型支持的时间单位及其版本：

**PERIOD 触发**：

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
| `q` | 季度 | **v3.4.3** |

**offset 示例**：

```sql
PERIOD(1w, 1d)       -- 每周二 00:00:00 触发
PERIOD(1n, 14d)      -- 每月 15 日 00:00:00 触发
PERIOD(1y, 31d)      -- 每年 2 月 1 日 00:00:00 触发
PERIOD(1q)           -- 每季度首月 1 日 00:00:00 触发 [v3.4.3]
PERIOD(1q, 15d)      -- 每季度第 16 日触发 [v3.4.3]
```

**SLIDING 触发**：

| 单位 | 含义 | 版本 |
| --- | --- | --- |
| `a` | 毫秒 | 已支持 |
| `s` | 秒 | 已支持 |
| `m` | 分钟 | 已支持 |
| `h` | 小时 | 已支持 |
| `d` | 天 | 已支持 |
| `w` | 周 | 已支持 |
| `n` | 月 | **v3.4.3** |
| `q` | 季度 | **v3.4.3** |
| `y` | 年 | **v3.4.3** |

```sql
SLIDING(1n)          -- 每月滑动触发 [v3.4.3]
SLIDING(1q)          -- 每季度滑动触发 [v3.4.3]
SLIDING(1y)          -- 每年滑动触发 [v3.4.3]
SLIDING(1q, 15d)     -- 每季度第 16 日滑动触发 [v3.4.3]
```

**INTERVAL 窗口触发**（interval_val 和 sliding_val 均适用）：

| 单位 | 含义 | 版本 |
| --- | --- | --- |
| `a` | 毫秒 | 已支持 |
| `s` | 秒 | 已支持 |
| `m` | 分钟 | 已支持 |
| `h` | 小时 | 已支持 |
| `d` | 天 | **v3.4.3** |
| `w` | 周 | **v3.4.3** |
| `n` | 月 | **v3.4.3** |
| `q` | 季度 | **v3.4.3** |
| `y` | 年 | **v3.4.3** |

```sql
INTERVAL(1n) SLIDING(1w)    -- 月窗口，每周滑动 [v3.4.3]
INTERVAL(1q) SLIDING(1n)    -- 季度窗口，每月滑动 [v3.4.3]
INTERVAL(1y) SLIDING(1q)    -- 年窗口，每季度滑动 [v3.4.3]
INTERVAL(1w) SLIDING(1d)    -- 周窗口，每天滑动 [v3.4.3]
```

### 查看流任务时区 [v3.4.3]

```sql
SELECT stream_name, timezone, first_day_of_week FROM information_schema.ins_streams;
```

## 各场景时区来源速查

| 场景 | 时区来源 | 版本说明 |
| --- | --- | --- |
| 写入 `INSERT` | 连接 → 服务端全局 → OS | 将时间字符串转为 UTC，已支持 |
| 读取 `SELECT ts` | 连接 → 客户端全局 → OS | 将 UTC 格式化为本地时间；连接级回退为 **v3.4.2** 起支持（此前仅用 OS 时区） |
| 函数（`TO_ISO8601` 等） | SQL 参数 → 连接 → 服务端全局 → OS | 固定偏移参数已支持；IANA 参数为 **v3.4.2** |
| `TIMETRUNCATE` | SQL 参数 → 连接 → 服务端全局 → OS | `d`/`w` 已支持；`n`/`q`/`y` 为 **v3.4.2**；时区字符串参数为 **v3.4.2** |
| `INTERVAL` 查询窗口 | 连接 → 服务端全局 → OS | `d` 已支持；`w`/`n`/`q`/`y` 为 **v3.4.2** |
| `SHOW` / `EXPLAIN` | 连接 → 客户端全局 → OS | 连接级回退为 **v3.4.2** 起支持（此前仅用 OS 时区） |
| 流计算触发与计算 | 服务端全局 → OS；**[v3.4.3]** 起支持 `TIMEZONE` 子句 → 连接 → 服务端全局 → OS（创建时固化） | v3.4.3 前使用服务端时区；v3.4.3 起支持固化 |

## 配置参数一览

| 参数 | 配置文件 | 类型 | 默认值 | 说明 | 版本 |
| --- | --- | --- | --- | --- | --- |
| `timezone` | 服务端/客户端侧 `taos.cfg` | 字符串 | OS 检测 | 全局时区 | 已支持 |
| `firstDayOfWeek` | 服务端侧 `taos.cfg` | 整数 0-6 | 1（周一） | 一周起始日 | **v3.4.2** |

## 错误信息

| 错误场景 | 错误信息 |
| --- | --- |
| 无效时区字符串 | `[0x2600] Invalid timezone: '<value>'` |
| firstDayOfWeek 超出范围 | `[0x2601] Invalid firstDayOfWeek: <value>, must be 0-6` |

## 版本支持矩阵

| 功能 | v3.4.2 之前 | v3.4.2 | v3.4.3 |
| --- | --- | --- | --- |
| `timezone` 配置文件（服务端/客户端） | ✅ | ✅ | ✅ |
| `TO_ISO8601` 固定偏移参数 | ✅ | ✅ | ✅ |
| `TIMETRUNCATE` `d`/`w` 截断 | ✅ | ✅ | ✅ |
| `INTERVAL` 查询 `d` 窗口 | ✅ | ✅ | ✅ |
| `TIMEZONE()` 函数 | ✅ | ✅（增强） | ✅ |
| PERIOD 触发 `a`/`s`/`m`/`h`/`d`/`w`/`n`/`y` | ✅ | ✅ | ✅ |
| SLIDING 触发 `a`/`s`/`m`/`h`/`d`/`w` | ✅ | ✅ | ✅ |
| INTERVAL 窗口触发 `a`/`s`/`m`/`h` | ✅ | ✅ | ✅ |
| `SET TIMEZONE` | ❌ | ✅ | ✅ |
| `SET FIRST_DAY_OF_WEEK` | ❌ | ✅ | ✅ |
| `firstDayOfWeek` 配置参数 | ❌ | ✅ | ✅ |
| `TO_ISO8601` IANA 时区参数 | ❌ | ✅ | ✅ |
| `TIMETRUNCATE` 时区字符串参数 | ❌ | ✅ | ✅ |
| `TIMETRUNCATE` `n`/`q`/`y` 截断 | ❌ | ✅ | ✅ |
| `INTERVAL` 查询 `w`/`n`/`q`/`y` 窗口 | ❌ | ✅ | ✅ |
| 普通列读取使用连接时区 | ❌ | ✅ | ✅ |
| SHOW/EXPLAIN 使用连接时区 | ❌ | ✅ | ✅ |
| 流任务 `TIMEZONE` 子句 | ❌ | ❌ | ✅ |
| 流任务时区/firstDayOfWeek 固化 | ❌ | ❌ | ✅ |
| PERIOD 触发 `q` 季度 | ❌ | ❌ | ✅ |
| SLIDING 触发 `n`/`q`/`y` | ❌ | ❌ | ✅ |
| INTERVAL 窗口触发 `d`/`w`/`n`/`q`/`y` | ❌ | ❌ | ✅ |
| `ins_streams` timezone/first_day_of_week 列 | ❌ | ❌ | ✅ |
