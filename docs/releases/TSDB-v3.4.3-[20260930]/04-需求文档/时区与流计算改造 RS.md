# 时区与流计算改造 - RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-26 | - | 0.1 | 关胜亮 | 初稿 |
| 2026-04-27 | - | 0.2 | 邝金清 | 更新流计算 TimeZone 子句为 STREAM_OPTIONS 中的 CONFIG |


## 2. 引言

### 2.1 背景

TDengine 流计算当前在时区、自然时间单位支持方面存在以下问题：

1. **流计算缺少独立时区**：所有触发类型使用服务端全局时区，无法为单个流任务指定时区，跨国公司按各地时区生成日报/周报的场景无法直接实现。
2. **时区未固化**：流任务运行时使用服务端全局时区，全局时区变更会影响已有流任务的行为，导致结果不可预测。
3. **PERIOD 不支持季度**：PERIOD 触发支持 `a`/`s`/`m`/`h`/`d`/`w`/`n`/`y`，但不支持季度（`q`）。
4. **SLIDING 触发不支持自然月/年/季度**：SLIDING 触发仅支持 `a`/`s`/`m`/`h`/`d`/`w`，代码明确禁止自然时间单位（`n`/`y`），也不支持季度（`q`）。
5. **INTERVAL 触发自然单位支持不完整**：INTERVAL 窗口触发的 interval_val 和 sliding_val 已支持 `d`/`w` / `n`/`y`，但仍不支持 `q`，自然单位实现是否标准还需要对齐代码。
6. **一周起始日未固化**：流任务运行时使用服务端全局 firstDayOfWeek，无法为单个流任务固定。

### 2.2 本版本目标

本版本的核心目标是实现流任务级时区独立与固化，并补齐各触发类型的自然时间单位能力：

| # | 目标 | 对应背景问题 |
| --- | --- | --- |
| G1 | 在 `STREAM_OPTIONS` 中新增 `CONFIG` 选项，支持为每个流任务显式指定 timezone 和 firstDayOfWeek | 问题 1、6 |
| G2 | 所有触发类型创建时固化 timezone 和 firstDayOfWeek，运行时不再依赖全局配置 | 问题 2、6 |
| G3 | PERIOD 新增季度（`q`）单位 | 问题 3 |
| G4 | SLIDING 触发新增自然月（`n`）、季度（`q`）、年（`y`）单位 | 问题 4 |
| G5 | INTERVAL 窗口触发新增 `n`/`q`/`y` 自然单位，并保持 `d`/`w` 既有能力兼容 | 问题 5 |

### 2.3 目标版本

**v3.4.3**，开源版与企业版均支持。

### 2.4 优先级

高优先级。流计算时区固化是跨国部署场景下实现各地时区独立日报/周报/月报的关键能力。

### 2.5 术语

| 术语 | 定义 |
| --- | --- |
| UTC | 协调世界时，TDengine 内部时间戳的存储基准 |
| IANA 时区名称 | 由 IANA 时区数据库维护的地理时区标识，如 `Asia/Shanghai`，自动包含夏令时规则 |
| 固定偏移 | 以 UTC 为基准的恒定偏移量，如 `+08:00`，不包含夏令时信息 |
| DST | 夏令时（Daylight Saving Time） |
| 挂钟时间 | 当地时钟显示的时间，DST 切换时会出现跳跃或重叠 |
| 自然时间单位 | 长度不固定的日历单位：天（`d`）、周（`w`）、月（`n`）、季度（`q`）、年（`y`） |
| L1-L5 | 五层时区优先级层级，详见第 4 章 |
| CONFIG | `STREAM_OPTIONS` 中的流任务局部配置项容器，用于显式设置 timezone 和 firstDayOfWeek |
| firstDayOfWeek | 一周起始日配置，0=周日、1=周一（默认）、...、6=周六 |
| 固化 | 流任务创建时将时区和 firstDayOfWeek 解析并写入流元数据，运行时直接使用固化值，不受后续全局配置变更影响 |

### 2.6 前置条件

本文档的实现依赖 v3.4.2 [时区与查询改造 RS](../../TSDB-v3.4.2-[20260630]/04-需求文档/时区与查询改造%20RS.md) 已交付的以下能力：

| 已有能力（v3.4.2） | 本文档使用方式 |
| --- | --- |
| L1-L5 五层时区层级体系 | 流任务创建时按 L1→L2→L4→L5 解析时区 |
| `SET TIMEZONE` / `SET FIRST_DAY_OF_WEEK` | 用户在创建流任务前设置连接级时区/firstDayOfWeek |
| 时区解析（`tzalloc`）和日历计算 | 流触发侧和计算侧复用时区/日历计算代码 |
| INTERVAL `w`/`n`/`q`/`y` 自然单位 | 流 AS subquery 中的 INTERVAL 窗口切分 |
| `firstDayOfWeek` 配置参数 | 流创建时固化 firstDayOfWeek |

### 2.7 相关文档

| 文档 | 位置 |
| --- | --- |
| 流计算定时触发自然时间单位 FS | `docs/releases/TSDB-v3.4.1-[20260331]/05-设计文档/流计算定时触发自然时间单位 FS.md` |
| 流计算语法参考 | `source/taos-community/docs/zh/14-reference/03-taos-sql/41-stream.md` |

## 3. 功能清单

| 序号 | 功能类别 | 功能名称 | 功能描述 |
| --- | --- | --- | --- |
| F1 | 流计算时区 | 流任务 CONFIG 选项 | `STREAM_OPTIONS(CONFIG(...))` 为流任务显式指定 timezone / firstDayOfWeek，适用于所有触发类型，作为 L1 层显式配置来源 |
| F2 | 流计算时区 | 全触发类型时区固化 | 所有流任务创建时按 L1→L2→L4→L5 解析 timezone，并按 L1→L2→L4→默认值 1 解析 firstDayOfWeek 后固化到流元数据；运行时使用固化值 |
| F3 | 流计算时区 | 流触发后查询使用固化时区 | 任意触发方式的流 SELECT 中，INTERVAL 窗口切分使用流元数据中固化的时区和 firstDayOfWeek |
| F4 | 一周起始日 | 全触发类型 firstDayOfWeek 固化 | 所有流任务创建时将 firstDayOfWeek 按 L1→L2→L4→默认值 固化到流元数据 |
| F5 | 流计算自然单位扩展 | PERIOD 支持季度 | PERIOD 触发新增 `q`（季度）单位，按季度首月 1 日 00:00:00 触发 |
| F6 | 流计算时区 | SLIDING 时区固化 | SLIDING 触发流任务创建时，timezone 按 L1→L2→L4→L5 固化（可通过 F1 指定 L1）、firstDayOfWeek 按 L1→L2→L4→默认值 固化 |
| F7 | 流计算自然单位扩展 | SLIDING 支持自然月/季/年 | SLIDING 触发新增 `n`（月）、`q`（季度）、`y`（年）单位，触发时刻与 PERIOD 同单位规则一致 |
| F8 | 流计算自然单位扩展 | INTERVAL 触发支持自然单位 | INTERVAL 窗口触发的 interval_val 和 sliding_val 新增 `n`/`q`/`y` 自然时间单位；既有 `d`/`w` 能力继续保留，窗口按各自然边界对齐，与查询侧 INTERVAL 规则一致 |

## 4. 流任务时区固化架构

### 4.1 固化原则

所有流任务在 `CREATE STREAM` 时均将 timezone 和 firstDayOfWeek（来自 `CONFIG` 显式配置或各自回退链解析结果）固化到流元数据，运行时一律从流元数据读取，不再回退服务端全局配置。

**timezone 固化回退链**：L1（`STREAM_OPTIONS(CONFIG(timezone = ...))`）→ L2（连接级）→ L4（服务端全局）→ L5（OS）

**firstDayOfWeek 固化回退链**：L1（`STREAM_OPTIONS(CONFIG(firstDayOfWeek = ...))`）→ L2（连接级）→ L4（服务端全局）→ 默认值 1（周一）

### 4.2 各触发类型的时区需求

| 触发类型 | 触发侧时区需求 | 计算侧（AS subquery）时区来源 | 涉及 F 项 |
| --- | --- | --- | --- |
| PERIOD | 支持 d/w/n/y/q 自然单位，涉及时区对齐 | 从流元数据读取固化时区 | F1、F2、F3、F4、F5 |
| SLIDING | 支持 d/w/n/q/y 自然单位，涉及时区对齐 | 从流元数据读取固化时区 | F1、F2、F3、F4、F6、F7 |
| INTERVAL 窗口触发 | 支持 d/w/n/q/y 自然单位，涉及时区对齐；其中 `d`/`w` 为既有能力，本次补齐 `n`/`q`/`y` | 从流元数据读取固化时区 | F1、F2、F3、F4、F8 |
| SESSION / STATE / EVENT / COUNT | 触发侧与时区无关 | 从流元数据读取固化时区 | F1、F2、F3、F4 |

## 5. 各场景详细行为

### 5.1 场景 ① 流计算触发（PERIOD/SLIDING/INTERVAL）

**timezone 来源**：当前 L4（服务端全局），**改为 L1→L2→L4→L5（创建时解析并固化）**

**firstDayOfWeek 来源**：当前 L4 / 默认值，**改为 L1→L2→L4→默认值 1（创建时解析并固化）**

```sql
-- L1: 通过 CONFIG 显式指定（适用所有触发类型）
CREATE STREAM s1 TRIGGER PERIOD(1w)
  STREAM_OPTIONS(CONFIG(timezone = 'Asia/Tokyo', firstDayOfWeek = 1))
  INTO tokyo_weekly AS SELECT AVG(current) FROM meters;

CREATE STREAM s2 TRIGGER SLIDING(1d) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'America/New_York'))
  INTO ny_daily AS SELECT _tprev_ts, _tcurrent_ts, AVG(current) FROM %%trows;

-- INTERVAL 触发使用自然单位
CREATE STREAM s_intv TRIGGER INTERVAL(1n) SLIDING(1w) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'Europe/London', firstDayOfWeek = 1))
  INTO uk_monthly AS SELECT _wstart, _wend, AVG(current) FROM %%trows;

-- L2: 使用连接级 timezone / firstDayOfWeek
SET TIMEZONE 'America/New_York';
SET FIRST_DAY_OF_WEEK 0;
CREATE STREAM s3 TRIGGER PERIOD(1n)
  INTO us_monthly AS SELECT SUM(energy) FROM meters;

-- 未设 L1/L2：timezone 回退 L4→L5，firstDayOfWeek 回退 L4→默认值
CREATE STREAM s4 TRIGGER PERIOD(1d)
  INTO daily AS SELECT COUNT(*) FROM meters;
```

**PERIOD 各单位触发时刻**：

| 单位 | 触发时刻 | 多倍数对齐 |
| --- | --- | --- |
| `d` | 指定时区 00:00:00 | epoch 整除 |
| `w` | 指定时区一周起始日 00:00:00 | epoch 整除 |
| `n` | 指定时区每月 1 日 00:00:00 | epoch 月份整除 |
| `q` | 指定时区每季度首月 1 日 00:00:00 | epoch 季度整除（Q1=1月, Q2=4月, Q3=7月, Q4=10月） |
| `y` | 指定时区每年 1 月 1 日 00:00:00 | epoch 年份整除 |

**SLIDING 各单位触发时刻**：与 PERIOD 规则完全一致（d/w/n/q/y 单位均按上表对齐），区别仅在于基于事件时间而非系统时间。

**INTERVAL 窗口触发自然单位窗口边界**（`d`/`w` 为既有能力，`n`/`q`/`y` 为本次新增）：

| 单位 | 窗口边界 | 多倍数对齐 |
| --- | --- | --- |
| `d` | 指定时区 00:00:00 | 本地时区 epoch 整除 |
| `w` | 指定时区一周起始日 00:00:00 | 本地时区 epoch 后第一个一周起始日整除 |
| `n` | 指定时区每月 1 日 00:00:00 | epoch 起月份计数整除 |
| `q` | 指定时区每季度首月 1 日 00:00:00 | epoch 起季度计数整除（Q1=1月, Q2=4月, Q3=7月, Q4=10月） |
| `y` | 指定时区每年 1 月 1 日 00:00:00 | epoch 起年份计数整除 |

INTERVAL 触发的窗口切分规则与查询侧 INTERVAL 完全一致。SLIDING 参数同样支持自然单位，其中 `d`/`w` 为既有能力，`n`/`q`/`y` 为本次新增；sliding 间隔的对齐方式与对应单位的窗口边界一致。

**DST 行为**：PERIOD、SLIDING、INTERVAL 三种触发类型的自然单位均遵循挂钟语义，与查询侧 INTERVAL 自然单位窗口切分规则完全一致。DST 切换日触发时刻/窗口边界在 UTC 坐标系上偏移，本地挂钟时间不变。

**闰年/变长月**（适用于 PERIOD、SLIDING、INTERVAL 所有自然单位）：
- 月单位：触发/窗口间隔随月份天数变化（28-31 天）
- 季度单位：触发/窗口间隔随季度天数变化（Q1=90/91天, Q2=91天, Q3=92天, Q4=92天）
- 年单位：闰年 366 天，平年 365 天

**offset 与 DST 的交互**：春跳日（如 02:00 不存在）时触发时刻顺延到跳后时刻。

**固化行为**：创建时按 L1→L2→L4→L5 解析 timezone、按 L1→L2→L4→默认值 解析 firstDayOfWeek，写入流元数据。运行时直接使用固化值，不受后续全局配置变更影响。

### 5.2 场景 ② 流触发后查询

**触发与计算分离**：`trigger_type` 决定何时触发，`AS subquery` 决定执行什么查询。时区需求出现在计算侧（subquery 可能包含 INTERVAL 自然单位窗口切分）。

**统一改造原则**：触发后执行流 SELECT 时，计算侧 INTERVAL 窗口切分统一使用流元数据中固化的时区和 firstDayOfWeek，与普通查询的 INTERVAL 窗口切分规则完全一致。区别仅在于时区来源为流元数据而非连接。

## 6. 新增 SQL 语法

### 6.1 流任务 CONFIG 选项

```sql
CREATE STREAM <stream_name> TRIGGER <trigger_type>
  [FROM <source_table>] [PARTITION BY <partition_expr>]
  [STREAM_OPTIONS(CONFIG(timezone = '<timezone_string>'[, firstDayOfWeek = <0-6>]) [| <other_stream_option> ...])]
  INTO <target_table> AS <select_statement>;
```

`CONFIG` 用于为流任务显式指定 `timezone` 和 `firstDayOfWeek`，适用于所有触发类型。两个键都为可选项，但至少需要出现一个；未显式指定的键仍按各自回退链解析并固化。

PERIOD、SLIDING、INTERVAL 触发的触发侧均涉及自然单位时区对齐，其他触发类型的计算侧（AS subquery）同样可能使用固化后的 timezone / firstDayOfWeek。

**示例**：

```sql
-- PERIOD 触发
CREATE STREAM weekly_us TRIGGER PERIOD(1w)
  STREAM_OPTIONS(CONFIG(timezone = 'America/New_York', firstDayOfWeek = 0))
  INTO us_weekly AS SELECT AVG(current) FROM meters;

-- SLIDING 触发
CREATE STREAM slide_ny TRIGGER SLIDING(1q) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'America/New_York'))
  INTO ny_quarterly AS SELECT _tprev_ts, _tcurrent_ts, AVG(current) FROM %%trows;

-- INTERVAL 触发使用自然单位
CREATE STREAM monthly_uk TRIGGER INTERVAL(1n) SLIDING(1w) FROM meters
  STREAM_OPTIONS(CONFIG(timezone = 'Europe/London', firstDayOfWeek = 1))
  INTO uk_monthly AS SELECT _wstart, _wend, AVG(current) FROM %%trows;

-- 其他触发类型（计算侧使用固化时区）
CREATE STREAM event_tokyo TRIGGER EVENT_WINDOW(START WITH voltage > 220 END WITH voltage <= 220) FROM meters PARTITION BY tbname
  STREAM_OPTIONS(CONFIG(timezone = 'Asia/Tokyo'))
  INTO event_out AS SELECT _twstart, _twend, AVG(current) FROM %%trows;
```

### 6.2 PERIOD 季度单位

```sql
PERIOD(1q)        -- 每季度触发
PERIOD(1q, 15d)   -- 每季度第 16 日触发
```

### 6.3 SLIDING 自然月/季/年单位

```sql
SLIDING(1n)        -- 每月滑动触发（新增）
SLIDING(1q)        -- 每季度滑动触发（新增）
SLIDING(1y)        -- 每年滑动触发（新增）
SLIDING(1q, 15d)   -- 每季度第 16 日滑动触发
```

### 6.4 INTERVAL 触发自然单位扩展

```sql
INTERVAL(1n) SLIDING(1w)    -- 月窗口，每周滑动（`1n` 为本次新增）
INTERVAL(1q) SLIDING(1n)    -- 季度窗口，每月滑动（`1q` 为本次新增）
INTERVAL(1y) SLIDING(1q)    -- 年窗口，每季度滑动（`1y` 为本次新增）
INTERVAL(1w) SLIDING(1d)    -- 周窗口，每天滑动（`1w` / `1d` 为既有能力，保留兼容）
```

## 7. 元数据变更

`information_schema.ins_streams` 视图新增：

| 列名 | 类型 | 说明 |
| --- | --- | --- |
| `timezone` | VARCHAR | 流任务固化的时区字符串；未显式配置时为按回退链解析后的结果 |
| `first_day_of_week` | INT | 流任务固化的一周起始日；未显式配置时为按回退链解析后的结果 |

## 8. 兼容性

**升级兼容**：
- 升级后现有流任务继续正常运行（未持久化时区字段的旧任务按全局时区解释，等价旧行为）
- 新建流任务才会固化 timezone / firstDayOfWeek

**降级兼容**：
- 降级到旧版本后，含 `STREAM_OPTIONS(CONFIG(...))` 创建的流任务无法识别 CONFIG 中固化的 timezone / firstDayOfWeek，回退到全局时区与默认周起始日行为
- 含 `firstDayOfWeek` 非默认值的流任务在旧版本中按周一处理

## 9. 测试需求

| 测试类别 | 覆盖要点 |
| --- | --- |
| PERIOD CONFIG 时区 | 不同时区流任务触发时刻正确性；全局时区变更后已有流不受影响；`CONFIG(timezone=...)` 生效 |
| SLIDING CONFIG 时区 | SLIDING 触发 d/w 单位对齐；`CONFIG(timezone=...)` 生效 |
| SLIDING 自然单位 | `SLIDING(1n)`/`SLIDING(1q)`/`SLIDING(1y)` 触发时刻；跨闰年间隔；DST 区域边界 |
| PERIOD 季度 | `PERIOD(1q)` 触发时刻；跨闰年季度间隔；DST 区域季度边界 |
| INTERVAL 自然单位 | `INTERVAL(1n)`/`INTERVAL(1q)`/`INTERVAL(1y)` 窗口边界正确性；`d`/`w` 既有能力回归；DST 区域边界 |
| 计算侧固化时区 | 流 SELECT 中 INTERVAL 自然单位使用固化时区切分 |
| firstDayOfWeek 固化 | `CONFIG(firstDayOfWeek=...)` 与连接级回退规则正确；创建后修改全局值不影响已有流 |
| 所有触发类型 | PERIOD/SLIDING/INTERVAL/SESSION/STATE/EVENT/COUNT 均正确固化 |
| 兼容性 | 升级后旧流任务正常运行 |
| 元数据 | `ins_streams` 新增列正确展示 |

## 10. 文档需求

| 文档 | 修改内容 |
| --- | --- |
| 流计算语法参考 | `STREAM_OPTIONS(CONFIG(...))` 语法说明（适用所有触发类型，支持 timezone / firstDayOfWeek）；PERIOD/SLIDING `q` 季度单位；INTERVAL 触发新增 `n`/`q`/`y` 自然单位并说明 `d`/`w` 既有兼容能力 |
| `ins_streams` 视图参考 | 新增 `timezone`、`first_day_of_week` 列说明 |
