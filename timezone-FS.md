# 时区与查询改造 - 设计方案

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-29 | - | 0.1 | Tony Zhang | 初稿 |

## 2. 引言

### 2.1 目的

本文档是「时区与查询改造」的功能设计方案，依据需求规格说明书（RS）落地以下设计决策：

- **时区层级体系**：定义 L1–L5 五层优先级和各场景的回退链，统一连接时区在客户端展示（F1、F2）与服务端计算（F5、F6、INTERVAL）中的生效规则。
- **新增 SQL 语法**：`SET TIMEZONE`（F3）的语法定义、参数约束、与现有 `ALTER LOCAL` 的层级关系。
- **函数时区扩展**：`TO_ISO8601` / `TO_CHAR` 支持 IANA 时区参数及无参时的回退行为（F5）；`TIMETRUNCATE` 第三参数扩展为字符串时区（F6）。
- **自然单位扩展**：`TIMETRUNCATE` 新增 `n`/`q`/`y` 截断规则（F10）；`INTERVAL` 新增季度单位 `q`（F11）；`1w` 对齐基准改为 `firstDayOfWeek`（F9）。
- **一周起始日**：`firstDayOfWeek` 服务端配置（F7）与 `SET FIRST_DAY_OF_WEEK` 连接级覆盖语法（F8），共同定义周相关计算的优先级链。
- **错误码与兼容性**：新增错误码定义；逐项说明行为变更范围和高风险变更。

本文档聚焦**行为规格**（做什么、如何表现），不涉及具体实现方案和测试用例。

### 2.2 需求来源

基于需求规格说明书 `timezone-rs.md`。经代码审查，对 RS 中的描述做了以下勘误：

| RS 原文 | 代码实际 | 本方案处理 |
| --- | --- | --- |
| "TIMETRUNCATE `1w` 文档描述为 Thursday" | 代码确实按 epoch（1970-01-01 星期四）取模对齐，结果落在星期四而非星期一 | F9 修正为按 `firstDayOfWeek` 对齐 |
| "INTERVAL 不支持 `w`/`n`/`y`/`q`" | INTERVAL 已支持 `w`（固定 7d）、`n`（月）、`y`（年），仅 `q`（季度）未支持 | F11 范围缩减为仅新增 `q` |
| TIMETRUNCATE 第三参数 "0=UTC, 1=连接时区" | 对外参数名仍为 `use_current_timezone`，用户可见语义与手册一致：省略或传 `1` 时，`d`/`w` 按连接时区对齐；传 `0` 时直接按 UTC 边界截断。当前代码内部使用布尔变量 `ignoreTz` 承载该语义，但该命名与对外语义相反，属于实现细节 | 设计文档保持 `use_current_timezone` 命名，并补充内部实现说明 |

### 2.3 术语

| 术语 | 定义 |
| --- | --- |
| UTC | 协调世界时，TDengine 内部时间戳的存储基准 |
| IANA 时区名称 | 由 IANA 时区数据库维护的地理时区标识，如 `Asia/Shanghai`，自动包含夏令时规则 |
| 固定偏移 | 以 UTC 为基准的恒定偏移量，如 `+08:00`，不包含夏令时信息 |
| DST | 夏令时（Daylight Saving Time） |
| 挂钟时间 | 当地时钟显示的时间，DST 切换时会出现跳跃或重叠 |
| 固定时间单位 | 长度恒定的单位：毫秒（`a`）、秒（`s`）、分钟（`m`）、小时（`h`）等 |
| 自然时间单位 | 长度不固定的日历单位：天（`d`）、周（`w`）、月（`n`）、季度（`q`）、年（`y`） |
| L1-L5 | 本文定义的五层时区优先级层级，详见第 4 章 |
| firstDayOfWeek | 一周起始日配置，0=周日、1=周一（默认）、...、6=周六 |

## 3. 功能总览与现状对照

| 功能 | RS 编号 | 现状 | 本次改造 |
| --- | --- | --- | --- |
| 普通列读取使用连接时区 | F1 | 使用客户端时区 | 改为连接时区 |
| SHOW/EXPLAIN 使用连接时区 | F2 | 使用客户端时区 | 改为连接时区 |
| `SET TIMEZONE` 语法 | F3 | 不存在（仅 C API `taos_options_connection`） | 新增 |
| TIMEZONE() 函数增强 | F4 | 返回单个时区字符串 | 新增可选参数 `all`；默认保持单字符串，`all=1` 返回包含三级时区的字符串 |
| TO_ISO8601 / TO_CHAR 支持 IANA 时区 | F5 | 仅支持 `±HH:MM` 偏移格式；TO_CHAR 无时区参数 | 扩展支持 IANA；无时区参数时回退连接时区 |
| TIMETRUNCATE 第三参数扩展 | F6 | 仅支持整数 `0`/`1` | 新增字符串时区参数 |
| `firstDayOfWeek` 配置参数 | F7 | 不存在 | 新增（服务端 `taos.cfg` 静态配置或 `ALTER ALL DNODES` 动态修改） |
| `SET FIRST_DAY_OF_WEEK` 语法 | F8 | 不存在 | 新增连接级覆盖，优先级高于服务端 `firstDayOfWeek` |
| TIMETRUNCATE `1w` 对齐修正 | F9 | 按 epoch（星期四）整除 | 改为按 `firstDayOfWeek` 对齐 |
| TIMETRUNCATE 支持 `n`/`q`/`y` | F10 | 仅支持 `b`/`u`/`a`/`s`/`m`/`h`/`d`/`w` | 新增 `n`/`q`/`y` |
| INTERVAL 支持 `q` | F11 | 已支持 `w`/`n`/`y`，不支持 `q` | 新增 `q` |

## 4. 时区层级体系

### 4.1 五层优先级

| 层级 | 名称 | 初始化 | 修改方式 | 生命周期 |
| --- | --- | --- | --- | --- |
| L1 | SQL 级 | 无需初始化 | 函数时区参数（如 `TO_ISO8601(ts, 'America/New_York')`） | 单条 SQL |
| L2 | 连接级 | 继承 L3 值 | `SET TIMEZONE` / C API `taos_options_connection` | 单个连接 |
| L3 | 客户端全局 | 客户端 `taos.cfg` 文件读取 | `ALTER LOCAL 'timezone' '<value>'` | 客户端进程 |
| L4 | 服务端全局 | 服务端 `taos.cfg` 文件读取 | `ALTER ALL DNODES 'timezone' '<value>'` | 服务端进程 |
| L5 | 系统默认 | 无需初始化 | OS 自动检测 | 永久 |

### 4.2 各场景回退链

| 场景 | 回退链 |
| --- | --- |
| 客户端本地展示（普通列读取 F1、SHOW/EXPLAIN F2） | L2 → L3 → L5 |
| 服务端日历计算（函数、INTERVAL、TIMETRUNCATE） | L1 → L2 → L4 → L5 |
| 按当前连接时区解析的时间字符串（INSERT、WHERE/CAST/JOIN 中的时间字面量） | L2 → L3 → L5 |

### 4.3 日期时间函数回退链

系统支持的所有日期时间函数，凡涉及将时间戳转换为本地日历值的，均受时区层级影响；周相关函数还额外受 `firstDayOfWeek` 配置影响。

#### 时区回退链

| 函数 / 场景 | 时区参数 | 回退链 |
| --- | --- | --- |
| `TO_ISO8601(ts, tz)` / `TO_CHAR(ts, fmt, tz)` / `TIMETRUNCATE(ts, unit, tz)` | 有（L1 生效） | **L1** → L2 → L4 → L5 |
| `TO_ISO8601(ts)` / `TO_CHAR(ts, fmt)` / `TIMETRUNCATE(ts, unit)` | 无 | L2 → L4 → L5 |
| `TODAY()` / `NOW()` | 无 | L2 → L3 → L5 |
| `DAYOFWEEK` / `WEEK` / `WEEKDAY` / `WEEKOFYEAR` 等 | 无 | L2 → L4 → L5 |
| `INTERVAL`（`d`/`w`/`n`/`q`/`y` 自然单位） | 无 | L2 → L4 → L5 |

固定单位（`b`/`u`/`a`/`s`/`m`/`h`）为恒定时长，不依赖时区，不在此列。

`TODAY()` 的语义是“取当前连接时区下当天 `00:00:00` 对应的 UTC 时间戳”。`NOW()` 不读取时区配置，直接返回当前时刻的 UTC 时间戳；但是时区在后续格式化展示时会影响其显示结果。

#### firstDayOfWeek 回退链

以下函数或场景在确定「一周起始日」时，除时区外还额外依赖 `firstDayOfWeek`：

| 函数 / 场景 | 说明 |
| --- | --- |
| `WEEK(ts [, mode])` | 年内周序号的归属计算依赖周起始日，mode 为 L1 级参数 |
| `WEEKOFYEAR(ts)` | 年内周序号计算依赖周起始日 |
| `DAYOFWEEK(ts)` | 不受影响 |
| `WEEKDAY(ts)` | 不受影响 |
| `TIMETRUNCATE(ts, Nw [, tz])` | 对齐到本周起始日 00:00:00（详见第 10.5 节） |
| `INTERVAL(Nw)` | 窗口边界对齐到本周起始日（详见第 11.5 节） |

`firstDayOfWeek` 支持连接级覆盖，其完整回退链见第 6.4 节：

```
SET FIRST_DAY_OF_WEEK  →  服务端 firstDayOfWeek  →  默认值 1（周一）
```

## 5. SET TIMEZONE 语法（F3）

### 5.1 语法

```sql
SET TIMEZONE '<timezone_string>';
```

### 5.2 参数、格式与约束

**语义**：设置当前连接的时区，影响后续所有 SQL 的时间解释和计算。

**参数**：
- `timezone_string`：支持IANA 时区名或固定偏移字符串

| 格式 | 示例 | DST 感知 |
| --- | --- | --- |
| IANA 名称 | `'Asia/Shanghai'`、`'America/New_York'` | 是 |
| 固定偏移 | `'+08:00'`、`'+0800'`、`'+08'`、`'-05:00'`、`'Z'` | 否 |

**约束**：
- 仅影响当前连接，不持久化
- 固定偏移支持 `Z`、`±HH`、`±HHMM`、`±HH:MM` 四种写法；不接受 `+8`、`-4` 这类单数字小时写法
- 固定偏移范围为 `-14:00` 到 `+14:00`；当小时为 `14` 时，分钟必须为 `00`
- 无效时区返回错误 `[0x2600] Invalid timezone: '<value>'`

### 5.3 示例

**示例**：

```sql
SET TIMEZONE 'Asia/Shanghai';
SET TIMEZONE 'America/New_York';
SET TIMEZONE 'UTC';
SET TIMEZONE 'CST';  -- 返回错误, 因为 CST 既可能是 China Standard Time (UTC+8) 又可能是 Central Standard Time (UTC-6)
SET TIMEZONE '+08:00';
SET TIMEZONE '+08';
SET TIMEZONE '-08';
SET TIMEZONE '+0800';
SET TIMEZONE '+08:30';
SET TIMEZONE '+14:30'; -- 返回错误, 超出允许范围

```

### 5.4 与 `ALTER LOCAL` 的关系

**与 `ALTER LOCAL` 的关系**：

当前 TDengine 已有 `ALTER LOCAL 'timezone' '<value>'` 语法，但它与 `SET TIMEZONE` 作用于**不同层级**：

| 维度 | `ALTER LOCAL 'timezone' '<value>'` | `SET TIMEZONE '<value>'` |
| --- | --- | --- |
| 修改目标 | 客户端进程全局 `tsTimezoneStr`（**L3 层**） | 当前连接 `STscObj.optionInfo.timezone`（**L2 层**） |
| 影响范围 | 当前客户端进程内修改后**新创建的连接** | **仅当前连接** |
| 持久化 | 否（纯内存） | 否（纯内存） |
| 等价 C API | 无直接等价 | `taos_options_connection(taos, TSDB_OPTION_CONNECTION_TIMEZONE, ...)` |

两者互不影响、互不替代：
- `ALTER LOCAL` 修改 L3（客户端全局），影响所有需要初始化 L2 的连接
- `SET TIMEZONE` 修改 L2（当前连接），优先级高于 L3，仅影响当前连接
- 在同一连接上先执行 `ALTER LOCAL` 再执行 `SET TIMEZONE`，后续查询使用 L2（`SET TIMEZONE` 的值）

### 5.5 TIMEZONE() 函数增强（F4）

#### 5.5.1 语法

```sql
SELECT TIMEZONE([all]);
```

**参数**：
- `all`：可选整数参数，仅接受 `0` 或 `1`
- 省略或传 `0`：返回单个字符串，行为与当前版本一致
- 传 `1`：返回单个 JSON 格式字符串，包含连接级、客户端、服务端三层时区信息

#### 5.5.2 现有行为

返回单个字符串：客户端时区字符串。

#### 5.5.3 改造后行为

默认行为保持兼容：

- `TIMEZONE()` 或 `TIMEZONE(0)`：返回单个字符串，行为与当前版本一致，即返回客户端时区字符串
- `TIMEZONE(1)`：返回单个 JSON 格式字符串，包含三级时区信息

`TIMEZONE(1)` 的返回格式定义为：

```text
{"session":"<session_timezone>","client":"<client_timezone>","server":"<server_timezone>"}
```

其中：
- `session_timezone`：连接级时区（L2）；未显式设置时仍输出实际生效的连接时区值，即继承客户端时区后的结果
- `client_timezone`：客户端 `taos.cfg timezone` 配置值；未配置时回退到系统检测值
- `server_timezone`：服务端 `taos.cfg timezone` 配置值；未配置时回退到系统检测值

**示例 1**：已设置连接级时区

```sql
-- 假设客户端/服务端配置文件中配置了 timezone=Asia/Shanghai
SET TIMEZONE 'America/New_York';
SELECT TIMEZONE();
```

返回：

```text
Asia/Shanghai
```

```sql
SELECT TIMEZONE(1);
```

返回：

```text
{"session":"America/New_York (UTC-5, EST)","client":"Asia/Shanghai (UTC+8, CST)","server":"Asia/Shanghai (UTC+8, CST)"}
```

**示例 2**：未设置连接级时区

```sql
SELECT TIMEZONE(1);
```

返回：

```text
{"session":"Asia/Shanghai (UTC+8, CST)","client":"Asia/Shanghai (UTC+8, CST)","server":"Asia/Shanghai (UTC+8, CST)"}
```

**各字段含义**：
- **session**（L2）：通过 `SET TIMEZONE` 或 C API `taos_options_connection` 设置的连接级时区，未设置时继承客户端时区
- **client**（L3→L5）：客户端 `taos.cfg timezone` 配置值，未配置时回退到系统检测值
- **server**（L4→L5）：服务端 `taos.cfg timezone` 配置值，未配置时回退到系统检测值

#### 5.5.4 兼容性考虑

默认调用 `TIMEZONE()` / `TIMEZONE(0)` 保持单行单列字符串返回，是**兼容变更**。仅显式使用 `TIMEZONE(1)` 时，才返回包含三级时区信息的 JSON 格式字符串。

## 6. firstDayOfWeek 配置与覆盖（F7, F8）

### 6.1 参数定义

| 属性 | 值 |
| --- | --- |
| 配置端 | 服务端 `taos.cfg` |
| 参数名 | `firstDayOfWeek` |
| 类型 | 整数 |
| 范围 | 0-6 |
| 默认值 | 1 |
| 影响 | 所有 `w` 单位的边界对齐（TIMETRUNCATE、INTERVAL） |

**取值含义**：0=周日，1=周一（默认），2=周二，...，6=周六

### 6.2 配置方式

**静态配置**（服务端 `taos.cfg`）：

```
firstDayOfWeek 1
```

**动态修改**（运行时生效，集群内所有节点同步）：

```sql
ALTER ALL DNODES 'firstDayOfWeek' '1';
```

**约束**：
- 超出 0-6 范围返回 `[0x2601] Invalid firstDayOfWeek: <value>, must be 0-6`
- 为全局配置，不支持单个 dnode 单独设置（`ALTER DNODE N` 对此参数会被拒绝）

### 6.3 `SET FIRST_DAY_OF_WEEK` 语法

```sql
SET FIRST_DAY_OF_WEEK <value>;
```

**语义**：为当前连接设置一周起始日，影响该连接后续 `WEEK` / `WEEKOFYEAR` / `TIMETRUNCATE(..., w)` / `INTERVAL(..., w)` 等依赖周边界的计算。

**参数**：
- `<value>`：0-6 整数，0=周日，1=周一（默认），...，6=周六

**约束**：
- 仅影响当前连接，不持久化
- 超出 0-6 范围返回 `[0x2601] Invalid firstDayOfWeek: <value>, must be 0-6`
- 优先级高于服务端 `firstDayOfWeek` 配置，仅覆盖当前连接

**示例**：

```sql
SET FIRST_DAY_OF_WEEK 0;
SET FIRST_DAY_OF_WEEK 1;
```

### 6.4 回退链

连接级 `SET FIRST_DAY_OF_WEEK` → 服务端 `taos.cfg firstDayOfWeek` → 默认值 1

## 7. 查询相关行为（F1, F2）

### 7.1 行为变更

| 场景 | 改造前 | 改造后 |
| --- | --- | --- |
| `SELECT ts FROM t` 展示 | 使用客户端时区 (L3→L5) | 使用连接时区 (L2→L3→L5) |
| `SHOW TABLES` 等创建时间 | 使用客户端时区 | 使用连接时区 |
| `EXPLAIN` 输出中时间 | 使用客户端时区 | 使用连接时区 |

### 7.2 行为示例

```sql
-- 假设 ts 存储值为 1742054400000 (2026-03-16 00:00:00 UTC)

SET TIMEZONE 'Asia/Shanghai';
SELECT ts FROM t;
-- 结果: 2026-03-16 08:00:00.000

SET TIMEZONE 'America/New_York';
SELECT ts FROM t;
-- 结果: 2026-03-15 20:00:00.000 (EDT, UTC-4)
```

### 7.3 WHERE / CAST / JOIN

`WHERE` 条件中的时间字符串字面量按当前连接时区解释后再参与比较；`CAST` 在时间戳与字符串之间转换时遵循同一时区口径；`JOIN` 条件中若包含时间字符串比较，语义与 `WHERE` 一致。

### 7.4 WHERE / CAST / JOIN 回退链

该场景中的时间字符串字面量按当前连接时区解析，不使用服务端 `L4`；连接未设置时回退到客户端全局和系统默认：

```
L2 → L3 → L5
```

### 7.5 WHERE / CAST / JOIN 兼容性

该场景本次不改行为，仅在文档中补齐并与整体时区口径对齐。

### 7.6 查询中遇到 DST 春跳 / 秋退时间

查询路径需要区分两类场景：

- **展示已有时间戳**：`SELECT ts`、`TO_ISO8601(ts)`、`TO_CHAR(ts, ...)` 这类场景输入的是已存储的 UTC int64 绝对时刻，只是在结果展示阶段按连接时区或显式时区做本地化，因此**不存在“本地时间不存在/重复导致无法确定结果”的问题**。系统只需根据目标时刻所在的 DST 状态选择对应偏移进行显示。
- **解析查询中的时间字符串字面量**：`WHERE ts >= '2026-03-08 02:30:00'`、`CAST('2026-11-01 01:30:00' AS TIMESTAMP)`、`JOIN ... ON t1.ts = '2026-11-01 01:30:00'` 这类场景需要先把字符串按连接时区（L2 → L3 → L5）解析为 UTC 绝对时刻，再参与比较、连接或函数计算。

当查询中的时间字符串字面量落在 DST 切换窗口内时，处理规则与写入路径保持一致：

- **春跳（gap，不存在的本地时间）**：自动归一化到跳变后的第一个有效时刻。例如连接时区为 `America/New_York` 时，`'2026-03-08 02:30:00'` 会按 `2026-03-08 03:30:00 EDT` 解析后再参与比较。
- **秋退（overlap，重复的本地时间）**：默认按**第一次出现**解析。例如连接时区为 `America/New_York` 时，`'2026-11-01 01:30:00'` 默认解释为第一次出现的 `01:30:00`，即 `2026-11-01 05:30:00 UTC`。

也就是说，查询引擎内部仍然只比较**绝对时刻**；DST 带来的影响仅发生在“把字符串解释成哪个绝对时刻”这一步，而不会改变后续比较逻辑。

```sql
SET TIMEZONE 'America/New_York';

SELECT *
FROM t
WHERE ts >= '2026-03-08 02:30:00';
-- 等价于 WHERE ts >= '2026-03-08 03:30:00 EDT'

SELECT CAST('2026-11-01 01:30:00' AS TIMESTAMP);
-- 默认按第一次出现的 01:30:00 处理，即 2026-11-01 05:30:00 UTC
```

**建议**：若查询条件、JOIN 条件或 CAST 输入位于 DST 切换窗口，且业务需要精确区分两次出现的本地时间，优先使用整型时间戳或带明确偏移的字符串（如 `2026-11-01T01:30:00-04:00` / `2026-11-01T01:30:00-05:00`），避免歧义。

## 8. 写入行为与 DST

### 8.1 基本原则

TDengine 时间戳在服务端始终以 UTC int64 存储，写入操作本身不受时区影响。**影响写入的是字符串时间字面量的解析**：当 SQL 中包含本地时间字符串（如 `'2026-03-08 02:30:00'`）时，客户端使用连接时区（L2 → L3 → L5）将其解析为 UTC 时间戳后再发往服务端。

整型时间戳（如 `1741395000000`）无歧义，不受 DST 影响。

### 8.2 春跳（Spring Forward）— 本地时间不存在

以 `America/New_York` 为例：2026-03-08 02:00:00 EST（UTC-5）跳至 03:00:00 EDT（UTC-4），本地时间 `02:00:00`–`02:59:59` 这一小时在物理上不存在（gap）。

写入落在该 gap 内的字符串时间戳时：

```sql
SET TIMEZONE 'America/New_York';
INSERT INTO t VALUES ('2026-03-08 02:30:00', 42.0);
```

行为：春跳不存在的时间自动调整为跳后时刻。例如，上述 SQL 实际写入的时间戳对应 `2026-03-08 03:30:00 EDT`（UTC-4），即 `1741395000000`。

**建议**：在 DST 区域写入跨越春跳边界的数据时，使用整型时间戳或带明确偏移的字符串，避免歧义。

### 8.3 秋退（Fall Back）— 本地时间重复

以 `America/New_York` 为例：2026-11-01 会从 02:00 EDT（UTC-4）回拨到 01:00 EST（UTC-5），所以本地时间 `01:00:00`–`01:59:59` 会出现两次。

写入落在该重叠区间内的字符串时间戳时：

```sql
SET TIMEZONE 'America/New_York';
INSERT INTO t VALUES ('2026-11-01 01:30:00', 42.0);
```

行为：这类本地时间有歧义，系统通常按**第一次出现**处理，也就是 `2026-11-01 05:30:00 UTC`。如果要明确表示第二次出现的 `01:30:00`，需要使用整型时间戳或带明确偏移的字符串。

**建议**：在秋退重叠窗口内需精确区分两次出现时，使用整型时间戳或带明确偏移的字符串，避免歧义。

### 8.4 总结

| 写入方式 | DST 影响 | 说明 |
| --- | --- | --- |
| 整型时间戳 | 无 | UTC，无歧义，推荐用于高精度场景 |
| 字符串 + 固定偏移（如 `'+00:00'`） | 无 | 偏移明确，无歧义 |
| 字符串 + 连接时区为 IANA（本地时间） | **有** | gap 内时间被 normalize；overlap 内取第一次出现 |

## 9. TO_ISO8601 / TO_CHAR 时区扩展（F5）

### 9.1 语法

```sql
TO_ISO8601(ts [, timezone])
TO_CHAR(ts, format_str [, timezone])
```

两个函数均将 `timezone` 扩展为可选第三（TO_CHAR）/第二（TO_ISO8601）参数，支持 IANA 名称或固定偏移字符串。

### 9.2 现有行为（保持不变）

**TO_ISO8601**：

```sql
TO_ISO8601(ts)                        -- 按客户端时区（L3→L5）输出，附加偏移后缀
TO_ISO8601(ts, '+08:00')              -- 输出 UTC+8，后缀 +08:00
TO_ISO8601(ts, 'Z')                   -- 输出 UTC，后缀 Z
```

**TO_CHAR**：

```sql
TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') -- 按客户端时区（L3→L5）格式化
```

### 9.3 新增行为：IANA 时区参数

**TO_ISO8601**：

```sql
TO_ISO8601(ts, 'Asia/Shanghai')       -- 输出指定时区时间，后缀 +08:00
TO_ISO8601(ts, 'America/New_York')    -- 输出指定时区时间，后缀根据 DST 变化
```

**TO_CHAR**：

```sql
TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York')  -- 按纽约时区格式化
TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS', '+08:00')            -- 按固定偏移格式化
```

有 `timezone` 参数时，该参数作为 L1 覆盖连接时区，两个函数语义一致。

### 9.4 DST 行为

两个函数使用 IANA 时区时，偏移量（输出后缀或格式化结果）均随**目标时刻**的 DST 状态动态变化：

```sql
-- America/New_York: EST(UTC-5) / EDT(UTC-4)
-- 2026-03-08 02:00 EST → 03:00 EDT (春跳)

SELECT TO_ISO8601(ts, 'America/New_York') FROM t;
-- 2026-01-15T12:00:00.000-05:00  (冬季, EST)
-- 2026-07-15T13:00:00.000-04:00  (夏季, EDT)

SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York') FROM t;
-- 2026-01-15 07:00:00  (冬季, EST, UTC-5)
-- 2026-07-15 09:00:00  (夏季, EDT, UTC-4)
```

### 9.5 无 timezone 参数时的回退（改造点）

两个函数在**无 `timezone` 参数**时，均改为使用连接级时区（L2→L3→L5）格式化输出：

| 函数 | 改造前 | 改造后 |
| --- | --- | --- |
| `TO_ISO8601(ts)` | 按客户端时区（L3→L5），附加偏移后缀 | 按连接时区（L2→L3→L5），附加偏移后缀 |
| `TO_CHAR(ts, fmt)` | 按客户端时区（L3→L5） | 按连接时区（L2→L3→L5） |

```sql
-- 假设客户端时区为 Asia/Shanghai，连接时区设置为 America/New_York
SET TIMEZONE 'America/New_York';

SELECT TO_ISO8601(ts) FROM t;
-- 改造前: 2026-03-16T08:00:00.000+08:00  (客户端时区 Asia/Shanghai)
-- 改造后: 2026-03-15T20:00:00.000-04:00  (连接时区 America/New_York)

SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') FROM t;
-- 改造前: 2026-03-16 08:00:00  (客户端时区 Asia/Shanghai)
-- 改造后: 2026-03-15 20:00:00  (连接时区 America/New_York)
```

未设置 `SET TIMEZONE` 时，两个函数均回退到原行为，完全兼容。

## 10. TIMETRUNCATE 改造（F6, F9, F10）

### 10.1 语法

```sql
TIMETRUNCATE(ts, time_unit [, tz_or_flag])
```

### 10.2 time_unit 扩展（F10）

**新增支持**的时间单位：

| 单位 | 含义 | 示例 | 多倍数 |
| --- | --- | --- | --- |
| `n` | 月 | `1n`, `3n`, `6n` | 支持 |
| `q` | 季度 | `1q`, `2q` | 支持 |
| `y` | 年 | `1y`, `2y` | 支持 |

原有单位 `b`/`u`/`a`/`s`/`m`/`h`/`d`/`w` 行为不变。

### 10.3 `n`/`q`/`y` 截断规则

自然单位统一在**连接时区的本地时间坐标系**中执行截断；其中 `d`/`w` 的对齐规则分别见 10.4、10.5、10.6，本节仅展开 `n`/`q`/`y`。

| 单位 | 截断规则 | 示例（Asia/Shanghai） |
| --- | --- | --- |
| `1n` | 对齐到当月 1 日 00:00:00 | `TIMETRUNCATE('2026-03-15 10:30:00', 1n)` → `2026-03-01 00:00:00` |
| `3n` | 按 epoch 起月份计数整除 | `TIMETRUNCATE('2026-05-15', 3n)` → `2026-04-01 00:00:00` |
| `1q` | 对齐到当季首月 1 日 00:00:00 | `TIMETRUNCATE('2026-05-15', 1q)` → `2026-04-01 00:00:00` |
| `2q` | 按 epoch 起季度计数整除 | `TIMETRUNCATE('2026-05-15', 2q)` → `2026-01-01 00:00:00` |
| `1y` | 对齐到当年 1 月 1 日 00:00:00 | `TIMETRUNCATE('2026-08-15', 1y)` → `2026-01-01 00:00:00` |

**季度定义**（固定，不可配置）：
- Q1: 1-3月，Q2: 4-6月，Q3: 7-9月，Q4: 10-12月

**多倍数对齐基准**：以 Unix epoch（1970-01-01 UTC）为基准计算。

### 10.4 第三参数扩展（F6）

第三参数类型扩展为 **整数或字符串**，运行时按类型区分语义。对于整数参数，对外名称仍使用用户手册中的 `use_current_timezone`：

| 第三参数 | 类型 | 语义 | 兼容性 |
| --- | --- | --- | --- |
| 省略 | - | 默认行为；当前实现等同 `use_current_timezone=1`，即 `d`/`w` 按连接时区对齐，其他按 UTC 边界截断 | 完全兼容 |
| `0` | 整数 | `use_current_timezone=0`：忽略连接时区，直接按 UTC 边界截断 | 完全兼容 |
| `1` | 整数 | `use_current_timezone=1`：等同省略，`d`/`w` 按连接时区对齐 | 完全兼容 |
| `'Asia/Shanghai'` | 字符串 | 使用指定时区对齐，覆盖连接时区 | 新增 |
| `'+08:00'` | 字符串 | 使用指定偏移对齐 | 新增 |

这里的“按 UTC 边界截断”是指：先把输入时间戳视为 UTC 时间，再按 UTC 时区下该单位的边界取整，不先换算到连接时区。例如对 `1d` 而言，就是截断到 UTC 当天的 `00:00:00`；对 `1h` 而言，就是截断到 UTC 当前小时的整点。

**实现说明**：当前代码内部使用布尔变量 `ignoreTz` 表示该开关，但其命名与对外语义相反：对外是 `use_current_timezone=1` 表示“按当前时区截断”，内部实现却由 `ignoreTz=true` 路径触发时区补偿。该差异仅是内部命名问题，不改变用户可见行为；本设计文档统一以 `use_current_timezone` 描述对外接口。

**示例**：

```sql
-- 旧语义不变
SET TIMEZONE 'Asia/Shanghai';
-- 按连接时区的当天 00:00 -> 2026-03-15 00:00:00
TIMETRUNCATE('2026-03-15 10:30:00', 1d)
TIMETRUNCATE('2026-03-15 10:30:00', 1d, 1)

-- '2026-03-15 10:30:00' 对应 UTC 时间为 2026-03-15 02:30:00
-- 因此按 UTC 截断到天边界后，结果对应 2026-03-15 00:00:00 UTC，显示结果为 2026-03-15 08:00:00（连接时区 Asia/Shanghai）
TIMETRUNCATE('2026-03-15 10:30:00', 1d, 0)

-- 新增：字符串时区参数
-- 输入字符串先按连接时区 Asia/Shanghai 解析为 2026-03-15 02:30:00 UTC
-- 再按纽约时区的当天 00:00 截断，显示结果为 2026-03-14 12:00:00（连接时区 Asia/Shanghai）
TIMETRUNCATE('2026-03-15 10:30:00', 1d, 'America/New_York')

-- 显式冲突示例：输入字符串自带 +08:00，但第三参数指定 America/New_York
TIMETRUNCATE('2026-03-15T10:30:00+08:00', 1d, 'America/New_York')
-- 预期返回：2026-03-14 12:00:00
```

当输入时间字符串自带时区或使用连接时区解析，而第三参数又指定了另一个时区时，分两步处理：

1. 先按输入字符串自带的时区解析出**绝对时刻**。以上例中，`2026-03-15T10:30:00+08:00` 先被解析为 `2026-03-15 02:30:00 UTC`。
2. 再按第三参数指定的时区确定**截断边界**。以上例中，第三参数为 `America/New_York`，此时对应当地时间为 `2026-03-14 22:30:00`（EDT, UTC-4），因此 `1d` 需要对齐到纽约当天的 `00:00:00`。
3. 纽约当天 `00:00:00` 对应的 UTC 时刻为 `2026-03-14 04:00:00 UTC`。
4. 结果最终按当前连接时区展示。若连接时区为 `Asia/Shanghai`，则显示为 `2026-03-14 12:00:00`；若连接时区为 `America/New_York`，则显示为 `2026-03-14 00:00:00`。

也就是说，**输入字符串里的时区只决定“这是哪个绝对时刻”，第三参数时区只决定“按哪个时区的日历边界做截断”**。

### 10.5 `1w` 对齐修正（F9）

**改造前**：`TIMETRUNCATE(ts, 1w)` 按 Unix epoch 整除，对齐到星期四。

**改造后**：按 `firstDayOfWeek` 对齐到本地周起始日 00:00:00。

```sql
-- 假设服务端配置 firstDayOfWeek = 1（周一）
TIMETRUNCATE('2026-04-30', 1w);  -- 2026-04-27 (周一)

-- 假设服务端配置 firstDayOfWeek = 0（周日）
TIMETRUNCATE('2026-04-30', 1w);  -- 2026-04-26 (周日)
```

**多倍数 `Nw`**：先对齐到本周起始日，再按 N 周对齐到 epoch 起始后的周计数。

### 10.6 DST 行为

对 `d`/`w`/`n`/`q`/`y` 单位，使用 IANA 时区时，截断边界始终按**本地挂钟时间 00:00:00** 确定：

```sql
SET TIMEZONE 'America/New_York';
-- 2026-03-08 是春跳日 (02:00 → 03:00)

TIMETRUNCATE('2026-03-08 12:00:00', 1d);
-- 结果: 2026-03-08 00:00:00 (EST, 当天只有23小时)
```

## 11. INTERVAL 季度支持（F11）

### 11.1 语法

```sql
SELECT ... FROM ... INTERVAL(Nq) [SLIDING(Mq)];
```

其中 `q` 为新增的季度单位。

### 11.2 窗口切分规则

| 属性 | 规则 |
| --- | --- |
| 窗口边界 | 本地时区每季度首月 1 日 00:00:00 |
| 季度定义 | Q1: 1-3月，Q2: 4-6月，Q3: 7-9月，Q4: 10-12月 |
| 多倍数 `2q` | 按 epoch 起季度计数整除 |
| SLIDING | 支持 `Nq`（季度滑动）或更小的自然/固定单位 |

### 11.3 语义等价

`INTERVAL(1q)` 在行为上等价于 `INTERVAL(3n)`：

```sql
-- 以下两条查询结果相同
SELECT COUNT(*) FROM t INTERVAL(1q);
SELECT COUNT(*) FROM t INTERVAL(3n);
```

### 11.4 窗口示例

```sql
SET TIMEZONE 'Asia/Shanghai';
SELECT _wstart, _wend, COUNT(*) FROM t INTERVAL(1q);

-- 输出窗口:
-- [2026-01-01 00:00:00, 2026-04-01 00:00:00)  -- Q1, 90天
-- [2026-04-01 00:00:00, 2026-07-01 00:00:00)  -- Q2, 91天
-- [2026-07-01 00:00:00, 2026-10-01 00:00:00)  -- Q3, 92天
-- [2026-10-01 00:00:00, 2027-01-01 00:00:00)  -- Q4, 92天
-- Interval 窗口为左闭右开区间
```

### 11.5 `w` 单位尊重 firstDayOfWeek

INTERVAL `w` 窗口的起始日改为按 `firstDayOfWeek` 对齐（与 TIMETRUNCATE `1w` 一致）：

```sql
-- 假设服务端配置 firstDayOfWeek = 0（周日）
SELECT _wstart, COUNT(*) FROM t INTERVAL(1w);
-- 窗口起始日为周日，而非当前的周四
```

## 12. 错误码

| 错误码 | 场景 | 消息 |
| --- | --- | --- |
| `0x2600` | 无效时区字符串 | `Invalid timezone: '<value>'` |
| `0x2601` | firstDayOfWeek 超出范围 | `Invalid firstDayOfWeek: <value>, must be 0-6` |

## 13. 兼容性总结

| 模块 / 场景 | 是否改动 | 兼容性影响 |
| --- | --- | --- |
| `SET TIMEZONE` | 新增 | 新增语法，不影响旧 SQL；只有显式使用该语法或等价 C API 时，连接级时区才会覆盖原有客户端时区口径 |
| `ALTER LOCAL 'timezone'` | 不改动 | 仍用于修改客户端全局时区（L3）；未使用 `SET TIMEZONE` 时，行为与当前版本保持一致 |
| 普通列读取、`SHOW`、`EXPLAIN` 时间展示 | 有条件改动 | 仅在设置连接级时区后，展示口径从客户端时区改为连接时区；未设置时按原有 L3→L5 行为回退 |
| `WHERE` / `CAST` / `JOIN` 中时间字符串解析 | 不改动 | 本次不改变现有解析链路，仍按 L2→L3→L5 解释时间字面量；兼容现有 SQL 行为 |
| 写入路径：整型时间戳、带明确偏移字符串 | 不改动 | 整型时间戳及带显式偏移的时间字符串仍无歧义；本次仅补充文档说明，不改变兼容性 |
| 写入路径：本地时间字符串 + IANA 时区 | 不改动 | DST 春跳自动归一化、秋退重叠时刻按当前系统/时区库处理；本次仅把既有行为文档化 |
| `TO_ISO8601(ts, '+08:00')` 等旧偏移调用 | 不改动 | 现有偏移格式继续可用；新增 IANA 支持不影响旧调用 |
| `TO_ISO8601(ts)` 无参回退 | 有条件改动 | 仅在设置连接级时区后，回退口径从客户端时区改为连接时区；未设置时保持原行为 |
| `TO_CHAR(ts, fmt)` 二参数调用 | 有条件改动 | 仅在设置连接级时区后，回退口径从客户端时区改为连接时区；未设置时保持原行为 |
| `TO_CHAR(ts, fmt, tz)` / `TO_ISO8601(ts, tz)` IANA 参数 | 新增 | 新增能力，不影响原有二参数/单参数调用 |
| `TIMETRUNCATE(..., unit, 0/1)` | 不改动 | 整数第三参数 `0`/`1` 保持原语义和兼容性 |
| `TIMETRUNCATE(..., unit, '<timezone>')` | 新增 | 新增字符串时区参数，不影响旧 SQL |
| `TIMETRUNCATE(..., 1w)` | 行为变更 | **高风险**：周对齐基准从星期四改为 `firstDayOfWeek`。默认 `firstDayOfWeek=1` 时结果变为按周一对齐，与旧行为不同 |
| `TIMETRUNCATE(..., n/q/y)` | 新增 | 新增自然时间单位，不影响旧 SQL |
| 服务端 `firstDayOfWeek` 配置 / `SET FIRST_DAY_OF_WEEK` | 新增 | 新增配置和连接级覆盖能力；默认值为 1，本身不破坏旧语法兼容性，但会与新的周对齐规则共同决定结果 |
| `INTERVAL(..., q)` | 新增 | 新增季度窗口，不影响旧 SQL |
| `INTERVAL(..., w)` | 行为变更 | **高风险**：窗口起始日从星期四改为 `firstDayOfWeek`；默认值 1 时变为按周一起窗 |
| `TIMEZONE()` 返回值 | 有条件改动 | 默认 `TIMEZONE()` / `TIMEZONE(0)` 保持旧格式兼容；仅新增 `TIMEZONE(1)` 扩展字符串返回三级时区信息 |
| 新增错误码 `0x2600` / `0x2601` | 新增 | 仅在使用新增语法或传入非法参数时返回；不影响原有合法 SQL |

**高风险变更标记**：
- TIMETRUNCATE `1w` 和 INTERVAL `1w` 的对齐基准变化（星期四 → 周一），即使使用默认配置也会影响现有查询结果。建议在版本说明中明确告知。

## 14. 参考资料

| 文档 | 说明 |
| --- | --- |
| [时区与查询改造 RS](https://git.tdengine.net/rd-public/tsdb/-/blob/main/docs/releases/TSDB-v3.4.2-%5B20260630%5D/04-%E9%9C%80%E6%B1%82%E6%96%87%E6%A1%A3/%E6%97%B6%E5%8C%BA%E4%B8%8E%E6%9F%A5%E8%AF%A2%E6%94%B9%E9%80%A0%20RS.md?ref_type=heads) | 需求说明书 |
