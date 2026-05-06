# 时区与查询改造 - 详细设计说明书（DS）

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-27 | - | 0.1 | AI | 初稿 |
| 2026-05-06 | - | 0.2 | AI | 基于 FS 与当前代码重整；补充“代码基线 vs 目标差异”；按最新约定将季度单位视为已支持（来自待 rebase PR） |

## 2. 引言

### 2.1 目的

本文档用于把 `timezone-FS.md` 的行为规格落到可执行实现方案，并明确：

1. 当前代码基线（截至 2026-05-06）
2. 与 FS 的差距
3. 各功能点（F1-F11）的实现路径、模块改动与交付顺序

### 2.2 范围

覆盖模块：

- parser：新增 `SET TIMEZONE`、`SET FIRST_DAY_OF_WEEK`，扩展函数参数类型
- client：连接级时区/周起始日配置与结果展示路径
- function/scalar：`TO_ISO8601`、`TO_CHAR`、`TIMETRUNCATE`、`TIMEZONE()`
- common/config：`firstDayOfWeek` 全局配置
- executor：INTERVAL 周窗口与季度窗口行为

### 2.3 术语

沿用 `timezone-FS.md` 第 2.3 节，不再重复。

## 3. 代码基线与假设

### 3.1 当前代码基线（截至本分支）

1. 已有连接级时区基础设施：
- `taos_options_connection(..., TSDB_OPTION_CONNECTION_TIMEZONE, ...)` 已支持
- `SOptionInfo.timezone` 已存在
- `timezone_t/tzalloc/localtime_rz/mktime_z` 体系已存在

2. 当前未落地（与 FS 有差距）：
- 无 `SET TIMEZONE` SQL 语法
- 无 `SET FIRST_DAY_OF_WEEK` SQL 语法
- 无服务端 `firstDayOfWeek` 配置项
- `TO_ISO8601` 第二参数仅接受固定偏移格式，不支持 IANA
- `TO_CHAR` 仅双参数，不支持时区参数
- `TIMETRUNCATE` 第三参数仅支持 `0/1`，且单位校验仍限定 `1[b|u|a|s|m|h|d|w]`
- `TIMEZONE()` 仅返回单字符串，不支持 `TIMEZONE(1)`

3. 现有错误码：
- `0x2600` / `0x2601` 目前是 parser 语法相关错误码（非“Invalid timezone/firstDayOfWeek”语义）

### 3.2 本 DS 的实现假设

1. 以 `timezone-FS.md` 为目标行为准绳。
2. 根据最新约定：
- “季度时间单位（`q`）已在另一个 PR 中实现并将 rebase 合入当前分支”。
- 因此本 DS 默认 `q` 在最终集成分支中可用，不再重复设计另一套冲突实现。
3. 本 DS 中涉及 `q` 的内容按“集成视角”描述，若本地分支暂未看到代码，按 rebase 后状态验收。

## 4. 总体设计

### 4.1 层级模型

按 FS 固化两条核心优先级链：

1. 时区层级：
- SQL 参数（L1） → 连接级（L2） → 客户端全局/服务端全局（L3/L4） → 系统默认（L5）

2. 周起始日层级：
- `SET FIRST_DAY_OF_WEEK`（连接级） → 服务端 `firstDayOfWeek` → 默认值 `1`

### 4.2 设计原则

1. 默认兼容：旧 SQL 不变。
2. 最小侵入：复用现有 `timezone_t` 与时间转换工具链。
3. 明确分层：
- 客户端本地展示走连接时区
- 服务端日历计算走函数/连接/服务端回退链

## 5. 功能点设计（F1-F11）

### 5.1 F3: `SET TIMEZONE` 语法

#### 5.1.1 目标语法

```sql
SET TIMEZONE '<timezone_string>';
```

#### 5.1.2 Parser 设计

文件：
- `source/libs/parser/inc/sql.y`
- `source/libs/parser/src/parAstCreater.c`

新增 AST 节点：
- `QUERY_NODE_SET_TIMEZONE_STMT`

新增构造函数：
- `createSetTimezoneStmt(...)`

#### 5.1.3 执行位置与行为

- 客户端本地执行，不下发服务端 DDL/DCL。
- 复用现有连接选项路径，最终更新 `STscObj.optionInfo.timezone`。

#### 5.1.4 参数校验

支持：
- IANA 名称（如 `Asia/Shanghai`）
- 固定偏移（`Z` / `±HH` / `±HHMM` / `±HH:MM`）

校验策略：
- 先按固定偏移规则校验
- 失败后按内置 tzdata 校验 IANA 名称
- 不能仅以 `tzalloc(name)` 返回非 NULL 作为合法条件，需显式拒绝 `CST` 等无法唯一映射到 IANA 条目的模糊缩写
- 上述校验逻辑应抽成共享 helper，供 `SET TIMEZONE`、`TO_ISO8601`、`TO_CHAR`、`TIMETRUNCATE` 复用

### 5.2 F8: `SET FIRST_DAY_OF_WEEK`

#### 5.2.1 目标语法

```sql
SET FIRST_DAY_OF_WEEK <0..6>;
```

#### 5.2.2 数据结构

文件：`source/client/inc/clientInt.h`

在 `SOptionInfo` 增加：

```c
int8_t firstDayOfWeek;  // -1: unset, 0..6: session override
```

#### 5.2.3 执行行为

- 客户端本地执行
- 仅影响当前连接

### 5.3 F7: 服务端 `firstDayOfWeek` 配置

#### 5.3.1 配置注册

文件：`source/common/src/tglobal.c`

新增全局配置：
- 名称：`firstDayOfWeek`
- 范围：`0..6`
- 默认：`1`

#### 5.3.2 读取回退

服务端取值顺序：
- 连接级 `fdow`（若设置）
- 服务端全局 `tsFirstDayOfWeek`
- 默认 `1`

### 5.4 F1/F2: 普通查询列、SHOW/EXPLAIN 使用连接时区

#### 5.4.1 目标

所有客户端展示时间戳路径优先使用连接时区（L2）。

#### 5.4.2 方案

- 在 `TAOS_RES` 与相关结果上下文中确保可访问连接 `timezone_t`
- 时间戳字符串化统一调用带 `timezone_t` 的转换路径

说明：
- 现有基础设施已具备连接时区字段，核心工作是“展示路径收口”而非新增时区库能力。

### 5.5 F5: `TO_ISO8601` / `TO_CHAR` 时区扩展

#### 5.5.1 `TO_ISO8601`

当前：第二参数仅固定偏移。

目标：
- 保持旧偏移格式兼容
- 新增 IANA 名称支持
- 无参回退链改为连接时区优先（L2→L3→L5）

实现点：
- `source/libs/function/src/builtins.c` 扩展时区参数校验
- `source/libs/scalar/src/sclfunc.c` 对 IANA 每行做 `localtime_rz` 计算偏移

#### 5.5.2 `TO_CHAR`

当前：`TO_CHAR(ts, fmt)`

目标：
- 扩展到 `TO_CHAR(ts, fmt [, timezone])`
- `timezone` 支持 IANA/固定偏移
- 无第三参数时使用连接时区回退

实现点：
- builtins 注册由 `maxParamNum=2` 改为 `3`
- 增加第三参数校验与翻译
- scalar 层按传入或回退时区格式化

### 5.6 F6/F10: `TIMETRUNCATE` 扩展

#### 5.6.1 第三参数扩展

当前：仅 `0/1`

目标：
- 保持 `0/1` 兼容
- 新增字符串时区参数（IANA/offset）

实现点：
- builtins 第三参数从“固定整数”改为“整数或字符串”
- scalar `timeTruncateFunction` 根据参数类型分支

#### 5.6.2 单位扩展

当前代码单位校验仍受限。

目标：
- 支持自然单位：`n/q/y`（其中 `q` 视为已由待合并 PR 提供）
- 多倍数支持：`2q`, `3n`, `2y` 等

实现点：
- `validateTimeUnitParam` 从固定 `1x` 改为可解析多倍数自然单位
- 自然单位在“本地时区日历坐标系”截断

### 5.7 F9: `TIMETRUNCATE(..., 1w)` 对齐修正

当前：基于 epoch 取整导致周四对齐。

目标：
- 改为按 `firstDayOfWeek` 对齐到本地周起始日 00:00:00

实现点：
- 周截断逻辑改为“先本地化 -> 计算 wday/fdow 差值 -> 回退到周起点 -> 转回 UTC”

### 5.8 F11: `INTERVAL(..., q)`

约定：`q` 由待 rebase PR 提供，本文按“已支持”描述集成要求。

集成要求：
1. parser/translator 允许 `q`
2. `IS_CALENDAR_TIME_DURATION` 包含 `q`
3. executor 里 `q` 复用月窗口逻辑（`1q == 3n`）
4. 与 `SLIDING`、`OFFSET`、边界计算规则一致

### 5.9 F4: `TIMEZONE()` 增强

#### 5.9.1 目标语法

```sql
TIMEZONE()
TIMEZONE(0)
TIMEZONE(1)
```

#### 5.9.2 行为与执行位置

**无参数或参数为 0**：
- `TIMEZONE()` / `TIMEZONE(0)`：保持现有单字符串兼容
- 可在客户端执行，直接返回客户端时区字符串

**参数为 1**：
- `TIMEZONE(1)`：返回 JSON 字符串，包含三层时区信息 (`session/client/server`)
- **需下发服务端执行**（新增）：原因是需要访问服务端的 `tsTimezoneStr` 配置与 `firstDayOfWeek` 等信息
- 服务端收集三层数据后组装 JSON 返回

#### 5.9.3 实现点

- builtins 参数从 0 参改为可选 1 参（取值 0/1）
- scalar `timezoneFunction` 按参数分支：
  - 参数 `0` 或无参：客户端执行，返回客户端时区字符串
  - 参数 `1`：标记为需服务端执行的函数，通过服务端 scalar 函数收集并输出三层信息
- 服务端对应的 scalar 函数可访问 `tsTimezoneStr` 全局配置与请求上下文中的时区信息

## 6. 数据结构与接口变更

### 6.1 客户端结构

文件：`source/client/inc/clientInt.h`

- `SOptionInfo.firstDayOfWeek`（新增）

### 6.2 请求上下文

在查询请求/执行上下文中保证可携带：
- `timezone_t tz`（已存在）
- `int8_t firstDayOfWeek`（新增）
- 必要时附带 client tz 字符串用于 `TIMEZONE(1)`

### 6.3 SQL 与函数签名

新增：
- `SET TIMEZONE '<tz>'`
- `SET FIRST_DAY_OF_WEEK <0..6>`

扩展：
- `TO_CHAR(ts, fmt [, tz])`
- `TIMETRUNCATE(ts, unit [, tz_or_flag])`
- `TIMEZONE([all])`

## 7. 错误码设计

FS 中定义：
- `0x2600`: Invalid timezone
- `0x2601`: Invalid firstDayOfWeek

实现建议：
- 不复用当前 parser 的 `0x2600/0x2601` 编号（现语义已占用）
- 在函数/参数错误码域新增专用错误码，文案保持 FS 语义

## 8. 兼容性与风险

### 8.1 兼容性

1. 旧 SQL 默认不受影响。
2. 仅在显式使用新语法/新参数时行为改变。
3. `TIMEZONE()` 默认返回保持兼容。

### 8.2 高风险行为变更

1. `TIMETRUNCATE(..., 1w)` 对齐基准变化（周四 -> firstDayOfWeek）
2. `INTERVAL(..., w)` 周起始日变化

需在版本说明与升级指南明确提示。

### 8.3 执行路径变化

`TIMEZONE()` 函数执行位置变化：

- `TIMEZONE()` / `TIMEZONE(0)`：可在客户端执行，返回客户端时区字符串
- `TIMEZONE(1)`：**需下发服务端执行**，以访问服务端的 `tsTimezoneStr` 和其他全局配置

这要求：
1. Planner 识别 `TIMEZONE(1)` 为"需服务端参与"的表达式
2. Server-side scalar 函数 `timezoneFunction` 支持参数 `1` 的分支逻辑
3. 确保请求路径可携带必要的时区与周起始日信息下发到服务端

## 9. 实施顺序（建议）

1. P1：语法与配置基础设施
- `SET TIMEZONE`
- `SET FIRST_DAY_OF_WEEK`
- 服务端 `firstDayOfWeek`

2. P2：展示口径统一
- F1/F2 查询结果、SHOW/EXPLAIN

3. P3：函数扩展
- `TO_ISO8601` IANA
- `TO_CHAR` 第三参数
- `TIMETRUNCATE` 第三参数字符串

4. P4：自然单位与周对齐
- `TIMETRUNCATE` 的 `n/q/y` 与 `1w` 修正

5. P5：季度集成收口
- rebase 合并 `q` PR
- 与 parser/translator/executor 联调验收

6. P6：`TIMEZONE(1)` 服务端扩展
- 因涉及服务端执行路径，排在后期并与 P2 协同完成

## 10. 验收要点

1. 语法：`SET TIMEZONE` / `SET FIRST_DAY_OF_WEEK` 可解析并生效。
2. 回退链：L1-L5 与 `firstDayOfWeek` 回退链符合 FS。
3. 函数：
- `TO_ISO8601/TO_CHAR` 支持 IANA
- `TIMETRUNCATE` 支持字符串时区与自然单位
- `TIMEZONE()` / `TIMEZONE(0)` 返回单字符串（客户端执行）
- `TIMEZONE(1)` 返回三层信息 JSON（服务端执行，正确收集 session/client/server 数据）
4. 周/季度：
- `1w` 对齐符合 `firstDayOfWeek`
- `INTERVAL(1q)` 与 `INTERVAL(3n)` 等价（按 FS 语义）
5. 执行路径：
- 服务端可访问连接级时区与 `firstDayOfWeek`
- 必要时下发请求到服务端（如 `TIMEZONE(1)`、`TIMETRUNCATE` 带 IANA 等）

## 11. 参考文件

- `timezone-FS.md`
- `source/client/src/clientMain.c`
- `source/client/inc/clientInt.h`
- `source/libs/function/src/builtins.c`
- `source/libs/scalar/src/sclfunc.c`
- `source/libs/parser/inc/sql.y`
- `source/libs/parser/src/parAstCreater.c`
- `source/libs/parser/src/parTranslater.c`
- `include/common/ttime.h`
