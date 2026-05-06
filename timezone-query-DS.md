# 时区与查询改造 - 详细设计说明书（DS）

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-27 | - | 0.1 | AI | 初稿 |

## 2. 引言

### 2.1 目的

本文档为「时区与查询改造」的详细设计说明书，基于需求规格说明书（RS）`timezone-rs.md` 编写。文档描述各功能点（F1-F11）在 TDengine 代码中的具体实现方案，包括模块变更、数据结构修改、接口变化和交互流程。

### 2.2 范围

覆盖以下模块的改造：
- **parser**：新增 `SET TIMEZONE`、`SET FIRST_DAY_OF_WEEK` 语法；TIMETRUNCATE 第三参数扩展；INTERVAL 新增 `q` 单位
- **scalar**：TIMETRUNCATE 自然单位截断（重点新增 `n`/`q`/`y`，并统一 `d`/`w` 语义）；TO_ISO8601 IANA 时区支持
- **executor**：INTERVAL `q` 季度窗口切分
- **client**：连接级时区用于普通列读取与 SHOW/EXPLAIN 格式化
- **config**：新增 `firstDayOfWeek` 配置参数
- **function/builtins**：TIMEZONE() 函数增强

### 2.3 受众

TDengine 内核开发工程师、测试工程师、文档工程师。

### 2.4 RS 需求文档勘误

经代码审查，RS 中以下描述与当前实现不符，本设计以代码实际为准：

| RS 描述 | 实际情况 | 设计调整 |
| --- | --- | --- |
| 2.1 #6："INTERVAL 不支持 `w`/`n`/`y`/`q`" | INTERVAL 已支持 `w`（作为 7d 固定单位）、`n`（月）、`y`（年），不支持 `q`（季度） | F11 仅需新增 `q` 季度单位 |
| F11："新增 `w`/`n`/`q`/`y`" | `w`/`n`/`y` 已存在 | F11 实际改动仅为新增 `q` |
| TIMETRUNCATE 第三参数默认语义 | RS 称"0=UTC, 1=连接时区"，实际代码 `ignoreTz` 默认 `true`（即默认不使用时区），传 `1` 时仅 `d`/`w` 补偿时区偏移 | 向后兼容设计需精确匹配当前行为 |

## 3. 术语

同 RS 第 2.4 节。

## 4. 概述

### 4.1 架构

改造涉及的核心模块及数据流：

```
┌─────────────────────────────────────────────────────┐
│                    Client (taosc)                    │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │ SET TIMEZONE │  │ optionInfo   │  │ 时间戳格式化│  │
│  │ SET FDOW    │  │ .timezone    │  │ (F1/F2)    │  │
│  │ (F3/F8)     │  │ .fdow (新增) │  │            │  │
│  └──────┬──────┘  └──────┬───────┘  └─────┬──────┘  │
│         │  SQL 发送到服务端  │  连接时区用于展示  │        │
└─────────┼────────────────┼────────────────┼──────────┘
          │                │                │
┌─────────▼────────────────▼────────────────▼──────────┐
│                    Server (taosd)                     │
│  ┌────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐  │
│  │ Parser │→│ Planner  │→│ Executor │→│ Scalar │  │
│  │ sql.y  │  │          │  │ interval │  │ trunc  │  │
│  │        │  │          │  │ q 窗口   │  │ n/q/y  │  │
│  └────────┘  └──────────┘  └──────────┘  └────────┘  │
│  ┌────────────┐  ┌──────────────────┐                 │
│  │ tglobal.c  │  │ builtins.c       │                 │
│  │ firstDOW   │  │ TO_ISO8601 IANA  │                 │
│  └────────────┘  │ TIMEZONE() 增强  │                 │
│                  └──────────────────┘                 │
└──────────────────────────────────────────────────────┘
```

### 4.2 技术

- C11, Lemon parser generator
- POSIX `timezone_t`/`tzalloc()`/`localtime_rz()` 用于 IANA 时区解析
- 内部哈希表 `pTimezoneMap` 用于 timezone_t 缓存

### 4.3 依赖项

- IANA tzdata（系统 `/usr/share/zoneinfo`）
- Windows 上 `tzalloc` 不可用，需通过 ICU 或自行实现（当前 Windows 不支持连接级时区，本次不变）

## 5. 设计考虑

### 5.1 假设和限制

1. Windows 平台暂不支持连接级时区（维持现有 `TSDB_CODE_NOT_SUPPORTTED_IN_WINDOWS`）
2. `firstDayOfWeek` 仅影响 `w` 单位，不影响 `d`/`n`/`y`/`q`
3. 季度定义固定为 Q1=1-3月、Q2=4-6月、Q3=7-8月、Q4=10-12月（公历，不可配置）
4. 多倍数月/季度/年单位（如 `2q`/`3n`）以 Unix epoch（1970-01-01 UTC）为对齐基准

### 5.2 设计原则

- **向后兼容**：所有默认行为不变
- **最小侵入**：复用现有 `timezone_t` 基础设施
- **类型复用**：TIMETRUNCATE 第三参数通过运行时类型判断（整数 vs 字符串）而非语法分支

### 5.3 风险和缓解

| 风险 | 缓解 |
| --- | --- |
| TIMETRUNCATE 自然单位截断在 DST 边界产生歧义 | 统一使用 `localtime_rz`/`mktime_z` 在本地时间坐标系截断后换回 UTC |
| INTERVAL `q` 窗口与已有 `n`/`y` 逻辑不一致 | 复用 `n` 的月份对齐逻辑，`q` 视为 `3n` 的别名 |
| `firstDayOfWeek` 在服务端与客户端不同步 | 通过连接参数传递，回退链 L2 → L4 → 默认值 1 |

## 6. 详细设计

### 6.1 F3: SET TIMEZONE SQL 语法

#### 6.1.1 Parser 改动

**文件**：`source/libs/parser/inc/sql.y`

新增 `SET TIMEZONE` 产生式：

```
cmd ::= SET TIMEZONE NK_STRING(A).  { pCxt->pRootNode = createSetTimezoneStmt(pCxt, &A); }
```

需要新增 token `TIMEZONE` 到保留字列表（如果尚未存在为 token——当前 `TIMEZONE` 是 `noarg_func` 的一个值，需确认是否可复用或需要新增独立 token）。

**文件**：`source/libs/parser/src/parAstCreater.c`

新增 `createSetTimezoneStmt()` 函数：

```c
SNode* createSetTimezoneStmt(SAstCreateContext* pCxt, const SToken* pTz) {
    // 创建 SSetTimezoneStmt 节点
    // 去除引号，验证 timezone 字符串合法性（IANA 或 offset）
    // 调用 tzalloc() 验证，失败报 0x2600 错误
}
```

**文件**：`include/common/tmsg.h` / `source/common/src/tmsg.c`

新增消息类型 `TDMT_MND_SET_TIMEZONE`（或复用客户端本地处理——SET TIMEZONE 仅影响连接级，不需要发送到服务端 mnode）。

**设计决策**：`SET TIMEZONE` 是纯客户端操作，不需要服务端参与。Parser 解析后，客户端直接调用 `setConnnectionTz()` 更新 `STscObj.optionInfo.timezone`。类似于 `USE db` 的处理方式。

#### 6.1.2 客户端执行

**文件**：`source/client/src/clientImpl.c`

在 `handleQueryAst()` 或类似入口中，识别 `QUERY_NODE_SET_TIMEZONE_STMT`，调用现有 `setConnnectionTz()` 设置连接时区。

#### 6.1.3 服务端传递

`SET TIMEZONE` 执行后，连接时区存储在 `STscObj.optionInfo.timezone` 中。后续 SQL 执行时，该时区通过请求消息中的 timezone 字段传递给服务端（现有机制已支持）。

### 6.2 F8: SET FIRST_DAY_OF_WEEK 语法

#### 6.2.1 Parser 改动

**文件**：`source/libs/parser/inc/sql.y`

```
cmd ::= SET FIRST_DAY_OF_WEEK NK_INTEGER(A).  { pCxt->pRootNode = createSetFirstDayOfWeekStmt(pCxt, &A); }
```

需要新增 token `FIRST_DAY_OF_WEEK`。由于含下划线的多词 token 不易处理，考虑使用 `FIRST_DAY_OF_WEEK` 作为单一 token（在 tokenizer 中识别），或改为 `SET FIRSTDAYOFWEEK`。

**推荐方案**：使用 `SET FIRST_DAY_OF_WEEK` 的形式，在 sql.y 中将 `FIRST_DAY_OF_WEEK` 定义为复合关键字序列：

```
cmd ::= SET FIRST_DAY_OF_WEEK NK_INTEGER(A). { ... }
```

其中 `FIRST`、`DAY`、`OF`、`WEEK` 分别为已有或新增的 token。注意 `OF` 和 `WEEK` 可能已存在。

#### 6.2.2 数据结构

**文件**：`source/client/inc/clientInt.h`

在 `SOptionInfo` 中新增字段：

```c
typedef struct SOptionInfo {
    timezone_t timezone;
    int8_t     firstDayOfWeek;  // 新增，0-6，默认 -1 表示未设置
} SOptionInfo;
```

#### 6.2.3 执行逻辑

客户端本地执行，验证值在 0-6 范围内，超出范围返回 `0x2601` 错误。值存储在 `STscObj.optionInfo.firstDayOfWeek` 中，通过请求消息传递到服务端。

### 6.3 F7: firstDayOfWeek 服务端配置

#### 6.3.1 配置注册

**文件**：`source/common/src/tglobal.c`

```c
int8_t tsFirstDayOfWeek = 1;  // 默认周一

// 在 taosAddCfg() 中注册：
cfgAddInt32(pCfg, "firstDayOfWeek", tsFirstDayOfWeek, 0, 6, CFG_SCOPE_SERVER, ...);
```

#### 6.3.2 回退链

在需要 firstDayOfWeek 的服务端计算中（TIMETRUNCATE、INTERVAL `w`）：

```c
int8_t getFirstDayOfWeek(SExecTaskInfo* pTaskInfo) {
    // L2: 连接级（从请求消息中获取）
    if (pTaskInfo->fdow >= 0) return pTaskInfo->fdow;
    // L4: 服务端全局
    return tsFirstDayOfWeek;
}
```

### 6.4 F1: 普通列读取使用连接时区

#### 6.4.1 当前行为

客户端在 `taos_print_row()` 和结果格式化中使用全局 `tsTimezone` 将 UTC int64 转为本地时间字符串。

#### 6.4.2 改造方案

**文件**：`source/client/src/clientSml.c`、`source/client/src/clientTmq.c`、`tools/shell/src/shellEngine.c`

在时间戳格式化路径中，将 `localtime_r()` 替换为 `localtime_rz(tz, ...)`，其中 `tz` 来自 `pTscObj->optionInfo.timezone`。如果 `timezone` 为 NULL，回退到全局行为（`localtime_r`）。

关键改动点：

1. `taos_print_row()` — 增加 timezone_t 参数或从 TLS 获取当前连接的 timezone
2. `shellFormatTimestamp()` — 使用连接级 timezone
3. REST/WebSocket 接口 — 从连接对象获取 timezone

**设计考量**：为避免 API 不兼容，不修改 `taos_print_row()` 签名。改为在 `TAOS_RES` 中增加对所属连接的引用，格式化时从中获取 timezone。

### 6.5 F2: SHOW/EXPLAIN 使用连接时区

#### 6.5.1 改造方案

SHOW 命令结果中的时间戳列，与普通列读取走相同的格式化路径（F1 改造后自动生效）。

EXPLAIN 输出中若有时间戳（如计划中的窗口边界展示），同样使用连接级 timezone 格式化。

### 6.6 F5: TO_ISO8601 支持 IANA 时区

#### 6.6.1 参数验证

**文件**：`source/libs/function/src/builtins.c`

修改 `validateTimezoneFormat()` 函数：

```c
int32_t validateTimezoneFormat(const SValueNode* pVal) {
    const char* tz = pVal->literal;
    int32_t     len = strlen(tz);

    // 1. 尝试现有 offset 格式验证
    if (isValidOffsetFormat(tz, len)) return TSDB_CODE_SUCCESS;

    // 2. 尝试 IANA 名称验证
    timezone_t t = tzalloc(tz);
    if (t != NULL) {
        tzfree(t);
        return TSDB_CODE_SUCCESS;
    }

    return TSDB_CODE_INVALID_TIMEZONE;
}
```

#### 6.6.2 运行时计算

**文件**：`source/libs/scalar/src/sclfunc.c`

修改 `toISO8601Function()`：

```c
// 当前：offsetOfTimezone(tz, &offset) 仅支持固定偏移
// 改造：
if (isIANA(tz)) {
    timezone_t tzt = tzalloc(tz);  // 或从缓存获取
    struct tm result;
    time_t    sec = ts / 1000;  // 按精度调整
    localtime_rz(tzt, &sec, &result);
    int32_t offset = result.tm_gmtoff;  // 动态 DST 感知偏移
    // 使用 offset 格式化 ISO8601 输出
} else {
    // 现有 offset 逻辑不变
    offsetOfTimezone(tz, &offset);
}
```

关键点：IANA 时区时，每个时间戳的偏移量可能不同（DST 切换），因此需要对每行数据计算一次。

### 6.7 F6: TIMETRUNCATE 第三参数扩展

#### 6.7.1 参数验证

**文件**：`source/libs/function/src/builtins.c`

修改 TIMETRUNCATE 的第三参数验证：

```c
// 当前：第三参数仅接受整数 0/1
// 改造：第三参数可以是整数 0/1（旧语义）或字符串（时区，新语义）
if (pParam3->type == TSDB_DATA_TYPE_INT) {
    // 旧语义，验证 0 或 1
} else if (pParam3->type == TSDB_DATA_TYPE_VARCHAR) {
    // 新语义，验证时区字符串（复用 validateTimezoneFormat）
}
```

#### 6.7.2 运行时

**文件**：`source/libs/scalar/src/sclfunc.c`

```c
void timeTruncateFunction(...) {
    // 判断第三参数类型
    if (isStringParam3) {
        timezone_t tz = resolveTz(param3Str);  // IANA 或 offset
        // 使用 tz 进行截断计算
    } else {
        // 现有 ignoreTz 逻辑不变
    }
}
```

### 6.8 F10: TIMETRUNCATE 支持 n/q/y 单位

#### 6.8.1 参数验证

**文件**：`source/libs/function/src/builtins.c`

修改 `validateTimeUnitParam()` 函数，扩展允许的单位字符集：

```c
// 当前允许：'b', 'u', 'a', 's', 'm', 'h', 'd', 'w'
// 新增允许：'n', 'q', 'y'
```

但多倍数时需注意：当前验证要求 literal 为 `1X` 格式（即系数固定为 1）。对于 `n`/`q`/`y`，支持多倍数（如 `2q`、`3n`）。需修改验证逻辑允许月/季度/年单位的多倍数。

#### 6.8.2 截断实现

**文件**：`source/libs/scalar/src/sclfunc.c`

在 `timeTruncateFunction()` 中新增 `n`/`q`/`y` 处理分支：

```c
switch (timeUnit) {
    case 'n': {  // 月截断
        struct tm local;
        time_t sec = tsVal / factor;  // factor = 精度转秒
        localtime_rz(tz, &sec, &local);
        int32_t totalMonths = (local.tm_year + 1900) * 12 + local.tm_mon;
        totalMonths = (totalMonths / multiple) * multiple;  // 多倍数对齐
        local.tm_year = totalMonths / 12 - 1900;
        local.tm_mon  = totalMonths % 12;
        local.tm_mday = 1;
        local.tm_hour = local.tm_min = local.tm_sec = 0;
        tsVal = mktime_z(tz, &local) * factor;
        break;
    }
    case 'q': {  // 季度截断，复用月逻辑
        // 与 'n' 相同，但 multiple *= 3
        struct tm local;
        time_t sec = tsVal / factor;
        localtime_rz(tz, &sec, &local);
        int32_t totalMonths = (local.tm_year + 1900) * 12 + local.tm_mon;
        int32_t monthMultiple = multiple * 3;
        totalMonths = (totalMonths / monthMultiple) * monthMultiple;
        local.tm_year = totalMonths / 12 - 1900;
        local.tm_mon  = totalMonths % 12;
        local.tm_mday = 1;
        local.tm_hour = local.tm_min = local.tm_sec = 0;
        tsVal = mktime_z(tz, &local) * factor;
        break;
    }
    case 'y': {  // 年截断
        struct tm local;
        time_t sec = tsVal / factor;
        localtime_rz(tz, &sec, &local);
        int32_t year = local.tm_year + 1900;
        year = (year / multiple) * multiple;
        local.tm_year = year - 1900;
        local.tm_mon  = 0;
        local.tm_mday = 1;
        local.tm_hour = local.tm_min = local.tm_sec = 0;
        tsVal = mktime_z(tz, &local) * factor;
        break;
    }
}
```

### 6.9 F11: INTERVAL 支持 q（季度）单位

#### 6.9.1 Parser 改动

**文件**：`source/libs/parser/src/parAstCreater.c`

在 `createDurationValueNode()` 的允许 unit 列表中新增 `'q'`。

**文件**：`include/common/ttime.h`

修改 `IS_CALENDAR_TIME_DURATION` 宏：

```c
// 当前：#define IS_CALENDAR_TIME_DURATION(u) ((u) == 'n' || (u) == 'y')
// 改为：
#define IS_CALENDAR_TIME_DURATION(u) ((u) == 'n' || (u) == 'y' || (u) == 'q')
```

#### 6.9.2 Translator 改动

**文件**：`source/libs/parser/src/parTranslater.c`

在 `checkIntervalWindow()` 中对 `q` 的处理：

```c
if (pInterval->unit == 'q') {
    // 验证：sliding 如果指定，也必须是自然单位且 <= interval
    // q 视为 3 个月，转换 interval 值
    // 复用 'n' 的逻辑路径
}
```

#### 6.9.3 Executor 改动

**文件**：`source/libs/executor/src/timewindowoperator.c`

在窗口边界计算中新增 `q` 分支：

```c
if (pInterval->intervalUnit == 'q') {
    // 将 interval 转换为月数（interval * 3）
    // 复用现有月级窗口边界计算逻辑
    int32_t monthInterval = pInterval->interval * 3;
    // ... 月级对齐逻辑
}
```

### 6.10 F4: TIMEZONE() 函数增强

#### 6.10.1 返回值格式

**文件**：`source/libs/scalar/src/sclfunc.c`

修改 `timezoneFunction()` 返回三行信息：

```
Connection: Asia/Shanghai (UTC+8)
Client: Asia/Shanghai (UTC+8)
Server: America/New_York (UTC-5, DST)
```

或返回 JSON 格式：

```json
{"connection":"Asia/Shanghai","client":"Asia/Shanghai","server":"America/New_York"}
```

**设计决策**：为保持 VARCHAR 返回值的简洁性和可解析性，推荐使用分号分隔：

```
conn:Asia/Shanghai;client:Asia/Shanghai;server:America/New_York
```

**注意**：需要服务端将 `tsTimezoneStr`（服务端全局时区）通过某种方式传递给 scalar 函数执行上下文。当前 `timezoneFunction` 的 `pInput` 中有 `tz`（连接级）和全局 `tsTimezoneStr`，还需要区分客户端全局时区。

实现方案：
1. 连接级时区：从 `pInput->tz` 获取
2. 服务端时区：从全局 `tsTimezoneStr` 获取
3. 客户端时区：需要在请求消息中新增字段传递客户端全局时区

### 6.11 F9: TIMETRUNCATE 1w 行为修正

#### 6.11.1 当前行为

`TIMETRUNCATE(ts, 1w)` 当前按 UTC epoch 以来的 7 天整除，再加时区偏移。这导致截断结果不一定是某个周一。

#### 6.11.2 改造后行为

```c
case 604800: {  // 1w = 7 days
    struct tm local;
    time_t sec = tsVal / factor;
    localtime_rz(tz, &sec, &local);
    // 计算当前是一周中的第几天（相对于 firstDayOfWeek）
    int32_t wday = local.tm_wday;  // 0=Sunday
    int32_t fdow = getFirstDayOfWeek();
    int32_t daysSinceStart = (wday - fdow + 7) % 7;
    // 回退到本周起始日
    local.tm_mday -= daysSinceStart;
    local.tm_hour = local.tm_min = local.tm_sec = 0;
    tsVal = mktime_z(tz, &local) * factor;
    break;
}
```

多倍数 `Nw` 的处理：先找到本周起始日，再按 N 周对齐到 epoch 后的周计数。

### 6.12 F7: firstDayOfWeek 对 INTERVAL w 的影响

**文件**：`source/libs/executor/src/timewindowoperator.c`

在 `w` 单位的窗口起始计算中，将硬编码的周一（`tm_wday == 1`）替换为从连接或全局配置获取的 `firstDayOfWeek`。

## 7. 关键数据结构

### 7.1 SOptionInfo 扩展

```c
// source/client/inc/clientInt.h
typedef struct SOptionInfo {
    timezone_t timezone;         // 连接级时区（已有）
    int8_t     firstDayOfWeek;   // 连接级一周起始日，-1=未设置（新增）
} SOptionInfo;
```

### 7.2 请求消息扩展

在查询请求消息中增加 `firstDayOfWeek` 字段（int8_t），跟随 timezone 一起传递到服务端。

### 7.3 Scalar 执行上下文扩展

```c
// SScalarParam 或其上下文中需要新增：
int8_t firstDayOfWeek;   // 用于 TIMETRUNCATE 1w
char   clientTzStr[64];  // 客户端全局时区字符串，用于 TIMEZONE() 增强
```

## 8. 接口规范

### 8.1 新增 SQL 语法

| SQL | 语义 | 执行位置 |
| --- | --- | --- |
| `SET TIMEZONE '<tz>'` | 设置连接级时区 | 客户端本地 |
| `SET FIRST_DAY_OF_WEEK <0-6>` | 设置连接级一周起始日 | 客户端本地 |

### 8.2 修改的函数签名

| 函数 | 变更 |
| --- | --- |
| `TO_ISO8601(ts [, tz])` | `tz` 参数新增 IANA 名称支持 |
| `TIMETRUNCATE(ts, unit [, tz_or_flag])` | 第三参数新增字符串类型（时区字符串） |
| `TIMEZONE()` | 返回值扩展为包含三级时区信息 |

### 8.3 新增配置参数

| 参数 | 端 | 类型 | 范围 | 默认值 |
| --- | --- | --- | --- | --- |
| `firstDayOfWeek` | 服务端 | int | 0-6 | 1 |

### 8.4 新增错误码

| 错误码 | 消息 |
| --- | --- |
| `0x2600` | `Invalid timezone: '<value>'` |
| `0x2601` | `Invalid firstDayOfWeek: <value>, must be 0-6` |

## 9. 安全考虑

1. `SET TIMEZONE` 接受用户输入字符串，通过 `tzalloc()` 验证合法性，拒绝非法输入，避免路径注入（IANA 时区名对应 `/usr/share/zoneinfo/` 下的文件路径）
2. 时区字符串长度限制：最大 64 字节，超长截断
3. `firstDayOfWeek` 严格限制 0-6 范围，整数输入无注入风险

## 10. 性能和可扩展性

### 10.1 性能影响

| 场景 | 影响 | 缓解 |
| --- | --- | --- |
| TO_ISO8601 IANA 时区 | 每行需调用 `localtime_rz()`，比固定偏移慢 | timezone_t 对象缓存在 `pTimezoneMap` 中，避免重复 `tzalloc()` |
| TIMETRUNCATE n/q/y | 每行需 `localtime_rz()` + `mktime_z()` | 同上；且自然单位通常用于聚合场景，数据量受 group by 约束 |
| INTERVAL q | 窗口边界计算新增 q 分支 | 复用 n 的代码路径，无额外开销 |

### 10.2 可扩展性

本次设计为后续扩展预留空间：
- 时区层级体系（L1-L5）可用于未来新增的时区感知函数
- `firstDayOfWeek` 框架可扩展到其他日历配置（如财年起始月）

## 11. 部署和配置

### 11.1 新增配置

服务端 `taos.cfg` 新增可选参数 `firstDayOfWeek`，默认值 1 保证向后兼容。

### 11.2 版本兼容性

- 旧客户端连接新服务端：无影响，不传 firstDayOfWeek 时服务端使用配置默认值
- 新客户端连接旧服务端：`SET TIMEZONE` / `SET FIRST_DAY_OF_WEEK` 语法在客户端执行，不影响旧服务端；但 TIMETRUNCATE `n`/`q`/`y` 和 INTERVAL `q` 需要新服务端支持，旧服务端将返回语法错误
- 回滚：删除 `firstDayOfWeek` 配置项即可恢复默认行为

## 12. 实现阶段建议

| 阶段 | 功能点 | 优先级 | 说明 |
| --- | --- | --- | --- |
| P1 | F3, F7, F8 | 高 | 基础设施：SET TIMEZONE/FDOW 语法 + firstDayOfWeek 配置 |
| P2 | F1, F2 | 高 | 连接时区统一：普通列/SHOW/EXPLAIN 展示 |
| P3 | F5, F6 | 高 | 函数时区增强：TO_ISO8601 IANA + TIMETRUNCATE tz 参数 |
| P4 | F10, F9 | 中 | TIMETRUNCATE 自然单位 + 1w 修正 |
| P5 | F11 | 中 | INTERVAL q 季度支持 |
| P6 | F4 | 低 | TIMEZONE() 增强 |

## 13. 参考资料

| 文档 | 说明 |
| --- | --- |
| `timezone-rs.md` | 需求规格说明书 |
| `source/client/inc/clientInt.h` | 客户端连接数据结构 |
| `source/libs/function/src/builtins.c` | 函数注册与参数验证 |
| `source/libs/scalar/src/sclfunc.c` | Scalar 函数实现 |
| `source/libs/parser/inc/sql.y` | SQL 语法定义 |
| `source/libs/executor/src/timewindowoperator.c` | INTERVAL 窗口执行 |
| `source/common/src/tglobal.c` | 全局配置注册 |
| POSIX `tzalloc(3)` / `localtime_rz(3)` | IANA 时区 API |
