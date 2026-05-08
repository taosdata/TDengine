# 时区与查询改造 - 执行计划

## 1. 修订记录

| 编写日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2026-05-06 | 0.1 | AI | 初稿，基于 FS v0.1 与 DS v0.2 |
| 2026-05-06 | 0.2 | AI | 对齐 FS/DS 评审：修正错误码已占用说明；补充 CST/单数字小时拒绝机制；Task 1.1 验收标准节归位；Task 3.3 统一 use_current_timezone 命名；Task 4.4 明确 DAYOFWEEK/WEEKDAY 不变及 WEEK mode L1 优先级；新增 Task 2.5 TODAY() 回退链；测试计划补 test_interval_natural / test_today_tz；Task 6.1 补 L2 未设置时 session 字段验收 |
| 2026-05-06 | 0.3 | AI | Review 修正：明确错误码推荐范围 0x26B2+；补充 Task 1.5 涉及结构体名称；Task 2.1 补充涉及文件；Task 3.1/3.2 补充当前代码缺陷说明；Task 3.3 补充内部参数索引映射说明；Task 4.1 限定 validateTimeUnitParam 修改范围；Task 4.2 补充 Nw 计算示例；Task 4.3 拆分 d 单位子步骤并补充涉及文件；Task 4.4 补充 MySQL week mode 说明；Task 6.1 明确 TIMEZONE(0) 执行位置；测试计划补充 DAYOFWEEK/WEEKDAY/NOW 回归组与 TIMETRUNCATE 多倍数组；风险表补充执行位置风险 |
| 2026-05-06 | 0.4 | AI | 统一 TO_ISO8601/TO_CHAR 无参回退链为 `L2→L3→L5`：这两个函数始终在客户端执行，无需访问服务端 L4；移除服务端执行方案说明；同步修改 FS/DS/Plan 三处文档 |
| 2026-05-06 | 0.5 | AI | 评审收口：确认 TO_ISO8601/TO_CHAR/TODAY 为客户端执行函数；修正 TO_ISO8601 当前缺陷为 translator 默认折叠固定偏移；明确 `d/w` 不纳入 `IS_CALENDAR_TIME_DURATION`，改走专用日历分支 |
| 2026-05-06 | 0.6 | AI | 代码比照修正：Task 3.1 明确 `validateTimezoneFormat` 实为纯格式校验（不含 tzalloc）及 scalar inputNum 死代码问题；Task 3.3 修正内部参数顺序（precision 在 timezoneStr 之前），补充 translator 也需停止注入固定偏移；Task 3.2 补充 `toCharFunction` 无 translator 注入问题，明确第三参数的 scalar 分支实现路径 |
| 2026-05-06 | 0.7 | AI | 增补测试优先执行节奏：先生成单元/pytest 测试清单与测试代码，待人工评审通过后再进入功能开发与全量回归 |
| 2026-05-07 | 0.8 | AI | 补充 9.1 当前可编译单元测试与阻塞项说明：增强 common 自然单位/周边界断言；记录 parser 新语法、共享 timezone helper、TIMEZONE(1) 组装与消息透传因功能未落地暂无法补充 |

## 2. 概述

本文档是「时区与查询改造」的详细执行计划，基于 `timezone-FS.md`（行为规格）和 `timezone-query-DS.md`（设计方案）细化为可执行的开发任务。

### 2.1 前置假设

1. 季度单位 `q` 已在另一个 PR 中实现，将通过 rebase 合入当前分支。
2. 现有 `timezone_t/tzalloc/localtime_rz/mktime_z` 基础设施可用。
3. 连接级时区 C API `taos_options_connection` 已存在。

### 2.2 作用域约束

1. 普通查询链路中，服务端仅需要感知连接级时区（session timezone / L2）。
2. 客户端全局时区（L3）仅用于客户端本地展示与兼容回退，不作为普通查询链路的服务端协议透传目标。
3. `TIMEZONE(1)` 作为诊断接口可返回三层时区（session/client/server）。

### 2.3 规范优先级约定

1. 本执行计划以 `timezone-FS.md` 为唯一行为基准；`timezone-query-DS.md` 用于实现路径参考。
2. 关于 `TO_ISO8601(ts)` / `TO_CHAR(ts, fmt)` 无参回退链，这两个函数已确认始终在客户端执行，回退链为 `L2 → L3 → L5`（与客户端展示一致）。`TODAY()` 也已确认走客户端执行路径，回退链同样为 `L2 → L3 → L5`。`TIMETRUNCATE(ts, unit)` 在服务端执行，回退链为 `L2 → L4 → L5`。
3. 本文中所有相关设计、实现与测试项，均以该最终口径为准。

### 2.4 总体分期

| 阶段 | 内容 | 依赖 |
| --- | --- | --- |
| P1 | 语法与配置基础设施 | 无 |
| P2 | 展示口径统一 | P1 |
| P3 | 函数扩展（TO_ISO8601/TO_CHAR/TIMETRUNCATE 时区参数） | P1 |
| P4 | 自然单位与周对齐 | P1, P3 |
| P5 | 季度集成收口 | P4 + rebase |
| P6 | TIMEZONE(1) 服务端扩展 | P1, P2 |

### 2.5 执行节奏与评审门禁

1. 本项目按“测试先行”推进：先落单元测试与 pytest 用例，再开始功能开发。
2. 第一轮交付目标是测试设计与测试代码，而不是实现代码；测试应尽量覆盖本计划第 9 章列出的功能点、回退链、DST 边界和兼容性约束。
3. 测试生成完成后，先由你检查测试内容是否正确、是否遗漏关键场景；未获得评审结论前，不进入实现阶段。
4. 只有在测试评审通过后，才进入对应功能开发、局部验证与最终全量回归。

---

## 3. P1：语法与配置基础设施

### 3.1 Task 1.1：`SET TIMEZONE` 语法

**目标**：实现 `SET TIMEZONE '<timezone_string>'` SQL 语法。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/libs/parser/inc/sql.y` | 新增 `SET TIMEZONE` 产生式 |
| `source/libs/parser/src/parAstCreater.c` | 新增 `createSetTimezoneStmt()` 构造函数 |
| `include/libs/nodes/querynodes.h` | 新增 `QUERY_NODE_SET_TIMEZONE_STMT` 节点类型 |
| `source/client/src/clientMain.c` | 客户端本地执行分支，更新 `STscObj.optionInfo.timezone` |

**实现步骤**：

1. 在 `querynodes.h` 注册新节点类型 `QUERY_NODE_SET_TIMEZONE_STMT`
2. 在 `sql.y` 添加语法规则：
   ```
   cmd ::= SET TIMEZONE NK_STRING(A). { pCxt->pRootNode = createSetTimezoneStmt(pCxt, &A); }
   ```
3. 在 `parAstCreater.c` 实现 `createSetTimezoneStmt()`：
   - 解析时区字符串
   - 校验：先按固定偏移规则（`Z`/`±HH`/`±HHMM`/`±HH:MM`），失败后用内置 tzdata 校验 IANA 名称
   - 固定偏移中 `±HH` 形式的小时部分必须是两位数字，`+8`/`-4` 等单数字小时写法视为非法
   - 偏移范围限制 `-14:00` ~ `+14:00`，小时为 14 时分钟必须为 00
   - IANA 名称须在内置 tzdata 中存在；不能仅靠 `tzalloc()` 返回非 NULL 判定合法（某些平台 `tzalloc("CST")` 可能成功），需显式拒绝所有无法明确映射到唯一 IANA 条目的模糊缩写
   - 将上述校验逻辑抽成共享 helper，供 `SET TIMEZONE`、`TO_ISO8601`、`TO_CHAR`、`TIMETRUNCATE` 复用，确保所有入口对模糊缩写、偏移范围、非法格式的处理一致
   - 失败返回 `TSDB_CODE_PAR_INVALID_TIMEZONE`（具体编号由 Task 1.4 分配）
4. 在客户端执行路径识别该节点，调用 `taos_options_connection` 等价逻辑更新连接时区
5. 该语句不下发服务端

**验收标准**：

- `SET TIMEZONE 'Asia/Shanghai'` 可解析、执行，后续查询使用该时区
- `SET TIMEZONE '+08:00'` / `'+0800'` / `'+08'` / `'Z'` 均可正常设置
- `SET TIMEZONE '+8'` / `'-4'`（单数字小时）返回 `TSDB_CODE_PAR_INVALID_TIMEZONE`
- `SET TIMEZONE 'CST'` 返回 `TSDB_CODE_PAR_INVALID_TIMEZONE`（模糊缩写须显式拒绝）
- `SET TIMEZONE '+14:30'` 返回 `TSDB_CODE_PAR_INVALID_TIMEZONE`
- 不影响其他连接

---

### 3.2 Task 1.2：`SET FIRST_DAY_OF_WEEK` 语法

**目标**：实现 `SET FIRST_DAY_OF_WEEK <0..6>` SQL 语法。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/libs/parser/inc/sql.y` | 新增产生式 |
| `source/libs/parser/src/parAstCreater.c` | 新增构造函数 |
| `include/libs/nodes/querynodes.h` | 新增节点类型 |
| `source/client/inc/clientInt.h` | `SOptionInfo` 增加 `int8_t firstDayOfWeek` 字段 |
| `source/client/src/clientMain.c` | 客户端本地执行 |

**实现步骤**：

1. 在 `SOptionInfo` 新增 `int8_t firstDayOfWeek`，初始值 `-1`（表示未设置）
2. 在 `sql.y` 添加语法规则：
   ```
   cmd ::= SET FIRST_DAY_OF_WEEK NK_INTEGER(A). { pCxt->pRootNode = createSetFirstDayOfWeekStmt(pCxt, &A); }
   ```
3. 校验范围 0-6，超出返回 `TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK`（具体编号由 Task 1.4 分配）
4. 客户端本地执行，更新 `STscObj.optionInfo.firstDayOfWeek`

**验收标准**：

- `SET FIRST_DAY_OF_WEEK 0` ~ `SET FIRST_DAY_OF_WEEK 6` 均可执行
- `SET FIRST_DAY_OF_WEEK 7` / `SET FIRST_DAY_OF_WEEK -1` 返回 `TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK`
- 仅影响当前连接

---

### 3.3 Task 1.3：服务端 `firstDayOfWeek` 配置

**目标**：在服务端注册 `firstDayOfWeek` 全局配置项。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/common/src/tglobal.c` | 注册配置项 |
| `include/common/tglobal.h` | 声明 `extern int8_t tsFirstDayOfWeek` |

**实现步骤**：

1. 在 `tglobal.c` 声明全局变量 `int8_t tsFirstDayOfWeek = 1`
2. 在配置注册表中添加：
   - 名称：`firstDayOfWeek`
   - 类型：`CFG_DTYPE_INT8`
   - 范围：`0..6`
   - 默认值：`1`
3. 支持 `ALTER ALL DNODES 'firstDayOfWeek' '<value>'` 动态修改
4. 拒绝单节点 `ALTER DNODE N 'firstDayOfWeek'`

**验收标准**：

- `taos.cfg` 中可配置 `firstDayOfWeek 0` ~ `6`
- `ALTER ALL DNODES 'firstDayOfWeek' '0'` 动态生效
- `ALTER DNODE 1 'firstDayOfWeek' '0'` 被拒绝

---

### 3.4 Task 1.4：分配专用错误码并注册

**目标**：分配新的专用错误码，承载 FS 中 `Invalid timezone` / `Invalid firstDayOfWeek` 语义。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `include/util/taoserror.h` | 定义错误码宏 |
| `source/util/src/terror.c` | 注册错误消息 |

**实现步骤**：

1. 0x2600/0x2601 已被现有 parser 语法错误码占用（`TSDB_CODE_PAR_SYNTAX_ERROR` / `TSDB_CODE_PAR_INCOMPLETE_SQL`），必须在函数/参数错误码域分配新的专用错误码编号，不得复用现有编号。当前 parser 范围最高已用编号为 `0x26B1`，推荐从 `0x26B2` 起分配
2. 定义：
   - `TSDB_CODE_PAR_INVALID_TIMEZONE`（建议 `0x26B2`） → `"Invalid timezone: '%s'"`
   - `TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK`（建议 `0x26B3`） → `"Invalid firstDayOfWeek: %d, must be 0-6"`
3. 在中英文错误码文档中同步更新
4. 同步更新 FS 第 12 章错误码编号（当前 FS 仍写为 0x2600/0x2601，与实际占用冲突）

**验收标准**：

- 非法时区字符串返回正确错误码和消息
- 非法 firstDayOfWeek 返回正确错误码和消息

---

### 3.5 Task 1.5：请求上下文携带 firstDayOfWeek

**目标**：确保查询请求/执行上下文可携带连接级 `firstDayOfWeek`。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `include/common/tmsg.h`（`SSubQueryMsg` 结构体） | 增加 `int8_t firstDayOfWeek` 字段 |
| `include/common/tmsg.h`（`SInterval` 结构体） | 增加 `int8_t firstDayOfWeek` 字段 |
| `source/libs/scalar/inc/sclInt.h`（`SScalarParam` 或类似） | 透传 `firstDayOfWeek` 到 scalar 函数 |
| translator/planner 上下文 | 透传该字段 |

**实现步骤**：

1. 在 `SSubQueryMsg` 和 `SInterval` 中增加 `firstDayOfWeek` 字段，并完成序列化/反序列化代码
2. 客户端发送请求时，仅透传连接级覆盖值：
   - 已执行 `SET FIRST_DAY_OF_WEEK` 时，填充 `0..6`
   - 未设置时，填充 unset sentinel（如 `-1`）
3. 服务端执行器读取请求上下文后，再按回退链完成解析：连接级 override → 服务端 `tsFirstDayOfWeek` → 默认 1
4. 周相关函数与窗口逻辑统一从该解析结果取值，避免客户端提前折叠服务端配置

**验收标准**：

- 未设置连接级覆盖时，服务端 `firstDayOfWeek` 配置仍然生效
- 已设置连接级覆盖时，可正确覆盖服务端 `firstDayOfWeek`
- 服务端函数和窗口计算时可读到正确的最终 `firstDayOfWeek` 值

---

## 4. P2：展示口径统一（F1, F2）

### 4.1 Task 2.1：普通列时间戳展示使用连接时区

**目标**：`SELECT ts FROM t` 等场景优先使用连接时区（L2）展示。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `tools/shell/src/shellEngine.c` | `shellFormatTimestamp` 改为使用连接 `timezone_t` |
| `source/client/src/clientMain.c` | `taos_print_row` 时间戳格式化路径（当前直接输出 int64，需确认是否需改） |
| REST / WebSocket 接口格式化路径 | 若涉及时间戳字符串化，需同步改为连接 `timezone_t` |

**实现步骤**：

1. 定位客户端将 int64 时间戳转为字符串的路径：主要是 `shellFormatTimestamp`（当前使用 `taosLocalTime(&tt, &ptm, buf, bufSize, NULL)`，第五个参数 NULL 表示使用系统时区）、`taos_print_row`（当前直接 `snprintf` 输出 int64）
2. 将时区参数从客户端全局 `tsTimezone` 改为从 `TAOS_RES` 关联的连接 `timezone_t` 获取
3. 若连接未设置 L2，回退到 L3→L5（行为与旧版一致）

**验收标准**：

```sql
SET TIMEZONE 'America/New_York';
SELECT ts FROM t;
-- 结果按 America/New_York 展示
```

- 未执行 `SET TIMEZONE` 时，行为与当前版本一致

---

### 4.2 Task 2.2：SHOW / EXPLAIN 时间展示使用连接时区

**目标**：`SHOW TABLES`、`EXPLAIN` 等系统命令中的时间戳使用连接时区展示。

**实现步骤**：

1. SHOW 命令结果集中的时间列走与普通查询相同的格式化路径
2. EXPLAIN 输出中若包含时间信息，同样使用连接时区

**验收标准**：

- `SET TIMEZONE 'UTC'` 后 `SHOW TABLES` 中 create_time 按 UTC 展示

### 4.3 Task 2.3：WHERE / CAST / JOIN 时间字面量口径与回退链对齐

**目标**：与 FS 保持一致，明确查询时间字面量按 `L2 → L3 → L5` 解析，不引入服务端 `L4`。

**实现步骤**：

1. 复核 parser/translator/scalar 中时间字符串解析入口，确保查询字面量解析链路保持 `L2 → L3 → L5`
2. 明确本次该场景为“行为对齐与回归加固”，不引入新语义变更
3. 补齐该场景的回归测试，覆盖 WHERE/CAST/JOIN 三类表达式

**验收标准**：

- WHERE/CAST/JOIN 中时间字符串在设置连接级时区后按连接时区解析
- 未设置连接级时区时回退到客户端全局与系统默认

### 4.4 Task 2.4：写入路径 DST 行为回归加固

**目标**：按 FS 固化写入路径 DST 行为（春跳归一化、秋退默认第一次出现），避免改造回归。

**实现步骤**：

1. 增加写入路径 DST 回归用例：本地时间字符串写入、带偏移字符串写入、整型时间戳写入
2. 校验春跳 gap 与秋退 overlap 的既有行为不变
3. 将该组用例纳入 CI 场景，防止后续函数/解析改动误伤写入行为

**验收标准**：

- 春跳 gap 时间写入按既有规则归一化
- 秋退 overlap 时间写入按既有规则解析（默认第一次出现）
- 整型时间戳与带偏移写入行为不受影响

---

### 4.5 Task 2.5：`TODAY()` 连接时区行为验证

**目标**：按 FS 确认 `TODAY()` 使用 `L2 → L3 → L5` 回退链，与 `TO_ISO8601(ts)`、`TO_CHAR(ts, fmt)` 的回退链一致（均为客户端执行函数）。

**实现步骤**：

1. 记录并固化 `TODAY()` 的执行位置：该函数已确认在客户端执行，时区读取链路为 `L2 → L3 → L5`，不访问服务端 L4
2. 补充回归用例：在设置连接时区后，`TODAY()` 返回的 UTC 时间戳与连接时区下当天 `00:00:00` 对应一致
3. 验证未设置 L2 时，`TODAY()` 按 L3→L5 回退（与旧版行为一致）

**验收标准**：

```sql
SET TIMEZONE 'America/New_York';
SELECT TODAY();
-- 返回纽约时区当天 00:00:00 对应的 UTC 时间戳（L2 生效）

-- 假设服务端时区为 Asia/Shanghai，客户端时区为 UTC
-- 未执行 SET TIMEZONE 时，TODAY() 按客户端 UTC（L3）计算，不使用服务端 Asia/Shanghai（L4）
```

---

## 5. P3：函数扩展

### 5.1 Task 3.1：`TO_ISO8601` 支持 IANA 时区参数

**目标**：第二参数扩展支持 IANA 时区名称；无参时回退改为连接时区。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/libs/function/src/builtins.c` | 扩展第二参数校验逻辑；修正 translator 默认注入时区参数的逻辑 |
| `source/libs/scalar/src/sclfunc.c` | `toISO8601Function` 支持 IANA 解析 |

**实现步骤**：

1. builtins 中第二参数校验：现有 `validateTimezoneFormat()` 只做纯字符串格式校验（长度/符号），不支持 IANA 名称（`len > 6` 直接拒绝）。需用 Task 1.1 的共享时区校验 helper 替换，使第二参数同时支持固定偏移与 IANA 名称；扩展时注意不得以 `tzalloc()` 返回非 NULL 作为唯一合法性判据（某些平台对模糊缩写如 `CST` 也会返回非 NULL）
2. scalar 实现中：由于 translator 当前总是注入第二参数，`toISO8601Function` 在运行时永远收到 2 个参数，`inputNum == 1` 分支实际上是死代码。"无参数使用连接时区"的路径必须先在 translator 层打通（步骤 3），scalar 才能感知到真正的无参调用：
   - 有参数且为 IANA：对每行时间戳调用 `localtime_rz` 计算当时偏移，拼接 ISO8601 后缀
   - 有参数且为固定偏移：保持现有 `offsetOfTimezone` + `taosGmTimeR` 逻辑
   - 无参数（translator 不再注入）：使用连接时区（L2→L3→L5，客户端执行）
3. translator 默认参数逻辑：无第二参数时，不再把连接 `timezone_t` 在 translate 阶段折叠成固定 offset 字符串；需保留运行时可见的连接时区语义，避免丢失 IANA/DST 信息并避免把时区值冻结在翻译时刻
4. DST 感知：输出后缀随目标时刻的 DST 状态变化

**当前代码缺陷说明**：现有无参路径的核心问题不在 scalar 末端，而在 translator。当前 `translateToIso8601()` 会通过 `addTimezoneParam()` 把连接 `timezone_t` 提前折叠成固定 offset 字符串（如 `+0800`），该折叠发生在翻译时刻（`taosGetTZOffsetSeconds` 取当前偏移），导致无参调用丢失 IANA 名称及目标时刻 DST 语义，并把连接时区冻结为翻译时刻的偏移值。由于 translator 总是注入参数，scalar 层的 `inputNum` 永远等于 2，无法区分"有参"与"无参"场景。修复时需同时改 translator（停止注入固定偏移）与 scalar（增加 IANA 支持及无参分支），两处缺一不可。

**验收标准**：

```sql
SELECT TO_ISO8601(ts, 'America/New_York') FROM t;
-- 冬季返回 -05:00 后缀，夏季返回 -04:00 后缀

SET TIMEZONE 'America/New_York';
SELECT TO_ISO8601(ts) FROM t;
-- 按连接时区 America/New_York 输出
```

---

### 5.2 Task 3.2：`TO_CHAR` 支持第三参数时区

**目标**：扩展为 `TO_CHAR(ts, fmt [, timezone])`。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/libs/function/src/builtins.c` | `maxParamNum` 从 2 改为 3；增加第三参数校验 |
| `source/libs/scalar/src/sclfunc.c` | `toCharFunction` 增加时区参数处理分支 |

**实现步骤**：

1. builtins 注册：`maxParamNum = 3`，第三参数类型为 `TSDB_DATA_TYPE_VARCHAR`
2. 第三参数校验：支持 IANA / 固定偏移，复用 Task 1.1 的校验函数
3. scalar 层：
   - 有第三参数：按指定时区格式化
   - 无第三参数：按连接时区（L2→L3→L5，客户端执行）格式化

**当前代码说明**：现有 `toCharFunction` 目前仅接受 2 个用户参数，scalar 层通过 `pInput->tz`（运行时 `timezone_t`，非 translator 注入的固定偏移）直接获取连接时区。该路径未经 translator 在翻译时冻结，无参时自然回退到 L3→L5，无需额外修改 translator。增加第三参数后，scalar 需新增 `inputNum == 3` 分支：读取 `pInput[2]` 字符串时区，调用共享时区校验与解析 helper，在指定时区坐标系下格式化，替代直接使用 `pInput->tz`。

**验收标准**：

```sql
SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS', 'America/New_York') FROM t;
-- 按纽约时区格式化

SET TIMEZONE 'America/New_York';
SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') FROM t;
-- 按连接时区格式化
```

---

### 5.3 Task 3.3：`TIMETRUNCATE` 第三参数扩展为字符串时区

**目标**：第三参数从仅支持整数 `0/1` 扩展为同时支持字符串时区。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/libs/function/src/builtins.c` | 第三参数类型从纯整数改为"整数或字符串" |
| `source/libs/scalar/src/sclfunc.c` | `timeTruncateFunction` 增加字符串时区分支 |

**实现步骤**：

1. builtins：第三参数允许 `TSDB_DATA_TYPE_INT` 或 `TSDB_DATA_TYPE_VARCHAR`
2. scalar 层按参数类型分支：
   - 整数 0/1：保持现有 `use_current_timezone` 语义不变（内部实现名 `ignoreTz` 与对外语义相反，文档统一使用对外名称）
   - 字符串：复用 Task 1.1 的共享时区校验 helper 解析，在该时区坐标系中执行截断
3. 截断逻辑：
   - 先将输入时间戳解析为绝对 UTC 时刻
   - 按第三参数指定时区的本地日历边界截断
   - 返回截断后边界对应的 UTC 时间戳

**内部参数索引映射说明**：`timeTruncateFunction` 在运行时接收的参数数量取决于用户是否提供第三参数。`translateTimeTruncate` 会额外注入两个参数（precision 和 timezone 字符串），因此：
- 用户无第三参数（`TIMETRUNCATE(ts, unit)`）：4 个内部参数，顺序为 `pInput[0]=ts, pInput[1]=unit, pInput[2]=precision, pInput[3]=timezoneStr`
- 用户有第三参数（`TIMETRUNCATE(ts, unit, 0/1)`）：5 个内部参数，顺序为 `pInput[0]=ts, pInput[1]=unit, pInput[2]=ignoreTz, pInput[3]=precision, pInput[4]=timezoneStr`

此外，`translateTimeTruncate` 也通过 `addTimezoneParam()` 在翻译阶段将 `timezone_t` 折叠为固定 offset 字符串（与 TO_ISO8601 同源问题）。当用户提供字符串时区作为新第三参数时，translator 需同样停止注入固定偏移，改为透传用户的字符串时区；scalar 层需增加对该字符串时区的 IANA 解析支持，与 Task 3.1 复用相同处理路径。

**验收标准**：

```sql
SET TIMEZONE 'Asia/Shanghai';
SELECT TIMETRUNCATE('2026-03-15 10:30:00', 1d, 'America/New_York') FROM t;
-- 按纽约当天 00:00 截断

-- 旧语义不变
SELECT TIMETRUNCATE('2026-03-15 10:30:00', 1d, 0) FROM t;
SELECT TIMETRUNCATE('2026-03-15 10:30:00', 1d, 1) FROM t;
```

---

## 6. P4：自然单位与周对齐

### 6.1 Task 4.1：`TIMETRUNCATE` 支持 `n`/`q`/`y` 单位

**目标**：新增月、季度、年截断单位。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/libs/function/src/builtins.c` | 单位校验扩展 |
| `source/libs/scalar/src/sclfunc.c` | 截断计算逻辑 |
| parser 中 `validateTimeUnitParam` | 允许 `n`/`q`/`y` 及多倍数 |

**实现步骤**：

1. 单位校验：仅对 `TIMETRUNCATE` 的调用路径，将 `validateTimeUnitParam` 从固定 `1x` 改为可解析 `Nx` 多倍数格式（`N` 为正整数，`x` 为单位字母）。其他调用者（如 `ELAPSED`、`STATE_DURATION` 等）仍保持 `1x` 限制，避免引入意外行为变更。如果 `validateTimeUnitParam` 是共享函数，则需新增一个允许多倍数的变体（如 `validateTimeUnitParamEx`）供 TIMETRUNCATE 单独使用
2. 新增自然单位映射：`n`=月, `q`=季度, `y`=年
3. 截断算法（在本地时区坐标系中）：
   - `1n`：对齐到当月 1 日 00:00:00
   - `Nn`（N>1）：按 epoch 起月份计数整除对齐
   - `1q`：对齐到当季首月 1 日 00:00:00（Q1=1月, Q2=4月, Q3=7月, Q4=10月）
   - `Nq`（N>1）：按 epoch 起季度计数整除对齐
   - `1y`：对齐到当年 1 月 1 日 00:00:00
   - `Ny`（N>1）：按 epoch 起年计数整除对齐

**验收标准**：

```sql
SET TIMEZONE 'Asia/Shanghai';
SELECT TIMETRUNCATE('2026-03-15 10:30:00', 1n);   -- 2026-03-01 00:00:00
SELECT TIMETRUNCATE('2026-05-15 10:30:00', 1q);   -- 2026-04-01 00:00:00
SELECT TIMETRUNCATE('2026-08-15 10:30:00', 1y);   -- 2026-01-01 00:00:00
SELECT TIMETRUNCATE('2026-05-15 10:30:00', 3n);   -- 2026-04-01 00:00:00
SELECT TIMETRUNCATE('2026-05-15 10:30:00', 2q);   -- 2026-01-01 00:00:00
```

---

### 6.2 Task 4.2：`TIMETRUNCATE` `1w` 对齐修正（F9）

**目标**：周截断从 epoch 取整（星期四）改为按 `firstDayOfWeek` 对齐。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/libs/scalar/src/sclfunc.c` | 周截断核心逻辑 |

**实现步骤**：

1. 获取 `firstDayOfWeek`：按回退链 `SET FIRST_DAY_OF_WEEK` → 服务端 `tsFirstDayOfWeek` → 默认 1
2. 新算法：
   ```
   a. 将 UTC 时间戳转为本地时间
   b. 计算当前 wday（0=周日, 1=周一, ..., 6=周六）
   c. 计算回退天数 = (wday - firstDayOfWeek + 7) % 7
   d. 回退到本周起始日 00:00:00
   e. 转回 UTC 时间戳
   ```
3. 多倍数 `Nw`：先对齐到本周起始日，再按 N 周对齐到 epoch 起始后的周计数。当 `firstDayOfWeek` 变化时，epoch 周计数的起点类似于“找到 epoch 之后第一个包含 `firstDayOfWeek` 的周起始日”作为基准。示例：
   ```
   -- firstDayOfWeek = 1 (周一)
   -- 2026-04-30 是周四，回退 3 天到周一 2026-04-27
   -- 2w: epoch 后周一起点开始计数，按 2 周整除对齐
   SELECT TIMETRUNCATE('2026-04-30 10:00:00', 2w);
   -- 结果取决于 epoch-based 周序号的奇偶性
   ```

**验收标准**：

```sql
-- firstDayOfWeek = 1 (周一)
SELECT TIMETRUNCATE('2026-04-30 10:00:00', 1w);  -- 2026-04-27 00:00:00 (周一)

SET FIRST_DAY_OF_WEEK 0;
SELECT TIMETRUNCATE('2026-04-30 10:00:00', 1w);  -- 2026-04-26 00:00:00 (周日)
```

**风险提示**：这是高风险行为变更，会影响所有现有 `TIMETRUNCATE(..., 1w)` 查询结果。

---

### 6.3 Task 4.3：`INTERVAL` 自然单位边界与步进对齐

**目标**：`INTERVAL(d/w/n/q/y)` 的窗口边界与窗口步进统一按本地日历语义执行；其中 `w` 额外受 `firstDayOfWeek` 影响。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/common/src/ttime.c` | `taosTimeTruncate`、`getNextTimeWindowStart` 等窗口边界计算函数 |
| `source/libs/executor/src/executorInt.c` | `getAlignQueryTimeWindow` 等窗口对齐函数 |
| `source/libs/executor/src/executil.c` | `getNextTimeWindow` 窗口推进函数 |
| `include/common/ttime.h` | 保持 `IS_CALENDAR_TIME_DURATION` 仅包含 `n/y(/q)`，不纳入 `d/w` |

**实现步骤**：

1. 定位 `INTERVAL(d/w/n/q/y)` 的窗口边界计算与窗口推进代码，区分“首窗口对齐”和“后续窗口步进”两条路径
2. `INTERVAL(Nw)` 的首窗口边界改为与 Task 4.2 相同的 `firstDayOfWeek` 对齐算法，替换当前 epoch 取模逻辑
3. **`d` 单位改造说明**：保持 `d` 不纳入 `IS_CALENDAR_TIME_DURATION`。在现有 `else` 分支上补专用“本地自然日”处理：首窗口对齐到本地 `00:00:00`，后续窗口推进通过日历日加法而非固定 `24h` 累加完成，确保 DST 切换日窗口长度可为 23h/25h 而边界仍落在本地午夜
4. **`w` 单位改造说明**：保持 `w` 不纳入 `IS_CALENDAR_TIME_DURATION`。周窗口继续走专用分支，但其首窗口对齐与后续步进均改为 `firstDayOfWeek` 感知的本地日历周算法，不再依赖 epoch 取模或固定 `168h` 累加
5. **宏定义约束**：`IS_CALENDAR_TIME_DURATION` 继续只承载月/季度/年这类按 month-year 维度对齐的单位；`d/w` 改造通过显式 helper 或分支完成，避免把语义不同的日/周路径混入该宏
6. `INTERVAL(d/w/n/q/y)` 的后续窗口推进与 `SLIDING` 统一改为本地日历运算，不再依赖固定秒数累加，避免 DST 切换日/周出现 24h 或 168h 漂移
7. `INTERVAL(1n)` / `INTERVAL(1q)` / `INTERVAL(1y)` 的窗口边界统一对齐到目标本地日历边界；其中 `1q` 与 `3n` 保持语义等价
8. 固定单位（`a/s/m/h` 等）保持现有固定时长语义，不纳入本 Task 改造范围

**验收标准**：

```sql
-- firstDayOfWeek = 1 (周一)
SELECT _wstart, COUNT(*) FROM t INTERVAL(1w);
-- 窗口起始日为周一

SET FIRST_DAY_OF_WEEK 0;
SELECT _wstart, COUNT(*) FROM t INTERVAL(1w);
-- 窗口起始日为周日

SET TIMEZONE 'America/New_York';
SELECT _wstart, _wend, COUNT(*) FROM t INTERVAL(1d);
-- DST 春跳日窗口边界仍对齐到本地 00:00:00，窗口推进不因固定 24h 累加而漂移

SET TIMEZONE 'Asia/Shanghai';
SELECT COUNT(*) FROM t INTERVAL(1q);
SELECT COUNT(*) FROM t INTERVAL(3n);
-- 两者窗口边界与聚合结果一致
```

**风险提示**：高风险行为变更，与 Task 4.2 同级。

---

### 6.4 Task 4.4：`WEEK` / `WEEKOFYEAR` 接入 `firstDayOfWeek`

**目标**：`WEEK`、`WEEKOFYEAR` 这两个依赖周序号边界的函数统一使用 `firstDayOfWeek` 回退链。`DAYOFWEEK`、`WEEKDAY` 不依赖周起始日配置，**不在本 Task 改动范围内**。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| 周相关 scalar/function 实现文件 | `WEEK`/`WEEKOFYEAR` 周序号与周归属计算改为读取统一的 `firstDayOfWeek` 解析结果 |
| 共享时间工具或 helper | 抽取/复用 `firstDayOfWeek` 解析 helper，供 `TIMETRUNCATE`、`INTERVAL`、`WEEK`、`WEEKOFYEAR` 共用 |

**实现步骤**：

1. 定位 `WEEK`、`WEEKOFYEAR` 当前周边界/周归属计算逻辑
2. 将周起始日来源统一切换为 Task 1.5 中的服务端解析结果，而不是隐式固定周一或局部常量
3. `WEEK(ts, mode)` 中的 `mode` 参数作为 L1 级参数，优先级高于 `firstDayOfWeek` 回退链。`mode` 取值 0-7，语义兼容 MySQL `WEEK()` 函数：mode 同时控制周起始日（0=周日, 1=周一）和年边界归属规则（ISO vs simple）。实现时需确保 mode 指定的周起始日可以独立覆盖 `firstDayOfWeek` 的效果，而非与 `firstDayOfWeek` 值混用
4. 若当前代码已存在独立周计算 helper，则收口为共享 helper，确保 `TIMETRUNCATE`、`INTERVAL`、`WEEK`、`WEEKOFYEAR` 语义一致
5. 校验连接级覆盖、服务端配置、默认值三层回退在上述函数中的结果一致性

**验收标准**：

```sql
-- 服务端 firstDayOfWeek = 1 (周一)
SELECT WEEKOFYEAR('2026-01-01 12:00:00');
-- 结果按周一作为周起始日计算

SET FIRST_DAY_OF_WEEK 0;
SELECT WEEK('2026-01-01 12:00:00');
SELECT WEEKOFYEAR('2026-01-01 12:00:00');
-- 结果按周日作为周起始日计算，并覆盖服务端配置
```

**风险提示**：若现有 `WEEK` / `WEEKOFYEAR` 已被上层业务依赖固定周一语义，这一改动同样属于行为变更，需要通过回归用例显式锁定。

---

## 7. P5：季度集成收口

### 7.1 Task 5.1：Rebase 季度 PR ✅

**目标**：将已有季度 `q` PR 合入当前分支。

**状态**：已完成。季度 PR 已 rebase 合入，代码已支持季度（`q`）时间单位。

**步骤**：

1. Rebase 或 merge 包含 `q` 实现的 PR
2. 解决冲突（特别注意 parser 与 executor 中的单位枚举）
3. 确保 `IS_CALENDAR_TIME_DURATION` 包含 `q`

---

### 7.2 Task 5.2：`INTERVAL(Nq)` 集成验证

**目标**：验证季度窗口与本分支的时区/firstDayOfWeek 体系协同工作。

**验收标准**：

```sql
SET TIMEZONE 'Asia/Shanghai';
SELECT _wstart, _wend, COUNT(*) FROM t INTERVAL(1q);
-- [2026-01-01, 2026-04-01), [2026-04-01, 2026-07-01), ...

-- INTERVAL(1q) 与 INTERVAL(3n) 等价
SELECT COUNT(*) FROM t INTERVAL(1q);
SELECT COUNT(*) FROM t INTERVAL(3n);
-- 两者结果一致
```

---

## 8. P6：TIMEZONE(1) 服务端扩展

### 8.1 Task 6.1：`TIMEZONE()` 参数扩展

**目标**：支持 `TIMEZONE([0|1])`，参数为 1 时返回三层 JSON。

**涉及文件**：

| 文件 | 改动 |
| --- | --- |
| `source/libs/function/src/builtins.c` | 参数从 0 参改为可选 1 参 |
| `source/libs/scalar/src/sclfunc.c` | `timezoneFunction` 按参数分支 |
| planner | `TIMEZONE(1)` 标记为需服务端执行 |
| 查询请求消息结构体与序列化代码 | 增加/透传 `clientTimezoneStr`（仅用于 `TIMEZONE(1)`） |
| parser/planner/executor 请求上下文 | 补充 `clientTimezoneStr` 透传与读取 |

**实现步骤**：

1. builtins：`minParamNum=0, maxParamNum=1`，参数仅接受整数 0 或 1
2. scalar 分支：
   - 无参 / 参数 0：**始终在客户端执行**，返回客户端时区字符串（兼容旧行为）。注意：当前代码中 `timezoneFunction` 在服务端执行时会检查 `pInput->tz`，若非 NULL 则返回连接/服务端时区，这与 FS 要求的“返回客户端时区字符串”不一致。实现时需确保 planner 不会将 `TIMEZONE(0)` 路由到服务端执行
   - 参数 1：需下发服务端执行
3. 服务端执行时：
   - 从请求上下文读取 session timezone（L2）
   - 读取服务端 `tsTimezoneStr`（L4）
   - 保留 `TIMEZONE(1)` 三层语义：`session/client/server`
   - 其中 `client` 层由客户端本地时区（L3）填充；该透传仅用于 `TIMEZONE(1)`，普通查询链路不要求服务端感知 L3
   - 最终返回 JSON：`{"session":"<tz>","client":"<tz>","server":"<tz>"}`
4. Planner 识别 `TIMEZONE(1)` 为"需服务端参与"的表达式

### 8.2 Task 6.2：`TIMEZONE(1)` 的 client timezone 来源与透传

**目标**：补齐 `TIMEZONE(1)` 三层输出中的 `client` 字段来源，避免实现断点。

**实现步骤**：

1. 统一 client timezone 取值来源：连接创建时可见的客户端 L3 最终字符串（含回退到 L5 后的最终值）
2. 在查询请求结构中增加 `clientTimezoneStr` 字段，并完成编解码
3. 仅在 `TIMEZONE(1)` 请求路径填充该字段；普通查询请求不要求填充
4. 服务端 `timezoneFunction(1)` 从请求上下文读取该字段并写入 JSON `client` 键
5. 字段缺失或为空时定义兜底策略（返回 `unknown` 或回退到请求侧可用值），并固化测试

**验收标准**：

- `TIMEZONE(1)` 在不同客户端全局时区下可稳定返回正确 `client` 字段
- 普通查询路径不依赖 `clientTimezoneStr`，不引入额外语义耦合

**验收标准**：

```sql
SET TIMEZONE 'America/New_York';
SELECT TIMEZONE();
-- 返回: Asia/Shanghai（兼容旧行为，返回客户端时区字符串）

SELECT TIMEZONE(0);
-- 返回: Asia/Shanghai（同上）

SELECT TIMEZONE(1);
-- 返回: {"session":"America/New_York (UTC-5, EST)","client":"Asia/Shanghai (UTC+8, CST)","server":"Asia/Shanghai (UTC+8, CST)"}

-- 未设置连接级时区时，session 字段继承客户端时区，与 client 字段相同
SELECT TIMEZONE(1);
-- 返回: {"session":"Asia/Shanghai (UTC+8, CST)","client":"Asia/Shanghai (UTC+8, CST)","server":"Asia/Shanghai (UTC+8, CST)"}
```

---

## 9. 测试计划

本章同时作为当前阶段的首要交付范围。执行顺序固定为：先补齐下述单元测试与集成测试，再由人工评审测试内容；评审通过后，才进入代码实现和回归阶段。

### 9.1 单元测试

| 模块 | 测试内容 |
| --- | --- |
| parser | `SET TIMEZONE` / `SET FIRST_DAY_OF_WEEK` 语法解析正确性与错误码 |
| 时区校验函数 | IANA / 固定偏移 / 非法输入覆盖 |
| scalar/TIMETRUNCATE | n/q/y 截断算法、周对齐算法、DST 边界 |
| 周相关函数 | `WEEK` / `WEEKOFYEAR` 的 `firstDayOfWeek` 回退与边界归属 |
| TIMEZONE(1) 组装 | `session/client/server` 三层字段来源与缺省分支 |

#### 9.1.1 当前已补充

1. `source/common/test/ttimeNaturalUnitsTest.cpp`
   - 补强 `w/n/y` 自然单位边界断言，避免仅检查 `tm` 字段而不锁定精确 epoch 值
   - 补充 `2026-04-30 -> 2026-04-27` 的 `1w` 精确边界测试
   - 补充基于当前 epoch Monday 锚点的 `2w` 精确边界测试
   - 补强 `3n` / `2y` 多倍数边界的精确起点断言
2. `source/common/test/commonTests.cpp`
   - 现有 `taosTimeTruncate_DST_day_interval` 已覆盖 DST 日边界的核心单元场景，可继续作为 9.1 中 DST 边界验证的当前承载点

#### 9.1.2 当前阻塞项

以下单元测试依赖的代码结构、字段或执行分支当前尚未落地，因此暂不补充为可编译测试；待对应功能代码进入分支后补齐：

1. parser 新语法单元测试
   - 阻塞原因：`QUERY_NODE_SET_TIMEZONE_STMT` / `QUERY_NODE_SET_FIRST_DAY_OF_WEEK_STMT` 节点类型、对应 grammar 规则和 AST 构造函数当前尚不存在，直接补 parser gtest 会因符号缺失无法编译
2. 共享 timezone 校验 helper 单元测试
   - 阻塞原因：当前 `validateTimezoneFormat()` 仍是 `source/libs/function/src/builtins.c` 内部 `static` 函数，且行为仍是旧的“纯格式校验”；plan 要求的共享 helper 及 IANA 校验入口尚未抽出，无法做稳定的独立单测
3. `firstDayOfWeek` 请求透传 / 编解码单元测试
   - 阻塞原因：`SSubQueryMsg` / `SInterval` 当前尚无 `firstDayOfWeek` 字段，`tSerializeSSubQueryMsg()` / `tDeserializeSSubQueryMsg()` 也无对应编解码路径
4. `TIMEZONE(1)` 组装单元测试
   - 阻塞原因：`timezoneFunction()` 当前仍仅返回单层时区字符串，尚无 `TIMEZONE(1)` 的 `session/client/server` JSON 组装分支；`clientTimezoneStr` 透传字段也未进入请求结构
5. `WEEK` / `WEEKOFYEAR` 的 `firstDayOfWeek` 回退链单元测试
   - 阻塞原因：当前周函数尚未接入 plan 要求的 `SET -> server -> default` 解析链，补单测只能固化旧行为，和目标实现不一致

### 9.2 集成测试（pytest）

| 用例分组 | 覆盖内容 |
| --- | --- |
| `test_set_timezone` | SET TIMEZONE 各种合法/非法输入；连接隔离性 |
| `test_set_first_day_of_week` | 合法/非法输入；回退链验证 |
| `test_to_iso8601_iana` | IANA 参数；DST 动态偏移；无参回退链 |
| `test_to_char_timezone` | 三参数调用；DST；无参回退 |
| `test_timetruncate_tz` | 字符串时区参数；与旧 0/1 兼容 |
| `test_timetruncate_natural` | n/q/y 单位及多倍数 |
| `test_timetruncate_week` | 1w 对齐修正；firstDayOfWeek 各值 |
| `test_interval_quarter` | INTERVAL(1q) 窗口边界；与 INTERVAL(3n) 等价性 |
| `test_interval_natural` | INTERVAL(d/n/y) 在设置连接时区后的窗口边界、窗口步进与时区回退链回归；覆盖 DST 切换日，防止固定秒数推进导致边界漂移 |
| `test_interval_week` | INTERVAL(1w) 尊重 firstDayOfWeek |
| `test_week_functions_fdow` | `WEEK` / `WEEKOFYEAR` 尊重 firstDayOfWeek；覆盖连接级 override / 服务端配置 / 默认值；`WEEK(ts, mode)` 中 mode 作为 L1 覆盖 firstDayOfWeek |
| `test_today_tz` | TODAY() 在设置连接时区后返回正确 UTC 时间戳；未设置 L2 时按 L3→L5 回退，不使用服务端 L4 |
| `test_now_tz` | NOW() 返回原始 UTC 时间戳，不受 SET TIMEZONE 影响；确认行为未变 |
| `test_timezone_func` | TIMEZONE() / TIMEZONE(0) / TIMEZONE(1) |
| `test_display_tz` | 普通列/SHOW/EXPLAIN 使用连接时区展示 |
| `test_where_cast_join_tz` | WHERE/CAST/JOIN 时间字面量按 `L2→L3→L5` 解析 |
| `test_write_dst_parse` | 写入路径春跳/秋退解析行为回归（含整型/带偏移对照） |
| `test_dst_edge` | 春跳/秋退边界场景 |
| `test_dayofweek_weekday_unchanged` | `DAYOFWEEK` / `WEEKDAY` 不受 `firstDayOfWeek` 影响的负面回归验证 |
| `test_timetruncate_multiples` | TIMETRUNCATE 多倍数单位（`3n`、`2q`、`2y`、`2w`）的对齐算法专项测试 |

### 9.3 回归测试

前置条件：仅在“测试已生成且评审通过”“对应功能实现已完成”之后执行本节回归项。

- 所有现有 `TO_ISO8601`、`TO_CHAR`、`TIMETRUNCATE`、`INTERVAL` 用例在未设置 `SET TIMEZONE` 时行为不变
- `ALTER LOCAL 'timezone'` 行为不受影响
- `TIMEZONE()` / `TIMEZONE(0)` 默认返回行为与 FS 定义保持一致

---

## 10. 风险与缓解

| 风险 | 影响 | 缓解措施 |
| --- | --- | --- |
| `TIMETRUNCATE(1w)` / `INTERVAL(1w)` 行为变更 | 现有查询结果变化 | 版本说明明确告知；默认 `firstDayOfWeek=1` 时从周四变为周一 |
| 季度 PR rebase 冲突 | 阻塞 P5 | P1-P4 先行，P5 独立收口 |
| `TIMEZONE(1)` 需服务端执行 | 增加复杂度 | 放在 P6 最后阶段，充分测试 |
| 错误码 0x2600/0x2601 编号冲突 | 与现有语义冲突 | 已确认占用，必须在函数/参数错误码域分配新编号；不得复用现有 parser 错误码 |
| DST 边界行为差异 | 不同平台 tzdata 版本差异 | 使用 TDengine 内置 tzdata；测试覆盖 DST 切换日 |
| FS 口径变更后的实现漂移 | plan 与最新 FS 不同步，导致实现/测试偏离最终行为 | 区分场景对齐 FS：服务端日历计算按 `L2 → L4 → L5`，客户端格式化函数（TO_ISO8601/TO_CHAR）按 `L2 → L3 → L5`，并在评审与回归测试中显式校验 |
| `L2→L4→L5` 回退链执行位置 | 仅服务端执行的函数（TIMETRUNCATE/INTERVAL/WEEK 等）使用该链 | 已确认 TO_ISO8601/TO_CHAR/TODAY 始终在客户端执行，回退链为 `L2→L3→L5`，不需访问 L4 |
| `d/w` 与月年单位共用宏 | 若把 `d/w` 纳入 `IS_CALENDAR_TIME_DURATION`，易把“按日历日/周推进”和“按月份整除”两类语义混为一谈 | 已明确 `d/w` 不纳入该宏，改走专用日历分支；通过测试锁定 DST 与 firstDayOfWeek 行为 |

---

## 11. 用户手册修改指南

本项目除代码与测试外，还需同步更新用户手册，避免新语法、新回退链和高风险行为变更只存在于 FS/DS 与测试中而未对外说明。

### 11.1 修改范围

| 文档类型 | 必改内容 |
| --- | --- |
| SQL 语法手册 | `SET TIMEZONE`、`SET FIRST_DAY_OF_WEEK` 新语法、参数约束、错误码 |
| 函数手册 | `TIMEZONE([0|1])`、`TO_ISO8601`、`TO_CHAR`、`TIMETRUNCATE` 的新增参数、回退链、DST 语义 |
| 查询/窗口函数手册 | `INTERVAL(w/n/q/y)`、`TIMETRUNCATE(..., w/n/q/y)`、`WEEK` / `WEEKOFYEAR` 与 `firstDayOfWeek` 的关系 |
| 配置手册 | 服务端 `firstDayOfWeek` 配置项、默认值、动态修改方式与限制 |
| 错误码文档 | `TSDB_CODE_PAR_INVALID_TIMEZONE`、`TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK` |
| 版本说明 / 升级说明 | `TIMETRUNCATE(1w)`、`INTERVAL(1w)` 行为变更与兼容性提示 |

### 11.2 必须补齐的主题

1. `SET TIMEZONE` 需写明支持的固定偏移格式（`Z` / `±HH` / `±HHMM` / `±HH:MM`）、小时两位限制、`-14:00 ~ +14:00` 范围、模糊缩写如 `CST` 非法。
2. `SET FIRST_DAY_OF_WEEK` 与服务端 `firstDayOfWeek` 配置需明确优先级链：连接级 override → 服务端配置 → 默认值 1。
3. `TIMEZONE()` / `TIMEZONE(0)` 需明确保持兼容，返回客户端时区字符串；`TIMEZONE(1)` 返回 `session/client/server` 三层 JSON 字符串。
4. `TO_ISO8601(ts)` / `TO_CHAR(ts, fmt)` 无参时的最终回退链为 `L2 → L3 → L5`（客户端执行）；`TIMETRUNCATE(ts, unit)` 无参时为 `L2 → L4 → L5`（服务端执行）；`TODAY()` 为 `L2 → L3 → L5`。
5. `WHERE` / `CAST` / `JOIN` 中时间字面量的解析需明确仍按 `L2 → L3 → L5`，不引入服务端 `L4`。
6. `TIMETRUNCATE` 第三参数需说明整数 `0/1` 兼容语义与字符串时区新语义并存；字符串参数支持 IANA 与固定偏移。
7. `TIMETRUNCATE(..., 1w)` 与 `INTERVAL(1w)` 需明确从“按 epoch 星期四对齐”变为“按 `firstDayOfWeek` 对齐”，这是对现有结果有影响的行为变更。
8. `INTERVAL(d/w/n/q/y)` 与 `TIMETRUNCATE(d/w/n/q/y)` 需补充自然单位在本地日历坐标系中对齐的说明，并给出 DST 切换日示例。
9. 写入路径与查询路径的 DST 说明需区分“字符串解析为绝对时刻”和“已存储时间戳本地化展示”两个阶段，避免手册混淆。

### 11.3 示例要求

1. 每个新增语法至少给出 1 个合法示例和 1 个非法示例。
2. `TIMEZONE(1)` 需给出“已设置 L2”和“未设置 L2”两组示例输出。
3. `TO_ISO8601` / `TO_CHAR` 需给出 IANA 时区示例，并覆盖 DST 冬夏令时偏移变化。
4. `TIMETRUNCATE` 需给出 `1d`、`1w`、`1n`、`1q`、`1y` 与第三参数字符串时区示例。
5. `INTERVAL(1q)` 与 `INTERVAL(3n)` 需给出等价示例；`INTERVAL(1w)` 需给出 `firstDayOfWeek=1/0` 对比示例。

### 11.4 文档验收标准

1. 中英文手册需同步更新，术语、错误码、回退链与 FS/plan 保持一致。
2. 用户手册中的默认行为必须与兼容性要求一致，不能把 `TIMEZONE()` 默认行为误写成连接时区。
3. 所有高风险行为变更都需在版本说明中显式标注，不得只出现在函数章节示例中。
4. 手册示例与测试口径一致，至少覆盖 `CST` 非法、DST 边界、`TIMEZONE(1)` JSON 输出、`firstDayOfWeek` 对周边界影响。

---

## 12. 交付检查清单

- [x] 第 9 章单元测试与 pytest 用例已先行生成，覆盖范围可供评审
- [x] 测试内容已完成人工评审，确认正确且覆盖充分后再进入开发
- [x] P1：`SET TIMEZONE` 语法可用
- [x] P1：`SET FIRST_DAY_OF_WEEK` 语法可用
- [x] P1：服务端 `firstDayOfWeek` 配置项可用
- [x] P1：分配新错误码（替代已占用的 0x2600/0x2601）并注册
- [x] P1：请求上下文携带 firstDayOfWeek
- [x] P2：普通列展示使用连接时区
- [ ] P2：SHOW/EXPLAIN 使用连接时区
- [x] P2：WHERE/CAST/JOIN 时间字面量口径与回退链对齐
- [x] P2：写入路径 DST 行为回归加固
- [ ] P3：TO_ISO8601 支持 IANA
- [ ] P3：TO_CHAR 支持第三参数
- [ ] P3：TIMETRUNCATE 第三参数支持字符串时区
- [ ] P4：TIMETRUNCATE 支持 n/q/y
- [ ] P4：TIMETRUNCATE 1w 对齐修正
- [ ] P4：INTERVAL(w) 尊重 firstDayOfWeek
- [x] P5：季度 PR rebase 完成
- [ ] P5：INTERVAL(1q) 集成验证
- [ ] P6：TIMEZONE(1) 服务端执行
- [ ] P6：TIMEZONE(1) client timezone 来源与透传打通
- [ ] P4：`d/w` 保持不纳入 `IS_CALENDAR_TIME_DURATION`，并以专用日历分支完成对齐与步进
- [ ] P4：TIMEZONE(0) planner 不路由到服务端执行确认
- [ ] FS 第 12 章错误码编号同步更新（当前仍为 0x2600/0x2601，与实际占用冲突）
- [ ] 全量回归测试通过
- [ ] 用户手册已补充新语法、函数参数、回退链、DST 与兼容性说明
- [ ] 中英文错误码文档已同步新增错误码与消息
- [ ] 版本说明文档更新（高风险变更项）
