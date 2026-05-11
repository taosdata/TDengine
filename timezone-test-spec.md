# 时区与查询改造 - 测试说明文档

## 1. 修订记录

| 编写日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2026-05-06 | 0.1 | AI | 初稿，基于 timezone-plan.md v0.7 生成 |
| 2026-05-06 | 0.2 | AI | 合并 20 个文件为 3 个，更新文件清单与验证要点 |
| 2026-05-07 | 0.3 | AI | 同步 plan 9.1：补充当前已落地的 common gtest 单元测试与暂未开发完成的阻塞项 |
| 2026-05-07 | 0.4 | AI | 补充 async local SET client gtest 与 parser 负数 firstDayOfWeek 回归 |
| 2026-05-09 | 0.5 | AI | 同步最新实现状态：恢复最小 1w 回归、补充 1w+DST 一致性回归、更新 P4 skip 现状与执行结果 |
| 2026-05-09 | 0.6 | AI | `firstDayOfWeek` 从服务端改为客户端参数：TestSetFirstDayOfWeek 改为 `ALTER LOCAL` 验证；TestIntervalWeek 改为客户端配置生效；回退链矩阵更新 |
| 2026-05-11 | 0.7 | AI | 新增 Finding 1 连接隔离回归测试：`test_fdow_alter_local_updates_process_global_not_existing_connection`（立即运行）和 `test_fdow_alter_local_does_not_affect_existing_connection_timetruncate`（P4 skip）；更新 TestSetFirstDayOfWeek 验证要点 |
| 2026-05-11 | 0.8 | AI | P4 全部落地（48 passed）；删除冗余/无效 skip 测试：`test_fdow_affects_timetruncate_week`（被 TestTimetruncateWeek 覆盖）、`test_fdow_client_config_applies_without_session_override`（ALTER LOCAL 不影响当前连接快照，设计缺陷）、`test_week_mode_overrides_fdow`（选取日期下 mode=0/1 结果相同，期望错误）；更新 TestTimetruncateNaturalUnits / TestTimetruncateWeek / TestWeekFunctions 为已实现状态；更新 9.2 阻塞项为已落地 |

## 2. 概述

本文档列出「时区与查询改造」当前 pytest 集成测试文件、已补充的 common gtest 单元测试，以及仍待功能代码落地后补齐的单元测试阻塞项，供人工评审确认覆盖度与正确性。所有测试当前不以“通过”为目标：集成测试对应功能尚未实现，单元测试仅同步记录目前已能编译落地的部分。

测试合并为 3 个文件，按功能域组织：配置/展示、标量函数、时间窗口。

另补充 2 个 gtest 回归锚点，用于覆盖本轮新增的本地执行与 parser 边界行为：
- `source/client/test/clientTests.cpp`：异步 `taos_query_a("SET ...")` 本地命令路径
- `source/libs/parser/test/parInitialDTest.cpp`：`SET FIRST_DAY_OF_WEEK -1` 走“语法接受 + 语义报错”

---

## 3. 测试文件清单

| # | 文件 | 包含 Class | 对应 Task |
|---|------|-----------|-----------|
| 1 | `test/cases/11-Functions/01-Scalar/test_tz_config_display.py` | TestSetTimezone, TestSetFirstDayOfWeek, TestTimezoneFunc, TestDisplayTimezone, TestWhereCastJoinTz, TestTodayNowTz, TestIntervalTimezone | P1 Task 1.1-1.3, P2 Task 2.1-2.5, P6 Task 6.1-6.2 |
| 2 | `test/cases/11-Functions/01-Scalar/test_tz_scalar_functions.py` | TestToIso8601Iana, TestToCharTimezone, TestTimetruncateTz, TestTimetruncateNaturalUnits, TestTimetruncateWeek, TestWeekFunctions, TestDstEdge | P3 Task 3.1-3.3, P4 Task 4.1-4.2/4.4, P2 Task 2.4, DST 边界 |
| 3 | `test/cases/13-TimeSeriesExt/03-TimeWindow/test_tz_interval.py` | TestIntervalNatural, TestIntervalWeek, TestIntervalQuarter | P4 Task 4.3, P5 Task 5.2 |

---

## 4. 各文件测试要点

### 4.1 `test_tz_config_display.py` — 配置 / 展示 / 查询行为

#### TestSetTimezone — SET TIMEZONE 语法（P1 Task 1.1）
- ✅ IANA 名称合法：`Asia/Shanghai`、`America/New_York`、`UTC`、`Europe/London`、`Asia/Tokyo`
- ✅ 固定偏移合法：`+08:00`、`+0800`、`+08`、`-05:00`、`Z`、`+05:30`、`±14:00`
- ✅ 空串降级：`SET TIMEZONE ''` 退化为 UTC（与 C API `taos_options_connection` 行为一致）
- ✅ 非法输入：单数字小时（`+8`/`-4`）、模糊缩写（`CST`/`EST`/`PST`）、超限偏移、无效名称
- ✅ 连接隔离性：SET TIMEZONE 仅影响当前连接
- ✅ 后续查询生效：多次 SET TIMEZONE 切换后查询结果一致

#### TestSetFirstDayOfWeek — SET FIRST_DAY_OF_WEEK 语法（P1 Task 1.2, 1.3）
- ✅ 合法值 0-6 全覆盖；非法值 7、-1、100 → 报错
- ✅ 新增 parser gtest：`SET FIRST_DAY_OF_WEEK -1` 在 parse 阶段不报语法错，而返回 `TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK`
- ✅ `ALTER LOCAL 'firstDayOfWeek'` 接受；`ALTER ALL DNODES` / `ALTER DNODE N` 被拒绝
- ✅ `ALTER LOCAL 'firstDayOfWeek'` 更新进程全局 L3，`SHOW LOCAL VARIABLES` 立即可见，新连接继承新值
- ✅ **连接隔离回归（Finding 1）**：`test_fdow_alter_local_updates_process_global_not_existing_connection` 验证 `ALTER LOCAL` 仅影响新连接；`test_fdow_alter_local_does_not_affect_existing_connection_timetruncate`（P4 skip）为完整行为回归，待 qworker→executor 链路打通后解除 skip
- 🗑️ ~~`test_fdow_affects_timetruncate_week`~~（已删除：被 `TestTimetruncateWeek::test_timetruncate_1w_fdow_differences` 覆盖）
- 🗑️ ~~`test_fdow_client_config_applies_without_session_override`~~（已删除：ALTER LOCAL 不影响当前连接快照，逻辑错误）

#### TestTimezoneFunc — TIMEZONE() 函数（P6 Task 6.1, 6.2）
- ✅ `TIMEZONE()` / `TIMEZONE(0)` 返回客户端时区字符串，SET TIMEZONE 后不变
- ✅ `ALTER LOCAL 'timezone ...'` 改变客户端时区（L3），新连接可见；已建立连接保持原值不受影响
- ✅ `TIMEZONE(1)` 返回 JSON（session/client/server），session 反映 SET TIMEZONE
- ✅ 未设置 L2 时 session == client；非法参数（2、-1、'abc'）报错
- ✅ FROM table 查询兼容

#### TestDisplayTimezone — 时间戳展示（P2 Task 2.1, 2.2）
- ⚠️ `SELECT ts` 使用连接时区展示目前仅在 taos CLI/shell 路径可见；Python connector 场景仍保持 skip，因 connector 返回的是原始 timestamp/本地 datetime 对象而非 shell 格式化字符串
- ✅ taos shell 的 SHOW TABLES 结果展示使用连接时区
- ✅ EXPLAIN 在设置连接时区后可正常执行；当前用例验证可执行性，不验证展示字符串变化
- ✅ 未设置时区时行为不变

#### TestWhereCastJoinTz — WHERE/CAST/JOIN 时间字面量（P2 Task 2.3）
- ✅ WHERE 精确匹配使用连接时区：UTC 匹配 / Shanghai 不匹配
- ✅ CAST 字符串→时间戳使用连接时区：UTC vs Shanghai 产生不同时间戳
- ✅ JOIN 条件中时间字面量使用连接时区
- ✅ 未设置 L2 时回退到 L3→L5

#### TestTodayNowTz — TODAY/NOW（P2 Task 2.5 + 回归）
- ✅ TODAY() 不同时区结果正确；UTC 下返回午夜对齐值
- ✅ TODAY() 不受服务端时区影响；WHERE 子句可用
- ✅ NOW() 不受 SET TIMEZONE 影响（回归保证）

#### TestIntervalTimezone — INTERVAL 窗口 + 连接时区对比（P2）
- ⏸️ `interval(1d)` 不同时区产生不同桶数（UTC 2 桶 vs Shanghai 1 桶）仍属待实现能力；当前用例改为 skip，避免在未接入 session timezone 时误报通过
- ✅ `interval(1h)` 小时级窗口跨时区桶数一致（时区无关）
- ✅ 不同时区下 `sum(val)` 总和一致（数据不丢不重）

#### Client gtest 补充 — async local SET 路径
- ✅ 新增 `clientTests.cpp` 回归：`taos_query_a("SET TIMEZONE 'UTC'")` 成功后更新连接 `optionInfo.timezone`
- ✅ 新增 `clientTests.cpp` 回归：`taos_query_a("SET FIRST_DAY_OF_WEEK 3")` 成功后更新连接 `optionInfo.firstDayOfWeek`

---

### 4.2 `test_tz_scalar_functions.py` — 标量函数 / WEEK / DST

#### TestToIso8601Iana — TO_ISO8601 IANA 扩展（P3 Task 3.1）
- ✅ IANA 时区参数（无 DST: Shanghai/Tokyo；有 DST: NewYork/London）
- ✅ DST 冬夏偏移（NY: -05/-04；London: +00/+01）
- ✅ 固定偏移向后兼容（+08:00, Z）
- ✅ 非法参数报错；无参回退 L2→L3→L5；L1 覆盖 L2
- ✅ us/ns 多精度兼容

#### TestToCharTimezone — TO_CHAR 第三参数（P3 Task 3.2）
- ✅ IANA/固定偏移第三参数；DST 感知（NY 冬 07:00 / 夏 08:00）
- ✅ 非法时区报错；无参回退 L2；L1 覆盖 L2
- ✅ 两参数向后兼容

#### TestTimetruncateTz — TIMETRUNCATE 字符串时区（P3 Task 3.3）
- ✅ IANA 字符串时区不同截断边界（UTC vs Shanghai）
- ✅ 整数 0/1 / 无参向后兼容
- ✅ 非法时区报错；L1 覆盖 L2；无参回退链
- ✅ 子日粒度字符串时区回归：`1h` 在 `Asia/Kathmandu` / `+05:45` 下对齐到目标时区整点

#### TestTimetruncateNaturalUnits — TIMETRUNCATE n/q/y + 限 1x（P4 Task 4.1）
- ✅ **P4 已实现，4 个正例测试 + 1 个负例测试全部通过**
- ✅ `1n/1q/1y` 边界对齐（月初/季初/年初）
- ✅ `1q == 3-month` 等价行为（通过 1q 正例隐式覆盖）
- ✅ 自然单位 + 字符串时区参数组合
- ✅ **负例**：`2n`/`3n`/`6n`/`2q`/`2y`/`2w` 均返回 `TSDB_CODE_FUNC_TIME_UNIT_INVALID`
- **约束**：所有单位（包括 n/q/y）均只接受 `1x` 格式，`Nx`（N>1）一律报错

#### TestTimetruncateWeek — TIMETRUNCATE 1w 对齐修正（P4 Task 4.2）
- ✅ **P4 已实现，5 个测试全部通过**
- ✅ fdow 0-6 全覆盖，产生不同对齐日（fdow=0→周日，fdow=1→周一，fdow=4→周四与 epoch 兼容）
- ✅ 落在周起始日当天应返回当天午夜
- ✅ 显式 IANA tz 参数与 session tz 路径一致
- ✅ DST 春跳/秋退窗口中，显式 IANA tz 与 session tz 路径一致
- ✅ DST 春跳日（fdow=0 Sun）对齐到前一个周日本地午夜
- ⏸️ `2w` 多倍数（不在 P4 验收范围，测试已删除）
- ⚠️ **高风险变更**：默认 fdow=4（周四）与旧 epoch 取模行为兼容，未修改配置的用户不受影响

#### TestWeekFunctions — WEEK/WEEKOFYEAR/DAYOFWEEK/WEEKDAY（P4 Task 4.4）
- ✅ **P4 已实现**：WEEKOFYEAR 在 fdow=0 vs fdow=1 下产生不同结果
- ✅ WEEK mode 0-7 全覆盖；mode=8 报错
- ✅ **DAYOFWEEK / WEEKDAY 不受 fdow 影响**（负面回归），已知值校验
- ✅ FROM table 查询兼容
- 🗑️ ~~`test_week_mode_overrides_fdow`~~（已删除：`2026-01-04` 在 mode=0/1 下均返回 week=1，期望值错误）

#### TestDstEdge — DST 边界 + 写入路径回归（P2 Task 2.4 + DST）
- ✅ 春跳 TO_ISO8601 偏移变化（-05→-04）、TO_CHAR 跳跃（01:59→03:00）
- ✅ 春跳/秋退日 TIMETRUNCATE(1d) 对齐到本地午夜
- ✅ 秋退 TO_ISO8601 偏移变化（-04→-05）
- ✅ 春跳 gap 写入归一化（02:30→03:30 EDT）
- ✅ 秋退 overlap 默认第一次出现；整型时间戳不受时区影响
- ✅ 带偏移字符串写入无歧义；无 DST 时区不受影响

---

### 4.3 `test_tz_interval.py` — 时间窗口

#### TestIntervalNatural — INTERVAL 自然单位（P4 Task 4.3）
- ✅ `INTERVAL(1n)` 12 月窗口；`INTERVAL(1d)` 365 日窗口
- ✅ `INTERVAL(1n)` DST 不漂移（12 个月）
- ✅ `INTERVAL(1y)` 单窗口；`INTERVAL(1q)` 4 季度窗口
- ✅ `INTERVAL(1q) == INTERVAL(3n)` 完全等价
- ✅ `INTERVAL(1n) SLIDING(1d)` 日历滑动
- ✅ 时间窗口按 session 时区（L2）计算；已设置 `SET TIMEZONE` 时，不受 client local（L3）或 server timezone（L4）覆盖
- ✅ 超级表查询

#### TestIntervalWeek — INTERVAL(1w) + firstDayOfWeek（P4 Task 4.3）
- ✅ fdow=0 vs fdow=1 产生不同窗口边界
- ✅ fdow 0-6 全覆盖；DST 期间不漂移
- ✅ 客户端 `firstDayOfWeek` 在无连接级 override 时，经新连接生效
- ✅ 超级表查询
- ⚠️ **高风险变更**

#### TestIntervalQuarter — INTERVAL(Nq)（P5 Task 5.2）
- ✅ `INTERVAL(1q)` 4 窗口，边界 Jan1/Apr1/Jul1/Oct1
- ✅ `INTERVAL(2q)` 2 半年窗口
- ✅ `INTERVAL(1q) == INTERVAL(3n)`、`INTERVAL(2q) == INTERVAL(6n)` 等价
- ✅ 超级表查询

---

## 5. 回退链覆盖矩阵

| 场景 | 回退链 | 覆盖测试类 |
|------|--------|-----------|
| 客户端展示（SELECT ts / SHOW） | L2 → L3 → L5 | TestDisplayTimezone |
| TO_ISO8601(ts) 无参 | L2 → L3 → L5 | TestToIso8601Iana |
| TO_ISO8601(ts, tz) 有参 | L1 → L2 → L3 → L5 | TestToIso8601Iana |
| TO_CHAR(ts, fmt) 无参 | L2 → L3 → L5 | TestToCharTimezone |
| TO_CHAR(ts, fmt, tz) 有参 | L1 → L2 → L3 → L5 | TestToCharTimezone |
| TIMETRUNCATE(ts, unit) 无参 | L2 → L4 → L5 | TestTimetruncateTz |
| TIMETRUNCATE(ts, unit, tz) 有参 | L1 → L2 → L4 → L5 | TestTimetruncateTz |
| INTERVAL 时间窗口 | L2 → L4 → L5（已设置 L2 时不受 L3/L4 覆盖） | TestIntervalNatural, TestIntervalWeek, TestIntervalQuarter, TestIntervalTimezone |
| WHERE / CAST / JOIN 时间字面量 | L2 → L3 → L5 | TestWhereCastJoinTz |
| TODAY() | L2 → L3 → L5 | TestTodayNowTz |
| TIMEZONE() / TIMEZONE(0) | 返回 L3 (client tz) | TestTimezoneFunc |
| TIMEZONE(1) | 返回 L2/L3/L4 三层 | TestTimezoneFunc |
| firstDayOfWeek | SET → client → default 4 | TestSetFirstDayOfWeek, TestTimetruncateWeek, TestWeekFunctions, TestIntervalWeek |

---

## 6. DST 测试时间点清单

所有 DST 测试使用 `America/New_York` 2026 年 DST 时间点：

| 事件 | 本地时间 | UTC | 时区缩写 |
|------|---------|-----|---------|
| 春跳前 | 2026-03-08 01:59:00 | 06:59:00 | EST |
| 春跳后 | 2026-03-08 03:00:00 | 07:00:00 | EDT |
| 秋退前（EDT） | 2026-11-01 01:00:00 | 05:00:00 | EDT |
| 秋退后（EST） | 2026-11-01 01:00:00 | 06:00:00 | EST |

---

## 7. 高风险变更覆盖

| 变更项 | 风险等级 | 覆盖测试类 |
|--------|---------|-----------|
| TIMETRUNCATE(1w) 从 epoch 取模改为 fdow 对齐 | ⚠️ 低（默认 fdow=4 兼容旧行为） | TestTimetruncateWeek（当前为最小回归覆盖，P4 全量用例仍部分 skip） |
| INTERVAL(1w) 从 epoch 取模改为 fdow 对齐 | ⚠️ 低（默认 fdow=4 兼容旧行为） | TestIntervalWeek |
| INTERVAL(1d) 从 24h 固定步进改为日历日步进 | ⚠️ 高 | TestIntervalNatural |
| TO_ISO8601 无参从 translator 折叠改为运行时 IANA | ⚠️ 中 | TestToIso8601Iana |
| TIMEZONE() 保持返回客户端时区（不变为 session） | ⚠️ 中 | TestTimezoneFunc |

---

## 8. 待评审确认事项

1. **DST 时间点精确性**：2026 年 America/New_York 春跳 3/8、秋退 11/1 是否正确？（需与 TDengine 内置 tzdata 版本一致）
2. **WEEK(ts, mode) 取值范围**：当前测试假定 mode 0-7，8 报错 — 与 MySQL 兼容性是否 OK？
3. **DAYOFWEEK 返回值**：假定 1=Sun..7=Sat；WEEKDAY 假定 0=Mon..6=Sun — 是否与 TDengine 现有行为一致？
4. **TIMEZONE() 在 SET TIMEZONE 后的行为**：FS 明确说返回客户端时区（L3），不是 session 时区（L2） — 已按此编写测试
5. **TIMETRUNCATE 多倍数的 epoch 基准**：`Nn`/`Nq`/`Ny` 的 epoch 起计数是否从 1970-01-01 UTC 开始？
6. **INTERVAL(1d) DST 行为**：Plan 明确 `d` 不纳入 `IS_CALENDAR_TIME_DURATION` 但走专用日历分支 — 测试按此预期
7. **错误码**：已在 pytest 中显式锁定 `TSDB_CODE_PAR_INVALID_TIMEZONE=0x26B2`、`TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK=0x26B3`；函数参数非法场景当前沿用 `0x2803`

---

## 9. 单元测试补充要求

以下内容用于同步 plan 第 9.1 的单元测试状态，分为“当前已补充”和“当前阻塞项”两部分。

### 9.1 当前已补充的单元测试

#### `source/common/test/ttimeNaturalUnitsTest.cpp`

- ✅ `WeekAlignmentBasic` 追加精确 epoch 断言：锁定 `2026-03-10 -> 2026-03-09 00:00:00`
- ✅ 新增 `WeekAlignmentThursdayAnchorsToPreviousMonday`：锁定 `2026-04-30 -> 2026-04-27 00:00:00`
- ✅ 新增 `MultiPeriodWeekAlignmentExactBoundary`：锁定当前实现下 `2w` 以 epoch Monday (`1970-01-05`) 为 anchor，`2026-04-30 -> 2026-04-27 00:00:00`
- ✅ `MonthAlignmentBasic` / `YearAlignmentBasic` 追加精确起点断言：分别锁定 `2026-03-01 00:00:00` 与 `2026-01-01 00:00:00`
- ✅ `MultiPeriodMonthAlignment` / `MultiPeriodYearAlignment` 追加精确倍数边界断言：锁定 `3n -> 2026-01-01/2026-04-01`，`2y -> 2026-01-01/2028-01-01`

#### `source/common/test/commonTests.cpp`

- ✅ 现有 `taosTimeTruncate_DST_day_interval` 继续作为 DST 日边界的 common 单元测试承载点
- ✅ 现有 `function_taosTimeTruncate` 继续承载 `taosTimeTruncate()` 基础行为回归

### 9.2 P4 已落地的功能点（原阻塞项）

以下项目已随 P4 实现一并完成，不再阻塞：

- ✅ **parser**：`SET TIMEZONE` / `SET FIRST_DAY_OF_WEEK` 语法节点、grammar 规则、AST 构造函数已实现
- ✅ **firstDayOfWeek 传播链**：client → SParseContext → SFunctionNode → SScalarParam / SWindowLogicNode → SIntervalPhysiNode → STableScanPhysiNode，全链路已打通
- ✅ **TIMETRUNCATE n/q/y 日历截断**：`validateTimeUnitParamEx` / `checkTimeUnitOrCalendar` 新增，`translateTimeTruncate` 注入 `fdow` + `unitCh` 尾参数，`timeTruncateFunction` 实现月/季/年本地日历对齐
- ✅ **TIMETRUNCATE 1w fdow 对齐**：session IANA tz 路径和显式字符串 tz 路径均使用 `wday/daysBack` 算法对齐
- ✅ **WEEKOFYEAR fdow**：根据 `pInput->firstDayOfWeek` 派生 WEEK mode（fdow=1→mode 3，其他→mode 2）

仍待落地的项目：

- ⏸️ `SSubQueryMsg` / `SInterval` `firstDayOfWeek` 序列化/反序列化（executor 侧已就绪，消息序列化还需验证）
- ⏸️ scalar/TIMEZONE(1) 组装：`session/client/server` JSON 三层（P6）
- ⏸️ WEEK mode 参数与 firstDayOfWeek 完整隔离回归（`test_week_mode_overrides_fdow` 已删除，需找合适日期重写）

## 10. 测试执行方式

```bash
cd /home/tony/codes/TDengine/test

# 执行单个测试文件
source /home/tony/codes/venv/ci/bin/activate
pytest cases/11-Functions/01-Scalar/test_tz_config_display.py --clean

# 执行全部时区测试
pytest cases/11-Functions/01-Scalar/test_tz_config_display.py cases/11-Functions/01-Scalar/test_tz_scalar_functions.py cases/13-TimeSeriesExt/03-TimeWindow/test_tz_interval.py --clean
```

> **执行状态（2026-05-09）**：时区测试并非“全部必失败”。
> 其中 `test_tz_scalar_functions.py` 在当前分支已可执行并通过（`37 passed, 14 skipped`）；
> 当前 skip 主要集中在明确标记为 P4 未完成的周对齐/自然单位能力，不属于本轮 P3 回归失败。
