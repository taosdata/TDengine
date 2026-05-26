# 时区与查询改造 TS

## 1. 修订记录

| 编写日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2026-05-18 | 1.0 | Tony Zhang | 初稿 |
| 2026-05-19 | 1.1 | Tony Zhang | 同步 `hash_join` 支持 `TIMETRUNCATE` 主键后的测试说明 |
| 2026-05-23 | 1.2 | Tony Zhang | 增补 `ALTER LOCAL timezone` 统一校验用例与固定偏移兼容语义说明 |

## 2. 文档范围

本说明仅基于以下 4 个测试文件：

1. `test/cases/11-Functions/01-Scalar/test_tz_config_display.py`
2. `test/cases/11-Functions/01-Scalar/test_tz_scalar_functions.py`
3. `test/cases/13-TimeSeriesExt/03-TimeWindow/test_tz_interval.py`
4. `test/cases/11-Functions/01-Scalar/test_tz_config_unification.py`

不包含其他 gtest/文档计划状态。

## 3. 测试文件清单与映射

| # | 文件 | Class 列表 | 主题 |
| --- | --- | --- | --- |
| 1 | `test_tz_config_display.py` | `TestSetTimezone`, `TestSetFirstDayOfWeek`, `TestTimezoneFunc`, `TestDisplayTimezone`, `TestWhereCastJoinTz`, `TestTodayNowTz`, `TestIntervalTimezone` | 配置、展示、字面量解析、TODAY/NOW、TIMEZONE()、INTERVAL 行为 |
| 2 | `test_tz_scalar_functions.py` | `TestToIso8601Iana`, `TestToCharTimezone`, `TestTimetruncateTz`, `TestTimetruncateNaturalUnits`, `TestTimetruncateUnitMultiplierValidation`, `TestTimetruncateWeek`, `TestWeekFunctions`, `TestSessionFirstDayOfWeekFunction`, `TestDstEdge`, `TestToIso8601FixedOffset`, `TestJoinDst` | 标量函数、固定偏移兼容路径、TIMETRUNCATE、WEEK/WEEKDAY、DST、DST JOIN、first_day_of_week() |
| 3 | `test_tz_interval.py` | `TestIntervalNatural`, `TestIntervalWeek`, `TestIntervalQuarter` | INTERVAL 自然单位、周窗口、季度窗口 |
| 4 | `test_tz_config_unification.py` | `TestTimezoneConfigUnification` | `ALTER LOCAL timezone` 校验统一、Windows 名称兼容、`GMT` 拒绝、固定偏移配置语义 |

## 4. 关键覆盖点

### 4.1 配置与会话行为（`test_tz_config_display.py`）

#### `TestSetTimezone`
- 验证合法 IANA 时区与固定偏移时区（含 `+14:00/-14:00` 边界）。
- 验证 `SET TIMEZONE ''` 退化到 UTC。
- 验证非法输入返回 `0x26B2`。
- 验证同连接多次切换生效，以及 reconnect 后隔离。
- 当前文件级 `pytestmark` 对 Windows 整体跳过，原因是该套件中的 `SET TIMEZONE` 用例不支持 Windows。

#### `TestSetFirstDayOfWeek`
- 验证 `SET FIRST_DAY_OF_WEEK` 合法范围 `0..6`。
- 验证非法值 `7/100/-1` 返回 `0x26B3`。
- 验证 `ALTER LOCAL 'firstDayOfWeek'` 可配置，`ALTER ALL DNODES`/`ALTER DNODE` 被拒绝（`0x0119`）。
- 验证 `ALTER LOCAL` 更新 process-global 配置后，仅影响新连接快照，不影响已建立连接（通过新旧连接 `TIMETRUNCATE(1w)` 对比）。

#### `TestTimezoneFunc`
- 验证 `TIMEZONE()` 返回连接级时区（SET 后立即可见）。
- 验证未 SET 时回退到连接快照（重连后恢复 baseline）。
- 验证 `ALTER LOCAL timezone` 仅对新连接可见。
- 验证 `timezone(<arg>)` 非法。
- 验证 `TIMEZONE()` 可用于 `FROM table` 查询上下文。

#### `TestDisplayTimezone` / `TestWhereCastJoinTz` / `TestTodayNowTz`
- `SELECT ts` CLI 展示受连接时区影响。
- `SHOW/EXPLAIN` 在连接时区切换后可执行。
- 未显式 `SET TIMEZONE` 时，展示与时间字面量解析维持 `L3 -> L5` 旧回退行为。
- `WHERE/CAST/JOIN` 时间字面量按连接时区解析。
- `TODAY()` 跟随连接时区，`NOW()` 保持回归兼容（不受时区切换影响）。
- `TODAY()` 额外覆盖 UTC 午夜对齐、`WHERE` 子句可用，以及不受 server timezone（L4）影响。

#### `TestIntervalTimezone`
- 校验 `interval(1d)` 不同时区的桶划分差异。
- 校验 `interval(1h)` 跨时区桶数一致。
- 校验跨时区分桶后总和一致（不丢不重）。
- 当前该文件中的 `interval(1d)` 覆盖已按 session timezone 生效路径断言，不再是“预留/待接线”状态。

### 4.2 标量函数与 DST（`test_tz_scalar_functions.py`）

#### `TestToIso8601Iana` / `TestToCharTimezone` / `TestToIso8601FixedOffset`
- 覆盖 IANA 时区、固定偏移、DST 冬夏偏移。
- 覆盖 L1 参数覆盖 L2 连接时区。
- 覆盖非法时区报错与多精度兼容（us/ns）。
- 额外覆盖 `TO_ISO8601` 固定偏移专门路径：`+0800`、`-0500`、`Z`、`+05:30`，确认 legacy fixed-offset 解释仍然生效（例如 `+08:00` 仍表示东八区）。
- 额外覆盖 `TO_CHAR` 走通用时区校验路径时的兼容语义：固定偏移字符串保留符号并按 POSIX 解释，因此 `+08:00` 在该路径下表示 `UTC-08:00`。

#### `TestTimetruncateTz`
- 覆盖 `TIMETRUNCATE(ts, unit, tz_string)`、整数兼容参数 `0/1` 与无参回退行为。
- 覆盖字符串时区在子日粒度（如 `+05:45`）下的对齐正确性。
- 覆盖 `TIMETRUNCATE` 的字符串时区参数走通用时区校验路径，固定偏移保留符号并按 POSIX 语义解释。

#### `TestTimetruncateNaturalUnits` + `TestTimetruncateUnitMultiplierValidation`
- 覆盖 `1n/1q/1y` 对齐语义。
- 覆盖 `n/q/y` 与显式时区参数组合行为。
- 覆盖所有单位 `N>1` 的拒绝行为（`2n/2q/2y/2w/2d/2h/2m/2s/2a/2u/2b/...`），并覆盖带时区参数、表列参数两条错误路径。

#### `TestTimetruncateWeek` / `TestWeekFunctions`
- 覆盖 `TIMETRUNCATE(1w)` 与 `FIRST_DAY_OF_WEEK` 的对齐关系（0..6）。
- 覆盖显式 IANA 参数路径与 session 路径一致性。
- 覆盖 DST 周窗口下的一致性。
- 覆盖 `SET FIRST_DAY_OF_WEEK` 非法值的格式化错误信息，确认错误文本不残留 `%d` 占位符。
- 覆盖 `WEEK(mode 0..7)` 合法、`WEEK(...,8)` 非法，以及 `WEEKOFYEAR`/`DAYOFWEEK`/`WEEKDAY` 与 fdow 的关系。

#### `TestSessionFirstDayOfWeekFunction`
- 新增覆盖：
	- `SET FIRST_DAY_OF_WEEK <v>` 后，`select first_day_of_week()` 返回 `<v>`。
	- 验证值样本：`0/1/3/6`。

#### `TestDstEdge`
- 覆盖春跳/秋退的 `TO_ISO8601`、`TO_CHAR`、`TIMETRUNCATE(1d)` 行为。
- 覆盖 DST 重叠时间写入与显式偏移写入。
- 覆盖整型时间戳写入不受时区影响。
- 当前代码中春跳缺失时刻写入归一化检查已保留测试实现，但在 `test_dst_edge()` 中处于跳过状态，原因是服务端当前直接拒绝 spring-gap 时间。

#### `TestJoinDst`
- 覆盖 `JOIN ON TIMETRUNCATE(ts, 1d)` 在 IANA 时区下的 DST 感知日对齐回归。
- 覆盖 DST 同一本地日应能 join 命中、跨本地日应返回 0 行。
- 覆盖非 DST 时区（`Asia/Shanghai`）下同日 join 正常。
- 覆盖默认 merge join 与显式 `hash_join` 两条路径结果一致。
- 覆盖 `hash_join` 提示下 `TIMETRUNCATE` 连接键可正常执行，不再预期 planner/internal error。

### 4.3 时间窗口（`test_tz_interval.py`）

#### `TestIntervalNatural`
- `INTERVAL(1n/1d/1y/1q)` 桶数与边界。
- `INTERVAL(1q)==INTERVAL(3n)` 等价性。
- `SLIDING(1d)` 组合行为。
- 验证 INTERVAL 优先使用 session 时区（L2），并分别对 client local（L3）、server timezone（L4）、fixed-offset session timezone 做隔离对比。
- 覆盖 supertable 查询路径与 DST 月窗口 no-drift。

#### `TestIntervalWeek`
- 覆盖 `INTERVAL(1w)` 在 fdow 变化时的 `_wstart` 变化。
- 覆盖 fdow 0..6 全范围、DST 周、supertable 路径。
- 覆盖 `ALTER LOCAL firstDayOfWeek` + reconnect 生效。

#### `TestIntervalQuarter`
- 覆盖 `INTERVAL(1q)` 边界（Jan/Apr/Jul/Oct）。
- 覆盖 `INTERVAL(2q)` 桶数。
- 覆盖 `1q==3n`、`2q==6n` 等价性。

### 4.4 配置路径统一（`test_tz_config_unification.py`）

#### `TestTimezoneConfigUnification`
- 覆盖 `ALTER LOCAL 'timezone'` 接受 IANA 名称、裸固定偏移、`UTC±...` 与 Windows 标准时区名称。
- 覆盖 `GMT` / `GMT±...`、有歧义缩写（`CST`/`EST`/`PST`/`IST`）、单数字小时偏移、越界偏移、垃圾输入统一返回 `0x26B2`。
- 覆盖配置路径的固定偏移兼容语义：`+08:00`、`UTC+08:00` 在新连接上表现为 `UTC-08:00`；`-05:30`、`UTC-05:30` 在新连接上表现为 `UTC+05:30`。
- 覆盖 `ALTER LOCAL` 只影响新连接，不回写既有连接快照。

## 5. 回退链与优先级覆盖矩阵

| 场景 | 预期优先级链路 | 覆盖类 |
| --- | --- | --- |
| `TIMEZONE()` | L2 -> L3 -> L5 | `TestTimezoneFunc` |
| `TO_ISO8601(ts)` 无参 | L2 -> L3 -> L5 | `TestToIso8601Iana` |
| `TO_ISO8601(ts, tz)` 有参 | L1 -> L2 -> L3 -> L5 | `TestToIso8601Iana`, `TestToIso8601FixedOffset` |
| `TO_CHAR(ts, fmt)` 无参 | L2 -> L3 -> L5 | `TestToCharTimezone` |
| `TO_CHAR(ts, fmt, tz)` 有参 | L1 -> L2 -> L3 -> L5 | `TestToCharTimezone` |
| `TIMETRUNCATE(ts, unit)` 无参 | L2（会话） | `TestTimetruncateTz`, `TestTimetruncateWeek` |
| `TIMETRUNCATE(ts, unit, tz)` 有参 | L1 -> L2 | `TestTimetruncateTz`, `TestTimetruncateWeek` |
| `INTERVAL` | L2 优先，且应不被 L3/L4 覆盖 | `TestIntervalNatural`, `TestIntervalWeek`, `TestIntervalQuarter`, `TestIntervalTimezone` |
| 时间字面量解析（WHERE/CAST/JOIN） | L2 -> L3 -> L5 | `TestWhereCastJoinTz` |
| `JOIN ON TIMETRUNCATE(..., 1d)` | L2 + IANA DST 感知日边界 | `TestJoinDst` |
| `first_day_of_week()` | 当前 session fdow | `TestSessionFirstDayOfWeekFunction` |
| `ALTER LOCAL 'timezone'` | L3 更新后仅影响新连接 | `TestTimezoneConfigUnification`, `TestTimezoneFunc` |

## 6. 错误码覆盖

| 常量 | 值 | 主要覆盖场景 |
| --- | --- | --- |
| `ERR_INVALID_TIMEZONE` | `0x26B2` | `SET TIMEZONE` / 时区参数非法 |
| `ERR_INVALID_FIRST_DAY_OF_WEEK` | `0x26B3` | `SET FIRST_DAY_OF_WEEK` 非法 |
| `ERR_INVALID_FUNCTION_PARAM` | `0x2803` | `WEEK(...,8)` 等非法参数 |
| `ERR_INVALID_CFG` | `0x0119` | 非 client-scope 配置 `firstDayOfWeek` |

补充说明：

- `test_tz_config_display.py` 还定义了 `ERR_INVALID_DNODE_CFG = 0x03B2`，但当前用例未实际消费。
- `TIMETRUNCATE` 的 `N>1` 非法主要通过错误信息 `Invalid time unit : timetruncate` 断言，而非错误码断言。

## 7. 高风险变更项与对应回归

| 风险项 | 覆盖类 |
| --- | --- |
| `TIMETRUNCATE(1w)` 改为 fdow 对齐 | `TestTimetruncateWeek` |
| `INTERVAL(1w)` 改为 fdow 对齐 | `TestIntervalWeek` |
| `INTERVAL(1d)` 时区边界行为 | `TestIntervalNatural`, `TestIntervalTimezone` |
| `TIMEZONE()` / `first_day_of_week()` 会话可见性 | `TestTimezoneFunc`, `TestSessionFirstDayOfWeekFunction` |
| 固定偏移字符串路径未生效或入口语义混淆 | `TestToIso8601FixedOffset`, `TestToCharTimezone`, `TestTimetruncateTz`, `TestTimezoneConfigUnification` |
| DST 日边界参与 JOIN 产生错误匹配/漏匹配，或 hash join / merge join 结果不一致 | `TestJoinDst` |
| `ALTER LOCAL` 误污染既有连接快照 | `TestSetFirstDayOfWeek`, `TestTimezoneFunc`, `TestIntervalWeek` |
