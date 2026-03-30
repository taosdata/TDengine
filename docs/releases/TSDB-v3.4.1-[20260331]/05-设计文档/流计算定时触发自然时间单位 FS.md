# 流计算定时触发自然时间单位 FS

### 1. 修订记录

| **编写日期** | **发布日期** | **版本** | **修订人** | **主要修改内容** |
| --- | --- | --- | --- | --- |
| 2026-02-27 | 2026-02-27 | 0.1 | 邝金清 | 初稿 |

### 2. 背景

TDengine 流计算的定时触发（`PERIOD`）目前仅支持毫秒（`a`）、秒（`s`）、分钟（`m`）、天（`d`）等固定时长单位。这些单位以固定时间间隔触发，无法对齐自然日历边界（如每周一、每月 1 日、每年 1 月 1 日），导致用户无法直接实现"每周汇总"、"每月报表"、"年度统计"等业务场景，需要在应用层做额外的时间对齐处理。
本特性在 `PERIOD` 语法中新增三个自然时间单位：周（`w`）、月（`n`）、年（`y`），并为所有单位（含现有单位）的 offset 参数新增小时（`h`）支持，使用户能够以自然日历周期精确控制流计算的触发时刻。

### 3. 定义

- **自然时间边界（Natural Time Boundary）**：触发对齐的基准时间点，以服务端时区的 00:00:00 为准。周单位对齐每周一 00:00:00（服务端时区），月单位对齐每月 1 日 00:00:00（服务端时区），年单位对齐每年 1 月 1 日 00:00:00（服务端时区）。
- **触发偏移量（Trigger Offset）**：`PERIOD` 的第二个可选参数，表示在自然时间边界基础上的正向偏移量，由单一数值和单一单位表达（如 `1d`、`12h`），不支持多单位组合（如 `2d12h`）。
- **epoch 基准**：多倍数周期的边界对齐参考点，为服务端时区的 1970-01-01 00:00:00。
- **最短月份组合**：用于月单位 offset 合法性静态校验的基准，`PERIOD(Nn, offset)` 中 offset 必须严格小于 N 个连续月份中最短可能的总天数（如 `1n` 对应 28 天，`2n` 对应 59 天）。

### 4. 行为说明

#### 4.1 新增时间单位语法

`PERIOD` 的间隔参数新增以下单位：

| **单位** | **含义** | **示例** |
| --- | --- | --- |
| `w` | 周 | `PERIOD(1w)` — 每周触发一次 |
| `n` | 月 | `PERIOD(1n)` — 每月触发一次 |
| `y` | 年 | `PERIOD(1y)` — 每年触发一次 |

支持整数倍数，如 `PERIOD(2w)`、`PERIOD(3n)`、`PERIOD(2y)`。

#### 4.2 自然时间边界对齐

不同单位的默认触发时刻（不指定 offset 时）：

| **单位** | **默认触发时刻** |
| --- | --- |
| `w` | 每周一 00:00:00（服务端时区） |
| `n` | 每月 1 日 00:00:00（服务端时区） |
| `y` | 每年 1 月 1 日 00:00:00（服务端时区） |

多倍数周期以服务端时区的 epoch（1970-01-01 00:00:00 服务端时区）为基准整除对齐，保证所有任务触发时刻全局一致。例如 `PERIOD(2w)` 触发时刻为距 epoch 整数倍双周的周一，与任务创建时间无关。

#### 4.3 offset 参数

`PERIOD` 的第二个参数 offset 对所有单位均生效（包括现有的 `a`/`s`/`m`/`d` 和新增的 `w`/`n`/`y`）。offset 表示在自然时间边界基础上的正向偏移，新增支持小时单位 `h`。
**offset 合法单位**：`a`（毫秒）、`s`（秒）、`m`（分钟）、`h`（小时）、`d`（天，本特性新增）
**offset 格式**：单一数值 + 单一单位，不支持多单位组合。
```sql
-- 合法：每周二 00:00:00（服务端时区）触发
CREATE STREAM s1 TRIGGER PERIOD(1w, 1d) ...

-- 合法：每月 15 日 00:00:00（服务端时区）触发
CREATE STREAM s3 TRIGGER PERIOD(1n, 14d) ...

-- 合法：每年 2 月 1 日 00:00:00（服务端时区）触发
CREATE STREAM s4 TRIGGER PERIOD(1y, 31d) ...

-- 非法：不支持多单位组合 offset
-- CREATE STREAM s2 TRIGGER PERIOD(1w, 2d12h30m) ...
```

不指定 offset 时，触发时刻为自然时间边界本身：
```sql
-- 每周一 00:00:00（服务端时区）触发
CREATE STREAM s5 TRIGGER PERIOD(1w) ...

-- 每月 1 日 00:00:00（服务端时区）触发
CREATE STREAM s6 TRIGGER PERIOD(1n) ...
```

#### 4.4 首次触发的数据窗口

任务创建时刻在周期中间时，第一次触发的数据窗口从上一个自然边界（含 offset）回溯开始，到第一个自然边界结束，窗口为完整的一个周期，包含任务创建前的历史数据。
示例：用户在周三创建 `PERIOD(1w)` 任务，下一个周一触发时，数据窗口为上一个周一 00:00:00 到本周一 00:00:00（完整一周）。

#### 4.5 出错处理

**offset 超出触发周期**：
```plaintext
[0x3xxx] PERIOD offset must be strictly less than the interval.
  PERIOD(1w, 7d): offset 7d equals interval 1w (7 days), which is not allowed.
  Valid offset range: [0, 7d)
```

**月单位 offset 静态溢出校验**：
```plaintext
[0x3xxx] PERIOD offset may overflow in shortest month.
  PERIOD(1n, 28d): offset 28d >= 1 * 28d (minimum month threshold).
  Valid offset range for PERIOD(1n): [0, 28d)
  Valid offset range for PERIOD(2n): [0, 56d)
```

**无效时间单位**：
```plaintext
[0x3xxx] Invalid time unit 'x' in PERIOD interval.
  Supported interval units: a (millisecond), s (second), m (minute), h (hour), d (day), w (week), n (month), y (year).
  Supported offset units: a (millisecond), s (second), m (minute), h (hour), d (day).
```

### 5. 性能

新增单位的触发时间计算为纯整数运算（epoch 整除），不引入额外的 I/O 或锁竞争，对写入和查询性能无影响。
月/年单位因涉及日历计算（月份天数、闰年判断），计算复杂度略高于固定时长单位，但仍为 O(1) 操作，对 Trigger 性能影响可忽略不计。

### 6. 安全

本特性仅扩展 SQL 语法解析和调度逻辑，不涉及新的网络接口、权限模型或数据访问路径，对安全无额外影响。

### 7. 兼容性

**向后兼容**：现有使用 `a`/`s`/`m`/`d` 单位的流计算任务行为不变，offset 参数的现有语义不变（新增 `h` 单位不影响已有配置）。
**元数据兼容**：新增单位需要在流任务元数据中存储，升级后旧版本无法识别含 `w`/`n`/`y` 单位的任务。降级场景下，含新单位的流任务将无法正常运行，需在降级前手动删除相关任务。

### 8. 运维

- 升级后无需重建现有流计算任务，新单位仅对新创建的任务生效。

### 9. 使用场景

#### 9.1 **场景 1：每周业务汇总报表**

每周一生成上周的设备运行汇总数据：
```sql
CREATE STREAM weekly_summary
TRIGGER PERIOD(1w)
INTO summary_table
AS SELECT AVG(current), MAX(voltage) FROM meters
   WHERE ts >= PERIOD_START AND ts < PERIOD_END;
```

#### 9.2 **场景 2：每周二发送周报（offset 示例）**

业务要求在每周二 00:00:00 触发，覆盖上周二到本周二的数据：
```sql
CREATE STREAM weekly_report
TRIGGER PERIOD(1w, 1d)
INTO report_table
AS SELECT COUNT(*), SUM(energy) FROM meters;
```

#### 9.3 **场景 3：每月财务对账**

每月 1 日触发，覆盖上月完整数据：
```sql
CREATE STREAM monthly_billing
TRIGGER PERIOD(1n)
INTO billing_table
AS SELECT meter_id, SUM(energy) FROM meters GROUP BY meter_id;
```

#### 9.4 **场景 4：每季度统计（多倍数月）**

每 3 个月触发一次，对齐 1/4/7/10 月（基于 epoch 整除）：
```sql
CREATE STREAM quarterly_stats
TRIGGER PERIOD(3n)
INTO quarterly_table
AS SELECT AVG(current), AVG(voltage) FROM meters;
```

#### 9.5 **场景 5：年度数据归档**

每年 1 月 1 日触发，归档上一年全量数据：
```sql
CREATE STREAM yearly_archive
TRIGGER PERIOD(1y)
INTO archive_table
AS SELECT * FROM meters;
```

#### 9.6 **场景 6：每月 15 日结算（月 + offset）**

每月 15 日 00:00:00 触发结算任务：
```sql
CREATE STREAM mid_month_settlement
TRIGGER PERIOD(1n, 14d)
INTO settlement_table
AS SELECT meter_id, SUM(energy) FROM meters GROUP BY meter_id;
```

### 10. 约束和限制

**约束**：
- offset 单位仅支持 `a`/`s`/`m`/`h`/`d`，不支持 `w`/`n`/`y` 作为 offset 单位。
- offset 必须为正值且严格小于触发周期（`<`，不含等于）。
- 月单位 offset 以 28 天/月为基准静态校验：`PERIOD(Nn, offset)` 要求 offset < N × 28 天，以此类推。
- offset 只支持单一数值加单一单位（如 `1d`、`12h`），不支持多单位组合（如 `2d12h`）。
- 时间边界计算基于服务端时区，对齐服务端时区的 00:00:00；夏令时切换时以服务端时区的挂钟时间为准。
- 不支持负 offset（即触发时刻不能早于自然边界）。
- 不支持"每月最后一天"等相对日历表达。
**限制**：
- 多倍数周期（如 `PERIOD(2w)`）的触发时刻由 epoch 基准决定，与任务创建时间无关；用户无法自定义对齐基准点。
- 首次触发会回溯到上一个自然边界，可能包含任务创建前的历史数据，用户需确保历史数据已写入或接受空结果。

### 11. 常见错误和排查

| 错误场景 | 错误信息关键词 | 排查方法 |
| --- | --- | --- |
| offset >= 触发周期 | `offset must be strictly less than` | 检查 offset 值是否小于间隔，如 `PERIOD(1w, 7d)` 应改为 `PERIOD(1w, 6d)` 或更小值 |
| 月单位 offset 可能溢出 | `offset may overflow in shortest month` | 减小 offset 值，`PERIOD(1n, offset)` 要求 offset < 28 天 |
| 无效间隔单位 | `Invalid time unit` in interval | 检查间隔单位，合法值为 `a/s/m/h/d/w/n/y` |
| 无效 offset 单位 | `Invalid time unit` in offset | 检查 offset 单位，合法值为 `a/s/m/h/d`（不支持 `w/n/y`） |
| 服务重启后任务未触发 | - | 检查 Trigger 日志，确认 epoch 基准计算的下次触发时间是否正确；确认任务状态为 running |

### 12. 可观测性

- **taos shell**：视图 `information_schema.ins_streams` 中，新单位的 `PERIOD` 参数以原始语法显示（如 `PERIOD(1w, 1d)`）。

### 13. 安装和卸载

本特性为纯软件逻辑扩展，不引入新的配置文件、系统表或存储格式变更（流任务元数据中新增单位的序列化需向后兼容）。安装和卸载流程与标准版本升级/降级一致，无额外步骤。
降级注意事项：降级前需手动删除所有使用 `w`/`n`/`y` 单位的流计算任务，否则旧版本启动后这些任务将处于异常状态。

### 14. 文档

**需要修改官网文档**：
- 流计算 `PERIOD` 语法说明页：新增 `w`/`n`/`y` 单位描述、自然边界对齐规则、offset 合法范围说明、错误码说明。
- 流计算使用示例页：新增按周/月/年触发的典型 SQL 示例（参考第 9 节使用场景）。
**需要修改企业版文档**：无

### 15. 参考文档

无

### 16. 附录

#### 16.1 offset 合法范围速查表

| 触发间隔 | offset 最大合法值（严格小于） | 说明 |
| --- | --- | --- |
| `1w` | < 7 天（604800 秒） | 固定 7 天 |
| `2w` | < 14 天 | 固定 14 天 |
| `1n` | < 28 天（1 × 28） | 28 天/月基准 |
| `2n` | < 56 天（2 × 28） | 28 天/月基准 |
| `3n` | < 84 天（3 × 28） | 28 天/月基准 |
| `Nn` | < N × 28 天 | 通用公式 |
| `1y` | < 365 天 | 以平年为基准 |
| `2y` | < 730 天 | 以两个平年为基准 |
