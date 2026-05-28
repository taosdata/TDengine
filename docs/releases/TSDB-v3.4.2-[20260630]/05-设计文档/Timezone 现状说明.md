# TDengine Timezone 实现现状与设计分析（Functional Spec）

> **文档基础信息**：本文档基于 TDengine 主仓库 main 分支，commit ID: `7dd65be901`，分析时间：2026-05-09

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-09 | 2026-05-09 | 1.0 | Tony Zhang | 初始版本：Timezone 使用现状、设计缺陷 |

---

## 2. 背景

TDengine 作为工业级时序数据库，跨地域、多时区的部署场景普遍存在。
Timezone（时区）的正确处理对用户数据的正确分组和查询至关重要。

当前 TDengine 的 Timezone 使用和配置存在两个层面：
- **客户端时区**：用于时间函数计算和结果显示
- **服务端时区**：用于窗口计算的边界判定

本文档系统地梳理 TDengine 中 Timezone 的使用现状、实现缺陷，为后续的架构改进奠定基础。

---

## 3. 定义

| 术语 | 定义 |
|-----|------|
| **Unix Timestamp** | 从 1970-01-01 00:00:00 UTC 到当前时刻的秒数（或微秒数），是绝对时间的标准表示，与时区无关 |
| **客户端 Timezone** | 客户端应用所在的地理位置时区，用于解析用户输入的时间字符串、计算时间函数、以及格式化显示结果 |
| **服务端 Timezone** | taosd 配置文件中指定的全局默认时区，在当前实现中主要用于窗口函数计算 |
| **INTERVAL 窗口** | 基于固定时间间隔（如 1d、1h）的分组窗口，常用于时序数据的聚合分析 |
| **Logical Plan** | 查询规划的逻辑阶段，表示查询的高级操作序列，独立于具体的数据存储和执行引擎 |
| **Physical Plan** | 查询规划的物理阶段，表示具体的执行计划，包含所有执行参数和优化信息 |
| **DST (Daylight Saving Time)** | 夏令时，某些地区为了充分利用阳光而人为调整的时间制度，通常在春秋进行±1 小时的调整 |

---

## 4. 关键事实

本节总结 TDengine 中 Timezone 使用的三个核心事实：

### 关键事实 1：服务端 Timezone 只用于窗口计算

绝大多数时间函数（NOW、TODAY、DATE、WEEK 等）都使用**客户端** Timezone，而非服务端 Timezone。仅在 INTERVAL 窗口计算、流计算触发等涉及时间边界判定的场景中，才会使用服务端 Timezone。

### 关键事实 2：窗口计算存在实现缺陷

虽然架构设计上应在解析阶段携带客户端 Timezone 信息到执行阶段，但在逻辑规划（Logical Plan）和物理规划（Physical Plan）阶段，Timezone 字段未被正确传递，导致执行阶段无法获取客户端 Timezone 信息，最终降级使用全局默认 Timezone。

### 关键事实 3：集群节点 Timezone 必须相同

正因为窗口计算依赖服务端全局默认 Timezone，集群中所有节点必须配置相同的 Timezone，否则会导致分布式查询中同一时间戳在不同节点被分配到不同的窗口，造成数据分组不一致。

---

## 5. 行为说明

### 完整的 Timezone 使用参考表

####  **来源 1：客户端 Timezone** （从 parseContext 的 `pInput->tz` 获取）

| 操作 / 函数 | 说明 | 代码位置 |
|-----------|------|--------|
| `NOW()` | 返回当前 Unix Timestamp，但如果出现在 WHERE 子句需要根据客户端时区判断日期范围 | `sclfunc.c:nowFunction()` |
| `TODAY()` | 计算"今天"的开始时间戳，需要客户端时区确定日期边界 | `sclfunc.c:todayFunction()` → `taosGetTimestampToday(timePrec, pInput->tz)` |
| `TO_TIMESTAMP(ts_str, format_str)` | 解析时间字符串时，需要理解其在哪个时区 | `sclfunc.c:toTimestampFunction()` |
| `TO_CHAR(ts, fmt)` / `TO_ISO8601(ts, [tz])` | 时间戳格式化为字符串，显示必须转换为客户端本地时区 | `sclfunc.c` 中的格式化函数 |
| `DATE(ts)` | 提取时间戳所在的日期，需要时区判断 | `sclfunc.c:dateFunction()` |
| `WEEK(ts)` / `WEEKOFYEAR(ts)` / `WEEKDAY(ts)` | 提取周数或周几，需要时区确定周的边界 | `sclfunc.c` 中的周期函数 |
| `TO_UNIXTIMESTAMP(str)` | 根据客户端时区理解输入字符串 | `sclfunc.c:toUnixtimestampFunction()` |

**特点**：这些函数都在 **标量计算阶段** 执行，从 SScalarParam.tz 字段获取客户端时区

---

####  **来源 2：服务端 Timezone（全局默认，taosd.cfg）** — **实现缺陷**

| 操作 | 说明 | 代码位置 | 缺陷说明 |
|-----|------|--------|--------|
| `INTERVAL(1d)` 窗口计算 | 按日期/周/月分组，需要确定日期边界 | `executil.c:doCalculateTimeWindow()` → `taosTimeTruncate(ts, pInterval)` | `interval.timezone=NULL` → 使用全局默认时区 |
| **流计算触发（基于 INTERVAL）** | 流计算触发窗口为 INTERVAL/日历窗口时，会走窗口边界计算 | `source/stream/` + `executil.c` | 继承 INTERVAL 缺陷，使用全局默认时区 |

**架构缺陷详解**：
```
【解析阶段】✓ SIntervalWindowNode.timezone = 客户端 timezone（存在）
     ↓
【逻辑计划】✗ SWindowLogicNode 中没有 timezone 字段（丢失）
     ↓
【物理计划】✗ SIntervalPhysiNode 中没有 timezone 字段（丢失）
     ↓
【执行阶段】创建 SInterval 时缺少 .timezone 初始化
SInterval interval = {.interval = pPhyNode->interval, ...};
              // ❌ 缺少 .timezone 字段
              // 导致 interval.timezone = NULL
              // 降级使用全局默认时区（taosd.cfg）
```

**具体例子**：服务端 UTC，客户端 Asia/Shanghai (UTC+8)

假设用户在2024-01-14 23:30:00 UTC 时刻写入一条数据（对应时间戳 1705276200）

| 对比项 | 预期行为 | 实际行为 |
|-------|--------|--------|
| **数据写入** | UTC时间戳 1705276200 | UTC时间戳 1705276200 |
| **查询INTERVAL(1d)** | 使用上海时区划分日期 | 使用UTC划分日期 |
| **时间戳所属日期** | 2024-01-15 07:30:00(上海) → **属于2024-01-15** | 2024-01-14 23:30:00(UTC) → **属于2024-01-14** |
| **窗口范围** | 2024-01-15 00:00~23:59 (上海) | 2024-01-14 00:00~23:59 (UTC) |
| **_wstart时间戳** | 1705262400 | 1705190400 |
| **_wstart显示值** | 2024-01-15 00:00:00 | 2024-01-14 08:00:00 |
| **数据分组** | 被分到"2024-01-15"组 | 被分到"2024-01-14"组 |

**后果**：
- 用户期望按"上海日期"分组，但实际按"UTC日期"分组
- 同一个物理日期的数据可能被分到两个不同的窗口（跨越日期边界时）
- 查询结果与预期完全不符

---

####  **来源 3：可配置（参数指定）**

| 函数 | 参数 | 说明 | 代码位置 |
|-----|------|------|--------|
| `TIME_TRUNCATE(ts, unit, use_current_tz)` | 第 3 参数 | `use_current_tz=0` 使用 UTC；`use_current_tz=1` 使用客户端 timezone（默认） | `sclfunc.c:timetruncateFunction()` |
| `TO_ISO8601(ts, [tz])` | 第 2 参数（可选） | 可显式指定输出时区，默认为客户端系统时区 | ISO 8601 格式化函数 |

---

####  **来源 4：无关（与 timezone 无关）**

| 操作 | 说明 |
|-----|------|
| 时间戳存储 | 存储的是 Unix Timestamp（UTC），与 timezone 无关 |
| 查询结果返回 | 服务端返回 Unix Timestamp，客户端负责转换显示 |
| `SESSION()` 会话窗口 | 按时间戳差值与 gap 切窗，使用的是绝对时间差，不依赖 timezone |
| `TIMEDIFF(ts1, ts2)` | 计算两个时间戳的差值（秒），与 timezone 无关 |
| 数据库内部计算 | 所有内部时间戳比较、排序等都基于 Unix Timestamp，与 timezone 无关 |

---

### 核心规律总结

| 规律 | 说明 | 影响 |
|-----|------|------|
| **1. 绝大多数标量时间函数使用客户端 timezone** | NOW()、TODAY()、TO_TIMESTAMP()、DATE()、WEEK() 等都从 parseContext 获取 | 不同 timezone 的客户端查询同一数据会看到不同的时间表示（正常行为）|
| **2. 窗口计算有实现缺陷** | 本应使用客户端 timezone，但架构缺陷导致丢失，降级为服务端全局 timezone | **集群所有节点 timezone 必须一致** |
| **3. 存储和比较都基于 UTC** | 无论客户端/服务端 timezone 如何，数据库内部都使用 Unix Timestamp | 数据一致性有保障 |
| **4. 显示转换在客户端** | 服务端返回 UTC 时间戳，客户端根据自己的 timezone 显示 | 跨时区协作的标准做法 |

---

## 6. 兼容性

### 向后兼容性

当前实现中：
- **有兼容性问题**：现有依赖服务端全局 Timezone 进行窗口计算的业务，如果升级到修复后的版本，可能看到不同的窗口分组结果

### 改进计划中的兼容性

当后续任务完成、Timezone 架构得到修复后：
- **批查询窗口计算**将使用客户端/连接级 Timezone，而不再是全局服务端 Timezone
- **流计算触发**也将同步改进
- 现有业务可能需要调整配置或重新验证数据分组结果

### Windows 平台限制

- **Linux/macOS**：支持 `UTC-8`、`UTC+8`、`GMT-8`、`GMT+8`、`Asia/Shanghai` 等所有格式
- **Windows**：不支持 `UTC±X` 格式，只支持 `Asia/Shanghai` 这样的 IANA 时区名

---

## 7. 结论

- TDengine 的 timezone 使用设计存在明显的实现缺陷，导致窗口计算依赖服务端全局默认 timezone，这就是为什么集群节点必须使用相同的 timezone，否则会导致窗口计算结果不一致
- 流计算触发场景还依赖服务端 timezone 计算窗口边界
- 绝大多数时间函数（NOW、TODAY、DATE、WEEK等）都正确使用客户端 timezone，用户查询时会看到符合预期的时间表示
- [任务 6661700117](https://project.feishu.cn/taosdata_td/feature/detail/6661700117)计划完全修复批查询窗口函数的 timezone 实现缺陷，自此后批查询窗口计算将正确使用客户端/连接级 timezone，解决当前的核心问题
- 在流计算触发场景也不依赖服务端 timezone 后（预期2026q3），服务端 timezone 的必要性将大幅降低
- 未来可以考虑将服务端 timezone 作为一个可选配置项，默认使用 UTC，允许用户根据需要启用或禁用，以简化部署和减少潜在的配置错误风险，同时为查询时区失效情况（可认为是bug）提供保底方案，确保系统在任何情况下都能稳定运行。
