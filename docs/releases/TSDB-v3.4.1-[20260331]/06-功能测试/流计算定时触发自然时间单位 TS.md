# 流计算定时触发自然时间单位 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-10 | 2026-03-10 | 0.1 | 邝金清 | 初稿 |

## 2. 测试目标

本测试用于验证 TDengine 流计算 `PERIOD` 触发器新增自然时间单位周(`w`)/月(`n`)/年(`y`)以及扩展 offset(`h`/`d`) 的功能正确性、边界行为、兼容性与性能指标。
- 语法与校验：`PERIOD(interval[, offset])` 支持 `w/n/y`，offset 支持 `a/s/m/h/d`，且错误码与错误信息符合契约
- 行为正确：触发时刻对齐自然边界（周一/月初/年初 00:00:00，服务端时区），offset 在边界基础上正向偏移
- 窗口正确：首次窗口从上一个自然边界(含 offset)回溯开始，窗口推进连续（闭区间窗口：下一窗口 `skey = 上一窗口 ekey + 1`）
- 边界覆盖：多倍数周期 epoch 对齐、大小月/闰年、精度(milli/micro/nano)
- 回归与兼容：不破坏既有 `a/s/m/h/d` 单位流任务；升级/降级元数据兼容行为明确
- 性能：`alignToNaturalBoundary()` / `getDuration()` 平均耗时 < 1ms（10000 次均值）

## 3. 参考文档

- 对应需求跟踪：https://project.feishu.cn/taosdata_td/feature/detail/6490755304
- 功能规格文档：[流计算定时触发自然时间单位 FS](https://taosdata.feishu.cn/wiki/LKGfwSd0qiTEjckvXvMcgCc5nue)

## 4. 测试结论

结论：测试通过。
覆盖范围（核心项）：
- 单元测试（ctest/GoogleTest）：`community/source/common/test/ttimeNaturalUnitsTest.cpp`（自然边界对齐、精度与性能）、`community/source/libs/new-stream/test/streamTriggerTaskTest.cpp`（窗口计算/推进、闰年/大小月、epoch 对齐）、`community/source/libs/parser/test/parStreamTest.cpp`（PERIOD w/n/y 与 offset(h/d) 语法与校验）
- 系统测试（pytest/new_test_framework）：`community/test/cases/18-StreamProcessing/03-TriggerMode/test_period_natural_units.py`（w/n/y 与 offset 组合、范围/错误场景、元数据查询）

## 5. 测试环境

- OS: Linux x86_64（CI/主要验证环境），macOS/Windows（开发与冒烟验证）
- TDengine: `3.0` 分支衍生特性分支 `001-stream-natural-time-units`（以实际构建产物版本号为准）
- Tools: `taosd`/`taos`，Python 3.x + pytest，新测试框架（`new_test_framework`），ctest/GoogleTest

## 6. 功能测试

### 6.1 SQL 语法与参数校验（PERIOD interval/offset）

#### 6.1.1 测试要点

- interval 单位：新增 `w/n/y`，并保持既有 `a/s/m/h/d` 行为不变
- interval 范围：`w:[1,520]`，`n:[1,120]`，`y:[1,10]`，越界时返回明确错误
- offset 单位：支持 `a/s/m/h/d`，不允许 `w/n/y`
- offset 约束：必须为单一数值 + 单一单位；必须满足 `offset < interval`（严格小于）
- 月单位 offset 静态溢出校验：以 28 天/月为最短月份组合基准，创建时拒绝可能在 2 月溢出的配置
- 错误码与错误信息：符合 `contracts/error-codes.md` 的格式与关键字（至少包含支持的单位列表/合法范围/示例）

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 01 | PERIOD 周单位创建 | 执行 `CREATE STREAM ... PERIOD(1w)`，预期创建成功；`information_schema.ins_streams` 中能查询到该 stream | 通过 |
| 02 | PERIOD 多倍数周 | 执行 `PERIOD(2w)`/`PERIOD(4w)`；预期创建成功，元数据能持久化（重连后仍可查询） | 通过 |
| 03 | PERIOD 周单位越界 | `PERIOD(0w)`、`PERIOD(522w)`；预期创建失败，报错包含合法范围与示例 | 通过 |
| 04 | PERIOD 无效 interval 单位 | `PERIOD(1x)`；预期报错包含支持的 interval 单位列表（a/s/m/h/d/w/n/y） | 通过 |
| 05 | PERIOD 月单位创建 | `PERIOD(1n)`/`PERIOD(3n)`；预期创建成功；元数据可查询 | 通过 |
| 06 | PERIOD 月单位越界 | `PERIOD(0n)`、`PERIOD(121n)`；预期创建失败，错误信息明确 | 通过 |
| 07 | PERIOD 年单位创建 | `PERIOD(1y)`/`PERIOD(2y)`；预期创建成功；元数据可查询 | 通过 |
| 08 | PERIOD 年单位越界 | `PERIOD(0y)`、`PERIOD(11y)`；预期创建失败，错误信息明确 | 通过 |
| 09 | offset 基础（h/d） | `PERIOD(1w, 1d)`/`PERIOD(1w, 12h)`；预期创建成功；元数据可查询 | 通过 |
| 10 | offset 跨单位组合 | `PERIOD(2w, 3d)`、`PERIOD(1n, 14d)`、`PERIOD(1y, 31d)`；预期创建成功 | 通过 |
| 11 | offset 单位非法 | `PERIOD(1n, 1w)`、`PERIOD(1y, 1n)`、`PERIOD(1y, 1y)`；预期报错并提示 offset 支持单位（a/s/m/h/d） | 通过 |
| 12 | offset 与周期大小关系 | `PERIOD(1w, 7d)`、`PERIOD(1w, 8d)`；预期报错并提示 `offset < interval`（严格小于）与合法范围 | 通过 |
| 13 | 月单位 offset 溢出校验 | `PERIOD(1n, 28d)`、`PERIOD(2n, 56d)` 预期失败；`PERIOD(1n, 27d)` 预期成功 | 通过 |
| 14 | offset 多单位组合 | `PERIOD(1w, 2d12h)`；预期失败并提示仅支持单一单位（如需要，提示转换为单一单位的等价表达） | 通过 |
| 15 | offset 负值 | `PERIOD(1w, -1d)`；预期创建失败（可接受语法错误或明确的 offset 非法错误，但必须可定位原因） | 通过 |
| 16 | 旧单位回归 | `PERIOD(10s)`、`PERIOD(1d, 12h)` 等既有语法在新版本下创建/执行行为不变 | 通过 |

### 6.2 自然边界对齐与时间窗口计算（alignToNaturalBoundary/TriggerTask）

#### 6.2.1 测试要点

- `alignToNaturalBoundary()`：对齐规则正确（周一/月初/年初），offset 正确叠加
- 多倍数周期：基于 epoch（1970-01-01 00:00:00 服务端时区）整除对齐，全局一致
- `stTriggerTaskGetTimeWindow()`：窗口起止正确；创建时间在周期中间时窗口回溯到上一个自然边界（满足 FR-015）
- `stTriggerTaskNextTimeWindow()`：窗口推进连续（闭区间窗口：下一窗口 skey = 上一窗口 ekey + 1）
- 日历边界：大小月（28/29/30/31）、闰年（2/29）、年窗口长度（365/366）
- 精度：毫秒/微秒/纳秒精度下结果正确

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 01 | 周边界对齐 | 输入周二时间戳，`alignToNaturalBoundary(...,'w')` 应对齐到周一 00:00:00 | 通过 |
| 02 | 月边界对齐 | 输入 2026-03-15，`alignToNaturalBoundary(...,'n')` 对齐到 2026-03-01 00:00:00 | 通过 |
| 03 | 年边界对齐 | 输入 2026-06-15，`alignToNaturalBoundary(...,'y')` 对齐到 2026-01-01 00:00:00 | 通过 |
| 04 | 周 + offset | `alignToNaturalBoundary(...,'w',offset=1d)` 应对齐到周二 00:00:00 | 通过 |
| 05 | 多倍数周对齐（2w） | 同一 2 周周期内不同时间戳对齐结果一致；相邻周期边界差为 14 天 | 通过 |
| 06 | 多倍数月对齐（3n） | 季度对齐：Q1/Q2/Q3/Q4 起点正确（基于 epoch 对齐） | 通过 |
| 07 | 多倍数年对齐（2y） | 两年周期边界对齐正确，窗口连续 | 通过 |
| 08 | 周窗口计算 | `stTriggerTaskGetTimeWindow(unit=w)` 返回窗口 [周一 00:00:00, 下周一 00:00:00] | 通过 |
| 09 | 周窗口推进 | `stTriggerTaskNextTimeWindow()` 后窗口连续，且 `skey = 旧 ekey + 1` | 通过 |
| 10 | 月窗口推进（大小月） | 1 月(31 天) -> 2 月(28/29 天) -> 3 月(31 天) 的窗口推进正确 | 通过 |
| 11 | 年窗口推进（闰年） | 2024 年窗口长度为 366 天，推进到 2025 年窗口长度为 365 天 | 通过 |
| 12 | 2/29 边界 | 2024-02-29 输入时，月窗口应为 [2024-02-01, 2024-03-01] | 通过 |
| 13 | 精度：微秒 | 微秒精度下对齐/窗口计算正确，结果量纲正确 | 通过 |
| 14 | 精度：纳秒 | 纳秒精度下对齐/窗口计算正确，结果量纲正确 | 通过 |

### 6.3 系统集成：流任务创建、元数据与重启恢复

#### 6.3.1 测试要点

- 系统测试覆盖：用 pytest 创建 `PERIOD(w/n/y)` 与 offset 组合，验证元数据可查询
- 元数据持久化：重连或重启后 `information_schema.ins_streams` 可查询到配置且单位字符未丢失
- 服务重启恢复（FR-014）：重启后正确恢复下一次触发时间计算；若停机期间跨过触发点，应跳到下一个未来触发点

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 01 | 系统测：自然单位综合 | 运行 `community/test/cases/18-StreamProcessing/03-TriggerMode/test_period_natural_units.py`，覆盖 w/n/y 与 offset 校验 | 通过 |
| 02 | 元数据查询一致性 | 创建 `PERIOD(1w,1d)`/`PERIOD(1n,14d)`/`PERIOD(1y,31d)` 后查询 `information_schema.ins_streams`，确认 PERIOD 字符串/字段一致 | 通过 |
| 03 | 重启恢复（冒烟） | 创建包含新单位的 stream -> 停止 taosd -> 启动 taosd -> 确认 stream 元数据加载成功且无异常日志/崩溃 | 通过 |
| 04 | 重启恢复（跨触发点） | 停机期间模拟跨过一次触发点（需要可控时间或等待），重启后应跳到下一个未来触发时刻 | 通过 |

## 7. 性能测试

目标：验证 `alignToNaturalBoundary()` 与 `getDuration()` 的平均耗时满足 < 1ms。
- 单元测试基准：`community/source/common/test/ttimeNaturalUnitsTest.cpp`
- 运行方式：构建完成后执行 `build/bin/ttimeNaturalUnitsTest`
测试结果：alignToNaturalBoundary() 平均耗时 10us，getDuration() 平均耗时 0.02us，满足预期

## 8. 已知问题和限制

- offset 不支持负值；`PERIOD(1w, -1d)` 预期失败
- offset 仅支持 `a/s/m/h/d`，不支持 `w/n/y`
- offset 必须严格小于周期；等于/大于均非法
- 月单位 offset 静态校验以 28 天/月为基准，可能拒绝某些在大月可运行但在 2 月可能溢出的配置（这是设计选择）
- 多倍数周期对齐基于 epoch 整除对齐，与任务创建时刻无关（用户不可自定义对齐基准）
