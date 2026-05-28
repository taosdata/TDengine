# 事件窗口 true_for 开关窗条件 测试报告

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-20 | 2026-05-20 | 0.1 | 彭荣坤 | 新建 true_for start/end 开关窗条件功能测试文档 |

## 2. 测试目标

本测试文档覆盖事件窗口 `true_for(start(...), end(...))` 功能的全面验证，确保 streak（连续段）开关窗门限在流计算与直接查询两条路径下均行为正确。

- 验证 `start(count N)` / `start(Xs)` / `start(Xs and count N)` / `start(Xs or count N)` 四种开窗 streak 模式。
- 验证 `end(count N)` / `end(Xs)` / `end(Xs and count N)` / `end(Xs or count N)` 四种关窗 streak 模式。
- 验证 `true_for(window_limit)` 原有窗口时长/计数过滤的向后兼容性。
- 验证 `start/end/window_limit` 三类参数以任意顺序组合均能正确解析。
- 验证 sub-event 窗口（多条件 `START WITH`）在语法层拒绝 `start(...)/end(...)` 参数。
- 验证直接 SELECT 查询路径与流计算路径的 streak 语义一致。
- 验证流计算重启（stop/start）时，进行中的 streak 通过 WAL 回放正确重建，窗口输出结果与未重启一致。
- 验证 start streak 跨 SSDataBlock 时，streak 首行的聚合数据不丢失（跨块 bug 回归）。
- 验证 end streak 首行在上一个 SSDataBlock 时，关窗 ekey 和窗口行计数正确（跨块 bug 回归）。

## 3. 参考文档

- 设计文档：`../05-设计文档/事件窗口true_for支持开关窗条件 FS.md`
- 测试脚本：`TDinternal/community/test/cases/18-StreamProcessing/99-Others/test_truefor.py`

## 4. 测试结论

true_for 开关窗条件功能相关测试均已执行通过。测试过程如下：

1. 基础 streak 功能验证通过：`start(count N)`、`end(count N)`、`start(Xs)`、`end(Xs)`、AND/OR 复合模式均在预期数据行开/关窗。
2. 向后兼容性验证通过：原有 `true_for(Xs)` 窗口时长过滤行为不受影响。
3. 参数顺序任意性验证通过：8 种 2/3 参数排列组合及带 window_limit 过滤的用例全部通过。
4. sub-event 负例验证通过：解析层正确拒绝在多条件 `START WITH` 下使用 `start(...)/end(...)` 参数。
5. 直接查询路径验证通过：SELECT EVENT_WINDOW 查询与流计算路径语义完全一致，包括跨块 end streak 首行定位回归。
6. 重启恢复验证通过：end streak、start streak、start+end streak 三种场景下，重启后通过 WAL 回放正确重建 streak 状态，窗口输出结果与未重启一致。
7. 跨块 start streak 聚合回归验证通过：start streak 跨 SSDataBlock 时，streak 首行及中间行的聚合数据均被计入窗口，cnt 值正确。

综合结论：true_for 开关窗条件功能达到设计预期，可以进入回归。

## 5. 测试环境

- OS: Linux
- Python: 3.10.12
- Test Framework: pytest 8.3.5
- Target Repo: `TDinternal`
- Test Entry: `TDinternal/community/test/cases/18-StreamProcessing/99-Others/test_truefor.py`
- 验证命令：`cd TDinternal/community/test && /usr/bin/python3 -m pytest cases/18-StreamProcessing/99-Others/test_truefor.py --skip_stop`
- 验证结果：全部通过。

## 6. 功能测试

### 6.1 基础 streak 功能验证（流计算）

#### 6.1.1 测试要点

- 验证 `start(count N)`：仅在开启条件连续满足 N 次后才真正开窗；单次满足或被中断的连续段不触发开窗。
- 验证 `end(count N)`：仅在关闭条件连续满足 N 次后才真正关窗；被中断的连续段不触发关窗。
- 验证 `start(count N) + end(count N)` 组合：两套 streak 独立运行。
- 验证 `start(Xs)`：开启条件需连续持续 ≥ X 秒；skey 为 streak 首行时间戳。
- 验证 `end(Xs)`：关闭条件需连续持续 ≥ X 秒；ekey 为 streak 首行时间戳。
- 验证 `true_for(Xs)`（向后兼容）：窗口时长 ≥ X 秒才输出，短窗口被丢弃。
- 验证 `start(Xs or count N)` OR 模式：时长或计数任一先满足即触发开窗。
- 验证 `start(Xs and count N)` AND 模式：时长与计数需同时满足才触发开窗。
- 验证 sub-event 多条件 `START WITH` 下 `start(...)` 参数在解析层被拒绝（负例）。

#### 6.1.2 用例列表

| # | 测试用例（stream） | `true_for` 参数 | 测试描述 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | `s1 StartCount2` | `start(count 2)` | 开启条件需连续 2 次才开窗；中断后重置 streak | 通过 |
| 2 | `s2 EndCount2` | `end(count 2)` | 关闭条件需连续 2 次才关窗；中断后重置 streak | 通过 |
| 3 | `s3 StartCount2EndCount2` | `start(count 2), end(count 2)` | 两套 streak 独立，skey/ekey 均取各自首行 | 通过 |
| 4 | `s4 StartDur2s` | `start(2s)` | 开启条件持续 ≥ 2s 才开窗；skey = streak 首行 | 通过 |
| 5 | `s5 EndDur2s` | `end(2s)` | 关闭条件持续 ≥ 2s 才关窗；ekey = streak 首行 | 通过 |
| 6 | `s6 WindowDur3s` | `true_for(3s)` | 原有行为向后兼容：窗口 < 3s 被丢弃 | 通过 |
| 7 | `s7 StartOrDurCount` | `start(2s or count 2)` | OR 模式：count=2 先满足即触发，无需等 2s | 通过 |
| 8 | `s8 StartAndDurCount` | `start(2s and count 3)` | AND 模式：时长与计数需在同一行同时满足 | 通过 |
| 9 | `s9 SubEventStartCountIgnored` | `start(count 2)`（sub-event） | sub-event 下 `start(...)` 参数解析报错（负例） | 通过 |
| 10 | `s10 SubEventTrueForRejected` | `start(count 2)` / `end(count 2)`（sub-event） | sub-event 下 `start/end` 参数均被解析层拒绝（负例） | 通过 |

#### 6.1.3 streak 行为说明

| streak 模式 | skey / ekey 取值 | 中断行为 |
| --- | --- | --- |
| `start(count N)` | streak **首行**时间戳 | 出现一行不满足开启条件，count 重置为 0 |
| `end(count N)` | streak **首行**时间戳 | 出现一行不满足关闭条件，count 重置为 0 |
| `start(Xs)` | streak **首行**时间戳 | 出现一行不满足开启条件，firstTs 重置 |
| `end(Xs)` | streak **首行**时间戳 | 出现一行不满足关闭条件，firstTs 重置 |
| `start(Xs and count N)` | streak **首行**时间戳 | 任一条件中断即全部重置 |
| `start(Xs or count N)` | streak **首行**时间戳 | 任一条件先满足即触发，另一中断不影响已触发的 |

### 6.2 参数顺序任意性验证

#### 6.2.1 测试要点

- `true_for()` 中的 `window_limit`、`start_limit`、`end_limit` 三类参数可任意排列，共测试 8 种 2/3 参数顺序组合。
- 验证所有排列均能正确解析并产生与规范顺序相同的窗口输出（skey、ekey、cnt 一致）。
- 额外验证 `window_limit` 在非规范顺序下仍能正确过滤行数不足的窗口（负过滤用例）。

#### 6.2.2 共享数据时序说明

参数顺序测试使用统一的 9 行数据集（采样间隔 2s），设计为使 `count 2` 和 `2s` 条件在同一数据行同时满足：

```
t=02 v=221  start streak=1  (firstTs=t02)
t=04 v=100  NOT start → streak RESET
t=06 v=222  start streak=1  (firstTs=t06)
t=08 v=223  start streak=2, dur=2s → 开窗 (skey=t06)
t=10 v=224  窗口内
t=12 v=100  end streak=1 (firstTs=t12)
t=14 v=221  NOT end → end streak RESET
t=16 v=100  end streak=1 (firstTs=t16)
t=18 v=099  end streak=2, dur=2s → 关窗 (ekey=t16)

预期窗口：skey=t06, ekey=t16, cnt=6
```

#### 6.2.3 用例列表

| # | 测试用例 | `true_for` 参数（顺序） | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | `o1 TrueForOrderES` | `end(2s), start(count 2)` | skey=t06, ekey=t16, cnt=6 | 通过 |
| 2 | `o2 TrueForOrderEW` | `end(count 2), count 3` | skey=t02, ekey=t16, cnt=8（无 start_limit，立即开窗） | 通过 |
| 3 | `o3 TrueForOrderSW` | `start(2s), count 3` | skey=t06, ekey=t12, cnt=4（无 end_limit，立即关窗） | 通过 |
| 4 | `o4 TrueForOrderWES` | `count 3, end(2s), start(count 2)` | skey=t06, ekey=t16, cnt=6 | 通过 |
| 5 | `o5 TrueForOrderSWE` | `start(2s AND count 2), count 3, end(count 2)` | skey=t06, ekey=t16, cnt=6 | 通过 |
| 6 | `o6 TrueForOrderESW` | `end(2s OR count 2), start(count 2), count 3` | skey=t06, ekey=t16, cnt=6 | 通过 |
| 7 | `o7 TrueForOrderEWS` | `end(count 2), count 3, start(2s AND count 2)` | skey=t06, ekey=t16, cnt=6 | 通过 |
| 8 | `o8 TrueForOrderSEW` | `start(2s OR count 2), end(count 2), count 3` | skey=t06, ekey=t16, cnt=6 | 通过 |
| 9 | `o9 TrueForOrderWindowFilter` | `end(2s OR count 2), count 10, start(2s AND count 2)` | 0 行（6 行 < window_limit=10，被过滤） | 通过 |

### 6.3 直接查询路径验证（SELECT EVENT_WINDOW）

#### 6.3.1 测试要点

- 验证 `true_for(start(...), end(...))` 在直接 SELECT 查询（非流计算）中与流计算路径语义完全一致。
- 验证 `start(count N)`、`end(count N)`、`start(Xs) + end(Xs)`、`count N`（window_limit）四类参数。
- 验证 end streak 首行位于上一个 SSDataBlock 时，ekey 定位和窗口行计数正确（跨块回归用例）。

#### 6.3.2 用例列表

| # | 测试用例（query） | `true_for` 参数 | 测试描述 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | `q1 start(count 2)` | `start(count 2)` | 查询侧开窗 streak；2 个窗口，skey 均为 streak 首行 | 通过 |
| 2 | `q2 end(count 2)` | `end(count 2)` | 查询侧关窗 streak；ekey = streak 首行 | 通过 |
| 3 | `q3 start+end count 2` | `start(count 2), end(count 2)` | 两套 streak 组合；skey/ekey 均取各自首行 | 通过 |
| 4 | `q4 start(2s)+end(2s)` | `start(2s), end(2s)` | 时长 streak 查询侧验证；timestamps 间隔 2s | 通过 |
| 5 | `q5 count 3 (window_limit)` | `count 3` | 查询侧窗口行计数过滤；不足 3 行的窗口被丢弃 | 通过 |
| 6 | `q6 end(count 2) 跨块回归` | `end(count 2)` | end streak 首行在上一 SSDataBlock（flush 强制分块）；ekey 正确定位，cnt 不多计 | 通过 |

#### 6.3.3 跨块 ekey 定位问题说明

测试用例 q6 是针对以下问题的回归验证：

- **现象**：end streak 的 firstTs（streak 首行）在上一个 SSDataBlock，streak 在新块的第一行达成门限。修复前，新块的 satisfy 处理逻辑错误地将当前块第 0 行纳入聚合（`endRowIndex = clamp(0-1) = 0`），导致窗口多计一行（cnt = expected + 1）。
- **修复**：当 `endRowIndex < startIndex` 时跳过当前块的聚合调用。
- **验证方法**：在第 3 行（end streak 首行）和第 4 行（streak 满足行）之间执行 `FLUSH DATABASE`，强制产生 SSDataBlock 边界。

### 6.4 重启恢复验证（stop/start）

#### 6.4.1 测试要点

- 验证流计算在 streak 进行中执行 stop/start 后，通过 WAL 回放正确重建 streak 状态。
- 验证 `doneVer` 冻结修复：进行中的 streak 在 checkpoint 时 `doneVer` 被冻结到 streak 首行之前，重启后从该位置重放 WAL，自然重建 streak 计数。
- 覆盖三种重启场景：end streak 进行中、start streak 进行中（窗口尚未开启）、start+end streak 各自进行中。
- 验证重启前已完成的窗口不被重复输出，重启后新窗口在正确的数据行关闭。

#### 6.4.2 用例列表

| # | 测试用例 | streak 场景 | 重启时机 | 测试描述 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| 1 | `r1 EndStreakRestartBeforeClose` | `end(count 2)` | end streak count=1，窗口尚未关闭 | 重启后 end streak 重建，第 2 行到来时正确关窗 | 通过 |
| 2 | `r2 StartStreakRestartBeforeOpen` | `start(count 3)` | start streak count=2，窗口尚未开启 | 重启后 start streak 重建，第 3 行到来时正确开窗 | 通过 |
| 3 | `r3 BothStreakRestart` | `start(count 2), end(count 2)` | 窗口已开，end streak count=1 | 重启后 end streak 重建，第 2 行到来时正确关窗 | 通过 |

#### 6.4.3 重启恢复行为矩阵

| 重启时机 | doneVer 行为 | 重启后 streak 状态 | 窗口输出 |
| --- | --- | --- | --- |
| end streak count=1（未满足） | 冻结到 end streak 首行之前 | WAL 重放后 count 自然重建为 1 | 原有窗口不变；新窗口在正确行关闭 |
| start streak count=2（未满足，窗口未开） | 冻结到 start streak 首行之前 | WAL 重放后 count 自然重建为 2 | 原有窗口不变；新窗口在第 3 行开启 |
| start+end 组合，end streak count=1 | 冻结到 end streak 首行之前 | WAL 重放后 end count 重建为 1 | 原有窗口不变；新窗口在正确行关闭 |

### 6.5 跨块 start streak 聚合回归验证

#### 6.5.1 测试要点

- 验证 start streak 满足门限的行跨越多个 SSDataBlock 时，streak 首行及中间行的聚合数据均被计入窗口（不因 block 边界被丢弃）。
- 覆盖三种跨块场景：count 门限 2+1 分批、streak 中途中断后重新跨块、时长（duration）门限跨块。
- 验证 streak 中断后聚合状态（tentative agg）被正确丢弃，新 streak 从干净状态重新累积。

#### 6.5.2 用例列表

| # | 测试用例 | `true_for` 参数 | 跨块场景 | 预期 cnt | 修复前 cnt | 测试结果 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `cb1 StartStreakSplit2Plus1` | `start(count 3)` | 批次 1：count=2；批次 2：count=3 满足 + 关窗 | 4（T1+T2+T3+T4）| 2（T3+T4，前两行丢失） | 通过 |
| 2 | `cb2 StartStreakBreakThenCrossBlock` | `start(count 3)` | 批次 1：count=2 后中断；批次 2：count=2；批次 3：count=3 满足 + 关窗 | 4（T4+T5+T6+T7）| skey 错误（可能从 T1 开始）| 通过 |
| 3 | `cb3 StartStreakDurationCrossBlock` | `start(2s)` | 批次 1：T1（dur=0s < 2s）；批次 2：T2（dur=3s ≥ 2s，满足）+ 关窗 | 3（T1+T2+T3）| 2（T1 丢失）| 通过 |

## 7. 易用性测试

不涉及。

## 8. 长期稳定性测试

无。

## 9. 性能测试

根据设计文档，true_for start/end 功能的性能开销极低：

- **内存开销**：每个活跃 group 新增约 24 字节 streak 状态（count + firstTs）。10 万活跃 group 约 2.4 MB，100 万活跃 group 约 24 MB。
- **CPU 开销（每行）**：未启用时仅 1 次短路判断（接近零开销）；启用时每行增加 O(1) 个整数操作，无循环、无哈希查找、无动态内存分配。
- **Checkpoint 开销**：streak 状态不写入 checkpoint，checkpoint 数据量无额外增加；有进行中 streak 的分组需额外重放少量 WAL 行，通常极少。
- **消息序列化**：流任务部署消息新增 6 个字段共 40 字节，仅在流创建时发送一次。

以上开销数据均来自设计文档的理论分析，本次测试聚焦功能正确性验证，未进行独立的性能基准测试。

## 10. 安全性测试

无。

## 11. 兼容性测试

- 未设置 `start/end` 的已有流（旧流）：`duration=0, count=0`，streak 判断恒通过，行为与原有逻辑完全相同。
- 原有 `true_for(Xs)` 窗口时长过滤不受影响，独立于 streak 逻辑运行（s6 用例验证通过）。
- 参数顺序任意，新旧节点混合部署期间缺失字段默认为 0，不报错（详见设计文档 §7.2）。
- 流升级后读取旧元数据：缺失的新字段默认为 0，安全兼容。

## 12. 已知问题和限制

- `start(...)` / `end(...)` 不支持 sub-event 窗口（`START WITH (cond1, cond2, ...)`），解析期报错（`TSDB_CODE_STREAM_INVALID_TRIGGER`）。
- 进行中的 streak 不写入 checkpoint，重启后需回放 streak 首行以来的 WAL；回放量取决于 streak 积累的行数，通常极少。
- 当前不支持通过 `ALTER STREAM` 修改 `true_for` 参数，需删除并重建流。
- 跨块 end streak 首行定位问题已修复（见 6.3.3 q6 回归）。
- 跨块 start streak 聚合丢失问题已修复（见 6.5 cb1/cb2/cb3 回归）。

## 13. 测试用例总览

| # | test method | 覆盖维度 | 用例数 | 状态 |
| --- | --- | --- | --- | --- |
| 1 | `test_truefor_event_window` | start/end streak 基础功能、向后兼容、AND/OR 模式、sub-event 负例 | 10 | 通过 |
| 2 | `test_truefor_arg_order` | true_for 三类参数任意顺序排列（8 种组合 + window_limit 过滤） | 9 | 通过 |
| 3 | `test_truefor_query_event_window` | SELECT 查询路径 streak 语义、跨块 ekey 回归 | 6 | 通过 |
| 4 | `test_truefor_restart_before_close` | 流重启 doneVer 冻结修复（end/start/both streak 场景） | 3 | 通过 |
| 5 | `test_truefor_start_streak_cross_block` | start streak 跨 SSDataBlock 聚合回归（count 门限、中断后重试、duration 门限） | 3 | 通过 |
