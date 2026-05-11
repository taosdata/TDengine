# P4 实现审查报告

## Review summary

- **Status**: completed
- **Target**: working_tree [unstaged+staged both]，HEAD 相对基线
- **Coverage**: Change Summarizer ✓ · Broad Scanner ✓ · Security Reviewer ✓（无注入/认证问题）· Performance Reviewer ✓（无热点回退）· Maintainability Reviewer ✓ · Rule Reviewer ✓

---

## Change Summarizer（全局预步骤）

本次 diff（22 文件，+307/-180）实现了 P4 阶段的部分交付：

1. **TIMETRUNCATE 自然单位（Task 4.1）**：新增 `validateTimeUnitParamEx` / `FUNC_PARAM_MUST_BE_TIME_UNIT_OR_CALENDAR`，scalar 层新增 n/q/y 月历截断分支。
2. **TIMETRUNCATE 1w fdow 对齐（Task 4.2）**：scalar 层新增 `hasStringTz+1w` 和 `useCurrentTz+1w` 两条 fdow-aware 路径。
3. **firstDayOfWeek 传播链（Task 1.5 / 4.3 前置）**：parse→plan→physi 各节点补字段，三处 interval 算子初始化填入 `firstDayOfWeek`，`SIntervalPhysiNode` 序列化已更新。
4. **WEEKOFYEAR 初步接入 fdow（Task 4.4 局部）**。
5. **测试**：解除 7 个 skip，删除不合规的 Nx 多倍数用例，新增 Nx 报错断言。

主要风险热点：
- 物理节点序列化（`STableScanPhysiNode` 缺 `firstDayOfWeek` 编解码）
- 无 session timezone 时 n/q/y 截断的边界语义
- `ttime.c` 未更新，`INTERVAL(1w)` 的 fdow 对齐还未实际生效

---

## Final findings

### P1（必须阻塞合并）

#### 1. [P1 / confirmed] Serialization: `STableScanPhysiNode.firstDayOfWeek` 未纳入物理计划编解码

- **Problem**: 分布式场景下 coordinator 将 `STableScanPhysiNode` 序列化后发往 vnode，反序列化后该字段零初始化（→0，即周日），而非配置值（默认 4）。`extractIntervalInfo` 从 `pTableScanNode->firstDayOfWeek` 读取后写入 `SInterval`，导致 vnode 端窗口对齐以周日为起点，与 coordinator 行为不一致。
- **Evidence**:
  - [nodesCodeFuncs.c](source/libs/nodes/src/nodesCodeFuncs.c#L2462) `physiTableScanNodeToJson` / `jsonToPhysiTableScanNode` 中没有任何 `firstDayOfWeek` 的 encode/decode。
  - `SIntervalPhysiNode` 已正确序列化（line 3539/3563/3597 有 `jkIntervalPhysiPlanFirstDayOfWeek`）。
  - [executil.c](source/libs/executor/src/executil.c) diff: `.firstDayOfWeek = pTableScanNode->firstDayOfWeek`。
- **Why change related**: 本次 diff 为 `STableScanPhysiNode` 增加了字段并在运行时读取，却未同步更新 `nodesCodeFuncs.c`，形成新的序列化缺口。
- **Fix direction**: 仿照 `SIntervalPhysiNode` 的方式，在 `physiTableScanNodeToJson` / `jsonToPhysiTableScanNode` 中增加 `jkTableScanPhysiPlanFirstDayOfWeek` 的 encode/decode，默认回退值设为 4（向后兼容旧计划）。

---

#### 2. [P1 / confirmed] Correctness: n/q/y 无 timezone 时静默返回错误结果

- **Problem**: 当 `pInput->tz == NULL`（未设 L2 且无显式 tz 参数）时，`activeTz = NULL` → `tzReady = false`，`isCalendarUnit && !tzReady` 不命中，最终落入 else 分支：
  ```c
  timeVal = timeVal / timeUnit * timeUnit
  ```
  对 `1n`（`timeUnit=1`）等于恒等变换，时间戳原样返回；对 `3n`（`timeUnit=3`）产生毫无意义的整除结果——不报错、不回退到服务端 L4、静默输出错误值。
- **Evidence**:
  - [sclfunc.c](source/libs/scalar/src/sclfunc.c) diff，新增 `isCalendarUnit` 路径：
    ```c
    bool tzReady = (activeTz != NULL);
    if (isCalendarUnit && tzReady) { ... }   // tzReady=false 时无 else 分支
    ```
  - 最终 else：`timeVal = timeVal / timeUnit * timeUnit`（`timeUnit` 来自月数，≥1）。
  - Plan 回退链规格：TIMETRUNCATE 应为 `L2→L4→L5`；L2 缺失时应降到服务端 L4。
- **Why change related**: 本次 diff 新增了日历单位分支，但仅在 `tzReady=true` 时有效，未实现 L4 fallback，引入新的静默错误路径。
- **Fix direction**:
  1. 优先方案：`tzReady=false` 时，尝试用服务端 tz 兜底；或
  2. 安全方案：`tzReady=false` 且 `isCalendarUnit` 时，直接 `return TSDB_CODE_FUNC_MISSING_TZ`，并在文档中说明 n/q/y 需要有效时区（L2 或显式参数）。

---

#### 3. [P1 / confirmed] Spec: `FUNC_PARAM_MUST_BE_TIME_UNIT_OR_CALENDAR` 注释与实现矛盾

- **Problem**: [functionMgtInt.h](source/libs/function/inc/functionMgtInt.h) 注释写 *"also accepts … multi-digit multipliers (e.g. 3n, 2y)"*，但 `validateTimeUnitParamEx` 明确拒绝首字符非 `'1'` 的单位（`literal[0] != '1'` → `TSDB_CODE_FUNC_TIME_UNIT_INVALID`）。Plan Task 4.1 验收标准要求 `2n/3n/2q/2y/2w` 均应报错。注释自相矛盾，将误导维护者，或在后续 PR 中被当作"intentional"而移除该限制。
- **Evidence**:
  - [functionMgtInt.h](source/libs/function/inc/functionMgtInt.h) diff：注释第 3 行。
  - [builtins.c](source/libs/function/src/builtins.c) diff：`validateTimeUnitParamEx`，`pVal->literal[0] != '1'` → INVALID。
  - [test_tz_scalar_functions.py](test/cases/11-Functions/01-Scalar/test_tz_scalar_functions.py) diff：`test_timetruncate_1q_equals_3n_is_invalid` 确认 `3n/2q/2y/2w/6n` 均 `tdSql.error`。
- **Why change related**: 本次 diff 新增该常量和注释，注释直接引入了错误描述。
- **Fix direction**: 将注释改为：
  ```
  /* Like FUNC_PARAM_MUST_BE_TIME_UNIT but also accepts calendar units
   * n/q/y with multiplier strictly 1 (i.e. 1n/1q/1y). Nx (N>1) is invalid. */
  ```

---

#### 4. [P1 / confirmed] Coverage gap: `INTERVAL(1w)` fdow 对齐在 `ttime.c` 未实现，但 TODO 已被部分移除

- **Problem**: `ttime.c:taosTimeTruncate` 对 `'w'` 单位仍使用 `getTZOffsetAtTicks`（epoch 取模），完全不读取 `pInterval->firstDayOfWeek`。Task 4.3 验收标准（`INTERVAL(1w)` 尊重 `firstDayOfWeek`）实际上未达成。同时，`createMergeAlignedIntervalOperatorInfo` 已写入 `.firstDayOfWeek` 但 TODO 注释仍保留，造成"已完成"的错误印象，与 plan 检查清单矛盾。
- **Evidence**:
  - [ttime.c](source/common/src/ttime.c#L1185) 对 `'w'`：
    ```c
    start -= getTZOffsetAtTicks(start, precision, pInterval->timezone);
    ```
    无任何 `firstDayOfWeek` 逻辑。
  - [timewindowoperator.c](source/libs/executor/src/timewindowoperator.c) diff，`createMergeAlignedIntervalOperatorInfo`：
    ```c
    .firstDayOfWeek = pNode->firstDayOfWeek};
    /* TODO(P4): read pNode->firstDayOfWeek (0-6) into interval.firstDayOfWeek */
    ```
    矛盾残留。
  - Plan 检查清单 `[ ] P4: INTERVAL(w) 尊重 firstDayOfWeek` 仍未勾选。
- **Why change related**: 本次 diff 打通了传播链并删除了部分 TODO，造成"功能已落地"的外观，但实际行为未变。
- **Fix direction**:
  - 在 `ttime.c:taosTimeTruncate` 中，对 `slidingUnit == 'w'` 路径新增 fdow-aware 对齐（仿 `sclfunc.c` 中新增的 1w 逻辑）；
  - 删除 `createMergeAlignedIntervalOperatorInfo` 中已失效的 TODO 注释。

---

### P2（应在本 PR 或后续 task 中修复）

#### 5. [P2 / confirmed] Stale TODO: `timewindowoperator.c` `createMergeAlignedIntervalOperatorInfo`

- **Problem**: `.firstDayOfWeek = pNode->firstDayOfWeek` 已写，紧随其后的 `/* TODO(P4): read pNode->firstDayOfWeek ... */` 未删除，与实现矛盾。
- **Evidence**: [timewindowoperator.c](source/libs/executor/src/timewindowoperator.c) diff，line ~3295 与残留注释。
- **Fix direction**: 删除该注释行。

---

#### 6. [P2 / confirmed] Maintainability: `WEEKOFYEAR` fdow 映射仅覆盖 fdow=0/1，fdow=2..6 均退化为 Sunday-start

- **Problem**:
  ```c
  int32_t mode = (pInput->firstDayOfWeek == 1) ? 3 : 2;
  ```
  `fdow=2`（周二）到 `fdow=6`（周六）全部映射为 `mode=2`（周日起始），与"WEEKOFYEAR 尊重 firstDayOfWeek"的语义不符。MySQL WEEK() mode 只支持 Sun-start / Mon-start，这是架构局限，但代码无任何注释说明此约束。
- **Evidence**: [sclfunc.c](source/libs/scalar/src/sclfunc.c) diff，`weekofyearFunction` 末尾。
- **Fix direction**: 补充注释说明 MySQL WEEK mode 只支持 Sun-start / Mon-start；或针对 `fdow=2..6` 直接返回 `TSDB_CODE_FUNC_PARAM_INVALID` 并在文档写明支持范围。

---

### P3（可选改进）

#### 7. [P3 / confirmed] Dead code: `isCalendarUnit` 中 `unitCh == 'q'` 永不命中

- **Problem**: 注释写明 *"'q' is already converted to 'n' (3n) by parseNatualDuration"*，运行时 `unitCh` 永远是 `'n'`，`unitCh == 'q'` / `unitCh == 'Q'` 子表达式是死代码。
- **Fix direction**: 删除 `unitCh == 'q' || unitCh == 'Q'` 子表达式，或将注释移到 `isCalendarUnit` 定义处说明原因。

---

#### 8. [P3 / medium] `SFunctionNode.firstDayOfWeek` 未序列化，影响分布式 WEEKOFYEAR

- **Problem**: `functionNodeToJson` 不序列化 `firstDayOfWeek`（与 `tz` 同源问题），vnode 反序列化后 `firstDayOfWeek=0`，`WEEKOFYEAR` 默认退化为周日起始。这与现有 `tz` 字段的处理方式一致（`tz` 为指针，TIMETRUNCATE 通过注入参数绕过），属于已接受的基础设施局限；但 `WEEKOFYEAR` 目前没有注入路径，在分布式环境下存在行为差异。
- **Fix direction**: 与 `tz` 对齐：either 为 `WEEKOFYEAR` 也注入 fdow 参数（参考 `translateTimeTruncate` 的 `addUint8Param` 模式），或在 `functionNodeToJson` 补序列化。

---

## 阻塞合并问题汇总

| # | 位置 | 核心问题 |
|---|------|---------|
| 1 | `nodesCodeFuncs.c` physiTableScan | `STableScanPhysiNode.firstDayOfWeek` 未序列化，分布式退化为 Sunday |
| 2 | `sclfunc.c` timeTruncateFunction | n/q/y 无 session tz 时静默返回原始时间戳（不报错不降级） |
| 3 | `functionMgtInt.h` 注释 | 注释说支持 3n/2y，实现拒绝，将误导后续维护者 |
| 4 | `ttime.c` + stale TODO | `INTERVAL(1w)` 实际未使用 fdow，但 TODO 已被部分删除造成完成假象 |

---

## Publish preview

- Summary comment: disabled
- Inline comments: disabled
