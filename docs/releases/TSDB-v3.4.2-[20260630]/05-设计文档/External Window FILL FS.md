# External Window FILL 功能支持 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-01 | - | 0.1 | 任新胜 | 按模板整理文档格式，保留原设计结论 |
| 2026-04-03 | - | 0.2 | 任新胜 | 补充伪列无 FILL 时空窗口输出说明；补充 merge aligned + HAVING 顺序说明 |
| 2026-04-13 | 2026-04-13 | 1.0 | 关胜亮 | 评审、重命名文档、发布 |

## 2. 背景

当前 external_window 在聚合模式下已经具备空窗口计算能力，但 FILL 能力仍分别在语法层、语义层和计划层被阻断，默认整体行为等价于 `FILL(NONE)`。为了让 external_window 与 interval fill 在窗口级语义上具备可比较、可演进的能力，需要明确 external_window FILL 的适用范围、模式定义、边界场景、实现路径和验收标准。

## 3. 定义

1. 空窗口：指由 external_window 子查询显式定义、但窗口范围内没有源数据命中的窗口。
2. external_window FILL：指对空窗口按照指定填充模式生成结果行的语义。
3. forced / non-forced：指 `NULL` 与 `NULL_F`、`VALUE` 与 `VALUE_F` 之间是否在“当前窗口集合完全无自然结果行”时仍强制输出窗口结果的差异。

## 4. 行为说明

### 4.1 核心语义

> **external_window FILL 的含义是：对在窗口定义范围内没有源数据命中的“空窗口”，按照指定的填充模式生成结果行。**

与 INTERVAL FILL 的类比如下：

| 对比项 | INTERVAL FILL | External Window FILL |
| --- | --- | --- |
| 窗口集合 | 由 `time_range / interval` 隐式推算 | 由子查询显式定义（确定的窗口数组） |
| “空窗口”含义 | 某个时间间隔内无数据 | 子查询定义的某个窗口范围内无源数据 |
| 填充粒度 | 窗口级（每个空窗口一行） | 窗口级（每个空窗口一行） |
| 窗口间距 | 等宽（固定 interval/sliding） | 不等宽（由子查询决定） |

关键差异在于 INTERVAL 窗口等宽等距，因此可以支持依赖规则时间轴的 `LINEAR`；external_window 的窗口由子查询显式定义，可能不等宽、不等距，因此本设计不支持 `LINEAR`。

### 4.2 适用范围

| 执行模式 | 是否支持 FILL | 原因 |
| --- | --- | --- |
| `EEXT_MODE_AGG`（聚合） | 是 | 每窗口一行输出，空窗口需决定是否出行、出什么值 |
| `EEXT_MODE_SCALAR`（投影） | 否 | 投影模式按行原样输出，无“空窗口”概念 |

补充说明：当前 external_window 实际实现范围可按“聚合模式 vs 投影模式”定义，`EEXT_MODE_INDEFR_FUNC` 在 external_window 场景下暂不作为需求边界讨论对象。

设计选择：当 SELECT 中无聚合函数时，FILL 子句应在语义层报错，和 INTERVAL 场景中的聚合要求保持一致。

### 4.3 各 FILL 模式行为定义

| FILL 模式 | 行为描述 | 示例 |
| --- | --- | --- |
| **NONE** | 跳过空窗口，不输出行，等价于当前默认行为 | W1=有数据，W2=空，W3=有数据 -> 输出 W1、W3 |
| **NULL** | 为空窗口输出一行；可填充聚合列输出 `NULL`。当当前窗口集合至少存在一个非空窗口时，`NULL` 与 `NULL_F` 对空窗口的填充值表现一致 | W2 -> `(_wstart=W2.skey, _wend=W2.ekey, avg=NULL)` |
| **NULL_F** | 强制填充。在“部分窗口为空”场景下与 `NULL` 一致；差异仅体现在“当前窗口集合中所有窗口都为空”时，`NULL_F` 仍输出所有窗口结果行。| W2 -> `(_wstart=W2.skey, _wend=W2.ekey, avg=NULL)`；当所有窗口均为空时仍出行 |
| **VALUE(v1,v2...)** | 为空窗口输出一行，可填充聚合列使用用户指定值。当当前窗口集合至少存在一个非空窗口时，`VALUE` 与 `VALUE_F` 对空窗口的填充值表现一致 | W2 -> `(_wstart=W2.skey, sum=v1, avg=v2)` |
| **VALUE_F(v1,v2...)** | 强制 VALUE 版本。在“部分窗口为空”场景下与 `VALUE` 一致；差异仅体现在“当前窗口集合中所有窗口都为空”时，`VALUE_F` 仍输出所有窗口结果行 | 同上 |
| **PREV** | 用前一个非空窗口的整行聚合结果填充空窗口；若不存在前序非空窗口，则聚合列为 `NULL` | W2 用 W1 的结果 |
| **NEXT** | 用后一个非空窗口的整行聚合结果填充空窗口；若不存在后序非空窗口，则聚合列为 `NULL` | W2 用 W3 的结果 |
| **LINEAR** | 不支持 | 语义层报错 |
| **NEAR** | 不支持（INTERP 专属） | 语义层报错 |

说明：上表为目标语义设计，不等同于当前 external_window 已支持。

### 4.4 与 INTERVAL FILL 的能力关系

| 模式 | INTERVAL 现状 | external_window 当前现状 | external_window 建议 |
| --- | --- | --- | --- |
| NONE | 支持 | 语法可写但语义层禁用 | 支持 |
| NULL / NULL_F | 支持 | 语法可写但语义层禁用 | 支持 |
| VALUE / VALUE_F | 支持 | external_window 语法当前不支持 | 建议支持 |
| PREV / NEXT | 支持 | external_window 语法当前不支持 | 支持 |
| LINEAR | 支持 | external_window 语法可写但语义层禁用 | 不支持 |
| NEAR | INTERP 支持，INTERVAL 不支持 | external_window 不支持 | 不支持 |

本设计最终范围如下：

- 支持：`NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT`
- 不支持：`LINEAR/NEAR/SURROUND`

### 4.5 伪列处理

未指定 FILL 或显式使用 `FILL(NONE)` 时，即使 SELECT 列表中包含 `_wstart`、`_wend`、`_wduration` 或 `w.xxx` 等伪列，空窗口也不额外产出结果行。伪列只描述最终已经输出的窗口，不具备单独把空窗口物化出来的能力。

**补充说明（无 FILL 且查询包含伪列时空窗口是否输出）：**

当查询中存在伪列且没有设置 FILL（或设置为 `FILL(NONE)`）时，空窗口不输出。原因如下：

- external_window executor 在聚合模式下，空窗口虽然会参与计算（`extWinAggHandleEmptyWins` 用空 block 调用 `extWinAggDo`），但在输出阶段（`extWinAggOutputSingleCGrpRes`）会跳过空窗口，整体行为等价于 `FILL(NONE)`。
- 伪列（`_wstart`、`_wend`、`_wduration`、`w.xxx`）只在窗口结果行已被输出时才会填充对应值，它们不会触发空窗口的物化。
- 这与 interval 的行为一致：interval 不带 FILL 时，空窗口也不输出，即使 SELECT 中包含 `_wstart` 等伪列。

简言之：“伪列是被动填充，不是主动触发”。空窗口是否出行完全由 FILL 模式决定，伪列的存在不改变窗口输出策略。

空窗口被填充时：

| 伪列 | 值来源 | 说明 |
| --- | --- | --- |
| `_wstart` | 窗口定义 `pWin->tw.skey` | 不受 FILL 模式影响 |
| `_wend` | 窗口定义 `pWin->tw.ekey` | 不受 FILL 模式影响 |
| `_wduration` | `ekey - skey` | 不受 FILL 模式影响 |
| 窗口属性列 `w.xxx` | 子查询该行对应值 | 不受 FILL 模式影响，子查询保证每窗口都有一行 |

### 4.6 PARTITION BY、MERGE ALIGNED 与 HAVING 交互

- FILL 在每个 partition 内独立执行。
- `PREV/NEXT` 不跨 partition 边界。
- 第一个 partition 的第一个空窗口使用 `PREV` 时，行为与 INTERVAL FILL 一致，该列为 `NULL`。
- 在 `PARTITION BY` 场景下，按 interval 已验证的现状处理：对完全缺席的分组，不额外补出该分组；`NULL/NULL_F`、`VALUE/VALUE_F` 结果等价。
- HAVING 在 FILL 之后执行。

**补充说明（merge aligned external window + HAVING 处理顺序）：**

在 merge aligned external window 场景下（多 vgroup 汇聚），执行顺序为：

1. **各 vgroup 分别执行 external_window 聚合**，各 vgroup 独立计算每个窗口的聚合结果。
2. **Merge Aligned 层：汇聚对齐**，`SMergeAlignedExternalWindowOperator` 将多个 vgroup 的同一窗口结果合并，产出每个窗口的最终聚合值。
3. **FILL 处理**：合并完成后，对空窗口按 FILL 模式生成填充结果行。FILL 逻辑嵌入在 ExternalWindowOperator 内部，不是独立的 FillOperator。
4. **HAVING 过滤**：对 FILL 后的结果行执行 HAVING 过滤，填充产生的窗口可以被 HAVING 保留或剔除。

整体顺序保证：

```
vgroup-level agg -> merge aligned -> FILL -> HAVING -> ORDER BY -> PROJECTION
```

这与 interval + FILL + HAVING 的顺序一致：总是先完成窗口聚合和对齐，再 FILL，最后 HAVING。不会出现“HAVING 在 FILL 之前就裁剪了空窗口”的情况。

**注意事项**：

- 当 HAVING 中引用了 SELECT 列表中不存在的聚合函数（如 `avg(v)`），该函数会被收集到 planner 的 `pFuncs` 列表中，但不影响 FILL 值的映射顺序（已通过从 `pProjectionList` 构建 `pFillExprs` 修复，见 bug 修复记录）。
- merge aligned 层的窗口合并是透明的：无论单 vgroup 还是多 vgroup，FILL 和 HAVING 的语义均保持一致。

### 4.7 Forced vs Non-Forced

本节只定义无 `PARTITION BY` 场景下的主语义。

1. 只要当前窗口集合中至少存在一个非空窗口，`NULL == NULL_F`，`VALUE == VALUE_F`。
2. forced / non-forced 的差异，只出现在“当前窗口集合没有任何自然结果行”时。
3. 当前 external_window 回归脚本显式覆盖了“源表完全为空”场景；“源表在窗口集合之外有数据”的 forced/non-forced 差异由 interval 对照用例覆盖，语义上保持一致。

| 场景 | Non-Forced (`NULL/VALUE`) | Forced (`NULL_F/VALUE_F`) |
| --- | --- | --- |
| 所有窗口均有数据 | 正常输出 | 正常输出 |
| 部分窗口空，但至少有一个窗口非空 | 对空窗口填充；整体结果与 forced 版本一致 | 对空窗口填充；整体结果与 non-forced 版本一致 |
| 所有窗口均空，且源表在窗口集合之外有数据 | 不输出任何行 | 仍输出所有窗口 |

说明：

- “所有窗口均空”指 external_window 当前这组显式窗口中，没有任何窗口命中源数据。
- 源表本身无数据时，行为同上，不再单独展开。
- 有分组场景单独遵循 `4.6 PARTITION BY、MERGE ALIGNED 与 HAVING 交互`，forced / non-forced 结果等价，不再额外拉开差异。

### 4.8 SURROUND 支持

不支持，也不纳入本设计范围，原因如下：

- `SURROUND` 的 duration 语义依赖等宽时间间隔。
- external_window 窗口不等宽，`SURROUND` 的“时间距离”定义不直观。
- 可在语义层报错：`SURROUND not supported with EXTERNAL_WINDOW`。

### 4.9 边界场景

| 场景 | 预期行为 |
| --- | --- |
| 所有窗口都有数据 | FILL 不影响结果，正常输出 |
| 只有一个窗口且为空（NULL/VALUE 系） | `NONE/NULL/VALUE` 无输出；`NULL_F/VALUE_F` 输出一行 |
| 首个窗口空（PREV） | 聚合列填 `NULL`，因为无前序数据 |
| 末个窗口空（NEXT） | 聚合列填 `NULL`，因为无后续数据 |
| 连续多个空窗口（PREV） | 全部使用同一个前序非空窗口值填充 |
| 部分窗口空，但至少一个窗口非空 | `NULL == NULL_F`，`VALUE == VALUE_F` |
| 所有窗口均空，但源表在窗口集合外仍有数据 | `NULL/VALUE` 不出行；`NULL_F/VALUE_F` 仍输出全部窗口 |
| `PARTITION BY` 下某个分组在查询范围内完全无数据 | 按当前 interval 已验证的现状处理：该分组不出行；`NULL/NULL_F`、`VALUE/VALUE_F` 结果等价 |
| `PARTITION BY` + 空窗口 | 每个 partition 独立处理，不跨 partition 引用 `PREV/NEXT` |
| 无 FILL / `FILL(NONE)` 填充时 | 空窗口不出行 |
| `FILL(...)` + `HAVING(...)` | 先 fill，再按 HAVING 过滤；filled window 可以被 HAVING 保留或剔除 |
| merge aligned external_window + HAVING | 保持“先对齐并生成 fill 结果，再执行 HAVING”的顺序 |
| 非聚合查询 + FILL | 语义报错：`Fill only supports aggregate query with external window` |
| 窗口属性列 `w.xxx` | 不受 FILL 影响，始终取子查询对应行的值 |
| `_wstart` / `_wend` | 始终取窗口定义值，不受 FILL 影响 |

## 5. 性能

推荐在 `ExternalWindowOperator` 内部实现填充逻辑，而不是复用独立 `FillOperator`。原因如下：

- `taosFillResultDataBlock()` 的核心循环按固定 interval 推进 `currentKey`，无法直接适配不等宽窗口。
- ExternalWindowOperator 已持有完整窗口数组、空窗口识别结果和结果行缓存，改动可以集中在现有执行链路中。
- `NEXT` 即使采用两遍扫描策略，因外部窗口数组已预构建在内存中，额外成本可控。

## 6. 安全

不涉及。

## 7. 兼容性

需要重点关注以下兼容性风险：

1. 空窗口“参与计算但默认不输出”的既有行为被改动后，老查询结果集行数可能发生变化。
2. `first/last` 等聚合函数在空窗口填充行上的表现仍需通过回归用例明确守卫，避免后续实现调整带来语义漂移。
3. 多 partition 场景下若 fill 状态串组，容易出现 PREV/NEXT 跨分组污染。

兼容性目标如下：

1. 默认 external_window 用例在不开启 FILL 的情况下行为不变。
2. 历史 external_window 用例在默认配置下 100% 通过。
3. explain、错误码和日志能够区分“未支持、参数错误、执行异常”三类失败。

## 8. 运维

不涉及。

## 9. 使用场景

一些使用示例如下：

```sql
-- 基本语法
SELECT _wstart, _wend, COUNT(*), AVG(val)
FROM source_table
EXTERNAL_WINDOW(
  (SELECT ts, endtime FROM window_def) w
  FILL(NULL)
);

-- 带 VALUE 填充
SELECT _wstart, SUM(val) AS total, AVG(val) AS avg_val
FROM source_table
EXTERNAL_WINDOW(
  (SELECT ts, endtime FROM window_def) w
  FILL(VALUE, 0, 0)
);

-- 带 PREV 填充 + PARTITION BY
SELECT t1, _wstart, AVG(val)
FROM source_table
PARTITION BY t1
EXTERNAL_WINDOW(
  (SELECT ts, endtime FROM window_def) w
  FILL(PREV)
);

-- 强制填充，即使所有窗口都为空也出行
SELECT _wstart, COUNT(*) AS cnt
FROM source_table
EXTERNAL_WINDOW(
  (SELECT ts, endtime FROM window_def) w
  FILL(NULL_F)
);
```

## 10. 约束和限制

1. 本设计最终只支持 `NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT`。
2. `LINEAR`、`NEAR`、`SURROUND` 不支持，也不纳入本次实现范围。
3. FILL 仅对 external_window 聚合模式生效，非聚合查询使用 FILL 需要在语义层报错。
4. `PARTITION BY` 下完全缺席的分组不额外物化，force 模式也不单独补出该分组。
5. 本设计是 external_window 的目标语义设计，不等同于当前版本已经支持所有模式。

## 11. 常见错误和排查

1. `LINEAR/NEAR/SURROUND` 应统一在语义层报不支持，避免和 parser 阶段报错混淆。
2. `VALUE/VALUE_F` 需要校验值个数与可填充聚合列是否一致，同时校验类型能否转换到目标列。
3. 如出现 explain 或远端执行异常，需要优先检查 planner 新增字段是否在 clone/code/msg 链路中遗漏。
4. 如出现分组结果串组，需要优先检查 `PREV/NEXT` 填充状态是否按 partition 隔离。

## 12. 可观测性

验收时需要确保以下可观测性要求满足：

1. explain 能够体现 external_window fill 的模式与相关配置。
2. 错误码和报错文案能够区分“不支持的 fill 模式”“参数错误”“执行异常”。
3. 回归用例同时覆盖输出行数、关键列值和错误码三类结果，形成最小闭环。

## 13. 安装和卸载

无。

## 14. 文档

1. 需要维护 external_window FILL 设计文档本身。
2. 需要同步维护配套测试文档：`../06-功能测试/external_window FILL-TS.md`。

## 15. 参考文档

1. `../06-功能测试/external_window FILL-TS.md`
2. `community/source/libs/parser/inc/sql.y`
3. `community/source/libs/parser/src/parTranslater.c`
4. `community/source/libs/planner/src/planLogicCreater.c`

## 16. 附录

### 16.1 当前现状

#### 16.1.1 禁用方式

FILL 在三层被阻断：

| 层次 | 现状 | 代码位置 |
| --- | --- | --- |
| **语法层** | `external_window_fill_opt` 只接受 `FILL(fill_mode)`，即 `NONE/NULL/NULL_F/LINEAR`；`VALUE/VALUE_F/PREV/NEXT/NEAR` 在 external_window 语法下直接报错 `0x80002600` | `community/source/libs/parser/inc/sql.y` L2751-L2756 |
| **语义层** | `translateWindow()` 中检测到 `pExtWin->pFill != NULL` 后无条件拒绝，报错 `Fill not allowed in external window query` (`0x80002657`) | `community/source/libs/parser/src/parTranslater.c` L9669-L9673 |
| **计划层** | `SExternalWindowPhysiNode` 无 `pFill` 字段；`createFillLogicNode()` 仅对 `QUERY_NODE_INTERVAL_WINDOW` 生效 | `community/source/libs/planner/src/planLogicCreater.c` L2947 |

AST 层 `SExternalWindowNode` 已有 `pFill` 字段预留，但从未生效。

#### 16.1.2 空窗口的现有处理

在聚合模式（`EEXT_MODE_AGG`）中，external_window executor 已有空窗口处理：

- `extWinAggHandleEmptyWins()`：对没有源数据落入的窗口，用 `pEmptyInputBlock` 调用 `extWinAggDo()`，在聚合计算阶段生成该窗口的空聚合结果。
- `extWinAggOutputSingleCGrpRes()`：在结果输出阶段，非 vtable 路径会对空窗口执行 `continue` 跳过；仅 vtable（`isDynWindow`）为了列对齐会输出全 `NULL` 行。

说明：“空窗口会参与计算”与“空窗口默认不出行”并不矛盾，前者发生在计算阶段，后者是输出阶段策略；当前默认行为整体等价于 `FILL(NONE)`。

### 16.2 实现路径分析

#### 16.2.1 方案选择

| 方案 | 优点 | 缺点 |
| --- | --- | --- |
| 方案 A：在 `ExternalWindowOperator` 内实现 | 窗口数组已在手；空窗口处理逻辑已有雏形；改动集中 | `PREV/NEXT` 逻辑需要新写 |
| 方案 B：复用独立 `FillOperator` | 可复用 `PREV/NEXT` 逻辑；架构更统一 | `FillOperator` 假设等宽 interval，`currentKey += sliding` 不适用；需大量改造 `taosFillResultDataBlock` |

推荐方案 A：在 `ExternalWindowOperator` 内部实现填充逻辑。

#### 16.2.2 各层改动清单

Parser 层：

1. 修改 `sql.y`，扩展 `external_window_fill_opt` 语法，使 external_window 能解析 `VALUE/VALUE_F/PREV/NEXT/LINEAR/NEAR/SURROUND` 等 fill 语法形式。
2. 在 `parTranslater.c` 中移除 `translateWindow()` 的无条件拒绝；增加 `translateExternalWindowFill()`，统一校验 FILL 模式约束，禁用 `LINEAR/NEAR/SURROUND`，并校验 `VALUE` 值个数。
3. `parAstCreater.c` 中已有 `createExternalWindowClause()` 会存储 `pFill`，无需改动。

Planner 层：

1. 在 `SExternalWindowPhysiNode` 中增加 `EFillMode mode`、`SNode* pValues`、`SNodeList* pFillExprs` 等字段。
2. 在 `createWindowLogicNodeByExternal()` 中处理 `pExtWin->pFill`，将 fill 信息存入 `SWindowLogicNode`。
3. 在 `createExternalWindowPhysiNode()` 中将 fill 信息传入物理节点。
4. 在 `nodesCloneFuncs.c`、`nodesCodeFuncs.c`、`nodesMsgFuncs.c` 中补齐序列化和克隆链路。

Executor 层：

1. 在 `createExternalWindowOperator()` 中读取 fill 配置并初始化 fill 状态。
2. 在聚合输出函数中把“空窗口直接 continue”替换为“按 fill 模式输出”。
3. `PREV` 模式记录上一个非空窗口的结果行，空窗口复制之。
4. `NEXT` 模式通过预扫描或延迟输出找到下一个非空窗口并完成回填。
5. `VALUE` 模式直接使用 `pFillValues` 填充。

#### 16.2.3 PREV/NEXT 的实现策略

`PREV`：顺序输出，维护 `prevRow` 缓存。每输出一个非空窗口就更新 `prevRow`；遇到空窗口时复制 `prevRow`；如果 `prevRow` 不存在则输出 `NULL`。

`NEXT`：需要前瞻，可选两种策略：

1. 两遍扫描：第一遍标记每个窗口是否为空，第二遍输出时从后向前找下一个非空窗口。窗口数组已全部在内存中，成本可控。
2. 延迟输出：缓存连续空窗口，遇到非空窗口时回溯填充并批量输出。

推荐策略 1，因为外部窗口数组在 `extWinOpen()` 中已全部预构建。

### 16.3 测试覆盖规划

| 测试类别 | 内容 |
| --- | --- |
| 正向 - 基本 | 各 FILL 模式 + 有空窗口，验证输出行数和值 |
| 正向 - 全空 | 所有窗口空 + `NULL` vs `NULL_F` / `VALUE` vs `VALUE_F`，验证 forced/non-forced |
| 正向 - 表内有数据但窗口集合全空 | 验证 non-forced 不出行、forced 仍出行 |
| 正向 - 部分空窗口 | 验证 `NULL == NULL_F`、`VALUE == VALUE_F` |
| 正向 - 全有数据 | 无空窗口 + FILL，验证不影响结果 |
| 正向 - PARTITION | `PARTITION BY + FILL(PREV/NEXT)`，验证不跨分区 |
| 正向 - PARTITION 全缺席分组 | 验证 `NULL/NULL_F`、`VALUE/VALUE_F` 在完全缺席分组上结果等价 |
| 正向 - 窗口属性 | `w.xxx` 在填充行中可正常引用 |
| 正向 - 伪列 | 填充行的 `_wstart/_wend/_wduration` 正确 |
| 负向 | 非聚合 + FILL 报错 |
| 负向 | `FILL(LINEAR/NEAR)` 语义层报错 |
| 负向 | `FILL(VALUE)` 值个数不匹配时报错 |
| 负向 | `SURROUND` 语义层报错 |

### 16.4 开发任务拆分

#### 16.4.1 本次实现范围

目标：落地最终支持范围 `NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT`。

Parser 模块任务：

1. 修改 `sql.y`，扩展 `external_window_fill_opt`。
2. 不在 parser 阶段拦截 `LINEAR/NEAR/SURROUND`，统一留到语义层报错。
3. 更新 parser 负例，覆盖 VALUE 值个数、常量约束和类型约束。

Translator 模块任务：

1. 移除 external_window 上对 `pFill` 的无条件拒绝。
2. 新增 `translateExternalWindowFill()`，校验聚合约束、VALUE 个数与类型转换，并统一拦截 `LINEAR/NEAR/SURROUND`。
3. 统一错误码和报错文案，避免与 interval 分支混淆。

Planner 模块任务：

1. 在 `SWindowLogicNode` / `SExternalWindowPhysiNode` 增加 fill 相关字段。
2. `createWindowLogicNodeByExternal()` 传递 fill 信息到 window logic node。
3. `createExternalWindowPhysiNode()` 完成 fill 字段下发。
4. 补齐 clone/code/msg 三处序列化链路。

Executor 模块任务：

1. `createExternalWindowOperator()` 读取 fill 配置并初始化 fill 状态。
2. 在聚合输出函数中替换“空窗口直接 continue”为“按 fill 模式输出”。
3. 按分组独立处理，不跨 partition 泄露填充状态。

Test 模块任务：

1. 将现有 `fill_external_window_negative` 拆分为已支持模式的正例和未支持模式的负例。
2. 新增 all-empty、partial-empty、out-of-range-but-table-non-empty、all-non-empty 四类核心回归。
3. 新增 `PARTITION BY` + 完全缺席分组回归，验证 `NULL/NULL_F`、`VALUE/VALUE_F` 在该场景下结果等价。
4. 覆盖 `_wstart/_wend/_wduration` 与 `w.xxx` 的填充行行为。

#### 16.4.2 明确不支持范围

1. `LINEAR`：不实现。
2. `NEAR`：不实现。
3. `SURROUND`：不实现。
4. 如后续新增需求，需单独补充设计并重新评审。

### 16.5 风险与回滚策略

#### 16.5.1 主要风险

1. 语义风险：空窗口“参与计算但默认不输出”的既有行为被改动后，老查询结果集行数变化。
2. 分组风险：多 partition 场景下 fill 状态串组，尤其是 `PREV/NEXT`。
3. 工程风险：planner 字段新增后若 clone/code/msg 漏改，可能导致 explain 或远端执行异常。
4. 范围风险：若实现时误把 `LINEAR/NEAR/SURROUND` 一并放开，会扩大改动面并引入未定义语义。

#### 16.5.2 风险控制

1. 范围控制：本次仅实现 `NONE/NULL/NULL_F/VALUE/VALUE_F/PREV/NEXT`，明确禁止 `LINEAR/NEAR/SURROUND`。
2. 结果守卫：在回归中加入“行数 + 关键列值 + 错误码”三重断言。
3. 差异审计：对历史 external_window 用例做 A/B 比较，确认默认路径无回归。

#### 16.5.3 回滚策略

1. 快速回滚：回滚 parser/translater 放开逻辑，恢复到“语义层拒绝 fill”的旧行为。
2. 执行层回滚：如有必要，同时回滚 external_window executor 的 fill 输出逻辑。
3. 数据安全：该功能只影响查询结果，不落盘，无数据格式升级风险。

### 16.6 测试矩阵与验收标准

#### 16.6.1 测试矩阵

| 维度 | 子项 | 验证点 |
| --- | --- | --- |
| 模式 | NONE | 空窗口不出行，结果与当前行为一致 |
| 模式 | NULL / NULL_F | 空窗口强制输出行，无数据时填充 `NULL`；仅在“所有窗口均空”时出现 forced / non-forced 差异 |
| 模式 | VALUE / VALUE_F | 空窗口出行；填充值个数与类型匹配；仅在“所有窗口均空”时出现 forced / non-forced 差异 |
| 模式 | PREV / NEXT | 空窗口按相邻非空窗口结果填充；不跨 partition |
| 数据分布 | all-empty | 非强制模式不出行；强制模式出行 |
| 数据分布 | partial-empty | 仅空窗口受影响，非空窗口结果不变；`NULL == NULL_F`，`VALUE == VALUE_F` |
| 数据分布 | out-of-range-but-table-non-empty | 源表有数据但当前窗口集合全空时，行为与 all-empty 一致 |
| 数据分布 | all-non-empty | 与不开启 fill 结果完全一致 |
| 分组 | PARTITION BY | 各 partition 独立填充，不串组；按当前 interval 已验证的现状，完全缺席分组下 `NULL/NULL_F`、`VALUE/VALUE_F` 结果等价 |
| 列类型 | 数值/字符串/布尔 | VALUE 类型转换符合预期，错误场景报错准确 |
| 伪列 | `_wstart/_wend/_wduration` | 填充行伪列值来源正确 |
| 窗口属性 | `w.xxx` | 填充行仍可读取窗口属性 |
| 负例 | `LINEAR/NEAR/SURROUND` | 报错稳定且错误码正确 |

#### 16.6.2 验收标准

1. 功能正确性：本设计支持的模式全部通过新增回归，结果符合本文语义。
2. 语义一致性：新增回归与本文定义的 fill 语义保持一致。
3. 兼容性：旧 external_window 用例在默认配置下 100% 通过。
4. 稳定性：无新增 crash、assert、内存泄漏。
5. 可观测性：explain、错误码、日志可区分“未支持、参数错误、执行异常”三类失败。
6. 可回退性：回滚 parser/translater 与 executor fill 改动后，行为与当前版本一致。

