# enh/tag-ref vs enh/3.0/vstable-ref 查询计划差异分析

## 1. 差异概述

对比当前分支 `enh/tag-ref` 和 `enh/3.0/vstable-ref` 的查询计划，在**没有 tag-ref** 的场景下，发现以下差异：

### 1.1 计划结构一致的部分
- **窗口查询 (window)**: interval/session/state/event — 完全一致 ✅
- **投影查询 (projection)**: select * / select cols — 完全一致 ✅
- **普通聚合 (无tag参数)**: avg/count/sum 等仅操作列的聚合 — 完全一致 ✅
- **虚拟子表查询 (vctable)**: 所有查询 — 完全一致 ✅
- **columns=9→10, width=800→804**: 已知的 decimal 列新增导致的变更 — 已处理 ✅

### 1.2 计划结构不一致的部分（60个EXPLAIN查询）

**影响范围**: 所有聚合函数中包含 tag 参数的查询：
- `bottom(int_tag, 9)`, `top(int_tag, 9)`
- `first(nchar_32_tag, ...)`, `last(nchar_32_tag, ...)`
- `last_row(nchar_32_tag, ...)`
- `max(nchar_32_tag, ...)`, `min(nchar_32_tag, ...)`
- 以上查询在带 `partition by` 的变体中也受影响

**差异表现**:
| 分支 | 计划类型 | 说明 |
|------|---------|------|
| enh/3.0/vstable-ref | `Dynamic Query Control for Virtual Stable Agg` | 聚合下推到各 vnode |
| enh/tag-ref | `Dynamic Query Control for Virtual Stable Scan` + `Virtual Table Scan` | 不做聚合下推，回退到扫描路径 |

## 2. 根因分析

### 2.1 直接原因

commit `429a8711e4` ("fix: improve conditional logic in extractDataBlockFromFetchRsp for clarity") 在 `vstableAggShouldBeOptimized()` 中新增了一行检查：

```c
// source/libs/planner/src/planOptimizer.c:10339
// enh/3.0/vstable-ref 中没有此行:
functionHasTagParam(pFunc) ||
```

完整上下文：
```c
FOREACH(pAggFunc, pAgg->pAggFuncs) {
    SFunctionNode *pFunc = (SFunctionNode *)pAggFunc;
    if (fmIsSelectValueFunc(pFunc->funcId) || fmisSelectGroupConstValueFunc(pFunc->funcId) ||
        fmIsGroupKeyFunc(pFunc->funcId) ||
        functionHasTagParam(pFunc) ||   // ← 新增：阻止所有含tag参数的聚合走VStableAgg
        (fmIsAggFunc(pFunc->funcId) && !fmIsSelectFunc(pFunc->funcId) && functionHasTagOrPkParam(pFunc)) ||
        !fmIsDistExecFunc(pFunc->funcId)) {
      return false;
    }
}
```

- `functionHasTagParam()`: 检查函数参数中是否包含**任何** tag 列 (COLUMN_TYPE_TAG 或 COLUMN_TYPE_TBNAME)
- `functionHasTagOrPkParam()`: 只对非 select 的 agg 函数检查（范围更窄）

### 2.2 为什么不能简单修复

同一 commit 还添加了 `functionHasTagRefParam()` 函数（仅检查 `hasRef` 的 tag），但在 `vstableAggShouldBeOptimized` 中使用了更宽泛的 `functionHasTagParam`。

尝试将 `functionHasTagParam` 替换为 `functionHasTagRefParam`（仅阻止有 ref 的 tag）后：
- **查询计划**: 恢复与 vstable-ref 一致 ✅
- **查询结果**: `select bottom(int_tag, 9) from vtb_virtual_stb` 等查询返回 `NULL`，而 vstable-ref 返回 `0` ❌

这说明 `enh/tag-ref` 分支的 executor 层在处理 VStableAgg 优化路径时，对含 tag 参数的聚合函数的处理存在问题。`functionHasTagParam` 是一个**有意的 workaround**，避开了有问题的 VStableAgg 执行路径。

### 2.3 Executor 层的差异

`enh/tag-ref` 分支对 executor 做了大量修改（2300+ 行），关键影响：

1. **`extractColMap()` 逻辑变更** (`virtualtablescanoperator.c`):
   - 旧: `pCol->colType == COLUMN_TYPE_TAG || '\0' == pCol->tableAlias[0]`
   - 新: `pCol->colType != COLUMN_TYPE_COLUMN && !pCol->hasRef`
   - 影响 tag 列的 blockId 识别

2. **Tag 下游检测重写**: 从基于 index 的假设改为显式循环

3. **`lastTs` 初始化修改**: `0` → `INT64_MIN`（bug fix，正确）

4. **VStableAgg 路径中 tag 数据传播**: VStableAgg 优化会重构计划，将 VirtualScan 替换为直接的 Aggregate+TableScan。在 tag-ref 修改后，这个重构过程中 tag 值的传播可能被破坏。

### 2.4 第二个计划差异：pConditions guard

commit `d371242c52` 还添加了：
```c
// 如果 VirtualScan 携带 tag 过滤条件，则不做 VStableAgg 优化
if (pDynCtrl && LIST_LENGTH(pDynCtrl->node.pChildren) >= 1) {
  SLogicNode* pFirst = (SLogicNode*)nodesListGetNode(pDynCtrl->node.pChildren, 0);
  if (QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN == nodeType(pFirst) && pFirst->pConditions) {
    return false;
  }
}
```

这个 guard 对非 tag-ref 查询**没有影响**（pConditions 为 NULL），是安全的。

## 3. 结论

### 当前状态
- **无 tag 参数的聚合查询**: 计划与 vstable-ref 完全一致 ✅
- **含 tag 参数的聚合查询**: 计划结构不同，但查询结果正确 ⚠️
  - `functionHasTagParam` 是避免 VStableAgg 执行错误的必要 workaround
  - 回退到 VStableScan 路径确保正确性，但牺牲了部分优化

### 修复建议
要让含 tag 参数的聚合查询也恢复 VStableAgg 优化，需要：
1. 排查 executor 中 `virtualtablescanoperator.c` 的 `extractColMap()` 变更对 tag blockId 识别的影响
2. 确认 `dynqueryctrloperator.c` 中 VStableAgg 路径的 tag 数据传播在 tag-ref 重构后是否正确
3. 修复后移除 `functionHasTagParam` workaround，替换为 `functionHasTagRefParam`

### 影响评估
- 性能影响: 含 tag 参数的聚合查询无法做聚合下推，需要全量扫描后在客户端聚合
- 正确性: 不受影响，结果正确
- 涉及查询: bottom/top/first/last/last_row/max/min 等使用 tag 列作为参数的查询
