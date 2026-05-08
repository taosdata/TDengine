# VST 继承查询：EXPAND 方案 vs UNION ALL 内部改写方案 全面对比

> 基于 [17-vst-inheritance-fs.md](./17-vst-inheritance-fs.md) 的设计规格。
>
> **场景**：用户写 `SELECT * FROM parent_vst EXPAND(-1)`，引擎内部如何实现？
>
> **当前实现状态**：方案 B（UNION ALL 改写）已实施。`expandFromAncestor` 字段已移除（PRIVATE 由 schema 投影天然排除，无需运行时判断）。

---

## 1. FS 核心行为要点

### 1.1 数据模型

```
parent_vst (ts, val INT)                     TAGS (t1 INT)
  ├── child_vst (ts, val, extra FLOAT)       TAGS (t1, t2 BINARY(16))
  │     └── grandchild_vst (ts, val, extra, deep INT)  TAGS (t1, t2, t3 INT)
  └── child_vst2 (ts, val, temp FLOAT)       TAGS (t1, t4 INT)
```

VCT 列表：
- parent_vst: vct_p1, vct_p2
- child_vst: vct_c1, vct_c2, vct_c3, vct_c4
- grandchild_vst: vct_g1
- child_vst2: vct_m1
- **总计 8 个 VCT**

### 1.2 EXPAND 核心规则

| 规则 | 说明 |
|------|------|
| **Schema = 被查询 VST** | `SELECT * FROM parent_vst EXPAND(-1)` 的 schema 永远是 (ts, val)，不含子孙私有列 |
| **EXPAND 只扩展行** | 纳入子孙 VCT 的数据行，不扩展列 |
| **只能引用自身 schema** | `SELECT extra FROM parent_vst EXPAND(-1)` → 语义错误 |
| **PRIVATE 是 VCT 私有列** | VCT 的私有列不在 VST schema 中，由 schema 投影天然排除 |
| **Tag 同理** | 只能引用被查询 VST 自身 Tag |

### 1.3 关键查询行为（FS §3.4）

```sql
-- 1. parent_vst EXPAND(-1): schema=(ts, val), 含全部 8 个 VCT, 16 行
--    子孙额外列（extra, deep, temp）不在结果中，由 schema 投影排除
SELECT * FROM parent_vst EXPAND(-1);

-- 2. child_vst EXPAND(-1): schema=(ts, val, extra), 含 vct_c1~c4 + vct_g1, 10 行
--    grandchild 的 deep 不在结果中
SELECT * FROM child_vst EXPAND(-1);

-- 3. INTERVAL 直接支持
SELECT _wstart, COUNT(*) FROM parent_vst EXPAND(-1) INTERVAL(5s);

-- 4. Stream 直接支持
CREATE STREAM s INTO out AS
SELECT _wstart, SUM(val) FROM parent_vst EXPAND(-1) INTERVAL(10s);
```

---

## 2. 两种实现方案定义

### 方案 A：当前 EXPAND 实现（单算子 + 运行时）

```sql
SELECT * FROM parent_vst EXPAND(-1) WHERE val > 10;
```

引擎生成一个紧凑的执行计划：
```
Project (schema: ts, val)
  └── DynQueryCtrl (expandLevel=-1, descendants=[child_vst, grandchild_vst, child_vst2])
        ├── VirtualScanNode (parent_vst 模板)
        │     ├── RealTableScan (源表扫描模板)
        │     └── TagScan
        └── SysTableScan (ins_vc_cols, 全量扫描不过滤 stableId)
```

运行时行为：
1. 一次 sys scan 返回所有 VCT 的 colRef（不按 stableId 过滤）
2. 用 `pExpandDescendantStbs` hash 过滤：仅保留属于 parent/child/grandchild/child2 的 VCT
3. 输出 schema = parent_vst 自身 (ts, val)，只取这些列的值
4. 对每个 VCT 逐个构建 Exchange 参数，发 RPC 到源表取 val 值
5. VCT 的私有列不在 VST schema 中，由 schema 投影天然排除

### 方案 B：Translator/Planner 改写为 UNION ALL

用户写同样的 SQL，Translator 在编译时将其改写为内部等价查询：
```
Project (输出 schema: ts, val)
  └── SetOperator (UNION ALL)
        ├── DynQueryCtrl (parent_vst, SELECT ts, val)
        ├── DynQueryCtrl (child_vst, SELECT ts, val)
        ├── DynQueryCtrl (grandchild_vst, SELECT ts, val)
        └── DynQueryCtrl (child_vst2, SELECT ts, val)
```

每个分支是独立的虚拟表查询：
- 投影列 = 父 VST schema 列（ts, val）
- 只扫描该 VST **自身** VCT（不含子孙）
- VCT 私有列不在投影列中，天然排除

---

## 3. 架构层面对比

```
                    方案 A（EXPAND）                        方案 B（UNION 改写）
               ┌─────────────────────┐            ┌────────────────────────────────┐
  Translator   │ 获取 descendants     │            │ 获取 descendants               │
               │ schema = 被查询 VST  │            │ 为每个 VST 生成独立 SSelectStmt │
               │ → 传给 Planner       │            │ 投影列 = 父 schema 列          │
               └─────────┬───────────┘            │ → SSetOperator 二叉树           │
                         │                        └───────────────┬────────────────┘
                         │                                        │
  Planner      │ 1 个 DynQueryCtrl    │            │ N 个 DynQueryCtrl              │
               │ + 父 VST schema      │            │ + SetOpProject (UNION ALL)     │
               └─────────┬───────────┘            └───────────────┬────────────────┘
                         │                                        │
  Splitter     │ virtualTableSplit    │            │ unionAllSplit (拆 N 分支)       │
               │ 1 次分裂            │            │ + 每分支内 virtualTableSplit     │
               └─────────┬───────────┘            └───────────────┬────────────────┘
                         │                                        │
  Executor     │ 1 个 DynQueryCtrl    │            │ N 个 DynQueryCtrl 算子          │
               │ 内部循环所有 VCT     │            │ + Exchange + Project(合并)      │
               │ 仅取父 schema 列     │            │ 各分支仅取父 schema 列          │
               └─────────────────────┘            └────────────────────────────────┘

  智能位置:     Executor (运行时)                    Planner (编译时)
```

---

## 4. UNION 改写具体实现（基于 TDengine 代码库）

### 4.1 Translator 改写入口（parTranslater.c）

当前 `translateVirtualTable()`（L6834-L6875）流程：
```c
// 现在做的：
pVTable->expandLevel = pRealTable->expandLevel;          // L6839
catalogGetVstDescendants(..., &pDescArr);                // L6856
// 将后代列表存入 pVTable->pExpandDescendants 传给 Planner
```

**UNION 改写需替换为：**

```c
// 新逻辑：不再传 pExpandDescendants 给 Planner，
// 而是在 Translator 阶段直接改写 AST
static int32_t rewriteExpandToUnionAll(STranslateContext* pCxt,
                                       SSelectStmt* pSelect,
                                       SVirtualTableNode* pVTable) {
  int32_t code = TSDB_CODE_SUCCESS;

  // 1. 获取后代 VST 列表
  SArray* pDescArr = NULL;
  int32_t maxLvl = (pVTable->expandLevel == -1) ? -1 : pVTable->expandLevel;
  code = catalogGetVstDescendants(pCatalog, &conn, dbFName, pName->tname, maxLvl, &pDescArr);
  if (code != TSDB_CODE_SUCCESS || !pDescArr || taosArrayGetSize(pDescArr) == 0) {
    return code;  // 无后代 → EXPAND 等于 no-op
  }

  // 2. 提取被查询 VST 的 schema 列（用于每个分支的 SELECT list）
  //    schema = parent_vst 自身列: [ts, val]
  SNodeList* pParentSchemaExprs = extractProjectExprsFromSchema(pVTable->pMeta);

  // 3. 构建 N+1 个分支的 SSelectStmt（自身 + N 个后代）
  SNodeList* pBranches = NULL;
  nodesMakeList(&pBranches);

  // 3a. 自身分支（parent_vst, expandLevel=0 即不再展开）
  SSelectStmt* pSelfBranch = buildBranchSelect(pCxt, pVTable->tableName,
                                                pParentSchemaExprs,
                                                pSelect->pWhere,
                                                /*expandLevel=*/0);
  nodesListAppend(pBranches, (SNode*)pSelfBranch);

  // 3b. 每个后代 VST 分支
  for (int32_t i = 0; i < taosArrayGetSize(pDescArr); i++) {
    char* descName = *(char**)taosArrayGet(pDescArr, i);
    // 需要 catalogGetTableMeta 获取后代 meta 来验证列存在性
    SSelectStmt* pDescBranch = buildBranchSelect(pCxt, descName,
                                                  pParentSchemaExprs,
                                                  pSelect->pWhere,
                                                  /*expandLevel=*/0);
    nodesListAppend(pBranches, (SNode*)pDescBranch);
  }

  // 4. 构建 SSetOperator 二叉树（UNION ALL）
  SNode* pUnionRoot = buildUnionAllBinaryTree(pBranches);

  // 5. 替换原始 pSelect 为子查询包装
  //    原始 SELECT ... FROM parent_vst EXPAND(-1) WHERE ... ORDER BY ... LIMIT ...
  //    → SELECT ... FROM (UNION ALL 子查询) ORDER BY ... LIMIT ...
  pSelect->pFromTable = createTempTableNode(pUnionRoot);
  pSelect->pWhere = NULL;  // WHERE 已下推到各分支

  return code;
}
```

### 4.2 分支 SELECT 构建细节

每个分支是一个完整的虚拟表查询，但 **expandLevel=0**（仅查自身 VCT）：

```c
// buildBranchSelect 为单个 VST 生成：
// SELECT ts, val FROM <vstName> [WHERE ...]
static SSelectStmt* buildBranchSelect(STranslateContext* pCxt,
                                       const char* vstName,
                                       SNodeList* pProjectExprs,
                                       SNode* pWhere,
                                       int32_t expandLevel) {
  SSelectStmt* pStmt = nodesMakeNode(QUERY_NODE_SELECT_STMT);

  // 1. FROM clause: 虚拟超级表（expandLevel=0，不再递归展开）
  SRealTableNode* pTable = createRealTableNode(vstName);
  pTable->expandLevel = expandLevel;
  pTable->hasExpand = (expandLevel != 0);
  pStmt->pFromTable = (STableNode*)pTable;

  // 2. SELECT list: 只取父 schema 列
  //    对每个列名在后代 VST 的 schema 中查找对应 colId
  pStmt->pProjectionList = nodesCloneList(pProjectExprs);

  // 3. WHERE: 深拷贝原始条件
  if (pWhere) {
    nodesCloneNode(pWhere, &pStmt->pWhere);
  }

  return pStmt;
}
```

**每个分支独立翻译**（translateQuery）→ 走现有虚拟表路径 → 生成各自的 DynQueryCtrl。

### 4.3 SSetOperator 二叉树构建

TDengine 的 `SSetOperator` 是二元结构（pLeft + pRight）。N 个分支需要递归嵌套：

```c
// 4 个分支的结果:
//   UNION(UNION(UNION(parent, child), grandchild), child2)
// 即左倾树，深度 = N-1
static SNode* buildUnionAllBinaryTree(SNodeList* pBranches) {
  SNode* pResult = nodesListGetNode(pBranches, 0);
  for (int i = 1; i < nodeListGetSize(pBranches); i++) {
    SSetOperator* pSet = nodesMakeNode(QUERY_NODE_SET_OPERATOR);
    pSet->opType = SET_OP_TYPE_UNION_ALL;
    pSet->pLeft = pResult;
    pSet->pRight = nodesListGetNode(pBranches, i);
    pResult = (SNode*)pSet;
  }
  return pResult;
}
```

**深度问题**：
- 4 个子孙 → 3 层嵌套
- 10 个子孙 → 9 层嵌套
- FS 允许 10 级深层继承 → 可能 100+ 个后代 → 99 层嵌套

### 4.4 Logic Plan（planLogicCreater.c）

UNION 改写后直接复用现有路径：

```
createSetOperatorLogicNode() [L5111-L5126]
  └── createSetOpLogicNode() [L5071-L5109]
        ├── SET_OP_TYPE_UNION_ALL → createSetOpProjectLogicNode()
        ├── createQueryLogicNode(pLeft)  → 虚拟表查询子树
        └── createQueryLogicNode(pRight) → UNION 子树（递归）
```

**无需修改**：现有 UNION ALL 逻辑计划创建完全适用。

每个分支内部的虚拟表查询仍走 `createDynQueryCtrlLogicNode()`，但 expandLevel=0（不展开），只查自身 VCT。

### 4.5 Splitter（planSpliter.c）

利用现有 `unionAllSplit()`（L1911）：

```
splitLogicPlan
  └── unionAllSplit()     ← 检测到 SetOpProject 节点
        ├── 分支 1 subplan → virtualTableSplit (parent_vst)
        ├── 分支 2 subplan → virtualTableSplit (child_vst)
        ├── 分支 3 subplan → virtualTableSplit (grandchild_vst)
        └── 分支 4 subplan → virtualTableSplit (child_vst2)
```

**无需额外代码**：现有 unionAllSplit 已经能递归处理。

### 4.6 Executor 阶段改动

**删除 DynQueryCtrl 中的 EXPAND 逻辑**：

```c
// 需要删除的代码（dynqueryctrloperator.c）：
// 1. pExpandDescendantStbs hash 构建 (L4281-L4296)
// 2. tableInfoNeedCollectForExpand() 函数 (L3624-L3639)
// 3. needExpand 条件分支 (L4321-L4322)
// 4. pExpandDescendantStbs 清理 (L287-L289)
// 5. expandLevel 相关字段初始化 (L5465-L5466)
```

**依赖现有设施**：
- SetOperator executor 合并各分支结果
- Exchange 算子处理跨节点传输
- 每个分支的 DynQueryCtrl 只负责自身 VCT

### 4.7 PRIVATE 列处理

**PRIVATE 列 = VCT 的私有列**，不属于任何 VST 的 schema。

```
在 UNION ALL 方案中，每个分支只 SELECT 父 VST schema 的列：
  SELECT ts, val FROM child_vst   -- 只投影 parent_vst 的 (ts, val)

child_vst 的 VCT 可能有私有列 (secret_col)：
  vct_c1: 有 secret_col → 但不在投影列中 → 不出现在结果中
  vct_c2: 无私有列 → 不影响
```

**结论**：UNION ALL 改写的 schema 投影**天然排除** VCT 私有列，无需任何运行时判断。这是 UNION ALL 方案相对于方案 A 的一个简化优势。

### 4.8 完整代码改动清单（已实施）

| 文件 | 改动类型 | 实际改动 |
|------|----------|----------|
| `source/libs/parser/src/parTranslater.c` | 新增 `rewriteVstExpandToUnionAll()` + 辅助函数 | +~120 行 |
| `source/libs/parser/src/parTranslater.c` | 删除 expandFromAncestor 传递逻辑 | -20 行 |
| `source/libs/planner/src/planLogicCreater.c` | 删除 expandFromAncestor 传递 | -2 行 |
| `source/libs/planner/src/planPhysiCreater.c` | 删除 expandFromAncestor 传递 | -2 行 |
| `source/libs/executor/src/dynqueryctrloperator.c` | 删除 expandFromAncestor 初始化 | -1 行 |
| `source/libs/executor/inc/dynqueryctrl.h` | 删除 expandFromAncestor 字段 | -1 行 |
| `source/libs/nodes/src/nodesCloneFuncs.c` | 删除 expandFromAncestor clone | -4 行 |
| `source/libs/nodes/src/nodesCodeFuncs.c` | 删除 expandFromAncestor 序列化 | -11 行 |
| `include/libs/nodes/plannodes.h` | 删除 expandFromAncestor 字段 | -4 行 |
| `include/libs/nodes/querynodes.h` | 删除 expandFromAncestor 字段 | -2 行 |

> 注：PRIVATE 由 schema 投影天然排除，无需 expandFromAncestor 运行时判断。

---

## 5. FS 行为逐项对比实现

### 5.1 基础查询（FS §3.4 示例 1）

```sql
SELECT * FROM parent_vst EXPAND(-1) ORDER BY ts;
-- 期望：16 行, schema=(ts, val)
```

| 步骤 | 方案 A (EXPAND) | 方案 B (UNION) |
|------|----------------|----------------|
| 计划结构 | 1 个 DynQueryCtrl | 4 个 DynQueryCtrl + UNION ALL |
| VCT 发现 | 1 次 sys scan 全量 + hash 过滤 | 4 次 sys scan（每分支各自） |
| Schema 确定 | Translator 传入 parent_vst meta | 每个分支 SELECT ts, val |
| PRIVATE 列 | schema 投影排除 | schema 投影天然排除 |
| ORDER BY | 单一结果集排序 | UNION ALL 结果再排序（需外层 Sort） |
| 结果相同？ | ✅ | ✅ |

### 5.2 分层 EXPAND（FS §3.2）

```sql
SELECT * FROM parent_vst EXPAND(1);
-- 期望：14 行（不含 grandchild_vst 的 vct_g1）
```

| 方案 | 实现 |
|------|------|
| A | `expandLevel=1` → catalogGetVstDescendants(maxLvl=1) → 只返回 [child_vst, child_vst2] |
| B | 同样调 catalogGetVstDescendants(maxLvl=1) → 只生成 3 个分支（parent + child + child2） |

两种方案层级控制实现难度相同。

### 5.3 WHERE 过滤（FS §3.4 示例 4）

```sql
SELECT val FROM parent_vst EXPAND(-1) WHERE val > 35 ORDER BY val;
-- 期望：40, 41, 50, 50, 51, 51, 60, 61 (8 行)
```

| 方案 | WHERE 处理 |
|------|------------|
| A | DynQueryCtrl 内部对每个 VCT 查源表时下推 WHERE val > 35 |
| B | 每个分支独立 WHERE val > 35（WHERE 在外层 SELECT 上，各分支自动继承） |

**差异**：方案 B 需要在 Translator 阶段深拷贝 WHERE 到每个分支。由于 schema 以父 VST 为准，所有分支都有 val 列（继承列），WHERE 可以直接复制，无列名冲突。

### 5.4 聚合查询（FS §3.4 示例 5）

```sql
SELECT COUNT(*), SUM(val), AVG(val) FROM parent_vst EXPAND(-1);
-- 期望：16, 457, 32.64
```

| 方案 | 实现方式 |
|------|----------|
| A | DynQueryCtrl 收集全部 16 行 → 单一聚合算子 | 
| B | **问题出现** → 见下 |

方案 B 的聚合处理：

**选择 1：聚合在外层**
```sql
SELECT COUNT(*), SUM(val), AVG(val) FROM (
  SELECT ts, val FROM parent_vst
  UNION ALL SELECT ts, val FROM child_vst
  UNION ALL SELECT ts, val FROM grandchild_vst
  UNION ALL SELECT ts, val FROM child_vst2
);
-- ✅ 语义正确：外层对 UNION 子查询做聚合
-- ⚠️ 需要 Translator 将聚合上提到外层，内部分支只做投影
```

**选择 2：聚合下推到分支**
```sql
SELECT SUM(branch_count), SUM(branch_sum), SUM(branch_sum)/SUM(branch_count)
FROM (
  SELECT COUNT(*) AS branch_count, SUM(val) AS branch_sum FROM parent_vst
  UNION ALL SELECT COUNT(*), SUM(val) FROM child_vst
  UNION ALL ...
);
-- ⚠️ 对 COUNT/SUM 可行（可加聚合）
-- ❌ 对 AVG 需拆分为 SUM/COUNT 再合并
-- ❌ 对 PERCENTILE/APERCENTILE 完全不可拆分
```

**方案 B 必须选择 1**：将聚合保留在外层，内部 UNION ALL 只返回原始行。

### 5.5 INTERVAL 窗口查询（FS §3.4 示例 8）⚠️ 关键差异

```sql
SELECT _wstart, COUNT(*), SUM(val) FROM parent_vst EXPAND(-1) INTERVAL(5s);
```

**方案 A**：DynQueryCtrl 收集全部行 → INTERVAL 算子在统一时间线上切窗口 → ✅ 直接支持

**方案 B**：

```sql
-- 选择 1：INTERVAL 在外层
SELECT _wstart, COUNT(*), SUM(val) FROM (
  SELECT ts, val FROM parent_vst
  UNION ALL SELECT ts, val FROM child_vst
  UNION ALL ...
) INTERVAL(5s);
-- ❌ TDengine 不支持对 UNION 子查询使用 INTERVAL
-- 原因：INTERVAL 需要单一表源的时间线，子查询不满足
```

```sql
-- 选择 2：INTERVAL 下推到每个分支
SELECT _wstart, COUNT(*), SUM(val) FROM parent_vst INTERVAL(5s)
UNION ALL
SELECT _wstart, COUNT(*), SUM(val) FROM child_vst INTERVAL(5s)
UNION ALL ...
-- ❌ 语义错误：不同分支的相同时间窗口产生多行
-- 例：[00:00:00, 00:00:05) parent分支=62, child分支=122
-- 应该合并为 184，但 UNION ALL 输出两行
```

```sql
-- 选择 3：下推 + 外层再聚合
SELECT _wstart, SUM(cnt), SUM(s) FROM (
  SELECT _wstart, COUNT(*) AS cnt, SUM(val) AS s FROM parent_vst INTERVAL(5s)
  UNION ALL
  SELECT _wstart, COUNT(*), SUM(val) FROM child_vst INTERVAL(5s)
  UNION ALL ...
) GROUP BY _wstart;
-- ⚠️ 语义近似正确，但：
-- 1. _wend, _wduration 等窗口伪列丢失
-- 2. 需要在 Translator 中做复杂的聚合拆分/合并改写
-- 3. 对不可拆分聚合（PERCENTILE 等）无解
-- 4. SLIDING 语义更复杂（滑动窗口对齐问题）
-- 5. 不再是真正的 INTERVAL 查询 → 失去流式计算兼容性
```

**结论：INTERVAL 在方案 B 中没有完美解决方案。**

### 5.6 PARTITION BY + INTERVAL（FS §3.4 示例 8.3）

```sql
SELECT tbname, _wstart, COUNT(*) FROM parent_vst EXPAND(-1)
    PARTITION BY tbname INTERVAL(10s);
```

**方案 A**：每个 VCT 独立 partition → INTERVAL 切窗 → ✅ 直接支持

**方案 B**：
```sql
-- PARTITION BY 在 UNION 子查询上的行为未定义
-- 需要将 PARTITION BY 同时下推到每个分支 + 外层合并
-- 如果不同 VCT 有相同 tbname（不可能但设计上需考虑）→ 行为不确定
```

### 5.7 Stream 流计算（FS §3.4 示例 15）❌ 致命不兼容

```sql
CREATE STREAM expand_stream INTO expand_result AS
SELECT _wstart, COUNT(*), SUM(val) FROM parent_vst EXPAND(-1) INTERVAL(10s);
```

**方案 A**：DynQueryCtrl 注册为流计算源 → 监听所有子孙 VCT 的源表变更 → ✅

**方案 B**：
```
TDengine Stream 的硬约束：
  1. FROM 必须是单一 SRealTableNode 或 SVirtualTableNode
  2. 不允许 FROM 子查询中包含 UNION（parTranslater.c L12210 检查）
  3. INTERVAL 不支持 UNION 子查询

改写后等价：
  CREATE STREAM ... AS SELECT ... FROM (... UNION ALL ...) INTERVAL(10s);
  → ❌ 语法错误 + 语义不支持
```

**无解决方案**，除非彻底改造 Stream 引擎：
- 涉及 `tqSink`, `tqPush`, `streamDispatch`, `streamState` 等模块
- 需要支持多源表订阅 + 跨源合并
- 工程量 >> EXPAND 方案本身

### 5.8 LAST/FIRST（FS §3.4 示例 13）

```sql
SELECT LAST(val) FROM parent_vst EXPAND(-1);
-- 期望：61（所有 VCT 中最新的非 NULL val）
```

| 方案 | 实现 |
|------|------|
| A | DynQueryCtrl 扫描全部 VCT → LAST 函数在全局时间线找最新非 NULL 值 → ✅ |
| B | 每分支 LAST(val) → UNION ALL → 外层再取 MAX → ⚠️ 需要改写为 `SELECT MAX(branch_last) FROM (SELECT LAST(val) AS branch_last FROM parent_vst UNION ALL ...)` → 非标改写 |

### 5.9 ORDER BY + LIMIT（FS §3.4 示例 7）

```sql
SELECT val FROM parent_vst EXPAND(-1) ORDER BY val DESC LIMIT 3;
-- 期望：61, 60, 51
```

| 方案 | 实现 |
|------|------|
| A | 单一结果集 → Sort → Limit → ✅ |
| B | UNION ALL 结果无序 → 需外层 Sort + Limit → ✅ 可行但多一层算子 |

方案 B 可以将 ORDER BY + LIMIT 保留在外层 SELECT（不下推到各分支）。

但 **LIMIT 不能下推**：
```sql
-- ❌ 错误做法：每分支各取 LIMIT 3 再合并
SELECT val FROM parent_vst ORDER BY val DESC LIMIT 3
UNION ALL
SELECT val FROM child_vst ORDER BY val DESC LIMIT 3
-- 结果 > 3 行，需要外层再 LIMIT → 效率低
```

### 5.10 GROUP BY tbname（FS §3.4 示例 6）

```sql
SELECT tbname, COUNT(*), MAX(val) FROM parent_vst EXPAND(-1) GROUP BY tbname;
```

| 方案 | 实现 |
|------|------|
| A | 全局分组 → ✅ |
| B | 每分支 GROUP BY tbname → UNION ALL → 外层无需再聚合（因为 tbname 在各分支内唯一） → ✅ 可行 |

GROUP BY 是少数方案 B 可以下推的场景。

---

## 6. 改成 UNION 会碰到的具体问题汇总

### 6.1 ⚠️ N 倍 Catalog RPC

```
方案 A：
  catalogGetVstDescendants()         → 1 次 RPC
  sys scan ins_vc_cols (全量)        → 1 次 scan
  总计: ~2 次

方案 B：
  catalogGetVstDescendants()         → 1 次 RPC
  catalogGetTableMeta(每个后代)      → N 次 RPC（构建各分支 meta）
  catalogGetTableVgroupList(每个)    → N 次 RPC
  sys scan per VST                   → N 次 scan
  每个 VST 的 refTables 处理          → N 次
  总计: ~4N+1 次 RPC
```

### 6.2 ⚠️ 二叉树嵌套深度

```
FS 允许 10 级深度继承，假设每级 3 个子孙：
  level 1: 3 VSTs
  level 2: 9 VSTs
  ...
  level 10: 59049 VSTs (理论极端)

实际案例：50 个后代 VST → SSetOperator 嵌套 49 层
→ 递归翻译/计划创建可能栈溢出
→ plan 序列化/反序列化耗时 O(N²)
```

### 6.3 ⚠️ 计划膨胀

```
每个分支 ≈ 5 个节点（DynQueryCtrl + VirtualScan + RealTableScan + SysTableScan + TagScan）

子孙数 N | 方案 A 节点数 | 方案 B 节点数
---------|-------------|-------------
  1      |     ~5      |     ~11 (2×5 + 1×SetOp)
  4      |     ~5      |     ~29 (5×5 + 4×SetOp)
 10      |     ~5      |     ~64 (11×5 + 10×SetOp)
 50      |     ~5      |    ~314 (51×5 + 50×SetOp)
```

### 6.4 ⚠️ 计划缓存失效

| 事件 | 方案 A | 方案 B |
|------|--------|--------|
| 新增子孙 VST | 更新 descendant 列表，计划结构不变 | 计划结构改变（新增分支），缓存失效 |
| 新增 VCT | 无影响 | 无影响 |
| ALTER 子 VST ADD COLUMN | 无影响（父 schema 不变） | 无影响 |
| DROP 子 VST | 更新 descendant 列表 | 计划结构改变，缓存失效 |
| ALTER 继承关系 | 更新 descendant 列表 | 计划完全重建 |

### 6.5 ~~PRIVATE 列的复杂交互~~ （已简化）

在新的 PRIVATE 语义下（VCT 私有列不属于 VST schema），PRIVATE 列不会出现在投影列表中：

```sql
SELECT val FROM parent_vst EXPAND(-1) WHERE val IS NULL;
-- val 是 parent_vst schema 的列
-- 所有后代 VST 的 VCT 都有 val（非私有），正常参与查询
-- VCT 的私有列（如 secret_col）不在投影中，完全不参与
```

**结论**：UNION ALL + schema 投影天然解决了 PRIVATE 问题，无需运行时 NULL 替换。这是 UNION ALL 方案的一个**优势**。

---

## 7. UNION 方案的优势

### 7.1 ✅ 天然并行执行

```
方案 A (串行)：DynQueryCtrl 内部 for 循环逐 VCT 处理
  VCT_1 → scan → output → VCT_2 → scan → output → ...

方案 B (并行)：unionAllSplit 后各分支成独立 subplan
  分支_1(parent) ──→ scan ──┐
  分支_2(child)  ──→ scan ──┼→ Project(合并)
  分支_3(grand)  ──→ scan ──┘
  各分支无依赖，可在不同 qnode 并发
```

### 7.2 ✅ 分支级别剪枝

```sql
SELECT * FROM parent_vst EXPAND(-1) WHERE val > 100;
```
- 优化器可以为每个分支做统计信息判断
- 如果某个 VST 下没有 VCT → 该分支在编译时直接消除
- 方案 A 需要运行时 sys scan 后才能发现空 VST

### 7.3 ✅ 调试直观 + EXPLAIN 友好

```sql
EXPLAIN SELECT * FROM parent_vst EXPAND(-1);

-- 方案 A:
-- DynQueryCtrl (expandLevel=-1, descendants=[child,grand,child2])
--   VirtualScan (parent_vst)
--     ...

-- 方案 B:
-- Project
--   SetOperator (UNION ALL)
--     DynQueryCtrl (parent_vst)
--     DynQueryCtrl (child_vst)
--     DynQueryCtrl (grandchild_vst)
--     DynQueryCtrl (child_vst2)
-- 每个分支清晰可见，便于定位性能瓶颈
```

### 7.4 ✅ 代码复用现有设施

- 复用 `translateSetOperator()`（L12201）
- 复用 `createSetOpLogicNode()`（L5071）
- 复用 `unionAllSplit()`（L1911）
- 复用 SetOperator executor
- 不需要在 DynQueryCtrl 中维护 EXPAND 特殊路径

---

## 8. 执行效率对比

### 8.1 资源消耗

| 维度 | 方案 A (EXPAND) | 方案 B (UNION) |
|------|----------------|----------------|
| Sys Scan 次数 | 1 次（全量） | N 次（每 VST 一次） |
| Catalog RPC | ~2 次 | ~4N+1 次 |
| 计划序列化大小 | O(1) | O(N) |
| 内存峰值 | ~1 份 output buffer | ~N 份 buffer |
| 数据拷贝 | VCT→输出 1 次 | VCT→分支→UNION→输出 2 次 |
| 编译耗时 | O(1) | O(N) (每分支独立翻译) |

### 8.2 延迟对比（估算）

```
场景：4 个子孙 VST，每个 VST 有 50 个 VCT，每个 VCT 源表查询 10ms

方案 A (串行)：
  编译:     ~5ms
  Sys scan: ~20ms (1 次全量)
  数据扫描: 200 VCT × 10ms = 2000ms (串行)
  总延迟:   ~2025ms

方案 B (4 分支并行)：
  编译:     ~50ms (4N+1 次 RPC)
  Sys scan: ~20ms × 4 = 并行后 ~20ms
  数据扫描: 每分支 50 × 10ms = 500ms (4 分支并行 → wall time 500ms)
  总延迟:   ~570ms

加速比:   ~3.5x (理想情况)
```

但实际受限于：
- 编译时 catalog RPC 串行
- 可用 qnode 数量有限
- 内存压力（N 份 buffer 同时存在）
- UNION ALL 后排序开销

### 8.3 方案 A 并行化改进

方案 A 的串行执行瓶颈可以通过 **DynQueryCtrl 内部多线程** 解决：
```
DynQueryCtrl (expandLevel=-1)
  ├── Thread Pool (size=min(numVCT, maxWorkers))
  │     ├── Worker 1: VCT_1 → scan → queue
  │     ├── Worker 2: VCT_2 → scan → queue
  │     └── Worker N: VCT_N → scan → queue
  └── Merge thread: dequeue → output
```

改动量约 200 行，不改变计划结构，保留所有优势。

---

## 9. 功能兼容性总表（基于 FS 规格）

| FS 功能要求 | 方案 A (EXPAND) | 方案 B (UNION) | 说明 |
|------------|:---:|:---:|------|
| EXPAND(-1) 全部子孙 | ✅ | ✅ | |
| EXPAND(N) 层级控制 | ✅ | ✅ | |
| Schema = 被查询 VST (§3.3) | ✅ | ✅ | |
| PRIVATE VCT 私有列排除 (§3.3) | ✅ | ✅ schema 投影天然排除 | §4.7 |
| Tag 以被查询 VST 为准 (§3.3) | ✅ | ✅ | |
| WHERE 过滤 | ✅ | ✅ (需深拷贝 WHERE) | §5.3 |
| 聚合 COUNT/SUM/AVG | ✅ 直接 | ⚠️ 需外层聚合 | §5.4 |
| INTERVAL 窗口 | ✅ 直接 | ❌ 不支持 | §5.5 致命 |
| INTERVAL + SLIDING | ✅ 直接 | ❌ 不支持 | §5.5 |
| PARTITION BY + INTERVAL | ✅ 直接 | ❌ 不支持 | §5.6 |
| SESSION/STATE_WINDOW | ✅ 直接 | ❌ 不支持 | |
| ORDER BY + LIMIT | ✅ 直接 | ⚠️ 外层排序 | §5.9 |
| GROUP BY | ✅ 直接 | ⚠️ 可下推但需验证 | §5.10 |
| LAST/FIRST | ✅ 直接 | ⚠️ 需改写合并 | §5.8 |
| DISTINCT | ✅ 直接 | ✅ UNION ALL + 外层 DISTINCT | |
| JOIN 其他表 | ✅ 直接 | ⚠️ 需子查询包装 | |
| 子查询嵌套 | ✅ 直接 | ✅ | |
| Stream (§3.4 示例15) | ✅ | ❌ 致命 | §5.7 |
| Prepared Stmt 缓存 | ✅ 稳定 | ⚠️ 继承树变→失效 | §6.4 |
| 多分支继承 (A←B, A←C) | ✅ | ✅ | |
| 10 级深层继承 (§6) | ✅ O(1) | ⚠️ O(N) 膨胀 | §6.2/6.3 |

---

## 10. 灵活性对比

### 从用户角度

| 能力 | 方案 A | 方案 B |
|------|--------|--------|
| 一条 SQL 搞定全部 | ✅ | ✅ (内部透明) |
| 自动发现新增子孙 | ✅ | ✅ |
| 直接 INTERVAL/窗口 | ✅ | ❌ |
| 直接聚合 | ✅ | ⚠️ 仅简单聚合 |
| 直接 JOIN | ✅ | ❌ (需嵌套) |
| Stream 支持 | ✅ | ❌ |
| EXPLAIN 可读性 | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

### 从引擎开发者角度

| 能力 | 方案 A | 方案 B |
|------|--------|--------|
| 并行执行 | 需加多线程 (~200行) | ✅ 天然 |
| 分支剪枝 | 运行时 | ✅ 编译时 |
| PRIVATE 实现 | ✅ schema 投影排除 | ✅ schema 投影排除（天然） |
| 代码量 | 已实现 ~150 行 | 需 ~500 行改写 + 无法解决 Stream/INTERVAL |
| 复用现有设施 | 少（专用逻辑） | ✅ 多（UNION + Exchange） |
| 计划缓存 | ✅ 稳定 | ⚠️ 易失效 |
| 新增 VST 时的影响 | 无（运行时动态） | 缓存失效需重编译 |

---

## 11. 总结

### 评分表

| 维度 | 方案 A (EXPAND) | 方案 B (UNION) |
|------|:---:|:---:|
| FS 功能完整性 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| 并行执行能力 | ⭐⭐⭐ (可改进到⭐⭐⭐⭐) | ⭐⭐⭐⭐⭐ |
| 元数据效率 | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| 计划紧凑性 | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| PRIVATE 列支持 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 深层继承 (10级) | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| 代码简洁/复用 | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| EXPLAIN 直观性 | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 计划缓存稳定 | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| Stream/INTERVAL | ⭐⭐⭐⭐⭐ | ⭐ (致命缺陷) |

### 结论

**方案 A（EXPAND）是唯一满足 FS 全部规格的方案**：

1. **Stream 致命** → UNION 完全无法支持（§5.7），TDengine Stream 架构不支持多源 UNION
2. **INTERVAL 致命** → 无法对 UNION 子查询做窗口计算（§5.5），任何 workaround 都有语义损失
3. **PRIVATE VCT 私有列** → 两种方案都通过 schema 投影天然排除，无运行时开销
4. **元数据效率** → 4N+1 次 RPC 在深层继承时不可接受（§6.1）
5. **并行可后补** → DynQueryCtrl 内部加线程池即可达到方案 B 的并行度（§8.3）
6. **实现代价** → 方案 A 已完成且仅 ~150 行，方案 B 需 ~500 行且仍无法解决致命问题

### 一句话

> **EXPAND 在运行时做智能调度（代价：串行，可通过线程池补齐）；UNION 在编译时做静态展开（代价：丧失窗口/流计算，无法修复）。对时序数据库，窗口计算和流计算是核心能力，不可放弃，因此 EXPAND 是正确且唯一的选择。**
