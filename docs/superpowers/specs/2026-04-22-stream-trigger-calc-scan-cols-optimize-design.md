# 流计算触发/计算 AST 扫描列优化 - 详细设计说明书（Design Spec）

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-22 | 2026-04-22 | 0.1 | brainstorming session | 初稿 |

## 2. 引言

### 2.1 目的

优化 `CREATE STREAM` 语句在 client 端解析阶段生成 plan 时，触发 AST 与计算 AST 的扫描列集合，使两者各自只扫描其逻辑上必需的列；并保证使用 `%%trows` 时计算端独立扫描出的数据与触发端完全一致。

### 2.2 范围

仅涉及 client 侧 parser/planner：

- `source/libs/parser/src/parTranslater.c`
- `source/libs/planner/src/planLogicCreater.c`
- `include/libs/planner/planner.h`（若 `SPlanContext.streamCxt.triggerScanList` 字段定义在此）

不涉及 mnode、snode、vnode 执行链路；不修改任何 RPC 协议字段。

### 2.3 受众

流计算模块开发者、CR reviewer。

## 3. 术语

- **触发 AST（trigger AST）**：流计算建流时生成、用于驱动 trigger 触发判断的 SELECT 语句 AST，由 `createStreamReqBuildTriggerSelect` 构造。
- **计算 AST（calc AST）**：用户在 `CREATE STREAM ... AS <query>` 中提供的 query AST，用于每次触发后实际计算输出。
- **`%%trows`**：占位符，语义为"使用与本次触发相同的数据"。在 `translatePlaceHolderTable` 中被改写为对触发表的 `SRealTableNode`，并保留 `placeholderType = SP_PARTITION_ROWS`。**注意：触发数据并不通过流框架传递给计算，计算端是独立再扫一次触发表。**
- **pre_filter**：`STREAM_OPTIONS(PRE_FILTER(<expr>))` 中的过滤表达式，作用于触发侧扫描的数据，决定哪些行参与触发判断。
- **trigger 扫描列 / calc 扫描列**：plan 阶段 `SScanLogicNode.pScanCols` 最终落到 scan 节点要从存储层读取的列集合。

## 4. 概述

### 4.1 当前实现存在的问题

示例 SQL：

```sql
CREATE STREAM s1
  STATE_WINDOW(c1) FROM stb
  PARTITION BY t1
  STREAM_OPTIONS(PRE_FILTER(c2 > 2))
  INTO res_stb
  AS SELECT _c0, sum(c3), avg(t2) FROM %%trows;
```

| AST | 当前扫描列 | 期望扫描列 | 问题 |
|---|---|---|---|
| 触发 AST | ts, c1, c2, c3, t1, t2 | ts, c1, c2 | 多扫了计算端引用的 c3、t2 |
| 计算 AST | ts, c3, t2 | ts, c2, c3, t2 | 缺少 pre_filter 引用列 c2，且未应用 `c2 > 2` 过滤 |

根因：

1. **触发侧多列**：`parTranslater.c:19205-19216` 在 `PLACE_HOLDER_PARTITION_ROWS` 时把计算 plan 阶段收集的 `triggerScanList`（来自 `planLogicCreater.c:600`，使用 `COLLECT_COL_TYPE_ALL` 收集计算 SELECT 全部列）追加进触发 select 的 `pProjectionList`。这是一种基于"计算用什么、触发就也带上"的错误假设；实际上触发只关心 trigger window / partition / pre_filter 引用的列。

2. **计算侧缺过滤**：`%%trows` 的语义要求计算端扫描结果与触发一致，但当前实现不会把 pre_filter 注入计算 select，导致 calc 端取到 pre_filter 之外的额外行，且不扫 pre_filter 引用的列。

### 4.2 优化目标

- 触发 AST 扫描列 = 时间列（plan 自动加） + trigger window 引用列 + partition 引用列 + pre_filter 引用列 + tbname。
- 计算 AST 扫描列 = SELECT/原 WHERE 引用列 + （`%%trows` 时）pre_filter 引用列；同时 pre_filter 作为 WHERE 应用，使两端取到同样的行。

### 4.3 涉及代码

- `source/libs/parser/src/parTranslater.c`：
  - `createStreamReqBuildCalc`（19104）
  - `translateWhere`（10522）
- `source/libs/planner/src/planLogicCreater.c`：
  - `createScanLogicNodeByRealTable`（598-601）
- `SPlanContext.streamCxt.triggerScanList` 定义点（`include/libs/planner/planner.h` 或同一文件 struct 定义处）

## 5. 设计考虑

### 5.1 假设和限制

- pre_filter 表达式中的列引用与触发表 schema 一致；`%%trows` 改写后的 `SRealTableNode` 与触发表为同一张表，列名空间相同，`translateExpr` 对 clone 后的 pre_filter 能正常解析。
- 现有限制保留：`parTranslater.c:19144-19152` 已经禁止"虚拟表 + `%%trows` + pre_filter"组合，本设计不改变此行为。
- 用户在 `%%trows` 计算 select 中显式写 WHERE 仍然报错（沿用 `translateWhere` 现有错误信息 `%%trows can not be used with WHERE clause.`）。

### 5.2 设计原则

- **语义优先于实现取巧**：把 pre_filter 当作计算 select 的 WHERE 注入，而不是在 plan/reader 层"额外补列、额外过滤"。这样后续 translate / planner / scan-condition 下推 / 索引下推全部沿用标准路径。
- **改动局部化**：所有改动集中在 parser + planner 两个文件；不引入新模块，不修改协议。
- **死代码清理**：方案落地后 `triggerScanList` 不再有读端，应一并删除字段及其填充点，避免后续维护困惑。

### 5.3 风险与缓解

| 风险 | 缓解 |
|---|---|
| pre_filter clone 后 translate 失败（语义检查、列解析等） | 注入时机选择在 `translateStreamCalcQuery` 之前，确保走完整 translate；先通过测试用例覆盖典型 pre_filter 形式（含 tag 引用、函数调用、常量折叠）。 |
| union（`SSetOperator`）中 `%%trows` 出现 | 注入逻辑递归处理 SET_OPERATOR 的左右子，并复用各自一份 clone。 |
| `pSelect->pWhere` 由用户写入（与注入冲突） | 注入前显式检查 `pWhere == NULL`，非空时报与原 `translateWhere` 一致的错误，提前于 translate 报出。 |
| `triggerScanList` 字段被其他模块隐式读取 | grep 全仓库确认无第三方读端后再删除字段。 |
| 兼容性：旧版 stream 持久化 plan 不变 | 仅影响新建 stream 时 client 生成的 plan 内容，不影响 mnode/snode 已存 plan 反序列化路径。 |

## 6. 详细设计

### 6.1 触发侧改动

**核心**：删除"计算列追加进触发投影"的代码块。

**位置**：`source/libs/parser/src/parTranslater.c:19205-19216`

```c
// REMOVE:
if (BIT_FLAG_TEST_MASK(pReq->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) &&
    LIST_LENGTH(calcCxt.streamCxt.triggerScanList) > 0) {
  PAR_ERR_JRET(nodesListAppendList(pTriggerSelect->pProjectionList,
                                   calcCxt.streamCxt.triggerScanList));
  SNode* pCol = NULL;
  FOREACH(pCol, pTriggerSelect->pProjectionList) {
    if (nodeType(pCol) == QUERY_NODE_COLUMN) {
      SColumnNode* pColumn = (SColumnNode*)pCol;
      tstrncpy(pColumn->tableAlias, pColumn->tableName, TSDB_TABLE_NAME_LEN);
    }
  }
}
```

删除后触发投影列由现有 `createStreamReqBuildTriggerSelect`（18241）维持：

- `nodesCollectColumnsFromNode(pStmt->pTrigger, NULL, COLLECT_COL_TYPE_COL, ...)` —— 收集 trigger 节点中出现的所有 COL 类型列（trigger window 与 pre_filter 中的普通列）；
- 若有 pre_filter，单独收集其 TAG 列；
- 追加 `tbname()` 函数。

partition 列（如 `t1`）由 `createStreamReqBuildTriggerPlan`（17935）通过 `createStreamSetListSlotId` 单独收集到 `pPartitionCols`，并序列化进 `pReq->partitionCols`，不需要混入 trigger select 的 projection。

### 6.2 计算侧改动

#### 6.2.1 注入 pre_filter 到计算 query

新增 helper（建议位于 `parTranslater.c` 中 `createStreamReqBuildCalc` 上方，模块私有 static）：

```c
// Inject trigger's pre_filter as WHERE into calc query when %%trows is used.
// This guarantees calc side independent re-scan returns the exact same rows
// as trigger side (since %%trows means "same data as trigger").
static int32_t injectPreFilterIntoCalcQuery(STranslateContext* pCxt,
                                            SCreateStreamStmt* pStmt);
```

行为：

1. 取 `pPreFilter = pStmt->pTrigger->pOptions ? pStmt->pTrigger->pOptions->pPreFilter : NULL`。若为 NULL，直接返回成功。
2. 递归遍历 `pStmt->pQuery`：
   - `QUERY_NODE_SELECT_STMT`：
     - 若 `pSelect->pFromTable` 不是 `QUERY_NODE_PLACE_HOLDER_TABLE` 或 `placeholderType != SP_PARTITION_ROWS` → 跳过（非 `%%trows` 不注入）。
     - 否则，若 `pSelect->pWhere != NULL` → 报错 `%%trows can not be used with WHERE clause.`（沿用现有错误码 `TSDB_CODE_PAR_INVALID_STREAM_QUERY`）。
     - 否则，`nodesCloneNode(pPreFilter, &pSelect->pWhere)`；置 `pSelect->pWhereInjectedFromPreFilter = true`。
   - `QUERY_NODE_SET_OPERATOR`：递归 left 和 right。
   - 其他类型：直接返回（沿用 `translateStreamCalcQuery` 内对类型校验的处理；不在注入阶段重复报错）。

调用时机：在 `createStreamReqBuildCalc`（19104）中，`translateStreamCalcQuery`（19140）调用之前插入：

```c
PAR_ERR_JRET(injectPreFilterIntoCalcQuery(pCxt, pStmt));
PAR_ERR_JRET(translateStreamCalcQuery(pCxt, pTriggerPartition, ...));
```

#### 6.2.2 调整 `translateWhere`

`parTranslater.c:10522-10529` 当前实现：

```c
if (pSelect->pWhere &&
    BIT_FLAG_TEST_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) &&
    inStreamCalcClause(pCxt)) {
  PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                      "%%%%trows can not be used with WHERE clause."));
}
```

修改为：

```c
if (pSelect->pWhere && !pSelect->pWhereInjectedFromPreFilter &&
    BIT_FLAG_TEST_MASK(pCxt->streamInfo.placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS) &&
    inStreamCalcClause(pCxt)) {
  PAR_ERR_RET(generateSyntaxErrMsgExt(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_STREAM_QUERY,
                                      "%%%%trows can not be used with WHERE clause."));
}
```

#### 6.2.3 `SSelectStmt` 字段扩展

在 `include/nodes/querynodes.h` 的 `SSelectStmt` 结构里新增：

```c
bool pWhereInjectedFromPreFilter;  // true if pWhere was injected by stream pre_filter
```

仅用于 §6.2.2 的检查旁路。其他模块（克隆 / 序列化）按现有 bool 字段处理方式同步即可（如 `nodesCloneNode` 默认按字段拷贝；序列化暂不需要写入到下发协议）。

### 6.3 死代码清理

| 文件 | 行 | 操作 |
|---|---|---|
| `parTranslater.c` | 19185 | 删除 `.streamCxt.triggerScanList = NULL` 初始化 |
| `parTranslater.c` | 19205-19216 | 删除追加块（同 §6.1） |
| `planLogicCreater.c` | 598-601 | 删除 `if (placeholderType == SP_PARTITION_ROWS) ... triggerScanList ...` 收集 |
| `SPlanContext.streamCxt` | 字段定义 | 删除 `triggerScanList` 字段及关联清理代码 |

清理前 grep 全仓库确认无其他读端：

```bash
rg "triggerScanList" --type c
```

### 6.4 关键数据结构

无新增数据结构。仅 `SSelectStmt` 增加一个 bool 字段（§6.2.3）。

### 6.5 数据流图

```
CREATE STREAM SQL
        │
        ▼
   Parser (sql.y) ──► SCreateStreamStmt {pTrigger, pQuery}
        │
        ▼
buildCreateStreamReq
        │
        ├─► createStreamReqBuildTriggerAst
        │       └─► createStreamReqBuildTriggerSelect
        │              proj: trigger window cols + pre_filter cols + tbname()
        │
        ├─► createStreamReqBuildCalc
        │       ├─► [NEW] injectPreFilterIntoCalcQuery
        │       │       SELECT FROM %%trows  →  SELECT FROM <triggerTbl> WHERE <preFilter clone>
        │       ├─► translateStreamCalcQuery
        │       │       translateWhere 不再误报（旁路标记）
        │       ├─► qCreateQueryPlan
        │       │       scan cols 自然包含 pre_filter 引用列
        │       │       pre_filter 作为 scan condition 下推
        │       │     [REMOVED] 19205-19216 追加块
        │       └─► createStreamReqBuildCalcPlan
        │
        └─► createStreamReqBuildTriggerPlan
                (partition cols 单独通道，独立于 trigger select 投影)
```

## 7. 接口规范

无对外接口变化。仅 client 内部 parser/planner 的实现层调整。

`SCMCreateStreamReq`（下发到 mnode 的 RPC 包）的字段语义不变；`triggerScanPlan` / `triggerCols` / `triggerFilterCols` / `partitionCols` / 计算 plan 内容因列集合调整而内容变化，但格式与解析路径不变，mnode/snode 端无需配套修改。

## 8. 安全考虑

不涉及。

## 9. 性能和可扩展性

- **触发侧**：减少不必要的列扫描（示例中减少 c3、t2 两列），单次触发 IO 与解码量下降；列越多收益越显著。
- **计算侧**：新增 pre_filter 作为 scan condition 下推后，计算端在存储层即可过滤掉无关行，相比"全量扫 + 上层过滤"通常更优。pre_filter 中的列即使被多扫一份，由于过滤效果一般明显，整体仍属性能正向。
- **行为正确性**：解决了 `%%trows` 场景下计算端取到 pre_filter 之外行的隐性 BUG。

## 10. 部署和配置

- 无新增配置项。
- 仅影响新建 stream 时 client 生成的 plan；已有 stream 不受影响。
- 回滚策略：直接 revert 本次提交即可，无 schema/协议兼容问题。

## 11. 监控和维护

- 现有 `parserDebug` / `parserError` 日志保留；新增 helper `injectPreFilterIntoCalcQuery` 内部使用同样的 `parserDebug` 输出注入是否生效，便于排查。
- 需在 `test/cases/18-StreamProcessing/` 下补充用例，覆盖：
  - 用户原文示例（state_window + pre_filter + `%%trows`）；
  - pre_filter 引用 tag；
  - pre_filter 含函数调用；
  - 用户在 `%%trows` 上写 WHERE 仍应报错；
  - union 中两边都用 `%%trows`；
  - 无 pre_filter 时 `%%trows` 行为不变。

## 12. 参考资料

- 流计算 SQL 语法：https://docs.taosdata.com/reference/taos-sql/stream/
- 语法定义：`source/libs/parser/inc/sql.y`（CREATE STREAM 规则 1565-1594）
- 触发 AST 构造：`source/libs/parser/src/parTranslater.c:18241`
- 计算 AST 构造：`source/libs/parser/src/parTranslater.c:19104`
- `%%trows` 改写：`source/libs/parser/src/parTranslater.c:7374`
- `triggerScanList` 填充点：`source/libs/planner/src/planLogicCreater.c:598`
