# 流计算优化（执行计划 + 历史数据拉取 + 虚拟表）— 统一设计说明书

> 本文合并 PR [#35196](https://github.com/taosdata/TDengine/pull/35196) 涉及的三组优化，覆盖 client→mnode→reader 全链路，并在最后给出"需求 vs 代码"逐项核对结论。
>
> 原始需求：飞书 wiki [流计算优化](https://taosdata.feishu.cn/wiki/OIwhw3iBmifwEIk4Iq3cAms9nte)
>
> 取代/合并的旧文档：
> - `docs/superpowers/specs/2026-04-22-stream-trigger-calc-scan-cols-optimize-design.md`（client/parser 子集）
> - 会话本地 `2026-04-20-stream-history-pull-optimization-design.md`（reader/history 子集）

## 1. 修订记录

| 日期 | 版本 | 修订人 | 主要内容 |
| --- | --- | --- | --- |
| 2026-04-23 | 1.0 | Copilot | 三个优化合并：parser 触发/计算列分离；reader 触发/计算独立 filter+schema；TSDB 历史拉取 9 个新接口 + 虚拟表 history 双缓冲；含 PR #35196 代码核对 |

## 2. 引言

### 2.1 目的

在一个 PR 内联动完成 client（parser/planner）、mnode（兼容 sver）、reader（vnode）三处改动，使流计算"触发数据"与"计算数据"在 plan/扫描/过滤三层完全解耦，并把历史数据拉取协议升级为高效的 DiffRange / SameRange 批量接口。

### 2.2 范围

| 优化项 | 涉及模块 | 关键文件 |
| --- | --- | --- |
| ① 触发/计算 AST 扫描列分离 | client parser/planner | `parTranslater.c`, `planLogicCreater.c`, `planner.h`, `querynodes.h` |
| ② 触发/计算 Filter 物理分离 | reader | `streamReader.h`, `streamReader.c`, `vnodeStream.c` |
| ③ TSDB 历史拉取协议升级 | 协议 + reader | `streamMsg.h`, `streamMsg.c`, `vnodeStream.c` |
| ④ 虚拟表历史路径双缓冲 | reader | `streamReader.h`, `streamReader.c`, `vnodeStream.c` |
| ⑤ 跨版本兼容 | mnode | `mndStream.h`, `mndDef.c`, `mndStream.c`, `mndStreamMgmt.c` |

不在范围：trigger 端（snode）状态机、WAL 协议（仍走旧路径，仅 filter 选择有调整）、文档/示例。

### 2.3 受众

流计算 reader/trigger 双方开发者、CR、QA。

## 3. 术语

| 术语 | 含义 |
| --- | --- |
| 触发 AST | `createStreamReqBuildTriggerSelect` 构造、用于驱动触发判断的 SELECT |
| 计算 AST | 用户在 `CREATE STREAM ... AS <query>` 中提供的 query AST |
| `%%trows` | 占位符，语义为"使用与本次触发相同的数据"；计算端会**独立再扫一次触发表** |
| pre_filter | `STREAM_OPTIONS(PRE_FILTER(<expr>))` 中的过滤表达式 |
| triggerCols / calcCols | `STableScanPhysiNode.scan.pScanCols`，分别是触发 AST 与计算 AST 的扫描列 |
| triggerBlock / calcBlock | reader 端基于 triggerResBlock / calcResBlock 衍生的"对外统一 schema + 末尾 uid 列"block |
| pFilterInfoTrigger / pFilterInfoCalc | 由 `triggerAst->pNode->pConditions` / `calcAst->pNode->pConditions` 分别 init 的 SFilterInfo |
| DiffRange | 同一请求中多个 (uid, [skey,ekey]) 的拉取，每表时间范围可不同 |
| SameRange | 同一请求中所有表共用同一时间范围 [skey,ekey] |
| `isOldPlan` | 由 mnode 序列化版本（sver==8 → true）传给 reader 的兼容标志，控制 reader 是否走"trigger schema 扫描 → calc schema transform"老路径 |
| `vSetTableListHistory` | `SStreamTriggerReaderInfo` 新增字段，TSDB 路径专用的虚拟表原始表清单（`vSetTableList` 留给 WAL 路径） |
| `uidHashTriggerHistory` / `uidHashCalcHistory` | history 路径的 `<uid → <slotId→colId>>` 映射，与实时路径 TSWAP 双缓冲隔离 |
| `streamTaskMapHistory` | `getSessionKey` → `SDiffRangeIter*`，DiffRange 跨 `_NEXT` 续传迭代器 |

## 4. 概述

### 4.1 当前实现的问题

示例 SQL：

```sql
CREATE STREAM s1 STATE_WINDOW(c1) FROM stb PARTITION BY t1
  STREAM_OPTIONS(PRE_FILTER(c2 > 2))
  INTO res_stb
  AS SELECT _c0, sum(c3), avg(t2) FROM %%trows;
```

| AST | 旧实现扫描列 | 期望扫描列 | 旧实现问题 |
| --- | --- | --- | --- |
| 触发 AST | ts, c1, c2, c3, t1, t2 | ts, c1, c2 | 多扫了计算端引用的 c3、t2 |
| 计算 AST | ts, c3, t2 | ts, c2, c3, t2 + WHERE c2>2 | 缺少 pre_filter 引用列 c2，且 reader 用的是触发 schema 扫描后再赋值（多扫 + 多拷贝） |

历史数据拉取层：旧 7 个 enum（`STRIGGER_PULL_TSDB_TS_DATA` … `STRIGGER_PULL_TSDB_DATA_NEXT`）每张表一次 RPC + 每次都重建 task，效率低；虚拟表 schema 按全列计算，多余列填 NULL 也走传输。

### 4.2 优化目标

1. 触发列 = 时间列 + window 引用列 + partition 列 + pre_filter 列 + tbname。
2. 计算列 = SELECT/原 WHERE 引用列 + （`%%trows` 时）pre_filter 列；`pre_filter` 作为 WHERE 注入，使两端取到同样行。
3. reader 历史路径用 9 个新 enum：`META`, `META_NEXT`, `DATA_DIFF_RANGE` × {Trigger, Calc} × {首, NEXT}, `DATA_SAME_RANGE` × {同上}, `SET_TABLE_HISTORY`。响应统一为单 `SSDataBlock`。
4. 虚拟表历史路径全部使用 `*History` 字段，不污染实时路径。
5. mnode 持久化版本号升级（`MND_STREAM_VER_NUMBER` 8→9），向下兼容旧 stream（标 `isOldPlan=true`）。

### 4.3 端到端架构

```
┌──────────── Client (parser/planner) ────────────┐
│ createStreamReqBuildTriggerSelect               │
│   ├ trigger window cols + pre_filter cols       │  (triggerScanList 收集已删除)
│   └ + tbname()                                  │
│ createStreamReqBuildCalc                        │
│   ├ injectPreFilterIntoCalcQuery (clone WHERE)  │  (新增)
│   └ translateStreamCalcQuery → planner          │
│        scan cols 自然包含 pre_filter 引用列     │
└─────────────────┬───────────────────────────────┘
                  │ SCMCreateStreamReq + isOldPlan=false (sver=9)
                  ▼
┌──────────── mnode ───────────────────────────────┐
│ MND_STREAM_VER_NUMBER 8→9                       │
│ 反序列化时 sver==8 → 标 isOldPlan=true 兼容旧流 │
│ msmBuildReaderDeployInfo 把 isOldPlan 透传       │
└─────────────────┬───────────────────────────────┘
                  │ SStreamReaderDeployFromTrigger (含 isOldPlan)
                  ▼
┌──────────── Reader (vnode) ──────────────────────┐
│ SStreamTriggerReaderInfo                        │
│   triggerCols   / calcCols    (独立 schema)     │
│   triggerBlock  / calcBlock   (独立物理 block)  │
│   pFilterInfoTrigger / pFilterInfoCalc          │
│   uidHashTrigger* / uidHashCalc*                │
│   uidHashTriggerHistory / uidHashCalcHistory    │  ← TSDB 路径
│   vSetTableList / vSetTableListHistory          │
│   streamTaskMap / streamTaskMapHistory          │
│                                                 │
│ TDMT_STREAM_TRIGGER_PULL dispatcher             │
│   SET_TABLE / SET_TABLE_HISTORY → 同 handler    │
│   TSDB_META[_NEXT]                              │
│       isVtableStream → MetaVtableReq            │
│       else           → MetaReq                  │
│   TSDB_DATA_DIFF_RANGE[_CALC][_NEXT]            │
│       → DiffRangeReq                            │
│   TSDB_DATA_SAME_RANGE[_CALC][_NEXT]            │
│       → SameRangeReq (拒绝虚拟表)               │
└──────────────────────────────────────────────────┘
```

## 5. 设计考虑

### 5.1 假设

- **同 schema 的物理隔离**：`triggerBlock` 与 `calcBlock` 由 reader 在 `createStreamReaderInfo` 时按各自 ResBlock + 末尾 1 列 BIGINT(uid) 构造，handler 直接以对应 schema 扫描，省一次 transform。
- **`%%trows` 改写位置不变**：`translatePlaceHolderTable` 仍把 `%%trows` 改写为对触发表的 `SRealTableNode`，不影响本次改动。
- **trigger 端会按新 enum 发起请求**：本 PR 后由 trigger 侧逐步切换；DEPRECATED 旧 enum 暂保留 `default → APP_ERROR` 路径，确保未切换时报错可见。

### 5.2 设计原则

1. **物理分离 > 标志位旁路**：`triggerCols`/`calcCols` 字段独立，避免任何"按场景切换同一字段"导致并发或时序问题。
2. **TSWAP 替换 > 编辑原地**：`SET_TABLE_HISTORY` 走完全独立的 *History 字段集，不与实时路径竞争锁。
3. **新 enum > 复用旧 enum**：协议层用类型代替布尔字段，避免分支噪声；旧 enum 保留 + DEPRECATED 注释，待 trigger 侧切完后清理。
4. **代码注释一律英文**（参见 `~/.copilot/copilot-instructions.md`）。

### 5.3 风险

| 风险 | 缓解 |
| --- | --- |
| 旧 stream（sver=8）反序列化后字段缺失 | `tDecodeSStreamObj` 在 `sver == OLD_TRIGGER_COLS(8)` 时设 `isOldPlan=true`；reader `TRANSFORM_DATA_TO_CALC` 宏在该路径下做 schema 转换 |
| pre_filter 注入在 union(`SSetOperator`) 两侧失败一侧 | `injectPreFilterIntoCalcQueryImpl` 在右侧失败时回滚左侧已注入的 pWhere |
| 虚拟表 + `%%trows` + pre_filter 历史限制被解除 | 旧版禁用块已删除（line 19250）；依赖 calc query 独立 plan 与 reader 端 calcCols 工作正常 |
| DiffRange iter 跨 `_NEXT` 丢失 | `streamTaskMapHistory` 用 `freeFp = releaseDiffRangeIterFp`；handler 错误路径主动 `taosHashRemove` 让后续 NEXT 收 `STREAM_NO_CONTEXT` |
| SameRange 误传虚拟表 | 入口 `if (isVtableStream) return INVALID_PARA` |
| 旧 7 个 TSDB 接口残留代码 | 5 个旧 handler 已删除；旧 enum 进入 `default` 分支报 `APP_ERROR` |

## 6. 详细设计

### 6.1 ① Client：触发/计算列分离

#### 6.1.1 触发侧 — 删除"计算列追加"逻辑

`parTranslater.c:19205-19216` 整块删除，触发投影列回归 `createStreamReqBuildTriggerSelect` 默认行为：

- `nodesCollectColumnsFromNode(pStmt->pTrigger, ..., COLLECT_COL_TYPE_COL, ...)` 收集 trigger 节点（window + pre_filter 普通列）
- 单独收集 pre_filter 的 TAG 列
- 追加 `tbname()`

partition 列由 `createStreamReqBuildTriggerPlan` → `createStreamSetListSlotId` 单独通道写入 `pReq->partitionCols`。

#### 6.1.2 计算侧 — `injectPreFilterIntoCalcQuery`

新增静态 helper（`parTranslater.c:19102+`），在 `translateStreamCalcQuery` 之前调用：

```c
static int32_t injectPreFilterIntoCalcQueryImpl(STranslateContext*, SNode* pPreFilter, SNode* pQuery);
```

行为：
1. `pQuery` 为 `SSetOperator` → 递归 left/right；右失败时回滚左已注入的 `pWhere` 与 `pWhereInjectedFromPreFilter`。
2. `pQuery` 为 `SSelectStmt` 且 `pFromTable` 为 `SP_PARTITION_ROWS` 占位符：
   - 若 `pSelect->pWhere != NULL` → 报 `%%trows can not be used with WHERE clause.`
   - 否则 `nodesCloneNode(pPreFilter, &pSelect->pWhere)`；置 `pWhereInjectedFromPreFilter = true`。
3. 其他类型直接返回成功（沿用原 `translateStreamCalcQuery` 校验路径）。

#### 6.1.3 `SSelectStmt` 字段

```c
// include/libs/nodes/querynodes.h
bool pWhereInjectedFromPreFilter;  // true if pWhere was cloned from stream pre_filter
```

#### 6.1.4 `translateWhere` 旁路

```c
if (pSelect->pWhere && !pSelect->pWhereInjectedFromPreFilter && ...) {
  // 报 %%trows + WHERE 错误
}
```

#### 6.1.5 死代码清理

| 文件 | 操作 |
| --- | --- |
| `parTranslater.c:19185` | 删除 `.streamCxt.triggerScanList = NULL` |
| `parTranslater.c:19250-19261` | 删除"虚拟表 + %%trows + pre_filter"禁用块 |
| `parTranslater.c:19268-19279` | 删除"追加 calc cols 到 trigger 投影"块 |
| `planLogicCreater.c:598-601` | 删除 `if (placeholderType == SP_PARTITION_ROWS) ... triggerScanList ...` 收集 |
| `planner.h:SPlanStreamContext` | 删除 `triggerScanList` 字段 |

### 6.2 ② Reader：触发/计算 filter 与 schema 物理分离

`SStreamTriggerReaderInfo` 字段升级（`streamReader.h`）：

```c
- SNode*       pConditions;            // 删除（仅在 createStreamReaderInfo 局部使用过）
- SFilterInfo* pFilterInfo;            // 删除
+ SFilterInfo* pFilterInfoTrigger;     // 由 triggerAst->pNode->pConditions init
+ SFilterInfo* pFilterInfoCalc;        // 由 calcAst->pNode->pConditions init
+ SNodeList*   calcCols;               // calcAst 的 scan.pScanCols
+ int8_t       isOldPlan;              // 老 plan 兼容
- SNodeList*   triggerPseudoCols;      // 改成局部变量
```

`createStreamReaderInfo` 内：
- 触发 AST：`pFilterInfoTrigger = filterInitFromNode(triggerAst->pNode->pConditions)`；`triggerCols = scan.pScanCols`；构造 `triggerBlock = triggerResBlock + 1 列 BIGINT(uid)`。
- 计算 AST：`pFilterInfoCalc = filterInitFromNode(calcAst->pNode->pConditions)`；`calcCols = scan.pScanCols`；`calcBlock = calcResBlock + 1 列 BIGINT(uid)`。

WAL 路径（`processWalVerMetaDataNew` / `processWalVerDataNew` / `filterData`）：
- `filterData` 根据 `(!isOldPlan && resultRsp->isCalc)` 选 `pFilterInfoCalc`，否则用 `pFilterInfoTrigger`。
- 虚拟表流跳过 `filterData`（虚拟表过滤在 trigger 端做）。

### 6.3 ③ 历史拉取协议（9 enum + 2 新结构）

#### 6.3.1 enum 增 9（`streamMsg.h:679+`）

```c
// 旧 7 个标 DEPRECATED：
STRIGGER_PULL_TSDB_TS_DATA, STRIGGER_PULL_TSDB_TRIGGER_DATA[_NEXT],
STRIGGER_PULL_TSDB_CALC_DATA[_NEXT], STRIGGER_PULL_TSDB_DATA[_NEXT]

// 新增：
STRIGGER_PULL_TSDB_DATA_DIFF_RANGE,
STRIGGER_PULL_TSDB_DATA_DIFF_RANGE_NEXT,
STRIGGER_PULL_TSDB_DATA_DIFF_RANGE_CALC,
STRIGGER_PULL_TSDB_DATA_DIFF_RANGE_CALC_NEXT,
STRIGGER_PULL_TSDB_DATA_SAME_RANGE,
STRIGGER_PULL_TSDB_DATA_SAME_RANGE_NEXT,
STRIGGER_PULL_TSDB_DATA_SAME_RANGE_CALC,
STRIGGER_PULL_TSDB_DATA_SAME_RANGE_CALC_NEXT,
STRIGGER_PULL_SET_TABLE_HISTORY,
```

#### 6.3.2 新结构

```c
typedef struct SSTriggerTableTimeRange {
  int64_t suid;   // 0 表示非虚拟表；虚拟表时为 uid 对应的 suid
  int64_t uid;
  int64_t skey;
  int64_t ekey;
} SSTriggerTableTimeRange;

typedef struct SSTriggerTsdbDataDiffRangeRequest {
  SSTriggerPullRequest base;
  int64_t              ver;
  int8_t               order;
  SArray*              ranges;   // SArray<SSTriggerTableTimeRange>
} SSTriggerTsdbDataDiffRangeRequest;

typedef struct SSTriggerTsdbDataSameRangeRequest {
  SSTriggerPullRequest base;
  int64_t              ver;
  int64_t              gid;     // 0=全部表；非 0=单 group
  int64_t              skey;
  int64_t              ekey;
  int8_t               order;
} SSTriggerTsdbDataSameRangeRequest;

typedef SSTriggerPullRequest SSTriggerTsdbDataDiffRangeNextRequest;
typedef SSTriggerPullRequest SSTriggerTsdbDataSameRangeNextRequest;
```

`SSTriggerSetTableRequest` **不变**：`SET_TABLE_HISTORY` 复用同一结构、同一序列化分支（`case STRIGGER_PULL_SET_TABLE: case STRIGGER_PULL_SET_TABLE_HISTORY:` fallthrough）。

#### 6.3.3 响应

所有 9 个接口（除 `SET_TABLE_HISTORY`，无 payload）统一 `buildRsp(SSDataBlock*)` → 单 block。列布局：

| 接口 | 非虚拟表 列 | 虚拟表 列 |
| --- | --- | --- |
| `META` / `META_NEXT` | sKey, eKey, uid, gid, rows | sKey, eKey, uid, rows |
| `DATA_DIFF_RANGE[_CALC]` | triggerResBlock 或 calcResBlock 列 + uid | 同左 + 按 `uidHash*History` 做 slotId→colId 映射 |
| `DATA_SAME_RANGE[_CALC]` | 同上 | **不支持** |

### 6.4 ④ Reader：handler 实现

#### 6.4.1 dispatch（`vnodeStream.c:4265+`）

```c
case STRIGGER_PULL_SET_TABLE:
case STRIGGER_PULL_SET_TABLE_HISTORY:
  → vnodeProcessStreamSetTableReq            // 按 base.type 路由 *History 字段

case STRIGGER_PULL_TSDB_META[_NEXT]:
  isVtableStream ? vnodeProcessStreamTsdbMetaVtableReq
                 : vnodeProcessStreamTsdbMetaReq

case STRIGGER_PULL_TSDB_DATA_DIFF_RANGE[_CALC][_NEXT]:
  → vnodeProcessStreamTsdbDataDiffRangeReq

case STRIGGER_PULL_TSDB_DATA_SAME_RANGE[_CALC][_NEXT]:
  → vnodeProcessStreamTsdbDataSameRangeReq   // 入口拒绝虚拟表

default:  // 包含 5 个旧 enum
  return TSDB_CODE_APP_ERROR
```

#### 6.4.2 `vnodeProcessStreamSetTableReq`

```c
taosWLockLatch(&info->lock);
if (req->base.type == STRIGGER_PULL_SET_TABLE_HISTORY) {
  TSWAP(info->uidHashTriggerHistory, req->setTableReq.uidInfoTrigger);
  TSWAP(info->uidHashCalcHistory,    req->setTableReq.uidInfoCalc);
  qStreamClearTableInfo(&info->vSetTableListHistory);
  initStreamTableListInfo(&info->vSetTableListHistory);
  qBuildVTableListHistory(info);    // 用 uidHashTriggerHistory 重建 vSetTableListHistory
} else {
  TSWAP(info->uidHashTrigger, ...);
  TSWAP(info->uidHashCalc,    ...);
  qStreamClearTableInfo(&info->vSetTableList);
  initStreamTableListInfo(&info->vSetTableList);
  qBuildVTableList(info);
}
taosWUnLockLatch(&info->lock);
```

实时与历史路径完全 TSWAP 隔离。

#### 6.4.3 `vnodeProcessStreamTsdbMetaVtableReq`（虚拟表 META）

虚拟表 `vSetTableListHistory` 可能含多个 suid，每个 suid 需独立 `createStreamTask`：

```c
if (isFirst) {
  qStreamCopyTableInfo(info, &tableInfo, /*isHistory=*/true);
  qStreamIterTableList(&tableInfo, &pList, &pNum, &suid);    // 取第一组
  createStreamTask(...); TSWAP(pTaskInner->vTableInfo, tableInfo);
  taosHashPut(streamTaskMap, &key, &pTaskInner);
  createBlockForTsdbMeta(&pTaskInner->pResBlockDst, /*isVtable=*/true);  // 4 列
} else {
  pTaskInner = streamTaskMap[key];
}

while (true) {
  getTableDataInfo(pTaskInner, &hasNext);
  if (!hasNext) {
    // 当前 suid 扫完，取下一 suid 重建 task（继承 vTableInfo 游标）
    qStreamIterTableList(&pTaskInner->vTableInfo, &pList, &pNum, &suid);
    if (pNum == 0) break;
    createStreamTask(...); TSWAP(pTaskInnerNew->vTableInfo, pTaskInner->vTableInfo);
    taosHashPut(streamTaskMap, &key, &pTaskInnerNew);   // 替换旧 task
    pTaskInner = pTaskInnerNew;
  }
  pTaskInner->pResBlock->info.id.groupId = qStreamGetGroupIdFromSet(...);
  addColData(pResBlockDst, idx, &skey/&ekey/&uid/&rows);
  if (rows >= STREAM_RETURN_ROWS_NUM) break;
}
buildRsp(pTaskInner->pResBlockDst);
if (!hasNext) taosHashRemove(streamTaskMap, &key);
```

#### 6.4.4 `vnodeProcessStreamTsdbDataDiffRangeReq`

迭代器 `SDiffRangeIter { ranges, pos, ver, order, isCalc }` 缓存到 `streamTaskMapHistory[sessionKey(sid, baseType)]`，`baseType` 区分 trigger/calc 共用同一 session。

```c
isCalc  = (type == DIFF_RANGE_CALC || type == DIFF_RANGE_CALC_NEXT)
isFirst = (type == DIFF_RANGE       || type == DIFF_RANGE_CALC)
baseType= isCalc ? DIFF_RANGE_CALC : DIFF_RANGE
key     = sessionKey(sid, baseType)

if (isFirst) {
  init streamTaskMapHistory(once) with freeFp=releaseDiffRangeIterFp
  newDiffRangeIter(req.ranges, ver, order, isCalc, &iter)
  taosHashPut(streamTaskMapHistory, key, iter)  // 失败时手动 destroyDiffRangeIter
} else {
  iter = streamTaskMapHistory[key]    // 没有 → STREAM_NO_CONTEXT
}

// pCur 复用：scanOneTableForRange 内部检查 *outBlock==NULL 才创建
while (iter->pos < N) {
  scanOneTableForRange(.., iter->ranges[pos], iter->ver, iter->order, iter->isCalc, &pCur)
  iter->pos++
  if (pCur && pCur->rows >= STREAM_RETURN_ROWS_NUM) break
}

TRANSFORM_DATA_TO_CALC          // 老 plan 时 trigger schema → calc schema
buildRsp(pCur)
if (iter->pos >= N) taosHashRemove(streamTaskMapHistory, key)  // freeFp 释放 iter

end:
  if (code != 0 && iter != NULL) taosHashRemove(...);   // 错误时驱逐残块迭代器
  blockDataDestroy(pCur)
```

#### 6.4.5 `scanOneTableForRange`（DiffRange 工作函数）

签名：`(pVnode, info, range, ver, order, isCalc, SSDataBlock** outBlock)`，**累积模式**：每次 range 把行 append 到 `*outBlock`。

```c
NEW_CALC = (!info->isOldPlan && isCalc)
tmplBlock = NEW_CALC ? info->calcBlock : info->triggerBlock

if (info->isVtableStream) {
  pickSchemasHistory(info, range->suid, range->uid, isCalc,
                     &schemas, &slotIdList);   // 按 uidHash*History 取列
  options.schemas    = schemas;
  options.pSlotList  = &slotIdList;
  options.isSchema   = true;
} else {
  options.schemas = NEW_CALC ? info->calcCols : info->triggerCols;
  options.isSchema = false;
}

if (*outBlock == NULL) createOneDataBlock(tmplBlock, false, outBlock);
startIndex = (*outBlock)->info.rows;

createStreamTask(.., tmplBlock, &(STableKeyInfo){range->uid, gid}, 1, ..);

while (getTableDataInfo(pTaskInner, &hasNext), hasNext) {
  getTableData(pTaskInner, &pBlock);
  if (pBlock->rows > 0) processTag(info, isCalc, uid, pBlock, 0, rows, 1);
  if (!info->isVtableStream)
    qStreamFilter(pBlock, NEW_CALC ? pFilterInfoCalc : pFilterInfoTrigger, NULL);
  if (pBlock->rows > 0) {
    blockDataMerge(*outBlock, pBlock);
    totalRows += pBlock->rows;
  }
}

// uid 列填到本 range 新增行段：
colDataSetNItems(lastCol(*outBlock), startIndex, &range->uid, totalRows, 1, false);

end: free schemas/slotIdList; releaseStreamTask(&pTaskInner);
```

#### 6.4.6 `vnodeProcessStreamTsdbDataSameRangeReq`

```c
if (info->isVtableStream) return TSDB_CODE_INVALID_PARA;     // 入口拒绝

isCalc/isFirst/baseType/key 同 DiffRange，base 用 SAME_RANGE
tmplBlock = NEW_CALC ? info->calcBlock : info->triggerBlock

if (isFirst) {
  qStreamGetTableList(info, gid, &pList, &pNum);
  options.schemas = NEW_CALC ? info->calcCols : info->triggerCols;
  createStreamTask(.., tmplBlock, pList, pNum, ..);
  taosHashPut(streamTaskMap, key, pTaskInner);     // 注：用 streamTaskMap，不是 *History
} else {
  pTaskInner = streamTaskMap[key];
}

createOneDataBlock(tmplBlock, false, &pCur);    // 累积响应 block

while (getTableDataInfo(pTaskInner, &hasNext), hasNext) {
  getTableData(pTaskInner, &pBlock);            // pBlock 借用 pTaskInner 内部缓冲
  if (pBlock->rows > 0) processTag(info, isCalc, pBlock->info.id.uid, pBlock, 0, rows, 1);
  qStreamFilter(pBlock, NEW_CALC ? pFilterInfoCalc : pFilterInfoTrigger, NULL);
  if (pBlock->rows == 0) continue;

  // 在 pBlock 末列就地写 uid（pBlock 与 tmplBlock 同 schema，末列是 BIGINT(uid)）
  colDataSetNItems(lastCol(pBlock), 0, &pBlock->info.id.uid, pBlock->rows, 1, false);

  blockDataMerge(pCur, pBlock);                 // 同 schema，安全 append
  if (pCur->rows >= STREAM_RETURN_ROWS_NUM) break;
}

TRANSFORM_DATA_TO_CALC
buildRsp(pCur)
if (!hasNext) taosHashRemove(streamTaskMap, key)
end: blockDataDestroy(pCur); free(pList);
```

#### 6.4.7 `TRANSFORM_DATA_TO_CALC` 宏（兼容老 plan）

```c
#define TRANSFORM_DATA_TO_CALC \
  if (info->isOldPlan && isCalc && pCur && pCur->info.rows > 0) { \
    createOneDataBlock(info->calcBlock, false, &pResult);          \
    blockDataEnsureCapacity(pResult, pCur->info.capacity);          \
    blockDataTransform(pResult, pCur);                              \
    blockDataDestroy(pCur);                                         \
    pCur = pResult; pResult = NULL;                                 \
  }
```

老 plan 时 reader 仍按 trigger schema 扫描（因为旧 calcCols 不可信），扫完再统一 transform 到 calc schema。新 plan (isOldPlan=false) 直接以 calc schema 扫描，跳过本宏。

#### 6.4.8 DataBlock 生命周期总表

| handler | pCur 来源 | pBlock 来源 | merge 单位 | uid 写位置 |
| --- | --- | --- | --- | --- |
| DiffRange | `scanOneTableForRange` 第一次创建后跨 range 累积 | pTaskInner 内部缓冲（每 range 新建 task） | range 内每批 append 到 `*outBlock` | 每 range 末尾写入 [startIndex, +totalRows) |
| SameRange | handler 入口 `createOneDataBlock(tmplBlock)` | pTaskInner 内部缓冲（一次 task 多批） | 每批 append 到 `pCur` | pBlock 末列就地写 [0, pBlock->rows) → 随 merge 进入 pCur |
| Meta(非虚拟表) | pTaskInner->pResBlockDst（task 内置） | pTaskInner->pResBlock | 逐行 addColData | `addColData(.., &pResBlock->info.id.uid)` |
| Meta(虚拟表) | 同上 | 同上 + 跨 suid 重建 task 时通过 vTableInfo 游标接力 | 同上 | 4 列布局，无 gid |

### 6.5 ⑤ Mnode 兼容（`mndStream.h/c`、`mndDef.c`、`mndStreamMgmt.c`）

```c
#define MND_STREAM_VER_NUMBER             9
#define MND_STREAM_COMPATIBLE_VER_NUMBER  7
#define MND_STREAM_OLD_TRIGGER_COLS       8
```

`tDecodeSStreamObj`：

```c
if (sver >= MND_STREAM_OLD_TRIGGER_COLS) {  // 8 或 9
  tDeserializeSCMCreateStreamReqImpl(pDecoder, pObj->pCreate);
  pObj->pCreate->isOldPlan = (sver == MND_STREAM_OLD_TRIGGER_COLS);  // 8 → true
} else {                                     // 7
  tDeserializeSCMCreateStreamReqImplOld(...);
}
```

`mndStreamActionDecode`：`sver < COMPATIBLE_VER_NUMBER(7)` 才视为不兼容；`>=7` 一律可解析。

`msmBuildReaderDeployInfo`：把 `pInfo->pCreate->isOldPlan` 写入 `pTrigger->isOldPlan`，序列化到 `SStreamReaderDeployFromTrigger.isOldPlan`，reader 在 `createStreamReaderInfo` 取出存入 `info->isOldPlan`。

## 7. 接口规范

### 7.1 RPC 字段不变

`SCMCreateStreamReq` 新增 `bool isOldPlan`（仅 mnode↔reader 部署消息内部用，不对外）。其他下发协议字段语义不变。

### 7.2 9 个新拉取接口

| Trigger 请求类型 | enum | 内容 | 非虚拟表响应 | 虚拟表响应 |
| --- | --- | --- | --- | --- |
| TsdbMetaNew | `STRIGGER_PULL_TSDB_META` | ver, sKey, eKey, gid, order | `<sKey,eKey,uid,gid,rows>` | `<sKey,eKey,uid,rows>` |
| TsdbMetaNewNext | `STRIGGER_PULL_TSDB_META_NEXT` | – | 同上 | 同上 |
| TsdbDataDiffRangeNew[Calc] | `STRIGGER_PULL_TSDB_DATA_DIFF_RANGE[_CALC]` | ver, order, `Array<{suid,uid,sKey,eKey}>` | triggerResBlock\|calcResBlock + uid | 同左，按 `uidHash*History` 做 slotId→colId 映射，无映射列填 NULL |
| TsdbDataDiffRangeNew[Calc]Next | `_NEXT` 变体 | – | 同上 | 同上 |
| TsdbDataSameRangeNew[Calc] | `STRIGGER_PULL_TSDB_DATA_SAME_RANGE[_CALC]` | ver, gid, sKey, eKey, order | 同上 | **不支持**（`INVALID_PARA`） |
| TsdbDataSameRangeNew[Calc]Next | `_NEXT` 变体 | – | 同上 | 不支持 |
| SetTableHistory | `STRIGGER_PULL_SET_TABLE_HISTORY` | `SSTriggerSetTableRequest`（同 SET_TABLE） | – | 写入 `vSetTableListHistory` + `uidHash*History` |

## 8. 性能与正确性

- 触发列减少 → 每次触发扫描 IO/解码下降，列越多收益越大。
- 计算端独立 calcCols + pre_filter 下推 → 存储层即可过滤，避免 reader 多扫 + 多次列赋值。
- DiffRange 一次 RPC 拉多 (uid, range) → trigger↔reader RPC 数量与 task 创建次数显著下降。
- `_NEXT` 续传 + `STREAM_RETURN_ROWS_NUM` 截断 → 大结果集不阻塞单 RPC 且不丢游标。
- 双缓冲 (`*History`) → 实时与历史拉取互不阻塞，TSWAP 替换避免锁竞争。
- 行为修复：`%%trows` 计算端真正与触发端取到同行。

## 9. 测试

- `source/libs/new-stream/test/streamMsgTest.cpp`：6 个 gtest，覆盖 SET_TABLE 两类 type、DiffRange/SameRange 4 种变体、空/大量/连续 destroy 场景。
- `test/cases/18-StreamProcessing/04-Options/test_pre_filter_trows_scan_cols.py`：覆盖 user 原 SQL + pre_filter 引用 tag / 函数 / 用户写 WHERE 仍报错 / 无 pre_filter 行为不变。

## 10. 需求 ↔ 代码核对

> 标记：✅ 完全符合 / ⚠️ 符合但有遗留 / ❌ 不符合

| # | 需求 | 实现位置 | 结论 |
| --- | --- | --- | --- |
| 1 | 触发 AST 不再多扫计算端列（c3,t2） | `parTranslater.c` 删 19250-19279 / 19268-19279；`planLogicCreater.c` 删 598-601；`planner.h` 删 `triggerScanList` | ✅ |
| 2 | 计算 AST 增加 pre_filter 引用列 + WHERE 注入 | `parTranslater.c:19102+ injectPreFilterIntoCalcQuery*`；`querynodes.h` 加 `pWhereInjectedFromPreFilter`；`translateWhere` 旁路 | ✅ |
| 3 | server 端计算用独立 calc AST 扫描（不再借触发数据赋值） | `streamReader.h/c`：新增 `calcCols` / `pFilterInfoCalc` / `calcBlock` 独立字段；`scanOneTableForRange` `NEW_CALC` 选 calc schema | ✅ |
| 4 | 9 个新 enum + 2 新结构 | `streamMsg.h:679+`、`streamMsg.c` 序列化/反序列化/destroy | ✅ |
| 5 | DiffRange `Array<suid,uid,sKey,eKey>` + ver + order | `SSTriggerTsdbDataDiffRangeRequest` + `SSTriggerTableTimeRange` | ✅ |
| 6 | SameRange ver/gid/skey/ekey/order；虚拟表不支持 | 入口 `if (isVtableStream) return INVALID_PARA` | ✅ |
| 7 | SetTableHistory 走相同 SSTriggerSetTableRequest | encode/decode/destroy 三处均 fallthrough；`vnodeProcessStreamSetTableReq` 按 base.type 路由 *History 字段 | ✅ |
| 8 | 新增 `vSetTableListHistory` / `uidHashTriggerHistory` / `uidHashCalcHistory` | `streamReader.h:60-100`；TSDB 路径全用 *History；WAL 路径不变 | ✅ |
| 9 | 虚拟表 DiffRange 用 *History 做 slotId→colId 映射 | `pickSchemasHistory(.., uidHash*History..)` + `options.pSlotList` 传给 tsdReader | ✅ |
| 10 | 兼容旧 stream（mnode 序列化升级） | `MND_STREAM_VER_NUMBER 8→9`；`OLD_TRIGGER_COLS=8` 视为 isOldPlan；reader `TRANSFORM_DATA_TO_CALC` 宏兜底 | ✅ |

### 10.1 实现侧值得关注的点（非阻塞）

| # | 现象 | 位置 | 评估 |
| --- | --- | --- | --- |
| A | "虚拟表 + %%trows + pre_filter" 历史限制被解除 | `parTranslater.c` 删除原 19250 块 | ⚠️ 需求文档未明示是否要保留；新方案 calcCols 独立后理论可支持，建议补 e2e 用例验证 |
| B | SameRange handler 内残留 `if (!isVtableStream)` 守卫（line 3454） | `vnodeStream.c:3454` | ⚠️ 入口已拒虚拟表，此处恒真；冗余无害，可后续清理 |
| C | DiffRange / SameRange 函数中 `pResult` 局部变量仅在 `TRANSFORM_DATA_TO_CALC` 宏内部赋值 | `vnodeStream.c:3284, 3378` | ⚠️ end 路径 `blockDataDestroy(NULL)` 安全；可读性不佳，建议把宏改为 inline helper 或加注释 |
| D | DEPRECATED 旧 7 个 enum 仍占据 `ESTriggerPullType` 数值位 | `streamMsg.h:680-687` | ⚠️ 等待 trigger 端切换完后清理；当前 dispatcher 走 `default → APP_ERROR` 不会误处理 |
| E | `processTag` 在 calc 路径也调用（旧版 `!isCalc` 守卫被去掉） | DiffRange/SameRange handler | ✅ 与新需求一致：calc AST 也可能引用 tag 列（如 `avg(t2)`） |
| F | `scanOneTableForRange` 复用 `*outBlock`：跨 range 累积，逐 range 写 uid | `vnodeStream.c:3211-3260` | ✅ 起点 `startIndex`，写入区间 `[startIndex, +totalRows)`，与 merge 行为对齐 |
| G | DiffRange 错误路径 iter 驱逐 | `vnodeStream.c:3350-3354` | ✅ 防止下一 `_NEXT` 复用残块迭代器 |
| H | mnode `mndStreamActionDecode` 由"== 等值校验"改为">= 兼容版本"放行 | `mndStream.c:85` | ✅ 更宽松，向下兼容；同时配合 `tDecodeSStreamObj` 内部分支 |

### 10.2 建议的后续行动

1. 补一个用例：虚拟表 + `%%trows` + pre_filter 端到端，验证 §10.1-A 限制解除后行为正确。
2. 删除 §10.1-B 冗余守卫；§10.1-C 把 `TRANSFORM_DATA_TO_CALC` 宏重构为 inline helper（或保留宏但补 doc comment）。
3. trigger 侧切到 9 个新 enum 后，把 §10.1-D 中 7 个旧 enum 从 `ESTriggerPullType` 移除（含 `streamMsg.c` 序列化兜底分支）。
4. 把本文从 `docs/superpowers/specs/` 移交到产品文档库，并归档 `2026-04-22-stream-trigger-calc-scan-cols-optimize-design.md`、本地会话 `2026-04-20-stream-history-pull-optimization-design.md`。

## 11. 参考资料

- 需求源：[飞书 wiki — 流计算优化](https://taosdata.feishu.cn/wiki/OIwhw3iBmifwEIk4Iq3cAms9nte)
- PR：[taosdata/TDengine#35196](https://github.com/taosdata/TDengine/pull/35196)
- 流计算 SQL 语法：https://docs.taosdata.com/reference/taos-sql/stream/
- 虚拟表语法：https://docs.taosdata.com/reference/taos-sql/virtualtable/
- 关键源码位置（截至 PR HEAD `d18cce6`）：
  - 协议：`include/common/streamMsg.h:679-916`、`source/common/src/msg/streamMsg.c:3097-3570`
  - Reader 信息：`include/libs/new-stream/streamReader.h:41-170`、`source/libs/new-stream/src/streamReader.c:178-780`
  - Reader handler：`source/dnode/vnode/src/vnd/vnodeStream.c:2818-3489, 4255-4328`
  - Parser/Planner：`source/libs/parser/src/parTranslater.c:10522-10529, 19102-19260`、`source/libs/planner/src/planLogicCreater.c:595-602`
  - Mnode 兼容：`source/dnode/mnode/impl/inc/mndStream.h:102-110`、`source/dnode/mnode/impl/src/mndDef.c:47-55`、`source/dnode/mnode/impl/src/mndStream.c:80-90`、`source/dnode/mnode/impl/src/mndStreamMgmt.c:765-770`
