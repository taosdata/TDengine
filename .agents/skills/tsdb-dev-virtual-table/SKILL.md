---
name: tsdb-dev-virtual-table
description: "TDengine 虚拟表（Virtual Table）开发指南。覆盖虚拟表从 SQL 解析、计划生成、执行器到 DDL 的完整代码链路、核心数据结构和已有优化。适用于开发虚拟表新功能、调试虚拟表查询问题、理解虚拟表架构。触发关键词: virtual table, 虚拟表, vtable, virtualScan, 虚拟表开发, vtable scan, 虚拟表查询"
metadata:
  author: Jing Sima
  version: 1.0.0
  owner_team: engine
---

# TDengine 虚拟表（Virtual Table）开发指南

## When to Use

- 开发虚拟表相关新功能（新增列引用类型、扫描优化等）
- 调试虚拟表查询链路问题（解析错误、计划生成异常、执行器结果错误）
- 理解虚拟表与 Stream 的集成关系
- 修改虚拟表 DDL（CREATE/DROP VIRTUAL TABLE）逻辑
- 为虚拟表添加新的优化规则

## Prerequisites

- 已 clone TDinternal 仓库并可编译
- 熟悉 TDengine 查询引擎基本架构（Parser → Planner → Executor）
- 了解 TDengine 的超级表/子表/普通表模型

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-virtual-table version=0.1.0 author=Jing Sima`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

## 一、虚拟表概念

虚拟表（Virtual Table）是 TDengine 中一种**不实际存储数据**的逻辑表，其列通过**列引用（ColRef）**映射到其他真实表的列。查询虚拟表时，引擎自动将查询拆解为对引用源表的子查询，然后按时间戳对齐合并结果。

### 表类型定义（`include/common/taosdef.h`）

```c
TSDB_VIRTUAL_NORMAL_TABLE = 8,  // 虚拟普通表
TSDB_VIRTUAL_CHILD_TABLE  = 9,  // 虚拟子表（挂在虚拟超级表下）
```

虚拟超级表复用 `TSDB_SUPER_TABLE` 类型，通过 `virtualStb` 标志位区分。

### 列引用结构（`include/common/tmsg.h`）

```c
typedef struct {
  bool     hasRef;
  col_id_t id;
  char     refSourceName[TSDB_EXT_SOURCE_NAME_LEN]; // 外部数据源名（空=内部引用）
  char     refDbName[TSDB_DB_NAME_LEN];
  char     refTableName[TSDB_TABLE_NAME_LEN];
  char     refColName[TSDB_COL_NAME_LEN];
  char     colName[TSDB_COL_NAME_LEN];
} SColRef;
```

`SColRef` 是虚拟表的核心——每一列映射到 `refDbName.refTableName.refColName`。

## 二、完整代码链路

### 2.1 DDL 链路：CREATE / DROP VIRTUAL TABLE

**关键数据结构**（`include/libs/nodes/cmdnodes.h`）：

| 结构体 | 用途 |
|--------|------|
| `SCreateVTableStmt` | 虚拟普通表建表语句 |
| `SCreateVSubTableStmt` | 虚拟子表建表语句，含 `pColRefs`（列引用列表）和 `pTagRefs`（标签引用） |
| `SDropVirtualTableStmt` | 虚拟表删除语句 |

**CREATE 流程**：

```
SQL → Parser (sql.y)
  → parAstCreater.c 创建 SCreateVTableStmt / SCreateVSubTableStmt
  → parTranslater.c::checkCreateVirtualTable()  校验列定义
  → parTranslater.c::buildVirtualTableBatchReq() 构造 vnode 创建请求
  → vnode meta 层: metaTable2.c 创建虚拟表元数据，写入 SColRef
```

**关键文件**：
- `source/libs/parser/inc/sql.y` — 语法规则
- `source/libs/parser/src/parAstCreater.c` — AST 节点创建
- `source/libs/parser/src/parTranslater.c` — 语义校验与请求构造
- `source/dnode/vnode/src/meta/metaTable2.c` — vnode 元数据存储

**meta 层关键逻辑**（`metaTable2.c`）：
- 虚拟普通表和虚拟子表使用 `TSDB_VIRTUAL_NORMAL_TABLE` / `TSDB_VIRTUAL_CHILD_TABLE` 类型
- 通过 `TABLE_SET_VIRTUAL(flags)` 标记超级表为虚拟超级表
- 虚拟普通表的最大行字节数为 `TSDB_MAX_BYTES_PER_ROW_VIRTUAL`（大于普通表）
- 虚拟普通表的最大列数为 `TSDB_MAX_COLUMNS`，非虚拟表为 `TSDB_MAX_COLUMNS_NON_VIRTUAL`

### 2.2 查询链路：SELECT

```
SQL Parser → Translator → Logic Planner → Optimizer → Plan Splitter → Physical Planner → Executor
```

#### 2.2.1 Parser / Translator

**入口**：`parTranslater.c::translateVirtualTable()`

```c
static int32_t translateVirtualTable(STranslateContext* pCxt, SNode** pTable, SName* pName) {
  // 1. 仅支持 SELECT 操作
  // 2. 不支持 topic 查询
  // 3. 根据 pMeta->tableType 分发：
  //    - TSDB_SUPER_TABLE → translateVirtualSuperTable()
  //    - TSDB_VIRTUAL_CHILD_TABLE / TSDB_VIRTUAL_NORMAL_TABLE
  //      → 流计算中: translateVirtualNormalChildTableInStream()
  //      → 普通查询: translateVirtualNormalChildTable()
}
```

**核心工作**：将 `SRealTableNode` 转换为 `SVirtualTableNode`，填充列引用信息、vgroup 信息。

**`SVirtualTableNode`**（`include/libs/nodes/querynodes.h`）：

```c
typedef struct SVirtualTableNode {
  STableNode         table;       // QUERY_NODE_VIRTUAL_TABLE
  struct STableMeta* pMeta;       // 含 SColRef 数组
  SVgroupsInfo*      pVgroupList;
  SNodeList*         refTables;   // 引用的真实表节点列表
} SVirtualTableNode;
```

**列解析**：
- `createColumnsByVirtualTable()` — 遍历虚拟表 schema 创建列节点
- `findAndSetVirtualTableColumn()` — 解析 SELECT 中引用的列并设置映射信息
- `setVtbColumnInfoBySchema()` — 设置列的元信息

#### 2.2.2 逻辑计划（Planner）

**入口**：`planLogicCreater.c`

**核心逻辑节点** `SVirtualScanLogicNode`（`include/libs/nodes/plannodes.h`）：

```c
typedef struct SVirtualScanLogicNode {
  SLogicNode    node;
  bool          scanAllCols;     // 是否扫描所有列（优化标志）
  SNodeList*    pScanCols;       // 需要扫描的列
  SNodeList*    pScanPseudoCols; // 伪列（如 tbname）
  int8_t        tableType;
  uint64_t      tableId;
  uint64_t      stableId;
  SVgroupsInfo* pVgroupList;
  EScanType     scanType;
  SName         tableName;
} SVirtualScanLogicNode;
```

**普通虚拟表的逻辑计划生成**（简化流程）：

```
createVirtualTableLogicNode()
  → makeVirtualScanLogicNode()        // 填充 VirtualScan 基本信息
  → 遍历 colRef：
      → addSubScanNode()             // 为每个引用源表创建子扫描节点
        → findRefTableNode()         // 查找引用表
        → findRefColId()             // 查找引用列 ID
        → createRefScanLogicNode()   // 创建引用表的 SScanLogicNode（首次）
        → scanAddCol()               // 将列添加到子扫描节点
  → eliminateDupScanCols()           // 去重（多列引用同一源列）
```

**虚拟超级表额外引入 DynQueryCtrl**（`createVirtualSuperTableLogicNode`）：

```
SVirtualScanLogicNode
  ├── SScanLogicNode (真实表扫描)     // 子节点 1：引用源表
  ├── SScanLogicNode (tag 扫描)       // 子节点 2：伪列/tag
  └── parent: SDynQueryCtrlLogicNode  // 动态查询控制节点
                                      //（运行时根据子表逐个调度扫描）
       └── SScanLogicNode (ins_columns)  // 查询系统表获取子表列引用信息
```

#### 2.2.3 优化器

**条件下推**（`planOptimizer.c::pdcDealVirtualTable`）：

将虚拟表上的时间范围条件下推到各引用源表的子扫描节点：

```c
static int32_t pdcDealVirtualTable(SOptimizeContext* pCxt, SVirtualScanLogicNode* pVScan) {
  // 1. 分离条件：主键条件、tag 条件、其他条件
  // 2. 计算时间窗口 timeRange
  // 3. 遍历子扫描节点，推送 primaryKeyCond + scanRange
  // 4. 跳过超级表（超级表走 DynQueryCtrl 路径）
}
```

**条件验证**（`pdcCheckVirtualTableCond`）：确保重写后的条件引用正确的列。

#### 2.2.4 计划拆分（Plan Splitter）

**`planSpliter.c::virtualTableSplit()`**：

```c
static int32_t virtualTableSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  // 1. 找到 QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN 节点
  // 2. 为每个子扫描节点创建 Exchange 节点 + 独立子计划
  // 3. 超级表设置 seqRecvData = true（顺序接收）
  // 4. DynQueryCtrl 场景设 processOneBlock = true
  // 5. 主子计划设为 SUBPLAN_TYPE_MERGE
}
```

#### 2.2.5 物理计划

**`planPhysiCreater.c::createVirtualTableScanPhysiNode()`**：

```c
SVirtualScanPhysiNode = makePhysiNode(..., QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN)
  → createVirtualScanCols()                   // 创建扫描列
  → createVirtualTableScanPhysiNodeFinalize()  // 填充物理节点
  → setMultiBlockSlotId()                      // 设置多数据块 slot 映射
```

**`SVirtualScanPhysiNode`**（`include/libs/nodes/plannodes.h`）：

```c
typedef struct SVirtualScanPhysiNode {
  SScanPhysiNode scan;
  SNodeList*     pGroupTags;
  bool           groupSort;
  bool           scanAllCols;    // scanAllCols 优化标志
  SNodeList*     pTargets;       // 目标列映射
  SNodeList*     pTags;
  SNode*         pSubtable;
  int8_t         igExpired;
  int8_t         igCheckUpdate;
} SVirtualScanPhysiNode;
```

#### 2.2.6 执行器

**核心文件**：`source/libs/executor/src/virtualtablescanoperator.c`

**核心算子**：`VirtualTableScanOperator`（operator type = `QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN`）

**主要数据结构**：

```c
typedef struct SVirtualTableScanInfo {
  STableScanBase base;
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;     // 多源排序句柄
  int32_t        bufPageSize;
  uint64_t       sortBufSize;
  SSDataBlock*   pIntermediateBlock;
  SSDataBlock*   pInputBlock;
  SSHashObj*     dataSlotMap;     // 列映射：(blockId << 16 | slotId) → 输出 slotId
  SSHashObj*     refSlotMap;      // 引用列映射（多列引用同一源列场景）
  SArray*        refSlotGroups;   // 引用列分组
  int32_t        tsSlotId;        // 时间戳列 slotId
  int64_t        tagBlockId;      // tag 数据块 ID
  int32_t        tagDownStreamId;
  bool           scanAllCols;
  bool           useOrgTsCol;
  SArray*        pSortCtxList;
  tb_uid_t       vtableUid;
} SVirtualTableScanInfo;
```

**执行流程**：

```
virtualTableGetNext()
  → openVirtualTableScanOperator()
      → createSortHandle() / createSortHandleFromParam()
         // 创建 SORT_MULTISOURCE_TS_MERGE 排序句柄
         // 设置 ForceUsePQSort（优先队列排序）
         // 为每个下游添加 SSortSource
  → doVirtualTableMerge()
      → doGetVtableMergedBlockData()    // 普通表
      → doGetVStableMergedBlockData()   // 超级表
         // 从排序器获取 tuple，按时间戳对齐
         // 同一时间戳合并到同一行
         // 通过 dataSlotMap 映射列
         // refSlotMap 处理同源列复制
  → doSetTagColumnData()                // 填充 tag/伪列
  → doFilter()                          // 应用过滤条件
```

## 三、已有优化

### 3.1 scanAllCols 优化

当查询只需要时间戳列（如 `SELECT count(*) FROM vtable`），设置 `scanAllCols = true`，
执行器仅读取时间戳列做对齐，跳过数据列的 slot 映射，大幅减少 I/O。

**逻辑**（`planLogicCreater.c`）：
```c
if (scanAllCols) {
    pVtableScan->scanAllCols = true;
    // 子扫描节点添加所有列但不做逐列映射
}
```

**执行器**（`doGetVtableMergedBlockData`）：
```c
int32_t colNum = pInfo->virtualScanInfo.scanAllCols ? 1 : tsortGetColNum(pTupleHandle);
// scanAllCols 时只处理 ts 列
```

### 3.2 强制优先队列排序（PQ Sort）

```c
tsortSetForceUsePQSort(pVirtualScanInfo->pSortHandle);
```

虚拟表多源合并固定使用优先队列排序（`SORT_MULTISOURCE_TS_MERGE`），因为各源已按时间戳有序，PQ Sort 在这种场景下最优。

### 3.3 时间戳列 hasNull 优化

```c
// ts column will never have null value. set hasNull = false here can accelerate the sort
p->hasNull = false;
```

显式标记时间戳列无空值，加速排序比较。

### 3.4 条件下推到子扫描节点

`pdcDealVirtualTable()` 将虚拟表上的时间范围条件克隆并下推到每个引用源表的子扫描节点，减少数据读取量。

### 3.5 引用列去重 & refSlotMap

- `eliminateDupScanCols()` — 多列引用同一源列时去重，避免重复扫描
- `refSlotGroups` / `refSlotMap` — 执行器中同一源列数据复制到多个输出 slot，避免重复读取

### 3.6 DynQueryCtrl（超级表）

虚拟超级表查询通过 `SDynQueryCtrlLogicNode` 动态控制：
- 先查 `ins_columns` 系统表获取子表列引用信息
- 运行时逐子表调度扫描，支持 `processOneBlock` 模式逐块处理

## 四、关键文件索引

| 层级 | 文件 | 关键函数/结构体 |
|------|------|----------------|
| **类型定义** | `include/common/taosdef.h` | `TSDB_VIRTUAL_NORMAL_TABLE`, `TSDB_VIRTUAL_CHILD_TABLE` |
| **消息结构** | `include/common/tmsg.h` | `SColRef`, `SColRefWrapper`, `SVCTableRefCols` |
| **系统表** | `include/common/systable.h` | `TSDB_INS_TABLE_VIRTUAL_TABLES_REFERENCING` |
| **查询节点** | `include/libs/nodes/querynodes.h` | `SVirtualTableNode` |
| **DDL 节点** | `include/libs/nodes/cmdnodes.h` | `SCreateVTableStmt`, `SCreateVSubTableStmt`, `SDropVirtualTableStmt` |
| **计划节点** | `include/libs/nodes/plannodes.h` | `SVirtualScanLogicNode`, `SVirtualScanPhysiNode` |
| **语法** | `source/libs/parser/inc/sql.y` | 虚拟表 SQL 语法规则 |
| **AST 创建** | `source/libs/parser/src/parAstCreater.c` | 虚拟表 AST 节点创建 |
| **语义翻译** | `source/libs/parser/src/parTranslater.c` | `translateVirtualTable()`, `buildVirtualTableBatchReq()`, `checkCreateVirtualTable()` |
| **逻辑计划** | `source/libs/planner/src/planLogicCreater.c` | `addSubScanNode()`, `makeVirtualScanLogicNode()`, `createVirtualSuperTableLogicNode()` |
| **优化器** | `source/libs/planner/src/planOptimizer.c` | `pdcDealVirtualTable()`, `pdcCheckVirtualTableCond()` |
| **计划拆分** | `source/libs/planner/src/planSpliter.c` | `virtualTableSplit()`, `virtualTableFindSplitNode()` |
| **物理计划** | `source/libs/planner/src/planPhysiCreater.c` | `createVirtualTableScanPhysiNode()`, `createVirtualScanCols()` |
| **执行器** | `source/libs/executor/src/virtualtablescanoperator.c` | `createVirtualTableMergeOperatorInfo()`, `virtualTableGetNext()`, `doVirtualTableMerge()` |
| **执行器头** | `source/libs/executor/inc/virtualtablescan.h` | `VTS_ERR_RET`, `VTS_ERR_JRET` |
| **元数据** | `source/dnode/vnode/src/meta/metaTable.c` | 虚拟表类型判断 |
| **元数据(建表)** | `source/dnode/vnode/src/meta/metaTable2.c` | 虚拟表建表、列引用存储、schema 校验 |
| **流集成** | `source/dnode/mnode/impl/src/mndStreamMgmt.c` | `STREAM_IS_VIRTUAL_TABLE()` |

## 五、开发注意事项

1. **新增列引用类型**：修改 `SColRef` 结构后，需同步更新编解码函数（`tEncodeSColRef` / `tDecodeSColRef`），以及 catalog 缓存更新逻辑。

2. **新增优化规则**：在 `planOptimizer.c` 的优化规则表中注册，注意区分普通虚拟表（直接推给子扫描）和超级表（走 DynQueryCtrl）。

3. **执行器修改**：`doGetVtableMergedBlockData`（普通表）与 `doGetVStableMergedBlockData`（超级表）逻辑不同，需分别修改和测试。

4. **超级表路径**：虚拟超级表查询多了 `DynQueryCtrl` 层，运行时按子表逐个调度；修改执行器时注意 `pOperatorGetParam` 是否为 NULL 来区分两条路径。

5. **节点序列化**：新增/修改节点字段后，务必同步 `nodesCodeFuncs.c`、`nodesMsgFuncs.c`、`nodesCloneFuncs.c` 等节点操作文件。

6. **错误码**：虚拟表专用错误码前缀为 `TSDB_CODE_VTABLE_*`（如 `TSDB_CODE_VTABLE_SCAN_INVALID_DOWNSTREAM`、`TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR`）。

## Safety

- 本 Skill 仅提供代码导航和开发指导，不执行任何修改操作
- 修改虚拟表相关代码后务必运行完整的虚拟表测试用例
- DDL 相关改动需确保向后兼容元数据格式（`SColRef` 序列化）
- 涉及 Stream 集成的改动需同步验证流计算场景
