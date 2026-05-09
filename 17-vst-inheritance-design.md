# 技术设计文档: 虚拟超级表继承 (VST Inheritance)

> 版本: 1.0
> 日期: 2026-04-30
> 对应 FS: 17-vst-inheritance-fs.md

---

## 1. 概述

### 1.1 背景

当前 TDengine 中的虚拟超级表 (VST) 是扁平结构，每个 VST 独立定义自己的 schema。FS 要求实现 VST 之间的继承关系，使子 VST 可以继承父 VST 的列/Tag 定义并添加新列，形成树状继承层次。查询时支持通过 `EXPAND(N)` 语法展开子孙 VST 的 VCT 数据。

### 1.2 现状分析

| 组件 | 现状 | 需改造 |
|------|------|--------|
| `SStbObj` (mndDef.h:919) | 有 `virtualStb` 标志位，**无** `parentSuid` 等继承字段 | 是 |
| `SMCreateStbReq` (tmsg.h:1290) | 有 `virtualStb` 标志位，**无**继承字段 | 是 |
| `SVCreateStbReq` (tmsg.h:4902) | MNode→VNode 的 STB 创建请求，**无**继承字段 | 是 |
| Parser (parAstCreater.c) | 支持 `CREATE VIRTUAL STABLE`/`TABLE`，**无** `BASE ON`/`EXPAND` 语法 | 是 |
| Planner (planLogicCreater.c) | `SVirtualScanLogicNode` 只扫描当前 VST 的 VCT | 是 |
| Executor (dynqueryctrloperator.c) | `buildVirtualSuperTableScanChildTableMap()` 只查当前 VST 的 VCT | 是 |
| System Table | 有 `ins_virtual_child_columns`、`ins_virtual_tables_referencing`，**无** `ins_inherits` | 是 |
| SDB 版本 | `STB_VER_NUMBER = STB_VER_SUPPORT_OWNER(4)` | 需升至 5 |

---

## 2. 元数据层 (MNode)

### 2.1 SStbObj 扩展

**文件**: `source/dnode/mnode/impl/inc/mndDef.h`

```c
typedef struct {
  // ... 现有字段 ...
  int8_t      virtualStb;
  int8_t      secureDelete;

  // ---- 新增继承字段 ----
  int64_t     parentSuid;      // 父 VST 的 UID，0 表示无父（根节点）
  int64_t     parentDbUid;     // 父 VST 所在数据库 UID（支持跨库继承）
  int32_t     inheritDepth;    // 当前继承深度，根节点为 0
  int32_t     ownColStart;     // 自身新增列的起始 index（前面是继承来的列）
  int32_t     ownTagStart;     // 自身新增 Tag 的起始 index
} SStbObj;
```

**设计要点**：
- `parentSuid = 0` 表示根 VST 或非继承 VST，保持向后兼容
- `ownColStart` 用于区分继承列和自身新增列，支持 ALTER CASCADE 时精确定位
- `inheritDepth` 创建时计算（父节点 depth + 1），限制最大值为 10

### 2.2 SDB 版本升级

**文件**: `source/dnode/mnode/impl/src/mndStb.c`

```c
#define STB_VER_SUPPORT_INHERIT 5
#define STB_VER_NUMBER          STB_VER_SUPPORT_INHERIT
```

在 `mndStbActionDecode()` 中：
```c
if (sver < STB_VER_SUPPORT_INHERIT) {
  pStb->parentSuid    = 0;
  pStb->parentDbUid   = 0;
  pStb->inheritDepth  = 0;
  pStb->ownColStart   = 0;
  pStb->ownTagStart   = 0;
} else {
  SDB_GET_INT64(pRaw, dataPos, &pStb->parentSuid, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->parentDbUid, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->inheritDepth, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->ownColStart, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->ownTagStart, _OVER)
}
```

`mndStbActionEncode()` 中对应添加 `SDB_SET_INT64/INT32` 写入。

### 2.3 消息结构扩展

**文件**: `include/common/tmsg.h`

`SMCreateStbReq` 新增字段：
```c
typedef struct {
  // ... 现有字段 ...
  int8_t   virtualStb;
  int8_t   secureDelete;

  // ---- 新增 ----
  int64_t  parentSuid;     // 父 VST UID
  char     parentDbFName[TSDB_DB_FNAME_LEN]; // 父 VST 所在库全名
} SMCreateStbReq;
```

序列化函数 `tSerializeSMCreateStbReq()` / `tDeserializeSMCreateStbReq()` 需追加字段。采用尾部追加策略，反序列化时若 buffer 不足则默认为 0，兼容旧版本。

`SVCreateStbReq` (MNode→VNode) 同样追加 `parentSuid`、`inheritDepth`，使 VNode 侧感知继承关系（用于后续 meta 查询优化）。

---

## 3. DDL 实现

### 3.1 CREATE VIRTUAL STABLE ... BASE ON

#### 3.1.1 语法解析 (Parser)

**文件**: `source/libs/parser/src/parAstCreater.c`

新增 AST 创建函数：
```c
SNode* createCreateVTableInheritStmt(SAstCreateContext* pCxt,
                                     SToken* pDbName,
                                     SToken* pTableName,
                                     SToken* pParentDbName,
                                     SToken* pParentTableName,
                                     SNodeList* pCols,
                                     SNodeList* pTags,
                                     STableOptions* pOptions);
```

**文件**: `include/libs/nodes/cmdnodes.h`

新增或扩展 `SCreateVTableStmt`：
```c
typedef struct SCreateVTableStmt {
  ENodeType  type;                               // QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT
  char       dbName[TSDB_DB_NAME_LEN];
  char       tableName[TSDB_TABLE_NAME_LEN];
  bool       ignoreExists;
  SNodeList* pCols;

  // ---- 新增继承字段 ----
  bool       hasParent;                           // 是否有 BASE ON
  char       parentDbName[TSDB_DB_NAME_LEN];      // 父 VST 数据库
  char       parentTableName[TSDB_TABLE_NAME_LEN]; // 父 VST 表名
  SNodeList* pNewCols;                            // 子 VST 自身新增列
  SNodeList* pNewTags;                            // 子 VST 自身新增 Tag
} SCreateVTableStmt;
```

#### 3.1.2 语义校验 (Translator)

**文件**: `source/libs/parser/src/parTranslater.c`

新增 `translateCreateVirtualStableInherit()` 函数，在 `translateVirtualTable()` 中根据 `hasParent` 路由：

```
translateVirtualTable()
  ├── hasParent == false → 原有逻辑 translateVirtualSuperTable()
  └── hasParent == true  → translateCreateVirtualStableInherit()
```

`translateCreateVirtualStableInherit()` 核心逻辑：

1. **父表存在性校验**：通过 catalog 获取父表 meta，检查 `virtualStb == 1`
2. **深度校验**：父表 `inheritDepth + 1 <= 10`，否则报 `TSDB_CODE_MND_VST_INHERIT_DEPTH_EXCEED`
3. **循环检测**：沿 `parentSuid` 链回溯到根，确保不出现当前 suid
4. **列名冲突检测**：新增列名不可与父表已有列/Tag 重名
5. **Schema 合并**：`finalCols = parentCols + newCols`，`finalTags = parentTags + newTags`
6. **colId 分配**：继承列保留父表的 colId，新增列从父表 `nextColId` 开始递增
7. **构建 `SMCreateStbReq`**：带上 `parentSuid`、`parentDbFName`、合并后的完整 schema

#### 3.1.3 MNode 处理

**文件**: `source/dnode/mnode/impl/src/mndStb.c`

在 `mndProcessCreateStbReq()` 中增加继承处理分支：

```c
if (createReq.parentSuid != 0) {
  // 1. 获取父 SStbObj
  SStbObj *pParent = mndAcquireStb(pMnode, parentFullName);
  // 2. 校验父表是 virtualStb
  // 3. 校验深度
  // 4. 循环检测
  // 5. 构建子 SStbObj，设置 parentSuid/parentDbUid/inheritDepth/ownColStart/ownTagStart
  // 6. 权限继承：从父表复制权限到子表
  mndInheritStbPrivilege(pMnode, pTrans, pParent, pNewStb);
}
```

### 3.2 DROP 带继承检查

**文件**: `source/dnode/mnode/impl/src/mndStb.c`

在 `mndProcessDropStbReq()` 中，DROP 前新增子表检查：

```c
static int32_t mndCheckDropStbForChildren(SMnode *pMnode, SStbObj *pStb) {
  // 遍历 SDB 中所有 SStbObj，检查是否有 parentSuid == pStb->uid
  // 若找到，返回 TSDB_CODE_MND_VST_HAS_CHILDREN
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;
  while (1) {
    SStbObj *pChild = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pChild);
    if (pIter == NULL) break;
    if (pChild->parentSuid == pStb->uid) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pChild);
      return TSDB_CODE_MND_VST_HAS_CHILDREN;
    }
    sdbRelease(pSdb, pChild);
  }
  return TSDB_CODE_SUCCESS;
}
```

在 `mndProcessDropStbReq()` 的 `pStb->virtualStb == 1` 分支中调用此检查。

### 3.3 ALTER CASCADE（父表 ADD COLUMN 级联到子孙）

**文件**: `source/dnode/mnode/impl/src/mndStb.c`

新增 `mndCascadeAlterAddColumn()`：

```
mndProcessAlterStbReq()
  └── TSDB_ALTER_TABLE_ADD_COLUMN && pStb->virtualStb
       └── mndCascadeAlterAddColumn(pMnode, pTrans, pDb, pStb, pAlter)
            ├── 对当前 VST 执行 mndAddSuperTableColumn()
            └── 遍历 SDB 找所有 parentSuid == pStb->uid 的子 VST
                 ├── 在子 VST schema 的 ownColStart 位置插入新列
                 ├── 子 VST 的 ownColStart += ncols
                 ├── 递归处理子 VST 的子孙
                 └── 为每个子 VST 生成 mndSetAlterStbRedoActions
```

**关键细节**：
- 新增列在子 VST 中的 colId 与父表一致（不重新分配）
- 列插入位置在 `ownColStart` 前（即继承区域末尾）
- 事务内所有子孙 VST 的 ALTER 操作打包为同一个 `STrans`

**DROP COLUMN / MODIFY COLUMN**：
- `DROP COLUMN`：若 `pStb->virtualStb && hasChildren(pStb)` → 拒绝，返回 `TSDB_CODE_MND_VST_HAS_CHILDREN`
- `MODIFY COLUMN`（兼容类型变更）：同 ADD COLUMN 的级联逻辑，递归修改所有子孙中对应 colId 的类型

---

## 4. 查询链路实现 (EXPAND)

### 4.1 语法解析

#### 4.1.1 Lemon 语法扩展

**文件**: `source/libs/parser/src/taos_lemon_sql.tab.c` (由 .y 文件生成)

在 `FROM` 子句中增加 EXPAND 修饰：

```
from_clause ::= FROM virtual_table_ref.
virtual_table_ref ::= full_table_name EXPAND.
virtual_table_ref ::= full_table_name EXPAND NK_LP NK_INTEGER NK_RP.
```

#### 4.1.2 AST 节点

**文件**: `include/libs/nodes/querynodes.h`

扩展 `SVirtualTableNode`：
```c
typedef struct SVirtualTableNode {
  STableNode         table;
  struct STableMeta* pMeta;
  SVgroupsInfo*      pVgroupList;
  SNodeList*         refTables;

  // ---- 新增 ----
  int32_t            expandLevel;  // -1=全部, 0=不展开, N=N层
  bool               hasExpand;    // 是否使用了 EXPAND 关键字
} SVirtualTableNode;
```

### 4.2 Translator 处理

**文件**: `source/libs/parser/src/parTranslater.c`

在 `translateVirtualSuperTable()` 中处理 EXPAND：

```c
static int32_t translateVirtualSuperTableWithExpand(STranslateContext* pCxt,
                                                     SVirtualTableNode* pVTable) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t expandLevel = pVTable->expandLevel;

  if (expandLevel == 0) {
    // 不展开，走原有逻辑
    return translateVirtualSuperTable(pCxt, pVTable);
  }

  // 1. 获取当前 VST 的继承树（通过 catalog 查询 ins_inherits）
  SArray* pDescendants = NULL;  // SArray<SVstInheritInfo>
  code = catalogGetVstDescendants(pCxt->pParseCxt->pCatalog,
                                   pVTable->pMeta->suid,
                                   expandLevel,
                                   &pDescendants);

  // 2. 计算列并集（所有子孙 VST 列的 union），不含 VCT 私有列
  SNodeList* pUnionCols = NULL;
  code = buildExpandedColumnList(pCxt, pVTable, pDescendants, &pUnionCols);

  // 3. 将展开信息传递给 planner
  pVTable->pExpandDescendants = pDescendants;
  pVTable->pExpandCols = pUnionCols;

  return code;
}
```

**列并集规则**：
- 所有子孙 VST 的列取并集，相同 colId 只出现一次
- 某 VCT 不具有某列时填 NULL
- VCT 私有列（PRIVATE 定义的列）在 VST EXPAND 查询中不可见

### 4.3 Planner 扩展

**文件**: `source/libs/planner/src/planLogicCreater.c`

#### 4.3.1 SVirtualScanLogicNode 扩展

**文件**: `include/libs/nodes/plannodes.h`

```c
typedef struct SVirtualScanLogicNode {
  // ... 现有字段 ...

  // ---- 新增 ----
  int32_t    expandLevel;       // EXPAND 层数
  SNodeList* pExpandSuids;      // 展开的子孙 VST suid 列表
  SNodeList* pExpandCols;       // 列并集 schema
} SVirtualScanLogicNode;
```

**注意**：在 `nodesCloneFuncs.c` 中添加对应 `COPY_SCALAR_FIELD(expandLevel)` 和 `CLONE_NODE_LIST_FIELD(pExpandSuids)` / `CLONE_NODE_LIST_FIELD(pExpandCols)`。

#### 4.3.2 Logic Plan 创建

在 `createVirtualSuperTableLogicNode()` 中：

```c
if (pVTable->expandLevel != 0) {
  pScanNode->expandLevel = pVTable->expandLevel;
  pScanNode->pExpandSuids = pVTable->pExpandDescendants;
  pScanNode->pExpandCols = pVTable->pExpandCols;
}
```

#### 4.3.3 SDynQueryCtrlVtbScan 扩展

**文件**: `include/libs/nodes/plannodes.h`

```c
typedef struct SDynQueryCtrlVtbScan {
  // ... 现有字段 ...

  // ---- 新增 ----
  int32_t    expandLevel;       // EXPAND 层数
  SNodeList* pExpandSuids;      // 要展开的子孙 suid 列表
} SDynQueryCtrlVtbScan;
```

#### 4.3.4 Physical Plan 创建

`SVirtualScanPhysiNode` 同步增加 `expandLevel` 和 `pExpandSuids`。在 `planPhysiCreater.c` 的 `createVirtualTableScanPhysiNode()` 中传递这些字段。

### 4.4 Executor 扩展

**文件**: `source/libs/executor/src/dynqueryctrloperator.c`

#### 4.4.1 Child Table Map 构建

核心改造在 `buildVirtualSuperTableScanChildTableMap()` 中。当 `expandLevel != 0` 时：

```
buildVirtualSuperTableScanChildTableMap()
  ├── 查询当前 VST 的 ins_virtual_child_columns → 当前 VST 的 VCT 列表
  └── if expandLevel != 0:
       ├── 获取 pExpandSuids 列表中每个子孙 VST
       ├── 对每个子孙 VST 查询 ins_virtual_child_columns → 子孙 VCT 列表
       ├── 合并到 childTableList / childTableMap
       └── 对每个 VCT 的 colRef 信息做列映射：
            ├── 子孙 VST 有而父 VST 没有的列 → 输出时填 NULL
            └── 父 VST 有而子孙 VST 没有的列 → 输出时填 NULL
```

#### 4.4.2 列映射与 NULL 填充

新增 `SVtbExpandColMapping` 结构：

```c
typedef struct SVtbExpandColMapping {
  int32_t  outputSlotId;    // 在输出 DataBlock 中的 slot
  int32_t  sourceSlotId;    // 在源 VCT DataBlock 中的 slot，-1 表示需填 NULL
  int8_t   dataType;        // 列类型
  int32_t  dataBytes;       // 列长度
} SVtbExpandColMapping;
```

在 `vtbScanNext()` 中：
- 若非 EXPAND 模式，行为不变
- 若 EXPAND 模式，从 VCT 获取的 DataBlock 需经过列映射，缺失列填 NULL 后输出

#### 4.4.3 VCT 私有列过滤

EXPAND 查询时，构建 childTableList 的 colRef 信息时需排除 VCT 私有列。
在 `sysTableScanUserVcCols()` 的查询结果中，VCT 私有列有 `isPrivate` 标记（或通过 colId 范围区分），EXPAND 逻辑中跳过这些列。

---

## 5. 系统表 ins_inherits

### 5.1 定义

**文件**: `include/common/systable.h`

```c
#define TSDB_INS_TABLE_INHERITS  "ins_inherits"
```

**Schema**：

| 列名 | 类型 | 说明 |
|------|------|------|
| parent_db | VARCHAR(64) | 父 VST 数据库名 |
| parent_stable | VARCHAR(192) | 父 VST 名 |
| parent_uid | BIGINT | 父 VST UID |
| child_db | VARCHAR(64) | 子 VST 数据库名 |
| child_stable | VARCHAR(192) | 子 VST 名 |
| child_uid | BIGINT | 子 VST UID |
| depth | INT | 子 VST 继承深度 |
| create_time | TIMESTAMP | 子 VST 创建时间 |

### 5.2 实现

**文件**: `source/dnode/mnode/impl/src/mndInfoSchema.c`

注册 `ins_inherits` 系统表 schema。数据源为遍历 SDB 中所有 `SStbObj`，筛选 `parentSuid != 0` 的记录。

**文件**: `source/libs/executor/src/sysscanoperator.c`

新增 `sysTableScanVstInherits()` 函数处理 `ins_inherits` 的查询请求。

### 5.3 SHOW 语法

```sql
SHOW VSTABLE INHERITS;
-- 等价于 SELECT * FROM information_schema.ins_inherits;
```

在 `include/common/tmsg.h` 新增：
```c
QUERY_NODE_SHOW_VST_INHERITS_STMT,
```

在 Parser 中将 `SHOW VSTABLE INHERITS` 重写为对 `ins_inherits` 的 SELECT。

---

## 6. DCL 权限继承

### 6.1 创建时继承

**文件**: `source/dnode/mnode/impl/src/mndUser.c`

新增 `mndInheritStbPrivilege()`：

```c
int32_t mndInheritStbPrivilege(SMnode *pMnode, STrans *pTrans,
                                SStbObj *pParent, SStbObj *pChild) {
  // 遍历所有用户，找出对 pParent->name 有权限的用户
  // 为每个用户对 pChild->name 复制相同权限
  // 将权限变更加入事务 pTrans
}
```

### 6.2 父表权限变更时覆盖

在现有的 `mndProcessGrantReq()` / `mndProcessRevokeReq()` 中，若目标是 VST 且该 VST 有子 VST，需级联更新：

```c
if (pStb->virtualStb) {
  mndCascadePrivilegeChange(pMnode, pTrans, pStb, grantType);
}
```

递归遍历所有 `parentSuid == pStb->uid` 的子孙 VST，对每个执行相同的权限变更。

---

## 7. Catalog 缓存

**文件**: `source/libs/catalog/`

### 7.1 缓存继承信息

在 `STableMeta` 或辅助结构中缓存：
- `parentSuid`、`inheritDepth`
- 继承链的列映射关系

### 7.2 新增 Catalog API

```c
// 获取 VST 的所有子孙（递归或按层数）
int32_t catalogGetVstDescendants(SCatalog* pCtg, uint64_t suid,
                                  int32_t expandLevel,
                                  SArray** ppDescendants);

// 获取继承树的列并集
int32_t catalogGetVstExpandedSchema(SCatalog* pCtg, uint64_t suid,
                                     int32_t expandLevel,
                                     SArray** ppColumns);
```

实现方式：查询 `ins_inherits` 系统表获取子孙列表，再逐个获取 schema 并合并。

---

## 8. 错误码

**文件**: `include/util/taoserror.h`

```c
#define TSDB_CODE_MND_VST_HAS_CHILDREN        TAOS_DEF_ERROR_CODE(0, 0x03E0)  // "VST has child virtual stables"
#define TSDB_CODE_MND_VST_INHERIT_DEPTH_EXCEED TAOS_DEF_ERROR_CODE(0, 0x03E1)  // "VST inheritance depth exceeds limit"
#define TSDB_CODE_MND_VST_CIRCULAR_INHERIT     TAOS_DEF_ERROR_CODE(0, 0x03E2)  // "Circular VST inheritance detected"
#define TSDB_CODE_MND_VST_PARENT_NOT_VIRTUAL   TAOS_DEF_ERROR_CODE(0, 0x03E3)  // "Parent table is not a virtual stable"
#define TSDB_CODE_MND_VST_COL_NAME_CONFLICT    TAOS_DEF_ERROR_CODE(0, 0x03E4)  // "Column name conflicts with parent VST"
#define TSDB_CODE_PAR_VST_EXPAND_NO_INHERIT    TAOS_DEF_ERROR_CODE(0, 0x2680)  // "EXPAND used on non-inherited VST"
```

---

## 9. 序列化兼容性

所有新增消息字段采用尾部追加策略：

| 组件 | 策略 |
|------|------|
| `SStbObj` SDB 编解码 | `sver < STB_VER_SUPPORT_INHERIT` 时新字段默认 0 |
| `SMCreateStbReq` 网络协议 | 反序列化时 buffer 不足则默认 0 |
| `SVCreateStbReq` 编解码 | `tEncodeSVCreateStbReq` 追加字段，解码端兼容缺失 |
| `nodesCloneFuncs.c` | 新增字段对应 `COPY_SCALAR_FIELD` / `CLONE_NODE_LIST_FIELD` |
| `nodesCodeFuncs.c` | 新增字段的 JSON 序列化/反序列化 |

---

## 10. 核心流程总结

### 10.1 CREATE VIRTUAL STABLE child BASE ON parent

```
Client
  │  SQL: CREATE VIRTUAL STABLE child BASE ON parent (extra INT) TAGS (loc NCHAR(64))
  ▼
Parser (parAstCreater.c)
  │  创建 SCreateVTableStmt { hasParent=true, parentTableName="parent", pNewCols=[extra], pNewTags=[loc] }
  ▼
Translator (parTranslater.c)
  │  translateCreateVirtualStableInherit()
  │  ├── catalog 获取 parent meta
  │  ├── 校验：virtualStb、depth <= 10、无循环、无列名冲突
  │  ├── 合并 schema：finalCols = parent.cols + [extra], finalTags = parent.tags + [loc]
  │  └── 构建 SMCreateStbReq { parentSuid=parent.uid, ... }
  ▼
MNode (mndStb.c)
  │  mndProcessCreateStbReq()
  │  ├── 获取父 SStbObj，再次校验
  │  ├── 构建子 SStbObj { parentSuid, parentDbUid, inheritDepth=parent.depth+1, ownColStart, ownTagStart }
  │  ├── 权限继承 mndInheritStbPrivilege()
  │  └── 事务提交（写 SDB + 下发 VNode）
  ▼
VNode
  │  metaCreateSTable()：存储带继承字段的 STB 元数据
  ▼
完成
```

### 10.2 SELECT ... FROM vst EXPAND(-1)

```
Client
  │  SQL: SELECT * FROM vst_root EXPAND(-1)
  ▼
Parser
  │  SVirtualTableNode { expandLevel=-1, hasExpand=true }
  ▼
Translator
  │  translateVirtualSuperTableWithExpand()
  │  ├── catalogGetVstDescendants(suid, -1) → [vst_mid, vst_mid2]
  │  ├── buildExpandedColumnList() → {ts, val, extra, temp} (列并集)
  │  └── 设置 pExpandDescendants、pExpandCols
  ▼
Planner
  │  createVirtualSuperTableLogicNode()
  │  ├── SVirtualScanLogicNode { expandLevel=-1, pExpandSuids=[mid_uid, mid2_uid] }
  │  └── SDynQueryCtrlVtbScan { expandLevel=-1, pExpandSuids=[...] }
  ▼
Executor
  │  buildVirtualSuperTableScanChildTableMap()
  │  ├── 查询 vst_root 的 VCT → vct_r1, vct_r2
  │  ├── 查询 vst_mid 的 VCT → vct_m1
  │  ├── 查询 vst_mid2 的 VCT → vct_m2
  │  └── 合并 childTableList = [vct_r1, vct_r2, vct_m1, vct_m2]
  │
  │  vtbScanNext() 循环
  │  ├── 逐个 VCT 获取数据
  │  ├── 经列映射（缺失列填 NULL）
  │  └── 输出 DataBlock {ts, val, extra, temp}
  ▼
结果返回
```

---

## 11. 涉及文件清单

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `include/common/tmsg.h` | 修改 | `SMCreateStbReq`/`SVCreateStbReq` 追加字段，新增 `QUERY_NODE_SHOW_VST_INHERITS_STMT` |
| `include/common/systable.h` | 修改 | 新增 `TSDB_INS_TABLE_INHERITS` |
| `include/common/taoserror.h` | 修改 | 新增继承相关错误码 |
| `include/libs/nodes/cmdnodes.h` | 修改 | `SCreateVTableStmt` 扩展继承字段 |
| `include/libs/nodes/querynodes.h` | 修改 | `SVirtualTableNode` 增加 `expandLevel` |
| `include/libs/nodes/plannodes.h` | 修改 | `SVirtualScanLogicNode`/`SDynQueryCtrlVtbScan`/`SVirtualScanPhysiNode` 扩展 |
| `source/dnode/mnode/impl/inc/mndDef.h` | 修改 | `SStbObj` 新增继承字段 |
| `source/dnode/mnode/impl/src/mndStb.c` | 修改 | 编解码、CREATE/DROP/ALTER 继承逻辑 |
| `source/dnode/mnode/impl/src/mndUser.c` | 修改 | 权限继承逻辑 |
| `source/dnode/mnode/impl/src/mndInfoSchema.c` | 修改 | 注册 `ins_inherits` |
| `source/libs/parser/src/parAstCreater.c` | 修改 | `BASE ON` / `EXPAND` AST 创建 |
| `source/libs/parser/src/parTranslater.c` | 修改 | 继承校验、EXPAND schema 合并 |
| `source/libs/planner/src/planLogicCreater.c` | 修改 | EXPAND 逻辑计划 |
| `source/libs/planner/src/planPhysiCreater.c` | 修改 | EXPAND 物理计划 |
| `source/libs/nodes/src/nodesCloneFuncs.c` | 修改 | 新增字段 clone |
| `source/libs/nodes/src/nodesCodeFuncs.c` | 修改 | 新增字段序列化 |
| `source/libs/executor/src/dynqueryctrloperator.c` | 修改 | EXPAND childTableMap 构建、列映射 |
| `source/libs/executor/src/sysscanoperator.c` | 修改 | `ins_inherits` 查询实现 |
| `source/libs/catalog/` | 修改 | 新增 `catalogGetVstDescendants()` 等 API |
