# Tag-Ref 设计文档

## 1. 背景

JIRA：TS-XXXXX

在 TDengine 中，虚拟表（VTABLE）是一种逻辑表，它将一个或多个底层物理表的数据和标签映射到一个统一的查询视图中。在 tag-ref 功能之前，虚拟表的标签列只能使用字面值（literal value）——即在 `CREATE VTABLE` 时静态定义的固定值。这意味着：
- 如果源表的标签值发生变化，虚拟表中的标签值不会自动更新，导致数据过期
- 同一个标签值可能需要在多处重复维护，存在数据冗余
- 无法直接通过虚拟表查询源表的动态标签信息

**Tag-ref（标签引用）** 功能允许虚拟表的标签列动态引用（reference）源表中子表的标签列。标签值不再以静态副本存储，而是在查询时从源表实时获取。

SQL 示例：

```sql
-- 源超级表
CREATE STABLE src_stb (ts TIMESTAMP, val INT) TAGS (city NCHAR(20), pop INT);

-- 源子表
CREATE TABLE sub_t0 USING src_stb TAGS ('beijing', 2154);

-- 创建虚拟超级表
CREATE STABLE vst (ts TIMESTAMP, val INT) TAGS (t_city NCHAR(20), t_pop INT) VIRTUAL 1;

-- 创建虚拟子表，标签引用源子表的标签
CREATE VTABLE vtb (val FROM src_stb.sub_t0.val) USING vst
  TAGS (t_city FROM src_stb.sub_t0.city, t_pop FROM src_stb.sub_t0.pop);

-- 查询时动态获取源表标签值
SELECT t_city, t_pop FROM vtb;  -- 返回 'beijing', 2154
```

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
|------|------|--------|-------------|
| 2026/03/23 | 0.1 | - | 初稿，虚拟超级表 tag-ref 支持 |
| 2026/03/26 | 0.2 | - | PDA 优化器适配，scalar 函数增强 |
| 2026/03/27 | 0.3 | - | 修复 PDA 崩溃、TagRefSource rescan、COUNT(*) 零行问题 |
| 2026/03/28 | 0.4 | - | VSTB 标签条件拆分（tbname→SystemScan，tag-ref→VirtualScan） |
| 2026/03/30 | 0.5 | - | 分层 catalog 解析、查询规划与执行、协议与错误码 |
| 2026/04/01 | 0.6 | - | 清理未使用代码，优化错误日志 |

## 3. 整体架构

### 3.1 核心思路

tag-ref 的核心思路是将虚拟表的标签分为两类：

1. **本地标签（Local Tags）**：没有引用关系的标签，使用字面值，通过传统 TagScan 路径获取
2. **引用标签（Referenced Tags）**：使用 `FROM` 语法引用源表标签的列，需要在查询时从源超级表的子表中实时扫描

在查询规划阶段，planner 会自动分类标签，为每个引用源创建独立的 `TagRefSource` 逻辑节点，并在执行阶段由专用的 `TagRefSource` 算子完成源表标签扫描。

### 3.2 模块交互图

```
  Parser (SQL → AST)
     │  parTranslater.c: checkTagRef(), checkAndReplaceTagRefs()
     │  验证 REF 合法性，在表元数据中记录 tagRef[] 数组
     ▼
  Catalog (元数据管理)
     │  ctgAsync.c / ctgCache.c / ctgUtil.c
     │  tagRef 元数据序列化/反序列化/缓存/传播
     ▼
  Planner (AST → LogicPlan → Optimize → Split → PhysiPlan)
     │  planLogicCreater.c: classifyTagColumns(), createTagRefSourceLogicNode()
     │  planOptimizer.c: 虚拟扫描消除、标签扫描简化、窗口/聚合下推
     │  planSpliter.c: TagRefSource 跨 vgroup 拆分
     │  planPhysiCreater.c: createTagRefSourcePhysiNode()
     ▼
  Executor (物理执行)
     │  tagrefsourceoperator.c: TagRefSource 算子（源表标签扫描）
     │  virtualtablescanoperator.c: 虚拟表扫描算子（合并 tag-ref 块）
     │  operator.c: 算子创建分发
     ▼
  Storage (TDB 元数据读取)
     metaReader API: getTableEntryByUid(), extractTagVal()
```

### 3.3 查询计划树结构

对于带 tag-ref 的虚拟超级表查询，逻辑计划树结构如下：

```
VirtualScan (虚拟表扫描)
  ├── TagRefSource_1 (扫描源表 A 的引用标签)
  │     例: 从 ts_a 扫描 city, pop
  ├── TagRefSource_2 (扫描源表 B 的引用标签)
  │     例: 从 ts_b 扫描 name, code
  ├── TagScan (扫描本地标签，如有)
  └── TableScan (扫描数据列)
```

对于不带 tag-ref 的虚拟表查询，结构与原来相同（无 TagRefSource 节点）。

## 4. DDL 语法与校验

### 4.1 DDL 语法

创建带 tag-ref 的虚拟子表：

```sql
CREATE VTABLE vtb_name
  (col_def [, col_def ...])
  USING vst_name
  TAGS (
    tag_name FROM [db_name.]source_stb.source_ctb.source_tag,  -- 指定标签名引用
    ...
  );
```

支持的引用语法形式：

| 语法形式 | 示例 | 说明 |
|----------|------|------|
| 指定标签名引用 | `TAGS (city FROM db.stb.ctb.tag)` | 明确指定虚拟表的哪个标签引用哪个源标签 |
| 混合标签定义 | `TAGS (100, city FROM db.stb.ctb.tag)` | 字面值与引用混合使用 |

### 4.2 DDL 校验规则

校验在 Parser 层（`parTranslater.c` 的 `checkTagRef()` 函数）完成：

1. **引用目标必须是子表**：`FROM` 后面指定的源表必须是子表（`TSDB_CHILD_TABLE`），不能是超级表或普通表
2. **引用列必须是标签列**：引用的列必须是源子表的标签列，不能是普通数据列
3. **类型必须匹配**：虚拟表标签的类型必须与源标签的类型完全一致（包括变长类型的长度）
4. **主时间戳列不能有引用**：虚拟表的主时间戳列不支持引用（`TSDB_CODE_VTABLE_PRIMTS_HAS_REF`）

### 4.3 错误码

| 错误码 | 含义 |
|--------|------|
| `TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR` (0x6200) | 虚拟表扫描内部错误 |
| `TSDB_CODE_VTABLE_SCAN_INVALID_DOWNSTREAM` (0x6201) | 无效的下游算子 |
| `TSDB_CODE_VTABLE_PRIMTS_HAS_REF` (0x6202) | 主时间戳列不允许有引用 |
| `TSDB_CODE_VTABLE_NOT_VIRTUAL_SUPER_TABLE` (0x6203) | 目标不是虚拟超级表 |
| `TSDB_CODE_VTABLE_TOO_MANY_REFERENCE` (0x6209) | 引用数量超限 |
| `TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED` (0x620C) | 引用深度超限 |
| `TSDB_CODE_VTABLE_INVALID_REF_COLUMN` (0x620D) | 无效的引用列 |
| `TSDB_CODE_PAR_INVALID_REF_COLUMN` (0x268D) | 引用列不存在或不合法 |
| `TSDB_CODE_PAR_INVALID_REF_COLUMN_TYPE` (0x268F) | 引用列类型不匹配 |

## 5. 元数据模型

### 5.1 核心数据结构

#### SColRef（标签引用描述符）

定义在 `include/common/tmsg.h`，描述单个标签列的引用关系：

```c
typedef struct SColRef {
  bool    hasRef;            // 是否为引用标签
  col_id_t id;               // 虚拟表中的列 ID
  char    refDbName[TSDB_DB_NAME_LEN];     // 源数据库名
  char    refTableName[TSDB_TABLE_NAME_LEN]; // 源表名
  char    refColName[TSDB_COL_NAME_LEN];   // 源列名
} SColRef;
```

虚拟表的元数据（`STableMetaRsp`）中包含：
- `pTagRefs`：`SColRef` 数组，记录每个标签的引用关系
- `numOfTagRefs`：引用标签的数量

`hasRef=false` 的标签是本地标签，`hasRef=true` 的标签是引用标签。

#### STagRefColumn（计划节点中的引用列映射）

定义在 `include/libs/nodes/plannodes.h`：

```c
typedef struct STagRefColumn {
  ENodeType type;                // QUERY_NODE_TAG_REF_COLUMN
  col_id_t  colId;               // 虚拟表中的列 ID
  col_id_t  sourceColId;         // 源表中的标签列 ID
  char      colName[TSDB_COL_NAME_LEN];       // 虚拟表列名
  char      sourceColName[TSDB_COL_NAME_LEN]; // 源表列名
  int32_t   bytes;               // 列宽度
  int8_t    dataType;            // 数据类型
} STagRefColumn;
```

### 5.2 新增节点类型

| 节点类型 | 枚举值 | 用途 |
|----------|--------|------|
| `QUERY_NODE_TAG_REF_COLUMN` | 叶子节点 | 描述单个标签引用映射 |
| `QUERY_NODE_LOGIC_PLAN_TAG_REF_SOURCE` | 逻辑计划节点 | 扫描源表获取引用标签值 |
| `QUERY_NODE_PHYSICAL_PLAN_TAG_REF_SOURCE` | 物理计划节点 | TagRefSource 物理执行节点 |

### 5.3 Catalog 层元数据传播

tag-ref 元数据通过 Catalog 模块在集群节点间传播：

1. **序列化**：`pTagRefs` 数组随表 schema 一起序列化，存储在 schema 数据之后
2. **缓存**：Catalog 缓存中包含 tagRef 信息，通过 `ctgCache.c` 管理
3. **异步获取**：通过 `ctgAsync.c` 异步获取远程节点的 tagRef 元数据
4. **校验**：查询时通过 `ctgUtil.c` 中的工具函数读取和校验 tagRef 信息

### 5.4 新增计划节点结构

#### STagRefSourceLogicNode（逻辑计划节点）

```c
typedef struct STagRefSourceLogicNode {
  SLogicNode    node;
  SName         sourceTableName;    // 源表名 (db.table)
  uint64_t      sourceSuid;         // 源超级表 UID
  int32_t       sourceId;           // 源标识
  SNodeList*    pRefCols;           // STagRefColumn 列表
  SVgroupsInfo* pVgroupList;        // 源表 vgroup 分布
  bool          isUsedInFilter;     // 是否用于过滤条件
  bool          isUsedInProjection; // 是否用于投影
} STagRefSourceLogicNode;
```

#### STagRefSourcePhysiNode（物理计划节点）

```c
typedef struct STagRefSourcePhysiNode {
  SPhysiNode    node;
  SName         sourceTableName;
  uint64_t      sourceSuid;
  int32_t       sourceId;
  SNodeList*    pRefCols;
  SVgroupsInfo* pVgroupList;
  SNodeList*    pScanCols;          // 实际扫描列节点
  bool          isUsedInFilter;
  bool          isUsedInProjection;
} STagRefSourcePhysiNode;
```

#### SVirtualScanLogicNode / SVirtualScanPhysiNode 扩展字段

```c
// 新增的 tag-ref 相关字段
SNodeList*    pTagRefSources;   // TagRefSource 节点列表
SNodeList*    pLocalTags;       // 本地标签（无引用）
SNodeList*    pRefTagCols;      // 引用标签列
SNode*        pTagFilterCond;   // 标签过滤条件
bool          hasTagRef;        // 是否有引用标签
bool          hasLocalTag;      // 是否有本地标签
```

## 6. Planner 层设计

### 6.1 逻辑计划创建

入口函数：`createVirtualSuperTableLogicNode()` 和 `createVirtualNormalChildTableLogicNode()`（`planLogicCreater.c`）

#### 6.1.1 标签分类（classifyTagColumns）

在创建虚拟超级表的逻辑计划时，首先对标签进行分类：

1. 遍历虚拟表扫描的伪列（pseudo columns）中的所有标签列
2. 对每个标签列，通过 `findTagRefIndex()` / `findTagRefIndexByName()` 查找元数据中的 `tagRef[]` 数组
3. 如果 `tagRef[i].hasRef == true`，归入引用标签组（`pRefTagCols`）
4. 否则归入本地标签组（`pLocalTags`）
5. 引用标签按源表分组，存储在 `pRefSourceMap` 哈希表中

分类结果存储在 `STagClassifyResult` 结构中：

```c
typedef struct STagClassifyResult {
  SNodeList* pLocalTags;      // 本地标签
  SNodeList* pRefTagCols;     // 引用标签列
  SHashObj*  pRefSourceMap;   // 按源表分组的引用映射
} STagClassifyResult;
```

#### 6.1.2 TagRefSource 节点创建（createTagRefSourceLogicNode）

对于每个在查询中实际使用（WHERE / SELECT / GROUP BY 等）的引用标签：

1. 从 `SColRef` 元数据解析源表名和超级表 UID
2. 克隆源表的 vgroup 分布列表
3. 创建 `STagRefColumn`，建立虚拟表列 ID 到源表标签列 ID 的映射
4. 构建输出目标列（供下游交换节点使用）
5. 通过 `extractFilterConditionForSingleRef()` 从 WHERE 子句中提取相关过滤条件
6. 设置 `isUsedInFilter` / `isUsedInProjection` 标志

#### 6.1.3 过滤条件提取

- `extractFilterConditionForSingleRef()`：从 WHERE 子句中提取引用特定标签列的条件
- `extractFilterConditionForRefs()`：汇总所有引用标签的过滤条件
- 提取过程在 AND 条件下逐个参数进行 walker 遍历
- 提取的条件设置在每个 TagRefSource 节点和虚拟扫描节点的 `pTagFilterCond` 上

#### 6.1.4 构建虚拟扫描树（buildVirtualScanTree）

最终将 TagRefSource 节点作为 VirtualScan 的子节点附加：

```
VirtualScan
  ├── TagRefSource_1 (源表 A 的引用标签)
  ├── TagRefSource_2 (源表 B 的引用标签)
  ├── TagScan (本地标签)
  └── TableScan (数据列)
```

### 6.2 优化规则

`planOptimizer.c` 中有多个优化规则与 tag-ref 相关：

#### 6.2.1 VSTB 条件拆分（pdcDealVirtualSuperTableScan）

对于虚拟超级表，将 WHERE 条件拆分：
- 纯 `tbname` 条件（使用 `FUNCTION_TYPE_TBNAME`，不包含 `COLUMN_TYPE_TAG` 列）→ 推送到 SystemScan
- 包含 `COLUMN_TYPE_TAG` 列的条件 → 保留在 VirtualScan（因为超级表元数据中没有每个标签的引用信息，任何标签都可能是引用标签）

#### 6.2.2 虚拟扫描消除（eliminateVirtualScanOptimize）

当 VirtualScan 只有一个子节点且所有扫描列都有 `hasRef=true`（因此可以从下游解析）时，移除虚拟扫描层。

#### 6.2.3 标签扫描简化（vtableTagScanOptimize）

当虚拟表查询是纯标签聚合时（GROUP BY tbname，无聚合函数，无普通列），消除 DynQueryCtrl + VirtualScan 层，简化为直接 TagScan。

#### 6.2.4 窗口/聚合下推

- `vtableWindowOptimize()` / `vstableWindowOptimize()`：将窗口操作下推到源表扫描
- `vstableAggOptimize()`：将聚合操作下推到源表扫描（含分区感知和无分区变体）

#### 6.2.5 PDA 重构排除

TagRefSource 子节点被排除在 PDA（谓词下推/聚合）重构之外（`planOptimizer.c` 第 8841 行），避免将不适用的优化规则应用于标签引用源节点。

### 6.3 拆分阶段（planSpliter.c）

TagRefSource 节点可能需要跨 vgroup 拆分：

1. **VirtualScan 子节点下的 TagRefSource**：由 `VirtualtableSplit` 统一处理，为所有子节点创建交换节点和子计划。不经过 SuperTableSplit。

2. **独立的 TagRefSource**：由 `stbSplSplitTagRefSourceNode()` 处理，为每个 vgroup 创建交换节点和扫描子计划。

3. **vgroup 信息**：`splSetSubplanVgroups()` 为 `TAG_REF_SOURCE` 节点从节点本身（而非父扫描节点）获取 vgroup 分布，因为源表可能与虚拟表位于不同的 vgroup 中。

### 6.4 物理计划创建

`createTagRefSourcePhysiNode()`（`planPhysiCreater.c`）将逻辑节点转换为物理节点：

1. 复制源表名、suid、sourceId
2. 克隆引用列列表
3. 处理 vgroup 列表（可能已在拆分阶段转移到子计划中）
4. 从 `STagRefColumn` 条目创建 `pScanCols`——列节点使用 `hasRef=true` 和匹配的 `refDbName/refTableName/refColName`，确保 `getSlotKey()` 生成一致的 slot key
5. 调用 `addDataBlockSlots()` 和 `setConditionsSlotId()` 设置数据块描述符

### 6.5 节点序列化

完整的生命周期支持：

| 操作 | 文件 | 函数 |
|------|------|------|
| JSON 序列化 | `nodesCodeFuncs.c` | `tagRefColumnToJson()`, `logicTagRefSourceNodeToJson()`, `physiTagRefSourceNodeToJson()` |
| JSON 反序列化 | `nodesCodeFuncs.c` | `jsonToTagRefColumn()`, `jsonToLogicTagRefSourceNode()`, `jsonToPhysiTagRefSourceNode()` |
| 二进制 TLV 序列化 | `nodesMsgFuncs.c` | `tagRefColumnToMsg()`, `physiTagRefSourceNodeToMsg()` |
| 二进制 TLV 反序列化 | `nodesMsgFuncs.c` | `msgToTagRefColumn()`, `msgToPhysiTagRefSourceNode()` |
| 节点克隆 | `nodesCloneFuncs.c` | `tagRefColumnCopy()`, `logicTagRefSourceCopy()`, `physiTagRefSourceCopy()` |
| 节点销毁 | `nodesUtilFuncs.c` | 各节点类型的 destroy case |

## 7. Executor 层设计

### 7.1 TagRefSource 算子

文件：`source/libs/executor/src/tagrefsourceoperator.c`

#### 7.1.1 算子信息结构

```c
typedef struct STagRefSourceOperatorInfo {
  SSDataBlock*     pRes;              // 结果数据块
  uint64_t         sourceSuid;        // 源超级表 UID
  SName            sourceTableName;   // 源表名
  SNodeList*       pRefCols;          // STagRefColumn 列表
  SNodeList*       pScanCols;         // 扫描列列表
  SVgroupsInfo*    pVgroupList;       // 源表 vgroup 分布
  int32_t          curPos;            // 当前扫描位置
  bool             scanCompleted;     // 扫描是否完成
  SReadHandle      readHandle;        // 读取句柄
  STableListInfo*  pTableListInfo;    // 表列表信息
  SStorageAPI*     pStorageAPI;       // 存储层 API
  SNode*           pTagCond;          // 标签过滤条件
  SNode*           pTagIndexCond;     // 标签索引条件
  SColMatchInfo    matchInfo;         // 列匹配信息
  SLimitInfo       limitInfo;         // limit/offset 信息
  int32_t          capacity;          // 数据块容量（默认 4096）
} STagRefSourceOperatorInfo;
```

#### 7.1.2 执行流程

TagRefSource 算子是**叶子算子**（无下游算子），执行流程如下：

1. **初始化**：`createTagRefSourceOperatorInfo()` 创建算子
   - 通过 `getTableEntryByName()` 查找源表元数据
   - 构造 `SScanPhysiNode`，设置 uid、suid、tableType、tableName
   - 调用 `initQueriedTableSchemaInfo()` 初始化 schema
   - 调用 `createScanTableListInfo()` 获取源超级表下所有子表列表
   - 从物理节点克隆 pRefCols、pScanCols、pVgroupList
   - 创建结果数据块，初始化列匹配信息和过滤条件

2. **逐表扫描**：`tagRefSourceGetNext()` 主扫描循环
   - 初始化 `SMetaReader`
   - 遍历 `pTableListInfo` 中的所有子表
   - 直到结果块满（4096 行）或全部表扫描完成

3. **单表标签读取**：`tagRefSourceScanOneTable()`
   - 通过 `metaReaderFn.getTableEntryByUid()` 获取子表元数据
   - 遍历 `pRefCols`（STagRefColumn 列表）
   - 对每个引用列，使用 `sourceColId` 通过 `metaFn.extractTagVal()` 从子表标签中提取值
   - 通过 `colDataSetVal()` 将值写入结果块的对应槽位

4. **扫描完成**：当 `curPos >= tableCount` 时，设置 `OP_EXEC_DONE` 标志

5. **应用限制**：通过 `applyLimitOffset()` 处理 limit/offset

#### 7.1.3 算子生命周期

| 阶段 | 函数 | 说明 |
|------|------|------|
| 创建 | `createTagRefSourceOperatorInfo()` | 分配结构，初始化 schema 和表列表 |
| 打开 | `tagRefSourceOpen()` | 重置 curPos，清空结果块 |
| 执行 | `tagRefSourceGetNext()` | 主扫描循环 |
| 关闭 | `tagRefSourceClose()` | 记录统计信息 |
| 销毁 | `destroyTagRefSourceOperatorInfo()` | 释放结果块、列列表、vgroup 列表等 |

### 7.2 VirtualTableScan 算子集成

文件：`source/libs/executor/src/virtualtablescanoperator.c`

#### 7.2.1 新增字段

```c
// SVirtualTableScanInfo 新增字段
SArray*     pTagRefSourceBlockIds;   // TagRefSource 下游算子的 block ID 列表
SSHashObj*  pTagRefDownstreamMap;    // blockId → STagRefDownstreamInfo 映射
SArray*     pSavedTagRefBlocks;      // 缓存的 tag-ref 数据块

// 辅助结构
typedef struct {
  int32_t downstreamId;   // 下游算子索引
  int64_t blockId;        // 数据块 ID
} STagRefDownstreamInfo;

typedef struct {
  int64_t      blockId;   // 数据块 ID
  SSDataBlock* pBlock;    // 缓存的数据块（每个源一行）
} STagRefSavedBlock;
```

#### 7.2.2 下游算子分类

在创建排序句柄（`createSortHandle()`）时，下游算子被分为三类：

| 类型 | 判断函数 | 处理方式 |
|------|----------|----------|
| 可排序数据交换 | `isSortableDataBlockId()` | 参与排序/合并 |
| TagScan 下游 | 检查 blockId | 提供单行标签块 |
| TagRefSource 下游 | `isRefTagSourceBlockId()` | 不参与排序，单独缓存 |

TagRefSource 下游算子**不参与排序和合并**，其标签数据通过独立路径获取。

#### 7.2.3 标签引用块获取

在 `virtualTableGetNext()` 首次执行时：

1. 遍历 `pTagRefSourceBlockIds` 中的每个条目
2. 调用对应下游 TagRefSource 算子的 `getNextFn` 获取单行标签块
3. 通过 `createOneDataBlock()` 复制该块
4. 保存到 `pSavedTagRefBlocks` 以备后续重用

#### 7.2.4 标签数据填充

在 `doSetTagColumnData()` 中填充输出块的标签值时：

1. 对于本地标签：从 TagScan 缓存的 `pSavedTagBlock` 中读取
2. 对于引用标签（`pRefTagCols` 中的列）：
   - 通过 blockId 在 `pSavedTagRefBlocks` 中查找对应的缓存块
   - 从缓存块的 `pRefCol->slotId` 位置提取源列数据
   - 通过 `setTagColumnValue()` 复制到目标槽位

#### 7.2.5 清理

在算子关闭/重置/销毁时，所有 tag-ref 相关结构都会被清理：

```c
cleanupSavedTagRefBlocks(pInfo);
tSimpleHashCleanup(pInfo->pTagRefDownstreamMap);
taosArrayDestroy(pInfo->pTagRefSourceBlockIds);
```

### 7.3 DynQueryCtrl 集成

DynQueryCtrl 通过 `refType == 1` 区分标签引用列：

- 标签引用不参与虚拟扫描引用 slot 分组
- 标签引用源表不添加到 `otbNameToOtbInfoMap`（因为其值来自标签扫描路径，而非表扫描交换）
- 非标签引用列仍使用 `resolveTagValsForVtbChild()` 函数处理

## 8. 端到端数据流

以查询 `SELECT city FROM vtb WHERE city = 'beijing'` 为例（city 是引用标签，引用 ts_a 的子表标签）：

```
SQL 层:
  SELECT city FROM vtb WHERE city = 'beijing'

Parser 层:
  → 解析列引用，标记 city 列的 hasRef=true
  → 构建 AST

Planner 层:
  1. classifyTagColumns(): city 归为引用标签
  2. createTagRefSourceLogicNode(): 创建 TagRefSource 逻辑节点
     - sourceTableName = "ts_a"
     - pRefCols = [STagRefColumn{colId=X, sourceColId=Y, colName="city", sourceColName="city"}]
  3. extractFilterConditionForRefs(): 提取 city='beijing' 条件
  4. buildVirtualScanTree(): TagRefSource 作为 VirtualScan 子节点

Optimizer 层:
  - PDA 排除 TagRefSource 子节点
  - 标签条件拆分：city='beijing' → 推送到 TagRefSource

Splitter 层:
  - VirtualtableSplit 为 TagRefSource 创建交换子计划

Physical Plan 层:
  - createTagRefSourcePhysiNode(): 转换为物理节点

Executor 层:
  1. 创建 TagRefSource 算子
     - 获取 ts_a 下所有子表列表
  2. 创建 VirtualTableScan 算子
     - 识别 TagRefSource 下游
  3. 执行:
     a. TagRefSource.getNext():
        - 遍历 ts_a 的所有子表
        - 对每个子表，通过 metaReader 读取 city 标签值
        - 返回包含所有子表 city 值的 SSDataBlock
     b. VirtualTableScan:
        - 缓存 TagRefSource 返回的标签块
        - 合并数据扫描结果
        - 填充输出块的 city 列（从缓存块中提取）
        - 应用过滤条件 city='beijing'
     c. 返回结果块
```

## 9. 与普通虚拟表扫描的对比

| 维度 | 普通虚拟表扫描 | Tag-Ref 增强扫描 |
|------|---------------|------------------|
| 标签值来源 | 虚拟表元数据中的静态字面值 | 从源表实时获取 |
| 下游算子 | DataScan + TagScan | DataScan + TagScan + TagRefSource |
| 计划节点 | VirtualScan + Scan + TagScan | VirtualScan + Scan + TagScan + TagRefSourcePhysiNode(s) |
| 跨库支持 | 标签仅来自同一数据库 | 标签可引用不同数据库的表 |
| 过滤条件下推 | 仅本地标签 | 引用标签的过滤条件也可推送到 TagRefSource |
| 拆分行为 | 标准 SuperTableSplit | TagRefSource 由 VirtualtableSplit 处理 |
| SColRef 元数据 | `tagRef[i].hasRef = false` | `tagRef[i].hasRef = true` + 源表信息 |

## 10. 限制与约束

1. **引用目标限制**：只能引用子表的标签列，不能引用超级表或普通表的列
2. **类型严格匹配**：引用标签与源标签的类型和长度必须完全一致
3. **主时间戳不可引用**：虚拟表的主时间戳列不支持引用
4. **引用深度限制**：不支持嵌套引用（引用链深度超限会报错 `TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED`）
5. **引用数量限制**：单个虚拟表的引用标签数量有上限（`TSDB_CODE_VTABLE_TOO_MANY_REFERENCE`）

## 11. 测试覆盖

| 测试文件 | 覆盖范围 |
|----------|----------|
| `test/cases/05-VirtualTables/test_vtable_tag_ref.py` | 核心回归测试：70+ 表、100+ 值校验、同库/跨库/多库/重复引用/混合/本地+引用混合场景 |
| `test/cases/05-VirtualTables/test_vtable_query_*_vtb_ref.py` | 聚合/SMA/函数/分组/投影/窗口查询专项测试（同库和跨库各一套） |
| `test/cases/05-VirtualTables/test_vtable_validate_referencing.py` | 引用合法性校验测试 |
| `test/cases/05-VirtualTables/test_vtable_query_eliminate_virtual_scan.py` | 虚拟扫描消除优化测试 |
| `tests/army/vtable/test_vtable_tag_ref.py` | 语法回归测试：旧语法/指定语法/位置语法/混合语法 |
