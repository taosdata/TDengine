# 虚拟表引用虚拟表功能设计

## 概述

### 背景
当前 TDengine 的虚拟表只能引用原始表（普通表/子表/超级表），本设计旨在支持虚拟表引用虚拟表的功能。

### 目标
1. 允许虚拟表引用其他虚拟表
2. 虚拟子表查询：客户端逐级获取源表 meta，定位到原始表的 vgid
3. 虚拟超级表查询：Executor 动态计算源表位置，更新执行顺序

### 限制
- 最大引用深度：5 层（`TSDB_MAX_VTABLE_REF_DEPTH`）
- 禁止循环引用

## 现状分析

### 已实现功能（无需修改）

| 功能 | 文件 | 函数 | 状态 |
|------|------|------|------|
| 引用类型检查 | `source/libs/parser/src/parTranslater.c` | `checkColRef()` | ✅ 已支持虚拟表类型 |
| 循环引用检测 | `source/libs/parser/src/parTranslater.c` | `detectCircularReference()` | ✅ 已实现 |
| 深度限制检查 | `source/libs/parser/src/parTranslater.c` | `checkRefDepth()` | ✅ 最大5层 |
| 递归获取源表vgroup | `source/libs/parser/src/parTranslater.c` | `getOriginalTableVgroupInfo()` | ✅ 已实现 |

### 需要增强的功能

| 模块 | 修改点 |
|------|--------|
| Catalog | 增强元数据获取，支持虚拟表引用链递归解析 |
| Planner | 增强虚拟表查询规划，使用正确的源表 vgid |
| Executor | 增强动态查询控制，支持虚拟表引用链的动态任务分发 |

## 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                        创建虚拟表流程                              │
│  ┌──────────────┐    ┌───────────────────┐    ┌──────────────┐  │
│  │ SQL 解析     │ -> │ 引用检查(已有)     │ -> │ 元数据存储    │  │
│  │              │    │ - 循环引用检测     │    │              │  │
│  │              │    │ - 深度限制(≤5)     │    │              │  │
│  └──────────────┘    └───────────────────┘    └──────────────┘  │
│                              ✅ 已实现                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      查询虚拟表流程                               │
│  ┌──────────────┐    ┌───────────────────┐    ┌──────────────┐  │
│  │ 客户端       │ -> │ 递归获取源表Meta   │ -> │ 获取源表vgid │  │
│  │ (虚拟子表)   │    │ 🔧 需增强          │    │ ✅ 已有框架  │  │
│  └──────────────┘    └───────────────────┘    └──────────────┘  │
│                                                                │
│  ┌──────────────┐    ┌───────────────────┐    ┌──────────────┐  │
│  │ Executor     │ -> │ 动态计算源表位置   │ -> │ 更新执行顺序  │  │
│  │ (虚拟超级表) │    │ 🔧 需增强          │    │ 🔧 需增强    │  │
│  └──────────────┘    └───────────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 详细设计

### 1. Catalog 层增强

#### 文件
- `source/libs/catalog/src/ctgRemote.c`
- `source/libs/catalog/src/ctgAsync.c`

#### 新增函数
```c
/**
 * @brief 递归获取虚拟表的原始源表元数据
 * @param pCtg Catalog 句柄
 * @param pDbName 当前数据库名
 * @param pTableName 当前表名
 * @param ppOriginalMeta 输出：原始表元数据
 * @param pOriginalVgid 输出：原始表 vgid
 * @param pVisited 已访问表列表（循环检测）
 * @param depth 当前递归深度
 */
static int32_t ctgGetOriginalTableMeta(SCatalog* pCtg, const char* pDbName, 
                                        const char* pTableName,
                                        STableMeta** ppOriginalMeta,
                                        SVgroupInfo* pOriginalVgid,
                                        SArray* pVisited, int32_t depth);
```

#### 实现逻辑
1. 深度检查：超过 `TSDB_MAX_VTABLE_REF_DEPTH` 返回错误
2. 循环引用检测：复用 `detectCircularReference` 逻辑
3. 获取当前表 meta
4. 如果是虚拟表，递归调用获取引用表
5. 如果是原始表，获取 vgid 并返回

### 2. Planner 层增强

#### 文件
- `source/libs/planner/src/planLogicCreater.c`

#### 修改点
在 `createVirtualSuperTableLogicNode` 函数中：
- 检查引用的源表是否为虚拟表
- 如果是虚拟表，使用 `getOriginalTableVgroupInfo` 递归获取原始表 vgroup
- 使用原始表的 vgroup 信息创建 scan 节点

### 3. Executor 层增强

#### 文件
- `source/libs/executor/src/dynqueryctrloperator.c`
- `source/libs/executor/src/virtualtablescanoperator.c`

#### 修改点
在 `dynQueryCtrlProcessVtableScan` 函数中：
- 对虚拟超级表的每个子表
- 递归获取原始源表信息
- 使用原始表的 vgid 创建 exchange 参数

## 查询流程

### 虚拟子表查询
```
用户查询: SELECT * FROM db.vtable2 (虚拟表引用虚拟表)
                    │
                    ▼
客户端 Catalog 层:
  1. ctgGetTbMeta("db.vtable2")
     - tableType = TSDB_VIRTUAL_NORMAL_TABLE
     - colRef[0].refTableName = "vtable1" (也是虚拟表)
  
  2. 递归: ctgGetTbMetaForVirtual("db", "vtable1")
     - tableType = TSDB_VIRTUAL_NORMAL_TABLE
     - colRef[0].refTableName = "original_table" (原始表)
  
  3. 递归: ctgGetTbMeta("db.original_table")
     - tableType = TSDB_NORMAL_TABLE
     - 返回 vgid = 3
  
  4. 返回完整引用链信息给 Planner
                    │
                    ▼
Planner 层:
  使用 getOriginalTableVgroupInfo() 获取源表 vgid
  生成 Exchange 节点，路由到正确的 vnode
```

### 虚拟超级表查询
```
用户查询: SELECT * FROM db.vstable WHERE ts > now() - 1h
                    │
                    ▼
Planner 层:
  1. 识别 vstable 为虚拟超级表
  2. 获取引用的源超级表信息（递归解析虚拟表引用链）
  3. 生成 Dynamic Query Control 节点
                    │
                    ▼
Executor 层 (dynqueryctrloperator.c):
  1. 动态获取虚拟超级表的子表列表
  2. 对每个虚拟子表:
     a. 递归解析获取原始源表
     b. 计算源表所在 vgid
     c. 创建对应的 scan 任务
  3. 更新执行计划，分发到正确的 vnode
  4. 合并结果返回
```

## 关键代码修改

### Catalog 层
```c
// ctgRemote.c - 新增函数
static int32_t ctgGetOriginalTableMeta(SCatalog* pCtg, const char* pDbName, 
                                        const char* pTableName,
                                        STableMeta** ppOriginalMeta,
                                        SVgroupInfo* pOriginalVgid,
                                        SArray* pVisited, int32_t depth) {
  // 1. 深度检查
  if (depth > TSDB_MAX_VTABLE_REF_DEPTH) {
    return TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED;
  }
  
  // 2. 循环引用检测
  // ... 复用 detectCircularReference 逻辑
  
  // 3. 获取当前表 meta
  STableMeta* pMeta = NULL;
  int32_t code = ctgGetTbMetaFromMnode(pCtg, pDbName, pTableName, &pMeta);
  
  // 4. 判断是否为虚拟表
  if (isVirtualTable(pMeta)) {
    // 递归获取引用表的原始表
    return ctgGetOriginalTableMeta(pCtg, 
                                    pMeta->colRef[0].refDbName,
                                    pMeta->colRef[0].refTableName,
                                    ppOriginalMeta, pOriginalVgid,
                                    pVisited, depth + 1);
  }
  
  // 5. 找到原始表，获取 vgid
  *ppOriginalMeta = pMeta;
  return ctgGetVgroupInfo(pCtg, pDbName, pTableName, pOriginalVgid);
}
```

### Planner 层
```c
// planLogicCreater.c - 修改 createVirtualSuperTableLogicNode
static int32_t createVirtualSuperTableLogicNode(...) {
  // 获取引用的源表
  SRealTableNode* pRefTable = nodesListGetNode(pVirtualTable->refTables, 0);
  
  // 新增：检查源表是否也是虚拟表
  if (isVirtualTable(pRefTable->pMeta)) {
    // 使用 getOriginalTableVgroupInfo 递归获取原始表的 vgroup
    SVgroupInfo originalVgInfo = {0};
    SArray* pVisitedTables = taosArrayInit(16, sizeof(char*), NULL, taosMemoryFree);
    code = getOriginalTableVgroupInfo(pCxt, pRefTable->table.dbName,
                                       pRefTable->table.tableName,
                                       &originalVgInfo, pVisitedTables);
    taosArrayDestroy(pVisitedTables);
    // 使用原始表的 vgroup 信息创建 scan 节点
  }
  // ... 其余逻辑
}
```

### Executor 层
```c
// dynqueryctrloperator.c - 增强 dynQueryCtrlProcessVtableScan
static int32_t dynQueryCtrlProcessVtableScan(...) {
  // 对虚拟超级表的每个子表
  for (int32_t i = 0; i < numOfVtables; i++) {
    SVirtualChildTableInfo* pVChild = taosArrayGet(pVtableList, i);
    
    // 新增：递归获取原始源表信息
    SVgroupInfo originalVgInfo = {0};
    code = getOriginalVgroupForVtable(pVChild->dbName, pVChild->tableName, 
                                       &originalVgInfo);
    
    // 使用原始表的 vgid 创建 exchange 参数
    SVTableScanOperatorParam* pVScan = taosMemoryMalloc(sizeof(...));
    pVScan->vgid = originalVgInfo.vgId;
    // ...
  }
}
```

## 测试计划

### 单元测试
1. 循环引用检测测试
2. 引用深度限制测试
3. 递归 meta 获取测试

### 集成测试
```sql
-- 基本场景：虚拟普通表引用链
CREATE TABLE db.t1 (ts TIMESTAMP, v INT);
CREATE VTABLE db.vt1 REFERENCE db.t1 (ts REF t1.ts, v REF t1.v);
CREATE VTABLE db.vt2 REFERENCE db.vt1 (ts REF vt1.ts, v REF vt1.v);
SELECT * FROM db.vt2;  -- 应该能正确查询到 t1 的数据

-- 超级表场景
CREATE STABLE db.st1 (ts TIMESTAMP, v INT) TAGS (tid INT);
CREATE TABLE db.ct1 USING db.st1 TAGS (1);
CREATE VSTABLE db.vst1 REFERENCE db.st1 ...;
CREATE VTABLE db.vct1 REFERENCE db.ct1 ...;
CREATE VSTABLE db.vst2 REFERENCE db.vst1 ...;
SELECT * FROM db.vst2 WHERE ...;

-- 错误场景：循环引用
CREATE VTABLE db.vt3 (ts TIMESTAMP, v INT) 
  REFERENCE db.vt4 (ts REF vt4.ts, v REF vt4.v);
CREATE VTABLE db.vt4 (ts TIMESTAMP, v INT) 
  REFERENCE db.vt3 (ts REF vt3.ts, v REF vt3.v);
-- 应报错：循环引用

-- 错误场景：深度超限
-- 创建6层引用链，应报错
```

## 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 递归深度过大导致性能问题 | 查询延迟 | 限制最大深度为5，可配置 |
| 引用链上表被删除 | 查询失败 | 增加引用完整性检查 |
| 并发修改引用链 | 数据不一致 | 使用元数据版本机制 |

## 关键文件清单

| 文件 | 修改类型 | 说明 |
|------|----------|------|
| `source/libs/parser/src/parTranslater.c` | 无需修改 | 已支持虚拟表引用检查 |
| `source/libs/catalog/src/ctgRemote.c` | 增强 | 新增递归获取原始表函数 |
| `source/libs/catalog/src/ctgAsync.c` | 增强 | 异步版本支持 |
| `source/libs/planner/src/planLogicCreater.c` | 增强 | 虚拟表查询规划 |
| `source/libs/executor/src/dynqueryctrloperator.c` | 增强 | 动态查询控制 |
| `source/libs/executor/src/virtualtablescanoperator.c` | 增强 | 虚拟表扫描 |

## 参考代码

- 现有循环引用检测：`parTranslater.c:detectCircularReference()`
- 现有深度检查：`parTranslater.c:checkRefDepth()`
- 现有递归获取vgroup：`parTranslater.c:getOriginalTableVgroupInfo()`
