# VST Inheritance 实现总结

## 1. 功能概述

实现虚拟超级表(VST)的继承机制，允许子 VST 通过 `BASE ON` 语法继承父 VST 的列和标签定义，并通过 `EXPAND` 子句在查询时展开继承层级中的后代 VCT 数据。

### 语法示例

```sql
-- 创建父 VST
CREATE VIRTUAL STABLE parent_vst (ts TIMESTAMP, val INT) TAGS (t1 INT) VIRTUAL 1;

-- 创建子 VST，继承 parent_vst 的 ts, val, t1，新增 extra 列和 t2 标签
CREATE VIRTUAL STABLE child_vst BASE ON parent_vst (extra FLOAT) TAGS (t2 BINARY(16)) VIRTUAL 1;

-- 创建孙 VST，继承 child_vst 的所有列和标签
CREATE VIRTUAL STABLE grandchild_vst BASE ON child_vst (deep INT) TAGS (t3 INT) VIRTUAL 1;

-- 查询时展开后代
SELECT * FROM parent_vst EXPAND(-1);   -- 展开所有后代
SELECT * FROM child_vst EXPAND(1);     -- 展开1层后代
SELECT * FROM child_vst EXPAND(0);     -- 仅自身VCT（默认）
```

---

## 1.1 EXPAND 查询完整流程图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CLIENT: SQL 输入                                   │
│  SELECT val, t1, t2 FROM child_vst EXPAND(1) ORDER BY val               │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         PARSER                                            │
│  sql.y: 解析 EXPAND(1) → SRealTableNode.expandLevel = 1                 │
│  parAstCreater.c: createRealTableNodeWithExpand()                        │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       TRANSLATOR                                          │
│  parTranslater.c: translateExpandClause()                                │
│    ① 校验 child_vst 是虚拟超级表                                          │
│    ② expandLevel = 1 传播到 SSelectStmt.expandLevel                      │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        CATALOG                                            │
│  catalogGetVstDescendants(child_vst, maxDepth=1)                         │
│    → RPC: TDMT_MND_GET_VST_DESCENDANTS → mnode                          │
│    → mnode 遍历 SDB 找 parentSuid == child_vst.uid 的 STB               │
│    → 返回后代列表: ["db.grandchild_vst"]                                  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        PLANNER                                            │
│  planLogicCreater.c:                                                     │
│    SVirtualScanLogicNode.expandLevel = 1                                 │
│    SDynQueryCtrlLogicNode.expandLevel = 1                                │
│    SDynQueryCtrlLogicNode.pExpandDescendants = ["db.grandchild_vst"]     │
│                                                                          │
│  planPhysiCreater.c:                                                     │
│    SVirtualTableScanPhysiNode.expandLevel = 1                            │
│    SDynQueryCtrlPhysiNode.expandLevel = 1                                │
│    SDynQueryCtrlPhysiNode.pExpandDescendants = [...]                     │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   EXECUTOR: DynQueryCtrlOperator                          │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ 初始化阶段 (dynqueryctrloperator.c)                                 │ │
│  │                                                                    │ │
│  │ ① 构建后代哈希表 pExpandDescendantStbs                              │ │
│  │    {"db.grandchild_vst": NULL}                                     │ │
│  │                                                                    │ │
│  │ ② 构建标签名→colId映射 pExpandTagNameToColId                        │ │
│  │    {"t1": colId=2, "t2": colId=3}  (child_vst schema的colId)       │ │
│  │                                                                    │ │
│  │ ③ 系统表扫描 ins_virtual_child_columns                              │ │
│  │    扫描所有 VCT，检查每个 VCT 所属的 STB:                             │ │
│  │    • VCT属于child_vst → needCollect=true (父节点自身的VCT)            │ │
│  │    • VCT属于grandchild_vst → needExpand=true (后代的VCT)             │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │ 执行阶段: 对每个 VCT 执行查询                                        │ │
│  │                                                                    │ │
│  │  ┌─────────────────────────────────────────────────────────┐       │ │
│  │  │ 父节点 VCT (vct_c1, vct_c2) — 属于 child_vst            │       │ │
│  │  │                                                         │       │ │
│  │  │ ① 从 SColRefInfo 获取列引用信息                          │       │ │
│  │  │    val FROM db.src_c1.c1                                │       │ │
│  │  │                                                         │       │ │
│  │  │ ② resolveTagValsForVtbChild() 解析标签                   │       │ │
│  │  │    • 从 meta 读取 VCT 的 tag 数据 (STag)                │       │ │
│  │  │    • 按 colId 直接提取: t1=10, t2='hello'               │       │ │
│  │  │    • 写入 pResolvedTags 数组                            │       │ │
│  │  │                                                         │       │ │
│  │  │ ③ VirtualTableScanOperator 执行数据扫描                  │       │ │
│  │  │    • 从源表 src_c1 读取 c1 列数据                        │       │ │
│  │  │    • setTagColumnValue: 使用 pResolvedTagVal             │       │ │
│  │  │    • 输出: (ts, val=30, t1=10, t2='hello')              │       │ │
│  │  └─────────────────────────────────────────────────────────┘       │ │
│  │                                                                    │ │
│  │  ┌─────────────────────────────────────────────────────────┐       │ │
│  │  │ 子节点 VCT (vct_g1) — 属于 grandchild_vst (后代)         │       │ │
│  │  │                                                         │       │ │
│  │  │ ① 从 SColRefInfo 获取列引用信息                          │       │ │
│  │  │    val FROM db.src_g1.c1                                │       │ │
│  │  │                                                         │       │ │
│  │  │ ② resolveTagValsForVtbChild() 解析标签 【关键差异】       │       │ │
│  │  │    • 从 meta 读取 VCT 的 tag 数据 (STag)                │       │ │
│  │  │    • grandchild_vst 的 tag schema:                      │       │ │
│  │  │      t0(colId=2), t1(colId=3), t2(colId=4), t3(colId=5)│       │ │
│  │  │    • child_vst 查询的 schema:                           │       │ │
│  │  │      t1(colId=2), t2(colId=3)                           │       │ │
│  │  │    • colId 不匹配! 需要按名称映射:                        │       │ │
│  │  │                                                         │       │ │
│  │  │    ┌─ EXPAND 标签解析 (appendResolvedTagVal) ──────┐    │       │ │
│  │  │    │ 遍历 grandchild 的 tag schema:                │    │       │ │
│  │  │    │   tag "t1" → pExpandTagNameToColId 查找       │    │       │ │
│  │  │    │            → 父的 colId=2                     │    │       │ │
│  │  │    │   从 STag 按 grandchild colId=3 提取值=100    │    │       │ │
│  │  │    │   → 写入 resolvedTag{cid=2, val=100}         │    │       │ │
│  │  │    │                                              │    │       │ │
│  │  │    │   tag "t2" → pExpandTagNameToColId 查找       │    │       │ │
│  │  │    │            → 父的 colId=3                     │    │       │ │
│  │  │    │   从 STag 按 grandchild colId=4 提取值='deep' │    │       │ │
│  │  │    │   → 写入 resolvedTag{cid=3, val='deep'}      │    │       │ │
│  │  │    └──────────────────────────────────────────────┘    │       │ │
│  │  │                                                         │       │ │
│  │  │ ③ VirtualTableScanOperator 执行数据扫描                  │       │ │
│  │  │    • 从源表 src_g1 读取 c1 列数据                        │       │ │
│  │  │    • setTagColumnValue:                                 │       │ │
│  │  │      pResolvedTagVal != NULL → 直接使用 (修复后逻辑)      │       │ │
│  │  │      不再依赖 tag scan 的 pSrcCol (可能含垃圾数据)        │       │ │
│  │  │    • 输出: (ts, val=50, t1=100, t2='deep')              │       │ │
│  │  └─────────────────────────────────────────────────────────┘       │ │
│  │                                                                    │ │
│  │  合并所有 VCT 结果 → 执行 ORDER BY val                              │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        CLIENT: 结果返回                                   │
│  tbname  | val | t1  | t2                                                │
│  vct_c1  | 30  | 10  | hello                                            │
│  vct_c2  | 40  | 20  | world                                            │
│  vct_g1  | 50  | 100 | deep     ← 子节点VCT，标签通过名称映射解析          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 父节点 vs 子节点查询的关键区别

```
┌──────────────────────┬─────────────────────────────────────────────────┐
│                      │  父节点 VCT (直属)        │ 子节点 VCT (EXPAND后代)  │
├──────────────────────┼──────────────────────────┼─────────────────────────┤
│ 所属 STB             │ child_vst               │ grandchild_vst          │
│ 收集方式             │ needCollect=true         │ needExpand=true         │
│ tag colId            │ 与查询schema一致          │ 与查询schema不同!        │
│ tag解析              │ 按colId直接从STag提取     │ 按名称查pExpandTagName.. │
│                      │                          │ →colId映射后提取          │
│ setTagColumnValue    │ pResolvedTagVal优先       │ pResolvedTagVal优先      │
│                      │ (pSrcCol也可能有效)       │ (pSrcCol含垃圾数据!)     │
│ 数据列               │ 直接读源表列              │ 直接读源表列(相同)        │
└──────────────────────┴──────────────────────────┴─────────────────────────┘
```

---

## 2. 数据结构扩展

### 2.1 SStbObj (mndDef.h)

```c
typedef struct SStbObj {
  // ... 原有字段 ...
  int64_t  parentSuid;     // 父 VST 的 SUID，0 表示无继承
  int64_t  parentDbUid;    // 父 VST 所在 DB 的 UID
  int8_t   inheritDepth;   // 继承深度（1=直接子级，2=孙级...）
  int16_t  ownColStart;    // 自身新增列的起始索引（继承列在前）
  int16_t  ownTagStart;    // 自身新增标签的起始索引（继承标签在前）
} SStbObj;
```

### 2.2 SMCreateStbReq (tmsg.h)

在消息结构中新增对应字段，用于 client → mnode 传递继承信息：

```c
int64_t  parentSuid;
int8_t   inheritDepth;
int16_t  ownColStart;
int16_t  ownTagStart;
```

### 2.3 SVstDescendantsReq/Rsp (tmsg.h)

新增 RPC 消息类型 `TDMT_MND_GET_VST_DESCENDANTS`，用于 catalog 向 mnode 查询某 VST 的后代列表：

```c
typedef struct SVstDescendantsReq {
  char    db[TSDB_DB_FNAME_LEN];
  char    stb[TSDB_TABLE_NAME_LEN];
  int32_t maxDepth;  // -1=all
} SVstDescendantsReq;

typedef struct SVstDescendantsRsp {
  int32_t numOfDescendants;
  char**  pDescendants;  // "db.stb" 全名数组
} SVstDescendantsRsp;
```

### 2.4 计划节点 (plannodes.h)

在 `SVirtualScanLogicNode`、`SDynQueryCtrlLogicNode`、`SVirtualTableScanPhysiNode`、`SDynQueryCtrlPhysiNode` 中新增：

```c
int32_t expandLevel;  // INT32_MIN=none, 0=self, N>0=N levels, -1=all
```

### 2.5 AST 节点 (querynodes.h)

`SRealTableNode` 新增：
```c
bool    hasExpand;
int32_t expandLevel;
```

`SSelectStmt` 新增：
```c
int32_t expandLevel;  // INT32_MIN=none
```

---

## 3. 模块实现细节

### 3.1 Parser 层

| 文件 | 改动内容 |
|------|----------|
| `sql.y` | 添加 `EXPAND` 关键字和 `BASE ON` 语法规则 |
| `parTokenizer.c` | 注册 `EXPAND`、`BASE` 为保留关键字 |
| `parAstCreater.c` | `createRealTableNodeWithExpand()` 解析 EXPAND(N) 到 AST |
| `parAstParser.c` | `SHOW VSTABLE INHERITS` 语句解析 |
| `parTranslater.c` | 继承语义校验 + EXPAND 传播到 SELECT 节点 |

**Translator 校验逻辑** (`parTranslater.c`):
- `translateCreateVirtualInheritedStb()`: 校验父 VST 存在、是虚拟表、列名不冲突，合并继承列/标签到 CREATE 请求
- `translateExpandClause()`: 校验 EXPAND 只能用于虚拟超级表，传播 expandLevel 到 SelectStmt

### 3.2 MNode 层

| 文件 | 改动内容 |
|------|----------|
| `mndStb.c` | CREATE 继承处理 + ALTER 级联 + 后代查询接口 |
| `mndDef.h` | SStbObj 结构扩展 |
| `mmHandle.c` | 注册 `TDMT_MND_GET_VST_DESCENDANTS` 消息处理 |
| `mndShow.c` | SHOW VSTABLE INHERITS 支持 |

**核心函数** (`mndStb.c`):
- `mndBuildStbFromReq()`: 填充 parentSuid/inheritDepth/ownColStart/ownTagStart
- `mndProcessAlterStbAddColumn()`: ALTER ADD COLUMN 时级联到所有子 VST
- `mndProcessAlterStbModifyColumn()`: ALTER MODIFY COLUMN 级联
- `mndProcessDropStbCheck()`: DROP 时检查是否有子表依赖
- `mndProcessGetVstDescendants()`: 处理后代查询请求，遍历 SDB 查找所有 parentSuid 匹配的 STB

**ALTER 级联策略**:
- ADD COLUMN: 父表新增列，子表的 `ownColStart` 右移，保持列序一致
- MODIFY COLUMN: 父表修改列宽度，子表中对应继承列同步修改
- DROP COLUMN: 如果列被子表继承则**拒绝**删除（返回错误码）

### 3.3 Catalog 层

| 文件 | 改动内容 |
|------|----------|
| `catalog.c` | `catalogGetVstDescendants()` API |
| `ctgAsync.c` | 异步任务支持 |

**功能**: 客户端通过 `catalogGetVstDescendants(db, stb, maxDepth)` 向 mnode 请求后代 VST 列表，结果缓存在 catalog 中供 planner 使用。

### 3.4 Planner 层

| 文件 | 改动内容 |
|------|----------|
| `planLogicCreater.c` | expandLevel 传播到逻辑计划 |
| `planPhysiCreater.c` | expandLevel 传播到物理计划 |

**逻辑**: 如果 `SELECT` 语句带有 `expandLevel != INT32_MIN`，在创建 `SVirtualScanLogicNode` 和 `SDynQueryCtrlLogicNode` 时设置 expandLevel 字段，后续传递到物理计划的 `SVirtualTableScanPhysiNode` 和 `SDynQueryCtrlPhysiNode`。

### 3.5 Executor 层

| 文件 | 改动内容 |
|------|----------|
| `dynqueryctrloperator.c` | EXPAND 后代 VCT 发现 + 标签解析 |
| `virtualtablescanoperator.c` | 标签值设置优先级修复 |
| `scanoperator.c` | expandLevel 传播 |
| `sysscanoperator.c` | 系统表扫描适配 |
| `dynqueryctrl.h` | 新增 pExpandTagNameToColId 字段 |

**EXPAND 执行流程** (`dynqueryctrloperator.c`):
1. `initExpandDescendants()`: 根据 expandLevel 从 catalog 获取后代 VST 列表
2. 对每个后代 VST，获取其 VCT 子表列表加入扫描计划
3. `appendResolvedTagVal()`: 为后代 VCT 解析标签值（按名称匹配，转换 colId）
4. `resolveTagValsForVtbChild()`: 通过 `pExpandTagNameToColId` 哈希表将后代标签映射到父表 schema 的 colId

**关键修复** (`virtualtablescanoperator.c`):
```c
static int32_t setTagColumnValue(...) {
  // 优先使用已解析的标签值（EXPAND 后代的标签 colId 与父不同）
  if (pResolvedTagVal != NULL) {
    return setTagValueToColumn(pDstCol, pResolvedTagVal, rows);
  }
  // 回退到标签扫描结果（仅非 EXPAND 场景）
  if (pSrcCol == NULL || colDataIsNull_s(pSrcCol, 0) || ...) {
    colDataSetNNULL(pDstCol, 0, rows);
    return TSDB_CODE_SUCCESS;
  }
  // ... 使用 pSrcCol 数据
}
```

**Bug 根因**: EXPAND 后代 VCT 的标签扫描返回的 `pSrcCol` 中，BINARY 类型列的 `varmeta.offset[0]` 未正确设为 -1（因 colId 不匹配），导致 `colDataIsNull_s` 误判非 NULL，读取到垃圾数据。修复方案：当 `pResolvedTagVal` 可用时无条件优先使用。

### 3.6 Nodes 层

| 文件 | 改动内容 |
|------|----------|
| `nodesCloneFuncs.c` | expandLevel 字段克隆 |
| `nodesCodeFuncs.c` | expandLevel 序列化/反序列化（TLV） |
| `nodesMsgFuncs.c` | 消息编解码 |
| `nodesUtilFuncs.c` | 节点创建函数 |

### 3.7 系统表 & 错误码

| 文件 | 改动内容 |
|------|----------|
| `systable.c/h` | 定义 `ins_inherits` 系统表（parent_db, parent_stb, child_db, child_stb, depth） |
| `taoserror.h` | 新增错误码 |
| `terror.c` | 错误码字符串 |

**新增错误码**:
- `TSDB_CODE_MND_VST_INHERIT_DEPTH_EXCEEDED` — 继承深度超限
- `TSDB_CODE_MND_VST_INHERIT_PARENT_NOT_VIRTUAL` — 父表非虚拟表
- `TSDB_CODE_MND_VST_INHERIT_COL_CONFLICT` — 列名冲突
- `TSDB_CODE_MND_VST_INHERIT_DROP_INHERITED_COL` — 尝试删除被继承的列
- `TSDB_CODE_MND_VST_INHERIT_HAS_CHILDREN` — 删除有子表的 VST
- `TSDB_CODE_MND_VST_EXPAND_NOT_VIRTUAL` — EXPAND 用于非虚拟表

---

## 4. 查询管线数据流

```
SQL: SELECT val, t1, t2 FROM child_vst EXPAND(1) ORDER BY val

Parser:
  SSelectStmt.expandLevel = 1
  SRealTableNode.hasExpand = true, expandLevel = 1

Translator:
  校验 child_vst 是虚拟超级表
  expandLevel 传播到 SSelectStmt

Planner (Logic):
  SVirtualScanLogicNode.expandLevel = 1
  SDynQueryCtrlLogicNode.expandLevel = 1

Planner (Physi):
  SVirtualTableScanPhysiNode.expandLevel = 1
  SDynQueryCtrlPhysiNode.expandLevel = 1

Executor:
  DynQueryCtrl 检测 expandLevel=1
  → catalogGetVstDescendants(child_vst, maxDepth=1)
  → 得到 [grandchild_vst]
  → 获取 grandchild_vst 的 VCT 列表
  → 合并到扫描计划
  → 为后代 VCT 解析标签（按名称匹配，转 colId）
  → setTagColumnValue 优先使用 pResolvedTagVal
```

---

## 5. 权限模型

子 VST 使用自身的 STB 级权限，与普通超级表一致：
- 创建时检查 `mndCheckDbPrivilege` (USE_DB) + `mndCheckDbPrivilegeByNameRecF` (TBL_CREATE)
- 查询时通过 `mndCheckStbPrivilege` 独立鉴权
- 无特殊的继承/级联权限逻辑

---

## 6. 测试覆盖

| 测试文件 | 内容 |
|----------|------|
| `tests/system-test/0-others/vst_inheritance_ddl.py` | DDL 继承创建、ALTER 级联、DROP 检查 |
| `tests/system-test/2-query/vst_expand.py` | EXPAND 语法基础测试 |
| `tests/system-test/2-query/vst_expand_data.py` | 22 项 EXPAND 数据测试 |
| `test/cases/05-VirtualTables/test_vst_inheritance.py` | 完整集成测试 |

**EXPAND 数据测试覆盖**:
- 多层继承展开 (EXPAND(0), EXPAND(1), EXPAND(-1))
- BINARY/VARCHAR 标签跨层正确性
- 子节点私有列访问 (extra, deep)
- 聚合函数 (COUNT, SUM)
- WHERE 过滤
- tbname 伪列
- 叶子节点 EXPAND 行为
- EXPAND(0) 与无 EXPAND 等价性

---

## 7. 文件变更统计

- **头文件**: 8 个文件，+75 行
- **核心源码**: 26 个文件，+1132 行，-13 行
- **测试文件**: 8 个文件，+2694 行
- **总计**: 42 个文件，+3901 行，-13 行
