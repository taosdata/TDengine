# VST 继承功能实现计划

## 问题描述

根据 `17-vst-inheritance-fs.md`（v0.4）实现虚拟超级表（VST）多继承功能。
当前 3.0 分支**没有任何继承实现**，仅有基础的 VST/VCT 支持（virtualStb 标志、CREATE VTABLE/VTABLE USING 语法）。

### 核心需求（来自 FS v0.4）

- 通过 `BASE ON parent1, parent2` 实现多继承（最多 10 个父表，DAG 结构，同 DB）
- 仅**叶子 VST**（无子 VST）可拥有 VCT
- 已有 VCT 的 VST 不能被继承
- **一个父 VST 可被多个子 VST 同时继承**（一对多，不限扇出）
- 查询非叶 VST → 自动下推到叶子后代，投影裁剪
- 列顺序：ts → 父1列(无ts) → 父2列(无ts) → ... → 自有列(无ts)
- Tag 顺序：父1Tags → 父2Tags → ... → 自有Tags
- 多父之间列名/Tag名冲突 = **解析期报错，错误消息须包含冲突列名及来源父 VST**
- 子 VST 必须声明 TS 列（主键列不隐式继承）
- ALTER STABLE ADD/DROP BASE ON（继承关系动态变更）
- DROP BASE ON 时，VCT 中被移除列的 colRef **级联删除**
- 父 VST ALTER ADD/DROP COLUMN 自动级联到后代
- 无 PRIVATE、无 EXPAND、无 MODIFY COLUMN 级联
- VCT 创建沿用现有 `CREATE VTABLE ... USING ...` 语法（不变）
- 新系统表 `ins_vstable_inherits`

## 当前基线（3.0 分支）

| 组件 | 当前状态 |
|------|---------|
| `SStbObj`（mndDef.h:950） | 无继承相关字段 |
| `SMCreateStbReq`（tmsg.h:1287） | 无继承相关字段 |
| `SVCreateStbReq`（tmsg.h:4903） | 无继承相关字段 |
| 语法（sql.y:1054） | `CREATE STABLE ... table_options` — 无 BASE ON |
| SDB 版本 | STB_VER_NUMBER = 4（STB_VER_SUPPORT_OWNER） |
| 错误码 | 无 VST 继承相关错误码 |
| 系统表 | `ins_virtual_tables_referencing` 存在（VCT 引用，非继承） |
| 翻译器 | `translateCreateSuperTable`（parTranslater.c:15024）— 无继承逻辑 |
| 计划器/执行器 | VSTB 扫描支持已有，无继承展开逻辑 |

---

## 阶段 1：基础 — 错误码与数据结构

### 1.1 错误码

**文件**：`include/util/taoserror.h`、`source/util/src/terror.c`

新增错误码（分配在 0x03Cx-0x03Dx 范围，靠近现有 MND_STB 错误码）：

```
TSDB_CODE_MND_VST_HAS_CHILDREN           — 拒绝 DROP/ALTER 有子 VST 的父表
TSDB_CODE_MND_VST_PARENT_NOT_VIRTUAL      — BASE ON 目标不是 virtualStb=1
TSDB_CODE_MND_VST_COL_NAME_CONFLICT       — 列名/Tag名冲突（消息须含冲突列名及来源父 VST）
TSDB_CODE_MND_VST_CIRCULAR_INHERIT        — 检测到 DAG 环路
TSDB_CODE_MND_VST_MAX_PARENTS_EXCEED      — 父 VST 数量超过 10
TSDB_CODE_MND_VST_PARENT_HAS_VCT          — 父 VST 已有 VCT，不能被继承
TSDB_CODE_MND_VST_NOT_LEAF                — 非叶 VST 不能创建 VCT
TSDB_CODE_MND_VST_DROP_BASE_MIN_COLS      — 取消继承后列/Tag不满足最小要求
TSDB_CODE_MND_VST_CROSS_DB               — 父子 VST 不在同一 DB
```

**列名冲突错误消息格式**：
- 多父之间冲突：`Column '<name>' conflicts between parent '<parent1>' and parent '<parent2>'`
- 自有列与父列冲突：`Column '<name>' conflicts with parent '<parent>'`
- Tag 冲突同理，将 `Column` 替换为 `Tag`

### 1.2 常量

**文件**：`include/common/tmsgdef.h` 或 `tmsg.h`

```c
#define TSDB_MAX_VST_PARENTS 10
```

### 1.3 SStbObj 扩展

**文件**：`source/dnode/mnode/impl/inc/mndDef.h`

在 `SStbObj` 中新增：
```c
int8_t   numParents;                         // 0 = 无继承
int64_t  parentSuids[TSDB_MAX_VST_PARENTS];  // 父 VST UID 数组
int16_t  ownColStart;                        // 自有列起始位置（继承列之后）
int16_t  ownTagStart;                        // 自有 Tag 起始位置（继承 Tag 之后）
```

### 1.4 SMCreateStbReq 扩展

**文件**：`include/common/tmsg.h`

在 `SMCreateStbReq` 中新增：
```c
int8_t   numParents;
char     parentStbFNames[TSDB_MAX_VST_PARENTS][TSDB_TABLE_FNAME_LEN];
int16_t  ownColStart;
int16_t  ownTagStart;
```

### 1.5 SVCreateStbReq 扩展

**文件**：`include/common/tmsg.h`

在 `SVCreateStbReq` 中新增：
```c
int8_t   numParents;
int64_t  parentSuids[TSDB_MAX_VST_PARENTS];
int16_t  ownColStart;
int16_t  ownTagStart;
```

### 1.6 消息序列化

**文件**：`source/common/src/msg/tmsg.c`

- `tSerializeSMCreateStbReq` / `tDeserializeSMCreateStbReq`：在现有字段之后追加继承字段。反序列化时若旧消息则默认 numParents=0。
- `tSerializeSVCreateStbReq` / `tDeserializeSVCreateStbReq`：同样处理。

### 1.7 SDB 编解码

**文件**：`source/dnode/mnode/impl/src/mndStb.c`

- 新增 `#define STB_VER_SUPPORT_INHERIT 5`，更新 `STB_VER_NUMBER`
- `mndStbActionEncode`：写入 numParents、parentSuids[]、ownColStart、ownTagStart
- `mndStbActionDecode`：读取新字段；`if (sver < STB_VER_SUPPORT_INHERIT)` 默认 numParents=0

---

## 阶段 2：语法与 AST

### 2.1 Token：TK_BASE

**文件**：`source/libs/parser/inc/sql.y`（token 声明区域）

将 `BASE` 添加为非保留关键字，用于 `BASE ON` 复合关键字。

### 2.2 语法：CREATE STABLE ... BASE ON

**文件**：`source/libs/parser/inc/sql.y`

在现有 CREATE STABLE 规则旁新增：

```yacc
cmd ::= CREATE STABLE not_exists_opt(A) full_table_name(B)
  NK_LP column_def_list(C) NK_RP tags_def(D) BASE ON base_on_list(F) table_options(E).
  { pCxt->pRootNode = createCreateInheritedStableStmt(pCxt, A, B, C, D, E, F); }

cmd ::= CREATE STABLE not_exists_opt(A) full_table_name(B)
  NK_LP column_def_list(C) NK_RP BASE ON base_on_list(F) table_options(E).
  { pCxt->pRootNode = createCreateInheritedStableStmt(pCxt, A, B, C, NULL, E, F); }

base_on_list(A) ::= full_table_name(B).
  { A = createNodeList(pCxt, B); }
base_on_list(A) ::= base_on_list(B) NK_COMMA full_table_name(C).
  { A = addNodeToList(pCxt, B, C); }
```

说明：
- `table_options` 已处理 `VIRTUAL NK_INTEGER`，无需额外修改
- `tags_def` 可选（第二条规则无 TAGS 子句）
- BASE ON 放在 table_options 之前，避免与 VIRTUAL 产生歧义

### 2.3 语法：ALTER STABLE ADD/DROP BASE ON

**文件**：`source/libs/parser/inc/sql.y`

```yacc
alter_table_clause(A) ::= full_table_name(B) ADD BASE ON base_on_list(C).
  { A = createAlterTableAddBaseOn(pCxt, B, C); }
alter_table_clause(A) ::= full_table_name(B) DROP BASE ON base_on_list(C).
  { A = createAlterTableDropBaseOn(pCxt, B, C); }
```

新增 alter 类型：
- `TSDB_ALTER_TABLE_ADD_BASE_ON`
- `TSDB_ALTER_TABLE_DROP_BASE_ON`

### 2.4 语法：SHOW VSTABLE INHERITS

**文件**：`source/libs/parser/inc/sql.y`

```yacc
cmd ::= SHOW VSTABLE INHERITS.
  { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VSTABLE_INHERITS_STMT); }
```

### 2.5 AST 节点：SCreateTableStmt 扩展

**文件**：`include/libs/nodes/cmdnodes.h`

扩展 `SCreateTableStmt`：
```c
typedef struct SCreateTableStmt {
  ENodeType      type;
  char           dbName[TSDB_DB_NAME_LEN];
  char           tableName[TSDB_TABLE_NAME_LEN];
  bool           ignoreExists;
  SNodeList*     pCols;
  SNodeList*     pTags;
  STableOptions* pOptions;
  SNodeList*     pBaseOnList;   // 新增：父 VST full_table_name 节点列表（NULL = 无继承）
} SCreateTableStmt;
```

### 2.6 ENodeType（SHOW 语句）

**文件**：`include/common/tmsg.h`

在 `ENodeType` 枚举中新增 `QUERY_NODE_SHOW_VSTABLE_INHERITS_STMT`。

### 2.7 Parser AST 构造函数

**文件**：`source/libs/parser/src/parAstCreater.c`

- `createCreateInheritedStableStmt()`：创建带 pBaseOnList 的 SCreateTableStmt
- `createAlterTableAddBaseOn()`：创建 TSDB_ALTER_TABLE_ADD_BASE_ON 类型的 SAlterTableStmt
- `createAlterTableDropBaseOn()`：创建 TSDB_ALTER_TABLE_DROP_BASE_ON 类型的 SAlterTableStmt

---

## 阶段 3：翻译器 — 语义分析

### 3.1 CREATE STABLE with BASE ON

**文件**：`source/libs/parser/src/parTranslater.c`

新增函数 `translateCreateInheritedStable()` 或扩展 `translateCreateSuperTable()`：

1. **校验 VIRTUAL**：pOptions->virtualStb 必须为 true（继承仅适用于 VST）
2. **遍历每个父 VST（pBaseOnList）**：
   a. 通过 catalog 获取父表元数据（`catalogGetTableMeta`）
   b. 校验父表是 VST（`virtualStb == 1`）→ 否则 `TSDB_CODE_MND_VST_PARENT_NOT_VIRTUAL`
   c. 校验父表无 VCT → 否则 `TSDB_CODE_MND_VST_PARENT_HAS_VCT`
   d. 校验同一 DB → 否则 `TSDB_CODE_MND_VST_CROSS_DB`
3. **检查父表数量上限**（≤10）→ 否则 `TSDB_CODE_MND_VST_MAX_PARENTS_EXCEED`
4. **列名冲突检测**：
   - 收集所有父列（排除 TS）+ 自有列（排除 TS），同时记录每列来源父 VST
   - 若有重名列 → `TSDB_CODE_MND_VST_COL_NAME_CONFLICT`
   - **错误消息须包含冲突列名及来源**：
     - 多父冲突：`Column '<name>' conflicts between parent '<parent1>' and parent '<parent2>'`
     - 自有列与父列冲突：`Column '<name>' conflicts with parent '<parent>'`
   - Tag 冲突同理，将 `Column` 替换为 `Tag`
5. **TS 列校验**：自有列列表必须以 `ts TIMESTAMP` 开头
6. **环路检测**：从每个父表向上 BFS/DFS 遍历其祖先 → `TSDB_CODE_MND_VST_CIRCULAR_INHERIT`
7. **Schema 合并**：构建合并后的列和 Tag 列表：
   - 列：ts + 父1列(无ts) + 父2列(无ts) + ... + 自有列(无ts)
   - Tags：父1Tags + 父2Tags + ... + 自有Tags
8. **构建 SMCreateStbReq**：
   - 合并后的 pColumns/pTags 数组
   - numParents、parentStbFNames[]
   - ownColStart（自有列起始索引）、ownTagStart

### 3.2 ALTER STABLE ADD BASE ON

翻译 `TSDB_ALTER_TABLE_ADD_BASE_ON`：
1. 获取当前 VST 元数据
2. 执行与 3.1 相同的校验（父表检查、冲突、上限、环路）
3. 构建包含父表信息的 alter 请求

### 3.3 ALTER STABLE DROP BASE ON

翻译 `TSDB_ALTER_TABLE_DROP_BASE_ON`：
1. 获取当前 VST 元数据
2. 计算移除父表列/Tag 后的剩余 schema
3. 校验剩余列 ≥ 2（含 TS）+ 剩余 Tag ≥ 1 → 否则 `TSDB_CODE_MND_VST_DROP_BASE_MIN_COLS`

### 3.4 VCT 创建校验

在现有 VCT 创建翻译路径中：
- 解析目标 VST 后，检查其无子 VST → `TSDB_CODE_MND_VST_NOT_LEAF`
- 此检查也可在 mnode 侧完成（翻译器可能没有子表信息）

### 3.5 DROP STABLE 校验

在翻译器或 mnode 中：检查目标 VST 无子表 → `TSDB_CODE_MND_VST_HAS_CHILDREN`

---

## 阶段 4：Mnode — DDL 处理

### 4.1 创建继承 VST

**文件**：`source/dnode/mnode/impl/src/mndStb.c`

扩展 `mndCheckCreateStbReq()`：
- 若 numParents > 0：
  - 对每个 parentStbFName，通过 `mndAcquireStb()` 查找 SStbObj
  - 校验父表 virtualStb == 1
  - 校验父表无 VCT（`mndStbHasVCT()`）
  - 注意：父表可以有子 VST（非叶父表是允许的；只有叶子才能有 VCT）

扩展 `mndBuildStbFromReq()`：
- 将 numParents、parentSuids[]、ownColStart、ownTagStart 复制到 SStbObj

### 4.2 工具函数

在 `mndStb.c` 中新增函数：

```c
// 检查 VST 是否有子 VST（用于 VCT 创建门控 + DROP 门控）
bool mndStbHasChildren(SMnode *pMnode, int64_t suid);

// 检查 VST 是否有 VCT（用于继承门控）
bool mndStbHasVCT(SMnode *pMnode, int64_t suid);

// 获取所有叶子后代 VST（用于查询下推）
int32_t mndGetLeafDescendants(SMnode *pMnode, int64_t suid, SArray **ppLeaves);

// DAG 环路检测
bool mndCheckCyclicInherit(SMnode *pMnode, int64_t childSuid, int64_t *parentSuids, int8_t numParents);
```

`mndStbHasChildren` 实现方式：
- 遍历 SDB 中所有 STB，检查是否有 `parentSuids[]` 包含给定 suid
- 或维护反向索引（子表列表）— 性能与复杂度的权衡
- 初始实现使用 SDB 遍历即可（STB 数量通常较少）

### 4.3 DROP 校验

在 `mndDropStb()` 或 `mndProcessDropStbReq()` 中：
- 删除前调用 `mndStbHasChildren()` → 拒绝并返回 `TSDB_CODE_MND_VST_HAS_CHILDREN`

### 4.4 VCT 创建门控

在 VCT 创建路径（mnode 侧）：
- 创建 VCT 时检查目标 VST 是否有子表 `mndStbHasChildren()` → 拒绝并返回 `TSDB_CODE_MND_VST_NOT_LEAF`

### 4.5 ALTER 级联：ADD/DROP COLUMN

在 `mndAlterStb()` 中：
- 父 VST 添加/删除列后：
  - 查找所有直接子 VST（扫描 SDB，找 parentSuids 包含该 suid 的 STB）
  - 对每个子 VST：
    - 在正确位置（父表继承区域内）添加/删除列
    - 调整受影响子 VST 的 ownColStart / ownTagStart
    - 递归处理孙代
  - 为每个后代构建 `SMAlterStbReq` 并应用

### 4.6 ALTER ADD BASE ON

`TSDB_ALTER_TABLE_ADD_BASE_ON` 的 mnode 处理：
1. 获取目标 SStbObj
2. 获取每个新父 SStbObj
3. 校验所有约束（与创建类似，但为增量式）
4. 将父表列/Tag 合并到目标 schema 中（插入到 ownColStart/ownTagStart 之前）
5. 更新 parentSuids[]、numParents、ownColStart、ownTagStart
6. 发送 vnode alter 请求以更新 schema

### 4.7 ALTER DROP BASE ON

`TSDB_ALTER_TABLE_DROP_BASE_ON` 的 mnode 处理：
1. 找到要移除的父表
2. 确定来自该父表的列/Tag（在父表继承区域边界之间）
3. 校验剩余 schema ≥ 2 列 + ≥ 1 Tag
4. 从 schema 中移除父表的列/Tag
5. 更新 parentSuids[]、numParents、ownColStart、ownTagStart
6. **VCT 级联删除**（若该 VST 已有 VCT）：
   a. 遍历该 VST 下所有 VCT
   b. 从每个 VCT 中移除被移除列的 colRef 映射
   c. 从每个 VCT 中移除被移除 Tag 的值
   d. 发送 vnode alter 请求更新 VCT schema
   e. 后续查询这些 VCT 不再包含被移除的列和 Tag

---

## 阶段 5：系统表与 SHOW

### 5.1 系统表：ins_vstable_inherits

**文件**：`include/common/systable.h`

```c
#define TSDB_INS_TABLE_VSTABLE_INHERITS "ins_vstable_inherits"
```

**文件**：`source/common/src/systable.c`

定义列 schema：
```
db_name       VARCHAR(64)
parent_stable VARCHAR(192)
parent_uid    BIGINT
child_stable  VARCHAR(192)
child_uid     BIGINT
create_time   TIMESTAMP
```

### 5.2 Mnode 检索函数

**文件**：`source/dnode/mnode/impl/src/mndStb.c`

新增函数 `mndRetrieveVstableInherits()`：
- 遍历 SDB 中所有 SStbObj
- 对每个 numParents > 0 的，每个父表输出一行
- 通过 `mndSetMsgHandle()` 在 `mndInitStb()` 中注册

### 5.3 SHOW VSTABLE INHERITS

**文件**：`source/libs/parser/src/parTranslater.c`

将 `QUERY_NODE_SHOW_VSTABLE_INHERITS_STMT` 翻译为 `SELECT * FROM information_schema.ins_vstable_inherits`

### 5.4 SHOW CREATE STABLE

**文件**：`source/dnode/mnode/impl/src/mndStb.c`（或 SHOW CREATE STABLE 的处理位置）

当 numParents > 0 时，输出中包含 `BASE ON parent1, parent2` 子句。

### 5.5 DESCRIBE 增强

在 DESCRIBE 输出中，为继承列标注来源父 VST 名称（Note/comment 字段）。

---

## 阶段 6：查询 — 非叶 VST 下推

### 6.1 Catalog：叶子后代发现

**文件**：`source/libs/catalog/`

计划器需要知道非叶 VST 的所有叶子后代。方案：
- **方案 A**：Mnode 提供消息类型，返回给定 suid 的叶子后代
- **方案 B**：Catalog 获取所有继承关系后本地计算
- **推荐**：方案 A — 新增消息 `TDMT_MND_GET_VST_LEAF_DESCENDANTS`，返回叶子 SStbObj 元数据数组

### 6.2 计划器：逻辑计划创建

**文件**：`source/libs/planner/src/planLogicCreater.c`

在 `createScanLogicNode()` 或等效函数中：
- 当扫描一个 numParents=0 但有子 VST 的 VST（非叶）时：
  - 通过 catalog 获取叶子后代
  - 为每个叶子创建虚拟扫描逻辑节点
  - 用 MERGE/UNION 逻辑节点组合（所有叶子都包含祖先的列）
  - 应用投影裁剪：仅输出被查询祖先 VST 自身 schema 的列

### 6.3 计划器：物理计划创建

**文件**：`source/libs/planner/src/planPhysiCreater.c`

- 将多叶子扫描逻辑计划转换为物理 exchange + scan 节点

### 6.4 执行器：投影裁剪

**文件**：`source/libs/executor/`

- 每个叶子 VCT 扫描输出叶子 VST 的所有列
- 执行器通过 slot 映射裁剪为仅查询的祖先列
- 类似现有的列裁剪 — 将祖先 colId 映射到叶子 slot 位置

---

## 阶段 7：测试

### 7.1 单元测试

- Schema 合并逻辑（列顺序、Tag 顺序）
- 环路检测（DAG）
- 列名冲突检测
- SDB 编解码往返测试（含继承字段）

### 7.2 系统测试（Python）

**文件**：`tests/system-test/2-query/vst_inherit.py`（新建）

测试用例：
1. 创建继承 VST（单父、多父）
2. 在叶子 VST 下创建 VCT
3. 在非叶 VST 下创建 VCT → 报错
4. 继承已有 VCT 的 VST → 报错
5. 列名冲突 → 报错
6. 环路检测 → 报错
7. 超过最大父表数 → 报错
8. 查询叶子 VST（完整 schema）
9. 查询非叶 VST（下推 + 投影裁剪）
10. ALTER 父表 ADD COLUMN → 级联
11. ALTER 父表 DROP COLUMN → 级联
12. ALTER ADD BASE ON
13. ALTER DROP BASE ON（最少列/Tag 检查）
14. DROP 有子表的父 VST → 报错
15. SHOW VSTABLE INHERITS
16. SHOW CREATE STABLE
17. DESCRIBE 继承标注
18. 跨 DB 继承 → 报错
19. 通过非叶 VST 查询 Tag

---

## 依赖关系图

```
阶段 1（基础）
  ├── 阶段 2（语法与 AST）──依赖── 阶段 1
  │     └── 阶段 3（翻译器）──依赖── 阶段 1, 2
  │           └── 阶段 4（Mnode DDL）──依赖── 阶段 1, 3
  │                 ├── 阶段 5（系统表）──依赖── 阶段 4
  │                 └── 阶段 6（查询下推）──依赖── 阶段 4, 5
  └── 阶段 7（测试）──依赖── 所有阶段
```

阶段 5 和阶段 6 可在阶段 4 稳定后并行开发。

---

## 推荐实现顺序

1. **阶段 1**：错误码、常量、结构体扩展、序列化 → 确保编译链接通过
2. **阶段 2**：语法 + AST → 解析器编译通过，新语法可解析
3. **阶段 3**：翻译器 → 语义校验 + schema 合并，CREATE 返回正确的 SMCreateStbReq
4. **阶段 4.1-4.4**：Mnode 创建/删除/VCT 门控 → 基础继承端到端可用
5. **阶段 5.1-5.3**：系统表 + SHOW → 继承关系可查询
6. **阶段 4.5-4.7**：ALTER 级联 + ADD/DROP BASE ON → 完整 DDL 支持
7. **阶段 6**：查询下推 → 非叶 VST 查询可用
8. **阶段 7**：测试
9. **阶段 5.4-5.5**：SHOW CREATE STABLE / DESCRIBE 增强

## 需修改的关键文件

| 文件 | 修改内容 |
|------|---------|
| `include/util/taoserror.h` | 新增错误码 |
| `source/util/src/terror.c` | 错误码字符串 |
| `source/dnode/mnode/impl/inc/mndDef.h` | SStbObj 继承字段 |
| `include/common/tmsg.h` | SMCreateStbReq/SVCreateStbReq 字段、ENodeType、常量 |
| `source/common/src/msg/tmsg.c` | 序列化/反序列化 |
| `include/libs/nodes/cmdnodes.h` | SCreateTableStmt.pBaseOnList |
| `source/libs/parser/inc/sql.y` | 语法规则 |
| `source/libs/parser/src/parAstCreater.c` | AST 构造函数 |
| `source/libs/parser/src/parTranslater.c` | 翻译逻辑 |
| `source/dnode/mnode/impl/src/mndStb.c` | Mnode DDL + 系统表 |
| `include/common/systable.h` | ins_vstable_inherits 定义 |
| `source/common/src/systable.c` | 系统表 schema |
| `source/libs/planner/src/planLogicCreater.c` | 非叶展开 |
| `source/libs/planner/src/planPhysiCreater.c` | 物理计划 |
| `source/libs/executor/` | 投影裁剪 |
| `source/libs/nodes/src/nodesCloneFuncs.c` | 克隆 pBaseOnList |
| `source/libs/nodes/src/nodesCodeFuncs.c` | 新节点类型序列化/命名 |
| `source/libs/nodes/src/nodesUtilFuncs.c` | 节点创建工具 |
