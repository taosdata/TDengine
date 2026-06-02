# VST Inheritance 分支 Review 报告

> 分支: `feat/addVirtualHire`
> 基准: `a9121556337` (Phase 1+2) .. `2ae6fbe5230` (HEAD)
> 范围: 21 文件, ~5400 行新增代码
> 日期: 2026-06-01

---

## 一、规格与实现不一致

以下为 `vst-inheritance-fs.md` 规格文档与分支实际代码的逐项对比。

### 1.1 §4.1 CREATE 路径缺少多父列名/Tag 名冲突检测

**FS 规格** (§4.1 规则表):

> 所有父 VST 之间、以及新增列与所有父列之间，列名不可重复。冲突时**在解析期报错**，错误消息须包含冲突列名及来源父 VST 名称（例如：`Column 'status' conflicts between parent 'base_device' and parent 'base_sensor'`）

**实现现状**:

- `ALTER STABLE ADD BASE ON` 路径 (`mndAlterStbAddBaseOn`, `mndStb.c:3422`) **有**冲突检测，逐个检查新父列与子表已有 schema 的冲突
- `CREATE STABLE ... BASE ON` 路径 (`mndCreateStb`, `mndStb.c:1300`) 的 schema 合并代码**没有**检查父表之间的列名/Tag 名冲突

**影响**: 如果两个父表有同名列（如 `base_x` 和 `base_y` 都有 `dup_col`），CREATE 时不会报错，后一个父的列直接覆盖前一个父的列，静默丢失数据。

**修复建议**: 在 `mndCreateStb` 的 schema 合并循环中，对每个父表的每列/Tag 检查是否已被前面的父表使用过。

---

### 1.2 §4.4 父 VST ALTER 列级联完全未实现

**FS 规格** (§4.4):

> | 操作 | 行为 |
> |------|------|
> | 父 VST `ADD COLUMN` | **自动级联**到所有子孙 VST（新列追加到对应父的继承区域末尾） |
> | 父 VST `DROP COLUMN` | **自动级联**删除所有子孙 VST 中对应的继承列 |
> | 子 VST `ADD COLUMN` | 不影响父 VST，仅子 VST 及其子孙 |

**实现现状**: 完全未实现。当父 VST 通过 `ALTER STABLE parent ADD COLUMN new_col INT` 添加新列时，已继承该父的所有子 VST 不会自动获得新列。

**影响**:

1. FS §4.6.2 承诺的 "保证列完整"（所有叶子后代一定包含祖先的全部列）在父表 ALTER 后不成立
2. 如果父 VST 新增一列后查询非叶 VST，UNION ALL 展开会尝试在叶子表上 SELECT 这个新列，但叶子表没有，查询会报错
3. 这属于功能缺失，不是 bug

**修复建议**: 在 `mndProcessAlterStbReq` 中，当检测到被 ALTER 的 VST 有子 VST（`mndStbHasChildren` 返回 true）时，遍历所有子孙 VST，在对应继承区域追加/删除列，使用事务保证原子性。

---

### 1.3 §4.5.2 DROP BASE ON 的 VCT colRef 级联删除未实现

**FS 规格** (§4.5.2):

> 若该 VST 已有 VCT，被移除列的 colRef **级联删除**——对应列的 colRef 映射从所有 VCT 中移除，后续查询这些列返回 NULL

详细行为:
1. VST schema 中移除继承列 C1、C2 和继承 Tag T1
2. 所有已有 VCT 中 C1、C2 对应的 colRef 映射被删除
3. T1 对应的 Tag 值被删除
4. 后续查询不再包含这些列

**实现现状**: `mndAlterStbDropBaseOn` 只修改了 VST 的 schema（移除继承列），没有更新已有 VCT 的 colRef 映射和 Tag 值。已有 VCT 的 colRef 仍指向已不存在的列 ID。

**影响**: 已有 VCT 的列引用失效，查询时可能产生未定义行为（取决于 executor 如何处理无效 colRef）。

**修复建议**: 在 DROP BASE ON 事务中增加一步：遍历该 VST 下所有 VCT，删除引用被移除列的 colRef 条目，删除被移除 Tag 的值。

---

### 1.4 §16.7 DESCRIBE 无继承来源标注

**FS 规格** (§12 + §16.7 示例):

```
status    | INT       | 4      | inherited from base_device
value     | FLOAT     | 4      | inherited from base_metric
```

DESCRIBE 的 Note 列应标注 `inherited from xxx`。

**实现现状**: 未实现。DESCRIBE 没有任何继承来源信息。

**修复建议**: 在 DESCRIBE 的逻辑中，根据列的 index 与 `ownColStart`/`ownTagStart` 的关系判断是否为继承列，并查找来源父表名称填入 Note 列。

---

### 1.5 §11 错误码定义了但未使用（2 个）

**FS 规格** (§11):

| 错误码 | 含义 |
|--------|------|
| `TSDB_CODE_MND_VST_COL_NAME_CONFLICT` | 列名/Tag 名冲突，消息格式 `Column '<name>' conflicts between '<parent1>' and '<parent2>'` |
| `TSDB_CODE_MND_VST_DROP_BASE_MIN_COLS` | 取消继承后列/Tag 不满足最小要求 |

**实现现状**:

- `TSDB_CODE_MND_VST_COL_NAME_CONFLICT` 已定义 (`taoserror.h:0x04A4`, `terror.c`) 但**从未被任何代码返回**。实际冲突时使用 `TSDB_CODE_MND_COLUMN_ALREADY_EXIST`
- `TSDB_CODE_MND_VST_DROP_BASE_MIN_COLS` 已定义 (`taoserror.h:0x04A9`, `terror.c`) 但**从未被任何代码返回**。实际用 `TSDB_CODE_PAR_INVALID_DROP_COL` 和 `TSDB_CODE_MND_INVALID_STB_OPTION` 代替

**修复建议**:

1. 在 `mndAlterStbAddBaseOn` 和 `mndCreateStb` 的冲突检测中使用 `TSDB_CODE_MND_VST_COL_NAME_CONFLICT`
2. 在 `mndAlterStbDropBaseOn` 的最少列检查中使用 `TSDB_CODE_MND_VST_DROP_BASE_MIN_COLS`
3. 错误消息按 FS 格式输出

---

### 1.6 SHOW CREATE STABLE 输出缺少 VIRTUAL 1 标记

**FS 规格** (§4.8 + §16.7):

```
CREATE STABLE `sensor_vst` (...) TAGS (...) BASE ON `base_device`, `base_metric` VIRTUAL 1
```

**实现现状** (`command.c`):

```
TAGS (...) BASE ON `base_device`, `base_metric` SECURITY_LEVEL 0 ...
```

`VIRTUAL 1` 不在 BASE ON 之后紧跟的位置。

**修复建议**: 确认 `VIRTUAL 1` 是否通过 options 机制输出，如果没有则需要在 BASE ON 输出后追加。

---

### 1.7 系统表 `ins_vstable_inherits` 列名与 §3 定义有差异

**FS 规格内部矛盾**: §3 与 §16.7 对系统表格式描述不一致。

| §3 定义 | §16.7 示例列名 | 实际实现 |
|---------|-------------|---------|
| db_name | db_name | db_name |
| stable_name | child_stable | child_stable_name |
| parent_stable_name | parent_stable | parent_stable_name |
| - | parent_uid | parent_uid |
| - | child_uid | child_uid |
| create_time | create_time | create_time |

实现跟随 §16.7 的格式（每 (child, parent) 一行），比 §3 多了 uid 列。§3 需要更新以匹配 §16.7 和实现。

---

## 二、代码质量问题

### 2.1 [P1] `mndStbHasChildren()` 每次查元数据都全表扫描 O(N)

**位置**: `mndStb.c:596` + `mndStb.c:2893`

**问题**: `mndBuildStbSchemaImp` 在每次虚拟超级表的元数据响应时调用 `mndStbHasChildren()`，该函数遍历 SDB 中所有超级表来检查谁引用了当前 VST。集群有 10000 个超级表时，每个 VST 元数据请求扫描 10000 条记录。

**修复建议**: 在 `SStbObj` 里存 `hasChildren` 布尔字段，在 `mndStbActionInsert`/`mndStbActionDelete` 中维护它。`STableMetaRsp.hasInheritors` 已有传输位，改为从 SStbObj 直接读取而非动态计算。

---

### 2.2 [P1] 非叶 VST 查询（UNION ALL 展开）零测试覆盖

**位置**: `test/cases/05-VirtualTables/test_vst_inheritance_cascade.py`

**问题**: 20 个测试全部查询叶子 VST，没有任何测试 SELECT 非叶 VST。以下代码路径零覆盖：
- `rewriteNonLeafVstQuery()`
- `buildLeafSelectStmt()`
- `mndProcessGetVstLeavesReq()`
- `catalogGetVstLeaves()`
- UNION ALL 构建逻辑

**修复建议**: 添加测试 SELECT 非叶 VST，验证单叶子、多叶子、零叶子后代场景。

---

### 2.3 [P1] DROP BASE ON 按列名匹配会误删其他父表的列

**位置**: `mndStb.c:3559` (`mndIsColFromParent`)

**问题**: DROP BASE ON 用列名判断列来源。如果两个父表都有 `status` 列，`DROP BASE ON P1` 会把合并后的 `status` 删掉，即使该列可能来自 P2。

**修复建议**: 在合并 schema 中为每个继承列记录来源父表的 `parentSuid`，DROP 时按来源匹配而非名称。或在 ADD BASE ON 时拒绝不同父表有同名列。

---

### 2.4 [P1] BFS 队列固定大小，溢出时静默截断

**位置**: `mndStb.c:628`（环检测 queue[128]）和 `mndStb.c:5027`（叶子查找 queue[256]）

**问题**: 固定大小的 BFS 队列在 DAG 超过限制时静默截断：
- 环检测: 漏检循环继承
- 叶子查找: UNION ALL 展开不完整，查询少数据
- 两者都不报错

**修复建议**: 使用 `taosArray` 动态数组，到达上限时返回错误。

---

### 2.5 [P1] DROP BASE ON 没有检查子 VST 是否已有 VCT

**位置**: `mndStb.c:3822`

**问题**: ADD BASE ON 正确使用 SERIAL 事务先检查父表 VCT。但 DROP BASE ON 走普通 `mndAlterStbImp`，没有串行执行也没有检查子 VST 的 VCT。

**修复建议**: DROP BASE ON 也走 SERIAL 事务，或至少验证子 VST 没有 VCT。

---

### 2.6 [P2] `mndStbHasVCT()` 是死代码

**位置**: `mndStb.c:618`

**问题**: 永远返回 false，带 TODO 注释。从未被调用。实际 VCT 检查走 vnode RPC。

**修复建议**: 删除。

---

### 2.7 [P2] 读锁内嵌套 SDB 全表扫描

**位置**: `mndStb.c:2893`

**问题**: `mndBuildStbSchemaImp` 持有 `pStb->lock` 读锁时调用 `mndStbHasChildren()`，后者在循环中 acquire/release 其他 SStbObj 的引用，形成 `pStb->lock -> SDB lock` 嵌套。如果有其他路径以相反顺序获取这些锁，可能死锁。

**修复建议**: 如果解决了 2.1（缓存 hasChildren），此问题自然消失。否则应在获取 latch 之前计算 hasInheritors。

---

### 2.8 [P2] 系统表 `ins_vstable_inherits` 性能 O(C*P*N)

**位置**: `mndStb.c:4926` (`mndRetrieveVstableInherits`)

**问题**: 对每个有继承关系的子 VST 的每个父，都做一次全 SDB 扫描来解析 parentSuid 到名称。C 个子 VST，P 个父，N 个总 STB 时复杂度 O(C*P*N)。

**修复建议**: 预构建 uid→name 的 hash map。

---

### 2.9 [P2] `mndProcessGetVstLeavesReq` O(D*N) 全表扫描

**位置**: `mndStb.c:5027`

**问题**: BFS 的每一步都做一次全 SDB 扫描找子节点，对每个后代调用 `mndStbHasChildren`（又是一次全扫描），再对每个叶子做一次全扫描查名称。总复杂度 O(D*N)。

**修复建议**: 一次 SDB 扫描构建 parentSuid→children 的 hash map，后续都用 map 查找。

---

### 2.10 [P2] `metaOpenCtbCursor` 返回 NULL 时静默返回成功

**位置**: `vnodeQuery.c:953`

**问题**: `vnodeProcessCheckHasCtbReq` 中，如果 `metaOpenCtbCursor()` 返回 NULL（内存错误），code 保持 0，发送成功响应。DDL 会继续执行，但实际上检查没有运行。

**修复建议**: 将 `pCur == NULL` 视为错误。

---

### 2.11 [P2] DRY 违规：多处重复的 uid 查找模式

**位置**: `mndStb.c` 多处

重复模式：
1. 按 uid 查找 SStbObj 并提取名称（lines 3005, 4954）
2. 按 uid 查找 SStbObj 获取完整对象（lines 636, 5058）
3. `mndIsColFromParent` 和 `mndIsTagFromParent` 逻辑相同

**修复建议**: 提取 `mndGetStbNameByUid()`, `mndFindStbByUid()`, `mndSchemaContainsName()` 公共函数。

---

### 2.12 [P2] 魔法数字

**位置**: `mndStb.c` 多处

- BFS 队列: 128 (line 631), 256 (line 5027)
- 事务 groupId: 0, 2 (lines 915, 2810) 无命名常量

**修复建议**: 定义 `VST_CYCLE_QUEUE_SIZE`, `VST_MAX_DESCENDANTS`, `TRN_GROUP_DEFAULT`, `TRN_GROUP_DDL`。

---

## 三、质量评分

| 维度 | 评分 | 说明 |
|------|------|------|
| 功能完整性 | 6/10 | 核心功能已实现，但 ALTER 列级联和 VCT 级联删除缺失 |
| 规格符合度 | 5/10 | 7 处与规格不一致，含 1 项功能缺失 |
| 代码质量 | 5/10 | 多处 O(N) 全表扫描、死代码、DRY 违规 |
| 测试覆盖 | 4/10 | 非叶查询零覆盖，核心路径无测试 |
| **综合** | **5/10** | |
