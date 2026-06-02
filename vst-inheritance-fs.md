# FS: 虚拟超级表继承 (VST Inheritance)

## 1. 修订记录

| 日期 | 版本 | 负责人 | 主要修改内容 |
|------|------|--------|-------------|
| 2026-05-07 | 0.1 | 邓怡豪 | 初稿 |
| 2026-05-08 | 0.2 | 邓怡豪 | 重写：多继承、叶子专有VCT、去除EXPAND、统一CREATE STABLE语法 |
| 2026-05-08 | 0.3 | 邓怡豪 | 去除PRIVATE关键字 |
| 2026-05-08 | 0.4 | 邓怡豪 | 补充：一对多继承、冲突提示含列名及来源、DROP BASE ON 级联细化、需求清单对照 |

## 2. 背景

TDengine 虚拟超级表（VST）当前仅支持单层结构。为支持更复杂的数据建模场景（如多维度设备分类、组合指标体系），需要为 VST 引入**多继承**机制：

- 子 VST 可继承多个父 VST 的列和 Tag 定义，形成 schema 组合（宽表）
- 只有**叶子 VST**（无子 VST 的 VST）可以拥有 VCT（数据）
- 查询非叶 VST 时，自动下推到所有叶子后代，投影该 VST 的列子集

## 3. 定义

| 术语 | 定义 |
|------|------|
| VST（虚拟超级表） | 带有 `VIRTUAL 1` 标记的超级表，通过 VCT 的 colRef 从源表获取数据 |
| VCT（虚拟子表） | 属于某个**叶子 VST** 的子表，通过列引用（colRef）映射到源表的具体列 |
| 父 VST | 被其他 VST 通过 `BASE ON` 引用的 VST，自身不能拥有 VCT |
| 叶子 VST | 没有子 VST 的 VST，**唯一**可以拥有 VCT 的层级 |
| 继承 | 通过 `BASE ON` 声明的父子关系，子 VST 自动包含所有父 VST 的全部列和 Tag |
| 源表 | 被虚拟表引用的物理表，是 VCT 数据与标签的原始来源 |

**数据模型**：

```
base_device (cols: ts, status INT)                    TAGS (region INT)      ← 非叶，无VCT
base_metric (cols: ts, value FLOAT)                   TAGS (unit BINARY(8))  ← 非叶，无VCT

sensor_vst BASE ON base_device, base_metric           ← 叶子，有VCT
  schema: (ts, status, value, accuracy INT)
  TAGS:   (region, unit, sensor_id INT)

actuator_vst BASE ON base_device                      ← 叶子，有VCT
  schema: (ts, status, cmd INT)
  TAGS:   (region, actuator_type INT)
```

- 子 VST 自动继承所有父 VST 的全部列和 Tag
- 子 VST 可新增自有列和自有 Tag（也可不新增）
- **只有叶子 VST 可以拥有 VCT**，父 VST 不能有 VCT
- 已有 VCT 的 VST 不能被后续继承（不能成为父 VST）
- **一个父 VST 可被多个子 VST 同时继承**（一对多关系），不限制继承扇出

**新增系统表** `ins_vstable_inherits`：

| 字段 | 含义 | 类型 |
|------|------|------|
| db_name | 所在数据库名 | VARCHAR |
| stable_name | VST 名称 | VARCHAR |
| parent_stable_name | 父 VST 名称 | VARCHAR |
| create_time | VST 的创建时间 | TIMESTAMP |

> 注：每个有继承关系的 VST 输出一行。若有多个直接父 VST，`parent_stable_name` 以 `(parent1, parent2, ..., parentN)` 格式显示。

**输出示例**：

假设存在以下继承关系：
- `child_single` 继承自 `parent_a`
- `child_multi` 继承自 `parent_a` 和 `parent_b`

```
taos> SELECT * FROM information_schema.ins_vstable_inherits;
        db_name        |       stable_name       |        parent_stable_name        |       create_time       |
===================================================================================================================
 test_db                | child_single            | (parent_a)                         | 2026-05-09 10:00:00.000 |
 test_db                | child_multi             | (parent_a, parent_b)             | 2026-05-09 10:01:00.000 |
```

- 单继承时 `parent_stable_name` 直接显示父名称
- 多继承时 `parent_stable_name` 以 `(parent1, parent2, ..., parentN)` 格式显示，父名称按继承声明顺序排列

## 4. 行为说明

### 4.1 DDL：创建继承 VST

**语法**：

```sql
-- 有新增列和 Tag
CREATE STABLE [db_name.]childVstName (
    colName dataType [, colName dataType ...]
) TAGS (
    tagName dataType [, tagName dataType ...]
) BASE ON [db_name.]parentVst1 [, [db_name.]parentVst2 ...] VIRTUAL 1;

-- 有新增 Tag，无新增普通列（仍需声明 TS）
CREATE STABLE [db_name.]childVstName (
    ts TIMESTAMP
) TAGS (tagName dataType [, ...])
    BASE ON [db_name.]parentVst1 [, ...] VIRTUAL 1;

-- 无新增列也无新增 Tag（仍需声明 TS）
CREATE STABLE [db_name.]childVstName (
    ts TIMESTAMP
) BASE ON [db_name.]parentVst1 [, ...] VIRTUAL 1;
```

行为：创建一个继承自一个或多个父 VST 的子 VST。子 VST 自动继承所有父 VST 的全部列和 Tag，括号内仅声明新增列/Tag。

**规则**：

| 规则 | 说明 |
|------|------|
| 父表必须是 VST | `BASE ON` 目标必须是 `virtualStb=1` |
| 父表不能有 VCT | 已拥有 VCT 的 VST 不能被继承 |
| 全量继承 | 子 VST **自动继承**所有父 VST 的全部列和 Tag，SQL 中仅声明新增列 |
| 列顺序 | 结果 schema = TS列 + 父1列(不含TS) + 父2列(不含TS) + ... + 自有列(不含TS)（各父按 BASE ON 声明顺序） |
| Tag 顺序 | 结果 Tags = 父1Tags + 父2Tags + ... + 自有Tags |
| colId 独立 | 子 VST 有**独立的 colId 命名空间**，与父表 colId 无关 |
| 列名不可冲突 | 所有父 VST 之间、以及新增列与所有父列之间，列名不可重复。冲突时**在解析期报错**，错误消息须包含冲突列名及来源父 VST 名称（例如：`Column 'status' conflicts between parent 'base_device' and parent 'base_sensor'`） |
| Tag名不可冲突 | 所有父 VST 之间、以及新增 Tag 与所有父 Tag 之间，Tag 名不可重复。冲突时同样报错并提示冲突 Tag 名及来源 |
| 最大父表数 | 最多继承 **10** 个父 VST |
| 同一 DB | 父子 VST 必须在同一数据库内 |
| 禁止循环 | 创建时通过 DAG 遍历检测环路 |
| 可无新增列 | 允许不声明新增普通列，但**必须声明 TS 列**（`ts TIMESTAMP`） |
| 权限 | 使用子 VST 自身所在 DB 的权限体系 |

### 4.2 DDL：创建 VCT

**语法**：

```sql
CREATE TABLE [IF NOT EXISTS] [db_name.]vctName USING [db_name.]vstName (tag_cols) TAGS (tagValue [, ...])
    [(colName FROM srcDb.srcTable.srcCol [, ...])]
```

行为：在 vstName 下创建一个虚拟子表 vctName。

**约束**：vstName 必须是**叶子 VST**（没有子 VST）。若 vstName 已有子 VST，则拒绝创建 VCT。

**列引用与 Tag 规则**：

| 规则 | 说明 |
|------|------|
| 完整 schema | VCT 的 colRef 可引用 VST 完整 schema 中的所有列，包括**从父 VST 继承的列**和**自有列** |
| Tag 赋值 | VCT 的 TAGS 值列表按 VST 完整 Tag schema 的顺序赋值，包括**从父 VST 继承的 Tag**和**自有 Tag** |
| 继承列引用 | 继承自父 VST 的列，同样通过 `colName FROM srcDb.srcTable.srcCol` 语法建立 colRef 映射 |

### 4.3 DDL：DROP

| 场景 | 行为 |
|------|------|
| DROP 有子 VST 的父 VST | **拒绝**（返回 TSDB_CODE_MND_VST_HAS_CHILDREN） |
| DROP 叶子 VST（有 VCT） | 先删 VCT 再删 VST，或直接删除 |
| DROP 叶子 VST（无 VCT） | 正常删除 |

### 4.4 DDL：ALTER 列级联

| 操作 | 行为 |
|------|------|
| 父 VST `ADD COLUMN` | **自动级联**到所有子孙 VST（新列追加到对应父的继承区域末尾） |
| 父 VST `DROP COLUMN` | **自动级联**删除所有子孙 VST 中对应的继承列 |
| 子 VST `ADD COLUMN` | 不影响父 VST，仅子 VST 及其子孙 |

### 4.5 DDL：ALTER 继承关系

#### 4.5.1 添加继承

**语法**：

```sql
ALTER STABLE [db_name.]vstName ADD BASE ON [db_name.]parentVst1 [, [db_name.]parentVst2 ...];
```

行为：为已有 VST 添加父继承关系。

**规则**：

| 规则 | 说明 |
|------|------|
| 目标 VST 必须是 VST | `virtualStb=1` |
| 父表必须是 VST 且无 VCT | 父 VST 不能拥有 VCT |
| 列名不可冲突 | 新增父的列/Tag 不能与 VST 已有列/Tag 重名，冲突时**在解析期报错**并提示冲突列名及来源 |
| 最大父表数 | 添加后总父表数不超过 10 |
| 同一 DB | 父子必须在同一数据库 |
| 禁止循环 | DAG 环路检测 |
| 已有 VCT 影响 | 若该 VST 已有 VCT，需要考虑 VCT 的 colRef 补全（新继承列无映射，查询返回 NULL） |

#### 4.5.2 取消继承

**语法**：

```sql
ALTER STABLE [db_name.]vstName DROP BASE ON [db_name.]parentVst1 [, [db_name.]parentVst2 ...];
```

行为：解除与指定父 VST 的继承关系，移除来自该父的继承列和 Tag。

**约束**：

| 约束 | 说明 |
|------|------|
| 不能完全取消 | 解除后 VST 的可用列（含 TS）至少 **2 列** |
| Tag 最少 1 个 | 解除后 VST 至少保留 **1 个 Tag** |
| 已有 VCT 影响 | 若该 VST 已有 VCT，被移除列的 colRef **级联删除**——对应列的 colRef 映射从所有 VCT 中移除，后续查询这些列返回 NULL |

**VCT 级联行为详细说明**：

当 `DROP BASE ON parentX` 移除了继承列 C1、C2 和继承 Tag T1 时：
1. VST schema 中移除 C1、C2、T1
2. 该 VST 下所有已有 VCT 中，C1、C2 对应的 colRef 映射被删除
3. T1 对应的 Tag 值被删除
4. 后续对这些 VCT 的查询不再包含 C1、C2、T1（schema 已变更）

### 4.6 DQL：查询 VST

#### 4.6.1 查询叶子 VST

```sql
SELECT ... FROM [db_name.]leafVstName [WHERE ...] [ORDER BY ...];
```

行为：正常的虚拟超级表扫描，schema 为该叶子 VST 的完整 schema（所有继承列 + 自有列）。可使用从父 VST 继承的列和 Tag 进行 SELECT、WHERE、ORDER BY 等操作。与普通 VST 查询行为一致。

#### 4.6.2 查询非叶 VST（隐式下推）

```sql
SELECT ... FROM [db_name.]parentVstName [WHERE ...] [ORDER BY ...];
```

行为：自动下推到所有以 parentVstName 为祖先的**叶子 VST**，扫描其 VCT 数据，**只投影** parentVstName 自身 schema 中的列。多个叶子 VST 的结果行合并返回。

**规则**：

| 规则 | 说明 |
|------|------|
| 投影裁剪 | 结果 schema = 被查询 VST 自身的列和 Tag（不包含子孙的扩展列） |
| 列引用限制 | SELECT/WHERE/ORDER BY 只能引用被查询 VST schema 中的列 |
| 无需特殊语法 | 查询非叶 VST 自动触发下推，无需 EXPAND 等关键字 |
| 保证列完整 | 所有叶子后代一定包含祖先的全部列（继承保证），无需 NULL 填充 |

#### 4.6.3 列可见性规则

**核心原则**：查询结果 schema 始终 = 被查询 VST 自身的 schema。叶子 VST 的扩展列在查询祖先 VST 时不可见。

### 4.7 DQL：查询继承关系

**语法**：

```sql
SHOW VSTABLE INHERITS;
```

行为：查询所有 VST 之间的继承关系。等价于 `SELECT * FROM information_schema.ins_vstable_inherits`。

返回列：db_name, stable_name, parent_stable_name, create_time。若 VST 有多个直接父 VST，`parent_stable_name` 以 `(parent1, parent2, ..., parentN)` 格式显示。

### 4.8 DQL：SHOW CREATE STABLE

**语法**：

```sql
SHOW CREATE STABLE [db_name.]vstName;
```

行为：显示 vstName 的建表语句。
- 无继承 VST → 输出标准 `CREATE STABLE ... VIRTUAL 1`
- 有继承 VST → 输出 `CREATE STABLE ... BASE ON parent1, parent2 VIRTUAL 1`

### 4.9 DCL：权限

| 场景 | 行为 |
|------|------|
| 创建子 VST | 使用子 VST 所在 DB 的标准权限检查 |
| 查询叶子 VST | 需要对该 VST 本身的 SELECT 权限 |
| 查询非叶 VST（下推） | 只需要对 FROM 子句中的 VST 的 SELECT 权限即可 |

## 5. 性能

- 叶子 VST 查询与普通 VST 查询性能一致
- 非叶 VST 查询性能与叶子后代数量成线性关系（每个叶子产生一组 VCT 扫描）
- 投影裁剪在执行器层完成，只选取祖先列对应的 slot，无额外 I/O 开销
- `SHOW VSTABLE INHERITS` 查询系统表，性能取决于继承关系数量（通常很小）

## 6. 安全

- 权限管控：非叶 VST 查询下推仅需要对 FROM 子句中 VST 的 SELECT 权限，不需要对叶子 VST 的额外授权
- 系统表安全：`ins_vstable_inherits` 仅支持查询操作，继承关系元数据不可被篡改

## 7. 兼容性

- 无继承的 VST 查询行为完全向后兼容
- 现有 VST（无 BASE ON）不受影响
- 已有 VCT 的 VST 继续正常工作，只是不能再被其他 VST 继承

## 8. 运维

1. 删除父 VST 前需先删除所有子 VST（系统会拒绝直接删除有子的 VST）
2. 父 VST `ADD COLUMN` 会自动级联到所有子孙，运维人员需注意变更影响范围
3. 通过 `SHOW VSTABLE INHERITS` 可随时查看继承关系
4. 单个 VST 最多继承 10 个父 VST

## 9. 使用场景

**场景 1：多维度设备建模（多继承）**

工厂设备同时具备"设备通用属性"和"电力指标属性"两个维度。通过多继承将两个维度组合：

```sql
CREATE STABLE base_device (ts TIMESTAMP, status INT) TAGS (region INT) VIRTUAL 1;
CREATE STABLE base_power  (ts TIMESTAMP, voltage FLOAT, current FLOAT) TAGS (phase INT) VIRTUAL 1;

-- 叶子 VST 继承两个维度
CREATE STABLE transformer (ts TIMESTAMP, capacity INT) TAGS (model BINARY(16))
    BASE ON base_device, base_power VIRTUAL 1;
-- transformer schema: (ts, status, voltage, current, capacity) TAGS(region, phase, model)
```

查询 `base_device` → 自动下推到 `transformer` 等所有叶子后代，只投影 `(ts, status)` 列。

**场景 2：纯 schema 复用（无新增列）**

多个叶子 VST 共享同一个父 VST 的 schema，不添加新列：

```sql
CREATE STABLE common_metrics (ts TIMESTAMP, cpu FLOAT, mem FLOAT) TAGS (host BINARY(32)) VIRTUAL 1;

CREATE STABLE app_server_metrics (ts TIMESTAMP) TAGS (app_name BINARY(32))
    BASE ON common_metrics VIRTUAL 1;

CREATE STABLE db_server_metrics (ts TIMESTAMP) TAGS (db_type BINARY(16))
    BASE ON common_metrics VIRTUAL 1;
```

**场景 3：动态添加继承**

已有 VST 后期需要扩展能力：

```sql
-- 初始创建
CREATE STABLE my_sensor (ts TIMESTAMP, temp FLOAT) TAGS (loc INT) VIRTUAL 1;

-- 后期增加继承
ALTER STABLE my_sensor ADD BASE ON base_device;
-- my_sensor schema 扩展为: (ts, temp, status) TAGS(loc, region)
```

## 10. 约束和限制

| 约束 | 说明 |
|------|------|
| 最大父 VST 数量 | 10 |
| 循环继承 | 禁止，创建时通过 DAG 遍历检测 |
| DROP 父 VST | 有子时拒绝 |
| 跨 DB 继承 | 不支持，父子 VST 必须在同一 DB |
| 父 VST 有 VCT | 已有 VCT 的 VST 不能被继承 |
| 叶子 VST 才有 VCT | 有子 VST 的 VST 不能创建 VCT |
| 多父列名冲突 | 所有父之间列名/Tag名冲突在解析期报错，错误消息包含冲突列名及来源父 VST |
| 一对多继承 | 一个父 VST 可被多个子 VST 同时继承，不限制继承扇出 |
| 取消继承最少保留 | 取消后至少保留 2 列（含 TS）+ 1 个 Tag |
| 订阅（TMQ/TOPIC） | 仅支持叶子 VST，非叶 VST 不支持创建 TOPIC |
| Stream | 仅支持叶子 VST 作为 Stream 的源表，非叶 VST 不支持 |

## 11. 常见错误和排查

| 错误码 | 含义 | 排查方法 |
|--------|------|----------|
| `TSDB_CODE_MND_VST_HAS_CHILDREN` | DROP/ALTER 有子 VST 的父表 | 先通过 SHOW VSTABLE INHERITS 查看子代，逐级删除 |
| `TSDB_CODE_MND_VST_PARENT_NOT_VIRTUAL` | BASE ON 目标不是虚拟表 | 确认父表是 VIRTUAL 1 |
| `TSDB_CODE_MND_VST_COL_NAME_CONFLICT` | 列名/Tag名与父表冲突或多父之间冲突。消息格式：`Column '<name>' conflicts between '<parent1>' and '<parent2>'` | 根据错误消息中指出的冲突列名及来源修改列名 |
| `TSDB_CODE_MND_VST_CIRCULAR_INHERIT` | 检测到循环继承 | 检查 BASE ON 目标是否形成环 |
| `TSDB_CODE_MND_VST_MAX_PARENTS_EXCEED` | 父 VST 数量超过 10 | 减少继承的父 VST 数量 |
| `TSDB_CODE_MND_VST_PARENT_HAS_VCT` | 父 VST 已有 VCT，不能被继承 | 先删除父 VST 的 VCT |
| `TSDB_CODE_MND_VST_NOT_LEAF` | 非叶子 VST 不能创建 VCT | 确认目标 VST 没有子 VST |
| `TSDB_CODE_MND_VST_DROP_BASE_MIN_COLS` | 取消继承后列/Tag不满足最小要求 | 保留更多继承关系或先添加自有列 |

## 12. 可观测性

- `SHOW VSTABLE INHERITS`：查看继承关系
- `DESCRIBE vst_name`：查看完整 schema（含继承列）
- `SHOW CREATE STABLE vst_name`：查看建表语句（含 BASE ON）

## 13. 安装和卸载

无特殊安装步骤，随 TDengine 版本发布。

## 14. 文档

- 需在官网文档新增"虚拟超级表继承"章节，涵盖 DDL/DQL 语法、多继承用法
- 需在系统表文档中补充 `ins_vstable_inherits` 的 schema 定义与查询示例
- 需在 SHOW 命令文档中补充 `SHOW VSTABLE INHERITS` 说明

## 15. 参考文档

无

## 16. 附录：SQL 示例

> 以下示例基于 §3 数据模型。

### 16.1 DDL 示例：创建继承关系

```sql
CREATE DATABASE demo VGROUPS 2;
USE demo;

-- 创建源数据表
CREATE STABLE src_stb (ts TIMESTAMP, c1 INT, c2 FLOAT, c3 INT) TAGS (region INT);

CREATE TABLE src_s1 USING src_stb TAGS(1);
INSERT INTO src_s1 VALUES ('2023-01-01 00:00:01', 10, 1.1, 100);
INSERT INTO src_s1 VALUES ('2023-01-01 00:00:02', 11, 1.2, 101);

CREATE TABLE src_s2 USING src_stb TAGS(2);
INSERT INTO src_s2 VALUES ('2023-01-01 00:00:03', 20, 2.1, 200);
INSERT INTO src_s2 VALUES ('2023-01-01 00:00:04', 21, 2.2, 201);

CREATE TABLE src_a1 USING src_stb TAGS(3);
INSERT INTO src_a1 VALUES ('2023-01-01 00:00:05', 30, 3.1, 300);
INSERT INTO src_a1 VALUES ('2023-01-01 00:00:06', 31, 3.2, 301);

-- 创建父 VST（无 VCT，纯 schema 定义）
CREATE STABLE base_device (ts TIMESTAMP, status INT) TAGS (region INT) VIRTUAL 1;
CREATE STABLE base_metric (ts TIMESTAMP, value FLOAT) TAGS (unit BINARY(8)) VIRTUAL 1;

-- 创建叶子 VST（多继承，自有列 accuracy）
CREATE STABLE sensor_vst (ts TIMESTAMP, accuracy INT) TAGS (sensor_id INT)
    BASE ON base_device, base_metric VIRTUAL 1;
-- sensor_vst 完整 schema: (ts, status, value, accuracy) TAGS(region, unit, sensor_id)

-- 创建叶子 VST（单继承，自有列 cmd）
CREATE STABLE actuator_vst (ts TIMESTAMP, cmd INT) TAGS (actuator_type INT)
    BASE ON base_device VIRTUAL 1;
-- actuator_vst 完整 schema: (ts, status, cmd) TAGS(region, actuator_type)

-- 在叶子 VST 下创建 VCT（colRef 覆盖继承列 + 自有列）
CREATE TABLE vct_s1 USING sensor_vst TAGS(1, 'celsius', 101)
    (status FROM demo.src_s1.c1, value FROM demo.src_s1.c2, accuracy FROM demo.src_s1.c3);
CREATE TABLE vct_s2 USING sensor_vst TAGS(2, 'celsius', 102)
    (status FROM demo.src_s2.c1, value FROM demo.src_s2.c2, accuracy FROM demo.src_s2.c3);

CREATE TABLE vct_a1 USING actuator_vst TAGS(3, 1)
    (status FROM demo.src_a1.c1, cmd FROM demo.src_a1.c3);
```

### 16.2 DQL 示例：查询叶子 VST

```sql
-- 查询 sensor_vst（叶子），返回完整宽表 schema
SELECT * FROM sensor_vst ORDER BY ts;
```

结果（schema = ts, status, value, accuracy）：

```
ts                    | status | value | accuracy
----------------------|--------|-------|--------
2023-01-01 00:00:01   | 10     | 1.1   | 100      ← vct_s1
2023-01-01 00:00:02   | 11     | 1.2   | 101      ← vct_s1
2023-01-01 00:00:03   | 20     | 2.1   | 200      ← vct_s2
2023-01-01 00:00:04   | 21     | 2.2   | 201      ← vct_s2
```

```sql
-- 查询 actuator_vst（叶子），返回完整 schema
SELECT * FROM actuator_vst ORDER BY ts;
```

结果（schema = ts, status, cmd）：

```
ts                    | status | cmd
----------------------|--------|-----
2023-01-01 00:00:05   | 30     | 300    ← vct_a1
2023-01-01 00:00:06   | 31     | 301    ← vct_a1
```

### 16.3 DQL 示例：查询非叶 VST（隐式下推）

```sql
-- 查询 base_device → 自动下推到 sensor_vst + actuator_vst 的所有 VCT
-- 只投影 base_device 的 schema (ts, status)
SELECT * FROM base_device ORDER BY ts;
```

结果：

```
ts                    | status
----------------------|-------
2023-01-01 00:00:01   | 10       ← vct_s1
2023-01-01 00:00:02   | 11       ← vct_s1
2023-01-01 00:00:03   | 20       ← vct_s2
2023-01-01 00:00:04   | 21       ← vct_s2
2023-01-01 00:00:05   | 30       ← vct_a1
2023-01-01 00:00:06   | 31       ← vct_a1
```

共 6 行（3 个 VCT 合并，只投影 base_device 的列，accuracy/value/cmd 不可见）。

```sql
-- 查询 base_metric → 只下推到 sensor_vst（actuator_vst 不继承 base_metric）
-- 只投影 base_metric 的 schema (ts, value)
SELECT * FROM base_metric ORDER BY ts;
```

结果：

```
ts                    | value
----------------------|------
2023-01-01 00:00:01   | 1.1      ← vct_s1
2023-01-01 00:00:02   | 1.2      ← vct_s1
2023-01-01 00:00:03   | 2.1      ← vct_s2
2023-01-01 00:00:04   | 2.2      ← vct_s2
```

共 4 行（actuator_vst 不继承 base_metric，不参与）。

### 16.4 DQL 示例：Tag 查询

```sql
-- 通过 base_device 查 Tag（只有 region）
SELECT tbname, region FROM base_device ORDER BY tbname;
```

结果：

```
tbname  | region
--------|-------
vct_a1  | 3
vct_s1  | 1
vct_s2  | 2
```

```sql
-- 通过 sensor_vst 查 Tag（继承 region + unit，自有 sensor_id）
SELECT tbname, region, unit, sensor_id FROM sensor_vst ORDER BY tbname;
```

结果：

```
tbname  | region | unit    | sensor_id
--------|--------|---------|----------
vct_s1  | 1      | celsius | 101
vct_s2  | 2      | celsius | 102
```

### 16.5 DQL 示例：聚合

```sql
-- base_device 下推聚合
SELECT COUNT(*) FROM base_device;
-- 结果：6

SELECT SUM(status) FROM base_device;
-- 结果：10 + 11 + 20 + 21 + 30 + 31 = 123

-- base_metric 下推聚合
SELECT AVG(value) FROM base_metric;
-- 结果：(1.1 + 1.2 + 2.1 + 2.2) / 4 = 1.65

-- GROUP BY tag（通过非叶 VST）
SELECT region, COUNT(*) AS cnt FROM base_device GROUP BY region ORDER BY region;
```

结果：

```
region | cnt
-------|----
1      | 2     ← vct_s1
2      | 2     ← vct_s2
3      | 2     ← vct_a1
```

### 16.6 DDL 示例：ALTER 继承关系

```sql
-- 已有 VST 添加继承
CREATE STABLE my_sensor (ts TIMESTAMP, temp FLOAT) TAGS (loc INT) VIRTUAL 1;
ALTER STABLE my_sensor ADD BASE ON base_device;
-- my_sensor schema 扩展为: (ts, temp, status) TAGS(loc, region)

-- 取消继承（需保留 ≥2列 + ≥1 Tag）
ALTER STABLE my_sensor DROP BASE ON base_device;
-- my_sensor schema 恢复为: (ts, temp) TAGS(loc)
```

### 16.7 DQL 示例：SHOW 语句

```sql
-- 查看继承关系
SHOW VSTABLE INHERITS;
```

结果：

```
db_name | parent_stable | parent_uid | child_stable | child_uid | create_time
--------|---------------|------------|--------------|-----------|-------------------
demo    | base_device   | 100001     | sensor_vst   | 200001    | 2023-01-01 ...
demo    | base_metric   | 100002     | sensor_vst   | 200001    | 2023-01-01 ...
demo    | base_device   | 100001     | actuator_vst | 200002    | 2023-01-01 ...
```

```sql
-- 查看建表语句
SHOW CREATE STABLE sensor_vst;
```

结果：

```
Stable          | Create Stable
----------------|------------------------------------------------------
sensor_vst      | CREATE STABLE `sensor_vst` (`ts` TIMESTAMP, `accuracy` INT)
                |   TAGS (`sensor_id` INT)
                |   BASE ON `base_device`, `base_metric` VIRTUAL 1
```

```sql
-- 查看完整 schema（含继承列）
DESCRIBE sensor_vst;
```

结果：

```
Field     | Type      | Length | Note
----------|-----------|--------|------------------
ts        | TIMESTAMP | 8      |
status    | INT       | 4      | inherited from base_device
value     | FLOAT     | 4      | inherited from base_metric
accuracy  | INT       | 4      |
region    | INT       | 4      | TAG, inherited from base_device
unit      | BINARY    | 8      | TAG, inherited from base_metric
sensor_id | INT       | 4      | TAG
```

### 16.8 DQL 示例：错误用法

```sql
-- ❌ 引用不在被查询 VST schema 中的列
SELECT accuracy FROM base_device;
-- 错误：accuracy 不在 base_device schema 中

-- ❌ 在非叶 VST 下创建 VCT
CREATE TABLE vct_bad USING base_device TAGS(99)
    (status FROM demo.src_s1.c1);
-- 错误：TSDB_CODE_MND_VST_NOT_LEAF（base_device 有子 VST）

-- ❌ 继承已有 VCT 的 VST
CREATE STABLE child_of_sensor (ts TIMESTAMP) TAGS (extra_tag INT)
    BASE ON sensor_vst VIRTUAL 1;
-- 错误：TSDB_CODE_MND_VST_PARENT_HAS_VCT（sensor_vst 已有 VCT）

-- ❌ 多父列名冲突（假设两个父都有同名非 TS 列）
CREATE STABLE base_x (ts TIMESTAMP, dup_col INT) TAGS (tx INT) VIRTUAL 1;
CREATE STABLE base_y (ts TIMESTAMP, dup_col INT) TAGS (ty INT) VIRTUAL 1;
CREATE STABLE bad_vst (ts TIMESTAMP) BASE ON base_x, base_y VIRTUAL 1;
-- 错误：TSDB_CODE_MND_VST_COL_NAME_CONFLICT
-- 消息：Column 'dup_col' conflicts between 'base_x' and 'base_y'
```

## 17. 需求对照清单

> 以下列出本期全部需求点及其在文档中的对应位置，确保无遗漏。

| # | 需求 | 覆盖章节 | 状态 |
|---|------|---------|------|
| 1 | 支持多继承：子 VST 同时继承多个父 VST | §2 背景、§4.1 规则（最大父表数 10、DAG） | ✅ |
| 2 | 列定义可选：允许不定义新增列，也允许定义额外列 | §4.1 规则（可无新增列，但须声明 TS） | ✅ |
| 3 | 列名冲突检测：解析期报错，提示冲突列名及来源 | §4.1 规则（列名/Tag名不可冲突，含错误消息格式）、§11 错误码 | ✅ |
| 4 | 支持被多个 VST 继承：一对多关系 | §3 定义、§10 约束（一对多继承，不限扇出） | ✅ |
| 5 | 继承关系动态变更：ADD/DROP BASE ON，含 VCT 级联 | §4.5.1 添加继承、§4.5.2 取消继承（含 VCT 级联行为详细说明） | ✅ |
| 6 | 主键列必须显式指定 | §4.1 规则（可无新增列，但**必须声明 TS 列**） | ✅ |
| 7 | 继承来源上限：最多 10 个父 VST | §4.1 规则（最大父表数 10）、§10 约束 | ✅ |
| 8 | 不支持 PRIVATE 列 | v0.3 修订已移除，文档中无 PRIVATE 相关内容 | ✅ |
| 9 | 仅叶子节点 VST 可创建 VCT | §3 定义（叶子 VST）、§4.2（约束）、§10 约束 | ✅ |
| 10 | 不需要 EXPAND 语法 | v0.2 修订已移除，文档中无 EXPAND 相关内容 | ✅ |

## 18. 结论

VST 继承特性通过 `BASE ON` 语法实现多继承 schema 组合，非叶 VST 查询自动下推到叶子后代并投影裁剪。整体设计保持向后兼容，对现有 VST 查询无影响。