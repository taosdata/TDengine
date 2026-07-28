---
sidebar_label: 虚拟表
title: 虚拟表
description: 对虚拟表的各种管理操作
---

## 虚拟表概述

虚拟表是一类逻辑表，不直接存储数据。查询虚拟表时，TDengine 会根据虚拟表定义，从一个或多个真实表或已有虚拟表中读取列数据，并按时间戳对齐后生成结果。

虚拟表包括以下几类：

- 虚拟普通表：独立存在的虚拟表，可直接定义列与数据源的对应关系。
- 虚拟超级表：作为模板定义一组虚拟子表的公共列和标签，本身不直接保存数据。
- 虚拟子表：以虚拟超级表为模板创建，列数据来自其他表，标签可以是常量，也可以引用其他表的标签。

## 创建虚拟表

`CREATE VTABLE` 语句用于创建虚拟普通表和以虚拟超级表为模板创建虚拟子表。

### 创建虚拟超级表

见 [创建超级表](./03-stable.md#创建超级表) 中的 `VIRTUAL` 参数。

### 创建虚拟普通表

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name.]vtb_name
    (
        ts_col_name timestamp,
        create_definition[ ,create_definition] ...
    )

  create_definition:
    vtb_col_name column_definition

  column_definition:
    type_name [FROM [db_name.]table_name.col_name]

```

### 创建虚拟子表

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name.]vtb_name
    (create_definition[ ,create_definition] ...)
    USING [db_name.]stb_name
    [(tag_name [, tag_name] ...)]
    TAGS (tag_value [, tag_value] ...)

  create_definition:
     [stb_col_name FROM] [db_name.]table_name.col_name
  tag_value:
     const_value
     | [tag_name] FROM [db_name.]table_name.tag_name
     | [db_name.]table_name.tag_name
```

**`tag_value` 语法说明**

- `const_value`：使用字面量常量作为标签值，与普通子表行为一致。
- `FROM [db_name.]table_name.tag_name`：标签引用（tag-ref），引用指定表的标签列。省略 `tag_name` 前缀时，目标标签名与源标签名相同；指定 `tag_name FROM ...` 时，可将源标签映射到不同名称的目标标签。
- `[db_name.]table_name.tag_name`：不带 `FROM` 的标签引用简写，常用于 `ALTER VTABLE ... SET TAG`。

此语法与列引用的 `[stb_col_name FROM] table_name.col_name` 设计一致：`FROM` 关键字前的名称为虚拟表侧的名称，`FROM` 后为数据源侧的名称。

**使用说明**

1. 虚拟表（列）名命名规则参见 [名称命名规则](../11-appendix/02-limit.md#名称命名规则)。
2. 虚拟表中列的最大个数为 32767。
3. 表名最大长度为 192。
4. 表的第一个字段必须是 `TIMESTAMP`，并且系统自动将其设为主键。
5. 表的每行长度不能超过 512 KiB。每个 `VARCHAR`、`NCHAR`、`GEOMETRY` 类型的列还会额外占用 2 个字节的存储位置。
6. 使用 `VARCHAR`、`NCHAR`、`GEOMETRY` 数据类型时，需指定其最长字节数。例如 `VARCHAR(20)` 表示最长 20 字节。
7. 虚拟表不支持 `BLOB` 和 `MEDIUMBLOB` 数据类型。
8. 创建虚拟表时使用 `FROM` 指定列的数据源，支持使用 `db_name` 跨库指定数据源。不指定 `db_name` 时默认使用当前数据库；若未使用数据库且未指定 `db_name`，则会报错。
9. 创建虚拟表时不能显式指定 `ts` 列的数据源。`ts` 列的取值是查询虚拟表时，查询语句中包含的所有列对应的原始表主键时间戳的并集。
10. 虚拟超级表下只支持创建虚拟子表，虚拟子表也只能以虚拟超级表为模板创建。
11. 创建虚拟表时需要保证虚拟表中的列、标签与指定的数据源列、标签的数据类型相同，否则会报错。
12. 在同一个数据库内，虚拟表名称不允许重名，虚拟表名和表名也不允许重名。虚拟表名和视图名允许重名但不推荐；当视图与虚拟表重名时，写入、查询、授权、回收权限等操作优先使用同名表。
13. 创建虚拟子表和虚拟普通表时，使用 `FROM` 指定某一列的数据来源时，该列支持来源于普通表、普通子表以及已有虚拟表；不支持来源于超级表、视图，也不支持来源于有复合主键的表。
14. 创建虚拟子表时，`TAGS (...)` 中的标签既可以使用常量值，也可以使用标签引用（语法见上方 `tag_value` 定义）。跨库时可写成 `db_name.table.tag`。
15. 引用相关的限制和行为详见下方“当前支持的引用能力”章节。

### 虚拟表引用能力

#### 引用的含义

“引用”指虚拟表的列或标签不存储实际数据，而是在查询时动态从源表获取数据。引用是动态绑定而非快照：

- **标签引用（tag-ref）**：虚拟子表的标签值引用另一张表的标签列，查询时实时解析为源标签的当前值。如果源表标签被 `ALTER TABLE ... SET TAG` 修改，虚拟表查询结果会立即反映新值。
- **列引用（col-ref）**：虚拟表的数据列引用另一张表（含虚拟表）的数据列，查询时从源表读取对应数据。如果源表有新数据写入，虚拟表查询即可看到。

#### 引用链

- 虚拟表列支持引用已有虚拟表的列，因此可以构建多跳引用链（虚拟表 → 虚拟表 → 物理表）。
- 标签引用与列引用可以混合多跳；当前引用深度上限为 32，超过时校验或查询会返回错误码 `0x8000620C`。
- 支持同库和跨库引用场景。

#### 变更场景下的行为

| 变更操作 | 对虚拟表的影响 |
| --- | --- |
| 源表标签被修改（`ALTER TABLE ... SET TAG`） | 标签引用查询立即反映新值 |
| 源表写入新数据 | 列引用查询可见新数据 |
| 源表被删除（`DROP TABLE`） | 虚拟表查询报错（引用源不存在） |
| 源表列被删除（`ALTER TABLE ... DROP COLUMN`） | 虚拟表查询报错（引用列不存在） |
| 虚拟表自身被删除 | 引用它的其他虚拟表查询报错 |
| 源表标签列类型变更 | 不允许：有标签引用依赖时 `ALTER` 会拒绝 |

#### 限制

- 标签引用只能引用标签列，不能引用数据列；源标签与目标标签数据类型必须一致。
- 列引用只能引用数据列，不能引用标签列；源列与目标列数据类型必须一致。
- 不支持引用超级表、视图，不支持引用有复合主键的表。
- 引用链中不允许出现环（A→B→A），创建时会校验。
- 引用深度上限为 32 跳。

## 虚拟超级表继承

虚拟超级表可以通过 `BASE ON` 子句从一个或多个父虚拟超级表继承列和标签，用于构建层次化的虚拟表模型。例如，一个设备类型虚拟超级表可以从基础设备虚拟超级表继承公共字段，再添加自身的特有列。

### 创建继承的虚拟超级表

```sql
CREATE STABLE [IF NOT EXISTS] [db_name.]stb_name
    (col_name col_type [, ...])
    [TAGS (tag_name tag_type [, ...])]
    BASE ON [db_name.]parent_stb_name [, [db_name.]parent_stb_name] ...
    VIRTUAL 1
```

`BASE ON` 子句指定一个或多个父虚拟超级表。子虚拟超级表会继承每个父虚拟超级表除主时间戳列以外的列以及全部标签，子虚拟超级表自身的列和标签追加在继承列和继承标签之后。

示例：

```sql
-- 父虚拟超级表：通用设备字段
CREATE STABLE p_device (ts timestamp, status int) TAGS (region int) VIRTUAL 1;

-- 子虚拟超级表：继承 status 和 region，增加温度列
CREATE STABLE p_temp (ts timestamp, temp float) TAGS (sensor_id int)
    BASE ON test_db.p_device VIRTUAL 1;

-- 在子虚拟超级表下创建虚拟子表
CREATE VTABLE vct_t1 (status FROM src.c1, temp FROM src.c2)
    USING test_db.p_temp TAGS (100, 1);

-- 查询子虚拟超级表（叶子）
SELECT * FROM test_db.p_temp;

-- 查询父虚拟超级表（非叶子）
SELECT count(*) FROM test_db.p_device;
```

### 修改继承关系

```sql
-- 添加父表
ALTER STABLE [db_name.]stb_name ADD BASE ON [db_name.]parent_stb_name;

-- 移除父表（该父表贡献的列和标签会被级联删除）
ALTER STABLE [db_name.]stb_name DROP BASE ON [db_name.]parent_stb_name;
```

### 非叶子虚拟超级表查询

查询非叶子虚拟超级表（即有子虚拟超级表继承的虚拟超级表）时，引擎会按其后代虚拟子表展开查询：

- `SELECT * FROM parent_vst` 返回所有后代虚拟子表的数据。
- `SELECT count(*) FROM parent_vst` 跨所有后代聚合。
- 支持多层继承（祖父→父→叶子）——查询祖父会遍历整个后代树。

### 查看继承关系

```sql
SHOW VTABLE INHERITS;
```

显示虚拟超级表之间的父子继承关系，包括数据库名、子表名、父表名。

### 约束条件

1. **仅限虚拟超级表**：`BASE ON` 要求父表和子表都是虚拟超级表（`VIRTUAL 1`）。
2. **同库限制**：父表和子表必须在同一个数据库中。
3. **列名/标签名冲突**：子表自身的列名和标签名不能与继承自任何父表的名称冲突。
4. **循环继承**：循环依赖链会被检测并拒绝。
5. **最大父表数**：一个虚拟超级表最多可继承 10 个父虚拟超级表。
6. **非叶子限制**：非叶子虚拟超级表不能直接创建虚拟子表；已有虚拟子表的虚拟超级表也不能作为 `BASE ON` 目标。
7. **Schema 变更**：有子虚拟超级表继承的父虚拟超级表不支持执行 `ADD COLUMN`、`DROP COLUMN`、`ADD TAG`、`DROP TAG`、`RENAME TAG` 和修改标签宽度等 Schema 变更。

## 查询虚拟表

虚拟表与普通表的查询语法基本一致。不同之处在于，虚拟表所呈现的数据集会根据查询中选择的列动态生成，具体规则参见下方“虚拟表数据生成规则”。

### 虚拟表数据生成规则

1. 虚拟表以时间戳为基准，对多个原始表的数据进行对齐。
2. 如果多个原始表在相同时间戳下有数据，则这些列的值组合成同一行；否则，对于缺失的列，填充 `NULL`。
3. 虚拟表的时间戳的值是查询中包含的所有列所在的原始表的时间戳的并集，因此当不同查询选择列不同时可能出现结果集行数不一样的情况。
4. 你可以从多个表中选择任意列进行组合，未选择的列不会出现在虚拟表中。

**示例**

假设有表 t1、t2、t3 结构和数据如下：

<table>
    <tr>
        <th colspan="2" align="center">t1</th>
        <th rowspan="7" align="center"></th>
        <th colspan="2" align="center">t2</th>
        <th rowspan="7" align="center"></th>
        <th colspan="3" align="center">t3</th>
    </tr>
    <tr>
        <td align="center">ts</td>
        <td align="center">value</td>
        <td align="center">ts</td>
        <td align="center">value</td>
        <td align="center">ts</td>
        <td align="center">value1</td>
        <td align="center">value2</td>
    </tr>
    <tr>
        <td align="center">0:00:01</td>
        <td align="center">1</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center">0:00:02</td>
        <td align="center">20</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center">0:00:03</td>
        <td align="center">300</td>
        <td align="center">3000</td>
    </tr>
    <tr>
        <td align="center">0:00:04</td>
        <td align="center">4</td>
        <td align="center">0:00:04</td>
        <td align="center">40</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center">0:00:05</td>
        <td align="center">50</td>
        <td align="center">0:00:05</td>
        <td align="center">500</td>
        <td align="center">5000</td>
    </tr>
</table>

并且有虚拟普通表 v1，创建方式如下：

```sql
CREATE VTABLE v1 (
    ts timestamp,
    c1 int from t1.value,
    c2 int from t2.value,
    c3 int from t3.value1,
    c4 int from t3.value2);
```

那么根据虚拟表对于多表数据的整合规则，执行如下查询时：

```sql
SELECT * FROM v1;
```

结果如下：

<table>
    <tr>
        <th colspan="5" align="center">v1</th>
    </tr>
    <tr>
        <td align="center">ts</td>
        <td align="center">c1</td>
        <td align="center">c2</td>
        <td align="center">c3</td>
        <td align="center">c4</td>
    </tr>
    <tr>
        <td align="center">0:00:01</td>
        <td align="center">1</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center">0:00:02</td>
        <td align="center"></td>
        <td align="center">20</td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center">0:00:03</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center">300</td>
        <td align="center">3000</td>
    </tr>
    <tr>
        <td align="center">0:00:04</td>
        <td align="center">4</td>
        <td align="center">40</td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center">0:00:05</td>
        <td align="center"></td>
        <td align="center">50</td>
        <td align="center">500</td>
        <td align="center">5000</td>
    </tr>
</table>

如果没有选择全部列，只是选择了部分列，查询结果只会包含所选列对应原始表的时间戳。例如执行如下查询：

```sql
SELECT c1, c2 FROM v1;
```

得到的结果如下图所示：

<table>
    <tr>
        <th colspan="5" align="center">v1</th>
    </tr>
    <tr>
        <td align="center">ts</td>
        <td align="center">c1</td>
        <td align="center">c2</td>
    </tr>
    <tr>
        <td align="center">0:00:01</td>
        <td align="center">1</td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center">0:00:02</td>
        <td align="center"></td>
        <td align="center">20</td>
    </tr>
    <tr>
        <td align="center">0:00:04</td>
        <td align="center">4</td>
        <td align="center">40</td>
    </tr>
    <tr>
        <td align="center">0:00:05</td>
        <td align="center"></td>
        <td align="center">50</td>
    </tr>
</table>

因为 `c1`、`c2` 列对应的原始表 `t1`、`t2` 中没有 `0:00:03` 这个时间戳，所以最后的结果也不会包含这个时间戳。

## 修改虚拟普通表

```sql
ALTER VTABLE [db_name.]vtb_name alter_table_clause

alter_table_clause: {
  ADD COLUMN vtb_col_name vtb_column_type [FROM table_name.col_name]
  | DROP COLUMN vtb_col_name
  | ALTER COLUMN vtb_col_name SET {table_name.col_name | NULL }
  | MODIFY COLUMN col_name column_type
  | RENAME COLUMN old_col_name new_col_name
}
```

### 使用说明

对虚拟普通表可以进行如下修改操作：

1. `ADD COLUMN`：添加列。
2. `DROP COLUMN`：删除列。
3. `MODIFY COLUMN`：修改列定义。如果数据列是可变长类型，可以使用此指令修改宽度，只能改大，不能改小。如果虚拟表该列已指定数据源，修改列宽会因为修改后的列宽与数据源列宽不匹配而报错，可以先将数据源置为空后再修改列宽。
4. `RENAME COLUMN`：修改列名称。
5. `ALTER COLUMN ... SET`：修改列的数据源。`SET NULL` 表示将虚拟表某列的数据源置为空。

### 增加列

```sql
ALTER VTABLE vtb_name ADD COLUMN vtb_col_name vtb_col_type [FROM [db_name.]table_name.col_name]
```

### 删除列

```sql
ALTER VTABLE vtb_name DROP COLUMN vtb_col_name
```

### 修改列宽

```sql
ALTER VTABLE vtb_name MODIFY COLUMN vtb_col_name data_type(length);
```

### 修改列名

```sql
ALTER VTABLE vtb_name RENAME COLUMN old_col_name new_col_name
```

### 修改列的数据源

```sql
ALTER VTABLE vtb_name ALTER COLUMN vtb_col_name SET {[db_name.]table_name.col_name | NULL}
```

## 修改虚拟子表

```sql
ALTER VTABLE [db_name.]vtb_name alter_table_clause

alter_table_clause: {
  ALTER COLUMN vtb_col_name SET table_name.col_name
  | SET TAG tag_name = {new_tag_value | [db_name.]table_name.tag_name}
}
```

**使用说明**

1. 对虚拟子表的列和标签的修改，除了更改标签值以外，都要通过虚拟超级表才能进行。

### 修改虚拟子表标签值

```sql
ALTER VTABLE tb_name SET TAG tag_name1 = new_tag_value1, tag_name2 = new_tag_value2 ...;
```

`SET TAG` 既可以把标签改成静态字面量值，也可以把标签改成标签引用。

#### 把标签设置为字面量

```sql
ALTER VTABLE v0 SET TAG local_tag='local0_updated';
```

将标签设置为字面量值时，如果该标签原本是标签引用，则其引用关系会被清除，之后该标签变为一个静态值，不再随源表变化。

#### 把标签设置为标签引用（设置或重设引用）

```sql
-- 同库引用：引用同库中 src0 表的 city 标签
ALTER VTABLE v0 SET TAG ref_city=src0.city;

-- 跨库引用：使用 db_name.table.tag 三段式
ALTER VTABLE v0 SET TAG ref_city=db1.src1.city;
```

将标签设置为标签引用后，查询时会实时解析为被引用标签的当前值。该操作既可以为一个原本是字面量的标签新增引用，也可以把一个已有的标签引用重新指向另一个源标签。约束与 `CREATE VTABLE` 时的标签引用一致：

- 被引用对象必须是标签列（普通子表或虚拟子表的标签），不能是数据列。
- 源标签与目标标签的数据类型必须一致。
- 不允许形成引用环（例如把 `v_a` 的标签指向最终又引用回 `v_a` 的虚拟表），创建或修改时会校验并报错。
- 引用链总深度不能超过 32 跳，超过会返回错误码 `0x8000620C`。

#### 多跳引用链下的重设行为

当虚拟子表的标签是多跳引用链的一环时（例如 `v2_0.l2_ref_city -> v0.ref_city -> src0.city`），可以在任意一层用 `SET TAG` 调整引用，变更会按照动态绑定的规则在查询时向上传导：

- 重设链路中间层的引用（如 `ALTER VTABLE v0 SET TAG ref_city=src1.city`），上层引用它的虚拟表查询结果会随之改变。
- 重设最上层的引用（如 `ALTER VTABLE v2_0 SET TAG l2_ref_city=db.v1.ref_city`），可以让其指向另一条引用链，甚至直接指向物理表标签从而“压平”引用链。
- 把链路中间层的标签设置为字面量，会清除该层的引用，从而切断上层对原始物理源的传导，上层将解析为该字面量值。

> **说明**：通过虚拟超级表批量修改标签的形式 `ALTER VTABLE USING stb_name SET TAG ... WHERE ...` 只接受字面量值，不支持把标签设置为标签引用。如需设置或重设标签引用，请使用上面针对单个虚拟子表的 `ALTER VTABLE vtb_name SET TAG ...` 语法。

### 修改列的数据源

```sql
ALTER VTABLE vtb_name ALTER COLUMN vtb_col_name SET {[db_name.]table_name.col_name | NULL}
```

## 删除虚拟表

```sql
DROP VTABLE [IF EXISTS] [db_name.]vtb_name;
```

## 查看虚拟表的信息

### 显示某个数据库下所有虚拟表

如下 SQL 语句可以列出当前数据库中的所有虚拟表名。

```sql
SHOW [NORMAL | CHILD] [db_name.]VTABLES [LIKE 'pattern'];
```

**使用说明**

1. 如果没有指定 `db_name`，显示当前数据库下的所有虚拟普通表和虚拟子表的信息。若没有使用数据库并且没有指定 `db_name`，则会报错 `database not specified`。可以使用 `LIKE` 对表名进行模糊匹配。`NORMAL` 指定只显示虚拟普通表信息，`CHILD` 指定只显示虚拟子表信息。
2. `SHOW TABLES` 不会返回这里展示的虚拟普通表和虚拟子表，请使用 `SHOW VTABLES` 查看。

### 显示虚拟表创建语句

```sql
SHOW CREATE VTABLE [db_name.]vtable_name;
```

显示 `vtable_name` 指定的虚拟表的创建语句。支持虚拟普通表和虚拟子表。常用于数据库迁移。对一个已经存在的虚拟表，返回其创建语句；在另一个集群中执行该语句，就能得到一个结构完全相同的虚拟表。
对于使用标签引用创建的虚拟子表，返回结果会保留对应的标签引用定义。

### 获取虚拟表结构信息

```sql
DESCRIBE [db_name.]vtb_name;
```

`DESCRIBE` 会展示虚拟表的列与标签结构；对于标签引用和列引用，结果中会包含对应的引用来源信息。

### 查看虚拟子表当前标签值

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
SHOW TAGS FROM [db_name.]child_table_name;
```

对于使用标签引用的虚拟子表，`SHOW TAGS` 返回的是当前解析后的标签值。

### 校验虚拟表引用关系

```sql
SHOW VTABLE VALIDATE FOR [db_name.]vtb_name;
```

`SHOW VTABLE VALIDATE` 用于检查虚拟普通表或虚拟子表的列和标签引用关系，并返回与 `information_schema.ins_virtual_tables_referencing` 一致的校验结果（包括 `err_code`、`err_msg`）。

### 查看虚拟普通表和虚拟子表信息

```sql
SELECT ... FROM information_schema.ins_tables WHERE type = 'VIRTUAL_NORMAL_TABLE' OR type = 'VIRTUAL_CHILD_TABLE';
```

```sql
SELECT ... FROM information_schema.ins_virtual_tables_referencing;
```

其中 `ins_virtual_tables_referencing` 可用于批量查询虚拟表列和标签的源库、源表、源列以及校验状态。

## 写入虚拟表

不支持向虚拟表中写入数据，以及不支持删除虚拟表中的数据。虚拟表只是对原始表进行运算后的计算结果，是一张逻辑表，因此只能对其进行查询，不可以写入或删除数据。

## 虚拟表与视图

虚拟表与视图看起来相似，但是有很多不同点：

| 属性 | 虚拟表（Virtual Table） | 视图（View） |
| --- | --- | --- |
| 定义 | 根据多表的列和时间戳组合规则生成逻辑表。 | 基于 SQL 查询保存查询逻辑定义。 |
| 数据来源 | 来自多个原始表，可以动态选择列，并通过时间戳对齐数据。 | 来自单个或多个表的查询结果。 |
| 数据存储 | 不实际存储数据，所有数据在查询时动态生成。 | 不实际存储数据，仅保存 SQL 查询逻辑。 |
| 时间戳处理 | 通过时间戳对齐将不同表的列整合到统一的时间轴上。 | 不提供虚拟表的时间戳对齐规则，数据由查询逻辑决定。 |
| 更新机制 | 原始表数据变更时，虚拟表查询结果动态反映变化。 | 依赖视图定义的查询逻辑，不涉及虚拟表的对齐或数据整合。 |
| 应用场景 | 时间序列对齐、跨表数据整合、多源数据对比分析等场景。 | 简化复杂查询逻辑、限制用户访问、封装业务逻辑等场景。 |

不支持虚拟表和视图之间的相互转化，如根据虚拟表建立视图或者根据视图建立虚拟表。

## 虚拟表的权限

### 权限说明

虚拟表的权限分为 `READ`、`WRITE` 两种，查询操作需要具备 `READ` 权限，对虚拟表本身的删除和修改操作需要具备 `WRITE` 权限。

### 语法

#### 授权

```sql
GRANT privileges ON [db_name.]vtable_name TO user_name
privileges: {
    ALL,
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
}
```

#### 回收权限

```sql
REVOKE privileges ON [db_name.]vtable_name FROM user_name
privileges: {
    ALL,
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
}
```

### 权限规则

1. 虚拟表的创建者和 `root` 用户默认具备所有权限。
2. 用户可以通过 `db_name.vtb_name` 来为指定的虚拟表（包括虚拟超级表和虚拟普通表）授予或回收其读写权限，不支持直接对虚拟子表授予或回收权限。
3. 虚拟子表和虚拟超级表不支持基于标签的授权（表级授权），虚拟子表继承虚拟超级表的权限。
4. 对其他用户进行授权与回收权限可以通过 `GRANT` 和 `REVOKE` 语句进行，该操作只能由 `root` 用户进行。
5. 具体相关权限控制细则总结如下：

| 序号 | 操作 | 权限要求 |
| --- | --- | --- |
| 1 | `CREATE VTABLE` | 用户对虚拟表所属数据库有 `WRITE` 权限，且用户对虚拟表数据源对应的源表或源虚拟表有 `READ` 权限。 |
| 2 | `DROP VTABLE` / `ALTER VTABLE` | 用户对虚拟表有 `WRITE` 权限；若在操作中指定列引用或标签引用的数据源，需要同时对对应的源表或源虚拟表有 `READ` 权限。 |
| 3 | `SHOW VTABLES` | 无 |
| 4 | `SHOW CREATE VTABLE` | 无 |
| 5 | `DESCRIBE VTABLE` | 无 |
| 6 | 系统表查询 | 无 |
| 7 | `SELECT FROM VTABLE` | 操作用户对虚拟表有 `READ` 权限。 |
| 8 | `GRANT` / `REVOKE` | 只有 `root` 用户有权限。 |

## 使用场景

| SQL 查询 | SQL 写入 | STMT 查询 | STMT 写入 |
| --- | --- | --- | --- |
| 支持 | 不支持 | 不支持 | 不支持 |
