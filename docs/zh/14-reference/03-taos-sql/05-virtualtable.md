---
sidebar_label: 虚拟表
title: 虚拟表
description: 对虚拟表的各种管理操作
---

## 创建虚拟表

`CREATE VTABLE` 语句用于创建虚拟普通表和以虚拟超级表为模板创建虚拟子表。

### 创建虚拟超级表

见 [创建超级表](./04-stable.md#创建超级表) 中的 `VIRTUAL` 参数。

### 创建虚拟普通表

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name].vtb_name 
    ts_col_name timestamp, 
    (create_definition[ ,create_definition] ...) 
     
  create_definition:
    vtb_col_name column_definition
    
  column_definition:
    type_name [FROM [db_name.]table_name.col_name]

```

### 创建虚拟子表

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name].vtb_name 
    (create_definition[ ,create_definition] ...) 
    USING [db_name.]stb_name 
    [(tag_name [, tag_name] ...)] 
    TAGS (tag_value [, tag_value] ...)
     
  create_definition:
    [stb_col_name FROM] [db_name.]table_name.col_name
  tag_value:
     const_value
```

###### 使用说明

1. 虚拟表（列）名命名规则参见 [名称命名规则](./91-limit.md#名称命名规则)。
2. 表名最大长度为 192。
3. 表的第一个字段必须是 TIMESTAMP，并且系统自动将其设为主键。
4. 表的每行长度不能超过 64KB（注意：每个 VARCHAR/NCHAR/GEOMETRY 类型的列还会额外占用 2 个字节的存储位置）。
5. 使用数据类型 VARCHAR/NCHAR/GEOMETRY，需指定其最长的字节数，如 VARCHAR(20)，表示 20 字节。
6. 创建虚拟表时使用 FROM 来指定列的数据源，支持使用 db_name 跨库指定数据源，不指定 db_name 时默认使用当前 use 的数据库，若不指定 db_name 且未 use 数据库，则会报错。
7. 创建虚拟表时不显式的指定 ts 列的数据源，ts 列的取值是查询虚拟表时查询语句中包含的所有列对应的原始表的主键时间戳合并的结果。
8. 虚拟超级表下只支持创建虚拟子表，虚拟子表也只能以虚拟超级表为模版来创建。
9. 创建虚拟表时需要保证虚拟表中的列、标签和指定的数据来源列、标签的数据类型相同，否则会报错。
10. 在同一个数据库内，虚拟表名称不允许重名，虚拟表名和表名也不允许重名。虚拟表名和视图名允许重名（不推荐）当出现视图与虚拟表名重名时，写入、查询、授权、回收权限等操作优先使用同名表。
11. 创建虚拟子表和虚拟普通表时，使用 FROM 指定某一列的数据来源时，该列只能来源于普通子表或普通表，不支持来源于超级表、视图或其他虚拟表。也不支持来源于有复合主键的表。

## 查询虚拟表

虚拟表与正常表无论是查询语法还是范围都没有区别，不同之处在于虚拟表所呈现的数据集在不同的查询中可能是不相同的，具体可以参考虚拟表数据生成规则。

### 虚拟表数据生成规则

1. 虚拟表以时间戳为基准，对多个原始表的数据进行对齐。
2. 如果多个原始表在相同时间戳下有数据，则这些列的值组合成同一行；否则，对于缺失的列，填充 NULL。
3. 虚拟表的时间戳的值是查询中包含的所有列所在的原始表的时间戳的并集，因此当不同查询选择列不同时可能出现结果集行数不一样的情况。
4. 用户可以从多个表中选择任意列进行组合，未选择的列不会出现在虚拟表中。

###### 示例

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
create vtable v1 (
    ts timestamp,
    c1 int from t1.value,
    c2 int from t2.value,
    c3 int from t3.value1,
    c4 int from t3.value2);
```

那么根据虚拟表对于多表数据的整合规则，执行如下查询时：

```sql
select * from v1;
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

如果没有选择全部列，只是选择了部分列，查询的结果只会包含选择的列的原始表的时间戳，例如执行如下查询：

```sql
select c1, c2 from v1;
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

因为 c1、c2 列对应的原始表 t1、t2 中没有 0:00:03 这个时间戳，所以最后的结果也不会包含这个时间戳。

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

###### 使用说明

对虚拟普通表可以进行如下修改操作

1. ADD COLUMN：添加列。
2. DROP COLUMN：删除列。
3. MODIFY COLUMN：修改列定义，如果数据列的类型是可变长类型，那么可以使用此指令修改其宽度，只能改大，不能改小。如果虚拟表该列已指定数据源，那么修改列宽会因为修改后的列宽和数据源的列宽不匹配而报错，可以先将数据源置为空后再修改列宽。
4. RENAME COLUMN：修改列名称。
5. ALTER COLUMN .. SET：修改列的数据源。SET NULL 表示将虚拟表某列的数据源置为空。

### 增加列

```sql
ALTER VTABLE vtb_name ADD COLUMN vtb_col_name vtb_col_type [FROM [db_name].table_name.col_name]
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
  | SET TAG tag_name = new_tag_value
}
```

###### 使用说明

1. 对虚拟子表的列和标签的修改，除了更改标签值以外，都要通过虚拟超级表才能进行。

### 修改虚拟子表标签值

```sql
ALTER VTABLE tb_name SET TAG tag_name1=new_tag_value1, tag_name2=new_tag_value2 ...;
```

### 修改列的数据源

```sql
ALTER VTABLE vtb_name ALTER COLUMN vtb_col_name SET {[db_name.]table_name.col_name | NULL}
```

## 删除虚拟表

```sql
DROP VTABLE [IF EXISTS] [dbname].vtb_name;
```

## 查看虚拟表的信息

### 显示某个数据库下所有虚拟表

如下 SQL 语句可以列出当前数据库中的所有虚拟表名。

```sql
SHOW [NORMAL | CHILD] [db_name.]VTABLES [LIKE 'pattern'];
```

###### 使用说明

1. 如果没有指定 db_name，显示当前数据库下的所有虚拟普通表和虚拟子表的信息。若没有使用数据库并且没有指定 db_name, 则会报错 database not specified。可以使用 LIKE 对表名进行模糊匹配。NORMAL 指定只显示虚拟普通表信息，CHILD 指定只显示虚拟子表信息。

### 显示虚拟表创建语句

```sql
SHOW CREATE VTABLE [db_name.]vtable_name;
```

显示 vtable_name 指定的虚拟表的创建语句。支持虚拟普通表和虚拟子表。常用于数据库迁移。对一个已经存在的虚拟表，返回其创建语句；在另一个集群中执行该语句，就能得到一个结构完全相同的虚拟表。

### 获取虚拟表结构信息

```sql
DESCRIBE [db_name.]vtb_name;
```

### 查看所有虚拟表信息

```sql
SELECT ... FROM information_schema.ins_tables where type = 'VIRTUAL_NORMAL_TABLE' or type = 'VIRTUAL_CHILD_TABLE';
```

## 写入虚拟表

不支持向虚拟表中写入数据，以及不支持删除虚拟表中的数据。虚拟表只是对原始表进行运算后的计算结果，是一张逻辑表，因此只能对其进行查询，不可以写入或删除数据。

## 虚拟表与视图

虚拟表与视图看起来相似，但是有很多不同点：

| 属性          |虚拟表 (Virtual Table) | 视图 (View)|
|----------------|------------------------|-----------|
| 定义            |虚拟表是一种动态数据结构，根据多表的列和时间戳组合规则生成逻辑表。 | 视图是一种基于 SQL 查询的虚拟化表结构，用于保存查询逻辑的定义。|
| 数据来源        |来自多个原始表，可以动态选择列，并通过时间戳对齐数据。 | 来自单个或多个表的查询结果，通常是一个复杂的 SQL 查询。|
| 数据存储        |不实际存储数据，所有数据在查询时动态生成。 | 不实际存储数据，仅保存 SQL 查询逻辑。|
| 时间戳处理       |通过时间戳对齐将不同表的列整合到统一的时间轴上。| 不支持时间戳对齐，数据由查询逻辑直接决定。|
| 更新机制        |动态更新，原始表数据变更时，虚拟表数据实时反映变化。| 动态更新，但依赖于视图定义的查询逻辑，不涉及对齐或数据整合。|
| 功能特性        |支持空值填充和插值（如 prev、next、linear）。 | 不支持内置填充和插值功能，需通过查询逻辑自行实现。|
| 应用场景        |时间序列对齐、跨表数据整合、多源数据对比分析等场景。| 简化复杂查询逻辑、限制用户访问、封装业务逻辑等场景。|
| 性能            |由于多表对齐和空值处理，查询复杂度可能较高，尤其在数据量大时。| 性能通常取决于视图的查询语句复杂度，与单表查询性能相似。|

不支持虚拟表和视图之间的相互转化，如根据虚拟表建立视图或者根据视图建立虚拟表。

## 虚拟表的权限

### 权限说明

虚拟表的权限分为 READ、WRITE 两种，查询操作需要具备 READ 权限，对虚拟表本身的删除和修改操作需要具备 WRITE 权限。

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

1. 虚拟表的创建者和 root 用户默认具备所有权限。
2. 用户可以通过 dbname.vtbname 来为指定的虚拟表（包括虚拟超级表和虚拟普通表）授予或回收其读写权限，不支持直接对虚拟子表授予或回收权限。
3. 虚拟子表和虚拟超级表不支持基于标签的授权（表级授权），虚拟子表继承虚拟超级表的权限。
4. 对其他用户进行授权与回收权限可以通过 GRANT 和 REVOKE 语句进行，该操作只能由 root 用户进行。
5. 具体相关权限控制细则总结如下：

| 序号 | 操作 | 权限要求                                                     |
|------|------|----------------------------------------------------------|
| 1 | CREATE VTABLE | 用户对虚拟表所属数据库有 WRITE 权限 且<br /> 用户对虚拟表的数据源对应的原始表有 READ 权限。 |
| 2 | DROP/ALTER VTABLE | 用户对虚拟表有 WRITE 权限，若要指定某一列的数据源，需要同时对数据源对应的原始表有 READ 权限。    |
| 3 |SHOW VTABLES | 无                                                        |
| 4 | SHOW CREATE VTABLE | 无                                                        |
| 5 | DESCRIBE VTABLE | 无                                                        |
| 6 | 系统表查询 | 无                                                        |
| 7 | SELECT FROM VTABLE | 操作用户对虚拟表有 READ 权限                                        |
| 8 | GRANT/REVOKE | 只有 root 用户有权限                                            |

## 使用场景

| SQL 查询 | SQL 写入 | STMT 查询 | STMT 写入 | 订阅 | 流计算 |
|---------|--------|---------|------|--------|---|
| 支持     | 不支持    | 不支持     | 不支持 | 不支持 | 支持 |
