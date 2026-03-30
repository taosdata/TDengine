# OPC 校验讨论稿

## 1. 会议信息

会议主题：OPC 校验规则讨论
会议时间：Oct 17 (Thu) 14:58 - 15:51 (GMT+08)
参会人：@周营昭 @霍琳贺

## 2. 会议议程

## 3. 问题背景

TD-31908


TD-31926

### 3.1 原始问题

Opc 数据写入任务执行过程中报错：
```sql
insert into 
`t_3_1001` (`ts`,`val`,`quality`,`rts`) 
VALUES (1725520889000,-49.00103,0,1725520889183) 
`t_3_1003` (`ts`,`val`,`quality`,`rts`) 
VALUES (1725520889000,-0.4900103,0,1725520889183) 

[0x2603] Internal error: `Table does not exist`
```

问题溯源：
Opc 配置中针对 tag[tag::int::groupid] csv 文件中配置的值为字符串类型 `abc`, 所以无法成功创建子表，在写入 SQL 时就会有 “Table does not exist” 的错误。

### 3.2 针对 value 字段的讨论

9月5日，Wade 组织营昭、志宇讨论上述问题时引申到了 value 字段配置的校验。讨论细节后对单列模型和多列模型的 value 列的要求是不同的：
单列模型，超级表只有一个 value 字段，映射到这个超级表的点位必须配置一样的 `value 字段名`；
多列模型，超级表有多个 value 字段，映射到这个超级表的点位可以配置不同的 value 字段名；
为了实现 value 字段配置的校验，要求超级表名必须配置为常量字符串，不能再支持模版配置，比如现在支持的 `opc_{type}`。

## 4. 解决方案

### 4.1 对于原问题的解决方案

添加两项校验规则：
1. 对配置的 tag 值做校验，根据 tag::{datatype}::{tagname} 模式，获取 tag 的数据类型，对配置模版中的 tag 值做类型校验；
2. 通过 form 表单新增点位时，~~判断 pointid 是否在 opc 配置文件中已经存在~~。
3. 新增规则，和超级表冲突：int 型 tag, 但是form 中填写的是字符串

### 4.2 对 value 值校验规则

1. 超级表必须配置为明确的表名；
2. 数据模型可选择 单列/多列
   - 单列模型下，同一个超级表下点位配置的 val 字段名配置必须一致，并且在超级表中必须存在；
   - 多列模型下，超级表下点位配置的 val 字段名必须在超级表中存在

### 4.3 兼容性

单列模型中允许出现类似 opc_{type} 的字符串模板配置，遇到这类超级表名配置不做校验。
多列模型中，不允许有类似 opc_{type} 的配置。

### 4.4 问题

#### 4.4.1 校验可以解决，但没有加的问题

1. TD-31926，tag 类型和配置的值不匹配

| point_id | (point_type) | stable | tbname | val_col | val_type | tag::INT::tag_name |
| --- | --- | --- | --- | --- | --- | --- |
| ns=3;i=1001 | int | opc_int | t_{ns}_{id} | val | int | abc |

1. CSV 配置的数据类型与数据库的数据类型不一致

| point_id | (point_type) | stable | tbname | val_col | val_type | tag::INT::tag_name |
| --- | --- | --- | --- | --- | --- | --- |
| ns=3;i=1001 | int | stb | tb_123 | val | int | 123 |

表在数据库中已经存在，`create table stb (ts timestamp, val ``double``)`。
1. CSV 配置的列名与数据库的列名不一致

| point_id | (point_type) | stable | tbname | val_col | val_type | tag::INT::tag_name |
| --- | --- | --- | --- | --- | --- | --- |
| ns=3;i=1001 | int | stb | tb_123 | col_1 | int | 123 |

表在数据库中已经存在，`create table stb (ts timestamp, ``val`` int)`。

#### 4.4.2 校验解决不了的问题

1. 点位类型和配置类型不匹配

| point_id | (point_type) | stable | tbname | val_col | val_type | tag::INT::tag_name |
| --- | --- | --- | --- | --- | --- | --- |
| ns=3;i=1001 | double | opc_int | t_{ns}_{id} | val | int | 123 |

1. 当`stable = opc_{type}`时，同一类型的数据，写入到不同的列。

| point_id | (point_type) | stable | tbname | val_col | val_type | tag::INT::tag_name |
| --- | --- | --- | --- | --- | --- | --- |
| ns=3;i=1001 | double | opc_{type} | t_{ns}_{id} | col1 | double | 111 |
| ns=3;i=1002 | double | opc_{type} | t_{ns}_{id} | col2 | double | 222 |

单列模型，自动建表。
写第一条数据：`insert into t_3_1001 (ts, col1) values(ts, value)`，写入失败，表不存在；
触发自动建超级表：`create table opc_double (ts timestamp, col1 double)`；
触发自动建子表：`create table tb_3_1001 using opc_double tags(111)`；
写第一条数据：`insert into t_3_1001 (ts, col1) values(ts, value)`，写入成功；
写第二条数据：`insert into t_3_1002 (ts, col2) values(ts, value)`，写入失败，表不存在；
触发自动建子表：`create table tb_3_1002 using opc_double tags(111)`；
写第二条数据：`insert into t_3_1002 (ts, col2) values(ts, value)`，写入失败；
