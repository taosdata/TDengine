# 需求说明：从 CSV 批量建表

TS-4917

## 1. 引言

### 1.1 需求背景

 [产品会议 20240327](https://taosdata.feishu.cn/wiki/PVoVwRnkci3zfbk9v5McW7B2nqe) 要求在 3.3.1.0 版本支持“从 CSV 批量建表”的功能。与中国业务部沟通后，没有人了解这个需求，因此按通用功能来编写需求说明。
![](./images/img_Jw2cbCzd5o67fyxcDSVcEh6Ynsl.png)

### 1.2 优先级要求

Jeff 提出的需求，需要尽快排期。

### 1.3 版本要求

企业版支持，社区版支持。

## 2. 需求目标

CSV 文件是一张二维表，有很多行和很多列。TDengine 支持导入 CSV 文件，当不需要进行任何 Transformer 变换时，通过 taos shell 即可导入；当需要进行 Transformer 变换时，通过 taosX 导入。CSV 文件中的数据行有如下几种场景。
1. 场景 1-4 都已经支持
2. 场景 5-7 待支持，但当子表不存在时，是否更新子表标签值的行为待商榷
3. 场景 8 不支持

| 场景 | 数据行含 普通列 | 数据行含 子表名称列 | 数据行含 标签列 | 说明 | 状态 |
| --- | --- | --- | --- | --- | --- |
| 1 | 是 | 1. 导入时序数据 1. 通过 SQL 语句指定超级表名 1. 如果子表存在，不更新标签值 1. 如果子表不存在，创建子表，标签值取子表的第一行数据 | 已支持 |
| 2 | 否 | 1. 导入时序数据 1. 通过 SQL 语句指定超级表名 1. 如果子表存在，不更新标签值 1. 如果子表不存在，创建子表，标签值设置为 NULL | 已支持 |
| 3 | 是 | - | 不支持 |
| 4 | 否 | 1. 导入时序数据 1. 通过 SQL 语句指定子表名 1. 如果子表存在，不更新标签值 1. 如果子表不存在，创建子表，标签值设置为 NULL | 已支持 |
| 5 | 是 | 1. 不导入时序数据，仅建表 1. 通过 SQL 语句指定超级表名 1. 如果子表存在，更新标签，标签值取子表的最后一行数据 1. 如果子表不存在，创建子表，标签值取子表的最后一行数据 | 待支持 |
| 6 | 否 | 1. 不导入时序数据，仅建表 1. 通过 SQL 语句指定超级表名 1. 如果子表存在，不做任何处理 1. 如果子表不存在，创建子表，标签值设置为 NULL | 待支持 |
| 7 | 是 | - | 不支持 |
| 8 | 否 | - | 不支持 |

## 3. 功能需求

### 3.1 通过超级表写入数据的语法（已支持）

```sql
INSERT INTO
    stb1_name [(field1_name, ...)]       
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    [stb2_name [(field1_name, ...)]  
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    ...];
```

**使用说明**
- 执行写入语句时，insert into 支持超级表
- 当向超级表写入时，在 field_name 列表中必须指定 tbname 列，否则报错. tbname 列是字符串, 其中字符不用转义, 不能包含点 ‘.‘
- 当向超级表写入时，在批量写入语句中，对于同一个超级表，支持不同的 tbname 列
- 当向超级表写入时，在批量写入语句中，支持不同的超级表
- 当向超级表写入时，在 field_name 列表中支持标签列，当子表已经存在时，指定标签值并不会触发标签值的修改；当子表不存在时会使用所指定的标签值建立子表
- 当向超级表写入时，当 tbname 对应的子表不存在时，触发自动建子表功能，如果没有指定任何标签列，则把所有标签列的值设置为NULL

### 3.2 超级表结构定义

```sql {wrap}
drop database if exists db;
create database db vgroups 1;
use db;
create table meters (ts timestamp, current float, voltage int, phase float) tags (location varchar(64), groupId int);
```

### 3.3 场景一（已支持）

数据行含普通列，含子表名称列，含标签列。

#### 3.3.1 准备数据

```bash {wrap}
#/users/guanshengliang/downloads/auto1.csv
'2018-10-04 06:38:05.000',10.30000,219,0.31000,'California.SanFrancisco',2,'d1001'
'2018-10-05 06:38:15.000',12.60000,218,0.33000,'California.SanFrancisco',3,'d1002'
'2018-10-06 06:38:16.800',13.30000,221,0.32000,'California.SanFrancisco',2,'d1001'
'2018-10-07 06:38:05.000',13.30000,219,0.33000,'California.SanFrancisco',3,'d1002'
'2018-10-08 06:38:05.000',14.30000,219,0.34000,'California.LosAngeles',2,'d1003'
'2018-10-09 06:38:05.000',15.30000,219,0.35000,'California.LosAngeles',3,'d1004'
'2018-10-10 06:38:05.000',16.30000,219,0.31000,'California.LosAngeles',2,'d1003'
'2018-10-11 06:38:05.000',17.30000,219,0.32000,'California.LosAngeles',3,'d1004'
'2018-10-12 06:38:05.000',18.30000,219,0.31000,'California.LosAngeles',2,'d1003'
```

#### 3.3.2 导入数据

```sql {wrap}
insert into meters (ts, current, voltage, phase, location, groupId, tbname) file '/users/guanshengliang/downloads/auto1.csv';
```

### 3.4 场景二（已支持）

数据行含普通列，含子表名称列，不含标签列。

#### 3.4.1 准备数据

```bash {wrap}
#/users/guanshengliang/downloads/auto2.csv
'2018-10-04 06:38:05.000',10.30000,219,0.31000,'d1001'
'2018-10-05 06:38:15.000',12.60000,218,0.33000,'d1002'
'2018-10-06 06:38:16.800',13.30000,221,0.32000,'d1001'
'2018-10-07 06:38:05.000',13.30000,219,0.33000,'d1002'
'2018-10-08 06:38:05.000',14.30000,219,0.34000,'d1003'
'2018-10-09 06:38:05.000',15.30000,219,0.35000,'d1004'
'2018-10-10 06:38:05.000',16.30000,219,0.31000,'d1003'
'2018-10-11 06:38:05.000',17.30000,219,0.32000,'d1004'
'2018-10-12 06:38:05.000',18.30000,219,0.31000,'d1003'
```

#### 3.4.2 导入数据

```sql {wrap}
insert into meters (ts, current, voltage, phase, tbname) file '/users/guanshengliang/downloads/auto2.csv';
```

### 3.5 ~~场景三（不支持）~~

~~数据行含普通列，不含子表名称列，含标签列。~~

#### 3.5.1 ~~准备数据~~

```bash {wrap}
#/users/guanshengliang/downloads/auto3.csv
'2018-10-04 06:38:05.000',10.30000,219,0.31000,'California.SanFrancisco',2
'2018-10-05 06:38:15.000',12.60000,218,0.33000,'California.SanFrancisco',3
'2018-10-06 06:38:16.800',13.30000,221,0.32000,'California.SanFrancisco',2
'2018-10-07 06:38:05.000',13.30000,219,0.33000,'California.SanFrancisco',3
'2018-10-08 06:38:05.000',14.30000,219,0.34000,'California.LosAngeles',2
'2018-10-09 06:38:05.000',15.30000,219,0.35000,'California.LosAngeles',3
'2018-10-10 06:38:05.000',16.30000,219,0.31000,'California.LosAngeles',2
'2018-10-11 06:38:05.000',17.30000,219,0.32000,'California.LosAngeles',3
'2018-10-12 06:38:05.000',18.30000,219,0.31000,'California.LosAngeles',2
```

#### 3.5.2 ~~导入数据~~

```sql {wrap}
insert into d1001 (ts, current, voltage, phase, location, groupId) file '/users/guanshengliang/downloads/auto3.csv';
```

### 3.6 场景四（已支持）

数据行含普通列，不含子表名称列，不含标签列。

#### 3.6.1 准备数据

```bash {wrap}
#/users/guanshengliang/downloads/auto4.csv
'2018-10-04 06:38:05.000',10.30000,219,0.31000
'2018-10-05 06:38:15.000',12.60000,218,0.33000
'2018-10-06 06:38:16.800',13.30000,221,0.32000
'2018-10-07 06:38:05.000',13.30000,219,0.33000
'2018-10-08 06:38:05.000',14.30000,219,0.34000
'2018-10-09 06:38:05.000',15.30000,219,0.35000
'2018-10-10 06:38:05.000',16.30000,219,0.31000
'2018-10-11 06:38:05.000',17.30000,219,0.32000
'2018-10-12 06:38:05.000',18.30000,219,0.31000
```

#### 3.6.2 导入数据

```sql {wrap}
insert into d1001 (ts, current, voltage, phase) file '/users/guanshengliang/downloads/auto4.csv';
```

### 3.7 场景五（待支持）

数据行不含普通列，含子表名称列，含标签列。

#### 3.7.1 准备数据

```bash {wrap}
#/users/guanshengliang/downloads/auto5.csv
'California.SanFrancisco',2,'d1001'
'California.SanFrancisco',3,'d1002'
'California.SanFrancisco',2,'d1001'
'California.SanFrancisco',3,'d1002'
'California.LosAngeles',2,'d1003'
'California.LosAngeles',3,'d1004'
'California.LosAngeles',2,'d1003'
'California.LosAngeles',3,'d1004'
'California.LosAngeles',2,'d1003'
```

#### 3.7.2 导入数据

```sql {wrap}
insert into meters (location, groupId, tbname) file '/users/guanshengliang/downloads/auto5.csv';
```

### 3.8 场景六（待支持）

数据行不含普通列，含子表名称列，不含标签列。

#### 3.8.1 准备数据

```bash {wrap}
#/users/guanshengliang/downloads/auto6.csv
'd1001'
'd1002'
'd1001'
'd1002'
'd1003'
'd1004'
'd1003'
'd1004'
'd1003'
```

#### 3.8.2 导入数据

```sql {wrap}
insert into meters (tbname) file '/users/guanshengliang/downloads/auto6.csv';
```

### 3.9 ~~场景七（不支持）~~ {folded="true"}

~~数据行不含普通列，不含子表名称列，含标签列。~~

#### 3.9.1 ~~准备数据~~

```bash {wrap}
#/users/guanshengliang/downloads/auto7.csv
'California.SanFrancisco',2
'California.SanFrancisco',3
```

#### 3.9.2 ~~导入数据~~

```sql {wrap}
insert into d1001 (location, groupId) file '/users/guanshengliang/downloads/auto7.csv';
```

### 3.10 ~~场景八（不支持）~~

~~数据行不含普通列，不含子表名称列，不含标签列。~~
