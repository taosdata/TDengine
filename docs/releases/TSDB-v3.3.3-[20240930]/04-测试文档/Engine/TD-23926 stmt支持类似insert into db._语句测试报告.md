# TD-23926 stmt支持类似insert into db.?语句测试报告

## 1. 测试目标

本次测试的目标是，在进行stmt动态绑定的时候，代码是否仅支持insert into db.?类型的SQL语句，对于?.tbl db.tbl ?.?类型的SQL语句不支持。是否即使客户端在连接时指定了数据库名或者使用了use database，在进行insert into db.?执行时也会使用当前的数据库名覆盖之前的指定。

## 2. 变更历史

| Date | Version | Owner | Momo |
| --- | --- | --- | --- |
| 2024-07-04 | 1.0 | Hansen huang |  |

## 3. 测试结论

- 关于db.?的支持测试**通过**
- 关于?.tbl的支持测试不通过
- 关于db.tbl的支持测试**通过**
- 关于?.?的支持测试不通过

## 4. 已知问题和限制

无

## 5. 测试资源及环境

测试平台：windows11的wsl的Ubuntu24.04子系统，x86_64环境
测试版本：开源版本3.3.3.0的3.0分支，gitinfo：0bf5290407acf40e94c8500bb08c3a5784bbfa7a

## 6. 测试范围及方法

### 6.1 测试范围

stmt动态绑定时，insert into SQL语句对db.?、?.tbl、db.tbl、?.?类型的支持，在用户use database的情况下仍按照insert语句中指定的bd进行数据插入。
重点是测试在用户use database后stmt参数绑定的insert语句对db.?的支持。

### 6.2 测试方法

使用python原生连接方式连接taos数据库，创建power数据库、demo数据库，并在两个数据库中建立meters超级表，然后连接到demo数据库执行四种stmt参数绑定插入语句的代码，向power数据库中建立基于meters创建的表并写入数据观察返回结果。

## 7. 测试数据

```python
lines = [('d1001', '2018-10-03 14:38:05.000', 10.30000, 219, 0.31000, 'California.SanFrancisco', 2),
         ('d1001', '2018-10-03 14:38:15.000', 12.60000, 218, 0.33000, 'California.SanFrancisco', 2),
         ('d1001', '2018-10-03 14:38:16.800', 12.30000, 221, 0.31000, 'California.SanFrancisco', 2),
         ('d1002', '2018-10-03 14:38:16.650', 10.30000, 218, 0.25000, 'California.SanFrancisco', 3),
         ('d1003', '2018-10-03 14:38:05.500', 11.80000, 221, 0.28000, 'California.LosAngeles', 2),
         ('d1003', '2018-10-03 14:38:16.600', 13.40000, 223, 0.29000, 'California.LosAngeles', 2),
         ('d1004', '2018-10-03 14:38:05.000', 10.80000, 223, 0.29000, 'California.LosAngeles', 3),
         ('d1004', '2018-10-03 14:38:06.500', 11.50000, 221, 0.35000, 'California.LosAngeles', 3)]
```

## 8. 测试用例

### 8.1 创建数据库环境

```sql

## 9. 创建power数据库以及power.meters超级表

CREATE DATABASE IF NOT EXISTS power keep 36500;
CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

```sql

## 10. 创建demo数据库以及power.meters超级表

CREATE DATABASE IF NOT EXISTS demo keep 36500;
CREATE STABLE IF NOT EXISTS demo.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

### 10.1 insert语法测试

使用python连接到demo数据库，使用测试数据将参数绑定至下面的语句，并执行代码观察返回信息。

#### 10.1.1 对于db.?方式

```sql
insert into power.? using power.meters tags(?, ?) values(?, ?, ?, ?);
```

经过测试，该方式可以成功写入指定数据库的任意表。

#### 10.1.2 对于?.tbl方式

与开发@周新纪沟通后，在代码层面并没有实现绑定database的功能，因此stmt方式不支持insert 沟通后，在代码层面并没有实现绑定database的功能，因此stmt方式不支持insert into ?.? 的形式

#### 10.1.3 对于db.tbl方式

```sql
insert into power.d1000111 values(?, ?, ?, ?);
```

经过测试，该方式可以成功写入指定数据库的数据表。

#### 10.1.4 对于?.?方式

与开发@周新纪沟通后，在代码层面并没有实现绑定dbname的功能，因此stmt方式不支持insert 沟通后，在代码层面并没有实现绑定dbname的功能，因此stmt方式不支持insert into ?.? 的形式

## 11. 问题

## 12. 测试计划

2024-07-04至2024-07-05
