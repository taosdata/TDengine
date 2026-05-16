# TD-21826 stmt错误处理增强测试报告

## 1. 测试目标

在进行STMT参数绑定时，只允许出现values字段和tags字段全是`?`的SQL语句。如果在这些字段中出现常量值（例如 insert into t values(1, ?)），系统将返回错误信息，并保留该错误码。在后续的STMT参数绑定过程中，如果检测到存在错误码，系统将立即返回该错误码，而不会执行后续的函数。

## 2. 变更历史

| Date | Version | Owner | Momo |
| --- | --- | --- | --- |
| 2024-07-11 | 1.0 | @黄帅 |  |

## 3. 测试结论

- 对stmt动态绑定中values或tags出现?与常量混用的情况是否报错的测试**通过**
```python
[0x0200]: no mix usage for ? and values
```

- 对stmt动态绑定中指定表名后抛出警告的测试**不通过**
<quote-container>
无法正常执行
</quote-container>

```python
python: /home/hanser/TDengine/source/util/src/tlog.c:892: taosAssertDebug: Assertion `0' failed.
```

- 对stmt动态绑定中没有tags字段会抛出警告的测试**不通过**
<quote-container>
可以正常执行但taoslog和taosdlog均未发现Warning异常警告
</quote-container>

- 对stmt动态绑定api出错后提前返回错误码的测试**通过**
```python
[0x0200]: no mix usage for ? and values
```

## 4. 已知问题和限制

## 5. 测试环境

- 测试平台：Windows11的wsl的Ubuntu24.04子系统，x86_64环境
- 测试版本：开源版本3.3.3.0.alpha的3.0分支
- 构建平台和时间：Linux-x64 2024-07-12 09:11:52 +0800
- gitinfo：9493ad96b68916de4e02343a2758597bbabddf3a

## 6. 测试范围和方法

### 6.1 测试范围

1. 在创建STMT时传入bool类型isStmtBind，如果在后续进行SQL解析时发现values和tags字段出现常量（非`?`的值），则返回错误信息，这么做涵盖了所有？和列混用的情况。
2. 在进行stmtSetTbName时，如果发现SQL中已经指定了表名，在日志中加入Warning信息，不返回错误。
3. 在进行stmtSetTbTags时，如果发现SQL中没有tags关键字，在日志中加入Warning信息，不返回错误。
4. 当之前调用STMT的API错误码不为0时，调用后续API将不会实际执行，而直接返回之前API调用产生的错误码，即提前返回错误码。

### 6.2 测试方法

使用Python原生连接taos数据库，执行stmt动态绑定插入语句，观察执行结果。

## 7. 测试数据

```sql
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

### 8.1 创建测试环境

```sql {wrap}
CREATE DATABASE if not exists td_21826 keep 36500
```

```sql {wrap}
CREATE STABLE if not exists td_21826.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)
```

### 8.2 stmt动态绑定?与常量混用测试

- tags混用
```sql {wrap}
INSERT INTO td_21826.? USING td_21826.meters TAGS(?, 1) VALUES(?, ?, ?, ?)
```

```sql {wrap}
INSERT INTO td_21826.? USING td_21826.meters TAGS('taos', ?) VALUES(?, ?, ?, ?)
```

- values混用
```sql {wrap}
INSERT INTO td_21826.? USING td_21826.meters TAGS(?, ?) VALUES(?, ?, ?, 1.0)
```

```sql {wrap}
INSERT INTO td_21826.? USING td_21826.meters TAGS(?, ?) VALUES(now(), ?, ?, ?)
```

### 8.3 stmtSetTbName指定表名后抛出警告测试

```sql
INSERT INTO td_21826.d111 USING td_21826.meters TAGS(?, ?) VALUES(?, ?, ?, ?)
```

### 8.4 stmtSetTbTags缺少tags关键字后抛出警告测试

需保证td_21826数据库和d1001数据表存在，可以事先创建表或者以插入语句的方式创建表
```sql
INSERT INTO d1001 USING meters tags('xxx', 1) VALUES (now(), 1, 2, 3);
```

```sql
INSERT INTO td_21826.? VALUES(?, ?, ?, ?)
```

### 8.5 stmt报错提前返回错误码测试

```sql
INSERT INTO td_21826.d1001 using td_21826.meters tags(?, ?) VALUES(?, ?, ?, 1)
```

在执行该语句的代码行后面加上其他的执行语句，观察上述语句下方的语句是否被执行。

## 9. 问题

## 10. 测试计划

2024-07-08、2024-07-11、2024-07-12
