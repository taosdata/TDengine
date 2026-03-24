# TS-4437 [积成电子] 标签 binary 类型超长报错不精确测试报告

## 1. 测试目标

数据库对于类型超长的列名（cloName）或标签名（tagName）是否可以正常报错：Value too long for column/tag:  colName or tagName
（新增）schemaless写入情况下是否列名/标签名可以自动扩充以及自动扩展上限是多大

## 2. 变更历史

| Date | Version | Owner | Momo |
| --- | --- | --- | --- |
| 2024-07-05 | 1.0 | @黄帅 |  |
| 2024-07-22 | 1.1 | @黄帅 | 新增schemaless相关测试 |

## 3. 测试结论

对shell中执行的语句中超长列名或标签名的正常报错测试**通过**
对stmt绑定语句超长列名或标签名的正常报错测试**通过**
（新增）对schemaless写入语句超长列名或标签名的正常报错测试**通过**
```sql {wrap}
TDengine ERROR (8000263d): Tags length exceeds max length
TDengine ERROR (8000021f): invalid key or key is too long than 64
```

（新增）schemaless写入语句varchar列名长度上限为**64个ASCII字符**
（新增）schemaless写入语句varchar标签名长度上限为**4078个ASCII字符**

## 4. 已知问题和限制

无

## 5. 测试环境

- 测试平台：Windows11的wsl的Ubuntu24.04子系统，x86_64环境
- 测试版本：开源版本3.3.3.0的3.0分支
- gitinfo：0bf5290407acf40e94c8500bb08c3a5784bbfa7a
- taos-jdbcdriver：3.3.0

## 6. 测试范围及方法

### 6.1 测试范围

- 建表语句中出现过长tagName或colName
- 插入语句中出现过长tagName或colName
- 更改tag语句中出现过长tagName或colName
- 使用stmt动态绑定时出现过长tagName或colName
- （新增）schemaless写入情况下列名/标签名是否能自动扩展以及自动扩展上限

### 6.2 测试方法

对于非stmt动态绑定形式的建表语句、插入语句和更换TAGS语句，在Linux的shell里使用语句进行测试。
对于stmt动态绑定形式的插入语句，使用python原生连接方式连接linux的taos数据库执行stmt动态绑定的插入语句进行测试。
（新增）schemaless相关测试使用java连接器taos-jdbcdriver-3.3.0连接taos数据库进行写入测试。

## 7. 测试数据

stmt动态绑定测试数据
```python
lines = [('d1001', '2018-10-03 14:38:05.000', 10, '', 2),
         ('d1001', '2018-10-03 14:38:15.000', 12, 'California', 2),
         ('d1001', '2018-10-03 14:38:16.800', 12, 'California.SanFrancisco', 2),]
```

## 8. 测试用例

### 8.1 创建数据库环境

```sql {wrap}

## 9. 创建ts_4437数据库以及ts_4437.meters超级表

CREATE DATABASE IF NOT EXISTS ts_4437;
use ts_4437;
CREATE STABLE IF NOT EXISTS ts_4437.meters (ts TIMESTAMP, id INT) TAGS (location BINARY(10), groupId INT); 
```

### 9.1 shell建表语句测试

```sql
CREATE TABLE IF NOT EXISTS ts_4437.test01 USING ts_4437.meters TAGS('zhongguokexuejishudaxue', 1);
```

### 9.2 shell插入语句测试

```sql
INSERT INTO ts_4437.test02 USING ts_4437.meters TAGS('zhongguokexuejishudaxue', 1) VALUES(now, 1);
```

### 9.3 shell更换TAGS语句测试

```sql {wrap}
ALTER TABLE test02 SET tag location="sadadbadbasda";
```

### 9.4 stmt插入语句测试

使用测试数据作为stmt的绑定参数进行插入语句测试。
```sql
INSERT INTO ts_4437.? USING ts_4437.meters TAGS(?, ?) VALUES(?, ?)
```

### 9.5 （新增）schemaless写入语句标签名超出自动扩展上限测试

```sql {wrap}
"st2,t1=\"aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzzaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmmnnnnnnnnnnooooooooooppppppppppqqqqqqqqqqrrrrrrrrrrssssssssssttttttttttuuuuuuuuuuvvvvvvvvvvwwwwwwwwwwxxxxxxxxxxyyyyyyyyyyzzzzzzzzzz12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890\",t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";
```

### 9.6 （新增）schemaless写入语句列名超出自动扩展上限测试

```sql {wrap}
"st2,t1=\"aaaaaaaaa\",t2=4f64,t3=\"t3\" c1=3i64,aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffggggg=\"passit\",c2=false,c4=4f64 1626006833639000000";
```


## 10. 问题

## 11. 测试计划

2024-07-05，2024-07-08，2024-07-22
