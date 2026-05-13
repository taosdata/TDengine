# TS-4893 TDengine 支持 Mysql 函数 Test Spec

## 1. 测试目标

测试需求文档：[TDengine 支持 Mysql 函数](https://taosdata.feishu.cn/wiki/P8Y0w9S13icde2kFZj7c4DUQnIb)
本次测试主要验证以下方面：
- TDengine增加的支持mysql函数是否可以正常运行
- TDengine支持的函数与mysql同名函数性能对比

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-07-11 | 1.0 | @黄帅 |  |

## 3. 测试结论

测试环境：192.168.0.172
测试版本：3.3.3.0.alpha
测试结论：通过，功能测试符合预期，对函数性能的优化在  追踪。
TD-22100

已提测函数（27个）：pi、round、truncate/trunc、exp、ln、mod、rand、sign、degrees、radians、char_length、char、ascii、position、trim、replcae、repeat、substrings/substr、substring_index、timediff、week、weekday、weekofyear、dayofweek、stddev_pop、var_pop、max/min
未提测函数（2个）：greatest、leastest
CI 测试用例：https://github.com/taosdata/TDengine/blob/3.0/tests/army/query/function/test_function.py

## 4. 开发质量报告

结论：增加的函数开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数（测试阻塞，无法进行） |  |
| 基础测试用例不通过 |  |
| BUG总数 |  |
| 严重BUG数 |  |

## 5. 已知问题和限制

1. 在已经提测的函数（除了rand、greatest、leastest、max\min）中，关于char函数的CI测试由于char函数有部分返回值为非打印字符导致无法编写测试用例，这部分内容由python脚本对比taos和mysql返回值来确认，详细测试结论见9.1中关于char的测试
2. 在已经提测的函数（除了rand、greatest、leastest、max\min）中，关于trim函数的CI测试在参数有多字节内容（比如中文）时，会自动转化为二进制处理导致检测返回值不通过，实际结果是正确的，这部分内容由python脚本对比taos和mysql返回值来确认，详细测试结论见9.1中关于trim函数的测试

## 6. 测试资源及环境

### 6.1 TAOS数据库环境

client_version 3.3.3.0.alpha
server_version ver:3.3.3.0.alpha
build:Linux-x64 2024-08-23 09:52:04 +0800
gitinfo:0f9f451459fe717f19165b596a94a911c820fb8b

### 6.2 MYSQL数据库环境

```plaintext {wrap}
mysql-server/noble-updates,noble-security 8.0.37-0ubuntu0.24.04.1 all
```


```shell {wrap}
(base) hanser@fjwyz:~$ sudo service mysql status
 * /usr/bin/mysqladmin  Ver 8.0.37-0ubuntu0.24.04.1 for Linux on x86_64 ((Ubuntu))
Copyright (c) 2000, 2024, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Server version          8.0.37-0ubuntu0.24.04.1
Protocol version        10
Connection              Localhost via UNIX socket
UNIX socket             /var/run/mysqld/mysqld.sock
Uptime:                 18 sec

Threads: 2  Questions: 8  Slow queries: 0  Opens: 119  Flush tables: 3  Open tables: 38  Queries per second avg: 0.444
```

## 7. 测试范围及重点

本次测试主要对需求中提到的场景进行复测及性能数据对比
本次为增加更多sql函数第一阶段的测试，主要针对一些与MySQL同名函数的测试，详细函数名见下表。主要测试内容为，是否函数达到了其约定的功能，是否taos中的下列函数与MySQL同名函数使用测试用例的结果一致，以及taos支持的MySQL函数与MySQL数据库的函数性能对比，包括大表（一亿条数据）的查询效率以及多次调用某个函数的执行效率。

| PI() | ROUND(expr[, digits]) | TRUNCATE/TRUNC(expr, digits) | EXP(expr) |
| --- | --- | --- | --- |
| LN(expr) | MOD(expr1, expr2) | RAND([seed]) | SIGN(expr) |
| DEGREES(expr) | RADIANS(expr) | GREATEST(expr1, expr2[, expr]...) | LEAST(expr1, expr2[, expr]...) |
| CHAR_LENGTH(expr) | CHAR(expr1 [, expr2] [, epxr3] ...) | ASCII(expr) | POSITION(expr1 IN expr2) |
| TRIM([[LEADING | TRAILING | BOTH] [remstr] FROM] expr) | REPLACE(expr, from_str, to_str) | REPEAT(expr, count) | SUBSTRING/SUBSTR(expr, pos [, len]) SUBSTRING/SUBSTR(expr FROM pos [FOR len]) |
| SUBSTRING_INDEX(expr, delim, count) | TIMEDIFF(expr1, expr2 [, time_unit]) | WEEK(expr [, mode]) | WEEKDAY(expr) |
| WEEKOFYEAR(expr) | DAYOFWEEK(expr) | STDDEV_POP(expr) | VAR_POP(expr) |
| MAX/MIN(expr) |  |  |  |

## 8. 测试环境准备

### 8.1 测试数据

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: BohxbHrY2oYsk9xu30McRfFanXE)

</view>


<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: EifibOqgzoR0IUxry48cwTR9njb)

</view>

### 8.2 非聚合函数测试环境准备

部分测试用例直接在shell中测试
当需要函数对整个列的数据进行操作时使用taosBenchmark的insert插入数据
或使用stmt方式写入，下面是相关代码
```python
def create_stable():
    conn = taos.connect()
    try:
        conn.execute("CREATE DATABASE IF NOT EXISTS ts_4893 keep 3650d")
        conn.execute("CREATE STABLE IF NOT EXISTS ts_4893.meters (ts timestamp, current float, voltage int, "
                     "phase float, id int, name varchar(8), s1 nchar(20), s2 nchar(20), s3 varchar(20), "
                     "s4 varchar(20)) TAGS (location BINARY(64), groupId INT)")
    finally:
        conn.close()

def bind_row_by_row(stmt: taos.TaosStmt):
    tb_name = None
    for row in lines:
        values: taos.TaosBind = taos.new_bind_params(10)  # 10 is count of columns
        values[0].timestamp(get_ts(row[0]))
        values[1].float(row[1])
        values[2].int(row[2])
        values[3].float(row[3])
        values[4].int(row[4])
        values[5].varchar(row[5])
        values[6].nchar(row[6])
        values[7].nchar(row[7])
        values[8].varchar(row[8])
        values[9].varchar(row[9])
        stmt.bind_param(values)

stmt = conn.statement("INSERT INTO ts_4893.test USING ts_4893.meters TAGS('Sunnyvale', 3) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
```

### 8.3 聚合函数所需测试环境准备

```shell {wrap}
sudo rm -r /var/lib/mysql-files/data.csv
sudo cp data.csv /var/lib/mysql-files
```


MySQL上
```sql {wrap}
mysql -uroot -p --local-infile

set global local_infile=on;

CREATE TABLE meters (
    ts DATETIME(3),
    current FLOAT,
    voltage INT,
    phase FLOAT,
    id INT,
    name VARCHAR(8),
    s1 NCHAR(20),
    s2 NCHAR(20),
    s3 VARCHAR(20),
    s4 VARCHAR(20),
    groupid int,
    location VARCHAR(64)
);

LOAD DATA LOCAL INFILE '/var/lib/mysql-files/data.csv'
INTO TABLE meters
CHARACTER SET utf8
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@timestamp, current, voltage, phase, id, name, s1, s2, s3, s4, groupid, location)
SET ts = STR_TO_DATE(@timestamp, '%Y-%m-%d %H:%i:%s.%f');
```

taos上使用taosBenchmark插入数据，相关json文件在8.1.1，也可以使用stmt方式写入，相关代码见8.2

### 8.4 性能测试环境准备

在linux运行taosbenchmark生成一亿条数据，同时导出这些数据，在MySQL中建立相同的数据表写入数据，在两个数据库中分别执行非聚合函数一万次，聚合函数一次，对比计算所需时间。这一步可以使用python辅助测试，编写循环语句执行一万次非聚合函数。

## 9. 测试用例

<source-synced align="1">

  ## 功能测试
</source-synced>

说明，在需要用到表的查询中，meters表代表超级表，其下有11张子表，每张子表1000条数据，每张表的数据一致，均来自8.1的data.csv，d1001表代表子表。

| 待测函数 | 测试用例 | 测试用例说明 | 预期输出（符合函数的预期输入的结果来自MySQL输出） | 实际输出 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| PI() | 1. PI() 1. PI('111') | 1. 无参调用时 1. 填入参数时 | 1. 3.141593 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'PI' | 1. 3.141592653589793 1. 语法错误 | **通过** |
| ROUND(expr[, digits]) | 1. ROUND() 1. ROUND(10,) 1. ROUND(, 2) 1. ROUND(10,NULL) 1. ROUND(NULL, 2) 1. ROUND(10.55, 3) 1. ROUND(10.55, 2) 1. ROUND(10.55, 1) 1. ROUND(10.55, 0) 1. ROUND(10.55) 1. ROUND(10.55, -1) 1. ROUND(10.55, -10) 1. ROUND(-10.55, 1) 1. ROUND(99, 1) 1. ROUND('abc', 2) 1. ROUND(123.23, 'a') 1. select round(current) from d1001; 1. select round(current) from meters; | 1. 无参调用时 1. digits未填写时 1. expr未填写时 1. digits为NULL时 1. expr为NULL时 1. digits大于expr的小数位数 1. digits等于expr的小数位数 1. digits大于0小于expr的小数位数时 1. digits等于0时 1. 未指定digits时 1. digits小于0时 1. digits小于零且绝对值大于expr长度时 1. 负数的舍入 1. expr为整数时 1. 非数字的舍入 1. digits不为数值类型 1. 在数值类型的列中使用该函数 1. 在超级表中使用该函数 | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'round' 1. 同1 1. 同1 1. NULL 1. NULL 1. 10.55 1. 10.55 1. 10.6 1. 11 1. 11 1. 10 1. 0 1. -10.6 1. 99 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 正常查询 1. 正常查询 | 1. 语法错误 1. 语法错误 1. 语法错误 1. NULL 1. NULL 1. 10.55 1. 10.55 1. 10.6 1. 11 1. 11 1. 10 1. 0 1. -10.6 1. 99 1. 无效的参数类型错误 1. 无效的参数类型错误 1. 正常查询 1. 正常查询 | **通过** |
| TRUNCATE/TRUNC(expr, digits) | 1. TRUNCATE() 1. TRUNCATE(99.99,) 1. TRUNCATE(, 3) 1. TRUNCATE(99.99,NULL) 1. TRUNCATE(NULL, 3) 1. TRUNCATE(99.99, 3) 1. TRUNCATE(99.99, 2) 1. TRUNCATE(99.99, 1) 1. TRUNCATE(99.99, 0) 1. TRUNCATE(99.99) 1. TRUNCATE(99.99, -1) 1. TRUNCATE(99.99, -10) 1. TRUNCATE(99, 1) 1. TRUNCATE('12', 1) 1. TRUNCATE(12, '1') 1. select truncate(current, 1) from d1001; 1. select truncate(current, 1) from meters; | 1. 无参调用时 1. digits未填写时 1. expr未填写时 1. digits为NULL时 1. expr为NULL时 1. digits大于expr的小数位数 1. digits等于expr的小数位数 1. digits大于0小于expr的小数位数时 1. digits等于0时 1. 未指定digits时 1. digits小于0时 1. digits小于零且绝对值大于expr长度时 1. expr为整数时 1. expr1为非数值类型时 1. expr2为非数值类型时 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. 同1 1. 同1 1. NULL 1. NULL 1. 99.99 1. 99.99 1. 99.9 1. 99 1. 同1 1. 90 1. 0 1. 99 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 正常查询 1. 正常查询 | 1. 语法错误 1. 语法错误 1. 语法错误 1. NULL 1. NULL 1. 99.99 1. 99.99 1. 99.9 1. 99 1. 无效的参数数量错误 1. 90 1. 0 1. 99 1. 无效的参数类型错误 1. 无效的参数类型错误 1. 正常查询 1. 正常查询 | **通过** |
| EXP(expr) | 1. EXP() 1. EXP('1') 1. EXP(NULL) 1. EXP(2) 1. EXP(0.5) 1. select exp(current) from d1001; 1. select exp(current) from meters; | 1. 无参调用时 1. expr为字符时 1. expr为NULL时 1. expr为整型数值时 1. expr为浮点型数值时 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'EXP' 1. 同1 1. NULL 1. 7.38905609893065 1. 1.6487212707001282 1. 正常查询 1. 正常查询 | 1. 语法错误 1. 无效的参数类型错误 1. NULL 1. 7.389056098930650 1. 1.648721270700128 1. 正常查询 1. 正常查询 | **通过** |
| LN(expr) | 1. LN() 1. Ln('1') 1. LN(NULL) 1. LN(10) 1. LN(PI()) 1. select ln(current) from d1001; 1. select ln(current) from meters; | 1. 无参调用时 1. expr为字符时 1. expr为NULL时 1. expr为整型数值时 1. expr为浮点型数值时 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'LN' 1. 同1 1. NULL 1. 2.302585092994046 1. 1.1447298858494002 1. 正常查询 1. 正常查询 | 1. 语法错误 1. 无效的参数类型错误 1. NULL 1. 2.302585092994046 1. 1.144729885849400 1. 正常查询 1. 正常查询 | **通过** |
| MOD(expr1, expr2) | 1. MOD() 1. MOD(, 2) 1. MOD(10,) 1. MOD(NULL, 2) 1. MOD(10,NULL) 1. MOD(10, 0) 1. MOD(10, -3) 1. MOD(10, 3) 1. MOD(10, 'a') 1. MOD('abc', 3) 1. MOD(10, 3, 1) 1. select mod(id, 3) from d1001; 1. select mod(id, 3) from meters; | 1. 无参调用时 1. expr1未指定时 1. expr2未指定时 1. expr1为NULL时 1. expr2为NULL时 1. expr2为0时 1. expr2为负数时 1. expr1、expr2均为合法输入时 1. expr2为非数值类型时 1. expr1为非数值类型时 1. 多参数调用 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. 同1 1. 同1 1. NULL 1. NULL 1. NULL 1. 1 1. 1 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 同1 1. 正常查询 1. 正常查询 | 1. 语法错误 1. 语法错误 1. 语法错误 1. NULL 1. NULL 1. NULL 1. 1 1. 1 1. 无效参数错误 1. 无效参数错误 1. 无效参数错误 1. 正常查询 1. 正常查询 | **通过** |
| RAND([seed]) | 1. RAND() 1. RAND(NULL) 1. RAND(10) 1. RAND(10)?=RAND(10) 1. RAND(0.5) 1. RAND('abc') 1. RAND(1, 2) 1. select rand(id) from d1001; 1. select rand(id) from meters; 1. select rand() from (select 1) t limit 1; | 1. 无参调用时 1. send为NULL时 1. 指定随机数种子 1. 测试随机数种子相同时随机数是否相同 1. seed为浮点数值 1. 随机数种子非数值类型 1. 多参数调用 1. 在单子表查询 1. 在超级表查询 1. rand() 与计算列结合使用 | 1. 正常查询 1. 0.15522042769493574 1. 0.6570515219653505 1. 0.6570515219653505 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'RAND' 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 正常查询 1. 与 RAND() 行为相同 1. 0.565810732341283 1. 0.565810732341283 1. 无效参数错误 1. 无效参数错误 1. 无效参数错误 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| SIGN(expr) | 1. SIGN() 1. SIGN(NULL) 1. SIGN(1) 1. SIGN(10) 1. SIGN(0) 1. SIGN(-1) 1. SIGN(-10) 1. SIGN('abc') 1. SIGN(10,1) 1. select sign(current) from d1001; 1. select sign(current) from meters; | 1. 无参调用时 1. expr为NULL时 1. expr为正且等于一时 1. expr为正且不等于一时 1. expr为0时 1. expr为负且等于负一时 1. expr为负且等于负十时 1. expr不为数值类型 1. 多参数调用 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'SIGN' 1. NULL 1. 1 1. 1 1. 0 1. -1 1. -1 1. 输入类型不符合预期报错 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'SIGN' 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 1 1. 1 1. 0 1. -1 1. -1 1. 无效参数错误 1. 无效参数错误 1. 正常查询 1. 正常查询 | **通过** |
| DEGREES(expr) | 1. DEGREES() 1. DEGREES(NULL) 1. DEGREES(PI()) 1. DEGREES('abc') 1. select degrees(phase) from d1001; 1. select degrees(phase) from meters; | 1. 无参调用时 1. expr为NULL时 1. expr为实数时 1. expr不为数值类型时 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'DEGREES' 1. NULL 1. 180 1. 输入类型不符合预期报错 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 180 1. 无效参数报错 1. 正常查询 1. 正常查询 | **通过** |
| RADIANS(EXPR) | 1. RADIANS() 1. RADIANS(NULL) 1. RADIANS(180) 1. RADIANS('abc') 1. select radians(phase) from d1001; 1. select radians(phase) from meters; | 1. 无参调用时 1. expr为NULL时 1. expr为实数时 1. expr不为数值类型时 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'RADIANS' 1. NULL 1. 3.141592653589793 1. 输入类型不符合预期报错 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 3.141592653589793 1. 无效参数报错 1. 正常查询 1. 正常查询 | **通过** |
| GREATEST(expr1, expr2[, expr]...) | 1. GREATEST() 1. GREATEST(10) 1. GREATEST(10, NULL) 1. GREATEST(10, 23) 1. GREATEST(10,23,21,31,2,2,2,2,2,2,2,2,2,2,2,2) 1. GREATEST(10, 'a') 1. GREATEST(10, 'abc', 1000000) 1. select greatest(current) from d1001; 1. select greatest(current) from meters; | 1. 无参调用时 1. 只有一个expr时 1. expr中有NULL 1. 两个expr时 1. 多个expr时 1. expr中有非数值类型时 1. expr中有非数值类型时 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'GREATEST' 1. 同1 1. NULL 1. 23 1. 31 1. a 1. abc 1. 同1 1. 同1 |  |  |
| LEAST(expr1, expr2[, expr]...) | 1. LEAST() 1. LEAST(10) 1. GREATEST(10, NULL) 1. LEAST(10, 23) 1. LEAST(10,23,21,31,2,2,2,2,2,2,2,2,2,2,2,1) 1. LEAST(10, 'a') 1. LEAST(10, 'abc', 1000000) 1. select least(current) from d1001; 1. select least(current) from meters; | 1. 无expr时 1. 只有一个expr时 1. expr中有NULL 1. 两个expr时 1. 多个expr时 1. expr中有非数值类型时 1. expr中有非数值类型时 1. expr为数值列的列名 1. 在单子表查询 1. 在超级表查询 | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'LEAST' 1. 同1 1. NULL 1. 10 1. 1 1. 10 1. 10 1. 同1 1. 同1 |  |  |
| CHAR_LENGTH(expr) | 1. CHAR_LENGTH() 1. CHAR_LENGTH(NULL) 1. CHAR_LENGTH('taos') 1. CHAR_LENGTH('涛思') 1. CHAR_LENGTH('涛思taos') 1. CHAR_LENGTH('tao's') 1. CHAR_LENGTH('tao\'s') 1. CHAR_LENGTH(123.45) 1. CHAR_LENGTH(123.45.67) 1. CHAR_LENGTH('a', 'b') 1. select char_length(s1) from d1001; 1. select char_length(s3) from d1001; 1. select char_length(s1) from meters; 1. select char_length(s3) from meters; | 1. 无参调用 1. expr为NULL时 1. expr为单字节字符时 1. expr为多字节字符时 1. expr为单字节多字节混合字符时 1. expr为不完整字符串时 1. expr含转义字符时 1. expr为数值类型时 1. expr为非合法数值类型时 1. expr不止一个时 1. expr为nchar字符列的列名（单子表） 1. expr为varchar字符列的列名（单子表） 1. expr为nchar字符列的列名（超级表） 1. expr为varchar字符列的列名（超级表） | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'CHAR_LENGTH' 1. NULL 1. 4 1. 2 1. 6 1. 同1 1. 3 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 同1 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 4 1. 2 1. 6 1. 语法错误 1. 3 1. 无效参数报错 1. 无效参数报错 1. 无效参数报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| CHAR(expr1 [, expr2] [, epxr3] ...) - 注：测试阶段使用python脚本将返回值转成bytes类型后对比 | 1. CHAR() 1. CHAR(NULL) 1. CHAR(77) 1. CHAR(77*256+77) 1. CHAR('ustc') 1. CHAR('123') 1. CHAR('a1b23') 1. CHAR(77, NULL, '123', 'taos') 1. select char(id) from d1001; 1. select char(s1) from d1001; 1. select char(s3) from d1001; 1. select char(id) from meters; 1. select char(s1) from meters; 1. select char(s3) from meters; | 1. 无参调用 1. expr为NULL 1. expr为正常数值输入 1. expr为越界数值输入 1. expr为正常字符输入 1. expr为数字字符输入 1. expr为数字和字母字符组合输入 1. expr为多个输入 1. expr为数值列的列名（单子表） 1. expr为nchar字符列的列名（单子表） 1. expr为varchar字符列的列名（单子表） 1. expr为数值列的列名（超级表） 1. expr为nchar字符列的列名（超级表） 1. expr为varchar字符列的列名（超级表） | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. 无返回值或返回值为非打印字符(HEX: 0x00) 1. M(HEX: 0x4D) 1. MM(HEX: 0x4D4D) 1. 无返回值或返回值为非打印字符(HEX: 0x00) 1. {(HEX: 0x7B) 1. 无返回值或返回值为非打印字符(HEX: 0x00) 1. M{(HEX: 0x4D7B00) 1. 正常查询 1. 正常查询（mysql结果错误抛出警告） 1. 正常查询（mysql结果错误抛出警告） 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. 无返回值或返回值为非打印字符 (脚本结果为True) 1. M (脚本结果为True) 1. MM (脚本结果为True) 1. 无返回值或返回值为非打印字符 (脚本结果为True) 1. {(脚本结果为True) 1. 无返回值或返回值为非打印字符(脚本结果为True) 1. M{(脚本结果为True) 1. 正常查询 1. 正常查询（无返回值） 1. 正常查询（无返回值） 1. 正常查询 1. 正常查询（无返回值） 1. 正常查询（无返回值） | **通过** |
| ASCII(expr) | 1. ASCII() 1. ASCII(NULL) 1. ASCII('taos') 1. ASCII('t') 1. ASCII(123) 1. ASCII(1) 1. ASCII(2) 1. ASCII(1.5) 1. ASCII('\'') 1. select ascii(name) from d1001; 1. select ascii(s1) from d1001; 1. select ascii(s3) from d1001; 1. select ascii(name) from meters; 1. select ascii(s1) from meters; 1. select ascii(s3) from meters; | 1. 无参调用 1. expr为NULL 1. expr为多字符输入 1. expr为单字符输入 1. expr为整型数值类型 1. expr为整型数值类型 1. expr为整型数值类型 1. expr为浮点型数值类型 1. expr为单引号里加转义的单引号 1. expr为字符列的列名（单子表） 1. expr为nchar字符列的列名（单子表） 1. expr为varchar字符列的列名（单子表） 1. expr为字符列的列名（超级表） 1. expr为nchar字符列的列名（超级表） 1. expr为varchar字符列的列名（超级表） | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. NULL 1. 116 1. 116 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 39 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 116 1. 116 1. 无效参数报错 1. 无效参数报错 1. 无效参数报错 1. 无效参数报错 1. 39 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| POSITION(expr1 IN expr2) | 1. POSITION('t' IN) 1. POSITION('t' IN NULL) 1. POSITION(IN 'taos') 1. POSITION(NULL IN 'taos') 1. POSITION('t' IN 'taos') 1. POSITION('ustc' IN 'taos') 1. POSITION('' IN '') 1. POSITION('' IN 'taos') 1. POSITION(1 IN 2213) 1. POSITION(1 IN '2213') 1. POSITION('1' IN 2213) 1. select position(s2 in s1) from d1001; 1. select position(s2 in s3) from d1001; 1. select position(s4 in s1) from d1001; 1. select position(s4 in s3) from d1001; 1. select position(s2 in s1) from meters; 1. select position(s2 in s3) from meters; 1. select position(s4 in s1) from meters; 1. select position(s4 in s3) from meters; | 1. expr2为空 1. expr2为NULL 1. expr1为空 1. expr1为NULL 1. expr1在expr2中存在 1. expr1在expr2中不存在 1. expr1和expr2均为空串 1. expr1为空串 1. expr均为数值类型 1. expr1为数值类型，expr2为字符型 1. expr2为数值类型，expr1为字符型 1. expr1和expr2为nchar列（单子表） 1. expr1为nchar列，expr2为varchar列（单子表） 1. expr1为varchar列，expr2为nchar列（单子表） 1. expr1和expr2为varchar列（单子表） 1. expr1和expr2为nchar列（超级表） 1. expr1为nchar列，expr2为varchar列（超级表） 1. expr1为varchar列，expr2为nchar列（超级表） 1. expr1和expr2为varchar列（超级表） | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. NULL 1. 同1 1. NULL 1. 1 1. 0 1. 1 1. 1 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 语法错误 1. NULL 1. 1 1. 0 1. 1 1. 1 1. 无效参数报错 1. 无效参数报错 1. 无效参数报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| TRIM([[LEADING | TRAILING | BOTH] [remstr] FROM] expr) - 注：返回值的空格符均使用replace函数替换为下划线，若expr有空格但结果无空格会另行说明 | 1. TRIM() 1. TRIM(NULL) 1. TRIM(' A ') 1. TRIM(' 涛思 ') 1. TRIM('a' FROM 'aaab bbba') 1. TRIM(LEADING FROM ' aaa ') 1. TRIM(LEADING 'a' FROM ' aaa abab aaaa ') 1. TRIM(LEADING 'a' FROM 'aaa abab aaaa ') 1. TRIM(LEADING '北' FROM '北京涛思数据科技有限公司北') 1. TRIM(LEADING '北' FROM '北bei京涛思数据科技有限公司北') 1. TRIM(TRAILING FROM ' aaa abab aaaa ') 1. TRIM(TRAILING 'a' FROM 'aaa abab aaaa') 1. TRIM(TRAILING 'a' FROM ' aaa abab aaaa') 1. TRIM(TRAILING '北' FROM '北京涛思数据科技有限公司北') 1. TRIM(TRAILING '北' FROM '北京涛思数据科技有限公司bei北') 1. TRIM(BOTH FROM ' aaa abab aaaa ') 1. TRIM(BOTH 'a' FROM ' aaa abab aaaa ') 1. TRIM(BOTH 'a' FROM 'aaa abab aaaa') 1. TRIM(BOTH '北' FROM '北京涛思数据科技有限公司北') 1. TRIM(123) 1. TRIM(BOTH 1 FROM 123) 1. select trim(s2 from s1) from d1001; 1. select trim(s2 from s3) from d1001; 1. select trim(s4 from s1) from d1001; 1. select trim(s4 from s3) from d1001; 1. select trim(s2 from s1) from meters; 1. select trim(s2 from s3) from meters; 1. select trim(s4 from s1) from meters; 1. select trim(s4 from s3) from meters; | 1. 无参调用 1. 参数为NULL时 1. 不使用任何关键字 1. expr含多字节字符 1. 只使用FROM关键字 1. 使用LEADING关键字但不指定字符 1. 使用LEADING关键字并指定字符但expr首字符不是指定关键字 1. 使用LEADING关键字并指定字符且expr首字符是指定关键字 1. 使用LEADING关键字且remstr与expr均为多字节字符 1. 使用LEADING关键字且remstr为多字节字符，expr为单字节多字节混合字符 1. 使用TRAILING关键字但不指定字符 1. 使用TRAILING关键字并指定字符且expr首字符为指定关键字 1. 使用TRAILING关键字并指定字符且expr首字符不是指定关键字 1. 使用TRAILING关键字且remstr和expr均为多字节字符 1. 使用TRAILING关键字且remstr为多字节字符，expr为单字节多字节混合字符 1. 使用BOTH关键字但不指定字符 1. 使用BOTH关键字并指定字符但expr首尾均不是指定关键字 1. 使用BOTH关键字并指定字符且expr首尾均不是指定关键字 1. 使用BOTH关键字且remstr与expr均为多字节字符 1. expr为数值类型 1. remstr与expr为数值类型 1. remstr和expr为nchar列（单子表） 1. remstr为nchar列，expr为varchar列（单子表） 1. remstr为varchar列，expr为nchar列（单子表） 1. remstr和expr为varchar列（单子表） 1. remstr和expr为nchar列（超级表） 1. remstr为nchar列，expr为varchar列（超级表） 1. remstr为varchar列，expr为nchar列（超级表） 1. remstr和expr为varchar列（超级表） | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. NULL 1. A (左右均无空格) 1. 涛思 (左右均无空格) 1. b___bbb 1. aaa______ 1. ___aaa____abab___aaaa___ 1. ____abab___aaaa___ 1. 京涛思数据科技有限公司北 1. bei京涛思数据科技有限公司北 1. ___aaa____abab___aaaa 1. aaa____abab___ 1. ___aaa____abab___ 1. 北京涛思数据科技有限公司 1. 北京涛思数据科技有限公司bei 1. aaa____abab___aaaa 1. ___aaa____abab___aaa 1. ____abab___ 1. 京涛思数据科技有限公司 1. 输入类型与预期不符报错 1. 输入类型与预期不符报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. A 1. 涛思 1. b___bbb 1. aaa______ 1. ___aaa____abab___aaaa___ 1. ____abab___aaaa___ 1. 京涛思数据科技有限公司北 1. bei京涛思数据科技有限公司北 1. ___aaa____abab___aaaa 1. aaa____abab___ 1. ___aaa____abab___ 1. 北京涛思数据科技有限公司 1. 北京涛思数据科技有限公司bei 1. aaa____abab___aaaa 1. ___aaa____abab___aaaa____ 1. ____abab___ 1. 京涛思数据科技有限公司 1. 无效参数报错 1. 无效参数报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| REPLACE(expr, from_str, to_str) | 1. REPLACE() 1. REPLACE(NULL,'aa' , 'ee') 1. REPLACE('aabbccdd', NULL, 'ee') 1. REPLACE('aabbccdd', , 'ee') 1. REPLACE('aabbccdd','aa' ,) 1. REPLACE(,'aa' , 'ee') 1. REPLACE('aabbccdd','aa' , 'ee') 1. REPLACE('aabbccdd','AA' , 'ee') 1. REPLACE(123345,1, 'ee') 1. REPLACE('aabbccdd',1 , 'ee') 1. REPLACE('aabbccdd','a' , 1) 1. REPLACE('北京','北' , '南') 1. REPLACE('北京','京' , '南') 1. REPLACE('北京taos','北' , '南') 1. select replace(s1, s2, 't') from d1001; 1. select replace(s1, s4, 't') from d1001; 1. select replace(s3, s2, 't') from d1001; 1. select replace(s3, s4, 't') from d1001; 1. select replace(s1, s2, s3) from d1001; 1. select replace(s1, s2, 't') from meters; 1. select replace(s1, s4, 't') from meters; 1. select replace(s3, s2, 't') from meters; 1. select replace(s3, s4, 't') from meters; 1. select replace(s1, s2, s3) from meters; | 1. 无参调用时 1. expr为NULL时 1. from_str为NULL时 1. from_str时 1. to_str时 1. 缺失expr时 1. expr、from_str、to_str均是合法单字节字符 1. 测试是否大小写敏感 1. expr为数值类型时 1. from_str为数值类型时 1. to_str为数值类型时 1. expr、from_str、to_str均是合法多字节字符 1. expr、from_str、to_str均是合法多字节字符 1. expr、from_str、to_str为合法单字节多字节字符混合 1. expr为nchar，from_str为nchar（单子表） 1. expr为nchar，from_str为varchar（单子表） 1. expr为varchar，from_str为nchar（单子表） 1. expr为varchar，from_str为varchar（单子表） 1. expr为nchar，from_str为nchar，to_str为varchar（单子表） 1. expr为nchar，from_str为nchar（超级表） 1. expr为nchar，from_str为varchar（超级表） 1. expr为varchar，from_str为nchar（超级表） 1. expr为varchar，from_str为varchar（超级表） 1. expr为nchar，from_str为nchar，to_str为varchar（超级表） | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. NULL 1. NULL 1. 同1 1. 同1 1. 同1 1. eebbccdd 1. aabbccdd 1. 输入类型与预期不符报错 1. 输入类型与预期不符报错 1. 输入类型与预期不符报错 1. 南京 1. 北南 1. 南京taos 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. NULL 1. 语法错误 1. 语法错误 1. 语法错误 1. eebbccdd 1. aabbccdd 1. 无效参数 1. 无效参数 1. 无效参数 1. 南京 1. 北南 1. 南京taos 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| REPEAT(expr, count) | 1. REPEAT() 1. REPEAT('taos', NULL) 1. REPEAT(NULL, 3) 1. REPEAT('taos', ) 1. REPEAT(, 3) 1. REPEAT('taos') 1. REPEAT(1) 1. REPEAT('taos', 0) 1. REPEAT('taos', 1) 1. REPEAT(taos, 1) 1. REPEAT('taos', 2) 1. REPEAT(123, 2) 1. REPEAT('taos', 1.5) 1. REPEAT('taos', 1.4) 1. REPEAT('taos', 12, 3) 1. select repeat(name, 3) from d1001; 1. select repeat(id, 3) from d1001; 1. select repeat(s1, 3) from d1001; 1. select repeat(s3, 3) from d1001; 1. select repeat(name, groupid) from d1001; 1. select repeat(s1, groupid) from d1001; 1. select repeat(name, 3) from meters; 1. select repeat(id, 3) from meters; 1. select repeat(s1, 3) from meters; 1. select repeat(s3, 3) from meters; 1. select repeat(name, groupid) from meters; 1. select repeat(s1, groupid) from meters; | 1. 无参调用时 1. count为NULL时 1. expr为NULL时 1. count缺失 1. expr缺失 1. 缺失count时 1. 缺失expr时 1. count<1时 1. count=1时 1. expr为非数值和字符类型 1. count>1时 1. expr为数值类型时 1. count为非整数类型且四舍五入是向上取整时 1. count为非整数类型且四舍五入是向下取整时 1. 超出定义的参数数量 1. expr为varchar字符列的列名（单子表） 1. expr为数值列的列名（单子表） 1. expr为nchar字符列的列名（单子表） 1. expr为varchar字符列的列名（单子表） 1. count为标签列的列名（单子表） 1. count为表中整型数值列的列名（单子表） 1. expr为varchar字符列的列名（超级表） 1. expr为数值列的列名（超级表） 1. expr为nchar字符列的列名（超级表） 1. expr为varchar字符列的列名（超级表） 1. count为标签列的列名（超级表） 1. count为表中整型数值列的列名（超级表） | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. NULL 1. NULL 1. 同1 1. 同1 1. 同1 1. 同1 1. (无返回值) 1. taos 1. ERROR 1054 (42S22): Unknown column 'taos' in 'field list' 1. taostaos 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 同1 1. 正常查询 1. 输入类型不符合预期报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 输入类型不符合预期报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. NULL 1. 语法错误 1. 语法错误 1. 无效参数报错 1. 无效参数报错 1. (无返回值) 1. taos 1. DB error: Invalid column name: taos (0.000282s) 1. taostaos 1. 无效参数类型报错 1. 无效参数类型报错 1. 无效参数类型报错 1. 无效参数数量报错 1. 正常查询 1. 无效参数类型报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 无效参数类型报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| SUBSTRING/SUBSTR(expr, pos [, len]) SUBSTRING/SUBSTR(expr FROM pos [FOR len]) | 1. SUBSTRING() 1. SUBSTRING('tdengine', NULL, 3) 1. SUBSTRING(NULL, 1, 3) 1. SUBSTRING('tdengine', 1, NULL) 1. SUBSTRING('tdengine', , 3) 1. SUBSTRING(, 1, 3) 1. SUBSTRING('tdengine', 1, ) 1. SUBSTRING('tdengine', 0) 1. SUBSTRING('tdengine', 10) 1. SUBSTRING('tdengine', 2) 1. SUBSTRING('tdengine', 8) 1. SUBSTRING('tdengine', 1, 3) 1. SUBSTRING('tdengine', 2, 99) 1. SUBSTRING('tdengine', 1, 0) 1. SUBSTRING('tdengine', 1, -1) 1. SUBSTRING('tdengine', -1, 10) 1. SUBSTRING(9876543, 1, 3) 1. SUBSTRING('tdengine', '1', 3) 1. SUBSTRING('tdengine', 1, '3') 1. SUBSTRING('中国', 1, 3) 1. SUBSTRING('中国tdengine', 1, 3) 1. SUBSTRING('tdengine', 1, 3, 4) 1. select substring(s3 , 1, 5) from d1001; 1. select substring(s1 , 1, 5) from d1001; 1. select substring(s3 , 1, 5) from meters; 1. select substring(s1 , 1, 5) from meters; | 1. 无参调用 1. pos为NULL 1. expr为NULL 1. len为NULL 1. pos缺失 1. expr缺失 1. len缺失 1. pos为0 1. pos大于expr长度 1. pos介于0与expr长度之间 1. pos等于expr长度 1. 正常调用 1. pos+len>len(expr) 1. len小于1 1. len为负值 1. pos为负数 1. expr为数值类型 1. pos为非数值类型 1. len为非数值类型 1. expr为多字节字符 1. expr为单字节多字节字符组合 1. 超出定义的参数数量 1. expr为varchar字符列的列名（单子表） 1. expr为nchar字符列的列名（单子表） 1. expr为varchar字符列的列名（超级表） 1. expr为nchar字符列的列名（超级表） | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. NULL 1. NULL 1. NULL 1. 同1 1. 同1 1. 同1 1. (无返回值) 1. (无返回值) 1. dengine 1. e 1. tde 1. dengine 1. (无返回值) 1. (无返回值) 1. e 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 中国 1. 中国t 1. 同1 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. NULL 1. NULL 1. 语法错误 1. 语法错误 1. 语法错误 1. (无返回值) 1. (无返回值) 1. dengine 1. e 1. tde 1. dengine 1. (无返回值) 1. (无返回值) 1. e 1. 无效的参数类型报错 1. 无效的参数类型报错 1. 无效的参数类型报错 1. 中国 1. 中国t 1. 无效的参数数量报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| SUBSTRING_INDEX(expr, delim, count) | 1. SUBSTRING_INDEX() 1. SUBSTRING_INDEX(NULL, '.', 2) 1. SUBSTRING_INDEX('www.taosdata.com', NULL, 2) 1. SUBSTRING_INDEX('www.taosdata.com', '.', NULL) 1. SUBSTRING_INDEX(, '.', 2) 1. SUBSTRING_INDEX('www.taosdata.com', , 2) 1. SUBSTRING_INDEX('www.taosdata.com', '.', ) 1. SUBSTRING_INDEX('www.taosdata.com', '.', 2) 1. SUBSTRING_INDEX('www.taosdata.com', '.', -2) 1. SUBSTRING_INDEX('www.taosdata.com', '.', 0) 1. SUBSTRING_INDEX('中国.科学.www.taosdata.com', '.', 2) 1. SUBSTRING_INDEX('北京。涛思。数据。科技', '。', 2) 1. SUBSTRING_INDEX(123456789, '7', 1) 1. SUBSTRING_INDEX('www.taosdata.com', c, 0) 1. SUBSTRING_INDEX('www.taosdata.com', '.', '2') 1. SUBSTRING_INDEX('www.taosdata.com', '.', 2, 3) 1. select substring_index(s1, 'a', 2) from d1001; 1. select substring_index(s1, s2, 2) from d1001; 1. select substring_index(s1, s4, 2) from d1001; 1. select substring_index(s3, s2, 2) from d1001; 1. select substring_index(s3, s4, 2) from d1001; 1. select substring_index(s3, s4, id) from d1001; 1. select substring_index(s1, 'a', 2) from meters; 1. select substring_index(s1, s2, 2) from meters; 1. select substring_index(s1, s4, 2) from meters; 1. select substring_index(s3, s2, 2) from meters; 1. select substring_index(s3, s4, 2) from meters; 1. select substring_index(s3, s4, id) from meters; | 1. 无参调用 1. expr为NULL 1. delim为NULL 1. count为NULL 1. expr缺失 1. delim缺失 1. count缺失 1. count为整数的正常调用 1. count为负数的正常调用 1. count为0 1. expr为多字节字符与单字节字符混合 1. expr多字节字符 1. expr为非字符类型 1. delim为非字符类型 1. count为数值类型 1. 超出定义的参数数量 1. expr为字符列，delim字符串，count为数字（单子表） 1. expr为nchar字符列，delim是nchar字符列，count为数字（单子表） 1. expr为nchar字符列，delim是varchar字符列，count为数字（单子表） 1. expr为varchar字符列，delim是b'ca'h字符列，count为数字（单子表） 1. expr为varchar字符列，delim是varchar字符列，count为数字（单子表） 1. expr为varchar字符列，delim是varchar字符列，count为数字列（单子表） 1. expr为字符列，delim字符串，count为数字（超级表） 1. expr为nchar字符列，delim是nchar字符列，count为数字（超级表） 1. expr为nchar字符列，delim是varchar字符列，count为数字（超级表） 1. expr为varchar字符列，delim是b'ca'h字符列，count为数字（超级表） 1. expr为varchar字符列，delim是varchar字符列，count为数字（超级表） 1. expr为varchar字符列，delim是varchar字符列，count为数字列（超级表） | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'SUBSTRING_INDEX' 1. NULL 1. NULL 1. NULL 1. 同1 1. 同1 1. 同1 1. www.taosdata 1. taosdata.com 1. (无返回值) 1. 中国.科学 1. 北京。涛思 1. 输入类型不符合预期报错 1. ERROR 1054 (42S22): Unknown column 'c' in 'field list' 1. 输入类型不符合预期报错 1. 输入类型不符合预期报错 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. NULL 1. NULL 1. 语法错误 1. 语法错误 1. 语法错误 1. www.taosdata 1. taosdata.com 1. (无返回值) 1. 中国.科学 1. 北京。涛思 1. 无效的参数类型 1. 无效列名错误 1. 无效的参数类型 1. 无效的参数数量 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| TIMEDIFF(expr1, expr2 [, time_unit]) | 1. TIMEDIFF() 1. TIMEDIFF(NULL, '2022-01-01 08:00:01',1s) 1. TIMEDIFF('2022-01-01 08:00:00', NULL,1s) 1. TIMEDIFF('2022-01-01 08:00:00', '2022-01-01 08:00:01',NULL) 1. TIMEDIFF(, '2022-01-01 08:00:01',1s) 1. TIMEDIFF('2022-01-01 08:00:00', ,1s) 1. TIMEDIFF('2022-01-01 08:00:00', '2022-01-01 08:00:01',) 1. TIMEDIFF('2022-01-01 08:00:00', '2022-01-01 08:00:10',1s) 1. TIMEDIFF('2023-01-01 08:00:00', '2022-01-01 08:00:00',1s) 1. TIMEDIFF('2022-01-01 08:00:03', '2022-01-01 08:00:00',1b) 1. TIMEDIFF('2022-01-01 08:00:03', '2022-01-01 08:00:00',1u) 1. TIMEDIFF('2022-01-01 08:00:03', '2022-01-01 08:00:00',1a) 1. TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00',1m) 1. TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00',1h) 1. TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00',1d) 1. TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00',1w) 1. TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00') 1. TIMEDIFF('2022-01-31 08:00:0', '2022-01-01 08:00:00',1s) 1. TIMEDIFF('2022/01/31', '2022/01/01',1s) 1. TIMEDIFF('2022-01-31', '2022-01-01',1s) 1. TIMEDIFF('20220131', '20220101',1s) 1. TIMEDIFF('22/01/31', '22/01/01',1s) 1. TIMEDIFF('01/31/22', '01/01/22',1s) 1. TIMEDIFF('31-JAN-22', '01-JAN-22',1s) 1. TIMEDIFF('22/01/31', '22/01/01') 1. TIMEDIFF('www', 'ttt') 1. TIMEDIFF(1720769589, 1720769529, 1s) 1. TIMEDIFF(1720769589123, 1720769529123, 1s) 1. TIMEDIFF(1720769589, '2022-01-01 08:00:00', 1s) 1. TIMEDIFF('2022-01-01 08:00:00', 1720769589, 1s) 1. TIMEDIFF(1720769589231, '2022-01-01 08:00:00', 1s) 1. TIMEDIFF('2022-01-01 08:00:00', 1720769589123, 1s) 1. TIMEDIFF(1720769589123, 1720769529123, 2s) 1. select timediff(ts, 1720769589123) from d1001; 1. select timediff(ts, 1720769589123) from meters; | 1. 无参调用 1. expr1为NULL 1. expr2为NULL 1. expr3为NULL 1. expr1缺失 1. expr2缺失 1. expr3缺失 1. 是否支持秒（s）单位 1. 是否支持expr1后于expr2 1. 是否支持纳秒（b）单位 1. 是否支持微秒（us）单位 1. 是否支持毫秒（a）单位 1. 是否支持分（m）单位 1. 是否支持小时（h）单位 1. 是否支持天（d）单位 1. 是否支持周（w）单位 1. 未指定time_uint 1. 不完整的时间类型expr1 1. dd/mm/yyyy时间类型 1. yyyy-mm-dd时间类型 1. yyyymmdd时间类型 1. yy/mm/dd时间类型 1. mm/dd/yy时间类型 1. dd-mon-yy时间类型 1. 无unit的yy/mm/dd时间类型 1. 错误的时间类型 1. unix秒时间戳类型 1. unix毫秒时间戳类型 1. unix秒时间戳与符合日期格式的VARCHAR类型混用 1. unix秒时间戳与符合日期格式的VARCHAR类型混用 1. unix毫秒时间戳与符合日期格式的VARCHAR类型混用 1. unix毫秒时间戳与符合日期格式的VARCHAR类型混用 1. time_unit不属于指定参数范围 1. 对表中ts列与固定时间戳对比查询（单子表） 1. 对表中ts列与固定时间戳对比查询（超级表） | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'TIMEDIFF' 1. 同1 1. 同1 1. 同1 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ', '2022-01-01 08:00:01',1s)' at line 1 1. 同5 1. 同5 1. -10 1. 31536000 1. 3000000000 1. 3000000 1. 3000 1. 43200 1. 720 1. 30 1. 4 1. 2592000000（取决于使用的表的精度） 1. 2592000 1. NULL 1. 2592000 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 60 1. 60 1. 79774389 1. -79774389 1. 79774389 1. -79774389 1. error 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. NULL 1. NULL 1. 语法错误 1. 语法错误 1. 语法错误 1. -10 1. 31536000 1. 3000000000 1. 3000000 1. 3000 1. 43200 1. 720 1. 30 1. 4 1. 2592000000000000（取决于使用的表的精度） 1. 2592000 1. NULL 1. 2592000 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 60 1. 60 1. 79774389 1. -79774389 1. 79774389 1. -79774389 1. DB error: TIMEDIFF function time unit parameter should be one of the following: [1b, 1u, 1a, 1s, 1m, 1h, 1d, 1w] 1. 正常查询 1. 正常查询 | **通过** |
| WEEK(expr [, mode]) | 1. WEEK() 1. WEEK(,0) 1. WEEK('2000-01-01',) 1. WEEK(NULL,0) 1. WEEK('2000-01-01',NULL) 1. WEEK('abc') 1. WEEK(123) 1. WEEK('2000-01-01',-1) 1. WEEK('2000-01-01',0) 1. WEEK('2000-01-01',1) 1. WEEK('2000-01-01',2) 1. WEEK('2000-01-01',3) 1. WEEK('2000-01-01',4) 1. WEEK('2000-01-01',5) 1. WEEK('2000-01-01',6) 1. WEEK('2000-01-01',7) 1. WEEK('2000-01-01',8) 1. WEEK('2000-01-01',1.0) 1. WEEK('1721020591',0) 1. WEEK(1721020591,0) 1. WEEK('1721020666229',0) 1. WEEK('2020-01-01 00:00:00', 2) 1. WEEK('01/01/2020', 2) 1. WEEK('20200101', 2) 1. WEEK('20/01/01', 2) 1. WEEK('11/01/31', 2) 1. WEEK('01-JAN-20', 2) 1. select week(ts) from d1001; 1. select week(ts) from meters; | 1. 无参调用 1. expr缺失 1. 有逗号但mode缺失 1. expr为NULL 1. 有逗号但mode为NULL 1. expr为非表示时间的字符串 1. expr为非表示时间的数值 1. mode小于设定最小值 1. 正常调用，mode为0 1. 正常调用，mode为1 1. 正常调用，mode为2 1. 正常调用，mode为3 1. 正常调用，mode为4 1. 正常调用，mode为5 1. 正常调用，mode为6 1. 正常调用，mode为7 1. mode大于设定最大值 1. mode为浮点值 1. expr为unix时间戳，单位为秒 1. expr为数值类型 1. expr为unix时间戳，单位毫秒 1. expr为符合日期格式的VARCHAR类型 1. dd/mm/yyyy时间类型 1. yyyymmdd时间类型 1. yy/mm/dd时间类型 1. mm/dd/yy时间类型 1. dd-mon-yy时间类型 1. 参数为表中的时间戳列（单子表） 1. 参数为表中的时间戳列（超级表） | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ')' at line 1 1. 同1 1. 同1 1. NULL 1. 0 1. NULL 1. 0 1. error（mode超出定义范围） 1. 0 1. 0 1. 52 1. 52 1. 0 1. 0 1. 52 1. 52 1. error（mode超出定义范围） 1. 输入类型不符合预期报错 1. NULL 1. 3 1. NULL 1. 52 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 正常查询 1. 正常查询 | 1. 语法错误 1. 语法错误 1. 语法错误 1. NULL 1. 0 1. NULL 1. 0 1. 无效的参数值报错 1. 0 1. 0 1. 52 1. 52 1. 0 1. 0 1. 52 1. 52 1. 无效的参数值报错 1. 无效的参数类型报错 1. NULL 1. 3 1. NULL 1. 52 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 正常查询 1. 正常查询 | **通过** |
| WEEKDAY(expr) | 1. WEEKDAY() 1. WEEKDAY(NULL) 1. WEEKDAY('2020-01-01') 1. WEEKDAY(1721020591) 1. WEEKDAY(1721020666229) 1. WEEKDAY('1721020591') 1. WEEKDAY('1721020666229') 1. WEEKDAY('2020-01-01 00:00:00') 1. WEEKDAY('abc') 1. WEEKDAY('01/01/2020',2) 1. WEEKDAY('01/01/2020') 1. WEEKDAY('20200101') 1. WEEKDAY('20/01/01') 1. WEEKDAY('11/01/32') 1. WEEKDAY('01-JAN-20') 1. select weekday(ts) from d1001; 1. select weekday(ts) from meters; | 1. 无参调用 1. expr为NULL 1. expr为日期类型的正常调用 1. expr为unix时间戳，单位为秒 1. expr为unix时间戳，单位毫秒 1. expr为字符类型unix时间戳 1. expr为字符类型unix时间戳 1. expr为精确到秒的VARCHAR日期类型 1. expr为非日期的字符类型 1. expr为多参数的调用 1. dd/mm/yyyy时间类型 1. yyyymmdd时间类型 1. yy/mm/dd时间类型 1. mm/dd/yy时间类型 1. dd-mon-yy时间类型 1. 参数为表中的时间戳列（单子表） 1. 参数为表中的时间戳列（超级表） | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'WEEKDAY' 1. NULL 1. 2 1. 2 1. 0 1. NULL 1. NULL 1. 2 1. NULL 1. 输入数据不符合规则报错 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 2 1. 2 1. 0 1. NULL 1. NULL 1. 2 1. NULL 1. 无效的参数数量 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 正常查询 1. 正常查询 | **通过** |
| WEEKOFYEAR(expr) | 1. WEEKOFYEAR() 1. WEEKOFYEAR(NULL) 1. WEEKOFYEAR('2020-01-01') 1. WEEKOFYEAR('1721020591') 1. WEEKOFYEAR('1721020666229') 1. WEEKOFYEAR(1721020666) 1. WEEKOFYEAR(1721020666229) 1. WEEKOFYEAR('abc') 1. WEEKOFYEAR('2020-01-01',1) 1. WEEKOFYEAR('01/01/2020') 1. WEEKOFYEAR('20200101') 1. WEEKOFYEAR('20/01/01') 1. WEEKOFYEAR('11/01/31') 1. WEEKOFYEAR('01-JAN-20') 1. select weekofyear(ts) from d1001; 1. select weekofyear(ts) from meters; | 1. 无参调用 1. expr为NULL 1. expr为日期类型 1. expr为字符类型unix时间戳 1. expr为字符类型unix时间戳 1. expr为秒类型的unix时间戳 1. expr为毫秒类型的unix时间戳 1. expr为非日期的字符类型 1. expr为多参数的调用 1. dd/mm/yyyy时间类型 1. yyyymmdd时间类型 1. yy/mm/dd时间类型 1. mm/dd/yy时间类型 1. dd-mon-yy时间类型 1. 参数为表中的时间戳列（单子表） 1. 参数为表中的时间戳列（超级表） | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'WEEKOFYEAR' 1. NULL 1. 1 1. NULL 1. NULL 1. 4 1. 29 1. NULL 1. 同1 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 1 1. NULL 1. NULL 1. 4 1. 29 1. NULL 1. 无效的参数数量报错 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 正常查询 1. 正常查询 | **通过** |
| DAYOFWEEK(expr) | 1. DAYOFWEEK() 2. DAYOFWEEK(NULL) 3. DAYOFWEEK('2020-01-01') 4. DAYOFWEEK('1721020591') 5. DAYOFWEEK('1721020666229') 6. DAYOFWEEK(1721020666) 1. DAYOFWEEK(1721020666229) 1. DAYOFWEEK('abc') 1. DAYOFWEEK('2020-01-01',1) 1. DAYOFWEEK('01/01/2020') 1. DAYOFWEEK('20200101') 1. DAYOFWEEK('20/01/01') 1. DAYOFWEEK('11/01/31') 1. DAYOFWEEK('01-JAN-20') 1. select dayofweek(ts) from d1001; 1. select dayofweek(ts) from meters; | 1. 无参调用 1. expr为NULL 1. expr为日期类型 1. expr为字符类型unix时间戳 1. expr为字符类型unix时间戳 1. expr为秒类型的unix时间戳 1. expr为毫秒类型的unix时间戳 1. expr为非日期的字符类型 1. expr为多参数的调用 1. dd/mm/yyyy时间类型 1. yyyymmdd时间类型 1. yy/mm/dd时间类型 1. mm/dd/yy时间类型 1. dd-mon-yy时间类型 1. 参数为表中的时间戳列（单子表） 1. 参数为表中的时间戳列（超级表） | 1. ERROR 1582 (42000): Incorrect parameter count in the call to native function 'DAYOFWEEK' 1. NULL 1. 4 1. NULL 1. NULL 1. 4 1. 2 1. NULL 1. 同1 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 4 1. NULL 1. NULL 1. 4 1. 2 1. NULL 1. 非法的参数数量 1. NULL 1. NULL 1. NULL 1. NULL 1. NULL 1. 正常查询 1. 正常查询 | **通过** |
| STDDEV_POP(expr) | 1. select stddev_pop() from meters; 1. select stddev_pop(NULL) from meters; 1. select stddev_pop(ts) from meters; 1. select stddev_pop(id) from meters; 1. select stddev_pop(name) from meters; 1. select stddev_pop(current) from meters; 1. select stddev_pop(id, current) from meters; | 1. 无参调用 1. expr为NULL 1. expr为时间戳类型 1. expr为整型数值类型 1. expr为字符类型 1. expr为浮点型数值类型 1. expr为多个列参数 | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ') from test' at line 1 1. NULL 1. 输入类型不符合预期报错 1. 288.6749902572095 1. 输入类型不符合预期报错 1. 1.186547472344684 1. 同1 | 1. 语法错误 1. NULL 1. 无效参数类型报错 1. 288.674990257209515 1. 无效参数类型报错 1. 1.186547472344684 1. 无效的参数数量报错 | **通过** |
| VAR_POP(expr) | 1. select var_pop() from meters; 2. select var_pop(NULL) from meters; 3. select var_pop(ts) from meters; 4. select var_pop(id) from meters; 5. select var_pop(name) from meters; 6. select var_pop(current) from meters; 1. select var_pop(id, current) from meters; | 1. 无参调用 1. expr为NULL 1. expr为时间戳类型 1. expr为整型数值类型 1. expr为字符类型 1. expr为浮点型数值类型 1. expr为多个列参数 | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ') from test' at line 1 1. NULL 1. 输入类型不符合预期报错 1. 83333 1. 输入类型不符合预期报错 1. 1.407894904127559 1. 同1 | 1. 语法错误 1. NULL 1. 无效的参数类型报错 1. 83333.250000000000000 1. 无效的参数类型 1. 1.407894904127559 1. 无效的参数数量 | **通过** |
| MAX/MIN(expr) | 1. select max() from meters; 2. select max(NULL) from meters; 3. select max(ts) from meters; 4. select max(id) from meters; 5. select max(name) from meters; 6. select max(current) from meters; 1. select max(nch1) from meters; 1. select max(var1) from meters; 1. select min() from meters; 1. select min(NULL) from meters; 1. select min(ts) from meters; 1. select min(id) from meters; 1. select min(name) from meters; 1. select min(current) from meters; 1. select min(s1) from meters; 1. select min(s3) from meters; | 1. max函数无参调用 1. max函数expr为NULL 1. max函数expr为时间戳类型 1. max函数expr为整型数值类型 1. max函数expr为字符类型 1. max函数expr为浮点型数值类型 1. max函数expr为nchar列的列名 1. max函数expr为varchar列的列名 1. min函数无参调用 1. min函数expr为NULL 1. min函数expr为时间戳类型 1. min函数expr为整型数值类型 1. min函数expr为字符类型 1. min函数expr为浮点型数值类型 1. min函数expr为nchar列的列名 1. min函数expr为varchar列的列名 | 1. ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ') from test' at line 1 1. NULL 1. 输入类型不符合预期报错 1. 998 1. x 1. 11.993 1. 正常查询 1. 正常查询 1. 同1 1. NULL 1. 输入类型不符合预期报错 1. 0 1. haha 1. 8.013 1. 正常查询 1. 正常查询 | 1. 语法错误 1. NULL 1. 无效的参数类型 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 语法错误 1. NULL 1. 无效的参数类型 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 1. 正常查询 | **通过** |
| ```sql {wrap} select ts, current, phase, id, name from meters where id < 678 group by name event_window start with greatest(current, phase) * voltage > 0 end with greatest(current, phase) * voltage < 30 having location='Sunnyvale'; ``` |  | **报错，不支持** |  |  |
| ```sql {wrap} select ts, current, phase, id, name from meters where id < 678 partition by name event_window start with greatest(current, phase) * voltage > 0 end with greatest(current, phase) * voltage < 30 having location='Sunnyvale'; ``` |  |  |  |  |
|  | ```sql {wrap} select ts, current, phase, id, name, count(*) from meters where id < 678 group by name event_window start with leastest(current, phase) * voltage > 0 end with greatest(current, phase) * voltage < 30 having location='Sunnyvale'; ``` |  | **报错，不支持** |  |  |
|  | ```sql {wrap} select ts, current, phase, id, name, count(*) from meters where id < 678 partition by name event_window start with leastest(current, phase) * voltage > 0 end with greatest(current, phase) * voltage < 30 having location='Sunnyvale'; ``` |  | **报错，不支持** |  |  |

### 9.1 性能测试

性能测试用例从9.1中挑选可以成功执行的，使用python连接器连接taos数据库和mysql数据库分别执行一定次对比执行效率，保证执行时间不少于120s

#### 9.1.1 同功能函数MySQL与taos对比测试

| 测试函数 | 测试用例 | 谁的结果 | 第一次 | 第二次 | 第三次 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| Taos运行结果 | 204.94 | 193.36 | 189.41 |
| MySQL结果 | 150.38 | 150.50 | 166.32 |
| Taos运行结果 | 216.02 | 208.56 | 204.48 |
| MySQL结果 | 152.82 | 154.63 | 150.40 |
| Taos运行结果 | 132.68 | 132.44 | 132.73 |
| MySQL结果 | 122.81 | 121.19 | 121.00 |
| Taos运行结果 | 224.79 | 224.03 | 223.64 |
| MySQL结果 | 150.46 | 149.86 | 156.87 |
| Taos运行结果 | 132.86 | 131.60 | 131.28 |
| MySQL结果 | 119.71 | 120.68 | 120.97 |
| Taos运行结果 | 205.54 | 210.74 | 203.64 |
| MySQL结果 | 149.41 | 151.13 | 146.66 |
| Taos运行结果 | 129.95 | 130.39 | 130.57 |
| MySQL结果 | 135.20 | 134.83 | 135.04 |
| Taos运行结果 | 203.22 | 208.80 | 206.73 |
| MySQL结果 | 146.16 | 148.68 | 147.15 |
| Taos运行结果 | 130.00 | 130.33 | 131.07 |
| MySQL结果 | 130.27 | 135.73 | 129.59 |
| Taos运行结果 | 208.94 | 239.72 | 223.19 |
| MySQL结果 | 135.27 | 129.61 | 135.41 |
| Taos运行结果 | 159.92 | 161.82 | 154.98 |
| MySQL结果 | 110.40 | 109.65 | 110.94 |
| Taos运行结果 | 144.43 | 143.33 | 144.36 |
| MySQL结果 | 161.08 | 163.41 | 160.85 |
| Taos运行结果 | 148.22 | 144.47 | 147.41 |
| MySQL结果 | 162.37 | 160.47 | 162.94 |
| Taos运行结果 | 147.74 | 159.15 | 152.07 |
| MySQL结果 | 90.36 | 88.75 | 89.74 |
| Taos运行结果 | 205.82 | 205.88 | 210.79 |
| MySQL结果 | 150.37 | 143.06 | 158.34 |
| Taos运行结果 | 129.98 | 127.99 | 128.07 |
| MySQL结果 | 105.36 | 104.73 | 102.44 |
| Taos运行结果 | 200.65 | 208.29 | 203.85 |
| MySQL结果 | 149.74 | 149.61 | 170.60 |
| Taos运行结果 | 132.20 | 131.20 | 131.78 |
| MySQL结果 | 133.70 | 132.31 | 115.35 |
| Taos运行结果 | 205.47 | 193.52 | 201.67 |
| MySQL结果 | 167.39 | 167.28 | 192.70 |
| Taos运行结果 | 132.01 | 132.01 | 136.26 |
| MySQL结果 | 132.84 | 133.87 | 109.60 |
| Taos运行结果 |  |  |  |
| MySQL结果 |  |  |  |
| Taos运行结果 |  |  |  |
| MySQL结果 |  |  |  |
| Taos运行结果 |  |  |  |
| MySQL结果 |  |  |  |
| Taos运行结果 |  |  |  |
| MySQL结果 |  |  |  |
| Taos运行结果 | 204.99 | 196.16 | 202.32 |
| MySQL结果 | 132.29 | 132.22 | 137.80 |
| Taos运行结果 | 134.80 | 135.05 | 135.03 |
| MySQL结果 | 93.05 | 98.10 | 96.03 |
| Taos运行结果 | 154.95 | 155.97 | 151.23 |
| MySQL结果 | 94.35 | 94.86 | 102.62 |
| Taos运行结果 | 209.51 | 200.45 | 199.88 |
| MySQL结果 | 130.26 | 131.26 | 155.91 |
| Taos运行结果 | 149.99 | 147.39 | 148.35 |
| MySQL结果 | 91.75 | 93.65 | 93.60 |
| Taos运行结果 | 194.58 | 189.20 | 188.58 |
| MySQL结果 | 130.96 | 126.76 | 149.85 |
| Taos运行结果 | 132.45 | 131.30 | 132.12 |
| MySQL结果 | 94.83 | 96.20 | 95.81 |
| Taos运行结果 | 146.85 | 144.88 | 134.57 |
| MySQL结果 | 95.11 | 95.81 | 98.53 |
| Taos运行结果 | 191.65 | 205.89 | 201.42 |
| MySQL结果 | 133.47 | 140.49 | 160.51 |
| Taos运行结果 | 144.18 | 144.33 | 144.16 |
| MySQL结果 | 107.38 | 105.97 | 108.37 |
| Taos运行结果 | 183.35 | 174.04 | 178.41 |
| MySQL结果 | 109.81 | 114.67 | 109.83 |
| Taos运行结果 | 186.54 | 183.94 | 183.50 |
| MySQL结果 | 105.47 | 102.52 | 103.03 |
| Taos运行结果 | 171.05 | 170.90 | 170.20 |
| MySQL结果 | 101.29 | 101.43 | 101.31 |
| Taos运行结果 | 199.55 | 209.98 | 238.67 |
| MySQL结果 | 132.26 | 163.81 | 176.52 |
| Taos运行结果 | 245.91 | 242.05 | 228.77 |
| MySQL结果 | 159.13 | 176.07 | 175.56 |
| Taos运行结果 | 182.08 | 182.22 | 182.17 |
| MySQL结果 | 99.55 | 102.12 | 102.48 |
| Taos运行结果 | 182.33 | 182.50 | 183.09 |
| MySQL结果 | 104.55 | 106.10 | 105.77 |
| Taos运行结果 | 200.60 | 195.84 | 200.47 |
| MySQL结果 | 105.25 | 107.46 | 109.43 |
| Taos运行结果 | 169.13 | 160.80 | 161.48 |
| MySQL结果 | 97.99 | 99.29 | 96.67 |
| Taos运行结果 | 196.05 | 213.70 | 225.31 |
| MySQL结果 | 154.24 | 175.71 | 170.44 |
| Taos运行结果 | 189.82 | 189.65 | 191.21 |
| MySQL结果 | 121.18 | 113.63 | 114.06 |
| Taos运行结果 | 219.93 | 217.79 | 214.32 |
| MySQL结果 | 119.28 | 119.05 | 114.91 |
| Taos运行结果 | 198.45 | 198.50 | 193.45 |
| MySQL结果 | 104.37 | 106.30 | 106.38 |
| Taos运行结果 | 176.88 | 181.95 | 180.19 |
| MySQL结果 | 96.38 | 99.05 | 98.91 |
| Taos运行结果 | 206.98 | 209.12 | 208.38 |
| MySQL结果 | 118.12 | 115.34 | 115.94 |
| Taos运行结果 | 204.84 | 219.77 | 221.44 |
| MySQL结果 | 156.85 | 173.10 | 179.58 |
| Taos运行结果 | 199.91 | 179.41 | 172.41 |
| MySQL结果 | 81.68 | 85.56 | 83.92 |
| Taos运行结果 | 147.16 | 148.37 | 142.14 |
| MySQL结果 | 76.00 | 82.88 | 82.67 |
| Taos运行结果 | 145.44 | 145.76 | 146.36 |
| MySQL结果 | 107.38 | 103.17 | 105.06 |
| Taos运行结果 | 195.22 | 190.17 | 190.82 |
| MySQL结果 | 87.10 | 87.41 | 87.22 |
| Taos运行结果 | 218.90 | 236.45 | 225.71 |
| MySQL结果 | 177.42 | 178.29 | 123.29 |
| Taos运行结果 | 148.28 | 156.52 | 156.78 |
| MySQL结果 | 105.45 | 101.41 | 99.47 |
| Taos运行结果 | 161.18 | 160.64 | 163.64 |
| MySQL结果 | 97.37 | 96.64 | 95.80 |
| Taos运行结果 | 220.66 | 237.25 | 230.02 |
| MySQL结果 | 179.57 | 142.60 | 163.75 |
| Taos运行结果 | 215.43 | 195.30 | 200.97 |
| MySQL结果 | 111.64 | 110.44 | 111.65 |
| Taos运行结果 | 198.75 | 192.55 | 200.44 |
| MySQL结果 | 103.88 | 111.96 | 109.77 |
| Taos运行结果 | 211.94 | 225.74 | 233.66 |
| MySQL结果 | 119.25 | 114.11 | 118.59 |
| Taos运行结果 | 189.27 | 188.89 | 191.26 |
| MySQL结果 | 116.24 | 113.50 | 104.29 |
| Taos运行结果 | 180.56 | 181.32 | 180.59 |
| MySQL结果 | 92.81 | 93.98 | 92.22 |
| Taos运行结果 | 182.44 | 178.74 | 179.95 |
| MySQL结果 | 97.21 | 94.74 | 94.57 |
| Taos运行结果 | 210.00 | 209.06 | 208.90 |
| MySQL结果 | 11.17 | 10.80 | 12.39 |
| Taos运行结果 | 278.62 | 275.93 | 258.85 |
| MySQL结果 | 125.80 | 126.74 | 117.91 |
| Taos运行结果 | 260.09 | 247.46 | 223.34 |
| MySQL结果 | 116.86 | 123.37 | 128.66 |
| Taos运行结果 | 212.12 | 215.32 | 240.49 |
| MySQL结果 | 140.68 | 122.49 | 130.04 |
| Taos运行结果 | 237.34 | 243.75 | 204.85 |
| MySQL结果 | 132.67 | 150.74 | 155.87 |
| Taos运行结果 | 213.76 | 238.13 | 277.20 |
| MySQL结果 | 120.27 | 120.45 | 121.18 |
| Taos运行结果 | 224.14 | 225.37 | 224.52 |
| MySQL结果 | 101.09 | 100.26 | 99.82 |
| Taos运行结果 | 242.01 | 235.04 | 245.39 |
| MySQL结果 | 150.96 | 144.20 | 148.32 |
| Taos运行结果 | 201.77 | 204.61 | 201.25 |
| MySQL结果 | 104.99 | 100.70 | 100.00 |
| Taos运行结果 | 252.39 | 244.03 | 256.35 |
| MySQL结果 | 168.31 | 138.02 | 176.06 |
| Taos运行结果 | 202.93 | 206.98 | 208.62 |
| MySQL结果 | 100.28 | 101.38 | 95.50 |
| Taos运行结果 | 237.49 | 232.73 | 242.82 |
| MySQL结果 | 160.07 | 141.93 | 157.79 |
| Taos运行结果 | 208.18 | 211.94 | 212.03 |
| MySQL结果 | 105.10 | 103.95 | 99.91 |
| Taos运行结果 | 251.10 | 256.89 | 235.05 |
| MySQL结果 | 165.86 | 139.81 | 156.62 |
| Taos运行结果 | 205.60 | 207.39 | 208.36 |
| MySQL结果 | 102.96 | 101.95 | 96.23 |
| Taos运行结果 | 91.34 | 93.45 | 92.35 |
| MySQL结果 | 46.00 | 43.97 | 47.77 |
| Taos运行结果 | 94.27 | 93.47 | 96.16 |
| MySQL结果 | 44.32 | 46.77 | 45.60 |
| Taos运行结果 | 90.85 | 93.25 | 92.02 |
| MySQL结果 | 46.38 | 43.43 | 47.05 |
| Taos运行结果 | 94.08 | 93.09 | 96.26 |
| MySQL结果 | 44.52 | 48.02 | 42.66 |
| Taos运行结果 | 76.22 | 76.05 | 76.28 |
| MySQL结果 | 24.90 | 24.47 | 24.93 |
| Taos运行结果 | 77.40 | 77.79 | 77.24 |
| MySQL结果 | 26.40 | 25.84 | 26.23 |
| Taos运行结果 | 75.16 | 76.16 | 76.11 |
| MySQL结果 | 26.02 | 26.21 | 26.02 |
| Taos运行结果 | 77.84 | 83.09 | 81.60 |
| MySQL结果 | 26.44 | 26.87 | 27.00 |
| Taos运行结果 | 83.91 | 79.12 | 77.86 |
| MySQL结果 | 26.36 | 26.25 | 26.02 |
| Taos运行结果 | 76.23 | 76.33 | 76.49 |
| MySQL结果 | 24.29 | 24.17 | 24.17 |
| Taos运行结果 | 76.92 | 77.41 | 77.62 |
| MySQL结果 | 26.33 | 26.67 | 26.53 |
| Taos运行结果 | 76.31 | 75.86 | 75.90 |
| MySQL结果 | 26.31 | 26.00 | 26.46 |
| Taos运行结果 | 78.34 | 79.44 | 79.29 |
| MySQL结果 | 26.20 | 26.74 | 26.31 |
| Taos运行结果 | 78.93 | 78.46 | 78.43 |
| MySQL结果 | 26.06 | 26.13 | 25.98 |


#### 9.1.2 ~~taos数据库常用生产环境性能测试~~

~~本测试项内容是关于taos数据库常用生产环境的测试，常用生产环境即一个超级表有多个子表，且数据总量非常大，本次测试取100,000,000条数据，共分为10,000个子表，每个子表10,000条数据，分为4个vgroup~~

| 测试函数 | 测试用例 | 运行用时 | 运行用时 | 运行用时 | 备注 |
| --- | --- | --- | --- | --- | --- |
| PI() | PI() | / | / | / | 与表的大小和数据量无关且9.2.1测过 |
| ROUND(10.55) | / |  |  | 与表的大小和数据量无关 |
| select round(current,1) from meters |  |  |  |  |
| TRUNCATE(PI(),2) | / |  |  | 与表的大小和数据量无关 |
| select truncate(current,1) from meters |  |  |  |  |
| EXP(2) | / |  |  | 与表的大小和数据量无关 |
| select exp(current) from meters; |  |  |  |  |
| LN(2) | / |  |  | 与表的大小和数据量无关 |
| select ln(current) from meters; |  |  |  |  |
| MOD(PI(),2) | / |  |  | 与表的大小和数据量无关 |
| select mod(current,1) from meters; |  |  |  |  |
| RAND() | / |  |  | 与表的大小和数据量无关 |
| select mod(current,2) from meters; |  |  |  |  |
| SIGN(10) | / |  |  | 与表的大小和数据量无关 |
| select sgin(current) from meters |  |  |  |  |
| DEGREES(PI()) | / |  |  | 与表的大小和数据量无关 |
| select degrees(phase) from meters |  |  |  |  |
| RADIANS(PI()) | / |  |  | 与表的大小和数据量无关 |
| select radians(phase) from meters |  |  |  |  |
| GREATEST(1,2,3) | / |  |  | 与表的大小和数据量无关 |
| select greatest(current) from meters; |  |  |  |  |
| LEASTEST(1,2,3) | / |  |  | 与表的大小和数据量无关 |
| select leastest(current) from meters; |  |  |  |  |
| CHAR_LENGTH('taos') | / |  |  | 与表的大小和数据量无关 |
| select char_length(s1) from meters; |  |  |  |  |
| select char_length(s3) from meters; |  |  |  |  |
| char(77) | / |  |  | 与表的大小和数据量无关 |
| select char(groupid) from meters; |  |  |  |  |
| ascii('t') | / |  |  | 与表的大小和数据量无关 |
| select ascii(s1) from meters; |  |  |  |  |
| select ascii(s3) from meters; |  |  |  |  |
| POSITION('t' IN 'taos') | / |  |  | 与表的大小和数据量无关 |
| select position(s2 in s1) from meters; |  |  |  |  |
| select position(s2 in s3) from meters; |  |  |  |  |
| select position(s4 in s1) from meters; |  |  |  |  |
| select position(s4 in s3) from meters; |  |  |  |  |
| TRIM(BOTH 'a' FROM 'aaa abab aaaa') | / |  |  | 与表的大小和数据量无关 |
| TRIM(BOTH '北' FROM '北京涛思数据科技有限公司北') | / |  |  | 与表的大小和数据量无关 |
| select trim(s2 from s1) from meters; |  |  |  |  |
| select trim(s2 from s3) from meters; |  |  |  |  |
| select trim(s4 from s1) from meters; |  |  |  |  |
| select trim(s4 from s3) from meters; |  |  |  |  |
| REPLACE('aabbccdd','aa' , 'ee') | / |  |  | 与表的大小和数据量无关 |
| select replace(s1, s2, 't') from meters; |  |  |  |  |
| select replace(s1, s4, 't') from meters; |  |  |  |  |
| select replace(s3, s2, 't') from meters; |  |  |  |  |
| select replace(s3, s4, 't') from meters; |  |  |  |  |
| select replace(s1, s2, s3) from meters; |  |  |  |  |
| REPEAT('taos', 2) | / |  |  | 与表的大小和数据量无关 |
| select repeat(s1, 3) from meters; |  |  |  |  |
| select repeat(s3, 3) from meters; |  |  |  |  |
| select repeat(name, groupid) from meters; |  |  |  |  |
| select repeat(s1, id) from meters; |  |  |  |  |
| SUBSTRING('中国tdengine', 1, 3) | / |  |  | 与表的大小和数据量无关 |
| select substring(s1 , 1, 5) from meters; |  |  |  |  |
| select substring(s3 , 1, 5) from meters; |  |  |  |  |
| SUBSTRING_INDEX('中国.科学.www.taosdata.com', '.', 2) | / |  |  | 与表的大小和数据量无关 |
| select substring_index(s1, 'a', 2) from meters; |  |  |  |  |
| select substring_index(s1, s2, 2) from meters; |  |  |  |  |
| select substring_index(s1, s4, 2) from meters; |  |  |  |  |
| select substring_index(s3, s2, 2) from meters; |  |  |  |  |
| select substring_index(s3, s4, 2) from meters; |  |  |  |  |
| select substring_index(s3, s4, id) from meters; |  |  |  |  |
| TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00') | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-01 08:00:03', '2022-01-01 08:00:00',1b) | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-01 08:00:03', '2022-01-01 08:00:00',1u) | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-01 08:00:03', '2022-01-01 08:00:00',1a) | / |  |  | 与表的大小和数据量无关 |
| timestampdiff(s,'2022-01-01 08:00:03', '2022-01-01 08:00:00') | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2023-01-01 08:00:00', '2022-01-01 08:00:00',1s) | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00',1m) | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00',1h) | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00',1d) | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00',1w) | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-31 08:00:00', '2022-01-01 08:00:00') | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF('2022-01-31', '2022-01-01',1s) | / |  |  | 与表的大小和数据量无关 |
| TIMEDIFF(1720769589123, 1720769529123, 1s) | / |  |  | 与表的大小和数据量无关 |
| select timediff(ts, 1720769529123) from meters; |  |  |  |  |
| WEEK('2000-01-01',0) | / |  |  | 与表的大小和数据量无关 |
| WEEK(1721020666229,0) | / |  |  | 与表的大小和数据量无关 |
| select week(ts) from meters; |  |  |  |  |
| WEEKDAY('2020-01-01 00:00:00') | / |  |  | 与表的大小和数据量无关 |
| WEEKDAY(1721020666229) | / |  |  | 与表的大小和数据量无关 |
| select weekday(ts) from meters; |  |  |  |  |
| WEEKOFYEAR('2020-01-01') | / |  |  | 与表的大小和数据量无关 |
| WEEKOFYEAR(1721020666229) | / |  |  | 与表的大小和数据量无关 |
| select weekofyear(ts) from meters; |  |  |  |  |
| DAYOFWEEK('2020-01-01') | / |  |  | 与表的大小和数据量无关 |
| DAYOFWEEK(1721020666229) | / |  |  | 与表的大小和数据量无关 |
| select dayofweek(ts) from meters; |  |  |  |  |
| select stddev_pop(id) from meters; |  |  |  |  |
| select stddev_pop(current) from meters; |  |  |  |  |
| select var_pop(id) from meters; |  |  |  |  |
| select var_pop(current) from meters; |  |  |  |  |
| select max(ts) from meters; |  |  |  |  |
| select max(name) from meters; |  |  |  |  |
| select max(current) from meters; |  |  |  |  |
| select min(ts) from meters; |  |  |  |  |
| select min(name) from meters; |  |  |  |  |
| select min(current) from meters; |  |  |  |  |
| select max(s1) from meters; |  |  |  |  |
| select min(s1) from meters; |  |  |  |  |
| select max(s3) from meters; |  |  |  |  |
| select min(s3) from meters; |  |  |  |  |


## 10. 问题

## 11. 测试计划 

## 12. 测试备忘 

## 13. 参考文档
