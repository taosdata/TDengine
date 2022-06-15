---
sidebar_label: 数据写入
title: 数据写入
---

## 写入语法

```
INSERT INTO
    tb_name
        [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
        [(field1_name, ...)]
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    [tb2_name
        [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
        [(field1_name, ...)]
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    ...];
```

## 插入一条或多条记录

指定已经创建好的数据子表的表名，并通过 VALUES 关键字提供一行或多行数据，即可向数据库写入这些数据。例如，执行如下语句可以写入一行记录：

```
INSERT INTO d1001 VALUES (NOW, 10.2, 219, 0.32);
```

或者，可以通过如下语句写入两行记录：

```
INSERT INTO d1001 VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32) (1626164208000, 10.15, 217, 0.33);
```

:::note

1. 在第二个例子中，两行记录的首列时间戳使用了不同格式的写法。其中字符串格式的时间戳写法不受所在 DATABASE 的时间精度设置影响；而长整形格式的时间戳写法会受到所在 DATABASE 的时间精度设置影响——例子中的时间戳在毫秒精度下可以写作 1626164208000，而如果是在微秒精度设置下就需要写为 1626164208000000，纳秒精度设置下需要写为 1626164208000000000。
2. 在使用“插入多条记录”方式写入数据时，不能把第一列的时间戳取值都设为 NOW，否则会导致语句中的多条记录使用相同的时间戳，于是就可能出现相互覆盖以致这些数据行无法全部被正确保存。其原因在于，NOW 函数在执行中会被解析为所在 SQL 语句的实际执行时间，出现在同一语句中的多个 NOW 标记也就会被替换为完全相同的时间戳取值。
3. 允许插入的最老记录的时间戳，是相对于当前服务器时间，减去配置的 keep 值（数据保留的天数）；允许插入的最新记录的时间戳，是相对于当前服务器时间，加上配置的 days 值（数据文件存储数据的时间跨度，单位为天）。keep 和 days 都是可以在创建数据库时指定的，缺省值分别是 3650 天和 10 天。

:::

## 插入记录，数据对应到指定的列

向数据子表中插入记录时，无论插入一行还是多行，都可以让数据对应到指定的列。对于 SQL 语句中没有出现的列，数据库将自动填充为 NULL。主键（时间戳）不能为 NULL。例如：

```
INSERT INTO d1001 (ts, current, phase) VALUES ('2021-07-13 14:06:33.196', 10.27, 0.31);
```

:::info
如果不指定列，也即使用全列模式——那么在 VALUES 部分提供的数据，必须为数据表的每个列都显式地提供数据。全列模式写入速度会远快于指定列，因此建议尽可能采用全列写入方式，此时空列可以填入 NULL。

:::

## 向多个表插入记录

可以在一条语句中，分别向多个表插入一条或多条记录，并且也可以在插入过程中指定列。例如：

```
INSERT INTO d1001 VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d1002 (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31）;
```

## 插入记录时自动建表

如果用户在写数据时并不确定某个表是否存在，此时可以在写入数据时使用自动建表语法来创建不存在的表，若该表已存在则不会建立新表。自动建表时，要求必须以超级表为模板，并写明数据表的 TAGS 取值。例如：

```
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32);
```

也可以在自动建表时，只是指定部分 TAGS 列的取值，未被指定的 TAGS 列将置为 NULL。例如：

```
INSERT INTO d21001 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:33.196', 10.15, 217, 0.33);
```

自动建表语法也支持在一条语句中向多个表插入记录。例如：

```
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) VALUES ('2021-07-13 14:06:34.630', 10.2, 219, 0.32) ('2021-07-13 14:06:35.779', 10.15, 217, 0.33)
            d21002 USING meters (groupId) TAGS (2) VALUES ('2021-07-13 14:06:34.255', 10.15, 217, 0.33)
            d21003 USING meters (groupId) TAGS (2) (ts, current, phase) VALUES ('2021-07-13 14:06:34.255', 10.27, 0.31);
```

:::info
在 2.0.20.5 版本之前，在使用自动建表语法并指定列时，子表的列名必须紧跟在子表名称后面，而不能如例子里那样放在 TAGS 和 VALUES 之间。从 2.0.20.5 版本开始，两种写法都可以，但不能在一条 SQL 语句中混用，否则会报语法错误。
:::

## 插入来自文件的数据记录

除了使用 VALUES 关键字插入一行或多行数据外，也可以把要写入的数据放在 CSV 文件中（英文逗号分隔、英文单引号括住每个值）供 SQL 指令读取。其中 CSV 文件无需表头。例如，如果 /tmp/csvfile.csv 文件的内容为：

```
'2021-07-13 14:07:34.630', '10.2', '219', '0.32'
'2021-07-13 14:07:35.779', '10.15', '217', '0.33'
```

那么通过如下指令可以把这个文件中的数据写入子表中：

```
INSERT INTO d1001 FILE '/tmp/csvfile.csv';
```

## 插入来自文件的数据记录，并自动建表

从 2.1.5.0 版本开始，支持在插入来自 CSV 文件的数据时，以超级表为模板来自动创建不存在的数据表。例如：

```
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) FILE '/tmp/csvfile.csv';
```

也可以在一条语句中向多个表以自动建表的方式插入记录。例如：

```
INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) FILE '/tmp/csvfile_21001.csv'
            d21002 USING meters (groupId) TAGS (2) FILE '/tmp/csvfile_21002.csv';
```

## 历史记录写入

可使用 IMPORT 或者 INSERT 命令，IMPORT 的语法，功能与 INSERT 完全一样。

针对 insert 类型的 SQL 语句，我们采用的流式解析策略，在发现后面的错误之前，前面正确的部分 SQL 仍会执行。下面的 SQL 中，INSERT 语句是无效的，但是 d1001 仍会被创建。

```
taos> CREATE TABLE meters(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);
Query OK, 0 row(s) affected (0.008245s)

taos> SHOW STABLES;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 meters                         | 2020-08-06 17:50:27.831 |       4 |      2 |           0 |
Query OK, 1 row(s) in set (0.001029s)

taos> SHOW TABLES;
Query OK, 0 row(s) in set (0.000946s)

taos> INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('a');

DB error: invalid SQL: 'a' (invalid timestamp) (0.039494s)

taos> SHOW TABLES;
           table_name           |      created_time       | columns |          stable_name           |
======================================================================================================
 d1001                          | 2020-08-06 17:52:02.097 |       4 | meters                         |
Query OK, 1 row(s) in set (0.001091s)
```
