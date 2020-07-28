# TAOS SQL

本文档说明TAOS SQL支持的语法规则、主要查询功能、支持的SQL查询函数，以及常用技巧等内容。阅读本文档需要读者具有基本的SQL语言的基础。

TAOS SQL是用户对TDengine进行数据写入和查询的主要工具。TAOS SQL为了便于用户快速上手，在一定程度上提供类似于标准SQL类似的风格和模式。严格意义上，TAOS SQL并不是也不试图提供SQL标准的语法。此外，由于TDengine针对的时序性结构化数据不提供修改和更新功能，因此在TAO SQL中不提供数据更新和数据删除的相关功能。

本章节SQL语法遵循如下约定：

- < > 里的内容是用户需要输入的，但不要输入<>本身
- [ ]表示内容为可选项，但不能输入[]本身
- | 表示多选一，选择其中一个即可，但不能输入|本身
- … 表示前面的项可重复多个

为更好地说明SQL语法的规则及其特点，本文假设存在一个数据集。该数据集是针对两种类型的设备温度（湿度）传感器、气压（海拔）传感器建立的数据模型。
针对温度传感器，具有超级表（super table） temp_stable。其数据模型如下：
```
taos> describe temp_stable;
Field       | Type     | Length | Note |
=======================================================================================================
ts          |TIMESTAMP | 8      |      |
temperature |FLOAT     | 4      |      |
humidity    |TINYINT   | 1      |      |
status      |TINYINT   | 1      |      |
deviceid    |BIGINT    | 12     |tag   |
location    |BINARY    | 20     |tag   |
```
数据集包含2个温度传感器的数据，按照TDengine的建模规则，对应2个子表，其名称分别是 temp_tb_1，temp_tb_2 。
针对压力（海拔）传感器，具有超级表（super table） pressure_stable。其数据模型如下：
数据集包含2个压力传感器数据，对应2个子表，分别是 press_tb_1，press_tb_2。

```text
taos> describe pressure_stable;
Field      | Type     | Length | Note |
=======================================================================================================
ts         |TIMESTAMP | 8      |      |
height     |FLOAT     | 4      |      |
pressure   |FLOAT     | 4      |      |
devstat    |TINYINT   | 1      |      |
id         |BIGINT    | 8      |tag   |
city       |NCHAR     | 20     |tag   |
longitude  |FLOAT     | 4      |tag   |
latitude   |FLOAT     | 4      |tag   |
```
## 支持的数据类型

使用TDengine，最重要的是时间戳。创建并插入记录、查询历史记录的时候，均需要指定时间戳。时间戳有如下规则：

- 时间格式为```YYYY-MM-DD HH:mm:ss.MS```, 默认时间分辨率为毫秒。比如：```2017-08-12 18:25:58.128```
- 内部函数now是服务器的当前时间
- 插入记录时，如果时间戳为0，插入数据时使用服务器当前时间
- Epoch Time: 时间戳也可以是一个长整数，表示从1970-01-01 08:00:00.000开始的毫秒数
- 时间可以加减，比如 now-2h，表明查询时刻向前推2个小时(最近2小时)。数字后面的时间单位：a(毫秒), s(秒), m(分), h(小时), d(天)，w(周), n(月), y(年)。比如select * from t1 where ts > now-2w and ts <= now-1w, 表示查询两周前整整一周的数据
- TDengine暂不支持时间窗口按照自然年和自然月切分。Where条件中的时间窗口单位的换算关系如下：interval(1y) 等效于 interval(365d), interval(1n) 等效于 interval(30d), interval(1w) 等效于 interval(7d)

TDengine缺省的时间戳是毫秒精度，但通过修改配置参数enableMicrosecond就可支持微秒。

在TDengine中，普通表的数据模型中可使用以下10种数据类型。 

|      |   类型    | Bytes  | 说明                                                         |
| ---- | :-------: | ------ | ------------------------------------------------------------ |
| 1    | TIMESTAMP | 8      | 时间戳。最小精度毫秒。从格林威治时间 1970-01-01 00:00:00.000 (UTC/GMT) 开始，计时不能早于该时间。 |
| 2    |    INT    | 4      | 整型，范围 [-2^31+1,   2^31-1], -2^31用作Null           |
| 3    |  BIGINT   | 8      | 长整型，范围 [-2^63+1,   2^63-1], -2^63用于NULL                                 |
| 4    |   FLOAT   | 4      | 浮点型，有效位数6-7，范围 [-3.4E38, 3.4E38]                  |
| 5    |  DOUBLE   | 8      | 双精度浮点型，有效位数15-16，范围 [-1.7E308,   1.7E308]      |
| 6    |  BINARY   | 自定义 | 用于记录字符串，理论上，最长可以有16374字节，但由于每行数据最多16K字节，实际上限一般小于理论值。 binary仅支持字符串输入，字符串两端使用单引号引用，否则英文全部自动转化为小写。使用时须指定大小，如binary(20)定义了最长为20个字符的字符串，每个字符占1byte的存储空间。如果用户字符串超出20字节将会报错。对于字符串内的单引号，可以用转义字符反斜线加单引号来表示， 即 **\’**。 |
| 7    | SMALLINT  | 2      | 短整型， 范围 [-32767, 32767], -32768用于NULL                                |
| 8    |  TINYINT  | 1      | 单字节整型，范围 [-127, 127], -128用于NULL                                |
| 9    |   BOOL    | 1      | 布尔型，{true,   false}                                      |
| 10   |   NCHAR   | 自定义 | 用于记录非ASCII字符串，如中文字符。每个nchar字符占用4bytes的存储空间。字符串两端使用单引号引用，字符串内的单引号需用转义字符 **\’**。nchar使用时须指定字符串大小，类型为nchar(10)的列表示此列的字符串最多存储10个nchar字符，会固定占用40bytes的空间。如用户字符串长度超出声明长度，则将会报错。 |

**Tips**: TDengine对SQL语句中的英文字符不区分大小写，自动转化为小写执行。因此用户大小写敏感的字符串及密码，需要使用单引号将字符串引起来。

## 数据库管理

- **创建数据库**  
    ```mysql
    CREATE DATABASE [IF NOT EXISTS] db_name [KEEP keep]
    ```
    创建数据库。`KEEP`是该数据库的数据保留多长天数，缺省是3650天(10年)，数据库会自动删除超过时限的数据。数据库还有更多与存储相关的配置参数，请参见[系统管理](../administrator/#服务端配置)。


- **使用数据库**
    ```mysql
    USE db_name
    ```
    使用/切换数据库


- **删除数据库**
    ```mysql
    DROP DATABASE [IF EXISTS] db_name
    ```
    删除数据库。所包含的全部数据表将被删除，谨慎使用


- **显示系统所有数据库**
    ```mysql
    SHOW DATABASES
    ```


## 表管理
- **创建数据表**
  
    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...])
    ```
    说明：1）表的第一个字段必须是TIMESTAMP，并且系统自动将其设为主键；2）表的每行长度不能超过16K字节；3）使用数据类型binary或nchar，需指定其最长的字节数，如binary(20)，表示20字节。


- **删除数据表**

    ```mysql
    DROP TABLE [IF EXISTS] tb_name
    ```

- **显示当前数据库下的所有数据表信息**

    ```mysql
    SHOW TABLES [LIKE tb_name_wildcar]
    ```

    显示当前数据库下的所有数据表信息。说明：可在like中使用通配符进行名称的匹配。 通配符匹配：1）’%’ (百分号)匹配0到任意个字符；2）’_’下划线匹配一个字符。


- **获取表的结构信息**

    ```mysql
    DESCRIBE tb_name
    ```

- **表增加列**

    ```mysql
    ALTER TABLE tb_name ADD COLUMN field_name data_type
    ```

- **表删除列**

    ```mysql
    ALTER TABLE tb_name DROP COLUMN field_name 
    ```

    如果表是通过[超级表](../super-table/)创建，更改表结构的操作只能对超级表进行。同时针对超级表的结构更改对所有通过该结构创建的表生效。对于不是通过超级表创建的表，可以直接修改表结构

  **Tips**：SQL语句中操作的当前数据库（通过use db_name的方式指定）中的表不需要指定表所属数据库。如果要操作非当前数据库中的表，需要采用“库名”.“表名”的方式。例如：demo.tb1，是指数据库demo中的表tb1。

## 数据写入

- **插入一条记录**
    ```mysql
    INSERT INTO tb_name VALUES (field_value, ...);
    ```
    向表tb_name中插入一条记录


- **插入一条记录，数据对应到指定的列**
    ```mysql
    INSERT INTO tb_name (field1_name, ...) VALUES(field1_value, ...)
    ```
    向表tb_name中插入一条记录，数据对应到指定的列。SQL语句中没有出现的列，数据库将自动填充为NULL。主键（时间戳）不能为NULL。


- **插入多条记录**
    ```mysql
    INSERT INTO tb_name VALUES (field1_value1, ...) (field1_value2, ...)...;
    ```
    向表tb_name中插入多条记录


- **按指定的列插入多条记录**
    ```mysql
    INSERT INTO tb_name (field1_name, ...) VALUES(field1_value1, ...) (field1_value2, ...)
    ```
    向表tb_name中按指定的列插入多条记录


- **向多个表插入多条记录**
    ```mysql
    INSERT INTO tb1_name VALUES (field1_value1, ...)(field1_value2, ...)... 
                tb2_name VALUES (field1_value1, ...)(field1_value2, ...)...;
    ```
    同时向表tb1_name和tb2_name中分别插入多条记录


- **同时向多个表按列插入多条记录**
    ```mysql
    INSERT INTO tb1_name (tb1_field1_name, ...) VALUES (field1_value1, ...) (field1_value1, ...)
                tb2_name (tb2_field1_name, ...) VALUES(field1_value1, ...) (field1_value2, ...)
    ```
    同时向表tb1_name和tb2_name中按列分别插入多条记录 

注意：1、对同一张表，插入的新记录的时间戳必须递增，否则会跳过插入该条记录。如果时间戳为0，系统将自动使用服务器当前时间作为该记录的时间戳。
      2、允许插入的最老记录的时间戳，是相对于当前服务器时间，减去配置的keep值（数据保留的天数），允许插入的最新记录的时间戳，是相对于当前服务器时间，加上配置的days值（数据文件存储数据的时间跨度，单位为天）。keep和days都是可以在创建数据库时指定的，缺省值分别是3650天和10天。

**IMPORT**：如果需要将时间戳小于最后一条记录时间的记录写入到数据库中，可使用IMPORT替代INSERT命令，IMPORT的语法与INSERT完全一样。

## 数据查询

### 查询语法：

```mysql
SELECT [DISTINCT] select_expr [, select_expr ...]
FROM {tb_name_list}
[WHERE where_condition]
[INTERVAL [interval_offset,] interval_val]
[FILL fill_val]
[SLIDING fill_val]
[GROUP BY col_list]
[ORDER BY col_list { DESC | ASC }]
[HAVING expr_list]
[SLIMIT limit_val [, SOFFSET offset_val]]
[LIMIT limit_val [, OFFSET offset_val]]
[>> export_file]
```
#### SELECT子句
一个选择子句可以是联合查询（UNION）和另一个查询的子查询（SUBQUERY）。

##### 通配符
通配符 * 可以用于代指全部列。对于普通表，结果中只有普通列。
```
taos> select * from temp_tb_1;
ts                   | temperature |humidity|status|
============================================================
19-04-28 14:22:07.000| 20.00000    | 34     | 1    |
19-04-28 14:22:08.000| 21.50000    | 38     | 1    |
19-04-28 14:22:09.000| 21.30000    | 38     | 1    |
19-04-28 14:22:10.000| 21.20000    | 38     | 1    |
19-04-28 14:22:11.000| 21.30000    | 35     | 0    |
19-04-28 14:22:12.000| 22.00000    | 34     | 0    |
```
在针对超级表，通配符包含 _标签列_ 。
```
taos> select * from temp_stable;
ts                   | temperature |humidity|status| deviceid | location |
==============================================================================================
19-04-28 14:22:07.000| 21.00000    | 37     | 1    |54197     |beijing   |
19-04-28 14:22:07.000| 20.00000    | 34     | 1    |91234     |beijing   |
19-04-28 14:22:08.000| 21.50000    | 38     | 1    |91234     |beijing   |
19-04-28 14:22:09.000| 21.30000    | 38     | 1    |91234     |beijing   |
19-04-28 14:22:10.000| 21.20000    | 38     | 1    |91234     |beijing   |
19-04-28 14:22:11.000| 21.30000    | 35     | 0    |91234     |beijing   |
19-04-28 14:22:12.000| 22.00000    | 34     | 0    |91234     |beijing   |
```
通配符支持表名前缀，以下两个SQL语句均为返回全部的列：
```
select * from temp_tb_1;
select temp_tb_1.* from temp_tb_1;
```
在Join查询中，带前缀的\*和不带前缀\*返回的结果有差别， \*返回全部表的所有列数据（不包含标签），带前缀的通配符，则只返回该表的列数据。
```
taos> select * from temp_tb_1,temp_tb_2 where temp_tb_1.ts=temp_tb_2.ts;
ts                   | temperature |humidity|status| ts                   | temperature |humidity|status|
========================================================================================================================
19-04-28 14:22:07.000| 20.00000    | 34     | 1    | 19-04-28 14:22:07.000| 21.00000    | 37     | 1    |
```

```
taos> select temp_tb_1.* from temp_tb_1,temp_tb_2 where temp_tb_1.ts=temp_tb_2.ts;
ts                   | temperature |humidity|status|
============================================================
19-04-28 14:22:07.000| 20.00000    | 34     | 1    |
```

在使用SQL函数来进行查询过程中，部分SQL函数支持通配符操作。其中的区别在于：
```count(\*)```函数只返回一列。```first```、```last```、```last_row```函数则是返回全部列。

```
taos> select count(*) from temp_tb_1;
count(*)  |
======================
1         |
```

```
taos> select first(*) from temp_tb_1;
first(ts)            | first(temperature) |first(humidity)|first(status)|
==========================================================================
19-04-28 14:22:07.000| 20.00000           | 34            | 1           |
```

#### 结果集列名

```SELECT```子句中，如果不指定返回结果集合的列名，结果集列名称默认使用```SELECT```子句中的表达式名称作为列名称。此外，用户可使用```AS```来重命名返回结果集合中列的名称。例如：
```
taos> select ts, ts as primary_key_ts from temp_tb_1;
ts                   | primary_key_ts       |
==============================================
19-04-28 14:22:07.000| 19-04-28 14:22:07.000|
```
但是针对```first(*)```、```last(*)```、```last_row(*)```不支持针对单列的重命名。

#### DISTINCT修饰符*
只能用于修饰标签列（TAGS）的结果，不能用于修饰普通列来获得去重后的结果。并且应用```DISTINCT```以后，只能进行单列的标签输出。
```count(distinct column_name)```用以返回近似的不重复结果的数量，该结果是近似值。

#### 隐式结果列
```Select_exprs```可以是表所属列的列名，也可以是基于列的函数表达式或计算式，数量的上限256个。当用户使用了```interval```或```group by tags```的子句以后，在最后返回结果中会强制返回时间戳列（第一列）和group by子句中的标签列。后续的版本中可以支持关闭group by子句中隐式列的输出，列输出完全由select子句控制。

#### 表（超级表）列表

FROM关键字后面可以是若干个表（超级表）列表，也可以是子查询的结果。
如果没有指定用户的当前数据库，可以在表名称之前使用数据库的名称来指定表所属的数据库。例如：```sample.temp_tb_1``` 方式来跨库使用表。
```
SELECT * FROM sample.temp_tb_1;
------------------------------
use sample;
SELECT * FROM temp_tb_1;
```
From子句中列表可以使用别名来让SQL整体更加简单。
```
SELECT t.ts FROM temp_tb_1 t ;
```
> 暂不支持FROM子句的表别名

#### 特殊功能
部分特殊的查询功能可以不使用FROM子句执行。获取当前所在的数据库 database() 
```
taos> SELECT database();
database() |
=================================
sample     |
```
如果登录的时候没有指定默认数据库，且没有使用```use``命令切换数据，则返回NULL。
```
taos> select database();
database() |
=================================
NULL       |
```
获取服务器和客户端版本号:
```
SELECT client_version()
SELECT server_version()
```
服务器状态检测语句。如果服务器正常，返回一个数字（例如 1）。如果服务器异常，返回error code。该SQL语法能兼容连接池对于TDengine状态的检查及第三方工具对于数据库服务器状态的检查。并可以避免出现使用了错误的心跳检测SQL语句导致的连接池连接丢失的问题。
```
SELECT server_status()
SELECT server_status() AS result
```
#### TAOS SQL中特殊关键词

 >   TBNAME： 在超级表查询中可视为一个特殊的标签，代表查询涉及的子表名<br>
    \_c0: 表示表（超级表）的第一列

#### 小技巧
获取一个超级表所有的子表名及相关的标签信息：
```
SELECT TBNAME, location FROM temp_stable
```
统计超级表下辖子表数量：
```
SELECT COUNT(TBNAME) FROM temp_stable
```
以上两个查询均只支持在Where条件子句中添加针对标签（TAGS）的过滤条件。例如：
```
taos> select count(tbname) from temp_stable;
count(tbname) |
======================
2             |

taos> select count(tbname) from temp_stable where deviceid > 60000;
count(tbname) |
======================
1             |
```

- 可以使用* 返回所有列，或指定列名。可以对数字列进行四则运算，可以给输出的列取列名
- where语句可以使用各种逻辑判断来过滤数字值，或使用通配符来过滤字符串
- 输出结果缺省按首列时间戳升序排序，但可以指定按降序排序(_c0指首列时间戳)。使用ORDER BY对其他字段进行排序为非法操作。
- 参数LIMIT控制输出条数，OFFSET指定从第几条开始输出。LIMIT/OFFSET对结果集的执行顺序在ORDER BY之后。
- 通过”>>"输出结果可以导出到指定文件

### 支持的条件过滤操作

| Operation | Note                          | Applicable Data Types                 |
| --------- | ----------------------------- | ------------------------------------- |
| >         | larger than                   | **`timestamp`** and all numeric types |
| <         | smaller than                  | **`timestamp`** and all numeric types |
| >=        | larger than or equal to       | **`timestamp`** and all numeric types |
| <=        | smaller than or equal to      | **`timestamp`** and all numeric types |
| =         | equal to                      | all types                             |
| <>        | not equal to                  | all types                             |
| %         | match with any char sequences | **`binary`** **`nchar`**              |
| _         | match with a single char      | **`binary`** **`nchar`**              |

1. 同时进行多个字段的范围过滤需要使用关键词AND进行连接不同的查询条件，暂不支持OR连接的查询条件。
2. 针对某一字段的过滤只支持单一区间的过滤条件。例如：value>20 and value<30是合法的过滤条件, 而Value<20 AND value<>5是非法的过滤条件。

### Some Examples 

- 对于下面的例子，表tb1用以下语句创建

    ```mysql
    CREATE TABLE tb1 (ts timestamp, col1 int, col2 float, col3 binary(50))
    ```

- 查询tb1刚过去的一个小时的所有记录

    ```mysql
    SELECT * FROM tb1 WHERE ts >= NOW - 1h
    ```

- 查询表tb1从2018-06-01 08:00:00.000 到2018-06-02 08:00:00.000时间范围，并且col3的字符串是'nny'结尾的记录，结果按照时间戳降序

    ```mysql
    SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC
    ```

- 查询col1与col2的和，并取名complex, 时间大于2018-06-01 08:00:00.000, col2大于1.2，结果输出仅仅10条记录，从第5条开始

    ```mysql
    SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' and col2 > 1.2 LIMIT 10 OFFSET 5
    ```

- 查询过去10分钟的记录，col2的值大于3.14，并且将结果输出到文件 `/home/testoutpu.csv`.

    ```mysql
    SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutpu.csv
    ```

## SQL函数

###聚合函数

TDengine支持针对数据的聚合查询。提供支持的聚合和选择函数如下：

- **COUNT**
    ```mysql
    SELECT COUNT([*|field_name]) FROM tb_name [WHERE clause]
    ```
    功能说明：统计表/超级表中记录行数或某列的非空值个数。  
    返回结果数据类型：长整型INT64。  
    应用字段：应用全部字段。  
    适用于：表、超级表。  
    说明：1）可以使用星号*来替代具体的字段，使用星号(*)返回全部记录数量。2）针对同一表的（不包含NULL值）字段查询结果均相同。3）如果统计对象是具体的列，则返回该列中非NULL值的记录数量。 


- **AVG**
    ```mysql
    SELECT AVG(field_name) FROM tb_name [WHERE clause]
    ```
    功能说明：统计表/超级表中某列的平均值。  
    返回结果数据类型：双精度浮点数Double。  
    应用字段：不能应用在timestamp、binary、nchar、bool字段。  
    适用于：表、超级表。  


- **TWA**
    ```mysql
    SELECT TWA(field_name) FROM tb_name WHERE clause
    ```
    功能说明：时间加权平均函数。统计表/超级表中某列在一段时间内的时间加权平均。  
    返回结果数据类型：双精度浮点数Double。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：时间加权平均(time weighted average, TWA）查询需要指定查询时间段的 _开始时间_ 和 _结束时间_ 。
    适用于：表、超级表。


- **SUM**
    ```mysql
    SELECT SUM(field_name) FROM tb_name [WHERE clause]
    ```
    功能说明：统计表/超级表中某列的和。  
    返回结果数据类型：双精度浮点数Double和长整型INT64。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    适用于：表、超级表。


- **STDDEV**
    ```mysql
    SELECT STDDEV(field_name) FROM tb_name [WHERE clause]
    ```
    功能说明：统计表中某列的均方差。  
    返回结果数据类型：双精度浮点数Double。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    适用于：表。


- **LEASTSQUARES**
    ```mysql
    SELECT LEASTSQUARES(field_name, start_val, step_val) FROM tb_name [WHERE clause]
    ```
    功能说明：统计表中某列的值是主键（时间戳）的拟合直线方程。start_val是自变量初始值，step_val是自变量的步长值。  
    返回结果数据类型：字符串表达式（斜率, 截距）。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    说明：自变量是时间戳，因变量是该列的值。  
    适用于：表。


### 选择函数

- **MIN**
    ```mysql
    SELECT MIN(field_name) FROM {tb_name | stb_name} [WHERE clause]
    ```
    功能说明：统计表/超级表中某列的值最小值。  
    返回结果数据类型：同应用的字段。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。


- **MAX**
    ```mysql
    SELECT MAX(field_name) FROM { tb_name | stb_name } [WHERE clause]
    ```
    功能说明：统计表/超级表中某列的值最大值。  
    返回结果数据类型：同应用的字段。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。


- **FIRST**
    ```mysql
    SELECT FIRST(field_name) FROM { tb_name | stb_name } [WHERE clause]
    ```
    功能说明：统计表/超级表中某列的值最先写入的非NULL值。  
    返回结果数据类型：同应用的字段。  
    应用字段：所有字段。  
    说明：1）如果要返回各个列的首个（时间戳最小）非NULL值，可以使用FIRST(*)；2) 如果结果集中的某列全部为NULL值，则该列的返回结果也是NULL；3) 如果结果集中所有列全部为NULL值，则不返回结果。


- **LAST**
    ```mysql
    SELECT LAST(field_name) FROM { tb_name | stb_name } [WHERE clause]
    ```
    功能说明：统计表/超级表中某列的值最后写入的非NULL值。  
    返回结果数据类型：同应用的字段。  
    应用字段：所有字段。  
    说明：1）如果要返回各个列的最后（时间戳最大）一个非NULL值，可以使用LAST(*)；2）如果结果集中的某列全部为NULL值，则该列的返回结果也是NULL；如果结果集中所有列全部为NULL值，则不返回结果。


- **TOP**
    ```mysql
    SELECT TOP(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
    ```
    功能说明： 统计表/超级表中某列的值最大*k*个非NULL值。若多于k个列值并列最大，则返回时间戳小的。     
    返回结果数据类型：同应用的字段。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    说明：1）*k*值取值范围1≤*k*≤100；2）系统同时返回该记录关联的时间戳列。 


- **BOTTOM**
    ```mysql
    SELECT BOTTOM(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
    ```
    功能说明：统计表/超级表中某列的值最小*k*个非NULL值。若多于k个列值并列最小，则返回时间戳小的。  
    返回结果数据类型：同应用的字段。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    说明：1）*k*值取值范围1≤*k*≤100；2）系统同时返回该记录关联的时间戳列。


- **PERCENTILE**
    ```mysql
    SELECT PERCENTILE(field_name, P) FROM { tb_name | stb_name } [WHERE clause]
    ```
    功能说明：统计表中某列的值百分比分位数。  
    返回结果数据类型： 双精度浮点数Double。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    说明：*k*值取值范围0≤*P*≤100，为0的时候等同于MIN，为100的时候等同于MAX。

- **APERCENTILE**
    ```mysql
    SELECT APERCENTILE(field_name, P) FROM { tb_name | stb_name } [WHERE clause]
    ```
    功能说明：统计表中某列的值百分比分位数，与PERCENTILE函数相似，但是返回近似结果。  
    返回结果数据类型： 双精度浮点数Double。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    说明：*k*值取值范围0≤*P*≤100，为0的时候等同于MIN，为100的时候等同于MAX。推荐使用```APERCENTILE```函数，该函数性能远胜于```PERCENTILE```函数
    
- **LAST_ROW**
    ```mysql
    SELECT LAST_ROW(field_name) FROM { tb_name | stb_name }
    ```
    功能说明：返回表（超级表）的最后一条记录。  
    返回结果数据类型：同应用的字段。  
    应用字段：所有字段。  
    说明：与last函数不同，last_row不支持时间范围限制，强制返回最后一条记录。


### 计算函数
- **DIFF**
    ```mysql
    SELECT DIFF(field_name) FROM tb_name [WHERE clause]
    ```
    功能说明：统计表中某列的值与前一行对应值的差。  
    返回结果数据类型： 同应用字段。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    说明：输出结果行数是范围内总行数减一，第一行没有结果输出。


- **SPREAD**
    ```mysql
    SELECT SPREAD(field_name) FROM { tb_name | stb_name } [WHERE clause]
    ```
    功能说明：统计表/超级表中某列的最大值和最小值之差。  
    返回结果数据类型： 双精度浮点数。  
    应用字段：不能应用在binary、nchar、bool类型字段。  
    说明：可用于TIMESTAMP字段，此时表示记录的时间覆盖范围。


- **四则运算**

    ```mysql
    SELECT field_name [+|-|*|/|%][Value|field_name] FROM { tb_name | stb_name }  [WHERE clause]
    ```
    功能说明：统计表/超级表中某列或多列间的值加、减、乘、除、取余计算结果。  
    返回结果数据类型：双精度浮点数。  
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。  
    说明：1）支持两列或多列之间进行计算，可使用括号控制计算优先级;2）NULL字段不参与计算，如果参与计算的某行中包含NULL，该行的计算结果为NULL。


##时间维度聚合
TDengine支持按时间段进行聚合，可以将表中数据按照时间段进行切割后聚合生成结果，比如温度传感器每秒采集一次数据，但需查询每隔10分钟的温度平均值。这个聚合适合于降维(down sample)操作, 语法如下：

```mysql
SELECT function_list FROM tb_name 
  [WHERE where_condition]
  INTERVAL (interval)
  [FILL ({NONE | VALUE | PREV | NULL | LINEAR})]

SELECT function_list FROM stb_name 
  [WHERE where_condition]
  INTERVAL (interval)
  [FILL ({ VALUE | PREV | NULL | LINEAR})]
  [GROUP BY tags]
```

- 聚合时间段的长度由关键词INTERVAL指定，最短时间间隔10毫秒（10a）。聚合查询中，能够同时执行的聚合和选择函数仅限于单个输出的函数：count、avg、sum 、stddev、leastsquares、percentile、min、max、first、last，不能使用具有多行输出结果的函数（例如：top、bottom、diff以及四则运算）。
- WHERE语句可以指定查询的起止时间和其他过滤条件 
- FILL语句指定某一时间区间数据缺失的情况下的填充模式。填充模式包括以下几种：
  1. 不进行填充：NONE(默认填充模式)。
  
  2. VALUE填充：固定值填充，此时需要指定填充的数值。例如：fill(value, 1.23)。
  
  3. NULL填充：使用NULL填充数据。例如：fill(null)。
  
  4. PREV填充：使用前一个非NULL值填充数据。例如：fill(prev)。 
  

说明：  
  1. 使用FILL语句的时候可能生成大量的填充输出，务必指定查询的时间区间。针对每次查询，系统可返回不超过1千万条具有插值的结果。
  2. 在时间维度聚合中，返回的结果中时间序列严格单调递增。
  3. 如果查询对象是超级表，则聚合函数会作用于该超级表下满足值过滤条件的所有表的数据。如果查询中没有使用group by语句，则返回的结果按照时间序列严格单调递增；如果查询中使用了group by语句分组，则返回结果中每个group内不按照时间序列严格单调递增。

**示例：**温度数据表的建表语句如下：

```mysql
create table sensor(ts timestamp, degree double, pm25 smallint) 
```

针对传感器采集的数据，以10分钟为一个阶段，计算过去24小时的温度数据的平均值、最大值、温度的中位数、以及随着时间变化的温度走势拟合直线。如果没有计算值，用前一个非NULL值填充。

```mysql
SELECT AVG(degree),MAX(degree),LEASTSQUARES(degree), PERCENTILE(degree, 50) FROM sensor
  WHERE TS>=NOW-1d
  INTERVAL(10m)
  FILL(PREV);
```

