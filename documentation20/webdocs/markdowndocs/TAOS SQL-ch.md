# TAOS SQL

本文档说明TAOS SQL支持的语法规则、主要查询功能、支持的SQL查询函数，以及常用技巧等内容。阅读本文档需要读者具有基本的SQL语言的基础。

TAOS SQL是用户对TDengine进行数据写入和查询的主要工具。TAOS SQL为了便于用户快速上手，在一定程度上提供类似于标准SQL类似的风格和模式。严格意义上，TAOS SQL并不是也不试图提供SQL标准的语法。此外，由于TDengine针对的时序性结构化数据不提供删除功能，因此在TAO SQL中不提供数据删除的相关功能。

本章节SQL语法遵循如下约定：

- < > 里的内容是用户需要输入的，但不要输入<>本身
- [ ]表示内容为可选项，但不能输入[]本身
- | 表示多选一，选择其中一个即可，但不能输入|本身
- … 表示前面的项可重复多个

为更好地说明SQL语法的规则及其特点，本文假设存在一个数据集。以智能电表(meters)为例，假设每个智能电表采集电流、电压、相位三个量。其建模如下：
```mysql
taos> DESCRIBE meters;
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | TAG        |
 groupid                        | INT                |           4 | TAG        |
```
数据集包含4个智能电表的数据，按照TDengine的建模规则，对应4个子表，其名称分别是 d1001, d1002, d1003, d1004。

## 支持的数据类型

使用TDengine，最重要的是时间戳。创建并插入记录、查询历史记录的时候，均需要指定时间戳。时间戳有如下规则：

- 时间格式为```YYYY-MM-DD HH:mm:ss.MS```, 默认时间分辨率为毫秒。比如：```2017-08-12 18:25:58.128```
- 内部函数now是服务器的当前时间
- 插入记录时，如果时间戳为now，插入数据时使用服务器当前时间
- Epoch Time: 时间戳也可以是一个长整数，表示从1970-01-01 08:00:00.000开始的毫秒数
- 时间可以加减，比如 now-2h，表明查询时刻向前推2个小时(最近2小时)。 数字后面的时间单位可以是 a(毫秒)、s(秒)、 m(分)、h(小时)、d(天)、w(周)。 比如select * from t1 where ts > now-2w and ts <= now-1w, 表示查询两周前整整一周的数据。 在指定降频操作(down sampling)的时间窗口(interval)时，时间单位还可以使用 n(自然月) 和 y(自然年)。

TDengine缺省的时间戳是毫秒精度，但通过修改配置参数enableMicrosecond就可支持微秒。

在TDengine中，普通表的数据模型中可使用以下10种数据类型。 

|      |   类型    | Bytes  | 说明                                                         |
| ---- | :-------: | ------ | ------------------------------------------------------------ |
| 1    | TIMESTAMP | 8      | 时间戳。缺省精度毫秒，可支持微秒。从格林威治时间 1970-01-01 00:00:00.000 (UTC/GMT) 开始，计时不能早于该时间。 |
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
  CREATE DATABASE [IF NOT EXISTS] db_name [KEEP keep] [UPDATE 1];
  ```
  说明：

  1) KEEP是该数据库的数据保留多长天数，缺省是3650天(10年)，数据库会自动删除超过时限的数据；

  2) UPDATE 标志数据库支持更新相同时间戳数据；

  3) 数据库名最大长度为33；
  4) 一条SQL 语句的最大长度为65480个字符；
  5) 数据库还有更多与存储相关的配置参数，请参见系统管理。

- **显示系统当前参数**

  ```mysql
  SHOW VARIABLES;
  ```

- **使用数据库**

    ```mysql
    USE db_name;
    ```
    使用/切换数据库

- **删除数据库**
    ```mysql
    DROP DATABASE [IF EXISTS] db_name;
    ```
    删除数据库。所包含的全部数据表将被删除，谨慎使用

- **修改数据库参数**
    ```mysql
    ALTER DATABASE db_name COMP 2;
    ```
    COMP参数是指修改数据库文件压缩标志位，取值范围为[0, 2]. 0表示不压缩，1表示一阶段压缩，2表示两阶段压缩。

    ```mysql
    ALTER DATABASE db_name REPLICA 2;
    ```
    REPLICA参数是指修改数据库副本数，取值范围[1, 3]。在集群中使用，副本数必须小于或等于dnode的数目。

    ```mysql
    ALTER DATABASE db_name KEEP 365;
    ```
    KEEP参数是指修改数据文件保存的天数，缺省值为3650，取值范围[days, 365000]，必须大于或等于days参数值。

    ```mysql
    ALTER DATABASE db_name QUORUM 2;
    ```
    QUORUM参数是指数据写入成功所需要的确认数。取值范围[1, 3]。对于异步复制，quorum设为1，具有master角色的虚拟节点自己确认即可。对于同步复制，需要至少大于等于2。原则上，Quorum >=1 并且 Quorum <= replica(副本数)，这个参数在启动一个同步模块实例时需要提供。

    ```mysql
    ALTER DATABASE db_name BLOCKS 100;
    ```
    BLOCKS参数是每个VNODE (TSDB) 中有多少cache大小的内存块，因此一个VNODE的用的内存大小粗略为（cache * blocks）。取值范围[3, 1000]。

    **Tips**: 以上所有参数修改后都可以用show databases来确认是否修改成功。

- **显示系统所有数据库**
    ```mysql
    SHOW DATABASES;
    ```

## 表管理
- **创建数据表**

    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]);
    ```
    说明：
    1) 表的第一个字段必须是TIMESTAMP，并且系统自动将其设为主键；
    2) 表名最大长度为193；
    3) 表的每行长度不能超过16k个字符;
    4) 子表名只能由字母、数字和下划线组成，且不能以数字开头
    5) 使用数据类型binary或nchar，需指定其最长的字节数，如binary(20)，表示20字节；

- **以超级表为模板创建数据表**

    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1 [, tag_value2 ...]);
    ```
    以指定的超级表为模板，指定 tags 的值来创建数据表。

- **删除数据表**

    ```mysql
    DROP TABLE [IF EXISTS] tb_name;
    ```

- **显示当前数据库下的所有数据表信息**

    ```mysql
    SHOW TABLES [LIKE tb_name_wildcar];
    ```

    显示当前数据库下的所有数据表信息。说明：可在 like 中使用通配符进行名称的匹配。 通配符匹配：1）“%”（百分号）匹配 0 到任意个字符；2）“\_”（下划线）匹配一个字符。

- **在线修改显示字符宽度**

    ```mysql
    SET MAX_BINARY_DISPLAY_WIDTH <nn>;
    ```

- **获取表的结构信息**

    ```mysql
    DESCRIBE tb_name;
    ```

- **表增加列**

    ```mysql
    ALTER TABLE tb_name ADD COLUMN field_name data_type;
    ```
    说明：
    1) 列的最大个数为1024，最小个数为2；
    2) 列名最大长度为65；

- **表删除列**

    ```mysql
    ALTER TABLE tb_name DROP COLUMN field_name; 
    ```
    如果表是通过[超级表](../super-table/)创建，更改表结构的操作只能对超级表进行。同时针对超级表的结构更改对所有通过该结构创建的表生效。对于不是通过超级表创建的表，可以直接修改表结构

## 超级表STable管理
- **创建超级表**

    ```mysql
    CREATE TABLE [IF NOT EXISTS] stb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]) TAGS (tag1_name tag_type1, tag2_name tag_type2 [, tag3_name tag_type3]);
    ```
    创建STable, 与创建表的SQL语法相似，但需指定TAGS字段的名称和类型

    说明：
    1) TAGS 列的数据类型不能是timestamp类型；
    2) TAGS 列名不能与其他列名相同；
    3) TAGS 列名不能为预留关键字；
    4) TAGS 最多允许128个，至少1个，总长度不超过16k个字符。

- **删除超级表**

    ```mysql
    DROP TABLE [IF EXISTS] stb_name;
    ```
    删除STable会自动删除通过STable创建的子表。

- **显示当前数据库下的所有超级表信息**

    ```mysql
    SHOW STABLES [LIKE tb_name_wildcar];
    ```
    查看数据库内全部STable，及其相关信息，包括STable的名称、创建时间、列数量、标签（TAG）数量、通过该STable建表的数量。

- **获取超级表的结构信息**

    ```mysql
    DESCRIBE stb_name;
    ```

- **超级表增加列**

    ```mysql
    ALTER TABLE stb_name ADD COLUMN field_name data_type;
    ```

- **超级表删除列**

    ```mysql
    ALTER TABLE stb_name DROP COLUMN field_name; 
    ```

## 超级表 STable 中 TAG 管理
- **添加标签**

    ```mysql
    ALTER TABLE stb_name ADD TAG new_tag_name tag_type;
    ```
    为STable增加一个新的标签，并指定新标签的类型。标签总数不能超过128个，总长度不超过16k个字符。

- **删除标签**

    ```mysql
    ALTER TABLE stb_name DROP TAG tag_name;
    ```
    删除超级表的一个标签，从超级表删除某个标签后，该超级表下的所有子表也会自动删除该标签。

- **修改标签名**

    ```mysql
    ALTER TABLE stb_name CHANGE TAG old_tag_name new_tag_name;
    ```
    修改超级表的标签名，从超级表修改某个标签名后，该超级表下的所有子表也会自动更新该标签名。

- **修改子表标签值**

    ```mysql
    ALTER TABLE tb_name SET TAG tag_name=new_tag_value;
    ```
    说明：除了更新标签的值的操作是针对子表进行，其他所有的标签操作（添加标签、删除标签等）均只能作用于STable，不能对单个子表操作。对STable添加标签以后，依托于该STable建立的所有表将自动增加了一个标签，所有新增标签的默认值都是NULL。

## 数据写入

- **插入一条记录**
    ```mysql
    INSERT INTO tb_name VALUES (field_value, ...);
    ```
    向表tb_name中插入一条记录

- **插入一条记录，数据对应到指定的列**
    ```mysql
    INSERT INTO tb_name (field1_name, ...) VALUES (field1_value, ...)
    ```
    向表tb_name中插入一条记录，数据对应到指定的列。SQL语句中没有出现的列，数据库将自动填充为NULL。主键（时间戳）不能为NULL。

- **插入多条记录**
    ```mysql
    INSERT INTO tb_name VALUES (field1_value1, ...) (field1_value2, ...) ...;
    ```
    向表tb_name中插入多条记录

- **按指定的列插入多条记录**
    ```mysql
    INSERT INTO tb_name (field1_name, ...) VALUES (field1_value1, ...) (field1_value2, ...) ...;
    ```
    向表tb_name中按指定的列插入多条记录

- **向多个表插入多条记录**
    ```mysql
    INSERT INTO tb1_name VALUES (field1_value1, ...) (field1_value2, ...) ...
                tb2_name VALUES (field1_value1, ...) (field1_value2, ...) ...;
    ```
    同时向表tb1_name和tb2_name中分别插入多条记录

- **同时向多个表按列插入多条记录**
    ```mysql
    INSERT INTO tb1_name (tb1_field1_name, ...) VALUES (field1_value1, ...) (field1_value2, ...) ...
                tb2_name (tb2_field1_name, ...) VALUES (field1_value1, ...) (field1_value2, ...) ...;
    ```
    同时向表tb1_name和tb2_name中按列分别插入多条记录 

    注意：
    1) 如果时间戳为0，系统将自动使用服务器当前时间作为该记录的时间戳；
    2) 允许插入的最老记录的时间戳，是相对于当前服务器时间，减去配置的keep值（数据保留的天数），允许插入的最新记录的时间戳，是相对于当前服务器时间，加上配置的days值（数据文件存储数据的时间跨度，单位为天）。keep和days都是可以在创建数据库时指定的，缺省值分别是3650天和10天。

**历史记录写入**：可使用IMPORT或者INSERT命令，IMPORT的语法，功能与INSERT完全一样。

## 数据查询

### 查询语法：

```mysql
SELECT select_expr [, select_expr ...]
    FROM {tb_name_list}
    [WHERE where_condition]
    [INTERVAL (interval_val [, interval_offset])]
    [FILL fill_val]
    [SLIDING fill_val]
    [GROUP BY col_list]
    [ORDER BY col_list { DESC | ASC }]
    [SLIMIT limit_val [, SOFFSET offset_val]]
    [LIMIT limit_val [, OFFSET offset_val]]
    [>> export_file]
```
说明：针对 insert 类型的 SQL 语句，我们采用的流式解析策略，在发现后面的错误之前，前面正确的部分SQL仍会执行。下面的sql中，insert语句是无效的，但是d1001仍会被创建。
```mysql
taos> CREATE TABLE meters(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);
Query OK, 0 row(s) affected (0.008245s)

taos> SHOW STABLES;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 meters                         | 2020-08-06 17:50:27.831 |       4 |      2 |           0 |
Query OK, 1 row(s) in set (0.001029s)

taos> SHOW TABLES;
Query OK, 0 row(s) in set (0.000946s)

taos> INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2);

DB error: invalid SQL: keyword VALUES or FILE required

taos> SHOW TABLES;
           table_name           |      created_time       | columns |          stable_name           |
======================================================================================================
 d1001                          | 2020-08-06 17:52:02.097 |       4 | meters                         |
Query OK, 1 row(s) in set (0.001091s)
```

#### SELECT子句
一个选择子句可以是联合查询（UNION）和另一个查询的子查询（SUBQUERY）。

##### 通配符
通配符 * 可以用于代指全部列。对于普通表，结果中只有普通列。
```mysql
taos> SELECT * FROM d1001;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
Query OK, 3 row(s) in set (0.001165s)
```

在针对超级表，通配符包含 _标签列_ 。
```mysql
taos> SELECT * FROM meters;
           ts            |       current        |   voltage   |        phase         |            location            |   groupid   |
=====================================================================================================================================
 2018-10-03 14:38:05.500 |             11.80000 |         221 |              0.28000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:16.600 |             13.40000 |         223 |              0.29000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:05.000 |             10.80000 |         223 |              0.29000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:06.500 |             11.50000 |         221 |              0.35000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:04.000 |             10.20000 |         220 |              0.23000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:16.650 |             10.30000 |         218 |              0.25000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 | Beijing.Chaoyang               |           2 |
Query OK, 9 row(s) in set (0.002022s)
```

通配符支持表名前缀，以下两个SQL语句均为返回全部的列：
```mysql
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```
在Join查询中，带前缀的\*和不带前缀\*返回的结果有差别， \*返回全部表的所有列数据（不包含标签），带前缀的通配符，则只返回该表的列数据。
```mysql
taos> SELECT * FROM d1001, d1003 WHERE d1001.ts=d1003.ts;
           ts            | current |   voltage   |    phase     |           ts            | current |   voltage   |    phase     |
==================================================================================================================================
 2018-10-03 14:38:05.000 | 10.30000|         219 |      0.31000 | 2018-10-03 14:38:05.000 | 10.80000|         223 |      0.29000 |
Query OK, 1 row(s) in set (0.017385s)
```
```mysql
taos> SELECT d1001.* FROM d1001,d1003 WHERE d1001.ts = d1003.ts;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
Query OK, 1 row(s) in set (0.020443s)
```

在使用SQL函数来进行查询过程中，部分SQL函数支持通配符操作。其中的区别在于：
```count(\*)```函数只返回一列。```first```、```last```、```last_row```函数则是返回全部列。

```mysql
taos> SELECT COUNT(*) FROM d1001;
       count(*)        |
========================
                     3 |
Query OK, 1 row(s) in set (0.001035s)
```

```mysql
taos> SELECT FIRST(*) FROM d1001;
        first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
=========================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |            219 |              0.31000 |
Query OK, 1 row(s) in set (0.000849s)
```

##### 标签列

从 2.0.14 版本开始，支持在普通表的查询中指定 _标签列_，且标签列的值会与普通列的数据一起返回。
```mysql
taos> SELECT location, groupid, current FROM d1001 LIMIT 2;
            location            |   groupid   |       current        |
======================================================================
 Beijing.Chaoyang               |           2 |             10.30000 |
 Beijing.Chaoyang               |           2 |             12.60000 |
Query OK, 2 row(s) in set (0.003112s)
```

注意：普通表的通配符 * 中并不包含 _标签列_。

#### 结果集列名

```SELECT```子句中，如果不指定返回结果集合的列名，结果集列名称默认使用```SELECT```子句中的表达式名称作为列名称。此外，用户可使用```AS```来重命名返回结果集合中列的名称。例如：
```mysql
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
           ts            |     primary_key_ts      |
====================================================
 2018-10-03 14:38:05.000 | 2018-10-03 14:38:05.000 |
 2018-10-03 14:38:15.000 | 2018-10-03 14:38:15.000 |
 2018-10-03 14:38:16.800 | 2018-10-03 14:38:16.800 |
Query OK, 3 row(s) in set (0.001191s)
```
但是针对```first(*)```、```last(*)```、```last_row(*)```不支持针对单列的重命名。

#### 隐式结果列
```Select_exprs```可以是表所属列的列名，也可以是基于列的函数表达式或计算式，数量的上限256个。当用户使用了```interval```或```group by tags```的子句以后，在最后返回结果中会强制返回时间戳列（第一列）和group by子句中的标签列。后续的版本中可以支持关闭group by子句中隐式列的输出，列输出完全由select子句控制。

#### 表（超级表）列表

FROM关键字后面可以是若干个表（超级表）列表，也可以是子查询的结果。
如果没有指定用户的当前数据库，可以在表名称之前使用数据库的名称来指定表所属的数据库。例如：```power.d1001``` 方式来跨库使用表。
```mysql
SELECT * FROM power.d1001;
------------------------------
USE power;
SELECT * FROM d1001;
```

#### 特殊功能
部分特殊的查询功能可以不使用FROM子句执行。获取当前所在的数据库 database() 
```mysql
taos> SELECT DATABASE();
           database()           |
=================================
 power                          |
Query OK, 1 row(s) in set (0.000079s)
```
如果登录的时候没有指定默认数据库，且没有使用```use```命令切换数据，则返回NULL。
```mysql
taos> SELECT DATABASE();
           database()           |
=================================
 NULL                           |
Query OK, 1 row(s) in set (0.000184s)
```
获取服务器和客户端版本号:
```mysql
taos> SELECT CLIENT_VERSION();
 client_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000070s)

taos> SELECT SERVER_VERSION();
 server_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000077s)
```
服务器状态检测语句。如果服务器正常，返回一个数字（例如 1）。如果服务器异常，返回error code。该SQL语法能兼容连接池对于TDengine状态的检查及第三方工具对于数据库服务器状态的检查。并可以避免出现使用了错误的心跳检测SQL语句导致的连接池连接丢失的问题。
```mysql
taos> SELECT SERVER_STATUS();
 server_status() |
==================
               1 |
Query OK, 1 row(s) in set (0.000074s)

taos> SELECT SERVER_STATUS() AS status;
   status    |
==============
           1 |
Query OK, 1 row(s) in set (0.000081s)
```
#### TAOS SQL中特殊关键词

 >   TBNAME： 在超级表查询中可视为一个特殊的标签，代表查询涉及的子表名<br>
    \_c0: 表示表（超级表）的第一列

#### 小技巧
获取一个超级表所有的子表名及相关的标签信息：
```mysql
SELECT TBNAME, location FROM meters;
```
统计超级表下辖子表数量：
```mysql
SELECT COUNT(TBNAME) FROM meters;
```
以上两个查询均只支持在Where条件子句中添加针对标签（TAGS）的过滤条件。例如：
```mysql
taos> SELECT TBNAME, location FROM meters;
             tbname             |            location            |
==================================================================
 d1004                          | Beijing.Haidian                |
 d1003                          | Beijing.Haidian                |
 d1002                          | Beijing.Chaoyang               |
 d1001                          | Beijing.Chaoyang               |
Query OK, 4 row(s) in set (0.000881s)

taos> SELECT COUNT(tbname) FROM meters WHERE groupId > 2;
     count(tbname)     |
========================
                     2 |
Query OK, 1 row(s) in set (0.001091s)
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

1. 同时进行多个字段的范围过滤，需要使用关键词 AND 来连接不同的查询条件，暂不支持 OR 连接的不同列之间的查询过滤条件。
2. 针对单一字段的过滤，如果是时间过滤条件，则一条语句中只支持设定一个；但针对其他的（普通）列或标签列，则可以使用``` OR``` 关键字进行组合条件的查询过滤。例如：((value > 20 and value < 30) OR (value < 12)) 。

### SQL 示例 

- 对于下面的例子，表tb1用以下语句创建

    ```mysql
    CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
    ```

- 查询tb1刚过去的一个小时的所有记录

    ```mysql
    SELECT * FROM tb1 WHERE ts >= NOW - 1h;
    ```

- 查询表tb1从2018-06-01 08:00:00.000 到2018-06-02 08:00:00.000时间范围，并且col3的字符串是'nny'结尾的记录，结果按照时间戳降序

    ```mysql
    SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
    ```

- 查询col1与col2的和，并取名complex, 时间大于2018-06-01 08:00:00.000, col2大于1.2，结果输出仅仅10条记录，从第5条开始

    ```mysql
    SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
    ```

- 查询过去10分钟的记录，col2的值大于3.14，并且将结果输出到文件 `/home/testoutpu.csv`.

    ```mysql
    SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutpu.csv;
    ```

## SQL 函数

### 聚合函数

TDengine支持针对数据的聚合查询。提供支持的聚合和选择函数如下：

- **COUNT**
    ```mysql
    SELECT COUNT([*|field_name]) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表/超级表中记录行数或某列的非空值个数。
    返回结果数据类型：长整型INT64。
    应用字段：应用全部字段。
    适用于：表、超级表。
    说明：1）可以使用星号*来替代具体的字段，使用星号(*)返回全部记录数量。2）针对同一表的（不包含NULL值）字段查询结果均相同。3）如果统计对象是具体的列，则返回该列中非NULL值的记录数量。

    示例：
    ```mysql
    taos> SELECT COUNT(*), COUNT(voltage) FROM meters;
        count(*)        |    count(voltage)     |
    ================================================
                        9 |                     9 |
    Query OK, 1 row(s) in set (0.004475s)

    taos> SELECT COUNT(*), COUNT(voltage) FROM d1001;
        count(*)        |    count(voltage)     |
    ================================================
                        3 |                     3 |
    Query OK, 1 row(s) in set (0.001075s)
    ```

- **AVG**
    ```mysql
    SELECT AVG(field_name) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的平均值。
    返回结果数据类型：双精度浮点数Double。
    应用字段：不能应用在timestamp、binary、nchar、bool字段。
    适用于：表、超级表。

    示例：
    ```mysql
    taos> SELECT AVG(current), AVG(voltage), AVG(phase) FROM meters;
        avg(current)        |       avg(voltage)        |        avg(phase)         |
    ====================================================================================
                11.466666751 |             220.444444444 |               0.293333333 |
    Query OK, 1 row(s) in set (0.004135s)

    taos> SELECT AVG(current), AVG(voltage), AVG(phase) FROM d1001;
        avg(current)        |       avg(voltage)        |        avg(phase)         |
    ====================================================================================
                11.733333588 |             219.333333333 |               0.316666673 |
    Query OK, 1 row(s) in set (0.000943s)
    ```

- **TWA**
    ```mysql
    SELECT TWA(field_name) FROM tb_name WHERE clause;
    ```
    功能说明：时间加权平均函数。统计表/超级表中某列在一段时间内的时间加权平均。
    返回结果数据类型：双精度浮点数Double。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：时间加权平均（time weighted average, TWA）查询需要指定查询时间段的 _开始时间_ 和 _结束时间_ 。
    适用于：表、超级表。

- **SUM**
    ```mysql
    SELECT SUM(field_name) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的和。
    返回结果数据类型：双精度浮点数Double和长整型INT64。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    适用于：表、超级表。

    示例：
    ```mysql
    taos> SELECT SUM(current), SUM(voltage), SUM(phase) FROM meters;
        sum(current)        |     sum(voltage)      |        sum(phase)         |
    ================================================================================
                103.200000763 |                  1984 |               2.640000001 |
    Query OK, 1 row(s) in set (0.001702s)

    taos> SELECT SUM(current), SUM(voltage), SUM(phase) FROM d1001;
        sum(current)        |     sum(voltage)      |        sum(phase)         |
    ================================================================================
                35.200000763 |                   658 |               0.950000018 |
    Query OK, 1 row(s) in set (0.000980s)
    ```

- **STDDEV**
    ```mysql
    SELECT STDDEV(field_name) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表中某列的均方差。
    返回结果数据类型：双精度浮点数Double。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    适用于：表。

    示例：
    ```mysql
    taos> SELECT STDDEV(current) FROM d1001;
        stddev(current)      |
    ============================
                1.020892909 |
    Query OK, 1 row(s) in set (0.000915s)
    ```

- **LEASTSQUARES**
    ```mysql
    SELECT LEASTSQUARES(field_name, start_val, step_val) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表中某列的值是主键（时间戳）的拟合直线方程。start_val是自变量初始值，step_val是自变量的步长值。
    返回结果数据类型：字符串表达式（斜率, 截距）。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：自变量是时间戳，因变量是该列的值。
    适用于：表。

    示例：
    ```mysql
    taos> SELECT LEASTSQUARES(current, 1, 1) FROM d1001;
                leastsquares(current, 1, 1)             |
    =====================================================
    {slop:1.000000, intercept:9.733334}                 |
    Query OK, 1 row(s) in set (0.000921s)
    ```

### 选择函数

- **MIN**
    ```mysql
    SELECT MIN(field_name) FROM {tb_name | stb_name} [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最小值。
    返回结果数据类型：同应用的字段。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    示例：
    ```mysql
    taos> SELECT MIN(current), MIN(voltage) FROM meters;
        min(current)     | min(voltage) |
    ======================================
                10.20000 |          218 |
    Query OK, 1 row(s) in set (0.001765s)

    taos> SELECT MIN(current), MIN(voltage) FROM d1001;
        min(current)     | min(voltage) |
    ======================================
                10.30000 |          218 |
    Query OK, 1 row(s) in set (0.000950s)
    ```

- **MAX**
    ```mysql
    SELECT MAX(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最大值。
    返回结果数据类型：同应用的字段。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。

    示例：
    ```mysql
    taos> SELECT MAX(current), MAX(voltage) FROM meters;
        max(current)     | max(voltage) |
    ======================================
                13.40000 |          223 |
    Query OK, 1 row(s) in set (0.001123s)

    taos> SELECT MAX(current), MAX(voltage) FROM d1001;
        max(current)     | max(voltage) |
    ======================================
                12.60000 |          221 |
    Query OK, 1 row(s) in set (0.000987s)
    ```

- **FIRST**
    ```mysql
    SELECT FIRST(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最先写入的非NULL值。
    返回结果数据类型：同应用的字段。
    应用字段：所有字段。
    说明：1）如果要返回各个列的首个（时间戳最小）非NULL值，可以使用FIRST(\*)；2) 如果结果集中的某列全部为NULL值，则该列的返回结果也是NULL；3) 如果结果集中所有列全部为NULL值，则不返回结果。

    示例：
    ```mysql
    taos> SELECT FIRST(*) FROM meters;
            first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
    =========================================================================================
    2018-10-03 14:38:04.000 |             10.20000 |            220 |              0.23000 |
    Query OK, 1 row(s) in set (0.004767s)

    taos> SELECT FIRST(current) FROM d1002;
        first(current)    |
    =======================
                10.20000 |
    Query OK, 1 row(s) in set (0.001023s)
    ```

- **LAST**
    ```mysql
    SELECT LAST(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最后写入的非NULL值。
    返回结果数据类型：同应用的字段。
    应用字段：所有字段。
    说明：1）如果要返回各个列的最后（时间戳最大）一个非NULL值，可以使用LAST(\*)；2）如果结果集中的某列全部为NULL值，则该列的返回结果也是NULL；如果结果集中所有列全部为NULL值，则不返回结果。

    示例：
    ```mysql
    taos> SELECT LAST(*) FROM meters;
            last(ts)         |    last(current)     | last(voltage) |     last(phase)      |
    ========================================================================================
    2018-10-03 14:38:16.800 |             12.30000 |           221 |              0.31000 |
    Query OK, 1 row(s) in set (0.001452s)

    taos> SELECT LAST(current) FROM d1002;
        last(current)     |
    =======================
                10.30000 |
    Query OK, 1 row(s) in set (0.000843s)
    ```

- **TOP**
    ```mysql
    SELECT TOP(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明： 统计表/超级表中某列的值最大*k*个非NULL值。若多于k个列值并列最大，则返回时间戳小的。
    返回结果数据类型：同应用的字段。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：1）*k*值取值范围1≤*k*≤100；2）系统同时返回该记录关联的时间戳列。

    示例：
    ```mysql
    taos> SELECT TOP(current, 3) FROM meters;
            ts            |   top(current, 3)    |
    =================================================
    2018-10-03 14:38:15.000 |             12.60000 |
    2018-10-03 14:38:16.600 |             13.40000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 3 row(s) in set (0.001548s)

    taos> SELECT TOP(current, 2) FROM d1001;
            ts            |   top(current, 2)    |
    =================================================
    2018-10-03 14:38:15.000 |             12.60000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 2 row(s) in set (0.000810s)
    ```

- **BOTTOM**
    ```mysql
    SELECT BOTTOM(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的值最小*k*个非NULL值。若多于k个列值并列最小，则返回时间戳小的。
    返回结果数据类型：同应用的字段。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：1）*k*值取值范围1≤*k*≤100；2）系统同时返回该记录关联的时间戳列。

    示例：
    ```mysql
    taos> SELECT BOTTOM(voltage, 2) FROM meters;
            ts            | bottom(voltage, 2) |
    ===============================================
    2018-10-03 14:38:15.000 |                218 |
    2018-10-03 14:38:16.650 |                218 |
    Query OK, 2 row(s) in set (0.001332s)

    taos> SELECT BOTTOM(current, 2) FROM d1001;
            ts            |  bottom(current, 2)  |
    =================================================
    2018-10-03 14:38:05.000 |             10.30000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 2 row(s) in set (0.000793s)
    ```

- **PERCENTILE**
    ```mysql
    SELECT PERCENTILE(field_name, P) FROM { tb_name } [WHERE clause];
    ```
    功能说明：统计表中某列的值百分比分位数。
    返回结果数据类型： 双精度浮点数Double。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：*P*值取值范围0≤*P*≤100，为0的时候等同于MIN，为100的时候等同于MAX。

    示例：
    ```mysql
    taos> SELECT PERCENTILE(current, 20) FROM d1001;
    percentile(current, 20)  |
    ============================
                11.100000191 |
    Query OK, 1 row(s) in set (0.000787s)
    ```

- **APERCENTILE**
    ```mysql
    SELECT APERCENTILE(field_name, P) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表中某列的值百分比分位数，与PERCENTILE函数相似，但是返回近似结果。
    返回结果数据类型： 双精度浮点数Double。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：*P*值取值范围0≤*P*≤100，为0的时候等同于MIN，为100的时候等同于MAX。推荐使用```APERCENTILE```函数，该函数性能远胜于```PERCENTILE```函数
    ```mysql
    taos> SELECT APERCENTILE(current, 20) FROM d1001;
    apercentile(current, 20)  |
    ============================
                10.300000191 |
    Query OK, 1 row(s) in set (0.000645s)
    ```
    
- **LAST_ROW**
    ```mysql
    SELECT LAST_ROW(field_name) FROM { tb_name | stb_name };
    ```
    功能说明：返回表（超级表）的最后一条记录。
    返回结果数据类型：同应用的字段。
    应用字段：所有字段。
    说明：与last函数不同，last_row不支持时间范围限制，强制返回最后一条记录。

    示例：
    ```mysql
    taos> SELECT LAST_ROW(current) FROM meters;
    last_row(current)   |
    =======================
                12.30000 |
    Query OK, 1 row(s) in set (0.001238s)

    taos> SELECT LAST_ROW(current) FROM d1002;
    last_row(current)   |
    =======================
                10.30000 |
    Query OK, 1 row(s) in set (0.001042s)
    ```

### 计算函数
- **DIFF**
    ```mysql
    SELECT DIFF(field_name) FROM tb_name [WHERE clause];
    ```
    功能说明：统计表中某列的值与前一行对应值的差。
    返回结果数据类型： 同应用字段。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：输出结果行数是范围内总行数减一，第一行没有结果输出。

    示例：
    ```mysql
    taos> SELECT DIFF(current) FROM d1001;
            ts            |    diff(current)     |
    =================================================
    2018-10-03 14:38:15.000 |              2.30000 |
    2018-10-03 14:38:16.800 |             -0.30000 |
    Query OK, 2 row(s) in set (0.001162s)
    ```

- **SPREAD**
    ```mysql
    SELECT SPREAD(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    功能说明：统计表/超级表中某列的最大值和最小值之差。
    返回结果数据类型： 双精度浮点数。
    应用字段：不能应用在binary、nchar、bool类型字段。
    说明：可用于TIMESTAMP字段，此时表示记录的时间覆盖范围。

    示例：
    ```mysql
    taos> SELECT SPREAD(voltage) FROM meters;
        spread(voltage)      |
    ============================
                5.000000000 |
    Query OK, 1 row(s) in set (0.001792s)

    taos> SELECT SPREAD(voltage) FROM d1001;
        spread(voltage)      |
    ============================
                3.000000000 |
    Query OK, 1 row(s) in set (0.000836s)
    ```

- **四则运算**

    ```mysql
    SELECT field_name [+|-|*|/|%][Value|field_name] FROM { tb_name | stb_name }  [WHERE clause];
    ```
    功能说明：统计表/超级表中某列或多列间的值加、减、乘、除、取余计算结果。
    返回结果数据类型：双精度浮点数。
    应用字段：不能应用在timestamp、binary、nchar、bool类型字段。
    说明：1）支持两列或多列之间进行计算，可使用括号控制计算优先级;2）NULL字段不参与计算，如果参与计算的某行中包含NULL，该行的计算结果为NULL。

    ```mysql
    taos> SELECT current + voltage * phase FROM d1001;
    (current+(voltage*phase)) |
    ============================
                78.190000713 |
                84.540003240 |
                80.810000718 |
    Query OK, 3 row(s) in set (0.001046s)
    ```

## 时间维度聚合
TDengine支持按时间段进行聚合，可以将表中数据按照时间段进行切割后聚合生成结果，比如温度传感器每秒采集一次数据，但需查询每隔10分钟的温度平均值。这个聚合适合于降维(down sample)操作, 语法如下：

```mysql
SELECT function_list FROM tb_name
  [WHERE where_condition]
  INTERVAL (interval [, offset])
  [FILL ({NONE | VALUE | PREV | NULL | LINEAR})]

SELECT function_list FROM stb_name
  [WHERE where_condition]
  INTERVAL (interval [, offset])
  [FILL ({ VALUE | PREV | NULL | LINEAR})]
  [GROUP BY tags]
```

- 聚合时间段的长度由关键词INTERVAL指定，最短时间间隔10毫秒（10a），并且支持偏移（偏移必须小于间隔）。聚合查询中，能够同时执行的聚合和选择函数仅限于单个输出的函数：count、avg、sum 、stddev、leastsquares、percentile、min、max、first、last，不能使用具有多行输出结果的函数（例如：top、bottom、diff以及四则运算）。
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

**示例:** 智能电表的建表语句如下：

```mysql
CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

针对智能电表采集的数据，以10分钟为一个阶段，计算过去24小时的电流数据的平均值、最大值、电流的中位数、以及随着时间变化的电流走势拟合直线。如果没有计算值，用前一个非NULL值填充。
使用的查询语句如下：

```mysql
SELECT AVG(current), MAX(current), LEASTSQUARES(current, start_val, step_val), PERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d
  INTERVAL(10m)
  FILL(PREV);
```

## TAOS SQL 边界限制
- 数据库名最大长度为32
- 表名最大长度为192，每行数据最大长度16k个字符
- 列名最大长度为64，最多允许1024列，最少需要2列，第一列必须是时间戳
- 标签最多允许128个，可以0个，标签总长度不超过16k个字符
- SQL语句最大长度65480个字符，但可通过系统配置参数maxSQLLength修改，最长可配置为1M
- 库的数目，超级表的数目、表的数目，系统不做限制，仅受系统资源限制

##  TAOS SQL其他约定

**group by的限制**

TAOS  SQL支持对标签、tbname进行group by操作，也支持普通列进行group by，前提是：仅限一列且该列的唯一值小于10万个。

**join操作的限制**

TAOS SQL支持表之间按主键时间戳来join两张表的列，暂不支持两个表之间聚合后的四则运算。

**is not null与不为空的表达式适用范围**

is not null支持所有类型的列。不为空的表达式为 <>""，仅对非数值类型的列适用。