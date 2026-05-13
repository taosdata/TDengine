# Stream Processing

[TD-21454流式计算新功能——写入已存在的超级表](https://taosdata.feishu.cn/docx/TyTzdV2MAoKuWDxTNEJcy6TDnlg) 
[TD-21455流式计算新功能——自定义tag](https://taosdata.feishu.cn/docx/NKgodwzzzoKOT9xDDACcecrrnOg) 
[TD-22268 创建流计算增加option，不做update data的检查](https://taosdata.feishu.cn/docx/LYZmd0WVgolFu6xDn6zcas1unDd)

### 1. 写入已存在的超级表

#### 1.1 语法说明

CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name[(field1_name, ...)] SUBTABLE(expression) AS subquery
- field1_name,...用来指定stb_name的列与subquery输出结果的对应关系。如果stb_name的列与subquery输出结果的位置、数量全部匹配，则不需要显示指定对应关系。如果stb_name的列与subquery输出结果的数据类型不匹配，会把subquery输出结果的类型转换成对应的stb_name的列的类型。用法可以参考“insert into select”。
- 如果子表已经存在，并且子表名的命名规则符合自定义表名规则，则流计算会自动向改表中插入数据，不会报错。

#### 1.2 功能说明

1.如果超级表已存在。
1.1 检查列的schema信息是否匹配，对于不匹配的，则自动进行类型转换，当前只有数据长度大于4096byte时才报错，其余场景都能进行类型转换。
1.2 检查列的个数是否相同，如果不同，需要显示的指定超级表与subquery的列的对应关系，否则报错；如果相同，可以指定对应关系，也可以不指定，不指定则按位置顺序对应。
1.3 至少自定义一个tag，否则报错。详见自定义tag
2.如果超级表不存在，会自动创建超级表，这个是之前的功能，本次没有修改，所以这里不做描述。

#### 1.3 示例

```sql
taos> create database test  vgroups 1;
Create OK, 0 row(s) affected (0.101425s)

taos> use test;
Database changed.

taos> create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);
Create OK, 0 row(s) affected (0.007546s)

taos> create table t1 using st tags(1,1,1);
Create OK, 0 row(s) affected (0.003583s)

taos> create table t2 using st tags(2,2,2);
Create OK, 0 row(s) affected (0.005412s)

taos> create stable streamt0(ts timestamp,a int,b int) tags(ta int,tb varchar(100),tc int);
Create OK, 0 row(s) affected (0.007152s)

taos> create stream streams0 trigger at_once  into streamt0 tags(tb) as select  _wstart, count(*) c1, max(a) c2 from st partition by tbname tb interval(10s);
Create OK, 0 row(s) affected (0.191799s)

taos> insert into t1 values(1648791213000,1,2,3);
Insert OK, 1 row(s) affected (0.002019s)

taos> select * from streamt0;
           ts            |      a      |      b      |     ta      |               tb               |     tc      |
===================================================================================================================
 2022-04-01 13:33:30.000 |           1 |           1 |        NULL | t1                             |        NULL |
Query OK, 1 row(s) in set (0.006882s)

```



### 2. 自定义tag

#### 2.1 语法说明

CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name [TAGS (create_definition [, create_definition] ...)] SUBTABLE(expression)  AS subquery
create_definition:
    col_name column_definition
column_definition:
    type_name [COMMENT 'string_value']

1.1 TAGS和SUBTABLE可以同时使用，也可以分别单独使用，TAGS后的create_definition只能是partition by后面的别名。create_definition参考创建超级表的SQL，对应URL：https://docs.taosdata.com/taos-sql/stable/
1.2 在创建流时不使用 TAGS子句时，流式计算创建的超级表有唯一的 tag 列 groupId，每个 partition 会被分配唯一 groupId，并用groupId作为每个子表的TAG值。
1.3 若创建流的语句中使用 TAGS子句，用户可以为每个 partition 对应的子表生成自定义的TAG值。

#### 2.2 功能说明

1.如果超级表已存在。
1.1 检查tag的schema信息是否匹配，对于不匹配的，则自动进行数据类型转换，当前只有数据长度大于4096byte时才报错，其余场景都能进行类型转换。
1.2 检查tag的个数是否相同，如果不同，需要显示的指定超级表与subquery的tag的对应关系，否则报错；如果相同，可以指定对应关系，也可以不指定，不指定则按位置顺序对应。
1.3 至少自定义一个tag，否则报错。
2.如果超级表不存在，会自动创建超级表
2.1 用户自定义了tag，并且按照用户自定义规则创建tag
2.2 用户没有自定义tag，这个是之前的功能，本次没有修改，所以这里不做描述。

#### 2.3 示例

```sql
taos> create database test  vgroups 1;
Create OK, 0 row(s) affected (0.121182s)

taos> use test;
Database changed.

taos> create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);
Create OK, 0 row(s) affected (0.009638s)

taos> create table t1 using st tags(1,1,1);
Create OK, 0 row(s) affected (0.004970s)

taos> create table t2 using st tags(2,2,2);
Create OK, 0 row(s) affected (0.003981s)

taos> create stream streams1 trigger at_once  into streamt1 TAGS(cc varchar(100)) as select  _wstart, count(*) c1 from st partition by concat("tag-", tbname) as cc interval(10s);
Create OK, 0 row(s) affected (0.201794s)

taos> desc streamt1;
             field              |         type         |   length    |   note   |
=================================================================================
 _wstart                        | TIMESTAMP            |           8 |          |
 c1                             | BIGINT               |           8 |          |
 cc                             | VARCHAR              |         100 | TAG      |
Query OK, 3 row(s) in set (0.005404s)

taos> insert into t1 values(1648791213000,1,2,3);
Insert OK, 1 row(s) affected (0.020893s)

taos> select * from streamt1;
         _wstart         |          c1           |               cc               |
===================================================================================
 2022-04-01 13:33:30.000 |                     1 | tag-t1                         |
Query OK, 1 row(s) in set (0.020906s)
```


### 3. 新增建流的参数：ignore update

#### 3.1 语法说明

CREATE STREAM [IF NOT EXISTS] stream_name [stream_options] INTO stb_name [TAGS (create_definition [, create_definition] ...)] SUBTABLE(expression)  AS subquery

stream_options: {
 TRIGGER    [AT_ONCE | WINDOW_CLOSE | MAX_DELAY time]
 WATERMARK   time
 IGNORE UPDATE [0|1]
}

#### 3.2 功能说明

1.不指定IGNORE UPDATE 或 指定 IGNORE UPDATE 0，则走既有流程，检查数据是否是update，本次没做改变，这里不做详述。
2.指定IGNORE UPDATE 1，则不会检查数据是否是update，对于所有数据都做增量运算。如果update数据，不会触发扫盘。注意：流的计算结果与批查询的计算结果不同，即流的结果是错误的。

#### 3.3 示例

```sql
taos> create database test1  vgroups 1;
Create OK, 0 row(s) affected (0.096999s)

taos> use test1;
Database changed.

taos> create table t1(ts timestamp, a int, b int , c int);
Create OK, 0 row(s) affected (0.002179s)

taos> 
taos> create stream streams1 trigger at_once ignore update 1 into streamt1 as select  _wstart c1, count(*) c2, max(b) c3 from t1 session(ts, 10s);
Create OK, 0 row(s) affected (0.021307s)

taos> insert into t1 values(1648791213000,1,1,1);
Insert OK, 1 row(s) affected (0.001214s)

taos> insert into t1 values(1648791213000,2,2,2);
Insert OK, 1 row(s) affected (0.001239s)

taos> select * from streamt1 order by 1,2,3;
           c1            |          c2           |     c3      |       group_id        |
========================================================================================
 2022-04-01 13:33:33.000 |                     2 |           2 |                     0 |
Query OK, 1 row(s) in set (0.003956s)

```



### 4. 流计算新增配置参数：disableStream

#### 4.1 用法说明

taos.cfg中增加一个配置选项disableStream，默认是0。如果配置为1，则所有的流计算任务全部停止。

#### 4.2 功能说明

该参数用户紧急情况下恢复客户环境。如果因为stream导致taosd无法正常启动，可以将配置参数配置为1，然后重启taosd，然后drop stream。恢复完环境后，需要将该参数再改回0，重启taosd，否则所有的流计算无法继续工作。

#### 4.3 示例

```sql
dataDir /root/data/TDengine_data/data
logDir  /root/data/TDengine_data/log
monitor 0
asyncLog              0
qDebugFlag            0
disableStream         1

```
