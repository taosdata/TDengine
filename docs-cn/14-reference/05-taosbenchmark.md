---
title: taosBenchmark
sidebar_label: taosBenchmark
---

## 简介

taosBenchmark 是一个用于 TDengine 的性能测试的应用程序。taosBenchmark 可以测试 TDengine 的插入、查询和订阅功能，它可以模拟由大量设备产生的大量数据，还可以灵活地控制列的数量，数据类型、线程等。以前叫 taosdemo，现在改名为为 taosBenchmark，安装包提供 taosdemo 作为 taosBenchmark 的软链接。
配置，一种是[命令行配置](#cli)，另一种是[JSON 文件](#json)。

## CLI

| 选项名称                                 | 描述                                                      |
| :--------------------------------------- | --------------------------------------------------------- |
| [-f/--file](#json)                       | JSON 配置文件, 与命令行其他参数不能同时使用               |
| -c/--config-dir                          | 配置文件所在的目录，默认路径是 /etc/taos/                 |
| -h/--host                                | 用于连接 taosd 服务器的 FQDN，默认值为 localhost。        |
| -P/--port                                | 用于连接 taosd 服务器的端口号，默认值为 6030。            |
| [-I/--interface](#insert-mode)           | taosBenchmark 如何插入数据，默认值为 taosc。              |
| -u/--user                                | 用于连接 taosd 服务器的用户名，默认值为 root。            |
| -p/--password                            | 用于连接 taosd 服务器的密码，默认值为 taosdata。          |
| -o/--output                              | 指定结果输出文件的路径，默认值为 ./output.txt。           |
| -T/--thread                              | 指定插入数据的线程数，默认值为 8                          |
| [-i/--insert-interval](#insert-interval) | 隔行插入模式的插入间隔，单位为 ms，默认值为 0。           |
| -S/--timestampstep                       | 每个子表中每条记录的插入时间戳步长，单位是 ms，默认值是 1 |
| [-B/--interlace-rows](#interlace-rows)   | 向子表插入交错行的数量                                    |
| [-r/--rec-per-req](#record-per-request)  | 每次插入请求的记录数，默认值为 30000                      |
| -t/--tables                              | 子表的数量，默认值为 10000。                              |
| -n/--records                             | 每个子表插入的记录数，默认值为 10000。                    |
| -d/--database                            | 数据库的名称，默认值为 test。                             |
| [-l/--columns](#columns)                 | 子表的列数，将使用 int 数据类型的列。                     |
| [-A/--tag-type](#tag-type)               | 子表的标签的数据类型。                                    |
| [-b/--data-type](#data-type)             | 子表的列的数据类型                                        |
| -w/--binwidth                            | 调整 nchar 和 binary 数据类型的默认长度，默认值为 64。    |
| -m/--table-prefix                        | 子表名称的前缀，默认值为 d                                |
| -E/--escape-character                    | 在超级表和子表名称中使用转义字符，可选。                  |
| -C/--chinese                             | nchar 和 binary 是否基本的 Unicode 中文字符，可选。       |
| [-N/--normal-table](#normal-table)       | 只创建普通表，不创建超级表，可选。                        |
| [-M/--random](#random)                   | 数据源否使用随机的，可选。                                |
| -x/--aggr-func                           | 插入后查询聚合函数，可选。                                |
| -y/--answer-yes                          | 通过确认提示继续，可选。                                  |
| [-R/--disorder-range](#disorder-range)   | 失序时间戳的范围，基于数据库的精度，默认值为 1000         |
| [-O/--disorder](#disorder-ratio)         | 插入无序时间戳的数据的概率，默认为 0。                    |
| [-F/--prepare_rand](#prepared-rand)      | 生产随机数据的随机个数，默认为 10000                      |
| -a/--replica                             | 创建数据库时的副本数量，默认值为 1。                      |
| -V/--version                             | 显示版本信息并退出                                        |
| -?/--help                                | 显示帮助信息并退出。                                      |

## JSON

### 1、插入 json 配置文件

```json
{
  "filetype": "insert",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "connection_pool_size": 8,
  "thread_count": 4,
  "result_file": "./insert_res.txt",
  "confirm_parameter_prompt": "no",
  "insert_interval": 0,
  "interlace_rows": 100,
  "num_of_records_per_req": 100,
  "prepared_rand": 10000,
  "chinese": "no",
  "databases": [
    {
      "dbinfo": {
        "name": "db",
        "drop": "yes",
        "replica": 1,
        "days": 10,
        "cache": 16,
        "blocks": 8,
        "precision": "ms",
        "keep": 3650,
        "minRows": 100,
        "maxRows": 4096,
        "comp": 2,
        "walLevel": 1,
        "cachelast": 0,
        "quorum": 1,
        "fsync": 3000,
        "update": 0
      },
      "super_tables": [
        {
          "name": "stb",
          "child_table_exists": "no",
          "childtable_count": 100,
          "childtable_prefix": "stb_",
          "escape_character": "yes",
          "auto_create_table": "no",
          "batch_create_tbl_num": 5,
          "data_source": "rand",
          "insert_mode": "taosc",
          "line_protocol": "line",
          "insert_rows": 100000,
          "childtable_limit": 10,
          "childtable_offset": 100,
          "interlace_rows": 0,
          "insert_interval": 0,
          "partial_col_num": 0,
          "disorder_ratio": 0,
          "disorder_range": 1000,
          "timestamp_step": 10,
          "start_timestamp": "2020-10-01 00:00:00.000",
          "sample_format": "csv",
          "sample_file": "./sample.csv",
          "use_sample_ts": "no",
          "tags_file": "",
          "columns": [
            { "type": "INT", "name": "id" },
            { "type": "DOUBLE", "count": 10 },
            { "type": "BINARY", "len": 16, "count": 3 },
            { "type": "BINARY", "len": 32, "count": 6 }
          ],
          "tags": [
            { "type": "TINYINT", "count": 2, "max": 10, "min": 98 },
            {
              "type": "BINARY",
              "len": 16,
              "count": 5,
              "values": ["beijing", "shanghai"]
            }
          ]
        }
      ]
    }
  ]
}
```

### 参数

| 组           | 选项名称                                      | 描述                                                                                      |
| ------------ | --------------------------------------------- | ----------------------------------------------------------------------------------------- |
|              | filetype                                      | 文件类型，指定哪种测试，对于插入测试，需要为 insert。                                     |
|              | cfgdir                                        | taosd 配置文件所在的目录，默认值是 /etc/taos。                                            |
|              | host                                          | taosd 服务器的 FQDN，默认为 localhost。                                                   |
|              | port                                          | taosd 服务器的端口号，默认为 6030。                                                       |
|              | user                                          | 连接 taosd 服务器的用户名，默认为 root。                                                  |
|              | password                                      | 连接 taosd 服务器的密码，默认为 taosdata。                                                |
|              | [connection_pool_size](#connection-pool-size) | taos 连接池的大小，默认为线程数。                                                         |
|              | thread_count                                  | 插入和创建表的线程数，默认为 8。                                                          |
|              | result_file                                   | 保存输出结果的文件路径，默认为 ./output.txt。                                             |
|              | confirm_parameter_prompt                      | 在执行过程中传递确认提示，默认为无。                                                      |
|              | [insert_interval](#insert-interval)           | 插入隔行扫描模式的间隔时间，默认值为 0                                                    |
|              | [interlace_rows](#interlace-rows)             | 每个子表的交错行数，默认值为 0。                                                          |
|              | [num_of_records_per_req](#record-per-request) | 每个请求中的记录数，默认值为 30000。                                                      |
|              | [prepare_rand](#prepared-rand)                | 随机产生的数据数量，默认值为 10000                                                        |
|              | chinese                                       | nchar 和 binary 都是 rand 中文，默认值为否。                                              |
| dbinfo       | name                                          | 数据库名称，必填                                                                          |
| dbinfo       | drop                                          | 插入测试前是否删除数据库，默认值为是。                                                    |
| dbinfo       | replica                                       | 复制的数量，默认值是 1。                                                                  |
| dbinfo       | days                                          | 在文件中存储数据的时间跨度，默认值为 10。                                                 |
| dbinfo       | cache                                         | 内存块的大小，单位是 MB，默认值是 16。                                                    |
| dbinfo       | blocks                                        | 每个 vnode(tsdb) 中的缓存大小的内存块的数量，默认值为 6。                                 |
| dbinfo       | precision                                     | 数据库时间精度，默认值为 "ms"。                                                           |
| dbinfo       | keep                                          | 保留数据的天数，默认值为 3650。                                                           |
| dbinfo       | minRows                                       | 文件块中的最小记录数，默认值为 100                                                        |
| dbinfo       | minRows                                       | 文件块中的最大记录数，默认值为 4096                                                       |
| dbinfo       | comp                                          | 文件压缩标志，默认值为 2。                                                                |
| dbinfo       | walLevel                                      | wal 级别，默认值是 1。                                                                    |
| dbinfo       | cachelast                                     | 是否允许将每个表的最后一条记录保留在内存中，默认值为 0                                    |
| dbinfo       | quorum                                        | 异步写需要的确认次数，默认为 1                                                            |
| dbinfo       | fsync                                         | 当 wal 设置为 2 时，fsync 的间隔时间，单位为 ms，默认值为 3000。                          |
| dbinfo       | update                                        | 是否支持数据更新，默认值为 0。                                                            |
| super_tables | name                                          | 超级表的名称，必须填写。                                                                  |
| super_tables | child_table_exists                            | 子表是否已经存在，默认为否。                                                              |
| super_tables | child_table_count                             | 子表的数量，必填                                                                          |
| super_tables | childtable_prefix                             | 子表名称的前缀，必须填写。                                                                |
| super_tables | escape_character                              | 超级表和子表的名称包括转义字符，默认为否。                                                |
| super_tables | [auto_create_table](#auto-create-table)       | SQL 语句是否自动创建子表。                                                                |
| Super_tables | [batch_create_tbl_num](#batch-create-tbl-num) | 为每个请求创建的子表数量，默认为 10。                                                     |
| super_tables | [data_source](#data-source)                   | 数据资源类型，选项：rand, sample。                                                        |
| super_tables | [insert_mode](#insert-mode)                   | 插入模式，选项：taosc, rest, stmt, sml，默认为 taosc。                                    |
| super_tables | [non_stop_mode](#non-stop-mode)               | 插入模式是否为持续不停的写入，默认为 no                                                   |
| super_tables | [line_protocol](#line-protocol)               | 行协议，可选项：line, telnet, json, 默认为 line。                                         |
| super_tables | [tcp_transfer](#tcp-transfer)                 | 使用 tcp 还是 http 协议，默认为 http。                                                    |
| super_tables | insert_rows                                   | 每个子表的记录数，默认为 0。                                                              |
| super_tables | [childtable_offset](#childtable-offset)       | 子表的偏移量。                                                                            |
| super_tables | [childtable_limit](#childtable-limit)         | 插入数据的子表数量。                                                                      |
| super_tables | [interlace_rows](#interlace-rows)             | 每个子表的间隔行，默认为 0。                                                              |
| super_tables | [insert_interval](#insert-interval)           | 两个请求之间的插入时间间隔，当 interlace_rows 大于 0 时有效。                             |
| super_tables | [partial_col_num](#partial-col-num)           | 指定仅向前多少列写入，默认为 0。                                                          |
| super_tables | [disorder_ratio](#disorder-ratio)             | 紊乱时间戳的数据比例，默认为 0                                                            |
| super_tables | [disorder_range](#disorder-range)             | 无序时间戳的范围，只有当 disorder_ratio 大于 0 时才有效，默认为 1000。                    |
| super_tables | timestamp_step                                | 每条记录的时间戳步骤，默认为 1。                                                          |
| super_tables | start_timestamp                               | 每个子表的时间戳起始值，默认值是 now。                                                    |
| super_tables | sample_format                                 | 样本数据文件的类型，现在只支持 csv。                                                      |
| super_tables | [sample_file](#sample-file)                   | 样本文件，仅当 data_source 为 "sample "时有效。                                           |
| super_tables | [use_sample_ts](#use-sample-ts)               | 样本文件是否包含时间戳，默认为否。                                                        |
| super_tables | [tags_file](#tags-file)                       | 原理与[sample_file](#sample-file)相同，标签数据样本文件，仅支持 taosc、rest insert 模式。 |
| columns/tags | [type](#type)                                 | 数据类型，必填                                                                            |
| columns/tags | [len](#len)                                   | 数据长度，默认为 8。                                                                      |
| columns/tags | [count](#count)                               | 该列的连续数，默认为 1。                                                                  |
| columns/tags | [name](#name)                                 | 这一列的名称，连续的列名将是 name\_#{number}。                                            |
| columns/tags | min                                           | 数字数据类型列/标签的最小值                                                               |
| columns/tags | max                                           | 数字数据类型列/标签的最大值                                                               |
| columns/tags | values                                        | nchar/binary 列/标签的值，将从值中随机选择。                                              |

### 2、查询测试 json 配置文件

```json
{
  "filetype": "query",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "confirm_parameter_prompt": "no",
  "databases": "db",
  "query_times": 2,
  "query_mode": "taosc",
  "specified_table_query": {
    "query_interval": 1,
    "concurrent": 3,
    "sqls": [
      {
        "sql": "select last_row(*) from stb0 ",
        "result": "./query_res0.txt"
      },
      {
        "sql": "select count(*) from stb00_1",
        "result": "./query_res1.txt"
      }
    ]
  },
  "super_table_query": {
    "stblname": "stb1",
    "query_interval": 1,
    "threads": 3,
    "sqls": [
      {
        "sql": "select last_row(ts) from xxxx",
        "result": "./query_res2.txt"
      }
    ]
  }
}
```

### 查询测试 JSON 文件的参数

| 组                                      | 选项                     | 描述                                          |
| --------------------------------------- | ------------------------ | --------------------------------------------- | ------------ |
|                                         | filetype                 | 文件类型，指定哪种测试，对于查询测试，需要    |
|                                         | cfgdir                   | taosd 配置文件所在的目录。                    |
|                                         | host                     | taosd 服务器的 FQDN，默认为 localhost。       |
|                                         | port                     | taosd 服务器的端口号，默认为 6030。           |
|                                         | user                     | 连接 taosd 服务器的用户名，默认为 root。      |
|                                         | password                 | 连接 taosd 服务器的密码，默认为 taosdata。    |
|                                         | confirm_parameter_prompt | 在执行过程中传递确认提示，默认为否。          |
|                                         | database                 | 数据库的名称，需要                            |
|                                         | query_times              | 查询次数                                      |
|                                         | query mode               | 查询模式，选项：taosc 和 rest，默认为 taosc。 |
| specified_table_query/super_table_query | query_interval           | 查询时间间隔，单位是秒，默认是 0              |
| specified_table_query/super_table_query | concurrent/threads       | 执行 SQL 的线程数，默认为 1。                 |
| super_table_query                       | stblname                 | supertable name, required                     | 超级表名称。 |
| sqls                                    | [sql](#sql)              | SQL 命令，必填                                |
| sqls                                    | result                   | 查询结果的结果文件，没有则为空。              |

### 3、订阅 json 配置文件

```json
{
  "filetype": "subscribe",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "databases": "db",
  "confirm_parameter_prompt": "no",
  "specified_table_query": {
    "concurrent": 1,
    "interval": 0,
    "restart": "yes",
    "keepProgress": "yes",
    "sqls": [
      {
        "sql": "select * from stb00_0 ;",
        "result": "./subscribe_res0.txt"
      }
    ]
  },
  "super_table_query": {
    "stblname": "stb0",
    "threads": 1,
    "interval": 10000,
    "restart": "yes",
    "keepProgress": "yes",
    "sqls": [
      {
        "sql": "select * from xxxx where ts > '2021-02-25 11:35:00.000' ;",
        "result": "./subscribe_res1.txt"
      }
    ]
  }
}
```

### 订阅测试 JSON 文件的参数

| 组                                      | 选项                     | 描述                                       |
| --------------------------------------- | ------------------------ | ------------------------------------------ |
|                                         | filetype                 | 文件类型，指定哪种测试，对于订阅测试，需要 |
|                                         | cfgdir                   | taosd 配置文件的目录。                     |
|                                         | host                     | taosd 服务器的 FQDN，默认为 localhost。    |
|                                         | port                     | taosd 服务器的端口号，默认为 6030。        |
|                                         | user                     | 连接 taosd 服务器的用户名，默认为 root。   |
|                                         | password                 | 连接 taosd 服务器的密码，默认为 taosdata。 |
|                                         | databases                | 数据库名称，需要                           |
|                                         | confirm_parameter_prompt | 在执行过程中是否通过确认提示。             |
| specified_table_query/super_table_query | concurrent/threads       | 执行 SQL 的线程数，默认为 1。              |
| specified_table_query/super_table_query | interval                 | 执行订阅的时间间隔，默认为 0。             |
| specified_table_query/super_table_query | restart                  | no: 继续之前的订阅，yes: 开始新的订阅。    |
| specified_table_query/super_table_query | keepProgress             | 是否保留订阅的进度。                       |
| specified_table_query/super_table_query | resubAfterConsume        | 是否取消订阅，然后再次订阅？               |
| super_table_query                       | stblname                 | supertable 的名称，必须的。                |
| sqls                                    | [sql](#sql)              | SQL 命令，必填                             |
| sqls                                    | result                   | 查询结果的结果文件，没有则为空。           |

## 参数具体说明

#### [返回](#taosbenchmark)

- #### insert mode

  可选项有 taosc, rest, stmt, sml, sml-rest, 分别对应 c 接口，restful，参数绑定，schemaless 的 c 接口，taosAdapter schemaless 写入

- #### insert interval

  只有当[interlace rows](#interlace-rows)大于 0 时才起作用。
  意味着线程在为每个子表插入隔行扫描记录后，会等待该值的时间再进行下一轮写入。

- #### partial col num

  若该值为 5， 则仅向前 5 列写入，仅当 [insert_mode](#insert-mode) 为 taosc 和 rest 时生效，为 0 则是向全部列写入。

- #### batch create tbl num

  创建子表时的批数，默认为 10。

  注：实际的批数不一定与该值相同，当执行的 SQL 语句大于支持的最大长度时，会自动截断再执行，继续创建。

- #### auto create table

  仅当 [insert_mode](#insert-mode) 为 taosc, rest, stmt 时 并且 childtable_exists 为 ”no“ 时生效，此参数表示执行写入的 SQL 语句，若为 “yes” 则为 ` insert into tb using stb tags (xxx,xxx,...) values (xxx,xxx,...)` 这样的格式，若为 “no“ 则为 ` insert into tb values (xxx,xxx,...)` 这样的格式，前者不需要提前建立子表，后者需要。

- #### interlace rows

  如果它的值为 0，意味着逐个子表逐个子表插入，如果它的值大于零，比如 5，将首先插入
  到第一个子表的 5 条记录，然后是第二个子表 5 条记录、然后第三个，以此类推。当所有的子表都写入 5 条记录后，线程将从第一个子表继续写入，以此类推。

- #### record per request

  每次插入数据请求/调用 api 时包含的数据行数，也是批次数，当批次数过大时，taos 客户端会返回相应的错误信息，此时需要调整这个数来满足写入要求。

- #### columns

  如果同时设置了该参数和[-b/--data-type](#data-type)，会比较该参数与[-b/--data-type](#data-type)设置的列类型的列的个数，如果前者大于后者，比如: `-l 5 -b float,double`， 那么最后的列为 `FLOAT,DOUBLE,INT,INT,INT`。如果前者小于等于后者，比如: `-l 3 -b float,double,float,bigint`，那么最后的列为 `FLOAT,DOUBLE,FLOAT,BIGINT`。

- #### tag type

  设置超级表的标签类型，nchar 和 binary 也可以设置长度，例如:

  ```
  taosBenchmark -A INT,DOUBLE,NCHAR,BINARY(16)
  ```

  默认是 INT,BINARY(16)。

  注意：在有的 shell 比如 bash 命令里面 “()” 需要转义，则上述指令应为：

  ```
  taosBenchmark -A INT,DOUBLE,NCHAR,BINARY\(16\)
  ```

- #### data type

  与[-A/--tag-type](#tag-type)相同，但用于列，默认为 FLOAT,INT,FLOAT

- #### random

  默认的情况下，数据是模拟电表的采集点的数据，数据值有特定的大小范围，若配置次参数，数据将随机从支持最大的正负 32 位整数中产生。若有修改表结构的其他参数，如 [-l](#columns), [-b](#data-type), [-A](#tag-type)等...，将自动使用从支持最大的正负 32 位整数内随机产生的数据。

- #### disorder ratio

  随机乱序时间戳的概率，最大为 50，即为 50%。随机的乱序时间戳为当前应插入数据的时间戳倒退随机[disorder-range](#disorder-range)内的时间戳。

- #### disorder range

  只有当[-O/--disorder](#disorder-ratio)大于 0 时才有效，单位与数据库的时间精确度相同。

- #### prepared rand

  作为数据源预先生成的随机数据的数量，小的 prepare_rand
  会节省内存，但会减少数据种类。若为 1，则生成的所有同类型的数据相同。

- #### connection pool size

  所有与 taos c 客户端相关的交互都需要 taos 实例，实例会预先创建，以防止运行过程中由于中断导致的失败，该值决定 taos 实例预先创建的数量。若不配置，则与线程数相同。

- #### data source

  数据的来源，默认为 taosBenchmark 随机产生，可以配置为 sample，即为使用 sample_file 参数指定的文件内的数据。

- #### line protocol

  行协议，仅当 [insert_mode](#insert-mode) 为 sml 与 sml-rest 时生效，可选项为 line, telnet, json。

- #### non stop mode

  写入模式是否为不停的持续写入，若为 “yes” 则 insert_rows 失效，直到 Ctrl + C 停止程序，写入才会停止。

  注：即使 insert_rows 失效，但是也必须是一个非零正整数。

- #### tcp transfer

  仅当 [insert_mode](#insert-mode) 为 sml-rest 并且 [line_protocol](#line-protocol) 为 telnet 时生效，支持两种通讯协议: tcp 与 http， 默认为 http。

- #### normal table

  仅当 [insert_mode](#insert-mode) 为 taosc, stmt, rest 模式下可以使用，不创建超级表，只创建普通表。

- #### childtable limit

  仅当 childtable_exists 为 yes 时生效，为使用语句
  ` select tbname from stable limit x offset y;`
  中的 limit 的值 x，即为获取插入的现有的子表数量。

- #### childtable offset

  仅当 childtable_exists 为 yes 时生效，为使用语句
  ` select tbname from stable limit x offset y;`
  中的 offset 的值 y，即为获取插入子表的偏移量。

- #### sample file

  是否使用以 csv 格式的数据作为数据源，仅当 data_source 为 sample 时生效。

  注：这里与最终生成的数据不同，最终数据源的数据与[prepared_rand](#prepared-rand)的值有关，若 csv 文件内的数据行数小于 prepared_rand，那么会循环读取 csv 文件数据直到与 prepared_rand 相同，若大于，则会只读取 prepared_rand 个数的行的数据。

- #### use sample ts

  仅当 data_source 为 sample 时生效，sample_file 指定的 csv 文件内是否包含第一列时间戳，默认为 no， 若为 yes， 则使用 csv 文件第一列的时间戳，由于同一子表时间戳不能重复，生成的数据个数必须与 csv 文件内的数据行数相同，此时 insert_rows 失效。

- #### tags file

  仅当 [insert_mode](#insert-mode) 为 taosc, rest 的模式下生效。

  注：这里原理与 [sample-file](#sample-file) 类似，最终的 tag 的数值与 childtable_count 有关，如果 csv 文件内的 tag 数据行小于给定的子表数量，那么会循环读取 csv 文件数据直到与子表数量相同，若大于，则只会读取 childtable_count 行 tag 数据。

- #### type

  可选值请参考官方支持的数据类型(https://www.taosdata.com/docs/cn/v2.0/taos-sql#data-type)。

  注：JSON 数据类型比较特殊，只有在 tags 里面可以选择，并且有且仅有一列 JSON tag 才可以，此时 [count](#count) 和 [len](#len) 代表的意义分别是 JSON tag 内的 key-value pair 的个数和每个 KV pair 的 value 的值的长度，value 默认为 string。

- #### count

  该列连续的个数，比如我们想测试 4096 个列的性能时，不用罗列出 4096 个列来表示，直接使用 `"count": 4096` 即可。

- #### len

  该数据类型的长度，对 NCHAR，BINARY 和 JSON 数据类型有效，若对其他数据类型配置，若为 0 ， 则代表这列始终都是以 null 值写入，若为其他数，则没有意义，不建议配置。

- #### name

  列的名字，若与 count 同时使用，比如 `"name"："current", "count":3`, 则 3 个列的名字分别为 current, current_2. current_3

- #### sql

  对于超级表的查询 SQL，在 SQL 命令中保留 "xxxx"，程序会自动将其替换为超级表的所有子表名。
  替换为超级表中所有的子表名。
