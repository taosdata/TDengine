---
title: taosBenchmark
sidebar_label: taosBenchmark
toc_max_heading_level: 4
description: 'taosBenchmark (曾用名 taosdemo ) 是一个用于测试 TDengine 产品性能的工具'
---

## 简介

taosBenchmark (曾用名 taosdemo ) 是一个用于测试 TDengine 产品性能的工具。taosBenchmark 可以测试 TDengine 的插入、查询和订阅等功能的性能，它可以模拟由大量设备产生的大量数据，还可以灵活地控制数据库、超级表、标签列的数量和类型、数据列的数量和类型、子表的数量、每张子表的数据量、插入数据的时间间隔、taosBenchmark 的工作线程数量、是否以及如何插入乱序数据等。为了兼容过往用户的使用习惯，安装包提供 了 taosdemo 作为 taosBenchmark 的软链接。

## 安装

taosBenchmark 有两种安装方式:

- 安装 TDengine 官方安装包的同时会自动安装 taosBenchmark, 详情请参考[ TDengine 安装](../../get-started/)。

- 单独编译 taos-tools 并安装, 详情请参考 [taos-tools](https://github.com/taosdata/taos-tools) 仓库。

## 运行

### 配置和运行方式

taosBenchmark 需要在操作系统的终端执行,该工具支持两种配置方式：[命令行参数](#命令行参数详解) 和 [JSON 配置文件](#配置文件参数详解)。这两种方式是互斥的，在使用配置文件时只能使用一个命令行参数 `-f <json file>` 指定配置文件。在使用命令行参数运行 taosBenchmark 并控制其行为时则不能使用 `-f` 参数而要用其它参数来进行配置。除此之外，taosBenchmark 还提供了一种特殊的运行方式，即无参数运行。

taosBenchmark 支持对 TDengine 做完备的性能测试，其所支持的 TDengine 功能分为三大类：写入、查询和订阅。这三种功能之间是互斥的，每次运行 taosBenchmark 只能选择其中之一。值得注意的是，所要测试的功能类型在使用命令行配置方式时是不可配置的，命令行配置方式只能测试写入性能。若要测试 TDengine 的查询和订阅性能，必须使用配置文件的方式，通过配置文件中的参数 `filetype` 指定所要测试的功能类型。

**在运行 taosBenchmark 之前要确保 TDengine 集群已经在正确运行。**

### 无命令行参数运行

执行下列命令即可快速体验 taosBenchmark 对 TDengine 进行基于默认配置的写入性能测试。

```bash
taosBenchmark
```

在无参数运行时，taosBenchmark 默认连接 `/etc/taos` 下指定的 TDengine 集群，并在 TDengine 中创建一个名为 test 的数据库，test 数据库下创建名为 meters 的一张超级表，超级表下创建 10000 张表，每张表中写入 10000 条记录。注意，如果已有 test 数据库，这个命令会先删除该数据库后建立一个全新的 test 数据库。

### 使用命令行配置参数运行

在使用命令行参数运行 taosBenchmark 并控制其行为时，`-f <json file>` 参数不能使用。所有配置参数都必须通过命令行指定。以下是使用命令行方式测试 taosBenchmark 写入性能的一个示例。

```bash
taosBenchmark -I stmt -n 200 -t 100
```

上面的命令 `taosBenchmark` 将创建一个名为`test`的数据库，在其中建立一张超级表`meters`，在该超级表中建立 100 张子表并使用参数绑定的方式为每张子表插入 200 条记录。

### 使用配置文件运行

taosBenchmark 安装包中提供了配置文件的示例，位于 `<install_directory>/examples/taosbenchmark-json` 下

使用如下命令行即可运行 taosBenchmark 并通过配置文件控制其行为。

```bash
taosBenchmark -f <json file>
```

**下面是几个配置文件的示例：**

#### 插入场景 JSON 配置文件示例

<details>
<summary>insert.json</summary>

```json
{{#include /taos-tools/example/insert.json}}
```

</details>

#### 查询场景 JSON 配置文件示例

<details>
<summary>query.json</summary>

```json
{{#include /taos-tools/example/query.json}}
```

</details>

#### 订阅场景 JSON 配置文件示例 

<details>
<summary>tmq.json</summary>

```json
{{#include /taos-tools/example/tmq.json}}
```

</details>

## 命令行参数详解

- **-f/--file <json file\>** :
  要使用的 JSON 配置文件，由该文件指定所有参数，本参数与命令行其他参数不能同时使用。没有默认值。

- **-c/--config-dir <dir\>** :
  TDengine 集群配置文件所在的目录，默认路径是 /etc/taos 。

- **-h/--host <host\>** :
  指定要连接的 TDengine 服务端的 FQDN，默认值为 localhost 。

- **-P/--port <port\>** :
  要连接的 TDengine 服务器的端口号，默认值为 6030 。

- **-I/--interface <insertMode\>** :
  插入模式，可选项有 taosc, rest, stmt, sml, sml-rest, 分别对应普通写入、restful 接口写入、参数绑定接口写入、schemaless 接口写入、restful schemaless 接口写入 (由 taosAdapter 提供)。默认值为 taosc。

- **-u/--user <user\>** :
  用于连接 TDengine 服务端的用户名，默认为 root 。

- **-U/--supplement-insert ** :
  写入数据而不提前建数据库和表，默认关闭。

- **-p/--password <passwd\>** :
  用于连接 TDengine 服务端的密码，默认值为 taosdata。

- **-o/--output <file\>** :
  结果输出文件的路径，默认值为 ./output.txt。

- **-T/--thread <threadNum\>** :
  插入数据的线程数量，默认为 8 。

- **-B/--interlace-rows <rowNum\>** :
  启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0， 即向一张子表完成数据插入后才会向下一张子表进行数据插入。

- **-i/--insert-interval <timeInterval\>** :
  指定交错插入模式的插入间隔，单位为 ms，默认值为 0。 只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。

- **-r/--rec-per-req <rowNum\>** :
  每次向 TDengine 请求写入的数据行数，默认值为 30000 。

- **-t/--tables <tableNum\>** :
  指定子表的数量，默认为 10000 。

- **-S/--timestampstep <stepLength\>** :
  每个子表中插入数据的时间戳步长，单位是 ms，默认值是 1。

- **-n/--records <recordNum\>** :
  每个子表插入的记录数，默认值为 10000 。

- **-d/--database <dbName\>** :
  所使用的数据库的名称，默认值为 test 。

- **-b/--data-type <colType\>** :
  超级表的数据列的类型。如果不使用则默认为有三个数据列，其类型分别为 FLOAT, INT, FLOAT 。

- **-l/--columns <colNum\>** :
  超级表的数据列的总数量。如果同时设置了该参数和 `-b/--data-type`，则最后的结果列数为两者取大。如果本参数指定的数量大于 `-b/--data-type` 指定的列数，则未指定的列类型默认为 INT， 例如: `-l 5 -b float,double`， 那么最后的列为 `FLOAT,DOUBLE,INT,INT,INT`。如果 columns 指定的数量小于或等于 `-b/--data-type` 指定的列数，则结果为 `-b/--data-type` 指定的列和类型，例如: `-l 3 -b float,double,float,bigint`，那么最后的列为 `FLOAT,DOUBLE,FLOAT,BIGINT` 。

- **-L/--partial-col-num <colNum\> **：
  指定某些列写入数据，其他列数据为 NULL。默认所有列都写入数据。

- **-A/--tag-type <tagType\>** :
  超级表的标签列类型。nchar 和 binary 类型可以同时设置长度，例如:

```
taosBenchmark -A INT,DOUBLE,NCHAR,BINARY(16)
```

如果没有设置标签类型，默认是两个标签，其类型分别为 INT 和 BINARY(16)。
注意：在有的 shell 比如 bash 命令里面 “()” 需要转义，则上述指令应为：

```
taosBenchmark -A INT,DOUBLE,NCHAR,BINARY\(16\)
```

- **-w/--binwidth <length\>**:
  nchar 和 binary 类型的默认长度，默认值为 64。

- **-m/--table-prefix <tablePrefix\>** :
  子表名称的前缀，默认值为 "d"。

- **-E/--escape-character** :
  开关参数，指定在超级表和子表名称中是否使用转义字符。默认值为不使用。

- **-C/--chinese** :
  开关参数，指定 nchar 和 binary 是否使用 Unicode 中文字符。默认值为不使用。

- **-N/--normal-table** :
  开关参数，指定只创建普通表，不创建超级表。默认值为 false。仅当插入模式为 taosc, stmt, rest 模式下可以使用。

- **-M/--random** :
  开关参数，插入数据为生成的随机值。默认值为 false。若配置此参数，则随机生成要插入的数据。对于数值类型的 标签列/数据列，其值为该类型取值范围内的随机值。对于 NCHAR 和 BINARY 类型的 标签列/数据列，其值为指定长度范围内的随机字符串。

- **-x/--aggr-func** :
  开关参数，指示插入后查询聚合函数。默认值为 false。

- **-y/--answer-yes** :
  开关参数，要求用户在提示后确认才能继续。默认值为 false 。

- **-O/--disorder <Percentage\>** :
  指定乱序数据的百分比概率，其值域为 [0,50]。默认为 0，即没有乱序数据。

- **-R/--disorder-range <timeRange\>** :
  指定乱序数据的时间戳回退范围。所生成的乱序时间戳为非乱序情况下应该使用的时间戳减去这个范围内的一个随机值。仅在 `-O/--disorder` 指定的乱序数据百分比大于 0 时有效。

- **-F/--prepare_rand <Num\>** :
  生成的随机数据中唯一值的数量。若为 1 则表示所有数据都相同。默认值为 10000 。

- **-a/--replica <replicaNum\>** :
  创建数据库时指定其副本数，默认值为 1 。

- ** -k/--keep-trying <NUMBER\>** : 失败后进行重试的次数，默认不重试。需使用 v3.0.9 以上版本。

- ** -z/--trying-interval <NUMBER\>** : 失败重试间隔时间，单位为毫秒，仅在 -k 指定重试后有效。需使用 v3.0.9 以上版本。

- **-v/--vgroups <NUMBER\>** :
  创建数据库时指定 vgroups 数，仅对 TDengine v3.0+ 有效。

- **-V/--version** :
  显示版本信息并退出。不能与其它参数混用。

- **-?/--help** :
  显示帮助信息并退出。不能与其它参数混用。

## 配置文件参数详解

### 通用配置参数

本节所列参数适用于所有功能模式。

- **filetype** : 要测试的功能，可选值为 `insert`, `query` 和 `subscribe`。分别对应插入、查询和订阅功能。每个配置文件中只能指定其中之一。
- **cfgdir** : TDengine 客户端配置文件所在的目录，默认路径是 /etc/taos 。

- **host** : 指定要连接的 TDengine 服务端的 FQDN，默认值为 localhost。

- **port** : 要连接的 TDengine 服务器的端口号，默认值为 6030。

- **user** : 用于连接 TDengine 服务端的用户名，默认为 root。

- **password** : 用于连接 TDengine 服务端的密码，默认值为 taosdata。

### 插入场景配置参数

插入场景下 `filetype` 必须设置为 `insert`，该参数及其它通用参数详见[通用配置参数](#通用配置参数)

- ** keep_trying ** : 失败后进行重试的次数，默认不重试。需使用 v3.0.9 以上版本。

- ** trying_interval ** : 失败重试间隔时间，单位为毫秒，仅在 keep_trying 指定重试后有效。需使用 v3.0.9 以上版本。
- ** childtable_from 和 childtable_to ** : 指定写入子表范围，开闭区间为 [childtable_from, childtable_to).
 
- ** continue_if_fail ** : 允许用户定义失败后行为

  “continue_if_fail”:  “no”, 失败 taosBenchmark 自动退出，默认行为
  “continue_if_fail”: “yes”, 失败 taosBenchmark 警告用户，并继续写入
  “continue_if_fail”: “smart”, 如果子表不存在失败，taosBenchmark 会建立子表并继续写入

#### 数据库相关配置参数

创建数据库时的相关参数在 json 配置文件中的 `dbinfo` 中配置，个别具体参数如下。其余参数均与 TDengine 中 `create database` 时所指定的数据库参数相对应，详见[../../taos-sql/database]

- **name** : 数据库名。

- **drop** : 插入前是否删除数据库，可选项为 "yes" 或者 "no", 为 "no" 时不创建。默认删除。

#### 流式计算相关配置参数

创建流式计算的相关参数在 json 配置文件中的 `stream` 中配置，具体参数如下。

- **stream_name** : 流式计算的名称，必填项。

- **stream_stb** : 流式计算对应的超级表名称，必填项。

- **stream_sql** : 流式计算的sql语句，必填项。

- **trigger_mode** : 流式计算的触发模式，可选项。

- **watermark** : 流式计算的水印，可选项。

- **drop** : 是否创建流式计算，可选项为 "yes" 或者 "no", 为 "no" 时不创建。

#### 超级表相关配置参数

创建超级表时的相关参数在 json 配置文件中的 `super_tables` 中配置，具体参数如下。

- **name**: 超级表名，必须配置，没有默认值。

- **child_table_exists** : 子表是否已经存在，默认值为 "no"，可选值为 "yes" 或 "no"。

- **child_table_count** : 子表的数量，默认值为 10。

- **child_table_prefix** : 子表名称的前缀，必选配置项，没有默认值。

- **escape_character** : 超级表和子表名称中是否包含转义字符，默认值为 "no"，可选值为 "yes" 或 "no"。

- **auto_create_table** : 仅当 insert_mode 为 taosc, rest, stmt 并且 childtable_exists 为 "no" 时生效，该参数为 "yes" 表示 taosBenchmark 在插入数据时会自动创建不存在的表；为 "no" 则表示先提前建好所有表再进行插入。

- **batch_create_tbl_num** : 创建子表时每批次的建表数量，默认为 10。注：实际的批数不一定与该值相同，当执行的 SQL 语句大于支持的最大长度时，会自动截断再执行，继续创建。

- **data_source** : 数据的来源，默认为 taosBenchmark 随机产生，可以配置为 "rand" 和 "sample"。为 "sample" 时使用 sample_file 参数指定的文件内的数据。

- **insert_mode** : 插入模式，可选项有 taosc, rest, stmt, sml, sml-rest, 分别对应普通写入、restful 接口写入、参数绑定接口写入、schemaless 接口写入、restful schemaless 接口写入 (由 taosAdapter 提供)。默认值为 taosc 。

- **non_stop_mode** : 指定是否持续写入，若为 "yes" 则 insert_rows 失效，直到 Ctrl + C 停止程序，写入才会停止。默认值为 "no"，即写入指定数量的记录后停止。注：即使在持续写入模式下 insert_rows 失效，但其也必须被配置为一个非零正整数。

- **line_protocol** : 使用行协议插入数据，仅当 insert_mode 为 sml 或 sml-rest 时生效，可选项为 line, telnet, json。

- **tcp_transfer** : telnet 模式下的通信协议，仅当 insert_mode 为 sml-rest 并且 line_protocol 为 telnet 时生效。如果不配置，则默认为 http 协议。

- **insert_rows** : 每个子表插入的记录数，默认为 0 。

- **childtable_offset** : 仅当 childtable_exists 为 yes 时生效，指定从超级表获取子表列表时的偏移量，即从第几个子表开始。

- **childtable_limit** : 仅当 childtable_exists 为 yes 时生效，指定从超级表获取子表列表的上限。

- **interlace_rows** : 启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0， 即向一张子表完成数据插入后才会向下一张子表进行数据插入。

- **insert_interval** : 指定交错插入模式的插入间隔，单位为 ms，默认值为 0。 只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。

- **partial_col_num** : 若该值为正数 n 时， 则仅向前 n 列写入，仅当 insert_mode 为 taosc 和 rest 时生效，如果 n 为 0 则是向全部列写入。

- **disorder_ratio** : 指定乱序数据的百分比概率，其值域为 [0,50]。默认为 0，即没有乱序数据。

- **disorder_range** : 指定乱序数据的时间戳回退范围。所生成的乱序时间戳为非乱序情况下应该使用的时间戳减去这个范围内的一个随机值。仅在 `-O/--disorder` 指定的乱序数据百分比大于 0 时有效。

- **timestamp_step** : 每个子表中插入数据的时间戳步长，单位与数据库的 `precision` 一致，默认值是 1。

- **start_timestamp** : 每个子表的时间戳起始值，默认值是 now。

- **sample_format** : 样本数据文件的类型，现在只支持 "csv" 。

- **sample_file** : 指定 csv 格式的文件作为数据源，仅当 data_source 为 sample 时生效。若 csv 文件内的数据行数小于等于 prepared_rand，那么会循环读取 csv 文件数据直到与 prepared_rand 相同；否则则会只读取 prepared_rand 个数的行的数据。也即最终生成的数据行数为二者取小。

- **use_sample_ts** : 仅当 data_source 为 sample 时生效，表示 sample_file 指定的 csv 文件内是否包含第一列时间戳，默认为 no。 若设置为 yes， 则使用 csv 文件第一列作为时间戳，由于同一子表时间戳不能重复，生成的数据量取决于 csv 文件内的数据行数相同，此时 insert_rows 失效。

- **tags_file** : 仅当 insert_mode 为 taosc, rest 的模式下生效。 最终的 tag 的数值与 childtable_count 有关，如果 csv 文件内的 tag 数据行小于给定的子表数量，那么会循环读取 csv 文件数据直到生成 childtable_count 指定的子表数量；否则则只会读取 childtable_count 行 tag 数据。也即最终生成的子表数量为二者取小。

#### tsma配置参数

指定tsma的配置参数在 `super_tables` 中的 `tsmas` 中，具体参数如下。

- **name** : 指定 tsma 的名字，必选项。

- **function** : 指定 tsma 的函数，必选项。

- **interval** : 指定 tsma 的时间间隔，必选项。

- **sliding** : 指定 tsma 的窗口时间位移，必选项。

- **custom** : 指定 tsma 的创建语句结尾追加的自定义配置，可选项。

- **start_when_inserted** : 指定当插入多少行时创建 tsma，可选项，默认为 0。

#### 标签列与数据列配置参数

指定超级表标签列与数据列的配置参数分别在 `super_tables` 中的 `columns` 和 `tag` 中。

- **type** : 指定列类型，可选值请参考 TDengine 支持的数据类型。
  注：JSON 数据类型比较特殊，只能用于标签，当使用 JSON 类型作为 tag 时有且只能有这一个标签，此时 count 和 len 代表的意义分别是 JSON tag 内的 key-value pair 的个数和每个 KV pair 的 value 的值的长度，value 默认为 string。

- **len** : 指定该数据类型的长度，对 NCHAR，BINARY 和 JSON 数据类型有效。如果对其他数据类型配置了该参数，若为 0 ， 则代表该列始终都是以 null 值写入；如果不为 0 则被忽略。

- **count** : 指定该类型列连续出现的数量，例如 "count": 4096 即可生成 4096 个指定类型的列。

- **name** : 列的名字，若与 count 同时使用，比如 "name"："current", "count":3, 则 3 个列的名字分别为 current, current_2. current_3。

- **min** : 数据类型的 列/标签 的最小值。生成的值将大于或等于最小值。

- **max** : 数据类型的 列/标签 的最大值。生成的值将小于最小值。

- **fun** : 此列数据以函数填充，目前只支持 sin 和 cos 两函数，输入参数为时间戳换算成角度值，换算公式： 角度 x = 输入的时间列ts值 % 360。同时支持系数调节，随机波动因子调节，以固定格式的表达式展现，如 fun=“10\*sin(x)+100\*random(5)” , x 表示角度，取值 0 ~ 360度，增长步长与时间列步长一致。10 表示乘的系数，100 表示加或减的系数，5 表示波动幅度在 5% 的随机范围内。目前支持的数据类型为 int, bigint, float, double 四种数据类型。注意：表达式为固定模式，不可前后颠倒。

- **values** : nchar/binary 列/标签的值域，将从值中随机选择。

- **sma**: 将该列加入 SMA 中，值为 "yes" 或者 "no"，默认为 "no"。

#### 插入行为配置参数

- **thread_count** : 插入数据的线程数量，默认为 8。

- **create_table_thread_count** : 建表的线程数量，默认为 8。

- **connection_pool_size** : 预先建立的与 TDengine 服务端之间的连接的数量。若不配置，则与所指定的线程数相同。

- **result_file** : 结果输出文件的路径，默认值为 ./output.txt。

- **confirm_parameter_prompt** : 开关参数，要求用户在提示后确认才能继续。默认值为 false 。

- **interlace_rows** : 启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0， 即向一张子表完成数据插入后才会向下一张子表进行数据插入。
  在 `super_tables` 中也可以配置该参数，若配置则以 `super_tables` 中的配置为高优先级，覆盖全局设置。

- **insert_interval** :
  指定交错插入模式的插入间隔，单位为 ms，默认值为 0。 只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。
  在 `super_tables` 中也可以配置该参数，若配置则以 `super_tables` 中的配置为高优先级，覆盖全局设置。

- **num_of_records_per_req** :
  每次向 TDengine 请求写入的数据行数，默认值为 30000 。当其设置过大时，TDengine 客户端驱动会返回相应的错误信息，此时需要调低这个参数的设置以满足写入要求。

- **prepare_rand** : 生成的随机数据中唯一值的数量。若为 1 则表示所有数据都相同。默认值为 10000 。

### 查询场景配置参数

查询场景下 `filetype` 必须设置为 `query`。
`query_times` 指定运行查询的次数，数值类型

查询场景可以通过设置 `kill_slow_query_threshold` 和 `kill_slow_query_interval` 参数来控制杀掉慢查询语句的执行，threshold 控制如果 exec_usec 超过指定时间的查询将被 taosBenchmark 杀掉，单位为秒；interval 控制休眠时间，避免持续查询慢查询消耗 CPU ，单位为秒。

其它通用参数详见[通用配置参数](#通用配置参数)。

#### 执行指定查询语句的配置参数

查询指定表（可以指定超级表、子表或普通表）的配置参数在 `specified_table_query` 中设置。

- **query_interval** : 查询时间间隔，单位是秒，默认值为 0。

- **threads/concurrent** : 执行查询 SQL 的线程数，默认值为 1。

- **sqls**：
  - **sql**: 执行的 SQL 命令，必填。
  - **result**: 保存查询结果的文件，未指定则不保存。

#### 查询超级表的配置参数

查询超级表的配置参数在 `super_table_query` 中设置。

- **stblname** : 指定要查询的超级表的名称，必填。

- **query_interval** : 查询时间间隔，单位是秒，默认值为 0。

- **threads** : 执行查询 SQL 的线程数，默认值为 1。

- **sqls** ：
  - **sql** : 执行的 SQL 命令，必填；对于超级表的查询 SQL，在 SQL 命令中保留 "xxxx"，程序会自动将其替换为超级表的所有子表名。
    替换为超级表中所有的子表名。
  - **result** : 保存查询结果的文件，未指定则不保存。

### 订阅场景配置参数

订阅场景下 `filetype` 必须设置为 `subscribe`，该参数及其它通用参数详见[通用配置参数](#通用配置参数)

#### 执行指定订阅语句的配置参数

订阅指定表（可以指定超级表、子表或者普通表）的配置参数在 `specified_table_query` 中设置。

- **threads/concurrent** : 执行 SQL 的线程数，默认为 1。

- **sqls** ：
  - **sql** : 执行的 SQL 命令，必填。
 
#### 配置文件中数据类型书写对照表

| #   |     **引擎**      | **taosBenchmark** 
| --- | :----------------: | :---------------:
| 1   |  TIMESTAMP         |    timestamp
| 2   |  INT               |    int
| 3   |  INT UNSIGNED      |    uint
| 4   |  BIGINT            |    bigint
| 5   |  BIGINT UNSIGNED   |    ubigint
| 6   |  FLOAT             |    float
| 7   |  DOUBLE            |    double
| 8   |  BINARY            |    binary
| 9   |  SMALLINT          |    smallint
| 10  |  SMALLINT UNSIGNED |    usmallint
| 11  |  TINYINT           |    tinyint
| 12  |  TINYINT UNSIGNED  |    utinyint
| 13  |  BOOL              |    bool
| 14  |  NCHAR             |    nchar
| 15  |  VARCHAR           |    varchar
| 15  |  JSON              |    json

注意：taosBenchmark 配置文件中数据类型必须小写方可识别



