---
title: taosBenchmark 参考手册
sidebar_label: taosBenchmark
toc_max_heading_level: 4
---

taosBenchmark 是 TDengine TSDB 产品性能基准测试工具，提供对 TDengine TSDB 产品写入、查询及订阅性能测试，输出性能指标。

## 工具获取

taosBenchmark 是 TDengine TSDB 服务器及客户端安装包中默认安装组件，安装后即可使用，参考 [TDengine TSDB 安装](../../../get-started/)

## 运行

taosBenchmark 支持无参数、命令行、配置文件三种运行模式，`命令行` 为 `配置文件` 功能子集，两者同时使用时，以命令行方式优先。

:::tip
在运行 taosBenchmark 之前要确保 TDengine TSDB 集群已经在正确运行。
:::

### 无参数模式

```bash
taosBenchmark
```

在无参数运行时，taosBenchmark 默认连接 `/etc/taos/taos.cfg` 中指定的 TDengine TSDB 集群。
连接成功后，会默认创建智能电表示例数据库 test，创建超级表 meters，创建子表 1 万，每子写入数据 1 万条，若 test 库已存在，默认会先删再建。

### 命令行模式

命令行支持的参数为写入功能中使用较为频繁的参数，查询与订阅功能不支持命令行方式。
示例：

```bash
taosBenchmark -d db -t 100 -n 1000 -T 4 -I stmt -y
```

此命令表示使用 `taosBenchmark` 将创建一个名为 `db` 的数据库，并建立默认超级表 `meters`，子表 100，使用参数绑定 (stmt) 方式为每张子表写入 1000 条记录。

### 配置文件模式

以 JSON 配置文件方式运行提供了全部功能，所有命令行参数都可以在配置文件中配置运行。

```bash
taosBenchmark -f <json file>
```

## 命令行参数

| 命令行参数                     | 功能说明                                         |
| ---------------------------- | ----------------------------------------------- |
| -f/--file \<json file>       | 要使用的 JSON 配置文件，由该文件指定所有参数，本参数与命令行其他参数不能同时使用。没有默认值 |
| -c/--config-dir \<dir>       | TDengine TSDB 集群配置文件所在的目录，默认路径是 /etc/taos |
| -h/--host \<host>            | 指定要连接的 TDengine TSDB 服务端的 FQDN，默认值为 localhost  |
| -P/--port \<port>            | 要连接的 TDengine TSDB 服务器的端口号，默认值为 6030 |
| -I/--interface \<insertMode> | 插入模式，可选项有 taosc、rest、stmt、sml、sml-rest，分别对应普通写入、restful 接口写入、参数绑定接口写入、schemaless 接口写入、restful schemaless 接口写入 (由 taosAdapter 提供)。默认值为 taosc |
| -u/--user \<user>            | 用于连接 TDengine TSDB 服务端的用户名，默认为 root  |
| -U/--supplement-insert       | 写入数据而不提前建数据库和表，默认关闭 |
| -p/--password \<passwd>      | 用于连接 TDengine TSDB 服务端的密码，默认值为 taosdata |
| -o/--output \<file>          | 结果输出文件的路径，默认值为 ./output.txt |
| -j/--output-json-file \<file>| 结果输出的 JSON 文件的路径 |
| -T/--thread \<threadNum>     | 插入数据的线程数量，默认为 8  |
| -B/--interlace-rows \<rowNum>        |启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0，即向一张子表完成数据插入后才会向下一张子表进行数据插入 |
| -i/--insert-interval \<timeInterval> | 指定交错插入模式的插入间隔，单位为 ms，默认值为 0。只有当 `-B/--interlace-rows` 大于 0 时才起作用 |意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入 |
| -r/--rec-per-req \<rowNum>           | 每次向 TDengine TSDB 请求写入的数据行数，默认值为 30000  |
| -t/--tables \<tableNum>              | 指定子表的数量，默认为 10000  |
| -s/--start-timestamp \<NUMBER>      | 每个子表数据开始时间，默认值为 1500000000000 |
| -S/--timestampstep \<stepLength>     | 每个子表中插入数据的时间戳步长，单位是 ms，默认值是 1 |
| -n/--records \<recordNum>            | 每个子表插入的记录数，默认值为 10000  |
| -d/--database \<dbName>              | 所使用的数据库的名称，默认值为 test  |
| -b/--data-type \<colType>            | 指定超级表普通列数据类型，多个使用逗号分隔，默认值："FLOAT,INT,FLOAT" 如：`taosBenchmark -b "FLOAT,BINARY(8),NCHAR(16)"`|
| -A/--tag-type  \<tagType>            | 指定超级表标签列数据类型，多个使用逗号分隔，默认值："INT,BINARY(24)"  如：`taosBenchmark -A "INT,BINARY(8),NCHAR(8)"`|
| -l/--columns \<colNum>               | 超级表的数据列的总数量。如果同时设置了该参数和 `-b/--data-type`，则最后的结果列数为两者取大。如果本参数指定的数量大于 `-b/--data-type` 指定的列数，则未指定的列类型默认为 INT，例如  `-l 5 -b float,double`，那么最后的列为 `FLOAT,DOUBLE,INT,INT,INT`。如果 columns 指定的数量小于或等于 `-b/--data-type` 指定的列数，则结果为 `-b/--data-type` 指定的列和类型，例如：`-l 3 -b float,double,float,bigint`，那么最后的列为 `FLOAT,DOUBLE,FLOAT,BIGINT`  |
| -L/--partial-col-num \<colNum>       | 指定某些列写入数据，其他列数据为 NULL。默认所有列都写入数据 |
| -w/--binwidth \<length>          | nchar 和 binary 类型的默认长度，默认值为 64 |
| -m/--table-prefix \<tablePrefix> | 子表名称的前缀，默认值为 "d" |
| -E/--escape-character            | 开关参数，指定在超级表和子表名称中是否使用转义字符。默认值为不使用 |
| -C/--chinese                     | 开关参数，指定 nchar 和 binary 是否使用 Unicode 中文字符。默认值为不使用 |
| -N/--normal-table                | 开关参数，指定只创建普通表，不创建超级表。默认值为 false。仅当插入模式为 taosc、stmt、rest 模式下可以使用 |
| -M/--random                      | 开关参数，插入数据为生成的随机值。默认值为 false。若配置此参数，则随机生成要插入的数据。对于数值类型的 标签列/数据列，其值为该类型取值范围内的随机值。对于 NCHAR 和 BINARY 类型的 标签列/数据列，其值为指定长度范围内的随机字符串 |
| -x/--aggr-func                   | 开关参数，指示插入后查询聚合函数。默认值为 false |
| -y/--answer-yes                  | 开关参数，要求用户在提示后确认才能继续 |默认值为 false。
| -O/--disorder \<Percentage>      | 指定乱序数据的百分比概率，其值域为 [0,50]。默认为 0，即没有乱序数据 |
| -R/--disorder-range \<timeRange> | 指定乱序数据的时间戳回退范围。所生成的乱序时间戳为非乱序情况下应该使用的时间戳减去这个范围内的一个随机值。仅在 `-O/--disorder` 指定的乱序数据百分比大于 0 时有效|
| -F/--prepare_rand \<Num>         | 生成的随机数据中唯一值的数量。若为 1 则表示所有数据都相同。默认值为 10000 |
| -a/--replica \<replicaNum>       | 创建数据库时指定其副本数，默认值为 1 |
| -k/--keep-trying \<NUMBER>      | 失败后进行重试的次数，默认不重试。需使用 v3.0.9 以上版本|
| -z/--trying-interval \<NUMBER>  | 失败重试间隔时间，单位为毫秒，仅在 -k 指定重试后有效。需使用 v3.0.9 以上版本 |
| -v/--vgroups \<NUMBER>           | 创建数据库时指定 vgroups 数，仅对 TDengine TSDB v3.0+ 有效|
| -V/--version                     | 显示版本信息并退出。不能与其它参数混用|
| -?/--help                        | 显示帮助信息并退出。不能与其它参数混用|
| -Z/--connect-mode \<NUMBER>      | 指定连接方式，0 表示采用原生连接方式，1 表示采用 WebSocket 连接方式，默认采用原生连接方式。|

## 配置文件参数

### 通用配置参数

本节所列参数适用于所有功能模式。

- **filetype**：功能分类，可选值为 `insert`、`query`、`subscribe` 和 `csvfile`。分别对应插入、查询、订阅和生成 csv 文件功能。每个配置文件中只能指定其中之一。

- **cfgdir**：TDengine TSDB 客户端配置文件所在的目录，默认路径是 /etc/taos。

- **output_dir**：指定输出文件的目录，当功能分类是 `csvfile` 时，指生成的 csv 文件的保存目录，默认值为 ./output/ 。

- **host**：指定要连接的 TDengine TSDB 服务端的 FQDN，默认值为 localhost。

- **port**：要连接的 TDengine TSDB 服务器的端口号，默认值为 6030。

- **user**：用于连接 TDengine TSDB 服务端的用户名，默认值为 root。

- **password**：用于连接 TDengine TSDB 服务端的密码，默认值为 taosdata。

- **result_json_file**：指定结果输出的 JSON 文件路径，若未配置则不输出该文件。

### 写入配置参数

写入场景下 `filetype` 必须设置为 `insert`，该参数及其它通用参数详见 [通用配置参数](#通用配置参数)

- **keep_trying**：失败后进行重试的次数，默认不重试。需使用 v3.0.9 以上版本。

- **trying_interval**：失败重试间隔时间，单位为毫秒，仅在 keep_trying 指定重试后有效。需使用 v3.0.9 以上版本。

- **childtable_from 和 childtable_to**：指定写入子表范围，开闭区间为 [childtable_from, childtable_to] 。

- **escape_character**：超级表和子表名称中是否包含转义字符，默认值为 "no"，可选值为 "yes" 或 "no" 。

- **continue_if_fail**：允许用户定义失败后行为。

  “continue_if_fail”：“no”，失败 taosBenchmark 自动退出，默认行为。
  “continue_if_fail”：“yes”，失败 taosBenchmark 警告用户，并继续写入。
  “continue_if_fail”：“smart”，如果子表不存在失败，taosBenchmark 会建立子表并继续写入。

#### 数据库相关

创建数据库时的相关参数在 json 配置文件中的 `dbinfo` 中配置，个别具体参数如下。其余参数均与 TDengine TSDB 中 `create database` 时所指定的数据库参数相对应，详见 [../../taos-sql/database]

- **name**：数据库名。

- **drop**：数据库已存在时是否删除，可选项为 "yes" 或 "no"，默认为“yes” 。

#### 超级表相关

创建超级表时的相关参数在 json 配置文件中的 `super_tables` 中配置，具体参数如下。

- **name**：超级表名，必须配置，没有默认值。

- **child_table_exists**：子表是否已经存在，默认值为 "no"，可选值为 "yes" 或 "no" 。

- **childtable_count**：子表的数量，默认值为 10。

- **childtable_prefix**：子表名称的前缀，必选配置项，没有默认值。

- **auto_create_table**：仅当 insert_mode 为 taosc、rest、stmt 并且 child_table_exists 为 "no" 时生效，该参数为 "yes" 表示 taosBenchmark 在插入数据时会自动创建不存在的表；为 "no" 则表示先提前建好所有表再进行插入。

- **batch_create_tbl_num**：创建子表时每批次的建表数量，默认为 10。注：实际的批数不一定与该值相同，当执行的 SQL 语句大于支持的最大长度时，会自动截断再执行，继续创建。

- **data_source**：数据的来源，默认为 taosBenchmark 随机产生，可以配置为 "rand" 和 "sample"。为 "sample" 时使用 sample_file 参数指定的文件内的数据。

- **insert_mode**：插入模式，可选项有 taosc、rest、stmt、stmt2、sml、sml-rest，分别对应普通写入、restful 接口写入、参数绑定接口写入、schemaless 接口写入、restful schemaless 接口写入 (由 taosAdapter 提供)。默认值为 taosc。

- **non_stop_mode**：指定是否持续写入(此参数仅支持 `interlace_rows > 0`)，若为 "yes" 则 insert_rows 失效，直到 Ctrl + C 停止程序，写入才会停止。默认值为 "no"，即写入指定数量的记录后停止。注：即使在持续写入模式下 insert_rows 失效，但其也必须被配置为一个非零正整数。

- **line_protocol**：使用行协议插入数据，仅当 insert_mode 为 sml 或 sml-rest 时生效，可选项为 line、telnet、json。

- **tcp_transfer**：telnet 模式下的通信协议，仅当 insert_mode 为 sml-rest 并且 line_protocol 为 telnet 时生效。如果不配置，则默认为 http 协议。

- **insert_rows**：每个子表插入的记录数，默认为 0。

- **childtable_offset**：仅当 child_table_exists 为 yes 时生效，指定从超级表获取子表列表时的偏移量，即从第几个子表开始。

- **childtable_limit**：仅当 child_table_exists 为 yes 时生效，指定从超级表获取子表列表的上限。

- **interlace_rows**：启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0，即向一张子表完成数据插入后才会向下一张子表进行数据插入。

- **insert_interval**：指定交错插入模式的插入间隔，单位为 ms，默认值为 0。只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。

- **partial_col_num**：若该值为正数 n 时，则仅向前 n 列写入，仅当 insert_mode 为 taosc 和 rest 时生效，如果 n 为 0 则是向全部列写入。

- **disorder_ratio**：指定乱序数据的百分比概率，其值域为 [0,50]。默认为 0，即没有乱序数据。

- **disorder_range**：指定乱序数据的时间戳回退范围。所生成的乱序时间戳为非乱序情况下应该使用的时间戳减去这个范围内的一个随机值。仅在 `-O/--disorder` 指定的乱序数据百分比大于 0 时有效。

- **timestamp_step**：每个子表中插入数据的时间戳步长，单位与数据库的 `precision` 一致，默认值是 1。

- **start_timestamp**：每个子表的时间戳起始值，默认值是 now。

- **sample_format**：样本数据文件的类型，现在只支持 "csv" 。

- **sample_file**：指定 csv 格式的文件作为数据源，仅当 data_source 为 sample 时生效。若 csv 文件内的数据行数小于等于 prepared_rand，那么会循环读取 csv 文件数据直到与 prepared_rand 相同；否则则会只读取 prepared_rand 个数的行的数据。也即最终生成的数据行数为二者取小。

- **use_sample_ts**：仅当 data_source 为 sample 时生效，表示 sample_file 指定的 csv 文件内是否包含第一列时间戳，默认为 no。若设置为 yes，则使用 csv 文件第一列作为时间戳，由于同一子表时间戳不能重复，生成的数据量取决于 csv 文件内的数据行数相同，此时 insert_rows 失效。

- **tags_file**：仅当 insert_mode 为 taosc，rest 的模式下生效。最终的 tag 的数值与 childtable_count 有关，如果 csv 文件内的 tag 数据行小于给定的子表数量，那么会循环读取 csv 文件数据直到生成 childtable_count 指定的子表数量；否则则只会读取 childtable_count 行 tag 数据。也即最终生成的子表数量为二者取小。

- **use_tag_table_name**：当设置为 yes 时，csv 文件内的 tag 数据中的第一列为需要创建的子表名称，反之有系统自动生成子表名称进行创建。

- **primary_key**：指定超级表是否有复合主键，取值 1 和 0，复合主键列只能是超级表的第二列，指定生成复合主键后要确保第二列符合复合主键的数据类型，否则会报错。

- **primary_key_name**：指定超级表主键的名称，如果不指定默认为 `ts`。

- **repeat_ts_min**：数值类型，复合主键开启情况下指定生成相同时间戳记录的最小个数，生成相同时间戳记录的个数是在范围[repeat_ts_min, repeat_ts_max] 内的随机值，最小值等于最大值时为固定个数。

- **repeat_ts_max**：数值类型，复合主键开启情况下指定生成相同时间戳记录的最大个数。

- **sqls**：字符串数组类型，指定超级表创建成功后要执行的 sql 数组，sql 中指定表名前面要带数据库名，否则会报未指定数据库错误。

- **csv_file_prefix**：字符串类型，设置生成的 csv 文件名称的前缀，默认值为 data。

- **csv_ts_format**：字符串类型，设置生成的 csv 文件名称中时间字符串的格式，格式遵循 `strftime` 格式标准，如果没有设置表示不按照时间段切分文件。支持的模式有：
  - %Y: 年份，四位数表示（例如：2025）
  - %m: 月份，两位数表示（01 到 12）
  - %d: 一个月中的日子，两位数表示（01 到 31）
  - %H: 小时，24 小时制，两位数表示（00 到 23）
  - %M: 分钟，两位数表示（00 到 59）
  - %S: 秒，两位数表示（00 到 59）

- **csv_ts_interval**：字符串类型，设置生成的 csv 文件名称中时间段间隔，支持天、小时、分钟、秒级间隔，如 1d/2h/30m/40s，默认值为 1d。

- **csv_output_header**：字符串类型，设置生成的 csv 文件是否包含列头描述，默认值为 yes。

- **csv_tbname_alias**：字符串类型，设置 csv 文件列头描述中 tbname 字段的别名，默认值为 device_id。

- **csv_compress_level**：字符串类型，设置生成 csv 编码数据并自动压缩成 gzip 格式文件的压缩等级。此过程直接编码并压缩，而非先生成 csv 文件再压缩。可选值为：
  - none：不压缩
  - fast：gzip 1 级压缩
  - balance：gzip 6 级压缩
  - best：gzip 9 级压缩

#### 标签列与数据列

指定超级表标签列与数据列的配置参数分别在 `super_tables` 中的 `columns` 和 `tag` 中。

- **type**：指定列类型，可选值请参考 TDengine TSDB 支持的数据类型。
  注：JSON 数据类型比较特殊，只能用于标签，当使用 JSON 类型作为 tag 时有且只能有这一个标签，此时 count 和 len 代表的意义分别是 JSON tag 内的 key-value pair 的个数和每个 KV pair 的 value 的值的长度，value 默认为 string。

- **len**：指定该数据类型的长度，对 NCHAR，BINARY 和 JSON 数据类型有效。如果对其他数据类型配置了该参数，若为 0，则代表该列始终都是以 null 值写入；如果不为 0 则被忽略。

- **count**：指定该类型列连续出现的数量，例如 "count"：4096 即可生成 4096 个指定类型的列。

- **name**：列的名字，若与 count 同时使用，比如 "name"："current"，"count"：3，则 3 个列的名字分别为 current、current_2、current_3。

- **min**：浮点数类型，数据类型的 列/标签 的最小值。生成的值将大于或等于最小值。

- **max**：浮点数类型，数据类型的 列/标签 的最大值。生成的值将小于最大值。

- **dec_min**：字符串类型，指定 DECIMAL 数据类型的列的最小值。当 min 无法表达足够的精度时，使用此字段。生成的值将大于或等于最小值。

- **dec_max**：字符串类型，指定 DECIMAL 数据类型的列的最大值。当 max 无法表达足够的精度时，使用此字段。生成的值将小于最大值。

- **precision**：数字的总位数（包括小数点前后的所有数字），仅适用于 DECIMAL 类型，其有效值范围为 0 至 38。

- **scale**：小数点右边的数字位数。对于 FLOAT 类型，scale 的有效范围是 0 至 6；对于 DOUBLE 类型，该范围是 0 至 15；而对于 DECIMAL 类型，scale 的有效范围是 0 至其 precision 值。

- **fun**：此列数据以函数填充，目前只支持 sin 和 cos 两函数，输入参数为时间戳换算成角度值，换算公式：角度 x = 输入的时间列 ts 值 % 360。同时支持系数调节，随机波动因子调节，以固定格式的表达式展现，如 fun=“10\*sin(x)+100\*random(5)” , x 表示角度，取值 0 ~ 360 度，增长步长与时间列步长一致。10 表示乘的系数，100 表示加或减的系数，5 表示波动幅度在 5% 的随机范围内。目前支持的数据类型为 int、bigint、float、double 四种数据类型。注意：表达式为固定模式，不可前后颠倒。

- **values**：nchar/binary 列/标签的值域，将从值中随机选择。

- **sma**：将该列加入 SMA 中，值为 "yes" 或者 "no"，默认为 "no" 。

- **encode**：字符串类型，指定此列两级压缩中的第一级编码算法，详细参见创建超级表。
  
- **compress**：字符串类型，指定此列两级压缩中的第二级加密算法，详细参见创建超级表。

- **level**：字符串类型，指定此列两级压缩中的第二级加密算法的压缩率高低，详细参见创建超级表。

- **gen**：字符串类型，指定此列生成数据的方式，不指定为随机，若指定为“order”，会按自然数顺序增长。

- **fillNull**：字符串类型，指定此列是否随机插入 NULL 值，可指定为“true”或 "false"，只有当 generate_row_rule 为 2 时有效。

#### 写入行为相关

- **thread_count**：插入数据的线程数量，默认为 8。

- **thread_bind_vgroup**：写入时 vgroup 是否和写入线程绑定，绑定后可提升写入速度，取值为 "yes" 或 "no"，默认值为“no”，设置为“no”后与原来行为一致。当设为“yes”时，如果 thread_count 大于写入数据库 vgroups 数量，thread_count 自动调整为 vgroups 数量；如果 thread_count 小于 vgroups 数量，写入线程数量不做调整，一个线程写完一个 vgroup 数据后再写下一个，同时保持一个 vgroup 同时只能由一个线程写入的规则。

- **create_table_thread_count**：建表的线程数量，默认为 8。

- **result_file**：结果输出文件的路径，默认值为 ./output.txt。

- **confirm_parameter_prompt**：开关参数，要求用户在提示后确认才能继续，可取值 "yes" or "no"。默认值为 "no" 。

- **interlace_rows**：启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0，即向一张子表完成数据插入后才会向下一张子表进行数据插入。
  在 `super_tables` 中也可以配置该参数，若配置则以 `super_tables` 中的配置为高优先级，覆盖全局设置。

- **insert_interval**：
  指定交错插入模式的插入间隔，单位为 ms，默认值为 0。只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。
  在 `super_tables` 中也可以配置该参数，若配置则以 `super_tables` 中的配置为高优先级，覆盖全局设置。

- **num_of_records_per_req**：
  每次向 TDengine TSDB 请求写入的数据行数，默认值为 30000。当其设置过大时，TDengine TSDB 客户端驱动会返回相应的错误信息，此时需要调低这个参数的设置以满足写入要求。

- **prepare_rand**：生成的随机数据中唯一值的数量。若为 1 则表示所有数据都相同。默认值为 10000。

- **pre_load_tb_meta**：是否提前加载子表的 meta 数据，取值为“yes”or "no"。当子表数量非常多时，打开此选项可提高写入速度。

### 查询配置参数

查询场景下 `filetype` 必须设置为 `query`。

`query_mode`  查询连接方式，取值为：  

- “taosc”: 通过 Native  连接方式查询。  
- “rest” : 通过 restful 连接方式查询。  

`query_times` 指定运行查询的次数，数值类型。

其它通用参数详见 [通用配置参数](#通用配置参数)。

**说明：从 v3.3.5.6 及以上版本不再支持 json 文件中同时配置 `specified_table_query` 和  `super_table_query`**

#### 执行指定查询语句

查询指定表（可以指定超级表、子表或普通表）的配置参数在 `specified_table_query` 中设置。

- **mixed_query**：混合查询开关。  
  “yes”: 开启“混合查询”。
  “no” : 关闭“混合查询” ，即“普通查询”。  

  - 普通查询：

  `sqls` 中每个 sql 启动 `threads` 个线程查询此 sql, 执行完 `query_times` 次查询后退出，执行此 sql 的所有线程都完成后进入下一个 sql
  `查询总次数` = `sqls` 个数 *`query_times`* `threads`
  
  - 混合查询：

  `sqls` 中所有 sql 分成 `threads` 个组，每个线程执行一组，每个 sql 都需执行 `query_times` 次查询  
  `查询总次数` = `sqls` 个数 * `query_times`

- **batch_query**：批查询功开关。  
  取值范围“yes”表示开启，"no" 不开启，其它值报错。  
  批查询是指 `sqls` 中所有 sql 分成 `threads` 个组，每个线程执行一组，每个 sql 只执行一次查询后退出，主线程等待所有线程都执行完，再判断是否设置有 `query_interval` 参数，如果有需要 sleep 指定时间，再启动各线程组重复前面的过程，直到查询次数耗尽为止。  
  功能限制条件：  
  - 只支持 `mixed_query` 为 "yes" 的场景。  
  - 不支持 restful 查询，即 `query_mode` 不能为 "rest"。  

- **query_interval**：查询时间间隔，单位：millisecond，默认值为 0。
  "batch_query" 开关打开时，表示是每批查询完间隔时间；关闭时，表示每个 sql 查询完间隔时间
  如果执行查询的时间超过间隔时间，那么将不再等待，如果执行查询的时间不足间隔时间，需等待补足间隔时间

- **threads**：执行查询 SQL 的线程数，默认值为 1。

- **sqls**：
  - **sql**：执行的 SQL 命令，必填。
  - **result**：保存查询结果的文件，未指定则不保存。

#### 查询超级表

查询超级表的配置参数在 `super_table_query` 中设置。  
超级表查询的线程模式与上面介绍的指定查询语句查询的 `普通查询` 模式相同，不同之处是本 `sqls` 使用所有子表填充。

- **stblname**：指定要查询的超级表的名称，必填。

- **query_interval**：查询时间间隔，单位是秒，默认值为 0。

- **threads**：执行查询 SQL 的线程数，默认值为 1。

- **sqls**：
  - **sql**：执行的 SQL 命令，必填；对于超级表的查询 SQL，在 SQL 命令中必须保留 "xxxx"，会替换为超级下所有子表名后再执行。
  - **result**：保存查询结果的文件，未指定则不保存。
  - **限制项**：sqls 下配置 sql 数组最大为 100 个。

### 订阅配置参数

订阅场景下 `filetype` 必须设置为 `subscribe`，该参数及其它通用参数详见 [通用配置参数](#通用配置参数)

订阅配置参数在 `tmq_info` 项下设置，参数如下：

- **concurrent**：消费订阅的消费者数量，或称并发消费数量，默认值：1。
- **create_mode**：创建消费者模式，可取值 sequential：顺序创建，parallel：并发同时创建，必填项，无默认值。
- **group_mode**：生成消费者 groupId 模式，可取值 share：所有消费者只生成一个 groupId，independent：每个消费者生成一个独立的 groupId，如果 `group.id` 未设置，此项为必填项，无默认值。
- **poll_delay**：调用 tmq_consumer_poll 传入的轮询超时时间，单位为毫秒，负数表示默认超时 1 秒。
- **enable.manual.commit**：是否允许手动提交，可取值 true：允许手动提交，每次消费完消息后手动调用 tmq_commit_sync 完成提交，false：不进行提交，默认值：false。
- **rows_file**：存储消费数据的文件，可以为全路径或相对路径，带文件名。实际保存的文件会在后面加上消费者序号，如 rows_file 为 result，实际文件名为 result_1（消费者 1）result_2（消费者 2） ...
- **expect_rows**：期望每个消费者消费的行数，数据类型，当消费达到这个数，消费会退出，不设置会一直消费。
- **topic_list**：指定消费的 topic 列表，数组类型。topic 列表格式示例：`{"name": "topic1", "sql": "select * from test.meters;"}`，name：指定 topic 名，sql：指定创建 topic 的 sql 语句，需保证 sql 正确，框架会自动创建出 topic。

以下参数透传订阅属性，参见 [订阅创建参数](../../../develop/tmq/#创建参数) 说明：

- **client.id**
- **auto.offset.reset**
- **enable.manual.commit**
- **enable.auto.commit**
- **msg.with.table.name**
- **auto.commit.interval.ms**
- **group.id**：若此值不指定，将由 `group_mode` 指定规则生成 groupId，若指定此值，`group_mode` 参数不再有效。

### 数据类型对照表

| #   |   **TDengine TSDB**     | **taosBenchmark**
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
| 16  |  VARBINARY         |    varbinary
| 17  |  GEOMETRY          |    geometry
| 18  |  JSON              |    json
| 19  |  DECIMAL           |    decimal

注意：taosBenchmark 配置文件中数据类型必须小写方可识别。

## 配置文件示例

**下面为支持的写入、查询、订阅三大功能的配置文件示例：**

### 写入 JSON 示例

<details>
<summary>insert.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/insert.json}}
```

</details>

### 查询 JSON 示例

<details>
<summary>query.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/query.json}}
```

</details>

<details>
<summary>queryStb.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/queryStb.json}}
```

</details>

### 订阅 JSON 示例

<details>
<summary>tmq.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/tmq.json}}
```

</details>

### 生成 CSV 文件 JSON 示例

<details>
<summary>csv-export.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/csv-export.json}}
```

</details>

查看更多 json 配置文件示例可 [点击这里](https://github.com/taosdata/TDengine/tree/main/tools/taos-tools/example)

## 输出性能指标

#### 写入指标

写入结束后会在最后两行输出总体性能指标，格式如下：

``` bash
SUCC: Spent 8.527298 (real 8.117379) seconds to insert rows: 10000000 with 8 thread(s) into test 1172704.41 (real 1231924.74) records/second
SUCC: insert delay, min: 19.6780ms, avg: 64.9390ms, p90: 94.6900ms, p95: 105.1870ms, p99: 130.6660ms, max: 157.0830ms
```

第一行写入速度统计：

- Spent：写入总耗时，单位秒，从开始写入第一个数据开始计时到最后一条数据结束，这里表示共花了 8.527298 秒。
- real：写入总耗时（调用引擎），此耗时已抛去测试框架准备数据时间，纯统计在引擎调用上花费的时间，示例为 8.117379 秒，8.527298 - 8.117379 = 0.409919 秒则为测试框架准备数据消耗时间
- rows：写入总行数，为 1000 万条数据。
- threads：写入线程数，这里是 8 个线程同时写入。
- records/second 写入速度 = `写入总耗时`/ `写入总行数`，括号中 `real` 同前，表示纯引擎写入速度。
第二行单个写入延时统计：
- min：写入最小延时。
- avg：写入平时延时。
- p90：写入延时 p90 百分位上的延时数。
- p95：写入延时 p95 百分位上的延时数。
- p99：写入延时 p99 百分位上的延时数。
- max：写入最大延时。
通过此系列指标，可观察到写入请求延时分布情况。

#### 查询指标

查询性能测试主要输出查询请求速度 QPS 指标，输出格式如下：

``` bash
complete query with 3 threads and 10000 query delay avg:  0.002686s min:  0.001182s max:  0.012189s p90:  0.002977s p95:  0.003493s p99:  0.004645s SQL command: select ...
INFO: Spend 26.9530 second completed total queries: 30000, the QPS of all threads:   1113.049
```

- 第一行表示 3 个线程每个线程执行 10000 次查询及查询请求延时百分位分布情况，`SQL command` 为测试的查询语句。
- 第二行表示查询总耗时为 26.9653 秒，每秒查询率 (QPS) 为：1113.049 次/秒。
- 如果在查询中设置了 `continue_if_fail` 选项为 `yes`，在最后一行中会输出失败请求个数及错误率，格式 error + 失败请求个数 (错误率)。
- QPS   = 成功请求数量 / 花费时间 (单位秒)
- 错误率 = 失败请求数量 /（成功请求数量 + 失败请求数量）

#### 订阅指标

订阅性能测试主要输出消费者消费速度指标，输出格式如下：

``` bash
INFO: consumer id 0 has poll total msgs: 376, period rate: 37.592 msgs/s, total rows: 3760000, period rate: 375924.815 rows/s
INFO: consumer id 1 has poll total msgs: 362, period rate: 36.131 msgs/s, total rows: 3620000, period rate: 361313.504 rows/s
INFO: consumer id 2 has poll total msgs: 364, period rate: 36.378 msgs/s, total rows: 3640000, period rate: 363781.731 rows/s
INFO: consumerId: 0, consume msgs: 1000, consume rows: 10000000
INFO: consumerId: 1, consume msgs: 1000, consume rows: 10000000
INFO: consumerId: 2, consume msgs: 1000, consume rows: 10000000
INFO: Consumed total msgs: 3000, total rows: 30000000
```

- 1 ~ 3 行实时输出每个消费者当前的消费速度，`msgs/s` 表示消费消息个数，每个消息中包含多行数据，`rows/s` 表示按行数统计的消费速度。
- 4 ~ 6 行是测试完成后每个消费者总体统计，统计共消费了多少条消息，共计多少行。
- 第 7 行所有消费者总体统计，`msgs` 表示共消费了多少条消息，`rows` 表示共消费了多少行数据。
