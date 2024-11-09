---
title: taosBenchmark
sidebar_label: taosBenchmark
toc_max_heading_level: 4
description: 'taosBenchmark (曾用名 taosdemo ) 是一个用于测试 TDengine 产品性能的工具'
---

## 简介

taosBenchmark (曾用名 taosdemo ) 是一个用于测试 TDengine 产品性能的工具。taosBenchmark 可以测试 TDengine 的插入、查询和订阅等功能的性能，它可以模拟由大量设备产生的大量数据，还可以灵活地控制数据库、超级表、标签列的数量和类型、数据列的数量和类型、子表的数量、每张子表的数据量、插入数据的时间间隔、taosBenchmark 的工作线程数量、是否以及如何插入乱序数据等。为了兼容过往用户的使用习惯，安装包提供 了 taosdemo 作为 taosBenchmark 的软链接。

:::note 非常重要
在使用 TDengine Cloud 的时候，请注意，没有授权的用户是没有办法通过任何工具包括 taosBenchmark 来创建数据库的。只能通过 TDengine Cloud 的数据浏览器来创建数据库。这个文档中提到的任何创建数据库的内容请忽略，并在 TDengine Cloud 里面手动创建数据库。
:::

## 安装

taosBenchmark 有两种安装方式:

- 安装 TDengine 官方[TDengine 安装包](https://docs.taosdata.com/releases/tdengine/)的同时会自动安装 taosBenchmark

- 单独编译 taos-tools 并安装, 详情请参考 [taos-tools](https://github.com/taosdata/taos-tools) 仓库。

## 运行

### 配置和运行方式

运行下面命令来设置 TDengine Cloud 的 DSN 环境变量：

```bash
export TDENGINE_CLOUD_DSN="<DSN>"
```

<!-- exclude -->
:::note IMPORTANT
获取真实的 `DSN` 的值，请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”工具“菜单，然后选择”taosBenchmark“。
:::
<!-- exclude-end -->

用户只能使用一个命令行参数 `-f <json file>` 指定配置文件。

taosBenchmark 支持对 TDengine 做完备的性能测试，其所支持的 TDengine 功能分为三大类：写入、查询和订阅。这三种功能之间是互斥的，每次运行 taosBenchmark 只能选择其中之一。值得注意的是，所要测试的功能类型在使用命令行配置方式时是不可配置的，命令行配置方式只能测试写入性能。若要测试 TDengine 的查询和订阅性能，必须使用配置文件的方式，通过配置文件中的参数 `filetype` 指定所要测试的功能类型。

**在运行 taosBenchmark 之前要确保 TDengine 集群已经在正确运行。**

### 使用配置文件运行

taosBenchmark 安装包中提供了配置文件的示例，位于 `<install_directory>/examples/taosbenchmark-json` 下

使用如下命令行即可运行 taosBenchmark 并通过配置文件控制其行为。

```bash
taosBenchmark -f <json file>
```

**下面是几个配置文件的示例：**

#### 插入场景 JSON 配置文件示例
```json
{
	"filetype": "insert",
	"cfgdir": "/etc/taos",
	"connection_pool_size": 8,
	"thread_count": 4,
	"create_table_thread_count": 7,
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
				"name": "test",
				"drop": "no",
				"replica": 1,
				"precision": "ms",
				"keep": 3650,
				"minRows": 100,
				"maxRows": 4096,
				"comp": 2
			},
			"super_tables": [
				{
					"name": "meters",
					"child_table_exists": "no",
					"childtable_count": 10000,
					"childtable_prefix": "d",
					"escape_character": "yes",
					"auto_create_table": "no",
					"batch_create_tbl_num": 5,
					"data_source": "rand",
					"insert_mode": "taosc",
					"non_stop_mode": "no",
					"line_protocol": "line",
					"insert_rows": 10000,
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
						{
							"type": "FLOAT",
							"name": "current",
							"count": 1,
							"max": 12,
							"min": 8
						},
						{ "type": "INT", "name": "voltage", "max": 225, "min": 215 },
						{ "type": "FLOAT", "name": "phase", "max": 1, "min": 0 }
					],
					"tags": [
						{
							"type": "TINYINT",
							"name": "groupid",
							"max": 10,
							"min": 1
						},
						{
							"name": "location",
							"type": "BINARY",
							"len": 16,
							"values": ["San Francisco", "Los Angles", "San Diego",
								"San Jose", "Palo Alto", "Campbell", "Mountain View",
								"Sunnyvale", "Santa Clara", "Cupertino"]
						}
					]
				}
			]
		}
	]
}

```

<!-- #### 查询场景 JSON 配置文件示例

```json
{
	"filetype": "query",
	"cfgdir": "/etc/taos",
	"host": "127.0.0.1",
	"port": 6030,
	"user": "root",
	"password": "taosdata",
	"confirm_parameter_prompt": "no",
	"databases": "test",
	"query_times": 2,
	"query_mode": "taosc",
	"specified_table_query": {
		"query_interval": 1,
		"concurrent": 3,
		"sqls": [
			{
				"sql": "select last_row(*) from meters",
				"result": "./query_res0.txt"
			},
			{
				"sql": "select count(*) from d0",
				"result": "./query_res1.txt"
			}
		]
	},
	"super_table_query": {
		"stblname": "meters",
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

``` -->

## 配置文件参数详解

### 通用配置参数

本节所列参数适用于所有功能模式。

- **filetype** ：要测试的功能，可选值为 `insert`, `query` 和 `subscribe`。分别对应插入、查询和订阅功能。每个配置文件中只能指定其中之一。
- **cfgdir** ：TDengine 客户端配置文件所在的目录，默认路径是 /etc/taos 。

<!-- - **host** ：指定要连接的 TDengine 服务端的 FQDN，默认值为 localhost。

- **port** ：要连接的 TDengine 服务器的端口号，默认值为 6030。

- **user** ：用于连接 TDengine 服务端的用户名，默认为 root。

- **password** ：用于连接 TDengine 服务端的密码，默认值为 taosdata。 -->

### 插入场景配置参数

插入场景下 `filetype` 必须设置为 `insert`，该参数及其它通用参数详见[通用配置参数](#通用配置参数)

- ** keep_trying ** ：失败后进行重试的次数，默认不重试。需使用 v3.0.9 以上版本。

- ** trying_interval ** ：失败重试间隔时间，单位为毫秒，仅在 keep_trying 指定重试后有效。需使用 v3.0.9 以上版本。

#### 流式计算相关配置参数

创建流式计算的相关参数在 json 配置文件中的 `stream` 中配置，具体参数如下。

- **stream_name** ：流式计算的名称，必填项。

- **stream_stb** ：流式计算对应的超级表名称，必填项。

- **stream_sql** ：流式计算的sql语句，必填项。

- **trigger_mode** ：流式计算的触发模式，可选项。

- **watermark** ：流式计算的水印，可选项。

- **drop** ：是否创建流式计算，可选项为 "yes" 或者 "no", 为 "no" 时不创建。

#### 超级表相关配置参数

创建超级表时的相关参数在 json 配置文件中的 `super_tables` 中配置，具体参数如下。

- **name**：超级表名，必须配置，没有默认值。

- **child_table_exists** ：子表是否已经存在，默认值为 "no"，可选值为 "yes" 或 "no"。

- **child_table_count** ：子表的数量，默认值为 10。

- **child_table_prefix** ：子表名称的前缀，必选配置项，没有默认值。

- **escape_character** ：超级表和子表名称中是否包含转义字符，默认值为 "no"，可选值为 "yes" 或 "no"。

- **auto_create_table** ：仅当 insert_mode 为 taosc, rest, stmt 并且 childtable_exists 为 "no" 时生效，该参数为 "yes" 表示 taosBenchmark 在插入数据时会自动创建不存在的表；为 "no" 则表示先提前建好所有表再进行插入。

- **batch_create_tbl_num** ：创建子表时每批次的建表数量，默认为 10。注：实际的批数不一定与该值相同，当执行的 SQL 语句大于支持的最大长度时，会自动截断再执行，继续创建。

- **data_source** ：数据的来源，默认为 taosBenchmark 随机产生，可以配置为 "rand" 和 "sample"。为 "sample" 时使用 sample_file 参数指定的文件内的数据。

- **insert_mode** ：插入模式，可选项有 taosc, rest, stmt, sml, sml-rest, 分别对应普通写入、restful 接口写入、参数绑定接口写入、schemaless 接口写入、restful schemaless 接口写入 (由 taosAdapter 提供)。默认值为 taosc 。

- **non_stop_mode** ：指定是否持续写入，若为 "yes" 则 insert_rows 失效，直到 Ctrl + C 停止程序，写入才会停止。默认值为 "no"，即写入指定数量的记录后停止。注：即使在持续写入模式下 insert_rows 失效，但其也必须被配置为一个非零正整数。

- **line_protocol** ：使用行协议插入数据，仅当 insert_mode 为 sml 或 sml-rest 时生效，可选项为 line, telnet, json。

- **tcp_transfer** ：telnet 模式下的通信协议，仅当 insert_mode 为 sml-rest 并且 line_protocol 为 telnet 时生效。如果不配置，则默认为 http 协议。

- **insert_rows** ：每个子表插入的记录数，默认为 0 。

- **childtable_offset** ：仅当 childtable_exists 为 yes 时生效，指定从超级表获取子表列表时的偏移量，即从第几个子表开始。

- **childtable_limit** ：仅当 childtable_exists 为 yes 时生效，指定从超级表获取子表列表的上限。

- **interlace_rows** ：启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0， 即向一张子表完成数据插入后才会向下一张子表进行数据插入。

- **insert_interval** ：指定交错插入模式的插入间隔，单位为 ms，默认值为 0。 只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。

- **partial_col_num** ：若该值为正数 n 时， 则仅向前 n 列写入，仅当 insert_mode 为 taosc 和 rest 时生效，如果 n 为 0 则是向全部列写入。

- **disorder_ratio** ：指定乱序数据的百分比概率，其值域为 [0,50]。默认为 0，即没有乱序数据。

- **disorder_range** ：指定乱序数据的时间戳回退范围。所生成的乱序时间戳为非乱序情况下应该使用的时间戳减去这个范围内的一个随机值。仅在 `-O/--disorder` 指定的乱序数据百分比大于 0 时有效。

- **timestamp_step** ：每个子表中插入数据的时间戳步长，单位与数据库的 `precision` 一致，默认值是 1。

- **start_timestamp** ：每个子表的时间戳起始值，默认值是 now。

- **sample_format** ：样本数据文件的类型，现在只支持 "csv" 。

- **sample_file** ：指定 csv 格式的文件作为数据源，仅当 data_source 为 sample 时生效。若 csv 文件内的数据行数小于等于 prepared_rand，那么会循环读取 csv 文件数据直到与 prepared_rand 相同；否则则会只读取 prepared_rand 个数的行的数据。也即最终生成的数据行数为二者取小。

- **use_sample_ts** ：仅当 data_source 为 sample 时生效，表示 sample_file 指定的 csv 文件内是否包含第一列时间戳，默认为 no。 若设置为 yes， 则使用 csv 文件第一列作为时间戳，由于同一子表时间戳不能重复，生成的数据量取决于 csv 文件内的数据行数相同，此时 insert_rows 失效。

- **tags_file** ：仅当 insert_mode 为 taosc, rest 的模式下生效。 最终的 tag 的数值与 childtable_count 有关，如果 csv 文件内的 tag 数据行小于给定的子表数量，那么会循环读取 csv 文件数据直到生成 childtable_count 指定的子表数量；否则则只会读取 childtable_count 行 tag 数据。也即最终生成的子表数量为二者取小。

#### tsma配置参数

指定tsma的配置参数在 `super_tables` 中的 `tsmas` 中，具体参数如下。

- **name** ：指定 tsma 的名字，必选项。

- **function** ：指定 tsma 的函数，必选项。

- **interval** ：指定 tsma 的时间间隔，必选项。

- **sliding** ：指定 tsma 的窗口时间位移，必选项。

- **custom** ：指定 tsma 的创建语句结尾追加的自定义配置，可选项。

- **start_when_inserted** ：指定当插入多少行时创建 tsma，可选项，默认为 0。

#### 标签列与数据列配置参数

指定超级表标签列与数据列的配置参数分别在 `super_tables` 中的 `columns` 和 `tag` 中。

- **type** ：指定列类型，可选值请参考 TDengine 支持的数据类型。
  注：JSON 数据类型比较特殊，只能用于标签，当使用 JSON 类型作为 tag 时有且只能有这一个标签，此时 count 和 len 代表的意义分别是 JSON tag 内的 key-value pair 的个数和每个 KV pair 的 value 的值的长度，value 默认为 string。

- **len** ：指定该数据类型的长度，对 NCHAR，BINARY 和 JSON 数据类型有效。如果对其他数据类型配置了该参数，若为 0 ， 则代表该列始终都是以 null 值写入；如果不为 0 则被忽略。

- **count** ：指定该类型列连续出现的数量，例如 "count"：4096 即可生成 4096 个指定类型的列。

- **name** ：列的名字，若与 count 同时使用，比如 "name"："current", "count":3, 则 3 个列的名字分别为 current, current_2. current_3。

- **min** ：数据类型的 列/标签 的最小值。生成的值将大于或等于最小值。

- **max** ：数据类型的 列/标签 的最大值。生成的值将小于最小值。

- **values** ：nchar/binary 列/标签的值域，将从值中随机选择。

- **sma**：将该列加入 SMA 中，值为 "yes" 或者 "no"，默认为 "no"。

#### 插入行为配置参数

- **thread_count** ：插入数据的线程数量，默认为 8。

- **create_table_thread_count** ：建表的线程数量，默认为 8。

- **connection_pool_size** ：预先建立的与 TDengine 服务端之间的连接的数量。若不配置，则与所指定的线程数相同。

- **result_file** ：结果输出文件的路径，默认值为 ./output.txt。

- **confirm_parameter_prompt** ：开关参数，要求用户在提示后确认才能继续。默认值为 false 。

- **interlace_rows** ：启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0， 即向一张子表完成数据插入后才会向下一张子表进行数据插入。
  在 `super_tables` 中也可以配置该参数，若配置则以 `super_tables` 中的配置为高优先级，覆盖全局设置。

- **insert_interval** ：指定交错插入模式的插入间隔，单位为 ms，默认值为 0。 只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。
  在 `super_tables` 中也可以配置该参数，若配置则以 `super_tables` 中的配置为高优先级，覆盖全局设置。

- **num_of_records_per_req** ：每次向 TDengine 请求写入的数据行数，默认值为 30000 。当其设置过大时，TDengine 客户端驱动会返回相应的错误信息，此时需要调低这个参数的设置以满足写入要求。

- **prepare_rand** ：生成的随机数据中唯一值的数量。若为 1 则表示所有数据都相同。默认值为 10000 。

### 查询场景配置参数

查询场景下 `filetype` 必须设置为 `query`。

查询场景可以通过设置 `kill_slow_query_threshold` 和 `kill_slow_query_interval` 参数来控制杀掉慢查询语句的执行，threshold 控制如果 exec_usec 超过指定时间的查询将被 taosBenchmark 杀掉，单位为秒；interval 控制休眠时间，避免持续查询慢查询消耗 CPU ，单位为秒。

其它通用参数详见[通用配置参数](#通用配置参数)。

#### 执行指定查询语句的配置参数

查询子表或者普通表的配置参数在 `specified_table_query` 中设置。

- **query_interval** ：查询时间间隔，单位是秒，默认值为 0。

- **threads** ：执行查询 SQL 的线程数，默认值为 1。

- **sql**：执行的 SQL 命令，必填。

- **result**：保存查询结果的文件，未指定则不保存。

#### 查询超级表的配置参数

查询超级表的配置参数在 `super_table_query` 中设置。

- **stblname** ：指定要查询的超级表的名称，必填。

- **query_interval** ：查询时间间隔，单位是秒，默认值为 0。

- **threads** ：执行查询 SQL 的线程数，默认值为 1。

- **sql** ：执行的 SQL 命令，必填；对于超级表的查询 SQL，在 SQL 命令中保留 "xxxx"，程序会自动将其替换为超级表的所有子表名。
    替换为超级表中所有的子表名。
		
- **result** ：保存查询结果的文件，未指定则不保存。

### 订阅场景配置参数

订阅场景下 `filetype` 必须设置为 `subscribe`，该参数及其它通用参数详见[通用配置参数](#通用配置参数)

#### 执行指定订阅语句的配置参数

订阅子表或者普通表的配置参数在 `specified_table_query` 中设置。

- **threads/concurrent** ：执行 SQL 的线程数，默认为 1。

- **sqls** ：
  - **sql** ：执行的 SQL 命令，必填。
