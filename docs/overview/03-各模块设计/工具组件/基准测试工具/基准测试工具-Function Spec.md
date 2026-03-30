# 基准测试工具-Function Spec

## 1. 修订记录

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/01/16 | 0.1 | 段宽军 | 创建 |
| 2025/01/17 | 1.0 | 段宽军 | 内容格式完善 |
| 2026/01/09 | 1.1 | 佘彦杰 | 根据新需求修改，去除 rest 支持 |

## 2. 背景

TDengine 3.0 产品高性能是非常重要一个指标，为衡量产品性能达到设计预期，需开发一款性能基准测试工具进行验证。
工具能够对 TDengine 的 SQL 写入、参数绑定写入及无模式写入等多种写入方式进行性能测试、同时希望能够支持对超级表的子表并发查询、指定 SQL 查询的多种查询方式性能测试，另外也需要对订阅功能有简单的性能测试。

## 3. 定义

**WebSocket **：是一种在单个TCP连接上进行全双工通信的协议。它基于TCP协议，通过HTTP/1.1协议的101状态码进行握手，从而建立连接。WebSocket允许服务器和客户端之间进行实时双向通信，客户端和服务器之间的数据交换变得更加简单和高效。
**RESTFUL **:**  **指一种基于 HTTP 协议的 web 服务架构风格，也被称为 RESTful 架构。REST（Representational State Transfer）是一种用于设计网络应用程序的架构风格，它使用客户端和服务器之间的 HTTP 协议进行通信。
**SQL  写入**:  指把多个类似 insert into ... 语法拼写在一起，形成一个大的 SQL 语句的方式
**Native  写入**:  指直接调用引擎提供的 libtaos.so 客户端库接口的行为
**SML 写入**: Schemaless 无模式写入，一种使用极为方便无需提前建表的自动建表写入方式

## 4. 行为说明

### 4.1 连接方式

| 需求编号 | **功能名称** | 使用方法 |
| --- | --- | --- |
| 1.1 | 选择连接方式 |  |
|  |  | **命令行方式：** 命令行：-Z/--connect-mode <NUMBER> 指定连接方式，0 表示采用原生连接方式，1 表示采用 WebSocket 连接方式，默认采用原生连接方式 **示例：** taosBenchmark -Z 0 -h 127.0.0.1 -P 6030 -t 100 -n 10000 表示向主机： 127.0.0.1 端口：6030 ，使用 Native 方式写入子表 100 个，每表 10000 行，SQL 写入，使用默认数据库及超级表名 taosBenchmark -Z 1 -h 127.0.0.1 -P 6041 -t 100 -n 10000 表示向主机： 127.0.0.1 端口：6041 ，使用 WebSocket 方式写入子表 100 个，每表 10000 行，SQL 写入，使用默认数据库及超级表名 |

### 4.2 写入

运行方式 ：taosBenchmark  命令行 或  taosBenchmark -f  json 写入配置文件

| 需求编号 | **功能名称** | 使用方法 |
| --- | --- | --- |
| 2.1 | SQL 写入 |  |
|  |  | **命令行方式：** 命令行：-I, --interface=IFACE 参数：taosc **示例：** taosBenchmark -h 127.0.0.1 -P 6030 -t 100 -n 10000 -I taosc 表示原生连接方式向主机： 127.0.0.1 端口：6030 写入子表 100 个，每表 10000 行，SQL 方式写入，使用默认数据库及超级表名 |
|  |  | **配置文件方式：** filetype 配置为 “insert” 写入类型 databases->super_tables -> insert_mode 配置为 "taosc" ```json {wrap} { "filetype": "insert", "host": "127.0.0.1", "port": 6030, ... "databases": [ { "dbinfo": { "name": "test", ... }, "super_tables": [{ "name": "meters", "childtable_count": 100, "insert_rows": 10000, "insert_mode": "taosc", ... }] }] } ``` |
| 2.2 | STMT 写入 |  |
|  |  | **命令行方式：** 命令行：-I, --interface=IFACE 参数：stmt **示例：** taosBenchmark -h 127.0.0.1 -P 6030 -t 100 -n 10000 -I stmt 表示原生连接方式向主机： 127.0.0.1 端口：6030 写入子表 100 个，每表 10000 行，STMT 方式写入，使用默认数据库及超级表名 |
|  |  | **配置文件方式：** filetype 配置为 “insert” 写入类型 databases->super_tables -> insert_mode 配置为 "stmt" ```json {wrap} { "filetype": "insert", "host": "127.0.0.1", "port": 6030, ... "databases": [ { "dbinfo": { "name": "test", ... }, "super_tables": [{ "name": "meters", "insert_mode": "stmt", ... }] }] } ``` |
| 2.3 | STMT2 写入 |  |
|  |  | **命令行方式：** 命令行：-I, --interface=IFACE 参数：stmt2 **示例：** taosBenchmark -h 127.0.0.1 -P 6030 -t 100 -n 10000 -I stmt2 表示向主机： 127.0.0.1 端口：6030 写入子表 100 个，每表 10000 行，STMT2 方式写入，使用默认数据库及超级表名 |
|  |  | **配置文件方式：** filetype 配置为 “insert” 写入类型 databases->super_tables -> insert_mode 配置为 "stmt2" ```json {wrap} { "filetype": "insert", "host": "127.0.0.1", "port": 6030, ... "databases": [ { "dbinfo": { "name": "test", ... }, "super_tables": [{ "name": "meters", "insert_mode": "stmt2", ... }] }] } ``` |
| 2.4 | sml(line) 写入 |  |
|  |  | **命令行方式：不支持** |
|  |  | **配置文件方式：** filetype 配置为 “insert” 写入类型 databases->super_tables -> insert_mode 配置为 "sml" databases->super_tables -> line_protocol 配置为 "line" ```json {wrap} { "filetype": "insert", "host": "127.0.0.1", "port": 6030, ... "databases": [ { "dbinfo": { "name": "test", ... }, "super_tables": [{ "name": "meters", "insert_mode": "sml", "line_protocol": "line", ... }] }] } ``` |
| 2.5 | sml(json) 写入 |  |
|  |  | **命令行方式：不支持** |
|  |  | **配置文件方式：** filetype 配置为 “insert” 写入类型 databases->super_tables -> insert_mode 配置为 "sml" databases->super_tables -> line_protocol 配置为 "json" ```json {wrap} { "filetype": "insert", "host": "127.0.0.1", "port": 6030, ... "databases": [ { "dbinfo": { "name": "test", ... }, "super_tables": [{ "name": "meters", "insert_mode": "sml", "line_protocol": "json", ... }] }] } ``` |
| 2.6 | sml(telnet) 写入 |  |
|  |  | **命令行方式：不支持** |
|  |  | **配置文件方式：** filetype 配置为 “insert” 写入类型 databases->super_tables -> insert_mode 配置为 "sml" databases->super_tables -> line_protocol 配置为 "telnet" ```json {wrap} { "filetype": "insert", "host": "127.0.0.1", "port": 6030, ... "databases": [ { "dbinfo": { "name": "test", ... }, "super_tables": [{ "name": "meters", "insert_mode": "sml", "line_protocol": "telnet", ... }] }] } ``` |
| 2.7 | 写入模式：批写入 |  |
|  |  | **命令行方式：** 默认为批写入，可通过以下命令行设置批相关参数： 命令行： -r, --rec-per-req=NUMBER 指定批写入大小，默认大小为 30000 **示例：** taosBenchmark -h 127.0.0.1 -P 6030 -t 100 -n 10000 -r 2000 表示向主机： 127.0.0.1 端口：6030 写入子表 100 个，每表 10000 行，每批写入 2000 行，使用默认数据库及超级表名 |
|  |  | **配置文件方式：** 默认为批写入 filetype 配置为 “insert” 写入类型 num_of_records_per_req 可配置批写大小属性，此处配置为 2000 databases->super_tables -> interlace_rows 配置为 0，不配置默认也为 0 ```json {wrap} { "filetype": "insert", "host": "127.0.0.1", "port": 6030, "num_of_records_per_req": 2000, ... "databases": [ { "dbinfo": { "name": "test", ... }, "super_tables": [{ "name": "meters", "childtable_count": 100, "insert_rows": 10000, "interlace_rows": 0, ... }] }] } ``` |
| 2.8 | 写入模式分类：交叉(interlace)写入 |  |
|  |  | **命令行方式：** 默认为批写入，交叉写入需使用以上命令行： 命令行：-B, --interlace-rows=NUMBER 参数 number： 指定交叉写入行数 **示例：** taosBenchmark -h 127.0.0.1 -P 6030 -t 100 -n 10000 -B 1 表示向主机： 127.0.0.1 端口：6030 写入子表 100 个，每表 10000 行，交叉写入行数为 1 行，使用默认数据库及超级表名 |
|  |  | **配置文件方式：** 默认为批写入 filetype 配置为 “insert” 写入类型 databases->super_tables -> interlace_rows 配置为 1，表示交叉写入每次 1 行 ```json {wrap} { "filetype": "insert", "host": "127.0.0.1", "port": 6030, ... "databases": [ { "dbinfo": { "name": "test", ... }, "super_tables": [{ "name": "meters", "childtable_count": 100, "insert_rows": 10000, "interlace_rows": 1, ... }] }] } ``` |
| 2.9 | 写入耗时 |  |
|  |  | 1. 是性能指标 分两个，一个为写入总耗时，另一个为引擎耗时（带 real 标识） 总耗时 = 引擎耗时 + 测试框架耗时 1. 此指标在完成时输出 1. 输出格式及示例： SUCC: Spent 0.38 (real 0.29) seconds to insert rows ... 0.38 秒为总耗时， 0.29 秒为引擎上耗时，可推出 0.9 秒为测试框架耗时 |
| 2.10 | 写入数据量 |  |
|  |  | 1. 是性能指标 表示写入总数据量，单位为行 1. 此指标在完成时输出 1. 输出格式及示例： SUCC: Spent 0.38 (real 0.29) seconds to insert rows: 1000000 with ... 表示本次测试共向测试数据库写入 1000000 行数据 |
| 2.11 | 写入吞吐量 |  |
|  |  | 1. 是性能指标 指每秒写入的数据总行数，单位为 rows/s 分两个，一个根据写入总耗时计算得到的吞吐量，另一个根据引擎耗时（带 real 标识）计算得到的实际引擎吞吐量 1. 此指标在完成时输出 1. 输出格式及示例： SUCC: Spent 0.38 (real 0.29) seconds to insert rows: 1000000 with 8 thread(s) into test 2630789 (real 3384369) records/second 表示本次测试总吞吐量为： 2630789 行/秒 计算引擎实际的吞吐量为： 3384369 行/秒 |
| 2.12 | 写入请求延时 | 1. 是性能指标 写入请求的 min max avg p90 p95 p99 分布情况， 单位为毫秒 作用是协助开发人员分析写入延时的性能问题 1. 此指标在完成时输出 1. 输出格式及示例： SUCC: insert delay, min: 20.6630ms, avg: 23.6381ms, p90: 30.4230ms, p95: 34.2920ms, p99: 36.6840ms, max: 36.6840ms 多个指标在最后一行集中输出，方便对比查看 |

### 4.3 查询

运行方式 ： taosBenchmark -f  json 查询配置文件  

| 需求编号 | **功能名称** | 使用方法 |
| --- | --- | --- |
| 3.1 | 超级表查询 |  |
|  |  | **通用配置参数：** filetype : "query" 必填项，表示是查询性能测试 host : 连接 TDengine 主机名 port : 连接 TDengine 的端口号 user: 连接 TDengine 用户名 password: 连接 TDengine 密码 databases: 要查询的数据库，指定后在 sqls 中可省略数据库名 query_times： 每个 SQL 查询次数, 为 int32 数据类型 **超级表查询参数：** stblname：查询超级表名 threads： 查询并发线程数 sqls: 要测试的 SQL 语句，SQL 中的 xxxx 在执行时将被依次替换为具体子表执行 result: 输出查询结果到文件【可选】，如果没有查询拉回数据到内存后丢弃 |
|  |  | 示例： ```json {wrap} { "filetype": "query", "host": "127.0.0.1", "port": 6030, "user": "root", "password": "taosdata", "databases": "test", "query_times": 10, "super_table_query": { "stblname": "meters", "threads": 3, "sqls": [ { "sql": "select last_row(ts) from xxxx", "result": "./query_res.txt" } ] } } ``` 表示对数据库 test 下的超级表 meters 下所有子表执行查询压力测试，threads = 3 表示所有子表平分给 3 个线程完成，每个查询重复执行 10 次 |
| 3.2 | 自定义 SQL 查询 | taosBenchmark -f json 配置文件 配置文件如下： |
|  |  | **通用配置参数：** 见 2.1 需求，略 **给定 SQL 查询参数：** threads： 查询并发线程数 mixed_query: 查询模式，取值 “yes” 为`混合查询`， "no" 为`正常查询` , 默认值为 “no” `**混合查询**`：`sqls` 中所有 sql 按 `threads` 线程数分组，每个线程执行一组， 线程中每个 sql 都需执行 `query_times` 次查询 `**普通查询**`：`sqls` 中每个 sql 启动 `threads` 个线程，每个线程执行完 `query_times` 次后退出，下个 sql 需等待上个 sql 线程全部执行完退出后方可执行 查询总次数（混合查询） = `sqls` 个数 * `query_times` 查询总次数（普通查询） = `sqls` 个数 * `query_times` * `threads` ` 普通查询` 每个 sql 都会启动 `threads` 个线程 `混合查询` 只启动一次 `threads` 线程执行完所有 SQL, 两者启动线程次数不一样。 sqls: 要测试的 SQL 语句，SQL 中的 xxxx 在执行时将被依次替换为具体子表执行 result: 输出查询结果到文件【可选】，如果没有查询拉回数据到内存后丢弃 |
|  |  | 示例： ```json {wrap} { "filetype": "query", "host": "127.0.0.1", "port": 6030, "user": "root", "password": "taosdata", "databases": "test", "query_times": 10, "specified_table_query": { "threads": 3, "sqls": [ { "sql": "select last_row(*) from meters", "result": "./query_res0.txt" }, { "sql": "select count(*) from d0", "result": "./query_res1.txt" } ] } } ``` 表示对数据库 test 下的两条SQL 进行压力测试，启动 3 个并发，每个 SQL 执行 10 次查询 |
| 3.3 | 总查询耗时 |  |
|  |  | 1. 是性能指标 表示从查询线程启动至最后一个查询线程结束的耗时统计，统计单位为秒 1. 在查询完成时输出 1. 输出格式及示例： INFO: Spend 1.688 second completed ... |
| 3.4 | 总查询次数 |  |
|  |  | 1. 是性能指标 表示本次查询测试总计查询次数，单位为次，计算公式为： 总查询次数 = sqls 个数 * threads * query_times 1. 在查询完成时输出 1. 输出格式及示例： INFO: Total specified queries: 60 |
| 3.5 | 总 QPS |  |
|  |  | 1. 是性能指标 表示本次查询测试每秒查询请求次数，单位为次/秒，计算公式为： 总 QPS = 总查询次数 / 总查询耗时 1. 在查询完成时输出 1. 输出格式及示例： Spend 1.688 second completed total queries: 60, the QPS of all threads: 35.545 |
| 3.6 | 查询延时分布 |  |
|  |  | 1. 是性能指标 统计查询请求延时分布情况，协助开发人员定位查询性能不佳原因 1. 在每个查询线程结果时输出 1. 输出格式及示例 complete query with 3 threads and 10 query delay avg: 0.266733s min: 0.179818s max: 0.298656s p90: 0.294558s p95: 0.298475s p99: 0.298656s SQL command: select last_row(*) from meters |

<callout emoji="bell" background-color="light-orange" border-color="light-orange">
查询类仅支持 json 配置文件方式，不支持命令行模式
</callout>


### 4.4 订阅

运行方式 ： taosBenchmark -f  json 订阅配置文件  

| 需求编号 | **功能名称** | 使用方法 |
| --- | --- | --- |
| 4.1 | 设置订阅 | "filetype" 属性设置为 "subscribe", 表示本性能测试为订阅类型，字符串类型 |
| 4.2 | 订阅 HOST | 通过配置 "host" 属性实现, 可以为 ip 地址或主机 hostname ，字符串类型 |
| 4.3 | 订阅 PORT | 通过配置 "port" 属性实现，数值类型 |
| 4.4 | 用户名 | 通过配置 "user" 属性实现，字符串类型 |
| 4.5 | 密码 | 通过配置 "password" 属性实现，字符串类型 |
| 4.6 | 消费线程数 | 通过配置 "tmq_info->concurrent" 属性实现，数值类型 |
| 4.7 | 消费组 ID | 通过配置 "tmq_info->group.id" 属性实现，字符串类型 |
| 4.8 | 消费组模式 | 通过配置 "tmq_info->group_mode" 属性实现，字符串类型 取值： "share" : 共享模式（默认），表示不同线程使用相同的 groupId 去消费相同 topic "independent": 独立模式，表示不同线程使用不同的 groupId 去消费相同 topic |
| 4.9 | 创建模式 | 通过配置 "tmq_info->create_mode" 属性实现字符串类型 取值： "sequential": 表示串行连续创建消费者，在发起消费线程之前串行创建好消费者 "parallel": 表示并行创建消费者，即在消费线程中并发创建消费者 此值必须要配置一个值，没有默认值 |
| 4.10 | 消息最大延时 | 通过配置 "tmq_info->poll_delay" 属性实现，数值类型，单位为毫秒 此参数做为第二个参数传递给获取消息的 API tmq_consumer_poll ，表示等待消息的最大超时时间 |
| 411 | 客户端ID | 通过配置 "tmq_info->client.id" 属性实现，字符串类型 |
| 4.12 | 自动提交 | 通过配置 "tmq_info->enable.auto.commit" 属性实现，BOOL 类型, 取值 "true" or "false" |
| 4.13 | 订阅 TOPIC 列表 | 通过配置 "tmq_info->topic_list" 数组可配置多个 订阅的 topic , 属性： "name": 表示创建 topic 的名称，符合 TDengine 命名规范字符串都可 "sql": 表示创建 topic 的 sql 语句， topic 以此 sql 语句按默认选项创建出来 |
|  |  | 示例： ```json {wrap} { "filetype": "subscribe", "host": "127.0.0.1", "port": 6030, "user": "root", "password": "taosdata", "result_file": "tmq_res.txt", "tmq_info": { "concurrent": 3, "poll_delay": 10000, "group.id": "group001", "group_mode": "independent", "create_mode": "parallel", "client.id": "client001", "enable.auto.commit": "false", "auto.offset.reset": "earliest", "topic_list": [ { "name": "topic1", "sql": "select * from test.meters;" } ] } } ``` 示例连接订阅的 TDengine 数据库 IP 为 127.0.0.1 , 端口号 6030, 连接用户名 root, 连接密码为 taosdata, 创建订阅一个主题 topic1 ，使用 sql 为 "select * from test.meters;" 消费者组 ID 为 group001, 使用独立消费方式创建，客户端ID 为 client001 使用三个线程并发消费，消费等待消息的最大超时为 10000 毫秒，不允许自动提交, 独立消费方式进行消息消费， 消费消息内容放到当前目录下 tmq_res.txt 中保存 |
| 4.14 | 线程消费消息块个数 | 1. 线程性能指标 1. 在线程结束时输出 1. 输出格式及示例 INFO: consumerId: 1, consume msgs: 45, ... |
| 4.15 | 线程消费消息行数 | 1. 线程性能指标 1. 在线程结束时输出 1. 输出格式及示例 INFO: consumerId: 1, consume msgs: 45, consume rows: 450000 |
| 4.16 | 整体消费消息块个数 | 1. 整体性能指标 1. 在测试结束时输出 1. 输出格式及示例 INFO: Consumed total msgs: 100, ... |
| 4.17 | 整体消费消息行数 | 1. 整体性能指标 1. 在测试结束时输出 1. 输出格式及示例 INFO: Consumed total msgs: 100, total rows: 1000000 |

<callout emoji="bell" background-color="light-orange" border-color="light-orange">
订阅也仅支持 json 配置文件方式，不支持命令行模式
</callout>

## 5. 性能

**性能设计**：数据准备工作前移
为实现性能需求，需对测试过程进行分阶段，把需要耗用 CPU 高的工作前移，放到数据准备阶段进行预处理及计算，在开始测量性能期间直接使用已处理好的数据，减小测试框架本身对性能影响。
**性能要求：**写入性能测试中测试框架时间占用控制在 30% 以内
以官网智能电表数据为例，写入 1 万子表，第子表 1 万 共 1 亿数据，创建 4 个VNODE ， 8 线程写入，测试框架消耗时间达到 30 % 以内

## 6. 兼容性

1. 对原有写入命令行实现全兼容
2. 对配置文件中的原有项实现全兼容

## 7. 运维

1. 软件使用 C 语言编写，运维期间需配置好 core 文件生成，方便崩溃后快速定位原因
2. 软件在配置文件中可配置日志生成文件
3. 软件可多个实例同时独立运行，互不影响
4. 提供 debug 模式运行选项 -g

## 8. 使用场景

1. 对各种写入方式性能测试对比
2. 对不同查询的性能测试对比
3. 对不同订阅参数的性能测试对比
4. TDengine 产品性能基准测试
5. TDengine 产品压力测试

## 9. 约束和限制

约束：无
限制：
1. 建表数量受限：数据都存储在内存中，能够创建子表数量受内存限制，根据内存大小控制子表创建数量
2. 数据准备规模受限：提前准备数据也都存储在内存中，创建准备数据大小受内存限制，根据内存大小合理配置准备数据在大小。

## 10. 常见错误和排查

1. 配置文件为标准 json 格式，如果 json 格式不正确会抛出解析错误，请修正格式后再重试
2. 抛出引擎错误需参考引擎错误码表说明
3. 支持调试模式 -g 参数,可输出详细日志

## 11. 可观测性

程序启动后会依次输出以下可观测内容：
1. Json 配置文件信息
2. 数据准备过程信息
3. 连接服务器信息
4. 创建数据库或表等信息
5. 写入数据或查询及订阅相关进度等信息
6. 总体指标统计信息

## 12. 安装和卸载

不提供单独安装包，随 TDengine 安装/卸载。

## 13. 文档

1. 需要在企业版文档中增加工具使用说明
2. 需要在官网文档中提供对外使用说明

## 14. 参考文档

1. 需求文档 [taosBenchmark-Requirement Spec](https://taosdata.feishu.cn/wiki/XnnywyidriNKBGk9efBcJwkmnEd)
2. 引擎接口文档 ：[内核](https://taosdata.feishu.cn/wiki/NsOlwRcXbifgbtkbHzYcPC3Mnlg)
3. 命令行处理开源文档 http://emfisis.physics.uiowa.edu/Software/C/libargp

## 15. 附录

无
