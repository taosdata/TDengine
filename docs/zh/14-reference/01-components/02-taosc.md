---
title: 客户端驱动参考手册
sidebar_label: taosc
toc_max_heading_level: 4
---

TDengine 客户端驱动提供了应用编程所需要的全部 API，并且在整个集群的分布式计算中扮演着重要角色。客户端驱动的行为除了 API 及其具体参数以外，也可以通过配置文件的形式进行全局控制，本节列举 TDengine 客户端可以使用的配置参数。

## 配置参数

| 参数名称     | 参数含义                                                    |
|:-----------:|:----------------------------------------------------------:|
|firstEp | taos 启动时，主动连接的集群中首个 dnode 的 endpoint，缺省值：hostname:6030，若无法获取该服务器的 hostname，则赋值为 localhost  |
|secondEp | 启动时，如果 firstEp 连接不上，尝试连接集群中第二个 dnode 的 endpoint，没有缺省值 |
|numOfRpcSessions | 一个客户端能创建的最大连接数，取值范围：10-50000000(单位为毫秒)；缺省值：500000 |
|telemetryReporting | 是否上传 telemetry，0: 不上传，1： 上传；缺省值：1 |
|crashReporting | 是否上传 telemetry，0: 不上传，1： 上传；缺省值：1  |
|queryPolicy | 查询语句的执行策略，1: 只使用 vnode，不使用 qnode; 2: 没有扫描算子的子任务在 qnode 执行，带扫描算子的子任务在 vnode 执行; 3: vnode 只运行扫描算子，其余算子均在 qnode 执行 ；缺省值：1 |
|querySmaOptimize | sma index 的优化策略，0: 表示不使用 sma index，永远从原始数据进行查询; 1: 表示使用 sma index，对符合的语句，直接从预计算的结果进行查询；缺省值：0 |
|keepColumnName | Last、First、LastRow 函数查询且未指定别名时，自动设置别名为列名（不含函数名），因此 order by 子句如果引用了该列名将自动引用该列对应的函数; 1: 表示自动设置别名为列名(不包含函数名), 0: 表示不自动设置别名; 缺省值: 0 |
|countAlwaysReturnValue | count/hyperloglog函数在输入数据为空或者NULL的情况下是否返回值; 0：返回空行，1：返回; 缺省值 1; 该参数设置为 1 时，如果查询中含有 INTERVAL 子句或者该查询使用了TSMA时, 且相应的组或窗口内数据为空或者NULL， 对应的组或窗口将不返回查询结果. 注意此参数客户端和服务端值应保持一致. |
|multiResultFunctionStarReturnTags | 查询超级表时，last(\*)/last_row(\*)/first(\*) 是否返回标签列；查询普通表、子表时，不受该参数影响; 0：不返回标签列，1：返回标签列 ; 缺省值: 0; 该参数设置为 0 时，last(\*)/last_row(\*)/first(\*) 只返回超级表的普通列；为 1 时，返回超级表的普通列和标签列 |
|maxTsmaCalcDelay| 查询时客户端可允许的tsma计算延迟, 若tsma的计算延迟大于配置值, 则该TSMA将不会被使用.; 取值范围: 600s - 86400s, 即10分钟-1小时 ; 缺省值：600 秒|
|tsmaDataDeleteMark |TSMA计算的历史数据中间结果保存时间, 单位为毫秒; 取值范围：>= 3600000, 即大于等于1h; 缺省值: 86400000, 即1d  |
|timezone | 时区; 缺省从系统中动态获取当前的时区设置 |
|locale | 系统区位信息及编码格式, 缺省从系统中获取 |
|charset | 字符集编码，缺省从系统中获取 |
|metaCacheMaxSize | 指定单个客户端元数据缓存大小的最大值, 单位 MB; 缺省值 -1，表示无限制 |
|logDir | 日志文件目录，客户端运行日志将写入该目录, 缺省值: /var/log/taos |
|minimalLogDirGB | 当日志文件夹所在磁盘可用空间大小小于该值时，停止写日志; 缺省值  1 |
|numOfLogLines | 单个日志文件允许的最大行数; 缺省值 10,000,000 |
|asyncLog | 是否异步写入日志，0：同步；1：异步；缺省值：1 |
|logKeepDays | 日志文件的最长保存时间; 缺省值: 0，表示无限保存; 大于 0 时，日志文件会被重命名为 taosdlog.xxx，其中 xxx 为日志文件最后修改的时间戳|
|smlChildTableName | schemaless 自定义的子表名的 key, 无缺省值 |
|smlAutoChildTableNameDelimiter | schemaless tag之间的连接符，连起来作为子表名，无缺省值 |
|smlTagName | schemaless tag 为空时默认的 tag 名字, 缺省值 "_tag_null" |
|smlTsDefaultName | schemaless自动建表的时间列名字通过该配置设置, 缺省值 "_ts" |
|smlDot2Underline | schemaless 把超级表名中的 dot 转成下划线 |
|enableCoreFile | crash 时是否生成 core 文件，0: 不生成， 1： 生成；缺省值：1 |
|enableScience | 是否开启科学计数法显示浮点数; 0: 不开始, 1: 开启；缺省值：1 |
|compressMsgSize | 是否对 RPC 消息进行压缩; -1: 所有消息都不压缩; 0: 所有消息都压缩; N (N>0): 只有大于 N 个字节的消息才压缩; 缺省值 -1|
|queryTableNotExistAsEmpty | 查询表不存在时是否返回空结果集; false: 返回错误; true: 返回空结果集; 缺省值 false|

## API

请参考[连接器](../../connector)
