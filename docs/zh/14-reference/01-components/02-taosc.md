---
title: 客户端驱动参考手册
sidebar_label: taosc
toc_max_heading_level: 4
---

TDengine 客户端驱动提供了应用编程所需要的全部 API，并且在整个集群的分布式计算中扮演着重要角色。客户端驱动的行为除了 API 及其具体参数以外，也可以通过配置文件的形式进行全局控制，本节列举 TDengine 客户端可以使用的配置参数。

## 配置参数

### 连接相关
|参数名称|支持版本|参数含义|
|----------------------|----------|-------------|
|firstEp               |                  |启动时，主动连接的集群中首个 dnode 的 endpoint，缺省值：hostname:6030，若无法获取该服务器的 hostname，则赋值为 localhost|
|secondEp              |                  |启动时，如果 firstEp 连接不上，尝试连接集群中第二个 dnode 的 endpoint，没有缺省值|
|compressMsgSize       |                  |是否对 RPC 消息进行压缩；-1：所有消息都不压缩；0：所有消息都压缩；N (N>0)：只有大于 N 个字节的消息才压缩；缺省值 -1|
|shellActivityTimer    |                  |客户端向 mnode 发送心跳的时长，单位为秒，取值范围 1-120，默认值 3|
|numOfRpcSessions      |                  |RPC 支持的最大连接数，取值范围 100-100000，缺省值 30000|
|numOfRpcThreads       |                  |RPC 收发数据线程数目，默认值为 CPU 核数的一半|
|numOfTaskQueueThreads |                  |客户端处理 RPC消息的线程数|
|timeToGetAvailableConn| 3.3.4.*之后取消   |获得可用连接的最长等待时间，取值范围 10-50000000，单位为毫秒，缺省值 500000|
|useAdapter            |          |内部参数，是否使用 taosadapter，影响 CSV 文件导入|
|shareConnLimit        |3.3.4.x 后|内部参数，一个链接可以共享的查询数目，取值范围 1-256，默认值 10|
|readTimeout           |3.3.4.x 后|内部参数，最小超时时间，取值范围 64-604800，单位为秒，默认值 900|

### 查询相关
|参数名称|支持版本|参数含义|
|---------------------------------|---------|-|
|countAlwaysReturnValue           |         |count/hyperloglog 函数在输入数据为空或者 NULL 的情况下是否返回值；0：返回空行，1：返回；默认值 1；该参数设置为 1 时，如果查询中含有 INTERVAL 子句或者该查询使用了 TSMA 时，且相应的组或窗口内数据为空或者 NULL，对应的组或窗口将不返回查询结果；注意此参数客户端和服务端值应保持一致|
|keepColumnName                   |         |Last、First、LastRow 函数查询且未指定别名时，自动设置别名为列名（不含函数名），因此 order by 子句如果引用了该列名将自动引用该列对应的函数；1：表示自动设置别名为列名(不包含函数名)，0：表示不自动设置别名；缺省值：0|
|multiResultFunctionStarReturnTags|3.3.3.0 后|查询超级表时，last(\*)/last_row(\*)/first(\*) 是否返回标签列；查询普通表、子表时，不受该参数影响；0：不返回标签列，1：返回标签列；缺省值：0；该参数设置为 0 时，last(\*)/last_row(\*)/first(\*) 只返回超级表的普通列；为 1 时，返回超级表的普通列和标签列|
|metaCacheMaxSize                 |         |指定单个客户端元数据缓存大小的最大值，单位 MB；缺省值 -1，表示无限制|
|maxTsmaCalcDelay                 |         |查询时客户端可允许的 tsma 计算延迟，若 tsma 的计算延迟大于配置值，则该 TSMA 将不会被使用；取值范围 600s - 86400s，即 10 分钟 - 1 小时；缺省值：600 秒|
|tsmaDataDeleteMark               |         |TSMA 计算的历史数据中间结果保存时间，单位为毫秒；取值范围 >= 3600000，即大于等于1h；缺省值：86400000，即 1d |
|queryPolicy                      |         |查询语句的执行策略，1：只使用 vnode，不使用 qnode；2：没有扫描算子的子任务在 qnode 执行，带扫描算子的子任务在 vnode 执行；3：vnode 只运行扫描算子，其余算子均在 qnode 执行；缺省值：1|
|queryTableNotExistAsEmpty        |         |查询表不存在时是否返回空结果集；false：返回错误；true：返回空结果集；缺省值 false|
|querySmaOptimize                 |         |sma index 的优化策略，0：表示不使用 sma index，永远从原始数据进行查询；1：表示使用 sma index，对符合的语句，直接从预计算的结果进行查询；缺省值：0|
|queryPlannerTrace                |         |内部参数，查询计划是否输出详细日志|
|queryNodeChunkSize               |         |内部参数，查询计划的块大小|
|queryUseNodeAllocator            |         |内部参数，查询计划的分配方法|
|queryMaxConcurrentTables         |         |内部参数，查询计划的并发数目|
|enableQueryHb                    |         |内部参数，是否发送查询心跳消息|
|minSlidingTime                   |         |内部参数，sliding 的最小允许值|
|minIntervalTime                  |         |内部参数，interval 的最小允许值|

### 写入相关
|参数名称|支持版本|参数含义|
|------------------------------|----------|-|
|smlChildTableName             |          |schemaless 自定义的子表名的 key，无缺省值|
|smlAutoChildTableNameDelimiter|          |schemaless tag 之间的连接符，连起来作为子表名，无缺省值|
|smlTagName                    |          |schemaless tag 为空时默认的 tag 名字，缺省值 "_tag_null"|
|smlTsDefaultName              |          |schemaless 自动建表的时间列名字通过该配置设置，缺省值 "_ts"|
|smlDot2Underline              |          |schemaless 把超级表名中的 dot 转成下划线|
|maxInsertBatchRows            |          |内部参数，一批写入的最大条数|

### 区域相关
|参数名称|支持版本|参数含义|
|-----------------|----------|-|
|timezone         |          |时区；缺省从系统中动态获取当前的时区设置|
|locale           |          |系统区位信息及编码格式，缺省从系统中获取|
|charset          |          |字符集编码，缺省从系统中获取|

### 存储相关
|参数名称|支持版本|参数含义|
|-----------------|----------|-|
|tempDir          |          |指定所有运行过程中的临时文件生成的目录，Linux 平台默认值为 /tmp|
|minimalTmpDirGB  |          |tempDir 所指定的临时文件目录所需要保留的最小空间，单位 GB，缺省值：1|

### 日志相关
|参数名称|支持版本|参数含义|
|-----------------|----------|-|
|logDir           |          |日志文件目录，运行日志将写入该目录，缺省值：/var/log/taos|
|minimalLogDirGB  |          |日志文件夹所在磁盘可用空间大小小于该值时，停止写日志，单位 GB，缺省值：1|
|numOfLogLines    |          |单个日志文件允许的最大行数，缺省值：10,000,000|
|asyncLog         |          |日志写入模式，0：同步，1：异步，缺省值：1|
|logKeepDays      |          |日志文件的最长保存时间，单位：天，缺省值：0，意味着无限保存，日志文件不会被重命名，也不会有新的日志文件滚动产生，但日志文件的内容有可能会不断滚动，取决于日志文件大小的设置；当设置为大于 0 的值时，当日志文件大小达到设置的上限时会被重命名为 taoslogx.yyy，其中 yyy 为日志文件最后修改的时间戳，并滚动产生新的日志文件|
|debugFlag        |          |运行日志开关，131（输出错误和警告日志），135（输出错误、警告和调试日志），143（输出错误、警告、调试和跟踪日志）；默认值 131 或 135 （取决于不同模块）|
|tmrDebugFlag     |          |定时器模块的日志开关，取值范围同上|
|uDebugFlag       |          |共用功能模块的日志开关，取值范围同上|
|rpcDebugFlag     |          |rpc 模块的日志开关，取值范围同上|
|jniDebugFlag     |          |jni 模块的日志开关，取值范围同上|
|qDebugFlag       |          |query 模块的日志开关，取值范围同上|
|cDebugFlag       |          |客户端模块的日志开关，取值范围同上|
|simDebugFlag     |          |内部参数，测试工具的日志开关，取值范围同上|
|tqClientDebugFlag|3.3.4.3 后|客户端模块的日志开关，取值范围同上|

### 调试相关
|参数名称|支持版本|参数含义|
|-----------------|-----------|-|
|crashReporting   |          |是否上传 crash 到 telemetry，0：不上传，1：上传；缺省值：1|
|enableCoreFile   |          |crash 时是否生成 core 文件，0：不生成，1：生成；缺省值：1|
|assert           |          |断言控制开关，缺省值：0|
|configDir        |          |配置文件所在目录|
|scriptDir        |          |内部参数，测试用例的目录|
|randErrorChance  |3.3.3.0 后|内部参数，用于随机失败测试|
|randErrorDivisor |3.3.3.0 后|内部参数，用于随机失败测试|
|randErrorScope   |3.3.3.0 后|内部参数，用于随机失败测试|
|safetyCheckLevel |3.3.3.0 后|内部参数，用于随机失败测试|
|simdEnable       |3.3.4.3 后|内部参数，用于测试 SIMD 加速|
|AVX512Enable     |3.3.4.3 后|内部参数，用于测试 AVX512 加速|

### SHELL 相关
|参数名称|支持版本|参数含义|
|-----------------|----------|-|
|enableScience    |          |是否开启科学计数法显示浮点数；0：不开始，1：开启；缺省值：1|

## API

请参考[连接器](../../connector)
