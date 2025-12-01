---
title: "TDengine 配置参数作用范围对比"
sidebar_label: "config-scope"
toc_max_heading_level: 4
---

本文档对比了 TDengine TSDB 中 taosd（服务端）和 taosc（客户端）的配置参数，明确标识每个参数的作用范围。

## 配置参数作用范围对比表

| 参数名称 | 作用范围 | 说明 |
|---------|---------|------|
| **连接相关** | | |
| firstEp | both | 启动时，主动连接的集群中首个 dnode 的 endpoint |
| secondEp | both | 启动时，如果 firstEp 连接不上，尝试连接集群中第二个 dnode 的 endpoint |
| fqdn | taosd | taosd 监听的服务地址 |
| serverPort | both | taosd 监听的端口 |
| compressMsgSize | both | 是否对 RPC 消息进行压缩 |
| shellActivityTimer | both | 客户端向 mnode 发送心跳的时长 |
| numOfRpcSessions | both | RPC 支持的最大连接数 |
| numOfRpcThreads | both | RPC 收发数据的线程数目 |
| numOfTaskQueueThreads | both | RPC 处理消息的线程数目 |
| rpcQueueMemoryAllowed | taosd | dnode 已经收到并等待处理的 RPC 消息占用内存的最大值 |
| resolveFQDNRetryTime | taosd | FQDN 解析失败时的重试次数 |
| timeToGetAvailableConn | taosc | 获得可用连接的最长等待时间 |
| maxShellConns | taosd | 允许创建的最大连接数 |
| maxRetryWaitTime | both | 重连最大超时时间，从重试时候开始计算 |
| shareConnLimit | both | 一个链接可以共享的请求的数目 |
| readTimeout | both | 单个请求最小超时时间 |
| useAdapter | taosc | 是否使用 taosadapter，影响 CSV 文件导入 |
| **监控相关** | | |
| monitor | taosd | 是否收集监控数据并上报 |
| monitorFqdn | taosd | taosKeeper 服务所在服务器的地址 |
| monitorPort | taosd | taosKeeper 服务所监听的端口号 |
| monitorInterval | taosd | 监控数据库记录系统参数（CPU/内存）的时间间隔 |
| monitorMaxLogs | taosd | 缓存的待上报日志条数 |
| monitorComp | taosd | 是否采用压缩方式上报监控日志 |
| monitorLogProtocol | taosd | 是否打印监控日志 |
| monitorForceV2 | taosd | 是否使用 V2 版本协议上报日志 |
| telemetryReporting | taosd | 是否上传 telemetry |
| telemetryServer | taosd | telemetry 服务器地址 |
| telemetryPort | taosd | telemetry 服务器端口号 |
| telemetryInterval | taosd | telemetry 上传时间间隔 |
| crashReporting | both | 是否使用 V2 版本协议上报日志 |
| enableMetrics | taosd | 是否打开写入诊断工具，收集并上传写入指标 |
| metricsInterval | taosd | 写入诊断工具上传写入指标的间隔 |
| metricsLevel | taosd | 写入诊断工具上传写入指标的级别 |
| **查询相关** | | |
| countAlwaysReturnValue | both | count/hyperloglog 函数在输入数据为空或者 NULL 的情况下是否返回值 |
| tagFilterCache | taosd | 是否缓存标签过滤结果 |
| stableTagFilterCache | taosd | 流计算中，是否缓存标签等值条件的过滤结果，不会因为增删子表、更新标签值或修改超级表标签而失效 |
| queryBufferSize | taosd | 查询可用的缓存大小 |
| queryRspPolicy | taosd | 查询响应策略 |
| queryUseMemoryPool | taosd | 查询是否使用内存池管理内存 |
| minReservedMemorySize | taosd | 内存池开启时，最小预留的系统可用内存数量 |
| singleQueryMaxMemorySize | taosd | 单个查询在单个节点 (dnode) 上可以使用的内存上限 |
| filterScalarMode | taosd | 强制使用标量过滤模式 |
| queryNoFetchTimeoutSec | taosd | 查询中当应用长时间不 FETCH 数据时的超时时间 |
| queryPlannerTrace | both | 查询计划是否输出详细日志 |
| queryNodeChunkSize | both | 查询计划的块大小 |
| queryUseNodeAllocator | both | 查询计划的分配方法 |
| queryMaxConcurrentTables | both | 查询计划的分配方法 |
| queryRsmaTolerance | taosd | 查询计划的分配方法 |
| enableQueryHb | both | 是否发送查询心跳消息 |
| pqSortMemThreshold | taosd | 排序使用的内存阈值 |
| keepColumnName | taosc | Last、First、LastRow 函数查询且未指定别名时，自动设置别名为列名 |
| multiResultFunctionStarReturnTags | taosc | 查询超级表时，last(*)/last_row(*)/first(*) 是否返回标签列 |
| metaCacheMaxSize | taosc | 指定单个客户端元数据缓存大小的最大值 |
| maxTsmaCalcDelay | taosc | 查询时客户端可允许的 tsma 计算延迟 |
| tsmaDataDeleteMark | taosc | TSMA 计算的历史数据中间结果保存时间 |
| queryPolicy | taosc | 查询语句的执行策略 |
| queryTableNotExistAsEmpty | taosc | 查询表不存在时是否返回空结果集 |
| querySmaOptimize | taosc | querSmaOptimize，永远从原始数据进行查询 |
| queryMaxConcurrentTables | taosc | 查询计划的并发数目 |
| minSlidingTime | taosc | sliding 的最小允许值 |
| minIntervalTime | taosc | interval 的最小允许值 |
| compareAsStrInGreatest | taosc | 用于决定 greatest、least 函数的参数既有数值类型又有字符串类型时，比较类型的转换规则 |
| showFullCreateTableColumn | taosc | show create table 的返回值是否带 column 的压缩信息 |
| **区域相关** | | |
| timezone | both | 时区 |
| locale | both | 系统区位信息及编码格式 |
| charset | both | 字符集编码 |
| **存储相关** | | |
| dataDir | taosd | 数据文件目录，所有的数据文件都将写入该目录 |
| diskIDCheckEnabled | taosd | 在重启 dnode 时增加了检查 dataDir 所在磁盘 id 是否发生改变 |
| tempDir | both | 指定所有系统运行过程中的临时文件生成的目录 |
| minimalDataDirGB | taosd | dataDir 指定的时序数据存储目录所需要保留的最小空间 |
| minimalTmpDirGB | both | tempDir 所指定的临时文件目录所需要保留的最小空间 |
| minDiskFreeSize | taosd | 当某块磁盘上的可用空间小于等于这个阈值时，该磁盘将不再被选择用于生成新的数据文件 |
| ssAutoMigrateIntervalSec | taosd | 本地数据文件自动迁移共享存储的触发周期 |
| ssEnabled | taosd | 是否启用共享存储 |
| ssAccessString | taosd | 共享存储访问参数字符串 |
| ssPageCacheSize | taosd | 共享存储 page cache 缓存页数目 |
| ssUploadDelaySec | taosd | data 文件持续多长时间不再变动后上传至共享存储 |
| cacheLazyLoadThreshold | taosd | 缓存的装载策略 |
| **集群相关** | | |
| supportVnodes | taosd | dnode 支持的最大 vnode 数目 |
| numOfCommitThreads | taosd | 落盘线程的最大数量 |
| numOfCompactThreads | taosd | 合并线程的最大数量 |
| numOfMnodeReadThreads | taosd | mnode 的 Read 线程数目 |
| numOfVnodeQueryThreads | taosd | vnode 的 Query 线程数目 |
| numOfVnodeFetchThreads | taosd | vnode 的 Fetch 线程数目 |
| numOfVnodeRsmaThreads | taosd | vnode 的 Rsma 线程数目 |
| numOfQnodeQueryThreads | taosd | qnode 的 Query 线程数目 |
| ttlUnit | taosd | ttl 参数的单位 |
| ttlPushInterval | taosd | ttl 检测超时频率 |
| ttlChangeOnWrite | taosd | ttl 到期时间是否伴随表的修改操作改变 |
| ttlBatchDropNum | taosd | ttl 一批删除子表的数目 |
| retentionSpeedLimitMB | taosd | 数据在不同级别硬盘上迁移时的速度限制 |
| maxTsmaNum | taosd | 集群内可创建的 TSMA 个数 |
| tmqMaxTopicNum | taosd | 订阅最多可建立的 topic 数量 |
| tmqRowSize | taosd | 订阅数据块的最大记录条数 |
| audit | taosd | 审计功能开关 |
| auditInterval | taosd | 审计数据上报的时间间隔 |
| auditCreateTable | taosd | 是否针对创建子表开启申计功能 |
| encryptAlgorithm | taosd | 数据加密算法 |
| encryptScope | taosd | 加密范围 |
| encryptPassAlgorithm | taosd | 加密存储用户密码功能开关 |
| enableWhiteList | taosd | 白名单功能开关 |
| syncLogBufferMemoryAllowed | taosd | 一个 dnode 允许的 sync 日志缓存消息占用的内存最大值 |
| syncApplyQueueSize | taosd | sync 日志 apply 队列的大小 |
| syncElectInterval | taosd | 用于同步模块调试 |
| syncHeartbeatInterval | taosd | 用于同步模块调试 |
| syncHeartbeatTimeout | taosd | 用于同步模块调试 |
| syncSnapReplMaxWaitN | taosd | 用于同步模块调试 |
| arbHeartBeatIntervalSec | taosd | 用于同步模块调试 |
| arbCheckSyncIntervalSec | taosd | 用于同步模块调试 |
| arbSetAssignedTimeoutSec | taosd | 用于同步模块调试 |
| mndLogRetention | taosd | 用于 mnode 模块调试 |
| skipGrant | taosd | 用于授权检查 |
| trimVDbIntervalSec | taosd | 用于删除过期数据 |
| ttlFlushThreshold | taosd | ttl 定时器的频率 |
| compactPullupInterval | taosd | 数据重整定时器的频率 |
| walFsyncDataSizeLimit | taosd | WAL 进行 FSYNC 的阈值 |
| walForceRepair | taosd | 强制修复 wal 文件 |
| transPullupInterval | taosd | mnode 执行事务的重试间 |
| forceKillTrans | taosd | 用于 mnode 事务模块调试 |
| mqRebalanceInterval | taosd | 消费者再平衡的时间间隔 |
| uptimeInterval | taosd | 用于记录系统启动时间 |
| timeseriesThreshold | taosd | 用于统计用量 |
| udf | taosd | 是否启动 UDF 服务 |
| udfdResFuncs | taosd | 用于统计用量 |
| udfdLdLibPath | taosd | 用于统计用量 |
| enableStrongPassword | taosd | 密码要符合一个要求：至少包含大写字母、小写字母、数字、特殊字符中的三类 |
| **流计算参数** | | |
| numOfMnodeStreamMgmtThreads | taosd | mnode 流计算管理线程个数 |
| numOfStreamMgmtThreads | taosd | snode 流计算管理线程个数 |
| numOfVnodeStreamReaderThreads | taosd | vnode 流计算读线程个数 |
| numOfStreamTriggerThreads | taosd | 流计算触发线程个数 |
| numOfStreamRunnerThreads | taosd | 流计算执行线程个数 |
| streamBufferSize | taosd | 流计算可以使用的最大缓存大小，只适用于 %%trows 的结果缓存 |
| streamNotifyMessageSize | taosd | 用于控制事件通知的消息大小 |
| streamNotifyFrameSize | taosd | 用于控制事件通知消息发送时底层的帧大小 |
| **日志相关** | | |
| logDir | both | 日志文件目录，运行日志将写入该目录 |
| minimalLogDirGB | both | 日志文件夹所在磁盘可用空间大小小于该值时，停止写日志 |
| numOfLogLines | both | 单个日志文件允许的最大行数 |
| asyncLog | both | 日志写入模式 |
| logKeepDays | both | 日志文件的最长保存时间 |
| slowLogThreshold | taosd | 慢查询门限值，大于等于门限值认为是慢查询 |
| slowLogMaxLen | taosd | 慢查询日志最大长度 |
| slowLogScope | taosd | 慢查询记录类型 |
| slowLogExceptDb | taosd | 指定的数据库不上报慢查询，仅支持配置换一个数据库 |
| debugFlag | both | 运行日志开关，该参数的设置会影响到所有模块的开关 |
| tmrDebugFlag | both | 定时器模块的日志开关 |
| uDebugFlag | both | 共用功能模块的日志开关 |
| rpcDebugFlag | both | rpc 模块的日志开关 |
| qDebugFlag | both | query 模块的日志开关 |
| dDebugFlag | taosd | dnode 模块的日志开关 |
| vDebugFlag | taosd | vnode 模块的日志开关 |
| mDebugFlag | taosd | mnode 模块的日志开关 |
| azDebugFlag | taosd | S3 模块的日志开关 |
| sDebugFlag | taosd | sync 模块的日志开关 |
| tsdbDebugFlag | taosd | tsdb 模块的日志开关 |
| tqDebugFlag | taosd | tq 模块的日志开关 |
| fsDebugFlag | taosd | fs 模块的日志开关 |
| udfDebugFlag | taosd | udf 模块的日志开关 |
| smaDebugFlag | taosd | sma 模块的日志开关 |
| idxDebugFlag | taosd | index 模块的日志开关 |
| tdbDebugFlag | taosd | tdb 模块的日志开关 |
| metaDebugFlag | taosd | meta 模块的日志开关 |
| stDebugFlag | taosd | stream 模块的日志开关 |
| sndDebugFlag | taosd | snode 模块的日志开关 |
| jniDebugFlag | taosc | jni 模块的日志开关 |
| cDebugFlag | taosc | 客户端模块的日志开关 |
| simDebugFlag | taosc | 测试工具的日志开关 |
| tqClientDebugFlag | taosc | 测试工具的日志开关 |
| rpcRecvLogThreshold| taosd| rpc 模块的警告日志的阈值|
| **调试相关** | | |
| enableCoreFile | both | crash 时是否生成 core 文件 |
| configDir | both | 配置文件所在目录 |
| forceReadConfig | taosd | 是否使用持久化的局部配置参数 |
| scriptDir | both | 测试工具的脚本目录 |
| assert | both | 断言控制开关 |
| randErrorChance | both | 用于随机失败测试 |
| randErrorDivisor | both | 用于随机失败测试 |
| randErrorScope | both | 用于随机失败测试 |
| safetyCheckLevel | both | 用于随机失败测试 |
| experimental | taosd | 用于一些实验特性 |
| simdEnable | both | 用于测试 SIMD 加速 |
| AVX512Enable | both | 用于测试 AVX512 加速 |
| rsyncPort | taosd | 用于调试流计算 |
| snodeAddress | taosd | 用于调试流计算 |
| checkpointBackupDir | taosd | 用于恢复 snode 数据 |
| enableAuditDelete | taosd | 用于测试审计功能 |
| slowLogThresholdTest | taosd | 用于测试慢日志 |
| bypassFlag | both | 用于短路测试 |
| **压缩参数** | | |
| fPrecision | taosd | 设置 float 类型浮点数压缩精度，小于此值的浮点数尾数部分将被截断 |
| dPrecision | taosd | 设置 double 类型浮点数压缩精度，小于此值的浮点数尾数部分将被截取 |
| lossyColumn | taosd | 对 float 和/或 double 类型启用 TSZ 有损压缩 |
| ifAdtFse | taosd | 在启用 TSZ 有损压缩时，使用 FSE 算法替换 HUFFMAN 算法 |
| enableIpv6 | taosd | 用于节点直接通过 ipv6 通信 |
| maxRange | taosd | 用于有损压缩设置 |
| curRange | taosd | 用于有损压缩设置 |
| compressor | taosd | 用于有损压缩设置 |
| **写入相关** | | |
| smlChildTableName | taosc | schemaless 自定义的子表名的 key |
| smlAutoChildTableNameDelimiter | taosc | schemaless tag 之间的连接符，连起来作为子表名 |
| smlTagName | taosc | schemaless tag 为空时默认的 tag 名字 |
| smlTsDefaultName | taosc | schemaless 自动建表的时间列名字通过该配置设置 |
| smlDot2Underline | taosc | schemaless 把超级表名中的 dot 转成下划线 |
| maxInsertBatchRows | taosc | 一批写入的最大条数 |
| **SHELL 相关** | | |
| enableScience | taosc | 是否开启科学计数法显示浮点数 |
| **WebSocket 相关** | | |
| serverPort | taosc | taosadapter 监听的端口 |
| timezone | taosc | 时区 |
| logDir | taosc | 日志文件目录，运行日志将写入该目录 |
| logKeepDays | taosc | 日志文件的最长保存时间 |
| rotationCount | taosc | 日志文件轮转数量 |
| rotationSize | taosc | 单个日志文件最大大小（支持 KB/MB/GB 单位） |
| compression | taosc | 是否对 WebSocket 消息进行压缩 |
| adapterList | taosc | taosAdapter 地址列表，用于负载均衡和故障转移 |
| connRetries | taosc | 连接失败时的最大重试次数 |
| retryBackoffMs | taosc | 连接失败时的初始等待时间（毫秒） |
| retryBackoffMaxMs | taosc | 连接失败时的最大等待时间（毫秒） |

## 说明

- **taosd**: 仅服务端生效的配置参数
- **taosc**: 仅客户端生效的配置参数  
- **both**: 服务端和客户端都生效的配置参数
